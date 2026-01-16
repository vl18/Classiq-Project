from __future__ import annotations

import asyncio
import os
import socket
import time
import traceback
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from typing import Any, Dict, Optional

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine

from common.config import Settings
from common.db import create_engine, session_factory, ping_db
from common.logging_config import setup_logging, log_event
from common.repo import (
    TaskRow,
    claim_tasks,
    requeue_stale_processing,
    mark_completed,
    mark_failed_or_retry,
    queue_counts,
)
from .cpu import available_cpu_cores
from .executor import run_qasm3_counts


settings = Settings()
logger = setup_logging(
    service="worker",
    level=settings.log_level,
    log_dir=settings.log_dir,
    max_bytes=settings.log_max_bytes,
    backup_count=settings.log_backup_count,
)

engine: AsyncEngine = create_engine(settings)
SessionLocal = session_factory(engine)

WORKER_ID = f"{socket.gethostname()}:{os.getpid()}"


def retry_delay(attempt: int) -> int:
    # Simple exponential backoff with cap
    base = settings.task_retry_base_seconds
    cap = settings.task_retry_max_seconds
    delay = min(cap, base * (2 ** max(0, attempt - 1)))
    return int(delay)


@dataclass
class InFlight:
    task: TaskRow
    submitted_at: float


class Worker:
    def __init__(self) -> None:
        self.cores = available_cpu_cores()
        self.sem = asyncio.Semaphore(self.cores)  # bound concurrent executions
        self.queue: asyncio.Queue[TaskRow] = asyncio.Queue(maxsize=self.cores * 2)
        self._last_sweep = 0.0
        self._pool_lock = asyncio.Lock()
        self._pool_broken = False
        self.pool = self._create_pool()

    def _create_pool(self) -> ProcessPoolExecutor:
        ctx = mp.get_context("spawn")
        return ProcessPoolExecutor(max_workers=self.cores, mp_context=ctx)

    async def _reset_pool(self, *, reason: str, error: str) -> None:
        async with self._pool_lock:
            if self._pool_broken:
                return
            self._pool_broken = True
            try:
                self.pool.shutdown(wait=False, cancel_futures=True)
            except Exception:
                pass
            self.pool = self._create_pool()
            self._pool_broken = False
            log_event(logger, event="pool_reset", worker_id=WORKER_ID, reason=reason, error=error)

    async def _run_qasm(self, task: TaskRow) -> tuple[Dict[str, Any], Dict[str, Any]]:
        loop = asyncio.get_running_loop()
        fut = loop.run_in_executor(self.pool, run_qasm3_counts, task.qc, task.shots)
        timeout = settings.task_exec_timeout_seconds
        if timeout and timeout > 0:
            return await asyncio.wait_for(fut, timeout=timeout)
        return await fut

    async def start(self) -> None:
        await ping_db(engine)
        log_event(logger, event="startup", worker_id=WORKER_ID, cores=self.cores, pool_size=self.cores, channel=settings.worker_channel)

        # Kickstart: claim immediately
        await self.try_claim(reason="startup")

        # Start background tasks: notify listener, slow poll, metrics
        await asyncio.gather(
            self.notify_dispatcher(),
            self.slow_poll_dispatcher(),
            self.metrics_reporter(),
            self.consumer_loop(),
        )

    async def notify_dispatcher(self) -> None:
        """Lightweight dispatcher: LISTEN/NOTIFY and trigger try_claim()."""
        while True:
            conn: Optional[asyncpg.Connection] = None
            try:
                conn = await asyncpg.connect(settings.database_url.replace("postgresql+asyncpg://", "postgresql://"))
                await conn.add_listener(settings.worker_channel, self._on_notify)
                await conn.execute(f"LISTEN {settings.worker_channel};")
                log_event(logger, event="listen_ready", worker_id=WORKER_ID, channel=settings.worker_channel)
                # Keep connection alive
                while True:
                    await asyncio.sleep(60)
                    if conn.is_closed():
                        raise ConnectionError("listen connection closed")
                    await conn.execute("SELECT 1")
            except Exception as e:
                log_event(logger, event="listen_error", worker_id=WORKER_ID, error=str(e))
                await asyncio.sleep(2)
            finally:
                if conn is not None and not conn.is_closed():
                    await conn.close()

    def _on_notify(self, connection, pid, channel, payload) -> None:
        # This callback runs in asyncpg internals; schedule coroutine safely.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.call_soon_threadsafe(asyncio.create_task, self.try_claim(reason="notify", payload=payload))

    async def slow_poll_dispatcher(self) -> None:
        while True:
            await asyncio.sleep(settings.worker_poll_interval_seconds)
            await self.try_claim(reason="poll")

    async def metrics_reporter(self) -> None:
        while True:
            await asyncio.sleep(settings.metrics_interval_seconds)
            async with SessionLocal() as session:
                counts = await queue_counts(session)
            log_event(
                logger,
                event="metrics",
                worker_id=WORKER_ID,
                pending=counts.get("pending", 0),
                processing=counts.get("processing", 0),
                completed=counts.get("completed", 0),
                failed=counts.get("failed", 0),
                inmem_queue=self.queue.qsize(),
                pool_size=self.cores,
            )

    async def try_claim(self, *, reason: str, payload: Optional[str] = None) -> None:
        if self._pool_broken:
            log_event(logger, event="claim_skipped_pool_unhealthy", worker_id=WORKER_ID, reason=reason)
            return
        # Avoid pointless DB work if we're full.
        free_slots = self.queue.maxsize - self.queue.qsize()
        if free_slots <= 0:
            log_event(logger, event="claim_skipped_full", worker_id=WORKER_ID, reason=reason)
            return

        t0 = time.perf_counter()

        # Periodic stale sweep (cheap & safe)
        now = time.time()
        if now - self._last_sweep >= settings.worker_stale_sweep_seconds:
            async with SessionLocal() as session:
                n = await requeue_stale_processing(session)
                await session.commit()
            self._last_sweep = now
            if n:
                log_event(logger, event="stale_requeued", worker_id=WORKER_ID, count=n)

        batch = min(settings.worker_claim_batch, free_slots)

        async with SessionLocal() as session:
            tasks = await claim_tasks(
                session,
                worker_id=WORKER_ID,
                lease_seconds=settings.worker_lease_seconds,
                limit=batch,
            )
            await session.commit()

        if not tasks:
            return

        for t in tasks:
            await self.queue.put(t)
            log_event(logger, event="task_claimed", task_id=t.id, worker_id=WORKER_ID, attempt=t.attempts, shots=t.shots, reason=reason)

        log_event(logger, event="claim_done", worker_id=WORKER_ID, reason=reason, claimed=len(tasks), duration_ms=int((time.perf_counter()-t0)*1000))

    async def consumer_loop(self) -> None:
        while True:
            task = await self.queue.get()
            await self.sem.acquire()
            asyncio.create_task(self._run_one(task))

    async def _run_one(self, task: TaskRow) -> None:
        t_start = time.perf_counter()
        log_event(logger, event="task_started", task_id=task.id, worker_id=WORKER_ID, attempt=task.attempts, shots=task.shots)

        try:
            outcome = "completed"
            counts: Dict[str, Any] = {}
            exec_metrics: Dict[str, Any] = {}
            err: Optional[str] = None

            try:
                counts, exec_metrics = await self._run_qasm(task)
            except asyncio.TimeoutError as e:
                outcome = "error"
                err = f"TimeoutError: task exceeded {settings.task_exec_timeout_seconds}s"
                await self._reset_pool(reason="timeout", error=str(e))
            except BrokenProcessPool as e:
                outcome = "error"
                err = f"BrokenProcessPool: {e}"
                await self._reset_pool(reason="broken_pool", error=str(e))
            except Exception as e:
                outcome = "error"
                err = f"{type(e).__name__}: {e}"
                err += f" | trace: {traceback.format_exc(limit=5).strip()}"

            duration_ms = int((time.perf_counter() - t_start) * 1000)

            async with SessionLocal() as session:
                try:
                    if outcome == "completed":
                        ok = await mark_completed(
                            session,
                            task_id=task.id,
                            worker_id=WORKER_ID,
                            result=counts,
                            metrics={"duration_ms": duration_ms, **exec_metrics},
                        )
                        await session.commit()
                        if ok:
                            log_event(
                                logger,
                                event="task_completed",
                                task_id=task.id,
                                worker_id=WORKER_ID,
                                duration_ms=duration_ms,
                                attempt=task.attempts,
                                **exec_metrics,
                            )
                        else:
                            log_event(
                                logger,
                                event="task_complete_skipped",
                                task_id=task.id,
                                worker_id=WORKER_ID,
                                duration_ms=duration_ms,
                                attempt=task.attempts,
                                reason="rowcount=0",
                            )
                    else:
                        next_delay = retry_delay(task.attempts)
                        new_state = await mark_failed_or_retry(
                            session,
                            task_id=task.id,
                            worker_id=WORKER_ID,
                            error=err or "Unknown error",
                            attempts=task.attempts,
                            max_attempts=task.max_attempts,
                            retry_delay_seconds=next_delay,
                            metrics={"duration_ms": duration_ms, "error": err},
                        )
                        await session.commit()
                        log_event(
                            logger,
                            event="task_failed",
                            task_id=task.id,
                            worker_id=WORKER_ID,
                            duration_ms=duration_ms,
                            attempt=task.attempts,
                            new_state=new_state,
                            retry_in_seconds=(next_delay if new_state == "pending" else None),
                            error=err,
                        )
                except Exception as e:
                    try:
                        await session.rollback()
                    except Exception:
                        pass
                    log_event(
                        logger,
                        event="db_update_failed",
                        task_id=task.id,
                        worker_id=WORKER_ID,
                        error=f"{type(e).__name__}: {e}",
                    )
        finally:
            # After completion/failure, immediately try to claim more (reduces latency even if NOTIFY was missed)
            self.sem.release()
            self.queue.task_done()
            await self.try_claim(reason="after_completion")



async def main() -> None:
    w = Worker()
    await w.start()


if __name__ == "__main__":
    asyncio.run(main())
