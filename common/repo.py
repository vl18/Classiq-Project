from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True)
class TaskRow:
    id: str
    qc: str
    shots: int
    status: str
    attempts: int
    max_attempts: int
    locked_by: Optional[str] = None
    lease_expires_at: Optional[str] = None
    next_run_at: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    last_error: Optional[str] = None


def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)


async def create_task(
    session: AsyncSession,
    *,
    qc: str,
    shots: int,
    max_attempts: int,
) -> str:
    q = text(
        """
        INSERT INTO tasks (qc, shots, status, max_attempts)
        VALUES (:qc, :shots, 'pending', :max_attempts)
        RETURNING id::text;
        """
    )
    res = await session.execute(q, {"qc": qc, "shots": shots, "max_attempts": max_attempts})
    (task_id,) = res.one()
    return task_id


async def notify_task(session: AsyncSession, *, channel: str, payload: str) -> None:
    # pg_notify expects text payload
    q = text("SELECT pg_notify(:channel, :payload);")
    await session.execute(q, {"channel": channel, "payload": payload})


async def get_task_status(session: AsyncSession, task_id: str) -> Optional[Dict[str, Any]]:
    q = text(
        """
        SELECT status, result, last_error
        FROM tasks
        WHERE id = :id
        """
    )
    res = await session.execute(q, {"id": task_id})
    row = res.first()
    if not row:
        return None
    status, result, last_error = row
    return {"status": status, "result": result, "last_error": last_error}


async def claim_tasks(
    session: AsyncSession,
    *,
    worker_id: str,
    lease_seconds: int,
    limit: int,
) -> List[TaskRow]:
    # Keep binds as :named; SQLAlchemy will compile them to asyncpg positional binds.
    q = text(
        """
        WITH cte AS (
          SELECT id
          FROM tasks
          WHERE status = 'pending' AND next_run_at <= now()
          ORDER BY created_at
          FOR UPDATE SKIP LOCKED
          LIMIT :limit
        )
        UPDATE tasks t
        SET status = 'processing',
            locked_by = :worker_id,
            lease_expires_at = now() + (:lease_seconds * interval '1 second'),
            started_at = COALESCE(started_at, now()),
            attempts = t.attempts + 1
        FROM cte
        WHERE t.id = cte.id
        RETURNING
          t.id::text,
          t.qc,
          t.shots,
          t.status,
          t.attempts,
          t.max_attempts,
          t.locked_by,
          t.lease_expires_at::text,
          t.next_run_at::text,
          t.result,
          t.last_error;
        """
    )
    res = await session.execute(
        q,
        {"worker_id": worker_id, "lease_seconds": int(lease_seconds), "limit": int(limit)},
    )
    rows: List[TaskRow] = []
    for r in res.fetchall():
        rows.append(
            TaskRow(
                id=r[0],
                qc=r[1],
                shots=int(r[2]),
                status=r[3],
                attempts=int(r[4]),
                max_attempts=int(r[5]),
                locked_by=r[6],
                lease_expires_at=r[7],
                next_run_at=r[8],
                result=r[9],
                last_error=r[10],
            )
        )
    return rows


async def requeue_stale_processing(session: AsyncSession) -> int:
    q = text(
        """
        UPDATE tasks
        SET status='pending', locked_by=NULL, lease_expires_at=NULL, next_run_at=now()
        WHERE status='processing' AND lease_expires_at IS NOT NULL AND lease_expires_at < now();
        """
    )
    res = await session.execute(q)
    return res.rowcount or 0


async def mark_completed(
    session: AsyncSession,
    *,
    task_id: str,
    worker_id: str,
    result: Dict[str, Any],
    metrics: Dict[str, Any],
) -> bool:
    q = text(
        """
        UPDATE tasks
        SET status='completed',
            result = :result,
            metrics = :metrics,
            completed_at=now(),
            locked_by=NULL,
            lease_expires_at=NULL
        WHERE id=:id AND status='processing' AND locked_by=:worker_id
        """
    ).bindparams(
        bindparam("result", type_=JSONB),
        bindparam("metrics", type_=JSONB),
    )
    res = await session.execute(
        q,
        {"id": task_id, "worker_id": worker_id, "result": result, "metrics": metrics},
    )
    return (res.rowcount or 0) > 0


async def mark_failed_or_retry(
    session: AsyncSession,
    *,
    task_id: str,
    worker_id: str,
    error: str,
    attempts: int,
    max_attempts: int,
    retry_delay_seconds: int,
    metrics: Dict[str, Any],
) -> str:
    if attempts >= max_attempts:
        q = text(
            """
            UPDATE tasks
            SET status='failed',
                last_error=:err,
                metrics = :metrics,
                completed_at=now(),
                locked_by=NULL,
                lease_expires_at=NULL
            WHERE id=:id AND status='processing' AND locked_by=:worker_id
            """
        ).bindparams(bindparam("metrics", type_=JSONB))
        await session.execute(
            q,
            {"id": task_id, "worker_id": worker_id, "err": error, "metrics": metrics},
        )
        return "failed"

    q = text(
        """
        UPDATE tasks
        SET status='pending',
            last_error=:err,
            metrics = :metrics,
            locked_by=NULL,
            lease_expires_at=NULL,
            next_run_at = now() + (:delay * interval '1 second')
        WHERE id=:id AND status='processing' AND locked_by=:worker_id
        """
    ).bindparams(bindparam("metrics", type_=JSONB))
    await session.execute(
        q,
        {"id": task_id, "worker_id": worker_id, "err": error, "delay": int(retry_delay_seconds), "metrics": metrics},
    )
    return "pending"


async def queue_counts(session: AsyncSession) -> Dict[str, int]:
    q = text("SELECT status, count(*)::int FROM tasks GROUP BY status;")
    res = await session.execute(q)
    out = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}
    for status, cnt in res.fetchall():
        out[str(status)] = int(cnt)
    return out
