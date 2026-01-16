import asyncio
from unittest.mock import AsyncMock

import pytest
from concurrent.futures.process import BrokenProcessPool

from common.repo import TaskRow
from worker.worker import main as worker_main


class DummySession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def commit(self):
        return None

    async def rollback(self):
        return None


@pytest.mark.asyncio
async def test_worker_timeout_resets_pool(monkeypatch):
    worker = worker_main.Worker()
    monkeypatch.setattr(worker_main, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "try_claim", AsyncMock())
    monkeypatch.setattr(worker, "_reset_pool", AsyncMock())
    monkeypatch.setattr(worker_main, "mark_completed", AsyncMock())
    mark_failed = AsyncMock(return_value="failed")
    monkeypatch.setattr(worker_main, "mark_failed_or_retry", mark_failed)

    async def fake_run_qasm(_task):
        raise asyncio.TimeoutError()

    monkeypatch.setattr(worker, "_run_qasm", fake_run_qasm)

    task = TaskRow(id="t1", qc="q", shots=1, status="processing", attempts=1, max_attempts=1)
    await worker.sem.acquire()
    await worker._run_one(task)

    worker._reset_pool.assert_awaited()
    mark_failed.assert_awaited()


@pytest.mark.asyncio
async def test_worker_broken_pool_resets(monkeypatch):
    worker = worker_main.Worker()
    monkeypatch.setattr(worker_main, "SessionLocal", lambda: DummySession())
    monkeypatch.setattr(worker, "try_claim", AsyncMock())
    monkeypatch.setattr(worker, "_reset_pool", AsyncMock())
    monkeypatch.setattr(worker_main, "mark_completed", AsyncMock())
    mark_failed = AsyncMock(return_value="failed")
    monkeypatch.setattr(worker_main, "mark_failed_or_retry", mark_failed)

    async def fake_run_qasm(_task):
        raise BrokenProcessPool("boom")

    monkeypatch.setattr(worker, "_run_qasm", fake_run_qasm)

    task = TaskRow(id="t2", qc="q", shots=1, status="processing", attempts=1, max_attempts=1)
    await worker.sem.acquire()
    await worker._run_one(task)

    worker._reset_pool.assert_awaited()
    mark_failed.assert_awaited()
