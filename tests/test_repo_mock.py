import pytest
from unittest.mock import AsyncMock

from common import repo


class FakeResult:
    def __init__(self, *, one=None, first=None, fetchall=None, rowcount=None):
        self._one = one
        self._first = first
        self._fetchall = fetchall or []
        self._rowcount = rowcount

    def one(self):
        return self._one

    def first(self):
        return self._first

    def fetchall(self):
        return self._fetchall

    @property
    def rowcount(self):
        return self._rowcount


@pytest.mark.asyncio
async def test_create_task_uses_insert():
    session = AsyncMock()
    session.execute.return_value = FakeResult(one=("task-id",))

    task_id = await repo.create_task(session, qc="OPENQASM 3;", shots=1, max_attempts=3)

    assert task_id == "task-id"
    sql = session.execute.call_args.args[0]
    assert "INSERT INTO tasks" in sql.text


@pytest.mark.asyncio
async def test_get_task_status_returns_row():
    session = AsyncMock()
    session.execute.return_value = FakeResult(first=("completed", {"0": 1}, None))

    status = await repo.get_task_status(session, "task-id")

    assert status == {"status": "completed", "result": {"0": 1}, "last_error": None}
    sql = session.execute.call_args.args[0]
    assert "SELECT status, result, last_error" in sql.text


@pytest.mark.asyncio
async def test_claim_tasks_maps_rows():
    session = AsyncMock()
    session.execute.return_value = FakeResult(
        fetchall=[
            ("id1", "qc", 3, "processing", 1, 3, "w1", "lease", "next", None, None),
        ]
    )

    rows = await repo.claim_tasks(session, worker_id="w1", lease_seconds=10, limit=1)

    assert rows[0].id == "id1"
    assert rows[0].shots == 3
    assert rows[0].attempts == 1


@pytest.mark.asyncio
async def test_mark_failed_or_retry_failed_path():
    session = AsyncMock()
    session.execute.return_value = FakeResult(rowcount=1)

    state = await repo.mark_failed_or_retry(
        session,
        task_id="id",
        worker_id="w1",
        error="boom",
        attempts=3,
        max_attempts=3,
        retry_delay_seconds=5,
        metrics={"duration_ms": 1},
    )

    assert state == "failed"
    sql = session.execute.call_args.args[0]
    assert "SET status='failed'" in sql.text
