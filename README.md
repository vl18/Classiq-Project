# API for executing Quantum Circuits - Classiq home exercise

This solution implements the required API for **asynchronous quantum circuit execution** with a strong focus on:
- **Task integrity** (no task loss, crash-safe)
- **Async processing architecture**
- **CPU-core-sized multiprocessing**
- **Pydantic validation**
- **PostgreSQL-backed durable queue**
- **Hybrid wake-up mechanism using LISTEN/NOTIFY + fallback polling**
- **Rotating logs + performance-oriented structured logging**

## Architecture

**Services (docker-compose):**
- `api` (FastAPI): accepts tasks, writes to Postgres, emits `NOTIFY`
- `worker` (Python asyncio): claims tasks from Postgres and executes them in a process pool
- `db` (Postgres): durable task store + queue state machine

**Why Postgres as the queue?**
- Single source of truth for durability and state transitions.
- Atomic claims with `SELECT ... FOR UPDATE SKIP LOCKED`.
- Recovery from crashes via **lease** and **stale requeue**.

**Why LISTEN/NOTIFY?**
- Reduces pickup latency and avoids tight polling loops.
- Notifications are not durable; therefore workers also:
  - claim immediately **after each completion**
  - perform a **slow polling fallback**

## API

### POST /tasks
Input: `{ "qc": "<qasm3>", "shots": <optional> }`
Output:
`{ "task_id": "...", "message": "Task submitted successfully." }`

### GET /tasks/{id}
- Completed:
`{ "status": "completed", "result": {"00": 128, "11": 128} }`
- Pending (including processing):
`{ "status": "pending", "message": "Task is still in progress." }`
- Not found:
`{ "status": "error", "message": "Task not found." }`

## Worker concurrency
Worker uses `ProcessPoolExecutor(max_workers=<cpu_cores>)`.
CPU cores are detected in a container-aware way (cpuset/quota; fallback to `os.cpu_count()`).

## Task integrity mechanics

- Tasks are persisted as `pending` before any processing.
- Claiming is atomic; multiple workers won't double-claim.
- Each claimed task gets a **lease** (`lease_expires_at`).
- If a worker dies, tasks stuck in `processing` are moved back to `pending` once lease expires.

## Logging

- JSON structured logs to **stdout** and **rotating file** (`/var/log/app/app.log`).
- Key performance fields:
  - `event`, `task_id`, `worker_id`, `duration_ms`, `attempt`
  - execution metrics: `build_ms`, `exec_ms`, `shots`, `num_qubits`, `depth`
- Periodic `metrics` event includes queue counts and in-memory backlog.

## Configuration (12-factor)
All config comes from environment variables (`.env`), managed via `pydantic-settings`.
Copy `.env.example` to `.env` and edit as needed.

## Run

```bash
cp .env.example .env
docker compose up --build
```

Swagger UI:
- http://localhost:8000/docs

## Tests

Start the stack, then run:
```bash
pytest -q
```

Integration tests (require running services). Set `INTEGRATION=1` to enable them:
```bash
INTEGRATION=1 pytest tests/test_integration.py -q
```

Optional: override API URL (default http://localhost:8000):
```bash
API_BASE_URL=http://localhost:8000 INTEGRATION=1 pytest tests/test_integration.py -q
```
