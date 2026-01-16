from __future__ import annotations

import time
from fastapi import FastAPI, HTTPException
from sqlalchemy.exc import SQLAlchemyError

from common.config import Settings
from common.db import create_engine, session_factory, ping_db
from common.logging_config import setup_logging, log_event
from common.repo import create_task, notify_task, get_task_status
from api.schemas import TaskCreate, TaskCreateResponse, TaskStatusResponse, PendingResponse, ErrorResponse


settings = Settings()
logger = setup_logging(
    service="api",
    level=settings.log_level,
    log_dir=settings.log_dir,
    max_bytes=settings.log_max_bytes,
    backup_count=settings.log_backup_count,
)

engine = create_engine(settings)
SessionLocal = session_factory(engine)

app = FastAPI(title="Classiq Home Exercise API")


@app.on_event("startup")
async def startup() -> None:
    await ping_db(engine)
    log_event(logger, event="startup", api_host=settings.api_host, api_port=settings.api_port)


@app.post("/tasks", response_model=TaskCreateResponse)
async def submit_task(payload: TaskCreate) -> TaskCreateResponse:
    t0 = time.perf_counter()
    qc = payload.qc
    if len(qc.encode("utf-8")) > settings.max_qc_bytes:
        raise HTTPException(status_code=413, detail="QC payload too large")

    shots = payload.shots or settings.default_shots

    async with SessionLocal() as session:
        try:
            task_id = await create_task(session, qc=qc, shots=shots, max_attempts=settings.task_max_attempts)
            await notify_task(session, channel=settings.worker_channel, payload=task_id)
            await session.commit()
        except SQLAlchemyError as e:
            await session.rollback()
            log_event(logger, event="task_create_failed", duration_ms=int((time.perf_counter() - t0) * 1000), error=str(e))
            raise HTTPException(status_code=500, detail="Failed to create task")

    log_event(
        logger,
        event="task_created",
        task_id=task_id,
        duration_ms=int((time.perf_counter() - t0) * 1000),
        qc_bytes=len(qc.encode("utf-8")),
        shots=shots,
    )
    return TaskCreateResponse(task_id=task_id, message="Task submitted successfully.")


@app.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task(task_id: str):
    t0 = time.perf_counter()
    async with SessionLocal() as session:
        row = await get_task_status(session, task_id=task_id)
    if row is None:
        log_event(logger, event="task_not_found", task_id=task_id, duration_ms=int((time.perf_counter() - t0) * 1000))
        # Match exercise's "error" output semantics
        return {"status": "error", "message": "Task not found."}

    status = row["status"]
    if status == "completed":
        log_event(logger, event="task_get_completed", task_id=task_id, duration_ms=int((time.perf_counter() - t0) * 1000))
        return {"status": "completed", "result": row["result"] or {}}

    if status in ("pending", "processing"):
        log_event(logger, event="task_get_pending", task_id=task_id, duration_ms=int((time.perf_counter() - t0) * 1000), status=status)
        return {"status": "pending", "message": "Task is still in progress."}

    # failed
    log_event(logger, event="task_get_failed", task_id=task_id, duration_ms=int((time.perf_counter() - t0) * 1000))
    return {"status": "error", "message": "Task failed."}
