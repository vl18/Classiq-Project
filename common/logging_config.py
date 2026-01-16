from __future__ import annotations

import json
import logging
import os
import time
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional


class ServiceFilter(logging.Filter):
    def __init__(self, service: str) -> None:
        super().__init__()
        self._service = service

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "service"):
            record.service = self._service
        return True


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base: Dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)),
            "level": record.levelname,
            "logger": record.name,
            "service": getattr(record, "service", None),
            "event": getattr(record, "event", None),
            "msg": record.getMessage(),
            "task_id": getattr(record, "task_id", None),
            "worker_id": getattr(record, "worker_id", None),
            "duration_ms": getattr(record, "duration_ms", None),
            "attempt": getattr(record, "attempt", None),
        }

        extra_fields = getattr(record, "fields", None)
        if isinstance(extra_fields, dict):
            base.update(extra_fields)

        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)

        base = {k: v for k, v in base.items() if v is not None}
        return json.dumps(base, ensure_ascii=False)


def setup_logging(*, service: str, level: str, log_dir: str, max_bytes: int, backup_count: int) -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger()
    logger.setLevel(level)

    if logger.handlers:
        # If something already configured logging, still ensure our service filter exists.
        logger.addFilter(ServiceFilter(service))
        return logger

    formatter = JsonFormatter()
    service_filter = ServiceFilter(service)

    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(formatter)
    sh.addFilter(service_filter)
    logger.addHandler(sh)

    fh = RotatingFileHandler(
        filename=os.path.join(log_dir, "app.log"),
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    fh.setLevel(level)
    fh.setFormatter(formatter)
    fh.addFilter(service_filter)
    logger.addHandler(fh)

    return logger


def log_event(
    logger: logging.Logger,
    *,
    event: str,
    task_id: Optional[str] = None,
    worker_id: Optional[str] = None,
    duration_ms: Optional[int] = None,
    attempt: Optional[int] = None,
    **fields: Any,
) -> None:
    logger.info(
        event,
        extra={
            "event": event,
            "task_id": task_id,
            "worker_id": worker_id,
            "duration_ms": duration_ms,
            "attempt": attempt,
            "fields": fields or None,
        },
    )
