from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # DB
    database_url: str = Field(..., alias="DATABASE_URL")

    # API
    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8000, alias="API_PORT")
    default_shots: int = Field(1024, alias="DEFAULT_SHOTS")
    max_qc_bytes: int = Field(2_000_000, alias="MAX_QC_BYTES")

    # Worker
    worker_channel: str = Field("tasks_channel", alias="WORKER_CHANNEL")
    worker_claim_batch: int = Field(32, alias="WORKER_CLAIM_BATCH")
    worker_lease_seconds: int = Field(120, alias="WORKER_LEASE_SECONDS")
    worker_poll_interval_seconds: int = Field(10, alias="WORKER_POLL_INTERVAL_SECONDS")
    worker_stale_sweep_seconds: int = Field(30, alias="WORKER_STALE_SWEEP_SECONDS")
    task_max_attempts: int = Field(3, alias="TASK_MAX_ATTEMPTS")
    task_retry_base_seconds: int = Field(5, alias="TASK_RETRY_BASE_SECONDS")
    task_retry_max_seconds: int = Field(300, alias="TASK_RETRY_MAX_SECONDS")
    task_exec_timeout_seconds: int = Field(60, alias="TASK_EXEC_TIMEOUT_SECONDS")
    metrics_interval_seconds: int = Field(15, alias="METRICS_INTERVAL_SECONDS")

    # Logging
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_dir: str = Field("/var/log/app", alias="LOG_DIR")
    log_max_bytes: int = Field(10_485_760, alias="LOG_MAX_BYTES")
    log_backup_count: int = Field(5, alias="LOG_BACKUP_COUNT")
