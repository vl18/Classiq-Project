from __future__ import annotations

from typing import Dict, Optional, Union
from pydantic import BaseModel, Field, field_validator


class TaskCreate(BaseModel):
    qc: str = Field(..., description="Serialized quantum circuit in QASM3 format")
    shots: Optional[int] = Field(None, ge=1, le=1_000_000)

    @field_validator("qc")
    @classmethod
    def qc_not_empty(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("qc must be a non-empty string")
        return v


class TaskCreateResponse(BaseModel):
    task_id: str
    message: str


class CompletedResponse(BaseModel):
    status: str = "completed"
    result: Dict[str, int]


class PendingResponse(BaseModel):
    status: str = "pending"
    message: str


class ErrorResponse(BaseModel):
    status: str = "error"
    message: str


TaskStatusResponse = Union[CompletedResponse, PendingResponse, ErrorResponse]
