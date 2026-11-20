"""Pydantic request/response models for the operations web app."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class EventIn(BaseModel):
    device_id: str = Field(..., min_length=1, description="设备号")
    event_type: Literal["fault", "recovery"] = Field(..., description="故障/恢复")
    team_no: str | None = Field(None, description="班组号（故障缺失会被接收端拒收）")
    description: str | None = None


class EventOut(BaseModel):
    id: int
    event_key: str
    device_id: str
    seq: int
    event_type: str
    payload: dict[str, Any]
    status: str
    attempts: int
    last_error: str | None = None
    last_response: dict[str, Any] | None = None
    next_attempt_at: str | None = None
    created_at: str
    updated_at: str


class ReplayIn(BaseModel):
    team_no: str = Field(..., min_length=1, description="值班员补全的班组号")
