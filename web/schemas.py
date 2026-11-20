"""Pydantic request/response models for the operations web app."""
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator


class EventIn(BaseModel):
    device_id: str = Field(..., min_length=1, description="设备号")
    event_type: Literal["fault", "recovery"] = Field(..., description="故障/恢复")
    team_no: str | None = Field(None, description="班组号（故障缺失会被接收端拒收）")
    description: str | None = None
    gateway_no: str | None = Field(
        None, description="采集网关编号；未注明的上报归原网关")
    is_handover: bool = Field(
        False, description="是否以本条上报发起网关接替（每设备限一次）")

    @model_validator(mode="after")
    def _normalize(self) -> "EventIn":
        if self.gateway_no is not None:
            self.gateway_no = self.gateway_no.strip() or None
        if self.team_no is not None:
            self.team_no = self.team_no.strip() or None
        return self


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
    gateway_no: str | None = None
    is_handover: bool = False


class ReplayIn(BaseModel):
    team_no: str = Field(..., min_length=1, description="值班员补全的班组号")
