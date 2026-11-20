"""Maintenance receiver (检修接收端).

Rules implemented here:

* A ``fault`` event without ``team_no`` is *explicitly rejected* (HTTP 422,
  reason_code ``MISSING_TEAM_NO``).  A ``recovery`` event is still accepted so
  that recovery of the same device is never blocked by a quarantined fault.
* Every event is idempotent on ``event_key`` (device_id + seq): a redelivery
  or replay of an already dispatched event returns the original work order and
  never creates a second dispatch record.
* Replayed/old fault events keep their processing record but must not overwrite
  a newer recovery: the device state only advances to a strictly greater seq.
* Every rejection is persisted in ``reject_records`` so the two-strike
  quarantine decision upstream is auditable on the receiver as well.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .db import connect, init_db, transaction, utcnow, with_write_retry

REASON_MISSING_TEAM = "MISSING_TEAM_NO"


class RejectEvent(Exception):
    """Carries a validated rejection out of the DB transaction.

    Raising FastAPI's HTTPException *inside* the transaction context manager
    would roll the transaction back and lose the persisted reject record, so
    the route raises this exception instead; the registered handler converts
    it to a 422 response after the record has been committed.
    """

    def __init__(self, status_code: int, detail: dict[str, Any]):
        self.status_code = status_code
        self.detail = detail
        super().__init__(detail.get("reason", "rejected"))


class DispatchEvent(BaseModel):
    event_key: str = Field(..., min_length=1)
    device_id: str = Field(..., min_length=1)
    seq: int = Field(..., ge=1)
    event_type: str = Field(..., pattern="^(fault|recovery)$")
    team_no: str | None = None
    description: str | None = None
    is_replay: bool = False
    occurred_at: str | None = None


def create_app(db_path: str | None = None) -> FastAPI:
    db_path = db_path or os.environ.get("RECEIVER_DB", "/data/receiver.db")
    init_db(db_path)
    app = FastAPI(title="检修接收端", version="1.0.0")
    app.state.db_path = db_path

    @app.exception_handler(RejectEvent)
    async def reject_handler(request: Request, exc: RejectEvent) -> JSONResponse:
        return JSONResponse(status_code=exc.status_code,
                            content={"detail": exc.detail})

    @app.exception_handler(sqlite3.OperationalError)
    async def locked_handler(request: Request,
                             exc: sqlite3.OperationalError) -> JSONResponse:
        # Transient write-lock contention: 503 lets the worker back off and
        # retry instead of treating it as an unhandled server error.
        return JSONResponse(
            status_code=503,
            content={"detail": {
                "reason_code": "RECEIVER_BUSY",
                "reason": f"接收端数据库繁忙，请重试：{exc}"}},
        )

    def get_conn() -> sqlite3.Connection:
        return connect(app.state.db_path)

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post("/api/dispatch")
    def dispatch(event: DispatchEvent, request: Request) -> dict[str, Any]:
        expected_key = f"{event.device_id}:{event.seq}"
        if event.event_key != expected_key:
            raise HTTPException(
                status_code=400,
                detail={
                    "reason_code": "BAD_EVENT_KEY",
                    "reason": f"event_key 应为 {expected_key}",
                },
            )

        # Simulated receiver outage for failure-continuation tests.  Triggered
        # only by an opt-in header so normal flows are unaffected.
        if request.headers.get("x-simulate-failure") == "1":
            raise HTTPException(
                status_code=503,
                detail={"reason_code": "SIMULATED_OUTAGE",
                        "reason": "接收端模拟不可用"},
            )

        conn = get_conn()
        try:
            def _dispatch() -> dict[str, Any]:
                with transaction(conn):
                    # 1) Idempotency: already dispatched -> return original order.
                    existing = conn.execute(
                        "SELECT * FROM dispatch_records WHERE event_key = ?",
                        (event.event_key,),
                    ).fetchone()
                    if existing is not None:
                        return {
                            "accepted": True,
                            "duplicate": True,
                            "work_order_no": existing["work_order_no"],
                            "event_key": event.event_key,
                            "message": "事件已派单，幂等返回原工单",
                        }

                    # 2) Explicit validation rejection (fault must carry team_no).
                    #    The reject record is committed inside the transaction;
                    #    the 422 response is raised afterwards so the audit row
                    #    survives.
                    if event.event_type == "fault" and not (event.team_no or "").strip():
                        conn.execute(
                            """INSERT INTO reject_records
                                   (event_key, device_id, seq, event_type,
                                    reason_code, reason, payload_json, rejected_at)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (
                                event.event_key, event.device_id, event.seq,
                                event.event_type, REASON_MISSING_TEAM,
                                "故障事件缺少班组号，接收端拒收",
                                event.model_dump_json(), utcnow(),
                            ),
                        )
                        reject_detail = {
                            "reason_code": REASON_MISSING_TEAM,
                            "reason": "故障事件缺少班组号，接收端拒收",
                            "event_key": event.event_key,
                        }
                        # Commit now, then signal the rejection outside the block.
                        conn.commit()
                        raise RejectEvent(status_code=422, detail=reject_detail)

                    # 3) Accept and dispatch.
                    work_order_no = f"WO-{event.device_id}-{event.seq:06d}"
                    payload = event.model_dump()
                    conn.execute(
                        """INSERT INTO dispatch_records
                               (event_key, device_id, seq, event_type, team_no,
                                payload_json, work_order_no, is_replay, received_at)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            event.event_key, event.device_id, event.seq,
                            event.event_type, event.team_no,
                            json.dumps(payload, ensure_ascii=False),
                            work_order_no,
                            1 if event.is_replay else 0, utcnow(),
                        ),
                    )

                    # 4) Device state only advances to a strictly newer seq.
                    #    A replayed old fault therefore never overwrites a newer
                    #    recovery (or any newer event) for the same device.
                    state = conn.execute(
                        "SELECT * FROM device_state WHERE device_id = ?",
                        (event.device_id,),
                    ).fetchone()
                    state_updated = False
                    if state is None or event.seq > state["latest_seq"]:
                        conn.execute(
                            """INSERT INTO device_state
                                   (device_id, latest_seq, latest_event_key,
                                    latest_type, updated_at)
                               VALUES (?, ?, ?, ?, ?)
                               ON CONFLICT(device_id) DO UPDATE SET
                                   latest_seq = excluded.latest_seq,
                                   latest_event_key = excluded.latest_event_key,
                                   latest_type = excluded.latest_type,
                                   updated_at = excluded.updated_at""",
                            (
                                event.device_id, event.seq, event.event_key,
                                event.event_type, utcnow(),
                            ),
                        )
                        state_updated = True

                    return {
                        "accepted": True,
                        "duplicate": False,
                        "work_order_no": work_order_no,
                        "event_key": event.event_key,
                        "state_updated": state_updated,
                        "message": "已派单" + ("" if state_updated
                                           else "（旧事件重放，未覆盖较新设备状态）"),
                    }

            # Retry only on write-lock contention; RejectEvent and other
            # exceptions propagate untouched.
            return with_write_retry(_dispatch)
        finally:
            conn.close()

    @app.get("/api/work-orders")
    def list_work_orders() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM dispatch_records ORDER BY id ASC"
            ).fetchall()
            return {"work_orders": [dict(r) for r in rows]}
        finally:
            conn.close()

    @app.get("/api/rejections")
    def list_rejections() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM reject_records ORDER BY id ASC"
            ).fetchall()
            return {"rejections": [dict(r) for r in rows]}
        finally:
            conn.close()

    @app.get("/api/device-state")
    def device_state() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM device_state ORDER BY device_id ASC"
            ).fetchall()
            return {"device_state": [dict(r) for r in rows]}
        finally:
            conn.close()

    return app


app = create_app()
