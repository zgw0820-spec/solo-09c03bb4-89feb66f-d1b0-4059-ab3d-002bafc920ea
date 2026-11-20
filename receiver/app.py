"""Maintenance receiver (检修接收端).

Rules implemented here:

* A ``fault`` event without ``team_no`` is *explicitly rejected* (HTTP 422,
  reason_code ``MISSING_TEAM_NO``).  A ``recovery`` event is still accepted so
  that recovery of the same device is never blocked by a quarantined fault.
* Every event is idempotent on ``event_key`` (device_id + seq): a redelivery
  or replay of an already dispatched event returns the original work order and
  never creates a second dispatch record — this also covers a redelivered
  gateway-handover first report, which must produce exactly one work order.
* Replayed/old fault events keep their processing record but must not overwrite
  a newer recovery: the device state only advances to a strictly greater seq.
* Every rejection is persisted in ``reject_records`` so the two-strike
  quarantine decision upstream is auditable on the receiver as well.

Gateway handover (采集网关接替) rules — the receiver is the authority:

* Each device is bound to a *current gateway* (``device_gateways``).  An event
  that carries no ``gateway_no`` is attributed to the device's original
  gateway.
* A handover first report (``is_handover=true`` + new ``gateway_no``) is
  accepted exactly once per device: in the same transaction it dispatches a
  work order, switches the current gateway and refreshes the device state from
  that first report even when the old gateway already owns higher-seq work
  orders.  A second handover for the same device is rejected
  (``HANDOVER_LIMIT``).
* Until the first report is accepted, the old gateway keeps dispatching
  normally: ordinary reports are only checked against the current gateway, so
  nothing is blocked while the handover report is still in flight.
* After the switch, ordinary reports from any other gateway — including
  messages already queued or replayed from quarantine — are *terminally*
  rejected (``GATEWAY_NOT_CURRENT``) with a ``gateway_audit`` trail; the worker
  ends delivery for them instead of retrying.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, model_validator

from .db import connect, init_db, transaction, utcnow, with_write_retry

REASON_MISSING_TEAM = "MISSING_TEAM_NO"

# Terminal gateway reason codes: the worker must stop delivery (suppress the
# message) instead of retrying or moving it into the two-strike quarantine.
REASON_GATEWAY_NOT_CURRENT = "GATEWAY_NOT_CURRENT"
REASON_HANDOVER_LIMIT = "HANDOVER_LIMIT"
REASON_HANDOVER_GATEWAY_REQUIRED = "HANDOVER_GATEWAY_REQUIRED"
REASON_HANDOVER_SAME_GATEWAY = "HANDOVER_SAME_GATEWAY"
GATEWAY_TERMINAL_CODES = {
    REASON_GATEWAY_NOT_CURRENT,
    REASON_HANDOVER_LIMIT,
    REASON_HANDOVER_GATEWAY_REQUIRED,
    REASON_HANDOVER_SAME_GATEWAY,
}


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
    # 采集网关编号：未注明时按设备原网关归属。
    gateway_no: str | None = None
    # 接替首报标记：受理后切换该设备的当前网关（每设备仅限一次）。
    is_handover: bool = False

    @model_validator(mode="after")
    def _normalize(self) -> "DispatchEvent":
        if self.gateway_no is not None:
            self.gateway_no = self.gateway_no.strip() or None
        if self.team_no is not None:
            self.team_no = self.team_no.strip() or None
        return self


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
            def _insert_reject(conn: sqlite3.Connection,
                               ev: DispatchEvent, reason_code: str,
                               reason: str) -> None:
                conn.execute(
                    """INSERT INTO reject_records
                           (event_key, device_id, seq, event_type,
                            reason_code, reason, payload_json, rejected_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ev.event_key, ev.device_id, ev.seq, ev.event_type,
                     reason_code, reason, ev.model_dump_json(), utcnow()),
                )

            def _insert_audit(conn: sqlite3.Connection, ev: DispatchEvent,
                              effective_gateway: str | None, action: str,
                              reason_code: str | None,
                              reason: str | None) -> None:
                conn.execute(
                    """INSERT INTO gateway_audit
                           (event_key, device_id, seq, gateway_no,
                            effective_gateway, action, reason_code, reason,
                            created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (ev.event_key, ev.device_id, ev.seq, ev.gateway_no,
                     effective_gateway, action, reason_code, reason,
                     utcnow()),
                )
            def _terminal_reject(conn: sqlite3.Connection, ev: DispatchEvent,
                                 reason_code: str, reason: str,
                                 effective_gateway: str | None) -> None:
                """Persist reject + gateway audit in one commit, then signal a
                422 *after* the commit so both audit rows survive (see
                RejectEvent)."""
                _insert_reject(conn, ev, reason_code, reason)
                _insert_audit(conn, ev, effective_gateway, "suppressed",
                              reason_code, reason)
                conn.commit()
                raise RejectEvent(status_code=422, detail={
                    "reason_code": reason_code,
                    "reason": reason,
                    "event_key": ev.event_key,
                    "terminal": True,
                })

            def _dispatch() -> dict[str, Any]:
                with transaction(conn):
                    # 1) Idempotency: already dispatched -> return original
                    #    order.  A redelivered handover first report therefore
                    #    creates exactly one work order and never re-switches.
                    existing = conn.execute(
                        "SELECT * FROM dispatch_records WHERE event_key = ?",
                        (event.event_key,),
                    ).fetchone()
                    if existing is not None:
                        gw = conn.execute(
                            "SELECT * FROM device_gateways WHERE device_id = ?",
                            (event.device_id,),
                        ).fetchone()
                        return {
                            "accepted": True,
                            "duplicate": True,
                            "work_order_no": existing["work_order_no"],
                            "event_key": event.event_key,
                            "current_gateway":
                                gw["current_gateway"] if gw else None,
                            "message": "事件已派单，幂等返回原工单",
                        }

                    # 2) Explicit validation rejection (fault must carry
                    #    team_no).  The reject record is committed inside the
                    #    transaction; the 422 response is raised afterwards so
                    #    the audit row survives.
                    if event.event_type == "fault" and not (event.team_no or "").strip():
                        _insert_reject(conn, event, REASON_MISSING_TEAM,
                                       "故障事件缺少班组号，接收端拒收")
                        reject_detail = {
                            "reason_code": REASON_MISSING_TEAM,
                            "reason": "故障事件缺少班组号，接收端拒收",
                            "event_key": event.event_key,
                        }
                        conn.commit()
                        raise RejectEvent(status_code=422,
                                          detail=reject_detail)

                    gw = conn.execute(
                        "SELECT * FROM device_gateways WHERE device_id = ?",
                        (event.device_id,),
                    ).fetchone()

                    handover_accepted = False
                    previous_gateway: str | None = None

                    # 3a) Gateway handover first report.
                    if event.is_handover:
                        if not event.gateway_no:
                            _terminal_reject(
                                conn, event,
                                REASON_HANDOVER_GATEWAY_REQUIRED,
                                "接替首报必须注明新网关编号",
                                event.gateway_no)
                        if gw is not None \
                                and event.gateway_no == gw["current_gateway"]:
                            _terminal_reject(
                                conn, event, REASON_HANDOVER_SAME_GATEWAY,
                                "新网关编号与当前网关相同，无需接替",
                                event.gateway_no)
                        if gw is not None and gw["handover_count"] >= 1:
                            _terminal_reject(
                                conn, event, REASON_HANDOVER_LIMIT,
                                "该设备已完成过一次网关接替，不能再次接替",
                                event.gateway_no)

                        previous_gateway = (
                            gw["current_gateway"] if gw is not None else None)
                        original_gateway = (
                            gw["original_gateway"] if gw is not None
                            else event.gateway_no)
                        now = utcnow()
                        if gw is None:
                            # First sighting of the device is itself a
                            # handover: register directly on the new gateway;
                            # the unknown old gateway stays NULL.
                            conn.execute(
                                """INSERT INTO device_gateways
                                       (device_id, original_gateway,
                                        current_gateway, handover_count,
                                        handover_event_key, handover_at,
                                        updated_at)
                                   VALUES (?, ?, ?, 1, ?, ?, ?)""",
                                (event.device_id, None, event.gateway_no,
                                 event.event_key, now, now))
                        else:
                            conn.execute(
                                """UPDATE device_gateways
                                      SET current_gateway = ?,
                                          handover_count = handover_count + 1,
                                          handover_event_key = ?,
                                          handover_at = ?, updated_at = ?
                                    WHERE device_id = ?""",
                                (event.gateway_no, event.event_key, now, now,
                                 event.device_id))
                        _insert_audit(
                            conn, event, event.gateway_no, "handover", None,
                            f"网关接替生效：{previous_gateway or '（无）'}"
                            f" -> {event.gateway_no}")
                        handover_accepted = True
                    else:
                        # 3b) Ordinary report.
                        if gw is None:
                            # Auto-register the device: attributed gateway
                            # (explicit gateway_no, else NULL/未注明) becomes
                            # both the original and the current gateway.
                            effective = event.gateway_no
                            conn.execute(
                                """INSERT INTO device_gateways
                                       (device_id, original_gateway,
                                        current_gateway, handover_count,
                                        updated_at)
                                   VALUES (?, ?, ?, 0, ?)""",
                                (event.device_id, effective, effective,
                                 utcnow()))
                            _insert_audit(conn, event, effective, "register",
                                          None,
                                          "设备首次上报，登记当前网关"
                                          + (f"：{effective}" if effective
                                             else "（未注明网关）"))
                        else:
                            # 未注明网关编号的上报归原网关。
                            effective = event.gateway_no or gw["original_gateway"]
                            if effective != gw["current_gateway"]:
                                # 切换后旧网关（或任何非当前网关）的消息：
                                # 留痕并终止投递，不再重试/隔离。
                                _terminal_reject(
                                    conn, event, REASON_GATEWAY_NOT_CURRENT,
                                    f"上报归属网关 {effective or '（未注明）'}"
                                    f" 不是设备当前网关 {gw['current_gateway']}，"
                                    "终止投递并留痕",
                                    effective)

                    # 4) Accept and dispatch (same transaction as the switch).
                    work_order_no = f"WO-{event.device_id}-{event.seq:06d}"
                    payload = event.model_dump()
                    conn.execute(
                        """INSERT INTO dispatch_records
                               (event_key, device_id, seq, event_type, team_no,
                                payload_json, work_order_no, is_replay,
                                received_at, gateway_no, is_handover)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            event.event_key, event.device_id, event.seq,
                            event.event_type, event.team_no,
                            json.dumps(payload, ensure_ascii=False),
                            work_order_no,
                            1 if event.is_replay else 0, utcnow(),
                            event.gateway_no,
                            1 if event.is_handover else 0,
                        ),
                    )

                    # 5) Device state.
                    #    * Handover first report: refresh state from this
                    #      report unconditionally — even when the old gateway
                    #      already owns higher-seq work orders.
                    #    * Ordinary reports: only advance to a strictly newer
                    #      seq, so a replayed old fault never overwrites a
                    #      newer recovery.
                    state = conn.execute(
                        "SELECT * FROM device_state WHERE device_id = ?",
                        (event.device_id,),
                    ).fetchone()
                    state_updated = False
                    if handover_accepted or state is None \
                            or event.seq > state["latest_seq"]:
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

                    gw_after = conn.execute(
                        "SELECT * FROM device_gateways WHERE device_id = ?",
                        (event.device_id,),
                    ).fetchone()
                    message = "已派单"
                    if handover_accepted:
                        message += (
                            f"；网关接替生效，当前网关切换为 "
                            f"{gw_after['current_gateway']}")
                    elif not state_updated:
                        message += "（旧事件重放，未覆盖较新设备状态）"
                    return {
                        "accepted": True,
                        "duplicate": False,
                        "work_order_no": work_order_no,
                        "event_key": event.event_key,
                        "state_updated": state_updated,
                        "handover": handover_accepted,
                        "previous_gateway": previous_gateway,
                        "current_gateway": gw_after["current_gateway"],
                        "original_gateway": gw_after["original_gateway"],
                        "message": message,
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

    @app.get("/api/gateways")
    def list_gateways() -> dict[str, Any]:
        """设备当前/原网关与接替结果（持久化，重启后可查实）。"""
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM device_gateways ORDER BY device_id ASC"
            ).fetchall()
            return {"gateways": [dict(r) for r in rows]}
        finally:
            conn.close()

    @app.get("/api/gateway-audit")
    def list_gateway_audit() -> dict[str, Any]:
        """网关登记/接替/拦截全过程留痕。"""
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM gateway_audit ORDER BY id ASC"
            ).fetchall()
            return {"gateway_audit": [dict(r) for r in rows]}
        finally:
            conn.close()

    return app


app = create_app()
