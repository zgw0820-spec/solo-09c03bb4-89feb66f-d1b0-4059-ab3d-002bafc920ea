"""Operations web app (操作网页).

Responsibilities:

* Accept fault/recovery events per device with a monotonic per-device seq.
* Persist every event before it is handed to the worker (store-and-forward),
  so a restart loses nothing.
* Expose dispatch status, quarantine list and status log, and proxy the
  receiver's work-order / rejection / device-state views.
* Let the operator fill in the missing team_no and replay a quarantined
  fault.  Replay puts the event back into the worker queue as a new delivery
  attempt with ``is_replay=true``; the receiver keeps the original processing
  record and refuses to let an old fault overwrite a newer recovery.
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from . import db as dbmod
from .schemas import EventIn, ReplayIn

STATIC_DIR = Path(__file__).parent / "static"


def create_app(
    db_path: str | None = None,
    receiver_url: str | None = None,
) -> FastAPI:
    db_path = db_path or os.environ.get("WEB_DB", "/data/web.db")
    receiver_url = (
        receiver_url
        or os.environ.get("RECEIVER_URL", "http://receiver:8000")
    ).rstrip("/")
    dbmod.init_db(db_path)

    app = FastAPI(title="检修事件投递操作台", version="1.0.0")
    app.state.db_path = db_path
    app.state.receiver_url = receiver_url

    def get_conn() -> sqlite3.Connection:
        return dbmod.connect(app.state.db_path)

    # ------------------------------------------------------------------ events
    @app.post("/api/events", status_code=201)
    def submit_event(event_in: EventIn) -> dict[str, Any]:
        conn = get_conn()
        try:
            now = dbmod.utcnow()

            def _submit() -> dict[str, Any]:
                with dbmod.transaction(conn):
                    # Per-device monotonic seq.  The UNIQUE(device_id, seq)
                    # constraint plus this transaction make concurrent submits
                    # safe.
                    row = conn.execute(
                        "SELECT COALESCE(MAX(seq), 0) AS max_seq "
                        "FROM events WHERE device_id = ?",
                        (event_in.device_id,),
                    ).fetchone()
                    seq = int(row["max_seq"]) + 1
                    event_key = f"{event_in.device_id}:{seq}"
                    payload = {
                        "event_key": event_key,
                        "device_id": event_in.device_id,
                        "seq": seq,
                        "event_type": event_in.event_type,
                        "team_no": event_in.team_no,
                        "description": event_in.description,
                        "is_replay": False,
                        "occurred_at": now,
                    }
                    cur = conn.execute(
                        """INSERT INTO events
                               (event_key, device_id, seq, event_type, payload,
                                status, attempts, next_attempt_at,
                                created_at, updated_at)
                           VALUES (?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)""",
                        (
                            event_key, event_in.device_id, seq,
                            event_in.event_type,
                            json.dumps(payload, ensure_ascii=False),
                            now, now, now,
                        ),
                    )
                    event_id = cur.lastrowid
                    dbmod.log_status(conn, event_id, event_key, None,
                                     "pending", "事件已提交，等待工作器投递")
                    return {"event_key": event_key, "seq": seq,
                            "status": "pending"}

            return dbmod.with_write_retry(_submit)
        finally:
            conn.close()

    @app.get("/api/events")
    def list_events() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM events ORDER BY id ASC"
            ).fetchall()
            return {"events": [dbmod.row_to_dict(r) for r in rows]}
        finally:
            conn.close()

    @app.get("/api/events/{event_key}")
    def get_event(event_key: str) -> dict[str, Any]:
        conn = get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM events WHERE event_key = ?", (event_key,)
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="事件不存在")
            return dbmod.row_to_dict(row)  # type: ignore[return-value]
        finally:
            conn.close()

    # ------------------------------------------------------------- quarantine
    @app.get("/api/quarantine")
    def list_quarantine() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM quarantine ORDER BY id ASC"
            ).fetchall()
            out = []
            for r in rows:
                item = dict(r)
                try:
                    item["payload"] = json.loads(item["payload"])
                except (TypeError, ValueError):
                    pass
                out.append(item)
            return {"quarantine": out}
        finally:
            conn.close()

    @app.post("/api/quarantine/{event_key}/replay")
    def replay_event(event_key: str, replay_in: ReplayIn) -> dict[str, Any]:
        conn = get_conn()
        try:
            now = dbmod.utcnow()
            with dbmod.transaction(conn):
                event = conn.execute(
                    "SELECT * FROM events WHERE event_key = ?", (event_key,)
                ).fetchone()
                if event is None:
                    raise HTTPException(status_code=404, detail="隔离事件不存在")
                q = conn.execute(
                    "SELECT * FROM quarantine WHERE event_id = ?",
                    (event["id"],),
                ).fetchone()
                if q is None:
                    raise HTTPException(status_code=400,
                                        detail="该事件不在隔离区，无需重放")
                if q["replay_status"] == "replayed":
                    raise HTTPException(
                        status_code=409,
                        detail="该隔离事件已重放，不能重复重放",
                    )

                payload = json.loads(event["payload"])
                payload["team_no"] = replay_in.team_no
                payload["is_replay"] = True

                # Re-queue: a fresh delivery attempt through the same worker
                # path.  The event row's identity (event_key) is unchanged so
                # the receiver's UNIQUE(event_key) keeps dispatch idempotent.
                conn.execute(
                    """UPDATE events
                       SET payload = ?, status = 'pending', attempts = 0,
                           last_error = NULL, last_response = NULL,
                           next_attempt_at = ?, claimed_at = NULL, updated_at = ?
                     WHERE id = ?""",
                    (json.dumps(payload, ensure_ascii=False), now, now,
                     event["id"]),
                )
                conn.execute(
                    """UPDATE quarantine
                       SET replay_status = 'replayed', replayed_at = ?
                     WHERE event_id = ?""",
                    (now, event["id"]),
                )
                dbmod.log_status(conn, event["id"], event_key,
                                 event["status"], "pending",
                                 f"值班员补全班组号 {replay_in.team_no} 后重放")
            return {"event_key": event_key, "status": "pending",
                    "message": "已补全班组号并重新入队，等待工作器投递"}
        finally:
            conn.close()

    # ------------------------------------------------------------------ audit
    @app.get("/api/status-log")
    def status_log() -> dict[str, Any]:
        conn = get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM status_log ORDER BY id ASC"
            ).fetchall()
            return {"status_log": [dict(r) for r in rows]}
        finally:
            conn.close()

    # ----------------------------------------------------- receiver proxies
    def _proxy(path: str) -> JSONResponse:
        try:
            resp = httpx.get(f"{app.state.receiver_url}{path}", timeout=10.0)
            return JSONResponse(status_code=resp.status_code,
                                content=resp.json())
        except httpx.HTTPError as exc:
            return JSONResponse(
                status_code=502,
                content={"detail": f"接收端暂不可用: {type(exc).__name__}"},
            )

    @app.get("/api/receiver/work-orders")
    def proxy_work_orders() -> JSONResponse:
        return _proxy("/api/work-orders")

    @app.get("/api/receiver/rejections")
    def proxy_rejections() -> JSONResponse:
        return _proxy("/api/rejections")

    @app.get("/api/receiver/device-state")
    def proxy_device_state() -> JSONResponse:
        return _proxy("/api/device-state")

    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    app.mount("/", StaticFiles(directory=str(STATIC_DIR), html=True),
              name="static")
    return app


app = create_app()
