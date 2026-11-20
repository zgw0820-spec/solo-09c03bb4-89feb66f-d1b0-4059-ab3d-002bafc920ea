"""Consumer worker (消费工作器).

Delivery state machine for each event row:

    pending -> processing -> accepted           (receiver returned 2xx)
                           -> rejected (explicit 4xx, e.g. missing team_no)
                                  -> 2nd explicit rejection -> quarantined
                                  -> otherwise back to pending with backoff
                           -> suppressed (terminal gateway 4xx, e.g. an old-
                                  gateway report after the handover switch;
                                  receiver keeps the audit trail, delivery ends)
                           -> pending (network/5xx failure, backoff; other
                                       eligible events keep being processed)

Reliability guarantees:

* Events are claimed row-by-row with a conditional UPDATE (status/claimed_at),
  so two workers or a worker restarted mid-delivery can never dispatch the same
  event twice through this layer; the receiver additionally enforces
  UNIQUE(event_key).
* Stale ``processing`` rows left by a crashed worker are released on startup
  and after a timeout, then redelivered — the receiver's idempotency makes the
  redelivery safe.
* Two *explicit* rejections move the fault into the quarantine table; the row
  stays there permanently (no queue loss).  Later recovery events for the same
  device are independent rows and are delivered normally.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterator

import httpx

from web import db as dbmod


@contextmanager
def _txn(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Like dbmod.transaction, but retries on SQLITE_BUSY/LOCKED because the
    web process may be writing the shared database at the same moment."""
    for attempt in range(5):
        try:
            with dbmod.transaction(conn):
                yield conn
            return
        except sqlite3.OperationalError as exc:
            if ("locked" in str(exc).lower()
                    or "busy" in str(exc).lower()) and attempt < 4:
                time.sleep(0.2 * (attempt + 1))
                continue
            raise

# Explicit receiver rejection reason codes that count toward quarantine.
EXPLICIT_REJECT_CODES = {"MISSING_TEAM_NO"}
# Terminal gateway rejections: the message has been audited at the receiver
# and delivery must end (no retry, no quarantine). Covers ordinary reports
# arriving from a non-current gateway — including messages already queued or
# replayed out of quarantine — and rejected second handover attempts.
GATEWAY_TERMINAL_CODES = {
    "GATEWAY_NOT_CURRENT",
    "HANDOVER_LIMIT",
    "HANDOVER_GATEWAY_REQUIRED",
    "HANDOVER_SAME_GATEWAY",
}
MAX_EXPLICIT_REJECTS = 2
MAX_BACKOFF_SECONDS = 30.0
STALE_PROCESSING_SECONDS = 60
# Explicit rejections retry quickly so the second strike reaches quarantine
# promptly; transient failures use the exponential backoff below.
EXPLICIT_REJECT_RETRY_SECONDS = 1.0
POLL_INTERVAL = float(os.environ.get("WORKER_POLL_INTERVAL", "1.0"))


def _backoff(attempts: int) -> float:
    return min(MAX_BACKOFF_SECONDS, 0.5 * (2 ** max(0, attempts - 1)))


class Worker:
    def __init__(
        self,
        db_path: str,
        receiver_url: str,
        sender: Callable[[str, dict[str, Any], dict[str, str]],
                         httpx.Response] | None = None,
    ) -> None:
        self.db_path = db_path
        self.receiver_url = receiver_url.rstrip("/")
        dbmod.init_db(self.db_path)
        # Injectable sender lets the self-tests talk to an in-process ASGI app.
        self.sender = sender or self._http_send
        self._stop = False

    # ------------------------------------------------------------------ http
    def _http_send(
        self, url: str, payload: dict[str, Any], headers: dict[str, str]
    ) -> httpx.Response:
        with httpx.Client(timeout=10.0) as client:
            return client.post(url, json=payload, headers=headers)

    def stop(self) -> None:
        self._stop = True

    def _conn(self) -> sqlite3.Connection:
        return dbmod.connect(self.db_path)

    # ------------------------------------------------------------- recovery
    def release_stale(self) -> int:
        """Release processing rows whose claim is older than the stale timeout.

        Also runs once at startup so an ungraceful restart never wedges an
        event in ``processing`` forever.
        """
        cutoff_iso = (
            datetime.now(timezone.utc)
            - timedelta(seconds=STALE_PROCESSING_SECONDS)
        ).isoformat()
        conn = self._conn()
        released = 0
        try:
            with _txn(conn):
                rows = conn.execute(
                    """SELECT id, event_key FROM events
                       WHERE status = 'processing'
                         AND claimed_at IS NOT NULL
                         AND claimed_at < ?""",
                    (cutoff_iso,),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        """UPDATE events
                           SET status = 'pending', claimed_at = NULL,
                               next_attempt_at = ?, updated_at = ?
                         WHERE id = ? AND status = 'processing'""",
                        (dbmod.utcnow(), dbmod.utcnow(), row["id"]),
                    )
                    dbmod.log_status(
                        conn, row["id"], row["event_key"], "processing",
                        "pending", "检测到陈旧处理锁，释放后重新投递（幂等）")
                    released += 1
        finally:
            conn.close()
        return released

    # ---------------------------------------------------------------- claiming
    def requeue_due_rejections(self) -> int:
        """Move due ``rejected`` rows back to ``pending`` for another attempt."""
        now = dbmod.utcnow()
        conn = self._conn()
        count = 0
        try:
            with _txn(conn):
                rows = conn.execute(
                    """SELECT id, event_key FROM events
                       WHERE status = 'rejected'
                         AND (next_attempt_at IS NULL OR next_attempt_at <= ?)""",
                    (now,),
                ).fetchall()
                for row in rows:
                    conn.execute(
                        """UPDATE events
                           SET status = 'pending', updated_at = ?
                         WHERE id = ? AND status = 'rejected'""",
                        (now, row["id"]),
                    )
                    dbmod.log_status(
                        conn, row["id"], row["event_key"], "rejected",
                        "pending", "拒收退避到期，重新投递")
                    count += 1
        finally:
            conn.close()
        return count

    def claim_next(self) -> sqlite3.Row | None:
        """Atomically claim one due event; return None when the queue is empty."""
        conn = self._conn()
        try:
            now = dbmod.utcnow()
            with _txn(conn):
                row = conn.execute(
                    """SELECT * FROM events
                       WHERE status = 'pending'
                         AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                       ORDER BY id ASC
                       LIMIT 1""",
                    (now,),
                ).fetchone()
                if row is None:
                    return None
                cur = conn.execute(
                    """UPDATE events
                       SET status = 'processing', claimed_at = ?, updated_at = ?
                     WHERE id = ? AND status = 'pending'""",
                    (now, now, row["id"]),
                )
                if cur.rowcount != 1:  # lost a race with another worker
                    return None
                dbmod.log_status(conn, row["id"], row["event_key"],
                                 "pending", "processing", "工作器领取事件")
                return conn.execute(
                    "SELECT * FROM events WHERE id = ?", (row["id"],)
                ).fetchone()
        finally:
            conn.close()

    # ------------------------------------------------------------- quarantine
    def _quarantine(self, conn: sqlite3.Connection, row: sqlite3.Row,
                    reason: str, attempts: int) -> None:
        conn.execute(
            """INSERT OR IGNORE INTO quarantine
                   (event_id, event_key, device_id, seq, event_type,
                    payload, reason, quarantined_at, replay_status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'quarantined')""",
            (row["id"], row["event_key"], row["device_id"], row["seq"],
             row["event_type"], row["payload"], reason, dbmod.utcnow()),
        )
        conn.execute(
            """UPDATE events
               SET status = 'quarantined', attempts = ?, next_attempt_at = NULL,
                   claimed_at = NULL, updated_at = ?
             WHERE id = ?""",
            (attempts, dbmod.utcnow(), row["id"]),
        )
        dbmod.log_status(conn, row["id"], row["event_key"], "processing",
                         "quarantined", reason)

    # ------------------------------------------------------------- one event
    def process_event(self, row: sqlite3.Row) -> str:
        """Process one claimed event. Returns the resulting status."""
        conn = self._conn()
        try:
            payload = json.loads(row["payload"])
            headers = {"x-delivery-attempt": str(row["attempts"] + 1)}
            try:
                resp = self.sender(
                    f"{self.receiver_url}/api/dispatch", payload, headers)
            except Exception as exc:
                # Any transport-level or unexpected sender error: requeue with
                # backoff so the worker thread never dies and the event is
                # retried rather than wedged in 'processing'.
                with _txn(conn):
                    return self._apply_retry(
                        conn, row,
                        f"投递异常: {type(exc).__name__}: {exc}")

            now = dbmod.utcnow()
            with _txn(conn):
                # Re-read: the row snapshot may predate an attempts update.
                fresh = conn.execute(
                    "SELECT * FROM events WHERE id = ?", (row["id"],)
                ).fetchone()
                attempts = fresh["attempts"] + 1
                row = fresh

                if 200 <= resp.status_code < 300:
                    body = _safe_json(resp)
                    conn.execute(
                        """UPDATE events
                           SET status = 'accepted', attempts = ?,
                               last_error = NULL, last_response = ?,
                               next_attempt_at = NULL, claimed_at = NULL,
                               updated_at = ?
                         WHERE id = ?""",
                        (attempts, json.dumps(body, ensure_ascii=False),
                         now, row["id"]),
                    )
                    note = "接收端已派单"
                    if isinstance(body, dict) and body.get("duplicate"):
                        note += "（重复投递，幂等返回，未重复派单）"
                    dbmod.log_status(conn, row["id"], row["event_key"],
                                     "processing", "accepted", note)
                    return "accepted"

                # Non-2xx: decide explicit reject vs transient failure.
                body = _safe_json(resp)
                detail = body.get("detail") if isinstance(body, dict) else None
                reason_code = ""
                reason_text = f"HTTP {resp.status_code}"
                if isinstance(detail, dict):
                    reason_code = str(detail.get("reason_code", ""))
                    reason_text = str(detail.get("reason", reason_text))

                if reason_code in GATEWAY_TERMINAL_CODES:
                    # 网关规则终端拒收：接收端已留痕（gateway_audit +
                    # reject_records）。消息在此结束投递，保留全部记录。
                    conn.execute(
                        """UPDATE events
                           SET status = 'suppressed', attempts = ?,
                               last_error = ?, last_response = ?,
                               next_attempt_at = NULL, claimed_at = NULL,
                               updated_at = ?
                         WHERE id = ?""",
                        (attempts, reason_text,
                         json.dumps(body, ensure_ascii=False),
                         now, row["id"]),
                    )
                    dbmod.log_status(
                        conn, row["id"], row["event_key"], "processing",
                        "suppressed",
                        f"网关规则终端拒收（{reason_code}）：{reason_text}；"
                        "已留痕并结束投递")
                    return "suppressed"

                if reason_code in EXPLICIT_REJECT_CODES:
                    # Two explicit rejections -> quarantine (permanent).
                    if attempts >= MAX_EXPLICIT_REJECTS:
                        self._quarantine(
                            conn, fresh,
                            f"连续 {attempts} 次被接收端显式拒收：{reason_text}",
                            attempts)
                        return "quarantined"
                    conn.execute(
                        """UPDATE events
                           SET status = 'rejected', attempts = ?,
                               last_error = ?, last_response = ?,
                               next_attempt_at = ?, claimed_at = NULL,
                               updated_at = ?
                         WHERE id = ?""",
                        (attempts, reason_text,
                         json.dumps(body, ensure_ascii=False),
                         _next_at(attempts, EXPLICIT_REJECT_RETRY_SECONDS),
                         now, row["id"]),
                    )
                    dbmod.log_status(
                        conn, row["id"], row["event_key"], "processing",
                        "rejected",
                        f"第 {attempts} 次显式拒收（{reason_code}），"
                        f"将重试；累计 {MAX_EXPLICIT_REJECTS} 次后隔离")
                    return "rejected"

                # Transient failure (5xx, outage, unexpected 4xx): requeue with
                # backoff; other events remain eligible for processing.
                return self._apply_retry(
                    conn, fresh, f"投递失败 {reason_text}",
                    attempts=attempts, response_body=body)
        finally:
            conn.close()

    def _apply_retry(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        error: str,
        attempts: int | None = None,
        response_body: Any | None = None,
    ) -> str:
        """Requeue an event with backoff. Caller owns the transaction."""
        now = dbmod.utcnow()
        fresh = conn.execute(
            "SELECT * FROM events WHERE id = ?", (row["id"],)
        ).fetchone()
        attempts = fresh["attempts"] + 1 if attempts is None else attempts
        conn.execute(
            """UPDATE events
               SET status = 'pending', attempts = ?, last_error = ?,
                   last_response = ?, next_attempt_at = ?,
                   claimed_at = NULL, updated_at = ?
             WHERE id = ?""",
            (attempts, error,
             json.dumps(response_body, ensure_ascii=False)
             if response_body is not None else None,
             _next_at(attempts), now, row["id"]),
        )
        dbmod.log_status(
            conn, row["id"], row["event_key"], fresh["status"],
            "pending", f"{error}；退避后重试（第 {attempts} 次尝试）")
        return "pending"

    # ------------------------------------------------------------------ loop
    def run_once(self) -> int:
        """Process every currently due event once. Returns event count."""
        self.release_stale()
        self.requeue_due_rejections()
        count = 0
        while not self._stop:
            row = self.claim_next()
            if row is None:
                break
            self.process_event(row)
            count += 1
        return count

    def run_forever(self) -> None:
        self.release_stale()
        while not self._stop:
            processed = self.run_once()
            if processed == 0:
                time.sleep(POLL_INTERVAL)


def _next_at(attempts: int, delay: float | None = None) -> str:
    if delay is None:
        delay = _backoff(attempts)
    return (datetime.now(timezone.utc)
            + timedelta(seconds=delay)).isoformat()


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except ValueError:
        return {"raw": resp.text[:500]}


def main() -> None:
    worker = Worker(
        db_path=os.environ.get("WEB_DB", "/data/web.db"),
        receiver_url=os.environ.get("RECEIVER_URL", "http://receiver:8000"),
    )
    worker.run_forever()


if __name__ == "__main__":
    main()
