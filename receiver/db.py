"""SQLite helpers for the maintenance receiver service.

The receiver owns its own database.  Idempotency is enforced by a UNIQUE
constraint on ``event_key``: a redelivered event never creates a second work
order, even if the web/worker layer restarts or replays.
"""
from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterator, TypeVar

T = TypeVar("T")

SCHEMA = """
CREATE TABLE IF NOT EXISTS dispatch_records (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key     TEXT NOT NULL UNIQUE,
    device_id     TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    event_type    TEXT NOT NULL,
    team_no       TEXT,
    payload_json  TEXT NOT NULL,
    work_order_no TEXT NOT NULL,
    is_replay     INTEGER NOT NULL DEFAULT 0,
    received_at   TEXT NOT NULL,
    gateway_no    TEXT,
    is_handover   INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_dispatch_device
    ON dispatch_records (device_id, seq);

CREATE TABLE IF NOT EXISTS device_state (
    device_id        TEXT PRIMARY KEY,
    latest_seq       INTEGER NOT NULL,
    latest_event_key TEXT NOT NULL,
    latest_type      TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reject_records (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key    TEXT NOT NULL,
    device_id    TEXT NOT NULL,
    seq          INTEGER NOT NULL,
    event_type   TEXT NOT NULL,
    reason_code  TEXT NOT NULL,
    reason       TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    rejected_at  TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_reject_event ON reject_records (event_key);

-- 当前采集网关归属（接收端是网关接替的权威记录方）。
-- original/current gateway 均允许为空：设备的消息从未注明网关时，
-- 归属按 NULL（未注明）处理；首条消息即接替首报时原网关无从得知。
CREATE TABLE IF NOT EXISTS device_gateways (
    device_id          TEXT PRIMARY KEY,
    original_gateway   TEXT,
    current_gateway    TEXT,
    handover_count     INTEGER NOT NULL DEFAULT 0,
    handover_event_key TEXT,
    handover_at        TEXT,
    updated_at         TEXT NOT NULL
);

-- 网关接替与旧网关消息拦截的全过程留痕（注册/接替/抑制/拒收）。
CREATE TABLE IF NOT EXISTS gateway_audit (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key         TEXT NOT NULL,
    device_id         TEXT NOT NULL,
    seq               INTEGER NOT NULL,
    gateway_no        TEXT,
    effective_gateway TEXT,
    action            TEXT NOT NULL,
    reason_code       TEXT,
    reason            TEXT,
    created_at        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gwaudit_device
    ON gateway_audit (device_id, id);
"""

# Columns added after the baseline release; SQLite has no "ADD COLUMN IF NOT
# EXISTS", so PRAGMA-table_info guards each migration on pre-existing volumes.
_ADDED_COLUMNS = {
    "dispatch_records": [
        ("gateway_no", "TEXT"),
        ("is_handover", "INTEGER NOT NULL DEFAULT 0"),
    ],
}


def _migrate(conn: sqlite3.Connection) -> None:
    for table, columns in _ADDED_COLUMNS.items():
        existing = {r["name"] for r in conn.execute(
            f"PRAGMA table_info({table})")}
        for name, decl in columns:
            if name not in existing:
                conn.execute(
                    f"ALTER TABLE {table} ADD COLUMN {name} {decl}")


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db(db_path: str | Path) -> None:
    with connect(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate(conn)


def with_write_retry(fn: Callable[[], T],
                     attempts: int = 5, delay: float = 0.2) -> T:
    """Retry on SQLITE_BUSY/LOCKED. WAL allows concurrent readers plus one
    writer; when several requests race to write, the losers wait and retry
    instead of surfacing a 500."""
    last_exc: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            if "locked" in str(exc).lower() or "busy" in str(exc).lower():
                time.sleep(delay * (i + 1))
                continue
            raise
    assert last_exc is not None
    raise last_exc


@contextmanager
def transaction(conn: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """Explicit begin/commit/rollback (do not rely on the connection-level
    context manager, which only commits on clean block exit and would drop
    writes when an exception carries control out of the block)."""
    try:
        conn.execute("BEGIN")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
