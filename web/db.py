"""SQLite helpers for the operations web app and the consumer worker.

The web process and the worker process share one SQLite database file.
WAL journal mode allows a reader/writer (web) and a writer (worker) to run
concurrently across processes, and ``busy_timeout`` makes lock contention wait
instead of raising immediately.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, TypeVar

T = TypeVar("T")

# A process-local lock that serialises write transactions inside this process.
# Cross-process coordination is handled by SQLite's own file locking + WAL.
_write_lock = threading.RLock()

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key       TEXT NOT NULL UNIQUE,
    device_id       TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    event_type      TEXT NOT NULL CHECK (event_type IN ('fault', 'recovery')),
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_response   TEXT,
    next_attempt_at TEXT,
    claimed_at      TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    gateway_no      TEXT,
    is_handover     INTEGER NOT NULL DEFAULT 0,
    UNIQUE (device_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_events_dispatch
    ON events (status, next_attempt_at);

CREATE TABLE IF NOT EXISTS quarantine (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        INTEGER NOT NULL UNIQUE REFERENCES events(id),
    event_key       TEXT NOT NULL,
    device_id       TEXT NOT NULL,
    seq             INTEGER NOT NULL,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    reason          TEXT NOT NULL,
    quarantined_at  TEXT NOT NULL,
    replay_status   TEXT NOT NULL DEFAULT 'quarantined',
    replayed_at     TEXT
);

CREATE TABLE IF NOT EXISTS status_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id    INTEGER NOT NULL REFERENCES events(id),
    event_key   TEXT NOT NULL,
    from_status TEXT,
    to_status   TEXT NOT NULL,
    detail      TEXT,
    created_at  TEXT NOT NULL
);
"""

# Post-baseline column migrations for pre-existing SQLite volumes.
_ADDED_COLUMNS = {
    "events": [
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
    # Microsecond precision keeps sub-second backoff comparisons correct.
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
    """Retry on SQLITE_BUSY/LOCKED so concurrent web/worker writers degrade
    to short waits instead of errors."""
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
    """Serialise writers in-process; SQLite serialises across processes.

    Explicit begin/commit/rollback rather than the connection-level context
    manager, so writes are definitely committed before any exception escapes.
    """
    with _write_lock:
        try:
            conn.execute("BEGIN")
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def row_to_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out = dict(row)
    if "payload" in out and isinstance(out["payload"], str):
        try:
            out["payload"] = json.loads(out["payload"])
        except (TypeError, ValueError):
            pass
    if "last_response" in out and isinstance(out["last_response"], str):
        try:
            out["last_response"] = json.loads(out["last_response"])
        except (TypeError, ValueError):
            pass
    return out


def log_status(
    conn: sqlite3.Connection,
    event_id: int,
    event_key: str,
    from_status: str | None,
    to_status: str,
    detail: str | None = None,
) -> None:
    conn.execute(
        """INSERT INTO status_log
               (event_id, event_key, from_status, to_status, detail, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (event_id, event_key, from_status, to_status, detail, utcnow()),
    )
