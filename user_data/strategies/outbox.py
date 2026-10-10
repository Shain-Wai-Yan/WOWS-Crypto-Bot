import os
import json
import sqlite3
import hashlib
import time
from typing import Optional, List, Dict, Any

class ConflictingEventError(Exception):
    """Raised when an event ID already exists with different payload details."""
    pass

class OutboxManager:
    """
    Thread-safe, durable SQLite outbox for outbound trading bot webhook events.
    Enforces WAL mode, busy timeout, SHA256 payload integrity, and conflict detection.
    """
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or os.getenv("SQLITE_DB_PATH", "outbox.db")
        parent_dir = os.path.dirname(self.db_path)
        if parent_dir and not os.path.exists(parent_dir):
            os.makedirs(parent_dir, exist_ok=True)
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode = WAL;")
        conn.execute("PRAGMA busy_timeout = 5000;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS outbox (
                    event_id TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'PENDING',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    next_retry_at INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at INTEGER NOT NULL,
                    sent_at INTEGER
                );
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_outbox_pending 
                ON outbox (status, next_retry_at);
            """)
            conn.commit()

    def record_fill(self, event_id: str, payload_dict: Dict[str, Any]) -> bool:
        """
        Persists a fill event idempotently.
        If event_id exists with differing payload details, raises ConflictingEventError.
        Returns True if inserted, False if identical event already recorded.
        """
        payload_json = json.dumps(payload_dict, sort_keys=True)
        payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
        now_ms = int(time.time() * 1000)

        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT payload_hash, status FROM outbox WHERE event_id = ?", (event_id,))
            row = cur.fetchone()
            if row:
                if row["payload_hash"] != payload_hash:
                    raise ConflictingEventError(
                        f"Conflict: Event ID {event_id} already exists with different payload hash "
                        f"({row['payload_hash']} vs {payload_hash})"
                    )
                return False  # Identical event already present

            cur.execute("""
                INSERT INTO outbox (event_id, payload, payload_hash, status, attempt_count, next_retry_at, created_at)
                VALUES (?, ?, ?, 'PENDING', 0, 0, ?)
            """, (event_id, payload_json, payload_hash, now_ms))
            conn.commit()
            return True

    def get_pending_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Fetches pending events whose next_retry_at is <= now."""
        now_ms = int(time.time() * 1000)
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT event_id, payload, payload_hash, status, attempt_count, next_retry_at, created_at
                FROM outbox
                WHERE status = 'PENDING' AND next_retry_at <= ?
                ORDER BY created_at ASC
                LIMIT ?
            """, (now_ms, limit))
            return [dict(row) for row in cur.fetchall()]

    def mark_acknowledged(self, event_id: str) -> None:
        """Marks an event successfully acknowledged by the server."""
        now_ms = int(time.time() * 1000)
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE outbox 
                SET status = 'ACKNOWLEDGED', sent_at = ?, last_error = NULL
                WHERE event_id = ?
            """, (now_ms, event_id))
            conn.commit()

    def mark_retry(self, event_id: str, error: str, next_retry_delay_sec: float) -> None:
        """Increments attempt count and schedules next retry with exponential backoff."""
        next_retry_ms = int((time.time() + next_retry_delay_sec) * 1000)
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE outbox 
                SET attempt_count = attempt_count + 1,
                    last_error = ?,
                    next_retry_at = ?
                WHERE event_id = ?
            """, (error[:500], next_retry_ms, event_id))
            conn.commit()

    def mark_quarantined(self, event_id: str, error: str) -> None:
        """Marks an event quarantined after a permanent client error."""
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE outbox 
                SET status = 'QUARANTINED', last_error = ?
                WHERE event_id = ?
            """, (error[:500], event_id))
            conn.commit()

    def get_event_status(self, event_id: str) -> Optional[Dict[str, Any]]:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT event_id, status, attempt_count, last_error, created_at, sent_at FROM outbox WHERE event_id = ?", (event_id,))
            row = cur.fetchone()
            return dict(row) if row else None

    def get_unresolved_count(self) -> int:
        with self._get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT count(*) as count FROM outbox WHERE status IN ('PENDING', 'QUARANTINED')")
            return cur.fetchone()["count"]
