"""SQLite persistence for upload sessions, chunk digests and received bitmaps.

A single connection guarded by an RLock is used; every mutating operation runs
inside a transaction so a crash or restart can never leave the chunk table and
the session bitmap/count out of sync. WAL mode allows readers to proceed while
a chunk is being recorded.
"""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id              TEXT PRIMARY KEY,
    file_size       INTEGER NOT NULL,
    chunk_size      INTEGER NOT NULL,
    total_chunks    INTEGER NOT NULL,
    file_sha256     TEXT NOT NULL,
    expires_at      TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'active',
    bitmap          BLOB NOT NULL,
    received_chunks INTEGER NOT NULL DEFAULT 0,
    artifact_path   TEXT,
    error_code      TEXT,
    error_details   TEXT,
    created_at      TEXT NOT NULL,
    completed_at    TEXT
);

CREATE TABLE IF NOT EXISTS chunks (
    session_id  TEXT NOT NULL REFERENCES sessions(id),
    idx         INTEGER NOT NULL,
    sha256      TEXT NOT NULL,
    size        INTEGER NOT NULL,
    path        TEXT NOT NULL,
    received_at TEXT NOT NULL,
    PRIMARY KEY (session_id, idx)
);
"""

STATUS_ACTIVE = "active"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def to_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def from_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


class Database:
    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._lock = threading.RLock()
        with self._lock, self._conn:
            self._conn.executescript(SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # sessions
    # ------------------------------------------------------------------
    def create_session(
        self,
        *,
        session_id: str,
        file_size: int,
        chunk_size: int,
        total_chunks: int,
        file_sha256: str,
        expires_at: datetime,
        bitmap: bytes,
    ) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO sessions
                    (id, file_size, chunk_size, total_chunks, file_sha256,
                     expires_at, status, bitmap, received_chunks, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
                """,
                (
                    session_id,
                    file_size,
                    chunk_size,
                    total_chunks,
                    file_sha256,
                    to_iso(expires_at),
                    STATUS_ACTIVE,
                    bitmap,
                    to_iso(utcnow()),
                ),
            )

    def get_session(self, session_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM sessions WHERE id = ?", (session_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def list_finalizable_sessions(self) -> list[str]:
        """Active sessions whose chunks are all present (crash-recovery scan)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM sessions WHERE status = ? AND received_chunks = total_chunks",
                (STATUS_ACTIVE,),
            ).fetchall()
        return [row["id"] for row in rows]

    def list_completed_sessions(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id FROM sessions WHERE status = ?", (STATUS_COMPLETED,)
            ).fetchall()
        return [row["id"] for row in rows]

    def mark_completed(self, session_id: str, artifact_path: str) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                """
                UPDATE sessions
                   SET status = ?, artifact_path = ?, completed_at = ?
                 WHERE id = ?
                """,
                (STATUS_COMPLETED, artifact_path, to_iso(utcnow()), session_id),
            )

    def mark_failed(self, session_id: str, code: str, details: dict[str, Any]) -> None:
        with self._lock, self._conn:
            self._conn.execute(
                "UPDATE sessions SET status = ?, error_code = ?, error_details = ? WHERE id = ?",
                (STATUS_FAILED, code, json.dumps(details), session_id),
            )

    # ------------------------------------------------------------------
    # chunks
    # ------------------------------------------------------------------
    def get_chunk(self, session_id: str, index: int) -> dict[str, Any] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM chunks WHERE session_id = ? AND idx = ?",
                (session_id, index),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_chunk(
        self,
        *,
        session_id: str,
        index: int,
        sha256: str,
        size: int,
        path: str,
        bitmap: bytes,
    ) -> None:
        """Insert the chunk row and update the session bitmap atomically.

        Raises ``sqlite3.IntegrityError`` if the chunk index already exists
        (concurrent upload won the race); the bitmap update is rolled back
        together with the insert.
        """
        with self._lock, self._conn:
            self._conn.execute(
                """
                INSERT INTO chunks (session_id, idx, sha256, size, path, received_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (session_id, index, sha256, size, path, to_iso(utcnow())),
            )
            self._conn.execute(
                """
                UPDATE sessions
                   SET bitmap = ?, received_chunks = received_chunks + 1
                 WHERE id = ?
                """,
                (bitmap, session_id),
            )

    def delete_chunk(self, session_id: str, index: int, bitmap: bytes) -> None:
        """Compensating delete used only if the on-disk commit fails after insert."""
        with self._lock, self._conn:
            self._conn.execute(
                "DELETE FROM chunks WHERE session_id = ? AND idx = ?",
                (session_id, index),
            )
            self._conn.execute(
                """
                UPDATE sessions
                   SET bitmap = ?, received_chunks = received_chunks - 1
                 WHERE id = ?
                """,
                (bitmap, session_id),
            )
