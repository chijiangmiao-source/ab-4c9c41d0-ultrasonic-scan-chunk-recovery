"""Business logic: session lifecycle, chunk ingestion, integrity-gated publish."""

from __future__ import annotations

import json
import logging
import sqlite3
import uuid
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any

from . import bitmap as bitmap_mod
from .db import (
    STATUS_ACTIVE,
    STATUS_COMPLETED,
    STATUS_FAILED,
    Database,
    from_iso,
    utcnow,
)
from .errors import ApiError
from .models import (
    ArtifactInfo,
    ChunkUploadResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    ErrorBody,
    FinalizeResponse,
    SessionStatusResponse,
    normalize_sha256,
)
from .storage import Storage

logger = logging.getLogger(__name__)

STATUS_EXPIRED = "expired"  # derived, never stored


class UploadService:
    def __init__(self, db: Database, storage: Storage) -> None:
        self._db = db
        self._storage = storage

    def close(self) -> None:
        self._db.close()

    # ------------------------------------------------------------------
    # session creation
    # ------------------------------------------------------------------
    def create_session(self, request: CreateSessionRequest) -> CreateSessionResponse:
        now = utcnow()
        if request.expires_at <= now:
            raise ApiError(
                422,
                "EXPIRY_NOT_IN_FUTURE",
                "expires_at must be in the future",
                {"expires_at": request.expires_at.isoformat(), "now": now.isoformat()},
            )
        total_chunks = -(-request.file_size // request.chunk_size)  # ceil division
        session_id = uuid.uuid4().hex
        self._db.create_session(
            session_id=session_id,
            file_size=request.file_size,
            chunk_size=request.chunk_size,
            total_chunks=total_chunks,
            file_sha256=request.file_sha256,
            expires_at=request.expires_at,
            bitmap=bytes(bitmap_mod.new_bitmap(total_chunks)),
        )
        session = self._require_session(session_id)
        return CreateSessionResponse(
            session_id=session_id,
            status=STATUS_ACTIVE,
            file_size=session["file_size"],
            chunk_size=session["chunk_size"],
            total_chunks=session["total_chunks"],
            received_chunks=0,
            file_sha256=session["file_sha256"],
            expires_at=from_iso(session["expires_at"]),
            created_at=from_iso(session["created_at"]),
        )

    # ------------------------------------------------------------------
    # status
    # ------------------------------------------------------------------
    def get_status(self, session_id: str) -> SessionStatusResponse:
        return self._status_view(self._require_session(session_id))

    # ------------------------------------------------------------------
    # chunk upload
    # ------------------------------------------------------------------
    async def upload_chunk(
        self,
        session_id: str,
        index: int,
        digest_header: str | None,
        stream: AsyncIterator[bytes],
        content_length: int | None,
    ) -> ChunkUploadResponse:
        session = self._require_session(session_id)
        total_chunks = session["total_chunks"]

        if not 0 <= index < total_chunks:
            raise ApiError(
                422,
                "CHUNK_INDEX_OUT_OF_RANGE",
                f"chunk index {index} is outside [0, {total_chunks})",
                {"index": index, "total_chunks": total_chunks},
            )

        if digest_header is None:
            raise ApiError(
                400,
                "MISSING_CHUNK_DIGEST",
                "header X-Chunk-SHA256 is required",
            )
        try:
            declared_digest = normalize_sha256(digest_header, "X-Chunk-SHA256")
        except ValueError as exc:
            raise ApiError(400, "INVALID_CHUNK_DIGEST", str(exc)) from exc

        # Idempotency / conflict are decided from the persisted digest alone,
        # so they hold even after expiry, failure or completion.
        existing = self._db.get_chunk(session_id, index)
        if existing is not None:
            if existing["sha256"] == declared_digest:
                return self._chunk_view(session, index, existing, duplicate=True)
            raise ApiError(
                409,
                "CHUNK_CONFLICT",
                f"chunk {index} was already received with different content",
                {
                    "index": index,
                    "stored_sha256": existing["sha256"],
                    "provided_sha256": declared_digest,
                },
            )

        if session["status"] == STATUS_FAILED:
            raise self._session_failed_error(session)
        if self._is_expired(session):
            raise ApiError(
                410,
                "SESSION_EXPIRED",
                "session has expired; no new chunks are accepted",
                {"expires_at": session["expires_at"]},
            )

        expected_length = self._expected_chunk_length(session, index)
        if content_length is not None and content_length != expected_length:
            raise ApiError(
                422,
                "CHUNK_LENGTH_MISMATCH",
                f"chunk {index} must be exactly {expected_length} bytes",
                {
                    "index": index,
                    "expected_length": expected_length,
                    "content_length": content_length,
                },
            )

        tmp_path, actual_digest, actual_length = await self._storage.write_chunk_stream(
            session_id, index, stream
        )
        try:
            if actual_length != expected_length:
                raise ApiError(
                    422,
                    "CHUNK_LENGTH_MISMATCH",
                    f"chunk {index} must be exactly {expected_length} bytes",
                    {
                        "index": index,
                        "expected_length": expected_length,
                        "actual_length": actual_length,
                    },
                )
            if actual_digest != declared_digest:
                raise ApiError(
                    422,
                    "CHUNK_DIGEST_MISMATCH",
                    "chunk body does not match the X-Chunk-SHA256 header",
                    {
                        "index": index,
                        "declared_sha256": declared_digest,
                        "actual_sha256": actual_digest,
                    },
                )

            new_bitmap = bitmap_mod.new_bitmap(total_chunks)
            new_bitmap[:] = session["bitmap"]
            bitmap_mod.set_bit(new_bitmap, index)
            final_path = self._storage.chunk_path(session_id, index)
            try:
                # Insert first: a unique-violation means a concurrent upload of
                # the same index won, and its file is still untouched on disk.
                self._db.record_chunk(
                    session_id=session_id,
                    index=index,
                    sha256=actual_digest,
                    size=actual_length,
                    path=str(final_path),
                    bitmap=bytes(new_bitmap),
                )
            except sqlite3.IntegrityError:
                winner = self._db.get_chunk(session_id, index)
                if winner is not None and winner["sha256"] == actual_digest:
                    return self._chunk_view(session, index, winner, duplicate=True)
                raise ApiError(
                    409,
                    "CHUNK_CONFLICT",
                    f"chunk {index} was already received with different content",
                    {
                        "index": index,
                        "stored_sha256": winner["sha256"] if winner else None,
                        "provided_sha256": actual_digest,
                    },
                )
            try:
                self._storage.commit_chunk(tmp_path, session_id, index)
            except OSError:
                # The DB row must not point at a file that does not exist.
                self._db.delete_chunk(session_id, index, bytes(session["bitmap"]))
                raise
        finally:
            self._storage.discard(tmp_path)

        session = self._require_session(session_id)
        if session["received_chunks"] == session["total_chunks"]:
            # Last missing chunk arrived: assemble, verify and publish, or fail
            # the session with a structured integrity error.
            artifact = self.finalize(session_id).artifact
            session = self._require_session(session_id)
        else:
            artifact = None
        return ChunkUploadResponse(
            session_id=session_id,
            index=index,
            sha256=actual_digest,
            size=actual_length,
            duplicate=False,
            received_chunks=session["received_chunks"],
            total_chunks=session["total_chunks"],
            session_status=self._effective_status(session),
            artifact=artifact,
        )

    # ------------------------------------------------------------------
    # finalize / publish
    # ------------------------------------------------------------------
    def finalize(self, session_id: str) -> FinalizeResponse:
        session = self._require_session(session_id)

        if session["status"] == STATUS_COMPLETED:
            return FinalizeResponse(
                session_id=session_id,
                status=STATUS_COMPLETED,
                received_chunks=session["received_chunks"],
                total_chunks=session["total_chunks"],
                artifact=self._artifact_info(session),
            )
        if session["status"] == STATUS_FAILED:
            raise self._session_failed_error(session)

        missing = bitmap_mod.missing_indices(session["bitmap"], session["total_chunks"])
        if missing:
            if self._is_expired(session):
                raise ApiError(
                    410,
                    "SESSION_EXPIRED",
                    "session has expired; no new chunks are accepted",
                    {"expires_at": session["expires_at"]},
                )
            raise ApiError(
                409,
                "CHUNKS_INCOMPLETE",
                f"{len(missing)} chunk(s) are still missing",
                {"missing_chunks": missing, "missing_count": len(missing)},
            )

        # All chunks confirmed on disk: assemble in order and verify the
        # whole-file digest before anything is published.
        tmp_path, actual_digest, size = self._storage.assemble_to_temp(
            session_id, session["total_chunks"]
        )
        if actual_digest != session["file_sha256"] or size != session["file_size"]:
            self._storage.discard(tmp_path)
            details = {
                "expected_sha256": session["file_sha256"],
                "actual_sha256": actual_digest,
                "expected_size": session["file_size"],
                "actual_size": size,
                "message": "assembled file does not match the declared SHA-256; "
                "the session is failed and its chunks are immutable",
            }
            self._db.mark_failed(session_id, "INTEGRITY_MISMATCH", details)
            raise ApiError(
                422,
                "INTEGRITY_MISMATCH",
                details["message"],
                {k: v for k, v in details.items() if k != "message"},
            )

        artifact_path = self._storage.publish_artifact(tmp_path, session_id)
        self._db.mark_completed(session_id, str(artifact_path))
        # Chunk bodies are no longer needed; digests stay in SQLite so
        # idempotent re-uploads and conflict detection keep working.
        self._storage.discard_chunk_bodies(session_id)
        logger.info("session %s completed, artifact published at %s", session_id, artifact_path)

        session = self._require_session(session_id)
        return FinalizeResponse(
            session_id=session_id,
            status=STATUS_COMPLETED,
            received_chunks=session["received_chunks"],
            total_chunks=session["total_chunks"],
            artifact=self._artifact_info(session),
        )

    # ------------------------------------------------------------------
    # artifact download
    # ------------------------------------------------------------------
    def artifact_file(self, session_id: str) -> tuple[str, str]:
        """Return (path, sha256) of the published artifact for download."""
        session = self._require_session(session_id)
        if session["status"] != STATUS_COMPLETED or not session["artifact_path"]:
            raise ApiError(
                409,
                "ARTIFACT_NOT_READY",
                "artifact is only available after the session completed",
                {"status": self._effective_status(session)},
            )
        return session["artifact_path"], session["file_sha256"]

    # ------------------------------------------------------------------
    # startup recovery
    # ------------------------------------------------------------------
    def recover_after_restart(self) -> None:
        """Resume interrupted work after an API restart.

        Sessions whose chunks were fully confirmed but which crashed before
        publishing are finalized now; leftover temp files are swept; chunk
        bodies of completed sessions that crashed mid-cleanup are removed.
        """
        self._storage.sweep_temp_files()
        for session_id in self._db.list_finalizable_sessions():
            try:
                self.finalize(session_id)
                logger.info("recovered session %s after restart", session_id)
            except ApiError as exc:
                logger.warning(
                    "recovery of session %s failed: %s %s", session_id, exc.code, exc.message
                )
        for session_id in self._db.list_completed_sessions():
            self._storage.discard_chunk_bodies(session_id)

    # ------------------------------------------------------------------
    # views / helpers
    # ------------------------------------------------------------------
    def _require_session(self, session_id: str) -> dict[str, Any]:
        session = self._db.get_session(session_id)
        if session is None:
            raise ApiError(
                404,
                "SESSION_NOT_FOUND",
                f"no upload session with id {session_id}",
                {"session_id": session_id},
            )
        return session

    @staticmethod
    def _is_expired(session: dict[str, Any]) -> bool:
        return from_iso(session["expires_at"]) <= utcnow()

    def _effective_status(self, session: dict[str, Any]) -> str:
        if session["status"] == STATUS_ACTIVE and self._is_expired(session):
            return STATUS_EXPIRED
        return session["status"]

    @staticmethod
    def _expected_chunk_length(session: dict[str, Any], index: int) -> int:
        if index < session["total_chunks"] - 1:
            return session["chunk_size"]
        return session["file_size"] - session["chunk_size"] * (session["total_chunks"] - 1)

    def _artifact_info(self, session: dict[str, Any]) -> ArtifactInfo:
        return ArtifactInfo(
            sha256=session["file_sha256"],
            size=session["file_size"],
            path=session["artifact_path"],
            url=f"/sessions/{session['id']}/artifact",
        )

    def _status_view(self, session: dict[str, Any]) -> SessionStatusResponse:
        error = None
        if session["status"] == STATUS_FAILED and session["error_code"]:
            details = json.loads(session["error_details"] or "{}")
            message = details.pop("message", session["error_code"])
            error = ErrorBody(code=session["error_code"], message=message, details=details)
        artifact = (
            self._artifact_info(session) if session["status"] == STATUS_COMPLETED else None
        )
        return SessionStatusResponse(
            session_id=session["id"],
            status=self._effective_status(session),
            file_size=session["file_size"],
            chunk_size=session["chunk_size"],
            total_chunks=session["total_chunks"],
            received_chunks=session["received_chunks"],
            missing_chunks=bitmap_mod.missing_indices(
                session["bitmap"], session["total_chunks"]
            ),
            file_sha256=session["file_sha256"],
            expires_at=from_iso(session["expires_at"]),
            created_at=from_iso(session["created_at"]),
            completed_at=(
                from_iso(session["completed_at"]) if session["completed_at"] else None
            ),
            artifact=artifact,
            error=error,
        )

    def _chunk_view(
        self,
        session: dict[str, Any],
        index: int,
        chunk: dict[str, Any],
        *,
        duplicate: bool,
    ) -> ChunkUploadResponse:
        artifact = (
            self._artifact_info(session) if session["status"] == STATUS_COMPLETED else None
        )
        return ChunkUploadResponse(
            session_id=session["id"],
            index=index,
            sha256=chunk["sha256"],
            size=chunk["size"],
            duplicate=duplicate,
            received_chunks=session["received_chunks"],
            total_chunks=session["total_chunks"],
            session_status=self._effective_status(session),
            artifact=artifact,
        )

    @staticmethod
    def _session_failed_error(session: dict[str, Any]) -> ApiError:
        details = json.loads(session["error_details"] or "{}")
        return ApiError(
            409,
            "SESSION_FAILED",
            "session has permanently failed; create a new session to retry the file",
            {"failure": {"code": session["error_code"], "details": details}},
        )
