"""Request/response schemas."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, field_validator

SHA256_HEX_RE = re.compile(r"[0-9a-f]{64}")


def normalize_sha256(value: str, field: str) -> str:
    candidate = value.strip().lower()
    if not SHA256_HEX_RE.fullmatch(candidate):
        raise ValueError(f"{field} must be exactly 64 hexadecimal characters")
    return candidate


class CreateSessionRequest(BaseModel):
    """Parameters the measuring equipment declares before uploading anything."""

    file_size: int = Field(gt=0, description="Total file size in bytes")
    chunk_size: int = Field(gt=0, description="Size of every chunk except the last one")
    file_sha256: str = Field(description="SHA-256 of the whole file, hex encoded")
    expires_at: datetime = Field(
        description="Session expiry as timezone-aware ISO 8601 (or unix seconds)"
    )

    @field_validator("file_sha256")
    @classmethod
    def _validate_file_sha256(cls, value: str) -> str:
        return normalize_sha256(value, "file_sha256")

    @field_validator("expires_at")
    @classmethod
    def _validate_expires_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError(
                "expires_at must include a timezone, e.g. 2026-09-18T12:00:00Z"
            )
        return value.astimezone(timezone.utc)


class ErrorBody(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = {}


class ErrorResponse(BaseModel):
    error: ErrorBody


class ArtifactInfo(BaseModel):
    sha256: str
    size: int
    path: str
    url: str


class CreateSessionResponse(BaseModel):
    session_id: str
    status: str
    file_size: int
    chunk_size: int
    total_chunks: int
    received_chunks: int
    file_sha256: str
    expires_at: datetime
    created_at: datetime


class SessionStatusResponse(BaseModel):
    session_id: str
    status: str
    file_size: int
    chunk_size: int
    total_chunks: int
    received_chunks: int
    missing_chunks: list[int]
    file_sha256: str
    expires_at: datetime
    created_at: datetime
    completed_at: datetime | None
    artifact: ArtifactInfo | None
    error: ErrorBody | None


class ChunkUploadResponse(BaseModel):
    session_id: str
    index: int
    sha256: str
    size: int
    duplicate: bool
    received_chunks: int
    total_chunks: int
    session_status: str
    artifact: ArtifactInfo | None


class FinalizeResponse(BaseModel):
    session_id: str
    status: str
    received_chunks: int
    total_chunks: int
    artifact: ArtifactInfo | None
