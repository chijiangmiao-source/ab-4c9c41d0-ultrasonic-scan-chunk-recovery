from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_content(size: int, seed: int = 1234) -> bytes:
    rng = random.Random(seed)
    return rng.randbytes(size)


def split_chunks(content: bytes, chunk_size: int) -> list[bytes]:
    return [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]


def future_iso(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def create_session(
    client: TestClient,
    content: bytes,
    chunk_size: int,
    *,
    expires_in: float = 3600,
    file_sha256: str | None = None,
):
    return client.post(
        "/sessions",
        json={
            "file_size": len(content),
            "chunk_size": chunk_size,
            "file_sha256": file_sha256 or sha256_hex(content),
            "expires_at": future_iso(expires_in),
        },
    )


def put_chunk(
    client: TestClient,
    session_id: str,
    index: int,
    body: bytes,
    *,
    digest: str | None = None,
    send_digest: bool = True,
):
    headers = {}
    if send_digest:
        headers["X-Chunk-SHA256"] = digest if digest is not None else sha256_hex(body)
    return client.put(
        f"/sessions/{session_id}/chunks/{index}",
        content=body,
        headers=headers,
    )


@pytest.fixture()
def settings(tmp_path) -> Settings:
    return Settings(data_dir=tmp_path / "data")


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as test_client:
        yield test_client
