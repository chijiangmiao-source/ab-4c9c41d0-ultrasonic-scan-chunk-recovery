"""Restart resilience: resume from confirmed position, crash recovery, races."""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
import uvicorn
from fastapi.testclient import TestClient

from app.main import create_app
from tests.conftest import (
    create_session,
    make_content,
    put_chunk,
    sha256_hex,
    split_chunks,
)


def test_restart_resumes_from_confirmed_chunks(settings):
    content = make_content(1000)
    chunks = split_chunks(content, 256)

    app1 = create_app(settings)
    with TestClient(app1) as client1:
        session_id = create_session(client1, content, 256).json()["session_id"]
        assert put_chunk(client1, session_id, 0, chunks[0]).status_code == 200
        assert put_chunk(client1, session_id, 2, chunks[2]).status_code == 200

    # "Restart": a brand-new app instance over the same data directory.
    app2 = create_app(settings)
    with TestClient(app2) as client2:
        status = client2.get(f"/sessions/{session_id}").json()
        assert status["status"] == "active"
        assert status["received_chunks"] == 2
        assert status["missing_chunks"] == [1, 3]

        # Already-confirmed chunks stay idempotent across the restart.
        duplicate = put_chunk(client2, session_id, 0, chunks[0])
        assert duplicate.status_code == 200
        assert duplicate.json()["duplicate"] is True

        # A differing retransmission still conflicts with the persisted digest.
        conflict = put_chunk(client2, session_id, 0, make_content(256, seed=42))
        assert conflict.status_code == 409

        # Finish the upload: the artifact must be exactly the declared file.
        assert put_chunk(client2, session_id, 1, chunks[1]).status_code == 200
        final = put_chunk(client2, session_id, 3, chunks[3])
        assert final.status_code == 200
        assert final.json()["session_status"] == "completed"
        assert (settings.artifacts_dir / f"{session_id}.bin").read_bytes() == content


def test_restart_recovers_interrupted_finalize(settings, monkeypatch):
    """Crash after the last chunk is stored but before the artifact is published."""
    content = make_content(600)
    chunks = split_chunks(content, 256)

    app1 = create_app(settings)
    # raise_server_exceptions=False: we want to observe the structured 500
    # the client would see when the publish step crashes.
    with TestClient(app1, raise_server_exceptions=False) as client1:
        session_id = create_session(client1, content, 256).json()["session_id"]
        put_chunk(client1, session_id, 0, chunks[0])
        put_chunk(client1, session_id, 1, chunks[1])

        service = app1.state.service

        def boom(*args, **kwargs):
            raise OSError("simulated crash during publish")

        monkeypatch.setattr(service._storage, "publish_artifact", boom)
        crashed = put_chunk(client1, session_id, 2, chunks[2])
        assert crashed.status_code == 500
        assert crashed.json()["error"]["code"] == "INTERNAL_ERROR"

        # All chunks are confirmed; only the publish step is missing.
        status = client1.get(f"/sessions/{session_id}").json()
        assert status["status"] == "active"
        assert status["missing_chunks"] == []
        assert not (settings.artifacts_dir / f"{session_id}.bin").exists()

    # After the restart the service finishes the pending publish by itself.
    app2 = create_app(settings)
    with TestClient(app2) as client2:
        status = client2.get(f"/sessions/{session_id}").json()
        assert status["status"] == "completed"
        assert status["artifact"]["sha256"] == sha256_hex(content)
        assert (settings.artifacts_dir / f"{session_id}.bin").read_bytes() == content


def test_failed_session_survives_restart(settings):
    content = make_content(600)
    wrong_declared = sha256_hex(make_content(600, seed=777))
    chunks = split_chunks(content, 256)

    app1 = create_app(settings)
    with TestClient(app1) as client1:
        session_id = create_session(
            client1, content, 256, file_sha256=wrong_declared
        ).json()["session_id"]
        for index, chunk in enumerate(chunks):
            put_chunk(client1, session_id, index, chunk)
        assert client1.get(f"/sessions/{session_id}").json()["status"] == "failed"

    app2 = create_app(settings)
    with TestClient(app2) as client2:
        status = client2.get(f"/sessions/{session_id}").json()
        assert status["status"] == "failed"
        assert status["error"]["code"] == "INTEGRITY_MISMATCH"
        # Retransmitting a confirmed chunk is still idempotent after restart.
        assert put_chunk(client2, session_id, 0, chunks[0]).json()["duplicate"] is True


def test_expiry_state_survives_restart(settings):
    content = make_content(1000)
    app1 = create_app(settings)
    with TestClient(app1) as client1:
        session_id = create_session(client1, content, 256, expires_in=0.5).json()[
            "session_id"
        ]
        put_chunk(client1, session_id, 0, split_chunks(content, 256)[0])
    time.sleep(0.7)

    app2 = create_app(settings)
    with TestClient(app2) as client2:
        status = client2.get(f"/sessions/{session_id}").json()
        assert status["status"] == "expired"
        assert status["received_chunks"] == 1
        response = put_chunk(client2, session_id, 1, split_chunks(content, 256)[1])
        assert response.status_code == 410


# ----------------------------------------------------------------------
# Concurrent uploads against a real server
# ----------------------------------------------------------------------
def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture()
def live_server(settings):
    app = create_app(settings)
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started:
        if time.time() > deadline:
            raise RuntimeError("uvicorn did not start")
        time.sleep(0.02)
    try:
        with httpx.Client(base_url=f"http://127.0.0.1:{port}", timeout=30) as client:
            yield client
    finally:
        server.should_exit = True
        thread.join(timeout=10)


def _create_session_http(client: httpx.Client, content: bytes, chunk_size: int) -> str:
    response = client.post(
        "/sessions",
        json={
            "file_size": len(content),
            "chunk_size": chunk_size,
            "file_sha256": sha256_hex(content),
            "expires_at": "2999-01-01T00:00:00Z",
        },
    )
    assert response.status_code == 201, response.text
    return response.json()["session_id"]


def _put_chunk_http(client: httpx.Client, session_id: str, index: int, body: bytes):
    return client.put(
        f"/sessions/{session_id}/chunks/{index}",
        content=body,
        headers={"X-Chunk-SHA256": sha256_hex(body)},
    )


def test_concurrent_identical_retransmission_is_idempotent(live_server):
    content = make_content(1000)
    session_id = _create_session_http(live_server, content, 256)
    chunk = content[0:256]

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(
            pool.map(lambda _: _put_chunk_http(live_server, session_id, 0, chunk), range(6))
        )
    assert all(r.status_code == 200 for r in responses)
    assert sum(1 for r in responses if r.json()["duplicate"] is False) == 1

    status = live_server.get(f"/sessions/{session_id}").json()
    assert status["received_chunks"] == 1
    assert status["missing_chunks"] == [1, 2, 3]


def test_concurrent_conflicting_chunks_exactly_one_wins(live_server):
    content = make_content(1000)
    session_id = _create_session_http(live_server, content, 256)
    variants = [make_content(256, seed=seed) for seed in range(6)]

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(
            pool.map(
                lambda body: _put_chunk_http(live_server, session_id, 0, body), variants
            )
        )
    codes = sorted(r.status_code for r in responses)
    assert codes == [200, 409, 409, 409, 409, 409]

    # The stored chunk is exactly the winner's content and stays immutable.
    winner_digest = next(
        r.json()["sha256"] for r in responses if r.status_code == 200
    )
    winner_body = next(b for b in variants if sha256_hex(b) == winner_digest)
    assert _put_chunk_http(live_server, session_id, 0, winner_body).status_code == 200
    loser_body = next(b for b in variants if sha256_hex(b) != winner_digest)
    assert _put_chunk_http(live_server, session_id, 0, loser_body).status_code == 409

    status = live_server.get(f"/sessions/{session_id}").json()
    assert status["received_chunks"] == 1


def test_concurrent_distinct_chunks_all_land(live_server):
    content = make_content(1000)
    session_id = _create_session_http(live_server, content, 256)
    chunks = split_chunks(content, 256)

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(
            pool.map(
                lambda item: _put_chunk_http(live_server, session_id, item[0], item[1]),
                list(enumerate(chunks)),
            )
        )
    assert all(r.status_code == 200 for r in responses)
    status = live_server.get(f"/sessions/{session_id}").json()
    assert status["status"] == "completed"
    assert status["missing_chunks"] == []
    downloaded = live_server.get(f"/sessions/{session_id}/artifact")
    assert downloaded.content == content
