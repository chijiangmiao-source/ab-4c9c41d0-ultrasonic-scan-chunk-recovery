"""Session creation rules and status reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tests.conftest import create_session, future_iso, make_content, put_chunk, sha256_hex


def test_create_session_computes_chunk_plan(client):
    content = make_content(1000)
    response = create_session(client, content, chunk_size=256)
    assert response.status_code == 201, response.text
    body = response.json()
    # ceil(1000 / 256) == 4
    assert body["total_chunks"] == 4
    assert body["file_size"] == 1000
    assert body["chunk_size"] == 256
    assert body["received_chunks"] == 0
    assert body["status"] == "active"
    assert body["file_sha256"] == sha256_hex(content)
    assert body["session_id"]


def test_create_session_single_chunk_when_file_smaller_than_chunk(client):
    content = make_content(100)
    response = create_session(client, content, chunk_size=256)
    assert response.status_code == 201
    assert response.json()["total_chunks"] == 1


def test_create_session_normalizes_uppercase_sha256(client):
    content = make_content(64)
    digest = sha256_hex(content)
    response = client.post(
        "/sessions",
        json={
            "file_size": len(content),
            "chunk_size": 64,
            "file_sha256": digest.upper(),
            "expires_at": future_iso(60),
        },
    )
    assert response.status_code == 201
    assert response.json()["file_sha256"] == digest


def test_create_session_rejects_past_expiry(client):
    content = make_content(64)
    response = create_session(client, content, 64, expires_in=-1)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "EXPIRY_NOT_IN_FUTURE"


def test_create_session_rejects_malformed_sha256(client):
    content = make_content(64)
    for bad in ("abc", "z" * 64, ""):
        response = client.post(
            "/sessions",
            json={
                "file_size": len(content),
                "chunk_size": 64,
                "file_sha256": bad,
                "expires_at": future_iso(60),
            },
        )
        assert response.status_code == 422, bad
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_create_session_rejects_naive_expiry(client):
    content = make_content(64)
    naive = (datetime.now() + timedelta(hours=1)).isoformat()
    response = client.post(
        "/sessions",
        json={
            "file_size": len(content),
            "chunk_size": 64,
            "file_sha256": sha256_hex(content),
            "expires_at": naive,
        },
    )
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_create_session_rejects_non_positive_sizes(client):
    content = make_content(64)
    for field in ("file_size", "chunk_size"):
        payload = {
            "file_size": 64,
            "chunk_size": 64,
            "file_sha256": sha256_hex(content),
            "expires_at": future_iso(60),
        }
        payload[field] = 0
        response = client.post("/sessions", json=payload)
        assert response.status_code == 422
        assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_status_unknown_session_returns_structured_404(client):
    response = client.get("/sessions/does-not-exist")
    assert response.status_code == 404
    body = response.json()
    assert body["error"]["code"] == "SESSION_NOT_FOUND"
    assert set(body["error"]) == {"code", "message", "details"}


def test_status_lists_missing_chunks_in_ascending_order(client):
    content = make_content(1000)
    session_id = create_session(client, content, 256).json()["session_id"]

    status = client.get(f"/sessions/{session_id}").json()
    assert status["missing_chunks"] == [0, 1, 2, 3]
    assert status["received_chunks"] == 0

    # Upload out of order: missing list must stay sorted.
    put_chunk(client, session_id, 2, content[512:768])
    put_chunk(client, session_id, 0, content[0:256])
    status = client.get(f"/sessions/{session_id}").json()
    assert status["missing_chunks"] == [1, 3]
    assert status["received_chunks"] == 2
    assert status["status"] == "active"
    assert status["artifact"] is None
    assert status["completed_at"] is None
