"""Finalize, integrity gating, atomic publish, expiry and artifact download."""

from __future__ import annotations

import time

from tests.conftest import create_session, make_content, put_chunk, sha256_hex, split_chunks


def _upload_all(client, session_id, chunks):
    last = None
    for index, chunk in enumerate(chunks):
        last = put_chunk(client, session_id, index, chunk)
    return last


def test_last_chunk_auto_publishes_artifact(client, settings):
    content = make_content(1000)
    session_id = create_session(client, content, 256).json()["session_id"]
    chunks = split_chunks(content, 256)

    response = _upload_all(client, session_id, chunks)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["session_status"] == "completed"
    assert body["artifact"]["sha256"] == sha256_hex(content)
    assert body["artifact"]["size"] == 1000

    # The artifact on disk is byte-identical to the declared file.
    artifact_path = settings.artifacts_dir / f"{session_id}.bin"
    assert artifact_path.read_bytes() == content

    status = client.get(f"/sessions/{session_id}").json()
    assert status["status"] == "completed"
    assert status["missing_chunks"] == []
    assert status["completed_at"] is not None
    assert status["artifact"]["url"] == f"/sessions/{session_id}/artifact"


def test_finalize_endpoint_reports_missing_chunks(client):
    content = make_content(1000)
    session_id = create_session(client, content, 256).json()["session_id"]
    put_chunk(client, session_id, 1, split_chunks(content, 256)[1])

    response = client.post(f"/sessions/{session_id}/finalize")
    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == "CHUNKS_INCOMPLETE"
    assert body["error"]["details"]["missing_chunks"] == [0, 2, 3]

    # State is untouched: the session is still active and resumable.
    status = client.get(f"/sessions/{session_id}").json()
    assert status["status"] == "active"
    assert status["missing_chunks"] == [0, 2, 3]


def test_finalize_is_idempotent_after_completion(client):
    content = make_content(600)
    session_id = create_session(client, content, 256).json()["session_id"]
    _upload_all(client, session_id, split_chunks(content, 256))

    first = client.post(f"/sessions/{session_id}/finalize")
    second = client.post(f"/sessions/{session_id}/finalize")
    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json()["artifact"]["sha256"] == sha256_hex(content)
    assert second.json()["artifact"]["sha256"] == sha256_hex(content)


def test_integrity_mismatch_fails_session_permanently(client, settings):
    content = make_content(600)
    # Declared digest belongs to a different file: every chunk is individually
    # valid, but the assembled file can never match the declared SHA-256.
    wrong_declared = sha256_hex(make_content(600, seed=777))
    session_id = create_session(client, content, 256, file_sha256=wrong_declared).json()[
        "session_id"
    ]
    chunks = split_chunks(content, 256)

    response = _upload_all(client, session_id, chunks)
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "INTEGRITY_MISMATCH"
    assert body["error"]["details"]["expected_sha256"] == wrong_declared
    assert body["error"]["details"]["actual_sha256"] == sha256_hex(content)

    # Nothing was published.
    assert not (settings.artifacts_dir / f"{session_id}.bin").exists()

    status = client.get(f"/sessions/{session_id}").json()
    assert status["status"] == "failed"
    assert status["error"]["code"] == "INTEGRITY_MISMATCH"
    assert status["artifact"] is None

    # Chunks are immutable after failure: same content is an idempotent
    # success, different content conflicts, and no new state is accepted.
    assert put_chunk(client, session_id, 0, chunks[0]).json()["duplicate"] is True
    conflict = put_chunk(client, session_id, 0, make_content(256, seed=5))
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "CHUNK_CONFLICT"

    # Finalize keeps returning the stored failure instead of flipping state.
    finalize = client.post(f"/sessions/{session_id}/finalize")
    assert finalize.status_code == 409
    assert finalize.json()["error"]["code"] == "SESSION_FAILED"
    assert (
        finalize.json()["error"]["details"]["failure"]["code"] == "INTEGRITY_MISMATCH"
    )


def test_expired_session_rejects_new_chunks_but_stays_consistent(client):
    content = make_content(1000)
    session_id = create_session(client, content, 256, expires_in=0.8).json()["session_id"]
    chunks = split_chunks(content, 256)
    assert put_chunk(client, session_id, 0, chunks[0]).status_code == 200

    time.sleep(1.0)

    # New chunks are refused.
    response = put_chunk(client, session_id, 1, chunks[1])
    assert response.status_code == 410
    assert response.json()["error"]["code"] == "SESSION_EXPIRED"

    # A retransmission of an already-confirmed chunk is not a *new* chunk:
    # it stays an idempotent success and does not change progress.
    duplicate = put_chunk(client, session_id, 0, chunks[0])
    assert duplicate.status_code == 200
    assert duplicate.json()["duplicate"] is True

    # Finalize cannot succeed either; missing state is preserved.
    finalize = client.post(f"/sessions/{session_id}/finalize")
    assert finalize.status_code == 410
    assert finalize.json()["error"]["code"] == "SESSION_EXPIRED"

    status = client.get(f"/sessions/{session_id}").json()
    assert status["status"] == "expired"
    assert status["received_chunks"] == 1
    assert status["missing_chunks"] == [1, 2, 3]


def test_artifact_download_requires_completion(client):
    content = make_content(600)
    session_id = create_session(client, content, 256).json()["session_id"]

    early = client.get(f"/sessions/{session_id}/artifact")
    assert early.status_code == 409
    assert early.json()["error"]["code"] == "ARTIFACT_NOT_READY"

    _upload_all(client, session_id, split_chunks(content, 256))
    downloaded = client.get(f"/sessions/{session_id}/artifact")
    assert downloaded.status_code == 200
    assert downloaded.content == content
    assert downloaded.headers["x-content-sha256"] == sha256_hex(content)
    assert sha256_hex(downloaded.content) == sha256_hex(content)


def test_artifact_download_unknown_session(client):
    response = client.get("/sessions/nope/artifact")
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "SESSION_NOT_FOUND"
