"""Chunk upload boundaries: size, digest, idempotency, conflict, range."""

from __future__ import annotations

from tests.conftest import create_session, make_content, put_chunk, sha256_hex, split_chunks


def _open_session(client, size=1000, chunk_size=256, seed=1234):
    content = make_content(size, seed=seed)
    session_id = create_session(client, content, chunk_size).json()["session_id"]
    return content, session_id


def test_upload_chunk_success(client):
    content, session_id = _open_session(client)
    chunk = content[0:256]
    response = put_chunk(client, session_id, 0, chunk)
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["duplicate"] is False
    assert body["sha256"] == sha256_hex(chunk)
    assert body["size"] == 256
    assert body["received_chunks"] == 1
    assert body["total_chunks"] == 4
    assert body["session_status"] == "active"


def test_upload_rejects_out_of_range_index(client):
    content, session_id = _open_session(client)
    for bad_index in (4, 99, -1):
        response = put_chunk(client, session_id, bad_index, content[0:256])
        assert response.status_code == 422, bad_index
        assert response.json()["error"]["code"] == "CHUNK_INDEX_OUT_OF_RANGE"
    # Nothing was recorded.
    status = client.get(f"/sessions/{session_id}").json()
    assert status["received_chunks"] == 0
    assert status["missing_chunks"] == [0, 1, 2, 3]


def test_upload_rejects_wrong_length_middle_chunk(client):
    content, session_id = _open_session(client)
    short = content[0:200]  # middle chunks must be exactly chunk_size
    response = put_chunk(client, session_id, 1, short)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CHUNK_LENGTH_MISMATCH"
    assert client.get(f"/sessions/{session_id}").json()["missing_chunks"] == [0, 1, 2, 3]


def test_upload_rejects_wrong_length_last_chunk(client):
    content, session_id = _open_session(client)
    # Last chunk must be 1000 - 3*256 == 232 bytes.
    response = put_chunk(client, session_id, 3, content[768:768 + 231])
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CHUNK_LENGTH_MISMATCH"
    response = put_chunk(client, session_id, 3, content[768:] + b"x")  # 233 bytes
    assert response.status_code == 422
    # Exact length is accepted.
    assert put_chunk(client, session_id, 3, content[768:1000]).status_code == 200


def test_upload_rejects_oversized_body_with_matching_digest_header(client):
    # A lying Content-Length fast-path must not be bypassable: actual bytes win.
    content, session_id = _open_session(client)
    body = content[0:256] + b"extra"
    response = put_chunk(client, session_id, 0, body, digest=sha256_hex(body))
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "CHUNK_LENGTH_MISMATCH"


def test_upload_rejects_digest_mismatch_and_does_not_record(client):
    content, session_id = _open_session(client)
    chunk = content[0:256]
    wrong_digest = sha256_hex(b"something else")
    response = put_chunk(client, session_id, 0, chunk, digest=wrong_digest)
    assert response.status_code == 422
    body = response.json()
    assert body["error"]["code"] == "CHUNK_DIGEST_MISMATCH"
    assert body["error"]["details"]["declared_sha256"] == wrong_digest
    assert body["error"]["details"]["actual_sha256"] == sha256_hex(chunk)
    # Not recorded: the index stays missing and can be retried correctly.
    assert client.get(f"/sessions/{session_id}").json()["missing_chunks"] == [0, 1, 2, 3]
    assert put_chunk(client, session_id, 0, chunk).status_code == 200


def test_upload_requires_digest_header(client):
    content, session_id = _open_session(client)
    response = put_chunk(client, session_id, 0, content[0:256], send_digest=False)
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "MISSING_CHUNK_DIGEST"


def test_upload_rejects_malformed_digest_header(client):
    content, session_id = _open_session(client)
    response = put_chunk(client, session_id, 0, content[0:256], digest="not-a-digest")
    assert response.status_code == 400
    assert response.json()["error"]["code"] == "INVALID_CHUNK_DIGEST"


def test_reupload_same_content_is_idempotent(client):
    content, session_id = _open_session(client)
    chunk = content[0:256]
    first = put_chunk(client, session_id, 0, chunk)
    assert first.status_code == 200
    assert first.json()["duplicate"] is False

    second = put_chunk(client, session_id, 0, chunk)
    assert second.status_code == 200
    assert second.json()["duplicate"] is True

    status = client.get(f"/sessions/{session_id}").json()
    assert status["received_chunks"] == 1
    assert status["missing_chunks"] == [1, 2, 3]


def test_reupload_different_content_conflicts(client):
    content, session_id = _open_session(client)
    chunk = content[0:256]
    assert put_chunk(client, session_id, 0, chunk).status_code == 200

    other = make_content(256, seed=999)
    response = put_chunk(client, session_id, 0, other)
    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == "CHUNK_CONFLICT"
    assert body["error"]["details"]["stored_sha256"] == sha256_hex(chunk)
    assert body["error"]["details"]["provided_sha256"] == sha256_hex(other)

    # Original chunk is still the one recorded.
    status = client.get(f"/sessions/{session_id}").json()
    assert status["received_chunks"] == 1


def test_upload_to_unknown_session_is_404(client):
    response = put_chunk(client, "nope", 0, b"x" * 10)
    assert response.status_code == 404
    assert response.json()["error"]["code"] == "SESSION_NOT_FOUND"


def test_single_chunk_file_roundtrip(client):
    content = make_content(100)
    session_id = create_session(client, content, 256).json()["session_id"]
    response = put_chunk(client, session_id, 0, content)
    assert response.status_code == 200
    assert response.json()["session_status"] == "completed"
    assert response.json()["artifact"]["sha256"] == sha256_hex(content)
