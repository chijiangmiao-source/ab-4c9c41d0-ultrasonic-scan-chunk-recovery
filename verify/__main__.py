"""Acceptance run against a live API instance.

Drives the complete equipment workflow — session declaration, out-of-order
chunk upload with simulated retransmissions, integrity-gated publication and
artifact verification — plus the failure modes (digest mismatch, conflicts,
expiry, incomplete finalize). Every check uses freshly generated content and
real SHA-256 digests; nothing is stubbed.

Usage:  python -m verify
Env:    API_BASE_URL (default http://api:8000)
"""

from __future__ import annotations

import hashlib
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import httpx

BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000").rstrip("/")
HEALTH_TIMEOUT_SECONDS = float(os.environ.get("VERIFY_HEALTH_TIMEOUT", "60"))

_FAILURES: list[str] = []
_CHECKS = 0


def check(name: str, condition: bool, context: str = "") -> None:
    global _CHECKS
    _CHECKS += 1
    if condition:
        print(f"  PASS  {name}")
    else:
        _FAILURES.append(name)
        suffix = f" | {context}" if context else ""
        print(f"  FAIL  {name}{suffix}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def wait_for_api(client: httpx.Client) -> bool:
    deadline = time.monotonic() + HEALTH_TIMEOUT_SECONDS
    while time.monotonic() < deadline:
        try:
            response = client.get("/healthz", timeout=5)
            if response.status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


def future_iso(seconds: float) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def create_session(
    client: httpx.Client, content: bytes, chunk_size: int, **overrides
) -> httpx.Response:
    payload = {
        "file_size": len(content),
        "chunk_size": chunk_size,
        "file_sha256": sha256_hex(content),
        "expires_at": future_iso(3600),
    }
    payload.update(overrides)
    return client.post("/sessions", json=payload)


def put_chunk(
    client: httpx.Client,
    session_id: str,
    index: int,
    body: bytes,
    digest: str | None = None,
) -> httpx.Response:
    return client.put(
        f"/sessions/{session_id}/chunks/{index}",
        content=body,
        headers={"X-Chunk-SHA256": digest if digest is not None else sha256_hex(body)},
    )


def scenario_happy_path_with_retransmission(client: httpx.Client) -> None:
    print("[1] resumable upload with dropouts, retransmission and atomic publish")
    rng = random.Random(20260918)
    chunk_size = 256 * 1024
    content = rng.randbytes(2 * chunk_size + 100_000)  # 3 chunks, short last one
    chunks = [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]

    created = create_session(client, content, chunk_size)
    check("create session -> 201", created.status_code == 201, created.text)
    session = created.json()
    session_id = session["session_id"]
    check("total_chunks == ceil(size/chunk_size)", session["total_chunks"] == 3)

    # Upload out of order, simulating a reconnecting uplink.
    order = [2, 0, 1]
    expected_missing = [[0, 1], [1], []]
    for step, index in enumerate(order):
        response = put_chunk(client, session_id, index, chunks[index])
        check(f"chunk {index} accepted", response.status_code == 200, response.text)
        status = client.get(f"/sessions/{session_id}").json()
        check(
            f"missing list after chunk {index} is ascending {expected_missing[step]}",
            status["missing_chunks"] == expected_missing[step],
            str(status["missing_chunks"]),
        )
        if step < 2:
            # Mid-flight dropout: client retries chunk it already sent.
            retry = put_chunk(client, session_id, index, chunks[index])
            check(
                f"retransmission of chunk {index} is idempotent",
                retry.status_code == 200 and retry.json()["duplicate"] is True,
                retry.text,
            )
            # A different payload for the same index must be refused.
            other = rng.randbytes(len(chunks[index]))
            conflict = put_chunk(client, session_id, index, other)
            check(
                f"conflicting retransmission of chunk {index} -> 409",
                conflict.status_code == 409
                and conflict.json()["error"]["code"] == "CHUNK_CONFLICT",
                conflict.text,
            )

    last = client.get(f"/sessions/{session_id}").json()
    check("session completed after last chunk", last["status"] == "completed", str(last))
    check("no missing chunks remain", last["missing_chunks"] == [])
    check(
        "artifact digest equals declared digest",
        last["artifact"]["sha256"] == sha256_hex(content),
    )

    downloaded = client.get(f"/sessions/{session_id}/artifact")
    check("artifact download -> 200", downloaded.status_code == 200, downloaded.text)
    check(
        "downloaded artifact is byte-identical",
        downloaded.content == content,
        f"{len(downloaded.content)} vs {len(content)} bytes",
    )
    check(
        "artifact SHA-256 header matches",
        downloaded.headers.get("x-content-sha256") == sha256_hex(content),
    )

    again = client.post(f"/sessions/{session_id}/finalize")
    check(
        "finalize is idempotent after completion",
        again.status_code == 200 and again.json()["status"] == "completed",
        again.text,
    )


def scenario_rejected_chunks(client: httpx.Client) -> None:
    print("[2] per-chunk validation never pollutes progress")
    rng = random.Random(7)
    chunk_size = 64 * 1024
    content = rng.randbytes(chunk_size + 10)  # 2 chunks
    session_id = create_session(client, content, chunk_size).json()["session_id"]
    chunks = [content[:chunk_size], content[chunk_size:]]

    bad_digest = put_chunk(client, session_id, 0, chunks[0], digest=sha256_hex(b"nope"))
    check(
        "digest mismatch -> 422 CHUNK_DIGEST_MISMATCH",
        bad_digest.status_code == 422
        and bad_digest.json()["error"]["code"] == "CHUNK_DIGEST_MISMATCH",
        bad_digest.text,
    )
    wrong_len = put_chunk(client, session_id, 0, chunks[0][:-1])
    check(
        "wrong length -> 422 CHUNK_LENGTH_MISMATCH",
        wrong_len.status_code == 422
        and wrong_len.json()["error"]["code"] == "CHUNK_LENGTH_MISMATCH",
        wrong_len.text,
    )
    out_of_range = put_chunk(client, session_id, 5, chunks[0])
    check(
        "out-of-range index -> 422 CHUNK_INDEX_OUT_OF_RANGE",
        out_of_range.status_code == 422
        and out_of_range.json()["error"]["code"] == "CHUNK_INDEX_OUT_OF_RANGE",
        out_of_range.text,
    )
    missing = client.get(f"/sessions/{session_id}").json()["missing_chunks"]
    check("rejected chunks were not recorded", missing == [0, 1], str(missing))

    incomplete = client.post(f"/sessions/{session_id}/finalize")
    check(
        "finalize with missing chunks -> 409 CHUNKS_INCOMPLETE",
        incomplete.status_code == 409
        and incomplete.json()["error"]["code"] == "CHUNKS_INCOMPLETE"
        and incomplete.json()["error"]["details"]["missing_chunks"] == [0, 1],
        incomplete.text,
    )

    for index, chunk in enumerate(chunks):
        assert put_chunk(client, session_id, index, chunk).status_code == 200
    status = client.get(f"/sessions/{session_id}").json()
    check("session recovers and completes", status["status"] == "completed")


def scenario_integrity_failure(client: httpx.Client) -> None:
    print("[3] whole-file integrity gate")
    rng = random.Random(99)
    chunk_size = 64 * 1024
    content = rng.randbytes(chunk_size * 2)
    wrong_declared = sha256_hex(rng.randbytes(chunk_size * 2))
    session_id = create_session(
        client, content, chunk_size, file_sha256=wrong_declared
    ).json()["session_id"]

    chunks = [content[:chunk_size], content[chunk_size:]]
    put_chunk(client, session_id, 0, chunks[0])
    last = put_chunk(client, session_id, 1, chunks[1])
    check(
        "assembled-hash mismatch -> 422 INTEGRITY_MISMATCH",
        last.status_code == 422
        and last.json()["error"]["code"] == "INTEGRITY_MISMATCH"
        and last.json()["error"]["details"]["expected_sha256"] == wrong_declared,
        last.text,
    )
    status = client.get(f"/sessions/{session_id}").json()
    check(
        "session is marked failed with reviewable error",
        status["status"] == "failed" and status["error"]["code"] == "INTEGRITY_MISMATCH",
        str(status),
    )
    artifact = client.get(f"/sessions/{session_id}/artifact")
    check(
        "no artifact is published for a failed session",
        artifact.status_code == 409
        and artifact.json()["error"]["code"] == "ARTIFACT_NOT_READY",
        artifact.text,
    )


def scenario_expiry(client: httpx.Client) -> None:
    print("[4] expired sessions refuse new chunks")
    rng = random.Random(5)
    chunk_size = 64 * 1024
    content = rng.randbytes(chunk_size * 2)
    session_id = create_session(
        client, content, chunk_size, expires_at=future_iso(1.5)
    ).json()["session_id"]
    first = content[:chunk_size]
    check(
        "chunk before expiry accepted",
        put_chunk(client, session_id, 0, first).status_code == 200,
    )
    time.sleep(2.0)
    expired = put_chunk(client, session_id, 1, content[chunk_size:])
    check(
        "new chunk after expiry -> 410 SESSION_EXPIRED",
        expired.status_code == 410
        and expired.json()["error"]["code"] == "SESSION_EXPIRED",
        expired.text,
    )
    status = client.get(f"/sessions/{session_id}").json()
    check(
        "expired session keeps confirmed progress",
        status["status"] == "expired" and status["missing_chunks"] == [1],
        str(status),
    )


def scenario_error_shape(client: httpx.Client) -> None:
    print("[5] errors are always structured")
    unknown = client.get("/sessions/unknown-id")
    body = unknown.json()
    check(
        "unknown session -> 404 with error.code",
        unknown.status_code == 404
        and body["error"]["code"] == "SESSION_NOT_FOUND"
        and isinstance(body["error"]["message"], str)
        and isinstance(body["error"]["details"], dict),
        unknown.text,
    )
    invalid = client.post(
        "/sessions",
        json={
            "file_size": -1,
            "chunk_size": 0,
            "file_sha256": "xyz",
            "expires_at": "not-a-date",
        },
    )
    check(
        "invalid declaration -> 422 VALIDATION_ERROR",
        invalid.status_code == 422
        and invalid.json()["error"]["code"] == "VALIDATION_ERROR",
        invalid.text,
    )


def main() -> int:
    print(f"verify: acceptance run against {BASE_URL}")
    with httpx.Client(base_url=BASE_URL, timeout=60) as client:
        if not wait_for_api(client):
            print(f"verify: API did not become healthy within {HEALTH_TIMEOUT_SECONDS}s")
            return 1
        print("  PASS  /healthz")
        scenario_happy_path_with_retransmission(client)
        scenario_rejected_chunks(client)
        scenario_integrity_failure(client)
        scenario_expiry(client)
        scenario_error_shape(client)

    print(f"verify: {_CHECKS - len(_FAILURES)}/{_CHECKS} checks passed")
    if _FAILURES:
        print("verify: FAILED checks:")
        for name in _FAILURES:
            print(f"  - {name}")
        return 1
    print("verify: OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
