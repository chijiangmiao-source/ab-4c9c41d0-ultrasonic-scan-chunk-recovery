"""HTTP routes."""

from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import FileResponse

from .models import (
    ChunkUploadResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    FinalizeResponse,
    SessionStatusResponse,
)
from .service import UploadService

router = APIRouter()

CHUNK_DIGEST_HEADER = "x-chunk-sha256"


def get_service(request: Request) -> UploadService:
    return request.app.state.service


@router.post(
    "/sessions",
    status_code=201,
    response_model=CreateSessionResponse,
    summary="Declare a file and open a resumable upload session",
)
def create_session(
    payload: CreateSessionRequest,
    service: UploadService = Depends(get_service),
) -> CreateSessionResponse:
    return service.create_session(payload)


@router.get(
    "/sessions/{session_id}",
    response_model=SessionStatusResponse,
    summary="Session status with ascending list of missing chunk indices",
)
def get_session_status(
    session_id: str,
    service: UploadService = Depends(get_service),
) -> SessionStatusResponse:
    return service.get_status(session_id)


@router.put(
    "/sessions/{session_id}/chunks/{index}",
    response_model=ChunkUploadResponse,
    summary="Upload one chunk (idempotent per index)",
)
async def upload_chunk(
    session_id: str,
    index: int,
    request: Request,
    service: UploadService = Depends(get_service),
) -> ChunkUploadResponse:
    content_length_header = request.headers.get("content-length")
    content_length = None
    if content_length_header is not None:
        try:
            content_length = int(content_length_header)
        except ValueError:
            content_length = None
    return await service.upload_chunk(
        session_id=session_id,
        index=index,
        digest_header=request.headers.get(CHUNK_DIGEST_HEADER),
        stream=request.stream(),
        content_length=content_length,
    )


@router.post(
    "/sessions/{session_id}/finalize",
    response_model=FinalizeResponse,
    summary="Assemble, verify and atomically publish the artifact",
)
def finalize_session(
    session_id: str,
    service: UploadService = Depends(get_service),
) -> FinalizeResponse:
    return service.finalize(session_id)


@router.get(
    "/sessions/{session_id}/artifact",
    summary="Download the published, hash-verified artifact",
)
def download_artifact(
    session_id: str,
    service: UploadService = Depends(get_service),
) -> FileResponse:
    path, sha256 = service.artifact_file(session_id)
    return FileResponse(
        path,
        media_type="application/octet-stream",
        filename=f"{session_id}.bin",
        headers={"X-Content-SHA256": sha256},
    )
