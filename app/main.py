"""FastAPI application factory."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__
from .config import Settings, settings_from_env
from .db import Database
from .errors import ApiError, error_payload
from .routes import router
from .service import UploadService
from .storage import Storage

logger = logging.getLogger(__name__)

_HTTP_ERROR_CODES = {
    400: "BAD_REQUEST",
    401: "UNAUTHORIZED",
    403: "FORBIDDEN",
    404: "NOT_FOUND",
    405: "METHOD_NOT_ALLOWED",
    415: "UNSUPPORTED_MEDIA_TYPE",
}


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or settings_from_env()
    database = Database(settings.db_path)
    storage = Storage(settings)
    service = UploadService(database, storage)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Resume from the persisted position: finish sessions whose chunks were
        # all confirmed before a restart, sweep interrupted temp writes.
        service.recover_after_restart()
        yield
        service.close()

    app = FastAPI(
        title="Resumable Scan Upload API",
        version=__version__,
        description=(
            "Chunked, resumable upload of large ultrasonic scan files with "
            "SHA-256 integrity gating and atomic artifact publication."
        ),
        lifespan=lifespan,
    )
    app.state.service = service
    app.state.settings = settings

    @app.exception_handler(ApiError)
    async def api_error_handler(request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.status_code,
            content=error_payload(exc.code, exc.message, exc.details),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=error_payload(
                "VALIDATION_ERROR",
                "request failed validation",
                {"errors": jsonable_encoder(exc.errors())},
            ),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = _HTTP_ERROR_CODES.get(exc.status_code, "HTTP_ERROR")
        return JSONResponse(
            status_code=exc.status_code,
            content=error_payload(code, str(exc.detail)),
        )

    @app.exception_handler(Exception)
    async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(
            status_code=500,
            content=error_payload("INTERNAL_ERROR", "unexpected internal error"),
        )

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(router)
    return app


app = create_app()
