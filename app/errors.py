"""Structured API errors.

Every error response produced by this service has the shape::

    {"error": {"code": "<MACHINE_CODE>", "message": "<human readable>", "details": {...}}}
"""

from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """Domain error that maps directly onto an HTTP response."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details or {}


def error_payload(code: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"error": {"code": code, "message": message, "details": details or {}}}
