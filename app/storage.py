"""Filesystem storage for chunk bodies and published artifacts.

Chunk bodies live under ``<data_dir>/chunks/<session_id>/``; finished files are
published under ``<data_dir>/artifacts/``. Every write goes to a temporary file
in the same directory first and is then moved into place with ``os.replace``,
which is atomic on POSIX — a reader never observes a partially written chunk or
artifact, and a crash leaves at most a harmless ``*.tmp`` file behind.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from collections.abc import AsyncIterator
from pathlib import Path

from .config import Settings

_STREAM_BUFFER = 1024 * 1024  # 1 MiB


class Storage:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        settings.chunks_dir.mkdir(parents=True, exist_ok=True)
        settings.artifacts_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # paths
    # ------------------------------------------------------------------
    def chunk_dir(self, session_id: str) -> Path:
        return self._settings.chunks_dir / session_id

    def chunk_path(self, session_id: str, index: int) -> Path:
        return self.chunk_dir(session_id) / f"{index:08d}.part"

    def artifact_path(self, session_id: str) -> Path:
        return self._settings.artifacts_dir / f"{session_id}.bin"

    # ------------------------------------------------------------------
    # chunk ingestion
    # ------------------------------------------------------------------
    async def write_chunk_stream(
        self, session_id: str, index: int, stream: AsyncIterator[bytes]
    ) -> tuple[Path, str, int]:
        """Spool the request body to a temp file while hashing it.

        Returns ``(tmp_path, sha256_hex, size)``. The caller is responsible for
        either committing or discarding the temp file.
        """
        self.chunk_dir(session_id).mkdir(parents=True, exist_ok=True)
        tmp_path = self.chunk_dir(session_id) / f".{index:08d}.{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as handle:
                async for part in stream:
                    if not part:
                        continue
                    hasher.update(part)
                    handle.write(part)
                    size += len(part)
                handle.flush()
                os.fsync(handle.fileno())
        except BaseException:
            self.discard(tmp_path)
            raise
        return tmp_path, hasher.hexdigest(), size

    def commit_chunk(self, tmp_path: Path, session_id: str, index: int) -> Path:
        final_path = self.chunk_path(session_id, index)
        os.replace(tmp_path, final_path)
        return final_path

    # ------------------------------------------------------------------
    # assembly / publication
    # ------------------------------------------------------------------
    def assemble_to_temp(self, session_id: str, total_chunks: int) -> tuple[Path, str, int]:
        """Concatenate all chunks in order into a temp artifact, hashing on the fly."""
        tmp_path = self._settings.artifacts_dir / f".{session_id}.{uuid.uuid4().hex}.tmp"
        hasher = hashlib.sha256()
        size = 0
        try:
            with open(tmp_path, "wb") as out:
                for index in range(total_chunks):
                    chunk_path = self.chunk_path(session_id, index)
                    with open(chunk_path, "rb") as chunk_file:
                        while True:
                            block = chunk_file.read(_STREAM_BUFFER)
                            if not block:
                                break
                            hasher.update(block)
                            out.write(block)
                            size += len(block)
                out.flush()
                os.fsync(out.fileno())
        except BaseException:
            self.discard(tmp_path)
            raise
        return tmp_path, hasher.hexdigest(), size

    def publish_artifact(self, tmp_path: Path, session_id: str) -> Path:
        """Atomically move the verified temp artifact to its final name."""
        final_path = self.artifact_path(session_id)
        os.replace(tmp_path, final_path)
        self._fsync_dir(self._settings.artifacts_dir)
        return final_path

    # ------------------------------------------------------------------
    # cleanup helpers
    # ------------------------------------------------------------------
    @staticmethod
    def discard(path: Path) -> None:
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    def discard_chunk_bodies(self, session_id: str) -> None:
        """Best-effort removal of chunk bodies once the artifact is published.

        Per-chunk digests remain in SQLite, so idempotent re-uploads and
        conflict detection keep working after the bodies are gone.
        """
        directory = self.chunk_dir(session_id)
        if not directory.is_dir():
            return
        for entry in directory.iterdir():
            try:
                entry.unlink()
            except OSError:
                pass
        try:
            directory.rmdir()
        except OSError:
            pass

    def sweep_temp_files(self) -> None:
        """Remove leftover temp files from interrupted writes (startup hook)."""
        for root in (self._settings.chunks_dir, self._settings.artifacts_dir):
            if not root.is_dir():
                continue
            for entry in root.rglob("*.tmp"):
                try:
                    entry.unlink()
                except OSError:
                    pass

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
