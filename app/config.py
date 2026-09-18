"""Runtime configuration, sourced from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

DEFAULT_DATA_DIR = "./data"


@dataclass(frozen=True)
class Settings:
    """Filesystem locations the service persists state to."""

    data_dir: Path

    @property
    def db_path(self) -> Path:
        return self.data_dir / "uploads.db"

    @property
    def chunks_dir(self) -> Path:
        return self.data_dir / "chunks"

    @property
    def artifacts_dir(self) -> Path:
        return self.data_dir / "artifacts"


def settings_from_env() -> Settings:
    data_dir = Path(os.environ.get("DATA_DIR", DEFAULT_DATA_DIR)).resolve()
    return Settings(data_dir=data_dir)
