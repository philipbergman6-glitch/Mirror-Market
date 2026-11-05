"""Startup configuration — the one place the environment is read.

Invariant 1: a missing or malformed setting is a startup error, never a
default. The server would otherwise come up serving nothing, or serving at a
guessable path, or on a port the platform is not routing to.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path

ENV_DB_PATH = "MIRROR_MCP_DB_PATH"
ENV_PATH_TOKEN = "MIRROR_MCP_PATH_TOKEN"
ENV_LOG_PATH = "MIRROR_MCP_LOG_PATH"
ENV_DB_BUILT_AT = "MIRROR_MCP_DB_BUILT_AT"
ENV_HOST = "MIRROR_MCP_HOST"
ENV_PORT = "MIRROR_MCP_PORT"
REQUIRED_ENV = (ENV_DB_PATH, ENV_PATH_TOKEN, ENV_LOG_PATH, ENV_DB_BUILT_AT, ENV_HOST, ENV_PORT)

MIN_TOKEN_LENGTH = 32
_TOKEN_SHAPE = re.compile(r"^[A-Za-z0-9_-]+$")


class SettingsError(RuntimeError):
    """A setting is missing or cannot be used; the server must not start."""


@dataclass(frozen=True)
class Settings:
    db_path: Path
    path_token: str
    log_path: Path
    db_built_at: str
    host: str = "127.0.0.1"
    port: int = 8000

    @classmethod
    def from_env(cls) -> Settings:
        missing = [k for k in REQUIRED_ENV if not os.environ.get(k)]
        if missing:
            raise SettingsError(f"missing environment: {', '.join(missing)}")
        try:
            port = int(os.environ[ENV_PORT])
        except ValueError as exc:
            raise SettingsError(f"{ENV_PORT} is not an integer: {os.environ[ENV_PORT]!r}") from exc
        return cls(
            db_path=Path(os.environ[ENV_DB_PATH]),
            path_token=os.environ[ENV_PATH_TOKEN],
            log_path=Path(os.environ[ENV_LOG_PATH]),
            db_built_at=os.environ[ENV_DB_BUILT_AT],
            host=os.environ[ENV_HOST],
            port=port,
        )

    def validated(self) -> Settings:
        """Check every setting against what the server will do with it."""
        if not self.db_path.is_file():
            raise SettingsError(f"db_path does not exist: {self.db_path}")
        if len(self.path_token) < MIN_TOKEN_LENGTH or not _TOKEN_SHAPE.match(self.path_token):
            raise SettingsError(
                f"path_token must be at least {MIN_TOKEN_LENGTH} URL-safe characters (got {len(self.path_token)})"
            )
        if not self.db_built_at.strip():
            raise SettingsError("db_built_at is blank")
        if not 1 <= self.port <= 65535:
            raise SettingsError(f"port out of range: {self.port}")
        try:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SettingsError(f"log_path parent cannot be created: {exc}") from exc
        return self

    @property
    def mcp_path(self) -> str:
        return f"/mcp/{self.path_token}"
