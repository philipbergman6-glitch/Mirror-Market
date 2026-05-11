"""
Database connection abstraction for Mirror Market.

Provides a single get_connection() function that returns either:
  - A Turso (libsql) cloud connection when TURSO_DATABASE_URL is set
  - A local SQLite connection as fallback

This allows the same SQL code to work both locally and on cloud platforms
where the filesystem is ephemeral.

Key concepts for learning:
    - Environment variables control which database backend is used
    - The connection object supports the same API (execute, fetchall, etc.)
    - libsql is a fork of SQLite that adds network access
    - Free Turso tier: 9GB storage, 500 databases
"""

import logging
import os
import sqlite3

from config import DB_PATH, STORAGE_DIR, TURSO_AUTH_TOKEN, TURSO_DATABASE_URL

logger = logging.getLogger(__name__)


class TursoUnavailableError(RuntimeError):
    """Raised when Turso is required (MIRROR_REQUIRE_TURSO=1) but unreachable."""


def _require_turso() -> bool:
    return os.getenv("MIRROR_REQUIRE_TURSO", "").strip() == "1"


def get_connection():
    """
    Get a database connection — cloud (Turso) or local (SQLite).

    If TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are configured in config.py
    (via environment variables), connects to a hosted Turso database.
    Otherwise, falls back to the local SQLite file.

    When MIRROR_REQUIRE_TURSO=1, a failed Turso connection raises
    TursoUnavailableError instead of silently falling back to local.
    """
    if TURSO_DATABASE_URL and TURSO_AUTH_TOKEN:
        try:
            import libsql
            # libsql needs a local file for caching + sync_url for the cloud DB
            os.makedirs(STORAGE_DIR, exist_ok=True)
            local_replica = os.path.join(STORAGE_DIR, "local.db")
            conn = libsql.connect(
                local_replica,
                sync_url=TURSO_DATABASE_URL,
                auth_token=TURSO_AUTH_TOKEN,
            )
            conn.sync()
            return conn
        except ImportError as exc:
            msg = "libsql not installed; cannot use Turso cloud database"
            if _require_turso():
                raise TursoUnavailableError(msg) from exc
            logger.error("%s — falling back to local SQLite", msg)
        except Exception as exc:
            msg = f"Turso connection failed: {exc}"
            if _require_turso():
                raise TursoUnavailableError(msg) from exc
            logger.error("%s — falling back to local SQLite", msg)

    # Local SQLite fallback
    os.makedirs(STORAGE_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def is_cloud() -> bool:
    """Check if we're configured to use Turso cloud database."""
    return bool(TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)
