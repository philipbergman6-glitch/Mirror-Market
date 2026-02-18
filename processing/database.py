"""
Database connection abstraction for Mirror Market.

Provides a single get_connection() function that returns either:
  - A Turso (libsql) cloud connection when TURSO_DATABASE_URL is set
  - A local SQLite connection as fallback

This allows the same SQL code to work both locally and on Streamlit Cloud
where the filesystem is ephemeral.

Key concepts for learning:
    - Environment variables control which database backend is used
    - The connection object supports the same API (execute, fetchall, etc.)
    - libsql is a fork of SQLite that adds network access
    - Free Turso tier: 9GB storage, 500 databases
"""

import os
import sqlite3

from config import DB_PATH, STORAGE_DIR, TURSO_DATABASE_URL, TURSO_AUTH_TOKEN


def get_connection():
    """
    Get a database connection — cloud (Turso) or local (SQLite).

    If TURSO_DATABASE_URL and TURSO_AUTH_TOKEN are configured in config.py
    (via environment variables), connects to a hosted Turso database.
    Otherwise, falls back to the local SQLite file.

    Returns
    -------
    connection
        A database connection object supporting standard DB-API 2.0 methods.
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
        except ImportError:
            # libsql not installed — fall back to local
            pass
        except Exception:
            # Connection failed — fall back to local
            pass

    # Local SQLite fallback
    os.makedirs(STORAGE_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def is_cloud() -> bool:
    """Check if we're configured to use Turso cloud database."""
    return bool(TURSO_DATABASE_URL and TURSO_AUTH_TOKEN)
