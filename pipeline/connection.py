"""
Database connection for Mirror Market: one backend, local SQLite.

`get_connection()` returns a `sqlite3.Connection` to `config.DB_PATH`, in CI
and on every developer machine. CI persistence is git-committed CSVs in
`data/history/` (invariant 6, decided 2026-07-30), not a hosted database.
A dormant Turso branch lived here until #318; it was unreachable as
the project is installed and was deleted so the storage story is stated once.
"""

import logging
import os
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager

from config import DB_PATH, STORAGE_DIR

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    """Open the local SQLite database, creating the storage directory if needed."""
    os.makedirs(STORAGE_DIR, exist_ok=True)
    return sqlite3.connect(DB_PATH)


@contextmanager
def managed_connection(conn) -> Iterator:
    """Commit or roll back through the connection context, then close it.

    ``sqlite3.Connection`` implements ``with conn`` for transaction handling
    only; it deliberately does not close the file descriptor on exit.
    """
    try:
        with conn:
            yield conn
    finally:
        close = getattr(conn, "close", None)
        if callable(close):
            close()


def is_cloud() -> bool:
    """Always False: there is no cloud backend. Removed in the next commit."""
    return False
