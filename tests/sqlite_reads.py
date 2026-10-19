"""Which tables a piece of code reads, observed at the SQLite connection (B37 #534).

The availability basket is checked against the tables a basket-grade block
*derives* from its descriptor; this records the tables it *actually* read, so a
test can hold the two together.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


def tables_read(conn: sqlite3.Connection, run: Callable[[], T]) -> tuple[T, set[str]]:
    """``run()``'s result, and every table a statement it prepared reads a column of.

    The authorizer sees each column a statement reads at prepare time, so a
    read is recorded even where the table holds no rows. SQLite's own catalog
    (a ``sqlite_master`` existence probe) is not a data dependency.
    """
    seen: set[str] = set()

    def authorize(action: int, arg1: str | None, *_: object) -> int:
        if action == sqlite3.SQLITE_READ and arg1 and not arg1.startswith("sqlite_"):
            seen.add(arg1)
        return sqlite3.SQLITE_OK

    conn.set_authorizer(authorize)
    try:
        result = run()
    finally:
        conn.set_authorizer(None)
    return result, seen
