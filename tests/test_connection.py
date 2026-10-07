"""Tests for pipeline.connection — one storage backend, SQLite, stated once.

get_connection() is exercised transitively by tests/test_store.py and
tests/test_query.py (via the patched_db fixture). Here we lock down the
pure helpers and the two guards that keep invariant 6 (no cloud DB,
decided 2026-07-30) from being reversed by accident: the dependency guard
and the symbol guard added when the dormant Turso path was deleted (#318).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest

from pipeline import connection

REPO = Path(__file__).resolve().parent.parent


def test_get_connection_returns_sqlite(monkeypatch, tmp_path):
    """get_connection returns a working sqlite3 connection, nothing else."""
    import sqlite3

    monkeypatch.setattr("pipeline.connection.DB_PATH", str(tmp_path / "local.db"))
    monkeypatch.setattr("pipeline.connection.STORAGE_DIR", str(tmp_path))

    conn = connection.get_connection()
    try:
        assert isinstance(conn, sqlite3.Connection)
        assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        conn.close()


def test_get_connection_creates_storage_dir(monkeypatch, tmp_path):
    storage = tmp_path / "nested" / "storage"
    monkeypatch.setattr("pipeline.connection.DB_PATH", str(storage / "local.db"))
    monkeypatch.setattr("pipeline.connection.STORAGE_DIR", str(storage))

    connection.get_connection().close()
    assert storage.is_dir()


def test_managed_connection_closes_after_context_exit():
    class FakeConnection:
        closed = False
        entered = False

        def __enter__(self):
            self.entered = True
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def close(self):
            self.closed = True

    conn = FakeConnection()

    with connection.managed_connection(conn) as active:
        assert active is conn
        assert conn.entered is True
        assert conn.closed is False

    assert conn.closed is True


def test_libsql_is_not_a_declared_dependency():
    """Invariant 6, pinned at the one fact that actually enforces it.

    Adding `libsql` is how the 2026-07-30 no-cloud-DB decision gets reversed
    by accident, so this fails if it appears in either requirements file.
    Reintroducing it deliberately means deleting this test, which is the
    review conversation the decision deserves.
    """
    for name in ("requirements.txt", "requirements-dev.txt"):
        text = (REPO / name).read_text(encoding="utf-8").lower()
        assert "libsql" not in text, (
            f"{name} declares libsql, against invariant 6 "
            f"(no cloud DB, decided 2026-07-30; Turso path deleted in #318)."
        )


# Symbols of the deleted Turso path. `Turso` as a prose word in a history
# note is fine; these identifiers are not.
_DELETED_SYMBOLS = re.compile(
    r"\bTURSO_[A-Z_]+\b|\blibsql\b|\bis_cloud\b|\bmaybe_sync\b"
    r"|\bTursoUnavailableError\b|\bMIRROR_REQUIRE_TURSO\b"
)
# History stays as written; the two invariant-6 notes say the path was
# removed and why, and may name the env vars that are gone.
_SYMBOL_GUARD_EXEMPT = {"CHANGELOG.md", "AUDIT-2026-08-22.md", "LAYERS.md", "CLAUDE.md"}


def _tracked_text_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "--", "*.py", "*.md", "*.yml", "*.yaml", "*.txt", "*.toml"],
        cwd=REPO,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.split("\n")
    return [REPO / p for p in out if p and not p.startswith("docs/")]


def test_no_turso_symbol_remains_outside_history():
    """#318 acceptance: the dormant Turso path is deleted, not just unreachable.

    One storage backend, stated once. A second backend that can never run is
    an unspecified system, and `if not is_cloud() and ...` guards scattered
    across readers were the cost of carrying it.
    """
    this_file = Path(__file__).resolve()
    offenders: list[str] = []
    for path in _tracked_text_files():
        if path.name in _SYMBOL_GUARD_EXEMPT or path == this_file:
            continue
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if _DELETED_SYMBOLS.search(line):
                offenders.append(f"{path.relative_to(REPO)}:{lineno}: {line.strip()}")
    assert not offenders, "Turso symbols remain:\n" + "\n".join(offenders)


def test_connection_module_exposes_only_sqlite_api():
    for gone in ("is_cloud", "maybe_sync", "TursoUnavailableError", "_require_turso"):
        assert not hasattr(connection, gone), f"pipeline.connection.{gone} should be deleted"
    with pytest.raises(ImportError):
        from config import TURSO_DATABASE_URL  # noqa: F401
