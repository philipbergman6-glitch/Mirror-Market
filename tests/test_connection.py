"""Tests for pipeline.connection.is_cloud and the require-turso guard.

get_connection() itself is exercised transitively by tests/test_store.py
and tests/test_query.py (via the patched_db fixture). Here we just lock
down the small pure helpers.
"""

from __future__ import annotations

import pytest

from pipeline import connection


def test_is_cloud_false_when_no_env_vars(monkeypatch):
    monkeypatch.setattr("pipeline.connection.TURSO_DATABASE_URL", None)
    monkeypatch.setattr("pipeline.connection.TURSO_AUTH_TOKEN", None)
    assert connection.is_cloud() is False


def test_is_cloud_false_when_only_url_set(monkeypatch):
    monkeypatch.setattr("pipeline.connection.TURSO_DATABASE_URL", "libsql://example.turso.io")
    monkeypatch.setattr("pipeline.connection.TURSO_AUTH_TOKEN", None)
    assert connection.is_cloud() is False


def test_is_cloud_true_when_both_env_vars_set(monkeypatch):
    monkeypatch.setattr("pipeline.connection.TURSO_DATABASE_URL", "libsql://example.turso.io")
    monkeypatch.setattr("pipeline.connection.TURSO_AUTH_TOKEN", "secret")
    assert connection.is_cloud() is True


def test_require_turso_false_when_unset(monkeypatch):
    monkeypatch.delenv("MIRROR_REQUIRE_TURSO", raising=False)
    assert connection._require_turso() is False


def test_require_turso_true_when_one(monkeypatch):
    monkeypatch.setenv("MIRROR_REQUIRE_TURSO", "1")
    assert connection._require_turso() is True


def test_require_turso_false_when_other_value(monkeypatch):
    monkeypatch.setenv("MIRROR_REQUIRE_TURSO", "yes")
    assert connection._require_turso() is False


def test_get_connection_falls_back_to_sqlite_without_turso_env(monkeypatch, tmp_path):
    """Without TURSO env vars, get_connection should return a sqlite3 conn."""
    monkeypatch.setattr("pipeline.connection.TURSO_DATABASE_URL", None)
    monkeypatch.setattr("pipeline.connection.TURSO_AUTH_TOKEN", None)
    monkeypatch.setattr("pipeline.connection.DB_PATH", str(tmp_path / "fallback.db"))
    monkeypatch.setattr("pipeline.connection.STORAGE_DIR", str(tmp_path))

    conn = connection.get_connection()
    try:
        # Round-trip a trivial query to confirm it's a working connection.
        result = conn.execute("SELECT 1").fetchone()
        assert result == (1,)
    finally:
        conn.close()


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


def test_get_connection_raises_when_require_turso_and_libsql_missing(monkeypatch):
    """MIRROR_REQUIRE_TURSO=1 + no libsql ⇒ TursoUnavailableError, not fallback."""
    monkeypatch.setattr("pipeline.connection.TURSO_DATABASE_URL", "libsql://example.turso.io")
    monkeypatch.setattr("pipeline.connection.TURSO_AUTH_TOKEN", "secret")
    monkeypatch.setenv("MIRROR_REQUIRE_TURSO", "1")

    # Force the libsql import inside get_connection to fail.
    import builtins

    real_import = builtins.__import__

    def _fake_import(name, *args, **kwargs):
        if name == "libsql":
            raise ImportError("simulated: libsql not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(connection.TursoUnavailableError):
        connection.get_connection()
