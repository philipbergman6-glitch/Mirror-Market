"""Tests for pipeline.db_snapshot and scripts/db_snapshot.py (the R2 seed).

No network: an in-memory ObjectStore stands in for R2. The real R2Store is
exercised only for construction, which boto3 does offline.
"""

from __future__ import annotations

import gzip
import io
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest

from pipeline import db_snapshot
from pipeline.db_snapshot import (
    LATEST_KEY,
    META_SHA256,
    SnapshotError,
    clear_history_tables,
    config_from_env,
    daily_key,
    pull,
    push,
    verify_sqlite,
)
from pipeline.history import import_history

BUILT_AT = datetime(2026, 11, 16, 19, 42, tzinfo=timezone.utc)
FULL_ENV = {
    "R2_ACCOUNT_ID": "acct",
    "R2_ACCESS_KEY_ID": "key",
    "R2_SECRET_ACCESS_KEY": "secret",
    "R2_BUCKET": "mirror-market-db",
}


class MemoryStore:
    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}
        self.puts: list[str] = []

    def put(self, key: str, path: Path, metadata: dict[str, str]) -> None:
        self.objects[key] = (path.read_bytes(), dict(metadata))
        self.puts.append(key)

    def get(self, key: str, path: Path) -> dict[str, str] | None:
        if key not in self.objects:
            return None
        body, metadata = self.objects[key]
        path.write_bytes(body)
        return dict(metadata)


def _make_db(path: Path) -> Path:
    """A seed-shaped DB: one self-healing table, two history tables."""
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE prices (Date TEXT PRIMARY KEY, Close REAL)")
        conn.execute("INSERT INTO prices VALUES ('2026-11-13', 1001.5)")
        conn.execute("CREATE TABLE data_freshness (layer_name TEXT PRIMARY KEY, status TEXT)")
        conn.execute("INSERT INTO data_freshness VALUES ('cbot', 'success')")
        conn.execute(
            "CREATE TABLE layer_partial_streak (layer_name TEXT PRIMARY KEY, streak INTEGER)"
        )
        conn.execute("INSERT INTO layer_partial_streak VALUES ('magyp', 2)")
    conn.close()
    return path


def _rows(path: Path, table: str) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(f"SELECT * FROM {table}").fetchall()
    finally:
        conn.close()


# ── configuration ────────────────────────────────────────────────────


def test_no_env_is_unconfigured() -> None:
    assert config_from_env({}) is None


def test_blank_env_is_unconfigured() -> None:
    assert config_from_env({k: "  " for k in FULL_ENV}) is None


def test_partial_env_is_an_error_not_a_fallback() -> None:
    env = dict(FULL_ENV, R2_SECRET_ACCESS_KEY="")
    with pytest.raises(SnapshotError, match="R2_SECRET_ACCESS_KEY"):
        config_from_env(env)


def test_full_env_builds_endpoint_and_hides_secret() -> None:
    cfg = config_from_env(FULL_ENV)
    assert cfg is not None
    assert cfg.endpoint == "https://acct.r2.cloudflarestorage.com"
    assert "secret" not in repr(cfg)


def test_daily_key_is_utc_dated_and_needs_a_timezone() -> None:
    assert daily_key(BUILT_AT, "42") == "daily/2026-11-16/run-42.db.gz"
    with pytest.raises(SnapshotError):
        daily_key(datetime(2026, 11, 16), "42")


# ── verification ─────────────────────────────────────────────────────


def test_verify_refuses_the_zero_byte_file(tmp_path: Path) -> None:
    empty = tmp_path / "mirror_market.db"
    empty.write_bytes(b"")
    with pytest.raises(SnapshotError, match="no tables"):
        verify_sqlite(empty)


def test_verify_refuses_garbage(tmp_path: Path) -> None:
    junk = tmp_path / "junk.db"
    junk.write_bytes(b"not a database at all" * 100)
    with pytest.raises(SnapshotError):
        verify_sqlite(junk)


def test_verify_counts_tables(tmp_path: Path) -> None:
    assert verify_sqlite(_make_db(tmp_path / "a.db")) == 3


def test_clear_history_tables_keeps_self_healing_rows(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "a.db")
    cleared = clear_history_tables(db)
    assert cleared == ["data_freshness", "layer_partial_streak"]
    assert _rows(db, "data_freshness") == []
    assert _rows(db, "layer_partial_streak") == []
    assert _rows(db, "prices") == [("2026-11-13", 1001.5)]


# ── push / pull ──────────────────────────────────────────────────────


def test_push_writes_dated_copy_before_latest(tmp_path: Path) -> None:
    store = MemoryStore()
    result = push(_make_db(tmp_path / "a.db"), store, run_id="42", built_at=BUILT_AT)
    assert store.puts == ["daily/2026-11-16/run-42.db.gz", LATEST_KEY]
    assert result.keys == tuple(store.puts)
    _, meta = store.objects[LATEST_KEY]
    assert meta[META_SHA256] == result.sha256
    assert meta["run-id"] == "42"


def test_push_refuses_an_unusable_database(tmp_path: Path) -> None:
    empty = tmp_path / "empty.db"
    empty.write_bytes(b"")
    store = MemoryStore()
    with pytest.raises(SnapshotError):
        push(empty, store, run_id="42", built_at=BUILT_AT)
    assert store.puts == []


def test_round_trip_restores_and_empties_history_tables(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)

    target = tmp_path / "storage" / "mirror_market.db"
    result = pull(target, store)

    assert result.state == "restored"
    assert result.metadata["run-id"] == "42"
    assert set(result.cleared_tables) == {"data_freshness", "layer_partial_streak"}
    assert _rows(target, "prices") == [("2026-11-13", 1001.5)]
    assert _rows(target, "layer_partial_streak") == []


def test_pull_with_nothing_pushed_is_no_seed(tmp_path: Path) -> None:
    target = tmp_path / "mirror_market.db"
    assert pull(target, MemoryStore()).state == "no_seed"
    assert not target.exists()


def test_pull_refuses_to_overwrite(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    existing = _make_db(tmp_path / "mirror_market.db")
    with pytest.raises(SnapshotError, match="already exists"):
        pull(existing, store)


def test_checksum_mismatch_places_nothing(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    body, meta = store.objects[LATEST_KEY]
    store.objects[LATEST_KEY] = (body + b"x", meta)
    target = tmp_path / "mirror_market.db"
    with pytest.raises(SnapshotError, match="sha256 mismatch"):
        pull(target, store)
    assert not target.exists()


def test_missing_checksum_metadata_is_refused(tmp_path: Path) -> None:
    store = MemoryStore()
    push(_make_db(tmp_path / "src.db"), store, run_id="42", built_at=BUILT_AT)
    body, meta = store.objects[LATEST_KEY]
    store.objects[LATEST_KEY] = (body, {k: v for k, v in meta.items() if k != META_SHA256})
    with pytest.raises(SnapshotError, match="no sha256"):
        pull(tmp_path / "mirror_market.db", store)


def test_corrupt_payload_with_matching_checksum_is_refused(tmp_path: Path) -> None:
    gz = tmp_path / "bad.gz"
    with gzip.open(gz, "wb") as fh:
        fh.write(b"definitely not sqlite" * 50)
    store = MemoryStore()
    store.put(
        LATEST_KEY,
        gz,
        {META_SHA256: db_snapshot._sha256(gz), "built-at": BUILT_AT.isoformat()},
    )
    target = tmp_path / "mirror_market.db"
    with pytest.raises(SnapshotError):
        pull(target, store)
    assert not target.exists()


# ── the trap this design exists to avoid ─────────────────────────────


def test_row_removed_after_the_seed_is_not_resurrected(
    patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A streak row in the seed, deleted by a later run's CSV, stays deleted.

    import_history() only inserts, so without clear_history_tables() the
    seed's copy of an authoritative-state row would survive the CSV import.
    """
    seed_src = tmp_path / "seed_src.db"
    seed_src.write_bytes(patched_db.read_bytes())
    conn = sqlite3.connect(str(seed_src))
    with conn:
        conn.execute(
            "INSERT INTO layer_partial_streak "
            "(layer_name, consecutive_runs, missing_keys, first_seen, last_seen) "
            "VALUES ('magyp', 2, '[]', '2026-11-12', '2026-11-13')"
        )
    conn.close()
    store = MemoryStore()
    push(seed_src, store, run_id="42", built_at=BUILT_AT)

    # A later run reset the streak: the committed CSV has the header only.
    history_dir = tmp_path / "history"
    history_dir.mkdir()
    cols = [
        r[1]
        for r in sqlite3.connect(str(patched_db)).execute(
            "PRAGMA table_info(layer_partial_streak)"
        )
    ]
    (history_dir / "layer_partial_streak.csv").write_text(",".join(cols) + "\n")

    patched_db.unlink()
    pull(patched_db, store)
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(history_dir))
    monkeypatch.setattr(
        "pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db))
    )
    import_history()

    assert _rows(patched_db, "layer_partial_streak") == []


# ── CLI ──────────────────────────────────────────────────────────────


def _cli():
    import importlib.util

    path = Path(__file__).resolve().parent.parent / "scripts" / "db_snapshot.py"
    spec = importlib.util.spec_from_file_location("db_snapshot_cli", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in FULL_ENV:
        monkeypatch.delenv(key, raising=False)


def test_cli_pull_unconfigured_warns_and_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    assert _cli().main(["--db", str(tmp_path / "x.db"), "pull"]) == 0
    assert "::warning title=DB seed unconfigured::" in capsys.readouterr().out


def test_cli_push_unconfigured_warns_and_succeeds(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    db = _make_db(tmp_path / "a.db")
    rc = _cli().main(["--db", str(db), "push", "--run-id", "1", "--built-at", "2026-11-16T19:00:00Z"])
    assert rc == 0
    assert "::warning title=DB snapshot unconfigured::" in capsys.readouterr().out


def test_cli_partial_config_fails_loudly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _clear_env(monkeypatch)
    monkeypatch.setenv("R2_BUCKET", "mirror-market-db")
    assert _cli().main(["--db", str(tmp_path / "x.db"), "pull"]) == 1
    assert "::error title=DB seed restore failed::" in capsys.readouterr().out


def test_cli_round_trip_through_a_stubbed_store(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for key, value in FULL_ENV.items():
        monkeypatch.setenv(key, value)
    store = MemoryStore()
    monkeypatch.setattr(db_snapshot, "R2Store", lambda cfg: store)
    cli = _cli()
    src = _make_db(tmp_path / "src.db")
    assert cli.main(["--db", str(src), "push", "--run-id", "7", "--built-at", "2026-11-16T19:00:00Z"]) == 0
    target = tmp_path / "out" / "mirror_market.db"
    assert cli.main(["--db", str(target), "pull"]) == 0
    out = capsys.readouterr().out
    assert "::notice title=DB snapshot saved::" in out
    assert "::notice title=DB seed restored::Started from run 7" in out
    assert _rows(target, "prices") == [("2026-11-13", 1001.5)]


def test_push_includes_committed_wal_rows(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "source.db")
    store = MemoryStore()
    writer = sqlite3.connect(db)
    try:
        writer.execute("PRAGMA journal_mode=WAL")
        writer.execute("INSERT INTO prices VALUES ('2026-11-16', 1002.5)")
        writer.commit()
        push(db, store, run_id="1", built_at=BUILT_AT)
        restored = tmp_path / "restored.db"
        pull(restored, store, now=BUILT_AT)
        assert ("2026-11-16", 1002.5) in _rows(restored, "prices")
    finally:
        writer.close()


def test_push_refuses_private_matif_rows(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "source.db")
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE matif_futures (Close REAL)")
        conn.execute("INSERT INTO matif_futures VALUES (500)")
    store = MemoryStore()
    with pytest.raises(SnapshotError, match="matif_futures"):
        push(db, store, run_id="1", built_at=BUILT_AT)
    assert store.puts == []



def test_r2_get_reads_body_and_metadata_from_one_response(tmp_path: Path, monkeypatch) -> None:
    boto3 = pytest.importorskip("boto3")
    client = Mock()
    body = io.BytesIO(b"snapshot")
    client.get_object.return_value = {"Body": body, "Metadata": {"SHA256": "digest"}}
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    config = config_from_env(FULL_ENV)
    assert config is not None
    output = tmp_path / "seed.db.gz"

    assert db_snapshot.R2Store(config).get(LATEST_KEY, output) == {"sha256": "digest"}
    assert output.read_bytes() == b"snapshot"
    assert body.closed
    client.get_object.assert_called_once_with(Bucket=config.bucket, Key=LATEST_KEY)


def test_recover_preserves_authoritative_history(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "source.db")
    store = MemoryStore()
    archive, _ = push(db, store, run_id="1", built_at=BUILT_AT).keys
    restored = tmp_path / "recovered.db"

    result = pull(restored, store, key=archive, now=BUILT_AT, preserve_history=True)

    assert result.cleared_tables == ()
    assert _rows(restored, "data_freshness") == [("cbot", "success")]
    assert ("2026-11-13", 1001.5) in _rows(restored, "prices")


def test_r2_archive_is_create_only(tmp_path: Path, monkeypatch) -> None:
    boto3 = pytest.importorskip("boto3")
    from botocore.exceptions import ClientError

    client = Mock()
    client.put_object.side_effect = ClientError(
        {"Error": {"Code": "PreconditionFailed"}}, "PutObject"
    )
    monkeypatch.setattr(boto3, "client", lambda *args, **kwargs: client)
    config = config_from_env(FULL_ENV)
    assert config is not None
    path = tmp_path / "archive.gz"
    path.write_bytes(b"snapshot")
    key = daily_key(BUILT_AT, "1")

    with pytest.raises(SnapshotError, match="archive already exists"):
        db_snapshot.R2Store(config).put(key, path, {"sha256": "digest"})

    assert client.put_object.call_args.kwargs["IfNoneMatch"] == "*"
    client.upload_file.assert_not_called()


def test_push_archives_public_manifest_before_advancing_seed(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "source.db")
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"schema_version":1,"edition":{},"coverage":[]}')
    store = MemoryStore()

    archive, evidence, latest = push(
        db, store, run_id="1-attempt-2", built_at=BUILT_AT, public_manifest=manifest
    ).keys

    assert evidence == archive.removesuffix(".db.gz") + ".manifest.json.gz"
    assert store.puts == [archive, evidence, latest]
    assert latest == LATEST_KEY
    body, metadata = store.objects[evidence]
    assert gzip.decompress(body) == manifest.read_bytes()
    assert metadata[db_snapshot.META_RUN_ID] == "1-attempt-2"


def test_verify_latest_archive_requires_matching_dated_object(tmp_path: Path) -> None:
    db = _make_db(tmp_path / "source.db")
    store = MemoryStore()
    archive, _ = push(db, store, run_id="1", built_at=BUILT_AT).keys
    assert db_snapshot.verify_latest_archive(store, now=BUILT_AT) == archive

    del store.objects[archive]
    with pytest.raises(SnapshotError, match="dated archive recovery failed"):
        db_snapshot.verify_latest_archive(store, now=BUILT_AT)
