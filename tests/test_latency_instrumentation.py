"""The stamps must survive the round trip, and must never be invented.

``latency.domain`` can only be as honest as what the pipeline records. These
tests run the real ``save_freshness`` against a real SQLite file and read it
back through the real ``read_freshness``, because the failure this whole
phase guards against — a fabricated stamp that makes a slow fetch look
instant — is exactly the kind that a mocked store would not show.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest

from latency import clock as run_clock

UTC = timezone.utc


@pytest.fixture
def db(tmp_path, monkeypatch):
    """A real database in tmp_path, with every module-level path repointed."""
    import config
    import pipeline.connection as connection
    import pipeline.store as store

    storage = tmp_path / "storage"
    storage.mkdir()
    db_path = str(storage / "test.db")
    monkeypatch.setattr(config, "DB_PATH", db_path)
    monkeypatch.setattr(config, "STORAGE_DIR", str(storage))
    monkeypatch.setattr(connection, "DB_PATH", db_path)
    monkeypatch.setattr(store, "DB_PATH", db_path)
    monkeypatch.setattr(store, "STORAGE_DIR", str(storage))
    import pipeline.query as query

    monkeypatch.setattr(query, "DB_PATH", db_path)
    store.init_database()
    return db_path


def _read(layer: str):
    from pipeline.query import read_freshness

    frame = read_freshness()
    rows = frame.set_index("layer_name").to_dict("index")
    return rows[layer]


def test_a_clock_is_persisted_as_four_distinct_stamps(db):
    from pipeline.store import save_freshness

    run_clock.reset()
    clock = run_clock.start("prices")
    clock.fetch_started_at = datetime(2026, 8, 18, 22, 30, 0, tzinfo=UTC)
    clock.fetch_completed_at = datetime(2026, 8, 18, 22, 32, 0, tzinfo=UTC)
    clock.stored_at = datetime(2026, 8, 18, 22, 33, 0, tzinfo=UTC)
    clock.observed(date(2026, 8, 18))

    save_freshness("prices", rows_fetched=10, clock=clock)

    row = _read("prices")
    assert str(row["observed_at"]).startswith("2026-08-18")
    assert str(row["fetch_started_at"]).startswith("2026-08-18 22:30")
    assert str(row["fetch_completed_at"]).startswith("2026-08-18 22:32")
    assert str(row["stored_at"]).startswith("2026-08-18 22:33")


def test_no_clock_records_nulls_rather_than_now(db):
    """The core rule: an unmeasured fetch must not look like an instant one."""
    import pandas as pd

    from pipeline.store import save_freshness

    run_clock.reset()
    save_freshness("prices", rows_fetched=10)

    row = _read("prices")
    assert pd.isna(row["fetch_started_at"])
    assert pd.isna(row["fetch_completed_at"])
    assert pd.isna(row["stored_at"])
    assert pd.isna(row["observed_at"])
    # last_attempt is still written — the layer did run.
    assert not pd.isna(row["last_attempt"])


def test_observation_is_recorded_on_a_failed_run_too(db):
    """A stale layer's newest observation is what sizes the hole."""
    from pipeline.store import save_freshness

    run_clock.reset()
    clock = run_clock.start("safex")
    clock.observed(date(2026, 8, 4))
    clock.fetch_completed_at = datetime(2026, 8, 19, 13, 0, tzinfo=UTC)

    save_freshness("safex", rows_fetched=2, status="stale", clock=clock)

    row = _read("safex")
    assert row["status"] == "stale"
    assert str(row["observed_at"]).startswith("2026-08-04")


def test_a_failed_status_preserves_the_prior_last_success(db):
    """Unchanged behaviour — pinned because the write path was rewritten."""
    import pandas as pd

    from pipeline.store import save_freshness

    run_clock.reset()
    save_freshness("prices", rows_fetched=10, status="success")
    first = _read("prices")["last_success"]
    save_freshness("prices", rows_fetched=0, status="failed")
    second = _read("prices")
    assert second["last_success"] == first
    assert second["status"] == "failed"
    assert not pd.isna(first)


def test_the_measured_chain_reads_back_off_the_database(db):
    """End to end: stamp it, store it, measure it."""
    from latency.domain import Verdict
    from latency.measure import measure
    from pipeline.store import save_freshness

    run_clock.reset()
    clock = run_clock.start("prices")
    clock.fetch_started_at = datetime(2026, 8, 18, 22, 30, tzinfo=UTC)
    clock.fetch_completed_at = datetime(2026, 8, 18, 22, 31, tzinfo=UTC)
    clock.stored_at = datetime(2026, 8, 18, 22, 32, tzinfo=UTC)
    clock.observed(date(2026, 8, 18))
    save_freshness("prices", rows_fetched=10, clock=clock)

    published = datetime(2026, 8, 18, 22, 45, tzinfo=UTC)
    measurements = {m.layer: m for m in measure(published_at=published)}
    prices = measurements["prices"]

    from datetime import timedelta

    assert prices.fetch_duration == timedelta(minutes=1)
    assert prices.processing == timedelta(minutes=1)
    assert prices.pipeline == timedelta(minutes=14)
    assert prices.pipeline_verdict is Verdict.MEETS
    # 18:15 UTC settlement -> 22:31 fetch
    assert prices.acquisition == timedelta(hours=4, minutes=16)


def test_clock_registry_is_reset_between_runs():
    run_clock.reset()
    run_clock.start("prices")
    assert run_clock.get("prices") is not None
    run_clock.reset()
    assert run_clock.get("prices") is None


def test_starting_a_layer_twice_replaces_rather_than_merges():
    """A retry must not inherit the previous attempt's fetch-completed stamp."""
    run_clock.reset()
    first = run_clock.start("prices")
    first.fetched()
    second = run_clock.start("prices")
    assert second is not first
    assert second.fetch_completed_at is None
    assert run_clock.get("prices") is second
