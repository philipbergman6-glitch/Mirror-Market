"""Shared pytest fixtures.

- synthetic_ohlcv: 300 business days of deterministic OHLCV data — long
  enough for 200-day MA, MACD warm-up, and the 120-day Bollinger squeeze
  lookback. Seeded RNG so values are reproducible across runs.
- tmp_db: temp SQLite connection with the full schema applied. Useful
  when a test wants a raw connection.
- patched_db: temp SQLite file with all tables created, with
  pipeline.store and pipeline.query monkeypatched to use it. The fixture
  returns the path so tests can also poke at the DB directly via raw SQL.
- freshness_calls: captures main.py's save_freshness calls without a DB.

Two guards are always on, session-wide (see tests/_guards.py, ticket #84):
writing anywhere inside the repo's committed `data/history/` raises, and so
does any outbound socket connect. The network guard is opt-out per test with
`@pytest.mark.network`; the history guard has no opt-out.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import main
from pipeline import schema
from tests import _guards

# Every table the pipeline creates, taken from the schema module itself.
# Re-listing them here let a new table (Layer 24's sagis_supply_demand)
# exist in production while being absent from every test DB.
_SCHEMA_CONSTANTS = list(schema.ALL_SCHEMAS)


@pytest.fixture(scope="session", autouse=True)
def _isolation_guards():
    """Block writes to data/history/ and outbound connections, suite-wide.

    Session-scoped so the patches are installed once, before the first test
    body runs, and cover fixture setup as well as test bodies. They go up
    after collection, so a write at module-import time slips past them —
    as does anything a subprocess does. CI's post-suite `git status` check
    is the backstop for both.
    """
    with _guards.block_history_writes(), _guards.block_network():
        yield


@pytest.fixture(autouse=True)
def _network_marker(request: pytest.FixtureRequest):
    """Let `@pytest.mark.network` through the network guard, for that test only."""
    allowed = request.node.get_closest_marker("network") is not None
    _guards.set_network_allowed(allowed)
    try:
        yield
    finally:
        _guards.set_network_allowed(False)


@pytest.fixture
def synthetic_ohlcv() -> pd.DataFrame:
    """300 business days of seeded random-walk OHLCV.

    Variance is large enough to exercise rolling indicators but the
    seed pins the values so tests are deterministic. Index is a
    DatetimeIndex named "Date".
    """
    rng = np.random.default_rng(seed=42)
    n = 300
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    idx.name = "Date"

    returns = rng.normal(loc=0.0005, scale=0.012, size=n)
    close = 100.0 * np.exp(np.cumsum(returns))

    intraday_range = np.abs(rng.normal(loc=0.0, scale=0.008, size=n)) * close
    high = close + intraday_range
    low = close - intraday_range
    open_ = close - rng.normal(loc=0.0, scale=0.004, size=n) * close
    volume = rng.integers(low=50_000, high=200_000, size=n).astype(float)

    return pd.DataFrame(
        {
            "Open": open_,
            "High": high,
            "Low": low,
            "Close": close,
            "Volume": volume,
        },
        index=idx,
    )


@pytest.fixture
def tmp_db(tmp_path: Path) -> sqlite3.Connection:
    """Temp on-disk SQLite with every project table created.

    Returns a raw sqlite3.Connection for tests that prefer to manage
    connection lifecycle themselves.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in _SCHEMA_CONSTANTS:
        conn.execute(ddl)
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def freshness_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """Capture every save_freshness call main.py makes, without touching a DB.

    Each entry is {"layer", "rows", "status"}. `_HARD_FAILURES` is module
    state on main, so it is cleared either side of the test — the freshness
    row and the hard-failure set are the two things the layer-grading tests
    assert on, and they have to be read together.

    Key coverage (#182) is accepted and dropped: it describes a run, it
    never changes the verdict these tests pin, and several of them compare
    the captured dict with `==`. tests/test_layer_coverage.py shadows this
    fixture with one that keeps the coverage pair.
    """
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success",
                 keys_returned=None, keys_expected=None):
        calls.append({"layer": layer_name, "rows": rows_fetched, "status": status})

    monkeypatch.setattr(main, "save_freshness", _capture)
    main._HARD_FAILURES.clear()
    yield calls
    main._HARD_FAILURES.clear()


@pytest.fixture
def patched_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite file wired into pipeline.store and pipeline.query.

    Creates a fresh DB with every table in schema.ALL_SCHEMAS, then
    monkeypatches
    `get_connection`, `DB_PATH`, `STORAGE_DIR`, and `is_cloud` in both
    pipeline modules so save_* / read_* functions transparently target
    the temp DB. Returns the DB path so a test can also issue raw SQL.
    """
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in _SCHEMA_CONSTANTS:
        conn.execute(ddl)
    conn.commit()
    conn.close()

    def _connect() -> sqlite3.Connection:
        return sqlite3.connect(str(db_path))

    for module in ("pipeline.store", "pipeline.query"):
        monkeypatch.setattr(f"{module}.get_connection", _connect)
        monkeypatch.setattr(f"{module}.DB_PATH", str(db_path))
        monkeypatch.setattr(f"{module}.is_cloud", lambda: False)
    monkeypatch.setattr("pipeline.store.STORAGE_DIR", str(tmp_path))

    return db_path
