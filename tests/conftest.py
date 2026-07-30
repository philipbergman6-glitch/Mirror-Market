"""Shared pytest fixtures.

- synthetic_ohlcv: 300 business days of deterministic OHLCV data — long
  enough for 200-day MA, MACD warm-up, and the 120-day Bollinger squeeze
  lookback. Seeded RNG so values are reproducible across runs.
- tmp_db: temp SQLite connection with the full schema applied. Useful
  when a test wants a raw connection.
- patched_db: temp SQLite file with all tables created, with
  pipeline.store and pipeline.query monkeypatched to use it. The fixture
  returns the path so tests can also poke at the DB directly via raw SQL.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline import schema

_SCHEMA_CONSTANTS = [
    schema._CREATE_PRICES,
    schema._CREATE_ECONOMIC,
    schema._CREATE_USDA,
    schema._CREATE_COT,
    schema._CREATE_WEATHER,
    schema._CREATE_PSD,
    schema._CREATE_CURRENCIES,
    schema._CREATE_WORLDBANK,
    schema._CREATE_DCE_FUTURES,
    schema._CREATE_CROP_PROGRESS,
    schema._CREATE_EXPORT_SALES,
    schema._CREATE_FORWARD_CURVE,
    schema._CREATE_WASDE,
    schema._CREATE_INSPECTIONS,
    schema._CREATE_INSPECTION_PORT_FLOWS,
    schema._CREATE_EIA_ENERGY,
    schema._CREATE_BRAZIL_ESTIMATES,
    schema._CREATE_INDIA_DOMESTIC,
    schema._CREATE_BRAZIL_SPOT,
    schema._CREATE_SAFEX,
    schema._CREATE_DATA_FRESHNESS,
    schema._CREATE_COMMODITY_FRESHNESS,
    schema._CREATE_BRIEFINGS,
]


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
    return conn


@pytest.fixture
def patched_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp SQLite file wired into pipeline.store and pipeline.query.

    Creates a fresh DB with all 21 tables, then monkeypatches
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
