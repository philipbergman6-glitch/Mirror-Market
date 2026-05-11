"""Microbenchmark for pipeline.store save_* functions.

Measures rows/second for INSERT OR REPLACE writes on a temp SQLite.
Use this to compare before/after the executemany migration.

Usage:
    python scripts/benchmark_store.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from pipeline import schema, store  # noqa: E402

_DDLS = [
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
    schema._CREATE_EIA_ENERGY,
    schema._CREATE_BRAZIL_ESTIMATES,
    schema._CREATE_INDIA_DOMESTIC,
    schema._CREATE_BRAZIL_SPOT,
    schema._CREATE_SAFEX,
    schema._CREATE_DATA_FRESHNESS,
    schema._CREATE_COMMODITY_FRESHNESS,
]


@contextmanager
def temp_db(monkeypatch_target=store):
    """Spin up a fresh temp SQLite and rebind get_connection to it."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "bench.db")
        conn = sqlite3.connect(db_path)
        for ddl in _DDLS:
            conn.execute(ddl)
        conn.commit()
        conn.close()

        original_get = monkeypatch_target.get_connection
        original_is_cloud = monkeypatch_target.is_cloud
        monkeypatch_target.get_connection = lambda: sqlite3.connect(db_path)
        monkeypatch_target.is_cloud = lambda: False
        try:
            yield db_path
        finally:
            monkeypatch_target.get_connection = original_get
            monkeypatch_target.is_cloud = original_is_cloud


def _synthetic_ohlcv(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(42)
    idx = pd.date_range("2000-01-01", periods=n, freq="B")
    idx.name = "Date"
    close = 100.0 * np.exp(np.cumsum(rng.normal(0.0005, 0.012, n)))
    return pd.DataFrame(
        {
            "Open": close - rng.normal(0, 0.004, n) * close,
            "High": close + np.abs(rng.normal(0, 0.008, n)) * close,
            "Low": close - np.abs(rng.normal(0, 0.008, n)) * close,
            "Close": close,
            "Volume": rng.integers(50_000, 200_000, n).astype(float),
        },
        index=idx,
    )


def _synthetic_cot(n: int) -> pd.DataFrame:
    rng = np.random.default_rng(7)
    dates = pd.date_range("2000-01-04", periods=n, freq="W-TUE")
    return pd.DataFrame(
        {
            "Date": dates,
            "commercial_long": rng.integers(50_000, 200_000, n).astype(float),
            "commercial_short": rng.integers(40_000, 180_000, n).astype(float),
            "commercial_net": rng.integers(-50_000, 50_000, n).astype(float),
            "noncommercial_long": rng.integers(100_000, 300_000, n).astype(float),
            "noncommercial_short": rng.integers(80_000, 250_000, n).astype(float),
            "noncommercial_net": rng.integers(-50_000, 50_000, n).astype(float),
            "total_open_interest": rng.integers(500_000, 800_000, n).astype(float),
        }
    )


def _time(label: str, fn, repeats: int = 3) -> float:
    """Run fn() `repeats` times, return median seconds."""
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    median = sorted(times)[len(times) // 2]
    print(f"  {label:40s} {median*1000:8.1f} ms  (min={min(times)*1000:.1f} ms)")
    return median


def main():
    print("pipeline.store benchmark")
    print("=" * 60)

    n_rows = 10_000

    # --- save_price_data ---
    print(f"\nsave_price_data  (N={n_rows:,} OHLCV rows)")
    with temp_db():
        df = _synthetic_ohlcv(n_rows)
        t = _time(
            "INSERT OR REPLACE prices",
            lambda: store.save_price_data("Soybeans", df),
        )
        print(f"  {'→ throughput':40s} {n_rows / t:8.0f} rows/sec")

    # --- save_cot_data ---
    print(f"\nsave_cot_data  (N={n_rows:,} COT rows)")
    with temp_db():
        df = _synthetic_cot(n_rows)
        t = _time(
            "INSERT OR REPLACE cot",
            lambda: store.save_cot_data("Soybeans", df),
        )
        print(f"  {'→ throughput':40s} {n_rows / t:8.0f} rows/sec")

    print()


if __name__ == "__main__":
    main()
