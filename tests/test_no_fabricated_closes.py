"""Regression coverage for B1 (#307): no fabricated closes, ever.

Runs the real path a partial bar would travel — clean_ohlcv →
save_price_data → prices table → analysis.loaders.load_prices →
compute_all_technicals — and asserts the partial bar's date never
reaches `prices.Close` or any derived metric. Under the removed
ffill, the fabricated close survived this whole path as a real-looking
0% session in RSI/MACD/daily-change.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis import loaders
from pipeline import query, store
from pipeline.clean import clean_ohlcv


@pytest.fixture(autouse=True)
def _fresh_loader_cache():
    loaders.clear_loader_cache()
    yield
    loaders.clear_loader_cache()


def _raw_frame_with_partial_bar(n: int = 30) -> tuple[pd.DataFrame, pd.Timestamp]:
    """A steady uptrend where bar #20 printed everything except Close."""
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    closes = np.linspace(100.0, 110.0, n)
    df = pd.DataFrame(
        {
            "Open": closes - 0.5,
            "High": closes + 0.5,
            "Low": closes - 1.0,
            "Close": closes,
            "Volume": [1000.0] * n,
        },
        index=idx,
    )
    partial_date = idx[20]
    df.iloc[20, df.columns.get_loc("Close")] = np.nan
    return df, partial_date


def test_partial_bar_never_reaches_prices_close_or_derived_metrics(patched_db):
    raw, partial_date = _raw_frame_with_partial_bar()

    store.save_price_data("Soybeans", clean_ohlcv(raw, label="Soybeans"))

    # Store: the partial bar's date is not in the prices table at all.
    stored = query.read_prices("Soybeans")
    assert partial_date.strftime("%Y-%m-%d") not in set(stored["Date"])
    assert stored["Close"].notna().all()

    # Analysis/render: the canonical loader (what the briefing and the site
    # blocks consume) carries neither the date nor any zero-delta ghost of it.
    frames = loaders.load_prices(with_technicals=True)
    df = frames["Soybeans"]
    assert partial_date not in df.index
    assert df["Close"].notna().all()
    assert not df[["Open", "High", "Low"]].isna().any().any()

    # No fabricated 0% session anywhere: every remaining day moved.
    deltas = df["Close"].diff().dropna()
    assert (deltas != 0.0).all()

    # Derived metrics computed where real observations exist, none on the
    # dropped date, and the daily change spans the gap honestly.
    assert partial_date not in df.dropna(subset=["RSI"]).index
    assert df["daily_pct_change"].dropna().gt(0).all()
