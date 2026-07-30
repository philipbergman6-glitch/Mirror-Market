"""Tests for returns-based correlations and detrended seasonal analysis.

Correlations must run on daily returns, not price levels — two independently
trending random walks show spuriously high level correlation but ~zero return
correlation. Seasonal analysis must expose the detrended (*_dev_pct) fields
alongside the level-based back-compat fields consumed by the dashboard.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from analysis.correlations import (
    MIN_RETURN_OBS,
    commodity_correlation_matrix,
    commodity_vs_currency,
    rolling_correlation,
)
from analysis.seasonal import current_vs_seasonal, monthly_seasonal


def _trending_df(seed: int, n: int = 500, drift: float = 0.001) -> pd.DataFrame:
    """Upward-trending random walk with independent noise per seed."""
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2022-01-03", periods=n, freq="B")
    close = 100.0 * np.exp(np.cumsum(rng.normal(drift, 0.01, n)))
    return pd.DataFrame({"Close": close}, index=idx)


# ---------------------------------------------------------------------------
# commodity_correlation_matrix
# ---------------------------------------------------------------------------

def test_matrix_uses_returns_not_levels():
    """Two independent trending walks: levels correlate spuriously (>0.8),
    returns must not."""
    a, b = _trending_df(seed=1, drift=0.004), _trending_df(seed=2, drift=0.004)
    level_corr = a["Close"].corr(b["Close"])
    assert abs(level_corr) > 0.6  # the trap this change removes

    matrix = commodity_correlation_matrix({"A": a, "B": b})
    assert abs(matrix.loc["A", "B"]) < 0.3


def test_matrix_perfectly_comoving_returns_is_one():
    a = _trending_df(seed=3)
    b = pd.DataFrame({"Close": a["Close"] * 2.5}, index=a.index)
    matrix = commodity_correlation_matrix({"A": a, "B": b})
    assert matrix.loc["A", "B"] == pytest.approx(1.0)


def test_matrix_below_min_periods_is_nan():
    a = _trending_df(seed=4, n=MIN_RETURN_OBS - 10)
    b = _trending_df(seed=5, n=MIN_RETURN_OBS - 10)
    matrix = commodity_correlation_matrix({"A": a, "B": b})
    assert pd.isna(matrix.loc["A", "B"])


def test_matrix_fewer_than_two_series_is_empty():
    assert commodity_correlation_matrix({"A": _trending_df(seed=6)}).empty
    assert commodity_correlation_matrix({}).empty


# ---------------------------------------------------------------------------
# commodity_vs_currency
# ---------------------------------------------------------------------------

def test_vs_currency_returns_based():
    a, b = _trending_df(seed=7), _trending_df(seed=8)
    r = commodity_vs_currency(a, b, "A", "B")
    assert abs(r) < 0.3
    # inverted returns => strongly negative
    inv = pd.DataFrame({"Close": 1.0 / a["Close"]}, index=a.index)
    assert commodity_vs_currency(a, inv, "A", "InvA") < -0.9


def test_vs_currency_insufficient_overlap_is_nan():
    a = _trending_df(seed=9, n=MIN_RETURN_OBS - 5)
    b = _trending_df(seed=10, n=MIN_RETURN_OBS - 5)
    assert pd.isna(commodity_vs_currency(a, b))
    assert pd.isna(commodity_vs_currency(pd.DataFrame(), b))


# ---------------------------------------------------------------------------
# rolling_correlation
# ---------------------------------------------------------------------------

def test_rolling_correlation_on_returns():
    a = _trending_df(seed=11)
    b = pd.DataFrame({"Close": a["Close"] * 3.0}, index=a.index)
    rc = rolling_correlation(a["Close"], b["Close"], window=60)
    assert not rc.empty
    assert rc.dropna().iloc[-1] == pytest.approx(1.0)


def test_rolling_correlation_short_series_is_empty():
    a = _trending_df(seed=12, n=30)
    b = _trending_df(seed=13, n=30)
    assert rolling_correlation(a["Close"], b["Close"], window=60).empty


# ---------------------------------------------------------------------------
# seasonal — detrended fields
# ---------------------------------------------------------------------------

def _seasonal_df(years: int = 8, amp: float = 0.10, trend: float = 0.08) -> pd.DataFrame:
    """Daily series with a June peak (multiplicative sine) on a rising trend."""
    idx = pd.date_range("2016-01-01", periods=years * 365, freq="D")
    t = np.arange(len(idx)) / 365.0
    seasonal = 1.0 + amp * np.sin(2 * np.pi * (idx.dayofyear - 80) / 365.0)
    close = 100.0 * (1 + trend) ** t * seasonal
    return pd.DataFrame({"Close": close}, index=idx)


def test_monthly_seasonal_has_detrended_column_and_backcompat_columns():
    out = monthly_seasonal(_seasonal_df())
    assert set(["month", "avg_close", "min_close", "max_close", "n_years", "avg_dev_pct"]) <= set(
        out.columns
    )
    # Sine peaks near June (day ~171) and troughs near December: the
    # detrended profile must recover that shape.
    by_month = out.set_index("month")["avg_dev_pct"]
    assert by_month.idxmax() in (5, 6, 7)
    assert by_month.idxmin() in (11, 12, 1)


def test_monthly_seasonal_flat_across_months_when_no_seasonality():
    """With a steady trend and NO seasonality, every month carries the same
    constant trend offset (close sits ~trend/2 above its trailing 12m mean),
    so the cross-month *spread* of avg_dev_pct must be ~zero — no month looks
    seasonally special. (The offset itself cancels in detrended_delta_pct.)"""
    out = monthly_seasonal(_seasonal_df(amp=0.0, trend=0.15))
    spread = out["avg_dev_pct"].max() - out["avg_dev_pct"].min()
    assert spread < 1.0


def test_current_vs_seasonal_returns_detrended_and_backcompat_keys():
    result = current_vs_seasonal(_seasonal_df())
    for key in ("current_price", "seasonal_avg", "deviation_pct", "assessment", "n_years"):
        assert key in result
    for key in ("current_dev_pct", "seasonal_dev_pct", "detrended_delta_pct"):
        assert key in result
        assert result[key] is not None
    assert "seasonal norm" in result["assessment"]


def test_current_vs_seasonal_empty_input():
    assert current_vs_seasonal(pd.DataFrame()) == {}


def test_current_vs_seasonal_short_history_is_empty():
    """< SEASONAL_MIN_YEARS_PER_MONTH years must still return {}."""
    idx = pd.date_range("2024-01-01", periods=400, freq="D")
    df = pd.DataFrame({"Close": np.linspace(100, 120, 400)}, index=idx)
    assert current_vs_seasonal(df) == {}
