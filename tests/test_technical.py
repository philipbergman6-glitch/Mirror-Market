"""Smoke tests for analysis.technical.

One happy-path test per public function plus a non-mutation invariant.
Math checks use simple constructed series (constant, monotonic,
alternating) where the expected indicator value is unambiguous.

Failures should be treated as findings against the indicator function,
not as test bugs.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from analysis.technical import (
    add_moving_averages,
    add_price_changes,
    add_rsi,
    calculate_bollinger,
    calculate_macd,
    calculate_volatility,
    compute_all_technicals,
)

# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _constant_close(value: float = 100.0, n: int = 250) -> pd.DataFrame:
    """n rows with Close == value, business-day index."""
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    return pd.DataFrame({"Close": [value] * n}, index=idx)


def _monotonic_close(start: float, step: float, n: int) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    return pd.DataFrame({"Close": [start + i * step for i in range(n)]}, index=idx)


def _alternating_close(low: float, high: float, n: int) -> pd.DataFrame:
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    values = [low if i % 2 == 0 else high for i in range(n)]
    return pd.DataFrame({"Close": values}, index=idx)


# ---------------------------------------------------------------------------
# add_moving_averages
# ---------------------------------------------------------------------------


def test_add_moving_averages_constant_close():
    df = _constant_close(value=100.0, n=250)
    out = add_moving_averages(df)

    for col in ("MA_20", "MA_50", "MA_200"):
        assert col in out.columns

    # Once the window is filled, the MA of a constant series equals the constant.
    assert out["MA_20"].iloc[-1] == pytest.approx(100.0)
    assert out["MA_50"].iloc[-1] == pytest.approx(100.0)
    assert out["MA_200"].iloc[-1] == pytest.approx(100.0)

    # Warm-up rows are NaN.
    assert out["MA_200"].iloc[198] != out["MA_200"].iloc[198]  # NaN check


def test_add_moving_averages_custom_windows():
    df = _constant_close(value=50.0, n=30)
    out = add_moving_averages(df, windows=[5, 10])
    assert "MA_5" in out.columns
    assert "MA_10" in out.columns
    assert "MA_20" not in out.columns
    assert out["MA_5"].iloc[-1] == pytest.approx(50.0)


def test_add_moving_averages_does_not_mutate_input():
    df = _constant_close(value=100.0, n=30)
    before = df.copy(deep=True)
    add_moving_averages(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# add_rsi
# ---------------------------------------------------------------------------


def test_add_rsi_monotonic_up_converges_to_100():
    df = _monotonic_close(start=100.0, step=1.0, n=30)
    out = add_rsi(df)

    assert "RSI" in out.columns
    # All gains, no losses → RSI saturates at 100.
    assert out["RSI"].iloc[-1] == pytest.approx(100.0)


def test_add_rsi_monotonic_down_converges_to_0():
    df = _monotonic_close(start=200.0, step=-1.0, n=30)
    out = add_rsi(df)

    assert out["RSI"].iloc[-1] == pytest.approx(0.0)


def test_add_rsi_alternating_hovers_near_50():
    df = _alternating_close(low=100.0, high=101.0, n=40)
    out = add_rsi(df)

    # Balanced gains/losses ⇒ RSI in the neighbourhood of 50.
    last = out["RSI"].iloc[-1]
    assert 40 <= last <= 60


def test_add_rsi_warmup_rows_are_nan():
    df = _monotonic_close(start=100.0, step=1.0, n=30)
    out = add_rsi(df)
    # period=14 ⇒ first valid RSI is at row 14; rows 0..13 NaN.
    assert out["RSI"].iloc[:14].isna().all()
    assert pd.notna(out["RSI"].iloc[14])


def test_add_rsi_does_not_mutate_input():
    df = _monotonic_close(start=100.0, step=1.0, n=30)
    before = df.copy(deep=True)
    add_rsi(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# add_price_changes
# ---------------------------------------------------------------------------


def test_add_price_changes_known_step():
    # 100, 110, 100, 100, 100, 121 → +10%, -9.09%, 0, 0, 0, +21%
    idx = pd.date_range("2024-01-01", periods=6, freq="B")
    df = pd.DataFrame({"Close": [100.0, 110.0, 100.0, 100.0, 100.0, 121.0]}, index=idx)

    out = add_price_changes(df)

    assert "daily_pct_change" in out.columns
    assert "weekly_pct_change" in out.columns

    # First row has no prior close → NaN.
    assert pd.isna(out["daily_pct_change"].iloc[0])
    assert out["daily_pct_change"].iloc[1] == pytest.approx(10.0)

    # Weekly needs 5 prior closes; first 5 rows NaN.
    assert out["weekly_pct_change"].iloc[:5].isna().all()
    assert out["weekly_pct_change"].iloc[5] == pytest.approx(21.0)


def test_add_price_changes_does_not_mutate_input():
    df = _monotonic_close(start=100.0, step=1.0, n=10)
    before = df.copy(deep=True)
    add_price_changes(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# calculate_macd
# ---------------------------------------------------------------------------


def test_calculate_macd_constant_close_is_zero():
    df = _constant_close(value=100.0, n=100)
    out = calculate_macd(df)

    for col in ("MACD", "MACD_Signal", "MACD_Histogram"):
        assert col in out.columns

    # On a flat series the EMAs are equal, so MACD, signal, and histogram are 0.
    assert out["MACD"].iloc[-1] == pytest.approx(0.0)
    assert out["MACD_Signal"].iloc[-1] == pytest.approx(0.0)
    assert out["MACD_Histogram"].iloc[-1] == pytest.approx(0.0)


def test_calculate_macd_histogram_equals_macd_minus_signal():
    df = _monotonic_close(start=100.0, step=0.5, n=80)
    out = calculate_macd(df)
    diff = out["MACD"] - out["MACD_Signal"]
    pdt.assert_series_equal(out["MACD_Histogram"], diff, check_names=False)


def test_calculate_macd_does_not_mutate_input():
    df = _constant_close(value=100.0, n=50)
    before = df.copy(deep=True)
    calculate_macd(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# calculate_bollinger
# ---------------------------------------------------------------------------


def test_calculate_bollinger_constant_close_zero_width():
    df = _constant_close(value=100.0, n=50)
    out = calculate_bollinger(df)

    for col in ("BB_Upper", "BB_Middle", "BB_Lower", "BB_Width"):
        assert col in out.columns

    # Constant prices ⇒ std = 0 ⇒ bands collapse onto middle, width = 0.
    last = out.iloc[-1]
    assert last["BB_Middle"] == pytest.approx(100.0)
    assert last["BB_Upper"] == pytest.approx(100.0)
    assert last["BB_Lower"] == pytest.approx(100.0)
    assert last["BB_Width"] == pytest.approx(0.0)


def test_calculate_bollinger_width_formula():
    # Known sequence: BB_Width = (Upper - Lower) / Middle * 100 = 4 * std / mean * 100.
    rng = np.random.default_rng(seed=1)
    idx = pd.date_range("2024-01-01", periods=40, freq="B")
    df = pd.DataFrame({"Close": 100.0 + rng.normal(0, 2, size=40)}, index=idx)
    out = calculate_bollinger(df)
    expected_width = (out["BB_Upper"] - out["BB_Lower"]) / out["BB_Middle"] * 100
    pdt.assert_series_equal(out["BB_Width"], expected_width, check_names=False)


def test_calculate_bollinger_does_not_mutate_input():
    df = _constant_close(value=100.0, n=30)
    before = df.copy(deep=True)
    calculate_bollinger(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# calculate_volatility
# ---------------------------------------------------------------------------


def test_calculate_volatility_constant_close_is_zero():
    df = _constant_close(value=100.0, n=80)
    out = calculate_volatility(df)

    assert "HV_20" in out.columns
    assert "HV_60" in out.columns
    # Constant prices ⇒ daily returns 0 ⇒ HV 0 (after warm-up).
    assert out["HV_20"].iloc[-1] == pytest.approx(0.0)
    assert out["HV_60"].iloc[-1] == pytest.approx(0.0)


def test_calculate_volatility_known_returns():
    # Daily return alternates exactly ±0.01. For a window of 20 (10 +0.01,
    # 10 −0.01), the *sample* std (ddof=1, what pandas rolling uses) is
    # 0.01 * sqrt(20/19), so HV_20 = 0.01 * sqrt(20/19) * sqrt(252) * 100.
    n = 30
    closes = [100.0]
    sign = 1
    for _ in range(n - 1):
        closes.append(closes[-1] * (1.0 + sign * 0.01))
        sign *= -1
    idx = pd.date_range("2024-01-01", periods=n, freq="B")
    df = pd.DataFrame({"Close": closes}, index=idx)

    out = calculate_volatility(df, windows=[20])
    expected = 0.01 * (20 / 19) ** 0.5 * (252 ** 0.5) * 100
    assert out["HV_20"].iloc[-1] == pytest.approx(expected, rel=1e-6)


def test_calculate_volatility_does_not_mutate_input():
    df = _constant_close(value=100.0, n=80)
    before = df.copy(deep=True)
    calculate_volatility(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# compute_all_technicals
# ---------------------------------------------------------------------------


EXPECTED_TECHNICAL_COLUMNS = [
    "MA_20",
    "MA_50",
    "MA_200",
    "RSI",
    "daily_pct_change",
    "weekly_pct_change",
    "MACD",
    "MACD_Signal",
    "MACD_Histogram",
    "BB_Upper",
    "BB_Middle",
    "BB_Lower",
    "BB_Width",
    "HV_20",
    "HV_60",
]


def test_compute_all_technicals_adds_expected_columns(synthetic_ohlcv):
    out = compute_all_technicals(synthetic_ohlcv)
    for col in EXPECTED_TECHNICAL_COLUMNS:
        assert col in out.columns, f"missing {col}"


def test_compute_all_technicals_does_not_mutate_input(synthetic_ohlcv):
    before = synthetic_ohlcv.copy(deep=True)
    compute_all_technicals(synthetic_ohlcv)
    pdt.assert_frame_equal(synthetic_ohlcv, before)
