"""Smoke tests for pipeline.clean.

Two tests per cleaner:
    1. Happy path — typical input produces the expected transformation.
    2. Non-mutation — the input DataFrame/Series is unchanged after the call.

Plus a parametrized empty-input test across all DataFrame cleaners and a
dedicated empty-Series test for clean_fred_series.

These tests pin down the cleaners' documented contracts; failures should be
treated as findings against the cleaner, not as test bugs.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pandas.testing as pdt
import pytest

from pipeline.clean import (
    clean_brazil_spot,
    clean_conab,
    clean_cot,
    clean_dce_futures,
    clean_eia,
    clean_export_sales,
    clean_forward_curve,
    clean_fred_series,
    clean_india_domestic,
    clean_inspections,
    clean_ohlcv,
    clean_psd,
    clean_safex,
    clean_wasde,
    clean_weather,
    clean_worldbank,
)

# ---------------------------------------------------------------------------
# Empty-input contract — every cleaner short-circuits on an empty input.
# ---------------------------------------------------------------------------

DATAFRAME_CLEANERS = [
    clean_ohlcv,
    clean_cot,
    clean_weather,
    clean_psd,
    clean_dce_futures,
    clean_export_sales,
    clean_forward_curve,
    clean_wasde,
    clean_eia,
    clean_inspections,
    clean_conab,
    clean_india_domestic,
    clean_brazil_spot,
    clean_safex,
    clean_worldbank,
]


@pytest.mark.parametrize("cleaner", DATAFRAME_CLEANERS, ids=lambda c: c.__name__)
def test_empty_dataframe_returns_empty(cleaner):
    result = cleaner(pd.DataFrame())
    assert isinstance(result, pd.DataFrame)
    assert result.empty


def test_empty_series_returns_empty():
    result = clean_fred_series(pd.Series(dtype=float))
    assert isinstance(result, pd.Series)
    assert result.empty


# ---------------------------------------------------------------------------
# clean_ohlcv
# ---------------------------------------------------------------------------

def _make_ohlcv() -> pd.DataFrame:
    """Three rows with a gap and a fully-NaN row that should be dropped."""
    idx = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
    return pd.DataFrame(
        {
            "Open": [10.0, np.nan, 12.0],
            "High": [11.0, np.nan, 13.0],
            "Low": [9.5, np.nan, 11.5],
            "Close": [10.5, np.nan, 12.5],
            "Volume": [1000, 1100, 1200],
        },
        index=idx,
    )


def test_clean_ohlcv_happy_path():
    df = _make_ohlcv()
    out = clean_ohlcv(df)

    # Index is a DatetimeIndex named "Date"
    assert isinstance(out.index, pd.DatetimeIndex)
    assert out.index.name == "Date"

    # All-NaN price row was dropped before ffill, so length drops to 2
    assert len(out) == 2

    # No NaNs left in OHLC after the all-NaN row is dropped
    assert not out[["Open", "High", "Low", "Close"]].isna().any().any()


def test_clean_ohlcv_does_not_mutate_input():
    df = _make_ohlcv()
    before = df.copy(deep=True)
    clean_ohlcv(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# ffill(limit=3) + technical indicators: pin down the interaction.
#
# clean_ohlcv ffills small gaps (weekends, single-day holidays) up to 3 days.
# When the analysis layer then computes RSI/MACD on the cleaned frame, the
# filled bars contribute *zero-delta* days (Close = prior Close). This test
# locks that behaviour in so a future change to clean_ohlcv or technical.py
# doesn't silently shift how indicators respond to filled bars.
# ---------------------------------------------------------------------------


def test_clean_ohlcv_ffills_short_gaps_up_to_three_days():
    """A single-day NaN row is forward-filled from the prior bar."""
    idx = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"])
    df = pd.DataFrame(
        {
            "Open":   [10.0, np.nan, 12.0, 13.0],
            "High":   [11.0, np.nan, 13.0, 14.0],
            "Low":    [9.5,  np.nan, 11.5, 12.5],
            "Close":  [10.5, np.nan, 12.5, 13.5],
            "Volume": [1000, np.nan, 1200, 1300],
        },
        index=idx,
    )

    out = clean_ohlcv(df)

    # The all-NaN price row is dropped (not just ffilled), so length is 3.
    assert len(out) == 3
    # The remaining bars carry their real values; nothing fabricated.
    assert out["Close"].tolist() == [10.5, 12.5, 13.5]


def test_clean_ohlcv_drops_rows_with_all_ohlc_nan_before_ffill():
    """A row where every OHLC value is NaN is dropped, not ffilled.

    `dropna(how="all")` runs before `ffill(limit=3)`, so weekend/holiday
    bars that appear as all-NaN never make it to the ffill step. The risk
    of RSI being computed on a phantom weekend bar is therefore mitigated
    by the drop, not by the ffill — important to pin down because reversing
    the order would silently change RSI/MACD behaviour across gaps.
    """
    idx = pd.date_range("2026-01-01", periods=5, freq="B")
    df = pd.DataFrame(
        {
            "Open":   [10.0, np.nan, 11.0, 12.0, 13.0],
            "High":   [11.0, np.nan, 12.0, 13.0, 14.0],
            "Low":    [9.5,  np.nan, 10.5, 11.5, 12.5],
            "Close":  [10.5, np.nan, 11.5, 12.5, 13.5],
            "Volume": [1000, 1100,   1200, 1300, 1400],  # Volume present
        },
        index=idx,
    )
    out = clean_ohlcv(df)
    # The bar with all-NaN OHLC is dropped even though Volume was present.
    assert len(out) == 4
    assert out["Close"].tolist() == [10.5, 11.5, 12.5, 13.5]


def test_clean_ohlcv_ffill_followed_by_rsi_produces_zero_delta_on_filled_bar():
    """RSI computed on a partial-NaN ffilled bar treats it as zero-delta.

    When at least one OHLC value survives on a bar (so it's not dropped),
    `ffill(limit=3)` carries the prior values into the NaN cells. The
    analysis layer can't distinguish a partially-filled bar from a real
    one, so RSI/MACD/daily_pct_change will compute a 0% move across the
    filled Close. This is intentional — keeps index alignment stable —
    but pin it down so a future change doesn't silently shift behaviour.
    """
    from analysis.technical import add_rsi

    n = 30
    idx = pd.date_range("2026-01-01", periods=n, freq="B")
    closes = np.linspace(100.0, 110.0, n)  # steady uptrend
    df = pd.DataFrame(
        {
            "Open":   closes - 0.5,
            "High":   closes + 0.5,
            "Low":    closes - 1.0,
            "Close":  closes,
            "Volume": [1000.0] * n,
        },
        index=idx,
    )
    # Only Close on bar #20 is NaN — High/Low/Open survive. dropna keeps
    # the row (not all OHLC are NaN), then ffill restores Close from #19.
    df.iloc[20, df.columns.get_loc("Close")] = np.nan

    cleaned = clean_ohlcv(df)

    # Length unchanged — the partial-NaN row was kept and Close was ffilled.
    assert len(cleaned) == n
    assert cleaned["Close"].iloc[20] == pytest.approx(cleaned["Close"].iloc[19])

    # delta across 19 → 20 is exactly 0; RSI computes at bar 20 (we're past
    # the 14-day warm-up) and the indicator treats the filled bar as a
    # no-movement day. RSI does NOT skip filled bars.
    with_rsi = add_rsi(cleaned)
    delta_at_filled_bar = with_rsi["Close"].diff().iloc[20]
    assert delta_at_filled_bar == pytest.approx(0.0)
    assert pd.notna(with_rsi["RSI"].iloc[20])


# ---------------------------------------------------------------------------
# clean_fred_series
# ---------------------------------------------------------------------------

def _make_fred_series() -> pd.Series:
    idx = pd.to_datetime(["2026-01-03", "2026-01-01", "2026-01-02"])
    return pd.Series([np.nan, 1.0, np.nan], index=idx)


def test_clean_fred_series_happy_path():
    s = _make_fred_series()
    out = clean_fred_series(s)

    # Sorted ascending
    assert out.index.is_monotonic_increasing

    # Forward-fill carries 1.0 across the two NaN rows
    assert (out == 1.0).all()
    assert len(out) == 3


def test_clean_fred_series_does_not_mutate_input():
    s = _make_fred_series()
    before = s.copy(deep=True)
    clean_fred_series(s)
    pdt.assert_series_equal(s, before)


# ---------------------------------------------------------------------------
# clean_cot
# ---------------------------------------------------------------------------

def _make_cot() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-15", "2026-01-01", "2026-01-08"],
            "commercial_long": [100, 110, np.nan],
            "commercial_short": [50, 55, np.nan],
            "noncommercial_long": [200, 210, np.nan],
            "noncommercial_short": [80, 85, np.nan],
        }
    )


def test_clean_cot_happy_path():
    df = _make_cot()
    out = clean_cot(df)

    # Date is datetime, sorted ascending
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing

    # All-NaN position row was dropped
    assert len(out) == 2


def test_clean_cot_does_not_mutate_input():
    df = _make_cot()
    before = df.copy(deep=True)
    clean_cot(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_weather
# ---------------------------------------------------------------------------

def _make_weather() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-03", "2026-01-01", "2026-01-02"],
            "temp_max": [20.0, 18.0, 19.0],
            "precip_mm": [0.0, 5.0, 1.0],
        }
    )


def test_clean_weather_happy_path():
    df = _make_weather()
    out = clean_weather(df)
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing


def test_clean_weather_does_not_mutate_input():
    df = _make_weather()
    before = df.copy(deep=True)
    clean_weather(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_psd
# ---------------------------------------------------------------------------

def _make_psd() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "country": ["  Brazil ", "Argentina", "Paraguay"],
            "year": ["2024", "2025", "not-a-year"],
            "value": [120.0, 50.0, np.nan],
        }
    )


def test_clean_psd_happy_path():
    df = _make_psd()
    out = clean_psd(df)

    # Whitespace stripped
    assert "Brazil" in out["country"].tolist()
    # Year coerced to int
    assert pd.api.types.is_integer_dtype(out["year"])
    # Bad year row + NaN value row both dropped (1 row remains)
    assert len(out) == 2  # Brazil and Argentina survive


def test_clean_psd_does_not_mutate_input():
    df = _make_psd()
    before = df.copy(deep=True)
    clean_psd(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_dce_futures
# ---------------------------------------------------------------------------

def _make_dce() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "date": ["2026-01-02", "2026-01-01"],
            "open": [4000.0, 3950.0],
            "high": [4050.0, 4000.0],
            "low": [3980.0, 3920.0],
            "close": [4020.0, 3970.0],
            "volume": [100, 90],
            "hold": [500, 480],
            "settle": [4020.0, 3970.0],
        }
    )


def test_clean_dce_futures_happy_path():
    df = _make_dce()
    out = clean_dce_futures(df)

    # Columns renamed to project conventions
    for col in ("Date", "Open", "High", "Low", "Close", "Volume", "Open_Interest", "Settle"):
        assert col in out.columns

    # Date is datetime, sorted ascending
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing


def test_clean_dce_futures_does_not_mutate_input():
    df = _make_dce()
    before = df.copy(deep=True)
    clean_dce_futures(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_export_sales
# ---------------------------------------------------------------------------

def _make_export_sales() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "week_ending": ["2026-01-08", "2026-01-01", "2026-01-15"],
            "net_sales": ["500000", "400000", np.nan],
            "weekly_exports": ["300000", "250000", "200000"],
            "accumulated_exports": ["1000000", "750000", "1200000"],
            "outstanding_sales": ["2000000", "1800000", "1900000"],
        }
    )


def test_clean_export_sales_happy_path():
    df = _make_export_sales()
    out = clean_export_sales(df)

    assert pd.api.types.is_datetime64_any_dtype(out["week_ending"])
    assert out["week_ending"].is_monotonic_increasing
    # Row with NaN net_sales is dropped
    assert len(out) == 2
    # Numeric coercion happened
    assert pd.api.types.is_numeric_dtype(out["net_sales"])


def test_clean_export_sales_does_not_mutate_input():
    df = _make_export_sales()
    before = df.copy(deep=True)
    clean_export_sales(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_forward_curve
# ---------------------------------------------------------------------------

def _make_forward_curve() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "contract_month": ["2026-03-01", "2026-05-01", "2026-07-01", "2026-09-01"],
            "close": ["1100", "0", "-5", "1150"],
        }
    )


def test_clean_forward_curve_happy_path():
    df = _make_forward_curve()
    out = clean_forward_curve(df)

    # Zero and negative close rows dropped
    assert len(out) == 2
    assert (out["close"] > 0).all()
    assert pd.api.types.is_numeric_dtype(out["close"])


def test_clean_forward_curve_does_not_mutate_input():
    df = _make_forward_curve()
    before = df.copy(deep=True)
    clean_forward_curve(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_wasde
# ---------------------------------------------------------------------------

def _make_wasde() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "year": [2025, 2026],
            "Value": ["1,234", "5,678"],
        }
    )


def test_clean_wasde_happy_path():
    df = _make_wasde()
    out = clean_wasde(df)

    # Year coerced to string
    assert out["year"].dtype == object
    assert out["year"].tolist() == ["2025", "2026"]

    # Comma stripped, numeric
    assert pd.api.types.is_numeric_dtype(out["Value"])
    assert out["Value"].tolist() == [1234.0, 5678.0]


def test_clean_wasde_does_not_mutate_input():
    df = _make_wasde()
    before = df.copy(deep=True)
    clean_wasde(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_eia
# ---------------------------------------------------------------------------

def _make_eia() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-15", "2026-01-01", "2026-01-08"],
            "value": ["1.50", "1.45", np.nan],
        }
    )


def test_clean_eia_happy_path():
    df = _make_eia()
    out = clean_eia(df)

    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing
    assert pd.api.types.is_numeric_dtype(out["value"])
    # NaN row dropped
    assert len(out) == 2


def test_clean_eia_does_not_mutate_input():
    df = _make_eia()
    before = df.copy(deep=True)
    clean_eia(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_inspections
# ---------------------------------------------------------------------------

def _make_inspections() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "week_ending": ["2026-01-15", "2026-01-01", "2026-01-08"],
            "inspections_mt": ["500000", "450000", np.nan],
        }
    )


def test_clean_inspections_happy_path():
    df = _make_inspections()
    out = clean_inspections(df)

    assert pd.api.types.is_datetime64_any_dtype(out["week_ending"])
    assert out["week_ending"].is_monotonic_increasing
    assert pd.api.types.is_numeric_dtype(out["inspections_mt"])
    assert len(out) == 2


def test_clean_inspections_does_not_mutate_input():
    df = _make_inspections()
    before = df.copy(deep=True)
    clean_inspections(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_conab
# ---------------------------------------------------------------------------

def _make_conab() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "commodity": ["  Soybeans ", "Corn"],
            "crop_year": [" 2025/26 ", "2025/26"],
            "attribute": ["production ", " yield"],
            "source": ["CONAB", "CONAB "],
            "value": ["120.5", np.nan],
        }
    )


def test_clean_conab_happy_path():
    df = _make_conab()
    out = clean_conab(df)

    assert pd.api.types.is_numeric_dtype(out["value"])
    # NaN value row dropped
    assert len(out) == 1
    # Strings stripped
    assert out["commodity"].iloc[0] == "Soybeans"
    assert out["attribute"].iloc[0] == "production"


def test_clean_conab_does_not_mutate_input():
    df = _make_conab()
    before = df.copy(deep=True)
    clean_conab(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_india_domestic
# ---------------------------------------------------------------------------

def _make_india() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-02", "2026-01-01", "2026-01-03"],
            "Open": ["4000", "3950", "4020"],
            "High": ["4050", "4000", "4080"],
            "Low": ["3980", "3920", "4000"],
            "Close": ["4020", "0", "4060"],
            "Volume": ["100", "90", "110"],
        }
    )


def test_clean_india_domestic_happy_path():
    df = _make_india()
    out = clean_india_domestic(df)

    # Close == 0 row dropped
    assert len(out) == 2
    assert (out["Close"] > 0).all()
    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing


def test_clean_india_domestic_does_not_mutate_input():
    df = _make_india()
    before = df.copy(deep=True)
    clean_india_domestic(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_brazil_spot
# ---------------------------------------------------------------------------

def _make_brazil_spot() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-02", "2026-01-01", "2026-01-03"],
            "price_brl_mt": ["2000", "0", "2100"],
        }
    )


def test_clean_brazil_spot_happy_path():
    df = _make_brazil_spot()
    out = clean_brazil_spot(df)

    # Zero-price row dropped
    assert len(out) == 2
    assert (out["price_brl_mt"] > 0).all()
    assert out["Date"].is_monotonic_increasing


def test_clean_brazil_spot_does_not_mutate_input():
    df = _make_brazil_spot()
    before = df.copy(deep=True)
    clean_brazil_spot(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_safex
# ---------------------------------------------------------------------------

def _make_safex() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-02", "2026-01-01", "2026-01-03"],
            "Close": ["8000", "0", "8100"],
            "Volume": ["200", "150", "180"],
        }
    )


def test_clean_safex_happy_path():
    df = _make_safex()
    out = clean_safex(df)

    # Zero-close row dropped
    assert len(out) == 2
    assert (out["Close"] > 0).all()
    assert out["Date"].is_monotonic_increasing


def test_clean_safex_does_not_mutate_input():
    df = _make_safex()
    before = df.copy(deep=True)
    clean_safex(df)
    pdt.assert_frame_equal(df, before)


# ---------------------------------------------------------------------------
# clean_worldbank
# ---------------------------------------------------------------------------

def _make_worldbank() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Date": ["2026-01-15", "2026-01-01", "2026-02-01"],
            "price": [100.0, 95.0, np.nan],
        }
    )


def test_clean_worldbank_happy_path():
    df = _make_worldbank()
    out = clean_worldbank(df)

    assert pd.api.types.is_datetime64_any_dtype(out["Date"])
    assert out["Date"].is_monotonic_increasing
    # NaN price row dropped
    assert len(out) == 2


def test_clean_worldbank_does_not_mutate_input():
    df = _make_worldbank()
    before = df.copy(deep=True)
    clean_worldbank(df)
    pdt.assert_frame_equal(df, before)
