"""Smoke tests for pipeline.units.

Covers all four public functions plus the CONVERSION_FACTORS table.
Numeric expectations are computed against the docstring conversion factors
in pipeline/units.py.
"""

from __future__ import annotations

import pandas as pd
import pandas.testing as pdt
import pytest

from pipeline.units import (
    CONVERSION_FACTORS,
    convert_df_to_mt,
    mt_label,
    native_label,
    to_metric_tons,
)

# ---------------------------------------------------------------------------
# to_metric_tons
# ---------------------------------------------------------------------------

# (commodity, native_price, expected_usd_mt)
NUMERIC_CONVERSIONS = [
    # cents/bu × 36.7437 / 100 = USD/MT
    ("Soybeans", 1000.0, 1000.0 * 36.7437 / 100),
    ("Wheat", 800.0, 800.0 * 36.7437 / 100),
    # cents/bu × 39.368 / 100 = USD/MT
    ("Corn", 400.0, 400.0 * 39.368 / 100),
    # cents/lb × 2204.62 / 100 = USD/MT
    ("Soybean Oil", 50.0, 50.0 * 2204.62 / 100),
    ("Sugar", 20.0, 20.0 * 2204.62 / 100),
    ("Cotton", 70.0, 70.0 * 2204.62 / 100),
    ("Coffee", 150.0, 150.0 * 2204.62 / 100),
    ("Live Cattle", 180.0, 180.0 * 2204.62 / 100),
    ("Lean Hogs", 80.0, 80.0 * 2204.62 / 100),
    # USD/short ton / 0.907185 = USD/MT
    ("Soybean Meal", 300.0, 300.0 / 0.907185),
    # USD/MT already (CME calendar swap) — identity
    ("Palm Oil (CME)", 1127.5, 1127.5),
]


@pytest.mark.parametrize("commodity,native,expected", NUMERIC_CONVERSIONS,
                         ids=[c[0] for c in NUMERIC_CONVERSIONS])
def test_to_metric_tons_numeric(commodity, native, expected):
    assert to_metric_tons(native, commodity) == pytest.approx(expected)


def test_to_metric_tons_unknown_commodity_returns_none():
    assert to_metric_tons(100.0, "NotARealCommodity") is None


def test_conversion_factors_table_covers_documented_commodities():
    expected_keys = {
        "Soybeans", "Soybean Oil", "Soybean Meal",
        "Corn", "Wheat", "Sugar", "Cotton", "Coffee",
        "Live Cattle", "Lean Hogs", "Palm Oil (CME)",
    }
    assert expected_keys.issubset(set(CONVERSION_FACTORS.keys()))


# ---------------------------------------------------------------------------
# convert_df_to_mt
# ---------------------------------------------------------------------------

def _make_price_df() -> pd.DataFrame:
    """Two rows with OHLC + indicator columns at typical Soybean prices."""
    return pd.DataFrame(
        {
            "Open": [1000.0, 1010.0],
            "High": [1020.0, 1030.0],
            "Low": [995.0, 1005.0],
            "Close": [1015.0, 1025.0],
            "Volume": [10000, 11000],
            "MA_20": [1005.0, 1010.0],
            "MA_50": [990.0, 995.0],
            "MA_200": [970.0, 972.0],
            "BB_Upper": [1040.0, 1045.0],
            "BB_Lower": [980.0, 985.0],
            "BB_Middle": [1010.0, 1015.0],
        }
    )


def test_convert_df_to_mt_scales_ohlc():
    df = _make_price_df()
    out = convert_df_to_mt(df, "Soybeans")
    factor = CONVERSION_FACTORS["Soybeans"]

    for col in ("Open", "High", "Low", "Close"):
        for i in range(len(df)):
            assert out[col].iloc[i] == pytest.approx(df[col].iloc[i] * factor)


def test_convert_df_to_mt_scales_indicator_columns():
    df = _make_price_df()
    out = convert_df_to_mt(df, "Soybeans")
    factor = CONVERSION_FACTORS["Soybeans"]

    for col in ("MA_20", "MA_50", "MA_200", "BB_Upper", "BB_Lower", "BB_Middle"):
        for i in range(len(df)):
            assert out[col].iloc[i] == pytest.approx(df[col].iloc[i] * factor)


def test_convert_df_to_mt_leaves_volume_untouched():
    df = _make_price_df()
    out = convert_df_to_mt(df, "Soybeans")
    assert out["Volume"].tolist() == df["Volume"].tolist()


def test_convert_df_to_mt_does_not_mutate_input():
    df = _make_price_df()
    before = df.copy(deep=True)
    convert_df_to_mt(df, "Soybeans")
    pdt.assert_frame_equal(df, before)


def test_convert_df_to_mt_palm_oil_identity_conversion():
    df = _make_price_df()
    out = convert_df_to_mt(df, "Palm Oil (CME)")
    # CPO=F is natively USD/MT — factor 1.0 leaves values unchanged
    pdt.assert_frame_equal(out, df)
    # But it is a distinct object (a copy)
    assert out is not df


def test_convert_df_to_mt_unknown_commodity_returns_unchanged_copy():
    df = _make_price_df()
    out = convert_df_to_mt(df, "NotARealCommodity")
    pdt.assert_frame_equal(out, df)
    assert out is not df


# ---------------------------------------------------------------------------
# mt_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "commodity,expected",
    [
        ("Palm Oil (CME)", "USD/MT"),
        ("Soybeans", "USD/MT"),
        ("Corn", "USD/MT"),
        ("Soybean Meal", "USD/MT"),
        ("NotARealCommodity", "USD/MT"),  # default branch
    ],
)
def test_mt_label(commodity, expected):
    assert mt_label(commodity) == expected


# ---------------------------------------------------------------------------
# native_label
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "commodity,expected",
    [
        ("Soybeans", "cents/bu"),
        ("Corn", "cents/bu"),
        ("Wheat", "cents/bu"),
        ("Soybean Oil", "cents/lb"),
        ("Sugar", "cents/lb"),
        ("Cotton", "cents/lb"),
        ("Coffee", "cents/lb"),
        ("Live Cattle", "cents/lb"),
        ("Lean Hogs", "cents/lb"),
        ("Soybean Meal", "$/short ton"),
        ("Palm Oil (CME)", "USD/MT"),
        ("NotARealCommodity", ""),  # default branch
    ],
)
def test_native_label(commodity, expected):
    assert native_label(commodity) == expected
