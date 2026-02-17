"""
Metric ton conversion utilities for display-layer price conversion.

All commodity futures prices are stored in their native exchange units
(cents/bushel, cents/lb, $/short ton, MYR/MT). This module converts
them to USD/MT for professional international display.

Key concept: Raw database data stays in native units — conversion
happens only at the display layer. This avoids double-conversion bugs
and keeps the internal math (crush spread, technicals) working with
the same units the exchange uses.

Conversion factors:
    Soybeans:     1 MT = 36.7437 bushels
    Soybean Oil:  1 MT = 2204.62 lbs
    Soybean Meal: 1 short ton = 0.907185 MT
    Corn:         1 MT = 39.368 bushels
    Wheat:        1 MT = 36.7437 bushels
    Sugar/Cotton/Coffee/Cattle/Hogs: 1 MT = 2204.62 lbs
    Palm Oil:     already MYR/MT (needs FX, not unit conversion)
"""

import pandas as pd


# ---------------------------------------------------------------------------
# Conversion factors: native exchange unit → USD/MT
#
# Each entry is a tuple: (multiplier, description)
# To convert: price_native * multiplier = USD/MT
# ---------------------------------------------------------------------------
CONVERSION_FACTORS = {
    # Soybeans: cents/bu → USD/MT
    # cents/bu × (36.7437 bu/MT) / 100 = USD/MT
    "Soybeans": 36.7437 / 100,

    # Soybean Oil: cents/lb → USD/MT
    # cents/lb × (2204.62 lb/MT) / 100 = USD/MT
    "Soybean Oil": 2204.62 / 100,

    # Soybean Meal: USD/short ton → USD/MT
    # USD/short ton × (1 / 0.907185) = USD/MT
    "Soybean Meal": 1 / 0.907185,

    # Corn: cents/bu → USD/MT
    # cents/bu × (39.368 bu/MT) / 100 = USD/MT
    "Corn": 39.368 / 100,

    # Wheat: cents/bu → USD/MT
    # cents/bu × (36.7437 bu/MT) / 100 = USD/MT
    "Wheat": 36.7437 / 100,

    # Sugar: cents/lb → USD/MT
    "Sugar": 2204.62 / 100,

    # Cotton: cents/lb → USD/MT
    "Cotton": 2204.62 / 100,

    # Coffee: cents/lb → USD/MT
    "Coffee": 2204.62 / 100,

    # Live Cattle: cents/lb → USD/MT
    "Live Cattle": 2204.62 / 100,

    # Lean Hogs: cents/lb → USD/MT
    "Lean Hogs": 2204.62 / 100,

    # Palm Oil (BMD): MYR/MT — flag only, needs FX conversion
    # Not a simple unit conversion; requires MYR/USD exchange rate
    "Palm Oil (BMD)": None,
}


def to_metric_tons(value: float, commodity: str) -> float | None:
    """
    Convert a single price value from native exchange units to USD/MT.

    Parameters
    ----------
    value : float
        Price in native exchange units (e.g., cents/bu for soybeans).
    commodity : str
        Commodity name matching COMMODITY_TICKERS keys in config.py.

    Returns
    -------
    float or None
        Price in USD/MT, or None if conversion not available (e.g., Palm Oil
        which needs FX conversion, or unknown commodity).
    """
    factor = CONVERSION_FACTORS.get(commodity)
    if factor is None:
        return None
    return value * factor


def convert_df_to_mt(df: pd.DataFrame, commodity: str) -> pd.DataFrame:
    """
    Convert OHLC price columns in a DataFrame from native units to USD/MT.

    Creates a copy — the original DataFrame is never mutated.

    Parameters
    ----------
    df : pd.DataFrame
        Price DataFrame with Open, High, Low, Close columns.
    commodity : str
        Commodity name matching COMMODITY_TICKERS keys.

    Returns
    -------
    pd.DataFrame
        Copy with OHLC columns converted to USD/MT.
        Returns original copy unchanged if conversion not available.
    """
    factor = CONVERSION_FACTORS.get(commodity)
    if factor is None:
        return df.copy()

    result = df.copy()
    for col in ["Open", "High", "Low", "Close"]:
        if col in result.columns:
            result[col] = result[col] * factor

    # Also convert technical indicator columns that are in price units
    price_columns = [
        "MA_20", "MA_50", "MA_200",
        "BB_Upper", "BB_Lower", "BB_Middle",
    ]
    for col in price_columns:
        if col in result.columns:
            result[col] = result[col] * factor

    return result


def mt_label(commodity: str) -> str:
    """
    Return the display label for a commodity's MT-converted price.

    Parameters
    ----------
    commodity : str
        Commodity name.

    Returns
    -------
    str
        "USD/MT" for most commodities, "MYR/MT" for Palm Oil,
        or "USD/MT" as default for unknown commodities.
    """
    if commodity == "Palm Oil (BMD)":
        return "MYR/MT"
    return "USD/MT"


def native_label(commodity: str) -> str:
    """
    Return the native exchange unit label for a commodity.

    Useful for tooltips or when showing both native and MT prices.
    """
    labels = {
        "Soybeans": "cents/bu",
        "Soybean Oil": "cents/lb",
        "Soybean Meal": "$/short ton",
        "Corn": "cents/bu",
        "Wheat": "cents/bu",
        "Sugar": "cents/lb",
        "Cotton": "cents/lb",
        "Coffee": "cents/lb",
        "Live Cattle": "cents/lb",
        "Lean Hogs": "cents/lb",
        "Palm Oil (BMD)": "MYR/MT",
    }
    return labels.get(commodity, "")
