"""
Spread calculations for the soy complex.

Two daily, cross-market relationships are computed here:

1. Crush spread (CBOT board crush): (Oil × 11) + (Meal × 2.2) − Soybeans.
   Tells you whether processors can profitably crush beans into oil+meal.

2. Brazil basis (CEPEA Paraná farm-gate vs CBOT): the price differential
   between Brazilian domestic soy and CBOT futures, normalized to USD/MT.
   Negative basis = Brazilian product trades below CBOT (export-competitive).
   This is a Brazil-domestic-vs-CBOT spread; the true Paranaguá port FOB
   premium is a future fetcher (Phase 2).
"""

import pandas as pd

from config import CRUSH_MEAL_FACTOR, CRUSH_OIL_FACTOR
from pipeline.units import to_metric_tons


def compute_crush_spread(
    soybeans_df: pd.DataFrame,
    oil_df: pd.DataFrame,
    meal_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute daily crush margin from CBOT futures prices.

    Formula (CBOT board crush, all in cents per bushel):
        Crush Spread = (Soybean Oil price x 11) + (Soybean Meal price x 2.2) - Soybean price

    Where:
        - Oil price is in cents/lb; 1 bushel → ~11 lbs oil → multiply by 11
        - Meal price is in $/short ton; 1 bushel → ~0.022 short tons meal
          but we multiply by 100 to convert $ to cents → 0.022 x 100 = 2.2
        - Soybean price is already in cents/bushel

    Parameters
    ----------
    soybeans_df : pd.DataFrame
        Soybean prices with 'Close' column and Date index.
    oil_df : pd.DataFrame
        Soybean oil prices with 'Close' column and Date index.
    meal_df : pd.DataFrame
        Soybean meal prices with 'Close' column and Date index.

    Returns
    -------
    pd.DataFrame
        Columns: Date, soybeans_close, oil_close, meal_close, crush_spread, oil_value_share

        oil_value_share = (oil x 11) / (oil x 11 + meal x 2.2), range 0-1.
        Rising share = biodiesel/oil demand dominating; falling = meal/livestock demand dominating.
    """
    # Align all three on the same dates using inner join
    combined = pd.DataFrame({
        "soybeans_close": soybeans_df["Close"],
        "oil_close":      oil_df["Close"],
        "meal_close":     meal_df["Close"],
    }).dropna()

    oil_value = combined["oil_close"] * CRUSH_OIL_FACTOR
    meal_value = combined["meal_close"] * CRUSH_MEAL_FACTOR
    combined["crush_spread"] = oil_value + meal_value - combined["soybeans_close"]
    combined["oil_value_share"] = oil_value / (oil_value + meal_value)

    result = combined.reset_index()
    result.columns = ["Date", "soybeans_close", "oil_close", "meal_close", "crush_spread", "oil_value_share"]

    return result


def compute_domestic_basis(
    soybeans_df: pd.DataFrame,
    domestic_df: pd.DataFrame,
    fx_df: pd.DataFrame,
    price_col: str = "Close",
) -> pd.DataFrame:
    """
    Compute a daily domestic-vs-CBOT soybean basis in USD/MT, date-aligned.

    Generic engine behind `compute_brazil_basis`, also used for the SAFEX
    (ZAR) and India mandi (INR) legs. All three series are inner-joined on
    Date so the basis never mixes observations from different days:

        cbot_usd_mt     = to_metric_tons(soybeans_close_cents_bu, "Soybeans")
        domestic_usd_mt = domestic_price_local_mt * fx_usd    # USD per local unit
        basis_usd_mt    = domestic_usd_mt − cbot_usd_mt
        basis_pct       = basis_usd_mt / cbot_usd_mt

    Parameters
    ----------
    soybeans_df : pd.DataFrame
        CBOT soybean prices with DatetimeIndex and a 'Close' column in
        cents/bushel (native CBOT unit).
    domestic_df : pd.DataFrame
        Domestic prices in local currency per MT. Accepts either a 'Date'
        column or a DatetimeIndex; the price is read from `price_col`.
    fx_df : pd.DataFrame
        FX rate with DatetimeIndex and a 'Close' column, quoted USD per
        local-currency unit (yfinance XXXUSD=X convention).
    price_col : str
        Name of the price column in `domestic_df` (default 'Close').

    Returns
    -------
    pd.DataFrame
        Columns: Date, cbot_usd_mt, domestic_usd_mt, fx_usd, basis_usd_mt,
        basis_pct. Rows where any input is missing or where fx_usd ≤ 0 are
        dropped. Empty inputs return an empty DataFrame with the expected
        columns.
    """
    empty_columns = ["Date", "cbot_usd_mt", "domestic_usd_mt", "fx_usd",
                     "basis_usd_mt", "basis_pct"]

    if soybeans_df.empty or domestic_df.empty or fx_df.empty:
        return pd.DataFrame(columns=empty_columns)

    domestic = domestic_df.copy()
    if "Date" in domestic.columns:
        domestic["Date"] = pd.to_datetime(domestic["Date"])
        domestic = domestic.set_index("Date")
    if price_col not in domestic.columns:
        raise KeyError(f"domestic_df must have a '{price_col}' column (local currency/MT)")
    domestic_series = pd.to_numeric(domestic[price_col], errors="coerce").dropna()

    soybeans_close = pd.to_numeric(soybeans_df["Close"], errors="coerce").dropna()
    fx_close = pd.to_numeric(fx_df["Close"], errors="coerce").dropna()

    combined = pd.DataFrame({
        "soybeans_cents_bu": soybeans_close,
        "domestic_local_mt": domestic_series,
        "fx_usd":            fx_close,
    }).dropna()

    # Drop rows where the FX rate would produce divide-by-zero downstream
    # (basis_pct denominator) or nonsensical USD values.
    combined = combined[combined["fx_usd"] > 0]
    if combined.empty:
        return pd.DataFrame(columns=empty_columns)

    cbot_usd_mt = combined["soybeans_cents_bu"].apply(
        lambda v: to_metric_tons(v, "Soybeans")
    )
    domestic_usd_mt = combined["domestic_local_mt"] * combined["fx_usd"]
    basis_usd_mt = domestic_usd_mt - cbot_usd_mt
    basis_pct = basis_usd_mt / cbot_usd_mt

    result = pd.DataFrame({
        "cbot_usd_mt":     cbot_usd_mt,
        "domestic_usd_mt": domestic_usd_mt,
        "fx_usd":          combined["fx_usd"],
        "basis_usd_mt":    basis_usd_mt,
        "basis_pct":       basis_pct,
    }).reset_index().rename(columns={"index": "Date"})

    if "Date" not in result.columns and result.columns[0] != "Date":
        result = result.rename(columns={result.columns[0]: "Date"})

    return result[empty_columns]


def compute_brazil_basis(
    soybeans_df: pd.DataFrame,
    cepea_df: pd.DataFrame,
    brl_usd_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the daily Brazil-domestic-vs-CBOT basis in USD/MT.

    Formula (all aligned on Date):
        cbot_usd_mt  = to_metric_tons(soybeans_close_cents_bu, "Soybeans")
        cepea_usd_mt = cepea_price_brl_mt * brl_usd_rate     # USD per BRL
        basis_usd_mt = cepea_usd_mt − cbot_usd_mt
        basis_pct    = basis_usd_mt / cbot_usd_mt

    The stored BRL/USD series uses Yahoo Finance ticker BRLUSD=X, which
    quotes USD per BRL (≈ 0.18). To convert BRL/MT → USD/MT, multiply.
    (This matches the convention already used in
    `analysis/soy_analytics.emerging_markets_analysis`.)

    Negative basis = Brazilian product trades at a discount to CBOT
    (export-competitive). Positive basis = domestic premium.

    Parameters
    ----------
    soybeans_df : pd.DataFrame
        CBOT soybean prices with DatetimeIndex and a 'Close' column in
        cents/bushel (native CBOT unit).
    cepea_df : pd.DataFrame
        CEPEA spot prices with a price column in BRL/MT. Accepts either
        a 'Date' column or a DatetimeIndex, and either column name:
        `price_brl_mt` (raw fetcher output) or `price_brl` (the DB schema
        rename in `save_brazil_spot`). Both are BRL/MT.
    brl_usd_df : pd.DataFrame
        BRL/USD rate with DatetimeIndex and a 'Close' column. Close is
        USD-per-BRL (yfinance BRLUSD=X convention).

    Returns
    -------
    pd.DataFrame
        Columns: Date, cbot_usd_mt, cepea_usd_mt, brl_usd, basis_usd_mt,
        basis_pct. Rows where any input is missing or where brl_usd ≤ 0
        are dropped. Empty inputs return an empty DataFrame with the
        expected columns.
    """
    empty_columns = ["Date", "cbot_usd_mt", "cepea_usd_mt", "brl_usd",
                     "basis_usd_mt", "basis_pct"]

    if soybeans_df.empty or cepea_df.empty or brl_usd_df.empty:
        return pd.DataFrame(columns=empty_columns)

    # Normalize CEPEA into a Date-indexed Series of BRL/MT. The fetcher emits
    # `price_brl_mt`; the storage layer renames it to `price_brl` (still BRL/MT)
    # when persisting, so `read_brazil_spot()` returns the latter. Accept both.
    cepea = cepea_df.copy()
    if "Date" in cepea.columns:
        cepea["Date"] = pd.to_datetime(cepea["Date"])
        cepea = cepea.set_index("Date")
    if "price_brl_mt" in cepea.columns:
        price_col = "price_brl_mt"
    elif "price_brl" in cepea.columns:
        price_col = "price_brl"
    else:
        raise KeyError(
            "cepea_df must have a 'price_brl_mt' or 'price_brl' column (BRL/MT)"
        )
    result = compute_domestic_basis(
        soybeans_df, cepea, brl_usd_df, price_col=price_col
    ).rename(columns={"domestic_usd_mt": "cepea_usd_mt", "fx_usd": "brl_usd"})

    return result[empty_columns]
