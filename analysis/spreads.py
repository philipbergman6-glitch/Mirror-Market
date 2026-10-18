"""
Spread calculations for the soy complex.

Two daily, cross-market relationships are computed here:

1. Crush spread (CBOT board crush): (Oil × 11) + (Meal × 2.2) − Soybeans.
   Tells you whether processors can profitably crush beans into oil+meal.

2. Brazil basis (CEPEA Paraná farm-gate vs CBOT): the price differential
   between Brazilian domestic soy and CBOT futures, normalized to USD/MT.
   Negative basis = Brazilian product trades below CBOT (export-competitive).
   This is a Brazil-domestic-vs-CBOT spread. The AgRural Paranaguá leg
   (Layer 19) is an on-truck "sobre rodas" port bid, not FOB vessel (B21
   #483), and no publisher of record exists for a Paranaguá FOB premium.
"""

from dataclasses import dataclass

import pandas as pd

from config import (
    CROSS_VENUE_SPREAD_MAX_SESSION_LAG_DAYS,
    CRUSH_MEAL_FACTOR,
    CRUSH_OIL_FACTOR,
    FX_ALIGNMENT_MAX_GAP_DAYS,
)
from pipeline.units import to_metric_tons
from pricing.fx_alignment import align_fx

# Metric-native crush yields, derived from the same 60-lb-bushel mass
# balance behind the CBOT board-crush factors: ~11 lb oil + ~44 lb meal
# per bushel. CRUSH_OIL_FACTOR is the oil pounds directly; CRUSH_MEAL_FACTOR
# is in cents/bu terms ($/short ton × 44/2000 × 100), so the meal pounds are
# recovered as factor × 20. Kept derived so a yield-assumption change in
# config.py propagates here automatically.
_BUSHEL_LBS = 60.0
CRUSH_OIL_YIELD_MT = CRUSH_OIL_FACTOR / _BUSHEL_LBS               # ≈0.183 MT oil/MT beans
CRUSH_MEAL_YIELD_MT = (CRUSH_MEAL_FACTOR * 20.0) / _BUSHEL_LBS    # ≈0.733 MT meal/MT beans

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


# ---------------------------------------------------------------------------
# Cross-venue spreads: a CBOT leg against a home-currency leg, one session
# ---------------------------------------------------------------------------
def daily_close(series: pd.Series) -> pd.Series:
    """A close series on a tz-naive, midnight-normalised, ascending DatetimeIndex.

    Venues' frames arrive with different index conventions (a ``Date`` column,
    a tz-aware provider index); two legs can only share a session once their
    dates compare equal.
    """
    index = pd.DatetimeIndex(pd.to_datetime(series.index))
    if index.tz is not None:
        index = index.tz_localize(None)
    out = pd.Series(series.to_numpy(dtype=float), index=index.normalize()).dropna()
    return out[~out.index.duplicated(keep="last")].sort_index()


def fx_close_on(
    fx: pd.DataFrame | None, when: pd.Timestamp, pair: str
) -> tuple[float, pd.Timestamp] | None:
    """The ``pair`` close a price dated ``when`` converts at under A1 (#298).

    Thin adapter over :func:`pricing.fx_alignment.align_fx` — the one site that
    applies the FX-date rule — for a ``Close``-column frame on a DatetimeIndex.
    ``None`` means the leg renders without a USD figure: there is no close on or
    before ``when`` inside the cap, and a later-dated rate is never used.
    """
    if fx is None or fx.empty or "Close" not in fx.columns:
        return None
    closes = pd.Series(
        fx["Close"].to_numpy(dtype=float),
        index=pd.DatetimeIndex(fx.index).normalize(),
    ).dropna()
    closes = closes[closes > 0]
    res = align_fx(
        pair,
        (
            (ts.date(), float(v))
            for ts, v in zip(
                pd.DatetimeIndex(closes.index).to_pydatetime(),
                closes.to_numpy(dtype=float),
                strict=True,
            )
        ),
        pd.Timestamp(when).date(),
    )
    if not res.ok or res.rate is None or res.observed_on is None:
        return None
    return float(res.rate), pd.Timestamp(res.observed_on)


@dataclass(frozen=True)
class CrossVenueStrike:
    """A home-currency leg minus a CBOT leg in USD/MT, or the reason there is none.

    Exactly one of ``spread_usd_mt`` / ``reason_code`` is set. ``struck_on`` is
    carried on an FX refusal too, so the withheld line can name the session it
    would have used.
    """

    spread_usd_mt: float | None
    struck_on: pd.Timestamp | None = None
    fx_observed_on: pd.Timestamp | None = None
    reason_code: str | None = None
    reason: str | None = None


def strike_cross_venue_spread(
    *,
    own: pd.Series,
    own_commodity: str,
    own_label: str,
    other: pd.Series,
    other_label: str,
    fx: pd.DataFrame | None,
    fx_pair: str,
) -> CrossVenueStrike:
    """``other`` (home currency per MT) minus ``own`` (CBOT native units), one session.

    The spread is struck on the latest session both legs printed, no more than
    ``CROSS_VENUE_SPREAD_MAX_SESSION_LAG_DAYS`` behind the newer leg, at that
    session's A1-aligned FX (invariant 8). Each leg's own latest print is the
    caller's to display; this function only ever subtracts like from like.
    """
    own = own.dropna()
    other = other.dropna()
    if own.empty or other.empty:
        return CrossVenueStrike(
            None, reason_code="leg_missing", reason=f"{own_label} or {other_label} has no print"
        )
    own_last_on = pd.Timestamp(own.index[-1])
    other_last_on = pd.Timestamp(other.index[-1])
    common = own.index.intersection(other.index)
    newest = max(own_last_on, other_last_on)
    if common.empty or (newest - pd.Timestamp(common[-1])).days > CROSS_VENUE_SPREAD_MAX_SESSION_LAG_DAYS:
        return CrossVenueStrike(
            None,
            reason_code="no_common_session",
            reason=(
                f"no common session within {CROSS_VENUE_SPREAD_MAX_SESSION_LAG_DAYS} days: "
                f"{own_label} last printed {own_last_on.strftime('%Y-%m-%d')}, "
                f"{other_label} {other_last_on.strftime('%Y-%m-%d')}"
            ),
        )

    struck_on = pd.Timestamp(common[-1])
    strike_fx = fx_close_on(fx, struck_on, fx_pair)
    if strike_fx is None:
        return CrossVenueStrike(
            None,
            struck_on=struck_on,
            reason_code="fx_gap_exceeded",
            reason=(
                f"fx_gap_exceeded: no {fx_pair} close within {FX_ALIGNMENT_MAX_GAP_DAYS} days "
                f"at or before the strike session {struck_on.strftime('%Y-%m-%d')}"
            ),
        )
    rate, fx_observed_on = strike_fx
    own_struck = to_metric_tons(float(own.loc[struck_on]), own_commodity)
    if own_struck is None:
        return CrossVenueStrike(
            None,
            struck_on=struck_on,
            reason_code="unit_conversion_failed",
            reason=f"{own_commodity} has no USD/MT conversion",
        )
    return CrossVenueStrike(
        float(other.loc[struck_on]) * rate - own_struck,
        struck_on=struck_on,
        fx_observed_on=fx_observed_on,
    )
