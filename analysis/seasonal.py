"""
Seasonal pattern analysis for commodity prices.

Many commodities have predictable seasonal patterns driven by planting,
growing, and harvest cycles:
    - Soybeans: US plants in May, harvests Sep-Nov → prices often peak
      in Jun-Jul (weather uncertainty) and dip at harvest
    - Coffee: Brazil harvests May-Sep → supply pressure
    - These patterns repeat year after year, though individual years vary

Detrending:
    Raw monthly averages over a 15-year window mostly capture the price
    *trend* (inflation, structural demand shifts), not seasonality. The
    detrended measure divides each daily close by its trailing 12-month
    mean, giving a % deviation series that is trend-free; averaging that
    by calendar month isolates the true seasonal shape. Level-based
    averages (avg_close etc.) are retained for display/back-compat, but
    the seasonal read should come from the *_dev_pct fields.

Sample-size guard:
    Monthly averages over short windows (2y) are not "seasonal norms" — they
    confound trend and season. We require at least SEASONAL_MIN_YEARS_PER_MONTH
    observations per calendar month before reporting an average; otherwise the
    function returns an empty result.
"""

import pandas as pd

from config import SEASONAL_MIN_YEARS_PER_MONTH

# Trailing window used to detrend prices before extracting seasonality.
_TREND_WINDOW = "365D"
# Minimum observations inside the trailing window before the trend mean is
# trusted (≈ half a year of trading days). Below this, deviation is NaN.
_TREND_MIN_OBS = 126


def _trend_deviation_pct(close: pd.Series) -> pd.Series:
    """% deviation of each close from its trailing 12-month mean.

    NaN for the warm-up period where the trailing window has fewer than
    _TREND_MIN_OBS observations.
    """
    close = close.dropna().sort_index()
    if close.empty:
        return pd.Series(dtype=float)
    trend = close.rolling(_TREND_WINDOW, min_periods=_TREND_MIN_OBS).mean()
    return (close / trend - 1.0) * 100.0


def monthly_seasonal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average closing price by calendar month across all years,
    plus the detrended seasonal deviation per month.

    Months with fewer than SEASONAL_MIN_YEARS_PER_MONTH distinct years of
    observations are dropped so we don't report short-window noise as
    a "seasonal norm". If no month clears the bar, returns an empty DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with 'Close' column and a DatetimeIndex.

    Returns
    -------
    pd.DataFrame
        Columns: month (1-12), avg_close, min_close, max_close, n_years,
        avg_dev_pct. One row per calendar month that passed the guard.
        avg_close/min_close/max_close are raw levels (kept for display);
        avg_dev_pct is the month's average % deviation from the trailing
        12-month mean — the detrended seasonal signal (NaN if the series
        is too short to establish a trend).
    """
    if df.empty or "Close" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["month"] = df.index.month
    df["year"] = df.index.year
    df["dev_pct"] = _trend_deviation_pct(df["Close"])

    seasonal = df.groupby("month").agg(
        avg_close=("Close", "mean"),
        min_close=("Close", "min"),
        max_close=("Close", "max"),
        n_years=("year", "nunique"),
        avg_dev_pct=("dev_pct", "mean"),
    ).reset_index()

    seasonal = seasonal[seasonal["n_years"] >= SEASONAL_MIN_YEARS_PER_MONTH]
    return seasonal.reset_index(drop=True)


def current_vs_seasonal(df: pd.DataFrame) -> dict:
    """
    Compare current price to its seasonal norm.

    Detrended comparison (preferred): current % deviation from the trailing
    12-month mean vs the historical average deviation for this calendar
    month. Level comparison (deviation_pct) is kept for back-compat but
    conflates trend with season.

    Returns an empty dict when the current calendar month doesn't have
    enough history to compute a trustworthy seasonal average.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with 'Close' column and a DatetimeIndex.

    Returns
    -------
    dict
        Keys: current_price, seasonal_avg, deviation_pct, assessment,
        n_years (level-based, back-compat), plus detrended fields:
        current_dev_pct (current % above/below trailing 12m mean),
        seasonal_dev_pct (typical deviation for this month),
        detrended_delta_pct (current_dev_pct - seasonal_dev_pct; the
        seasonality-adjusted read — None when history is too short).
        `assessment` uses the detrended delta when available.
        Empty dict if sample size is insufficient.
    """
    if df.empty or "Close" not in df.columns:
        return {}

    current_price = df["Close"].iloc[-1]
    current_month = df.index[-1].month

    seasonal = monthly_seasonal(df)
    if seasonal.empty:
        return {}

    month_row = seasonal[seasonal["month"] == current_month]
    if month_row.empty:
        return {}

    seasonal_avg = month_row["avg_close"].iloc[0]
    n_years = int(month_row["n_years"].iloc[0])
    deviation_pct = ((current_price - seasonal_avg) / seasonal_avg) * 100

    # Detrended comparison
    dev_series = _trend_deviation_pct(df["Close"])
    current_dev_pct = dev_series.iloc[-1] if not dev_series.empty else float("nan")
    seasonal_dev_pct = month_row["avg_dev_pct"].iloc[0]

    detrended_delta_pct = None
    if pd.notna(current_dev_pct) and pd.notna(seasonal_dev_pct):
        detrended_delta_pct = float(current_dev_pct - seasonal_dev_pct)

    if detrended_delta_pct is not None:
        direction = "Above" if detrended_delta_pct > 0 else "Below"
        assessment = (
            f"{direction} seasonal norm ({detrended_delta_pct:+.1f}% vs "
            f"this month's typical deviation from 12m trend)"
        )
    elif deviation_pct > 0:
        assessment = f"Above 15y avg level (+{deviation_pct:.1f}%, trend not removed)"
    else:
        assessment = f"Below 15y avg level ({deviation_pct:.1f}%, trend not removed)"

    return {
        "current_price": current_price,
        "seasonal_avg": seasonal_avg,
        "deviation_pct": deviation_pct,
        "assessment": assessment,
        "n_years": n_years,
        "current_dev_pct": float(current_dev_pct) if pd.notna(current_dev_pct) else None,
        "seasonal_dev_pct": float(seasonal_dev_pct) if pd.notna(seasonal_dev_pct) else None,
        "detrended_delta_pct": detrended_delta_pct,
    }
