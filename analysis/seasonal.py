"""
Seasonal pattern analysis for commodity prices.

Many commodities have predictable seasonal patterns driven by planting,
growing, and harvest cycles:
    - Soybeans: US plants in May, harvests Sep-Nov → prices often peak
      in Jun-Jul (weather uncertainty) and dip at harvest
    - Coffee: Brazil harvests May-Sep → supply pressure
    - These patterns repeat year after year, though individual years vary

Key concepts for learning:
    - Grouping by calendar month to find the "average" price pattern
    - Comparing current prices to the seasonal norm shows whether the
      market is behaving unusually (which might signal an opportunity)

Sample-size guard:
    Monthly averages over short windows (2y) are not "seasonal norms" — they
    confound trend and season. We require at least SEASONAL_MIN_YEARS_PER_MONTH
    observations per calendar month before reporting an average; otherwise the
    function returns an empty result.
"""

import pandas as pd

from config import SEASONAL_MIN_YEARS_PER_MONTH


def monthly_seasonal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average closing price by calendar month across all years.

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
        Columns: month (1-12), avg_close, min_close, max_close, n_years
        One row per calendar month that passed the sample-size guard.
    """
    if df.empty or "Close" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["month"] = df.index.month
    df["year"] = df.index.year

    seasonal = df.groupby("month").agg(
        avg_close=("Close", "mean"),
        min_close=("Close", "min"),
        max_close=("Close", "max"),
        n_years=("year", "nunique"),
    ).reset_index()

    seasonal = seasonal[seasonal["n_years"] >= SEASONAL_MIN_YEARS_PER_MONTH]
    return seasonal.reset_index(drop=True)


def current_vs_seasonal(df: pd.DataFrame) -> dict:
    """
    Compare current price to its seasonal average.

    Returns a dict telling you whether the current price is above or
    below its historical average for this month. Returns an empty dict
    when the current calendar month doesn't have enough history to
    compute a trustworthy seasonal average.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with 'Close' column and a DatetimeIndex.

    Returns
    -------
    dict
        Keys: current_price, seasonal_avg, deviation_pct, assessment, n_years
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

    if deviation_pct > 0:
        assessment = f"Above seasonal (+{deviation_pct:.1f}%)"
    else:
        assessment = f"Below seasonal ({deviation_pct:.1f}%)"

    return {
        "current_price": current_price,
        "seasonal_avg": seasonal_avg,
        "deviation_pct": deviation_pct,
        "assessment": assessment,
        "n_years": n_years,
    }
