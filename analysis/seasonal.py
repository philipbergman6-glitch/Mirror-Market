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
"""

import pandas as pd


def monthly_seasonal(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average closing price by calendar month across all years.

    This reveals the typical seasonal pattern — e.g. soybeans tend to
    be most expensive in June-July and cheapest at harvest in October.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with 'Close' column and a DatetimeIndex.

    Returns
    -------
    pd.DataFrame
        Columns: month (1-12), avg_close, min_close, max_close
        One row per calendar month.
    """
    if df.empty or "Close" not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["month"] = df.index.month

    seasonal = df.groupby("month")["Close"].agg(
        avg_close="mean",
        min_close="min",
        max_close="max",
    ).reset_index()

    return seasonal


def current_vs_seasonal(df: pd.DataFrame) -> dict:
    """
    Compare current price to its seasonal average.

    Returns a dict telling you whether the current price is above or
    below its historical average for this month.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with 'Close' column and a DatetimeIndex.

    Returns
    -------
    dict
        Keys: current_price, seasonal_avg, deviation_pct, assessment
        - deviation_pct > 0 means price is above seasonal average
        - assessment is a human-readable string like "Above seasonal (+5.2%)"
    """
    if df.empty or "Close" not in df.columns:
        return {}

    current_price = df["Close"].iloc[-1]
    current_month = df.index[-1].month

    # Get seasonal average for this month
    seasonal = monthly_seasonal(df)
    if seasonal.empty:
        return {}

    month_row = seasonal[seasonal["month"] == current_month]
    if month_row.empty:
        return {}

    seasonal_avg = month_row["avg_close"].iloc[0]
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
    }
