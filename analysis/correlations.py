"""
Cross-market correlation analysis.

Correlations tell you how two markets move relative to each other:
    - +1.0: move perfectly together (when A goes up, B always goes up)
    -  0.0: no relationship
    - -1.0: move perfectly opposite (when A goes up, B always goes down)

All correlations here are computed on **daily returns** (`pct_change`), not
raw price levels. Correlating levels over multi-year windows mostly measures
shared trend (inflation, broad commodity cycles) and produces spuriously
high coefficients — e.g. Soybeans vs Wheat is ~0.84 on levels but ~0.35 on
returns over the same 15y window. Returns strip the trend and answer the
question traders actually ask: "when A moves today, does B move with it?"

Key concepts for learning:
    - Soybean oil and palm oil often move together (substitutes)
    - BRL/USD and soybean prices are often negatively correlated
      (weak Real → cheap Brazilian exports → lower global soy prices)
    - Rolling correlations: the relationship between two markets can
      change over time, so we compute correlation over a moving window
"""

import pandas as pd

# Minimum overlapping daily-return observations for a correlation to be
# reported. Below this the estimate is noise, so we return NaN instead.
MIN_RETURN_OBS = 60


def _daily_returns(series: pd.Series) -> pd.Series:
    """Daily % returns of a price series, gaps dropped before differencing."""
    return series.dropna().pct_change()


def commodity_correlation_matrix(price_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a daily-return correlation matrix across multiple commodities.

    Parameters
    ----------
    price_dict : dict
        {commodity_name: DataFrame} — each DataFrame must have a 'Close'
        column with a DatetimeIndex.

    Returns
    -------
    pd.DataFrame
        N x N correlation matrix where N = number of commodities.
        Values range from -1 to +1. Pairs with fewer than MIN_RETURN_OBS
        overlapping return observations are NaN.
    """
    returns = {}
    for name, df in price_dict.items():
        if not df.empty and "Close" in df.columns:
            series = _daily_returns(df["Close"])
            series.name = name
            returns[name] = series

    if len(returns) < 2:
        return pd.DataFrame()

    combined = pd.DataFrame(returns)
    return combined.corr(min_periods=MIN_RETURN_OBS)


def commodity_vs_currency(
    price_df: pd.DataFrame,
    currency_df: pd.DataFrame,
    commodity_name: str = "Commodity",
    currency_name: str = "Currency",
) -> float:
    """
    Correlation between a commodity's daily returns and a currency pair's.

    Example: How does BRL/USD move relative to soybean prices?
    A negative correlation means a weaker Real → higher soy prices (in USD).

    Parameters
    ----------
    price_df : pd.DataFrame
        Commodity price data with 'Close' column and DatetimeIndex.
    currency_df : pd.DataFrame
        Currency data with 'Close' column and DatetimeIndex.

    Returns
    -------
    float
        Correlation coefficient (-1 to +1), or NaN if fewer than
        MIN_RETURN_OBS overlapping return observations.
    """
    if price_df.empty or currency_df.empty:
        return float("nan")

    combined = pd.DataFrame({
        commodity_name: _daily_returns(price_df["Close"]),
        currency_name:  _daily_returns(currency_df["Close"]),
    }).dropna()

    if len(combined) < MIN_RETURN_OBS:
        return float("nan")

    return combined[commodity_name].corr(combined[currency_name])


def rolling_correlation(
    series_a: pd.Series,
    series_b: pd.Series,
    window: int = 60,
) -> pd.Series:
    """
    Rolling correlation of daily returns between two price series.

    This shows how the relationship between two markets changes over time.
    A 60-day rolling window means each point is the correlation of daily
    returns computed over the prior 60 trading days.

    Parameters
    ----------
    series_a : pd.Series
        First price series (e.g. soybean closing prices).
    series_b : pd.Series
        Second price series (e.g. BRL/USD closing prices).
    window : int
        Rolling window size in trading days (default 60 ≈ 3 months).

    Returns
    -------
    pd.Series
        Rolling return correlation (NaN for the first 'window' rows).
        Empty Series when there's less than a full window of overlap.
    """
    combined = pd.DataFrame({
        "a": _daily_returns(series_a),
        "b": _daily_returns(series_b),
    }).dropna()

    if len(combined) < window:
        return pd.Series(dtype=float)

    return combined["a"].rolling(window=window).corr(combined["b"])
