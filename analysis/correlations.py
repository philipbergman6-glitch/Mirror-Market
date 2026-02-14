"""
Cross-market correlation analysis.

Correlations tell you how two markets move relative to each other:
    - +1.0: move perfectly together (when A goes up, B always goes up)
    -  0.0: no relationship
    - -1.0: move perfectly opposite (when A goes up, B always goes down)

Key concepts for learning:
    - Soybean oil and palm oil often move together (substitutes)
    - BRL/USD and soybean prices are often negatively correlated
      (weak Real → cheap Brazilian exports → lower global soy prices)
    - Rolling correlations: the relationship between two markets can
      change over time, so we compute correlation over a moving window
"""

import pandas as pd


def commodity_correlation_matrix(price_dict: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Build a correlation matrix across multiple commodities.

    Parameters
    ----------
    price_dict : dict
        {commodity_name: DataFrame} — each DataFrame must have a 'Close'
        column with a DatetimeIndex.

    Returns
    -------
    pd.DataFrame
        N x N correlation matrix where N = number of commodities.
        Values range from -1 to +1.
    """
    # Combine closing prices into one DataFrame, aligned on dates
    closes = {}
    for name, df in price_dict.items():
        if not df.empty and "Close" in df.columns:
            series = df["Close"].copy()
            series.name = name
            closes[name] = series

    if len(closes) < 2:
        return pd.DataFrame()

    combined = pd.DataFrame(closes)
    return combined.corr()


def commodity_vs_currency(
    price_df: pd.DataFrame,
    currency_df: pd.DataFrame,
    commodity_name: str = "Commodity",
    currency_name: str = "Currency",
) -> float:
    """
    Compute correlation between a commodity's closing price and a currency pair.

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
        Correlation coefficient (-1 to +1), or NaN if not enough data.
    """
    if price_df.empty or currency_df.empty:
        return float("nan")

    combined = pd.DataFrame({
        commodity_name: price_df["Close"],
        currency_name:  currency_df["Close"],
    }).dropna()

    if len(combined) < 30:
        return float("nan")

    return combined[commodity_name].corr(combined[currency_name])


def rolling_correlation(
    series_a: pd.Series,
    series_b: pd.Series,
    window: int = 60,
) -> pd.Series:
    """
    Compute rolling correlation between two time series.

    This shows how the relationship between two markets changes over time.
    A 60-day rolling window means each point is the correlation computed
    over the prior 60 trading days.

    Parameters
    ----------
    series_a : pd.Series
        First time series (e.g. soybean closing prices).
    series_b : pd.Series
        Second time series (e.g. BRL/USD closing prices).
    window : int
        Rolling window size in trading days (default 60 ≈ 3 months).

    Returns
    -------
    pd.Series
        Rolling correlation values (NaN for first 'window' rows).
    """
    combined = pd.DataFrame({"a": series_a, "b": series_b}).dropna()

    if len(combined) < window:
        return pd.Series(dtype=float)

    return combined["a"].rolling(window=window).corr(combined["b"])
