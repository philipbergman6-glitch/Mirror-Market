"""
Technical indicators for commodity price analysis.

All functions take a pandas DataFrame with at least a 'Close' column
(and optionally 'Volume') and add new columns for indicators.

Key concepts for learning:
    - Moving averages: smooth out noise to show the trend direction.
      A price above its 200-day MA is generally in an uptrend.
    - RSI (Relative Strength Index): measures momentum on a 0-100 scale.
      Above 70 = overbought (price may be too high, could pull back).
      Below 30 = oversold (price may be too low, could bounce).
    - Rolling windows: pandas .rolling(n) looks at the last n rows.
"""

import pandas as pd


def add_moving_averages(df: pd.DataFrame, windows: list[int] = [20, 50, 200]) -> pd.DataFrame:
    """
    Add moving average columns (e.g. MA_20, MA_50, MA_200).

    A moving average is just the average closing price over the last N days.
    Short MAs (20) react quickly to price changes.
    Long MAs (200) show the big-picture trend.

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.
    windows : list[int]
        Moving average window sizes to compute.

    Returns
    -------
    pd.DataFrame
        Same DataFrame with new MA_N columns added.
    """
    df = df.copy()
    for w in windows:
        df[f"MA_{w}"] = df["Close"].rolling(window=w).mean()
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    """
    Add RSI (Relative Strength Index) column.

    RSI = 100 - (100 / (1 + RS))
    where RS = average gain / average loss over 'period' days.

    - RSI > 70 → overbought (price rose too fast, watch for pullback)
    - RSI < 30 → oversold (price fell too fast, watch for bounce)

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.
    period : int
        Lookback window (default 14 days, the industry standard).

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'RSI' column added.
    """
    df = df.copy()
    delta = df["Close"].diff()

    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    # Use Wilder smoothing after the initial SMA seed value
    first_valid = avg_gain.first_valid_index()
    if first_valid is not None:
        start_loc = avg_gain.index.get_loc(first_valid) + 1
        for i in range(start_loc, len(avg_gain)):
            avg_gain.iloc[i] = (avg_gain.iloc[i - 1] * (period - 1) + gain.iloc[i]) / period
            avg_loss.iloc[i] = (avg_loss.iloc[i - 1] * (period - 1) + loss.iloc[i]) / period

    rs = avg_gain / avg_loss
    df["RSI"] = 100 - (100 / (1 + rs))

    return df


def add_price_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add daily and weekly percentage change columns.

    These tell you how fast the price is moving:
    - daily_pct_change: yesterday → today (e.g. +0.8% means it went up 0.8%)
    - weekly_pct_change: 5 trading days ago → today

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'daily_pct_change' and 'weekly_pct_change' columns.
    """
    df = df.copy()
    df["daily_pct_change"] = df["Close"].pct_change() * 100
    df["weekly_pct_change"] = df["Close"].pct_change(periods=5) * 100
    return df


def calculate_macd(
    df: pd.DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    """
    Add MACD (Moving Average Convergence Divergence) columns.

    MACD is one of the most popular momentum indicators in commodity trading.
    It shows the relationship between two exponential moving averages (EMAs):

        MACD Line    = EMA(fast) - EMA(slow)     (default: EMA12 - EMA26)
        Signal Line  = EMA of the MACD Line       (default: 9-period EMA)
        Histogram    = MACD Line - Signal Line

    How to read it:
        - MACD crosses ABOVE signal → bullish momentum (buy signal)
        - MACD crosses BELOW signal → bearish momentum (sell signal)
        - Histogram growing → momentum strengthening
        - Histogram shrinking → momentum fading

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.
    fast : int
        Fast EMA period (default 12).
    slow : int
        Slow EMA period (default 26).
    signal : int
        Signal line EMA period (default 9).

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'MACD', 'MACD_Signal', 'MACD_Histogram' columns.
    """
    df = df.copy()
    ema_fast = df["Close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["Close"].ewm(span=slow, adjust=False).mean()
    df["MACD"] = ema_fast - ema_slow
    df["MACD_Signal"] = df["MACD"].ewm(span=signal, adjust=False).mean()
    df["MACD_Histogram"] = df["MACD"] - df["MACD_Signal"]
    return df


def calculate_bollinger(df: pd.DataFrame, window: int = 20, num_std: float = 2.0) -> pd.DataFrame:
    """
    Add Bollinger Bands columns.

    Bollinger Bands measure volatility by placing bands around a moving average:

        Middle Band = SMA(window)
        Upper Band  = SMA + (num_std * standard deviation)
        Lower Band  = SMA - (num_std * standard deviation)

    How to read them:
        - Bands squeeze together → low volatility, breakout coming
        - Price touches upper band → potentially overbought
        - Price touches lower band → potentially oversold
        - Bands widen → volatility expanding, trend strengthening

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.
    window : int
        Moving average window (default 20).
    num_std : float
        Number of standard deviations for bands (default 2.0).

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'BB_Upper', 'BB_Middle', 'BB_Lower', 'BB_Width' columns.
    """
    df = df.copy()
    df["BB_Middle"] = df["Close"].rolling(window=window).mean()
    rolling_std = df["Close"].rolling(window=window).std()
    df["BB_Upper"] = df["BB_Middle"] + (num_std * rolling_std)
    df["BB_Lower"] = df["BB_Middle"] - (num_std * rolling_std)
    # BB_Width as a percentage of middle band — useful for detecting squeezes
    df["BB_Width"] = ((df["BB_Upper"] - df["BB_Lower"]) / df["BB_Middle"]) * 100
    return df


def calculate_volatility(df: pd.DataFrame, windows: list[int] = [20, 60]) -> pd.DataFrame:
    """
    Add historical volatility columns (annualised).

    Historical volatility is the standard deviation of daily returns,
    annualised by multiplying by sqrt(252) (trading days per year).
    It tells you how much the price has been swinging around.

    - High volatility → large daily moves, higher risk
    - Low volatility → calm market, smaller moves
    - Comparing 20-day vs 60-day shows if volatility is rising or falling

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Close' column.
    windows : list[int]
        Rolling window sizes (default [20, 60]).

    Returns
    -------
    pd.DataFrame
        Same DataFrame with 'HV_20', 'HV_60' (etc.) columns as percentages.
    """
    df = df.copy()
    daily_returns = df["Close"].pct_change()
    for w in windows:
        df[f"HV_{w}"] = daily_returns.rolling(window=w).std() * (252 ** 0.5) * 100
    return df


def compute_all_technicals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convenience function: runs all technical indicators on a price DataFrame.

    Returns a new DataFrame with moving averages, RSI, MACD, Bollinger Bands,
    historical volatility, and price change columns added.
    """
    df = add_moving_averages(df)
    df = add_rsi(df)
    df = add_price_changes(df)
    df = calculate_macd(df)
    df = calculate_bollinger(df)
    df = calculate_volatility(df)
    return df
