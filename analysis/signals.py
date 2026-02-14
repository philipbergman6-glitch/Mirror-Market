"""
Trading signal detection.

These functions scan price data for common technical patterns that
traders watch for.  Each signal has a severity level:
    - "info":    notable but not urgent
    - "warning": something traders should be aware of
    - "alert":   strong signal, requires attention

Key concepts for learning:
    - Golden cross: short MA crosses ABOVE long MA → bullish (uptrend)
    - Death cross: short MA crosses BELOW long MA → bearish (downtrend)
    - Volume spike: unusually high trading volume often precedes big moves
    - RSI extremes: overbought/oversold conditions may signal reversals
"""

import pandas as pd


def detect_ma_crossovers(df: pd.DataFrame, commodity: str) -> list[dict]:
    """
    Detect golden cross and death cross events for both 20/50 and 50/200 pairs.

    20/50 crossover: short-term trend shift (moderate signal)
    50/200 crossover: major trend shift — the "big" golden/death cross that
    institutional traders watch (strong signal)

    Parameters
    ----------
    df : pd.DataFrame
        Must have MA_20, MA_50, and ideally MA_200 columns.
    commodity : str
        Name of the commodity for the signal description.

    Returns
    -------
    list[dict]
        Each dict has: date, commodity, signal_type, severity, description
    """
    signals = []

    if len(df) < 2:
        return signals

    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    # --- 20/50 crossover (moderate signal) ---
    if "MA_20" in df.columns and "MA_50" in df.columns:
        if (pd.notna(today["MA_20"]) and pd.notna(today["MA_50"])
                and pd.notna(yesterday["MA_20"]) and pd.notna(yesterday["MA_50"])):
            if yesterday["MA_20"] <= yesterday["MA_50"] and today["MA_20"] > today["MA_50"]:
                signals.append({
                    "date": today.name if hasattr(today.name, "date") else str(today.name),
                    "commodity": commodity,
                    "signal_type": "golden_cross_20_50",
                    "severity": "warning",
                    "description": f"{commodity} golden cross (20-day MA crossed above 50-day MA)",
                })
            if yesterday["MA_20"] >= yesterday["MA_50"] and today["MA_20"] < today["MA_50"]:
                signals.append({
                    "date": today.name if hasattr(today.name, "date") else str(today.name),
                    "commodity": commodity,
                    "signal_type": "death_cross_20_50",
                    "severity": "warning",
                    "description": f"{commodity} death cross (20-day MA crossed below 50-day MA)",
                })

    # --- 50/200 crossover (major signal — the "big" golden/death cross) ---
    if "MA_50" in df.columns and "MA_200" in df.columns:
        if (pd.notna(today["MA_50"]) and pd.notna(today["MA_200"])
                and pd.notna(yesterday["MA_50"]) and pd.notna(yesterday["MA_200"])):
            if yesterday["MA_50"] <= yesterday["MA_200"] and today["MA_50"] > today["MA_200"]:
                signals.append({
                    "date": today.name if hasattr(today.name, "date") else str(today.name),
                    "commodity": commodity,
                    "signal_type": "golden_cross_50_200",
                    "severity": "alert",
                    "description": f"{commodity} MAJOR golden cross (50-day MA crossed above 200-day MA)",
                })
            if yesterday["MA_50"] >= yesterday["MA_200"] and today["MA_50"] < today["MA_200"]:
                signals.append({
                    "date": today.name if hasattr(today.name, "date") else str(today.name),
                    "commodity": commodity,
                    "signal_type": "death_cross_50_200",
                    "severity": "alert",
                    "description": f"{commodity} MAJOR death cross (50-day MA crossed below 200-day MA)",
                })

    return signals


def detect_volume_spikes(df: pd.DataFrame, commodity: str, threshold: float = 2.0) -> list[dict]:
    """
    Detect when today's volume is unusually high.

    A volume spike (> threshold x 20-day average) often means big
    players are trading — could precede a significant price move.

    Parameters
    ----------
    df : pd.DataFrame
        Must have a 'Volume' column.
    commodity : str
        Name of the commodity.
    threshold : float
        Multiple of 20-day average volume to trigger signal (default 2x).

    Returns
    -------
    list[dict]
        Signal dicts if volume spike detected.
    """
    signals = []

    if "Volume" not in df.columns or len(df) < 21:
        return signals

    avg_volume = df["Volume"].iloc[-21:-1].mean()
    today_volume = df["Volume"].iloc[-1]

    if pd.isna(avg_volume) or avg_volume == 0 or pd.isna(today_volume):
        return signals

    ratio = today_volume / avg_volume
    if ratio >= threshold:
        signals.append({
            "date": df.index[-1] if hasattr(df.index[-1], "date") else str(df.index[-1]),
            "commodity": commodity,
            "signal_type": "volume_spike",
            "severity": "info",
            "description": f"{commodity} volume spike ({ratio:.1f}x normal)",
        })

    return signals


def detect_rsi_extremes(df: pd.DataFrame, commodity: str) -> list[dict]:
    """
    Detect overbought (RSI > 70) and oversold (RSI < 30) conditions.

    Parameters
    ----------
    df : pd.DataFrame
        Must have an 'RSI' column (from technical.add_rsi).
    commodity : str
        Name of the commodity.

    Returns
    -------
    list[dict]
        Signal dicts for RSI extremes.
    """
    signals = []

    if "RSI" not in df.columns or df.empty:
        return signals

    latest_rsi = df["RSI"].iloc[-1]
    if pd.isna(latest_rsi):
        return signals

    if latest_rsi > 70:
        signals.append({
            "date": df.index[-1] if hasattr(df.index[-1], "date") else str(df.index[-1]),
            "commodity": commodity,
            "signal_type": "rsi_overbought",
            "severity": "warning",
            "description": f"{commodity} RSI overbought ({latest_rsi:.0f})",
        })
    elif latest_rsi < 30:
        signals.append({
            "date": df.index[-1] if hasattr(df.index[-1], "date") else str(df.index[-1]),
            "commodity": commodity,
            "signal_type": "rsi_oversold",
            "severity": "warning",
            "description": f"{commodity} RSI oversold ({latest_rsi:.0f})",
        })

    return signals


def detect_rsi_divergence(df: pd.DataFrame, commodity: str, lookback: int = 20) -> list[dict]:
    """
    Detect RSI divergence — the most powerful RSI signal.

    Bearish divergence: price makes a new high but RSI makes a lower high.
    This means momentum is fading even though price is still rising — often
    precedes a reversal downward.

    Bullish divergence: price makes a new low but RSI makes a higher low.
    Momentum is improving even though price is falling — often precedes
    a bounce upward.

    Parameters
    ----------
    df : pd.DataFrame
        Must have 'Close' and 'RSI' columns.
    commodity : str
        Name of the commodity.
    lookback : int
        How many days back to compare highs/lows (default 20).

    Returns
    -------
    list[dict]
        Signal dicts for detected divergences.
    """
    signals = []

    if "RSI" not in df.columns or "Close" not in df.columns or len(df) < lookback + 1:
        return signals

    recent = df.iloc[-lookback:]
    current = df.iloc[-1]

    if pd.isna(current["RSI"]):
        return signals

    # Bearish divergence: price at/near high of lookback, RSI below its lookback high
    price_high = recent["Close"].max()
    rsi_at_price_high_idx = recent["Close"].idxmax()
    rsi_at_price_high = recent.loc[rsi_at_price_high_idx, "RSI"] if pd.notna(rsi_at_price_high_idx) else None

    if (pd.notna(rsi_at_price_high)
            and current["Close"] >= price_high * 0.99  # price at/near high
            and current["RSI"] < rsi_at_price_high - 5  # RSI meaningfully lower
            and rsi_at_price_high_idx != df.index[-1]):  # not the same bar
        signals.append({
            "date": current.name if hasattr(current.name, "date") else str(current.name),
            "commodity": commodity,
            "signal_type": "bearish_divergence",
            "severity": "warning",
            "description": f"{commodity} bearish RSI divergence (price near high but RSI falling)",
        })

    # Bullish divergence: price at/near low of lookback, RSI above its lookback low
    price_low = recent["Close"].min()
    rsi_at_price_low_idx = recent["Close"].idxmin()
    rsi_at_price_low = recent.loc[rsi_at_price_low_idx, "RSI"] if pd.notna(rsi_at_price_low_idx) else None

    if (pd.notna(rsi_at_price_low)
            and current["Close"] <= price_low * 1.01  # price at/near low
            and current["RSI"] > rsi_at_price_low + 5  # RSI meaningfully higher
            and rsi_at_price_low_idx != df.index[-1]):  # not the same bar
        signals.append({
            "date": current.name if hasattr(current.name, "date") else str(current.name),
            "commodity": commodity,
            "signal_type": "bullish_divergence",
            "severity": "warning",
            "description": f"{commodity} bullish RSI divergence (price near low but RSI rising)",
        })

    return signals


def detect_macd_crossover(df: pd.DataFrame, commodity: str) -> list[dict]:
    """
    Detect MACD crossover signals.

    Bullish: MACD line crosses above signal line.
    Bearish: MACD line crosses below signal line.

    Parameters
    ----------
    df : pd.DataFrame
        Must have 'MACD' and 'MACD_Signal' columns.
    commodity : str
        Name of the commodity.

    Returns
    -------
    list[dict]
        Signal dicts for MACD crossovers.
    """
    signals = []

    if "MACD" not in df.columns or "MACD_Signal" not in df.columns or len(df) < 2:
        return signals

    today = df.iloc[-1]
    yesterday = df.iloc[-2]

    if (pd.isna(today["MACD"]) or pd.isna(today["MACD_Signal"])
            or pd.isna(yesterday["MACD"]) or pd.isna(yesterday["MACD_Signal"])):
        return signals

    # Bullish crossover
    if yesterday["MACD"] <= yesterday["MACD_Signal"] and today["MACD"] > today["MACD_Signal"]:
        signals.append({
            "date": today.name if hasattr(today.name, "date") else str(today.name),
            "commodity": commodity,
            "signal_type": "macd_bullish",
            "severity": "info",
            "description": f"{commodity} MACD bullish crossover (momentum turning up)",
        })

    # Bearish crossover
    if yesterday["MACD"] >= yesterday["MACD_Signal"] and today["MACD"] < today["MACD_Signal"]:
        signals.append({
            "date": today.name if hasattr(today.name, "date") else str(today.name),
            "commodity": commodity,
            "signal_type": "macd_bearish",
            "severity": "info",
            "description": f"{commodity} MACD bearish crossover (momentum turning down)",
        })

    return signals


def detect_bollinger_squeeze(df: pd.DataFrame, commodity: str) -> list[dict]:
    """
    Detect Bollinger Band squeeze — low volatility often precedes a breakout.

    A squeeze happens when BB_Width drops to its lowest level in 120 days.

    Parameters
    ----------
    df : pd.DataFrame
        Must have 'BB_Width' column.
    commodity : str
        Name of the commodity.

    Returns
    -------
    list[dict]
        Signal dicts for Bollinger squeezes.
    """
    signals = []

    if "BB_Width" not in df.columns or len(df) < 120:
        return signals

    current_width = df["BB_Width"].iloc[-1]
    if pd.isna(current_width):
        return signals

    lookback_min = df["BB_Width"].iloc[-120:].min()
    if pd.notna(lookback_min) and current_width <= lookback_min * 1.05:
        signals.append({
            "date": df.index[-1] if hasattr(df.index[-1], "date") else str(df.index[-1]),
            "commodity": commodity,
            "signal_type": "bollinger_squeeze",
            "severity": "info",
            "description": f"{commodity} Bollinger Band squeeze (volatility at 120-day low — breakout likely)",
        })

    return signals


def detect_all_signals(df: pd.DataFrame, commodity: str) -> list[dict]:
    """
    Run all signal detectors on a single commodity's DataFrame.

    Parameters
    ----------
    df : pd.DataFrame
        Price data with technicals already computed
        (needs MA_20, MA_50, MA_200, RSI, MACD, BB_Width, Volume columns).
    commodity : str
        Name of the commodity.

    Returns
    -------
    list[dict]
        Combined list of all detected signals.
    """
    signals = []
    signals.extend(detect_ma_crossovers(df, commodity))
    signals.extend(detect_volume_spikes(df, commodity))
    signals.extend(detect_rsi_extremes(df, commodity))
    signals.extend(detect_rsi_divergence(df, commodity))
    signals.extend(detect_macd_crossover(df, commodity))
    signals.extend(detect_bollinger_squeeze(df, commodity))
    return signals
