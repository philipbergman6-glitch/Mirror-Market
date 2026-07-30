"""PRICES section — also collects signals and returns enriched DataFrames.

This section is the source of two cross-section dependencies:
  - `signals` are passed down to the SIGNALS section
  - `enriched` (price frames with technical indicators applied) is passed
    to correlations, seasonal, and market_drivers.
"""

import pandas as pd

from analysis.signals import detect_all_signals
from analysis.technical import compute_all_technicals
from config import RSI_OVERBOUGHT, RSI_OVERSOLD
from pipeline.units import mt_label, to_metric_tons


def format(price_data: dict[str, pd.DataFrame]) -> tuple[str, list[dict], dict[str, pd.DataFrame]]:  # noqa: A001
    """Format the PRICES section.

    Returns:
        (text, signals, enriched) where:
            text     — the PRICES section as a string
            signals  — list of signal dicts across all commodities
            enriched — dict[commodity] → DataFrame with technicals applied
    """
    lines = [
        "PRICES:",
        "  Note: technicals computed on yfinance front-month series; "
        "signals within ±3 trading days of contract roll may be data artifacts rather than economic moves.",
    ]
    all_signals: list[dict] = []
    enriched: dict[str, pd.DataFrame] = {}

    for commodity, df in price_data.items():
        if df.empty:
            lines.append(f"  {commodity}: No data")
            continue

        df = compute_all_technicals(df)
        enriched[commodity] = df
        latest = df.iloc[-1]
        close = latest["Close"]
        daily_chg = latest.get("daily_pct_change", 0)
        rsi = latest.get("RSI", None)

        close_mt = to_metric_tons(close, commodity)
        unit = mt_label(commodity)

        parts = [f"{close_mt:,.1f} {unit}"] if close_mt is not None else [f"{close:,.2f}"]
        if pd.notna(daily_chg):
            sign = "+" if daily_chg >= 0 else ""
            parts.append(f"({sign}{daily_chg:.1f}%)")

        ma50 = latest.get("MA_50", None)
        ma200 = latest.get("MA_200", None)
        if pd.notna(ma200):
            if close > ma200:
                parts.append("Above 200-day MA")
            else:
                parts.append("Below 200-day MA")
        elif pd.notna(ma50):
            if close > ma50:
                parts.append("Above 50-day MA")
            else:
                parts.append("Below 50-day MA")

        if pd.notna(rsi):
            if rsi > RSI_OVERBOUGHT:
                parts.append(f"RSI {rsi:.0f} (overbought)")
            elif rsi < RSI_OVERSOLD:
                parts.append(f"RSI {rsi:.0f} (oversold)")

        macd_hist = latest.get("MACD_Histogram", None)
        if pd.notna(macd_hist):
            parts.append(f"MACD {'positive' if macd_hist > 0 else 'negative'}")

        hv20 = latest.get("HV_20", None)
        if pd.notna(hv20):
            parts.append(f"Vol {hv20:.0f}%")

        lines.append(f"  {commodity + ':':16s} {('  '.join(parts))}")

        signals = detect_all_signals(df, commodity)
        all_signals.extend(signals)

    return "\n".join(lines), all_signals, enriched
