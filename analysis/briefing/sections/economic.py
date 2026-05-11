"""ECONOMIC CONTEXT (FRED) section + YIELD CURVE block."""

import pandas as pd

from pipeline.query import read_economic


def format_fred() -> str:
    """ECONOMIC CONTEXT (FRED) — dollar index, CPI, Fed funds, etc."""
    lines = ["ECONOMIC CONTEXT (FRED):"]
    econ_data = read_economic()

    if econ_data.empty:
        return "ECONOMIC CONTEXT (FRED): No data"

    for series_name in econ_data["series_name"].unique():
        subset = econ_data[econ_data["series_name"] == series_name].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        value = latest["value"]

        comment = ""
        if len(subset) >= 2:
            prev = subset.iloc[-2]
            if pd.notna(prev["value"]) and prev["value"] != 0:
                chg = value - prev["value"]
                chg_pct = (chg / prev["value"]) * 100
                direction = "up" if chg > 0 else "down"

                if "Dollar" in series_name:
                    impact = "headwind for commodities" if chg > 0 else "tailwind for commodities"
                    comment = f"({direction} {abs(chg_pct):.1f}% — {impact})"
                elif "CPI" in series_name:
                    comment = f"({direction} {abs(chg_pct):.1f}%)"
                elif "Fed Funds" in series_name:
                    comment = f"({value:.2f}% — {'tightening' if chg > 0 else 'easing'})"

        if "Fed Funds" in series_name:
            lines.append(f"  {series_name}: {value:.2f}% {comment}")
        elif "CPI" in series_name:
            lines.append(f"  {series_name}: {value:.1f} {comment}")
        else:
            lines.append(f"  {series_name}: {value:.2f} {comment}")

    if len(lines) == 1:
        lines.append("  Data available but no series matched")

    return "\n".join(lines)


def format_yield_curve() -> str:
    """YIELD CURVE block — 2Y/10Y spread with recession/growth signal."""
    econ_data = read_economic()

    if econ_data.empty:
        return ""

    t2y = econ_data[econ_data["series_name"] == "Treasury 2Y"].sort_values("Date")
    t10y = econ_data[econ_data["series_name"] == "Treasury 10Y"].sort_values("Date")

    if t2y.empty or t10y.empty:
        return ""

    latest_2y = t2y.iloc[-1]["value"]
    latest_10y = t10y.iloc[-1]["value"]

    if pd.isna(latest_2y) or pd.isna(latest_10y):
        return ""

    spread = latest_10y - latest_2y
    if spread < 0:
        assessment = "INVERTED — recession signal, demand destruction risk for commodities"
    elif spread < 0.5:
        assessment = "flat — economic uncertainty"
    else:
        assessment = "normal — growth environment"

    return (
        f"YIELD CURVE:\n"
        f"  2Y: {latest_2y:.2f}%  |  10Y: {latest_10y:.2f}%  |  "
        f"Spread: {spread:+.2f}% ({assessment})"
    )
