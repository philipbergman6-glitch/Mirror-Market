"""WORLD PRICES (World Bank Monthly) section."""

import pandas as pd

from pipeline.query import read_worldbank_prices


def format() -> str:  # noqa: A001
    lines = ["WORLD PRICES (World Bank Monthly):"]
    wb_data = read_worldbank_prices()

    if wb_data.empty:
        return "WORLD PRICES (World Bank Monthly): No data"

    for commodity in wb_data["commodity"].unique():
        subset = wb_data[wb_data["commodity"] == commodity].sort_values("Date")
        if len(subset) < 2:
            continue

        latest = subset.iloc[-1]
        prev = subset.iloc[-2]
        price = latest["price"]
        unit = latest.get("unit", "")

        if pd.notna(prev["price"]) and prev["price"] != 0:
            chg_pct = ((price - prev["price"]) / prev["price"]) * 100
            sign = "+" if chg_pct >= 0 else ""
            price_str = f"${price:,.0f}/mt" if "mt" in str(unit).lower() else f"{price:,.2f} {unit}"
            lines.append(
                f"  {commodity}: {price_str} ({sign}{chg_pct:.1f}% vs last month)"
            )
        else:
            price_str = f"${price:,.0f}/mt" if "mt" in str(unit).lower() else f"{price:,.2f} {unit}"
            lines.append(f"  {commodity}: {price_str}")

    return "\n".join(lines)
