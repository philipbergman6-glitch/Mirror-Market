"""BIOFUEL & ENERGY (EIA) section — ethanol, biodiesel, diesel prices."""

import pandas as pd

from pipeline.query import read_eia_data


def format() -> str:  # noqa: A001
    lines = ["BIOFUEL & ENERGY (EIA):"]
    eia = read_eia_data()

    if eia.empty:
        return "BIOFUEL & ENERGY (EIA): No data (set EIA_API_KEY to enable)"

    for series_name in eia["series_name"].unique():
        subset = eia[eia["series_name"] == series_name].sort_values("Date")
        if subset.empty or len(subset) < 2:
            continue

        latest = subset.iloc[-1]
        prev = subset.iloc[-2]
        value = latest["value"]
        unit = latest.get("unit", "")

        if pd.notna(prev["value"]) and prev["value"] != 0:
            chg_pct = ((value - prev["value"]) / prev["value"]) * 100
            sign = "+" if chg_pct >= 0 else ""
            lines.append(f"  {series_name}: {value:,.2f} {unit} ({sign}{chg_pct:.1f}% vs prior)")
        else:
            lines.append(f"  {series_name}: {value:,.2f} {unit}")

    if len(lines) == 1:
        lines.append("  Data available but no series found")

    return "\n".join(lines)
