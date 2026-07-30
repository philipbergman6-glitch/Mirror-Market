"""USDA FUNDAMENTALS section — year-over-year production/yield."""

import pandas as pd

from pipeline.query import read_usda


def format() -> str:  # noqa: A001
    lines = ["USDA FUNDAMENTALS:"]
    usda_data = read_usda()

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No data"

    # Monthly CRUSHED rows would spam the annual YoY table — they get
    # their own line in the crush section instead.
    if "stat_category" in usda_data.columns:
        usda_data = usda_data[usda_data["stat_category"] != "CRUSHED"].copy()
    if usda_data.empty:
        return "USDA FUNDAMENTALS: No data"

    usda_data["year_int"] = pd.to_numeric(usda_data["year"], errors="coerce")
    usda_data = usda_data.dropna(subset=["year_int"])

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No valid year data"

    years = sorted(usda_data["year_int"].unique())
    if len(years) < 2:
        latest_year = years[-1]
        latest = usda_data[usda_data["year_int"] == latest_year]
        for _, row in latest.head(5).iterrows():
            desc = row.get("short_desc", "")
            val = row.get("Value", "")
            lines.append(f"  {desc}: {val}")
        return "\n".join(lines)

    latest_year = years[-1]
    prev_year = years[-2]
    latest = usda_data[usda_data["year_int"] == latest_year]
    prev = usda_data[usda_data["year_int"] == prev_year]

    for _, row in latest.iterrows():
        desc = row.get("short_desc", "")
        val_str = str(row.get("Value", "")).replace(",", "")
        unit = row.get("unit_desc", "")

        try:
            val = float(val_str)
        except (ValueError, TypeError):
            continue

        prev_match = prev[prev["short_desc"] == desc]
        if prev_match.empty:
            lines.append(f"  {desc}: {val:,.0f} {unit} ({int(latest_year)})")
            continue

        prev_val_str = str(prev_match.iloc[0].get("Value", "")).replace(",", "")
        try:
            prev_val = float(prev_val_str)
        except (ValueError, TypeError):
            lines.append(f"  {desc}: {val:,.0f} {unit} ({int(latest_year)})")
            continue

        if prev_val != 0:
            yoy_pct = ((val - prev_val) / prev_val) * 100
            sign = "+" if yoy_pct >= 0 else ""
            lines.append(f"  {desc}: {val:,.0f} {unit} ({sign}{yoy_pct:.1f}% YoY)")
        else:
            lines.append(f"  {desc}: {val:,.0f} {unit}")

    if len(lines) == 1:
        lines.append("  Data available but no production/yield data found")

    return "\n".join(lines)
