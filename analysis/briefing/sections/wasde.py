"""WASDE ESTIMATES section — monthly USDA supply/demand forecasts."""

import pandas as pd

from pipeline.query import read_wasde


def format() -> str:  # noqa: A001
    lines = ["WASDE ESTIMATES (USDA Monthly Forecasts):"]
    wasde = read_wasde()

    if wasde.empty:
        return "WASDE ESTIMATES: No data"

    for commodity in wasde["commodity"].unique():
        subset = wasde[wasde["commodity"] == commodity]
        if subset.empty:
            continue

        commodity_lines = []
        for attribute in subset["attribute"].unique():
            attr_data = subset[subset["attribute"] == attribute].copy()
            if attr_data.empty:
                continue

            # WASDE rows include multiple marketing years per release (e.g. 2024/25
            # Est. and 2025/26 Proj. in the same April report). Pin to the latest
            # MY so the MoM revision math compares like with like.
            latest_my = attr_data["year"].max()
            attr_data = attr_data[attr_data["year"] == latest_my]
            attr_data = attr_data.sort_values("reference_period")
            latest = attr_data.iloc[-1]
            val = latest.get("value")
            unit = latest.get("unit", "")
            ref = latest.get("reference_period", "")

            if pd.isna(val):
                continue

            if len(attr_data) >= 2:
                prev = attr_data.iloc[-2]
                prev_val = prev.get("value")
                if pd.notna(prev_val) and prev_val != 0:
                    revision = val - prev_val
                    sign = "+" if revision >= 0 else ""
                    direction = "UP" if revision > 0 else "DOWN"
                    commodity_lines.append(
                        f"    {attribute}: {val:,.0f} {unit} "
                        f"(revised {direction} {sign}{revision:,.0f} vs prior month)"
                    )
                    continue

            commodity_lines.append(f"    {attribute}: {val:,.0f} {unit} ({ref})")

        if commodity_lines:
            lines.append(f"  {commodity}:")
            lines.extend(commodity_lines)

    if len(lines) == 1:
        lines.append("  Data available but no forecast data found")

    return "\n".join(lines)
