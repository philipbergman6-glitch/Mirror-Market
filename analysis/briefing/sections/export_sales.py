"""EXPORT SALES section — weekly USDA FAS demand data."""

import pandas as pd

from pipeline.query import read_export_sales


def format() -> str:  # noqa: A001
    lines = ["EXPORT SALES (USDA Weekly):"]
    es_data = read_export_sales()

    if es_data.empty:
        return "EXPORT SALES (USDA Weekly): No data (set FAS_API_KEY to enable)"

    for commodity in es_data["commodity"].unique():
        subset = es_data[es_data["commodity"] == commodity]
        if subset.empty:
            continue

        latest_week = subset["week_ending"].max()
        week_data = subset[subset["week_ending"] == latest_week]

        total_net_sales = week_data["net_sales"].sum() if "net_sales" in week_data.columns else 0
        total_exports = week_data["weekly_exports"].sum() if "weekly_exports" in week_data.columns else 0

        top_buyers = week_data.nlargest(3, "net_sales") if "net_sales" in week_data.columns else pd.DataFrame()
        buyer_parts = []
        for _, row in top_buyers.iterrows():
            country = row.get("country", "Unknown")
            sales = row.get("net_sales", 0)
            if pd.notna(sales) and sales != 0:
                buyer_parts.append(f"{country} ({sales:,.0f} MT)")

        parts = [f"Net sales: {total_net_sales:,.0f} MT"]
        if total_exports:
            parts.append(f"Exports: {total_exports:,.0f} MT")
        if buyer_parts:
            parts.append(f"Top buyers: {', '.join(buyer_parts)}")

        week_str = latest_week.strftime("%m/%d") if hasattr(latest_week, "strftime") else str(latest_week)
        lines.append(f"  {commodity} (w/e {week_str}): {' | '.join(parts)}")

    if len(lines) == 1:
        lines.append("  Data available but no sales data found")

    return "\n".join(lines)
