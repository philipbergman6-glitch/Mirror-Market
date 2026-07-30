"""BRAZIL CROP ESTIMATES (CONAB) section — compares to USDA PSD."""

import pandas as pd

from pipeline.query import read_brazil_estimates, read_psd


def format() -> str:  # noqa: A001
    lines = ["BRAZIL CROP ESTIMATES (CONAB):"]
    brazil = read_brazil_estimates()

    if brazil.empty:
        return "BRAZIL CROP ESTIMATES (CONAB): No data"

    psd = read_psd()

    for commodity in brazil["commodity"].unique():
        subset = brazil[brazil["commodity"] == commodity]
        if subset.empty:
            continue

        latest_year = subset["crop_year"].max()
        latest = subset[subset["crop_year"] == latest_year]

        commodity_parts = []
        for _, row in latest.iterrows():
            attr = row.get("attribute", "")
            val = row.get("value")
            unit = row.get("unit", "")

            if pd.isna(val):
                continue

            part = f"{attr}: {val:,.0f} {unit}"

            if not psd.empty and attr == "Production":
                psd_match = psd[
                    (psd["commodity"] == commodity)
                    & (psd["country"] == "Brazil")
                    & (psd["attribute"] == "Production")
                ]
                if not psd_match.empty:
                    psd_latest = psd_match[psd_match["year"] == psd_match["year"].max()]
                    if not psd_latest.empty:
                        usda_val = psd_latest.iloc[0]["value"]
                        usda_unit = str(psd_latest.iloc[0].get("unit", "") or "")
                        if pd.notna(usda_val):
                            # Only derive a gap when both legs are metric tons.
                            # PSD reports cotton in 1000 480-lb bales vs CONAB's
                            # 1000 MT lint — subtracting those fabricates a gap.
                            if "MT" in usda_unit.upper():
                                gap = val - usda_val
                                part += f" (vs USDA {usda_val:,.0f} — gap: {gap:+,.0f})"
                            else:
                                part += (
                                    f" (vs USDA {usda_val:,.0f} {usda_unit.strip()}"
                                    " — units differ, no gap)"
                                )

            commodity_parts.append(f"    {part}")

        if commodity_parts:
            lines.append(f"  {commodity} ({latest_year}):")
            lines.extend(commodity_parts)

    if len(lines) == 1:
        lines.append("  No CONAB estimate data available")

    return "\n".join(lines)
