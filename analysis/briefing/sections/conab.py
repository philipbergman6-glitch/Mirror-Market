"""BRAZIL CROP ESTIMATES (CONAB) section — compares to USDA PSD, one crop year.

The CONAB → PSD Market_Year mapping is registry data on the Brazil market
descriptor (#403); the gap is struck only where PSD carries the mapped
year, otherwise the line says PSD has not published that crop yet.
"""

import pandas as pd

from analysis.crop_year import is_split_crop_year, psd_year_for_crop_year, psd_year_label
from config import MARKETS
from pipeline.query import read_brazil_estimates, read_psd


def format() -> str:  # noqa: A001
    lines = ["BRAZIL CROP ESTIMATES (CONAB):"]
    brazil = read_brazil_estimates()

    if brazil.empty:
        return "BRAZIL CROP ESTIMATES (CONAB): No data"

    psd = read_psd()
    market = MARKETS["brazil"]
    offset = int(market["crop_estimates"]["psd_year_offset"])
    psd_country = market["psd_country"]

    for commodity in brazil["commodity"].unique():
        subset = brazil[brazil["commodity"] == commodity]
        if subset.empty:
            continue

        latest_year = subset["crop_year"].max()
        latest = subset[subset["crop_year"] == latest_year]
        if "report_date" in latest.columns:
            latest = latest[latest["report_date"] == latest["report_date"].max()]
        # CONAB labels some crops by calendar year (wheat: "2025"); no PSD
        # mapping is defined for that convention, so the PSD leg is skipped
        # for the commodity and the line says so. Never a guessed year.
        split_label = is_split_crop_year(latest_year)
        psd_year = psd_year_for_crop_year(str(latest_year), offset) if split_label else None
        psd_label = psd_year_label(psd_year) if psd_year is not None else None

        commodity_parts = []
        for _, row in latest.iterrows():
            attr = row.get("attribute", "")
            val = row.get("value")
            unit = row.get("unit", "")

            if pd.isna(val):
                continue

            part = f"{attr}: {val:,.0f} {unit}"

            if attr == "Production" and not split_label:
                part += (
                    f" (no USDA comparison: CONAB labels {commodity} by calendar"
                    f" year '{latest_year}', and no PSD mapping is defined for that)"
                )
            elif not psd.empty and attr == "Production":
                psd_match = psd[
                    (psd["commodity"] == commodity)
                    & (psd["country"] == psd_country)
                    & (psd["attribute"] == "Production")
                    & psd["value"].notna()
                ]
                mapped = psd_match[psd_match["year"].astype(int) == psd_year]
                if psd_match.empty:
                    pass
                elif mapped.empty:
                    part += (
                        f" (USDA PSD has not published {psd_label} yet — no gap;"
                        f" PSD's newest year is MY{int(psd_match['year'].max())})"
                    )
                else:
                    usda_val = mapped.iloc[-1]["value"]
                    usda_unit = str(mapped.iloc[-1].get("unit", "") or "")
                    # Only derive a gap when both legs are metric tons.
                    # PSD reports cotton in 1000 480-lb bales vs CONAB's
                    # 1000 MT lint — subtracting those fabricates a gap.
                    if "MT" in usda_unit.upper():
                        gap = val - usda_val
                        part += f" (vs USDA {usda_val:,.0f} for {psd_label} — gap: {gap:+,.0f})"
                    else:
                        part += (
                            f" (vs USDA {usda_val:,.0f} {usda_unit.strip()} for {psd_label}"
                            " — units differ, no gap)"
                        )

            commodity_parts.append(f"    {part}")

        if commodity_parts:
            lines.append(f"  {commodity} ({latest_year}):")
            lines.extend(commodity_parts)

    if len(lines) == 1:
        lines.append("  No CONAB estimate data available")

    return "\n".join(lines)
