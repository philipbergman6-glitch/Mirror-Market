"""EXPORT INSPECTIONS section — actual shipments vs commitments.

Also surfaces AMS port-area flows (Gulf vs PNW vs Interior tonnage) —
these are export *flows* (where the grain physically shipped from), not
price basis.
"""

import logging

import pandas as pd

from pipeline.query import read_export_sales, read_inspections, read_port_flows

logger = logging.getLogger(__name__)

# Regions shown in the briefing line; other regions (ATLANTIC, LAKES)
# still count toward the total for share-of-total math.
_FLOW_REGIONS = ("GULF", "PACIFIC", "INTERIOR")
_REGION_LABELS = {"GULF": "Gulf", "PACIFIC": "PNW", "INTERIOR": "Interior"}
_FLOW_COMMODITIES = ("Soybeans", "Corn Yellow")


def _region_subtotals(flows: pd.DataFrame, week: pd.Timestamp) -> dict[str, float]:
    """Per-region SUBTOTAL tonnage for one week, keyed by region name."""
    rows = flows[(flows["week_ending"] == week) & (flows["port_area"] == "SUBTOTAL")]
    out: dict[str, float] = {}
    for region, sub in rows.groupby("region"):
        total = sub["inspections_mt"].sum(skipna=True)
        if pd.notna(total):
            out[str(region)] = float(total)
    return out


def _port_flow_lines() -> list[str]:
    """One line per commodity: latest-week regional tonnage, shares, WoW."""
    flows = read_port_flows()
    if flows.empty or "port_area" not in flows.columns:
        return []

    lines: list[str] = []
    for commodity in _FLOW_COMMODITIES:
        subset = flows[flows["commodity"] == commodity]
        if subset.empty:
            continue
        weeks = sorted(subset["week_ending"].dropna().unique())
        if not weeks:
            continue
        latest_week = pd.Timestamp(weeks[-1])
        current = _region_subtotals(subset, latest_week)
        total = sum(current.values())
        if not current or total <= 0:
            continue
        previous = (
            _region_subtotals(subset, pd.Timestamp(weeks[-2])) if len(weeks) >= 2 else {}
        )

        parts = []
        for region in _FLOW_REGIONS:
            vol = current.get(region)
            if vol is None:
                continue
            share = vol / total * 100
            bit = f"{_REGION_LABELS[region]} {vol:,.0f} MT ({share:.0f}%"
            prev = previous.get(region)
            if prev:
                wow = (vol - prev) / prev * 100
                sign = "+" if wow >= 0 else ""
                bit += f", WoW {sign}{wow:.1f}%"
            bit += ")"
            parts.append(bit)
        if not parts:
            continue
        week_str = latest_week.strftime("%m/%d")
        lines.append(f"    {commodity} (w/e {week_str}): {' | '.join(parts)}")

    if lines:
        lines.insert(0, "  US export flows by port region (AMS tonnage, not basis):")
    return lines


def format() -> str:  # noqa: A001
    lines = ["EXPORT INSPECTIONS (USDA Weekly):"]
    insp = read_inspections()

    if insp.empty:
        try:
            flow_lines = _port_flow_lines()
        except Exception as exc:
            logger.debug("Port flow lines failed: %s", exc)
            flow_lines = []
        if flow_lines:
            return "\n".join(["EXPORT INSPECTIONS (USDA Weekly):", *flow_lines])
        return "EXPORT INSPECTIONS: No data"

    es_data = read_export_sales()

    for commodity in insp["commodity"].unique():
        subset = insp[insp["commodity"] == commodity].sort_values("week_ending")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        vol = latest.get("inspections_mt", 0)
        week = latest["week_ending"]
        week_str = week.strftime("%m/%d") if hasattr(week, "strftime") else str(week)

        parts = [f"Inspected: {vol:,.0f} MT (w/e {week_str})"]

        if not es_data.empty:
            es_comm = es_data[es_data["commodity"] == commodity]
            if not es_comm.empty and "outstanding_sales" in es_comm.columns:
                latest_es_week = es_comm["week_ending"].max()
                es_latest = es_comm[es_comm["week_ending"] == latest_es_week]
                outstanding = es_latest["outstanding_sales"].sum()
                if outstanding > 0:
                    parts.append(f"Outstanding sales: {outstanding:,.0f} MT")

        lines.append(f"  {commodity}: {' | '.join(parts)}")

    if len(lines) == 1:
        lines.append("  No inspection data available")

    try:
        lines.extend(_port_flow_lines())
    except Exception as exc:
        logger.debug("Port flow lines failed: %s", exc)

    return "\n".join(lines)
