"""EXPORT INSPECTIONS section — actual shipments vs commitments."""

from pipeline.query import read_export_sales, read_inspections


def format() -> str:  # noqa: A001
    lines = ["EXPORT INSPECTIONS (USDA Weekly):"]
    insp = read_inspections()

    if insp.empty:
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

    return "\n".join(lines)
