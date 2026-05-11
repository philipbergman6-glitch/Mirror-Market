"""COT POSITIONING section — commercials vs. specs with z-scores vs 3-year baseline.

Raw net-position numbers ("commercials net long 120k") aren't actionable on their
own; "net long is +2.3σ vs the 3-year mean" tells you whether the positioning is
historically crowded. We attach both — the raw count for context, the z-score for
the signal.
"""

import pandas as pd

from analysis.zscore import format_zscore, zscore
from pipeline.query import read_cot

_LOOKBACK = pd.Timedelta(days=365 * 3)


def _zscore_for(subset: pd.DataFrame, column: str, latest_date: pd.Timestamp) -> float | None:
    """Z-score of the latest value in `column` vs the trailing 3-year baseline."""
    if column not in subset.columns:
        return None
    cutoff = latest_date - _LOOKBACK
    baseline = subset.loc[
        (subset["Date"] >= cutoff) & (subset["Date"] < latest_date), column
    ]
    latest_value = subset.loc[subset["Date"] == latest_date, column]
    if latest_value.empty:
        return None
    return zscore(float(latest_value.iloc[-1]), baseline)


def _format_side(label: str, value: float, z: float | None) -> str:
    direction = "long" if value > 0 else "short"
    z_text = format_zscore(z)
    suffix = f", {z_text} vs 3yr" if z_text else ""
    return f"{label} net {direction} {abs(value):,.0f}{suffix}"


def format() -> str:  # noqa: A001
    lines = ["COT POSITIONING:"]
    cot_data = read_cot()

    if cot_data.empty:
        return "COT POSITIONING: No data"

    for commodity in cot_data["commodity"].unique():
        subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        latest_date = latest["Date"]
        comm_net = latest.get("commercial_net", None)
        spec_net = latest.get("noncommercial_net", None)

        parts = []
        if pd.notna(comm_net):
            z = _zscore_for(subset, "commercial_net", latest_date)
            parts.append(_format_side("Commercials", comm_net, z))
        if pd.notna(spec_net):
            z = _zscore_for(subset, "noncommercial_net", latest_date)
            parts.append(_format_side("Specs", spec_net, z))

        lines.append(f"  {commodity}: {', '.join(parts)}")

    return "\n".join(lines)
