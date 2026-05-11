"""FORWARD CURVE section — contango/backwardation per commodity."""

from analysis.forward_curve import analyze_curve
from pipeline.query import read_forward_curve


def format() -> str:  # noqa: A001
    lines = ["FORWARD CURVE:"]
    fc_data = read_forward_curve()

    if fc_data.empty:
        return "FORWARD CURVE: No data"

    for commodity in fc_data["commodity"].unique():
        subset = fc_data[fc_data["commodity"] == commodity]
        if subset.empty or len(subset) < 2:
            continue

        result = analyze_curve(subset)
        if result:
            lines.append(f"  {commodity}: {result['summary']}")

    if len(lines) == 1:
        lines.append("  Insufficient contracts for curve analysis")

    return "\n".join(lines)
