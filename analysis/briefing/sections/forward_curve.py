"""FORWARD CURVE section — contango/backwardation per commodity."""

import pandas as pd

from analysis.forward_curve import analyze_curve
from pipeline.query import read_forward_curve


def _observed(subset) -> str:
    """The curve's own session(s) (B48 #626): commodities are observed on
    different sessions — ICE US one weekday after CBOT — so the edition date
    is not this line's. Never a guessed date when none was stored."""
    if "observation_date" not in subset.columns:
        return "observation date not recorded"
    dates = sorted({f"{d:%Y-%m-%d}" for d in pd.to_datetime(subset["observation_date"]).dropna()})
    if not dates:
        return "observation date not recorded"
    return "observed " + ", ".join(dates)


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
            lines.append(f"  {commodity}: {result['summary']} — {_observed(subset)}")

    if len(lines) == 1:
        lines.append("  Insufficient contracts for curve analysis")

    return "\n".join(lines)
