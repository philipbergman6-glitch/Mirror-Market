"""WEATHER ALERTS — the shared rule set, annotated with anomaly z-scores.

The rules live in `analysis.weather_alerts` (#355): this section and every
site surface grade weather with the same six rules, so the briefing and the
market pages cannot disagree about a pin. This module only renders.

The z-score — computed against a trailing 90-day baseline per region — tells
you how anomalous the reading is in context: 25mm of rain in monsoon season is
normal, the same amount during a dry stretch is not. We attach the σ to the
alert text so the reader sees both the raw value and the deviation.

A rule the data cannot answer (the 30-day deficit on a thin record, most
often) prints as "not assessed" with its reason — never as a quiet absence
that reads like normal rain.
"""

import pandas as pd

from analysis.weather_alerts import (
    RULE_DRY_DAY,
    RULE_HEAT,
    RULE_HEAVY_RAIN,
    RULE_POD_FILL_HEAT,
    Withheld,
    assess_region,
    group_withheld,
)
from analysis.zscore import format_zscore, trailing_zscore
from pipeline.query import read_weather

_LOOKBACK = pd.Timedelta(days=90)

# Which reading a rule's σ annotation is computed on. The dry spell and the
# deficit are multi-day aggregates; a one-day σ would misdescribe them.
_ZSCORE_COLUMN = {
    RULE_HEAVY_RAIN: "precipitation",
    RULE_DRY_DAY: "precipitation",
    RULE_POD_FILL_HEAT: "temp_max",
    RULE_HEAT: "temp_max",
}

# Up to this many regions are named on a "not assessed" line; more are counted.
_NAMED_REGIONS = 3


def _annotate(text: str, z: float | None) -> str:
    z_text = format_zscore(z)
    return f"{text} [{z_text} vs 90d]" if z_text else text


def _regions_note(regions: list[str]) -> str:
    if len(regions) <= _NAMED_REGIONS:
        return ", ".join(regions)
    return f"{len(regions)} regions"


def format() -> str:  # noqa: A001
    lines = ["WEATHER ALERTS:"]
    weather_data = read_weather()

    if weather_data.empty:
        return "WEATHER ALERTS: No data"

    has_alert = False
    withheld: list[Withheld] = []
    for region, subset in weather_data.groupby("region", sort=False):
        assessed = assess_region(str(region), subset)
        if assessed is None:
            continue
        withheld.extend(assessed.withheld)
        for alert in assessed.alerts:
            has_alert = True
            column = _ZSCORE_COLUMN.get(alert.rule)
            z = (
                trailing_zscore(assessed.observed, column, assessed.observed["Date"].max(), _LOOKBACK)
                if column else None
            )
            lines.append(f"  {region}: {_annotate(alert.text, z)}")

    if not has_alert:
        lines.append("  No significant weather alerts")

    for group in group_withheld(withheld):
        lines.append(
            f"  {group['label']} not assessed ({_regions_note(group['regions'])}): "
            f"{group['reason']}"
        )

    return "\n".join(lines)
