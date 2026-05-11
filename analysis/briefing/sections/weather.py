"""WEATHER ALERTS — threshold-based alerts annotated with anomaly z-scores.

Thresholds (from config.py) decide whether a region is flagged. The z-score —
computed against a trailing 90-day baseline per region — tells you how anomalous
the reading is in context: 25mm of rain in monsoon season is normal, the same
amount during a dry stretch is not. We attach the σ to the alert text so the
reader sees both the raw value and the deviation.
"""

import pandas as pd

from analysis.zscore import format_zscore, zscore
from config import WEATHER_DRY_THRESHOLD_MM, WEATHER_EXTREME_HEAT_C, WEATHER_HEAVY_RAIN_MM
from pipeline.query import read_weather

_LOOKBACK = pd.Timedelta(days=90)


def _zscore_for(subset: pd.DataFrame, column: str, latest_date: pd.Timestamp) -> float | None:
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


def _annotate(text: str, z: float | None) -> str:
    z_text = format_zscore(z)
    return f"{text} [{z_text} vs 90d]" if z_text else text


def format() -> str:  # noqa: A001
    lines = ["WEATHER ALERTS:"]
    weather_data = read_weather()

    if weather_data.empty:
        return "WEATHER ALERTS: No data"

    has_alert = False
    for region in weather_data["region"].unique():
        subset = weather_data[weather_data["region"] == region].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        latest_date = latest["Date"]
        precip = latest.get("precipitation", 0)
        temp_max = latest.get("temp_max", None)

        alerts = []
        if pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM:
            z = _zscore_for(subset, "precipitation", latest_date)
            alerts.append(_annotate(f"Heavy rain ({precip:.0f}mm) — harvest delays possible", z))
        elif pd.notna(precip) and precip < WEATHER_DRY_THRESHOLD_MM:
            z = _zscore_for(subset, "precipitation", latest_date)
            alerts.append(_annotate("Dry conditions — watch soil moisture", z))

        if pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C:
            z = _zscore_for(subset, "temp_max", latest_date)
            alerts.append(_annotate(f"Extreme heat ({temp_max:.0f}C) — crop stress risk", z))

        if alerts:
            has_alert = True
            for alert in alerts:
                lines.append(f"  {region}: {alert}")

    if not has_alert:
        lines.append("  No significant weather alerts")

    return "\n".join(lines)
