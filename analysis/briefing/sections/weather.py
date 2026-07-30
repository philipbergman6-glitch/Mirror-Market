"""WEATHER ALERTS — threshold-based alerts annotated with anomaly z-scores.

Thresholds (from config.py) decide whether a region is flagged. The z-score —
computed against a trailing 90-day baseline per region — tells you how anomalous
the reading is in context: 25mm of rain in monsoon season is normal, the same
amount during a dry stretch is not. We attach the σ to the alert text so the
reader sees both the raw value and the deviation.

Observed vs forecast: the weather table carries an `is_forecast` flag (1 =
Open-Meteo model forecast for a future date). All alerting and z-score math
runs on observed rows only — a forecast heatwave is not a reading. Rows with
NULL is_forecast (written before the flag existed) are treated as observed.

Agronomic alerts beyond the single-day thresholds:
    - Dry spell: trailing consecutive observed days with precip below
      WEATHER_DRY_THRESHOLD_MM; alert at WEATHER_DRY_SPELL_ALERT_DAYS.
    - 30-day precip deficit: total observed precip over the last 30 days vs
      the region's own trailing norm (mean daily precip over the preceding
      baseline window, scaled to 30 days).
    - Pod-fill heat: soy regions use the lower WEATHER_POD_FILL_HEAT_C bar
      during their pod-fill months (US: Jul-Aug; South America: Jan-Feb).
"""

import pandas as pd

from analysis.zscore import format_zscore, trailing_zscore
from config import (
    WEATHER_DRY_SPELL_ALERT_DAYS,
    WEATHER_DRY_THRESHOLD_MM,
    WEATHER_EXTREME_HEAT_C,
    WEATHER_HEAVY_RAIN_MM,
    WEATHER_POD_FILL_HEAT_C,
    WEATHER_PRECIP_DEFICIT_ALERT_PCT,
    WEATHER_PRECIP_DEFICIT_BASELINE_DAYS,
    WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS,
    WEATHER_PRECIP_DEFICIT_WINDOW_DAYS,
    WEATHER_SOY_POD_FILL_MONTHS,
)
from pipeline.query import read_weather

_LOOKBACK = pd.Timedelta(days=90)


def observed_only(subset: pd.DataFrame) -> pd.DataFrame:
    """Rows that are observations, not forecasts.

    NULL / missing `is_forecast` (rows written before the flag existed)
    counts as observed.
    """
    if subset.empty or "is_forecast" not in subset.columns:
        return subset
    flag = pd.to_numeric(subset["is_forecast"], errors="coerce").fillna(0)
    return subset[flag == 0]


def consecutive_dry_days(observed: pd.DataFrame) -> int:
    """Trailing consecutive observed days with precip < WEATHER_DRY_THRESHOLD_MM.

    A missing precip reading breaks the streak (conservative — we don't
    assume a gap was dry).
    """
    if observed.empty or "precipitation" not in observed.columns:
        return 0
    precip = observed.sort_values("Date")["precipitation"]
    count = 0
    for val in reversed(precip.tolist()):
        if pd.notna(val) and val < WEATHER_DRY_THRESHOLD_MM:
            count += 1
        else:
            break
    return count


def precip_deficit_30d(observed: pd.DataFrame) -> tuple[float | None, float | None]:
    """(total_30d_mm, deficit_pct) for the trailing 30 observed-window days.

    deficit_pct is the % difference of the 30-day total vs the region's own
    trailing norm (mean daily precip over the preceding baseline window,
    scaled to 30 days). Negative = drier than normal. Returns (None, None)
    when there's no data; (total, None) when the baseline is too thin or has
    zero norm.
    """
    if observed.empty or "precipitation" not in observed.columns:
        return None, None
    observed = observed.sort_values("Date")
    latest_date = observed["Date"].max()
    if pd.isna(latest_date):
        return None, None

    window = pd.Timedelta(days=WEATHER_PRECIP_DEFICIT_WINDOW_DAYS)
    recent_cut = latest_date - window
    recent = observed.loc[observed["Date"] > recent_cut, "precipitation"].dropna()
    if recent.empty:
        return None, None
    total_30d = float(recent.sum())

    baseline_cut = recent_cut - pd.Timedelta(days=WEATHER_PRECIP_DEFICIT_BASELINE_DAYS)
    baseline = observed.loc[
        (observed["Date"] > baseline_cut) & (observed["Date"] <= recent_cut),
        "precipitation",
    ].dropna()
    if len(baseline) < WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS:
        return total_30d, None

    norm = float(baseline.mean()) * WEATHER_PRECIP_DEFICIT_WINDOW_DAYS
    if norm <= 0:
        return total_30d, None
    return total_30d, (total_30d - norm) / norm * 100


def heat_threshold_for(region: str, month: int) -> float:
    """Heat-stress bar for a region/month — lower during soy pod fill."""
    pod_fill_months = WEATHER_SOY_POD_FILL_MONTHS.get(region)
    if pod_fill_months and month in pod_fill_months:
        return WEATHER_POD_FILL_HEAT_C
    return WEATHER_EXTREME_HEAT_C


def _zscore_for(subset: pd.DataFrame, column: str, latest_date: pd.Timestamp) -> float | None:
    return trailing_zscore(subset, column, latest_date, _LOOKBACK)


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
        observed = observed_only(subset)
        if observed.empty:
            continue

        latest = observed.iloc[-1]
        latest_date = latest["Date"]
        precip = latest.get("precipitation", 0)
        temp_max = latest.get("temp_max", None)
        dry_days = consecutive_dry_days(observed)
        total_30d, deficit_pct = precip_deficit_30d(observed)

        alerts = []
        if pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM:
            z = _zscore_for(observed, "precipitation", latest_date)
            alerts.append(_annotate(f"Heavy rain ({precip:.0f}mm) — harvest delays possible", z))
        elif dry_days >= WEATHER_DRY_SPELL_ALERT_DAYS:
            alerts.append(
                f"Dry spell — {dry_days} consecutive days <{WEATHER_DRY_THRESHOLD_MM}mm"
                " — soil moisture depleting"
            )
        elif pd.notna(precip) and precip < WEATHER_DRY_THRESHOLD_MM:
            z = _zscore_for(observed, "precipitation", latest_date)
            alerts.append(_annotate("Dry conditions — watch soil moisture", z))

        if deficit_pct is not None and deficit_pct <= -WEATHER_PRECIP_DEFICIT_ALERT_PCT:
            alerts.append(
                f"30d precip deficit — {total_30d:.0f}mm, "
                f"{abs(deficit_pct):.0f}% below trailing norm"
            )

        heat_bar = heat_threshold_for(str(region), latest_date.month)
        if pd.notna(temp_max) and temp_max > heat_bar:
            z = _zscore_for(observed, "temp_max", latest_date)
            if heat_bar == WEATHER_POD_FILL_HEAT_C and heat_bar < WEATHER_EXTREME_HEAT_C:
                text = f"Pod-fill heat ({temp_max:.0f}C > {heat_bar:.0f}C) — pod abortion risk"
            else:
                text = f"Extreme heat ({temp_max:.0f}C) — crop stress risk"
            alerts.append(_annotate(text, z))

        if alerts:
            has_alert = True
            for alert in alerts:
                lines.append(f"  {region}: {alert}")

    if not has_alert:
        lines.append("  No significant weather alerts")

    return "\n".join(lines)
