"""Weather alert rules — one rule set, one implementation (#355).

Every surface that grades weather asks this module: the briefing's weather
section, market block 06, the Risk Monitor, Emerging Markets, the headline's
competing-oil strip and the market-drivers "weather premium" line. Before
#355 the site applied three of the briefing's six rules and printed a
different answer for the same pin on the same day.

The six observed rules (thresholds in config.py — this module adds none):

    heavy rain      latest precip > WEATHER_HEAVY_RAIN_MM
    dry spell       trailing run of days < WEATHER_DRY_THRESHOLD_MM reaches
                    WEATHER_DRY_SPELL_ALERT_DAYS
    dry day         latest precip < WEATHER_DRY_THRESHOLD_MM
    30d deficit     30-day total ≥ WEATHER_PRECIP_DEFICIT_ALERT_PCT below the
                    region's own norm over the preceding 90-day baseline
    pod-fill heat   latest temp_max > WEATHER_POD_FILL_HEAT_C in the region's
                    WEATHER_SOY_POD_FILL_MONTHS
    extreme heat    latest temp_max > WEATHER_EXTREME_HEAT_C otherwise

Precedence is the briefing's: heavy rain, dry spell and dry day are one chain
(at most one fires); the deficit and the heat bar are independent of it.

Observed only. A forecast heatwave is not a reading, so `is_forecast = 1`
rows are dropped before any rule runs (NULL = observed, pre-flag data).

A rule the data cannot answer is **withheld with a reason**, never reported
as "no alert". The deficit needs WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS
observed days in the baseline before its 30-day window; a fresh CI runner
holds ~31 observed days (Open-Meteo `past_days=30`, and weather is not a
history table), so on today's pipeline that rule is withheld everywhere —
and every surface says so.

Alert classes. Every alert carries a ``basis`` naming what it was graded on
and a ``severity`` on the shared alert > warning > info scale. Observed alerts
are ``warning``: that is how every site surface has always rendered them, so
this records the existing grade rather than inventing one. A second class —
the footprint forecast of map #356 ("forecast · ECMWF 00z", capped at
``warning``, K2 #361) — is a separate assessor returning the same
``WeatherAlert`` type with its own basis; it must not be folded into
``assess_region``, which is observed-only by contract.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

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

OBSERVED = "observed"

SEVERITIES = ("alert", "warning", "info")

RULE_HEAVY_RAIN = "heavy_rain"
RULE_DRY_SPELL = "dry_spell"
RULE_DRY_DAY = "dry_day"
RULE_PRECIP_DEFICIT = "precip_deficit"
RULE_POD_FILL_HEAT = "pod_fill_heat"
RULE_HEAT = "heat"

# The briefing's wording, shared so a reader sees one name per rule.
RULE_LABELS = {
    RULE_HEAVY_RAIN: "Heavy rain",
    RULE_DRY_SPELL: "Dry spell",
    RULE_DRY_DAY: "Dry conditions",
    RULE_PRECIP_DEFICIT: "30d precip deficit",
    RULE_POD_FILL_HEAT: "Pod-fill heat",
    RULE_HEAT: "Extreme heat",
}

# Calendar days of history behind the latest observation that every rule
# needs to be answerable — what a windowed reader must fetch at minimum.
LOOKBACK_DAYS = WEATHER_PRECIP_DEFICIT_WINDOW_DAYS + WEATHER_PRECIP_DEFICIT_BASELINE_DAYS

_REQUIRED_COLUMNS = ("Date", "temp_max", "precipitation")


@dataclass(frozen=True)
class WeatherAlert:
    region: str
    rule: str
    text: str          # the reading against its bar, in the briefing's words
    as_of: date
    basis: str = OBSERVED
    severity: str = "warning"

    def __post_init__(self) -> None:
        if self.rule not in RULE_LABELS:
            raise ValueError(f"unknown weather rule {self.rule!r}")
        if self.severity not in SEVERITIES:
            raise ValueError(f"unknown severity {self.severity!r}")

    @property
    def label(self) -> str:
        return RULE_LABELS[self.rule]

    def as_dict(self) -> dict:
        return {
            "region": self.region,
            "rule": self.rule,
            "alert": self.label,
            "text": self.text,
            "as_of": self.as_of.isoformat(),
            "basis": self.basis,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class Withheld:
    """A rule this region's data cannot answer — not a clear reading."""

    region: str
    rule: str
    reason: str

    def __post_init__(self) -> None:
        if self.rule not in RULE_LABELS:
            raise ValueError(f"unknown weather rule {self.rule!r}")
        if not self.reason:
            raise ValueError(f"withheld {self.rule} for {self.region} carries no reason")


@dataclass(frozen=True)
class RegionWeather:
    """One region graded on its latest observed day."""

    region: str
    as_of: date
    temp_max: float | None
    temp_min: float | None
    precip: float | None
    dry_days: int
    precip_30d_mm: float | None
    deficit_pct: float | None
    alerts: tuple[WeatherAlert, ...]
    withheld: tuple[Withheld, ...]
    observed: pd.DataFrame = field(repr=False, compare=False)


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


def _precip_deficit(observed: pd.DataFrame) -> tuple[float | None, float | None, str | None]:
    """(total_30d_mm, deficit_pct, reason the pct could not be computed)."""
    if observed.empty or "precipitation" not in observed.columns:
        return None, None, "no observed precipitation rows"
    observed = observed.sort_values("Date")
    latest_date = observed["Date"].max()
    if pd.isna(latest_date):
        return None, None, "no dated observed rows"

    window = pd.Timedelta(days=WEATHER_PRECIP_DEFICIT_WINDOW_DAYS)
    recent_cut = latest_date - window
    recent = observed.loc[observed["Date"] > recent_cut, "precipitation"].dropna()
    if recent.empty:
        return None, None, (
            f"no precipitation readings in the last {WEATHER_PRECIP_DEFICIT_WINDOW_DAYS} days"
        )
    total_30d = float(recent.sum())

    baseline_cut = recent_cut - pd.Timedelta(days=WEATHER_PRECIP_DEFICIT_BASELINE_DAYS)
    baseline = observed.loc[
        (observed["Date"] > baseline_cut) & (observed["Date"] <= recent_cut),
        "precipitation",
    ].dropna()
    if len(baseline) < WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS:
        return total_30d, None, (
            f"{len(baseline)} of {WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS} observed days "
            f"needed in the {WEATHER_PRECIP_DEFICIT_BASELINE_DAYS}-day baseline before "
            f"the {WEATHER_PRECIP_DEFICIT_WINDOW_DAYS}-day window"
        )

    norm = float(baseline.mean()) * WEATHER_PRECIP_DEFICIT_WINDOW_DAYS
    if norm <= 0:
        return total_30d, None, "the baseline recorded no rain, so a deficit has no base"
    return total_30d, (total_30d - norm) / norm * 100, None


def precip_deficit_30d(observed: pd.DataFrame) -> tuple[float | None, float | None]:
    """(total_30d_mm, deficit_pct) for the trailing 30 observed-window days.

    deficit_pct is the % difference of the 30-day total vs the region's own
    trailing norm (mean daily precip over the preceding baseline window,
    scaled to 30 days). Negative = drier than normal. Returns (None, None)
    when there's no data; (total, None) when the baseline is too thin or has
    zero norm.
    """
    total, pct, _ = _precip_deficit(observed)
    return total, pct


def heat_threshold_for(region: str, month: int) -> float:
    """Heat-stress bar for a region/month — lower during soy pod fill."""
    pod_fill_months = WEATHER_SOY_POD_FILL_MONTHS.get(region)
    if pod_fill_months and month in pod_fill_months:
        return WEATHER_POD_FILL_HEAT_C
    return WEATHER_EXTREME_HEAT_C


def _num(value) -> float | None:
    return None if value is None or pd.isna(value) else float(value)


def _validated(region: str, rows: pd.DataFrame) -> pd.DataFrame:
    missing = [col for col in _REQUIRED_COLUMNS if col not in rows.columns]
    if missing:
        raise ValueError(f"weather rows for {region} lack columns {missing}")
    if "region" in rows.columns:
        others = set(rows["region"].dropna().astype(str)) - {region}
        if others:
            raise ValueError(f"weather rows for {region} carry other regions: {sorted(others)}")
    rows = rows.copy()
    rows["Date"] = pd.to_datetime(rows["Date"], errors="raise")
    return rows.sort_values("Date")


def assess_region(region: str, rows: pd.DataFrame) -> RegionWeather | None:
    """Grade one region on its latest observed day.

    ``rows`` is that region's weather rows — observed and forecast alike,
    any order; forecast rows are dropped here so no caller can forget to.
    Returns None when there is no observed row at all (the caller names the
    gap). Raises on rows that are not this region's or lack the columns the
    rules read.
    """
    if rows.empty:
        return None
    observed = observed_only(_validated(region, rows))
    if observed.empty:
        return None

    latest = observed.iloc[-1]
    as_of = latest["Date"].date()
    precip = _num(latest.get("precipitation"))
    temp_max = _num(latest.get("temp_max"))
    dry_days = consecutive_dry_days(observed)
    total_30d, deficit_pct, deficit_reason = _precip_deficit(observed)

    alerts: list[WeatherAlert] = []
    withheld: list[Withheld] = []

    def fire(rule: str, text: str) -> None:
        alerts.append(WeatherAlert(region, rule, text, as_of))

    def withhold(rule: str, reason: str) -> None:
        withheld.append(Withheld(region, rule, reason))

    # Precipitation chain — heavy rain, then dry spell, then dry day.
    if precip is None:
        for rule in (RULE_HEAVY_RAIN, RULE_DRY_SPELL, RULE_DRY_DAY):
            withhold(rule, f"the latest observed day ({as_of}) has no precipitation reading")
    elif precip > WEATHER_HEAVY_RAIN_MM:
        fire(RULE_HEAVY_RAIN, f"Heavy rain ({precip:.0f}mm) — harvest delays possible")
    elif dry_days >= WEATHER_DRY_SPELL_ALERT_DAYS:
        fire(
            RULE_DRY_SPELL,
            f"Dry spell — {dry_days} consecutive days <{WEATHER_DRY_THRESHOLD_MM}mm"
            " — soil moisture depleting",
        )
    else:
        if precip < WEATHER_DRY_THRESHOLD_MM:
            fire(RULE_DRY_DAY, "Dry conditions — watch soil moisture")
        if 0 < dry_days == len(observed):
            # Every observed day is dry and the record is shorter than a
            # spell: the run may have started before the data did.
            withhold(
                RULE_DRY_SPELL,
                f"all {dry_days} observed days are dry and the record starts there — "
                f"a {WEATHER_DRY_SPELL_ALERT_DAYS}-day spell cannot be ruled out",
            )

    if deficit_reason is not None:
        withhold(RULE_PRECIP_DEFICIT, deficit_reason)
    elif deficit_pct is not None and deficit_pct <= -WEATHER_PRECIP_DEFICIT_ALERT_PCT:
        fire(
            RULE_PRECIP_DEFICIT,
            f"30d precip deficit — {total_30d:.0f}mm, {abs(deficit_pct):.0f}% below trailing norm",
        )

    heat_bar = heat_threshold_for(region, as_of.month)
    pod_fill = heat_bar == WEATHER_POD_FILL_HEAT_C and heat_bar < WEATHER_EXTREME_HEAT_C
    heat_rule = RULE_POD_FILL_HEAT if pod_fill else RULE_HEAT
    if temp_max is None:
        withhold(heat_rule, f"the latest observed day ({as_of}) has no max temperature")
    elif temp_max > heat_bar:
        if pod_fill:
            fire(heat_rule, f"Pod-fill heat ({temp_max:.0f}C > {heat_bar:.0f}C) — pod abortion risk")
        else:
            fire(heat_rule, f"Extreme heat ({temp_max:.0f}C) — crop stress risk")

    return RegionWeather(
        region=region,
        as_of=as_of,
        temp_max=temp_max,
        temp_min=_num(latest.get("temp_min")),
        precip=precip,
        dry_days=dry_days,
        precip_30d_mm=total_30d,
        deficit_pct=deficit_pct,
        alerts=tuple(alerts),
        withheld=tuple(withheld),
        observed=observed,
    )


def group_withheld(withheld: Iterable[Withheld]) -> list[dict]:
    """Withheld rules grouped by (rule, reason) so a surface prints one line
    per distinct gap rather than one per region."""
    groups: dict[tuple[str, str], list[str]] = {}
    for item in withheld:
        groups.setdefault((item.rule, item.reason), []).append(item.region)
    return [
        {"rule": rule, "label": RULE_LABELS[rule], "reason": reason, "regions": regions}
        for (rule, reason), regions in groups.items()
    ]
