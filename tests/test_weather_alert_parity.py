"""#355 — one weather alert rule set, the same answer on every surface.

The briefing graded weather on six rules (heat, heavy rain, dry day, dry
spell, 30-day deficit vs a 90-day baseline, pod-fill heat) while the site's
market block 06, Risk Monitor and Emerging Markets applied three. These tests
seed one fixture that trips each of the six rules and require every surface to
raise exactly the briefing's alerts for the regions it shows.

Dates are pinned (late January), never the wall clock: Mato Grosso's pod fill
is Jan–Feb and the US pins are out of theirs, so the fixture separates
pod-fill heat from extreme heat by calendar, not by luck (#349).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from analysis import soy_analytics
from analysis.briefing.sections import weather as weather_section
from app.block_builders import SiteContext, build_blocks
from app.markets import load_markets
from app.sections import emerging_markets_section, risk_monitor_section
from pipeline import store

LAST_DAY = date(2026, 1, 31)
DAYS = 120

IOWA = "US Midwest (Iowa)"
ILLINOIS = "US Illinois"
NEBRASKA = "US Nebraska"
MATO_GROSSO = "Brazil Mato Grosso"

# The briefing's own wording, written out by hand — the independent source
# the surfaces are checked against.
LABELS = (
    "Extreme heat",
    "Pod-fill heat",
    "Heavy rain",
    "Dry spell",
    "Dry conditions",
    "30d precip deficit",
)

EXPECTED = {
    (IOWA, "Extreme heat"),          # 40C in January: off pod fill, over 38C
    (IOWA, "Heavy rain"),            # 25mm on the latest day
    (IOWA, "30d precip deficit"),    # 25mm in 30d against a 150mm norm
    (ILLINOIS, "Dry spell"),         # 11 straight days under 1mm
    (NEBRASKA, "Dry conditions"),    # 0.5mm today after a wet day
    (MATO_GROSSO, "Pod-fill heat"),  # 35C in January: over the 34C pod-fill bar
}


def _series(precip: list[float], temp_max: list[float]) -> pd.DataFrame:
    dates = pd.date_range(end=pd.Timestamp(LAST_DAY), periods=len(precip))
    return pd.DataFrame({
        "Date": dates,
        "temp_max": temp_max,
        "temp_min": [10.0] * len(precip),
        "precipitation": precip,
        "is_forecast": [0] * len(precip),
    })


def _seed_six_rules() -> None:
    wet, mild = [5.0] * DAYS, [25.0] * DAYS
    store.save_weather_data(IOWA, _series([5.0] * 90 + [0.0] * 29 + [25.0], mild[:-1] + [40.0]))
    store.save_weather_data(ILLINOIS, _series(wet[:-11] + [0.0] * 11, mild))
    store.save_weather_data(NEBRASKA, _series(wet[:-1] + [0.5], mild))
    store.save_weather_data(MATO_GROSSO, _series(wet, mild[:-1] + [35.0]))


def _briefing_alerts() -> set[tuple[str, str]]:
    """(region, label) pairs from the briefing's rendered weather section."""
    out = set()
    for line in weather_section.format().splitlines()[1:]:
        region, _, text = line.strip().partition(": ")
        label = next((lbl for lbl in LABELS if text.startswith(lbl)), None)
        if label is not None:
            out.add((region, label))
    return out


def _restricted(alerts: set[tuple[str, str]], regions) -> set[tuple[str, str]]:
    return {pair for pair in alerts if pair[0] in set(regions)}


@pytest.fixture
def six_rules(patched_db: Path):
    _seed_six_rules()
    return patched_db


def test_the_briefing_raises_all_six_rules(six_rules):
    assert _briefing_alerts() == EXPECTED


@pytest.mark.parametrize("slug", ["cbot", "brazil", "dalian"])
def test_market_weather_block_matches_the_briefing(six_rules, slug):
    registry = load_markets()
    market = registry[slug]
    conn = sqlite3.connect(str(six_rules))
    try:
        ctx = SiteContext(conn=conn, today=LAST_DAY)
        block = next(b for b in build_blocks(market, None, ctx, markets=registry)
                     if b.id == "weather")
    finally:
        conn.close()
    site = {(a["region"], a["alert"]) for a in block.data["alerts"]}
    assert site == _restricted(_briefing_alerts(), market.weather_regions)
    assert site  # every one of these pages carries at least one fixture pin


def test_risk_monitor_matches_the_briefing(six_rules):
    data = risk_monitor_section(soy_analytics.risk_analysis())["data"]
    site = {(a["region"], a["alert"]) for a in data["weather_alerts"]}
    assert site == _restricted(_briefing_alerts(), soy_analytics.SOY_WEATHER_REGIONS)


def test_emerging_markets_matches_the_briefing(six_rules):
    data = emerging_markets_section(soy_analytics.emerging_markets_analysis())["data"]
    brazil = next(c for c in data["countries"] if c["name"] == "Brazil")
    site = {(a["region"], a["alert"]) for a in brazil["weather_alerts"]}
    assert site == {(MATO_GROSSO, "Pod-fill heat")}
    assert site == _restricted(
        _briefing_alerts(), soy_analytics.EMERGING_MARKET_WEATHER["Brazil"]
    )


# ---------------------------------------------------------------------------
# A rule the data cannot answer is withheld with a reason, never "no alert".
#
# CI's weather table carries ~31 observed days (Open-Meteo past_days=30 on a
# fresh runner; weather is not a history table), and the 30-day deficit needs
# 45 baseline days *before* its window. Silence there would read as "rain is
# normal" when the truth is "not measured".
# ---------------------------------------------------------------------------
@pytest.fixture
def thin_history(patched_db: Path):
    store.save_weather_data(IOWA, _series([5.0] * 31, [25.0] * 31))
    return patched_db


def test_briefing_says_the_deficit_was_not_assessed(thin_history):
    out = weather_section.format()
    assert "30d precip deficit not assessed" in out
    assert IOWA in out


def test_market_block_withholds_the_deficit_with_a_reason(thin_history):
    registry = load_markets()
    conn = sqlite3.connect(str(thin_history))
    try:
        ctx = SiteContext(conn=conn, today=LAST_DAY)
        block = next(b for b in build_blocks(registry["cbot"], None, ctx, markets=registry)
                     if b.id == "weather")
    finally:
        conn.close()
    withheld = {w["rule"]: w for w in block.data["withheld"]}
    assert "precip_deficit" in withheld
    assert IOWA in withheld["precip_deficit"]["regions"]
    assert withheld["precip_deficit"]["reason"]


# ---------------------------------------------------------------------------
# Observed only, on the site too: a forecast heatwave is not a reading.
# ---------------------------------------------------------------------------
def test_market_block_ignores_forecast_rows(patched_db: Path):
    observed = _series([5.0] * 40, [25.0] * 40)
    forecast = pd.DataFrame({
        "Date": pd.date_range(start=pd.Timestamp(LAST_DAY) + pd.Timedelta(days=1), periods=3),
        "temp_max": [45.0] * 3, "temp_min": [20.0] * 3,
        "precipitation": [60.0] * 3, "is_forecast": [1] * 3,
    })
    store.save_weather_data(IOWA, pd.concat([observed, forecast], ignore_index=True))
    registry = load_markets()
    conn = sqlite3.connect(str(patched_db))
    try:
        ctx = SiteContext(conn=conn, today=LAST_DAY)
        block = next(b for b in build_blocks(registry["cbot"], None, ctx, markets=registry)
                     if b.id == "weather")
    finally:
        conn.close()
    card = next(r for r in block.data["regions"] if r["region"] == IOWA)
    assert card["as_of"] == LAST_DAY.isoformat()
    assert card["temp_max"] == 25.0
    assert block.data["alerts"] == []


# ---------------------------------------------------------------------------
# One implementation: no surface re-reads the alert thresholds itself.
# ---------------------------------------------------------------------------
THRESHOLDS = (
    "WEATHER_EXTREME_HEAT_C",
    "WEATHER_POD_FILL_HEAT_C",
    "WEATHER_HEAVY_RAIN_MM",
    "WEATHER_DRY_THRESHOLD_MM",
    "WEATHER_DRY_SPELL_ALERT_DAYS",
    "WEATHER_PRECIP_DEFICIT_ALERT_PCT",
)
SURFACES = (
    "app/block_builders.py",
    "app/sections.py",
    "analysis/soy_analytics.py",
    "analysis/briefing/sections/weather.py",
    "analysis/briefing/sections/market_drivers.py",
)


@pytest.mark.parametrize("path", SURFACES)
def test_no_surface_reimplements_the_thresholds(path):
    source = (Path(__file__).resolve().parents[1] / path).read_text()
    assert [name for name in THRESHOLDS if name in source] == []
    assert "def weather_alert(" not in source
