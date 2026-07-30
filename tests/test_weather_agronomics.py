"""Tests for the observed/forecast weather split and agronomic alerting.

Covers:
- fetcher is_forecast flag (rows dated after today)
- is_forecast schema migration + save path (NULL for legacy callers)
- clean_weather keeps is_forecast out of the forward-fill
- observed_only / consecutive_dry_days / precip_deficit_30d helpers
- section alerts: forecast exclusion, dry spell, 30d deficit, pod-fill heat
- snapshot weather block observed-only behavior + new fields
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from analysis.briefing.sections import weather as weather_section
from analysis.briefing.sections.weather import (
    consecutive_dry_days,
    heat_threshold_for,
    observed_only,
    precip_deficit_30d,
)
from config import (
    WEATHER_EXTREME_HEAT_C,
    WEATHER_POD_FILL_HEAT_C,
)
from fetchers.weather import fetch_region_weather
from pipeline import store
from pipeline.clean import clean_weather
from pipeline.store import _migrate_weather_is_forecast

# ---------------------------------------------------------------------------
# Fetcher — is_forecast flag
# ---------------------------------------------------------------------------

def test_fetcher_flags_future_rows_as_forecast():
    today = pd.Timestamp.today().normalize()
    dates = [today - pd.Timedelta(days=2), today, today + pd.Timedelta(days=2)]
    payload = {
        "daily": {
            "time": [d.strftime("%Y-%m-%d") for d in dates],
            "temperature_2m_max": [30.0, 31.0, 32.0],
            "temperature_2m_min": [20.0, 21.0, 22.0],
            "precipitation_sum": [0.0, 5.0, 2.0],
        }
    }
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    with patch("fetchers.weather.requests.get", return_value=resp):
        df = fetch_region_weather("Testville", 0.0, 0.0)

    assert list(df["is_forecast"]) == [0, 0, 1]


# ---------------------------------------------------------------------------
# Store — migration + save
# ---------------------------------------------------------------------------

def test_migrate_weather_is_forecast_adds_column_once():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE weather (region TEXT, Date TEXT, temp_max REAL,"
        " temp_min REAL, precipitation REAL, PRIMARY KEY (region, Date))"
    )
    _migrate_weather_is_forecast(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(weather)").fetchall()}
    assert "is_forecast" in cols
    _migrate_weather_is_forecast(conn)  # idempotent — must not raise
    conn.close()


def test_save_weather_without_flag_writes_null(patched_db):
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-01"]),
        "temp_max": [30.0], "temp_min": [20.0], "precipitation": [1.0],
    })
    store.save_weather_data("Legacy Region", df)
    conn = sqlite3.connect(str(patched_db))
    row = conn.execute("SELECT is_forecast FROM weather WHERE region='Legacy Region'").fetchone()
    conn.close()
    assert row[0] is None


def test_save_weather_with_flag_roundtrips(patched_db):
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-01", "2026-01-02"]),
        "temp_max": [30.0, 31.0], "temp_min": [20.0, 21.0],
        "precipitation": [1.0, 2.0], "is_forecast": [0, 1],
    })
    store.save_weather_data("Flagged Region", df)
    conn = sqlite3.connect(str(patched_db))
    rows = conn.execute(
        "SELECT Date, is_forecast FROM weather WHERE region='Flagged Region' ORDER BY Date"
    ).fetchall()
    conn.close()
    assert [r[1] for r in rows] == [0, 1]


# ---------------------------------------------------------------------------
# Clean — flag held out of forward-fill
# ---------------------------------------------------------------------------

def test_clean_weather_does_not_ffill_is_forecast():
    df = pd.DataFrame({
        "Date": ["2026-01-01", "2026-01-02", "2026-01-03"],
        "temp_max": [30.0, np.nan, 32.0],
        "is_forecast": [1, np.nan, 0],
    })
    out = clean_weather(df)
    assert out["temp_max"].iloc[1] == 30.0          # measurements ffilled
    assert pd.isna(out["is_forecast"].iloc[1])      # flag never inherited


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _obs_df(dates, precip, is_forecast=None, temp_max=None):
    n = len(dates)
    return pd.DataFrame({
        "Date": pd.to_datetime(dates),
        "temp_max": temp_max if temp_max is not None else [25.0] * n,
        "temp_min": [15.0] * n,
        "precipitation": precip,
        "is_forecast": is_forecast if is_forecast is not None else [None] * n,
    })


def test_observed_only_treats_null_as_observed():
    df = _obs_df(
        pd.date_range("2026-01-01", periods=4),
        [1.0] * 4,
        is_forecast=[None, 0, 1, 1],
    )
    obs = observed_only(df)
    assert len(obs) == 2
    df_nocol = df.drop(columns=["is_forecast"])
    assert len(observed_only(df_nocol)) == 4


def test_consecutive_dry_days_counts_trailing_streak():
    dates = pd.date_range("2026-01-01", periods=15)
    precip = [5.0, 3.0] + [0.0] * 13
    assert consecutive_dry_days(_obs_df(dates, precip)) == 13
    # NaN breaks the streak
    precip_nan = [0.0] * 10 + [np.nan] + [0.0] * 4
    assert consecutive_dry_days(_obs_df(dates, precip_nan)) == 4
    # wet latest day => zero
    assert consecutive_dry_days(_obs_df(dates, [0.0] * 14 + [8.0])) == 0


def test_precip_deficit_30d_flags_dry_month():
    # 90 baseline days at 4mm/day, then 30 recent days at 0.5mm/day
    dates = pd.date_range("2026-01-01", periods=120)
    precip = [4.0] * 90 + [0.5] * 30
    total, deficit = precip_deficit_30d(_obs_df(dates, precip))
    assert total == 15.0
    assert deficit is not None and deficit < -80


def test_precip_deficit_30d_thin_baseline_returns_none_pct():
    dates = pd.date_range("2026-01-01", periods=40)
    total, deficit = precip_deficit_30d(_obs_df(dates, [1.0] * 40))
    assert total is not None
    assert deficit is None
    assert precip_deficit_30d(pd.DataFrame()) == (None, None)


def test_heat_threshold_pod_fill_months():
    assert heat_threshold_for("US Midwest (Iowa)", 7) == WEATHER_POD_FILL_HEAT_C
    assert heat_threshold_for("US Midwest (Iowa)", 9) == WEATHER_EXTREME_HEAT_C
    assert heat_threshold_for("Brazil Mato Grosso", 1) == WEATHER_POD_FILL_HEAT_C
    assert heat_threshold_for("Brazil Mato Grosso", 7) == WEATHER_EXTREME_HEAT_C
    assert heat_threshold_for("China Heilongjiang", 7) == WEATHER_EXTREME_HEAT_C


# ---------------------------------------------------------------------------
# Section — alert behavior
# ---------------------------------------------------------------------------

def test_section_ignores_forecast_rows(patched_db):
    """A forecast heatwave/downpour must not fire an alert."""
    dates = pd.date_range("2026-06-01", periods=10)
    df = _obs_df(
        dates,
        precip=[2.0] * 7 + [50.0] * 3,
        is_forecast=[0] * 7 + [1] * 3,
        temp_max=[25.0] * 7 + [45.0] * 3,
    )
    store.save_weather_data("China Heilongjiang", df)
    out = weather_section.format()
    assert "Heavy rain" not in out
    assert "heat" not in out.lower().replace("weather", "")
    assert "No significant weather alerts" in out


def test_section_dry_spell_alert(patched_db):
    dates = pd.date_range("2026-06-01", periods=12)
    df = _obs_df(dates, precip=[0.0] * 12)
    store.save_weather_data("China Heilongjiang", df)
    out = weather_section.format()
    assert "Dry spell — 12 consecutive days" in out


def test_section_precip_deficit_alert(patched_db):
    dates = pd.date_range("2026-01-01", periods=120)
    # Wet baseline, then bone-dry 30 days but recent day has 2mm (no dry
    # spell, no single-day dry flag) => only the deficit alert fires.
    precip = [5.0] * 90 + [0.0] * 29 + [2.0]
    df = _obs_df(dates, precip)
    store.save_weather_data("China Heilongjiang", df)
    out = weather_section.format()
    assert "30d precip deficit" in out
    assert "below trailing norm" in out


def test_section_pod_fill_heat_alert(patched_db):
    # July in Iowa: 35C is below the generic 38C bar but above the 34C
    # pod-fill bar.
    dates = pd.date_range("2026-07-01", periods=10)
    df = _obs_df(dates, precip=[3.0] * 10, temp_max=[30.0] * 9 + [35.0])
    store.save_weather_data("US Midwest (Iowa)", df)
    out = weather_section.format()
    assert "Pod-fill heat (35C > 34C)" in out


def test_section_same_temp_no_alert_off_season(patched_db):
    # Same 35C reading in September: below the 38C bar => no heat alert.
    dates = pd.date_range("2026-09-01", periods=10)
    df = _obs_df(dates, precip=[3.0] * 10, temp_max=[30.0] * 9 + [35.0])
    store.save_weather_data("US Midwest (Iowa)", df)
    out = weather_section.format()
    assert "heat" not in out.lower().replace("weather", "")


# ---------------------------------------------------------------------------
# Snapshot — observed-only + new fields
# ---------------------------------------------------------------------------

def test_snapshot_weather_block_observed_only(patched_db):
    from analysis.briefing.snapshot import _weather_block

    dates = pd.date_range("2026-06-01", periods=12)
    df = _obs_df(
        dates,
        precip=[2.0] * 9 + [40.0] * 3,
        is_forecast=[0] * 9 + [1] * 3,
        temp_max=[25.0] * 9 + [45.0] * 3,
    )
    store.save_weather_data("China Heilongjiang", df)
    block = _weather_block()
    row = block["China Heilongjiang"]

    assert row["date"] == "2026-06-09"           # last observed, not forecast
    assert row["temp_max_c"] == 25.0
    assert row["precipitation_mm"] == 2.0
    assert row["forecast_days"] == 3
    assert row["consecutive_dry_days"] == 0
    assert row["precip_30d_mm"] == 18.0
    assert "precip_30d_deficit_pct" in row
