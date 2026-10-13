"""B15 #455 — Layer 5's day boundary and provenance.

Open-Meteo's `time` axis is local to the pin (`timezone=auto`), so "today"
must be the pin's today, not the CI runner's. Today is a running total
(unfinished max, partial precip sum), so the elapsed side ends at D−1.
And every row — past days included — is weather-model output, never an
observation, so each carries a basis label saying so.
"""

from __future__ import annotations

import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from analysis.weather_alerts import observed_only
from fetchers.weather import fetch_region_weather
from pipeline import store
from pipeline.clean import clean_weather
from pipeline.query import read_weather
from pipeline.store import _migrate_weather_basis


def _fetch(dates: list[str], timezone: str | None, now: pd.Timestamp) -> pd.DataFrame:
    payload: dict = {
        "daily": {
            "time": dates,
            "temperature_2m_max": [30.0] * len(dates),
            "temperature_2m_min": [20.0] * len(dates),
            "precipitation_sum": [1.0] * len(dates),
        }
    }
    if timezone is not None:
        payload["timezone"] = timezone
    resp = MagicMock(status_code=200)
    resp.json.return_value = payload
    with patch("fetchers.weather.requests.get", return_value=resp):
        return fetch_region_weather("Testville", 0.0, 0.0, now=now)


def test_utc_plus_8_pin_after_its_midnight_flags_its_today_not_the_runners():
    # 17:00 UTC on 10-12 is 01:00 on 10-13 in Kuala Lumpur. The runner's
    # calendar says 10-12 is today; the pin's says 10-12 is complete.
    df = _fetch(
        ["2026-10-11", "2026-10-12", "2026-10-13", "2026-10-14"],
        "Asia/Kuala_Lumpur",
        pd.Timestamp("2026-10-12 17:00", tz="UTC"),
    )
    assert list(df["is_forecast"]) == [0, 0, 1, 1]


def test_utc_plus_8_pin_at_01_utc_keeps_its_today_out_of_the_elapsed_side():
    df = _fetch(
        ["2026-10-12", "2026-10-13", "2026-10-14"],
        "Asia/Kuala_Lumpur",
        pd.Timestamp("2026-10-13 01:00", tz="UTC"),
    )
    assert list(df["is_forecast"]) == [0, 1, 1]


def test_utc_minus_4_pin_before_its_midnight_still_has_an_unfinished_day():
    # 01:00 UTC on 10-13 is 21:00 on 10-12 in Cuiabá: the runner calls
    # 10-12 complete, but the pin's 10-12 still has three hours to run.
    df = _fetch(
        ["2026-10-11", "2026-10-12", "2026-10-13"],
        "America/Cuiaba",
        pd.Timestamp("2026-10-13 01:00", tz="UTC"),
    )
    assert list(df["is_forecast"]) == [0, 1, 1]


def test_a_row_dated_the_pins_today_is_excluded_from_observed_only():
    df = _fetch(
        ["2026-10-12", "2026-10-13"],
        "Asia/Kuala_Lumpur",
        pd.Timestamp("2026-10-13 01:00", tz="UTC"),
    )
    assert list(observed_only(df)["Date"]) == ["2026-10-12"]


@pytest.mark.parametrize("timezone", [None, "", "Mars/Olympus_Mons"])
def test_a_response_without_a_resolvable_timezone_hard_fails(timezone):
    with pytest.raises(ValueError, match="timezone"):
        _fetch(["2026-10-12"], timezone, pd.Timestamp("2026-10-13 01:00", tz="UTC"))


def test_a_naive_clock_hard_fails():
    with pytest.raises(ValueError, match="timezone-aware"):
        _fetch(["2026-10-12"], "UTC", pd.Timestamp("2026-10-13 01:00"))


def test_every_row_past_days_included_is_labelled_a_model_estimate():
    # Open-Meteo's past days are model analysis, not station observations
    # (footprint spec §7: never "observed").
    df = _fetch(
        ["2026-10-11", "2026-10-12", "2026-10-13"],
        "Asia/Kuala_Lumpur",
        pd.Timestamp("2026-10-13 01:00", tz="UTC"),
    )
    assert list(df["basis"]) == ["model estimate · Open-Meteo"] * 3


def test_the_basis_label_survives_clean_and_store(patched_db):
    df = _fetch(
        ["2026-10-12", "2026-10-13"],
        "Asia/Kuala_Lumpur",
        pd.Timestamp("2026-10-13 01:00", tz="UTC"),
    )
    store.save_weather_data("Testville", clean_weather(df))
    stored = read_weather("Testville")
    assert list(stored["basis"]) == ["model estimate · Open-Meteo"] * 2
    assert list(stored["is_forecast"]) == [0, 1]


def test_a_legacy_row_with_no_basis_stores_null_not_a_guess(patched_db):
    df = pd.DataFrame({
        "Date": pd.to_datetime(["2026-01-01"]),
        "temp_max": [30.0], "temp_min": [20.0], "precipitation": [1.0],
    })
    store.save_weather_data("Legacy Region", df)
    assert read_weather("Legacy Region")["basis"].isna().all()


def test_migrate_weather_basis_adds_the_column_once():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE weather (region TEXT, Date TEXT, temp_max REAL,"
        " temp_min REAL, precipitation REAL, is_forecast INTEGER,"
        " PRIMARY KEY (region, Date))"
    )
    _migrate_weather_basis(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(weather)").fetchall()}
    assert "basis" in cols
    _migrate_weather_basis(conn)  # idempotent — must not raise
    conn.close()
