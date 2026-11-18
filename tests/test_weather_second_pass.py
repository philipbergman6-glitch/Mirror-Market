"""#577: a region lost to transient Open-Meteo throttling gets one more pass."""

from __future__ import annotations

import pandas as pd

import fetchers.weather as weather


def _row() -> pd.DataFrame:
    return pd.DataFrame({"Date": ["2026-11-15"], "temp_max": [30.0]})


def test_empty_region_is_retried_once_after_the_pause(monkeypatch) -> None:
    calls: list[str] = []
    sleeps: list[float] = []
    flaky = "South Africa Mpumalanga"

    def fake(region_name, lat, lon):
        calls.append(region_name)
        if region_name == flaky and calls.count(flaky) == 1:
            return pd.DataFrame()
        return _row()

    monkeypatch.setattr(weather, "fetch_region_weather", fake)
    monkeypatch.setattr(weather.time, "sleep", sleeps.append)

    results = weather.fetch_all_regions()

    assert all(not df.empty for df in results.values())
    assert calls.count(flaky) == 2
    assert len(calls) == len(weather.GROWING_REGIONS) + 1
    assert sleeps == [weather.WEATHER_SECOND_PASS_DELAY_S]


def test_no_pause_when_every_region_answered(monkeypatch) -> None:
    sleeps: list[float] = []
    monkeypatch.setattr(weather, "fetch_region_weather", lambda region_name, lat, lon: _row())
    monkeypatch.setattr(weather.time, "sleep", sleeps.append)

    weather.fetch_all_regions()

    assert sleeps == []


def test_region_still_empty_after_second_pass_stays_empty(monkeypatch) -> None:
    """Never patched: the miss travels on as an empty frame (usable_partial)."""
    dead = "Nigeria Kaduna"
    monkeypatch.setattr(
        weather, "fetch_region_weather",
        lambda region_name, lat, lon: pd.DataFrame() if region_name == dead else _row(),
    )
    monkeypatch.setattr(weather.time, "sleep", lambda s: None)

    results = weather.fetch_all_regions()

    assert results[dead].empty
    assert sum(df.empty for df in results.values()) == 1
