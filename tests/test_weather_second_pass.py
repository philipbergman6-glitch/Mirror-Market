"""#577: a region lost to transient Open-Meteo throttling gets one more pass.

Since #637 a pass is one batched request (`_fetch_batch`), so the second
pass re-asks only the pins the first left empty, together.
"""

from __future__ import annotations

import pandas as pd

import fetchers.weather as weather


def _row() -> pd.DataFrame:
    return pd.DataFrame({"Date": ["2026-11-15"], "temp_max": [30.0]})


def _batch(answer):
    """A fake `_fetch_batch` that records each pass's regions."""
    passes: list[list[str]] = []

    def fake(regions, now):
        passes.append(list(regions))
        return {name: answer(name, len(passes)) for name in regions}

    return fake, passes


def test_empty_region_is_retried_once_after_the_pause(monkeypatch) -> None:
    sleeps: list[float] = []
    flaky = "South Africa Mpumalanga"
    fake, passes = _batch(
        lambda name, n: pd.DataFrame() if name == flaky and n == 1 else _row()
    )
    monkeypatch.setattr(weather, "_fetch_batch", fake)
    monkeypatch.setattr(weather.time, "sleep", sleeps.append)

    results = weather.fetch_all_regions()

    assert all(not df.empty for df in results.values())
    assert passes == [list(weather.GROWING_REGIONS), [flaky]]
    assert sleeps == [weather.WEATHER_SECOND_PASS_DELAY_S]


def test_no_pause_when_every_region_answered(monkeypatch) -> None:
    sleeps: list[float] = []
    fake, passes = _batch(lambda name, n: _row())
    monkeypatch.setattr(weather, "_fetch_batch", fake)
    monkeypatch.setattr(weather.time, "sleep", sleeps.append)

    weather.fetch_all_regions()

    assert sleeps == []
    assert len(passes) == 1


def test_region_still_empty_after_second_pass_stays_empty(monkeypatch) -> None:
    """Never patched: the miss travels on as an empty frame (usable_partial)."""
    dead = "Nigeria Kaduna"
    fake, _passes = _batch(lambda name, n: pd.DataFrame() if name == dead else _row())
    monkeypatch.setattr(weather, "_fetch_batch", fake)
    monkeypatch.setattr(weather.time, "sleep", lambda s: None)

    results = weather.fetch_all_regions()

    assert results[dead].empty
    assert sum(df.empty for df in results.values()) == 1
