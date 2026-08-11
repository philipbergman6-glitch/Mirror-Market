"""Health checks must judge freshness on observed rows only.

The weather layer writes a forward forecast horizon (~7 days) alongside
observed history. If `analysis/health.py` takes MAX(Date) over the whole
table, a dead fetcher keeps looking fresh for as long as its last run's
forecast reaches into the future — the outage surfaces a week late, which
is exactly when it matters least. Mirrors the observed-only rule in
`analysis/briefing/sections/weather.py::observed_only`.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline import schema


def _days_from_now(n: int) -> str:
    return (datetime.now(timezone.utc).date() + timedelta(days=n)).isoformat()


@pytest.fixture
def weather_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    db_path = tmp_path / "health.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(schema._CREATE_WEATHER)
    conn.execute(schema._CREATE_PRICES)
    conn.commit()
    conn.close()

    monkeypatch.setattr("analysis.health.get_connection",
                        lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr("analysis.health.DB_PATH", str(db_path))
    monkeypatch.setattr("analysis.health.is_cloud", lambda: False)
    return db_path


def _insert_weather(db: Path, region: str, date: str, is_forecast: int | None) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO weather (region, Date, temp_max, temp_min, precipitation, "
        "is_forecast) VALUES (?, ?, 20, 10, 0, ?)",
        (region, date, is_forecast),
    )
    conn.commit()
    conn.close()


def _insert_price(db: Path, commodity: str, date: str) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO prices (commodity, Date, Open, High, Low, Close, Volume) "
        "VALUES (?, ?, 1, 1, 1, 1, 1)",
        (commodity, date),
    )
    conn.commit()
    conn.close()


def _stale_keys(issues: list[dict], table: str) -> set[str]:
    return {
        i["commodity"] for i in issues
        if i["table"] == table and i["message"].startswith("STALE")
    }


def _region() -> str:
    from config import GROWING_REGIONS
    return next(iter(GROWING_REGIONS))


def test_forecast_rows_do_not_mask_a_dead_weather_fetcher(weather_db: Path) -> None:
    """Last observation 8 days old, forecasts out to +6 → still STALE."""
    from analysis.health import _check_weather

    region = _region()
    _insert_weather(weather_db, region, _days_from_now(-8), 0)
    for ahead in range(-7, 7):
        _insert_weather(weather_db, region, _days_from_now(ahead), 1)

    assert _stale_keys(_check_weather(), "weather") == {region}


def test_fresh_observations_stay_fresh(weather_db: Path) -> None:
    """A live fetcher (observation from yesterday) is not flagged."""
    from analysis.health import _check_weather

    region = _region()
    _insert_weather(weather_db, region, _days_from_now(-1), 0)
    for ahead in range(0, 7):
        _insert_weather(weather_db, region, _days_from_now(ahead), 1)

    assert _stale_keys(_check_weather(), "weather") == set()


def test_legacy_null_flag_rows_count_as_observed(weather_db: Path) -> None:
    """Rows written before is_forecast existed are observations."""
    from analysis.health import _check_weather

    region = _region()
    _insert_weather(weather_db, region, _days_from_now(-1), None)

    assert _stale_keys(_check_weather(), "weather") == set()


def test_future_dated_rows_never_count_as_fresh(weather_db: Path) -> None:
    """Even an unflagged future row is a forecast — it can't prove liveness."""
    from analysis.health import _check_weather

    region = _region()
    _insert_weather(weather_db, region, _days_from_now(-8), None)
    _insert_weather(weather_db, region, _days_from_now(5), None)

    assert _stale_keys(_check_weather(), "weather") == {region}


def test_commodity_status_reports_observed_age(weather_db: Path) -> None:
    """The dashboard status mirrors the issue list, not the forecast horizon."""
    from analysis.health import _build_commodity_status

    region = _region()
    _insert_weather(weather_db, region, _days_from_now(-8), 0)
    for ahead in range(0, 7):
        _insert_weather(weather_db, region, _days_from_now(ahead), 1)

    entry = next(e for e in _build_commodity_status() if e["table"] == "weather")
    assert entry["age_days"] == 8
    assert entry["status"] == "stale"


def test_region_with_only_forecast_rows_is_missing(weather_db: Path) -> None:
    """No observations at all is a critical gap, not a fresh region."""
    from analysis.health import _check_weather

    region = _region()
    for ahead in range(0, 7):
        _insert_weather(weather_db, region, _days_from_now(ahead), 1)

    issues = _check_weather()
    assert any(i["commodity"] == region and i["severity"] == "critical"
               for i in issues)


def test_tables_without_the_flag_column_still_check(weather_db: Path) -> None:
    """prices has no is_forecast column — the filter must not break it."""
    from analysis.health import _check_prices

    _insert_price(weather_db, "Soybeans", _days_from_now(-8))
    assert "Soybeans" in _stale_keys(_check_prices(), "prices")
