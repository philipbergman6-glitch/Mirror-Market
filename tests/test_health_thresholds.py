"""Per-layer staleness thresholds in analysis/health.py.

Health checks run per commodity on the stored table, one level below the
per-layer freshness block. Both must share the same cadence policy —
otherwise weekly COT and monthly World Bank rows are flagged STALE on
every single run and the reader learns to ignore the health report.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline import schema

_TABLES = [
    schema._CREATE_PRICES,
    schema._CREATE_COT,
    schema._CREATE_WORLDBANK,
]


def _days_ago(n: int) -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=n)).isoformat()


@pytest.fixture
def health_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp DB wired into analysis.health, with prices/cot/worldbank tables."""
    db_path = tmp_path / "health.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in _TABLES:
        conn.execute(ddl)
    conn.commit()
    conn.close()

    monkeypatch.setattr("analysis.health.get_connection",
                        lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr("analysis.health.DB_PATH", str(db_path))
    monkeypatch.setattr("analysis.health.is_cloud", lambda: False)
    return db_path


def _insert_price(db: Path, commodity: str, date: str) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO prices (commodity, Date, Open, High, Low, Close, Volume) "
        "VALUES (?, ?, 1, 1, 1, 1, 1)",
        (commodity, date),
    )
    conn.commit()
    conn.close()


def _insert_cot(db: Path, commodity: str, date: str) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO cot (commodity, Date, commercial_long, commercial_short, "
        "commercial_net, noncommercial_long, noncommercial_short, "
        "noncommercial_net, total_open_interest) "
        "VALUES (?, ?, 0, 0, 0, 0, 0, 0, 0)",
        (commodity, date),
    )
    conn.commit()
    conn.close()


def _insert_worldbank(db: Path, commodity: str, date: str) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT INTO worldbank_prices (commodity, Date, price, unit) "
        "VALUES (?, ?, 1.0, 'USD/mt')",
        (commodity, date),
    )
    conn.commit()
    conn.close()


def _stale_keys(issues: list[dict], table: str) -> set[str]:
    return {
        i["commodity"] for i in issues
        if i["table"] == table and i["message"].startswith("STALE")
    }


def test_weekly_cot_at_eight_days_is_not_stale(health_db: Path) -> None:
    """COT publishes weekly — an 8-day-old row is on cadence, not an outage."""
    from analysis.health import _check_cot

    _insert_cot(health_db, "Soybeans", _days_ago(8))
    assert _stale_keys(_check_cot(), "cot") == set()


def test_weekly_cot_past_its_own_threshold_is_stale(health_db: Path) -> None:
    """Two missed weekly releases is a real outage."""
    from analysis.health import _check_cot

    _insert_cot(health_db, "Soybeans", _days_ago(20))
    assert _stale_keys(_check_cot(), "cot") == {"Soybeans"}


def test_daily_prices_keep_the_tight_threshold(health_db: Path) -> None:
    """Daily tables must not inherit the slower per-layer allowances."""
    from analysis.health import _check_prices

    _insert_price(health_db, "Soybeans", _days_ago(8))
    _insert_price(health_db, "Corn", _days_ago(1))
    assert _stale_keys(_check_prices(), "prices") == {"Soybeans"}


def test_monthly_worldbank_is_not_stale_in_commodity_status(health_db: Path) -> None:
    """World Bank publishes monthly — a 30-day-old row is fresh enough."""
    from analysis.health import _build_commodity_status

    _insert_worldbank(health_db, "Palm Oil", _days_ago(30))
    entry = next(e for e in _build_commodity_status()
                 if e["table"] == "worldbank_prices")
    assert entry["status"] != "stale"


def test_monthly_worldbank_beyond_threshold_is_stale(health_db: Path) -> None:
    from analysis.health import _build_commodity_status

    _insert_worldbank(health_db, "Palm Oil", _days_ago(60))
    entry = next(e for e in _build_commodity_status()
                 if e["table"] == "worldbank_prices")
    assert entry["status"] == "stale"


def test_daily_prices_stay_stale_in_commodity_status(health_db: Path) -> None:
    from analysis.health import _build_commodity_status

    _insert_price(health_db, "Soybeans", _days_ago(8))
    entry = next(e for e in _build_commodity_status() if e["table"] == "prices")
    assert entry["status"] == "stale"


def test_every_mapped_layer_exists_in_the_freshness_policy() -> None:
    """The table→layer map must resolve against the per-layer policy."""
    from config import FRESHNESS_WARNING_DAYS_BY_LAYER, HEALTH_TABLE_LAYERS

    unknown = set(HEALTH_TABLE_LAYERS.values()) - set(FRESHNESS_WARNING_DAYS_BY_LAYER)
    assert unknown == set()
