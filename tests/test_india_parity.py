"""India meal export premium and oil import parity off the SEA sheet (#72)."""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from analysis.india_parity import india_parity
from pipeline.schema import ALL_SCHEMAS
from pipeline.units import usd_per_short_ton_to_usd_mt

_SHEETS = {
    # as_on: (meal FAS USD/MT, degum CIF USD/MT, SE soy oil Indore INR/MT)
    "2026-09-25": (515.0, 1305.0, 133_500.0),
    "2026-10-01": (520.0, 1300.0, 134_000.0),
}
_USD_PER_INR = 0.0112


@pytest.fixture
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    for day, (fas, cif, se_oil) in _SHEETS.items():
        for series, value, unit in (
            ("Soybean Meal FAS Kandla", fas, "USD/MT"),
            ("Soybean Oil CIF Mumbai", cif, "USD/MT"),
            ("Soybean Oil SE Indore", se_oil, "INR/MT"),
        ):
            conn.execute(
                "INSERT INTO sea_india_rates (Date, series, value, unit) VALUES (?, ?, ?, ?)",
                (day, series, value, unit),
            )
    return conn


def _meal_bar(conn: sqlite3.Connection, day: str, close: float, ticker: str = "ZMZ26.CBT") -> None:
    conn.execute(
        "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Close, Volume, "
        "fetched_date) VALUES ('Soybean Meal', ?, '2026-12-01', ?, ?, 1000, '2026-10-05')",
        (ticker, day, close),
    )


def _fx(conn: sqlite3.Connection, day: str, rate: float = _USD_PER_INR) -> None:
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES ('INR/USD', ?, ?)", (day, rate))


def _week(result: dict, as_on: str) -> dict:
    return next(w for w in result["data"]["weeks"] if w["as_on"] == as_on)


def test_meal_premium_is_struck_on_the_sheets_own_cbot_session(db) -> None:
    """ZMV26 is in delivery on 1 Oct (first notice 30 Sep), so the board leg
    is December — the first month a cargo can still be priced against."""
    _meal_bar(db, "2026-10-01", 353.3)

    result = india_parity(db, today=date(2026, 10, 6))

    assert result["state"] == "ok"
    meal = _week(result, "2026-10-01")["meal"]
    board = usd_per_short_ton_to_usd_mt(353.3)
    assert meal["fas_usd_mt"] == 520.0
    assert meal["board_contract"] == "ZMZ26"
    assert meal["board_usd_mt"] == pytest.approx(board)
    assert meal["premium_usd_mt"] == pytest.approx(520.0 - board)


def test_no_cbot_session_on_the_sheet_date_withholds_the_premium(db) -> None:
    """Invariant 8: a premium over the previous session's close would be the
    intervening board move reported as an Indian premium."""
    _meal_bar(db, "2026-09-30", 353.3)

    meal = _week(india_parity(db, today=date(2026, 10, 6)), "2026-10-01")["meal"]

    assert meal["fas_usd_mt"] == 520.0
    assert meal["board_usd_mt"] is None and meal["premium_usd_mt"] is None
    assert "2026-10-01" in meal["note"]


def test_oil_parity_lands_cif_with_duty_against_domestic_oil(db) -> None:
    """Landed = CIF × (1 + duty in force that week); domestic SE soy oil
    Indore converted at the sheet date's own INR/USD print."""
    _fx(db, "2026-10-01")

    oil = _week(india_parity(db, today=date(2026, 10, 6)), "2026-10-01")["oil"]

    assert oil["duty"] == 0.11
    assert oil["landed_usd_mt"] == pytest.approx(1300.0 * 1.11)
    assert oil["domestic_usd_mt"] == pytest.approx(134_000.0 * _USD_PER_INR)
    assert oil["gap_usd_mt"] == pytest.approx(134_000.0 * _USD_PER_INR - 1300.0 * 1.11)
    assert oil["above_parity"] is True


def test_the_duty_schedule_switches_on_its_effective_date(db) -> None:
    """The 24 Sep 2026 cut (16.5% → 11%) is what flips the window here: near
    the same CIF and domestic oil, two weeks apart, read opposite ways."""
    db.execute("UPDATE sea_india_rates SET Date = '2026-09-18' WHERE Date = '2026-09-25'")
    for day in ("2026-09-18", "2026-10-01"):
        _fx(db, day)

    result = india_parity(db, today=date(2026, 10, 6))

    before, after = _week(result, "2026-09-18")["oil"], _week(result, "2026-10-01")["oil"]
    assert before["duty"] == 0.165
    assert before["landed_usd_mt"] == pytest.approx(1305.0 * 1.165)
    assert before["above_parity"] is False
    assert after["above_parity"] is True
    assert after["flipped"] is True and before["flipped"] is False


def test_no_fx_print_on_the_sheet_date_withholds_the_domestic_leg(db) -> None:
    """Invariant 7: a home-currency leg converts at its own date's rate or
    renders blank — never yesterday's."""
    _fx(db, "2026-09-30")

    oil = _week(india_parity(db, today=date(2026, 10, 6)), "2026-10-01")["oil"]

    assert oil["landed_usd_mt"] == pytest.approx(1300.0 * 1.11)
    assert oil["domestic_usd_mt"] is None and oil["gap_usd_mt"] is None
    assert oil["above_parity"] is None
    assert "INR/USD" in oil["note"]


def test_a_week_before_the_duty_schedule_is_withheld_not_guessed(db) -> None:
    db.execute("UPDATE sea_india_rates SET Date = '2025-05-02' WHERE Date = '2026-09-25'")
    _fx(db, "2025-05-02")

    result = india_parity(db, today=date(2025, 5, 6))

    oil = _week(result, "2025-05-02")["oil"]
    assert oil["duty"] is None and oil["landed_usd_mt"] is None and oil["gap_usd_mt"] is None
    assert "duty" in oil["note"]


def test_an_empty_table_names_the_layer() -> None:
    conn = sqlite3.connect(":memory:")
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)

    result = india_parity(conn, today=date(2026, 10, 6))

    assert result["state"] == "empty"
    assert "Layer 32" in result["reason"]


def test_a_stale_sheet_is_withheld_as_a_stopped_feed(db) -> None:
    result = india_parity(db, today=date(2026, 11, 1))

    assert result["state"] == "empty"
    assert "2026-10-01" in result["reason"] and "budget" in result["reason"]
