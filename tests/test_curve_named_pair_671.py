"""#671 — the briefing and the workstation give the same Jan–Mar reading.

Seeded with the stored CBOT soybean strip on session 2026-11-25: Jan 1243.25 ·
Mar 1244.25 · May 1248.5 · Jul 1251.5 · Aug 1231.75 (cents/bu). Jan→Jul is
carry; only Jul→Aug is lower. The legacy analyser compared the first fetched
month against the last, printed "Soybeans curve in backwardation, front–next
−0.9%" and read it as "nearby supply tight relative to deferred".
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

SESSION = date(2026, 11, 25)
AS_OF = date(2026, 11, 26)
STRIP = (
    ("2027-01-01", "Jan 2027", "ZSF27.CBT", 1243.25),
    ("2027-03-01", "Mar 2027", "ZSH27.CBT", 1244.25),
    ("2027-05-01", "May 2027", "ZSK27.CBT", 1248.50),
    ("2027-07-01", "Jul 2027", "ZSN27.CBT", 1251.50),
    ("2027-08-01", "Aug 2027", "ZSQ27.CBT", 1231.75),
)
NEARBY = (
    "ZSF27 1243.25 / ZSH27 1244.25: Mar +1.00 cents/bu (+0.37 USD/MT) over Jan, "
    "session 2026-11-25"
)
INVERSION = "Jul 2027→Aug 2027"


@pytest.fixture
def strip_db(patched_db: Path) -> Path:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES ('Soybeans', ?, ?, ?, ?, ?, ?)",
        [(month, label, ticker, close, SESSION.isoformat(), AS_OF.isoformat())
         for month, label, ticker, close in STRIP],
    )
    conn.commit()
    conn.close()
    return patched_db


def _workstation_curve(db: Path):
    from analysis.futures.curve import analyse_curve
    from analysis.futures.providers import open_provider

    conn = sqlite3.connect(str(db))
    try:
        return analyse_curve(open_provider(conn).curve("Soybeans", as_of=AS_OF), as_of=AS_OF)
    finally:
        conn.close()


def test_briefing_driver_names_the_nearby_pair_in_carry_and_locates_the_inversion(strip_db):
    from analysis.briefing.sections.market_drivers import _curve_drivers

    drivers = _curve_drivers()
    assert len(drivers) == 1
    text = "\n".join(drivers[0].lines(1))
    assert NEARBY in text
    assert INVERSION in text
    assert "backwardation" not in text
    assert "tight" not in drivers[0].interpretation
    assert "front–next" not in text
    assert "gross of storage and financing" in text


def test_briefing_forward_curve_section_reads_the_same_pair(strip_db):
    from analysis.briefing.sections import forward_curve

    text = forward_curve.format()
    line = next(line for line in text.splitlines() if "Soybeans" in line)
    assert NEARBY in line
    assert "nearby carry" in line
    assert "inverted Jul 2027→Aug 2027 (ZSN27→ZSQ27)" in line
    assert "backwardation" not in line


def test_snapshot_archives_the_one_session_legs_the_reading_is_derived_from(strip_db):
    """Components only (the snapshot schema rule): the nearby pair and the
    located inversion are re-derived from the archived legs."""
    from analysis.briefing.snapshot import _forward_curve_block

    block = _forward_curve_block()["Soybeans"]
    assert block["observation_date"] == "2026-11-25"
    assert block["coherent"] is True
    assert [(leg["symbol"], leg["price"]) for leg in block["legs"]] == [
        ("ZSF27", 1243.25), ("ZSH27", 1244.25), ("ZSK27", 1248.50),
        ("ZSN27", 1251.50), ("ZSQ27", 1231.75),
    ]
    assert not {"structure", "spread", "spread_pct", "front_price"} & set(block)


def test_briefing_and_workstation_agree_on_jan_mar(strip_db):
    """The workstation's own path (provider over its connection → analyse_curve
    → curve_alerts) prints the same nearby sentence the briefing does."""
    from analysis.briefing.sections.market_drivers import _curve_drivers
    from analysis.futures.alerts import curve_alerts

    workstation = _workstation_curve(strip_db)
    assert workstation.nearby is not None
    assert workstation.nearby.sentence() == NEARBY
    alert = next(a for a in curve_alerts(workstation) if a.kind == "curve_inversion")
    assert NEARBY in alert.message and INVERSION in alert.message

    briefing = "\n".join(_curve_drivers()[0].lines(1))
    assert NEARBY in briefing


def test_the_workstation_page_card_prints_the_same_pair(strip_db):
    """The rendered workstation row, not just the analysis under it."""
    from app.workstation_page import _curve_section

    workstation = _workstation_curve(strip_db)
    row = _curve_section({"Soybeans": workstation}, {})["data"]["commodities"][0]
    assert row["nearby"] == NEARBY
    assert "inverted Jul 2027→Aug 2027 (ZSN27→ZSQ27)" in row["reading"]
    assert row["nearby_symbols"] == "ZSF27-ZSH27"
    assert row["nearby_usd_mt_per_month"] > 0          # carry, not the old −0.60 slope


def test_headline_forward_curve_block_reads_the_named_pair(strip_db):
    from analysis import soy_analytics
    from app.sections import forward_curves_section

    section = forward_curves_section(soy_analytics.forward_curve_analysis())
    assert section["state"] == "ok"
    leg = next(leg for leg in section["data"]["legs"] if leg["name"] == "Soybeans")
    assert leg["nearby"] == NEARBY
    assert "nearby carry" in leg["structure"].lower()
    assert "inverted Jul 2027→Aug 2027" in leg["reading"]
    assert leg["as_of"] == "2026-11-25"


def test_the_legacy_analyser_is_gone():
    import importlib.util

    assert importlib.util.find_spec("analysis.forward_curve") is None


FLAT_NEARBY = (
    ("2027-01-01", "Jan 2027", "ZSF27.CBT", 1244.00),
    ("2027-03-01", "Mar 2027", "ZSH27.CBT", 1244.00),
    ("2027-05-01", "May 2027", "ZSK27.CBT", 1248.50),
    ("2027-07-01", "Jul 2027", "ZSN27.CBT", 1240.00),
)


@pytest.fixture
def flat_nearby_db(patched_db: Path) -> Path:
    conn = sqlite3.connect(str(patched_db))
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES ('Soybeans', ?, ?, ?, ?, ?, ?)",
        [(month, label, ticker, close, SESSION.isoformat(), AS_OF.isoformat())
         for month, label, ticker, close in FLAT_NEARBY],
    )
    conn.commit()
    conn.close()
    return patched_db


def test_a_flat_nearby_pair_is_never_called_carry_by_the_briefing(flat_nearby_db):
    """Review of #674: "not inverted" is not "carry". Jan = Mar, then carry to
    May and an inversion at May→Jul: the workstation reads it as mixed, and the
    briefing driver must not say the nearby pair is in carry (#671 AC6)."""
    from analysis.briefing.sections.market_drivers import _curve_drivers
    from analysis.futures.curve import CurveStructure

    assert _workstation_curve(flat_nearby_db).structure is CurveStructure.MIXED
    text = "\n".join(line for d in _curve_drivers() for line in d.lines(1))
    assert "nearby pair in carry" not in text
    assert "nearby pair flat" in text
    assert "May 2027→Jul 2027" in text
