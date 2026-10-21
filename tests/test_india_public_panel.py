"""SEA (Layer 32) and SOPA (Layer 33) on the headline's India panel.

Opened 2026-10-21 by owner decision (A18 stance, licence risk accepted). Each
renders only while its publish gate is open, carries its attribution, and is
withheld with a stated reason rather than dropped (invariant 2).
"""

from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pytest

import config
from analysis import soy_analytics
from analysis.india_parity import india_parity
from app.sections import _country_notes, _india_groups
from pipeline.schema import ALL_SCHEMAS

_AS_ON = "2026-10-01"
_SHEET = {
    "Soybean Indore": (4_700.0, "INR/MT"),
    "Soybean Meal FAS Kandla": (520.0, "USD/MT"),
    "Soybean Oil CIF Mumbai": (1_300.0, "USD/MT"),
    "Soybean Oil SE Indore": (134_000.0, "INR/MT"),
}


def _db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    for series, (value, unit) in _SHEET.items():
        conn.execute(
            "INSERT INTO sea_india_rates (Date, series, value, unit) VALUES (?, ?, ?, ?)",
            (_AS_ON, series, value, unit),
        )
    conn.commit()
    return conn


def test_the_sheet_is_every_series_quoted_in_config_order(tmp_path: Path) -> None:
    # Meal ex-Indore, FOR Kandla and the other oils are NQ here: absent, never zero.
    data = india_parity(_db(tmp_path / "t.db"), today=date(2026, 10, 6))["data"]
    assert data["sheet"] == [
        {"series": s, "value": v, "unit": u}
        for s, (v, u) in sorted(_SHEET.items(), key=lambda kv: list(config.SEA_SERIES).index(kv[0]))
    ]


def test_a_sheet_series_in_an_unstated_unit_raises(tmp_path: Path) -> None:
    conn = _db(tmp_path / "t.db")
    conn.execute("UPDATE sea_india_rates SET unit = 'INR/QTL' WHERE series = 'Soybean Indore'")
    with pytest.raises(ValueError, match="Soybean Indore stored in 'INR/QTL'"):
        india_parity(conn, today=date(2026, 10, 6))


_SEA_DATA = {
    "newest": _AS_ON,
    "sheet": [{"series": s, "value": v, "unit": u} for s, (v, u) in _SHEET.items()],
    "weeks": [{"as_on": _AS_ON,
               "meal": {"premium_usd_mt": 130.5, "board_contract": "ZMZ26", "note": None},
               "oil": {"gap_usd_mt": -42.0, "duty": 0.11, "note": None}}],
    "attribution": config.SEA_ATTRIBUTION,
}
_CROP_DATA = {
    "crop_year": 2025, "read_on": "2026-10-07",
    "headline": {"production_lakh_t": 110.267, "production_mt": 11_026_700.0},
    "yoy": {"production_lakh_t_pct": -14.4},
    "state_shares": [{"state": "Maharashtra", "share_pct": 47.4},
                     {"state": "Madhya Pradesh", "share_pct": 39.2},
                     {"state": "Others", "share_pct": 13.4}],
    "attribution": config.SOPA_ATTRIBUTION,
}


def test_the_india_panel_renders_the_sheet_the_parity_and_the_crop() -> None:
    sheet, parity, crop = _india_groups({"india_sea": _SEA_DATA, "india_crop": _CROP_DATA})

    assert sheet["title"] == f"SEA weekly rate sheet (as on {_AS_ON})"
    assert sheet["note"] == config.SEA_ATTRIBUTION
    cards = {c["label"]: c for c in sheet["cards"]}
    assert cards["Soybean Indore"]["prefix"] == "₹" and cards["Soybean Indore"]["caption"] == "INR/MT"
    assert cards["Soybean Meal FAS Kandla"]["prefix"] == "$"

    p = {c["label"]: c for c in parity["cards"]}
    assert p["Meal FAS Kandla over CBOT"]["value"] == 130.5
    assert "ZMZ26" in p["Meal FAS Kandla over CBOT"]["caption"]
    assert "domestic cheaper" in p["Soy oil Indore vs landed import"]["caption"]
    assert "11.0% duty" in p["Soy oil Indore vs landed import"]["caption"]

    assert crop["note"] == config.SOPA_ATTRIBUTION
    c = {card["label"]: card for card in crop["cards"]}
    assert c["Production (kharif 2025)"]["value"] == 11_026_700.0
    assert c["Production (kharif 2025)"]["delta"] == "-14.4% YoY"
    assert c["Maharashtra share"]["value"] == 47.4


def test_a_withheld_parity_leg_says_why_and_drops_its_card() -> None:
    sea = {**_SEA_DATA, "weeks": [{"as_on": _AS_ON,
                                   "meal": {"premium_usd_mt": None, "note": "no ZMZ26 close on 2026-10-01"},
                                   "oil": {"gap_usd_mt": -42.0, "duty": 0.11, "note": None}}]}
    _sheet, parity = _india_groups({"india_sea": sea})
    assert [c["label"] for c in parity["cards"]] == ["Soy oil Indore vs landed import"]
    assert parity["note"] == "no ZMZ26 close on 2026-10-01"


def test_withheld_reads_are_stated_on_the_page() -> None:
    notes = _country_notes("India", {
        "india_domestic": {"soybean_mandi_inr": 1.0},
        "india_sea_withheld": "the newest SEA sheet is dated 2026-09-01, 50 days ago",
        "india_crop_withheld": "no SOPA estimate is stored",
    })
    assert notes == [
        "SEA weekly rate sheet: withheld — the newest SEA sheet is dated 2026-09-01, 50 days ago",
        "SOPA kharif crop estimate: withheld — no SOPA estimate is stored",
    ]


def test_the_entry_follows_the_publish_gates(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "t.db"
    _db(path).close()
    monkeypatch.setattr("pipeline.connection.get_connection", lambda: sqlite3.connect(str(path)))

    entry = soy_analytics._india_sea_and_crop(date(2026, 10, 6))
    assert entry["india_sea"]["newest"] == _AS_ON
    assert entry["india_crop_withheld"].startswith("no SOPA estimate is stored")

    monkeypatch.setattr(config, "SEA_PUBLISH", False)
    monkeypatch.setattr(config, "SOPA_PUBLISH", False)
    assert soy_analytics._india_sea_and_crop(date(2026, 10, 6)) == {}


def test_a_read_that_raises_is_withheld_with_the_error(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "t.db"
    conn = _db(path)
    conn.execute("UPDATE sea_india_rates SET unit = 'INR/QTL' WHERE series = 'Soybean Indore'")
    conn.commit()
    conn.close()
    monkeypatch.setattr("pipeline.connection.get_connection", lambda: sqlite3.connect(str(path)))

    entry = soy_analytics._india_sea_and_crop(date(2026, 10, 6))
    assert "india_sea" not in entry
    assert "INR/QTL" in entry["india_sea_withheld"]


def test_a_fully_withheld_parity_group_still_says_why() -> None:
    # Found rendering real data (2026-10-21): no board close and no FX print on
    # the sheet date left the group cardless, and _group dropped it with its
    # reasons — a silent gap. The reasons move to the country's notes.
    sea = {**_SEA_DATA, "weeks": [{"as_on": _AS_ON,
                                   "meal": {"premium_usd_mt": None, "note": "no ZMZ26 close on 2026-10-01"},
                                   "oil": {"gap_usd_mt": None, "note": "no INR/USD print on 2026-10-01"}}]}
    groups = _india_groups({"india_sea": sea})
    assert [g["title"] for g in groups] == [f"SEA weekly rate sheet (as on {_AS_ON})"]
    assert _country_notes("India", {"india_domestic": {"x": 1}, "india_sea": sea}) == [
        f"India vs the board (SEA, as on {_AS_ON}): withheld — "
        "no ZMZ26 close on 2026-10-01; no INR/USD print on 2026-10-01",
    ]
