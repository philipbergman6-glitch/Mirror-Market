"""Regression tests for #403 — CONAB vs USDA must compare one mapped crop year.

Before the fix, ``supply_analysis()`` took CONAB's ``crop_year.max()`` and
PSD's ``year.max()`` independently and subtracted. In October the local DB
holds CONAB ``2025/26`` and PSD Market_Year ``2026`` (= USDA's 2026/27
projection), so the published "gap" was two different crops, not a forecast
disagreement — invariant 2, absence of alignment became an assumption.

The mapping is registry data (``config.MARKETS["brazil"]["crop_estimates"]``):
PSD keys a crop to the first year of CONAB's split label (``2025/26`` → 2025),
verified on the 2026-10-07 bulk file (CONAB 180,463.5 kt vs PSD MY2025
180,500 kt, while MY2026 = 186,000 kt).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from analysis.briefing.sections import conab as conab_section
from analysis.crop_year import psd_year_for_crop_year, psd_year_label
from analysis.soy_analytics import supply_analysis
from config import MARKETS

CONAB_ROWS = [
    ("CONAB", "Soybeans", "2025/26", "Production", 180_000.0, "1000 MT", "2026-08-10"),
    ("CONAB", "Soybeans", "2025/26", "Production", 180_463.5, "1000 MT", "2026-09-11"),
    ("CONAB", "Soybeans", "2024/25", "Production", 171_500.0, "1000 MT", "2026-09-11"),
]


def _seed(db: Path, psd_rows: list[tuple]) -> None:
    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO brazil_estimates (source, commodity, crop_year, attribute,"
        " value, unit, report_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        CONAB_ROWS,
    )
    conn.executemany(
        "INSERT INTO psd (commodity, country, attribute, year, value, unit)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        psd_rows,
    )
    conn.execute(
        "INSERT INTO data_freshness (layer_name, last_success, status)"
        " VALUES ('psd', '2026-10-07 06:12:00', 'success')"
    )
    conn.commit()
    conn.close()


# --- the mapping itself ----------------------------------------------------

def test_brazil_descriptor_carries_the_conab_psd_mapping() -> None:
    desc = MARKETS["brazil"]["crop_estimates"]
    assert desc["agency"] == "CONAB"
    assert desc["table"] == "brazil_estimates"
    assert desc["psd_year_offset"] == 0


@pytest.mark.parametrize(
    ("label", "offset", "expected"),
    [("2025/26", 0, 2025), ("2024/25", 0, 2024), ("1999/00", 0, 1999), ("2025/26", -1, 2024)],
)
def test_psd_year_for_crop_year(label: str, offset: int, expected: int) -> None:
    assert psd_year_for_crop_year(label, offset) == expected


@pytest.mark.parametrize("label", ["2025", "2025/27", "25/26", "", "2025-26", "2025/2026"])
def test_psd_year_for_crop_year_hard_fails_on_an_unparseable_label(label: str) -> None:
    with pytest.raises(ValueError):
        psd_year_for_crop_year(label, 0)


def test_psd_year_label_renders_usda_split_year() -> None:
    assert psd_year_label(2025) == "2025/26"
    assert psd_year_label(1999) == "1999/00"


# --- supply_analysis ------------------------------------------------------

def test_gap_is_withheld_when_psd_has_not_published_the_mapped_year(patched_db: Path) -> None:
    # PSD carries only MY2024 (= 2024/25) and MY2026 (= 2026/27): the newest
    # PSD year is NOT the CONAB crop, and the mapped year is missing.
    _seed(patched_db, [
        ("Soybeans", "Brazil", "Production", 2024, 172_500.0, "1000 MT"),
        ("Soybeans", "Brazil", "Production", 2026, 186_000.0, "1000 MT"),
    ])

    out = supply_analysis()["conab_vs_usda"]

    assert out["conab_production"] == 180_463.5
    assert out["conab_crop_year"] == "2025/26"
    assert out["conab_report_date"] == "2026-09-11"
    assert out["usda_psd_year"] == 2025
    assert out["usda_psd_year_label"] == "2025/26"
    assert "gap" not in out
    assert "usda_production" not in out
    assert "2025/26" in out["gap_withheld_reason"]
    # The newest PSD vintage is named so the reader sees the lag, never a
    # 186,000 − 180,463 "gap" between two different crops.
    assert out["usda_psd_newest_year"] == 2026


def test_gap_is_computed_only_on_the_mapped_year(patched_db: Path) -> None:
    _seed(patched_db, [
        ("Soybeans", "Brazil", "Production", 2025, 180_500.0, "1000 MT"),
        ("Soybeans", "Brazil", "Production", 2026, 186_000.0, "1000 MT"),
    ])

    out = supply_analysis()["conab_vs_usda"]

    assert out["conab_crop_year"] == "2025/26"
    assert out["usda_psd_year"] == 2025
    assert out["usda_production"] == 180_500.0
    assert out["gap"] == pytest.approx(-36.5)
    assert out["usda_psd_fetched_at"] == "2026-10-07 06:12:00"
    assert "gap_withheld_reason" not in out


def test_conab_leg_is_the_latest_report_for_the_crop_year(patched_db: Path) -> None:
    _seed(patched_db, [])
    out = supply_analysis()["conab_vs_usda"]
    # Two CONAB releases for 2025/26 — the September one wins, not the
    # first row the DB happened to return.
    assert out["conab_production"] == 180_463.5
    assert out["conab_report_date"] == "2026-09-11"
    assert out["gap_withheld_reason"]


# --- the page -------------------------------------------------------------

def test_dashboard_renders_both_vintages_and_the_withheld_reason() -> None:
    from scripts.generate_html import _build_supply

    data = {"conab_vs_usda": {
        "conab_production": 180_463.5, "crop_year": "2025/26",
        "conab_crop_year": "2025/26", "conab_report_date": "2026-09-11",
        "usda_psd_year": 2025, "usda_psd_year_label": "2025/26",
        "usda_psd_newest_year": 2026,
        "gap_withheld_reason": "USDA PSD has not published 2025/26 (PSD MY2025) yet",
    }}
    html = _build_supply(data)["conab_html"]
    assert "180,464" in html
    assert "2025/26" in html
    assert "2026-09-11" in html
    assert "has not published" in html
    assert "Gap" not in html

    data["conab_vs_usda"].pop("gap_withheld_reason")
    data["conab_vs_usda"].update({"usda_production": 180_500.0, "gap": -36.5,
                                  "usda_psd_fetched_at": "2026-10-07 06:12:00"})
    html = _build_supply(data)["conab_html"]
    assert "180,500" in html
    assert "-36" in html
    assert "PSD MY2025" in html


# --- the briefing ---------------------------------------------------------

def test_briefing_section_withholds_the_gap_on_an_unmapped_year(patched_db: Path) -> None:
    _seed(patched_db, [("Soybeans", "Brazil", "Production", 2026, 186_000.0, "1000 MT")])
    text = conab_section.format()
    assert "186,000" not in text
    assert "gap" not in text.lower() or "no gap" in text.lower()
    assert "not published 2025/26" in text


def test_briefing_section_subtracts_on_the_mapped_year(patched_db: Path) -> None:
    _seed(patched_db, [("Soybeans", "Brazil", "Production", 2025, 180_500.0, "1000 MT")])
    text = conab_section.format()
    assert "vs USDA 180,500" in text
    assert "gap: -36" in text


# --- calendar-year labels (deploy 2026-10-07 20:49 UTC failed on this) -----

def test_is_split_crop_year_distinguishes_conab_conventions() -> None:
    from analysis.crop_year import is_split_crop_year

    assert is_split_crop_year("2025/26")
    assert not is_split_crop_year("2025")  # CONAB wheat
    assert not is_split_crop_year(None)


def test_briefing_section_survives_a_calendar_year_label_and_says_why(patched_db: Path) -> None:
    """CONAB labels wheat by calendar year. One such row killed the whole briefing
    (``crop year label '2025' is not of the form YYYY/YY``) and the promotion
    contract refused the candidate. The PSD leg is skipped *for that commodity*
    with a reason; soybeans still compare; nothing raises."""
    _seed(patched_db, [("Soybeans", "Brazil", "Production", 2025, 180_500.0, "1000 MT"),
                       ("Wheat", "Brazil", "Production", 2025, 8_000.0, "1000 MT")])
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO brazil_estimates (source, commodity, crop_year, attribute,"
        " value, unit, report_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("CONAB", "Wheat", "2025", "Production", 7_900.0, "1000 MT", "2026-09-11"),
    )
    conn.commit()
    conn.close()

    text = conab_section.format()
    assert "vs USDA 180,500" in text                       # soybeans still compared
    assert "no USDA comparison" in text and "calendar" in text  # wheat explains itself
    assert "8,000" not in text                             # PSD wheat never subtracted
