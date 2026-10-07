"""India's soybean crop off SOPA's state-wise estimate (Layer 33)."""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

import config
from analysis.india_crop import india_crop
from pipeline.schema import ALL_SCHEMAS

# (crop_year, fetched_date) → {state: (area lakh ha, yield kg/ha, production lakh t)}.
# Kharif 2025 was read twice: the first estimate and the in-place revision.
_READINGS = {
    (2024, "2025-10-20"): {
        "Madhya Pradesh": (52.009, 1082, 56.272),
        "Maharashtra": (47.0, 1105, 51.941),
        "Others": (19.31, 1065, 20.608),
        config.SOPA_ALL_INDIA: (118.319, 1089, 128.821),
    },
    (2025, "2025-10-20"): {
        "Maharashtra": (44.683, 1120, 50.045),
        "Madhya Pradesh": (48.640, 850, 41.344),
        "Others": (18.817, 742, 13.971),
        config.SOPA_ALL_INDIA: (112.140, 940, 105.360),
    },
    (2025, "2026-10-07"): {
        "Maharashtra": (44.683, 1169, 52.229),
        "Madhya Pradesh": (48.640, 889, 43.247),
        "Others": (18.817, 786, 14.791),
        config.SOPA_ALL_INDIA: (112.140, 983, 110.267),
    },
}


@pytest.fixture
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    for (year, fetched), rows in _READINGS.items():
        for state, (area, yld, prod) in rows.items():
            conn.execute(
                "INSERT INTO sopa_crop_estimates (crop_year, state, fetched_date, "
                "area_lakh_ha, yield_kg_ha, production_lakh_t) VALUES (?, ?, ?, ?, ?, ?)",
                (year, state, fetched, area, yld, prod),
            )
    return conn


def test_the_read_is_the_newest_reading_of_the_newest_kharif(db) -> None:
    envelope = india_crop(db, today=date(2026, 10, 8))

    assert envelope["state"] == "ok"
    data = envelope["data"]
    assert (data["crop_year"], data["read_on"], data["age_days"]) == (2025, "2026-10-07", 1)
    assert data["headline"] == {"production_lakh_t": 110.267, "production_mt": 11_026_700.0}
    assert data["attribution"] == config.SOPA_ATTRIBUTION
    assert data["published"] is False


def test_year_on_year_is_against_the_prior_kharif_as_currently_published(db) -> None:
    yoy = india_crop(db, today=date(2026, 10, 8))["data"]["yoy"]

    assert yoy["prior_year"] == 2024
    assert yoy["production_lakh_t_pct"] == pytest.approx(-14.4, abs=0.05)
    assert yoy["area_lakh_ha_pct"] == pytest.approx(-5.2, abs=0.05)
    assert yoy["yield_kg_ha_pct"] == pytest.approx(-9.7, abs=0.05)
    assert "revised" in yoy["note"]


def test_state_shares_are_of_the_all_india_total_largest_first(db) -> None:
    shares = india_crop(db, today=date(2026, 10, 8))["data"]["state_shares"]

    assert [entry["state"] for entry in shares] == ["Maharashtra", "Madhya Pradesh", "Others"]
    assert shares[0]["share_pct"] == pytest.approx(47.4, abs=0.05)
    assert sum(entry["share_pct"] for entry in shares) == pytest.approx(100.0, abs=0.2)


def test_the_kept_readings_of_the_kharif_form_its_revision_path(db) -> None:
    revisions = india_crop(db, today=date(2026, 10, 8))["data"]["revisions"]

    assert revisions == [
        {"read_on": "2025-10-20", "production_lakh_t": 105.36},
        {"read_on": "2026-10-07", "production_lakh_t": 110.267},
    ]


def test_an_unchanged_re_read_does_not_add_a_revision(db) -> None:
    for state, (area, yld, prod) in _READINGS[(2025, "2026-10-07")].items():
        db.execute(
            "INSERT INTO sopa_crop_estimates VALUES (2025, ?, '2026-10-08', ?, ?, ?)",
            (state, area, yld, prod),
        )

    revisions = india_crop(db, today=date(2026, 10, 8))["data"]["revisions"]

    assert [r["read_on"] for r in revisions] == ["2025-10-20", "2026-10-07"]


def test_year_on_year_is_withheld_without_the_prior_kharif(db) -> None:
    db.execute("DELETE FROM sopa_crop_estimates WHERE crop_year = 2024")

    data = india_crop(db, today=date(2026, 10, 8))["data"]

    assert data["yoy"] is None
    assert data["headline"]["production_lakh_t"] == 110.267


def test_a_stale_reading_is_withheld_as_a_stopped_feed(db) -> None:
    envelope = india_crop(db, today=date(2028, 1, 1))

    assert envelope["state"] == "empty"
    assert "stopped" in envelope["reason"]


def test_an_empty_table_and_a_missing_table_are_withheld_with_their_reasons() -> None:
    conn = sqlite3.connect(":memory:")
    assert "does not exist" in india_crop(conn, today=date(2026, 10, 8))["reason"]
    for ddl in ALL_SCHEMAS:
        conn.execute(ddl)
    assert "has not run" in india_crop(conn, today=date(2026, 10, 8))["reason"]
