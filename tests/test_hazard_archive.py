"""What the site published, archived (S2 #374 §3.2 / §7.4, slice 4 #390).

``hazard_flags`` holds one row per place in ``flag`` or ``watch`` per run —
the registry coordinates frozen in, so a later registry edit cannot rewrite
history — and rides to ``data/history/`` on the same export as the storms.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

from app.block_builders import SiteContext, hazard_flag_rows
from app.markets import load_markets
from pipeline import schema
from pipeline.history import HISTORY_TABLES
from tests.cyclone_fixtures import seed_francine, seed_quiet

TODAY_2026 = datetime(2026, 10, 7, 20, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(tmp_path: Path):
    conn = sqlite3.connect(str(tmp_path / "flags.db"))
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.commit()
    yield conn
    conn.close()


def test_hazard_flags_is_a_history_table_keyed_as_the_spec_says(db):
    assert HISTORY_TABLES["hazard_flags"] == ("run_date", "place_id", "source", "storm_id")
    columns = {row[1] for row in db.execute("PRAGMA table_info(hazard_flags)")}
    assert {"run_date", "place_id", "source", "storm_id", "state", "severity", "band_kt",
            "place_lat", "place_lon", "legs", "first_arrival_at", "closest_km"} <= columns


def test_francine_archives_one_row_for_new_orleans_with_every_leg_that_carried_it(db):
    now = seed_francine(db, "010")
    ctx = SiteContext(conn=db, today=now.date(), now=now)
    rows = hazard_flag_rows(ctx, load_markets(today=now.date()), run_date=now.date())
    assert len(rows) == 1
    row = rows[0]
    assert row["run_date"] == "2024-09-11"
    assert row["place_id"] == "P-NOLA" and row["source"] == "NHC" and row["storm_id"] == "al062024"
    assert row["state"] == "flag" and row["severity"] == "warning" and row["band_kt"] == 34
    assert row["storm_name"] == "Francine" and row["advisory"] == "10"
    assert row["first_arrival_tau_h"] == 24 and row["first_arrival_at"] == "2024-09-12T00:00:00Z"
    assert row["closest_km"] == 107 and row["closest_tau_h"] == 27
    assert (row["place_lat"], row["place_lon"]) == (29.9369, -90.0619)
    assert row["legs"] == "cbot:board,us_gulf:cif,dalian:board"


def test_a_quiet_day_archives_nothing(db):
    seed_quiet(db, TODAY_2026)
    ctx = SiteContext(conn=db, today=TODAY_2026.date(), now=TODAY_2026)
    assert hazard_flag_rows(ctx, load_markets(today=TODAY_2026.date()), run_date=TODAY_2026.date()) == []


def test_save_hazard_flags_upserts_and_refuses_a_state_that_is_not_published(db, monkeypatch, tmp_path):
    from pipeline import store

    db_path = tmp_path / "flags.db"
    monkeypatch.setattr("pipeline.store.get_connection", lambda: sqlite3.connect(str(db_path)))
    now = seed_francine(db, "010")
    ctx = SiteContext(conn=db, today=now.date(), now=now)
    rows = hazard_flag_rows(ctx, load_markets(today=now.date()), run_date=now.date())
    store.save_hazard_flags(rows)
    store.save_hazard_flags(rows)  # re-run safe
    assert db.execute("SELECT COUNT(*) FROM hazard_flags").fetchone()[0] == 1
    with pytest.raises(ValueError, match="clear"):
        store.save_hazard_flags([{**rows[0], "state": "clear"}])
    store.save_hazard_flags([])  # nothing to write is not an error
