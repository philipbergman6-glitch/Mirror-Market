"""Seed a test database with cyclone rows the way the pipeline would store them.

Shared by the slice-4 render tests (ledger chip, block 06, origins, archive):
one seed path, so the ledger and the storms section are tested against the
same stored Francine advisory the assessment regression pins.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from pipeline import schema
from tests.test_hazards import _iso, _status, _status_row, _utc, francine

NHC_OK = {"cyclones_nhc": "success", "cyclones_jtwc": "success"}


def ensure_cyclone_tables(conn: sqlite3.Connection) -> None:
    for ddl in (
        schema._CREATE_CYCLONE_SOURCE_STATUS,
        schema._CREATE_CYCLONE_STORMS,
        schema._CREATE_CYCLONE_TRACK_POINTS,
        schema._CREATE_DATA_FRESHNESS,
    ):
        conn.execute(ddl)


def seed_cyclones(
    conn: sqlite3.Connection,
    status: pd.DataFrame,
    storms: pd.DataFrame,
    track: pd.DataFrame,
    layer_states: dict[str, str | None] | None = None,
) -> None:
    """Write the three frames plus one ``data_freshness`` row per layer."""
    ensure_cyclone_tables(conn)
    status.to_sql("cyclone_source_status", conn, if_exists="append", index=False)
    if not storms.empty:
        storms.to_sql("cyclone_storms", conn, if_exists="append", index=False)
    if not track.empty:
        track.to_sql("cyclone_track_points", conn, if_exists="append", index=False)
    for layer, state in (layer_states or NHC_OK).items():
        if state is None:
            continue
        conn.execute(
            "INSERT OR REPLACE INTO data_freshness (layer_name, status, last_success, last_attempt) "
            "VALUES (?,?,?,?)",
            (layer, state, "2026-10-07 19:05:00" if state == "success" else None, "2026-10-07 19:05:00"),
        )
    conn.commit()


def seed_francine(conn: sqlite3.Connection, advisory: str = "010") -> datetime:
    """Francine ``advisory`` from NHC, a quiet JTWC. Returns the ``now`` to render at."""
    status, storms, track = francine(advisory)
    checked = _utc(status.iloc[0]["checked_at"])
    jtwc = _status(_status_row("JTWC", checked, 0))
    seed_cyclones(conn, pd.concat([status, jtwc], ignore_index=True), storms, track)
    return checked + timedelta(hours=2)


def seed_quiet(conn: sqlite3.Connection, now: datetime) -> None:
    """Both sources answered an hour before ``now``: no storm anywhere."""
    checked = now - timedelta(hours=1)
    status = _status(_status_row("NHC", checked, 0), _status_row("JTWC", checked, 0))
    empty_storms = pd.DataFrame()
    seed_cyclones(conn, status, empty_storms, pd.DataFrame())


__all__ = ["NHC_OK", "ensure_cyclone_tables", "seed_cyclones", "seed_francine", "seed_quiet", "_iso"]
