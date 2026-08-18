"""The one-off cleanup for forward-curve legs left by a same-day re-run.

The interesting cases are all about what it must *not* delete. A curve leg is
irreplaceable — the fetcher serves the current session only — so an
over-eager prune costs history permanently, and the legacy pre-column rows look
superficially like the defect they are not.
"""

from __future__ import annotations

import sqlite3

import pytest

from pipeline import schema
from scripts.prune_curve_snapshots import find_leftovers, main, prune

INSERT = (
    "INSERT INTO forward_curve "
    "(commodity, contract_month, label, ticker, close, observation_date, fetched_date) "
    "VALUES (?,?,?,?,?,?,?)"
)


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        connection.execute(ddl)
    yield connection
    connection.close()


def rows(conn):
    return conn.execute(
        "SELECT ticker, observation_date FROM forward_curve ORDER BY ticker"
    ).fetchall()


def test_a_clean_single_session_snapshot_has_nothing_to_prune(conn):
    conn.executemany(INSERT, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75, "2026-08-18", "2026-08-18"),
        ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.00, "2026-08-18", "2026-08-18"),
    ])
    conn.commit()
    assert find_leftovers(conn) == []


def test_a_leg_from_an_earlier_run_the_same_day_is_a_leftover(conn):
    """The Cotton 2026-08-12 case in the committed history: two sessions, one
    fetched_date. The 11th's legs are what the 12th's run failed to replace."""
    conn.executemany(INSERT, [
        ("Cotton", "2026-10-01", "Oct 2026", "CTV26.NYB", 68.1, "2026-08-12", "2026-08-12"),
        ("Cotton", "2026-12-01", "Dec 2026", "CTZ26.NYB", 69.0, "2026-08-12", "2026-08-12"),
        ("Cotton", "2027-03-01", "Mar 2027", "CTH27.NYB", 70.2, "2026-08-11", "2026-08-12"),
    ])
    conn.commit()
    leftovers = find_leftovers(conn)
    assert [item.ticker for item in leftovers] == ["CTH27.NYB"]
    assert leftovers[0].kept_observation_date == "2026-08-12"

    assert prune(conn, leftovers) == 1
    assert [ticker for ticker, _ in rows(conn)] == ["CTV26.NYB", "CTZ26.NYB"]


def test_an_undated_leg_beside_dated_ones_is_a_leftover(conn):
    """The 2026-08-11 transition day: six legs stamped that session plus one
    NULL carried from a run that predated the observation_date column."""
    conn.executemany(INSERT, [
        ("Soybeans", "2026-09-01", "Sep 2026", "ZSU26.CBT", 1150.0, "2026-08-11", "2026-08-11"),
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.0, "2026-08-11", "2026-08-11"),
        ("Soybeans", "2027-07-01", "Jul 2027", "ZSN27.CBT", 1200.0, None, "2026-08-11"),
    ])
    conn.commit()
    leftovers = find_leftovers(conn)
    assert [item.ticker for item in leftovers] == ["ZSN27.CBT"]
    assert leftovers[0].observation_date is None
    assert "observed NULL" in leftovers[0].describe()


def test_a_wholly_undated_snapshot_is_left_completely_alone(conn):
    """The case that must not be touched.

    Every leg before 2026-08-11 predates the observation_date column, so a
    whole group of NULLs is a schema change, not a duplicate run. Deleting it
    would destroy real history — and the fetcher serves only the current
    session, so it could never be re-fetched.
    """
    conn.executemany(INSERT, [
        ("Wheat", "2026-09-01", "Sep 2026", "ZWU26.CBT", 520.0, None, "2026-08-05"),
        ("Wheat", "2026-12-01", "Dec 2026", "ZWZ26.CBT", 535.0, None, "2026-08-05"),
        ("Wheat", "2027-03-01", "Mar 2027", "ZWH27.CBT", 548.0, None, "2026-08-05"),
    ])
    conn.commit()
    assert find_leftovers(conn) == []
    prune(conn, find_leftovers(conn))
    assert len(rows(conn)) == 3


def test_groups_are_separated_by_commodity_and_by_fetched_date(conn):
    """A newer session for one commodity must not condemn another's legs."""
    conn.executemany(INSERT, [
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.0, "2026-08-18", "2026-08-18"),
        ("Corn", "2026-12-01", "Dec 2026", "ZCZ26.CBT", 430.0, "2026-08-17", "2026-08-17"),
        ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1160.0, "2026-08-17", "2026-08-17"),
    ])
    conn.commit()
    assert find_leftovers(conn) == []


def test_the_script_defaults_to_reporting_and_deletes_only_with_apply(conn, tmp_path, capsys):
    path = tmp_path / "curve.db"
    disk = sqlite3.connect(path)
    for ddl in schema.ALL_SCHEMAS:
        disk.execute(ddl)
    disk.executemany(INSERT, [
        ("Cotton", "2026-10-01", "Oct 2026", "CTV26.NYB", 68.1, "2026-08-12", "2026-08-12"),
        ("Cotton", "2027-03-01", "Mar 2027", "CTH27.NYB", 70.2, "2026-08-11", "2026-08-12"),
    ])
    disk.commit()
    disk.close()

    assert main(["--database", str(path)]) == 0
    survived = sqlite3.connect(path)
    assert len(survived.execute("SELECT 1 FROM forward_curve").fetchall()) == 2
    survived.close()

    assert main(["--database", str(path), "--apply"]) == 0
    after = sqlite3.connect(path)
    assert [r[0] for r in after.execute("SELECT ticker FROM forward_curve")] == ["CTV26.NYB"]
    after.close()
