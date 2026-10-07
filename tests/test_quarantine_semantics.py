"""Quarantine / rejection semantics decided in A2 (#299), built by B5 (#311).

The dividing line, stated once: quarantine what might be true; reject what
can never be. These tests pin the three deltas B5 adds to the legacy path:

1. A day-over-day move past the one threshold on a *fresh* date quarantines
   even with no stored same-PK contradiction (the 10× unit misread that
   previously sailed through with a warning).
2. Confirmation release: a later, independent fetch re-serving a held value
   stores it and marks the quarantine row released. Evidence-only — no
   manual path, no clock.
3. Quarantine events reach the run summary, the CI alert and the briefing
   as a warning signal. Rejected partial bars stay log-only.

Failures should be treated as findings against pipeline.divergence /
pipeline.quarantine, not as test bugs.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from config import (
    QUARANTINE_CONFIRMATION_TOLERANCE,
    SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD,
)
from pipeline import divergence, history, quarantine, store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _fetchall(db_path: Path, sql: str, params: tuple = ()) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql, params).fetchall()
    finally:
        conn.close()


def _price_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": closes,
            "High": [c + 1 for c in closes],
            "Low": [c - 1 for c in closes],
            "Close": closes,
            "Volume": [1000.0] * len(dates),
        },
        index=idx,
    )


def _prices(db_path: Path) -> list[tuple]:
    return _fetchall(db_path, "SELECT Date, Close FROM prices ORDER BY Date")


def _quarantined(db_path: Path) -> list[tuple]:
    return _fetchall(
        db_path,
        "SELECT row_key, kind, stored_value, incoming_value, released_at "
        "FROM quarantined_revisions ORDER BY row_key, incoming_value",
    )


@pytest.fixture(autouse=True)
def fresh_run():
    """Every test starts a new pipeline run, so nothing leaks between them."""
    quarantine.reset()
    yield
    quarantine.reset()


# ---------------------------------------------------------------------------
# 1. Day-over-day move on a fresh date
# ---------------------------------------------------------------------------


def test_scale_shift_on_a_fresh_date_is_quarantined(patched_db):
    """A 10× unit misread on a new date no longer sails through."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))

    assert _prices(patched_db) == [("2026-01-02", 1200.0)]
    held = _quarantined(patched_db)
    assert len(held) == 1
    row_key, kind, stored_value, incoming_value, released_at = held[0]
    assert "2026-01-05" in row_key
    assert kind == "daily_move"
    assert stored_value == 1200.0 and incoming_value == 12000.0
    assert released_at is None


def test_a_collapse_on_a_fresh_date_is_quarantined_too(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [120.0]))

    assert _prices(patched_db) == [("2026-01-02", 1200.0)]
    assert [r[1] for r in _quarantined(patched_db)] == ["daily_move"]


@pytest.mark.parametrize(
    "incoming, expect_stored",
    [
        (1200.0 * (1 + SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD), True),       # exactly at: writes
        (1200.0 * (1 + SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD) + 1, False),  # past: held
    ],
)
def test_fresh_date_threshold_boundary(patched_db, incoming, expect_stored):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [incoming]))

    stored_dates = [d for d, _ in _prices(patched_db)]
    assert ("2026-01-05" in stored_dates) is expect_stored


def test_a_big_session_still_publishes(patched_db):
    """10–20% is a fact about the market: publish + warn, unchanged."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [1380.0]))  # +15%

    assert _prices(patched_db) == [("2026-01-02", 1200.0), ("2026-01-05", 1380.0)]
    assert _quarantined(patched_db) == []


def test_no_stored_history_means_no_move_check(patched_db):
    """The first observation of a series is not an anomaly (trust ledger
    rule). On the ephemeral CI database the self-healing tables start empty
    and must not quarantine their own 15-year history against itself."""
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-02", "2026-01-05"], [1200.0, 12000.0])
    )

    assert _prices(patched_db) == [("2026-01-02", 1200.0), ("2026-01-05", 12000.0)]
    assert _quarantined(patched_db) == []


def test_an_accepted_row_in_the_same_frame_is_the_predecessor(patched_db):
    """Day N+1 is judged against day N when N is in the same fetch."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-05", "2026-01-06"], [1210.0, 12100.0])
    )

    assert _prices(patched_db) == [("2026-01-02", 1200.0), ("2026-01-05", 1210.0)]
    held = _quarantined(patched_db)
    assert len(held) == 1 and "2026-01-06" in held[0][0]
    assert held[0][2] == 1210.0  # judged against the accepted in-frame row


def test_a_held_row_is_never_the_predecessor(patched_db):
    """A bad print must not drag the next good session down with it."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-05", "2026-01-06"], [12000.0, 1210.0])
    )

    assert _prices(patched_db) == [("2026-01-02", 1200.0), ("2026-01-06", 1210.0)]
    held = _quarantined(patched_db)
    assert len(held) == 1 and "2026-01-05" in held[0][0]


def test_predecessor_is_the_latest_earlier_session_not_the_latest_stored(patched_db):
    """A backfilled older date is judged against its own predecessor."""
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-02", "2026-01-09"], [1200.0, 1250.0])
    )
    # Backfill 2026-01-05: nothing stored under that key; predecessor is 01-02.
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [1220.0]))

    assert [d for d, _ in _prices(patched_db)] == ["2026-01-02", "2026-01-05", "2026-01-09"]


def test_a_stored_zero_predecessor_never_quarantines(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [0.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [1200.0]))

    assert _prices(patched_db) == [("2026-01-02", 0.0), ("2026-01-05", 1200.0)]
    assert _quarantined(patched_db) == []


def test_series_are_never_compared_across_each_other(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybean Meal", _price_df(["2026-01-05"], [350.0]))

    assert _quarantined(patched_db) == []


def test_snapshot_only_table_keyed_date_first_is_screened_per_series(patched_db):
    """brazil_spot_prices keys (Date, commodity): the series is the commodity,
    not the date, and the predecessor lookup must know the difference."""
    store.save_brazil_spot(
        "Soybeans", pd.DataFrame({"Date": ["2026-01-02"], "price_brl_mt": [130.0]})
    )
    store.save_brazil_spot(
        "Soybeans", pd.DataFrame({"Date": ["2026-01-05"], "price_brl_mt": [1300.0]})
    )
    store.save_brazil_spot(
        "Soybean Meal", pd.DataFrame({"Date": ["2026-01-05"], "price_brl_mt": [60.0]})
    )

    rows = _fetchall(
        patched_db, "SELECT Date, commodity, price_brl FROM brazil_spot_prices ORDER BY Date"
    )
    assert rows == [("2026-01-02", "Soybeans", 130.0), ("2026-01-05", "Soybean Meal", 60.0)]
    assert [r[1] for r in _quarantined(patched_db)] == ["daily_move"]


def test_same_pk_contradiction_keeps_its_own_kind(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert [r[1] for r in _quarantined(patched_db)] == ["same_pk_revision"]


# ---------------------------------------------------------------------------
# 2. Confirmation release
# ---------------------------------------------------------------------------


def test_a_later_independent_fetch_releases_a_held_revision(patched_db):
    """Two independent observations agreeing against one stored value."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    assert _prices(patched_db) == [("2026-01-02", 1200.0)]

    quarantine.reset()  # a new pipeline run: independent fetch
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert _prices(patched_db) == [("2026-01-02", 12.0)]
    held = _quarantined(patched_db)
    assert len(held) == 1
    assert held[0][4] is not None, "the quarantine row records its release"


def test_a_held_daily_move_is_released_by_the_next_run(patched_db):
    """A genuine roll-day or devaluation costs one run, never the data."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))
    assert _prices(patched_db) == [("2026-01-02", 1200.0)]

    quarantine.reset()
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-05", "2026-01-06"], [12000.0, 12100.0])
    )

    assert _prices(patched_db) == [
        ("2026-01-02", 1200.0), ("2026-01-05", 12000.0), ("2026-01-06", 12100.0),
    ]
    assert all(r[4] is not None for r in _quarantined(patched_db))


def test_a_replay_in_the_same_run_is_not_independent(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert _prices(patched_db) == [("2026-01-02", 1200.0)]
    assert [r[4] for r in _quarantined(patched_db)] == [None]


def test_release_tolerates_float_noise_but_not_a_different_value(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    quarantine.reset()
    within = 12.0 * (1 + QUARANTINE_CONFIRMATION_TOLERANCE / 2)
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [within]))
    assert _prices(patched_db) == [("2026-01-02", within)]

    # A *different* bad value is a second corruption, not a confirmation.
    store.save_price_data("Corn", _price_df(["2026-01-02"], [1500.0]))
    store.save_price_data("Corn", _price_df(["2026-01-02"], [15.0]))
    quarantine.reset()
    store.save_price_data("Corn", _price_df(["2026-01-02"], [16.0]))
    assert _fetchall(patched_db, "SELECT Close FROM prices WHERE commodity='Corn'") == [(1500.0,)]
    unreleased = [r for r in _quarantined(patched_db) if r[4] is None and "Corn" in r[0]]
    assert sorted(r[3] for r in unreleased) == [15.0, 16.0]


def test_a_released_value_is_accepted_again_without_a_new_record(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    quarantine.reset()
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    quarantine.reset()
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert _prices(patched_db) == [("2026-01-02", 12.0)]
    assert len(_quarantined(patched_db)) == 1


def test_release_is_recorded_in_the_same_transaction_as_the_write(patched_db):
    """A release row without the released value stored would be a lie."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    quarantine.reset()

    conn = sqlite3.connect(str(patched_db))
    try:
        frame = pd.DataFrame(
            {"commodity": ["Soybeans"], "Date": ["2026-01-02"], "Open": [1.0],
             "High": [1.0], "Low": [1.0], "Close": [12.0], "Volume": [1.0]}
        )
        conn.execute("BEGIN")
        accepted, held, released = divergence.screen(
            conn, "prices", frame, ["commodity", "Date"], "prices/Soybeans"
        )
        divergence.record(conn, held)
        divergence.release(conn, released)
        conn.execute("ROLLBACK")
    finally:
        conn.close()

    assert len(accepted) == 1 and held == [] and len(released) == 1
    assert [r[4] for r in _quarantined(patched_db)] == [None]


# ---------------------------------------------------------------------------
# 3. Visibility: run summary, CI alert, briefing
# ---------------------------------------------------------------------------


def test_run_summary_counts_held_and_released(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))
    quarantine.reset()
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-09"], [120.0]))

    summary = quarantine.summary()
    assert summary["held"] == 1
    assert summary["released"] == 1
    assert summary["by_table"] == {"prices": {"held": 1, "released": 1}}
    kinds = sorted((e["kind"], e["released"]) for e in summary["events"])
    assert kinds == [("daily_move", False), ("daily_move", True)]
    event = next(e for e in summary["events"] if not e["released"])
    assert event["table"] == "prices"
    assert event["series"] == "Soybeans"
    assert event["date"] == "2026-01-09"
    assert event["divergence"] == pytest.approx(0.99)


def test_reset_starts_a_new_run(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    before = quarantine.summary()["run_id"]

    quarantine.reset()

    after = quarantine.summary()
    assert after["held"] == 0 and after["events"] == []
    assert after["run_id"] != before


def test_a_clean_run_summarises_to_zero():
    assert quarantine.summary()["held"] == 0
    assert quarantine.summary()["released"] == 0
    assert quarantine.briefing_signals(quarantine.summary()) == []


def test_briefing_signal_is_a_warning_naming_series_and_date(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))

    signals = quarantine.briefing_signals(quarantine.summary())

    assert len(signals) == 1
    signal = signals[0]
    assert signal["severity"] == "warning"
    assert signal["signal_type"] == "quarantine"
    assert signal["commodity"] == "Soybeans"
    assert signal["date"] == "2026-01-05"
    assert "Soybeans" in signal["description"] and "2026-01-05" in signal["description"]
    # The held number is not a price and is not printed as one.
    assert "12000" not in signal["description"]


def test_a_release_is_an_info_signal(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))
    quarantine.reset()
    store.save_price_data("Soybeans", _price_df(["2026-01-05"], [12000.0]))

    signals = quarantine.briefing_signals(quarantine.summary())
    assert [s["severity"] for s in signals] == ["info"]
    assert "released" in signals[0]["description"]


def test_briefing_reads_the_last_run_summary_from_pipeline_status(tmp_path, monkeypatch):
    monkeypatch.setattr("config.STORAGE_DIR", str(tmp_path))
    assert quarantine.read_run_summary() is None

    (tmp_path / "pipeline_status.json").write_text(
        json.dumps({"mode": "full", "quarantine": {"held": 2, "released": 0}}),
        encoding="utf-8",
    )
    assert quarantine.read_run_summary() == {"held": 2, "released": 0}

    (tmp_path / "pipeline_status.json").write_text(json.dumps({"mode": "full"}))
    assert quarantine.read_run_summary() is None


def test_briefing_signals_tolerate_a_missing_summary():
    assert quarantine.briefing_signals(None) == []


def test_alert_lines_name_the_held_rows():
    summary = {
        "held": 1, "released": 1,
        "by_table": {"prices": {"held": 1, "released": 1}},
        "events": [
            {"table": "prices", "label": "prices/Soybeans", "kind": "daily_move",
             "series": "Soybeans", "date": "2026-01-09", "divergence": 0.99,
             "released": False},
            {"table": "prices", "label": "prices/Soybeans", "kind": "daily_move",
             "series": "Soybeans", "date": "2026-01-05", "divergence": 9.0,
             "released": True},
        ],
    }
    lines = quarantine.alert_lines(summary)
    text = "\n".join(lines)
    assert "prices" in text and "Soybeans" in text and "2026-01-09" in text
    assert "released" in text
    assert quarantine.alert_lines(None) == []
    assert quarantine.alert_lines({"held": 0, "released": 0, "by_table": {}, "events": []}) == []


# ---------------------------------------------------------------------------
# The quarantine must survive the ephemeral CI runner, or the release rule
# can never fire there
# ---------------------------------------------------------------------------


def test_quarantine_round_trips_through_history(patched_db, tmp_path, monkeypatch):
    assert "quarantined_revisions" in history.HISTORY_TABLES
    history_dir = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(history_dir))
    monkeypatch.setattr(
        "pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db))
    )

    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    history.export_history()
    assert (history_dir / "quarantined_revisions.csv").exists()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM quarantined_revisions")
    conn.commit()
    conn.close()
    history.import_history()

    quarantine.reset()  # tomorrow's run, seeded from the committed CSV
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    assert _prices(patched_db) == [("2026-01-02", 12.0)]
    assert [r[4] is not None for r in _quarantined(patched_db)] == [True]


def test_orchestrator_carries_quarantine_signals_into_the_briefing(patched_db, monkeypatch):
    from analysis.briefing import generate_briefing_data, orchestrator

    summary = {
        "run_id": "x", "held": 1, "released": 0,
        "by_table": {"prices": {"held": 1, "released": 0}},
        "events": [
            {"table": "prices", "label": "prices/Soybeans", "kind": "daily_move",
             "series": "Soybeans", "date": "2026-01-09", "divergence": 0.99,
             "released": False},
        ],
    }
    monkeypatch.setattr(orchestrator.quarantine, "read_run_summary", lambda: summary)

    data = generate_briefing_data(archive=False)

    assert any(s["signal_type"] == "quarantine" for s in data.signals)
    assert "2026-01-09" in data.section_texts["signals"]
