"""Same-PK divergence quarantine at the store layer (T19 · F9, #67).

INSERT OR REPLACE lets one corrupted fetch overwrite a good stored value
under the same primary key. These tests pin the guard: a price that
disagrees with what is already stored for *that same observation* by more
than the threshold is held back, recorded, and logged loudly — and the
stored value survives.

Failures should be treated as findings against pipeline.divergence /
pipeline.store, not as test bugs.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from config import SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD
from pipeline import divergence, query, store

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


def _currency_df(dates: list[str], closes: list[float]) -> pd.DataFrame:
    idx = pd.to_datetime(dates)
    idx.name = "Date"
    return pd.DataFrame(
        {
            "Open": closes,
            "High": [c + 0.01 for c in closes],
            "Low": [c - 0.01 for c in closes],
            "Close": closes,
        },
        index=idx,
    )


def _quarantined(db_path: Path) -> list[tuple]:
    return _fetchall(
        db_path,
        "SELECT table_name, row_key, value_column, stored_value, incoming_value "
        "FROM quarantined_revisions ORDER BY row_key",
    )


# ---------------------------------------------------------------------------
# The headline: a corrupted close does not overwrite a good one
# ---------------------------------------------------------------------------


def test_corrupted_close_does_not_overwrite_stored_price(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(1200.0,)]


def test_quarantined_row_is_recorded_with_both_values(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    rows = _quarantined(patched_db)
    assert len(rows) == 1
    table_name, row_key, value_column, stored_value, incoming_value = rows[0]
    assert table_name == "prices"
    assert value_column == "Close"
    assert stored_value == 1200.0
    assert incoming_value == 12.0
    assert "Soybeans" in row_key and "2026-01-02" in row_key


def test_quarantine_keeps_the_whole_rejected_row(patched_db):
    """The rejected row is auditable in full, not just its close."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    (payload,) = _fetchall(patched_db, "SELECT row_json FROM quarantined_revisions")[0]
    assert '"Close": 12.0' in payload
    assert '"Volume": 1000.0' in payload


def test_divergence_is_flagged_at_error_level(patched_db, caplog):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    with caplog.at_level(logging.ERROR, logger="pipeline.divergence"):
        store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert errors, "a quarantined revision must be logged loudly"
    assert "quarantine" in " ".join(r.getMessage() for r in errors).lower()


# ---------------------------------------------------------------------------
# What must still get through
# ---------------------------------------------------------------------------


def test_first_observation_of_a_key_is_always_accepted(patched_db):
    """Nothing stored means nothing to diverge from."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(12.0,)]
    assert _quarantined(patched_db) == []


def test_ordinary_revision_still_overwrites(patched_db):
    """A 1% correction is a correction, not a corruption."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1212.0]))

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(1212.0,)]
    assert _quarantined(patched_db) == []


def test_one_bad_row_does_not_hold_back_the_good_rows(patched_db):
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-02", "2026-01-05"], [1200.0, 1210.0])
    )
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-02", "2026-01-05"], [12.0, 1215.0])
    )

    assert _fetchall(
        patched_db, "SELECT Date, Close FROM prices ORDER BY Date"
    ) == [("2026-01-02", 1200.0), ("2026-01-05", 1215.0)]
    assert len(_quarantined(patched_db)) == 1


def test_a_different_key_is_never_compared(patched_db):
    """Meal at 350 must not be screened against beans at 1200."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybean Meal", _price_df(["2026-01-02"], [350.0]))

    assert sorted(_fetchall(patched_db, "SELECT Close FROM prices")) == [
        (350.0,), (1200.0,)
    ]
    assert _quarantined(patched_db) == []


def test_a_zero_stored_value_never_quarantines(patched_db):
    """|incoming - 0| / 0 is not a measurement — the fill-in is accepted."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [0.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(1200.0,)]
    assert _quarantined(patched_db) == []


def test_unguarded_table_is_untouched(patched_db):
    """A weather series legitimately swings; only price columns are screened."""
    frame = pd.DataFrame(
        {"Date": ["2026-01-02"], "temp_max": [30.0], "temp_min": [1.0],
         "precipitation": [0.0]}
    )
    store.save_weather_data("Mato Grosso", frame)
    frame["temp_max"] = [2.0]
    store.save_weather_data("Mato Grosso", frame)

    assert _fetchall(patched_db, "SELECT temp_max FROM weather") == [(2.0,)]
    assert _quarantined(patched_db) == []


# ---------------------------------------------------------------------------
# The threshold itself
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("incoming", "expect_stored"),
    [
        # Exactly at the threshold is accepted — the rule is "diverging *past*".
        (1200.0 * (1 + float(SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD)), None),
        # A hair past it is not.
        (1200.0 * (1 + float(SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD) + 0.001), 1200.0),
    ],
)
def test_threshold_boundary(patched_db, incoming: float, expect_stored: float | None):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [incoming]))

    expected = expect_stored if expect_stored is not None else incoming
    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(expected,)]


def test_a_collapse_quarantines_as_readily_as_a_spike(patched_db):
    """Divergence is a magnitude, not a direction."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [600.0]))

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(1200.0,)]
    assert len(_quarantined(patched_db)) == 1


# ---------------------------------------------------------------------------
# Coverage beyond prices
# ---------------------------------------------------------------------------


def test_currencies_are_screened_too(patched_db):
    store.save_currency_data("BRL=X", _currency_df(["2026-01-02"], [5.40]))
    store.save_currency_data("BRL=X", _currency_df(["2026-01-02"], [0.185]))

    assert _fetchall(patched_db, "SELECT Close FROM currencies") == [(5.40,)]
    assert len(_quarantined(patched_db)) == 1


def test_snapshot_only_price_tables_are_screened(patched_db):
    """These are the tables where an overwrite is unrecoverable (no upstream history)."""
    good = pd.DataFrame({"Date": ["2026-01-02"], "price_brl_mt": [130.0]})
    store.save_brazil_spot("Soybeans", good)
    store.save_brazil_spot(
        "Soybeans", pd.DataFrame({"Date": ["2026-01-02"], "price_brl_mt": [13.0]})
    )

    assert _fetchall(patched_db, "SELECT price_brl FROM brazil_spot_prices") == [(130.0,)]
    assert len(_quarantined(patched_db)) == 1


# ---------------------------------------------------------------------------
# Re-running the same corrupted fetch
# ---------------------------------------------------------------------------


def test_replaying_the_same_corruption_does_not_multiply_records(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    assert len(_quarantined(patched_db)) == 1
    assert _fetchall(patched_db, "SELECT Close FROM prices") == [(1200.0,)]


def test_two_distinct_corruptions_are_both_kept(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [99999.0]))

    assert len(_quarantined(patched_db)) == 2


# ---------------------------------------------------------------------------
# The screen in isolation
# ---------------------------------------------------------------------------


def test_a_duplicate_index_label_does_not_drop_a_good_row(patched_db):
    """The held row is removed positionally — dropping by index *label* would
    take every row sharing that label, discarding good observations."""
    store.save_price_data(
        "Soybeans", _price_df(["2026-01-02", "2026-01-05"], [1200.0, 1210.0])
    )
    frame = pd.DataFrame(
        {
            "commodity": ["Soybeans", "Soybeans"],
            "Date": ["2026-01-02", "2026-01-05"],
            "Open": [12.0, 1215.0], "High": [12.0, 1215.0], "Low": [12.0, 1215.0],
            "Close": [12.0, 1215.0], "Volume": [1.0, 1.0],
        },
        index=[0, 0],  # both rows share one label
    )
    conn = sqlite3.connect(str(patched_db))
    try:
        accepted, held = divergence.screen(
            conn, "prices", frame, ["commodity", "Date"], "prices/Soybeans"
        )
    finally:
        conn.close()

    assert len(held) == 1
    assert accepted["Close"].tolist() == [1215.0]


def test_a_non_numeric_value_passes_through_rather_than_failing_the_save(patched_db):
    """Divergence is a ratio; a cell that is not a number has none to take."""
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    frame = _price_df(["2026-01-02"], [1200.0])
    frame["Close"] = frame["Close"].astype(object)
    frame.loc[frame.index[0], "Close"] = "n/a"

    store.save_price_data("Soybeans", frame)  # must not raise

    assert _fetchall(patched_db, "SELECT Close FROM prices") == [("n/a",)]
    assert _quarantined(patched_db) == []


def test_read_quarantined_revisions_serves_the_audit(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [12.0]))

    held = query.read_quarantined_revisions()
    assert len(held) == 1
    assert held["table_name"].iloc[0] == "prices"
    assert query.read_quarantined_revisions("currencies").empty


def test_read_quarantined_revisions_is_empty_on_a_clean_run(patched_db):
    store.save_price_data("Soybeans", _price_df(["2026-01-02"], [1200.0]))

    assert query.read_quarantined_revisions().empty


def test_screen_returns_the_frame_untouched_when_nothing_is_stored(patched_db):
    conn = sqlite3.connect(str(patched_db))
    try:
        frame = pd.DataFrame(
            {"commodity": ["Soybeans"], "Date": ["2026-01-02"], "Open": [1.0],
             "High": [1.0], "Low": [1.0], "Close": [1200.0], "Volume": [1.0]}
        )
        accepted, held = divergence.screen(
            conn, "prices", frame, ["commodity", "Date"], "prices/Soybeans"
        )
    finally:
        conn.close()

    assert held == []
    assert accepted is frame
