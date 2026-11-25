"""B47 (#622): a stored forward-curve leg captured before Yahoo finished the bar
is corrected once the final bar is in ``contract_bars``.

``forward_curve`` is keyed by ``fetched_date``, so a row was written once and
never re-read: 671 of 1,037 soy rows in ``data/history/`` hold a value that is
not their own observation date's final close (an overnight print or a last
trade). ``contract_bars`` (Layer 11b) re-fetches every listed contract in full
on every daily run, so it holds the final bar. The repair copies that bar onto
the curve leg — the same ticker, the same session — and nothing else.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date

import pandas as pd
import pytest

from pipeline import store

TODAY = date(2026, 11, 27)


def _curve_row(fetched: str, ticker: str, close: float, observed: str | None = "2026-11-24",
               volume: float | None = 4244.0, commodity: str = "Soybean Oil") -> tuple:
    return (commodity, "2027-01-01" if "F27" in ticker else "2027-03-01", ticker[:6], ticker,
            close, observed, volume, None, fetched)


def _bar_row(ticker: str, day: str, close: float, fetched: str, volume: float | None = 80123.0,
             commodity: str = "Soybean Oil") -> tuple:
    return (commodity, ticker, "2027-01-01", day, close, close, close, close, volume, fetched)


def _seed(db: str, curve: list[tuple], bars: list[tuple]) -> None:
    with sqlite3.connect(db) as conn:
        conn.executemany(
            "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
            "observation_date, volume, open_interest, fetched_date) VALUES (?,?,?,?,?,?,?,?,?)",
            curve,
        )
        conn.executemany(
            "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Open, High, Low, "
            "Close, Volume, fetched_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
            bars,
        )


def _curve(db: str) -> pd.DataFrame:
    with sqlite3.connect(db) as conn:
        return pd.read_sql(
            "SELECT fetched_date, ticker, close, volume FROM forward_curve ORDER BY fetched_date, ticker",
            conn,
        )


def test_an_overnight_capture_is_replaced_by_the_final_bar(patched_db) -> None:
    """The ticket fixture: the 2026-11-25 ZLF27 leg holds 57.84 at observation
    11-24 with volume 4,244 — an overnight print. The final 11-24 bar is 58.32
    on ~80k contracts, fetched on 11-26 (after New York midnight)."""
    _seed(str(patched_db),
          [_curve_row("2026-11-25", "ZLF27.CBT", 57.84), _curve_row("2026-11-25", "ZLH27.CBT", 57.90)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 58.32, "2026-11-26"),
           _bar_row("ZLH27.CBT", "2026-11-24", 58.41, "2026-11-26", volume=20000.0)])

    repaired = store.repair_forward_curve(today=TODAY)

    assert repaired == {"Soybean Oil": 2}
    out = _curve(str(patched_db))
    assert out.close.tolist() == [58.32, 58.41]
    assert out.volume.tolist() == [80123.0, 20000.0]


def test_a_leg_already_equal_to_the_final_bar_is_not_counted(patched_db) -> None:
    _seed(str(patched_db), [_curve_row("2026-11-25", "ZLF27.CBT", 58.32, volume=80123.0)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 58.32, "2026-11-26")])
    assert store.repair_forward_curve(today=TODAY) == {}


def test_a_bar_not_provably_final_is_never_copied(patched_db) -> None:
    """A bar fetched the day after its label may have been read before New
    York midnight (04:00/05:00 UTC), so it proves nothing. Copying it would
    swap one pre-final value for another."""
    _seed(str(patched_db), [_curve_row("2026-11-25", "ZLF27.CBT", 57.84)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 57.82, "2026-11-25")])
    assert store.repair_forward_curve(today=TODAY) == {}
    assert _curve(str(patched_db)).close.tolist() == [57.84]


def test_a_snapshot_is_repaired_whole_or_not_at_all(patched_db, caplog) -> None:
    """Invariant 8: a curve is one session's number. Repairing some legs of a
    snapshot and leaving the rest overnight prints would turn the overnight
    move into fake curve structure, so a snapshot with any leg lacking a
    final bar keeps every leg as captured, and says so."""
    _seed(str(patched_db),
          [_curve_row("2026-11-25", "ZLF27.CBT", 57.84), _curve_row("2026-11-25", "ZLH27.CBT", 57.90)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 58.32, "2026-11-26")])  # no ZLH27 bar
    with caplog.at_level(logging.WARNING, logger="pipeline.store"):
        assert store.repair_forward_curve(today=TODAY) == {}
    assert _curve(str(patched_db)).close.tolist() == [57.84, 57.90]
    assert "ZLH27.CBT" in caplog.text and "not repaired" in caplog.text


def test_a_leg_with_no_observation_date_is_left_alone(patched_db) -> None:
    """Rows from before observation_date existed never said which session
    they were. Matching them to one would invent it."""
    _seed(str(patched_db), [_curve_row("2026-11-25", "ZLF27.CBT", 57.84, observed=None)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 58.32, "2026-11-26")])
    assert store.repair_forward_curve(today=TODAY) == {}


def test_the_repair_is_bounded_by_its_lookback(patched_db) -> None:
    _seed(str(patched_db),
          [_curve_row("2026-07-01", "ZLF27.CBT", 57.84, observed="2026-06-30")],
          [_bar_row("ZLF27.CBT", "2026-06-30", 58.32, "2026-11-26")])
    assert store.repair_forward_curve(today=TODAY, lookback_days=30) == {}
    assert store.repair_forward_curve(today=TODAY, lookback_days=200) == {"Soybean Oil": 1}


def test_the_repair_is_idempotent_and_logged(patched_db, caplog) -> None:
    _seed(str(patched_db), [_curve_row("2026-11-25", "ZLF27.CBT", 57.84)],
          [_bar_row("ZLF27.CBT", "2026-11-24", 58.32, "2026-11-26")])
    with caplog.at_level(logging.WARNING, logger="pipeline.store"):
        assert store.repair_forward_curve(today=TODAY) == {"Soybean Oil": 1}
    assert "Soybean Oil" in caplog.text and "2026-11-25" in caplog.text and "2026-11-24" in caplog.text
    assert store.repair_forward_curve(today=TODAY) == {}


def test_a_lookback_below_one_day_is_refused(patched_db) -> None:
    with pytest.raises(ValueError, match="lookback"):
        store.repair_forward_curve(today=TODAY, lookback_days=0)
