"""Tests for the bar-fed continuous series (A4 #301 / B4 #310).

`build_from_bars` stitches Layer 11b's named-contract daily bars on the
package's own roll rule (5 business days before last trade). Around the
Sep 2026 soybean roll that rule assigns ZSU26 through Thu 2026-09-03 and
ZSX26 from Fri 2026-09-04 — verified against `active_contract` directly,
so a rule change breaks these loudly rather than silently.

What is pinned:
- the ratio adjustment is struck from the *same session's* two closes on
  the roll day, never from consecutive prints;
- a session where the assigned contract did not print is a gap, not a
  substitution;
- a roll with no joinable session drops the older history with a stated
  reason, rather than gluing unadjusted segments;
- levels after the newest roll are untouched real prices in every mode.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from analysis.futures.continuous import (
    MIN_SESSIONS,
    active_contract,
    build_from_bars,
)
from analysis.futures.domain import RollMethod

# Sessions around the Sep 2026 soybean roll (all weekdays).
PRE_ROLL = ["2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28",
            "2026-08-31", "2026-09-01", "2026-09-02", "2026-09-03"]
POST_ROLL = ["2026-09-04", "2026-09-07", "2026-09-08", "2026-09-09",
             "2026-09-10", "2026-09-11"]
ROLL_DAY = date(2026, 9, 4)

U26, X26 = "ZSU26.CBT", "ZSX26.CBT"


def _bars(closes: dict[str, dict[str, float]]) -> pd.DataFrame:
    """{ticker: {date: close}} → the long frame Layer 11b stores."""
    rows = [
        {"ticker": ticker, "Date": day, "Close": close}
        for ticker, series in closes.items()
        for day, close in series.items()
    ]
    return pd.DataFrame(rows)


def _both_contracts_printing() -> pd.DataFrame:
    """U26 and X26 print every session; X26 sits 10.0 over U26."""
    u = {d: 1000.0 + i for i, d in enumerate(PRE_ROLL + POST_ROLL)}
    x = {d: u[d] + 10.0 for d in PRE_ROLL + POST_ROLL}
    return _bars({U26: u, X26: x})


def test_roll_rule_assigns_what_these_tests_assume():
    assert active_contract("Soybeans", date(2026, 9, 3)).provider_symbol == U26
    assert active_contract("Soybeans", ROLL_DAY).provider_symbol == X26


def test_unadjusted_levels_are_the_raw_closes():
    series = build_from_bars(_both_contracts_printing(), "Soybeans", adjustment="unadjusted")
    assert series is not None
    assert series.roll_method is RollMethod.CALENDAR_ROLL_UNADJUSTED
    levels = dict((d.isoformat(), p) for d, p in series.points)
    assert levels["2026-09-03"] == 1008.0          # U26's own close
    assert levels["2026-09-04"] == 1019.0          # X26's own close (1009 + 10)


def test_ratio_adjustment_is_struck_same_session_on_the_roll_day():
    series = build_from_bars(_both_contracts_printing(), "Soybeans", adjustment="ratio")
    assert series is not None
    levels = dict((d.isoformat(), p) for d, p in series.points)
    # Same-session step on 09-04: X26/U26 = 1019/1009.
    step = 1019.0 / 1009.0
    assert levels["2026-09-03"] == pytest.approx(1008.0 * step)
    assert levels["2026-08-24"] == pytest.approx(1000.0 * step)
    # Post-roll levels are real prices, untouched.
    assert levels["2026-09-04"] == 1019.0
    assert levels["2026-09-11"] == 1024.0


def test_roll_date_and_contract_labels_are_recorded():
    series = build_from_bars(_both_contracts_printing(), "Soybeans", adjustment="ratio")
    assert series.roll_dates == (ROLL_DAY,)
    by_date = dict(series.contract_by_date)
    assert by_date[date(2026, 9, 3)] == U26
    assert by_date[ROLL_DAY] == X26
    assert series.is_hedgeable is False


def test_missing_session_is_a_gap_not_a_substitution():
    frame = _both_contracts_printing()
    # U26 did not print 2026-08-27 — X26 did, but must not stand in.
    frame = frame[~((frame["ticker"] == U26) & (frame["Date"] == "2026-08-27"))]
    series = build_from_bars(frame, "Soybeans", adjustment="unadjusted")
    days = {d.isoformat() for d, _ in series.points}
    assert "2026-08-27" not in days


def test_unjoinable_roll_drops_older_history_with_reason():
    # U26 stops printing a week before the roll: no session in the
    # 5-session lookback holds both closes, so pre-roll history must go.
    u = {d: 1000.0 + i for i, d in enumerate(PRE_ROLL[:5])}       # ends 08-28
    x = {d: 1100.0 for d in PRE_ROLL + POST_ROLL}
    # Enough post-roll sessions to clear the floor on their own.
    extra = ["2026-09-14", "2026-09-15", "2026-09-16", "2026-09-17", "2026-09-18"]
    for i, d in enumerate(extra):
        x[d] = 1100.0 + i
    series = build_from_bars(_bars({U26: u, X26: x}), "Soybeans", adjustment="ratio")
    assert series is not None
    days = sorted(d for d, _ in series.points)
    assert days[0] == ROLL_DAY                     # everything earlier dropped
    assert "dropped" in series.adjustment_note
    assert U26.split(".")[0] in series.adjustment_note or U26 in series.adjustment_note


def test_below_floor_returns_none():
    u = {d: 1000.0 for d in PRE_ROLL[: MIN_SESSIONS - 1]}
    assert build_from_bars(_bars({U26: u}), "Soybeans", adjustment="ratio") is None


def test_empty_and_unknown_adjustment():
    assert build_from_bars(pd.DataFrame(), "Soybeans") is None
    with pytest.raises(ValueError):
        build_from_bars(_both_contracts_printing(), "Soybeans", adjustment="panama")


def test_adjustment_note_travels_with_the_series():
    series = build_from_bars(_both_contracts_printing(), "Soybeans", adjustment="ratio")
    assert "NOT tradeable" in series.adjustment_note
