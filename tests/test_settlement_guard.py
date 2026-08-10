"""Tests for the settlement guard (fetchers/_settlement.py).

The guard exists because a pipeline run landing mid-session stored an
unfinished Yahoo bar as the day's close (observed: ZS=F 2026-08-07 stored
at 1181.25 vs a 1156.50 settlement, a 2.1% error). These tests pin the
contract: before the venue settles, the current session's row must not
survive; after it settles, nothing is touched.

The clock is injected everywhere — these must not depend on when they run.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import pandas as pd
import pandas.testing as pdt
import pytest

from fetchers._settlement import drop_unsettled_session, session_is_settled

# 2026-08-07 was a Friday, CDT (UTC-5): settlement 18:15 UTC,
# guard cutoff 14:30 CT = 19:30 UTC.
MIDSESSION = datetime(2026, 8, 7, 14, 15, tzinfo=timezone.utc)   # 09:15 CT
AFTER_SETTLE = datetime(2026, 8, 7, 21, 0, tzinfo=timezone.utc)  # 16:00 CT

# 2026-12-04 was a Friday, CST (UTC-6): settlement 19:15 UTC,
# guard cutoff 14:30 CT = 20:30 UTC.
WINTER_EARLY = datetime(2026, 12, 4, 20, 0, tzinfo=timezone.utc)  # 14:00 CT
WINTER_LATE = datetime(2026, 12, 4, 21, 0, tzinfo=timezone.utc)   # 15:00 CT


def _frame(dates: list[str]) -> pd.DataFrame:
    idx = pd.DatetimeIndex(pd.to_datetime(dates), name="Date")
    return pd.DataFrame(
        {
            "Open": range(len(dates)),
            "High": range(len(dates)),
            "Low": range(len(dates)),
            "Close": range(len(dates)),
            "Volume": range(len(dates)),
        },
        index=idx,
    )


def test_drops_current_session_before_cutoff():
    df = _frame(["2026-08-05", "2026-08-06", "2026-08-07"])
    out = drop_unsettled_session(df, label="ZS=F", now=MIDSESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-05", "2026-08-06"]


def test_keeps_current_session_after_cutoff():
    df = _frame(["2026-08-05", "2026-08-06", "2026-08-07"])
    out = drop_unsettled_session(df, label="ZS=F", now=AFTER_SETTLE)
    pdt.assert_frame_equal(out, df)


def test_cutoff_follows_us_dst_not_a_fixed_utc_offset():
    """20:00 UTC is past the cutoff in CDT but not in CST."""
    df = _frame(["2026-12-03", "2026-12-04"])

    early = drop_unsettled_session(df, now=WINTER_EARLY)
    assert list(early.index.strftime("%Y-%m-%d")) == ["2026-12-03"]

    late = drop_unsettled_session(df, now=WINTER_LATE)
    pdt.assert_frame_equal(late, df)


def test_run_spilling_past_utc_midnight_keeps_the_settled_bar():
    """00:30 UTC Saturday is 19:30 Friday in Chicago — Friday has settled."""
    df = _frame(["2026-08-06", "2026-08-07"])
    saturday_utc = datetime(2026, 8, 8, 0, 30, tzinfo=timezone.utc)
    pdt.assert_frame_equal(drop_unsettled_session(df, now=saturday_utc), df)


def test_no_row_for_the_current_session_is_a_noop():
    df = _frame(["2026-08-05", "2026-08-06"])
    pdt.assert_frame_equal(drop_unsettled_session(df, now=MIDSESSION), df)


def test_does_not_mutate_input():
    df = _frame(["2026-08-06", "2026-08-07"])
    before = df.copy()
    drop_unsettled_session(df, label="ZS=F", now=MIDSESSION)
    pdt.assert_frame_equal(df, before)


def test_empty_frame_passes_through():
    df = pd.DataFrame()
    pdt.assert_frame_equal(drop_unsettled_session(df, now=MIDSESSION), df)


def test_drop_is_logged_not_silent(caplog):
    df = _frame(["2026-08-06", "2026-08-07"])
    with caplog.at_level(logging.WARNING, logger="fetchers._settlement"):
        drop_unsettled_session(df, label="ZS=F", now=MIDSESSION)
    assert "2026-08-07" in caplog.text
    assert "ZS=F" in caplog.text


def test_tz_aware_index_uses_the_bar_label_not_a_conversion():
    """The date label on a daily bar is the session date; don't shift it."""
    idx = pd.DatetimeIndex(
        pd.to_datetime(["2026-08-06 00:00", "2026-08-07 00:00"]), name="Date"
    ).tz_localize("UTC")
    df = pd.DataFrame({"Close": [1.0, 2.0]}, index=idx)
    out = drop_unsettled_session(df, now=MIDSESSION)
    assert len(out) == 1


def test_naive_now_is_treated_as_utc():
    df = _frame(["2026-08-06", "2026-08-07"])
    naive = MIDSESSION.replace(tzinfo=None)
    assert len(drop_unsettled_session(df, now=naive)) == 1


@pytest.mark.parametrize(
    "now,expected",
    [(MIDSESSION, False), (AFTER_SETTLE, True), (WINTER_EARLY, False), (WINTER_LATE, True)],
)
def test_session_is_settled(now, expected):
    assert session_is_settled(now) is expected
