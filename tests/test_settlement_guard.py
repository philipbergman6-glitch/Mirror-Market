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

from fetchers._settlement import (
    EXCHANGE_SESSION,
    FX_SESSION,
    drop_unsettled_session,
    session_is_settled,
)

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
    assert "2026-08-06" in caplog.text  # the newest bar that IS settled
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


# ---------------------------------------------------------------------------
# The overnight hole, both venues
# ---------------------------------------------------------------------------
#
# The guard used to ask "has Chicago settled?" and, if yes, keep everything.
# Both cases below produce a bar labelled with a date the venue has not
# reached the end of, at a moment when Chicago has settled — so both survived.


def test_fx_bar_opened_at_the_ny_close_is_not_a_close():
    """The live 2026-08-19 defect, pinned.

    At 03:45 UTC on 2026-08-19 (22:45 Chicago, 23:45 New York) yfinance
    returned a BRL=X bar labelled 2026-08-19 whose High equalled its Open and
    Low equalled its Close — an FX day under four hours old. Chicago was past
    14:30, so the exchange rule declared everything settled and it was stored
    as that day's FX close.
    """
    now = datetime(2026, 8, 19, 3, 45, tzinfo=timezone.utc)
    df = _frame(["2026-08-17", "2026-08-18", "2026-08-19"])

    kept = drop_unsettled_session(df, label="BRL=X", now=now, rule=FX_SESSION)
    assert list(kept.index.strftime("%Y-%m-%d")) == ["2026-08-17", "2026-08-18"]


def test_fx_bar_is_complete_once_the_ny_close_has_passed():
    """18:00 New York on D: the bar labelled D closed an hour ago."""
    now = datetime(2026, 8, 19, 22, 0, tzinfo=timezone.utc)  # 18:00 EDT
    df = _frame(["2026-08-18", "2026-08-19"])
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=FX_SESSION), df)


def test_fx_bar_is_still_open_at_1659_new_york():
    """One minute before the roll, D's bar is still being written."""
    now = datetime(2026, 8, 19, 20, 59, tzinfo=timezone.utc)  # 16:59 EDT
    df = _frame(["2026-08-18", "2026-08-19"])
    out = drop_unsettled_session(df, now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-18"]


def test_fx_rule_follows_us_dst():
    """21:30 UTC is past 17:00 New York in EDT but not in EST."""
    df = _frame(["2026-12-03", "2026-12-04"])
    winter = datetime(2026, 12, 4, 21, 30, tzinfo=timezone.utc)  # 16:30 EST
    assert list(
        drop_unsettled_session(df, now=winter, rule=FX_SESSION).index.strftime("%Y-%m-%d")
    ) == ["2026-12-03"]

    summer_df = _frame(["2026-08-18", "2026-08-19"])
    summer = datetime(2026, 8, 19, 21, 30, tzinfo=timezone.utc)  # 17:30 EDT
    pdt.assert_frame_equal(
        drop_unsettled_session(summer_df, now=summer, rule=FX_SESSION), summer_df
    )


def test_futures_overnight_session_bar_is_dropped():
    """CME opens the next trade date at 19:00 CT; that bar is not a close.

    The old rule dropped rows *equal to* the Chicago local date, so a bar
    labelled D+1 emitted during D's evening compared unequal and survived —
    while `session_is_settled` simultaneously said yes.
    """
    now = datetime(2026, 8, 19, 1, 30, tzinfo=timezone.utc)  # 20:30 CT on 08-18
    df = _frame(["2026-08-17", "2026-08-18", "2026-08-19"])
    out = drop_unsettled_session(df, label="ZS=F", now=now, rule=EXCHANGE_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-17", "2026-08-18"]


def test_weekend_run_touches_nothing():
    """Saturday: no venue has an open session, so no bar can be unfinished."""
    now = datetime(2026, 8, 22, 15, 0, tzinfo=timezone.utc)  # Saturday 10:00 CT
    df = _frame(["2026-08-20", "2026-08-21"])
    for rule in (EXCHANGE_SESSION, FX_SESSION):
        pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=rule), df)


def test_sunday_fx_reopen_does_not_publish_mondays_open_bar():
    """FX reopens 17:00 New York Sunday; the bar it starts is labelled Monday."""
    now = datetime(2026, 8, 24, 1, 0, tzinfo=timezone.utc)  # 21:00 EDT Sunday
    df = _frame(["2026-08-21", "2026-08-24"])
    out = drop_unsettled_session(df, now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-21"]


def test_holiday_leaves_the_prior_settled_session_standing():
    """A closed venue emits no bar for the holiday; the guard must not reach back.

    2026-12-25 is a Friday and a CME holiday. A run that evening sees the
    24th as the newest bar, and the 24th settled.
    """
    now = datetime(2026, 12, 25, 22, 0, tzinfo=timezone.utc)  # 16:00 CST
    df = _frame(["2026-12-23", "2026-12-24"])
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now), df)


def test_currencies_fetch_uses_the_fx_rule(monkeypatch):
    """The rule must actually be wired to Layer 7, not merely exist."""
    import fetchers.yfinance as yf_module

    seen: list[object] = []

    def fake_fetch_one(ticker, period=None, rule=None):
        seen.append(rule)
        return pd.DataFrame()

    monkeypatch.setattr(yf_module, "fetch_one", fake_fetch_one)
    yf_module.fetch_currencies(period="5d")
    assert seen and all(rule is FX_SESSION for rule in seen)


def test_prices_fetch_uses_the_exchange_rule(monkeypatch):
    import fetchers.yfinance as yf_module

    seen: list[object] = []

    def fake_fetch_one(ticker, period=None, rule=EXCHANGE_SESSION):
        seen.append(rule)
        return pd.DataFrame()

    monkeypatch.setattr(yf_module, "fetch_one", fake_fetch_one)
    yf_module.fetch_all(period="5d")
    assert seen and all(rule is EXCHANGE_SESSION for rule in seen)
