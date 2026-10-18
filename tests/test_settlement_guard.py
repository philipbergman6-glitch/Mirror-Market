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
    SessionRule,
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

    At 03:45 UTC on 2026-08-19 (22:45 Chicago, 04:45 London) yfinance
    returned a BRL=X bar labelled 2026-08-19 whose High equalled its Open and
    Low equalled its Close — an FX day under five hours old. Chicago was past
    14:30, so the exchange rule declared everything settled and it was stored
    as that day's FX close.
    """
    now = datetime(2026, 8, 19, 3, 45, tzinfo=timezone.utc)
    df = _frame(["2026-08-17", "2026-08-18", "2026-08-19"])

    kept = drop_unsettled_session(df, label="BRL=X", now=now, rule=FX_SESSION)
    assert list(kept.index.strftime("%Y-%m-%d")) == ["2026-08-17", "2026-08-18"]


# B20 (#482). Yahoo's FX daily bar is a London calendar day: the chart API
# reports exchangeTimezoneName Europe/London, a regular period of
# 00:00-23:59 London, and stamps each bar at 00:00 London — 23:00 UTC in BST.
# yfinance hands the bar over labelled with that London date (2026-10-12T23:00Z
# arrives as 2026-10-13; measured 2026-10-18 for BRL/ZAR/CNY/INR), either
# naive or as a Europe/London-aware midnight. Both forms are pinned.


def _london_day_frame(dates: list[str], aware: bool) -> pd.DataFrame:
    df = _frame(dates)
    if aware:
        df.index = df.index.tz_localize("Europe/London")
    return df


@pytest.mark.parametrize("aware", [False, True], ids=["naive", "london-aware"])
def test_fx_bar_stamped_23z_is_open_at_22z_in_bst(aware):
    """Ticket fixture, summer: the 2026-10-12T23:00Z bar is London day 10-13,
    which ends 23:00 UTC on 10-13. At 22:00 UTC it has an hour to run."""
    df = _london_day_frame(["2026-10-12", "2026-10-13"], aware)
    now = datetime(2026, 10, 13, 22, 0, tzinfo=timezone.utc)  # 23:00 BST
    out = drop_unsettled_session(df, label="BRLUSD=X", now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-10-12"]


@pytest.mark.parametrize("aware", [False, True], ids=["naive", "london-aware"])
def test_fx_bar_stamped_23z_is_closed_after_the_london_day_end_in_bst(aware):
    df = _london_day_frame(["2026-10-12", "2026-10-13"], aware)
    now = datetime(2026, 10, 13, 23, 0, tzinfo=timezone.utc)  # 00:00 BST on 10-14
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=FX_SESSION), df)


@pytest.mark.parametrize("aware", [False, True], ids=["naive", "london-aware"])
def test_fx_bar_is_open_until_utc_midnight_in_gmt(aware):
    """Ticket fixture, winter: London day 12-03 ends 00:00 UTC on 12-04, so
    both the 22:00 UTC clock and the 23:30 UTC catch-up cron still see it open."""
    df = _london_day_frame(["2026-12-02", "2026-12-03"], aware)
    for now in (
        datetime(2026, 12, 3, 22, 0, tzinfo=timezone.utc),
        datetime(2026, 12, 3, 23, 30, tzinfo=timezone.utc),
    ):
        out = drop_unsettled_session(df, now=now, rule=FX_SESSION)
        assert list(out.index.strftime("%Y-%m-%d")) == ["2026-12-02"], now


@pytest.mark.parametrize("aware", [False, True], ids=["naive", "london-aware"])
def test_fx_bar_is_closed_after_the_london_day_end_in_gmt(aware):
    df = _london_day_frame(["2026-12-02", "2026-12-03"], aware)
    now = datetime(2026, 12, 4, 0, 0, tzinfo=timezone.utc)  # 00:00 GMT on 12-04
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=FX_SESSION), df)


def test_fx_bar_is_still_open_after_the_new_york_rollover():
    """The window the 17:00 New York rule leaked: 18:00 EDT is 23:00 BST.

    The old rule kept this bar from 21:00 UTC; it has an hour left to run.
    """
    now = datetime(2026, 8, 19, 22, 0, tzinfo=timezone.utc)
    df = _frame(["2026-08-18", "2026-08-19"])
    out = drop_unsettled_session(df, now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-18"]


def test_session_rule_rejects_a_negative_close_day_offset():
    with pytest.raises(ValueError, match="close_day_offset"):
        SessionRule(name="bad", timezone="UTC", close_local=(0, 0), close_day_offset=-1)


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
    """FX reopens Sunday evening; the London-day bar it feeds is labelled Monday."""
    now = datetime(2026, 8, 24, 1, 0, tzinfo=timezone.utc)  # 02:00 BST Monday
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


def test_the_guard_sits_in_fetch_one_not_in_the_download(monkeypatch):
    """The split for the trusted path must not move the guard off the v1 path.

    ``download_bars`` exists so a trusted adapter can see the unfinished bar
    and apply — and record — its own policy. That is only safe while every v1
    caller still goes through ``fetch_one``, which guards.
    """
    import fetchers.yfinance as yf_module

    frame = pd.DataFrame(
        {"Open": [1.0, 2.0], "High": [1.0, 2.0], "Low": [1.0, 2.0], "Close": [1.0, 2.0]},
        index=pd.DatetimeIndex([pd.Timestamp("2026-08-12"), pd.Timestamp("2026-08-13")]),
    )
    monkeypatch.setattr(
        yf_module, "download_bars", lambda ticker, period=None, max_retries=3: frame
    )
    monkeypatch.setattr(
        yf_module,
        "drop_unsettled_session",
        lambda df, label="", now=None, rule=EXCHANGE_SESSION: df.iloc[:-1],
    )

    assert len(yf_module.download_bars("ZS=F", period="5d")) == 2
    assert len(yf_module.fetch_one("ZS=F", period="5d")) == 1


# B26 (#496): at weekends Yahoo appends a *live row* stamped at the last tick
# (BRLUSD=X 2026-10-17T09:58:16Z, ZARUSD=X 2026-10-18T04:21:18Z — chart API,
# measured 2026-10-18), and yfinance floors it to that tick's London date, so
# it arrives looking exactly like a 00:00-London bar labelled Saturday or
# Sunday. Two years of daily bars for all ten configured pairs (519 rows
# each) carry no weekend label apart from that live row.


def test_saturday_live_fx_row_is_dropped_on_sunday():
    """Ticket fixture: by Sunday the clock counts Saturday as a finished
    London day, so only the weekday rule stands between the single tick and
    the store."""
    now = datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)  # Sunday 09:00 BST
    df = _frame(["2026-10-15", "2026-10-16", "2026-10-17"])
    df = df.astype({"Open": float, "High": float, "Low": float, "Close": float})
    df.loc[pd.Timestamp("2026-10-17"), ["Open", "High", "Low", "Close"]] = 0.197266
    out = drop_unsettled_session(df, label="BRLUSD=X", now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-10-15", "2026-10-16"]


@pytest.mark.parametrize("aware", [False, True], ids=["naive", "london-aware"])
def test_friday_london_day_bar_passes_the_weekday_rule(aware):
    df = _london_day_frame(["2026-10-15", "2026-10-16"], aware)
    now = datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=FX_SESSION), df)


def test_sunday_live_fx_row_is_dropped_once_sunday_has_ended():
    """The same row on Monday morning is past the London-day clock too."""
    now = datetime(2026, 10, 19, 6, 0, tzinfo=timezone.utc)  # Monday 07:00 BST
    df = _frame(["2026-10-16", "2026-10-18"])
    out = drop_unsettled_session(df, label="ZARUSD=X", now=now, rule=FX_SESSION)
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-10-16"]


def test_weekend_fx_drop_is_logged_with_its_own_reason(caplog):
    now = datetime(2026, 10, 18, 8, 0, tzinfo=timezone.utc)
    df = _frame(["2026-10-16", "2026-10-17"])
    with caplog.at_level(logging.WARNING, logger="fetchers._settlement"):
        drop_unsettled_session(df, label="BRLUSD=X", now=now, rule=FX_SESSION)
    assert "BRLUSD=X" in caplog.text
    assert "2026-10-17" in caplog.text
    assert "not a session day" in caplog.text


def test_exchange_rule_has_no_weekday_filter():
    """Narrow by design: the weekday rule is FX evidence, not a futures one."""
    assert EXCHANGE_SESSION.session_weekdays is None
    assert FX_SESSION.session_weekdays == frozenset({0, 1, 2, 3, 4})


def test_session_rule_rejects_an_impossible_weekday():
    with pytest.raises(ValueError, match="session_weekdays"):
        SessionRule(name="bad", timezone="UTC", close_local=(0, 0), session_weekdays=frozenset({7}))
