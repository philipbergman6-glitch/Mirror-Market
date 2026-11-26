"""B48 (#626): Yahoo's ICE US softs bar labelled D is final only at 00:00
New York ending the next weekday after D.

The B47 (#622) rule — final at 00:00 New York on D+1 — is measured
sufficient for CBOT and CME and necessary but not sufficient for ICE US
cotton (``CT``) and sugar (``SB``). Measured over the 90 committed snapshots
of ``data/history/contract_bars.csv`` (2026-08-23 -> 2026-11-25), counting a
value as *served* by a run only when that run re-saved the row (its
``fetched_date`` is the commit's UTC date — ``save_contract_bars`` stamps
every row it writes):

* 24 ICE values were still stored after New York midnight and later changed.
* 5 of them (CTZ26 on 10-09; CTZ26/H27/K27/N27 on 10-14) were never served
  after it: Yahoo left the bar out of the next runs' frames, so the stored
  pre-midnight row (served 19:12 / 18:20 New York on the label day) was
  carried. The B47 rule already drops those.
* 19, in three sessions, were served after New York midnight and revised
  later. The latest such serve is 13:47 New York on the next weekday
  (10-06, for the 10-05 bar); every value served after 00:00 New York
  ending the next weekday — 1,011 (contract, session) pairs — equals the
  value stored today.

The clock is injected everywhere; these must not depend on when they run.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pandas.testing as pdt
import pytest

from fetchers._settlement import (
    EXCHANGE_SESSION,
    ICE_US_SESSION,
    drop_unsettled_session,
    exchange_rule_for,
)

UTC = timezone.utc

# (ticker, session, value served, value it was later replaced with, the UTC
# commit time of the latest run that served the first value). Commit time is
# an upper bound on the fetch time, so it is the conservative instant to test.
SERVED_AFTER_NY_MIDNIGHT = [
    # Fri 2026-08-21: served Sun 08-23 13:51 EDT, replaced by Mon 08-24 07:49 EDT.
    *[
        (ticker, "2026-08-21", old, new, datetime(2026, 8, 23, 17, 51, 52, tzinfo=UTC))
        for ticker, old, new in [
            ("CTH27.NYB", 90.06, 90.15),
            ("CTK27.NYB", 91.15, 91.19),
            ("CTN27.NYB", 90.32, 90.38),
            ("CTV26.NYB", 87.12, 87.07),
            ("CTZ26.NYB", 88.28, 88.35),
            ("SBH28.NYB", 18.24, 18.27),
            ("SBK27.NYB", 18.07, 18.10),
            ("SBN27.NYB", 17.79, 17.81),
            ("SBV26.NYB", 17.60, 17.61),
            ("SBV27.NYB", 17.88, 17.90),
        ]
    ],
    # Mon 2026-10-05: served Tue 10-06 13:47 EDT, replaced by 14:36 EDT — the
    # latest post-midnight serve of a revised value in the record.
    *[
        (ticker, "2026-10-05", old, new, datetime(2026, 10, 6, 17, 47, 43, tzinfo=UTC))
        for ticker, old, new in [
            ("CTH27.NYB", 83.74, 83.71),
            ("CTK27.NYB", 85.32, 85.27),
            ("CTN27.NYB", 85.80, 85.71),
            ("CTV27.NYB", 78.30, 79.10),
        ]
    ],
    # Fri 2026-10-16: served Sun 10-18 05:29 EDT, replaced by Mon 10-19 07:44 EDT.
    *[
        (ticker, "2026-10-16", old, new, datetime(2026, 10, 18, 9, 29, 56, tzinfo=UTC))
        for ticker, old, new in [
            ("CTN27.NYB", 85.42, 85.41),
            ("CTV27.NYB", 79.45, 79.31),
            ("SBH28.NYB", 19.63, 19.66),
            ("SBN27.NYB", 19.53, 19.54),
            ("SBV27.NYB", 19.39, 19.40),
        ]
    ],
]

# Stored after New York midnight but last *served* before it (see module
# docstring): the pre-midnight run is the one that wrote the value.
CARRIED_FROM_BEFORE_NY_MIDNIGHT = [
    # Fri 10-09, served 19:12 EDT that evening; carried in the Mon 10-12
    # 06:04 EDT snapshot (fetched_date still 2026-10-09), replaced 06:44 EDT.
    ("CTZ26.NYB", "2026-10-09", 80.39, 80.54, datetime(2026, 10, 9, 23, 12, 32, tzinfo=UTC)),
    # Wed 10-14, served 18:20 EDT that evening; carried through Thu 10-15
    # 19:32 EDT (fetched_date still 2026-10-14), replaced by Fri 10-16 18:07 EDT.
    *[
        (ticker, "2026-10-14", old, new, datetime(2026, 10, 14, 22, 20, 17, tzinfo=UTC))
        for ticker, old, new in [
            ("CTH27.NYB", 83.98, 84.00),
            ("CTK27.NYB", 85.67, 85.69),
            ("CTN27.NYB", 86.11, 86.13),
            ("CTZ26.NYB", 80.65, 80.66),
        ]
    ],
]

ALL_24 = SERVED_AFTER_NY_MIDNIGHT + CARRIED_FROM_BEFORE_NY_MIDNIGHT


def _bars(session: str, close: float) -> pd.DataFrame:
    """The prior session plus the session under test, closing at ``close``."""
    idx = pd.DatetimeIndex(pd.to_datetime(["2026-08-03", session]), name="Date")
    return pd.DataFrame(
        {"Open": [1.0, close], "High": [1.0, close], "Low": [1.0, close], "Close": [1.0, close], "Volume": [0.0, 0.0]},
        index=idx,
    )


def _ids(cases):
    return [f"{ticker}-{session}" for ticker, session, *_ in cases]


def test_the_record_has_all_24_cases():
    assert len(SERVED_AFTER_NY_MIDNIGHT) == 19
    assert len(CARRIED_FROM_BEFORE_NY_MIDNIGHT) == 5


@pytest.mark.parametrize("ticker,session,served,_final,served_at", ALL_24, ids=_ids(ALL_24))
def test_no_pre_final_ice_value_survives_the_guard(ticker, session, served, _final, served_at):
    """Every one of the 24 values a later run replaced is dropped when served."""
    out = drop_unsettled_session(_bars(session, served), label=ticker, now=served_at, rule=exchange_rule_for(ticker))
    assert list(out.index.strftime("%Y-%m-%d")) == ["2026-08-03"]


@pytest.mark.parametrize(
    "ticker,session,served,_final,served_at", SERVED_AFTER_NY_MIDNIGHT, ids=_ids(SERVED_AFTER_NY_MIDNIGHT)
)
def test_the_new_york_midnight_rule_alone_kept_19_of_them(ticker, session, served, _final, served_at):
    """The defect, pinned: B47's rule passes these, so ICE needs its own."""
    df = _bars(session, served)
    pdt.assert_frame_equal(drop_unsettled_session(df, now=served_at, rule=EXCHANGE_SESSION), df)


# The cutoff, per label weekday: 00:00 New York ending the next weekday.
@pytest.mark.parametrize(
    "session,final_utc",
    [
        ("2026-10-05", datetime(2026, 10, 7, 4, 0, tzinfo=UTC)),  # Mon -> Wed 00:00 EDT
        ("2026-10-08", datetime(2026, 10, 10, 4, 0, tzinfo=UTC)),  # Thu -> Sat 00:00 EDT
        ("2026-10-16", datetime(2026, 10, 20, 4, 0, tzinfo=UTC)),  # Fri -> Tue 00:00 EDT
        ("2026-11-20", datetime(2026, 11, 24, 5, 0, tzinfo=UTC)),  # Fri -> Tue 00:00 EST
    ],
    ids=["mon", "thu", "fri-edt", "fri-est"],
)
def test_an_ice_bar_is_final_at_new_york_midnight_ending_the_next_weekday(session, final_utc):
    df = _bars(session, 80.0)
    just_before = final_utc - pd.Timedelta(minutes=1)
    assert ICE_US_SESSION.final_at(pd.Timestamp(session).date()) == final_utc
    kept = drop_unsettled_session(df, label="CTZ26.NYB", now=just_before, rule=ICE_US_SESSION)
    assert list(kept.index.strftime("%Y-%m-%d")) == ["2026-08-03"]
    pdt.assert_frame_equal(drop_unsettled_session(df, label="CTZ26.NYB", now=final_utc, rule=ICE_US_SESSION), df)


def test_a_cbot_bar_is_unaffected():
    """The ICE rule is ICE's alone: a CBOT Friday bar is still final Sat 00:00."""
    df = _bars("2026-10-16", 1000.0)
    now = datetime(2026, 10, 17, 4, 1, tzinfo=UTC)  # 00:01 EDT Saturday
    pdt.assert_frame_equal(drop_unsettled_session(df, now=now, rule=exchange_rule_for("ZSX26.CBT")), df)


@pytest.mark.parametrize(
    "ticker,rule",
    [
        ("CT=F", ICE_US_SESSION),
        ("SB=F", ICE_US_SESSION),
        ("CTZ26.NYB", ICE_US_SESSION),
        ("SBH28.NYB", ICE_US_SESSION),
        ("ZS=F", EXCHANGE_SESSION),
        ("ZLF27.CBT", EXCHANGE_SESSION),
        ("LE=F", EXCHANGE_SESSION),
        ("HEZ26.CME", EXCHANGE_SESSION),
        ("CPO=F", EXCHANGE_SESSION),
    ],
)
def test_each_futures_ticker_is_matched_to_its_venue_rule(ticker, rule):
    assert exchange_rule_for(ticker) is rule


@pytest.mark.parametrize("ticker", ["CT=F", "CTZ26.NYB", "ZS=F"])
def test_fetch_one_applies_the_tickers_own_venue_rule(monkeypatch, ticker):
    """Layer 1, 11 and 11b all call fetch_one without a rule: it must choose."""
    import fetchers.yfinance as yf_module

    seen: list[object] = []
    monkeypatch.setattr(yf_module, "download_bars", lambda *a, **k: _bars("2026-10-16", 1.0))
    monkeypatch.setattr(
        yf_module, "drop_unsettled_session", lambda df, label="", now=None, rule=None: seen.append(rule) or df
    )
    yf_module.fetch_one(ticker, period="5d")
    assert seen == [exchange_rule_for(ticker)]
