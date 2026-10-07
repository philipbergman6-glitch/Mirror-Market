"""Venue holidays and the ``closed`` verdict (#399).

Golden Week 2026 is the scenario: the DCE's last session before the
holiday is Tuesday 30 September; it is shut 1–7 October and reopens
Thursday 8 October. Every clock is injected; the DCE closes 15:00
Asia/Shanghai = 07:00 UTC.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from latency.calendars import KNOWN_VENUES, closure_basis, venue_closures
from latency.domain import LAYER_LATENCY_BY_KEY, ObservationClock, Verdict
from latency.measure import measure_from_rows
from latency.report import measurement_to_dict, to_text

UTC = timezone.utc
DCE = (LAYER_LATENCY_BY_KEY["dce"],)


def _dce(observed_on: date, fetched: datetime, published: datetime | None = None,
         *, publish: bool = True):
    """A dce freshness row fetched at ``fetched`` holding ``observed_on``'s bar.

    Published ten minutes after the fetch unless given, so the pipeline leg
    is measured and MEETS; ``publish=False`` leaves it unmeasured.
    """
    if published is None and publish:
        published = fetched + timedelta(minutes=10)
    row = {
        "status": "success",
        "observed_at": datetime(observed_on.year, observed_on.month, observed_on.day, tzinfo=UTC),
        "fetch_started_at": fetched - timedelta(minutes=2),
        "fetch_completed_at": fetched,
        "stored_at": fetched + timedelta(minutes=1),
        "last_attempt": fetched + timedelta(minutes=1),
    }
    return measure_from_rows({"dce": row}, published_at=published, specs=DCE)[0]


# ---------------------------------------------------------------------------
# The calendar
# ---------------------------------------------------------------------------

def test_dce_2026_calendar_matches_the_exchange_notice():
    closures = venue_closures("dce", 2026)
    assert closures is not None
    # National Day: shut 1–7 Oct, open Thursday 8 Oct.
    assert {date(2026, 10, d) for d in range(1, 8)} <= closures
    assert date(2026, 9, 30) not in closures
    assert date(2026, 10, 8) not in closures
    # Spring Festival 15–23 Feb, reopening Tuesday 24 Feb.
    assert date(2026, 2, 23) in closures and date(2026, 2, 24) not in closures
    assert "DCE notice" in (closure_basis("dce", 2026) or "")


def test_a_year_not_entered_is_unknown_not_empty():
    assert venue_closures("dce", 2027) is None
    assert closure_basis("dce", 2027) is None


def test_an_unknown_venue_raises():
    with pytest.raises(ValueError, match="no closure calendar for venue 'cbot'"):
        venue_closures("cbot", 2026)
    assert KNOWN_VENUES == ("dce",)


def test_only_the_dce_clock_names_a_venue():
    assert LAYER_LATENCY_BY_KEY["dce"].clock.venue == "dce"
    assert LAYER_LATENCY_BY_KEY["prices"].clock.venue is None


# ---------------------------------------------------------------------------
# The clock
# ---------------------------------------------------------------------------

def test_latest_completed_session_walks_back_over_the_holiday():
    clock = ObservationClock("Asia/Shanghai", (15, 0), venue="dce")
    # Wednesday 7 Oct, mid-holiday.
    assert clock.latest_completed_session(datetime(2026, 10, 7, 15, 24, tzinfo=UTC)) == date(2026, 9, 30)
    # Thursday 8 Oct before the 07:00 UTC close: still 30 Sep.
    assert clock.latest_completed_session(datetime(2026, 10, 8, 6, 0, tzinfo=UTC)) == date(2026, 9, 30)
    # Thursday 8 Oct after the close: today.
    assert clock.latest_completed_session(datetime(2026, 10, 8, 7, 0, tzinfo=UTC)) == date(2026, 10, 8)


def test_latest_completed_session_is_unknown_without_a_calendar():
    clock = ObservationClock("Asia/Shanghai", (15, 0), venue="dce")
    assert clock.latest_completed_session(datetime(2027, 10, 4, 10, 30, tzinfo=UTC)) is None
    assert ObservationClock("America/Chicago", (13, 15)).latest_completed_session(
        datetime(2026, 10, 7, 15, 24, tzinfo=UTC)
    ) is None
    assert ObservationClock().is_closure(date(2026, 10, 7)) is None


# ---------------------------------------------------------------------------
# The verdict
# ---------------------------------------------------------------------------

def test_mid_holiday_fetch_holding_the_last_session_is_closed():
    """The run that was red: 7 Oct 15:24 UTC, newest bar 30 Sep."""
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 7, 15, 24, tzinfo=UTC),
             published=datetime(2026, 10, 7, 15, 31, tzinfo=UTC))
    assert m.acquisition_verdict is Verdict.BREACHES   # 7 days against 6 h, as measured
    assert m.pipeline_verdict is Verdict.MEETS
    assert m.venue_closed is True
    assert m.verdict is Verdict.CLOSED
    assert m.closure_basis and m.closure_basis.startswith("DCE notice")


def test_first_holiday_day_is_closed_too():
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 1, 10, 30, tzinfo=UTC))
    assert m.verdict is Verdict.CLOSED


def test_reopening_day_before_the_close_is_still_closed():
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 8, 6, 0, tzinfo=UTC))
    assert m.verdict is Verdict.CLOSED


def test_reopening_day_after_the_close_with_the_old_bar_is_a_real_breach():
    """8 Oct 10:30 UTC: the session closed at 07:00 and we still hold 30 Sep."""
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 8, 10, 30, tzinfo=UTC))
    assert m.venue_closed is False
    assert m.verdict is Verdict.BREACHES
    assert m.closure_basis is None


def test_reopening_day_with_the_new_bar_meets():
    m = _dce(date(2026, 10, 8), datetime(2026, 10, 8, 10, 30, tzinfo=UTC))
    assert m.acquisition == timedelta(hours=3, minutes=30)
    assert m.verdict is Verdict.MEETS


def test_an_ordinary_late_landing_is_not_excused():
    """Tuesday 18 Aug, fetched 23:00 UTC: 16 h after the close, no holiday anywhere."""
    m = _dce(date(2026, 8, 18), datetime(2026, 8, 18, 23, 0, tzinfo=UTC))
    assert m.venue_closed is False
    assert m.verdict is Verdict.BREACHES


def test_a_weekend_alone_is_not_a_closure():
    """Monday 24 Aug 02:00 UTC (before the close) holding Friday's bar."""
    m = _dce(date(2026, 8, 21), datetime(2026, 8, 24, 2, 0, tzinfo=UTC))
    assert m.venue_closed is False
    assert m.verdict is Verdict.BREACHES


def test_an_unmeasured_pipeline_during_the_holiday_stays_unknown():
    """The holiday excuses acquisition only; our own leg must still be shown."""
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 7, 15, 24, tzinfo=UTC), publish=False)
    assert m.venue_closed is True
    assert m.pipeline_verdict is Verdict.UNKNOWN
    assert m.verdict is Verdict.UNKNOWN


def test_a_pipeline_breach_during_the_holiday_is_still_ours():
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 7, 15, 0, tzinfo=UTC),
             published=datetime(2026, 10, 7, 16, 0, tzinfo=UTC))
    assert m.pipeline_verdict is Verdict.BREACHES   # 60 min against 25
    assert m.venue_closed is True
    assert m.verdict is Verdict.BREACHES


def test_a_year_without_a_calendar_falls_back_to_breaches():
    """National Day 2027 with no 2027 notice entered: red, as before, never excused."""
    m = _dce(date(2027, 9, 30), datetime(2027, 10, 4, 10, 30, tzinfo=UTC))
    assert m.venue_closed is False
    assert m.verdict is Verdict.BREACHES


def test_a_venue_without_a_calendar_is_unchanged():
    """CBOT on Christmas: no venue, so a holiday still reads as a breach."""
    row = {
        "status": "success",
        "observed_at": datetime(2026, 12, 24, tzinfo=UTC),
        "fetch_started_at": None,
        "fetch_completed_at": datetime(2026, 12, 25, 22, 0, tzinfo=UTC),
        "stored_at": datetime(2026, 12, 25, 22, 1, tzinfo=UTC),
        "last_attempt": None,
    }
    m = measure_from_rows({"prices": row}, specs=(LAYER_LATENCY_BY_KEY["prices"],))[0]
    assert m.venue_closed is False
    assert m.verdict is Verdict.BREACHES


# ---------------------------------------------------------------------------
# The reports
# ---------------------------------------------------------------------------

def test_reports_carry_the_closed_verdict_with_its_basis():
    m = _dce(date(2026, 9, 30), datetime(2026, 10, 7, 15, 24, tzinfo=UTC),
             published=datetime(2026, 10, 7, 15, 31, tzinfo=UTC))
    now = datetime(2026, 10, 7, 15, 31, tzinfo=UTC)
    as_dict = measurement_to_dict(m, now)
    assert as_dict["verdict"] == "closed"
    assert as_dict["closure_basis"].startswith("DCE notice")

    text = to_text([m], now)
    assert "shut" in text
    assert "1 venue closed" in text
    assert "venue closed: dce — DCE notice" in text


def test_a_breaching_report_has_no_basis():
    m = _dce(date(2026, 8, 18), datetime(2026, 8, 18, 23, 0, tzinfo=UTC))
    assert measurement_to_dict(m, datetime(2026, 8, 19, tzinfo=UTC))["closure_basis"] is None


# ---------------------------------------------------------------------------
# The CI gate
# ---------------------------------------------------------------------------

def test_the_scoped_gate_passes_a_closed_venue_and_says_so(tmp_path, capsys):
    """The 7 Oct run, replayed: gated on dce, dce closed → exit 0, line printed."""
    import json

    from scripts.latency_report import main as report_main

    manifest = {
        "schema_version": 1,
        "edition": {"mode": "fast", "generated_at": "2026-10-07T15:24:11+00:00"},
        "latency": {"layers": [
            {"layer": "dce", "class": "board_price", "verdict": "closed",
             "closure_basis": "DCE notice of 2025-12-17 …"},
            {"layer": "cot", "class": "fundamentals", "verdict": "breaches"},
        ]},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")

    assert report_main(["--manifest", str(path), "--fail-on-breach-layers", "dce"]) == 0
    out = capsys.readouterr().out
    assert "venue closed: dce — DCE notice" in out
    # A real breach elsewhere still fails the unscoped flag.
    assert report_main(["--manifest", str(path), "--fail-on-breach"]) == 1
