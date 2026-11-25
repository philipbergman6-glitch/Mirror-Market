"""The masthead states the CBOT board's session date beside the data age.

Owner decision on #622 (2026-11-25): an age alone does not tell a buyer which
close they are reading, and an evening edition holds CBOT D-1 by design
(Yahoo finishes the bar only at 00:00 New York on D+1). DESIGN.md, decision
log 2026-11-25.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from bs4 import BeautifulSoup

from latency.domain import LAYER_LATENCY_BY_KEY
from latency.measure import board_session, measure_from_rows
from scripts import generate_html

UTC = timezone.utc
NOW = datetime(2026, 11, 25, 7, 0, tzinfo=UTC)
NAV: list[dict] = []


def _rows(**observed: str) -> dict:
    return {
        layer: {
            "observed_at": day,
            "fetch_started_at": "2026-11-25T06:03:00+00:00",
            "fetch_completed_at": "2026-11-25T06:04:00+00:00",
            "stored_at": "2026-11-25T06:05:00+00:00",
            "status": "success",
        }
        for layer, day in observed.items()
    }


def test_the_board_session_is_the_cbot_bars_own_date():
    ms = measure_from_rows(_rows(prices="2026-11-24", forward_curve="2026-11-24",
                                 dce="2026-11-25"))
    assert board_session(ms) == date(2026, 11, 24)


def test_a_curve_behind_the_front_month_names_the_older_session():
    ms = measure_from_rows(_rows(prices="2026-11-24", forward_curve="2026-11-23"))
    assert board_session(ms) == date(2026, 11, 23)


def test_no_board_observation_states_no_session():
    assert board_session(measure_from_rows(_rows(currencies="2026-11-24"))) is None
    assert board_session(measure_from_rows({})) is None


def test_the_session_is_read_through_the_clock_not_the_utc_date():
    """13:15 CT on D is still D in UTC, but the inverse must go through the
    clock (a close_day_offset clock would otherwise drift a day)."""
    spec = LAYER_LATENCY_BY_KEY["prices"]
    instant = spec.clock.instant(date(2026, 11, 24))
    assert instant is not None and spec.clock.session_of(instant) == date(2026, 11, 24)


def test_the_masthead_carries_age_and_session_from_one_measurement(monkeypatch):
    calls = []

    def fake_measure(**kwargs):
        calls.append(kwargs)
        return measure_from_rows(_rows(prices="2026-11-24", forward_curve="2026-11-24",
                                       currencies="2026-11-24"))

    monkeypatch.setattr("latency.measure.measure", fake_measure)
    masthead = generate_html._build_masthead([], NOW, health={"issues": []})
    assert masthead["board_session"] == "Tue 24 Nov"
    assert masthead["price_age"]  # stated beside it
    assert len(calls) == 1


def test_a_failed_measurement_drops_both_and_never_fails_the_build(monkeypatch):
    def boom(**_):
        raise RuntimeError("db gone")

    monkeypatch.setattr("latency.measure.measure", boom)
    masthead = generate_html._build_masthead([], NOW, health={"issues": []})
    assert masthead["price_age"] is None and masthead["board_session"] is None


def _render(template: str, masthead: dict) -> BeautifulSoup:
    from app.templating import site_environment

    html = site_environment().get_template(template).render(
        sections=generate_html.SECTIONS, generated_at="2026-11-25 06:10 UTC",
        masthead=masthead, freshness_items=[], market_nav=NAV, root="",
        current_page="headline", briefing_text="", briefing_uri="data:text/plain,x",
    )
    return BeautifulSoup(html, "html.parser")


def test_both_masthead_pages_render_the_session_next_to_the_age():
    masthead = {"day_line": "Wednesday · 25 November 2026", "price_age": "10h 55m",
                "board_session": "Tue 24 Nov"}
    for template in ("dashboard.html.j2", "briefing.html.j2"):
        meta = _render(template, masthead).select_one(".mast-meta")
        text = meta.get_text(" ", strip=True)
        assert "priced from data 10h 55m old" in text, template
        session = meta.select_one(".mast-session")
        assert session.get_text(" ", strip=True) == "CBOT session Tue 24 Nov", template
        assert "midnight New York" in session["title"]


def test_a_masthead_without_a_session_renders_no_empty_label():
    meta = _render("dashboard.html.j2", {"price_age": "3h"}).select_one(".mast-meta")
    assert meta.select_one(".mast-session") is None
    assert "priced from data 3h old" in meta.get_text(" ", strip=True)
