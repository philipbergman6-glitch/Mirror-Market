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


SOY = ("Soybeans", "Soybean Oil", "Soybean Meal")


def _stored(prices: str | None = "2026-11-24", curve: str | None = "2026-11-24", **override):
    out = {"prices": dict.fromkeys(SOY, prices), "forward_curve": dict.fromkeys(SOY, curve)}
    for key, value in override.items():
        layer, _, commodity = key.partition("__")
        out[layer][commodity.replace("_", " ")] = value
    return out


MEASURED = measure_from_rows(_rows(prices="2026-11-24", forward_curve="2026-11-24"))


def test_the_board_session_is_the_soy_legs_stored_session():
    assert board_session(_stored(), MEASURED) == date(2026, 11, 24)


def test_the_commodities_are_the_soy_complex():
    from latency.domain import CBOT_BOARD_COMMODITIES

    assert CBOT_BOARD_COMMODITIES == SOY


def test_a_curve_behind_the_front_month_names_the_older_session():
    assert board_session(_stored(curve="2026-11-23"), MEASURED) == date(2026, 11, 23)


def test_one_soy_leg_behind_names_its_session_not_the_layer_max():
    """The layer's observed_at is the max over all tickers (cattle, sugar…):
    soybeans stuck at Fri 20 Nov must not read as Mon 23 Nov."""
    stored = _stored(prices="2026-11-23", curve="2026-11-23", prices__Soybeans="2026-11-20")
    assert board_session(stored, MEASURED) == date(2026, 11, 20)


def test_a_missing_soy_leg_withholds_the_session():
    assert board_session(_stored(forward_curve__Soybean_Meal=None), MEASURED) is None
    assert board_session({}, MEASURED) is None


def test_a_board_layer_unobserved_this_run_withholds_the_session():
    """A failed curve fetch: observed_at NULL. Dropping the leg would name the
    prices session while the page serves an older curve."""
    measured = measure_from_rows(_rows(prices="2026-11-24"))  # curve: not-run
    assert board_session(_stored(), measured) is None


def test_read_board_sessions_reads_each_soy_leg_from_the_store(tmp_path, monkeypatch):
    import sqlite3

    import pipeline.query as query

    db = tmp_path / "t.db"
    conn = sqlite3.connect(db)
    conn.executescript("""
        CREATE TABLE prices (commodity TEXT, Date TEXT, Close REAL);
        CREATE TABLE forward_curve (commodity TEXT, contract_month TEXT, observation_date TEXT,
                                    fetched_date TEXT);
        INSERT INTO prices VALUES ('Soybeans','2026-11-20',1),('Soybeans','2026-11-24',1),
                                  ('Live Cattle','2026-11-25',1);
        INSERT INTO forward_curve VALUES ('Soybeans','2027-01','2026-11-23','2026-11-24'),
                                         ('Soybeans','2027-03','2026-11-23','2026-11-24'),
                                         ('Soybeans','2027-01','2026-11-20','2026-11-21');
    """)
    conn.commit()
    conn.close()
    monkeypatch.setattr(query, "DB_PATH", str(db))
    monkeypatch.setattr(query, "get_connection", lambda: sqlite3.connect(db))
    got = query.read_board_sessions(SOY)
    assert got["prices"] == {"Soybeans": "2026-11-24", "Soybean Oil": None, "Soybean Meal": None}
    assert got["forward_curve"]["Soybeans"] == "2026-11-23"


def test_the_session_is_read_through_the_clock_not_the_utc_date():
    spec = LAYER_LATENCY_BY_KEY["prices"]
    instant = spec.clock.instant(date(2026, 11, 24))
    assert instant is not None and spec.clock.session_of(instant) == date(2026, 11, 24)


def test_the_masthead_carries_age_and_session(monkeypatch):
    monkeypatch.setattr("latency.measure.measure", lambda **_: measure_from_rows(
        _rows(prices="2026-11-24", forward_curve="2026-11-24", currencies="2026-11-24")))
    monkeypatch.setattr("pipeline.query.read_board_sessions", lambda c: _stored())
    masthead = generate_html._build_masthead([], NOW, health={"issues": []})
    assert masthead["board_session"] == "Tue 24 Nov"
    assert masthead["price_age"]


def test_an_unreadable_store_withholds_only_the_session(monkeypatch):
    def boom(_):
        raise FileNotFoundError("db")

    monkeypatch.setattr("latency.measure.measure", lambda **_: MEASURED)
    monkeypatch.setattr("pipeline.query.read_board_sessions", boom)
    masthead = generate_html._build_masthead([], NOW, health={"issues": []})
    assert masthead["board_session"] is None and masthead["price_age"]


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
