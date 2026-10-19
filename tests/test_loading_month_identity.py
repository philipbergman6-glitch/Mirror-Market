"""B39 #540: a windowed venue's prints are compared by loading-month identity.

MAGyP re-cuts each product's curve into a fresh partition every circular and
omits the single-month band in some regimes, so the nearest open band flips
``2026-09`` ↔ ``2026-09..2026-10`` — 58 "rolls" a year on beans, median run 2
sessions (replay of ``data/history/argentina_fob.csv``, A20 #536). The owner's
rule: the comparison key is the **start month** of the nearest open band. Band
selection is unchanged; a band change inside a loading month is like-for-like
(the change shows, with a note); a change of loading month is a roll (the
change is withheld with its reason, never bridged).

The fixture is the stored bean circulars of 18–30 Sep 2026, verbatim: nearest
bands ``09 → 09..11 ×3 → 09..10 → 09 → 09..10 → 09 → 10``. One roll.
"""

from __future__ import annotations

import inspect
import re
import sqlite3
from datetime import date

import pytest
from jinja2 import Environment, FileSystemLoader

from analysis.origins.domain import LoadingMonth, ShipmentWindow
from app import block_builders
from app.block_builders import SiteContext, basis_block, ledger_block, price_block
from app.markets import load_markets
from pipeline import schema

SEP, OCT = LoadingMonth(2026, 9), LoadingMonth(2026, 10)
TEMPLATES = block_builders.__file__.rsplit("/", 1)[0] + "/templates"

# data/history/argentina_fob.csv, Soybeans 12019000190C, 2026-09-18 → 09-30.
CIRCULARS: dict[date, tuple[tuple[str, str, float], ...]] = {
    date(2026, 9, 18): (("2026-09", "2026-09", 504.0), ("2026-10", "2026-10", 505.0),
                        ("2026-11", "2027-08", 506.0)),
    date(2026, 9, 21): (("2026-09", "2026-11", 514.0), ("2026-12", "2027-08", 509.0)),
    date(2026, 9, 22): (("2026-09", "2026-11", 514.0), ("2026-12", "2027-08", 508.0)),
    date(2026, 9, 23): (("2026-09", "2026-11", 510.0), ("2026-12", "2027-08", 506.0)),
    date(2026, 9, 24): (("2026-09", "2026-10", 510.0), ("2026-11", "2027-08", 511.0)),
    date(2026, 9, 25): (("2026-09", "2026-09", 511.0), ("2026-10", "2027-08", 512.0)),
    date(2026, 9, 28): (("2026-09", "2026-10", 501.0), ("2026-11", "2027-08", 502.0)),
    date(2026, 9, 29): (("2026-09", "2026-09", 505.0), ("2026-10", "2026-10", 504.0),
                        ("2026-11", "2027-08", 506.0)),
    date(2026, 9, 30): (("2026-10", "2026-10", 503.0), ("2026-11", "2027-09", 504.0)),
}
SESSIONS = sorted(CIRCULARS)
NEAREST = ["2026-09", "2026-09..2026-11", "2026-09..2026-11", "2026-09..2026-11",
           "2026-09..2026-10", "2026-09", "2026-09..2026-10", "2026-09", "2026-10"]
BAND_CHANGES = (date(2026, 9, 21), date(2026, 9, 24), date(2026, 9, 25),
                date(2026, 9, 28), date(2026, 9, 29))
ROLL = date(2026, 9, 30)


def _connect(last: date) -> sqlite3.Connection:
    """The circulars up to and including ``last`` — the page as it read that day."""
    conn = sqlite3.connect(":memory:")
    for ddl in (schema._CREATE_PRICES, schema._CREATE_CURRENCIES, schema._CREATE_ARGENTINA_FOB):
        conn.execute(ddl)
    for day in SESSIONS:
        if day > last:
            continue
        conn.execute(
            "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
            ("Soybeans", day.isoformat(), 1050.0, 15000.0),
        )
        for ship_from, ship_to, price in CIRCULARS[day]:
            conn.execute(
                "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
                "price_usd_mt) VALUES (?,?,?,?,?,?)",
                (day.isoformat(), "Soybeans", "12019000190C", ship_from, ship_to, price),
            )
    conn.commit()
    return conn


@pytest.fixture
def registry():
    return load_markets()


def _windows(registry, last: date = ROLL):
    ctx = SiteContext(conn=_connect(last), today=last)
    return ctx, ctx.session_windows(registry["argentina"].price, "Soybeans")


def _render(template: str, data: dict) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=True)
    block = type("B", (), {"data": data})()
    return env.get_template(template).render(root="../", block=block, d=data)


def _ledger_fob(registry, last: date) -> dict:
    state, reason, data = ledger_block(
        registry["argentina"], SiteContext(conn=_connect(last), today=last), markets=registry
    )
    assert state == "ok", reason
    return next(row for row in data["rows"] if row["leg_id"] == "argentina:fob")


# ---------------------------------------------------------------------------
# The selector
# ---------------------------------------------------------------------------
def test_band_selection_is_unchanged_nearest_open(registry):
    _, windows = _windows(registry)
    assert [windows[day].chosen.label for day in SESSIONS] == NEAREST


def test_the_september_bean_sequence_is_one_roll(registry):
    _, windows = _windows(registry)
    span = block_builders._window_span(windows, SESSIONS)
    assert span is not None
    assert span.months == (SEP, OCT)
    assert span.rolls == ((ROLL, SEP, OCT),)
    assert span.band_changes == BAND_CHANGES
    assert span.describe() == (
        "spans 2 loading months, rolled once — last on 30 Sep (Sep → Oct); "
        "Oct loading · 1 session, 0 band changes"
    )


def test_a_loading_month_names_its_run_and_band_changes(registry):
    _, windows = _windows(registry)
    span = block_builders._window_span(windows, SESSIONS[:-1])
    assert span is not None and span.rolls == ()
    assert span.describe() == "Sep loading · 8 sessions, 5 band changes"


def test_a_run_back_to_an_earlier_month_is_named_by_the_newest_session(registry):
    """Sep → Oct → Sep: two rolls, and the run is September's, not October's."""
    _, windows = _windows(registry)
    back = date(2026, 10, 1)
    windows = {**windows, back: windows[date(2026, 9, 29)]}
    span = block_builders._window_span(windows, [*SESSIONS, back])
    assert span is not None
    assert span.rolls[-1] == (back, OCT, SEP)
    assert span.current == SEP
    assert span.describe().endswith("Sep loading · 1 session, 0 band changes")


def test_identity_is_the_start_month_of_the_chosen_band():
    """Venue-neutral: AMS's Current slot keys on its report month too."""
    magyp = ShipmentWindow(date(2026, 9, 1), date(2026, 11, 30), "2026-09..2026-11")
    ams = ShipmentWindow(date(2026, 9, 29), date(2026, 9, 30), "Current (prompt)")
    assert magyp.loading_month == ams.loading_month == SEP


def test_the_one_selector_never_branches_on_the_market():
    """No ``if market ==`` anywhere on the path (CLAUDE.md invariant 5)."""
    for function in (
        block_builders.SessionWindows,
        block_builders._same_identity,
        block_builders._band_note,
        block_builders._window_span,
        block_builders._current_window_run,
        block_builders._roll_breaks,
        block_builders._band_ticks,
    ):
        source = inspect.getsource(function)
        assert not re.search(r"\b(market|slug)\s*(==|!=|in\b)|\.slug\b", source), function


def test_a_month_number_out_of_range_is_refused():
    with pytest.raises(ValueError, match="calendar month"):
        LoadingMonth(2026, 13)
    with pytest.raises(ValueError, match="calendar month"):
        ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), "2026-13").band_name()


@pytest.mark.parametrize(
    ("window", "label"),
    [
        (ShipmentWindow(date(2026, 9, 1), date(2026, 10, 31), "2026-09..2026-10"),
         "Sep loading (band Sep–Oct 2026)"),
        (ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), "2026-09"),
         "Sep loading (Sep 2026)"),
        (ShipmentWindow(date(2026, 12, 1), date(2027, 1, 31), "2026-12..2027-01"),
         "Dec loading (band Dec 2026–Jan 2027)"),
        (ShipmentWindow(date(2026, 9, 29), date(2026, 9, 30), "Current (prompt)"),
         "Sep loading (Current (prompt))"),
    ],
)
def test_the_headline_label(window, label):
    assert window.loading_label() == label


# ---------------------------------------------------------------------------
# Moves: a band change inside the month is shown; a roll is withheld
# ---------------------------------------------------------------------------
def test_a_within_month_band_change_shows_the_change_and_the_note(registry):
    """29 Sep prints the single Sep band, 28 Sep printed Sep–Oct: like-for-like."""
    day = date(2026, 9, 29)
    state, reason, data = price_block(
        registry["argentina"], SiteContext(conn=_connect(day), today=day), markets=registry
    )
    assert state == "ok", reason
    headline = data["headline"]
    assert headline["loading"] == "Sep loading (Sep 2026)"
    assert headline["chg_1_pct"] == pytest.approx((505.0 - 501.0) / 501.0 * 100.0)
    note = "band Sep–Oct → Sep, same loading month"
    assert f"session move: {note}" in headline["move_notes"]
    html = _render("blocks/01_price.html.j2", data)
    assert note in html and "Sep loading (Sep 2026)" in html

    row = _ledger_fob(registry, day)
    assert row["usd_chg_pct"] == pytest.approx((505.0 - 501.0) / 501.0 * 100.0)
    assert row["move_note"] == note
    assert row["loading"] == "Sep loading (Sep 2026)"


def test_a_multi_month_band_names_itself_on_the_headline(registry):
    day = date(2026, 9, 28)
    row = _ledger_fob(registry, day)
    assert row["loading"] == "Sep loading (band Sep–Oct 2026)"
    assert row["move_note"] == "band Sep → Sep–Oct, same loading month"


def test_a_loading_month_roll_withholds_the_change_with_the_reason(registry):
    state, reason, data = price_block(
        registry["argentina"], SiteContext(conn=_connect(ROLL), today=ROLL), markets=registry
    )
    assert state == "ok", reason
    headline = data["headline"]
    reason_text = "loading month rolled Sep → Oct — no like-for-like change"
    assert headline["chg_1_pct"] is None and headline["chg_5_pct"] is None
    assert f"session move withheld: {reason_text}" in headline["move_notes"]
    assert f"5-obs move withheld: {reason_text}" in headline["move_notes"]
    assert reason_text in _render("blocks/01_price.html.j2", data)

    row = _ledger_fob(registry, ROLL)
    assert row["usd_chg_pct"] is None
    assert row["move_note"] == f"move withheld: {reason_text}"


def test_no_session_inside_september_withholds_a_move(registry):
    """Zero withheld changes on within-identity band changes (acceptance)."""
    for day in SESSIONS[1:-1]:
        row = _ledger_fob(registry, day)
        assert row["usd_chg_pct"] is not None, day
        assert "withheld" not in (row["move_note"] or ""), day


# ---------------------------------------------------------------------------
# Signals, ranges, drill-down, basis
# ---------------------------------------------------------------------------
def test_signals_read_the_whole_loading_month_across_band_changes(registry, monkeypatch):
    seen: list = []
    monkeypatch.setattr(block_builders, "_signal_chips", lambda rows, key: seen.append(rows) or [])
    day = date(2026, 9, 29)
    _, _, data = price_block(
        registry["argentina"], SiteContext(conn=_connect(day), today=day), markets=registry
    )
    assert [d for d, _, _ in seen[0]] == SESSIONS[:-1]
    assert data["signals_note"] == "signals read on Sep loading alone, since 18 Sep"


def test_signals_after_the_roll_are_withheld_with_the_roll_named(registry):
    _, _, data = price_block(
        registry["argentina"], SiteContext(conn=_connect(ROLL), today=ROLL), markets=registry
    )
    assert data["signals_note"].startswith(
        "signals withheld — loading month rolled Sep → Oct on 30 Sep, and Oct loading has 1 session"
    )


def test_the_drilldown_breaks_only_at_the_roll_and_ticks_each_band_change(registry):
    drill = _ledger_fob(registry, ROLL)["drill"]
    origin = date.fromisoformat(drill["origin"])
    assert drill["breaks"] == [(ROLL - origin).days]
    assert drill["ticks"] == [(day - origin).days for day in BAND_CHANGES]
    note = next(w for w in drill["windows"] if w["label"] == "30d")["roll_note"]
    assert "spans 2 loading months" in note
    assert "band changes inside a loading month are ticks on the line, not breaks" in note


def test_a_drill_window_inside_one_month_still_names_its_band_changes(registry):
    drill = _ledger_fob(registry, date(2026, 9, 29))["drill"]
    assert drill["breaks"] == []
    note = next(w for w in drill["windows"] if w["label"] == "30d")["roll_note"]
    assert note == (
        "Sep loading · 8 sessions, 5 band changes; band changes inside a loading "
        "month are ticks on the line, not breaks"
    )


def test_the_range_band_inside_one_month_is_labelled_not_counted(registry):
    band = _ledger_fob(registry, date(2026, 9, 29))["band"]
    assert band["has_range"]
    assert band["loading_months"] is None
    assert band["window_note"] == "Sep loading · 8 sessions, 5 band changes"


def test_the_basis_history_names_its_loading_month(registry):
    day = date(2026, 9, 29)
    state, reason, data = basis_block(
        registry["argentina"], SiteContext(conn=_connect(day), today=day), markets=registry
    )
    assert state == "ok", reason
    assert data["history_windows"] == "Sep loading · 8 sessions, 5 band changes"
    assert data["loading"] == "Sep loading (Sep 2026)"
    assert "Sep loading (Sep 2026)" in _render("blocks/04_basis.html.j2", data)
