"""#513: a statistic over a windowed series says which windows it spans.

After B30 #504 each session's print is its nearest shipment window, and a move
across a window roll is withheld. Three history consumers still read that
series straight across the rolls: block 04's mean and range, block 01's signal
chips, and the ledger's drill-down chart and range band. A roll is a
calendar-spread step — Oct cargo to Nov cargo — not a market move.

Measured (``data/history`` CSVs, 2026-10-19): MAGyP's exact nearest band holds
at most 21 sessions in a row, median 2, because the circular flips between a
single-month and a multi-month band; AMS's Current slot holds at most 22. B39
#540 (A20 #536) therefore compares by **loading month** — a band change inside
a month is like-for-like — and the *ranges* label the loading months they span
while the chart breaks its line only at loading-month rolls. The signal chips
are the other rule: a crossover a roll manufactured is not fixed by a label, so
chips read the current loading month only and are withheld until it holds
enough sessions — A4 #301's suppression, for windows.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pytest
from jinja2 import Environment, FileSystemLoader

from analysis.origins.domain import LoadingMonth
from app import block_builders
from app.block_builders import SiteContext, basis_block, ledger_block, price_block
from app.markets import load_markets
from pipeline import schema
from pipeline.units import to_metric_tons

TODAY = date(2026, 10, 16)
ROLL = date(2026, 10, 1)
SEP, OCT = LoadingMonth(2026, 9), LoadingMonth(2026, 10)
TEMPLATES = block_builders.__file__.rsplit("/", 1)[0] + "/templates"

# Before the roll the circular's nearest band is September's; from 1 Oct it is
# October's. Flat inside each window, so any movement a statistic reports is
# the roll's calendar step and nothing else.
SEP_BANDS = (("2026-09", "2026-09", 400.0), ("2026-10", "2027-03", 420.0))
OCT_BANDS = (("2026-10", "2026-10", 450.0), ("2026-11", "2027-04", 455.0))
# AMS's Current slot, both months quoted over November (X); ZSX26's own close.
GULF_CURRENT = {9: 11.00, 10: 11.50}
ZSX26 = 1000.0
FRONT_MONTH = 1050.0


def _sessions(start: date, end: date) -> list[date]:
    days, day = [], start
    while day <= end:
        if day.weekday() < 5:
            days.append(day)
        day += timedelta(days=1)
    return days


SESSIONS = _sessions(date(2026, 9, 1), TODAY)


def _usd(cents_per_bu: float) -> float:
    usd = to_metric_tons(cents_per_bu, "Soybeans")
    assert usd is not None
    return usd


def _seed(conn, sessions) -> None:
    for day in sessions:
        conn.execute(
            "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
            ("Soybeans", day.isoformat(), FRONT_MONTH, 15000.0),
        )
        for ship_from, ship_to, price in (SEP_BANDS if day < ROLL else OCT_BANDS):
            conn.execute(
                "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
                "price_usd_mt) VALUES (?,?,?,?,?,?)",
                (day.isoformat(), "Soybeans", "12019000190C", ship_from, ship_to, price),
            )
        conn.execute(
            "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
            "futures_month, futures_month_high) VALUES (?,?,?,?,?,?,?)",
            (day.isoformat(), "Soybeans", "Gulf Coast Ports", "Current",
             GULF_CURRENT[day.month], 11, 11),
        )
        conn.execute(
            "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Close, "
            "fetched_date) VALUES (?,?,?,?,?,?)",
            ("Soybeans", "ZSX26.CBT", "2026-11-01", day.isoformat(), ZSX26, day.isoformat()),
        )
    conn.commit()


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_CONTRACT_BARS,
        schema._CREATE_BRAZIL_SPOT,
    ):
        connection.execute(ddl)
    _seed(connection, SESSIONS)
    yield connection
    connection.close()


@pytest.fixture
def registry():
    return load_markets()


def _ctx(conn) -> SiteContext:
    return SiteContext(conn=conn, today=TODAY)


def _render(template: str, data: dict, **extra) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=True)
    block = type("B", (), {"data": data})()
    return env.get_template(template).render(root="../", block=block, d=data, **extra)


# ---------------------------------------------------------------------------
# The span — which windows a run of sessions is for
# ---------------------------------------------------------------------------
def test_a_span_names_its_windows_and_every_roll(conn, registry):
    source = registry["argentina"].price
    windows = _ctx(conn).session_windows(source, "Soybeans")
    span = block_builders._window_span(windows, SESSIONS)
    assert span is not None
    assert span.months == (SEP, OCT)
    assert span.rolls == ((ROLL, SEP, OCT),)
    assert "spans 2 loading months" in span.describe()
    assert "1 Oct" in span.describe() and "(Sep → Oct)" in span.describe()
    assert span.describe().endswith("Oct loading · 12 sessions, 0 band changes")


def test_a_span_inside_one_window_says_so(conn, registry):
    source = registry["argentina"].price
    windows = _ctx(conn).session_windows(source, "Soybeans")
    span = block_builders._window_span(windows, [d for d in SESSIONS if d >= ROLL])
    assert span is not None and span.rolls == ()
    assert span.describe() == "Oct loading · 12 sessions, 0 band changes"


def test_amss_current_slot_is_keyed_by_its_loading_month(conn, registry):
    """"Current (prompt)" is twelve windows a year; the loading month says which."""
    source = registry["cbot"].basis
    windows = _ctx(conn).session_windows(source, "Soybeans")
    span = block_builders._window_span(windows, SESSIONS)
    assert span is not None
    assert span.months == (SEP, OCT)
    assert span.band_changes == ()


def test_an_unwindowed_series_has_no_span_and_no_run_cut():
    rows = [(d, 1.0, 1) for d in SESSIONS]
    assert block_builders._window_span({}, SESSIONS) is None
    assert block_builders._current_window_run(rows, {}) is rows


# ---------------------------------------------------------------------------
# Block 04 — the mean and range
# ---------------------------------------------------------------------------
def test_argentinas_basis_range_names_the_loading_months_it_spans(conn, registry):
    state, reason, data = basis_block(registry["argentina"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert data["n_obs"] == len(SESSIONS) >= 20
    assert data["avg"] is not None
    assert data["high"] - data["low"] == pytest.approx(50.0)  # the roll's step, all of it
    assert "spans 2 loading months" in data["history_windows"]
    assert "Sep → Oct" in data["history_windows"]
    assert data["history_board"] is None
    html = _render("blocks/04_basis.html.j2", data)
    assert "spans 2 loading months" in html
    assert "calendar-spread step" in html


def test_the_history_building_note_names_its_windows_too(conn, registry):
    for table, column in (("argentina_fob", "date"), ("prices", "Date")):
        conn.execute(f"DELETE FROM {table} WHERE {column} > '2026-09-10'")  # noqa: S608
    conn.commit()
    ctx = SiteContext(conn=conn, today=date(2026, 9, 10))
    state, reason, data = basis_block(registry["argentina"], ctx, markets=registry)
    assert state == "ok", reason
    assert data["avg"] is None
    assert data["history_windows"] == "Sep loading · 8 sessions, 0 band changes"
    assert "Sep loading · 8 sessions" in _render("blocks/04_basis.html.j2", data)


def test_the_gulf_range_is_struck_over_the_named_contract_not_the_front_month(conn, registry):
    """The range is struck by the engine that struck the level (#512)."""
    state, reason, data = basis_block(registry["cbot"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    bases = [_usd(GULF_CURRENT[d.month] * 100) - _usd(ZSX26) for d in SESSIONS]
    assert data["n_obs"] == len(SESSIONS)
    assert data["avg"] == pytest.approx(sum(bases) / len(bases))
    assert data["low"] == pytest.approx(min(bases))
    assert data["high"] == pytest.approx(max(bases))
    assert "(Sep → Oct)" in data["history_windows"]
    html = _render("blocks/04_basis.html.j2", data)
    assert "front month" not in html
    assert "over the contract its window names" in html


def test_a_gulf_session_with_no_named_close_leaves_the_range(conn, registry):
    """Never padded with the front month: the session drops out of the history."""
    conn.execute("DELETE FROM contract_bars WHERE Date = ?", (SESSIONS[0].isoformat(),))
    conn.commit()
    state, reason, data = basis_block(registry["cbot"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert data["n_obs"] == len(SESSIONS) - 1


# ---------------------------------------------------------------------------
# Block 01 — signal chips
# ---------------------------------------------------------------------------
def _capture_chips(monkeypatch) -> list:
    seen: list = []

    def fake(rows, key):
        seen.append(list(rows))
        return []

    monkeypatch.setattr(block_builders, "_signal_chips", fake)
    return seen


def test_chips_read_only_the_current_window_and_say_why_they_are_withheld(
    conn, registry, monkeypatch
):
    seen = _capture_chips(monkeypatch)
    state, reason, data = price_block(registry["argentina"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert seen == []  # never computed across the roll, not computed-then-hidden
    assert data["signals"] == []
    note = data["signals_note"]
    assert "withheld" in note and "Oct loading" in note and "1 Oct" in note
    assert "loading month rolled Sep → Oct" in note
    assert str(block_builders.SIGNAL_MIN_OBS) in note
    assert note in _render("blocks/01_price.html.j2", data)


def test_chips_on_a_window_long_enough_are_computed_on_it_alone(conn, registry, monkeypatch):
    seen = _capture_chips(monkeypatch)
    monkeypatch.setattr(block_builders, "SIGNAL_MIN_OBS", 10)  # October holds 12
    state, reason, data = price_block(registry["argentina"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert [day for day, _, _ in seen[0]] == [d for d in SESSIONS if d >= ROLL]
    assert "Oct loading" in data["signals_note"] and "withheld" not in data["signals_note"]


def test_an_unwindowed_price_feeds_its_whole_series_to_the_chips(conn, registry, monkeypatch):
    seen = _capture_chips(monkeypatch)
    state, reason, data = price_block(registry["cbot"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert [day for day, _, _ in seen[0]] == SESSIONS
    assert data["signals_note"] is None


# ---------------------------------------------------------------------------
# Ledger — the range band and the drill-down
# ---------------------------------------------------------------------------
def _ledger_rows(conn, registry, slug="argentina") -> dict[str, dict]:
    state, reason, data = ledger_block(registry[slug], _ctx(conn), markets=registry)
    assert state == "ok", reason
    return {row["leg_id"]: row for row in data["rows"]}


def test_the_range_band_counts_the_loading_months_it_spans(conn, registry):
    band = _ledger_rows(conn, registry)["argentina:fob"]["band"]
    assert band["has_range"]
    assert (band["low"], band["high"]) == (400.0, 450.0)
    assert band["loading_months"] == 2
    assert "Sep → Oct" in band["window_note"]


def test_the_drilldown_breaks_its_line_at_every_roll(conn, registry):
    drill = _ledger_rows(conn, registry)["argentina:fob"]["drill"]
    origin = date.fromisoformat(drill["origin"])
    assert drill["breaks"] == [(ROLL - origin).days]
    assert drill["ticks"] == []
    by_label = {w["label"]: w for w in drill["windows"]}
    assert "spans 2 loading months" in by_label["90d"]["roll_note"]
    # 16 Sep → 16 Oct still contains the 1 Oct roll.
    assert "spans 2 loading months" in by_label["30d"]["roll_note"]


def test_a_drill_window_inside_one_window_carries_no_roll_note(conn, registry):
    conn.execute("DELETE FROM argentina_fob WHERE date < ?", (ROLL.isoformat(),))
    conn.commit()
    drill = _ledger_rows(conn, registry)["argentina:fob"]["drill"]
    assert drill["breaks"] == []
    assert all(w["roll_note"] is None for w in drill["windows"])


def test_the_pinned_overlay_breaks_at_its_own_rolls(conn, registry):
    state, reason, data = ledger_block(registry["argentina"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    reference = data["reference"]
    assert reference is not None and reference["leg_id"] == "argentina:fob"
    origin = date.fromisoformat(reference["origin"])
    assert reference["breaks"] == [(ROLL - origin).days]


# ---------------------------------------------------------------------------
# Every unwindowed leg is byte-for-byte what it was
# ---------------------------------------------------------------------------
PRE_513_DRILL_KEYS = {
    "leg_id", "label", "origin", "d", "usd", "home_unit", "has_home_quote", "fx_pair",
    "single_currency_note", "history_from", "history_from_label", "windows", "default_days",
}
PRE_513_DRILL_WINDOW_KEYS = {
    "days", "label", "count", "treatment", "starts", "starts_label", "truncated",
    "truncation_note", "withheld_note",
}
PRE_513_REFERENCE_KEYS = {"leg_id", "label", "origin", "d", "usd", "truncated", "truncation_note"}


def test_an_unwindowed_legs_drill_payload_has_exactly_its_old_keys(conn, registry):
    """The payload ships inline in the page, so a new key — even a null one —
    would change every unwindowed page's bytes."""
    rows = _ledger_rows(conn, registry, slug="cbot")
    drill = rows["cbot:board"]["drill"]
    assert set(drill) == PRE_513_DRILL_KEYS
    assert all(set(w) == PRE_513_DRILL_WINDOW_KEYS for w in drill["windows"])
    band = rows["cbot:board"]["band"]
    assert band["loading_months"] is None and band["window_note"] is None


def test_an_unwindowed_pinned_overlay_has_exactly_its_old_keys(conn, registry):
    state, reason, data = ledger_block(registry["cbot"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert set(data["reference"]) == PRE_513_REFERENCE_KEYS


def test_an_unwindowed_basis_renders_no_window_or_board_label(conn, registry):
    """India's mandi basis: no window, no named contract, nothing new on the card."""
    conn.execute(schema._CREATE_INDIA_DOMESTIC)
    for day in SESSIONS:
        conn.execute("INSERT INTO india_domestic_prices (Date, commodity, Close, unit) "
                     "VALUES (?,?,?,?)", (day.isoformat(), "Soybean (Mandi MP)", 67250.0, "INR/MT"))
        conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                     ("INR/USD", day.isoformat(), 0.0105))
    conn.commit()
    state, reason, data = basis_block(registry["india"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    assert data["history_windows"] is None and data["history_board"] is None
    html = _render("blocks/04_basis.html.j2", data)
    caption = f"{len(SESSIONS)} common sessions · mean "
    assert caption in html
    line = next(line for line in html.splitlines() if caption in line)
    assert line.endswith("</div>") and "window" not in line and "struck" not in line


def test_the_roll_note_names_an_ams_window_by_its_loading_month():
    """AMS calls every month's prompt slot "Current (prompt)"; the day-over-day
    note must not read as a roll from a window to itself (#513 follow-up) —
    the loading month names it (B39 #540)."""
    from analysis.origins.domain import ShipmentWindow
    from app.block_builders import SessionWindows, WindowQuote, _roll_note

    def session(start: date, end: date) -> SessionWindows:
        window = ShipmentWindow(start=start, end=end, label="Current (prompt)")
        return SessionWindows(printed=(WindowQuote(window=window, value=11.0, quotes=1),))

    windows = {
        date(2026, 9, 30): session(date(2026, 9, 30), date(2026, 9, 30)),
        date(2026, 10, 1): session(date(2026, 10, 1), date(2026, 10, 31)),
    }
    note = _roll_note(windows, date(2026, 10, 1), date(2026, 9, 30), "move")
    assert note == "move withheld: loading month rolled Sep → Oct — no like-for-like change"
