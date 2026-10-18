"""B30 #504: a venue that prints several shipment windows a session is read as
one named window, never as their mean.

V1 #474 (PR #499) hand-checked two headline physical numbers that matched no
published quote: Argentina's 483.0 was (493 + 495 + 461) / 3 across MAGyP's
Oct-26, Nov–Apr and May–Sep bands, and the US Gulf leg was the mean of six AMS
3147 delivery slots (Current → Feb) priced over three CBOT months. A physical
buyer prices a window. These pin that every surface — the price card, the
basis card, the ledger row, the landed stack's origin step and the briefing's
FOB board — shows one window's quote and names it.
"""

from __future__ import annotations

import calendar
import re
import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest
from jinja2 import Environment, FileSystemLoader

import config
from analysis.briefing.sections import fob_board
from analysis.origins.landed_cost import compute_landed_cost
from app import block_builders
from app.block_builders import SiteContext, build_blocks, headline_ledger, ledger_block
from app.markets import WINDOW_SCHEMES, _source, load_markets
from pipeline import schema
from pipeline.units import to_metric_tons

TODAY = datetime.now(timezone.utc).date()
D0 = TODAY - timedelta(days=1)
D1 = TODAY - timedelta(days=2)
TEMPLATES = Path(__file__).resolve().parents[1] / "app" / "templates"

# MAGyP circular 2081 (2026-10-16), position 12019000190C, as V1 read it.
AR_BANDS = ((0, 0, 493.0), (1, 6, 495.0), (7, 11, 461.0))
# AMS 3147 "Grain Report for 10/16/2026 - Final", Gulf Coast Ports, CIF-B.
GULF_SLOTS = (
    ("Current", 13.5325),
    ("+1¹", 13.7175),
    ("+1²", 13.7375),
    ("+2", 13.7775),
    ("+3", 13.8225),
    ("+4", 13.8375),
)


def _month(day: date, ahead: int) -> str:
    index = day.year * 12 + day.month - 1 + ahead
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def _slot(day: date, spec: str) -> str:
    """``+1¹`` → the AMS label one month after the report month, first half."""
    if spec == "Current":
        return spec
    ahead = int(spec[1])
    month = (day.month - 1 + ahead) % 12 + 1
    return calendar.month_abbr[month] + spec[2:]


def _seed_argentina(conn, day: date, bands=AR_BANDS, *, anchor: date = D0) -> None:
    """Bands are months after ``anchor``'s, so two sessions either side of a
    month end still print the same windows."""
    for start, end, price in bands:
        conn.execute(
            "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
            "price_usd_mt) VALUES (?,?,?,?,?,?)",
            (day.isoformat(), "Soybeans", "12019000190C", _month(anchor, start),
             _month(anchor, end), price),
        )


def _seed_gulf(conn, day: date) -> None:
    for spec, average in GULF_SLOTS:
        conn.execute(
            "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
            "VALUES (?,?,?,?,?)",
            (day.isoformat(), "Soybeans", "Gulf Coast Ports", _slot(day, spec), average),
        )


@pytest.fixture
def conn(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    db_path = tmp_path / "windows.db"
    connection = sqlite3.connect(str(db_path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_GULF_BIDS,
    ):
        connection.execute(ddl)
    for day in (D0, D1):
        connection.execute(
            "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
            ("Soybeans", day.isoformat(), 1050.0, 15000.0),
        )
    _seed_argentina(connection, D0)
    _seed_gulf(connection, D0)
    connection.commit()
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    yield connection
    connection.close()


@pytest.fixture
def registry():
    return load_markets()


def _ctx(conn) -> SiteContext:
    return SiteContext(conn=conn, today=TODAY)


def _block(conn, registry, slug, block_id):
    blocks = build_blocks(registry[slug], None, _ctx(conn), markets=registry)
    return next(b for b in blocks if b.id == block_id)


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(str(TEMPLATES)), autoescape=True)


def _render(template: str, **kw) -> str:
    return _env().get_template(template).render(root="../", **kw)


def _gulf_usd(value: float) -> float:
    usd = to_metric_tons(value * 100.0, "Soybeans")
    assert usd is not None
    return usd


# ---------------------------------------------------------------------------
# The numbers
# ---------------------------------------------------------------------------
def test_argentinas_headline_is_the_nearest_bands_quote_not_their_mean(conn, registry):
    price = _block(conn, registry, "argentina", "price")
    assert price.state == "ok", price.reason
    headline = price.data["headline"]
    assert headline["usd_mt"] == 493.0  # the Oct band MAGyP published — never 483.0
    assert headline["window"] == _month(D0, 0)
    assert headline["windows_printed"] == 3
    assert headline["quotes"] == 1
    assert [row["usd_mt"] for row in headline["window_rows"]] == [493.0, 495.0, 461.0]
    assert [row["is_chosen"] for row in headline["window_rows"]] == [True, False, False]


def test_the_gulf_leg_is_the_current_slot_not_six_slots_averaged(conn, registry):
    basis = _block(conn, registry, "cbot", "basis")
    assert basis.state == "ok", basis.reason
    assert basis.data["local_usd_mt"] == pytest.approx(_gulf_usd(13.5325))
    mean = sum(value for _, value in GULF_SLOTS) / len(GULF_SLOTS)
    assert basis.data["local_usd_mt"] != pytest.approx(_gulf_usd(mean))
    assert basis.data["window"] == "Current (prompt)"
    assert basis.data["windows_printed"] == 6
    assert basis.data["basis_usd_mt"] == pytest.approx(
        basis.data["local_usd_mt"] - basis.data["board_usd_mt"]
    )


def test_argentinas_basis_is_struck_on_the_same_named_band(conn, registry):
    basis = _block(conn, registry, "argentina", "basis")
    assert basis.state == "ok", basis.reason
    assert basis.data["local_usd_mt"] == 493.0
    assert basis.data["window"] == _month(D0, 0)


def test_ledger_rows_carry_the_window_their_print_is_for(conn, registry):
    state, reason, data = ledger_block(registry["argentina"], _ctx(conn), markets=registry)
    assert state == "ok", reason
    rows = {row["leg_id"]: row for row in data["rows"]}
    assert rows["argentina:fob"]["usd_mt"] == 493.0
    assert rows["argentina:fob"]["window"] == _month(D0, 0)
    assert rows["us_gulf:cif"]["usd_mt"] == pytest.approx(_gulf_usd(13.5325))
    assert rows["us_gulf:cif"]["window"] == "Current (prompt)"
    # An unwindowed leg says nothing about windows rather than inventing one.
    assert rows["cbot:board"]["window"] is None


def test_a_session_on_two_places_still_averages_inside_one_window(conn, registry):
    """Two barge locations quoting one slot are one cargo at two places."""
    conn.execute(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
        "VALUES (?,?,?,?,?)",
        (D0.isoformat(), "Soybeans", "Texas Gulf", "Current", 13.6325),
    )
    conn.commit()
    basis = _block(conn, registry, "cbot", "basis")
    assert basis.data["local_usd_mt"] == pytest.approx(_gulf_usd((13.5325 + 13.6325) / 2))
    assert basis.data["window"] == "Current (prompt)"
    assert basis.data["window_rows"][0]["quotes"] == 2


def test_a_window_that_has_sailed_is_never_the_prompt(registry):
    """AMS listed Mar¹ (1–15 Mar) on its 18 Mar 2025 report — a stale slot,
    not the nearest one a cargo could load in."""
    connection = sqlite3.connect(":memory:")
    connection.execute(schema._CREATE_GULF_BIDS)
    for slot, average in (("Mar¹", 10.10), ("Mar²", 10.20), ("Apr", 10.30)):
        connection.execute(
            "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
            "VALUES (?,?,?,?,?)",
            ("2025-03-18", "Soybeans", "Gulf Coast Ports", slot, average),
        )
    source = registry["cbot"].basis
    prints = block_builders._read_prints(connection, source, date(2025, 3, 20))
    session = prints.windows["Soybeans"][date(2025, 3, 18)]
    assert session.chosen.label == "Mar 2025 2nd half"
    assert prints.series["Soybeans"] == [(date(2025, 3, 18), 10.20, 1)]


def test_when_every_window_has_sailed_the_latest_is_taken_and_named(registry):
    """LAYERS.md (Layer 21, B30 #504): labelled, never withheld, and never the
    oldest stale band."""
    connection = sqlite3.connect(":memory:")
    connection.execute(schema._CREATE_ARGENTINA_FOB)
    for ship_from, ship_to, price in (("2026-07", "2026-07", 470.0), ("2026-08", "2026-09", 480.0)):
        connection.execute(
            "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
            "price_usd_mt) VALUES (?,?,?,?,?,?)",
            ("2026-10-16", "Soybeans", "12019000190C", ship_from, ship_to, price),
        )
    prints = block_builders._read_prints(
        connection, registry["argentina"].price, date(2026, 10, 18)
    )
    session = prints.windows["Soybeans"][date(2026, 10, 16)]
    assert session.chosen.label == "2026-08..2026-09"
    assert prints.series["Soybeans"] == [(date(2026, 10, 16), 480.0, 1)]


def test_a_mistyped_window_column_raises_rather_than_reading_as_no_data(conn, registry):
    broken = replace(registry["argentina"].price, window_columns=("ship_frm", "ship_to"))
    with pytest.raises(sqlite3.OperationalError, match="ship_frm"):
        block_builders._read_prints(conn, broken, TODAY)


def test_a_missing_table_is_still_no_data(registry):
    prints = block_builders._read_prints(
        sqlite3.connect(":memory:"), registry["argentina"].price, TODAY
    )
    assert prints.series == {key: [] for key in registry["argentina"].price.keys}


# ---------------------------------------------------------------------------
# One window per move
# ---------------------------------------------------------------------------
def test_a_move_across_a_window_roll_is_withheld(conn, registry):
    # The prior circular carried no current-month band, so its nearest window
    # is Nov–Apr: 495 → 493 is a calendar spread, not a session move.
    _seed_argentina(conn, D1, bands=((1, 6, 480.0), (7, 11, 450.0)))
    conn.commit()
    rolled = f"window rolled {_month(D0, 1)}..{_month(D0, 6)} → {_month(D0, 0)}"
    price = _block(conn, registry, "argentina", "price")
    headline = price.data["headline"]
    assert headline["chg_1_pct"] is None
    # Withheld with its reason, never a bare blank (review of #508).
    assert headline["move_notes"] == [f"session move withheld — {rolled}"]
    assert f"session move withheld — {rolled}" in _render("blocks/01_price.html.j2", block=price)
    state, _, data = ledger_block(registry["argentina"], _ctx(conn), markets=registry)
    row = next(r for r in data["rows"] if r["leg_id"] == "argentina:fob")
    assert row["usd_chg_pct"] is None and row["home_chg_pct"] is None
    assert row["move_note"] == f"move withheld — {rolled}"
    assert f"move withheld — {rolled}" in _render("blocks/_ledger_table.html.j2", d=data)


def test_a_move_inside_one_window_is_reported(conn, registry):
    _seed_argentina(conn, D1, bands=((0, 0, 490.0), (1, 6, 490.0), (7, 11, 460.0)))
    conn.commit()
    headline = _block(conn, registry, "argentina", "price").data["headline"]
    assert headline["chg_1_pct"] == pytest.approx((493.0 - 490.0) / 490.0 * 100.0)


# ---------------------------------------------------------------------------
# The markup
# ---------------------------------------------------------------------------
_MEAN = re.compile(r"mean of (\d+) (\w+)")


def _assert_no_unlabelled_blend(html: str) -> None:
    """Every "mean of N …" on these surfaces is a labelled blend of windows or
    a mean of places inside one named window — and this fixture has one row
    per window, so here it may only be the former."""
    for count, noun in _MEAN.findall(html):
        assert noun == "windows", f"unlabelled blend 'mean of {count} {noun}' rendered"


def test_rendered_headlines_print_their_window_and_no_blend(conn, registry):
    price = _block(conn, registry, "argentina", "price")
    price_html = _render("blocks/01_price.html.j2", block=price)
    assert "$493.0" in price_html and "$483.0" not in price_html
    assert f"window {_month(D0, 0)}" in price_html
    assert "nearest of 3 printed" in price_html
    # Per-window rows on the same page, one click away.
    assert "<details" in price_html
    assert f"{_month(D0, 1)}..{_month(D0, 6)}" in price_html
    _assert_no_unlabelled_blend(price_html)

    basis = _block(conn, registry, "cbot", "basis")
    basis_html = _render("blocks/04_basis.html.j2", block=basis)
    assert "window Current (prompt)" in basis_html
    assert "All 6 windows printed" in basis_html
    _assert_no_unlabelled_blend(basis_html)

    for slug in ("argentina", "cbot"):
        _, _, data = ledger_block(registry[slug], _ctx(conn), markets=registry)
        ledger_html = _render("blocks/_ledger_table.html.j2", d=data)
        assert "window Current (prompt)" in ledger_html
        _assert_no_unlabelled_blend(ledger_html)

    state, reason, data = headline_ledger(registry, _ctx(conn))
    assert state == "ok", reason
    headline_html = _render("blocks/_ledger_table.html.j2", d=data)
    assert f"window {_month(D0, 0)}" in headline_html
    _assert_no_unlabelled_blend(headline_html)


# ---------------------------------------------------------------------------
# Hard failures
# ---------------------------------------------------------------------------
def test_an_unreadable_window_fails_the_block_rather_than_guessing(conn, registry):
    conn.execute(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, price_usd_mt) "
        "VALUES (?,?,?,?,?,?)",
        (D0.isoformat(), "Soybeans", "other", "UPRIVER", None, 400.0),
    )
    conn.commit()
    price = _block(conn, registry, "argentina", "price")
    assert price.state != "ok"
    assert price.reason == "generation error"


@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"window_scheme": "ams_delivery"}, "reads 1 column"),
        ({"window_columns": ("delivery",)}, "not in"),
        ({"window_scheme": "weekly", "window_columns": ("x",)}, "not in"),
        ({"window_scheme": "magyp_months", "window_columns": ("ship_from",)}, "reads 2 column"),
    ],
)
def test_half_a_window_declaration_fails_at_load(patch, message):
    raw = {**config.MARKETS["cbot"]["basis"]}
    raw.pop("window_scheme")
    raw.pop("window_columns")
    with pytest.raises(ValueError, match=message):
        _source({**raw, **patch}, slug="cbot", block="basis")


def test_every_origin_leg_window_matches_its_market_descriptor():
    """The origins page and the market pages read one table through two
    declarations; a window column on one and not the other would publish two
    different numbers for the same leg."""
    for leg_id, leg in config.ORIGIN_LEGS.items():
        if leg.get("absent_reason"):
            continue
        source = config.MARKETS[leg["market"]][leg["block"]]
        scheme = leg.get("window_scheme", "none")
        if scheme == "none":
            assert "window_scheme" not in source, leg_id
            continue
        columns = leg.get("window_columns") or (leg["window_column"],)
        assert source["window_scheme"] == scheme, leg_id
        assert tuple(source["window_columns"]) == tuple(columns), leg_id
        assert len(columns) == WINDOW_SCHEMES[scheme], leg_id


# ---------------------------------------------------------------------------
# The landed stack and the briefing
# ---------------------------------------------------------------------------
def test_the_landed_stacks_origin_step_names_its_window():
    from tests.test_landed_cost import CN, OCT, UP_RIVER, _full_set, _quote

    result = compute_landed_cost(
        _quote(UP_RIVER, 493.0, shipment_window=OCT),
        CN, OCT, _full_set(), today=date(2026, 10, 16),
    )
    assert "shipment window Oct 2026" in result.steps[0].basis

    unwindowed = compute_landed_cost(
        _quote(UP_RIVER, 493.0, shipment_window=None),
        CN, OCT, _full_set(), today=date(2026, 10, 16),
    )
    assert "no shipment window published" in unwindowed.steps[0].basis


def test_the_briefing_fob_board_names_each_legs_window(monkeypatch):
    bids = pd.DataFrame({
        "report_date": pd.to_datetime(["2026-10-16"] * 2),
        "delivery": ["Current", "Dec"],
        "average": [13.5325, 13.7775],
    })
    fob = pd.DataFrame({
        "date": pd.to_datetime(["2026-10-16"] * 3),
        "ship_from": ["2026-10", "2026-11", "2027-05"],
        "ship_to": ["2026-10", "2027-04", "2027-09"],
        "price_usd_mt": [493.0, 495.0, 461.0],
    })
    monkeypatch.setattr(fob_board, "read_gulf_bids", lambda _c: bids)
    monkeypatch.setattr(fob_board, "read_argentina_fob", lambda _p: fob)
    monkeypatch.setattr(fob_board, "_brazil_leg", lambda _p, _c: None)
    board = fob_board.format({}, {})
    gulf = next(line for line in board.splitlines() if "US Gulf" in line)
    argentina = next(line for line in board.splitlines() if "Argentina" in line)
    # The site's own labels, so the briefing and the pages name one window.
    assert "window Current (prompt))" in gulf and "$497.2" in gulf
    assert "window 2026-10)" in argentina and "$493.0" in argentina


def test_the_briefing_picks_the_nearest_open_slot_by_the_sites_rule(monkeypatch):
    """Not "Current, else the first listed row": on 18 Mar 2025 AMS listed a
    sailed Mar¹ slot first; the nearest open one is Mar² (same rule as
    `_read_windowed`)."""
    bids = pd.DataFrame({
        "report_date": pd.to_datetime(["2025-03-18"] * 3),
        "delivery": ["Mar¹", "Mar²", "Apr"],
        "average": [10.10, 10.20, 10.30],
    })
    fob = pd.DataFrame({
        "date": pd.to_datetime(["2025-03-18"] * 2),
        "ship_from": ["2025-06", "2025-04"],
        "ship_to": ["2025-12", "2025-05"],
        "price_usd_mt": [300.0, 310.0],
    })
    monkeypatch.setattr(fob_board, "read_gulf_bids", lambda _c: bids)
    monkeypatch.setattr(fob_board, "read_argentina_fob", lambda _p: fob)
    gulf = fob_board._gulf_leg()
    argentina = fob_board._argentina_leg()
    assert gulf is not None and argentina is not None
    assert gulf[0] == pytest.approx(_gulf_usd(10.20))
    assert gulf[1] == "2025-03-18 · window Mar 2025 2nd half"
    assert argentina == (310.0, "2025-03-18 · window 2025-04..2025-05")

