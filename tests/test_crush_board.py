"""The headline crush board — section 04 (M16 #208).

Four markets' crush margins side by side. What these tests pin, in order of
how much a bug would cost:

1. **A kind label is mandatory.** Board (CBOT, Dalian), physical (Brazil) and
   administered (Argentina, Ley 21.453) are not the same claim, and four
   numbers in one row of cards is the easiest place on the site for them to
   collapse into one "crush" line (M2 #144 constraint 3 / M3 #145 constraint 4).
2. **The board prints the same number as block 03.** The board is a scan of
   margins the market pages carry in depth; a second engine here would let the
   headline and the page disagree about the same market's margin — the failure
   M7 #149 named and `analysis.futures.crush` exists to prevent.
3. **A range is struck only by the engine that struck the level.** A 1y mean
   off the continuous front-month series beside a named-contract margin would
   be a range around a different number.
4. **A leg with no margin says why.** Brazil has no oil or meal quote ingested
   at all; that card states the unbuilt scrape rather than vanishing, and never
   borrows another market's number.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from app import markets as markets_mod
from app.block_builders import (
    CRUSH_RANGE_MIN_OBS,
    SiteContext,
    crush_block,
    headline_crush_board,
)
from app.markets import load_markets
from pipeline import schema

TODAY = date(2026, 8, 12)

# Dalian's seeded legs, CNY/MT, and Argentina's, USD/MT.
DCE_BEAN, DCE_OIL, DCE_MEAL = 3600.0, 8000.0, 3000.0
CNY_USD = 0.14
ARG_BEAN, ARG_OIL, ARG_MEAL = 400.0, 1100.0, 340.0

# Enough sessions for a range on one leg and deliberately too few on another —
# the two states sub-question 2 of #208 had to decide between.
DALIAN_SESSIONS = 40
ARGENTINA_SESSIONS = 5


def _day(offset: int) -> str:
    return (TODAY - timedelta(days=offset)).isoformat()


@pytest.fixture
def seeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """One DB carrying every board leg, at numbers chosen to be checkable."""
    db_path = tmp_path / "crush_board.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_DCE_FUTURES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_FORWARD_CURVE,
    ):
        conn.execute(ddl)

    # CBOT — named contracts out of `forward_curve`, three stored sessions.
    # Sep 2026 is the prompt crush period at TODAY.
    for offset in (0, 1, 2):
        for commodity, month, ticker, close in (
            ("Soybeans",     "2026-09-01", "ZSU26.CBT", 1050.0),
            ("Soybean Meal", "2026-09-01", "ZMU26.CBT",  300.0),
            ("Soybean Oil",  "2026-09-01", "ZLU26.CBT",   52.0),
        ):
            conn.execute(
                "INSERT INTO forward_curve (commodity, contract_month, label, ticker, "
                "close, observation_date, fetched_date) VALUES (?,?,?,?,?,?,?)",
                (commodity, month, ticker[:-4], ticker, close, _day(offset), _day(offset)),
            )

    # Dalian — a long enough run for a range, at a rate that MOVES, so a
    # margin converted at today's rate is a different number from one
    # converted at its own day's rate.
    for offset in range(DALIAN_SESSIONS):
        conn.execute(
            "INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
            ("CNY/USD", _day(offset), CNY_USD + offset * 0.001),
        )
        for key, close in (
            ("DCE Soybean No.2", DCE_BEAN),
            ("DCE Soybean Oil", DCE_OIL),
            ("DCE Soybean Meal", DCE_MEAL),
            ("DCE Soybean No.1", 4900.0),
        ):
            conn.execute(
                "INSERT INTO dce_futures (commodity, Date, Close) VALUES (?,?,?)",
                (key, _day(offset), close),
            )

    # Argentina — natively USD/MT, and a handful of sessions only.
    for offset in range(ARGENTINA_SESSIONS):
        for product, price in (
            ("Soybeans", ARG_BEAN),
            ("Soybean Oil", ARG_OIL),
            ("Soybean Meal", ARG_MEAL),
        ):
            conn.execute(
                "INSERT INTO argentina_fob (date, product, position, ship_from, "
                "price_usd_mt) VALUES (?,?,?,?,?)",
                (_day(offset), product, f"pos-{product}", "2026-08", price),
            )
    conn.commit()

    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    ctx = SiteContext(conn=conn, today=TODAY)
    try:
        yield ctx
    finally:
        conn.close()


@pytest.fixture
def registry():
    return load_markets()


def _rows(seeded, registry) -> dict[str, dict]:
    state, reason, data = headline_crush_board(registry, seeded)
    assert state == "ok", reason
    return {row["market_slug"]: row for row in data["rows"]}


def _yields() -> dict[str, float]:
    from app.markets import CRUSH_YIELD_SETS

    return CRUSH_YIELD_SETS["soy_board"]


# ---------------------------------------------------------------------------
# The board's shape
# ---------------------------------------------------------------------------
def test_the_board_is_the_registrys_four_markets_in_registry_order(seeded, registry):
    """Which markets sit on the board is registry data, not a code path."""
    _state, _reason, data = headline_crush_board(registry, seeded)
    assert [row["market_slug"] for row in data["rows"]] == list(config.CRUSH_BOARD)
    assert config.CRUSH_BOARD == ("cbot", "dalian", "brazil", "argentina")


def test_every_card_links_to_the_market_page_that_carries_the_depth(seeded, registry):
    for slug, row in _rows(seeded, registry).items():
        assert row["href"] == registry[slug].url


def test_every_card_states_its_kind_and_the_kinds_do_not_collapse(seeded, registry):
    """M2 constraint 3: board, physical and administered stay three animals."""
    rows = _rows(seeded, registry)
    assert rows["cbot"]["kind"] == "board"
    assert rows["dalian"]["kind"] == "board"
    assert rows["argentina"]["kind"] == "administered"
    for slug in ("cbot", "dalian", "argentina"):
        assert rows[slug]["kind_label"], f"{slug} renders no kind label"
    # Brazil's leg is not built, so it has no kind to claim — and inventing
    # one ("physical", the kind it *would* be) would label a card that carries
    # no number.
    assert rows["brazil"]["kind"] is None


def test_the_board_is_empty_with_a_reason_when_no_leg_strikes_a_margin(
    tmp_path: Path, registry
):
    conn = sqlite3.connect(str(tmp_path / "bare.db"))
    for ddl in (schema._CREATE_DCE_FUTURES, schema._CREATE_ARGENTINA_FOB,
                schema._CREATE_FORWARD_CURVE, schema._CREATE_CURRENCIES):
        conn.execute(ddl)
    conn.commit()
    state, reason, _data = headline_crush_board(registry, SiteContext(conn=conn, today=TODAY))
    conn.close()
    assert state == "empty"
    assert reason.strip()


# ---------------------------------------------------------------------------
# One engine — the board and block 03 cannot disagree
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("slug", ["cbot", "dalian", "argentina"])
def test_each_card_prints_the_same_margin_as_that_markets_block_03(slug, seeded, registry):
    state, reason, block = crush_block(registry[slug], seeded)
    assert state == "ok", reason
    row = _rows(seeded, registry)[slug]
    assert row["margin_usd_mt"] == block["margin_usd_mt"]
    assert row["as_of"] == block["as_of"]
    assert row["contract_basis"] == block["contract_basis"]


def test_the_dalian_card_keys_off_no2_the_imported_crush_bean(seeded, registry):
    """#152: No.1 is the domestic food bean and Chinese crushers do not crush it."""
    row = _rows(seeded, registry)["dalian"]
    assert row["bean_key"] == "DCE Soybean No.2"
    yields = _yields()
    expected = (
        (DCE_OIL * yields["oil"] + DCE_MEAL * yields["meal"] - DCE_BEAN) * CNY_USD
    )
    assert round(row["margin_usd_mt"], 6) == round(expected, 6)
    # The China story is a CNY one too — the legs share one per-MT currency.
    assert row["home_currency"] == "CNY"
    assert round(row["margin_home"], 6) == round(
        DCE_OIL * yields["oil"] + DCE_MEAL * yields["meal"] - DCE_BEAN, 6
    )


def test_the_argentina_card_is_a_verified_leg_not_a_provisional_one(seeded, registry):
    """#162 cross-checked NCM 23040010100B against dataset 358's labelled series."""
    row = _rows(seeded, registry)["argentina"]
    yields = _yields()
    assert round(row["margin_usd_mt"], 6) == round(
        ARG_OIL * yields["oil"] + ARG_MEAL * yields["meal"] - ARG_BEAN, 6
    )
    assert row["provisional"] is False
    assert row["provisional_note"] is None
    # Natively USD/MT — one currency, so there is no second quote to make.
    assert row["margin_home"] is None


def test_the_cbot_card_names_its_contracts_and_the_dalian_card_cannot(seeded, registry):
    rows = _rows(seeded, registry)
    assert rows["cbot"]["contract_basis"] == "named_contract"
    assert rows["cbot"]["legs_named"] is True
    assert rows["dalian"]["contract_basis"] == "continuous"
    assert rows["dalian"]["legs_named"] is False
    assert "not published" in rows["dalian"]["contract_note"]


# ---------------------------------------------------------------------------
# Brazil — the honest empty state (#208 sub-question 3)
# ---------------------------------------------------------------------------
def test_brazil_states_the_unbuilt_scrape_rather_than_showing_a_margin(seeded, registry):
    row = _rows(seeded, registry)["brazil"]
    assert row["state"] == "absent"
    assert row["margin_usd_mt"] is None
    assert row["reason"] == registry["brazil"].absent_reason("crush")
    assert "unbuilt scrape" in row["reason"]


def test_an_absent_card_with_no_stated_reason_raises_rather_than_rendering_blank(
    seeded, registry
):
    """The `Block` type's rule, kept where the type cannot reach.

    An unexplained empty card in a row of four margins reads as a market with
    no crush industry — the loudest thing this board can say by accident.
    """
    from dataclasses import replace

    brazil = replace(registry["brazil"], absent_reasons={"crush": "   "})
    with pytest.raises(ValueError, match="no reason"):
        headline_crush_board({**registry, "brazil": brazil}, seeded)


def test_an_absent_leg_never_borrows_another_markets_number(seeded, registry):
    rows = _rows(seeded, registry)
    for field in ("margin_usd_mt", "margin_home", "as_of", "range"):
        assert rows["brazil"][field] is None


# ---------------------------------------------------------------------------
# Level vs context (#208 sub-questions 2 and 4)
# ---------------------------------------------------------------------------
def test_a_leg_with_enough_history_carries_its_own_mean_and_range(seeded, registry):
    row = _rows(seeded, registry)["dalian"]
    band = row["range"]
    assert band is not None
    assert band["n_obs"] == DALIAN_SESSIONS
    yields = _yields()
    home = DCE_OIL * yields["oil"] + DCE_MEAL * yields["meal"] - DCE_BEAN
    # Every session's margin is converted at THAT session's rate, so a flat
    # CNY margin over a moving rate is a moving USD one. Struck at today's
    # rate throughout, low and high would be the same number.
    assert band["low"] < band["high"]
    assert round(band["high"], 6) == round(
        home * (CNY_USD + (DALIAN_SESSIONS - 1) * 0.001), 6
    )
    assert band["low"] <= band["mean"] <= band["high"]
    # Sessions, never "1Y": the read is bounded and a leg's stored depth is its
    # own, so a year is a claim about coverage this cannot make.
    assert band["window"] == f"{DALIAN_SESSIONS}-session"


def test_a_leg_with_too_little_history_says_no_range_yet_and_prints_the_level(
    seeded, registry
):
    """Argentina's own case: a handful of observations is not a series."""
    row = _rows(seeded, registry)["argentina"]
    assert row["margin_usd_mt"] is not None       # the level still prints
    assert row["range"] is None
    assert str(ARGENTINA_SESSIONS) in row["range_note"]
    assert "no range yet" in row["range_note"].lower()
    assert ARGENTINA_SESSIONS < CRUSH_RANGE_MIN_OBS


def test_a_named_contract_margin_gets_no_range_off_a_different_engine(seeded, registry):
    """A range is struck only by the engine that struck the level.

    The CBOT card's number is ZSU26/ZMU26/ZLU26 out of `forward_curve`. The
    continuous front-month series in `prices` runs back fifteen years and
    would produce a mean and a range — around a different margin.
    """
    row = _rows(seeded, registry)["cbot"]
    assert row["margin_usd_mt"] is not None
    assert row["range"] is None
    assert "named" in row["range_note"].lower()


def test_the_range_only_counts_sessions_every_leg_printed(seeded, registry):
    """A margin struck across two days is not a margin — in history either."""
    seeded.conn.execute(
        "INSERT INTO dce_futures (commodity, Date, Close) VALUES (?,?,?)",
        ("DCE Soybean Oil", _day(DALIAN_SESSIONS + 3), DCE_OIL),
    )
    seeded.conn.commit()
    row = _rows(SiteContext(conn=seeded.conn, today=TODAY), registry)["dalian"]
    assert row["range"]["n_obs"] == DALIAN_SESSIONS


def test_the_board_never_ranges_a_leg_it_could_not_restrike(seeded, registry):
    """Whatever the card shows, `range` and `range_note` are never both empty.

    The pair is the claim: a number with neither a range nor a reason there is
    none reads as though nobody looked.
    """
    for row in _rows(seeded, registry).values():
        if row["state"] != "ok":
            continue
        assert (row["range"] is not None) != bool(row["range_note"])


def test_a_session_older_than_every_rate_is_dropped_not_converted_forward(
    seeded, registry
):
    """History never borrows a rate from a session's own future.

    ``ctx.fx_on`` falls back to the oldest stored rate, which is a carry of at
    most a weekend for a level and a fabrication for a two-year-old margin.
    """
    # Drop the five OLDEST rates; those sessions now predate every rate stored.
    seeded.conn.execute("DELETE FROM currencies WHERE Date <= ?", (_day(DALIAN_SESSIONS - 5),))
    seeded.conn.commit()
    row = _rows(SiteContext(conn=seeded.conn, today=TODAY), registry)["dalian"]
    assert row["range"]["n_obs"] == DALIAN_SESSIONS - 5


# ---------------------------------------------------------------------------
# The rendered section
# ---------------------------------------------------------------------------
def _render(context: dict) -> str:
    from jinja2 import Environment, FileSystemLoader

    from scripts import generate_html

    env = Environment(
        loader=FileSystemLoader(str(generate_html.TEMPLATE_DIR)), autoescape=False
    )
    return env.get_template("dashboard.html.j2").render(
        sections=generate_html.SECTIONS,
        generated_at="2026-08-12 06:00 UTC",
        masthead={},
        freshness_items=[],
        **context,
    )


def test_the_rendered_board_labels_every_card_and_links_every_market(seeded, registry):
    from bs4 import BeautifulSoup

    state, reason, data = headline_crush_board(registry, seeded)
    html = _render({"crush_board": {"state": state, "reason": reason, "data": data}})
    section = BeautifulSoup(html, "html.parser").select_one("#crush-board")

    cards = section.select("[data-crush-market]")
    assert [card["data-crush-market"] for card in cards] == list(config.CRUSH_BOARD)
    for card in cards:
        assert card.select_one("a[href]"), "a card that does not link to its page"
        if card.get("data-crush-kind"):
            assert card.select_one("span.kind"), "a margin rendered with no kind label"
    # Brazil's card keeps its slot as a labelled empty state, never a gap.
    brazil = section.select_one('[data-crush-market="brazil"]')
    assert "empty-state" in brazil.get("class")
    assert "no source" in brazil.get_text(" ", strip=True)


def test_the_section_numbers_on_the_page_match_the_index_nav():
    """A renumber that touched one and not the other would ship a broken scan."""
    from bs4 import BeautifulSoup

    from scripts.generate_html import SECTIONS

    soup = BeautifulSoup(_render({}), "html.parser")
    for spec in SECTIONS:
        section = soup.select_one(f"section#{spec['id']}")
        assert section is not None, f"no section for nav entry {spec['id']}"
        assert section.select_one(".sec-no").get_text(strip=True) == spec["no"]
