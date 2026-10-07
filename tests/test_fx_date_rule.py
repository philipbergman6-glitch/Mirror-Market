"""B2 (#308): A1's FX-date rule enforced end to end on every public calculator.

Each test seeds a home-currency print dated D and an FX series that is *only*
a prior close (inside or beyond the 3-day cap) or *only* a later close, then
asserts what the price, ledger, spread, crush and basis builders — and the
origins and futures FX readers — do with it. The rule (#298):

* own date's rate, else a labelled prior close at most 3 calendar days older,
  else blank with reason ``fx_gap_exceeded``;
* never a later-dated rate;
* both dates carried wherever a substitution happened.
"""
from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from analysis.origins import sources as origin_sources
from app import markets as markets_mod
from app.block_builders import SiteContext, build_blocks
from app.markets import load_markets
from pipeline import schema
from pricing.fx_alignment import FX_GAP_EXCEEDED, FX_NO_PRIOR_RATE

TODAY = date(2026, 10, 7)
D = TODAY - timedelta(days=1)           # the print's own date
INSIDE = D - timedelta(days=3)          # the cap, exactly
BEYOND = D - timedelta(days=4)          # one day past it, for the print on D
FAR = D - timedelta(days=5)             # past it for the D-1 prints too
LATER = D + timedelta(days=1)           # the print's own future


def _iso(day: date) -> str:
    return day.isoformat()


@pytest.fixture
def db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    path = tmp_path / "fx.db"
    conn = sqlite3.connect(str(path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_DCE_FUTURES,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_FORWARD_CURVE,
        schema._CREATE_WEATHER,
        schema._CREATE_PSD,
    ):
        conn.execute(ddl)
    # CBOT board on D and the prior sessions — the basis reference leg.
    for back in (0, 1, 2, 5):
        conn.execute(
            "INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
            ("Soybeans", _iso(D - timedelta(days=back)), 1050.0),
        )
    # Brazil: CEPEA (price/ledger) and AgRural FOB (basis), BRL/MT, on D and D-1.
    for back in (0, 1):
        for key, brl in (("Soybean (CEPEA)", 2000.0), ("Soybean (AgRural Paranaguá FOB)", 2100.0)):
            conn.execute(
                "INSERT INTO brazil_spot_prices (Date, commodity, price_brl) VALUES (?,?,?)",
                (_iso(D - timedelta(days=back)), key, brl),
            )
    # Dalian crush triplet on D, CNY/MT.
    for key, close in (
        ("DCE Soybean No.2", 3600.0), ("DCE Soybean Oil", 8000.0), ("DCE Soybean Meal", 3000.0),
    ):
        conn.execute(
            "INSERT INTO dce_futures (commodity, Date, Close) VALUES (?,?,?)", (key, _iso(D), close)
        )
    conn.commit()
    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(path)))
    monkeypatch.setattr(config, "DB_PATH", str(path))
    try:
        yield conn
    finally:
        conn.close()


@pytest.fixture
def registry():
    return load_markets()


def _fx(conn: sqlite3.Connection, pair: str, day: date, rate: float) -> None:
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", (pair, _iso(day), rate))
    conn.commit()


def _block(conn, registry, slug: str, block_id: str):
    ctx = SiteContext(conn=conn, today=TODAY)
    return next(b for b in build_blocks(registry[slug], None, ctx, markets=registry) if b.id == block_id)


# ---------------------------------------------------------------------------
# 01 price
# ---------------------------------------------------------------------------
def test_price_block_converts_at_a_prior_close_inside_the_cap_and_carries_both_dates(db, registry):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    leg = _block(db, registry, "brazil", "price").data["headline"]
    assert leg["as_of"] == _iso(D)
    assert round(leg["usd_mt"], 1) == 400.0
    assert leg["fx_observed_on"] == _iso(INSIDE)
    assert leg["fx_gap_days"] == 3
    assert leg["fx_aligned"] is True
    assert leg["fx_reason"] is None
    assert _iso(INSIDE) in leg["fx_label"]


def test_price_block_withholds_usd_beyond_the_cap_with_fx_gap_exceeded(db, registry):
    _fx(db, "BRL/USD", BEYOND, 0.20)
    leg = _block(db, registry, "brazil", "price").data["headline"]
    assert leg["usd_mt"] is None
    assert leg["home_value"] == 2000.0           # the BRL print still renders
    assert leg["fx_reason"] == FX_GAP_EXCEEDED
    assert leg["fx_observed_on"] is None


def test_price_block_never_converts_at_a_later_dated_rate(db, registry):
    """The ``fallback_to_oldest`` case A1 killed: only a future rate exists."""
    _fx(db, "BRL/USD", LATER, 0.20)
    leg = _block(db, registry, "brazil", "price").data["headline"]
    assert leg["usd_mt"] is None
    assert leg["fx_reason"] == FX_NO_PRIOR_RATE


def test_price_block_on_its_own_date_is_not_labelled_as_aligned(db, registry):
    _fx(db, "BRL/USD", D, 0.20)
    leg = _block(db, registry, "brazil", "price").data["headline"]
    assert round(leg["usd_mt"], 1) == 400.0
    assert leg["fx_aligned"] is False
    assert leg["fx_label"] is None


def test_a_usd_leg_carries_no_fx_fields_to_label(db, registry):
    leg = _block(db, registry, "cbot", "price").data["headline"]
    assert leg["fx_observed_on"] is None and leg["fx_label"] is None and leg["fx_reason"] is None


# ---------------------------------------------------------------------------
# 02 ledger + spread
# ---------------------------------------------------------------------------
def _ledger_rows(db, registry, slug: str) -> dict:
    block = _block(db, registry, slug, "ledger")
    return {row["leg_id"]: row for row in block.data["rows"]}


def test_ledger_row_converts_inside_the_cap_and_labels_the_substitution(db, registry):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    row = _ledger_rows(db, registry, "brazil")["brazil:cepea"]
    assert round(row["usd_mt"], 1) == 400.0
    assert row["fx_observed_on"] == _iso(INSIDE)
    assert row["fx_aligned"] is True
    assert _iso(INSIDE) in row["fx_label"]


def test_ledger_row_withholds_beyond_the_cap(db, registry):
    _fx(db, "BRL/USD", BEYOND, 0.20)
    row = _ledger_rows(db, registry, "brazil")["brazil:cepea"]
    assert row["usd_mt"] is None
    assert row["fx_reason"] == FX_GAP_EXCEEDED
    assert row["home_value"] == 2000.0


def test_ledger_row_never_converts_at_a_later_dated_rate(db, registry):
    _fx(db, "BRL/USD", LATER, 0.20)
    row = _ledger_rows(db, registry, "brazil")["brazil:cepea"]
    assert row["usd_mt"] is None
    assert row["fx_reason"] == FX_NO_PRIOR_RATE


def test_ledger_drilldown_history_applies_the_same_cap_per_point(db, registry):
    """The per-point chart series used its own ``_rate_at_or_before`` with no
    cap — one policy, both readers (#298 §6). The D-1 print is exactly 3 days
    from the rate and converts; the D print is 4 days away and is withheld.
    """
    _fx(db, "BRL/USD", BEYOND, 0.20)
    row = _ledger_rows(db, registry, "brazil")["brazil:cepea"]
    assert row["drill"]["usd"] == [400.0, None]
    _fx(db, "BRL/USD", FAR, 0.30)  # still the only rate inside anyone's cap is BEYOND
    db.execute("DELETE FROM currencies WHERE Date = ?", (_iso(BEYOND),))
    db.commit()
    row = _ledger_rows(db, registry, "brazil")["brazil:cepea"]
    assert row["drill"]["usd"] == [None, None]


def test_spread_is_withheld_when_a_legs_fx_is_beyond_the_cap(db, registry):
    """Brazil's ledger spreads the AgRural FOB leg over the pinned CEPEA leg.

    Both legs printed on D and D-1; with the only rate 5 days back neither
    session can be stated in USD, so there is no spread — not one struck at a
    stale rate.
    """
    _fx(db, "BRL/USD", FAR, 0.20)
    rows = _ledger_rows(db, registry, "brazil")
    spread_rows = [r for r in rows.values() if not r["is_own"] and r["market_slug"] == "brazil"]
    assert spread_rows, "fixture: Brazil's ledger has no non-pinned home leg"
    for row in spread_rows:
        assert row["spread_usd_mt"] is None
        assert row["spread_note"]


# ---------------------------------------------------------------------------
# 03 crush
# ---------------------------------------------------------------------------
def test_crush_block_strikes_at_a_prior_close_inside_the_cap_and_says_so(db, registry):
    _fx(db, "CNY/USD", INSIDE, 0.14)
    block = _block(db, registry, "dalian", "crush")
    assert block.state == "ok", block.reason
    assert block.data["as_of"] == _iso(D)
    assert block.data["fx_observed_on"] == _iso(INSIDE)
    assert block.data["fx_aligned"] is True
    assert _iso(INSIDE) in block.data["fx_label"]


def test_crush_block_is_empty_beyond_the_cap_with_fx_gap_exceeded(db, registry):
    _fx(db, "CNY/USD", BEYOND, 0.14)
    block = _block(db, registry, "dalian", "crush")
    assert block.state != "ok"
    assert FX_GAP_EXCEEDED in block.reason


def test_crush_block_never_strikes_at_a_later_dated_rate(db, registry):
    _fx(db, "CNY/USD", LATER, 0.14)
    block = _block(db, registry, "dalian", "crush")
    assert block.state != "ok"
    assert FX_NO_PRIOR_RATE in block.reason


# ---------------------------------------------------------------------------
# 04 basis
# ---------------------------------------------------------------------------
def test_basis_block_strikes_inside_the_cap_and_labels_the_fx_date(db, registry):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    block = _block(db, registry, "brazil", "basis")
    assert block.state == "ok", block.reason
    assert block.data["as_of"] == _iso(D)
    assert block.data["fx_observed_on"] == _iso(INSIDE)
    assert block.data["fx_aligned"] is True
    assert _iso(INSIDE) in block.data["fx_label"]


def test_basis_block_is_empty_beyond_the_cap(db, registry):
    _fx(db, "BRL/USD", FAR, 0.20)
    block = _block(db, registry, "brazil", "basis")
    assert block.state != "ok"
    assert FX_GAP_EXCEEDED in block.reason


def test_basis_block_falls_back_to_the_newest_session_it_can_strike(db, registry):
    """D's print is 4 days from the rate and is withheld; D-1's is 3 and
    converts. The basis is D-1's, said so, with the FX date labelled."""
    _fx(db, "BRL/USD", BEYOND, 0.20)
    block = _block(db, registry, "brazil", "basis")
    assert block.state == "ok", block.reason
    assert block.data["as_of"] == _iso(D - timedelta(days=1))
    assert block.data["fx_observed_on"] == _iso(BEYOND)
    assert block.data["fx_gap_days"] == 3


def test_basis_block_never_strikes_at_a_later_dated_rate(db, registry):
    _fx(db, "BRL/USD", LATER, 0.20)
    block = _block(db, registry, "brazil", "basis")
    assert block.state != "ok"


# ---------------------------------------------------------------------------
# The readers themselves
# ---------------------------------------------------------------------------
def test_site_context_fx_on_has_no_fallback_to_oldest(db):
    import inspect

    assert "fallback_to_oldest" not in inspect.signature(SiteContext.fx_on).parameters


def test_site_context_fx_on_returns_a_dated_resolution(db):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    res = SiteContext(conn=db, today=TODAY).fx_on("BRL/USD", D)
    assert res.rate == 0.20
    assert res.alignment.observed_on == INSIDE
    assert res.alignment.price_date == D


def test_origins_fx_on_applies_the_cap(db):
    _fx(db, "BRL/USD", BEYOND, 0.20)
    assert origin_sources.fx_on(db, "BRL/USD", D) is None
    _fx(db, "BRL/USD", INSIDE, 0.21)
    obs = origin_sources.fx_on(db, "BRL/USD", D)
    assert obs is not None and obs.observed_on == INSIDE and obs.usd_per_unit == 0.21


def test_origins_fx_resolution_names_the_reason(db):
    _fx(db, "BRL/USD", BEYOND, 0.20)
    res = origin_sources.fx_resolution_on(db, "BRL/USD", D)
    assert res.reason == FX_GAP_EXCEEDED


def test_futures_provider_fx_rate_applies_the_cap(db):
    from analysis.futures.providers import SqliteQuoteProvider

    _fx(db, "BRL/USD", BEYOND, 0.20)
    provider = SqliteQuoteProvider(db)
    assert provider.fx_rate("BRL/USD", on=D) is None
    _fx(db, "BRL/USD", INSIDE, 0.21)
    assert provider.fx_rate("BRL/USD", on=D) == (INSIDE, 0.21)


# ---------------------------------------------------------------------------
# Rendered
# ---------------------------------------------------------------------------
def _render(block_template: str, block) -> str:
    from app.templating import site_environment

    return site_environment().get_template(block_template).render(block=block, root="../")


def test_price_card_prints_the_substitution_caption_only_where_dates_differ(db, registry):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    html = _render("blocks/01_price.html.j2", _block(db, registry, "brazil", "price"))
    assert f"struck at FX {_iso(INSIDE)} (3d prior)" in html
    db.execute("DELETE FROM currencies")
    _fx(db, "BRL/USD", D, 0.20)
    html = _render("blocks/01_price.html.j2", _block(db, registry, "brazil", "price"))
    assert "struck at FX" not in html


def test_price_card_says_why_usd_is_withheld_and_still_prints_the_home_number(db, registry):
    _fx(db, "BRL/USD", BEYOND, 0.20)
    html = _render("blocks/01_price.html.j2", _block(db, registry, "brazil", "price"))
    assert f"USD/MT withheld — {FX_GAP_EXCEEDED}" in html
    assert "2,000.00" in html


def test_ledger_crush_and_basis_print_the_same_caption(db, registry):
    _fx(db, "BRL/USD", INSIDE, 0.20)
    _fx(db, "CNY/USD", INSIDE, 0.14)
    label = f"FX {_iso(INSIDE)} (3d prior)"
    assert label in _render("blocks/02_ledger.html.j2", _block(db, registry, "brazil", "ledger"))
    assert label in _render("blocks/03_crush.html.j2", _block(db, registry, "dalian", "crush"))
    assert label in _render("blocks/04_basis.html.j2", _block(db, registry, "brazil", "basis"))
