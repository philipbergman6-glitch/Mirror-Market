"""The propagation ledger — block 02 and the headline's eight rows (M19 #223).

M3 #145 fixed the shape, M12 #161 the counterpart sets. What these tests pin,
in order of how much a bug would cost:

1. **A row stamp is not a print.** Grain SA re-dates a carried SAFEX price with
   Volume 0; a ledger that read that as a reprice would be lying in the exact
   place it claims authority. Where the registry names a ``trade_proof_column``,
   a row failing it is not a print.
2. **Silence never reads as flat.** Every row carries a state pill, and being
   behind is judged against the gap that is normal for *that leg* — not against
   ``FRESHNESS_WARNING_DAYS = 7``, which lets a daily leg go six days silent.
3. **A spread is one session's number.** Struck on a session both legs printed,
   or not struck at all. Two dates subtracted is a calendar artefact.
4. **The counterpart set is registry data, validated at load.** Leg ids live in
   a different id space from market slugs, so nothing else would catch a typo:
   an unresolvable leg renders an empty row, which reads as "that market has not
   printed" — the ledger's most important statement, made by accident.
5. **M12's decisions survive contact with the code.** India has no foreign
   counterpart; Europe and Nigeria have no ledger at all; Dalian and Argentina
   run four rows and are never padded to five.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from app import markets as markets_mod
from app.block_builders import (
    LEDGER_STATE_DARK,
    LEDGER_STATE_NO_PRINT,
    LEDGER_STATE_OUT_OF_CADENCE,
    LEDGER_STATE_REPRICED,
    SiteContext,
    headline_ledger,
    ledger_block,
)
from app.blocks import absent_reason
from app.markets import load_markets
from pipeline import schema

TODAY = date(2026, 8, 12)


def _day(offset: int) -> str:
    return (TODAY - timedelta(days=offset)).isoformat()


@pytest.fixture
def seeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """One DB carrying every ledger leg, at numbers chosen to be checkable."""
    db_path = tmp_path / "ledger.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_DCE_FUTURES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_INDIA_DOMESTIC,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_SAFEX,
        schema._CREATE_EC_OILSEED_PRICES,
    ):
        conn.execute(ddl)

    # CBOT — the freshest leg, so it is the ledger's leading edge.
    for offset, close in ((0, 1100.0), (1, 1000.0)):
        conn.execute(
            "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
            ("Soybeans", _day(offset), close, 15000.0),
        )
    # US Gulf — two barge locations on one report date (the averaging case),
    # and the day before, so the leg has a move.
    for offset, low, high in ((1, 11.20, 11.60), (2, 11.00, 11.40)):
        for location, average in (("NOLA", low), ("TEXAS", high)):
            conn.execute(
                "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
                "VALUES (?,?,?,?,?)",
                (_day(offset), "Soybeans", location, "SPOT", average),
            )
    # Brazil — the FX case. The BRL price is FLAT across the two prints while
    # the rate moves 5%, so the USD move is entirely the currency's doing.
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", ("BRL/USD", _day(2), 0.20))
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", ("BRL/USD", _day(1), 0.21))
    for offset in (1, 2):
        conn.execute(
            "INSERT INTO brazil_spot_prices (Date, commodity, price_brl) VALUES (?,?,?)",
            (_day(offset), "Soybean (CEPEA)", 2000.0),
        )
    # Paranaguá prints on ONE of CEPEA's two sessions — the spread has exactly
    # one session it may legally be struck on.
    conn.execute(
        "INSERT INTO brazil_spot_prices (Date, commodity, price_brl) VALUES (?,?,?)",
        (_day(2), "Soybean (AgRural Paranaguá FOB)", 2100.0),
    )
    # Argentina — natively USD/MT, so it has no second currency to quote in.
    conn.execute(
        "INSERT INTO argentina_fob (date, product, position, ship_from, price_usd_mt) "
        "VALUES (?,?,?,?,?)",
        (_day(1), "Soybeans", "12010090100W", "UPRIVER", 450.0),
    )
    # SAFEX — the carry-forward trap (#157). The NEWEST row is the venue
    # re-stamping a price that did not trade (Volume 0); the real last print is
    # the day before it.
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", ("ZAR/USD", _day(0), 0.055))
    conn.execute(
        "INSERT INTO safex_prices (Date, commodity, Close, Volume) VALUES (?,?,?,?)",
        (_day(0), "Soybean (SAFEX)", 8000.0, 0.0),
    )
    conn.execute(
        "INSERT INTO safex_prices (Date, commodity, Close, Volume) VALUES (?,?,?,?)",
        (_day(3), "Soybean (SAFEX)", 8000.0, 412.0),
    )
    conn.commit()

    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    return SiteContext(conn=conn, today=TODAY)


@pytest.fixture
def registry():
    return load_markets()


def _rows(slug, seeded, registry) -> dict[str, dict]:
    state, reason, data = ledger_block(registry[slug], seeded, markets=registry)
    assert state == "ok", reason
    return {row["leg_id"]: row for row in data["rows"]}


# ---------------------------------------------------------------------------
# 1. A row stamp is not a print
# ---------------------------------------------------------------------------
def test_a_carried_forward_row_is_not_a_reprice(seeded, registry):
    """SAFEX's newest row has Volume 0 — the venue re-dated a price, nothing traded.

    Without this the SA page would claim South Africa repriced today, on a day
    the contract did not trade at all. `LastTradedTime` is a *row* stamp.
    """
    row = _rows("south_africa", seeded, registry)["south_africa:safex"]
    assert row["as_of"] == _day(3), "the zero-volume row was read as a print"
    assert row["state"] == LEDGER_STATE_NO_PRINT
    assert row["trade_proved"] is True


def test_a_leg_with_no_proof_column_still_counts_its_rows(seeded, registry):
    """An assessment is not a trade — demanding volume of one is a category error.

    CEPEA, AgRural, AMS, MAGyP and the mandis publish no volume by nature, and
    their `quote_kind` already says which animal they are.
    """
    row = _rows("brazil", seeded, registry)["brazil:cepea"]
    assert row["trade_proved"] is False
    assert row["as_of"] == _day(1)


def test_a_null_proof_value_keeps_the_row(seeded, registry):
    """NULL is proof of nothing; dropping it would invent an outage.

    `<= 0` is the venue saying nothing traded. A missing volume is the venue
    saying nothing at all, and a ledger that reads silence as an outage is worse
    than one that reads it as a print.
    """
    seeded.conn.execute("DELETE FROM prices WHERE Date = ?", (_day(0),))
    seeded.conn.execute(
        "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,NULL)",
        ("Soybeans", _day(0), 1100.0),
    )
    seeded._cache.clear()
    row = _rows("cbot", seeded, registry)["cbot:board"]
    assert row["as_of"] == _day(0)


# ---------------------------------------------------------------------------
# 2. Silence never reads as flat
# ---------------------------------------------------------------------------
def test_the_leading_edge_is_repriced_and_everyone_behind_it_is_not(seeded, registry):
    rows = _rows("cbot", seeded, registry)
    assert rows["cbot:board"]["state"] == LEDGER_STATE_REPRICED
    assert rows["us_gulf:cif"]["state"] == LEDGER_STATE_NO_PRINT
    assert _day(1) in rows["us_gulf:cif"]["state_detail"]


def test_being_behind_is_judged_against_this_legs_own_expected_gap(seeded, registry):
    """M4 section 3.4 trap 5 — the freshness window is too loose to say this.

    `FRESHNESS_WARNING_DAYS = 7` lets a daily leg go six days silent without a
    word, so "overdue" has to come from a per-leg expected gap instead.
    """
    rows = _rows("cbot", seeded, registry)
    assert rows["us_gulf:cif"]["overdue"] is False, "one day behind is a normal Gulf gap"
    assert rows["brazil:paranagua"]["age_days"] == 2
    assert rows["brazil:paranagua"]["overdue"] is False

    seeded.conn.execute("DELETE FROM gulf_bids")
    seeded._cache.clear()
    gulf = _rows("cbot", seeded, registry)["us_gulf:cif"]
    assert gulf["state"] == LEDGER_STATE_DARK
    assert gulf["usd_mt"] is None


def test_a_leg_past_its_layers_recency_budget_is_dark(seeded, registry):
    """`dark` uses the same budget main.py grades the layer on — one answer."""
    seeded.conn.execute("DELETE FROM safex_prices")
    seeded._cache.clear()
    state, reason, _ = ledger_block(registry["south_africa"], seeded, markets=registry)
    # The pinned leg going dark empties the whole block: with no own leg there
    # is nothing for the counterparts to be a spread against.
    assert state == "empty"
    assert "no trade-proved print" in reason


# ---------------------------------------------------------------------------
# 3. Dual quote and the FX tag
# ---------------------------------------------------------------------------
def test_the_fx_tag_fires_when_the_currency_did_the_work(seeded, registry):
    """The BRL price is flat across both prints; only the rate moved 5%.

    Printing one move would credit the market with a currency move — the exact
    misread M3 #145's dual quote exists to prevent.
    """
    row = _rows("brazil", seeded, registry)["brazil:cepea"]
    assert row["home_chg_pct"] == pytest.approx(0.0)
    assert row["usd_chg_pct"] == pytest.approx(5.0)
    assert row["fx_tag"] is True
    assert "BRL/USD" in row["fx_note"]


def test_a_usd_native_leg_is_not_dual_quoted(seeded, registry):
    """Argentina quotes in USD/MT — restating that number is not a second view.

    The EC workbook taught this from the other direction: its EUR column is the
    USD one divided by an ECB rate, so a dual quote there would be our own
    arithmetic dressed as the venue's second opinion (#163).
    """
    row = _rows("argentina", seeded, registry)["argentina:fob"]
    assert row["has_home_quote"] is False
    assert row["home_unit"] == "USD/MT"
    assert row["usd_mt"] == pytest.approx(450.0)


def test_the_home_print_carries_the_unit_the_venue_publishes(seeded, registry):
    """A price is only a price with its unit, and one table holds three."""
    rows = _rows("cbot", seeded, registry)
    assert rows["cbot:board"]["home_unit"] == "cents/bu"
    assert rows["us_gulf:cif"]["home_unit"] == "USD/bu"
    assert rows["brazil:paranagua"]["home_unit"] == "BRL/MT"


def test_several_quotes_on_one_date_are_averaged_and_counted(seeded, registry):
    row = _rows("cbot", seeded, registry)["us_gulf:cif"]
    assert row["quotes"] == 2
    assert row["home_value"] == pytest.approx(11.40)


# ---------------------------------------------------------------------------
# 4. A spread is one session's number
# ---------------------------------------------------------------------------
def test_the_spread_is_struck_on_a_session_both_legs_printed(seeded, registry):
    """CEPEA prints twice, Paranaguá once — one legal session for the spread.

    Both legs at BRL 2000 and 2100 on that session, converted at that session's
    own rate (0.20): a spread of exactly +$20/MT.
    """
    row = _rows("brazil", seeded, registry)["brazil:paranagua"]
    assert row["spread_as_of"] == _day(2)
    assert row["spread_usd_mt"] == pytest.approx(20.0)


def test_no_common_session_means_no_spread_rather_than_a_wrong_one(seeded, registry):
    seeded.conn.execute(
        "DELETE FROM brazil_spot_prices WHERE commodity = 'Soybean (CEPEA)' AND Date = ?",
        (_day(2),),
    )
    seeded._cache.clear()
    row = _rows("brazil", seeded, registry)["brazil:paranagua"]
    assert row["spread_usd_mt"] is None
    assert "calendar artefact" in row["spread_note"]


def test_the_pinned_leg_has_no_spread_against_itself(seeded, registry):
    assert _rows("cbot", seeded, registry)["cbot:board"]["spread_usd_mt"] is None


# ---------------------------------------------------------------------------
# 5. M12's decisions, as data
# ---------------------------------------------------------------------------
def test_every_ledger_pins_its_own_market_first(registry):
    for slug, market in registry.items():
        if market.ledger is None:
            continue
        assert market.ledger.own.market.slug == slug


def test_india_has_no_foreign_counterpart(registry):
    """M12's most surprising result — and it is data, not a code path.

    GM bean imports are banned behind a tariff wall, so no origin is connected
    to this bean by trade. A counterpart row would invite an arbitrage that
    cannot be worked (#206), so the ledger is two domestic state medians.
    """
    ledger = registry["india"].ledger
    assert [leg.leg_id for leg in ledger.legs] == ["india:mandi_mp", "india:mandi_mh"]
    assert all(leg.market.slug == "india" for leg in ledger.legs)
    assert "no origin qualifies" in ledger.note.lower()


def test_dalian_and_argentina_are_four_rows_and_are_not_padded(registry):
    """A layout constant is not a reason to render a relationship."""
    assert len(registry["dalian"].ledger.legs) == 4
    assert len(registry["argentina"].ledger.legs) == 4
    assert "argentina:fob" not in [leg.leg_id for leg in registry["dalian"].ledger.legs]


def test_cbot_is_not_pinned_on_every_page(registry):
    """M4 found CBOT is our least reliable same-day leg, not a default row."""
    assert "cbot:board" not in [leg.leg_id for leg in registry["india"].ledger.legs]
    sa = registry["south_africa"].ledger
    assert "cbot:board" in sa.reference_leg_ids


def test_south_africas_cbot_row_renders_last_because_it_is_a_reference(seeded, registry):
    _state, _reason, data = ledger_block(registry["south_africa"], seeded, markets=registry)
    assert data["rows"][-1]["leg_id"] == "cbot:board"
    assert data["rows"][-1]["is_reference"] is True


def test_every_ledger_is_one_good(registry):
    """M3's "kinds do not mix" has a twin: goods do not mix either.

    In a single USD/MT column a per-row label is not strong enough to stop five
    numbers reading as one price in five places, which is why the straw man's
    DCE-meal row is out and a meal ledger would be a second block.
    """
    meal_or_oil = ("Meal", "Oil")
    for market in registry.values():
        if market.ledger is None:
            continue
        for leg in market.ledger.legs:
            assert not any(word in leg.key for word in meal_or_oil), leg.leg_id


def test_europe_and_nigeria_have_no_ledger_block_at_all(registry):
    """A ledger-less page is a legal configuration, not a degraded one."""
    for slug in ("europe", "nigeria"):
        assert registry[slug].ledger is None
        reason = absent_reason(registry[slug], "ledger")
        assert reason and len(reason) > 20


def test_every_ledger_declares_a_rule_and_states_it(registry):
    for market in registry.values():
        if market.ledger is None:
            continue
        assert market.ledger.rule in config.LEDGER_RULES
        assert market.ledger.rule_statement
        assert market.ledger.note


# ---------------------------------------------------------------------------
# Registry validation — a typo must fail the build, never render a blank row
# ---------------------------------------------------------------------------
def _reload(monkeypatch, **overrides):
    for name, value in overrides.items():
        monkeypatch.setattr(config, name, value)
    return load_markets()


def test_an_unknown_leg_id_fails_the_build(monkeypatch):
    ledgers = {**config.LEDGERS, "cbot": {**config.LEDGERS["cbot"], "legs": ["cbot:board", "typo:leg"]}}
    with pytest.raises(ValueError, match="not in config.LEDGER_LEGS"):
        _reload(monkeypatch, LEDGERS=ledgers)


def test_a_leg_pointing_at_a_missing_key_fails_the_build(monkeypatch):
    legs = {**config.LEDGER_LEGS, "cbot:board": {**config.LEDGER_LEGS["cbot:board"], "key": "Soybena"}}
    with pytest.raises(ValueError, match="not one of"):
        _reload(monkeypatch, LEDGER_LEGS=legs)


def test_a_non_daily_leg_fails_the_build(monkeypatch):
    """The ledger is daily-only — a weekly row would read as an outage."""
    legs = {
        **config.LEDGER_LEGS,
        "europe:moselle": {
            "market": "europe", "block": "price",
            "key": "EU Rapeseed (Moselle)", "label": "EU Moselle",
        },
    }
    ledgers = {
        **config.LEDGERS,
        "cbot": {**config.LEDGERS["cbot"], "legs": ["cbot:board", "europe:moselle"]},
    }
    with pytest.raises(ValueError, match="daily-only"):
        _reload(monkeypatch, LEDGER_LEGS=legs, LEDGERS=ledgers)


def test_a_ledger_that_does_not_pin_its_own_market_fails_the_build(monkeypatch):
    ledgers = {
        **config.LEDGERS,
        "cbot": {**config.LEDGERS["cbot"], "legs": ["brazil:paranagua", "cbot:board"]},
    }
    with pytest.raises(ValueError, match="the first leg is always the page's own"):
        _reload(monkeypatch, LEDGERS=ledgers)


def test_a_market_with_no_ledger_and_no_reason_fails_the_build(monkeypatch):
    with pytest.raises(ValueError, match="LEDGER_ABSENT_REASONS"):
        _reload(monkeypatch, LEDGER_ABSENT_REASONS={})


def test_a_new_market_must_declare_a_ledger(monkeypatch):
    """Silence is not a decision — a tenth market states its set or an explicit None."""
    ledgers = {k: v for k, v in config.LEDGERS.items() if k != "nigeria"}
    with pytest.raises(ValueError, match="declare no ledger"):
        _reload(monkeypatch, LEDGERS=ledgers)


def test_a_leg_with_no_quote_kind_fails_the_build(monkeypatch):
    """M3 constraint 4: an unlabelled quote reads as whatever its neighbours are."""
    markets = {
        **config.MARKETS,
        "cbot": {
            **config.MARKETS["cbot"],
            "basis": {k: v for k, v in config.MARKETS["cbot"]["basis"].items() if k != "quote_kind"},
        },
    }
    with pytest.raises(ValueError, match="declares no quote_kind"):
        _reload(monkeypatch, MARKETS=markets)


# ---------------------------------------------------------------------------
# The headline's eight rows
# ---------------------------------------------------------------------------
def test_the_headline_carries_one_row_per_market_in_registry_order(seeded, registry):
    state, reason, data = headline_ledger(registry, seeded)
    assert state == "ok", reason
    assert [row["market_slug"] for row in data["rows"]] == list(registry)


def test_the_headline_has_no_spread_column(seeded, registry):
    """There is no pinned own leg on the headline, so nothing to spread against."""
    _state, _reason, data = headline_ledger(registry, seeded)
    assert data["has_spread"] is False
    assert all(row["spread_usd_mt"] is None for row in data["rows"])


def test_europe_is_out_of_cadence_rather_than_dark(seeded, registry):
    """Its leg is weekly — out of the ledger's cadence, not missing.

    A `dark` pill there would report an outage that is not happening.
    """
    _state, _reason, data = headline_ledger(registry, seeded)
    europe = next(row for row in data["rows"] if row["market_slug"] == "europe")
    assert europe["state"] == LEDGER_STATE_OUT_OF_CADENCE
    assert europe["usd_mt"] is None


def test_nigeria_is_dark_with_the_registrys_own_reason(seeded, registry):
    _state, _reason, data = headline_ledger(registry, seeded)
    nigeria = next(row for row in data["rows"] if row["market_slug"] == "nigeria")
    assert nigeria["state"] == LEDGER_STATE_DARK
    assert "AFEX" in nigeria["state_detail"]


def test_every_headline_row_links_to_its_market_page(seeded, registry):
    _state, _reason, data = headline_ledger(registry, seeded)
    for row in data["rows"]:
        assert row["href"] == registry[row["market_slug"]].url
