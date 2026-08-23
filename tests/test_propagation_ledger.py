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
    ctx = SiteContext(conn=conn, today=TODAY)
    try:
        yield ctx
    finally:
        conn.close()


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


def test_rows_render_in_declared_order_even_when_a_later_leg_is_fresher(seeded, registry):
    """M20 #236: row position is role in the trade, never recency.

    On the CBOT ledger Paranaguá is declared before Argentina but is a day
    staler — the fresher print must NOT move up. Recency is carried entirely
    by the state pill and the leading-edge caption.
    """
    _state, _reason, data = ledger_block(registry["cbot"], seeded, markets=registry)
    rendered = [row["leg_id"] for row in data["rows"]]
    assert rendered == [leg.leg_id for leg in registry["cbot"].ledger.legs]

    rows = {row["leg_id"]: row for row in data["rows"]}
    # Fixture guard: the question is only posed if the earlier-declared leg
    # really is staler than the one after it.
    assert rows["brazil:paranagua"]["as_of"] < rows["argentina:fob"]["as_of"]
    assert rendered.index("brazil:paranagua") < rendered.index("argentina:fob")


def test_cbot_lands_last_on_brazil_and_argentina_as_declared(seeded, registry):
    """The issue's render check: CBOT last among the counterparts, not second."""
    for slug in ("brazil", "argentina"):
        _state, _reason, data = ledger_block(registry[slug], seeded, markets=registry)
        assert data["rows"][-1]["leg_id"] == "cbot:board", slug


def test_the_leading_edge_names_the_leg_that_produced_it(seeded, registry):
    """A caption date the reader has to attribute by scanning is half a fact."""
    _state, _reason, data = ledger_block(registry["brazil"], seeded, markets=registry)
    assert data["leading_edge"] == _day(0)
    assert data["leading_edge_legs"] == ["CBOT board (ZS front)"]


def test_the_headline_leading_edge_names_its_market(seeded, registry):
    _state, _reason, data = headline_ledger(registry, seeded)
    assert data["leading_edge"] == _day(0)
    assert data["leading_edge_legs"] == ["CBOT"]


def test_the_ledger_note_states_declared_order_and_drops_the_tie_clause(seeded, registry):
    """The old note ("ordered by print date … two legs sharing a date are not
    ordered against each other") is now false on both counts."""
    _state, _reason, data = ledger_block(registry["cbot"], seeded, markets=registry)
    note = data["no_timestamp_note"]
    assert "declared" in note and "role in the trade" in note
    assert "sharing a date" not in note
    assert "time of day" in note


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


def test_a_reference_leg_declared_mid_set_fails_the_build(monkeypatch):
    """M20 #236: with the settlement sort gone, nothing re-seats a mid-set
    reference row — so the registry must refuse to declare one."""
    ledgers = {
        **config.LEDGERS,
        "south_africa": {
            **config.LEDGERS["south_africa"],
            "legs": [
                "south_africa:safex",
                "argentina:fob",
                "cbot:board",
                "brazil:paranagua",
            ],
        },
    }
    with pytest.raises(ValueError, match="declared last"):
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
    """M3 constraint 4: an unlabelled quote reads as whatever its neighbours are.

    `_source` now rejects any price leg with no kind, so this trips at the
    descriptor rather than at the ledger and `_ledger_leg`'s own check has
    become the second line. Both are kept: the ledger states the requirement it
    depends on, and a leg that ever points at a non-price source would still
    need it.
    """
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


# ---------------------------------------------------------------------------
# The row drill-down (M21 #250)
#
# What these pin, in order of how much a bug would cost:
#
# 1. **A connecting line asserts a path that was never observed.** Under eight
#    prints the chart is dots, and under three there is no chart at all. On a
#    snapshot source publishing one number a day with holes, drawing through
#    the holes is the whole error — and it is invisible, because the line looks
#    exactly like a line drawn through real data.
# 2. **The chart reads prints, not rows.** #157 one level down: a dot is a
#    stronger claim than a table cell, because it *looks* like evidence that
#    something printed.
# 3. **India draws no reference line.** A rendering absence, and therefore
#    invisible to every other kind of check.
# 4. **The headline is untouched.** The table partial is shared, so a change
#    aimed at block 02 lands on the headline section unless something stops it.
# ---------------------------------------------------------------------------
from jinja2 import Environment, FileSystemLoader  # noqa: E402

from app.block_builders import (  # noqa: E402
    LEDGER_DRILLDOWN_DEFAULT_DAYS,
    LEDGER_DRILLDOWN_MAX_OBS,
    LEDGER_DRILLDOWN_WINDOW_DAYS,
)

TEMPLATE_DIR = Path(__file__).resolve().parents[1] / "app" / "templates"


def _template_env() -> Environment:
    return Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)


def _ledger_html(data: dict) -> str:
    """The shared table partial — the surface block 02 and the headline both use."""
    return _template_env().get_template("blocks/_ledger_table.html.j2").render(
        d=data, root="../"
    )


def _block_html(data: dict) -> str:
    """Block 02 whole, which is where the block-wide statements are rendered."""
    return _template_env().get_template("blocks/02_ledger.html.j2").render(
        block={"data": data, "id": "ledger"}, root="../"
    )


def _block(slug, seeded, registry) -> dict:
    state, reason, data = ledger_block(registry[slug], seeded, markets=registry)
    assert state == "ok", reason
    return data


def _window(drill: dict, days: int) -> dict:
    return next(w for w in drill["windows"] if w["days"] == days)


def _seed_cbot_prints(seeded, count: int, *, volume: float = 15000.0) -> None:
    """Exactly ``count`` trade-proved CBOT prints, newest today."""
    seeded.conn.execute("DELETE FROM prices WHERE commodity = 'Soybeans'")
    for offset in range(count):
        seeded.conn.execute(
            "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
            ("Soybeans", _day(offset), 1000.0 + offset, volume),
        )
    seeded.conn.commit()
    seeded._cache.clear()


def _seed_india_prints(seeded, count: int = 6) -> None:
    """Both mandi legs, inside the 7-day india_domestic budget."""
    seeded.conn.execute(
        "INSERT OR REPLACE INTO currencies (pair, Date, Close) VALUES (?,?,?)",
        ("INR/USD", _day(0), 0.0115),
    )
    for offset in range(count):
        for key, level in (("Soybean (Mandi MP)", 45000.0), ("Soybean (Mandi MH)", 46000.0)):
            seeded.conn.execute(
                "INSERT OR REPLACE INTO india_domestic_prices "
                "(Date, commodity, Close) VALUES (?,?,?)",
                (_day(offset), key, level + offset * 100),
            )
    seeded.conn.commit()
    seeded._cache.clear()


@pytest.mark.parametrize(
    ("observations", "treatment"),
    [(2, "none"), (3, "dots"), (7, "dots"), (8, "line")],
)
def test_the_three_bands_fall_at_two_three_seven_and_eight(
    observations, treatment, seeded, registry
):
    """The prototype drew a line at 3–7 prints. That is the defect, not the shape.

    A connecting line asserts a path between prints that was never observed.
    Eight is where a shape becomes readable; below three there is nothing to
    draw at all and the pane states the level and the start date instead.
    """
    _seed_cbot_prints(seeded, observations)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)["treatment"] == treatment


def test_every_band_states_where_the_history_starts_and_how_many_prints(seeded, registry):
    _seed_cbot_prints(seeded, 5)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    window = _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)
    assert window["count"] == 5
    assert window["starts"] == _day(4)


def test_a_zero_volume_row_is_not_an_observation_on_the_chart(seeded, registry):
    """#157 at chart level — and worse here, because a dot looks like evidence.

    Grain SA re-dates a carried SAFEX price with Volume 0. Six such rows must
    move the count by nothing; the leg keeps the single print that traded.
    """
    for offset in range(4, 10):
        seeded.conn.execute(
            "INSERT INTO safex_prices (Date, commodity, Close, Volume) VALUES (?,?,?,?)",
            (_day(offset), "Soybean (SAFEX)", 8000.0, 0.0),
        )
    # A rate as old as the oldest print, so this test measures the proof column
    # and nothing else — a print predating FX coverage is withheld for a
    # different reason, pinned by its own test below.
    seeded.conn.execute(
        "INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", ("ZAR/USD", _day(9), 0.055)
    )
    seeded.conn.commit()
    seeded._cache.clear()
    rows = {row["leg_id"]: row for row in _block("south_africa", seeded, registry)["rows"]}
    drill = rows["south_africa:safex"]["drill"]
    assert _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)["count"] == 1
    assert _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)["treatment"] == "none"


def test_india_draws_no_reference_line(seeded, registry):
    """The `policy_blocked` case — an absence, and so invisible to other checks.

    India's ledger names no foreign leg because no foreign bean is connected to
    a mandi by trade. A dashed series under a mandi one would render the +66%
    policy spread as a gap that closes, which is the read #222 refused.
    """
    _seed_india_prints(seeded)
    data = _block("india", seeded, registry)
    assert data["reference"] is None
    assert data["reference_note"], "an absent reference line must say why"
    html = _block_html(data)
    assert "No reference line" in html, "the absence is stated on the block, not hidden"
    # The *reason* is the ledger's own note, not restated by the drill-down: a
    # future single-market ledger could exist for a different reason entirely,
    # and a hard-coded "policy-blocked" would then be a false causal claim.
    assert "GM soybean imports" in html
    assert "policy-blocked" not in data["reference_note"]


def test_the_overlay_is_the_pages_own_pinned_leg_not_cbot(seeded, registry):
    """CEPEA on Brazil, not CBOT — and SAFEX on South Africa, whose ledger
    carries CBOT as a labelled reference row and would be the easy thing to
    reach for. CBOT is the least reliable same-day leg (M4), so pinning it
    everywhere would encode the assumption the ledger exists to correct."""
    assert _block("brazil", seeded, registry)["reference"]["leg_id"] == "brazil:cepea"
    assert _block("south_africa", seeded, registry)["reference"]["leg_id"] == (
        "south_africa:safex"
    )
    assert _block("cbot", seeded, registry)["reference"]["leg_id"] == "cbot:board"


def test_each_point_converts_at_its_own_dates_fx_rate(seeded, registry):
    """A series converted at today's rate moves the venue on days it did not.

    CEPEA is flat at BRL 2000 across both prints while BRL/USD goes 0.20 →
    0.21, so the USD series must show the currency's move and only that.
    """
    rows = {row["leg_id"]: row for row in _block("brazil", seeded, registry)["rows"]}
    drill = rows["brazil:cepea"]["drill"]
    assert drill["usd"] == [400.0, 420.0]


def test_a_usd_native_leg_gets_a_sentence_not_a_second_panel(seeded, registry):
    """M3's dual-quote rule: there is no second currency, so there is no panel."""
    rows = {row["leg_id"]: row for row in _block("argentina", seeded, registry)["rows"]}
    drill = rows["argentina:fob"]["drill"]
    assert drill["fx_pair"] is None
    assert "USD/MT" in drill["single_currency_note"]


def test_the_fx_panel_reads_the_pairs_own_series(seeded, registry):
    data = _block("brazil", seeded, registry)
    rows = {row["leg_id"]: row for row in data["rows"]}
    assert rows["brazil:cepea"]["drill"]["fx_pair"] == "BRL/USD"
    assert data["fx_series"]["BRL/USD"]["v"] == [0.20, 0.21]


def test_the_windows_are_thirty_ninety_and_a_year_never_all(seeded, registry):
    """A control labelled "all" beside a capped series is a lie about the data."""
    assert LEDGER_DRILLDOWN_WINDOW_DAYS == (30, 90, 365)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert [w["days"] for w in drill["windows"]] == [30, 90, 365]
    assert "all" not in " ".join(w["label"] for w in drill["windows"]).lower()
    assert drill["default_days"] == 90


def test_the_payload_is_capped_and_says_so_only_where_it_truncates(seeded, registry):
    """The cap bites the 1y window and leaves 30d untouched.

    A leg-level note would print "the last 260 of 320" beside a count of 31 —
    two populations in one sentence, and the smaller one is the true one.
    """
    _seed_cbot_prints(seeded, 320)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert len(drill["d"]) == LEDGER_DRILLDOWN_MAX_OBS
    assert len(drill["usd"]) == LEDGER_DRILLDOWN_MAX_OBS

    year = _window(drill, 365)
    assert year["truncated"] is True
    assert str(LEDGER_DRILLDOWN_MAX_OBS) in year["truncation_note"]
    assert "320" in year["truncation_note"]

    month = _window(drill, 30)
    assert month["truncated"] is False
    assert month["truncation_note"] is None
    assert month["count"] == 31


def test_a_window_states_where_it_starts_drawing_not_where_history_starts(
    seeded, registry
):
    """The 1y window starts at the CAP boundary, not at the leg's first print.

    Rendering that as "history starts 26 Nov" would state a payload-size
    artefact as a fact about the market — in the one block whose subject is
    what we do and do not know. The leg's own reach is carried separately.
    """
    _seed_cbot_prints(seeded, 320)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert _window(drill, 30)["starts"] == _day(30)
    assert _window(drill, 365)["starts"] == _day(LEDGER_DRILLDOWN_MAX_OBS - 1)
    # …and what the page actually reads back to is stated once, on the leg.
    assert drill["history_from"] == _day(319)
    assert drill["history_from_label"] is not None


def test_points_are_parallel_arrays_never_an_array_of_objects(seeded, registry):
    """The prototype's array-of-objects took one page from 26 KB to 89 KB."""
    _seed_cbot_prints(seeded, 30)
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert all(isinstance(day, int) for day in drill["d"])
    assert all(value is None or isinstance(value, float) for value in drill["usd"])
    assert len(drill["d"]) == len(drill["usd"])


def test_the_drilldown_payload_stays_inside_its_budget(seeded, registry):
    """A market page runs 13–19 KB today; the drill-down must not swamp it."""
    import json

    _seed_cbot_prints(seeded, 400)
    data = _block("cbot", seeded, registry)
    size = len(json.dumps(data, default=str))
    assert size < 90_000, f"the ledger block serialises to {size / 1024:.0f} KB"


# -- the collapsed row, which must not need JavaScript ----------------------
def test_the_range_band_is_rendered_in_the_row_not_by_script(seeded, registry):
    """With JS off the ledger keeps every number it states today, plus the band.

    "High in its own range" is often the entire question for a buyer scanning
    five legs, so it must not depend on a click — or on script running at all.
    """
    _seed_cbot_prints(seeded, 20)
    data = _block("cbot", seeded, registry)
    row = data["rows"][0]
    band = row["band"]
    assert band["has_range"] is True
    assert band["low"] < band["high"]
    assert band["low"] <= row["usd_mt"] <= band["high"]
    assert 0.0 <= band["position"] <= 1.0
    assert band["since"] == _day(19)
    html = _ledger_html(data)
    assert "range-band" in html
    assert band["since_label"] in html


def test_under_eight_observations_the_band_says_so_rather_than_drawing_one(
    seeded, registry
):
    """Two points are not a range; a band drawn across them invents one."""
    _seed_cbot_prints(seeded, 4)
    data = _block("cbot", seeded, registry)
    band = data["rows"][0]["band"]
    assert band["has_range"] is False
    assert band["count"] == 4
    assert "4 obs" in _ledger_html(data)
    assert "no range yet" in _ledger_html(data)


def test_a_leg_with_no_print_in_the_window_has_neither_band_nor_drilldown(
    seeded, registry
):
    """No print in the window is not a chart with nothing on it.

    Only the MP leg is seeded, so MH is dark — and a dark leg must carry no
    chart to open and no range to sit in, rather than an empty pane implying
    both exist and happen to be blank today.
    """
    _seed_india_prints(seeded)
    seeded.conn.execute("DELETE FROM india_domestic_prices WHERE commodity LIKE '%MH%'")
    seeded.conn.commit()
    seeded._cache.clear()
    rows = {row["leg_id"]: row for row in _block("india", seeded, registry)["rows"]}
    assert rows["india:mandi_mh"]["state"] == LEDGER_STATE_DARK
    assert rows["india:mandi_mh"]["drill"] is None
    assert rows["india:mandi_mh"]["band"] is None
    html = _ledger_html(_block("india", seeded, registry))
    assert html.count("drill-row") == 1, "only the leg with prints opens"


# -- the headline, which this ticket does not touch -------------------------
def test_the_headline_ledger_has_no_drilldown(seeded, registry):
    """A headline row is a MARKET, and the market cell is already the link.

    An expansion would compete with the one affordance that row exists to
    offer. The gate is an explicit flag, not `has_spread` — that is a statement
    about the spread column and tying the two together would couple two
    unrelated decisions.
    """
    _state, _reason, data = headline_ledger(registry, seeded)
    assert data["has_drilldown"] is False
    assert all(row.get("drill") is None for row in data["rows"])
    assert all(row.get("band") is None for row in data["rows"])


def test_the_headline_table_markup_carries_no_drilldown(seeded, registry):
    """The partial is shared with block 02 — this is what stops the drift."""
    _state, _reason, data = headline_ledger(registry, seeded)
    html = _ledger_html(data)
    assert "drill" not in html
    assert "range-band" not in html


def test_a_market_page_ledger_opens_one_row_at_a_time(seeded, registry):
    """Never a modal: the ledger's claim is comparative, and a modal covers the
    rows being compared against. The expansion is a row under its own row."""
    _seed_cbot_prints(seeded, 20)
    data = _block("cbot", seeded, registry)
    assert data["has_drilldown"] is True
    html = _ledger_html(data)
    assert "drill-row" in html
    assert "modal" not in html.lower()
    assert 'aria-expanded="false"' in html


# -- what the review of the first cut found, pinned so it cannot come back ---
def test_a_print_that_cannot_be_converted_is_not_an_observation(seeded, registry):
    """The count must describe what is DRAWN, not what was fetched.

    CEPEA prints ten times with no BRL/USD rate stored. Counting dates would
    caption the pane "10 observations" over a chart with nothing on it — an
    absence with no reason, which is the one thing this block must never do.
    """
    seeded.conn.execute("DELETE FROM currencies WHERE pair = 'BRL/USD'")
    for offset in range(10):
        seeded.conn.execute(
            "INSERT OR REPLACE INTO brazil_spot_prices (Date, commodity, price_brl) "
            "VALUES (?,?,?)",
            (_day(offset), "Soybean (CEPEA)", 2000.0),
        )
    seeded.conn.commit()
    seeded._cache.clear()
    rows = {row["leg_id"]: row for row in _block("brazil", seeded, registry)["rows"]}
    drill = rows["brazil:cepea"]["drill"]
    window = _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)
    assert window["count"] == 0
    assert window["treatment"] == "none"
    assert "no BRL/USD rate" in window["withheld_note"]
    # …and the collapsed row agrees with the pane, rather than the two
    # disagreeing about the same ten prints.
    assert rows["brazil:cepea"]["band"]["count"] == 0


def test_a_print_older_than_the_fx_series_is_withheld_not_back_converted(
    seeded, registry
):
    """Invariant 7: that row's own date's rate, or blank. Never a later one.

    `SiteContext.fx_on` falls back to the oldest rate it holds, which on two
    row values is a small error and on a 260-point chart is a stretch of the
    currency's movement drawn under the venue's name.
    """
    seeded.conn.execute("DELETE FROM currencies WHERE pair = 'BRL/USD'")
    seeded.conn.execute(
        "INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", ("BRL/USD", _day(1), 0.21)
    )
    seeded._cache.clear()
    rows = {row["leg_id"]: row for row in _block("brazil", seeded, registry)["rows"]}
    drill = rows["brazil:cepea"]["drill"]
    assert drill["usd"] == [None, 420.0], "the pre-coverage print was converted anyway"
    assert _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)["count"] == 1


def test_a_cents_per_bushel_leg_says_it_has_no_currency_component(seeded, registry):
    """CBOT and the Gulf are USD legs in a non-MT unit — a conversion, not a rate.

    The first cut keyed the FX panel off "not usd_per_mt", so these two rows got
    neither the panel nor the sentence: two rows in the same table treated
    differently with nothing saying why.
    """
    rows = {row["leg_id"]: row for row in _block("cbot", seeded, registry)["rows"]}
    for leg_id in ("cbot:board", "us_gulf:cif"):
        drill = rows[leg_id]["drill"]
        assert drill["fx_pair"] is None
        assert "unit conversion, not a currency one" in drill["single_currency_note"]


def test_a_future_dated_row_is_not_counted(seeded, registry):
    """The client drops anything after today; the count must too, or the caption
    claims one more observation than the chart draws."""
    _seed_cbot_prints(seeded, 8)
    seeded.conn.execute(
        "INSERT INTO prices (commodity, Date, Close, Volume) VALUES (?,?,?,?)",
        ("Soybeans", _day(-3), 1200.0, 15000.0),
    )
    seeded.conn.commit()
    seeded._cache.clear()
    drill = _block("cbot", seeded, registry)["rows"][0]["drill"]
    assert _window(drill, LEDGER_DRILLDOWN_DEFAULT_DAYS)["count"] == 8


def test_the_flag_alone_turns_the_drilldown_off(seeded, registry):
    """`has_drilldown: False` must be sufficient — no markup may reach a surface
    that did not ask for it, whatever the rows happen to carry."""
    _seed_cbot_prints(seeded, 20)
    data = _block("cbot", seeded, registry)
    off = _ledger_html({**data, "has_drilldown": False})
    stripped = _ledger_html({
        **data,
        "has_drilldown": False,
        "rows": [{**row, "drill": None, "band": None} for row in data["rows"]],
    })
    assert off == stripped
    for artefact in ("drill", "range-band", "aria-expanded"):
        assert artefact not in off
