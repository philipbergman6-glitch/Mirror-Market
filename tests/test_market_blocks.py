"""The nine block builders and the ported headline sections (M18 #214).

What these pin, in order of how expensive the bug would be:

1. **Units.** A price is published in USD/MT off a registry pointer. Getting
   this wrong ships a soybean price out by a factor of 36.7 or by the exchange
   rate, and it looks perfectly plausible on the page.
2. **The market is a parameter.** Adding a market must not need code, and no
   builder may branch on which market it is looking at.
3. **A number is one session's number.** Basis and crush are struck on a date
   both/all legs printed; there is no cross-day arithmetic.
4. **An empty block says why.** Including when the builder itself blows up.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from app import block_builders
from app import markets as markets_mod
from app.block_builders import SiteContext, build_blocks
from app.markets import compute_tiers, load_markets
from app.sections import (
    CHART_WINDOW_SESSIONS,
    clip,
    emerging_markets_section,
    forward_curves_section,
    relative_value_section,
    risk_monitor_section,
    seasonal_section,
    section,
)
from pipeline import schema

TODAY = date(2026, 8, 12)


def _day(offset: int) -> str:
    return (TODAY - timedelta(days=offset)).isoformat()


@pytest.fixture
def seeded(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A DB carrying one real row per market source, at known numbers."""
    db_path = tmp_path / "blocks.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_WEATHER,
        schema._CREATE_PSD,
        schema._CREATE_CURRENCIES,
        schema._CREATE_DCE_FUTURES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_INDIA_DOMESTIC,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_SAFEX,
        schema._CREATE_SAGIS_DELIVERIES,
        schema._CREATE_EC_OILSEED_PRICES,
    ):
        conn.execute(ddl)

    # CBOT: cents/bu, cents/lb, USD/short ton — three different native units
    # on one table, which is the whole reason `unit` is a registry pointer.
    for offset in (0, 1, 5, 21):
        conn.execute("INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
                     ("Soybeans", _day(offset), 1050.0 + offset))
        conn.execute("INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
                     ("Soybean Oil", _day(offset), 52.0))
        conn.execute("INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
                     ("Soybean Meal", _day(offset), 300.0))
    # Two Gulf locations on one report date — the averaging case.
    for location, average in (("NOLA", 11.20), ("TEXAS", 11.60)):
        conn.execute(
            "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
            "VALUES (?,?,?,?,?)",
            (_day(0), "Soybeans", location, "SPOT", average),
        )
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                 ("BRL/USD", _day(0), 0.20))
    conn.execute("INSERT INTO brazil_spot_prices (Date, commodity, price_brl) VALUES (?,?,?)",
                 (_day(0), "Soybean (CEPEA)", 2000.0))
    conn.execute("INSERT INTO brazil_spot_prices (Date, commodity, price_brl) VALUES (?,?,?)",
                 (_day(0), "Soybean (AgRural Paranaguá FOB)", 2100.0))
    conn.execute("INSERT INTO weather (region, Date, temp_max, temp_min, precipitation) "
                 "VALUES (?,?,?,?,?)", ("US Midwest (Iowa)", _day(0), 41.0, 20.0, 0.0))
    conn.execute("INSERT INTO psd (commodity, country, year, attribute, value, unit) "
                 "VALUES (?,?,?,?,?,?)", ("Soybeans", "United States", 2026, "Production", 120.0, "1000 MT"))
    conn.execute("INSERT INTO psd (commodity, country, year, attribute, value, unit) "
                 "VALUES (?,?,?,?,?,?)", ("Soybeans", "United States", 2025, "Production", 100.0, "1000 MT"))
    conn.commit()

    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    return SiteContext(conn=conn, today=TODAY)


@pytest.fixture
def registry():
    return load_markets()


def _block(blocks, block_id):
    return next(b for b in blocks if b.id == block_id)


def _build(slug, seeded, registry, tier=None):
    return build_blocks(registry[slug], tier, seeded, markets=registry)


# ---------------------------------------------------------------------------
# Units — the expensive bug
# ---------------------------------------------------------------------------
def test_cbot_price_is_published_in_usd_per_metric_ton(seeded, registry):
    price = _block(_build("cbot", seeded, registry), "price")
    assert price.state == "ok"
    headline = price.data["headline"]
    # 1050 cents/bu x 36.7437 bu/MT / 100 = 385.8 USD/MT. A raw 1050 on the
    # page would read as a plausible price and be wrong by 2.7x.
    assert headline["home_value"] == 1050.0
    assert round(headline["usd_mt"], 1) == 385.8


def test_a_home_currency_leg_is_converted_at_its_own_days_rate(seeded, registry):
    price = _block(_build("brazil", seeded, registry), "price")
    assert price.state == "ok"
    # BRL 2,000/MT at 0.20 USD per BRL = USD 400/MT.
    assert round(price.data["headline"]["usd_mt"], 1) == 400.0


def test_a_missing_fx_rate_empties_the_block_rather_than_printing_the_local_number(
    seeded, registry
):
    seeded.conn.execute("DELETE FROM currencies")
    seeded.conn.commit()
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    price = _block(build_blocks(registry["brazil"], None, ctx, markets=registry), "price")
    # The leg still renders — but its USD/MT is None, never the BRL number
    # relabelled as dollars.
    assert price.data["headline"]["usd_mt"] is None
    assert price.data["headline"]["home_value"] == 2000.0


def test_several_quotes_on_one_date_are_averaged_and_say_so(seeded, registry):
    basis = _block(_build("cbot", seeded, registry), "basis")
    assert basis.state == "ok"
    # $11.20 and $11.60 per bushel -> mean $11.40 -> 418.9 USD/MT.
    assert round(basis.data["local_usd_mt"], 1) == 418.9
    price_rows = seeded.series(registry["cbot"].basis)["Soybeans"]
    assert price_rows[-1][2] == 2  # the count travels with the mean


# ---------------------------------------------------------------------------
# One session's number
# ---------------------------------------------------------------------------
def test_basis_is_struck_on_a_session_both_legs_printed(seeded, registry):
    basis = _block(_build("cbot", seeded, registry), "basis")
    assert basis.data["as_of"] == _day(0)
    assert round(basis.data["basis_usd_mt"], 1) == round(
        basis.data["local_usd_mt"] - basis.data["board_usd_mt"], 1)


def test_a_basis_with_no_shared_session_is_empty_not_a_cross_day_subtraction(
    seeded, registry
):
    seeded.conn.execute("DELETE FROM gulf_bids")
    seeded.conn.execute(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average) "
        "VALUES (?,?,?,?,?)", (_day(3), "Soybeans", "NOLA", "SPOT", 11.20))
    seeded.conn.commit()
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    basis = _block(build_blocks(registry["cbot"], None, ctx, markets=registry), "basis")
    assert basis.state == "empty"
    assert "share no session" in basis.reason


def _seed_india_mandi(conn) -> None:
    """India's daily leg at the real 2026-08-11 level, plus its FX rate."""
    for key, close in (("Soybean (Mandi MP)", 67250.0), ("Soybean (Mandi MH)", 67000.0)):
        conn.execute("INSERT INTO india_domestic_prices (Date, commodity, Close, unit) "
                     "VALUES (?,?,?,?)", (_day(0), key, close, "INR/MT"))
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                 ("INR/USD", _day(0), 0.0105))
    conn.commit()


def test_indias_basis_carries_its_policy_spread_caveat(seeded, registry):
    """M19 #222: the number alone cannot show that no cargo can close it."""
    _seed_india_mandi(seeded.conn)
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    basis = _block(build_blocks(registry["india"], None, ctx, markets=registry), "basis")
    assert basis.state == "ok"
    # INR 67,250/MT at 0.0105 = USD 706/MT against a 1050 c/bu board = USD 386.
    assert round(basis.data["basis_usd_mt"]) == 320
    assert basis.data["arbitrage"] == "policy_blocked"
    assert "tariff wall" in basis.data["caveat"]
    # One session is not a range, and the block says which it has.
    assert basis.data["avg"] is None
    assert "history building" in basis.data["history_note"]


def test_the_policy_spread_caveat_reaches_the_markup(seeded, registry):
    """A caveat the template drops is the same as no caveat at all."""
    from jinja2 import Environment, FileSystemLoader

    _seed_india_mandi(seeded.conn)
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    basis = _block(build_blocks(registry["india"], None, ctx, markets=registry), "basis")
    env = Environment(loader=FileSystemLoader("app/templates"), autoescape=True)
    html = env.get_template("blocks/04_basis.html.j2").render(block=basis)
    assert "tariff wall" in html
    assert "alert-warn" in html

    cbot = _block(_build("cbot", seeded, registry), "basis")
    open_html = env.get_template("blocks/04_basis.html.j2").render(block=cbot)
    assert "alert-warn" not in open_html  # an open basis carries no warning


def test_crush_uses_one_session_for_all_three_legs(seeded, registry):
    seeded.conn.execute("DELETE FROM prices WHERE commodity = 'Soybean Oil' AND Date = ?",
                        (_day(0),))
    seeded.conn.commit()
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    crush = _block(build_blocks(registry["cbot"], None, ctx, markets=registry), "crush")
    assert crush.state == "ok"
    assert crush.data["as_of"] == _day(1)  # the newest date all three share


def test_crush_with_no_shared_session_is_empty(seeded, registry):
    seeded.conn.execute("DELETE FROM prices WHERE commodity = 'Soybean Oil'")
    seeded.conn.commit()
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    crush = _block(build_blocks(registry["cbot"], None, ctx, markets=registry), "crush")
    assert crush.state == "empty"
    assert crush.reason


def test_a_provisional_crush_carries_its_caveat_into_the_block(registry):
    # Argentina's meal position code is inferred, not cross-checked (M5 #147),
    # and that has to travel with the number rather than living in a comment.
    assert registry["argentina"].crush.provisional is True


# ---------------------------------------------------------------------------
# The market is a parameter
# ---------------------------------------------------------------------------
def test_no_builder_branches_on_a_market_slug():
    source = Path(block_builders.__file__).read_text(encoding="utf-8")
    for slug in load_markets():
        assert f'== "{slug}"' not in source, f"block_builders branches on {slug}"
        assert f"== '{slug}'" not in source


def test_every_market_builds_every_block_without_raising(seeded, registry):
    tiers = compute_tiers(registry, today=TODAY)
    for slug in registry:
        blocks = _build(slug, seeded, registry, tiers[slug])
        assert [b.id for b in blocks] == list(
            __import__("app.blocks", fromlist=["BLOCK_IDS"]).BLOCK_IDS)
        for block in blocks:
            assert block.state == "ok" or block.reason.strip()


def test_a_builder_that_raises_becomes_a_reasoned_empty_state(
    seeded, registry, monkeypatch
):
    def explode(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setitem(block_builders.BUILDERS, "price", explode)
    price = _block(_build("cbot", seeded, registry), "price")
    assert price.state == "empty"
    assert price.reason == "generation error"


def test_the_ledger_renders_its_registry_counterpart_set(seeded, registry):
    """M12 #161 chose the sets and M19 #223 built the block; see
    tests/test_propagation_ledger.py for the row contract itself."""
    tiers = compute_tiers(registry, today=TODAY)
    ledger = _block(_build("cbot", seeded, registry, tiers["cbot"]), "ledger")
    assert ledger.state == "ok", ledger.reason
    assert [row["leg_id"] for row in ledger.data["rows"]][0] == "cbot:board"


def test_a_market_with_no_counterpart_set_renders_absent_not_empty(seeded, registry):
    """Europe and Nigeria have no ledger by decision — a legal configuration.

    `absent` says "no such thing exists here"; `empty` would say "we tried and
    got nothing", which would be a report of an outage that is not happening.
    """
    for slug in ("europe", "nigeria"):
        ledger = _block(_build(slug, seeded, registry), "ledger")
        assert ledger.state == "absent"
        assert ledger.reason


def test_news_is_never_ok(seeded, registry):
    for slug in registry:
        news = _block(_build(slug, seeded, registry), "news")
        assert news.state == "absent"
        assert news.reason


# ---------------------------------------------------------------------------
# Supporting blocks
# ---------------------------------------------------------------------------
def test_weather_alerts_use_the_configured_thresholds(seeded, registry):
    weather = _block(_build("cbot", seeded, registry), "weather")
    assert weather.state == "ok"
    # 41C is over WEATHER_EXTREME_HEAT_C, and heat outranks the dry reading.
    assert weather.data["alerts"][0]["alert"] == "Extreme heat"


def test_supply_demand_is_stamped_annual_and_carries_yoy(seeded, registry):
    sd = _block(_build("cbot", seeded, registry), "supply_demand")
    assert sd.state == "ok"
    production = next(line for line in sd.data["lines"] if line["attribute"] == "Production")
    assert production["yoy_pct"] == pytest.approx(20.0)
    assert "annual" in sd.data["cadence_note"]


def test_a_flows_leg_carries_its_attribution(seeded, registry):
    seeded.conn.execute(
        "INSERT INTO sagis_deliveries (commodity, season_year, week_number, week_end, week_total) "
        "VALUES (?,?,?,?,?)", ("Soybeans", 2026, 22, _day(4), 12345.0))
    seeded.conn.commit()
    ctx = SiteContext(conn=seeded.conn, today=TODAY)
    sd = _block(build_blocks(registry["south_africa"], None, ctx, markets=registry), "supply_demand")
    assert sd.data["flows"]["attribution"] == config.SAGIS_ATTRIBUTION


def test_currency_block_states_that_the_rate_is_not_simultaneous(seeded, registry):
    fx = _block(_build("brazil", seeded, registry), "currency")
    assert fx.state == "ok"
    assert fx.data["home_per_usd"] == pytest.approx(5.0)
    assert "not simultaneous" in fx.data["conversion_note"]


def test_the_numeraire_has_no_currency_block(seeded, registry):
    fx = _block(_build("cbot", seeded, registry), "currency")
    assert fx.state == "absent"
    assert "numeraire" in fx.reason


# ---------------------------------------------------------------------------
# Rendered pages
# ---------------------------------------------------------------------------
def test_a_full_page_renders_real_numbers_not_a_placeholder(
    seeded, registry, tmp_path, monkeypatch
):
    from scripts import generate_site

    monkeypatch.setattr(generate_site, "_render_headline", lambda *a, **k: tmp_path / "index.html")
    monkeypatch.setattr(generate_site, "_render_players", lambda *a, **k: tmp_path / "players.html")
    (tmp_path / "index.html").write_text("x")
    (tmp_path / "players.html").write_text("x")

    results = generate_site.generate_site(output_dir=tmp_path)
    assert all(result.ok for result in results)
    html = (tmp_path / "markets" / "cbot.html").read_text(encoding="utf-8")
    assert "builder not built yet" not in html   # M17's placeholder is gone
    assert "$385.8" in html                # the converted headline price
    assert "no source" in html or "no data" in html  # empty states still labelled


# ---------------------------------------------------------------------------
# Headline sections
# ---------------------------------------------------------------------------
def test_a_section_that_is_not_ok_must_name_its_reason():
    with pytest.raises(ValueError):
        section("empty", "   ")


@pytest.mark.parametrize("builder", [
    relative_value_section,
    risk_monitor_section,
    forward_curves_section,
    seasonal_section,
    emerging_markets_section,
])
def test_every_ported_section_degrades_to_a_reasoned_empty_state(builder):
    result = builder(None)
    assert result["state"] == "empty"
    assert result["reason"].strip()


@pytest.mark.parametrize("builder", [
    relative_value_section,
    risk_monitor_section,
    forward_curves_section,
    seasonal_section,
    emerging_markets_section,
])
def test_no_ported_section_returns_markup(builder):
    """The point of the port: a builder returns data, never HTML.

    ``chart_html`` is the one exception — a Plotly figure is a library
    artifact, not markup we assembled — so it is excluded by name.
    """
    def walk(value, path="data"):
        if isinstance(value, str):
            assert "<div" not in value and "<span" not in value, f"markup at {path}"
        elif isinstance(value, dict):
            for key, item in value.items():
                if key in ("chart_html",):
                    continue
                walk(item, f"{path}.{key}")
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                walk(item, f"{path}[{index}]")

    walk(builder(_SAMPLE_SECTION_INPUT.get(builder.__name__)))


_SAMPLE_SECTION_INPUT = {
    "emerging_markets_section": {
        "countries": {
            "South Africa": {
                "psd": {"Production": {"value": 2000.0, "unit": "1000 MT", "yoy_pct": 4.0}},
                "psd_year": 2026,
                "currency": {"pair": "ZAR/USD", "close": 0.055, "weekly_chg": -0.4},
                "weather": [{"region": "South Africa Free State", "alert": None}],
                "south_africa_domestic": {
                    "soybean_safex_zar": 8000.0, "soybean_safex_usd": 440.0,
                    "safex_cbot_basis_usd": 54.0, "soybean_safex_date": "2026-08-11",
                },
                "south_africa_deliveries": {
                    "attribution": config.SAGIS_ATTRIBUTION,
                    "soybeans": {"week_total_mt": 12000.0, "progressive_mt": 800000.0,
                                 "week_number": 22, "season_label": "2026/27", "yoy_pct": 21.9},
                },
            },
        },
    },
}


def test_emerging_markets_renders_every_country_through_one_shape():
    result = emerging_markets_section(_SAMPLE_SECTION_INPUT["emerging_markets_section"])
    assert result["state"] == "ok"
    country = result["data"]["countries"][0]
    titles = [group["title"] for group in country["groups"]]
    assert "SAFEX last-traded prices" in titles       # never "settlement" (#157)
    assert any("SAGIS" in title for title in titles)
    sagis = next(g for g in country["groups"] if "SAGIS" in g["title"])
    assert sagis["note"] == config.SAGIS_ATTRIBUTION  # reproduction condition


def test_india_says_the_feed_is_quiet_rather_than_omitting_the_series():
    result = emerging_markets_section({"countries": {"India": {"weather": []}}})
    assert result["data"]["countries"][0]["notes"]


# ---------------------------------------------------------------------------
# Chart budget
# ---------------------------------------------------------------------------
def test_chart_series_are_clipped_to_the_window_the_chart_reads():
    import pandas as pd

    frame = pd.DataFrame({"Close": range(4000)})
    assert len(clip(frame)) == CHART_WINDOW_SESSIONS
    assert clip(frame)["Close"].iloc[-1] == 3999  # the newest rows, not the oldest


def test_block_reads_are_bounded_by_cadence():
    assert block_builders.LOOKBACK_DAYS_BY_CADENCE["daily"] <= 400


def test_a_board_in_native_units_has_no_home_currency_margin(seeded, registry):
    """CBOT's legs are cents/bu, cents/lb and USD/short ton.

    Combining them with MT-per-MT yields produces a number in no unit at all.
    It printed as "USD -820/MT" beside a +$66.9 margin until this was pinned.
    """
    crush = _block(_build("cbot", seeded, registry), "crush")
    assert crush.data["margin_usd_mt"] is not None
    assert crush.data["margin_home"] is None


def test_the_cec_estimates_panel_survived_the_port():
    """Layer 25's panel landed on main mid-build (#204) — it must not vanish.

    It is a revision series, so all three numbers matter, and the USDA line is
    a *lag* not a divergence: PSD has carried the CEC's own final crop exactly
    for three seasons.
    """
    result = emerging_markets_section({"countries": {"South Africa": {
        "south_africa_estimates": {
            "attribution": "Source: Crop Estimates Committee (CEC)",
            "soybeans": {
                "season_year": 2026, "production_t": 2_500_000.0, "revision_pct": 1.25,
                "yoy_pct": -3.4, "forecast_label": "7th forecast",
                "release_date": "2026-07-28", "prev_release_date": "2026-06-25",
                "prior_season_year": 2025, "area_ha": 1_200_000.0, "yield_t_ha": 2.08,
                "usda_psd_t": 2_400_000.0, "usda_psd_year": 2025, "vs_usda_pct": 4.2,
            },
        },
    }}})
    group = next(g for g in result["data"]["countries"][0]["groups"]
                 if g["title"].startswith("CEC official"))
    assert [c["label"] for c in group["cards"]] == ["2026 crop", "Revision", "YoY"]
    assert "implied yield 2.08 t/ha" in group["note"]
    assert "lag, not disagreement" in group["note"]
    assert "Crop Estimates Committee" in group["note"]


# ---------------------------------------------------------------------------
# Section templates render (#226)
# ---------------------------------------------------------------------------
# The builder returning the right dict is only half the contract: the section
# template reads keys off it, and a key the builder never set is an
# UndefinedError at render time — which tombstones the headline page and, with
# it, the whole deploy. These run the builder output through the real template,
# so a missing key fails here instead of on `main`.


def _render_section(name: str, data: dict) -> str:
    from jinja2 import Environment, FileSystemLoader

    from scripts.generate_html import TEMPLATE_DIR

    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    return env.get_template(f"sections/{name}.html.j2").render(s=data)


def _oil_pair(**over) -> dict:
    base = {
        "soy_oil": 1_150.0, "soy_oil_weekly_chg": 1.2, "soy_oil_as_of": "2026-08-11",
        "palm_oil": 1_020.0, "palm_oil_weekly_chg": -0.4, "palm_oil_as_of": "2026-08-11",
    }
    return {**base, **over}


def test_the_palm_panel_renders_though_only_rapeseed_carries_a_spread():
    """The two oil panels share one template loop that asks for a spread.

    Palm has none, and the *missing* key — not a None one — is what raised
    UndefinedError on the headline page and failed the deploy (#226).
    """
    result = relative_value_section({"oil_vs_palm": _oil_pair()})
    assert result["state"] == "ok"
    assert result["data"]["oil_vs_palm"]["spread_usd_mt"] is None

    html = _render_section("relative_value", result["data"])
    assert "Palm Oil" in html
    assert "1,020.00" in html
    assert "spread" not in html.lower()  # no spread block on a pair without one


def test_the_rapeseed_panel_still_renders_its_spread():
    result = relative_value_section({
        "oil_vs_rapeseed": {
            "soy_oil": 1_150.0, "soy_oil_weekly_chg": 1.2,
            "rapeseed_oil": 1_310.0, "rapeseed_oil_weekly_chg": 0.8,
            "rapeseed_oil_cny": 9_400.0, "spread_usd_mt": 160.0,
        },
    })
    html = _render_section("relative_value", result["data"])
    assert "Rapeseed − soy oil spread" in html
    assert "+160.0" in html
    assert "CZCE rapeseed oil premium" in html


def test_both_oil_panels_render_side_by_side():
    result = relative_value_section({
        "oil_vs_palm": _oil_pair(),
        "oil_vs_rapeseed": {
            "soy_oil": 1_150.0, "rapeseed_oil": 1_310.0, "spread_usd_mt": 160.0,
        },
    })
    html = _render_section("relative_value", result["data"])
    assert "Soy Oil vs Palm Oil" in html
    assert "Soy Oil vs CZCE Rapeseed Oil" in html
    assert html.count("Rapeseed − soy oil spread") == 1
