"""Contract tests for the multi-page site skeleton (M17 #213, contract from M8 #150).

These are the four assertions M8 said pin the contract, plus the registry and
tier-rule tests M1 #143's "tier is computed from the DB" constraint needs:

1. Every full page emits all nine block ids in the fixed order.
2. Every non-``ok`` block carries a non-empty reason.
3. Every registered market produces exactly one file at its registry-derived URL.
4. Every page is under the size budget.

Golden-file snapshots per page were rejected upstream — a golden file for a
page whose numbers move daily is a test that gets deleted.
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from app import blocks as blocks_mod
from app import markets as markets_mod
from app.block_builders import SiteContext, build_blocks
from app.blocks import BLOCK_IDS, Block, make_block
from pipeline import schema
from scripts import generate_site

# The nav / ledger order declared once in config.MARKETS and consumed by both
# the masthead nav and M2's headline ledger. Pinned here because "declared once,
# two consumers" only holds if a reorder has to be deliberate.
EXPECTED_ORDER = [
    "cbot", "dalian", "brazil", "argentina",
    "india", "europe", "south_africa", "nigeria",
]


# ---------------------------------------------------------------------------
# Fixture DB
# ---------------------------------------------------------------------------
@pytest.fixture
def site_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    """Empty DB with the full schema, wired into app.markets' tier probe."""
    db_path = tmp_path / "site.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_WEATHER,
        schema._CREATE_DCE_FUTURES,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_INDIA_DOMESTIC,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_SAFEX,
        schema._CREATE_SAGIS_DELIVERIES,
        schema._CREATE_FORWARD_CURVE,
    ):
        conn.execute(ddl)
    conn.commit()

    monkeypatch.setattr(markets_mod, "get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    try:
        yield conn
    finally:
        conn.close()


def _seed_prices(
    conn,
    keys,
    *,
    days_ago: int = 0,
    table="prices",
    key_col="commodity",
    date_col="Date",
    extra: dict | None = None,
):
    """Insert one dated row per key. Only the key and date columns matter here —
    the tier probe reads MAX(date) and nothing else."""
    stamp = (date.today() - timedelta(days=days_ago)).isoformat()
    extra = extra or {}
    cols = [key_col, date_col, *extra]
    placeholders = ",".join("?" for _ in cols)
    for key in keys:
        conn.execute(
            f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) VALUES ({placeholders})",  # noqa: S608
            (key, stamp, *extra.values()),
        )
    conn.commit()


def _seed_cbot_page(conn) -> None:
    """Give CBOT a daily leg plus crush, basis and weather — a full page."""
    _seed_prices(conn, ["Soybeans", "Soybean Oil", "Soybean Meal"])
    # CBOT's crush block reads NAMED contracts out of `forward_curve`, not the
    # continuous front-month series in `prices` — so the tier probe does too,
    # and a page that has the series but no curve is genuinely a brief.
    _seed_prices(
        conn,
        ["Soybeans", "Soybean Oil", "Soybean Meal"],
        table="forward_curve",
        date_col="observation_date",
        extra={"contract_month": "2026-09-01", "fetched_date": date.today().isoformat()},
    )
    _seed_prices(
        conn,
        ["Soybeans"],
        table="gulf_bids",
        key_col="commodity",
        date_col="report_date",
        extra={"location": "NOLA", "delivery": "SPOT"},
    )
    _seed_prices(conn, ["US Midwest (Iowa)", "US Illinois"], table="weather", key_col="region")


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------
def test_registry_loads_and_validates():
    markets = markets_mod.load_markets()
    assert list(markets) == EXPECTED_ORDER


def test_urls_are_derived_from_slugs_and_unique():
    markets = markets_mod.load_markets()
    urls = [m.url for m in markets.values()]
    assert urls == [f"markets/{slug}.html" for slug in EXPECTED_ORDER]
    assert len(set(urls)) == len(urls)


def test_registry_holds_pointers_not_values():
    """No descriptor may carry a price, a tier or a date (M8 constraint 5)."""
    forbidden = {"tier", "price_usd", "close", "value", "as_of", "last_updated"}
    for slug, raw in config.MARKETS.items():
        assert not (forbidden & raw.keys()), f"{slug} descriptor holds a value, not a pointer"


def test_every_absent_block_names_a_reason():
    """A None source without a reason is rejected at load (M1 constraint 2)."""
    markets = markets_mod.load_markets()
    for market in markets.values():
        for block in ("price", "crush", "basis"):
            if getattr(market, block) is None:
                assert market.absent_reason(block).strip()


def test_unknown_weather_region_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    broken = {"cbot": dict(
        config.MARKETS["cbot"],
        weather_regions=[("Mars Olympus Mons", "domestic crop")],
    )}
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="not in GROWING_REGIONS"):
        markets_mod.load_markets()


def test_a_weather_pin_without_a_role_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    """M14 #207: every pin says why it is on this page, or the build stops.

    Europe's pins are rapeseed and Dalian's second pin is in Brazil — an
    unlabelled pin renders as this market's own soy crop, which is a wrong
    number wearing a region's name.
    """
    broken = {"cbot": dict(config.MARKETS["cbot"], weather_regions=["US Illinois"])}
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="not a \\(region, role\\) pair"):
        markets_mod.load_markets()

    blank = {"cbot": dict(config.MARKETS["cbot"], weather_regions=[("US Illinois", "  ")])}
    monkeypatch.setattr(config, "MARKETS", blank)
    with pytest.raises(ValueError, match="empty role label"):
        markets_mod.load_markets()


def test_every_basis_says_whether_arbitrage_connects_its_two_legs():
    """M19 #222: a spread no cargo can close must not read like a workable basis."""
    markets = markets_mod.load_markets()
    for market in markets.values():
        if market.basis is None:
            continue
        assert market.basis.arbitrage in markets_mod.ARBITRAGE_KINDS, market.slug
        if market.basis.arbitrage == "policy_blocked":
            assert market.basis.caveat.strip(), market.slug


def test_unknown_arbitrage_kind_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    raw = config.MARKETS["cbot"]
    broken = {"cbot": dict(raw, basis=dict(raw["basis"], arbitrage="probably fine"))}
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="arbitrage"):
        markets_mod.load_markets()


def test_basis_without_an_arbitrage_verdict_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    """Omission must not default to 'open' — that is the unlabelled-spread ship."""
    raw = config.MARKETS["cbot"]
    basis = {k: v for k, v in raw["basis"].items() if k != "arbitrage"}
    monkeypatch.setattr(config, "MARKETS", {"cbot": dict(raw, basis=basis)})
    with pytest.raises(ValueError, match="must declare arbitrage"):
        markets_mod.load_markets()


def test_policy_blocked_basis_without_a_caveat_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    raw = config.MARKETS["india"]
    broken = {"india": dict(raw, basis=dict(raw["basis"], caveat="  "))}
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="must carry a caveat"):
        markets_mod.load_markets()


def test_india_basis_is_labelled_a_policy_spread():
    """#206 validated the level; #222 decided it renders only with its framing."""
    india = markets_mod.load_markets()["india"]
    assert india.basis is not None
    assert india.basis.arbitrage == "policy_blocked"
    assert "policy spread" in india.basis.label.lower()
    # Struck on the MP median alone — the price block's own headline key.
    assert india.basis.keys == ("Soybean (Mandi MP)",)


def test_unknown_quote_kind_fails_the_build(monkeypatch: pytest.MonkeyPatch):
    raw = config.MARKETS["cbot"]
    broken = {"cbot": dict(raw, price=dict(raw["price"], quote_kind="vibes"))}
    monkeypatch.setattr(config, "MARKETS", broken)
    with pytest.raises(ValueError, match="quote_kind"):
        markets_mod.load_markets()


def test_every_price_leg_names_its_quote_kind():
    """Basis legs as much as price legs (M3 #145 constraint 4).

    The basis legs shipped unkinded because block 01, the only reader at the
    time, renders one number of one animal and stamps it in the block header.
    Two later consumers put the same descriptor beside a board price — the
    ledger's shared USD/MT column and block 04's Local-vs-CBOT pair — and each
    had to discover the omission again.
    """
    for market in markets_mod.load_markets().values():
        for block in ("price", "basis", "flows"):
            source = getattr(market, block)
            if source is not None and source.is_price:
                assert source.quote_kind in markets_mod.QUOTE_KINDS, (
                    f"{market.slug} {block} is a price leg with no quote_kind"
                )


def test_a_price_leg_with_no_quote_kind_fails_at_the_descriptor(
    monkeypatch: pytest.MonkeyPatch,
):
    """Enforced on the descriptor, not once per consumer that renders it."""
    raw = config.MARKETS["argentina"]
    stripped = {k: v for k, v in raw["basis"].items() if k != "quote_kind"}
    monkeypatch.setattr(config, "MARKETS", {"argentina": dict(raw, basis=stripped)})
    with pytest.raises(ValueError, match="declares no quote_kind"):
        markets_mod.load_markets()


def test_a_volume_leg_needs_no_quote_kind():
    """`tonnes` and `observation` are not prices and must not invent a kind."""
    flows = markets_mod.load_markets()["south_africa"].flows
    assert flows is not None and not flows.is_price
    assert flows.quote_kind is None


# ---------------------------------------------------------------------------
# Tier rule — computed from the DB, never hard-coded (M1 constraint 3)
# ---------------------------------------------------------------------------
def test_empty_db_makes_every_market_a_stub(site_db):
    tiers = markets_mod.compute_tiers()
    assert {t.tier for t in tiers.values()} == {"stub"}


def test_daily_leg_plus_three_supporting_blocks_is_a_page(site_db):
    _seed_cbot_page(site_db)
    tiers = markets_mod.compute_tiers()
    assert tiers["cbot"].tier == "page"
    assert tiers["cbot"].has_daily_leg
    assert set(tiers["cbot"].present) == {"ledger", "crush", "basis", "weather"}


def test_daily_leg_with_too_little_around_it_is_a_brief(site_db):
    _seed_prices(site_db, ["Soybeans", "Soybean Oil", "Soybean Meal"])
    tiers = markets_mod.compute_tiers()
    # ledger + crush only — two supporting blocks, short of the three a page needs.
    assert tiers["cbot"].tier == "brief"


def test_no_daily_leg_with_two_blocks_is_a_brief(site_db):
    """Nigeria has no price leg at all; weather alone is one block, not two."""
    _seed_prices(site_db, ["Nigeria Benue", "Nigeria Kaduna"], table="weather", key_col="region")
    tiers = markets_mod.compute_tiers()
    assert tiers["nigeria"].tier == "stub"
    assert tiers["nigeria"].present == ("weather",)


def test_a_stale_leg_does_not_count_as_present(site_db):
    """Rows existing is not rows being current — the layer's own budget applies."""
    _seed_cbot_page(site_db)
    fresh = markets_mod.compute_tiers()
    assert fresh["cbot"].tier == "page"

    site_db.execute("DELETE FROM prices")
    site_db.execute("DELETE FROM forward_curve")
    _seed_prices(site_db, ["Soybeans", "Soybean Oil", "Soybean Meal"], days_ago=90)
    # The crush is a *second* source now (named contracts, not the price
    # series), so ageing the price leg alone would leave it standing — which is
    # correct, and is why both are aged here.
    _seed_prices(
        site_db,
        ["Soybeans", "Soybean Oil", "Soybean Meal"],
        days_ago=90,
        table="forward_curve",
        date_col="observation_date",
        extra={"contract_month": "2026-09-01", "fetched_date": "2026-05-01"},
    )
    stale = markets_mod.compute_tiers()
    assert not stale["cbot"].has_daily_leg
    assert "crush" not in stale["cbot"].present
    assert stale["cbot"].tier == "brief"  # basis + weather still current


def _seed_india(conn, *, days_ago: int = 0) -> None:
    """India's whole stack: one mandi leg (which is also its basis) + weather."""
    _seed_prices(
        conn,
        ["Soybean (Mandi MP)", "Soybean (Mandi MH)"],
        days_ago=days_ago,
        table="india_domestic_prices",
    )
    _seed_prices(
        conn,
        ["India Madhya Pradesh", "India Maharashtra"],
        days_ago=days_ago,
        table="weather",
        key_col="region",
    )


def test_india_is_a_page_once_its_basis_line_is_restored(site_db):
    """M19 #222: ledger + basis + weather is three, and M1 forecast India a page.

    India has no crush (mandi is bean-only), so the basis line #206 unblocked is
    the difference between a brief and a page.
    """
    _seed_india(site_db)
    # `today` is pinned to the same clock the seed used: compute_tiers defaults
    # to the UTC date, which is a day behind local for part of every day, and a
    # tier test that swings on the tester's timezone is a flake.
    tier = markets_mod.compute_tiers(today=date.today())["india"]
    assert tier.tier == "page"
    assert set(tier.present) == {"ledger", "basis", "weather"}


def test_india_demotes_when_the_mandi_feed_goes_quiet(site_db):
    """8 days > the 7-day india_domestic budget: two of three blocks are the leg.

    The tier is computed from the DB every run (M1 constraint 3), so a dark
    scraper takes the page down to a stub at the same URL — it does not leave a
    full page standing on a stale number. Pinned at 8 days because the 14-day
    default would still call this a page.
    """
    _seed_india(site_db, days_ago=8)
    tier = markets_mod.compute_tiers(today=date.today())["india"]
    assert not tier.has_daily_leg
    assert tier.present == ("weather",)
    assert tier.tier == "stub"


def test_weekly_cadence_is_never_a_daily_leg(site_db):
    """Europe's only leg is a weekly assessment, so it can never carry a ledger."""
    site_db.execute("CREATE TABLE IF NOT EXISTS ec_oilseed_prices (series TEXT, Date TEXT)")
    _seed_prices(site_db, ["EU Rapeseed (Moselle)"], table="ec_oilseed_prices", key_col="series")
    tiers = markets_mod.compute_tiers()
    assert not tiers["europe"].has_daily_leg
    assert "ledger" not in tiers["europe"].present


def test_missing_table_tiers_as_stub_rather_than_crashing(site_db):
    """A DB predating a schema change must degrade, not blow up the site."""
    tiers = markets_mod.compute_tiers()
    assert tiers["europe"].tier == "stub"  # ec_oilseed_prices does not exist here


# ---------------------------------------------------------------------------
# Block envelope
# ---------------------------------------------------------------------------
def test_non_ok_block_without_a_reason_is_rejected():
    with pytest.raises(ValueError, match="must name its reason"):
        make_block("crush", state="absent")
    with pytest.raises(ValueError, match="must name its reason"):
        make_block("crush", state="empty", reason="   ")


def test_unknown_block_state_is_rejected():
    with pytest.raises(ValueError, match="state"):
        Block(id="x", no="01", title="X", why="", state="probably-fine")


def test_built_blocks_are_the_nine_ids_in_fixed_order(site_db):
    _seed_cbot_page(site_db)
    markets = markets_mod.load_markets()
    tiers = markets_mod.compute_tiers(markets)
    ctx = SiteContext(conn=site_db, today=date.today())
    built = build_blocks(markets["cbot"], tiers["cbot"], ctx, markets=markets)
    assert [b.id for b in built] == list(BLOCK_IDS)
    assert [b.no for b in built] == [f"{i:02d}" for i in range(1, len(BLOCK_IDS) + 1)]


def test_positioning_is_not_in_the_market_skeleton():
    """Cut deliberately by M1: CBOT-only, so seven pages would carry a blank."""
    assert "positioning" not in BLOCK_IDS


def test_generation_error_reason_is_available_to_builders():
    block = make_block("crush", state="empty", reason=blocks_mod.GENERATION_ERROR)
    assert block.reason == "generation error"


# ---------------------------------------------------------------------------
# Rendering — the four M8 assertions
# ---------------------------------------------------------------------------
@pytest.fixture
def rendered(site_db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Render every market page against the fixture DB.

    The headline and players pages are stubbed out: they pull the whole
    analysis stack and are covered by their own tests. What this fixture
    exercises is the market half of the page list.
    """
    _seed_cbot_page(site_db)
    out = tmp_path / "docs"
    monkeypatch.setattr(generate_site, "_render_headline", lambda d, nav, **k: _stub_page(d / "index.html"))
    monkeypatch.setattr(generate_site, "_render_players", lambda d, nav, **k: _stub_page(d / "players.html"))
    results = generate_site.generate_site(output_dir=out)
    return out, results


def _stub_page(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("<html><body>stub</body></html>", encoding="utf-8")
    return path


def test_every_market_produces_exactly_one_file_at_its_registry_url(rendered):
    out, results = rendered
    markets = markets_mod.load_markets()
    written = sorted(p.relative_to(out).as_posix() for p in out.rglob("markets/*.html"))
    assert written == sorted(m.url for m in markets.values())
    assert all(r.ok for r in results), [r.error for r in results if not r.ok]


def test_full_page_emits_all_nine_blocks_in_order(rendered):
    out, _ = rendered
    html = (out / "markets" / "cbot.html").read_text()
    positions = [html.find(f'id="block-{bid}"') for bid in BLOCK_IDS]
    assert all(p >= 0 for p in positions), "a full page is missing a block"
    assert positions == sorted(positions), "blocks are out of the fixed order"


def test_every_non_ok_block_carries_a_reason_in_the_markup(rendered):
    out, _ = rendered
    for path in (out / "markets").glob("*.html"):
        html = path.read_text()
        for chunk in html.split('<div class="empty-state')[1:]:
            body = chunk.split("</div>")[0]
            text = body.split(">", 1)[1].replace('<span class="es-label">', "")
            assert text.replace("no source", "").replace("no data", "").strip(), (
                f"{path.name} renders an empty state with no reason"
            )


def test_every_page_is_under_the_size_budget(rendered):
    out, _ = rendered
    for path in out.rglob("*.html"):
        size = path.stat().st_size
        assert size <= generate_site.PAGE_SIZE_BUDGET_BYTES, (
            f"{path.relative_to(out)} is {size / 1024:.0f} KB, over the "
            f"{generate_site.PAGE_SIZE_BUDGET_BYTES / 1024:.0f} KB budget"
        )


def test_url_is_stable_across_tier_changes(site_db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A brief is different contents at the same address, never a 404 (M8 c3)."""
    monkeypatch.setattr(generate_site, "_render_headline", lambda d, nav, **k: _stub_page(d / "index.html"))
    monkeypatch.setattr(generate_site, "_render_players", lambda d, nav, **k: _stub_page(d / "players.html"))
    out = tmp_path / "docs"

    _seed_cbot_page(site_db)
    generate_site.generate_site(output_dir=out, only="cbot")
    as_page = (out / "markets" / "cbot.html").read_text()
    assert 'class="tier-pill">page' in as_page

    site_db.execute("DELETE FROM gulf_bids")
    site_db.execute("DELETE FROM weather")
    site_db.commit()
    generate_site.generate_site(output_dir=out, only="cbot")
    as_brief = (out / "markets" / "cbot.html").read_text()
    assert "brief" in as_brief
    assert (out / "markets" / "cbot.html").exists()


def test_stub_page_lists_a_reason_per_block(rendered):
    out, _ = rendered
    html = (out / "markets" / "nigeria.html").read_text()
    assert "has no page yet" in html
    assert "no Nigerian soybean price source is ingested" in html


def test_market_nav_lists_every_market_on_every_page(rendered):
    out, _ = rendered
    markets = markets_mod.load_markets()
    for path in (out / "markets").glob("*.html"):
        html = path.read_text()
        for slug, market in markets.items():
            assert f'href="../markets/{slug}.html"' in html, f"{path.name} nav is missing {slug}"
            assert f">{market.name}</a>" in html


def test_a_failed_page_is_replaced_by_a_tombstone_not_left_stale(
    site_db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """Pages ships the whole docs/ dir, so yesterday's file is the #157 shape."""
    monkeypatch.setattr(generate_site, "_render_headline", lambda d, nav, **k: _stub_page(d / "index.html"))
    monkeypatch.setattr(generate_site, "_render_players", lambda d, nav, **k: _stub_page(d / "players.html"))
    out = tmp_path / "docs"
    stale = out / "markets" / "cbot.html"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("<html>YESTERDAY 999.99</html>", encoding="utf-8")

    def _boom(*a, **k):
        raise RuntimeError("scraper exploded")

    monkeypatch.setattr(generate_site, "_render_market", _boom)
    results = generate_site.generate_site(output_dir=out, only="cbot")

    html = stale.read_text()
    assert "YESTERDAY" not in html
    assert "scraper exploded" in html
    assert "could not be generated today" in html
    assert results[0].ok is False


def test_a_tombstone_reds_the_run(site_db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(generate_site, "_render_headline", lambda d, nav, **k: _stub_page(d / "index.html"))
    monkeypatch.setattr(generate_site, "_render_players", lambda d, nav, **k: _stub_page(d / "players.html"))
    monkeypatch.setattr(generate_site, "_render_market", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("nope")))
    assert generate_site.main(["--only", "cbot", "--output-dir", str(tmp_path / "docs")]) == 1


def test_a_failed_headline_fails_the_run(site_db, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """There is no product without the headline — no tombstone, no green run."""
    def _boom(*a, **k):
        raise RuntimeError("analysts down")

    monkeypatch.setattr(generate_site, "_render_headline", _boom)
    with pytest.raises(RuntimeError, match="analysts down"):
        generate_site.generate_site(output_dir=tmp_path / "docs", only="headline")


def test_only_rejects_an_unknown_page(site_db, tmp_path: Path):
    with pytest.raises(SystemExit, match="matches no page"):
        generate_site.generate_site(output_dir=tmp_path / "docs", only="atlantis")


def test_relative_root_matches_page_depth():
    assert markets_mod.relative_root("index.html") == ""
    assert markets_mod.relative_root("players.html") == ""
    assert markets_mod.relative_root("markets/cbot.html") == "../"


# ── #212: the tier reason must name our outage as ours ─────────────────────
#
# One data.gov.in rate limit costs India two blocks at once — the daily leg
# and, because the ledger is daily-only, the ledger with it — which is enough
# to demote the market. The demotion itself is correct: we genuinely do not
# have the number. What was wrong is that the page printed it in the same
# words it uses for "nobody publishes this", turning our ingest failure into
# a judgement about India.


def _seed_freshness(conn, layer: str, status: str, last_success: str | None) -> None:
    conn.execute(schema._CREATE_DATA_FRESHNESS)
    conn.execute(
        "INSERT OR REPLACE INTO data_freshness "
        "(layer_name, last_success, last_attempt, rows_fetched, status) "
        "VALUES (?, ?, ?, ?, ?)",
        (layer, last_success, date.today().isoformat(), 0, status),
    )
    conn.commit()


def test_missing_rows_after_a_failed_ingest_says_so(site_db):
    """"no rows" alone cannot tell a rate limit from a market with no source."""
    _seed_freshness(site_db, "india_domestic", "failed", "2026-08-11T00:00:00")

    tier = markets_mod.compute_tiers()["india"]

    assert tier.has_daily_leg is False
    assert "our india_domestic ingest failed upstream" in tier.notes["price"]
    assert "last good run 2026-08-11" in tier.notes["price"]


@pytest.mark.parametrize(
    ("status", "phrase"),
    [
        ("stale", "stale last-known-good"),
        ("incomplete", "incomplete key coverage"),
    ],
)
def test_missing_rows_name_the_non_transport_degradation(site_db, status, phrase):
    _seed_freshness(site_db, "india_domestic", status, "2026-08-11T00:00:00")

    tier = markets_mod.compute_tiers()["india"]

    assert phrase in tier.notes["price"]


def test_a_market_with_no_source_is_not_blamed_on_our_ingest(site_db):
    """The other half of the distinction: absent by nature, not by outage."""
    _seed_freshness(site_db, "india_domestic", "success", date.today().isoformat())

    tier = markets_mod.compute_tiers()["india"]

    assert "ingest failed" not in tier.notes["price"]
    # India has no crush source at all — that reason is about the market.
    assert "ingest failed" not in tier.notes["crush"]


def test_a_healthy_ingest_leaves_the_note_alone(site_db):
    _seed_prices(
        site_db,
        ["Soybean (Mandi MP)", "Soybean (Mandi MH)"],
        table="india_domestic_prices",
    )
    _seed_freshness(site_db, "india_domestic", "success", date.today().isoformat())

    tier = markets_mod.compute_tiers()["india"]

    assert tier.has_daily_leg is True
    assert tier.notes["price"].startswith("current to")
