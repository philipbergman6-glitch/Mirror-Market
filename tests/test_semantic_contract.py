"""The repository-wide semantic safety contract.

``tests/test_price_semantics.py`` pins the vocabulary and
``tests/test_price_semantics_rendering.py`` pins four surfaces against two
claims. This file is the general form of both, and it exists because the bug it
guards is not a wording slip in one template — it is a *future* feature reusing
an existing number correctly and describing it wrongly.

Three kinds of assertion, in the order of how hard they are to defeat:

1. **Structural.** A continuous research series cannot reach a hedge, a ticket
   or a named-contract calculation, because those entry points refuse it by
   type at runtime rather than by a reviewer noticing. A confidence that its
   quote kind cannot support cannot be constructed at all.
2. **Rendered.** Every page the site publishes, plus both private workspace
   editions, plus the briefing text, is scanned against one catalogue of
   forbidden claims — held in ``pricing.policy``, not restated here.
3. **Promotion.** The same scan runs inside the promotion contract, so a
   misleading edition is refused at the gate and not only in CI.

What is deliberately *not* asserted: that a surface says nothing. Honest
denials ("delayed daily closes, not proven exchange settlements") must survive,
and a test that banned the word would delete the sentence that tells the truth.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

import config
from analysis.futures.crush import CrushWithheld
from analysis.futures.domain import (
    ContinuousSeries,
    RollMethod,
    Side,
    named_contract,
)
from analysis.futures.hedge import build_hedge, propose_crush_hedge, propose_hedge, size_leg
from analysis.futures.ticket import build_ticket
from app import markets as markets_mod
from pipeline import schema
from pricing.policy import (
    CLAIM_SUPPORTED_BY,
    FORBIDDEN_CLAIMS,
    ClaimKind,
    NotHedgeable,
    SemanticContractError,
    assert_language_permitted,
    may_claim,
    permitted_claims,
    require_hedgeable,
    require_traded_price,
    scan,
)
from pricing.semantics import Confidence, PriceType
from scripts import generate_site
from tests.test_futures_hedge import AS_OF, BEANS, MEAL, OIL, curve, exposure, quote

TODAY = date.today()


def _day(offset: int = 0) -> str:
    return (TODAY - timedelta(days=offset)).isoformat()


# ---------------------------------------------------------------------------
# 1. The policy itself
# ---------------------------------------------------------------------------
def test_no_price_type_this_project_ingests_may_claim_a_settlement():
    """The claim is about the provider, and no provider here proves one.

    ``ATTESTED_SETTLEMENT`` is the one exception and it is not ingested — it
    arrives on a clearing statement the user supplied, and reaches one private
    surface.
    """
    ingested = (
        PriceType.DELAYED_CLOSE,
        PriceType.LAST_TRADE,
        PriceType.ASSESSMENT,
        PriceType.ADMINISTERED,
        PriceType.MANUAL,
    )
    for price_type in ingested:
        for kind in (ClaimKind.SETTLEMENT, ClaimKind.OFFICIAL_CLOSE, ClaimKind.EXECUTABLE):
            assert not may_claim(price_type, kind), f"{price_type.value} may claim {kind.value}"


def test_only_a_proven_settlement_is_executable():
    assert may_claim(PriceType.SETTLEMENT, ClaimKind.EXECUTABLE)
    assert CLAIM_SUPPORTED_BY[ClaimKind.EXECUTABLE] == frozenset({PriceType.SETTLEMENT})


def test_an_attested_statement_may_say_settlement_but_is_still_not_executable():
    assert may_claim(PriceType.ATTESTED_SETTLEMENT, ClaimKind.SETTLEMENT)
    assert not may_claim(PriceType.ATTESTED_SETTLEMENT, ClaimKind.EXECUTABLE)


def test_no_physical_assessment_is_a_firm_offer():
    assert not may_claim(PriceType.ASSESSMENT, ClaimKind.FIRM_OFFER)
    # And nothing else is either: this stack ingests no counterparty quote.
    assert CLAIM_SUPPORTED_BY[ClaimKind.FIRM_OFFER] == frozenset()


def test_no_administered_reference_is_a_traded_price():
    assert not may_claim(PriceType.ADMINISTERED, ClaimKind.TRADED_PRICE)
    assert not may_claim(PriceType.ASSESSMENT, ClaimKind.TRADED_PRICE)
    assert may_claim(PriceType.DELAYED_CLOSE, ClaimKind.TRADED_PRICE)
    assert may_claim(PriceType.LAST_TRADE, ClaimKind.TRADED_PRICE)


def test_permitted_claims_is_the_union_over_what_a_surface_renders():
    assert permitted_claims(()) == frozenset()
    assert ClaimKind.TRADED_PRICE in permitted_claims([PriceType.DELAYED_CLOSE])
    assert ClaimKind.SETTLEMENT not in permitted_claims([PriceType.DELAYED_CLOSE])


def test_every_claim_kind_carries_at_least_one_phrase_and_a_reason():
    kinds = {claim.kind for claim in FORBIDDEN_CLAIMS}
    assert kinds == set(ClaimKind)
    for claim in FORBIDDEN_CLAIMS:
        assert claim.why.strip(), f"{claim.pattern} has no stated reason"


@pytest.mark.parametrize(
    ("text", "kind"),
    [
        ("Soybeans settled at the official close of 1167.75.", ClaimKind.OFFICIAL_CLOSE),
        ("This is an executable price for 5,000 MT.", ClaimKind.EXECUTABLE),
        ("The exchange settlement was 1167.75.", ClaimKind.SETTLEMENT),
        ("Paranaguá FOB is a firm offer at 452.", ClaimKind.FIRM_OFFER),
        ("The Argentine official FOB is the traded price.", ClaimKind.TRADED_PRICE),
    ],
)
def test_the_scanner_catches_each_claim(text, kind):
    violations = scan(text, surface="probe")
    assert [v.kind for v in violations] == [kind]


def test_a_denial_in_the_same_sentence_is_not_a_claim():
    honest = "Delayed daily closes, not proven exchange settlements."
    assert scan(honest, surface="probe") == ()


def test_a_denial_in_a_different_sentence_does_not_launder_the_claim():
    """The origins-page bug, exactly: the second sentence denies something else."""
    text = "Three exchange settlements. This is what a hedge locks, not what a plant earns."
    assert [v.kind for v in scan(text, surface="probe")] == [ClaimKind.SETTLEMENT]


def test_a_surface_that_renders_an_attested_statement_may_say_settlement():
    text = "The settlement price on the clearing statement was 1167.75."
    assert scan(text, surface="book") != ()
    assert scan(text, surface="book", price_types=[PriceType.ATTESTED_SETTLEMENT]) == ()


def test_script_and_style_content_is_not_read_as_prose():
    """A chart payload is not something a reader sees."""
    html = "<script>var t = 'executable';</script><style>.x{}</style><p>Fine.</p>"
    assert scan(html, surface="probe") == ()


def test_assert_language_permitted_names_the_surface_and_the_sentence():
    with pytest.raises(SemanticContractError) as excinfo:
        assert_language_permitted("<p>An executable price.</p>", surface="origins")
    message = str(excinfo.value)
    assert "origins" in message
    assert "executable" in message


# ---------------------------------------------------------------------------
# 2. Structural — a research series cannot become a trade
# ---------------------------------------------------------------------------
def _continuous() -> ContinuousSeries:
    return ContinuousSeries(
        commodity="Soybeans",
        roll_method=RollMethod.PROVIDER_FRONT_MONTH,
        points=((AS_OF, 1167.75),),
    )


def test_a_named_contract_and_its_quote_declare_themselves_hedgeable():
    contract = named_contract("Soybeans", 2026, 11)
    assert contract.is_hedgeable is True
    assert quote("Soybeans", 2026, 11, 1167.75).is_hedgeable is True
    assert _continuous().is_hedgeable is False


def test_require_hedgeable_refuses_anything_that_does_not_declare_itself():
    require_hedgeable(quote("Soybeans", 2026, 11, 1167.75), calculation="probe")
    for refused in (_continuous(), object(), None, "ZSX26"):
        with pytest.raises(NotHedgeable):
            require_hedgeable(refused, calculation="probe")


def test_sizing_a_leg_off_a_continuous_series_is_refused():
    with pytest.raises(NotHedgeable):
        size_leg(_continuous(), side=Side.SHORT, physical_mt=10_000)


def test_building_a_hedge_with_a_continuous_leg_is_refused():
    proposal = propose_hedge(exposure(Side.LONG), curve("Soybeans", BEANS), as_of=AS_OF)
    poisoned = replace(proposal.legs[0], quote=_continuous())
    with pytest.raises(NotHedgeable):
        build_hedge(proposal.exposure, (poisoned,), as_of=AS_OF)


def test_a_ticket_will_not_print_a_continuous_leg():
    proposal = propose_hedge(exposure(Side.LONG), curve("Soybeans", BEANS), as_of=AS_OF)
    poisoned = replace(proposal, legs=(replace(proposal.legs[0], quote=_continuous()),))
    with pytest.raises(NotHedgeable):
        build_ticket(poisoned, generated_at=datetime(2026, 8, 18, 21, 30))


def test_the_crush_hedge_refuses_a_continuous_leg_however_it_arrives(monkeypatch):
    """Three legs, one crush period — and no route in for a stitched series.

    The month is chosen by ``select_hedge_month``; a provider that handed back
    a continuous series there would have every leg sized off it. It is stubbed
    rather than constructed because the refusal must not depend on *which*
    caller made the mistake.
    """
    from analysis.futures import hedge as hedge_mod

    monkeypatch.setattr(
        hedge_mod, "select_hedge_month", lambda *a, **k: (_continuous(), None)
    )
    with pytest.raises(NotHedgeable):
        propose_crush_hedge(
            exposure(Side.LONG),
            curve("Soybeans", BEANS), curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL),
            as_of=AS_OF,
        )


def test_a_continuous_series_has_no_path_to_a_named_crush():
    """The withheld answer is a different type, so it cannot be read as a margin."""
    from analysis.futures.crush import continuous_withheld

    withheld = continuous_withheld("Soybeans", _continuous())
    assert isinstance(withheld, CrushWithheld)
    assert not withheld.is_ok


def test_an_administered_price_cannot_size_a_futures_leg():
    """A hedge is placed in a traded market; an official minimum is not one."""
    administered = replace(
        quote("Soybeans", 2026, 11, 1167.75), price_type=PriceType.ADMINISTERED
    )
    with pytest.raises(SemanticContractError):
        size_leg(administered, side=Side.SHORT, physical_mt=10_000)


def test_require_traded_price_refuses_an_assessment_and_accepts_a_board_close():
    require_traded_price(PriceType.DELAYED_CLOSE, context="probe")
    for refused in (PriceType.ADMINISTERED, PriceType.ASSESSMENT, PriceType.MANUAL):
        with pytest.raises(SemanticContractError):
            require_traded_price(refused, context="probe")


def test_evidence_cannot_be_given_a_confidence_its_quote_kind_cannot_support():
    from analysis.opportunities.domain import Evidence
    from tests.opportunity_fixtures import make_evidence

    make_evidence(quote_kind="physical", confidence=Confidence.INDICATIVE)
    with pytest.raises(SemanticContractError):
        make_evidence(quote_kind="physical", confidence=Confidence.EXECUTABLE)
    with pytest.raises(SemanticContractError):
        make_evidence(quote_kind="administered", confidence=Confidence.BOARD_REFERENCE)
    assert Evidence  # imported for the reader; the fixture builds it


def test_a_tonnage_observation_has_no_price_claim_to_over_state():
    """"Not a price" and "nobody classified this" are different facts.

    A weekly export tonnage is evidence, and it is not a quote — asking for its
    confidence ceiling is asking the wrong question about the row. An unknown
    quote kind, by contrast, has to keep raising: that is a typo, and a typo
    that defaulted to permissive is the bug this module exists to close.
    """
    from tests.opportunity_fixtures import make_evidence

    make_evidence(quote_kind="observation", confidence=Confidence.INDICATIVE)
    with pytest.raises(KeyError):
        make_evidence(quote_kind="setlement", confidence=Confidence.INDICATIVE)


def test_a_block_cannot_carry_an_unclassified_quote_kind():
    from app.blocks import make_block

    with pytest.raises(KeyError):
        make_block("price", state="ok", kind="settlement_feed")


# ---------------------------------------------------------------------------
# 3. Rendered — every published page, both private editions, the briefing
# ---------------------------------------------------------------------------
def _seed(conn: sqlite3.Connection) -> None:
    """One realistic current session for every market leg on the site.

    Rich rather than minimal on purpose: an empty database renders empty
    states, and an empty state cannot make a misleading claim. What has to be
    scanned is a page with numbers on it.
    """
    from analysis.futures.crush import crush_contract_candidates

    for offset in (0, 1, 5, 21):
        for commodity, close in (
            ("Soybeans", 1167.75), ("Soybean Oil", 68.18), ("Soybean Meal", 310.80),
            ("Corn", 415.25), ("Wheat", 542.0),
        ):
            conn.execute(
                "INSERT OR REPLACE INTO prices (commodity, Date, Open, High, Low, Close, Volume) "
                "VALUES (?,?,?,?,?,?,?)",
                (commodity, _day(offset), close, close, close, close, 90_000.0),
            )

    # Named contracts, taken from the crush's own listed-month rule so the
    # board crush computes instead of being withheld for want of a month.
    for contracts in crush_contract_candidates(TODAY, count=4):
        for contract, close in (
            (contracts.bean, 1167.75), (contracts.meal, 310.80), (contracts.oil, 68.18),
        ):
            conn.execute(
                "INSERT OR REPLACE INTO forward_curve (commodity, contract_month, label, ticker, "
                "close, observation_date, volume, fetched_date) VALUES (?,?,?,?,?,?,?,?)",
                (
                    contract.spec.name,
                    date(contract.year, contract.month, 1).isoformat(),
                    contract.label,
                    f"{contract.symbol}.CBT",
                    close,
                    _day(0),
                    4210.0,
                    _day(0),
                ),
            )

    for pair, rate in (
        ("BRL/USD", 0.1958), ("CNY/USD", 0.1396), ("INR/USD", 0.0115),
        ("ZAR/USD", 0.0556), ("NGN/USD", 0.00065), ("EUR/USD", 1.09),
    ):
        for offset in (0, 1, 5):
            conn.execute(
                "INSERT OR REPLACE INTO currencies (pair, Date, Open, High, Low, Close) "
                "VALUES (?,?,?,?,?,?)",
                (pair, _day(offset), rate, rate, rate, rate),
            )

    for location, average, basis in (("NOLA", 12.5563, 95.0), ("TEXAS", 12.6875, 100.0)):
        conn.execute(
            "INSERT OR REPLACE INTO gulf_bids (report_date, commodity, location, delivery, "
            "average, futures_month, basis_low, basis_high) VALUES (?,?,?,?,?,?,?,?)",
            (_day(0), "Soybeans", location, "Current", average, 11, basis, basis),
        )
    for product, position, price in (
        ("Soybeans", "12019000190C", 449.0),
        ("Soybean Oil", "15071000900J", 1120.0),
        ("Soybean Meal", "23040010100B", 385.0),
        ("Sunflower Oil", "15121110310E", 1180.0),
    ):
        conn.execute(
            "INSERT OR REPLACE INTO argentina_fob (date, product, position, ship_from, ship_to, "
            "price_usd_mt) VALUES (?,?,?,?,?,?)",
            (_day(0), product, position, _day(0)[:7], _day(0)[:7], price),
        )
    for key, price in (
        ("Soybean (CEPEA)", 2000.0),
        ("Soybean (ESALQ/B3 Paranaguá)", 2050.0),
        ("Soybean (AgRural Paranaguá FOB)", 2433.33),
    ):
        conn.execute(
            "INSERT OR REPLACE INTO brazil_spot_prices (Date, commodity, price_brl, unit) "
            "VALUES (?,?,?,?)", (_day(0), key, price, "BRL/MT"),
        )
    for key, close in (("Soybean (Mandi MP)", 67_250.0), ("Soybean (Mandi MH)", 66_100.0)):
        conn.execute(
            "INSERT OR REPLACE INTO india_domestic_prices (Date, commodity, Close, unit) "
            "VALUES (?,?,?,?)", (_day(0), key, close, "INR/MT"),
        )
    for key, close in (("Soybean (SAFEX)", 9_100.0), ("Sunflower (SAFEX)", 10_400.0)):
        conn.execute(
            "INSERT OR REPLACE INTO safex_prices (Date, commodity, Close, Volume, unit, contract) "
            "VALUES (?,?,?,?,?,?)", (_day(0), key, close, 42.0, "ZAR/MT", "SEP26"),
        )
    for key, close in (
        ("DCE Soybean No.2", 3600.0), ("DCE Soybean Oil", 8000.0),
        ("DCE Soybean Meal", 3000.0), ("DCE Soybean No.1", 4900.0),
    ):
        conn.execute(
            "INSERT OR REPLACE INTO dce_futures (commodity, Date, Close) VALUES (?,?,?)",
            (key, _day(0), close),
        )
    conn.execute(
        "INSERT OR REPLACE INTO ec_oilseed_prices (series, Date, price_usd, price_eur, cadence, "
        "quote_kind) VALUES (?,?,?,?,?,?)",
        ("EU Rapeseed (Moselle)", _day(3), 512.0, 470.0, "weekly", "assessment"),
    )
    conn.execute(
        "INSERT OR REPLACE INTO sagis_deliveries (commodity, season_year, week_number, week_end, "
        "season_status, first_published, adjustments, week_total, unit) VALUES (?,?,?,?,?,?,?,?,?)",
        ("Soybeans", TODAY.year, 22, _day(4), "Active", 40_000.0, 0.0, 40_000.0, "MT"),
    )
    for region in ("US Midwest (Iowa)", "US Illinois", "Brazil Mato Grosso", "India Madhya Pradesh"):
        conn.execute(
            "INSERT OR REPLACE INTO weather (region, Date, temp_max, temp_min, precipitation) "
            "VALUES (?,?,?,?,?)", (region, _day(0), 31.0, 18.0, 2.0),
        )
    for year, value in ((TODAY.year, 120_000.0), (TODAY.year - 1, 100_000.0)):
        for country in ("United States", "Brazil", "Argentina", "China", "India"):
            conn.execute(
                "INSERT OR REPLACE INTO psd (commodity, country, year, attribute, value, unit) "
                "VALUES (?,?,?,?,?,?)",
                ("Soybeans", country, year, "Production", value, "1000 MT"),
            )
    stamp = f"{_day(0)} 21:30:00"
    for layer, *_ in config.PRODUCTION_LAYERS:
        conn.execute(
            "INSERT OR REPLACE INTO data_freshness (layer_name, last_success, last_attempt, "
            "rows_fetched, status) VALUES (?,?,?,?,?)", (layer, stamp, stamp, 10, "success"),
        )
    conn.commit()


@pytest.fixture
def published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Generate the whole site off a seeded database. Returns (public, private)."""
    db_path = tmp_path / "semantic.db"
    conn = sqlite3.connect(str(db_path))
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    _seed(conn)

    def connect() -> sqlite3.Connection:
        return sqlite3.connect(str(db_path))

    monkeypatch.setattr(markets_mod, "get_connection", connect)
    monkeypatch.setattr(markets_mod, "is_cloud", lambda: False)
    monkeypatch.setattr(config, "DB_PATH", str(db_path))
    for module in ("pipeline.store", "pipeline.query"):
        monkeypatch.setattr(f"{module}.get_connection", connect)
        monkeypatch.setattr(f"{module}.DB_PATH", str(db_path))
        monkeypatch.setattr(f"{module}.is_cloud", lambda: False)
    private = tmp_path / "workspace"
    monkeypatch.setattr(config, "OPPORTUNITY_PRIVATE_OUTPUT_DIR", str(private))

    out = tmp_path / "docs"
    results = generate_site.generate_site(output_dir=out)
    assert all(result.ok for result in results), [r.error for r in results if not r.ok]
    conn.close()
    return out, private


def _pages(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(root.rglob("*.html"))
    }


def test_every_published_page_was_generated_and_scanned(published):
    """The scan is worthless if it silently ran over three pages."""
    from trust.site_promotion import expected_site_paths

    public, _ = published
    rendered = _pages(public)
    assert set(expected_site_paths()) <= set(rendered)
    assert len(rendered) >= 13


def test_no_published_page_makes_a_claim_no_source_supports(published):
    public, _ = published
    for relpath, html in _pages(public).items():
        assert_language_permitted(html, surface=relpath)


def test_the_private_workspace_editions_are_scanned_too(published):
    """Private is not exempt: the desk trades off these pages.

    They may say "settlement" about an attested clearing statement, which is
    the one number in this repository that is one — and about nothing else.
    """
    _, private = published
    rendered = _pages(private)
    assert rendered, "no private edition was written"
    for relpath, html in rendered.items():
        assert_language_permitted(
            html, surface=f"private:{relpath}", price_types=[PriceType.ATTESTED_SETTLEMENT]
        )


def test_the_private_trial_dashboard_is_scanned(published):
    from app.trial_page import build_view, render_trial_page
    from tests.trial_fixtures import TODAY as TRIAL_TODAY
    from tests.trial_fixtures import full_window

    sessions, days = full_window()
    html = render_trial_page(build_view(sessions, days, today=TRIAL_TODAY))
    assert_language_permitted(html, surface="private:trial.html")


def test_the_briefing_text_makes_no_claim_no_source_supports(published):
    from analysis.briefing import generate_briefing

    text = generate_briefing()
    assert "CRUSH SPREAD" in text
    assert_language_permitted(text, surface="briefing")


def test_the_workstation_still_denies_settlement_in_so_many_words(published):
    """Silence is not honesty — the denial has to survive the scan."""
    public, _ = published
    workstation = (public / "workstation.html").read_text(encoding="utf-8")
    assert "not proven exchange settlements" in workstation.lower()


def test_a_market_page_names_the_animal_beside_the_number(published):
    public, _ = published
    cbot = (public / "markets" / "cbot.html").read_text(encoding="utf-8")
    assert "delayed close" in cbot.lower()
    argentina = (public / "markets" / "argentina.html").read_text(encoding="utf-8")
    assert "administered" in argentina.lower()


def test_the_scan_would_catch_a_regression_in_a_real_page(published):
    """A page that passes today must not be passing because the scan is inert."""
    public, _ = published
    html = (public / "origins.html").read_text(encoding="utf-8")
    poisoned = html.replace("</body>", "<p>Every leg is an executable price.</p></body>")
    with pytest.raises(SemanticContractError):
        assert_language_permitted(poisoned, surface="origins.html")


# ---------------------------------------------------------------------------
# 4. Promotion — the gate carries the same scan
# ---------------------------------------------------------------------------
def test_the_promotion_contract_refuses_a_misleading_edition():
    from trust.site_promotion import expected_site_paths, verify_site_candidate

    pages = {path: "<html><body><p>Nothing here.</p></body></html>" for path in expected_site_paths()}
    pages["index.html"] = (
        "<html><body><p>Soybeans at the official close of 1167.75.</p></body></html>"
    )
    verdict = verify_site_candidate(pages)
    assert not verdict.verified
    assert any("official close" in failure for failure in verdict.failures)


def test_the_promotion_contract_keeps_an_honest_denial():
    from trust.site_promotion import expected_site_paths, verify_site_candidate

    pages = {path: "<html><body><p>Nothing here.</p></body></html>" for path in expected_site_paths()}
    pages["workstation.html"] = (
        "<html><body><p>Delayed daily closes, not proven exchange settlements.</p></body></html>"
    )
    verdict = verify_site_candidate(pages)
    assert not any("misleading claim" in failure for failure in verdict.failures)
