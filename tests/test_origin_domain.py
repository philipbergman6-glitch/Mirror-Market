"""Units, FX direction, incoterms and the fail-closed invariants (Phase 2).

These are the tests that pin the things which, when wrong, produce a *plausible*
number rather than a crash: a landed cost that added dollars to percentages, an
FX conversion applied the wrong way round, a CIF-barge bid compared against an
FOB-vessel offer, or a total published beside its own blockers.
"""

from __future__ import annotations

from datetime import date

import pytest

import config
from analysis.origins.domain import (
    AD_VALOREM_COMPONENTS,
    COMPONENT_ORDER,
    CONFIDENCE_BY_QUOTE_KIND,
    CRUSH_COST_COMPONENTS,
    Blocker,
    Carrier,
    Comparability,
    Confidence,
    ContractRef,
    CostComponent,
    Freshness,
    FxObservation,
    Grade,
    Incoterm,
    LandedCost,
    Money,
    OriginQuote,
    OriginRanking,
    Port,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    fingerprint,
    usd_mt,
    worst_confidence,
)

GULF = Port("us_gulf", "US Gulf", "United States", "US")
CN = Port("cn_north", "North China", "China", "CN", role="destination")
SEP = ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026")


def _quote(**overrides) -> OriginQuote:
    base = {
        "origin": GULF,
        "grade": Grade(specification="US No. 2 Yellow"),
        "quote_kind": QuoteKind.PHYSICAL,
        "incoterm": Incoterm.CIF,
        "carrier": Carrier.BARGE,
        "price": usd_mt(466.19),
        "native_price": 12.6875,
        "native_currency": "USD",
        "native_unit": "usd_per_bushel",
        "observation_date": date(2026, 8, 11),
        "publication_date": None,
        "source": SourceRef("gulf_bids", "gulf_bids", "Soybeans"),
        "shipment_window": SEP,
    }
    return OriginQuote(**{**base, **overrides})


# ---------------------------------------------------------------------------
# Money — units and currencies never add silently
# ---------------------------------------------------------------------------
def test_money_adds_when_currency_and_unit_agree():
    assert (usd_mt(100.0) + usd_mt(52.0)).amount == pytest.approx(152.0)


def test_money_refuses_to_add_across_currencies():
    with pytest.raises(ValueError, match="cannot combine"):
        usd_mt(100.0) + Money(100.0, "BRL", "usd_per_mt")


def test_money_refuses_to_add_across_units():
    with pytest.raises(ValueError, match="cannot combine"):
        usd_mt(100.0) + Money(100.0, "USD", "usd_per_bushel")


def test_money_rejects_nan_because_a_missing_value_is_none():
    with pytest.raises(ValueError, match="NaN"):
        Money(float("nan"))


def test_money_scaling_keeps_the_unit():
    scaled = usd_mt(100.0).scaled(0.03)
    assert (scaled.currency, scaled.unit) == ("USD", "usd_per_mt")


# ---------------------------------------------------------------------------
# FX direction — the one convention that gets remembered backwards
# ---------------------------------------------------------------------------
def test_fx_converts_local_to_usd_by_multiplying():
    """`<CCY>/USD` is USD per one unit of local currency, so BRL 2,433 x 0.1958."""
    fx = FxObservation("BRL/USD", 0.1958480179309845, date(2026, 8, 11))
    assert fx.to_usd(2433.33) == pytest.approx(476.56, abs=0.01)


def test_fx_round_trips_in_both_directions():
    fx = FxObservation("BRL/USD", 0.19584, date(2026, 8, 11))
    assert fx.to_local(fx.to_usd(2433.33)) == pytest.approx(2433.33)


def test_fx_the_wrong_way_round_is_wildly_wrong_and_that_is_the_point():
    """Dividing instead of multiplying gives 12,425 USD/MT, not 476.

    Pinned so that a future 'fix' inverting the convention fails loudly rather
    than shipping a Brazilian soybean at twelve thousand dollars a tonne.
    """
    fx = FxObservation("BRL/USD", 0.19584, date(2026, 8, 11))
    assert fx.to_usd(2433.33) < 500
    assert fx.to_local(2433.33) > 12_000


def test_fx_rejects_a_non_positive_rate():
    with pytest.raises(ValueError, match="must be positive"):
        FxObservation("BRL/USD", 0.0, date(2026, 8, 11))


def test_fx_rejects_an_unsupported_convention():
    with pytest.raises(ValueError, match="usd-per-local-unit"):
        FxObservation("BRL/USD", 0.19, date(2026, 8, 11), convention="local-per-usd")


# ---------------------------------------------------------------------------
# Shipment windows
# ---------------------------------------------------------------------------
def test_window_rejects_an_inverted_range():
    with pytest.raises(ValueError, match="ends .* before it starts"):
        ShipmentWindow(date(2026, 10, 1), date(2026, 9, 1))


def test_windows_overlap_by_day_count_not_by_equality():
    """A MAGyP band and an AMS half-month slot are never equal and often overlap."""
    band = ShipmentWindow(date(2026, 9, 1), date(2026, 10, 31))
    slot = ShipmentWindow(date(2026, 9, 16), date(2026, 9, 30))
    assert band.overlaps(slot)
    assert band.overlap_days(slot) == 15
    assert band.contains(slot)


def test_adjacent_windows_do_not_overlap():
    august = ShipmentWindow(date(2026, 8, 1), date(2026, 8, 31))
    september = ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30))
    assert not august.overlaps(september)
    assert august.overlap_days(september) == 0


# ---------------------------------------------------------------------------
# Quotes
# ---------------------------------------------------------------------------
def test_a_quote_must_already_be_usd_per_mt():
    with pytest.raises(ValueError, match="must already be USD"):
        _quote(price=Money(2433.0, "BRL", "usd_per_mt"))


def test_a_local_currency_quote_must_carry_its_fx_observation():
    """Without it the USD figure cannot be audited, only believed."""
    with pytest.raises(ValueError, match="FX observation"):
        _quote(native_currency="BRL", native_unit="home_per_mt", fx=None)


def test_freshness_is_measured_against_the_layers_own_budget():
    quote = _quote(max_age_days=5)
    assert quote.freshness(date(2026, 8, 14)) is Freshness.CURRENT
    assert quote.freshness(date(2026, 8, 20)) is Freshness.STALE


def test_every_quote_kind_maps_to_a_confidence():
    assert set(CONFIDENCE_BY_QUOTE_KIND) == set(QuoteKind)


def test_only_a_board_quote_is_executable():
    executable = {
        kind for kind, conf in CONFIDENCE_BY_QUOTE_KIND.items()
        if conf is Confidence.EXECUTABLE
    }
    assert executable == {QuoteKind.BOARD}


def test_an_administered_minimum_is_not_merely_low_confidence():
    """It is precisely known and legally binding — and simply not a traded price."""
    assert CONFIDENCE_BY_QUOTE_KIND[QuoteKind.ADMINISTERED] is Confidence.ADMINISTERED


def test_worst_confidence_is_the_weakest_link():
    assert worst_confidence(
        Confidence.EXECUTABLE, Confidence.INDICATIVE
    ) is Confidence.INDICATIVE
    assert worst_confidence(None, None) is Confidence.UNAVAILABLE


# ---------------------------------------------------------------------------
# Component order — the ad-valorem chain
# ---------------------------------------------------------------------------
def test_duty_is_applied_after_freight_and_vat_after_duty():
    """Reordering these changes the answer, which is why the order is versioned."""
    order = list(COMPONENT_ORDER)
    assert order.index(CostComponent.OCEAN_FREIGHT) < order.index(CostComponent.IMPORT_DUTY)
    assert order.index(CostComponent.IMPORT_DUTY) < order.index(CostComponent.IMPORT_VAT)


def test_ad_valorem_components_are_the_percentage_ones():
    assert {
        CostComponent.IMPORT_DUTY,
        CostComponent.IMPORT_VAT,
        CostComponent.MARINE_INSURANCE,
    } == AD_VALOREM_COMPONENTS


def test_crush_rungs_are_not_part_of_the_cargo_waterfall():
    """They are the same assumption contract, and a different calculation."""
    assert not set(CRUSH_COST_COMPONENTS) & set(COMPONENT_ORDER)


def test_the_configured_landed_stack_is_all_waterfall_components():
    for name in config.LANDED_STACK:
        assert CostComponent(name) in COMPONENT_ORDER


# ---------------------------------------------------------------------------
# LandedCost — fail closed
# ---------------------------------------------------------------------------
def _landed(**overrides) -> LandedCost:
    base = {
        "quote": _quote(),
        "destination": CN,
        "requested_window": SEP,
        "steps": (),
        "landed": usd_mt(609.61),
        "fob_equivalent": usd_mt(480.69),
        "comparability": Comparability.COMPARABLE,
        "confidence": Confidence.INDICATIVE,
        "freshness": Freshness.CURRENT,
    }
    return LandedCost(**{**base, **overrides})


def test_a_total_cannot_be_published_beside_its_own_blockers():
    with pytest.raises(ValueError, match="complete and blocked"):
        _landed(blockers=(Blocker("assumption_missing", "no freight"),))


def test_a_comparable_row_must_carry_a_landed_cost():
    with pytest.raises(ValueError, match="must carry a landed cost"):
        _landed(landed=None)


def test_only_a_comparable_row_is_rankable():
    assert _landed().is_rankable
    assert not _landed(
        comparability=Comparability.WINDOW_UNKNOWN
    ).is_rankable


# ---------------------------------------------------------------------------
# Ranking
# ---------------------------------------------------------------------------
def _row(origin: str, landed: float | None, comparability=Comparability.COMPARABLE) -> LandedCost:
    port = Port(origin, origin, "X", "XX")
    return LandedCost(
        quote=_quote(origin=port),
        destination=CN,
        requested_window=SEP,
        steps=(),
        landed=usd_mt(landed) if landed is not None else None,
        fob_equivalent=None,
        comparability=comparability,
        confidence=Confidence.INDICATIVE,
        freshness=Freshness.CURRENT,
        blockers=() if landed is not None else (Blocker("x", "y"),),
    )


def _ranking(*rows) -> OriginRanking:
    return OriginRanking(
        destination=CN,
        requested_window=SEP,
        as_of=date(2026, 8, 18),
        rows=rows,
        method_version="1.0.0",
        assumption_set_id="abc",
    )


def test_a_ranking_of_one_publishes_no_advantage():
    """'Cheapest of one' reads like a finding and contains none."""
    ranking = _ranking(_row("a", 500.0), _row("b", None, Comparability.MISSING_INPUT))
    assert ranking.cheapest is not None
    assert ranking.advantage_usd_mt is None
    assert not ranking.is_decisive


def test_the_advantage_is_against_the_second_rankable_row_not_the_cheapest_overall():
    """A cheaper *unrankable* row must not shrink the published advantage.

    This is the real Brazil case: its Paranaguá level lands below the US Gulf
    but publishes no shipment window, so it is excluded — and the advantage the
    page states is Argentina against the US Gulf, not against Brazil.
    """
    ranking = _ranking(
        _row("argentina", 570.88),
        _row("us_gulf", 609.61),
        _row("brazil", 595.87, Comparability.WINDOW_UNKNOWN),
    )
    assert ranking.cheapest.quote.origin.key == "argentina"
    assert ranking.advantage_usd_mt == pytest.approx(38.73, abs=0.01)
    assert ranking.advantage_pct == pytest.approx(6.353, abs=0.01)


def test_excluded_rows_are_kept_rather_than_dropped():
    ranking = _ranking(_row("a", 500.0), _row("b", None, Comparability.WINDOW_MISMATCH))
    assert [row.quote.origin.key for row in ranking.excluded] == ["b"]
    assert len(ranking.rows) == 2


def test_ranking_confidence_is_the_worst_of_the_ranked_rows():
    ranking = _ranking(_row("a", 500.0), _row("b", 510.0))
    assert ranking.confidence is Confidence.INDICATIVE


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
def test_fingerprint_is_stable_across_key_order():
    assert fingerprint({"a": 1, "b": 2}) == fingerprint({"b": 2, "a": 1})


def test_fingerprint_changes_with_a_value():
    assert fingerprint({"a": 1}) != fingerprint({"a": 1.0001})


def test_contract_ref_round_trips():
    ref = ContractRef("cbot", "ZS", "2026-11")
    assert ref.to_dict()["delivery_month"] == "2026-11"
