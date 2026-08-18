"""The canonical landed-cost calculation (Phase 2).

Three deterministic scenarios with hand-verified arithmetic sit at the top —
they are the reference the rest of the module is checked against, and each one
is worked out longhand in its own docstring so a reviewer can disagree with the
number rather than with the code.

Everything below them is the fail-closed contract: a missing input blocks, an
expired input blocks, an unbridged incoterm blocks, and nothing after a blocked
rung is computed on a base it would be wrong against.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.origins.assumptions import Assumption, AssumptionSet
from analysis.origins.domain import (
    Carrier,
    Comparability,
    Confidence,
    CostComponent,
    FxObservation,
    Grade,
    Incoterm,
    Money,
    OriginQuote,
    Port,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    usd_mt,
)
from analysis.origins.landed_cost import (
    bridge_components,
    compute_landed_cost,
    window_verdict,
)

TODAY = date(2026, 8, 18)
SEP = ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026")
OCT = ShipmentWindow(date(2026, 10, 1), date(2026, 10, 31), label="Oct 2026")

GULF = Port("us_gulf", "US Gulf", "United States", "US")
PARANAGUA = Port("br_paranagua", "Paranaguá", "Brazil", "BR")
UP_RIVER = Port("ar_up_river", "Up-river", "Argentina", "AR")
CN = Port("cn_north", "North China", "China", "CN", role="destination")


def _assumption(component: str, value: float, unit: str, **kw) -> Assumption:
    return Assumption(
        id=kw.pop("id", f"{component}.{kw.get('origin') or 'any'}"),
        component=CostComponent(component),
        value=value,
        unit=unit,
        basis="fixture",
        source="test",
        entered_by="tests",
        entered_at=date(2026, 8, 1),
        expires_on=kw.pop("expires_on", date(2026, 12, 31)),
        confidence=Confidence.INDICATIVE,
        **kw,
    )


def _full_set(**overrides) -> AssumptionSet:
    """The complete North China stack used by the three reference scenarios."""
    values = {
        "elevation_us": 14.50,
        "freight_us": 52.00,
        "freight_br": 44.00,
        "freight_ar": 46.50,
        "insurance": 0.0012,
        "duty": 0.03,
        "vat": 0.09,
        "port": 6.00,
        "financing": 0.065,
        "financing_days": 45,
        "quality": 0.0,
        **overrides,
    }
    return AssumptionSet(assumptions=(
        _assumption("elevation", values["elevation_us"], "usd_per_mt",
                    origin="us_gulf", destination="cn_north"),
        _assumption("ocean_freight", values["freight_us"], "usd_per_mt",
                    origin="us_gulf", destination="cn_north", id="freight.us"),
        _assumption("ocean_freight", values["freight_br"], "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", id="freight.br"),
        _assumption("ocean_freight", values["freight_ar"], "usd_per_mt",
                    origin="ar_up_river", destination="cn_north", id="freight.ar"),
        _assumption("marine_insurance", values["insurance"], "fraction", destination="cn_north"),
        _assumption("import_duty", values["duty"], "fraction", destination="cn_north"),
        _assumption("import_vat", values["vat"], "fraction", destination="cn_north"),
        _assumption("destination_port_costs", values["port"], "usd_per_mt", destination="cn_north"),
        _assumption("financing", values["financing"], "rate_per_annum",
                    destination="cn_north", days=values["financing_days"]),
        _assumption("quality_adjustment", values["quality"], "usd_per_mt", destination="cn_north"),
    ))


def _quote(port: Port, price: float, **kw) -> OriginQuote:
    base = {
        "origin": port,
        "grade": Grade(specification="fixture"),
        "quote_kind": QuoteKind.PHYSICAL,
        "incoterm": Incoterm.FOB,
        "carrier": Carrier.VESSEL,
        "price": usd_mt(price),
        "native_price": price,
        "native_currency": "USD",
        "native_unit": "usd_per_mt",
        "observation_date": date(2026, 8, 11),
        "publication_date": None,
        "source": SourceRef("layer", "table", "Soybeans"),
        "shipment_window": SEP,
        "max_age_days": 30,
    }
    return OriginQuote(**{**base, **kw})


def _amounts(result) -> dict[CostComponent, float]:
    return {step.component: step.amount.amount for step in result.steps}


# ---------------------------------------------------------------------------
# Scenario 1 — US Gulf, CIF barge, the incoterm bridge
# ---------------------------------------------------------------------------
def test_scenario_1_us_gulf_cif_barge_to_north_china():
    """Hand-verified, AMS 3147 Sep-2026 1st-half bid of $12.6875/bu.

        12.6875 $/bu x 100 x 0.367437 =  466.19   origin (CIF barge NOLA)
                              + 14.50 =  480.69   elevation -> FOB vessel
                              + 52.00 =  532.69   ocean freight -> CFR
              0.12% x 532.69 =   0.64 =  533.33   marine insurance -> CIF
                 3% x 533.33 =  16.00 =  549.33   import duty
                 9% x 549.33 =  49.44 =  598.77   import VAT
                              +  6.00 =  604.77   destination port
        6.5%/yr x 45/365 x 604.77 = 4.85 = 609.62 financing
                              +  0.00 =  609.62   quality
    """
    result = compute_landed_cost(
        _quote(GULF, 466.1919, incoterm=Incoterm.CIF, carrier=Carrier.BARGE),
        CN, SEP, _full_set(), today=TODAY,
    )
    assert result.comparability is Comparability.COMPARABLE
    amounts = _amounts(result)
    assert amounts[CostComponent.ELEVATION] == pytest.approx(14.50)
    assert amounts[CostComponent.OCEAN_FREIGHT] == pytest.approx(52.00)
    assert amounts[CostComponent.MARINE_INSURANCE] == pytest.approx(0.6392, abs=0.001)
    assert amounts[CostComponent.IMPORT_DUTY] == pytest.approx(16.00, abs=0.01)
    assert amounts[CostComponent.IMPORT_VAT] == pytest.approx(49.44, abs=0.01)
    assert amounts[CostComponent.FINANCING] == pytest.approx(4.85, abs=0.01)
    assert result.fob_equivalent.amount == pytest.approx(480.69, abs=0.01)
    assert result.landed.amount == pytest.approx(609.61, abs=0.02)


# ---------------------------------------------------------------------------
# Scenario 2 — Argentina, FOB vessel, no bridge
# ---------------------------------------------------------------------------
def test_scenario_2_argentina_administered_fob_to_north_china():
    """Hand-verified, MAGyP 2026-09..2026-10 band at $452.00/MT.

        452.00                          origin (FOB vessel, administered)
        + 46.50            =  498.50    ocean freight
        0.12% x 498.50=0.60=  499.10    marine insurance
        3%   x 499.10 =14.97=  514.07   import duty
        9%   x 514.07 =46.27=  560.34   import VAT
        + 6.00             =  566.34    destination port
        6.5% x 45/365 x 566.34 = 4.54 = 570.88   financing

    Note there is no elevation rung: an FOB-vessel quote already sits on the
    common footing, so its bridge is empty rather than zero-costed.
    """
    result = compute_landed_cost(_quote(UP_RIVER, 452.00), CN, SEP, _full_set(), today=TODAY)
    amounts = _amounts(result)
    assert CostComponent.ELEVATION not in amounts
    assert result.fob_equivalent.amount == pytest.approx(452.00)
    assert amounts[CostComponent.IMPORT_DUTY] == pytest.approx(14.97, abs=0.01)
    assert amounts[CostComponent.IMPORT_VAT] == pytest.approx(46.27, abs=0.01)
    assert result.landed.amount == pytest.approx(570.88, abs=0.02)


# ---------------------------------------------------------------------------
# Scenario 3 — Brazil, BRL conversion, undated window
# ---------------------------------------------------------------------------
def test_scenario_3_brazil_converts_at_its_own_days_rate_and_is_never_ranked():
    """Hand-verified, AgRural BRL 2,433.33/MT at BRL/USD 0.1958480179.

        2,433.33 x 0.1958480179 = 476.56   origin (FOB vessel Paranaguá)
                       + 44.00  = 520.56   ocean freight
        0.12% x 520.56  =  0.62 = 521.18   marine insurance
        3%    x 521.18  = 15.64 = 536.82   import duty
        9%    x 536.82  = 48.31 = 585.13   import VAT
                       +  6.00 = 591.13    destination port
        6.5% x 45/365 x 591.13 = 4.74 = 595.87  financing

    The total is real and lands *below* the US Gulf. It is still not ranked:
    AgRural publishes no shipment period, so this level cannot be said to be
    quoted for September — or for any other month.
    """
    fx = FxObservation("BRL/USD", 0.1958480179309845, date(2026, 8, 11))
    quote = _quote(
        PARANAGUA,
        fx.to_usd(2433.33),
        native_price=2433.33,
        native_currency="BRL",
        native_unit="home_per_mt",
        fx=fx,
        shipment_window=None,
    )
    result = compute_landed_cost(quote, CN, SEP, _full_set(), today=TODAY)
    assert result.landed.amount == pytest.approx(595.87, abs=0.02)
    assert result.comparability is Comparability.WINDOW_UNKNOWN
    assert not result.is_rankable


# ---------------------------------------------------------------------------
# Fail closed
# ---------------------------------------------------------------------------
def test_a_missing_assumption_blocks_the_row_and_produces_no_total():
    partial = AssumptionSet(assumptions=(
        _assumption("elevation", 14.5, "usd_per_mt", origin="us_gulf", destination="cn_north"),
    ))
    result = compute_landed_cost(
        _quote(GULF, 466.19, incoterm=Incoterm.CIF, carrier=Carrier.BARGE),
        CN, SEP, partial, today=TODAY,
    )
    assert result.landed is None
    assert result.comparability is Comparability.MISSING_INPUT
    assert CostComponent.OCEAN_FREIGHT in result.missing_inputs
    assert any("ocean_freight" in blocker.message for blocker in result.blockers)


def test_a_missing_input_never_defaults_to_zero():
    """The whole point: a blocked row must not become a cheap row."""
    complete = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, _full_set(), today=TODAY)
    without_freight = AssumptionSet(assumptions=tuple(
        a for a in _full_set().assumptions if a.component is not CostComponent.OCEAN_FREIGHT
    ))
    blocked = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, without_freight, today=TODAY)
    assert complete.landed is not None
    assert blocked.landed is None


def test_nothing_after_a_blocked_rung_is_computed_on_the_wrong_base():
    """Duty is a percentage of the CIF value, so it cannot be struck on FOB.

    Without freight, computing duty anyway prints 3% of 466 rather than 3% of
    ~533 — a specific, confident, wrong number on a row that is already
    blocked. Every later rung is looked up (so the page can list them all) and
    none is applied.
    """
    without_freight = AssumptionSet(assumptions=tuple(
        a for a in _full_set().assumptions if a.component is not CostComponent.OCEAN_FREIGHT
    ))
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, without_freight, today=TODAY)
    applied = _amounts(result)
    assert CostComponent.IMPORT_DUTY not in applied
    assert CostComponent.IMPORT_VAT not in applied
    assert any("not computed once the waterfall blocked" in note for note in result.notes)


def test_an_expired_assumption_blocks_rather_than_being_used():
    lapsed = AssumptionSet(assumptions=tuple(
        a if a.component is not CostComponent.OCEAN_FREIGHT
        else _assumption(
            "ocean_freight", a.value, "usd_per_mt",
            origin=a.origin, destination=a.destination, id=a.id,
            expires_on=date(2026, 8, 10),
        )
        for a in _full_set().assumptions
    ))
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, lapsed, today=TODAY)
    assert result.landed is None
    assert any(blocker.code == "assumption_expired" for blocker in result.blockers)
    assert any("2026-08-10" in blocker.message for blocker in result.blockers)


def test_an_expired_route_assumption_does_not_fall_back_to_a_wider_one():
    """A lapsed US-Gulf rate must not silently become the global rate."""
    mixed = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 52.0, "usd_per_mt", origin="us_gulf",
                    destination="cn_north", id="lapsed", expires_on=date(2026, 8, 10)),
        _assumption("ocean_freight", 999.0, "usd_per_mt", id="global"),
    ))
    quote = _quote(GULF, 466.19)
    result = compute_landed_cost(quote, CN, SEP, mixed, today=TODAY)
    assert any(blocker.code == "assumption_expired" for blocker in result.blockers)
    assert 999.0 not in _amounts(result).values()


def test_two_equally_specific_live_assumptions_are_an_error_not_a_tie_break():
    from analysis.origins.assumptions import AssumptionError

    duplicated = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 52.0, "usd_per_mt",
                    origin="us_gulf", destination="cn_north", id="a"),
        _assumption("ocean_freight", 61.0, "usd_per_mt",
                    origin="us_gulf", destination="cn_north", id="b"),
    ))
    with pytest.raises(AssumptionError, match="equally specific"):
        duplicated.lookup(
            CostComponent.OCEAN_FREIGHT,
            origin="us_gulf", destination="cn_north", window=SEP, on=TODAY,
        )


def test_the_most_specific_live_assumption_wins():
    layered = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 60.0, "usd_per_mt", id="global"),
        _assumption("ocean_freight", 52.0, "usd_per_mt",
                    origin="us_gulf", destination="cn_north", id="route"),
    ))
    found = layered.lookup(
        CostComponent.OCEAN_FREIGHT,
        origin="us_gulf", destination="cn_north", window=SEP, on=TODAY,
    )
    assert found.value == 52.0


# ---------------------------------------------------------------------------
# Incoterms
# ---------------------------------------------------------------------------
def test_fob_vessel_owes_nothing_to_reach_the_common_footing():
    assert bridge_components("FOB", "vessel") == ()


def test_cif_barge_owes_an_elevation():
    assert bridge_components("CIF", "barge") == (CostComponent.ELEVATION,)


def test_an_unknown_delivery_term_blocks_rather_than_being_treated_as_free():
    result = compute_landed_cost(
        _quote(GULF, 466.19, incoterm=Incoterm.DDP, carrier=Carrier.RAIL),
        CN, SEP, _full_set(), today=TODAY,
    )
    assert bridge_components("DDP", "rail") is None
    assert result.comparability is Comparability.INCOTERM_UNBRIDGED
    assert result.fob_equivalent is None
    assert result.landed is None


def test_the_elevation_bridge_is_what_makes_the_two_us_and_brazil_quotes_comparable():
    """Without it the US Gulf reads 14.50 cheaper than it is, every single day."""
    with_bridge = compute_landed_cost(
        _quote(GULF, 466.19, incoterm=Incoterm.CIF, carrier=Carrier.BARGE),
        CN, SEP, _full_set(), today=TODAY,
    )
    as_if_fob = compute_landed_cost(_quote(GULF, 466.19), CN, SEP, _full_set(), today=TODAY)
    assert with_bridge.fob_equivalent.amount - as_if_fob.fob_equivalent.amount == pytest.approx(14.50)


# ---------------------------------------------------------------------------
# Window alignment
# ---------------------------------------------------------------------------
def test_a_quote_for_another_month_is_shown_but_never_ranked():
    result = compute_landed_cost(
        _quote(UP_RIVER, 452.0, shipment_window=OCT), CN, SEP, _full_set(), today=TODAY
    )
    assert result.comparability is Comparability.WINDOW_MISMATCH
    assert result.landed is not None       # the level is real and is rendered
    assert not result.is_rankable          # and it is not ranked against September


def test_an_undated_quote_is_window_unknown_not_prompt():
    assert window_verdict(_quote(PARANAGUA, 476.0, shipment_window=None), SEP) is (
        Comparability.WINDOW_UNKNOWN
    )


def test_an_overlapping_band_is_comparable():
    band = ShipmentWindow(date(2026, 9, 1), date(2027, 7, 31))
    assert window_verdict(_quote(UP_RIVER, 452.0, shipment_window=band), SEP) is (
        Comparability.COMPARABLE
    )


def test_a_stale_quote_is_labelled_and_not_ranked():
    result = compute_landed_cost(
        _quote(UP_RIVER, 452.0, observation_date=date(2026, 6, 1), max_age_days=7),
        CN, SEP, _full_set(), today=TODAY,
    )
    assert result.comparability is Comparability.STALE
    assert not result.is_rankable


# ---------------------------------------------------------------------------
# Extreme values
# ---------------------------------------------------------------------------
def test_a_negative_quality_adjustment_reduces_the_landed_cost():
    discounted = _full_set(quality=-8.0)
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, discounted, today=TODAY)
    assert result.landed.amount == pytest.approx(570.88 - 8.0, abs=0.02)


def test_a_zero_rate_stack_leaves_the_origin_price_plus_flat_costs():
    free = _full_set(insurance=0.0, duty=0.0, vat=0.0, financing=0.0, port=0.0, freight_ar=0.0)
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, free, today=TODAY)
    assert result.landed.amount == pytest.approx(452.0)


def test_an_absurd_flat_cost_is_rejected_at_entry_rather_than_computed():
    from analysis.origins.assumptions import AssumptionError

    with pytest.raises(AssumptionError, match="larger than the cargo"):
        _assumption("ocean_freight", 5200.0, "usd_per_mt")


def test_a_percentage_typed_as_a_whole_number_is_rejected():
    from analysis.origins.assumptions import AssumptionError

    with pytest.raises(AssumptionError, match="fraction must be in"):
        _assumption("import_duty", 3.0, "fraction")


def test_an_ad_valorem_component_cannot_be_entered_as_usd_per_mt():
    from analysis.origins.assumptions import AssumptionError

    with pytest.raises(AssumptionError, match="must be quoted in"):
        _assumption("import_duty", 14.0, "usd_per_mt")


def test_a_rate_over_time_component_needs_its_days():
    from analysis.origins.assumptions import AssumptionError

    with pytest.raises(AssumptionError, match="needs `days`"):
        _assumption("financing", 0.065, "rate_per_annum")


def test_a_zero_price_quote_still_costs_the_stack_without_dividing_by_it():
    result = compute_landed_cost(_quote(UP_RIVER, 0.0), CN, SEP, _full_set(), today=TODAY)
    assert result.landed is not None
    assert result.landed.amount > 0


# ---------------------------------------------------------------------------
# Confidence and provenance
# ---------------------------------------------------------------------------
def test_one_hand_entered_input_drags_an_executable_quote_to_indicative():
    board = _quote(UP_RIVER, 452.0, quote_kind=QuoteKind.BOARD)
    result = compute_landed_cost(board, CN, SEP, _full_set(), today=TODAY)
    assert board.base_confidence is Confidence.EXECUTABLE
    assert result.confidence is Confidence.INDICATIVE


def test_every_step_carries_its_owner_and_expiry():
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, _full_set(), today=TODAY)
    entered = [step for step in result.steps if step.assumption_id]
    assert entered
    assert all(step.entered_by and step.expires_on for step in entered)


def test_the_origin_step_records_the_fx_it_was_converted_at():
    fx = FxObservation("BRL/USD", 0.19584, date(2026, 8, 11))
    quote = _quote(
        PARANAGUA, fx.to_usd(2433.33), native_price=2433.33,
        native_currency="BRL", native_unit="home_per_mt", fx=fx,
    )
    result = compute_landed_cost(quote, CN, SEP, _full_set(), today=TODAY)
    assert "BRL/USD" in result.steps[0].basis
    assert "2026-08-11" in result.steps[0].basis


def test_the_method_version_is_stamped_on_every_result():
    import config

    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, _full_set(), today=TODAY)
    assert result.method_version == config.LANDED_COST_METHOD_VERSION


def test_money_guard_survives_the_whole_waterfall():
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, _full_set(), today=TODAY)
    for step in result.steps:
        assert isinstance(step.amount, Money)
        assert step.amount.currency == "USD" and step.amount.unit == "usd_per_mt"
