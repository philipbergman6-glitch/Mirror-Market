"""Hedge sizing: long and short physicals, rounding, cross hedges, what is left over."""

from __future__ import annotations

from dataclasses import replace
from datetime import date

import pytest

from analysis.futures.curve import analyse_curve
from analysis.futures.domain import (
    YFINANCE_DELAYED,
    ContractQuote,
    NamedContract,
    PriceType,
    Side,
    named_contract,
    spec_for,
)
from analysis.futures.hedge import (
    BasisConvention,
    FxExposure,
    PhysicalExposure,
    PhysicalUnit,
    Rounding,
    build_hedge,
    fx_exposure_from_rate,
    propose_crush_hedge,
    propose_hedge,
    size_leg,
    to_metric_tons,
)
from analysis.futures.providers import CurveObservation

AS_OF = date(2026, 8, 18)


def quote(commodity: str, year: int, month: int, price: float, *, observed=AS_OF) -> ContractQuote:
    return ContractQuote(
        contract=named_contract(commodity, year, month),
        price=price,
        price_type=PriceType.DELAYED_CLOSE,
        observation_date=observed,
        provider=YFINANCE_DELAYED,
    )


def unencoded_contract(commodity: str, year: int, month: int) -> NamedContract:
    """A contract whose product has no encoded termination rule.

    All nine products this stack carries now have one — the two ICE softs were
    encoded off the rulebook once the counting convention could be proved. The
    *degradation* those two used to demonstrate is still the load-bearing
    behaviour (no days-to-expiry, no carry, no roll window, no hedge month, and
    a named refusal instead of a guess), and the next product added may well
    arrive without a rule, so it keeps its tests. It just needs a subject that
    cannot silently become encoded underneath them.
    """
    spec = replace(spec_for(commodity), expiry_rule=None, first_notice_rule=None)
    return NamedContract(spec=spec, year=year, month=month, last_trade=None, first_notice=None)


def unencoded_quote(
    commodity: str, year: int, month: int, price: float, *, observed=AS_OF
) -> ContractQuote:
    return ContractQuote(
        contract=unencoded_contract(commodity, year, month),
        price=price,
        price_type=PriceType.DELAYED_CLOSE,
        observation_date=observed,
        provider=YFINANCE_DELAYED,
    )


def curve(commodity: str, legs: list[ContractQuote], *, coherent: bool = True):
    return analyse_curve(
        CurveObservation(
            commodity=commodity,
            legs=tuple(legs),
            observation_date=legs[0].observation_date,
            fetched_date=legs[0].observation_date,
            coherent=coherent,
            coherence_note="" if coherent else "mixed sessions",
        ),
        as_of=AS_OF,
    )


BEANS = [quote("Soybeans", 2026, 11, 1167.75), quote("Soybeans", 2027, 1, 1183.00)]
MEAL = [quote("Soybean Meal", 2026, 12, 310.80), quote("Soybean Meal", 2027, 1, 313.30)]
OIL = [quote("Soybean Oil", 2026, 12, 68.18), quote("Soybean Oil", 2027, 1, 68.10)]


def exposure(side: Side, quantity=10_000, **kwargs) -> PhysicalExposure:
    defaults = dict(
        commodity="Soybeans",
        side=side,
        quantity=quantity,
        unit=PhysicalUnit.METRIC_TON,
        pricing_start=date(2026, 9, 1),
        pricing_end=date(2026, 10, 20),
        basis_convention=BasisConvention.BASIS_OVER_FUTURES,
        basis_usd_per_mt=-12.5,
        basis_source="AMS CIF NOLA",
    )
    defaults.update(kwargs)
    return PhysicalExposure(**defaults)


# ---------------------------------------------------------------------------
# Unit conversion into the sizing
# ---------------------------------------------------------------------------


def test_physical_unit_conversions():
    assert to_metric_tons(1, PhysicalUnit.METRIC_TON, "Soybeans") == 1.0
    assert to_metric_tons(1, PhysicalUnit.SHORT_TON, "Soybean Meal") == pytest.approx(0.907185)
    assert to_metric_tons(2204.62, PhysicalUnit.POUND, "Soybean Oil") == pytest.approx(1.0)
    # A soybean bushel is 60 lb and a corn bushel is 56 — one factor for both
    # would be a 7% error that looks like nothing.
    assert to_metric_tons(36.7437, PhysicalUnit.BUSHEL, "Soybeans") == pytest.approx(1.0)
    assert to_metric_tons(39.3683, PhysicalUnit.BUSHEL, "Corn") == pytest.approx(1.0)


def test_bushel_exposure_sizes_the_same_hedge_as_its_tonne_equivalent():
    in_bushels = exposure(Side.LONG, quantity=367_437, unit=PhysicalUnit.BUSHEL)
    in_tonnes = exposure(Side.LONG, quantity=10_000)
    assert in_bushels.quantity_mt == pytest.approx(in_tonnes.quantity_mt, rel=1e-9)


# ---------------------------------------------------------------------------
# Direction
# ---------------------------------------------------------------------------


def test_long_physical_is_hedged_short_and_short_physical_long():
    analysis = curve("Soybeans", BEANS)
    long_hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    short_hedge = propose_hedge(exposure(Side.SHORT), analysis, as_of=AS_OF)
    assert long_hedge.legs[0].side is Side.SHORT
    assert short_hedge.legs[0].side is Side.LONG
    # Same size either way — direction is not a size.
    assert long_hedge.legs[0].contracts == short_hedge.legs[0].contracts


def test_short_hedge_gains_when_the_board_rallies():
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    leg = hedge.legs[0]
    # Short 73 ZSX26 at 1167.75; a 10-cent fall is +73 x 10 x 50 = +$3,650.
    assert leg.value_change_usd(1157.75) == pytest.approx(leg.contracts * 10 * 50.0)
    assert leg.value_change_usd(1177.75) == pytest.approx(-leg.contracts * 10 * 50.0)


# ---------------------------------------------------------------------------
# Sizing and rounding
# ---------------------------------------------------------------------------


def test_hand_checked_contract_count_and_residual():
    """10,000 MT of beans / 136.0777 MT per ZS = 73.49 -> 73 contracts.

    Residual = 10,000 - 73 x 136.0777494 = 66.3235 MT, unhedged.
    """
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    assert hedge.legs[0].contracts == 73
    assert hedge.residual_mt == pytest.approx(66.3234, abs=1e-3)
    assert hedge.coverage_pct == pytest.approx(99.3368, abs=1e-3)
    assert hedge.is_over_hedged is False


def test_rounding_policy_changes_the_sign_of_the_residual():
    analysis = curve("Soybeans", BEANS)
    up = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF, rounding=Rounding.UP)
    down = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF, rounding=Rounding.DOWN)
    assert up.legs[0].contracts == 74
    assert down.legs[0].contracts == 73
    assert up.residual_mt < 0 and up.is_over_hedged is True
    assert down.residual_mt > 0


def test_rounding_up_never_leaves_the_physical_short_of_cover():
    analysis = curve("Soybean Oil", OIL)
    exposed = exposure(Side.LONG, commodity="Soybean Oil", quantity=500)
    hedge = propose_hedge(exposed, analysis, as_of=AS_OF, rounding=Rounding.UP)
    assert sum(leg.futures_mt for leg in hedge.legs) >= exposed.quantity_mt


# ---------------------------------------------------------------------------
# Month selection
# ---------------------------------------------------------------------------


def test_the_hedge_month_must_still_trade_when_the_physical_prices():
    analysis = curve("Soybeans", BEANS)
    late = exposure(Side.LONG, pricing_end=date(2026, 12, 1))
    hedge = propose_hedge(late, analysis, as_of=AS_OF)
    # ZSX26 stops trading 13 Nov 2026 — before the pricing window closes.
    assert hedge.legs[0].contract.symbol == "ZSF27"


def test_no_month_covers_the_window_is_an_alert_not_a_guess():
    analysis = curve("Soybeans", BEANS)
    far = exposure(Side.LONG, pricing_end=date(2028, 6, 1))
    hedge = propose_hedge(far, analysis, as_of=AS_OF)
    assert hedge.legs == ()
    assert any(w.code == "no_hedge_month" and w.severity == "alert" for w in hedge.warnings)


def test_first_notice_inside_the_pricing_window_is_flagged():
    """Last trade is not the binding date — first notice is.

    ZSX26 still *trades* until 13 Nov, so the month-selection rule accepts it
    for a window closing on the 5th. But its first notice day is 30 Oct, and a
    merchant still short into FND can be delivered against. Selecting it is
    right; saying nothing about the delivery risk would not be.
    """
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(
        exposure(Side.LONG, pricing_end=date(2026, 11, 5)), analysis, as_of=AS_OF
    )
    assert hedge.legs[0].contract.symbol == "ZSX26"
    assert any(
        w.code == "fnd_inside_pricing_window" and w.severity == "alert" for w in hedge.warnings
    )

    # A window that closes before FND raises no such warning.
    early = propose_hedge(
        exposure(Side.LONG, pricing_end=date(2026, 10, 15)), analysis, as_of=AS_OF
    )
    assert not any(w.code == "fnd_inside_pricing_window" for w in early.warnings)


def test_a_requested_month_absent_from_the_curve_falls_back_loudly():
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF, contract_symbol="ZSK27")
    assert any(w.code == "requested_month_absent" for w in hedge.warnings)
    assert hedge.legs[0].contract.symbol != "ZSK27"


# ---------------------------------------------------------------------------
# What the hedge leaves behind
# ---------------------------------------------------------------------------


def test_basis_risk_is_reported_per_tonne_of_physical():
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    assert hedge.basis_risk_usd_per_mt_move == pytest.approx(10_000.0)
    assert hedge.basis_value_usd == pytest.approx(-125_000.0)


def test_a_flat_priced_exposure_is_told_a_hedge_creates_a_position():
    analysis = curve("Soybeans", BEANS)
    hedge = propose_hedge(
        exposure(Side.LONG, basis_convention=BasisConvention.FLAT_PRICE), analysis, as_of=AS_OF
    )
    flat = [w for w in hedge.warnings if w.code == "flat_priced"]
    assert flat and flat[0].severity == "alert"


def test_an_incoherent_curve_is_an_alert_on_the_hedge():
    analysis = curve("Soybeans", BEANS, coherent=False)
    hedge = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    assert any(w.code == "curve_incoherent" and w.severity == "alert" for w in hedge.warnings)


def test_non_usd_exposure_without_a_pair_is_refused_at_construction():
    with pytest.raises(ValueError, match="names no FX pair"):
        exposure(Side.LONG, currency="BRL")


def test_fx_exposure_is_quantified_from_a_dated_rate():
    exposed = exposure(Side.LONG, currency="BRL", fx_pair="BRL/USD")
    fx = fx_exposure_from_rate(exposed, 429.07, (date(2026, 8, 11), 0.1958))
    assert fx.amount_usd == pytest.approx(429.07 * 10_000)
    assert fx.amount_home == pytest.approx(429.07 * 10_000 / 0.1958)
    assert fx.rate_date == date(2026, 8, 11)
    assert fx.hedged is False


def test_missing_fx_rate_is_an_alert_rather_than_a_zero():
    analysis = curve("Soybeans", BEANS)
    exposed = exposure(Side.LONG, currency="BRL", fx_pair="BRL/USD")
    fx = fx_exposure_from_rate(exposed, 429.07, None)
    hedge = build_hedge(exposed, (), as_of=AS_OF, fx=fx)
    assert any(w.code == "fx_rate_missing" and w.severity == "alert" for w in hedge.warnings)
    assert hedge.fx.amount_usd is None
    assert analysis is not None  # curve unused here; the FX gap alone must fire


def test_usd_exposure_reports_no_currency_leg():
    fx = fx_exposure_from_rate(exposure(Side.LONG), 429.07, None)
    assert fx.pair is None
    assert isinstance(fx, FxExposure)


# ---------------------------------------------------------------------------
# Cross hedges
# ---------------------------------------------------------------------------


def test_cross_hedge_is_labelled_when_the_contract_is_another_commodity():
    meal_curve = curve("Soybean Meal", MEAL)
    exposed = exposure(Side.LONG, commodity="Soybeans")
    hedge = propose_hedge(exposed, meal_curve, as_of=AS_OF, hedge_ratio=0.7333,
                          hedge_ratio_source="crush yield")
    assert hedge.legs[0].is_cross_hedge is True
    assert "cross hedge" in hedge.legs[0].cross_hedge_note


def test_crush_hedge_shorts_beans_and_buys_the_products_at_the_yields():
    hedge = propose_crush_hedge(
        exposure(Side.LONG, quantity=1_000),
        curve("Soybeans", BEANS), curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL),
        as_of=AS_OF,
    )
    by_symbol = {leg.contract.spec.root: leg for leg in hedge.legs}
    assert by_symbol["ZS"].side is Side.SHORT
    assert by_symbol["ZM"].side is Side.LONG
    assert by_symbol["ZL"].side is Side.LONG
    # 1,000 MT beans -> 733.3 MT meal / 90.7185 = 8.08 -> 8 contracts
    #                -> 183.3 MT oil  / 27.2156 = 6.73 -> 7 contracts
    assert by_symbol["ZM"].contracts == 8
    assert by_symbol["ZL"].contracts == 7
    assert by_symbol["ZM"].is_cross_hedge and by_symbol["ZL"].is_cross_hedge


def test_crush_coverage_is_the_bean_leg_not_the_sum_of_three():
    hedge = propose_crush_hedge(
        exposure(Side.LONG, quantity=1_000),
        curve("Soybeans", BEANS), curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL),
        as_of=AS_OF,
    )
    assert 90.0 < hedge.coverage_pct < 110.0


def test_crush_hedge_refuses_a_short_bean_position():
    with pytest.raises(ValueError, match="long bean position"):
        propose_crush_hedge(
            exposure(Side.SHORT, quantity=1_000),
            curve("Soybeans", BEANS), curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL),
            as_of=AS_OF,
        )


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------


def test_the_same_inputs_produce_the_same_proposal_id():
    analysis = curve("Soybeans", BEANS)
    first = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    second = propose_hedge(exposure(Side.LONG), analysis, as_of=AS_OF)
    assert first.identifier == second.identifier
    third = propose_hedge(exposure(Side.LONG, quantity=10_001), analysis, as_of=AS_OF)
    assert third.identifier != first.identifier


def test_size_leg_rejects_a_non_positive_hedge_ratio():
    with pytest.raises(ValueError, match="hedge_ratio must be positive"):
        size_leg(BEANS[0], side=Side.SHORT, physical_mt=100, hedge_ratio=0.0)
