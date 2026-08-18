"""Combined futures / basis / FX / crush-yield scenarios over a sized hedge.

The arithmetic here is the part a trader checks by hand before believing any of
it, so the expected numbers are worked out in the docstrings rather than read
back off the implementation.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import Side, spec_for
from analysis.futures.hedge import (
    BasisConvention,
    FxExposure,
    PhysicalUnit,
    Rounding,
    propose_crush_hedge,
    propose_hedge,
)
from analysis.futures.scenarios import (
    Scenario,
    basis_shock,
    crush_yield_shock,
    default_panel_for,
    futures_shock,
    futures_shock_usd_mt,
    fx_shock,
    run_panel,
    run_scenario,
    value_share_shock,
)
from tests.test_futures_hedge import AS_OF, BEANS, MEAL, OIL, curve, exposure


def hedged(**kwargs):
    return propose_hedge(exposure(Side.LONG, **kwargs), curve("Soybeans", BEANS), as_of=AS_OF)


# ---------------------------------------------------------------------------
# One shock at a time
# ---------------------------------------------------------------------------


def test_a_perfect_hedge_leaves_only_the_residual_exposed_to_the_board():
    """10,000 MT long, short 73 ZSX26 at 1167.75, board +5%.

    Board move: 1167.75 x 5% = 58.3875 c/bu = 21.4537 USD/MT.
    Physical:  +21.4537 x 10,000       = +214,537
    Futures:   -73 x 58.3875 x 50      = -213,114
    Net:        21.4537 x 66.3234 MT   =   +1,423   (the unhedged residual)
    """
    hedge = hedged()
    result = run_scenario(hedge, Scenario("board +5%", (futures_shock(5.0),)))
    move_usd_mt = spec_for("Soybeans").native_to_usd_per_mt(1167.75 * 0.05)

    assert result.physical_pnl_usd == pytest.approx(move_usd_mt * 10_000)
    assert result.futures_pnl_usd == pytest.approx(-73 * 1167.75 * 0.05 * 50.0)
    assert result.net_pnl_usd == pytest.approx(move_usd_mt * hedge.residual_mt, rel=1e-9)
    assert result.residual_pnl_usd == pytest.approx(result.net_pnl_usd, rel=1e-9)
    assert result.hedge_effectiveness_pct > 99.0


def test_the_hedge_is_symmetric():
    hedge = hedged()
    up = run_scenario(hedge, Scenario("up", (futures_shock(5.0),)))
    down = run_scenario(hedge, Scenario("down", (futures_shock(-5.0),)))
    assert up.net_pnl_usd == pytest.approx(-down.net_pnl_usd)


def test_a_futures_hedge_does_nothing_at_all_about_basis():
    """The whole point of the section. 10 USD/MT x 10,000 MT = 100,000, unhedged."""
    hedge = hedged()
    result = run_scenario(hedge, Scenario("basis +10", (basis_shock(10.0),)))
    assert result.futures_pnl_usd == pytest.approx(0.0)
    assert result.basis_pnl_usd == pytest.approx(100_000.0)
    assert result.net_pnl_usd == pytest.approx(100_000.0)
    assert result.hedge_effectiveness_pct == pytest.approx(0.0)


def test_a_short_physical_loses_when_the_flat_price_rises():
    short = propose_hedge(exposure(Side.SHORT), curve("Soybeans", BEANS), as_of=AS_OF)
    result = run_scenario(short, Scenario("board +5%", (futures_shock(5.0),)))
    assert result.physical_pnl_usd < 0
    assert result.futures_pnl_usd > 0
    assert abs(result.net_pnl_usd) < abs(result.physical_pnl_usd) * 0.02


def test_an_absolute_shock_is_stated_in_usd_per_mt_and_converted_once():
    hedge = hedged()
    result = run_scenario(hedge, Scenario("+20/MT", (futures_shock_usd_mt(20.0),)))
    assert result.physical_pnl_usd == pytest.approx(20.0 * 10_000, rel=1e-9)


def test_a_shock_targeting_another_commodity_leaves_this_one_alone():
    hedge = hedged()
    result = run_scenario(
        hedge, Scenario("oil only", (futures_shock(10.0, commodity="Soybean Oil"),))
    )
    assert result.futures_pnl_usd == pytest.approx(0.0)
    assert result.physical_pnl_usd == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Combined
# ---------------------------------------------------------------------------


def test_board_basis_and_fx_combine_additively_and_are_attributed_separately():
    """A Brazilian seller's combination: board -5%, basis +10, BRL -4%.

    Physical  = (-21.4537 + 10) x 10,000
    Futures   = +73 x 58.3875 x 50
    FX        = home amount x (rate x 0.96 - rate)
    """
    exposed = exposure(Side.LONG, currency="BRL", fx_pair="BRL/USD")
    rate = 0.1958
    front_usd_mt = spec_for("Soybeans").native_to_usd_per_mt(1167.75)
    fx = FxExposure(
        pair="BRL/USD", rate=rate, rate_date=date(2026, 8, 11),
        amount_home=front_usd_mt * 10_000 / rate, amount_usd=front_usd_mt * 10_000,
    )
    hedge = propose_hedge(exposed, curve("Soybeans", BEANS), as_of=AS_OF, fx=fx)

    combined = run_scenario(hedge, Scenario(
        "combined", (futures_shock(-5.0), basis_shock(10.0), fx_shock(-4.0)),
    ))
    parts = [
        run_scenario(hedge, Scenario("board", (futures_shock(-5.0),))),
        run_scenario(hedge, Scenario("basis", (basis_shock(10.0),))),
        run_scenario(hedge, Scenario("fx", (fx_shock(-4.0),))),
    ]
    assert combined.net_pnl_usd == pytest.approx(sum(p.net_pnl_usd for p in parts), rel=1e-9)
    assert combined.fx_pnl_usd == pytest.approx(fx.amount_home * (rate * 0.96 - rate), rel=1e-9)
    assert combined.fx_pnl_usd < 0     # a weaker real earns fewer dollars
    assert combined.basis_pnl_usd == pytest.approx(100_000.0)


def test_an_fx_shock_on_a_usd_position_is_ignored_with_a_reason_rather_than_zeroed_silently():
    hedge = hedged()
    result = run_scenario(hedge, Scenario("fx", (fx_shock(-4.0),)))
    assert result.fx_pnl_usd == 0.0
    assert any("FX shock ignored" in note for note in result.notes)


def test_the_default_panel_drops_fx_shocks_for_a_usd_position():
    hedge = hedged()
    panel = default_panel_for(hedge)
    assert all(shock.kind != "fx" for scenario in panel for shock in scenario.shocks)
    assert len(run_panel(hedge, panel)) == len(panel)


# ---------------------------------------------------------------------------
# Crush
# ---------------------------------------------------------------------------


def crush_hedge():
    return propose_crush_hedge(
        exposure(Side.LONG, quantity=1_000),
        curve("Soybeans", BEANS), curve("Soybean Meal", MEAL), curve("Soybean Oil", OIL),
        as_of=AS_OF, rounding=Rounding.NEAREST,
    )


def test_a_yield_shock_resizes_the_product_legs_rather_than_repricing_them():
    hedge = crush_hedge()
    before = {leg.contract.spec.name: leg.contracts for leg in hedge.legs}
    result = run_scenario(hedge, Scenario(
        "lower oil yield", (crush_yield_shock("Soybean Oil", 0.16),),
    ))
    after = {leg.symbol: leg.contracts for leg in result.legs}
    # 1,000 MT x 0.16 = 160 MT / 27.2156 = 5.88 -> 6 contracts, down from 7.
    oil_symbol = next(s for s in after if s.startswith("ZL"))
    assert after[oil_symbol] == 6
    assert before["Soybean Oil"] == 7
    assert any("re-sized" in note for note in result.notes)


def test_a_value_share_shock_moves_the_split_and_holds_the_total():
    hedge = crush_hedge()
    result = run_scenario(hedge, Scenario("oil share +5pts", (value_share_shock(0.05),)))
    outcomes = {leg.symbol[:2]: leg for leg in result.legs}
    assert outcomes["ZL"].price_after > outcomes["ZL"].price_before   # oil richer
    assert outcomes["ZM"].price_after < outcomes["ZM"].price_before   # meal poorer

    from analysis.futures.hedge import CRUSH_YIELDS_MT

    def product_value(before: bool) -> float:
        total = 0.0
        for leg in result.legs:
            if leg.symbol.startswith("ZS"):
                continue
            spec = spec_for("Soybean Oil" if leg.symbol.startswith("ZL") else "Soybean Meal")
            price = leg.price_before if before else leg.price_after
            total += spec.native_to_usd_per_mt(price) * CRUSH_YIELDS_MT[spec.name]
        return total

    assert product_value(False) == pytest.approx(product_value(True), rel=1e-9)


def test_a_value_share_shock_on_a_flat_position_says_it_does_not_apply():
    result = run_scenario(hedged(), Scenario("share", (value_share_shock(0.05),)))
    assert any("no oil and meal pair" in note for note in result.notes)


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------


def test_a_scenario_id_is_stable_and_distinguishes_its_shocks():
    left = Scenario("x", (futures_shock(5.0), basis_shock(10.0)))
    right = Scenario("x", (futures_shock(5.0), basis_shock(10.0)))
    other = Scenario("x", (futures_shock(5.0), basis_shock(11.0)))
    assert left.identifier == right.identifier != other.identifier


def test_effectiveness_is_undefined_rather_than_flattering_when_nothing_moved():
    result = run_scenario(hedged(), Scenario("nothing", ()))
    assert result.unhedged_pnl_usd == 0.0
    assert result.hedge_effectiveness_pct is None


def test_a_flat_priced_exposure_still_reports_the_futures_leg_as_a_new_position():
    hedge = propose_hedge(
        exposure(Side.LONG, basis_convention=BasisConvention.FLAT_PRICE),
        curve("Soybeans", BEANS), as_of=AS_OF,
    )
    assert any(w.code == "flat_priced" for w in hedge.warnings)
    assert PhysicalUnit.METRIC_TON is hedge.exposure.unit
