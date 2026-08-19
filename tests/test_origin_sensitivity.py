"""Which input flips the ranking, and by how much.

The point of the module under test is a decision boundary rather than a
forecast: "freight has to be 12 USD/MT wrong before the answer changes" is
something a trader can judge, and "Brazil is 8 dollars cheaper" on its own is
not.

The solved move is checked the only way it can honestly be checked — by
applying it as an override and re-running the whole waterfall. A derivative
that agrees with itself proves nothing; a derivative that agrees with the
arithmetic proves the two have not drifted.

Every value here is a fixture.
"""

from __future__ import annotations

from datetime import date

import pytest

import config
from analysis.origins.assumptions import Assumption, AssumptionSet
from analysis.origins.domain import (
    Carrier,
    Confidence,
    CostComponent,
    Grade,
    Incoterm,
    OriginRanking,
    Port,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    usd_mt,
)
from analysis.origins.landed_cost import compute_landed_cost
from analysis.origins.scenarios import (
    freight_breakeven,
    input_flip_moves,
    marginal_landed_per_unit,
    most_fragile_input,
)

TODAY = date(2026, 8, 18)
OCT = ShipmentWindow(date(2026, 10, 1), date(2026, 10, 31), label="Oct 2026")
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
        entered_by="tests@example.com",
        entered_at=date(2026, 8, 1),
        expires_on=date(2026, 12, 31),
        confidence=Confidence.INDICATIVE,
        **kw,
    )


def _assumptions(freight_br: float = 44.0, freight_ar: float = 46.5) -> AssumptionSet:
    return AssumptionSet(assumptions=(
        _assumption("ocean_freight", freight_br, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", id="freight.br"),
        _assumption("ocean_freight", freight_ar, "usd_per_mt",
                    origin="ar_up_river", destination="cn_north", id="freight.ar"),
        _assumption("marine_insurance", 0.0012, "fraction", destination="cn_north"),
        _assumption("import_duty", 0.03, "fraction", destination="cn_north"),
        _assumption("import_vat", 0.09, "fraction", destination="cn_north"),
        _assumption("destination_port_costs", 6.0, "usd_per_mt", destination="cn_north"),
        _assumption("financing", 0.065, "rate_per_annum", destination="cn_north", days=45),
        _assumption("quality_adjustment", -2.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", id="quality.br"),
        _assumption("quality_adjustment", 0.0, "usd_per_mt",
                    origin="ar_up_river", destination="cn_north", id="quality.ar"),
    ))


def _quote(port: Port, price: float):
    from analysis.origins.domain import OriginQuote

    return OriginQuote(
        origin=port,
        grade=Grade(specification="fixture"),
        quote_kind=QuoteKind.PHYSICAL,
        incoterm=Incoterm.FOB,
        carrier=Carrier.VESSEL,
        price=usd_mt(price),
        native_price=price,
        native_currency="USD",
        native_unit="usd_per_mt",
        observation_date=date(2026, 8, 17),
        publication_date=None,
        source=SourceRef("layer", "table", "Soybeans"),
        shipment_window=OCT,
        max_age_days=30,
    )


def _ranking(brazil: float = 420.0, argentina: float = 430.0, **kw) -> OriginRanking:
    assumptions = kw.pop("assumptions", None) or _assumptions()
    rows = tuple(
        compute_landed_cost(_quote(port, price), CN, OCT, assumptions, today=TODAY)
        for port, price in ((PARANAGUA, brazil), (UP_RIVER, argentina))
    )
    return OriginRanking(
        destination=CN,
        requested_window=OCT,
        as_of=TODAY,
        rows=rows,
        method_version=config.LANDED_COST_METHOD_VERSION,
        assumption_set_id=assumptions.set_id,
        observation_spread_days=0,
    )


def _landed(origin_price: float, port: Port, assumptions: AssumptionSet, **overrides) -> float:
    row = compute_landed_cost(
        _quote(port, origin_price), CN, OCT, assumptions, today=TODAY, overrides=overrides or None
    )
    assert row.landed_usd_mt is not None
    return row.landed_usd_mt


# ---------------------------------------------------------------------------
# The derivative
# ---------------------------------------------------------------------------
def test_a_freight_dollar_costs_more_than_a_dollar_on_a_duty_paying_route():
    row = _ranking().rankable[0]
    marginal = marginal_landed_per_unit(row, CostComponent.OCEAN_FREIGHT)
    assert marginal is not None
    # duty and VAT are charged on top of freight, and financing on top of those
    assert marginal > 1.10


def test_the_derivative_matches_a_re_run_of_the_whole_waterfall():
    assumptions = _assumptions()
    row = _ranking(assumptions=assumptions).rankable[0]
    for component, bump in (
        (CostComponent.OCEAN_FREIGHT, 1.0),
        (CostComponent.DESTINATION_PORT_COSTS, 1.0),
        (CostComponent.IMPORT_DUTY, 0.01),
        (CostComponent.FINANCING, 0.01),
        (CostComponent.MARINE_INSURANCE, 0.0005),
        (CostComponent.QUALITY_ADJUSTMENT, 1.0),
    ):
        marginal = marginal_landed_per_unit(row, component)
        assert marginal is not None, component
        current = next(
            step.rate if step.rate is not None else step.amount.amount
            for step in row.steps if step.component is component
        )
        moved = _landed(420.0, PARANAGUA, assumptions, **{component: current + bump})
        assert moved - row.landed_usd_mt == pytest.approx(marginal * bump, abs=1e-9)


def test_a_financing_rate_of_exactly_zero_reports_no_derivative_rather_than_a_wrong_one():
    """The carry period is recovered from the step; at a zero rate there is no trace of it."""
    assumptions = AssumptionSet(assumptions=tuple(
        item if item.component is not CostComponent.FINANCING
        else _assumption("financing", 0.0, "rate_per_annum", destination="cn_north", days=45)
        for item in _assumptions().assumptions
    ))
    row = _ranking(assumptions=assumptions).rankable[0]
    assert marginal_landed_per_unit(row, CostComponent.FINANCING) is None


# ---------------------------------------------------------------------------
# The flip table
# ---------------------------------------------------------------------------
def test_the_solved_flip_move_actually_levels_the_two_landed_totals():
    assumptions = _assumptions()
    ranking = _ranking(assumptions=assumptions)
    leader, challenger = ranking.rankable[0], ranking.rankable[1]
    for move in input_flip_moves(ranking):
        if move.move is None:
            continue
        current = next(
            step.rate if step.rate is not None else step.amount.amount
            for step in leader.steps if step.component is move.component
        )
        flipped = _landed(420.0, PARANAGUA, assumptions, **{move.component: current + move.move})
        assert flipped == pytest.approx(challenger.landed_usd_mt, abs=1e-6)


def test_a_shared_input_cannot_flip_the_answer_and_says_so_instead_of_printing_a_number():
    ranking = _ranking()
    duty = next(m for m in input_flip_moves(ranking) if m.component is CostComponent.IMPORT_DUTY)
    assert duty.shared is True
    assert duty.move is None
    assert duty.reason and "moves both landed totals together" in duty.reason


def test_a_per_origin_input_carries_both_sides_of_the_move():
    ranking = _ranking()
    freight = next(
        m for m in input_flip_moves(ranking) if m.component is CostComponent.OCEAN_FREIGHT
    )
    assert freight.shared is False
    assert freight.move is not None and freight.move > 0      # leader's freight rises
    assert freight.challenger_move is not None                 # or the challenger's falls
    assert freight.leader_assumption_id == "freight.br"
    assert freight.challenger_assumption_id == "freight.ar"


def test_the_table_is_ordered_by_how_wrong_the_input_would_have_to_be():
    ranking = _ranking()
    moves = [m for m in input_flip_moves(ranking) if m.move_pct_of_current is not None]
    assert moves == sorted(moves, key=lambda m: m.move_pct_of_current or 0.0)
    assert most_fragile_input(ranking) == moves[0]


def test_the_most_fragile_input_is_the_one_with_the_least_room():
    """A wide freight advantage is harder to overturn than a narrow one."""
    wide = most_fragile_input(_ranking(brazil=380.0, argentina=460.0))
    narrow = most_fragile_input(_ranking(brazil=428.0, argentina=430.0))
    assert wide is not None and narrow is not None
    assert narrow.move is not None and wide.move is not None
    assert narrow.move < wide.move


def test_a_ranking_of_one_produces_no_flip_table_at_all():
    single = _ranking()
    single = OriginRanking(
        destination=CN,
        requested_window=OCT,
        as_of=TODAY,
        rows=single.rows[:1],
        method_version=single.method_version,
        assumption_set_id=single.assumption_set_id,
    )
    assert input_flip_moves(single) == ()
    assert most_fragile_input(single) is None
    assert freight_breakeven(single) is None


def test_freight_breakeven_still_agrees_with_the_general_table():
    ranking = _ranking()
    breakeven = freight_breakeven(ranking)
    freight = next(
        m for m in input_flip_moves(ranking) if m.component is CostComponent.OCEAN_FREIGHT
    )
    assert breakeven is not None and freight.move is not None
    assert breakeven.move_usd_mt == pytest.approx(freight.move, abs=1e-9)


def test_a_blocked_ranking_has_nothing_to_flip():
    """Missing inputs block before any sensitivity is meaningful."""
    thin = AssumptionSet(assumptions=tuple(
        item for item in _assumptions().assumptions
        if item.component is not CostComponent.OCEAN_FREIGHT
    ))
    assert input_flip_moves(_ranking(assumptions=thin)) == ()
