"""Scenario and sensitivity engine for origin comparison (Phase 2).

Nobody trades on a point estimate of a landed cost built on a hand-entered
freight number. The useful question is not "what is the landed cost" but "how
wrong does freight have to be before the cheapest origin changes", and this
module answers that one.

Three ideas:

**A shock names what it moves.** ``FreightShock``, ``FxShock``, ``BasisShock``,
``RateShock`` — each targets a specific input, and each records the input's
identity in the result. A scenario that shifts "everything by 5%" teaches
nothing; a scenario that shifts *US Gulf ocean freight* by +10 USD/MT and
flips the ranking teaches the whole thing.

**A shock is applied to the input, never to the answer.** Every scenario
re-runs :func:`~analysis.origins.landed_cost.compute_landed_cost` from shocked
inputs. Adjusting a computed landed total by a percentage would silently skip
the ad-valorem chain — a freight rise raises the CIF value, which raises duty,
which raises VAT — and would understate every freight sensitivity on a duty-
paying route.

**A scenario is reproducible or it is not a scenario.** Every result carries
the method version, the assumption-set fingerprint and a hash of its own
shocks; the same inputs give the same hash and the same numbers, on any
machine and in any order. There is no clock, no RNG and no iteration-order
dependence anywhere in this module.

The breakeven search is the piece a trader will actually use:
:func:`freight_breakeven` reports, for the current cheapest origin, how far
its freight would have to move before it loses first place. That is a decision
boundary rather than a forecast, which is the only kind of forward-looking
number this stack is entitled to publish.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from analysis.origins.assumptions import REQUIRED_UNIT, AssumptionSet
from analysis.origins.domain import (
    COMPONENT_ORDER,
    Comparability,
    CostComponent,
    FxObservation,
    LandedCost,
    Money,
    OriginQuote,
    OriginRanking,
    QuoteKind,
    fingerprint,
    usd_mt,
)
from analysis.origins.landed_cost import compute_landed_cost
from analysis.origins.sources import to_usd_per_mt

log = logging.getLogger(__name__)

# Quote kinds whose price contains a physical premium over the board. A basis
# shock moves these and leaves a board settlement alone: shifting "basis" on a
# CBOT futures price is shifting the flat price and calling it something else.
_BASIS_BEARING_KINDS = frozenset({
    QuoteKind.PHYSICAL,
    QuoteKind.ADMINISTERED,
    QuoteKind.WEEKLY_ASSESSMENT,
    QuoteKind.BOARD_LAST_TRADED,
})


@dataclass(frozen=True)
class Shock:
    """One named change to one named input."""

    kind: str            # freight | fx | basis | rate | cost
    target: str          # component value, FX pair, or origin port key; "*" for all
    mode: str            # absolute | relative
    value: float
    label: str

    def __post_init__(self) -> None:
        if self.kind not in {"freight", "fx", "basis", "rate", "cost"}:
            raise ValueError(f"unknown shock kind {self.kind!r}")
        if self.mode not in {"absolute", "relative"}:
            raise ValueError(f"unknown shock mode {self.mode!r}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "target": self.target,
            "mode": self.mode,
            "value": self.value,
            "label": self.label,
        }


def freight_shock(usd_mt_delta: float, *, origin: str = "*") -> Shock:
    return Shock(
        kind="freight",
        target=origin,
        mode="absolute",
        value=usd_mt_delta,
        label=f"ocean freight {usd_mt_delta:+.1f} USD/MT"
        + ("" if origin == "*" else f" ({origin} only)"),
    )


def fx_shock(pct: float, *, pair: str = "*") -> Shock:
    return Shock(
        kind="fx",
        target=pair,
        mode="relative",
        value=pct,
        label=f"{'all local currencies' if pair == '*' else pair} {pct:+.1f}% vs USD",
    )


def basis_shock(usd_mt_delta: float, *, origin: str = "*") -> Shock:
    return Shock(
        kind="basis",
        target=origin,
        mode="absolute",
        value=usd_mt_delta,
        label=f"physical premium {usd_mt_delta:+.1f} USD/MT"
        + ("" if origin == "*" else f" ({origin} only)"),
    )


def tariff_shock(new_rate: float) -> Shock:
    return Shock(
        kind="cost",
        target=CostComponent.IMPORT_DUTY.value,
        mode="absolute",
        value=new_rate,
        label=f"import duty set to {new_rate * 100:.4g}%",
    )


def financing_shock(new_rate: float) -> Shock:
    return Shock(
        kind="rate",
        target=CostComponent.FINANCING.value,
        mode="absolute",
        value=new_rate,
        label=f"financing set to {new_rate * 100:.4g}%/yr",
    )


@dataclass(frozen=True)
class Scenario:
    """A named bundle of shocks, applied together."""

    id: str
    label: str
    shocks: tuple[Shock, ...]
    rationale: str = ""

    @property
    def digest(self) -> str:
        return fingerprint([shock.to_dict() for shock in self.shocks])

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "rationale": self.rationale,
            "digest": self.digest,
            "shocks": [shock.to_dict() for shock in self.shocks],
        }


BASE = Scenario(id="base", label="As observed", shocks=(), rationale="No shocks applied.")


def _shock_quote(quote: OriginQuote, scenario: Scenario) -> OriginQuote:
    """Apply FX and basis shocks to one quote, re-deriving its USD price.

    An FX shock re-runs the *conversion*, it does not scale the USD figure: for
    a ``home_per_mt`` leg those are the same number, but for a leg already
    quoted in USD (Argentina's administered FOB) they are not — the second
    would move a price that has no local-currency exposure at all.
    """
    price = quote.price
    fx = quote.fx
    notes = list(quote.notes)

    for shock in scenario.shocks:
        if shock.kind == "fx" and fx is not None:
            if shock.target not in ("*", fx.pair):
                continue
            shocked = FxObservation(
                pair=fx.pair,
                usd_per_unit=fx.usd_per_unit * (1.0 + shock.value / 100.0),
                observed_on=fx.observed_on,
            )
            converted = to_usd_per_mt(
                quote.native_price,
                unit=quote.native_unit,
                key=quote.source.key,
                fx=shocked.usd_per_unit,
            )
            if converted is None:
                continue
            fx = shocked
            price = usd_mt(converted)
            notes.append(f"scenario: {shock.label}")
        elif shock.kind == "basis" and quote.quote_kind in _BASIS_BEARING_KINDS:
            if shock.target not in ("*", quote.origin.key):
                continue
            price = price + usd_mt(shock.value)
            notes.append(f"scenario: {shock.label}")

    if price is quote.price and fx is quote.fx:
        return quote
    return replace(quote, price=Money(price.amount), fx=fx, notes=tuple(notes))


def _overrides(quote: OriginQuote, scenario: Scenario, base_row: LandedCost) -> dict[CostComponent, float]:
    """Component value overrides implied by a scenario, keyed by component.

    A freight shock is expressed as a delta against the *base row's own* freight
    assumption, which is why the base row is a parameter: "+10 USD/MT" is only
    meaningful relative to the number it moves, and a scenario that silently
    became an absolute 10 would look like a collapse in freight.
    """
    base_amounts = {step.component: step for step in base_row.steps}
    out: dict[CostComponent, float] = {}
    for shock in scenario.shocks:
        if shock.kind == "freight":
            if shock.target not in ("*", quote.origin.key):
                continue
            step = base_amounts.get(CostComponent.OCEAN_FREIGHT)
            if step is None:
                continue  # blocked route — nothing to vary, and it stays blocked
            current = step.amount.amount
            out[CostComponent.OCEAN_FREIGHT] = (
                current + shock.value if shock.mode == "absolute"
                else current * (1.0 + shock.value / 100.0)
            )
        elif shock.kind in ("cost", "rate"):
            component = CostComponent(shock.target)
            step = base_amounts.get(component)
            if step is None:
                continue
            if shock.mode == "absolute":
                out[component] = shock.value
            else:
                base_value = step.rate if step.rate is not None else step.amount.amount
                out[component] = base_value * (1.0 + shock.value / 100.0)
    return out


@dataclass(frozen=True)
class ScenarioResult:
    """One scenario's ranking, and what it did to the answer."""

    scenario: Scenario
    ranking: OriginRanking
    base_cheapest: str | None
    cheapest: str | None
    landed_delta_usd_mt: dict[str, float]

    @property
    def flips_the_answer(self) -> bool:
        return (
            self.cheapest is not None
            and self.base_cheapest is not None
            and self.cheapest != self.base_cheapest
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario.to_dict(),
            "base_cheapest": self.base_cheapest,
            "cheapest": self.cheapest,
            "flips_the_answer": self.flips_the_answer,
            "advantage_usd_mt": self.ranking.advantage_usd_mt,
            "landed_delta_usd_mt": self.landed_delta_usd_mt,
            "is_decisive": self.ranking.is_decisive,
        }


def run_scenario(
    base: OriginRanking,
    scenario: Scenario,
    assumptions: AssumptionSet,
    *,
    today: date,
) -> ScenarioResult:
    """Re-cost every row under one scenario. Pure — same inputs, same output."""
    rows: list[LandedCost] = []
    deltas: dict[str, float] = {}
    for base_row in base.rows:
        quote = _shock_quote(base_row.quote, scenario)
        row = compute_landed_cost(
            quote,
            base.destination,
            base.requested_window,
            assumptions,
            today=today,
            overrides=_overrides(quote, scenario, base_row),
        )
        # The observation-spread verdict is a property of the whole set and was
        # already decided on the base ranking; a scenario cannot move a date, so
        # it is carried across rather than silently re-earned.
        if base_row.comparability is Comparability.OBSERVATION_SPREAD and row.is_rankable:
            row = replace(row, comparability=Comparability.OBSERVATION_SPREAD)
        rows.append(row)
        if base_row.landed_usd_mt is not None and row.landed_usd_mt is not None:
            deltas[row.quote.origin.key] = row.landed_usd_mt - base_row.landed_usd_mt

    ranking = replace(
        base,
        rows=tuple(rows),
        notes=(*base.notes, f"scenario {scenario.id}: {scenario.label}"),
    )
    return ScenarioResult(
        scenario=scenario,
        ranking=ranking,
        base_cheapest=base.cheapest.quote.origin.key if base.cheapest else None,
        cheapest=ranking.cheapest.quote.origin.key if ranking.cheapest else None,
        landed_delta_usd_mt=deltas,
    )


# The standing panel. Fixed, not generated: a sensitivity table whose rows
# change between runs cannot be read as a time series, and the whole value of
# "+10 freight flips it" is being able to see that it did not, yesterday.
STANDARD_SCENARIOS: tuple[Scenario, ...] = (
    BASE,
    Scenario(
        id="freight_up_10",
        label="Ocean freight +10 USD/MT",
        shocks=(freight_shock(10.0),),
        rationale=(
            "A Panamax rate move of this size is an ordinary quarter, and freight is "
            "the largest hand-entered input in the stack."
        ),
    ),
    Scenario(
        id="freight_down_10",
        label="Ocean freight −10 USD/MT",
        shocks=(freight_shock(-10.0),),
        rationale="The same move the other way — the ranking should be symmetric.",
    ),
    Scenario(
        id="brl_weaker_5",
        label="BRL 5% weaker vs USD",
        shocks=(fx_shock(-5.0, pair="BRL/USD"),),
        rationale=(
            "Brazil's leg is quoted in BRL, so a weaker real cuts its USD offer "
            "without any change in the local market."
        ),
    ),
    Scenario(
        id="brl_stronger_5",
        label="BRL 5% stronger vs USD",
        shocks=(fx_shock(5.0, pair="BRL/USD"),),
        rationale="The mirror case, which is where Brazil loses its edge.",
    ),
    Scenario(
        id="basis_firm_5",
        label="Physical premiums +5 USD/MT",
        shocks=(basis_shock(5.0),),
        rationale=(
            "Every physical and administered leg firms together — the board is "
            "untouched, so this isolates the cash market."
        ),
    ),
    Scenario(
        id="duty_free",
        label="Import duty removed",
        shocks=(tariff_shock(0.0),),
        rationale=(
            "The policy case. Duty is ad valorem, so removing it also removes the "
            "VAT charged on top of it — which is why a scenario re-runs the stack "
            "rather than subtracting a line."
        ),
    ),
    Scenario(
        id="financing_8pct",
        label="Financing at 8%/yr",
        shocks=(financing_shock(0.08),),
        rationale="Carry is the input a buyer controls least and notices last.",
    ),
)


def run_panel(
    base: OriginRanking,
    assumptions: AssumptionSet,
    *,
    today: date,
    scenarios: tuple[Scenario, ...] = STANDARD_SCENARIOS,
) -> tuple[ScenarioResult, ...]:
    return tuple(run_scenario(base, scenario, assumptions, today=today) for scenario in scenarios)


@dataclass(frozen=True)
class Breakeven:
    """How far one input can move before the answer changes."""

    origin: str
    challenger: str
    component: CostComponent
    move_usd_mt: float
    current_advantage_usd_mt: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "challenger": self.challenger,
            "component": self.component.value,
            "move_usd_mt": self.move_usd_mt,
            "current_advantage_usd_mt": self.current_advantage_usd_mt,
        }


def freight_breakeven(ranking: OriginRanking) -> Breakeven | None:
    """How much the cheapest origin's freight can rise before it loses first place.

    Solved rather than searched. On a duty-paying route a dollar of freight
    costs more than a dollar landed — duty and VAT are charged on top of it —
    so the freight move that closes a given landed gap is the gap divided by
    the *marginal* landed cost of a freight dollar. That multiplier is read off
    the row's own waterfall instead of assumed, so a duty-free route and a
    12%-all-in route give different, correct answers from the same code.
    """
    ordered = ranking.rankable
    if len(ordered) < 2:
        return None
    leader, challenger = ordered[0], ordered[1]
    gap = challenger.landed_usd_mt - leader.landed_usd_mt  # type: ignore[operator]
    multiplier = _marginal_landed_per_freight_dollar(leader)
    if multiplier <= 0:
        return None
    return Breakeven(
        origin=leader.quote.origin.key,
        challenger=challenger.quote.origin.key,
        component=CostComponent.OCEAN_FREIGHT,
        move_usd_mt=gap / multiplier,
        current_advantage_usd_mt=gap,
    )


def _compounding_after(row: LandedCost, component: CostComponent) -> float:
    """What a dollar added at ``component``'s rung is worth by the end of the stack.

    Every ad-valorem rung applied *after* it compounds onto it, and financing
    compounds onto those. Reading the rates off the row's own steps keeps this
    exact for whatever stack the route actually has, rather than assuming the
    one North China happens to use.
    """
    from analysis.origins.domain import COMPONENT_ORDER

    steps = {step.component: step for step in row.steps}
    index = COMPONENT_ORDER.index(component)
    multiplier = 1.0
    for later in COMPONENT_ORDER[index + 1:]:
        step = steps.get(later)
        if step is None or step.rate is None:
            continue
        if later is CostComponent.FINANCING:
            # rate x days / 365; the days are folded into the amount already, so
            # recover the effective fraction from the step itself.
            effective = (
                step.amount.amount / (step.running_total.amount - step.amount.amount)
                if step.running_total.amount != step.amount.amount else 0.0
            )
            multiplier *= 1.0 + effective
        else:
            multiplier *= 1.0 + step.rate
    return multiplier


def marginal_landed_per_unit(row: LandedCost, component: CostComponent) -> float | None:
    """d(landed) / d(this input's own value), in the unit the input is entered in.

    Three unit families, three derivatives, and they are genuinely different
    numbers: a dollar of freight is worth its own compounding; a point of duty
    is worth the CIF value it is charged on, compounded; a point of financing is
    worth that value again scaled by the carry period. The period is recovered
    from the step (``amount / rate``) rather than re-read from the assumption,
    so a scenario override is differentiated as the row actually stands.

    ``None`` where the derivative cannot be recovered — a financing rate entered
    as exactly zero carries no trace of its own day count, and inventing one to
    return a number would be the fabrication this package is built against.
    """
    from analysis.origins.domain import AD_VALOREM_COMPONENTS, RATE_TIME_COMPONENTS

    steps = {step.component: step for step in row.steps}
    step = steps.get(component)
    if step is None:
        return None
    after = _compounding_after(row, component)
    base = step.running_total.amount - step.amount.amount
    if component in AD_VALOREM_COMPONENTS:
        return base * after
    if component in RATE_TIME_COMPONENTS:
        if not step.rate:
            return None
        # amount = base x rate x days/365, so amount/rate is base x days/365 —
        # exactly the coefficient the rate multiplies.
        return (step.amount.amount / step.rate) * after
    return after


def _marginal_landed_per_freight_dollar(row: LandedCost) -> float:
    value = marginal_landed_per_unit(row, CostComponent.OCEAN_FREIGHT)
    return value if value is not None else 0.0


@dataclass(frozen=True)
class FlipMove:
    """How far one named input has to move before the ranking changes.

    Two sides, because a trader can be wrong in either direction: ``move`` is
    how far the *leader's* input must rise, ``challenger_move`` how far the
    *challenger's* must fall. Both are stated in the input's own entered unit —
    a fraction for duty, USD/MT for freight — because a "0.02 move" and a "2
    point move" are the same thing said in the two units this stack keeps
    deliberately distinct.

    ``shared`` is the case worth reading twice. A destination-scoped input —
    duty, VAT, discharge — is the *same entry* on both rows, so moving it moves
    both landed totals and mostly cannot flip anything. Where it genuinely
    cannot, ``move`` is ``None`` and ``reason`` says so, rather than printing a
    large number that would read as "unlikely, but possible".
    """

    component: CostComponent
    unit: str
    leader: str
    challenger: str
    shared: bool
    current_advantage_usd_mt: float
    move: float | None = None
    challenger_move: float | None = None
    current_value: float | None = None
    move_pct_of_current: float | None = None
    leader_assumption_id: str | None = None
    challenger_assumption_id: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component.value,
            "unit": self.unit,
            "leader": self.leader,
            "challenger": self.challenger,
            "shared": self.shared,
            "current_advantage_usd_mt": self.current_advantage_usd_mt,
            "move": self.move,
            "challenger_move": self.challenger_move,
            "current_value": self.current_value,
            "move_pct_of_current": self.move_pct_of_current,
            "leader_assumption_id": self.leader_assumption_id,
            "challenger_assumption_id": self.challenger_assumption_id,
            "reason": self.reason,
        }


# Below this the denominator is numerically indistinguishable from zero and the
# "required move" is an artefact of floating point rather than a decision
# boundary. One ten-thousandth of a dollar of landed cost per unit of input.
_NEGLIGIBLE_MARGINAL = 1e-4


def _input_value(row: LandedCost, component: CostComponent) -> float | None:
    """The input's own entered value as the row used it — rate, or flat amount."""
    for step in row.steps:
        if step.component is component:
            return step.rate if step.rate is not None else step.amount.amount
    return None


def input_flip_moves(ranking: OriginRanking) -> tuple[FlipMove, ...]:
    """Every input that could change the answer, least room first.

    The ordering question is real: "freight must rise 12 USD/MT" and "duty must
    rise 0.4 points" are not comparable as written, so rows are sorted by the
    move **as a percentage of the input's current value** — how wrong the number
    would have to be, which is the thing a trader is actually judging. An input
    entered as zero has no such percentage and sorts last rather than first,
    because a percentage of nothing is not a small number, it is no number.

    Solved analytically. Every rung is linear in its own input given the others,
    so the flip point is exact rather than searched — and the tests check the
    solved move against a re-run of the whole waterfall, which is the only proof
    that the derivative and the arithmetic agree.
    """
    ordered = ranking.rankable
    if len(ordered) < 2:
        return ()
    leader, challenger = ordered[0], ordered[1]
    gap = challenger.landed_usd_mt - leader.landed_usd_mt  # type: ignore[operator]

    leader_ids = {step.component: step.assumption_id for step in leader.steps}
    challenger_ids = {step.component: step.assumption_id for step in challenger.steps}
    components = [
        component for component in COMPONENT_ORDER
        if component is not CostComponent.ORIGIN_PRICE
        and (component in leader_ids or component in challenger_ids)
    ]

    moves: list[FlipMove] = []
    for component in components:
        leader_marginal = marginal_landed_per_unit(leader, component)
        challenger_marginal = marginal_landed_per_unit(challenger, component)
        shared = (
            leader_ids.get(component) is not None
            and leader_ids.get(component) == challenger_ids.get(component)
        )
        unit = REQUIRED_UNIT[component]
        current = _input_value(leader, component)
        reason: str | None = None
        move: float | None = None
        challenger_move: float | None = None

        if leader_marginal is None:
            reason = (
                "this route does not carry that input, or its entered rate is zero and "
                "the period it is charged over cannot be recovered from the waterfall"
            )
        else:
            denominator = leader_marginal - (
                challenger_marginal if shared and challenger_marginal else 0.0
            )
            if denominator > _NEGLIGIBLE_MARGINAL:
                move = gap / denominator
            else:
                reason = (
                    "both origins are costed off the same entry, so moving it moves both "
                    "landed totals together and cannot change which is cheaper"
                    if shared else
                    "this input has no effect on the leader's landed total"
                )
        if not shared and challenger_marginal:
            challenger_move = gap / challenger_marginal

        moves.append(
            FlipMove(
                component=component,
                unit=unit,
                leader=leader.quote.origin.key,
                challenger=challenger.quote.origin.key,
                shared=shared,
                current_advantage_usd_mt=gap,
                move=move,
                challenger_move=challenger_move,
                current_value=current,
                move_pct_of_current=(
                    abs(move / current) * 100.0
                    if move is not None and current else None
                ),
                leader_assumption_id=leader_ids.get(component),
                challenger_assumption_id=challenger_ids.get(component),
                reason=reason,
            )
        )

    return tuple(
        sorted(
            moves,
            key=lambda item: (
                item.move_pct_of_current is None,
                item.move_pct_of_current or 0.0,
                item.component.value,
            ),
        )
    )


def most_fragile_input(ranking: OriginRanking) -> FlipMove | None:
    """The input the answer is least robust to, or ``None`` if nothing can flip it."""
    for move in input_flip_moves(ranking):
        if move.move is not None:
            return move
    return None


__all__ = [
    "BASE",
    "STANDARD_SCENARIOS",
    "Breakeven",
    "FlipMove",
    "Scenario",
    "ScenarioResult",
    "Shock",
    "basis_shock",
    "financing_shock",
    "freight_breakeven",
    "freight_shock",
    "fx_shock",
    "input_flip_moves",
    "marginal_landed_per_unit",
    "most_fragile_input",
    "run_panel",
    "run_scenario",
    "tariff_shock",
]
