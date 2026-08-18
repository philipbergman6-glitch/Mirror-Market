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

from analysis.origins.assumptions import AssumptionSet
from analysis.origins.domain import (
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


def _marginal_landed_per_freight_dollar(row: LandedCost) -> float:
    """What one extra USD/MT of ocean freight adds to the landed total.

    Every ad-valorem rung applied *after* freight compounds onto it, and
    financing compounds onto those. Reading the rates off the row's own steps
    keeps this exact for whatever stack the route actually has.
    """
    from analysis.origins.domain import COMPONENT_ORDER

    steps = {step.component: step for step in row.steps}
    if CostComponent.OCEAN_FREIGHT not in steps:
        return 0.0
    freight_index = COMPONENT_ORDER.index(CostComponent.OCEAN_FREIGHT)
    multiplier = 1.0
    for component in COMPONENT_ORDER[freight_index + 1:]:
        step = steps.get(component)
        if step is None or step.rate is None:
            continue
        if component is CostComponent.FINANCING:
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


__all__ = [
    "BASE",
    "STANDARD_SCENARIOS",
    "Breakeven",
    "Scenario",
    "ScenarioResult",
    "Shock",
    "basis_shock",
    "financing_shock",
    "freight_breakeven",
    "freight_shock",
    "fx_shock",
    "run_panel",
    "run_scenario",
    "tariff_shock",
]
