"""Scenario analysis over a hedged physical position (Phase 3).

A hedge report that shows only today's numbers answers the wrong question. The
useful one is *what happens to me if the board moves 5%, the basis widens 8
USD/MT and the real weakens 4% — together*, because those three rarely move
alone and a hedger's P&L is the sum of all of them.

The design mirrors ``analysis/origins/scenarios.py``, deliberately:

**A shock names what it moves.** ``futures_shock``, ``basis_shock``,
``fx_shock``, ``crush_yield_shock``, ``value_share_shock``. A scenario that
moves "everything by 5%" teaches nothing; one that moves *soybean oil* +8% and
flips a crush hedge from profit to loss teaches the whole thing.

**A shock is applied to the input, never to the answer.** Every result is
recomputed from shocked prices through the same leg arithmetic the hedge
calculator uses. Adjusting a computed P&L by a percentage would skip the
rounding — the residual tonnes are unhedged and move with the *physical* only,
which is precisely the exposure a scenario is run to find.

**A scenario is reproducible or it is not a scenario.** Every result carries
the method version and a hash of its own shocks. No clock, no RNG, no
iteration-order dependence.

Sign conventions, stated once
-----------------------------
* Physical P&L is signed by the physical side: a long physical gains when the
  flat price (futures + basis) rises.
* Futures P&L is signed by each leg's own side.
* FX P&L exists only when the physical is invoiced in a non-USD currency; it is
  the change in the USD value of that fixed local amount.
* Net P&L is the sum. A perfect hedge nets to the basis move on the hedged
  tonnes plus the full flat move on the residual tonnes — which is the result
  the tests pin, because it is the one a trader checks by hand.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date
from typing import Any

from analysis.futures.domain import METHOD_VERSION, ContractQuote, fingerprint
from analysis.futures.hedge import (
    CRUSH_YIELDS_MT,
    HedgeLeg,
    HedgeProposal,
    Rounding,
    size_leg,
)


@dataclass(frozen=True)
class Shock:
    """One named change to one named input."""

    kind: str            # futures | basis | fx | crush_yield | value_share
    target: str          # commodity name, FX pair, product leg, or "*"
    mode: str            # absolute | relative
    value: float
    label: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind, "target": self.target, "mode": self.mode,
            "value": self.value, "label": self.label,
        }


def futures_shock(pct: float, *, commodity: str = "*") -> Shock:
    """Move a board price by ``pct`` per cent."""
    return Shock("futures", commodity, "relative", pct,
                 f"{commodity} futures {pct:+.2f}%")


def futures_shock_usd_mt(usd_mt: float, *, commodity: str = "*") -> Shock:
    """Move a board price by an absolute USD/MT amount."""
    return Shock("futures", commodity, "absolute", usd_mt,
                 f"{commodity} futures {usd_mt:+.2f} USD/MT")


def basis_shock(usd_mt: float) -> Shock:
    """Widen (positive) or narrow (negative) the physical basis, USD/MT."""
    return Shock("basis", "*", "absolute", usd_mt, f"basis {usd_mt:+.2f} USD/MT")


def fx_shock(pct: float, *, pair: str = "*") -> Shock:
    """Move the home currency by ``pct`` per cent against USD.

    Positive strengthens the home currency (the ``<CCY>/USD`` rate rises), so a
    seller invoicing locally receives more USD for the same local amount.
    """
    return Shock("fx", pair, "relative", pct, f"{pair} {pct:+.2f}%")


def crush_yield_shock(product: str, new_yield_mt: float) -> Shock:
    """Change a crush yield (MT of product per MT of beans) and re-size the leg."""
    return Shock("crush_yield", product, "absolute", new_yield_mt,
                 f"{product} yield -> {new_yield_mt:.4f} MT/MT")


def value_share_shock(delta: float) -> Shock:
    """Shift the oil share of product value by ``delta`` (0.02 = +2 points).

    Total product value is held constant and the split between oil and meal is
    moved, which is the shape of the real event: a biodiesel-driven oil rally
    that pulls meal down, not a parallel shift in both.
    """
    return Shock("value_share", "*", "absolute", delta, f"oil value share {delta:+.3f}")


@dataclass(frozen=True)
class Scenario:
    """A named bundle of shocks, applied together."""

    name: str
    shocks: tuple[Shock, ...] = ()
    note: str = ""

    @property
    def identifier(self) -> str:
        return fingerprint({"name": self.name, "shocks": [s.to_dict() for s in self.shocks]})

    def of_kind(self, kind: str) -> tuple[Shock, ...]:
        return tuple(s for s in self.shocks if s.kind == kind)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "id": self.identifier,
            "note": self.note,
            "shocks": [s.to_dict() for s in self.shocks],
        }


@dataclass(frozen=True)
class LegOutcome:
    symbol: str
    side: str
    contracts: int
    price_before: float
    price_after: float
    usd_mt_before: float
    usd_mt_after: float
    pnl_usd: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbol": self.symbol, "side": self.side, "contracts": self.contracts,
            "price_before": round(self.price_before, 4), "price_after": round(self.price_after, 4),
            "usd_mt_before": round(self.usd_mt_before, 2), "usd_mt_after": round(self.usd_mt_after, 2),
            "pnl_usd": round(self.pnl_usd, 2),
        }


@dataclass(frozen=True)
class ScenarioResult:
    """What a scenario does to a hedged position, attributed by cause."""

    scenario: Scenario
    as_of: date
    legs: tuple[LegOutcome, ...]
    physical_pnl_usd: float
    futures_pnl_usd: float
    basis_pnl_usd: float
    fx_pnl_usd: float
    net_pnl_usd: float
    residual_pnl_usd: float
    unhedged_pnl_usd: float
    method_version: str = METHOD_VERSION
    notes: tuple[str, ...] = ()

    @property
    def hedge_effectiveness_pct(self) -> float | None:
        """How much of the unhedged P&L swing the hedge removed.

        None when the unhedged position would not have moved at all — a ratio
        with a zero denominator is not "100% effective", it is undefined, and
        printing 100 there would be the most flattering possible lie.
        """
        if self.unhedged_pnl_usd == 0:
            return None
        return (1 - abs(self.net_pnl_usd) / abs(self.unhedged_pnl_usd)) * 100.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario.to_dict(),
            "as_of": self.as_of.isoformat(),
            "method_version": self.method_version,
            "legs": [leg.to_dict() for leg in self.legs],
            "physical_pnl_usd": round(self.physical_pnl_usd, 2),
            "futures_pnl_usd": round(self.futures_pnl_usd, 2),
            "basis_pnl_usd": round(self.basis_pnl_usd, 2),
            "fx_pnl_usd": round(self.fx_pnl_usd, 2),
            "residual_pnl_usd": round(self.residual_pnl_usd, 2),
            "unhedged_pnl_usd": round(self.unhedged_pnl_usd, 2),
            "net_pnl_usd": round(self.net_pnl_usd, 2),
            "hedge_effectiveness_pct": (
                None if self.hedge_effectiveness_pct is None
                else round(self.hedge_effectiveness_pct, 1)
            ),
            "notes": list(self.notes),
        }


# ---------------------------------------------------------------------------
# Applying shocks
# ---------------------------------------------------------------------------


def _shocked_price(quote: ContractQuote, scenario: Scenario) -> float:
    """The leg's price after every futures shock that targets it."""
    spec = quote.contract.spec
    price = quote.price
    for shock in scenario.of_kind("futures"):
        if shock.target not in ("*", spec.name, spec.root):
            continue
        if shock.mode == "relative":
            price *= (1 + shock.value / 100.0)
        else:
            # An absolute shock is stated in USD/MT because that is the unit a
            # physical trader thinks in; it is converted into the leg's native
            # units here rather than applied to a USD/MT number and converted
            # back, which would round twice.
            price += shock.value / spec.native_to_usd_per_mt(1.0)
    return price


def _value_share_prices(
    legs: tuple[HedgeLeg, ...], scenario: Scenario, prices: dict[str, float]
) -> tuple[dict[str, float], tuple[str, ...]]:
    """Re-split product value between oil and meal, holding the total constant.

    Only meaningful on a crush structure carrying both product legs; on any
    other shape the shock is recorded as inapplicable rather than silently
    doing nothing.
    """
    shocks = scenario.of_kind("value_share")
    if not shocks:
        return prices, ()

    oil = next((leg for leg in legs if leg.contract.spec.name == "Soybean Oil"), None)
    meal = next((leg for leg in legs if leg.contract.spec.name == "Soybean Meal"), None)
    if oil is None or meal is None:
        return prices, (
            "value-share shock ignored: this position has no oil and meal pair to re-split",
        )

    oil_value = (
        oil.contract.spec.native_to_usd_per_mt(prices[oil.contract.symbol])
        * CRUSH_YIELDS_MT["Soybean Oil"]
    )
    meal_value = (
        meal.contract.spec.native_to_usd_per_mt(prices[meal.contract.symbol])
        * CRUSH_YIELDS_MT["Soybean Meal"]
    )
    total = oil_value + meal_value
    if total == 0:
        return prices, ("value-share shock ignored: product value is zero",)

    share = oil_value / total
    for shock in shocks:
        share = min(max(share + shock.value, 0.0), 1.0)

    new_oil_value = total * share
    new_meal_value = total - new_oil_value
    updated = dict(prices)
    updated[oil.contract.symbol] = _usd_mt_to_native(
        oil, new_oil_value / CRUSH_YIELDS_MT["Soybean Oil"]
    )
    updated[meal.contract.symbol] = _usd_mt_to_native(
        meal, new_meal_value / CRUSH_YIELDS_MT["Soybean Meal"]
    )
    return updated, (
        f"oil value share moved to {share:.3f} with total product value held constant",
    )


def _usd_mt_to_native(leg: HedgeLeg, usd_mt: float) -> float:
    spec = leg.contract.spec
    return usd_mt / spec.native_to_usd_per_mt(1.0)


def resize_for_yield_shocks(
    proposal: HedgeProposal, scenario: Scenario, *, rounding: Rounding | None = None
) -> tuple[HedgeLeg, ...]:
    """Re-size product legs when a crush-yield shock changes the hedge ratio.

    A yield change is not a price change: it changes how many contracts the
    hedge *needs*, and a scenario that shocked the yield without re-sizing
    would report the P&L of a hedge nobody would have placed.
    """
    shocks = {shock.target: shock.value for shock in scenario.of_kind("crush_yield")}
    if not shocks:
        return proposal.legs
    rounding = rounding or proposal.rounding
    resized = []
    for leg in proposal.legs:
        new_yield = shocks.get(leg.contract.spec.name)
        if new_yield is None:
            resized.append(leg)
            continue
        resized.append(size_leg(
            leg.quote, side=leg.side, physical_mt=leg.target_physical_mt,
            hedge_ratio=new_yield,
            hedge_ratio_source=f"scenario override: {leg.contract.spec.name} yield {new_yield:.4f} MT/MT",
            rounding=rounding, cross_hedge_note=leg.cross_hedge_note,
        ))
    return tuple(resized)


def run_scenario(proposal: HedgeProposal, scenario: Scenario) -> ScenarioResult:
    """Apply ``scenario`` to a sized hedge and attribute the result."""
    exposure = proposal.exposure
    quantity_mt = exposure.quantity_mt
    notes: list[str] = []

    legs = resize_for_yield_shocks(proposal, scenario)
    if legs is not proposal.legs:
        notes.append("product legs re-sized for the shocked crush yield")

    prices = {leg.contract.symbol: _shocked_price(leg.quote, scenario) for leg in legs}
    prices, share_notes = _value_share_prices(legs, scenario, prices)
    notes.extend(share_notes)

    leg_outcomes: list[LegOutcome] = []
    futures_pnl = 0.0
    for leg in legs:
        after = prices[leg.contract.symbol]
        pnl = leg.value_change_usd(after)
        futures_pnl += pnl
        leg_outcomes.append(LegOutcome(
            symbol=leg.contract.symbol,
            side=leg.side.value,
            contracts=leg.contracts,
            price_before=leg.quote.price,
            price_after=after,
            usd_mt_before=leg.quote.usd_per_mt,
            usd_mt_after=leg.contract.spec.native_to_usd_per_mt(after),
            pnl_usd=pnl,
        ))

    # The physical moves with the flat price of *its own* commodity — the board
    # leg that references it — plus the basis. A crush hedge's product legs do
    # not reprice the beans in the yard.
    reference = next(
        (leg for leg in legs if leg.contract.spec.name == exposure.commodity),
        legs[0] if legs else None,
    )
    if reference is None:
        notes.append("no futures leg — the physical is shown unhedged")
        futures_move_usd_mt = 0.0
    else:
        before = reference.quote.usd_per_mt
        after = reference.contract.spec.native_to_usd_per_mt(prices[reference.contract.symbol])
        futures_move_usd_mt = after - before

    basis_move_usd_mt = sum(shock.value for shock in scenario.of_kind("basis"))
    physical_move_usd_mt = futures_move_usd_mt + basis_move_usd_mt
    physical_pnl = exposure.side.sign * physical_move_usd_mt * quantity_mt
    basis_pnl = exposure.side.sign * basis_move_usd_mt * quantity_mt
    residual_pnl = exposure.side.sign * futures_move_usd_mt * proposal.residual_mt

    fx_pnl, fx_notes = _fx_pnl(proposal, scenario, futures_move_usd_mt)
    notes.extend(fx_notes)

    net = physical_pnl + futures_pnl + fx_pnl
    unhedged = physical_pnl + fx_pnl

    return ScenarioResult(
        scenario=scenario,
        as_of=proposal.as_of,
        legs=tuple(leg_outcomes),
        physical_pnl_usd=physical_pnl,
        futures_pnl_usd=futures_pnl,
        basis_pnl_usd=basis_pnl,
        fx_pnl_usd=fx_pnl,
        net_pnl_usd=net,
        residual_pnl_usd=residual_pnl,
        unhedged_pnl_usd=unhedged,
        notes=tuple(notes),
    )


def _fx_pnl(
    proposal: HedgeProposal, scenario: Scenario, futures_move_usd_mt: float
) -> tuple[float, tuple[str, ...]]:
    """USD P&L from the currency leg.

    Modelled as: the physical is contracted in the home currency, so its local
    amount is fixed and the USD it converts to moves with the rate. The futures
    hedge is USD-settled and unaffected, which is exactly why the exposure
    survives the hedge.
    """
    shocks = scenario.of_kind("fx")
    fx = proposal.fx
    if not shocks:
        return 0.0, ()
    if fx.pair is None or fx.rate is None or fx.amount_home is None:
        return 0.0, (
            "FX shock ignored: this position has no quantified currency exposure "
            f"({fx.note})",
        )
    applicable = [s for s in shocks if s.target in ("*", fx.pair)]
    if not applicable:
        return 0.0, (f"FX shock ignored: no shock targets {fx.pair}",)

    new_rate = fx.rate
    for shock in applicable:
        new_rate *= (1 + shock.value / 100.0)
    pnl = fx.amount_home * (new_rate - fx.rate)
    return pnl, (
        f"{fx.pair} {fx.rate:.6f} -> {new_rate:.6f} on a fixed "
        f"{fx.amount_home:,.0f} {proposal.exposure.currency} contract value",
    )


def run_panel(proposal: HedgeProposal, scenarios: tuple[Scenario, ...]) -> tuple[ScenarioResult, ...]:
    return tuple(run_scenario(proposal, scenario) for scenario in scenarios)


DEFAULT_PANEL: tuple[Scenario, ...] = (
    Scenario("Board +5%", (futures_shock(5.0),), "a broad rally with the basis unchanged"),
    Scenario("Board -5%", (futures_shock(-5.0),), "a broad break with the basis unchanged"),
    Scenario("Basis +10 USD/MT", (basis_shock(10.0),), "the physical firms against an unchanged board"),
    Scenario("Basis -10 USD/MT", (basis_shock(-10.0),), "the physical weakens against an unchanged board"),
    Scenario(
        "Board -5%, basis +10, FX -4%",
        (futures_shock(-5.0), basis_shock(10.0), fx_shock(-4.0)),
        "the combination a South American seller actually fears: board down, local premium up, "
        "home currency weaker",
    ),
)


def default_panel_for(proposal: HedgeProposal) -> tuple[Scenario, ...]:
    """The standard panel, trimmed to shocks this position can actually feel."""
    has_fx = proposal.fx.pair is not None and proposal.fx.rate is not None
    if has_fx:
        return DEFAULT_PANEL
    return tuple(
        replace(scenario, shocks=tuple(s for s in scenario.shocks if s.kind != "fx"))
        for scenario in DEFAULT_PANEL
    )


__all__ = [
    "DEFAULT_PANEL",
    "LegOutcome",
    "Scenario",
    "ScenarioResult",
    "Shock",
    "basis_shock",
    "crush_yield_shock",
    "default_panel_for",
    "futures_shock",
    "futures_shock_usd_mt",
    "fx_shock",
    "resize_for_yield_shocks",
    "run_panel",
    "run_scenario",
    "value_share_shock",
]
