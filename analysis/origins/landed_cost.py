"""The canonical landed-cost calculation (Phase 2).

One function, one order of operations, one version string. Every surface —
the page, the scenario engine, the stored history, the tests — calls
:func:`compute_landed_cost` and nothing re-implements a rung of it. That is the
point: a landed cost assembled twice is a landed cost that disagrees with
itself the first time somebody adds VAT to one copy.

The stack, in the order it is applied:

===  =============================  ===========================================
 1   origin price                   as quoted, already USD/MT
 2   incoterm bridge                whatever the leg's own term owes to reach
                                    **FOB vessel at its own port** — elevation
                                    for a barge-CIF bid, inland + port for an
                                    ex-works one, nothing for an FOB-vessel one
     ---------------------------->  = FOB-vessel equivalent
 3   ocean freight                  origin port -> destination range
 4   marine insurance               % of the running (CFR) value
     ---------------------------->  = CIF destination
 5   import duty                    % of CIF
 6   import VAT                     % of duty-paid value
 7   destination port costs         discharge, handling, storage
 8   financing                      rate x days / 365 on the pre-financing value
 9   quality adjustment             +/- protein, FM, moisture differential
     ---------------------------->  = landed, duty-paid, destination port
===  =============================  ===========================================

Rungs 5 and 6 are why this is a *sequence* rather than a sum. Duty is a
percentage of the CIF value, so it cannot be computed before freight; VAT is a
percentage of the duty-paid value, so it cannot be computed before duty.
Reordering the tuple changes the answer, which is exactly why the order lives
in one place and carries a version.

**Fail-closed.** Every rung after the origin price is an entered assumption
(``analysis/origins/assumptions.py``). A missing or expired one produces a
:class:`~analysis.origins.domain.Blocker`, and a blocked calculation returns
``landed=None`` — never a partial total. A partial landed cost rendered beside
two complete ones is the single most dangerous artefact this module could
produce, because it is cheaper than both of them and looks like a finding.

Rung 2 has the same discipline for a subtler reason. A CIF-barge NOLA bid and a
Paranaguá FOB offer differ by the cost of lifting grain out of a barge and into
a vessel. That number is real, it is tens of dollars a tonne, and it is
published nowhere free. Defaulting it to zero would make the US origin look
structurally cheapest, every single day, in the same direction — a bias that
would survive review precisely because it never looks like an error.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date

import config
from analysis.origins.assumptions import (
    UNIT_FRACTION,
    UNIT_RATE_PER_ANNUM,
    Assumption,
    AssumptionMiss,
    AssumptionSet,
)
from analysis.origins.domain import (
    COMPONENT_ORDER,
    USD,
    USD_PER_MT,
    Blocker,
    Comparability,
    Confidence,
    CostComponent,
    Freshness,
    LandedCost,
    Money,
    OriginQuote,
    Port,
    ShipmentWindow,
    WaterfallStep,
    usd_mt,
    worst_confidence,
)

log = logging.getLogger(__name__)

DAYS_PER_YEAR = 365.0

# Human labels for the waterfall. Kept beside the arithmetic rather than in a
# template so the stored history and the page name each rung identically — a
# ranking read back in six months should not need the template that rendered it.
COMPONENT_LABELS: dict[CostComponent, str] = {
    CostComponent.ORIGIN_PRICE: "Origin price, as quoted",
    CostComponent.INLAND_TRANSPORT: "Inland transport to port",
    CostComponent.ORIGIN_PORT_COSTS: "Origin port costs",
    CostComponent.ELEVATION: "Elevation (barge → vessel)",
    CostComponent.OCEAN_FREIGHT: "Ocean freight",
    CostComponent.MARINE_INSURANCE: "Marine insurance",
    CostComponent.IMPORT_DUTY: "Import duty",
    CostComponent.IMPORT_VAT: "Import VAT",
    CostComponent.DESTINATION_PORT_COSTS: "Destination port costs",
    CostComponent.FINANCING: "Financing / carry",
    CostComponent.QUALITY_ADJUSTMENT: "Quality adjustment",
    CostComponent.PLANT_FREIGHT_IN: "Freight to plant",
    CostComponent.PROCESSING_COST: "Processing cost",
    CostComponent.ENERGY_COST: "Energy",
    CostComponent.WORKING_CAPITAL: "Working capital",
}

# Which comparability verdict wins when several apply. First match wins, and
# the order is "what the reader most needs to know", not severity: being
# quoted for the wrong month makes the freight question moot, so it is said
# first even though a missing freight number also blocks the row.
_COMPARABILITY_PRECEDENCE = (
    Comparability.WINDOW_MISMATCH,
    Comparability.WINDOW_UNKNOWN,
    Comparability.INCOTERM_UNBRIDGED,
    Comparability.STALE,
    Comparability.MISSING_INPUT,
    Comparability.OBSERVATION_SPREAD,
)


def _worst_comparability(*verdicts: Comparability) -> Comparability:
    for candidate in _COMPARABILITY_PRECEDENCE:
        if candidate in verdicts:
            return candidate
    return Comparability.COMPARABLE


def bridge_components(incoterm: str, carrier: str) -> tuple[CostComponent, ...] | None:
    """What this delivery term owes to reach FOB vessel, or ``None`` if unknown.

    ``None`` is not an empty bridge. An unknown (incoterm, carrier) pair means
    nobody has decided what that term costs to normalise, and guessing "nothing"
    would silently promote a farmgate price to an export offer.
    """
    key = (str(incoterm), str(carrier))
    raw = config.INCOTERM_BRIDGE_TO_FOB_VESSEL.get(key)
    if raw is None:
        return None
    return tuple(CostComponent(name) for name in raw)


def _apply(
    component: CostComponent,
    assumption: Assumption,
    running: Money,
) -> tuple[Money, str, float | None]:
    """The amount this rung adds, its basis sentence, and its rate if any.

    The three unit families are genuinely different arithmetic, and each is
    applied against the running total *at this rung's position* — which is what
    makes duty-then-VAT come out right without either of them knowing about the
    other.
    """
    if assumption.unit == UNIT_FRACTION:
        amount = running.scaled(assumption.value)
        basis = (
            f"{assumption.value * 100:.4g}% of {running.amount:,.2f} USD/MT "
            f"({component.value} base at this rung)"
        )
        return amount, basis, assumption.value
    if assumption.unit == UNIT_RATE_PER_ANNUM:
        days = assumption.days or 0
        amount = running.scaled(assumption.value * days / DAYS_PER_YEAR)
        basis = (
            f"{assumption.value * 100:.4g}%/yr x {days}d / 365 on "
            f"{running.amount:,.2f} USD/MT"
        )
        return amount, basis, assumption.value
    return usd_mt(assumption.value), f"{assumption.value:+,.2f} USD/MT, flat", None


def _step(
    component: CostComponent,
    assumption: Assumption,
    running: Money,
    *,
    override: float | None,
) -> WaterfallStep:
    if override is not None:
        assumption = type(assumption)(
            **{**_as_kwargs(assumption), "value": override}
        )
    amount, basis, rate = _apply(component, assumption, running)
    if override is not None:
        basis = f"{basis} [scenario override of {assumption.id}]"
    return WaterfallStep(
        component=component,
        label=COMPONENT_LABELS[component],
        amount=amount,
        running_total=running + amount,
        basis=basis,
        confidence=assumption.confidence,
        assumption_id=assumption.id,
        entered_by=assumption.entered_by,
        entered_at=assumption.entered_at,
        expires_on=assumption.expires_on,
        rate=rate,
    )


def _as_kwargs(assumption: Assumption) -> dict:
    from dataclasses import fields

    return {field.name: getattr(assumption, field.name) for field in fields(assumption)}


def window_verdict(quote: OriginQuote, requested: ShipmentWindow) -> Comparability:
    """Whether this quote is priced for the window the trader asked about.

    Three answers, and the middle one is the one that matters. A source that
    publishes a window and whose window does not overlap is quoting a different
    product (``WINDOW_MISMATCH``). A source that publishes no window at all —
    AgRural's port-side level — cannot be *said* to be quoting the requested
    month, so it is ``WINDOW_UNKNOWN`` and never ranked against one, however
    plausible the level looks.
    """
    if quote.shipment_window is None:
        return Comparability.WINDOW_UNKNOWN
    if not quote.shipment_window.overlaps(requested):
        return Comparability.WINDOW_MISMATCH
    return Comparability.COMPARABLE


def compute_landed_cost(
    quote: OriginQuote,
    destination: Port,
    requested_window: ShipmentWindow,
    assumptions: AssumptionSet,
    *,
    today: date,
    overrides: Mapping[CostComponent, float] | None = None,
    stack: tuple[str, ...] | None = None,
) -> LandedCost:
    """Build one origin's complete waterfall, or say precisely why it cannot.

    ``overrides`` is the scenario engine's hook: it replaces an assumption's
    *value* while keeping its identity, owner and expiry, so a sensitivity run
    is visibly a variation on a named input rather than a free-floating number.
    An override for a component whose assumption is missing does **not**
    unblock the row — there is nothing to vary.
    """
    overrides = dict(overrides or {})
    method_version = config.LANDED_COST_METHOD_VERSION
    steps: list[WaterfallStep] = []
    blockers: list[Blocker] = []
    missing: list[CostComponent] = []
    deferred: list[CostComponent] = []
    confidences: list[Confidence] = [quote.base_confidence]

    running = Money(quote.price.amount, USD, USD_PER_MT)
    steps.append(
        WaterfallStep(
            component=CostComponent.ORIGIN_PRICE,
            label=COMPONENT_LABELS[CostComponent.ORIGIN_PRICE],
            amount=running,
            running_total=running,
            basis=(
                f"{quote.quote_kind.value} quote, {quote.incoterm.value} "
                f"{quote.carrier.value} {quote.origin.name}, observed "
                f"{quote.observation_date.isoformat()}"
                + (
                    f", converted at {quote.fx.pair} {quote.fx.usd_per_unit:.6g} "
                    f"({quote.fx.observed_on.isoformat()})"
                    if quote.fx else ""
                )
            ),
            confidence=quote.base_confidence,
        )
    )

    def take(component: CostComponent) -> None:
        """Resolve one rung, mutating `running`, `steps`, `blockers`, `missing`.

        Every rung is looked up even after the waterfall has already blocked —
        so the page can name *all* the inputs a route needs, not just the first
        one — but nothing further is **applied**. That distinction matters
        because half this stack is ad valorem: duty is a percentage of the CIF
        value, so computing it while ocean freight is missing charges duty on
        an FOB base and prints a confident, specific, wrong number (3% of
        466.19 rather than of 466.19 + freight). A rung that cannot be computed
        on the right base is recorded as deferred and carries no amount at all.
        """
        nonlocal running
        found = assumptions.lookup(
            component,
            origin=quote.origin.key,
            destination=destination.key,
            window=requested_window,
            on=today,
        )
        if isinstance(found, AssumptionMiss):
            missing.append(component)
            blockers.append(
                Blocker(
                    code=found.code,
                    message=found.reason,
                    remedy=(
                        f"enter a {component.value} assumption for "
                        f"{quote.origin.key} -> {destination.key} covering "
                        f"{requested_window.describe()} "
                        f"(python scripts/enter_assumption.py --component {component.value} ...)"
                    ),
                    component=component,
                )
            )
            return
        if blockers:
            deferred.append(component)
            return
        step = _step(component, found, running, override=overrides.get(component))
        steps.append(step)
        running = step.running_total
        confidences.append(step.confidence)

    # --- rung 2: normalise to FOB vessel -----------------------------------
    bridge = bridge_components(quote.incoterm.value, quote.carrier.value)
    if bridge is None:
        blockers.append(
            Blocker(
                code="incoterm_unbridged",
                message=(
                    f"no costed path from {quote.incoterm.value} {quote.carrier.value} to "
                    "FOB vessel is defined — comparing it against an FOB offer would "
                    "compare two different delivery terms as if they were one price"
                ),
                remedy=(
                    "add the term to config.INCOTERM_BRIDGE_TO_FOB_VESSEL naming the "
                    "components it owes"
                ),
            )
        )
        bridge = ()
        unbridged = True
    else:
        unbridged = False
        for component in bridge:
            take(component)

    fob_equivalent = None if (blockers or unbridged) else Money(running.amount, USD, USD_PER_MT)

    # --- rungs 3..9: the landed stack --------------------------------------
    for name in (stack if stack is not None else config.LANDED_STACK):
        component = CostComponent(name)
        if component not in COMPONENT_ORDER:
            raise ValueError(
                f"{component.value} is not part of the landed waterfall — see "
                "CostComponent's note on the crush rungs"
            )
        take(component)

    freshness = quote.freshness(today)
    verdicts = [window_verdict(quote, requested_window)]
    if unbridged:
        verdicts.append(Comparability.INCOTERM_UNBRIDGED)
    if freshness is not Freshness.CURRENT:
        verdicts.append(Comparability.STALE)
    if blockers:
        verdicts.append(Comparability.MISSING_INPUT)
    comparability = _worst_comparability(*verdicts)

    landed = None if blockers else Money(running.amount, USD, USD_PER_MT)
    if comparability is Comparability.COMPARABLE and landed is None:
        # Defensive: blockers always demote the verdict above, so this cannot
        # fire. It stays because LandedCost's own invariant is the thing being
        # protected and a future rung that forgets to append a verdict should
        # fail here rather than publish a comparable row with no total.
        comparability = Comparability.MISSING_INPUT

    notes: list[str] = []
    if deferred:
        notes.append(
            "not computed once the waterfall blocked: "
            + ", ".join(COMPONENT_LABELS[component] for component in deferred)
            + " — these are entered and available, but duty and VAT are percentages "
            "of the running value, so charging them on an incomplete base would "
            "produce a specific and wrong number"
        )
    if quote.quotes_averaged > 1:
        notes.append(
            f"{quote.quotes_averaged} quotes on {quote.observation_date.isoformat()} "
            "averaged into this level"
        )
    if freshness is not Freshness.CURRENT:
        notes.append(
            f"observed {quote.observation_date.isoformat()}, "
            f"{quote.age_days(today)}d old against a {quote.max_age_days}d budget"
        )
    notes.extend(quote.notes)

    return LandedCost(
        quote=quote,
        destination=destination,
        requested_window=requested_window,
        steps=tuple(steps),
        landed=landed,
        fob_equivalent=fob_equivalent,
        comparability=comparability,
        confidence=worst_confidence(*confidences),
        freshness=freshness,
        blockers=tuple(blockers),
        missing_inputs=tuple(missing),
        method_version=method_version,
        notes=tuple(notes),
    )


__all__ = [
    "COMPONENT_LABELS",
    "DAYS_PER_YEAR",
    "bridge_components",
    "compute_landed_cost",
    "window_verdict",
]
