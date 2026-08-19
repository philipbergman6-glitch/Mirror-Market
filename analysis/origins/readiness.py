"""Route onboarding: what has to be entered before a route can be compared.

The landed-cost stack is fail-closed by design, and on a fresh clone that means
every row blocks. Correct — and, on its own, not yet a workflow. A trader
looking at eight blocked rows needs the other half of the sentence: *which*
inputs, for *which* legs, in *which* unit, and the exact command that supplies
each one.

This module is that half. It is deliberately **database-free**: what a route
requires is a property of its delivery term and the landed stack, both of which
live in ``config``, so the checklist is answerable on a clone with no data at
all. Nothing here reads a price, and nothing here proposes a value — a
requirement carries a ``<VALUE>`` placeholder, never a number, because a
suggested default is a fabricated default with an extra step.

Three things it computes:

**Required components, per leg.** The incoterm bridge (what this leg's own
delivery term owes to reach FOB vessel — elevation for the CIF-barge Gulf bid,
inland haulage and port costs for an ex-works leg) followed by
``config.LANDED_STACK``. Derived, never listed: adding a rung to the stack adds
it to every checklist, and a leg whose term changes gets a different bridge with
no edit here.

**Status, per requirement.** ``satisfied`` / ``expiring`` / ``expired`` /
``missing``, resolved through the same ``AssumptionSet.lookup`` the calculation
uses — so a checklist that says "satisfied" and a page that says "blocked"
cannot disagree. ``expiring`` is not a state the calculation has; it is the one
the *workflow* needs, and it is the difference between renewing a freight
indication and explaining a blocked page on a Monday morning.

**The renewal queue.** ``expiry_review`` answers "what lapses next, who owns it,
and which routes go dark when it does" — sorted by the date it happens, because
that is the order the work has to be done in.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

import config
from analysis.origins.assumptions import (
    REQUIRED_UNIT,
    UNIT_RATE_PER_ANNUM,
    Assumption,
    AssumptionMiss,
    AssumptionSet,
)
from analysis.origins.domain import CostComponent, ShipmentWindow
from analysis.origins.landed_cost import COMPONENT_LABELS, bridge_components

# The scope policy has exactly one home — validation.py — so the command this
# module prints cannot drift from the check that would reject the entry it makes.
from analysis.origins.validation import (
    DESTINATION_SCOPED_COMPONENTS as _DESTINATION_SCOPED,
)
from analysis.origins.validation import EXPIRY_WARNING_DAYS
from analysis.origins.validation import (
    ORIGIN_SCOPED_COMPONENTS as _ORIGIN_SCOPED,
)

# What each input is, and what a trader has to go and get. Facts about the
# input, never a level: "a Panamax rate for this voyage" is guidance, "52
# USD/MT" would be a fabricated default in a docstring.
COMPONENT_GUIDANCE: dict[CostComponent, str] = {
    CostComponent.OCEAN_FREIGHT: (
        "The voyage rate for this exact origin-to-destination leg, in USD per tonne of "
        "cargo. From your own broker indication, a fixture you have done, or a route "
        "assessment you licence. It is per route: US Gulf and Paranaguá to North China "
        "are different distances, and the difference between them is routinely larger "
        "than the FOB spread the page is comparing."
    ),
    CostComponent.ELEVATION: (
        "Barge-to-vessel elevation at the loading area. Required only by a leg quoted "
        "CIF onto a barge — the AMS NOLA bid is one lift short of being on a vessel. "
        "Published nowhere free; from an elevator quote or your own execution history. "
        "Left at zero it would make the US structurally cheapest every single day."
    ),
    CostComponent.MARINE_INSURANCE: (
        "Cargo insurance as a fraction of the CFR value (0.0012 is 0.12%), from your "
        "own policy or broker. It is a percentage, not a flat rate, so it moves with "
        "the value of the cargo."
    ),
    CostComponent.DESTINATION_PORT_COSTS: (
        "Discharge, handling and storage at the destination range, USD per tonne, from "
        "your receiver or agent. One number for the range, not per berth."
    ),
    CostComponent.FINANCING: (
        "Your cost of carry as an annual rate, plus the number of days the cargo is "
        "financed (`--days`). Both are yours, not the market's: there is no "
        "market-wide financing number to look up, which is why the rate and the "
        "period are entered together and charged as rate × days / 365."
    ),
    CostComponent.QUALITY_ADJUSTMENT: (
        "The protein / FM / moisture differential between this origin's contract "
        "specification and what the destination is paying for, USD per tonne, signed. "
        "This stack ingests no protein series. It is NOT zero — US No. 2 Yellow and "
        "Brazilian contract standard are different specifications and a Chinese crusher "
        "pays for protein — so entering zero is a decision that needs your name on it."
    ),
    CostComponent.INLAND_TRANSPORT: (
        "Route-specific inland cost: getting the cargo from its pricing point to the "
        "loading berth, USD per tonne. Required by any leg quoted away from the port "
        "(ex-works, or FCA truck/rail). Truck and rail are different numbers on the "
        "same lane, so enter the mode you will actually use."
    ),
    CostComponent.ORIGIN_PORT_COSTS: (
        "Loading-port charges — terminal, handling, statutory fees — USD per tonne, "
        "for a leg not already quoted FOB vessel."
    ),
    CostComponent.IMPORT_DUTY: (
        "Ad-valorem import duty on the CIF value, as a fraction. A published policy "
        "rate rather than a market view — but still perishable, and an "
        "origin-differentiated retaliatory rate is entered as its own origin-scoped "
        "entry rather than by editing the MFN one."
    ),
    CostComponent.IMPORT_VAT: (
        "Import VAT as a fraction of the duty-paid value. Charged on top of duty, "
        "which is why the waterfall applies it at its own rung rather than adding the "
        "two rates together."
    ),
}

STATUS_SATISFIED = "satisfied"
STATUS_EXPIRING = "expiring"
STATUS_EXPIRED = "expired"
STATUS_MISSING = "missing"
STATUS_UNAVAILABLE = "no_price_series"

#: Statuses that stop a route from producing a landed total.
BLOCKING_STATUSES = frozenset({STATUS_EXPIRED, STATUS_MISSING, STATUS_UNAVAILABLE})


@dataclass(frozen=True)
class InputRequirement:
    """One input a route needs, and where it stands."""

    component: CostComponent
    label: str
    unit: str
    status: str
    origin: str | None
    destination: str
    window: ShipmentWindow
    guidance: str
    command: str
    assumption_id: str | None = None
    entered_by: str | None = None
    entered_at: date | None = None
    expires_on: date | None = None
    days_to_expiry: int | None = None
    confidence: str | None = None
    detail: str = ""

    @property
    def is_blocking(self) -> bool:
        return self.status in BLOCKING_STATUSES

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component.value,
            "label": self.label,
            "unit": self.unit,
            "status": self.status,
            "is_blocking": self.is_blocking,
            "origin": self.origin,
            "destination": self.destination,
            "window": self.window.describe(),
            "guidance": self.guidance,
            "command": self.command,
            "assumption_id": self.assumption_id,
            "entered_by": self.entered_by,
            "entered_at": self.entered_at.isoformat() if self.entered_at else None,
            "expires_on": self.expires_on.isoformat() if self.expires_on else None,
            "days_to_expiry": self.days_to_expiry,
            "confidence": self.confidence,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class RouteReadiness:
    """One origin leg into one destination for one window: ready, or what is missing."""

    leg_id: str
    origin_key: str
    origin_name: str
    label: str
    destination_key: str
    destination_name: str
    window: ShipmentWindow
    incoterm: str
    carrier: str
    requirements: tuple[InputRequirement, ...]
    unavailable_reason: str | None = None

    @property
    def blocking(self) -> tuple[InputRequirement, ...]:
        return tuple(item for item in self.requirements if item.is_blocking)

    @property
    def expiring(self) -> tuple[InputRequirement, ...]:
        return tuple(item for item in self.requirements if item.status == STATUS_EXPIRING)

    @property
    def is_ready(self) -> bool:
        """Every input this route needs is entered and live.

        Ready means *costable*, not *comparable*: a route can have every input
        and still not be ranked, because its quote is for a different shipment
        window or its origin published no window at all. Those are facts about
        the market, not about the onboarding, and they are stated on the
        comparison itself rather than folded in here.
        """
        return self.unavailable_reason is None and not self.blocking

    def to_dict(self) -> dict[str, Any]:
        return {
            "leg_id": self.leg_id,
            "origin_key": self.origin_key,
            "origin_name": self.origin_name,
            "label": self.label,
            "destination_key": self.destination_key,
            "destination_name": self.destination_name,
            "window": self.window.describe(),
            "window_start": self.window.start.isoformat(),
            "incoterm": f"{self.incoterm} {self.carrier}",
            "is_ready": self.is_ready,
            "unavailable_reason": self.unavailable_reason,
            "requirements": [item.to_dict() for item in self.requirements],
            "blocking": [item.component.value for item in self.blocking],
            "expiring": [item.component.value for item in self.expiring],
            "satisfied_count": sum(
                1 for item in self.requirements if item.status == STATUS_SATISFIED
            ),
            "required_count": len(self.requirements),
        }


def required_components(leg_id: str) -> tuple[CostComponent, ...]:
    """Every component this leg needs, in waterfall order, or ``()`` if unbridged.

    The bridge comes first because it is what makes the leg's own quote
    comparable at all: a CIF-barge bid and an FOB-vessel offer are two different
    products until the elevation between them is paid.
    """
    leg = config.ORIGIN_LEGS[leg_id]
    bridge = bridge_components(str(leg["incoterm"]), str(leg["carrier"]))
    if bridge is None:
        return ()
    stack = tuple(CostComponent(name) for name in config.LANDED_STACK)
    return (*bridge, *stack)


def _command(
    component: CostComponent,
    *,
    origin: str | None,
    destination: str,
    window: ShipmentWindow,
) -> str:
    """The exact command that supplies this input, with a placeholder for the value.

    A placeholder rather than a suggestion, and that is the whole discipline of
    this directory: a pre-filled number would be read as a starting point, and a
    starting point that nobody changed is a fabricated default.
    """
    unit = REQUIRED_UNIT[component]
    parts = [
        "python scripts/enter_assumption.py",
        f"--component {component.value}",
        "--value <VALUE>",
        f"--unit {unit}",
    ]
    if unit == UNIT_RATE_PER_ANNUM:
        parts.append("--days <CARRY_DAYS>")
    if component in _ORIGIN_SCOPED:
        parts.append(f"--origin {origin}")
    if component in _DESTINATION_SCOPED:
        parts.append(f"--destination {destination}")
    parts.extend([
        f"--window {window.start.isoformat()}:{window.end.isoformat()}",
        '--basis "<WHAT THIS NUMBER IS AND WHERE IT CAME FROM>"',
        "--entered-by <you@example.com>",
        "--expires <YYYY-MM-DD>",
        "--confidence indicative",
    ])
    return " \\\n  ".join(parts)



def _requirement(
    component: CostComponent,
    *,
    assumptions: AssumptionSet,
    origin: str,
    destination: str,
    window: ShipmentWindow,
    today: date,
    expiring_days: int,
) -> InputRequirement:
    found = assumptions.lookup(
        component, origin=origin, destination=destination, window=window, on=today
    )
    base = {
        "component": component,
        "label": COMPONENT_LABELS[component],
        "unit": REQUIRED_UNIT[component],
        "origin": origin if component in _ORIGIN_SCOPED else None,
        "destination": destination,
        "window": window,
        "guidance": COMPONENT_GUIDANCE.get(component, ""),
        "command": _command(component, origin=origin, destination=destination, window=window),
    }
    if isinstance(found, AssumptionMiss):
        return InputRequirement(
            status=STATUS_EXPIRED if found.expired else STATUS_MISSING,
            detail=found.reason,
            # An expired entry still names its owner: that is who renews it, and
            # a renewal queue with no name on it is a queue nobody works.
            assumption_id=found.expired[0].id if found.expired else None,
            entered_by=found.expired[0].entered_by if found.expired else None,
            expires_on=found.expired[0].expires_on if found.expired else None,
            days_to_expiry=(
                (found.expired[0].expires_on - today).days if found.expired else None
            ),
            **base,  # type: ignore[arg-type]
        )
    days_left = (found.expires_on - today).days
    return InputRequirement(
        status=STATUS_EXPIRING if days_left <= expiring_days else STATUS_SATISFIED,
        assumption_id=found.id,
        entered_by=found.entered_by,
        entered_at=found.entered_at,
        expires_on=found.expires_on,
        days_to_expiry=days_left,
        confidence=found.confidence.value,
        detail=found.basis,
        **base,  # type: ignore[arg-type]
    )


def assess_route(
    assumptions: AssumptionSet,
    *,
    leg_id: str,
    destination_key: str,
    window: ShipmentWindow,
    today: date,
    expiring_days: int = EXPIRY_WARNING_DAYS,
) -> RouteReadiness:
    """What this leg still needs before it can be costed to this destination."""
    leg = config.ORIGIN_LEGS[leg_id]
    origin_key = str(leg["port"])
    origin = config.ORIGIN_PORTS[origin_key]
    destination = config.DESTINATION_PORTS[destination_key]

    components = required_components(leg_id)
    unavailable = leg.get("absent_reason")
    requirements = tuple(
        _requirement(
            component,
            assumptions=assumptions,
            origin=origin_key,
            destination=destination_key,
            window=window,
            today=today,
            expiring_days=expiring_days,
        )
        for component in components
    )
    if not components:
        # An unbridged delivery term is a gap in the *model*, not in the
        # assumptions, and no amount of entering fixes it — so it is reported as
        # the route's own reason rather than as a missing input.
        unavailable = unavailable or (
            f"no costed path from {leg['incoterm']} {leg['carrier']} to FOB vessel is "
            "defined, so this leg cannot be put on the same footing as the others — "
            "add the term to config.INCOTERM_BRIDGE_TO_FOB_VESSEL naming what it owes"
        )

    return RouteReadiness(
        leg_id=leg_id,
        origin_key=origin_key,
        origin_name=origin["name"],
        label=str(leg["label"]),
        destination_key=destination_key,
        destination_name=destination["name"],
        window=window,
        incoterm=str(leg["incoterm"]),
        carrier=str(leg["carrier"]),
        requirements=requirements,
        unavailable_reason=unavailable,
    )


def assess_routes(
    assumptions: AssumptionSet,
    *,
    window: ShipmentWindow,
    today: date,
    destination_key: str | None = None,
    leg_ids: tuple[str, ...] | None = None,
    expiring_days: int = EXPIRY_WARNING_DAYS,
) -> tuple[RouteReadiness, ...]:
    """Every declared leg into one destination, in registry order."""
    destination = destination_key or next(iter(config.DESTINATION_PORTS))
    legs = leg_ids if leg_ids is not None else tuple(config.ORIGIN_LEGS)
    return tuple(
        assess_route(
            assumptions,
            leg_id=leg_id,
            destination_key=destination,
            window=window,
            today=today,
            expiring_days=expiring_days,
        )
        for leg_id in legs
    )


@dataclass(frozen=True)
class RenewalRow:
    """One entered assumption, when it lapses, and what goes dark with it."""

    assumption: Assumption
    days_to_expiry: int
    state: str
    routes_blocked: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.assumption.to_dict(),
            "days_to_expiry": self.days_to_expiry,
            "state": self.state,
            "routes_blocked": list(self.routes_blocked),
        }


def expiry_review(
    assumptions: AssumptionSet,
    *,
    today: date,
    horizon_days: int = 30,
    window: ShipmentWindow | None = None,
    destination_key: str | None = None,
) -> dict[str, Any]:
    """The renewal queue: what lapses, when, who owns it, and what it takes down.

    ``routes_blocked`` is resolved by re-running the readiness check with the
    entry treated as already gone, rather than by matching its scope by hand —
    an entry can be shadowed by a more specific one, in which case its lapse
    blocks nothing, and only the real lookup knows that.
    """
    window = window or _first_offered_window(today)
    destination = destination_key or next(iter(config.DESTINATION_PORTS))

    rows: list[RenewalRow] = []
    for assumption in sorted(assumptions.assumptions, key=lambda item: item.expires_on):
        days_left = (assumption.expires_on - today).days
        if days_left < 0:
            state = STATUS_EXPIRED
        elif days_left <= horizon_days:
            state = STATUS_EXPIRING
        else:
            continue
        rows.append(
            RenewalRow(
                assumption=assumption,
                days_to_expiry=days_left,
                state=state,
                routes_blocked=_routes_that_lose(
                    assumptions,
                    assumption,
                    window=window,
                    destination_key=destination,
                    today=today,
                ),
            )
        )

    return {
        "as_of": today.isoformat(),
        "horizon_days": horizon_days,
        "window": window.describe(),
        "destination": destination,
        "expired": [row.to_dict() for row in rows if row.state == STATUS_EXPIRED],
        "expiring": [row.to_dict() for row in rows if row.state == STATUS_EXPIRING],
        # NOT "clear": Jinja resolves an attribute before a key, and `dict.clear`
        # is a bound method — always truthy — so a renewals section keyed on it
        # would render "nothing due" over a queue with work in it.
        "nothing_due": not rows,
        "note": (
            "An expired input is never replaced by a wider one — the route blocks "
            "instead. That is the point: a lapsed freight number reads as an entered "
            "one, and the only thing worse than no number is a number nobody is "
            "answering for any more."
        ),
    }


def _first_offered_window(today: date) -> ShipmentWindow:
    from analysis.origins.sources import offered_windows

    windows = offered_windows(today)
    return windows[1] if len(windows) > 1 else windows[0]


def _routes_that_lose(
    assumptions: AssumptionSet,
    assumption: Assumption,
    *,
    window: ShipmentWindow,
    destination_key: str,
    today: date,
) -> tuple[str, ...]:
    """Which legs stop being costable if this entry is not renewed."""
    without = AssumptionSet(
        assumptions=tuple(
            item for item in assumptions.assumptions if item.id != assumption.id
        ),
        loaded_from=assumptions.loaded_from,
    )
    lost: list[str] = []
    for leg_id in config.ORIGIN_LEGS:
        before = assess_route(
            assumptions,
            leg_id=leg_id,
            destination_key=destination_key,
            window=window,
            today=today,
        )
        after = assess_route(
            without,
            leg_id=leg_id,
            destination_key=destination_key,
            window=window,
            today=today,
        )
        if before.is_ready and not after.is_ready:
            lost.append(leg_id)
    return tuple(lost)


__all__ = [
    "BLOCKING_STATUSES",
    "COMPONENT_GUIDANCE",
    "STATUS_EXPIRED",
    "STATUS_EXPIRING",
    "STATUS_MISSING",
    "STATUS_SATISFIED",
    "InputRequirement",
    "RenewalRow",
    "RouteReadiness",
    "assess_route",
    "assess_routes",
    "expiry_review",
    "required_components",
]
