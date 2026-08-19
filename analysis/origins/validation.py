"""Set-level validation of entered assumptions (Phase 6).

``assumptions.py`` validates one entry at a time — its unit against its
component, its expiry against its entry date, its fraction against the [0, 1)
band. That is everything a single mapping can be wrong about on its own. It is
not everything a *file of them* can be wrong about, and the remaining faults
are the ones that survive review:

**An ambiguous pair.** Two live assumptions with the same component and the
same scope, whose shipment windows overlap, are two answers to one question.
``AssumptionSet.lookup`` already refuses to pick between them — but it refuses
at *read* time, deep inside a page build, for whichever route happened to ask
first. Found here instead, it is named at the keyboard with both ids, which is
where the person who can retire one of them is standing.

**A scope too wide to mean anything.** Ocean freight is a voyage: US Gulf and
Paranaguá to North China are different distances and different money. An entry
with no ``origin`` matches every origin, so one broker indication silently
becomes the freight for all three and the ranking it produces is arithmetic
about nothing. The same holds for elevation, inland haulage and the protein
differential. These components must name the leg they belong to.

**A scope key that matches nothing.** ``origin: us-gulf`` (a hyphen, not an
underscore) parses, loads, and never matches a route — so it reads on the page
as "nobody entered a freight", which is the one thing it is not. Keys are
checked against ``config.ORIGIN_PORTS`` and ``config.DESTINATION_PORTS``.

**A window that has already sailed.** An entry whose shipment window ended
before it was entered cannot apply to anything, and is almost always a typed
year.

Severity is the whole design here. ``ERROR`` is a fault in the *file* — it is
raised at load, because a set that cannot be resolved unambiguously must not
silently produce a number for the route that happens to resolve. ``WARNING`` is
a fault in the *world* — an entry that has expired, or is about to — which is
reported, never raised: an expired assumption is a record of what was believed
and when, and deleting it to make a loader quiet would destroy the audit trail
this whole contract exists to keep.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import TYPE_CHECKING, Any

import config
from analysis.origins.domain import CostComponent

if TYPE_CHECKING:  # pragma: no cover - typing only
    from analysis.origins.assumptions import Assumption, AssumptionSet

# Components whose value is a property of one loading place. An entry without
# an `origin` applies to every origin, which for a voyage cost is not a
# generous default — it is a different number wearing the right label.
ORIGIN_SCOPED_COMPONENTS = frozenset({
    CostComponent.OCEAN_FREIGHT,
    CostComponent.ELEVATION,
    CostComponent.INLAND_TRANSPORT,
    CostComponent.ORIGIN_PORT_COSTS,
    CostComponent.QUALITY_ADJUSTMENT,
})

# Components whose value is a property of one discharge range: the voyage's far
# end, the tariff schedule that applies there, and the cost of the berth.
DESTINATION_SCOPED_COMPONENTS = frozenset({
    CostComponent.OCEAN_FREIGHT,
    CostComponent.IMPORT_DUTY,
    CostComponent.IMPORT_VAT,
    CostComponent.DESTINATION_PORT_COSTS,
})

# `origin` on a crush-plant cost is a MARKET SLUG, not a port key (see
# data/reference/assumptions/crush_plant.yml). Those components are scoped, but
# against a different catalog, so the port-key check must not be applied to them.
_MARKET_SCOPED_COMPONENTS = frozenset({
    CostComponent.PROCESSING_COST,
    CostComponent.ENERGY_COST,
    CostComponent.PLANT_FREIGHT_IN,
    CostComponent.WORKING_CAPITAL,
})

# How far ahead a lapse is worth saying out loud. Two weeks is roughly how long
# it takes to get a fresh freight indication back from a broker, which is the
# point of warning rather than waiting for the page to block.
EXPIRY_WARNING_DAYS = 14


class Severity(str, Enum):
    ERROR = "error"      # the file is wrong; raised at load
    WARNING = "warning"  # the world moved; reported, never raised


@dataclass(frozen=True)
class ValidationIssue:
    """One named fault, carrying every id involved in it."""

    code: str
    severity: Severity
    message: str
    ids: tuple[str, ...]
    remedy: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity.value,
            "message": self.message,
            "ids": list(self.ids),
            "remedy": self.remedy,
        }

    def __str__(self) -> str:
        return f"[{self.code}] {self.message}"


def _scope_key(assumption: Assumption) -> tuple[Any, ...]:
    return (assumption.component, assumption.origin, assumption.destination)


def _validity_overlaps(left: Assumption, right: Assumption) -> bool:
    """Whether the two are ever usable on the same day.

    A renewal entered while the old one is still running is the ordinary
    workflow, and it is only ambiguous where the two lifetimes actually
    intersect — so the fix a reader is told about is "shorten the outgoing
    entry's ``expires_on``", not "delete it".
    """
    return left.entered_at <= right.expires_on and right.entered_at <= left.expires_on


def _windows_overlap(left: Assumption, right: Assumption) -> bool:
    if left.window is None or right.window is None:
        # A windowless entry is route-wide: it applies to every window, so it
        # collides with any windowed sibling of the same scope.
        return True
    return left.window.overlaps(right.window)


def _ambiguity_issues(assumptions: tuple[Assumption, ...]) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    ordered = sorted(assumptions, key=lambda item: item.id)
    for index, left in enumerate(ordered):
        for right in ordered[index + 1:]:
            if _scope_key(left) != _scope_key(right):
                continue
            if not _validity_overlaps(left, right) or not _windows_overlap(left, right):
                continue
            issues.append(
                ValidationIssue(
                    code="ambiguous_overlap",
                    severity=Severity.ERROR,
                    message=(
                        f"{left.id} and {right.id} both answer "
                        f"{left.component.value} for {left.origin or 'any origin'} → "
                        f"{left.destination or 'any destination'} over overlapping "
                        "shipment windows and overlapping lifetimes — two answers to "
                        "one question means one of them is stale, and a loader that "
                        "picked between them would decide the ranking by file order"
                    ),
                    ids=(left.id, right.id),
                    remedy=(
                        f"shorten the outgoing entry's expires_on to the day before "
                        f"{max(left.entered_at, right.entered_at).isoformat()}, or narrow "
                        "one of the shipment windows so they no longer overlap"
                    ),
                )
            )
    return issues


def _scope_issues(assumption: Assumption) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    component = assumption.component
    if component in ORIGIN_SCOPED_COMPONENTS and not assumption.origin:
        issues.append(
            ValidationIssue(
                code="scope_too_wide",
                severity=Severity.ERROR,
                message=(
                    f"{assumption.id}: {component.value} is a property of one loading "
                    "place and must name an `origin` — without one it applies to every "
                    "origin at once, so a single indication would silently price all "
                    "three legs and the ranking between them would be arithmetic about "
                    "nothing"
                ),
                ids=(assumption.id,),
                remedy=f"add `origin:` (one of {sorted(config.ORIGIN_PORTS)})",
            )
        )
    if component in DESTINATION_SCOPED_COMPONENTS and not assumption.destination:
        issues.append(
            ValidationIssue(
                code="scope_too_wide",
                severity=Severity.ERROR,
                message=(
                    f"{assumption.id}: {component.value} is a property of one discharge "
                    "range and must name a `destination`"
                ),
                ids=(assumption.id,),
                remedy=f"add `destination:` (one of {sorted(config.DESTINATION_PORTS)})",
            )
        )
    if (
        assumption.origin
        and component not in _MARKET_SCOPED_COMPONENTS
        and assumption.origin not in config.ORIGIN_PORTS
    ):
        issues.append(
            ValidationIssue(
                code="unknown_scope_key",
                severity=Severity.ERROR,
                message=(
                    f"{assumption.id}: origin {assumption.origin!r} is not a known port "
                    f"key ({sorted(config.ORIGIN_PORTS)}) — an entry scoped to a key that "
                    "matches nothing reads on the page as 'never entered', which is the "
                    "one thing it is not"
                ),
                ids=(assumption.id,),
                remedy="correct the key, or retire the entry",
            )
        )
    if assumption.destination and assumption.destination not in config.DESTINATION_PORTS:
        issues.append(
            ValidationIssue(
                code="unknown_scope_key",
                severity=Severity.ERROR,
                message=(
                    f"{assumption.id}: destination {assumption.destination!r} is not a "
                    f"known port key ({sorted(config.DESTINATION_PORTS)})"
                ),
                ids=(assumption.id,),
                remedy="correct the key, or retire the entry",
            )
        )
    return issues


def _window_issues(assumption: Assumption) -> list[ValidationIssue]:
    window = assumption.window
    if window is None or window.end >= assumption.entered_at:
        return []
    return [
        ValidationIssue(
            code="window_already_sailed",
            severity=Severity.ERROR,
            message=(
                f"{assumption.id}: shipment window {window.describe()} ended before the "
                f"entry was made ({assumption.entered_at.isoformat()}), so it can never "
                "apply to a window the page offers — almost always a mistyped year"
            ),
            ids=(assumption.id,),
            remedy="correct the window, or drop it to make the entry route-wide",
        )
    ]


def _lifetime_issues(assumption: Assumption, on: date, horizon: int) -> list[ValidationIssue]:
    if assumption.expires_on < on:
        return [
            ValidationIssue(
                code="expired",
                severity=Severity.WARNING,
                message=(
                    f"{assumption.id} expired {assumption.expires_on.isoformat()} "
                    f"({(on - assumption.expires_on).days}d ago) — every route that needs "
                    f"{assumption.component.value} on this scope is blocked until it is "
                    "renewed, and it is deliberately not replaced by a wider entry"
                ),
                ids=(assumption.id,),
                remedy=f"re-enter it (owner of record: {assumption.entered_by})",
            )
        ]
    if assumption.entered_at > on:
        return [
            ValidationIssue(
                code="not_yet_live",
                severity=Severity.WARNING,
                message=(
                    f"{assumption.id} is dated {assumption.entered_at.isoformat()}, which "
                    "is in the future — it is not used until that day"
                ),
                ids=(assumption.id,),
            )
        ]
    if assumption.expires_on <= on + timedelta(days=horizon):
        return [
            ValidationIssue(
                code="expiring",
                severity=Severity.WARNING,
                message=(
                    f"{assumption.id} lapses {assumption.expires_on.isoformat()} "
                    f"({(assumption.expires_on - on).days}d) — after that the route blocks "
                    "rather than falling back to anything"
                ),
                ids=(assumption.id,),
                remedy=f"refresh it before then (owner of record: {assumption.entered_by})",
            )
        ]
    return []


def _sorted(issues: list[ValidationIssue]) -> tuple[ValidationIssue, ...]:
    return tuple(
        sorted(issues, key=lambda issue: (issue.severity is Severity.WARNING, issue.code, issue.ids))
    )


def structural_issues(assumptions: AssumptionSet) -> tuple[ValidationIssue, ...]:
    """The faults that are wrong on any date — scope, keys, ambiguity.

    Separated from the lifetime checks because these are what ``load_assumptions``
    raises on, and a loader whose verdict depended on today's date would fail
    a file on Tuesday that it accepted on Monday.
    """
    issues: list[ValidationIssue] = []
    for assumption in assumptions.assumptions:
        issues.extend(_scope_issues(assumption))
        issues.extend(_window_issues(assumption))
    issues.extend(_ambiguity_issues(assumptions.assumptions))
    return _sorted(issues)


def validate_set(
    assumptions: AssumptionSet,
    *,
    on: date,
    expiry_horizon_days: int = EXPIRY_WARNING_DAYS,
) -> tuple[ValidationIssue, ...]:
    """Every fault in the set, errors first. Pure: no clock, no I/O."""
    issues: list[ValidationIssue] = list(structural_issues(assumptions))
    for assumption in assumptions.assumptions:
        issues.extend(_lifetime_issues(assumption, on, expiry_horizon_days))
    return _sorted(issues)


def errors(issues: tuple[ValidationIssue, ...]) -> tuple[ValidationIssue, ...]:
    return tuple(issue for issue in issues if issue.severity is Severity.ERROR)


def warnings(issues: tuple[ValidationIssue, ...]) -> tuple[ValidationIssue, ...]:
    return tuple(issue for issue in issues if issue.severity is Severity.WARNING)


__all__ = [
    "DESTINATION_SCOPED_COMPONENTS",
    "EXPIRY_WARNING_DAYS",
    "ORIGIN_SCOPED_COMPONENTS",
    "Severity",
    "ValidationIssue",
    "errors",
    "structural_issues",
    "validate_set",
    "warnings",
]
