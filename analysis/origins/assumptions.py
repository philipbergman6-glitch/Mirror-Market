"""The manual-assumption input contract (Phase 2).

Ocean freight, elevation spreads, port charges and processing costs are not
available to this stack for free. Panamax route assessments are Baltic/Platts
products; US barge freight is a paid AMS-adjacent product; nobody publishes a
NOLA barge-to-vessel elevation spread at all. The choice is therefore between
**fabricating precision** and **building the input contract and asking a human
to fill it**, and this module is the second one.

An entered assumption carries five things a fabricated one never does, and each
is required rather than optional:

* **who** entered it (``entered_by``),
* **when** (``entered_at``),
* **what route and window** it applies to,
* **when it stops being usable** (``expires_on``) — the single most important
  field here, because a freight number is perishable and an expired one is
  worse than a missing one: it looks entered,
* **how firm it is** (``confidence``), which propagates to every row it touches.

Two rules make this safe rather than merely documented:

**An expired or absent assumption blocks the row.** It does not default to
zero, and it does not fall back to a wider route. A landed cost with a silently
zero freight leg is exactly the plausible-wrong number this whole phase exists
to avoid. ``lookup`` returns a typed miss with a reason, and
``landed_cost.py`` turns that into a :class:`~analysis.origins.domain.Blocker`.

**Selection is by specificity, never by recency.** Route-and-window beats
route beats destination beats global, and ties are an error rather than a
coin toss — two assumptions that both claim the same route/window/component
mean somebody entered the same thing twice and one of them is stale.

Files live in ``data/reference/assumptions/*.yml`` and are validated on load;
an invalid file raises rather than being skipped, because a silently ignored
assumption file is the same failure as an expired one with better manners.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from typing import Any

import yaml

from analysis.origins.domain import (
    AD_VALOREM_COMPONENTS,
    RATE_TIME_COMPONENTS,
    Confidence,
    CostComponent,
    ShipmentWindow,
    fingerprint,
)

log = logging.getLogger(__name__)

# Units an assumption may be quoted in. Closed, and checked at load: an
# assumption whose unit is a typo would be read as USD/MT and add 3 dollars
# where 3 percent was meant.
UNIT_USD_MT = "usd_per_mt"
UNIT_FRACTION = "fraction"          # 0.03 == 3%
UNIT_RATE_PER_ANNUM = "rate_per_annum"  # 0.065 == 6.5%/yr, needs `days`
UNITS = frozenset({UNIT_USD_MT, UNIT_FRACTION, UNIT_RATE_PER_ANNUM})

# Which unit each component must be quoted in. Not a suggestion: an ad-valorem
# duty entered as USD/MT parses cleanly and is wrong by two orders of
# magnitude, and nothing downstream could tell.
REQUIRED_UNIT: dict[CostComponent, str] = {
    component: (
        UNIT_RATE_PER_ANNUM if component in RATE_TIME_COMPONENTS
        else UNIT_FRACTION if component in AD_VALOREM_COMPONENTS
        else UNIT_USD_MT
    )
    for component in CostComponent
}

# Confidence values that describe an *observed market price* and can never
# describe something a person typed into a YAML file. `board_reference` joined
# `executable` here when the two were separated: a hand-entered freight number
# is not a venue's own delayed close any more than it is a proven settlement.
_MARKET_ONLY_CONFIDENCE = frozenset({Confidence.EXECUTABLE, Confidence.BOARD_REFERENCE})

REQUIRED_FIELDS = (
    "id", "component", "value", "unit", "basis", "source",
    "entered_by", "entered_at", "expires_on", "confidence",
)


class AssumptionError(ValueError):
    """An assumption file is malformed. Never swallowed — see module docstring."""


@dataclass(frozen=True)
class Assumption:
    """One entered cost input, scoped to a route, a window and a lifetime."""

    id: str
    component: CostComponent
    value: float
    unit: str
    basis: str
    source: str
    entered_by: str
    entered_at: date
    expires_on: date
    confidence: Confidence
    origin: str | None = None
    destination: str | None = None
    window: ShipmentWindow | None = None
    days: int | None = None
    citation: str | None = None

    def __post_init__(self) -> None:
        if self.unit not in UNITS:
            raise AssumptionError(f"{self.id}: unit {self.unit!r} not in {sorted(UNITS)}")
        expected = REQUIRED_UNIT[self.component]
        if self.unit != expected:
            raise AssumptionError(
                f"{self.id}: component {self.component.value} must be quoted in {expected!r}, "
                f"got {self.unit!r} — an ad-valorem rate entered as USD/MT parses cleanly "
                "and is wrong by two orders of magnitude"
            )
        if self.expires_on < self.entered_at:
            raise AssumptionError(f"{self.id}: expires_on precedes entered_at")
        if self.component in RATE_TIME_COMPONENTS and not self.days:
            raise AssumptionError(
                f"{self.id}: {self.component.value} is a rate over time and needs `days` "
                "(the carry period the rate is charged over)"
            )
        if self.unit == UNIT_FRACTION and not 0.0 <= self.value < 1.0:
            raise AssumptionError(
                f"{self.id}: a fraction must be in [0, 1) — {self.value} looks like a percentage "
                "entered as a whole number"
            )
        if self.unit == UNIT_USD_MT and abs(self.value) > 1000:
            raise AssumptionError(
                f"{self.id}: {self.value} USD/MT is larger than the cargo it is a cost on — "
                "check the unit"
            )

    @property
    def specificity(self) -> int:
        """Higher wins. Route+window is the most specific thing that can be entered."""
        score = 0
        if self.origin:
            score += 4
        if self.destination:
            score += 2
        if self.window:
            score += 1
        return score

    def is_live(self, on: date) -> bool:
        return self.entered_at <= on <= self.expires_on

    def applies_to(
        self,
        *,
        component: CostComponent,
        origin: str | None,
        destination: str | None,
        window: ShipmentWindow,
    ) -> bool:
        if self.component is not component:
            return False
        if self.origin is not None and self.origin != origin:
            return False
        if self.destination is not None and self.destination != destination:
            return False
        # A windowless assumption is route-wide and applies to any window; a
        # windowed one applies only where the periods actually overlap. Never
        # "nearest window" — the nearest window to October is September, and
        # September freight priced as October freight is a wrong number that
        # nothing downstream can detect.
        return self.window is None or self.window.overlaps(window)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "component": self.component.value,
            "value": self.value,
            "unit": self.unit,
            "days": self.days,
            "origin": self.origin,
            "destination": self.destination,
            "window": self.window.to_dict() if self.window else None,
            "basis": self.basis,
            "source": self.source,
            "entered_by": self.entered_by,
            "entered_at": self.entered_at.isoformat(),
            "expires_on": self.expires_on.isoformat(),
            "confidence": self.confidence.value,
            "citation": self.citation,
        }


@dataclass(frozen=True)
class AssumptionMiss:
    """Why no assumption was available. A typed absence, not ``None``.

    ``expired`` is separated from ``absent`` because they call for different
    human actions and read very differently on a page: "nobody has ever entered
    a US Gulf → North China freight" is a gap in the workflow, while "the one
    that was entered lapsed on 17 September" is a task with a due date that
    passed.
    """

    component: CostComponent
    reason: str
    expired: tuple[Assumption, ...] = ()

    @property
    def code(self) -> str:
        return "assumption_expired" if self.expired else "assumption_missing"


def _as_date(value: Any, field: str, where: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError) as exc:
        raise AssumptionError(f"{where}: {field} must be an ISO date, got {value!r}") from exc


def _parse_window(raw: Any, where: str) -> ShipmentWindow | None:
    if raw is None:
        return None
    if not isinstance(raw, dict) or {"start", "end"} - raw.keys():
        raise AssumptionError(f"{where}: window needs `start` and `end`")
    return ShipmentWindow(
        start=_as_date(raw["start"], "window.start", where),
        end=_as_date(raw["end"], "window.end", where),
        label=raw.get("label"),
    )


def parse_assumption(raw: dict, *, where: str) -> Assumption:
    """Build one assumption from a mapping, hard-failing on anything ambiguous."""
    if not isinstance(raw, dict):
        raise AssumptionError(f"{where}: each assumption must be a mapping, got {type(raw).__name__}")
    missing = [field for field in REQUIRED_FIELDS if raw.get(field) in (None, "")]
    if missing:
        raise AssumptionError(
            f"{where}: assumption {raw.get('id', '<unnamed>')!r} is missing {missing} — "
            "an assumption with no owner, date or expiry is indistinguishable from a guess"
        )
    try:
        component = CostComponent(str(raw["component"]))
    except ValueError as exc:
        raise AssumptionError(
            f"{where}: unknown component {raw['component']!r}; "
            f"known: {[c.value for c in CostComponent]}"
        ) from exc
    try:
        confidence = Confidence(str(raw["confidence"]))
    except ValueError as exc:
        raise AssumptionError(f"{where}: unknown confidence {raw['confidence']!r}") from exc
    if confidence in _MARKET_ONLY_CONFIDENCE:
        raise AssumptionError(
            f"{where}: an entered assumption cannot be `{confidence.value}` — those values are "
            "reserved for an observed market price (a proven settlement, or a venue's own "
            "delayed close), and no hand-entered freight number is either"
        )
    return Assumption(
        id=str(raw["id"]),
        component=component,
        value=float(raw["value"]),
        unit=str(raw["unit"]),
        basis=str(raw["basis"]),
        source=str(raw["source"]),
        entered_by=str(raw["entered_by"]),
        entered_at=_as_date(raw["entered_at"], "entered_at", where),
        expires_on=_as_date(raw["expires_on"], "expires_on", where),
        confidence=confidence,
        origin=raw.get("origin"),
        destination=raw.get("destination"),
        window=_parse_window(raw.get("window"), where),
        days=int(raw["days"]) if raw.get("days") is not None else None,
        citation=raw.get("citation"),
    )


@dataclass(frozen=True)
class AssumptionSet:
    """Every live assumption, plus the lookup rule.

    ``set_id`` fingerprints the whole set. A stored ranking records it, so a
    ranking that moved because freight was re-entered is distinguishable from
    one that moved because the market did — which is the difference between a
    signal and a bookkeeping artefact.
    """

    assumptions: tuple[Assumption, ...]
    loaded_from: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ids = [assumption.id for assumption in self.assumptions]
        duplicates = sorted({key for key in ids if ids.count(key) > 1})
        if duplicates:
            raise AssumptionError(f"duplicate assumption id(s): {duplicates}")

    @property
    def set_id(self) -> str:
        return fingerprint([assumption.to_dict() for assumption in self.assumptions])

    def lookup(
        self,
        component: CostComponent,
        *,
        origin: str | None,
        destination: str | None,
        window: ShipmentWindow,
        on: date,
    ) -> Assumption | AssumptionMiss:
        """The single most specific live assumption, or a typed miss.

        Never falls back to a wider route when the specific one has expired:
        an expired US Gulf → North China rate does not become a global rate,
        and quietly substituting one would hide the lapse behind a number.
        """
        candidates = [
            assumption for assumption in self.assumptions
            if assumption.applies_to(
                component=component, origin=origin, destination=destination, window=window
            )
        ]
        if not candidates:
            return AssumptionMiss(
                component,
                f"no {component.value} assumption covers {origin or 'any origin'} → "
                f"{destination or 'any destination'} for {window.describe()}",
            )
        # Specificity is resolved BEFORE liveness, not after. Resolving it after
        # means a lapsed route rate quietly loses to a live global one and the
        # page shows a number for a route whose own assumption expired — the
        # exact silent substitution this module exists to prevent. The most
        # specific tier wins the question; whether that tier is still live then
        # decides between an answer and a named miss.
        best = max(assumption.specificity for assumption in candidates)
        finalists = [
            assumption for assumption in candidates if assumption.specificity == best
        ]
        live = [assumption for assumption in finalists if assumption.is_live(on)]
        if not live:
            lapsed = tuple(sorted(finalists, key=lambda a: a.expires_on, reverse=True))
            newest = lapsed[0]
            wider = len(candidates) - len(finalists)
            return AssumptionMiss(
                component,
                f"the {component.value} assumption for this route expired on "
                f"{newest.expires_on.isoformat()} ({newest.id}) — a lapsed freight number "
                "reads as an entered one, which is why it is not used"
                + (
                    f", and the {wider} less specific assumption(s) that would still match "
                    "are deliberately not substituted"
                    if wider else ""
                ),
                expired=lapsed,
            )
        finalists = live
        if len(finalists) > 1:
            raise AssumptionError(
                f"{component.value}: {len(finalists)} equally specific live assumptions "
                f"({[a.id for a in finalists]}) — two answers to one question means one of "
                "them is stale; retire it rather than letting the loader pick"
            )
        return finalists[0]

    def live(self, on: date) -> tuple[Assumption, ...]:
        return tuple(assumption for assumption in self.assumptions if assumption.is_live(on))

    def expiring_within(self, days: int, *, on: date) -> tuple[Assumption, ...]:
        from datetime import timedelta

        horizon = on + timedelta(days=days)
        return tuple(
            assumption for assumption in self.live(on) if assumption.expires_on <= horizon
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "set_id": self.set_id,
            "loaded_from": list(self.loaded_from),
            "assumptions": [assumption.to_dict() for assumption in self.assumptions],
        }


def load_assumptions(directory: str | os.PathLike[str] | None = None) -> AssumptionSet:
    """Read and validate every ``*.yml`` in the assumptions directory.

    A missing directory is an empty set — a fresh clone has entered nothing,
    and that is a legitimate state whose consequence (every landed cost
    blocked, loudly) is exactly right. A *present but malformed* file raises:
    the difference between "nothing entered" and "something entered wrongly"
    is the difference this module is for.
    """
    import config

    root = str(directory) if directory is not None else config.ASSUMPTIONS_DIR
    if not os.path.isdir(root):
        log.info("No assumptions directory at %s — every costed leg will block", root)
        return AssumptionSet(assumptions=())

    parsed: list[Assumption] = []
    files: list[str] = []
    for name in sorted(os.listdir(root)):
        if not name.endswith((".yml", ".yaml")):
            continue
        path = os.path.join(root, name)
        with open(path, encoding="utf-8") as handle:
            document = yaml.safe_load(handle)
        if document is None:
            files.append(name)
            continue
        if not isinstance(document, list):
            raise AssumptionError(f"{name}: expected a YAML list of assumptions")
        for index, raw in enumerate(document):
            parsed.append(parse_assumption(raw, where=f"{name}[{index}]"))
        files.append(name)

    result = AssumptionSet(assumptions=tuple(parsed), loaded_from=tuple(files))

    # Set-level validation. One entry can be valid on its own and the file still
    # be unusable — an ocean freight with no origin prices all three legs off one
    # indication, and two overlapping entries make the answer depend on file
    # order. Those are raised here rather than at lookup, because at lookup the
    # complaint surfaces halfway through a page build for whichever route asked
    # first, and the person who can retire the stale entry is not there.
    from analysis.origins.validation import errors, structural_issues

    faults = errors(structural_issues(result))
    if faults:
        raise AssumptionError(
            f"{len(faults)} unusable assumption(s) in {root}:\n  "
            + "\n  ".join(f"{issue}\n    remedy: {issue.remedy}" for issue in faults)
        )

    log.info("Loaded %d assumption(s) from %s (set %s)", len(parsed), root, result.set_id)
    return result


__all__ = [
    "REQUIRED_UNIT",
    "UNIT_FRACTION",
    "UNIT_RATE_PER_ANNUM",
    "UNIT_USD_MT",
    "Assumption",
    "AssumptionError",
    "AssumptionMiss",
    "AssumptionSet",
    "load_assumptions",
    "parse_assumption",
]
