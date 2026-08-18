"""The vocabulary of the opportunity engine (Phase 4).

Standard library plus the Phase 2 value objects, and nothing else: persistence
lives in ``registry.py``, SQL in ``signals.py``, presentation in ``app/``. Same
separation ``analysis/origins/domain.py`` and ``analysis/futures/domain.py``
keep, for the same reason — a vocabulary that can read a database grows a
database's opinions.

Four ideas carry this module, and each exists because getting it wrong produces
a *plausible* opportunity rather than a crash.

**A price difference is not a trade.** The single most dangerous output this
project could produce is "Brazil is 12 dollars cheaper into China" rendered as
something to do. Whether that spread can be closed depends on policy, freight,
quality, timing and liquidity, and none of those are in the price. So every
candidate carries its :class:`Blocker` set, a *hard* blocker demotes it down the
ladder by construction, and :class:`Opportunity` refuses to be built as
``ACTIONABLE`` while one is present.

**The ladder is five rungs, not a boolean.** A market signal, a lead, an
actionable opportunity, a proposed trade and completed business are five
different things, and collapsing them is how a screen full of observations
starts reading like a pipeline of deals. The first three are reachable from
ingested data. The last two are reachable only from a human's own record, and
:class:`Opportunity.__post_init__` enforces that — a detector cannot promote
anything to ``PROPOSED_TRADE``.

**Unknown stays unknown.** No counterparty, contact, volume, freight rate or
window is ever invented to fill a field. A missing volume is ``None`` with a
stated reason, never a "typical Panamax". This is the same rule
``UnavailableOrigin`` encodes in Phase 2: there is nothing to place-hold.

**Private and public are different objects, not different templates.**
Everything a human typed — owner, notes, contact dates, outcomes, dismissal
reasons, and every workflow status past ``detected`` — lives on
:class:`WorkflowRecord`, and ``to_dict(audience=PUBLIC)`` does not serialise it.
A template that forgets a condition cannot leak what was never in its context.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, replace
from datetime import date, timedelta
from enum import Enum
from typing import Any

from analysis.origins.domain import (
    Confidence,
    Freshness,
    Grade,
    Incoterm,
    Money,
    Port,
    ShipmentWindow,
    SourceRef,
    worst_confidence,
)

__all__ = [
    "AUDIENCE_PRIVATE",
    "AUDIENCE_PUBLIC",
    "DETECTOR_STATUSES",
    "PRIVATE_FIELD_NAMES",
    "STATUS_LADDER",
    "AuditEntry",
    "Blocker",
    "BlockerCode",
    "Blockers",
    "Confidence",
    "Counterparty",
    "Dislocation",
    "Economics",
    "Evidence",
    "Feedback",
    "FeedbackKind",
    "Ladder",
    "MarketSignal",
    "Opportunity",
    "OpportunityError",
    "OpportunityStatus",
    "PartyRole",
    "ScoreCard",
    "SignalKind",
    "Volume",
    "WorkflowRecord",
    "identity_key",
    "opportunity_id",
]


class OpportunityError(ValueError):
    """A candidate said something that cannot be true. Raised, never rendered."""


# ---------------------------------------------------------------------------
# The ladder — requirement 10, made structural
# ---------------------------------------------------------------------------
class Ladder(str, Enum):
    """What kind of thing this is. Ordered; higher rungs need more.

    ``MARKET_SIGNAL``  an observation about the market. No lane, no counterparty,
                       nothing to act on. Most of what the pipeline produces.
    ``LEAD``           a signal attached to a lane (origin → destination) with
                       named candidate counterparties — but blocked, unpriced or
                       resting on evidence too old or too thin to act on.
    ``ACTIONABLE``     economics computed on comparable inputs, evidence inside
                       its own recency budget, no hard blocker. This is the only
                       rung that says "you could do this today".
    ``PROPOSED_TRADE`` a human took it up. Only reachable from a workflow record.
    ``COMPLETED``      it was won or lost. Only reachable from a workflow record.
    """

    MARKET_SIGNAL = "market_signal"
    LEAD = "lead"
    ACTIONABLE = "actionable"
    PROPOSED_TRADE = "proposed_trade"
    COMPLETED = "completed_business"

    @property
    def rank(self) -> int:
        return _LADDER_RANK[self]

    @property
    def meaning(self) -> str:
        return _LADDER_MEANING[self]


_LADDER_RANK = {
    Ladder.MARKET_SIGNAL: 0,
    Ladder.LEAD: 1,
    Ladder.ACTIONABLE: 2,
    Ladder.PROPOSED_TRADE: 3,
    Ladder.COMPLETED: 4,
}

_LADDER_MEANING = {
    Ladder.MARKET_SIGNAL: (
        "An observation about the market. It names no lane and no counterparty; "
        "it is the input to a lead, not a thing to do."
    ),
    Ladder.LEAD: (
        "A signal on a named lane with candidate counterparties, held back by at "
        "least one blocker, missing input, or evidence past its budget. Worth a "
        "phone call, not a price."
    ),
    Ladder.ACTIONABLE: (
        "Economics computed on comparable inputs, every piece of evidence inside "
        "its own recency budget, no hard blocker. The trade can plausibly be "
        "worked today."
    ),
    Ladder.PROPOSED_TRADE: (
        "A person has taken this up — contacted or negotiating. This rung cannot "
        "be reached by a detector; it exists only in the local workflow file."
    ),
    Ladder.COMPLETED: (
        "Business that was won or lost. Recorded by a person, never inferred from "
        "market data."
    ),
}

#: The rungs a detector may emit. Everything above needs a human's own record —
#: enforced in ``Opportunity.__post_init__`` rather than left to convention.
DETECTABLE_RUNGS = frozenset({Ladder.MARKET_SIGNAL, Ladder.LEAD, Ladder.ACTIONABLE})


class OpportunityStatus(str, Enum):
    """Where this sits in a human's own process."""

    DETECTED = "detected"
    REVIEWING = "reviewing"
    ACTIONABLE = "actionable"
    CONTACTED = "contacted"
    NEGOTIATING = "negotiating"
    WON = "won"
    LOST = "lost"
    EXPIRED = "expired"
    DISMISSED = "dismissed"


#: The two statuses a detector can set on its own. `detected` is what a rule
#: produces; `expired` is what the clock produces. Every other status is a
#: statement about what a person did, so it can only come from the workflow
#: file — and is therefore private. This frozenset is the privacy boundary's
#: definition, not a hint: `tests/test_opportunities_page.py` asserts that no
#: other status can reach a public artifact.
DETECTOR_STATUSES = frozenset({OpportunityStatus.DETECTED, OpportunityStatus.EXPIRED})

#: Which rung a status implies. A status never *lowers* the detected rung —
#: a contacted lead is still a lead in market terms — it only raises it.
STATUS_LADDER: dict[OpportunityStatus, Ladder | None] = {
    OpportunityStatus.DETECTED: None,
    OpportunityStatus.REVIEWING: None,
    OpportunityStatus.ACTIONABLE: Ladder.ACTIONABLE,
    OpportunityStatus.CONTACTED: Ladder.PROPOSED_TRADE,
    OpportunityStatus.NEGOTIATING: Ladder.PROPOSED_TRADE,
    OpportunityStatus.WON: Ladder.COMPLETED,
    OpportunityStatus.LOST: Ladder.COMPLETED,
    OpportunityStatus.EXPIRED: None,
    OpportunityStatus.DISMISSED: None,
}

#: Statuses that stop an opportunity from being worked further.
TERMINAL_STATUSES = frozenset({
    OpportunityStatus.WON,
    OpportunityStatus.LOST,
    OpportunityStatus.EXPIRED,
    OpportunityStatus.DISMISSED,
})

AUDIENCE_PUBLIC = "public"
AUDIENCE_PRIVATE = "private"

#: Attributes of :class:`Opportunity` that must never appear in a public
#: artifact. Asserted against ``to_dict(AUDIENCE_PUBLIC)`` by test.
PRIVATE_FIELD_NAMES = frozenset({"workflow"})


# ---------------------------------------------------------------------------
# Blockers — requirement 2, made structural
# ---------------------------------------------------------------------------
class BlockerCode(str, Enum):
    """Why a spread may not be closable. Closed set: an unnamed reason is a bug.

    Each of these is a real reason a physical trade fails to happen, and each
    has been seen in this project's own data. They are not severities in
    disguise — see :data:`HARD_BLOCKERS`, which is the separate question of
    whether the trade is *impossible* or merely *harder*.
    """

    POLICY_BARRIER = "policy_barrier"
    FREIGHT_UNKNOWN = "freight_unknown"
    QUALITY_UNPRICED = "quality_unpriced"
    WINDOW_INCOMPATIBLE = "window_incompatible"
    LIQUIDITY_UNPROVEN = "liquidity_unproven"
    EVIDENCE_STALE = "evidence_stale"
    NO_COUNTERPARTY = "no_counterparty"
    NOT_COMPARABLE = "not_comparable"
    INGEST_OUTAGE = "ingest_outage"
    SIZE_UNKNOWN = "size_unknown"


#: Blockers that make the trade impossible rather than merely uncertain. A
#: candidate carrying one of these can never be ``ACTIONABLE``; it is a lead.
#:
#: ``POLICY_BARRIER`` is the reason this set exists. India's mandi bean prints
#: ~+66% over CBOT and has reached ~2x; that is not an arbitrage, because GM
#: imports are banned behind a tariff wall and nothing closes it (M19 #222).
#: Rendered as an opportunity it would invite a trade that cannot be taken.
HARD_BLOCKERS = frozenset({
    BlockerCode.POLICY_BARRIER,
    BlockerCode.FREIGHT_UNKNOWN,
    BlockerCode.WINDOW_INCOMPATIBLE,
    BlockerCode.EVIDENCE_STALE,
    BlockerCode.NOT_COMPARABLE,
    BlockerCode.NO_COUNTERPARTY,
    BlockerCode.INGEST_OUTAGE,
})


@dataclass(frozen=True)
class Blocker:
    """A named reason this may not be workable, and what would clear it.

    ``remedy`` is required in spirit and checked in practice: every blocker this
    engine raises is either fixable by a human action (enter a freight number,
    verify a counterparty, wait for a scraper) or permanent and worth saying so
    ("nothing clears this — it is policy"). A blocked row with no remedy is a
    row a reader learns to skip.
    """

    code: BlockerCode
    message: str
    remedy: str
    detail: str | None = None

    def __post_init__(self) -> None:
        if not self.message.strip() or not self.remedy.strip():
            raise OpportunityError(
                f"blocker {self.code.value} needs both a message and a remedy — a "
                "blocked opportunity that does not say what would clear it is one "
                "nobody acts on and nobody removes"
            )

    @property
    def is_hard(self) -> bool:
        return self.code in HARD_BLOCKERS

    @property
    def severity(self) -> str:
        return "hard" if self.is_hard else "soft"

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "severity": self.severity,
            "message": self.message,
            "remedy": self.remedy,
            "detail": self.detail,
        }


Blockers = tuple[Blocker, ...]


# ---------------------------------------------------------------------------
# Evidence and signals
# ---------------------------------------------------------------------------
class SignalKind(str, Enum):
    """Which family of market observation started this."""

    LANDED_ADVANTAGE = "landed_advantage"
    FLOW_SHIFT = "flow_shift"
    COMMITMENT_SHIFT = "commitment_shift"
    SUPPLY_DEFICIT = "supply_deficit"
    CRUSH_MARGIN = "crush_margin"
    CURRENCY_SHIFT = "currency_shift"


@dataclass(frozen=True)
class Evidence:
    """One number this rests on, precisely enough to go and look at it.

    ``observed_on`` is the date the *market* was observed, never the date the
    pipeline ran — the two differ on any run landing before settlement, and a
    freshness judgement made on the wrong one grades a three-day-old print as
    today's. ``max_age_days`` is the owning layer's own
    ``config.LAYER_MAX_DATA_AGE_DAYS`` budget, so this page and ``main.py``
    cannot disagree about whether a source is alive.
    """

    label: str
    value: float | None
    unit: str
    observed_on: date
    source: SourceRef
    max_age_days: int
    quote_kind: str | None = None
    confidence: Confidence = Confidence.INDICATIVE
    note: str | None = None

    def age_days(self, today: date) -> int:
        return (today - self.observed_on).days

    def freshness(self, today: date) -> Freshness:
        return (
            Freshness.CURRENT
            if self.age_days(today) <= self.max_age_days
            else Freshness.STALE
        )

    def to_dict(self, today: date | None = None) -> dict[str, Any]:
        return {
            "label": self.label,
            "value": self.value,
            "unit": self.unit,
            "observed_on": self.observed_on.isoformat(),
            "age_days": self.age_days(today) if today else None,
            "freshness": self.freshness(today).value if today else None,
            "max_age_days": self.max_age_days,
            "quote_kind": self.quote_kind,
            "confidence": self.confidence.value,
            "note": self.note,
            "source": self.source.to_dict(),
        }


@dataclass(frozen=True)
class MarketSignal:
    """The bottom rung: something the market did, with its evidence.

    A signal is deliberately *not* an opportunity and carries no counterparty,
    no lane and no economics. Rules turn signals into candidates; a signal that
    no rule takes up is still published, because "we saw this and did nothing
    with it" is a more honest screen than one that only shows what fired.
    """

    signal_id: str
    kind: SignalKind
    headline: str
    detail: str
    observed_on: date
    evidence: tuple[Evidence, ...]
    #: How long this observation stays meaningful. A daily price signal is stale
    #: in days; a monthly balance-sheet signal is not. Set by the rule that
    #: raised it, never a global default.
    validity_days: int
    magnitude: float | None = None
    magnitude_unit: str | None = None
    subject: str | None = None            # commodity / country / lane, free text
    ladder: Ladder = Ladder.MARKET_SIGNAL

    def __post_init__(self) -> None:
        if not self.evidence:
            raise OpportunityError(
                f"signal {self.signal_id!r} carries no evidence — an observation "
                "with no source is an assertion"
            )
        if self.validity_days <= 0:
            raise OpportunityError(f"signal {self.signal_id!r}: validity_days must be positive")

    @property
    def expires_on(self) -> date:
        return self.observed_on + timedelta(days=self.validity_days)

    def is_live(self, today: date) -> bool:
        return today <= self.expires_on

    def worst_evidence_confidence(self) -> Confidence:
        return worst_confidence(*(item.confidence for item in self.evidence))

    def to_dict(self, today: date | None = None) -> dict[str, Any]:
        return {
            "signal_id": self.signal_id,
            "kind": self.kind.value,
            "headline": self.headline,
            "detail": self.detail,
            "subject": self.subject,
            "observed_on": self.observed_on.isoformat(),
            "expires_on": self.expires_on.isoformat(),
            "validity_days": self.validity_days,
            "magnitude": self.magnitude,
            "magnitude_unit": self.magnitude_unit,
            "ladder": self.ladder.value,
            "evidence": [item.to_dict(today) for item in self.evidence],
        }


# ---------------------------------------------------------------------------
# Counterparties, volume, economics
# ---------------------------------------------------------------------------
class PartyRole(str, Enum):
    SELLER = "seller"
    BUYER = "buyer"
    FACILITY = "facility"


@dataclass(frozen=True)
class Counterparty:
    """A candidate, with the evidence for it and the age of that evidence.

    A thin, opportunity-side view of ``analysis.origins.players.Counterparty``,
    carried rather than re-derived so an opportunity is self-contained. Nothing
    here is a contact: the players base records company-level published contact
    surfaces only, never named individuals (#111 decision 5), and this engine
    does not read even those — a screen is not a place to put somebody's phone
    number.
    """

    name: str
    country: str
    role: PartyRole
    roles: tuple[str, ...] = ()
    products: tuple[str, ...] = ()
    tier: int = 2
    lane_evidenced: bool = False
    lane_note: str | None = None
    confidence: str = "inferred"          # observed | inferred, from the entry
    last_verified: date | None = None
    citation: str | None = None
    footprint: str | None = None
    scope: str = "local"

    def verification_age_days(self, today: date) -> int | None:
        return None if self.last_verified is None else (today - self.last_verified).days

    def to_dict(self, today: date | None = None) -> dict[str, Any]:
        return {
            "name": self.name,
            "country": self.country,
            "role": self.role.value,
            "roles": list(self.roles),
            "products": list(self.products),
            "tier": self.tier,
            "lane_evidenced": self.lane_evidenced,
            "lane_note": self.lane_note,
            "confidence": self.confidence,
            "last_verified": self.last_verified.isoformat() if self.last_verified else None,
            "verification_age_days": self.verification_age_days(today) if today else None,
            "citation": self.citation,
            "footprint": self.footprint,
            "scope": self.scope,
        }


@dataclass(frozen=True)
class Volume:
    """A tonnage estimate that came from somewhere, or nothing at all.

    Every field is required, ``basis`` included. This project ingests no cargo
    manifest, no line-up and no contract, so the only honest volumes are ones
    derived from a published flow (a week's inspections to a destination, a
    month's crush) and the derivation has to be stated. Where no such number
    exists the opportunity carries ``volume=None`` and a
    :class:`BlockerCode.SIZE_UNKNOWN`; a "typical Panamax" would be a number
    this software invented.
    """

    low_mt: float
    high_mt: float
    basis: str
    source: SourceRef

    def __post_init__(self) -> None:
        if self.low_mt <= 0 or self.high_mt < self.low_mt:
            raise OpportunityError(
                f"volume range {self.low_mt}..{self.high_mt} MT is not a range"
            )
        if not self.basis.strip():
            raise OpportunityError("a volume estimate must state what it was derived from")

    @property
    def is_point(self) -> bool:
        return abs(self.high_mt - self.low_mt) < 1e-9

    def to_dict(self) -> dict[str, Any]:
        return {
            "low_mt": self.low_mt,
            "high_mt": self.high_mt,
            "is_point": self.is_point,
            "basis": self.basis,
            "source": self.source.to_dict(),
        }


@dataclass(frozen=True)
class Economics:
    """What the opportunity is worth per tonne, and how that was struck.

    ``per_mt`` is the edge, not the price: a landed advantage, a crush margin, a
    dislocation. ``total_usd`` exists only where a :class:`Volume` does — an
    edge times an invented tonnage is an invented number, and it is the one a
    reader would quote.
    """

    per_mt: Money
    method: str
    method_version: str
    struck_on: date
    components: tuple[tuple[str, float], ...] = ()
    total_low_usd: float | None = None
    total_high_usd: float | None = None
    note: str | None = None

    def with_volume(self, volume: Volume | None) -> Economics:
        if volume is None:
            return self
        return replace(
            self,
            total_low_usd=self.per_mt.amount * volume.low_mt,
            total_high_usd=self.per_mt.amount * volume.high_mt,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "per_mt": self.per_mt.amount,
            "currency": self.per_mt.currency,
            "unit": self.per_mt.unit,
            "method": self.method,
            "method_version": self.method_version,
            "struck_on": self.struck_on.isoformat(),
            "components": [{"label": label, "value": value} for label, value in self.components],
            "total_low_usd": self.total_low_usd,
            "total_high_usd": self.total_high_usd,
            "note": self.note,
        }


@dataclass(frozen=True)
class Dislocation:
    """What is out of line, expressed against its own normal.

    Separate from :class:`Economics` on purpose. A 12 USD/MT landed advantage is
    money; "China's share of US inspections is 2.4 standard deviations above its
    52-week mean" is a dislocation and is worth nothing per tonne. Rendering the
    second as the first is how a screen starts recommending trades that have no
    margin in them.
    """

    kind: str                 # landed_advantage | flow | balance_sheet | fx | margin
    label: str
    value: float
    unit: str
    baseline: float | None = None
    baseline_label: str | None = None
    z_score: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "label": self.label,
            "value": self.value,
            "unit": self.unit,
            "baseline": self.baseline,
            "baseline_label": self.baseline_label,
            "z_score": self.z_score,
        }


# ---------------------------------------------------------------------------
# Scoring — requirement 3
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ScoreCard:
    """Five components, each 0–100, and the weighted composite of them.

    The components are the product; the composite is a sort key. Both are
    rendered, and the page shows the components beside the total rather than a
    bare number — a ranked list whose ordering cannot be explained is a list
    whose ordering will be believed.

    Every component's arithmetic is stated in ``scoring.py`` and is simple
    enough to check by hand from the rendered inputs. That is a requirement,
    not an accident: a score a trader cannot reproduce is a score a trader
    cannot argue with.
    """

    economic: float
    evidence: float
    freshness: float
    counterparty: float
    feasibility: float
    weights: tuple[tuple[str, float], ...]
    notes: tuple[tuple[str, str], ...] = ()

    _COMPONENTS = ("economic", "evidence", "freshness", "counterparty", "feasibility")

    def __post_init__(self) -> None:
        for name in self._COMPONENTS:
            value = getattr(self, name)
            if not 0.0 <= value <= 100.0:
                raise OpportunityError(f"score component {name} = {value} is outside 0..100")
        total = sum(weight for _, weight in self.weights)
        if abs(total - 1.0) > 1e-9:
            raise OpportunityError(f"score weights sum to {total}, not 1.0")
        if {name for name, _ in self.weights} != set(self._COMPONENTS):
            raise OpportunityError("score weights must name exactly the five components")

    @property
    def composite(self) -> float:
        lookup = dict(self.weights)
        return round(sum(getattr(self, name) * lookup[name] for name in self._COMPONENTS), 1)

    def to_dict(self) -> dict[str, Any]:
        note_lookup = dict(self.notes)
        return {
            "composite": self.composite,
            "components": [
                {
                    "key": name,
                    "value": round(getattr(self, name), 1),
                    "weight": dict(self.weights)[name],
                    "note": note_lookup.get(name),
                }
                for name in self._COMPONENTS
            ],
        }


# ---------------------------------------------------------------------------
# The private half — requirement 6 and 8
# ---------------------------------------------------------------------------
class FeedbackKind(str, Enum):
    """What a person learned. Recorded, counted, and acted on by nobody but them.

    Deliberately not wired into scoring. A rule that quietly re-weighted itself
    on five dismissals would be a model nobody trained, evaluated or can turn
    off; the honest thing at this scale is to count the outcomes and put them in
    front of the person who entered them.
    """

    DISMISSED = "dismissed"
    FALSE_SIGNAL = "false_signal"
    CONTACTED_NO_INTEREST = "contacted_no_interest"
    PROGRESSED = "progressed"
    WON = "won"
    LOST = "lost"


@dataclass(frozen=True)
class Feedback:
    kind: FeedbackKind
    recorded_on: date
    reason: str
    by: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind.value,
            "recorded_on": self.recorded_on.isoformat(),
            "reason": self.reason,
            "by": self.by,
        }


@dataclass(frozen=True)
class AuditEntry:
    """One recorded change. Append-only by convention — the file is the log."""

    on: date
    by: str
    what: str
    detail: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "on": self.on.isoformat(),
            "by": self.by,
            "what": self.what,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class WorkflowRecord:
    """Everything a person typed. Never serialised into a public artifact.

    This is the whole private surface, in one object, so the privacy boundary is
    a type rather than a habit. ``Opportunity.to_dict(AUDIENCE_PUBLIC)`` does not
    reach it, the public page builder never receives it, and
    ``tests/test_opportunities_page.py`` renders the public page against a
    workflow full of distinctive strings and greps the HTML for every one.
    """

    opportunity_id: str
    status: OpportunityStatus = OpportunityStatus.DETECTED
    owner: str | None = None
    notes: tuple[str, ...] = ()
    contacted_on: date | None = None
    next_action: str | None = None
    next_action_due: date | None = None
    counterparty_named: str | None = None
    feedback: tuple[Feedback, ...] = ()
    audit: tuple[AuditEntry, ...] = ()
    source_file: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "opportunity_id": self.opportunity_id,
            "status": self.status.value,
            "owner": self.owner,
            "notes": list(self.notes),
            "contacted_on": self.contacted_on.isoformat() if self.contacted_on else None,
            "next_action": self.next_action,
            "next_action_due": (
                self.next_action_due.isoformat() if self.next_action_due else None
            ),
            "counterparty_named": self.counterparty_named,
            "feedback": [item.to_dict() for item in self.feedback],
            "audit": [item.to_dict() for item in self.audit],
            "source_file": self.source_file,
        }


# ---------------------------------------------------------------------------
# Identity — requirement 7
# ---------------------------------------------------------------------------
def identity_key(
    *,
    rule_id: str,
    product: str,
    origin_key: str | None,
    destination_key: str | None,
    window_start: date | None,
) -> str:
    """What makes two detections the same opportunity.

    Deliberately excludes the numbers. The same lane, product, window and rule
    on two consecutive days is *one* opportunity that moved, not two — and an
    identity that included the landed advantage would mint a fresh id every time
    the market ticked, destroying the first-detected date, the workflow link and
    the expiry clock in one go.
    """
    parts = [
        rule_id,
        product.strip().lower(),
        (origin_key or "-").strip().lower(),
        (destination_key or "-").strip().lower(),
        window_start.isoformat() if window_start else "-",
    ]
    return "|".join(parts)


def opportunity_id(key: str, *, first_detected: date) -> str:
    """A stable, human-quotable id: ``OPP-20260818-3f9c2a``.

    The date is the *first* detection, not today's run. An id that moved with
    the run date would be a different id every morning, which is the same bug as
    an identity that includes the price.
    """
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:6]
    return f"OPP-{first_detected.strftime('%Y%m%d')}-{digest}"


# ---------------------------------------------------------------------------
# The object itself
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Opportunity:
    """Who might buy or sell what, where, when, why now, how sure, and what next.

    Construction is checked rather than trusted, because every one of these
    invariants has a failure mode that renders as a confident, wrong screen:

    * a hard blocker with an ``ACTIONABLE`` rung is a trade that cannot be done
      presented as one;
    * a detector-produced rung above ``ACTIONABLE`` is market data claiming a
      person did something;
    * a status past ``detected`` with no workflow record is the same claim by
      another route;
    * economics with a total but no volume is an invented cargo size.
    """

    opportunity_id: str
    identity: str
    rule_id: str
    rule_label: str
    ladder: Ladder
    status: OpportunityStatus

    product: str
    grade: Grade
    origin: Port | None
    destination: Port | None
    incoterm: Incoterm | None
    shipment_window: ShipmentWindow | None

    why_now: str
    signals: tuple[MarketSignal, ...]
    evidence: tuple[Evidence, ...]
    confidence: Confidence

    first_detected_on: date
    detected_on: date
    expires_on: date

    sellers: tuple[Counterparty, ...] = ()
    buyers: tuple[Counterparty, ...] = ()
    facilities: tuple[Counterparty, ...] = ()

    volume: Volume | None = None
    economics: Economics | None = None
    dislocation: Dislocation | None = None

    blockers: Blockers = ()
    missing_information: tuple[str, ...] = ()
    suggested_next_action: str = ""
    related_ids: tuple[str, ...] = ()

    #: The private half. ``None`` on every detector-produced opportunity; filled
    #: only by ``workflow.attach``. See :data:`PRIVATE_FIELD_NAMES`.
    workflow: WorkflowRecord | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.signals:
            raise OpportunityError(
                f"{self.opportunity_id}: an opportunity with no signal is an assertion"
            )
        if not self.evidence:
            raise OpportunityError(f"{self.opportunity_id}: no evidence")
        if self.expires_on < self.evidence_as_of():
            raise OpportunityError(
                f"{self.opportunity_id}: expires ({self.expires_on}) before its own oldest "
                f"evidence was observed ({self.evidence_as_of()}) — that is arithmetic, not data"
            )
        # An opportunity whose expiry is already behind `detected_on` is NOT an
        # error: it means the newest data a rule could find was already past its
        # own layer's budget when the rule ran. That is a real and common state
        # — a weekly source three weeks dark — and the honest handling is to
        # build it and let `registry.prune_expired` mark it expired, not to
        # crash and leave the screen looking as though the rule found nothing.
        if self.first_detected_on > self.detected_on:
            raise OpportunityError(
                f"{self.opportunity_id}: first detected after this detection"
            )
        if (
            self.workflow is None
            and self.has_hard_blocker
            and self.ladder.rank >= Ladder.ACTIONABLE.rank
        ):
            # Only a *detector* is held to this. Once a person has taken the row
            # up, the rung describes what they did, and a trader phoning a
            # counterparty about a policy-blocked lane is doing something
            # perfectly reasonable that this software has no business refusing
            # to record.
            codes = sorted({b.code.value for b in self.blockers if b.is_hard})
            raise OpportunityError(
                f"{self.opportunity_id}: {codes} are hard blockers, so this cannot be "
                f"{self.ladder.value} — a trade that cannot be worked must not be "
                "rendered beside ones that can"
            )
        if self.workflow is None:
            if self.ladder not in DETECTABLE_RUNGS:
                raise OpportunityError(
                    f"{self.opportunity_id}: {self.ladder.value} is not a rung a detector "
                    "can reach — proposed trades and completed business come only from "
                    "the local workflow file"
                )
            if self.status not in DETECTOR_STATUSES:
                raise OpportunityError(
                    f"{self.opportunity_id}: status {self.status.value!r} asserts that a "
                    "person did something, with no workflow record to back it"
                )
        elif self.workflow.opportunity_id != self.opportunity_id:
            raise OpportunityError(
                f"{self.opportunity_id}: workflow record belongs to "
                f"{self.workflow.opportunity_id}"
            )
        if self.economics is not None:
            has_total = self.economics.total_low_usd is not None
            if has_total and self.volume is None:
                raise OpportunityError(
                    f"{self.opportunity_id}: a total value with no volume estimate is an "
                    "invented cargo size"
                )
        if self.ladder is Ladder.ACTIONABLE and not self.suggested_next_action.strip():
            raise OpportunityError(
                f"{self.opportunity_id}: an actionable opportunity must say what to do next"
            )

    # -- derived ---------------------------------------------------------
    @property
    def has_hard_blocker(self) -> bool:
        return any(blocker.is_hard for blocker in self.blockers)

    @property
    def is_terminal(self) -> bool:
        return self.status in TERMINAL_STATUSES

    def is_expired(self, today: date) -> bool:
        return today > self.expires_on

    def days_remaining(self, today: date) -> int:
        return (self.expires_on - today).days

    def age_days(self, today: date) -> int:
        return (today - self.first_detected_on).days

    @property
    def lane(self) -> str:
        origin = self.origin.country_iso if self.origin else "—"
        destination = self.destination.country_iso if self.destination else "—"
        return f"{origin}→{destination}"

    @property
    def is_public_safe(self) -> bool:
        """Whether this may be rendered into the public artifact at all.

        A detector-produced opportunity always is. One carrying a workflow
        record never is, whatever its status: the moment a person has touched
        it, the fact that it is being worked is itself commercial information.
        """
        return self.workflow is None

    def worst_evidence_freshness(self, today: date) -> Freshness:
        states = [item.freshness(today) for item in self.evidence]
        return Freshness.STALE if Freshness.STALE in states else Freshness.CURRENT

    def evidence_as_of(self) -> date:
        return min(item.observed_on for item in self.evidence)

    # -- serialisation ---------------------------------------------------
    def to_dict(self, *, audience: str = AUDIENCE_PUBLIC, today: date | None = None) -> dict[str, Any]:
        """Plain data. ``audience`` decides whether the private half exists at all.

        Not a filter over one payload: the public branch never builds the
        private keys, so a template, a JSON dump or a debug print of the public
        dict cannot reach them by accident.
        """
        if audience not in (AUDIENCE_PUBLIC, AUDIENCE_PRIVATE):
            raise OpportunityError(f"unknown audience {audience!r}")
        payload: dict[str, Any] = {
            "opportunity_id": self.opportunity_id,
            "identity": self.identity,
            "rule_id": self.rule_id,
            "rule_label": self.rule_label,
            "ladder": self.ladder.value,
            "ladder_meaning": self.ladder.meaning,
            "status": self.status.value,
            "product": self.product,
            "grade": self.grade.to_dict(),
            "origin": self.origin.to_dict() if self.origin else None,
            "destination": self.destination.to_dict() if self.destination else None,
            "lane": self.lane,
            "incoterm": self.incoterm.value if self.incoterm else None,
            "shipment_window": (
                self.shipment_window.to_dict() if self.shipment_window else None
            ),
            "shipment_window_label": (
                self.shipment_window.describe() if self.shipment_window else None
            ),
            "why_now": self.why_now,
            "signals": [signal.to_dict(today) for signal in self.signals],
            "evidence": [item.to_dict(today) for item in self.evidence],
            "confidence": self.confidence.value,
            "first_detected_on": self.first_detected_on.isoformat(),
            "detected_on": self.detected_on.isoformat(),
            "expires_on": self.expires_on.isoformat(),
            "evidence_as_of": self.evidence_as_of().isoformat(),
            "days_remaining": self.days_remaining(today) if today else None,
            "age_days": self.age_days(today) if today else None,
            "freshness": (
                self.worst_evidence_freshness(today).value if today else None
            ),
            "sellers": [party.to_dict(today) for party in self.sellers],
            "buyers": [party.to_dict(today) for party in self.buyers],
            "facilities": [party.to_dict(today) for party in self.facilities],
            "volume": self.volume.to_dict() if self.volume else None,
            "economics": self.economics.to_dict() if self.economics else None,
            "dislocation": self.dislocation.to_dict() if self.dislocation else None,
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "has_hard_blocker": self.has_hard_blocker,
            "missing_information": list(self.missing_information),
            "suggested_next_action": self.suggested_next_action,
            "related_ids": list(self.related_ids),
        }
        if audience == AUDIENCE_PRIVATE:
            payload["workflow"] = self.workflow.to_dict() if self.workflow else None
        return payload
