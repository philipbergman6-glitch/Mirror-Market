"""The vocabulary of the trader validation trial (Phase 5).

Standard library only: persistence lives in ``records.py``, the git and SQLite
reads in ``release.py``, presentation in ``app/``. Same separation
``analysis/opportunities/domain.py`` keeps, for the same reason — a vocabulary
that can read a database grows a database's opinions.

Five ideas carry this module, and each exists because getting it wrong produces
a *believable* trial result rather than a crash. A wrong number on a dashboard
gets caught by the trader looking at it. A wrong number in the evidence that
decides whether the product is trustworthy gets caught by nobody.

**A record is a measurement, not a diary.** Every field a metric reads is typed
and closed-set. ``issues`` is one list of :class:`Issue`, each carrying an
:class:`IssueClass` from a fixed set, rather than six free-text fields for
"missing data", "stale results", "false alerts" and so on — six fields can
disagree with each other about the same event, and the metrics would have to
pick a winner.

**An abandoned session is data.** A task the trader gave up on is the single
most informative record in the trial, so :class:`SessionRecord` refuses to be
built as abandoned or blocked without at least one issue saying why. The
failure mode this prevents is a trial that reports 100% completion because the
sessions that failed were never written down.

**An external lookup must say what we could not answer.** The whole trial
measures displacement of an outside tool, and "checked Bloomberg" measures
nothing. :class:`ExternalLookup` requires ``unanswered_question`` — the thing
Mirror Market did not have — because that field, aggregated, *is* the product
backlog.

**Every result is stamped with what produced it.** :class:`ReleaseStamp` pins
the code revision and a fingerprint of the data the trader was actually looking
at. Without it, "the crush number was wrong on Tuesday" is unreproducible by
Thursday, and the finding has to be taken on trust — which is precisely what
this phase exists not to do.

**Private and aggregate are different objects, not different templates.**
A trader's identity, their decision, their notes and their evidence live on the
record and are never serialised into anything shareable.
:meth:`SessionRecord.to_dict` with ``audience=AUDIENCE_AGGREGATE`` does not
build those keys at all — see ``sanitize.py``, which pins it.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from enum import Enum
from typing import Any

__all__ = [
    "AUDIENCE_AGGREGATE",
    "AUDIENCE_PRIVATE",
    "CORRECTNESS_ISSUES",
    "PRIVATE_FIELD_NAMES",
    "DayObservation",
    "ExternalLookup",
    "ExternalTool",
    "Issue",
    "IssueClass",
    "Outcome",
    "ReleaseStamp",
    "SessionRecord",
    "Severity",
    "TaskId",
    "TrialError",
    "session_id",
    "session_identity_key",
]


class TrialError(ValueError):
    """A trial record said something that cannot be true. Raised, never rendered.

    A malformed session is not a session with a gap in it — it is a measurement
    whose meaning is unknown, and averaging it in would corrupt the only
    evidence this phase produces.
    """


# ---------------------------------------------------------------------------
# Audiences — the privacy boundary, declared before anything uses it
# ---------------------------------------------------------------------------

#: The full record, including everything a trader typed. Never leaves the
#: machine it was written on.
AUDIENCE_PRIVATE = "private"

#: The shareable projection: counts, durations, classifications and rates. No
#: identity, no decision, no notes, no counterparty, no free text of any kind.
AUDIENCE_AGGREGATE = "aggregate"

#: Field names that carry a human's own words or identity. ``sanitize.py``
#: asserts that none of them appears in an aggregate payload, at any depth.
PRIVATE_FIELD_NAMES = frozenset({
    "trader",
    "decision",
    "notes",
    "evidence",
    "question",
    "answer",
    "unanswered_question",
    # Named exactly, not as the bare "detail" this once was. A generic name
    # denies every field that happens to end up called that — it caught
    # Metric.detail, a per-task breakdown of numbers this project computed — and
    # a guard that fires on public data gets loosened, which is how the guard
    # stops guarding. The private field is the one that names a trader's tool.
    "tool_detail",
    "counterparty",
    "owner",
})


# ---------------------------------------------------------------------------
# The ten recurring tasks — requirement "two or more traders, these tasks"
#
# This enum is the single source of truth for the protocol document as well as
# for the record store: docs/trial/PROTOCOL.md is generated from it, so a task
# cannot be described one way in the instructions and measured another way in
# the metrics.
# ---------------------------------------------------------------------------
class TaskId(str, Enum):
    """A recurring job a soy trader actually does, framed as a decision.

    Each member names a task the trial runs repeatedly across the window. They
    are decisions rather than features on purpose: "use the origins page" is not
    a task, it is an instruction, and a trial that instructs where to look
    cannot measure whether the trader would have gone there.
    """

    MORNING_BRIEF = "morning_brief"
    ORIGIN_COMPARISON = "origin_comparison"
    CRUSH_HEDGE = "crush_hedge"
    WASDE_EVENT = "wasde_event"
    CHINA_RECONCILIATION = "china_reconciliation"
    WEATHER_RESPONSE = "weather_response"
    COUNTERPARTY_ID = "counterparty_id"
    FAILURE_DRILL = "failure_drill"
    PRICE_AUDIT = "price_audit"
    TICKET_REVIEW = "ticket_review"

    @property
    def label(self) -> str:
        """The task's name in the protocol.

        Named ``label`` and not ``title`` because ``TaskId`` subclasses ``str``,
        where ``title`` is already a method. A property of that name would shadow
        it — working fine everywhere until something called ``task.title()`` and
        got a ``TypeError`` from an object that plainly has a ``title``.
        """
        return _TASK_SPECS[self]["title"]

    @property
    def decision_question(self) -> str:
        """The question the trader must answer. Not "what to look at"."""
        return _TASK_SPECS[self]["question"]

    @property
    def success_criteria(self) -> str:
        """What counts as having completed it. Read aloud before the session."""
        return _TASK_SPECS[self]["success"]

    @property
    def target_minutes(self) -> int:
        """The time this task takes on the tools the trader uses today.

        The baseline is stated per task rather than measured, and it is stated
        as a *target*, not a benchmark: this project has no instrumented
        Bloomberg session to compare against, and pretending otherwise would
        manufacture a speed-up. What the trial measures against it is whether
        the median lands inside the same order of magnitude.
        """
        return int(_TASK_SPECS[self]["target_minutes"])

    @property
    def cadence(self) -> str:
        """How often the protocol asks for this task across the window."""
        return _TASK_SPECS[self]["cadence"]


_TASK_SPECS: dict[TaskId, dict[str, Any]] = {
    TaskId.MORNING_BRIEF: {
        "title": "Pre-open / morning brief",
        "question": (
            "What moved overnight, what repriced, what has not printed yet, and "
            "which of those changes anything I hold or intend to do today?"
        ),
        "success": (
            "The trader can state the overnight move in the soy complex, name at "
            "least one market that has NOT yet repriced, and say whether the day's "
            "plan changes — without opening a second tool first."
        ),
        "target_minutes": 10,
        "cadence": "every trading day",
    },
    TaskId.ORIGIN_COMPARISON: {
        "title": "Origin comparison for a real shipment window",
        "question": (
            "For a named destination and a real shipment window, which origin is "
            "cheapest landed, by how much, and what would have to be true for that "
            "to flip?"
        ),
        "success": (
            "A ranked landed cost with every cost component visible, the shipment "
            "window stated, and the trader able to name the one input that most "
            "moves the ranking."
        ),
        "target_minutes": 20,
        "cadence": "at least twice a week",
    },
    TaskId.CRUSH_HEDGE: {
        "title": "Physical crush and hedge scenario",
        "question": (
            "Given a physical crush position, what is the board margin, what hedge "
            "would cover it, and what does the position lose under a stated shock?"
        ),
        "success": (
            "A sized hedge in whole contracts against a stated tonnage, a named "
            "contract month with its expiry and first notice day, and a shocked P&L "
            "the trader can reproduce from the numbers on the page."
        ),
        "target_minutes": 25,
        "cadence": "at least twice a week",
    },
    TaskId.WASDE_EVENT: {
        "title": "USDA / WASDE event response",
        "question": (
            "What did the report change versus the prior month, and is the board's "
            "reaction consistent with the revision?"
        ),
        "success": (
            "Month-over-month revisions for the US and world balance sheets, with "
            "stocks-to-use, read off the product within the session; the trader can "
            "say whether the move looks over- or under-done."
        ),
        "target_minutes": 20,
        "cadence": "each WASDE release day inside the window",
    },
    TaskId.CHINA_RECONCILIATION: {
        "title": "China demand and shipment reconciliation",
        "question": (
            "Do committed sales, actual inspections and the Dalian board agree "
            "about Chinese demand, and where do they disagree?"
        ),
        "success": (
            "Outstanding sales and shipped-to-date for China stated with their own "
            "report dates, set beside the DCE import parity, with any disagreement "
            "named rather than averaged away."
        ),
        "target_minutes": 15,
        "cadence": "weekly, on the export-sales release",
    },
    TaskId.WEATHER_RESPONSE: {
        "title": "Weather-risk response",
        "question": (
            "Which growing region has moved outside its own normal, and does the "
            "board already carry a premium for it?"
        ),
        "success": (
            "A named region with an anomaly stated against its own history, and an "
            "explicit judgement on whether price has already responded."
        ),
        "target_minutes": 10,
        "cadence": "at least twice a week",
    },
    TaskId.COUNTERPARTY_ID: {
        "title": "Counterparty / opportunity identification",
        "question": (
            "Is there a lane worth working today, who would be on the other side of "
            "it, and what is stopping it?"
        ),
        "success": (
            "A ranked lane with named candidate counterparties, its blockers stated, "
            "and the trader able to say whether it is workable today or merely worth "
            "a phone call."
        ),
        "target_minutes": 15,
        "cadence": "at least twice a week",
    },
    TaskId.FAILURE_DRILL: {
        "title": "Deliberate data-source and deployment failure drill",
        "question": (
            "When a source dies, a payload freezes, a page fails to build or a "
            "deploy fails, does the product say so — and is the last good edition "
            "still what the trader sees?"
        ),
        "success": (
            "The trader, shown a degraded edition without being told which drill "
            "ran, can name what is missing and say whether they would still trade "
            "off the page."
        ),
        "target_minutes": 15,
        "cadence": "five drills across the window, one per failure mode",
    },
    TaskId.PRICE_AUDIT: {
        "title": "Price and calculation audit",
        "question": (
            "Pick a displayed number at random: what exactly is it, where did it "
            "come from, and can the trader reproduce it?"
        ),
        "success": (
            "The number's product, venue, price type, currency, unit, contract or "
            "window, and observation date are all recoverable from the product, and "
            "any derived figure reproduces by hand from its stated inputs."
        ),
        "target_minutes": 15,
        "cadence": "at least three times a week, on a number chosen by the trader",
    },
    TaskId.TICKET_REVIEW: {
        "title": "Proposed hedge / trade-ticket review",
        "question": (
            "Is this proposed ticket one the trader would send to a broker, and if "
            "not, what is wrong with it?"
        ),
        "success": (
            "Every leg carries a named contract, a side, a quantity in whole "
            "contracts and a price basis; the trader states accept, amend or reject "
            "with a reason."
        ),
        "target_minutes": 10,
        "cadence": "at least twice a week",
    },
}


# ---------------------------------------------------------------------------
# Issue classification — requirement 4, as a closed set
# ---------------------------------------------------------------------------
class IssueClass(str, Enum):
    """What kind of wrong this was. Closed set: an unclassified issue is a bug.

    The set is deliberately split along the line that matters for the trial's
    own verdict. ``NUMERICAL_ERROR``, ``SEMANTIC_MISMATCH`` and ``STALE_DATA``
    mean the product told the trader something untrue, and they are the numerator
    of the decision-risk metric. ``MISSING_COVERAGE``, ``MISLEADING_UX`` and
    ``WORKFLOW_FRICTION`` mean it failed to help, which is a different and much
    less dangerous failure. Collapsing the two groups into "issues" would let a
    pile of missing features hide a single wrong price.
    """

    NUMERICAL_ERROR = "numerical_error"
    SEMANTIC_MISMATCH = "semantic_mismatch"
    STALE_DATA = "stale_data"
    MISSING_COVERAGE = "missing_coverage"
    MISLEADING_UX = "misleading_ux"
    WORKFLOW_FRICTION = "workflow_friction"
    FALSE_ALERT = "false_alert"
    MISSED_ALERT = "missed_alert"
    UPSTREAM_OUTAGE = "upstream_outage"
    REQUESTED_ENHANCEMENT = "requested_enhancement"

    @property
    def meaning(self) -> str:
        return _ISSUE_MEANING[self]

    @property
    def is_correctness(self) -> bool:
        """Did the product state something untrue?

        ``UPSTREAM_OUTAGE`` is deliberately excluded. A source dying is not this
        product being wrong — it is this product being *right* about not having
        the number, provided it said so. If it did not say so, the issue is a
        ``STALE_DATA`` or ``MISLEADING_UX`` one and is classified as such.
        """
        return self in CORRECTNESS_ISSUES


CORRECTNESS_ISSUES = frozenset({
    IssueClass.NUMERICAL_ERROR,
    IssueClass.SEMANTIC_MISMATCH,
    IssueClass.STALE_DATA,
})

_ISSUE_MEANING = {
    IssueClass.NUMERICAL_ERROR: (
        "A displayed number is arithmetically wrong, or disagrees with the source "
        "it claims to come from."
    ),
    IssueClass.SEMANTIC_MISMATCH: (
        "The number is right but is not what it is labelled as — a farmgate price "
        "shown as FOB, a last trade shown as a settlement, a bid shown as a price."
    ),
    IssueClass.STALE_DATA: (
        "The value is past its own source's cadence and the product did not say so. "
        "A value labelled stale is not this; that is the product working."
    ),
    IssueClass.MISSING_COVERAGE: (
        "The question is a reasonable one for this product and it has no answer at "
        "all. The most common issue class, and the one that becomes the roadmap."
    ),
    IssueClass.MISLEADING_UX: (
        "The number is correct and the trader read it wrongly anyway. Treated as a "
        "product defect, not a user error."
    ),
    IssueClass.WORKFLOW_FRICTION: (
        "The answer was there and took too many steps, too many pages, or a manual "
        "calculation to reach."
    ),
    IssueClass.FALSE_ALERT: (
        "An alert or signal fired and the trader, having checked, judged there was "
        "nothing there."
    ),
    IssueClass.MISSED_ALERT: (
        "Something happened that this product should have flagged and did not. Only "
        "recordable against a stated expectation, never in hindsight alone."
    ),
    IssueClass.UPSTREAM_OUTAGE: (
        "A source was genuinely down or dark. Recorded to measure availability, and "
        "explicitly not counted as this product being wrong."
    ),
    IssueClass.REQUESTED_ENHANCEMENT: (
        "Nothing is broken; the trader wants something that does not exist. Kept "
        "separate from missing coverage, which is a gap in what is already claimed."
    ),
}


class Severity(str, Enum):
    """How much a finding matters, judged by the decision it affects.

    Severity is about the *decision*, never about the effort to fix. A one-line
    label fix on a number a trader would hedge off is a blocker; a missing page
    nobody needed is minor.
    """

    BLOCKER = "blocker"
    MAJOR = "major"
    MINOR = "minor"

    @property
    def rank(self) -> int:
        return {Severity.BLOCKER: 0, Severity.MAJOR: 1, Severity.MINOR: 2}[self]

    @property
    def meaning(self) -> str:
        return {
            Severity.BLOCKER: (
                "A trader could place or size a real trade wrongly off this. Stop "
                "the trial for this surface until it is fixed."
            ),
            Severity.MAJOR: (
                "The task cannot be completed in the product, or the answer needs an "
                "external check every time. Fix inside the window."
            ),
            Severity.MINOR: (
                "Friction, polish, or a gap the trader routed around without risk."
            ),
        }[self]


class Outcome(str, Enum):
    """How the session ended.

    ``ABANDONED`` and ``BLOCKED`` differ in where the wall was: abandoned means
    the trader stopped (too slow, not worth it), blocked means the product could
    not answer at all. Both require an issue on the record saying which.
    """

    COMPLETED = "completed"
    ABANDONED = "abandoned"
    BLOCKED = "blocked"

    @property
    def is_complete(self) -> bool:
        return self is Outcome.COMPLETED


class ExternalTool(str, Enum):
    """Where the trader went instead. Closed set, with ``OTHER`` named in text."""

    BLOOMBERG = "bloomberg"
    BROKER = "broker"
    SPREADSHEET = "spreadsheet"
    EXCHANGE = "exchange"
    REFINITIV = "refinitiv"
    NEWS = "news"
    COLLEAGUE = "colleague"
    OTHER = "other"


# ---------------------------------------------------------------------------
# Release identity — requirement 6
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ReleaseStamp:
    """What code, and what data, produced the thing the trader was looking at.

    Requirement 6 in one object. Three parts, and all three are needed:

    ``code_revision``   the git commit the site was generated from, suffixed
                        ``-dirty`` when the working tree was not clean. A dirty
                        stamp is not an error — a trial run against a local fix
                        is legitimate — but it is recorded, because it is the
                        difference between a reproducible finding and a story.
    ``data_fingerprint`` a hash over every layer's last-success date and status.
                        Two editions built from the same commit but different
                        data are different editions, and the commit alone cannot
                        tell them apart.
    ``edition_id``      the trust-layer edition, where one exists. Optional, and
                        ``None`` is honest: the v1 static site is not yet built
                        through ``trust.edition``, so most stamps carry a commit
                        and a fingerprint and say so.

    ``captured_at`` is when the stamp was taken, not when the data was observed.
    Those are different, and conflating them is the mistake ``forward_curve``'s
    ``observation_date`` / ``fetched_date`` split exists to prevent.
    """

    code_revision: str
    data_fingerprint: str
    captured_at: datetime
    edition_id: str | None = None
    dirty: bool = False
    layer_count: int | None = None

    def __post_init__(self) -> None:
        if not self.code_revision.strip():
            raise TrialError(
                "a release stamp needs a code revision — an unreproducible finding "
                "is an opinion, and this phase produces evidence"
            )
        if not self.data_fingerprint.strip():
            raise TrialError("a release stamp needs a data fingerprint")
        if self.captured_at.tzinfo is None:
            raise TrialError("captured_at must be timezone-aware")
        if self.edition_id is not None and not self.edition_id.strip():
            raise TrialError("edition_id must be a real id or None, never an empty string")

    @property
    def short_code(self) -> str:
        """First 12 of the commit, plus the dirty marker. For display only."""
        base = self.code_revision.split("-dirty")[0][:12]
        return f"{base}-dirty" if self.dirty else base

    @property
    def is_reproducible(self) -> bool:
        """Can this exact result be rebuilt?

        A dirty tree cannot: the commit does not describe the code that ran. The
        finding is still real and still actionable — it just has to be reproduced
        by hand rather than by checkout.
        """
        return not self.dirty

    def matches(self, other: ReleaseStamp) -> bool:
        """Same code and same data. Timestamps and edition ids do not count."""
        return (
            self.code_revision == other.code_revision
            and self.data_fingerprint == other.data_fingerprint
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "code_revision": self.code_revision,
            "short_code": self.short_code,
            "data_fingerprint": self.data_fingerprint,
            "captured_at": self.captured_at.isoformat(),
            "edition_id": self.edition_id,
            "dirty": self.dirty,
            "layer_count": self.layer_count,
            "is_reproducible": self.is_reproducible,
        }


# ---------------------------------------------------------------------------
# The pieces of a session
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ExternalLookup:
    """One trip to a tool that is not this one.

    ``unanswered_question`` is required, and it is the point of the whole class.
    "Consulted Bloomberg" is a tally mark; "needed the Dec/Mar ZL spread and this
    product only carries the front month" is a backlog item with a decision
    attached to it. Requirement 3 asks for what Mirror Market could not answer,
    so the field that records it cannot be optional.

    ``answer_found`` is allowed to be ``False``: a lookup that failed elsewhere
    too is evidence the question is hard, not evidence of a gap here.
    """

    tool: ExternalTool
    unanswered_question: str
    answer_found: bool
    minutes: float | None = None
    tool_detail: str | None = None

    def __post_init__(self) -> None:
        if not self.unanswered_question.strip():
            raise TrialError(
                "an external lookup must state what this product could not answer — "
                "a bare tally of terminal visits measures nothing the trial needs"
            )
        if self.tool is ExternalTool.OTHER and not (self.tool_detail or "").strip():
            raise TrialError("tool 'other' must name the tool in tool_detail")
        if self.minutes is not None and self.minutes < 0:
            raise TrialError(f"external lookup minutes cannot be negative, got {self.minutes}")

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
            raise TrialError(f"unknown audience {audience!r}")
        payload: dict[str, Any] = {
            "tool": self.tool.value,
            "answer_found": self.answer_found,
            "minutes": self.minutes,
        }
        if audience == AUDIENCE_PRIVATE:
            payload["unanswered_question"] = self.unanswered_question
            payload["tool_detail"] = self.tool_detail
        return payload


@dataclass(frozen=True)
class Issue:
    """Something that went wrong, classified, evidenced, and tied to a decision.

    Three required fields beyond the classification, each because a finding
    without it cannot be acted on:

    ``summary``           what happened, in one line.
    ``evidence``          how to see it again — a page, a number, a screenshot
                          path, a query. A finding nobody can reproduce cannot be
                          fixed and cannot be closed.
    ``affected_decision`` what the trader would have got wrong. This is what
                          sets the severity, and a finding that affects no
                          decision is a note, not an issue.

    Same shape, and the same reasoning, as ``opportunities.domain.Blocker``
    requiring a remedy: the field that makes the record useful is the one the
    writer is most tempted to leave out.
    """

    classification: IssueClass
    summary: str
    evidence: str
    affected_decision: str
    severity: Severity = Severity.MINOR
    page: str | None = None
    expected: str | None = None
    observed: str | None = None

    def __post_init__(self) -> None:
        for name, value in (
            ("summary", self.summary),
            ("evidence", self.evidence),
            ("affected_decision", self.affected_decision),
        ):
            if not value.strip():
                raise TrialError(
                    f"issue {self.classification.value} needs a non-empty {name} — an "
                    "unevidenced finding cannot be reproduced, prioritised or closed"
                )
        if self.classification.is_correctness and self.severity is Severity.MINOR:
            raise TrialError(
                f"{self.classification.value} is a correctness issue and cannot be minor — "
                "the product stated something untrue, and the trial's whole risk half "
                "is counted from these"
            )

    @property
    def is_correctness(self) -> bool:
        return self.classification.is_correctness

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
            raise TrialError(f"unknown audience {audience!r}")
        payload: dict[str, Any] = {
            "classification": self.classification.value,
            "severity": self.severity.value,
            "is_correctness": self.is_correctness,
            "page": self.page,
        }
        if audience == AUDIENCE_PRIVATE:
            payload.update({
                "summary": self.summary,
                "evidence": self.evidence,
                "affected_decision": self.affected_decision,
                "expected": self.expected,
                "observed": self.observed,
            })
        return payload


# ---------------------------------------------------------------------------
# The session record — requirement 2
# ---------------------------------------------------------------------------
def session_identity_key(
    trading_day: date, trader: str, task: TaskId, started_at: datetime
) -> tuple[str, str, str, str]:
    """What makes two session records the same session.

    The start timestamp is in the key because a trader legitimately runs the
    same task twice in one day — a second morning brief after a WASDE release is
    a different session, not a correction of the first.
    """
    return (trading_day.isoformat(), trader.strip().lower(), task.value, started_at.isoformat())


def session_id(trading_day: date, trader: str, task: TaskId, started_at: datetime) -> str:
    """``TS-YYYYMMDD-<8 hex>``. Deterministic, so a re-read is the same session.

    The trader id is hashed into it rather than printed: a session id ends up in
    backlog items and review output, and those circulate further than the record
    store does.
    """
    key = "|".join(session_identity_key(trading_day, trader, task, started_at))
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]
    return f"TS-{trading_day.strftime('%Y%m%d')}-{digest}"


@dataclass(frozen=True)
class SessionRecord:
    """One trader, one task, one sitting. The atom of the whole trial.

    Every field requirement 2 asks for is here and typed. Three of them are
    derived rather than stored, because a stored duration can disagree with its
    own timestamps and a stored "had a stale result" can disagree with its own
    issue list.

    The validation rules are all of the same kind: they refuse records whose
    parts contradict each other, rather than records that are merely incomplete.
    An abandoned session with no issue is contradictory — something stopped the
    trader and the record does not say what. A session marked ``would_act`` that
    never completed is contradictory — there was no output to act on.
    """

    trader: str
    task: TaskId
    trading_day: date
    started_at: datetime
    ended_at: datetime
    outcome: Outcome
    confidence: int
    would_act: bool
    release: ReleaseStamp
    decision: str = ""
    pages_used: tuple[str, ...] = ()
    external_lookups: tuple[ExternalLookup, ...] = ()
    issues: tuple[Issue, ...] = ()
    notes: tuple[str, ...] = ()
    evidence: tuple[str, ...] = ()
    protocol_version: str = "1.0.0"
    source_file: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if not self.trader.strip():
            raise TrialError("a session needs a trader id (a pseudonym — never a real name)")
        if self.started_at.tzinfo is None or self.ended_at.tzinfo is None:
            raise TrialError(
                f"{self.trader}/{self.task.value}: start and end must be timezone-aware — "
                "a trial spanning Chicago, London and Singapore cannot subtract naive clocks"
            )
        if self.ended_at <= self.started_at:
            raise TrialError(
                f"{self.trader}/{self.task.value}: ended_at {self.ended_at.isoformat()} is not "
                f"after started_at {self.started_at.isoformat()}"
            )
        if self.confidence not in (1, 2, 3, 4, 5):
            raise TrialError(
                f"confidence must be 1-5 on the protocol's scale, got {self.confidence!r}"
            )
        if self.outcome.is_complete and not self.decision.strip():
            raise TrialError(
                f"{self.trader}/{self.task.value}: a completed session must record the decision "
                "or output it reached — completion with no output is not completion"
            )
        if not self.outcome.is_complete and not self.issues:
            raise TrialError(
                f"{self.trader}/{self.task.value}: an {self.outcome.value} session must carry at "
                "least one issue saying what stopped it. A trial that records only its "
                "successes measures nothing."
            )
        if self.would_act and not self.outcome.is_complete:
            raise TrialError(
                f"{self.trader}/{self.task.value}: would_act cannot be true on an "
                f"{self.outcome.value} session — there was no output to act on"
            )

    # -- derived ------------------------------------------------------------
    @property
    def session_id(self) -> str:
        return session_id(self.trading_day, self.trader, self.task, self.started_at)

    @property
    def duration_minutes(self) -> float:
        return (self.ended_at - self.started_at).total_seconds() / 60.0

    @property
    def external_lookup_count(self) -> int:
        return len(self.external_lookups)

    @property
    def correctness_issues(self) -> tuple[Issue, ...]:
        """Issues where the product stated something untrue. The risk numerator."""
        return tuple(issue for issue in self.issues if issue.is_correctness)

    @property
    def had_wrong_or_stale(self) -> bool:
        return bool(self.correctness_issues)

    def issues_of(self, classification: IssueClass) -> tuple[Issue, ...]:
        return tuple(issue for issue in self.issues if issue.classification is classification)

    @property
    def false_alerts(self) -> int:
        return len(self.issues_of(IssueClass.FALSE_ALERT))

    @property
    def missed_alerts(self) -> int:
        return len(self.issues_of(IssueClass.MISSED_ALERT))

    @property
    def missing_data(self) -> tuple[Issue, ...]:
        return self.issues_of(IssueClass.MISSING_COVERAGE)

    @property
    def upstream_outages(self) -> tuple[Issue, ...]:
        return self.issues_of(IssueClass.UPSTREAM_OUTAGE)

    @property
    def worst_severity(self) -> Severity | None:
        return min((issue.severity for issue in self.issues), key=lambda s: s.rank, default=None)

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        """Plain data. ``audience`` decides whether the private half exists at all.

        Not a filter over one payload: the aggregate branch never builds the
        private keys, so a JSON dump, a template or a debug print of an aggregate
        payload cannot reach them by accident. Same construction, and the same
        reason, as ``opportunities.domain.Opportunity.to_dict``.
        """
        if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
            raise TrialError(f"unknown audience {audience!r}")
        payload: dict[str, Any] = {
            "session_id": self.session_id,
            "task": self.task.value,
            "trading_day": self.trading_day.isoformat(),
            "outcome": self.outcome.value,
            "duration_minutes": round(self.duration_minutes, 1),
            "confidence": self.confidence,
            "would_act": self.would_act,
            "pages_used": list(self.pages_used),
            "external_lookup_count": self.external_lookup_count,
            "external_lookups": [
                lookup.to_dict(audience=audience) for lookup in self.external_lookups
            ],
            "issues": [issue.to_dict(audience=audience) for issue in self.issues],
            "had_wrong_or_stale": self.had_wrong_or_stale,
            "false_alerts": self.false_alerts,
            "missed_alerts": self.missed_alerts,
            "protocol_version": self.protocol_version,
            "release": self.release.to_dict(),
        }
        if audience == AUDIENCE_PRIVATE:
            payload.update({
                "trader": self.trader,
                "decision": self.decision,
                "notes": list(self.notes),
                "evidence": list(self.evidence),
                "started_at": self.started_at.isoformat(),
                "ended_at": self.ended_at.isoformat(),
                "source_file": self.source_file,
            })
        return payload


# ---------------------------------------------------------------------------
# The daily edition observation — the other half of requirement 5
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DayObservation:
    """What the product itself did on one trading day, independent of any session.

    Two of the eleven metrics — source availability and deployment reliability —
    are properties of the *product*, not of a trader's sitting. They cannot be
    derived from session records, because a day on which the site failed to
    deploy is precisely a day on which no trader logged a session, and a metric
    that only sees the days somebody worked would report perfect reliability
    through a week-long outage.

    They also cannot be derived after the fact. ``data_freshness`` is keyed by
    layer name and holds one row per layer, so it describes today and has no
    memory; the deploy workflow leaves its outcome in a GitHub Actions log, not
    in the repository. So the trial takes one observation per trading day, and
    ``scripts/trial.py day`` computes every field from the database and the built
    site rather than asking the operator to type them — the only typed field is
    ``note``.

    ``edition_published`` and ``edition_current`` are separate because they fail
    separately: a deploy can succeed and carry yesterday's data (the pipeline
    failed but the site built), and a deploy can fail while the data is perfect.
    Collapsing them would hide whichever half broke.
    """

    trading_day: date
    edition_published: bool
    edition_current: bool
    critical_layers_expected: int
    critical_layers_available: int
    release: ReleaseStamp
    degraded_layers: tuple[str, ...] = ()
    drill: str | None = None
    note: str = ""
    source_file: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.critical_layers_expected <= 0:
            raise TrialError(
                "critical_layers_expected must be positive — a day observed against no "
                "expected layers reports 100% availability of nothing"
            )
        if not 0 <= self.critical_layers_available <= self.critical_layers_expected:
            raise TrialError(
                f"{self.trading_day.isoformat()}: {self.critical_layers_available} available of "
                f"{self.critical_layers_expected} expected is not a share"
            )
        if self.edition_current and not self.edition_published:
            raise TrialError(
                f"{self.trading_day.isoformat()}: an edition cannot be current without having "
                "been published"
            )

    @property
    def deployment_ok(self) -> bool:
        """Did the trader see today's edition today? Both halves, or neither."""
        return self.edition_published and self.edition_current

    @property
    def source_availability(self) -> float:
        return self.critical_layers_available / self.critical_layers_expected

    @property
    def is_drill(self) -> bool:
        return self.drill is not None

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        """Nothing here is private — but the audience argument is kept anyway.

        Every ``to_dict`` in this package takes the same parameter so that a
        caller sanitising a mixed payload cannot be tripped by the one object
        that does not accept it. ``note`` is the single free-text field and is
        dropped on the aggregate path on principle: an operator writing "held
        back, waiting on Cargill" would not expect it in a shared report.
        """
        if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
            raise TrialError(f"unknown audience {audience!r}")
        payload: dict[str, Any] = {
            "trading_day": self.trading_day.isoformat(),
            "edition_published": self.edition_published,
            "edition_current": self.edition_current,
            "deployment_ok": self.deployment_ok,
            "critical_layers_expected": self.critical_layers_expected,
            "critical_layers_available": self.critical_layers_available,
            "source_availability": round(self.source_availability, 4),
            "degraded_layers": list(self.degraded_layers),
            "drill": self.drill,
            "release": self.release.to_dict(),
        }
        if audience == AUDIENCE_PRIVATE:
            payload["note"] = self.note
            payload["source_file"] = self.source_file
        return payload


def utc_now() -> datetime:
    """The one clock this package reads. Kept here so tests can point at it."""
    return datetime.now(timezone.utc)
