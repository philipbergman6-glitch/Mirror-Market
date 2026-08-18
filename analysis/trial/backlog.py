"""Validated findings become prioritised work, and nothing else does (Phase 5).

Requirement 7: a workflow turning validated findings into backlog items carrying
severity, evidence, affected decision, and acceptance criteria. Four things about
that sentence are load-bearing, and each is enforced here rather than left to
whoever runs the weekly review.

**"Validated" is a rule, not a mood.** A trader reporting one surprising number
on one afternoon is an observation. It becomes a backlog item when something
corroborates it: a second occurrence, a second trader, a blocker severity, or a
correctness class where being wrong once is already the whole problem.
:func:`draft_backlog` applies that rule and *keeps* the rejects, as
:class:`Observation`, with the reason they did not promote. Discarding them would
delete the evidence that the rule is too strict.

**Recurrence is counted, not summed.** The same complaint from two traders is one
item seen twice, so issues are grouped by a stable identity — classification,
page, and a normalised summary — and never by their free text alone. An item
seen four times on four days outranks a louder one seen once, which is the
ordering a desk actually wants and the one a raw severity sort will not give.

**Acceptance criteria are drafted, then owned.** Every class carries a template
phrased as a testable condition, and every drafted item is flagged
``criteria_are_drafted`` until a human replaces it. The flag exists because a
generated criterion reads exactly like a considered one, and shipping against an
unread acceptance criterion is how a fix closes a ticket without fixing anything.

**Publishing is a separate, deliberate act.** A finding's evidence is free text a
trader typed during a live session; it may name a cargo, a counterparty or a
position. So :func:`issue_body` refuses to render a public body until the owner
has cleared that specific item, and the aggregate projection carries counts and
classes with no free text at all. The private body is the full one and is written
where private things are written.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field, replace
from datetime import date
from typing import Any

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    Issue,
    IssueClass,
    ReleaseStamp,
    SessionRecord,
    Severity,
    TaskId,
    TrialError,
)

__all__ = [
    "BacklogItem",
    "BacklogSet",
    "Observation",
    "PROMOTION_RULES",
    "backlog_markdown",
    "draft_backlog",
    "finding_key",
    "issue_body",
    "suggested_acceptance_criteria",
]


#: Why an observation was promoted to the backlog. Recorded on the item so the
#: weekly review can show the rule that fired, and so a rule that never fires —
#: or one that fires for everything — is visible rather than inferred.
PROMOTION_RULES: dict[str, str] = {
    "blocker": "severity is a blocker: it stopped a trader completing the task",
    "correctness": (
        "a correctness class (wrong number, wrong meaning, stale data): being wrong "
        "once is the finding, so it needs no second occurrence"
    ),
    "recurrence": "seen more than once",
    "corroborated": "seen by more than one trader",
}

#: Acceptance-criteria templates, one per issue class. Each is phrased as a
#: condition someone can test against a built page, not as an intention — "the
#: figure matches the source to the stated precision" can be checked, "improve
#: accuracy" cannot.
_CRITERIA_TEMPLATES: dict[IssueClass, str] = {
    IssueClass.NUMERICAL_ERROR: (
        "The figure on {page} reproduces its source to the precision the page states, "
        "verified against the upstream value for the day in the evidence, and a test "
        "pins that day so the arithmetic cannot silently change back."
    ),
    IssueClass.SEMANTIC_MISMATCH: (
        "The label on {page} names what the number actually is (venue, unit, quote kind, "
        "and the session it was observed on), and the descriptor carries that label so "
        "every consumer of the same leg inherits it."
    ),
    IssueClass.STALE_DATA: (
        "{page} states the age of the figure and names the last known good date; a payload "
        "older than the layer's own LAYER_MAX_DATA_AGE_DAYS budget is graded stale rather "
        "than published as current."
    ),
    IssueClass.MISSING_COVERAGE: (
        "The series named in the evidence is either ingested as a graded layer, or {page} "
        "states in words that this project does not carry it and why. Silence is not an "
        "acceptable outcome."
    ),
    IssueClass.MISLEADING_UX: (
        "A trader who has not read this ticket reaches the correct reading of {page} "
        "unaided, confirmed by re-running the originating task with a second trader."
    ),
    IssueClass.WORKFLOW_FRICTION: (
        "The task completes without the step described in the evidence, and the median "
        "completion time for that task does not regress."
    ),
    IssueClass.FALSE_ALERT: (
        "The condition in the evidence no longer raises an alert, the conditions that "
        "should still raise one are pinned by a test, and the change is stated in the "
        "alert's own description."
    ),
    IssueClass.MISSED_ALERT: (
        "The condition in the evidence raises an alert at the severity a trader would act "
        "on, pinned by a test against that day's data."
    ),
    IssueClass.UPSTREAM_OUTAGE: (
        "The outage is visible on {page} as an outage — naming our failed ingest and the "
        "last good date — and is never rendered in the same words as a market that "
        "published nothing."
    ),
    IssueClass.REQUESTED_ENHANCEMENT: (
        "The behaviour described in the evidence is available on {page}, and the decision "
        "it supports can be completed without the external lookup that prompted it."
    ),
}


def _normalise(text: str) -> str:
    """Reduce a summary to its identity: lowercase, no punctuation, no digits.

    Digits go deliberately. "Paranagua FOB reads 421 not 418" and the same
    complaint the next day at different levels are one finding, and keeping the
    numbers in the key would file them as two.
    """
    lowered = re.sub(r"[^a-z\s]+", " ", text.lower())
    return " ".join(lowered.split())


def finding_key(issue: Issue) -> str:
    """Stable identity for a finding: ``(classification, page, normalised summary)``.

    Deliberately excludes severity and every number. Severity is a judgement two
    traders will make differently about one defect, and grouping on it would file
    the same bug twice at two ranks.
    """
    basis = "|".join((issue.classification.value, (issue.page or "").strip().lower(), _normalise(issue.summary)))
    return "BL-" + hashlib.sha256(basis.encode("utf-8")).hexdigest()[:10]


def suggested_acceptance_criteria(issue: Issue) -> str:
    """A testable acceptance criterion for this class of finding. A draft."""
    template = _CRITERIA_TEMPLATES.get(issue.classification)
    if template is None:  # pragma: no cover - every class carries a template
        raise TrialError(f"no acceptance-criteria template for {issue.classification.value!r}")
    return template.format(page=issue.page or "the affected page")


@dataclass(frozen=True)
class Observation:
    """A finding that did not meet the promotion rule, and why it did not.

    Kept rather than dropped. A trial whose observations vastly outnumber its
    backlog items is telling you the rule is too strict; one with no observations
    at all is telling you it is too loose. Neither is visible if the rejects are
    thrown away.
    """

    key: str
    classification: IssueClass
    summary: str
    occurrences: int
    trader_count: int
    reason_not_promoted: str

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "classification": self.classification.value,
            "occurrences": self.occurrences,
            "trader_count": self.trader_count,
            "reason_not_promoted": self.reason_not_promoted,
        }
        if audience == AUDIENCE_PRIVATE:
            payload["summary"] = self.summary
        return payload


@dataclass(frozen=True)
class BacklogItem:
    """One validated finding, ready to be worked.

    Carries the four things requirement 7 names — severity, evidence, affected
    decision, acceptance criteria — plus the provenance that makes them checkable:
    which sessions saw it, on which trading days, against which release stamps,
    and whether any of those stamps can be reproduced.
    """

    key: str
    classification: IssueClass
    severity: Severity
    summary: str
    affected_decision: str
    acceptance_criteria: str
    evidence: tuple[str, ...] = ()
    page: str | None = None
    expected: str | None = None
    observed: str | None = None
    tasks: tuple[TaskId, ...] = ()
    session_ids: tuple[str, ...] = ()
    trading_days: tuple[date, ...] = ()
    releases: tuple[ReleaseStamp, ...] = field(default=(), repr=False)
    trader_count: int = 1
    promotion_rules: tuple[str, ...] = ()
    criteria_are_drafted: bool = True
    cleared_for_public: bool = False

    def __post_init__(self) -> None:
        if not self.summary.strip():
            raise TrialError(f"{self.key}: a backlog item needs a summary")
        if not self.affected_decision.strip():
            raise TrialError(
                f"{self.key}: a backlog item needs the decision it affects — a defect with no "
                "named decision behind it cannot be prioritised against one that has"
            )
        if not self.acceptance_criteria.strip():
            raise TrialError(f"{self.key}: a backlog item needs an acceptance criterion")
        if not self.evidence:
            raise TrialError(
                f"{self.key}: a backlog item needs evidence — this workflow promotes validated "
                "findings, and a claim with nothing behind it has not been validated"
            )
        if not self.promotion_rules:
            raise TrialError(f"{self.key}: a backlog item must record which promotion rule fired")
        if self.trader_count < 1:
            raise TrialError(f"{self.key}: trader_count must be at least 1")

    @property
    def occurrences(self) -> int:
        return len(self.session_ids)

    @property
    def reproducible_occurrences(self) -> int:
        """How many sightings landed on a build that can be checked out again."""
        return sum(1 for stamp in self.releases if stamp.is_reproducible)

    @property
    def priority(self) -> tuple[int, int, int]:
        """Sort key, highest first: severity, then traders, then occurrences.

        Three separate integers rather than a weighted score. A blended number
        would make a minor issue seen six times outrank a blocker seen once, and
        no desk would accept that ordering once they saw what produced it.
        """
        # Severity.rank is a sort *position* — 0 is a blocker — so it is negated
        # here to share one descending sort with the two counts, which run the
        # other way. Mixing the directions in one tuple is the bug this comment
        # exists to stop being reintroduced.
        return (-self.severity.rank, self.trader_count, self.occurrences)

    @property
    def title(self) -> str:
        where = f" [{self.page}]" if self.page else ""
        return f"[{self.severity.value}] {self.classification.value}{where}: {self.summary}"

    def cleared(self) -> BacklogItem:
        """Return a copy marked publishable. The owner's decision, made explicit."""
        return replace(self, cleared_for_public=True)

    def with_criteria(self, criteria: str) -> BacklogItem:
        """Replace the drafted criterion with an owned one."""
        if not criteria.strip():
            raise TrialError(f"{self.key}: acceptance criteria cannot be blank")
        return replace(self, acceptance_criteria=criteria, criteria_are_drafted=False)

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "classification": self.classification.value,
            "severity": self.severity.value,
            "page": self.page,
            "occurrences": self.occurrences,
            "trader_count": self.trader_count,
            "reproducible_occurrences": self.reproducible_occurrences,
            "promotion_rules": list(self.promotion_rules),
            "criteria_are_drafted": self.criteria_are_drafted,
            "tasks": [task.value for task in self.tasks],
        }
        if audience == AUDIENCE_AGGREGATE:
            # No summary, no evidence, no decision, no session ids, no dates. An
            # aggregate reader learns that three major numerical errors were
            # found on the origins page; it learns nothing a trader typed.
            return payload
        payload.update(
            {
                "summary": self.summary,
                "affected_decision": self.affected_decision,
                "acceptance_criteria": self.acceptance_criteria,
                "evidence": list(self.evidence),
                "expected": self.expected,
                "observed": self.observed,
                "session_ids": list(self.session_ids),
                "trading_days": [day.isoformat() for day in self.trading_days],
                "cleared_for_public": self.cleared_for_public,
            }
        )
        return payload


@dataclass(frozen=True)
class BacklogSet:
    """The promoted items and the rejected observations, from one drafting run."""

    items: tuple[BacklogItem, ...] = ()
    observations: tuple[Observation, ...] = ()
    session_count: int = 0
    issue_count: int = 0

    @property
    def by_severity(self) -> dict[str, int]:
        return dict(Counter(item.severity.value for item in self.items))

    @property
    def by_class(self) -> dict[str, int]:
        return dict(Counter(item.classification.value for item in self.items))

    @property
    def blockers(self) -> tuple[BacklogItem, ...]:
        return tuple(item for item in self.items if item.severity is Severity.BLOCKER)

    @property
    def undrafted_count(self) -> int:
        """Items still carrying a generated acceptance criterion nobody has read."""
        return sum(1 for item in self.items if item.criteria_are_drafted)

    def get(self, key: str) -> BacklogItem | None:
        for item in self.items:
            if item.key == key:
                return item
        return None

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        return {
            "session_count": self.session_count,
            "issue_count": self.issue_count,
            "item_count": len(self.items),
            "observation_count": len(self.observations),
            "undrafted_count": self.undrafted_count,
            "by_severity": self.by_severity,
            "by_class": self.by_class,
            "items": [item.to_dict(audience=audience) for item in self.items],
            "observations": [obs.to_dict(audience=audience) for obs in self.observations],
        }


def _promotion_rules(*, severity: Severity, classification: IssueClass, occurrences: int, traders: int) -> list[str]:
    fired: list[str] = []
    if severity is Severity.BLOCKER:
        fired.append("blocker")
    if classification.is_correctness:
        fired.append("correctness")
    if occurrences > 1:
        fired.append("recurrence")
    if traders > 1:
        fired.append("corroborated")
    return fired


def draft_backlog(sessions: Iterable[SessionRecord]) -> BacklogSet:
    """Group every issue across the trial, apply the promotion rule, draft criteria.

    Pure: takes records, returns a set. It writes nothing, files nothing and
    contacts no tracker — emitting an item is :func:`issue_body`, and filing it
    is a human running ``gh``. A routine that opened tickets by itself would put
    a trader's live-session free text into a public repository on no decision.
    """
    grouped: dict[str, list[tuple[SessionRecord, Issue]]] = {}
    issue_count = 0
    session_count = 0
    for session in sessions:
        session_count += 1
        for issue in session.issues:
            issue_count += 1
            grouped.setdefault(finding_key(issue), []).append((session, issue))

    items: list[BacklogItem] = []
    observations: list[Observation] = []
    for key, pairs in grouped.items():
        traders = {session.trader for session, _ in pairs}
        # The worst severity anyone assigned wins. Two traders disagreeing about
        # how bad a defect is means at least one of them was blocked by it.
        worst = min((issue.severity for _, issue in pairs), key=lambda severity: severity.rank)
        lead = next(issue for _, issue in pairs if issue.severity is worst)
        fired = _promotion_rules(
            severity=worst,
            classification=lead.classification,
            occurrences=len(pairs),
            traders=len(traders),
        )
        if not fired:
            observations.append(
                Observation(
                    key=key,
                    classification=lead.classification,
                    summary=lead.summary,
                    occurrences=len(pairs),
                    trader_count=len(traders),
                    reason_not_promoted=(
                        "seen once, by one trader, at a severity below blocker and in a class "
                        "where a single sighting is not yet evidence — re-report it to promote"
                    ),
                )
            )
            continue

        evidence = tuple(dict.fromkeys(issue.evidence for _, issue in pairs if issue.evidence.strip()))
        items.append(
            BacklogItem(
                key=key,
                classification=lead.classification,
                severity=worst,
                summary=lead.summary,
                affected_decision=lead.affected_decision,
                acceptance_criteria=suggested_acceptance_criteria(lead),
                evidence=evidence,
                page=lead.page,
                expected=lead.expected,
                observed=lead.observed,
                tasks=tuple(dict.fromkeys(session.task for session, _ in pairs)),
                session_ids=tuple(session.session_id for session, _ in pairs),
                trading_days=tuple(dict.fromkeys(session.trading_day for session, _ in pairs)),
                releases=tuple(session.release for session, _ in pairs),
                trader_count=len(traders),
                promotion_rules=tuple(fired),
                criteria_are_drafted=True,
            )
        )

    items.sort(key=lambda item: item.priority, reverse=True)
    observations.sort(key=lambda obs: (obs.occurrences, obs.trader_count), reverse=True)
    return BacklogSet(
        items=tuple(items),
        observations=tuple(observations),
        session_count=session_count,
        issue_count=issue_count,
    )


def issue_body(item: BacklogItem, *, audience: str = AUDIENCE_PRIVATE) -> str:
    """The markdown body for a tracker ticket.

    The public form refuses to render until the item is cleared, because the
    evidence field is whatever a trader typed mid-session and this repository is
    public. Clearing is one call (:meth:`BacklogItem.cleared`) and is the owner's
    judgement, which is exactly where that judgement belongs.
    """
    if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
        raise TrialError(f"unknown audience {audience!r}")
    if audience == AUDIENCE_AGGREGATE and not item.cleared_for_public:
        raise TrialError(
            f"{item.key} has not been cleared for public rendering; its evidence is free text "
            "captured during a live trading session and may name a cargo, a counterparty or a "
            "position. Review it and call BacklogItem.cleared() to publish."
        )

    lines = [
        f"## {item.title}",
        "",
        f"- **Severity**: {item.severity.value} — {item.severity.meaning}",
        f"- **Class**: {item.classification.value} — {item.classification.meaning}",
        f"- **Seen**: {item.occurrences}x by {item.trader_count} trader(s)",
        f"- **Promoted by**: {', '.join(PROMOTION_RULES[rule] for rule in item.promotion_rules)}",
        f"- **Reproducible sightings**: {item.reproducible_occurrences} of {item.occurrences}",
    ]
    if item.tasks:
        lines.append(f"- **Tasks affected**: {', '.join(task.label for task in item.tasks)}")
    lines += ["", "### Affected decision", "", item.affected_decision, "", "### Evidence", ""]
    for entry in item.evidence:
        lines.append(f"- {entry}")
    if item.expected or item.observed:
        lines += ["", f"Expected: {item.expected or 'not stated'}", f"Observed: {item.observed or 'not stated'}"]
    lines += ["", "### Acceptance criteria", "", item.acceptance_criteria]
    if item.criteria_are_drafted:
        lines += [
            "",
            "> **This acceptance criterion was drafted from the issue class and has not been "
            "reviewed by a human.** Replace it before the ticket is worked.",
        ]
    if audience == AUDIENCE_PRIVATE:
        lines += ["", f"<!-- sessions: {', '.join(item.session_ids)} -->"]
    return "\n".join(lines)


def backlog_markdown(backlog: BacklogSet, *, audience: str = AUDIENCE_PRIVATE) -> str:
    """The whole backlog as one document, in priority order."""
    lines = [
        "# Trial backlog",
        "",
        f"{len(backlog.items)} validated item(s) from {backlog.issue_count} issue(s) "
        f"across {backlog.session_count} session(s).",
        "",
    ]
    if backlog.undrafted_count:
        lines += [
            f"**{backlog.undrafted_count} item(s) still carry a drafted acceptance criterion.** "
            "They are not ready to be worked.",
            "",
        ]
    for item in backlog.items:
        if audience == AUDIENCE_AGGREGATE and not item.cleared_for_public:
            lines += [f"## {item.key} — withheld", "", "Not cleared for public rendering.", ""]
            continue
        lines += [issue_body(item, audience=audience), ""]
    if backlog.observations:
        lines += ["## Observations (not promoted)", ""]
        for obs in backlog.observations:
            label = obs.summary if audience == AUDIENCE_PRIVATE else obs.key
            lines.append(f"- `{obs.classification.value}` {label} — seen {obs.occurrences}x")
        lines.append("")
    return "\n".join(lines)
