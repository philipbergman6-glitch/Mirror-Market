"""The local, private trader workflow (Phase 4) — requirements 6 and 8.

Status, owner, notes, contact dates, next actions and outcomes come from
``data/reference/opportunities/*.yml`` and from nowhere else. Same contract and
same reasoning as ``data/reference/positions`` in Phase 3: this project ingests
no CRM, no mailbox and no deal system, so the only honest source of "we called
them on Tuesday" is a file the trader wrote — and a *present but malformed* file
raises rather than rendering as an empty workflow, because "nothing recorded"
and "something recorded wrongly" are different states and only one of them is
safe to show as blank.

**The privacy boundary is here, and it is one-way.** Everything this module
loads is private. It is attached to an :class:`~analysis.opportunities.domain.Opportunity`
as a :class:`~analysis.opportunities.domain.WorkflowRecord`, which
``to_dict(audience="public")`` does not serialise, and the public page builder
never receives an opportunity carrying one — ``app/opportunities_page.py``
filters on ``is_public_safe`` before it builds anything. Three independent
guards for one rule, because a leak here is not a rendering bug, it is a
commercial disclosure:

1. the private half is a separate object, not a set of optional fields;
2. the public serialiser does not build the key at all;
3. ``pipeline.store.save_opportunity_detections`` rejects the private field
   names outright, so the git-committed archive cannot hold them either.

**Feedback is counted, never learned from.** :func:`feedback_summary` reports
per-rule outcomes to the person who entered them. It does not re-weight a rule.
A screen that quietly retuned itself on five dismissals would be a model nobody
trained, evaluated, or can turn off — and at this sample size it would be
fitting noise.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass, replace
from datetime import date
from pathlib import Path
from typing import Any

from analysis.opportunities.domain import (
    STATUS_LADDER,
    AuditEntry,
    Feedback,
    FeedbackKind,
    Ladder,
    Opportunity,
    OpportunityStatus,
    WorkflowRecord,
)

log = logging.getLogger(__name__)

__all__ = [
    "WORKFLOW_FIELDS",
    "WorkflowError",
    "WorkflowSet",
    "attach",
    "feedback_summary",
    "load_workflow",
    "parse_records",
]


class WorkflowError(ValueError):
    """A workflow file said something that cannot be true. Never swallowed."""


#: The document's accepted keys. Closed, and checked: a typo'd ``note`` where
#: ``notes`` was meant would silently drop the one thing the file exists to
#: record.
WORKFLOW_FIELDS = frozenset({
    "opportunity_id", "status", "owner", "notes", "contacted_on", "next_action",
    "next_action_due", "counterparty", "feedback", "audit",
})


def _as_date(value: Any, field: str, where: str) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError) as exc:
        raise WorkflowError(f"{where}: {field} must be an ISO date, got {value!r}") from exc


def _optional_date(value: Any, field: str, where: str) -> date | None:
    return None if value in (None, "") else _as_date(value, field, where)


def _status(value: Any, where: str) -> OpportunityStatus:
    try:
        return OpportunityStatus(str(value or "detected").strip().lower())
    except ValueError as exc:
        known = [status.value for status in OpportunityStatus]
        raise WorkflowError(f"{where}: unknown status {value!r}; known: {known}") from exc


def _feedback(raw: Any, where: str) -> Feedback:
    if not isinstance(raw, dict):
        raise WorkflowError(f"{where}: each feedback entry must be a mapping")
    try:
        kind = FeedbackKind(str(raw.get("kind", "")).strip().lower())
    except ValueError as exc:
        known = [item.value for item in FeedbackKind]
        raise WorkflowError(
            f"{where}: unknown feedback kind {raw.get('kind')!r}; known: {known}"
        ) from exc
    reason = str(raw.get("reason") or "").strip()
    if not reason:
        raise WorkflowError(
            f"{where}: feedback needs a `reason` — a dismissal with no reason teaches "
            "nobody anything, which is the only purpose this record has"
        )
    return Feedback(
        kind=kind,
        recorded_on=_as_date(raw.get("recorded_on"), "recorded_on", where),
        reason=reason,
        by=str(raw.get("by") or "").strip() or "unattributed",
    )


def _audit(raw: Any, where: str) -> AuditEntry:
    if not isinstance(raw, dict):
        raise WorkflowError(f"{where}: each audit entry must be a mapping")
    what = str(raw.get("what") or "").strip()
    if not what:
        raise WorkflowError(f"{where}: an audit entry must say what changed")
    return AuditEntry(
        on=_as_date(raw.get("on"), "on", where),
        by=str(raw.get("by") or "").strip() or "unattributed",
        what=what,
        detail=raw.get("detail"),
    )


def parse_records(document: Any, *, where: str) -> list[WorkflowRecord]:
    """Validate one workflow document. Every failure is loud.

    ``opportunity_id`` is required and is not checked against the engine's
    current output here: an id that no longer detects is a perfectly normal
    state (the condition went away while somebody was still working it), and
    rejecting it would delete the trader's own record of a live conversation.
    ``attach`` reports orphans instead.
    """
    if document is None:
        return []
    if not isinstance(document, list):
        raise WorkflowError(f"{where}: expected a YAML list of workflow records")

    records: list[WorkflowRecord] = []
    for index, raw in enumerate(document):
        label = f"{where}[{index}]"
        if not isinstance(raw, dict):
            raise WorkflowError(f"{label}: each record must be a mapping")
        unknown = sorted(set(raw) - WORKFLOW_FIELDS)
        if unknown:
            raise WorkflowError(
                f"{label}: unknown field(s) {unknown} — known: {sorted(WORKFLOW_FIELDS)}. "
                "A typo here silently drops the thing the file exists to record."
            )
        opportunity_id = str(raw.get("opportunity_id") or "").strip()
        if not opportunity_id:
            raise WorkflowError(f"{label}: opportunity_id is required")
        notes_raw = raw.get("notes") or []
        if isinstance(notes_raw, str):
            notes_raw = [notes_raw]
        if not isinstance(notes_raw, list):
            raise WorkflowError(f"{label}: notes must be a string or a list of strings")

        status = _status(raw.get("status"), label)
        records.append(WorkflowRecord(
            opportunity_id=opportunity_id,
            status=status,
            owner=(str(raw["owner"]).strip() if raw.get("owner") else None),
            notes=tuple(str(note) for note in notes_raw),
            contacted_on=_optional_date(raw.get("contacted_on"), "contacted_on", label),
            next_action=(str(raw["next_action"]).strip() if raw.get("next_action") else None),
            next_action_due=_optional_date(
                raw.get("next_action_due"), "next_action_due", label
            ),
            counterparty_named=(
                str(raw["counterparty"]).strip() if raw.get("counterparty") else None
            ),
            feedback=tuple(
                _feedback(item, f"{label}.feedback[{n}]")
                for n, item in enumerate(raw.get("feedback") or ())
            ),
            audit=tuple(
                _audit(item, f"{label}.audit[{n}]")
                for n, item in enumerate(raw.get("audit") or ())
            ),
            source_file=where,
        ))
    return records


@dataclass(frozen=True)
class WorkflowSet:
    """Every record the local files carry, indexed by opportunity id."""

    records: tuple[WorkflowRecord, ...] = ()
    loaded_from: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        ids = [record.opportunity_id for record in self.records]
        duplicates = sorted({key for key in ids if ids.count(key) > 1})
        if duplicates:
            raise WorkflowError(
                f"duplicate workflow record(s) for {duplicates} — two rows for one "
                "opportunity means one of them is stale; delete it rather than letting "
                "the loader pick"
            )

    @property
    def by_id(self) -> dict[str, WorkflowRecord]:
        return {record.opportunity_id: record for record in self.records}

    @property
    def is_empty(self) -> bool:
        return not self.records


def load_workflow(directory: str | os.PathLike[str] | None = None) -> WorkflowSet:
    """Read every ``*.yml`` in the private workflow directory.

    A missing directory is an empty workflow — a fresh clone has recorded
    nothing, and that is the correct state for it. A present but malformed file
    raises.
    """
    import config

    root = Path(str(directory) if directory is not None else config.OPPORTUNITY_WORKFLOW_DIR)
    if not root.is_dir():
        log.info("no opportunity workflow directory at %s — nothing is being worked", root)
        return WorkflowSet()

    import yaml

    records: list[WorkflowRecord] = []
    files: list[str] = []
    for path in sorted(root.glob("*.yml")):
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        records.extend(parse_records(document, where=str(path)))
        files.append(str(path))
    return WorkflowSet(records=tuple(records), loaded_from=tuple(files))


# ---------------------------------------------------------------------------
# Attaching
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class AttachResult:
    """Opportunities with their workflow, plus what did not line up.

    ``orphans`` are records naming an id the engine did not detect today. That
    is not an error and is not silently dropped: an opportunity somebody is
    actively working, whose underlying condition has gone away, is exactly the
    thing they need to be told about.
    """

    opportunities: tuple[Opportunity, ...]
    orphans: tuple[WorkflowRecord, ...]
    warnings: tuple[str, ...] = ()


def attach(
    opportunities: list[Opportunity],
    workflow: WorkflowSet,
    *,
    today: date | None = None,
) -> AttachResult:
    """Join the private records onto today's detections.

    The status may raise the rung — ``contacted`` and ``negotiating`` become
    ``PROPOSED_TRADE``, ``won`` and ``lost`` become ``COMPLETED`` — and never
    lowers it: a contacted lead is still, in market terms, a lead, and the
    ladder is about what the *thing* is, with the status saying what was done
    about it.

    Anything carrying a record becomes private by construction: attaching sets
    ``workflow``, and ``Opportunity.is_public_safe`` is then ``False``. The
    public page filters on that before it builds anything, so an opportunity
    somebody has touched cannot reach the published artifact at all — the fact
    that a desk is working a lane is itself commercial information.
    """
    del today  # accepted for symmetry with the other builders; not needed here
    lookup = workflow.by_id
    out: list[Opportunity] = []
    warnings: list[str] = []
    matched: set[str] = set()

    for opportunity in opportunities:
        record = lookup.get(opportunity.opportunity_id)
        if record is None:
            out.append(opportunity)
            continue
        matched.add(record.opportunity_id)
        promoted = STATUS_LADDER.get(record.status)
        ladder = opportunity.ladder
        if promoted is not None and promoted.rank > ladder.rank:
            ladder = promoted
        next_action = record.next_action or opportunity.suggested_next_action
        out.append(replace(
            opportunity,
            workflow=record,
            status=record.status,
            ladder=ladder,
            suggested_next_action=next_action,
        ))
        if record.status is OpportunityStatus.ACTIONABLE and opportunity.has_hard_blocker:
            codes = sorted({b.code.value for b in opportunity.blockers if b.is_hard})
            warnings.append(
                f"{record.opportunity_id} is marked actionable but still carries hard "
                f"blocker(s) {codes} — the record is kept as entered, and the blockers "
                "are still shown"
            )

    orphans = tuple(
        record for record in workflow.records if record.opportunity_id not in matched
    )
    for record in orphans:
        warnings.append(
            f"{record.opportunity_id} has a workflow record but did not detect today — "
            "the condition behind it may have gone away while it was being worked"
        )
    return AttachResult(
        opportunities=tuple(out), orphans=orphans, warnings=tuple(warnings)
    )


# ---------------------------------------------------------------------------
# Feedback — requirement 8
# ---------------------------------------------------------------------------
def feedback_summary(
    workflow: WorkflowSet,
    opportunities: list[Opportunity] | tuple[Opportunity, ...] = (),
) -> dict[str, Any]:
    """Per-rule outcomes, counted. Reported to a person; not fed to a model.

    ``rule_id`` is resolved from today's detections where possible. A record
    whose opportunity no longer detects is counted under ``unattributed`` rather
    than guessed at from the id — the id's hash carries no rule.
    """
    rule_by_id = {
        opportunity.opportunity_id: opportunity.rule_id for opportunity in opportunities
    }
    by_rule: dict[str, dict[str, int]] = {}
    totals: dict[str, int] = {}
    entries: list[dict[str, Any]] = []

    for record in workflow.records:
        rule_id = rule_by_id.get(record.opportunity_id, "unattributed")
        for item in record.feedback:
            by_rule.setdefault(rule_id, {})
            by_rule[rule_id][item.kind.value] = by_rule[rule_id].get(item.kind.value, 0) + 1
            totals[item.kind.value] = totals.get(item.kind.value, 0) + 1
            entries.append({
                "opportunity_id": record.opportunity_id,
                "rule_id": rule_id,
                **item.to_dict(),
            })

    return {
        "totals": dict(sorted(totals.items())),
        "by_rule": {rule: dict(sorted(counts.items())) for rule, counts in sorted(by_rule.items())},
        "entries": sorted(entries, key=lambda row: row["recorded_on"], reverse=True),
        "kinds": [kind.value for kind in FeedbackKind],
        "note": (
            "Counted and reported, never learned from. Nothing here re-weights a rule: a "
            "screen that retuned itself on a handful of dismissals would be a model nobody "
            "trained, evaluated or can turn off."
        ),
    }


def ladder_promoted(record: WorkflowRecord) -> Ladder | None:
    """Which rung this status implies, if any. Exposed for tests and the page."""
    return STATUS_LADDER.get(record.status)
