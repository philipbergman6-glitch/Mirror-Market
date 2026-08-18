"""One run of the opportunity engine (Phase 4).

The order below is the contract, and every step in it is placed where it is for
a reason that would otherwise be a bug:

    detect            measurements, one pass, failures isolated and reported
    identify          recover each identity's first-seen date and stable id
    assemble          counterparties, blockers, ladder rung, next action
    collapse          one row per identity
    score             five components, then rank
    ARCHIVE           <- before any private record is attached
    expire            past its shortest evidence budget, kept for a grace period
    link              corroborating rules cross-referenced, never merged
    attach            the local, private workflow
    split             public set and private set

Two placements matter more than the rest.

**Archiving happens before ``attach``.** The archive is git-committed history,
so it must be built from opportunities that carry no workflow record at all —
not from ones that carry one and are serialised carefully. A guard that depends
on calling the right serialiser is a guard that fails the first time somebody
adds a debug dump.

**The public/private split happens last, and it is a filter on the object.**
An opportunity somebody has touched is private whatever its status, because the
fact that a desk is working a lane is itself commercial information. The public
page receives the filtered list; it is not trusted to check.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Any

import config
from analysis.opportunities import registry as registry_mod
from analysis.opportunities import rules as rules_mod
from analysis.opportunities import scoring as scoring_mod
from analysis.opportunities import signals as signals_mod
from analysis.opportunities import workflow as workflow_mod
from analysis.opportunities.domain import Ladder, Opportunity, ScoreCard

log = logging.getLogger(__name__)

__all__ = ["EngineResult", "ScoredOpportunity", "run"]


@dataclass(frozen=True)
class ScoredOpportunity:
    """An opportunity and its scorecard, kept together so neither travels alone."""

    opportunity: Opportunity
    score: ScoreCard

    def to_dict(self, *, audience: str, today: date) -> dict[str, Any]:
        return {
            **self.opportunity.to_dict(audience=audience, today=today),
            "score": self.score.to_dict(),
        }


@dataclass(frozen=True)
class EngineResult:
    """Everything one run produced, already split by audience.

    ``public`` and ``private`` are not two views of one list: ``public`` is
    strictly the opportunities carrying no workflow record, and ``private`` is
    everything. A caller that renders ``public`` cannot leak, whatever it does
    with the data.
    """

    as_of: date
    public: tuple[ScoredOpportunity, ...]
    private: tuple[ScoredOpportunity, ...]
    expired: tuple[ScoredOpportunity, ...]
    stopped_detecting: tuple[dict[str, Any], ...]
    coverage: tuple[dict[str, Any], ...]
    feedback: dict[str, Any]
    workflow_loaded_from: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    archived_rows: int = 0
    errors: tuple[str, ...] = field(default_factory=tuple)

    @property
    def counts(self) -> dict[str, int]:
        by_rung = {rung.value: 0 for rung in Ladder}
        for item in self.private:
            by_rung[item.opportunity.ladder.value] += 1
        return {
            "public": len(self.public),
            "private": len(self.private),
            "expired": len(self.expired),
            "stopped_detecting": len(self.stopped_detecting),
            **by_rung,
        }


def run(
    conn,
    *,
    today: date,
    assumptions=None,
    workflow_dir: str | None = None,
    players_entries: list[dict] | None = None,
    archive: bool = True,
) -> EngineResult:
    """Detect, assemble, score, rank, archive and split. Never raises on data."""
    errors: list[str] = []
    warnings: list[str] = []

    detection_run = signals_mod.detect_all(conn, today=today, assumptions=assumptions)
    for failure in detection_run.failed:
        errors.append(f"{failure['label']}: {failure['error']}")

    candidates = rules_mod.build_candidates(
        detection_run.detections,
        today=today,
        conn=conn,
        entries=players_entries,
        lookup=registry_mod.make_lookup(conn),
    )
    candidates = registry_mod.collapse_exact_duplicates(candidates)

    cards: dict[str, ScoreCard] = {}
    kept: list[Opportunity] = []
    for candidate in candidates:
        try:
            cards[candidate.opportunity_id] = scoring_mod.score(candidate, today=today)
            kept.append(candidate)
        except Exception as exc:  # noqa: BLE001 — one bad score must not empty the screen
            log.warning("could not score %s", candidate.opportunity_id, exc_info=True)
            errors.append(f"scoring {candidate.opportunity_id}: {type(exc).__name__}: {exc}")

    ranked = scoring_mod.rank([(item, cards[item.opportunity_id]) for item in kept])

    archived = 0
    if archive:
        try:
            archived = registry_mod.archive(ranked, today=today)
        except Exception:  # noqa: BLE001 — archiving must never fail a render
            log.warning("could not archive this run's opportunity detections", exc_info=True)
            warnings.append(
                "this run's detections were not archived — opportunity ids stay stable only "
                "while the archive is being written"
            )

    live, expired = registry_mod.prune_expired([item for item, _ in ranked], today=today)
    live = registry_mod.link_related(live)

    try:
        workflow = workflow_mod.load_workflow(workflow_dir)
    except workflow_mod.WorkflowError:
        # A malformed workflow file is the trader's own record being wrong, and
        # is exactly the case Phase 3 refuses to render as empty. It fails the
        # build rather than quietly showing nothing.
        raise
    attached = workflow_mod.attach(live, workflow, today=today)
    warnings.extend(attached.warnings)

    private = tuple(
        ScoredOpportunity(item, cards[item.opportunity_id])
        for item in attached.opportunities
        if item.opportunity_id in cards
    )
    public = tuple(item for item in private if item.opportunity.is_public_safe)

    return EngineResult(
        as_of=today,
        public=public,
        private=private,
        expired=tuple(
            ScoredOpportunity(item, cards[item.opportunity_id])
            for item in expired
            if item.opportunity_id in cards
        ),
        stopped_detecting=tuple(registry_mod.expired_from_archive(
            conn, today=today, seen_identities={item.identity for item, _ in ranked}
        )),
        coverage=detection_run.coverage,
        feedback=workflow_mod.feedback_summary(
            workflow, [item.opportunity for item in private]
        ),
        workflow_loaded_from=workflow.loaded_from,
        warnings=tuple(warnings),
        archived_rows=archived,
        errors=tuple(errors),
    )


def method_note() -> str:
    return (
        f"Opportunity engine v{config.OPPORTUNITY_METHOD_VERSION} · landed cost "
        f"v{config.LANDED_COST_METHOD_VERSION}"
    )
