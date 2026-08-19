"""Data for the Opportunities page (Phase 4) — data only, never markup.

Same split every other page family keeps (M18 #214, ``app/origins_page.py``,
``app/workstation_page.py``): this module returns numbers, labels and reasons;
``app/templates/opportunities.html.j2`` decides how they look. The reason is
sharper here than anywhere else on the site — an opportunity screen that renders
a blocked lead and a workable trade with the same treatment teaches a reader
that the distinction is cosmetic, and the distinction is the entire product.

**The audience is a parameter of the build, not of the render.** ``build_view``
takes ``audience`` and, on the public path, is handed only the opportunities
that carry no workflow record at all (``EngineResult.public``). The private
half is therefore not something the template is trusted to omit: it is not in
the template's context. Two artifacts, two builds, one builder — and
``tests/test_opportunities_page.py`` renders the public page against a workflow
stuffed with distinctive strings and greps the HTML for every one of them.

Section order is the order the question is asked:

    01  the board — what is worth looking at, ranked, filterable
    02  the detail — lineage, economics, evidence, blockers, sensitivity
    03  the ladder — signal, lead, opportunity, proposal, business
    04  coverage — which rules ran, on what thresholds, and what they found
    05  lifecycle — what expired, and what stopped detecting
    06  method — how the score is built, and what this page will not do
    07  workflow (private only) — status, owner, notes, next actions
    08  feedback (private only) — outcomes recorded, counted, not learned from
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import config
from analysis.opportunities import engine as engine_mod
from analysis.opportunities import scoring as scoring_mod
from analysis.opportunities import sensitivity as sensitivity_mod
from analysis.opportunities.domain import (
    AUDIENCE_PRIVATE,
    AUDIENCE_PUBLIC,
    DETECTOR_STATUSES,
    HARD_BLOCKERS,
    Ladder,
    OpportunityStatus,
    PartyRole,
)
from pricing.semantics import CONFIDENCE_CEILING, PROVEN_SETTLEMENT_SOURCES, PriceType

log = logging.getLogger(__name__)

#: Confidence levels no ingested number can reach today, and therefore levels
#: the method section must not advertise. `executable` requires a provider that
#: proves an official settlement; `PROVEN_SETTLEMENT_SOURCES` is empty, so no
#: layer here produces one. Derived rather than hard-coded, so buying a
#: settlement feed makes the rung appear on the page by itself.
UNREACHABLE_CONFIDENCE = frozenset(
    {CONFIDENCE_CEILING[PriceType.SETTLEMENT]} if not PROVEN_SETTLEMENT_SOURCES else set()
)

STATE_OK = "ok"
STATE_EMPTY = "empty"
STATE_ABSENT = "absent"

#: ``(id, title, why, audiences)``. The last field is the privacy boundary made
#: declarative: a section marked private-only is never built on the public path,
#: so it cannot be leaked by a template that forgets a condition.
_BOTH = (AUDIENCE_PUBLIC, AUDIENCE_PRIVATE)
_PRIVATE_ONLY = (AUDIENCE_PRIVATE,)

SECTION_SPECS = (
    ("board", "The board", "ranked, filterable, and every rung labelled", _BOTH),
    ("detail", "Detail", "lineage, economics, evidence, blockers, sensitivity", _BOTH),
    ("ladder", "Signal, lead, opportunity", "five rungs, and what separates them", _BOTH),
    ("coverage", "Rule coverage", "which rules ran, on what thresholds, what they found", _BOTH),
    ("lifecycle", "Expired & withdrawn", "what aged out, and what stopped detecting", _BOTH),
    ("method", "Method & limits", "how the score is built, and what this page will not do", _BOTH),
    ("workflow", "Working file", "status, owner, notes, contacts — local and private", _PRIVATE_ONLY),
    ("feedback", "Feedback", "outcomes recorded, counted, and not learned from", _PRIVATE_ONLY),
)

LADDER_LABELS = {
    Ladder.MARKET_SIGNAL: "market signal",
    Ladder.LEAD: "lead",
    Ladder.ACTIONABLE: "actionable",
    Ladder.PROPOSED_TRADE: "proposed trade",
    Ladder.COMPLETED: "completed business",
}

ROLE_LABELS = {
    PartyRole.BUYER: "find a buyer",
    PartyRole.SELLER: "find a seller",
    PartyRole.FACILITY: "facility",
}

#: Which side each rule calls for. Restated from ``scoring._wanted_side`` would
#: be two sources of truth for one fact, so it is imported from there instead.
def _role_for(opportunity) -> PartyRole:
    role, _ = scoring_mod._wanted_side(opportunity)  # noqa: SLF001 — one definition, deliberately
    return role


def _sections_for(audience: str) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (sid, title, why)
        for sid, title, why, audiences in SECTION_SPECS
        if audience in audiences
    )


def _section(section_id: str, *, audience: str, state: str, reason: str = "", data: Any = None) -> dict:
    for index, (sid, title, why) in enumerate(_sections_for(audience), start=1):
        if sid == section_id:
            if state != STATE_OK and not reason.strip():
                raise ValueError(
                    f"section {section_id!r} is {state!r} with no reason — every empty state "
                    "must name its reason"
                )
            return {
                "id": sid, "no": f"{index:02d}", "title": title, "why": why,
                "state": state, "reason": reason, "data": data,
            }
    raise KeyError(f"section {section_id!r} is not rendered for the {audience} audience")


# ---------------------------------------------------------------------------
# Row views
# ---------------------------------------------------------------------------
def _row(item: engine_mod.ScoredOpportunity, *, audience: str, today: date) -> dict:
    opportunity = item.opportunity
    payload = item.to_dict(audience=audience, today=today)
    role = _role_for(opportunity)
    payload.update({
        "ladder_label": LADDER_LABELS[opportunity.ladder],
        "role": role.value,
        "role_label": ROLE_LABELS[role],
        "origin_iso": opportunity.origin.country_iso if opportunity.origin else None,
        "destination_iso": (
            opportunity.destination.country_iso if opportunity.destination else None
        ),
        "hard_blocker_codes": sorted({
            blocker.code.value for blocker in opportunity.blockers if blocker.is_hard
        }),
        "counterparty_count": len(opportunity.sellers) + len(opportunity.buyers),
    })
    return payload


def _facets(rows: list[dict]) -> dict[str, list[dict]]:
    """The filter options, derived from what is actually on the board.

    Derived rather than declared, so a filter can never offer a value that
    matches nothing. A static page with a control that silently does nothing is
    worse than no control — the same rule the origins window selector keeps.
    """
    def collect(key: str, label_key: str | None = None) -> list[dict]:
        counts: dict[str, int] = {}
        labels: dict[str, str] = {}
        for row in rows:
            value = row.get(key)
            if value in (None, ""):
                continue
            counts[value] = counts.get(value, 0) + 1
            labels[value] = str(row.get(label_key) or value) if label_key else str(value)
        return [
            {"value": value, "label": labels[value], "count": count}
            for value, count in sorted(counts.items(), key=lambda pair: (-pair[1], pair[0]))
        ]

    return {
        "product": collect("product"),
        "origin": collect("origin_iso"),
        "destination": collect("destination_iso"),
        "role": collect("role", "role_label"),
        "confidence": collect("confidence"),
        "status": collect("status"),
        "ladder": collect("ladder", "ladder_label"),
    }


def _board(rows: list[dict], *, audience: str) -> dict:
    return {
        "rows": rows,
        "facets": _facets(rows),
        "counts_by_ladder": {
            rung.value: sum(1 for row in rows if row["ladder"] == rung.value)
            for rung in Ladder
        },
        "audience": audience,
        "filter_note": (
            "Filters are a class toggle over pre-rendered rows — this is a static page and "
            "there is no server to ask. Every option below matches at least one row, because "
            "the options are derived from the board rather than declared."
        ),
        "rank_note": (
            "Ranked by the weighted composite of five components, every one of which is "
            "printed beside it. The composite is a sort key; the components are the answer."
        ),
    }


def _detail(
    conn,
    items: list[engine_mod.ScoredOpportunity],
    *,
    audience: str,
    today: date,
    assumptions=None,
) -> dict:
    cards = []
    for item in items:
        try:
            sensitivity = sensitivity_mod.sensitivity_for(
                conn, item.opportunity, item.score, today=today, assumptions=assumptions
            )
        except Exception:  # noqa: BLE001 — a sensitivity panel is never worth the page
            log.warning(
                "sensitivity failed for %s", item.opportunity.opportunity_id, exc_info=True
            )
            sensitivity = {"headroom": None, "swings": [], "landed": None}
        cards.append({
            **_row(item, audience=audience, today=today),
            "sensitivity": sensitivity,
            "lineage": [
                {
                    "label": evidence.label,
                    "value": evidence.value,
                    "unit": evidence.unit,
                    "observed_on": evidence.observed_on.isoformat(),
                    "age_days": evidence.age_days(today),
                    "budget_days": evidence.max_age_days,
                    "layer": evidence.source.layer,
                    "table": evidence.source.table,
                    "key": evidence.source.key,
                    "detail": evidence.source.detail,
                    "href": evidence.source.href,
                    "quote_kind": evidence.quote_kind,
                    "confidence": evidence.confidence.value,
                    "note": evidence.note,
                }
                for evidence in item.opportunity.evidence
            ],
        })
    return {"cards": cards}


def _ladder_section_data() -> dict:
    return {
        "rungs": [
            {
                "key": rung.value,
                "label": LADDER_LABELS[rung],
                "rank": rung.rank,
                "meaning": rung.meaning,
                "detector_reachable": rung.rank <= Ladder.ACTIONABLE.rank,
            }
            for rung in Ladder
        ],
        "note": (
            "The top two rungs cannot be reached by a detector. A proposed trade and completed "
            "business are statements about what a person did, so they exist only in the local "
            "workflow file — and an opportunity that carries one is private and never appears "
            "on the published page at all."
        ),
        "hard_blockers": sorted(code.value for code in HARD_BLOCKERS),
        "hard_blocker_note": (
            "A hard blocker caps a candidate at 'lead' by construction: the Opportunity type "
            "refuses to be built as actionable while one is present. Policy is the standing "
            "example — India's mandi bean prints far over CBOT and no trade closes it, because "
            "GM imports are banned behind a tariff wall."
        ),
    }


def _coverage(result: engine_mod.EngineResult) -> dict:
    return {
        "rules": [
            {
                **entry,
                "thresholds": {
                    key: value
                    for key, value in config.OPPORTUNITY_RULES.get(entry["rule_id"], {}).items()
                    if key not in ("label", "question")
                },
            }
            for entry in result.coverage
        ],
        "errors": list(result.errors),
        "note": (
            "A screen with three items looks the same whether three rules found one each or "
            "one rule found three and two crashed. This is how those are told apart."
        ),
    }


def _lifecycle(result: engine_mod.EngineResult, *, audience: str, today: date) -> dict:
    return {
        "expired": [_row(item, audience=audience, today=today) for item in result.expired],
        "stopped_detecting": list(result.stopped_detecting),
        "grace_days": config.OPPORTUNITY_EXPIRY_GRACE_DAYS,
        "note": (
            "An opportunity expires when the shortest of its evidence budgets runs out — not "
            "on a fixed timer. It stays visible, marked expired, for "
            f"{config.OPPORTUNITY_EXPIRY_GRACE_DAYS} days: a screen that silently deletes "
            "yesterday's items cannot be checked against yesterday's decisions."
        ),
        "stopped_note": (
            "These identities are in the archive but did not detect today. Their numbers are "
            "deliberately not shown — they were never recomputed, and rendering yesterday's "
            "figures as today's is the substitution this whole phase avoids."
        ),
    }


def _method(result: engine_mod.EngineResult, *, audience: str) -> dict:
    return {
        "method_version": config.OPPORTUNITY_METHOD_VERSION,
        "landed_method_version": config.LANDED_COST_METHOD_VERSION,
        "weights": [
            {"key": key, "weight": weight}
            for key, weight in sorted(config.OPPORTUNITY_SCORE_WEIGHTS.items())
        ],
        "formulas": [
            {
                "key": "economic",
                "formula": (
                    f"min(100, |edge USD/MT| / "
                    f"{config.OPPORTUNITY_ECONOMIC_FULL_SCALE_USD_MT:,.0f} x 100)"
                ),
                "why": (
                    "Saturating, not linear: the difference between a 40 and an 80 dollar "
                    "advantage is not 'twice as interesting', it is 'both are enormous, go "
                    "and check the inputs'."
                ),
            },
            {
                "key": "evidence",
                "formula": (
                    "score(worst evidence confidence) + "
                    f"{scoring_mod.CORROBORATION_BONUS:.0f} if two or more source layers"
                ),
                "why": (
                    "Worst-wins. One hand-entered input drags an otherwise well-evidenced row "
                    "down, and averaging it away is how a weak number gets carried by a "
                    "strong one."
                ),
            },
            {
                "key": "freshness",
                "formula": "100 x (1 - age / that layer's own recency budget), worst item",
                "why": (
                    "Each source is judged on its own budget — the number main.py grades on — "
                    "so a weekly source is not punished for being four days old."
                ),
            },
            {
                "key": "counterparty",
                "formula": (
                    "40 (any) + 20 (three or more) + 25 (lane evidenced) + 15 (tier 1) "
                    "- 20 (all citations stale)"
                ),
                "why": (
                    "Scored on the side the signal actually calls for. Five well-researched "
                    "sellers must not cover for nobody knowing who would buy."
                ),
            },
            {
                "key": "feasibility",
                "formula": (
                    f"0 if any hard blocker, else 100 - {scoring_mod.SOFT_BLOCKER_PENALTY:.0f} "
                    "per soft blocker"
                ),
                "why": (
                    "Zero, not partial credit: a blocked row must not float up a ranking on "
                    "the strength of its economics."
                ),
            },
        ],
        # Only the levels an ingested number can actually reach. `executable`
        # is in the model and is unreachable today: it requires a provider that
        # proves an official settlement, and this stack ingests none — CBOT and
        # DCE arrive as delayed daily closes, which is what `board_reference`
        # means. Listing a scale rung nothing can score would read as a rung
        # something does.
        "confidence_scores": [
            {"key": key.value, "score": value}
            for key, value in scoring_mod.CONFIDENCE_SCORE.items()
            if key not in UNREACHABLE_CONFIDENCE
        ],
        "confidence_note": (
            "The top rung of the scale is deliberately absent: it is reserved for a price "
            "proven to be an exchange settlement, and no source here is one. The board "
            "legs are the venue's own delayed daily closes."
        ),
        "privacy": {
            "audience": audience,
            # The only two statuses a detector can set on its own: one comes
            # from a rule, the other from the clock. Every other status is a
            # statement about what a person did, and is private by definition.
            "public_statuses": sorted(status.value for status in DETECTOR_STATUSES),
            "statement": (
                "This is the PUBLIC edition. It carries only what a detector produced from "
                "ingested market data. Status, owner, notes, contact dates and outcomes are "
                "recorded in a local file that this build never reads, and any opportunity "
                "somebody has touched is excluded from this page entirely — the fact that a "
                "desk is working a lane is itself commercial information."
            ) if audience == AUDIENCE_PUBLIC else (
                "This is the PRIVATE edition. It is written outside docs/ and is gitignored, "
                "because docs/ is what the Pages deploy uploads. It carries the local working "
                "file: status, owner, notes, contacts and outcomes."
            ),
        },
        "no_invention": (
            "No counterparty, contact, tonnage, freight rate or shipment window on this page "
            "was invented to complete a row. Where one is unknown the field is blank and, "
            "where the unknown is load-bearing, it appears as a blocker naming who could "
            "resolve it."
        ),
        "no_routing": (
            "Nothing here is an offer, a bid or an instruction. This project has no "
            "connection to any venue, broker, counterparty or execution system."
        ),
        "warnings": list(result.warnings),
        "archived_rows": result.archived_rows,
    }


def _workflow(result: engine_mod.EngineResult, *, today: date) -> dict:
    """PRIVATE ONLY. Never called on the public path — see ``build_view``."""
    worked = [
        {
            **_row(item, audience=AUDIENCE_PRIVATE, today=today),
        }
        for item in result.private
        if item.opportunity.workflow is not None
    ]
    return {
        "records": worked,
        "loaded_from": list(result.workflow_loaded_from),
        "statuses": [status.value for status in OpportunityStatus],
        "warnings": list(result.warnings),
        "directory": config.OPPORTUNITY_WORKFLOW_DIR,
        "note": (
            "Entered by hand and read from data/reference/opportunities/. This project ingests "
            "no CRM, mailbox or deal system, so 'we called them on Tuesday' can only come from "
            "you — and a present but malformed file fails the build rather than rendering as an "
            "empty working file."
        ),
    }


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
def build_view(
    conn,
    *,
    today: date,
    audience: str = AUDIENCE_PUBLIC,
    assumptions=None,
    result: engine_mod.EngineResult | None = None,
    workflow_dir: str | None = None,
    archive: bool = True,
) -> dict:
    """Everything the Opportunities page renders, as plain data.

    ``result`` may be passed in so the public and private editions are built
    from ONE engine run: two runs would archive twice, and — worse — could
    disagree, which on this page means the public edition showing a row the
    private one knows somebody has already dismissed.
    """
    if audience not in (AUDIENCE_PUBLIC, AUDIENCE_PRIVATE):
        raise ValueError(f"unknown audience {audience!r}")

    result = result if result is not None else engine_mod.run(
        conn, today=today, assumptions=assumptions,
        workflow_dir=workflow_dir, archive=archive,
    )

    # The single line that makes the privacy boundary structural: the public
    # edition is built from the engine's already-filtered public list, which
    # contains no opportunity carrying a workflow record at all.
    items = list(result.public if audience == AUDIENCE_PUBLIC else result.private)
    rows = [_row(item, audience=audience, today=today) for item in items]

    board_state, board_reason = (
        (STATE_OK, "") if rows else (STATE_EMPTY, _empty_board_reason(result))
    )

    sections = [
        _section("board", audience=audience, state=board_state, reason=board_reason,
                 data=_board(rows, audience=audience) if rows else None),
        _section(
            "detail", audience=audience,
            state=STATE_OK if items else STATE_EMPTY,
            reason="" if items else "nothing on the board to detail",
            data=_detail(conn, items, audience=audience, today=today, assumptions=assumptions)
            if items else None,
        ),
        _section("ladder", audience=audience, state=STATE_OK, data=_ladder_section_data()),
        _section("coverage", audience=audience, state=STATE_OK, data=_coverage(result)),
        _section("lifecycle", audience=audience, state=STATE_OK,
                 data=_lifecycle(result, audience=audience, today=today)),
        _section("method", audience=audience, state=STATE_OK,
                 data=_method(result, audience=audience)),
    ]

    if audience == AUDIENCE_PRIVATE:
        workflow_data = _workflow(result, today=today)
        sections.append(_section(
            "workflow", audience=audience,
            state=STATE_OK if workflow_data["records"] else STATE_EMPTY,
            reason="" if workflow_data["records"] else (
                "nothing is being worked. Add a YAML document under "
                f"{config.OPPORTUNITY_WORKFLOW_DIR} — this project ingests no CRM, so a status "
                "can only come from you"
            ),
            data=workflow_data if workflow_data["records"] else None,
        ))
        sections.append(_section(
            "feedback", audience=audience,
            state=STATE_OK if result.feedback["entries"] else STATE_EMPTY,
            reason="" if result.feedback["entries"] else (
                "no outcome has been recorded yet. Dismissals, false signals, "
                "contacted-no-interest, progressed, won and lost all go in the same file"
            ),
            data=result.feedback if result.feedback["entries"] else None,
        ))

    return {
        "today": today.isoformat(),
        "audience": audience,
        "is_private": audience == AUDIENCE_PRIVATE,
        "method_version": config.OPPORTUNITY_METHOD_VERSION,
        "counts": result.counts,
        "sections": sections,
        "headline": _headline(rows, result),
    }


def _empty_board_reason(result: engine_mod.EngineResult) -> str:
    failed = [entry for entry in result.coverage if not entry["ran"]]
    if failed:
        names = ", ".join(entry["label"] for entry in failed)
        return (
            f"nothing on the board, and {len(failed)} rule(s) did not run ({names}). That is "
            "our failure, not a quiet market — see rule coverage below."
        )
    if result.expired:
        return (
            f"nothing currently live. {len(result.expired)} candidate(s) were detected but "
            "their evidence is already past its own layer's recency budget; they are listed "
            "as expired below."
        )
    return (
        "every rule ran and none of them fired. The thresholds each rule tested against are "
        "listed under rule coverage, so a quiet board can be told apart from a blind one."
    )


def _headline(rows: list[dict], result: engine_mod.EngineResult) -> dict:
    actionable = [row for row in rows if row["ladder"] == Ladder.ACTIONABLE.value]
    leads = [row for row in rows if row["ladder"] == Ladder.LEAD.value]
    top = rows[0] if rows else None
    return {
        "actionable": len(actionable),
        "leads": len(leads),
        "signals": len([row for row in rows if row["ladder"] == Ladder.MARKET_SIGNAL.value]),
        "expired": len(result.expired),
        "top_id": top["opportunity_id"] if top else None,
        "top_headline": top["why_now"] if top else None,
        "top_score": top["score"]["composite"] if top else None,
        "lead_sentence": (
            f"{len(actionable)} actionable, {len(leads)} lead(s)"
            if rows else "nothing on the board"
        ),
    }


__all__ = ["LADDER_LABELS", "SECTION_SPECS", "STATE_ABSENT", "STATE_EMPTY", "STATE_OK", "build_view"]
