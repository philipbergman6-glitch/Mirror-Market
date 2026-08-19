"""Ranking, in five parts that can be checked by hand (Phase 4).

Requirement 3, taken literally: the components are the product and the
composite is only a sort key. Every formula below is arithmetic a reader can
reproduce from the numbers printed beside it on the page, and that is a
constraint rather than a nicety — a ranked list whose ordering cannot be
explained is a ranked list that will be believed.

The five components answer five different questions, and they are kept apart
because they fail independently:

``economic``      how much money is on the table, per tonne
``evidence``      how firm the numbers are — traded, assessed, or administered
``freshness``     how close to today they were observed, on each layer's own budget
``counterparty``  whether anybody is actually named on the side this needs
``feasibility``   whether the trade can be worked at all

A single score would let a large number on stale evidence with no counterparty
outrank a modest, executable, well-evidenced one. That is the most common false
positive in this domain, and the weights in ``config.OPPORTUNITY_SCORE_WEIGHTS``
deliberately do not put economics on top.

Nothing here learns. The feedback captured in ``workflow.py`` is counted and
reported; it does not re-weight a rule. A screen that quietly retuned itself on
five dismissals would be a model nobody trained, evaluated, or can turn off.
"""

from __future__ import annotations

from datetime import date

import config
from analysis.opportunities.domain import (
    Confidence,
    Counterparty,
    Ladder,
    Opportunity,
    PartyRole,
    ScoreCard,
    worst_confidence,
)
from analysis.origins.players import VERIFICATION_STALE_DAYS

__all__ = [
    "CONFIDENCE_SCORE",
    "SOFT_BLOCKER_PENALTY",
    "counterparty_score",
    "economic_score",
    "evidence_score",
    "feasibility_score",
    "freshness_score",
    "rank",
    "score",
]

#: What each confidence level is worth before any adjustment.
#:
#: ``ADMINISTERED`` sits above ``PROVISIONAL`` on purpose. An Argentine Ley
#: 21.453 minimum export value is not a weak number — it is precisely known and
#: legally binding — it simply is not a traded price, which is a different
#: complaint and one the blockers already make.
#:
#: ``BOARD_REFERENCE`` is the CBOT/DCE case and is deliberately *not* 100. The
#: board price this stack holds is a delayed daily bar from a consumer endpoint
#: whose settlement no provider proves, so it is the best number here and not a
#: proven one. ``EXECUTABLE`` is unreachable today by construction
#: (``pricing.semantics.PROVEN_SETTLEMENT_SOURCES`` is empty) and stays in the
#: table for the day an authoritative feed is substituted.
CONFIDENCE_SCORE = {
    Confidence.EXECUTABLE: 100.0,
    Confidence.BOARD_REFERENCE: 90.0,
    Confidence.INDICATIVE: 75.0,
    Confidence.ADMINISTERED: 60.0,
    Confidence.PROVISIONAL: 40.0,
    Confidence.UNAVAILABLE: 0.0,
}

#: A second, independent source is worth ten points. Corroboration is the one
#: thing this stack can offer against a single scraped number, and it is capped
#: at one bonus: five readings of the same layer are not five sources.
CORROBORATION_BONUS = 10.0

#: Each soft blocker costs this much feasibility. Four of them takes a candidate
#: from 100 to 40 — which is the right shape: none of them individually stops
#: the trade, and all of them together mean it is a project rather than a call.
SOFT_BLOCKER_PENALTY = 15.0


def economic_score(opportunity: Opportunity) -> tuple[float, str]:
    """Per-tonne edge against a saturating scale.

    ``min(100, |edge| / config.OPPORTUNITY_ECONOMIC_FULL_SCALE_USD_MT * 100)``.

    Saturating rather than linear-forever because the difference between a 40
    and an 80 dollar advantage is not "twice as interesting" — it is "both are
    enormous, go and check the inputs". An opportunity with no priced edge
    scores zero rather than being excluded: a dislocation with no margin in it
    is a real thing to know and a poor thing to rank first.
    """
    if opportunity.economics is None:
        return 0.0, (
            "no per-tonne edge — this is a dislocation, not a priced spread, and it is "
            "ranked accordingly"
        )
    edge = abs(opportunity.economics.per_mt.amount)
    scale = float(config.OPPORTUNITY_ECONOMIC_FULL_SCALE_USD_MT)
    value = min(100.0, edge / scale * 100.0) if scale > 0 else 0.0
    return value, f"{edge:,.2f} USD/MT against a {scale:,.0f} USD/MT full-scale"


def evidence_score(opportunity: Opportunity) -> tuple[float, str]:
    """Worst evidence confidence, plus one corroboration bonus.

    ``CONFIDENCE_SCORE[worst] + 10 if two or more distinct source layers``,
    capped at 100. Worst-wins for the same reason Phase 2 uses it: one hand-
    entered input drags an otherwise-executable row down, and the page should
    say so rather than average it away.

    Read from the *evidence*, not from ``Opportunity.confidence``. The latter is
    the row's overall confidence and is deliberately dragged down by inferred
    counterparty research — which already has its own component. Scoring it here
    too would charge one weakness twice, and since most players-base entries are
    honestly tagged ``inferred``, it pinned this component at 40 for nearly every
    row and stopped it discriminating at all.
    """
    worst = worst_confidence(*(item.confidence for item in opportunity.evidence))
    base = CONFIDENCE_SCORE[worst]
    layers = {item.source.layer for item in opportunity.evidence}
    bonus = CORROBORATION_BONUS if len(layers) >= 2 else 0.0
    note = f"worst evidence confidence '{worst.value}' ({base:.0f})"
    if bonus:
        note += f" + {bonus:.0f} for {len(layers)} independent layers"
    else:
        note += f" · single layer ('{next(iter(layers), '—')}'), no corroboration bonus"
    return min(100.0, base + bonus), note


def freshness_score(opportunity: Opportunity, *, today: date) -> tuple[float, str]:
    """How much of the *worst* evidence item's own budget is left.

    ``100 x (1 - age / max_age)``, floored at zero, taken over the evidence item
    with the highest ratio. Each item is judged against its *own* layer's
    ``config.LAYER_MAX_DATA_AGE_DAYS`` budget — the number ``main.py`` grades
    on — so a weekly source is not punished for being four days old and a daily
    one is.
    """
    worst_ratio = 0.0
    worst_label = ""
    for item in opportunity.evidence:
        budget = max(1, item.max_age_days)
        ratio = item.age_days(today) / budget
        if ratio >= worst_ratio:
            worst_ratio, worst_label = ratio, item.label
    value = max(0.0, min(100.0, 100.0 * (1.0 - worst_ratio)))
    return value, (
        f"{worst_label} has used {worst_ratio * 100:.0f}% of its layer's recency budget"
    )


def _wanted_side(opportunity: Opportunity) -> tuple[PartyRole, tuple[Counterparty, ...]]:
    """Which side this opportunity actually needs somebody on.

    A tight importer needs a buyer named; a cheap origin needs a seller. Scoring
    the union would let five well-researched sellers cover for the fact that
    nobody knows who in China would take it.
    """
    if opportunity.rule_id in ("supply_deficit", "crush_margin", "destination_flow_shift",
                               "commitment_shift"):
        return PartyRole.BUYER, opportunity.buyers
    return PartyRole.SELLER, opportunity.sellers


def counterparty_score(opportunity: Opportunity, *, today: date) -> tuple[float, str]:
    """Whether anybody is named on the side that matters, and how well.

    ``40`` for at least one candidate, ``+20`` for three or more, ``+25`` where
    an entry's own research names this destination (``lane_evidenced`` — a
    strictly stronger claim than "operates in the right country"), ``+15`` for a
    tier-1 name, and ``-20`` where every candidate's newest citation is older
    than the players base's own one-year staleness line.
    """
    role, parties = _wanted_side(opportunity)
    if not parties:
        return 0.0, f"no {role.value} candidate is carried for this lane"
    value = 40.0
    reasons = [f"1+ {role.value} candidate (40)"]
    if len(parties) >= 3:
        value += 20.0
        reasons.append(f"{len(parties)} candidates (+20)")
    if any(party.lane_evidenced for party in parties):
        value += 25.0
        reasons.append("lane evidenced in an entry's own research (+25)")
    if any(party.tier == 1 for party in parties):
        value += 15.0
        reasons.append("a tier-1 name (+15)")
    ages = [party.verification_age_days(today) for party in parties]
    if all(age is None or age > VERIFICATION_STALE_DAYS for age in ages):
        value -= 20.0
        reasons.append(f"every citation older than {VERIFICATION_STALE_DAYS} days (-20)")
    return max(0.0, min(100.0, value)), " · ".join(reasons)


def feasibility_score(opportunity: Opportunity) -> tuple[float, str]:
    """Can this be worked at all.

    A hard blocker is zero, full stop — policy, missing freight, an incompatible
    window, stale evidence, no counterparty or our own outage each mean the
    trade cannot be done today, and a partial credit there would let a blocked
    row float up a ranking on the strength of its economics. Otherwise
    ``100 - 15 per soft blocker``, floored at zero.
    """
    hard = [blocker for blocker in opportunity.blockers if blocker.is_hard]
    if hard:
        codes = ", ".join(sorted({blocker.code.value for blocker in hard}))
        return 0.0, f"hard blocker(s): {codes} — this cannot be worked today"
    soft = [blocker for blocker in opportunity.blockers if not blocker.is_hard]
    value = max(0.0, 100.0 - SOFT_BLOCKER_PENALTY * len(soft))
    if not soft:
        return 100.0, "no blockers"
    return value, (
        f"{len(soft)} soft blocker(s) x {SOFT_BLOCKER_PENALTY:.0f} — "
        + "; ".join(blocker.code.value for blocker in soft)
    )


def score(opportunity: Opportunity, *, today: date) -> ScoreCard:
    """The five components and their declared weights."""
    economic, economic_note = economic_score(opportunity)
    evidence, evidence_note = evidence_score(opportunity)
    freshness, freshness_note = freshness_score(opportunity, today=today)
    counterparty, counterparty_note = counterparty_score(opportunity, today=today)
    feasibility, feasibility_note = feasibility_score(opportunity)
    return ScoreCard(
        economic=round(economic, 1),
        evidence=round(evidence, 1),
        freshness=round(freshness, 1),
        counterparty=round(counterparty, 1),
        feasibility=round(feasibility, 1),
        weights=tuple(sorted(config.OPPORTUNITY_SCORE_WEIGHTS.items())),
        notes=(
            ("economic", economic_note),
            ("evidence", evidence_note),
            ("freshness", freshness_note),
            ("counterparty", counterparty_note),
            ("feasibility", feasibility_note),
        ),
    )


def rank(
    scored: list[tuple[Opportunity, ScoreCard]],
) -> list[tuple[Opportunity, ScoreCard]]:
    """Workable first, then by composite, then deterministically by id.

    **Rung outranks score, and that is the point.** A policy-blocked spread can
    carry a huge economic component — India's mandi bean is 284 USD/MT over CBOT
    — and sorting on the composite alone puts a trade nobody can do at the top
    of a board headed "what to work today". The rung is the answer to "can this
    be worked at all"; the composite ranks within that answer.

    Rungs above ``ACTIONABLE`` are clamped to it for ordering. A proposed trade
    and a completed one are private, and a won deal floating permanently at the
    top of the working board would push the live work down.

    The id is the last key so two runs over the same data produce the same page
    and a diff means something changed — never dict or file order.
    """
    ceiling = Ladder.ACTIONABLE.rank
    return sorted(
        scored,
        key=lambda pair: (
            -min(pair[0].ladder.rank, ceiling),
            -pair[1].composite,
            pair[0].opportunity_id,
        ),
    )
