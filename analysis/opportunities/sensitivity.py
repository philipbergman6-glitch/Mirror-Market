"""What would have to change before this stops being an opportunity (Phase 4).

Requirement 5's second half. Three questions, each answered by arithmetic a
reader can redo from the numbers on the page:

**How much room is there against the rule's own threshold?** A crush margin of
24.18 USD/MT fired on a 15.00 threshold, so it has 9.18 to give before it stops
detecting. This is the honest version of "how strong is this": strength measured
against the line that was actually drawn, not against a feeling.

**Which score component is carrying it?** Each component contributes at most
``weight x 100`` to the composite, so the swing from zero to full is a fixed,
stated number. If the largest swing belongs to a component sitting near 100,
that component is the whole finding and its inputs are the ones to check.

**What breaks the ranking?** For a landed advantage, Phase 2 already answers
this properly — ``analysis.origins.scenarios`` re-runs the whole waterfall under
named shocks and reports which ones flip the cheapest origin. That is reused
rather than re-implemented, because a second sensitivity model over the same
waterfall would eventually disagree with the first.

What this module does *not* do is invent a distribution. There is no probability
attached to any move here: this stack ingests no implied volatility, no freight
curve and no correlation matrix that would support one, and a percentage
confidence would be the most quotable fabricated number on the page.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import config
from analysis.opportunities.domain import Opportunity, ScoreCard

log = logging.getLogger(__name__)

__all__ = [
    "landed_scenarios",
    "score_swings",
    "sensitivity_for",
    "threshold_headroom",
]

#: Which configured threshold each rule fired against, and how to read the
#: magnitude. Kept beside the rules rather than inside them so the page can
#: state the line without re-running the detector.
_THRESHOLDS = {
    "landed_advantage": ("min_advantage_usd_mt", "usd_per_mt", "landed advantage"),
    "crush_margin": ("min_margin_usd_mt", "usd_per_mt", "crush margin"),
    "destination_flow_shift": ("min_z", "sigma", "share z-score"),
    "commitment_shift": ("min_z", "sigma", "share z-score"),
    "currency_shift": ("min_move_pct", "pct", "currency move"),
}


def threshold_headroom(opportunity: Opportunity) -> dict[str, Any] | None:
    """How far this sits above the line that made it fire.

    ``None`` for a rule with no numeric threshold — the tight-stocks rule fires
    on "below its own prior five-year low", which is a comparison against a
    moving history rather than a constant, and inventing a headroom figure for
    it would be inventing the history's stability.
    """
    entry = _THRESHOLDS.get(opportunity.rule_id)
    if entry is None:
        return None
    key, unit, label = entry
    settings = config.OPPORTUNITY_RULES.get(opportunity.rule_id, {})
    threshold = settings.get(key)
    signal = opportunity.signals[0]
    observed = signal.magnitude
    if threshold is None or observed is None:
        return None
    magnitude = abs(observed)
    headroom = magnitude - float(threshold)
    return {
        "label": label,
        "unit": unit,
        "observed": round(magnitude, 3),
        "threshold": float(threshold),
        "headroom": round(headroom, 3),
        "headroom_pct": (
            round(headroom / float(threshold) * 100.0, 1) if threshold else None
        ),
        "reading": (
            f"{label} is {magnitude:,.2f} against a {float(threshold):,.2f} threshold — "
            f"it has {headroom:,.2f} to give before this stops detecting"
        ),
    }


def score_swings(card: ScoreCard) -> list[dict[str, Any]]:
    """Per component: what it contributes now, and the most it ever could.

    ``contribution = value x weight``; ``max_swing = 100 x weight``. Ordered by
    swing, largest first, so the component that decides the ranking is the one a
    reader looks at first. Nothing is modelled — this is the weighted sum,
    written out term by term.
    """
    weights = dict(card.weights)
    notes = dict(card.notes)
    rows = []
    for key in ScoreCard._COMPONENTS:
        value = getattr(card, key)
        weight = weights[key]
        rows.append({
            "key": key,
            "value": round(value, 1),
            "weight": weight,
            "contribution": round(value * weight, 2),
            "max_swing": round(100.0 * weight, 2),
            "headroom": round((100.0 - value) * weight, 2),
            "note": notes.get(key),
        })
    return sorted(rows, key=lambda row: -row["max_swing"])


def landed_scenarios(
    conn,
    opportunity: Opportunity,
    *,
    today: date,
    assumptions=None,
) -> dict[str, Any] | None:
    """Phase 2's own scenario panel, for a landed-advantage opportunity.

    Rebuilt from the registry rather than carried on the opportunity: an
    ``OriginRanking`` is a live object graph over a database connection, and
    hanging one off a value type that gets serialised, archived and diffed is
    how a domain object grows a database's opinions. The cost is one extra
    ranking build for the handful of rows this applies to.
    """
    if opportunity.rule_id != "landed_advantage" or opportunity.shipment_window is None:
        return None
    if opportunity.destination is None:
        return None

    from analysis.origins.assumptions import load_assumptions
    from analysis.origins.comparison import build_ranking
    from analysis.origins.scenarios import freight_breakeven, run_panel

    destination_key = opportunity.destination.key
    if destination_key not in config.DESTINATION_PORTS:
        return None
    assumptions = assumptions if assumptions is not None else load_assumptions()
    try:
        ranking = build_ranking(
            conn,
            destination_key=destination_key,
            window=opportunity.shipment_window,
            today=today,
            assumptions=assumptions,
        )
        results = run_panel(ranking, assumptions, today=today)
        breakeven = freight_breakeven(ranking)
    except Exception:  # noqa: BLE001 — a sensitivity panel is never worth the page
        log.warning(
            "could not run the landed scenario panel for %s",
            opportunity.opportunity_id, exc_info=True,
        )
        return None

    return {
        "base_cheapest": ranking.cheapest.quote.origin.name if ranking.cheapest else None,
        "scenarios": [
            {
                **result.to_dict(),
                "cheapest_name": (
                    result.ranking.cheapest.quote.origin.name
                    if result.ranking.cheapest else None
                ),
            }
            for result in results
        ],
        "flips": [result.scenario.label for result in results if result.flips_the_answer],
        "breakeven": breakeven.to_dict() if breakeven else None,
        "note": (
            "Each scenario re-runs the whole landed-cost waterfall under one named shock and "
            "reports whether the cheapest origin changes. A scenario that flips the answer is "
            "the reason this opportunity is worth checking before it is quoted."
        ),
    }


def sensitivity_for(
    conn,
    opportunity: Opportunity,
    card: ScoreCard,
    *,
    today: date,
    assumptions=None,
) -> dict[str, Any]:
    """Everything the detail view shows under "what would change this"."""
    return {
        "headroom": threshold_headroom(opportunity),
        "swings": score_swings(card),
        "landed": landed_scenarios(
            conn, opportunity, today=today, assumptions=assumptions
        ),
        "no_probability_note": (
            "No probability is attached to any move here. This stack ingests no implied "
            "volatility, no freight curve and no correlation matrix that would support one, "
            "and a percentage confidence would be the most quotable fabricated number on the "
            "page."
        ),
    }
