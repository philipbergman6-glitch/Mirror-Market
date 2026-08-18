"""Scoring, verified by hand.

Every expected value in this file is written as the arithmetic that produces it,
so the test doubles as the specification. If a formula changes, the arithmetic
here has to change with it — which is the point: a score nobody can reproduce
is a score nobody can argue with.
"""

from __future__ import annotations

from datetime import timedelta

from opportunity_fixtures import (
    TODAY,
    hard_blocker,
    make_counterparty,
    make_evidence,
    make_opportunity,
    soft_blocker,
)

import config
from analysis.opportunities.domain import Confidence, Economics, Money
from analysis.opportunities.scoring import (
    CORROBORATION_BONUS,
    SOFT_BLOCKER_PENALTY,
    counterparty_score,
    economic_score,
    evidence_score,
    feasibility_score,
    freshness_score,
    rank,
    score,
)

FULL_SCALE = config.OPPORTUNITY_ECONOMIC_FULL_SCALE_USD_MT


def economics(per_mt: float) -> Economics:
    return Economics(
        per_mt=Money(per_mt), method="fixture", method_version="1.0.0", struck_on=TODAY
    )


# ---------------------------------------------------------------------------
# Economic
# ---------------------------------------------------------------------------
def test_economic_is_the_edge_over_full_scale():
    half = make_opportunity(economics=economics(FULL_SCALE / 2))
    value, note = economic_score(half)
    assert value == 50.0  # 12.5 / 25 * 100
    assert f"{FULL_SCALE:,.0f}" in note


def test_economic_saturates_rather_than_running_away():
    huge = make_opportunity(economics=economics(FULL_SCALE * 40))
    assert economic_score(huge)[0] == 100.0


def test_a_dislocation_with_no_edge_scores_zero_but_is_still_ranked():
    from dataclasses import replace

    bare = replace(make_opportunity(), economics=None)
    value, note = economic_score(bare)
    assert value == 0.0
    assert "not a priced spread" in note


# ---------------------------------------------------------------------------
# Evidence
# ---------------------------------------------------------------------------
def test_evidence_is_worst_confidence_plus_one_corroboration_bonus():
    single = make_opportunity()
    assert evidence_score(single)[0] == 75.0  # one indicative layer, no bonus

    two_layers = make_opportunity(
        evidence=(
            make_evidence(layer="gulf_bids"),
            make_evidence(layer="agrural", label="Paranagua FOB"),
        ),
    )
    assert evidence_score(two_layers)[0] == 75.0 + CORROBORATION_BONUS


def test_five_readings_of_one_layer_are_not_five_sources():
    same_layer = make_opportunity(
        evidence=tuple(
            make_evidence(layer="gulf_bids", label=f"leg {n}") for n in range(5)
        ),
    )
    assert evidence_score(same_layer)[0] == 75.0


def test_administered_scores_above_provisional():
    """An administered value is precisely known; it is simply not a traded price."""
    administered = make_opportunity(
        evidence=(make_evidence(confidence=Confidence.ADMINISTERED),)
    )
    provisional = make_opportunity(
        evidence=(make_evidence(confidence=Confidence.PROVISIONAL),)
    )
    assert evidence_score(administered)[0] == 60.0
    assert evidence_score(provisional)[0] == 40.0


def test_evidence_reads_the_evidence_not_the_row_confidence():
    """The row's confidence is dragged down by inferred counterparty research,
    which already has its own component — charging it twice pinned this one."""
    inferred_counterparty = make_opportunity(
        confidence=Confidence.PROVISIONAL,
        evidence=(make_evidence(confidence=Confidence.INDICATIVE),),
    )
    assert evidence_score(inferred_counterparty)[0] == 75.0


# ---------------------------------------------------------------------------
# Freshness
# ---------------------------------------------------------------------------
def test_freshness_is_the_fraction_of_the_budget_left():
    fresh = make_opportunity(evidence=(make_evidence(observed_on=TODAY, max_age_days=7),))
    assert freshness_score(fresh, today=TODAY)[0] == 100.0

    half = make_opportunity(
        evidence=(make_evidence(observed_on=TODAY - timedelta(days=7), max_age_days=14),)
    )
    assert freshness_score(half, today=TODAY)[0] == 50.0  # 100 * (1 - 7/14)


def test_freshness_floors_at_zero_and_takes_the_worst_item():
    mixed = make_opportunity(
        evidence=(
            make_evidence(observed_on=TODAY, max_age_days=7, label="today"),
            make_evidence(observed_on=TODAY - timedelta(days=90), max_age_days=7, label="ancient"),
        ),
    )
    value, note = freshness_score(mixed, today=TODAY)
    assert value == 0.0
    assert "ancient" in note


def test_a_weekly_source_is_judged_on_its_own_budget():
    weekly = make_opportunity(
        evidence=(make_evidence(observed_on=TODAY - timedelta(days=7), max_age_days=21),)
    )
    daily = make_opportunity(
        evidence=(make_evidence(observed_on=TODAY - timedelta(days=7), max_age_days=7),)
    )
    assert freshness_score(weekly, today=TODAY)[0] > freshness_score(daily, today=TODAY)[0]


# ---------------------------------------------------------------------------
# Counterparty
# ---------------------------------------------------------------------------
def test_counterparty_adds_up_as_documented():
    one = make_opportunity(sellers=(make_counterparty(lane_evidenced=False, tier=2),))
    assert counterparty_score(one, today=TODAY)[0] == 40.0

    full = make_opportunity(sellers=tuple(
        make_counterparty(f"House {n}", lane_evidenced=(n == 0), tier=1 if n == 0 else 2)
        for n in range(3)
    ))
    # 40 (any) + 20 (three or more) + 25 (lane evidenced) + 15 (tier 1) = 100
    assert counterparty_score(full, today=TODAY)[0] == 100.0


def test_stale_research_costs_twenty():
    from analysis.origins.players import VERIFICATION_STALE_DAYS

    stale = make_opportunity(sellers=(
        make_counterparty(lane_evidenced=False, tier=2, verified_days_ago=VERIFICATION_STALE_DAYS + 1),
    ))
    value, note = counterparty_score(stale, today=TODAY)
    assert value == 20.0  # 40 - 20
    assert "-20" in note


def test_the_side_scored_is_the_side_the_rule_calls_for():
    """Five well-researched sellers must not cover for nobody knowing the buyer."""
    sellers_only = make_opportunity(
        rule_id="supply_deficit",
        sellers=tuple(make_counterparty(f"S{n}") for n in range(5)),
        buyers=(),
    )
    value, note = counterparty_score(sellers_only, today=TODAY)
    assert value == 0.0
    assert "no buyer candidate" in note


# ---------------------------------------------------------------------------
# Feasibility
# ---------------------------------------------------------------------------
def test_a_hard_blocker_is_zero_not_partial_credit():
    from analysis.opportunities.domain import Ladder

    blocked = make_opportunity(ladder=Ladder.LEAD, blockers=(hard_blocker(),))
    value, note = feasibility_score(blocked)
    assert value == 0.0
    assert "cannot be worked today" in note


def test_soft_blockers_cost_a_fixed_amount_each():
    from analysis.opportunities.domain import BlockerCode

    two = make_opportunity(blockers=(
        soft_blocker(BlockerCode.SIZE_UNKNOWN),
        soft_blocker(BlockerCode.LIQUIDITY_UNPROVEN),
    ))
    assert feasibility_score(two)[0] == 100.0 - 2 * SOFT_BLOCKER_PENALTY
    assert feasibility_score(make_opportunity())[0] == 100.0


# ---------------------------------------------------------------------------
# Composite and ranking
# ---------------------------------------------------------------------------
def test_a_whole_scorecard_can_be_checked_by_hand():
    opportunity = make_opportunity(
        economics=economics(FULL_SCALE / 2),                  # economic  = 50
        # one indicative layer, observed today                # evidence  = 75
        evidence=(make_evidence(observed_on=TODAY, max_age_days=7),),  # freshness = 100
        sellers=(make_counterparty(lane_evidenced=False, tier=2),),    # counterparty = 40
        blockers=(),                                          # feasibility = 100
    )
    card = score(opportunity, today=TODAY)
    assert (card.economic, card.evidence, card.freshness, card.counterparty, card.feasibility) == (
        50.0, 75.0, 100.0, 40.0, 100.0
    )
    weights = config.OPPORTUNITY_SCORE_WEIGHTS
    expected = (
        50 * weights["economic"] + 75 * weights["evidence"] + 100 * weights["freshness"]
        + 40 * weights["counterparty"] + 100 * weights["feasibility"]
    )
    assert card.composite == round(expected, 1)


def test_a_big_number_on_bad_inputs_loses_to_a_modest_clean_one():
    """The most common false positive in this domain, pinned as a test."""
    from analysis.opportunities.domain import Ladder

    flashy = make_opportunity(
        rule_id="landed_advantage",
        ladder=Ladder.LEAD,
        economics=economics(FULL_SCALE * 4),
        evidence=(make_evidence(
            observed_on=TODAY - timedelta(days=30), max_age_days=7,
            confidence=Confidence.PROVISIONAL,
        ),),
        sellers=(),
        blockers=(hard_blocker(),),
        expires_on=TODAY,
    )
    modest = make_opportunity(economics=economics(FULL_SCALE / 4))
    ordered = rank([
        (flashy, score(flashy, today=TODAY)),
        (modest, score(modest, today=TODAY)),
    ])
    assert ordered[0][0] is modest


def test_ranking_is_deterministic():
    items = [make_opportunity(rule_id=name) for name in ("a", "b", "c")]
    scored = [(item, score(item, today=TODAY)) for item in items]
    assert [o.opportunity_id for o, _ in rank(scored)] == [
        o.opportunity_id for o, _ in rank(list(reversed(scored)))
    ]
