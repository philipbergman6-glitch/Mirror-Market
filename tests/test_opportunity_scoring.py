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
            make_evidence(layer="agrural", label="Paranagua port bid"),
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


# ---------------------------------------------------------------------------
# A signal with no lane cannot outrank a lead with one (#672)
# ---------------------------------------------------------------------------
US_CRUSHER = {
    "name": "Fixture US Crusher",
    "country": "US",
    "roles": ["crusher", "processor"],
    "products": ["beans"],
    "confidence": "observed",
    "as_of": "2026-08-01",
    "tier": 1,
    "citations": [{"url": "https://example.invalid/us-crush", "accessed": "2026-08-01"}],
}


def test_a_board_crush_with_no_lane_cannot_outrank_a_lane_lead_with_a_lower_score(monkeypatch):
    """The live row: a 68.30 USD/MT CBOT board crush scored 83.9 as a lead,
    with the venue standing in for its destination. Through the real detector."""
    from opportunity_fixtures import (
        PLAYER_ENTRIES,
        board_crush_result,
        make_detection,
        stub_crush_stack,
    )

    from analysis.opportunities import signals as signals_mod
    from analysis.opportunities.domain import Ladder
    from analysis.opportunities.rules import build_candidate

    stub_crush_stack(monkeypatch, {"cbot": board_crush_result("cbot")})
    [detection] = [
        d for d in signals_mod.crush_margin_detections(None, today=TODAY, assumptions={})
        if d.context["market"] == "cbot"
    ]
    entries = [*PLAYER_ENTRIES, US_CRUSHER]
    crush = build_candidate(detection, today=TODAY, entries=entries)
    lead = build_candidate(
        make_detection(destination=None, without_window=True, economics=economics(1.0),
                       blockers=(soft_blocker(),)),
        today=TODAY, entries=entries,
    )
    crush_card, lead_card = score(crush, today=TODAY), score(lead, today=TODAY)

    assert lead.ladder is Ladder.LEAD and lead.origin is not None
    assert crush.ladder is Ladder.MARKET_SIGNAL
    # The precondition that makes this a regression: on raw arithmetic the
    # lane-less crush is the bigger number.
    assert crush_card.composite > lead_card.composite
    ordered = rank([(crush, crush_card), (lead, lead_card)])
    assert [item.opportunity_id for item, _ in ordered] == [
        lead.opportunity_id, crush.opportunity_id
    ]
    # The number itself is untouched — only its rank and its rung.
    assert crush.economics is not None and crush.economics.per_mt.amount == 68.30
    assert crush.signal_gap.startswith("no physical destination")


def test_a_market_signal_withholds_its_composite_but_prints_its_components():
    from analysis.opportunities.domain import Ladder

    signal = make_opportunity(ladder=Ladder.MARKET_SIGNAL, signal_gap="no physical destination: x")
    card = score(signal, today=TODAY).to_dict()
    assert card["composite"] is None
    assert "market signal" in card["withheld_reason"]
    assert [c["key"] for c in card["components"]] == [
        "economic", "evidence", "freshness", "counterparty", "feasibility"
    ]
    assert all(c["value"] is not None for c in card["components"])


def test_a_lead_prints_its_composite():
    from analysis.opportunities.domain import Ladder

    lead = make_opportunity(ladder=Ladder.LEAD)
    card = score(lead, today=TODAY)
    assert card.to_dict()["composite"] == card.composite
    assert card.to_dict()["withheld_reason"] is None
