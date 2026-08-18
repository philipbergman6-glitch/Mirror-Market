"""The Phase 4 vocabulary's invariants.

Every test here pins a rule whose violation produces a *plausible* screen rather
than a crash — which is why they are asserted at construction time rather than
checked in a renderer.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from opportunity_fixtures import (
    TODAY,
    hard_blocker,
    make_evidence,
    make_opportunity,
    make_signal,
)

from analysis.opportunities.domain import (
    AUDIENCE_PRIVATE,
    AUDIENCE_PUBLIC,
    HARD_BLOCKERS,
    Blocker,
    BlockerCode,
    Confidence,
    Ladder,
    OpportunityError,
    OpportunityStatus,
    ScoreCard,
    WorkflowRecord,
    identity_key,
    opportunity_id,
)


# ---------------------------------------------------------------------------
# Blockers
# ---------------------------------------------------------------------------
def test_blocker_needs_a_remedy():
    with pytest.raises(OpportunityError, match="message and a remedy"):
        Blocker(code=BlockerCode.FREIGHT_UNKNOWN, message="no freight", remedy="  ")


def test_hard_and_soft_are_read_from_one_set():
    assert Blocker(
        code=BlockerCode.POLICY_BARRIER, message="m", remedy="r"
    ).is_hard is True
    assert Blocker(code=BlockerCode.SIZE_UNKNOWN, message="m", remedy="r").is_hard is False
    # SIZE_UNKNOWN and LIQUIDITY_UNPROVEN must stay soft: almost every signal
    # carries one, and making either hard would cap the whole board at "lead".
    assert BlockerCode.SIZE_UNKNOWN not in HARD_BLOCKERS
    assert BlockerCode.LIQUIDITY_UNPROVEN not in HARD_BLOCKERS


# ---------------------------------------------------------------------------
# The ladder
# ---------------------------------------------------------------------------
def test_hard_blocker_cannot_be_actionable():
    with pytest.raises(OpportunityError, match="hard blockers"):
        make_opportunity(ladder=Ladder.ACTIONABLE, blockers=(hard_blocker(),))


def test_hard_blocker_is_fine_on_a_lead():
    opportunity = make_opportunity(ladder=Ladder.LEAD, blockers=(hard_blocker(),))
    assert opportunity.has_hard_blocker is True
    assert opportunity.ladder is Ladder.LEAD


def test_a_detector_cannot_reach_the_top_two_rungs():
    for rung in (Ladder.PROPOSED_TRADE, Ladder.COMPLETED):
        with pytest.raises(OpportunityError, match="not a rung a detector can reach"):
            make_opportunity(ladder=rung)


def test_a_detector_cannot_claim_a_person_acted():
    with pytest.raises(OpportunityError, match="asserts that a person did something"):
        make_opportunity(status=OpportunityStatus.CONTACTED)


def test_a_workflow_record_lifts_both_restrictions():
    record = WorkflowRecord(opportunity_id="x", status=OpportunityStatus.CONTACTED)
    opportunity = make_opportunity(ladder=Ladder.LEAD)
    record = WorkflowRecord(
        opportunity_id=opportunity.opportunity_id, status=OpportunityStatus.CONTACTED
    )
    promoted = make_opportunity(
        ladder=Ladder.PROPOSED_TRADE,
        status=OpportunityStatus.CONTACTED,
        blockers=(hard_blocker(),),
        workflow=WorkflowRecord(
            opportunity_id=opportunity.opportunity_id, status=OpportunityStatus.CONTACTED
        ),
    )
    assert promoted.ladder is Ladder.PROPOSED_TRADE
    assert promoted.has_hard_blocker is True
    assert record.status is OpportunityStatus.CONTACTED


def test_actionable_must_say_what_to_do():
    base = make_opportunity()
    from dataclasses import replace

    with pytest.raises(OpportunityError, match="must say what to do next"):
        replace(base, suggested_next_action="")


# ---------------------------------------------------------------------------
# Evidence and expiry
# ---------------------------------------------------------------------------
def test_an_already_lapsed_signal_still_builds():
    """A weekly source three weeks dark is data, not an exception.

    Crashing here would empty the board and make it look as though the rule
    found nothing, which is the opposite of what happened.
    """
    old = TODAY - timedelta(days=30)
    opportunity = make_opportunity(
        ladder=Ladder.LEAD,
        evidence=(make_evidence(observed_on=old, max_age_days=7),),
        expires_on=old + timedelta(days=7),
    )
    assert opportunity.is_expired(TODAY) is True


def test_expiry_before_its_own_evidence_is_arithmetic_not_data():
    with pytest.raises(OpportunityError, match="before its own oldest evidence"):
        make_opportunity(expires_on=date(2020, 1, 1))


def test_signal_needs_evidence():
    with pytest.raises(OpportunityError, match="carries no evidence"):
        make_signal(evidence=())


def test_signal_validity_must_be_positive():
    with pytest.raises(OpportunityError, match="validity_days must be positive"):
        make_signal(validity_days=0)


def test_evidence_freshness_uses_its_own_layer_budget():
    weekly = make_evidence(observed_on=TODAY - timedelta(days=10), max_age_days=21)
    daily = make_evidence(observed_on=TODAY - timedelta(days=10), max_age_days=7)
    assert weekly.freshness(TODAY).value == "current"
    assert daily.freshness(TODAY).value == "stale"


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------
def test_identity_ignores_the_numbers():
    """The same lane two days running is one opportunity that moved."""
    first = identity_key(
        rule_id="landed_advantage", product="beans", origin_key="us_gulf",
        destination_key="cn_north", window_start=date(2026, 9, 1),
    )
    second = identity_key(
        rule_id="landed_advantage", product="BEANS", origin_key="us_gulf",
        destination_key="cn_north", window_start=date(2026, 9, 1),
    )
    assert first == second


def test_identity_separates_rules_and_windows():
    base = dict(
        product="beans", origin_key="us_gulf", destination_key="cn_north",
        window_start=date(2026, 9, 1),
    )
    assert identity_key(rule_id="a", **base) != identity_key(rule_id="b", **base)
    assert identity_key(rule_id="a", **{**base, "window_start": date(2026, 10, 1)}) != (
        identity_key(rule_id="a", **base)
    )


def test_id_is_stable_while_first_seen_is():
    key = "rule|beans|us|cn|2026-09-01"
    assert opportunity_id(key, first_detected=date(2026, 8, 1)) == opportunity_id(
        key, first_detected=date(2026, 8, 1)
    )
    assert opportunity_id(key, first_detected=date(2026, 8, 1)) != opportunity_id(
        key, first_detected=date(2026, 8, 2)
    )


# ---------------------------------------------------------------------------
# Scores
# ---------------------------------------------------------------------------
def test_weights_must_sum_to_one():
    with pytest.raises(OpportunityError, match="sum to"):
        ScoreCard(
            economic=50, evidence=50, freshness=50, counterparty=50, feasibility=50,
            weights=(("economic", 0.5), ("evidence", 0.1), ("freshness", 0.1),
                     ("counterparty", 0.1), ("feasibility", 0.1)),
        )


def test_components_are_bounded():
    with pytest.raises(OpportunityError, match="outside 0..100"):
        ScoreCard(
            economic=120, evidence=50, freshness=50, counterparty=50, feasibility=50,
            weights=(("economic", 0.2), ("evidence", 0.2), ("freshness", 0.2),
                     ("counterparty", 0.2), ("feasibility", 0.2)),
        )


def test_composite_is_the_weighted_sum_by_hand():
    card = ScoreCard(
        economic=100, evidence=50, freshness=0, counterparty=80, feasibility=100,
        weights=(("counterparty", 0.15), ("economic", 0.30), ("evidence", 0.20),
                 ("feasibility", 0.20), ("freshness", 0.15)),
    )
    # 100*0.30 + 50*0.20 + 0*0.15 + 80*0.15 + 100*0.20 = 30 + 10 + 0 + 12 + 20 = 72
    assert card.composite == 72.0


# ---------------------------------------------------------------------------
# Serialisation and the privacy boundary
# ---------------------------------------------------------------------------
def test_public_serialisation_has_no_workflow_key_at_all():
    opportunity = make_opportunity(
        ladder=Ladder.LEAD,
        workflow=WorkflowRecord(
            opportunity_id=make_opportunity().opportunity_id,
            status=OpportunityStatus.NEGOTIATING,
            owner="Q-OWNER-SENTINEL",
            notes=("a note nobody outside should read",),
        ),
    )
    public = opportunity.to_dict(audience=AUDIENCE_PUBLIC, today=TODAY)
    assert "workflow" not in public
    assert "Q-OWNER-SENTINEL" not in str(public)
    assert "a note nobody outside should read" not in str(public)

    private = opportunity.to_dict(audience=AUDIENCE_PRIVATE, today=TODAY)
    assert private["workflow"]["owner"] == "Q-OWNER-SENTINEL"


def test_touched_opportunities_are_not_public_safe():
    clean = make_opportunity()
    assert clean.is_public_safe is True
    touched = make_opportunity(
        ladder=Ladder.LEAD,
        workflow=WorkflowRecord(
            opportunity_id=clean.opportunity_id, status=OpportunityStatus.REVIEWING
        ),
    )
    assert touched.is_public_safe is False


def test_unknown_audience_is_rejected():
    with pytest.raises(OpportunityError, match="unknown audience"):
        make_opportunity().to_dict(audience="internal")


def test_a_total_without_a_volume_is_an_invented_cargo():
    from dataclasses import replace

    base = make_opportunity()
    with pytest.raises(OpportunityError, match="invented cargo size"):
        replace(base, economics=replace(base.economics, total_low_usd=1_000.0))


def test_confidence_ordering_is_worst_wins():
    from analysis.opportunities.domain import worst_confidence

    assert worst_confidence(Confidence.EXECUTABLE, Confidence.PROVISIONAL) is (
        Confidence.PROVISIONAL
    )
    assert worst_confidence(None, None) is Confidence.UNAVAILABLE
