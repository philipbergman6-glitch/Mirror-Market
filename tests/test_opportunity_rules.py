"""Candidate assembly, and every reason a candidate is held back.

Requirement 9 in full: the rules must not produce an actionable opportunity from
stale, incomparable, policy-blocked or incomplete inputs. Each of those is a
separate test, because each fails in a different place.
"""

from __future__ import annotations

from datetime import date, timedelta

from opportunity_fixtures import (
    PLAYER_ENTRIES,
    TODAY,
    make_detection,
    make_evidence,
    make_signal,
    soft_blocker,
)

from analysis.opportunities.domain import BlockerCode, Ladder, PartyRole
from analysis.opportunities.rules import (
    assess_blockers,
    build_candidate,
    build_candidates,
    counterparties_for,
    ladder_for,
)
from analysis.opportunities.signals import country_port


def codes(blockers) -> set[str]:
    return {blocker.code.value for blocker in blockers}


# ---------------------------------------------------------------------------
# The happy path — what an actionable opportunity actually needs
# ---------------------------------------------------------------------------
def test_a_clean_lane_reaches_actionable():
    opportunity = build_candidate(
        make_detection(), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert opportunity.ladder is Ladder.ACTIONABLE
    assert not opportunity.has_hard_blocker
    assert opportunity.sellers
    assert opportunity.suggested_next_action.startswith("Sound out the seller side")
    assert "Fixture US Originator" in opportunity.suggested_next_action


def test_counterparties_come_only_from_the_knowledge_base():
    sellers, buyers, _ = counterparties_for(make_detection(), entries=PLAYER_ENTRIES)
    names = {party.name for party in sellers} | {party.name for party in buyers}
    assert names <= {entry["name"] for entry in PLAYER_ENTRIES}
    assert all(party.role in (PartyRole.SELLER, PartyRole.BUYER) for party in sellers + buyers)


def test_lane_evidence_travels_with_the_name():
    sellers, _, _ = counterparties_for(make_detection(), entries=PLAYER_ENTRIES)
    us = next(party for party in sellers if party.country == "US")
    assert us.lane_evidenced is True
    assert us.last_verified == date(2026, 8, 1)


# ---------------------------------------------------------------------------
# Suppression: stale evidence
# ---------------------------------------------------------------------------
def test_stale_evidence_is_a_hard_blocker_and_caps_the_rung():
    stale = make_signal(
        evidence=(make_evidence(observed_on=TODAY - timedelta(days=30), max_age_days=7),),
        observed_on=TODAY - timedelta(days=30),
    )
    opportunity = build_candidate(
        make_detection(signal=stale), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.EVIDENCE_STALE.value in codes(opportunity.blockers)
    assert opportunity.ladder is Ladder.LEAD
    assert "is graded on" in opportunity.blockers[0].message


def test_evidence_inside_its_own_weekly_budget_is_not_stale():
    weekly = make_signal(
        evidence=(make_evidence(observed_on=TODAY - timedelta(days=10), max_age_days=21),),
        observed_on=TODAY - timedelta(days=10),
    )
    opportunity = build_candidate(
        make_detection(signal=weekly), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.EVIDENCE_STALE.value not in codes(opportunity.blockers)


# ---------------------------------------------------------------------------
# Suppression: policy
# ---------------------------------------------------------------------------
def test_a_policy_blocked_market_can_never_be_actionable():
    """India: the mandi bean prints far over CBOT and no trade closes it."""
    india = country_port("IN", "India")
    opportunity = build_candidate(
        make_detection(destination=india), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.POLICY_BARRIER.value in codes(opportunity.blockers)
    assert opportunity.ladder is Ladder.LEAD
    policy = next(b for b in opportunity.blockers if b.code is BlockerCode.POLICY_BARRIER)
    # The caveat is the registry's own sentence, not a second copy of it here.
    assert "GM soybean imports" in policy.message
    assert "policy changes" in policy.remedy


def test_the_policy_caveat_is_read_from_the_market_registry():
    import config

    india = config.MARKETS["india"]["basis"]
    opportunity = build_candidate(
        make_detection(destination=country_port("IN", "India")),
        today=TODAY, entries=PLAYER_ENTRIES,
    )
    policy = next(b for b in opportunity.blockers if b.code is BlockerCode.POLICY_BARRIER)
    assert india["caveat"] in policy.message


# ---------------------------------------------------------------------------
# Suppression: incomplete inputs
# ---------------------------------------------------------------------------
def test_missing_freight_is_a_hard_blocker_naming_the_command_that_fixes_it():
    opportunity = build_candidate(
        make_detection(missing=("ocean_freight",)), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.FREIGHT_UNKNOWN.value in codes(opportunity.blockers)
    assert opportunity.ladder is Ladder.LEAD
    freight = next(b for b in opportunity.blockers if b.code is BlockerCode.FREIGHT_UNKNOWN)
    assert "enter_assumption" in freight.remedy


def test_an_unpriced_quality_differential_is_soft_not_hard():
    """Grade is money, but a differential can be priced in the negotiation."""
    opportunity = build_candidate(
        make_detection(missing=("quality_adjustment",)), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.QUALITY_UNPRICED.value in codes(opportunity.blockers)
    assert opportunity.ladder is Ladder.ACTIONABLE


def test_no_priced_edge_cannot_be_actionable():
    opportunity = build_candidate(
        make_detection(without_economics=True), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert opportunity.economics is None
    assert opportunity.ladder is Ladder.LEAD
    assert any("dislocation, not a priced spread" in m for m in opportunity.missing_information)


# ---------------------------------------------------------------------------
# Suppression: incomparable / incomplete lane
# ---------------------------------------------------------------------------
def test_a_lane_with_no_window_is_capped_at_lead():
    opportunity = build_candidate(
        make_detection(without_window=True), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.WINDOW_INCOMPATIBLE.value in codes(opportunity.blockers)
    assert opportunity.ladder is Ladder.LEAD


def test_a_windowless_margin_is_not_blocked_for_having_no_window():
    """A crush margin is not a parcel. Demanding a shipment period there would
    block every candidate from a rule that never described one."""
    opportunity = build_candidate(
        make_detection(
            rule_id="crush_margin", origin=None, without_window=True,
            role_wanted=PartyRole.BUYER
        ),
        today=TODAY, entries=PLAYER_ENTRIES,
    )
    assert BlockerCode.WINDOW_INCOMPATIBLE.value not in codes(opportunity.blockers)
    # Still a lead: one end of the lane is unnamed.
    assert opportunity.ladder is Ladder.LEAD


def test_a_one_ended_lane_is_a_lead_not_an_opportunity():
    opportunity = build_candidate(
        make_detection(destination=None, without_window=True), today=TODAY, entries=PLAYER_ENTRIES
    )
    assert opportunity.ladder is Ladder.LEAD


def test_no_counterparty_means_it_is_only_a_market_signal():
    opportunity = build_candidate(
        make_detection(
            origin=country_port("ZZ", "Nowhere", role="origin"),
            destination=country_port("YY", "Elsewhere"),
        ),
        today=TODAY, entries=PLAYER_ENTRIES,
    )
    assert opportunity.ladder is Ladder.MARKET_SIGNAL
    assert BlockerCode.NO_COUNTERPARTY.value in codes(opportunity.blockers)
    assert "will not invent" in next(
        b.remedy for b in opportunity.blockers if b.code is BlockerCode.NO_COUNTERPARTY
    )


# ---------------------------------------------------------------------------
# Our own outage, named as ours
# ---------------------------------------------------------------------------
def test_our_own_ingest_failure_is_stated_as_ours(tmp_db):
    tmp_db.execute(
        "INSERT INTO data_freshness (layer_name, last_success, status) VALUES (?, ?, ?)",
        ("gulf_bids", "2026-08-01", "failed"),
    )
    tmp_db.commit()
    opportunity = build_candidate(
        make_detection(), today=TODAY, conn=tmp_db, entries=PLAYER_ENTRIES
    )
    outage = next(b for b in opportunity.blockers if b.code is BlockerCode.INGEST_OUTAGE)
    assert "our outage, not the market's" in outage.message
    assert opportunity.ladder is Ladder.LEAD


def test_a_healthy_layer_raises_no_outage_blocker(tmp_db):
    tmp_db.execute(
        "INSERT INTO data_freshness (layer_name, last_success, status) VALUES (?, ?, ?)",
        ("gulf_bids", "2026-08-18", "success"),
    )
    tmp_db.commit()
    opportunity = build_candidate(
        make_detection(), today=TODAY, conn=tmp_db, entries=PLAYER_ENTRIES
    )
    assert BlockerCode.INGEST_OUTAGE.value not in codes(opportunity.blockers)


# ---------------------------------------------------------------------------
# Liquidity
# ---------------------------------------------------------------------------
def test_administered_numbers_are_flagged_as_untraded():
    signal = make_signal(evidence=(make_evidence(quote_kind="administered"),))
    opportunity = build_candidate(
        make_detection(signal=signal), today=TODAY, entries=PLAYER_ENTRIES
    )
    liquidity = next(
        b for b in opportunity.blockers if b.code is BlockerCode.LIQUIDITY_UNPROVEN
    )
    assert "traded on" in liquidity.message
    # Soft: an administered value is precisely known, it is simply not a print.
    assert liquidity.is_hard is False


# ---------------------------------------------------------------------------
# Blocker hygiene
# ---------------------------------------------------------------------------
def test_blockers_are_deduplicated_and_stably_ordered():
    detection = make_detection(
        blockers=(soft_blocker(), soft_blocker()), missing=("ocean_freight",)
    )
    first = assess_blockers(detection, today=TODAY, has_counterparty=True)
    second = assess_blockers(detection, today=TODAY, has_counterparty=True)
    assert [b.code for b in first] == [b.code for b in second]
    assert len(first) == len({(b.code, b.message) for b in first})


def test_ladder_for_is_the_whole_decision():
    detection = make_detection()
    assert ladder_for(detection, (), has_counterparty=False) is Ladder.MARKET_SIGNAL
    assert ladder_for(detection, (), has_counterparty=True) is Ladder.ACTIONABLE
    from opportunity_fixtures import hard_blocker

    assert ladder_for(detection, (hard_blocker(),), has_counterparty=True) is Ladder.LEAD


# ---------------------------------------------------------------------------
# Isolation
# ---------------------------------------------------------------------------
def test_one_unbuildable_candidate_does_not_empty_the_board():
    broken = make_detection()
    object.__setattr__(broken, "product", None)  # forces an error inside assembly
    good = make_detection(rule_id="crush_margin")
    out = build_candidates([broken, good], today=TODAY, entries=PLAYER_ENTRIES)
    assert [item.rule_id for item in out] == ["crush_margin"]


def test_known_identities_keep_their_original_id_and_birthday():
    detection = make_detection()
    fresh = build_candidate(detection, today=TODAY, entries=PLAYER_ENTRIES)

    def lookup(identity: str):
        assert identity == fresh.identity
        return "OPP-20260101-abcdef", date(2026, 1, 1)

    later = build_candidates(
        [detection], today=TODAY, entries=PLAYER_ENTRIES, lookup=lookup
    )[0]
    assert later.opportunity_id == "OPP-20260101-abcdef"
    assert later.first_detected_on == date(2026, 1, 1)
    assert later.detected_on == TODAY
    assert later.identity == fresh.identity
