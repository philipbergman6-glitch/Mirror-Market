"""Identity, duplicates, expiry and the archive — requirement 7."""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date, timedelta

from opportunity_fixtures import TODAY, make_evidence, make_opportunity

import config
from analysis.opportunities import registry as registry_mod
from analysis.opportunities.domain import Ladder, OpportunityStatus
from analysis.opportunities.scoring import score


# ---------------------------------------------------------------------------
# Duplicates
# ---------------------------------------------------------------------------
def test_exact_duplicates_collapse_to_the_higher_rung():
    lead = make_opportunity(ladder=Ladder.LEAD)
    actionable = make_opportunity(ladder=Ladder.ACTIONABLE)
    assert lead.identity == actionable.identity
    kept = registry_mod.collapse_exact_duplicates([lead, actionable])
    assert len(kept) == 1
    assert kept[0].ladder is Ladder.ACTIONABLE


def test_collapse_does_not_depend_on_order():
    lead = make_opportunity(ladder=Ladder.LEAD)
    actionable = make_opportunity(ladder=Ladder.ACTIONABLE)
    forward = registry_mod.collapse_exact_duplicates([lead, actionable])[0]
    backward = registry_mod.collapse_exact_duplicates([actionable, lead])[0]
    assert forward.ladder is backward.ladder


def test_different_rules_on_one_lane_are_linked_not_merged():
    """Two rules agreeing is corroboration; merging would hide the second's evidence."""
    landed = make_opportunity(rule_id="landed_advantage")
    flow = make_opportunity(rule_id="destination_flow_shift", ladder=Ladder.LEAD)
    linked = registry_mod.link_related([landed, flow])
    assert len(linked) == 2
    assert linked[0].related_ids == (flow.opportunity_id,)
    assert linked[1].related_ids == (landed.opportunity_id,)


def test_a_lane_with_no_ends_is_not_related_to_everything():
    from analysis.opportunities.domain import Opportunity

    a = replace(make_opportunity(rule_id="x"), origin=None, destination=None)
    b = replace(make_opportunity(rule_id="y"), origin=None, destination=None)
    linked = registry_mod.link_related([a, b])
    assert all(item.related_ids == () for item in linked)
    assert isinstance(linked[0], Opportunity)


# ---------------------------------------------------------------------------
# Expiry
# ---------------------------------------------------------------------------
def test_expired_items_are_re_stamped_and_demoted_not_deleted():
    old = TODAY - timedelta(days=2)
    stale = make_opportunity(
        evidence=(make_evidence(observed_on=old, max_age_days=1),),
        expires_on=old + timedelta(days=1),
    )
    live, expired = registry_mod.prune_expired([stale], today=TODAY)
    assert live == []
    assert len(expired) == 1
    assert expired[0].status is OpportunityStatus.EXPIRED
    # An expired candidate must never still read as actionable.
    assert expired[0].ladder is Ladder.MARKET_SIGNAL
    assert "Expired" in expired[0].suggested_next_action


def test_expired_items_drop_off_after_the_grace_period():
    long_ago = TODAY - timedelta(days=config.OPPORTUNITY_EXPIRY_GRACE_DAYS + 30)
    stale = make_opportunity(
        evidence=(make_evidence(observed_on=long_ago, max_age_days=1),),
        expires_on=long_ago + timedelta(days=1),
    )
    live, expired = registry_mod.prune_expired([stale], today=TODAY)
    assert (live, expired) == ([], [])


def test_a_live_item_is_untouched():
    live, expired = registry_mod.prune_expired([make_opportunity()], today=TODAY)
    assert len(live) == 1 and expired == []
    assert live[0].status is OpportunityStatus.DETECTED


# ---------------------------------------------------------------------------
# The archive, and the stable id it exists to protect
# ---------------------------------------------------------------------------
def test_the_archive_makes_the_id_survive_a_fresh_database(patched_db):
    opportunity = make_opportunity(first_detected_on=date(2026, 1, 5))
    registry_mod.archive(
        [(opportunity, score(opportunity, today=TODAY))], today=TODAY
    )
    conn = sqlite3.connect(str(patched_db))
    try:
        known = registry_mod.previous_sightings(conn)
        assert known[opportunity.identity] == (
            opportunity.opportunity_id, date(2026, 1, 5)
        )
        lookup = registry_mod.make_lookup(conn)
        assert lookup(opportunity.identity) == (
            opportunity.opportunity_id, date(2026, 1, 5)
        )
        assert lookup("something we have never seen") == (None, None)
    finally:
        conn.close()


def test_the_birthday_never_moves_forward(patched_db):
    early = make_opportunity(first_detected_on=date(2026, 1, 5))
    late = replace(early, first_detected_on=date(2026, 6, 1), detected_on=TODAY)
    registry_mod.archive([(early, score(early, today=TODAY))], today=date(2026, 1, 5))
    registry_mod.archive([(late, score(late, today=TODAY))], today=TODAY)
    conn = sqlite3.connect(str(patched_db))
    try:
        _, first_seen = registry_mod.make_lookup(conn)(early.identity)
        assert first_seen == date(2026, 1, 5)
    finally:
        conn.close()


def test_the_archive_refuses_private_fields():
    import pytest

    from pipeline.store import save_opportunity_detections

    row = {
        "run_date": TODAY.isoformat(), "identity": "i", "opportunity_id": "o",
        "rule_id": "r", "ladder": "lead", "product": "beans",
        "first_detected_on": TODAY.isoformat(), "expires_on": TODAY.isoformat(),
        "confidence": "indicative", "method_version": "1.0.0",
        "notes": ["something a trader typed"],
    }
    with pytest.raises(ValueError, match="private workflow field"):
        save_opportunity_detections([row])


def test_the_archived_snapshot_is_the_public_serialisation(patched_db):
    import json

    from analysis.opportunities.domain import OpportunityStatus, WorkflowRecord

    base = make_opportunity()
    touched = replace(
        base,
        ladder=Ladder.LEAD,
        workflow=WorkflowRecord(
            opportunity_id=base.opportunity_id,
            status=OpportunityStatus.NEGOTIATING,
            owner="ARCHIVE-LEAK-SENTINEL",
            notes=("do not commit this",),
        ),
    )
    registry_mod.archive([(touched, score(touched, today=TODAY))], today=TODAY)
    conn = sqlite3.connect(str(patched_db))
    try:
        blob = conn.execute("SELECT snapshot_json FROM opportunity_detections").fetchone()[0]
    finally:
        conn.close()
    assert "ARCHIVE-LEAK-SENTINEL" not in blob
    assert "do not commit this" not in blob
    assert "workflow" not in json.loads(blob)


def test_stopped_detecting_reports_identities_not_re_seen(patched_db):
    opportunity = make_opportunity()
    registry_mod.archive(
        [(opportunity, score(opportunity, today=TODAY))], today=TODAY - timedelta(days=1)
    )
    conn = sqlite3.connect(str(patched_db))
    try:
        gone = registry_mod.expired_from_archive(conn, today=TODAY, seen_identities=set())
        assert [row["opportunity_id"] for row in gone] == [opportunity.opportunity_id]
        # Numbers are deliberately not carried forward — they were never recomputed.
        assert "economics" not in gone[0]
        still_here = registry_mod.expired_from_archive(
            conn, today=TODAY, seen_identities={opportunity.identity}
        )
        assert still_here == []
    finally:
        conn.close()
