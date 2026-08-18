"""The local private workflow — requirements 6 and 8."""

from __future__ import annotations

from datetime import date

import pytest
import yaml
from opportunity_fixtures import TODAY, make_opportunity

from analysis.opportunities import workflow as workflow_mod
from analysis.opportunities.domain import Ladder, OpportunityStatus
from analysis.opportunities.workflow import (
    WorkflowError,
    WorkflowSet,
    attach,
    feedback_summary,
    load_workflow,
    parse_records,
)


def write(tmp_path, name: str, document) -> str:
    path = tmp_path / name
    path.write_text(yaml.safe_dump(document), encoding="utf-8")
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def test_a_missing_directory_is_an_empty_workflow(tmp_path):
    empty = load_workflow(str(tmp_path / "does-not-exist"))
    assert empty.is_empty
    assert empty.loaded_from == ()


def test_a_present_but_malformed_file_raises(tmp_path):
    """'Nothing recorded' and 'something recorded wrongly' are different states."""
    directory = write(tmp_path, "desk.yml", [{"status": "contacted"}])
    with pytest.raises(WorkflowError, match="opportunity_id is required"):
        load_workflow(directory)


def test_an_unknown_field_raises_rather_than_being_ignored(tmp_path):
    directory = write(tmp_path, "desk.yml", [{"opportunity_id": "OPP-1", "note": "typo"}])
    with pytest.raises(WorkflowError, match="unknown field"):
        load_workflow(directory)


def test_an_unknown_status_raises(tmp_path):
    directory = write(
        tmp_path, "desk.yml", [{"opportunity_id": "OPP-1", "status": "thinking-about-it"}]
    )
    with pytest.raises(WorkflowError, match="unknown status"):
        load_workflow(directory)


def test_feedback_without_a_reason_raises(tmp_path):
    directory = write(tmp_path, "desk.yml", [{
        "opportunity_id": "OPP-1",
        "feedback": [{"kind": "dismissed", "recorded_on": "2026-08-18", "by": "pb"}],
    }])
    with pytest.raises(WorkflowError, match="needs a `reason`"):
        load_workflow(directory)


def test_two_records_for_one_opportunity_raise():
    with pytest.raises(WorkflowError, match="duplicate workflow record"):
        WorkflowSet(records=tuple(
            parse_records([{"opportunity_id": "OPP-1"}, {"opportunity_id": "OPP-1"}], where="t")
        ))


def test_a_single_note_string_is_accepted_as_one_note():
    records = parse_records(
        [{"opportunity_id": "OPP-1", "notes": "just the one"}], where="t"
    )
    assert records[0].notes == ("just the one",)


def test_a_full_record_round_trips(tmp_path):
    directory = write(tmp_path, "desk.yml", [{
        "opportunity_id": "OPP-1",
        "status": "contacted",
        "owner": "pb",
        "contacted_on": "2026-08-13",
        "counterparty": "Fixture Trading SA",
        "next_action": "re-check the Sep band",
        "next_action_due": "2026-08-20",
        "notes": ["wants Sep, not Aug"],
        "feedback": [{
            "kind": "progressed", "recorded_on": "2026-08-13",
            "reason": "asked for an offer", "by": "pb",
        }],
        "audit": [{"on": "2026-08-13", "by": "pb", "what": "detected -> contacted"}],
    }])
    record = load_workflow(directory).records[0]
    assert record.status is OpportunityStatus.CONTACTED
    assert record.contacted_on == date(2026, 8, 13)
    assert record.feedback[0].kind.value == "progressed"
    assert record.audit[0].what == "detected -> contacted"


# ---------------------------------------------------------------------------
# Attaching
# ---------------------------------------------------------------------------
def test_a_status_raises_the_rung_but_never_lowers_it():
    lead = make_opportunity(ladder=Ladder.LEAD)
    records = parse_records(
        [{"opportunity_id": lead.opportunity_id, "status": "contacted"}], where="t"
    )
    result = attach([lead], WorkflowSet(records=tuple(records)))
    assert result.opportunities[0].ladder is Ladder.PROPOSED_TRADE

    actionable = make_opportunity(ladder=Ladder.ACTIONABLE)
    reviewing = parse_records(
        [{"opportunity_id": actionable.opportunity_id, "status": "reviewing"}], where="t"
    )
    kept = attach([actionable], WorkflowSet(records=tuple(reviewing)))
    assert kept.opportunities[0].ladder is Ladder.ACTIONABLE


def test_won_and_lost_reach_completed_business():
    for status in ("won", "lost"):
        opportunity = make_opportunity(ladder=Ladder.LEAD)
        records = parse_records(
            [{"opportunity_id": opportunity.opportunity_id, "status": status}], where="t"
        )
        attached = attach([opportunity], WorkflowSet(records=tuple(records)))
        assert attached.opportunities[0].ladder is Ladder.COMPLETED


def test_attaching_makes_an_opportunity_private():
    opportunity = make_opportunity()
    records = parse_records(
        [{"opportunity_id": opportunity.opportunity_id, "status": "reviewing"}], where="t"
    )
    attached = attach([opportunity], WorkflowSet(records=tuple(records)))
    assert attached.opportunities[0].is_public_safe is False
    # An untouched one stays public.
    assert attach([opportunity], WorkflowSet()).opportunities[0].is_public_safe is True


def test_an_entered_next_action_overrides_the_suggested_one():
    opportunity = make_opportunity()
    records = parse_records([{
        "opportunity_id": opportunity.opportunity_id,
        "next_action": "call them back on Thursday",
    }], where="t")
    attached = attach([opportunity], WorkflowSet(records=tuple(records)))
    assert attached.opportunities[0].suggested_next_action == "call them back on Thursday"


def test_an_orphan_record_is_reported_not_dropped():
    """Somebody is working a lane whose condition went away. That is news."""
    records = parse_records([{"opportunity_id": "OPP-19990101-deadbe"}], where="t")
    result = attach([make_opportunity()], WorkflowSet(records=tuple(records)))
    assert len(result.orphans) == 1
    assert any("did not detect today" in warning for warning in result.warnings)


def test_marking_a_hard_blocked_row_actionable_is_kept_but_warned():
    from opportunity_fixtures import hard_blocker

    blocked = make_opportunity(ladder=Ladder.LEAD, blockers=(hard_blocker(),))
    records = parse_records(
        [{"opportunity_id": blocked.opportunity_id, "status": "actionable"}], where="t"
    )
    result = attach([blocked], WorkflowSet(records=tuple(records)))
    assert result.opportunities[0].status is OpportunityStatus.ACTIONABLE
    assert result.opportunities[0].has_hard_blocker is True
    assert any("still carries hard blocker" in w for w in result.warnings)


# ---------------------------------------------------------------------------
# Feedback
# ---------------------------------------------------------------------------
def test_feedback_is_counted_per_rule():
    opportunity = make_opportunity(rule_id="landed_advantage")
    records = parse_records([{
        "opportunity_id": opportunity.opportunity_id,
        "feedback": [
            {"kind": "dismissed", "recorded_on": "2026-08-10", "reason": "no interest", "by": "pb"},
            {"kind": "false_signal", "recorded_on": "2026-08-11", "reason": "bad print", "by": "pb"},
        ],
    }], where="t")
    summary = feedback_summary(WorkflowSet(records=tuple(records)), [opportunity])
    assert summary["totals"] == {"dismissed": 1, "false_signal": 1}
    assert summary["by_rule"]["landed_advantage"] == {"dismissed": 1, "false_signal": 1}
    assert summary["entries"][0]["recorded_on"] == "2026-08-11"  # newest first


def test_feedback_for_an_unknown_opportunity_is_not_attributed_to_a_rule():
    records = parse_records([{
        "opportunity_id": "OPP-19990101-deadbe",
        "feedback": [{"kind": "won", "recorded_on": "2026-08-10", "reason": "done", "by": "pb"}],
    }], where="t")
    summary = feedback_summary(WorkflowSet(records=tuple(records)), [])
    assert list(summary["by_rule"]) == ["unattributed"]


def test_feedback_does_not_change_the_score():
    """Pinned deliberately: nothing here may become a silent re-weighting.

    A screen that retuned itself on a handful of dismissals would be a model
    nobody trained, evaluated or can turn off — so the score of an opportunity
    with five dismissals against its rule must equal the score of the same
    opportunity with none.
    """
    from analysis.opportunities.scoring import score

    opportunity = make_opportunity()
    before = score(opportunity, today=TODAY)

    records = parse_records([{
        "opportunity_id": opportunity.opportunity_id,
        "status": "dismissed",
        "feedback": [
            {"kind": "dismissed", "recorded_on": "2026-08-10", "reason": f"no {n}", "by": "pb"}
            for n in range(5)
        ],
    }], where="t")
    attached = attach([opportunity], WorkflowSet(records=tuple(records))).opportunities[0]
    after = score(attached, today=TODAY)

    assert before.to_dict() == after.to_dict()
    assert "never learned from" in workflow_mod.feedback_summary(WorkflowSet())["note"]
