"""The trial vocabulary refuses records that would produce a meaningless metric.

Every test here is about a record that would *parse* and then quietly wreck a
denominator. That is the whole reason these validations live in ``__post_init__``
rather than in a review checklist.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    CORRECTNESS_ISSUES,
    PRIVATE_FIELD_NAMES,
    DayObservation,
    ExternalLookup,
    ExternalTool,
    IssueClass,
    Outcome,
    SessionRecord,
    Severity,
    TaskId,
    TrialError,
    session_id,
)
from tests.trial_fixtures import (
    CLEAN_STAMP,
    DIRTY_STAMP,
    MARK,
    SYNTHETIC_TRADERS,
    TODAY,
    day_observation,
    issue,
    lookup,
    session,
)


def test_the_protocol_defines_exactly_the_ten_required_tasks() -> None:
    assert len(list(TaskId)) == 10


def test_every_task_states_a_decision_a_success_criterion_and_a_time_target() -> None:
    for task in TaskId:
        assert task.label.strip()
        assert task.decision_question.strip().endswith("?")
        assert task.success_criteria.strip()
        assert task.target_minutes > 0
        assert task.cadence.strip()


def test_task_label_is_not_called_title_because_taskid_subclasses_str() -> None:
    # str.title() must still be the method it has always been. A property of
    # that name would shadow it and break on the first caller that used it.
    assert TaskId.MORNING_BRIEF.title() == "Morning_Brief"
    assert TaskId.MORNING_BRIEF.label == "Pre-open / morning brief"


def test_the_ten_issue_classes_the_brief_asked_for_all_exist() -> None:
    assert {cls.value for cls in IssueClass} == {
        "numerical_error",
        "semantic_mismatch",
        "stale_data",
        "missing_coverage",
        "misleading_ux",
        "workflow_friction",
        "false_alert",
        "missed_alert",
        "upstream_outage",
        "requested_enhancement",
    }


def test_an_upstream_outage_is_not_counted_as_this_product_being_wrong() -> None:
    # A source being down is measured as availability. Counting it as a
    # correctness failure would make an honest outage look like a defect.
    assert IssueClass.UPSTREAM_OUTAGE not in CORRECTNESS_ISSUES
    assert IssueClass.NUMERICAL_ERROR in CORRECTNESS_ISSUES


def test_severity_rank_orders_blocker_first() -> None:
    assert Severity.BLOCKER.rank < Severity.MAJOR.rank < Severity.MINOR.rank


# --- ExternalLookup -------------------------------------------------------
def test_a_lookup_must_name_the_question_we_could_not_answer() -> None:
    with pytest.raises(TrialError, match="could not answer"):
        ExternalLookup(tool=ExternalTool.BLOOMBERG, unanswered_question="   ", answer_found=True)


def test_a_lookup_to_an_unnamed_other_tool_is_refused() -> None:
    with pytest.raises(TrialError, match="tool_detail"):
        ExternalLookup(tool=ExternalTool.OTHER, unanswered_question=f"{MARK} q", answer_found=True)


def test_a_lookups_question_never_reaches_the_aggregate_projection() -> None:
    payload = lookup().to_dict(audience=AUDIENCE_AGGREGATE)
    assert "unanswered_question" not in payload
    assert "tool" in payload


# --- Issue ----------------------------------------------------------------
def test_an_issue_needs_evidence_and_a_decision_it_affects() -> None:
    for field in ("evidence", "affected_decision"):
        with pytest.raises(TrialError):
            issue(**{field: "  "})  # type: ignore[arg-type]


def test_a_correctness_issue_cannot_be_filed_as_minor() -> None:
    # If a number was wrong, the question is how wrong — not whether it mattered.
    with pytest.raises(TrialError, match="correctness"):
        issue(IssueClass.NUMERICAL_ERROR, Severity.MINOR)


def test_a_non_correctness_issue_may_be_minor() -> None:
    assert issue(IssueClass.WORKFLOW_FRICTION, Severity.MINOR).severity is Severity.MINOR


# --- SessionRecord --------------------------------------------------------
def test_a_completed_session_must_state_what_was_decided() -> None:
    with pytest.raises(TrialError, match="decision"):
        SessionRecord(
            trader=SYNTHETIC_TRADERS[0],
            task=TaskId.MORNING_BRIEF,
            trading_day=TODAY,
            started_at=datetime(2026, 8, 18, 7, tzinfo=timezone.utc),
            ended_at=datetime(2026, 8, 18, 7, 10, tzinfo=timezone.utc),
            outcome=Outcome.COMPLETED,
            confidence=4,
            would_act=True,
            release=CLEAN_STAMP,
            decision="",
        )


def test_a_session_that_did_not_complete_must_carry_an_issue_saying_why() -> None:
    with pytest.raises(TrialError, match="issue"):
        session(outcome=Outcome.BLOCKED, issues=())


def test_an_abandoned_session_with_an_issue_is_a_legal_record() -> None:
    record = session(outcome=Outcome.ABANDONED, issues=(issue(),), would_act=False)
    assert not record.outcome.is_complete
    assert record.issues


def test_a_trader_cannot_say_they_would_act_on_a_session_they_did_not_complete() -> None:
    with pytest.raises(TrialError, match="would_act"):
        session(outcome=Outcome.BLOCKED, issues=(issue(),), would_act=True)


def test_a_naive_timestamp_is_refused_because_desks_span_continents() -> None:
    with pytest.raises(TrialError, match="timezone"):
        SessionRecord(
            trader=SYNTHETIC_TRADERS[0],
            task=TaskId.MORNING_BRIEF,
            trading_day=TODAY,
            started_at=datetime(2026, 8, 18, 7),
            ended_at=datetime(2026, 8, 18, 7, 10, tzinfo=timezone.utc),
            outcome=Outcome.COMPLETED,
            confidence=4,
            would_act=True,
            release=CLEAN_STAMP,
            decision=f"{MARK} d",
        )


def test_a_session_that_ended_before_it_started_is_refused() -> None:
    start = datetime(2026, 8, 18, 7, tzinfo=timezone.utc)
    with pytest.raises(TrialError):
        SessionRecord(
            trader=SYNTHETIC_TRADERS[0],
            task=TaskId.MORNING_BRIEF,
            trading_day=TODAY,
            started_at=start,
            ended_at=start - timedelta(minutes=1),
            outcome=Outcome.COMPLETED,
            confidence=4,
            would_act=True,
            release=CLEAN_STAMP,
            decision=f"{MARK} d",
        )


@pytest.mark.parametrize("confidence", [0, 6, -1])
def test_confidence_outside_the_stated_scale_is_refused(confidence: int) -> None:
    with pytest.raises(TrialError, match="confidence"):
        session(confidence=confidence)


def test_duration_is_measured_from_the_sessions_own_clock() -> None:
    assert session(minutes=17).duration_minutes == pytest.approx(17.0)


def test_the_session_id_is_stable_and_does_not_print_the_trader() -> None:
    args = (TODAY, SYNTHETIC_TRADERS[0], TaskId.MORNING_BRIEF, datetime(2026, 8, 18, 7, tzinfo=timezone.utc))
    first = session_id(*args)
    assert first == session_id(*args)
    assert SYNTHETIC_TRADERS[0] not in first
    assert first.startswith("TS-20260818-")


def test_a_different_trader_gets_a_different_session_id() -> None:
    started = datetime(2026, 8, 18, 7, tzinfo=timezone.utc)
    assert session_id(TODAY, "zephyr", TaskId.MORNING_BRIEF, started) != session_id(
        TODAY, "quartz", TaskId.MORNING_BRIEF, started
    )


def test_the_aggregate_projection_of_a_session_builds_no_private_key() -> None:
    payload = session(issues=(issue(),), lookups=(lookup(),)).to_dict(audience=AUDIENCE_AGGREGATE)
    for name in PRIVATE_FIELD_NAMES:
        assert name not in payload
    assert payload["task"] == TaskId.MORNING_BRIEF.value


def test_the_private_projection_of_a_session_keeps_everything() -> None:
    payload = session().to_dict(audience=AUDIENCE_PRIVATE)
    assert payload["trader"] == SYNTHETIC_TRADERS[0]
    assert MARK in payload["decision"]


# --- ReleaseStamp ---------------------------------------------------------
def test_a_stamp_taken_against_a_dirty_tree_is_never_reproducible() -> None:
    assert CLEAN_STAMP.is_reproducible
    assert not DIRTY_STAMP.is_reproducible


def test_two_stamps_match_only_on_both_code_and_data() -> None:
    from dataclasses import replace

    assert CLEAN_STAMP.matches(replace(CLEAN_STAMP, captured_at=datetime(2020, 1, 1, tzinfo=timezone.utc)))
    assert not CLEAN_STAMP.matches(replace(CLEAN_STAMP, data_fingerprint="a" * 64))


# --- DayObservation -------------------------------------------------------
def test_availability_is_a_share_of_the_layers_that_were_expected() -> None:
    day = day_observation(expected_layers=12, available_layers=9)
    assert day.source_availability == pytest.approx(0.75)


def test_a_day_cannot_report_more_layers_available_than_expected() -> None:
    with pytest.raises(TrialError):
        day_observation(expected_layers=5, available_layers=6)


def test_an_edition_cannot_be_current_without_having_been_published() -> None:
    with pytest.raises(TrialError):
        day_observation(published=False, current=True)


def test_deployment_is_only_ok_when_the_edition_is_current() -> None:
    assert day_observation(published=True, current=True).deployment_ok
    assert not day_observation(published=True, current=False).deployment_ok


def test_a_drill_day_is_marked_so_it_can_leave_the_reliability_metrics() -> None:
    assert day_observation(drill="critical_source_outage").is_drill
    assert not day_observation().is_drill


def test_a_days_operator_note_does_not_reach_the_aggregate_projection() -> None:
    payload = day_observation(note=f"{MARK} private note").to_dict(audience=AUDIENCE_AGGREGATE)
    assert "note" not in payload
    assert payload["critical_layers_available"] == 12


def test_a_release_stamp_without_a_trust_edition_says_so_rather_than_inventing_one() -> None:
    # The static site is not built through trust.edition, so there is no edition
    # id to inherit. None is the honest answer; a synthesised id would be a lie
    # that looked exactly like a fact.
    assert CLEAN_STAMP.edition_id is None
    assert CLEAN_STAMP.to_dict()["edition_id"] is None


def test_a_day_observation_needs_a_positive_expected_layer_count() -> None:
    with pytest.raises(TrialError):
        DayObservation(
            trading_day=date(2026, 8, 18),
            edition_published=True,
            edition_current=True,
            critical_layers_expected=0,
            critical_layers_available=0,
            release=CLEAN_STAMP,
        )


def test_correctness_issues_are_reported_separately_from_every_other_kind() -> None:
    record = session(
        issues=(issue(IssueClass.WORKFLOW_FRICTION), issue(IssueClass.STALE_DATA, Severity.MAJOR))
    )
    assert len(record.correctness_issues) == 1
    assert record.had_wrong_or_stale
    assert record.worst_severity is Severity.MAJOR
