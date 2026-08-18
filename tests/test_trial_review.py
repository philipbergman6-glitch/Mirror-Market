"""Promotion to backlog, the weekly go/no-go, and the 30-day scorecard.

The through-line of every test here is that a verdict must be *pointable at*.
A trial that reports "hold" without naming which metric held, or an "A-" whose
overall was averaged over four dimensions out of nine without saying so, is a
number people either trust blindly or ignore — and both failures look identical
from outside. So the assertions below are mostly about what the output refuses
to average, refuses to grade, and refuses to round away.
"""

from __future__ import annotations

from datetime import date, timedelta

import pytest

from analysis.trial.backlog import (
    BacklogItem,
    backlog_markdown,
    draft_backlog,
    finding_key,
    issue_body,
    suggested_acceptance_criteria,
)
from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    IssueClass,
    Outcome,
    Severity,
    TaskId,
    TrialError,
)
from analysis.trial.review import (
    DIMENSION_SOURCES,
    VERDICT_GO,
    VERDICT_INSUFFICIENT,
    VERDICT_NO_GO,
    Recommendation,
    Scorecard,
    ScorecardDimension,
    TrendEntry,
    grade_letter,
    review_markdown,
    scorecard,
    scorecard_markdown,
    weekly_review,
)
from tests.trial_fixtures import (
    BLOCKER_ISSUE,
    MARK,
    NUMERICAL_ISSUE,
    SYNTHETIC_TRADERS,
    TODAY,
    WINDOW_START,
    day_observation,
    full_window,
    issue,
    lookup,
    session,
    trading_days,
)


# --- finding identity -----------------------------------------------------
def test_the_same_complaint_at_two_price_levels_is_one_finding() -> None:
    # Digits are stripped from the key, so "reads 3 USD/MT above" and "reads 5
    # USD/MT above" group. Otherwise the same defect is re-reported every day at
    # a new number and never accumulates the recurrence that promotes it.
    first = issue(IssueClass.NUMERICAL_ERROR, Severity.MAJOR, summary="FOB reads 3 USD/MT high")
    second = issue(IssueClass.NUMERICAL_ERROR, Severity.MAJOR, summary="FOB reads 5 USD/MT high!")
    assert finding_key(first) == finding_key(second)


def test_the_same_words_on_two_pages_are_two_findings() -> None:
    here = issue(page="origins.html")
    there = issue(page="markets/cbot.html")
    assert finding_key(here) != finding_key(there)


def test_the_finding_key_ignores_severity_so_a_disagreement_does_not_split_it() -> None:
    mild = issue(IssueClass.WORKFLOW_FRICTION, Severity.MINOR)
    worse = issue(IssueClass.WORKFLOW_FRICTION, Severity.MAJOR)
    assert finding_key(mild) == finding_key(worse)


# --- promotion ------------------------------------------------------------
def test_a_single_minor_sighting_stays_an_observation_and_says_why() -> None:
    backlog = draft_backlog([session(issues=(issue(),))])
    assert not backlog.items
    assert backlog.observations[0].reason_not_promoted.strip()


def test_a_blocker_is_promoted_on_its_first_sighting() -> None:
    backlog = draft_backlog([session(issues=(BLOCKER_ISSUE,), outcome=Outcome.BLOCKED, would_act=False)])
    assert len(backlog.items) == 1
    assert "blocker" in backlog.items[0].promotion_rules


def test_a_correctness_issue_is_promoted_on_its_first_sighting() -> None:
    backlog = draft_backlog([session(issues=(NUMERICAL_ISSUE,))])
    assert "correctness" in backlog.items[0].promotion_rules


def test_a_second_sighting_promotes_on_recurrence() -> None:
    sessions = [session(issues=(issue(),)), session(hour=8, issues=(issue(),))]
    item = draft_backlog(sessions).items[0]
    assert "recurrence" in item.promotion_rules
    assert item.occurrences == 2


def test_a_second_trader_promotes_on_corroboration() -> None:
    sessions = [
        session(trader=SYNTHETIC_TRADERS[0], issues=(issue(),)),
        session(trader=SYNTHETIC_TRADERS[1], hour=8, issues=(issue(),)),
    ]
    item = draft_backlog(sessions).items[0]
    assert "corroborated" in item.promotion_rules
    assert item.trader_count == 2


def test_the_worst_severity_anyone_assigned_wins() -> None:
    # Two traders disagreeing about how bad a defect is means at least one of
    # them was blocked by it.
    sessions = [
        session(issues=(issue(IssueClass.WORKFLOW_FRICTION, Severity.MINOR),)),
        session(
            trader=SYNTHETIC_TRADERS[1],
            hour=8,
            issues=(issue(IssueClass.WORKFLOW_FRICTION, Severity.MAJOR),),
        ),
    ]
    assert draft_backlog(sessions).items[0].severity is Severity.MAJOR


def test_a_blocker_seen_once_outranks_a_minor_issue_seen_six_times() -> None:
    # A blended priority score would invert this, and no desk would accept the
    # ordering once they saw what produced it.
    sessions = [session(hour=7 + i, issues=(issue(),)) for i in range(6)]
    sessions.append(
        session(hour=14, issues=(BLOCKER_ISSUE,), outcome=Outcome.BLOCKED, would_act=False)
    )
    items = draft_backlog(sessions).items
    assert items[0].severity is Severity.BLOCKER


def test_every_promoted_item_carries_evidence_a_decision_and_a_criterion() -> None:
    for item in draft_backlog(full_window()[0]).items:
        assert item.evidence
        assert item.affected_decision.strip()
        assert item.acceptance_criteria.strip()
        assert item.promotion_rules


def test_a_drafted_criterion_is_marked_as_drafted_until_a_human_owns_it() -> None:
    item = draft_backlog([session(issues=(NUMERICAL_ISSUE,))]).items[0]
    assert item.criteria_are_drafted
    owned = item.with_criteria("SYNTHETIC: the origins FOB matches the AMS PDF to the cent")
    assert not owned.criteria_are_drafted


def test_a_blank_criterion_is_refused() -> None:
    item = draft_backlog([session(issues=(NUMERICAL_ISSUE,))]).items[0]
    with pytest.raises(TrialError, match="blank"):
        item.with_criteria("   ")


def test_every_issue_class_has_a_suggested_acceptance_criterion() -> None:
    for kind in IssueClass:
        assert suggested_acceptance_criteria(issue(kind, Severity.MAJOR)).strip()


def test_an_item_built_without_evidence_is_refused() -> None:
    # This workflow promotes *validated* findings; a claim with nothing behind
    # it has not been validated.
    with pytest.raises(TrialError, match="evidence"):
        BacklogItem(
            key="BL-0000000000",
            classification=IssueClass.WORKFLOW_FRICTION,
            severity=Severity.MINOR,
            summary="s",
            affected_decision="d",
            acceptance_criteria="c",
            promotion_rules=("recurrence",),
        )


def test_reproducible_occurrences_counts_only_sightings_on_a_clean_build() -> None:
    from tests.trial_fixtures import CLEAN_STAMP, DIRTY_STAMP

    sessions = [
        session(issues=(NUMERICAL_ISSUE,), release=CLEAN_STAMP),
        session(hour=8, issues=(NUMERICAL_ISSUE,), release=DIRTY_STAMP),
    ]
    item = draft_backlog(sessions).items[0]
    assert item.occurrences == 2
    assert item.reproducible_occurrences == 1


def test_the_backlog_markdown_renders_privately_without_clearance() -> None:
    backlog = draft_backlog([session(issues=(NUMERICAL_ISSUE,))])
    assert MARK in backlog_markdown(backlog)


def test_the_issue_body_names_the_rule_that_promoted_it() -> None:
    body = issue_body(draft_backlog([session(issues=(NUMERICAL_ISSUE,))]).items[0])
    assert "correctness" in body


# --- trend ----------------------------------------------------------------
def test_a_falling_lookup_count_reads_as_better_and_a_falling_rate_as_worse() -> None:
    # Reporting a raw delta and leaving the reader to remember which way each
    # metric runs is how a degrading trial reads as an improving one.
    lookups = TrendEntry("external_lookups_per_task", "L", 1.0, 2.0, lower_is_better=True)
    completion = TrendEntry("task_completion_rate", "C", 0.7, 0.9)
    assert lookups.direction == "better"
    assert completion.direction == "worse"


def test_the_first_week_has_no_previous_and_reports_new_rather_than_flat() -> None:
    assert TrendEntry("k", "L", 0.5, None).direction == "new"
    assert TrendEntry("k", "L", None, 0.5).direction == "lost"
    assert TrendEntry("k", "L", 0.5, 0.5).direction == "flat"


def test_a_review_with_no_previous_window_is_marked_as_the_first_week() -> None:
    sessions, days = full_window()
    assert weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY).is_first_week


# --- verdict --------------------------------------------------------------
def test_a_healthy_full_window_reaches_go() -> None:
    sessions, days = full_window()
    review = weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY)
    assert review.verdict == VERDICT_GO
    assert review.verdict_reason.strip()


def test_one_open_blocker_overrides_every_healthy_metric() -> None:
    # An average cannot express "a trader could size a real trade wrongly off
    # this surface", so this is an override rather than a weighting.
    sessions, days = full_window()
    sessions.append(
        session(hour=13, issues=(BLOCKER_ISSUE,), outcome=Outcome.BLOCKED, would_act=False)
    )
    review = weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY)
    assert review.verdict == VERDICT_NO_GO
    assert "blocker" in review.verdict_reason


def test_a_single_trader_cannot_produce_a_verdict_however_many_sessions() -> None:
    sessions = [session(hour=7 + (i % 10), trading_day=TODAY - timedelta(days=i)) for i in range(30)]
    review = weekly_review(sessions, [], week_start=TODAY - timedelta(days=40), week_end=TODAY)
    assert review.verdict == VERDICT_INSUFFICIENT
    assert "trader" in review.verdict_reason


def test_a_thin_window_is_insufficient_rather_than_go() -> None:
    sessions = [session(), session(trader=SYNTHETIC_TRADERS[1], hour=8)]
    review = weekly_review(sessions, [], week_start=TODAY, week_end=TODAY)
    assert review.verdict == VERDICT_INSUFFICIENT


def test_the_verdict_reason_names_the_metrics_that_held_or_failed() -> None:
    sessions, days = full_window()
    # Make every session need Bloomberg twice: the displacement metric fails.
    sessions = [
        session(
            trader=s.trader,
            task=s.task,
            trading_day=s.trading_day,
            hour=s.started_at.hour,
            lookups=(lookup(), lookup()),
        )
        for s in sessions
    ]
    review = weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY)
    assert review.verdict in (VERDICT_NO_GO, "hold")
    assert "external_lookups_per_task" in review.verdict_reason


# --- review content -------------------------------------------------------
def test_the_review_reports_only_what_the_records_show() -> None:
    sessions, days = full_window()
    review = weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY)
    assert review.worked
    assert review.session_count == len(sessions)
    assert review.trader_count == len(SYNTHETIC_TRADERS)


def test_unmet_questions_are_ranked_by_how_often_they_were_asked_elsewhere() -> None:
    asked = lookup(question=f"{MARK} what is the Nov Santos freight rate?", answer_found=True)
    sessions = [session(hour=7 + i, lookups=(asked,)) for i in range(3)]
    review = weekly_review(sessions, [], week_start=TODAY, week_end=TODAY)
    assert review.unmet_questions
    assert review.unmet_questions[0][1] == 3


def test_a_recommendation_quoting_a_question_is_withheld_from_the_aggregate() -> None:
    # The most valuable recommendations are the most disclosing: a question like
    # "can we still ship Nov from Santos" names a position by implication.
    private = Recommendation(text="quote: SYNTHETIC ship Nov from Santos?")
    assert private.to_dict()["text"].startswith("quote:")
    assert private.to_dict(audience=AUDIENCE_AGGREGATE) is None


def test_a_recommendation_with_a_shareable_form_survives_the_aggregate() -> None:
    both = Recommendation(text="quote: SYNTHETIC detail", shareable="2 freight questions unanswered")
    assert both.to_dict(audience=AUDIENCE_AGGREGATE) == {"text": "2 freight questions unanswered"}


def test_a_blank_recommendation_is_refused() -> None:
    with pytest.raises(TrialError, match="text"):
        Recommendation(text="  ")


def test_the_aggregate_review_reports_how_many_recommendations_it_withheld() -> None:
    # Silently dropping them would read as "there were no recommendations".
    sessions, days = full_window()
    payload = weekly_review(
        sessions, days, week_start=WINDOW_START, week_end=TODAY
    ).to_dict(audience=AUDIENCE_AGGREGATE)
    assert "recommendations_withheld" in payload
    assert "unmet_question_count" in payload
    assert "unmet_questions" not in payload
    assert "failures" not in payload


def test_the_review_markdown_renders_for_a_reader() -> None:
    sessions, days = full_window()
    text = review_markdown(weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY))
    assert "Verdict" in text or "verdict" in text


# --- scorecard ------------------------------------------------------------
def test_the_scorecard_scores_the_nine_rubric_dimensions() -> None:
    import config

    sessions, days = full_window()
    card = scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY)
    assert {dim.key for dim in card.dimensions} == set(config.TRIAL_SCORECARD_DIMENSIONS)


def test_every_dimension_states_the_arithmetic_behind_its_score() -> None:
    sessions, days = full_window()
    for dim in scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY).dimensions:
        assert dim.source == DIMENSION_SOURCES[dim.key]
        if dim.score is not None:
            assert dim.basis, f"{dim.key} scored with no basis"


def test_a_scored_dimension_without_a_basis_is_refused() -> None:
    # An unexplained number on a rubric is a judgement wearing a score's clothes.
    with pytest.raises(TrialError, match="basis"):
        ScorecardDimension(key="precision", score=90.0, observations=20, source="s")


def test_a_score_outside_the_scale_is_refused() -> None:
    with pytest.raises(TrialError, match="0-100"):
        ScorecardDimension(key="precision", score=140.0, observations=20, source="s", basis=("b",))


def test_an_ungraded_dimension_is_left_out_of_the_overall_rather_than_filled_in() -> None:
    # Scoring it zero or fifty would both be inventions.
    dims = (
        ScorecardDimension(key="a", score=90.0, observations=20, source="s", basis=("b",)),
        ScorecardDimension(key="b", score=10.0, observations=1, source="s", basis=("b",)),
    )
    card = Scorecard(
        window_start=WINDOW_START,
        window_end=TODAY,
        dimensions=dims,
        session_count=20,
        trader_count=2,
        day_count=20,
        trading_days_covered=20,
        verdict=VERDICT_GO,
        verdict_reason="r",
    )
    assert card.overall == pytest.approx(90.0)
    assert len(card.graded_dimensions) == 1


def test_the_overall_says_how_many_of_the_nine_it_was_computed_from() -> None:
    sessions, days = full_window()
    payload = scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY).to_dict()
    assert payload["dimension_count"] == 9
    assert payload["graded_count"] <= 9


def test_a_window_short_of_thirty_trading_days_is_not_a_complete_scorecard() -> None:
    sessions, days = full_window()
    card = scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY)
    assert card.trading_days_covered == len(trading_days())
    assert not card.is_complete  # the fixture window is 22 trading days, not 30


def test_a_complete_window_reports_complete() -> None:
    start = TODAY - timedelta(days=60)
    days_list = [d for d in _weekdays(start, TODAY)][-32:]
    sessions = [
        session(trader=trader, trading_day=day, hour=7 + offset)
        for day in days_list
        for offset, trader in enumerate(SYNTHETIC_TRADERS)
    ]
    observations = [day_observation(trading_day=day) for day in days_list]
    card = scorecard(sessions, observations, window_start=days_list[0], window_end=TODAY)
    assert card.trading_days_covered >= 30
    assert card.is_complete


def _weekdays(start: date, end: date) -> list[date]:
    out, cursor = [], start
    while cursor <= end:
        if cursor.weekday() < 5:
            out.append(cursor)
        cursor += timedelta(days=1)
    return out


def test_a_dimension_with_no_sessions_of_its_kind_is_insufficient_not_zero() -> None:
    # Nobody ran a futures task, so futures usefulness was not measured. Scoring
    # it zero would say the futures surface failed.
    sessions = [
        session(trader=t, task=TaskId.MORNING_BRIEF, trading_day=TODAY - timedelta(days=i), hour=7 + o)
        for i in range(12)
        for o, t in enumerate(SYNTHETIC_TRADERS)
    ]
    card = scorecard(sessions, [], window_start=TODAY - timedelta(days=30), window_end=TODAY)
    futures = next(dim for dim in card.dimensions if dim.key == "futures_usefulness")
    assert futures.score is None
    assert futures.letter == "n/a"


def test_the_grade_letters_run_the_whole_rubric() -> None:
    assert grade_letter(None) == "n/a"
    assert grade_letter(95) == "A"
    assert grade_letter(91) == "A-"
    assert grade_letter(84) == "B"
    assert grade_letter(71) == "C-"
    assert grade_letter(20) == "F"


def test_the_scorecard_markdown_marks_a_computed_but_ungraded_score() -> None:
    # A number printed beside an n/a grade reads as a result unless it is marked.
    sessions = [session(hour=7 + i) for i in range(3)]
    text = scorecard_markdown(scorecard(sessions, [], window_start=TODAY, window_end=TODAY))
    assert "*" in text
