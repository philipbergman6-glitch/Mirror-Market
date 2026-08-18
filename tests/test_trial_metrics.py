"""The thirteen trial metrics, their denominators, and their refusal to grade.

Two failure modes are being guarded against here, and they are opposites. The
first is a metric that grades a number it does not have enough of — a go/no-go
read off eight sessions, which looks measured and is not. The second is a metric
that reports ``insufficient`` when it genuinely has no number at all, which reads
as "thin" when the truth is "undefined". Every test below pins one or the other.
"""

from __future__ import annotations

import pytest

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    ExternalTool,
    IssueClass,
    Outcome,
    Severity,
    TaskId,
    TrialError,
)
from analysis.trial.metrics import (
    STATUS_GO,
    STATUS_HOLD,
    STATUS_INSUFFICIENT,
    STATUS_NO_GO,
    STATUS_REPORTED,
    Metric,
    compute_metrics,
)
from tests.trial_fixtures import (
    CLEAN_STAMP,
    DIRTY_STAMP,
    SYNTHETIC_TRADERS,
    day_observation,
    full_window,
    issue,
    lookup,
    session,
)

BARS = {"demo": {"go": 0.9, "no_go": 0.6}}
LOW_BARS = {"demo": {"go": 0.1, "no_go": 0.4}}


def _metric(value: float | None, observations: int = 20, **kw: object) -> Metric:
    defaults: dict[str, object] = {
        "key": "demo",
        "label": "Demo",
        "value": value,
        "unit": "rate",
        "observations": observations,
        "note": "a stated arithmetic",
        "go": 0.9,
        "no_go": 0.6,
        "min_observations": 10,
    }
    defaults.update(kw)
    return Metric(**defaults)  # type: ignore[arg-type]


# --- Metric ---------------------------------------------------------------
def test_a_metric_without_a_note_is_refused() -> None:
    # The note is what makes the number checkable against its own denominator.
    with pytest.raises(TrialError, match="note"):
        _metric(0.5, note="   ")


def test_a_metric_cannot_carry_a_negative_denominator() -> None:
    with pytest.raises(TrialError, match="denominator"):
        _metric(0.5, observations=-1)


def test_a_higher_is_better_metric_grades_on_both_sides_of_its_bars() -> None:
    assert _metric(0.95).status == STATUS_GO
    assert _metric(0.75).status == STATUS_HOLD
    assert _metric(0.60).status == STATUS_NO_GO
    assert _metric(0.10).status == STATUS_NO_GO


def test_a_lower_is_better_metric_grades_the_other_way_round() -> None:
    low = {"lower_is_better": True, "go": 0.1, "no_go": 0.4}
    assert _metric(0.05, **low).status == STATUS_GO  # type: ignore[arg-type]
    assert _metric(0.25, **low).status == STATUS_HOLD  # type: ignore[arg-type]
    assert _metric(0.40, **low).status == STATUS_NO_GO  # type: ignore[arg-type]


def test_a_thin_denominator_refuses_to_grade_even_with_a_perfect_value() -> None:
    # A 100% completion rate over three sessions is not a go.
    assert _metric(1.0, observations=3).status == STATUS_INSUFFICIENT


def test_an_undefined_metric_is_insufficient_rather_than_zero() -> None:
    # None means "we did not measure this". Rendering it as 0.0 would state a
    # result — the worst possible answer, because it grades as a hard no_go.
    metric = _metric(None, observations=40)
    assert metric.status == STATUS_INSUFFICIENT
    assert metric.display == "—"


def test_a_metric_with_no_bar_is_reported_and_never_graded() -> None:
    metric = _metric(42.0, unit="minutes", go=None, no_go=None)
    assert not metric.has_bar
    assert metric.status == STATUS_REPORTED


def test_the_display_string_matches_the_unit_it_declares() -> None:
    assert _metric(0.734).display == "73%"
    assert _metric(12.4, unit="minutes").display == "12 min"
    assert _metric(4.0, unit="score").display == "4.0"
    assert _metric(1.25, unit="count").display == "1.25"


# --- compute_metrics ------------------------------------------------------
def test_the_full_window_produces_the_thirteen_required_metrics() -> None:
    sessions, days = full_window()
    computed = compute_metrics(sessions, days)
    keys = {m.key for m in computed.metrics}
    assert keys == {
        "task_completion_rate",
        "median_completion_minutes",
        "external_lookups_per_task",
        "wrong_or_stale_rate",
        "false_alert_rate",
        "missed_alert_rate",
        "would_act_rate",
        "median_confidence",
        "opportunity_conversion",
        "feature_coverage_rate",
        "critical_source_availability",
        "deployment_reliability",
        "unreproducible_finding_rate",
    }


def test_completion_rate_counts_only_sessions_that_reached_an_output() -> None:
    sessions = [
        session(),
        session(hour=8),
        session(hour=9, outcome=Outcome.BLOCKED, would_act=False, issues=(issue(),)),
        session(hour=10, outcome=Outcome.ABANDONED, would_act=False, issues=(issue(),)),
    ]
    metric = compute_metrics(sessions).get("task_completion_rate")
    assert metric.value == pytest.approx(0.5)
    assert metric.observations == 4


def test_external_lookups_are_counted_per_completed_task_not_per_session() -> None:
    # A blocked session's lookups are still lookups, but the rate a desk reads is
    # "how often did a finished task still need Bloomberg", so the denominator is
    # completed tasks.
    sessions = [
        session(lookups=(lookup(), lookup(ExternalTool.BROKER))),
        session(hour=8, lookups=(lookup(),)),
    ]
    metric = compute_metrics(sessions).get("external_lookups_per_task")
    assert metric.value == pytest.approx(1.5)
    assert metric.observations == 2


def test_an_upstream_outage_does_not_raise_the_wrong_answer_rate() -> None:
    # The source being down is measured as availability. Counting it here would
    # make an honest outage look like this product computing a wrong number.
    outage = session(issues=(issue(IssueClass.UPSTREAM_OUTAGE, Severity.MAJOR),))
    wrong = session(hour=8, issues=(issue(IssueClass.NUMERICAL_ERROR, Severity.MAJOR),))
    metric = compute_metrics([outage, wrong]).get("wrong_or_stale_rate")
    assert metric.value == pytest.approx(0.5)


def test_alert_rates_count_sessions_not_alerts() -> None:
    # Two false alerts in one session is one session that saw noise, not two.
    noisy = session(
        issues=(
            issue(IssueClass.FALSE_ALERT, Severity.MAJOR),
            issue(IssueClass.FALSE_ALERT, Severity.MAJOR, summary="SYNTHETIC second"),
        )
    )
    computed = compute_metrics([noisy, session(hour=8)])
    assert computed.get("false_alert_rate").value == pytest.approx(0.5)
    assert computed.get("missed_alert_rate").value == pytest.approx(0.0)


def test_confidence_is_reported_as_a_median_with_its_own_distribution() -> None:
    sessions = [session(hour=7 + i, confidence=c) for i, c in enumerate([1, 4, 4, 5, 5])]
    metric = compute_metrics(sessions).get("median_confidence")
    assert metric.value == pytest.approx(4.0)
    assert dict(metric.detail)["5"] == 2.0
    assert dict(metric.detail)["1"] == 1.0


def test_opportunity_conversion_is_taken_over_worked_not_detected_rows() -> None:
    # Otherwise the rate is a function of how many rows the detectors emitted.
    metric = compute_metrics(
        [session()], worked_opportunities=4, progressed_opportunities=1
    ).get("opportunity_conversion")
    assert metric.value == pytest.approx(0.25)
    assert metric.observations == 4


def test_opportunity_conversion_with_nothing_worked_is_undefined_not_zero() -> None:
    metric = compute_metrics([session()]).get("opportunity_conversion")
    assert metric.value is None


def test_feature_coverage_is_taken_over_the_promotion_contracts_own_page_list() -> None:
    # Hardcoding the list would quietly report 100% coverage of a site that grew.
    from trust.site_promotion import expected_site_paths

    published = tuple(expected_site_paths())
    metric = compute_metrics([session(pages=(published[0],))]).get("feature_coverage_rate")
    assert metric.observations == len(published)
    assert metric.value == pytest.approx(1 / len(published))


def test_coverage_does_not_grade_itself_on_a_denominator_of_pages_alone() -> None:
    # Its denominator is the site's page count, which is 13 the moment the trial
    # opens — so without a separate sufficiency basis it clears the observation
    # floor with zero sessions behind it and grades a hard no_go.
    thin = compute_metrics([session()]).get("feature_coverage_rate")
    assert thin.status == STATUS_INSUFFICIENT
    assert thin.observations > 10  # the floor it would otherwise have cleared


def test_a_page_nobody_opened_is_absent_from_usage_rather_than_recorded_as_zero() -> None:
    computed = compute_metrics([session(pages=("index.html", "index.html"))])
    usage = dict(computed.page_usage)
    # Opened twice in one session is one session that used it.
    assert usage == {"index.html": 1}


def test_availability_is_the_mean_across_observed_days() -> None:
    days = [
        day_observation(expected_layers=10, available_layers=10),
        day_observation(expected_layers=10, available_layers=8),
    ]
    metric = compute_metrics([], days).get("critical_source_availability")
    assert metric.value == pytest.approx(0.9)
    assert metric.observations == 2


def test_a_published_but_stale_edition_counts_as_a_deployment_failure() -> None:
    # It deployed perfectly and answered the wrong day.
    days = [day_observation(published=True, current=True), day_observation(published=True, current=False)]
    assert compute_metrics([], days).get("deployment_reliability").value == pytest.approx(0.5)


def test_findings_taken_against_a_dirty_tree_are_counted_as_unreproducible() -> None:
    sessions = [session(release=CLEAN_STAMP), session(hour=8, release=DIRTY_STAMP)]
    assert compute_metrics(sessions).get("unreproducible_finding_rate").value == pytest.approx(0.5)


def test_every_task_appears_in_the_task_counts_even_at_zero() -> None:
    # A task nobody ran is the finding. Omitting it hides that.
    counts = dict(compute_metrics([session(task=TaskId.MORNING_BRIEF)]).task_counts)
    assert len(counts) == len(list(TaskId))
    assert counts[TaskId.MORNING_BRIEF.value] == 1
    assert counts[TaskId.FAILURE_DRILL.value] == 0


def test_every_issue_class_appears_in_the_issue_counts_even_at_zero() -> None:
    counts = dict(compute_metrics([session(issues=(issue(),))]).issue_counts)
    assert len(counts) == len(list(IssueClass))
    assert counts[IssueClass.MISLEADING_UX.value] == 1
    assert counts[IssueClass.NUMERICAL_ERROR.value] == 0


def test_traders_are_counted_case_insensitively_so_one_handle_is_one_trader() -> None:
    sessions = [session(trader="Zephyr"), session(hour=8, trader="zephyr ")]
    assert compute_metrics(sessions).trader_count == 1


def test_an_empty_window_computes_without_raising_and_grades_nothing() -> None:
    computed = compute_metrics([], [])
    assert computed.session_count == 0
    assert all(m.status == STATUS_INSUFFICIENT for m in computed.metrics)
    assert computed.graded == ()


def test_asking_for_a_metric_that_does_not_exist_raises_rather_than_returning_none() -> None:
    with pytest.raises(TrialError, match="no metric named"):
        compute_metrics([session()]).get("invented_metric")


def test_the_aggregate_projection_of_a_metric_set_carries_no_free_text_from_a_trader() -> None:
    sessions, days = full_window()
    payload = compute_metrics(sessions, days).to_dict(audience=AUDIENCE_AGGREGATE)
    blob = repr(payload)
    for trader in SYNTHETIC_TRADERS:
        assert trader not in blob
    assert "SYNTHETIC" not in blob


def test_a_metric_projection_refuses_an_unknown_audience() -> None:
    with pytest.raises(TrialError, match="audience"):
        _metric(0.5).to_dict(audience="public-ish")
