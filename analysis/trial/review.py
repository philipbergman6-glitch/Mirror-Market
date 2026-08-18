"""The weekly read and the 30-day verdict (Phase 5, requirements 8 and 10).

Two outputs, one discipline: **every number shown here traces to records a human
entered, and a dimension with nothing behind it scores nothing at all.**

Requirement 8 asks a weekly review for what worked, what failed, the top unmet
questions, the metrics trend, recommended changes, and a go/no-go for broader
use. Requirement 10 asks a final 30-day scorecard on the same strict rubric the
earlier audits of this project used: precision, accuracy, reliability,
timeliness, physical usefulness, futures usefulness, opportunity usefulness, UX,
trader trust.

Four rules hold both together.

**An ungraded dimension is not a pass.** Where the window carries too few
observations, the dimension scores ``None`` and grades ``insufficient`` — never
a default, never the midpoint, and never quietly dropped from the average. The
overall grade then states how many dimensions it was actually computed from,
because a scorecard averaging four of nine dimensions and printing "B" is worse
than one that admits it does not know.

**Every dimension names its inputs.** Each is a stated arithmetic over named
metrics and issue classes, and carries those inputs in ``basis`` so a trader who
disagrees with a score can check the derivation rather than argue with a letter.
Nothing here is a judgement call rendered as a number.

**A trend needs two windows, and says so.** Comparing this week to nothing and
reporting "stable" would be a fabrication, so a first review reports every metric
as ``new`` rather than flat.

**Go/no-go is decided by the bars in ``config``, not here.** The thresholds live
in ``config.TRIAL_DECISION_THRESHOLDS`` so the answer to "why did it say no-go"
is a lookup. This module applies them and adds exactly two overrides a metric
average cannot express: an open blocker is a no-go whatever the rates say, and a
window that has not met its trader or observation minimum is ``insufficient``
rather than a verdict.

Nothing in this module invents a participant, a session, a lookup or a result.
It reads what :mod:`analysis.trial.records` loaded and reports it.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from analysis.trial.backlog import BacklogSet, draft_backlog
from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    DayObservation,
    IssueClass,
    SessionRecord,
    Severity,
    TaskId,
    TrialError,
)
from analysis.trial.metrics import (
    STATUS_GO,
    STATUS_HOLD,
    STATUS_INSUFFICIENT,
    STATUS_NO_GO,
    MetricSet,
    compute_metrics,
)

__all__ = [
    "DIMENSION_SOURCES",
    "ScorecardDimension",
    "Scorecard",
    "Recommendation",
    "TrendEntry",
    "WeeklyReview",
    "grade_letter",
    "review_markdown",
    "scorecard",
    "scorecard_markdown",
    "weekly_review",
]

#: The verdicts a review can reach. ``INSUFFICIENT`` is a first-class answer, not
#: an error: a fortnight with one trader and six sessions has not earned a
#: go/no-go, and forcing one would be the single most misleading thing this
#: module could do.
VERDICT_GO = "go"
VERDICT_HOLD = "hold"
VERDICT_NO_GO = "no_go"
VERDICT_INSUFFICIENT = "insufficient"

#: Which tasks exercise which half of the product. Used only by the three
#: usefulness dimensions, and stated here rather than inferred from a task's name
#: so that adding a task forces a decision about what it evidences.
_PHYSICAL_TASKS = (
    TaskId.ORIGIN_COMPARISON,
    TaskId.CHINA_RECONCILIATION,
    TaskId.WEATHER_RESPONSE,
    TaskId.MORNING_BRIEF,
)
_FUTURES_TASKS = (
    TaskId.CRUSH_HEDGE,
    TaskId.TICKET_REVIEW,
    TaskId.WASDE_EVENT,
    TaskId.PRICE_AUDIT,
)
_OPPORTUNITY_TASKS = (TaskId.COUNTERPARTY_ID,)

#: What each scorecard dimension is computed from. Rendered on the scorecard so
#: the derivation is inspectable beside the score.
DIMENSION_SOURCES: dict[str, str] = {
    "precision": (
        "share of sessions free of a numerical error, i.e. a figure that did not reproduce "
        "its source"
    ),
    "accuracy": (
        "share of sessions free of any correctness issue — wrong number, wrong meaning, or "
        "data published as current when it was stale"
    ),
    "reliability": (
        "deployment reliability and critical-source availability from the day observations, "
        "averaged; drill days excluded"
    ),
    "timeliness": (
        "share of completed sessions finishing inside the task's own target time, less any "
        "session that reported stale data"
    ),
    "physical_usefulness": (
        "completion and would-act rate across the physical tasks (origin comparison, China "
        "reconciliation, weather response, morning brief)"
    ),
    "futures_usefulness": (
        "completion and would-act rate across the futures tasks (crush and hedge, ticket "
        "review, WASDE response, price audit)"
    ),
    "opportunity_usefulness": (
        "completion and would-act rate on the counterparty task, and the share of worked "
        "opportunities that progressed"
    ),
    "ux": (
        "share of sessions free of a misleading-UX or workflow-friction issue, and the "
        "external lookups those sessions still needed"
    ),
    "trader_trust": (
        "median confidence against the 1-5 scale, and the share of sessions where the trader "
        "would act on the answer"
    ),
}


def grade_letter(score: float | None) -> str:
    """A letter for a 0-100 score, on the rubric the earlier audits of this repo used.

    ``n/a`` for ``None``. There is deliberately no letter that means "we did not
    measure this" other than the absence of one.
    """
    if score is None:
        return "n/a"
    for bound, letter in ((93, "A"), (90, "A-"), (87, "B+"), (83, "B"), (80, "B-"),
                          (77, "C+"), (73, "C"), (70, "C-"), (67, "D+"), (60, "D")):
        if score >= bound:
            return letter
    return "F"


@dataclass(frozen=True)
class TrendEntry:
    """One metric's movement between two windows.

    ``direction`` accounts for the metric's own polarity: a falling
    external-lookup count is ``better``, a falling completion rate is ``worse``.
    Reporting a raw delta and leaving the reader to remember which way each
    metric runs is how a degrading trial reads as an improving one.
    """

    key: str
    label: str
    current: float | None
    previous: float | None
    lower_is_better: bool = False

    @property
    def delta(self) -> float | None:
        if self.current is None or self.previous is None:
            return None
        return self.current - self.previous

    @property
    def direction(self) -> str:
        if self.previous is None:
            return "new"
        if self.current is None:
            return "lost"
        delta = self.current - self.previous
        if abs(delta) < 1e-9:
            return "flat"
        improved = delta < 0 if self.lower_is_better else delta > 0
        return "better" if improved else "worse"

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "current": self.current,
            "previous": self.previous,
            "delta": self.delta,
            "direction": self.direction,
        }


@dataclass(frozen=True)
class Recommendation:
    """A next action, in two forms: the useful one and the shareable one.

    Recommendations are the one output that *must* quote the evidence to be
    worth reading — "this question was asked elsewhere twice and is still
    unanswered here" is actionable only with the question attached. But that
    question is free text a trader typed mid-session, and the most valuable ones
    are the most disclosing: "can we still ship Nov from Santos" names a position
    by implication.

    So a recommendation carries ``text`` (private, quotes freely) and
    ``shareable`` (aggregate, or ``None`` to withhold the item entirely). Splitting
    it at construction rather than filtering at render is deliberate: a filter is
    one forgotten branch away from publishing the private string, which is
    precisely the bug this type was added to fix.
    """

    text: str
    shareable: str | None = None

    def __post_init__(self) -> None:
        if not self.text.strip():
            raise TrialError("a recommendation needs text")

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any] | None:
        if audience == AUDIENCE_PRIVATE:
            return {"text": self.text}
        return None if self.shareable is None else {"text": self.shareable}


@dataclass(frozen=True)
class WeeklyReview:
    """One week's read, and whether the trial should widen.

    Deliberately holds both the metric set and the free-text material. The
    aggregate projection drops the second — unmet questions and issue summaries
    are typed by a trader mid-session and may name a cargo or a counterparty.
    """

    week_start: date
    week_end: date
    metrics: MetricSet
    trend: tuple[TrendEntry, ...] = ()
    worked: tuple[str, ...] = ()
    failures: tuple[str, ...] = ()
    unmet_questions: tuple[tuple[str, int], ...] = ()
    recommendations: tuple[Recommendation, ...] = ()
    backlog: BacklogSet | None = None
    verdict: str = VERDICT_INSUFFICIENT
    verdict_reason: str = ""
    trader_count: int = 0
    session_count: int = 0

    @property
    def is_first_week(self) -> bool:
        return all(entry.previous is None for entry in self.trend)

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "week_start": self.week_start.isoformat(),
            "week_end": self.week_end.isoformat(),
            "verdict": self.verdict,
            "verdict_reason": self.verdict_reason,
            "trader_count": self.trader_count,
            "session_count": self.session_count,
            "metrics": self.metrics.to_dict(audience=audience),
            "trend": [entry.to_dict() for entry in self.trend],
            "worked": list(self.worked),
            "recommendations": [
                rendered
                for rendered in (rec.to_dict(audience=audience) for rec in self.recommendations)
                if rendered is not None
            ],
        }
        if audience != AUDIENCE_PRIVATE:
            payload["recommendations_withheld"] = sum(1 for rec in self.recommendations if rec.shareable is None)
        if self.backlog is not None:
            payload["backlog"] = self.backlog.to_dict(audience=audience)
        if audience == AUDIENCE_PRIVATE:
            payload["failures"] = list(self.failures)
            payload["unmet_questions"] = [{"question": q, "count": n} for q, n in self.unmet_questions]
        else:
            # An aggregate reader learns how many questions went unanswered, and
            # nothing about what they were. The questions are the most sensitive
            # free text in the trial: "can we still ship Nov from Santos" names a
            # position by implication.
            payload["unmet_question_count"] = len(self.unmet_questions)
        return payload


def _in_window(day: date, start: date, end: date) -> bool:
    return start <= day <= end


def _verdict(metrics: MetricSet, backlog: BacklogSet, *, traders: int, sessions: int) -> tuple[str, str]:
    """Apply the config bars, plus the two overrides an average cannot express."""
    import config

    min_traders = getattr(config, "TRIAL_MIN_TRADERS", 2)
    min_obs = getattr(config, "TRIAL_MIN_OBSERVATIONS", 10)
    if traders < min_traders or sessions < min_obs:
        return (
            VERDICT_INSUFFICIENT,
            f"{sessions} session(s) from {traders} trader(s); the protocol asks for at least "
            f"{min_obs} sessions from {min_traders} traders before a verdict is meaningful",
        )

    blockers = backlog.blockers
    if blockers:
        return (
            VERDICT_NO_GO,
            f"{len(blockers)} open blocker(s) — a trader could size a real trade wrongly off "
            f"this surface: {'; '.join(item.key for item in blockers)}",
        )

    graded = [metric for metric in metrics.graded if metric.status != STATUS_INSUFFICIENT]
    if not graded:
        return VERDICT_INSUFFICIENT, "no metric reached its minimum observation count"
    failed = [metric.key for metric in graded if metric.status == STATUS_NO_GO]
    if failed:
        return VERDICT_NO_GO, f"{len(failed)} metric(s) below the no-go bar: {', '.join(failed)}"
    held = [metric.key for metric in graded if metric.status == STATUS_HOLD]
    if held:
        return VERDICT_HOLD, f"{len(held)} metric(s) between the bars: {', '.join(held)}"
    return VERDICT_GO, f"all {len(graded)} graded metric(s) at or above the go bar"


def _what_worked(sessions: Sequence[SessionRecord], metrics: MetricSet) -> tuple[str, ...]:
    """Only things the records actually show. No encouragement."""
    worked: list[str] = []
    completed = [s for s in sessions if s.outcome.is_complete]
    clean = [s for s in completed if not s.issues]
    if clean:
        by_task = Counter(s.task for s in clean)
        best = ", ".join(f"{task.label} ({count})" for task, count in by_task.most_common(3))
        worked.append(f"{len(clean)} session(s) completed with no issue raised at all: {best}")
    no_lookup = [s for s in completed if s.external_lookup_count == 0]
    if no_lookup:
        worked.append(
            f"{len(no_lookup)} of {len(completed)} completed session(s) needed no external "
            "tool — the task was answered inside Mirror Market"
        )
    would_act = [s for s in completed if s.would_act]
    if would_act:
        worked.append(f"the trader would have acted on {len(would_act)} of {len(completed)} completed session(s)")
    for metric in metrics.graded:
        if metric.status == STATUS_GO:
            worked.append(f"{metric.label}: {metric.display} (at or above the go bar)")
    return tuple(worked)


def _failures(sessions: Sequence[SessionRecord], days: Sequence[DayObservation]) -> tuple[str, ...]:
    """What went wrong, worst first. Free text — private only."""
    failures: list[str] = []
    for session in sessions:
        for issue in session.issues:
            if issue.severity is not Severity.MINOR:
                failures.append(
                    f"[{issue.severity.value}] {issue.classification.value} "
                    f"({issue.page or 'no page'}) — {issue.summary}"
                )
    for session in sessions:
        if not session.outcome.is_complete:
            failures.append(f"[{session.outcome.value}] {session.task.label} on {session.trading_day}")
    for day in days:
        if not day.is_drill and not day.deployment_ok:
            failures.append(f"[deployment] {day.trading_day}: no current edition published")
        if not day.is_drill and day.source_availability < 1.0:
            failures.append(
                f"[sources] {day.trading_day}: {day.critical_layers_available} of "
                f"{day.critical_layers_expected} critical layers available"
            )
    return tuple(failures)


def _unmet_questions(sessions: Iterable[SessionRecord]) -> tuple[tuple[str, int], ...]:
    """The questions this product could not answer, most frequent first.

    This is the single most valuable output of the whole trial and the reason
    :class:`~analysis.trial.domain.ExternalLookup` refuses to be constructed
    without one. A lookup count says the trader left; the question says why.
    """
    counter: Counter[str] = Counter()
    for session in sessions:
        for lookup in session.external_lookups:
            counter[lookup.unanswered_question.strip()] += 1
    return tuple(counter.most_common())


def _recommendations(
    metrics: MetricSet, backlog: BacklogSet, unmet: Sequence[tuple[str, int]]
) -> tuple[Recommendation, ...]:
    """Concrete next actions derived from what failed. Never generic advice.

    Each is built with its shareable form decided here, at the point where it is
    known whether the text quotes a trader. A blocker's summary and an unmet
    question both do, so their shareable forms name the item and the count and
    stop there; a metric falling below its bar quotes only numbers this module
    computed, so it shares whole.
    """
    out: list[Recommendation] = []
    for item in backlog.blockers:
        out.append(
            Recommendation(
                text=f"Fix {item.key} before the next session on that surface: {item.summary}",
                shareable=f"Fix {item.key} ({item.classification.value}) before the next session on that surface.",
            )
        )
    for metric in metrics.graded:
        if metric.status == STATUS_NO_GO:
            # Numbers this module computed, and a note it wrote. Nothing a trader
            # typed, so the private and shareable forms are the same string.
            text = f"{metric.label} is below the no-go bar at {metric.display}. {metric.note}"
            out.append(Recommendation(text=text, shareable=text))
    repeated = [(question, count) for question, count in unmet if count > 1]
    for question, count in repeated[:3]:
        out.append(
            Recommendation(
                text=f"Asked elsewhere {count}x and still unanswered here: {question!r} — scope it or state why not.",
                shareable=f"A question was asked elsewhere {count}x and is still unanswered here; text withheld.",
            )
        )
    if backlog.undrafted_count:
        text = (
            f"{backlog.undrafted_count} backlog item(s) still carry a generated acceptance "
            "criterion. Review them before any is worked."
        )
        out.append(Recommendation(text=text, shareable=text))
    return tuple(out)


def weekly_review(
    sessions: Sequence[SessionRecord],
    days: Sequence[DayObservation] = (),
    *,
    week_start: date,
    week_end: date | None = None,
    previous: MetricSet | None = None,
    worked_opportunities: int = 0,
    progressed_opportunities: int = 0,
) -> WeeklyReview:
    """Build one week's review from the records inside its window.

    ``previous`` is the prior window's metric set, and is optional: the first
    review of a trial has no comparison and reports every metric as ``new``
    rather than manufacturing a flat trend.
    """
    end = week_end if week_end is not None else week_start + timedelta(days=6)
    if end < week_start:
        raise TrialError(f"week_end {end} precedes week_start {week_start}")

    window = [s for s in sessions if _in_window(s.trading_day, week_start, end)]
    window_days = [d for d in days if _in_window(d.trading_day, week_start, end)]
    metrics = compute_metrics(
        window,
        window_days,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
    )
    backlog = draft_backlog(window)
    unmet = _unmet_questions(window)
    trend = tuple(
        TrendEntry(
            key=metric.key,
            label=metric.label,
            current=metric.value,
            previous=(previous.get(metric.key).value if previous and previous.get(metric.key) else None),
            lower_is_better=metric.lower_is_better,
        )
        for metric in metrics.metrics
    )
    traders = len({s.trader for s in window})
    verdict, reason = _verdict(metrics, backlog, traders=traders, sessions=len(window))
    return WeeklyReview(
        week_start=week_start,
        week_end=end,
        metrics=metrics,
        trend=trend,
        worked=_what_worked(window, metrics),
        failures=_failures(window, window_days),
        unmet_questions=unmet,
        recommendations=_recommendations(metrics, backlog, unmet),
        backlog=backlog,
        verdict=verdict,
        verdict_reason=reason,
        trader_count=traders,
        session_count=len(window),
    )


# ---------------------------------------------------------------------------
# Requirement 10 — the 30-day scorecard
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ScorecardDimension:
    """One rubric dimension: a score, its grade, and the arithmetic behind it."""

    key: str
    score: float | None
    observations: int
    source: str
    basis: tuple[str, ...] = ()
    min_observations: int = 10

    def __post_init__(self) -> None:
        if self.score is not None and not 0.0 <= self.score <= 100.0:
            raise TrialError(f"{self.key}: score {self.score} is outside 0-100")
        if self.score is not None and not self.basis:
            raise TrialError(
                f"{self.key}: a scored dimension must carry the basis it was computed from — "
                "an unexplained number on a rubric is a judgement wearing a score's clothes"
            )

    @property
    def graded(self) -> bool:
        return self.score is not None and self.observations >= self.min_observations

    @property
    def letter(self) -> str:
        return grade_letter(self.score) if self.graded else "n/a"

    @property
    def status(self) -> str:
        return "graded" if self.graded else STATUS_INSUFFICIENT

    def to_dict(self, **_: Any) -> dict[str, Any]:
        return {
            "key": self.key,
            "score": round(self.score, 1) if self.score is not None else None,
            "letter": self.letter,
            "status": self.status,
            "observations": self.observations,
            "source": self.source,
            "basis": list(self.basis),
        }


@dataclass(frozen=True)
class Scorecard:
    """The 30-day verdict across the nine rubric dimensions."""

    window_start: date
    window_end: date
    dimensions: tuple[ScorecardDimension, ...]
    session_count: int
    trader_count: int
    day_count: int
    trading_days_covered: int
    verdict: str
    verdict_reason: str

    @property
    def graded_dimensions(self) -> tuple[ScorecardDimension, ...]:
        return tuple(dim for dim in self.dimensions if dim.graded)

    @property
    def overall(self) -> float | None:
        """Mean of the *graded* dimensions, or ``None`` if none graded.

        Ungraded dimensions are omitted rather than scored zero or scored fifty.
        Both alternatives would be inventions, and the count of what it was
        computed from is reported beside it so the omission is visible.
        """
        graded = [dim.score for dim in self.graded_dimensions if dim.score is not None]
        return sum(graded) / len(graded) if graded else None

    @property
    def overall_letter(self) -> str:
        return grade_letter(self.overall)

    @property
    def is_complete(self) -> bool:
        """Did the window meet the protocol's own coverage requirement?"""
        import config

        return (
            self.trading_days_covered >= getattr(config, "TRIAL_WINDOW_TRADING_DAYS", 30)
            and self.trader_count >= getattr(config, "TRIAL_MIN_TRADERS", 2)
        )

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        return {
            "window_start": self.window_start.isoformat(),
            "window_end": self.window_end.isoformat(),
            "overall": round(self.overall, 1) if self.overall is not None else None,
            "overall_letter": self.overall_letter,
            "graded_count": len(self.graded_dimensions),
            "dimension_count": len(self.dimensions),
            "is_complete": self.is_complete,
            "trading_days_covered": self.trading_days_covered,
            "session_count": self.session_count,
            "trader_count": self.trader_count,
            "day_count": self.day_count,
            "verdict": self.verdict,
            "verdict_reason": self.verdict_reason,
            "dimensions": [dim.to_dict(audience=audience) for dim in self.dimensions],
        }


def _issue_free_rate(sessions: Sequence[SessionRecord], classes: Iterable[IssueClass]) -> float | None:
    wanted = set(classes)
    if not sessions:
        return None
    clean = sum(1 for s in sessions if not any(issue.classification in wanted for issue in s.issues))
    return 100.0 * clean / len(sessions)


def _task_usefulness(sessions: Sequence[SessionRecord], tasks: Sequence[TaskId]) -> tuple[float | None, int]:
    """Completion and would-act, weighted equally, over one family of tasks."""
    subset = [s for s in sessions if s.task in tasks]
    if not subset:
        return None, 0
    completed = [s for s in subset if s.outcome.is_complete]
    completion = 100.0 * len(completed) / len(subset)
    act = 100.0 * sum(1 for s in subset if s.would_act) / len(subset)
    return (completion + act) / 2, len(subset)


def _metric_value(metrics: MetricSet, key: str) -> float | None:
    metric = metrics.get(key)
    return metric.value if metric is not None else None


def scorecard(
    sessions: Sequence[SessionRecord],
    days: Sequence[DayObservation] = (),
    *,
    window_start: date,
    window_end: date,
    worked_opportunities: int = 0,
    progressed_opportunities: int = 0,
    min_observations: int | None = None,
) -> Scorecard:
    """The final 30-day scorecard.

    Every dimension is arithmetic over the records; none is a judgement. Where
    the window does not carry enough of the right kind of session, the dimension
    is ``insufficient`` and is left out of the overall rather than filled in.
    """
    import config

    floor = min_observations if min_observations is not None else getattr(config, "TRIAL_MIN_OBSERVATIONS", 10)
    window = [s for s in sessions if _in_window(s.trading_day, window_start, window_end)]
    window_days = [d for d in days if _in_window(d.trading_day, window_start, window_end)]
    live_days = [d for d in window_days if not d.is_drill]
    metrics = compute_metrics(
        window,
        window_days,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
        min_observations=floor,
    )
    total = len(window)

    dims: list[ScorecardDimension] = []

    # precision — did the numbers reproduce their sources?
    numerical = sum(len(s.issues_of(IssueClass.NUMERICAL_ERROR)) for s in window)
    precision = _issue_free_rate(window, (IssueClass.NUMERICAL_ERROR,))
    dims.append(
        ScorecardDimension(
            key="precision",
            score=precision,
            observations=total,
            source=DIMENSION_SOURCES["precision"],
            basis=(f"{numerical} numerical error(s) across {total} session(s)",) if precision is not None else (),
            min_observations=floor,
        )
    )

    # accuracy — the wider correctness family
    correctness = sum(len(s.correctness_issues) for s in window)
    accuracy = _issue_free_rate(
        window, (IssueClass.NUMERICAL_ERROR, IssueClass.SEMANTIC_MISMATCH, IssueClass.STALE_DATA)
    )
    dims.append(
        ScorecardDimension(
            key="accuracy",
            score=accuracy,
            observations=total,
            source=DIMENSION_SOURCES["accuracy"],
            basis=(f"{correctness} correctness issue(s) across {total} session(s)",) if accuracy is not None else (),
            min_observations=floor,
        )
    )

    # reliability — did the product exist when they came to use it?
    deploy = _metric_value(metrics, "deployment_reliability")
    availability = _metric_value(metrics, "critical_source_availability")
    parts = [value for value in (deploy, availability) if value is not None]
    reliability = 100.0 * sum(parts) / len(parts) if parts else None
    reliability_basis: list[str] = []
    if deploy is not None:
        reliability_basis.append(f"deployment reliability {deploy:.2f}")
    if availability is not None:
        reliability_basis.append(f"critical-source availability {availability:.2f}")
    if reliability_basis:
        reliability_basis.append(f"over {len(live_days)} non-drill day(s)")
    dims.append(
        ScorecardDimension(
            key="reliability",
            score=reliability,
            observations=len(live_days),
            source=DIMENSION_SOURCES["reliability"],
            basis=tuple(reliability_basis),
            min_observations=floor,
        )
    )

    # timeliness — inside the task's own target, and not reading stale numbers
    completed = [s for s in window if s.outcome.is_complete]
    timeliness: float | None
    timeliness_basis: tuple[str, ...]
    if completed:
        on_time = sum(1 for s in completed if s.duration_minutes <= s.task.target_minutes)
        stale = sum(1 for s in completed if s.issues_of(IssueClass.STALE_DATA))
        timeliness = max(0.0, 100.0 * (on_time - stale) / len(completed))
        timeliness_basis = (
            f"{on_time} of {len(completed)} completed session(s) inside the task target",
            f"less {stale} session(s) that reported stale data",
        )
    else:
        timeliness, timeliness_basis = None, ()
    dims.append(
        ScorecardDimension(
            key="timeliness",
            score=timeliness,
            observations=len(completed),
            source=DIMENSION_SOURCES["timeliness"],
            basis=timeliness_basis,
            min_observations=floor,
        )
    )

    # the three usefulness dimensions
    for key, tasks in (
        ("physical_usefulness", _PHYSICAL_TASKS),
        ("futures_usefulness", _FUTURES_TASKS),
        ("opportunity_usefulness", _OPPORTUNITY_TASKS),
    ):
        score, count = _task_usefulness(window, tasks)
        basis: tuple[str, ...] = ()
        if score is not None:
            basis = (
                f"{count} session(s) across {', '.join(task.label for task in tasks)}",
                "completion rate and would-act rate, weighted equally",
            )
            if key == "opportunity_usefulness" and worked_opportunities:
                conversion = _metric_value(metrics, "opportunity_conversion")
                if conversion is not None:
                    # Conversion is a third input here, and only here: it is the
                    # one dimension with an outcome outside the session record.
                    score = (score * 2 + 100.0 * conversion) / 3
                    basis += (
                        f"{progressed_opportunities} of {worked_opportunities} worked "
                        f"opportunit(ies) progressed",
                    )
        dims.append(
            ScorecardDimension(
                key=key,
                score=score,
                observations=count,
                source=DIMENSION_SOURCES[key],
                basis=basis,
                min_observations=floor,
            )
        )

    # ux — did the surface get in the way?
    ux_issues = sum(
        len(s.issues_of(IssueClass.MISLEADING_UX)) + len(s.issues_of(IssueClass.WORKFLOW_FRICTION)) for s in window
    )
    ux = _issue_free_rate(window, (IssueClass.MISLEADING_UX, IssueClass.WORKFLOW_FRICTION))
    lookups = _metric_value(metrics, "external_lookups_per_task")
    dims.append(
        ScorecardDimension(
            key="ux",
            score=ux,
            observations=total,
            source=DIMENSION_SOURCES["ux"],
            basis=(
                (
                    f"{ux_issues} misleading-UX or friction issue(s) across {total} session(s)",
                    f"{lookups:.2f} external lookup(s) per task" if lookups is not None else "no lookups recorded",
                )
                if ux is not None
                else ()
            ),
            min_observations=floor,
        )
    )

    # trader_trust — confidence and whether they would act
    confidence = _metric_value(metrics, "median_confidence")
    act_rate = _metric_value(metrics, "would_act_rate")
    trust_parts: list[float] = []
    trust_basis: list[str] = []
    if confidence is not None:
        scale = getattr(config, "TRIAL_CONFIDENCE_SCALE", (1, 2, 3, 4, 5))
        low, high = float(min(scale)), float(max(scale))
        trust_parts.append(100.0 * (confidence - low) / (high - low))
        trust_basis.append(f"median confidence {confidence:.1f} on a {int(low)}-{int(high)} scale")
    if act_rate is not None:
        trust_parts.append(100.0 * act_rate)
        trust_basis.append(f"would act in {act_rate:.0%} of session(s)")
    trust = sum(trust_parts) / len(trust_parts) if trust_parts else None
    dims.append(
        ScorecardDimension(
            key="trader_trust",
            score=trust,
            observations=total,
            source=DIMENSION_SOURCES["trader_trust"],
            basis=tuple(trust_basis),
            min_observations=floor,
        )
    )

    ordered_keys = getattr(config, "TRIAL_SCORECARD_DIMENSIONS", tuple(DIMENSION_SOURCES))
    by_key = {dim.key: dim for dim in dims}
    missing = [key for key in ordered_keys if key not in by_key]
    if missing:
        raise TrialError(
            f"config.TRIAL_SCORECARD_DIMENSIONS names {missing} but the scorecard computes no "
            "such dimension — a rubric dimension with no arithmetic behind it cannot be scored"
        )
    ordered = tuple(by_key[key] for key in ordered_keys)

    backlog = draft_backlog(window)
    traders = len({s.trader for s in window})
    covered = len({s.trading_day for s in window} | {d.trading_day for d in live_days})
    verdict, reason = _verdict(metrics, backlog, traders=traders, sessions=total)
    return Scorecard(
        window_start=window_start,
        window_end=window_end,
        dimensions=ordered,
        session_count=total,
        trader_count=traders,
        day_count=len(window_days),
        trading_days_covered=covered,
        verdict=verdict,
        verdict_reason=reason,
    )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def review_markdown(review: WeeklyReview, *, audience: str = AUDIENCE_PRIVATE) -> str:
    """The weekly review as a document. Private form carries the free text."""
    if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
        raise TrialError(f"unknown audience {audience!r}")
    lines = [
        f"# Trial weekly review — {review.week_start} to {review.week_end}",
        "",
        f"**Verdict: {review.verdict.upper()}** — {review.verdict_reason}",
        "",
        f"{review.session_count} session(s) from {review.trader_count} trader(s).",
    ]
    if audience == AUDIENCE_PRIVATE:
        lines.append("*Private record. Do not paste into a public tracker.*")
    lines += ["", "## Metrics", "", "| Metric | Value | Obs | Status | Trend |", "|---|---|---|---|---|"]
    trend_by_key = {entry.key: entry for entry in review.trend}
    for metric in review.metrics.metrics:
        entry = trend_by_key.get(metric.key)
        arrow = entry.direction if entry else "new"
        lines.append(f"| {metric.label} | {metric.display} | {metric.observations} | {metric.status} | {arrow} |")

    lines += ["", "## What worked", ""]
    lines += [f"- {item}" for item in review.worked] or ["- Nothing in the records supports a positive finding."]

    if audience == AUDIENCE_PRIVATE:
        lines += ["", "## Failures", ""]
        lines += [f"- {item}" for item in review.failures] or ["- None recorded."]
        lines += ["", "## Top unmet questions", ""]
        lines += [
            f"- ({count}x) {question}" for question, count in review.unmet_questions[:10]
        ] or ["- None recorded."]
    else:
        lines += [
            "",
            "## Unmet questions",
            "",
            f"{len(review.unmet_questions)} distinct question(s) recorded; text withheld.",
        ]

    lines += ["", "## Recommended changes", ""]
    rendered = [
        item.text if audience == AUDIENCE_PRIVATE else item.shareable
        for item in review.recommendations
    ]
    lines += [f"- {text}" for text in rendered if text] or ["- None."]
    withheld = sum(1 for item in review.recommendations if item.shareable is None)
    if audience == AUDIENCE_AGGREGATE and withheld:
        lines.append(f"- *({withheld} recommendation(s) withheld: they quote a trader's own words.)*")
    return "\n".join(lines)


def scorecard_markdown(card: Scorecard, *, audience: str = AUDIENCE_PRIVATE) -> str:
    """The 30-day scorecard as a document."""
    lines = [
        f"# Trial 30-day scorecard — {card.window_start} to {card.window_end}",
        "",
        f"**Overall: {card.overall_letter}**"
        + (f" ({card.overall:.1f}/100)" if card.overall is not None else "")
        + f" — computed from {len(card.graded_dimensions)} of {len(card.dimensions)} dimensions.",
        "",
        f"**Verdict: {card.verdict.upper()}** — {card.verdict_reason}",
        "",
        f"{card.session_count} session(s), {card.trader_count} trader(s), "
        f"{card.trading_days_covered} trading day(s) covered.",
    ]
    if not card.is_complete:
        lines += [
            "",
            "> **This window did not meet the protocol's coverage requirement.** The scores "
            "below describe what was observed and are not a 30-day result.",
        ]
    lines += ["", "| Dimension | Score | Grade | Obs | Basis |", "|---|---|---|---|---|"]
    ungraded = False
    for dim in card.dimensions:
        score = f"{dim.score:.1f}" if dim.score is not None else "—"
        if dim.score is not None and not dim.graded:
            # A number beside an "n/a" grade reads as a result. The marker says
            # the arithmetic ran but the window was too thin to grade it, which
            # is a different statement from either a score or a blank.
            score += "*"
            ungraded = True
        basis = "; ".join(dim.basis) if dim.basis else "insufficient observations"
        lines.append(f"| {dim.key.replace('_', ' ')} | {score} | {dim.letter} | {dim.observations} | {basis} |")
    if ungraded:
        lines += [
            "",
            "\\* computed, but below the observation floor — not graded and excluded from the overall.",
        ]
    lines += ["", "## How each dimension is computed", ""]
    for dim in card.dimensions:
        lines.append(f"- **{dim.key.replace('_', ' ')}** — {dim.source}")
    if audience == AUDIENCE_PRIVATE:
        lines += ["", "*Private record. Sanitise before sharing outside the desk.*"]
    return "\n".join(lines)
