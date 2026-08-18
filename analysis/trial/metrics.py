"""The trial metrics (Phase 5, requirement 5).

Thirteen numbers, computed from the private record store and nothing else. Every
one of them carries three things besides its value, and the three are what make
it evidence rather than a statistic:

**Its denominator.** ``observations`` is the count the rate was taken over, and
it is reported beside the rate always. "2% wrong-answer rate" over four sessions
and over four hundred are different claims, and only one of them supports a
decision about deploying this to a desk.

**Its insufficiency rule.** Below ``TRIAL_MIN_OBSERVATIONS`` a metric reports
``insufficient`` and its ``value`` is still computed but its ``status`` refuses
to grade. A go/no-go read off eight sessions is worse than no go/no-go, because
it looks like it was measured.

**Its note.** Each metric states, in one line, the arithmetic that produced it,
using the numbers printed next to it. A reader must be able to check the metric
against its own denominator by eye — the same rule the opportunity score
components carry, for the same reason: a composite nobody can reproduce is a
number people either trust blindly or ignore entirely.

Two of the thirteen — availability and deployment reliability — come from
:class:`~analysis.trial.domain.DayObservation` rather than from sessions, and
the reason is in that class's docstring: the days the product was broken are
exactly the days nobody logged a session.

Nothing here weights, blends or composes the metrics into a single score. The
go/no-go in ``review.py`` reads them individually against individually stated
bars, so a failure can always be pointed at.
"""

from __future__ import annotations

import statistics
from collections import Counter
from dataclasses import dataclass
from typing import Any

from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    AUDIENCE_PRIVATE,
    DayObservation,
    IssueClass,
    Outcome,
    SessionRecord,
    TaskId,
    TrialError,
)

__all__ = [
    "STATUS_GO",
    "STATUS_HOLD",
    "STATUS_INSUFFICIENT",
    "STATUS_NO_GO",
    "STATUS_REPORTED",
    "Metric",
    "MetricSet",
    "compute_metrics",
]

STATUS_GO = "go"
STATUS_HOLD = "hold"
STATUS_NO_GO = "no_go"
STATUS_INSUFFICIENT = "insufficient"

#: A metric with no bar in ``config.TRIAL_DECISION_THRESHOLDS``. Median
#: completion time is the clearest case: this project has no instrumented
#: baseline of the same task on a terminal, so a bar would be invented. It is
#: reported, compared to the task's stated target, and judged by a human.
STATUS_REPORTED = "reported"


@dataclass(frozen=True)
class Metric:
    """One measured number, its denominator, and how it grades.

    ``value`` is ``None`` only when the metric is undefined rather than merely
    thin — no completed sessions at all, so there is no median duration. That is
    different from ``insufficient``, which is a real number over too few
    observations, and the two must not render alike: one says "we did not
    measure this", the other says "we measured it and you should not lean on it".
    """

    key: str
    label: str
    value: float | None
    unit: str
    observations: int
    note: str
    lower_is_better: bool = False
    go: float | None = None
    no_go: float | None = None
    min_observations: int = 10
    detail: tuple[tuple[str, float], ...] = ()

    def __post_init__(self) -> None:
        if not self.note.strip():
            raise TrialError(
                f"metric {self.key} needs a note stating the arithmetic that produced it"
            )
        if self.observations < 0:
            raise TrialError(f"metric {self.key} cannot have a negative denominator")

    @property
    def has_bar(self) -> bool:
        return self.go is not None and self.no_go is not None

    @property
    def status(self) -> str:
        if self.value is None:
            return STATUS_INSUFFICIENT
        if self.observations < self.min_observations:
            return STATUS_INSUFFICIENT
        go, no_go = self.go, self.no_go
        if go is None or no_go is None:
            return STATUS_REPORTED
        if self.lower_is_better:
            if self.value <= go:
                return STATUS_GO
            return STATUS_NO_GO if self.value >= no_go else STATUS_HOLD
        if self.value >= go:
            return STATUS_GO
        return STATUS_NO_GO if self.value <= no_go else STATUS_HOLD

    @property
    def display(self) -> str:
        if self.value is None:
            return "—"
        if self.unit == "rate":
            return f"{self.value * 100:.0f}%"
        if self.unit == "minutes":
            return f"{self.value:.0f} min"
        if self.unit == "score":
            return f"{self.value:.1f}"
        return f"{self.value:.2f}"

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        if audience not in (AUDIENCE_PRIVATE, AUDIENCE_AGGREGATE):
            raise TrialError(f"unknown audience {audience!r}")
        return {
            "key": self.key,
            "label": self.label,
            "value": self.value,
            "display": self.display,
            "unit": self.unit,
            "observations": self.observations,
            "status": self.status,
            "note": self.note,
            "lower_is_better": self.lower_is_better,
            "go": self.go,
            "no_go": self.no_go,
            "detail": [list(pair) for pair in self.detail],
        }


@dataclass(frozen=True)
class MetricSet:
    """Every metric for one window, plus the usage breakdowns.

    ``page_usage`` and ``task_counts`` are distributions rather than metrics —
    "pages/features used" is not one number, and reducing it to a coverage
    percentage alone would hide the finding that matters, which is *which* page
    nobody opened twice.
    """

    metrics: tuple[Metric, ...]
    page_usage: tuple[tuple[str, int], ...] = ()
    task_counts: tuple[tuple[str, int], ...] = ()
    issue_counts: tuple[tuple[str, int], ...] = ()
    external_tool_counts: tuple[tuple[str, int], ...] = ()
    session_count: int = 0
    day_count: int = 0
    trader_count: int = 0

    @property
    def by_key(self) -> dict[str, Metric]:
        return {metric.key: metric for metric in self.metrics}

    def get(self, key: str) -> Metric:
        try:
            return self.by_key[key]
        except KeyError as exc:
            raise TrialError(f"no metric named {key!r}") from exc

    @property
    def graded(self) -> tuple[Metric, ...]:
        """Metrics that carry a bar and had enough observations to be read."""
        return tuple(
            m for m in self.metrics if m.has_bar and m.status not in (STATUS_INSUFFICIENT,)
        )

    def to_dict(self, *, audience: str = AUDIENCE_PRIVATE) -> dict[str, Any]:
        return {
            "metrics": [m.to_dict(audience=audience) for m in self.metrics],
            "page_usage": [list(pair) for pair in self.page_usage],
            "task_counts": [list(pair) for pair in self.task_counts],
            "issue_counts": [list(pair) for pair in self.issue_counts],
            "external_tool_counts": [list(pair) for pair in self.external_tool_counts],
            "session_count": self.session_count,
            "day_count": self.day_count,
            "trader_count": self.trader_count,
        }


def _rate(numerator: int, denominator: int) -> float | None:
    return None if denominator == 0 else numerator / denominator


def _bars(key: str, thresholds: dict[str, dict[str, float]]) -> tuple[float | None, float | None]:
    bar = thresholds.get(key)
    if bar is None:
        return None, None
    return bar.get("go"), bar.get("no_go")


def compute_metrics(
    sessions: tuple[SessionRecord, ...] | list[SessionRecord],
    days: tuple[DayObservation, ...] | list[DayObservation] = (),
    *,
    worked_opportunities: int = 0,
    progressed_opportunities: int = 0,
    thresholds: dict[str, dict[str, float]] | None = None,
    lower_is_better: frozenset[str] | None = None,
    min_observations: int | None = None,
    available_pages: tuple[str, ...] | None = None,
) -> MetricSet:
    """Compute every trial metric over one window.

    The opportunity-conversion inputs are passed in rather than read here,
    because they live in the Phase 4 private workflow store and this module has
    no business opening it — the same separation ``signals.py`` keeps by being
    the only SQL-aware module in its package. ``scripts/trial.py`` and
    ``app/trial_page.py`` supply them from ``opportunities.workflow``.
    """
    import config

    thresholds = thresholds if thresholds is not None else config.TRIAL_DECISION_THRESHOLDS
    lower = lower_is_better if lower_is_better is not None else config.TRIAL_LOWER_IS_BETTER
    floor = min_observations if min_observations is not None else config.TRIAL_MIN_OBSERVATIONS
    pages = available_pages if available_pages is not None else _default_pages()

    sessions = tuple(sessions)
    days = tuple(days)
    total = len(sessions)
    completed = tuple(s for s in sessions if s.outcome is Outcome.COMPLETED)
    completed_n = len(completed)

    def metric(
        key: str,
        label: str,
        value: float | None,
        unit: str,
        observations: int,
        note: str,
        *,
        detail: tuple[tuple[str, float], ...] = (),
    ) -> Metric:
        go, no_go = _bars(key, thresholds)
        return Metric(
            key=key,
            label=label,
            value=value,
            unit=unit,
            observations=observations,
            note=note,
            lower_is_better=key in lower,
            go=go,
            no_go=no_go,
            min_observations=floor,
            detail=detail,
        )

    lookups_total = sum(s.external_lookup_count for s in sessions)
    wrong_or_stale = sum(1 for s in sessions if s.had_wrong_or_stale)
    false_alert_sessions = sum(1 for s in sessions if s.false_alerts)
    missed_alert_sessions = sum(1 for s in sessions if s.missed_alerts)
    would_act = sum(1 for s in completed if s.would_act)
    durations = sorted(s.duration_minutes for s in completed)
    confidences = [s.confidence for s in sessions]
    unreproducible = sum(1 for s in sessions if not s.release.is_reproducible)

    page_counter: Counter[str] = Counter()
    for session in sessions:
        page_counter.update(set(session.pages_used))
    pages_touched = sum(1 for page in pages if page_counter.get(page, 0) > 0)

    deployment_ok = sum(1 for day in days if day.deployment_ok)
    availability_values = [day.source_availability for day in days]

    metrics: list[Metric] = [
        metric(
            "task_completion_rate",
            "Task completion rate",
            _rate(completed_n, total),
            "rate",
            total,
            f"{completed_n} of {total} started sessions reached a decision or output.",
        ),
        metric(
            "median_completion_minutes",
            "Median completion time",
            statistics.median(durations) if durations else None,
            "minutes",
            completed_n,
            (
                f"Median of {completed_n} completed sessions"
                + (f", range {durations[0]:.0f}-{durations[-1]:.0f} min." if durations else ".")
                + " No bar: this project has no instrumented baseline of the same task on a "
                "terminal, so any target here would be invented. Read it against each task's "
                "own stated target instead."
            ),
        ),
        metric(
            "external_lookups_per_task",
            "External lookups per completed task",
            _rate(lookups_total, completed_n),
            "count",
            completed_n,
            (
                f"{lookups_total} lookups to Bloomberg, a broker, a spreadsheet or an exchange "
                f"across {completed_n} completed tasks. This is the displacement measure — the "
                "headline number of the whole trial."
            ),
        ),
        metric(
            "wrong_or_stale_rate",
            "Wrong or stale answer rate",
            _rate(wrong_or_stale, total),
            "rate",
            total,
            (
                f"{wrong_or_stale} of {total} sessions hit a numerical error, a semantic "
                "mismatch, or a value past its own cadence that the page did not flag. A value "
                "labelled stale is not counted: that is the product working."
            ),
        ),
        metric(
            "false_alert_rate",
            "False alert rate",
            _rate(false_alert_sessions, total),
            "rate",
            total,
            f"{false_alert_sessions} of {total} sessions saw an alert the trader judged empty.",
        ),
        metric(
            "missed_alert_rate",
            "Missed alert rate",
            _rate(missed_alert_sessions, total),
            "rate",
            total,
            (
                f"{missed_alert_sessions} of {total} sessions found something the product should "
                "have flagged and did not, judged against an expectation stated at the time."
            ),
        ),
        metric(
            "would_act_rate",
            "Would act on the output",
            _rate(would_act, completed_n),
            "rate",
            completed_n,
            (
                f"{would_act} of {completed_n} completed sessions produced an output the trader "
                "would act on unaided."
            ),
        ),
        metric(
            "median_confidence",
            "Median trader confidence",
            statistics.median(confidences) if confidences else None,
            "score",
            len(confidences),
            (
                f"Median of {len(confidences)} confidence ratings on the protocol's 1-5 scale. "
                "Reported as a median, never a mean: the scale is ordinal and a mean would "
                "invent a precision it does not carry."
            ),
            detail=tuple(
                (str(score), float(confidences.count(score))) for score in (1, 2, 3, 4, 5)
            ),
        ),
        metric(
            "opportunity_conversion",
            "Opportunity conversion",
            _rate(progressed_opportunities, worked_opportunities),
            "rate",
            worked_opportunities,
            (
                f"{progressed_opportunities} of {worked_opportunities} opportunities the desk "
                "actually worked reached 'progressed' or better. The denominator is worked "
                "opportunities, not detected ones: a board row nobody opened was neither "
                "converted nor rejected, and counting it as a miss would make the rate a "
                "function of how many rows the detectors emitted."
            ),
        ),
        metric(
            "feature_coverage_rate",
            "Pages and features used",
            _rate(pages_touched, len(pages)),
            "rate",
            len(pages),
            (
                f"{pages_touched} of {len(pages)} published surfaces were opened at least once "
                "across the window. The per-page counts beside this are the finding — a page "
                "opened once and never again is a different signal from one never opened."
            ),
        ),
        metric(
            "critical_source_availability",
            "Critical source availability",
            statistics.mean(availability_values) if availability_values else None,
            "rate",
            len(days),
            (
                f"Mean share of the {len(config.TRIAL_CRITICAL_LAYERS)} trader-critical layers "
                f"inside their own cadence budget, across {len(days)} observed trading days."
            ),
        ),
        metric(
            "deployment_reliability",
            "Deployment reliability",
            _rate(deployment_ok, len(days)),
            "rate",
            len(days),
            (
                f"On {deployment_ok} of {len(days)} observed trading days the trader saw that "
                "day's edition. Published-but-stale counts as a failure: an edition carrying "
                "yesterday's data deployed perfectly and answered the wrong day."
            ),
        ),
        metric(
            "unreproducible_finding_rate",
            "Findings on unreproducible builds",
            _rate(unreproducible, total),
            "rate",
            total,
            (
                f"{unreproducible} of {total} sessions were stamped against a dirty working "
                "tree, so no commit describes the code that produced them. Not a product "
                "defect — a trial hygiene measure. It rising means findings are arriving that "
                "cannot be re-run."
            ),
        ),
    ]

    issue_counter: Counter[str] = Counter()
    tool_counter: Counter[str] = Counter()
    task_counter: Counter[str] = Counter()
    for session in sessions:
        task_counter[session.task.value] += 1
        for issue in session.issues:
            issue_counter[issue.classification.value] += 1
        for lookup in session.external_lookups:
            tool_counter[lookup.tool.value] += 1

    return MetricSet(
        metrics=tuple(metrics),
        page_usage=tuple(sorted(page_counter.items(), key=lambda kv: (-kv[1], kv[0]))),
        task_counts=tuple(
            (task.value, task_counter.get(task.value, 0)) for task in TaskId
        ),
        issue_counts=tuple(
            (kind.value, issue_counter.get(kind.value, 0)) for kind in IssueClass
        ),
        external_tool_counts=tuple(sorted(tool_counter.items(), key=lambda kv: (-kv[1], kv[0]))),
        session_count=total,
        day_count=len(days),
        trader_count=len({s.trader.strip().lower() for s in sessions}),
    )


def _default_pages() -> tuple[str, ...]:
    """Every surface a trader could have opened, from the promotion contract.

    Read from ``trust.site_promotion.expected_site_paths()`` rather than
    hardcoded, so a market promoted to a page or a new surface shipped mid-trial
    enters the coverage denominator automatically. A hardcoded list would quietly
    report 100% coverage of a site that had grown.
    """
    try:
        from trust.site_promotion import expected_site_paths

        return tuple(expected_site_paths())
    except Exception:  # pragma: no cover - defensive; the trial must not die here
        return ("index.html",)
