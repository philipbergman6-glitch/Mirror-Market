"""The private trial dashboard (Phase 5, requirement 5) — data only, never markup.

Same split as every other page family here: this module returns numbers, labels
and reasons; ``app/templates/trial.html.j2`` decides how they look.

**This page is private, and it is private structurally rather than by policy.**
Three things enforce that, and they are independent:

1. It is written to ``config.TRIAL_PRIVATE_OUTPUT_DIR`` — ``data/workspace/trial``
   — which is outside ``docs/`` and therefore outside what GitHub Pages deploys.
   :func:`analysis.trial.sanitize.assert_private_path` checks the destination on
   every write rather than trusting the constant.
2. It is absent from ``trust.site_promotion.expected_site_paths()``, so the
   promotion contract would reject it as an unexpected file even if it somehow
   landed in the candidate.
3. It is not registered in ``scripts/generate_site.py``'s page list at all. The
   site generator does not know this page exists, which is the difference
   between "not published" and "published unless someone remembers".

**It deliberately does not extend ``_base.html.j2``.** Every other page does,
and that is the right rule for pages *on the site* — one owner for the head, the
masthead and the nav, so no page drifts. This one is not on the site: it has no
place in the nav, it must render with no database connection and no market
registry, and giving it the shared base would put a page full of trader identity
one ``expected_site_paths()`` edit away from being deployed with everything else.
It follows DESIGN.md's palette and type scale directly instead, which is checked
by test rather than by inheritance.

The page shows what the trial measured, in the order the question is asked:

    01  verdict — go, hold, no-go, or not enough evidence yet, and why
    02  metrics — every metric with its denominator, bar and status
    03  scorecard — the nine rubric dimensions and the arithmetic behind each
    04  coverage — which tasks and pages were exercised, and which were not
    05  findings — the validated backlog, and the observations that did not promote
    06  reliability — day by day source availability and deployment
    07  reproducibility — how many findings can be re-run at all
    08  method — what this page will not do
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

from analysis.trial.backlog import draft_backlog
from analysis.trial.domain import AUDIENCE_PRIVATE, DayObservation, SessionRecord, TaskId
from analysis.trial.metrics import STATUS_INSUFFICIENT, compute_metrics
from analysis.trial.review import scorecard, weekly_review

log = logging.getLogger(__name__)

__all__ = ["build_trial_page", "build_view", "render_trial_page"]


def _empty(reason: str) -> dict[str, Any]:
    return {"state": "empty", "reason": reason, "data": {}}


def _ok(data: dict[str, Any]) -> dict[str, Any]:
    return {"state": "ok", "reason": "", "data": data}


def build_view(
    sessions: list[SessionRecord],
    days: list[DayObservation],
    *,
    worked_opportunities: int = 0,
    progressed_opportunities: int = 0,
    today: date | None = None,
) -> dict[str, Any]:
    """The whole page as plain data. Private audience only.

    There is no ``audience`` parameter, and that is the point: this view is not
    something a caller could accidentally render publicly, because the public
    projection of a trial dashboard is the weekly review's aggregate form, which
    is a different object built by a different function. One builder with an
    audience flag would make publishing this page a one-argument mistake.
    """
    when = today or date.today()
    if not sessions:
        return {
            "generated_for": when.isoformat(),
            "session_count": 0,
            "empty": True,
            "empty_reason": (
                "No trial sessions have been recorded yet. This page reports what traders "
                "logged; with nothing logged there is nothing to report, and a dashboard of "
                "zeros would read like a result."
            ),
            "sections": {},
        }

    metrics = compute_metrics(
        sessions,
        days,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
    )
    trading_days = sorted({s.trading_day for s in sessions})
    start, end = trading_days[0], trading_days[-1]
    card = scorecard(
        sessions,
        days,
        window_start=start,
        window_end=end,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
    )
    # Over the WHOLE window, not the last week. This page summarises the trial
    # to date, and the unmet-question list read from a one-day window silently
    # showed nothing on any day nobody happened to log a lookup — an empty list
    # that reads as "we answered everything" rather than as "wrong window".
    review = weekly_review(
        sessions,
        days,
        week_start=start,
        week_end=end,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
    )
    backlog = draft_backlog(sessions)

    sections: dict[str, dict[str, Any]] = {}

    # 01 verdict
    sections["verdict"] = _ok(
        {
            "verdict": card.verdict,
            "reason": card.verdict_reason,
            "window_start": start.isoformat(),
            "window_end": end.isoformat(),
            "trading_days_covered": card.trading_days_covered,
            "is_complete": card.is_complete,
            "session_count": len(sessions),
            "trader_count": card.trader_count,
        }
    )

    # 02 metrics
    sections["metrics"] = _ok(
        {
            "metrics": [metric.to_dict(audience=AUDIENCE_PRIVATE) for metric in metrics.metrics],
            "graded_count": sum(1 for m in metrics.metrics if m.status != STATUS_INSUFFICIENT),
        }
    )

    # 03 scorecard
    sections["scorecard"] = _ok(
        {
            "overall": round(card.overall, 1) if card.overall is not None else None,
            "overall_letter": card.overall_letter,
            "graded_count": len(card.graded_dimensions),
            "dimension_count": len(card.dimensions),
            "dimensions": [dim.to_dict() for dim in card.dimensions],
        }
    )

    # 04 coverage — what was exercised, and what was not
    # Keyed by the enum, not by its value: the count and the label then come
    # from one object, and the dict starts from every task in the protocol so a
    # task nobody ran appears as a zero rather than as a missing row. A task
    # with no sessions is a coverage gap, and a gap that does not render reads
    # as a task that passed.
    exercised = dict.fromkeys(TaskId, 0)
    for session in sessions:
        exercised[session.task] += 1
    untouched = [task.value for task, count in exercised.items() if count == 0]
    sections["coverage"] = _ok(
        {
            "tasks": [
                {"task": task.value, "label": task.label, "count": count}
                for task, count in exercised.items()
            ],
            "untouched": untouched,
            "untouched_note": (
                f"{len(untouched)} of {len(exercised)} task(s) were never run. A task with no "
                "sessions is not a passing task."
            )
            if untouched
            else "Every task in the protocol was exercised at least once.",
            # Already ordered by the metrics module (most-used first) — not
            # re-sorted here, so the page and the CLI agree on the order.
            "pages": [{"page": page, "count": count} for page, count in metrics.page_usage],
            "external_tools": [
                {"tool": tool, "count": count} for tool, count in metrics.external_tool_counts
            ],
        }
    )

    # 05 findings
    sections["findings"] = _ok(
        {
            # Named "backlog_items", not "items": Jinja resolves ``f.items`` to
            # dict.items before it ever looks at the key, so a section dict with
            # an "items" key renders as a bound method rather than as the list.
            "backlog_items": [item.to_dict(audience=AUDIENCE_PRIVATE) for item in backlog.items],
            "observations": [obs.to_dict(audience=AUDIENCE_PRIVATE) for obs in backlog.observations],
            "by_severity": backlog.by_severity,
            "issue_count": backlog.issue_count,
            "undrafted_count": backlog.undrafted_count,
            "unmet_questions": [
                {"question": question, "count": count} for question, count in review.unmet_questions[:15]
            ],
        }
    )

    # 06 reliability
    if days:
        sections["reliability"] = _ok(
            {
                "days": [
                    {
                        "trading_day": day.trading_day.isoformat(),
                        "published": day.edition_published,
                        "current": day.edition_current,
                        "available": day.critical_layers_available,
                        "expected": day.critical_layers_expected,
                        "availability": round(day.source_availability, 3),
                        "degraded": list(day.degraded_layers),
                        "drill": day.drill,
                        "note": day.note,
                    }
                    for day in sorted(days, key=lambda d: d.trading_day, reverse=True)
                ],
                "drill_days": sum(1 for day in days if day.is_drill),
            }
        )
    else:
        sections["reliability"] = _empty(
            "No day observations recorded. Source availability and deployment reliability "
            "cannot be inferred from session records — the days the product broke are the "
            "days nobody logged a session — so they are not estimated here. "
            "Run `python scripts/trial.py day` once per trading day."
        )

    # 07 reproducibility
    reproducible = sum(1 for s in sessions if s.release.is_reproducible)
    with_findings = [s for s in sessions if s.issues]
    findings_reproducible = sum(1 for s in with_findings if s.release.is_reproducible)
    sections["reproducibility"] = _ok(
        {
            "sessions_reproducible": reproducible,
            "session_count": len(sessions),
            "findings_reproducible": findings_reproducible,
            "findings_count": len(with_findings),
            "revisions": sorted({s.release.short_code for s in sessions}),
            "note": (
                "A session run against uncommitted changes is recorded, not refused — a "
                "hotfix session is a legitimate session — but its result cannot be produced "
                "again by any mechanism. Use `trial.py reproduce --session <id>`."
            ),
        }
    )

    # 08 method
    sections["method"] = _ok(
        {
            "will_not": [
                "It does not learn. Feedback is counted and reported; no rule is re-weighted "
                "by it, because retuning on a handful of dismissals would be a model nobody "
                "trained, evaluated or can turn off.",
                "It does not fill a gap with a default. A metric below its observation floor "
                "reports `insufficient`, and a scorecard dimension without enough sessions "
                "scores nothing and is excluded from the overall rather than averaged in.",
                "It does not blend. Every metric keeps its own denominator and its own bar; "
                "there is no single trial score to optimise.",
                "It does not leave this machine. This file is written outside `docs/`, is "
                "absent from the site promotion contract, and is not known to the site "
                "generator.",
            ],
            "thresholds_source": "config.TRIAL_DECISION_THRESHOLDS",
        }
    )

    return {
        "generated_for": when.isoformat(),
        "session_count": len(sessions),
        "empty": False,
        "empty_reason": "",
        "sections": sections,
    }


def render_trial_page(view: dict[str, Any], *, generated_at: str = "") -> str:
    """Render the view through the template. No I/O."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    env = Environment(
        loader=FileSystemLoader(str(Path(__file__).resolve().parent / "templates")),
        autoescape=select_autoescape(["html", "xml"]),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    return env.get_template("trial.html.j2").render(trial=view, generated_at=generated_at)


def build_trial_page(
    sessions: list[SessionRecord],
    days: list[DayObservation],
    *,
    worked_opportunities: int = 0,
    progressed_opportunities: int = 0,
    output_dir: str | Path | None = None,
) -> Path:
    """Build and write the private dashboard. Returns the path written.

    The destination is checked before anything is rendered, not after: the
    expensive half is the render, and the failure that matters is the write
    landing somewhere publishable.
    """
    import config
    from analysis.trial.domain import utc_now
    from analysis.trial.sanitize import assert_private_path

    directory = assert_private_path(
        output_dir if output_dir is not None else config.TRIAL_PRIVATE_OUTPUT_DIR,
        where="trial dashboard",
    )
    now = utc_now()
    view = build_view(
        sessions,
        days,
        worked_opportunities=worked_opportunities,
        progressed_opportunities=progressed_opportunities,
    )
    html = render_trial_page(view, generated_at=now.strftime("%Y-%m-%d %H:%M UTC"))
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "trial.html"
    target.write_text(html, encoding="utf-8")
    log.info("wrote the private trial dashboard to %s", target)
    return target
