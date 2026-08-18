#!/usr/bin/env python
"""The trader-validation runbook, as a command line (Phase 5).

Why a stub-and-edit flow rather than a question-and-answer prompt
-----------------------------------------------------------------
The brief asks for something "easy enough to complete during a real trading
day". An interactive wizard is the obvious answer and the wrong one: it holds
the trader hostage for the length of the form, loses everything if they are
interrupted by a call — which, on a desk, is the normal case rather than the
edge case — and cannot be filled in retrospectively at 16:00 when the day makes
sense again.

So ``trial.py start`` writes a **prefilled YAML stub** and prints its path. The
stub carries the release stamp captured at that moment, the trader's handle, the
task, the start time, and the task's own decision question and success criterion
as comments the trader is answering. They fill it in whenever they can, in the
editor they already have open, and ``trial.py check`` tells them if it is wrong.
Nothing is lost to an interrupted session, because the file exists from the
first second.

``--interactive`` is still there for the first session, when nobody has seen the
schema yet. It asks the minimum and writes the same stub.

What this script will not do
----------------------------
It does not invent a participant, a session, a lookup, a metric or a trade. Every
number it prints traces to a YAML file a human wrote or to a table the pipeline
filled. Where there is nothing to report it says so and exits, rather than
producing an empty scorecard that reads like a result.

It also never writes into ``docs/`` — every output path goes through
``analysis.trial.sanitize.assert_private_path`` first — with the single
exception of ``docs/trial/PROTOCOL.md``, which is generated instructions
carrying no trial data and is not on the site promotion contract.

Usage
-----
    python scripts/trial.py protocol                 # regenerate PROTOCOL.md
    python scripts/trial.py start --trader zeb --task morning_brief
    python scripts/trial.py start --interactive
    python scripts/trial.py day                      # today's availability record
    python scripts/trial.py check                    # validate every record
    python scripts/trial.py metrics
    python scripts/trial.py review --week-start 2026-08-17
    python scripts/trial.py scorecard --start 2026-08-01 --end 2026-08-30
    python scripts/trial.py backlog
    python scripts/trial.py drills
    python scripts/trial.py reproduce --session TS-20260817-1a2b3c4d
    python scripts/trial.py dashboard
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

log = logging.getLogger("trial")


def _parse_date(value: str | None, default: date | None = None) -> date:
    if value is None:
        if default is None:
            raise SystemExit("a date is required")
        return default
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise SystemExit(f"not an ISO date: {value!r}") from exc


# ---------------------------------------------------------------------------
# protocol
# ---------------------------------------------------------------------------
def cmd_protocol(args: argparse.Namespace) -> int:
    from analysis.trial.protocol import protocol_version, write_protocol

    target = write_protocol(args.output)
    print(f"protocol v{protocol_version()} written to {target}")
    return 0


# ---------------------------------------------------------------------------
# start — write a prefilled session stub
# ---------------------------------------------------------------------------
def _stub_text(trader: str, task: Any, started: datetime, stamp: Any) -> str:
    """A YAML session stub with the task's own criteria carried as comments."""
    return "\n".join(
        [
            f"# {task.label}",
            "#",
            f"# THE DECISION: {task.decision_question}",
            "#",
            f"# SUCCESS:      {task.success_criteria}",
            f"# TIME TARGET:  {task.target_minutes} minutes",
            "#",
            "# Fill in ended_at, outcome, confidence, would_act and decision.",
            "# Log EVERY time you left Mirror Market under external_lookups, with the",
            "# question we could not answer — that field is the point of the trial.",
            "# This file is gitignored. Notes and evidence never leave it.",
            "",
            f"- trader: {trader}",
            f"  task: {task.value}",
            f"  trading_day: {started.date().isoformat()}",
            f"  started_at: {started.isoformat()}",
            "  ended_at:            # REQUIRED, e.g. "
            f"{(started + timedelta(minutes=task.target_minutes)).isoformat()}",
            "  outcome: completed   # completed | abandoned | blocked",
            "  confidence:          # REQUIRED 1-5",
            "  would_act:           # REQUIRED true/false",
            "  decision: ''         # REQUIRED when outcome is completed",
            "  pages_used: []       # e.g. [index.html, markets/brazil.html]",
            "  external_lookups: []",
            "  #  - tool: bloomberg          # bloomberg|broker|spreadsheet|exchange|"
            "refinitiv|news|colleague|other",
            "  #    unanswered_question: ''  # REQUIRED — what could we not answer?",
            "  #    answer_found: true",
            "  #    minutes: 3",
            "  issues: []",
            "  #  - classification: numerical_error",
            "  #    severity: major          # blocker|major|minor",
            "  #    summary: ''",
            "  #    evidence: ''",
            "  #    affected_decision: ''",
            "  #    page: ''",
            "  notes: []",
            "  evidence: []",
            "  release:",
            f"    code_revision: {stamp.code_revision}",
            f"    data_fingerprint: {stamp.data_fingerprint}",
            f"    captured_at: {stamp.captured_at.isoformat()}",
            f"    dirty: {str(stamp.dirty).lower()}",
            f"    layer_count: {stamp.layer_count}",
            "",
        ]
    )


def cmd_start(args: argparse.Namespace) -> int:
    from analysis.trial.domain import TaskId, utc_now
    from analysis.trial.records import DAYS_SUBDIR, SESSIONS_SUBDIR, _resolve  # noqa: F401
    from analysis.trial.release import capture_release_stamp
    from analysis.trial.sanitize import assert_private_path

    trader = args.trader
    task_value = args.task
    if args.interactive or not trader or not task_value:
        print("Tasks:")
        for index, task in enumerate(TaskId, start=1):
            print(f"  {index:2}. {task.value:24} {task.label}")
        trader = trader or input("your handle (not your name, 3+ chars): ").strip()
        task_value = task_value or input("task (name or number): ").strip()
        if task_value.isdigit():
            task_value = list(TaskId)[int(task_value) - 1].value

    if not trader or len(trader) < 3:
        raise SystemExit("a trader handle of at least three characters is required")
    try:
        task = TaskId(task_value)
    except ValueError:
        raise SystemExit(f"unknown task {task_value!r}; run `trial.py protocol` for the list") from None

    started = utc_now()
    day = _parse_date(args.day, started.date())
    stamp = capture_release_stamp()

    directory = _resolve(args.directory, SESSIONS_SUBDIR)
    assert_private_path(directory, where="session directory")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{day.isoformat()}.yml"

    stub = _stub_text(trader, task, started, stamp)
    if target.exists():
        # Append. A trading day holds several sessions from several traders, and
        # one file per day is what makes "what happened on the 12th" one open.
        with target.open("a", encoding="utf-8") as handle:
            handle.write("\n" + stub)
    else:
        target.write_text(stub, encoding="utf-8")

    print(f"session stub appended to {target}")
    print(f"\n  {task.label}")
    print(f"  DECISION: {task.decision_question}")
    print(f"  SUCCESS:  {task.success_criteria}")
    print(f"  TARGET:   {task.target_minutes} min")
    if stamp.dirty:
        print("\n  WARNING: the working tree is dirty, so this session is recorded as")
        print("  NOT REPRODUCIBLE. Commit or stash before a session you want to re-run.")
    print("\nFill it in, then: python scripts/trial.py check")
    return 0


# ---------------------------------------------------------------------------
# day — the availability and deployment record
# ---------------------------------------------------------------------------
def cmd_day(args: argparse.Namespace) -> int:
    """Compute today's day observation from the freshness table and the built site.

    Everything here except ``note`` is measured, not typed. That is deliberate:
    availability and deployment reliability are the two metrics a trader cannot
    supply — the days the product broke are the days nobody logged a session —
    so they are read from ``data_freshness`` and from whether the site actually
    built, and a human only annotates.
    """
    import yaml

    import config
    from analysis.trial.domain import DayObservation
    from analysis.trial.records import DAYS_SUBDIR, _resolve, day_to_document
    from analysis.trial.release import capture_release_stamp
    from analysis.trial.sanitize import assert_private_path

    when = _parse_date(args.date, date.today())
    critical = tuple(getattr(config, "TRIAL_CRITICAL_LAYERS", ()))
    if not critical:
        raise SystemExit("config.TRIAL_CRITICAL_LAYERS is empty; nothing to measure availability against")

    available, degraded = 0, []
    try:
        from pipeline.query import read_freshness

        freshness = read_freshness()
    except Exception as exc:  # a missing DB is a real observation, not a crash
        log.warning("could not read data_freshness: %s", exc)
        freshness = None

    if freshness is None or getattr(freshness, "empty", True):
        print("WARNING: data_freshness is unreadable or empty — recording zero availability.")
        degraded = list(critical)
    else:
        rows = {str(row.get("layer_name")): row for row in freshness.to_dict(orient="records")}
        for layer in critical:
            row = rows.get(layer)
            if row is not None and str(row.get("status")) == "success":
                available += 1
            else:
                degraded.append(layer)

    index = REPO / "docs" / "index.html"
    published = index.exists()
    current = published and args.edition_current
    if published and not args.edition_current:
        print("NOTE: docs/index.html exists but --edition-current was not passed;")
        print("      recording the edition as published but not current.")

    observation = DayObservation(
        trading_day=when,
        edition_published=published,
        edition_current=current,
        critical_layers_expected=len(critical),
        critical_layers_available=available,
        release=capture_release_stamp(freshness=freshness),
        degraded_layers=tuple(degraded),
        drill=args.drill,
        note=args.note or "",
    )

    directory = _resolve(args.directory, DAYS_SUBDIR)
    assert_private_path(directory, where="day directory")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{when.isoformat()}.yml"
    target.write_text(yaml.safe_dump(day_to_document(observation), sort_keys=False), encoding="utf-8")

    print(f"day observation written to {target}")
    print(f"  critical sources: {available}/{len(critical)} available")
    if degraded:
        print(f"  degraded: {', '.join(degraded)}")
    print(f"  edition published={published} current={current}")
    return 0


# ---------------------------------------------------------------------------
# check / metrics / backlog / review / scorecard
# ---------------------------------------------------------------------------
def _load(args: argparse.Namespace) -> tuple[Any, Any]:
    from analysis.trial.records import load_day_observations, load_sessions

    return load_sessions(args.directory), load_day_observations(args.directory)


def cmd_check(args: argparse.Namespace) -> int:
    sessions, days = _load(args)
    if sessions.is_empty and not days.days:
        print("No trial records found. Start one with: python scripts/trial.py start")
        return 0
    print(f"{len(sessions.sessions)} session(s) from {len(sessions.traders)} trader(s), "
          f"{len(days.days)} day observation(s) — all parsed and validated.")
    print(f"  trading days covered: {len(sessions.trading_days)}")
    for session in sessions.sessions:
        flags = []
        if not session.release.is_reproducible:
            flags.append("NOT REPRODUCIBLE")
        if session.had_wrong_or_stale:
            flags.append("correctness issue")
        if not session.outcome.is_complete:
            flags.append(session.outcome.value)
        if flags:
            print(f"  {session.session_id} {session.task.value:24} {', '.join(flags)}")
    return 0


def cmd_metrics(args: argparse.Namespace) -> int:
    from analysis.trial.metrics import compute_metrics

    sessions, days = _load(args)
    if sessions.is_empty:
        print("No sessions recorded, so there is nothing to measure.")
        return 0
    result = compute_metrics(
        list(sessions.sessions),
        list(days.days),
        worked_opportunities=args.worked_opportunities,
        progressed_opportunities=args.progressed_opportunities,
    )
    width = max(len(metric.label) for metric in result.metrics)
    for metric in result.metrics:
        print(f"  {metric.label:{width}}  {metric.display:>12}  n={metric.observations:<4} {metric.status}")
    return 0


def cmd_backlog(args: argparse.Namespace) -> int:
    from analysis.trial.backlog import backlog_markdown, draft_backlog

    sessions, _ = _load(args)
    if sessions.is_empty:
        print("No sessions recorded, so there are no findings to promote.")
        return 0
    result = draft_backlog(list(sessions.sessions))
    print(backlog_markdown(result))
    return 0


def cmd_review(args: argparse.Namespace) -> int:
    from analysis.trial.review import review_markdown, weekly_review

    sessions, days = _load(args)
    if sessions.is_empty:
        print("No sessions recorded, so there is nothing to review.")
        return 0
    start = _parse_date(args.week_start, max(sessions.trading_days) - timedelta(days=6))
    previous = None
    if not args.no_compare:
        prior = weekly_review(
            list(sessions.sessions), list(days.days), week_start=start - timedelta(days=7)
        )
        previous = prior.metrics
    result = weekly_review(
        list(sessions.sessions),
        list(days.days),
        week_start=start,
        previous=previous,
        worked_opportunities=args.worked_opportunities,
        progressed_opportunities=args.progressed_opportunities,
    )
    text = review_markdown(result)
    print(text)
    if args.write:
        _write_private(f"review-{start.isoformat()}.md", text)
    return 0


def cmd_scorecard(args: argparse.Namespace) -> int:
    from analysis.trial.review import scorecard, scorecard_markdown

    sessions, days = _load(args)
    if sessions.is_empty:
        print("No sessions recorded, so there is nothing to score.")
        return 0
    start = _parse_date(args.start, min(sessions.trading_days))
    end = _parse_date(args.end, max(sessions.trading_days))
    card = scorecard(
        list(sessions.sessions),
        list(days.days),
        window_start=start,
        window_end=end,
        worked_opportunities=args.worked_opportunities,
        progressed_opportunities=args.progressed_opportunities,
    )
    text = scorecard_markdown(card)
    print(text)
    if args.write:
        _write_private(f"scorecard-{end.isoformat()}.md", text)
    return 0


def _write_private(filename: str, text: str) -> Path:
    import config
    from analysis.trial.sanitize import assert_private_path

    directory = assert_private_path(config.TRIAL_PRIVATE_OUTPUT_DIR, where="private trial output")
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / filename
    target.write_text(text, encoding="utf-8")
    print(f"\nwritten to {target} (private, outside docs/)")
    return target


# ---------------------------------------------------------------------------
# drills / reproduce / dashboard
# ---------------------------------------------------------------------------
def cmd_drills(args: argparse.Namespace) -> int:
    from analysis.trial.drills import DRILLS, run_all_drills, run_drill

    results = (run_drill(args.name),) if args.name else run_all_drills()
    failed = 0
    for result in results:
        print(f"\n[{result.verdict}] {result.drill} — {result.title}")
        print(f"  simulated: {result.simulated}")
        print(f"  expected:  {result.expected}")
        print(f"  observed:  {result.observed}")
        if result.trader_prompt:
            print(f"  ASK A TRADER: {result.trader_prompt}")
        failed += 0 if result.passed else 1
    print(f"\n{len(results) - failed} of {len(results)} drill(s) passed.")
    if not args.name:
        print(f"(available: {', '.join(DRILLS)})")
    return 1 if failed else 0


def cmd_reproduce(args: argparse.Namespace) -> int:
    from analysis.trial.release import reproduce

    sessions, _ = _load(args)
    matches = [s for s in sessions.sessions if s.session_id == args.session]
    if not matches:
        raise SystemExit(f"no session with id {args.session!r}; run `trial.py check` to list them")
    session = matches[0]
    check = reproduce(session.release)
    print(f"session {session.session_id} ({session.task.value}, {session.trading_day})")
    print(f"  verdict: {check.verdict}")
    print(f"  reason:  {check.reason}")
    print(f"  code:    {session.release.short_code} (matches now: {check.code_matches})")
    print(f"  data:    {session.release.data_fingerprint[:12]} (matches now: {check.data_matches})")
    if not check.reproducible and session.release.is_reproducible:
        print(f"\n  to re-run: {check.replay_command}")
    return 0


def cmd_dashboard(args: argparse.Namespace) -> int:
    from app.trial_page import build_trial_page

    sessions, days = _load(args)
    target = build_trial_page(
        list(sessions.sessions),
        list(days.days),
        worked_opportunities=args.worked_opportunities,
        progressed_opportunities=args.progressed_opportunities,
    )
    print(f"private trial dashboard written to {target}")
    print("This file is outside docs/ and is never deployed.")
    return 0


# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="trial.py",
        description="Mirror Market trader-validation runbook (Phase 5). Records live in "
        "config.TRIAL_RECORD_DIR and are gitignored; nothing here writes into docs/.",
        epilog="Start with: trial.py protocol, then trial.py start --interactive",
    )
    parser.add_argument("--directory", default=None, help="trial record directory (default: config.TRIAL_RECORD_DIR)")
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    def _opportunity_args(target: argparse.ArgumentParser) -> None:
        target.add_argument(
            "--worked-opportunities",
            type=int,
            default=0,
            help="opportunities the desk actually worked (from the private Phase 4 store; "
            "passed in rather than read, so this script never opens that file)",
        )
        target.add_argument("--progressed-opportunities", type=int, default=0)

    protocol = sub.add_parser("protocol", help="regenerate docs/trial/PROTOCOL.md")
    protocol.add_argument("--output", default=None)
    protocol.set_defaults(func=cmd_protocol)

    start = sub.add_parser("start", help="write a prefilled session stub")
    start.add_argument("--trader", default=None, help="your handle, 3+ chars — never your name")
    start.add_argument("--task", default=None)
    start.add_argument("--day", default=None, help="ISO trading day (default: today)")
    start.add_argument("--interactive", action="store_true")
    start.set_defaults(func=cmd_start)

    day = sub.add_parser("day", help="record today's source availability and deployment state")
    day.add_argument("--date", default=None)
    day.add_argument("--edition-current", action="store_true", help="today's pipeline ran and the site rebuilt")
    day.add_argument("--drill", default=None, help="name this day as a drill so it leaves the reliability metrics")
    day.add_argument("--note", default=None)
    day.set_defaults(func=cmd_day)

    check = sub.add_parser("check", help="load and validate every record")
    check.set_defaults(func=cmd_check)

    metrics = sub.add_parser("metrics", help="print the metric set")
    _opportunity_args(metrics)
    metrics.set_defaults(func=cmd_metrics)

    backlog = sub.add_parser("backlog", help="promote validated findings")
    backlog.set_defaults(func=cmd_backlog)

    review = sub.add_parser("review", help="the weekly review and go/no-go")
    review.add_argument("--week-start", default=None)
    review.add_argument("--no-compare", action="store_true", help="skip the prior-week trend")
    review.add_argument("--write", action="store_true", help="also write it to the private output dir")
    _opportunity_args(review)
    review.set_defaults(func=cmd_review)

    card = sub.add_parser("scorecard", help="the 30-day scorecard")
    card.add_argument("--start", default=None)
    card.add_argument("--end", default=None)
    card.add_argument("--write", action="store_true")
    _opportunity_args(card)
    card.set_defaults(func=cmd_scorecard)

    drills = sub.add_parser("drills", help="run the failure drills")
    drills.add_argument("--name", default=None)
    drills.set_defaults(func=cmd_drills)

    repro = sub.add_parser("reproduce", help="can a recorded session be produced again?")
    repro.add_argument("--session", required=True)
    repro.set_defaults(func=cmd_reproduce)

    dashboard = sub.add_parser("dashboard", help="render the private metrics dashboard")
    _opportunity_args(dashboard)
    dashboard.set_defaults(func=cmd_dashboard)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    for name in ("worked_opportunities", "progressed_opportunities"):
        if not hasattr(args, name):
            setattr(args, name, 0)
    try:
        return int(args.func(args))
    except Exception as exc:
        if args.verbose:
            raise
        raise SystemExit(f"{type(exc).__name__}: {exc}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
