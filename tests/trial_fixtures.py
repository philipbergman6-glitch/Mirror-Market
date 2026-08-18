"""Builders for the Phase 5 trial tests. **Every record here is SYNTHETIC.**

No trader in these fixtures is a person, no decision is a decision anyone made,
and no metric produced from them is a trial result. That is stated in the module
docstring, repeated in the ``SYNTHETIC`` prefix on every free-text field, and
enforced by :data:`SYNTHETIC_TRADERS` being handles no desk would use — because
the one genuinely dangerous failure mode of a validation harness is a fabricated
result being read later as a real one.

Hand-verifiable by construction, on the same terms as
``tests/opportunity_fixtures.py``: every number a test asserts on is set here,
once, so the assertion can be checked against the fixture by eye.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from analysis.trial.domain import (
    DayObservation,
    ExternalLookup,
    ExternalTool,
    Issue,
    IssueClass,
    Outcome,
    ReleaseStamp,
    SessionRecord,
    Severity,
    TaskId,
)

#: A marker on every free-text field. Tests grep for it; a human reading a
#: leaked file sees it immediately.
MARK = "SYNTHETIC"

#: Deliberately not names. Three characters or more, and not substrings of
#: ordinary English, because ``sanitize.assert_no_identifiers`` searches free
#: text for them and would otherwise fire on the word "adjusted".
SYNTHETIC_TRADERS = ("zephyr", "quartz")

TODAY = date(2026, 8, 18)
WINDOW_START = date(2026, 7, 20)

CLEAN_STAMP = ReleaseStamp(
    code_revision="0" * 40,
    data_fingerprint="f" * 64,
    captured_at=datetime(2026, 8, 17, 12, 0, tzinfo=timezone.utc),
    dirty=False,
    layer_count=27,
)

DIRTY_STAMP = ReleaseStamp(
    code_revision="1" * 40,
    data_fingerprint="e" * 64,
    captured_at=datetime(2026, 8, 17, 13, 0, tzinfo=timezone.utc),
    dirty=True,
    layer_count=27,
)


def lookup(
    tool: ExternalTool = ExternalTool.BLOOMBERG,
    question: str = f"{MARK} what is the Nov Santos freight rate?",
    *,
    answer_found: bool = True,
    minutes: float | None = 4.0,
) -> ExternalLookup:
    return ExternalLookup(
        tool=tool, unanswered_question=question, answer_found=answer_found, minutes=minutes
    )


def issue(
    classification: IssueClass = IssueClass.MISLEADING_UX,
    severity: Severity = Severity.MINOR,
    *,
    summary: str = f"{MARK} the crush chip does not say which session it is struck on",
    page: str | None = "markets/cbot.html",
    evidence: str = f"{MARK} evidence text",
    affected_decision: str = f"{MARK} whether to crush this week",
    expected: str | None = None,
    observed: str | None = None,
) -> Issue:
    return Issue(
        classification=classification,
        severity=severity,
        summary=summary,
        evidence=evidence,
        affected_decision=affected_decision,
        page=page,
        expected=expected,
        observed=observed,
    )


#: A correctness issue. Kept as a constant because several tests assert on the
#: promotion rule it triggers, and they must all mean the same finding.
NUMERICAL_ISSUE = issue(
    IssueClass.NUMERICAL_ERROR,
    Severity.MAJOR,
    summary=f"{MARK} Paranagua FOB reads 3 USD/MT above the source",
    page="origins.html",
    affected_decision=f"{MARK} which origin to buy",
    expected="418",
    observed="421",
)

BLOCKER_ISSUE = issue(
    IssueClass.SEMANTIC_MISMATCH,
    Severity.BLOCKER,
    summary=f"{MARK} the SAFEX row is labelled a settlement",
    page="markets/south_africa.html",
    affected_decision=f"{MARK} sizing a ZAR hedge",
)


def session(
    trader: str = SYNTHETIC_TRADERS[0],
    task: TaskId = TaskId.MORNING_BRIEF,
    trading_day: date = TODAY,
    *,
    minutes: int = 9,
    outcome: Outcome = Outcome.COMPLETED,
    confidence: int = 4,
    would_act: bool = True,
    issues: tuple[Issue, ...] = (),
    lookups: tuple[ExternalLookup, ...] = (),
    pages: tuple[str, ...] = ("index.html",),
    release: ReleaseStamp = CLEAN_STAMP,
    hour: int = 7,
) -> SessionRecord:
    started = datetime.combine(trading_day, datetime.min.time(), tzinfo=timezone.utc).replace(hour=hour)
    return SessionRecord(
        trader=trader,
        task=task,
        trading_day=trading_day,
        started_at=started,
        ended_at=started + timedelta(minutes=minutes),
        outcome=outcome,
        confidence=confidence,
        would_act=would_act,
        release=release,
        decision=f"{MARK} decision text" if outcome is Outcome.COMPLETED else "",
        pages_used=pages,
        external_lookups=lookups,
        issues=issues,
        notes=(f"{MARK} note",),
        evidence=(f"{MARK} evidence line",),
    )


def day_observation(
    trading_day: date = TODAY,
    *,
    published: bool = True,
    current: bool = True,
    expected_layers: int = 12,
    available_layers: int = 12,
    degraded: tuple[str, ...] = (),
    drill: str | None = None,
    note: str = "",
) -> DayObservation:
    return DayObservation(
        trading_day=trading_day,
        edition_published=published,
        edition_current=current,
        critical_layers_expected=expected_layers,
        critical_layers_available=available_layers,
        release=CLEAN_STAMP,
        degraded_layers=degraded,
        drill=drill,
        note=note,
    )


def trading_days(start: date = WINDOW_START, end: date = TODAY) -> list[date]:
    """Weekdays in the window. The trial's own definition of a trading day."""
    out, cursor = [], start
    while cursor <= end:
        if cursor.weekday() < 5:
            out.append(cursor)
        cursor += timedelta(days=1)
    return out


def full_window() -> tuple[list[SessionRecord], list[DayObservation]]:
    """A complete, healthy 30-ish-day window: two traders, all ten tasks, few issues.

    Shaped to clear every observation floor so that tests of the *grading* logic
    are not accidentally testing the insufficiency path. Tests that want a thin
    window build one explicitly.
    """
    tasks = list(TaskId)
    sessions: list[SessionRecord] = []
    days: list[DayObservation] = []
    for index, day in enumerate(trading_days()):
        for offset, trader in enumerate(SYNTHETIC_TRADERS):
            issues: tuple[Issue, ...] = ()
            if index % 6 == 0 and offset == 0:
                issues = (issue(),)
            sessions.append(
                session(
                    trader=trader,
                    task=tasks[(index + offset) % len(tasks)],
                    trading_day=day,
                    minutes=9 + (index % 5),
                    confidence=5 if index % 3 == 0 else 4,
                    issues=issues,
                    lookups=(lookup(),) if index % 5 == 0 else (),
                    pages=("index.html", "origins.html"),
                    hour=7 + offset,
                )
            )
        degraded = () if index % 8 else ("cepea", "agrural")
        days.append(
            day_observation(
                trading_day=day,
                available_layers=12 if index % 8 else 10,
                degraded=degraded,
                note=f"{MARK} note" if degraded else "",
            )
        )
    return sessions, days
