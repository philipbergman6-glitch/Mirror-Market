"""The per-participant decision floor (A6 #303, built by #412).

A verdict is read only off participants who logged enough: at least
``TRIAL_DECISION_FLOOR["sessions"]`` sessions, across at least
``TRIAL_DECISION_FLOOR["tasks"]`` distinct tasks, including at least
``TRIAL_DECISION_FLOOR["real_decisions"]`` completed sessions in the real
cargo/basis tasks. Trial-wide, at least ``TRIAL_MIN_PARTICIPANTS`` must stand
at the floor or the answer is ``insufficient`` — no verdict. A participant
below the floor is reported, never graded, and never silently dropped.
"""

from __future__ import annotations

from datetime import timedelta

import pytest

from analysis.trial.domain import AUDIENCE_AGGREGATE, AUDIENCE_PRIVATE, Outcome, TaskId, TrialError
from analysis.trial.floor import FloorResult, decision_floor, real_decision_tasks
from analysis.trial.sanitize import assert_no_identifiers, assert_sanitized
from tests.trial_fixtures import SYNTHETIC_PARTICIPANTS, TODAY, issue, session

ZEPHYR, QUARTZ = SYNTHETIC_PARTICIPANTS


def _at_floor(participant: str, *, sessions: int = 20, real: int = 8, tasks: int = 5) -> list:
    """Exactly ``sessions`` sessions over ``tasks`` distinct tasks with ``real`` real decisions."""
    real_tasks = list(real_decision_tasks())
    other_tasks = [t for t in TaskId if t not in real_tasks]
    out = []
    for index in range(sessions):
        if index < real:
            task = real_tasks[index % len(real_tasks)]
        else:
            task = other_tasks[index % max(1, tasks - len(real_tasks))]
        out.append(
            session(
                participant=participant,
                task=task,
                trading_day=TODAY - timedelta(days=index // 2),
                hour=7 + (index % 2),
            )
        )
    return out


def test_the_real_decision_tasks_are_origin_crush_and_opportunity() -> None:
    # A6: "≥8 real cargo/basis decisions (tasks 2 origin comparison, 3
    # crush/hedge, 7 opportunity)". Task 7 in the protocol's order is the
    # counterparty / opportunity identification task.
    assert real_decision_tasks() == (
        TaskId.ORIGIN_COMPARISON,
        TaskId.CRUSH_HEDGE,
        TaskId.COUNTERPARTY_ID,
    )


def test_the_floor_numbers_come_from_config() -> None:
    import config

    assert config.TRIAL_DECISION_FLOOR == {"sessions": 20, "tasks": 5, "real_decisions": 8}
    assert config.TRIAL_MIN_PARTICIPANTS == 2


def test_two_participants_at_the_floor_meet_it() -> None:
    result = decision_floor(_at_floor(ZEPHYR) + _at_floor(QUARTZ))
    assert isinstance(result, FloorResult)
    assert result.met
    assert len(result.at_floor) == 2
    assert all(standing.at_floor for standing in result.standings)


def test_one_participant_short_on_sessions_leaves_the_trial_insufficient() -> None:
    result = decision_floor(_at_floor(ZEPHYR) + _at_floor(QUARTZ, sessions=19))
    assert not result.met
    quartz = result.for_participant(QUARTZ)
    assert quartz.sessions == 19
    assert not quartz.at_floor
    assert any("19 of 20 sessions" in s for s in quartz.shortfalls)
    assert "insufficient" in result.reason
    assert "1 of 2 participant" in result.reason
    assert QUARTZ not in result.reason  # the reason travels in the aggregate projection
    assert any(QUARTZ in line and "19 of 20" in line for line in result.standing_lines())  # reported, not dropped


def test_twenty_sessions_on_four_tasks_are_below_the_floor() -> None:
    sessions = _at_floor(ZEPHYR, tasks=4)
    assert len({s.task for s in sessions}) == 4
    standing = decision_floor(sessions).for_participant(ZEPHYR)
    assert standing.tasks_covered == 4
    assert not standing.at_floor
    assert any("4 of 5 tasks" in s for s in standing.shortfalls)


def test_an_abandoned_origin_comparison_is_not_a_real_decision() -> None:
    # Eight sessions in real-decision tasks, one of them abandoned: seven decisions.
    sessions = _at_floor(ZEPHYR)
    first = sessions[0]
    assert first.task in real_decision_tasks()
    sessions[0] = session(
        participant=ZEPHYR,
        task=first.task,
        trading_day=first.trading_day,
        hour=first.started_at.hour,
        outcome=Outcome.ABANDONED,
        would_act=False,
        issues=(issue(),),
    )
    standing = decision_floor(sessions).for_participant(ZEPHYR)
    assert standing.real_decisions == 7
    assert not standing.at_floor
    assert any("7 of 8 real" in s for s in standing.shortfalls)


def test_twenty_sessions_with_seven_real_decisions_are_below_the_floor() -> None:
    standing = decision_floor(_at_floor(ZEPHYR, real=7)).for_participant(ZEPHYR)
    assert standing.sessions == 20
    assert standing.real_decisions == 7
    assert not standing.at_floor


def test_handles_are_compared_case_and_whitespace_insensitively() -> None:
    sessions = _at_floor("Zephyr", sessions=10) + _at_floor("zephyr ", sessions=10)
    # Ten and ten are one participant with twenty sessions, not two with ten.
    result = decision_floor(sessions)
    assert len(result.standings) == 1
    assert result.standings[0].sessions == 20


def test_no_sessions_is_insufficient_with_zero_participants() -> None:
    result = decision_floor([])
    assert not result.met
    assert result.standings == ()
    assert "0 participant" in result.reason


def test_the_private_projection_names_handles_and_the_aggregate_does_not() -> None:
    sessions = _at_floor(ZEPHYR) + _at_floor(QUARTZ, sessions=12)
    result = decision_floor(sessions)
    private = result.to_dict(audience=AUDIENCE_PRIVATE)
    assert {s["participant"] for s in private["standings"]} == {ZEPHYR, QUARTZ}
    shared = result.to_dict(audience=AUDIENCE_AGGREGATE)
    assert_sanitized(shared, where="floor aggregate")
    assert_no_identifiers(shared, SYNTHETIC_PARTICIPANTS, where="floor aggregate")
    assert shared["participants"] == 2
    assert shared["at_floor"] == 1
    assert shared["required_participants"] == 2
    assert shared["met"] is False


def test_a_misconfigured_real_decision_task_is_a_crash_not_a_silent_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    import config

    monkeypatch.setattr(config, "TRIAL_REAL_DECISION_TASKS", ("origin_comparison", "not_a_task"))
    with pytest.raises(TrialError, match="not_a_task"):
        real_decision_tasks()


@pytest.mark.parametrize("kind", ["weekly", "scorecard"])
def test_below_floor_participant_does_not_change_graded_results(kind):
    from analysis.trial.review import scorecard, weekly_review

    records = _at_floor(ZEPHYR) + _at_floor(QUARTZ)
    newcomer = session(participant="newcomer", outcome=Outcome.ABANDONED,
                       would_act=False, issues=(issue(),))
    def build(rows):
        if kind == "weekly":
            return weekly_review(rows, week_start=TODAY - timedelta(days=30), week_end=TODAY)
        return scorecard(rows, window_start=TODAY - timedelta(days=30), window_end=TODAY)
    before, after = build(records), build(records + [newcomer])
    assert after.participant_count == 3
    assert len(after.floor.below_floor) == 1
    if kind == "weekly":
        assert after.metrics == before.metrics
    else:
        assert after.dimensions == before.dimensions


@pytest.mark.parametrize("kind", ["weekly", "scorecard"])
def test_below_floor_blocker_still_escalates(kind):
    from analysis.trial.domain import IssueClass, Severity
    from analysis.trial.review import scorecard, weekly_review

    rows = _at_floor(ZEPHYR) + _at_floor(QUARTZ) + [session(
        participant="newcomer", issues=(issue(IssueClass.NUMERICAL_ERROR, Severity.BLOCKER),),
    )]
    if kind == "weekly":
        result = weekly_review(rows, week_start=TODAY - timedelta(days=30), week_end=TODAY)
    else:
        result = scorecard(rows, window_start=TODAY - timedelta(days=30), window_end=TODAY)
    assert result.verdict == "no_go"
    assert "blocker" in result.verdict_reason
