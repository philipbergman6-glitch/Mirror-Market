"""The private YAML record store: round-trips, and refuses what it cannot trust.

The store's job is to be boring and strict. A record it silently mangles is a
metric computed over the wrong denominator, with nothing anywhere saying so —
which is why every one of these refusals is a raise rather than a warning.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
import yaml

from analysis.trial.domain import IssueClass, Outcome, Severity, TaskId, TrialError
from analysis.trial.records import (
    DAYS_SUBDIR,
    SESSIONS_SUBDIR,
    DayObservationSet,
    SessionSet,
    day_to_document,
    load_day_observations,
    load_sessions,
    parse_day_observation,
    parse_sessions,
    session_to_document,
)
from tests.trial_fixtures import (
    MARK,
    SYNTHETIC_TRADERS,
    day_observation,
    full_window,
    issue,
    lookup,
    session,
)


def _write(root: Path, subdir: str, name: str, document: object) -> Path:
    directory = root / subdir
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / name
    target.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")
    return target


# --- round trip -----------------------------------------------------------
def test_a_session_survives_a_round_trip_through_yaml_unchanged() -> None:
    original = session(
        issues=(issue(), issue(IssueClass.STALE_DATA, Severity.MAJOR)),
        lookups=(lookup(),),
        pages=("index.html", "origins.html"),
    )
    document = session_to_document(original)
    restored = parse_sessions([document], where="test")[0]

    assert restored.trader == original.trader
    assert restored.task is original.task
    assert restored.started_at == original.started_at
    assert restored.ended_at == original.ended_at
    assert restored.confidence == original.confidence
    assert restored.would_act == original.would_act
    assert restored.decision == original.decision
    assert restored.pages_used == original.pages_used
    assert len(restored.issues) == 2
    assert restored.issues[0].summary == original.issues[0].summary
    assert restored.external_lookups[0].unanswered_question == original.external_lookups[0].unanswered_question
    assert restored.release.matches(original.release)
    assert restored.session_id == original.session_id


def test_a_day_observation_survives_a_round_trip_unchanged() -> None:
    original = day_observation(available_layers=9, degraded=("cepea",), note=f"{MARK} note")
    restored = parse_day_observation(day_to_document(original), where="test")
    assert restored.trading_day == original.trading_day
    assert restored.critical_layers_available == 9
    assert restored.degraded_layers == ("cepea",)
    assert restored.note == original.note


def test_the_whole_fixture_window_round_trips_through_disk(tmp_path: Path) -> None:
    sessions, days = full_window()
    grouped: dict[date, list] = {}
    for record in sessions:
        grouped.setdefault(record.trading_day, []).append(record)
    for day, group in grouped.items():
        _write(tmp_path, SESSIONS_SUBDIR, f"{day.isoformat()}.yml", [session_to_document(s) for s in group])
    for observation in days:
        _write(tmp_path, DAYS_SUBDIR, f"{observation.trading_day.isoformat()}.yml", day_to_document(observation))

    loaded = load_sessions(tmp_path)
    loaded_days = load_day_observations(tmp_path)
    assert len(loaded.sessions) == len(sessions)
    assert len(loaded_days.days) == len(days)
    assert set(loaded.traders) == set(SYNTHETIC_TRADERS)


# --- refusals -------------------------------------------------------------
def test_an_unknown_field_raises_rather_than_being_dropped() -> None:
    document = session_to_document(session())
    document["confidnce"] = 4  # a typo that would silently lose the value
    with pytest.raises(TrialError, match="confidnce"):
        parse_sessions([document], where="test")


def test_a_missing_required_field_raises() -> None:
    document = session_to_document(session())
    del document["confidence"]
    with pytest.raises(TrialError, match="confidence"):
        parse_sessions([document], where="test")


def test_a_naive_timestamp_in_a_file_is_refused() -> None:
    document = session_to_document(session())
    document["started_at"] = "2026-08-18T07:00:00"
    with pytest.raises(TrialError):
        parse_sessions([document], where="test")


def test_an_unknown_enum_value_names_the_field_and_the_value() -> None:
    document = session_to_document(session())
    document["task"] = "reading_the_news"
    with pytest.raises(TrialError, match="reading_the_news"):
        parse_sessions([document], where="test")


def test_a_non_boolean_would_act_is_refused_rather_than_coerced() -> None:
    # "yes" is truthy in Python and would parse as True forever. The trial's
    # would-act rate is a headline metric; a coerced string is a silent lie.
    document = session_to_document(session())
    document["would_act"] = "yes"
    with pytest.raises(TrialError):
        parse_sessions([document], where="test")


def test_a_document_that_is_not_a_list_of_sessions_is_refused() -> None:
    with pytest.raises(TrialError):
        parse_sessions({"trader": "zephyr"}, where="test")  # type: ignore[arg-type]


def test_a_malformed_file_raises_rather_than_loading_as_an_empty_trial(tmp_path: Path) -> None:
    directory = tmp_path / SESSIONS_SUBDIR
    directory.mkdir(parents=True)
    (directory / "2026-08-18.yml").write_text("this: is: not: valid: yaml:", encoding="utf-8")
    with pytest.raises(TrialError):
        load_sessions(tmp_path)


def test_a_missing_directory_is_an_empty_set_not_an_error(tmp_path: Path) -> None:
    loaded = load_sessions(tmp_path / "nothing-here")
    assert loaded.is_empty
    assert load_day_observations(tmp_path / "nothing-here").days == ()


def test_two_sessions_with_the_same_identity_are_refused(tmp_path: Path) -> None:
    # Same trader, same task, same start: one session logged twice. Counting it
    # twice would inflate every denominator in the trial.
    document = session_to_document(session())
    _write(tmp_path, SESSIONS_SUBDIR, "2026-08-18.yml", [document, dict(document)])
    with pytest.raises(TrialError, match="duplicate"):
        load_sessions(tmp_path)


def test_two_observations_for_one_trading_day_are_refused(tmp_path: Path) -> None:
    document = day_to_document(day_observation())
    _write(tmp_path, DAYS_SUBDIR, "2026-08-18.yml", document)
    _write(tmp_path, DAYS_SUBDIR, "also-2026-08-18.yml", document)
    with pytest.raises(TrialError, match="two day observations"):
        load_day_observations(tmp_path)


def test_the_error_names_the_file_it_came_from(tmp_path: Path) -> None:
    document = session_to_document(session())
    document["confidence"] = 99
    path = _write(tmp_path, SESSIONS_SUBDIR, "2026-08-18.yml", [document])
    with pytest.raises(TrialError, match=path.name):
        load_sessions(tmp_path)


# --- containers -----------------------------------------------------------
def test_a_session_set_filters_by_trader_task_and_window() -> None:
    sessions, _ = full_window()
    container = SessionSet(sessions=tuple(sessions))
    assert set(container.traders) == set(SYNTHETIC_TRADERS)
    assert all(s.trader == SYNTHETIC_TRADERS[0] for s in container.for_trader(SYNTHETIC_TRADERS[0]))
    assert all(s.task is TaskId.MORNING_BRIEF for s in container.for_task(TaskId.MORNING_BRIEF))
    window = container.between(date(2026, 8, 10), date(2026, 8, 14))
    assert window.sessions
    assert all(date(2026, 8, 10) <= s.trading_day <= date(2026, 8, 14) for s in window.sessions)


def test_an_empty_day_set_reports_empty_rather_than_pretending_to_be_a_window() -> None:
    assert DayObservationSet().days == ()


def test_an_incomplete_session_round_trips_with_its_issue(tmp_path: Path) -> None:
    record = session(outcome=Outcome.BLOCKED, would_act=False, issues=(issue(),))
    restored = parse_sessions([session_to_document(record)], where="test")[0]
    assert restored.outcome is Outcome.BLOCKED
    assert restored.issues[0].classification is IssueClass.MISLEADING_UX
