"""The protocol document cannot describe a task differently from how it is measured.

Requirement 1 asks for a versioned protocol with instructions, definitions,
confidentiality boundaries and success criteria. It is generated rather than
written, and these tests are what make that generation worth the trouble: they
pin the document to the enums the metrics are computed over, so a task renamed
in code cannot leave a stale instruction standing in a markdown file. The drift
that would otherwise occur is invisible — both halves keep working, and only the
trial's numbers quietly stop meaning anything.
"""

from __future__ import annotations

from pathlib import Path

from analysis.trial.domain import ExternalTool, IssueClass, Outcome, Severity, TaskId
from analysis.trial.protocol import (
    protocol_markdown,
    protocol_version,
    task_reference,
    write_protocol,
)

REPO = Path(__file__).resolve().parents[1]
DOC = protocol_markdown()


def test_the_protocol_states_the_version_it_was_generated_at() -> None:
    # Findings are reported against a protocol version; an unversioned document
    # makes week one and week four incomparable with nothing saying so.
    import config

    assert protocol_version() == config.TRIAL_PROTOCOL_VERSION
    assert protocol_version() in DOC


def test_every_task_appears_with_its_question_criterion_target_and_cadence() -> None:
    for task in TaskId:
        assert task.label in DOC, task.value
        assert task.decision_question in DOC, task.value
        assert task.success_criteria in DOC, task.value
        assert str(task.target_minutes) in DOC


def test_every_issue_class_and_severity_is_defined_for_the_trader() -> None:
    # A trader classifying a finding needs the definition in front of them, or
    # the classification metric measures how each person guessed.
    for kind in IssueClass:
        assert kind.value in DOC
        assert kind.meaning in DOC
    for severity in Severity:
        assert severity.value in DOC


def test_every_outcome_and_external_tool_the_record_accepts_is_documented() -> None:
    for outcome in Outcome:
        assert outcome.value in DOC
    for tool in ExternalTool:
        assert tool.value in DOC


def test_the_decision_thresholds_are_published_with_their_direction() -> None:
    # A bar without its direction is unreadable: 0.1 is a good lookup rate and a
    # terrible completion rate.
    import config

    for key, bars in config.TRIAL_DECISION_THRESHOLDS.items():
        assert key in DOC
        assert str(bars["go"]) in DOC
    assert "lower is better" in DOC
    assert "higher is better" in DOC


def test_the_protocol_states_the_window_and_the_participation_floors() -> None:
    import config

    assert str(config.TRIAL_WINDOW_TRADING_DAYS) in DOC
    assert str(config.TRIAL_MIN_TRADERS) in DOC
    assert str(config.TRIAL_MIN_OBSERVATIONS) in DOC


def test_the_protocol_states_the_confidentiality_boundary_and_where_records_live() -> None:
    lowered = DOC.lower()
    assert "confidential" in lowered or "privacy" in lowered
    assert "data/reference/trial" in DOC
    assert "data/workspace" in DOC


def test_the_protocol_quotes_no_path_from_the_machine_that_generated_it() -> None:
    # The configured directories are absolutised against the repository root, so
    # quoting them verbatim wrote a home directory into a committed document —
    # which then differed on every other checkout.
    assert str(REPO) not in DOC
    for marker in ("/Users/", "/home/", "C:\\"):
        assert marker not in DOC, f"{marker} appears in the generated protocol"


def test_the_protocol_carries_no_trial_data_of_its_own() -> None:
    # It lives inside docs/, which is safe only because it is instructions. If it
    # ever quoted a session it would be a record inside the published directory.
    assert "SYNTHETIC" not in DOC


def test_the_protocol_is_not_on_the_promotion_contract_so_it_is_not_uploaded() -> None:
    from trust.site_promotion import expected_site_paths

    assert "trial/PROTOCOL.md" not in set(expected_site_paths())


def test_writing_the_protocol_is_deterministic_and_returns_its_path(tmp_path: Path) -> None:
    first = write_protocol(tmp_path / "PROTOCOL.md")
    text = first.read_text(encoding="utf-8")
    second = write_protocol(tmp_path / "PROTOCOL.md")
    assert second == first
    assert second.read_text(encoding="utf-8") == text


def test_the_committed_protocol_matches_what_the_code_generates_today() -> None:
    # The check that makes all of the above load-bearing: if a task changes and
    # nobody regenerates, this fails rather than the trial running on stale
    # instructions. Regenerate with `python scripts/trial.py protocol`.
    committed = REPO / "docs" / "trial" / "PROTOCOL.md"
    assert committed.exists(), "run: python scripts/trial.py protocol"
    assert committed.read_text(encoding="utf-8") == DOC


def test_the_task_reference_exposes_the_same_specs_as_plain_data() -> None:
    reference = task_reference()
    assert set(reference) == {task.value for task in TaskId}
    entry = reference[TaskId.MORNING_BRIEF.value]
    assert entry["label"] == TaskId.MORNING_BRIEF.label
    assert entry["target_minutes"] == TaskId.MORNING_BRIEF.target_minutes
