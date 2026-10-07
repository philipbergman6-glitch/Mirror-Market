"""Desk-side transcription: a form export becomes session YAML (A6 #303, #412).

Participants never touch the repo. They fill a short form after each decision
and the desk transcribes weekly. The transcription goes through the *same*
parser as a hand-written file — the same closed field set, the same hard
failures — so a form cannot smuggle in a record the YAML path would refuse.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest
import yaml

from analysis.trial.domain import ExternalTool, IssueClass, Outcome, Severity, TaskId, TrialError
from analysis.trial.records import DAYS_SUBDIR, SESSIONS_SUBDIR, day_to_document, load_sessions
from analysis.trial.transcribe import (
    FORM_COLUMNS,
    parse_form_rows,
    transcribe_csv,
)
from tests.trial_fixtures import MARK, SYNTHETIC_PARTICIPANTS, TODAY, day_observation

ZEPHYR = SYNTHETIC_PARTICIPANTS[0]


def _row(**overrides: str) -> dict[str, str]:
    row = {
        "participant": ZEPHYR,
        "task": "origin_comparison",
        "trading_day": TODAY.isoformat(),
        "started_at": f"{TODAY.isoformat()}T07:00:00+00:00",
        "ended_at": f"{TODAY.isoformat()}T07:18:00+00:00",
        "outcome": "completed",
        "confidence": "4",
        "would_act": "yes",
        "decision": f"{MARK} Paranaguá Nov is cheapest landed",
        "pages_used": "index.html | origins.html",
        "notes": "",
        "evidence": "",
    }
    row.update(overrides)
    return row


def _trial_dir(tmp_path: Path) -> Path:
    root = tmp_path / "trial"
    days = root / DAYS_SUBDIR
    days.mkdir(parents=True)
    (days / f"{TODAY.isoformat()}.yml").write_text(
        yaml.safe_dump(day_to_document(day_observation(trading_day=TODAY))), encoding="utf-8"
    )
    return root


def _write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    columns: list[str] = []
    for row in rows:
        for key in row:
            if key not in columns:
                columns.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)
    return path


# --- parsing ---------------------------------------------------------------
def test_a_minimal_form_row_becomes_a_valid_session(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    (record,) = parse_form_rows([_row()], directory=root, where="form")
    assert record.participant == ZEPHYR
    assert record.task is TaskId.ORIGIN_COMPARISON
    assert record.outcome is Outcome.COMPLETED
    assert record.would_act is True
    assert record.pages_used == ("index.html", "origins.html")
    assert record.external_lookups == ()
    assert record.issues == ()


def test_the_release_stamp_is_the_days_recorded_release_never_invented(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    (record,) = parse_form_rows([_row()], directory=root, where="form")
    expected = day_observation(trading_day=TODAY).release
    assert record.release.code_revision == expected.code_revision
    assert record.release.data_fingerprint == expected.data_fingerprint


def test_a_day_with_no_observation_cannot_be_transcribed(tmp_path: Path) -> None:
    root = tmp_path / "trial"  # no days/ at all
    with pytest.raises(TrialError, match="no day observation"):
        parse_form_rows([_row()], directory=root, where="form")


def test_numbered_lookup_and_issue_groups_are_read(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    row = _row(
        lookup_1_tool="bloomberg",
        lookup_1_question=f"{MARK} Nov Santos freight?",
        lookup_1_answer_found="true",
        lookup_1_minutes="3",
        lookup_2_tool="other",
        lookup_2_tool_detail=f"{MARK} a text to a colleague",
        lookup_2_question=f"{MARK} is the plant still buying?",
        lookup_2_answer_found="no",
        issue_1_classification="stale_data",
        issue_1_severity="major",
        issue_1_summary=f"{MARK} CEPEA two days old and unlabelled",
        issue_1_evidence=f"{MARK} page footer",
        issue_1_affected_decision=f"{MARK} the Brazil basis leg",
        issue_1_page="origins.html",
    )
    (record,) = parse_form_rows([row], directory=root, where="form")
    assert [lk.tool for lk in record.external_lookups] == [ExternalTool.BLOOMBERG, ExternalTool.OTHER]
    assert record.external_lookups[0].minutes == 3.0
    assert record.external_lookups[1].answer_found is False
    assert record.issues[0].classification is IssueClass.STALE_DATA
    assert record.issues[0].severity is Severity.MAJOR


def test_a_wholly_blank_group_is_skipped_but_a_partial_one_fails(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    blank = _row(lookup_1_tool="", lookup_1_question="", lookup_1_answer_found="")
    (record,) = parse_form_rows([blank], directory=root, where="form")
    assert record.external_lookups == ()

    partial = _row(lookup_1_tool="bloomberg", lookup_1_question="", lookup_1_answer_found="true")
    with pytest.raises(TrialError, match="unanswered_question"):
        parse_form_rows([partial], directory=root, where="form")


def test_an_unknown_column_is_refused_not_ignored(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    with pytest.raises(TrialError, match="note"):
        parse_form_rows([_row(note="typo for notes")], directory=root, where="form")


def test_a_missing_required_column_is_refused(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    row = _row()
    del row["confidence"]
    with pytest.raises(TrialError, match="confidence"):
        parse_form_rows([row], directory=root, where="form")


def test_an_abandoned_row_without_an_issue_is_refused_like_the_yaml_path(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    row = _row(outcome="abandoned", would_act="no", decision="")
    with pytest.raises(TrialError, match="issue"):
        parse_form_rows([row], directory=root, where="form")


def test_an_unparseable_boolean_is_refused(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    with pytest.raises(TrialError, match="would_act"):
        parse_form_rows([_row(would_act="maybe")], directory=root, where="form")


def test_every_form_column_maps_to_a_session_field() -> None:
    from analysis.trial.records import SESSION_FIELDS

    assert set(FORM_COLUMNS) <= SESSION_FIELDS | {"participant"}
    assert "release" not in FORM_COLUMNS  # never typed by a participant
    assert "protocol_version" not in FORM_COLUMNS


# --- writing ---------------------------------------------------------------
def test_transcribe_csv_appends_to_the_days_session_file_and_round_trips(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    source = _write_csv(tmp_path / "export.csv", [_row(), _row(started_at=f"{TODAY.isoformat()}T09:00:00+00:00", ended_at=f"{TODAY.isoformat()}T09:10:00+00:00", task="morning_brief")])
    written = transcribe_csv(source, directory=root)
    assert written == [root / SESSIONS_SUBDIR / f"{TODAY.isoformat()}.yml"]
    loaded = load_sessions(root)
    assert len(loaded.sessions) == 2
    assert {s.task for s in loaded.sessions} == {TaskId.ORIGIN_COMPARISON, TaskId.MORNING_BRIEF}

    # A second transcription of a different session appends; the first survives.
    later = _write_csv(tmp_path / "export2.csv", [_row(started_at=f"{TODAY.isoformat()}T11:00:00+00:00", ended_at=f"{TODAY.isoformat()}T11:10:00+00:00")])
    transcribe_csv(later, directory=root)
    assert len(load_sessions(root).sessions) == 3


def test_transcribing_the_same_session_twice_is_refused(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    source = _write_csv(tmp_path / "export.csv", [_row()])
    transcribe_csv(source, directory=root)
    with pytest.raises(TrialError, match="duplicate"):
        transcribe_csv(source, directory=root)
    assert len(load_sessions(root).sessions) == 1


def test_a_bad_row_anywhere_writes_nothing(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    source = _write_csv(tmp_path / "export.csv", [_row(), _row(confidence="9")])
    with pytest.raises(TrialError, match="confidence"):
        transcribe_csv(source, directory=root)
    assert not (root / SESSIONS_SUBDIR).exists()


def test_an_empty_export_is_refused_not_a_silent_no_op(tmp_path: Path) -> None:
    root = _trial_dir(tmp_path)
    source = _write_csv(tmp_path / "export.csv", [])
    (tmp_path / "export.csv").write_text("participant,task\n", encoding="utf-8")
    with pytest.raises(TrialError, match="no rows"):
        transcribe_csv(source, directory=root)


def test_transcription_refuses_a_destination_inside_docs(tmp_path: Path) -> None:
    from analysis.trial.sanitize import PrivacyLeak

    source = _write_csv(tmp_path / "export.csv", [_row()])
    with pytest.raises(PrivacyLeak):
        transcribe_csv(source, directory=Path(__file__).resolve().parents[1] / "docs" / "trial")
