"""Tests for the briefing-blocks relay (scripts/ci_briefing_blocks.py, B45 #562).

The runner DB is the conftest ``patched_db`` holding a fixture snapshot; gh is
stubbed. Nothing here touches data/history/ or the network.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts import ci_briefing_blocks as relay

STARTED = "2026-10-19T20:00:00Z"


@pytest.fixture
def gh_calls(monkeypatch: pytest.MonkeyPatch):
    """Capture gh invocations; ``state`` controls the open issue and its thread."""
    calls: list[tuple[str, ...]] = []
    state: dict = {"open_issue": None, "thread": []}

    def fake_gh(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("issue", "list"):
            n = state["open_issue"]
            return json.dumps([{"number": n}] if n else [])
        if args[:2] == ("issue", "view"):
            body, *comments = state["thread"] or [""]
            return json.dumps({"body": body, "comments": [{"body": c} for c in comments]})
        return ""

    monkeypatch.setattr(relay, "_gh", fake_gh)
    return calls, state


def _briefing(db: Path, briefing_date: str, generated_at: str, snapshot: dict | str | None) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT OR REPLACE INTO briefings (briefing_date, text, signals_json, snapshot_json, generated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (briefing_date, "text", "[]",
         snapshot if snapshot is None or isinstance(snapshot, str) else json.dumps(snapshot),
         generated_at),
    )
    conn.commit()
    conn.close()


def _v4(failed: dict[str, str]) -> dict:
    return {"schema_version": 4, "date": "2026-10-19", "prices": {}, "failed_blocks": failed}


def _commands(calls: list[tuple[str, ...]]) -> list[tuple[str, ...]]:
    return [c[:2] for c in calls]


def _body(call: tuple[str, ...]) -> str:
    flag = "--comment" if "--comment" in call else "--body"
    return call[call.index(flag) + 1]


def _run(db: Path) -> int:
    return relay.main(["--started-at", STARTED, "--db", str(db)])


FAILED = {"seasonal.Soybeans": "ValueError: two rows for band 2026-11", "cot": "KeyError: 'net'"}


def test_each_failed_block_is_one_warning_annotation(patched_db, gh_calls, capsys):
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4(FAILED))

    assert _run(patched_db) == 0

    warnings = [line for line in capsys.readouterr().out.splitlines() if line.startswith("::warning")]
    assert len(warnings) == 2
    assert any("seasonal.Soybeans" in w and "two rows for band 2026-11" in w for w in warnings)
    assert any("cot" in w and "KeyError" in w for w in warnings)


def test_annotation_escapes_workflow_command_characters():
    line = relay.annotation("a.b", "ValueError: 100% wrong\nsecond line")
    assert "\n" not in line
    assert "100%25 wrong%0Asecond line" in line


def test_first_failing_run_creates_the_label_then_opens_the_issue(patched_db, gh_calls):
    calls, _ = gh_calls
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4(FAILED))

    _run(patched_db)

    commands = _commands(calls)
    assert commands.index(("label", "create")) < commands.index(("issue", "create"))
    create = next(c for c in calls if c[:2] == ("issue", "create"))
    assert relay.LABEL in create
    body = _body(create)
    assert "`seasonal.Soybeans`" in body and "two rows for band 2026-11" in body
    assert "2026-10-19" in body


def test_same_failures_as_last_reported_post_nothing(patched_db, gh_calls):
    calls, state = gh_calls
    state["open_issue"] = 9
    state["thread"] = [relay.build_body("2026-10-18", FAILED, before=set())]
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4(FAILED))

    _run(patched_db)

    assert ("issue", "comment") not in _commands(calls)
    assert ("issue", "create") not in _commands(calls)
    assert ("issue", "close") not in _commands(calls)


def test_a_changed_failure_set_comments_naming_what_changed(patched_db, gh_calls):
    calls, state = gh_calls
    state["open_issue"] = 9
    state["thread"] = [
        relay.build_body("2026-10-17", {"cot": "KeyError: 'net'"}, before=set()),
        relay.build_body("2026-10-18", {"cot": "KeyError: 'net'", "eia": "TypeError: x"}, before={"cot"}),
    ]
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4(FAILED))

    _run(patched_db)

    comment = next(c for c in calls if c[:2] == ("issue", "comment"))
    assert comment[2] == "9"
    body = _body(comment)
    assert "Newly failing" in body and "`seasonal.Soybeans`" in body
    assert "Recovered" in body and "`eia`" in body


def test_a_clean_run_closes_the_open_issue(patched_db, gh_calls):
    calls, state = gh_calls
    state["open_issue"] = 9
    state["thread"] = [relay.build_body("2026-10-18", FAILED, before=set())]
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4({}))

    _run(patched_db)

    close = next(c for c in calls if c[:2] == ("issue", "close"))
    assert close[2] == "9"
    assert "failed_blocks == {}" in _body(close)


def test_a_clean_run_without_an_open_issue_writes_nothing(patched_db, gh_calls, capsys):
    calls, _ = gh_calls
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4({}))

    _run(patched_db)

    assert _commands(calls) == [("issue", "list")]
    assert "::warning" not in capsys.readouterr().out


def test_a_snapshot_without_the_key_is_never_recorded_not_clean(patched_db, gh_calls, capsys):
    """A v1–v3 row means failures were never recorded (invariant 2): no close."""
    calls, state = gh_calls
    state["open_issue"] = 9
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", {"schema_version": 3, "prices": {}})

    assert _run(patched_db) == 0

    assert calls == []
    out = capsys.readouterr().out
    assert "::warning" in out and "never recorded" in out


def test_a_run_that_archived_no_briefing_leaves_the_issue_alone(patched_db, gh_calls, capsys):
    """The DB is seeded from the committed CSV: an older row is a previous run's."""
    calls, state = gh_calls
    state["open_issue"] = 9
    _briefing(patched_db, "2026-10-19", "2026-10-19 15:00:52", _v4({}))

    assert _run(patched_db) == 0

    assert calls == []
    assert "No briefing archived" in capsys.readouterr().out


def test_the_newest_row_this_run_wrote_is_the_one_read(patched_db, gh_calls):
    calls, _ = gh_calls
    _briefing(patched_db, "2026-10-18", "2026-10-18 21:00:00", _v4(FAILED))
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4({}))

    _run(patched_db)

    assert _commands(calls) == [("issue", "list")]


@pytest.mark.parametrize("snapshot", [
    "not json",
    json.dumps(["a list"]),
    json.dumps({"schema_version": 4, "failed_blocks": ["cot"]}),
    json.dumps({"schema_version": 4, "failed_blocks": {"cot": None}}),
])
def test_an_unreadable_snapshot_hard_fails(patched_db, gh_calls, snapshot):
    calls, _ = gh_calls
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", snapshot)

    with pytest.raises(relay.BriefingBlocksError):
        _run(patched_db)
    assert calls == []


def test_a_missing_start_stamp_hard_fails(patched_db, gh_calls, monkeypatch):
    monkeypatch.delenv("PIPELINE_STARTED_AT", raising=False)
    with pytest.raises(relay.BriefingBlocksError):
        relay.main(["--db", str(patched_db)])


def test_an_open_issue_without_a_readable_marker_is_commented(patched_db, gh_calls):
    """No record of what was last said → say it rather than guess it was the same."""
    calls, state = gh_calls
    state["open_issue"] = 9
    state["thread"] = ["hand-edited body"]
    _briefing(patched_db, "2026-10-19", "2026-10-19 20:30:00", _v4(FAILED))

    _run(patched_db)

    assert ("issue", "comment") in _commands(calls)


def test_the_body_marker_round_trips():
    body = relay.build_body("2026-10-19", FAILED, before=set())
    assert relay.last_reported(json.dumps({"body": body, "comments": []})) == set(FAILED)
