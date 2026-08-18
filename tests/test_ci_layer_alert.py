"""Tests for the CI layer alerter (scripts/ci_layer_alert.py).

The gh CLI is stubbed out — these tests verify the decision logic:
which runs raise an alert, which comment on the existing one, and
which close it. Network/subprocess behaviour is not under test.
"""

from __future__ import annotations

import json

import pytest

from scripts import ci_layer_alert as alerter


@pytest.fixture
def gh_calls(monkeypatch):
    """Capture gh invocations; `state['open_issue']` controls issue lookup."""
    calls: list[tuple[str, ...]] = []
    state = {"open_issue": None}

    def fake_gh(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("issue", "list"):
            n = state["open_issue"]
            return json.dumps([{"number": n}] if n else [])
        return ""

    monkeypatch.setattr(alerter, "_gh", fake_gh)
    return calls, state


def _status_file(tmp_path, payload):
    path = tmp_path / "pipeline_status.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _commands(calls):
    return [c[:2] for c in calls]


def test_missing_status_file_raises_crash_alert(tmp_path, gh_calls):
    calls, _ = gh_calls

    assert alerter.main(["prog", str(tmp_path / "absent.json")]) == 0

    assert ("issue", "create") in _commands(calls)
    create = next(c for c in calls if c[:2] == ("issue", "create"))
    body = create[create.index("--body") + 1]
    assert "crashed" in body


def test_hard_failures_open_new_issue_with_layer_names(tmp_path, gh_calls):
    calls, _ = gh_calls
    path = _status_file(tmp_path, {
        "succeeded": ["prices"],
        "hard_failures": ["psd", "agrural"],
        "critical_failures": [],
    })

    assert alerter.main(["prog", str(path)]) == 0

    create = next(c for c in calls if c[:2] == ("issue", "create"))
    body = create[create.index("--body") + 1]
    assert "`psd`" in body and "`agrural`" in body
    # label creation must precede issue creation so --label can't 404
    assert _commands(calls).index(("label", "create")) < _commands(calls).index(("issue", "create"))


def test_hard_failures_comment_on_existing_open_issue(tmp_path, gh_calls):
    calls, state = gh_calls
    state["open_issue"] = 42
    path = _status_file(tmp_path, {"hard_failures": ["psd"]})

    alerter.main(["prog", str(path)])

    assert ("issue", "comment") in _commands(calls)
    assert ("issue", "create") not in _commands(calls)
    comment = next(c for c in calls if c[:2] == ("issue", "comment"))
    assert comment[2] == "42"


def test_critical_failures_called_out_in_body(tmp_path, gh_calls):
    calls, _ = gh_calls
    path = _status_file(tmp_path, {
        "hard_failures": ["prices"],
        "critical_failures": ["prices"],
    })

    alerter.main(["prog", str(path)])

    create = next(c for c in calls if c[:2] == ("issue", "create"))
    body = create[create.index("--body") + 1]
    assert "Critical layers failed" in body


def test_alert_body_distinguishes_failure_classes():
    body = alerter.build_alert_body({
        "hard_failures": ["psd", "prices", "weather"],
        "critical_failures": [],
        "classifications": {
            "upstream_failure": ["psd"],
            "no_publication": ["crop_progress"],
            "stale_last_known_good": ["prices"],
            "incomplete_key_coverage": ["weather"],
        },
    })

    assert "Upstream or ingest failure" in body
    assert "Legitimate no-publication" in body
    assert "Stale last-known-good" in body
    assert "Incomplete key coverage" in body


def test_green_run_closes_open_alert(tmp_path, gh_calls):
    calls, state = gh_calls
    state["open_issue"] = 7
    path = _status_file(tmp_path, {"succeeded": ["prices", "psd"], "hard_failures": []})

    alerter.main(["prog", str(path)])

    close = next(c for c in calls if c[:2] == ("issue", "close"))
    assert close[2] == "7"


def test_green_run_without_open_alert_is_a_noop(tmp_path, gh_calls):
    calls, _ = gh_calls
    path = _status_file(tmp_path, {"succeeded": ["prices"], "hard_failures": []})

    alerter.main(["prog", str(path)])

    assert _commands(calls) == [("issue", "list")]
