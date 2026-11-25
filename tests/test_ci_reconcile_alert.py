"""Tests for the reconciliation alarm relay (scripts/ci_reconcile_alert.py).

Owner decision on #239 (2026-11-25): a v1-vs-trusted divergence opens or
comments on one rolling ``ci-reconcile-divergence`` issue, only on a state
change, and never turns the Update Dashboard run red. The gh CLI is stubbed;
these tests pin the decision logic from the reports and exit codes.
"""

from __future__ import annotations

import json

import pytest

from scripts import ci_reconcile_alert as relay


@pytest.fixture
def gh(monkeypatch):
    """Capture gh calls; ``state`` controls the open issue and its thread."""
    calls: list[tuple[str, ...]] = []
    state: dict = {"open_issue": None, "body": "", "comments": []}

    def fake_gh(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("issue", "list"):
            number = state["open_issue"]
            return json.dumps([{"number": number}] if number else [])
        if args[:2] == ("issue", "view"):
            return json.dumps({"body": state["body"], "comments": [{"body": c} for c in state["comments"]]})
        return ""

    monkeypatch.setattr(relay, "_gh", fake_gh)
    return calls, state


def _write(tmp_path, name: str, payload: dict) -> None:
    (tmp_path / name).write_text(json.dumps(payload), encoding="utf-8")


def _reconciliation(reconciled: bool) -> dict:
    return {
        "reconciled": reconciled,
        "matched_rows": 3 if reconciled else 2,
        "missing_in_trusted": [] if reconciled else [{"symbol": "ZMZ26"}],
        "missing_in_legacy": [],
        "field_differences": [],
    }


def reports(tmp_path, *, cbot_meal_ok=True, fx_diverged=(), magyp_status="reconciled"):
    _write(tmp_path, "magyp_fob.json", {
        "status": magyp_status, "resolved_day": "2026-11-24",
        **({"reconciliation": _reconciliation(magyp_status != "diverged")} if magyp_status != "no_circular" else {}),
    })
    _write(tmp_path, "cbot_benchmarks.json", {
        "status": "reconciled" if cbot_meal_ok else "diverged",
        "rejected_by_rule": {} if cbot_meal_ok else {"ohlc.complete": 1},
        "commodities": [
            {"commodity": "Soybeans", "reconciliation": _reconciliation(True)},
            {"commodity": "Soybean Meal", "reconciliation": _reconciliation(cbot_meal_ok)},
        ],
    })
    _write(tmp_path, "fx.json", {
        "status": "diverged" if fx_diverged else "reconciled",
        "session": "2026-11-24",
        "diverged_pairs": list(fx_diverged),
        "not_compared": [{"pair": "CNY/USD", "dataset_key": "fx-cny-usd",
                          "reason": "not compared: v1 source differs"}],
        "pairs": [],
    })


def run(tmp_path, *, magyp="0", cbot="0", fx="0") -> int:
    return relay.main([
        "--report-dir", str(tmp_path),
        "--exit", f"magyp={magyp}", "--exit", f"cbot={cbot}", "--exit", f"fx={fx}",
    ])


def _commands(calls):
    return [call[:2] for call in calls]


def _body(calls, command):
    call = next(c for c in calls if c[:2] == command)
    flag = "--body" if "--body" in call else "--comment"
    return call[call.index(flag) + 1]


def test_a_divergence_opens_the_rolling_issue_naming_the_leg(tmp_path, gh) -> None:
    calls, _ = gh
    reports(tmp_path, cbot_meal_ok=False)

    assert run(tmp_path, cbot="1") == 0

    commands = _commands(calls)
    assert commands.index(("label", "create")) < commands.index(("issue", "create"))
    body = _body(calls, ("issue", "create"))
    assert "Soybean Meal" in body
    assert "ohlc.complete" in body
    assert "cbot:Soybean Meal" in relay.parse_state(body)


def test_an_unchanged_divergence_does_not_comment_again(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"cbot:Soybean Meal"})
    reports(tmp_path, cbot_meal_ok=False)

    run(tmp_path, cbot="1")

    assert ("issue", "comment") not in _commands(calls)
    assert ("issue", "create") not in _commands(calls)


def test_a_new_leg_diverging_comments_once_with_the_newest_state(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"cbot:Soybean Meal"})
    reports(tmp_path, cbot_meal_ok=False, fx_diverged=("ZAR/USD",))

    run(tmp_path, cbot="1", fx="1")

    body = _body(calls, ("issue", "comment"))
    assert "**Now diverged:**" in body and "`fx:ZAR/USD`" in body
    assert relay.parse_state(body) == {"cbot:Soybean Meal", "fx:ZAR/USD"}


def test_the_newest_marker_in_the_thread_is_the_state(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"cbot:Soybean Meal"})
    state["comments"] = [relay.marker({"cbot:Soybean Meal", "fx:ZAR/USD"})]
    reports(tmp_path, cbot_meal_ok=False, fx_diverged=("ZAR/USD",))

    run(tmp_path, cbot="1", fx="1")

    assert ("issue", "comment") not in _commands(calls)


def test_recovery_closes_the_issue(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"cbot:Soybean Meal"})
    reports(tmp_path)

    run(tmp_path)

    body = _body(calls, ("issue", "close"))
    assert "**Recovered:**" in body and "`cbot:Soybean Meal`" in body
    assert relay.parse_state(body) == set()


def test_all_reconciled_and_no_issue_does_nothing(tmp_path, gh) -> None:
    calls, _ = gh
    reports(tmp_path)

    run(tmp_path)

    assert _commands(calls) == [("issue", "list")]


def test_cny_is_listed_as_not_compared_and_never_opens_the_issue(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"fx:ZAR/USD"})
    reports(tmp_path)  # reconcile_fx graded CNY out; status reconciled

    run(tmp_path)

    body = _body(calls, ("issue", "close"))
    assert "CNY/USD — not compared: v1 source differs" in body


def test_an_unavailable_upstream_neither_opens_nor_clears_the_alarm(tmp_path, gh) -> None:
    calls, state = gh
    reports(tmp_path)
    (tmp_path / "magyp_fob.json").unlink()

    run(tmp_path, magyp="2")
    assert ("issue", "create") not in _commands(calls)

    # With a MAGyP divergence open, an unreadable MAGyP day is not a recovery.
    calls.clear()
    state["open_issue"] = 7
    state["body"] = relay.marker({"magyp:FOB"})
    run(tmp_path, magyp="2")

    assert ("issue", "close") not in _commands(calls)
    assert ("issue", "comment") not in _commands(calls)


def test_a_weekend_with_nothing_to_compare_carries_the_open_divergence(tmp_path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 7
    state["body"] = relay.marker({"magyp:FOB"})
    reports(tmp_path, magyp_status="no_circular")

    run(tmp_path)

    assert ("issue", "close") not in _commands(calls)


def test_a_crashed_reconciler_alarms_rather_than_reading_as_a_divergence_or_a_pass(tmp_path, gh) -> None:
    calls, _ = gh
    reports(tmp_path)
    (tmp_path / "fx.json").unlink()  # a traceback exits 1 before the report is written

    run(tmp_path, fx="1")

    body = _body(calls, ("issue", "create"))
    assert "`fx:errored`" in body
    assert "crashed" in body


def test_a_step_that_did_not_run_is_said_so(tmp_path, gh) -> None:
    calls, _ = gh
    reports(tmp_path)

    run(tmp_path, cbot="")

    assert _commands(calls) == [("issue", "list")]
    assert relay.classify("cbot", "", tmp_path).state == "not_run"


def test_an_unknown_pilot_is_refused(tmp_path, gh) -> None:
    with pytest.raises(SystemExit):
        relay.main(["--report-dir", str(tmp_path), "--exit", "matif=0"])


def test_the_relay_reads_the_report_reconcile_fx_actually_writes(tmp_path, gh, monkeypatch) -> None:
    """Shape agreement across the seam: a real FX report, not a hand-built one."""
    from datetime import date

    from scripts.reconcile_fx import reconcile_once, write_report
    from tests.test_reconcile_fx import AFTER_FX_CLOSE, _shift_close, sessions

    calls, _ = gh
    reports(tmp_path)
    _shift_close(monkeypatch, "ZAR/USD")
    fx_run = reconcile_once(download=sessions([date(2026, 8, 12)]), now=AFTER_FX_CLOSE)
    write_report(fx_run, tmp_path / "fx.json")

    run(tmp_path, fx=str(fx_run.exit_code))

    body = _body(calls, ("issue", "create"))
    assert relay.parse_state(body) == {"fx:ZAR/USD"}
    assert "session 2026-08-12" in body and "1 field differences" in body
    assert "CNY/USD — not compared: v1 source differs" in body


def test_the_workflow_wires_the_alarm_without_gating_or_reddening_the_publish() -> None:
    """#239: alarm by issue, never a red run; reconcile still needs a green check (Q3)."""
    import yaml

    workflow = yaml.safe_load((relay.PROJECT_ROOT / ".github/workflows/deploy-dashboard.yml").read_text())
    jobs = workflow["jobs"]
    job = jobs["reconcile-trusted"]

    assert job["continue-on-error"] is True
    assert job["needs"] == "check"
    assert "reconcile-trusted" not in str(jobs["build-deploy"].get("needs"))

    steps = {step.get("id") or step.get("name"): step for step in job["steps"]}
    for step_id in ("reconcile_magyp", "reconcile_cbot", "reconcile_fx"):
        assert steps[step_id]["continue-on-error"] is True
        assert 'echo "code=$code" >> "$GITHUB_OUTPUT"' in steps[step_id]["run"]
    relay_step = steps["Relay reconciliation divergences"]
    assert relay_step["continue-on-error"] is True
    assert "scripts/ci_reconcile_alert.py" in relay_step["run"]
    for pilot, step_id in (("magyp", "reconcile_magyp"), ("cbot", "reconcile_cbot"), ("fx", "reconcile_fx")):
        assert f"{pilot}=${{{{ steps.{step_id}.outputs.code }}}}" in relay_step["run"]
    assert workflow["permissions"]["issues"] == "write"
