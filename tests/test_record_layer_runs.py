"""The deploy workflow's record step and the dead-source relay (B34 #520).

HISTORY_DIR points at tmp_path; the runner DB is the conftest ``patched_db``;
gh is stubbed. Nothing here touches data/history/ or the network.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pytest

import config
from pipeline import layer_runs
from pipeline.layer_runs import LayerRun, LayerRunsError
from scripts import ci_dead_source as relay
from scripts import record_layer_runs as recorder

RUN_ENV = {
    "GITHUB_RUN_ID": "777", "GITHUB_RUN_ATTEMPT": "1",
    "GITHUB_WORKFLOW": "Update Dashboard", "GITHUB_EVENT_NAME": "schedule",
}


@pytest.fixture
def history_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(directory))
    for key, value in RUN_ENV.items():
        monkeypatch.setenv(key, value)
    return directory


def _freshness(db: Path, layer: str, status: str, attempt: str, observed: str | None = None) -> None:
    conn = sqlite3.connect(str(db))
    conn.execute(
        "INSERT OR REPLACE INTO data_freshness (layer_name, last_attempt, status, observed_at) "
        "VALUES (?, ?, ?, ?)",
        (layer, attempt, status, observed),
    )
    conn.commit()
    conn.close()


def _args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--started-at", "2026-10-19T20:00:00Z",
        "--status", str(tmp_path / "pipeline_status.json"),
        "--evidence", str(tmp_path / "run_evidence.json"),
        "--receipt", str(tmp_path / "alert_receipt.json"),
        *extra,
    ]


def _evidence_block(
    roster: tuple[str, ...] | list[str] = config.PRODUCTION_LAYER_KEYS,
    configured: dict[str, bool] | None = None,
    started_at: str = "2026-10-19T20:00:30Z",
) -> dict:
    """What main.py says about the run it is (B38 #535): roster + per-key booleans."""
    configured = configured or {}
    return {
        "started_at": started_at,
        "roster": list(roster),
        "api_keys": {
            name: {"configured": configured.get(name, True), "skips": list(spec.skips)}
            for name, spec in config.API_KEY_CATALOG.items()
        },
    }


@pytest.fixture
def run_evidence(tmp_path: Path) -> Path:
    """The start-of-run evidence a crashed pipeline still leaves behind."""
    path = tmp_path / "run_evidence.json"
    path.write_text(json.dumps(_evidence_block()), encoding="utf-8")
    return path


def _summary(tmp_path: Path, **evidence: object) -> None:
    block = _evidence_block(**evidence)  # type: ignore[arg-type]
    (tmp_path / "pipeline_status.json").write_text(
        json.dumps({"layers_requested": block["roster"], "run_evidence": block}), encoding="utf-8",
    )


# ── record step ────────────────────────────────────────────────────────────


def test_record_writes_one_row_per_layer_from_this_runs_grades(
    history_dir: Path, patched_db: Path, tmp_path: Path,
) -> None:
    _freshness(patched_db, "safex", "failed", "2026-10-19 20:10:00")
    _freshness(patched_db, "dce", "success", "2026-10-19 20:11:00", "2026-10-19")
    _freshness(patched_db, "cepea", "success", "2026-10-18 20:11:00", "2026-10-18")  # yesterday's
    _summary(tmp_path)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"edition": {"generated_at": "2026-10-19T20:40:00+00:00"}}), encoding="utf-8")

    assert recorder.main(_args(tmp_path, "--manifest", str(manifest), "--deployment-outcome", "success")) == 0

    rows = {r.layer_name: r for r in layer_runs.read_runs()}
    assert len(rows) == len(config.PRODUCTION_LAYER_KEYS)
    assert rows["safex"].status == "failed"
    assert rows["dce"].status == "success"
    assert rows["dce"].observed_at == "2026-10-19"
    assert rows["dce"].rendered_at == "2026-10-19T20:40:00+00:00"
    assert rows["cepea"].status == "not_run"
    assert rows["dce"].run_id == "777"
    assert rows["dce"].workflow == "Update Dashboard/schedule"
    assert rows["dce"].session_date == date(2026, 10, 19)


def test_record_leaves_rendered_at_blank_when_nothing_was_promoted(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    assert recorder.main(_args(tmp_path, "--deployment-outcome", "skipped")) == 0
    assert {r.rendered_at for r in layer_runs.read_runs()} == {None}


def test_record_refuses_a_promoted_edition_without_its_manifest(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    with pytest.raises(LayerRunsError, match="manifest"):
        recorder.main(_args(tmp_path, "--manifest", str(tmp_path / "absent.json"),
                            "--deployment-outcome", "success"))
    assert layer_runs.read_runs() == []


def test_record_twice_for_one_run_is_refused(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    recorder.main(_args(tmp_path))
    with pytest.raises(LayerRunsError, match="already recorded"):
        recorder.main(_args(tmp_path))


def test_record_hard_fails_without_a_start_stamp(
    history_dir: Path, patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PIPELINE_STARTED_AT", raising=False)
    with pytest.raises(LayerRunsError, match="started-at"):
        recorder.main(["--status", str(tmp_path / "none.json")])


def test_record_carries_this_runs_alert_receipt(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    (tmp_path / "alert_receipt.json").write_text(
        json.dumps({"run_id": "777", "layers": ["safex"]}), encoding="utf-8",
    )
    recorder.main(_args(tmp_path))
    assert layer_runs.read_alerts() == {("777", "safex")}


def test_record_refuses_another_runs_receipt(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    (tmp_path / "alert_receipt.json").write_text(
        json.dumps({"run_id": "1", "layers": ["safex"]}), encoding="utf-8",
    )
    with pytest.raises(LayerRunsError, match="not this run"):
        recorder.main(_args(tmp_path))


# ── the ingesting run's own evidence (B38 #535) ────────────────────────────
#
# The record step runs from a checkout the snapshot step may have rebased onto
# a moved main, in a step with no API keys in its env. Roster and key
# configuration therefore come from what main.py wrote, never from either.


def test_record_files_under_the_runs_roster_not_the_rebased_checkouts(
    history_dir: Path, patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    ran_under = ("safex", "dce", "renamed_mid_run")  # R: what main.py ran
    # R′: a PR merged mid-run renamed a layer and added another.
    monkeypatch.setattr(config, "PRODUCTION_LAYER_KEYS", ("safex", "dce", "renamed", "added_mid_run"))
    _freshness(patched_db, "renamed_mid_run", "success", "2026-10-19 20:10:00", "2026-10-19")
    _summary(tmp_path, roster=ran_under)

    assert recorder.main(_args(tmp_path)) == 0

    rows = layer_runs.read_runs()
    assert [r.layer_name for r in rows] == list(ran_under)
    assert {r.layer_name: r.status for r in rows}["renamed_mid_run"] == "success"


def test_record_reads_key_configuration_from_the_run_not_its_own_env(
    history_dir: Path, patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in config.API_KEY_CATALOG:
        monkeypatch.delenv(name, raising=False)  # the record step's env: no keys at all
    monkeypatch.setenv("MARS_API_KEY", "set-here-but-not-in-the-run")
    usda = config.API_KEY_CATALOG["USDA_API_KEY"]
    _summary(tmp_path, configured={"USDA_API_KEY": True, "FRED_API_KEY": False, "MARS_API_KEY": False})

    recorder.main(_args(tmp_path))

    status = {r.layer_name: r.status for r in layer_runs.read_runs()}
    assert usda.skips and all(status[layer] == "not_run" for layer in usda.skips)
    assert status["fred"] == "skipped_unconfigured"
    assert status["us_processor_cash"] == "skipped_unconfigured"


def test_a_key_retired_mid_run_still_marks_the_layers_it_skipped(
    history_dir: Path, patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The run had a key the checkout's catalog no longer lists, unset. Its
    # layer was skipped for it in that run, whatever this checkout says now.
    monkeypatch.setattr(config, "API_KEY_CATALOG", {})
    monkeypatch.setattr(layer_runs, "API_KEY_CATALOG", {})
    block = _evidence_block(roster=("safex", "old_keyed_layer"))
    block["api_keys"]["RETIRED_API_KEY"] = {"configured": False, "skips": ["old_keyed_layer"]}
    (tmp_path / "pipeline_status.json").write_text(
        json.dumps({"layers_requested": ["safex", "old_keyed_layer"], "run_evidence": block}),
        encoding="utf-8",
    )
    recorder.main(_args(tmp_path))
    assert {r.layer_name: r.status for r in layer_runs.read_runs()}["old_keyed_layer"] == "skipped_unconfigured"


def test_a_crashed_run_files_under_its_start_evidence(
    history_dir: Path, patched_db: Path, tmp_path: Path,
) -> None:
    # No summary: main.py died mid-run. Its start-of-run evidence still names
    # the roster, and nothing says which layers were asked, so all not_run.
    (tmp_path / "run_evidence.json").write_text(
        json.dumps(_evidence_block(roster=("safex", "dce"), configured={"FRED_API_KEY": False})),
        encoding="utf-8",
    )
    assert recorder.main(_args(tmp_path)) == 0
    assert [(r.layer_name, r.status) for r in layer_runs.read_runs()] == [
        ("safex", "not_run"), ("dce", "not_run"),
    ]


def test_record_refuses_a_summary_without_run_evidence(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path,
) -> None:
    # The start file is there, but the summary is the run's complete account;
    # one without evidence was written by code that never recorded it.
    (tmp_path / "pipeline_status.json").write_text(
        json.dumps({"layers_requested": list(config.PRODUCTION_LAYER_KEYS)}), encoding="utf-8",
    )
    with pytest.raises(LayerRunsError, match="run_evidence"):
        recorder.main(_args(tmp_path))
    assert layer_runs.read_runs() == []


def test_record_refuses_when_the_run_left_no_evidence_at_all(
    history_dir: Path, patched_db: Path, tmp_path: Path,
) -> None:
    with pytest.raises(LayerRunsError, match="no run evidence"):
        recorder.main(_args(tmp_path))
    assert layer_runs.read_runs() == []


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda b: b.pop("roster"), "roster"),
        (lambda b: b.update(roster=[]), "roster"),
        (lambda b: b.update(roster=["safex", "safex"]), "roster"),
        (lambda b: b.pop("api_keys"), "api_keys"),
        (lambda b: b.update(api_keys={}), "api_keys"),
        (lambda b: b["api_keys"].update(USDA_API_KEY=True), "USDA_API_KEY"),
        (lambda b: b["api_keys"]["FRED_API_KEY"].update(configured="yes"), "boolean"),
        (lambda b: b.update(started_at="2026-10-19T19:59:59Z"), "predates"),
    ],
)
def test_record_refuses_incomplete_or_stale_evidence(
    history_dir: Path, patched_db: Path, tmp_path: Path, mutate, match: str,
) -> None:
    block = _evidence_block()
    mutate(block)
    (tmp_path / "pipeline_status.json").write_text(
        json.dumps({"layers_requested": [], "run_evidence": block}), encoding="utf-8",
    )
    with pytest.raises(LayerRunsError, match=match):
        recorder.main(_args(tmp_path))
    assert layer_runs.read_runs() == []


# ── dead-source relay ──────────────────────────────────────────────────────


@pytest.fixture
def gh(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple[str, ...]] = []
    state: dict = {"open_issue": None}

    def fake_gh(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("issue", "list"):
            n = state["open_issue"]
            return json.dumps([{"number": n}] if n else [])
        return ""

    monkeypatch.setattr(relay, "_gh", fake_gh)
    return calls, state


def _session(i: int) -> date:
    day, n = date(2026, 10, 5), 0
    while True:
        if day.weekday() < 5:
            if n == i:
                return day
            n += 1
        day += timedelta(days=1)


def _seed(statuses: list[str], layer: str = "safex", others: dict[str, list[str]] | None = None) -> None:
    """One run per session, every layer in each run (as the record step writes
    them); the last session's run is this run (``777``)."""
    by_layer = {name: ["success"] * len(statuses) for name in config.PRODUCTION_LAYER_KEYS}
    by_layer.update({layer: statuses, **(others or {})})
    rows = []
    for i in range(len(statuses)):
        run_id = "777" if i == len(statuses) - 1 else f"r{i}"
        for name, sts in by_layer.items():
            rows.append(LayerRun(
                run_id=run_id, workflow="w", run_started_at=f"{_session(i).isoformat()}T20:00:00Z",
                session_date=_session(i), layer_name=name, status=sts[i],
                observed_at=None, rendered_at=None,
            ))
    layer_runs.append_runs(config.PRODUCTION_LAYER_KEYS, rows)


def _cmds(calls):
    return [c[:2] for c in calls]


def _body(calls, cmd):
    call = next(c for c in calls if c[:2] == cmd)
    flag = "--body" if "--body" in call else "--comment"
    return call[call.index(flag) + 1]


def test_relay_opens_the_issue_when_a_layer_crosses_three_sessions(history_dir: Path, gh) -> None:
    calls, _ = gh
    _seed(["success", "failed", "failed", "failed"])
    assert relay.main([]) == 0
    assert ("issue", "create") in _cmds(calls)
    body = _body(calls, ("issue", "create"))
    assert "`safex`" in body and "3 consecutive sessions" in body
    assert "decision-impairing" not in body


def test_basket_layer_is_tagged_decision_impairing(history_dir: Path, gh) -> None:
    calls, _ = gh
    _seed(["failed", "failed", "failed"], layer="gulf_bids")
    relay.main([])
    assert "decision-impairing" in _body(calls, ("issue", "create"))


def test_relay_is_silent_while_the_dead_set_is_unchanged(history_dir: Path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 41
    _seed(["failed", "failed", "failed", "failed"])  # already dead before this run
    relay.main([])
    assert ("issue", "comment") not in _cmds(calls)
    assert ("issue", "create") not in _cmds(calls)


def test_relay_comments_on_a_new_dead_layer_when_the_issue_is_open(history_dir: Path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 41
    _seed(["success", "failed", "failed", "failed"], others={"cec": ["failed"] * 4})
    relay.main([])
    body = _body(calls, ("issue", "comment"))
    assert "Now dead" in body and "`safex`" in body
    assert "Still dead" in body and "`cec`" in body


def test_relay_closes_the_issue_with_the_recovered_streak(history_dir: Path, gh) -> None:
    calls, state = gh
    state["open_issue"] = 41
    _seed(["failed", "failed", "failed", "failed", "success"])
    relay.main([])
    assert ("issue", "close") in _cmds(calls)
    body = _body(calls, ("issue", "close"))
    assert "`safex`" in body and "4 unavailable session(s)" in body


def test_relay_does_nothing_when_nothing_is_or_was_dead(history_dir: Path, gh) -> None:
    calls, _ = gh
    _seed(["success", "failed", "success"])
    relay.main([])
    assert _cmds(calls) == [("issue", "list")]


# ── manifest block ─────────────────────────────────────────────────────────


def test_manifest_reliability_closes_on_the_session_before_the_edition(history_dir: Path) -> None:
    from datetime import datetime, timezone

    from app.manifest import _reliability

    assert _reliability(datetime(2026, 10, 5, 21, tzinfo=timezone.utc))["state"] == "no_record"

    _seed(["success", "failed", "success"])  # sessions Mon 5 → Wed 7
    # Built Wednesday evening: Wednesday is still being recorded, so the
    # window ends Tuesday and Wednesday's success is not yet in it.
    block = _reliability(datetime(2026, 10, 7, 21, tzinfo=timezone.utc))
    assert block["state"] == "ok"
    assert block["window"]["last_session"] == "2026-10-06"
    safex = next(e for e in block["layers"] if e["layer"] == "safex")
    assert safex["availability_pct"] == 50.0
    assert safex["open_at_end"] is True


def test_manifest_reliability_withholds_a_corrupt_record_with_a_reason(history_dir: Path) -> None:
    from datetime import datetime, timezone

    from app.manifest import _reliability

    history_dir.mkdir()
    (history_dir / "layer_runs.csv").write_text("garbage\n", encoding="utf-8")
    block = _reliability(datetime(2026, 10, 7, 21, tzinfo=timezone.utc))
    assert block["state"] == "unavailable"
    assert "header" in block["reason"]


def test_manifest_window_follows_the_pipeline_start_stamp(
    history_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import datetime, timezone

    from app.manifest import _reliability

    _seed(["success", "failed", "success"])  # Mon 5 → Wed 7
    # Pipeline started Wednesday before the 14:30 CT cutoff (session Tue 6),
    # site built after it: the record files the run under Tuesday, so the
    # window must close on Monday, not on Tuesday's unrecorded run.
    monkeypatch.setenv("PIPELINE_STARTED_AT", "2026-10-07T18:00:00Z")
    block = _reliability(datetime(2026, 10, 7, 21, tzinfo=timezone.utc))
    assert block["window"]["last_session"] == "2026-10-05"


def test_record_and_relay_write_to_the_given_history_dir(
    history_dir: Path, patched_db: Path, tmp_path: Path, run_evidence: Path, gh,
) -> None:
    target = tmp_path / "worktree" / "data" / "history"
    assert recorder.main(_args(tmp_path, "--history-dir", str(target))) == 0
    assert (target / "layer_runs.csv").exists()
    assert relay.main(["--history-dir", str(target)]) == 0
