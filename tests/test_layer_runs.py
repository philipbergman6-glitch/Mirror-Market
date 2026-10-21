"""The source reliability record's store (B34 #520): data/history/layer_runs.csv.

Append-only, one row per (run, layer). Every test points
``pipeline.history.HISTORY_DIR`` at ``tmp_path`` — the history-write guard
refuses the real directory, and only the deploy workflow writes there.
"""

from __future__ import annotations

import csv
import shutil
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import config
from pipeline import layer_runs
from pipeline.layer_runs import LayerRun, LayerRunsError, RunEvidence

# The private set as it stood until 2026-10-21, when the owner published both
# (config.PRIVATE_LAYERS is empty now). The blanking mechanism outlives its
# last member, so these tests declare a run's private layers themselves.
_PRIVATE = ("sea_india", "sopa_crop")


def _evidence(
    roster: tuple[str, ...] = config.PRODUCTION_LAYER_KEYS,
    unconfigured: tuple[str, ...] = (),
    private: tuple[str, ...] = _PRIVATE,
) -> RunEvidence:
    """A run's evidence as ``parse_run_evidence`` hands it to the store."""
    return RunEvidence(
        started_at=datetime(2026, 10, 19, 20, 0, tzinfo=timezone.utc), roster=roster,
        unconfigured=frozenset(unconfigured), private=frozenset(private),
    )


@pytest.fixture
def history_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    directory = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(directory))
    return directory


def _run(run_id: str = "1", layer: str = "safex", status: str = "success", **kw: object) -> LayerRun:
    fields: dict = {
        "run_id": run_id,
        "workflow": "Update Dashboard/schedule",
        "run_started_at": "2026-10-19T20:05:00Z",
        "session_date": date(2026, 10, 19),
        "layer_name": layer,
        "status": status,
        "observed_at": "2026-10-19",
        "rendered_at": "2026-10-19T20:40:00+00:00",
    }
    fields.update(kw)
    return LayerRun(**fields)


# ── store ──────────────────────────────────────────────────────────────────


def test_absent_store_reads_as_no_rows(history_dir: Path) -> None:
    assert layer_runs.read_runs() == []


def test_append_then_read_round_trips_with_fixed_columns(history_dir: Path) -> None:
    rows = [_run(layer="safex"), _run(layer="dce", status="failed", observed_at=None)]
    layer_runs.append_runs(_evidence(), rows)

    with open(history_dir / "layer_runs.csv", newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    assert tuple(header) == layer_runs.COLUMNS
    assert layer_runs.read_runs() == rows


def test_append_adds_rows_and_never_rewrites_earlier_ones(history_dir: Path) -> None:
    layer_runs.append_runs(_evidence(), [_run("1")])
    before = (history_dir / "layer_runs.csv").read_bytes()
    layer_runs.append_runs(_evidence(), [_run("2")])
    after = (history_dir / "layer_runs.csv").read_bytes()

    assert after.startswith(before)
    assert [r.run_id for r in layer_runs.read_runs()] == ["1", "2"]


def test_append_refuses_to_rewrite_an_existing_run_layer_row(history_dir: Path) -> None:
    layer_runs.append_runs(_evidence(), [_run("1", status="failed")])
    with pytest.raises(LayerRunsError, match="already recorded"):
        layer_runs.append_runs(_evidence(), [_run("1", status="success")])
    assert [r.status for r in layer_runs.read_runs()] == ["failed"]


def test_append_refuses_a_duplicate_inside_one_batch(history_dir: Path) -> None:
    with pytest.raises(LayerRunsError, match="twice"):
        layer_runs.append_runs(_evidence(), [_run("1"), _run("1")])
    assert not (history_dir / "layer_runs.csv").exists()


@pytest.mark.parametrize(
    ("override", "match"),
    [
        ({"status": "ok"}, "status"),
        ({"layer_name": "not_a_layer"}, "layer"),
        ({"run_id": ""}, "run_id"),
        ({"layer_name": "sea_india"}, "private"),
    ],
)
def test_append_hard_fails_on_an_invalid_row(history_dir: Path, override: dict, match: str) -> None:
    with pytest.raises(LayerRunsError, match=match):
        layer_runs.append_runs(_evidence(), [_run(**override)])


def test_private_layer_carries_name_and_status_only(history_dir: Path) -> None:
    row = _run(layer="sopa_crop", observed_at=None, rendered_at=None)
    layer_runs.append_runs(_evidence(), [row])
    assert layer_runs.read_runs() == [row]


def test_read_hard_fails_on_a_corrupt_store(history_dir: Path) -> None:
    history_dir.mkdir()
    (history_dir / "layer_runs.csv").write_text("run_id,status\n1,success\n", encoding="utf-8")
    with pytest.raises(LayerRunsError, match="header"):
        layer_runs.read_runs()


def test_alert_receipts_round_trip_and_refuse_rewrite(history_dir: Path) -> None:
    layer_runs.append_alerts(_evidence(), "7", ["safex", "agrural"])
    assert layer_runs.read_alerts() == {("7", "safex"), ("7", "agrural")}
    with pytest.raises(LayerRunsError, match="already recorded"):
        layer_runs.append_alerts(_evidence(), "7", ["safex"])


def test_store_is_not_a_snapshot_history_table() -> None:
    # export_history rewrites HISTORY_TABLES wholesale, sorted by PK; an
    # append-only log must never pass through it.
    from pipeline.history import HISTORY_TABLES

    assert "layer_runs" not in HISTORY_TABLES


# ── session date ───────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("moment", "expected"),
    [
        # Monday 21:00 UTC — CBOT settled (14:30 CT cutoff) → Monday.
        (datetime(2026, 10, 19, 21, 0, tzinfo=timezone.utc), date(2026, 10, 19)),
        # Monday 15:00 UTC — before the cutoff → the guard serves Friday.
        (datetime(2026, 10, 19, 15, 0, tzinfo=timezone.utc), date(2026, 10, 16)),
        # Saturday dispatch → Friday's session.
        (datetime(2026, 10, 17, 21, 0, tzinfo=timezone.utc), date(2026, 10, 16)),
        # Tuesday 03:00 UTC is still Monday evening in Chicago → Monday.
        (datetime(2026, 10, 20, 3, 0, tzinfo=timezone.utc), date(2026, 10, 19)),
    ],
)
def test_session_date_is_the_settlement_guard_session_on_a_weekday(moment: datetime, expected: date) -> None:
    assert layer_runs.session_date_for(moment) == expected


# ── one run's rows ─────────────────────────────────────────────────────────


STARTED = datetime(2026, 10, 19, 20, 0, tzinfo=timezone.utc)


def _fresh(status: str, last_attempt: str = "2026-10-19 20:10:00", observed: str | None = "2026-10-19") -> dict:
    return {"status": status, "last_attempt": last_attempt, "observed_at": observed}


def test_rows_for_run_covers_every_production_layer_once() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={},
        evidence=_evidence(), requested=None, rendered_at=None,
    )
    assert [r.layer_name for r in rows] == list(config.PRODUCTION_LAYER_KEYS)
    assert {r.session_date for r in rows} == {date(2026, 10, 19)}


def test_rows_for_run_reads_this_runs_grade_and_observation() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"safex": _fresh("failed", observed=None), "dce": _fresh("usable_partial")},
        evidence=_evidence(), requested=list(config.PRODUCTION_LAYER_KEYS),
        rendered_at="2026-10-19T20:40:00+00:00",
    )
    by_layer = {r.layer_name: r for r in rows}
    assert by_layer["safex"].status == "failed"
    assert by_layer["safex"].observed_at is None
    assert by_layer["dce"].status == "usable_partial"
    assert by_layer["dce"].observed_at == "2026-10-19"
    assert by_layer["dce"].rendered_at == "2026-10-19T20:40:00+00:00"


def test_a_freshness_row_from_an_earlier_run_is_not_this_runs_grade() -> None:
    # The runner's DB is seeded from yesterday's data_freshness.csv; a layer
    # this run never reached still carries yesterday's 'success'.
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"safex": _fresh("success", last_attempt="2026-10-18 20:10:00")},
        evidence=_evidence(), requested=list(config.PRODUCTION_LAYER_KEYS),
        rendered_at=None,
    )
    safex = next(r for r in rows if r.layer_name == "safex")
    assert safex.status == "not_run"
    assert safex.observed_at is None


def test_an_unconfigured_layer_is_skipped_unconfigured_not_failed() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={},
        evidence=_evidence(unconfigured=("us_processor_cash",)),
        requested=list(config.PRODUCTION_LAYER_KEYS), rendered_at=None,
    )
    by_layer = {r.layer_name: r.status for r in rows}
    assert by_layer["us_processor_cash"] == "skipped_unconfigured"
    assert by_layer["safex"] == "not_run"


def test_a_crashed_run_records_every_ungraded_layer_as_not_run() -> None:
    # requested=None: no run summary, so nothing says which layers were asked.
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={},
        evidence=_evidence(unconfigured=("us_processor_cash",)), requested=None, rendered_at=None,
    )
    assert {r.status for r in rows} == {"not_run"}


def test_private_layers_are_blanked_in_rows_for_run() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"sea_india": _fresh("success")},
        evidence=_evidence(), requested=None, rendered_at="2026-10-19T20:40:00+00:00",
    )
    sea = next(r for r in rows if r.layer_name == "sea_india")
    assert (sea.status, sea.observed_at, sea.rendered_at) == ("success", None, None)


def test_rows_for_run_files_under_the_given_roster_only() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={"safex": _fresh("success")},
        evidence=_evidence(roster=("safex", "gone_from_checkout")), requested=None,
        rendered_at=None,
    )
    assert [(r.layer_name, r.status) for r in rows] == [("safex", "success"), ("gone_from_checkout", "not_run")]


def test_append_validates_against_the_runs_roster_not_the_checkouts(history_dir: Path) -> None:
    # A layer the checkout no longer knows is stored as the run filed it; one
    # the run itself never named is still refused.
    row = _run(layer="renamed_mid_run")
    layer_runs.append_runs(_evidence(roster=("safex", "renamed_mid_run")), [row])
    assert layer_runs.read_runs() == [row]
    with pytest.raises(LayerRunsError, match="roster"):
        layer_runs.append_runs(_evidence(roster=("safex",)), [_run("2", layer="renamed_mid_run")])


def test_alerts_validate_against_the_runs_roster(history_dir: Path) -> None:
    layer_runs.append_alerts(_evidence(roster=("renamed_mid_run",)), "7", ["renamed_mid_run"])
    assert layer_runs.read_alerts() == {("7", "renamed_mid_run")}
    with pytest.raises(LayerRunsError, match="roster"):
        layer_runs.append_alerts(_evidence(roster=("safex",)), "8", ["renamed_mid_run"])


def test_run_evidence_says_configured_or_not_and_never_the_value(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in config.API_KEY_CATALOG:
        monkeypatch.delenv(name, raising=False)
    sentinel = "do-not-print-me-0123456789"
    monkeypatch.setenv("FRED_API_KEY", sentinel)
    block = layer_runs.build_run_evidence(STARTED, ("fred", "safex"))

    assert block["roster"] == ["fred", "safex"]
    assert block["started_at"] == "2026-10-19T20:00:00Z"
    assert set(block["api_keys"]) == set(config.API_KEY_CATALOG)
    assert block["api_keys"]["FRED_API_KEY"] == {"configured": True, "skips": ["fred"]}
    assert block["api_keys"]["MARS_API_KEY"]["configured"] is False
    assert block["private"] == list(config.PRIVATE_LAYERS)
    assert sentinel not in repr(block)

    evidence = layer_runs.parse_run_evidence(block, source="test", not_before=STARTED)
    assert evidence.roster == ("fred", "safex")
    assert evidence.configured("fred") is True
    assert evidence.configured("us_processor_cash") is False
    assert evidence.configured("safex") is True
    assert evidence.private == frozenset(config.PRIVATE_LAYERS)


def test_a_layer_private_in_the_run_is_blanked_though_the_checkout_renamed_it(
    history_dir: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A mid-run merge renamed sea_india: the checkout's PRIVATE_LAYERS no
    # longer names the run's layer, and its observation must still never
    # reach the public record (invariant 4).
    monkeypatch.setattr(layer_runs, "PRIVATE_LAYERS", ("sea_india_renamed",))
    evidence = _evidence(roster=("sea_india", "safex"), private=("sea_india",))
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"sea_india": _fresh("success")}, evidence=evidence, requested=None,
        rendered_at="2026-10-19T20:40:00+00:00",
    )
    sea = next(r for r in rows if r.layer_name == "sea_india")
    assert (sea.status, sea.observed_at, sea.rendered_at) == ("success", None, None)
    with pytest.raises(LayerRunsError, match="private"):
        layer_runs.append_runs(evidence, [_run(layer="sea_india")])


def test_a_layer_private_in_either_list_is_blanked(monkeypatch: pytest.MonkeyPatch) -> None:
    # A layer a mid-run merge made private: withholding is the safe direction.
    monkeypatch.setattr(layer_runs, "PRIVATE_LAYERS", ("sea_india",))
    evidence = _evidence(roster=("sea_india", "safex"), private=())
    assert evidence.is_private("sea_india") is True
    assert evidence.is_private("safex") is False


def test_run_identity_separates_rerun_attempts() -> None:
    env = {"GITHUB_RUN_ID": "123", "GITHUB_RUN_ATTEMPT": "1",
           "GITHUB_WORKFLOW": "Update Dashboard", "GITHUB_EVENT_NAME": "repository_dispatch"}
    assert layer_runs.run_identity(env) == ("123", "Update Dashboard/repository_dispatch")
    assert layer_runs.run_identity({**env, "GITHUB_RUN_ATTEMPT": "2"})[0] == "123-2"
    with pytest.raises(LayerRunsError, match="GITHUB_RUN_ID"):
        layer_runs.run_identity({})


def test_a_retired_layer_in_old_rows_never_bricks_the_record(history_dir: Path) -> None:
    # Rows were valid against their own day's roster. Retiring that layer
    # later must leave the record readable and appendable.
    history_dir.mkdir()
    (history_dir / "layer_runs.csv").write_text(
        ",".join(layer_runs.COLUMNS) + "\n"
        "1,w,2026-10-19T20:05:00Z,2026-10-19,retired_layer,old_word,,\n",
        encoding="utf-8",
    )
    assert [r.layer_name for r in layer_runs.read_runs()] == ["retired_layer"]
    layer_runs.append_runs(_evidence(), [_run("2")])
    assert len(layer_runs.read_runs()) == 2


# ── byte-append-only (B42 #555) ───────────────────────────────────────────
#
# The committed record mixes endings: the B34 append read the old body in
# universal-newline mode (CRLF → LF) and wrote its own rows CRLF, so every
# record commit rewrote the block before it. These fixtures are that shape.

_RUNS_HEADER = ",".join(layer_runs.COLUMNS)
_MIXED_RUNS = (
    _RUNS_HEADER + "\n"
    "1,Update Dashboard/schedule,2026-10-19T20:05:00Z,2026-10-19,safex,success,2026-10-19,\n"
    "2,Update Dashboard/schedule,2026-10-20T20:05:00Z,2026-10-20,safex,failed,,\r\n"
    "2,Update Dashboard/schedule,2026-10-20T20:05:00Z,2026-10-20,dce,success,2026-10-20,\r\n"
).encode("utf-8")
_MIXED_ALERTS = b"run_id,layer_name\n1,safex\n2,safex\r\n"


def _seed(history_dir: Path, name: str, body: bytes) -> Path:
    history_dir.mkdir(exist_ok=True)
    path = history_dir / name
    path.write_bytes(body)
    return path


@pytest.mark.parametrize("seed", [None, _MIXED_RUNS], ids=["fresh", "mixed-endings"])
def test_a_runs_append_keeps_every_earlier_byte_and_adds_lf_rows(history_dir: Path, seed: bytes | None) -> None:
    path = history_dir / "layer_runs.csv"
    if seed is None:
        layer_runs.append_runs(_evidence(), [_run("1")])
    else:
        _seed(history_dir, "layer_runs.csv", seed)
    before = path.read_bytes()

    layer_runs.append_runs(_evidence(), [_run("3"), _run("3", layer="dce")])

    after = path.read_bytes()
    assert after[: len(before)] == before
    tail = after[len(before):]
    assert tail.count(b"\n") == 2 and b"\r" not in tail


def test_an_alerts_append_keeps_every_earlier_byte_and_adds_lf_rows(history_dir: Path) -> None:
    path = _seed(history_dir, "layer_alerts.csv", _MIXED_ALERTS)
    layer_runs.append_alerts(_evidence(), "3", ["safex", "dce"])

    after = path.read_bytes()
    assert after[: len(_MIXED_ALERTS)] == _MIXED_ALERTS
    tail = after[len(_MIXED_ALERTS):]
    assert tail.count(b"\n") == 2 and b"\r" not in tail


def test_a_fresh_store_is_lf_throughout(history_dir: Path) -> None:
    layer_runs.append_runs(_evidence(), [_run("1")])
    layer_runs.append_alerts(_evidence(), "1", ["safex"])
    for name in ("layer_runs.csv", "layer_alerts.csv"):
        assert b"\r" not in (history_dir / name).read_bytes()


def test_an_append_onto_a_body_without_a_final_newline_only_adds_bytes(history_dir: Path) -> None:
    body = _MIXED_RUNS.rstrip(b"\r\n")
    path = _seed(history_dir, "layer_runs.csv", body)
    layer_runs.append_runs(_evidence(), [_run("3")])
    after = path.read_bytes()
    assert after.startswith(body + b"\n")
    assert [r.run_id for r in layer_runs.read_runs()] == ["1", "2", "2", "3"]


@pytest.mark.skipif(shutil.which("git") is None, reason="needs the git CLI")
def test_git_sees_a_record_append_as_insertions_only(history_dir: Path) -> None:
    # The check an auditor runs on the committed record: an append-only file's
    # commits show 0 deletions in `git diff --numstat`.
    _seed(history_dir, "layer_runs.csv", _MIXED_RUNS)
    _seed(history_dir, "layer_alerts.csv", _MIXED_ALERTS)

    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-c", "core.autocrlf=false", "-c", "user.name=t", "-c", "user.email=t@t",
             "-C", str(history_dir), *args],
            check=True, capture_output=True, text=True,
        ).stdout

    git("init", "-q")
    git("add", ".")
    git("commit", "-q", "-m", "seed")
    layer_runs.append_runs(_evidence(), [_run("3"), _run("3", layer="dce")])
    layer_runs.append_alerts(_evidence(), "3", ["safex"])

    numstat = sorted(line.split("\t") for line in git("diff", "--numstat").splitlines())
    assert numstat == [["1", "0", "layer_alerts.csv"], ["2", "0", "layer_runs.csv"]]


def test_mixed_endings_read_as_the_same_rows_and_figures(history_dir: Path) -> None:
    from pipeline import reliability

    _seed(history_dir, "layer_runs.csv", _MIXED_RUNS)
    _seed(history_dir, "layer_alerts.csv", _MIXED_ALERTS)
    mixed_rows, mixed_alerts = layer_runs.read_runs(), layer_runs.read_alerts()

    _seed(history_dir, "layer_runs.csv", _MIXED_RUNS.replace(b"\r\n", b"\n"))
    _seed(history_dir, "layer_alerts.csv", _MIXED_ALERTS.replace(b"\r\n", b"\n"))
    assert layer_runs.read_runs() == mixed_rows
    assert layer_runs.read_alerts() == mixed_alerts

    assert [(r.run_id, r.layer_name, r.status, r.observed_at) for r in mixed_rows] == [
        ("1", "safex", "success", "2026-10-19"),
        ("2", "safex", "failed", None),
        ("2", "dce", "success", "2026-10-20"),
    ]
    assert mixed_alerts == {("1", "safex"), ("2", "safex")}
    block = reliability.reliability(mixed_rows, mixed_alerts, through=date(2026, 10, 20),
                                    layers=("safex", "dce"), basket=())
    safex = next(entry for entry in block["layers"] if entry["layer"] == "safex")
    assert block["state"] == "ok"
    assert safex["logged_sessions"] == 2
    assert safex["availability_pct"] == 50.0
    assert safex["surfaced_within_one_run_pct"] == 100.0


@pytest.mark.parametrize(
    ("name", "header"),
    [("layer_runs.csv", _RUNS_HEADER), ("layer_alerts.csv", "run_id,layer_name")],
)
def test_a_header_only_crlf_store_keeps_its_header_bytes(history_dir: Path, name: str, header: str) -> None:
    # A file with a header and no rows yet is still an existing file: its
    # header is copied, never rewritten in this module's ending.
    body = (header + "\r\n").encode("utf-8")
    path = _seed(history_dir, name, body)
    if name == "layer_runs.csv":
        layer_runs.append_runs(_evidence(), [_run("1")])
    else:
        layer_runs.append_alerts(_evidence(), "1", ["safex"])

    after = path.read_bytes()
    assert after[: len(body)] == body
    tail = after[len(body):]
    assert tail.count(b"\n") == 1 and b"\r" not in tail
