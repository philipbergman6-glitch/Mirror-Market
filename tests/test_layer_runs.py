"""The source reliability record's store (B34 #520): data/history/layer_runs.csv.

Append-only, one row per (run, layer). Every test points
``pipeline.history.HISTORY_DIR`` at ``tmp_path`` — the history-write guard
refuses the real directory, and only the deploy workflow writes there.
"""

from __future__ import annotations

import csv
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

import config
from pipeline import layer_runs
from pipeline.layer_runs import LayerRun, LayerRunsError

ROSTER = config.PRODUCTION_LAYER_KEYS


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
    layer_runs.append_runs(ROSTER, rows)

    with open(history_dir / "layer_runs.csv", newline="", encoding="utf-8") as fh:
        header = next(csv.reader(fh))
    assert tuple(header) == layer_runs.COLUMNS
    assert layer_runs.read_runs() == rows


def test_append_adds_rows_and_never_rewrites_earlier_ones(history_dir: Path) -> None:
    layer_runs.append_runs(ROSTER, [_run("1")])
    before = (history_dir / "layer_runs.csv").read_text(encoding="utf-8")
    layer_runs.append_runs(ROSTER, [_run("2")])
    after = (history_dir / "layer_runs.csv").read_text(encoding="utf-8")

    assert after.startswith(before)
    assert [r.run_id for r in layer_runs.read_runs()] == ["1", "2"]


def test_append_refuses_to_rewrite_an_existing_run_layer_row(history_dir: Path) -> None:
    layer_runs.append_runs(ROSTER, [_run("1", status="failed")])
    with pytest.raises(LayerRunsError, match="already recorded"):
        layer_runs.append_runs(ROSTER, [_run("1", status="success")])
    assert [r.status for r in layer_runs.read_runs()] == ["failed"]


def test_append_refuses_a_duplicate_inside_one_batch(history_dir: Path) -> None:
    with pytest.raises(LayerRunsError, match="twice"):
        layer_runs.append_runs(ROSTER, [_run("1"), _run("1")])
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
        layer_runs.append_runs(ROSTER, [_run(**override)])


def test_private_layer_carries_name_and_status_only(history_dir: Path) -> None:
    row = _run(layer="sopa_crop", observed_at=None, rendered_at=None)
    layer_runs.append_runs(ROSTER, [row])
    assert layer_runs.read_runs() == [row]


def test_read_hard_fails_on_a_corrupt_store(history_dir: Path) -> None:
    history_dir.mkdir()
    (history_dir / "layer_runs.csv").write_text("run_id,status\n1,success\n", encoding="utf-8")
    with pytest.raises(LayerRunsError, match="header"):
        layer_runs.read_runs()


def test_alert_receipts_round_trip_and_refuse_rewrite(history_dir: Path) -> None:
    layer_runs.append_alerts(ROSTER, "7", ["safex", "agrural"])
    assert layer_runs.read_alerts() == {("7", "safex"), ("7", "agrural")}
    with pytest.raises(LayerRunsError, match="already recorded"):
        layer_runs.append_alerts(ROSTER, "7", ["safex"])


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
        roster=config.PRODUCTION_LAYER_KEYS, requested=None, configured=lambda _layer: True, rendered_at=None,
    )
    assert [r.layer_name for r in rows] == list(config.PRODUCTION_LAYER_KEYS)
    assert {r.session_date for r in rows} == {date(2026, 10, 19)}


def test_rows_for_run_reads_this_runs_grade_and_observation() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"safex": _fresh("failed", observed=None), "dce": _fresh("usable_partial")},
        roster=config.PRODUCTION_LAYER_KEYS, requested=list(config.PRODUCTION_LAYER_KEYS), configured=lambda _layer: True,
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
        roster=config.PRODUCTION_LAYER_KEYS, requested=list(config.PRODUCTION_LAYER_KEYS), configured=lambda _layer: True,
        rendered_at=None,
    )
    safex = next(r for r in rows if r.layer_name == "safex")
    assert safex.status == "not_run"
    assert safex.observed_at is None


def test_an_unconfigured_layer_is_skipped_unconfigured_not_failed() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={},
        roster=config.PRODUCTION_LAYER_KEYS, requested=list(config.PRODUCTION_LAYER_KEYS),
        configured=lambda layer: layer != "us_processor_cash", rendered_at=None,
    )
    by_layer = {r.layer_name: r.status for r in rows}
    assert by_layer["us_processor_cash"] == "skipped_unconfigured"
    assert by_layer["safex"] == "not_run"


def test_a_crashed_run_records_every_ungraded_layer_as_not_run() -> None:
    # requested=None: no run summary, so nothing says which layers were asked.
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={},
        roster=config.PRODUCTION_LAYER_KEYS, requested=None, configured=lambda layer: layer != "us_processor_cash", rendered_at=None,
    )
    assert {r.status for r in rows} == {"not_run"}


def test_private_layers_are_blanked_in_rows_for_run() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED,
        freshness={"sea_india": _fresh("success")},
        roster=config.PRODUCTION_LAYER_KEYS, requested=None, configured=lambda _layer: True, rendered_at="2026-10-19T20:40:00+00:00",
    )
    sea = next(r for r in rows if r.layer_name == "sea_india")
    assert (sea.status, sea.observed_at, sea.rendered_at) == ("success", None, None)


def test_rows_for_run_files_under_the_given_roster_only() -> None:
    rows = layer_runs.rows_for_run(
        run_id="9", workflow="w", run_started_at=STARTED, freshness={"safex": _fresh("success")},
        roster=("safex", "gone_from_checkout"), requested=None, configured=lambda _layer: True,
        rendered_at=None,
    )
    assert [(r.layer_name, r.status) for r in rows] == [("safex", "success"), ("gone_from_checkout", "not_run")]


def test_append_validates_against_the_runs_roster_not_the_checkouts(history_dir: Path) -> None:
    # A layer the checkout no longer knows is stored as the run filed it; one
    # the run itself never named is still refused.
    row = _run(layer="renamed_mid_run")
    layer_runs.append_runs(("safex", "renamed_mid_run"), [row])
    assert layer_runs.read_runs() == [row]
    with pytest.raises(LayerRunsError, match="roster"):
        layer_runs.append_runs(("safex",), [_run("2", layer="renamed_mid_run")])


def test_alerts_validate_against_the_runs_roster(history_dir: Path) -> None:
    layer_runs.append_alerts(("renamed_mid_run",), "7", ["renamed_mid_run"])
    assert layer_runs.read_alerts() == {("7", "renamed_mid_run")}
    with pytest.raises(LayerRunsError, match="roster"):
        layer_runs.append_alerts(("safex",), "8", ["renamed_mid_run"])


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
    assert sentinel not in repr(block)

    evidence = layer_runs.parse_run_evidence(block, source="test", not_before=STARTED)
    assert evidence.roster == ("fred", "safex")
    assert evidence.configured("fred") is True
    assert evidence.configured("us_processor_cash") is False
    assert evidence.configured("safex") is True


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
    layer_runs.append_runs(ROSTER, [_run("2")])
    assert len(layer_runs.read_runs()) == 2
