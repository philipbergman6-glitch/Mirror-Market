"""#567 — a private layer's dates are private too (owner decision 2026-11-25, option b).

Two halves:

- **Agreement.** The run record, the edition manifest and the Layer
  Freshness table say the same thing about a private layer: its status, and
  no date. The record is the reference (``pipeline.layer_runs.rows_for_run``).
- **A dated private row is a breach, not something to hide.** A private layer
  runs only where ``MATIF_PRIVATE_FETCH`` is set — the desk, never CI. A
  ``data_freshness`` row carrying a date for it in the history export or in
  the MCP's database means the switch reached a publishing machine, so both
  refuse (invariant 1) instead of blanking the row.

  Where (owner decision on #567, option B): the history export refuses only
  inside GitHub Actions (``GITHUB_ACTIONS=true``), the only place that commits
  ``data/history/``; a desk run with the switch set proceeds and the export
  logs one WARNING naming the layer. The MCP refuses everywhere, since its
  image is built in CI.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

import config
from pipeline import layer_runs
from pipeline.layer_runs import RunEvidence
from scripts import generate_html

PRIVATE = "matif"
assert PRIVATE in config.PRIVATE_LAYERS

STARTED = datetime(2026, 11, 25, 20, 0, tzinfo=timezone.utc)


def _public_rows() -> dict[str, dict]:
    """Every public layer graded ``success`` by this run; no private row (CI's shape)."""
    stamp = (STARTED + timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
    return {
        layer: {"layer_name": layer, "status": "success", "last_success": stamp,
                "last_attempt": stamp, "observed_at": "2026-11-25", "rows_fetched": 1}
        for layer in config.PRODUCTION_LAYER_KEYS if layer not in config.PRIVATE_LAYERS
    }


def test_record_manifest_and_freshness_table_agree_on_every_private_layer(monkeypatch):
    rows = _public_rows()
    unconfigured = frozenset(
        layer for spec in config.API_KEY_CATALOG.values() if spec.name == config.MATIF_FETCH_ENV
        for layer in spec.skips
    )
    evidence = RunEvidence(started_at=STARTED, roster=config.PRODUCTION_LAYER_KEYS,
                           unconfigured=unconfigured, private=frozenset(config.PRIVATE_LAYERS))
    record = {
        r.layer_name: r for r in layer_runs.rows_for_run(
            run_id="1", workflow="Update Dashboard/schedule", run_started_at=STARTED,
            freshness=rows, evidence=evidence, requested=config.PRODUCTION_LAYER_KEYS,
            rendered_at="2026-11-25T20:40:00+00:00",
        )
    }
    frame = pd.DataFrame(list(rows.values()))
    for column in ("last_success", "last_attempt", "observed_at"):
        frame[column] = pd.to_datetime(frame[column])
    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: frame)
    from app.manifest import _coverage

    manifest = {r["layer"]: r for r in _coverage()}
    table = {i["name"]: i for i in generate_html._build_freshness_items()}

    for layer in config.PRIVATE_LAYERS:
        assert record[layer].status == manifest[layer]["status"] == table[layer]["status"], layer
        assert record[layer].observed_at is None and record[layer].rendered_at is None
        assert manifest[layer]["observed_at"] is None and manifest[layer]["last_success"] is None
        assert table[layer]["observed"] is None and table[layer]["fetched"] is None
    # The public layers still carry their dates on every surface: the rule is
    # scoped to PRIVATE_LAYERS, not a blanket blank.
    assert record["prices"].observed_at == "2026-11-25"
    assert manifest["prices"]["observed_at"] is not None


@pytest.fixture
def history_db(patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A temp DB wired into pipeline.history, holding one public freshness row."""
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(tmp_path / "history"))
    monkeypatch.setattr("pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db)))
    with sqlite3.connect(str(patched_db)) as conn:
        conn.execute(
            "INSERT INTO data_freshness(layer_name,last_success,last_attempt,rows_fetched,status) "
            "VALUES ('prices','2026-11-25 20:05:00','2026-11-25 20:05:00',10,'success')"
        )
    return patched_db


def _dated_private_row(db: Path) -> None:
    """What a run with MATIF_PRIVATE_FETCH set writes (save_freshness always stamps last_attempt)."""
    with sqlite3.connect(str(db)) as conn:
        conn.execute(
            "INSERT INTO data_freshness(layer_name,last_success,last_attempt,rows_fetched,status,observed_at) "
            "VALUES (?,'2026-11-25 20:05:00','2026-11-25 20:05:00',3,'success','2026-11-25')", (PRIVATE,)
        )


# Every environment that is not GitHub Actions: GitHub sets the variable to
# the string "true" on every runner, and nothing else counts as CI.
DESK_ENVIRONMENTS = [pytest.param(None, id="unset"), pytest.param("", id="empty"),
                     pytest.param("false", id="false")]


def _set_github_actions(monkeypatch: pytest.MonkeyPatch, value: str | None) -> None:
    if value is None:
        monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    else:
        monkeypatch.setenv("GITHUB_ACTIONS", value)


def test_history_export_refuses_a_dated_private_freshness_row_in_ci(history_db, tmp_path, monkeypatch):
    from pipeline.history import export_history
    from pipeline.public_view import PrivateLayerLeak

    _set_github_actions(monkeypatch, "true")
    _dated_private_row(history_db)
    with pytest.raises(PrivateLayerLeak, match=PRIVATE):
        export_history()
    # Refused before anything was written: no table's CSV is half-updated.
    assert not (tmp_path / "history").exists() or not any((tmp_path / "history").iterdir())


@pytest.mark.parametrize("github_actions", DESK_ENVIRONMENTS)
def test_desk_history_export_proceeds_with_one_warning_naming_the_layer(
    history_db, tmp_path, monkeypatch, caplog, github_actions,
):
    """A desk run with MATIF_PRIVATE_FETCH set is not a leak: the desk never commits data/history/."""
    from pipeline.history import export_history

    _set_github_actions(monkeypatch, github_actions)
    _dated_private_row(history_db)
    with caplog.at_level("WARNING"):
        assert export_history() > 0
    named = [r for r in caplog.records if r.levelname == "WARNING" and PRIVATE in r.getMessage()]
    assert len(named) == 1, [r.getMessage() for r in caplog.records]
    assert config.MATIF_FETCH_ENV in named[0].getMessage()
    assert (tmp_path / "history" / "data_freshness.csv").exists()


@pytest.mark.parametrize("github_actions", [*DESK_ENVIRONMENTS, pytest.param("true", id="ci")])
def test_history_export_writes_the_freshness_table_when_no_private_row_exists(
    history_db, tmp_path, monkeypatch, caplog, github_actions,
):
    from pipeline.history import export_history

    _set_github_actions(monkeypatch, github_actions)
    with caplog.at_level("WARNING"):
        export_history()
    assert not [r for r in caplog.records if PRIVATE in r.getMessage()]
    text = (tmp_path / "history" / "data_freshness.csv").read_text()
    assert "prices" in text and PRIVATE not in text


TOKEN = "k" * 32


def _mcp_settings(db: Path, tmp_path: Path):
    from mirror_mcp.settings import Settings

    return Settings(db_path=db, path_token=TOKEN, log_path=tmp_path / "log" / "q.jsonl",
                    db_built_at="2026-11-25T21:00:00+00:00").validated()


@pytest.mark.parametrize("github_actions", [*DESK_ENVIRONMENTS, pytest.param("true", id="ci")])
def test_mcp_refuses_a_database_with_a_dated_private_freshness_row(history_db, tmp_path, monkeypatch, github_actions):
    """Unconditional: the MCP image is built in CI, so no environment may start it on a desk DB."""
    from mirror_mcp.server import build_server
    from pipeline.public_view import PrivateLayerLeak

    _set_github_actions(monkeypatch, github_actions)
    _dated_private_row(history_db)
    with pytest.raises(PrivateLayerLeak, match=PRIVATE):
        build_server(_mcp_settings(history_db, tmp_path))


def test_mcp_starts_on_a_database_with_no_private_row(history_db, tmp_path):
    from mirror_mcp.server import build_server

    assert build_server(_mcp_settings(history_db, tmp_path)) is not None
