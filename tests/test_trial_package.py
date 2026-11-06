"""The paste-in edition (mirror_mcp.package): every tool's envelope on disk, private path only.

Seams: ``build_package(settings, out_dir, clock=...)``. The DB is the project
schema with one freshness row and one price (the same shape test_mirror_mcp uses).
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest

import config
from analysis.trial.sanitize import PrivacyLeak
from mirror_mcp.package import MARKET_TOOLS, NOTE_FILE, ORIGINS_OMITTED, TOOLS_FILE, EmptyEdition, build_package
from mirror_mcp.settings import Settings

TOKEN = "k" * 32  # public-sample-key: not a secret
CLOCK = datetime(2026, 11, 7, 9, 0, tzinfo=timezone.utc)


@pytest.fixture
def settings(tmp_db: sqlite3.Connection, tmp_path: Path) -> Settings:
    tmp_db.execute(
        "INSERT INTO data_freshness(layer_name,last_success,last_attempt,rows_fetched,status) "
        "VALUES ('prices','2026-11-04T21:05:00+00:00','2026-11-04T21:05:00+00:00',10,'success')"
    )
    tmp_db.execute(
        "INSERT INTO prices(commodity,Date,Open,High,Low,Close,Volume) "
        "VALUES ('soybeans','2026-11-04',400,410,395,405.5,1000)"
    )
    tmp_db.execute(
        "INSERT INTO briefings(briefing_date,text,generated_at) "
        "VALUES ('2026-11-06','=== Mirror Market Daily Briefing — 2026-11-06 ===\nSOYBEANS: text only','2026-11-06T19:00:00Z')"
    )
    tmp_db.commit()
    return Settings(db_path=tmp_path / "test.db", path_token=TOKEN, log_path=tmp_path / "out" / "q.jsonl",
                    db_built_at="2026-11-06T19:00:00+00:00").validated()


def test_package_writes_every_tool_once_with_the_briefing(settings: Settings, tmp_path: Path) -> None:
    out = tmp_path / "out"
    upload = build_package(settings, out, clock=lambda: CLOCK)

    assert upload == out / "mirror-market-2026-11-07.md"
    text = upload.read_text(encoding="utf-8")
    assert "## Daily briefing (2026-11-06)" in text and "SOYBEANS: text only" in text
    assert "Never claim a price is settled" in text  # the server's own rules travel with the file
    for market in config.MARKETS:
        for tool in MARKET_TOOLS:
            assert f"### {tool}({market})" in text

    envelopes = json.loads((out / TOOLS_FILE).read_text(encoding="utf-8"))["envelopes"]
    expected = 2 + len(config.MARKETS) * len(MARKET_TOOLS) + len(config.DESTINATION_PORTS)
    assert len(envelopes) == expected
    assert all({"state", "reason", "data", "meta"} <= set(e) for e in envelopes.values())
    assert envelopes["freshness"]["state"] == "ok"
    # an empty answer stays an answer, with its reason
    assert envelopes["river"]["state"] == "empty" and envelopes["river"]["reason"]
    assert (out / NOTE_FILE).read_text(encoding="utf-8").count(upload.name) == 2


def test_origins_scaffolding_is_dropped_and_named(settings: Settings, tmp_path: Path) -> None:
    build_package(settings, tmp_path / "out", clock=lambda: CLOCK)
    envelopes = json.loads((tmp_path / "out" / TOOLS_FILE).read_text(encoding="utf-8"))["envelopes"]
    for destination in config.DESTINATION_PORTS:
        env = envelopes[f"origins:{destination}"]
        assert env["meta"]["omitted_from_package"] == list(ORIGINS_OMITTED)
        assert not set(ORIGINS_OMITTED) & set(env["data"])
        assert "rankings" in env["data"]


def test_docs_output_is_refused_before_any_tool_runs(settings: Settings) -> None:
    repo = Path(config.__file__).resolve().parent
    with pytest.raises(PrivacyLeak):
        build_package(settings, repo / "docs" / "package", clock=lambda: CLOCK)


def test_unstamped_db_is_refused(tmp_db: sqlite3.Connection, tmp_path: Path) -> None:
    settings = Settings(db_path=tmp_path / "test.db", path_token=TOKEN, log_path=tmp_path / "out" / "q.jsonl",
                        db_built_at="x").validated()
    with pytest.raises(EmptyEdition, match="data_freshness is empty"):
        build_package(settings, tmp_path / "out", clock=lambda: CLOCK)
    assert not (tmp_path / "out" / TOOLS_FILE).exists()
