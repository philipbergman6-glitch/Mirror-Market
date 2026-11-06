"""The hosted MCP edition (#578): one read-only server over the CI SQLite.

Seams: ``build_server(settings)`` (startup validation), the MCP client
interface over an in-memory ``fastmcp.Client``, the ``/health`` route, and the
question-log file. Nothing reaches inside the package.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest
from fastmcp import Client

import config
from mirror_mcp.server import build_server
from mirror_mcp.settings import Settings, SettingsError

pytestmark = pytest.mark.anyio

TOKEN = "k" * 32


@pytest.fixture
def db_path(tmp_db: sqlite3.Connection, tmp_path: Path) -> Path:
    tmp_db.execute(
        "INSERT INTO data_freshness(layer_name,last_success,last_attempt,rows_fetched,status) "
        "VALUES ('prices','2026-11-04T21:05:00+00:00','2026-11-04T21:05:00+00:00',10,'success')"
    )
    tmp_db.execute(
        "INSERT INTO prices(commodity,Date,Open,High,Low,Close,Volume) "
        "VALUES ('soybeans','2026-11-04',400,410,395,405.5,1000)"
    )
    tmp_db.commit()
    return tmp_path / "test.db"


@pytest.fixture
def settings(db_path: Path, tmp_path: Path) -> Settings:
    return Settings(
        db_path=db_path, path_token=TOKEN, log_path=tmp_path / "log" / "q.jsonl",
        db_built_at="2026-11-05T00:00:00+00:00",
    ).validated()


# -- startup -----------------------------------------------------------------

def test_missing_db_is_a_startup_error(tmp_path):
    with pytest.raises(SettingsError, match="db_path"):
        Settings(db_path=tmp_path / "nope.db", path_token=TOKEN, log_path=tmp_path / "q.jsonl",
                 db_built_at="x").validated()


def test_short_token_is_a_startup_error(db_path, tmp_path):
    with pytest.raises(SettingsError, match="path_token"):
        Settings(db_path=db_path, path_token="short", log_path=tmp_path / "q.jsonl",
                 db_built_at="x").validated()


def test_log_directory_is_created_not_assumed(settings):
    assert settings.log_path.parent.is_dir()


def test_env_is_the_one_config_site(monkeypatch, db_path, tmp_path):
    monkeypatch.setenv("MIRROR_MCP_DB_PATH", str(db_path))
    monkeypatch.setenv("MIRROR_MCP_PATH_TOKEN", TOKEN)
    monkeypatch.setenv("MIRROR_MCP_LOG_PATH", str(tmp_path / "q.jsonl"))
    monkeypatch.setenv("MIRROR_MCP_DB_BUILT_AT", "2026-11-05T00:00:00+00:00")
    monkeypatch.setenv("MIRROR_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("MIRROR_MCP_PORT", "8000")
    assert Settings.from_env().validated().path_token == TOKEN
    monkeypatch.delenv("MIRROR_MCP_PATH_TOKEN")
    with pytest.raises(SettingsError, match="MIRROR_MCP_PATH_TOKEN"):
        Settings.from_env()


# -- freshness ---------------------------------------------------------------

async def test_freshness_reports_each_layer_against_its_budget(settings):
    from datetime import datetime, timezone

    async with Client(build_server(settings, clock=lambda: datetime(2026, 11, 7, 9, 0, tzinfo=timezone.utc))) as client:
        result = await client.call_tool("freshness")
    body = result.structured_content
    assert body["state"] == "ok"
    rows = {r["layer"]: r for r in body["data"]["layers"]}
    prices = rows["prices"]
    assert prices["status"] == "success"
    assert prices["last_success"] == "2026-11-04T21:05:00+00:00"
    assert prices["number"] == "1"
    assert prices["budget_days"] >= 1
    assert prices["age_days"] == 2  # judged on the injected clock, 2026-11-07 vs the 11-04 success
    # A rostered layer with no freshness row is "never learned", not a zero — the manifest's own word.
    from mirror_mcp.server import LAYER_NOT_RUN

    assert rows["fred"]["status"] == LAYER_NOT_RUN == "not-run"
    assert rows["fred"]["last_success"] is None
    assert body["meta"]["db_built_at"] == "2026-11-05T00:00:00+00:00"
    assert body["meta"]["latest_session"] == "2026-11-04"


# -- series: allow-list, bounds, read-only ------------------------------------

def _query_read_tables() -> set[str]:
    """Tables pipeline.query's read_* functions serve, from the source itself."""
    import re
    from pathlib import Path

    from pipeline import query

    src = Path(query.__file__).read_text()
    return set(re.findall(r'_read_table\(\s*"([a-z_]+)"', src)) | set(re.findall(r"FROM ([a-z_]+)", src))


def test_allow_list_mirrors_pipeline_query_minus_the_excluded_seams():
    from mirror_mcp import db

    assert set(db.ALLOWED) == _query_read_tables() - db.EXCLUDED - {"sqlite_master"}
    assert _query_read_tables() >= db.EXCLUDED
    assert "quarantined_revisions" not in db.ALLOWED


def test_every_allow_list_column_exists_in_the_schema(tmp_db):
    from mirror_mcp import db

    for table, (filter_col, date_col) in db.ALLOWED.items():
        cols = {r[1] for r in tmp_db.execute(f"pragma table_info({table})")}
        assert date_col in cols, (table, date_col)
        assert filter_col is None or filter_col in cols, (table, filter_col)


async def test_series_is_bounded_newest_first_and_filtered(settings, db_path):
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO prices(commodity,Date,Close) VALUES (?,?,?)",
        [("soybeans", "2026-11-03", 401.0), ("soybean_oil", "2026-11-04", 50.0), ("soybeans", "2026-11-02", 399.0)],
    )
    conn.commit()
    conn.close()
    async with Client(build_server(settings)) as client:
        result = await client.call_tool("series", {"table": "prices", "key": "soybeans", "limit": 2})
        body = result.structured_content
        assert body["state"] == "ok"
        assert [r["Date"] for r in body["data"]["rows"]] == ["2026-11-04", "2026-11-03"]
        assert body["data"]["truncated"] is True
        ranged = await client.call_tool("series", {"table": "prices", "key": "soybeans", "end": "2026-11-02"})
        assert [r["Close"] for r in ranged.structured_content["data"]["rows"]] == [399.0]
        empty = await client.call_tool("series", {"table": "cot"})
        assert empty.structured_content["state"] == "empty"
        assert empty.structured_content["reason"]


async def test_series_refuses_unknown_table_naming_the_allowed_ones(settings):
    from fastmcp.exceptions import ToolError

    async with Client(build_server(settings)) as client:
        with pytest.raises(ToolError, match="quarantined_revisions.*not served|not served"):
            await client.call_tool("series", {"table": "quarantined_revisions"})
        with pytest.raises(ToolError, match="limit"):
            await client.call_tool("series", {"table": "prices", "limit": 5000})


def test_private_layer_tables_are_refused_at_startup_when_they_hold_rows(settings, db_path, monkeypatch):
    from mirror_mcp import db

    monkeypatch.setattr(config, "PRIVATE_LAYERS", ("sea_india",))
    # The schema creates every table in every database: an empty private
    # table is DDL, and the server starts. A row in it is the leak.
    build_server(settings)
    conn = sqlite3.connect(db_path)
    conn.execute("INSERT INTO sea_india_rates(Date, series, value, unit) VALUES ('2026-11-04','x',1.0,'USD/MT')")
    conn.commit()
    conn.close()
    with pytest.raises(db.PrivacyRefusal, match="sea_india_rates"):
        build_server(settings)


def test_a_table_outside_the_roster_is_refused_at_startup(settings, db_path):
    from mirror_mcp import db

    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE desk_positions(x)")
    conn.commit()
    conn.close()
    conn.close()
    with pytest.raises(db.PrivacyRefusal, match="desk_positions"):
        build_server(settings)


@pytest.fixture
def opened(monkeypatch):
    """The connections build_server opens, observed at db.open_readonly — the server's one seam to SQLite."""
    from mirror_mcp import db

    seen: list[sqlite3.Connection] = []
    real = db.open_readonly

    def spy(path):
        conn = real(path)
        seen.append(conn)
        return conn

    monkeypatch.setattr(db, "open_readonly", spy)
    return seen


def test_the_servers_connection_cannot_write(settings, opened):
    build_server(settings)
    (conn,) = opened
    with pytest.raises(sqlite3.OperationalError, match="readonly"):
        conn.execute("INSERT INTO prices(commodity,Date) VALUES ('x','2026-01-01')")


def test_an_unmapped_private_layer_is_a_startup_error(settings, monkeypatch):
    from mirror_mcp import db

    monkeypatch.setattr(config, "PRIVATE_LAYERS", ("brand_new_private_layer",))
    with pytest.raises(db.PrivacyRefusal, match="brand_new_private_layer"):
        build_server(settings)


# -- market blocks: price, crush, spread, supply_demand, weather ----------------

from datetime import date, datetime, timezone  # noqa: E402

from pricing import semantics  # noqa: E402

CLOCK = datetime(2026, 11, 5, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def market_db(db_path: Path) -> Path:
    conn = sqlite3.connect(db_path)
    for i, close in enumerate([400.0, 402.0, 405.5]):
        day = date(2026, 11, 2 + i).isoformat()
        for commodity, value in (("Soybeans", close), ("Soybean Oil", 50.0), ("Soybean Meal", 300.0)):
            conn.execute(
                "INSERT INTO prices(commodity,Date,Open,High,Low,Close,Volume) VALUES (?,?,?,?,?,?,1)",
                (commodity, day, value, value, value, value),
            )
    conn.commit()
    conn.close()
    return db_path


async def test_price_is_the_site_block_with_price_semantics_in_meta(settings, market_db):
    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        body = (await client.call_tool("price", {"market": "cbot"})).structured_content
    assert body["state"] == "ok"
    # 405.5 ¢/bu × 36.7437 bu/MT ÷ 100 = 149.00 USD/MT (CME contract spec: 60 lb/bu) — a worked example, not the code's own arithmetic
    assert body["data"]["headline"]["usd_mt"] == pytest.approx(149.0, abs=0.01)
    assert body["meta"]["as_of"] == "2026-11-04"
    assert body["meta"]["source_layer"] == "prices"
    assert body["meta"]["price_type"] == semantics.PriceType.DELAYED_CLOSE.value
    assert body["meta"]["confidence"] == semantics.Confidence.BOARD_REFERENCE.value
    assert "settlement" not in body["meta"]["permitted_claims"]
    assert body["meta"]["caveat"]
    assert body["data"]["currency"]["state"] == "absent"  # CBOT quotes in USD; the reason says so
    assert "USD" in body["data"]["currency"]["reason"]


async def test_market_slug_is_validated_against_the_registry(settings):
    from fastmcp.exceptions import ToolError

    async with Client(build_server(settings)) as client:
        with pytest.raises(ToolError, match="argentina.*cbot.*dalian"):
            await client.call_tool("price", {"market": "chicago"})


async def test_absent_and_empty_blocks_keep_their_reasons(settings, market_db):
    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        nigeria = (await client.call_tool("crush", {"market": "nigeria"})).structured_content
        crush = (await client.call_tool("crush", {"market": "cbot"})).structured_content
        sd = (await client.call_tool("supply_demand", {"market": "cbot"})).structured_content
    assert nigeria["state"] == "absent" and "no Nigerian meal or oil" in nigeria["reason"]
    assert crush["state"] == "empty" and "no named-contract curve" in crush["reason"]
    assert crush["meta"]["price_type"] == semantics.PriceType.DELAYED_CLOSE.value
    assert sd["state"] == "empty" and "balance sheet" in sd["reason"]


async def test_spread_carries_basis_and_ledger_as_named_parts(settings, market_db):
    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        body = (await client.call_tool("spread", {"market": "cbot"})).structured_content
    assert set(body["data"]) == {"basis", "ledger"}
    assert body["data"]["basis"]["state"] == "empty"
    assert "US Gulf CIF over CBOT" in body["data"]["basis"]["reason"]
    assert body["data"]["ledger"]["state"] == "ok"  # the board leg printed; nothing else did
    assert body["state"] == "ok" and body["meta"]["as_of"] == "2026-11-04"


async def test_weather_block_round_trips(settings, market_db):
    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        body = (await client.call_tool("weather", {"market": "cbot"})).structured_content
    assert body["state"] == "ok"
    assert {"regions", "alerts", "withheld", "rivers"} <= set(body["data"])


# -- origins, river, provenance, check_language, health, question log ----------

async def test_origins_is_the_public_view_and_never_a_desk_record(settings, market_db):
    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        body = (await client.call_tool("origins")).structured_content
    assert body["state"] in ("ok", "empty")
    assert body["meta"]["audience"] == "public"
    if body["state"] == "ok":
        assert {"destination", "windows", "views", "rankings"} <= set(body["data"])


async def test_origins_rejects_an_unknown_destination(settings, market_db):
    from fastmcp.exceptions import ToolError

    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        with pytest.raises(ToolError, match="destination"):
            await client.call_tool("origins", {"destination": "atlantis"})


async def test_river_serves_observed_stages_per_gauge_newest_first(settings, db_path):
    conn = sqlite3.connect(db_path)
    conn.executemany(
        "INSERT INTO river_levels(gauge,Date,stage,unit,is_forecast,source) VALUES (?,?,?,?,?,?)",
        [("st_louis", "2026-11-03", 3.1, "ft", 0, "USGS"), ("st_louis", "2026-11-04", 2.9, "ft", 0, "USGS"),
         ("st_louis", "2026-11-05", 2.5, "ft", 1, "NWS"), ("rosario", "2026-11-04", 1.2, "m", 0, "INA")]
        + [("st_louis", f"2026-12-{d:02d}", 9.9, "ft", 1, "NWS") for d in range(1, 29)],  # 28 forecasts
    )
    conn.commit()
    conn.close()
    async with Client(build_server(settings)) as client:
        body = (await client.call_tool("river", {"gauge": "st_louis", "per_gauge": 2})).structured_content
        everything = (await client.call_tool("river")).structured_content
    assert body["state"] == "ok"
    # forecasts are excluded before the per-gauge limit, so 28 forecast rows cannot crowd out the observations
    assert [r["Date"] for r in body["data"]["rows"]] == ["2026-11-04", "2026-11-03"]
    assert body["meta"]["as_of"] == "2026-11-04"
    assert {r["gauge"] for r in everything["data"]["rows"]} == {"st_louis", "rosario"}
    assert all(not r["is_forecast"] for r in everything["data"]["rows"])


async def test_provenance_names_the_layer_its_budget_and_its_freshness(settings):
    from fastmcp.exceptions import ToolError

    async with Client(build_server(settings)) as client:
        body = (await client.call_tool("provenance", {"layer": "prices"})).structured_content
        with pytest.raises(ToolError, match="unknown layer"):
            await client.call_tool("provenance", {"layer": "bloomberg"})
    assert body["state"] == "ok"
    d = body["data"]
    assert d["number"] == "1" and "Yahoo" in d["source"]
    assert d["freshness"]["status"] == "success"
    assert d["budget_days"] == config.freshness_limit_days("prices")
    assert d["markets_quoting_it"] and all("quote_kind" in m for m in d["markets_quoting_it"])


async def test_check_language_reports_forbidden_claims_and_what_is_permitted(settings):
    async with Client(build_server(settings)) as client:
        bad = (await client.call_tool("check_language", {"text": "Soybeans' settlement price was 149 USD/MT, an executable level."})).structured_content
        ok = (await client.call_tool("check_language", {"text": "Soybeans' delayed close was 149 USD/MT on 2026-11-04 (Yahoo)."})).structured_content
    assert bad["state"] == "ok" and bad["data"]["verdict"] == "withheld"
    kinds = {v["kind"] for v in bad["data"]["violations"]}
    assert "settlement" in kinds and "executable" in kinds
    assert ok["state"] == "ok" and ok["data"]["verdict"] == "clean" and ok["data"]["violations"] == []
    assert ok["data"]["permitted_claims"] == []  # nothing is permitted when no price type is given


def test_health_has_the_build_stamp_and_no_prices(settings, market_db):
    from starlette.testclient import TestClient

    from mirror_mcp.server import HEALTH_PATH

    app = build_server(settings, clock=lambda: CLOCK).http_app(path=settings.mcp_path)
    with TestClient(app) as http:
        body = http.get(HEALTH_PATH).json()
        assert http.get("/mcp").status_code == 404
    assert body == {"ok": True, "db_built_at": "2026-11-05T00:00:00+00:00", "latest_session": "2026-11-04", "tool_count": 11}


async def test_question_log_records_the_call_never_the_answer(settings, market_db):
    import json

    from fastmcp.exceptions import ToolError

    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        await client.call_tool("price", {"market": "cbot"})
        await client.call_tool("freshness")
        with pytest.raises(ToolError):
            await client.call_tool("price", {"market": "chicago"})
    lines = [json.loads(line) for line in settings.log_path.read_text().splitlines()]
    assert [(line["tool"], line["status"]) for line in lines] == [("price", "completed"), ("freshness", "completed"), ("price", "failed")]
    assert lines[0]["args"] == {"market": "cbot"}
    from mirror_mcp.question_log import FIELDS

    assert all(set(line) <= FIELDS for line in lines)  # a closed key set: nowhere for an answer to live
    assert "usd_mt" not in settings.log_path.read_text()


async def test_every_tool_payload_passes_the_client_record_guard(settings, market_db):
    """The public privacy guard is run on every payload — a desk key anywhere is a server error, not a leak."""
    from analysis.futures.privacy import assert_no_client_records

    async with Client(build_server(settings, clock=lambda: CLOCK)) as client:
        for name, args in (("price", {"market": "cbot"}), ("freshness", {}), ("origins", {}), ("weather", {"market": "cbot"})):
            assert_no_client_records((await client.call_tool(name, args)).structured_content, where=name)


def test_block_tools_read_only_tables_the_server_may_touch(settings, market_db, opened):
    """What the site builders actually read (sqlite authorizer) ⊆ roster − private − excluded seams."""
    import asyncio

    from mirror_mcp import db
    from tests.sqlite_reads import tables_read

    server = build_server(settings, clock=lambda: CLOCK)
    (conn,) = opened

    async def call(name: str, args: dict):
        async with Client(server) as client:
            return await client.call_tool(name, args)

    for name, args in (("price", {"market": "cbot"}), ("crush", {"market": "dalian"}), ("spread", {"market": "argentina"}),
                       ("weather", {"market": "brazil"}), ("supply_demand", {"market": "india"}), ("origins", {})):
        _, seen = tables_read(conn, lambda name=name, args=args: asyncio.run(call(name, args)))
        assert seen, name
        assert seen <= db.readable_tables(), (name, seen - db.readable_tables())
