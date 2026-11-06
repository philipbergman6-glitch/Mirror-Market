"""Layer 37 licence gate: Euronext delayed data is free for internal use only.

config.MATIF_PUBLISH is False and every clause of the gate is pinned here:
the table never round-trips through data/history/, no site builder reads it,
the private print can only land outside docs/, the layer runs only where the
desk flag is set, and the MCP server refuses a database holding its rows.
"""

from __future__ import annotations

import re
import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

import config
import main
from analysis import matif_curve
from analysis.futures.privacy import ClientDataLeak
from pipeline import history, query, reliability, schema, store
from pipeline.clean import clean_matif
from pipeline.layer_runs import LayerRun
from pipeline.results import FetchResult

ROOT = Path(__file__).resolve().parents[1]
TABLE = "matif_futures"
READERS = ("read_matif_futures", "read_matif_instruments", TABLE)


def test_the_flag_is_off():
    assert config.MATIF_PUBLISH is False


def test_the_table_is_not_in_the_history_export():
    assert TABLE not in history.HISTORY_TABLES
    assert TABLE in {
        re.search(r"CREATE TABLE IF NOT EXISTS (\w+)", ddl).group(1)  # type: ignore[union-attr]
        for ddl in schema.ALL_SCHEMAS
    }


def _source_files() -> list[Path]:
    files = sorted((ROOT / "app").rglob("*.py"))
    files += [ROOT / "scripts" / "generate_site.py", ROOT / "scripts" / "generate_html.py"]
    files += sorted((ROOT / "analysis").rglob("*.py"))
    return [f for f in files if f.exists() and f.name != "matif_curve.py"]


def test_no_site_builder_or_analysis_reads_the_table():
    offenders = [
        f.relative_to(ROOT).as_posix()
        for f in _source_files()
        if any(reader in f.read_text(encoding="utf-8") for reader in READERS)
    ]
    assert not offenders, f"MATIF_PUBLISH is False but these read Layer 37: {offenders}"


def test_the_layer_is_private_and_desk_gated():
    desc = {row[0]: row[-1] for row in config.PRODUCTION_LAYERS}["matif"]
    assert "(private)" in desc
    assert "matif" in config.PRIVATE_LAYERS
    assert config.API_KEY_CATALOG[config.MATIF_FETCH_ENV].skips == ("matif",)
    assert config.API_KEY_CATALOG[config.MATIF_FETCH_ENV].required_in_ci is False
    assert main.SCRAPER_LAYER_RUN_IF["matif"].__module__ == "fetchers.matif"


def test_the_mcp_never_serves_it_and_refuses_a_database_holding_rows(tmp_db, tmp_path):
    from mirror_mcp import db

    assert db.PRIVATE_LAYER_TABLES["matif"] == TABLE
    assert TABLE not in db.servable_tables()
    db.assert_public_roster(tmp_db)  # the empty, schema-created table is DDL, not a leak
    tmp_db.execute(
        f"INSERT INTO {TABLE}(Date, contract_month, isin, firds_name, last) VALUES ('2026-11-04','2027-02','X','Y',470)"
    )
    tmp_db.commit()
    with pytest.raises(db.PrivacyRefusal, match=TABLE):
        db.assert_public_roster(tmp_db)


def test_the_private_print_refuses_docs(tmp_path):
    target = matif_curve.private_curve_target(tmp_path / "workspace")
    assert target.name == "matif_curve.md" and "docs" not in target.parts
    with pytest.raises(ClientDataLeak):
        matif_curve.private_curve_target(ROOT / "docs")


def test_the_scraper_gate_skips_without_the_flag(monkeypatch, freshness_calls):
    monkeypatch.delenv(config.MATIF_FETCH_ENV, raising=False)
    called = []
    ok = main._run_scraper_layer(
        "matif", "Layer 37", "x", fetch=lambda: called.append(1) or FetchResult.empty(),
        save=lambda n, d: None, run_if=main.SCRAPER_LAYER_RUN_IF["matif"], skip_msg="skipped",
    )
    assert ok is False and not called and not freshness_calls


# ── store, query, clean, curve: the private path works end to end ─────────


def _rows() -> pd.DataFrame:
    return pd.DataFrame([
        {"Date": "2026-11-04", "contract_month": "2027-02", "isin": "FREN27722858",
         "firds_name": "Rapeseed / Colza FUTR PHYS JAN 2027", "expiry_date": "2027-01-29",
         "open": 470.0, "high": 474.5, "low": 470.0, "last": 473.0, "vwap": 472.9, "lots": 22.0,
         "trade_count": 4, "last_trade_at": "2026-11-04T19:14:59Z", "off_book_trades": 1,
         "off_book_lots": 100.0, "cancelled_trades": 0},
        {"Date": "2026-11-04", "contract_month": "2027-05", "isin": "FREX00164092",
         "firds_name": "Rapeseed / Colza FUTR PHYS APR 2027", "expiry_date": "2027-04-30",
         "open": 478.0, "high": 478.0, "low": 478.0, "last": 478.0, "vwap": 478.0, "lots": 1.0,
         "trade_count": 1, "last_trade_at": "2026-11-04T10:00:00Z", "off_book_trades": 0,
         "off_book_lots": 0.0, "cancelled_trades": 1},
        {"Date": "2026-11-03", "contract_month": "2027-08", "isin": "FREX00508066",
         "firds_name": "Rapeseed / Colza FUTR PHYS JUL 2027", "expiry_date": "2027-07-30",
         "open": 480.0, "high": 481.0, "low": 479.0, "last": 480.5, "vwap": 480.2, "lots": 3.0,
         "trade_count": 2, "last_trade_at": "2026-11-03T15:00:00Z", "off_book_trades": 0,
         "off_book_lots": 0.0, "cancelled_trades": 0},
    ])


def test_clean_sorts_types_and_hard_fails_on_a_last_outside_its_range():
    cleaned = clean_matif(_rows())
    assert list(cleaned["Date"].dt.strftime("%Y-%m-%d")) == ["2026-11-03", "2026-11-04", "2026-11-04"]
    bad = _rows()
    bad.loc[0, "last"] = 999.0
    with pytest.raises(ValueError, match="outside"):
        clean_matif(bad)


@pytest.fixture
def patched_store(tmp_path, monkeypatch):
    db_path = tmp_path / "matif.db"
    monkeypatch.setattr(store, "DB_PATH", str(db_path))
    monkeypatch.setattr(query, "DB_PATH", str(db_path))
    import pipeline.connection as connection
    monkeypatch.setattr(connection, "DB_PATH", str(db_path))
    conn = sqlite3.connect(str(db_path))
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.commit()
    conn.close()
    return db_path


def test_store_query_and_instrument_cache_round_trip(patched_store):
    store.save_matif_futures("ECO", clean_matif(_rows()))
    stored = query.read_matif_futures()
    assert len(stored) == 3 and set(stored["quote_kind"]) == {"board_last_traded"}
    assert set(stored["unit"]) == {"EUR/t"}
    feb = query.read_matif_futures("2027-02")
    assert len(feb) == 1 and feb["last"].iloc[0] == 473.0
    known = query.read_matif_instruments()
    assert set(known) == {"FREN27722858", "FREX00164092", "FREX00508066"}
    assert known["FREN27722858"].contract_month == "2027-02"
    assert known["FREN27722858"].expiry_date == date(2027, 1, 29)


def test_store_refuses_a_frame_missing_a_column(patched_store):
    with pytest.raises(ValueError, match="lacks column"):
        store.save_matif_futures("ECO", _rows().drop(columns=["vwap"]))


def test_latest_curve_is_one_session_and_flags_the_older_month(patched_store):
    store.save_matif_futures("ECO", clean_matif(_rows()))
    conn = sqlite3.connect(str(patched_store))
    env = matif_curve.latest_curve(conn, today=date(2026, 11, 6))
    assert env["state"] == "ok"
    data = env["data"]
    assert data["curve_session"] == "2026-11-04" and data["age_days"] == 2
    assert data["unit"] == "EUR/t" and "USD" in data["unit_note"]
    by_month = {m["contract_month"]: m for m in data["months"]}
    assert not by_month["2027-02"]["stale"] and by_month["2027-08"]["stale"]
    assert data["calendar_spreads"] == [
        {"near": "2027-02", "far": "2027-05", "far_minus_near": 5.0, "session": "2026-11-04"},
    ]
    text = matif_curve.render_markdown(env)
    assert "last on-book trade" in text and "settlement" not in text.lower().replace("not a settlement", "")
    assert "PRIVATE" in text


def test_latest_curve_withholds_on_an_empty_table(patched_store):
    env = matif_curve.latest_curve(sqlite3.connect(str(patched_store)), today=date(2026, 11, 6))
    assert env["state"] == "empty" and env["reason"]
    assert matif_curve.render_markdown(env).startswith("MATIF ECO: withheld")


def test_latest_curve_refuses_to_run_if_the_flag_were_flipped(monkeypatch, patched_store):
    monkeypatch.setattr(config, "MATIF_PUBLISH", True)
    env = matif_curve.latest_curve(sqlite3.connect(str(patched_store)), today=date(2026, 11, 6))
    assert env["state"] == "withheld" and "MATIF_PUBLISH" in env["reason"]


def test_main_writes_the_private_markdown(patched_store, tmp_path, monkeypatch, capsys):
    store.save_matif_futures("ECO", clean_matif(_rows()))
    monkeypatch.setattr(matif_curve, "private_output_dir", lambda: tmp_path / "workspace")
    assert matif_curve.main(["--write"]) == 0
    written = tmp_path / "workspace" / "matif_curve.md"
    assert written.exists() and "2027-02" in written.read_text()
    assert "written:" in capsys.readouterr().out


# ── a CI that never asks is not a dead source ─────────────────────────────


def _run(layer: str, session: date, status: str) -> LayerRun:
    stamp = f"{session.isoformat()}T20:00:00+00:00"
    return LayerRun(run_id=f"r{session.isoformat()}", workflow="deploy", run_started_at=stamp,
                    session_date=session, layer_name=layer, status=status, observed_at=None, rendered_at=None)


def test_a_streak_of_skipped_unconfigured_is_not_a_dead_source():
    days = [date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4), date(2026, 11, 5)]
    rows = [_run("matif", d, "skipped_unconfigured") for d in days]
    assert reliability.dead_layers(rows, layers=("matif",)) == {}
    # One real verdict inside the streak and the whole streak counts.
    rows[-1] = _run("matif", days[-1], "failed")
    assert set(reliability.dead_layers(rows, layers=("matif",))) == {"matif"}
