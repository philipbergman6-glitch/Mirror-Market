"""The pre-publish privacy check for the MCP edition (#580): each planted leak fails with its own reason."""

from __future__ import annotations

import importlib.util
import sqlite3
from pathlib import Path

import pytest

import config

_SPEC = importlib.util.spec_from_file_location(
    "mcp_privacy_check", Path(__file__).resolve().parents[1] / "scripts" / "mcp_privacy_check.py"
)
assert _SPEC is not None and _SPEC.loader is not None
mcp_privacy_check = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(mcp_privacy_check)

PUBLIC_ID = "public-set-abc"


@pytest.fixture
def clean_db(tmp_db: sqlite3.Connection, tmp_path: Path) -> Path:
    tmp_db.execute(
        "INSERT INTO prices(commodity,Date,Open,High,Low,Close,Volume) VALUES ('Soybeans','2026-11-04',1,1,1,1,1)"
    )
    tmp_db.execute(
        "INSERT INTO origin_rankings(run_date,destination,window_start,window_end,origin,rank,comparability,"
        "confidence,freshness,shipment_window,incoterm,quote_kind,method_version,assumption_set_id,input_digest,"
        "snapshot_json,generated_at) VALUES ('2026-11-04','cn_north','2026-12-01','2026-12-31','us_gulf',1,'ok',"
        "'indicative','fresh','Dec','CIF','physical','v1',?, 'd','{}','2026-11-04T00:00:00+00:00')",
        (PUBLIC_ID,),
    )
    tmp_db.commit()
    return tmp_path / "test.db"


def test_a_clean_public_db_passes_every_check(clean_db):
    passed = mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)
    assert [line.split(":")[0] for line in passed] == ["roster", "assumptions", "client records"]


def test_a_table_outside_the_roster_fails_first(clean_db):
    conn = sqlite3.connect(clean_db)
    conn.execute("CREATE TABLE positions(x)")
    conn.commit()
    conn.close()
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match="outside the schema roster.*positions"):
        mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)


def test_a_private_layer_table_fails(clean_db, monkeypatch):
    monkeypatch.setattr(config, "PRIVATE_LAYERS", ("sea_india",))
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match="private-layer tables present.*sea_india_rates"):
        mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)


def test_a_ranking_on_a_foreign_assumption_set_fails(clean_db):
    conn = sqlite3.connect(clean_db)
    conn.execute("UPDATE origin_rankings SET assumption_set_id = 'desk-private-set'")
    conn.commit()
    conn.close()
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match="not the public tier's.*desk-private-set"):
        mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)


def test_a_client_record_key_inside_a_stored_json_blob_fails(clean_db):
    import json

    from analysis.futures.privacy import CLIENT_RECORD_FIELDS

    field = sorted(CLIENT_RECORD_FIELDS)[0]
    conn = sqlite3.connect(clean_db)
    conn.execute(
        "INSERT INTO briefings(briefing_date,text,signals_json,snapshot_json,generated_at) VALUES (?,?,?,?,?)",
        ("2026-11-04", "x", "{}", json.dumps({"snapshot": {field: 1}}), "2026-11-04T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match=f"client-record guard.*{field}"):
        mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)


def test_a_record_directory_named_in_a_served_row_fails(clean_db):
    conn = sqlite3.connect(clean_db)
    conn.execute(
        "INSERT INTO briefings(briefing_date,text,signals_json,snapshot_json,generated_at) VALUES (?,?,?,?,?)",
        ("2026-11-04", "struck from data/reference/positions/book.csv", "{}", "{}", "2026-11-04T00:00:00+00:00"),
    )
    conn.commit()
    conn.close()
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match="client-record guard.*record directory"):
        mcp_privacy_check.check(clean_db, public_set_id=PUBLIC_ID)


def test_the_public_tier_id_is_read_from_the_checkout_when_not_given(clean_db):
    from analysis.futures.privacy import AUDIENCE_PUBLIC
    from analysis.origins.assumptions import load_assumptions

    conn = sqlite3.connect(clean_db)
    conn.execute("UPDATE origin_rankings SET assumption_set_id = ?", (load_assumptions(audience=AUDIENCE_PUBLIC).set_id,))
    conn.commit()
    conn.close()
    assert mcp_privacy_check.check(clean_db)[1].startswith("assumptions")


def test_missing_db_is_a_failure_not_a_pass(tmp_path):
    with pytest.raises(mcp_privacy_check.PrivacyCheckFailed, match="no database"):
        mcp_privacy_check.check(tmp_path / "nope.db", public_set_id=PUBLIC_ID)
