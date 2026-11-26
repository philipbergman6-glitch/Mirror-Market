"""#567 / #659 — what the public may see of a private layer.

A private layer (``config.PRIVATE_LAYERS``) never runs in CI, so it has no
``data_freshness`` row there. The run record files it as
``skipped_unconfigured``; the manifest and the Layer Freshness table used to
call it ``not-run``, and the masthead then listed it under "Stale:" every
day. The public surfaces now show the record's own state and leave the layer
out of the masthead tally exactly as ``disabled`` is.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import config
from scripts import generate_html

PRIVATE = "matif"
assert PRIVATE in config.PRIVATE_LAYERS


def _freshness(**layers: str) -> pd.DataFrame:
    """A freshness frame with one fresh ``success`` row per named public layer."""
    stamp = pd.Timestamp(datetime.now(timezone.utc) - timedelta(hours=2)).tz_localize(None)
    return pd.DataFrame([
        {"layer_name": name, "status": status, "last_success": stamp, "last_attempt": stamp,
         "observed_at": stamp, "rows_fetched": 10}
        for name, status in layers.items()
    ])


@pytest.fixture
def no_private_row(monkeypatch: pytest.MonkeyPatch) -> pd.DataFrame:
    """Every public layer fresh, the private layer absent — CI's shape."""
    frame = _freshness(**{k: "success" for k in config.PRODUCTION_LAYER_KEYS if k != PRIVATE})
    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: frame)
    return frame


def test_manifest_shows_a_private_layer_without_a_row_as_skipped_unconfigured(no_private_row):
    from app.manifest import _coverage

    row = {r["layer"]: r for r in _coverage()}[PRIVATE]
    assert row == {"layer": PRIVATE, "status": "skipped_unconfigured", "observed_at": None,
                   "last_success": None, "rows_fetched": None}


def test_a_public_layer_without_a_row_is_still_not_run(monkeypatch):
    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: _freshness(prices="success"))
    from app.manifest import _coverage

    assert {r["layer"]: r for r in _coverage()}["fred"]["status"] == "not-run"
    items = {i["name"]: i for i in generate_html._build_freshness_items()}
    assert items["fred"]["status"] == "not-run"


def test_freshness_table_shows_the_record_state_and_withholds_every_date(no_private_row):
    item = {i["name"]: i for i in generate_html._build_freshness_items()}[PRIVATE]
    assert item["status"] == "skipped_unconfigured"
    assert item["age"] == "private · desk only"
    assert (item["observed"], item["fetched"], item["data_age"], item["coverage"]) == (None,) * 4


def test_masthead_leaves_a_private_layer_out_of_both_counts(no_private_row):
    items = generate_html._build_freshness_items()
    masthead = generate_html._build_masthead(items, datetime.now(timezone.utc), health={"issues": []})

    public = len(config.PRODUCTION_LAYER_KEYS) - len(config.PRIVATE_LAYERS)
    assert masthead["total_layers"] == public
    assert masthead["on_schedule_count"] == public
    assert PRIVATE not in {i["name"] for i in masthead["late_layers"]}


def test_a_desk_row_for_a_private_layer_shows_its_status_and_no_dates(monkeypatch):
    """On the desk (MATIF_PRIVATE_FETCH set) the layer has a dated row; the
    local preview of the public site still shows its status only."""
    frame = _freshness(prices="success", **{PRIVATE: "success"})
    import pipeline.query as query
    monkeypatch.setattr(query, "read_freshness", lambda: frame)
    from app.manifest import _coverage

    row = {r["layer"]: r for r in _coverage()}[PRIVATE]
    assert row == {"layer": PRIVATE, "status": "success", "observed_at": None,
                   "last_success": None, "rows_fetched": None}
    items = generate_html._build_freshness_items()
    item = {i["name"]: i for i in items}[PRIVATE]
    assert (item["status"], item["age"], item["observed"], item["fetched"]) == (
        "success", "private · desk only", None, None)
    masthead = generate_html._build_masthead(items, datetime.now(timezone.utc), health={"issues": []})
    assert PRIVATE not in {i["name"] for i in masthead["late_layers"]}
    assert masthead["total_layers"] == len(config.PRODUCTION_LAYER_KEYS) - len(config.PRIVATE_LAYERS)


def test_a_health_critical_never_drags_a_private_layer_back_into_the_note(no_private_row, monkeypatch):
    monkeypatch.setitem(config.HEALTH_TABLE_WRITER_LAYERS, "matif_trades", (PRIVATE,))
    monkeypatch.setattr(generate_html, "HEALTH_TABLE_WRITER_LAYERS", config.HEALTH_TABLE_WRITER_LAYERS)
    items = generate_html._build_freshness_items()
    critical = {"severity": "critical", "table": "matif_trades", "message": "MISSING"}
    masthead = generate_html._build_masthead(items, datetime.now(timezone.utc), health={"issues": [critical]})

    item = {i["name"]: i for i in items}[PRIVATE]
    assert item["status"] == "skipped_unconfigured"
    assert item["age"] == "private · desk only"
    assert PRIVATE not in {i["name"] for i in masthead["late_layers"]}
