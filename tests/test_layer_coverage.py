"""#182: key coverage is recorded on every freshness write, and read.

`rows_fetched` existed for months with no consumer, and every failure path
threw the number away at the moment it was known — a partial outage that
returned 3 of 10 keys recorded 0. These tests pin both halves of the fix:

  Record — the finalizer passes the counts it already computed into the
           failure, partial-outage and stale recorders; transport failures
           (no payload at all) record NULL, not a fabricated zero.
  Surface — the briefing freshness block and the dashboard sidebar each
            render coverage, and only when it is below full.

Coverage describes, it never grades: LAYER_MIN_KEYS remains the sole
verdict, so nothing here asserts a status flip.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

import main
from config import (
    LAYER_KEY_CATALOGS,
    LAYER_MAX_DATA_AGE_DAYS,
    LAYER_MIN_KEYS,
    layer_expected_keys,
)
from pipeline import store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def freshness_calls(monkeypatch):
    """Capture every save_freshness call main.py makes, without a DB."""
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success",
                 keys_returned=None, keys_expected=None):
        calls.append({
            "layer": layer_name,
            "rows": rows_fetched,
            "status": status,
            "keys_returned": keys_returned,
            "keys_expected": keys_expected,
        })

    monkeypatch.setattr(main, "save_freshness", _capture)
    main._HARD_FAILURES.clear()
    yield calls
    main._HARD_FAILURES.clear()


def _indexed_frame(days_ago: float, rows: int = 5) -> pd.DataFrame:
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=days_ago)
    dates = pd.date_range(end=end, periods=rows, freq="D")
    return pd.DataFrame({"Close": range(rows)},
                        index=pd.DatetimeIndex(dates, name="Date"))


def _payload(n_keys: int, days_ago: float = 0, rows: int = 5) -> dict:
    return {f"k{i}": _indexed_frame(days_ago, rows) for i in range(n_keys)}


# ---------------------------------------------------------------------------
# The expected-count denominator
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("layer", sorted(set(LAYER_KEY_CATALOGS) & set(LAYER_MIN_KEYS)))
def test_floor_fits_inside_its_catalog(layer):
    """A floor above the catalog size is unreachable — the layer fails forever.

    Two of the LAYER_MIN_KEYS trailing comments had already drifted from the
    catalogs they described (weather "of 18" against 19 regions, fred "of 10"
    against 9 series), which is the argument for deriving the denominator
    rather than hand-keeping a second copy of it.
    """
    assert LAYER_MIN_KEYS[layer] <= len(LAYER_KEY_CATALOGS[layer])


def test_expected_keys_is_none_for_a_layer_with_no_catalog():
    """None → NULL → "coverage undefined". Never a fabricated 1/1."""
    assert layer_expected_keys("agrural") is None
    assert layer_expected_keys("prices") == len(LAYER_KEY_CATALOGS["prices"])


# ---------------------------------------------------------------------------
# Record: the finalizer's failure paths
# ---------------------------------------------------------------------------


def test_partial_outage_records_real_counts_not_zero(freshness_calls):
    """The issue's headline case: 3 of 10 keys used to record 0."""
    below_floor = LAYER_MIN_KEYS["prices"] - 1

    assert main._finalize_layer("prices", _payload(below_floor)) is False

    call = freshness_calls[0]
    assert call["status"] == "failed"
    assert call["keys_returned"] == below_floor
    assert call["keys_expected"] == len(LAYER_KEY_CATALOGS["prices"])
    assert call["rows"] == 5 * below_floor


def test_stale_layer_records_real_counts(freshness_calls):
    """A recency failure ran against a full payload — it knows its counts."""
    stale_by = LAYER_MAX_DATA_AGE_DAYS["prices"] + 1
    keys = LAYER_MIN_KEYS["prices"]

    assert main._finalize_layer("prices", _payload(keys, days_ago=stale_by)) is False

    call = freshness_calls[0]
    assert call["status"] == "failed"
    assert call["keys_returned"] == keys
    assert call["keys_expected"] == len(LAYER_KEY_CATALOGS["prices"])
    assert call["rows"] == 5 * keys


def test_undatable_payload_records_real_counts(freshness_calls):
    """The other recency failure: rows arrived, no date column anywhere."""
    payload = {
        f"k{i}": pd.DataFrame({"value": [1, 2, 3]})
        for i in range(LAYER_MIN_KEYS["prices"])
    }

    assert main._finalize_layer("prices", payload) is False

    call = freshness_calls[0]
    assert call["status"] == "failed"
    assert call["keys_returned"] == LAYER_MIN_KEYS["prices"]
    assert call["keys_expected"] == len(LAYER_KEY_CATALOGS["prices"])


def test_all_empty_layer_records_zero_returned_against_its_catalog(freshness_calls):
    """Zero means asked-and-got-nothing — distinct from NULL's never-learned."""
    empty = {f"k{i}": pd.DataFrame() for i in range(3)}

    assert main._finalize_layer("prices", empty) is False

    call = freshness_calls[0]
    assert call["status"] == "failed"
    assert call["keys_returned"] == 0
    assert call["keys_expected"] == len(LAYER_KEY_CATALOGS["prices"])


def test_transport_failure_records_null_coverage(freshness_calls):
    """No payload ever existed, so both key columns must stay NULL."""
    main._mark_failed("prices")

    call = freshness_calls[0]
    assert call["status"] == "failed"
    assert call["keys_returned"] is None
    assert call["keys_expected"] is None
    assert call["rows"] == 0


def test_empty_success_and_disabled_keep_zero_rows_and_null_coverage(freshness_calls):
    """Both are correct at 0 rows; neither has a catalog to be partial against."""
    main._mark_empty("safex")
    main._mark_disabled("cepea")

    for call in freshness_calls:
        assert call["rows"] == 0
        assert call["keys_returned"] is None
        assert call["keys_expected"] is None


def test_full_coverage_success_records_the_full_pair(freshness_calls):
    catalog = len(LAYER_KEY_CATALOGS["prices"])

    assert main._finalize_layer("prices", _payload(catalog)) is True

    call = freshness_calls[0]
    assert call["status"] == "success"
    assert call["keys_returned"] == catalog == call["keys_expected"]


def test_catalogless_layer_records_null_coverage_on_success(freshness_calls):
    """1/1 on a single-key layer is noise both surfaces would have to filter."""
    assert main._finalize_layer("agrural", _payload(1)) is True

    call = freshness_calls[0]
    assert call["keys_returned"] is None
    assert call["keys_expected"] is None


def test_coverage_does_not_grade(freshness_calls):
    """Above the floor but below full coverage is still a success (#182 d1)."""
    keys = LAYER_MIN_KEYS["weather"]
    assert keys < len(LAYER_KEY_CATALOGS["weather"])

    assert main._finalize_layer("weather", _payload(keys)) is True
    assert freshness_calls[0]["status"] == "success"
    assert "weather" not in main._HARD_FAILURES


# ---------------------------------------------------------------------------
# Record: the storage layer
# ---------------------------------------------------------------------------


def test_save_freshness_persists_coverage(patched_db):
    store.save_freshness("weather", rows_fetched=140, status="success",
                         keys_returned=14, keys_expected=19)

    conn = sqlite3.connect(str(patched_db))
    try:
        row = conn.execute(
            "SELECT rows_fetched, keys_returned, keys_expected FROM data_freshness "
            "WHERE layer_name = 'weather'"
        ).fetchone()
    finally:
        conn.close()
    assert row == (140, 14, 19)


def test_save_freshness_defaults_coverage_to_null(patched_db):
    store.save_freshness("agrural", rows_fetched=1, status="success")

    conn = sqlite3.connect(str(patched_db))
    try:
        row = conn.execute(
            "SELECT keys_returned, keys_expected FROM data_freshness "
            "WHERE layer_name = 'agrural'"
        ).fetchone()
    finally:
        conn.close()
    assert row == (None, None)


def test_migration_widens_a_pre_existing_narrow_table(tmp_path):
    """CREATE TABLE IF NOT EXISTS never widens — a local DB predating #182
    keeps the narrow shape unless the column migration adds both columns."""
    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(str(db))
    conn.execute(
        "CREATE TABLE data_freshness ("
        " layer_name TEXT NOT NULL PRIMARY KEY, last_success TEXT,"
        " last_attempt TEXT, rows_fetched INTEGER, status TEXT NOT NULL DEFAULT 'success')"
    )
    conn.commit()

    store._migrate_data_freshness(conn)

    cols = {r[1] for r in conn.execute("PRAGMA table_info(data_freshness)").fetchall()}
    conn.close()
    assert {"keys_returned", "keys_expected"} <= cols


# ---------------------------------------------------------------------------
# Surface: the briefing
# ---------------------------------------------------------------------------


def _fresh_row(layer: str, status: str, returned, expected) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return {
        "layer_name": layer,
        "last_success": now - timedelta(hours=1),
        "last_attempt": now,
        "rows_fetched": 100,
        "status": status,
        "keys_returned": returned,
        "keys_expected": expected,
    }


@pytest.fixture
def briefing_lines(monkeypatch):
    from analysis.briefing.sections import freshness as section

    def _run(rows: list[dict]) -> list[str]:
        frame = pd.DataFrame(rows)
        for col in ("last_success", "last_attempt"):
            frame[col] = pd.to_datetime(frame[col])
        monkeypatch.setattr(section, "read_freshness", lambda: frame)
        monkeypatch.setattr(
            "analysis.health.run_health_check",
            lambda: {"issues": [], "summary": "", "commodity_status": []},
        )
        return [ln for ln in section.format().splitlines()
                if "WARNING:" in ln or "NOTE:" in ln]

    return _run


def test_briefing_notes_degraded_coverage_on_a_passing_layer(briefing_lines):
    lines = briefing_lines([_fresh_row("weather", "success", 14, 19)])

    assert any("NOTE:" in ln and "weather" in ln and "14 of 19" in ln
               for ln in lines), lines


def test_briefing_warns_when_the_degraded_layer_also_failed(briefing_lines):
    lines = briefing_lines([_fresh_row("prices", "failed", 3, 10)])

    coverage = [ln for ln in lines if "3 of 10" in ln]
    assert len(coverage) == 1, lines
    assert "WARNING:" in coverage[0]


def test_briefing_is_silent_at_full_coverage(briefing_lines):
    assert briefing_lines([_fresh_row("prices", "success", 10, 10)]) == []


def test_briefing_is_silent_when_coverage_is_null(briefing_lines):
    assert briefing_lines([_fresh_row("agrural", "success", None, None)]) == []


# ---------------------------------------------------------------------------
# Surface: the dashboard sidebar
# ---------------------------------------------------------------------------


def _items(rows: list[dict], monkeypatch) -> list[dict]:
    import scripts.generate_html as gh

    frame = pd.DataFrame(rows)
    for col in ("last_success", "last_attempt"):
        frame[col] = pd.to_datetime(frame[col], utc=True)
    monkeypatch.setattr("pipeline.query.read_freshness", lambda: frame)
    return gh._build_freshness_items()


def test_sidebar_renders_coverage_only_below_full(monkeypatch):
    items = _items(
        [
            _fresh_row("weather", "success", 14, 19),
            _fresh_row("prices", "success", 10, 10),
            _fresh_row("agrural", "success", None, None),
        ],
        monkeypatch,
    )

    by_name = {i["name"]: i for i in items}
    assert by_name["weather"]["coverage"] == "14/19"
    assert by_name["prices"]["coverage"] is None
    assert by_name["agrural"]["coverage"] is None


def test_rendered_sidebar_shows_the_degraded_layer_and_only_it(monkeypatch):
    """The acceptance criterion itself, on the real template: a layer below
    full coverage is distinguishable from one at full coverage."""
    from jinja2 import Environment, FileSystemLoader

    import scripts.generate_html as gh

    items = _items(
        [_fresh_row("weather", "success", 14, 19),
         _fresh_row("prices", "success", 10, 10)],
        monkeypatch,
    )
    html = Environment(
        loader=FileSystemLoader(str(gh.TEMPLATE_DIR)), autoescape=False,
    ).get_template("dashboard.html.j2").render(
        sections=[],
        generated_at="2026-08-11 12:00 UTC",
        masthead=gh._build_masthead(items, datetime.now(timezone.utc)),
        freshness_items=items,
    )

    assert "14/19" in html
    assert "10/10" not in html


def test_sidebar_coverage_is_its_own_field_not_the_age_string(monkeypatch):
    """The age string already carries status prose; overloading it further is
    how the per-layer-cadence bug (#176) arose."""
    item = _items([_fresh_row("weather", "success", 14, 19)], monkeypatch)[0]

    assert "14/19" not in item["age"]
