"""T10 · F13 — the masthead must fold in `analysis.health` criticals.

The masthead used to count only `data_freshness` rows: a layer that ran
and wrote *something* read as fresh even when the health check said its
commodities were MISSING. That made "17/17 layers fresh" renderable on
the same page as a DATA HEALTH section full of criticals.

Invariant under test: if `run_health_check()` reports any critical, the
masthead may never claim every layer is fresh.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone

import pytest
from jinja2 import Environment, FileSystemLoader

from config import HEALTH_TABLE_WRITER_LAYERS
from scripts import generate_html


def _items(*names: str) -> list[dict]:
    return [{"name": n, "status": "fresh", "age": "1h ago"} for n in names]


def _critical(table: str, commodity: str = "Soybeans") -> dict:
    return {
        "severity": "critical",
        "table": table,
        "commodity": commodity,
        "message": f"MISSING from {table} — no observed rows",
    }


NOW = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)


def test_masthead_without_health_criticals_counts_every_fresh_layer() -> None:
    masthead = generate_html._build_masthead(
        _items("prices", "weather"), NOW, health={"issues": []}
    )

    assert masthead["fresh_count"] == 2
    assert masthead["total_layers"] == 2
    assert masthead["critical_count"] == 0
    assert masthead["degraded_layers"] == []


def test_health_critical_demotes_the_layer_that_writes_the_table() -> None:
    """A critical on `weather` must knock the weather layer out of the count."""
    items = _items("prices", "weather")
    masthead = generate_html._build_masthead(
        items, NOW, health={"issues": [_critical("weather", "Mato Grosso")]}
    )

    assert masthead["fresh_count"] == 1
    assert masthead["total_layers"] == 2
    assert masthead["critical_count"] == 1
    assert masthead["degraded_layers"] == ["weather"]
    assert [s["name"] for s in masthead["stale_layers"]] == ["weather"]


def test_demotion_is_reflected_in_the_shared_freshness_items() -> None:
    """The sidebar badge and the masthead count must not disagree."""
    items = _items("prices", "weather")
    generate_html._build_masthead(
        items, NOW, health={"issues": [_critical("weather")]}
    )

    by_name = {i["name"]: i for i in items}
    assert by_name["weather"]["status"] == "degraded"
    assert "data health" in by_name["weather"]["age"]
    assert by_name["prices"]["status"] == "fresh"


def test_warnings_do_not_demote_a_layer() -> None:
    items = _items("prices")
    warning = dict(_critical("prices"), severity="warning")
    masthead = generate_html._build_masthead(items, NOW, health={"issues": [warning]})

    assert masthead["fresh_count"] == 1
    assert masthead["critical_count"] == 0


def test_multi_writer_table_demotes_every_writing_layer() -> None:
    """`brazil_spot_prices` is written by three layers — all lose the badge."""
    items = _items("prices", "cepea", "agrural", "conab_precos")
    masthead = generate_html._build_masthead(
        items, NOW, health={"issues": [_critical("brazil_spot_prices")]}
    )

    assert masthead["fresh_count"] == 1
    assert masthead["degraded_layers"] == ["agrural", "cepea", "conab_precos"]


def test_disabled_layer_is_still_excluded_from_both_counts() -> None:
    items = _items("prices") + [
        {"name": "ncdex", "status": "disabled", "age": "disabled"}
    ]
    masthead = generate_html._build_masthead(items, NOW, health={"issues": []})

    assert masthead["fresh_count"] == 1
    assert masthead["total_layers"] == 1


def test_unattributable_critical_still_breaks_the_all_fresh_claim() -> None:
    """A critical on a table no layer claims (or "all") must not vanish.

    We cannot know which layer to demote, so the count stays put — but
    `critical_count` is non-zero, and the template renders it next to the
    fresh count, so the page never reads as unqualified all-green.
    """
    masthead = generate_html._build_masthead(
        _items("prices"), NOW, health={"issues": [_critical("all", "all")]}
    )

    assert masthead["fresh_count"] == masthead["total_layers"]
    assert masthead["critical_count"] == 1
    assert masthead["unmapped_critical_tables"] == ["all"]


def test_missing_health_payload_leaves_the_count_untouched() -> None:
    """`run_health_check` is wrapped in `_safe_call` and may return None."""
    masthead = generate_html._build_masthead(_items("prices"), NOW, health=None)

    assert masthead["fresh_count"] == 1
    assert masthead["critical_count"] == 0


def test_rendered_masthead_never_claims_all_fresh_while_criticals_exist() -> None:
    """End-to-end on the real template — the acceptance criterion itself."""
    template = Environment(
        loader=FileSystemLoader(str(generate_html.TEMPLATE_DIR)),
        autoescape=False,
    ).get_template("dashboard.html.j2")

    items = _items("prices", "weather")
    masthead = generate_html._build_masthead(
        items, NOW, health={"issues": [_critical("weather")]}
    )
    html = template.render(
        sections=[],
        generated_at="2026-08-11 12:00 UTC",
        masthead=masthead,
        freshness_items=items,
    )

    assert "2/2 layers fresh" not in html
    assert "1/2 layers fresh" in html
    assert "1 data critical" in html


def test_every_table_health_can_flag_maps_to_a_writing_layer(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Guard: a new `_check_*` without a mapping would go unattributed.

    Run the real health check against an empty database — every check
    then emits a critical naming its table, which is the full set of
    tables the masthead has to be able to attribute. Anything missing
    from `HEALTH_TABLE_WRITER_LAYERS` would only ever be counted, never
    demote a layer.
    """
    db_path = tmp_path / "empty.db"
    sqlite3.connect(str(db_path)).close()
    monkeypatch.setattr("analysis.health.get_connection",
                        lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr("analysis.health.DB_PATH", str(db_path))
    monkeypatch.setattr("analysis.health.is_cloud", lambda: False)

    from analysis.health import run_health_check

    tables = {
        issue["table"]
        for issue in run_health_check()["issues"]
        if issue["severity"] == "critical"
    }

    assert tables, "empty DB should produce criticals — fixture is wrong"
    assert tables <= set(HEALTH_TABLE_WRITER_LAYERS), (
        f"unmapped health table(s): {sorted(tables - set(HEALTH_TABLE_WRITER_LAYERS))} "
        "— add them to config.HEALTH_TABLE_WRITER_LAYERS"
    )
