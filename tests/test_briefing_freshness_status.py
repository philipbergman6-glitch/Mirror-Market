"""T12 (issue #60): briefing freshness must honor the `status` column.

`data_freshness.status` is one of 'success' | 'failed' | 'disabled'
(`pipeline.store.save_freshness`). A 'failed' row preserves the previous
`last_success`, so a layer whose fetch died today still looks fresh if you
only read the timestamp — the briefing said nothing while the dashboard
already flagged it. A 'disabled' row is the mirror image: not an outage,
so it must not produce a daily warning.

These tests pin parity with `scripts.generate_html._build_freshness_items`.
No DB, no network — `read_freshness` is monkeypatched at the section module
(the section binds it at import time, so patching `pipeline.query` is a no-op).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from analysis.briefing.sections import freshness as freshness_section


def _freshness_frame(rows: list[dict]) -> pd.DataFrame:
    """Build a data_freshness-shaped frame with naive-UTC datetime columns."""
    df = pd.DataFrame(rows)
    for col in ("last_success", "last_attempt"):
        df[col] = pd.to_datetime(df[col])
    return df


def _row(layer: str, status: str, success_age_days: float | None) -> dict:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    last_success = (
        None if success_age_days is None else now - timedelta(days=success_age_days)
    )
    return {
        "layer_name": layer,
        "last_success": last_success,
        "last_attempt": now,
        "rows_fetched": 0 if status != "success" else 100,
        "status": status,
    }


@pytest.fixture
def briefing_warnings(monkeypatch):
    """Return a callable: rows -> the freshness section's warning lines."""

    def _run(rows: list[dict]) -> list[str]:
        monkeypatch.setattr(
            freshness_section, "read_freshness", lambda: _freshness_frame(rows)
        )
        # The health check hits the real DB; it is a separate concern (T9/T11)
        # and would make this test environment-dependent.
        monkeypatch.setattr(
            "analysis.health.run_health_check",
            lambda: {"issues": [], "summary": "", "commodity_status": []},
        )
        text = freshness_section.format()
        return [ln for ln in text.splitlines() if "WARNING:" in ln]

    return _run


def test_failed_layer_warns_even_when_last_success_is_recent(briefing_warnings):
    """The core T12 case: fresh timestamp, dead layer.

    `save_freshness(..., status='failed')` carries the prior `last_success`
    forward, so an age-only check sees 0 days and stays silent. The briefing
    must warn anyway — the reader is looking at yesterday's numbers.
    """
    warnings = briefing_warnings([_row("prices", "failed", success_age_days=0.2)])

    assert len(warnings) == 1, warnings
    assert "prices" in warnings[0]
    assert "FAILED" in warnings[0]


def test_failed_layer_reports_age_of_last_good_fetch(briefing_warnings):
    """Parity with the dashboard's 'failed · last good Nd ago' label."""
    warnings = briefing_warnings([_row("agrural", "failed", success_age_days=3.4)])

    assert "3 days ago" in warnings[0], warnings


def test_failed_layer_that_never_succeeded_says_so(briefing_warnings):
    """No prior success => there is no 'older data' to be showing.

    The dashboard renders 'failed · never succeeded' for this row; claiming
    stale data exists would send the reader looking for numbers that are not
    in the DB at all.
    """
    warnings = briefing_warnings([_row("magyp_fob", "failed", success_age_days=None)])

    assert len(warnings) == 1, warnings
    assert "no recorded success" in warnings[0]
    assert "days ago" not in warnings[0]


def test_disabled_layer_produces_no_warning(briefing_warnings):
    """A deliberately switched-off layer is not an outage.

    `_mark_disabled()` in main.py writes status='disabled' without stamping
    `last_success`, so the row's timestamp goes arbitrarily stale. The
    dashboard buckets it as 'disabled' and drops it from the fresh/stale
    counts; before this fix the briefing warned about it every single day.
    """
    warnings = briefing_warnings([_row("cepea", "disabled", success_age_days=400)])

    assert warnings == []


def test_disabled_layer_with_no_success_row_is_also_silent(briefing_warnings):
    warnings = briefing_warnings([_row("cepea", "disabled", success_age_days=None)])

    assert warnings == []


def test_successful_recent_layer_produces_no_warning(briefing_warnings):
    warnings = briefing_warnings([_row("prices", "success", success_age_days=0.5)])

    assert warnings == []


def test_successful_but_stale_layer_still_warns_on_age(briefing_warnings):
    """The pre-existing age path must survive the status branching."""
    warnings = briefing_warnings([_row("prices", "success", success_age_days=9)])

    assert len(warnings) == 1, warnings
    assert "9 days old" in warnings[0]


def test_weekly_layer_keeps_its_own_cadence_threshold(briefing_warnings):
    """COT is a Friday release — 9 days old is normal, not an outage."""
    from config import FRESHNESS_WARNING_DAYS, FRESHNESS_WARNING_DAYS_BY_LAYER

    assert FRESHNESS_WARNING_DAYS_BY_LAYER["cot"] > FRESHNESS_WARNING_DAYS

    assert briefing_warnings([_row("cot", "success", success_age_days=9)]) == []
    assert briefing_warnings([_row("cot", "success", success_age_days=20)]) != []


def test_success_status_with_no_last_success_warns(briefing_warnings):
    """A layer with neither a success nor a failure on record is not 'fine'."""
    warnings = briefing_warnings([_row("psd", "success", success_age_days=None)])

    assert len(warnings) == 1, warnings
    assert "never fetched successfully" in warnings[0]


def test_legacy_row_without_status_column_falls_back_to_age_check(monkeypatch):
    """Rows written before the status column existed read as NaN, not str.

    `read_freshness` backfills 'success', but the section must not crash if a
    frame reaches it with a null status.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    df = pd.DataFrame(
        {
            "layer_name": ["prices"],
            "last_success": [now - timedelta(days=30)],
            "last_attempt": [now],
            "rows_fetched": [100],
            "status": [None],
        }
    )
    monkeypatch.setattr(freshness_section, "read_freshness", lambda: df)
    monkeypatch.setattr(
        "analysis.health.run_health_check",
        lambda: {"issues": [], "summary": "", "commodity_status": []},
    )

    warnings = [
        ln for ln in freshness_section.format().splitlines() if "WARNING:" in ln
    ]

    assert len(warnings) == 1, warnings
    assert "30 days old" in warnings[0]


@pytest.fixture
def dashboard_items(monkeypatch):
    """Return a callable: rows -> {layer: status} from the dashboard path."""

    def _run(rows: list[dict]) -> dict[str, str]:
        import pipeline.query as query
        from scripts import generate_html

        df = _freshness_frame(rows)
        monkeypatch.setattr(query, "read_freshness", lambda: df)
        return {i["name"]: i["status"] for i in generate_html._build_freshness_items()}

    return _run


def test_dashboard_buckets_use_the_layers_own_cadence(dashboard_items):
    """Issue #176: the sidebar bucketed every layer on a hardcoded 7 days.

    COT publishes Friday for the previous Tuesday, so 5 days old is its
    normal state, and the monthlies (42d) sat permanently in `old`. A badge
    that is negative on a healthy layer every day is why the reader stops
    reading badges.
    """
    statuses = dashboard_items(
        [
            _row("cot", "success", success_age_days=5),      # inside 12d
            _row("safex", "success", success_age_days=20),  # no entry => 7d
            _row("wasde", "success", success_age_days=20),   # inside 42d
            _row("usda", "success", success_age_days=200),   # inside 400d
            _row("prices", "success", success_age_days=3),   # inside default 7d
            _row("agrural", "success", success_age_days=9),  # past default 7d
        ]
    )

    assert statuses["cot"] == "stale"
    assert statuses["wasde"] == "stale"
    assert statuses["usda"] == "stale"
    assert statuses["prices"] == "stale"
    assert statuses["safex"] == "old"
    assert statuses["agrural"] == "old"


def test_dashboard_keeps_the_sub_day_fresh_boundary(dashboard_items):
    """Cadence moves the stale/old cutoff only.

    A daily layer fetched 3h ago and one fetched 20h ago are both `fresh`;
    a weekly layer at 2 days is not, even though it is well inside COT's
    12-day window. "Ran today" stays a distinct claim from "within cadence".
    """
    statuses = dashboard_items(
        [
            _row("prices", "success", success_age_days=0.1),
            _row("fred", "success", success_age_days=0.8),
            _row("cot", "success", success_age_days=2),
        ]
    )

    assert statuses["prices"] == "fresh"
    assert statuses["fred"] == "fresh"
    assert statuses["cot"] == "stale"


def test_status_buckets_match_the_dashboard_path(monkeypatch):
    """Cross-check: the same frame through both renderers must agree on which
    layers are problems and which are silent.

    This is the acceptance criterion for #60 — 'parity with the dashboard
    freshness path'. Wording differs by surface; the verdict must not.

    "Problem" on the dashboard is `old` (or `degraded`, which only the health
    path sets): `stale` means inside the layer's publication cadence but not
    from today, which the briefing deliberately says nothing about. The
    original fixtures (0.1d, 0.1d, 100d, 400d-disabled) all sat outside the
    band where the two renderers could disagree, so the equality held by
    selection — #176. These rows sit inside it: cot@5d and wasde@20d are
    silent on both surfaces, safex@20d (no cadence entry) fires on both.
    """
    from scripts import generate_html

    rows = [
        _row("prices", "failed", success_age_days=0.1),
        _row("cepea", "disabled", success_age_days=400),
        _row("fred", "success", success_age_days=0.1),
        _row("psd", "success", success_age_days=100),
        # The previously-untested 1–7d band and the slow-cadence layers.
        _row("agrural", "success", success_age_days=3),
        _row("cot", "success", success_age_days=5),
        _row("wasde", "success", success_age_days=20),
        _row("safex", "success", success_age_days=20),
    ]
    layers = [r["layer_name"] for r in rows]
    df = _freshness_frame(rows)

    monkeypatch.setattr(freshness_section, "read_freshness", lambda: df)
    monkeypatch.setattr(
        "analysis.health.run_health_check",
        lambda: {"issues": [], "summary": "", "commodity_status": []},
    )
    import pipeline.query as query

    monkeypatch.setattr(query, "read_freshness", lambda: df)

    briefing_lines = [
        ln for ln in freshness_section.format().splitlines() if "WARNING:" in ln
    ]
    briefing_flagged = {
        layer for layer in layers if any(layer in ln for ln in briefing_lines)
    }

    items = generate_html._build_freshness_items()
    dashboard_flagged = {
        i["name"] for i in items if i["status"] in ("old", "degraded")
    }

    expected = {"prices", "psd", "safex"}
    assert briefing_flagged == dashboard_flagged == expected
