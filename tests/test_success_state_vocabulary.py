"""B3 #309 — the success-state vocabulary A3 #300 decided, enforced.

One axis: ``success | usable_partial | incomplete | stale | failed |
no_publication | disabled``. The gate order is shape → floor → recency →
coverage, deterministic and with no tie-breaks:

  empty                      → failed / no_publication (as before)
  below LAYER_MIN_KEYS       → incomplete (recency not judged)
  above floor, past budget   → stale, even when also partial
  above floor, fresh, full   → success
  above floor, fresh, short  → usable_partial

``usable_partial`` advances ``last_success`` (the data is renderable and
fresh; freezing the clock would poison every staleness surface with a
false outage) and escalates to a *catalog-drift* alert only after
``config.USABLE_PARTIAL_ESCALATION_RUNS`` consecutive runs.

Age budgets flip to universal-by-default: every production layer carries a
``LAYER_MAX_DATA_AGE_DAYS`` entry or a written exemption, and an unlisted
layer is an import-time hard fail.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
from jinja2 import Environment, FileSystemLoader

import config
import main
from pipeline import grading, store

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _frame(days_ago: float, rows: int = 3) -> pd.DataFrame:
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=days_ago)
    dates = pd.date_range(end=end, periods=rows, freq="D")
    return pd.DataFrame({"Close": range(rows)}, index=pd.DatetimeIndex(dates, name="Date"))


def _weather_payload(returned: int, days_ago: float = 0) -> dict:
    names = list(config.LAYER_KEY_CATALOGS["weather"])
    return {name: _frame(days_ago) for name in names[:returned]}


@pytest.fixture
def captured(monkeypatch):
    """Capture main's freshness writes (with the new keyword) and reset its sets."""
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success", keys_returned=None,
                 keys_expected=None, missing_keys=None):
        calls.append({
            "layer": layer_name, "rows": rows_fetched, "status": status,
            "keys_returned": keys_returned, "keys_expected": keys_expected,
            "missing_keys": missing_keys,
        })

    monkeypatch.setattr(main, "save_freshness", _capture)
    sets = (main._HARD_FAILURES, main._STALE_LAST_KNOWN_GOOD,
            main._INCOMPLETE_KEY_COVERAGE, main._USABLE_PARTIAL, main._NO_PUBLICATION)
    for s in sets:
        s.clear()
    yield calls
    for s in sets:
        s.clear()


# ---------------------------------------------------------------------------
# 1. One axis, one word added
# ---------------------------------------------------------------------------


def test_vocabulary_is_the_seven_words_a3_decided():
    assert grading.FRESHNESS_STATUSES == (
        "success", "usable_partial", "incomplete", "stale", "failed",
        "no_publication", "disabled",
    )
    assert frozenset({"success", "usable_partial"}) == grading.ADVANCES_LAST_SUCCESS
    # The outage words a surface warns on — usable_partial is not one of them.
    assert frozenset({"failed", "stale", "incomplete"}) == grading.OUTAGE_STATUSES


# ---------------------------------------------------------------------------
# 2–4. Gate order in _finalize_layer
# ---------------------------------------------------------------------------


def test_full_catalog_and_fresh_is_success(captured):
    catalog = len(config.LAYER_KEY_CATALOGS["weather"])
    ok = main._finalize_layer("weather", _weather_payload(catalog))
    assert ok is True
    row = captured[-1]
    assert row["status"] == "success"
    assert row["missing_keys"] is None
    assert "weather" not in main._USABLE_PARTIAL


def test_one_missing_key_is_no_longer_a_success(captured):
    catalog = len(config.LAYER_KEY_CATALOGS["weather"])
    ok = main._finalize_layer("weather", _weather_payload(catalog - 1))
    row = captured[-1]
    assert ok is True, "usable_partial is renderable: the layer is not a failure"
    assert row["status"] == "usable_partial"
    assert (row["keys_returned"], row["keys_expected"]) == (catalog - 1, catalog)
    assert row["missing_keys"] == [list(config.LAYER_KEY_CATALOGS["weather"])[-1]]
    assert "weather" in main._USABLE_PARTIAL
    assert "weather" not in main._HARD_FAILURES
    assert "weather" not in main._INCOMPLETE_KEY_COVERAGE


def test_below_floor_is_incomplete_and_recency_is_not_judged(captured):
    floor = config.LAYER_MIN_KEYS["weather"]
    budget = config.LAYER_MAX_DATA_AGE_DAYS["weather"]
    # Stale AND below floor: shape wins, the row says incomplete, not stale.
    ok = main._finalize_layer("weather", _weather_payload(floor - 1, days_ago=budget + 30))
    assert ok is False
    assert captured[-1]["status"] == "incomplete"
    assert "weather" in main._INCOMPLETE_KEY_COVERAGE
    assert "weather" not in main._STALE_LAST_KNOWN_GOOD


def test_above_floor_but_stale_is_stale_even_when_also_partial(captured):
    floor = config.LAYER_MIN_KEYS["weather"]
    budget = config.LAYER_MAX_DATA_AGE_DAYS["weather"]
    ok = main._finalize_layer("weather", _weather_payload(floor, days_ago=budget + 30))
    assert ok is False
    row = captured[-1]
    assert row["status"] == "stale"
    # Coverage columns still show the shortfall (A3 §4).
    assert row["keys_returned"] == floor
    assert "weather" in main._STALE_LAST_KNOWN_GOOD
    assert "weather" not in main._USABLE_PARTIAL


def test_a_catalogless_layer_cannot_be_usable_partial(captured):
    # No catalog → nothing to be partial against; a non-empty fresh frame is success.
    ok = main._finalize_layer("worldbank", {"palm": _frame(1)})
    assert ok is True
    assert captured[-1]["status"] == "success"


def test_missing_catalog_keys_names_the_absent_and_empty_keys():
    catalog = {"a": 1, "b": 2, "c": 3}
    data = {"a": _frame(0), "b": pd.DataFrame()}
    assert grading.missing_catalog_keys(catalog, data) == ["b", "c"]
    assert grading.missing_catalog_keys(catalog, {k: _frame(0) for k in catalog}) == []


def test_missing_keys_are_withheld_with_a_reason_when_payload_names_differ():
    catalog = {"a": 1, "b": 2, "c": 3}
    missing = grading.missing_catalog_keys(catalog, {"x": _frame(0), "y": _frame(0)})
    assert len(missing) == 1 and missing[0].startswith("1 key(s) unnamed")
    assert grading.missing_catalog_keys(catalog, {"x": _frame(0), "y": _frame(0), "z": _frame(0)}) == []


# ---------------------------------------------------------------------------
# 3. last_success stamping and the streak
# ---------------------------------------------------------------------------


def _row(db_path, layer="weather"):
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT status, last_success FROM data_freshness WHERE layer_name = ?",
            (layer,),
        ).fetchone()
    finally:
        conn.close()


def test_usable_partial_advances_last_success(patched_db):
    store.save_freshness("weather", 100, status="usable_partial",
                         keys_returned=23, keys_expected=24, missing_keys=["Mato Grosso"])
    status, last_success = _row(patched_db)
    assert status == "usable_partial"
    assert last_success is not None


def test_incomplete_still_preserves_last_success(patched_db):
    store.save_freshness("weather", 100)
    _, first = _row(patched_db)
    store.save_freshness("weather", 10, status="incomplete", keys_returned=3, keys_expected=24)
    status, kept = _row(patched_db)
    assert status == "incomplete"
    assert kept == first


def test_usable_partial_without_missing_keys_is_refused(patched_db):
    with pytest.raises(ValueError, match="missing_keys"):
        store.save_freshness("weather", 100, status="usable_partial",
                             keys_returned=23, keys_expected=24)


def test_missing_keys_on_any_other_status_is_refused(patched_db):
    with pytest.raises(ValueError, match="missing_keys"):
        store.save_freshness("weather", 100, status="success", missing_keys=["x"])


def test_partial_streak_counts_consecutive_runs_and_resets(patched_db):
    for _ in range(2):
        store.save_freshness("weather", 100, status="usable_partial",
                             keys_returned=23, keys_expected=24, missing_keys=["Mato Grosso"])
    streak = grading.read_partial_streaks()["weather"]
    assert streak.consecutive_runs == 2
    assert streak.missing_keys == ["Mato Grosso"]

    store.save_freshness("weather", 100)
    assert "weather" not in grading.read_partial_streaks()

    store.save_freshness("weather", 100, status="usable_partial",
                         keys_returned=23, keys_expected=24, missing_keys=["Paraná"])
    assert grading.read_partial_streaks()["weather"].consecutive_runs == 1


def test_streak_escalation_threshold_is_the_config_number(patched_db):
    n = config.USABLE_PARTIAL_ESCALATION_RUNS
    assert n == 3
    for _ in range(n - 1):
        store.save_freshness("weather", 100, status="usable_partial",
                             keys_returned=23, keys_expected=24, missing_keys=["Mato Grosso"])
    assert grading.catalog_drift_layers() == {}
    store.save_freshness("weather", 100, status="usable_partial",
                         keys_returned=23, keys_expected=24, missing_keys=["Mato Grosso"])
    drift = grading.catalog_drift_layers()
    assert list(drift) == ["weather"]
    assert drift["weather"].missing_keys == ["Mato Grosso"]


def test_streak_table_round_trips_through_history():
    from pipeline import history

    assert "layer_partial_streak" in history.HISTORY_TABLES


# ---------------------------------------------------------------------------
# 6. Alerts: usable_partial is amber, catalog drift is its own issue
# ---------------------------------------------------------------------------


@pytest.fixture
def gh_calls(monkeypatch):
    from scripts import ci_layer_alert as alerter

    calls: list[tuple[str, ...]] = []
    state: dict[str, int | None] = {alerter.ALERT_LABEL: None, alerter.DRIFT_LABEL: None}

    def fake_gh(*args: str) -> str:
        calls.append(args)
        if args[:2] == ("issue", "list"):
            label = args[args.index("--label") + 1]
            n = state[label]
            return json.dumps([{"number": n}] if n else [])
        return ""

    monkeypatch.setattr(alerter, "_gh", fake_gh)
    return calls, state


def _status(tmp_path, **overrides):
    payload = {
        "succeeded": ["prices"],
        "hard_failures": [],
        "critical_failures": [],
        "classifications": {
            "upstream_failure": [], "no_publication": [],
            "stale_last_known_good": [], "incomplete_key_coverage": [],
            "usable_partial": [], "catalog_drift": [],
        },
        "usable_partial_detail": {},
    }
    payload.update(overrides)
    path = tmp_path / "pipeline_status.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_a_first_usable_partial_run_opens_nothing(tmp_path, gh_calls):
    from scripts import ci_layer_alert as alerter

    calls, _ = gh_calls
    path = _status(
        tmp_path,
        classifications={"usable_partial": ["weather"], "catalog_drift": []},
        usable_partial_detail={"weather": {"missing_keys": ["Mato Grosso"], "consecutive_runs": 1}},
    )
    assert alerter.main(["prog", str(path)]) == 0
    assert ("issue", "create") not in [c[:2] for c in calls]


def test_catalog_drift_opens_a_distinct_issue_naming_the_keys(tmp_path, gh_calls):
    from scripts import ci_layer_alert as alerter

    calls, _ = gh_calls
    path = _status(
        tmp_path,
        classifications={"usable_partial": ["weather"], "catalog_drift": ["weather"]},
        usable_partial_detail={"weather": {"missing_keys": ["Mato Grosso"], "consecutive_runs": 3}},
    )
    assert alerter.main(["prog", str(path)]) == 0
    creates = [c for c in calls if c[:2] == ("issue", "create")]
    assert len(creates) == 1
    create = creates[0]
    assert create[create.index("--label") + 1] == alerter.DRIFT_LABEL
    body = create[create.index("--body") + 1]
    assert "Mato Grosso" in body and "weather" in body and "3 consecutive" in body
    # No outage issue was opened for an amber-only run.
    assert all(c[c.index("--label") + 1] != alerter.ALERT_LABEL
               for c in creates if "--label" in c)


def test_a_full_run_closes_an_open_drift_issue(tmp_path, gh_calls):
    from scripts import ci_layer_alert as alerter

    calls, state = gh_calls
    state[alerter.DRIFT_LABEL] = 77
    assert alerter.main(["prog", str(_status(tmp_path))]) == 0
    closes = [c for c in calls if c[:2] == ("issue", "close")]
    assert closes and closes[0][2] == "77"


# ---------------------------------------------------------------------------
# 5. Masthead: green is success only; usable_partial is a distinct amber tally
# ---------------------------------------------------------------------------


def _freshness_frame(rows):
    df = pd.DataFrame(rows)
    for col in ("last_success", "last_attempt"):
        df[col] = pd.to_datetime(df[col])
    return df


def _fresh_row(layer, status, keys=None):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    returned, expected = keys or (None, None)
    return {"layer_name": layer, "last_success": now - timedelta(hours=1), "last_attempt": now,
            "rows_fetched": 100, "status": status,
            "keys_returned": returned, "keys_expected": expected}


def test_dashboard_buckets_usable_partial_as_partial_not_fresh(monkeypatch):
    import pipeline.query as query
    from scripts import generate_html

    df = _freshness_frame([
        _fresh_row("weather", "usable_partial", keys=(23, 24)),
        _fresh_row("prices", "success"),
    ])
    monkeypatch.setattr(query, "read_freshness", lambda: df)
    items = {i["name"]: i for i in generate_html._build_freshness_items()}
    assert items["weather"]["status"] == "partial"
    assert "23/24" in items["weather"]["coverage"]
    assert items["prices"]["status"] == "fresh"


def test_masthead_counts_partial_separately_from_green_and_red():
    from scripts import generate_html

    now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
    items = [
        {"name": "prices", "status": "fresh", "age": "1h ago"},
        {"name": "weather", "status": "partial", "age": "usable partial · 1h ago"},
        {"name": "cot", "status": "old", "age": "upstream failure · last good 3d ago"},
    ]
    masthead = generate_html._build_masthead(items, now, health=None)
    assert masthead["on_schedule_count"] == 1
    assert masthead["total_layers"] == 3
    assert masthead["partial_count"] == 1
    assert [i["name"] for i in masthead["partial_layers"]] == ["weather"]
    assert [i["name"] for i in masthead["late_layers"]] == ["cot"]


def test_rendered_masthead_shows_the_amber_tally(monkeypatch):
    from scripts import generate_html

    now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
    items = [
        {"name": "prices", "status": "fresh", "age": "1h ago"},
        {"name": "weather", "status": "partial", "age": "usable partial · 1h ago"},
    ]
    masthead = generate_html._build_masthead(items, now, health=None)
    template = Environment(
        loader=FileSystemLoader(str(generate_html.TEMPLATE_DIR)), autoescape=True,
    ).get_template("dashboard.html.j2")
    html = template.render(sections=[], generated_at="2026-10-07 12:00 UTC",
                           masthead=masthead, freshness_items=items)
    assert "1/2 layers on schedule" in html
    assert "mast-partial" in html
    assert "1 partial" in html
    # Amber is its own tally: the partial layer is not in the red stale note.
    assert "Stale:" not in html


# ---------------------------------------------------------------------------
# Briefing: a usable_partial layer is a NOTE, never a WARNING
# ---------------------------------------------------------------------------


def test_briefing_notes_a_usable_partial_layer_without_warning(monkeypatch):
    from analysis.briefing.sections import freshness as freshness_section

    df = _freshness_frame([_fresh_row("weather", "usable_partial", keys=(23, 24))])
    monkeypatch.setattr(freshness_section, "read_freshness", lambda: df)
    monkeypatch.setattr("analysis.health.run_health_check",
                        lambda: {"issues": [], "summary": "", "commodity_status": []})
    lines = freshness_section.format().splitlines()
    notes = [ln for ln in lines if "NOTE:" in ln and "weather" in ln]
    warnings = [ln for ln in lines if "WARNING:" in ln and "weather" in ln]
    assert notes and "23 of 24" in notes[0]
    assert "usable partial" in notes[0].lower()
    assert not warnings


# ---------------------------------------------------------------------------
# 7. Age budgets are universal by default
# ---------------------------------------------------------------------------


def test_every_production_layer_is_budgeted_or_exempt():
    for layer in config.PRODUCTION_LAYER_KEYS:
        budgeted = layer in config.LAYER_MAX_DATA_AGE_DAYS
        exempt = layer in config.LAYER_AGE_BUDGET_EXEMPT
        assert budgeted != exempt, f"{layer}: budgeted={budgeted} exempt={exempt}"


def test_every_exemption_carries_a_written_reason():
    for layer, reason in config.LAYER_AGE_BUDGET_EXEMPT.items():
        assert isinstance(reason, str) and len(reason) > 20, layer


def test_the_documented_exemptions_stand_and_forward_curve_is_budgeted():
    assert {"psd", "wasde", "usda", "crop_progress"} <= set(config.LAYER_AGE_BUDGET_EXEMPT)
    assert "forward_curve" not in config.LAYER_AGE_BUDGET_EXEMPT
    assert config.LAYER_MAX_DATA_AGE_DAYS["forward_curve"] == 7


@pytest.mark.parametrize("layer, budget", [
    ("agrural", 7), ("cepea", 7), ("gulf_bids", 7), ("magyp_fob", 14), ("forward_curve", 7),
])
def test_starting_budgets_for_the_named_layers(layer, budget):
    assert config.LAYER_MAX_DATA_AGE_DAYS[layer] == budget


def test_an_unlisted_production_layer_is_a_config_hard_fail():
    with pytest.raises(ValueError, match="unlisted"):
        config.validate_age_budgets(
            production_layers=("prices", "mystery"),
            budgets={"prices": 7},
            exemptions={},
        )


def test_a_layer_both_budgeted_and_exempt_is_a_config_hard_fail():
    with pytest.raises(ValueError, match="both"):
        config.validate_age_budgets(
            production_layers=("prices",),
            budgets={"prices": 7},
            exemptions={"prices": "a reason long enough to count"},
        )


def test_forward_curve_observation_date_now_grades_recency(captured):
    budget = config.LAYER_MAX_DATA_AGE_DAYS["forward_curve"]
    old = (pd.Timestamp.now().normalize() - pd.Timedelta(days=budget + 10)).date().isoformat()
    def _curve(commodity: str) -> pd.DataFrame:
        return pd.DataFrame({
            "commodity": [commodity], "contract_month": ["2027-11-01"],
            "close": [1000.0], "observation_date": [old],
        })

    payload = {name: _curve(name) for name in config.LAYER_KEY_CATALOGS["forward_curve"]}
    ok = main._finalize_layer("forward_curve", payload)
    assert ok is False
    assert captured[-1]["status"] == "stale"


def test_exempt_layer_passes_recency_without_a_date(captured):
    frame = pd.DataFrame({"Market_Year": [2026], "Value": [1.0]})
    assert main._check_layer_recency("psd", {"Soybeans": frame}) is True
