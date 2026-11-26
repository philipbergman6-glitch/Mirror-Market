"""#673 — three decision-readiness indicators beside the masthead layer count.

"37/41 layers on schedule" is a pipeline-health number. A buyer needs three
different answers: can origins be compared, is there a coherent three-leg
close, and did the critical observations arrive within their objectives.
Each indicator is a ``{state, reason, data}`` envelope; a non-ok state names
the input responsible and its observation date.
"""

from __future__ import annotations

import sqlite3
from dataclasses import replace
from datetime import date, datetime, timezone

import pytest

import config
from analysis.origins.assumptions import AssumptionSet
from analysis.origins.comparison import build_ranking
from analysis.origins.domain import Freshness, Money, ShipmentWindow
from app import decision_readiness as dr
from latency.measure import measure_from_rows
from pipeline import schema

NOW = datetime(2026, 11, 26, 5, 16, tzinfo=timezone.utc)


def _row(observed: str | None, fetched: str, status: str = "success") -> dict:
    return {
        "status": status,
        "observed_at": observed,
        "fetch_started_at": fetched,
        "fetch_completed_at": fetched,
        "stored_at": fetched,
    }


def _arrival_rows(**overrides: dict) -> dict:
    """Board, curve and FX all fetched on time for the 2026-11-25 session."""
    rows = {
        # CBOT settles 13:15 CT = 19:15 UTC in November; fetched 21:40 UTC.
        "prices": _row("2026-11-25", "2026-11-25T21:40:00+00:00"),
        "forward_curve": _row("2026-11-25", "2026-11-25T21:41:00+00:00"),
        # FX bar D closes at London midnight; fetched 01:30 UTC D+1.
        "currencies": _row("2026-11-24", "2026-11-25T01:30:00+00:00"),
        "gtr_ocean_freight": _row("2026-11-20", "2026-11-25T21:42:00+00:00"),
    }
    rows.update(overrides)
    return rows


def _arrival(rows: dict) -> dict:
    return dr.critical_arrival(measure_from_rows(rows, generated_at=NOW), now=NOW)


# ---------------------------------------------------------------------------
# Indicator 3 — before-cutoff arrival
# ---------------------------------------------------------------------------


def test_freight_that_succeeded_but_is_too_old_for_the_decision_is_not_a_failure() -> None:
    """The audit's case: layer_runs says success, observed_at is 2026-10-01."""
    result = _arrival(_arrival_rows(
        gtr_ocean_freight=_row("2026-10-01", "2026-11-25T21:42:00+00:00"),
    ))

    assert result["state"] == "degraded"
    assert "ocean freight" in result["reason"]
    assert "2026-10-01" in result["reason"]
    assert "failed" not in result["reason"]
    assert "56 d old — its run succeeded, but past the 7 d a freight number holds" in result["reason"]
    freight = next(i for i in result["data"]["inputs"] if i["layer"] == "gtr_ocean_freight")
    assert freight["run_status"] == "success"
    assert freight["observed_on"] == "2026-10-01"
    assert freight["grade"] == "too_old_for_decision"


def test_freight_whose_fetch_failed_reads_as_failed_not_as_too_old() -> None:
    result = _arrival(_arrival_rows(
        gtr_ocean_freight=_row("2026-10-01", "2026-11-25T21:42:00+00:00", status="failed"),
    ))

    assert result["state"] == "not_ready"
    assert "ocean freight" in result["reason"]
    assert "failed" in result["reason"]
    assert "2026-10-01" in result["reason"]
    freight = next(i for i in result["data"]["inputs"] if i["layer"] == "gtr_ocean_freight")
    assert freight["grade"] == "failed"


def test_every_critical_input_on_time_is_ready() -> None:
    result = _arrival(_arrival_rows())

    assert result["state"] == "ok"
    assert result["data"]["label"] == "ready"
    assert [i["grade"] for i in result["data"]["inputs"]] == [
        "on_time", "on_time", "on_time", "within_decision_life",
    ]


def test_a_board_fetched_past_its_objective_is_late_and_names_the_session() -> None:
    # 2026-11-25 settled 19:15 UTC; fetched 09:32 UTC next day — 14h 17m.
    result = _arrival(_arrival_rows(prices=_row("2026-11-25", "2026-11-26T09:32:00+00:00")))

    assert result["state"] == "degraded"
    assert result["reason"].startswith(
        "CBOT board observed 2026-11-25 landed 14h 17m after its close (acquisition objective 6h 0m)"
    )


def test_a_failed_board_with_no_observation_says_so_rather_than_inventing_a_date() -> None:
    result = _arrival(_arrival_rows(prices=_row(None, "2026-11-26T09:32:00+00:00", status="failed")))

    assert result["state"] == "not_ready"
    assert result["reason"].startswith("CBOT board failed this run — no observation recorded")


def test_failures_lead_the_reason_ahead_of_degradations() -> None:
    result = _arrival(_arrival_rows(
        prices=_row("2026-11-25", "2026-11-26T09:32:00+00:00"),
        currencies=_row("2026-11-24", "2026-11-25T01:30:00+00:00", status="failed"),
    ))

    assert result["state"] == "not_ready"
    assert result["reason"].index("FX failed") < result["reason"].index("CBOT board observed")


def test_a_layer_with_no_freshness_row_is_not_ready() -> None:
    rows = _arrival_rows()
    del rows["currencies"]
    result = _arrival(rows)

    assert result["state"] == "not_ready"
    assert "FX has no recorded run" in result["reason"]


def test_a_non_ok_indicator_without_a_reason_is_refused() -> None:
    with pytest.raises(ValueError, match="must name the input"):
        dr.indicator("Arrival", "degraded", "")


# ---------------------------------------------------------------------------
# Indicator 2 — three-leg close
# ---------------------------------------------------------------------------


def _cc(beans: str | None, meal: str | None, oil: str | None, crush: str | None) -> dict:
    def leg(name: str, as_of: str | None) -> dict:
        return {"name": name, "available": True, "as_of": as_of} if as_of else {
            "name": name, "available": False,
        }

    return {
        "legs": [leg("Soybeans", beans), leg("Soybean Oil", oil), leg("Soybean Meal", meal)],
        "crush": {"available": True, "as_of": crush} if crush else {"available": False},
    }


def test_three_legs_on_one_session_are_ready_and_name_the_session() -> None:
    result = dr.three_leg_close(_cc("2026-11-25", "2026-11-25", "2026-11-25", "2026-11-25"))

    assert result["state"] == "ok"
    assert result["data"]["session"] == "2026-11-25"
    assert result["data"]["session_label"] == "Wed 25 Nov"


def test_a_leg_behind_the_others_degrades_the_close_to_the_older_shared_session() -> None:
    result = dr.three_leg_close(_cc("2026-11-24", "2026-11-25", "2026-11-25", "2026-11-24"))

    assert result["state"] == "degraded"
    assert result["data"]["session"] == "2026-11-24"
    assert result["reason"] == (
        "Soybeans last closed 2026-11-24, behind Soybean Oil and Soybean Meal on "
        "2026-11-25 — the three-leg close is the 2026-11-24 session"
    )


def test_a_missing_leg_means_there_is_no_three_leg_close() -> None:
    result = dr.three_leg_close(_cc("2026-11-25", "2026-11-25", None, None))

    assert result["state"] == "not_ready"
    assert result["data"]["session"] is None
    assert "Soybean Oil has no stored close" in result["reason"]


def test_legs_that_never_shared_a_session_are_not_ready() -> None:
    result = dr.three_leg_close(_cc("2026-11-25", "2026-11-24", "2026-11-25", None))

    assert result["state"] == "not_ready"
    assert "no session on which all three legs closed" in result["reason"]
    assert "Soybean Meal 2026-11-24" in result["reason"]


def test_an_unbuilt_command_center_is_not_ready_with_a_reason() -> None:
    result = dr.three_leg_close(None)

    assert result["state"] == "not_ready"
    assert result["reason"]


def test_legs_each_ahead_of_the_shared_session_on_different_days_still_name_it() -> None:
    result = dr.three_leg_close(_cc("2026-11-25", "2026-11-23", "2026-11-24", "2026-11-20"))

    assert result["state"] == "degraded"
    assert result["reason"].startswith("the newest session all three legs closed on is 2026-11-20")
    assert "Soybean Meal on 2026-11-23" in result["reason"]


# ---------------------------------------------------------------------------
# Indicator 1 — origin comparison (FOB-equivalent, default destination)
# ---------------------------------------------------------------------------

ORIGIN_TODAY = date(2026, 8, 18)
SEP = ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026")


@pytest.fixture
def origins_db(tmp_path):
    """One session of all three origin legs (as tests/test_origin_comparison.py)."""
    conn = sqlite3.connect(str(tmp_path / "origins.db"))
    for ddl in (
        schema._CREATE_GULF_BIDS,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_CURRENCIES,
    ):
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month) VALUES (?,?,?,?,?,?)",
        [("2026-08-11", "Soybeans", "Gulf Coast Ports", "Sep¹", 12.6875, 11)],
    )
    conn.executemany(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
        "price_usd_mt) VALUES (?,?,?,?,?,?)",
        [("2026-08-11", "Soybeans", "12019000190C", "2026-09", "2026-10", 452.0)],
    )
    conn.executemany(
        "INSERT INTO brazil_spot_prices (Date, commodity, price_brl, unit) VALUES (?,?,?,?)",
        [("2026-08-11", "Soybean (AgRural Paranaguá FOB)", 2433.33, "BRL/MT")],
    )
    conn.executemany(
        "INSERT INTO currencies (pair, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)",
        [("BRL/USD", "2026-08-11", 0.1958, 0.1959, 0.1957, 0.1958)],
    )
    conn.commit()
    yield conn
    conn.close()


def _ranking(conn, legs: tuple[str, ...] | None = None):
    return build_ranking(
        conn, destination_key="cn_north", window=SEP, today=ORIGIN_TODAY,
        assumptions=AssumptionSet(()), leg_ids=legs,
    )


def _with_clone(ranking, *, observed: date, freshness=Freshness.CURRENT):
    """Add a second FOB-ready origin by cloning the Argentine row, $5 dearer."""
    (first,) = [row for row in ranking.rows if row.fob_equivalent is not None]
    clone = replace(
        first,
        quote=replace(
            first.quote,
            origin=replace(first.quote.origin, key="clone", name="Clone origin"),
            observation_date=observed,
        ),
        fob_equivalent=Money(first.fob_equivalent.amount + 5.0),
        freshness=freshness,
    )
    return replace(ranking, rows=(*ranking.rows, clone))


def test_one_fob_ready_origin_is_not_ready_and_names_each_excluded_input(origins_db) -> None:
    result = dr.origin_comparison(_ranking(origins_db), run_statuses={}, today=ORIGIN_TODAY)

    assert result["state"] == "not_ready"
    reason = result["reason"]
    assert reason.startswith("only Up-river (Rosario / San Lorenzo) is FOB-comparable for Sep 2026")
    assert "US Gulf (NOLA / Mississippi) observed 2026-08-11: no elevation assumption" in reason
    assert "Paranaguá observed 2026-08-11" in reason
    assert "publishes no shipment window" in reason


def test_two_fob_ready_origins_with_nothing_excluded_are_ready(origins_db) -> None:
    ranking = _with_clone(_ranking(origins_db, ("ar_up_river",)), observed=date(2026, 8, 11))
    result = dr.origin_comparison(ranking, run_statuses={}, today=ORIGIN_TODAY)

    assert result["state"] == "ok"
    assert [o["origin"] for o in result["data"]["board"]] == [
        "Up-river (Rosario / San Lorenzo)", "Clone origin",
    ]


def test_two_fob_ready_origins_with_one_excluded_are_degraded(origins_db) -> None:
    ranking = _with_clone(_ranking(origins_db, ("ar_up_river", "us_gulf")), observed=date(2026, 8, 11))
    result = dr.origin_comparison(ranking, run_statuses={}, today=ORIGIN_TODAY)

    assert result["state"] == "degraded"
    assert result["reason"].startswith("2 origins FOB-comparable for Sep 2026")
    assert "US Gulf (NOLA / Mississippi) observed 2026-08-11" in result["reason"]


def test_a_stale_quote_from_a_successful_run_reads_as_too_old_not_failed(origins_db) -> None:
    ranking = _with_clone(
        _ranking(origins_db, ("ar_up_river",)), observed=date(2026, 7, 20),
        freshness=Freshness.STALE,
    )
    result = dr.origin_comparison(
        ranking, run_statuses={"magyp_fob": "success"}, today=ORIGIN_TODAY,
    )

    assert result["state"] == "not_ready"
    assert "Clone origin observed 2026-07-20: 29 d old" in result["reason"]
    assert "its run succeeded" in result["reason"]
    assert "failed" not in result["reason"]


def test_a_stale_quote_from_a_failed_run_says_the_run_failed(origins_db) -> None:
    ranking = _with_clone(
        _ranking(origins_db, ("ar_up_river",)), observed=date(2026, 7, 20),
        freshness=Freshness.STALE,
    )
    result = dr.origin_comparison(
        ranking, run_statuses={"magyp_fob": "failed"}, today=ORIGIN_TODAY,
    )

    assert "Clone origin observed 2026-07-20: magyp_fob failed this run" in result["reason"]


def test_a_set_observed_too_far_apart_is_not_ready_with_the_board_refusal(
    origins_db, monkeypatch,
) -> None:
    monkeypatch.setattr(config, "ORIGIN_MAX_OBSERVATION_SPREAD_DAYS", 3)
    ranking = _with_clone(_ranking(origins_db, ("ar_up_river",)), observed=date(2026, 8, 5))
    result = dr.origin_comparison(ranking, run_statuses={}, today=ORIGIN_TODAY)

    assert result["state"] == "not_ready"
    assert "6 days apart" in result["reason"]
    assert "2026-08-05" in result["reason"]


# ---------------------------------------------------------------------------
# The masthead — headline and briefing pages
# ---------------------------------------------------------------------------


def _readiness() -> dict:
    origins = dr.indicator(
        "Origins", "not_ready",
        "only Up-river (Rosario / San Lorenzo) is FOB-comparable for Dec 2026 — Paranaguá "
        "observed 2026-11-16: publishes no shipment window",
    )
    close = dr.indicator("Three-leg close", "ok", session="2026-11-25", session_label="Wed 25 Nov")
    arrival = dr.indicator(
        "Arrival", "degraded",
        "ocean freight observed 2026-10-01, 56 d old — its run succeeded, but past the 7 d "
        "a freight number holds for a decision",
    )
    return {"indicators": [origins, close, arrival], "attention": [origins, arrival]}


def _render(template: str, readiness: dict | None) -> str:
    from app.templating import site_environment

    masthead = {
        "day_line": "Thursday · 26 November 2026", "on_schedule_count": 37,
        "total_layers": 41, "partial_count": 0, "critical_count": 0,
        "late_layers": [], "price_age": None,
    }
    return site_environment().get_template(template).render(
        masthead=masthead, decision_readiness=readiness, generated_at="2026-11-26 05:16 UTC",
        market_nav=[], root="", current_page="headline", sections=[], briefing_text="",
    )


@pytest.mark.parametrize("template", ["dashboard.html.j2", "briefing.html.j2"])
def test_the_masthead_states_each_indicator_beside_the_layer_count(template: str) -> None:
    html = _render(template, _readiness())

    # Directly above the layer count, which stays for diagnosis.
    line = html.index("Decision read ·")
    layer_count = html.index("37/41 layers on schedule")
    assert line < html.index("</div>", line) < layer_count
    assert 'class="mast-critical"' in html[line:html.index("</div>", line)]
    assert 'class="mast-partial"' in html[line:html.index("</div>", line)]
    assert "Three-leg close <b>ready</b> (Wed 25 Nov)" in html
    # The reasons are on the page, not only in a tooltip.
    note = html.index('<div class="stale-note"><b>Decision read:</b>')
    assert "publishes no shipment window" in html[note:]
    assert "ocean freight observed 2026-10-01" in html[note:]


@pytest.mark.parametrize("template", ["dashboard.html.j2", "briefing.html.j2"])
def test_an_all_ready_read_prints_no_warning_note(template: str) -> None:
    ready = dr.indicator("Origins", "ok")
    html = _render(template, {"indicators": [ready], "attention": []})

    assert "Origins <b>ready</b>" in html
    assert "<b>Decision read:</b>" not in html


@pytest.mark.parametrize("template", ["dashboard.html.j2", "briefing.html.j2"])
def test_a_render_without_readiness_still_renders_the_layer_count(template: str) -> None:
    html = _render(template, None)

    assert "37/41 layers on schedule" in html
    assert "Decision read ·" not in html


# ---------------------------------------------------------------------------
# The orchestrator
# ---------------------------------------------------------------------------


def test_the_public_read_costs_origins_off_the_public_assumption_tier(origins_db, monkeypatch) -> None:
    """Invariant 4: the masthead is on public pages, so desk entries never reach it."""
    import analysis.origins.assumptions as assumptions_mod
    import pipeline.connection as connection_mod

    seen: list[str] = []

    def fake_load(*, audience: str, **_: object) -> AssumptionSet:
        seen.append(audience)
        return AssumptionSet(())

    class _Unclosable:
        def __init__(self, conn) -> None:
            self._conn = conn

        def __getattr__(self, name: str):
            return getattr(self._conn, name)

        def close(self) -> None:
            pass

    monkeypatch.setattr(assumptions_mod, "load_assumptions", fake_load)
    monkeypatch.setattr(connection_mod, "get_connection", lambda: _Unclosable(origins_db))
    result = dr._public_origin_comparison({}, ORIGIN_TODAY)

    assert seen == ["public"]
    assert result["data"]["name"] == "Origins"


def test_an_indicator_that_raises_renders_not_ready_never_blank(monkeypatch) -> None:
    def boom(*_: object, **__: object) -> dict:
        raise RuntimeError("no prices table")

    monkeypatch.setattr(dr, "three_leg_close", boom)
    monkeypatch.setattr(dr, "_public_origin_comparison", lambda *_: dr.indicator("Origins", "ok"))
    monkeypatch.setattr(dr, "_run_statuses", dict)
    monkeypatch.setattr(dr, "critical_arrival", lambda *_, **__: dr.indicator("Arrival", "ok"))
    import latency.measure as measure_mod

    monkeypatch.setattr(measure_mod, "measure", lambda **_: [])

    result = dr.build_decision_readiness({}, now=NOW)

    close = result["indicators"][1]
    assert close["state"] == "not_ready"
    assert "could not be assessed this render (RuntimeError)" in close["reason"]
    assert result["attention"] == [close]


def test_freight_short_of_its_routes_is_usable_partial_before_its_age_is_judged() -> None:
    result = _arrival(_arrival_rows(
        gtr_ocean_freight=_row("2026-11-20", "2026-11-25T21:42:00+00:00", status="usable_partial"),
    ))

    assert result["state"] == "degraded"
    assert result["reason"] == "ocean freight usable partial this run, observed 2026-11-20"


def test_a_quiet_freight_publication_is_not_called_a_successful_run() -> None:
    result = _arrival(_arrival_rows(
        gtr_ocean_freight=_row("2026-10-01", "2026-11-25T21:42:00+00:00", status="no_publication"),
    ))

    assert result["state"] == "degraded"
    assert "its run reported no_publication" in result["reason"]
    assert "succeeded" not in result["reason"]


def test_a_disabled_board_is_not_ready_not_unmeasured() -> None:
    result = _arrival(_arrival_rows(prices=_row("2026-11-20", "2026-11-21T21:40:00+00:00", "disabled")))

    assert result["state"] == "not_ready"
    assert result["reason"] == "CBOT board is disabled — newest observed 2026-11-20"


def test_a_crush_that_raised_is_not_reported_as_legs_that_never_met() -> None:
    cc = _cc("2026-11-25", "2026-11-25", "2026-11-25", None)
    cc["crush"] = {"available": False, "error": "ValueError"}
    result = dr.three_leg_close(cc)

    assert result["state"] == "not_ready"
    assert "could not be computed this render (ValueError)" in result["reason"]
    assert "no session on which" not in result["reason"]


def test_every_arrival_input_is_declared_in_the_latency_roster() -> None:
    """A renamed latency layer must fail here, not turn the masthead red."""
    from latency.domain import LAYER_LATENCY_BY_KEY

    for layer, _label in dr.ARRIVAL_INPUTS:
        assert layer in LAYER_LATENCY_BY_KEY


def test_a_board_still_holding_an_old_session_after_a_newer_one_is_overdue_is_not_ready() -> None:
    """The run that fetched it may have been on time; nothing has arrived since.

    At 05:16 UTC on 26 Nov the 25 Nov CBOT session (19:15 UTC) is past its 6 h
    acquisition objective, so a board still on 16 Nov has missed it.
    """
    result = _arrival(_arrival_rows(prices=_row("2026-11-16", "2026-11-16T21:40:00+00:00")))

    assert result["state"] == "not_ready"
    assert result["reason"].startswith(
        "CBOT board still holds the 2026-11-16 session — the 2026-11-25 session closed "
        "10h 1m ago, past its 6h 0m acquisition objective"
    )
    board = next(i for i in result["data"]["inputs"] if i["layer"] == "prices")
    assert board["grade"] == "missed"


def test_fx_one_session_behind_before_the_newer_one_is_due_is_still_on_time() -> None:
    # FX 25 Nov closes 00:00 UTC 26 Nov; at 05:16 it is not yet 6 h past.
    result = _arrival(_arrival_rows())

    fx = next(i for i in result["data"]["inputs"] if i["layer"] == "currencies")
    assert fx["observed_on"] == "2026-11-24"
    assert fx["grade"] == "on_time"
