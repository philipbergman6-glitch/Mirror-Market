"""Refresh Prices slot routing, the two backstops, and the board slot's timing.

The owner's decision on #622 (2026-11-25, Option A): a cron-job.org dispatch
fires the CBOT board at 01:00 America/New_York Tue-Sat against a twelve-hour
objective, and the 10:30 UTC dce dispatch is the backstop when no board
dispatch delivered that day. These tests replace reading bash in YAML.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest
import yaml

from scripts import refresh_slot as rs
from scripts.refresh_slot import SlotError, plan

UTC = timezone.utc
NY = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]
WORKFLOW = ROOT / ".github" / "workflows" / "refresh-prices.yml"

# Wednesday 2026-11-25 (EST: New York = UTC-5).
WED_1030 = datetime(2026, 11, 25, 10, 30, tzinfo=UTC)
MON_1030 = datetime(2026, 11, 23, 10, 30, tzinfo=UTC)


class Lookup:
    """A stub of the gh lookup that records what it was asked."""

    def __init__(self, delivered: set[str] = frozenset()):  # type: ignore[assignment]
        self.delivered = delivered
        self.calls: list[tuple[str, datetime]] = []

    def __call__(self, title: str, since: datetime) -> bool:
        self.calls.append((title, since))
        return title in self.delivered


def _dispatch(*, layers: str = "", slot: str = "", now: datetime = WED_1030, lookup=None):
    return plan(event="workflow_dispatch", schedule="", layers_input=layers,
                slot_input=slot, now=now, delivered=lookup or Lookup())


def _scheduled(schedule: str, *, now: datetime = WED_1030, lookup=None):
    return plan(event="schedule", schedule=schedule, layers_input="", slot_input="",
                now=now, delivered=lookup or Lookup())


# ---------------------------------------------------------------------------
# Slot routing
# ---------------------------------------------------------------------------


def test_the_board_slot_fetches_and_gates_only_the_cbot_board():
    p = _dispatch(slot="board", now=datetime(2026, 11, 25, 6, 1, tzinfo=UTC))
    assert p.layers == "prices,forward_curve"
    assert p.gate == "prices,forward_curve"
    assert not (p.skip or p.dce_backstop or p.board_backstop or p.late_start_ok)


def test_the_board_slot_layers_are_the_latency_vocabularys_board():
    from latency.domain import CBOT_BOARD_LAYERS, LAYER_LATENCY_BY_KEY, LatencyClass

    assert rs.BOARD_LAYERS == CBOT_BOARD_LAYERS
    for layer in CBOT_BOARD_LAYERS:
        assert LAYER_LATENCY_BY_KEY[layer].latency_class is LatencyClass.BOARD_PRICE


@pytest.mark.parametrize("slot,layers", [("board", "prices"), ("boards", ""), ("dce", "")])
def test_an_unroutable_dispatch_fails_rather_than_guessing(slot, layers):
    with pytest.raises(SlotError):
        _dispatch(slot=slot, layers=layers)


def test_a_manual_layer_list_is_fetched_and_gated_as_asked():
    p = _dispatch(layers="currencies")
    assert (p.layers, p.gate, p.board_backstop) == ("currencies", "currencies", False)


def test_an_empty_manual_dispatch_fetches_the_default_set_and_gates_nothing():
    p = _dispatch()
    assert (p.layers, p.gate) == ("", "")


def test_the_evening_slot_gates_nothing_and_the_catchup_gates_fx():
    assert _scheduled(rs.SCHEDULE_EVENING).gate == ""
    catchup = _scheduled(rs.SCHEDULE_CATCHUP)
    assert (catchup.gate, catchup.late_start_ok) == ("currencies", True)


def test_an_unknown_schedule_or_event_fails():
    with pytest.raises(SlotError):
        _scheduled("15 3 * * 1-5")
    with pytest.raises(SlotError):
        plan(event="push", schedule="", layers_input="", slot_input="",
             now=WED_1030, delivered=Lookup())


# ---------------------------------------------------------------------------
# The DCE backstop (08:00 cron), unchanged in intent
# ---------------------------------------------------------------------------


def test_the_dce_backstop_stands_down_when_dce_was_delivered_today():
    lookup = Lookup({rs.DCE_DISPATCH_TITLE})
    p = _scheduled(rs.SCHEDULE_DCE_BACKSTOP, now=datetime(2026, 11, 25, 14, 0, tzinfo=UTC),
                   lookup=lookup)
    assert p.skip is True and p.dce_backstop is False
    assert lookup.calls == [(rs.DCE_DISPATCH_TITLE, datetime(2026, 11, 25, tzinfo=UTC))]


def test_the_dce_backstop_fetches_dce_when_nothing_delivered():
    p = _scheduled(rs.SCHEDULE_DCE_BACKSTOP, now=datetime(2026, 11, 25, 14, 0, tzinfo=UTC))
    assert (p.layers, p.gate, p.skip, p.dce_backstop) == ("dce", "dce", False, True)


# ---------------------------------------------------------------------------
# The board backstop (10:30 UTC dce dispatch)
# ---------------------------------------------------------------------------


def test_the_dce_dispatch_backstops_the_board_when_no_board_run_delivered():
    lookup = Lookup()
    p = _dispatch(layers="dce", lookup=lookup)
    assert p.layers == "dce,prices,forward_curve"
    assert p.gate == "dce"                      # the own gate stays dce's
    assert p.board_backstop is True
    assert p.board_gate == "prices,forward_curve"
    # Looked up since 00:00 New York of the same day (05:00 UTC in EST).
    assert lookup.calls == [(rs.BOARD_DISPATCH_TITLE, datetime(2026, 11, 25, 5, 0, tzinfo=UTC))]


def test_the_board_backstop_stands_down_when_the_board_was_delivered():
    p = _dispatch(layers="dce", lookup=Lookup({rs.BOARD_DISPATCH_TITLE}))
    assert (p.layers, p.gate, p.board_backstop) == ("dce", "dce", False)


def test_the_board_backstop_stands_down_on_a_day_no_board_dispatch_is_due():
    """Monday New York: Friday's session was carried by Saturday's 01:00 run."""
    lookup = Lookup()
    p = _dispatch(layers="dce", now=MON_1030, lookup=lookup)
    assert (p.layers, p.board_backstop) == ("dce", False)
    assert lookup.calls == []


def test_the_board_backstop_waits_out_the_grace_after_the_fire_time():
    """A manual dce dispatch at 06:30 UTC (01:30 EST) must not race the board run."""
    lookup = Lookup()
    p = _dispatch(layers="dce", now=datetime(2026, 11, 25, 6, 30, tzinfo=UTC), lookup=lookup)
    assert p.board_backstop is False and lookup.calls == []
    later = _dispatch(layers="dce", now=datetime(2026, 11, 25, 7, 0, tzinfo=UTC))
    assert later.board_backstop is True


@pytest.mark.parametrize(
    "now_utc,due_utc",
    [
        (datetime(2026, 7, 15, 10, 30, tzinfo=UTC), datetime(2026, 7, 15, 5, 0, tzinfo=UTC)),   # EDT
        (datetime(2026, 12, 15, 10, 30, tzinfo=UTC), datetime(2026, 12, 15, 6, 0, tzinfo=UTC)),  # EST
    ],
)
def test_the_board_is_due_at_01_00_new_york_in_both_seasons(now_utc, due_utc):
    assert rs.board_due_at(now_utc) == due_utc


def test_the_board_is_due_tuesday_to_saturday_new_york():
    due = {d: rs.board_due_at(datetime.combine(d, time(12), tzinfo=UTC)) is not None
           for d in (date(2026, 11, 22) + timedelta(days=n) for n in range(7))}
    # Sun 22 .. Sat 28
    assert [due[d] for d in sorted(due)] == [False, False, True, True, True, True, True]


# ---------------------------------------------------------------------------
# "Delivered" reads the own gate step, not the run's conclusion
# ---------------------------------------------------------------------------


def _run(run_id, title, conclusion, status="completed"):
    return {"databaseId": run_id, "displayTitle": title, "status": status,
            "conclusion": conclusion}


def _jobs(gate_conclusion, board_conclusion="failure"):
    return [
        {"name": "slot", "steps": [{"name": "Select layers for this slot", "conclusion": "success"}]},
        {"name": "refresh", "steps": [
            {"name": "Deploy to GitHub Pages", "conclusion": "success"},
            {"name": rs.OWN_GATE_STEP, "conclusion": gate_conclusion},
            {"name": "Gate the board backstop", "conclusion": board_conclusion},
        ]},
    ]


def test_a_green_run_delivered_without_reading_its_jobs():
    def jobs_of(_):
        raise AssertionError("jobs must not be read for a green run")

    assert rs.delivered_from_runs([_run(1, rs.DCE_DISPATCH_TITLE, "success")],
                                  rs.DCE_DISPATCH_TITLE, jobs_of)


def test_a_dce_run_red_only_on_the_board_backstop_still_delivered_dce():
    """Else the 08:00 backstop would refetch dce late and re-stamp it."""
    assert rs.delivered_from_runs([_run(1, rs.DCE_DISPATCH_TITLE, "failure")],
                                  rs.DCE_DISPATCH_TITLE, lambda _: _jobs("success"))


def test_a_run_red_on_its_own_gate_did_not_deliver():
    assert not rs.delivered_from_runs([_run(1, rs.DCE_DISPATCH_TITLE, "failure")],
                                      rs.DCE_DISPATCH_TITLE, lambda _: _jobs("failure", "skipped"))


def test_other_titles_and_unfinished_or_cancelled_runs_never_count():
    runs = [_run(1, rs.BOARD_DISPATCH_TITLE, "success"),
            _run(2, rs.DCE_DISPATCH_TITLE, None, status="in_progress"),
            _run(3, rs.DCE_DISPATCH_TITLE, "cancelled")]
    assert not rs.delivered_from_runs(runs, rs.DCE_DISPATCH_TITLE, lambda _: _jobs("success"))


def test_a_failed_gh_lookup_fails_the_slot(monkeypatch):
    import subprocess

    def boom(*a, **k):
        raise subprocess.CalledProcessError(1, "gh")

    monkeypatch.setattr(rs.subprocess, "run", boom)
    with pytest.raises(subprocess.CalledProcessError):
        rs.gh_delivered(rs.BOARD_DISPATCH_TITLE, WED_1030)


def test_main_writes_every_output_the_workflow_reads(tmp_path, monkeypatch):
    monkeypatch.setattr(rs, "gh_delivered", lambda title, since: False)
    out = tmp_path / "out"
    assert rs.main(["--event", "workflow_dispatch", "--slot", "board",
                    "--github-output", str(out)]) == 0
    written = dict(line.split("=", 1) for line in out.read_text().splitlines())
    jobs = yaml.safe_load(WORKFLOW.read_text())["jobs"]
    declared = set(jobs["slot"]["outputs"])
    assert set(written) == declared
    assert written["layers"] == "prices,forward_curve"


def test_main_reports_an_unroutable_run_as_an_error():
    assert rs.main(["--event", "workflow_dispatch", "--slot", "nope"]) == 1


# ---------------------------------------------------------------------------
# The workflow agrees with the script
# ---------------------------------------------------------------------------


def _workflow():
    return yaml.safe_load(WORKFLOW.read_text())


def test_the_workflow_crons_are_exactly_the_routed_schedules():
    doc = _workflow()
    on = doc.get("on", doc.get(True))
    crons = {c["cron"] for c in on["schedule"]}
    assert crons == {rs.SCHEDULE_DCE_BACKSTOP, rs.SCHEDULE_EVENING, rs.SCHEDULE_CATCHUP}
    assert {"layers", "slot"} <= set(on["workflow_dispatch"]["inputs"])


def test_the_step_and_job_names_the_lookup_reads_exist():
    refresh = _workflow()["jobs"][rs.REFRESH_JOB]
    names = [step.get("name") for step in refresh["steps"]]
    assert rs.OWN_GATE_STEP in names
    gate_at = names.index(rs.OWN_GATE_STEP)
    board_at = names.index("Gate the board backstop")
    # The board backstop step comes after the own gate, so the own gate's
    # success is decided before the board half can fail the run.
    assert board_at == gate_at + 1 == len(names) - 1


def test_the_run_name_titles_a_board_dispatch_as_the_lookup_expects():
    run_name = _workflow()["run-name"]
    assert "inputs.slot" in run_name
    assert rs.BOARD_DISPATCH_TITLE == "Refresh Prices (board)"


# ---------------------------------------------------------------------------
# The objective and the fire time, checked against each other
# ---------------------------------------------------------------------------


def _settlement(day: date) -> datetime:
    from latency.domain import LAYER_LATENCY_BY_KEY

    instant = LAYER_LATENCY_BY_KEY["prices"].clock.instant(day)
    assert instant is not None
    return instant


def test_an_on_time_board_fetch_meets_the_objective_every_day_of_the_year():
    """01:00 New York + 10 minutes of dispatch and fetch, for Mon-Fri sessions
    of 2026 in both seasons, is inside twelve hours; and the slot is after
    the bar is final (the settlement guard's own rule)."""
    from fetchers._settlement import EXCHANGE_SESSION
    from latency.domain import BOARD_ACQUISITION_TARGET

    day = date(2026, 1, 1)
    while day.year == 2026:
        if day.weekday() < 5:
            fire = datetime.combine(day + timedelta(days=1), rs.BOARD_FIRE_LOCAL, tzinfo=NY)
            assert rs.board_due_at(fire + timedelta(minutes=1)) == fire.astimezone(UTC)
            assert EXCHANGE_SESSION.last_settled_session(fire) == day, day
            acquisition = fire + timedelta(minutes=10) - _settlement(day)
            assert acquisition < BOARD_ACQUISITION_TARGET, (day, acquisition)
        day += timedelta(days=1)


def test_the_backstop_landing_breaches_the_objective():
    """10:30 UTC + 15 minutes is ~15-16h after the settlement: a miss."""
    from latency.domain import BOARD_ACQUISITION_TARGET

    for day in (date(2026, 7, 14), date(2026, 12, 15)):
        landing = datetime.combine(day + timedelta(days=1), time(10, 45), tzinfo=UTC)
        assert landing - _settlement(day) > BOARD_ACQUISITION_TARGET
