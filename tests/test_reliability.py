"""The source reliability figures (B34 #520 §3) and the dead-source streak (§4).

One function over the layer_runs store. These tests build rows in memory: the
computation never touches the filesystem, so no HISTORY_DIR is needed.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, time, timedelta
from zoneinfo import ZoneInfo

import pytest

import config
from pipeline import reliability
from pipeline.layer_runs import LayerRun

MON = date(2026, 10, 5)


def _weekdays(start: date, n: int) -> list[date]:
    out, day = [], start
    while len(out) < n:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def _row(session: date, layer: str, status: str, run: str | None = None,
         hour: int = 20, rendered: bool = True) -> LayerRun:
    return LayerRun(
        run_id=run or f"{session.isoformat()}-{hour}",
        workflow="Update Dashboard/schedule",
        run_started_at=f"{session.isoformat()}T{hour:02d}:00:00Z",
        session_date=session,
        layer_name=layer,
        status=status,
        observed_at=session.isoformat(),
        rendered_at=f"{session.isoformat()}T{hour:02d}:30:00+00:00" if rendered else None,
    )


def _layer(block: dict, name: str) -> dict:
    return next(entry for entry in block["layers"] if entry["layer"] == name)


# ── basket ─────────────────────────────────────────────────────────────────


def test_availability_basket_is_basket_v1s_nine_layers() -> None:
    # Pinned to docs/plans/source-reliability-a19.md §1: strata 1-6 and 9,
    # plus the Layer 11b closes the Gulf basis strikes on (B37 #534); derived
    # strata 7-8 have no layer of their own.
    assert config.AVAILABILITY_BASKET == (
        "forward_curve", "contract_bars", "dce", "agrural", "cepea",
        "magyp_fob", "gulf_bids", "currencies", "us_processor_cash",
    )
    assert set(config.AVAILABILITY_BASKET) <= set(config.PRODUCTION_LAYER_KEYS)
    assert not hasattr(config, "TRIAL_CRITICAL_LAYERS")


def test_private_layers_are_the_rosters_private_entries() -> None:
    private = {key for key, *_rest, desc in config.PRODUCTION_LAYERS if "(private)" in desc}
    assert set(config.PRIVATE_LAYERS) == private


def test_available_statuses_are_the_notes_three_words() -> None:
    assert frozenset({"success", "usable_partial", "no_publication"}) == reliability.AVAILABLE_STATUSES


# ── figures ────────────────────────────────────────────────────────────────


def test_empty_store_is_withheld_with_a_reason() -> None:
    block = reliability.reliability([], set(), through=MON)
    assert block["state"] == "no_record"
    assert block["reason"]


def test_all_green_window_is_fully_available() -> None:
    sessions = _weekdays(MON, 30)
    rows = [_row(s, "safex", "success") for s in sessions]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())

    safex = _layer(block, "safex")
    assert block["state"] == "ok"
    assert block["window"]["sessions"] == 30
    assert safex["logged_sessions"] == 30
    assert safex["availability_pct"] == 100.0
    assert safex["longest_unavailable_streak"] == 0
    assert safex["open_at_end"] is False
    assert safex["median_recovery_sessions"] is None


def test_a_weekday_with_no_run_row_is_no_run_and_unavailable() -> None:
    sessions = _weekdays(MON, 5)
    rows = [_row(s, "safex", "success") for s in sessions if s != sessions[2]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())

    safex = _layer(block, "safex")
    assert safex["logged_sessions"] == 4
    assert safex["availability_pct"] == 80.0
    assert safex["longest_unavailable_streak"] == 1
    assert block["edition"]["sessions_with_run"] == 4
    assert block["edition"]["longest_no_run_gap"] == 1


def test_weekend_runs_add_runs_never_sessions() -> None:
    # A Saturday dispatch serves Friday's session (session_date is a weekday
    # by construction); the window still counts five sessions for the week.
    sessions = _weekdays(MON, 5)
    rows = [_row(s, "safex", "success") for s in sessions]
    rows.append(_row(sessions[-1], "safex", "failed", run="sat", hour=23))
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())
    assert block["window"]["sessions"] == 5
    # Friday's Saturday re-run failed, but Friday had a success: still available
    # (any-success, owner 2026-11-25); the failed run is counted, not hidden.
    safex = _layer(block, "safex")
    assert safex["availability_pct"] == 100.0
    assert safex["failed_runs"] == 1


def test_a_successful_retry_makes_the_session_available_and_first_run_shows_it() -> None:
    sessions = _weekdays(MON, 2)
    rows = [
        _row(sessions[0], "gulf_bids", "failed", hour=20),
        _row(sessions[0], "gulf_bids", "success", hour=22),
        _row(sessions[1], "gulf_bids", "success", hour=20),
    ]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("gulf_bids",), basket=())
    gulf = _layer(block, "gulf_bids")
    assert gulf["availability_pct"] == 100.0
    assert gulf["first_run_availability_pct"] == 50.0


@pytest.mark.parametrize("status", ["stale", "failed", "incomplete", "skipped_unconfigured", "not_run"])
def test_every_other_state_is_unavailable(status: str) -> None:
    rows = [_row(MON, "safex", status)]
    block = reliability.reliability(rows, set(), through=MON, layers=("safex",), basket=())
    assert _layer(block, "safex")["availability_pct"] == 0.0


def test_streaks_recovery_and_open_at_end() -> None:
    sessions = _weekdays(MON, 10)
    pattern = ["success", "failed", "failed", "success", "failed", "failed", "failed", "failed",
               "success", "failed"]
    rows = [_row(s, "safex", st) for s, st in zip(sessions, pattern, strict=True)]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())
    safex = _layer(block, "safex")
    assert safex["longest_unavailable_streak"] == 4
    assert safex["median_recovery_sessions"] == 3.0  # closed streaks of 2 and 4
    assert safex["open_at_end"] is True
    assert safex["open_streak"] == 1


def test_window_is_the_last_n_sessions_through_the_given_session() -> None:
    sessions = _weekdays(MON, 35)
    rows = [_row(s, "safex", "failed") for s in sessions[:5]]
    rows += [_row(s, "safex", "success") for s in sessions[5:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())
    assert block["window"]["first_session"] == sessions[5].isoformat()
    assert _layer(block, "safex")["availability_pct"] == 100.0


def test_window_never_reaches_back_before_the_record_began() -> None:
    sessions = _weekdays(MON, 3)
    rows = [_row(s, "safex", "success") for s in sessions]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())
    assert block["window"]["sessions"] == 3
    assert block["window"]["record_started"] == sessions[0].isoformat()
    assert _layer(block, "safex")["availability_pct"] == 100.0


def test_rows_after_through_are_ignored() -> None:
    sessions = _weekdays(MON, 3)
    rows = [_row(s, "safex", "success") for s in sessions[:2]] + [_row(sessions[2], "safex", "failed")]
    block = reliability.reliability(rows, set(), through=sessions[1], layers=("safex",), basket=())
    assert _layer(block, "safex")["availability_pct"] == 100.0


def test_surfaced_within_one_run_reads_the_alert_receipts() -> None:
    sessions = _weekdays(MON, 2)
    rows = [_row(sessions[0], "safex", "failed", run="a"), _row(sessions[1], "safex", "stale", run="b"),
            _row(sessions[1], "dce", "success", run="b")]
    block = reliability.reliability(rows, {("a", "safex")}, through=sessions[-1],
                                    layers=("safex", "dce"), basket=())
    assert _layer(block, "safex")["failing_runs"] == 2
    assert _layer(block, "safex")["surfaced_within_one_run_pct"] == 50.0
    # No failing run: nothing to surface, so no figure — not a vacuous 100.
    assert _layer(block, "dce")["surfaced_within_one_run_pct"] is None


def test_basket_layers_are_tagged_and_edition_share_reported() -> None:
    sessions = _weekdays(MON, 2)
    rows = [_row(sessions[0], "dce", "success"), _row(sessions[1], "dce", "success", rendered=False)]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce", "safex"), basket=("dce",))
    assert _layer(block, "dce")["basket"] is True
    assert _layer(block, "safex")["basket"] is False
    assert block["edition"]["sessions_with_edition"] == 1


def test_thresholds_are_reported_from_config_never_gating() -> None:
    sessions = _weekdays(MON, 30)
    rows = [_row(s, "dce", "success") for s in sessions]
    rows[3] = _row(sessions[3], "dce", "failed")
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    t = block["thresholds"]
    assert t["T2_min_availability_pct"]["value"] == config.RELIABILITY_T2_MIN_AVAILABILITY_PCT
    assert t["T1_continuous"]["met"] is True
    assert t["T2_min_availability_pct"]["met"] is True  # 29/30 = 96.7
    assert t["T3_max_streak_sessions"]["met"] is True
    # A full window of the layer's own: the floor holds nothing back.
    assert t["T2_min_availability_pct"]["reason"] is None
    assert t["T3_max_streak_sessions"]["reason"] is None
    assert t["T4_surfaced_within_one_run_pct"]["met"] is False  # one unsurfaced failure


def test_t1_fails_on_a_short_record_or_a_long_gap() -> None:
    sessions = _weekdays(MON, 30)
    rows = [_row(s, "dce", "success") for s in sessions if s not in sessions[10:13]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    assert block["window"]["restarted_at"] == sessions[13].isoformat()
    assert block["window"]["restart_gap"] == 3
    assert block["window"]["sessions"] == 17
    assert block["thresholds"]["T1_continuous"]["met"] is False

    short = reliability.reliability(rows[:5], set(), through=sessions[4], layers=("dce",), basket=("dce",))
    assert short["thresholds"]["T1_continuous"]["met"] is False


def test_a_long_gap_restarts_the_window_never_trims_it() -> None:
    # The fifth audit's reproduction (#533): 1 logged + 3 with no run + 28
    # logged is 32 weekdays, and the last 30 hold only one gap day. Cutting
    # the window first trimmed the gap away; the restart must enforce it.
    sessions = _weekdays(MON, 32)
    rows = [_row(sessions[0], "dce", "failed")] + [_row(s, "dce", "success") for s in sessions[4:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))

    window = block["window"]
    assert window["record_started"] == sessions[0].isoformat()
    assert window["restarted_at"] == sessions[4].isoformat()
    assert window["restart_gap"] == 3
    assert window["evidence_sessions"] == 28
    assert window["sessions"] == 28
    assert window["first_session"] == sessions[4].isoformat()
    assert block["edition"]["longest_no_run_gap"] == 0
    assert block["thresholds"]["T1_continuous"]["met"] is False
    # T2–T4 compute over the same restarted window: the pre-gap failure is gone.
    dce = _layer(block, "dce")
    assert dce["availability_pct"] == 100.0
    assert dce["failing_runs"] == 0


def test_a_tolerated_gap_neither_restarts_nor_shortens_the_record() -> None:
    sessions = _weekdays(MON, 32)
    rows = [_row(sessions[0], "dce", "success")] + [_row(s, "dce", "success") for s in sessions[3:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))

    window = block["window"]
    assert window["restarted_at"] is None
    assert window["restart_gap"] is None
    assert window["evidence_sessions"] == 32
    assert window["sessions"] == 30
    assert block["edition"]["longest_no_run_gap"] == 1
    assert block["thresholds"]["T1_continuous"]["met"] is True


def test_the_last_long_gap_is_the_one_that_restarts() -> None:
    sessions = _weekdays(MON, 50)
    logged = sessions[:5] + sessions[9:20] + sessions[25:]
    rows = [_row(s, "dce", "success") for s in logged]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    assert block["window"]["restarted_at"] == sessions[25].isoformat()
    assert block["window"]["restart_gap"] == 5
    assert block["window"]["evidence_sessions"] == 25


def test_an_open_long_gap_withholds_the_figures_until_a_session_is_logged() -> None:
    # A dead deploy: the window has restarted but no session after the gap
    # exists yet, so there is no evidence to compute over.
    sessions = _weekdays(MON, 10)
    rows = [_row(s, "dce", "success") for s in sessions[:7]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    assert block["state"] == "restarting"
    assert sessions[7].isoformat() in block["reason"]
    # T1 is answered, not withheld: the open gap means it is not met.
    assert block["data"]["thresholds"]["T1_continuous"]["met"] is False
    window = block["data"]["window"]
    assert window["record_started"] == sessions[0].isoformat()
    assert window["gap_first_session"] == sessions[7].isoformat()
    assert window["restart_gap"] == 3
    assert window["restarted_at"] is None
    assert window["evidence_sessions"] == 0

    # Two trailing sessions are a tolerated gap, not a restart.
    tolerated = reliability.reliability(rows, set(), through=sessions[8], layers=("dce",), basket=("dce",))
    assert tolerated["state"] == "ok"
    assert tolerated["window"]["restarted_at"] is None


# ── dead source ────────────────────────────────────────────────────────────


def test_dead_layers_need_three_consecutive_unavailable_sessions() -> None:
    sessions = _weekdays(MON, 4)
    rows = [_row(sessions[0], "safex", "success")]
    rows += [_row(s, "safex", "failed") for s in sessions[1:3]]
    assert reliability.dead_layers(rows, layers=("safex",)) == {}

    rows.append(_row(sessions[3], "safex", "failed"))
    dead = reliability.dead_layers(rows, layers=("safex",))
    assert dead["safex"].sessions == 3
    assert dead["safex"].first_session == sessions[1]
    assert dead["safex"].last_status == "failed"


def test_dead_streak_counts_sessions_not_runs() -> None:
    # Seven failing runs in one morning are one unavailable session.
    rows = [_row(MON, "gulf_bids", "failed", run=f"r{h}", hour=h) for h in range(10, 17)]
    rows.append(_row(MON + timedelta(days=1), "gulf_bids", "success"))
    assert reliability.dead_layers(rows, layers=("gulf_bids",)) == {}


def test_no_run_sessions_extend_a_dead_streak() -> None:
    sessions = _weekdays(MON, 4)
    rows = [_row(sessions[0], "safex", "success"), _row(sessions[0], "dce", "success"),
            _row(sessions[3], "safex", "failed"), _row(sessions[3], "dce", "success")]
    dead = reliability.dead_layers(rows, layers=("safex", "dce"))
    assert set(dead) == {"safex"}
    assert dead["safex"].sessions == 3


def test_dead_streak_never_counts_sessions_before_the_record() -> None:
    rows = [_row(MON, "safex", "failed")]
    assert reliability.dead_layers(rows, layers=("safex",)) == {}


def test_dead_streak_says_when_it_opens_at_the_record_start() -> None:
    # #566: india_domestic was dark from 2026-10-09, the record began 10-16, so
    # "3 sessions from 10-16" is a floor. The streak must say it cannot see
    # further back, rather than read as an outage that began with the record.
    sessions = _weekdays(MON, 4)
    rows = [_row(s, "safex", "failed") for s in sessions]
    rows += [_row(sessions[0], "dce", "success")] + [_row(s, "dce", "failed") for s in sessions[1:]]
    dead = reliability.dead_layers(rows, layers=("safex", "dce"))
    assert dead["safex"].opens_at_record_start is True
    assert dead["safex"].first_session == sessions[0]
    assert dead["dce"].opens_at_record_start is False


def test_real_record_never_reports_matif_dead(monkeypatch: pytest.MonkeyPatch) -> None:
    # #633: the 2026-11-24 relay comment on #566 printed matif "unavailable at
    # least 28 consecutive sessions — the record begins 2026-10-16". Every
    # matif row is `skipped_unconfigured` (Layer 37 private, by design) and its
    # first row is 2026-11-05; the 11-09→11-12 weekdays with no run at all are
    # what defeated the only-skipped exemption. Read from the committed record
    # (read only), cut at the session that comment reported, so later appends
    # cannot move the case.
    from pipeline import history, layer_runs

    monkeypatch.setattr(history, "HISTORY_DIR", config.HISTORY_DIR)
    rows = [r for r in layer_runs.read_runs() if r.session_date <= date(2026, 11, 24)]
    matif = [r for r in rows if r.layer_name == "matif"]
    assert matif and {r.status for r in matif} == {"skipped_unconfigured"}
    assert min(r.session_date for r in matif) == date(2026, 11, 5)

    assert "matif" not in reliability.dead_layers(rows)


def test_real_record_manifest_streaks_start_at_the_layers_first_row(monkeypatch: pytest.MonkeyPatch) -> None:
    # #638: after #634 the alert counted a layer's streak from its own first
    # row while the manifest block counted it from the window's first session.
    # matif and afex joined the roster on 2026-11-05, the record began 10-16.
    # Committed record, read only, cut at fixed sessions so later appends
    # cannot move the case.
    from pipeline import history, layer_runs

    monkeypatch.setattr(history, "HISTORY_DIR", config.HISTORY_DIR)
    record = layer_runs.read_runs()
    assert min(r.session_date for r in record) == date(2026, 10, 16)
    for layer in ("matif", "afex"):
        assert min(r.session_date for r in record if r.layer_name == layer) == date(2026, 11, 5)

    def layers_at(through: date) -> dict[str, dict]:
        rows = [r for r in record if r.session_date <= through]
        block = reliability.reliability(rows, set(), through=through)
        assert block["state"] == "ok"
        return {entry["layer"]: entry for entry in block["layers"]}

    # 2026-11-10: the window still runs from 10-16 (the 11-09 no-run gap is
    # not yet longer than T1 tolerates). matif's streak is its own four
    # sessions (11-05, 11-06, 11-09, 11-10), not the window's eighteen; afex
    # succeeded on 11-05 and 11-06, so its only streak is the two no-run
    # weekdays after — the fourteen pre-roster weekdays are not an outage
    # it recovered from.
    at = layers_at(date(2026, 11, 10))
    assert at["matif"]["open_streak"] == 4
    assert at["matif"]["longest_unavailable_streak"] == 4
    assert at["afex"]["open_streak"] == 2
    assert at["afex"]["longest_unavailable_streak"] == 2
    assert at["afex"]["median_recovery_sessions"] is None

    # 2026-11-24: the window restarted at 11-13 after the 11-09→11-12 gap, so
    # the later of the two bounds holds — eight sessions, 11-13 → 11-24.
    at = layers_at(date(2026, 11, 24))
    assert at["matif"]["open_streak"] == 8
    assert at["afex"]["longest_unavailable_streak"] == 0


def test_real_record_availability_starts_at_the_layers_first_row(monkeypatch: pytest.MonkeyPatch) -> None:
    # #638 owner decision 2026-11-25: availability % runs from the layer's own
    # first row too, over a per-layer ``sessions`` denominator. The owner pack
    # on #638 read afex at the 11-10 cut as 11.1 % (2 of 18) before and 50.0 %
    # (2 of 4) after. Committed record, read only, fixed cut.
    from pipeline import history, layer_runs

    monkeypatch.setattr(history, "HISTORY_DIR", config.HISTORY_DIR)
    through = date(2026, 11, 10)
    rows = [r for r in layer_runs.read_runs() if r.session_date <= through]
    block = reliability.reliability(rows, set(), through=through)
    assert block["window"]["sessions"] == 18
    at = {entry["layer"]: entry for entry in block["layers"]}

    assert at["afex"]["sessions"] == 4
    assert at["afex"]["availability_pct"] == 50.0
    assert at["matif"]["sessions"] == 4
    assert at["matif"]["availability_pct"] == 0.0  # every row skipped_unconfigured
    # A layer on the roster since the record began keeps the whole window.
    assert at["prices"]["sessions"] == 18

    # The floor on the live cut: the window restarted at 11-13, so every
    # basket layer has 8 own sessions of 30 — T2 and T3 not met, said why.
    through = date(2026, 11, 24)
    rows = [r for r in layer_runs.read_runs() if r.session_date <= through]
    t = reliability.reliability(rows, set(), through=through)["thresholds"]
    for name in ("T2_min_availability_pct", "T3_max_streak_sessions"):
        assert t[name]["met"] is False
        assert all(f"{layer} (8 of 30)" in t[name]["reason"] for layer in config.AVAILABILITY_BASKET)


def test_a_layer_with_no_row_at_all_keeps_the_whole_window() -> None:
    # #638 decision 2: silence over the whole record is the layer going dark,
    # not a later start — 0 % over every session, never a blank.
    sessions = _weekdays(MON, 5)
    rows = [_row(s, "dce", "success") for s in sessions]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce", "safex"))
    safex = _layer(block, "safex")
    assert safex["sessions"] == 5
    assert safex["availability_pct"] == 0.0
    assert safex["first_run_availability_pct"] == 0.0


def test_first_run_availability_shares_the_layers_own_denominator() -> None:
    # #638 decision 3. safex joins on the fifth of eight sessions; its first
    # run there fails and a retry recovers it: 4 of 4 available, 3 of 4 on
    # the first run — not 4 and 3 of the window's 8.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions]
    rows += [_row(sessions[4], "safex", "failed", hour=20), _row(sessions[4], "safex", "success", hour=22)]
    rows += [_row(s, "safex", "success") for s in sessions[5:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",))
    safex = _layer(block, "safex")
    assert safex["sessions"] == 4
    assert safex["availability_pct"] == 100.0
    assert safex["first_run_availability_pct"] == 75.0


def test_t2_and_t3_are_not_met_while_a_basket_layer_has_fewer_than_a_window_of_its_own() -> None:
    # #638 decision 1, the session floor: a basket layer that joined two
    # sessions ago reads 2 of 2 = 100 % with no streak. Without the floor it
    # would meet T2 and T3 on two sessions while T1 is met by the older record.
    sessions = _weekdays(MON, config.RELIABILITY_WINDOW_SESSIONS)
    rows = [_row(s, "forward_curve", "success") for s in sessions]
    rows += [_row(s, "dce", "success") for s in sessions[-2:]]
    block = reliability.reliability(
        rows, set(), through=sessions[-1], layers=("forward_curve", "dce"), basket=("forward_curve", "dce"),
    )
    assert _layer(block, "dce")["sessions"] == 2
    assert _layer(block, "dce")["availability_pct"] == 100.0
    t = block["thresholds"]
    assert t["T1_continuous"]["met"] is True
    for name in ("T2_min_availability_pct", "T3_max_streak_sessions"):
        assert t[name]["met"] is False
        assert t[name]["floor_sessions"] == config.RELIABILITY_WINDOW_SESSIONS
        assert "dce" in t[name]["reason"] and "2 of" in t[name]["reason"]
        assert "forward_curve" not in t[name]["reason"]


def test_t2_and_t3_floor_holds_on_a_short_window_too() -> None:
    # The floor is the target window, not the window this cut happens to
    # have: after a restart every layer's own count is short, so neither
    # threshold can be met yet, however clean the sessions so far.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    t = block["thresholds"]
    assert t["T2_min_availability_pct"]["met"] is False
    assert t["T3_max_streak_sessions"]["met"] is False
    assert "8 of" in t["T2_min_availability_pct"]["reason"]


def test_skipped_only_streak_with_no_run_gap_is_not_dead() -> None:
    # A weekday on which nothing ran at all asked no source anything: inside a
    # streak of `skipped_unconfigured` it is no more evidence than the skip.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions[:2] + sessions[6:]]
    rows += [_row(s, "matif", "skipped_unconfigured") for s in sessions[:2] + sessions[6:]]
    assert "matif" not in reliability.dead_layers(rows, layers=("matif", "dce"))


def test_a_layer_missing_from_a_run_is_still_a_real_verdict() -> None:
    # A session where the pipeline ran but wrote no row for the layer is the
    # layer going silent, not "nothing asked": the exemption must not cover it.
    sessions = _weekdays(MON, 5)
    rows = [_row(s, "dce", "success") for s in sessions]
    rows += [_row(sessions[0], "matif", "skipped_unconfigured"),
             _row(sessions[4], "matif", "skipped_unconfigured")]
    dead = reliability.dead_layers(rows, layers=("matif", "dce"))
    assert set(dead) == {"matif"}
    assert dead["matif"].sessions == 5


def test_dead_streak_never_starts_before_the_layers_first_row() -> None:
    # A layer that joined the roster mid-record: the weekdays before its first
    # row are not its outage, so they neither count toward the threshold nor
    # date the streak.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions]
    assert reliability.dead_layers(
        rows + [_row(s, "safex", "failed") for s in sessions[6:]], layers=("safex",),
    ) == {}

    rows += [_row(s, "safex", "failed") for s in sessions[4:]]
    dead = reliability.dead_layers(rows, layers=("safex",))
    assert dead["safex"].sessions == 4
    assert dead["safex"].first_session == sessions[4]
    # The layer's own record cannot see before its first row either.
    assert dead["safex"].opens_at_record_start is True



def test_manifest_and_alert_agree_where_a_mid_record_layer_streak_starts() -> None:
    # #638: one rule site. safex joins on the fifth session and fails from
    # there; the manifest's open streak and the alert's streak are one figure.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions]
    rows += [_row(s, "safex", "failed") for s in sessions[4:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",))
    safex = block["layers"][0]
    dead = reliability.dead_layers(rows, threshold=1, layers=("safex",))
    assert safex["open_streak"] == dead["safex"].sessions == 4
    assert safex["longest_unavailable_streak"] == 4
    # Availability runs over the same four sessions (#638 owner decision).
    assert safex["sessions"] == 4
    assert safex["availability_pct"] == 0.0


def test_manifest_streak_start_comes_from_the_whole_record_not_the_window() -> None:
    # safex has a row before the window, then none on the window's first
    # session while the pipeline ran: that session is its streak, not a start
    # the window invented.
    sessions = _weekdays(MON, 8)
    rows = [_row(s, "dce", "success") for s in sessions]
    rows += [_row(sessions[0], "safex", "success")]
    rows += [_row(s, "safex", "failed") for s in sessions[5:]]
    block = reliability.reliability(rows, set(), through=sessions[-1], window=4, layers=("safex",))
    assert block["layers"][0]["open_streak"] == 4


def test_decision_cutoff_uses_one_promotion_and_keeps_all_sessions() -> None:
    friday = date(2026, 10, 9)
    friday, monday, tuesday, wednesday, thursday = _weekdays(friday, 5)
    first = _row(friday, "gulf_bids", "success", run="friday-1")
    second = _row(friday, "currencies", "success", run="friday-2")
    monday_freight = replace(
        _row(monday, "gulf_bids", "success", run="monday"),
        rendered_at="2026-10-13T06:00:00Z",
    )
    monday_fx = replace(monday_freight, layer_name="currencies", status="no_publication")
    tuesday_late = replace(
        _row(tuesday, "gulf_bids", "success", run="tuesday"),
        rendered_at="2026-10-14T06:00:01Z",
    )
    report = reliability.decision_cutoff_report(
        [
            first, second, monday_freight, monday_fx, tuesday_late,
            _row(wednesday, "gulf_bids", "success", rendered=False),
        ],
        through=thursday,
        cutoff_time=time(9),
        cutoff_zone=ZoneInfo("Asia/Jerusalem"),
        basket=("gulf_bids", "currencies"),
        window=6,
    )
    assert [row["state"] for row in report["sessions"]] == [
        "no_record", "basket_incomplete", "basket_ready", "late",
        "no_promoted_edition", "no_promoted_edition",
    ]
    assert report["sessions"][1]["cutoff_at"] == "2026-10-12T09:00:00+03:00"
    assert report["sessions"][2]["run_id"] == "monday"
    assert report["sessions"][2]["layers"]["currencies"]["status"] == "no_publication"
    assert report["sessions"][3]["first_late_rendered_at"] == "2026-10-14T06:00:01Z"
    assert report["sessions_with_record_window"] == 5
    assert report["readiness_pct"] == 20


# ── any-success availability (owner decision 2026-11-25, #518) ─────────────


def test_a_later_failed_rerun_no_longer_turns_an_available_session_into_a_miss() -> None:
    # Run 44526078466: a midday push run failed after 2026-11-18's two
    # successes, and last-run-wins called the session a miss (PR #615).
    sessions = _weekdays(MON, 2)
    rows = [
        _row(sessions[0], "magyp_fob", "success", run="a", hour=10),
        _row(sessions[0], "magyp_fob", "success", run="b", hour=12),
        _row(sessions[0], "magyp_fob", "failed", run="push", hour=14),
        _row(sessions[1], "magyp_fob", "success", run="c", hour=10),
    ]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("magyp_fob",), basket=())
    magyp = _layer(block, "magyp_fob")
    assert magyp["availability_pct"] == 100.0
    assert magyp["first_run_availability_pct"] == 100.0
    assert magyp["longest_unavailable_streak"] == 0
    assert magyp["open_at_end"] is False


def test_failed_runs_stay_visible_per_layer_beside_the_forgiving_session_rule() -> None:
    # Owner, 2026-11-25: any-success must not hide run-level failures. Every
    # run that tried and did not deliver counts — the outage words plus a
    # crashed run (`not_run`) — and the ones any-success forgave are named.
    # `skipped_unconfigured` and `disabled` never asked the source: not failures.
    sessions = _weekdays(MON, 3)
    rows = [
        _row(sessions[0], "safex", "success", run="a", hour=10),
        _row(sessions[0], "safex", "stale", run="b", hour=14),      # forgiven
        _row(sessions[1], "safex", "failed", run="c", hour=10),
        _row(sessions[1], "safex", "not_run", run="d", hour=14),    # crashed run
        _row(sessions[2], "safex", "skipped_unconfigured", run="e", hour=10),
        _row(sessions[2], "safex", "disabled", run="f", hour=12),
        _row(sessions[2], "safex", "usable_partial", run="g", hour=14),
    ]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("safex",), basket=())
    safex = _layer(block, "safex")
    assert safex["availability_pct"] == 66.7
    assert safex["failed_runs"] == 3
    assert safex["failed_runs_on_available_sessions"] == 1
    assert safex["failing_runs"] == 2  # T4's denominator: outage words only, unchanged


def test_a_session_with_any_success_breaks_a_dead_streak() -> None:
    # The relay reads the same session rule: a source that answered once on a
    # session was not dead on it, whatever a later re-run graded.
    sessions = _weekdays(MON, 3)
    rows = [_row(s, "safex", "failed", run=f"{s}-a", hour=10) for s in sessions]
    rows.append(_row(sessions[1], "safex", "success", run="mid", hour=12))
    rows.append(_row(sessions[1], "safex", "failed", run="push", hour=14))
    assert reliability.dead_layers(rows, layers=("safex",)) == {}


def test_a_session_whose_every_run_failed_still_counts_toward_dead() -> None:
    sessions = _weekdays(MON, 3)
    rows = [_row(s, "safex", "failed", run=f"{s}-{h}", hour=h) for s in sessions for h in (10, 14)]
    dead = reliability.dead_layers(rows, layers=("safex",))
    assert dead["safex"].sessions == 3
    assert dead["safex"].last_status == "failed"


def test_a_real_failure_under_a_later_unconfigured_skip_still_counts_toward_dead() -> None:
    # One real verdict inside the streak and it counts: a session is "only
    # skipped" when every run on it was, not when its newest run was.
    sessions = _weekdays(MON, 3)
    rows = []
    for s in sessions:
        rows.append(_row(s, "safex", "failed", run=f"{s}-a", hour=10))
        rows.append(_row(s, "safex", "skipped_unconfigured", run=f"{s}-b", hour=14))
    assert set(reliability.dead_layers(rows, layers=("safex",))) == {"safex"}


# ── the render-time mirror of the session rule (#632, owner option B on #518) ──
# data_freshness keeps only the latest attempt and the last usable run; the two
# readers that react to a failed latest run ask this one question of them.


def test_a_failed_rerun_after_a_usable_run_on_the_same_session_is_recovered() -> None:
    # magyp_fob, session 2026-11-18 (#518 wave-4 review): success 11:04 UTC on
    # 11-19, push run 44526078466 failed 12:50 UTC — both serve the 11-18
    # session, under the 14:30 CT cutoff and under #627's New York midnight.
    assert reliability.recovered_in_session("2026-11-19 11:04:00", "2026-11-19 12:50:00") is True


def test_a_usable_run_on_an_earlier_session_does_not_recover_this_one() -> None:
    # 11:04 UTC on 11-18 serves the 11-17 session; the 12:50 UTC 11-19 failure
    # serves 11-18, on which nothing usable was stored.
    assert reliability.recovered_in_session("2026-11-18 11:04:00", "2026-11-19 12:50:00") is False


def test_the_settlement_guards_session_roll_separates_sessions() -> None:
    # The rule has no clock of its own: it rolls exactly where the guard's
    # EXCHANGE_SESSION does (14:30 CT before #627, New York midnight after), so
    # the edge is found from the guard rather than hard-coded (#632 review).
    from datetime import datetime, timedelta, timezone

    from pipeline.layer_runs import session_date_for

    start = datetime(2026, 11, 19, 12, 0, tzinfo=timezone.utc)
    minutes = [start + timedelta(minutes=m) for m in range(24 * 60)]
    rolls = [b for a, b in zip(minutes, minutes[1:], strict=False) if session_date_for(a) != session_date_for(b)]
    assert len(rolls) == 1, rolls
    edge = rolls[0]

    def stamp(moment: datetime) -> str:
        return moment.strftime("%Y-%m-%d %H:%M:%S")

    before, after = edge - timedelta(minutes=1), edge + timedelta(minutes=1)
    assert reliability.recovered_in_session(stamp(before), stamp(after)) is False
    assert reliability.recovered_in_session(stamp(edge), stamp(edge + timedelta(hours=2))) is True


def test_never_succeeded_or_never_attempted_is_not_recovered() -> None:
    assert reliability.recovered_in_session(None, "2026-11-19 12:50:00") is False
    assert reliability.recovered_in_session("2026-11-19 11:04:00", None) is False


def test_an_unreadable_stamp_is_a_crash_not_a_guess() -> None:
    with pytest.raises(ValueError):
        reliability.recovered_in_session("yesterday", "2026-11-19 12:50:00")
