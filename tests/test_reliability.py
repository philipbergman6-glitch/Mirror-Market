"""The source reliability figures (B34 #520 §3) and the dead-source streak (§4).

One function over the layer_runs store. These tests build rows in memory: the
computation never touches the filesystem, so no HISTORY_DIR is needed.
"""

from __future__ import annotations

from datetime import date, timedelta

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


def test_availability_basket_is_basket_v1s_eight_layers() -> None:
    # Pinned to docs/plans/source-reliability-a19.md §1: strata 1-6 and 9;
    # derived strata 7-8 have no layer of their own.
    assert config.AVAILABILITY_BASKET == (
        "forward_curve", "dce", "agrural", "cepea",
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
    assert _layer(block, "safex")["availability_pct"] == 80.0  # last run of Friday failed


def test_last_run_decides_availability_and_first_run_shows_the_retry() -> None:
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
    assert t["T4_surfaced_within_one_run_pct"]["met"] is False  # one unsurfaced failure


def test_t1_fails_on_a_short_record_or_a_long_gap() -> None:
    sessions = _weekdays(MON, 30)
    rows = [_row(s, "dce", "success") for s in sessions if s not in sessions[10:13]]
    block = reliability.reliability(rows, set(), through=sessions[-1], layers=("dce",), basket=("dce",))
    assert block["edition"]["longest_no_run_gap"] == 3
    assert block["thresholds"]["T1_continuous"]["met"] is False

    short = reliability.reliability(rows[:5], set(), through=sessions[4], layers=("dce",), basket=("dce",))
    assert short["thresholds"]["T1_continuous"]["met"] is False


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
