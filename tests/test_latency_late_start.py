"""A breach GitHub's scheduler caused is reported, not failed — when the run opts in.

Replays the 2026-10-09 run: the 20:07 UTC Refresh Prices cron started at
00:16 UTC, six hours after the 18:15 UTC CBOT settlement, so the board
objective had lapsed before a single byte was fetched. The deploy succeeded
and the bar was the right one; the red said nothing a code change could fix.
Three things keep the excuse narrow: the bar must be the newest session as of
the fetch start (a stale upstream is still a breach), our own pipeline leg must
meet its target, and only a run that passes the flag gets it — the 08:00 DCE
backstop is red by design when the outside trigger did not fire.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from latency.domain import LAYER_LATENCY_BY_KEY, started_after_objective

UTC = timezone.utc
PRICES = LAYER_LATENCY_BY_KEY["prices"]
SETTLED_OCT_8 = datetime(2026, 10, 8, 18, 15, tzinfo=UTC)   # 13:15 CT
SETTLED_OCT_7 = datetime(2026, 10, 7, 18, 15, tzinfo=UTC)
LATE_START = datetime(2026, 10, 9, 0, 17, tzinfo=UTC)


def test_a_fetch_that_began_past_the_objective_on_the_newest_bar_started_late():
    assert started_after_objective(PRICES, SETTLED_OCT_8, LATE_START) is True


def test_a_fetch_that_began_inside_the_objective_did_not():
    on_time = datetime(2026, 10, 8, 23, 0, tzinfo=UTC)
    assert started_after_objective(PRICES, SETTLED_OCT_8, on_time) is False


def test_a_stale_bar_is_the_data_not_the_scheduler():
    """Holding Oct 7's bar when Oct 8 had settled is an upstream problem."""
    assert started_after_objective(PRICES, SETTLED_OCT_7, LATE_START) is False


def test_missing_stamps_never_excuse():
    assert started_after_objective(PRICES, None, LATE_START) is False
    assert started_after_objective(PRICES, SETTLED_OCT_8, None) is False


def test_a_day_granular_clock_never_excuses():
    """No session close to measure from means no proof of a late start."""
    cot = LAYER_LATENCY_BY_KEY["cot"]
    assert started_after_objective(cot, SETTLED_OCT_8, LATE_START) is False


# ---------------------------------------------------------------------------
# The CI gate
# ---------------------------------------------------------------------------

def _row(layer, observed, started, *, pipeline="meets"):
    return {
        "layer": layer, "class": "board_price", "verdict": "breaches",
        "acquisition_verdict": "breaches", "pipeline_verdict": pipeline,
        "observed_at": observed.isoformat(), "fetch_started_at": started.isoformat(),
    }


def _manifest(tmp_path, rows):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({
        "edition": {"mode": "fast", "generated_at": "2026-10-09T00:21:00+00:00"},
        "latency": {"layers": rows},
    }), encoding="utf-8")
    return str(path)


def test_the_opted_in_gate_passes_a_late_start_and_says_why(tmp_path, capsys):
    from scripts.latency_report import main

    path = _manifest(tmp_path, [_row("prices", SETTLED_OCT_8, LATE_START)])
    assert main(["--manifest", path, "--fail-on-breach-layers", "prices",
                 "--late-start-is-not-a-breach"]) == 0
    assert "LATE START: prices" in capsys.readouterr().err


def test_without_the_flag_the_same_run_is_still_red(tmp_path):
    from scripts.latency_report import main

    path = _manifest(tmp_path, [_row("prices", SETTLED_OCT_8, LATE_START)])
    assert main(["--manifest", path, "--fail-on-breach-layers", "prices"]) == 1


def test_a_stale_bar_fails_even_with_the_flag(tmp_path):
    from scripts.latency_report import main

    path = _manifest(tmp_path, [_row("prices", SETTLED_OCT_7, LATE_START)])
    assert main(["--manifest", path, "--fail-on-breach-layers", "prices",
                 "--late-start-is-not-a-breach"]) == 1


def test_a_pipeline_breach_fails_even_with_the_flag(tmp_path):
    from scripts.latency_report import main

    path = _manifest(tmp_path, [_row("prices", SETTLED_OCT_8, LATE_START, pipeline="breaches")])
    assert main(["--manifest", path, "--fail-on-breach-layers", "prices",
                 "--late-start-is-not-a-breach"]) == 1


def test_the_published_manifest_shape_with_an_unmeasured_pipeline_is_excused(tmp_path):
    """The live manifest is written before its own deploy: pipeline reads unknown."""
    from scripts.latency_report import main

    path = _manifest(tmp_path, [_row("prices", SETTLED_OCT_8, LATE_START, pipeline="unknown")])
    assert main(["--manifest", path, "--fail-on-breach-layers", "prices",
                 "--late-start-is-not-a-breach"]) == 0
