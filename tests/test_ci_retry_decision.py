"""Tests for the same-day retry decision (scripts/ci_retry_decision.py).

The gh CLI is never called: `decide()` is a pure function over the run list
the API returns plus a `status_of` lookup, so every branch is exercised here
without a network. What is NOT under test is the dispatch itself — that is
three lines of subprocess in `main()`.

The invariant these tests exist to pin: a retry fires only when the day's
last completed pipeline run hard-failed, and never twice in one day.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from scripts import ci_retry_decision as retry

# The nominal decision moment: 20:40 UTC cron, landed with typical drift.
NOW = datetime(2026, 8, 21, 22, 30, tzinfo=timezone.utc)


def _run(
    run_id: int,
    *,
    event: str = "schedule",
    status: str = "completed",
    conclusion: str | None = "success",
    created_at: str = "2026-08-21T21:04:00Z",
) -> dict:
    return {
        "databaseId": run_id,
        "event": event,
        "status": status,
        "conclusion": conclusion,
        "createdAt": created_at,
    }


def _decide(runs, status_by_id=None, now=NOW):
    statuses = status_by_id or {}
    return retry.decide(
        runs=[retry.Run.from_api(r) for r in runs],
        now=now,
        status_of=lambda run_id: statuses.get(run_id),
    )


# ── the retry case ───────────────────────────────────────────────────


def test_hard_failures_in_the_days_last_run_trigger_a_retry():
    decision = _decide(
        [_run(1)],
        {1: {"mode": "full", "hard_failures": ["gulf_bids"], "succeeded": ["prices"]}},
    )

    assert decision.retry is True
    assert decision.failed_run_id == 1
    assert "gulf_bids" in decision.reason


def test_missing_status_file_is_treated_as_a_crash_and_retried():
    """No artifact means main.py never finished — the alerter's own rule."""
    decision = _decide([_run(1)], {})

    assert decision.retry is True
    assert decision.failed_run_id == 1
    assert "no run summary" in decision.reason


def test_a_failed_run_is_retried_even_when_a_later_run_is_still_queued():
    """The queued run is a push build; the day's evidence is the completed one."""
    decision = _decide(
        [
            _run(1, created_at="2026-08-21T21:04:00Z"),
            _run(2, event="push", status="queued", conclusion=None,
                 created_at="2026-08-21T21:40:00Z"),
        ],
        {1: {"mode": "full", "hard_failures": ["mandi_prices"]}},
    )

    assert decision.retry is True
    assert decision.failed_run_id == 1


# ── the no-retry cases ───────────────────────────────────────────────


def test_a_clean_run_triggers_no_retry():
    decision = _decide([_run(1)], {1: {"mode": "full", "hard_failures": []}})

    assert decision.retry is False
    assert "no hard failures" in decision.reason


def test_the_days_last_completed_run_wins_over_an_earlier_failure():
    """A push build that recovered the layers leaves nothing to retry."""
    decision = _decide(
        [
            _run(1, created_at="2026-08-21T20:10:00Z"),
            _run(2, event="push", created_at="2026-08-21T22:30:00Z"),
        ],
        {
            1: {"mode": "full", "hard_failures": ["gulf_bids"]},
            2: {"mode": "full", "hard_failures": []},
        },
    )

    assert decision.retry is False


def test_no_run_today_triggers_no_retry():
    """Yesterday's failure is not today's — its snapshot day is already gone."""
    decision = _decide(
        [_run(1, created_at="2026-08-20T21:04:00Z")],
        {1: {"mode": "full", "hard_failures": ["gulf_bids"]}},
    )

    assert decision.retry is False
    assert "no pipeline run has completed today" in decision.reason


def test_an_in_flight_primary_defers_rather_than_retrying():
    """The scheduler put us ahead of the daily run; it has not had its shot."""
    decision = _decide(
        [_run(1, status="in_progress", conclusion=None)],
        {},
    )

    assert decision.retry is False
    assert "still in flight" in decision.reason


def test_cancelled_runs_are_not_evidence_of_anything():
    """cancel-in-progress kills push runs mid-pipeline; no status file results."""
    decision = _decide(
        [
            _run(1, event="push", conclusion="cancelled",
                 created_at="2026-08-21T22:30:00Z"),
            _run(2, created_at="2026-08-21T21:04:00Z"),
        ],
        {2: {"mode": "full", "hard_failures": []}},
    )

    assert decision.retry is False
    assert "no hard failures" in decision.reason


def test_a_fast_refresh_summary_is_not_evidence_about_the_daily_layers():
    """A fast run never asks for the snapshot-only layers, so it cannot judge them."""
    decision = _decide(
        [_run(1)],
        {1: {"mode": "fast", "hard_failures": ["gulf_bids"]}},
    )

    assert decision.retry is False
    assert "fast" in decision.reason


# ── the no-chain bound ───────────────────────────────────────────────


@pytest.mark.parametrize("conclusion", ["success", "failure"])
def test_a_retry_run_today_blocks_any_further_retry(conclusion):
    """Structural: the marker event is visible in the run list, so a retry
    that itself hard-fails cannot produce a second one."""
    decision = _decide(
        [
            _run(1, created_at="2026-08-21T21:04:00Z"),
            _run(2, event=retry.RETRY_EVENT, conclusion=conclusion,
                 created_at="2026-08-21T22:50:00Z"),
        ],
        {
            1: {"mode": "full", "hard_failures": ["gulf_bids"]},
            2: {"mode": "full", "hard_failures": ["gulf_bids"]},
        },
    )

    assert decision.retry is False
    assert "already" in decision.reason


def test_yesterdays_retry_does_not_block_todays():
    decision = _decide(
        [
            _run(1, event=retry.RETRY_EVENT, created_at="2026-08-20T22:50:00Z"),
            _run(2, created_at="2026-08-21T21:04:00Z"),
        ],
        {2: {"mode": "full", "hard_failures": ["gulf_bids"]}},
    )

    assert decision.retry is True
    assert decision.failed_run_id == 2


# ── the midnight cutoff ──────────────────────────────────────────────


def test_a_late_landing_decision_refuses_rather_than_spilling_past_midnight():
    """Two harms avoided: a run that cannot recover the day, and a marker run
    dated tomorrow that would consume tomorrow's one retry."""
    decision = _decide(
        [_run(1)],
        {1: {"mode": "full", "hard_failures": ["gulf_bids"]}},
        now=datetime(2026, 8, 21, 23, 45, tzinfo=timezone.utc),
    )

    assert decision.retry is False
    assert "too late" in decision.reason


def test_just_inside_the_cutoff_still_retries():
    decision = _decide(
        [_run(1)],
        {1: {"mode": "full", "hard_failures": ["gulf_bids"]}},
        now=datetime(2026, 8, 21, 23, 19, tzinfo=timezone.utc),
    )

    assert decision.retry is True


# ── parsing ──────────────────────────────────────────────────────────


def test_run_from_api_rejects_an_unparseable_timestamp():
    """Hard-fail rather than silently dropping a run out of `today`."""
    with pytest.raises(ValueError):
        retry.Run.from_api(_run(1, created_at="yesterday-ish"))
