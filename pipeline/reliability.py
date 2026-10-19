"""Source reliability figures over the layer_runs record (B34 #520).

One computation, two consumers: ``reliability`` is the block
``app/manifest.py`` publishes beside ``coverage`` and ``latency`` (A11 reads
it from the edition, not a side script), and ``dead_layers`` is the streak
``scripts/ci_dead_source.py`` relays. Requirements:
``docs/plans/source-reliability-a19.md`` §3–§4.

The unit is the **session** (``layer_runs.session_date``, weekdays only),
never the run. A session's verdict for a layer is its *last* run's status;
the first run's is reported beside it to show how much the retry carries.
A weekday inside the record with no row for a layer is ``no_run`` and
counts as unavailable — absence is never a quiet day (invariant 1). The
record's first session bounds every window: before it there is no record,
and no record is not an outage. A gap longer than T1 tolerates restarts the
window after it — the cut never trims a gap away (#533).

Thresholds T1–T4 are echoed from ``config`` with whether this window meets
them. They are reported, never gating: nothing here fails a run.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import config
from pipeline import grading
from pipeline.layer_runs import LayerRun

#: §3: a session is available when its last run graded one of these. Every
#: other word — ``stale``, ``failed``, ``incomplete``, ``disabled``,
#: ``skipped_unconfigured``, ``not_run`` and the computed ``no_run`` — is not.
AVAILABLE_STATUSES: frozenset[str] = frozenset({"success", "usable_partial", "no_publication"})

#: The computed word for a weekday with no row. Never stored.
NO_RUN = "no_run"


def weekday_sessions(first: date, last: date) -> list[date]:
    out, day = [], first
    while day <= last:
        if day.weekday() < 5:
            out.append(day)
        day += timedelta(days=1)
    return out


def previous_session(session: date) -> date:
    day = session - timedelta(days=1)
    while day.weekday() >= 5:
        day -= timedelta(days=1)
    return day


def _by_layer_session(rows: Iterable[LayerRun]) -> dict[str, dict[date, list[LayerRun]]]:
    out: dict[str, dict[date, list[LayerRun]]] = {}
    for row in rows:
        out.setdefault(row.layer_name, {}).setdefault(row.session_date, []).append(row)
    for sessions in out.values():
        for runs in sessions.values():
            runs.sort(key=lambda r: (r.run_started_at, r.run_id))
    return out


def _verdicts(runs_by_session: dict[date, list[LayerRun]], sessions: Sequence[date]) -> list[tuple[str, str]]:
    """``(first_status, last_status)`` per session, ``no_run`` where none."""
    out = []
    for session in sessions:
        runs = runs_by_session.get(session)
        out.append((runs[0].status, runs[-1].status) if runs else (NO_RUN, NO_RUN))
    return out


def _streaks(down: Sequence[bool]) -> list[int]:
    """Lengths of consecutive-unavailable runs, in order."""
    out, current = [], 0
    for flag in down:
        if flag:
            current += 1
        elif current:
            out.append(current)
            current = 0
    if current:
        out.append(current)
    return out


def _last_disqualifying_gap(
    every: Sequence[date], logged: set[date], tolerated: int,
) -> tuple[int, int] | None:
    """``(index after, length)`` of the last run of more than ``tolerated``
    sessions with no row at all, or ``None``. An index of ``len(every)``
    means the gap is still open at the record's end."""
    found, run = None, 0
    for i, session in enumerate(every):
        if session in logged:
            if run > tolerated:
                found = (i, run)
            run = 0
        else:
            run += 1
    return (len(every), run) if run > tolerated else found


def _pct(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 1) if denominator else None


@dataclass(frozen=True)
class Streak:
    """An open unavailable streak, counted in sessions."""

    layer: str
    sessions: int
    first_session: date
    last_session: date
    last_status: str


def _open_streak(layer: str, verdicts: Sequence[tuple[str, str]], sessions: Sequence[date]) -> Streak | None:
    length = 0
    for _first, last in reversed(verdicts):
        if last in AVAILABLE_STATUSES:
            break
        length += 1
    if not length:
        return None
    return Streak(
        layer=layer, sessions=length, first_session=sessions[-length],
        last_session=sessions[-1], last_status=verdicts[-1][1],
    )


def reliability(
    rows: Sequence[LayerRun],
    alerts: set[tuple[str, str]],
    *,
    through: date,
    window: int | None = None,
    layers: Sequence[str] | None = None,
    basket: Sequence[str] | None = None,
) -> dict[str, Any]:
    """The §3 figures for the ``window`` weekday sessions ending at ``through``.

    A gap of more than ``RELIABILITY_T1_MAX_NO_RUN_GAP`` sessions with no run
    restarts the evidence window at the first logged session after it, and
    every figure (T1–T4) is computed over that restarted window. While such a
    gap is still open at ``through`` there is no evidence yet: ``restarting``.

    ``alerts`` are ``layer_alerts`` receipts: ``(run_id, layer)`` pairs a
    run's own ci-alert comment named. A layer with no failing run in the
    window has no surfaced figure (``None``), never a vacuous 100 %.
    """
    window = config.RELIABILITY_WINDOW_SESSIONS if window is None else window
    layers = tuple(config.PRODUCTION_LAYER_KEYS if layers is None else layers)
    basket = tuple(config.AVAILABILITY_BASKET if basket is None else basket)
    in_scope = [r for r in rows if r.session_date <= through]
    if not in_scope:
        return {
            "state": "no_record",
            "reason": f"layer_runs holds no run on or before {through.isoformat()} — the record has not begun",
        }

    record_started = min(r.session_date for r in in_scope)
    every = weekday_sessions(record_started, through)
    tolerated = config.RELIABILITY_T1_MAX_NO_RUN_GAP
    gap = _last_disqualifying_gap(every, {r.session_date for r in in_scope}, tolerated)
    if gap is not None and gap[0] == len(every):
        opened = every[-gap[1]]
        return {
            "state": "restarting",
            "reason": (
                f"no run on the {gap[1]} sessions {opened.isoformat()}–{through.isoformat()} "
                f"(more than {tolerated}): the evidence window restarts at the next logged session"
            ),
        }
    evidence = every[gap[0]:] if gap else every
    sessions = evidence[-window:]
    span = set(sessions)
    windowed = [r for r in in_scope if r.session_date in span]
    grouped = _by_layer_session(windowed)

    logged = {r.session_date for r in windowed}
    edition = {r.session_date for r in windowed if r.rendered_at}
    no_run_gap = max(_streaks([s not in logged for s in sessions]), default=0)

    out_layers: list[dict[str, Any]] = []
    for layer in layers:
        verdicts = _verdicts(grouped.get(layer, {}), sessions)
        down = [last not in AVAILABLE_STATUSES for _first, last in verdicts]
        streaks = _streaks(down)
        closed = streaks[:-1] if down and down[-1] else streaks
        failing = [
            r for runs in grouped.get(layer, {}).values() for r in runs
            if r.status in grading.OUTAGE_STATUSES
        ]
        surfaced = sum(1 for r in failing if (r.run_id, layer) in alerts)
        open_streak = _open_streak(layer, verdicts, sessions)
        out_layers.append({
            "layer": layer,
            "basket": layer in basket,
            "logged_sessions": sum(1 for s in sessions if s in grouped.get(layer, {})),
            "availability_pct": _pct(down.count(False), len(sessions)),
            "first_run_availability_pct": _pct(
                sum(1 for first, _last in verdicts if first in AVAILABLE_STATUSES), len(sessions),
            ),
            "longest_unavailable_streak": max(streaks, default=0),
            "median_recovery_sessions": float(statistics.median(closed)) if closed else None,
            "open_at_end": bool(down and down[-1]),
            "open_streak": open_streak.sessions if open_streak else 0,
            "failing_runs": len(failing),
            "surfaced_within_one_run_pct": _pct(surfaced, len(failing)),
        })

    in_basket = [entry for entry in out_layers if entry["basket"]]
    failing_total = sum(entry["failing_runs"] for entry in out_layers)
    surfaced_total = sum(
        1 for r in windowed
        if r.layer_name in layers and r.status in grading.OUTAGE_STATUSES
        and (r.run_id, r.layer_name) in alerts
    )
    surfaced_all = _pct(surfaced_total, failing_total)

    return {
        "state": "ok",
        "reason": None,
        "window": {
            "sessions": len(sessions),
            "target_sessions": window,
            "first_session": sessions[0].isoformat(),
            "last_session": sessions[-1].isoformat(),
            "record_started": record_started.isoformat(),
            # §3 T1: a gap longer than the tolerated one restarts the window
            # at the first logged session after it — never trimmed by the cut.
            "restarted_at": evidence[0].isoformat() if gap else None,
            "restart_gap": gap[1] if gap else None,
            "evidence_sessions": len(evidence),
        },
        "edition": {
            "sessions_with_run": len(logged),
            "sessions_with_edition": len(edition),
            "edition_pct": _pct(len(edition), len(sessions)),
            "longest_no_run_gap": no_run_gap,
        },
        "thresholds": {
            "note": "Pre-buyer, owner-revisable at A12 (#406). Reported for A11 (#315); never gates a run.",
            "T1_continuous": {
                "value": config.RELIABILITY_T1_MAX_NO_RUN_GAP,
                "met": len(evidence) >= window and no_run_gap <= tolerated,
            },
            "T2_min_availability_pct": {
                "value": config.RELIABILITY_T2_MIN_AVAILABILITY_PCT,
                "met": all(
                    (e["availability_pct"] or 0.0) >= config.RELIABILITY_T2_MIN_AVAILABILITY_PCT
                    for e in in_basket
                ),
            },
            "T3_max_streak_sessions": {
                "value": config.RELIABILITY_T3_MAX_STREAK_SESSIONS,
                # The alert clause of T3 is evidence on the ci-dead-source
                # issue, not in this store; only the streak half is checked.
                "met": all(
                    e["longest_unavailable_streak"] <= config.RELIABILITY_T3_MAX_STREAK_SESSIONS
                    for e in in_basket
                ),
            },
            "T4_surfaced_within_one_run_pct": {
                "value": config.RELIABILITY_T4_MIN_SURFACED_PCT,
                "observed": surfaced_all,
                "met": surfaced_all is None or surfaced_all >= config.RELIABILITY_T4_MIN_SURFACED_PCT,
            },
        },
        "layers": out_layers,
    }


def dead_layers(
    rows: Sequence[LayerRun],
    *,
    threshold: int | None = None,
    layers: Sequence[str] | None = None,
) -> dict[str, Streak]:
    """Layers unavailable on the last ``threshold``+ consecutive sessions.

    Counted from the record's first session through its newest, so a trailing
    run of weekdays with no run at all extends a streak (``no_run``) while
    the time before the record began never does.
    """
    threshold = config.DEAD_SOURCE_SESSIONS if threshold is None else threshold
    layers = tuple(config.PRODUCTION_LAYER_KEYS if layers is None else layers)
    if not rows:
        return {}
    sessions = weekday_sessions(min(r.session_date for r in rows), max(r.session_date for r in rows))
    grouped = _by_layer_session(rows)
    out = {}
    for layer in layers:
        streak = _open_streak(layer, _verdicts(grouped.get(layer, {}), sessions), sessions)
        if streak is not None and streak.sessions >= threshold:
            out[layer] = streak
    return out
