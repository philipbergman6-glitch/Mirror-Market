"""Source reliability figures over the layer_runs record (B34 #520).

One computation, two consumers: ``reliability`` is the block
``app/manifest.py`` publishes beside ``coverage`` and ``latency`` (A11 reads
it from the edition, not a side script), and ``dead_layers`` is the streak
``scripts/ci_dead_source.py`` relays. Requirements:
``docs/plans/source-reliability-a19.md`` §3–§4.

The unit is the **session** (``layer_runs.session_date``, weekdays only),
never the run. A session is available for a layer if *any* of its runs
graded available (owner decision 2026-11-25, #518; it was last-run-wins
until then): a later failed re-run, e.g. a midday push run from a merge, no
longer turns the session into a miss. Run-level failures stay visible as
their own figure — ``failed_runs`` per layer, and how many of them fell on
an available session — beside first-run availability, which shows how much
the retry carries, and surfaced %, which is per failing run.

Consumers, and which verdict each reads (#518):

- ``reliability`` (manifest block; availability %, streaks, median recovery,
  open-at-end, T2/T3): the session rule above.
- ``dead_layers`` (``scripts/ci_dead_source.py``): the same session rule. A
  session whose every run failed is still down and extends the streak; a
  session with any success means the source answered, so it is not dead.
- First-run %, ``failed_runs``, ``failing_runs`` and T4 surfaced %: run level,
  unchanged by the rule.
- ``decision_cutoff_report``: per promoted edition by design (the whole
  basket in one run by the cutoff), so neither session rule applies.
A weekday inside the record with no row for a layer is ``no_run`` and
counts as unavailable — absence is never a quiet day (invariant 1). The
record's first session bounds every window: before it there is no record,
and no record is not an outage. A gap longer than T1 tolerates restarts the
window after it — the cut never trims a gap away (#533).

Where a layer's *streak* may start is one rule, ``streak_sessions``: never
before the layer's own first row, so a layer that joined the roster
mid-record has no streak from the weekdays before it (#633). ``dead_layers``
and the manifest's streak figures (``open_streak``,
``longest_unavailable_streak``, ``median_recovery_sessions``) both cut through
it (#638); the manifest's availability percentages keep the whole window.

Thresholds T1–T4 are echoed from ``config`` with whether this window meets
them. They are reported, never gating: nothing here fails a run.
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import config
from pipeline import grading
from pipeline.layer_runs import STATUS_NOT_RUN, STATUS_SKIPPED_UNCONFIGURED, LayerRun

#: §3: a session is available when its last run graded one of these. Every
#: other word — ``stale``, ``failed``, ``incomplete``, ``disabled``,
#: ``skipped_unconfigured``, ``not_run`` and the computed ``no_run`` — is not.
AVAILABLE_STATUSES: frozenset[str] = frozenset({"success", "usable_partial", "no_publication"})

#: The computed word for a weekday with no row. Never stored.
NO_RUN = "no_run"

#: A run that asked the source and did not deliver: the outage words plus a
#: run that crashed before grading (``not_run``). ``skipped_unconfigured`` and
#: ``disabled`` never asked, so they are unavailable but not failed. Counted
#: per layer as ``failed_runs`` so any-success availability hides nothing.
FAILED_RUN_STATUSES: frozenset[str] = grading.OUTAGE_STATUSES | {STATUS_NOT_RUN}


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


def decision_cutoff_report(
    rows: Sequence[LayerRun],
    *,
    through: date,
    cutoff_time: time,
    cutoff_zone: ZoneInfo,
    basket: Sequence[str] = config.AVAILABILITY_BASKET,
    window: int = config.RELIABILITY_WINDOW_SESSIONS,
) -> dict[str, Any]:
    """Check recorded promotion evidence against the next weekday desk cutoff.

    Require the whole basket in one promoted run. The run record timestamps
    edition generation, so an on-time result is evidence of generation and
    eventual promotion. It cannot measure when the public URL became reachable.
    """
    if window < 1 or not basket or len(set(basket)) != len(basket):
        raise ValueError("require a positive window and a unique, nonempty basket")
    if cutoff_time.tzinfo is not None:
        raise ValueError("cutoff time must be local to the supplied time zone")

    sessions = [through if through.weekday() < 5 else previous_session(through)]
    for _ in range(window - 1):
        sessions.append(previous_session(sessions[-1]))
    sessions.reverse()

    grouped: dict[date, dict[tuple[str, str], dict[str, LayerRun]]] = {}
    for row in rows:
        if sessions[0] <= row.session_date <= sessions[-1] and row.rendered_at:
            key = (row.rendered_at, row.run_id)
            grouped.setdefault(row.session_date, {}).setdefault(key, {})[row.layer_name] = row
    record_start = min((row.session_date for row in rows), default=None)

    results = []
    for session in sessions:
        desk_day = session + timedelta(days=1)
        while desk_day.weekday() >= 5:
            desk_day += timedelta(days=1)
        cutoff = datetime.combine(desk_day, cutoff_time, cutoff_zone)
        promoted = sorted(
            grouped.get(session, {}).items(),
            key=lambda edition: datetime.fromisoformat(edition[0][0]),
        )
        by_cutoff = [edition for edition in promoted if datetime.fromisoformat(edition[0][0]) <= cutoff]
        ready_by_cutoff = [
            edition for edition in by_cutoff
            if all(layer in edition[1] and edition[1][layer].status in AVAILABLE_STATUSES for layer in basket)
        ]
        evidence = ready_by_cutoff[-1] if ready_by_cutoff else by_cutoff[-1] if by_cutoff else None
        if evidence is None:
            if record_start is None or session < record_start:
                state = "no_record"
            else:
                state = "late" if promoted else "no_promoted_edition"
        else:
            state = "basket_ready" if ready_by_cutoff else "basket_incomplete"
        layers = evidence[1] if evidence is not None else {}
        results.append({
            "session_date": session.isoformat(),
            "cutoff_at": cutoff.isoformat(),
            "state": state,
            "run_id": evidence[0][1] if evidence is not None else None,
            "rendered_at": evidence[0][0] if evidence is not None else None,
            "first_late_rendered_at": next(
                (key[0] for key, _ in promoted if datetime.fromisoformat(key[0]) > cutoff), None,
            ),
            "layers": {
                layer: {
                    "status": layers[layer].status if layer in layers else None,
                    "observed_at": layers[layer].observed_at if layer in layers else None,
                }
                for layer in basket
            },
        })
    recorded = sum(result["state"] != "no_record" for result in results)
    ready = sum(result["state"] == "basket_ready" for result in results)
    return {
        "evidence": "promoted edition generation time",
        "cutoff_calendar": "next weekday after the recorded market session",
        "basket": list(basket),
        "sessions_with_record_window": recorded,
        "sessions_ready_by_cutoff": ready,
        "readiness_pct": _pct(ready, recorded),
        "sessions": results,
    }


def _by_layer_session(rows: Iterable[LayerRun]) -> dict[str, dict[date, list[LayerRun]]]:
    out: dict[str, dict[date, list[LayerRun]]] = {}
    for row in rows:
        out.setdefault(row.layer_name, {}).setdefault(row.session_date, []).append(row)
    for sessions in out.values():
        for runs in sessions.values():
            runs.sort(key=lambda r: (r.run_started_at, r.run_id))
    return out


def _session_status(runs: Sequence[LayerRun]) -> str:
    """The session's verdict word: available if *any* run was (owner, 2026-11-25).

    The newest available run's word when one exists, so a later failed re-run
    never turns the session into a miss; otherwise the newest run's word, so
    a session whose every run failed still reads as the failure it was.
    """
    available = [r.status for r in runs if r.status in AVAILABLE_STATUSES]
    return available[-1] if available else runs[-1].status


def _verdicts(runs_by_session: dict[date, list[LayerRun]], sessions: Sequence[date]) -> list[tuple[str, str]]:
    """``(first_status, session_status)`` per session, ``no_run`` where none."""
    out = []
    for session in sessions:
        runs = runs_by_session.get(session)
        out.append((runs[0].status, _session_status(runs)) if runs else (NO_RUN, NO_RUN))
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
    found, current = None, 0
    for i, session in enumerate(every):
        if session in logged:
            if current > tolerated:
                found = (i, current)
            current = 0
        else:
            current += 1
    return (len(every), current) if current > tolerated else found


def layer_first_sessions(rows: Iterable[LayerRun]) -> dict[str, date]:
    """Each layer's first recorded session over ``rows`` — its own record start."""
    out: dict[str, date] = {}
    for row in rows:
        if row.layer_name not in out or row.session_date < out[row.layer_name]:
            out[row.layer_name] = row.session_date
    return out


def streak_sessions(sessions: Sequence[date], first_recorded: date | None) -> list[date]:
    """The sessions a layer's streak may run over: the one rule (#633, #638).

    Sessions before the layer's first row say nothing about it — the roster
    did not carry it yet — so they neither count toward a streak nor date
    one. A layer with no row at all keeps every session: its absence from the
    whole record is the layer going silent, not a later start. Both
    ``dead_layers`` (the alert) and ``reliability`` (the manifest's streak
    figures) cut through here, so the two never disagree on where a layer's
    streak can begin. ``first_recorded`` must come from the layer's whole
    record, never a window of it: a window's first row for the layer is not
    where its record began.
    """
    return [s for s in sessions if first_recorded is None or s >= first_recorded]


def _pct(numerator: int, denominator: int) -> float | None:
    return round(100.0 * numerator / denominator, 1) if denominator else None


@dataclass(frozen=True)
class Streak:
    """An open unavailable streak, counted in sessions.

    ``opens_at_record_start`` marks a streak that runs from the first session
    of the record it was counted over: the record cannot see what came
    before, so ``sessions`` is a floor and ``first_session`` is where the
    record begins, not where the outage did (#566 — dark since 2026-10-09,
    record from 2026-10-16). For ``dead_layers`` that record is the layer's
    own, from its first row (#633).
    """

    layer: str
    sessions: int
    first_session: date
    last_session: date
    last_status: str
    opens_at_record_start: bool


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
        opens_at_record_start=length == len(sessions),
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
        # No logged session after the gap yet: no figures to compute, but T1
        # is answered — it is not met — and the gap is stated, not just told.
        return {
            "state": "restarting",
            "reason": (
                f"no run on the {gap[1]} sessions {opened.isoformat()}–{through.isoformat()} "
                f"(more than {tolerated}): the evidence window restarts at the next logged session"
            ),
            "data": {
                "window": {
                    "target_sessions": window,
                    "record_started": record_started.isoformat(),
                    "restarted_at": None,
                    "restart_gap": gap[1],
                    "gap_first_session": opened.isoformat(),
                    "evidence_sessions": 0,
                },
                "thresholds": {"T1_continuous": {"value": tolerated, "met": False}},
            },
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
    first_recorded = layer_first_sessions(in_scope)
    for layer in layers:
        verdicts = _verdicts(grouped.get(layer, {}), sessions)
        down = [last not in AVAILABLE_STATUSES for _first, last in verdicts]
        # Streak figures run over the sessions the one rule allows (#638): the
        # window, from the layer's own first row when that is later. The
        # availability percentages keep the whole window.
        own = streak_sessions(sessions, first_recorded.get(layer))
        skip = len(sessions) - len(own)
        streaks = _streaks(down[skip:])
        closed = streaks[:-1] if down and down[-1] else streaks
        failing = [
            r for runs in grouped.get(layer, {}).values() for r in runs
            if r.status in grading.OUTAGE_STATUSES
        ]
        surfaced = sum(1 for r in failing if (r.run_id, layer) in alerts)
        failed = [
            (session, r) for session, runs in grouped.get(layer, {}).items() for r in runs
            if r.status in FAILED_RUN_STATUSES
        ]
        available_sessions = {s for s, (_first, status) in zip(sessions, verdicts, strict=True)
                              if status in AVAILABLE_STATUSES}
        open_streak = _open_streak(layer, verdicts[skip:], own)
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
            # Run level, beside the session rule (owner 2026-11-25): every
            # failed run, and those a success on the same session outweighed.
            "failed_runs": len(failed),
            "failed_runs_on_available_sessions": sum(1 for s, _r in failed if s in available_sessions),
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
                "value": tolerated,
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

    Counted from the layer's own first recorded session (the record's first,
    for a layer with no row at all) through the record's newest, so a
    trailing run of weekdays with no row for it extends a streak (``no_run``)
    while the time before the layer's record began never does (#633).
    """
    threshold = config.DEAD_SOURCE_SESSIONS if threshold is None else threshold
    layers = tuple(config.PRODUCTION_LAYER_KEYS if layers is None else layers)
    if not rows:
        return {}
    sessions = weekday_sessions(min(r.session_date for r in rows), max(r.session_date for r in rows))
    ran = {r.session_date for r in rows}
    grouped = _by_layer_session(rows)
    first_recorded = layer_first_sessions(rows)
    out = {}
    for layer in layers:
        recorded = grouped.get(layer, {})
        # #633: matif's first row is 2026-11-05, not the record's 2026-10-16.
        own = streak_sessions(sessions, first_recorded.get(layer))
        verdicts = _verdicts(recorded, own)
        streak = _open_streak(layer, verdicts, own)
        if streak is None or streak.sessions < threshold:
            continue
        # A session the run was never configured to ask about says nothing
        # about the source: a streak made only of `skipped_unconfigured`
        # (Layer 37 on CI, by design) is unavailable but not dead. The
        # session's every run is read, not just its verdict word (#518): one
        # failed run under a later skip is still a real verdict. Nor does a
        # weekday on which nothing ran at all — no layer has a row — so such a
        # gap inside a skipped streak keeps the exemption (#633). A weekday the
        # pipeline ran without a row for this layer is the layer going silent,
        # a real verdict; one real verdict inside the streak and it counts in full.
        if all(
            all(r.status == STATUS_SKIPPED_UNCONFIGURED for r in recorded[s])
            if s in recorded else s not in ran
            for s in own[-streak.sessions:]
        ):
            continue
        out[layer] = streak
    return out
