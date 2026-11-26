"""Decision readiness — three answers a buyer reads beside the layer count (#673).

"37/41 layers on schedule" is a pipeline-health number, and it stays on the
masthead for diagnosis. A physical buyer opening the morning read is asking
something else: *can I make today's decision off this page?* That question
has three parts, and each one is answered here by one builder:

``origin_comparison``  can at least two origins be compared FOB-equivalent for
                       the default destination and window — and if not, which
                       origin input is missing or too old, with its date.
``three_leg_close``    do beans, meal and oil share one session close
                       (invariant 8), and which session it is.
``critical_arrival``   did the board, the curve, FX and ocean freight land
                       within their objectives on the run that fetched them.

Each returns the ``{state, reason, data}`` envelope every headline section
uses. The states are the masthead's existing three tallies: ``ok`` (ready —
plain, like the on-schedule count), ``degraded`` (amber, ``.mast-partial``)
and ``not_ready`` (red, ``.mast-critical``). A non-ok state must carry a
reason naming the input responsible and its observation date; the envelope
refuses one without it.

No new vocabulary underneath. Every judgement here is read off an existing
seam: ``data_freshness.status`` (``pipeline.grading``), the latency
acquisition objectives and venue calendars (``latency.domain``), the origins
FOB board (``analysis.origins.comparison``), and the command center's
same-session crush (``analysis.soy_analytics.front_month_crush``). The one
threshold that is not a pipeline objective — how long a freight number stays
true for a decision — is the existing assumption-lifetime policy
(``config.ASSUMPTION_DEFAULT_LIFETIME_DAYS``), not a number invented here.

Public inputs only (invariant 4): the masthead is on the public headline and
briefing pages, so the origins ranking reads the committed policy tier and
nothing else.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterable, Mapping
from datetime import date, datetime
from typing import Any

import config
from analysis.origins.comparison import fob_board, fob_board_refusal, fob_eligible
from analysis.origins.domain import Comparability, Freshness, LandedCost, OriginRanking
from analysis.origins.landed_cost import bridge_components, window_verdict
from app.sections import section
from latency.domain import (
    Granularity,
    LatencyClass,
    LayerMeasurement,
    Verdict,
    format_delta,
    newest_session_at,
)
from pipeline.grading import ADVANCES_LAST_SUCCESS, OUTAGE_STATUSES, STATUS_USABLE_PARTIAL

log = logging.getLogger(__name__)

STATE_OK = "ok"
STATE_DEGRADED = "degraded"
STATE_NOT_READY = "not_ready"
STATES = (STATE_OK, STATE_DEGRADED, STATE_NOT_READY)

# The words the masthead prints for each state.
STATE_LABELS = {STATE_OK: "ready", STATE_DEGRADED: "degraded", STATE_NOT_READY: "not ready"}

# Worst first: an indicator's state is the worst of its inputs'.
_SEVERITY = {STATE_NOT_READY: 0, STATE_DEGRADED: 1, STATE_OK: 2}


def indicator(name: str, state: str, reason: str = "", /, **data: Any) -> dict:
    """The headline envelope (``app.sections.section``), restricted to three states.

    A non-ok indicator must say why — the reason should name the input
    responsible and its observation date where one exists.
    """
    if state not in STATES:
        raise ValueError(f"readiness indicator {name!r} has state {state!r}, not one of {STATES}")
    if state != STATE_OK and not reason.strip():
        raise ValueError(
            f"readiness indicator {name!r} is {state!r} with no reason — a non-ok "
            "indicator must name the input responsible and its observation date"
        )
    return section(state, reason, name=name, label=STATE_LABELS[state], **data)


def _worst(states: Iterable[str]) -> str:
    return min(states, key=_SEVERITY.__getitem__, default=STATE_OK)


# ---------------------------------------------------------------------------
# Indicator 3 — before-cutoff arrival
# ---------------------------------------------------------------------------

# The observations a cargo decision cannot be made without, in reading order,
# with the words the reason line uses for each.
ARRIVAL_INPUTS: tuple[tuple[str, str], ...] = (
    ("prices", "CBOT board"),
    ("forward_curve", "CBOT forward curve"),
    ("currencies", "FX"),
    ("gtr_ocean_freight", "ocean freight"),
)

# Inputs judged by how long their number stays true for a decision rather
# than by a latency acquisition objective. GTR ocean freight is a monthly
# broker assessment stamped to the first of the month: its acquisition
# (observation -> fetch) is ~30-60 days by construction, so the 24 h
# fundamentals objective breaches structurally on every run — the reason
# LATENCY.md §4 scopes the latency gate away from it. What a buyer needs to
# know instead is whether the freight number is still a usable one, and the
# repo already states how long that is: a freight indication holds a week.
DECISION_LIFE_DAYS: dict[str, int] = {
    "gtr_ocean_freight": config.ASSUMPTION_DEFAULT_LIFETIME_DAYS["ocean_freight"],
}

# Per-input grades, each a combination of existing words: the run status
# (pipeline.grading) and the latency acquisition verdict (latency.domain).
GRADE_STATES: dict[str, str] = {
    "on_time": STATE_OK,               # acquisition verdict MEETS
    "venue_closed": STATE_OK,          # acquisition verdict CLOSED (declared holiday)
    "within_decision_life": STATE_OK,  # a decision-life input inside its life
    "late": STATE_DEGRADED,            # acquisition verdict BREACHES
    "unmeasured": STATE_DEGRADED,      # acquisition verdict UNKNOWN (a stamp is missing)
    "usable_partial": STATE_DEGRADED,  # run status usable_partial
    "too_old_for_decision": STATE_DEGRADED,  # ran successfully; older than its decision life
    "failed": STATE_NOT_READY,         # run status in OUTAGE_STATUSES
    "not_run": STATE_NOT_READY,        # no data_freshness row at all
    "disabled": STATE_NOT_READY,       # run status disabled: switched off on purpose
    "missed": STATE_NOT_READY,         # a newer session is past its objective; none arrived
}

_OUTAGE_WORDS = {
    "failed": "failed this run",
    "stale": "is past its source-age budget",
    "incomplete": "came back below its key floor",
}


def _run_outcome(status: str | None) -> str:
    """The run's own word for how it went — never upgraded to "succeeded".

    ``no_publication`` is a source with nothing new to say, not a success;
    only the two statuses that advance ``last_success`` read as one.
    """
    if status is None:
        return "no run status recorded"
    if status in ADVANCES_LAST_SUCCESS:
        return "its run succeeded"
    return f"its run reported {status}"


def _observed_on(measurement: LayerMeasurement) -> date | None:
    """The observation *date* — the session for a venue clock, the day otherwise."""
    observed = measurement.stamps.observed_at
    if observed is None:
        return None
    if measurement.granularity is Granularity.SESSION_CLOSE:
        return measurement.spec.clock.session_of(observed)
    return observed.date()


def _observed_phrase(observed_on: date | None) -> str:
    return f"observed {observed_on.isoformat()}" if observed_on else "no observation recorded"


def _grade_input(measurement: LayerMeasurement, label: str, now: datetime) -> dict:
    layer = measurement.layer
    status = measurement.status
    observed_on = _observed_on(measurement)
    observed = _observed_phrase(observed_on)

    def graded(grade: str, sentence: str = "") -> dict:
        return {
            "layer": layer,
            "input": label,
            "run_status": status,
            "observed_on": observed_on.isoformat() if observed_on else None,
            "grade": grade,
            "state": GRADE_STATES[grade],
            "sentence": sentence,
        }

    if status == "not-run":
        return graded("not_run", f"{label} has no recorded run — no observation recorded")
    if status == "disabled":
        return graded("disabled", f"{label} is disabled — newest {observed}")
    if status in OUTAGE_STATUSES:
        held = f"newest held {observed}" if observed_on else observed
        return graded("failed", f"{label} {_OUTAGE_WORDS[status]} — {held}")

    if status == STATUS_USABLE_PARTIAL:
        return graded("usable_partial", f"{label} usable partial this run, {observed}")

    life = DECISION_LIFE_DAYS.get(layer)
    if life is not None:
        if observed_on is None:
            return graded("unmeasured", f"{label} ran but recorded no observation date")
        age_days = (now.date() - observed_on).days
        if age_days > life:
            return graded(
                "too_old_for_decision",
                f"{label} {observed}, {age_days} d old — {_run_outcome(status)}, but past "
                f"the {life} d a freight number holds for a decision",
            )
        return graded("within_decision_life")

    # The run that fetched this bar may have been on time, and still nothing
    # newer has arrived since. Judged by the same clock and the same
    # acquisition objective: the newest session that has closed by now, and
    # whether its objective has lapsed. An undeclared holiday reads as a
    # missed session here — newest_session_at's conservative direction.
    newest = newest_session_at(measurement.spec.clock, now)
    if observed_on is not None and newest is not None and observed_on < newest:
        close = measurement.spec.clock.instant(newest)
        objective = measurement.spec.objective.acquisition_target
        if close is not None and now - close > objective:
            return graded(
                "missed",
                f"{label} still holds the {observed_on.isoformat()} session — the "
                f"{newest.isoformat()} session closed {format_delta(now - close)} ago, past "
                f"its {format_delta(objective)} acquisition objective",
            )

    verdict = measurement.acquisition_verdict
    if verdict is Verdict.MEETS:
        return graded("on_time")
    if verdict is Verdict.BREACHES:
        if measurement.venue_closed:
            return graded("venue_closed")
        target = format_delta(measurement.spec.objective.acquisition_target)
        return graded(
            "late",
            f"{label} {observed} landed {format_delta(measurement.acquisition)} after "
            f"its close (acquisition objective {target})",
        )
    return graded("unmeasured", f"{label} arrival unmeasured ({observed}, no fetch stamp)")


def critical_arrival(measurements: Iterable[LayerMeasurement], *, now: datetime) -> dict:
    """Did the board, curve, FX and freight land within their objectives?

    Board and FX are graded by their latency *acquisition* verdict — the
    observation-to-fetch leg of the run that fetched them. The overall verdict
    would be ``unknown`` at every build, because the publication stamp does
    not exist until after the deploy. A bar fetched on time still counts as
    *missed* (red) once a newer session's objective has lapsed with nothing
    fetched — the run that brought it is not the edition being read. Freight is
    graded by its decision life
    (see ``DECISION_LIFE_DAYS``). A failed run is told apart from a
    successful one whose number is too old: the first is red, the second
    amber, and the reason says which.
    """
    by_layer = {m.layer: m for m in measurements}
    inputs = []
    for layer, label in ARRIVAL_INPUTS:
        measurement = by_layer.get(layer)
        if measurement is None:
            raise ValueError(
                f"no latency measurement for {layer!r} — it must be declared in "
                "latency.domain.LAYER_LATENCIES to be graded for arrival"
            )
        if measurement.spec.latency_class not in (LatencyClass.BOARD_PRICE, LatencyClass.FX) \
                and layer not in DECISION_LIFE_DAYS:
            raise ValueError(f"{layer!r} has neither a board/FX objective nor a decision life")
        inputs.append(_grade_input(measurement, label, now))

    state = _worst(item["state"] for item in inputs)
    ordered = sorted(
        (item for item in inputs if item["state"] != STATE_OK),
        key=lambda item: _SEVERITY[item["state"]],
    )
    reason = "; ".join(item["sentence"] for item in ordered)
    return indicator("Arrival", state, reason, inputs=inputs)


# ---------------------------------------------------------------------------
# Indicator 2 — three-leg close
# ---------------------------------------------------------------------------

THREE_LEGS = ("Soybeans", "Soybean Oil", "Soybean Meal")


def _session_label(day: date) -> str:
    return day.strftime("%a %-d %b")


def three_leg_close(command_center: Mapping[str, Any] | None) -> dict:
    """Do beans, meal and oil share one session close, and which session?

    Read off the command center the headline already builds: each leg's
    newest stored close, and the front-month crush, which inner-joins the
    three legs on date (``analysis.spreads.compute_crush_spread``) and so is
    by construction the newest session on which all three printed
    (invariant 8). Ready when that session is every leg's newest close;
    degraded when one leg is ahead of the others, so the coherent close is an
    older session than the newest prints on the page; not ready when there
    is no shared session at all.

    The delivery-month question (beans list no Oct/Dec) is the crush tile's
    verdict, not this one: two legs on different months are still one
    session's numbers.
    """
    name = "Three-leg close"
    if not command_center:
        return indicator(
            name, STATE_NOT_READY,
            "the CBOT command center could not be built this render — no leg closes to compare",
            session=None, session_label=None, legs={},
        )

    legs: dict[str, date | None] = {leg: None for leg in THREE_LEGS}
    for leg in command_center.get("legs") or ():
        if leg.get("name") in legs and leg.get("available") and leg.get("as_of"):
            legs[leg["name"]] = date.fromisoformat(str(leg["as_of"]))
    leg_dates = {leg: day.isoformat() if day else None for leg, day in legs.items()}

    missing = [leg for leg, day in legs.items() if day is None]
    if missing:
        return indicator(
            name, STATE_NOT_READY,
            "; ".join(f"{leg} has no stored close" for leg in missing)
            + " — there is no three-leg close",
            session=None, session_label=None, legs=leg_dates,
        )

    crush = command_center.get("crush") or {}
    if crush.get("error"):
        return indicator(
            name, STATE_NOT_READY,
            f"the shared-session crush could not be computed this render ({crush['error']}) "
            "— see the build log; legs "
            + ", ".join(f"{leg} {leg_dates[leg]}" for leg in THREE_LEGS),
            session=None, session_label=None, legs=leg_dates,
        )
    if not crush.get("available") or not crush.get("as_of"):
        printed = ", ".join(f"{leg} {leg_dates[leg]}" for leg in THREE_LEGS)
        return indicator(
            name, STATE_NOT_READY,
            f"no session on which all three legs closed in the stored history ({printed})",
            session=None, session_label=None, legs=leg_dates,
        )

    session = date.fromisoformat(str(crush["as_of"]))
    data = {
        "session": session.isoformat(),
        "session_label": _session_label(session),
        "legs": leg_dates,
    }
    behind = [leg for leg in THREE_LEGS if legs[leg] == session]
    ahead = [leg for leg in THREE_LEGS if legs[leg] != session]
    if not ahead:
        return indicator(name, STATE_OK, **data)
    ahead_dates = {legs[leg] for leg in ahead}
    if len(ahead_dates) == 1:
        (only,) = ahead_dates
        ahead_text = f"{' and '.join(ahead)} on {only.isoformat()}"  # type: ignore[union-attr]
    else:
        ahead_text = " and ".join(f"{leg} on {leg_dates[leg]}" for leg in ahead)
    if not behind:
        # Every leg printed after the shared session, on different days.
        return indicator(
            name, STATE_DEGRADED,
            f"the newest session all three legs closed on is {session.isoformat()}; "
            f"since then {ahead_text}",
            **data,
        )
    return indicator(
        name, STATE_DEGRADED,
        f"{' and '.join(behind)} last closed {session.isoformat()}, behind "
        f"{ahead_text} — the three-leg close is the {session.isoformat()} session",
        **data,
    )


# ---------------------------------------------------------------------------
# Indicator 1 — origin comparison
# ---------------------------------------------------------------------------


def _structural_absences() -> frozenset[str]:
    """Absent reasons declared in the registry — an origin with no source by design."""
    return frozenset(
        str(leg["absent_reason"]) for leg in config.ORIGIN_LEGS.values() if leg.get("absent_reason")
    )


def _exclusion(row: LandedCost, ranking: OriginRanking, run_statuses: Mapping[str, str],
               today: date) -> str:
    """Why one origin is off the FOB board, naming the input and its date.

    Explains the three gates ``analysis.origins.comparison.fob_eligible``
    applies — recency, a costed path to FOB vessel, the requested window —
    in that order, every one that applies. It does not re-decide them: the
    caller only asks about rows the board's own gate left out.
    """
    quote = row.quote
    layer = quote.source.layer
    why: list[str] = []
    if row.freshness is not Freshness.CURRENT:
        status = run_statuses.get(layer)
        if status in OUTAGE_STATUSES:
            why.append(f"{layer} {_OUTAGE_WORDS[status]}, the quote is {quote.age_days(today)} d old")
        else:
            ran = f" — {_run_outcome(status)}" if status is not None else ""
            why.append(
                f"{quote.age_days(today)} d old, past its {quote.max_age_days} d budget{ran}"
            )
    if row.fob_equivalent is None:
        bridge = bridge_components(quote.incoterm.value, quote.carrier.value)
        pre_fob = [
            blocker.message for blocker in row.blockers
            if bridge is not None and blocker.component in bridge
        ]
        why.append(pre_fob[0] if pre_fob else "no costed path to FOB vessel")
    verdict = window_verdict(quote, ranking.requested_window)
    if verdict is Comparability.WINDOW_UNKNOWN:
        why.append("publishes no shipment window")
    elif verdict is Comparability.WINDOW_MISMATCH and quote.shipment_window is not None:
        why.append(
            f"quoted for {quote.shipment_window.describe()}, not "
            f"{ranking.requested_window.describe()}"
        )
    if not why:  # pragma: no cover — fob_eligible's gate and this one disagree
        raise ValueError(f"{quote.origin.key} is off the FOB board for no reason this module knows")
    return f"{quote.origin.name} observed {quote.observation_date.isoformat()}: {', '.join(why)}"


def origin_comparison(ranking: OriginRanking, *, run_statuses: Mapping[str, str],
                      today: date) -> dict:
    """Can at least two origins be compared FOB-equivalent for this window?

    The FOB board is the part of the origin answer that survives missing
    freight, so it is the test: two origins normalised to FOB vessel, inside
    their recency budgets, quoting the window, observed within the spread
    limit. Ready when every origin with a quote is on it; degraded when two or
    more are but another is left out; not ready under two. Each origin left
    out is named with its observation date and every gate it failed, and a
    stale quote says whether its layer's run failed or succeeded — a
    successful run whose number is too old for the decision is not an outage.

    Origins the registry declares absent (the PNW: no free series exists) are
    not held against readiness — no run can supply them. One the ingest has
    gone quiet on is.
    """
    name = "Origins"
    window = ranking.requested_window.describe()
    board = fob_board(ranking)
    eligible = fob_eligible(ranking)
    excluded = [
        _exclusion(row, ranking, run_statuses, today)
        for row in ranking.rows if row not in eligible
    ]
    structural = _structural_absences()
    excluded += [
        f"{origin.label}: {origin.reason}"
        for origin in ranking.unavailable if origin.reason not in structural
    ]
    data = {
        "window": window,
        "destination": ranking.destination.name,
        "board": [
            {
                "origin": row.quote.origin.name,
                "observed_on": row.quote.observation_date.isoformat(),
                "fob_usd_mt": row.fob_equivalent.amount if row.fob_equivalent else None,
            }
            for row in board
        ],
        "excluded": excluded,
    }

    refusal = fob_board_refusal(ranking)
    if refusal is not None:
        dated = ", ".join(
            f"{row.quote.origin.name} observed {row.quote.observation_date.isoformat()}"
            for row in eligible
        )
        return indicator(name, STATE_NOT_READY, f"{refusal} ({dated})", **data)

    tail = f" — {'; '.join(excluded)}" if excluded else ""
    if len(board) >= 2:
        if not excluded:
            return indicator(name, STATE_OK, **data)
        return indicator(
            name, STATE_DEGRADED,
            f"{len(board)} origins FOB-comparable for {window} to "
            f"{ranking.destination.name}{tail}",
            **data,
        )
    if len(board) == 1:
        lead = f"only {board[0].quote.origin.name} is FOB-comparable for {window}"
    else:
        lead = f"no origin is FOB-comparable for {window}"
    return indicator(name, STATE_NOT_READY, f"{lead}{tail}", **data)


# ---------------------------------------------------------------------------
# The masthead's read: all three, isolated
# ---------------------------------------------------------------------------


def _isolated(name: str, build: Callable[[], dict]) -> dict:
    """One indicator, or a red labelled state if it could not be built.

    The masthead must never fail the build (the price-age line's rule), and a
    readiness indicator that vanished would read as one with nothing to say.
    So a builder that raises renders *not ready*, saying it could not be
    assessed — never ready, and never blank.
    """
    try:
        return build()
    except Exception as exc:  # noqa: BLE001 — isolation is the point
        log.error("decision readiness: %s could not be assessed", name, exc_info=True)
        return indicator(
            name, STATE_NOT_READY,
            f"could not be assessed this render ({type(exc).__name__}) — see the build log",
        )


def _public_origin_comparison(run_statuses: Mapping[str, str], today: date) -> dict:
    """Indicator 1 off the public tier: committed policy rates, never desk entries."""
    from contextlib import closing

    from analysis.futures.privacy import AUDIENCE_PUBLIC
    from analysis.origins.assumptions import load_assumptions
    from analysis.origins.comparison import build_ranking, default_window
    from pipeline.connection import get_connection

    with closing(get_connection()) as conn:
        ranking = build_ranking(
            conn,
            destination_key=next(iter(config.DESTINATION_PORTS)),
            window=default_window(today),
            today=today,
            assumptions=load_assumptions(audience=AUDIENCE_PUBLIC),
        )
    return origin_comparison(ranking, run_statuses=run_statuses, today=today)


def _run_statuses() -> dict[str, str]:
    from pipeline.query import read_freshness

    frame = read_freshness()
    if frame.empty or "layer_name" not in frame.columns:
        return {}
    return {
        str(layer): str(status or "success")
        for layer, status in zip(frame["layer_name"], frame["status"], strict=True)
    }


def build_decision_readiness(command_center: Mapping[str, Any] | None, *, now: datetime) -> dict:
    """The three indicators for the masthead, in reading order.

    ``command_center`` is the headline's own analyst output, so the three-leg
    close describes the same legs the page prints. Freshness is read once for
    the origin run statuses; the latency measurement reads it again through
    its own seam (``latency.measure.measure``), as the price-age line does.
    """
    from latency.measure import measure

    statuses: dict[str, str] = {}
    try:
        statuses = _run_statuses()
    except Exception:  # noqa: BLE001 — origin reasons then omit run status, never guess it
        log.error("decision readiness: could not read data_freshness", exc_info=True)

    indicators = [
        _isolated("Origins", lambda: _public_origin_comparison(statuses, now.date())),
        _isolated("Three-leg close", lambda: three_leg_close(command_center)),
        _isolated("Arrival", lambda: critical_arrival(measure(generated_at=now), now=now)),
    ]
    return {
        "indicators": indicators,
        "attention": [item for item in indicators if item["state"] != STATE_OK],
    }


__all__ = [
    "ARRIVAL_INPUTS",
    "build_decision_readiness",
    "DECISION_LIFE_DAYS",
    "STATES",
    "STATE_LABELS",
    "THREE_LEGS",
    "critical_arrival",
    "indicator",
    "origin_comparison",
    "three_leg_close",
]
