"""Continuous series built from named contracts — and the roll method, stated.

The project's continuous series today is Yahoo's ``ZS=F``. It is useful and it
is unattributable: the provider switches the underlying contract on its own
schedule, publishes neither the roll date nor the old and new contract, and
adjusts nothing, so a roll day carries a price gap that is not a move.
``analysis.signals.is_near_roll`` exists to demote indicators that gap may have
faked, and LAYERS.md's "Front-month roll-day discontinuities" records the whole class of damage.

This module is the alternative: a series stitched from *our* named-contract
history, where the roll date is a rule we wrote down and the adjustment is
arithmetic anyone can reproduce.

It is deliberately honest about its own limit. The named-contract history this
stack holds is the ``forward_curve`` table, which began accumulating in
July 2026 — a series built here is as long as that table and no longer. Where
that is too short to be useful, :func:`build_continuous` says so and returns
None instead of padding the front with the provider series, which would be a
silent substitution of exactly the kind Phase 3 forbids.

Roll rule
---------
Switch out of the front contract ``roll_offset_bdays`` business days before its
last trade date, into the next listed month. The default is 5, which for a
CBOT grain puts the roll comfortably ahead of first notice day (last business
day of the month preceding delivery) — the date a physical hedger must be flat
by, and therefore the date the *tradeable* front month really changes.

Adjustment
----------
``UNADJUSTED``  levels are real prices; the roll-day gap is visible.
``DIFFERENCE``  each pre-roll segment is shifted by the roll-day price
                difference (Panama). Differences and volatility are continuous;
                historical *levels* are no longer tradeable prices.
``RATIO``       each pre-roll segment is scaled by the roll-day price ratio.
                Percentage returns are continuous; levels again are not prices.

Both adjusted forms carry that warning in
``ContinuousSeries.adjustment_note`` so it travels with the data.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd

from analysis.futures.domain import (
    ContinuousSeries,
    NamedContract,
    RollMethod,
    business_days_between,
    contracts_from,
    is_business_day,
    spec_for,
)
from analysis.futures.providers import QuoteProvider

log = logging.getLogger(__name__)

#: Business days before last trade that the series rolls out of the front.
DEFAULT_ROLL_OFFSET_BDAYS = 5

#: Below this many sessions a stitched series teaches nothing and is withheld.
MIN_SESSIONS = 10

_ADJUSTMENT_METHODS = {
    "unadjusted": RollMethod.CALENDAR_ROLL_UNADJUSTED,
    "difference": RollMethod.CALENDAR_ROLL_DIFFERENCE,
    "ratio": RollMethod.CALENDAR_ROLL_RATIO,
}

_ADJUSTMENT_NOTES = {
    "unadjusted": (
        f"rolled {DEFAULT_ROLL_OFFSET_BDAYS} business days before each front contract's last "
        "trade date; no adjustment applied, so every level is a real price and the roll-day gap "
        "is visible"
    ),
    "difference": (
        f"rolled {DEFAULT_ROLL_OFFSET_BDAYS} business days before each front contract's last "
        "trade date and back-adjusted by the roll-day difference — differences and volatility "
        "are continuous, but levels before the last roll are NOT tradeable prices"
    ),
    "ratio": (
        f"rolled {DEFAULT_ROLL_OFFSET_BDAYS} business days before each front contract's last "
        "trade date and back-adjusted by the roll-day ratio — percentage returns are continuous, "
        "but levels before the last roll are NOT tradeable prices"
    ),
}


def active_contract(
    commodity: str, on: date, *, roll_offset_bdays: int = DEFAULT_ROLL_OFFSET_BDAYS
) -> NamedContract | None:
    """Which named contract this roll rule considers the front month on ``on``.

    Returns None where the product's expiry rule is not encoded — there is no
    roll date without a last trade date, and choosing one from the delivery
    month label would be the guess this package refuses everywhere else.
    """
    spec = spec_for(commodity)
    if spec.expiry_rule is None:
        return None
    for contract in contracts_from(spec.name, on, count=6):
        if contract.last_trade is None:
            continue
        if business_days_between(on, contract.last_trade) > roll_offset_bdays:
            return contract
    return None


def roll_schedule(
    commodity: str, start: date, end: date, *, roll_offset_bdays: int = DEFAULT_ROLL_OFFSET_BDAYS
) -> tuple[tuple[date, str], ...]:
    """(date, symbol) for every session in [start, end] the rule assigns a contract to.

    Only sessions are listed, not calendar days, and only where a contract can
    be named. Exposed on its own so the page can render "this bar is ZSX26"
    beside the series, which is the fact the provider series cannot supply.
    """
    out: list[tuple[date, str]] = []
    cursor = start
    while cursor <= end:
        if is_business_day(cursor):
            contract = active_contract(commodity, cursor, roll_offset_bdays=roll_offset_bdays)
            if contract is not None:
                out.append((cursor, contract.symbol))
        cursor += timedelta(days=1)
    return tuple(out)


def build_continuous(
    provider: QuoteProvider,
    commodity: str,
    *,
    as_of: date,
    adjustment: str = "unadjusted",
    roll_offset_bdays: int = DEFAULT_ROLL_OFFSET_BDAYS,
    sessions: int = 250,
) -> ContinuousSeries | None:
    """Stitch a continuous series from stored named-contract history.

    Returns None — never a partial or a padded series — when the stored
    named-contract history is too short. The caller then either shows the
    provider front-month series *labelled as such*, or shows nothing.
    """
    if adjustment not in _ADJUSTMENT_METHODS:
        raise ValueError(f"unknown adjustment {adjustment!r}; expected one of {sorted(_ADJUSTMENT_METHODS)}")

    history = provider.curve_history(commodity, as_of=as_of, sessions=sessions)
    if not history:
        return None

    raw: list[tuple[date, float, str]] = []
    for snapshot in history:
        if not snapshot.coherent or snapshot.observation_date is None:
            continue
        wanted = active_contract(commodity, snapshot.observation_date, roll_offset_bdays=roll_offset_bdays)
        if wanted is None:
            continue
        leg = snapshot.leg(wanted.symbol)
        if leg is None:
            continue
        raw.append((snapshot.observation_date, leg.price, wanted.symbol))

    # One point per session — a re-run on the same date must not double a bar.
    deduped: dict[date, tuple[float, str]] = {}
    for observed, price, symbol in raw:
        deduped[observed] = (price, symbol)
    ordered = sorted((day, price, symbol) for day, (price, symbol) in deduped.items())

    if len(ordered) < MIN_SESSIONS:
        log.info(
            "%s: %d session(s) of named-contract history — below the %d-session floor, "
            "no stitched series built",
            commodity, len(ordered), MIN_SESSIONS,
        )
        return None

    roll_dates = tuple(
        day for (day, _, symbol), (_, _, previous) in zip(ordered[1:], ordered, strict=False)
        if symbol != previous
    )
    points = _adjust(ordered, adjustment, provider, commodity, roll_offset_bdays)

    return ContinuousSeries(
        commodity=commodity,
        roll_method=_ADJUSTMENT_METHODS[adjustment],
        points=points,
        contract_by_date=tuple((day, symbol) for day, _, symbol in ordered),
        roll_dates=roll_dates,
        adjustment_note=_ADJUSTMENT_NOTES[adjustment],
    )


def _adjust(
    ordered: list[tuple[date, float, str]],
    adjustment: str,
    provider: QuoteProvider,
    commodity: str,
    roll_offset_bdays: int,
) -> tuple[tuple[date, float], ...]:
    """Apply the back-adjustment, walking backwards from the newest segment.

    The adjustment on a roll is the *same-session* difference (or ratio)
    between the two contracts — old front and new front on the roll date — not
    the difference between the last old-contract print and the first new one.
    Using consecutive prints would fold one session's market move into the
    adjustment, which is the error that makes a back-adjusted series drift.
    """
    if adjustment == "unadjusted":
        return tuple((day, price) for day, price, _ in ordered)

    factors: list[float] = [0.0 if adjustment == "difference" else 1.0] * len(ordered)
    running = 0.0 if adjustment == "difference" else 1.0

    for position in range(len(ordered) - 1, 0, -1):
        day, _, symbol = ordered[position]
        _, _, previous_symbol = ordered[position - 1]
        if symbol != previous_symbol:
            snapshot = _snapshot_on(provider, commodity, day)
            step = _roll_step(snapshot, previous_symbol, symbol, adjustment)
            if step is not None:
                running = running + step if adjustment == "difference" else running * step
        factors[position - 1] = running

    if ordered:
        factors[-1] = 0.0 if adjustment == "difference" else 1.0

    return tuple(
        (day, price + factors[position] if adjustment == "difference" else price * factors[position])
        for position, (day, price, _) in enumerate(ordered)
    )


def _snapshot_on(provider: QuoteProvider, commodity: str, day: date):
    for snapshot in provider.curve_history(commodity, as_of=day, sessions=1):
        return snapshot
    return None


def _roll_step(snapshot, old_symbol: str, new_symbol: str, adjustment: str) -> float | None:
    """The same-session gap between the two contracts on the roll date."""
    if snapshot is None:
        return None
    old = snapshot.leg(old_symbol)
    new = snapshot.leg(new_symbol)
    if old is None or new is None:
        return None
    if adjustment == "difference":
        return new.price - old.price
    if old.price == 0:
        return None
    return new.price / old.price


#: How many sessions back a roll step may be struck when the roll date
#: itself lacks a same-session print of both contracts. Beyond this the two
#: segments cannot be honestly joined and the older one is dropped.
MAX_ROLL_STEP_LOOKBACK_SESSIONS = 5


def build_from_bars(
    bars: pd.DataFrame,
    commodity: str,
    *,
    adjustment: str = "ratio",
    roll_offset_bdays: int = DEFAULT_ROLL_OFFSET_BDAYS,
) -> ContinuousSeries | None:
    """Stitch a continuous series from Layer 11b's named-contract bars.

    The deep-history counterpart of :func:`build_continuous`: same roll
    rule, same adjustment vocabulary, but fed from ``contract_bars`` —
    full daily history per named contract — instead of forward-curve
    snapshots, so the same-session roll step is struck from two real
    closes on (or within :data:`MAX_ROLL_STEP_LOOKBACK_SESSIONS` sessions
    before) the roll date itself.

    Honesty rules, in order:
    - A session where the rule's assigned contract did not print is a gap,
      never a substitution from another contract.
    - A roll with no session where old and new front both printed cannot
      be adjusted across; everything *before* that roll is dropped, with
      the reason recorded in ``adjustment_note`` — a shorter honest series
      over a longer glued one (invariant 2).
    - Below MIN_SESSIONS the answer is None, exactly as build_continuous.

    Parameters
    ----------
    bars : pd.DataFrame
        Columns ``ticker`` (provider symbol, e.g. ``ZSX26.CBT``), ``Date``
        (ISO string or datetime), ``Close``. Extra columns are ignored.
    commodity : str
        Key in CONTRACT_SPECS / FORWARD_CURVE_CONTRACTS.
    """
    if adjustment not in _ADJUSTMENT_METHODS:
        raise ValueError(
            f"unknown adjustment {adjustment!r}; expected one of {sorted(_ADJUSTMENT_METHODS)}"
        )
    if bars is None or bars.empty:
        return None
    spec = spec_for(commodity)
    if spec.expiry_rule is None:
        # No last-trade rule → no roll date → no series. Same refusal as
        # active_contract; a calendar guessed from the delivery month is
        # the estimate this package exists to retire.
        return None

    # date → {provider_symbol: close}
    closes_by_date: dict[date, dict[str, float]] = {}
    for row in bars.itertuples(index=False):
        day = _bar_date(row.Date)
        if day is None or row.Close is None:
            continue
        close_any: Any = row.Close
        closes_by_date.setdefault(day, {})[str(row.ticker)] = float(close_any)

    ordered: list[tuple[date, float, str]] = []
    for day in sorted(closes_by_date):
        wanted = active_contract(commodity, day, roll_offset_bdays=roll_offset_bdays)
        if wanted is None:
            continue
        close = closes_by_date[day].get(wanted.provider_symbol)
        if close is None:
            continue  # the assigned contract did not print — a gap
        ordered.append((day, close, wanted.provider_symbol))

    if len(ordered) < MIN_SESSIONS:
        log.info(
            "%s: %d stitched session(s) from contract bars — below the %d-session floor",
            commodity, len(ordered), MIN_SESSIONS,
        )
        return None

    truncation_note = ""
    if adjustment != "unadjusted":
        ordered, truncation_note = _truncate_unjoinable(
            ordered, closes_by_date, commodity
        )
        if len(ordered) < MIN_SESSIONS:
            log.info(
                "%s: %d joinable session(s) after dropping unadjustable segments — "
                "below the %d-session floor", commodity, len(ordered), MIN_SESSIONS,
            )
            return None

    points = _adjust_from_map(ordered, closes_by_date, adjustment)
    roll_dates = tuple(
        day for (day, _, symbol), (_, _, previous) in zip(ordered[1:], ordered, strict=False)
        if symbol != previous
    )
    note = _ADJUSTMENT_NOTES[adjustment] + truncation_note
    return ContinuousSeries(
        commodity=commodity,
        roll_method=_ADJUSTMENT_METHODS[adjustment],
        points=points,
        contract_by_date=tuple((day, symbol) for day, _, symbol in ordered),
        roll_dates=roll_dates,
        adjustment_note=note,
    )


def _bar_date(value: Any) -> date | None:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        text = str(value)[:10]
        return date.fromisoformat(text)
    except (TypeError, ValueError):
        return None


def _roll_step_from_map(
    closes_by_date: dict[date, dict[str, float]],
    session_dates: list[date],
    roll_day: date,
    old_symbol: str,
    new_symbol: str,
    adjustment: str,
) -> float | None:
    """Same-session gap between the two contracts, on the roll day or the
    nearest earlier session (within the lookback) where both printed."""
    position = len([d for d in session_dates if d <= roll_day])
    candidates = session_dates[max(0, position - MAX_ROLL_STEP_LOOKBACK_SESSIONS):position]
    for day in reversed(candidates):
        legs = closes_by_date.get(day, {})
        old, new = legs.get(old_symbol), legs.get(new_symbol)
        if old is None or new is None:
            continue
        if adjustment == "difference":
            return new - old
        if old == 0:
            return None
        return new / old
    return None


def _truncate_unjoinable(
    ordered: list[tuple[date, float, str]],
    closes_by_date: dict[date, dict[str, float]],
    commodity: str,
) -> tuple[list[tuple[date, float, str]], str]:
    """Drop everything before the newest roll that cannot be struck.

    Walks backwards from the newest segment; the first roll with no
    same-session print of both contracts (within the lookback) ends the
    series — the older segments are dropped rather than glued unadjusted
    onto an adjusted run.
    """
    session_dates = sorted(closes_by_date)
    for position in range(len(ordered) - 1, 0, -1):
        day, _, symbol = ordered[position]
        _, _, previous_symbol = ordered[position - 1]
        if symbol == previous_symbol:
            continue
        step = _roll_step_from_map(
            closes_by_date, session_dates, day, previous_symbol, symbol, "ratio"
        )
        if step is None:
            note = (
                f"; history before {day.isoformat()} dropped — no session where "
                f"{previous_symbol} and {symbol} both printed, so that roll cannot "
                "be adjusted across"
            )
            log.warning("%s: %s", commodity, note.lstrip("; "))
            return ordered[position:], note
    return ordered, ""


def _adjust_from_map(
    ordered: list[tuple[date, float, str]],
    closes_by_date: dict[date, dict[str, float]],
    adjustment: str,
) -> tuple[tuple[date, float], ...]:
    """Back-adjust from the newest segment, same walk as :func:`_adjust`
    but struck from the bars map instead of curve snapshots."""
    if adjustment == "unadjusted":
        return tuple((day, price) for day, price, _ in ordered)

    session_dates = sorted(closes_by_date)
    identity = 0.0 if adjustment == "difference" else 1.0
    factors: list[float] = [identity] * len(ordered)
    running = identity

    for position in range(len(ordered) - 1, 0, -1):
        day, _, symbol = ordered[position]
        _, _, previous_symbol = ordered[position - 1]
        if symbol != previous_symbol:
            step = _roll_step_from_map(
                closes_by_date, session_dates, day, previous_symbol, symbol, adjustment
            )
            # _truncate_unjoinable has already dropped any roll with no
            # step, so step is present on every roll reached here.
            if step is not None:
                running = running + step if adjustment == "difference" else running * step
        factors[position - 1] = running

    factors[-1] = identity
    return tuple(
        (day, price + factors[position] if adjustment == "difference" else price * factors[position])
        for position, (day, price, _) in enumerate(ordered)
    )


def describe_provider_series(series: ContinuousSeries | None) -> dict[str, Any]:
    """Render-ready description of a series, method first.

    A continuous series must never appear on a surface without its roll method,
    so the method is the first key and the label is built here rather than in a
    template that could omit it.
    """
    if series is None:
        return {
            "available": False,
            "reason": (
                "no continuous series — the stored named-contract history is shorter than "
                f"{MIN_SESSIONS} sessions and this project does not pad it with the provider's "
                "own front-month series"
            ),
        }
    return {
        "available": True,
        "hedgeable": False,
        **series.to_dict(),
    }


__all__ = [
    "DEFAULT_ROLL_OFFSET_BDAYS",
    "MAX_ROLL_STEP_LOOKBACK_SESSIONS",
    "build_from_bars",
    "MIN_SESSIONS",
    "active_contract",
    "build_continuous",
    "describe_provider_series",
    "roll_schedule",
]
