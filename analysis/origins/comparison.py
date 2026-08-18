"""Assembling one origin comparison (Phase 2).

The trader question, executed: for a named destination and a named shipment
window, read every declared origin, cost each one to the destination, and rank
the ones that are genuinely comparable — saying plainly, for each one that is
not, why it is not.

Four alignment rules, and every one of them exists because breaking it produces
an arbitrage that is not there:

**Window before price.** Each origin's quotes are selected by *overlap with the
requested window*, and the row with the largest overlap wins. Never "the
nearest month" and never "the first row": September freight ranked as October
freight is a wrong number nothing downstream can detect. An origin with quotes
but no overlapping window still renders — with its nearest window shown and a
``WINDOW_MISMATCH`` verdict — because hiding it would suggest that origin is
not offering at all.

**No window is a verdict, not a default.** AgRural publishes a Paranaguá level
with no shipment period. It is not prompt, it is not October; it is undated.
The row shows its level and never enters a ranking against a dated one.

**One session, or none.** The origins print on different days by nature, so the
comparison records the spread between the newest and oldest observation it
used and refuses the whole ranking beyond
``config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS``. Two origins four days apart
differ by four days of flat price as much as by competitiveness.

**A ranking of one is not a ranking.** ``advantage_usd_mt`` needs two rankable
rows. "Cheapest origin: Brazil" with nothing to be cheaper than is a sentence
that reads like a finding and contains none.
"""

from __future__ import annotations

import logging
from dataclasses import replace
from datetime import date

import config
from analysis.origins.assumptions import AssumptionSet, load_assumptions
from analysis.origins.domain import (
    Comparability,
    LandedCost,
    OriginQuote,
    OriginRanking,
    Port,
    ShipmentWindow,
    UnavailableOrigin,
)
from analysis.origins.landed_cost import compute_landed_cost, window_verdict
from analysis.origins.sources import (
    offered_windows,
    port_for,
    read_origin_quotes,
)

log = logging.getLogger(__name__)


def select_quote(
    quotes: tuple[OriginQuote, ...],
    requested: ShipmentWindow,
) -> OriginQuote | None:
    """The one quote that best answers the requested window.

    Preference order, and the order is the whole point:

    1. the overlapping window with the **largest overlap** — an AMS half-month
       slot inside the requested month beats a MAGyP band that merely clips it;
    2. failing any overlap, the window whose midpoint is **nearest** the
       requested midpoint, so the mismatch that is rendered is the most
       informative one rather than an arbitrary row;
    3. failing any window at all, the undated quote — which the caller will
       mark ``WINDOW_UNKNOWN`` and never rank.

    Step 2 selects a row it will then refuse to rank. That is deliberate: the
    reader learns "Argentina is quoting, but for November" rather than
    "Argentina is silent", and those are opposite facts about a market.
    """
    if not quotes:
        return None
    dated = [quote for quote in quotes if quote.shipment_window is not None]
    if not dated:
        return quotes[0]
    overlapping = [
        quote for quote in dated
        if quote.shipment_window.overlaps(requested)  # type: ignore[union-attr]
    ]
    if overlapping:
        return max(
            overlapping,
            key=lambda quote: (
                quote.shipment_window.overlap_days(requested),  # type: ignore[union-attr]
                -abs((quote.shipment_window.midpoint() - requested.midpoint()).days),  # type: ignore[union-attr]
            ),
        )
    return min(
        dated,
        key=lambda quote: abs(
            (quote.shipment_window.midpoint() - requested.midpoint()).days  # type: ignore[union-attr]
        ),
    )


def _observation_spread(rows: tuple[LandedCost, ...]) -> int | None:
    dates = [row.quote.observation_date for row in rows]
    if len(dates) < 2:
        return None
    return (max(dates) - min(dates)).days


def build_ranking(
    conn,
    *,
    destination_key: str,
    window: ShipmentWindow,
    today: date,
    assumptions: AssumptionSet | None = None,
    leg_ids: tuple[str, ...] | None = None,
) -> OriginRanking:
    """Cost every declared origin into one destination for one window."""
    assumptions = assumptions if assumptions is not None else load_assumptions()
    destination = port_for(destination_key, role="destination")
    legs = leg_ids if leg_ids is not None else tuple(config.ORIGIN_LEGS)

    rows: list[LandedCost] = []
    unavailable: list[UnavailableOrigin] = []

    for leg_id in legs:
        leg = config.ORIGIN_LEGS[leg_id]
        port = port_for(leg["port"])
        if leg.get("absent_reason"):
            unavailable.append(
                UnavailableOrigin(port=port, label=leg["label"], reason=leg["absent_reason"])
            )
            continue
        quotes = read_origin_quotes(conn, leg_id, today=today)
        chosen = select_quote(quotes, window)
        if chosen is None:
            unavailable.append(
                UnavailableOrigin(
                    port=port,
                    label=leg["label"],
                    reason=(
                        f"no observation for {leg['key']} in the last "
                        f"{config.LAYER_MAX_DATA_AGE_DAYS.get(leg.get('layer', ''), 120)} "
                        "days of readable history — this is our ingest being quiet, not "
                        "the origin withdrawing an offer"
                    ),
                )
            )
            continue
        rows.append(
            compute_landed_cost(
                chosen, destination, window, assumptions, today=today
            )
        )

    rows_tuple = tuple(rows)
    spread = _observation_spread(tuple(row for row in rows_tuple if row.is_rankable))
    notes: list[str] = []
    if spread is not None and spread > config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS:
        # Refuse the whole ranking rather than a row: the spread is a property
        # of the *set*, and dropping the oldest row would leave a comparison
        # that looks tighter than the data it came from.
        rows_tuple = tuple(
            replace(row, comparability=Comparability.OBSERVATION_SPREAD)
            if row.is_rankable else row
            for row in rows_tuple
        )
        notes.append(
            f"origins were observed {spread} days apart, past the "
            f"{config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS}-day limit — over that gap the "
            "difference between two origins is measuring the calendar as much as the "
            "market, so no ranking is published"
        )

    return OriginRanking(
        destination=destination,
        requested_window=window,
        as_of=today,
        rows=rows_tuple,
        method_version=config.LANDED_COST_METHOD_VERSION,
        assumption_set_id=assumptions.set_id,
        observation_spread_days=spread,
        unavailable=tuple(unavailable),
        notes=tuple(notes),
    )


def fob_board(ranking: OriginRanking) -> tuple[LandedCost, ...]:
    """The window-aligned FOB-vessel comparison, cheapest first.

    This is the part of the answer that survives a missing freight assumption.
    Every origin here has been normalised to the same delivery term at its own
    port — the CIF-barge Gulf bid has had its elevation added — so the numbers
    are like for like, and the only thing separating them from a landed
    comparison is the leg across the ocean.

    It is *not* the answer to the trader question. Two origins equal FOB are
    not equal delivered, and the freight difference between the US Gulf and
    Brazil to North China is routinely larger than the FOB spread between them.
    The page labels it as an intermediate accordingly.
    """
    eligible = [
        row for row in ranking.rows
        if row.fob_equivalent is not None
        and window_verdict(row.quote, ranking.requested_window) is Comparability.COMPARABLE
    ]
    return tuple(sorted(eligible, key=lambda row: row.fob_equivalent.amount))  # type: ignore[union-attr]


def fob_advantage(ranking: OriginRanking) -> tuple[float | None, float | None]:
    """``(usd_mt, pct)`` the cheapest FOB origin leads the second by, or ``(None, None)``."""
    board = fob_board(ranking)
    if len(board) < 2:
        return None, None
    cheapest = board[0].fob_equivalent.amount  # type: ignore[union-attr]
    runner_up = board[1].fob_equivalent.amount  # type: ignore[union-attr]
    edge = runner_up - cheapest
    return edge, (edge / runner_up * 100.0 if runner_up else None)


def default_window(today: date) -> ShipmentWindow:
    """The window the page opens on: the first forward month, not prompt.

    A physical buyer asking "which origin" is almost never asking about the
    balance of today's month — that cargo is already fixed. The first whole
    forward month is the earliest window a decision is still open on.
    """
    windows = offered_windows(today)
    return windows[1] if len(windows) > 1 else windows[0]


def destinations() -> tuple[Port, ...]:
    return tuple(port_for(key, role="destination") for key in config.DESTINATION_PORTS)


__all__ = [
    "build_ranking",
    "default_window",
    "destinations",
    "fob_advantage",
    "fob_board",
    "select_quote",
]
