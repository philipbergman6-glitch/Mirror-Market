"""Data for the Origin Comparison page (Phase 2) — data only, never markup.

Same split the nine market blocks keep (M18 #214): this module returns numbers
and labels, ``app/templates/origins.html.j2`` decides how they look. The reason
is unchanged — an f-string builder cannot enforce "same treatment, every
origin", and the treatment is the honest part of this page.

The page is a static file with working selectors, which is a real constraint.
There is no server to ask for "October instead of September", so every offered
shipment window is costed at build time and embedded, and the selector is a
class toggle over pre-rendered sections. That bounds what can be offered —
``config.ORIGIN_WINDOW_MONTHS_AHEAD`` windows for each configured destination —
and the bound is stated on the page rather than hidden behind a control that
silently does nothing.

Section order is the order a physical trader asks the questions:

    01  the answer, or why there isn't one
    02  what has to be entered before each route can be one
    03  the waterfall behind it
    04  the FOB-equivalent board — the part that survives missing freight
    05  what would have to move to change the answer
    06  crush, at all three of the levels that are not the same number
    07  who could actually do it
    08  every input, its owner and its expiry
    09  what lapses next, and what goes dark with it
    10  what changed since the last run

Sections 02 and 09 are the operational half. A fail-closed page on a fresh
clone is correct and unusable in equal measure: "not comparable" is the honest
answer and the *next* question is always "so what do I have to enter, and who
enters it". Answering that on the page rather than in a README is what makes
the blocked state a workflow rather than a wall.
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

import config
from analysis.origins import crush as crush_mod
from analysis.origins import history as history_mod
from analysis.origins import players as players_mod
from analysis.origins import readiness as readiness_mod
from analysis.origins import scenarios as scenarios_mod
from analysis.origins.assumptions import AssumptionSet, load_assumptions
from analysis.origins.comparison import build_ranking, fob_advantage, fob_board
from analysis.origins.domain import Comparability, CostComponent, OriginRanking
from analysis.origins.landed_cost import COMPONENT_LABELS
from analysis.origins.sources import offered_windows

log = logging.getLogger(__name__)

# How far ahead the renewals section looks. A month is the horizon a desk can
# actually act on: a freight indication takes about a fortnight to refresh, and
# anything further out is noise on a page read daily.
RENEWAL_HORIZON_DAYS = 30

STATE_OK = "ok"
STATE_EMPTY = "empty"
STATE_ABSENT = "absent"

# id, title, and the right-aligned "why you're looking at this" note — the same
# shape the market blocks use, so the two page families read identically.
SECTION_SPECS = (
    ("decision", "Origin decision", "which origin is cheapest delivered, for this window"),
    ("readiness", "Route readiness", "what must be entered before each route is comparable"),
    ("waterfall", "Landed-cost waterfall", "every dollar between the origin quote and the berth"),
    ("fob", "FOB-equivalent board", "like-for-like at the loading port, before the ocean leg"),
    ("sensitivity", "Sensitivity", "what has to move before the answer changes"),
    ("crush", "Crush margins", "board, gross physical and net plant are three different numbers"),
    ("counterparties", "Counterparties", "who originates, who buys, and over which berth"),
    ("inputs", "Inputs & assumptions", "every number, its owner, and when it lapses"),
    ("renewals", "Renewals due", "what lapses next, who owns it, and what goes dark with it"),
    ("changes", "What changed", "ranking and input movement since the previous run"),
)

# Which comparability verdicts read as "we could not", versus "the market did
# something". Only the tone differs, but the tone is the message: a window
# mismatch is a fact about the origin, a missing assumption is a fact about us.
_OURS = {Comparability.MISSING_INPUT, Comparability.INCOTERM_UNBRIDGED}

COMPARABILITY_LABELS = {
    Comparability.COMPARABLE: "ranked",
    Comparability.WINDOW_MISMATCH: "different window",
    Comparability.WINDOW_UNKNOWN: "no window published",
    Comparability.INCOTERM_UNBRIDGED: "incoterm not bridged",
    Comparability.MISSING_INPUT: "missing input",
    Comparability.STALE: "stale quote",
    Comparability.OBSERVATION_SPREAD: "observed too far apart",
}

COMPARABILITY_EXPLANATIONS = {
    Comparability.WINDOW_MISMATCH: (
        "This origin is quoting, but for a different shipment period. The level is "
        "shown so it is clear the market is open; it is not ranked, because "
        "comparing two shipment windows measures carry as much as competitiveness."
    ),
    Comparability.WINDOW_UNKNOWN: (
        "This source publishes a port-side level with no shipment period attached. "
        "It cannot be said to be quoted for this window — or any other — so it is "
        "shown and never ranked."
    ),
    Comparability.INCOTERM_UNBRIDGED: (
        "No costed path from this delivery term to FOB vessel is defined, so the "
        "quote cannot be put on the same footing as the others."
    ),
    Comparability.MISSING_INPUT: (
        "One or more cost inputs are absent or lapsed. The row blocks rather than "
        "defaulting them to zero — a partial landed cost is cheaper than a complete "
        "one and looks exactly like a finding."
    ),
    Comparability.STALE: (
        "The quote is past its own layer's recency budget, so the level is the last "
        "known good rather than today's market."
    ),
    Comparability.OBSERVATION_SPREAD: (
        "The origins on this board were observed too many days apart. Over that gap "
        "the difference between them is measuring the calendar as much as the market."
    ),
}


def _section(section_id: str, *, state: str, reason: str = "", data: Any = None) -> dict:
    for index, (sid, title, why) in enumerate(SECTION_SPECS, start=1):
        if sid == section_id:
            if state != STATE_OK and not reason.strip():
                raise ValueError(
                    f"section {section_id!r} is {state!r} with no reason — every empty "
                    "state must name its reason (M1 #143 constraint 2)"
                )
            return {
                "id": sid,
                "no": f"{index:02d}",
                "title": title,
                "why": why,
                "state": state,
                "reason": reason,
                "data": data,
            }
    raise KeyError(f"unknown section id {section_id!r}")


# ---------------------------------------------------------------------------
# Row and ranking views
# ---------------------------------------------------------------------------
def _row_view(row, rank: int | None) -> dict:
    quote = row.quote
    return {
        "rank": rank,
        "origin_key": quote.origin.key,
        "origin_name": quote.origin.name,
        "country": quote.origin.country,
        "country_iso": quote.origin.country_iso,
        "label": quote.source.detail or quote.origin.name,
        "href": quote.source.href,
        "quote_kind": quote.quote_kind.value,
        "incoterm": f"{quote.incoterm.value} {quote.carrier.value}",
        "grade": quote.grade.specification,
        "origin_usd_mt": quote.price.amount,
        "native_price": quote.native_price,
        "native_currency": quote.native_currency,
        "native_unit": quote.native_unit,
        "fx": quote.fx.to_dict() if quote.fx else None,
        "observation_date": quote.observation_date.isoformat(),
        "publication_date": (
            quote.publication_date.isoformat() if quote.publication_date else None
        ),
        "shipment_window": (
            quote.shipment_window.describe() if quote.shipment_window else None
        ),
        "board_contract": (
            f"{quote.board_contract.code} {quote.board_contract.delivery_month}"
            if quote.board_contract else None
        ),
        "fob_usd_mt": row.fob_equivalent.amount if row.fob_equivalent else None,
        "landed_usd_mt": row.landed_usd_mt,
        "comparability": row.comparability.value,
        "comparability_label": COMPARABILITY_LABELS[row.comparability],
        "comparability_explanation": COMPARABILITY_EXPLANATIONS.get(row.comparability),
        "is_ours": row.comparability in _OURS,
        "confidence": row.confidence.value,
        "freshness": row.freshness.value,
        "blockers": [blocker.to_dict() for blocker in row.blockers],
        "missing_inputs": [
            COMPONENT_LABELS[component] for component in row.missing_inputs
        ],
        "steps": [step.to_dict() for step in row.steps],
        "notes": list(row.notes),
        "source": quote.source.to_dict(),
    }


def _decision(ranking: OriginRanking) -> dict:
    ranks = {row.quote.origin.key: index for index, row in enumerate(ranking.rankable, start=1)}
    rows = [_row_view(row, ranks.get(row.quote.origin.key)) for row in ranking.rows]
    rows.sort(key=lambda row: (row["rank"] is None, row["rank"] or 0, row["origin_key"]))
    cheapest = ranking.cheapest
    return {
        "window": ranking.requested_window.describe(),
        "window_start": ranking.requested_window.start.isoformat(),
        "window_end": ranking.requested_window.end.isoformat(),
        "destination": ranking.destination.name,
        "destination_key": ranking.destination.key,
        "rows": rows,
        "rankable_count": len(ranking.rankable),
        "is_decisive": ranking.is_decisive,
        "cheapest_origin": cheapest.quote.origin.name if cheapest else None,
        "cheapest_key": cheapest.quote.origin.key if cheapest else None,
        "cheapest_landed": cheapest.landed_usd_mt if cheapest else None,
        "advantage_usd_mt": ranking.advantage_usd_mt,
        "advantage_pct": ranking.advantage_pct,
        "confidence": ranking.confidence.value,
        "observation_spread_days": ranking.observation_spread_days,
        "observation_spread_limit": config.ORIGIN_MAX_OBSERVATION_SPREAD_DAYS,
        "unavailable": [origin.to_dict() for origin in ranking.unavailable],
        "notes": list(ranking.notes),
        # The sentence the page leads with when it cannot rank. Written here
        # rather than in the template because it is a statement about the data,
        # and the template must not be able to soften it.
        "not_comparable_reason": _not_comparable_reason(ranking),
    }


def _not_comparable_reason(ranking: OriginRanking) -> str | None:
    if ranking.is_decisive:
        return None
    if not ranking.rows:
        return (
            "No origin quote could be read for this window at all. This is our ingest "
            "being quiet, not the origins withdrawing."
        )
    if len(ranking.rankable) == 1:
        only = ranking.rankable[0]
        return (
            f"Only {only.quote.origin.name} is comparable for {ranking.requested_window.describe()}. "
            "A ranking of one is not a ranking — 'cheapest origin' with nothing to be "
            "cheaper than reads like a finding and contains none."
        )
    ours = sorted({
        COMPONENT_LABELS[component]
        for row in ranking.rows
        for component in row.missing_inputs
    })
    if ours:
        return (
            "No origin can be costed to the berth: "
            + ", ".join(ours)
            + " are not entered for this route and window. Every blocked row below names "
            "the command that supplies its missing input. The FOB-equivalent board in "
            "section 03 is the part of the answer that does not need them."
        )
    return (
        "No two origins are comparable for this window. Each row below states why in "
        "its own terms."
    )


def _waterfall(ranking: OriginRanking) -> dict:
    ranks = {row.quote.origin.key: index for index, row in enumerate(ranking.rankable, start=1)}
    return {
        "window": ranking.requested_window.describe(),
        "origins": [
            {
                **_row_view(row, ranks.get(row.quote.origin.key)),
                "step_count": len(row.steps),
            }
            for row in ranking.rows
        ],
        "method_version": ranking.method_version,
        "assumption_set_id": ranking.assumption_set_id,
    }


def _fob(ranking: OriginRanking) -> dict:
    board = fob_board(ranking)
    edge, pct = fob_advantage(ranking)
    excluded = [
        _row_view(row, None)
        for row in ranking.rows
        if row not in board
    ]
    return {
        "window": ranking.requested_window.describe(),
        "rows": [
            {**_row_view(row, index), "fob_usd_mt": row.fob_equivalent.amount}  # type: ignore[union-attr]
            for index, row in enumerate(board, start=1)
        ],
        "excluded": excluded,
        "advantage_usd_mt": edge,
        "advantage_pct": pct,
        "caveat": (
            "FOB-equivalent means every origin normalised to the same delivery term at "
            "its own loading port — the US Gulf bid has had its barge-to-vessel "
            "elevation added. It is NOT the answer to the origin question: two origins "
            "level FOB are not level delivered, and the freight difference to North "
            "China is routinely larger than the FOB spread between them."
        ),
    }


def _readiness(assumptions: AssumptionSet, ranking: OriginRanking, today: date) -> dict:
    """What each declared route still needs before it can be costed.

    Built from the registry and the entered set, never from the ranking's rows:
    a route whose price series is quiet still has an onboarding state, and
    "we have no freight for this leg" is a different sentence from "this origin
    did not print today" — the page has to be able to say both.
    """
    routes = readiness_mod.assess_routes(
        assumptions,
        window=ranking.requested_window,
        today=today,
        destination_key=ranking.destination.key,
    )
    outstanding = sorted({
        requirement.component.value
        for route in routes
        for requirement in route.blocking
    })
    return {
        "window": ranking.requested_window.describe(),
        "destination": ranking.destination.name,
        "routes": [route.to_dict() for route in routes],
        "ready_count": sum(1 for route in routes if route.is_ready),
        # Counted against the legs that *have* a price series: the PNW leg is
        # declared with none, so no amount of entering makes it costable and
        # putting it in the denominator would read as work outstanding.
        "route_count": sum(1 for route in routes if route.unavailable_reason is None),
        "unavailable_count": sum(1 for route in routes if route.unavailable_reason is not None),
        "outstanding_components": outstanding,
        "outstanding_labels": [
            COMPONENT_LABELS[CostComponent(name)] for name in outstanding
        ],
        "note": (
            "A route is ready when every input it needs is entered and live — which is "
            "not the same as being ranked. An origin quoting a different shipment "
            "window, or none at all, is still not compared, and that is a fact about "
            "the market rather than about the onboarding."
        ),
        "placeholder_note": (
            "Each command carries a <VALUE> placeholder rather than a starting number. "
            "A suggested default is a fabricated default with an extra step, and the "
            "one nobody changes is the one that decides the ranking."
        ),
    }


def _renewals(assumptions: AssumptionSet, ranking: OriginRanking, today: date) -> dict:
    return readiness_mod.expiry_review(
        assumptions,
        today=today,
        horizon_days=RENEWAL_HORIZON_DAYS,
        window=ranking.requested_window,
        destination_key=ranking.destination.key,
    )


def _sensitivity(ranking: OriginRanking, assumptions: AssumptionSet, today: date) -> dict:
    results = scenarios_mod.run_panel(ranking, assumptions, today=today)
    breakeven = scenarios_mod.freight_breakeven(ranking)
    flips = scenarios_mod.input_flip_moves(ranking)
    fragile = scenarios_mod.most_fragile_input(ranking)
    return {
        "flip_moves": [move.to_dict() for move in flips],
        "most_fragile": fragile.to_dict() if fragile else None,
        "flip_note": (
            "Every input, solved rather than searched: each rung is linear in its own "
            "value given the others, so the move that levels the two landed totals is "
            "exact. Rows are ordered by how wrong the input would have to be — a "
            "percentage of its own entered value — because a dollar of freight and a "
            "point of duty are not comparable as written. An input both origins share "
            "moves both landed totals together and mostly cannot flip anything; that is "
            "said in words rather than shown as a large number."
        ),
        **_scenario_panel(ranking, results, breakeven),
    }


def _scenario_panel(ranking: OriginRanking, results, breakeven) -> dict:
    return {
        "window": ranking.requested_window.describe(),
        "base_cheapest": ranking.cheapest.quote.origin.name if ranking.cheapest else None,
        "scenarios": [
            {
                **result.to_dict(),
                "cheapest_name": (
                    result.ranking.cheapest.quote.origin.name
                    if result.ranking.cheapest else None
                ),
            }
            for result in results
        ],
        "flips": [
            result.scenario.label for result in results if result.flips_the_answer
        ],
        "breakeven": breakeven.to_dict() if breakeven else None,
        "breakeven_note": (
            "Solved, not searched: on a duty-paying route one dollar of freight costs "
            "more than one dollar landed, because duty and VAT are charged on top of "
            "it. The multiplier is read off this row's own waterfall."
        ),
    }


def _crush(conn, assumptions: AssumptionSet, today: date) -> dict:
    markets = sorted(config.PHYSICAL_CRUSH)
    stacks = []
    for slug in markets:
        try:
            board, gross, net = crush_mod.crush_stack(conn, slug, assumptions, today=today)
        except Exception:  # noqa: BLE001 — one market must not cost the section
            log.warning("crush stack failed for %s", slug, exc_info=True)
            continue
        stacks.append({
            "market": slug,
            "market_name": config.MARKETS.get(slug, {}).get("name", slug),
            "href": f"markets/{slug}.html",
            "levels": [level.to_dict() for level in (board, gross, net)],
        })
    return {
        "stacks": stacks,
        "yields": crush_mod.YIELD_SETS["soy_board"],
        "yield_note": (
            "Yields are the co-product assumption and they are a real sensitivity: a "
            "plant running 18.6% oil against a nominal 18.3% earns a different margin "
            "at identical prices."
        ),
    }


def _counterparties(ranking: OriginRanking, today: date) -> dict:
    lanes = players_mod.counterparties_for_ranking(ranking)
    return {
        "destination": ranking.destination.name,
        "destination_iso": ranking.destination.country_iso,
        "lanes": [
            {
                "origin_iso": iso,
                **lane.to_dict(today),
            }
            for iso, lane in lanes.items()
        ],
        "stale_after_days": players_mod.VERIFICATION_STALE_DAYS,
        "note": (
            "Candidates, not counterparties: an entry appears because its own research "
            "says it originates or buys the right product in the right country. A "
            "'lane evidenced' badge means that entry's own prose names this "
            "destination, which is a stronger claim and is marked as one."
        ),
    }


def _inputs(ranking: OriginRanking, assumptions: AssumptionSet, today: date) -> dict:
    live = assumptions.live(today)
    lapsing = assumptions.expiring_within(30, on=today)
    expired = [a for a in assumptions.assumptions if not a.is_live(today)]
    used = {
        step["assumption_id"]
        for row in ranking.rows
        for step in (step.to_dict() for step in row.steps)
        if step["assumption_id"]
    }
    needed = sorted({
        component.value
        for row in ranking.rows
        for component in row.missing_inputs
    })
    return {
        "set_id": assumptions.set_id,
        "method_version": ranking.method_version,
        "files": list(assumptions.loaded_from),
        "live": [a.to_dict() for a in live],
        "lapsing": [a.to_dict() for a in lapsing],
        "expired": [a.to_dict() for a in expired],
        "used_ids": sorted(used),
        "missing_components": needed,
        "missing_labels": [COMPONENT_LABELS[CostComponent(name)] for name in needed],
        "directory": "data/reference/assumptions/",
        "lineage": [
            {
                "origin": row.quote.origin.name,
                "layer": row.quote.source.layer,
                "table": row.quote.source.table,
                "key": row.quote.source.key,
                "href": row.quote.source.href,
                "observation_date": row.quote.observation_date.isoformat(),
                "quote_kind": row.quote.quote_kind.value,
                "fx": row.quote.fx.to_dict() if row.quote.fx else None,
            }
            for row in ranking.rows
        ],
    }


def _changes(ranking: OriginRanking, conn, today: date) -> tuple[str, str, dict]:
    """Ranking and input movement since the previous archived run."""
    if conn is None:
        return STATE_EMPTY, "no database — nothing archived to compare against", {}
    current = history_mod.ranking_rows(ranking)
    try:
        rows = conn.execute(
            "SELECT run_date, origin, rank, snapshot_json FROM origin_rankings "
            "WHERE destination = ? AND window_start = ? AND run_date < ? "
            "ORDER BY run_date DESC",
            (
                ranking.destination.key,
                ranking.requested_window.start.isoformat(),
                today.isoformat(),
            ),
        ).fetchall()
    except Exception as exc:  # noqa: BLE001 — a missing table is data, not a crash
        log.debug("origin history unavailable: %s", exc)
        return STATE_EMPTY, (
            "the origin_rankings archive does not exist in this database yet — it is "
            "created by the pipeline's init_database() and filled one run at a time"
        ), {}
    if not rows:
        return STATE_EMPTY, (
            "no earlier run is archived for this destination and window — this page "
            "builds its own history one run at a time"
        ), {}
    previous_date = rows[0][0]
    previous = [
        {"origin": origin, "rank": rank, "snapshot_json": snapshot}
        for run_date, origin, rank, snapshot in rows
        if run_date == previous_date
    ]
    return STATE_OK, "", {
        "previous_run": str(previous_date)[:10],
        "rank_changes": [change.to_dict() for change in history_mod.rank_changes(previous, current)],
        "input_changes": [
            change.to_dict()
            for change in history_mod.input_changes(previous, current)
        ],
        "threshold": history_mod.MATERIAL_MOVE_USD_MT,
        "note": (
            "A re-entered assumption and a market move look identical in a series of "
            "one number, so the two are separated here: a step whose assumption id "
            "changed is bookkeeping, a step whose value moved with the same id is the "
            "market."
        ),
    }


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------
def build_view(
    conn,
    *,
    today: date,
    assumptions: AssumptionSet | None = None,
    destination_key: str | None = None,
) -> dict:
    """Everything the Origin Comparison page renders.

    Every offered shipment window is costed here, because the page is a static
    file and the selector has nothing to ask. The cost of that is linear in the
    window count and the arithmetic is trivial; the benefit is a selector that
    actually selects.
    """
    assumptions = assumptions if assumptions is not None else load_assumptions()
    destination_key = destination_key or next(iter(config.DESTINATION_PORTS))
    windows = offered_windows(today)

    views: list[dict] = []
    rankings: list[OriginRanking] = []
    for index, window in enumerate(windows):
        ranking = build_ranking(
            conn,
            destination_key=destination_key,
            window=window,
            today=today,
            assumptions=assumptions,
        )
        rankings.append(ranking)
        views.append({
            "key": window.start.isoformat(),
            "label": window.describe(),
            "is_default": index == 1 or (index == 0 and len(windows) == 1),
            "decision": _decision(ranking),
            "waterfall": _waterfall(ranking),
            "fob": _fob(ranking),
            "sensitivity": _sensitivity(ranking, assumptions, today),
            "readiness": _readiness(assumptions, ranking, today),
        })

    # Sections that do not vary with the window are built once, off the default
    # window's ranking — rendering seven identical counterparty tables would be
    # seven chances for them to disagree.
    default_index = next((i for i, view in enumerate(views) if view["is_default"]), 0)
    default_ranking = rankings[default_index]

    changes_state, changes_reason, changes_data = _changes(default_ranking, conn, today)

    sections = [
        _section("decision", state=STATE_OK),
        _section("readiness", state=STATE_OK),
        _section("waterfall", state=STATE_OK),
        _section("fob", state=STATE_OK),
        _section("sensitivity", state=STATE_OK),
        _section("crush", state=STATE_OK, data=_crush(conn, assumptions, today)),
        _section(
            "counterparties",
            state=STATE_OK,
            data=_counterparties(default_ranking, today),
        ),
        _section("inputs", state=STATE_OK, data=_inputs(default_ranking, assumptions, today)),
        _section(
            "renewals",
            state=STATE_OK,
            data=_renewals(assumptions, default_ranking, today),
        ),
        _section("changes", state=changes_state, reason=changes_reason, data=changes_data),
    ]

    return {
        "today": today.isoformat(),
        "destination": {
            "key": destination_key,
            **{
                key: value for key, value in config.DESTINATION_PORTS[destination_key].items()
                if key != "market"
            },
        },
        "destinations": [
            {"key": key, "name": value["name"]}
            for key, value in config.DESTINATION_PORTS.items()
        ],
        "windows": [
            {"key": view["key"], "label": view["label"], "is_default": view["is_default"]}
            for view in views
        ],
        "views": views,
        "sections": sections,
        "default_window": views[default_index]["key"],
        "method_version": config.LANDED_COST_METHOD_VERSION,
        "assumption_set_id": assumptions.set_id,
        "rankings": rankings,
        "selector_note": (
            "This is a static page: every window below was costed when it was "
            f"generated, {config.ORIGIN_WINDOW_MONTHS_AHEAD} months forward. A window "
            "beyond that is not offered rather than offered and empty."
        ),
        "destination_note": (
            "One destination is configured. Adding another is a "
            "config.DESTINATION_PORTS entry and a freight assumption, not code."
        ) if len(config.DESTINATION_PORTS) == 1 else None,
    }


__all__ = [
    "COMPARABILITY_EXPLANATIONS",
    "COMPARABILITY_LABELS",
    "SECTION_SPECS",
    "STATE_ABSENT",
    "STATE_EMPTY",
    "STATE_OK",
    "build_view",
]
