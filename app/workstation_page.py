"""Data for the Futures Workstation page (Phase 3) — data only, never markup.

Same split every other page family keeps (M18 #214, and ``app/origins_page.py``
for Phase 2): this module returns numbers and labels, and
``app/templates/workstation.html.j2`` decides how they look. The reason is the
same too — a hedge section and a P&L section that render differently teach a
trader that one is more trustworthy than the other, and here they are equally
trustworthy and equally delayed.

Section order is the order a hedger asks the questions:

    01  what needs attention — alerts tied to exposure, not to indicators
    02  the contracts — named months, expiry, price, and what kind of price
    03  the term structure — spreads, carry, percentile
    04  the hedge — sizing, coverage, residual, basis and FX left over
    05  scenarios — futures, basis and FX moved together
    06  the ticket — the proposal, not routed
    07  the book — entered positions, marks, P&L, limits
    08  what is scheduled — releases from sources this project ingests
    09  options — the interface, and why there is no chain
    10  provider and method — where every number came from

A static site has no server, so everything the page offers is computed at build
time. The hedge section therefore works from the **entered book** when one
exists, and from a clearly-labelled reference calculation when it does not. The
reference calculation is not a position and the page says so on the row itself:
a worked 1,000 MT example makes the arithmetic inspectable, which is worth more
than an empty section, but only while it cannot be mistaken for somebody's
actual exposure.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, datetime, timedelta, timezone
from typing import Any

from analysis.futures import alerts as alerts_mod
from analysis.futures import continuous as continuous_mod
from analysis.futures import events as events_mod
from analysis.futures import options as options_mod
from analysis.futures import positions as positions_mod
from analysis.futures import scenarios as scenarios_mod
from analysis.futures.curve import CurveAnalysis, analyse_curve
from analysis.futures.domain import (
    CONTRACT_SPECS,
    METHOD_VERSION,
    SOY_COMPLEX,
    ExpiryConfidence,
    Side,
    spec_for,
)
from analysis.futures.hedge import (
    BasisConvention,
    PhysicalExposure,
    PhysicalUnit,
    Rounding,
    fx_exposure_from_rate,
    propose_crush_hedge,
    propose_hedge,
)
from analysis.futures.providers import SqliteQuoteProvider, describe_provider, open_provider
from analysis.futures.ticket import build_ticket

log = logging.getLogger(__name__)

STATE_OK = "ok"
STATE_EMPTY = "empty"
STATE_ABSENT = "absent"

SECTION_SPECS = (
    ("alerts", "Exposure alerts", "expiry, roll, slippage, basis, limits, inversion, staleness"),
    ("contracts", "Named contracts", "the month, the expiry, and what kind of price this is"),
    ("curve", "Term structure", "calendar spreads, annualised carry and where they sit"),
    ("hedge", "Hedge calculator", "contracts, coverage, and what the hedge leaves behind"),
    ("scenarios", "Scenarios", "futures, basis and FX moved together, netted against the hedge"),
    ("ticket", "Proposed ticket", "a proposal — not routed, and never routable from here"),
    ("book", "Positions & P&L", "what was entered, what it marks at, and which limits are crossed"),
    ("calendar", "Release calendar", "scheduled releases from sources this project ingests"),
    ("options", "Options", "the interface, the model, and why there is no chain"),
    ("provider", "Provider & method", "where every number on this page came from"),
)

#: The working set. The soy complex is the product this project is about, and
#: a workstation covering nine commodities equally would bury it.
WORKSTATION_COMMODITIES = SOY_COMPLEX

#: A worked example, used only when no book has been entered. Labelled on the
#: page and in every number it produces.
REFERENCE_QUANTITY_MT = 1_000.0
REFERENCE_LABEL = (
    "reference calculation — a worked 1,000 MT example so the arithmetic can be checked. "
    "This is not a position and no position has been entered."
)


def _section(section_id: str, *, state: str, reason: str = "", data: Any = None) -> dict[str, Any]:
    index, title, why = next(
        ((position, spec[1], spec[2]) for position, spec in enumerate(SECTION_SPECS, start=1)
         if spec[0] == section_id),
        (0, section_id, ""),
    )
    if state != STATE_OK and not reason.strip():
        raise ValueError(f"section {section_id!r} is {state!r} with no reason")
    return {
        "id": section_id, "no": f"{index:02d}", "title": title, "why": why,
        "state": state, "reason": reason, "data": data,
    }


# ---------------------------------------------------------------------------
# Building blocks
# ---------------------------------------------------------------------------


def _curves(provider: SqliteQuoteProvider, *, as_of: date) -> dict[str, CurveAnalysis]:
    out: dict[str, CurveAnalysis] = {}
    for commodity in WORKSTATION_COMMODITIES:
        observation = provider.curve(commodity, as_of=as_of)
        history = provider.curve_history(commodity, as_of=as_of, sessions=120)
        out[commodity] = analyse_curve(observation, as_of=as_of, history=history)
    return out


def _reference_exposure(commodity: str, *, as_of: date) -> PhysicalExposure:
    """The worked example. Deliberately dull: long, USD, basis stated as zero."""
    return PhysicalExposure(
        commodity=commodity,
        side=Side.LONG,
        quantity=REFERENCE_QUANTITY_MT,
        unit=PhysicalUnit.METRIC_TON,
        pricing_start=as_of,
        # A quarter out — long enough that the front month is usually not the
        # answer, which is the part of the month-selection rule worth showing.
        pricing_end=as_of + timedelta(days=90),
        basis_convention=BasisConvention.UNPRICED,
        basis_usd_per_mt=None,
        note=REFERENCE_LABEL,
    )


def _exposures_from_book(book: positions_mod.Book, *, as_of: date) -> list[PhysicalExposure]:
    """Turn entered physical positions into hedgeable exposures.

    A position is what you have; an exposure is what you have *left to price*.
    They coincide only for an unpriced or basis-priced position, which is why
    the basis convention is carried through from the file rather than assumed.
    """
    out: list[PhysicalExposure] = []
    for position in book.physical:
        if position.commodity not in CONTRACT_SPECS:
            continue
        out.append(PhysicalExposure(
            commodity=position.commodity,
            side=position.side,
            quantity=position.quantity,
            unit=position.unit,
            pricing_start=as_of,
            pricing_end=as_of + timedelta(days=365),
            basis_convention=BasisConvention.BASIS_OVER_FUTURES,
            basis_usd_per_mt=position.current_basis_usd_mt,
            basis_source="entered with the position",
            currency=position.currency,
            fx_pair=position.fx_pair,
            note=position.note,
        ))
    return out


def build_view(
    conn: sqlite3.Connection,
    *,
    today: date | None = None,
    generated_at: datetime | None = None,
    positions_dir: str | None = None,
) -> dict[str, Any]:
    """Everything the workstation template renders, as plain data."""
    as_of = today or date.today()
    generated_at = generated_at or datetime.now(timezone.utc)
    provider = open_provider(conn)

    curves = _curves(provider, as_of=as_of)
    book = _load_book(positions_dir)
    valuation = _value_book(book, provider, curves, as_of=as_of)

    exposures = _exposures_from_book(book, as_of=as_of)
    is_reference = not exposures
    if is_reference:
        exposures = [_reference_exposure("Soybeans", as_of=as_of)]

    proposals = []
    for exposure in exposures:
        analysis = curves.get(exposure.commodity)
        if analysis is None or analysis.is_empty:
            continue
        fx = fx_exposure_from_rate(
            exposure,
            analysis.front_price_usd_mt,
            provider.fx_rate(exposure.fx_pair, on=as_of) if exposure.fx_pair else None,
        )
        proposals.append(propose_hedge(exposure, analysis, as_of=as_of, fx=fx, rounding=Rounding.NEAREST))

    crush_proposal = _crush_proposal(exposures, curves, as_of=as_of)

    scenario_results = tuple(
        scenarios_mod.run_panel(proposal, scenarios_mod.default_panel_for(proposal))
        for proposal in proposals
    )

    tickets = tuple(
        build_ticket(proposal, generated_at=generated_at, scenarios=results)
        for proposal, results in zip(proposals, scenario_results, strict=False)
    )

    page_alerts = alerts_mod.build_alerts(
        as_of=as_of,
        proposals=tuple(proposals),
        curves=tuple(curves.values()),
        valuation=valuation,
    )

    return {
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "method_version": METHOD_VERSION,
        "is_reference_calculation": is_reference,
        "reference_label": REFERENCE_LABEL,
        "sections": [
            _alerts_section(page_alerts),
            _contracts_section(curves),
            _curve_section(curves),
            _hedge_section(proposals, crush_proposal, is_reference),
            _scenarios_section(proposals, scenario_results, is_reference),
            _ticket_section(tickets, is_reference),
            _book_section(book, valuation),
            _calendar_section(conn, as_of=as_of),
            _options_section(curves),
            _provider_section(provider, curves, as_of=as_of),
        ],
    }


def _load_book(positions_dir: str | None) -> positions_mod.Book:
    try:
        return positions_mod.load_book(positions_dir)
    except positions_mod.PositionError:
        raise
    except Exception:  # noqa: BLE001 — a missing yaml module must not kill the page
        log.warning("could not load the position book", exc_info=True)
        return positions_mod.Book()


def _value_book(
    book: positions_mod.Book,
    provider: SqliteQuoteProvider,
    curves: dict[str, CurveAnalysis],
    *,
    as_of: date,
) -> positions_mod.BookValuation | None:
    if book.is_empty:
        return None

    def quote_for(commodity: str, symbol: str | None):
        analysis = curves.get(commodity)
        if analysis is None:
            analysis = analyse_curve(provider.curve(commodity, as_of=as_of), as_of=as_of)
            curves[commodity] = analysis
        if symbol is None:
            return analysis.front.quote if analysis.front else None
        for leg in analysis.legs:
            if leg.contract.symbol == symbol.upper():
                return leg.quote
        return None

    def fx_for(pair: str | None):
        return provider.fx_rate(pair, on=as_of) if pair else None

    return positions_mod.value_book(book, as_of=as_of, quote_for=quote_for, fx_for=fx_for)


def _crush_proposal(exposures, curves, *, as_of: date):
    bean = next((e for e in exposures if e.commodity == "Soybeans" and e.side is Side.LONG), None)
    if bean is None:
        return None
    needed = ("Soybeans", "Soybean Meal", "Soybean Oil")
    if any(curves.get(name) is None or curves[name].is_empty for name in needed):
        return None
    return propose_crush_hedge(
        bean, curves["Soybeans"], curves["Soybean Meal"], curves["Soybean Oil"], as_of=as_of
    )


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


def _alerts_section(page_alerts) -> dict[str, Any]:
    if not page_alerts:
        return _section("alerts", state=STATE_EMPTY, reason=(
            "nothing to flag: no contract is near expiry or first notice, no limit is crossed, "
            "no curve is inverted or stale"
        ))
    return _section("alerts", state=STATE_OK, data={
        "alerts": [alert.to_dict() for alert in page_alerts],
        "counts": {
            severity: sum(1 for alert in page_alerts if alert.severity == severity)
            for severity in alerts_mod.SEVERITIES
        },
    })


def _contracts_section(curves: dict[str, CurveAnalysis]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for commodity, analysis in curves.items():
        if analysis.is_empty:
            rows.append({
                "commodity": commodity,
                "state": STATE_EMPTY,
                "reason": analysis.coherence_note or "no curve stored for this commodity",
                "legs": [],
            })
            continue
        rows.append({
            "commodity": commodity,
            "display": spec_for(commodity).display,
            "state": STATE_OK,
            "coherent": analysis.coherent,
            "coherence_note": analysis.coherence_note,
            "observation_date": analysis.observation_date.isoformat() if analysis.observation_date else None,
            "freshness": analysis.freshness,
            "age_days": analysis.age_days,
            "expiry_confidence": spec_for(commodity).expiry_confidence.value,
            "contract_size": spec_for(commodity).contract_size,
            "size_unit": spec_for(commodity).size_unit.value,
            "mt_per_contract": round(spec_for(commodity).mt_per_contract, 4),
            "native_unit": spec_for(commodity).native_unit.value,
            "tick_size": spec_for(commodity).tick_size,
            "tick_value_usd": spec_for(commodity).tick_value_usd,
            "legs": [leg.to_dict() for leg in analysis.legs],
        })
    if not any(row["state"] == STATE_OK for row in rows):
        return _section("contracts", state=STATE_EMPTY, reason=(
            "no forward-curve rows for the soy complex — the Layer 11 fetch has not run, or "
            "every leg it returned was dropped as being from another session"
        ))
    return _section("contracts", state=STATE_OK, data={"commodities": rows})


def _curve_section(curves: dict[str, CurveAnalysis]) -> dict[str, Any]:
    rows = []
    for commodity, analysis in curves.items():
        if analysis.is_empty or len(analysis.legs) < 2:
            continue
        rows.append({
            "commodity": commodity,
            "display": spec_for(commodity).display,
            "structure": analysis.structure.value,
            "implication": analysis.structure.implication,
            "inverted": analysis.is_inverted,
            "coherent": analysis.coherent,
            "coherence_note": analysis.coherence_note,
            "slope_usd_mt_per_month": (
                None if analysis.slope_per_month_usd_mt is None
                else round(analysis.slope_per_month_usd_mt, 2)
            ),
            "spreads": [spread.to_dict() for spread in analysis.spreads],
            "histories": [history.to_dict() for history in analysis.histories],
            "volume_available": analysis.volume_available,
            "open_interest_available": analysis.open_interest_available,
        })
    if not rows:
        return _section("curve", state=STATE_EMPTY, reason=(
            "a term structure needs at least two legs observed on the same session; no commodity "
            "currently has that"
        ))
    from analysis.futures.curve import OPEN_INTEREST_UNAVAILABLE

    return _section("curve", state=STATE_OK, data={
        "commodities": rows,
        "open_interest_note": OPEN_INTEREST_UNAVAILABLE,
    })


def _hedge_section(proposals, crush_proposal, is_reference: bool) -> dict[str, Any]:
    if not proposals:
        return _section("hedge", state=STATE_EMPTY, reason=(
            "no hedge could be sized: there is no curve to place one against"
        ))
    return _section("hedge", state=STATE_OK, data={
        "is_reference": is_reference,
        "reference_label": REFERENCE_LABEL if is_reference else "",
        "proposals": [proposal.to_dict() for proposal in proposals],
        "crush": crush_proposal.to_dict() if crush_proposal is not None else None,
        "crush_note": (
            "A crusher who owns beans is long the crush, not the flat price: short the beans, "
            "long the meal and oil at the mass-balance yields. Coverage is reported on the bean "
            "leg alone — the three legs hedge one position once."
        ),
    })


def _scenarios_section(proposals, results, is_reference: bool) -> dict[str, Any]:
    if not results or not any(results):
        return _section("scenarios", state=STATE_EMPTY, reason="no hedge to run scenarios against")
    return _section("scenarios", state=STATE_OK, data={
        "is_reference": is_reference,
        "panels": [
            {
                "commodity": proposal.exposure.commodity,
                "quantity_mt": round(proposal.exposure.quantity_mt, 1),
                "results": [result.to_dict() for result in panel],
            }
            for proposal, panel in zip(proposals, results, strict=False)
        ],
    })


def _ticket_section(tickets, is_reference: bool) -> dict[str, Any]:
    if not tickets:
        return _section("ticket", state=STATE_EMPTY, reason="no hedge to write a ticket for")
    from analysis.futures.ticket import DISCLAIMER, NOT_ROUTED_BANNER

    return _section("ticket", state=STATE_OK, data={
        "is_reference": is_reference,
        "banner": NOT_ROUTED_BANNER,
        "disclaimer": DISCLAIMER,
        "tickets": [
            {"id": ticket.ticket_id, "text": ticket.to_text(), "json": ticket.to_json()}
            for ticket in tickets
        ],
    })


def _book_section(book: positions_mod.Book, valuation) -> dict[str, Any]:
    if book.is_empty or valuation is None:
        return _section("book", state=STATE_EMPTY, reason=(
            "no positions entered. Add a YAML document under data/reference/positions/ or import "
            "a CSV — this project ingests no account or clearing feed, so a position can only "
            "come from you"
        ))
    return _section("book", state=STATE_OK, data={
        "loaded_from": list(book.loaded_from),
        "valuation": valuation.to_dict(),
        "limits": [limit.to_dict() for limit in book.limits],
    })


def _calendar_section(conn, *, as_of: date) -> dict[str, Any]:
    calendar = events_mod.build_calendar(conn, as_of=as_of, horizon_days=45)
    if not calendar:
        return _section("calendar", state=STATE_EMPTY, reason=(
            "no scheduled release from an ingested source falls inside the next 45 days"
        ))
    return _section("calendar", state=STATE_OK, data={
        "events": [event.to_dict() for event in calendar],
        "scope_note": (
            "Only releases this project actually ingests are listed. Dates are computed from each "
            "agency's published cadence rule, not from a schedule file we read, so they can shift "
            "around a federal holiday; the observed column is our own evidence."
        ),
    })


def _options_section(curves: dict[str, CurveAnalysis]) -> dict[str, Any]:
    front = next(
        (analysis.front.contract for analysis in curves.values() if analysis.front is not None),
        None,
    )
    return _section("options", state=STATE_OK, data=options_mod.chain_status(front))


def _provider_section(provider, curves, *, as_of: date) -> dict[str, Any]:
    series = {}
    for commodity in curves:
        try:
            stitched = continuous_mod.build_continuous(provider, commodity, as_of=as_of)
        except Exception:  # noqa: BLE001 — a series is never worth the page
            log.warning("could not build a continuous series for %s", commodity, exc_info=True)
            stitched = None
        series[commodity] = continuous_mod.describe_provider_series(stitched)
        series[commodity]["provider_series"] = continuous_mod.describe_provider_series(
            provider.continuous(commodity, as_of=as_of)
        )

    return _section("provider", state=STATE_OK, data={
        "provider": describe_provider(provider.provider),
        "price_type": provider.price_type.value,
        "method_version": METHOD_VERSION,
        "continuous": series,
        "expiry_rules": [
            {
                "commodity": name,
                "root": spec.root,
                "exchange": spec.exchange.value,
                "confidence": spec.expiry_confidence.value,
                "rule": spec.expiry_rule.value if spec.expiry_rule else "not encoded",
                "first_notice_rule": spec.first_notice_rule or "not encoded",
            }
            for name, spec in CONTRACT_SPECS.items()
        ],
        "not_encoded": [
            name for name, spec in CONTRACT_SPECS.items()
            if spec.expiry_confidence is ExpiryConfidence.NOT_ENCODED
        ],
        "no_routing_note": (
            "This workstation has no connection to any venue, broker or execution system. "
            "Every output is a proposal or a report."
        ),
    })


__all__ = ["SECTION_SPECS", "WORKSTATION_COMMODITIES", "build_view"]
