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
    07  the book — entered positions, marks, P&L                    [private]
    08  exposure — flat price, basis, crush, FX, month, notice, residual [private]
    09  limits — every configured line, its headroom, its breaches   [private]
    10  clearing — the official P&L beside ours, never merged        [private]
    11  what is scheduled — releases from sources this project ingests
    12  options — the interface, the model, and why there is no chain
    13  entered options — the desk's own quotes, valued              [private]
    14  provider and method — where every number came from

**Two editions.** Sections marked ``[private]`` exist only because a client
entered something, and a book is front-runnable: it says what somebody owns,
at what cost, and where they are close to a mandate. The public edition renders
those five sections ``absent`` with a reason saying so — not ``empty``, which
would be a claim that the desk holds nothing — and the private edition renders
them in full and is written outside ``docs/``. The audience is a parameter of
:func:`build_view` and its default is **public**, so forgetting it is safe.

This is not a hypothetical. Until Phase 6 ``_book_section`` wrote
``valuation.to_dict()`` and the absolute path of the positions file into
``docs/workstation.html``, which is on the promotion contract and is uploaded
to Pages; it stayed quiet only because ``data/reference/positions/`` is empty
in CI. ``tests/test_workstation_privacy.py`` renders both editions and greps.

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
from analysis.futures import clearing as clearing_mod
from analysis.futures import continuous as continuous_mod
from analysis.futures import crush as crush_mod
from analysis.futures import events as events_mod
from analysis.futures import exposure as exposure_mod
from analysis.futures import options as options_mod
from analysis.futures import positions as positions_mod
from analysis.futures import scenarios as scenarios_mod
from analysis.futures.curve import CurveAnalysis, CurveLeg, analyse_curve
from analysis.futures.domain import (
    CONTRACT_SPECS,
    METHOD_VERSION,
    NO_NOTICE_DAY,
    SOY_COMPLEX,
    AggregateOpenInterest,
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
from analysis.futures.privacy import (
    AUDIENCE_PUBLIC,
    AUDIENCES,
    PRIVATE_SECTION_IDS,
    redact_for_public,
)
from analysis.futures.providers import SqliteQuoteProvider, describe_provider, open_provider
from analysis.futures.ticket import build_ticket
from app.tradingview import (
    TRADINGVIEW_STAMP,
    tradingview_symbol,
    tradingview_url,
)

log = logging.getLogger(__name__)

STATE_OK = "ok"
STATE_EMPTY = "empty"
STATE_ABSENT = "absent"

SECTION_SPECS = (
    ("alerts", "Exposure alerts", "expiry, roll, slippage, basis, limits, inversion, staleness"),
    ("contracts", "Named contracts", "the month, the expiry, and what kind of price this is"),
    ("curve", "Term structure", "calendar spreads, annualised carry and where they sit"),
    ("crush", "Board crush", "three named contracts of one crush period, on one session"),
    ("hedge", "Hedge calculator", "contracts, coverage, and what the hedge leaves behind"),
    ("scenarios", "Scenarios", "futures, basis and FX moved together, netted against the hedge"),
    ("ticket", "Proposed ticket", "a proposal — not routed, and never routable from here"),
    ("book", "Positions & P&L", "what was entered, what it marks at, on a management basis"),
    ("exposure", "Exposure", "flat price, basis, crush, FX, month, first notice and residual"),
    ("limits", "Desk limits", "every configured line, its headroom, and which are crossed"),
    ("clearing", "Clearing reconciliation", "the official P&L beside ours — reported, never merged"),
    ("calendar", "Release calendar", "scheduled releases from sources this project ingests"),
    ("options", "Options", "the interface, the model, its limits, and why there is no chain"),
    ("options_entered", "Entered options", "quotes this desk supplied, valued and labelled"),
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

#: Discount rate for the manual Black-76 valuations. A stated, inspectable
#: constant rather than a number lifted from the `economic` layer: Black-76
#: discounts the payoff at a risk-free rate the *user* should be choosing, and
#: silently substituting a 10-year Treasury yield would hide that choice inside
#: a premium. Rho is reported per rate point so the sensitivity to this number
#: is visible on every row.
OPTION_DISCOUNT_RATE = 0.04
OPTION_RATE_NOTE = (
    "Model values below discount at a stated 4.0% continuously-compounded rate. It is a "
    "constant on this page, not a sourced number — rho on each row shows what a different "
    "rate would be worth."
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
    options_dir: str | None = None,
    clearing_dir: str | None = None,
    audience: str = AUDIENCE_PUBLIC,
) -> dict[str, Any]:
    """Everything the workstation template renders, as plain data.

    ``audience`` decides whether the five client sections are rendered or
    reported absent. It defaults to **public**: a caller who forgets the
    argument gets the safe edition, which is the only default a privacy
    boundary may have.
    """
    if audience not in AUDIENCES:
        raise ValueError(
            f"audience {audience!r} is not one of {list(AUDIENCES)} — there is no third "
            "edition, and an unrecognised one must not fall through to the private view"
        )
    is_private = audience != AUDIENCE_PUBLIC
    as_of = today or date.today()
    generated_at = generated_at or datetime.now(timezone.utc)
    provider = open_provider(conn)

    curves = _curves(provider, as_of=as_of)
    book = _load_book(positions_dir)
    valuation = _value_book(book, provider, curves, as_of=as_of)

    # The hedge, its scenarios and its ticket are sized from the entered book
    # when there is one — which makes all three of them the book, restated in
    # lots. The public edition therefore always works the reference example,
    # exactly as it does for a clone that has entered nothing: the arithmetic
    # stays inspectable and the tonnage is ours, not the desk's.
    exposures = _exposures_from_book(book, as_of=as_of) if is_private else []
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

    # Whole-product open interest, per commodity, from the weekly COT report.
    # Read here rather than inside the curve section so the provider stays the
    # only thing that touches the database.
    open_interest = {
        commodity: provider.aggregate_open_interest(commodity, as_of=as_of)
        for commodity in curves
    }

    scenario_results = tuple(
        scenarios_mod.run_panel(proposal, scenarios_mod.default_panel_for(proposal))
        for proposal in proposals
    )

    tickets = tuple(
        build_ticket(proposal, generated_at=generated_at, scenarios=results)
        for proposal, results in zip(proposals, scenario_results, strict=False)
    )

    # The valuation-derived alerts name a limit key, its maximum and the
    # observed tonnage — that *is* the book, in one sentence, so the public
    # edition is not built with it rather than being built and then filtered.
    page_alerts = alerts_mod.build_alerts(
        as_of=as_of,
        proposals=tuple(proposals),
        curves=tuple(curves.values()),
        valuation=valuation if is_private else None,
    )

    report = (
        exposure_mod.build_exposure(book, valuation, as_of=as_of)
        if valuation is not None else None
    )
    ladder = _ladder(options_dir)

    view = {
        "as_of": as_of.isoformat(),
        "generated_at": generated_at.isoformat(),
        "method_version": METHOD_VERSION,
        "audience": audience,
        "is_reference_calculation": is_reference,
        "reference_label": REFERENCE_LABEL,
        "sections": [
            _alerts_section(page_alerts),
            _contracts_section(curves),
            _curve_section(curves, open_interest),
            _crush_section(provider, as_of=as_of),
            _hedge_section(proposals, crush_proposal, is_reference),
            _scenarios_section(proposals, scenario_results, is_reference),
            _ticket_section(tickets, is_reference),
            _book_section(book, valuation),
            _exposure_section(report),
            _limits_section(book, valuation),
            _clearing_section(valuation, clearing_dir=clearing_dir),
            _calendar_section(conn, as_of=as_of),
            _options_section(curves, as_of=as_of),
            _entered_options_section(ladder, curves, as_of=as_of),
            _provider_section(provider, curves, as_of=as_of),
        ],
    }
    if is_private:
        return view
    return redact_for_public(view, section_ids=PRIVATE_SECTION_IDS)


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


def _contract_leg(leg: CurveLeg) -> dict[str, Any]:
    """A curve leg, plus the third-party chart symbol the row can expand into.

    The symbol is carried on the leg rather than derived in the template for
    the usual reason: a template that can build a ticker can build a wrong
    one. Where the venue is not in ``app.tradingview``'s registry the key is
    ``None`` and the renderer draws no expander at all — see that module for
    why a guess is worse than a gap here.
    """
    payload = leg.to_dict()
    symbol = tradingview_symbol(leg.contract)
    payload["tradingview_symbol"] = symbol
    payload["tradingview_url"] = None if symbol is None else tradingview_url(symbol)
    return payload


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
            "legs": [_contract_leg(leg) for leg in analysis.legs],
        })
    if not any(row["state"] == STATE_OK for row in rows):
        return _section("contracts", state=STATE_EMPTY, reason=(
            "no forward-curve rows for the soy complex — the Layer 11 fetch has not run, or "
            "every leg it returned was dropped as being from another session"
        ))
    return _section("contracts", state=STATE_OK, data={
        "commodities": rows,
        # The words that frame the embedded chart — both lines, whole. They
        # live here, with every other label on this page, so the template
        # decides nothing about what a reader is being shown.
        "chart_stamp": TRADINGVIEW_STAMP,
        "chart_disclaimer": (
            "The prices in the table above are this project's own delayed closes, on the "
            "timestamps shown. The chart below is TradingView's, on their data and their "
            "timing — not our observation, not a settlement, and not routable. Expanding "
            "a row requests it from TradingView's servers."
        ),
    })


def _crush_section(provider: SqliteQuoteProvider, *, as_of: date) -> dict[str, Any]:
    """The board crush, on named contracts or not at all.

    The same calculation the Origins page, the Opportunity board and the daily
    briefing read — one crush, four surfaces. What it replaced was three
    provider front-month series that named no contract, which on a hedging page
    is the sharpest version of the problem: the number a trader would act on
    could not be placed anywhere.
    """
    outcome = crush_mod.named_board_crush(provider, as_of=as_of)
    if isinstance(outcome, crush_mod.CrushWithheld):
        return _section("crush", state=STATE_EMPTY, reason=outcome.reason)
    return _section("crush", state=STATE_OK, data=outcome.to_dict())


def _curve_section(
    curves: dict[str, CurveAnalysis],
    open_interest: dict[str, AggregateOpenInterest | None],
) -> dict[str, Any]:
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
            # Per contract month: still nothing, and still not inferred.
            "open_interest_available": analysis.open_interest_available,
            # Whole product, weekly, on its own date. A separate key because it
            # is a separate fact — see AggregateOpenInterest.
            "aggregate_open_interest": (
                None if (aggregate := open_interest.get(commodity)) is None
                else aggregate.to_dict()
            ),
        })
    if not rows:
        return _section("curve", state=STATE_EMPTY, reason=(
            "a term structure needs at least two legs observed on the same session; no commodity "
            "currently has that"
        ))
    from analysis.futures.curve import (
        OPEN_INTEREST_AGGREGATE_NOTE,
        OPEN_INTEREST_UNAVAILABLE,
    )

    return _section("curve", state=STATE_OK, data={
        "commodities": rows,
        "open_interest_note": OPEN_INTEREST_UNAVAILABLE,
        "aggregate_open_interest_note": OPEN_INTEREST_AGGREGATE_NOTE,
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
        "pnl_basis": clearing_mod.PnlBasis.MANAGEMENT_ESTIMATE.value,
        "pnl_basis_note": clearing_mod.PnlBasis.MANAGEMENT_ESTIMATE.description,
    })


def _exposure_section(report) -> dict[str, Any]:
    if report is None or report.is_empty:
        return _section("exposure", state=STATE_EMPTY, reason=(
            "no positions entered, so there is nothing to decompose. Exposure here is "
            "measured from the entered book, never from the board alone"
        ))
    return _section("exposure", state=STATE_OK, data=report.to_dict())


def _limits_section(book: positions_mod.Book, valuation) -> dict[str, Any]:
    if not book.limits:
        return _section("limits", state=STATE_EMPTY, reason=(
            "no limits configured. Add a `limits:` block to a positions document — a limit "
            "this software invented would be a mandate nobody agreed to"
        ))
    if valuation is None:
        return _section("limits", state=STATE_EMPTY, reason=(
            "limits are configured but there is no book to measure them against"
        ))
    checks = valuation.limit_checks
    return _section("limits", state=STATE_OK, data={
        "checks": [check.to_dict() for check in checks],
        "breaches": [check.to_dict() for check in valuation.breaches],
        "configured": len(book.limits),
        "measured": len(checks),
        "unmeasured_note": (
            "A limit whose exposure cannot be measured produces no row at all rather than a "
            "passing one — a green line nobody checked is the most dangerous output here."
            if len(checks) < len(book.limits) else ""
        ),
        "enforcement_note": (
            "Limits are reported, never enforced. This software stops nothing; it says which "
            "line was crossed, by how much, and against which exposure."
        ),
    })


def _clearing_section(valuation, *, clearing_dir: str | None) -> dict[str, Any]:
    statements = clearing_mod.load_statements(clearing_dir)
    if not statements:
        return _section("clearing", state=STATE_EMPTY, reason=(
            "no clearing statement supplied. This project ingests no account, broker or "
            "clearing feed, so the official P&L can only come from a file you export into "
            "data/reference/clearing/"
        ))
    if valuation is None:
        return _section("clearing", state=STATE_EMPTY, reason=(
            "a clearing statement is present but no position was entered, so there is "
            "nothing on our side to reconcile it against"
        ))
    latest = statements[0]
    return _section("clearing", state=STATE_OK, data={
        **clearing_mod.reconcile(valuation, latest).to_dict(),
        "statements_available": len(statements),
        "never_merged_note": (
            "The two columns are the official figure and ours. They are never averaged, "
            "netted or reconciled into one number — a single figure would belong to "
            "neither desk and would be acted on as both."
        ),
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


def _ladder(options_dir: str | None) -> options_mod.ManualLadder:
    """The desk's own quotes. A malformed document raises out of here on
    purpose — the same rule the book follows, because an option entered wrongly
    must not render as no option entered."""
    return options_mod.load_ladder(options_dir)


def _options_section(curves: dict[str, CurveAnalysis], *, as_of: date) -> dict[str, Any]:
    """Public: the chain's absence, the model, and the model's limits.

    Everything here is a fact about *this project* — that no ingested source
    publishes a chain, and that Black-76 is wrong about an American option in
    stated ways. None of it says anything about a client, which is why it is
    the one options section a public reader gets.
    """
    front = next(
        (analysis.front.contract for analysis in curves.values() if analysis.front is not None),
        None,
    )
    data = options_mod.chain_status(front)
    data.update({
        "rate": OPTION_DISCOUNT_RATE,
        "rate_note": OPTION_RATE_NOTE,
        "manual_note": (
            "A desk can enter a broker's premium or implied volatility by hand, or import an "
            "exported ladder, and get Black-76 values and Greeks from it. Those quotes are "
            "the client's own records and are rendered only to the private edition."
        ),
    })
    return _section("options", state=STATE_OK, data=data)


def _entered_options_section(
    ladder: options_mod.ManualLadder, curves: dict[str, CurveAnalysis], *, as_of: date,
) -> dict[str, Any]:
    if ladder.is_empty:
        return _section("options_entered", state=STATE_EMPTY, reason=(
            "no options entered. This is the correct state for a clone that has entered "
            "none — see data/reference/options/README.md for the file shape"
        ))
    forwards = {
        leg.contract.symbol: leg.quote.price
        for analysis in curves.values() for leg in analysis.legs
    }
    return _section("options_entered", state=STATE_OK, data={
        "entered": list(options_mod.value_manual_ladder(
            ladder, as_of=as_of, forwards=forwards, rate=OPTION_DISCOUNT_RATE,
        )),
        "entered_from": list(ladder.loaded_from),
        "rate": OPTION_DISCOUNT_RATE,
        "rate_note": OPTION_RATE_NOTE,
        "limitations": [limit.to_dict() for limit in options_mod.BLACK76_LIMITATIONS],
    })


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
                # Three states, not two. "not encoded" is a gap in this
                # project; NO_NOTICE_DAY is a fact about the contract, and a
                # hedger told the wrong one goes looking for a date that does
                # not exist (see ContractSpec.first_notice_rule).
                "first_notice_rule": spec.first_notice_rule or "not encoded",
                "first_notice_note": (
                    "this contract has no notice day — the delivery obligation "
                    "attaches at the close of the last trading day"
                    if spec.first_notice_rule == NO_NOTICE_DAY else ""
                ),
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
