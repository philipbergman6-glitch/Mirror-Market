"""Private, point-in-time soybean offer comparison and named hedge sizing."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from math import isfinite
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import config
from analysis.futures.domain import ContractQuote, NamedContract, Side, parse_symbol
from analysis.futures.hedge import size_leg
from analysis.futures.providers import ProviderError
from analysis.origins.assumptions import Assumption, AssumptionSet
from analysis.origins.domain import (
    Carrier,
    ContractRef,
    CostComponent,
    Grade,
    Incoterm,
    OriginQuote,
    OriginRanking,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    usd_mt,
)
from analysis.origins.landed_cost import compute_landed_cost
from analysis.origins.scenarios import freight_breakeven
from analysis.origins.sources import port_for

CARGO_WORKSPACE = Path(__file__).resolve().parents[2] / "data" / "workspace" / "cargo"

QuoteLookup = Callable[[NamedContract, date], ContractQuote | None]


def _instant(value: str) -> datetime:
    result = datetime.fromisoformat(value)
    if result.utcoffset() is None:
        raise ValueError("timestamps must include a timezone offset")
    return result


def _number(value: float, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not isfinite(value):
        raise ValueError(f"{field} must be a finite number")
    return float(value)


@dataclass(frozen=True)
class Offer:
    id: str
    origin: str
    price_usd_mt: float
    incoterm: str
    carrier: str
    grade: str
    observed_at: str
    expires_at: str
    loading_start: str
    loading_end: str
    available_mt: float
    unpriced_mt: float
    source: str
    contract: str | None = None
    basis_usd_mt: float | None = None
    pricing_end: str | None = None
    duty_condition_confirmed: bool = False

    def quote(self) -> OriginQuote:
        if not all(isinstance(x, str) and x.strip() for x in (self.id, self.grade, self.source)):
            raise ValueError("offer id, grade and source must be nonempty text")
        price = _number(self.price_usd_mt, "price_usd_mt")
        if price <= 0 or _instant(self.expires_at) <= _instant(self.observed_at):
            raise ValueError("offer requires a positive price and expiry after observation")
        if self.basis_usd_mt is not None:
            _number(self.basis_usd_mt, "basis_usd_mt")
        board_contract = None
        if self.contract:
            contract = parse_symbol(self.contract)
            if contract.spec.root != "ZS":
                raise ValueError("soybean offers require a named CBOT soybean contract")
            board_contract = ContractRef("CBOT", contract.symbol, f"{contract.year:04d}-{contract.month:02d}")
        return OriginQuote(
            origin=port_for(self.origin),
            grade=Grade(specification=self.grade),
            quote_kind=QuoteKind.PHYSICAL,
            incoterm=Incoterm(self.incoterm),
            carrier=Carrier(self.carrier),
            price=usd_mt(price),
            native_price=price,
            native_currency="USD",
            native_unit="usd_per_mt",
            observation_date=_instant(self.observed_at).date(),
            publication_date=None,
            source=SourceRef("private_offer", "cargo", self.source),
            shipment_window=ShipmentWindow(
                date.fromisoformat(self.loading_start), date.fromisoformat(self.loading_end)
            ),
            board_contract=board_contract,
        )


def _hedge(offer: Offer, decision_at: datetime, lookup: QuoteLookup) -> dict[str, Any]:
    if offer.unpriced_mt == 0:
        return {"state": "no_exposure", "reason": "selected offer is fully priced"}
    if not offer.contract or not offer.pricing_end:
        return {"state": "blocked", "reason": "named contract and physical pricing end are required"}
    contract = parse_symbol(offer.contract)
    as_of = decision_at.astimezone(ZoneInfo(config.SETTLEMENT_TIMEZONE)).date()
    pricing_end = date.fromisoformat(offer.pricing_end)
    deadlines = [d for d in (contract.first_notice, contract.last_trade) if d is not None]
    if pricing_end < as_of or not deadlines or pricing_end >= min(deadlines):
        return {"state": "blocked", "reason": "physical pricing end must precede notice and expiry"}

    # Stored quotes have a session date only. A same-day close cannot be assumed
    # available at an arbitrary desk cutoff, so use a prior completed session.
    quote_cutoff = as_of - timedelta(days=1)
    try:
        quote = lookup(contract, quote_cutoff)
        if quote is None or quote.contract != contract or not 0 <= (quote_cutoff - quote.observation_date).days <= 3:
            return {"state": "blocked", "reason": "no recent prior close for the exact named contract"}
        leg = size_leg(quote, side=Side.LONG, physical_mt=offer.unpriced_mt)
    except (ProviderError, ValueError) as exc:
        return {"state": "blocked", "reason": str(exc)}
    return {
        "state": "ok",
        **leg.to_dict(),
        "residual_mt": round(offer.unpriced_mt - leg.covered_physical_mt, 3),
    }


def evaluate_case(
    case: dict[str, Any],
    assumptions: AssumptionSet,
    lookup: QuoteLookup,
) -> dict[str, Any]:
    """Evaluate complete alternatives before handing the selected offer to sizing."""
    return _evaluate(assumptions, lookup, **case)


def _evaluate(
    assumptions: AssumptionSet,
    lookup: QuoteLookup,
    *,
    decision_at: str,
    destination: str,
    quantity_mt: float,
    loading_start: str,
    loading_end: str,
    offers: list[dict[str, Any]],
    selected_offer: str | None = None,
) -> dict[str, Any]:
    at = _instant(decision_at)
    quantity = _number(quantity_mt, "quantity_mt")
    if quantity <= 0 or len(offers) < 2:
        raise ValueError("a cargo case requires positive tonnage and at least two offers")
    destination_port = port_for(destination, role="destination")
    window = ShipmentWindow(date.fromisoformat(loading_start), date.fromisoformat(loading_end))
    if window.end < at.date():
        raise ValueError("requested loading window has ended")
    entries = [Offer(**entry) for entry in offers]
    by_id = {offer.id: offer for offer in entries}
    if len(by_id) != len(entries) or (selected_offer is not None and selected_offer not in by_id):
        raise ValueError("offer ids must be unique and the selected id must be present")

    costs = {}
    rendered = []
    for offer in entries:
        quote = offer.quote()
        unpriced = _number(offer.unpriced_mt, "unpriced_mt")
        if not 0 <= unpriced <= quantity:
            raise ValueError("unpriced tonnage must lie between zero and the case tonnage")
        if offer.pricing_end is not None:
            date.fromisoformat(offer.pricing_end)
        row = {
            "id": offer.id, "expires_at": offer.expires_at, "unpriced_mt": offer.unpriced_mt,
            "contract": quote.board_contract.code if quote.board_contract else None,
        }
        reason = None
        if not _instant(offer.observed_at) <= at < _instant(offer.expires_at):
            reason = "offer was unavailable at decision time"
        elif _number(offer.available_mt, "available_mt") < quantity:
            reason = "offer does not cover the requested tonnage"
        elif quote.shipment_window is None or (
            quote.shipment_window.start > window.start or quote.shipment_window.end < window.end
        ):
            reason = "offer does not cover the full requested loading window"
        if type(offer.duty_condition_confirmed) is not bool:
            raise ValueError("duty_condition_confirmed must be a boolean")
        duty = assumptions.lookup(
            CostComponent.IMPORT_DUTY, origin=quote.origin.key, destination=destination,
            window=window, on=at.date(),
        )
        if reason is None and isinstance(duty, Assumption) and duty.caveat and not offer.duty_condition_confirmed:
            reason = f"confirm the applicable duty condition: {duty.caveat}"
        if reason is not None:
            rendered.append({**row, "state": "blocked", "reason": reason})
            continue
        cost = compute_landed_cost(quote, destination_port, window, assumptions, today=at.date())
        rendered.append({**row, "state": "ok" if cost.is_rankable else "blocked", **cost.to_dict()})
        if cost.is_rankable:
            costs[offer.id] = cost

    ordered = sorted(costs, key=lambda id_: costs[id_].landed_usd_mt or 0.0)
    complete = len(ordered) == len(entries)
    rank = OriginRanking(
        destination_port, window, at.date(), tuple(costs[id_] for id_ in ordered),
        config.LANDED_COST_METHOD_VERSION, assumptions.set_id,
    )
    gap = rank.advantage_usd_mt if complete else None
    winner = ordered[0] if gap is not None and gap > 0 else None
    picked = selected_offer if selected_offer is not None else winner
    break_even = freight_breakeven(rank) if winner else None
    sensitivity = None
    if break_even is not None:
        freight = next(step.amount.amount for step in costs[ordered[0]].steps
                       if step.component is CostComponent.OCEAN_FREIGHT)
        sensitivity = {
            **break_even.to_dict(),
            "offer": ordered[0],
            "challenger_offer": ordered[1],
            "current_freight_usd_mt": freight,
            "break_even_freight_usd_mt": freight + break_even.move_usd_mt,
        }
    hedge = (
        _hedge(by_id[picked], at, lookup)
        if picked is not None and picked in costs
        else {"state": "blocked", "reason": "no comparable selected offer"}
    )
    return {
        "decision_at": decision_at,
        "destination": destination,
        "product": "soybeans",
        "quantity_mt": quantity,
        "offers": rendered,
        "comparison_state": "complete" if complete else "blocked",
        "winner": winner,
        "selected_offer": picked,
        "advantage_usd_mt": gap,
        "advantage_usd": gap * quantity if gap is not None else None,
        "freight_breakeven": sensitivity,
        "assumption_set_id": assumptions.set_id,
        "hedge": hedge,
    }
