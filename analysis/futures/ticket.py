"""Proposed trade ticket export (Phase 3).

The artifact a trader takes away from the workstation: a written statement of
the hedge that was modelled, complete enough that a risk manager or a broker
can read it without the screen it came from.

It is a **proposal**. There is no routing in this project, no order type, no
venue connectivity and no intention to add any. :data:`NOT_ROUTED_BANNER` is
rendered first in every format this module emits, and the tests assert its
presence — a ticket that could be mistaken for an order is the one failure mode
here that would matter outside the software.

What a ticket must carry, and why each one is not optional:

* **Instrument, side, month** — the trade. A ticket naming "soybeans" and not
  ZSX26 cannot be acted on and cannot be checked.
* **Quantity** in contracts *and* tonnes — the two audiences. The broker needs
  contracts; the physical desk reconciles tonnes.
* **Reference price with its type and its observation date** — the price this
  was sized at, labelled as the delayed close it is, dated to the session it
  belongs to. Not a live price, and it must not read as one.
* **Assumptions** — every input the sizing depended on, including the ones the
  user supplied by hand.
* **Expected hedge coverage** and the residual it leaves.
* **Warnings** — carried verbatim from the proposal, at their own severity.
* **A reproducible id** — the same inputs produce the same id, so two copies of
  a ticket can be compared without reading them.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from analysis.futures.domain import METHOD_VERSION, fingerprint
from analysis.futures.hedge import HedgeProposal
from analysis.futures.scenarios import ScenarioResult

NOT_ROUTED_BANNER = "PROPOSAL — NOT ROUTED"

DISCLAIMER = (
    "This ticket is a proposal produced from delayed reference data. It has not been sent to "
    "any venue, broker or execution system, and this software has no capability to do so. "
    "Prices are delayed daily bars, not proven exchange settlements; verify every level and "
    "every contract month against your executing broker before acting."
)


@dataclass(frozen=True)
class TicketLine:
    """One instrument on the ticket."""

    instrument: str          # exchange symbol, e.g. ZSX26
    description: str         # e.g. "CBOT Soybeans Nov 2026"
    side: str                # buy | sell
    contracts: int
    tonnes: float
    reference_price: float
    reference_price_unit: str
    reference_price_label: str
    reference_price_usd_mt: float
    observation_date: str
    last_trade: str | None
    first_notice: str | None
    notional_usd: float
    cross_hedge_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "instrument": self.instrument,
            "description": self.description,
            "side": self.side,
            "contracts": self.contracts,
            "tonnes": round(self.tonnes, 3),
            "reference_price": self.reference_price,
            "reference_price_unit": self.reference_price_unit,
            "reference_price_label": self.reference_price_label,
            "reference_price_usd_mt": round(self.reference_price_usd_mt, 2),
            "observation_date": self.observation_date,
            "last_trade": self.last_trade,
            "first_notice": self.first_notice,
            "notional_usd": round(self.notional_usd, 2),
            "cross_hedge_note": self.cross_hedge_note,
        }


@dataclass(frozen=True)
class TradeTicket:
    """A complete proposed hedge, ready to be read by someone else."""

    proposal_id: str
    generated_at: datetime
    lines: tuple[TicketLine, ...]
    exposure: dict[str, Any]
    coverage_pct: float
    residual_mt: float
    assumptions: tuple[str, ...]
    warnings: tuple[dict[str, str], ...]
    scenarios: tuple[dict[str, Any], ...] = ()
    method_version: str = METHOD_VERSION
    status: str = NOT_ROUTED_BANNER

    @property
    def ticket_id(self) -> str:
        """Reproducible from the proposal and the lines, not from the clock."""
        return fingerprint({
            "proposal": self.proposal_id,
            "lines": [line.to_dict() for line in self.lines],
            "method": self.method_version,
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "disclaimer": DISCLAIMER,
            "ticket_id": self.ticket_id,
            "proposal_id": self.proposal_id,
            "generated_at": self.generated_at.isoformat(),
            "method_version": self.method_version,
            "exposure": self.exposure,
            "lines": [line.to_dict() for line in self.lines],
            "expected_coverage_pct": round(self.coverage_pct, 2),
            "residual_mt": round(self.residual_mt, 3),
            "assumptions": list(self.assumptions),
            "warnings": [dict(w) for w in self.warnings],
            "scenarios": list(self.scenarios),
        }

    def to_json(self, *, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, sort_keys=False)

    def to_text(self) -> str:
        return render_text(self)


def _side_word(side: str) -> str:
    return "BUY" if side == "long" else "SELL"


def build_ticket(
    proposal: HedgeProposal,
    *,
    generated_at: datetime,
    scenarios: tuple[ScenarioResult, ...] = (),
    extra_assumptions: tuple[str, ...] = (),
) -> TradeTicket:
    """Turn a sized hedge into a ticket.

    ``generated_at`` is passed in rather than read from the clock so the
    function stays pure and testable; the ticket id deliberately excludes it,
    because two tickets for the same trade are the same trade.
    """
    lines = tuple(
        TicketLine(
            instrument=leg.contract.symbol,
            description=f"{leg.contract.spec.display} {leg.contract.label}",
            side=_side_word(leg.side.value),
            contracts=leg.contracts,
            tonnes=leg.futures_mt,
            reference_price=leg.quote.price,
            reference_price_unit=leg.contract.spec.native_unit.value,
            reference_price_label=leg.quote.price_label,
            reference_price_usd_mt=leg.quote.usd_per_mt,
            observation_date=leg.quote.observation_date.isoformat(),
            last_trade=leg.contract.last_trade.isoformat() if leg.contract.last_trade else None,
            first_notice=leg.contract.first_notice.isoformat() if leg.contract.first_notice else None,
            notional_usd=leg.notional_usd,
            cross_hedge_note=leg.cross_hedge_note,
        )
        for leg in proposal.legs
    )

    assumptions = [
        f"method version {proposal.method_version}",
        f"sized as of {proposal.as_of.isoformat()}",
        f"contract counts rounded {proposal.rounding.value}",
        f"basis convention: {proposal.exposure.basis_convention.value}",
    ]
    if proposal.exposure.basis_usd_per_mt is not None:
        assumptions.append(
            f"basis taken as {proposal.exposure.basis_usd_per_mt:+.2f} USD/MT"
            + (f" ({proposal.exposure.basis_source})" if proposal.exposure.basis_source else "")
        )
    else:
        assumptions.append("no basis level supplied — basis risk is stated as sensitivity only")
    for leg in proposal.legs:
        assumptions.append(
            f"{leg.contract.symbol}: hedge ratio {leg.hedge_ratio:.4f} ({leg.hedge_ratio_source}); "
            f"contract size {leg.contract.spec.contract_size:,.0f} "
            f"{leg.contract.spec.size_unit.value} = {leg.contract.spec.mt_per_contract:.4f} MT"
        )
        assumptions.append(
            f"{leg.contract.symbol}: reference price is a {leg.quote.price_label} from "
            f"{leg.quote.provider.display} for session {leg.quote.observation_date.isoformat()}"
        )
    if proposal.fx.pair:
        assumptions.append(
            f"FX {proposal.fx.pair} at "
            + (f"{proposal.fx.rate:.6f} ({proposal.fx.rate_date})" if proposal.fx.rate else "no rate available")
        )
    assumptions.extend(extra_assumptions)

    return TradeTicket(
        proposal_id=proposal.identifier,
        generated_at=generated_at,
        lines=lines,
        exposure=proposal.exposure.to_dict(),
        coverage_pct=proposal.coverage_pct,
        residual_mt=proposal.residual_mt,
        assumptions=tuple(assumptions),
        warnings=tuple(w.to_dict() for w in proposal.warnings),
        scenarios=tuple(result.to_dict() for result in scenarios),
    )


def render_text(ticket: TradeTicket) -> str:
    """Plain-text ticket — the format that survives being pasted anywhere."""
    exposure = ticket.exposure
    width = 78
    out: list[str] = [
        "=" * width,
        f"  {ticket.status}",
        "=" * width,
        f"  ticket {ticket.ticket_id}   proposal {ticket.proposal_id}",
        f"  generated {ticket.generated_at.isoformat()}   method {ticket.method_version}",
        "",
        "PHYSICAL EXPOSURE",
        f"  {exposure['side'].upper()} {exposure['quantity']:,.0f} {exposure['unit']} "
        f"{exposure['commodity']}  ({exposure['quantity_mt']:,.1f} MT)",
        f"  pricing {exposure['pricing_start']} to {exposure['pricing_end']}"
        f"   basis {exposure['basis_convention']}"
        + (f"   {exposure['basis_usd_per_mt']:+.2f} USD/MT" if exposure["basis_usd_per_mt"] is not None else ""),
        f"  invoiced in {exposure['currency']}"
        + (f" (FX {exposure['fx_pair']})" if exposure["fx_pair"] else ""),
        "",
        "PROPOSED FUTURES LINES",
    ]
    if not ticket.lines:
        out.append("  (none — no contract month could be selected; see warnings)")
    for line in ticket.lines:
        out.append(
            f"  {line.side:<4} {line.contracts:>5} x {line.instrument:<8} "
            f"{line.description:<28} @ {line.reference_price:>10,.2f} "
            f"({line.reference_price_label}, {line.observation_date})"
        )
        out.append(
            f"       {line.tonnes:>10,.1f} MT   {line.reference_price_usd_mt:>8,.2f} USD/MT   "
            f"notional {line.notional_usd:>14,.2f} USD"
        )
        out.append(
            f"       last trade {line.last_trade or 'not encoded'}   "
            f"first notice {line.first_notice or 'not encoded'}"
        )
        if line.cross_hedge_note:
            out.append(f"       {line.cross_hedge_note}")
    out += [
        "",
        f"EXPECTED HEDGE COVERAGE   {ticket.coverage_pct:.2f}%"
        f"   residual {ticket.residual_mt:+,.1f} MT"
        f" ({'over-hedged' if ticket.residual_mt < 0 else 'unhedged'})",
        "",
        "ASSUMPTIONS",
    ]
    out += [f"  - {item}" for item in ticket.assumptions]
    if ticket.warnings:
        out += ["", "WARNINGS"]
        out += [f"  [{w['severity'].upper()}] {w['message']}" for w in ticket.warnings]
    if ticket.scenarios:
        out += ["", "SCENARIOS"]
        for result in ticket.scenarios:
            out.append(
                f"  {result['scenario']['name']:<32} net {result['net_pnl_usd']:>14,.0f} USD"
                f"   (physical {result['physical_pnl_usd']:>+,.0f}, "
                f"futures {result['futures_pnl_usd']:>+,.0f}, fx {result['fx_pnl_usd']:>+,.0f})"
            )
    out += ["", "-" * width, DISCLAIMER, "=" * width]
    return "\n".join(out)


__all__ = [
    "DISCLAIMER",
    "NOT_ROUTED_BANNER",
    "TicketLine",
    "TradeTicket",
    "build_ticket",
    "render_text",
]
