"""Alerts tied to a trader's exposure, not to indicators (Phase 3).

``analysis/signals.py`` answers "is the market doing something interesting".
This module answers "is *my position* about to have a problem", which is a
different question with different inputs: it needs the book, the hedge and the
contracts, not a moving average.

Seven kinds, each tied to something a hedger has to act on:

``approaching_expiry``   a contract this book holds stops trading soon.
``roll_window``          first notice day is near — a physical hedger must be
                         out before it or risk delivery.
``hedge_slippage``       realised coverage has drifted from the intended hedge
                         ratio, usually because the physical quantity moved.
``basis_breach``         the basis has moved past a level the trader set.
``limit_breach``         a configured position or loss limit is crossed.
``curve_inversion``      the term structure inverted (or un-inverted) — the
                         carry a storage position was earning has gone.
``data_stale``           the number a hedge is marked against is older than its
                         layer's own budget. Last in the list and first in
                         importance: every other alert here is computed from
                         prices, and a stale price makes all of them lies.

Severity is ``alert`` when something must be done, ``warning`` when it must be
looked at, ``info`` when it is worth knowing. Nothing here is emitted on a
schedule or sent anywhere; the workstation renders them and the briefing can
read them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from analysis.futures.curve import CurveAnalysis
from analysis.futures.domain import Freshness
from analysis.futures.hedge import HedgeProposal
from analysis.futures.positions import BookValuation

SEVERITIES = ("alert", "warning", "info")

#: Business days before last trade at which an expiry alert fires.
EXPIRY_WARNING_BDAYS = 10
#: Calendar days before first notice day at which the roll alert fires.
ROLL_WINDOW_DAYS = 10
#: Coverage may drift this far from 100% before it counts as slippage.
COVERAGE_TOLERANCE_PCT = 2.0


@dataclass(frozen=True)
class Alert:
    kind: str
    severity: str
    subject: str          # what it is about: a symbol, a commodity, a limit key
    message: str
    detail: dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.severity not in SEVERITIES:
            raise ValueError(f"severity {self.severity!r} is not one of {SEVERITIES}")

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "subject": self.subject,
            "message": self.message,
            "detail": self.detail or {},
        }


_SEVERITY_ORDER = {"alert": 0, "warning": 1, "info": 2}


def sort_alerts(alerts: tuple[Alert, ...]) -> tuple[Alert, ...]:
    return tuple(sorted(alerts, key=lambda a: (_SEVERITY_ORDER[a.severity], a.kind, a.subject)))


# ---------------------------------------------------------------------------
# Individual checks
# ---------------------------------------------------------------------------


def expiry_alerts(proposal: HedgeProposal, *, as_of: date) -> tuple[Alert, ...]:
    """Contracts in this hedge that stop trading soon, or have no encoded expiry."""
    out: list[Alert] = []
    for leg in proposal.legs:
        contract = leg.contract
        if contract.last_trade is None:
            out.append(Alert(
                kind="approaching_expiry", severity="warning", subject=contract.symbol,
                message=(
                    f"{contract.symbol}: this product's termination rule is not encoded, so "
                    "no expiry or roll alert can fire for it — track it manually"
                ),
            ))
            continue
        days = contract.days_to_expiry(as_of)
        if days is None:
            continue
        if days <= 0:
            out.append(Alert(
                kind="approaching_expiry", severity="alert", subject=contract.symbol,
                message=(
                    f"{contract.symbol} stopped trading on {contract.last_trade.isoformat()} — "
                    "this hedge references an expired contract"
                ),
                detail={"last_trade": contract.last_trade.isoformat(), "days_to_expiry": days},
            ))
        elif days <= EXPIRY_WARNING_BDAYS:
            out.append(Alert(
                kind="approaching_expiry", severity="alert" if days <= 3 else "warning",
                subject=contract.symbol,
                message=(
                    f"{contract.symbol} has {days} session(s) to last trade "
                    f"({contract.last_trade.isoformat()})"
                ),
                detail={"last_trade": contract.last_trade.isoformat(), "days_to_expiry": days},
            ))
    return tuple(out)


def roll_alerts(proposal: HedgeProposal, *, as_of: date) -> tuple[Alert, ...]:
    """First notice day approaching on a physically-delivered contract.

    FND, not last trade, is the date a merchant's hedge has to be off: staying
    long past it invites delivery of a warehouse receipt, and staying short
    past it commits to making delivery. The alert fires on the earlier date for
    exactly that reason.
    """
    out: list[Alert] = []
    for leg in proposal.legs:
        fnd = leg.contract.first_notice
        if fnd is None:
            continue
        days = (fnd - as_of).days
        if days < 0:
            out.append(Alert(
                kind="roll_window", severity="alert", subject=leg.contract.symbol,
                message=(
                    f"{leg.contract.symbol} first notice day was {fnd.isoformat()} — a position "
                    f"still open is exposed to delivery"
                ),
                detail={"first_notice": fnd.isoformat(), "days": days},
            ))
        elif days <= ROLL_WINDOW_DAYS:
            out.append(Alert(
                kind="roll_window", severity="warning", subject=leg.contract.symbol,
                message=(
                    f"{leg.contract.symbol} first notice day is {fnd.isoformat()} ({days} days) — "
                    "roll or close before then"
                ),
                detail={"first_notice": fnd.isoformat(), "days": days},
            ))
    return tuple(out)


def slippage_alerts(proposal: HedgeProposal) -> tuple[Alert, ...]:
    """Coverage that has drifted away from a whole hedge."""
    drift = proposal.coverage_pct - 100.0
    if abs(drift) <= COVERAGE_TOLERANCE_PCT:
        return ()
    return (Alert(
        kind="hedge_slippage",
        severity="warning" if abs(drift) < 10 else "alert",
        subject=proposal.exposure.commodity,
        message=(
            f"hedge covers {proposal.coverage_pct:.1f}% of {proposal.exposure.quantity_mt:,.0f} MT — "
            f"{abs(proposal.residual_mt):,.0f} MT "
            f"{'over-hedged' if proposal.residual_mt < 0 else 'unhedged'}"
        ),
        detail={"coverage_pct": proposal.coverage_pct, "residual_mt": proposal.residual_mt},
    ),)


def basis_alerts(
    proposal: HedgeProposal,
    *,
    current_basis_usd_mt: float | None,
    floor_usd_mt: float | None = None,
    ceiling_usd_mt: float | None = None,
) -> tuple[Alert, ...]:
    """The basis has moved through a level the trader set.

    Both bounds are inputs. There is no default band: a "normal" basis range is
    a market judgement, and inferring one from our own short history would put
    a number the software invented behind an alert the trader acts on.
    """
    if current_basis_usd_mt is None:
        return ()
    out: list[Alert] = []
    exposure = proposal.exposure
    per_mt = exposure.quantity_mt
    if floor_usd_mt is not None and current_basis_usd_mt < floor_usd_mt:
        out.append(Alert(
            kind="basis_breach", severity="alert", subject=exposure.commodity,
            message=(
                f"basis {current_basis_usd_mt:+.2f} USD/MT is below the {floor_usd_mt:+.2f} floor — "
                f"{(floor_usd_mt - current_basis_usd_mt) * per_mt:,.0f} USD against the position"
            ),
            detail={"basis": current_basis_usd_mt, "floor": floor_usd_mt},
        ))
    if ceiling_usd_mt is not None and current_basis_usd_mt > ceiling_usd_mt:
        out.append(Alert(
            kind="basis_breach", severity="warning", subject=exposure.commodity,
            message=(
                f"basis {current_basis_usd_mt:+.2f} USD/MT is above the {ceiling_usd_mt:+.2f} "
                "ceiling"
            ),
            detail={"basis": current_basis_usd_mt, "ceiling": ceiling_usd_mt},
        ))
    return tuple(out)


def limit_alerts(valuation: BookValuation) -> tuple[Alert, ...]:
    return tuple(
        Alert(
            kind="limit_breach", severity="alert", subject=f"{breach.limit.key}:{breach.limit.scope}",
            message=(
                f"{breach.limit.key} limit for {breach.limit.scope} is {breach.limit.maximum:,.0f}; "
                f"observed {breach.observed:,.0f}"
            ),
            detail=breach.to_dict(),
        )
        for breach in valuation.breaches
    )


def curve_alerts(analysis: CurveAnalysis) -> tuple[Alert, ...]:
    """Inversion, and the coherence failure that would make it a false reading."""
    out: list[Alert] = []
    if not analysis.coherent:
        out.append(Alert(
            kind="data_stale", severity="alert", subject=analysis.commodity,
            message=(
                f"{analysis.commodity} curve is not a single session — every spread and every "
                f"structure reading on it is unreliable: {analysis.coherence_note}"
            ),
        ))
        return tuple(out)
    if analysis.is_inverted:
        front = analysis.front
        out.append(Alert(
            kind="curve_inversion", severity="warning", subject=analysis.commodity,
            message=(
                f"{analysis.commodity} is in {analysis.structure.value} — "
                f"{analysis.structure.implication}. A stored position is no longer being paid to "
                "carry"
            ),
            detail={
                "structure": analysis.structure.value,
                "front": front.contract.symbol if front else None,
                "slope_usd_mt_per_month": analysis.slope_per_month_usd_mt,
            },
        ))
    return tuple(out)


def staleness_alerts(analysis: CurveAnalysis) -> tuple[Alert, ...]:
    if analysis.freshness == Freshness.STALE.value:
        return (Alert(
            kind="data_stale", severity="alert", subject=analysis.commodity,
            message=(
                f"{analysis.commodity} curve was last observed "
                f"{analysis.observation_date.isoformat() if analysis.observation_date else 'never'}"
                f" ({analysis.age_days} days) — past this layer's recency budget. Marks, hedge "
                "sizing and P&L on this commodity are all struck on a stale price"
            ),
            detail={"age_days": analysis.age_days, "freshness": analysis.freshness},
        ),)
    if analysis.freshness == Freshness.BEHIND.value:
        return (Alert(
            kind="data_stale", severity="warning", subject=analysis.commodity,
            message=(
                f"{analysis.commodity} curve is {analysis.age_days} days old — inside the layer's "
                "budget but behind a daily cadence"
            ),
            detail={"age_days": analysis.age_days, "freshness": analysis.freshness},
        ),)
    return ()


def position_staleness_alerts(valuation: BookValuation) -> tuple[Alert, ...]:
    out: list[Alert] = []
    for position in valuation.positions:
        if position.mark is None:
            out.append(Alert(
                kind="data_stale", severity="alert", subject=position.key,
                message=f"{position.key} could not be marked: {'; '.join(position.warnings)}",
            ))
    return tuple(out)


# ---------------------------------------------------------------------------
# Assembly
# ---------------------------------------------------------------------------


def build_alerts(
    *,
    as_of: date,
    proposals: tuple[HedgeProposal, ...] = (),
    curves: tuple[CurveAnalysis, ...] = (),
    valuation: BookValuation | None = None,
    basis_bounds: dict[str, tuple[float | None, float | None]] | None = None,
) -> tuple[Alert, ...]:
    """Every exposure alert this workstation can raise, most severe first."""
    alerts: list[Alert] = []
    bounds = basis_bounds or {}

    for proposal in proposals:
        alerts.extend(expiry_alerts(proposal, as_of=as_of))
        alerts.extend(roll_alerts(proposal, as_of=as_of))
        alerts.extend(slippage_alerts(proposal))
        floor, ceiling = bounds.get(proposal.exposure.commodity, (None, None))
        alerts.extend(basis_alerts(
            proposal,
            current_basis_usd_mt=proposal.exposure.basis_usd_per_mt,
            floor_usd_mt=floor,
            ceiling_usd_mt=ceiling,
        ))

    for analysis in curves:
        alerts.extend(curve_alerts(analysis))
        alerts.extend(staleness_alerts(analysis))

    if valuation is not None:
        alerts.extend(limit_alerts(valuation))
        alerts.extend(position_staleness_alerts(valuation))

    # A commodity can raise the same staleness alert from two directions (its
    # curve, and a position marked against it). One line per fact.
    seen: set[tuple[str, str, str]] = set()
    unique: list[Alert] = []
    for alert in alerts:
        key = (alert.kind, alert.subject, alert.message)
        if key in seen:
            continue
        seen.add(key)
        unique.append(alert)

    return sort_alerts(tuple(unique))


__all__ = [
    "COVERAGE_TOLERANCE_PCT",
    "EXPIRY_WARNING_BDAYS",
    "ROLL_WINDOW_DAYS",
    "SEVERITIES",
    "Alert",
    "basis_alerts",
    "build_alerts",
    "curve_alerts",
    "expiry_alerts",
    "limit_alerts",
    "position_staleness_alerts",
    "roll_alerts",
    "slippage_alerts",
    "sort_alerts",
    "staleness_alerts",
]
