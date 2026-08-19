"""Physical-to-futures hedge sizing (Phase 3).

The one question this module answers: *I am long (or short) N tonnes of a
physical commodity, priced over a named window, on a named basis convention —
how many futures contracts of which month does that call for, and what is left
unhedged when I have done it?*

It is arithmetic, not advice. Everything it returns is reproducible from the
inputs it records, and every input it could not source is a named
:class:`HedgeWarning` rather than a default.

Four things it insists on.

**A hedge is placed on a named contract.** :class:`HedgeRequest` takes a
:class:`~analysis.futures.domain.NamedContract`, or resolves one from the curve
under a stated rule. It will not accept a continuous series, and there is no
code path in which "Soybeans" alone sizes a hedge.

**Contracts are whole.** Exchanges do not sell 73.487 contracts. The rounding
policy is explicit (:class:`Rounding`) and the tonnage the rounding leaves
uncovered is reported as residual exposure *with its sign* — over-hedged is a
different risk from under-hedged, and the sign is how a trader tells them
apart.

**Residual is not the only exposure left.** A ZS hedge against a physical
soybean cargo leaves basis risk by construction, and a cargo invoiced in BRL
leaves FX risk a CBOT hedge does not touch. Both are quantified beside the
futures leg, because a hedge report showing only the futures leg reads as
though the position were flat.

**A cross hedge says it is one.** Hedging meal and oil against a bean crush, or
any exposure against a contract that is not the same commodity, is a *cross*
hedge: the hedge ratio is not 1, and it is an input with a stated source, never
a number this module infers from a correlation it did not compute.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Any

from analysis.futures.curve import CurveAnalysis, hedge_month_candidates
from analysis.futures.domain import (
    BUSHELS_PER_MT_56LB,
    BUSHELS_PER_MT_60LB,
    METHOD_VERSION,
    MT_PER_SHORT_TON,
    POUNDS_PER_MT,
    ContractQuote,
    NamedContract,
    Side,
    fingerprint,
    spec_for,
)

# Crush yields, in metric-native form. Imported from analysis.spreads rather
# than restated so the hedge calculator and the board crush cannot disagree
# about how much oil a tonne of beans makes.
from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT

# The two refusals every sizing call makes, held centrally so a second sizing
# path cannot be written without them: a stitched series is not an instrument,
# and a price nobody traded at is not a market to hedge in.
from pricing.policy import require_hedgeable, require_traded_price


class Rounding(str, Enum):
    """How a fractional contract count becomes a whole one.

    ``NEAREST`` is the default because it minimises residual in absolute terms.
    ``DOWN`` never over-hedges (the conservative choice for a merchant who must
    not create a speculative position); ``UP`` never leaves the physical
    partially uncovered. The choice changes the sign of the residual, which is
    the whole point of making it explicit.
    """

    NEAREST = "nearest"
    DOWN = "down"
    UP = "up"


class BasisConvention(str, Enum):
    """How the physical is priced against the board.

    This is not decoration: it decides whether a futures hedge removes price
    risk at all. A flat-priced cargo has *no* futures exposure left to hedge —
    the price is already fixed — and hedging one is creating a position, not
    removing one. The calculator says so rather than sizing it silently.
    """

    #: Price fixed in the contract. Futures no longer offset anything.
    FLAT_PRICE = "flat_price"
    #: Priced as a differential to a named futures month — the classic case.
    BASIS_OVER_FUTURES = "basis_over_futures"
    #: Priced off a published index or formula (CEPEA, AMS, MAGyP FOB).
    #: Futures hedge the correlated part only; the rest is basis risk.
    FORMULA_PRICED = "formula_priced"
    #: Not yet priced — the exposure is to the flat price in full.
    UNPRICED = "unpriced"


class PhysicalUnit(str, Enum):
    METRIC_TON = "mt"
    SHORT_TON = "short_ton"
    BUSHEL = "bushel"
    POUND = "lb"


@dataclass(frozen=True)
class HedgeWarning:
    """Something the trader must read before acting on the numbers."""

    code: str
    message: str
    severity: str = "warning"   # info | warning | alert

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code, "message": self.message, "severity": self.severity}


def to_metric_tons(quantity: float, unit: PhysicalUnit, commodity: str) -> float:
    """Convert a physical quantity to metric tons.

    Bushel weight is a property of the crop, not a constant, so the commodity
    is required — a corn bushel is 56 lb and a soybean bushel is 60 lb, and
    using one factor for both is a 7% error that looks like nothing.
    """
    if unit is PhysicalUnit.METRIC_TON:
        return float(quantity)
    if unit is PhysicalUnit.SHORT_TON:
        return float(quantity) * MT_PER_SHORT_TON
    if unit is PhysicalUnit.POUND:
        return float(quantity) / POUNDS_PER_MT
    if unit is PhysicalUnit.BUSHEL:
        spec = spec_for(commodity)
        per_mt = BUSHELS_PER_MT_56LB if spec.root == "ZC" else BUSHELS_PER_MT_60LB
        return float(quantity) / per_mt
    raise ValueError(f"unhandled physical unit {unit}")


@dataclass(frozen=True)
class PhysicalExposure:
    """What the trader actually owns or owes.

    ``side`` is the *physical* side. Long means owning the goods or having
    bought them forward unpriced — a price fall hurts. Short means having sold
    forward without owning — a price rise hurts. The futures leg is always the
    opposite side, which is asserted rather than chosen.
    """

    commodity: str                       # e.g. "Soybeans" — the physical good
    side: Side
    quantity: float
    unit: PhysicalUnit
    pricing_start: date
    pricing_end: date
    basis_convention: BasisConvention
    #: What the physical is worth over (or under) the board, USD/MT. None means
    #: unknown, which is itself reported — a basis you cannot state is a basis
    #: you cannot monitor.
    basis_usd_per_mt: float | None = None
    basis_source: str = ""
    #: Invoice currency and the FX pair that converts it, in the stack's
    #: ``<CCY>/USD`` convention (USD per unit of home currency).
    currency: str = "USD"
    fx_pair: str | None = None
    counterparty: str = ""
    note: str = ""

    def __post_init__(self) -> None:
        if self.quantity <= 0:
            raise ValueError("physical quantity must be positive; direction is carried by `side`")
        if self.pricing_end < self.pricing_start:
            raise ValueError("pricing_end precedes pricing_start")
        if self.currency != "USD" and not self.fx_pair:
            raise ValueError(
                f"exposure is invoiced in {self.currency} but names no FX pair — the USD "
                "figures would silently be the local ones relabelled"
            )

    @property
    def quantity_mt(self) -> float:
        return to_metric_tons(self.quantity, self.unit, self.commodity)

    @property
    def futures_side(self) -> Side:
        """A long physical is hedged short, and vice versa. Not a choice."""
        return self.side.opposite

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "commodity": self.commodity,
            "side": self.side.value,
            "quantity": self.quantity,
            "unit": self.unit.value,
            "quantity_mt": round(self.quantity_mt, 4),
            "pricing_start": self.pricing_start.isoformat(),
            "pricing_end": self.pricing_end.isoformat(),
            "basis_convention": self.basis_convention.value,
            "basis_usd_per_mt": self.basis_usd_per_mt,
            "basis_source": self.basis_source,
            "currency": self.currency,
            "fx_pair": self.fx_pair,
            "note": self.note,
            "futures_side": self.futures_side.value,
        }
        # Emitted only when there is one. A counterparty names a client's trade
        # partner, so the public leak guard refuses the key outright; a blank
        # string carries no such information and an absent field is the honest
        # rendering of "no counterparty was entered" anyway.
        if self.counterparty:
            payload["counterparty"] = self.counterparty
        return payload


@dataclass(frozen=True)
class HedgeLeg:
    """One futures leg of a hedge proposal."""

    quote: ContractQuote
    side: Side
    contracts: int
    hedge_ratio: float
    hedge_ratio_source: str
    #: Physical tonnage this leg is meant to cover, before rounding.
    target_physical_mt: float
    #: Futures tonnage the whole contracts actually represent.
    futures_mt: float
    #: Physical tonnage that futures tonnage covers, given the ratio.
    covered_physical_mt: float
    is_cross_hedge: bool
    cross_hedge_note: str = ""

    @property
    def contract(self) -> NamedContract:
        return self.quote.contract

    @property
    def notional_usd(self) -> float:
        return self.contracts * self.quote.contract_value_usd

    @property
    def price_per_mt_usd(self) -> float:
        return self.quote.usd_per_mt

    def value_change_usd(self, new_price_native: float) -> float:
        """P&L on this leg if the contract settles at ``new_price_native``.

        Signed by the leg's own side: a short leg gains when the price falls.
        """
        spec = self.contract.spec
        per_contract = spec.value_usd(new_price_native) - spec.value_usd(self.quote.price)
        return self.side.sign * self.contracts * per_contract

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract": self.contract.to_dict(),
            "symbol": self.contract.symbol,
            "label": self.contract.label,
            "side": self.side.value,
            "contracts": self.contracts,
            "hedge_ratio": self.hedge_ratio,
            "hedge_ratio_source": self.hedge_ratio_source,
            "reference_price": self.quote.price,
            "reference_price_label": self.quote.price_label,
            "reference_price_usd_mt": round(self.price_per_mt_usd, 2),
            "observation_date": self.quote.observation_date.isoformat(),
            "target_physical_mt": round(self.target_physical_mt, 3),
            "futures_mt": round(self.futures_mt, 3),
            "covered_physical_mt": round(self.covered_physical_mt, 3),
            "notional_usd": round(self.notional_usd, 2),
            "mt_per_contract": round(self.contract.spec.mt_per_contract, 4),
            "is_cross_hedge": self.is_cross_hedge,
            "cross_hedge_note": self.cross_hedge_note,
            "days_to_expiry": self.quote.days_to_expiry(),
            "last_trade": self.contract.last_trade.isoformat() if self.contract.last_trade else None,
            "first_notice": self.contract.first_notice.isoformat() if self.contract.first_notice else None,
        }


@dataclass(frozen=True)
class FxExposure:
    """The part of the position a USD futures hedge does not touch."""

    pair: str | None
    rate: float | None
    rate_date: date | None
    #: Value exposed to the rate, in the invoice currency.
    amount_home: float | None
    amount_usd: float | None
    hedged: bool = False
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "rate": self.rate,
            "rate_date": self.rate_date.isoformat() if self.rate_date else None,
            "amount_home": None if self.amount_home is None else round(self.amount_home, 2),
            "amount_usd": None if self.amount_usd is None else round(self.amount_usd, 2),
            "hedged": self.hedged,
            "note": self.note,
        }


@dataclass(frozen=True)
class HedgeProposal:
    """A sized hedge, every input recorded, nothing routed."""

    exposure: PhysicalExposure
    legs: tuple[HedgeLeg, ...]
    as_of: date
    residual_mt: float
    residual_pct: float
    coverage_pct: float
    basis_risk_usd_per_mt_move: float
    basis_value_usd: float | None
    fx: FxExposure
    warnings: tuple[HedgeWarning, ...] = ()
    method_version: str = METHOD_VERSION
    rounding: Rounding = Rounding.NEAREST
    inputs: dict[str, Any] = field(default_factory=dict)

    @property
    def total_notional_usd(self) -> float:
        return sum(leg.notional_usd for leg in self.legs)

    @property
    def is_over_hedged(self) -> bool:
        return self.residual_mt < 0

    @property
    def identifier(self) -> str:
        """Reproducible id: same inputs, same hash, on any machine."""
        return fingerprint({
            "method": self.method_version,
            "as_of": self.as_of,
            "exposure": self.exposure.to_dict(),
            "legs": [leg.to_dict() for leg in self.legs],
            "rounding": self.rounding.value,
        })

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.identifier,
            "method_version": self.method_version,
            "as_of": self.as_of.isoformat(),
            "exposure": self.exposure.to_dict(),
            "legs": [leg.to_dict() for leg in self.legs],
            "residual_mt": round(self.residual_mt, 3),
            "residual_pct": round(self.residual_pct, 3),
            "coverage_pct": round(self.coverage_pct, 3),
            "over_hedged": self.is_over_hedged,
            "basis_risk_usd_per_mt_move": round(self.basis_risk_usd_per_mt_move, 2),
            "basis_value_usd": None if self.basis_value_usd is None else round(self.basis_value_usd, 2),
            "fx": self.fx.to_dict(),
            "warnings": [w.to_dict() for w in self.warnings],
            "rounding": self.rounding.value,
            "total_notional_usd": round(self.total_notional_usd, 2),
            "inputs": self.inputs,
        }


# ---------------------------------------------------------------------------
# Sizing
# ---------------------------------------------------------------------------


def _round_contracts(raw: float, rounding: Rounding) -> int:
    if rounding is Rounding.DOWN:
        return int(math.floor(raw))
    if rounding is Rounding.UP:
        return int(math.ceil(raw))
    # Banker's rounding would make 0.5 contracts depend on the parity of the
    # count, which is not a property a hedge should have.
    return int(math.floor(raw + 0.5))


def size_leg(
    quote: ContractQuote,
    *,
    side: Side,
    physical_mt: float,
    hedge_ratio: float = 1.0,
    hedge_ratio_source: str = "1:1 — same commodity, tonne for tonne",
    rounding: Rounding = Rounding.NEAREST,
    cross_hedge_note: str = "",
) -> HedgeLeg:
    """Size one futures leg against ``physical_mt`` tonnes of physical.

    ``hedge_ratio`` is *futures tonnes per physical tonne*. 1.0 for a
    like-for-like hedge; the crush yields for a cross hedge; anything else must
    arrive with a source, which is why the source is a required-in-practice
    argument rather than an optional label.
    """
    require_hedgeable(quote, calculation="size_leg")
    require_traded_price(quote.price_type, context=f"size_leg({quote.contract.symbol})")
    if hedge_ratio <= 0:
        raise ValueError("hedge_ratio must be positive")
    spec = quote.contract.spec
    target_futures_mt = physical_mt * hedge_ratio
    raw_contracts = target_futures_mt / spec.mt_per_contract
    contracts = _round_contracts(raw_contracts, rounding)
    futures_mt = contracts * spec.mt_per_contract
    return HedgeLeg(
        quote=quote,
        side=side,
        contracts=contracts,
        hedge_ratio=hedge_ratio,
        hedge_ratio_source=hedge_ratio_source,
        target_physical_mt=physical_mt,
        futures_mt=futures_mt,
        covered_physical_mt=futures_mt / hedge_ratio,
        is_cross_hedge=bool(cross_hedge_note),
        cross_hedge_note=cross_hedge_note,
    )


def select_hedge_month(
    analysis: CurveAnalysis, exposure: PhysicalExposure, *, min_days_to_expiry: int = 5
) -> tuple[ContractQuote | None, HedgeWarning | None]:
    """The nearest listed month that is still trading when the physical prices.

    Nearest rather than most liquid, because liquidity is not observable here:
    no source in this stack publishes open interest, and volume is only present
    once the curve fetcher records it. Choosing "most liquid" from data we do
    not have would be the fabrication this package refuses. The rule is stated
    on the page so a trader can override it, which is the honest arrangement.
    """
    candidates = hedge_month_candidates(
        analysis, pricing_end=exposure.pricing_end, min_days_to_expiry=min_days_to_expiry
    )
    if not candidates:
        return None, HedgeWarning(
            code="no_hedge_month",
            message=(
                f"no listed {exposure.commodity} contract is still trading on "
                f"{exposure.pricing_end.isoformat()} with at least {min_days_to_expiry} sessions "
                "left — either the curve is short, or this product's expiry rule is not encoded"
            ),
            severity="alert",
        )
    return candidates[0].quote, None


def build_hedge(
    exposure: PhysicalExposure,
    legs: tuple[HedgeLeg, ...],
    *,
    as_of: date,
    fx: FxExposure | None = None,
    rounding: Rounding = Rounding.NEAREST,
    extra_warnings: tuple[HedgeWarning, ...] = (),
) -> HedgeProposal:
    """Assemble a proposal and compute what the legs leave behind."""
    # Re-asked here rather than trusted from `size_leg`: legs can be built by
    # hand or replaced on a frozen proposal, and this is the last point before
    # a number becomes a trade a person would place.
    for leg in legs:
        require_hedgeable(leg.quote, calculation="build_hedge")
    quantity_mt = exposure.quantity_mt
    covered = sum(leg.covered_physical_mt for leg in legs)
    residual = quantity_mt - covered
    coverage_pct = (covered / quantity_mt * 100.0) if quantity_mt else 0.0

    warnings = list(extra_warnings)

    if exposure.basis_convention is BasisConvention.FLAT_PRICE:
        warnings.append(HedgeWarning(
            code="flat_priced",
            message=(
                "this exposure is flat priced — the price is already fixed, so a futures leg "
                "does not offset anything and creates a new position instead"
            ),
            severity="alert",
        ))
    if exposure.basis_usd_per_mt is None:
        warnings.append(HedgeWarning(
            code="basis_unknown",
            message=(
                "no basis level was supplied, so basis risk is reported as sensitivity only — "
                "the basis P&L in any scenario is a move from an unknown starting level"
            ),
        ))
    for leg in legs:
        if not leg.quote.is_settlement_proven:
            warnings.append(HedgeWarning(
                code="reference_not_settlement",
                message=(
                    f"{leg.contract.symbol} reference price is a {leg.quote.price_label} from "
                    f"{leg.quote.provider.display}, not a proven exchange settlement"
                ),
                severity="info",
            ))
        if leg.contract.last_trade is None:
            warnings.append(HedgeWarning(
                code="expiry_not_encoded",
                message=(
                    f"{leg.contract.symbol}: this product's termination rule is not encoded, so "
                    "there is no days-to-expiry, no roll window and no expiry alert for it"
                ),
                severity="alert",
            ))
        elif leg.contract.first_notice and leg.contract.first_notice <= exposure.pricing_end:
            warnings.append(HedgeWarning(
                code="fnd_inside_pricing_window",
                message=(
                    f"{leg.contract.symbol} first notice day ({leg.contract.first_notice.isoformat()}) "
                    f"falls on or before the pricing window ends ({exposure.pricing_end.isoformat()}) — "
                    "a hedge left on past FND risks delivery"
                ),
                severity="alert",
            ))
    if abs(residual) > 0:
        warnings.append(HedgeWarning(
            code="residual_exposure",
            message=(
                f"{abs(residual):,.1f} MT {'over-hedged' if residual < 0 else 'unhedged'} after "
                f"rounding to whole contracts ({rounding.value})"
            ),
            severity="info",
        ))

    if fx is None and exposure.currency == "USD":
        fx = FxExposure(
            pair=None, rate=None, rate_date=None, amount_home=None, amount_usd=None,
            note="invoiced in USD — no currency exposure beyond the board",
        )
    elif fx is None:
        fx = FxExposure(
            pair=exposure.fx_pair, rate=None, rate_date=None, amount_home=None, amount_usd=None,
            note=(
                f"invoiced in {exposure.currency} but no {exposure.fx_pair} rate was available — "
                "the FX exposure is real and unquantified"
            ),
        )
    if fx.pair is not None and fx.rate is None:
        warnings.append(HedgeWarning(
            code="fx_rate_missing",
            message=fx.note,
            severity="alert",
        ))

    basis_value = (
        exposure.basis_usd_per_mt * quantity_mt if exposure.basis_usd_per_mt is not None else None
    )

    return HedgeProposal(
        exposure=exposure,
        legs=legs,
        as_of=as_of,
        residual_mt=residual,
        residual_pct=(residual / quantity_mt * 100.0) if quantity_mt else 0.0,
        coverage_pct=coverage_pct,
        # One USD/MT of basis move is worth one dollar per tonne of physical,
        # every tonne, hedged or not — the futures leg does not touch it. This
        # is the number that makes basis risk comparable with futures risk.
        basis_risk_usd_per_mt_move=quantity_mt,
        basis_value_usd=basis_value,
        fx=fx,
        warnings=tuple(warnings),
        rounding=rounding,
        inputs={
            "method_version": METHOD_VERSION,
            "as_of": as_of.isoformat(),
            "rounding": rounding.value,
        },
    )


def propose_hedge(
    exposure: PhysicalExposure,
    analysis: CurveAnalysis,
    *,
    as_of: date,
    contract_symbol: str | None = None,
    hedge_ratio: float = 1.0,
    hedge_ratio_source: str = "1:1 — same commodity, tonne for tonne",
    rounding: Rounding = Rounding.NEAREST,
    fx: FxExposure | None = None,
    min_days_to_expiry: int = 5,
) -> HedgeProposal:
    """Size a single-leg hedge, choosing the month if one was not named."""
    warnings: list[HedgeWarning] = []
    quote: ContractQuote | None = None

    if contract_symbol:
        quote = next(
            (leg.quote for leg in analysis.legs if leg.contract.symbol == contract_symbol.upper()),
            None,
        )
        if quote is None:
            warnings.append(HedgeWarning(
                code="requested_month_absent",
                message=(
                    f"{contract_symbol.upper()} is not on the current {analysis.commodity} curve — "
                    "falling back to the nearest month that covers the pricing window"
                ),
                severity="alert",
            ))
    if quote is None:
        quote, problem = select_hedge_month(
            analysis, exposure, min_days_to_expiry=min_days_to_expiry
        )
        if problem:
            warnings.append(problem)

    if not analysis.coherent:
        warnings.append(HedgeWarning(
            code="curve_incoherent",
            message=(
                "the curve this hedge was sized from is not a single session: "
                f"{analysis.coherence_note}"
            ),
            severity="alert",
        ))

    legs: tuple[HedgeLeg, ...] = ()
    if quote is not None:
        legs = (size_leg(
            quote,
            side=exposure.futures_side,
            physical_mt=exposure.quantity_mt,
            hedge_ratio=hedge_ratio,
            hedge_ratio_source=hedge_ratio_source,
            rounding=rounding,
            cross_hedge_note=(
                "" if quote.contract.spec.name == exposure.commodity
                else (
                    f"cross hedge: {exposure.commodity} exposure hedged in "
                    f"{quote.contract.spec.display}; the ratio is an input, not a fitted number"
                )
            ),
        ),)

    return build_hedge(
        exposure, legs, as_of=as_of, fx=fx, rounding=rounding, extra_warnings=tuple(warnings)
    )


# ---------------------------------------------------------------------------
# Crush cross-hedge
# ---------------------------------------------------------------------------

CRUSH_YIELDS_MT = {
    "Soybean Meal": CRUSH_MEAL_YIELD_MT,
    "Soybean Oil": CRUSH_OIL_YIELD_MT,
}

CRUSH_YIELD_SOURCE = (
    "60-lb bushel mass balance behind the CBOT board crush factors "
    "(~11 lb oil + ~44 lb meal per bushel), shared with analysis/spreads.py"
)


def propose_crush_hedge(
    exposure: PhysicalExposure,
    bean_curve: CurveAnalysis,
    meal_curve: CurveAnalysis,
    oil_curve: CurveAnalysis,
    *,
    as_of: date,
    rounding: Rounding = Rounding.NEAREST,
    fx: FxExposure | None = None,
    min_days_to_expiry: int = 5,
) -> HedgeProposal:
    """Hedge a crusher's bean position: sell beans forward, buy back the products.

    A crusher who owns beans is not simply long a flat price — they are long the
    *crush*. Selling ZS alone locks the bean cost and leaves the product
    revenue floating. The three-leg hedge is the one that matches the business:
    short the beans they own, long the meal and oil they will produce, sized at
    the mass-balance yields.

    The product legs are cross hedges by definition and are labelled as such:
    ZM and ZL settle against US products, and a crusher outside the US carries
    the product basis between their local market and the board. That is
    reported, not assumed away.

    **The three months are one crush period, not three independent choices.**
    The bean month is chosen against the exposure's pricing window; the product
    months then *follow* it under the documented convention
    (``analysis.futures.crush.product_month_for`` — same month, except November
    beans, which crush into December). Letting each leg pick its own nearest
    month is how a Nov bean short ends up against Oct products: a legal set of
    contracts that is not a crush, and the P&L of the mismatch is invisible in
    the sizing.
    """
    from analysis.futures.crush import product_month_for

    if exposure.side is not Side.LONG:
        raise ValueError(
            "a crush hedge applies to a long bean position; a short bean position is the "
            "opposite trade and must be built explicitly"
        )

    warnings: list[HedgeWarning] = []
    legs: list[HedgeLeg] = []

    bean_quote, problem = select_hedge_month(bean_curve, exposure, min_days_to_expiry=min_days_to_expiry)
    if problem:
        warnings.append(problem)
    if bean_quote is not None:
        legs.append(size_leg(
            bean_quote, side=Side.SHORT, physical_mt=exposure.quantity_mt,
            hedge_ratio=1.0, hedge_ratio_source="1:1 — the beans owned", rounding=rounding,
        ))
        product_year, product_month = product_month_for(
            bean_quote.contract.year, bean_quote.contract.month
        )

        for commodity, analysis in (("Soybean Meal", meal_curve), ("Soybean Oil", oil_curve)):
            product_quote = next(
                (
                    leg.quote for leg in analysis.legs
                    if leg.contract.year == product_year and leg.contract.month == product_month
                ),
                None,
            )
            if product_quote is None:
                warnings.append(HedgeWarning(
                    code="crush_month_unquoted",
                    message=(
                        f"{bean_quote.contract.symbol} crushes into the {product_year}-"
                        f"{product_month:02d} {commodity} contract under this project's crush "
                        "convention, and no quote for it is stored — the product leg is left "
                        "off rather than placed in a month that is not the crush's"
                    ),
                    severity="alert",
                ))
                continue
            if product_quote.observation_date != bean_quote.observation_date:
                warnings.append(HedgeWarning(
                    code="crush_legs_mixed_sessions",
                    message=(
                        f"{product_quote.contract.symbol} is observed "
                        f"{product_quote.observation_date.isoformat()} against "
                        f"{bean_quote.contract.symbol} on "
                        f"{bean_quote.observation_date.isoformat()} — the crush value implied "
                        "by these legs spans two sessions"
                    ),
                    severity="alert",
                ))
            legs.append(size_leg(
                product_quote, side=Side.LONG, physical_mt=exposure.quantity_mt,
                hedge_ratio=CRUSH_YIELDS_MT[commodity],
                hedge_ratio_source=CRUSH_YIELD_SOURCE,
                rounding=rounding,
                cross_hedge_note=(
                    f"cross hedge: {commodity} produced from the bean position, sized at the "
                    "mass-balance yield; product basis to the local market is not hedged"
                ),
            ))

    proposal = build_hedge(
        exposure, tuple(legs), as_of=as_of, fx=fx, rounding=rounding, extra_warnings=tuple(warnings)
    )
    # Coverage for a crush hedge is the bean leg's, not the sum of three legs
    # sized on different yields — adding them would report ~192% coverage of a
    # position that is hedged once.
    bean_legs = tuple(leg for leg in proposal.legs if leg.contract.spec.name == "Soybeans")
    bean_covered = sum(leg.covered_physical_mt for leg in bean_legs)
    residual = exposure.quantity_mt - bean_covered
    return HedgeProposal(
        exposure=proposal.exposure,
        legs=proposal.legs,
        as_of=proposal.as_of,
        residual_mt=residual,
        residual_pct=(residual / exposure.quantity_mt * 100.0) if exposure.quantity_mt else 0.0,
        coverage_pct=(bean_covered / exposure.quantity_mt * 100.0) if exposure.quantity_mt else 0.0,
        basis_risk_usd_per_mt_move=proposal.basis_risk_usd_per_mt_move,
        basis_value_usd=proposal.basis_value_usd,
        fx=proposal.fx,
        warnings=proposal.warnings,
        rounding=proposal.rounding,
        inputs={**proposal.inputs, "structure": "crush", "crush_yields": dict(CRUSH_YIELDS_MT)},
    )


def fx_exposure_from_rate(
    exposure: PhysicalExposure,
    reference_price_usd_mt: float | None,
    rate: tuple[date, float] | None,
) -> FxExposure:
    """Quantify the currency exposure a USD futures hedge leaves behind.

    ``rate`` is (observation date, USD per unit of home currency) — the stack's
    ``<CCY>/USD`` convention, which callers *multiply* by. The rate's own date
    is carried because a hedge struck at a three-day-old rate must say so.
    """
    if exposure.currency == "USD":
        return FxExposure(
            pair=None, rate=None, rate_date=None, amount_home=None, amount_usd=None,
            note="invoiced in USD — no currency exposure beyond the board",
        )
    if rate is None or reference_price_usd_mt is None:
        return FxExposure(
            pair=exposure.fx_pair, rate=None, rate_date=None, amount_home=None, amount_usd=None,
            note=(
                f"invoiced in {exposure.currency}; no usable "
                f"{exposure.fx_pair or 'FX'} rate, so the exposure is real and unquantified"
            ),
        )
    rate_date, rate_value = rate
    amount_usd = reference_price_usd_mt * exposure.quantity_mt
    return FxExposure(
        pair=exposure.fx_pair,
        rate=rate_value,
        rate_date=rate_date,
        amount_home=amount_usd / rate_value if rate_value else None,
        amount_usd=amount_usd,
        hedged=False,
        note=(
            f"the futures leg is USD-settled; the whole {amount_usd:,.0f} USD of value is "
            f"exposed to {exposure.fx_pair} until it is sold forward"
        ),
    )


__all__ = [
    "CRUSH_YIELDS_MT",
    "CRUSH_YIELD_SOURCE",
    "BasisConvention",
    "FxExposure",
    "HedgeLeg",
    "HedgeProposal",
    "HedgeWarning",
    "PhysicalExposure",
    "PhysicalUnit",
    "Rounding",
    "build_hedge",
    "fx_exposure_from_rate",
    "propose_crush_hedge",
    "propose_hedge",
    "select_hedge_month",
    "size_leg",
    "to_metric_tons",
]
