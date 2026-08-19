"""Value objects for origin comparison and landed-cost economics (Phase 2).

This module is the shared vocabulary. It depends only on the standard library:
persistence, SQL and dataframe concerns belong in ``sources.py``, presentation
in ``app/``. The same separation ``trust/domain.py`` keeps, for the same reason
— a vocabulary that can read a database grows a database's opinions.

Three rules run through every type here, and each one exists because getting it
wrong produces a *plausible* number rather than a crash:

**Units and currencies never add silently.** ``Money`` refuses to combine two
amounts whose currency or unit differ. A landed cost is a sum of eight things
sourced from four venues in three currencies; the one failure mode that would
never be caught in review is a clean-looking total whose terms were not the
same animal.

**An incoterm is part of the price.** A CIF NOLA barge bid and a Paranaguá FOB
vessel offer are not comparable numbers, and the difference is roughly the cost
of elevating grain from a barge into a vessel — a real, non-zero, *unpublished*
figure. So a quote carries its incoterm and its carrier, and turning one into
another is an explicit, costed, attributable step (``landed_cost.py``), never a
relabelling.

**A shipment window is part of the price.** "Soybeans, US Gulf" is not a price;
"soybeans, US Gulf, October loading" is. Two origins quoted for different
windows differ by carry as much as by competitiveness, so a window mismatch is
a first-class state (:class:`Comparability`) rather than a footnote.

Nothing here computes an arbitrage. It defines what may be compared, and says
plainly what may not.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import date
from enum import Enum
from typing import Any

from pricing.semantics import (
    CONFIDENCE_CEILING,
    CONFIDENCE_RANK,
    Confidence,
    PriceType,
    price_type_for_quote_kind,
    worst_confidence,
)

# ---------------------------------------------------------------------------
# Units
# ---------------------------------------------------------------------------
USD = "USD"
USD_PER_MT = "usd_per_mt"

# Everything downstream of `sources.py` is USD/MT. The native units
# (cents/bu, BRL/MT, $/bu) are converted exactly once, at the read boundary,
# by `app.markets.Source.to_usd_mt` — the site's single conversion site. This
# module never converts; it only refuses to add things that were not converted.


class Incoterm(str, Enum):
    """Delivery terms, in the order they add cost from the seller outward.

    Only the terms this stack can actually source are listed. An incoterm that
    no ingested quote uses would be an untested code path pretending to be
    coverage.
    """

    EXW = "EXW"    # ex works / farmgate — buyer takes it at the gate
    FCA = "FCA"    # free carrier — loaded at a named inland point
    FOB = "FOB"    # free on board — over the rail at the origin port
    CFR = "CFR"    # cost and freight — ocean freight included, no insurance
    CIF = "CIF"    # cost, insurance and freight — to the named destination
    DAP = "DAP"    # delivered at place — duty NOT paid
    DDP = "DDP"    # delivered duty paid


class Carrier(str, Enum):
    """What the incoterm's named point is a point *for*.

    The distinction the AMS Gulf bid makes necessary: "CIF NOLA" on report 3147
    is CIF onto a **barge** at the New Orleans area, which is one elevation
    short of being on a vessel. Reading it as CIF-vessel understates the US
    origin by the elevation spread, silently and always in the same direction.
    """

    VESSEL = "vessel"
    BARGE = "barge"
    TRUCK = "truck"
    RAIL = "rail"
    GATE = "gate"      # no carrier — farmgate / ex-plant


class QuoteKind(str, Enum):
    """What sort of number this is. Vocabulary shared with ``app.markets``.

    Kept as its own enum rather than importing the frozenset from
    ``app/markets.py``: that one is the *site registry's* validation set and is
    consumed at descriptor-load time, while this is the economic model's. They
    are checked against each other in the tests, which is the right coupling —
    a shared constant would make ``analysis`` import ``app``, inverting the
    dependency ARCHITECTURE.md fixes one way.
    """

    BOARD = "board"
    BOARD_LAST_TRADED = "board_last_traded"
    PHYSICAL = "physical"
    ADMINISTERED = "administered"
    WEEKLY_ASSESSMENT = "weekly_assessment"


# `Confidence` and the worst-wins helper are the canonical ones from
# `pricing.semantics`, re-exported here so every existing import site keeps
# working. Defining a second copy is what allowed a delayed CBOT bar to be
# `executable` on this page while the workstation called the same number an
# unproven close.
_CONFIDENCE_RANK = CONFIDENCE_RANK

# Which confidence a quote of each kind can support on its own, before any
# assumption drags it down. *Derived* from the price type's ceiling rather than
# restated: a board quote reaches this stack as a delayed daily bar from a
# consumer endpoint, so its ceiling is `board_reference` and `executable` is
# unreachable while no provider proves a settlement. An administered minimum is
# not "low confidence" — it is precisely known and legally binding, and simply
# is not a traded price; collapsing those two ideas into one axis is why it
# keeps its own value.
CONFIDENCE_BY_QUOTE_KIND: dict[QuoteKind, Confidence] = {
    kind: CONFIDENCE_CEILING[price_type_for_quote_kind(kind.value)]
    for kind in QuoteKind
}


def price_type_of(kind: QuoteKind) -> PriceType:
    """What sort of number a quote of this kind is."""
    return price_type_for_quote_kind(kind.value)


class Freshness(str, Enum):
    CURRENT = "current"
    STALE = "stale"
    UNAVAILABLE = "unavailable"


class CostComponent(str, Enum):
    """One rung of the landed-cost waterfall, in application order.

    The order is the model. ``IMPORT_DUTY`` is a percentage of the CIF value,
    so it cannot be applied before ocean freight; ``VAT`` is a percentage of
    the duty-paid value, so it cannot be applied before duty. A component set
    is not a bag of numbers to sum — it is a sequence, and
    ``landed_cost.build_waterfall`` walks it in exactly this order.
    """

    ORIGIN_PRICE = "origin_price"
    INLAND_TRANSPORT = "inland_transport"
    ORIGIN_PORT_COSTS = "origin_port_costs"
    ELEVATION = "elevation"
    OCEAN_FREIGHT = "ocean_freight"
    MARINE_INSURANCE = "marine_insurance"
    IMPORT_DUTY = "import_duty"
    IMPORT_VAT = "import_vat"
    DESTINATION_PORT_COSTS = "destination_port_costs"
    FINANCING = "financing"
    QUALITY_ADJUSTMENT = "quality_adjustment"

    # Crush-plant rungs. Deliberately in the same vocabulary as the landed
    # stack — they are entered through the identical assumption contract, with
    # the same owner/expiry discipline — but they are NOT members of
    # COMPONENT_ORDER below, because they do not sit anywhere in the ad-valorem
    # chain of a cargo's landed cost. `build_waterfall` rejects them for that
    # reason rather than silently appending them at the end.
    PROCESSING_COST = "processing_cost"
    ENERGY_COST = "energy_cost"
    PLANT_FREIGHT_IN = "plant_freight_in"
    WORKING_CAPITAL = "working_capital"


# The canonical order. A component missing from this tuple cannot be placed in
# a waterfall, which is deliberate: adding a cost means deciding where in the
# ad-valorem chain it sits, and that decision belongs here rather than at a
# call site that happens to append.
COMPONENT_ORDER: tuple[CostComponent, ...] = (
    CostComponent.ORIGIN_PRICE,
    CostComponent.INLAND_TRANSPORT,
    CostComponent.ORIGIN_PORT_COSTS,
    CostComponent.ELEVATION,
    CostComponent.OCEAN_FREIGHT,
    CostComponent.MARINE_INSURANCE,
    CostComponent.IMPORT_DUTY,
    CostComponent.IMPORT_VAT,
    CostComponent.DESTINATION_PORT_COSTS,
    CostComponent.FINANCING,
    CostComponent.QUALITY_ADJUSTMENT,
)

# Components quoted as a rate against a running value rather than as USD/MT.
# Their `value` is a fraction (0.03 = 3%), and the base they apply to is fixed
# by COMPONENT_ORDER above, never by the assumption file.
AD_VALOREM_COMPONENTS = frozenset({
    CostComponent.IMPORT_DUTY,
    CostComponent.IMPORT_VAT,
    CostComponent.MARINE_INSURANCE,
})

# Financing is neither flat nor a plain percentage: it is rate x days / 365
# applied to the pre-financing value. Its own rung, shared with the crush
# plant's working capital, which is the same arithmetic on a different asset.
RATE_TIME_COMPONENTS = frozenset({CostComponent.FINANCING, CostComponent.WORKING_CAPITAL})

# The crush plant's own cost stack. Not part of a cargo's landed cost — see the
# comment on CostComponent.PROCESSING_COST.
CRUSH_COST_COMPONENTS: tuple[CostComponent, ...] = (
    CostComponent.PLANT_FREIGHT_IN,
    CostComponent.PROCESSING_COST,
    CostComponent.ENERGY_COST,
    CostComponent.WORKING_CAPITAL,
)


class Comparability(str, Enum):
    """Why two origin rows may or may not be put in one ranking.

    ``COMPARABLE`` is the only state that earns a rank. Everything else is
    rendered — hiding a row would hide the fact that an origin exists — but is
    excluded from "cheapest origin" and from the advantage figure. This is the
    fail-closed rule the whole module is built around: an unrankable row is a
    labelled statement, never a silent omission and never a rank it did not
    earn.
    """

    COMPARABLE = "comparable"
    WINDOW_MISMATCH = "window_mismatch"          # quoted for a different shipment period
    WINDOW_UNKNOWN = "window_unknown"            # the source publishes no window at all
    INCOTERM_UNBRIDGED = "incoterm_unbridged"    # no costed path to FOB vessel
    MISSING_INPUT = "missing_input"              # a required assumption is absent or expired
    STALE = "stale"                              # the quote is past its layer's recency budget
    OBSERVATION_SPREAD = "observation_spread"    # priced too many days from its peers


@dataclass(frozen=True)
class ShipmentWindow:
    """A loading/delivery period, closed on both ends.

    Windows are compared by *overlap*, not equality. MAGyP publishes
    ``2026-11`` → ``2027-07`` as one administered band while AMS quotes a
    half-month slot; demanding equality would reject every real pairing, and
    demanding nothing would compare October loading against next July's.
    """

    start: date
    end: date
    label: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.start, date) or not isinstance(self.end, date):
            raise ValueError("shipment window bounds must be dates")
        if self.end < self.start:
            raise ValueError(f"shipment window ends ({self.end}) before it starts ({self.start})")

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1

    def overlaps(self, other: ShipmentWindow) -> bool:
        return self.start <= other.end and other.start <= self.end

    def overlap_days(self, other: ShipmentWindow) -> int:
        if not self.overlaps(other):
            return 0
        first = max(self.start, other.start)
        last = min(self.end, other.end)
        return (last - first).days + 1

    def contains(self, other: ShipmentWindow) -> bool:
        return self.start <= other.start and other.end <= self.end

    def midpoint(self) -> date:
        from datetime import timedelta

        return self.start + timedelta(days=(self.end - self.start).days // 2)

    def describe(self) -> str:
        return self.label or f"{self.start.isoformat()}..{self.end.isoformat()}"

    def to_dict(self) -> dict[str, Any]:
        return {"start": self.start.isoformat(), "end": self.end.isoformat(), "label": self.label}


@dataclass(frozen=True)
class Money:
    """An amount with a currency and a unit, which refuses to lose either.

    Addition and subtraction are guarded; multiplication by a dimensionless
    factor is not, because scaling by a yield or a percentage keeps the unit.
    """

    amount: float
    currency: str = USD
    unit: str = USD_PER_MT

    def __post_init__(self) -> None:
        if not isinstance(self.amount, (int, float)) or isinstance(self.amount, bool):
            raise ValueError("Money.amount must be a real number")
        if self.amount != self.amount:  # NaN
            raise ValueError("Money.amount cannot be NaN — a missing value is None, not NaN")
        if not self.currency or len(self.currency) != 3:
            raise ValueError(f"Money.currency must be a three-letter code, got {self.currency!r}")
        object.__setattr__(self, "currency", self.currency.upper())

    def _check(self, other: Money) -> None:
        if self.currency != other.currency or self.unit != other.unit:
            raise ValueError(
                f"cannot combine {self.currency}/{self.unit} with "
                f"{other.currency}/{other.unit} — a landed cost that adds two "
                "different units looks exactly like one that does not"
            )

    def __add__(self, other: Money) -> Money:
        self._check(other)
        return Money(self.amount + other.amount, self.currency, self.unit)

    def __sub__(self, other: Money) -> Money:
        self._check(other)
        return Money(self.amount - other.amount, self.currency, self.unit)

    def scaled(self, factor: float) -> Money:
        return Money(self.amount * factor, self.currency, self.unit)

    def to_dict(self) -> dict[str, Any]:
        return {"amount": self.amount, "currency": self.currency, "unit": self.unit}


def usd_mt(amount: float) -> Money:
    return Money(float(amount), USD, USD_PER_MT)


@dataclass(frozen=True)
class FxObservation:
    """The rate a conversion was actually struck at, and when.

    Direction is stated, not inferred. Every currency series in this stack is
    ``<CCY>/USD`` meaning **USD per one unit of the local currency**, so the
    conversion multiplies — and the one place that gets remembered wrong is the
    only place it matters. ``usd_per_unit`` names the direction in the field
    itself so a reviewer does not have to recall a convention.
    """

    pair: str
    usd_per_unit: float
    observed_on: date
    convention: str = "usd-per-local-unit"

    def __post_init__(self) -> None:
        if self.usd_per_unit <= 0:
            raise ValueError(f"FX rate for {self.pair} must be positive, got {self.usd_per_unit}")
        if self.convention != "usd-per-local-unit":
            raise ValueError("only the usd-per-local-unit convention is supported")

    def to_usd(self, local_amount: float) -> float:
        return local_amount * self.usd_per_unit

    def to_local(self, usd_amount: float) -> float:
        return usd_amount / self.usd_per_unit

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "usd_per_unit": self.usd_per_unit,
            "observed_on": self.observed_on.isoformat(),
            "convention": self.convention,
        }


@dataclass(frozen=True)
class SourceRef:
    """Where a number came from, precisely enough to go and look at it.

    ``layer`` is the pipeline layer name — the same string ``main.py`` grades
    freshness on and ``config.LAYER_MAX_DATA_AGE_DAYS`` is keyed by — so the
    page and the pipeline cannot disagree about whether a source is alive.
    """

    layer: str
    table: str
    key: str
    detail: str | None = None
    href: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "layer": self.layer,
            "table": self.table,
            "key": self.key,
            "detail": self.detail,
            "href": self.href,
        }


@dataclass(frozen=True)
class ContractRef:
    """The board contract a physical price is hedged or quoted against.

    AMS report 3147 names one (``futures_month``); a Paranaguá FOB assessment
    does not. ``None`` is therefore a real answer and means "this price has no
    stated hedge leg", which is different from "we did not look".
    """

    exchange: str
    code: str
    delivery_month: str  # YYYY-MM

    def to_dict(self) -> dict[str, Any]:
        return {"exchange": self.exchange, "code": self.code, "delivery_month": self.delivery_month}


@dataclass(frozen=True)
class Grade:
    """Contractual quality. Differences here are money, so they are named.

    US No. 2 Yellow and Brazilian "soja em grão" are not the same contract
    specification (protein and FM allowances differ), and a Chinese crusher
    pays for protein. This stack ingests no protein series, so
    ``quality_adjustment`` is always an assumption and always zero until one is
    entered — but the *specification* is recorded so that when a differential
    is entered, what it is a differential between is on the record.
    """

    product: str = "soybeans"
    specification: str | None = None
    protein_pct: float | None = None
    moisture_pct: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "product": self.product,
            "specification": self.specification,
            "protein_pct": self.protein_pct,
            "moisture_pct": self.moisture_pct,
        }


@dataclass(frozen=True)
class Port:
    """A pricing location: a port, a river position, or an inland basis point."""

    key: str
    name: str
    country: str
    country_iso: str
    role: str = "origin"  # origin | destination

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "name": self.name,
            "country": self.country,
            "country_iso": self.country_iso,
            "role": self.role,
        }


@dataclass(frozen=True)
class OriginQuote:
    """One observed price at one origin, with everything needed to compare it.

    Every field the trader question needs is here or on one of its members;
    nothing is looked up later. That is deliberate — a quote that has to be
    re-joined against a table to find its own currency is a quote whose
    currency can be wrong.
    """

    origin: Port
    grade: Grade
    quote_kind: QuoteKind
    incoterm: Incoterm
    carrier: Carrier
    price: Money                      # already USD/MT (converted at the read boundary)
    native_price: float
    native_currency: str
    native_unit: str
    observation_date: date
    publication_date: date | None
    source: SourceRef
    shipment_window: ShipmentWindow | None = None
    fx: FxObservation | None = None
    board_contract: ContractRef | None = None
    quotes_averaged: int = 1
    max_age_days: int = 14
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.price.unit != USD_PER_MT or self.price.currency != USD:
            raise ValueError(
                "OriginQuote.price must already be USD/MT — conversion happens once, "
                "at the read boundary (see analysis/origins/sources.py)"
            )
        if self.native_currency != USD and self.fx is None:
            raise ValueError(
                f"{self.source.key}: a quote native in {self.native_currency} must carry the "
                "FX observation it was converted at, or its USD figure cannot be audited"
            )

    def age_days(self, today: date) -> int:
        return (today - self.observation_date).days

    def freshness(self, today: date) -> Freshness:
        return Freshness.CURRENT if self.age_days(today) <= self.max_age_days else Freshness.STALE

    @property
    def base_confidence(self) -> Confidence:
        return CONFIDENCE_BY_QUOTE_KIND[self.quote_kind]

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin.to_dict(),
            "grade": self.grade.to_dict(),
            "quote_kind": self.quote_kind.value,
            "incoterm": self.incoterm.value,
            "carrier": self.carrier.value,
            "price_usd_mt": self.price.amount,
            "native_price": self.native_price,
            "native_currency": self.native_currency,
            "native_unit": self.native_unit,
            "observation_date": self.observation_date.isoformat(),
            "publication_date": self.publication_date.isoformat() if self.publication_date else None,
            "source": self.source.to_dict(),
            "shipment_window": self.shipment_window.to_dict() if self.shipment_window else None,
            "fx": self.fx.to_dict() if self.fx else None,
            "board_contract": self.board_contract.to_dict() if self.board_contract else None,
            "quotes_averaged": self.quotes_averaged,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class Blocker:
    """A named reason a row cannot be ranked, and what would unblock it.

    ``remedy`` is not decoration. Every blocker this module raises is fixable
    by a human action — enter a freight assumption, pick a different window,
    wait for a scraper — and a blocked page that does not say which action is
    a page that gets ignored.
    """

    code: str
    message: str
    remedy: str | None = None
    component: CostComponent | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "remedy": self.remedy,
            "component": self.component.value if self.component else None,
        }


@dataclass(frozen=True)
class WaterfallStep:
    """One line of the landed-cost waterfall, with its provenance.

    ``running_total`` is stored rather than re-derived so the rendered
    waterfall is the arithmetic that was actually performed — a template that
    re-adds the column can disagree with the total it prints beneath it.
    """

    component: CostComponent
    label: str
    amount: Money
    running_total: Money
    basis: str
    confidence: Confidence
    assumption_id: str | None = None
    entered_by: str | None = None
    entered_at: date | None = None
    expires_on: date | None = None
    rate: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "component": self.component.value,
            "label": self.label,
            "amount_usd_mt": self.amount.amount,
            "running_total_usd_mt": self.running_total.amount,
            "basis": self.basis,
            "confidence": self.confidence.value,
            "assumption_id": self.assumption_id,
            "entered_by": self.entered_by,
            "entered_at": self.entered_at.isoformat() if self.entered_at else None,
            "expires_on": self.expires_on.isoformat() if self.expires_on else None,
            "rate": self.rate,
        }


@dataclass(frozen=True)
class LandedCost:
    """The result for one origin into one destination for one window.

    ``landed`` is ``None`` whenever any blocker is present. That is the whole
    fail-closed contract in one line: a partial waterfall does not produce a
    partial total, because a partial total is indistinguishable from a real one
    once it is rendered in a column beside two complete ones.
    """

    quote: OriginQuote
    destination: Port
    requested_window: ShipmentWindow
    steps: tuple[WaterfallStep, ...]
    landed: Money | None
    fob_equivalent: Money | None
    comparability: Comparability
    confidence: Confidence
    freshness: Freshness
    blockers: tuple[Blocker, ...] = ()
    missing_inputs: tuple[CostComponent, ...] = ()
    method_version: str = "unset"
    notes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.landed is not None and self.blockers:
            raise ValueError(
                "a landed cost cannot be both complete and blocked — a total published "
                "beside its own blockers is the number a reader will use anyway"
            )
        if self.comparability is Comparability.COMPARABLE and self.landed is None:
            raise ValueError("a comparable row must carry a landed cost")

    @property
    def is_rankable(self) -> bool:
        return self.comparability is Comparability.COMPARABLE and self.landed is not None

    @property
    def landed_usd_mt(self) -> float | None:
        return self.landed.amount if self.landed else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "quote": self.quote.to_dict(),
            "destination": self.destination.to_dict(),
            "requested_window": self.requested_window.to_dict(),
            "steps": [step.to_dict() for step in self.steps],
            "landed_usd_mt": self.landed_usd_mt,
            "fob_equivalent_usd_mt": self.fob_equivalent.amount if self.fob_equivalent else None,
            "comparability": self.comparability.value,
            "confidence": self.confidence.value,
            "freshness": self.freshness.value,
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "missing_inputs": [component.value for component in self.missing_inputs],
            "method_version": self.method_version,
            "notes": list(self.notes),
        }


@dataclass(frozen=True)
class UnavailableOrigin:
    """A declared origin this stack ingests no price for — rendered, never dropped.

    The US Pacific Northwest is the case this type exists for: a real third US
    origin a trader compares against, covered by no free series here. It gets
    its own type rather than a :class:`LandedCost` with placeholder numbers,
    because a placeholder price is a fabricated value and would reach the page
    the moment somebody rendered the field. There is nothing to place-hold —
    the honest object carries a name and a reason and no numbers at all.
    """

    port: Port
    label: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {"port": self.port.to_dict(), "label": self.label, "reason": self.reason}


@dataclass(frozen=True)
class OriginRanking:
    """The answer to the trader question, or an honest statement that there is none.

    ``advantage_usd_mt`` is the cheapest rankable row against the *second*
    cheapest rankable row — never against a blocked one, and never against a
    row whose window did not match. A one-row ranking has no advantage figure
    at all: "cheapest of one" is a sentence that reads like a finding.
    """

    destination: Port
    requested_window: ShipmentWindow
    as_of: date
    rows: tuple[LandedCost, ...]
    method_version: str
    assumption_set_id: str
    observation_spread_days: int | None = None
    unavailable: tuple[UnavailableOrigin, ...] = ()
    notes: tuple[str, ...] = ()

    @property
    def rankable(self) -> tuple[LandedCost, ...]:
        return tuple(
            sorted(
                (row for row in self.rows if row.is_rankable),
                key=lambda row: row.landed_usd_mt,  # type: ignore[arg-type,return-value]
            )
        )

    @property
    def excluded(self) -> tuple[LandedCost, ...]:
        return tuple(row for row in self.rows if not row.is_rankable)

    @property
    def cheapest(self) -> LandedCost | None:
        ordered = self.rankable
        return ordered[0] if ordered else None

    @property
    def advantage_usd_mt(self) -> float | None:
        ordered = self.rankable
        if len(ordered) < 2:
            return None
        return ordered[1].landed_usd_mt - ordered[0].landed_usd_mt  # type: ignore[operator]

    @property
    def advantage_pct(self) -> float | None:
        ordered = self.rankable
        advantage = self.advantage_usd_mt
        if advantage is None or not ordered[1].landed_usd_mt:
            return None
        return advantage / ordered[1].landed_usd_mt * 100.0

    @property
    def confidence(self) -> Confidence:
        return worst_confidence(*(row.confidence for row in self.rankable))

    @property
    def is_decisive(self) -> bool:
        """Whether the page may state a preferred origin at all."""
        return len(self.rankable) >= 2

    def to_dict(self) -> dict[str, Any]:
        return {
            "destination": self.destination.to_dict(),
            "requested_window": self.requested_window.to_dict(),
            "as_of": self.as_of.isoformat(),
            "method_version": self.method_version,
            "assumption_set_id": self.assumption_set_id,
            "observation_spread_days": self.observation_spread_days,
            "cheapest_origin": self.cheapest.quote.origin.key if self.cheapest else None,
            "advantage_usd_mt": self.advantage_usd_mt,
            "advantage_pct": self.advantage_pct,
            "confidence": self.confidence.value,
            "is_decisive": self.is_decisive,
            "rows": [row.to_dict() for row in self.rows],
            "unavailable": [origin.to_dict() for origin in self.unavailable],
            "notes": list(self.notes),
        }


def fingerprint(payload: Any) -> str:
    """Stable short hash of a JSON-able payload.

    Used to key a stored ranking to the exact inputs that produced it, so
    "the ranking changed" can be separated from "an input changed" without
    diffing two blobs by eye.
    """
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:16]


__all__ = [
    "AD_VALOREM_COMPONENTS",
    "COMPONENT_ORDER",
    "CONFIDENCE_BY_QUOTE_KIND",
    "CRUSH_COST_COMPONENTS",
    "RATE_TIME_COMPONENTS",
    "USD",
    "USD_PER_MT",
    "Blocker",
    "Carrier",
    "Comparability",
    "Confidence",
    "ContractRef",
    "CostComponent",
    "FxObservation",
    "Freshness",
    "Grade",
    "Incoterm",
    "LandedCost",
    "Money",
    "OriginQuote",
    "OriginRanking",
    "Port",
    "PriceType",
    "QuoteKind",
    "ShipmentWindow",
    "SourceRef",
    "UnavailableOrigin",
    "WaterfallStep",
    "fingerprint",
    "price_type_of",
    "replace",
    "usd_mt",
    "worst_confidence",
]
