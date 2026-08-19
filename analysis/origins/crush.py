"""Four crush margins that are not the same number (Phase 2, Phase 6).

A crusher's economics get quoted as one figure — "the crush" — and the figure
means several different things depending on who is saying it. The ladder is one
closed vocabulary, :class:`analysis.futures.crush.CrushLevel`, imported here
rather than redefined; this module computes the two physical levels and
delegates the two board levels to the named-contract calculation.

**Board crush — delayed-close reference.** Three *named contracts* of one crush
period: ``oil x yield + meal x yield − bean``. Struck by
``analysis.futures.crush.named_board_crush`` on ZSU26/ZMU26/ZLU26 and their
successors, never on a stitched front-month series — which names no contract,
rolls silently on the provider's own schedule and cannot be hedged. It is a
*paper* margin and it is the useful one for direction. It is not an exchange
settlement (no provider here proves one), and it is not what any plant earns.

**Board crush — official settlements.** The same arithmetic on proven
settlements. Not constructible today; see ``pricing.semantics``.

**Gross physical crush.** The same arithmetic on *physical* legs — a cash bean
delivered somewhere real against cash oil and meal at the same place on the
same day. It captures the basis on all three legs, which is most of what the
board misses. It still contains no cost of running a plant.

**Estimated net plant margin.** Gross physical, less the cost of turning beans
into products: freight to the plant, processing, energy, and the working
capital tied up while it happens. Every one of those is a hand-entered
assumption here, because no free source publishes any of them for any origin.
Which means this figure is *estimated* in the strong sense and says so in its
own name — and when the assumptions are absent it is blocked, not defaulted.

Three rules, the first two the same ones the landed cost keeps:

* **One session for all three legs.** A margin struck across days is not a
  margin. Where the legs do not share a date the result is empty with that as
  its reason.
* **A missing leg is named, never substituted.** The US has a published cash
  bean and no published cash oil or meal, so there is no US gross physical
  crush here. Falling back to the board and relabelling it is the failure this
  module exists to prevent.
* **What the legs structurally are is carried, not implied.**
  :class:`~analysis.futures.crush.ContractBasis` rides on every result. A
  margin off continuous main-contract series (Dalian) is arithmetically fine
  and structurally unhedgeable, and the page must be able to tell the two
  apart without reading a docstring.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import config
from analysis.futures.crush import (
    CRUSH_CONVENTION_NOTE,
    ContractBasis,
    CrushLevel,
    CrushWithheld,
    NamedCrush,
    named_board_crush,
)
from analysis.origins.assumptions import AssumptionMiss, AssumptionSet
from analysis.origins.domain import (
    CRUSH_COST_COMPONENTS,
    USD,
    USD_PER_MT,
    Blocker,
    Confidence,
    CostComponent,
    Money,
    QuoteKind,
    ShipmentWindow,
    SourceRef,
    WaterfallStep,
    usd_mt,
    worst_confidence,
)
from analysis.origins.landed_cost import COMPONENT_LABELS, DAYS_PER_YEAR
from analysis.origins.sources import fx_on, parse_magyp_window, to_usd_per_mt
from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT

log = logging.getLogger(__name__)

LOOKBACK_DAYS = 120

# The one numeric home for crush yields in this repo. Imported rather than
# restated: M7 #149 established that one crush engine serves every market and
# only the yields, FX and the kind-label vary, and a second copy of 0.183 is a
# second answer waiting to drift.
YIELD_SETS: dict[str, dict[str, float]] = {
    "soy_board": {"oil": CRUSH_OIL_YIELD_MT, "meal": CRUSH_MEAL_YIELD_MT},
}


# Labels and meanings live on the enum (`analysis.futures.crush`), which is the
# one place the four levels are described. These two mappings are kept as the
# derived views every existing call site already reads.
LEVEL_LABELS = {level: level.label for level in CrushLevel}
LEVEL_MEANINGS = {level: level.meaning for level in CrushLevel}

#: What a market's crush legs structurally are, keyed by the registry's own
#: `contracts` string. Rejected rather than defaulted: a descriptor that
#: forgot to say would silently inherit whichever basis the default happened to
#: be, and the whole point of the field is that "named" and "continuous" are
#: not interchangeable.
CONTRACT_BASIS_BY_DESCRIPTOR: dict[str, ContractBasis] = {
    "named": ContractBasis.NAMED_CONTRACT,
    "continuous": ContractBasis.CONTINUOUS,
    "physical": ContractBasis.PHYSICAL,
    "administered": ContractBasis.ADMINISTERED,
}


@dataclass(frozen=True)
class CrushLeg:
    name: str                 # bean | oil | meal
    key: str
    price: Money
    native_price: float
    native_unit: str
    quote_kind: QuoteKind
    source: SourceRef
    #: Set only where the leg *is* a listed contract. ``None`` on a continuous,
    #: physical or administered leg — and the difference is the whole point:
    #: an anonymous leg must not be rendered in the same shape as a named one.
    contract_symbol: str | None = None
    contract_month: str | None = None      # YYYY-MM
    observation_date: date | None = None
    price_type: str | None = None
    provider: str | None = None
    settlement_proven: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "key": self.key,
            "usd_mt": self.price.amount,
            "native_price": self.native_price,
            "native_unit": self.native_unit,
            "quote_kind": self.quote_kind.value,
            "source": self.source.to_dict(),
            "contract_symbol": self.contract_symbol,
            "contract_month": self.contract_month,
            "observation_date": (
                self.observation_date.isoformat() if self.observation_date else None
            ),
            "price_type": self.price_type,
            "provider": self.provider,
            "settlement_proven": self.settlement_proven,
        }


@dataclass(frozen=True)
class CrushResult:
    """One market's margin at one level, or a named reason there is none."""

    market: str
    level: CrushLevel
    label: str
    meaning: str
    legs: tuple[CrushLeg, ...]
    yields: dict[str, float]
    revenue: Money | None
    bean_cost: Money | None
    margin: Money | None
    as_of: date | None
    confidence: Confidence
    # The shipment band all three legs were quoted for, where the source
    # publishes one. ``None`` means the source quotes a single prompt level —
    # not that the window was ignored.
    shipment_window: str | None = None
    steps: tuple[WaterfallStep, ...] = ()
    blockers: tuple[Blocker, ...] = ()
    missing_inputs: tuple[str, ...] = ()
    note: str | None = None
    #: What the legs structurally are. A board margin off continuous
    #: main-contract series and one off named contracts are different claims
    #: and are never rendered alike.
    contract_basis: ContractBasis | None = None
    #: The crush period, where the legs are named contracts.
    period_label: str | None = None
    #: Lines that reproduce the printed margin from the legs.
    workings: tuple[str, ...] = ()
    #: The withheld code when a *named* calculation refused, so a surface can
    #: distinguish "we hold no source" from "the legs did not share a session".
    withheld_code: str | None = None

    @property
    def hedgeable(self) -> bool:
        """Whether the number refers to instruments an order can be entered against."""
        return bool(
            self.contract_basis is not None
            and self.contract_basis.is_hedgeable
            and self.margin is not None
        )

    def __post_init__(self) -> None:
        if self.margin is not None and self.blockers:
            raise ValueError(
                "a crush margin cannot be both computed and blocked — a number "
                "published beside its own blockers is the number that gets used"
            )

    @property
    def is_ok(self) -> bool:
        return self.margin is not None

    @property
    def margin_usd_mt(self) -> float | None:
        return self.margin.amount if self.margin else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "market": self.market,
            "level": self.level.value,
            "label": self.label,
            "meaning": self.meaning,
            "legs": [leg.to_dict() for leg in self.legs],
            "yields": dict(self.yields),
            "revenue_usd_mt": self.revenue.amount if self.revenue else None,
            "bean_cost_usd_mt": self.bean_cost.amount if self.bean_cost else None,
            "margin_usd_mt": self.margin_usd_mt,
            "as_of": self.as_of.isoformat() if self.as_of else None,
            "shipment_window": self.shipment_window,
            "confidence": self.confidence.value,
            "steps": [step.to_dict() for step in self.steps],
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "missing_inputs": list(self.missing_inputs),
            "note": self.note,
            "contract_basis": self.contract_basis.value if self.contract_basis else None,
            "hedgeable": self.hedgeable,
            "period_label": self.period_label,
            "workings": list(self.workings),
            "withheld_code": self.withheld_code,
        }


def _blocked(
    market: str,
    level: CrushLevel,
    reason: str,
    *,
    remedy: str | None = None,
    missing: tuple[str, ...] = (),
    code: str = "missing_leg",
    contract_basis: ContractBasis | None = None,
    withheld_code: str | None = None,
) -> CrushResult:
    return CrushResult(
        market=market,
        level=level,
        label=LEVEL_LABELS[level],
        meaning=LEVEL_MEANINGS[level],
        legs=(),
        yields={},
        revenue=None,
        bean_cost=None,
        margin=None,
        as_of=None,
        confidence=Confidence.UNAVAILABLE,
        blockers=(Blocker(code=code, message=reason, remedy=remedy),),
        missing_inputs=missing,
        contract_basis=contract_basis,
        withheld_code=withheld_code,
    )


# ---------------------------------------------------------------------------
# Leg reads
# ---------------------------------------------------------------------------
def _read_legs(
    conn,
    descriptor: dict,
    market_slug: str,
    *,
    today: date,
) -> tuple[date, dict[str, CrushLeg], str | None] | str:
    """Three legs on one shared session — and one shared shipment window.

    The session rule is the familiar one: a margin struck across two days is
    not a margin. The *window* rule is its twin, and it only shows up on a
    source that publishes a forward curve.

    MAGyP's circular quotes each product for several shipment bands at once —
    beans August / September-October / November-onward, oil in four bands.
    Averaging a product across its bands and then striking a crush produces a
    margin for a cargo nobody can ship: on 2026-08-11 the mean-of-all-bands
    margin was 21.2 USD/MT while the prompt-August margin, the one an actual
    August cargo earns, was 24.1. Same date, same source, three dollars apart,
    and nothing in the number says which it is. So where the descriptor names
    window columns, the legs are grouped by window and the crush is struck on
    a band all three products quote.
    """
    table = descriptor["table"]
    date_column = descriptor["date_column"]
    key_column = descriptor["key_column"]
    value_column = descriptor["value_column"]
    unit = descriptor["unit"]
    legs = descriptor["legs"]
    window_columns = tuple(descriptor.get("window_columns") or ())
    cutoff = (today - timedelta(days=LOOKBACK_DAYS)).isoformat()

    grouping = ", ".join([key_column, date_column, *window_columns])
    sql = (
        f"SELECT {grouping}, AVG({value_column}) "  # noqa: S608 - registry identifiers
        f"FROM {table} WHERE {key_column} IN ({','.join('?' for _ in legs)}) "
        f"AND {date_column} >= ? AND {value_column} IS NOT NULL "
        f"GROUP BY {grouping}"
    )
    try:
        rows = conn.execute(sql, (*legs.values(), cutoff)).fetchall()
    except sqlite3.Error as exc:
        return f"{table} is unreadable ({exc})"
    if not rows:
        return f"{table} holds no rows for {sorted(legs.values())} in the last {LOOKBACK_DAYS} days"

    # key -> day -> [(window|None, value)]
    by_key: dict[str, dict[str, list[tuple[ShipmentWindow | None, float]]]] = {}
    for row in rows:
        key, raw_date = str(row[0]), str(row[1])[:10]
        window = (
            parse_magyp_window(row[2], row[3] if len(window_columns) > 1 else None)
            if window_columns else None
        )
        by_key.setdefault(key, {}).setdefault(raw_date, []).append((window, float(row[-1])))

    missing = [name for name, key in legs.items() if key not in by_key]
    if missing:
        return (
            f"no rows for the {', '.join(sorted(missing))} leg"
            f"{'s' if len(missing) > 1 else ''} ({', '.join(legs[name] for name in sorted(missing))})"
        )

    common_days = set.intersection(*(set(days) for days in by_key.values()))
    if not common_days:
        return (
            "the bean, oil and meal legs share no session — a margin struck across "
            "different days is not a margin"
        )
    when = max(common_days)
    observed = date.fromisoformat(when)

    # Bands are matched by OVERLAP, never by label. MAGyP publishes the bean for
    # 2026-08 and the oil for 2026-08..2026-09 on the same circular: those are
    # the same August cargo, and string equality rejects the pair while quietly
    # falling through to an older session where the labels happen to line up.
    # The bean band leads — a plant buys beans for a crush period and sells the
    # products against it — and each product takes the band overlapping it most.
    day_rows = {name: by_key[key][when] for name, key in legs.items()}
    window_label: str | None = None
    if window_columns:
        bean_dated = sorted(
            ((window, value) for window, value in day_rows["bean"] if window is not None),
            key=lambda pair: pair[0].start,
        )
        if not bean_dated:
            return "the bean leg carries no readable shipment band on its newest session"
        chosen: dict[str, float] | None = None
        for bean_window, bean_value in bean_dated:
            picks = {"bean": bean_value}
            for name in ("oil", "meal"):
                overlapping = [
                    (window.overlap_days(bean_window), value)
                    for window, value in day_rows[name]
                    if window is not None and window.overlaps(bean_window)
                ]
                if overlapping:
                    picks[name] = max(overlapping)[1]
            if len(picks) == 3:
                chosen = picks
                window_label = bean_window.describe()
                break
        if chosen is None:
            return (
                "no shipment band on this session is quoted for all three products — "
                "this source publishes its own set of bands per product, and a margin "
                "combining two that do not overlap is a margin for a cargo nobody can ship"
            )
        values = chosen
    else:
        values = {name: rows_for_day[0][1] for name, rows_for_day in day_rows.items()}

    fx = fx_on(conn, config.MARKETS[market_slug].get("currency_pair"), observed)
    quote_kind = QuoteKind(descriptor.get("quote_kind", "board"))
    built: dict[str, CrushLeg] = {}
    for name, key in legs.items():
        native = values[name]
        usd = to_usd_per_mt(native, unit=unit, key=key, fx=fx.usd_per_unit if fx else None)
        if usd is None:
            return (
                f"no FX rate for {when} — the {name} leg quotes in "
                f"{config.MARKETS[market_slug]['home_currency']} and cannot be stated in USD/MT"
            )
        built[name] = CrushLeg(
            name=name,
            key=key,
            price=usd_mt(usd),
            native_price=native,
            native_unit=unit,
            quote_kind=quote_kind,
            source=SourceRef(
                layer=descriptor.get("layer", table),
                table=table,
                key=key,
                href=f"markets/{market_slug}.html",
            ),
        )
    return observed, built, (window_label or None)


def _margin(legs: dict[str, CrushLeg], yields: dict[str, float]) -> tuple[Money, Money, Money]:
    revenue = legs["oil"].price.scaled(yields["oil"]) + legs["meal"].price.scaled(yields["meal"])
    bean_cost = legs["bean"].price
    return revenue, bean_cost, revenue - bean_cost


# ---------------------------------------------------------------------------
# Level 1 — board
# ---------------------------------------------------------------------------
def board_crush(conn, market_slug: str, *, today: date) -> CrushResult:
    """The board level for one market — named contracts wherever they exist.

    Two paths, and which one a market takes is registry data (``contracts``),
    not a code path here. ``named`` markets are struck by
    ``analysis.futures.crush`` on explicit delivery months and are withheld
    with a reason when those cannot be had coherently. ``continuous`` markets
    (Dalian's main-contract series) keep the generic engine and are stamped
    :attr:`ContractBasis.CONTINUOUS`, which is what stops a surface rendering
    them as something an order can be placed against.
    """
    market = config.MARKETS.get(market_slug)
    if market is None:
        raise KeyError(f"unknown market {market_slug!r}")
    descriptor = market.get("crush")
    if descriptor is None:
        return _blocked(
            market_slug,
            CrushLevel.BOARD_REFERENCE,
            market.get("crush_absent_reason", f"no crush descriptor for {market_slug}"),
            code="no_source",
        )
    if descriptor.get("kind") != "board":
        return _blocked(
            market_slug,
            CrushLevel.BOARD_REFERENCE,
            (
                f"{market_slug}'s crush legs are {descriptor.get('kind')}, not board "
                "closes — there is no exchange here to take a paper margin against"
            ),
            code="no_source",
            contract_basis=_contract_basis(descriptor),
        )
    basis = _contract_basis(descriptor)
    if basis is ContractBasis.NAMED_CONTRACT:
        return _named_board_crush(conn, market_slug, descriptor, today=today)
    return _build(
        conn,
        market_slug,
        {**descriptor, "quote_kind": "board"},
        CrushLevel.BOARD_REFERENCE,
        today=today,
        contract_basis=basis,
    )


def _contract_basis(descriptor: dict) -> ContractBasis:
    """What a descriptor's legs structurally are. Hard-fails on an unknown value."""
    declared = descriptor.get("contracts")
    if declared is None:
        raise KeyError(
            "a crush descriptor must declare `contracts` (one of "
            f"{sorted(CONTRACT_BASIS_BY_DESCRIPTOR)}) — without it a continuous "
            "main-contract series is indistinguishable from a named contract"
        )
    try:
        return CONTRACT_BASIS_BY_DESCRIPTOR[declared]
    except KeyError as exc:
        raise KeyError(
            f"unknown crush contract basis {declared!r}; known: "
            f"{sorted(CONTRACT_BASIS_BY_DESCRIPTOR)}"
        ) from exc


def _named_board_crush(conn, market_slug: str, descriptor: dict, *, today: date) -> CrushResult:
    """Adapt the named-contract calculation into this module's result shape.

    A thin adapter on purpose: the arithmetic, the month convention and every
    refusal live in ``analysis.futures.crush``, so the Origins page, the
    Workstation, the Opportunity board and the briefing are reading one
    calculation rather than four re-derivations of it.
    """
    if conn is None:
        return _blocked(
            market_slug, CrushLevel.BOARD_REFERENCE, "no database connection",
            code="no_source", contract_basis=ContractBasis.NAMED_CONTRACT,
        )
    from analysis.futures.providers import open_provider

    outcome = named_board_crush(open_provider(conn), as_of=today)
    if isinstance(outcome, CrushWithheld):
        return _blocked(
            market_slug,
            CrushLevel.BOARD_REFERENCE,
            outcome.reason,
            remedy=outcome.remedy,
            code=outcome.code.value,
            contract_basis=ContractBasis.NAMED_CONTRACT,
            withheld_code=outcome.code.value,
        )
    return _from_named(market_slug, outcome, descriptor)


def _from_named(market_slug: str, crush: NamedCrush, descriptor: dict) -> CrushResult:
    from analysis.origins.domain import CONFIDENCE_BY_QUOTE_KIND

    quote_kind = QuoteKind(descriptor.get("quote_kind", "board"))
    layer = descriptor.get("layer", "forward_curve")
    legs = tuple(
        CrushLeg(
            name=leg.role,
            key=leg.symbol,
            price=usd_mt(leg.usd_per_mt),
            native_price=leg.native_price,
            native_unit=leg.native_unit,
            quote_kind=quote_kind,
            source=SourceRef(
                layer=layer,
                table="forward_curve",
                key=leg.symbol,
                href="workstation.html",
            ),
            contract_symbol=leg.symbol,
            contract_month=leg.contract_month,
            observation_date=leg.observation_date,
            price_type=leg.price_type.value,
            provider=leg.provider.key,
            settlement_proven=leg.settlement_proven,
        )
        for leg in crush.legs
    )
    return CrushResult(
        market=market_slug,
        level=crush.level,
        label=crush.label,
        meaning=crush.meaning,
        legs=legs,
        yields=dict(crush.yields),
        revenue=usd_mt(crush.revenue_usd_mt),
        bean_cost=usd_mt(crush.bean_cost_usd_mt),
        margin=usd_mt(crush.margin_usd_mt),
        as_of=crush.observation_date,
        confidence=CONFIDENCE_BY_QUOTE_KIND[quote_kind],
        contract_basis=crush.contract_basis,
        period_label=crush.period_label,
        workings=crush.workings(),
        note=CRUSH_CONVENTION_NOTE,
    )


# ---------------------------------------------------------------------------
# Level 2 — gross physical
# ---------------------------------------------------------------------------
def gross_physical_crush(conn, market_slug: str, *, today: date) -> CrushResult:
    descriptor = config.PHYSICAL_CRUSH.get(market_slug)
    if descriptor is None:
        return _blocked(
            market_slug,
            CrushLevel.GROSS_PHYSICAL,
            f"no physical crush legs are declared for {market_slug}",
            code="no_source",
        )
    if descriptor.get("absent_reason"):
        return _blocked(
            market_slug,
            CrushLevel.GROSS_PHYSICAL,
            descriptor["absent_reason"],
            missing=tuple(descriptor.get("missing_legs", ())),
            code="no_source",
        )
    result = _build(
        conn, market_slug, descriptor, CrushLevel.GROSS_PHYSICAL,
        today=today, contract_basis=ContractBasis.PHYSICAL,
    )
    if result.is_ok and descriptor.get("note"):
        from dataclasses import replace as _replace

        return _replace(result, note=descriptor["note"])
    return result


def _build(
    conn,
    market_slug: str,
    descriptor: dict,
    level: CrushLevel,
    *,
    today: date,
    contract_basis: ContractBasis | None = None,
) -> CrushResult:
    if conn is None:
        return _blocked(
            market_slug, level, "no database connection",
            code="no_source", contract_basis=contract_basis,
        )
    read = _read_legs(conn, descriptor, market_slug, today=today)
    if isinstance(read, str):
        return _blocked(market_slug, level, read, contract_basis=contract_basis)
    observed, legs, window_label = read
    yields = YIELD_SETS[descriptor.get("yield_set", "soy_board")]
    revenue, bean_cost, margin = _margin(legs, yields)
    quote_kind = QuoteKind(descriptor.get("quote_kind", "board"))
    from analysis.origins.domain import CONFIDENCE_BY_QUOTE_KIND

    return CrushResult(
        market=market_slug,
        level=level,
        label=LEVEL_LABELS[level],
        meaning=LEVEL_MEANINGS[level],
        legs=tuple(legs[name] for name in ("bean", "oil", "meal")),
        yields=dict(yields),
        revenue=revenue,
        bean_cost=bean_cost,
        margin=margin,
        as_of=observed,
        shipment_window=window_label,
        confidence=CONFIDENCE_BY_QUOTE_KIND[quote_kind],
        contract_basis=contract_basis,
        note=(
            "Legs are continuous main-contract series: the underlying contract changes on "
            "the provider's own schedule and is not published, so this margin names no "
            "contract and is not one an order can be placed against."
            if contract_basis is ContractBasis.CONTINUOUS else None
        ),
    )


# ---------------------------------------------------------------------------
# Level 3 — estimated net plant margin
# ---------------------------------------------------------------------------
def net_plant_margin(
    conn,
    market_slug: str,
    assumptions: AssumptionSet,
    *,
    today: date,
    window: ShipmentWindow | None = None,
) -> CrushResult:
    """Gross physical less the cost of actually running the plant.

    Built on the gross physical margin rather than the board one on purpose: a
    "net" margin computed off futures would net plant costs against a paper
    margin, which is two errors compounding in the same direction.

    Every cost rung is an entered assumption with an owner and an expiry, and a
    missing one blocks the figure. A plant margin with a silently zero energy
    line is the most confidently wrong number this module could produce.
    """
    gross = gross_physical_crush(conn, market_slug, today=today)
    if not gross.is_ok:
        return CrushResult(
            market=market_slug,
            level=CrushLevel.NET_PLANT,
            label=LEVEL_LABELS[CrushLevel.NET_PLANT],
            meaning=LEVEL_MEANINGS[CrushLevel.NET_PLANT],
            legs=gross.legs,
            yields=gross.yields,
            revenue=None,
            bean_cost=None,
            margin=None,
            as_of=gross.as_of,
            confidence=Confidence.UNAVAILABLE,
            blockers=(
                Blocker(
                    code="no_gross_physical",
                    message=(
                        "there is no gross physical crush to net plant costs against — "
                        + gross.blockers[0].message if gross.blockers else "no physical legs"
                    ),
                    remedy="ingest the missing cash legs, or read the board crush instead",
                ),
            ),
            missing_inputs=gross.missing_inputs,
            contract_basis=ContractBasis.PHYSICAL,
        )

    window = window or ShipmentWindow(today, today, label="spot")
    steps: list[WaterfallStep] = []
    blockers: list[Blocker] = []
    missing: list[str] = []
    confidences: list[Confidence] = [gross.confidence]
    running = Money(gross.margin.amount, USD, USD_PER_MT)  # type: ignore[union-attr]

    steps.append(
        WaterfallStep(
            component=CostComponent.ORIGIN_PRICE,
            label="Gross physical crush",
            amount=running,
            running_total=running,
            basis=(
                f"oil {gross.legs[1].price.amount:,.2f} x {gross.yields['oil']:.4f} + "
                f"meal {gross.legs[2].price.amount:,.2f} x {gross.yields['meal']:.4f} − "
                f"bean {gross.legs[0].price.amount:,.2f}, all on "
                f"{gross.as_of.isoformat() if gross.as_of else 'n/a'}"
            ),
            confidence=gross.confidence,
        )
    )

    for component in CRUSH_COST_COMPONENTS:
        found = assumptions.lookup(
            component,
            origin=market_slug,
            destination=None,
            window=window,
            on=today,
        )
        if isinstance(found, AssumptionMiss):
            missing.append(component.value)
            blockers.append(
                Blocker(
                    code=found.code,
                    message=found.reason,
                    remedy=(
                        f"enter a {component.value} assumption for {market_slug} "
                        f"(python scripts/enter_assumption.py --component {component.value} "
                        f"--origin {market_slug} ...)"
                    ),
                    component=component,
                )
            )
            continue
        if component is CostComponent.WORKING_CAPITAL:
            # Charged on the bean the plant is carrying, not on the margin.
            base = gross.bean_cost or usd_mt(0.0)
            amount = base.scaled(found.value * (found.days or 0) / DAYS_PER_YEAR)
            basis = (
                f"{found.value * 100:.4g}%/yr x {found.days}d / 365 on the bean cost "
                f"{base.amount:,.2f} USD/MT"
            )
            rate: float | None = found.value
        else:
            amount = usd_mt(found.value)
            basis = f"{found.value:,.2f} USD/MT of beans crushed, flat"
            rate = None
        running = running - amount
        steps.append(
            WaterfallStep(
                component=component,
                label=COMPONENT_LABELS[component],
                amount=amount.scaled(-1.0),
                running_total=running,
                basis=basis,
                confidence=found.confidence,
                assumption_id=found.id,
                entered_by=found.entered_by,
                entered_at=found.entered_at,
                expires_on=found.expires_on,
                rate=rate,
            )
        )
        confidences.append(found.confidence)

    return CrushResult(
        market=market_slug,
        level=CrushLevel.NET_PLANT,
        label=LEVEL_LABELS[CrushLevel.NET_PLANT],
        meaning=LEVEL_MEANINGS[CrushLevel.NET_PLANT],
        legs=gross.legs,
        yields=gross.yields,
        revenue=gross.revenue,
        bean_cost=gross.bean_cost,
        margin=None if blockers else running,
        as_of=gross.as_of,
        confidence=Confidence.UNAVAILABLE if blockers else worst_confidence(*confidences),
        steps=tuple(steps),
        blockers=tuple(blockers),
        missing_inputs=tuple(missing),
        contract_basis=ContractBasis.PHYSICAL,
    )


def crush_stack(
    conn,
    market_slug: str,
    assumptions: AssumptionSet,
    *,
    today: date,
) -> tuple[CrushResult, CrushResult, CrushResult]:
    """All three levels for one market, in the order they should be read."""
    return (
        board_crush(conn, market_slug, today=today),
        gross_physical_crush(conn, market_slug, today=today),
        net_plant_margin(conn, market_slug, assumptions, today=today),
    )


def yields_with_override(oil: float | None = None, meal: float | None = None) -> dict[str, float]:
    """The soy yield set with either co-product yield replaced — for scenarios.

    Crush yields are a real sensitivity: an extraction plant running 18.6% oil
    against a nominal 18.3% earns a different margin at identical prices, and a
    trader comparing two plants is comparing yields as much as prices.
    """
    base = dict(YIELD_SETS["soy_board"])
    if oil is not None:
        base["oil"] = oil
    if meal is not None:
        base["meal"] = meal
    return base


def margin_at_yields(result: CrushResult, yields: dict[str, float]) -> float | None:
    """Re-strike a computed margin at different co-product yields."""
    if not result.legs or len(result.legs) != 3:
        return None
    legs = {leg.name: leg for leg in result.legs}
    revenue, _, margin = _margin(legs, yields)
    del revenue
    return margin.amount


__all__ = [
    "CONTRACT_BASIS_BY_DESCRIPTOR",
    "LEVEL_LABELS",
    "LEVEL_MEANINGS",
    "YIELD_SETS",
    "ContractBasis",
    "CrushLeg",
    "CrushLevel",
    "CrushResult",
    "board_crush",
    "crush_stack",
    "gross_physical_crush",
    "margin_at_yields",
    "net_plant_margin",
    "yields_with_override",
]
