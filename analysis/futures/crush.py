"""The crush, struck on contracts that have names.

What this replaces
------------------
Until now every "board crush" on this site was three *provider front-month*
series — Yahoo's ``ZS=F``, ``ZM=F`` and ``ZL=F``, stored in ``prices`` under a
commodity name and no contract column at all. Three problems ride on that, and
they are not cosmetic:

* **It names no contract, so it cannot be reproduced.** "Board crush 99.74" is
  not checkable by anyone. Which September? Which session? The number carries
  no answer.
* **The underlying contract changes silently.** Yahoo rolls each of the three
  legs on its own schedule, unannounced and unadjusted. The legs need not roll
  on the same day, so a roll-day crush prints a move that no crusher earned —
  the same artifact ``analysis.signals.is_near_roll`` exists to demote, applied
  to a spread where it does *not* cancel.
* **It cannot be hedged, and read as though it could.** A crusher acting on a
  board crush places three orders in three named months. A stitched series has
  no month to place them in.

So the calculation here takes named contracts or it takes nothing. Every leg
carries its symbol, its delivery month, the session it was observed on, what
kind of price it is, which provider said so, and whether that provider proves a
settlement. Where those cannot be had coherently, the result is a
:class:`CrushWithheld` with the reason — never a number computed from whatever
was to hand.

The four levels
---------------
:class:`CrushLevel` is one closed vocabulary for the whole stack, because the
four things people call "the crush" are four different numbers and the gap
between them is where a plant's money is:

``board_reference``   named contracts, delayed closes — a paper margin, roughly
                      what a hedge locks, and not proven placeable against.
``board_settlement``  the same arithmetic on proven exchange settlements. Not
                      constructible today and that is the finding, not a gap:
                      ``pricing.semantics.PROVEN_SETTLEMENT_SOURCES`` is empty.
``gross_physical``    cash bean against cash oil and meal, one place, one day.
``net_plant``         gross physical less the cost of running the plant.

The two board levels are computed here. The two physical levels are computed in
``analysis.origins.crush``, which imports this enum rather than defining a
second one — the disagreement between four private vocabularies is exactly what
``pricing.semantics`` was written to end.

The month convention
--------------------
A crush is struck on one delivery period, so the three legs must belong to one.
ZS lists Jan/Mar/May/Jul/Aug/Sep/Nov; ZM and ZL list
Jan/Mar/May/Jul/Aug/Sep/Oct/Dec. Six of the seven bean months therefore pair
with the products' own same month, and November — which the products do not
list — pairs with December, the first listed product month after it. That is
this project's encoded convention (:data:`SOY_CRUSH_PRODUCT_MONTH`), derived
from the listed-month sets rather than typed out, and pinned against the
literal table in the tests. It is stated on every surface that renders the
result, because a reader who assumes same-month everywhere would mis-read the
November crush by a delivery period.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import TYPE_CHECKING, Any

from analysis.futures.domain import (
    METHOD_VERSION,
    ContractQuote,
    NamedContract,
    PriceType,
    Provider,
    contracts_from,
    named_contract,
    trading_months,
)
from pricing.policy import require_hedgeable

if TYPE_CHECKING:  # pragma: no cover - typing only
    from analysis.futures.domain import ContinuousSeries
    from analysis.futures.providers import CurveObservation, QuoteProvider

log = logging.getLogger(__name__)

#: Sessions a contract must still have left to be the month a crush is struck
#: in. Same default as ``hedge.select_hedge_month``: a margin quoted on a
#: contract with two days to run is not a margin anybody can act on.
MIN_DAYS_TO_EXPIRY = 5

#: How many bean months to walk before giving up. Six covers a full listing
#: cycle plus a spare, so a genuinely missing product leg is reported as one
#: rather than as an unlucky month.
DEFAULT_CANDIDATE_MONTHS = 6


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------
class ContractBasis(str, Enum):
    """What the legs of a margin are, structurally.

    The line this module draws. ``NAMED_CONTRACT`` legs can be traded;
    ``CONTINUOUS`` legs are a research artifact whose underlying instrument
    changes without notice and can never be hedged, whatever their price says.
    Carried as a field rather than as documentation so a surface cannot render
    the two identically.
    """

    NAMED_CONTRACT = "named_contract"
    CONTINUOUS = "continuous"
    PHYSICAL = "physical"
    ADMINISTERED = "administered"

    @property
    def is_hedgeable(self) -> bool:
        return self is ContractBasis.NAMED_CONTRACT


class CrushLevel(str, Enum):
    """The four numbers people call "the crush". Closed, and closed on purpose."""

    BOARD_REFERENCE = "board_reference"
    BOARD_SETTLEMENT = "board_settlement"
    GROSS_PHYSICAL = "gross_physical"
    NET_PLANT = "net_plant"

    @property
    def is_board(self) -> bool:
        return self in (CrushLevel.BOARD_REFERENCE, CrushLevel.BOARD_SETTLEMENT)

    @property
    def label(self) -> str:
        return _LEVEL_LABELS[self]

    @property
    def meaning(self) -> str:
        return _LEVEL_MEANINGS[self]


_LEVEL_LABELS: dict[CrushLevel, str] = {
    CrushLevel.BOARD_REFERENCE: "Board crush — delayed-close reference",
    CrushLevel.BOARD_SETTLEMENT: "Board crush — official settlements",
    CrushLevel.GROSS_PHYSICAL: "Gross physical crush",
    CrushLevel.NET_PLANT: "Estimated net plant margin",
}

_LEVEL_MEANINGS: dict[CrushLevel, str] = {
    CrushLevel.BOARD_REFERENCE: (
        "Three named contracts of one crush period, priced off delayed daily bars from a "
        "consumer endpoint. Roughly what a hedge locks. It is not an exchange settlement — "
        "no provider here proves one — and it is not what any plant earns, because no plant "
        "buys the board."
    ),
    CrushLevel.BOARD_SETTLEMENT: (
        "The same three named contracts priced off official exchange settlements proven by "
        "the provider. Nothing in this stack can currently produce this level; it exists so "
        "that substituting an authoritative feed changes the claim rather than the code."
    ),
    CrushLevel.GROSS_PHYSICAL: (
        "Cash bean against cash oil and meal at one location on one day. Captures the basis "
        "the board misses; still contains no cost of running a plant."
    ),
    CrushLevel.NET_PLANT: (
        "Gross physical less freight to plant, processing, energy and working capital. Every "
        "one of those is hand-entered — this figure is an estimate in the strong sense."
    ),
}


class WithheldReason(str, Enum):
    """Why there is no crush. Every one of these is a sentence on a page."""

    NO_CURVE = "no_curve"
    NO_CRUSH_MONTH = "no_crush_month"
    MIXED_SESSIONS = "mixed_sessions"
    MIXED_PRICE_TYPES = "mixed_price_types"
    MIXED_PROVIDERS = "mixed_providers"
    UNSUPPORTED_PRICE_TYPE = "unsupported_price_type"
    SETTLEMENT_UNPROVEN = "settlement_unproven"
    CONTINUOUS_SERIES = "continuous_series"
    EXPIRY_NOT_ENCODED = "expiry_not_encoded"


#: The price types a board crush may be struck on. A hand-entered number and a
#: physical assessment are both legitimate observations and neither is a board
#: print; three of them agreeing with each other does not make one.
_BOARD_PRICE_TYPES = frozenset({PriceType.DELAYED_CLOSE, PriceType.SETTLEMENT})


@dataclass(frozen=True)
class CrushSet:
    """Which three products make one crush. One good, three legs, in order."""

    bean: str
    meal: str
    oil: str

    @property
    def commodities(self) -> tuple[str, str, str]:
        return (self.bean, self.meal, self.oil)


SOY_CRUSH_SET = CrushSet(bean="Soybeans", meal="Soybean Meal", oil="Soybean Oil")


# ---------------------------------------------------------------------------
# The month convention
# ---------------------------------------------------------------------------
def _product_month_set() -> tuple[int, ...]:
    """The months *both* product legs list. Read from config, never restated."""
    meal = set(trading_months(SOY_CRUSH_SET.meal))
    oil = set(trading_months(SOY_CRUSH_SET.oil))
    return tuple(sorted(meal & oil))


def _derive_product_month_map() -> dict[int, int]:
    """Bean month -> product month: same month if listed, else the next listed one.

    Derived rather than typed out so a listing change upstream cannot leave a
    stale literal behind. The literal it currently produces is pinned in the
    tests, which is where a surprising change becomes visible.
    """
    products = _product_month_set()
    out: dict[int, int] = {}
    for bean_month in trading_months(SOY_CRUSH_SET.bean):
        later = [month for month in products if month >= bean_month]
        # Wrapping into the next year is deliberately *not* silent: it would
        # name a contract a delivery cycle away and price it as if it were the
        # bean's own. December is the last listed product month, so this branch
        # is unreachable today and is kept for the calendar that changes it.
        out[bean_month] = later[0] if later else products[0]
    return out


#: Bean delivery month -> the product delivery month it crushes into.
SOY_CRUSH_PRODUCT_MONTH: dict[int, int] = _derive_product_month_map()

#: One sentence, rendered wherever the result is. A reader who assumes
#: same-month everywhere mis-reads the November crush by a delivery period.
CRUSH_CONVENTION_NOTE = (
    "Crush months: beans against the products' own delivery month, except November "
    "beans, which the products do not list — those crush into December."
)


def product_month_for(year: int, bean_month: int) -> tuple[int, int]:
    """(year, month) of the product contracts a given bean month crushes into."""
    try:
        product_month = SOY_CRUSH_PRODUCT_MONTH[bean_month]
    except KeyError as exc:
        raise KeyError(
            f"month {bean_month} is not a listed soybean delivery month "
            f"({sorted(SOY_CRUSH_PRODUCT_MONTH)})"
        ) from exc
    return (year + 1, product_month) if product_month < bean_month else (year, product_month)


@dataclass(frozen=True)
class CrushContracts:
    """One crush period, named on all three legs."""

    bean: NamedContract
    meal: NamedContract
    oil: NamedContract

    @property
    def label(self) -> str:
        return f"{self.bean.label} beans / {self.meal.label} products"

    @property
    def symbols(self) -> tuple[str, str, str]:
        return (self.bean.symbol, self.meal.symbol, self.oil.symbol)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label,
            "bean": self.bean.to_dict(),
            "meal": self.meal.to_dict(),
            "oil": self.oil.to_dict(),
            "convention": CRUSH_CONVENTION_NOTE,
        }


def crush_contract_candidates(
    as_of: date, *, count: int = DEFAULT_CANDIDATE_MONTHS, crush_set: CrushSet = SOY_CRUSH_SET
) -> tuple[CrushContracts, ...]:
    """Crush periods still listed at ``as_of``, nearest first."""
    out: list[CrushContracts] = []
    for bean in contracts_from(crush_set.bean, as_of, count=count):
        year, month = product_month_for(bean.year, bean.month)
        out.append(CrushContracts(
            bean=bean,
            meal=named_contract(crush_set.meal, year, month),
            oil=named_contract(crush_set.oil, year, month),
        ))
    return tuple(out)


# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CrushWithheld:
    """No crush, and the reason. Never a partial number.

    Separate type from :class:`NamedCrush` rather than a nullable margin on it,
    for the reason ``analysis.origins.crush.CrushResult.__post_init__`` learned
    the hard way: a number published beside its own blockers is the number that
    gets used.
    """

    code: WithheldReason
    reason: str
    remedy: str | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_ok(self) -> bool:
        return False

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": False,
            "code": self.code.value,
            "reason": self.reason,
            "remedy": self.remedy,
            "detail": dict(self.detail),
            "margin_usd_mt": None,
        }


@dataclass(frozen=True)
class NamedCrushLeg:
    """One leg of the crush, with everything needed to defend it."""

    role: str                      # bean | meal | oil
    quote: ContractQuote
    yield_mt: float | None         # MT of product per MT of beans; None on the bean

    # -- identity ----------------------------------------------------------
    @property
    def contract(self) -> NamedContract:
        return self.quote.contract

    @property
    def symbol(self) -> str:
        return self.contract.symbol

    @property
    def provider_symbol(self) -> str:
        return self.contract.provider_symbol

    @property
    def commodity(self) -> str:
        return self.contract.spec.name

    @property
    def contract_month(self) -> str:
        return self.contract.delivery_month

    # -- provenance --------------------------------------------------------
    @property
    def observation_date(self) -> date:
        return self.quote.observation_date

    @property
    def price_type(self) -> PriceType:
        return self.quote.price_type

    @property
    def provider(self) -> Provider:
        return self.quote.provider

    @property
    def settlement_proven(self) -> bool:
        return self.quote.is_settlement_proven

    # -- numbers -----------------------------------------------------------
    @property
    def native_price(self) -> float:
        return self.quote.price

    @property
    def native_unit(self) -> str:
        return self.contract.spec.native_unit.value

    @property
    def usd_per_mt(self) -> float:
        return self.quote.usd_per_mt

    @property
    def contribution_usd_mt(self) -> float:
        """Signed contribution to the margin: products add, the bean subtracts."""
        if self.yield_mt is None:
            return -self.usd_per_mt
        return self.usd_per_mt * self.yield_mt

    @property
    def days_to_expiry(self) -> int | None:
        return self.contract.days_to_expiry(self.observation_date)

    @property
    def native_display(self) -> str:
        """The native price as printed. Full precision, no significant-figure cut.

        ``%.4g`` rendered 1150.25 as "1,150", which turns a reproduction line
        into an approximation of one — the whole point of the line is that a
        reader can redo the conversion and land on the same USD/MT.
        """
        return f"{self.native_price:,.6f}".rstrip("0").rstrip(".")

    def working(self) -> str:
        """The one line that reproduces this leg's contribution."""
        if self.yield_mt is None:
            return (
                f"bean  {self.symbol} {self.native_display} {self.native_unit} "
                f"= {self.usd_per_mt:,.4f} USD/MT (subtracted)"
            )
        return (
            f"{self.role:<5} {self.symbol} {self.native_display} {self.native_unit} "
            f"= {self.usd_per_mt:,.4f} USD/MT x {self.yield_mt:.6f} MT/MT "
            f"= {self.contribution_usd_mt:,.4f} USD/MT"
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "commodity": self.commodity,
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "contract_month": self.contract_month,
            "contract_label": self.contract.label,
            "observation_date": self.observation_date.isoformat(),
            "price_type": self.price_type.value,
            "price_label": self.quote.price_label,
            "price_caveat": self.price_type.caveat,
            "provider": self.provider.key,
            "provider_display": self.provider.display,
            "settlement_proven": self.settlement_proven,
            "native_price": self.native_price,
            "native_display": self.native_display,
            "native_unit": self.native_unit,
            "usd_per_mt": round(self.usd_per_mt, 6),
            "yield_mt": self.yield_mt,
            "contribution_usd_mt": round(self.contribution_usd_mt, 6),
            "days_to_expiry": self.days_to_expiry,
            "last_trade": self.contract.last_trade.isoformat() if self.contract.last_trade else None,
            "first_notice": (
                self.contract.first_notice.isoformat() if self.contract.first_notice else None
            ),
            "working": self.working(),
        }


@dataclass(frozen=True)
class NamedCrush:
    """A board crush on three named contracts of one crush period, one session."""

    level: CrushLevel
    bean: NamedCrushLeg
    meal: NamedCrushLeg
    oil: NamedCrushLeg
    yields: Mapping[str, float]
    observation_date: date
    as_of: date
    provider: Provider
    contracts: CrushContracts
    curve_notes: tuple[str, ...] = ()
    method_version: str = METHOD_VERSION

    def __post_init__(self) -> None:
        dates = {leg.observation_date for leg in self.legs}
        if len(dates) != 1:
            raise ValueError(
                f"a crush is one session; these legs span {sorted(d.isoformat() for d in dates)}"
            )
        if not self.level.is_board:
            raise ValueError(f"{self.level.value} is not a board level")

    # -- the numbers -------------------------------------------------------
    @property
    def legs(self) -> tuple[NamedCrushLeg, NamedCrushLeg, NamedCrushLeg]:
        return (self.bean, self.meal, self.oil)

    @property
    def revenue_usd_mt(self) -> float:
        return self.meal.contribution_usd_mt + self.oil.contribution_usd_mt

    @property
    def bean_cost_usd_mt(self) -> float:
        return self.bean.usd_per_mt

    @property
    def margin_usd_mt(self) -> float:
        return self.revenue_usd_mt - self.bean_cost_usd_mt

    @property
    def oil_value_share(self) -> float | None:
        """Share of gross product revenue coming from the oil leg."""
        revenue = self.revenue_usd_mt
        return self.oil.contribution_usd_mt / revenue if revenue else None

    # -- the claims --------------------------------------------------------
    @property
    def is_ok(self) -> bool:
        return True

    @property
    def contract_basis(self) -> ContractBasis:
        return ContractBasis.NAMED_CONTRACT

    @property
    def price_type(self) -> PriceType:
        return self.bean.price_type

    @property
    def is_settlement_proven(self) -> bool:
        return all(leg.settlement_proven for leg in self.legs)

    @property
    def is_hedgeable(self) -> bool:
        """Three named, unexpired contracts with published termination rules.

        Not "we recommend placing this" — it is the structural question of
        whether the number refers to instruments an order can be entered
        against at all, which a stitched series can never answer yes to.
        """
        return all(
            leg.contract.last_trade is not None
            and leg.contract.is_expired(self.observation_date) is False
            for leg in self.legs
        )

    @property
    def label(self) -> str:
        return self.level.label

    @property
    def meaning(self) -> str:
        return self.level.meaning

    @property
    def period_label(self) -> str:
        return self.contracts.label

    def workings(self) -> tuple[str, ...]:
        """Every line needed to reproduce the printed margin by hand."""
        return (
            f"{self.label} — {self.period_label}, all legs observed "
            f"{self.observation_date.isoformat()}",
            self.bean.working(),
            self.meal.working(),
            self.oil.working(),
            (
                f"revenue = {self.oil.contribution_usd_mt:,.4f} + "
                f"{self.meal.contribution_usd_mt:,.4f} = {self.revenue_usd_mt:,.4f} USD/MT"
            ),
            (
                f"margin  = {self.revenue_usd_mt:,.4f} - {self.bean_cost_usd_mt:,.4f} "
                f"= {self.margin_usd_mt:,.2f} USD/MT"
            ),
            CRUSH_CONVENTION_NOTE,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": True,
            "level": self.level.value,
            "label": self.label,
            "meaning": self.meaning,
            "contract_basis": self.contract_basis.value,
            "hedgeable": self.is_hedgeable,
            "settlement_proven": self.is_settlement_proven,
            "price_type": self.price_type.value,
            "period_label": self.period_label,
            "convention": CRUSH_CONVENTION_NOTE,
            "observation_date": self.observation_date.isoformat(),
            "as_of": self.as_of.isoformat(),
            "provider": self.provider.key,
            "provider_display": self.provider.display,
            "provider_note": self.provider.note,
            "yields": dict(self.yields),
            "legs": [leg.to_dict() for leg in self.legs],
            "revenue_usd_mt": round(self.revenue_usd_mt, 6),
            "bean_cost_usd_mt": round(self.bean_cost_usd_mt, 6),
            "margin_usd_mt": round(self.margin_usd_mt, 6),
            "oil_value_share": self.oil_value_share,
            "profitable": self.margin_usd_mt > 0,
            "curve_notes": list(self.curve_notes),
            "workings": list(self.workings()),
            "method_version": self.method_version,
        }


CrushOutcome = NamedCrush | CrushWithheld


# ---------------------------------------------------------------------------
# The refusal a continuous series earns
# ---------------------------------------------------------------------------
def continuous_withheld(commodity: str, series: ContinuousSeries | None = None) -> CrushWithheld:
    """The answer when the only data available is a stitched front-month series.

    Called by surfaces that must show a *hedgeable* crush and have nothing but
    ``prices``. There is deliberately no function here that would compute one:
    the previous board crush was exactly that path, and the fix is the absence
    of the path, not a warning label on it.
    """
    method = series.method_description if series is not None else (
        "Provider front-month: the underlying contract changes on the provider's own "
        "schedule, unannounced and unadjusted."
    )
    return CrushWithheld(
        code=WithheldReason.CONTINUOUS_SERIES,
        reason=(
            f"{commodity} is held only as a stitched front-month series, which names no "
            f"contract and cannot be hedged. {method}"
        ),
        remedy=(
            "Ingest the named-contract curve for this product (Layer 11 does this for the "
            "CBOT soy complex) and the crush is struck on it automatically."
        ),
        detail={"commodity": commodity},
    )


# ---------------------------------------------------------------------------
# The calculation
# ---------------------------------------------------------------------------
def _default_yields() -> dict[str, float]:
    """The one numeric home for crush yields in this repo (M7 #149).

    Imported rather than restated: a second copy of 0.183 is a second answer
    waiting to drift, and the hedge calculator sizes its product legs on the
    same two numbers.
    """
    from analysis.spreads import CRUSH_MEAL_YIELD_MT, CRUSH_OIL_YIELD_MT

    return {"oil": CRUSH_OIL_YIELD_MT, "meal": CRUSH_MEAL_YIELD_MT}


def named_board_crush(
    provider: QuoteProvider,
    *,
    as_of: date,
    crush_set: CrushSet = SOY_CRUSH_SET,
    yields: Mapping[str, float] | None = None,
    min_days_to_expiry: int = MIN_DAYS_TO_EXPIRY,
    candidate_months: int = DEFAULT_CANDIDATE_MONTHS,
) -> CrushOutcome:
    """The board crush on named contracts, or the reason there is none.

    The order of the refusals is the order of the questions a trader would ask:
    do we hold a curve at all; is there a crush period all three legs are still
    trading in; did those three legs print on one session; are they the same
    kind of number; and is that kind of number a board print.
    """
    yields = dict(yields) if yields is not None else _default_yields()

    curves: dict[str, CurveObservation] = {
        commodity: provider.curve(commodity, as_of=as_of)
        for commodity in crush_set.commodities
    }
    empty = [commodity for commodity, curve in curves.items() if curve.is_empty]
    if empty:
        return CrushWithheld(
            code=WithheldReason.NO_CURVE,
            reason=(
                "no named-contract curve is stored for "
                + ", ".join(sorted(empty))
                + " — a crush cannot be struck on the legs that are present"
            ),
            remedy="run the pipeline so Layer 11 fetches the forward curve for these products",
            detail={"missing": sorted(empty)},
        )

    chosen: tuple[CrushContracts, dict[str, ContractQuote]] | None = None
    considered: list[str] = []
    for candidate in crush_contract_candidates(
        as_of, count=candidate_months, crush_set=crush_set
    ):
        quotes: dict[str, ContractQuote] = {}
        for role, contract in (
            ("bean", candidate.bean), ("meal", candidate.meal), ("oil", candidate.oil)
        ):
            found = curves[contract.spec.name].leg(contract.symbol)
            if found is not None:
                quotes[role] = found

        missing = [
            contract.symbol
            for role, contract in (
                ("bean", candidate.bean), ("meal", candidate.meal), ("oil", candidate.oil)
            )
            if role not in quotes
        ]
        if missing:
            considered.append(f"{candidate.label}: no stored leg for {', '.join(missing)}")
            continue

        # An unencoded termination rule is a fact about the *product*, not
        # about this month, so it stops the calculation rather than rolling to
        # the next one: without a last trade date there is no roll window, and
        # a month picked without one is picked on nothing.
        unencoded = [
            quote.contract.symbol for quote in quotes.values() if quote.contract.last_trade is None
        ]
        if unencoded:
            return CrushWithheld(
                code=WithheldReason.EXPIRY_NOT_ENCODED,
                reason=(
                    "this project has not encoded a published termination rule for "
                    + ", ".join(sorted(unencoded))
                    + " — with no last trade date there is no roll window, so no crush month "
                    "can be chosen honestly"
                ),
                remedy="encode the product's termination rule in analysis.futures.domain.ExpiryRule",
                detail={"contracts": sorted(unencoded)},
            )

        short = [
            f"{quote.contract.symbol} has {quote.contract.days_to_expiry(as_of)} session(s) left"
            for quote in quotes.values()
            if (quote.contract.days_to_expiry(as_of) or 0) < min_days_to_expiry
        ]
        if short:
            considered.append(f"{candidate.label}: " + "; ".join(short))
            continue

        chosen = (candidate, quotes)
        break

    if chosen is None:
        return CrushWithheld(
            code=WithheldReason.NO_CRUSH_MONTH,
            reason=(
                "no crush period in the next "
                f"{candidate_months} listed bean months has all three legs still trading with "
                f"at least {min_days_to_expiry} sessions left — "
                + "; ".join(considered)
            ),
            remedy=(
                "wait for the next curve fetch, or widen candidate_months if the product "
                "calendar genuinely runs further out"
            ),
            detail={"considered": considered},
        )

    contracts, quotes = chosen

    # Structural, not a data check: whatever a provider hands back, this
    # calculation is defined on named contracts. A provider substitution that
    # returned a stitched series here would otherwise produce an arithmetically
    # fine margin nobody can place.
    for quote in quotes.values():
        require_hedgeable(quote, calculation="named_board_crush")

    sessions = {role: quote.observation_date for role, quote in quotes.items()}
    if len(set(sessions.values())) != 1:
        return CrushWithheld(
            code=WithheldReason.MIXED_SESSIONS,
            reason=(
                "the three legs did not print on one session ("
                + ", ".join(
                    f"{quotes[role].contract.symbol} {sessions[role].isoformat()}"
                    for role in ("bean", "meal", "oil")
                )
                + ") — a margin struck across days is the intervening move, not a margin"
            ),
            remedy="re-fetch the curve so all three products carry the same observation date",
            detail={role: day.isoformat() for role, day in sessions.items()},
        )

    price_types = {role: quote.price_type for role, quote in quotes.items()}
    if len(set(price_types.values())) != 1:
        return CrushWithheld(
            code=WithheldReason.MIXED_PRICE_TYPES,
            reason=(
                "the legs are not the same kind of number ("
                + ", ".join(
                    f"{quotes[role].contract.symbol} {price_types[role].value}"
                    for role in ("bean", "meal", "oil")
                )
                + ") — a margin mixing a board print with another kind of quote states neither"
            ),
            remedy="price all three legs from one provider, or read the physical crush instead",
            detail={role: kind.value for role, kind in price_types.items()},
        )

    providers = {role: quote.provider for role, quote in quotes.items()}
    if len({provider.key for provider in providers.values()}) != 1:
        return CrushWithheld(
            code=WithheldReason.MIXED_PROVIDERS,
            reason=(
                "the legs come from different providers ("
                + ", ".join(
                    f"{quotes[role].contract.symbol} {providers[role].display}"
                    for role in ("bean", "meal", "oil")
                )
                + ") — two provenances inside one number, and the margin would inherit the "
                "weaker without saying which leg carried it"
            ),
            remedy="price all three legs from one provider",
            detail={role: value.key for role, value in providers.items()},
        )

    price_type = price_types["bean"]
    if price_type not in _BOARD_PRICE_TYPES:
        return CrushWithheld(
            code=WithheldReason.UNSUPPORTED_PRICE_TYPE,
            reason=(
                f"a board crush cannot be struck on {price_type.value} quotes — "
                f"{price_type.caveat}"
            ),
            remedy="read the gross physical crush, which is the level these quotes belong to",
            detail={"price_type": price_type.value},
        )

    proven = [quote.is_settlement_proven for quote in quotes.values()]
    if price_type is PriceType.SETTLEMENT and not all(proven):
        unproven = sorted(
            quote.provider.display for quote in quotes.values() if not quote.is_settlement_proven
        )
        return CrushWithheld(
            code=WithheldReason.SETTLEMENT_UNPROVEN,
            reason=(
                "a leg is stamped as a settlement by a provider that does not prove one ("
                + ", ".join(unproven)
                + ") — a settlement is a claim about the provider, not about the number"
            ),
            remedy=(
                "add the provider to pricing.semantics.PROVEN_SETTLEMENT_SOURCES only when its "
                "feed genuinely carries exchange settlements"
            ),
            detail={"providers": unproven},
        )

    level = CrushLevel.BOARD_SETTLEMENT if all(proven) else CrushLevel.BOARD_REFERENCE
    notes = tuple(
        f"{commodity}: {curve.coherence_note}"
        for commodity, curve in curves.items()
        if not curve.coherent and curve.coherence_note
    )

    return NamedCrush(
        level=level,
        bean=NamedCrushLeg(role="bean", quote=quotes["bean"], yield_mt=None),
        meal=NamedCrushLeg(role="meal", quote=quotes["meal"], yield_mt=yields["meal"]),
        oil=NamedCrushLeg(role="oil", quote=quotes["oil"], yield_mt=yields["oil"]),
        yields=yields,
        observation_date=sessions["bean"],
        as_of=as_of,
        provider=quotes["bean"].provider,
        contracts=contracts,
        curve_notes=notes,
    )


__all__ = [
    "CRUSH_CONVENTION_NOTE",
    "DEFAULT_CANDIDATE_MONTHS",
    "MIN_DAYS_TO_EXPIRY",
    "SOY_CRUSH_PRODUCT_MONTH",
    "SOY_CRUSH_SET",
    "ContractBasis",
    "CrushContracts",
    "CrushLevel",
    "CrushOutcome",
    "CrushSet",
    "CrushWithheld",
    "NamedCrush",
    "NamedCrushLeg",
    "WithheldReason",
    "continuous_withheld",
    "crush_contract_candidates",
    "named_board_crush",
    "product_month_for",
]
