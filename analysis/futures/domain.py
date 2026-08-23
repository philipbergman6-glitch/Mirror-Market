"""Named-contract vocabulary for the futures workstation (Phase 3).

Standard library only, exactly like ``analysis/origins/domain.py`` and for the
same reason: persistence belongs in ``providers.py``, presentation in ``app/``.

Phase 3 exists because the stack had no *contract*. ``prices`` stores a
continuous front-month series under the label "Soybeans" with no contract
column at all; ``forward_curve`` stores a Yahoo ticker string and a close.
Neither says which contract a number belongs to, when that contract stops
trading, or whether the number is a settlement. A hedger cannot use either.

Four rules run through every type here.

**A named contract is not a continuous series.** ``NamedContract`` and
``ContinuousSeries`` are different types and neither is accepted where the
other is expected. The front-month series is a research artifact stitched
across expiries; a hedge is placed on ZSX26 and on nothing else. The project
already knows this hurts — ``analysis.signals.is_near_roll`` exists solely to
demote indicators that a silent Yahoo roll may have faked — and the fix is a
type, not another heuristic.

**A price carries what kind of price it is.** ``PriceType.DELAYED_CLOSE`` is
what this stack actually has: a delayed daily bar from a consumer endpoint.
It is *not* proven to be exchange settlement, so nothing here may call it one.
``PriceType.SETTLEMENT`` exists for the day an authoritative provider is
substituted, and :attr:`ContractQuote.is_settlement_proven` is the single
predicate every surface asks.

**Expiry is a published rule or it is absent.** Days-to-expiry drives the roll
alert, the hedge month choice and the carry annualisation, so a guessed last
trade date is worse than none. Rules are encoded per product with an explicit
:class:`ExpiryConfidence`, read off the exchange's own rulebook and cited at
the rule number; where this project has no such rule the product is left
``NOT_ENCODED`` rather than approximated, and every consumer degrades visibly.
All nine products are encoded as of the ICE additions below — the mechanism
stays because the next product added may not be.

**Units never convert implicitly.** A contract carries its native unit and its
size in that unit; the MT equivalent is derived from the same factors
``pipeline/units.py`` publishes, checked against them in the tests.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
from typing import Any, cast

from pricing.semantics import PriceType

# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------


class Exchange(str, Enum):
    """Venues this stack can name a contract on."""

    CBOT = "CBOT"
    CME = "CME"
    ICE_US = "ICE_US"


class NativeUnit(str, Enum):
    """The unit a venue quotes in. Never assumed, always carried."""

    CENTS_PER_BUSHEL = "cents_per_bushel"
    CENTS_PER_POUND = "cents_per_pound"
    USD_PER_SHORT_TON = "usd_per_short_ton"
    USD_PER_MT = "usd_per_mt"


class SizeUnit(str, Enum):
    """The unit a contract's *size* is expressed in."""

    BUSHEL = "bushel"
    POUND = "pound"
    SHORT_TON = "short_ton"
    METRIC_TON = "metric_ton"


# ``PriceType`` is the canonical classification from ``pricing.semantics``,
# imported rather than defined here. This module had it right first — a
# consumer-endpoint daily bar is a ``DELAYED_CLOSE`` and nothing may label it a
# settlement — but it was one of four vocabularies for the same observation, and
# the other three disagreed. There is now one, and this is a re-export of it.


class Freshness(str, Enum):
    """How current a quote is, on the layer's own budget.

    Deliberately the same three states the site already uses (``app/markets``,
    ``LAYER_MAX_DATA_AGE_DAYS``) so the workstation and the market pages cannot
    disagree about whether a source is alive.
    """

    CURRENT = "current"
    BEHIND = "behind"      # older than expected cadence, inside the layer budget
    STALE = "stale"        # past the layer's LAYER_MAX_DATA_AGE_DAYS budget
    UNKNOWN = "unknown"    # no observation date on the row


class ExpiryConfidence(str, Enum):
    """How the last-trade date was arrived at.

    ``DOCUMENTED`` means the contract's published termination rule is encoded
    below and reproduced by :func:`last_trade_date`. ``NOT_ENCODED`` means this
    project has not encoded it, so there is no days-to-expiry, no roll window
    and no expiry alert for that product — an absence, never an estimate.
    """

    DOCUMENTED = "documented"
    NOT_ENCODED = "not_encoded"


#: ``ContractSpec.first_notice_rule`` for a contract that has no notice day.
#: ICE Sugar No. 11 is the one such product here: Rule 11.06(b) puts the
#: delivery obligation on the close of the last trading day itself, so there is
#: no notice period to compute and none to be out ahead of.
NO_NOTICE_DAY = "no_notice_day_mechanism"


class ExpiryRule(str, Enum):
    """Published contract-termination rules, one per family."""

    #: CBOT grains & oilseeds — business day prior to the 15th calendar day of
    #: the contract month.
    CBOT_GRAIN_BD_BEFORE_15TH = "cbot_grain_bd_before_15th"
    #: CME Live Cattle — last business day of the contract month.
    LAST_BUSINESS_DAY_OF_MONTH = "last_business_day_of_month"
    #: CME Lean Hogs — 10th business day of the contract month.
    TENTH_BUSINESS_DAY_OF_MONTH = "tenth_business_day_of_month"
    #: ICE Sugar No. 11 — Rule 11.06(a): "the last full trading day of the
    #: month preceding the delivery month", with a January-only carve-out
    #: (the 2nd business day before the preceding 24 December).
    ICE_SUGAR_LAST_TRADING_DAY = "ice_sugar_last_trading_day"
    #: ICE Cotton No. 2 — Rule 10.02(a)(ix) read through (vii): Last Trading
    #: Day is the 10th business day before Last Delivery Day, and Last Delivery
    #: Day is the 7th-last business day of the delivery month. 10 + 7 = the
    #: 17th business day counted back from the month end, which is exactly the
    #: "seventeen business days from end of spot month" the contract summary
    #: states — two independent statements of one rule, which is why both were
    #: read before encoding it.
    ICE_COTTON_LAST_TRADING_DAY = "ice_cotton_last_trading_day"


class RollMethod(str, Enum):
    """How a continuous series was stitched. Always rendered, never implied.

    ``PROVIDER_FRONT_MONTH`` is what ``prices`` holds today: Yahoo's ``ZS=F``,
    where the underlying contract changes silently and unadjusted, leaving a
    price gap on roll day that is not an economic move. The other two are
    computed here from named-contract history, so the roll date and the
    adjustment are ours and are reproducible.
    """

    PROVIDER_FRONT_MONTH = "provider_front_month"
    CALENDAR_ROLL_UNADJUSTED = "calendar_roll_unadjusted"
    CALENDAR_ROLL_DIFFERENCE = "calendar_roll_difference"   # Panama / back-adjusted
    CALENDAR_ROLL_RATIO = "calendar_roll_ratio"


class Side(str, Enum):
    LONG = "long"
    SHORT = "short"

    @property
    def sign(self) -> int:
        return 1 if self is Side.LONG else -1

    @property
    def opposite(self) -> Side:
        return Side.SHORT if self is Side.LONG else Side.LONG


MONTH_CODES: dict[int, str] = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}
CODE_MONTHS: dict[str, int] = {code: month for month, code in MONTH_CODES.items()}

MONTH_ABBR = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# Mass conversions. Identical to pipeline/units.py's factors, expressed as the
# physical constants they come from rather than as pre-multiplied display
# factors — tests/test_futures_domain.py pins the two against each other so a
# change in one cannot drift from the other.
BUSHELS_PER_MT_60LB = 36.7437     # soybeans & wheat (60 lb bushel)
BUSHELS_PER_MT_56LB = 39.3683     # corn (56 lb bushel)
POUNDS_PER_MT = 2204.62
MT_PER_SHORT_TON = 0.907185


# ---------------------------------------------------------------------------
# Business-day and holiday arithmetic
#
# The expiry rules are all stated in *business days*, so a calendar that
# ignores exchange holidays would put a mid-January or early-September last
# trade date one day out — which is exactly the day a roll alert has to be
# right. The set below is the CME's US holiday schedule (the days the grain
# and livestock floors are closed for the whole session).
# ---------------------------------------------------------------------------


def _easter(year: int) -> date:
    """Western Easter Sunday (anonymous Gregorian algorithm)."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    lo = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * lo) // 451
    month, day = divmod(h + lo - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """The n-th ``weekday`` (Mon=0) of ``month``; n=-1 means the last one."""
    if n > 0:
        first = date(year, month, 1)
        offset = (weekday - first.weekday()) % 7
        return first + timedelta(days=offset + 7 * (n - 1))
    last_day = (date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1))
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _observed(day: date) -> date:
    """US federal observance: Saturday -> Friday, Sunday -> Monday."""
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def exchange_holidays(year: int) -> frozenset[date]:
    """CME full-day US holiday closures for ``year``.

    Early closes (the day after Thanksgiving, Christmas Eve) are deliberately
    absent: they are trading days, and a contract can terminate on one.
    """
    return frozenset({
        _observed(date(year, 1, 1)),                  # New Year's Day
        _nth_weekday(year, 1, 0, 3),                  # MLK Jr Day
        _nth_weekday(year, 2, 0, 3),                  # Presidents' Day
        _easter(year) - timedelta(days=2),            # Good Friday
        _nth_weekday(year, 5, 0, -1),                 # Memorial Day
        _observed(date(year, 6, 19)),                 # Juneteenth
        _observed(date(year, 7, 4)),                  # Independence Day
        _nth_weekday(year, 9, 0, 1),                  # Labor Day
        _nth_weekday(year, 11, 3, 4),                 # Thanksgiving
        _observed(date(year, 12, 25)),                # Christmas Day
    })


def is_business_day(day: date) -> bool:
    return day.weekday() < 5 and day not in exchange_holidays(day.year)


def previous_business_day(day: date) -> date:
    cursor = day - timedelta(days=1)
    while not is_business_day(cursor):
        cursor -= timedelta(days=1)
    return cursor


def next_business_day(day: date) -> date:
    cursor = day + timedelta(days=1)
    while not is_business_day(cursor):
        cursor += timedelta(days=1)
    return cursor


def business_days_between(start: date, end: date) -> int:
    """Business days from ``start`` (exclusive) to ``end`` (inclusive).

    Negative when ``end`` precedes ``start``. Used for days-to-expiry, so it
    counts sessions a hedge could still be traded in, not calendar days.
    """
    if end == start:
        return 0
    step = 1 if end > start else -1
    count = 0
    cursor = start
    while cursor != end:
        cursor += timedelta(days=step)
        if is_business_day(cursor):
            count += step
    return count


def nth_business_day_of_month(year: int, month: int, n: int) -> date:
    """The ``n``-th business day of a month (n >= 1); ``n = -1`` is the last."""
    if n == -1:
        cursor = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
        while not is_business_day(cursor):
            cursor -= timedelta(days=1)
        return cursor
    cursor = date(year, month, 1)
    seen = 0
    while True:
        if is_business_day(cursor):
            seen += 1
            if seen == n:
                return cursor
        cursor += timedelta(days=1)


def nth_business_day_from_month_end(year: int, month: int, n: int) -> date:
    """The ``n``-th business day counted backwards from the end of a month.

    ``n = 1`` is the last business day, ``n = 2`` the one before it. Both ICE
    rules encoded here are stated this way — Cotton's last trading day is the
    17th and its last delivery day the 7th — so the counting convention lives
    in one function rather than being re-derived at each call site, where an
    off-by-one is a hedge left open into the delivery period.
    """
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    cursor = date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1)
    seen = 0
    while True:
        if is_business_day(cursor):
            seen += 1
            if seen == n:
                return cursor
        cursor -= timedelta(days=1)


def business_days_before(day: date, n: int) -> date:
    """The business day ``n`` business days before ``day`` (``day`` excluded)."""
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    cursor = day
    for _ in range(n):
        cursor = previous_business_day(cursor)
    return cursor


# ---------------------------------------------------------------------------
# Contract specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ContractSpec:
    """Everything about a futures *product* that does not change per month.

    ``trading_months`` is not stored here — it is read from
    ``config.FORWARD_CURVE_CONTRACTS`` by :func:`trading_months`, so the fetcher
    and the workstation cannot disagree about which months exist.
    """

    root: str
    exchange: Exchange
    name: str                        # the project's commodity key, e.g. "Soybeans"
    display: str                     # what a trader calls it, e.g. "CBOT Soybeans"
    currency: str
    native_unit: NativeUnit
    contract_size: float
    size_unit: SizeUnit
    tick_size: float                 # in native price units
    tick_value_usd: float
    expiry_rule: ExpiryRule | None
    #: First notice day rule, where the project has it. Physically delivered
    #: CBOT grains: last business day of the month preceding the delivery
    #: month. Left None where not encoded — a hedger must be out before FND,
    #: so a guess here is a directly actionable wrong number.
    #:
    #: :data:`NO_NOTICE_DAY` is a third state, and the distinction matters on
    #: the page: None means *we* have not encoded the rule, while NO_NOTICE_DAY
    #: means the contract runs no notice-day mechanism at all. Rendering the
    #: second as the first invites a hedger to go looking for a date that does
    #: not exist, and to assume they have room they do not have.
    first_notice_rule: str | None = None
    provider_suffix: str = ""        # Yahoo's exchange suffix, e.g. ".CBT"

    @property
    def expiry_confidence(self) -> ExpiryConfidence:
        return (ExpiryConfidence.DOCUMENTED if self.expiry_rule is not None
                else ExpiryConfidence.NOT_ENCODED)

    @property
    def mt_per_contract(self) -> float:
        """Contract size expressed in metric tons."""
        if self.size_unit is SizeUnit.METRIC_TON:
            return self.contract_size
        if self.size_unit is SizeUnit.POUND:
            return self.contract_size / POUNDS_PER_MT
        if self.size_unit is SizeUnit.SHORT_TON:
            return self.contract_size * MT_PER_SHORT_TON
        # bushels — the bushel weight differs by crop, so it is not a constant
        per_mt = BUSHELS_PER_MT_56LB if self.root == "ZC" else BUSHELS_PER_MT_60LB
        return self.contract_size / per_mt

    def native_to_usd_per_mt(self, price: float) -> float:
        """Convert a native quote to USD/MT. The one conversion site for Phase 3.

        Equivalent by construction to ``value_usd(price) / mt_per_contract`` —
        pinned in the tests, because those are the two routes a hedge P&L can
        take to the same number and they must not be allowed to differ.
        """
        return self.value_usd(price) / self.mt_per_contract

    def value_usd(self, price: float) -> float:
        """Notional USD value of one contract at a native price."""
        if self.native_unit is NativeUnit.CENTS_PER_BUSHEL:
            return price * self.contract_size / 100.0
        if self.native_unit is NativeUnit.CENTS_PER_POUND:
            return price * self.contract_size / 100.0
        if self.native_unit is NativeUnit.USD_PER_SHORT_TON:
            return price * self.contract_size
        if self.native_unit is NativeUnit.USD_PER_MT:
            return price * self.contract_size
        raise ValueError(f"{self.root}: no USD valuation for {self.native_unit}")

    def ticks(self, price_move: float) -> float:
        return price_move / self.tick_size


# The nine products the price and forward-curve layers already fetch. Sizes,
# units and tick values are the exchange's published contract specifications;
# expiry rules are encoded only where this project has the published rule, and
# Sugar No. 11 and Cotton No. 2 are deliberately left without one (see
# ExpiryConfidence).
CONTRACT_SPECS: dict[str, ContractSpec] = {
    "Soybeans": ContractSpec(
        root="ZS", exchange=Exchange.CBOT, name="Soybeans", display="CBOT Soybeans",
        currency="USD", native_unit=NativeUnit.CENTS_PER_BUSHEL,
        contract_size=5_000, size_unit=SizeUnit.BUSHEL,
        tick_size=0.25, tick_value_usd=12.50,
        expiry_rule=ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH,
        first_notice_rule="last_business_day_of_prior_month",
        provider_suffix=".CBT",
    ),
    "Soybean Meal": ContractSpec(
        root="ZM", exchange=Exchange.CBOT, name="Soybean Meal", display="CBOT Soybean Meal",
        currency="USD", native_unit=NativeUnit.USD_PER_SHORT_TON,
        contract_size=100, size_unit=SizeUnit.SHORT_TON,
        tick_size=0.10, tick_value_usd=10.00,
        expiry_rule=ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH,
        first_notice_rule="last_business_day_of_prior_month",
        provider_suffix=".CBT",
    ),
    "Soybean Oil": ContractSpec(
        root="ZL", exchange=Exchange.CBOT, name="Soybean Oil", display="CBOT Soybean Oil",
        currency="USD", native_unit=NativeUnit.CENTS_PER_POUND,
        contract_size=60_000, size_unit=SizeUnit.POUND,
        tick_size=0.01, tick_value_usd=6.00,
        expiry_rule=ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH,
        first_notice_rule="last_business_day_of_prior_month",
        provider_suffix=".CBT",
    ),
    "Corn": ContractSpec(
        root="ZC", exchange=Exchange.CBOT, name="Corn", display="CBOT Corn",
        currency="USD", native_unit=NativeUnit.CENTS_PER_BUSHEL,
        contract_size=5_000, size_unit=SizeUnit.BUSHEL,
        tick_size=0.25, tick_value_usd=12.50,
        expiry_rule=ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH,
        first_notice_rule="last_business_day_of_prior_month",
        provider_suffix=".CBT",
    ),
    "Wheat": ContractSpec(
        root="ZW", exchange=Exchange.CBOT, name="Wheat", display="CBOT Wheat (SRW)",
        currency="USD", native_unit=NativeUnit.CENTS_PER_BUSHEL,
        contract_size=5_000, size_unit=SizeUnit.BUSHEL,
        tick_size=0.25, tick_value_usd=12.50,
        expiry_rule=ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH,
        first_notice_rule="last_business_day_of_prior_month",
        provider_suffix=".CBT",
    ),
    "Live Cattle": ContractSpec(
        root="LE", exchange=Exchange.CME, name="Live Cattle", display="CME Live Cattle",
        currency="USD", native_unit=NativeUnit.CENTS_PER_POUND,
        contract_size=40_000, size_unit=SizeUnit.POUND,
        tick_size=0.025, tick_value_usd=10.00,
        expiry_rule=ExpiryRule.LAST_BUSINESS_DAY_OF_MONTH,
        provider_suffix=".CME",
    ),
    "Lean Hogs": ContractSpec(
        root="HE", exchange=Exchange.CME, name="Lean Hogs", display="CME Lean Hogs",
        currency="USD", native_unit=NativeUnit.CENTS_PER_POUND,
        contract_size=40_000, size_unit=SizeUnit.POUND,
        tick_size=0.025, tick_value_usd=10.00,
        expiry_rule=ExpiryRule.TENTH_BUSINESS_DAY_OF_MONTH,
        provider_suffix=".CME",
    ),
    # The two ICE softs. Their termination rules are read off the ICE Futures
    # U.S. rulebook rather than off a summary page, because the summaries state
    # each rule in a *different* frame from the rulebook and only the pair of
    # them proves the counting convention (see ExpiryRule).
    #
    # Sugar No. 11 carries no first_notice_rule, and that absence is the
    # finding, not a gap: the contract has no notice-day mechanism at all.
    # Rule 11.06(b) obliges every open short to issue a "Memo of Deliverer"
    # after the close of business on the Last Trading Day, so the delivery
    # obligation attaches on the last trading day itself. That is earlier and
    # stricter than an FND, not looser, and inventing one here would tell a
    # hedger they had days they do not have.
    "Sugar": ContractSpec(
        root="SB", exchange=Exchange.ICE_US, name="Sugar", display="ICE Sugar No. 11",
        currency="USD", native_unit=NativeUnit.CENTS_PER_POUND,
        contract_size=112_000, size_unit=SizeUnit.POUND,
        tick_size=0.01, tick_value_usd=11.20,
        expiry_rule=ExpiryRule.ICE_SUGAR_LAST_TRADING_DAY,
        first_notice_rule=NO_NOTICE_DAY,
        provider_suffix=".NYB",
    ),
    "Cotton": ContractSpec(
        root="CT", exchange=Exchange.ICE_US, name="Cotton", display="ICE Cotton No. 2",
        currency="USD", native_unit=NativeUnit.CENTS_PER_POUND,
        contract_size=50_000, size_unit=SizeUnit.POUND,
        tick_size=0.01, tick_value_usd=5.00,
        expiry_rule=ExpiryRule.ICE_COTTON_LAST_TRADING_DAY,
        first_notice_rule="five_business_days_before_first_delivery_day",
        provider_suffix=".NYB",
    ),
}

SPEC_BY_ROOT: dict[str, ContractSpec] = {spec.root: spec for spec in CONTRACT_SPECS.values()}

#: The soy complex, in crush order. The workstation's default working set.
SOY_COMPLEX = ("Soybeans", "Soybean Meal", "Soybean Oil")


class UnknownContract(KeyError):
    """Raised when a commodity or root has no encoded contract specification."""


def spec_for(commodity_or_root: str) -> ContractSpec:
    """Look a spec up by project commodity name or by exchange root.

    Hard-fails rather than returning None: every caller here is about to do
    arithmetic with a contract size, and a missing spec must stop the
    calculation rather than produce a plausible number from a default.
    """
    spec = CONTRACT_SPECS.get(commodity_or_root) or SPEC_BY_ROOT.get(commodity_or_root.upper())
    if spec is None:
        raise UnknownContract(
            f"no contract specification for {commodity_or_root!r} — "
            f"known: {', '.join(sorted(CONTRACT_SPECS))}"
        )
    return spec


def trading_months(commodity_or_root: str) -> tuple[int, ...]:
    """Which calendar months this product lists, read from ``config``.

    Deliberately not duplicated into ``CONTRACT_SPECS``: the fetcher builds its
    tickers from ``config.FORWARD_CURVE_CONTRACTS`` and a second copy here
    would let the workstation offer a hedge month the curve never fetches.
    """
    from config import FORWARD_CURVE_CONTRACTS

    spec = spec_for(commodity_or_root)
    entry = FORWARD_CURVE_CONTRACTS.get(spec.name)
    if not entry:
        raise UnknownContract(f"config.FORWARD_CURVE_CONTRACTS has no entry for {spec.name!r}")
    # The config entry is a heterogeneous dict (months, tickers, …), so its
    # value type is `object` to a checker; the months key is a list of ints.
    months = cast(Sequence[int], entry["months"])
    return tuple(sorted(months))


# ---------------------------------------------------------------------------
# The named contract
# ---------------------------------------------------------------------------


def last_trade_date(spec: ContractSpec, year: int, month: int) -> date | None:
    """Published last trading day, or None when the rule is not encoded."""
    rule = spec.expiry_rule
    if rule is None:
        return None
    if rule is ExpiryRule.CBOT_GRAIN_BD_BEFORE_15TH:
        return previous_business_day(date(year, month, 15))
    if rule is ExpiryRule.LAST_BUSINESS_DAY_OF_MONTH:
        return nth_business_day_of_month(year, month, -1)
    if rule is ExpiryRule.TENTH_BUSINESS_DAY_OF_MONTH:
        return nth_business_day_of_month(year, month, 10)
    if rule is ExpiryRule.ICE_SUGAR_LAST_TRADING_DAY:
        if month == 1:
            # Rule 11.06(a)'s January carve-out. Unreachable through
            # NamedContract, which rejects any month outside the listed set
            # (Sugar lists Mar/May/Jul/Oct) — encoded because this function
            # takes a bare month, and a silently wrong January is worse than
            # a branch that never fires.
            return business_days_before(date(year - 1, 12, 24), 2)
        prior_year, prior_month = (year - 1, 12) if month == 1 else (year, month - 1)
        return nth_business_day_from_month_end(prior_year, prior_month, 1)
    if rule is ExpiryRule.ICE_COTTON_LAST_TRADING_DAY:
        return nth_business_day_from_month_end(year, month, 17)
    raise ValueError(f"unhandled expiry rule {rule}")


def first_notice_date(spec: ContractSpec, year: int, month: int) -> date | None:
    """First notice day, or None where the project has not encoded the rule."""
    if spec.first_notice_rule == "last_business_day_of_prior_month":
        prior_year, prior_month = (year - 1, 12) if month == 1 else (year, month - 1)
        return nth_business_day_of_month(prior_year, prior_month, -1)
    if spec.first_notice_rule == "five_business_days_before_first_delivery_day":
        # Cotton No. 2, Rule 10.02(a)(v)+(vi): First Delivery Day is the first
        # business day of the expiring month and First Notice Day is the fifth
        # business day before it. Note this lands *before* the last trading
        # day, not after — the opposite order to the CBOT grains — which is the
        # whole reason this project keys its roll alerts on FND.
        return business_days_before(nth_business_day_of_month(year, month, 1), 5)
    return None


@dataclass(frozen=True)
class NamedContract:
    """One listed contract: ZSX26, and nothing else.

    Constructed through :func:`named_contract` so the expiry metadata is always
    derived from the spec rather than passed in — a caller-supplied expiry is
    how a wrong one gets in.
    """

    spec: ContractSpec
    year: int
    month: int
    last_trade: date | None
    first_notice: date | None

    def __post_init__(self) -> None:
        if self.month not in range(1, 13):
            raise ValueError(f"contract month {self.month} out of range")
        listed = trading_months(self.spec.name)
        if self.month not in listed:
            raise ValueError(
                f"{self.spec.root}: {MONTH_ABBR[self.month]} is not a listed month "
                f"({', '.join(MONTH_ABBR[m] for m in listed)})"
            )

    @property
    def is_hedgeable(self) -> bool:
        """Always True — the counterpart of ``ContinuousSeries.is_hedgeable``.

        ``pricing.policy.require_hedgeable`` refuses anything that does not
        declare itself, so this property is what lets a named contract through
        a hedge, a ticket or a crush, and its absence is what keeps every
        research artifact — present and future — out of them. A property rather
        than a field: it is a fact about the type, and a field would put it in
        ``__eq__``, in the repr and in every constructor call site.
        """
        return True

    # -- identity ----------------------------------------------------------
    @property
    def month_code(self) -> str:
        return MONTH_CODES[self.month]

    @property
    def symbol(self) -> str:
        """Exchange-style symbol, e.g. ``ZSX26``."""
        return f"{self.spec.root}{self.month_code}{self.year % 100:02d}"

    @property
    def provider_symbol(self) -> str:
        """The provider's ticker for this contract, e.g. ``ZSX26.CBT``.

        A provider detail, kept beside the exchange symbol rather than instead
        of it: substituting an authoritative provider changes this string and
        must change nothing else.
        """
        return f"{self.symbol}{self.spec.provider_suffix}"

    @property
    def label(self) -> str:
        return f"{MONTH_ABBR[self.month]} {self.year}"

    @property
    def delivery_month(self) -> str:
        """``YYYY-MM`` — the form ``trust.domain.ContractIdentity`` requires."""
        return f"{self.year:04d}-{self.month:02d}"

    @property
    def contract_month_date(self) -> date:
        """First of the delivery month — how ``forward_curve`` keys a leg."""
        return date(self.year, self.month, 1)

    @property
    def expiry_confidence(self) -> ExpiryConfidence:
        return self.spec.expiry_confidence

    # -- time --------------------------------------------------------------
    def days_to_expiry(self, as_of: date) -> int | None:
        """Business days from ``as_of`` to last trade, or None if not encoded."""
        if self.last_trade is None:
            return None
        return business_days_between(as_of, self.last_trade)

    def calendar_days_to_expiry(self, as_of: date) -> int | None:
        if self.last_trade is None:
            return None
        return (self.last_trade - as_of).days

    def is_expired(self, as_of: date) -> bool | None:
        if self.last_trade is None:
            return None
        return as_of > self.last_trade

    def to_dict(self) -> dict[str, Any]:
        return {
            "exchange": self.spec.exchange.value,
            "root": self.spec.root,
            "symbol": self.symbol,
            "provider_symbol": self.provider_symbol,
            "commodity": self.spec.name,
            "delivery_month": self.delivery_month,
            "year": self.year,
            "month": self.month,
            "month_code": self.month_code,
            "label": self.label,
            "currency": self.spec.currency,
            "native_unit": self.spec.native_unit.value,
            "contract_size": self.spec.contract_size,
            "size_unit": self.spec.size_unit.value,
            "mt_per_contract": round(self.spec.mt_per_contract, 6),
            "last_trade": self.last_trade.isoformat() if self.last_trade else None,
            "first_notice": self.first_notice.isoformat() if self.first_notice else None,
            "expiry_confidence": self.expiry_confidence.value,
        }

    def trust_identity(self) -> dict[str, str]:
        """The payload ``trust.domain.ContractIdentity`` accepts.

        Kept as a dict rather than importing the trust type: ``analysis`` does
        not depend on ``trust`` anywhere else, and the coupling this needs is a
        shape, not an import.
        """
        return {
            "exchange": self.spec.exchange.value.lower(),
            "code": self.symbol,
            "delivery_month": self.delivery_month,
        }


def named_contract(commodity_or_root: str, year: int, month: int) -> NamedContract:
    """Build a :class:`NamedContract`, deriving all expiry metadata."""
    spec = spec_for(commodity_or_root)
    return NamedContract(
        spec=spec,
        year=year,
        month=month,
        last_trade=last_trade_date(spec, year, month),
        first_notice=first_notice_date(spec, year, month),
    )


def parse_symbol(symbol: str) -> NamedContract:
    """Parse ``ZSX26`` / ``ZSX26.CBT`` into a named contract.

    Two-digit years are resolved into 2000-2099, which is the whole of the
    range any listed contract in this stack falls in.
    """
    core = symbol.split(".")[0].strip().upper()
    if len(core) < 4:
        raise UnknownContract(f"{symbol!r} is not a contract symbol")
    root, code, yy = core[:-3], core[-3], core[-2:]
    if code not in CODE_MONTHS or not yy.isdigit():
        raise UnknownContract(f"{symbol!r} is not a contract symbol")
    return named_contract(root, 2000 + int(yy), CODE_MONTHS[code])


def contracts_from(commodity_or_root: str, as_of: date, count: int = 8) -> tuple[NamedContract, ...]:
    """The next ``count`` listed contracts, nearest first.

    The current delivery month is a *candidate*, not a skip — the same rule
    ``fetchers/forward_curve.py`` learned the hard way (#61): a CBOT grain
    contract trades until the business day before the 15th of its own delivery
    month, so for the first half of that month it is the front of the curve.
    Here the rule can be applied exactly rather than by fetching and seeing,
    because the termination date is encoded; where it is not encoded the month
    is kept (the fetcher's behaviour) and dated as unknown.
    """
    spec = spec_for(commodity_or_root)
    months = trading_months(spec.name)
    out: list[NamedContract] = []
    year = as_of.year
    while len(out) < count and year <= as_of.year + 4:
        for month in months:
            if len(out) >= count:
                break
            if year == as_of.year and month < as_of.month:
                continue
            contract = named_contract(spec.name, year, month)
            if contract.is_expired(as_of):
                continue
            out.append(contract)
        year += 1
    return tuple(out)


# ---------------------------------------------------------------------------
# Quotes and series
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Provider:
    """Where a number came from, and what may be claimed about it.

    The isolation seam Phase 3 asks for. Substituting an authoritative feed is
    a new ``Provider`` value plus a new implementation of the reader protocol
    in ``providers.py`` — every calculation downstream reads
    ``quote.price_type`` and ``provider.settlement_authoritative`` and needs no
    edit.
    """

    key: str
    display: str
    delayed: bool
    settlement_authoritative: bool
    #: Free text stating the licence/meaning limits a surface must repeat.
    note: str = ""


YFINANCE_DELAYED = Provider(
    key="yfinance_delayed",
    display="Yahoo Finance (delayed)",
    delayed=True,
    settlement_authoritative=False,
    note=(
        "Delayed consumer daily bar. Usually equals the exchange settlement but is not "
        "verified to; treat as a reference price, never as an official settlement."
    ),
)

MANUAL_ENTRY = Provider(
    key="manual",
    display="Manual entry",
    delayed=False,
    settlement_authoritative=False,
    note="Hand-entered by the user; carries whatever meaning the user gave it.",
)


@dataclass(frozen=True)
class AggregateOpenInterest:
    """Open interest for a *product*, across every listed month at once.

    Deliberately not a field on :class:`ContractQuote`, and deliberately a
    different type. No source this project ingests publishes open interest per
    contract month — the price feed carries none at all, and the one number
    that does exist comes from the CFTC's weekly Commitments of Traders report,
    which is a whole-product figure as of Tuesday's close.

    Putting that number on a contract row would say something false in two
    directions at once: it would attribute the whole product's open interest to
    one month, and it would date a Tuesday figure to whatever session the price
    came from. So it is carried as its own thing, with its own date, and the
    per-contract field stays ``None`` — which continues to mean "nobody told
    us", the only honest reading available for a single month.
    """

    commodity: str
    contracts: float
    report_date: date              # the CFTC report Tuesday, not our fetch date
    scope: str = "all contract months combined"
    source: str = "CFTC Commitments of Traders (weekly)"

    def age_days(self, as_of: date) -> int:
        return (as_of - self.report_date).days

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity": self.commodity,
            "contracts": self.contracts,
            "report_date": self.report_date.isoformat(),
            "scope": self.scope,
            "source": self.source,
        }


@dataclass(frozen=True)
class ContractQuote:
    """A price *for a named contract*, with everything needed to defend it."""

    contract: NamedContract
    price: float                     # native units
    price_type: PriceType
    observation_date: date           # the session the price belongs to
    provider: Provider
    freshness: Freshness = Freshness.UNKNOWN
    fetched_date: date | None = None
    volume: float | None = None      # None means "the provider did not say"
    open_interest: float | None = None
    session_timestamp: datetime | None = None
    #: The session's bar around the close, in native units. Nullable as a
    #: trio — the Layer 11b cleaner keeps all three or none, because a candle
    #: with an invented leg is worse than a close alone (invariant 2). Only
    #: `close_history` fills these; a curve-snapshot quote never carries them.
    session_open: float | None = None
    session_high: float | None = None
    session_low: float | None = None

    def __post_init__(self) -> None:
        if self.price is None:
            raise ValueError(f"{self.contract.symbol}: a quote must carry a price")

    @property
    def currency(self) -> str:
        return self.contract.spec.currency

    @property
    def native_unit(self) -> NativeUnit:
        return self.contract.spec.native_unit

    @property
    def usd_per_mt(self) -> float:
        return self.contract.spec.native_to_usd_per_mt(self.price)

    @property
    def session_bar_usd_per_mt(self) -> tuple[float, float, float] | None:
        """(open, high, low) in USD/MT, or None when the trio is absent.

        Same conversion site as :attr:`usd_per_mt` — the spec's
        ``native_to_usd_per_mt`` — applied to each leg of the bar, so a
        candle and its close can never disagree about the exchange of units.
        """
        if self.session_open is None or self.session_high is None or self.session_low is None:
            return None
        convert = self.contract.spec.native_to_usd_per_mt
        return (
            convert(self.session_open),
            convert(self.session_high),
            convert(self.session_low),
        )

    @property
    def contract_value_usd(self) -> float:
        return self.contract.spec.value_usd(self.price)

    @property
    def is_hedgeable(self) -> bool:
        """Whether this quote names an instrument a hedge can be placed on.

        Delegated to the contract rather than asserted: a quote is hedgeable
        because of what it is a quote *of*. Whether the price is any good is a
        separate question, answered by :attr:`is_settlement_proven`.
        """
        return self.contract.is_hedgeable

    @property
    def is_settlement_proven(self) -> bool:
        return self.price_type.is_settlement_proven and self.provider.settlement_authoritative

    @property
    def price_label(self) -> str:
        """What a surface is allowed to call this number."""
        if self.is_settlement_proven:
            return "settlement"
        if self.price_type is PriceType.DELAYED_CLOSE:
            return "delayed close"
        return self.price_type.value.replace("_", " ")

    def days_to_expiry(self) -> int | None:
        return self.contract.days_to_expiry(self.observation_date)

    def to_dict(self) -> dict[str, Any]:
        return {
            **self.contract.to_dict(),
            "price": self.price,
            "usd_per_mt": round(self.usd_per_mt, 4),
            "contract_value_usd": round(self.contract_value_usd, 2),
            "price_type": self.price_type.value,
            "price_label": self.price_label,
            "settlement_proven": self.is_settlement_proven,
            "observation_date": self.observation_date.isoformat(),
            "fetched_date": self.fetched_date.isoformat() if self.fetched_date else None,
            "provider": self.provider.key,
            "provider_display": self.provider.display,
            "provider_note": self.provider.note,
            "delayed": self.provider.delayed,
            "freshness": self.freshness.value,
            "volume": self.volume,
            "open_interest": self.open_interest,
            "days_to_expiry": self.days_to_expiry(),
        }


@dataclass(frozen=True)
class ContinuousSeries:
    """A stitched research series. **Never** a hedgeable instrument.

    Separate type, deliberately: nothing in ``hedge.py`` or ``ticket.py``
    accepts one. Every consumer that renders it must render
    :attr:`roll_method` beside it, which is why the method is a field rather
    than documentation.
    """

    commodity: str
    roll_method: RollMethod
    #: (date, price) pairs in native units, oldest first.
    points: tuple[tuple[date, float], ...]
    #: Which contract each point came from, where known. Empty for a provider
    #: front-month series — the provider does not say, which is the problem.
    contract_by_date: tuple[tuple[date, str], ...] = ()
    #: Dates the series changed contract, where known.
    roll_dates: tuple[date, ...] = ()
    adjustment_note: str = ""

    @property
    def is_hedgeable(self) -> bool:
        """Always False. Present so the answer is explicit at every call site."""
        return False

    @property
    def method_description(self) -> str:
        return ROLL_METHOD_DESCRIPTIONS[self.roll_method]

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity": self.commodity,
            "roll_method": self.roll_method.value,
            "method_description": self.method_description,
            "adjustment_note": self.adjustment_note,
            "points": [(d.isoformat(), p) for d, p in self.points],
            "roll_dates": [d.isoformat() for d in self.roll_dates],
            "hedgeable": False,
        }


ROLL_METHOD_DESCRIPTIONS: dict[RollMethod, str] = {
    RollMethod.PROVIDER_FRONT_MONTH: (
        "Provider front-month (Yahoo '=F'). The underlying contract changes on the provider's "
        "own schedule, unannounced and unadjusted, so roll days carry a price gap that is not "
        "an economic move. Which contract any given bar belongs to is not published."
    ),
    RollMethod.CALENDAR_ROLL_UNADJUSTED: (
        "Rolled on our calendar — switch to the next listed month N business days before the "
        "front contract's last trade date — and left unadjusted, so the roll-day gap is visible "
        "rather than smoothed away. Which contract each bar belongs to is recorded."
    ),
    RollMethod.CALENDAR_ROLL_DIFFERENCE: (
        "Rolled on our calendar and back-adjusted by the roll-day price difference (Panama "
        "adjustment). Differences and volatility are continuous; levels before the most recent "
        "roll are shifted and are NOT tradeable prices."
    ),
    RollMethod.CALENDAR_ROLL_RATIO: (
        "Rolled on our calendar and back-adjusted by the roll-day price ratio. Percentage "
        "returns are continuous; levels before the most recent roll are scaled and are NOT "
        "tradeable prices."
    ),
}


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------

#: Bumped whenever a formula in this package changes in a way that would move a
#: published number. Stamped on every hedge, scenario and ticket.
METHOD_VERSION = "phase3.futures.v1"


def fingerprint(payload: Any) -> str:
    """Stable 16-hex-char digest of a JSON-able payload.

    Same helper shape as ``analysis.origins.domain.fingerprint`` — sorted keys,
    no clock, no RNG — so a ticket produced from the same inputs on any machine
    carries the same id.
    """
    def _default(value: Any) -> Any:
        if isinstance(value, (date, datetime)):
            return value.isoformat()
        if isinstance(value, Enum):
            return value.value
        if hasattr(value, "to_dict"):
            return value.to_dict()
        raise TypeError(f"cannot fingerprint {type(value).__name__}")

    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=_default)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


__all__ = [
    "BUSHELS_PER_MT_56LB",
    "BUSHELS_PER_MT_60LB",
    "CODE_MONTHS",
    "CONTRACT_SPECS",
    "METHOD_VERSION",
    "MONTH_ABBR",
    "MONTH_CODES",
    "MT_PER_SHORT_TON",
    "NO_NOTICE_DAY",
    "POUNDS_PER_MT",
    "ROLL_METHOD_DESCRIPTIONS",
    "SOY_COMPLEX",
    "SPEC_BY_ROOT",
    "YFINANCE_DELAYED",
    "MANUAL_ENTRY",
    "AggregateOpenInterest",
    "ContinuousSeries",
    "ContractQuote",
    "ContractSpec",
    "Exchange",
    "ExpiryConfidence",
    "ExpiryRule",
    "Freshness",
    "NamedContract",
    "NativeUnit",
    "PriceType",
    "Provider",
    "RollMethod",
    "Side",
    "SizeUnit",
    "UnknownContract",
    "business_days_between",
    "contracts_from",
    "exchange_holidays",
    "fingerprint",
    "business_days_before",
    "first_notice_date",
    "is_business_day",
    "last_trade_date",
    "named_contract",
    "next_business_day",
    "nth_business_day_from_month_end",
    "nth_business_day_of_month",
    "parse_symbol",
    "previous_business_day",
    "spec_for",
    "trading_months",
]
