"""Curve analytics over named contracts (Phase 3).

``analysis/forward_curve.py`` answers "is this contango or backwardation" from
a DataFrame of closes. That is the right question for a briefing paragraph and
the wrong one for a hedger, who needs to know *which month* to hedge in, what
the carry between two named months is worth per tonne per month, whether that
spread is wide or narrow against its own history, and how many sessions are
left before the contract they are looking at stops trading.

This module answers those. It takes a :class:`~analysis.futures.providers.
CurveObservation` and returns values, never DataFrames, so the hedge
calculator and the page read the same objects.

Three deliberate refusals:

**An incoherent curve is not analysed.** If the legs are not one session, the
spreads between them mix term structure with the intervening move. The
analysis is still produced — a trader still wants to see the legs — but it is
stamped ``coherent=False`` and every spread carries ``same_session=False``,
which the page renders as a refusal rather than a footnote.

**Carry is annualised on business days between the two contracts' last trade
dates, not on their delivery-month labels.** Nov→Jan is not "two months" of
carry; it is the days between two termination dates, and using the labels
overstates or understates every annualised figure by up to a fortnight's worth.
Where either leg's expiry rule is not encoded, the annualised number is None
rather than approximated from the labels.

**Open interest is never inferred.** No provider in this stack publishes it.
Volume is passed through where the provider gave it and reported absent where
it did not; neither is ever derived from price.
"""

from __future__ import annotations

import logging
import statistics
from dataclasses import dataclass
from datetime import date
from enum import Enum
from typing import Any

from analysis.futures.domain import (
    ContractQuote,
    NamedContract,
    business_days_between,
)
from analysis.futures.providers import CurveObservation

log = logging.getLogger(__name__)

#: Business days in a year, for annualising a carry. 252 is the convention the
#: rest of the stack's volatility maths uses (``analysis/technical.py``), kept
#: identical so a carry and a vol are annualised on one calendar.
BUSINESS_DAYS_PER_YEAR = 252

#: A spread percentile computed from a handful of observations is noise wearing
#: a statistic's clothes. Below this the percentile is withheld and the sample
#: size is reported instead.
MIN_HISTORY_FOR_PERCENTILE = 20


class CurveStructure(str, Enum):
    CONTANGO = "contango"
    MILD_CONTANGO = "mild contango"
    BACKWARDATION = "backwardation"
    MILD_BACKWARDATION = "mild backwardation"
    FLAT = "flat"
    UNDETERMINED = "undetermined"

    @property
    def implication(self) -> str:
        if "backwardation" in self.value:
            return "tight nearby supply or strong prompt demand — storage is not being paid for"
        if "contango" in self.value:
            return "adequate supply; the market is paying to carry it forward"
        if self is CurveStructure.FLAT:
            return "no term premium either way"
        return "not enough legs to read a structure"


@dataclass(frozen=True)
class CalendarSpread:
    """One named month against another. Both legs, always.

    ``value`` is in the product's native price units (the units a spread is
    actually quoted and traded in — a ZS Nov/Jan spread is quoted in cents/bu),
    and ``usd_per_mt`` is the same number for a physical trader who thinks in
    tonnes. Both are carried because both are used, and deriving one from the
    other at the render site is how they drift.
    """

    near: NamedContract
    far: NamedContract
    near_price: float
    far_price: float
    value: float                    # far - near, native units
    usd_per_mt: float               # far - near, USD/MT
    same_session: bool
    observation_date: date | None
    calendar_months: int
    business_days: int | None       # between the two last-trade dates
    annualised_pct: float | None    # of the near price, per annum
    usd_per_mt_per_month: float | None

    @property
    def is_carry(self) -> bool:
        """True when the far month is above the near — the market pays to store."""
        return self.value > 0

    @property
    def label(self) -> str:
        return f"{self.near.label} / {self.far.label}"

    @property
    def symbols(self) -> str:
        return f"{self.near.symbol}-{self.far.symbol}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "near": self.near.symbol,
            "far": self.far.symbol,
            "near_label": self.near.label,
            "far_label": self.far.label,
            "label": self.label,
            "near_price": self.near_price,
            "far_price": self.far_price,
            "value": round(self.value, 4),
            "usd_per_mt": round(self.usd_per_mt, 3),
            "same_session": self.same_session,
            "observation_date": self.observation_date.isoformat() if self.observation_date else None,
            "calendar_months": self.calendar_months,
            "business_days": self.business_days,
            "annualised_pct": None if self.annualised_pct is None else round(self.annualised_pct, 3),
            "usd_per_mt_per_month": (
                None if self.usd_per_mt_per_month is None else round(self.usd_per_mt_per_month, 3)
            ),
            "is_carry": self.is_carry,
        }


@dataclass(frozen=True)
class SpreadHistory:
    """A named spread through time, with its percentile rank today.

    Percentile is the *today* value's rank inside its own history, so 95 means
    "wider carry than 95% of the sessions we have stored". It is withheld below
    :data:`MIN_HISTORY_FOR_PERCENTILE` observations, and the count is always
    reported — a percentile whose sample size is hidden is the statistic most
    likely to be believed and least likely to be true.
    """

    symbols: str
    points: tuple[tuple[date, float], ...]
    current: float | None
    percentile: float | None
    minimum: float | None
    maximum: float | None
    median: float | None
    sample_size: int
    withheld_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "symbols": self.symbols,
            "points": [(d.isoformat(), round(v, 4)) for d, v in self.points],
            "current": None if self.current is None else round(self.current, 4),
            "percentile": None if self.percentile is None else round(self.percentile, 1),
            "min": None if self.minimum is None else round(self.minimum, 4),
            "max": None if self.maximum is None else round(self.maximum, 4),
            "median": None if self.median is None else round(self.median, 4),
            "sample_size": self.sample_size,
            "withheld_reason": self.withheld_reason,
        }


@dataclass(frozen=True)
class CurveLeg:
    """A quote with the workstation's per-leg derived facts attached."""

    quote: ContractQuote
    days_to_expiry: int | None
    calendar_days_to_expiry: int | None
    is_front: bool
    volume: float | None
    open_interest: float | None

    @property
    def contract(self) -> NamedContract:
        return self.quote.contract

    def to_dict(self) -> dict[str, Any]:
        payload = self.quote.to_dict()
        payload.update({
            "is_front": self.is_front,
            "calendar_days_to_expiry": self.calendar_days_to_expiry,
            "volume_available": self.volume is not None,
            "open_interest_available": self.open_interest is not None,
        })
        return payload


@dataclass(frozen=True)
class CurveAnalysis:
    """Everything the workstation knows about one commodity's term structure."""

    commodity: str
    legs: tuple[CurveLeg, ...]
    structure: CurveStructure
    observation_date: date | None
    coherent: bool
    coherence_note: str
    spreads: tuple[CalendarSpread, ...]          # consecutive pairs
    front_spreads: tuple[CalendarSpread, ...]    # front against every deferred
    slope_per_month_usd_mt: float | None
    front_price_usd_mt: float | None
    back_price_usd_mt: float | None
    volume_available: bool
    open_interest_available: bool
    open_interest_note: str
    provider_note: str
    freshness: str
    age_days: int | None
    dropped_legs: tuple[str, ...] = ()
    histories: tuple[SpreadHistory, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not self.legs

    @property
    def front(self) -> CurveLeg | None:
        return self.legs[0] if self.legs else None

    @property
    def is_inverted(self) -> bool:
        return self.structure in (CurveStructure.BACKWARDATION, CurveStructure.MILD_BACKWARDATION)

    def spread(self, near_symbol: str, far_symbol: str) -> CalendarSpread | None:
        wanted = (near_symbol.upper(), far_symbol.upper())
        for candidate in (*self.spreads, *self.front_spreads):
            if (candidate.near.symbol, candidate.far.symbol) == wanted:
                return candidate
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "commodity": self.commodity,
            "legs": [leg.to_dict() for leg in self.legs],
            "structure": self.structure.value,
            "implication": self.structure.implication,
            "inverted": self.is_inverted,
            "observation_date": self.observation_date.isoformat() if self.observation_date else None,
            "coherent": self.coherent,
            "coherence_note": self.coherence_note,
            "spreads": [s.to_dict() for s in self.spreads],
            "front_spreads": [s.to_dict() for s in self.front_spreads],
            "slope_per_month_usd_mt": (
                None if self.slope_per_month_usd_mt is None else round(self.slope_per_month_usd_mt, 3)
            ),
            "front_price_usd_mt": (
                None if self.front_price_usd_mt is None else round(self.front_price_usd_mt, 2)
            ),
            "back_price_usd_mt": (
                None if self.back_price_usd_mt is None else round(self.back_price_usd_mt, 2)
            ),
            "volume_available": self.volume_available,
            "open_interest_available": self.open_interest_available,
            "open_interest_note": self.open_interest_note,
            "provider_note": self.provider_note,
            "freshness": self.freshness,
            "age_days": self.age_days,
            "dropped_legs": list(self.dropped_legs),
            "histories": [h.to_dict() for h in self.histories],
        }


OPEN_INTEREST_UNAVAILABLE = (
    "Open interest is not published by any source this project ingests. It is shown as "
    "unavailable rather than derived from volume or price — there is no honest way to derive it."
)


def _calendar_months(near: NamedContract, far: NamedContract) -> int:
    return (far.year - near.year) * 12 + (far.month - near.month)


def build_spread(near: ContractQuote, far: ContractQuote) -> CalendarSpread:
    """One calendar spread from two quotes.

    Annualisation uses the business days between the two contracts' *last trade
    dates* — the actual length of the carry being priced. Where either leg's
    expiry rule is not encoded (ICE Sugar, ICE Cotton), the annualised figure
    and the per-month figure are both None: the delivery-month labels would
    give a number, and it would be wrong by up to a fortnight either way.
    """
    spec = near.contract.spec
    value = far.price - near.price
    usd_per_mt = spec.native_to_usd_per_mt(far.price) - spec.native_to_usd_per_mt(near.price)
    months = _calendar_months(near.contract, far.contract)

    bdays: int | None = None
    annualised: float | None = None
    per_month: float | None = None
    if near.contract.last_trade and far.contract.last_trade:
        bdays = business_days_between(near.contract.last_trade, far.contract.last_trade)
        if bdays > 0 and near.price != 0:
            years = bdays / BUSINESS_DAYS_PER_YEAR
            annualised = (value / abs(near.price)) / years * 100.0
            per_month = usd_per_mt / (bdays / (BUSINESS_DAYS_PER_YEAR / 12))

    return CalendarSpread(
        near=near.contract,
        far=far.contract,
        near_price=near.price,
        far_price=far.price,
        value=value,
        usd_per_mt=usd_per_mt,
        same_session=near.observation_date == far.observation_date,
        observation_date=near.observation_date if near.observation_date == far.observation_date else None,
        calendar_months=months,
        business_days=bdays,
        annualised_pct=annualised,
        usd_per_mt_per_month=per_month,
    )


def _structure(legs: tuple[CurveLeg, ...]) -> CurveStructure:
    """Contango vs backwardation, sign-first.

    The sign of the front→back spread decides, and monotonicity only sets the
    strength — the rule ``analysis/forward_curve.py`` arrived at after a
    move-count majority labelled a 13%-inverted curve "contango" because the
    inversion happened in one step.
    """
    if len(legs) < 2:
        return CurveStructure.UNDETERMINED
    prices = [leg.quote.price for leg in legs]
    spread = prices[-1] - prices[0]
    ups = sum(1 for a, b in zip(prices, prices[1:], strict=False) if b > a)
    downs = sum(1 for a, b in zip(prices, prices[1:], strict=False) if b < a)
    if ups == downs == 0:
        return CurveStructure.FLAT
    if spread > 0:
        return CurveStructure.CONTANGO if downs == 0 else CurveStructure.MILD_CONTANGO
    if spread < 0:
        return CurveStructure.BACKWARDATION if ups == 0 else CurveStructure.MILD_BACKWARDATION
    return CurveStructure.FLAT


def _slope_usd_mt_per_month(legs: tuple[CurveLeg, ...]) -> float | None:
    if len(legs) < 2:
        return None
    spec = legs[0].contract.spec
    months = _calendar_months(legs[0].contract, legs[-1].contract)
    if months == 0:
        return None
    move = spec.native_to_usd_per_mt(legs[-1].quote.price) - spec.native_to_usd_per_mt(legs[0].quote.price)
    return move / months


def analyse_curve(
    observation: CurveObservation,
    *,
    as_of: date | None = None,
    history: tuple[CurveObservation, ...] = (),
) -> CurveAnalysis:
    """Turn a provider's curve observation into the workstation's view of it."""
    as_of = as_of or observation.observation_date or date.today()
    legs = tuple(
        CurveLeg(
            quote=quote,
            days_to_expiry=quote.contract.days_to_expiry(as_of),
            calendar_days_to_expiry=quote.contract.calendar_days_to_expiry(as_of),
            is_front=(position == 0),
            volume=quote.volume,
            open_interest=quote.open_interest,
        )
        for position, quote in enumerate(observation.legs)
    )

    spreads = tuple(
        build_spread(a.quote, b.quote)
        for a, b in zip(legs, legs[1:], strict=False)
    )
    front_spreads = tuple(
        build_spread(legs[0].quote, leg.quote) for leg in legs[1:]
    ) if legs else ()

    spec = legs[0].contract.spec if legs else None
    return CurveAnalysis(
        commodity=observation.commodity,
        legs=legs,
        structure=_structure(legs),
        observation_date=observation.observation_date,
        coherent=observation.coherent,
        coherence_note=observation.coherence_note,
        spreads=spreads,
        front_spreads=front_spreads,
        slope_per_month_usd_mt=_slope_usd_mt_per_month(legs),
        front_price_usd_mt=spec.native_to_usd_per_mt(legs[0].quote.price) if spec else None,
        back_price_usd_mt=spec.native_to_usd_per_mt(legs[-1].quote.price) if spec else None,
        volume_available=any(leg.volume is not None for leg in legs),
        open_interest_available=any(leg.open_interest is not None for leg in legs),
        open_interest_note=OPEN_INTEREST_UNAVAILABLE,
        provider_note=observation.provider.note,
        freshness=observation.freshness.value,
        age_days=observation.age_days,
        dropped_legs=observation.dropped_legs,
        histories=build_histories(observation, history),
    )


def build_histories(
    current: CurveObservation,
    history: tuple[CurveObservation, ...],
    *,
    max_spreads: int = 4,
) -> tuple[SpreadHistory, ...]:
    """Spread history and percentile for the front consecutive spreads.

    Only spreads whose *both* legs are present in a past observation contribute
    a point — a spread is a difference between two named contracts, and a
    session missing one of them has no value for it, not a value of zero.
    """
    if len(current.legs) < 2:
        return ()

    wanted = [
        (a.contract.symbol, b.contract.symbol)
        for a, b in zip(current.legs, current.legs[1:], strict=False)
    ][:max_spreads]

    out: list[SpreadHistory] = []
    for near_symbol, far_symbol in wanted:
        points: list[tuple[date, float]] = []
        for snapshot in history:
            near = snapshot.leg(near_symbol)
            far = snapshot.leg(far_symbol)
            if near is None or far is None or not snapshot.coherent:
                continue
            if near.observation_date != far.observation_date:
                continue
            points.append((near.observation_date, far.price - near.price))
        points.sort()

        near_now, far_now = current.leg(near_symbol), current.leg(far_symbol)
        current_value = (far_now.price - near_now.price) if near_now and far_now else None
        values = [value for _, value in points]

        percentile: float | None = None
        withheld = ""
        if current_value is None:
            withheld = "no current value for this spread"
        elif len(values) < MIN_HISTORY_FOR_PERCENTILE:
            withheld = (
                f"{len(values)} stored session(s) — a percentile needs at least "
                f"{MIN_HISTORY_FOR_PERCENTILE} to mean anything"
            )
        else:
            below = sum(1 for value in values if value < current_value)
            equal = sum(1 for value in values if value == current_value)
            percentile = (below + 0.5 * equal) / len(values) * 100.0

        out.append(SpreadHistory(
            symbols=f"{near_symbol}-{far_symbol}",
            points=tuple(points),
            current=current_value,
            percentile=percentile,
            minimum=min(values) if values else None,
            maximum=max(values) if values else None,
            median=statistics.median(values) if values else None,
            sample_size=len(values),
            withheld_reason=withheld,
        ))
    return tuple(out)


def hedge_month_candidates(
    analysis: CurveAnalysis, *, pricing_end: date, min_days_to_expiry: int = 5
) -> tuple[CurveLeg, ...]:
    """Legs that could carry a hedge for exposure priced out to ``pricing_end``.

    The rule a hedger actually applies: the hedge month must still be trading
    when the physical prices, with a working margin before last trade. Legs
    whose expiry rule is not encoded are *excluded* rather than assumed
    suitable — recommending a hedge month whose termination date is unknown is
    the one thing this module must not do.
    """
    out = []
    for leg in analysis.legs:
        last_trade = leg.contract.last_trade
        if last_trade is None:
            continue
        if last_trade < pricing_end:
            continue
        if leg.days_to_expiry is not None and leg.days_to_expiry < min_days_to_expiry:
            continue
        out.append(leg)
    return tuple(out)


__all__ = [
    "BUSINESS_DAYS_PER_YEAR",
    "MIN_HISTORY_FOR_PERCENTILE",
    "OPEN_INTEREST_UNAVAILABLE",
    "CalendarSpread",
    "CurveAnalysis",
    "CurveLeg",
    "CurveStructure",
    "SpreadHistory",
    "analyse_curve",
    "build_histories",
    "build_spread",
    "hedge_month_candidates",
]
