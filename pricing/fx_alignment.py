"""The one FX-date rule — A1, decided in #298, enforced by B2 (#308).

A ``home_per_mt`` leg converts at its own date's FX close when one exists;
otherwise at the newest **prior** close, at most ``FX_ALIGNMENT_MAX_GAP_DAYS``
calendar days older; otherwise it renders blank with a reason. Never at a
later-dated rate: a price converted at a rate from its own future is a
different number wearing the right currency.

A labelled prior close is a real observation, not an invention (invariant 2
forbids fabricating; this fabricates nothing). The result carries **both**
dates so every renderer can show the substitution wherever price date and FX
date differ. It is a conversion annotation, not a price claim — nothing here
touches ``pricing.semantics`` (invariant 3), and an aligned conversion never
satisfies invariant 8's same-session requirement: alignment is for stating a
single leg in USD, never for cross-leg arithmetic.

This is the single policy for every reader — the site's ``SiteContext.fx_on``,
the origins reader, the futures provider — and for every surface, public and
desk (#298 §6: display depth may differ, policy may not). ``align_fx`` is
pure: it takes the stored closes and the price date, and knows nothing about
where either came from, so #402's spread work and anything after it reuse it
unchanged.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from typing import Any

import config

__all__ = [
    "FX_GAP_EXCEEDED",
    "FX_NO_PAIR",
    "FX_NO_PRIOR_RATE",
    "FxAlignment",
    "FxResolution",
    "align_fx",
]

#: The newest prior close is older than the cap. Doubles as a frozen-feed alarm.
FX_GAP_EXCEEDED = "fx_gap_exceeded"
#: No close on or before the price date at all — including the case where
#: only *later* rates exist, which the dead ``fallback_to_oldest`` used to
#: convert at. Distinct from a gap: there is nothing to measure a gap to.
FX_NO_PRIOR_RATE = "fx_no_prior_rate"
#: The market has no currency pair (the USD numeraire). Nothing to align.
FX_NO_PAIR = "fx_no_pair"


@dataclass(frozen=True)
class FxAlignment:
    """The rate one conversion was struck at, with the date it was struck for
    and the date the rate was observed on.

    ``usd_per_unit`` is USD per one unit of the local currency — the
    ``<CCY>/USD`` series' own convention, so the stack *multiplies*.
    """

    pair: str
    usd_per_unit: float
    price_date: date
    observed_on: date

    def __post_init__(self) -> None:
        if self.usd_per_unit <= 0:
            raise ValueError(f"FX rate for {self.pair} must be positive, got {self.usd_per_unit}")
        if self.observed_on > self.price_date:
            raise ValueError(
                f"FX for {self.pair} observed {self.observed_on} is later than the price "
                f"date {self.price_date} — a later-dated rate is never an alignment"
            )

    @property
    def gap_days(self) -> int:
        return (self.price_date - self.observed_on).days

    @property
    def aligned(self) -> bool:
        """True where the rate is a prior close, not the price date's own."""
        return self.observed_on != self.price_date

    @property
    def label(self) -> str | None:
        """The substitution label a renderer prints beside the USD figure, or
        None on the price date's own rate (nothing was substituted)."""
        if not self.aligned:
            return None
        return f"FX {self.observed_on.isoformat()} ({self.gap_days}d prior)"

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": self.pair,
            "usd_per_unit": self.usd_per_unit,
            "price_date": self.price_date.isoformat(),
            "observed_on": self.observed_on.isoformat(),
            "gap_days": self.gap_days,
            "aligned": self.aligned,
            "label": self.label,
        }


@dataclass(frozen=True)
class FxResolution:
    """What ``align_fx`` decided: an alignment, or a reason there is none.

    Exactly one of ``alignment`` / ``reason`` is set. ``gap_days`` is carried
    on a refusal too, so a withheld figure can say *how* stale the newest
    prior close was; it is None when there was no prior close to measure.
    """

    pair: str | None
    price_date: date
    alignment: FxAlignment | None = None
    reason: str | None = None
    gap_days: int | None = None
    newest_prior: date | None = None

    def __post_init__(self) -> None:
        if (self.alignment is None) == (self.reason is None):
            raise ValueError("an FxResolution carries exactly one of alignment / reason")

    @property
    def ok(self) -> bool:
        return self.alignment is not None

    @property
    def rate(self) -> float | None:
        """USD per unit of the local currency, or None where withheld — the
        value ``Source.to_usd_mt`` takes as ``fx``."""
        return self.alignment.usd_per_unit if self.alignment else None

    @property
    def observed_on(self) -> date | None:
        return self.alignment.observed_on if self.alignment else None

    @property
    def label(self) -> str | None:
        return self.alignment.label if self.alignment else None

    @property
    def note(self) -> str | None:
        """A sentence for a withheld figure's reason string, or None when ok."""
        if self.alignment is not None:
            return None
        if self.reason == FX_NO_PAIR:
            return "no currency pair — the market is the USD numeraire"
        if self.reason == FX_GAP_EXCEEDED:
            assert self.newest_prior is not None and self.gap_days is not None
            return (
                f"{self.reason}: newest {self.pair} close before {self.price_date.isoformat()} "
                f"is {self.newest_prior.isoformat()}, {self.gap_days} calendar days older than "
                f"the {config.FX_ALIGNMENT_MAX_GAP_DAYS}-day cap (#298)"
            )
        return f"{self.reason}: no {self.pair} close on or before {self.price_date.isoformat()}"

    def to_dict(self) -> dict[str, Any]:
        base: dict[str, Any] = {
            "pair": self.pair,
            "price_date": self.price_date.isoformat(),
            "reason": self.reason,
            "gap_days": self.gap_days,
            "aligned": None,
            "observed_on": None,
            "usd_per_unit": None,
            "label": None,
        }
        if self.alignment is not None:
            base.update(self.alignment.to_dict())
        return base


def align_fx(
    pair: str | None,
    rates: Iterable[tuple[date, float]],
    when: date,
    *,
    max_gap_days: int | None = None,
) -> FxResolution:
    """Resolve the rate a price dated ``when`` converts at, under A1.

    ``rates`` are ``(observed_on, usd_per_unit)`` closes in any order; only
    those on or before ``when`` are ever considered. ``max_gap_days`` defaults
    to ``config.FX_ALIGNMENT_MAX_GAP_DAYS`` and exists for tests, not for
    callers to loosen — one policy, both surfaces.
    """
    cap = config.FX_ALIGNMENT_MAX_GAP_DAYS if max_gap_days is None else max_gap_days
    if cap < 0:
        raise ValueError(f"max_gap_days must be non-negative, got {cap}")
    if not pair:
        return FxResolution(pair=None, price_date=when, reason=FX_NO_PAIR)

    newest: tuple[date, float] | None = None
    for observed, rate in rates:
        if observed > when:
            continue  # a later-dated rate is never a candidate
        if newest is None or observed > newest[0]:
            newest = (observed, rate)
    if newest is None:
        return FxResolution(pair=pair, price_date=when, reason=FX_NO_PRIOR_RATE)

    observed, rate = newest
    gap = (when - observed).days
    if gap > cap:
        return FxResolution(
            pair=pair, price_date=when, reason=FX_GAP_EXCEEDED, gap_days=gap, newest_prior=observed
        )
    alignment = FxAlignment(pair=pair, usd_per_unit=float(rate), price_date=when, observed_on=observed)
    return FxResolution(pair=pair, price_date=when, alignment=alignment, gap_days=gap, newest_prior=observed)
