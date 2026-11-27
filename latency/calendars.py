"""Venue closure calendars for the latency vocabulary.

A board-price objective says how soon after a session's close we must hold
its bar. On a day the venue held no session there is nothing to be late for,
and the newest bar we hold is the newest bar that exists. Without a calendar
the measurement cannot tell that state from a provider outage, so every
exchange holiday read as a breach (#399: Golden Week 2026 turned the DCE
refresh gate red for a week on correct data).

Only declared closures count. A year that is not entered here returns
``None`` — "calendar unknown" — and the measurement falls back to today's
behaviour (a holiday reads as a breach) rather than guessing. Nothing is
inferred from the data: a missing bar is never read as a holiday.

Stdlib-only, like ``latency.domain``.
"""

from __future__ import annotations

from datetime import date, timedelta


def _span(first: date, last: date) -> set[date]:
    """Every calendar day from ``first`` to ``last`` inclusive."""
    if last < first:
        raise ValueError(f"closure span runs backwards: {first} > {last}")
    return {first + timedelta(days=n) for n in range((last - first).days + 1)}


# Dalian Commodity Exchange. Source: "Notice on 2026 Dalian Commodity
# Exchange Market Holiday Arrangements", DCE, 2025-12-17, following the CSRC
# holiday notice (DCE ref. 大商所发〔2025〕437号). Weekend days inside a span
# are included for completeness; only weekdays change a verdict.
_DCE_SOURCES: dict[int, str] = {
    2026: (
        "DCE notice of 2025-12-17 on 2026 market holiday arrangements, per the "
        "CSRC notice: New Year 1–3 Jan; Spring Festival 15–23 Feb; Qingming "
        "4–6 Apr; Labour Day 1–5 May; Duanwu 19–21 Jun; Mid-Autumn 25–27 Sep; "
        "National Day 1–7 Oct. Reopens the next trading day in each case."
    ),
}

_DCE_CLOSURES: dict[int, frozenset[date]] = {
    2026: frozenset(
        _span(date(2026, 1, 1), date(2026, 1, 3))
        | _span(date(2026, 2, 15), date(2026, 2, 23))
        | _span(date(2026, 4, 4), date(2026, 4, 6))
        | _span(date(2026, 5, 1), date(2026, 5, 5))
        | _span(date(2026, 6, 19), date(2026, 6, 21))
        | _span(date(2026, 9, 25), date(2026, 9, 27))
        | _span(date(2026, 10, 1), date(2026, 10, 7))
    ),
}

# CME Group (CBOT grains and oilseeds, CME livestock). Not a second list:
# the full-day closures are the rule set the futures calendar already uses
# for expiry arithmetic (analysis.futures.domain.exchange_holidays — stdlib
# only, so this module stays importable before any install). Declared per
# year like the DCE, so a year nobody has entered stays "calendar unknown"
# and keeps reading as a breach (#688: Thanksgiving 2026 turned the CBOT
# board gate red on the correct Wednesday bar). Early closes (the Friday
# after Thanksgiving, Christmas Eve) are trading days and are absent.
_CME_YEARS: tuple[int, ...] = (2026, 2027)


def _cme_closures(year: int) -> frozenset[date]:
    from analysis.futures.domain import exchange_holidays

    return exchange_holidays(year)


_CME_CLOSURES: dict[int, frozenset[date]] = {y: _cme_closures(y) for y in _CME_YEARS}

_CME_SOURCES: dict[int, str] = {
    y: (
        f"CME Group US holiday schedule {y}, full-day closures as ruled in "
        "analysis.futures.domain.exchange_holidays: New Year, MLK Jr Day, "
        "Presidents' Day, Good Friday, Memorial Day, Juneteenth, Independence "
        "Day, Labor Day, Thanksgiving, Christmas (federal observance). "
        "Early-close days trade and are not closures."
    )
    for y in _CME_YEARS
}

_CALENDARS: dict[str, tuple[dict[int, frozenset[date]], dict[int, str]]] = {
    "dce": (_DCE_CLOSURES, _DCE_SOURCES),
    "cme": (_CME_CLOSURES, _CME_SOURCES),
}

KNOWN_VENUES: tuple[str, ...] = tuple(_CALENDARS)


def venue_closures(venue: str, year: int) -> frozenset[date] | None:
    """Declared closure days for ``venue`` in ``year``; ``None`` if not entered.

    An unknown venue is a programming error and raises — a typo'd venue name
    would otherwise be a calendar that silently never applies.
    """
    try:
        closures, _ = _CALENDARS[venue]
    except KeyError:
        raise ValueError(
            f"no closure calendar for venue {venue!r}; known: {', '.join(KNOWN_VENUES)}"
        ) from None
    return closures.get(year)


def closure_basis(venue: str, year: int) -> str | None:
    """Where the ``year`` calendar for ``venue`` came from; ``None`` if not entered."""
    try:
        _, sources = _CALENDARS[venue]
    except KeyError:
        raise ValueError(
            f"no closure calendar for venue {venue!r}; known: {', '.join(KNOWN_VENUES)}"
        ) from None
    return sources.get(year)


__all__ = ["KNOWN_VENUES", "closure_basis", "venue_closures"]
