"""The thin-day label (#370, option 2): a label beside a median, never a filter.

A cross-sectional median (MSAMB's Maharashtra soybean modal, one row per APMC
lot) is honest on any day it is struck, but on a day few lots reported it is a
median of few lots. The reader is told so — beside the price, never instead of
it: no row is dropped, patched or replaced by an older one.

The rule, as the owner decided it on #370 (2026-11-25):

* **thin** when the day's lot count is under ``ratio`` × the median count of
  the trailing ``window`` *weekdays* (Mon–Fri) before it;
* the baseline reads only days on or after ``since`` — the count break where
  the source's row definition last changed (LAYERS.md Layer 16, #371). A count
  across a break is a different measurement, so no threshold may span one;
* complete days only: the caller passes rows its reader already shows, so a
  day still filling (#621) is neither labelled nor counted in the baseline;
* fewer than ``min_days`` baseline weekdays, or no stored count on the day,
  is no label — the rule makes no claim it cannot back. A missing count is
  never read as zero.

Wording: ``thin day · 18 lots (typical weekday ~131)``.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from statistics import median


@dataclass(frozen=True)
class ThinDayRule:
    """One source's thin-day declaration (``config.MARKETS`` → ``thin_day``)."""

    count_column: str
    count_noun: str
    since: date
    ratio: float
    window: int
    min_days: int

    def __post_init__(self) -> None:
        if not self.count_column.strip():
            raise ValueError("thin_day: count_column is empty")
        if not self.count_noun.strip():
            raise ValueError("thin_day: count_noun is empty — the label must say what it counts")
        if not 0.0 < self.ratio < 1.0:
            raise ValueError(f"thin_day: ratio {self.ratio!r} must lie strictly between 0 and 1")
        if self.window < 1:
            raise ValueError(f"thin_day: window {self.window!r} must be at least one weekday")
        if not 1 <= self.min_days <= self.window:
            raise ValueError(
                f"thin_day: min_days {self.min_days!r} must lie in 1..window ({self.window}) "
                "— a baseline the window can never fill would silently never label"
            )


def rule_from_spec(spec: dict) -> ThinDayRule:
    """A ``config.MARKETS`` ``thin_day`` declaration as a rule — the one parse,
    shared by the market registry (block 01, ledger) and the India card."""
    missing = {"count_column", "count_noun", "since", "ratio", "window", "min_days"} - spec.keys()
    if missing:
        raise ValueError(f"thin_day declaration is missing {sorted(missing)}")
    since = spec["since"]
    if not isinstance(since, date):
        raise ValueError(f"thin_day since {since!r} is not a date")
    return ThinDayRule(
        count_column=spec["count_column"],
        count_noun=spec["count_noun"],
        since=since,
        ratio=float(spec["ratio"]),
        window=int(spec["window"]),
        min_days=int(spec["min_days"]),
    )


@dataclass(frozen=True)
class ThinDay:
    """A day that was complete, but small."""

    day: date
    count: int
    typical: float
    noun: str

    @property
    def text(self) -> str:
        return f"thin day · {self.count} {self.noun} (typical weekday ~{round(self.typical)})"


def thin_day(
    counts: Iterable[tuple[date, float | None]],
    day: date,
    rule: ThinDayRule,
) -> ThinDay | None:
    """``day``'s thin-day label, or None where it is not thin or cannot be said.

    ``counts`` are ``(date, count)`` pairs for the complete days the caller's
    reader shows, in any order; a ``None`` count was never learned.
    """
    by_day = {when: count for when, count in counts}
    count = by_day.get(day)
    if count is None:
        return None
    baseline = [
        n
        for when, n in sorted(by_day.items())
        if rule.since <= when < day and when.weekday() < 5 and n is not None
    ][-rule.window:]
    if len(baseline) < rule.min_days:
        return None
    typical = float(median(baseline))
    if count >= rule.ratio * typical:
        return None
    return ThinDay(day=day, count=int(count), typical=typical, noun=rule.count_noun)
