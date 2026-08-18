"""Scheduled-release calendar for the sources this project actually ingests.

Scope rule, and it is the whole design: **an event is listed only if a layer in
this repository consumes its output.** A calendar that lists every USDA report
in existence would be a research note; this one is a risk tool, and its value
is that every row is a date on which a number *this stack renders* can move.
So there is no NOPA crush (not ingested), no StatsCan, no ABARES, and no
Brazilian CONAB weekly that we do not read.

Two kinds of date, never mixed:

**Rule dates** are computed from the agency's published schedule rule (COT is
Friday 15:30 ET; export sales are Thursday 08:30 ET). They are reliable in
cadence and can be wrong by a day around a federal holiday, which the agency
resolves by shifting — so a rule date carries
:attr:`ScheduledEvent.confidence` = ``rule`` and says it is derived.

**Observed dates** come from our own stored data: the newest observation the
layer actually holds, and the modal gap between the ones before it. That is
evidence rather than a schedule, and it is what makes a frozen upstream
visible — a rule date keeps ticking forward whether or not anybody published.

The two are shown side by side deliberately. A rule date in the future with an
observed date three weeks stale is the single most useful row this calendar can
produce, and it only exists because both are carried.

WASDE is the exception worth naming: USDA publishes its exact release dates a
year ahead, but this project does not ingest that schedule file, so the WASDE
row is a rule date ("around the 9th-12th, monthly") and says so rather than
pretending to a precision it did not source.
"""

from __future__ import annotations

import logging
import sqlite3
import statistics
from dataclasses import dataclass
from datetime import date, timedelta
from enum import Enum
from typing import Any

from analysis.futures.domain import is_business_day, next_business_day

log = logging.getLogger(__name__)


class EventConfidence(str, Enum):
    #: Computed from the agency's published cadence rule.
    RULE = "rule"
    #: Read from our own stored observations.
    OBSERVED = "observed"


@dataclass(frozen=True)
class EventSource:
    """One publisher whose output a layer here consumes."""

    key: str
    name: str
    agency: str
    layer: str                     # the pipeline layer that ingests it
    cadence: str                   # human description of the rule
    what_moves: str                # why a hedger cares
    #: (weekday, hour, minute, tz) for weekly releases; None for monthly ones.
    weekly: tuple[int, int, int, str] | None = None
    #: (day-of-month window) for monthly releases.
    monthly_window: tuple[int, int] | None = None
    #: table and date column to read the observed cadence from, when there is one.
    observation_table: str | None = None
    observation_column: str | None = None
    seasonal_note: str = ""


#: Every scheduled release behind a layer this repository ingests. Times are
#: the agency's own, in US Eastern, and are labels — nothing here schedules
#: anything, so a wrong minute costs nothing and a wrong day is what the
#: observed column is for.
EVENT_SOURCES: tuple[EventSource, ...] = (
    EventSource(
        key="wasde", name="WASDE", agency="USDA OCE", layer="wasde",
        cadence="monthly, typically the 9th-12th, 12:00 ET",
        what_moves="US and world balance sheets — the single most price-moving scheduled release",
        monthly_window=(9, 12),
        observation_table="wasde", observation_column=None,
    ),
    EventSource(
        key="cot", name="Commitments of Traders", agency="CFTC", layer="cot",
        cadence="weekly, Friday 15:30 ET, reporting the previous Tuesday",
        what_moves="managed-money positioning — crowding, and the fuel for a liquidation break",
        weekly=(4, 15, 30, "America/New_York"),
        observation_table="cot", observation_column="Date",
    ),
    EventSource(
        key="export_sales", name="Weekly Export Sales", agency="USDA FAS", layer="export_sales",
        cadence="weekly, Thursday 08:30 ET, reporting through the previous Thursday",
        what_moves="new-crop and old-crop commitments; the China line is the one that moves beans",
        weekly=(3, 8, 30, "America/New_York"),
        observation_table="export_sales", observation_column="week_ending",
    ),
    EventSource(
        key="inspections", name="Export Inspections", agency="USDA AMS", layer="usda",
        cadence="weekly, Monday 11:00 ET",
        what_moves="actual shipments against sold commitments — the pace check on demand",
        weekly=(0, 11, 0, "America/New_York"),
        observation_table="inspections", observation_column="week_ending",
    ),
    EventSource(
        key="crop_progress", name="Crop Progress & Condition", agency="USDA NASS", layer="crop_progress",
        cadence="weekly, Monday 16:00 ET, April through November",
        what_moves="condition ratings drive yield expectations through the growing season",
        weekly=(0, 16, 0, "America/New_York"),
        observation_table="crop_progress", observation_column="Date",
        seasonal_note="not published between roughly December and March",
    ),
    EventSource(
        key="eia", name="Weekly Petroleum Status", agency="EIA", layer="eia",
        cadence="weekly, Wednesday 10:30 ET",
        what_moves="ethanol and biodiesel production — the biofuel pull on soybean oil",
        weekly=(2, 10, 30, "America/New_York"),
        observation_table="eia_energy", observation_column="Date",
    ),
    EventSource(
        key="nass_crush", name="Fats & Oils / Oilseed Crushings", agency="USDA NASS", layer="usda",
        cadence="monthly, around the 1st, 15:00 ET",
        what_moves="US crush volumes and stocks of oil and meal — the demand side of the board crush",
        monthly_window=(1, 3),
        observation_table=None,
    ),
    EventSource(
        key="conab", name="Brazilian crop survey", agency="CONAB", layer="conab",
        cadence="monthly, around the second week",
        what_moves="Brazil production against USDA's own number — the divergence line",
        monthly_window=(8, 14),
        observation_table="brazil_estimates", observation_column=None,
    ),
    EventSource(
        key="cec", name="Crop Estimates Committee", agency="South Africa CEC", layer="cec",
        cadence="monthly, late in the month; no summer-crop table in December",
        what_moves="South African soybean and sunflower area and production, in season",
        monthly_window=(24, 28),
        observation_table="cec_estimates", observation_column="release_date",
        seasonal_note="the December release covers winter cereals only",
    ),
    EventSource(
        key="sagis_smd", name="Monthly supply & demand", agency="SAGIS", layer="sagis_smd",
        cadence="monthly, around the 24th-27th, reporting the previous month",
        what_moves="South African crush volume, trade and stocks",
        monthly_window=(24, 27),
        observation_table="sagis_supply_demand", observation_column=None,
    ),
    EventSource(
        key="sagis", name="Weekly producer deliveries", agency="SAGIS", layer="sagis",
        cadence="weekly, 12:00 on the third working day, reporting the previous week",
        what_moves="South Africa's only physical flow series",
        weekly=(2, 12, 0, "Africa/Johannesburg"),
        observation_table="sagis_deliveries", observation_column=None,
    ),
    EventSource(
        key="ec_oilseeds", name="Oilseeds market observatory", agency="European Commission",
        layer="ec_oilseeds",
        cadence="weekly, Wednesday-dated, published the following day",
        what_moves="EU rapeseed FOB — the Europe page's only price leg",
        weekly=(3, 12, 0, "Europe/Brussels"),
        observation_table="ec_oilseed_prices", observation_column="Date",
    ),
)


@dataclass(frozen=True)
class ScheduledEvent:
    """One dated row on the calendar."""

    source: EventSource
    expected_date: date
    confidence: EventConfidence
    days_away: int
    last_observed: date | None = None
    observed_gap_days: float | None = None
    stale: bool = False
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "key": self.source.key,
            "name": self.source.name,
            "agency": self.source.agency,
            "layer": self.source.layer,
            "cadence": self.source.cadence,
            "what_moves": self.source.what_moves,
            "expected_date": self.expected_date.isoformat(),
            "confidence": self.confidence.value,
            "days_away": self.days_away,
            "last_observed": self.last_observed.isoformat() if self.last_observed else None,
            "observed_gap_days": (
                None if self.observed_gap_days is None else round(self.observed_gap_days, 1)
            ),
            "stale": self.stale,
            "seasonal_note": self.source.seasonal_note,
            "note": self.note,
        }


def next_weekly(weekday: int, on: date) -> date:
    """The next occurrence of ``weekday`` (Mon=0) on or after ``on``.

    Shifted forward to the next business day when the computed date is an
    exchange holiday — which is what the agencies themselves do, and the
    reason this is a *rule* date rather than a published one.
    """
    ahead = (weekday - on.weekday()) % 7
    candidate = on + timedelta(days=ahead)
    return candidate if is_business_day(candidate) else next_business_day(candidate)


def next_monthly(window: tuple[int, int], on: date) -> date:
    """The next date inside a monthly day-of-month window, on or after ``on``."""
    start, end = window
    for offset in (0, 1, 2):
        year = on.year + (on.month - 1 + offset) // 12
        month = (on.month - 1 + offset) % 12 + 1
        for day in range(start, end + 1):
            try:
                candidate = date(year, month, day)
            except ValueError:
                continue
            if candidate >= on and is_business_day(candidate):
                return candidate
    return on


def _observed(conn: sqlite3.Connection, source: EventSource) -> tuple[date | None, float | None]:
    """Newest observation and the modal gap before it, from our own rows."""
    if not source.observation_table or not source.observation_column:
        return None, None
    try:
        rows = list(conn.execute(
            f"SELECT DISTINCT {source.observation_column} FROM {source.observation_table} "  # noqa: S608
            f"WHERE {source.observation_column} IS NOT NULL "
            f"ORDER BY {source.observation_column} DESC LIMIT 12"
        ))
    except sqlite3.Error as exc:
        log.debug("event calendar: cannot read %s — %s", source.observation_table, exc)
        return None, None
    dates: list[date] = []
    for row in rows:
        try:
            dates.append(date.fromisoformat(str(row[0])[:10]))
        except ValueError:
            continue
    if not dates:
        return None, None
    dates.sort()
    gaps = [(b - a).days for a, b in zip(dates, dates[1:], strict=False) if (b - a).days > 0]
    return dates[-1], (statistics.median(gaps) if gaps else None)


def build_calendar(
    conn: sqlite3.Connection | None,
    *,
    as_of: date,
    horizon_days: int = 45,
) -> tuple[ScheduledEvent, ...]:
    """The next scheduled release for every ingested source, soonest first.

    ``conn`` may be None — the rule dates need no database, and the calendar
    degrades to schedule-only with the observed columns blank rather than
    failing. That is the state a fresh clone is in.
    """
    horizon = as_of + timedelta(days=horizon_days)
    events: list[ScheduledEvent] = []

    for source in EVENT_SOURCES:
        if source.weekly is not None:
            expected = next_weekly(source.weekly[0], as_of)
        elif source.monthly_window is not None:
            expected = next_monthly(source.monthly_window, as_of)
        else:
            continue
        if expected > horizon:
            continue

        last_observed, gap = (_observed(conn, source) if conn is not None else (None, None))
        stale = False
        note = ""
        if last_observed is not None and gap:
            age = (as_of - last_observed).days
            # Two cadences' worth of silence is the point at which a rule date
            # in the future stops being reassuring and starts being the thing
            # to look at.
            stale = age > gap * 2
            if stale:
                note = (
                    f"our newest observation is {age} days old against a typical "
                    f"{gap:.0f}-day cadence — the schedule below is a rule, not evidence "
                    "that anything published"
                )

        events.append(ScheduledEvent(
            source=source,
            expected_date=expected,
            confidence=EventConfidence.RULE,
            days_away=(expected - as_of).days,
            last_observed=last_observed,
            observed_gap_days=gap,
            stale=stale,
            note=note,
        ))

    return tuple(sorted(events, key=lambda event: (event.expected_date, event.source.key)))


def events_within(events: tuple[ScheduledEvent, ...], days: int) -> tuple[ScheduledEvent, ...]:
    return tuple(event for event in events if event.days_away <= days)


__all__ = [
    "EVENT_SOURCES",
    "EventConfidence",
    "EventSource",
    "ScheduledEvent",
    "build_calendar",
    "events_within",
    "next_monthly",
    "next_weekly",
]
