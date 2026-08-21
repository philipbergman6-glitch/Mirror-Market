"""The latency vocabulary. Standard library only — no DB, no pandas.

Everything here answers one question in four parts: a number was observed
somewhere in the world at some instant, and became readable on a public page
at another. **Which parts of that interval did we cause, and which were
imposed on us?**

The decomposition
-----------------
Four intervals, three of them measured end to end and one of them declared:

``acquisition``  observation -> fetch completed
    Measured. It contains two things that cannot be separated by observing
    our own pipeline: the provider's own delay in publishing the datum, and
    our own wait before asking for it. Splitting it requires knowing when
    the provider *first* had the number, which needs polling the provider,
    not reading our database.

``processing``   fetch completed -> stored
    Measured, and entirely ours: clean, validate, write.

``publication``  stored -> publicly readable
    Measured, and entirely ours: analysis, site generation, deploy.

``provider_delay``  declared, per class, with its basis recorded
    The floor under ``acquisition``. It is a *claim about the provider*, in
    the same spirit as ``pricing.semantics.PROVEN_SETTLEMENT_SOURCES``: it
    is only as good as the basis string next to it, and it is never inferred
    from our own timings. Subtracting it from ``acquisition`` gives
    ``cadence_wait`` — the part of the staleness we chose by deciding how
    often to run.

That last subtraction is the point of the whole module. A trader looking at
a four-hour-old board price cannot tell whether Yahoo was slow or we simply
had not run yet, and those have completely different fixes: one is a
provider problem nothing in this repo can solve, the other is a cron line.

Why observation is an instant and not a date
--------------------------------------------
Almost every source here publishes a *date*, not a timestamp. Calling a
daily bar's age "0 days" the moment the date rolls over would understate it
by most of a day. So each layer declares the venue-local time at which its
observation for day D came into being — CBOT settlement, the FX 17:00 New
York rollover, an assessment's publication hour — and the age is measured
from that instant.

Where that hour is genuinely unknown, ``observation_close`` is ``None`` and
the layer is ``DAY`` granular: age is reported in whole days with the
uncertainty stated, never converted to a false hour. That is the same rule
the rest of this codebase applies to unknowns — absence never becomes an
assumption.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from zoneinfo import ZoneInfo

# ---------------------------------------------------------------------------
# Stages
# ---------------------------------------------------------------------------


class Stage(str, Enum):
    """The six points a number passes on its way to a reader."""

    OBSERVATION = "observation"
    FETCH = "fetch"
    STORE = "store"
    ANALYSIS = "analysis"
    GENERATION = "generation"
    PUBLICATION = "publication"


class Granularity(str, Enum):
    """How precisely the observation instant is known."""

    SESSION_CLOSE = "session_close"  # a declared venue-local hour on the date
    DAY = "day"                      # the source publishes a date and nothing more


class Verdict(str, Enum):
    MEETS = "meets"
    BREACHES = "breaches"
    UNKNOWN = "unknown"  # a stamp is missing; never silently "meets"


# ---------------------------------------------------------------------------
# Classes and their objectives
# ---------------------------------------------------------------------------


class LatencyClass(str, Enum):
    """The five things a reader treats differently.

    They are separated because their *acceptable* staleness differs by two
    orders of magnitude, not because their plumbing does. A weekly CFTC
    report four days old is on time; a board price four days old is an
    outage.
    """

    BOARD_PRICE = "board_price"
    PHYSICAL_ORIGIN = "physical_origin"
    FX = "fx"
    FUNDAMENTALS = "fundamentals"
    WEATHER = "weather"


@dataclass(frozen=True)
class LatencyObjective:
    """What "on time" means for one class.

    ``acquisition_target`` is what a reader feels: how old the number is by
    the time we have it. ``pipeline_target`` is the part we are wholly
    accountable for — fetch to publicly readable — and is the only one of
    the two that a code change can move without changing the schedule.
    """

    latency_class: LatencyClass
    acquisition_target: timedelta
    pipeline_target: timedelta
    basis: str

    @property
    def end_to_end_target(self) -> timedelta:
        return self.acquisition_target + self.pipeline_target


_PIPELINE_TARGET = timedelta(minutes=25)
"""One budget for fetch -> publicly readable, shared by every class.

The work after a fetch does not know or care what class the number belongs
to: the same clean, store, generate and deploy path carries all of them, and
the measured cost of it is dominated by a fixed site build (3.6 s for 13
pages) plus a Pages deploy. A per-class number here would be five copies of
one fact. 25 minutes is that measured cost with room for a deploy retry —
see LATENCY.md.
"""

OBJECTIVES: dict[LatencyClass, LatencyObjective] = {
    LatencyClass.BOARD_PRICE: LatencyObjective(
        latency_class=LatencyClass.BOARD_PRICE,
        acquisition_target=timedelta(hours=6),
        pipeline_target=_PIPELINE_TARGET,
        basis=(
            "CBOT grains settle 13:15 CT and the latest venue we pull (ICE cotton) at "
            "13:20 CT; config.SETTLEMENT_CUTOFF_LOCAL waits until 14:30 CT before a bar "
            "may be stored at all, which puts the floor at ~1h15m. The binding "
            "constraint above that floor is not our code but GitHub's scheduler: the "
            "workflow's own note records a +64 to +298 minute delay across 30 observed "
            "runs, so a 19:00 UTC cron lands anywhere in 20:00-24:00 UTC and a "
            "settlement-to-fetch interval of 1h45m to 5h45m is the honest envelope. "
            "Six hours is that envelope. Four would have been the tightest the guard "
            "permits and would have been missed on roughly a third of runs for reasons "
            "no change here can fix — a target missed that often is a target readers "
            "learn to ignore, which is the badge-blindness #176 removed."
        ),
    ),
    LatencyClass.FX: LatencyObjective(
        latency_class=LatencyClass.FX,
        acquisition_target=timedelta(hours=6),
        pipeline_target=_PIPELINE_TARGET,
        basis=(
            "Spot FX has no settlement; the bar labelled D closes at 17:00 New York "
            "on D, which is 21:00 UTC in EDT and 22:00 UTC in EST. The daily build "
            "lands 20:00-24:00 UTC, so on a summer evening it can straddle the close "
            "and on a winter one it usually misses it — publishing D-1 FX, correctly, "
            "because the guard drops the open bar. The 21:30 UTC fast refresh lands "
            "22:34-02:28 UTC, past the close on both sides of US DST, giving 0h34m to "
            "5h28m. Six hours is that envelope, and it is a target a single daily "
            "build cannot meet — which is precisely why the fast path exists."
        ),
    ),
    LatencyClass.PHYSICAL_ORIGIN: LatencyObjective(
        latency_class=LatencyClass.PHYSICAL_ORIGIN,
        acquisition_target=timedelta(hours=8),
        pipeline_target=_PIPELINE_TARGET,
        basis=(
            "The physical legs publish across a wide band of the day — AMS report "
            "3147 around 18:48 UTC, Argentina MAGyP after ~15:05 UTC, CEPEA after "
            "21:01 UTC — and none of them publishes a timestamp we can read. Eight "
            "hours covers the spread between the earliest and the latest of them from "
            "one build, and CEPEA is the leg it is sized on."
        ),
    ),
    LatencyClass.FUNDAMENTALS: LatencyObjective(
        latency_class=LatencyClass.FUNDAMENTALS,
        acquisition_target=timedelta(hours=24),
        pipeline_target=_PIPELINE_TARGET,
        basis=(
            "Weekly and monthly releases. The trader-relevant question is not hours "
            "but 'did we pick up this week's report before the reader looked', so the "
            "target is one scheduled build cycle from release. Tightening it would "
            "buy nothing: a WASDE is no fresher for being fetched at noon than at "
            "midnight of its release day."
        ),
    ),
    LatencyClass.WEATHER: LatencyObjective(
        latency_class=LatencyClass.WEATHER,
        acquisition_target=timedelta(hours=12),
        pipeline_target=_PIPELINE_TARGET,
        basis=(
            "Open-Meteo returns observations *and* forecast rows, so the newest "
            "observation date is routinely in the future and the measured age is "
            "negative. Twelve hours bounds the observed half; the forecast half is "
            "not a latency question at all."
        ),
    ),
}


# ---------------------------------------------------------------------------
# Per-layer specifications
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ObservationClock:
    """When a source's observation for day D actually came into being.

    ``None`` timezone/time is the honest state for a source that publishes a
    bare date: see ``Granularity.DAY``.
    """

    timezone: str | None = None
    close_local: tuple[int, int] | None = None

    @property
    def granularity(self) -> Granularity:
        if self.timezone is None or self.close_local is None:
            return Granularity.DAY
        return Granularity.SESSION_CLOSE

    def instant(self, observed_on: date) -> datetime | None:
        """The observation instant for ``observed_on``, or None if day-granular."""
        if self.timezone is None or self.close_local is None:
            return None
        return datetime.combine(
            observed_on, time(*self.close_local), tzinfo=ZoneInfo(self.timezone)
        )


@dataclass(frozen=True)
class LayerLatency:
    """One trader-critical layer's latency specification."""

    layer: str
    latency_class: LatencyClass
    clock: ObservationClock
    provider_delay: timedelta
    provider_delay_basis: str

    @property
    def objective(self) -> LatencyObjective:
        return OBJECTIVES[self.latency_class]


_CBOT = ObservationClock("America/Chicago", (13, 15))
_FX_CLOSE = ObservationClock("America/New_York", (17, 0))

# Measured on 2026-08-19 at 03:45 UTC: ZS=F carried a complete bar for the
# 2026-08-18 session, which settled at 18:15 UTC, so the settled bar was
# retrievable within 9h30m of settlement. That is an upper bound from a
# single observation, not a service level — Yahoo publishes none — so it is
# quoted as the bound it is. The 30 minutes below is the *working* figure
# used to separate cadence wait from provider delay, and it is deliberately
# tight: understating the provider's share attributes the remainder to us,
# which is the direction that keeps us honest about our own schedule.
_YAHOO_BASIS = (
    "Yahoo publishes no service level for the settled daily bar. Live probe "
    "2026-08-19 03:45 UTC found the 2026-08-18 CBOT session (settled 18:15 UTC) "
    "already complete, bounding the delay at <=9h30m. 30 minutes is used as the "
    "working floor: understating the provider's share attributes the rest to us, "
    "which is the conservative direction for judging our own schedule."
)

LAYER_LATENCIES: tuple[LayerLatency, ...] = (
    LayerLatency("prices", LatencyClass.BOARD_PRICE, _CBOT, timedelta(minutes=30), _YAHOO_BASIS),
    LayerLatency("forward_curve", LatencyClass.BOARD_PRICE, _CBOT, timedelta(minutes=30), _YAHOO_BASIS),
    LayerLatency(
        "dce", LatencyClass.BOARD_PRICE,
        ObservationClock("Asia/Shanghai", (15, 0)),
        timedelta(hours=2),
        "AKShare republishes the DCE/CZCE daily file after the 15:00 CST close; "
        "no publication timestamp is exposed, so two hours is a stated working "
        "assumption rather than a measurement.",
    ),
    LayerLatency("currencies", LatencyClass.FX, _FX_CLOSE, timedelta(minutes=30), _YAHOO_BASIS),
    # Physical legs. Each hour below is the source's own observed publication
    # window as recorded in the deploy workflow's schedule reasoning; none of
    # these sources stamps a time on the datum itself.
    LayerLatency(
        "cepea", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("America/Sao_Paulo", (18, 0)),
        timedelta(0),
        "CEPEA/ESALQ publishes day D's indicator after 21:01 UTC on D; Noticias "
        "Agricolas republishes it server-rendered within minutes. The publication "
        "hour IS the observation hour for an assessment, so there is no separable "
        "provider delay on top of it.",
    ),
    LayerLatency(
        "agrural", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("America/Sao_Paulo", (15, 0)),
        timedelta(0),
        "AgRural posts the Paranagua FOB quote during the Brazilian afternoon; "
        "an assessment's publication is its observation.",
    ),
    LayerLatency(
        "gulf_bids", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("America/Chicago", (13, 48)),
        timedelta(0),
        "AMS report 3147 lands ~18:48 UTC (13:48 CT), per the deploy workflow's "
        "own scheduling note. Published bids are the observation.",
    ),
    LayerLatency(
        "magyp_fob", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("America/Argentina/Buenos_Aires", (12, 5)),
        timedelta(0),
        "MAGyP's official FOB circular publishes after ~15:05 UTC (12:05 ART). An "
        "administered minimum is published, not observed and then published.",
    ),
    LayerLatency(
        "safex", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("Africa/Johannesburg", (12, 0)),
        timedelta(hours=2),
        "SAFEX grain futures close 12:00 SAST; Grain SA's free table republishes "
        "the last traded price afterwards with no stated lag, so two hours is a "
        "working assumption. Note the stored number is a last trade, not a "
        "settlement (see LAYERS.md Layer 18).",
    ),
    LayerLatency(
        "india_domestic", LatencyClass.PHYSICAL_ORIGIN,
        ObservationClock("Asia/Kolkata", (18, 0)),
        timedelta(0),
        "Agmarknet arrivals are reported through the trading day and the API "
        "serves the current day only; 18:00 IST is the working close of "
        "reporting. The daily number is a cross-sectional median of whichever "
        "mandis have reported, so it firms up during the day rather than "
        "arriving at an instant — an uncertainty no timestamp would remove.",
    ),
    LayerLatency(
        "conab_precos", LatencyClass.PHYSICAL_ORIGIN, ObservationClock(),
        timedelta(0),
        "CONAB's weekly farmgate file carries a week-ending date and no hour.",
    ),
    LayerLatency(
        "ec_oilseeds", LatencyClass.PHYSICAL_ORIGIN, ObservationClock(),
        timedelta(hours=24),
        "The EC observatory workbook is Wednesday-dated and published the "
        "following day; the workbook carries no time.",
    ),
    # Fundamentals and macro. All day-granular: a marketing-year or
    # week-ending key has no hour, and inventing one would be the false
    # precision this module exists to avoid.
    LayerLatency(
        "fred", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(hours=24),
        "FRED's daily series carry a one-day publication lag against their "
        "observation date and no intraday timestamp.",
    ),
    LayerLatency(
        "cot", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(days=3),
        "The CFTC releases Friday 15:30 ET reporting the previous Tuesday, so "
        "the observation is three days old at the moment it becomes available.",
    ),
    LayerLatency(
        "export_sales", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(days=6),
        "FAS releases Thursday 08:30 ET for the week ending the previous "
        "Thursday.",
    ),
    LayerLatency(
        "crush_inspections", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(days=4),
        "AMS publishes export inspections Monday for the week ending the "
        "previous Thursday.",
    ),
    LayerLatency(
        "gtr_ocean_freight", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(days=14),
        "AMS publishes the Grain Transportation Report's ocean-freight table "
        "monthly, assessed by a broker for the prior period.",
    ),
    LayerLatency(
        "gtr_vessels", LatencyClass.FUNDAMENTALS, ObservationClock(), timedelta(days=4),
        "The GTR vessel lineup is weekly, published for the week just ended.",
    ),
    LayerLatency(
        "weather", LatencyClass.WEATHER, ObservationClock(), timedelta(hours=6),
        "Open-Meteo reanalysis lands within hours of the observation day; the "
        "frame also carries forecast rows, whose age is negative by design.",
    ),
    LayerLatency(
        "river_us", LatencyClass.WEATHER, ObservationClock(), timedelta(hours=24),
        "NWPS republishes the USACE/USGS stage within the hour, but the stored "
        "value is a completed river day: the current day is a partial "
        "aggregate and is dropped, so the newest observation is D-1.",
    ),
    LayerLatency(
        "river_ar", LatencyClass.WEATHER, ObservationClock(), timedelta(hours=24),
        "INA publishes the Prefectura reading for 00:00 local during the "
        "morning of the same day; the probe on 2026-08-21 carried the series "
        "to 2026-08-20, so one day is the working assumption, not zero.",
    ),
)

LATENCY_CLASS_BY_LAYER: dict[str, LatencyClass] = {
    spec.layer: spec.latency_class for spec in LAYER_LATENCIES
}

LAYER_LATENCY_BY_KEY: dict[str, LayerLatency] = {
    spec.layer: spec for spec in LAYER_LATENCIES
}

TRADER_CRITICAL_LAYERS: tuple[str, ...] = tuple(spec.layer for spec in LAYER_LATENCIES)


# ---------------------------------------------------------------------------
# Measurements
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StageStamps:
    """The instants one layer's newest datum passed through.

    Every field is optional and a missing one yields ``Verdict.UNKNOWN``
    rather than a default. A latency computed against a fabricated stamp is
    worse than no latency at all: it reads as a measurement.
    """

    observed_at: datetime | None = None
    fetch_started_at: datetime | None = None
    fetch_completed_at: datetime | None = None
    stored_at: datetime | None = None
    generated_at: datetime | None = None
    published_at: datetime | None = None


def _delta(start: datetime | None, end: datetime | None) -> timedelta | None:
    if start is None or end is None:
        return None
    return end - start


@dataclass(frozen=True)
class LayerMeasurement:
    """One layer's measured chain, plus the verdict against its objective."""

    spec: LayerLatency
    stamps: StageStamps
    granularity: Granularity
    status: str = "success"

    @property
    def layer(self) -> str:
        return self.spec.layer

    @property
    def acquisition(self) -> timedelta | None:
        """Observation -> fetch completed. Provider delay plus our own wait."""
        return _delta(self.stamps.observed_at, self.stamps.fetch_completed_at)

    @property
    def fetch_duration(self) -> timedelta | None:
        return _delta(self.stamps.fetch_started_at, self.stamps.fetch_completed_at)

    @property
    def processing(self) -> timedelta | None:
        """Fetch completed -> stored. Ours."""
        return _delta(self.stamps.fetch_completed_at, self.stamps.stored_at)

    @property
    def publication(self) -> timedelta | None:
        """Stored -> publicly readable. Ours: analysis, generation, deploy."""
        return _delta(self.stamps.stored_at, self.stamps.published_at)

    @property
    def pipeline(self) -> timedelta | None:
        """Fetch completed -> publicly readable. The budget a code change moves."""
        return _delta(self.stamps.fetch_completed_at, self.stamps.published_at)

    @property
    def end_to_end(self) -> timedelta | None:
        return _delta(self.stamps.observed_at, self.stamps.published_at)

    @property
    def cadence_wait(self) -> timedelta | None:
        """The share of ``acquisition`` we chose, by deciding how often to run.

        ``acquisition`` minus the declared provider delay, floored at zero.
        A zero here does not prove the provider was the whole story — it
        means acquisition came in at or under the declared floor, so nothing
        is attributable to our schedule on the evidence available.
        """
        acquired = self.acquisition
        if acquired is None:
            return None
        remainder = acquired - self.spec.provider_delay
        return max(remainder, timedelta(0))

    def _verdict(self, measured: timedelta | None, target: timedelta) -> Verdict:
        if measured is None:
            return Verdict.UNKNOWN
        return Verdict.MEETS if measured <= target else Verdict.BREACHES

    @property
    def acquisition_verdict(self) -> Verdict:
        return self._verdict(self.acquisition, self.spec.objective.acquisition_target)

    @property
    def pipeline_verdict(self) -> Verdict:
        return self._verdict(self.pipeline, self.spec.objective.pipeline_target)

    @property
    def end_to_end_verdict(self) -> Verdict:
        return self._verdict(self.end_to_end, self.spec.objective.end_to_end_target)

    @property
    def verdict(self) -> Verdict:
        """The layer's overall standing — the worst of its parts.

        UNKNOWN outranks MEETS: a chain with a missing stamp has not been
        shown to meet anything.
        """
        parts = (self.acquisition_verdict, self.pipeline_verdict)
        if Verdict.BREACHES in parts:
            return Verdict.BREACHES
        if Verdict.UNKNOWN in parts:
            return Verdict.UNKNOWN
        return Verdict.MEETS


def age_at(measurement: LayerMeasurement, now: datetime) -> timedelta | None:
    """How old the newest observation is *right now*, as a reader sees it.

    This is the number that belongs on a page. ``end_to_end`` describes the
    journey of one datum; this describes the staleness of what is currently
    on screen, which keeps growing after publication and is what a reader is
    actually deciding on.
    """
    if measurement.stamps.observed_at is None:
        return None
    return now - measurement.stamps.observed_at


def format_delta(value: timedelta | None) -> str:
    """Human-readable, sign-preserving. Negative is real: forecast rows."""
    if value is None:
        return "unknown"
    total = int(value.total_seconds())
    sign = "-" if total < 0 else ""
    total = abs(total)
    days, rem = divmod(total, 86_400)
    hours, rem = divmod(rem, 3_600)
    minutes = rem // 60
    if days:
        return f"{sign}{days}d {hours}h"
    if hours:
        return f"{sign}{hours}h {minutes}m"
    return f"{sign}{minutes}m"
