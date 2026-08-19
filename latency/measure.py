"""The only database-aware module in ``latency``.

It reads ``data_freshness`` — nothing else — and turns each trader-critical
layer's row into a ``LayerMeasurement``. Everything it needs is already
there because ``pipeline.store.save_freshness`` stamps it: this module runs
no per-layer SQL against the twenty-odd data tables, so adding a layer to
``latency.domain.LAYER_LATENCIES`` needs no query written for it.

The one judgement it makes is turning an observation *date* into an
observation *instant*, and it makes it by asking the layer's declared
``ObservationClock``. Where no venue hour is known the measurement stays
day-granular and the instant is the end of the observation day in UTC — the
latest moment the datum could have come into being, which is the choice
that under-states our staleness rather than over-stating it, and is flagged
as ``Granularity.DAY`` so no surface can quote it as an hour.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from latency.domain import (
    LAYER_LATENCIES,
    Granularity,
    LayerLatency,
    LayerMeasurement,
    StageStamps,
)

logger = logging.getLogger(__name__)


def _as_utc(value: Any) -> datetime | None:
    """Coerce a stored stamp to an aware UTC datetime, or None."""
    if value is None:
        return None
    # pandas NaT and numpy NaN both fail this, which is what we want.
    if value != value:  # noqa: PLR0124 — NaN/NaT check without importing pandas
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        parsed = datetime.combine(value, time(0, 0))
    else:
        try:
            parsed = datetime.fromisoformat(str(value))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _observation_instant(
    spec: LayerLatency, observed_at: Any
) -> tuple[datetime | None, Granularity]:
    """Resolve a stored observation date to the instant it came into being."""
    stamp = _as_utc(observed_at)
    if stamp is None:
        return None, spec.clock.granularity

    observed_on = stamp.date()
    instant = spec.clock.instant(observed_on)
    if instant is not None:
        return instant.astimezone(timezone.utc), Granularity.SESSION_CLOSE

    # Day-granular: no declared venue hour. End of the observation day in
    # UTC is the latest instant the datum could have existed, so ages
    # computed from it are lower bounds. Stating a lower bound is honest;
    # picking midnight (and inflating every age by a day) is not.
    end_of_day = datetime.combine(observed_on, time(23, 59, 59), tzinfo=timezone.utc)
    return end_of_day, Granularity.DAY


def measure_from_rows(
    rows: Mapping[str, Mapping[str, Any]],
    *,
    published_at: datetime | None = None,
    generated_at: datetime | None = None,
    specs: tuple[LayerLatency, ...] = LAYER_LATENCIES,
) -> list[LayerMeasurement]:
    """Build measurements from freshness rows keyed by layer name.

    Separated from the database read so the whole measurement is testable
    against literal rows, and so a caller holding an already-read frame
    (the site generator does) need not re-open the database.

    ``published_at`` is when the edition became publicly readable. It is a
    parameter rather than something looked up because nothing inside this
    process can know it: the deploy happens after generation, in CI. A run
    that does not know it yet passes ``generated_at`` alone, and the
    publication leg reports ``unknown`` rather than a guess.
    """
    measurements: list[LayerMeasurement] = []
    for spec in specs:
        row = rows.get(spec.layer)
        if row is None:
            measurements.append(
                LayerMeasurement(
                    spec=spec,
                    stamps=StageStamps(generated_at=generated_at, published_at=published_at),
                    granularity=spec.clock.granularity,
                    status="not-run",
                )
            )
            continue

        observed_instant, granularity = _observation_instant(spec, row.get("observed_at"))
        measurements.append(
            LayerMeasurement(
                spec=spec,
                stamps=StageStamps(
                    observed_at=observed_instant,
                    fetch_started_at=_as_utc(row.get("fetch_started_at")),
                    fetch_completed_at=_as_utc(row.get("fetch_completed_at")),
                    stored_at=_as_utc(row.get("stored_at")) or _as_utc(row.get("last_attempt")),
                    generated_at=generated_at,
                    published_at=published_at,
                ),
                granularity=granularity,
                status=str(row.get("status") or "success"),
            )
        )
    return measurements


def measure(
    *,
    published_at: datetime | None = None,
    generated_at: datetime | None = None,
    specs: tuple[LayerLatency, ...] = LAYER_LATENCIES,
) -> list[LayerMeasurement]:
    """Measure every trader-critical layer from the live database."""
    from pipeline.query import read_freshness

    frame = read_freshness()
    rows: dict[str, Mapping[str, Any]] = {}
    if not frame.empty and "layer_name" in frame.columns:
        # to_dict("index") is typed as keyed by Hashable, so the layer names
        # are re-stated as str rather than asserted to be.
        rows = {
            str(layer): {str(column): value for column, value in values.items()}
            for layer, values in frame.set_index("layer_name").to_dict("index").items()
        }
    return measure_from_rows(
        rows, published_at=published_at, generated_at=generated_at, specs=specs
    )


def worst_observation_age(
    measurements: list[LayerMeasurement], now: datetime
) -> timedelta | None:
    """The staleness of the oldest board-price or FX leg on the page.

    The masthead needs one number, and the honest one is the worst of the
    legs a reader prices off *right now* — not an average, which a healthy
    weather layer would drag down, and not the newest, which would hide the
    dark leg that matters.
    """
    from latency.domain import LatencyClass, age_at

    ages = [
        age
        for m in measurements
        if m.spec.latency_class in (LatencyClass.BOARD_PRICE, LatencyClass.FX)
        and (age := age_at(m, now)) is not None
    ]
    return max(ages) if ages else None
