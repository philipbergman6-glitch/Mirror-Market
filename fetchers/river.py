"""
Layers 27 / 28 — river gauges. Water is the freight leg of a cash bid.

    27  Mississippi at Memphis and at St. Louis  — NOAA NWPS, feet
    28  Paraná at Rosario                        — Argentina INA, metres

Two providers, one shape: both land in `river_levels` with the same columns,
so the site reads one series type and the market stays a parameter. They are
graded as two layers because a quiet Argentine endpoint must not take the
Mississippi leg down with it.

WHY THIS IS A PRICE LAYER IN EVERYTHING BUT NAME. The 2022 Mississippi low
took St. Louis barge rates from ~$20 to ~$106/ton and US Gulf soybean basis to
a record +$3.00/bu, and it repeated in 2023 and 2024; the 2021 Paraná low
(0.06 m at Rosario against a 2.92 m 24-year median) cut cargo sizes by
5,500-7,000 t and Rosario soy exports by more than two thirds. The number is a
stage reading, and the trade it moves is the freight inside a cash bid.

WHAT THIS IS NOT. It is not a price and never enters `to_usd_mt`: `stage` is
feet on the Mississippi and metres on the Paraná, stamped per row, and the two
must never share a unit label — a metre rendered as a foot parses perfectly.
Memphis stage is on the local gauge datum, not an elevation, which is why its
record low reads -10.81 ft.

Four traps this module exists to survive:

1.  **USGS does not serve stage at Memphis.** `waterservices.usgs.gov/nwis/iv`
    with `parameterCd=00065` for site 07032000 answers HTTP 200 with an empty
    `timeSeries` list (probed 2026-08-21) — the stage there is a USACE
    measurement that NWPS republishes. A fetcher built on the obvious USGS
    endpoint would return nothing and log success. NWPS is the source, and
    `waterdata.usgs.gov`'s legacy pages are being decommissioned in 2026
    besides.

2.  **NWPS prints its "no value" sentinel in the data field.** `-999` and
    `-9999` appear inline in the same key as a real reading. On a gauge whose
    record low is -10.81 ft, a sentinel stored as a stage is not an obvious
    outlier — it is the deepest low-water event in history, every day. Every
    value is therefore checked against `RIVER_STAGE_BOUNDS` for its own unit
    and dropped by name if it falls outside.

3.  **A forecast row must never date an observation.** NWPS returns a forecast
    trace that starts *before* the newest observation (2026-08-21: observed to
    12:00Z, forecast issued the previous afternoon and running from 18:00Z the
    day before). Stored naively into a `(gauge, Date)` upsert, yesterday's
    model output overwrites yesterday's measurement. Forecast rows are cut to
    dates strictly after the newest observed date, and `is_forecast` rides on
    every row so `main._latest_observation_date` can drop them before dating
    the layer — otherwise a dead observed feed passes its recency budget on a
    14-day forecast trace alone.

4.  **A day's last reading is only the day's reading once the day is over.**
    NWPS samples hourly, so the current local day is a partial aggregate and
    is dropped — the same trade invariant 11 makes for an unsettled bar. INA
    publishes one point reading per day at 00:00 local and there is nothing to
    aggregate, so nothing is dropped there. The rule is derived from the data
    rather than hard-coded per provider (`_daily_last`): if any day carries
    more than one sample, the newest local day is partial.

Neither provider needs a key. The Paraná leg self-heals from INA's own
history (back to 1884); the Mississippi leg does not — NWPS serves a rolling
~30-day observed window — so `river_levels` round-trips through
`data/history/`.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from config import (
    INA_OBSERVATIONS_URL,
    MAX_RETRIES,
    NWPS_GAUGE_URL,
    REQUEST_TIMEOUT,
    RIVER_GAUGES_INA,
    RIVER_GAUGES_NWPS,
    RIVER_INA_LOOKBACK_DAYS,
    RIVER_NWPS_LOOKBACK_DAYS,
    RIVER_STAGE_BOUNDS,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; MirrorMarket/1.0)"}

# The columns every gauge lands in, whichever provider it came from.
RIVER_COLUMNS = ("Date", "stage", "unit", "is_forecast", "source", "attribution")


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(RIVER_COLUMNS))


def _get_json(url: str, params: dict | None, label: str) -> object | None:
    """GET and decode JSON with the project's shared retry policy.

    None means "never answered" — the caller must not read that as "nothing to
    report". An empty *payload* is a different fact and comes back as a
    decoded empty structure.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting %s (attempt %d) ...", label, attempt)
            resp = requests.get(
                url, params=params, headers=_HEADERS, timeout=REQUEST_TIMEOUT
            )
            if resp.status_code != 200:
                logger.warning(
                    "HTTP %d for %s: %s", resp.status_code, label, resp.text[:200]
                )
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)
                    continue
                return None
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            logger.warning(
                "Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, label, exc
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for %s", MAX_RETRIES, label)
    return None


class _Discards:
    """Counts values dropped by the bounds check, so the log says it once.

    Trap 2 fires 15 times in a single St. Louis payload (measured live
    2026-08-21). One line per dropped row buries the run's real output; one
    line per block, carrying the count and an example, does not.
    """

    def __init__(self, gauge: str, unit: str) -> None:
        self.gauge = gauge
        self.unit = unit
        self.count = 0
        self.example: float | None = None

    def record(self, value: float) -> None:
        self.count += 1
        if self.example is None:
            self.example = value

    def report(self, block: str) -> None:
        if not self.count:
            return
        low, high = RIVER_STAGE_BOUNDS[self.unit]
        logger.warning(
            "%s (%s): dropped %d value(s) outside the %.4g..%.4g %s band, "
            "e.g. %.4g — NWPS prints -999/-9999 inline for 'no value'",
            self.gauge, block, self.count, low, high, self.unit, self.example,
        )


def _in_bounds(value: float, unit: str, discards: _Discards) -> bool:
    """Is this a river level, or NWPS's sentinel wearing one's clothes?

    Trap 2. The check is per-unit because a plausible foot is an impossible
    metre, and it drops the row rather than clamping it — a clamped sentinel
    is still a number nobody measured.
    """
    low, high = RIVER_STAGE_BOUNDS[unit]
    if low <= value <= high:
        return True
    discards.record(value)
    return False


def _daily_last(
    samples: list[tuple[datetime, float]],
    *,
    gauge: str,
    today_local: date,
    complete_days_only: bool,
) -> list[tuple[date, float]]:
    """One value per local day: the day's last reading.

    Samples arrive stamped with their *instant*, not their day, and that is
    load-bearing: sorting `(day, value)` pairs orders same-day readings by
    stage, so "the day's last" silently becomes "the day's highest" — an
    upward bias in exactly the low-water direction that matters. Ordering is
    by time and the day is derived from it.

    Trap 4, and it applies to *observations* only — hence
    ``complete_days_only``. An hourly observed series has a partial current
    day and it is dropped, the same trade the settlement guard makes for an
    unsettled bar (invariant 11). A forecast trace is not an aggregate of
    anything: its value for today is the latest issued value for today, whole
    the moment it is published, and it is replaced by the observation when the
    day closes anyway.

    Whether the newest observed day is partial is read off the sampling rate
    rather than declared per provider: a source publishing one point a day has
    nothing to aggregate, so a provider that silently moves to sub-daily
    sampling starts dropping its partial day on its own, and says so.
    """
    by_day: dict[date, float] = {}
    counts: dict[date, int] = {}
    for when, value in sorted(samples, key=lambda row: row[0]):
        day = when.date()
        by_day[day] = value       # time-ordered, so the last write is the last reading
        counts[day] = counts.get(day, 0) + 1

    sub_daily = any(count > 1 for count in counts.values())
    if complete_days_only and sub_daily and today_local in by_day:
        logger.info(
            "%s: dropping %s — %d readings so far, so the day's last is not yet "
            "the day's reading",
            gauge, today_local.isoformat(), counts[today_local],
        )
        del by_day[today_local]
    return sorted(by_day.items())


def _frame(
    rows: list[tuple[date, float]],
    *,
    spec: dict,
    is_forecast: int,
) -> pd.DataFrame:
    if not rows:
        return _empty_frame()
    return pd.DataFrame({
        "Date": [when for when, _ in rows],
        "stage": [value for _, value in rows],
        "unit": spec["unit"],
        "is_forecast": is_forecast,
        "source": spec["provider"],
        "attribution": spec["attribution"],
    })


# ---------------------------------------------------------------------------
# Layer 27 — NOAA NWPS (Mississippi)
# ---------------------------------------------------------------------------
def _nwps_samples(
    block: object, *, gauge: str, spec: dict, block_name: str
) -> list[tuple[datetime, float]]:
    """Decode one NWPS series block into (local instant, stage) pairs.

    A block whose `primaryUnits` is not the unit this gauge is declared in is
    discarded whole: NWPS carries stage *and* flow in one payload under
    `primary`/`secondary`, and reading a kcfs discharge as a stage is trap 2
    at a larger scale.
    """
    if not isinstance(block, dict):
        return []
    units = block.get("primaryUnits")
    if units != spec["unit"]:
        logger.warning(
            "%s: NWPS answered in %r but this gauge is declared in %r — "
            "discarding the block rather than storing a unit we did not ask for",
            gauge, units, spec["unit"],
        )
        return []

    zone = ZoneInfo(spec["timezone"])
    discards = _Discards(gauge, spec["unit"])
    out: list[tuple[datetime, float]] = []
    for point in block.get("data") or []:
        raw_time = point.get("validTime")
        raw_value = point.get("primary")
        if raw_time is None or raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            logger.warning("%s: unparseable stage %r at %s", gauge, raw_value, raw_time)
            continue
        if not _in_bounds(value, spec["unit"], discards):
            continue
        when = pd.to_datetime(raw_time, utc=True, errors="coerce")
        if pd.isna(when):
            logger.warning("%s: unparseable validTime %r", gauge, raw_time)
            continue
        out.append((when.to_pydatetime().astimezone(zone), value))
    discards.report(block_name)
    return out


def fetch_nwps_gauge(gauge: str, spec: dict) -> pd.DataFrame:
    """Observed + forecast stage for one NWPS gauge. Empty frame on failure."""
    payload = _get_json(
        NWPS_GAUGE_URL.format(gauge_id=spec["gauge_id"]),
        params=None,
        label=f"NWPS stageflow {spec['gauge_id']} ({gauge})",
    )
    if not isinstance(payload, dict):
        return _empty_frame()

    zone = ZoneInfo(spec["timezone"])
    today_local = datetime.now(timezone.utc).astimezone(zone).date()
    cutoff = today_local - timedelta(days=RIVER_NWPS_LOOKBACK_DAYS)

    observed = [
        row for row in _daily_last(
            _nwps_samples(
                payload.get("observed"), gauge=gauge, spec=spec, block_name="observed"
            ),
            gauge=gauge,
            today_local=today_local,
            complete_days_only=True,
        )
        if row[0] >= cutoff
    ]
    forecast = _daily_last(
        _nwps_samples(
            payload.get("forecast"), gauge=gauge, spec=spec, block_name="forecast"
        ),
        gauge=gauge,
        today_local=today_local,
        complete_days_only=False,
    )

    # Trap 3: the forecast trace overlaps the observed record, and a
    # (gauge, Date) upsert would let a model output overwrite a measurement.
    if observed:
        newest_observed = observed[-1][0]
        forecast = [row for row in forecast if row[0] > newest_observed]

    frame = pd.concat(
        [
            _frame(observed, spec=spec, is_forecast=0),
            _frame(forecast, spec=spec, is_forecast=1),
        ],
        ignore_index=True,
    )
    if frame.empty:
        logger.warning("%s: NWPS answered but carried no usable stage rows", gauge)
        return _empty_frame()

    logger.info(
        "%s: %d observed day(s) to %s, %d forecast day(s)",
        gauge, len(observed), observed[-1][0].isoformat() if observed else "n/a",
        len(forecast),
    )
    return frame


def fetch_nwps_gauges() -> dict[str, pd.DataFrame]:
    """Layer 27 — every NWPS gauge in the registry.

    A gauge that failed is absent from the result rather than present and
    empty, so `LAYER_MIN_KEYS['river_us']` sees the outage. A river has a
    level every day: half the gauges answering is our transport or a renamed
    LID, never a quiet day.
    """
    results: dict[str, pd.DataFrame] = {}
    for gauge, spec in RIVER_GAUGES_NWPS.items():
        frame = fetch_nwps_gauge(gauge, spec)
        if not frame.empty:
            results[gauge] = frame
    return results


# ---------------------------------------------------------------------------
# Layer 28 — Argentina INA (Paraná at Rosario)
# ---------------------------------------------------------------------------
def fetch_ina_gauge(gauge: str, spec: dict) -> pd.DataFrame:
    """Observed stage for one INA a5 series. Empty frame on failure.

    Observed only. INA's forecast trace for this station (series 3387)
    answered HTTP 200 with `[]` on every probe (2026-08-21), so the Paraná
    leg carries no forecast — the asymmetry with the Mississippi leg is a
    fact about the publisher and is rendered as one, never filled in.
    """
    zone = ZoneInfo(spec["timezone"])
    today_local = datetime.now(timezone.utc).astimezone(zone).date()
    start = today_local - timedelta(days=RIVER_INA_LOOKBACK_DAYS)

    payload = _get_json(
        INA_OBSERVATIONS_URL.format(series_id=spec["gauge_id"]),
        params={
            "timestart": start.isoformat(),
            # One day past today: the service treats the window as a
            # half-open range and today's 00:00 local reading sits at its edge.
            "timeend": (today_local + timedelta(days=1)).isoformat(),
        },
        label=f"INA series {spec['gauge_id']} ({gauge})",
    )
    if not isinstance(payload, list):
        # The a5 API answers an error with a JSON *object* under HTTP 200
        # (`{"title": "Mensaje de error", ...}`), so a non-list payload is a
        # failure that never raised.
        if payload is not None:
            logger.warning(
                "%s: INA answered HTTP 200 with %r, not an observation list",
                gauge, str(payload)[:200],
            )
        return _empty_frame()

    discards = _Discards(gauge, spec["unit"])
    samples: list[tuple[datetime, float]] = []
    for point in payload:
        if not isinstance(point, dict):
            continue
        raw_time = point.get("timestart")
        raw_value = point.get("valor")
        if raw_time is None or raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            logger.warning("%s: unparseable stage %r at %s", gauge, raw_value, raw_time)
            continue
        if not _in_bounds(value, spec["unit"], discards):
            continue
        when = pd.to_datetime(raw_time, utc=True, errors="coerce")
        if pd.isna(when):
            logger.warning("%s: unparseable timestart %r", gauge, raw_time)
            continue
        samples.append((when.to_pydatetime().astimezone(zone), value))
    discards.report("observed")

    rows = _daily_last(
        samples, gauge=gauge, today_local=today_local, complete_days_only=True
    )
    if not rows:
        logger.warning("%s: INA answered but carried no usable stage rows", gauge)
        return _empty_frame()

    logger.info(
        "%s: %d observed day(s) to %s", gauge, len(rows), rows[-1][0].isoformat()
    )
    return _frame(rows, spec=spec, is_forecast=0)


def fetch_ina_gauges() -> dict[str, pd.DataFrame]:
    """Layer 28 — every INA gauge in the registry."""
    results: dict[str, pd.DataFrame] = {}
    for gauge, spec in RIVER_GAUGES_INA.items():
        frame = fetch_ina_gauge(gauge, spec)
        if not frame.empty:
            results[gauge] = frame
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    for label, data in (
        ("Layer 27 (NWPS)", fetch_nwps_gauges()),
        ("Layer 28 (INA)", fetch_ina_gauges()),
    ):
        logger.info("=== %s ===", label)
        for gauge, df in data.items():
            observed = df[df["is_forecast"] == 0]
            latest = observed.iloc[-1] if not observed.empty else None
            logger.info(
                "  %s: %d rows, latest observed %s = %s %s",
                gauge, len(df),
                latest["Date"] if latest is not None else "n/a",
                latest["stage"] if latest is not None else "n/a",
                df.iloc[0]["unit"],
            )
