"""Cyclone hazard assessment — one rule set, one implementation (S2 #374, slice 3 #389).

A **hazard flag** is a dated warning attached to a rendered leg because a
tropical cyclone threatens a place that prices it. This module grades each
registered place (``config.PLACES``) against the forecast tracks and quadrant
wind radii Layers 34/35 stored from NOAA NHC and JTWC, then rolls the places
up to a ledger leg through ``LedgerLeg.place_ids``. The site's ledger chip,
block 06 and the briefing all call these functions, so they cannot disagree —
the role ``analysis/weather_alerts.py`` plays for weather.

Spec: ``docs/specs/cyclone-hazard-flags.md`` §5 (geometry and severity),
§6 (states and failure modes), §8.4 (sentences). Reference implementation:
``spike/footprint-weather/storms.py``; the Francine regression fixture pins
this module to the spike's replay numbers.

Pure functions over DataFrames and config: no SQL, no network, no clock.
``now`` is passed in by the caller (the page-generation time), never read here.

The six place states (§6.1):

    flag         inside a forecast wind radius within CYCLONE_LOOKAHEAD_HOURS
    watch        a storm of ≥ CYCLONE_WATCH_MIN_KT passes within CYCLONE_WATCH_KM,
                 outside its radii
    clear        the source was asked, answered, and no storm reaches the place
    not_covered  the place's basin has no publishable source — NEVER "no storm"
    stale        the reading exists but is too old to call the place clear
    failed       the source could not be read

A place with ``basin: "none"`` (inland) has no state at all: agency wind radii
are valid only over water, so it is never assessed (§4.1, §14.1).

Forecast hours count from each advisory's **synoptic time** (``synoptic_at``),
not its issue time: slice 1 established that NHC's tau 12 is valid at
synoptic + 12 h, three hours before issued + 12 h, and JTWC's T000 is the
synoptic position (``LAYERS.md`` Layers 34/35). The spec's §5.2 wrote
``issued_at``; this module follows the stored data. The *age* of an advisory
is still measured from ``issued_at`` — that is when the agency spoke.

Nothing here is a settlement, a price, or a national warning. JTWC is US
military guidance and every sentence built here says so.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd

from config import (
    CYCLONE_ADVISORY_MAX_AGE_HOURS,
    CYCLONE_CHECK_MAX_AGE_HOURS,
    CYCLONE_LOOKAHEAD_HOURS,
    CYCLONE_RADII_KT,
    CYCLONE_WARNING_MAX_TAU_H,
    CYCLONE_WATCH_KM,
    CYCLONE_WATCH_MIN_KT,
)

logger = logging.getLogger(__name__)

NM_TO_KM = 1.852
EARTH_RADIUS_KM = 6371.0

PLACE_STATES = ("flag", "watch", "clear", "not_covered", "stale", "failed")
SEVERITIES = ("alert", "warning", "info")
# States that must say why (enforcement by type, like the block envelope).
_REASON_REQUIRED = frozenset({"not_covered", "stale", "failed"})
# Leg roll-up: a leg is `partial` when its state is one of these and an exposed
# place could not be read.
_UNCOVERED_STATES = frozenset({"not_covered", "stale", "failed"})

QUADRANTS = ("ne", "se", "sw", "nw")
_SEVERITY_RANK = {"alert": 0, "warning": 1, "info": 2}

# §8.4 — the agencies' own vocabulary, never a national scale.
_NHC_CLASS_LABELS = {
    "TS": "Tropical Storm",
    "TD": "Tropical Depression",
    "STS": "Subtropical Storm",
    "STD": "Subtropical Depression",
    "PTC": "Potential Tropical Cyclone",
    "PC": "Post-tropical Cyclone",
}
# Saffir-Simpson in knots, R2 §2.1: category → lower bound.
_SAFFIR_SIMPSON_KT = ((5, 137), (4, 113), (3, 96), (2, 83), (1, 64))


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PlaceHazard:
    """One place's grade for the day (§6.2).

    ``reason`` is mandatory for ``not_covered`` / ``stale`` / ``failed`` and
    carries the clear-line text ("no active storm listed", or the nearest
    storm) for ``clear``. Times are UTC ISO-8601 text as stored; ``*_at``
    instants are struck from the advisory's synoptic time.
    """

    place_id: str
    short: str
    state: str
    severity: str | None = None
    band_kt: int | None = None
    source: str | None = None
    storm_id: str | None = None
    storm_name: str | None = None
    advisory: str | None = None
    issued_at: str | None = None
    synoptic_at: str | None = None
    first_arrival_tau_h: int | None = None
    first_arrival_at: str | None = None
    closest_km: int | None = None
    closest_tau_h: int | None = None
    closest_at: str | None = None
    # Closest approach while the storm is at least tropical-storm strength —
    # the number a `watch` is defined on (§5.3).
    closest_ts_km: int | None = None
    closest_ts_tau_h: int | None = None
    closest_ts_at: str | None = None
    vmax_kt: int | None = None
    storm_class_label: str | None = None
    basin_prefix: str | None = None
    advisory_stale: bool = False
    advisory_age_h: int | None = None
    checked_at: str | None = None
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.state not in PLACE_STATES:
            raise ValueError(f"PlaceHazard {self.place_id}: unknown state {self.state!r} (one of {PLACE_STATES})")
        if self.severity is not None and self.severity not in SEVERITIES:
            raise ValueError(f"PlaceHazard {self.place_id}: unknown severity {self.severity!r} (one of {SEVERITIES})")
        if self.state in _REASON_REQUIRED and not (self.reason or "").strip():
            raise ValueError(f"PlaceHazard {self.place_id}: state {self.state!r} requires a non-empty reason")
        if self.state == "flag" and (self.severity is None or self.band_kt not in CYCLONE_RADII_KT):
            raise ValueError(f"PlaceHazard {self.place_id}: a flag needs a severity and a band in {CYCLONE_RADII_KT}")
        if self.state == "watch" and self.severity != "info":
            raise ValueError(f"PlaceHazard {self.place_id}: a watch is always severity 'info'")


@dataclass(frozen=True)
class LegHazard:
    """A ledger leg's roll-up over its exposed places (§6.3)."""

    state: str
    severity: str | None
    partial: bool
    primary: PlaceHazard | None
    places: tuple[PlaceHazard, ...]
    uncovered: tuple[str, ...]
    text: str


@dataclass(frozen=True)
class HourlySample:
    """One interpolated hour of a forecast track (§5.2)."""

    tau_h: int
    lat: float
    lon: float
    vmax_kt: float
    # band → (ne, se, sw, nw) in nautical miles; an unpublished (NULL) radius is 0 here.
    radii: dict[int, tuple[float, float, float, float]] = field(default_factory=dict)


@dataclass
class StormApproach:
    """What one storm does to one place over the horizon (§5.3). Mutable scratch."""

    first_tau: dict[int, int] = field(default_factory=dict)
    closest_km: float | None = None
    closest_tau_h: int | None = None
    closest_ts_km: float | None = None
    closest_ts_tau_h: int | None = None

    @property
    def first_arrival_tau_h(self) -> int | None:
        return min(self.first_tau.values()) if self.first_tau else None


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    h = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin((l2 - l1) / 2) ** 2
    return EARTH_RADIUS_KM * 2 * math.asin(math.sqrt(h))


def initial_bearing_deg(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial great-circle bearing from point 1 to point 2, degrees clockwise from north."""
    p1, l1, p2, l2 = map(math.radians, (lat1, lon1, lat2, lon2))
    y = math.sin(l2 - l1) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(l2 - l1)
    return (math.degrees(math.atan2(y, x)) + 360.0) % 360.0


def quadrant(bearing_deg: float) -> int:
    """0 = NE, 1 = SE, 2 = SW, 3 = NW — the agencies' quadrant order."""
    return int(bearing_deg // 90) % 4


def _radii_of(row: Any, band: int) -> tuple[float, float, float, float]:
    """A row's published radii for ``band``; NULL → 0 nm (cannot flag anything)."""
    values = []
    for q in QUADRANTS:
        v = row[f"r{band}_{q}"]
        values.append(0.0 if v is None or pd.isna(v) else float(v))
    return (values[0], values[1], values[2], values[3])


def hourly_track(track: pd.DataFrame, lookahead_h: int = CYCLONE_LOOKAHEAD_HOURS) -> list[HourlySample]:
    """§5.2: one sample per whole hour, linear in lat / wind / radii, longitude
    the short way round; the final point appended; kept to ``lookahead_h``.

    ``track`` is one storm's ``cyclone_track_points`` rows. Hours count from
    the advisory's synoptic time and are not re-based to the read time.
    """
    if track.empty:
        return []
    rows = track.sort_values("tau_h").to_dict("records")
    taus = [int(r["tau_h"]) for r in rows]
    if len(set(taus)) != len(taus):
        raise ValueError(f"hourly_track: duplicate forecast hours {taus}")
    out: list[HourlySample] = []
    for a, b in zip(rows, rows[1:], strict=False):
        ta, tb = int(a["tau_h"]), int(b["tau_h"])
        span = tb - ta
        dlon = ((float(b["lon"]) - float(a["lon"]) + 540.0) % 360.0) - 180.0
        ra = {k: _radii_of(a, k) for k in CYCLONE_RADII_KT}
        rb = {k: _radii_of(b, k) for k in CYCLONE_RADII_KT}
        for h in range(ta, tb):
            f = (h - ta) / span
            radii = {
                k: (
                    ra[k][0] + f * (rb[k][0] - ra[k][0]),
                    ra[k][1] + f * (rb[k][1] - ra[k][1]),
                    ra[k][2] + f * (rb[k][2] - ra[k][2]),
                    ra[k][3] + f * (rb[k][3] - ra[k][3]),
                )
                for k in CYCLONE_RADII_KT
            }
            out.append(HourlySample(
                tau_h=h,
                lat=float(a["lat"]) + f * (float(b["lat"]) - float(a["lat"])),
                lon=_wrap_lon(float(a["lon"]) + f * dlon),
                vmax_kt=float(a["vmax_kt"]) + f * (float(b["vmax_kt"]) - float(a["vmax_kt"])),
                radii=radii,
            ))
    last = rows[-1]
    out.append(HourlySample(
        tau_h=int(last["tau_h"]), lat=float(last["lat"]), lon=_wrap_lon(float(last["lon"])),
        vmax_kt=float(last["vmax_kt"]), radii={k: _radii_of(last, k) for k in CYCLONE_RADII_KT},
    ))
    return [s for s in out if s.tau_h <= lookahead_h]


def _wrap_lon(lon: float) -> float:
    """Normalise to (−180, 180]."""
    wrapped = ((lon + 180.0) % 360.0) - 180.0
    return 180.0 if wrapped == -180.0 else wrapped


def approach(place_lat: float, place_lon: float, samples: list[HourlySample]) -> StormApproach:
    """§5.3: one storm's hourly samples against one place."""
    result = StormApproach()
    for s in samples:
        d = haversine_km(s.lat, s.lon, place_lat, place_lon)
        if result.closest_km is None or d < result.closest_km:
            result.closest_km, result.closest_tau_h = d, s.tau_h
        if s.vmax_kt >= CYCLONE_WATCH_MIN_KT and (result.closest_ts_km is None or d < result.closest_ts_km):
            result.closest_ts_km, result.closest_ts_tau_h = d, s.tau_h
        q = quadrant(initial_bearing_deg(s.lat, s.lon, place_lat, place_lon))
        for band in CYCLONE_RADII_KT:
            r = s.radii.get(band, (0.0, 0.0, 0.0, 0.0))[q]
            if r > 0 and d <= r * NM_TO_KM and band not in result.first_tau:
                result.first_tau[band] = s.tau_h
    return result


def grade(a: StormApproach) -> tuple[str, str, int | None] | None:
    """§5.4: ``(state, severity, band_kt)`` for one approach, or ``None`` for no hit."""
    if 64 in a.first_tau:
        return ("flag", "alert", 64)
    if a.first_tau:
        band = 50 if 50 in a.first_tau else 34
        first = a.first_arrival_tau_h
        assert first is not None
        return ("flag", "warning" if first <= CYCLONE_WARNING_MAX_TAU_H else "info", band)
    if a.closest_ts_km is not None and a.closest_ts_km <= CYCLONE_WATCH_KM:
        return ("watch", "info", None)
    return None


# ---------------------------------------------------------------------------
# Registry helpers
# ---------------------------------------------------------------------------
def active_places(places: dict[str, dict[str, Any]], today: date) -> dict[str, dict[str, Any]]:
    """§4.1: the places active on ``today`` (both bounds inclusive, ``None`` open-ended)."""
    out = {}
    for place_id, raw in places.items():
        start = _as_date(raw.get("effective_from"), place_id, "effective_from")
        end = _as_date(raw.get("effective_to"), place_id, "effective_to")
        if (start is None or start <= today) and (end is None or today <= end):
            out[place_id] = raw
    return out


def _as_date(value: Any, place_id: str, field_name: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"config.PLACES[{place_id!r}].{field_name} {value!r} is not an ISO date") from exc


def _parse_utc(text: Any, label: str) -> datetime:
    if text is None or (isinstance(text, float) and math.isnan(text)) or not str(text).strip():
        raise ValueError(f"{label}: missing timestamp")
    stamp = pd.Timestamp(str(text))
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    return stamp.tz_convert("UTC").to_pydatetime()


def _iso(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _text(value: Any) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return str(value)


def _int(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return int(value)


# ---------------------------------------------------------------------------
# Current storms and de-duplication (§2.1, §6.2 step 4)
# ---------------------------------------------------------------------------
def newest_status(status: pd.DataFrame) -> dict[str, pd.Series]:
    """Each source's newest ``cyclone_source_status`` row, by ``checked_at``."""
    out: dict[str, pd.Series] = {}
    if status is None or status.empty:
        return out
    for source, group in status.groupby("source"):
        newest = group.sort_values("checked_at").iloc[-1]
        out[str(source)] = newest
    return out


def current_storms(status: pd.DataFrame, storms: pd.DataFrame, layer_states: dict[str, str | None],
                   basins: dict[str, dict[str, Any]] | None = None) -> pd.DataFrame:
    """Every storm row from each source's newest successful read, JTWC
    duplicates of NHC storms dropped.

    A source contributes only when its layer is ``success`` and it has a
    status row: a failed layer's stored storms are yesterday's and must not
    be tested against today's places. De-duplication is by ``(basin_prefix,
    storm_number, season)`` against NHC's *current* list — never by the
    ``ep``/``cp`` prefix alone (K2 §6: Nolo kept its ``ep`` id at JTWC after
    NHC stopped issuing). With NHC not current, nothing is dropped.
    """
    if storms is None or storms.empty:
        return pd.DataFrame(columns=storms.columns if storms is not None else [])
    layers = _source_layers(basins)
    newest = newest_status(status)
    frames = []
    for source, row in newest.items():
        if layer_states.get(layers.get(source, "")) != "success":
            continue
        frames.append(storms[(storms["source"] == source) & (storms["checked_at"] == row["checked_at"])])
    if not frames:
        return storms.iloc[0:0]
    current = pd.concat(frames, ignore_index=True)
    nhc_keys = {
        (r["basin_prefix"], int(r["storm_number"]), int(r["season"]))
        for _, r in current[current["source"] == "NHC"].iterrows()
    }
    if not nhc_keys:
        return current
    duplicate = current.apply(
        lambda r: r["source"] == "JTWC"
        and (r["basin_prefix"], int(r["storm_number"]), int(r["season"])) in nhc_keys,
        axis=1,
    )
    dropped = current[duplicate]
    if not dropped.empty:
        logger.info("cyclone dedupe: dropping JTWC %s — NHC carries the same storm(s)",
                    sorted(dropped["storm_id"]))
    return current[~duplicate].reset_index(drop=True)


def _source_layers(basins: dict[str, dict[str, Any]] | None) -> dict[str, str]:
    """source → layer key, from the basin registry (default: the two known layers)."""
    layers = {"NHC": "cyclones_nhc", "JTWC": "cyclones_jtwc"}
    if basins:
        for basin in basins.values():
            if basin.get("source") and basin.get("layer"):
                layers[str(basin["source"])] = str(basin["layer"])
    return layers


def _storm_hemisphere(storm: pd.Series, basins: dict[str, dict[str, Any]]) -> str:
    """``N`` or ``S`` — by the basin registry's id prefix, else by hour-0 latitude."""
    prefix = str(storm["basin_prefix"])
    for basin in basins.values():
        if basin.get("id_prefix") == prefix and basin.get("hemisphere") in ("N", "S"):
            return str(basin["hemisphere"])
    if prefix == "sh":
        return "S"
    lat = storm.get("lat")
    if lat is not None and not pd.isna(lat) and float(lat) < 0:
        return "S"
    return "N"


def _advisory_age_h(storm: pd.Series) -> int:
    """Whole hours from the advisory's issue to the read that stored it."""
    issued = _parse_utc(storm["issued_at"], f"{storm['source']} {storm['storm_id']} issued_at")
    checked = _parse_utc(storm["checked_at"], f"{storm['source']} {storm['storm_id']} checked_at")
    return int((checked - issued).total_seconds() // 3600)


# ---------------------------------------------------------------------------
# The assessment (§6.2)
# ---------------------------------------------------------------------------
def assess_places(
    places: dict[str, dict[str, Any]],
    basins: dict[str, dict[str, Any]],
    status: pd.DataFrame,
    storms: pd.DataFrame,
    track: pd.DataFrame,
    layer_states: dict[str, str | None],
    now: datetime,
    *,
    layer_last_success: dict[str, str | None] | None = None,
) -> dict[str, PlaceHazard]:
    """Grade every exposed, active place. §6.2's decision order, first match wins.

    ``places`` is the registry already filtered to today (``active_places``);
    a place with ``basin: "none"`` is skipped and has no key in the result.
    ``layer_states`` is ``data_freshness.status`` per layer key — the same
    row the tier probe reads — so the site cannot call a source alive that
    the pipeline has already failed.
    """
    if now.tzinfo is None:
        raise ValueError("assess_places: `now` must be timezone-aware UTC")
    now = now.astimezone(timezone.utc)
    newest = newest_status(status)
    current = current_storms(status, storms, layer_states, basins)
    layers = _source_layers(basins)

    # Geometry once per current storm with a readable track.
    approaches_by_storm: list[tuple[pd.Series, list[HourlySample]]] = []
    for _idx, storm in current.iterrows():
        if storm["track_state"] != "ok":
            continue
        rows = track[
            (track["source"] == storm["source"])
            & (track["storm_id"] == storm["storm_id"])
            & (track["issued_at"] == storm["issued_at"])
        ]
        if rows.empty:
            raise ValueError(
                f"{storm['source']} {storm['storm_id']} ({storm['issued_at']}) is track_state 'ok' "
                "but has no cyclone_track_points rows — the store is inconsistent"
            )
        approaches_by_storm.append((storm, hourly_track(rows)))

    results: dict[str, PlaceHazard] = {}
    for place_id, raw in places.items():
        basin_key = raw["basin"]
        if basin_key == "none":
            continue
        short = str(raw.get("short") or place_id)
        basin = basins[basin_key]

        # 1. not_covered — the basin has no publishable source.
        if basin.get("source") is None:
            results[place_id] = PlaceHazard(place_id=place_id, short=short, state="not_covered",
                                            basin_prefix=basin.get("id_prefix"),
                                            reason=str(raw.get("basin_reason") or
                                                       f"{basin_key}: no publishable cyclone source"))
            continue
        source = str(basin["source"])
        layer = str(basin.get("layer") or layers.get(source, ""))

        # 2. failed — the layer is not `success`, or the source never answered.
        state = layer_states.get(layer)
        if state != "success" or source not in newest:
            last = (layer_last_success or {}).get(layer)
            if source not in newest:
                why = f"{source} has no cyclone_source_status row — the source has never answered"
            else:
                why = f"{source} layer {layer} is {state!r}, not success"
            if last:
                why += f"; last success {last}"
            results[place_id] = PlaceHazard(place_id=place_id, short=short, state="failed",
                                            basin_prefix=basin.get("id_prefix"), reason=why)
            continue
        status_row = newest[source]
        checked_at = str(status_row["checked_at"])

        # 3. stale — the newest check is too old to call anything clear.
        age_h = (now - _parse_utc(checked_at, f"{source} checked_at")).total_seconds() / 3600
        if age_h > CYCLONE_CHECK_MAX_AGE_HOURS:
            results[place_id] = PlaceHazard(place_id=place_id, short=short, state="stale",
                                            basin_prefix=basin.get("id_prefix"), checked_at=checked_at,
                                            reason=f"last checked {format_utc(checked_at)}")
            continue

        results[place_id] = _assess_against_storms(
            place_id, short, raw, basin, basins, current, approaches_by_storm, checked_at,
        )
    return results


def _assess_against_storms(
    place_id: str, short: str, raw: dict[str, Any], basin: dict[str, Any],
    basins: dict[str, dict[str, Any]], current: pd.DataFrame,
    approaches_by_storm: list[tuple[pd.Series, list[HourlySample]]], checked_at: str,
) -> PlaceHazard:
    """§6.2 step 4: a place against every current storm, both sources."""
    place_lat, place_lon = float(raw["lat"]), float(raw["lon"])
    prefix = basin.get("id_prefix")
    hits: list[tuple[tuple[int, int, float], PlaceHazard]] = []
    watches: list[tuple[float, PlaceHazard]] = []
    nearest: tuple[float, pd.Series] | None = None
    nearest_ts_km: float | None = None

    for storm, samples in approaches_by_storm:
        a = approach(place_lat, place_lon, samples)
        if a.closest_km is not None and (nearest is None or a.closest_km < nearest[0]):
            nearest = (a.closest_km, storm)
        if a.closest_ts_km is not None and (nearest_ts_km is None or a.closest_ts_km < nearest_ts_km):
            nearest_ts_km = a.closest_ts_km
        graded = grade(a)
        if graded is None:
            continue
        state, severity, band = graded
        hazard = _place_hazard_from(place_id, short, state, severity, band, storm, a, basins, checked_at)
        if state == "flag":
            assert hazard.first_arrival_tau_h is not None and hazard.closest_km is not None
            hits.append(((_SEVERITY_RANK[severity], hazard.first_arrival_tau_h, hazard.closest_km), hazard))
        else:
            assert hazard.closest_ts_km is not None
            watches.append((hazard.closest_ts_km, hazard))

    # 4a. flag — the worst hit; ties to the earlier arrival, then the closer approach.
    if hits:
        hits.sort(key=lambda item: item[0])
        return hits[0][1]

    own_basin = current[current["basin_prefix"] == prefix] if prefix else current.iloc[0:0]

    # 4b. failed — a listed storm in this basin with no readable track.
    absent = own_basin[own_basin["track_state"] == "absent"]
    if not absent.empty:
        s = absent.iloc[0]
        return PlaceHazard(place_id=place_id, short=short, state="failed", basin_prefix=prefix,
                           source=str(s["source"]), storm_id=str(s["storm_id"]), checked_at=checked_at,
                           reason=f"{s['source']} lists {s['storm_id']} but its track product is absent")

    # 4c. stale — a storm in this basin on an advisory older than its hemisphere's limit.
    for _, s in own_basin.iterrows():
        if s["track_state"] != "ok":
            continue
        age = _advisory_age_h(s)
        if age > CYCLONE_ADVISORY_MAX_AGE_HOURS[_storm_hemisphere(s, basins)]:
            name = _text(s["name"]) or str(s["storm_id"])
            return PlaceHazard(place_id=place_id, short=short, state="stale", basin_prefix=prefix,
                               source=str(s["source"]), storm_id=str(s["storm_id"]), storm_name=name,
                               issued_at=_text(s["issued_at"]), advisory_age_h=age, checked_at=checked_at,
                               reason=f"{name}: advisory {age} h old when read")

    # 4d. watch — the closest tropical-storm-strength pass inside the watch radius.
    if watches:
        watches.sort(key=lambda item: item[0])
        return watches[0][1]

    # 4e. clear — with the nearest storm when any is active anywhere.
    if nearest is None:
        reason = "no active storm listed"
    else:
        km, s = nearest
        reason = f"nearest storm {_text(s['name']) or s['storm_id']} ({s['source']}) {round(km):,} km away"
    return PlaceHazard(place_id=place_id, short=short, state="clear", basin_prefix=prefix,
                       closest_km=round(nearest[0]) if nearest else None,
                       closest_ts_km=round(nearest_ts_km) if nearest_ts_km is not None else None,
                       checked_at=checked_at, reason=reason)


def _place_hazard_from(
    place_id: str, short: str, state: str, severity: str, band: int | None, storm: pd.Series,
    a: StormApproach, basins: dict[str, dict[str, Any]], checked_at: str,
) -> PlaceHazard:
    source = str(storm["source"])
    prefix = str(storm["basin_prefix"])
    synoptic_text = _text(storm.get("synoptic_at")) or str(storm["issued_at"])
    synoptic = _parse_utc(synoptic_text, f"{source} {storm['storm_id']} synoptic_at")
    age = _advisory_age_h(storm)
    stale = age > CYCLONE_ADVISORY_MAX_AGE_HOURS[_storm_hemisphere(storm, basins)]
    vmax = _int(storm.get("vmax_kt"))
    first = a.first_arrival_tau_h if state == "flag" else None
    assert a.closest_km is not None and a.closest_tau_h is not None
    return PlaceHazard(
        place_id=place_id, short=short, state=state, severity=severity, band_kt=band,
        source=source, storm_id=str(storm["storm_id"]), storm_name=_text(storm.get("name")),
        advisory=_text(storm.get("advisory")), issued_at=str(storm["issued_at"]), synoptic_at=synoptic_text,
        first_arrival_tau_h=first,
        first_arrival_at=_iso(synoptic + timedelta(hours=first)) if first is not None else None,
        closest_km=round(a.closest_km), closest_tau_h=a.closest_tau_h,
        closest_at=_iso(synoptic + timedelta(hours=a.closest_tau_h)),
        closest_ts_km=round(a.closest_ts_km) if a.closest_ts_km is not None else None,
        closest_ts_tau_h=a.closest_ts_tau_h,
        closest_ts_at=(_iso(synoptic + timedelta(hours=a.closest_ts_tau_h))
                       if a.closest_ts_tau_h is not None else None),
        vmax_kt=vmax,
        storm_class_label=storm_class_label(source, prefix, _text(storm.get("classification")), vmax),
        basin_prefix=prefix, advisory_stale=stale, advisory_age_h=age, checked_at=checked_at,
    )


# ---------------------------------------------------------------------------
# Leg roll-up (§6.3)
# ---------------------------------------------------------------------------
def _place_rank(p: PlaceHazard) -> tuple[int, int, int, int]:
    """Worst first: alert flag, warning flag, info flag, watch, failed, stale, not_covered, clear."""
    if p.state == "flag":
        return (0, _SEVERITY_RANK[p.severity or "info"], p.first_arrival_tau_h or 0, p.closest_km or 0)
    order = {"watch": 1, "failed": 2, "stale": 3, "not_covered": 4, "clear": 5}
    return (order[p.state], 0, 0, p.closest_km or 0)


def leg_hazard(place_ids: tuple[str, ...], place_results: dict[str, PlaceHazard]) -> LegHazard | None:
    """Roll a leg's places up to one state (§6.3).

    A place absent from ``place_results`` is not exposed (inland, or not
    active today) and does not count. With no exposed place the leg has no
    hazard and ``None`` is returned — the only route to a silent row.
    """
    exposed = sorted((place_results[p] for p in place_ids if p in place_results), key=_place_rank)
    if not exposed:
        return None
    states = {p.state for p in exposed}
    if "flag" in states:
        state = "flag"
    elif "watch" in states:
        state = "watch"
    elif "failed" in states:
        state = "failed"
    elif "stale" in states:
        state = "stale"
    elif states == {"not_covered"}:
        state = "not_covered"
    else:
        state = "clear"
    primary = exposed[0]
    uncovered = tuple(p for p in place_ids if p in place_results and place_results[p].state in _UNCOVERED_STATES)
    partial = state in ("flag", "watch", "clear") and bool(uncovered)
    severity = primary.severity if state in ("flag", "watch") else None
    return LegHazard(
        state=state, severity=severity, partial=partial, primary=primary, places=tuple(exposed),
        uncovered=uncovered, text=_leg_text(state, primary, exposed, uncovered, partial),
    )


def _leg_text(state: str, primary: PlaceHazard, exposed: list[PlaceHazard],
              uncovered: tuple[str, ...], partial: bool) -> str:
    by_id = {p.place_id: p for p in exposed}
    if state == "flag":
        text = chip_text(primary)
    elif state == "watch":
        text = (f"{primary.storm_name or primary.storm_id} ({primary.source}): passes within "
                f"{primary.closest_ts_km} km of {primary.short} at {format_utc(primary.closest_ts_at)}, "
                "outside its forecast wind radii")
    elif state == "clear":
        covered = [p.short for p in exposed if p.state == "clear"]
        text = f"no storm within {CYCLONE_LOOKAHEAD_HOURS} h of {', '.join(covered)}"
    else:
        text = f"{primary.short}: {primary.reason}"
    if partial:
        for place_id in uncovered:
            p = by_id[place_id]
            label = {"not_covered": "not covered", "stale": "stale", "failed": "no reading"}[p.state]
            text += f" · {p.short} {label}"
    return text


# ---------------------------------------------------------------------------
# Sentences (§8.4) — the site and the briefing say the same thing
# ---------------------------------------------------------------------------
def format_utc(text: str | None) -> str:
    """``Thu 12 Sep 03:00Z`` — absolute UTC, never a bare lead time (§8.1)."""
    if text is None:
        return "—"
    return _parse_utc(text, "format_utc").strftime("%a %d %b %H:%MZ")


def format_check_time(text: str | None) -> str:
    """``19:04Z 6 Oct`` — the storms head's check stamp (§8.3)."""
    if text is None:
        return "— see below"
    when = _parse_utc(text, "format_check_time")
    return f"{when.strftime('%H:%MZ')} {when.day} {when.strftime('%b')}"


def storm_class_label(source: str, basin_prefix: str, classification: str | None, vmax_kt: int | None) -> str:
    """The agency's own vocabulary (§8.4). An unknown NHC code renders raw and is logged."""
    if source == "NHC":
        code = (classification or "").strip().upper()
        if code == "HU":
            if vmax_kt is None:
                return "Hurricane"
            for category, floor in _SAFFIR_SIMPSON_KT:
                if vmax_kt >= floor:
                    return f"Hurricane, category {category}"
            return "Hurricane"
        if code in _NHC_CLASS_LABELS:
            return _NHC_CLASS_LABELS[code]
        logger.warning("NHC classification %r is not a known code; rendering it raw", classification)
        return code or "Tropical Cyclone"
    if basin_prefix == "wp":
        if vmax_kt is None:
            return "Tropical Cyclone"
        if vmax_kt < 34:
            return "Tropical Depression"
        if vmax_kt < 64:
            return "Tropical Storm"
        if vmax_kt < 130:
            return "Typhoon"
        return "Super Typhoon"
    return "Tropical Cyclone"


def band_words(source: str, basin_prefix: str | None, band_kt: int) -> str:
    if band_kt == 34:
        return "tropical-storm-force winds"
    if band_kt == 50:
        return "50-kt winds"
    if band_kt == 64:
        if source == "NHC":
            return "hurricane-force winds"
        if basin_prefix == "wp":
            return "typhoon-force winds"
        return "64-kt winds"
    raise ValueError(f"band_words: unknown band {band_kt} kt (one of {CYCLONE_RADII_KT})")


def storm_label(p: PlaceHazard) -> str:
    """``Francine (NHC adv 10, Hurricane, category 1, 80 kt)``; no ``adv`` for JTWC."""
    parts = [str(p.source)]
    if p.advisory:
        parts[0] += f" adv {p.advisory}"
    if p.storm_class_label:
        parts.append(p.storm_class_label)
    if p.vmax_kt is not None:
        parts.append(f"{p.vmax_kt} kt")
    return f"{p.storm_name or p.storm_id} ({', '.join(parts)})"


def source_line(source: str | None) -> str:
    return "NHC forecast." if source == "NHC" else "JTWC guidance, not a national warning."


def flag_sentence(p: PlaceHazard) -> str:
    """§8.4 flag sentence. Times are absolute UTC struck from the synoptic time."""
    if p.state != "flag" or p.band_kt is None:
        raise ValueError(f"flag_sentence: {p.place_id} is {p.state!r}, not a flag")
    anchor = _parse_utc(p.synoptic_at or p.issued_at, "flag_sentence").strftime("%H:%M")
    issued = _parse_utc(p.issued_at, "flag_sentence").strftime("%H:%M")
    lead = (f"+{p.first_arrival_tau_h} h from the {anchor}Z synoptic time"
            + (f" of the {issued}Z advisory" if issued != anchor else " advisory"))
    text = (
        f"{p.short}: {storm_label(p)} — {band_words(str(p.source), p.basin_prefix, p.band_kt)} forecast from "
        f"{format_utc(p.first_arrival_at)} ({lead}); closest approach {p.closest_km} km at "
        f"{format_utc(p.closest_at)}. {source_line(p.source)}"
    )
    if p.severity == "info":
        text += " Three to five days out."
    if p.advisory_stale:
        text += f" Advisory was {p.advisory_age_h} h old when read."
    return text


def watch_sentence(p: PlaceHazard) -> str:
    if p.state != "watch":
        raise ValueError(f"watch_sentence: {p.place_id} is {p.state!r}, not a watch")
    text = (f"{p.short}: {storm_label(p)} forecast to pass within {p.closest_ts_km} km at "
            f"{format_utc(p.closest_ts_at)}, outside its forecast wind radii. {source_line(p.source)}")
    if p.advisory_stale:
        text += f" Advisory was {p.advisory_age_h} h old when read."
    return text


def chip_text(p: PlaceHazard) -> str:
    """The chip tooltip: ``Francine (NHC): tropical-storm-force winds at New Orleans from Thu 12 Sep 03:00Z``."""
    if p.state != "flag" or p.band_kt is None:
        raise ValueError(f"chip_text: {p.place_id} is {p.state!r}, not a flag")
    return (f"{p.storm_name or p.storm_id} ({p.source}): {band_words(str(p.source), p.basin_prefix, p.band_kt)} "
            f"at {p.short} from {format_utc(p.first_arrival_at)}")
