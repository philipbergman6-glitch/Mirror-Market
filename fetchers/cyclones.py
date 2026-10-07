"""
Layers 34 / 35 — tropical-cyclone forecast tracks and wind radii.

    33  NOAA NHC  — North Atlantic, East and Central Pacific
    34  JTWC      — West Pacific, North Indian, all Southern Hemisphere

Storm wind is tradeable weather for the river's reason: a hurricane over the
lower Mississippi or a typhoon off Qingdao stops the loading a cash bid
prices. These fetchers only *read* each agency's active-storm index and every
listed storm's forecast track and quadrant wind radii. Grading a port against
them is ``analysis/hazards.py`` (spec ``docs/specs/cyclone-hazard-flags.md``
§5–§6), never this module.

Each fetcher returns exactly three frames — ``status``, ``storms``, ``track``
— and always a one-row ``status`` when its index answered. "Asked, none
active" is a real answer: the clear reading of a port depends on it, so a
quiet basin must stamp ``last_success`` rather than read as an outage.

WHAT THIS IS NOT. Not a price and not a warning. Winds are knots and radii
nautical miles exactly as published; nothing passes through ``to_usd_mt``.
JTWC is US military guidance, not the WMO regional centre for any basin, and
its attribution says so on every stored row.

Traps this module exists to survive (``LAYERS.md`` Layers 34/35):

1.  **NHC's DBF coordinates are whole degrees.** The ``LAT``/``LON`` attribute
    columns of the 5-day points shapefile are rounded; the position is the
    shape geometry (``shape.points[0]``, which is ``(lon, lat)``).
2.  **The NHC JSON has drifted from its reference PDF.** Keys are camelCase
    and ``intensity`` is a string. Parsed defensively; a shape change raises.
3.  **JTWC answers 403, not 404, for a product that does not exist.** Product
    names come from ``jtwc.rss`` — matched on the ``www.metoc.navy.mil`` URLs
    in the feed text, because its ``<link>`` values point at an S3 host that
    itself answers 403 — and a 403 on a listed product is stored as
    ``track_state = 'absent'``, a distinct state, never as a failure.
4.  **A missing radius is not a zero radius.** NHC forecasts 64-kt radii to
    72 h only, and neither agency publishes a band above the storm's wind. An
    unpublished band is NULL; a published ``0`` stays ``0`` (invariant 2).
5.  **``_latest`` is an alias.** The advisory number inside both NHC zips is
    asserted against the index's ``forecastTrack.advNum``, or a half-published
    advisory pairs a new index with an old track.
6.  **Forecast hours count from the synoptic time, not the issue time.** NHC's
    tau 12 is valid at synoptic + 12 h — three hours before issued + 12 h —
    and JTWC's T000 is the synoptic position. Both instants are stored
    (``synoptic_at``, ``issued_at``) so an absolute time is never struck off
    the wrong clock.
7.  **One storm id, two formats.** NHC ``al062024`` (four-digit year), JTWC
    ``wp2626`` (two-digit). Both normalise to a four-digit ``season``; an id
    outside the ATCF basin codes raises, naming itself.

Neither source needs a key.
"""

from __future__ import annotations

import io
import json
import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import shapefile

from config import (
    CYCLONE_ATTRIBUTION,
    CYCLONE_ID_PREFIXES,
    CYCLONE_JTWC_INDEX_URL,
    CYCLONE_JTWC_PRODUCT_URL,
    CYCLONE_NHC_INDEX_URL,
    CYCLONE_NHC_PRODUCT_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# JTWC's HTML pages refuse a short User-Agent (R2 §2.2); its products and the
# RSS do not, but the one header keeps both sources on the same footing.
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 Mirror-Market/1.0"
    ),
}

BANDS = (34, 50, 64)
QUADRANTS = ("ne", "se", "sw", "nw")
RADIUS_COLUMNS = tuple(f"r{band}_{quadrant}" for band in BANDS for quadrant in QUADRANTS)

STATUS_COLUMNS = (
    "source", "Date", "checked_at", "storms_listed", "storms_parsed",
    "products_absent", "index_last_modified", "attribution",
)
STORM_COLUMNS = (
    "source", "storm_id", "issued_at", "synoptic_at", "checked_at",
    "basin_prefix", "storm_number", "season", "name", "classification",
    "advisory", "lat", "lon", "vmax_kt", "max_tau_h", "track_state", "attribution",
)
TRACK_COLUMNS = (
    "source", "storm_id", "issued_at", "tau_h", "lat", "lon", "vmax_kt",
    *RADIUS_COLUMNS, "is_forecast",
)

_ID_SHAPES = {
    # source → (pattern, digits in the year)
    "NHC": re.compile(r"([a-z]{2})(\d{2})(\d{4})"),
    "JTWC": re.compile(r"([a-z]{2})(\d{2})(\d{2})"),
}

_JTWC_PRODUCT_RE = re.compile(r"https://www\.metoc\.navy\.mil/jtwc/products/(\w+)\.tcw")
_JTWC_LINE1_RE = re.compile(r"WT[A-Z]{2}\d{2}\s+[A-Z]{4}\s+(\d{2})(\d{2})(\d{2})\s*$")
_JTWC_LINE3_RE = re.compile(r"(\d{10})\s+(\d{2})([A-Z])\s+(\S+)\s+(\d{3})\b")
_JTWC_TRACK_START_RE = re.compile(r"T\d{3}\b")
_JTWC_TRACK_RE = re.compile(r"T(\d{3}) (\d{3})([NS]) (\d{4})([EW]) (\d{3})(.*)$")
_JTWC_RADII_RE = re.compile(
    r"R(\d{3}) (\d{3}) NE QD (\d{3}) SE QD (\d{3}) SW QD (\d{3}) NW QD"
)


class CycloneSourceError(ValueError):
    """An agency could not be read — transport, or a shape we do not know.

    Raised, never swallowed: ``main._run_dict_layer`` records the layer
    ``failed``, its ``last_success`` is preserved, and the 20:40 UTC retry
    fires. A half-read basin must never stamp a fresh success (invariant 1).
    """


@dataclass(frozen=True)
class NhcListing:
    """One storm as ``CurrentStorms.json`` lists it."""

    storm_id: str
    name: str | None
    classification: str
    intensity_kt: int
    advisory: str
    issued_at: str      # ISO UTC, the forecast-track product's issuance
    issued: datetime


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _iso(when: datetime) -> str:
    return when.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _request(url: str, label: str, *, absent_on_403: bool) -> requests.Response | None:
    """GET with the shared retry policy; raise when the source never answers.

    ``absent_on_403`` is JTWC's product contract (trap 3): S3 answers a key it
    does not hold with 403 AccessDenied, deterministically, so it is returned
    as None at once rather than retried — "no such product", not "blocked".
    """
    failure = "no attempt made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Requesting %s (attempt %d) ...", label, attempt)
            resp = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as exc:
            failure = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                return resp
            if resp.status_code == 403 and absent_on_403:
                return None
            failure = f"HTTP {resp.status_code}"
        logger.warning("Attempt %d/%d failed for %s: %s", attempt, MAX_RETRIES, label, failure)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise CycloneSourceError(f"{label}: no usable answer after {MAX_RETRIES} attempts ({failure})")


def _get(url: str, label: str) -> requests.Response:
    """An index or NHC product: anything but HTTP 200 fails the layer."""
    answer = _request(url, label, absent_on_403=False)
    if answer is None:  # unreachable: only absent_on_403 yields None
        raise CycloneSourceError(f"{label}: no answer")
    return answer


# ---------------------------------------------------------------------------
# Shared parsing
# ---------------------------------------------------------------------------
def parse_storm_id(storm_id: str, source: str) -> tuple[str, int, int]:
    """``(basin_prefix, storm_number, season)`` from an agency storm id.

    Trap 7: NHC carries a four-digit year, JTWC two. A prefix outside the ATCF
    basin codes raises rather than landing as a storm no basin claims.
    """
    shape = _ID_SHAPES.get(source)
    if shape is None:
        raise CycloneSourceError(f"unknown cyclone source {source!r} for storm id {storm_id!r}")
    match = shape.fullmatch(storm_id) if isinstance(storm_id, str) else None
    if match is None:
        raise CycloneSourceError(
            f"{source} storm id {storm_id!r} is not in that agency's id shape"
        )
    prefix, number, year = match.groups()
    if prefix not in CYCLONE_ID_PREFIXES:
        raise CycloneSourceError(
            f"{source} storm id {storm_id!r}: basin prefix {prefix!r} is not one of "
            f"{CYCLONE_ID_PREFIXES} — a new basin code must be added deliberately"
        )
    season = int(year) if len(year) == 4 else 2000 + int(year)
    return prefix, int(number), season


def parse_intensity(value: object) -> int:
    """NHC's ``intensity``: a string of knots ("80"), per the live file (trap 2)."""
    if isinstance(value, str) and re.fullmatch(r"\s*\d{1,3}\s*", value):
        return int(value)
    raise CycloneSourceError(f"NHC intensity {value!r} is not a whole-knot string")


def _advisory(value: object, label: str) -> str:
    """An advisory number without its zero padding: "010", "10" and 10 are one."""
    text = str(value).strip() if isinstance(value, (str, int)) and not isinstance(value, bool) else ""
    # NHC's index writes intermediate advisories in lower case ("004a");
    # the zip's own metadata upper-cases the same letter. One spelling here.
    match = re.fullmatch(r"0*(\d+)([A-Za-z]?)", text)
    if match is None:
        raise CycloneSourceError(f"{label}: advisory number {value!r} is not readable")
    return match.group(1) + match.group(2).upper()


def _int_value(value: object, field: str, label: str) -> int:
    if isinstance(value, bool):
        raise CycloneSourceError(f"{label}: {field} {value!r} is not a whole number")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and re.fullmatch(r"\s*-?\d+\s*", value):
        return int(value)
    raise CycloneSourceError(f"{label}: {field} {value!r} is not a whole number")


def _new_run(source: str, checked_at: datetime, last_modified: str | None,
                  listed: int) -> dict[str, Any]:
    return {
        "source": source,
        "checked_at": checked_at,
        "last_modified": last_modified,
        "listed": listed,
        "storms": [],
        "track": [],
        "absent": 0,
    }


def _frames(run: dict[str, Any]) -> dict[str, pd.DataFrame]:
    source = run["source"]
    storms = run["storms"]
    status = pd.DataFrame([{
        "source": source,
        "Date": run["checked_at"].date().isoformat(),
        "checked_at": _iso(run["checked_at"]),
        "storms_listed": run["listed"],
        "storms_parsed": sum(1 for row in storms if row["track_state"] == "ok"),
        "products_absent": run["absent"],
        "index_last_modified": run["last_modified"],
        "attribution": CYCLONE_ATTRIBUTION[source],
    }], columns=list(STATUS_COLUMNS))
    return {
        "status": status,
        "storms": pd.DataFrame(storms, columns=list(STORM_COLUMNS)),
        "track": pd.DataFrame(run["track"], columns=list(TRACK_COLUMNS)),
    }


def _storm_row(
    source: str, storm_id: str, *, issued_at: str, synoptic_at: str | None,
    checked_at: datetime, name: str | None, classification: str | None,
    advisory: str | None, points: list[dict], track_state: str,
) -> dict[str, Any]:
    prefix, number, season = parse_storm_id(storm_id, source)
    hour0 = points[0] if points else None
    return {
        "source": source,
        "storm_id": storm_id,
        "issued_at": issued_at,
        "synoptic_at": synoptic_at,
        "checked_at": _iso(checked_at),
        "basin_prefix": prefix,
        "storm_number": number,
        "season": season,
        "name": name,
        "classification": classification,
        "advisory": advisory,
        "lat": hour0["lat"] if hour0 else None,
        "lon": hour0["lon"] if hour0 else None,
        "vmax_kt": hour0["vmax_kt"] if hour0 else None,
        "max_tau_h": points[-1]["tau_h"] if points else None,
        "track_state": track_state,
        "attribution": CYCLONE_ATTRIBUTION[source],
    }


def _track_rows(source: str, storm_id: str, issued_at: str,
                points: dict[int, dict[str, Any]], label: str) -> list[dict]:
    if 0 not in points:
        raise CycloneSourceError(f"{label}: the track has no hour-0 point")
    rows = []
    for tau in sorted(points):
        point = points[tau]
        row: dict[str, Any] = {
            "source": source,
            "storm_id": storm_id,
            "issued_at": issued_at,
            "tau_h": tau,
            "lat": point["lat"],
            "lon": point["lon"],
            "vmax_kt": point["vmax_kt"],
        }
        radii = point["radii"]
        for band in BANDS:
            published = radii.get(band)
            for index, quadrant in enumerate(QUADRANTS):
                # Trap 4: an unpublished band is NULL, never 0.
                row[f"r{band}_{quadrant}"] = published[index] if published is not None else None
        row["is_forecast"] = 0 if tau == 0 else 1
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Layer 34 — NOAA NHC
# ---------------------------------------------------------------------------
def _parse_issuance(value: object, label: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise CycloneSourceError(f"{label}: issuance {value!r} is not a timestamp")
    try:
        stamp = pd.Timestamp(value)
    except (ValueError, TypeError) as exc:
        raise CycloneSourceError(f"{label}: issuance {value!r} is not a timestamp") from exc
    if stamp.tzinfo is None:
        raise CycloneSourceError(f"{label}: issuance {value!r} carries no time zone")
    return stamp.tz_convert("UTC").to_pydatetime()


def parse_nhc_index(text: str) -> list[NhcListing]:
    """Read ``CurrentStorms.json``. An empty ``activeStorms`` is a valid answer."""
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CycloneSourceError("NHC index is not JSON") from exc
    if not isinstance(payload, dict) or not isinstance(payload.get("activeStorms"), list):
        raise CycloneSourceError(
            "NHC index has no `activeStorms` list — the file's shape has changed"
        )

    listings: list[NhcListing] = []
    seen: set[str] = set()
    for storm in payload["activeStorms"]:
        if not isinstance(storm, dict):
            raise CycloneSourceError(f"NHC index lists a non-object storm: {storm!r:.80}")
        storm_id = storm.get("id")
        label = f"NHC index storm {storm_id!r}"
        if not isinstance(storm_id, str):
            raise CycloneSourceError(f"{label}: the storm id is not text")
        parse_storm_id(storm_id, "NHC")  # raises on an unknown id shape or basin
        if storm_id in seen:
            raise CycloneSourceError(f"{label} is listed twice")
        seen.add(storm_id)

        track = storm.get("forecastTrack")
        radii = storm.get("forecastWindRadiiGIS")
        if not isinstance(track, dict) or not isinstance(radii, dict):
            raise CycloneSourceError(
                f"{label} carries no forecastTrack / forecastWindRadiiGIS entry"
            )
        advisory = _advisory(track.get("advNum"), label)
        if _advisory(radii.get("advNum"), label) != advisory:
            raise CycloneSourceError(
                f"{label}: track advisory {track.get('advNum')!r} and radii advisory "
                f"{radii.get('advNum')!r} disagree — a half-published advisory"
            )
        classification = storm.get("classification")
        if not isinstance(classification, str) or not classification.strip():
            raise CycloneSourceError(f"{label}: classification {classification!r} is not a code")
        name = storm.get("name")
        if name is not None and not isinstance(name, str):
            raise CycloneSourceError(f"{label}: name {name!r} is not text")
        issued = _parse_issuance(track.get("issuance"), label)
        listings.append(NhcListing(
            storm_id=storm_id,
            name=(name.strip() or None) if isinstance(name, str) else None,
            classification=classification.strip(),
            intensity_kt=parse_intensity(storm.get("intensity")),
            advisory=advisory,
            issued_at=_iso(issued),
            issued=issued,
        ))
    return listings


def _read_shapefile(blob: bytes, suffix: str, label: str) -> tuple[list[str], list[Any]]:
    """``(field names, shape records)`` of the one member ending ``suffix``."""
    try:
        archive = zipfile.ZipFile(io.BytesIO(blob))
        members = [name for name in archive.namelist() if name.lower().endswith(suffix)]
        if len(members) != 1:
            raise CycloneSourceError(
                f"{label}: expected one *{suffix} member, found {members or 'none'}"
            )
        stem = members[0][:-4]
        reader = shapefile.Reader(
            shp=io.BytesIO(archive.read(stem + ".shp")),
            dbf=io.BytesIO(archive.read(stem + ".dbf")),
        )
        names = [field[0] for field in reader.fields[1:]]
        records = list(reader.iterShapeRecords())
    except CycloneSourceError:
        raise
    except Exception as exc:  # a truncated zip, a missing .dbf, a corrupt shape
        raise CycloneSourceError(f"{label}: unreadable shapefile archive ({exc})") from exc
    return names, records


def _require_fields(names: list[str], required: tuple[str, ...], label: str) -> None:
    missing = [field for field in required if field not in names]
    if missing:
        raise CycloneSourceError(f"{label}: shapefile is missing fields {missing}")


def _nhc_synoptic(validtime: object, issued: datetime, label: str) -> datetime:
    """Resolve the points file's tau-0 ``VALIDTIME`` ("11/0000") to an instant.

    The field carries day and time only. The synoptic time is the latest one
    at or before the issue time, so it is that day in the issue's month or the
    month before, within a day of issue.
    """
    match = re.fullmatch(r"(\d{2})/(\d{2})(\d{2})", str(validtime).strip())
    if match is None:
        raise CycloneSourceError(f"{label}: tau-0 VALIDTIME {validtime!r} is not DD/HHMM")
    day, hour, minute = (int(part) for part in match.groups())
    for back in (0, 1):
        candidate = (issued - timedelta(days=back)).date()
        if candidate.day != day:
            continue
        when = datetime(candidate.year, candidate.month, candidate.day, hour, minute,
                        tzinfo=timezone.utc)
        if issued - timedelta(hours=24) <= when <= issued:
            return when
    raise CycloneSourceError(
        f"{label}: tau-0 VALIDTIME {validtime!r} is not within a day before issue {_iso(issued)}"
    )


def parse_nhc_products(
    listing: NhcListing, five_day_zip: bytes, radii_zip: bytes, *, checked_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """One NHC storm: the 5-day points shapefile joined to its forecast radii."""
    label = f"NHC {listing.storm_id} advisory {listing.advisory}"

    names, records = _read_shapefile(five_day_zip, "_pts.shp", f"{label} 5-day track")
    _require_fields(names, ("ADVISNUM", "TAU", "MAXWIND", "VALIDTIME"), label)
    points: dict[int, dict[str, Any]] = {}
    synoptic: datetime | None = None
    for item in records:
        attrs = dict(zip(names, item.record, strict=True))
        if _advisory(attrs["ADVISNUM"], label) != listing.advisory:
            raise CycloneSourceError(
                f"{label}: the 5-day track zip holds advisory {attrs['ADVISNUM']!r} — "
                "the `_latest` alias has not caught up with the index"
            )
        if item.shape.shapeType != shapefile.POINT or len(item.shape.points) != 1:
            raise CycloneSourceError(f"{label}: a track record is not a single point")
        tau = _int_value(attrs["TAU"], "TAU", label)
        if tau in points:
            raise CycloneSourceError(f"{label}: TAU {tau} appears twice in the track")
        # Trap 1: the geometry, never the rounded LAT/LON attributes.
        lon, lat = item.shape.points[0]
        points[tau] = {
            "lat": float(lat),
            "lon": float(lon),
            "vmax_kt": _int_value(attrs["MAXWIND"], "MAXWIND", label),
            "radii": {},
        }
        if tau == 0:
            synoptic = _nhc_synoptic(attrs["VALIDTIME"], listing.issued, label)

    names, records = _read_shapefile(radii_zip, "_forecastradii.shp", f"{label} wind radii")
    _require_fields(names, ("RADII", "TAU", "ADVNUM", "SYNOPTIME", "NE", "SE", "SW", "NW"), label)
    radii_synoptic: set[str] = set()
    for item in records:
        attrs = dict(zip(names, item.record, strict=True))
        if _advisory(attrs["ADVNUM"], label) != listing.advisory:
            raise CycloneSourceError(
                f"{label}: the wind-radii zip holds advisory {attrs['ADVNUM']!r} — "
                "the `_latest` alias has not caught up with the index"
            )
        band = _int_value(attrs["RADII"], "RADII", label)
        tau = _int_value(attrs["TAU"], "TAU", label)
        if band not in BANDS:
            raise CycloneSourceError(f"{label}: unknown wind-radius band {band} kt")
        if tau not in points:
            raise CycloneSourceError(f"{label}: wind radii at TAU {tau}, which the track does not carry")
        if band in points[tau]["radii"]:
            raise CycloneSourceError(f"{label}: {band}-kt radii at TAU {tau} appear twice")
        points[tau]["radii"][band] = tuple(
            float(_int_value(attrs[q.upper()], q.upper(), label)) for q in QUADRANTS
        )
        radii_synoptic.add(str(attrs["SYNOPTIME"]).strip())

    if synoptic is None:
        raise CycloneSourceError(f"{label}: the track has no hour-0 point")
    if radii_synoptic and radii_synoptic != {synoptic.strftime("%Y%m%d%H")}:
        raise CycloneSourceError(
            f"{label}: radii synoptic time {sorted(radii_synoptic)} disagrees with the "
            f"track's {synoptic.strftime('%Y%m%d%H')}"
        )

    track = _track_rows("NHC", listing.storm_id, listing.issued_at, points, label)
    storm = _storm_row(
        "NHC", listing.storm_id,
        issued_at=listing.issued_at, synoptic_at=_iso(synoptic), checked_at=checked_at,
        name=listing.name, classification=listing.classification,
        advisory=listing.advisory, points=track, track_state="ok",
    )
    return storm, track


def fetch_nhc_storms() -> dict[str, pd.DataFrame]:
    """Layer 34 — every storm NHC lists, read whole or not at all.

    A listed storm whose zip is missing, unreadable or a different advisory
    from the index raises: a half-read basin must not stamp ``last_success``.
    """
    checked_at = _now_utc()
    index = _get(CYCLONE_NHC_INDEX_URL, "NHC CurrentStorms.json")
    listings = parse_nhc_index(index.text)
    run = _new_run("NHC", checked_at, index.headers.get("Last-Modified"), len(listings))

    for listing in listings:
        products = {}
        for product in ("5day", "fcst"):
            products[product] = _get(
                CYCLONE_NHC_PRODUCT_URL.format(storm_id=listing.storm_id, product=product),
                f"NHC {listing.storm_id} {product} zip",
            ).content
        storm, track = parse_nhc_products(
            listing, products["5day"], products["fcst"], checked_at=checked_at,
        )
        run["storms"].append(storm)
        run["track"].extend(track)
        logger.info(
            "NHC %s %s (%s): advisory %s, %d kt at hour 0, track to %d h",
            listing.storm_id, listing.name, listing.classification, listing.advisory,
            storm["vmax_kt"], storm["max_tau_h"],
        )

    logger.info("NHC: %d storm(s) listed, %d read", len(listings), len(run["storms"]))
    return _frames(run)


# ---------------------------------------------------------------------------
# Layer 35 — JTWC
# ---------------------------------------------------------------------------
def parse_jtwc_index(text: str) -> list[str]:
    """Storm ids from ``jtwc.rss``. A feed listing no product is a valid answer.

    Trap 3: ids come from the ``www.metoc.navy.mil/.../{id}.tcw`` URLs in the
    feed text (inside CDATA), never from the ``<link>`` values, whose S3 host
    answers 403.
    """
    try:
        # Parsed only to prove the answer is the feed and not an error page;
        # the ids are matched on the raw text below.
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError as exc:
        raise CycloneSourceError(f"JTWC RSS is not XML ({exc})") from exc
    if root.tag != "rss" or root.find("channel") is None:
        raise CycloneSourceError(f"JTWC RSS has an unexpected root <{root.tag}>")
    ids = sorted(set(_JTWC_PRODUCT_RE.findall(text)))
    for storm_id in ids:
        parse_storm_id(storm_id, "JTWC")
    return ids


def _jtwc_issued(day: int, hour: int, minute: int, synoptic: datetime, label: str) -> datetime:
    """The WTPN header's DDHHMM: the first such instant at or after synoptic."""
    for ahead in (0, 1):
        candidate = (synoptic + timedelta(days=ahead)).date()
        if candidate.day != day:
            continue
        when = datetime(candidate.year, candidate.month, candidate.day, hour, minute,
                        tzinfo=timezone.utc)
        if synoptic <= when <= synoptic + timedelta(hours=24):
            return when
    raise CycloneSourceError(
        f"{label}: header issue time {day:02d}{hour:02d}{minute:02d} is not within a day "
        f"after synoptic {_iso(synoptic)}"
    )


def parse_jtwc_tcw(
    storm_id: str, text: str, *, checked_at: datetime,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """One JTWC storm from its JMV 3.0 ``.tcw`` product.

    Line 1 is the WMO header (``WTPN53 PGTW 071500`` — issued 07th 15:00Z),
    line 3 the storm header (``2026100712 27W KOGUMA 011 ...`` — synoptic
    time, storm number, name, warning number), then one ``T`` line per
    forecast hour: ``T012 164N 1610E 055 R050 030 NE QD ... R034 ...``.
    """
    label = f"JTWC {storm_id}"
    _, number, _ = parse_storm_id(storm_id, "JTWC")
    lines = text.replace("\r\n", "\n").replace("\r", "\n").split("\n")

    line1 = _JTWC_LINE1_RE.match(lines[0].strip()) if lines else None
    line3 = _JTWC_LINE3_RE.match(lines[2].strip()) if len(lines) > 2 else None
    if line1 is None or line3 is None:
        raise CycloneSourceError(
            f"{label}: unknown header shape {lines[:3]!r:.160} — the JMV 3.0 format has changed"
        )
    synoptic_text, header_number, _basin_letter, name, _warning = line3.groups()
    if int(header_number) != number:
        raise CycloneSourceError(
            f"{label}: the product header names storm {header_number}{_basin_letter}, "
            f"not number {number:02d}"
        )
    try:
        synoptic = datetime.strptime(synoptic_text, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise CycloneSourceError(f"{label}: synoptic time {synoptic_text!r} is not a date") from exc
    day, hour, minute = (int(part) for part in line1.groups())
    issued = _jtwc_issued(day, hour, minute, synoptic, label)

    points: dict[int, dict[str, Any]] = {}
    for line in lines[3:]:
        if line.strip() == "AMP":  # the free-text bulletin follows; no track lines there
            break
        if not _JTWC_TRACK_START_RE.match(line):
            continue
        match = _JTWC_TRACK_RE.match(line.rstrip())
        if match is None:
            raise CycloneSourceError(f"{label}: track line in an unknown shape: {line.strip()[:60]!r}")
        tau_text, lat_text, ns, lon_text, ew, vmax_text, rest = match.groups()
        tau = int(tau_text)
        if tau in points:
            raise CycloneSourceError(f"{label}: T{tau:03d} appears twice")
        radii: dict[int, tuple[float, ...]] = {}
        for group in _JTWC_RADII_RE.findall(rest):
            band = int(group[0])
            if band not in BANDS:
                raise CycloneSourceError(f"{label}: T{tau:03d} carries an unknown {band}-kt band")
            if band in radii:
                raise CycloneSourceError(f"{label}: T{tau:03d} carries the {band}-kt band twice")
            radii[band] = tuple(float(value) for value in group[1:])
        leftover = _JTWC_RADII_RE.sub("", rest).strip()
        if leftover:
            raise CycloneSourceError(f"{label}: T{tau:03d} has unread text {leftover[:40]!r}")
        points[tau] = {
            "lat": int(lat_text) / 10 * (1 if ns == "N" else -1),
            "lon": int(lon_text) / 10 * (1 if ew == "E" else -1),
            "vmax_kt": int(vmax_text),
            "radii": radii,
        }
    if not points:
        raise CycloneSourceError(f"{label}: no track lines parsed — the product's shape has changed")

    issued_at = _iso(issued)
    track = _track_rows("JTWC", storm_id, issued_at, points, label)
    storm = _storm_row(
        "JTWC", storm_id,
        issued_at=issued_at, synoptic_at=_iso(synoptic), checked_at=checked_at,
        name=name, classification=None, advisory=None, points=track, track_state="ok",
    )
    return storm, track


def fetch_jtwc_storms() -> dict[str, pd.DataFrame]:
    """Layer 35 — every storm the JTWC feed lists.

    De-duplication against NHC is the assessment's job (spec §2.1), so every
    storm read here is stored, East/Central Pacific duplicates included.
    """
    checked_at = _now_utc()
    index = _get(CYCLONE_JTWC_INDEX_URL, "JTWC jtwc.rss")
    storm_ids = parse_jtwc_index(index.text)
    run = _new_run("JTWC", checked_at, index.headers.get("Last-Modified"), len(storm_ids))

    for storm_id in storm_ids:
        answer = _request(
            CYCLONE_JTWC_PRODUCT_URL.format(storm_id=storm_id),
            f"JTWC {storm_id}.tcw",
            absent_on_403=True,
        )
        if answer is None:
            # Listed but absent. No advisory time was read, so the run's own
            # check time keys the row (spec §3.2); position and wind stay NULL.
            logger.warning("JTWC lists %s but its .tcw answered 403 — stored as absent", storm_id)
            run["absent"] += 1
            run["storms"].append(_storm_row(
                "JTWC", storm_id,
                issued_at=_iso(checked_at), synoptic_at=None, checked_at=checked_at,
                name=None, classification=None, advisory=None, points=[],
                track_state="absent",
            ))
            continue
        storm, track = parse_jtwc_tcw(storm_id, answer.text, checked_at=checked_at)
        run["storms"].append(storm)
        run["track"].extend(track)
        logger.info(
            "JTWC %s %s: %d kt at hour 0, track to %d h",
            storm_id, storm["name"], storm["vmax_kt"], storm["max_tau_h"],
        )

    logger.info(
        "JTWC: %d storm(s) listed, %d read, %d product(s) absent",
        len(storm_ids), len(storm_ids) - run["absent"], run["absent"],
    )
    return _frames(run)


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    for label, fetch in (("Layer 34 (NHC)", fetch_nhc_storms), ("Layer 35 (JTWC)", fetch_jtwc_storms)):
        data = fetch()
        logger.info("=== %s ===", label)
        logger.info("%s", data["status"].to_string(index=False))
        if not data["storms"].empty:
            logger.info("%s", data["storms"][
                ["storm_id", "name", "issued_at", "vmax_kt", "max_tau_h", "track_state"]
            ].to_string(index=False))
