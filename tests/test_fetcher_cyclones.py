"""Layers 33 / 34 — tropical-cyclone forecasts (S2 #374, slice 1 #387).

The fetchers read each agency's active-storm index and every listed storm's
forecast track and quadrant wind radii. Grading a port against them is slice 3
(analysis/hazards.py); what is pinned here is that the stored numbers are the
agencies' own, and that every way of reading them wrong fails loudly:

1.  **NHC's DBF coordinates are whole degrees.** The position is the shape
    geometry; the `LAT`/`LON` attribute columns are rounded.
2.  **A missing radius is not a zero radius.** NHC forecasts 64-kt radii to
    72 h only; beyond that the band is NULL (never learned), and a published
    `0` stays `0` (invariant 2).
3.  **`_latest` is an alias.** The advisory inside each zip must be the one
    the index names, or a half-published advisory pairs an old track with a
    new index.
4.  **JTWC answers 403, not 404, for a product it does not have.** A listed
    storm whose product is absent is stored as `absent`, not as a failure.
5.  **"Asked, none active" is an answer.** A quiet basin stamps
    `last_success`; that is what a `clear` reading will depend on.

No network: every payload is a committed fixture captured live (2026-10-07)
or the NHC archive's own Francine 2024 zips, served through a stubbed
`requests.get`.
"""

from __future__ import annotations

import io
import json
import sqlite3
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest
import shapefile

import config
import fetchers.cyclones as cyclones
import main
from latency.domain import LAYER_LATENCY_BY_KEY, LatencyClass
from pipeline.clean import clean_cyclone_frame
from pipeline.history import HISTORY_TABLES

FIXTURES = Path(__file__).parent / "fixtures" / "cyclones"
FRANCINE = FIXTURES / "francine"
CHECKED = datetime(2026, 10, 7, 19, 4, 0, tzinfo=timezone.utc)

NHC_INDEX = config.CYCLONE_NHC_INDEX_URL
JTWC_INDEX = config.CYCLONE_JTWC_INDEX_URL


def _nhc_url(storm_id: str, product: str) -> str:
    return config.CYCLONE_NHC_PRODUCT_URL.format(storm_id=storm_id, product=product)


def _jtwc_url(storm_id: str) -> str:
    return config.CYCLONE_JTWC_PRODUCT_URL.format(storm_id=storm_id)


def _francine(product: str, advisory: str) -> bytes:
    return (FRANCINE / f"al062024_{product}_{advisory}.zip").read_bytes()


def _live_index() -> dict:
    return json.loads((FIXTURES / "nhc_CurrentStorms_2026-10-07.json").read_text())


def _francine_index(advisory: str = "010", issuance: str = "2024-09-11T03:00:00.000Z") -> dict:
    """The live index's own shape, carrying Francine instead of Isaias."""
    storm = json.loads(json.dumps(_live_index()["activeStorms"][0]))
    storm.update(id="al062024", name="Francine", classification="HU", intensity="65")
    for product in ("forecastTrack", "forecastWindRadiiGIS"):
        storm[product]["advNum"] = advisory
        storm[product]["issuance"] = issuance
    return {"activeStorms": [storm]}


class _Response:
    def __init__(self, status: int, body: bytes | str, headers: dict | None = None):
        self.status_code = status
        self.content = body.encode() if isinstance(body, str) else body
        self.text = self.content.decode("utf-8", errors="replace")
        self.headers = headers or {}


@pytest.fixture
def web(monkeypatch: pytest.MonkeyPatch):
    """URL → response map behind `requests.get`; anything unmapped is a 404."""
    routes: dict[str, _Response] = {}
    calls: list[str] = []

    def _get(url, **kwargs):
        calls.append(url)
        answer = routes.get(url)
        if isinstance(answer, Exception):
            raise answer
        return answer or _Response(404, "not found")

    monkeypatch.setattr(cyclones.requests, "get", _get)
    monkeypatch.setattr(cyclones, "retry_sleep", lambda attempt: None)
    monkeypatch.setattr(cyclones, "_now_utc", lambda: CHECKED)
    routes_obj = type("Web", (), {"routes": routes, "calls": calls})
    return routes_obj


def _serve_francine(web, advisory: str = "010", index: dict | None = None) -> None:
    web.routes[NHC_INDEX] = _Response(
        200, json.dumps(index or _francine_index(advisory)),
        {"Last-Modified": "Wed, 11 Sep 2024 03:08:00 GMT"},
    )
    web.routes[_nhc_url("al062024", "5day")] = _Response(200, _francine("5day", advisory))
    web.routes[_nhc_url("al062024", "fcst")] = _Response(200, _francine("fcst", advisory))


def _dbf_points(advisory: str) -> list[dict]:
    """The 5-day points shapefile read directly, attributes and geometry."""
    archive = zipfile.ZipFile(io.BytesIO(_francine("5day", advisory)))
    stem = next(n for n in archive.namelist() if n.endswith("_pts.shp"))[:-4]
    reader = shapefile.Reader(
        shp=io.BytesIO(archive.read(stem + ".shp")),
        dbf=io.BytesIO(archive.read(stem + ".dbf")),
    )
    names = [field[0] for field in reader.fields[1:]]
    return [
        {**dict(zip(names, rec.record, strict=True)), "geom": rec.shape.points[0]}
        for rec in reader.iterShapeRecords()
    ]


# ---------------------------------------------------------------------------
# NHC — the track and its radii
# ---------------------------------------------------------------------------
def test_nhc_track_uses_geometry_not_the_rounded_dbf_columns(web):
    _serve_francine(web)
    track = cyclones.fetch_nhc_storms()["track"].set_index("tau_h")

    dbf = {int(row["TAU"]): row for row in _dbf_points("010")}
    assert set(track.index) == set(dbf)
    rounded_away = 0
    for tau, row in dbf.items():
        lon, lat = row["geom"]
        assert track.loc[tau, "lat"] == pytest.approx(lat)
        assert track.loc[tau, "lon"] == pytest.approx(lon)
        if abs(row["LAT"] - lat) > 0.01 or abs(row["LON"] - lon) > 0.01:
            rounded_away += 1
    assert rounded_away >= 1, "the fixture no longer shows the whole-degree trap"


def test_nhc_radii_join_by_tau(web):
    _serve_francine(web)
    track = cyclones.fetch_nhc_storms()["track"].set_index("tau_h")

    # Advisory 010's forecast-radii DBF: 34 kt at TAU 12 = 80/110/70/60 nm.
    assert tuple(track.loc[12, ["r34_ne", "r34_se", "r34_sw", "r34_nw"]]) == (80, 110, 70, 60)
    assert tuple(track.loc[24, ["r64_ne", "r64_se", "r64_sw", "r64_nw"]]) == (10, 25, 15, 0)


def test_nhc_64kt_radius_beyond_72h_is_null_not_zero(web):
    """Advisory 008 carries a 96-h point, and no radius of any band there."""
    _serve_francine(web, "008", _francine_index("008", "2024-09-10T15:00:00.000Z"))
    track = cyclones.fetch_nhc_storms()["track"].set_index("tau_h")

    beyond = track[track.index > 72]
    assert list(beyond.index) == [96]
    assert beyond[["r64_ne", "r64_se", "r64_sw", "r64_nw"]].isna().all().all()
    # Inside 72 h a band the agency did not publish is NULL too: 34 kt is
    # forecast at 48 h, 64 kt is not.
    assert track.loc[48, ["r34_ne", "r34_se", "r34_sw", "r34_nw"]].notna().all()
    assert track.loc[48, ["r64_ne", "r64_se", "r64_sw", "r64_nw"]].isna().all()


def test_a_published_zero_radius_stays_zero(web):
    _serve_francine(web)
    track = cyclones.fetch_nhc_storms()["track"].set_index("tau_h")
    # Advisory 010, 64 kt at TAU 0: 30/30/0/0 nm.
    assert tuple(track.loc[0, ["r64_ne", "r64_se", "r64_sw", "r64_nw"]]) == (30, 30, 0, 0)


def test_nhc_storm_row_carries_the_advisory_and_the_hour_zero_reading(web):
    data = (_serve_francine(web), cyclones.fetch_nhc_storms())[1]
    storm = data["storms"].iloc[0]
    track = data["track"]

    assert storm["storm_id"] == "al062024"
    assert (storm["basin_prefix"], storm["storm_number"], storm["season"]) == ("al", 6, 2024)
    assert storm["advisory"] == "10"
    assert storm["classification"] == "HU"
    assert storm["issued_at"] == "2024-09-11T03:00:00Z"
    assert storm["track_state"] == "ok"
    assert storm["vmax_kt"] == 65  # MAXWIND at TAU 0, not the GUST column beside it
    hour0 = track[track["tau_h"] == 0].iloc[0]
    assert (storm["lat"], storm["lon"]) == (hour0["lat"], hour0["lon"])
    assert storm["max_tau_h"] == track["tau_h"].max()
    assert set(track["is_forecast"]) == {0, 1}
    assert track.loc[track["tau_h"] == 0, "is_forecast"].item() == 0
    assert storm["attribution"] == config.CYCLONE_ATTRIBUTION["NHC"]


def test_nhc_forecast_hours_count_from_the_synoptic_time_not_the_issue_time(web):
    """NHC's tau 12 is valid at synoptic + 12 h (the radii DBF says so), three
    hours before issued + 12 h. The time the hours count from is stored, so a
    later absolute time is never struck off the wrong clock."""
    _serve_francine(web)
    storm = cyclones.fetch_nhc_storms()["storms"].iloc[0]

    assert storm["synoptic_at"] == "2024-09-11T00:00:00Z"


def test_nhc_intensity_string_is_parsed():
    assert cyclones.parse_intensity("80") == 80
    assert cyclones.parse_intensity(" 105 ") == 105
    for bad in ("", "80 kt", None, 80.5, "N/A"):
        with pytest.raises(ValueError):
            cyclones.parse_intensity(bad)


def test_the_live_index_shape_parses():
    listings = cyclones.parse_nhc_index(json.dumps(_live_index()))
    assert [(item.storm_id, item.advisory) for item in listings] == [
        ("al092026", "4"), ("ep182026", "42"), ("ep202026", "1"),
    ]
    assert listings[0].issued_at == "2026-10-07T15:00:00Z"
    assert listings[0].name == "Isaias"
    assert listings[0].classification == "TS"


@pytest.mark.parametrize("payload", [
    "not json",
    json.dumps([]),
    json.dumps({"storms": []}),
    json.dumps({"activeStorms": {}}),
])
def test_an_unreadable_nhc_index_raises(payload):
    with pytest.raises(ValueError):
        cyclones.parse_nhc_index(payload)


def test_a_storm_missing_its_forecast_track_entry_raises():
    index = _live_index()
    del index["activeStorms"][0]["forecastTrack"]
    with pytest.raises(ValueError, match="al092026"):
        cyclones.parse_nhc_index(json.dumps(index))


def test_nhc_empty_active_storms_is_a_successful_answer(web):
    web.routes[NHC_INDEX] = _Response(200, json.dumps({"activeStorms": []}))
    data = cyclones.fetch_nhc_storms()

    assert set(data) == {"status", "storms", "track"}
    status = data["status"].iloc[0]
    assert status["storms_listed"] == 0
    assert status["storms_parsed"] == 0
    assert status["products_absent"] == 0
    assert status["source"] == "NHC"
    assert status["checked_at"] == "2026-10-07T19:04:00Z"
    assert pd.isna(status["index_last_modified"])  # not served → never learned
    assert data["storms"].empty and data["track"].empty
    assert list(data["storms"].columns) == list(cyclones.STORM_COLUMNS)
    assert list(data["track"].columns) == list(cyclones.TRACK_COLUMNS)


def test_the_index_last_modified_header_is_recorded(web):
    _serve_francine(web)
    status = cyclones.fetch_nhc_storms()["status"].iloc[0]
    assert status["index_last_modified"] == "Wed, 11 Sep 2024 03:08:00 GMT"


def test_nhc_advisory_mismatch_between_index_and_zip_raises(web):
    _serve_francine(web, index=_francine_index(advisory="011"))
    # Index says 011; both zips are 010 — the `_latest` alias lagging.
    with pytest.raises(cyclones.CycloneSourceError, match="advisory"):
        cyclones.fetch_nhc_storms()


def test_a_track_and_radii_pair_from_different_advisories_raises(web):
    _serve_francine(web)
    web.routes[_nhc_url("al062024", "fcst")] = _Response(200, _francine("fcst", "009"))
    with pytest.raises(cyclones.CycloneSourceError, match="advisory"):
        cyclones.fetch_nhc_storms()


def test_a_listed_nhc_storm_whose_zip_is_missing_fails_the_layer(web):
    _serve_francine(web)
    del web.routes[_nhc_url("al062024", "fcst")]
    with pytest.raises(cyclones.CycloneSourceError, match="al062024"):
        cyclones.fetch_nhc_storms()


def test_a_truncated_zip_served_as_200_fails_the_layer(web):
    _serve_francine(web)
    web.routes[_nhc_url("al062024", "5day")] = _Response(200, _francine("5day", "010")[:4000])
    with pytest.raises(cyclones.CycloneSourceError, match="al062024"):
        cyclones.fetch_nhc_storms()


def test_an_nhc_index_that_never_answers_fails_after_retries(web):
    web.routes[NHC_INDEX] = _Response(503, "busy")
    with pytest.raises(cyclones.CycloneSourceError):
        cyclones.fetch_nhc_storms()
    assert web.calls.count(NHC_INDEX) == config.MAX_RETRIES


# ---------------------------------------------------------------------------
# JTWC — the RSS index and the JMV 3.0 `.tcw` product
# ---------------------------------------------------------------------------
def _tcw(name: str) -> str:
    return (FIXTURES / name).read_text()


def test_jtwc_index_lists_products_by_their_metoc_urls_not_the_s3_links():
    rss = (FIXTURES / "jtwc_2026-10-07.rss").read_text()
    assert cyclones.parse_jtwc_index(rss) == ["ep1526", "ep1826", "ep2026", "wp2626", "wp2726"]


def test_jtwc_tcw_parses_track_and_radii():
    storm, points = cyclones.parse_jtwc_tcw(
        "wp2726", _tcw("jtwc_wp2726_2026-10-07.tcw"), checked_at=CHECKED,
    )
    track = pd.DataFrame(points).set_index("tau_h")

    assert storm["name"] == "KOGUMA"
    assert storm["synoptic_at"] == "2026-10-07T12:00:00Z"
    assert storm["issued_at"] == "2026-10-07T15:00:00Z"
    assert (storm["basin_prefix"], storm["storm_number"], storm["season"]) == ("wp", 27, 2026)
    assert storm["classification"] is None and storm["advisory"] is None
    assert (storm["lat"], storm["lon"], storm["vmax_kt"], storm["max_tau_h"]) == (15.7, 162.6, 50, 120)
    assert list(track.index) == [0, 12, 24, 36, 48, 60, 72, 96, 120]
    # T000: R050 030/030/000/000, R034 050/050/040/035, no 64-kt group.
    assert tuple(track.loc[0, ["r50_ne", "r50_se", "r50_sw", "r50_nw"]]) == (30, 30, 0, 0)
    assert tuple(track.loc[0, ["r34_ne", "r34_se", "r34_sw", "r34_nw"]]) == (50, 50, 40, 35)
    assert track.loc[0, ["r64_ne", "r64_se", "r64_sw", "r64_nw"]].isna().all()
    assert tuple(track.loc[24, ["r64_ne", "r64_se", "r64_sw", "r64_nw"]]) == (20, 20, 0, 10)
    assert track.loc[120, "lat"] == 26.6 and track.loc[120, "vmax_kt"] == 95


def test_jtwc_a_hemisphere_letter_signs_the_coordinate():
    storm, points = cyclones.parse_jtwc_tcw(
        "ep2026", _tcw("jtwc_ep2026_2026-10-07.tcw"), checked_at=CHECKED,
    )
    assert (storm["lat"], storm["lon"]) == (13.9, -100.6)
    # T000 of a 30-kt depression publishes no radius at all: every band NULL.
    hour0 = next(p for p in points if p["tau_h"] == 0)
    assert all(hour0[f"r{band}_{q}"] is None
               for band in (34, 50, 64) for q in ("ne", "se", "sw", "nw"))


def test_jtwc_tcw_with_no_track_lines_raises():
    text = _tcw("jtwc_wp2726_2026-10-07.tcw")
    headless = "\n".join(line for line in text.splitlines() if not line.startswith("T"))
    with pytest.raises(cyclones.CycloneSourceError, match="wp2726"):
        cyclones.parse_jtwc_tcw("wp2726", headless, checked_at=CHECKED)


def test_jtwc_unknown_header_shape_raises():
    text = _tcw("jtwc_wp2726_2026-10-07.tcw").replace("2026100712 27W KOGUMA", "KOGUMA 27W")
    with pytest.raises(cyclones.CycloneSourceError, match="header"):
        cyclones.parse_jtwc_tcw("wp2726", text, checked_at=CHECKED)


def test_jtwc_a_track_line_in_an_unknown_shape_raises():
    text = _tcw("jtwc_wp2726_2026-10-07.tcw").replace("T012 164N 1610E 055", "T012 164N 1610E 55KT")
    with pytest.raises(cyclones.CycloneSourceError, match="T012"):
        cyclones.parse_jtwc_tcw("wp2726", text, checked_at=CHECKED)


def test_jtwc_header_naming_another_storm_raises():
    with pytest.raises(cyclones.CycloneSourceError, match="26"):
        cyclones.parse_jtwc_tcw("wp2626", _tcw("jtwc_wp2726_2026-10-07.tcw"), checked_at=CHECKED)


def _serve_jtwc(web, rss: str, products: dict[str, _Response]) -> None:
    web.routes[JTWC_INDEX] = _Response(200, rss, {"Last-Modified": "Wed, 07 Oct 2026 16:58:26 GMT"})
    for storm_id, answer in products.items():
        web.routes[_jtwc_url(storm_id)] = answer


def _rss_listing(*storm_ids: str) -> str:
    items = "".join(
        f"<li><a href='https://www.metoc.navy.mil/jtwc/products/{sid}.tcw'>JMV 3.0 Data</a></li>"
        for sid in storm_ids
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
        "<title>JTWC</title><item><title>Current</title>"
        f"<description><![CDATA[<ul>{items}</ul>]]></description></item>"
        "</channel></rss>"
    )


def test_jtwc_403_on_a_listed_product_is_absent_not_failed(web):
    _serve_jtwc(web, _rss_listing("wp2726", "sh0126"), {
        "wp2726": _Response(200, _tcw("jtwc_wp2726_2026-10-07.tcw")),
        "sh0126": _Response(403, (FIXTURES / "jtwc_403_body.xml").read_text()),
    })
    data = cyclones.fetch_jtwc_storms()

    status = data["status"].iloc[0]
    assert (status["storms_listed"], status["storms_parsed"], status["products_absent"]) == (2, 1, 1)
    absent = data["storms"].set_index("storm_id").loc["sh0126"]
    assert absent["track_state"] == "absent"
    assert absent["issued_at"] == absent["checked_at"] == "2026-10-07T19:04:00Z"
    for column in ("lat", "lon", "vmax_kt", "max_tau_h", "synoptic_at", "name"):
        assert pd.isna(absent[column]), column
    assert (absent["basin_prefix"], absent["storm_number"], absent["season"]) == ("sh", 1, 2026)
    assert set(data["track"]["storm_id"]) == {"wp2726"}
    # A 403 is S3's "no such object": asked once, never retried.
    assert web.calls.count(_jtwc_url("sh0126")) == 1


def test_jtwc_a_listed_product_that_errors_otherwise_fails_the_layer(web):
    _serve_jtwc(web, _rss_listing("wp2726"), {"wp2726": _Response(500, "oops")})
    with pytest.raises(cyclones.CycloneSourceError, match="wp2726"):
        cyclones.fetch_jtwc_storms()


def test_jtwc_rss_with_no_products_is_a_successful_answer(web):
    _serve_jtwc(web, _rss_listing(), {})
    data = cyclones.fetch_jtwc_storms()

    status = data["status"].iloc[0]
    assert (status["storms_listed"], status["storms_parsed"], status["products_absent"]) == (0, 0, 0)
    assert status["index_last_modified"] == "Wed, 07 Oct 2026 16:58:26 GMT"
    assert status["attribution"] == config.CYCLONE_ATTRIBUTION["JTWC"]
    assert data["storms"].empty and data["track"].empty


def test_an_unparsable_jtwc_index_raises(web):
    _serve_jtwc(web, "<html>maintenance</html", {})
    with pytest.raises(cyclones.CycloneSourceError):
        cyclones.fetch_jtwc_storms()


def test_the_live_jtwc_payload_reads_end_to_end(web):
    """Every product the live RSS lists, with two captured and three absent."""
    _serve_jtwc(web, (FIXTURES / "jtwc_2026-10-07.rss").read_text(), {
        "wp2726": _Response(200, _tcw("jtwc_wp2726_2026-10-07.tcw")),
        "ep2026": _Response(200, _tcw("jtwc_ep2026_2026-10-07.tcw")),
        **{sid: _Response(403, "AccessDenied") for sid in ("ep1526", "ep1826", "wp2626")},
    })
    data = {name: clean_cyclone_frame(name, frame)
            for name, frame in cyclones.fetch_jtwc_storms().items()}
    assert len(data["storms"]) == 5
    assert data["status"].iloc[0]["storms_parsed"] == 2


# ---------------------------------------------------------------------------
# Storm ids
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("storm_id, source, expected", [
    ("al062024", "NHC", ("al", 6, 2024)),
    ("ep182026", "NHC", ("ep", 18, 2026)),
    ("wp2626", "JTWC", ("wp", 26, 2026)),
    ("ep1526", "JTWC", ("ep", 15, 2026)),
])
def test_storm_ids_normalise_to_a_four_digit_season(storm_id, source, expected):
    assert cyclones.parse_storm_id(storm_id, source) == expected


@pytest.mark.parametrize("storm_id, source", [
    ("xx062024", "NHC"),   # unknown basin
    ("zz1226", "JTWC"),
    ("al0624", "NHC"),     # JTWC shape under NHC
    ("wp262026", "JTWC"),  # NHC shape under JTWC
    ("WP2626", "JTWC"),    # ids are lower case
])
def test_unknown_basin_prefix_raises(storm_id, source):
    with pytest.raises(cyclones.CycloneSourceError, match=storm_id):
        cyclones.parse_storm_id(storm_id, source)


def test_an_unknown_prefix_in_the_jtwc_feed_fails_the_layer(web):
    _serve_jtwc(web, _rss_listing("xx0126"), {})
    with pytest.raises(cyclones.CycloneSourceError, match="xx0126"):
        cyclones.fetch_jtwc_storms()


# ---------------------------------------------------------------------------
# Clean — bounds and keys, nothing dropped silently
# ---------------------------------------------------------------------------
def _francine_frames(web) -> dict[str, pd.DataFrame]:
    _serve_francine(web)
    return cyclones.fetch_nhc_storms()


def test_the_cleaner_returns_a_copy_and_keeps_every_row(web):
    raw = _francine_frames(web)
    for name, frame in raw.items():
        before = frame.copy()
        cleaned = clean_cyclone_frame(name, frame)
        pd.testing.assert_frame_equal(frame, before)
        assert len(cleaned) == len(frame)


def test_the_cleaner_normalises_longitude_before_bounding_it(web):
    track = _francine_frames(web)["track"].copy()
    track.loc[0, "lon"] = track.loc[0, "lon"] + 360
    assert clean_cyclone_frame("track", track).loc[0, "lon"] == pytest.approx(track.loc[0, "lon"] - 360)


@pytest.mark.parametrize("column, value", [
    ("lat", 91.0), ("vmax_kt", 251), ("vmax_kt", -1), ("r34_ne", 1001.0),
    ("r50_sw", -5.0), ("tau_h", 241),
])
def test_the_cleaner_rejects_a_value_outside_its_band(web, column, value):
    track = _francine_frames(web)["track"].copy()
    track.loc[1, column] = value
    with pytest.raises(ValueError, match=column):
        clean_cyclone_frame("track", track)


def test_the_cleaner_rejects_a_duplicate_key(web):
    storms = _francine_frames(web)["storms"]
    with pytest.raises(ValueError, match="duplicate"):
        clean_cyclone_frame("storms", pd.concat([storms, storms], ignore_index=True))


def test_the_cleaner_rejects_an_unknown_frame():
    with pytest.raises(ValueError, match="radii"):
        clean_cyclone_frame("radii", pd.DataFrame({"x": [1]}))


# ---------------------------------------------------------------------------
# Store — window-replaced tracks and the hard-fails
# ---------------------------------------------------------------------------
def _cleaned(frames: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    return {name: clean_cyclone_frame(name, frame) for name, frame in frames.items()}


def _count(db: Path, table: str, source: str) -> int:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {table} WHERE source = ?", (source,)).fetchone()[0]
    finally:
        conn.close()


def test_track_is_window_replaced_and_cleared_on_an_empty_answer(web, patched_db):
    from pipeline import query, store

    for name, frame in _cleaned(_francine_frames(web)).items():
        store.save_cyclone_frame("NHC", name, frame)
    assert _count(patched_db, "cyclone_track_points", "NHC") == 7
    # Another source's track is never touched by an NHC rewrite.
    other = _cleaned(_francine_frames(web))["track"].assign(source="JTWC", storm_id="wp2726")
    store.save_cyclone_frame("JTWC", "track", other)

    web.routes[NHC_INDEX] = _Response(200, json.dumps({"activeStorms": []}))
    for name, frame in _cleaned(cyclones.fetch_nhc_storms()).items():
        store.save_cyclone_frame("NHC", name, frame)

    assert _count(patched_db, "cyclone_track_points", "NHC") == 0
    assert _count(patched_db, "cyclone_track_points", "JTWC") == 7
    # The storm row and both status rows are history and stay.
    assert len(query.read_cyclone_storms("NHC")) == 1
    assert len(query.read_cyclone_status("NHC")) == 1  # same day: one upserted row


def test_a_new_advisory_replaces_the_previous_track_rather_than_merging(web, patched_db):
    from pipeline import query, store

    for advisory, issued in (("008", "2024-09-10T15:00:00.000Z"), ("010", "2024-09-11T03:00:00.000Z")):
        _serve_francine(web, advisory, _francine_index(advisory, issued))
        for name, frame in _cleaned(cyclones.fetch_nhc_storms()).items():
            store.save_cyclone_frame("NHC", name, frame)

    track = query.read_cyclone_track("NHC")
    assert track["issued_at"].nunique() == 1
    assert len(query.read_cyclone_storms("NHC")) == 2  # both advisories kept as history


def test_the_store_refuses_a_row_without_attribution(web, patched_db):
    from pipeline import store

    storms = _cleaned(_francine_frames(web))["storms"].assign(attribution=None)
    with pytest.raises(ValueError, match="attribution"):
        store.save_cyclone_frame("NHC", "storms", storms)


def test_the_store_refuses_an_unknown_source(web, patched_db):
    from pipeline import store

    status = _cleaned(_francine_frames(web))["status"]
    with pytest.raises(ValueError, match="GDACS"):
        store.save_cyclone_frame("GDACS", "status", status.assign(source="GDACS"))
    with pytest.raises(ValueError, match="JTWC"):
        store.save_cyclone_frame("JTWC", "status", status)  # rows say NHC


@pytest.mark.parametrize("column", ["lat", "lon", "vmax_kt", "max_tau_h", "synoptic_at"])
def test_the_store_refuses_a_read_track_with_no_position_or_wind(web, patched_db, column):
    from pipeline import store

    storms = _cleaned(_francine_frames(web))["storms"]
    storms[column] = None
    with pytest.raises(ValueError, match=column):
        store.save_cyclone_frame("NHC", "storms", storms)


# ---------------------------------------------------------------------------
# Pipeline wiring and grading
# ---------------------------------------------------------------------------
LAYERS = ("cyclones_nhc", "cyclones_jtwc")


def test_layers_are_registered():
    production = {row[0]: row for row in config.PRODUCTION_LAYERS}
    dict_layers = {entry.key: entry for entry in main._build_dict_layers()}
    for layer in LAYERS:
        assert layer in production
        assert dict_layers[layer].empty_fails is True
        spec = LAYER_LATENCY_BY_KEY[layer]
        assert spec.latency_class is LatencyClass.WEATHER
        assert layer not in config.FAST_REFRESH_LAYERS
        # §3.5: no key catalog, no floor, no day-granular age budget — the
        # age that means something is each advisory's, graded in hours.
        assert layer not in config.LAYER_MIN_KEYS
        assert layer not in config.LAYER_KEY_CATALOGS
        assert layer not in config.LAYER_MAX_DATA_AGE_DAYS
    numbers = [production[layer][1] for layer in LAYERS]
    assert len(set(numbers)) == 2


def _run_layer(layer: str) -> bool:
    entry = next(e for e in main._build_dict_layers() if e.key == layer)
    return main._run_dict_layer(entry)


def _freshness(db: Path, layer: str) -> tuple:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute(
            "SELECT status, last_success, rows_fetched FROM data_freshness WHERE layer_name = ?",
            (layer,),
        ).fetchone()
    finally:
        conn.close()


def test_zero_storm_run_stamps_last_success(web, patched_db):
    web.routes[NHC_INDEX] = _Response(200, json.dumps({"activeStorms": []}))
    assert _run_layer("cyclones_nhc") is True

    status, last_success, rows = _freshness(patched_db, "cyclones_nhc")
    assert status == "success"
    assert last_success is not None
    assert rows == 1  # the status row: "asked, none active"


def test_an_absent_jtwc_product_still_grades_success(web, patched_db):
    _serve_jtwc(web, _rss_listing("sh0126"), {"sh0126": _Response(403, "AccessDenied")})
    assert _run_layer("cyclones_jtwc") is True
    assert _freshness(patched_db, "cyclones_jtwc")[0] == "success"


def test_a_half_read_basin_grades_failed_and_keeps_last_success(web, patched_db):
    _serve_francine(web)
    assert _run_layer("cyclones_nhc") is True
    first_success = _freshness(patched_db, "cyclones_nhc")[1]

    del web.routes[_nhc_url("al062024", "fcst")]
    main._HARD_FAILURES.clear()
    assert _run_layer("cyclones_nhc") is False

    status, last_success, _ = _freshness(patched_db, "cyclones_nhc")
    assert status == "failed"
    assert last_success == first_success
    assert "cyclones_nhc" in main._HARD_FAILURES  # → the 20:40 UTC retry
    main._HARD_FAILURES.clear()


def test_an_empty_return_is_a_failure_not_a_quiet_day(patched_db, monkeypatch):
    monkeypatch.setattr(main, "fetch_jtwc_storms", lambda: {})
    assert _run_layer("cyclones_jtwc") is False
    assert _freshness(patched_db, "cyclones_jtwc")[0] == "failed"
    main._HARD_FAILURES.clear()


def test_cyclone_tables_round_trip_through_git_history(web, patched_db, tmp_path, monkeypatch):
    from pipeline import history, store

    history_dir = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(history_dir))
    monkeypatch.setattr("pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db)))

    assert HISTORY_TABLES["cyclone_source_status"] == ("source", "Date")
    assert HISTORY_TABLES["cyclone_storms"] == ("source", "storm_id", "issued_at")
    assert "cyclone_track_points" not in HISTORY_TABLES  # [P1 #6]: summary rows only

    for name, frame in _cleaned(_francine_frames(web)).items():
        store.save_cyclone_frame("NHC", name, frame)
    _serve_jtwc(web, _rss_listing("sh0126"), {"sh0126": _Response(403, "AccessDenied")})
    for name, frame in _cleaned(cyclones.fetch_jtwc_storms()).items():
        store.save_cyclone_frame("JTWC", name, frame)

    def snapshot(table: str) -> list[tuple]:
        conn = sqlite3.connect(str(patched_db))
        try:
            return conn.execute(f"SELECT * FROM {table} ORDER BY 1, 2, 3").fetchall()
        finally:
            conn.close()

    before = {t: snapshot(t) for t in ("cyclone_source_status", "cyclone_storms")}
    history.export_history()
    assert (history_dir / "cyclone_storms.csv").exists()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM cyclone_source_status")
    conn.execute("DELETE FROM cyclone_storms")
    conn.commit()
    conn.close()
    history.import_history()

    after = {t: snapshot(t) for t in before}
    assert after == before
    # The absent JTWC storm's NULLs come back as NULL, not as blanks.
    absent = [row for row in after["cyclone_storms"] if row[1] == "sh0126"]
    assert absent and None in absent[0]


def test_the_readers_return_what_was_stored(web, patched_db):
    from pipeline import query, store

    for name, frame in _cleaned(_francine_frames(web)).items():
        store.save_cyclone_frame("NHC", name, frame)

    status = query.read_cyclone_status()
    assert status.iloc[0]["Date"] == pd.Timestamp("2026-10-07")
    storms = query.read_cyclone_storms()
    assert storms.iloc[0]["issued_at"] == "2024-09-11T03:00:00Z"
    track = query.read_cyclone_track()
    assert sorted(track["tau_h"]) == sorted(_francine_frames(web)["track"]["tau_h"])
    assert track.loc[track["tau_h"] == 72, "r64_ne"].isna().all()
