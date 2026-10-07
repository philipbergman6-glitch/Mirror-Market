"""The cyclone hazard assessment (storm flags slice 3, #389).

Spec: ``docs/specs/cyclone-hazard-flags.md`` §5–§6 and §10.2. ``analysis/hazards.py``
is the one rule set that grades a place against the stored NHC/JTWC tracks;
the site (slice 4) and the briefing (slice 5) both call it, so they cannot
disagree.

The regression fixture is Hurricane Francine 2024, NHC ``al062024``,
advisories 008–012, parsed from the committed zips by the slice-1 fetcher
with no network. The expected table is the spike's replay
(``spike/footprint-weather/results/storms_replay_*.json``); if the parse
stops reproducing it, reconcile against ``storms.py`` before touching an
expectation.

Every test here is offline and touches no database.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

import config
from analysis import hazards
from analysis.hazards import LegHazard, PlaceHazard, assess_places, leg_hazard
from app.markets import load_markets
from fetchers import cyclones

FRANCINE = Path(__file__).parent / "fixtures" / "cyclones" / "francine"

# Advisory → issue time (six-hourly from 008 at 2024-09-10 15Z; NHC issues at
# 03/09/15/21 UTC). The synoptic time inside each zip is three hours earlier.
FRANCINE_ISSUED = {
    "008": "2024-09-10T15:00:00Z",
    "009": "2024-09-10T21:00:00Z",
    "010": "2024-09-11T03:00:00Z",
    "011": "2024-09-11T09:00:00Z",
    "012": "2024-09-11T15:00:00Z",
}

# Spec §10.1, from the spike's replay JSON. (state, band, severity, first 34-kt
# arrival, closest approach km, at tau).
FRANCINE_EXPECTED = {
    "008": ("flag", 34, "warning", 33, 112, 38),
    "009": ("flag", 34, "warning", 28, 104, 33),
    "010": ("flag", 34, "warning", 24, 107, 27),
    "011": ("flag", 34, "warning", 21, 99, 23),
    "012": ("flag", 34, "warning", 11, 106, 16),
}

NHC_OK = {"cyclones_nhc": "success", "cyclones_jtwc": "success"}
NOLA = config.PLACES["P-NOLA"]


def _utc(text: str) -> datetime:
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def _iso(when: datetime) -> str:
    return when.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Frame builders — the shapes the fetcher stores, built by hand or from the fixture
# ---------------------------------------------------------------------------
def _status_row(source: str, checked_at: datetime, listed: int) -> dict:
    return {
        "source": source,
        "Date": checked_at.date().isoformat(),
        "checked_at": _iso(checked_at),
        "storms_listed": listed,
        "storms_parsed": listed,
        "products_absent": 0,
        "index_last_modified": None,
        "attribution": config.CYCLONE_ATTRIBUTION[source],
    }


def _status(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=list(cyclones.STATUS_COLUMNS))


def _storms(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=list(cyclones.STORM_COLUMNS))


def _track(*rows: dict) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=list(cyclones.TRACK_COLUMNS))


def francine(advisory: str, checked_at: datetime | None = None) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Francine ``advisory`` as the slice-1 fetcher would have stored it."""
    issued = _utc(FRANCINE_ISSUED[advisory])
    checked = checked_at or issued + timedelta(hours=1)
    listing = cyclones.NhcListing(
        storm_id="al062024", name="Francine", classification="HU", intensity_kt=65,
        advisory=str(int(advisory)), issued_at=_iso(issued), issued=issued,
    )
    storm, track = cyclones.parse_nhc_products(
        listing,
        (FRANCINE / f"al062024_5day_{advisory}.zip").read_bytes(),
        (FRANCINE / f"al062024_fcst_{advisory}.zip").read_bytes(),
        checked_at=checked,
    )
    return _status(_status_row("NHC", checked, 1)), _storms(storm), _track(*track)


def _synthetic_storm(
    source: str, storm_id: str, points: list[dict], *, issued_at: datetime, checked_at: datetime,
    name: str = "Synthetic", track_state: str = "ok",
) -> tuple[dict, list[dict]]:
    """A storm from ``points`` — each ``{tau, lat, lon, vmax, r34?, r50?, r64?}``,
    radii as 4-tuples (ne, se, sw, nw) or absent (NULL)."""
    prefix, number, season = cyclones.parse_storm_id(storm_id, source)
    rows = []
    for p in points:
        row = {
            "source": source, "storm_id": storm_id, "issued_at": _iso(issued_at),
            "tau_h": p["tau"], "lat": p["lat"], "lon": p["lon"], "vmax_kt": p["vmax"],
            "is_forecast": 0 if p["tau"] == 0 else 1,
        }
        for band in (34, 50, 64):
            radii = p.get(f"r{band}")
            for i, q in enumerate(("ne", "se", "sw", "nw")):
                row[f"r{band}_{q}"] = None if radii is None else float(radii[i])
        rows.append(row)
    hour0 = points[0] if track_state == "ok" else None
    storm = {
        "source": source, "storm_id": storm_id, "issued_at": _iso(issued_at),
        "synoptic_at": _iso(issued_at) if track_state == "ok" else None,
        "checked_at": _iso(checked_at), "basin_prefix": prefix, "storm_number": number,
        "season": season, "name": name, "classification": "TS" if source == "NHC" else None,
        "advisory": "5" if source == "NHC" else None,
        "lat": hour0["lat"] if hour0 else None, "lon": hour0["lon"] if hour0 else None,
        "vmax_kt": hour0["vmax"] if hour0 else None,
        "max_tau_h": points[-1]["tau"] if hour0 else None,
        "track_state": track_state, "attribution": config.CYCLONE_ATTRIBUTION[source],
    }
    return storm, rows if track_state == "ok" else []


def _stationary(lat: float, lon: float, vmax: int, *, r34=None, r50=None, r64=None, taus=(0, 12, 24)) -> list[dict]:
    """A storm that sits still: the same point at every forecast hour."""
    return [
        {"tau": t, "lat": lat, "lon": lon, "vmax": vmax, "r34": r34, "r50": r50, "r64": r64}
        for t in taus
    ]


NOW = _utc("2026-10-07T20:00:00Z")
CHECKED = NOW - timedelta(hours=1)


def _assess(storms_by_source: dict[str, list[tuple[dict, list[dict]]]], *, now=NOW, checked=CHECKED,
            layer_states=None, places=None, today=None):
    """Run ``assess_places`` over synthetic storms per source, both sources answering."""
    status_rows = [_status_row(src, checked, len(storms_by_source.get(src, []))) for src in ("NHC", "JTWC")]
    storm_rows = [s for items in storms_by_source.values() for s, _ in items]
    track_rows = [r for items in storms_by_source.values() for _, rows in items for r in rows]
    return assess_places(
        places if places is not None else hazards.active_places(config.PLACES, (today or now.date())),
        config.CYCLONE_BASINS,
        _status(*status_rows),
        _storms(*storm_rows),
        _track(*track_rows),
        layer_states if layer_states is not None else NHC_OK,
        now,
    )


# ---------------------------------------------------------------------------
# Francine — the regression fixture
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("advisory", sorted(FRANCINE_EXPECTED))
def test_francine_replay_flags_new_orleans(advisory):
    """Spec §10.1, reproduced exactly from the committed zips."""
    status, storms, track = francine(advisory)
    now = _utc(status.iloc[0]["checked_at"]) + timedelta(hours=2)
    jtwc = _status(_status_row("JTWC", _utc(status.iloc[0]["checked_at"]), 0))

    result = assess_places(
        hazards.active_places(config.PLACES, now.date()), config.CYCLONE_BASINS,
        pd.concat([status, jtwc], ignore_index=True), storms, track, NHC_OK, now,
    )
    nola = result["P-NOLA"]
    state, band, severity, first, closest, closest_tau = FRANCINE_EXPECTED[advisory]

    assert (nola.state, nola.band_kt, nola.severity) == (state, band, severity)
    assert nola.first_arrival_tau_h == first
    assert (nola.closest_km, nola.closest_tau_h) == (closest, closest_tau)
    assert nola.source == "NHC" and nola.storm_id == "al062024" and nola.storm_name == "Francine"
    assert nola.advisory_stale is False
    # Absolute times are struck from the synoptic time the forecast hours count from.
    synoptic = _utc(storms.iloc[0]["synoptic_at"])
    assert nola.first_arrival_at == _iso(synoptic + timedelta(hours=first))
    assert nola.closest_at == _iso(synoptic + timedelta(hours=closest_tau))


@pytest.mark.parametrize("advisory", sorted(FRANCINE_EXPECTED))
def test_francine_remnant_does_not_put_illinois_on_watch(advisory):
    """K2 §6 defect 1: the ≥ 34-kt gate on the watch. Francine's remnant passes
    ~350 km from central Illinois at 96 h, below tropical-storm strength."""
    status, storms, track = francine(advisory)
    now = _utc(status.iloc[0]["checked_at"]) + timedelta(hours=2)
    illinois = {
        "X-IL": {"name": "Illinois", "short": "Illinois", "kind": "export_port", "lat": 40.12, "lon": -89.30,
                 "basin": "north_atlantic", "effective_from": None, "effective_to": None},
    }
    result = assess_places(illinois, config.CYCLONE_BASINS, status, storms, track,
                           {"cyclones_nhc": "success"}, now)

    assert result["X-IL"].state == "clear"
    assert result["X-IL"].closest_km < 800          # the remnant does pass nearby...
    assert result["X-IL"].closest_ts_km > 500       # ...but never within 500 km while ≥ 34 kt


def test_francine_legs():
    """The real registry under advisory 010, rolled up per ledger leg."""
    status, storms, track = francine("010")
    now = _utc(status.iloc[0]["checked_at"]) + timedelta(hours=2)
    jtwc = _status(_status_row("JTWC", _utc(status.iloc[0]["checked_at"]), 0))
    places = assess_places(hazards.active_places(config.PLACES, now.date()), config.CYCLONE_BASINS,
                           pd.concat([status, jtwc], ignore_index=True), storms, track, NHC_OK, now)
    legs = {
        leg.leg_id: leg_hazard(leg.place_ids, places)
        for market in load_markets(today=now.date()).values() if market.ledger is not None
        for leg in market.ledger.legs
    }

    for leg_id in ("cbot:board", "us_gulf:cif", "dalian:board"):
        assert legs[leg_id] is not None and legs[leg_id].state == "flag", leg_id
        assert legs[leg_id].severity == "warning"
        assert legs[leg_id].primary is not None and legs[leg_id].primary.place_id == "P-NOLA"
    assert legs["cbot:board"].partial is False
    assert legs["dalian:board"].partial is True
    assert legs["dalian:board"].uncovered == ("P-PNG",)
    assert legs["brazil:paranagua"].state == "not_covered"
    assert legs["argentina:fob"].state == "not_covered"
    assert legs["south_africa:safex"].state == "clear"
    assert legs["south_africa:safex"].partial is False
    for leg_id in ("brazil:cepea", "india:mandi_mp", "india:mandi_mh"):
        assert legs[leg_id] is None, leg_id


def test_francine_flag_sentence():
    status, storms, track = francine("010")
    now = _utc(status.iloc[0]["checked_at"]) + timedelta(hours=2)
    nola = assess_places(hazards.active_places(config.PLACES, now.date()), config.CYCLONE_BASINS,
                         status, storms, track, NHC_OK, now)["P-NOLA"]
    text = hazards.flag_sentence(nola)

    assert text.startswith("New Orleans: Francine (NHC adv 10, Hurricane, category 1, 65 kt) — "
                           "tropical-storm-force winds forecast from Thu 12 Sep 00:00Z (+24 h")
    assert "closest approach 107 km at Thu 12 Sep 03:00Z." in text
    assert text.endswith("NHC forecast.")
    assert "Three to five days" not in text
    assert hazards.chip_text(nola) == "Francine (NHC): tropical-storm-force winds at New Orleans from Thu 12 Sep 00:00Z"


# ---------------------------------------------------------------------------
# De-duplication (§2.1)
# ---------------------------------------------------------------------------
def _far_storm(source: str, storm_id: str, name: str) -> tuple[dict, list[dict]]:
    """Mid-Pacific, nowhere near a place; exists only to be counted."""
    return _synthetic_storm(source, storm_id, _stationary(10.0, -150.0, 60, r34=(50, 50, 50, 50)),
                            issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED, name=name)


def test_dedupe_is_by_basin_number_and_year_not_prefix():
    """K2 §6 defect 2: Nolo kept its ``ep`` id at JTWC after NHC stopped issuing."""
    current = hazards.current_storms(
        _status(_status_row("NHC", CHECKED, 1), _status_row("JTWC", CHECKED, 4)),
        _storms(
            _far_storm("NHC", "ep182026", "Rachel")[0],
            _far_storm("JTWC", "ep1526", "Nolo")[0],
            _far_storm("JTWC", "ep1826", "Rachel")[0],
            _far_storm("JTWC", "wp2626", "Choi-wan")[0],
            _far_storm("JTWC", "wp2726", "Koguma")[0],
        ),
        NHC_OK,
    )
    kept = sorted(zip(current["source"], current["storm_id"], strict=True))

    assert kept == [("JTWC", "ep1526"), ("JTWC", "wp2626"), ("JTWC", "wp2726"), ("NHC", "ep182026")]


def test_no_dedupe_when_nhc_layer_failed():
    current = hazards.current_storms(
        _status(_status_row("NHC", CHECKED - timedelta(days=1), 1), _status_row("JTWC", CHECKED, 2)),
        _storms(
            _far_storm("NHC", "ep182026", "Rachel")[0],   # yesterday's read, not current
            _far_storm("JTWC", "ep1826", "Rachel")[0],
            _far_storm("JTWC", "wp2626", "Choi-wan")[0],
        ),
        {"cyclones_nhc": "failed", "cyclones_jtwc": "success"},
    )

    assert sorted(current["storm_id"]) == ["ep1826", "wp2626"]
    assert set(current["source"]) == {"JTWC"}


def test_only_the_newest_read_per_source_is_current():
    old, new = CHECKED - timedelta(days=1), CHECKED
    stale = _synthetic_storm("NHC", "al012026", _stationary(20, -60, 50, r34=(50,) * 4),
                             issued_at=old - timedelta(hours=3), checked_at=old, name="Old")[0]
    fresh = _synthetic_storm("NHC", "al022026", _stationary(20, -60, 50, r34=(50,) * 4),
                             issued_at=new - timedelta(hours=3), checked_at=new, name="New")[0]
    current = hazards.current_storms(
        _status(_status_row("NHC", old, 1), _status_row("NHC", new, 1)), _storms(stale, fresh), NHC_OK,
    )

    assert list(current["storm_id"]) == ["al022026"]


# ---------------------------------------------------------------------------
# Severity bands (§5.4)
# ---------------------------------------------------------------------------
def _nola_storm(points: list[dict], **kw) -> tuple[dict, list[dict]]:
    return _synthetic_storm("NHC", "al092026", points, issued_at=CHECKED - timedelta(hours=3),
                            checked_at=CHECKED, name="Synthetic", **kw)


def _over_nola(tau: int, **radii) -> dict:
    return {"tau": tau, "lat": NOLA["lat"], "lon": NOLA["lon"], "vmax": 80, **radii}


def _far_from_nola(tau: int, vmax: int = 80) -> dict:
    return {"tau": tau, "lat": 15.0, "lon": -60.0, "vmax": vmax}


def test_severity_bands_hurricane_force_is_alert():
    storm = _nola_storm([_over_nola(0, r34=(100,) * 4, r50=(60,) * 4, r64=(30,) * 4),
                         _over_nola(12, r34=(100,) * 4, r50=(60,) * 4, r64=(30,) * 4)])
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert (nola.state, nola.severity, nola.band_kt) == ("flag", "alert", 64)
    assert nola.first_arrival_tau_h == 0


def test_severity_bands_ts_force_inside_72h_is_warning():
    # Far away until +48, over New Orleans with a 34-kt radius at +60.
    storm = _nola_storm([_far_from_nola(0), _far_from_nola(48), _over_nola(60, r34=(100,) * 4)])
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert (nola.state, nola.severity, nola.band_kt) == ("flag", "warning", 34)
    assert nola.first_arrival_tau_h <= 72


def test_severity_bands_ts_force_beyond_72h_is_info():
    storm = _nola_storm([_far_from_nola(0), _far_from_nola(84), _over_nola(96, r34=(60,) * 4)])
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert (nola.state, nola.severity, nola.band_kt) == ("flag", "info", 34)
    assert nola.first_arrival_tau_h > 72
    assert hazards.flag_sentence(nola).endswith("NHC forecast. Three to five days out.")


def test_severity_bands_50kt_is_reported_as_50():
    storm = _nola_storm([_over_nola(0, r34=(100,) * 4, r50=(60,) * 4), _over_nola(12, r34=(100,) * 4, r50=(60,) * 4)])
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert (nola.state, nola.severity, nola.band_kt) == ("flag", "warning", 50)


def test_severity_bands_close_centre_at_ts_strength_outside_radii_is_watch():
    # ~400 km east of New Orleans, 40 kt, with 34-kt radii far too small to reach it.
    storm = _nola_storm(_stationary(NOLA["lat"], NOLA["lon"] + 4.1, 40, r34=(30,) * 4))
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert (nola.state, nola.severity, nola.band_kt) == ("watch", "info", None)
    assert 350 < nola.closest_km < 500
    assert nola.closest_ts_km == nola.closest_km
    assert "within" in hazards.watch_sentence(nola) and "outside its forecast wind radii" in hazards.watch_sentence(nola)


def test_severity_bands_close_centre_below_ts_strength_is_clear():
    storm = _nola_storm(_stationary(NOLA["lat"], NOLA["lon"] + 4.1, 30))
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert nola.state == "clear"
    assert nola.severity is None
    assert nola.closest_km < 500
    assert "Synthetic" in nola.reason and "km" in nola.reason


def test_worst_storm_wins_then_earlier_arrival():
    weak = _nola_storm([_far_from_nola(0), _over_nola(24, r34=(60,) * 4), _over_nola(36, r34=(60,) * 4)])
    strong = _synthetic_storm("NHC", "al102026", [_far_from_nola(0), _far_from_nola(36),
                                                    _over_nola(48, r34=(60,) * 4, r50=(30,) * 4, r64=(15,) * 4)],
                              issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED, name="Strong")
    nola = _assess({"NHC": [weak, strong]})["P-NOLA"]

    assert nola.storm_id == "al102026" and nola.severity == "alert"


def test_a_null_radius_never_flags():
    """Invariant 2: NHC publishes no 64-kt radius beyond 72 h; NULL is not 0 and not "everything"."""
    storm = _nola_storm([_far_from_nola(0), _far_from_nola(84), _over_nola(96, r34=(60,) * 4, r50=None, r64=None)])
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert nola.band_kt == 34


# ---------------------------------------------------------------------------
# Geometry (§5.2–§5.3)
# ---------------------------------------------------------------------------
def test_interpolation_crosses_the_antimeridian():
    points = [{"tau": 0, "lat": 20.0, "lon": 179.0, "vmax": 60, "r34": (60,) * 4},
              {"tau": 24, "lat": 20.0, "lon": -179.0, "vmax": 60, "r34": (60,) * 4}]
    samples = hazards.hourly_track(_track(*_synthetic_storm("JTWC", "wp2926", points, issued_at=CHECKED,
                                                            checked_at=CHECKED)[1]))
    lons = [s.lon for s in samples]

    assert len(samples) == 25
    assert lons[12] == pytest.approx(180.0) or lons[12] == pytest.approx(-180.0)
    assert all(abs(abs(lon) - 180.0) <= 1.0 for lon in lons)       # the short way round, not via Greenwich

    dateline = {"X-DL": {"name": "Dateline", "short": "Dateline", "kind": "export_port", "lat": 20.0, "lon": 180.0,
                         "basin": "west_pacific", "effective_from": None, "effective_to": None}}
    storm = _synthetic_storm("JTWC", "wp2926", points, issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED)
    assert _assess({"JTWC": [storm]}, places=dateline)["X-DL"].state == "flag"


def test_quadrants():
    """A 100-nm NE radius and 0 elsewhere: only the place to the north-east is flagged."""
    centre = (20.0, -60.0)
    offset = 1.0  # ~110 km NE / SW on the diagonal — inside 100 nm (185 km)
    places = {}
    for pid, (dlat, dlon) in {"X-NE": (offset, offset), "X-SW": (-offset, -offset)}.items():
        places[pid] = {"name": pid, "short": pid, "kind": "export_port", "lat": centre[0] + dlat,
                       "lon": centre[1] + dlon, "basin": "north_atlantic", "effective_from": None, "effective_to": None}
    storm = _synthetic_storm("NHC", "al112026", _stationary(*centre, 60, r34=(100, 0, 0, 0)),
                             issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED)
    result = _assess({"NHC": [storm]}, places=places)

    assert result["X-NE"].state == "flag"
    assert result["X-SW"].state == "watch"     # close, ≥ 34 kt, but outside every radius
    assert hazards.quadrant(hazards.initial_bearing_deg(*centre, places["X-NE"]["lat"], places["X-NE"]["lon"])) == 0
    assert hazards.quadrant(hazards.initial_bearing_deg(*centre, places["X-SW"]["lat"], places["X-SW"]["lon"])) == 2


def test_haversine_known_distance():
    # New Orleans to Houston (29.7604, -95.3698): ~510 km.
    assert hazards.haversine_km(NOLA["lat"], NOLA["lon"], 29.7604, -95.3698) == pytest.approx(511, abs=3)


def test_hourly_track_samples_every_hour_and_keeps_the_final_point():
    points = [{"tau": 0, "lat": 10.0, "lon": -50.0, "vmax": 40, "r34": (0, 0, 0, 0)},
              {"tau": 12, "lat": 11.0, "lon": -51.0, "vmax": 52, "r34": (60, 60, 60, 60)},
              {"tau": 120, "lat": 20.0, "lon": -60.0, "vmax": 100},
              {"tau": 144, "lat": 25.0, "lon": -65.0, "vmax": 100}]
    samples = hazards.hourly_track(_track(*_synthetic_storm("NHC", "al122026", points, issued_at=CHECKED,
                                                            checked_at=CHECKED)[1]))

    assert [s.tau_h for s in samples] == list(range(0, 121))
    assert samples[6].vmax_kt == pytest.approx(46)
    assert samples[6].radii[34][0] == pytest.approx(30)     # NULL on the far side would be 0, not 60
    assert samples[120].radii[34] == (0.0, 0.0, 0.0, 0.0)   # NULL at 120 interpolates as 0


# ---------------------------------------------------------------------------
# States and failure modes (§6)
# ---------------------------------------------------------------------------
def test_south_atlantic_is_not_covered_never_clear():
    result = _assess({})

    for pid in ("P-PNG", "P-UPR"):
        assert result[pid].state == "not_covered"
        assert "no publishable cyclone source" in result[pid].reason
    for pid in ("P-NOLA", "P-NCN", "P-DUR"):
        assert result[pid].state == "clear"
        assert result[pid].reason == "no active storm listed"


def test_failed_layer_makes_its_basin_failed_only():
    result = _assess({}, layer_states={"cyclones_nhc": "failed", "cyclones_jtwc": "success"})

    assert result["P-NOLA"].state == "failed"
    assert "NHC" in result["P-NOLA"].reason
    assert result["P-NCN"].state == "clear"
    assert result["P-DUR"].state == "clear"


def test_missing_layer_state_is_failed():
    result = _assess({}, layer_states={"cyclones_jtwc": "success"})

    assert result["P-NOLA"].state == "failed"


def test_a_source_with_no_status_row_is_failed():
    result = assess_places(
        hazards.active_places(config.PLACES, NOW.date()), config.CYCLONE_BASINS,
        _status(_status_row("NHC", CHECKED, 0)), _storms(), _track(), NHC_OK, NOW,
    )

    assert result["P-NOLA"].state == "clear"
    assert result["P-NCN"].state == "failed"
    assert "JTWC" in result["P-NCN"].reason


def test_old_check_is_stale():
    result = _assess({}, checked=NOW - timedelta(hours=37))

    assert result["P-NOLA"].state == "stale"
    assert result["P-NOLA"].reason.startswith("last checked ")


def test_a_check_inside_the_budget_is_not_stale():
    assert _assess({}, checked=NOW - timedelta(hours=35))["P-NOLA"].state == "clear"


def test_stale_advisory_keeps_a_flag_and_ages_it():
    storm = _synthetic_storm("NHC", "al092026", _stationary(NOLA["lat"], NOLA["lon"], 80, r34=(100,) * 4),
                             issued_at=CHECKED - timedelta(hours=13), checked_at=CHECKED)
    nola = _assess({"NHC": [storm]})["P-NOLA"]

    assert nola.state == "flag"
    assert nola.advisory_stale is True
    assert "Advisory was 13 h old when read." in hazards.flag_sentence(nola)


@pytest.mark.parametrize(("source", "storm_id", "age_h", "expected"), [
    ("NHC", "al092026", 13, "stale"),
    ("NHC", "al092026", 11, "clear"),
    ("JTWC", "wp2926", 13, "stale"),
    ("JTWC", "sh0126", 19, "stale"),
    ("JTWC", "sh0126", 17, "clear"),
])
def test_stale_advisory_makes_an_unflagged_place_in_its_basin_stale(source, storm_id, age_h, expected):
    place = {"al": "P-NOLA", "wp": "P-NCN", "sh": "P-DUR"}[storm_id[:2]]
    other = {"P-NOLA": "P-NCN", "P-NCN": "P-DUR", "P-DUR": "P-NOLA"}[place]
    lat = -15.0 if storm_id.startswith("sh") else 15.0
    storm = _synthetic_storm(source, storm_id, _stationary(lat, 0.0, 50, r34=(30,) * 4),
                             issued_at=CHECKED - timedelta(hours=age_h), checked_at=CHECKED, name="Far")
    result = _assess({source: [storm]})

    assert result[place].state == expected, result[place].reason
    if expected == "stale":
        assert f"advisory {age_h} h old when read" in result[place].reason
    assert result[other].state == "clear"       # another basin is untouched


def test_absent_track_makes_its_basin_failed():
    absent = _synthetic_storm("JTWC", "wp2726", _stationary(20.0, 130.0, 60), issued_at=CHECKED,
                              checked_at=CHECKED, name="Koguma", track_state="absent")
    result = _assess({"JTWC": [absent]})

    assert result["P-NCN"].state == "failed"
    assert result["P-NCN"].reason == "JTWC lists wp2726 but its track product is absent"
    assert result["P-DUR"].state == "clear"
    assert result["P-NOLA"].state == "clear"


def test_a_flag_beats_an_absent_track_in_the_same_basin():
    absent = _synthetic_storm("JTWC", "wp2726", [], issued_at=CHECKED, checked_at=CHECKED, track_state="absent")
    hit = _synthetic_storm("JTWC", "wp2826", _stationary(36.0833, 120.3170, 70, r34=(100,) * 4),
                           issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED, name="Hit")
    result = _assess({"JTWC": [absent, hit]})

    assert result["P-NCN"].state == "flag"


def test_inland_places_are_never_assessed():
    storms = [
        _synthetic_storm("NHC", "al092026", _stationary(*(config.PLACES["P-CBOT"][k] for k in ("lat", "lon")), 90,
                                                        r34=(200,) * 4, r64=(100,) * 4),
                         issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED),
        _synthetic_storm("JTWC", "sh0226", _stationary(*(config.PLACES["P-RFT"][k] for k in ("lat", "lon")), 90,
                                                       r34=(200,) * 4, r64=(100,) * 4),
                         issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED),
        _synthetic_storm("JTWC", "io0126", _stationary(*(config.PLACES["P-IDR"][k] for k in ("lat", "lon")), 90,
                                                       r34=(200,) * 4, r64=(100,) * 4),
                         issued_at=CHECKED - timedelta(hours=3), checked_at=CHECKED),
    ]
    result = _assess({"NHC": storms[:1], "JTWC": storms[1:]})

    assert not {"P-CBOT", "P-RFT", "P-IDR"} & set(result)
    assert set(result) == {"P-NOLA", "P-PNG", "P-UPR", "P-NCN", "P-DUR"}


def test_effective_dates_select_the_active_place():
    registry = {
        "X-OLD": {"name": "Old", "short": "Old", "kind": "export_port", "lat": 10.0, "lon": -40.0,
                  "basin": "north_atlantic", "effective_from": None, "effective_to": "2027-02-28"},
        "X-NEW": {"name": "New", "short": "New", "kind": "export_port", "lat": 10.0, "lon": -41.0,
                  "basin": "north_atlantic", "effective_from": "2027-03-01", "effective_to": None},
    }

    assert set(hazards.active_places(registry, date(2027, 2, 28))) == {"X-OLD"}
    assert set(hazards.active_places(registry, date(2027, 3, 1))) == {"X-NEW"}
    assert set(_assess({}, places=hazards.active_places(registry, date(2027, 2, 28)))) == {"X-OLD"}
    assert set(_assess({}, places=hazards.active_places(registry, date(2027, 3, 1)))) == {"X-NEW"}


def test_state_without_a_reason_is_rejected():
    with pytest.raises(ValueError, match="reason"):
        PlaceHazard(place_id="P-PNG", short="Paranaguá", state="not_covered", reason="")
    with pytest.raises(ValueError, match="state"):
        PlaceHazard(place_id="P-PNG", short="Paranaguá", state="maybe", reason="x")
    with pytest.raises(ValueError, match="severity"):
        PlaceHazard(place_id="P-NOLA", short="New Orleans", state="flag", severity="bad")


# ---------------------------------------------------------------------------
# Leg roll-up (§6.3)
# ---------------------------------------------------------------------------
def _ph(place_id: str, state: str, **kw) -> PlaceHazard:
    defaults = {"short": place_id, "reason": None}
    if state in ("not_covered", "stale", "failed"):
        defaults["reason"] = f"{state} reason"
    if state == "flag":
        defaults.update(severity=kw.pop("severity", "warning"), band_kt=34, source="NHC", storm_id="al092026",
                        storm_name="S", first_arrival_tau_h=kw.pop("first", 24),
                        first_arrival_at="2026-10-08T00:00:00Z", closest_km=100, closest_tau_h=30,
                        closest_at="2026-10-08T06:00:00Z", issued_at="2026-10-07T03:00:00Z",
                        synoptic_at="2026-10-07T00:00:00Z")
    if state == "watch":
        defaults.update(severity="info", source="NHC", storm_id="al092026", storm_name="S", closest_km=400,
                        closest_tau_h=30, closest_ts_km=400, closest_ts_tau_h=30, closest_ts_at="2026-10-08T06:00:00Z")
    defaults.update(kw)
    return PlaceHazard(place_id=place_id, state=state, **defaults)


def test_leg_with_no_exposed_place_has_no_hazard():
    results = {"P-NOLA": _ph("P-NOLA", "clear")}

    assert leg_hazard((), results) is None
    assert leg_hazard(("P-CBOT",), results) is None         # not in results → not exposed


@pytest.mark.parametrize(("states", "expected"), [
    (("clear", "flag"), "flag"),
    (("watch", "not_covered"), "watch"),
    (("failed", "stale", "clear"), "failed"),
    (("stale", "clear"), "stale"),
    (("not_covered", "not_covered"), "not_covered"),
    (("clear", "not_covered"), "clear"),
])
def test_leg_precedence(states, expected):
    results = {f"P-{i}": _ph(f"P-{i}", s) for i, s in enumerate(states)}
    leg = leg_hazard(tuple(results), results)

    assert isinstance(leg, LegHazard)
    assert leg.state == expected


def test_leg_partial_lists_the_uncovered_places():
    results = {"A": _ph("A", "flag"), "B": _ph("B", "not_covered"), "C": _ph("C", "stale")}
    leg = leg_hazard(("A", "B", "C"), results)

    assert leg.partial is True
    assert leg.uncovered == ("B", "C")
    assert leg.text.endswith(" · B not covered · C stale")

    assert leg_hazard(("B",), results).partial is False        # not_covered alone is not partial


def test_leg_worst_flag_is_primary():
    results = {"A": _ph("A", "flag", severity="warning", first=6), "B": _ph("B", "flag", severity="alert", first=48)}
    leg = leg_hazard(("A", "B"), results)

    assert leg.primary.place_id == "B" and leg.severity == "alert"
    assert [p.place_id for p in leg.places] == ["B", "A"]


# ---------------------------------------------------------------------------
# Labels (§8.4)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(("source", "prefix", "code", "vmax", "label"), [
    ("NHC", "al", "HU", 64, "Hurricane, category 1"),
    ("NHC", "al", "HU", 83, "Hurricane, category 2"),
    ("NHC", "al", "HU", 96, "Hurricane, category 3"),
    ("NHC", "al", "HU", 113, "Hurricane, category 4"),
    ("NHC", "al", "HU", 137, "Hurricane, category 5"),
    ("NHC", "al", "TS", 50, "Tropical Storm"),
    ("NHC", "al", "PTC", 30, "Potential Tropical Cyclone"),
    ("JTWC", "wp", None, 30, "Tropical Depression"),
    ("JTWC", "wp", None, 63, "Tropical Storm"),
    ("JTWC", "wp", None, 64, "Typhoon"),
    ("JTWC", "wp", None, 130, "Super Typhoon"),
    ("JTWC", "sh", None, 130, "Tropical Cyclone"),
    ("JTWC", "io", None, 40, "Tropical Cyclone"),
])
def test_storm_class_label(source, prefix, code, vmax, label):
    assert hazards.storm_class_label(source, prefix, code, vmax) == label


def test_unknown_nhc_code_is_rendered_raw_and_logged(caplog):
    with caplog.at_level("WARNING"):
        assert hazards.storm_class_label("NHC", "al", "XX", 50) == "XX"
    assert "XX" in caplog.text


@pytest.mark.parametrize(("source", "prefix", "band", "words"), [
    ("NHC", "al", 34, "tropical-storm-force winds"),
    ("NHC", "al", 50, "50-kt winds"),
    ("NHC", "al", 64, "hurricane-force winds"),
    ("JTWC", "wp", 64, "typhoon-force winds"),
    ("JTWC", "sh", 64, "64-kt winds"),
])
def test_band_words(source, prefix, band, words):
    assert hazards.band_words(source, prefix, band) == words


def test_storm_label_drops_the_advisory_for_jtwc():
    nhc = _ph("P-NOLA", "flag", advisory="10", storm_class_label="Hurricane, category 1", vmax_kt=80)
    jtwc = _ph("P-NCN", "flag", source="JTWC", storm_id="wp2726", storm_name="Koguma", advisory=None,
               storm_class_label="Tropical Storm", vmax_kt=35)

    assert hazards.storm_label(nhc) == "S (NHC adv 10, Hurricane, category 1, 80 kt)"
    assert hazards.storm_label(jtwc) == "Koguma (JTWC, Tropical Storm, 35 kt)"
    assert hazards.flag_sentence(jtwc).endswith("JTWC guidance, not a national warning.")


def test_format_utc():
    assert hazards.format_utc("2024-09-12T03:00:00Z") == "Thu 12 Sep 03:00Z"
    assert hazards.format_check_time("2026-10-06T19:04:11Z") == "19:04Z 6 Oct"
