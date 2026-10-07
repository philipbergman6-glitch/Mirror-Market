"""Footprint weights on the ECMWF 0.25° grid (docs/specs/footprint-weather.md §4.3, slice 0).

Run by hand on a desk machine with requirements-reference.txt installed. CI
never reads SPAM or Natural Earth.

For every SPAM 2020 5′ cell whose centre falls inside a footprint's admin-1
polygon(s), the cell's production of the footprint's crop is added to the
nearest ECMWF 0.25° node; weights are normalised to sum to 1 and each node's
land flag is read from the ECMWF land-sea mask (``lsm ≥ 0.5``). Pins (release
1 only) are the node nearest each ``GROWING_REGIONS`` pin; port boxes are the
3×3 nodes around the node nearest each ``PORT_RAIN_BOXES`` place.

Inputs come from the environment and are never committed:
``SPAM2020_DIR`` (the extracted ``spam2020_V2r2_global_P_{SOYB,SUNF,RAPE}_A.tif``),
``NATURAL_EARTH_ADMIN1`` (``ne_10m_admin_1_states_provinces.shp``, 5.1.2) and
``ECMWF_LSM_GRIB`` (one ``lsm`` step-0 message from any IFS 0.25° run).

Writes ``weights.csv`` and ``manifest.json`` beside ``crosswalk.csv``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

OUT_DIR = PROJECT_ROOT / "data" / "reference" / "footprints"
SPAM_VERSION = "2020 V2r2"
NATURAL_EARTH_VERSION = "5.1.2"
# Its VERSION.txt still reads 5.1.1: the admin-1 layer did not change in the
# 5.1.2 release. The hash pins the file the GitHub tag v5.1.2 serves.
NATURAL_EARTH_SHP_SHA256 = "c6f5c8b4b1320d9417033762419c6df1eb423989cd880fba78ea0b1e3522cbe4"
GRID_STEP = 0.25
GRID_NI, GRID_NJ = 1440, 721  # IFS 0.25°: lon −180 … 179.75 east, lat 90 … −90 south
LAND_LSM = 0.5
MIN_LAND_WEIGHT = 0.5
PORT_BOX_HALF = 1  # 3×3 [config.PORT_RAIN_BOX_CELLS]


def _env_path(name: str) -> Path:
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"{name} is not set")
    path = Path(value).expanduser()
    if not path.exists():
        raise SystemExit(f"{name}={path} does not exist")
    return path


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def node_of(lat, lon):
    """Nearest IFS 0.25° node as (row, col); row 0 is 90° N, col 0 is 180° W."""
    row = np.rint((90.0 - np.asarray(lat)) / GRID_STEP).astype(int)
    col = np.rint((np.asarray(lon) + 180.0) / GRID_STEP).astype(int) % GRID_NI
    return row, col


def node_latlon(row, col):
    return 90.0 - np.asarray(row) * GRID_STEP, -180.0 + np.asarray(col) * GRID_STEP


def read_lsm(path: Path) -> np.ndarray:
    import eccodes

    with open(path, "rb") as fh:
        h = eccodes.codes_grib_new_from_file(fh)
        if h is None:
            raise SystemExit(f"{path}: no GRIB message")
        try:
            meta = {k: eccodes.codes_get(h, k) for k in (
                "shortName", "Ni", "Nj", "latitudeOfFirstGridPointInDegrees",
                "longitudeOfFirstGridPointInDegrees", "iDirectionIncrementInDegrees", "jScansPositively")}
            values = eccodes.codes_get_values(h)
        finally:
            eccodes.codes_release(h)
    expected = {"shortName": "lsm", "Ni": GRID_NI, "Nj": GRID_NJ,
                "latitudeOfFirstGridPointInDegrees": 90.0,
                "longitudeOfFirstGridPointInDegrees": 180.0,
                "iDirectionIncrementInDegrees": GRID_STEP, "jScansPositively": 0}
    if meta != expected:
        raise SystemExit(f"{path}: unexpected grid {meta}")
    return values.reshape(GRID_NJ, GRID_NI)


def read_crosswalk(path: Path) -> dict[str, list[dict]]:
    by_fp: dict[str, list[dict]] = defaultdict(list)
    with open(path, newline="") as fh:
        for row in csv.DictReader(fh):
            by_fp[row["footprint"]].append(row)
    if set(by_fp) != set(config.FOOTPRINTS):
        raise SystemExit(f"crosswalk footprints ≠ config.FOOTPRINTS: "
                         f"{sorted(set(by_fp) ^ set(config.FOOTPRINTS))}")
    return by_fp


def footprint_geometry(key: str, spec: dict, xwalk: list[dict], ne_by_iso: dict, ne_by_region: dict):
    """Union of the footprint's admin-1 polygons, verified row by row against the crosswalk."""
    from shapely.geometry import shape
    from shapely.ops import unary_union

    units = spec["units"]
    if units == ("FR-GES",):
        dissolved = {r["iso_3166_2"] for r in ne_by_region.get("FR-GES", [])}
        if dissolved != {x["iso_3166_2"] for x in xwalk}:
            raise SystemExit(f"{key}: region_cod=FR-GES rows {sorted(dissolved)} ≠ crosswalk")
    elif tuple(x["iso_3166_2"] for x in xwalk) != units:
        raise SystemExit(f"{key}: crosswalk units ≠ config units {units}")
    shapes = []
    for x in xwalk:
        hits = ne_by_iso.get(x["iso_3166_2"], [])
        if len(hits) != 1:
            raise SystemExit(f"{key}: {x['iso_3166_2']} has {len(hits)} Natural Earth rows")
        rec, geom = hits[0]
        if rec["wikidataid"] != x["wikidata_id"] or rec["adm1_code"] != x["ne_adm1_code"]:
            raise SystemExit(f"{key}: {x['iso_3166_2']} is {rec['adm1_code']}/{rec['wikidataid']} in "
                             f"Natural Earth, {x['ne_adm1_code']}/{x['wikidata_id']} in the crosswalk")
        shapes.append(shape(geom))
    return unary_union(shapes)


def spam_cells(tif: Path, geom):
    """Centres, production and nodata flags of the SPAM cells inside ``geom``."""
    import rasterio
    from rasterio.windows import from_bounds
    from shapely import contains_xy

    with rasterio.open(tif) as r:
        window = from_bounds(*geom.bounds, r.transform).round_offsets().round_lengths()
        prod = r.read(1, window=window).astype("float64")
        t = r.window_transform(window)
        nodata = r.nodata
    rows, cols = np.indices(prod.shape)
    lon = t.c + (cols + 0.5) * t.a
    lat = t.f + (rows + 0.5) * t.e
    inside = contains_xy(geom, lon, lat)
    p = prod[inside]
    if np.isnan(p).any():
        # SPAM marks "no crop" with the nodata sentinel, never NaN: a NaN is a corrupt file.
        raise SystemExit(f"{tif.name}: {int(np.isnan(p).sum())} NaN cells inside the polygon")
    no_crop = (p == nodata) | (p < 0)
    return lat[inside], lon[inside], np.where(no_crop, 0.0, p), no_crop


def collapse(rows, cols, weights) -> list[tuple[int, int, float]]:
    acc: dict[tuple[int, int], float] = defaultdict(float)
    for r, c, w in zip(rows.tolist(), cols.tolist(), weights.tolist(), strict=True):
        acc[(r, c)] += w
    total = sum(acc.values())
    return [(r, c, w / total) for (r, c), w in sorted(acc.items())]


def build(skip_ports: bool) -> None:
    import shapefile

    spam_dir = _env_path("SPAM2020_DIR")
    ne_path = _env_path("NATURAL_EARTH_ADMIN1")
    lsm_path = _env_path("ECMWF_LSM_GRIB")
    if _sha256(ne_path) != NATURAL_EARTH_SHP_SHA256:
        raise SystemExit(f"{ne_path} is not Natural Earth {NATURAL_EARTH_VERSION} admin-1")
    crosswalk_path = OUT_DIR / "crosswalk.csv"
    crosswalk = read_crosswalk(crosswalk_path)
    lsm = read_lsm(lsm_path)

    ne_by_iso: dict[str, list] = defaultdict(list)
    ne_by_region: dict[str, list] = defaultdict(list)
    for sr in shapefile.Reader(str(ne_path)).iterShapeRecords():
        rec = sr.record.as_dict()
        ne_by_iso[rec["iso_3166_2"]].append((rec, sr.shape.__geo_interface__))
        ne_by_region[rec["region_cod"]].append(rec)

    inputs = {"natural_earth_shp": ne_path, "natural_earth_dbf": ne_path.with_suffix(".dbf"),
              "ecmwf_lsm": lsm_path, "crosswalk": crosswalk_path}
    rows_out: list[dict] = []
    stats: dict[str, dict] = {}

    def emit(footprint, kind, nodes):
        for r, c, w in nodes:
            lat, lon = node_latlon(r, c)
            rows_out.append({"footprint": footprint, "kind": kind, "grid_row": r, "grid_col": c,
                             "lat": round(float(lat), 2), "lon": round(float(lon), 2),
                             "weight": round(w, 9), "land": int(lsm[r, c] >= LAND_LSM)})

    for key, spec in config.FOOTPRINTS.items():
        tif = spam_dir / f"spam2020_V2r2_global_P_{spec['crop']}_A.tif"
        if not tif.exists():
            raise SystemExit(f"{key}: {tif} missing")
        inputs[f"spam_{spec['crop']}"] = tif
        geom = footprint_geometry(key, spec, crosswalk[key], ne_by_iso, ne_by_region)
        lat, lon, prod, missing = spam_cells(tif, geom)
        total = prod.sum()
        if total <= 0:
            raise SystemExit(f"{key}: zero {spec['crop']} production inside the footprint")
        # SPAM's nodata is "no recorded crop" (lakes, forest, Pantanal), so it is
        # zero production and reported, not failed: it is 16 % of Mato Grosso's
        # cells, and Illinois's are Lake Michigan. NaN is failed in spam_cells.
        area = np.cos(np.radians(lat))
        nodata_share = float(area[missing].sum() / area.sum())
        r, c = node_of(lat, lon)
        nodes = collapse(r, c, prod)
        land_weight = sum(w for rr, cc, w in nodes if lsm[rr, cc] >= LAND_LSM)
        if land_weight < MIN_LAND_WEIGHT:
            raise SystemExit(f"{key}: land weight {land_weight:.2f} < {MIN_LAND_WEIGHT}")
        ws = np.sort([w for _, _, w in nodes])[::-1]
        stats[key] = {"nodes": len(nodes), "production_t": round(float(total)),
                      "land_weight": round(land_weight, 4),
                      "top_quarter_share": round(float(ws[: max(1, len(ws) // 4)].sum()), 4),
                      "nodata_share": round(nodata_share, 5)}
        emit(key, "crop", nodes)

        pin = config.GROWING_REGIONS[key]
        pr, pc = node_of(pin["lat"], pin["lon"])
        emit(f"pin:{key}", "pin", [(int(pr), int(pc), 1.0)])

    if skip_ports:
        ports_included = False
    else:
        places = getattr(config, "PLACES", None)
        boxes = getattr(config, "PORT_RAIN_BOXES", None)
        if places is None or boxes is None:
            raise SystemExit("config.PLACES / PORT_RAIN_BOXES missing (storm-flags slice 2); "
                             "pass --skip-ports to build crop and pin weights only")
        for pid in boxes:
            pr, pc = node_of(places[pid]["lat"], places[pid]["lon"])
            span = range(-PORT_BOX_HALF, PORT_BOX_HALF + 1)
            box = [(int(pr) + dr, (int(pc) + dc) % GRID_NI, 1.0) for dr in span for dc in span]
            emit(pid, "port", [(rr, cc, w / len(box)) for rr, cc, w in box])
        ports_included = True

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUT_DIR / "weights.csv", "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows_out[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows_out)
    manifest = {
        "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "spam_version": SPAM_VERSION,
        "natural_earth_version": NATURAL_EARTH_VERSION,
        "ports_included": ports_included,
        "inputs_sha256": {name: _sha256(p) for name, p in sorted(inputs.items())},
        "footprints": stats,
    }
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=1, ensure_ascii=False) + "\n")
    print(f"weights.csv: {len(rows_out)} rows; ports {'included' if ports_included else 'SKIPPED'}")
    for key, s in stats.items():
        print(f"  {key:36s} {s['nodes']:5d} nodes  {s['production_t']:>11,} t  "
              f"land {s['land_weight']:.2f}  top¼ {s['top_quarter_share']:.2f}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--skip-ports", action="store_true",
                        help="build crop and pin weights only (until config.PLACES exists)")
    build(parser.parse_args().skip_ports)


if __name__ == "__main__":
    main()
