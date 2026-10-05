"""Footprint weights on the ECMWF 0.25 deg grid, built from SPAM 5' cells.

Each SPAM cell centre inside the polygon contributes a weight to its nearest ECMWF grid point.
Methods: admin1_area (cos-lat area), admin1_soy (SPAM 2020 soy production), county_nass (Iowa only:
NASS 2021-25 mean county production spread evenly over each county's cells), pin (nearest point)."""
import json, pickle, time
from collections import defaultdict
import numpy as np, rasterio, shapefile
from rasterio.windows import from_bounds
from shapely.geometry import shape
from shapely import contains_xy

PINS = {"BR-MT": (-12.64, -55.42), "US-IA": (42.03, -93.47)}  # config.GROWING_REGIONS

def ecmwf_index(lat, lon):
    row = np.rint((90.0 - lat) / 0.25).astype(int)
    col = np.rint((np.asarray(lon) + 180.0) / 0.25).astype(int) % 1440
    return row * 1440 + col

def admin1(iso):
    sf = shapefile.Reader("ne/ne_10m_admin_1_states_provinces")
    names = [f[0] for f in sf.fields[1:]]
    hits = [shape(s.shape.__geo_interface__) for s in sf.iterShapeRecords() if s.record[names.index("iso_3166_2")] == iso]
    if len(hits) != 1:
        raise SystemExit(f"{iso}: expected 1 polygon, got {len(hits)}")  # hard-fail, no fuzzy join
    return hits[0]

def spam_cells(geom):
    with rasterio.open("spam/spam2020_V2r2_global_P_SOYB_A.tif") as r:
        w = from_bounds(*geom.bounds, r.transform).round_offsets().round_lengths()
        prod = r.read(1, window=w).astype("float64")
        t = r.window_transform(w)
    prod[prod < 0] = 0.0  # nodata (-3.4e38) = no recorded soy
    rows, cols = np.indices(prod.shape)
    lon = t.c + (cols + 0.5) * t.a
    lat = t.f + (rows + 0.5) * t.e
    inside = contains_xy(geom, lon, lat)
    return lat[inside], lon[inside], prod[inside]

def collapse(idx, w):
    acc = defaultdict(float)
    for i, x in zip(idx, w):
        acc[int(i)] += float(x)
    keys = np.array(sorted(acc)); vals = np.array([acc[k] for k in keys])
    return keys, vals / vals.sum()

def county_weights(lat, lon):
    nass = json.load(open("nass_ia.json"))
    by = defaultdict(list)
    for r in nass:
        if 2021 <= r["year"] <= 2025:
            by[r["fips"]].append(float(r["bu"].replace(",", "")))
    mean = {f: np.mean(v) for f, v in by.items() if not f.endswith("998")}
    other = [f for f in by if f.endswith("998")]
    sf = shapefile.Reader("ne/cb_2024_us_county_500k")
    names = [f[0] for f in sf.fields[1:]]
    w = np.zeros(lat.size); hit = np.zeros(lat.size, bool); missing = []
    for s in sf.iterShapeRecords():
        rec = dict(zip(names, s.record))
        if rec["STATEFP"] != "19":
            continue
        g = shape(s.shape.__geo_interface__)
        m = contains_xy(g, lon, lat)
        hit |= m
        if rec["GEOID"] not in mean:
            missing.append(rec["NAME"]); continue
        if m.sum():
            w[m] = mean[rec["GEOID"]] / m.sum()
    return w, {"counties_with_data": len(mean), "counties_missing": missing, "other_combined_rows": other,
               "cells_outside_counties": int((~hit).sum())}

if __name__ == "__main__":
    t0 = time.time(); out = {}; meta = {}
    for iso, (plat, plon) in PINS.items():
        geom = admin1(iso)
        lat, lon, prod = spam_cells(geom)
        idx = ecmwf_index(lat, lon)
        fp = {"admin1_area": collapse(idx, np.cos(np.radians(lat))),
              "admin1_soy": collapse(idx, prod),
              "pin": (ecmwf_index(np.array([plat]), np.array([plon])), np.array([1.0]))}
        info = {"spam_cells": int(lat.size), "ecmwf_points": int(fp["admin1_area"][0].size),
                "spam_soy_t": round(float(prod.sum())), "cells_with_soy_pct": round(100 * float((prod > 0).mean()), 1)}
        if iso == "US-IA":
            cw, cinfo = county_weights(lat, lon)
            fp["county_nass"] = collapse(idx, cw); info.update(cinfo)
        # share of weight in the top 25% of points: how concentrated is the crop?
        k, v = fp["admin1_soy"]; vs = np.sort(v)[::-1]
        info["soy_weight_in_top_quarter_of_points_pct"] = round(100 * vs[: max(1, len(vs) // 4)].sum(), 1)
        out[iso] = fp; meta[iso] = info
    pickle.dump(out, open("footprints.pkl", "wb"))
    meta["build_seconds"] = round(time.time() - t0, 1)
    json.dump(meta, open("footprints_meta.json", "w"), indent=1); print(json.dumps(meta, indent=1))
