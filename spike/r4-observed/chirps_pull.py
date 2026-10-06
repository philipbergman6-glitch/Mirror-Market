"""R4: CHIRPS v3 daily over the two footprints via windowed /vsicurl reads (no full-file download).
final/sat + final/rnl through the last final month, prelim/sat after. Records per-file seconds and HTTP status."""
import json, pickle, sys, time, concurrent.futures as cf
import numpy as np, pandas as pd, rasterio
from rasterio.windows import from_bounds
from common import BOXES, FOOTPRINTS, grid_weights, wmean
B = "https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily"
START, END, FINAL_END = sys.argv[1], sys.argv[2], sys.argv[3]
ENV = dict(GDAL_DISABLE_READDIR_ON_OPEN="EMPTY_DIR", CPL_VSIL_CURL_ALLOWED_EXTENSIONS=".tif", GDAL_HTTP_MAX_RETRY="3", GDAL_HTTP_RETRY_DELAY="2")
_w = {}

def url(kind, d):
    if kind == "prelim":
        return f"{B}/prelim/sat/{d:%Y}/chirps-v3.0.prelim.{d:%Y.%m.%d}.tif"
    return f"{B}/final/{kind}/{d:%Y}/chirps-v3.0.{kind}.{d:%Y.%m.%d}.tif"

def read(job):
    kind, d = job; t0 = time.time(); out = {}
    try:
        with rasterio.Env(**ENV), rasterio.open("/vsicurl/" + url(kind, d)) as r:
            meta = {"res": r.res, "block": r.block_shapes[0], "compress": r.compression.value if r.compression else None, "nodata": r.nodata, "shape": r.shape}
            for iso, box in FOOTPRINTS.items():
                n, w_, s, e = BOXES[box]
                win = from_bounds(w_, s, e, n, r.transform).round_offsets().round_lengths()
                a = r.read(1, window=win).astype("float64"); t = r.window_transform(win)
                a[a < 0] = np.nan  # nodata -9999 (sea / no estimate) is never zero rain
                lats = t.f + (np.arange(a.shape[0]) + 0.5) * t.e; lons = t.c + (np.arange(a.shape[1]) + 0.5) * t.a
                if iso not in _w: _w[iso] = grid_weights(iso, lats, lons)
                rows, cols, w = _w[iso]
                v, lost = wmean(a, rows, cols, w)
                out[iso] = {"mm": v, "lost_w": lost, "wet_share": float(w[np.nan_to_num(a[rows, cols]) >= 1.0].sum())}
        return job, {"ok": True, "s": round(time.time() - t0, 2), "meta": meta, **out}
    except Exception as ex:
        return job, {"ok": False, "s": round(time.time() - t0, 2), "error": repr(ex)[:200]}

days = pd.date_range(START, END); fin = pd.Timestamp(FINAL_END)
jobs = [(k, d) for d in days for k in (("sat", "rnl") if d <= fin else ("prelim",))]
read(jobs[0])  # build weights single-threaded
t0 = time.time(); res = {}
with cf.ThreadPoolExecutor(8) as ex:
    for (k, d), r in ex.map(read, jobs):
        res[(k, d.strftime("%Y-%m-%d"))] = r
        if not r["ok"]: print(k, d.date(), r["error"], flush=True)
ok = [r for r in res.values() if r["ok"]]
T = {"files": len(jobs), "ok": len(ok), "wall_s": round(time.time() - t0, 1), "median_file_s": float(np.median([r["s"] for r in ok])), "meta": ok[0]["meta"] if ok else None}
pickle.dump(res, open("chirps/chirps.pkl", "wb")); json.dump(T, open("chirps/timing.json", "w"), indent=1, default=str); print(json.dumps(T, indent=1, default=str))
