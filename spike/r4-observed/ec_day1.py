"""R4: 'persisted ECMWF day 1' — the 0-24 h slice of each IFS 00z run (UTC day D of run D), backfilled from the AWS mirror.
Per run: tp@24h, mx2t3@3..24h (8 msgs), vsw L1@0h; lsm once. Byte-range fetch + eccodes, as K2's fetch_ecmwf.py."""
import json, pickle, sys, time, concurrent.futures as cf
import numpy as np, pandas as pd, requests, eccodes
sys.path.insert(0, "/Users/philipbergman/Documents/Coding_Projects/Mirror-Market-r4/spike/footprint-weather")
from footprints import ecmwf_index, collapse  # noqa: E402
from common import cells, FOOTPRINTS  # noqa: E402

START, END = sys.argv[1], sys.argv[2]
HOST = sys.argv[3] if len(sys.argv) > 3 else "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com"
S = requests.Session(); S.mount("https://", requests.adapters.HTTPAdapter(pool_maxsize=32))
fp = {}
for iso in FOOTPRINTS:
    lat, lon, prod = cells(iso); fp[iso] = collapse(ecmwf_index(lat, lon), prod)
UNION = np.unique(np.concatenate([i for i, _ in fp.values()]))

def base(run, step):
    return f"{HOST}/{run}/00z/ifs/0p25/oper/{run}000000-{step}h-oper-fc"

def want(step):
    w = []
    if step == 0: w += [("vsw", "1"), ("lsm", None)]
    if step == 24: w.append(("tp", None))
    if step > 0: w.append(("mx2t3", None))
    return w

def fetch_run(run):
    t0 = time.time(); nbytes = 0; out = {}
    for step in range(0, 25, 3):
        r = S.get(base(run, step) + ".index", timeout=60)
        if r.status_code != 200:
            return run, {"error": f"index step {step}: HTTP {r.status_code}"}
        recs = [json.loads(l) for l in r.text.splitlines() if l.strip()]
        for param, lev in want(step):
            hit = [x for x in recs if x["param"] == param and x.get("levelist") == lev]
            if len(hit) != 1:
                return run, {"error": f"step {step}: {param}/{lev} -> {len(hit)} index records; params={sorted({x['param'] for x in recs})[:60]}"}
            off, n = hit[0]["_offset"], hit[0]["_length"]
            b = S.get(base(run, step) + ".grib2", headers={"Range": f"bytes={off}-{off + n - 1}"}, timeout=120)
            if b.status_code != 206 or len(b.content) != n:
                return run, {"error": f"range {step} {param}: HTTP {b.status_code}"}
            nbytes += n
            h = eccodes.codes_new_from_message(b.content)
            try:
                if (eccodes.codes_get(h, "Ni"), eccodes.codes_get(h, "Nj"), eccodes.codes_get(h, "latitudeOfFirstGridPointInDegrees")) != (1440, 721, 90.0):
                    return run, {"error": "unexpected grid"}
                out[(param, step)] = eccodes.codes_get_values(h)[UNION].astype("float32")
            finally:
                eccodes.codes_release(h)
    return run, {"tp": out[("tp", 24)] * 1000, "tx": np.max([out[("mx2t3", s)] for s in range(3, 25, 3)], axis=0) - 273.15,
                 "vsw1": out[("vsw", 0)], "lsm": out[("lsm", 0)], "bytes": nbytes, "seconds": round(time.time() - t0, 2)}

runs = [d.strftime("%Y%m%d") for d in pd.date_range(START, END)]
t0 = time.time(); res = {}
with cf.ThreadPoolExecutor(12) as ex:
    for run, r in ex.map(fetch_run, runs):
        res[run] = r
        if "error" in r: print(run, r["error"], flush=True)
ok = [r for r in runs if "error" not in res[r]]
T = {"runs_asked": len(runs), "runs_ok": len(ok), "wall_s": round(time.time() - t0, 1), "mb": round(sum(res[r]["bytes"] for r in ok) / 1e6, 1),
     "per_run_serial_s_median": float(np.median([res[r]["seconds"] for r in ok])) if ok else None, "host": HOST,
     "missing": {r: res[r]["error"][:160] for r in runs if "error" in res[r]}}
pickle.dump({"union": UNION, "fp": fp, "res": res}, open(f"ec/day1_{START}_{END}.pkl", "wb"))
json.dump(T, open(f"ec/day1_{START}_{END}_timing.json", "w"), indent=1); print(json.dumps(T, indent=1))
