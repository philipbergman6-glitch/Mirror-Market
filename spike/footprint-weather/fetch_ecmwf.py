"""Byte-range fetch of ECMWF IFS 00z open data (tp, vsw 1-4, mx2t3/mn2t3 0-144h, mx2t6/mn2t6 150-360h, lsm),
decode with eccodes, keep only the grid points our footprints touch. Times every stage."""
import json, pickle, sys, time, concurrent.futures as cf
import numpy as np, requests, eccodes

RUN = sys.argv[1] if len(sys.argv) > 1 else "20261005"
BASE = f"https://data.ecmwf.int/forecasts/{RUN}/00z/ifs/0p25/oper/{RUN}000000-{{step}}h-oper-fc"
S = requests.Session()
fp = pickle.load(open("footprints.pkl", "rb"))
UNION = np.unique(np.concatenate([idx for f in fp.values() for idx, _ in f.values()]))

def wanted(step):
    w = []
    if step == 0: w.append(("lsm", None))
    if step % 24 == 0 and step > 0: w.append(("tp", None))
    if step % 24 == 0: w += [("vsw", str(l)) for l in (1, 2, 3, 4)]
    if 0 < step <= 144: w += [("mx2t3", None), ("mn2t3", None)]
    if step >= 150: w += [("mx2t6", None), ("mn2t6", None)]
    return w

STEPS = list(range(0, 145, 3)) + list(range(150, 361, 6))
T = {}; t0 = time.time()

def get_index(step):
    r = S.get(BASE.format(step=step) + ".index", timeout=60); r.raise_for_status()
    return step, [json.loads(l) for l in r.text.splitlines() if l.strip()]

with cf.ThreadPoolExecutor(16) as ex:
    indexes = dict(ex.map(get_index, STEPS))
T["index_s"] = round(time.time() - t0, 1)

jobs = []
for step in STEPS:
    recs = indexes[step]
    for param, lev in wanted(step):
        hit = [r for r in recs if r["param"] == param and r.get("levelist") == lev]
        if len(hit) != 1:  # invariant 1: the live index must carry exactly what we ask for
            raise SystemExit(f"step {step}: {param}/{lev} -> {len(hit)} index records")
        jobs.append((step, param, lev, hit[0]["_offset"], hit[0]["_length"]))

def fetch(job):
    step, param, lev, off, n = job
    r = S.get(BASE.format(step=step) + ".grib2", headers={"Range": f"bytes={off}-{off + n - 1}"}, timeout=120)
    if r.status_code != 206 or len(r.content) != n:
        raise SystemExit(f"range fetch {step} {param}: HTTP {r.status_code}, {len(r.content)}/{n} bytes")
    return job, r.content

def decode(blob):
    h = eccodes.codes_new_from_message(blob)
    try:
        g = {k: eccodes.codes_get(h, k) for k in ("Ni", "Nj", "latitudeOfFirstGridPointInDegrees",
             "longitudeOfFirstGridPointInDegrees", "jScansPositively", "shortName", "units", "stepRange")}
        if (g["Ni"], g["Nj"], g["latitudeOfFirstGridPointInDegrees"], g["jScansPositively"]) != (1440, 721, 90.0, 0) \
                or g["longitudeOfFirstGridPointInDegrees"] not in (180.0, -180.0):
            raise SystemExit(f"unexpected grid {g}")
        return g, eccodes.codes_get_values(h)
    finally:
        eccodes.codes_release(h)

t1 = time.time(); fields = {}; nbytes = 0; tdec = 0.0
with cf.ThreadPoolExecutor(16) as ex:
    for (step, param, lev, off, n), blob in ex.map(fetch, jobs):
        nbytes += n
        td = time.time(); g, vals = decode(blob); tdec += time.time() - td
        fields[(param, lev, step)] = {"units": g["units"], "stepRange": g["stepRange"], "v": vals[UNION].astype("float64")}
T["fetch_decode_s"] = round(time.time() - t1, 1); T["decode_cpu_s"] = round(tdec, 1)
T["messages"] = len(jobs); T["mb"] = round(nbytes / 1e6, 1); T["total_s"] = round(time.time() - t0, 1)
pickle.dump({"union": UNION, "fields": fields, "run": RUN}, open(f"ecmwf_{RUN}.pkl", "wb"))
json.dump(T, open(f"ecmwf_{RUN}_timing.json", "w")); print(T)
