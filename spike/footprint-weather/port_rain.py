"""Port rain as a footprint: 3x3 ECMWF cells (~75 km box) around the port, sea cells kept (rain over the berth counts)."""
import json, time, numpy as np, requests, eccodes
RUN = "20261005"; BASE = f"https://data.ecmwf.int/forecasts/{RUN}/00z/ifs/0p25/oper/{RUN}000000-{{step}}h-oper-fc"
PORTS = {"P-PNG": (-25.5047, -48.5108), "P-NOLA": (29.9369, -90.0619), "P-UPR": (-32.9575, -60.6394)}
def box(lat, lon):
    r0 = int(round((90 - lat) / 0.25)); c0 = int(round((lon + 180) / 0.25)) % 1440
    return np.array([(r0 + dr) * 1440 + (c0 + dc) % 1440 for dr in (-1, 0, 1) for dc in (-1, 0, 1)])
idx = {p: box(*c) for p, c in PORTS.items()}
t0 = time.time(); acc = {}; nb = 0
for step in range(24, 361, 24):
    rec = [json.loads(l) for l in requests.get(BASE.format(step=step) + ".index", timeout=60).text.splitlines() if '"tp"' in l]
    assert len(rec) == 1
    r = requests.get(BASE.format(step=step) + ".grib2", headers={"Range": f"bytes={rec[0]['_offset']}-{rec[0]['_offset'] + rec[0]['_length'] - 1}"}, timeout=60)
    nb += len(r.content); h = eccodes.codes_new_from_message(r.content); v = eccodes.codes_get_values(h); eccodes.codes_release(h)
    acc[step] = {p: v[i] * 1000 for p, i in idx.items()}
out = {}
for p in PORTS:
    daily = [float(np.mean(acc[s][p] - (acc[s - 24][p] if s > 24 else 0))) for s in range(24, 361, 24)]
    wet = [float(np.mean((acc[s][p] - (acc[s - 24][p] if s > 24 else 0)) >= 1.0)) for s in range(24, 361, 24)]
    out[p] = {"tp15_mm": round(sum(daily), 1), "daily_mm": [round(x, 1) for x in daily],
              "days_box_mean_ge_1mm": sum(x >= 1 for x in daily), "days_box_mean_ge_5mm": sum(x >= 5 for x in daily)}
out["_cost"] = {"messages": 15, "mb": round(nb / 1e6, 1), "seconds": round(time.time() - t0, 1)}
json.dump(out, open("port_rain.json", "w"), indent=1); print(json.dumps(out, indent=0))
