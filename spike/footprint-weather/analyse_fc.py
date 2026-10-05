"""Daily footprint aggregates from the decoded ECMWF points; pin comparison vs Open-Meteo (Layer 5)."""
import json, pickle, sys, time
import numpy as np
RUN = sys.argv[1] if len(sys.argv) > 1 else "20261005"
D = pickle.load(open(f"ecmwf_{RUN}.pkl", "rb")); fp = pickle.load(open("footprints.pkl", "rb"))
U = D["union"]; F = D["fields"]; pos = {int(i): k for k, i in enumerate(U)}
lsm = F[("lsm", None, 0)]["v"]
t0 = time.time()

def per_point_daily():
    tp = {d: (F[("tp", None, 24 * d)]["v"] - (F[("tp", None, 24 * (d - 1))]["v"] if d > 1 else 0)) * 1000 for d in range(1, 16)}
    tx, tn = {}, {}
    for d in range(1, 16):
        if d <= 6:
            st = range(24 * (d - 1) + 3, 24 * d + 1, 3); px, pn = "mx2t3", "mn2t3"
        else:
            st = range(24 * (d - 1) + 6, 24 * d + 1, 6); px, pn = "mx2t6", "mn2t6"
        tx[d] = np.max([F[(px, None, s)]["v"] for s in st], axis=0) - 273.15
        tn[d] = np.min([F[(pn, None, s)]["v"] for s in st], axis=0) - 273.15
    vsw = {(l, d): F[("vsw", str(l), 24 * d)]["v"] for l in (1, 2, 3, 4) for d in range(0, 16)}
    return tp, tx, tn, vsw

tp, tx, tn, vsw = per_point_daily()
def wmean(idx, w, arr, mask=None):
    k = np.array([pos[int(i)] for i in idx]); m = lsm[k] > 0.5
    if mask is not None: m &= mask[k]
    if not m.any(): return None
    ww = w[m] / w[m].sum(); return float((arr[k][m] * ww).sum())

out = {}
for iso, methods in fp.items():
    out[iso] = {}
    for meth, (idx, w) in methods.items():
        k = np.array([pos[int(i)] for i in idx])
        rows = []
        for d in range(1, 16):
            rows.append({"day": d, "tp_mm": wmean(idx, w, tp[d]), "tmax_c": wmean(idx, w, tx[d]), "tmin_c": wmean(idx, w, tn[d]),
                         "heat35_area_pct": None if meth == "pin" else 100 * wmean(idx, w, (tx[d] >= 35).astype(float)),
                         "vsw1": wmean(idx, w, vsw[(1, d)], vsw[(1, d)] > 0), "vsw2": wmean(idx, w, vsw[(2, d)], vsw[(2, d)] > 0)})
        tot = np.sum([tp[d] for d in range(1, 16)], axis=0); tot7 = np.sum([tp[d] for d in range(1, 8)], axis=0)
        s = {"tp15_mm": sum(r["tp_mm"] for r in rows), "tp7_mm": sum(r["tp_mm"] for r in rows[:7]),
             "tmax_mean_c": np.mean([r["tmax_c"] for r in rows]), "tmax_peak_c": max(r["tmax_c"] for r in rows),
             "days_mean_tmax_ge34": sum(r["tmax_c"] >= 34 for r in rows), "days_mean_tmax_ge38": sum(r["tmax_c"] >= 38 for r in rows),
             "vsw1_day0": wmean(idx, w, vsw[(1, 0)], vsw[(1, 0)] > 0), "vsw1_day15": rows[-1]["vsw1"],
             "lead_dry_days_lt1mm": next((r["day"] - 1 for r in rows if r["tp_mm"] >= 1), 15),
             "sea_points_masked": int((lsm[k] <= 0.5).sum()), "land_vsw_zero_points": int(((lsm[k] > 0.5) & (vsw[(1, 0)][k] == 0)).sum())}
        dry = np.zeros(k.size); alive = np.ones(k.size, bool)
        for d in range(1, 16):
            alive &= tp[d][k] < 1.0; dry += alive
        if len(idx) > 1:
            m = lsm[k] > 0.5
            s["area_lead_dry_ge10d_pct"] = round(100 * float(w[m & (dry >= 10)].sum() / w[m].sum()), 1)
            s["area_lead_dry_ge5d_pct"] = round(100 * float(w[m & (dry >= 5)].sum() / w[m].sum()), 1)
        if len(idx) > 1:  # spread of 15-day totals across the footprint (weighted percentiles)
            o = np.argsort(tot[k]); cw = np.cumsum(w[o]) / w.sum(); v = tot[k][o]
            s["tp15_p10_p50_p90"] = [float(v[np.searchsorted(cw, q)]) for q in (0.1, 0.5, 0.9)]
        out[iso][meth] = {"summary": s, "daily": rows}
# where does the pin's own 15-day total sit inside the soy-weighted footprint distribution?
for iso, methods in fp.items():
    idx, w = methods["admin1_soy"]; k = np.array([pos[int(i)] for i in idx])
    tot = np.sum([tp[d] for d in range(1, 16)], axis=0)
    pin_tot = out[iso]["pin"]["summary"]["tp15_mm"]
    out[iso]["pin"]["summary"]["pin_percentile_in_soy_footprint"] = round(100 * float(w[tot[k] <= pin_tot].sum() / w.sum()), 1)
# Open-Meteo (Layer 5) pin comparison, days 1-7 (OM days are local; ECMWF days are UTC 00-24)
om = json.load(open("openmeteo_pins.json"))
for iso, key in [] if RUN != "20261005" else (("BR-MT", "Brazil Mato Grosso"), ("US-IA", "US Midwest (Iowa)")):
    rows = om[key][:7]
    cmp = []
    for d, r in enumerate(rows, 1):
        cmp.append({"date": r["Date"], "om_pin_tp": r["precipitation"], "ec_pin_tp": out[iso]["pin"]["daily"][d - 1]["tp_mm"],
                    "ec_soy_tp": out[iso]["admin1_soy"]["daily"][d - 1]["tp_mm"], "om_pin_tmax": r["temp_max"],
                    "ec_pin_tmax": out[iso]["pin"]["daily"][d - 1]["tmax_c"], "ec_soy_tmax": out[iso]["admin1_soy"]["daily"][d - 1]["tmax_c"]})
    out[iso]["vs_openmeteo"] = cmp
out["aggregate_s"] = round(time.time() - t0, 2)
json.dump(out, open(f"fc_{RUN}.json", "w"), indent=1, default=float)
for iso in fp:
    print("==", iso)
    for meth, v in out[iso].items():
        if meth == "vs_openmeteo": continue
        print(f"  {meth:12s}", {k: (round(x, 3) if isinstance(x, float) else ([round(y, 1) for y in x] if isinstance(x, list) else x)) for k, x in v["summary"].items()})
    print("  vs OM (date, om_pin, ec_pin, ec_soy tp | om, ec_pin, ec_soy tmax)")
    for c in out[iso]["vs_openmeteo"]:
        print("   ", c["date"], *[round(c[k], 1) for k in ("om_pin_tp", "ec_pin_tp", "ec_soy_tp")], "|", *[round(c[k], 1) for k in ("om_pin_tmax", "ec_pin_tmax", "ec_soy_tmax")])
print("aggregate_s", out["aggregate_s"])
