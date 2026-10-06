"""R4: build per-footprint daily series from every candidate and score agreement with the ECMWF day-1 footprint.
All series are SPAM-2020-soy-weighted admin-1 means. Day labels are each product's own (see findings for the windows)."""
import glob, json, os, pickle, sys, zipfile
import numpy as np, pandas as pd, xarray as xr
sys.path.insert(0, "/Users/philipbergman/Documents/Coding_Projects/Mirror-Market")
from common import FOOTPRINTS, grid_weights, wmean  # noqa: E402
import config  # noqa: E402  thresholds only
DRY, SPELL, WIN, BASE, MINOBS, DEF = (config.WEATHER_DRY_THRESHOLD_MM, config.WEATHER_DRY_SPELL_ALERT_DAYS, config.WEATHER_PRECIP_DEFICIT_WINDOW_DAYS,
                                      config.WEATHER_PRECIP_DEFICIT_BASELINE_DAYS, config.WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS, config.WEATHER_PRECIP_DEFICIT_ALERT_PCT)
S = {iso: {} for iso in FOOTPRINTS}   # iso -> name -> Series
LOST = {}

# --- ECMWF day 1 (already on footprint points) ---
ec = pickle.load(open(sorted(glob.glob("ec/day1_*.pkl"))[-1], "rb")); U = ec["union"]; pos = {int(i): k for k, i in enumerate(U)}
for iso in FOOTPRINTS:
    idx, w = ec["fp"][iso]; k = np.array([pos[int(i)] for i in idx]); rows = {}
    for run, r in ec["res"].items():
        m = r["lsm"][k] > 0.5; ww = w[m] / w[m].sum()
        rows[pd.Timestamp(run)] = {"tp": float((np.clip(r["tp"][k][m], 0, None) * ww).sum()), "tx": float((r["tx"][k][m] * ww).sum()),
                                   "vsw1": float((r["vsw1"][k][m] * ww).sum()), "wet": float(ww[r["tp"][k][m] >= DRY].sum()),
                                   "min_tp": float(r["tp"][k].min())}
    df = pd.DataFrame(rows).T.sort_index()
    S[iso]["ec1_tp"], S[iso]["ec1_tx"], S[iso]["ec1_vsw1"], S[iso]["ec1_wet"] = df.tp, df.tx, df.vsw1, df.wet
    LOST[f"{iso} ec1 most-negative tp mm"] = float(df.min_tp.min())

# --- ERA5T hourly (CDS zips: accum + max streams) ---
def era5(box):
    tp, mx = [], []
    for f in sorted(glob.glob(f"cds/{box}_era5_2026??.nc")):
        d = f"cds/x/{os.path.basename(f)[:-3]}"
        if zipfile.is_zipfile(f):
            zipfile.ZipFile(f).extractall(d); parts = sorted(glob.glob(d + "/*.nc"))
        else:
            parts = [f]
        for p in parts:
            ds = xr.open_dataset(p)
            if "tp" in ds: tp.append(ds["tp"])
            if "mx2t" in ds: mx.append(ds["mx2t"])
    return xr.concat(tp, "valid_time").sortby("valid_time"), xr.concat(mx, "valid_time").sortby("valid_time")
for iso, box in FOOTPRINTS.items():
    if not glob.glob(f"cds/{box}_era5_2026??.nc"): continue
    tp, mx = era5(box); rows, cols, w = grid_weights(iso, tp.latitude.values, tp.longitude.values)
    # value at valid time t covers (t-1h, t]; UTC day D = hours 01..24
    day = (pd.to_datetime(tp.valid_time.values) - pd.Timedelta(hours=1)).normalize()
    out = {}
    for d in sorted(set(day)):
        sel = np.where(day == d)[0]
        if len(sel) != 24: continue  # incomplete UTC day at the ERA5T edge: never a partial sum
        a = tp.values[sel] * 1000; x = mx.values[sel] - 273.15
        if np.isnan(a).any() or np.isnan(x).any(): continue
        dt = a.sum(0); out[d] = {"tp": wmean(dt, rows, cols, w)[0], "tx": wmean(x.max(0), rows, cols, w)[0], "wet": float(w[dt[rows, cols] >= DRY].sum())}
    if out:
        df = pd.DataFrame(out).T.sort_index(); S[iso]["era5t_tp"], S[iso]["era5t_tx"], S[iso]["era5t_wet"] = df.tp, df.tx, df.wet
    f = f"cds/{box}_land_2026.nc"
    if os.path.exists(f):
        ds = xr.open_dataset(f); rows, cols, w = grid_weights(iso, ds.latitude.values, ds.longitude.values); o = {}; lost = []
        for i, t in enumerate(pd.to_datetime(ds.valid_time.values)):
            v, l = wmean(ds.swvl1.values[i], rows, cols, w); o[t.normalize()] = v; lost.append(l)
        S[iso]["land_swvl1"] = pd.Series(o).dropna(); LOST[f"{iso} era5-land weight on NaN nodes"] = max(lost)

# --- CHIRPS v3 ---
ch = pickle.load(open("chirps/chirps.pkl", "rb"))
for iso in FOOTPRINTS:
    for kind in ("sat", "rnl", "prelim"):
        o = {pd.Timestamp(d): r[iso]["mm"] for (k, d), r in ch.items() if k == kind and r["ok"]}
        if o: S[iso][f"chirps_{kind}_tp"] = pd.Series(o).sort_index()
        wet = {pd.Timestamp(d): r[iso]["wet_share"] for (k, d), r in ch.items() if k == kind and r["ok"]}
        if wet: S[iso][f"chirps_{kind}_wet"] = pd.Series(wet).sort_index()
    LOST[f"{iso} chirps max weight on nodata"] = max(r[iso]["lost_w"] for r in ch.values() if r["ok"])
    S[iso]["chirps_tp"] = pd.concat([S[iso]["chirps_sat_tp"], S[iso].get("chirps_prelim_tp", pd.Series(dtype=float))]).sort_index()  # what a CI job would hold

# --- CPC Unified (PSL netCDF, 0.5 deg) ---
for var, name in (("precip", "cpc_tp"), ("tmax", "cpc_tx")):
    ds = xr.open_dataset(f"cpc/{var}.2026.nc").sel(time=slice("2026-05-25", None))
    for iso in FOOTPRINTS:
        rows, cols, w = grid_weights(iso, ds.lat.values, ds.lon.values); o = {}; lost = []
        for i, t in enumerate(pd.to_datetime(ds.time.values)):
            v, l = wmean(ds[var].values[i], rows, cols, w); o[t] = v; lost.append(l)
        S[iso][name] = pd.Series(o).dropna(); LOST[f"{iso} {name} max weight on NaN nodes"] = max(lost)
        S[iso][name + "_nodes"] = len(w)

# --- scoring ---
def dry_run(s):
    n = 0
    for v in reversed(s.tolist()):
        if pd.notna(v) and v < DRY: n += 1
        else: break
    return n
def deficit(s):
    """analysis.weather_alerts._precip_deficit on a footprint series."""
    s = s.dropna(); last = s.index.max(); cut = last - pd.Timedelta(days=WIN); bcut = cut - pd.Timedelta(days=BASE)
    rec = s[s.index > cut]; base = s[(s.index > bcut) & (s.index <= cut)]
    if len(base) < MINOBS: return {"total_30d": float(rec.sum()), "pct": None, "baseline_obs": int(len(base))}
    norm = base.mean() * WIN
    return {"total_30d": round(float(rec.sum()), 1), "norm_mm": round(float(norm), 1), "pct": round(float((rec.sum() - norm) / norm * 100), 1) if norm > 0 else None, "baseline_obs": int(len(base))}
def rain_score(ref, x):
    j = pd.concat([ref, x], axis=1, keys=["r", "x"]).dropna()
    if len(j) < 10: return {"n": len(j)}
    p5 = j.rolling(5).sum().dropna().iloc[::5]; r30 = j.rolling(30).sum().dropna()
    return {"n": len(j), "r_daily": round(float(j.r.corr(j.x)), 3), "r_5d": round(float(p5.r.corr(p5.x)), 3), "mae_daily_mm": round(float((j.x - j.r).abs().mean()), 2),
            "total_ref_mm": round(float(j.r.sum()), 1), "total_x_mm": round(float(j.x.sum()), 1), "total_ratio": round(float(j.x.sum() / j.r.sum()), 3) if j.r.sum() else None,
            "dry_day_agree_pct": round(100 * float(((j.r < DRY) == (j.x < DRY)).mean()), 1), "dry_days_ref": int((j.r < DRY).sum()), "dry_days_x": int((j.x < DRY).sum()),
            "roll30_mean_abs_diff_mm": round(float((r30.x - r30.r).abs().mean()), 1), "roll30_max_abs_diff_mm": round(float((r30.x - r30.r).abs().max()), 1),
            "roll30_mean_ref_mm": round(float(r30.r.mean()), 1)}
def t_score(ref, x, bars=(34, 38)):
    j = pd.concat([ref, x], axis=1, keys=["r", "x"]).dropna()
    if len(j) < 10: return {"n": len(j)}
    o = {"n": len(j), "r": round(float(j.r.corr(j.x)), 3), "bias_c": round(float((j.x - j.r).mean()), 2), "mae_c": round(float((j.x - j.r).abs().mean()), 2), "max_abs_c": round(float((j.x - j.r).abs().max()), 2)}
    for b in bars: o[f"days_gt{b}_ref"] = int((j.r > b).sum()); o[f"days_gt{b}_x"] = int((j.x > b).sum()); o[f"gt{b}_agree_pct"] = round(100 * float(((j.r > b) == (j.x > b)).mean()), 1)
    return o
OUT = {"lost_weight": LOST, "coverage": {}, "rain_vs_ec1": {}, "rain_vs_era5t": {}, "tmax_vs_ec1": {}, "soil": {}, "alerts_at_common_end": {}, "shift_test": {}}
for iso in FOOTPRINTS:
    s = S[iso]
    OUT["coverage"][iso] = {k: {"n": int(v.notna().sum()), "first": str(v.dropna().index.min().date()), "last": str(v.dropna().index.max().date())} for k, v in s.items() if isinstance(v, pd.Series)}
    OUT["coverage"][iso]["cpc_grid_nodes"] = s.get("cpc_tp_nodes")
    rains = [k for k in ("era5t_tp", "chirps_tp", "chirps_sat_tp", "chirps_rnl_tp", "chirps_prelim_tp", "cpc_tp") if k in s]
    OUT["rain_vs_ec1"][iso] = {k: rain_score(s["ec1_tp"], s[k]) for k in rains}
    if "era5t_tp" in s: OUT["rain_vs_era5t"][iso] = {k: rain_score(s["era5t_tp"], s[k]) for k in rains if k != "era5t_tp"}
    OUT["tmax_vs_ec1"][iso] = {k: t_score(s["ec1_tx"], s[k]) for k in ("era5t_tx", "cpc_tx") if k in s}
    # is the CPC day (ending ~12Z) better matched by the previous UTC day's label?
    OUT["shift_test"][iso] = {f"cpc_tp_shift{sh}": rain_score(s["ec1_tp"], s["cpc_tp"].shift(sh, freq="D")).get("r_daily") for sh in (-1, 0, 1)}
    OUT["shift_test"][iso].update({f"chirps_sat_shift{sh}": rain_score(s["ec1_tp"], s["chirps_sat_tp"].shift(sh, freq="D")).get("r_daily") for sh in (-1, 0, 1)})
    if "land_swvl1" in s:
        j = pd.concat([s["ec1_vsw1"], s["land_swvl1"]], axis=1, keys=["ifs", "land"]).dropna()
        OUT["soil"][iso] = {"n": len(j), "r_level": round(float(j.ifs.corr(j.land)), 3), "bias_ifs_minus_land": round(float((j.ifs - j.land).mean()), 4),
                            "r_daily_change": round(float(j.ifs.diff().corr(j.land.diff())), 3), "ifs_range": [round(float(j.ifs.min()), 3), round(float(j.ifs.max()), 3)],
                            "land_range": [round(float(j.land.min()), 3), round(float(j.land.max()), 3)],
                            "bias_first30": round(float((j.ifs - j.land).iloc[:30].mean()), 4), "bias_last30": round(float((j.ifs - j.land).iloc[-30:].mean()), 4)}
    # run the production rules on every source at the last date all rain sources share
    end = min(s[k].dropna().index.max() for k in ["ec1_tp"] + [r for r in rains if r not in ("chirps_sat_tp", "chirps_rnl_tp", "chirps_prelim_tp")])
    A = {"common_end": str(end.date())}
    for k in ["ec1_tp"] + [r for r in rains if r in ("era5t_tp", "chirps_tp", "cpc_tp")]:
        x = s[k][:end]; d = deficit(x)
        A[k] = {"days_held": int(x.notna().sum()), "dry_run_days": dry_run(x), "dry_spell_fires": dry_run(x) >= SPELL, **d, "deficit_fires": d["pct"] is not None and d["pct"] <= -DEF}
    OUT["alerts_at_common_end"][iso] = A
    pd.DataFrame({k: v for k, v in s.items() if isinstance(v, pd.Series)}).round(4).to_csv(f"series_{FOOTPRINTS[iso]}.csv", index_label="date")
json.dump(OUT, open("agreement.json", "w"), indent=1, default=str); print(json.dumps(OUT, indent=1, default=str))
