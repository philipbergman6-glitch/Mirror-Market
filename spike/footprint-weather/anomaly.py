"""15-day anomaly of the 2026-10-05 00z footprint forecast vs ERA5 / ERA5-Land 1991-2020 (same UTC days, Oct 5-19)."""
import glob, json, pickle
import numpy as np, pandas as pd, xarray as xr
from footprints import admin1, spam_cells, collapse, ecmwf_index, PINS
fp = pickle.load(open("footprints.pkl", "rb")); fc = json.load(open("fc_20261005.json"))
D = pickle.load(open("ecmwf_20261005.pkl", "rb")); U = D["union"]; pos = {int(i): k for k, i in enumerate(U)}
lsm = D["fields"][("lsm", None, 0)]["v"]
BOX = {"BR-MT": "MT", "US-IA": "IA"}

def open_all(pattern):
    fs = sorted(glob.glob(pattern))
    if not fs: return None
    return xr.open_mfdataset(fs, combine="by_coords") if len(fs) > 1 else xr.open_dataset(fs[0])

def wavg(field2d, lat, lon, idx, w, land_only=True):
    """field2d on the 0.25 ERA5 grid (lat x lon) -> footprint weighted mean via ECMWF indices."""
    LA, LO = np.meshgrid(lat, lon, indexing="ij"); gi = ecmwf_index(LA.ravel(), LO.ravel())
    lut = dict(zip(gi.tolist(), field2d.ravel().tolist()))
    missing = [int(i) for i in idx if int(i) not in lut]
    if missing: raise SystemExit(f"{len(missing)} footprint points outside the ERA5 box")  # never pad
    k = np.array([pos[int(i)] for i in idx]); m = lsm[k] > 0.5 if land_only else np.ones(k.size, bool)
    v = np.array([lut[int(i)] for i in idx]); return float((v[m] * w[m]).sum() / w[m].sum())

out = {}
for iso, box in BOX.items():
    out[iso] = {}
    ds = open_all(f"clim/{box}_era5_*.nc")
    if ds is not None:
        tname = "valid_time" if "valid_time" in ds.dims else "time"
        tp = ds["tp"] * 1000; t2 = ds["t2m"] - 273.15
        # hourly tp at valid time t = accumulation over (t-1h, t]; UTC day d = (d 00:00, d+1 00:00]
        day = (pd.to_datetime(ds[tname].values) - pd.Timedelta(hours=1)).normalize()
        tp = tp.assign_coords(day=(tname, day)); t2 = t2.assign_coords(day2=(tname, pd.to_datetime(ds[tname].values).normalize()))
        dtp = tp.groupby("day").sum().compute(); dtx = t2.groupby("day2").max().compute()
        dtp = dtp.sel(day=[d for d in dtp.day.values if 5 <= pd.Timestamp(d).day <= 19 and pd.Timestamp(d).month == 10])
        dtx = dtx.sel(day2=[d for d in dtx.day2.values if 5 <= pd.Timestamp(d).day <= 19])
        lat, lon = ds.latitude.values, ds.longitude.values
        for meth, (idx, w) in fp[iso].items():
            years = sorted({pd.Timestamp(d).year for d in dtp.day.values})
            tot = []; txm = []
            for y in years:
                sel = [d for d in dtp.day.values if pd.Timestamp(d).year == y]
                if len(sel) != 15: raise SystemExit(f"{iso} {y}: {len(sel)} days of tp")
                tot.append(wavg(dtp.sel(day=sel).sum("day").values, lat, lon, idx, w))
                selx = [d for d in dtx.day2.values if pd.Timestamp(d).year == y]
                txm.append(wavg(dtx.sel(day2=selx).mean("day2").values, lat, lon, idx, w))
            tot = np.array(tot); txm = np.array(txm)
            f_tp = fc[iso][meth]["summary"]["tp15_mm"]; f_tx = fc[iso][meth]["summary"]["tmax_mean_c"]
            out[iso][meth] = {"n_years": len(years), "tp15_normal_mm": float(tot.mean()), "tp15_normal_median_mm": float(np.median(tot)),
                "tp15_terciles_mm": [float(np.percentile(tot, 33.3)), float(np.percentile(tot, 66.7))],
                "tp15_min_max_mm": [float(tot.min()), float(tot.max())], "fc_tp15_mm": f_tp,
                "tp15_anom_pct": 100 * (f_tp / tot.mean() - 1), "fc_rank_of_31": int((tot < f_tp).sum()) + 1,
                "tmax_normal_c": float(txm.mean()), "fc_tmax_mean_c": f_tx, "tmax_anom_c": f_tx - float(txm.mean()),
                "tmax_normal_sd_c": float(txm.std())}
    land = open_all(f"clim/{box}_land_*.nc")
    if land is not None:
        tname = "valid_time" if "valid_time" in land.dims else "time"
        sw = land["swvl1"]; t = pd.to_datetime(land[tname].values)
        oct5 = sw.isel({tname: [i for i, x in enumerate(t) if x.day == 5]})
        lat, lon = land.latitude.values, land.longitude.values
        geom = admin1(iso); clat, clon, prod = spam_cells(geom)
        # nearest ERA5-Land 0.1 point per SPAM cell
        r = np.abs(lat[:, None] - clat[None, :]).argmin(0); c = np.abs(lon[:, None] - clon[None, :]).argmin(0)
        vals = oct5.values  # (n_times, lat, lon)
        per = vals[:, r, c]; ok = ~np.isnan(per).any(0)
        for meth, wt in (("admin1_area", np.cos(np.radians(clat))), ("admin1_soy", prod)):
            yr = (per[:, ok] * wt[ok]).sum(1) / wt[ok].sum()
            yrs = pd.DatetimeIndex(oct5[tname].values).year
            ann = pd.Series(yr, index=yrs).groupby(level=0).mean()
            f0 = fc[iso][meth]["summary"]["vsw1_day0"]
            out[iso].setdefault(meth, {}).update({"swvl1_oct5_normal": float(ann.mean()), "swvl1_min_max": [float(ann.min()), float(ann.max())],
                "fc_vsw1_day0_ifs": f0, "vsw1_anom": f0 - float(ann.mean()), "ifs_day0_rank_of_31": int((ann < f0).sum()) + 1})
json.dump(out, open("anomaly.json", "w"), indent=1)
for iso, ms in out.items():
    for m, v in ms.items(): print(iso, m, {k: (round(x, 3) if isinstance(x, float) else [round(y, 2) for y in x] if isinstance(x, list) else x) for k, x in v.items()})
