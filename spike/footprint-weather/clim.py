"""ERA5 / ERA5-Land 1991-2020 for the spike window (Oct 5-19 UTC days), two footprint boxes.
Splits a request into decades, then single years, when CDS says 'cost limits exceeded'."""
import os, time, concurrent.futures as cf
from dotenv import load_dotenv
load_dotenv("/Users/philipbergman/Documents/Coding_Projects/Mirror-Market/.env")
import cdsapi
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "clim")
BOXES = {"MT": [-7.0, -62.0, -18.5, -50.0], "IA": [44.0, -97.0, 40.0, -90.0]}  # N W S E
YEARS = list(range(1991, 2021))
DAYS = [f"{d:02d}" for d in range(5, 20)]
HOURS = [f"{h:02d}:00" for h in range(24)]
JOBS = []
for box, area in BOXES.items():
    # ERA5 tp at hour h = accumulation over (h-1, h]; a UTC day d needs 01..24 -> also 00 of d+1. Fetch day 20 too.
    JOBS.append((f"{box}_era5", "reanalysis-era5-single-levels", ["total_precipitation", "2m_temperature"], DAYS + ["20"], HOURS, area))
    JOBS.append((f"{box}_land", "reanalysis-era5-land", ["volumetric_soil_water_layer_1"], DAYS, ["00:00", "12:00"], area))

def retrieve(name, ds, var, days, hours, area, years):
    target = os.path.join(OUT, f"{name}_{years[0]}_{years[-1]}.nc")
    if os.path.exists(target):
        return [(target, "cached", 0)]
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.environ["CDSAPI_KEY"], quiet=True, progress=False)
    req = {"variable": var, "year": [str(y) for y in years], "month": ["10"], "day": days, "time": hours,
           "area": area, "data_format": "netcdf", "download_format": "unarchived"}
    if ds == "reanalysis-era5-single-levels":
        req["product_type"] = ["reanalysis"]
    t0 = time.time()
    try:
        c.retrieve(ds, req, target)
    except Exception as e:
        if "cost limits" in str(e) and len(years) > 1:
            step = 10 if len(years) > 10 else 1
            out = []
            for i in range(0, len(years), step):
                out += retrieve(name, ds, var, days, hours, area, years[i:i + step])
            return out
        raise
    return [(target, "ok", round(time.time() - t0))]

with cf.ThreadPoolExecutor(4) as ex:
    futs = {ex.submit(retrieve, j[0], j[1], j[2], j[3], j[4], j[5], YEARS): j[0] for j in JOBS}
    for f in cf.as_completed(futs):
        try:
            for r in f.result():
                print(futs[f], *r, flush=True)
        except Exception as e:
            print(futs[f], "FAILED", repr(e)[:400], flush=True)
