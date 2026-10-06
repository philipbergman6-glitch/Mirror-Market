"""R4: live CDS pulls — ERA5(T) hourly tp + mx2t per footprint box, ERA5-Land(T) swvl1, and the daily-statistics product.
Times queue+download per request. Key comes from config.py's env loading; never printed."""
import json, os, sys, time, concurrent.futures as cf
sys.path.insert(0, "/Users/philipbergman/Documents/Coding_Projects/Mirror-Market")
import config  # noqa: F401,E402  loads the env file
import cdsapi  # noqa: E402
from common import BOXES  # noqa: E402
OUT = "cds"; LOG = "cds/timing.jsonl"
H = [f"{h:02d}:00" for h in range(24)]; D31 = [f"{d:02d}" for d in range(1, 32)]
JOBS = []
for box, area in BOXES.items():
    for m, days in (("06", D31), ("07", D31), ("08", D31), ("09", D31), ("10", D31[:6])):
        JOBS.append((f"{box}_era5_2026{m}", "reanalysis-era5-single-levels",
                     {"product_type": ["reanalysis"], "variable": ["total_precipitation", "maximum_2m_temperature_since_previous_post_processing"],
                      "year": ["2026"], "month": [m], "day": days, "time": H, "area": area}))
    JOBS.append((f"{box}_land_2026", "reanalysis-era5-land",
                 {"variable": ["volumetric_soil_water_layer_1"], "year": ["2026"], "month": ["06", "07", "08", "09", "10"], "day": D31, "time": ["00:00"], "area": area}))
    JOBS.append((f"{box}_dstat_202609", "derived-era5-single-levels-daily-statistics",
                 {"product_type": "reanalysis", "variable": ["total_precipitation"], "year": "2026", "month": ["09"], "day": D31[:30],
                  "daily_statistic": "daily_sum", "time_zone": "utc+00:00", "frequency": "1_hourly", "area": area}))
# what a daily CI increment would look like: one request, last 10 calendar days, one box
JOBS.append(("IA_era5_incr10d", "reanalysis-era5-single-levels",
             {"product_type": ["reanalysis"], "variable": ["total_precipitation", "maximum_2m_temperature_since_previous_post_processing"],
              "year": ["2026"], "month": ["09", "10"], "day": ["27", "28", "29", "30", "01", "02", "03", "04", "05", "06"], "time": H, "area": BOXES["IA"]}))

def run(job):
    name, ds, req = job
    target = f"{OUT}/{name}.nc"
    if os.path.exists(target):
        return
    req = dict(req, data_format="netcdf", download_format="unarchived")
    c = cdsapi.Client(url="https://cds.climate.copernicus.eu/api", key=os.environ["CDSAPI_KEY"], quiet=True, progress=False, retry_max=5)
    t0 = time.time(); rec = {"name": name, "dataset": ds, "submitted_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    try:
        c.retrieve(ds, req, target)
        rec.update(status="ok", seconds=round(time.time() - t0), bytes=os.path.getsize(target))
    except Exception as e:
        rec.update(status="failed", seconds=round(time.time() - t0), error=repr(e)[:500])
    with open(LOG, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(rec, flush=True)

only = sys.argv[1:]
jobs = [j for j in JOBS if not only or any(o in j[0] for o in only)]
jobs.sort(key=lambda j: ("incr" not in j[0], j[0][-2:] != "10", j[0]))
with cf.ThreadPoolExecutor(1) as ex:  # CDS rejects >1 queued request per user per dataset (measured 2026-10-06)
    list(ex.map(run, jobs))
