"""ERA5 1991–2020 daily normal for footprint weather (spec §6.1, slice 0).

A one-off reference build on a desk machine. Never a CI step; ``CDSAPI_KEY``
is never a CI secret.

Two stages, because the download is slow and the weights may not exist yet:

``download``
    Requests hourly ``total_precipitation`` and ``mx2t`` for the seven region
    boxes from CDS, serially (CDS rejects parallel requests per user per
    dataset). Each chunk is reduced at once to per-node *partial* UTC days —
    rain sum, Tmax max and the hour count — saved as ``.npz``, and the raw
    NetCDF is deleted. Resumable: a chunk whose ``.npz`` exists is skipped.
    A chunk refused for "cost limits" is split decade → year → month.

``reduce``
    Merges the partial days across chunks, hard-fails any day short of 24
    hours, and collapses nodes to footprints with ``weights.csv``, writing
    ``era5_daily_1991_2020.csv.gz``.

Hour convention (spec §6.1): ERA5 hourly ``tp`` and ``mx2t`` at valid time h
cover (h−1, h]. UTC day D is the hours ending 01 … 24, i.e. valid times
D 01:00 … D+1 00:00, so each value is labelled with the day of (h − 1 h).

Paths come from the environment and are never committed:
``ERA5_CACHE_DIR`` (default ``~/mirror-market-reference/era5``).
"""

from __future__ import annotations

import argparse
import logging
import os
import shutil
import sys
import tempfile
import time
import zipfile
from datetime import date
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

log = logging.getLogger("era5_normals")

DATASET = "reanalysis-era5-single-levels"
VARIABLES = ["total_precipitation", "maximum_2m_temperature_since_previous_post_processing"]
CDS_URL = "https://cds.climate.copernicus.eu/api"

FIRST_DAY = date(1991, 1, 1)
LAST_DAY = date(2021, 1, 15)  # the extra 15 days let a late-December window close
N_DAYS = (LAST_DAY - FIRST_DAY).days + 1  # 10,973 (spec §6.1 says 10,972: off by one)
HOURS_PER_DAY = 24

# North, West, South, East. About 1° beyond the admin-1 extents of every
# footprint in the group, on the 0.25° grid. A footprint node outside its box
# is a hard-fail in ``reduce``; widen the box and re-download that box only.
BOXES: dict[str, tuple[float, float, float, float]] = {
    "us": (44.5, -105.0, 36.0, -86.5),           # IA, IL, NE
    "south_america": (-6.5, -67.0, -42.0, -47.0),  # MT, PR, RS, Pampas, Córdoba, BA, E. Paraguay
    "europe": (55.5, 2.5, 43.0, 30.5),           # Grand Est, Mecklenburg-Vorpommern, Bărăgan
    "india": (27.5, 72.0, 15.0, 83.5),           # Madhya Pradesh, Maharashtra
    "china": (54.0, 121.0, 43.0, 135.5),         # Heilongjiang
    "southern_africa": (-24.0, 24.0, -31.0, 32.5),  # Free State, Mpumalanga
    "west_africa": (12.0, 5.5, 6.0, 10.5),       # Benue, Kaduna
}

DECADES = [(1991, 2000), (2001, 2010), (2011, 2020)]


def cache_dir() -> Path:
    return Path(os.environ.get("ERA5_CACHE_DIR", "~/mirror-market-reference/era5")).expanduser()


# --- chunks -----------------------------------------------------------------

def _chunk_name(box: str, years: tuple[int, int], month: int | None) -> str:
    span = f"{years[0]}" if years[0] == years[1] else f"{years[0]}-{years[1]}"
    return f"{box}_{span}" + (f"_{month:02d}" if month else "")


def _request(box: str, years: tuple[int, int], month: int | None) -> dict:
    north, west, south, east = BOXES[box]
    if years == (2021, 2021):
        # Only 2021-01-01 … 01-16; 01-16 00:00 closes 01-15.
        months, days = ["01"], [f"{d:02d}" for d in range(1, 17)]
    else:
        months = [f"{month:02d}"] if month else [f"{m:02d}" for m in range(1, 13)]
        days = [f"{d:02d}" for d in range(1, 32)]
    return {
        "product_type": ["reanalysis"],
        "variable": VARIABLES,
        "year": [str(y) for y in range(years[0], years[1] + 1)],
        "month": months,
        "day": days,
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": [north, west, south, east],
        "data_format": "netcdf",
        "download_format": "unarchived",
    }


def _open_download(path: Path, workdir: Path):
    """CDS serves a zip named ``.nc`` holding one NetCDF per step type. Sniff, never trust the name."""
    import xarray as xr

    with open(path, "rb") as fh:
        magic = fh.read(4)
    if magic[:2] == b"PK":
        unzipped = workdir / "unzipped"  # never beside the zip: it is itself named .nc
        with zipfile.ZipFile(path) as zf:
            zf.extractall(unzipped)
        files = sorted(unzipped.glob("*.nc"))
    elif magic[:3] == b"CDF" or magic == b"\x89HDF":
        files = [path]
    else:
        raise RuntimeError(f"{path}: neither zip nor NetCDF (magic {magic!r})")
    if not files:
        raise RuntimeError(f"{path}: zip held no .nc files")
    parts = [xr.open_dataset(f).drop_vars(["expver", "number"], errors="ignore") for f in files]
    return xr.merge(parts, compat="override", join="exact")


def _reduce_chunk(ds, out: Path) -> None:
    """Hourly → per-node partial UTC days (rain sum mm, Tmax max °C, hour count)."""
    for var in ("tp", "mx2t"):
        if var not in ds:
            raise RuntimeError(f"variable {var!r} missing; got {sorted(ds.data_vars)}")
    tname = "valid_time" if "valid_time" in ds.dims else "time"
    times = ds[tname].values.astype("datetime64[h]")
    if len(np.unique(times)) != len(times):
        raise RuntimeError("duplicate valid times in chunk")
    day_of = (times - np.timedelta64(1, "h")).astype("datetime64[D]")
    days = np.unique(day_of)
    lat = ds["latitude"].values
    lon = ds["longitude"].values
    rain = np.zeros((len(days), len(lat), len(lon)), dtype=np.float64)
    tmax = np.full((len(days), len(lat), len(lon)), -np.inf, dtype=np.float64)
    hours = np.zeros(len(days), dtype=np.int16)
    # One day at a time keeps a decade chunk out of memory.
    for i, d in enumerate(days):
        idx = np.nonzero(day_of == d)[0]
        tp = ds["tp"].isel({tname: idx}).values
        mx = ds["mx2t"].isel({tname: idx}).values
        if np.isnan(tp).any() or np.isnan(mx).any():
            raise RuntimeError(f"NaN in chunk on {d}")
        rain[i] = tp.sum(axis=0) * 1000.0
        tmax[i] = mx.max(axis=0) - 273.15
        hours[i] = len(idx)
    np.savez_compressed(
        out, days=days, lat=lat, lon=lon,
        rain_mm=rain.astype(np.float32), tmax_c=tmax.astype(np.float32), hours=hours,
    )


def _fetch(client, box: str, years: tuple[int, int], month: int | None, root: Path) -> list[str]:
    name = _chunk_name(box, years, month)
    out = root / "chunks" / f"{name}.npz"
    if out.exists():
        log.info("%s cached", name)
        return [name]
    # The raw download survives a failed reduce, so a fix never costs a re-download.
    target = root / "raw" / f"{name}.nc"
    t0 = time.time()
    if target.exists():
        log.info("%s raw cached, reducing", name)
    else:
        partial = target.with_suffix(".part")
        try:
            client.retrieve(DATASET, _request(box, years, month), str(partial))
        except Exception as exc:  # cdsapi raises bare HTTP errors
            if "cost limit" not in str(exc).lower():
                raise
            if years[0] != years[1]:
                log.info("%s refused (cost limits) → by year", name)
                return [n for y in range(years[0], years[1] + 1)
                        for n in _fetch(client, box, (y, y), None, root)]
            if month is None:
                log.info("%s refused (cost limits) → by month", name)
                return [n for m in range(1, 13) for n in _fetch(client, box, years, m, root)]
            raise
        partial.rename(target)
    mb = target.stat().st_size / 1e6
    with tempfile.TemporaryDirectory(dir=root) as tmp:
        ds = _open_download(target, Path(tmp))
        try:
            _reduce_chunk(ds, Path(tmp) / "out.npz")
        finally:
            ds.close()
        shutil.move(Path(tmp) / "out.npz", out)
    target.unlink()
    log.info("%s ok  %.0f MB  %.0f s", name, mb, time.time() - t0)
    return [name]


def download(boxes: list[str]) -> None:
    from dotenv import load_dotenv
    import cdsapi

    load_dotenv(PROJECT_ROOT / ".env")
    key = os.environ.get("CDSAPI_KEY")
    if not key:
        raise SystemExit("CDSAPI_KEY is not set (.env or environment)")
    root = cache_dir()
    (root / "chunks").mkdir(parents=True, exist_ok=True)
    (root / "raw").mkdir(exist_ok=True)
    client = cdsapi.Client(url=CDS_URL, key=key, quiet=True, progress=False)
    spans = DECADES + [(2021, 2021)]
    for box in boxes:
        for span in spans:
            _fetch(client, box, span, None, root)
    log.info("download complete for %s", ", ".join(boxes))


# --- reduce -----------------------------------------------------------------

def _merge_box(box: str, root: Path):
    """Sum partial days across a box's chunks; hard-fail a day short of 24 hours."""
    files = sorted((root / "chunks").glob(f"{box}_*.npz"))
    if not files:
        raise SystemExit(f"{box}: no chunks in {root / 'chunks'}")
    all_days = np.arange(np.datetime64(FIRST_DAY), np.datetime64(LAST_DAY) + 1)
    lat = lon = rain = tmax = None
    hours = np.zeros(len(all_days), dtype=np.int16)
    for f in files:
        z = np.load(f)
        if lat is None:
            lat, lon = z["lat"], z["lon"]
            rain = np.zeros((len(all_days), len(lat), len(lon)), dtype=np.float64)
            tmax = np.full((len(all_days), len(lat), len(lon)), -np.inf, dtype=np.float64)
        elif not (np.array_equal(lat, z["lat"]) and np.array_equal(lon, z["lon"])):
            raise SystemExit(f"{f.name}: grid differs from the box's other chunks")
        pos = (z["days"] - all_days[0]).astype(int)
        keep = (pos >= 0) & (pos < len(all_days))  # 1990-12-31 and 2021-01-16 are partial by design
        p = pos[keep]
        rain[p] += z["rain_mm"][keep]
        tmax[p] = np.maximum(tmax[p], z["tmax_c"][keep])
        hours[p] += z["hours"][keep]
    short = np.nonzero(hours != HOURS_PER_DAY)[0]
    if len(short):
        sample = ", ".join(f"{all_days[i]}={hours[i]}h" for i in short[:5])
        raise SystemExit(f"{box}: {len(short)} days without exactly 24 hours ({sample})")
    return all_days, lat, lon, rain, tmax


def reduce(weights_path: Path, out_path: Path) -> None:
    import pandas as pd

    weights = pd.read_csv(weights_path)
    weights = weights[weights["kind"] != "port"]  # ports need no normal
    root = cache_dir()
    frames = []
    unplaced = set(weights["footprint"])
    for box in BOXES:
        days, lat, lon, rain, tmax = _merge_box(box, root)
        north, west, south, east = BOXES[box]
        inside = weights[weights["lat"].between(south, north) & weights["lon"].between(west, east)]
        for fp, w in inside.groupby("footprint"):
            if fp not in unplaced:
                raise SystemExit(f"{fp}: nodes fall in more than one box")
            if len(w) != len(weights[weights["footprint"] == fp]):
                raise SystemExit(f"{fp}: some nodes lie outside box {box!r}; widen it")
            rows = np.searchsorted(-lat, -w["lat"].to_numpy())
            cols = np.searchsorted(lon, w["lon"].to_numpy())
            if not (np.allclose(lat[rows], w["lat"]) and np.allclose(lon[cols], w["lon"])):
                raise SystemExit(f"{fp}: weights nodes are not on the ERA5 grid")
            wt = w["weight"].to_numpy()
            frames.append(pd.DataFrame({
                "footprint": fp,
                "date": days.astype(str),
                "rain_mm": (rain[:, rows, cols] * wt).sum(axis=1).round(2),
                "tmax_c": (tmax[:, rows, cols] * wt).sum(axis=1).round(2),
            }))
            unplaced.discard(fp)
    if unplaced:
        raise SystemExit(f"footprints outside every box: {sorted(unplaced)}")
    result = pd.concat(frames, ignore_index=True)
    counts = result.groupby("footprint").size()
    if (counts != N_DAYS).any():
        raise SystemExit(f"footprints without {N_DAYS} days: {counts[counts != N_DAYS].to_dict()}")
    result.to_csv(out_path, index=False, compression="gzip")
    log.info("wrote %s: %d footprints × %d days", out_path, len(counts), N_DAYS)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="cmd", required=True)
    d = sub.add_parser("download", help="fetch and reduce hourly ERA5 chunks (resumable)")
    d.add_argument("--box", choices=sorted(BOXES), action="append",
                   help="limit to these boxes (default: all, in registry order)")
    r = sub.add_parser("reduce", help="collapse cached chunks to footprints")
    r.add_argument("--weights", type=Path,
                   default=PROJECT_ROOT / "data/reference/footprints/weights.csv")
    r.add_argument("--out", type=Path,
                   default=PROJECT_ROOT / "data/reference/footprints/era5_daily_1991_2020.csv.gz")
    args = parser.parse_args()
    if args.cmd == "download":
        download(args.box or list(BOXES))
    else:
        reduce(args.weights, args.out)


if __name__ == "__main__":
    main()
