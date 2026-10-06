"""R4 #369 shared helpers: SPAM-2020-soy-weighted admin-1 footprints projected onto any regular lat/lon grid.

Run from the scratch workdir (holds ne/ and spam/ symlinks). Throwaway research code."""
import sys
import numpy as np
sys.path.insert(0, "/Users/philipbergman/Documents/Coding_Projects/Mirror-Market-r4/spike/footprint-weather")
from footprints import admin1, spam_cells  # noqa: E402  (K2 spike: Natural Earth admin-1 + SPAM P_SOYB_A)

FOOTPRINTS = {"BR-MT": "MT", "US-IA": "IA"}
BOXES = {"MT": [-7.0, -62.0, -18.5, -50.0], "IA": [44.0, -97.0, 40.0, -90.0]}  # N W S E (K2 boxes)
_cells = {}

def cells(iso):
    if iso not in _cells:
        _cells[iso] = spam_cells(admin1(iso))
    return _cells[iso]

def grid_weights(iso, lats, lons):
    """SPAM soy production of each 5' cell -> nearest (lat, lon) grid node. Returns (row, col, weight) with sum(weight)=1.
    Hard-fails if any SPAM cell is further than one grid step from the grid (footprint outside the data)."""
    clat, clon, prod = cells(iso)
    lats = np.asarray(lats, float); lons = np.asarray(lons, float)
    lons = np.where(lons > 180, lons - 360, lons)
    r = np.abs(lats[:, None] - clat[None, :]).argmin(0); c = np.abs(lons[:, None] - clon[None, :]).argmin(0)
    step = max(abs(np.diff(lats)).max(), abs(np.diff(lons)).max())
    if (np.abs(lats[r] - clat) > step).any() or (np.abs(lons[c] - clon) > step).any():
        raise SystemExit(f"{iso}: SPAM cells outside the data grid")
    acc = {}
    for i, j, p in zip(r, c, prod):
        acc[(int(i), int(j))] = acc.get((int(i), int(j)), 0.0) + float(p)
    keys = sorted(acc); w = np.array([acc[k] for k in keys])
    return np.array([k[0] for k in keys]), np.array([k[1] for k in keys]), w / w.sum()

def wmean(field2d, rows, cols, w):
    """Weighted mean; NaN nodes are dropped and the lost weight share is returned (never silently padded)."""
    v = np.asarray(field2d, float)[rows, cols]; ok = np.isfinite(v)
    if not ok.any():
        return None, 1.0
    return float((v[ok] * w[ok]).sum() / w[ok].sum()), float(w[~ok].sum())
