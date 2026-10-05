"""K1 place list x today's active storms (NHC + JTWC, per R2). Throwaway.

Flag rule under test: a place is flagged when it falls inside a storm's forecast 34/50/64-kt quadrant radius at any
time 0-120 h (track and radii linearly interpolated hourly between forecast taus). A 'watch' is the storm centre
passing within WATCH_KM of the place without the place entering the 34-kt radius."""
import glob, json, math, re, sys, time, zipfile, io
import requests, shapefile
UA = {"User-Agent": "Mozilla/5.0 (Macintosh) MirrorMarket-spike"}
WATCH_KM = 500
NM = 1.852
PORTS = {"P-NOLA": (29.9369, -90.0619), "P-PNG": (-25.5047, -48.5108), "P-UPR": (-32.9575, -60.6394),
         "P-NCN": (36.0833, 120.3170), "P-DUR": (-29.8737, 31.0232), "P-MOS": (49.1197, 6.1769),
         "P-CBOT": (41.6259, -87.1334), "P-RFT": (-26.1797, 27.7042), "P-IDR": (22.7186, 75.8550),
         "P-PKG": (3.0, 101.4), "P-PGU": (1.45, 103.89), "P-PEN": (5.4157, 100.3660)}
PINS = {"G-IA": (42.03, -93.47), "G-IL": (40.12, -89.30), "G-NE": (40.90, -98.40), "G-MT": (-12.64, -55.42),
        "G-PR": (-24.04, -51.46), "G-RS": (-28.50, -53.50), "G-PAM": (-33.95, -60.33), "G-COR": (-31.42, -64.18),
        "G-BAS": (-38.40, -60.30), "G-PY": (-25.90, -55.30), "G-FR": (48.70, 4.30), "G-DE": (53.60, 12.70),
        "G-RO": (44.60, 27.00), "G-SK": (52.10, -106.70), "G-AB": (53.00, -112.80), "G-RIAU": (0.29, 101.71),
        "G-SBH": (5.42, 116.80), "G-MP": (22.72, 75.86), "G-MH": (19.75, 75.71), "G-HL": (47.36, 127.76),
        "G-FS": (-29.12, 26.21), "G-MPU": (-25.47, 30.00), "G-BEN": (7.73, 8.52), "G-KAD": (10.52, 7.43)}
PLACES = {**PORTS, **PINS}
# K1 section 3, leg -> places (ports + growing areas)
LEGS = {
 "CBOT Soybeans": ["P-CBOT", "P-NOLA", "G-IA", "G-IL", "G-NE"],
 "CBOT Soybean Oil / Meal": ["G-IA", "G-IL", "G-NE"],
 "US Gulf CIF": ["P-NOLA", "G-IA", "G-IL", "G-NE"],
 "DCE Soybean No.2 / oil / meal": ["P-NCN", "P-PNG", "P-NOLA", "G-MT", "G-HL"],
 "DCE Soybean No.1": ["G-HL"],
 "CEPEA/ESALQ Parana": ["G-PR", "G-MT", "G-RS"],
 "ESALQ/B3 Paranagua": ["P-PNG", "G-MT", "G-PR", "G-RS"],
 "AgRural Paranagua FOB": ["P-PNG", "G-MT", "G-PR", "G-RS"],
 "MAGyP FOB soy/oil/meal": ["P-UPR", "G-PAM", "G-COR", "G-PY"],
 "MAGyP FOB Sunflower Oil": ["P-UPR", "G-BAS"],
 "India mandi MP": ["P-IDR", "G-MP"],
 "India mandi MH": ["G-MH"],
 "EU Rapeseed (Moselle)": ["P-MOS", "G-FR", "G-DE", "G-RO"],
 "SAFEX Soybean": ["P-RFT", "P-DUR", "G-FS", "G-MPU"],
 "SAFEX Sunflower": ["P-RFT"],
 "SAGIS deliveries": ["G-FS", "G-MPU"],
 "Palm Oil (CPO=F)": ["P-PKG", "P-PGU", "P-PEN", "G-RIAU", "G-SBH"],
 "CZCE Rapeseed Oil": [],
}

def south_atlantic(lat, lon):  # R2: no publishable source -> 'not covered', never 'no storm'
    return lat < 0 and -70 <= lon <= 20

def hav(a, b):
    la1, lo1, la2, lo2 = map(math.radians, (*a, *b))
    h = math.sin((la2 - la1) / 2) ** 2 + math.cos(la1) * math.cos(la2) * math.sin((lo2 - lo1) / 2) ** 2
    return 6371 * 2 * math.asin(math.sqrt(h))

def bearing(a, b):  # from storm centre a to place b, degrees
    la1, lo1, la2, lo2 = map(math.radians, (*a, *b))
    y = math.sin(lo2 - lo1) * math.cos(la2)
    x = math.cos(la1) * math.sin(la2) - math.sin(la1) * math.cos(la2) * math.cos(lo2 - lo1)
    return (math.degrees(math.atan2(y, x)) + 360) % 360

def parse_jtwc(text):
    pts = []
    for m in re.finditer(r"^T(\d{3}) (\d{3})([NS]) (\d{4})([EW]) (\d{3})(.*)$", text, re.M):
        tau, la, ns, lo, ew, vmax, rest = m.groups()
        lat = int(la) / 10 * (1 if ns == "N" else -1); lon = int(lo) / 10 * (1 if ew == "E" else -1)
        radii = {}
        for r in re.finditer(r"R(\d{3}) (\d{3}) NE QD (\d{3}) SE QD (\d{3}) SW QD (\d{3}) NW QD", rest):
            radii[int(r.group(1))] = [int(x) for x in r.groups()[1:]]
        pts.append({"tau": int(tau), "lat": lat, "lon": lon, "vmax": int(vmax), "radii": radii})
    if not pts:
        raise SystemExit("JTWC tcw: no T-lines parsed (shape change?)")
    return pts

def parse_nhc(fivezip, fcstzip):
    pts = []
    z = zipfile.ZipFile(fivezip); n = [x for x in z.namelist() if x.endswith("_pts.shp")][0][:-4]
    sf = shapefile.Reader(shp=io.BytesIO(z.read(n + ".shp")), dbf=io.BytesIO(z.read(n + ".dbf")))
    names = [f[0] for f in sf.fields[1:]]
    for s in sf.iterShapeRecords():
        r = dict(zip(names, s.record)); lon, lat = s.shape.points[0]  # geometry, not the rounded LAT/LON attrs
        pts.append({"tau": int(r["TAU"]), "lat": lat, "lon": lon, "vmax": int(r["MAXWIND"]), "radii": {}, "type": r["STORMTYPE"]})
    z = zipfile.ZipFile(fcstzip); n = [x for x in z.namelist() if x.endswith("forecastradii.shp")][0][:-4]
    sf = shapefile.Reader(shp=io.BytesIO(z.read(n + ".shp")), dbf=io.BytesIO(z.read(n + ".dbf")))
    names = [f[0] for f in sf.fields[1:]]
    by = {p["tau"]: p for p in pts}
    for r in sf.records():
        r = dict(zip(names, r))
        if int(r["TAU"]) in by:
            by[int(r["TAU"])]["radii"][int(r["RADII"])] = [int(r[q]) for q in ("NE", "SE", "SW", "NW")]
    return sorted(pts, key=lambda p: p["tau"])

def hourly(pts):
    out = []
    for a, b in zip(pts, pts[1:]):
        for h in range(a["tau"], b["tau"]):
            f = (h - a["tau"]) / (b["tau"] - a["tau"])
            dlon = ((b["lon"] - a["lon"] + 540) % 360) - 180
            rad = {k: [ra + f * (rb - ra) for ra, rb in zip(a["radii"].get(k, [0] * 4), b["radii"].get(k, [0] * 4))]
                   for k in (34, 50, 64)}
            out.append((h, a["lat"] + f * (b["lat"] - a["lat"]), a["lon"] + f * dlon, a["vmax"] + f * (b["vmax"] - a["vmax"]), rad))
    last = pts[-1]; out.append((last["tau"], last["lat"], last["lon"], last["vmax"], {k: last["radii"].get(k, [0] * 4) for k in (34, 50, 64)}))
    return [o for o in out if o[0] <= 120]

def assess(storms):
    res = {}
    for pid, p in PLACES.items():
        if south_atlantic(*p):
            res[pid] = {"state": "not_covered", "reason": "South Atlantic: no publishable cyclone source (R2)"}; continue
        best = {"state": "clear", "nearest_km": None}
        for sid, track in storms.items():
            for h, lat, lon, vmax, rad in hourly(track):
                d = hav((lat, lon), p); q = int(bearing((lat, lon), p) // 90)
                if best["nearest_km"] is None or d < best["nearest_km"]:
                    best.update(nearest_km=round(d), nearest_storm=sid, nearest_tau_h=h)
                if vmax >= 34 and (best.get("nearest_ts_km") is None or d < best["nearest_ts_km"]):
                    best["nearest_ts_km"] = round(d)  # nearest approach while the storm is at least tropical-storm strength
                for kt in (64, 50, 34):
                    if d <= rad[kt][q] * NM:
                        lvl = {34: "ts_force", 50: "50kt", 64: "hurricane_force"}[kt]
                        cur = best.get("band", 0)
                        if kt > cur:
                            best.update(state="flag", band=kt, level=lvl, storm=sid, first_tau_h=h if kt != cur else best.get("first_tau_h", h))
                        if "first_34_tau_h" not in best and kt >= 34:
                            best["first_34_tau_h"] = h
                        break
        if best["state"] == "clear" and best.get("nearest_ts_km") is not None and best["nearest_ts_km"] <= WATCH_KM:
            best["state"] = "watch"
        res[pid] = best
    legs = {}
    for leg, places in LEGS.items():
        st = [res[p]["state"] for p in places]
        nc = [p for p in places if res[p]["state"] == "not_covered"]
        hot = [p for p in places if res[p]["state"] in ("flag", "watch")]
        if not places: legs[leg] = "no_places_listed"; continue
        base = "flag" if "flag" in st else "watch" if "watch" in st else "not_covered" if len(nc) == len(places) else "clear"
        legs[leg] = base + (f" at {hot}" if hot else "") + (f" | not covered: {nc}" if nc and base != "not_covered" else "")
    return res, legs

if __name__ == "__main__":
    t0 = time.time(); storms = {}; log = {}
    if len(sys.argv) > 1 and sys.argv[1] == "replay":  # Francine 2024, advisory given as argv[2]
        a = sys.argv[2]; storms[f"al062024 adv {a}"] = parse_nhc(f"al062024_5day_{a}.zip", f"al062024_fcst_{a}.zip")
    else:
        nhc = requests.get("https://www.nhc.noaa.gov/CurrentStorms.json", headers=UA, timeout=30); nhc.raise_for_status()
        nhc_ids = []
        for s in nhc.json()["activeStorms"]:
            sid = s["id"]; nhc_ids.append(sid)
            f5 = requests.get(f"https://www.nhc.noaa.gov/gis/forecast/archive/{sid}_5day_latest.zip", headers=UA, timeout=30)
            ff = requests.get(f"https://www.nhc.noaa.gov/gis/forecast/archive/{sid}_fcst_latest.zip", headers=UA, timeout=30)
            f5.raise_for_status(); ff.raise_for_status()
            storms[f"NHC {sid} {s['name']}"] = parse_nhc(io.BytesIO(f5.content), io.BytesIO(ff.content))
            log[sid] = {"source": "NHC", "lastUpdate": s["lastUpdate"], "intensity_kt": s["intensity"]}
        rss = requests.get("https://www.metoc.navy.mil/jtwc/rss/jtwc.rss", headers=UA, timeout=30); rss.raise_for_status()
        for tcw in sorted(set(re.findall(r"https://www\.metoc\.navy\.mil/jtwc/products/(\w+)\.tcw", rss.text))):
            r = requests.get(f"https://www.metoc.navy.mil/jtwc/products/{tcw}.tcw", headers=UA, timeout=30)
            if r.status_code == 403:
                log[tcw] = {"source": "JTWC", "state": "product_absent (403)"}; continue
            r.raise_for_status()
            hdr = r.text.splitlines()[2].split()
            # de-duplicate against NHC by storm number + basin, not by the 'ep' prefix: a storm that crossed 180 deg
            # (NHC drops it) keeps its 'E' id at JTWC
            num = int(tcw[2:4]); dup = any(i.startswith(tcw[:2]) and int(i[2:4]) == num for i in nhc_ids)
            pts = parse_jtwc(r.text)
            log[tcw] = {"source": "JTWC", "name": hdr[2], "issued": hdr[0], "t0": [pts[0]["lat"], pts[0]["lon"]],
                        "vmax_kt": pts[0]["vmax"], "max_tau_h": pts[-1]["tau"], "dropped_as_nhc_duplicate": dup}
            if not dup:
                storms[f"JTWC {tcw} {hdr[2]}"] = pts
    res, legs = assess(storms)
    out = {"storms": log, "places": res, "legs": legs, "seconds": round(time.time() - t0, 1)}
    json.dump(out, open(f"storms_{sys.argv[1] if len(sys.argv) > 1 else 'today'}{'_' + sys.argv[2] if len(sys.argv) > 2 else ''}.json", "w"), indent=1)
    print(json.dumps(log, indent=0)); print("seconds", out["seconds"])
    for pid, r in sorted(res.items(), key=lambda x: (x[1].get("nearest_km") or 1e9)):
        print(f"  {pid:7s} {r['state']:12s} nearest={r.get('nearest_km')} km {r.get('nearest_storm','')} tau={r.get('nearest_tau_h','')} {r.get('level','')} {r.get('first_34_tau_h','')}")
    for leg, s in legs.items(): print(f"  LEG {leg:32s} {s}")
