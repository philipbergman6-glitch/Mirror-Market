# Spec — footprint weather (build-ready)

Ticket: [S1 · Spec: footprint weather](https://github.com/philipbergman6-glitch/Mirror-Market/issues/373) · Map: [God's Eye View findings](https://github.com/philipbergman6-glitch/Mirror-Market/issues/356) · Written 2026-10-06 against `main` @ `6a63996`. Sibling: [S2 · Spec: cyclone hazard flags](https://github.com/philipbergman6-glitch/Mirror-Market/issues/374) (`docs/specs/cyclone-hazard-flags.md`, [PR #375](https://github.com/philipbergman6-glitch/Mirror-Market/pull/375)).

This is a plan, not a build. A build session should be able to execute it from top to bottom without re-deciding anything. Line numbers are from `6a63996` and will drift. Function and constant names are the reference.

**Where each decision comes from**

| Tag | Source |
|---|---|
| **[P1]** | [P1 · Presentation prototype + rules sign-off](https://github.com/philipbergman6-glitch/Mirror-Market/issues/362), signed off 2026-10-06, variant A. Prototype on branch `prototype/p1-footprint-presentation` |
| **[K2]** | [K2 · Spike](https://github.com/philipbergman6-glitch/Mirror-Market/issues/361). Findings in `research/k2-footprint-weather-spike.md`, reference code in `spike/footprint-weather/`, both on branch `spike/footprint-weather` |
| **[R4]** | [R4 · Observed side of footprint weather](https://github.com/philipbergman6-glitch/Mirror-Market/issues/369). `research/r4-observed-footprint.md` on branch `research/r4-observed-footprint` |
| **[R1]** | [R1 · ECMWF Open Data](https://github.com/philipbergman6-glitch/Mirror-Market/issues/357). `research/2026-10-05-issue-357-ecmwf-open-data.md` on branch `research/r1-ecmwf-open-data` |
| **[R3]** | [R3 · Climatology + production weights](https://github.com/philipbergman6-glitch/Mirror-Market/issues/359). `research/2026-10-05-issue-359-r3-climatology-weights.md` on branch `research/r3-climatology-weights` |
| **[K1]** | [K1 · Place list](https://github.com/philipbergman6-glitch/Mirror-Market/issues/360). `research/k1-place-list.md` on branch `research/k1-place-list` |
| **[S2]** | The sibling spec. S1 fills S2's port-rain slot and supplies two of its three `DESIGN.md` rows |
| **[S1]** | Decided in this spec, because the upstream tickets left it open or the code contradicted them. Every one is listed in §15 so the owner can overrule it in one place |

---

## 1. What ships

A **footprint** is an area outline whose weather is averaged as a whole (`CONTEXT.md`). Version 1 ships:

1. **One new pipeline layer**, `footprint_weather`. It reads the ECMWF IFS 00z open-data forecast once a day and reduces it to:
   - 20 crop footprints, each one or more admin-1 units weighted by SPAM 2020 production of the crop the footprint prices;
   - 5 port rain boxes.
2. **Two committed reference files**, both built once on a desk machine and never in CI:
   - the footprint point weights;
   - a daily ERA5 1991–2020 series per footprint, which is the "normal".
3. **The elapsed side** (R4). Each run's day-1 values are persisted, and a self-healing backfill from the ECMWF AWS mirror keeps at least 120 days per footprint. This replaces Open-Meteo's trailing 30 days as what the six existing rules grade. It is labelled **"day-1 estimate · ECMWF 00z"**, never "observed".
4. **A forecast alert class** on the shared seam `analysis/weather_alerts.py`. It has five rules, is capped at `warning`, and carries its own `basis`.
5. **One region reader**, `pipeline.query.read_region_weather()`, so every surface that grades weather reads the same series. Block 06, the Risk Monitor, the briefing, market drivers, Emerging Markets and players cannot disagree about a region.
6. **Surfaces:**
   - block 06 variant A cards;
   - Risk Monitor `forecast` rows;
   - a `WEATHER OUTLOOK` sub-block in the briefing's weather section;
   - the port-rain caption in S2's storm rows;
   - the attribution caption;
   - two `DESIGN.md` rows.
7. **The pin retirement.** Layer 5 (Open-Meteo) shrinks to the two palm pins, which have no footprint in v1. The two canola-prairie pins are dropped [P1 #9a].

**Not in version 1.** Each of these is a decision, not an omission.

| Not | Why |
|---|---|
| A soil-moisture alert or soil anomaly | IFS `vsw` and ERA5-Land `swvl1` disagree by up to 0.07 m³/m³ inside one season [R4 Soil], and MT's day-0 level moved 0.236 → 0.335 between consecutive runs [K2 §7.3]. The card shows level and 15-day change only |
| Footprints for palm (Riau, Sabah) | [P1 #2]: nothing on the headline, Emerging Markets or the competing-oil strip in v1. The palm pins stay on Layer 5 and keep feeding the strip |
| Official-statistics weights *between* footprints | Every card is one footprint [P1 #1], so there is no belt-level number to weight. SPAM sets the weights *inside* a footprint [K2 §7.1]. R3's NASS / IBGE / MAGyP 5-year means return only if a single per-leg belt number is ever wanted (§15.4) |
| An admin-2 (county) layer | NASS county weights moved Iowa by ≤ 0.2 mm across 4 runs [K2 §3] |
| The ECMWF ensemble or its rain probabilities | Not in [P1]. R1 found them open and cheap; this is a later idea, not fog |
| A real observation source (ERA5T, CPC, CHIRPS) | [R4]: ERA5T is also a model and arrives 6 days late behind a queue; CPC Tmax fails in MT; CHIRPS is 2–7 days late in pentad batches. Splicing two sources into one 120-day window fakes anomalies |
| Port-loading thresholds ("halts at X mm") | No sourced threshold exists [K2 §5]. Port rain renders as a number and a wet-day count, with no alert |
| Hazard flags | S2 |

---

## 2. Source — ECMWF Open Data (IFS)

| | |
|---|---|
| Layer key | `footprint_weather` |
| Run | **00z only** [R1]. Steps 0–144 h every 3 h, 150–360 h every 6 h |
| Primary host | `https://data.ecmwf.int/forecasts/{YYYYMMDD}/00z/ifs/0p25/oper/{YYYYMMDD}000000-{step}h-oper-fc.{index,grib2}` |
| Mirror | `https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/{same path}`. Anonymous, history back to 2023-01-18, **no retention promise** [R1 §3] |
| Portal retention | About 3½ days, "the most recent 12 forecast runs" [R1] |
| Published | Run D appeared on the mirror at 07:34 UTC on D (`Last-Modified`, 2026-10-06) [R4] |
| Access | `.index` is JSON Lines with `_offset` and `_length`. Each field is an HTTP Range request answered `206` [K2 `fetch_ecmwf.py`] |
| Decoder | `eccodes` (the `eccodeslib` binary wheel; no `apt-get`). CCSDS packing, 16 bits [R1 §8] |
| Grid | 0.25°, `Ni=1440`, `Nj=721`, first latitude 90, first longitude −180, `jScansPositively=0`. Flat index = `row × 1440 + col`, `row = rint((90 − lat)/0.25)`, `col = rint((lon + 180)/0.25) % 1440` |
| Key | none |
| Licence | CC BY 4.0 **plus** the ECMWF Terms of Use. Commercial use allowed. The service wording is mandatory (§8.7) [R1 §7] |

**Fields read per run** [R1, K2, S1]:

| Param | Steps | Messages | Use |
|---|---|---|---|
| `tp` (m, accumulated from run start) | 24, 48, …, 360 | 15 | Daily rain = `tp(24d) − tp(24(d−1))`, with `tp(0) = 0` |
| `mx2t3`, `mn2t3` (K) | 3, 6, …, 144 | 48 + 48 | Daily Tmax/Tmin for days 1–6 = max/min of the windows ending in `(24(d−1), 24d]` |
| `mx2t6`, `mn2t6` (K) | 150, 156, …, 360 | 36 + 36 | Same for days 7–15. Day 7 = steps 150–168. The 3 h / 6 h boundary falls exactly at 144 h, so no day mixes the two |
| `vsw` level 1 (m³/m³) | 0, 24, …, 360 | 16 | Soil now (step 0) and at day 15 (step 360). Land points only |
| **Total** | | **199** | |

K2 read 248 messages (`vsw` levels 1–4 plus `lsm`) at 155–156 MB. Dropping levels 2–4 and reading the land mask from the weights file (§4.3) gives **199 messages, about 125 MB**. That is an inference from K2's per-message size, not a measurement.

**Rights**, in `trust.registry`'s `RightsAction` vocabulary. Like Layers 27–32 and S2's layers, this one is *not* registered in `PILOT_REGISTRY`; the table is written so a later migration is a transcription.

| `RightsAction` | ECMWF Open Data | ERA5 (normal) | SPAM 2020 (weights) |
|---|---|---|---|
| `raw-content-retention` | allowed | allowed | allowed |
| `normalized-history-retention` | allowed | allowed | allowed |
| `internal-display` | allowed | allowed | allowed |
| `public-display` | allowed, with the §8.7 wording | allowed, "vs ERA5 1991–2020" + C3S wording | allowed, with the IFPRI disclaimer |
| `derived-publication` | allowed, with a modification note | allowed | allowed |
| `commercial-use` | allowed ("for any purpose, even commercially") | allowed (CC BY since 2025-07-02 [R4]) | allowed (CC BY 4.0 [R3]) |
| `redistribution` | allowed | allowed | allowed |

**Before go-live (carried from R1, not re-verified):** read the ECMWF service-agreement PDF. Anonymous download needs no agreement, and R1 inferred that the clause covers provision services, not download. That is a human read, not a build step (§16).

---

## 3. Data contract

### 3.1 Layer registration

Add one row to `config.PRODUCTION_LAYERS`, after `sea_india`:

```python
("footprint_weather", "<n>", "ECMWF Open Data (IFS 00z)", "Daily run, read daily", "15-day crop-weighted rain, heat and soil over 20 crop footprints + 5 port boxes"),
```

`<n>` is the next free group number when the build starts. On `6a63996` that is 33, but S2's two cyclone layers may take numbers first. Count from `config.PRODUCTION_LAYERS`, never from this prose.

Everything else the layer touches:

| File | Change |
|---|---|
| `fetchers/footprint_weather.py` (new) | `fetch_footprint_weather(today)` → `dict[str, pd.DataFrame]` with exactly the keys `"runs"`, `"daily"`, `"summary"` (§3.2). Holds the HTTP, index and Range logic and the backfill (§5.6) |
| `pipeline/footprints.py` (new) | Pure functions, no I/O beyond reading the two reference files: `load_weights()`, `decode_field()`, `daily_from_steps()`, `aggregate()`, `load_normal()`, `compare_to_normal()`. Unit-tested on fixtures |
| `pipeline/schema.py` | The three tables in §3.2 |
| `pipeline/clean.py` | `clean_footprint_frame(name, df)`. Returns a copy; applies §3.4 |
| `pipeline/store.py` | `save_footprint_frame(name, df)`, applying §3.3 |
| `pipeline/query.py` | `read_footprint_runs()`, `read_footprint_daily()`, `read_footprint_summary()`, and the region reader `read_region_weather()` (§7.1) |
| `pipeline/history.py` | Two `HISTORY_TABLES` entries (§3.2) |
| `main.py` `_build_dict_layers` | One `DictLayer` after `sea_india`, `empty_fails=True` |
| `latency/domain.py` | One `LayerLatency` entry (§10) |
| `requirements.txt` | `eccodes` with a `~=` pin on the version the build installs (2.49.x when written) |
| `requirements-dev.txt` | Nothing. The two build scripts' heavy dependencies (`rasterio`, `shapely`, `pyshp`, `cdsapi`, `xarray`, `netCDF4`) go in a new `requirements-reference.txt`, installed only on the machine that rebuilds the reference files. CI never installs it |
| `config.py` | `FOOTPRINTS`, `PORT_RAIN_BOXES`, the §7.3 constants, `LAYER_KEY_CATALOGS["footprint_weather"]`, `LAYER_MIN_KEYS["footprint_weather"] = 25` (all-or-nothing, §3.5). Not in `FAST_REFRESH_LAYERS`. No `LAYER_MAX_DATA_AGE_DAYS` entry (§3.5) |
| `data/reference/footprints/` (new) | `crosswalk.csv`, `weights.csv`, `era5_daily_1991_2020.csv.gz`, `manifest.json`, `README.md` (§4, §6) |
| `scripts/build_footprints.py`, `scripts/build_era5_normals.py` (new) | Local reference builders (§4.3, §6.1). Never imported by the pipeline |
| `LAYERS.md` | One layer entry carrying the §13 traps; Layer 5's entry rewritten for its two palm pins; layer counts |
| `ARCHITECTURE.md` | The tables in "Store"; a paragraph modelled on the `river_levels` bullet; a "footprint" row in "Where to add new things"; the reference-build step |
| `CLAUDE.md`, `README.md`, `tests/test_docs_claims.py` | Layer count +1, group count +1 (whatever `PRODUCTION_LAYERS` then says). Also fix the Layer 5 description, which says "19 growing regions" (`config.py:132`, `README.md:38`) when the dict holds 24 |

### 3.2 Tables

All dates are UTC ISO-8601 text. Rain is in mm, temperature in °C, soil in m³/m³, and shares are fractions 0–1. Nothing here is a price, and nothing passes through `to_usd_mt`.

**`footprint_weather_runs`** has one row per ECMWF run the layer ingested, daily or backfilled. History: **yes**.

| Column | Type | Meaning |
|---|---|---|
| `run_date` | TEXT NOT NULL | The 00z run's date |
| `origin` | TEXT NOT NULL | `daily` (all 199 fields) or `backfill` (day-1 fields only, §5.6) |
| `host` | TEXT NOT NULL | `data.ecmwf.int` or `aws-mirror` |
| `checked_at` | TEXT NOT NULL | When the index was read |
| `index_last_modified` | TEXT | The index's `Last-Modified` header, or NULL if not served |
| `messages` | INTEGER NOT NULL | Fields fetched: 199 daily, 10 backfill |
| `bytes` | INTEGER NOT NULL | |
| `model_cycle` | TEXT | Read from the GRIB keys (`stream/class/expver` + cycle), or NULL if the build cannot find a key that names it. A cycle change steps the series (§13.8) |
| `weights_built_at` | TEXT NOT NULL | From `manifest.json`, so a later weights rebuild cannot rewrite the meaning of history |
| `attribution` | TEXT NOT NULL | `ECMWF Open Data (IFS), CC BY 4.0` |

The primary key is `(run_date)`. A `daily` row replaces a `backfill` row for the same date. A `backfill` row never replaces a `daily` one.

**`footprint_weather_daily`** has one row per footprint per valid day, **for the latest daily run only**. History: **no**.

| Column | Type | Meaning |
|---|---|---|
| `footprint` | TEXT NOT NULL | The `config.FOOTPRINTS` / `PORT_RAIN_BOXES` key |
| `run_date` | TEXT NOT NULL | |
| `valid_date` | TEXT NOT NULL | `run_date + lead_day − 1` |
| `lead_day` | INTEGER NOT NULL | 1–15 |
| `rain_mm` | REAL NOT NULL | Weighted mean |
| `wet_area_share` | REAL NOT NULL | Weight share of points with ≥ `WEATHER_DRY_THRESHOLD_MM` that day |
| `heavy_area_share` | REAL NOT NULL | Weight share of points with ≥ `WEATHER_HEAVY_RAIN_MM` that day |
| `tmax_c`, `tmin_c` | REAL | Weighted means of the point daily extremes. NULL for port boxes, which read rain only |
| `vsw_start` | REAL | Weighted land-point `vsw` L1 at the start of the valid day (step `24(lead−1)`). NULL for port boxes |

The primary key is `(footprint, lead_day)`. The table is window-replaced on every daily run, so it never holds two runs.

**`footprint_weather_summary`** has one row per footprint per run. It is the system of record [P1 #6] and carries the run's day-1 values for the elapsed side [R4]. History: **yes**.

| Column | Type | Meaning |
|---|---|---|
| `footprint` | TEXT NOT NULL | |
| `run_date` | TEXT NOT NULL | |
| `kind` | TEXT NOT NULL | `crop` or `port` |
| `origin` | TEXT NOT NULL | `daily` or `backfill` |
| `d1_rain_mm` | REAL NOT NULL | Day 1 (valid = `run_date`) |
| `d1_wet_area_share` | REAL NOT NULL | |
| `d1_tmax_c` | REAL | NULL for ports |
| `d1_tmin_c` | REAL | NULL for ports **and for backfill rows** (the backfill does not fetch `mn2t3`; NULL = never learned) |
| `vsw_now` | REAL | Step 0. NULL for ports |
| *The rest is NULL on backfill rows:* | | |
| `rain15_mm` | REAL | Days 1–15 total |
| `wet_days_5mm` | INTEGER | Days with `rain_mm ≥ PORT_RAIN_WET_DAY_MM` (5). Rendered for ports only [P1 #7]; stored for all |
| `dry_run_area_share` | REAL | Weight share of points whose days 1 … `WEATHER_DRY_SPELL_ALERT_DAYS` are all < 1 mm (§7.3) |
| `heavy_area_share` | REAL | Weight share of points with any day ≥ `WEATHER_HEAVY_RAIN_MM` |
| `tmax_mean_c` | REAL | Mean of daily weighted Tmax, days 1–15 |
| `tmax_peak_c` | REAL | Highest daily weighted Tmax |
| `heat34_days` | INTEGER | Days with weighted Tmax ≥ `WEATHER_POD_FILL_HEAT_C`, counted only on valid days in the footprint's pod-fill months; NULL when the footprint has no pod-fill calendar |
| `heat34_days_all` | INTEGER | The same count ignoring the calendar, for the withheld caption (§7.3) |
| `vsw_day15` | REAL | Step 360 |
| `normal_rain15_mean`, `normal_rain15_lo`, `normal_rain15_hi`, `normal_rain15_min`, `normal_rain15_max` | REAL | The ERA5 window: mean, tercile bounds, 30-year min and max (§6.2) |
| `rain15_rank` | INTEGER | 1 = driest of 31 |
| `rain15_tercile` | TEXT | `lower`, `middle` or `upper` |
| `rain15_pct_vs_mean` | REAL | `(rain15 − mean) / mean × 100`. NULL when the mean is 0 |
| `normal_tmax_mean`, `normal_tmax_sd` | REAL | |
| `tmax_anom_c` | REAL | `tmax_mean_c − normal_tmax_mean` |
| `pin_rain15_mm`, `pin_rain15_tercile` | REAL / TEXT | The ECMWF value at the old Layer 5 pin's nearest grid point, and its tercile against the pin's own ERA5 series. Release 1 only (§8.3). NULL for ports |
| `attribution` | TEXT NOT NULL | |

The primary key is `(footprint, run_date)`.

`HISTORY_TABLES` entries, each with a comment in the file's own style:

```python
"footprint_weather_runs": ("run_date",),
"footprint_weather_summary": ("footprint", "run_date"),
```

Why these are history tables:
- ECMWF's portal keeps 3½ days and the mirror promises nothing [R1].
- The elapsed side needs ≥ 120 days on a deploy runner that starts from a blank DB (`weather_alerts.py:28-32`).

Why `footprint_weather_daily` is not: [P1 #6] approved summary rows only. The mirror can recompute daily detail if a verification study is ever wanted [K2 §7.5].

Rows: 25 per daily run, plus 1 runs row. About 6,800 a year on 260 weekday runs, plus about 2,600 once for the first-deploy backfill and about 2,100 a year for weekend backfill (§5.6). For scale, `contract_bars.csv` holds about 32k rows.

### 3.3 Write rules

`save_footprint_frame(name, df)`:

- `runs`: upsert, except that a `backfill` row never overwrites an existing `daily` row (check, then insert).
- `summary`: the same rule on `(footprint, run_date)`.
- `daily`: **window-replace**. Delete every row, then insert, in one transaction, using `_save(..., clear=(sql, params))` (the `brazil_exports` precedent). A backfill-only fetch carries no `daily` frame, and the table is then left untouched.
- Hard-fail (`ValueError`) on:
  - a missing `attribution`;
  - an unknown footprint key;
  - a `daily` frame that does not hold exactly 15 lead days for each of the 25 keys;
  - a `summary` row with `origin = 'daily'` and a NULL in any non-nullable-by-design column.

### 3.4 Clean rules

`clean_footprint_frame` returns a copy and:
- coerces types and drops nothing silently;
- converts K → °C and m → mm (**the only unit conversion here, so it lives in `pipeline/units.py` per invariant 7**: add `kelvin_to_c` and `metres_to_mm`);
- clamps a daily rain difference in [−0.05, 0) mm to 0. Anything below −0.05 hard-fails. The worst seen was −0.0153 mm from 16-bit packing [K2 §8];
- hard-fails on any of: rain outside 0–1,000 mm/day; Tmax/Tmin outside −80…+60 °C; Tmin > Tmax on the same footprint-day; `vsw` outside (0, 1] on a land point; shares outside 0–1; a lead day outside 1–15; a duplicate primary key.

### 3.5 How a run is graded

The layer is **all-or-nothing**: one global message serves every footprint, so a partial answer means a broken fetch, not a quiet footprint.

| What happened | Layer run state | Why |
|---|---|---|
| Today's 00z index answered, all 199 fields fetched and decoded, all 25 keys aggregated | `success` | |
| Today's run answered on the mirror after the portal failed | `success`, `host = aws-mirror` | Same file, same licence [R1 §3] |
| Today's index answers 404 on both hosts | `failed` | ECMWF publishes every run; a missing one is an outage. The 20:40 UTC retry (`retry-failed-pipeline.yml`) is the second shot |
| An index lacks a requested field, or holds it twice | `failed` | K2's `len(hit) != 1` assertion |
| A Range answer is not `206` with exactly `_length` bytes | `failed` | |
| GRIB keys disagree with §2's grid, or `dataDate` ≠ the requested run date | `failed` | The second check is invariant 10's protection: a date-pathed URL serving another date is a frozen file at HTTP 200 |
| A clean rule fails (§3.4) | `failed` | |
| The backfill could not fetch some missing days (§5.6) | **no effect on the state** | Today's run answered in full. The missing days stay NULL, the runs table records which ones, and the rules withhold with their own reasons |
| Returned `{}` | `failed` | `empty_fails=True` |

`no_publication` is never used by this layer: ECMWF has no "no release today". `incomplete` cannot occur: the key catalog is `FOOTPRINTS ∪ PORT_RAIN_BOXES` (25), `LAYER_MIN_KEYS = 25`, and the all-or-nothing rule fires first.

**No `LAYER_MAX_DATA_AGE_DAYS` entry, and why that is not a gap.** The fetcher always asks for today's run, and §3.5 hard-fails on `dataDate` ≠ that date. So the frame's newest `Date` equals today by construction, and a whole-day budget would pass trivially. The age that matters is checked at render time, per footprint (§3.6).

### 3.6 Per-footprint render states

Every reader asks `pipeline.footprints.footprint_state(summary_row, today)`, which returns one of the following.

| State | Condition | Card | Alerts |
|---|---|---|---|
| `ok` | Latest `daily` run = today's date | Normal | Forecast alerts render |
| `stale` | Latest `daily` run is 1 to `FOOTPRINT_FORECAST_MAX_AGE_DAYS` (3) days old | Normal, plus a `stale` tag beside the stamp naming the run date | Forecast alerts render; their basis names the older run |
| `failed` | No `daily` run within 3 days, and the layer's last run state is `failed` | Warm empty state: `no reading · {reason from data_freshness}` | Forecast rules withheld: "no ECMWF run within 3 days" |
| `no data` | No summary row at all (fresh install, `conn is None`) | The block's existing `STATE_EMPTY` path | none |

A footprint is never shown "beside" an older run from another source. The elapsed side (§7.1) has no state of its own: missing days are NULL, and the six rules already withhold on a thin record.

---

## 4. Footprint registry

### 4.1 `config.FOOTPRINTS`

**The key is the existing region name**, so `MARKETS[...]["weather_regions"]`, `WEATHER_GROWING_SEASON_MONTHS`, `WEATHER_SOY_POD_FILL_MONTHS` and `SOY_WEATHER_REGIONS` keep working unchanged. The K1 place id rides along for S2 and the docs.

```python
FOOTPRINTS = {
    "US Midwest (Iowa)": {
        "place_id": "G-IA", "label": "Iowa", "crop": "SOYB",
        "units": ("US-IA",),
    },
    ...
}
```

The 20 entries are below. `units` are ISO 3166-2 codes, joined to Natural Earth's `iso_3166_2` field. K1 wrote its codes "from memory… verify at build"; the crosswalk build (§4.3) is that verification, and it hard-fails.

| Key (existing region) | `place_id` | `label` | `units` | `crop` | Pages |
|---|---|---|---|---|---|
| US Midwest (Iowa) | G-IA | Iowa | US-IA | SOYB | CBOT |
| US Illinois | G-IL | Illinois | US-IL | SOYB | CBOT |
| US Nebraska | G-NE | Nebraska | US-NE | SOYB | CBOT |
| Brazil Mato Grosso | G-MT | Mato Grosso | BR-MT | SOYB | Brazil, Dalian |
| Brazil Parana | G-PR | Paraná | BR-PR | SOYB | Brazil |
| Brazil Rio Grande do Sul | G-RS | Rio Grande do Sul | BR-RS | SOYB | Brazil |
| Argentina Pampas | G-PAM | Pampas (Buenos Aires + Santa Fe) | AR-B, AR-S | SOYB | Argentina |
| Argentina Cordoba | G-COR | Córdoba | AR-X | SOYB | Argentina |
| Paraguay Alto Parana | G-PY | Eastern Paraguay | PY-10, PY-7, PY-14 | SOYB | Argentina |
| Argentina Buenos Aires (sunflower) | G-BAS | Buenos Aires | AR-B | SUNF | Argentina |
| India Madhya Pradesh | G-MP | Madhya Pradesh | IN-MP | SOYB | India |
| India Maharashtra | G-MH | Maharashtra | IN-MH | SOYB | India |
| China Heilongjiang | G-HL | Heilongjiang | CN-HL | SOYB | Dalian |
| France Champagne (Grand Est) | G-FR | Grand Est | FR-GES (dissolve, §4.3) | RAPE | Europe |
| Germany Mecklenburg-Vorpommern | G-DE | Mecklenburg-Vorpommern | DE-MV | RAPE | Europe |
| Romania Baragan (Danube plain) | G-RO | Bărăgan | RO-BR, RO-CT, RO-IL, RO-CL | RAPE | Europe |
| South Africa Free State | G-FS | Free State | ZA-FS | SOYB | South Africa |
| South Africa Mpumalanga | G-MPU | Mpumalanga | ZA-MP | SOYB | South Africa |
| Nigeria Benue | G-BEN | Benue | NG-BE | SOYB | Nigeria |
| Nigeria Kaduna | G-KAD | Kaduna | NG-KD | SOYB | Nigeria |

Multi-unit footprints follow [K2 §7.1]: G-PAM, G-PY and G-RO, plus G-FR's dissolve. R3 scoped Paraguay as Alto Paraná alone; K2's three-department footprint was the one approved at P1 [P1 #5].

**The card's crop word comes from `crop`:** `SOYB` → "soy area", `SUNF` → "sunflower area", `RAPE` → "rapeseed area". So Europe's cards read "Grand Est · rapeseed area", and the existing role line ("rapeseed, not soy — EU #1") stays.

**Nigeria keeps its footprints.** The Nigeria page renders block 06 today and still will. [P1 #9b] says only that Nigeria gets no hazard flags (S2's concern).

**Validation** goes in `app/markets.py` beside `_weather_roles`. `load_markets()` raises `ValueError` if:
- a market's `weather_regions` names a region that is neither in `FOOTPRINTS` nor a Layer 5 pin;
- a `FOOTPRINTS` key is not a `GROWING_REGIONS` key (the pin's coordinates are needed for the release-1 pin line);
- a footprint has no rows in `weights.csv`.

### 4.2 `config.PORT_RAIN_BOXES`

S2 owns the port registry (`config.PLACES`); S1 owns the rain. The keys are S2's place ids:

```python
PORT_RAIN_BOXES = ("P-NOLA", "P-PNG", "P-UPR", "P-NCN", "P-DUR")
PORT_RAIN_BOX_CELLS = 3          # 3×3 grid points centred on the nearest node [K2 §5]
PORT_RAIN_WET_DAY_MM = 5.0       # "days ≥ 5 mm (loading delays)" [P1 #7]
```

These are the five ports S2's block 06 lists (S2 §8.3). The three inland pricing points (Burns Harbor, Randfontein, Indore) are not ports, so they get no box [S1]. The box is equal-weighted and reads `tp` only. Coordinates come from `config.PLACES[place_id]`. **If S2's registry slice has not landed, the port-rain slice waits for it** (§14). S1 does not define a second copy of the port coordinates.

### 4.3 The weights file — `scripts/build_footprints.py`

This is run by hand on a desk machine with `requirements-reference.txt` installed. **CI never reads SPAM or Natural Earth** [K2 §7.1].

**Inputs:** paths come from the environment and are never committed:
- `SPAM2020_DIR`: the extracted `spam2020_V2r2_global_P_{SOYB,SUNF,RAPE}_A.tif`;
- `NATURAL_EARTH_ADMIN1`: `ne_10m_admin_1_states_provinces.shp`, release 5.1.2;
- `ECMWF_LSM_GRIB`: one `lsm` step-0 message saved from any run.

**Method** [K2 `footprints.py`]:
1. Select each unit's polygon from Natural Earth by `iso_3166_2`. G-FR instead dissolves every row whose `region_cod == "FR-GES"`.
2. For every SPAM 5′ cell whose centre falls inside the footprint polygon, add the cell's production to the **nearest ECMWF 0.25° node**.
3. Normalise the weights to sum to 1.
4. Record the node's land flag from `lsm ≥ 0.5`.

**Port boxes:** the 3×3 nodes around the node nearest the port, each weighted 1/9, land flag recorded.

**Pins:** for each `FOOTPRINTS` key, the node nearest the `GROWING_REGIONS` pin, written as footprint `pin:<key>` with weight 1. This exists for release 1 only (§8.3).

**Hard-fails** (invariant 1):
- a unit with zero or more than one Natural Earth row;
- a Natural Earth `wikidataid` that differs from `crosswalk.csv`;
- a footprint with zero total production;
- any SPAM nodata or NaN inside a polygon carrying more than 0.5 % of the footprint's production (R4 measured 0.0 %);
- a footprint whose land weight is below 50 % (soil would be meaningless).

**`crosswalk.csv`** is committed by hand in slice 1. Columns: `footprint, iso_3166_2, ne_adm1_code, wikidata_id, boundary_source`. The Wikidata id is the spine [R3 §3.2], so a Natural Earth release that renumbers or retypes a unit (`AR-B` is typed "Federal District", like CABA [R3]) fails loudly instead of silently picking the wrong polygon.

**Outputs:**

| File | Columns | Expected size |
|---|---|---|
| `weights.csv` | `footprint, kind, grid_row, grid_col, lat, lon, weight, land` | About 10k rows: MT alone is 1,278 nodes and IA 274 [K2 §2] |
| `manifest.json` | `built_at`, `spam_version` ("2020 V2r2"), `natural_earth_version` ("5.1.2"), SHA-256 of every input file, per-footprint `{nodes, production_t, land_weight, top_quarter_share}` | small |

### 4.4 The yearly rebuild and its age budget

`FOOTPRINT_WEIGHTS_MAX_AGE_DAYS = 548` (18 months [R3 §2.2]).

- Every run stamps `weights_built_at` on its runs row.
- When the manifest is older than the budget, the layer logs a `WARNING` and adds `footprint_weights_overdue` to the run summary's warnings list. **The layer state is not changed.** The numbers are still correct for the weights they used, and a rebuild with unchanged SPAM 2020 and Natural Earth 5.1.2 inputs produces the same file. The budget forces a *look*, not a re-run [S1].
- The look is a checklist in `data/reference/footprints/README.md`:
  1. Has SPAM published a newer release?
  2. Has Natural Earth?
  3. Did a rendered market gain or lose a weather region?

  If any answer is yes, rebuild. Either way, bump `built_at` with a one-line reason in the manifest.
- **No date-bomb test.** A unit test that fails when the manifest ages would kill the deploy on a calendar date, which is how the 2026-09-08 → 09-19 outage happened (crush test, U26). The age is reported, not asserted.

---

## 5. The daily fetch

### 5.1 Order of work

`fetch_footprint_weather(today)`:

1. Load `weights.csv` and `manifest.json`. Take the union of the grid indices.
2. Read the 85 step indices for `today`'s 00z run from the portal, falling back to the mirror. 16 threads, as in K2.
3. Resolve the 199 fields. Hard-fail unless each is held exactly once.
4. Range-fetch, decode with `eccodes.codes_new_from_message`, and assert the grid keys and `dataDate`. Keep only the union of indices: about 10k floats per field, not 1.04 M.
5. Build daily per-point series (§5.2), then aggregate per footprint (§5.3) and compare to the normal (§6.2).
6. Run the backfill (§5.6).
7. Return the `runs`, `daily` and `summary` frames.

Record wall time, decode CPU, message count and bytes in the log, the way K2's `timing.json` did.

### 5.2 Per-point daily values

- **Rain:** `rain[d] = tp(24d) − tp(24(d−1))` in mm, clamped per §3.4.
- **Tmax/Tmin:** the max/min over windows ending in `(24(d−1), 24d]`. That is `mx2t3`/`mn2t3` for d ≤ 6, and `mx2t6`/`mn2t6` for d ≥ 7 [R1].
- **`vsw`:** the value at step `24(d−1)`, plus step 360 for day 15's end. Land points only. A land point with `vsw ≤ 0` hard-fails.

### 5.3 Footprint aggregation

| Output | Rule |
|---|---|
| Weighted mean (rain, Tmax, Tmin) | `Σ wᵢ xᵢ` over all nodes |
| Soil | `Σ wᵢ xᵢ / Σ wᵢ` over land nodes only. K2's "sea = 0.0" constraint |
| `wet_area_share[d]` | `Σ wᵢ [rainᵢ[d] ≥ 1 mm]` |
| `heavy_area_share[d]` | `Σ wᵢ [rainᵢ[d] ≥ 20 mm]` |
| `dry_run_area_share` | `Σ wᵢ [rainᵢ[1..10] all < 1 mm]`, i.e. ≥ `WEATHER_DRY_SPELL_ALERT_DAYS` dry days counted from day 1 [K2 §7.3] |
| Summary `heavy_area_share` | `Σ wᵢ [max_d rainᵢ[d] ≥ 20 mm]` |

Aggregation for 25 keys takes about 0.02 s per K2's measured rate.

### 5.4 Pin values (release 1)

The `pin:<key>` footprints aggregate like any other. Only `rain15_mm` and its tercile are kept, written into the matching footprint's `pin_rain15_mm` and `pin_rain15_tercile`. They are not separate summary rows.

### 5.5 Timing

The deploy cron is `0 19 * * 1-5` (`deploy-dashboard.yml`). Run D is public by about 07:34 UTC, so the 19:00 run is never early.

### 5.6 Backfill — the elapsed side's 120 days

The self-healing behaviour comes from [R4].

1. After today's run, find every date in `[today − FOOTPRINT_ELAPSED_DAYS, today − 1]` (`FOOTPRINT_ELAPSED_DAYS = 125`, which is 120 plus slack for weekends and holidays) with no `runs` row.
2. For each missing date, read the day-1 fields only:
   - `tp@24`;
   - `mx2t3` at steps 3–24 (8 messages);
   - `vsw@0`.

   That is **10 messages**, as R4 measured. Use the mirror first, then the portal for the newest three days. Fewer than all 10 → skip that date and log it.
3. Write a `backfill` runs row and 25 summary rows with only the `d1_*` and `vsw_now` columns.
4. The cap is `FOOTPRINT_BACKFILL_MAX_RUNS = 130` per run. A failure on one date never fails the layer (§3.5).

**Expected load:**
- **First deploy:** about 120 runs. R4 measured 31.7 s and 845 MB for 128 runs from a home connection.
- **Every Monday:** 2 runs (Saturday and Sunday, because the cron is weekdays only). About 13 MB. **This is load-bearing:** without it every weekend leaves two NULL days, and a NULL breaks a dry streak (`consecutive_dry_days`) and thins the deficit baseline.
- **After a failed deploy:** 1–2 runs.

---

## 6. The ERA5 normal

### 6.1 Build — `scripts/build_era5_normals.py`

This is a one-off reference build on a desk machine. **It is never a CI step, and `CDSAPI_KEY` is never a CI secret** [K2 §8].

| | |
|---|---|
| Dataset | `reanalysis-era5-single-levels`, hourly |
| Variables | `total_precipitation` and **`maximum_2m_temperature_since_previous_post_processing`** (`mx2t`) |
| Period | 1991-01-01 → 2021-01-15 (the extra 15 days let a window starting in late December close) |
| Requests | One bounding box per region group (US, South America, Europe, India, China, Southern Africa, West Africa), **split by decade**. The daily-statistics datasets refused 30-year requests ("cost limits exceeded"), and hourly 30-year requests were auto-split anyway [K2 §8]. Submit them serially: CDS rejects parallel requests per user per dataset [R4] |
| Daily value | Rain = sum of the hourly accumulations ending 01 … 24 UTC. Tmax = max of the hourly `mx2t` ending 01 … 24 UTC. The same UTC day as the forecast |
| Weights | The same `weights.csv` points and weights, including `pin:` footprints. ERA5 0.25° nodes coincide with the IFS 0.25° grid [K2 §4] |
| Output | `data/reference/footprints/era5_daily_1991_2020.csv.gz`. Columns `footprint, date, rain_mm, tmax_c`. 40 footprints (20 crop + 20 pin; ports need no normal) × 10,972 days ≈ 439k rows, gzip, a few MB [INF: not measured] |
| Hard-fails | A footprint node outside its request box; a day with fewer than 24 hours (partial last days have happened [R4]); a file that sniffs as a zip despite a `.nc` name (unzip it, never trust the extension [K2 §8]); a footprint with fewer than 10,972 days |
| Expected wall time | **Hours to days.** K2's two boxes took about 15 h overnight. Seven boxes × three decades × two variables, submitted serially, is a background job. Start it first (§14, slice 0) |

**Why `mx2t` and not hourly `2t`** [S1]: K2's normal was the max of *hourly samples*, while the forecast is the max of 3 h / 6 h window extremes. K2 estimated that left a positive bias of a few tenths of a degree on every Tmax anomaly (unmeasured), and said "the label must say so, or the normal must be built from ERA5 `mx2t` instead". Building from `mx2t` removes the bias rather than labelling it.

**No soil normal**, because there is no soil anomaly (§1).

### 6.2 Comparison — `pipeline.footprints.compare_to_normal`

Run D's window is valid dates `D … D+14` (15 UTC days).

| Quantity | Definition |
|---|---|
| Normal year *y* | The same 15 month-day dates in year *y*, 1991–2020. If D is 29 Feb, the window starts 28 Feb in non-leap years. A window crossing 29 Feb in a leap year uses the dates as they fall, so it still holds 15 days |
| `normal_rain15_mean` | Mean of the 30 window totals |
| `normal_rain15_lo`, `_hi` | 33⅓ and 66⅔ percentiles of the 30 totals (`numpy.percentile`, linear) |
| `rain15_tercile` | `lower` if forecast < lo; `upper` if forecast > hi; else `middle` |
| `rain15_rank` | 1 + the number of years with a total below the forecast (ties count as below), out of **31** (30 years plus the forecast); 1 = driest. This reproduces K2's "rank 6 of 31" for Iowa on 2026-10-05 |
| `normal_rain15_min`, `_max` | Over the 30 years. The tercile track (§8.4) spans `min(min, forecast)` to `max(max, forecast)` |
| `rain15_pct_vs_mean` | Secondary number only. Normals are skewed (mean − median = 6–8 mm in MT [K2 §4]), so % of mean overstates a wet anomaly. NULL when the mean is 0 |
| `normal_tmax_mean`, `_sd` | Mean and SD of the 30 window means of daily Tmax |

A footprint missing from the reference file is a hard-fail in the comparison. The validation in §4.1 should already have caught it.

**Acceptance check for slice 2.** On the 2026-10-05 run, the comparison must reproduce K2's §4 table within tolerance: MT soy +30 %, upper tercile, rank 23; IA soy −69 %, lower tercile, rank 6. The tolerance is ±1 rank and ±3 pts. K2 used hourly `2t` for Tmax, so its Tmax anomalies are not the acceptance figures; expect them a few tenths of a degree lower.

---

## 7. Alerts — the shared seam

### 7.1 One region reader — `pipeline.query.read_region_weather(region=None)`

This returns the frame every grading surface reads, in `read_weather()`'s shape plus three columns:

| Column | Footprint region | Layer 5 pin (palm only) |
|---|---|---|
| `region` | The footprint key | The pin name |
| `Date` | `run_date` of each summary row | As today |
| `precipitation`, `temp_max`, `temp_min` | `d1_rain_mm`, `d1_tmax_c`, `d1_tmin_c` | As today |
| `is_forecast` | `0` for rows with `run_date` < the latest daily run; `1` for the latest run's day 1 (valid today, not yet complete) | As today |
| `wet_area_share` | `d1_wet_area_share` | NULL |
| `basis` | `day-1 estimate · ECMWF 00z` | `observed` |

So the elapsed side ends at D−1, and D onward is forecast: there is no gap [R4]. Rows go back `LOOKBACK_DAYS + 5` days.

**Swap every grading call site** from `read_weather` to `read_region_weather`. As of `6a63996` there are eight:

| File | Line |
|---|---|
| `analysis/soy_analytics.py` | 1247 (Risk Monitor) |
| `analysis/soy_analytics.py` | 1443 (Emerging Markets) |
| `analysis/briefing/sections/weather.py` | 59 |
| `analysis/briefing/sections/market_drivers.py` | 84 |
| `analysis/briefing/snapshot.py` | 896 |
| `app/sections.py` | 263–270 (competing-oil strip) |
| `scripts/generate_players.py` | 418 |
| `app/block_builders.py` | `_weather_rows` (1816) |

`read_weather()` stays as Layer 5's raw reader for the layer's own tests and health checks. A test (§11) asserts that nothing outside `pipeline/` and the Layer 5 tests calls it.

### 7.2 Elapsed rules — `assess_region` gains a basis

`assess_region(region, rows)` stays the one implementation of the six rules (#355), with two changes:

1. **Basis.** If `rows` carries a `basis` column, it must hold exactly one value (hard-fail otherwise). That value becomes every `WeatherAlert.basis` the call produces. Absent, the basis is `OBSERVED` as today.
2. **Dry day on area share** [R4]. When `wet_area_share` is present and not NULL, a day is dry iff `wet_area_share < WEATHER_FOOTPRINT_WET_AREA_MIN` (0.5), instead of `precipitation < 1 mm`. This applies to both `consecutive_dry_days` and the "dry day" rule.

   On the footprint mean, MT's IFS drizzle shows 50 spell-days against ERA5T's 64. On area share, the two nearly agree: 114 vs 118 [R4]. Heavy rain, the deficit and heat stay on the weighted mean.

Thresholds are unchanged. The deficit rule now has ≥ 120 days on the first deploy, so the "not assessed" lines #355 introduced disappear for footprint regions. They stay for the palm pins, which still have about 31 days.

### 7.3 Forecast rules — `assess_footprint_forecast(footprint, summary_row, today)`

This is a new function in `analysis/weather_alerts.py`. It returns `(alerts, withheld)` using the same `WeatherAlert` and `Withheld` types, `basis = "forecast · ECMWF 00z {run date}"`, with severity **capped at `warning`** [K2 §7.3, P1 #5]. The cap is enforced in code: `__post_init__` raises if a forecast-basis alert carries `alert`. The #355 docstring only states the cap; this makes it binding.

New rule ids, added to `RULE_LABELS`:

| Rule id | Label | Fires when | Severity | Text |
|---|---|---|---|---|
| `fc_dry_spell` | Dry spell ahead | `dry_run_area_share ≥ WEATHER_FC_DRY_AREA_SHARE` (0.5) and the run month is in `WEATHER_GROWING_SEASON_MONTHS[key]` | warning | `dry ≥ 10 days ahead on 57% of soy area` |
| `fc_rain_deficit` | 15-day rain deficit | `rain15_mm ≤ (1 − WEATHER_PRECIP_DEFICIT_ALERT_PCT/100) × normal_rain15_mean` (≤ 60 %) | warning | `15-day rain 10.6 mm, 31% of normal (≤ 60%)` |
| `fc_heavy_rain` | Heavy rain ahead | `heavy_area_share ≥ WEATHER_FC_HEAVY_AREA_SHARE` (0.25) | **info** | `a day ≥ 20 mm forecast on 31% of soy area` |
| `fc_pod_fill_heat` | Pod-fill heat ahead | `heat34_days ≥ WEATHER_FC_POD_FILL_MIN_DAYS` (3) | warning | `5 days ≥ 34 °C forecast in pod fill` |
| `fc_heat` | Extreme heat ahead | `tmax_peak_c ≥ WEATHER_EXTREME_HEAT_C` and the run month is in season | warning | `peak 38.4 °C forecast` |

The texts use the prototype's wording [P1 `alertsFor()`]. "Soy area" is replaced by the footprint's crop word (§4.1).

**Withheld, with a reason and never silent:**
- `fc_rain_deficit` when the normal mean is 0: "the 1991–2020 normal for these days is 0 mm".
- `fc_pod_fill_heat` **out of its pod-fill months, only when `heat34_days_all ≥ 3`**: "pod-fill heat not assessed: October is planting in Mato Grosso (7 forecast days ≥ 34 °C)". This is the prototype's caption. When the bar would not have been crossed anyway, nothing prints.
- Every rule when the footprint state is `failed` (§3.6).

**Ports get no alerts.** Port rain is a caption [P1 #7].

**No precedence chain.** These five are independent. A wet half and a dry half can both be true of one footprint.

Constants, added to `config.py` beside the existing `WEATHER_*` block:

```python
WEATHER_FOOTPRINT_WET_AREA_MIN = 0.5   # elapsed dry day: < 50 % of crop area got ≥ 1 mm (R4)
WEATHER_FC_DRY_AREA_SHARE = 0.5        # K2 §7.3
WEATHER_FC_HEAVY_AREA_SHARE = 0.25     # K2 §7.3, not tuned
WEATHER_FC_POD_FILL_MIN_DAYS = 3       # K2 §7.3
FOOTPRINT_FORECAST_MAX_AGE_DAYS = 3
FOOTPRINT_ELAPSED_DAYS = 125
FOOTPRINT_BACKFILL_MAX_RUNS = 130
FOOTPRINT_WEIGHTS_MAX_AGE_DAYS = 548
```

**Measured behaviour to reproduce in tests** [K2 §7.3]:
- `fc_dry_spell` fires for Iowa on the 10-03 run only (IA area shares 35 / 56 / 25 / 13 % on 10-02…10-05).
- `fc_rain_deficit` fires for Iowa on 10-05 (31 % of normal) and never for MT (130 %).
- MT's pod-fill gate holds in October.

---

## 8. Rendering

Check every visual change against `DESIGN.md`. [P1] approved three new `DESIGN.md` rows, all on existing palette tokens with no new colours. Rows 1 and 2 belong to this build (§8.6).

### 8.1 Block 06 — `weather_block` (`app/block_builders.py:1652`)

For each region in `market.weather_regions`:
- **It has a footprint:** build a card from its latest summary row (§8.3), its forecast alerts, and `assess_region` over `read_region_weather(region)` for the elapsed alerts.
- **It does not (impossible on market pages after slice 3, but the code stays general):** today's pin card.

The block stays `ok` when it has cards, **or** a river reading, **or** a storm row (S2 §8.3). Otherwise it is `STATE_EMPTY` with its reason.

The subtitle changes from "this market's growing regions" to **"this market's crop areas, next 15 days"**. The prototype said "soy areas", but Europe's areas are rapeseed [S1].

### 8.2 Stamp line

One line above the card grid, using the existing `caption` style:

```
forecast · ECMWF 00z 5 Oct · valid 5–19 Oct · vs ERA5 1991–2020 · crop-weighted state (SPAM 2020)
```

When any card on the page is `stale`, append ` · stale: run of 4 Oct`. Dates are absolute, never "today" [S2 §8.1].

### 8.3 The card (variant A) [P1 #1, #8]

```jinja
<div class="mc">
  <div class="mc-label">{{ c.label }} · {{ c.crop_word }}{% if c.season_note %} <span class="kind">off-season</span>{% endif %}</div>
  <div class="mc-val">{{ f.num(c.rain15_mm, 1) }}<small class="muted"> mm · 15-day rain</small></div>
  <div class="mc-delta">{{ tercile_track(c) }}{{ c.tercile }} tercile · rank {{ c.rank }} of 31 · <span class="{{ 'down' if c.pct < 0 else 'up' }}">{{ f.signed(c.pct, 0, '%') }}</span> vs mean</div>
  <div class="caption">Tmax {{ f.signed(c.tmax_anom_c, 1, ' °C') }} vs normal · {{ c.heat34_days_all }} days ≥ 34 °C · soil {{ f.num(c.vsw_now, 2) }} → {{ f.num(c.vsw_day15, 2) }} m³/m³</div>
  <div class="caption">last 30 days {{ f.num(c.elapsed30_mm, 0) }} mm · day-1 estimate through {{ c.elapsed_through }}</div>
  {% if c.pin_line %}<div class="caption">{{ c.pin_line }}</div>{% endif %}
  <div class="caption" style="margin-bottom:2px;">{{ c.role }}{% if c.season_note %} · {{ c.season_note }}{% endif %}</div>
</div>
```

**Order** [P1 #8]: tercile and rank lead; the % of mean comes after. The mock led with % and the build flips it.

**The elapsed line replaces the prototype's "pin observed: 0.0 mm · 20.5 °C max · 5 Oct"** [S1]. P1 approved that caption while the observed side still sat on Open-Meteo ("stays until R4"). R4 moved the observed side to the footprint's own day-1 estimate, so the caption shows the footprint's trailing 30 days. It is labelled as an estimate, never "observed". `elapsed30_mm` is NULL (and renders as a dash) when fewer than 30 elapsed days are held, which happens only before the first backfill.

**The pin line, release 1 only** [P1 #1, "from C"]: `the single point we used to show says {pin_rain15_mm} mm, {pin_tercile} tercile`. It is rendered only while `FOOTPRINT_SHOW_PIN_LINE = True`, and slice 6 sets it `False`. The value is ECMWF at the old pin's nearest node, not Open-Meteo. So the line compares *area* with *point* on one model, which is what K2 showed matters ("the gain is from area, not model" [K2 §7.2]), and it needs no Open-Meteo call [S1].

**`stale` card:** a `stale` tag (`.kind`) after the label. **`failed` card:** `<div class="empty-state state-empty"><span class="es-label">no reading</span>{{ label }}: {{ reason }}</div>` in the card's slot.

### 8.4 The tercile track

This is a sibling of the ledger range track (`DESIGN.md:103`): an inline 78 px SVG rendered server-side, so it survives script being off.
- The **span** is `min(normal_rain15_min, forecast)` → `max(normal_rain15_max, forecast)`.
- The **middle band** is `lo → hi`, in the neutral fill `#E4E8E4`.
- The **marker** is a tick at the forecast, coloured with the existing `--bearish` for the lower tercile, `--bullish` for upper, and muted for middle.
- The `title` is `31-year range {min}–{max} mm; middle band = middle tercile; vs ERA5 1991–2020`.

It is a server-side macro, `tercile_track(c)`, in `blocks/_macros.html.j2`.

### 8.5 Alert lines

Under the grid, in this order:

1. **Elapsed alerts:** `alert alert-warn`, as today, with the text suffixed ` (day-1 estimate · ECMWF 00z, through {date})`.
2. **Forecast alerts:** `<div class="alert alert-warn fc"><span class="kind">forecast</span> {{ label }} {{ crop_word }}: {{ text }}</div>`. `fc_heavy_rain` uses `alert-info`.
3. **The explicit all-clear:** `alert alert-ok` "No threshold breach in N regions", rendered only when neither 1 nor 2 produced a line (unchanged behaviour).
4. **Withheld captions** (#355's existing `group_withheld` lines) for both classes. Forecast withholds are grouped the same way.

In the **Risk Monitor** (`risk_analysis`, `soy_analytics.py:1197`), add `weather_forecast_alerts` beside `weather_alerts`. These are the forecast alerts for `SOY_WEATHER_REGIONS` footprints, each with `run_date`. In `risk_monitor.html.j2`, render them under the same "Weather Alerts" subheader after the elapsed rows, in the class from item 2, with ` (ECMWF 00z {run date})` [P1 #2]. Nothing goes on the headline, Emerging Markets or the competing-oil strip [P1 #2]. Those surfaces keep their layouts and simply read the region reader (§7.1).

### 8.6 The two `DESIGN.md` rows, plus the log entry

Add under "Component Patterns". The text below is what the build commits, expanding [P1]'s one-line approvals (quoted in S2 §8.5).

> **### Tercile track (block 06 footprint cards)**
> A sibling of the ledger's range track: a 78px inline SVG, server-rendered, spanning the lowest to highest 15-day total among the thirty ERA5 1991–2020 windows and the forecast. The middle tercile is a neutral band (`#E4E8E4`); the forecast is a tick in `--bearish` (lower tercile), `--bullish` (upper) or muted (middle). Its words are always beside it — "lower tercile · rank 6 of 31" — so the track is never the only carrier of the reading. The % of mean follows, never leads: the normals are skewed.

> **### Forecast alert line (block 06, Risk Monitor)**
> The existing alert line with a dashed left border and a leading `forecast` kind chip (`.kind`), so a forecast can never be read as a reading. Capped at the warning colour; heavy rain ahead is `alert-info`. Elapsed-side alerts keep the solid border and say "day-1 estimate · ECMWF 00z". Never "observed".

CSS goes in `_base.html.j2` beside `.alert-*`: `.alert.fc { border-left-style: dashed; }`, plus the track's few rules. No new colour tokens.

Add to the Decisions Log (the build fills in its own PR number):

> | 2026-10-06 | **Footprint weather replaces the pin forecast in block 06** | [P1 #362](https://github.com/philipbergman6-glitch/Mirror-Market/issues/362), spec [S1 #373](https://github.com/philipbergman6-glitch/Mirror-Market/issues/373). Variant A of three prototyped: one card per crop-weighted state footprint, 15-day rain with tercile + rank vs ERA5 1991–2020 leading and % of mean second, Tmax anomaly, heat days, soil now → day 15. A single pin missed its own state's 15-day rain by up to −98 %/+55 % and flipped the tercile in both Mato Grosso and Iowa (K2 #361). The elapsed side is the footprint's own persisted ECMWF day-1 estimate, labelled as such (R4 #369). Forecast alerts carry a dashed border and a `forecast` chip, capped at warning. No new colours. |

### 8.7 Attribution caption [P1 #10, R1 §7, R3]

One caption at the foot of block 06, on every page that shows a footprint card:

```
Forecast: based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF), www.ecmwf.int, CC BY 4.0 (creativecommons.org/licenses/by/4.0); area-averaged over crop-weighted state outlines and converted to mm and °C by Mirror-Market. ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use. Normal: contains modified Copernicus Climate Change Service information (ERA5, 1991–2020); neither the European Commission nor ECMWF is responsible for any use that may be made of the Copernicus information or data it contains. Crop weights: SPAM 2020 V2r2, provided by the International Food Policy Research Institute (IFPRI); IFPRI bears no responsibility for the analyses or interpretations presented here.
```

This is longer than the prototype's caption, deliberately [S1]. The ECMWF Terms of Use require the statement, the source link, the licence link, the disclaimer and a modification note on a *service* [R1 §7]. The C3S template carries its own disclaimer [R3], and SPAM adaptations "must state" IFPRI's [R3]. The prototype's short form drops all three. Natural Earth is public domain and needs no credit; it goes in `LAYERS.md` only.

### 8.8 Port rain — filling S2's slot

Add `SiteContext.port_rain(place_id)` (`app/block_builders.py`). It returns `{"tp15_mm": int, "wet_days": int, "stamp": "ECMWF 00z 5 Oct"}` from the port's latest `daily` summary row, or `None` when there is none or the row is older than `FOOTPRINT_FORECAST_MAX_AGE_DAYS`. That is exactly the contract S2 §8.3 defines. If S2 has not landed, nothing calls it, and the slice adds only the method and its test.

---

## 9. Layer 5 after this build

| Change | Why |
|---|---|
| Remove `Canada Saskatchewan (Saskatoon)` and `Canada Alberta (central)` from `GROWING_REGIONS`, every `WEATHER_*` calendar, and `COMPETING_OIL_WEATHER_BELTS` (the "Canola prairies" belt) | [P1 #9a]: linking them to CZCE rapeseed oil is an inference, not a leg; the ICE canola leg is dead (K1 §4.1) |
| Restrict `fetch_all_regions()` to `GROWING_REGIONS` keys **not** in `FOOTPRINTS`: the two palm pins | The standing rule (M14 #207, `config.py:367-376`): nothing is fetched without a reader downstream. After §7.1, no surface reads the 20 footprint regions' pin rows |
| `LAYER_MIN_KEYS["weather"] = 2` ("of 2"); `LAYER_KEY_CATALOGS["weather"]` = the palm pins | Grading follows the catalog |
| Rewrite the `PRODUCTION_LAYERS` description to "2 palm pins (competing-oil strip)" | `config.py:132` |

**Licence:** [#366](https://github.com/philipbergman6-glitch/Mirror-Market/issues/366) (Open-Meteo free tier is non-commercial) shrinks from 24 pins to 2 but does not close. It stays outside this map.

**Order:** Layer 5 shrinks in the same slice that switches the readers (slice 3), never before. A pin is dropped only once nothing reads it.

---

## 10. Briefing wording

The briefing's weather section (`analysis/briefing/sections/weather.py`, 22nd in `_SECTION_ORDER`) keeps its header and gains a sub-block. S2's `STORMS` section follows immediately after it (S2 §9).

```
WEATHER ALERTS:
  US Midwest (Iowa): Dry spell — 11 consecutive days <1mm — soil moisture depleting [+0.4σ vs 90d] (day-1 estimate · ECMWF 00z, through 4 Oct)
  Indonesia Riau (Sumatra): Heavy rain (34mm) — harvest delays possible [+2.1σ vs 90d]
  30d precip deficit not assessed (2 regions): 31 of 45 observed days needed in the 90-day baseline before the 30-day window
WEATHER OUTLOOK (forecast · ECMWF 00z 5 Oct · valid 5–19 Oct · vs ERA5 1991–2020):
  Iowa soy area: 10.6 mm — lower tercile, rank 6 of 31 (−69% vs mean) · Tmax +5.1 °C
    ! 15-day rain deficit — 15-day rain 10.6 mm, 31% of normal (≤ 60%)
  Mato Grosso soy area: 74.1 mm — upper tercile, rank 23 of 31 (+30% vs mean) · Tmax +0.4 °C
    pod-fill heat not assessed: October is planting in Mato Grosso (7 forecast days ≥ 34 °C)
  Near normal: Illinois, Nebraska, Paraná, Rio Grande do Sul, … (12 more)
```

**Rules:**
1. `WEATHER ALERTS` is unchanged except that each footprint-region line ends with its basis in brackets. Palm pin lines carry no suffix (their basis is `observed`, as today).
2. `WEATHER OUTLOOK` lists, in `SOY_WEATHER_REGIONS` order followed by the non-soy footprints:
   - one line per footprint with a forecast alert, or in the lower or upper tercile;
   - an indented `!` line per forecast alert;
   - an indented withheld line per forecast withhold.
3. `Near normal:` names the remaining middle-tercile footprints by label, the first five then a count, mirroring the weather section's `_NAMED_REGIONS` habit.
4. A `stale` footprint's line ends with ` (run of 4 Oct — stale)`. A `failed` footprint prints `NOT ASSESSED: {label} — {reason}` (S2's upper-case convention for statements about us).
5. With no summary rows: `WEATHER OUTLOOK: No data`.
6. Forecast alerts are **not** added to the `signals` section, and the market-drivers "weather premium" line keeps reading elapsed alerts only. A forecast is not a reading.

The example shows the format, not one real day. The outlook numbers are K2's 2026-10-05 run [K2 §4]; the alert lines are illustrative.

---

## 11. Latency — `latency/domain.py`

```python
LayerLatency(
    "footprint_weather", LatencyClass.WEATHER, ObservationClock(), timedelta(hours=0),
    "The elapsed series is each 00z run's day-1 estimate, so the estimate for "
    "day D exists (run public ~07:34 UTC on D) before D ends; the stored "
    "observation is the completed UTC day, read once at the 19:00 UTC run, "
    "so its age at read is ~19 h — all cadence_wait, none provider delay.",
),
```

- About **19 h exceeds the Weather class's 12 h acquisition objective** [R4]. The breach is structural: a daily run, reading a completed day.
- Add one line to `LATENCY.md` under the Weather class stating the basis above. The river layers' "completed day" is the precedent for the reasoning, although they declare 24 h of provider delay instead.
- Do **not** count the in-progress day D as observed to make the number look smaller. It is forecast.
- Do not move the run earlier for this: the 19:00 cron's time is chosen for the settlement window (`deploy-dashboard.yml:8`), not for weather.

---

## 12. Tests

Every test is offline. `tests/_guards.py` blocks sockets and writes under `data/history/`. Point `pipeline.history.HISTORY_DIR` at `tmp_path` where history is exercised.

### 12.1 Fixtures — `tests/fixtures/footprints/`

- Two real GRIB messages (`tp@24`, `mx2t3@3`), cut by Range from the 2026-10-05 run and **sub-gridded**: the decoder test reads them whole, the aggregator test uses a 10-node weights file. Keep the fixture under 2 MB. If two global messages exceed that, generate tiny synthetic GRIBs with `eccodes` from a template instead.
- `index_20261005_step24.jsonl`: one real index file.
- `weights_small.csv`: two footprints, 10 nodes, one sea node.
- `era5_small.csv.gz`: those two footprints, 30 years × the 2026-10-05 window, reproducing the §6.2 acceptance numbers.
- `summary_k2_runs.json`: K2's four runs' footprint summaries, for the §7.3 behaviour tests.

### 12.2 Test list

| File | Covers |
|---|---|
| `tests/test_footprints_math.py` | Index↔lat/lon round trip at the poles and the antimeridian; daily rain from accumulations, including the −0.05 clamp and the hard-fail below it; the 3 h/6 h Tmax split at 144 h; land-only soil; share computations; `dry_run_area_share` |
| `tests/test_footprints_normal.py` | Window dates, including 29 Feb; terciles, rank of 31 (ties), track span; the K2 §6.2 acceptance numbers from the small fixture |
| `tests/test_fetcher_footprint_weather.py` | Index resolution (missing or duplicate field → fail); Range `206` length check; grid-key and `dataDate` assertions; mirror fallback; all-or-nothing grading; backfill selects only missing dates, never overwrites `daily`, and caps at 130; a backfill failure leaves the layer `success` |
| `tests/test_clean_footprint.py`, `tests/test_store_footprint.py` | §3.3 and §3.4 rules, the window-replace of `daily`, history round trip of the two tables |
| `tests/test_weather_alert_parity.py` (extend) | `assess_region` with a `basis` column (single-valued, else fail); area-share dry day; forecast rules reproduce K2's four runs (§7.3); the cap raises; withheld captions |
| `tests/test_region_reader.py` (new) | `read_region_weather` shape; `is_forecast` on the latest run's day 1; the palm pins come from Layer 5; **no module outside `pipeline/` and the Layer 5 tests imports `read_weather`** |
| `tests/test_weather_mapping.py` (extend) | Every market `weather_regions` entry is a footprint or a Layer 5 pin; every footprint has weights and a normal; canola is gone; `FOOTPRINTS` keys ⊂ `GROWING_REGIONS` |
| `tests/test_market_blocks.py` (extend) | Block 06 renders variant A from a seeded DB; tercile/rank lead the % line; `stale` and `failed` cards; the stamp; the attribution caption; Europe says "rapeseed area"; the pin line disappears when the flag is off |
| `tests/test_briefing.py` (extend) | `WEATHER OUTLOOK` format and rules 1–6 |
| `tests/test_latency.py`, `tests/test_docs_claims.py`, `tests/test_layer_coverage.py` | The new layer is registered everywhere a layer must be |

---

## 13. Traps

For the `LAYERS.md` entry. Each one burned the spike or the research.

1. **`tp` is accumulated from run start.** Daily = difference, and differences go slightly negative from 16-bit packing. Clamp to −0.05 mm, fail below that [K2 §8].
2. **The 3 h / 6 h Tmax boundary is at 144 h.** `mx2t3` exists to 144 and `mx2t6` from 150. Mixing them inside a day double-counts [R1].
3. **`vsw` is 0.0 at sea.** Average soil over land nodes only; a coastal footprint otherwise reads artificially dry [K2].
4. **The portal keeps about 3½ days; the mirror promises nothing.** Persist our own numbers; never plan on re-reading old runs from the portal [R1 §3].
5. **CDS refuses large and parallel requests.** "Cost limits exceeded" for 30 years at once; "Number queued requests … temporarily limited" for parallel jobs. Split by decade, submit serially, expect hours [K2 §8, R4].
6. **CDS serves a zip named `.nc`.** It holds two NetCDFs (`stepType-instant`, `stepType-accum`) even with `download_format: unarchived`. Sniff the format [K2 §8].
7. **A partial last day from CDS** (18 of 24 hours on 2026-10-01) must never be summed [R4].
8. **An IFS cycle upgrade steps the series**, soil most of all. 50r1 landed 2026-05-13 inside R4's window. Record `model_cycle` per run. Do not "correct" a step; note it in `LAYERS.md` when one lands [R4].
9. **Weekend gaps.** The deploy cron is weekdays only, so Saturday and Sunday day-1 rows exist only through the Monday backfill. A NULL breaks a dry streak (§5.6).
10. **Natural Earth's `fips` is not unique** (Aube and Marne are both `FRA4`), **Grand Est is not one row**, and `AR-B` is typed like CABA. Join on `iso_3166_2`, verify on `wikidataid` [R3].
11. **The pin sits on wetter-than-typical soil in MT in every run** (+0.04 to +0.10 m³/m³ vs the footprint) [K2 §2]. A point's soil level is mostly texture. That is why the card shows the footprint's soil and no pin soil.
12. **`timezone=auto` days are not UTC days.** Open-Meteo's local-time day 1 is part-observed [K2 §3]. The footprint is UTC throughout; never compare day 1 across the two.

---

## 14. Build slices, in order

Each slice is one PR that leaves `main` green. Nothing renders differently until slice 4. Never edit `data/history/` in a PR; the deploy workflow writes it.

| # | Slice | Contains | Done when |
|---|---|---|---|
| 0 | **Reference builds** (desk machine, not a PR until the files exist) | `scripts/build_footprints.py`, `scripts/build_era5_normals.py`, `requirements-reference.txt`, `crosswalk.csv`, then the generated `weights.csv`, `manifest.json`, `era5_daily_1991_2020.csv.gz`, `README.md` | Both builds pass their hard-fails. The ERA5 build takes hours to days, so **start it first**. The weights build takes seconds |
| 1 | **Layer** | `fetchers/footprint_weather.py`, `pipeline/footprints.py`, `units.py` conversions, schema, clean, store, query (not the region reader yet), history entries, `main.py`, latency entry, `eccodes` pin, fixtures and tests, `LAYERS.md`, layer counts | Tests pass. The first deploy run after merge shows the layer `success`, 25 summary rows and about 120 backfill runs in the history CSVs. **Measure the runner's fetch time and bandwidth to `data.ecmwf.int` and the mirror**, both unmeasured until now |
| 2 | **Normal comparison** | `compare_to_normal`, summary normal columns populated | The §6.2 acceptance check reproduces K2 on the 2026-10-05 fixture |
| 3 | **Alerts + reader switch** | `read_region_weather`, the eight call-site swaps, `assess_region` basis + area-share dry day, `assess_footprint_forecast`, new rule ids and constants, **Layer 5 shrink and canola removal** (§9) | Parity tests pass. The briefing prints footprint-basis elapsed alerts. The "30d precip deficit not assessed" line is gone for footprint regions on the next deploy |
| 4 | **Site** | Block 06 variant A, stamp, tercile track macro, forecast alert lines, Risk Monitor forecast rows, attribution caption, `ctx.port_rain`, CSS, the two `DESIGN.md` rows and log entry, `ARCHITECTURE.md` | Render tests pass; `scripts/generate_site.py --only cbot`, `--only europe` and `--only brazil` reviewed against `DESIGN.md`; `scripts/smoke_site.py` passes |
| 5 | **Briefing** | The `WEATHER OUTLOOK` sub-block | `python -m analysis.briefing` prints it. May run in parallel with 4 |
| 6 | **Release 2 cleanup** | `FOOTPRINT_SHOW_PIN_LINE = False`; drop the `pin:` footprints from the weights and normal files and the two `pin_*` columns' writers (columns stay NULL in history) | One release after slice 4 ships [P1 #1, "for the first release only"] |

**Port rain** lights up in S2's storm rows once both slice 4 here and S2's slice 4 are merged, in either order.

---

## 15. Decisions made in this spec

Each was left open upstream or forced by the code. All are reversible in the place named.

1. **Footprints are keyed by the existing region names**, not K1's `G-*` ids, so 6+ config dicts and every `weather_regions` list keep working. `place_id` rides along. *Reverse by* a rename sweep.
2. **One region reader, and every grading surface switches to it** (§7.1). [P1 #2] kept new *rendering* off the headline, Emerging Markets and the strip. If those surfaces kept grading Open-Meteo pins while block 06 graded footprints, the same region would get two answers again, which is #355's defect. They keep their layouts and read the same series. *Reverse by* pointing a call site back at `read_weather`, which would reopen #355.
3. **Layer 5 shrinks to the two palm pins** once nothing reads the other 20 (§9). This follows from 2 plus the standing rule. *Reverse by* adding palm footprints (then Layer 5 has no reader left) or keeping pins as a fallback (forbidden: never fill from another source [R4]).
4. **No official-statistics weights in v1**, only SPAM inside each footprint. R3 recommended official 5-year means *between* regions; K2 limited that to a single-belt-number case [K2 §7.1], and P1 chose one card per footprint. *Reverse by* adding a belt line and the NASS / IBGE / MAGyP builders.
5. **The ERA5 normal is a daily per-footprint series, not R3's day-of-year mean.** Terciles, rank of 31 and the track's min–max need the 30 window totals; a mean cannot give them. It is also smaller than R3's estimate because it is footprint-reduced at build time.
6. **The ERA5 Tmax normal is built from `mx2t`, not hourly `2t`**, removing the bias K2 flagged rather than labelling it (§6.1).
7. **The pin line uses ECMWF at the pin's node, not Open-Meteo** (§8.3). Same model, so the line isolates area vs point, and no Open-Meteo call survives for those regions.
8. **The card's observed caption becomes the footprint's 30-day day-1 estimate**, replacing P1's "pin observed" caption, because R4 resolved after P1 and moved the observed side (§8.3).
9. **Weights age is reported, never a failing state or a failing test** (§4.4). R3 said "flag `stale`"; with static SPAM 2020 inputs a rebuild changes nothing, and a dated test is how the September deploy outage happened.
10. **All-or-nothing layer grading**; the backfill never fails the layer (§3.5).
11. **Five port boxes**, the five ports S2's block 06 lists; no box for inland pricing points (§4.2).
12. **Forecast rules have no precedence chain** and are season-gated exactly as K2's table says (dry spell and extreme heat by growing season, pod-fill by pod-fill months, deficit and heavy rain ungated).
13. **The forecast cap is enforced in code**, not only in the docstring (§7.3).
14. **The block subtitle says "crop areas"**, not the prototype's "soy areas", because Europe and the Argentine sunflower footprint are not soy.
15. **The attribution caption is the full required wording**, longer than the prototype's (§8.7).
16. **The Monday weekend backfill is part of the design**, not an edge case (§5.6). R4's "normal day: 0 runs" assumed a 7-day cron.

## 16. Open after this spec

Nothing blocks the build. These are deliberately left for evidence or a human read:

- **The ECMWF service-agreement clause:** one human read of the PDF before go-live [R1 §7]. It does not block the build; it blocks publishing the first deploy that renders slice 4.
- **Runner bandwidth to `data.ecmwf.int` and the mirror:** measured by slice 1's first deploy. If the 156 MB-class fetch is slow on the runner, switch the primary host to the mirror (one constant).
- **A positive elapsed-side alert case:** none occurred in R4's window. The first real one should be checked against CPC by hand once.
- **Footprints beyond Iowa and Mato Grosso:** only those two were measured. Slice 0's manifest gives node counts and concentration for all 20; look for a footprint under about 20 nodes (likely G-DE or G-RO) and say so in `LAYERS.md`.
- **IFS drizzle bias in dry regimes:** MT Jun–Aug ECMWF 30.5 mm vs CPC 20.8 mm [R4], one season and two footprints. The area-share dry rule absorbs most of it; revisit after one MT dry season.

## Attribution

Forecasts: ECMWF Open Data (IFS), CC BY 4.0 and the ECMWF Terms of Use, "based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF)". Normal: Copernicus Climate Change Service, ERA5 (CC BY 4.0). Crop weights: SPAM 2020 V2r2 (IFPRI, CC BY 4.0). Boundaries: Natural Earth (public domain). Measurements quoted here are K2's and R4's.
