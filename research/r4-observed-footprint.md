# R4 · Observed side of footprint weather (#369)

Map: #356. Measured 2026-10-06, 17:50–18:20 UTC, home broadband. Throwaway code and raw
numbers: `spike/r4-observed/` on this branch (`research/r4-observed-footprint`, never merged).

## Success criteria (set before measuring)

Per candidate × two footprints (Iowa `US-IA`, Mato Grosso `BR-MT`): licence quote, measured lag,
CI cost, agreement with the ECMWF forecast footprint. Final answer names a source per variable
(rain, Tmax, soil) with its lag, how the gap renders, how ≥120 days are kept, and the table shape.
All met; the one thing not exercised is a *positive* deficit alert (none fired in the window).

## Recommendation

| Variable | Source | Lag (newest complete UTC day) |
|---|---|---|
| Rain | **Persisted ECMWF IFS 00z day 1** (0–24 h `tp`) | **D−1** (measured: 10-05 held on 10-06) |
| Tmax | **Persisted ECMWF IFS 00z day 1** (`mx2t3`, steps 3–24) | **D−1** |
| Soil | **Persisted IFS `vsw` layer 1 at step 0** of each run; no observed-soil source, no soil alert (K2 rule stands) | D (the 00z analysis) |

- **It is a model estimate, not an observation, and must be labelled so**: "day-1 estimate · ECMWF 00z".
  The alert `basis` should say the same rather than `observed`. This is the one judgment call in this
  ticket — see "Why not a real observation" below.
- **No 0–5-day gap exists.** The elapsed side ends at D−1; day D is day 1 of the current forecast.
- **≥120 days:** `data/history/` rows (one per footprint per run, the summary row P1 already approved,
  carrying that run's day-1 values) plus a self-healing backfill of missing days from the AWS mirror.
- **One footprint table** for both halves.
- ERA5T, CHIRPS and CPC are not adopted. CPC rain is the only one worth keeping in mind, as an
  independent gauge cross-check — not built in v1.

## Method

- Footprints: Natural Earth admin-1, SPAM 2020 soy production weights (`spam2020_V2r2_global_P_SOYB_A.tif`
  from the `~/Downloads` zip), K2's `footprints.py`. Each 5′ SPAM cell goes to the nearest node of the
  target grid (`common.grid_weights`); a NaN node drops out and its weight share is reported.
  **Weight lost to NaN/nodata was 0.0 for every source and both footprints.**
- Nodes per footprint: ECMWF/ERA5 0.25° 274 (IA) / 1,278 (MT, K2); CPC 0.5° 73 / 339; CHIRPS 0.05° and
  ERA5-Land 0.1° sample finer than SPAM.
- Window: 2026-06-01 → 2026-09-30 (122 days) common to all; ECMWF day 1 to 10-06.
- "Agreement with the forecast footprint" = agreement with the ECMWF day-1 footprint, i.e. the
  first day of the same product the forecast card shows.
- Day labels are each product's own: ECMWF/ERA5 00–24 UTC; CPC rain ends ~12Z (varies by country);
  CPC Tmax 06Z–06Z; CHIRPS "a consistent date label … rather than … a uniform observation window".
  Daily scores for CPC/CHIRPS are therefore pessimistic; 5-day and 30-day scores are the fair ones.

## Measured — lag (2026-10-06 ~18:00 UTC)

| Source | Newest data held | Age of newest complete day | Evidence |
|---|---|---|---|
| ECMWF day 1 | run 10-06 00z (covers 10-06); complete day 10-05 | **1 d** | AWS `Last-Modified` 07:34 UTC same day; 128/128 runs present |
| ERA5T (CDS) | 2026-10-01 17:00 UTC | **6 d** (09-30) | request at 17:54 UTC returned 18 hours of 10-01, `expver 0005` |
| ERA5-Land-T `swvl1` | 2026-10-01 00:00 UTC | 5 d | `expver 0001` Jun–Jul, `0005` Aug–Oct |
| ARCO-ERA5 (Google) | `valid_time_stop_era5t: 2026-09-30`, updated 10-06 03:24 | 6 d | `.zattrs` read only, **not test-pulled** |
| CPC rain | PSL netCDF 10-04; raw FTP 10-05 (posted 10-06 17:26) | **1–2 d** | files re-stamped 2 days later (10-03 rewritten 10-05 21:51) |
| CPC Tmax | PSL netCDF 10-05 | 1 d | |
| CHIRPS v3 prelim (`sat`) | 09-30; 10-01…10-05 → HTTP 404 | **6 d** (cycle 2–7 d, pentad batches) | |
| CHIRPS v3 final | 08-31 | 36 d | September final absent on 10-06 |

Docs agree: ERA5T "the D-5 data are typically available by 12UTC" (ERA5 documentation);
CHIRPS prelim "two days after the end of a pentad", final "third week of the following month" (README).

## Measured — CI cost

| Source | What ran | Result |
|---|---|---|
| ECMWF day 1, daily | nothing extra: `tp@24h`, `mx2t3@3–24h`, `vsw@0h` are already in K2's forecast fetch | **0 extra bytes** |
| ECMWF day 1, backfill | 128 runs × 10 messages, byte-range from the AWS mirror, 12 threads | **31.7 s, 845 MB**, 0 missing; size is independent of footprint count |
| ERA5T (CDS) | 6 requests at once | **all rejected in 12–29 s**: "Number queued requests for this dataset is temporarily limited" |
| ERA5T (CDS) | serial: 10 monthly box requests (hourly `tp` + `mx2t`), 1 ten-day increment, 2 ERA5-Land | each **36–269 s** (median ~117 s); both boxes × 122 days took **27 min** wall |
| ERA5 daily-statistics (CDS) | 1 box, 30 days, daily sum | **still queued after 29 min**, then abandoned |
| CDS state | live dashboard 17:47 UTC | 592 running, 8,357 queued, 2,558 users queued |
| CHIRPS v3 | 214 windowed `/vsicurl` reads (striped LZW GeoTIFF), 8 threads | 112 s, median 3.7 s/file |
| CPC (PSL files) | `precip.2026.nc` 47 MB + `tmax.2026.nc` 61 MB | 77 s + 54 s |
| CPC (PSL NCSS subset) | one box, 134 days, one variable | 75–325 KB in **10–17 s** |

CDS also needs a secret in CI, the `cdsapi` dependency, the zip-named-`.nc` trap (hit again here), and
K2 saw connection errors and 3–6 h decade jobs on the same service. GitHub-runner speeds are unmeasured.

## Measured — agreement with the ECMWF day-1 footprint

### Rain (122 common days; CPC 126)

| | Iowa r daily | Iowa r 5-day | Iowa total ÷ ECMWF | MT r daily | MT r 5-day | MT total ÷ ECMWF |
|---|---|---|---|---|---|---|
| ERA5T | 0.931 | 0.961 | 1.02 | 0.906 | 0.948 | **0.61** |
| CPC | 0.598 | 0.859 | 0.96 | 0.655 | 0.970 | 0.91 |
| CHIRPS (final `sat` + prelim) | 0.686 | 0.730 | 0.95 | 0.732 | 0.896 | 1.25 |
| CHIRPS prelim only (Sep, 30 d) | 0.464 | 0.368 | 0.94 | 0.608 | 0.592 | 1.49 |

- Totals, mm, Jun–Aug / Sep — Iowa: ECMWF 401.5 / 183.3, ERA5T 399.5 / 199.4, CPC 356.4 / 199.3, CHIRPS 384.3 / 173.0.
- Totals, mm, Jun–Aug / Sep — MT (dry season): ECMWF 30.5 / 42.6, ERA5T **14.4** / 30.2, CPC 20.8 / 51.3, CHIRPS 28.2 / 63.5.
- **ERA5T is the dry outlier in Mato Grosso**: 44.6 mm vs CPC's 72.1 mm over the same 122 days (ratio 0.62).
  ECMWF day 1 sits closer to the gauge product (CPC ÷ ECMWF = 0.91) than ERA5T does.
- Against the gauge product, ECMWF day 1 is no worse than ERA5T: 5-day r vs CPC 0.859 / 0.970
  (ECMWF) and 0.865 / 0.965 (ERA5T); rolling-30-day mean abs difference vs CPC 20.6 mm (ECMWF) and
  17.3 mm (ERA5T) on a ~130 mm Iowa month.
- CPC day window: shifting CPC by −1 day gives r 0.52–0.53, by +1 day 0.04–0.25, so its "day"
  straddles two UTC days. Iowa dry-day agreement with ECMWF is only 73.8 % (ERA5T 89.3 %).

### The production rules, run on each source (`analysis.weather_alerts` thresholds)

- At the common end date 2026-09-30 every source holds ≥122 days, the 90-day baseline is full, and
  **no source fires a dry-spell or deficit alert in either footprint** — they agree, but only on a negative.
- 30-day vs prior-90-day, Iowa: ECMWF +37.6 %, ERA5T +50.3 %, CHIRPS +35.7 %, CPC +69.8 %.
- MT: +321 % to +642 % — the baseline is the dry season (norm 4.8–10.1 mm), so the percentage is
  unstable in every source. That is the rule's design, not a source defect.
- **Dry-spell caveat (MT):** days in a ≥10-day spell — ECMWF 50, CHIRPS 47, CPC 63, ERA5T 64. The
  footprint *mean* crosses 1 mm on IFS drizzle. Graded on wet-area share instead (K2's "share of
  production-weighted area"), ECMWF and ERA5T nearly agree: 114 vs 118 days with < 50 % of area wet.
  → S1 should grade the dry spell on area share, not the footprint mean.

### Tmax

| | Iowa MAE | Iowa max | MT MAE | MT max | MT days > 34 °C | MT days > 38 °C |
|---|---|---|---|---|---|---|
| ECMWF day 1 (reference) | – | – | – | – | 43–44 | 0 |
| ERA5T | 0.50 °C | 3.39 | 0.25 °C | 1.23 | 40 | 0 |
| CPC | 0.97 °C | 3.56 | **1.89 °C** | **6.66** | **59** | **4** |

CPC Tmax fails in Mato Grosso: four phantom "extreme heat" days and 15 extra pod-fill-bar days.

### Soil (IFS `vsw` L1 step 0 vs ERA5-Land `swvl1` 00 UTC, 123 days)

| | r (level) | r (daily change) | mean bias IFS − Land | first 30 d | last 30 d |
|---|---|---|---|---|---|
| Iowa | 0.846 | 0.865 | −0.064 | −0.103 | −0.034 |
| MT | 0.751 | 0.684 | −0.010 | +0.005 | −0.024 |

The offset drifts by up to 0.07 m³/m³ inside one season, so the two are not interchangeable —
K2's finding holds. Soil therefore stays single-model (IFS now → IFS day 15) with no alert.

## Licence for publishing

| Source | Text | Where |
|---|---|---|
| ECMWF Open Data | "governed by the Creative Commons CC-BY-4.0 licence and the ECMWF Terms of Use. This means that the data may be redistributed and used commercially, subject to appropriate attribution." | [ecmwf.int open data](https://www.ecmwf.int/en/forecasts/datasets/open-data); same on the [AWS registry](https://registry.opendata.aws/ecmwf-forecasts/) |
| ERA5 / ERA5-Land | dataset pages show "CC-BY licence"; it replaced the Licence to use Copernicus Products "on Wednesday 2nd July 2025" | [CDS dataset page](https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels); [ECMWF forum notice](https://forum.ecmwf.int/t/cc-by-licence-to-replace-licence-to-use-copernicus-products-on-02-july-2025/13464) |
| CHIRPS v3 | "CHIRPS3 is in the public domain, as registered with Creative Commons, and is licensed under a Creative Commons Attribution 4.0 International License." (self-contradictory; attribute anyway) | [chc.ucsb.edu/data/chirps3](https://chc.ucsb.edu/data/chirps3); GEE catalog says public domain |
| CPC via PSL | "Usage Restrictions None"; asks for "data provided by the NOAA PSL, Boulder, Colorado, USA, from their website at https://psl.noaa.gov" | [PSL dataset page](https://psl.noaa.gov/data/gridded/data.cpc.globalprecip.html); [PSL disclaimer](https://psl.noaa.gov/disclaimer/) |

None blocks publishing or commercial use. The recommendation adds **no new licence** — it is the
forecast's own source, already covered by P1's attribution caption. Open item carried from R1:
read the ECMWF Terms of Use clause before go-live (not re-verified here).

## Why not a real observation

- **ERA5T is also a model.** Its rain and `mx2t` are forecast parameters: "there are a number of
  forecast parameters, e.g. mean rates/fluxes and accumulations, that are not available from the
  analyses" (ERA5 documentation). It is the same kind of number as IFS day 1 from an older, coarser
  model — six days late, behind a queue that rejected parallel jobs today. *Inference:* swapping
  to it buys a "reanalysis" label, not a measurement.
- **CPC is a real gauge analysis** and fast, but its Tmax is unusable in MT, its day is 12Z-ended,
  it has no soil, and PSL's README calls it "poor over tropical Africa". It would also put a
  second model-vs-gauge seam in the middle of one card.
- **CHIRPS** is rain-only, arrives in pentad batches 2–7 days late, and its prelim month scored
  r 0.37–0.61 on 5-day totals. Its daily values are a disaggregation of pentads.
- **Splicing two sources inside one 120-day window is unsafe.** With ERA5T 39 % drier than IFS in MT,
  an ERA5T baseline under an IFS recent window would manufacture a wet anomaly. One series, one source.
- **Seamless join.** Same model, grid, weights and UTC day as the forecast card, so "last 30 days"
  and "next 15 days" are one continuous series.

Alternative reading I could not rule out: IFS day-1 rain may carry a drizzle bias in dry regimes
(MT Jun–Aug: 30.5 mm vs CPC 20.8, ERA5T 14.4). The sample is one season and two footprints.

## How it renders (`LATENCY.md` vocabulary)

- **No gap.** At the 19:00 UTC full run on day D the elapsed side runs to D−1; D onward is forecast.
- **Stamp:** `day-1 estimate · ECMWF 00z · through <D−1>` beside the forecast stamp. Never "observed".
- **provider_delay = 0 h**, basis: the run covering day D is public at 07:34 UTC on D (mirror
  `Last-Modified`, and ECMWF's dissemination schedule) — before the day ends.
- **Observation instant** = end of the UTC day (24:00 UTC D−1), like the river layers' "completed
  day". Age at the 19:00 UTC run ≈ **19 h, all `cadence_wait`**.
- That **exceeds the Weather class's 12 h acquisition objective**. S1 must either state the layer's
  own basis (as `river_us`/`river_ar` do with 24 h) or count in-progress day D. Not decided here.
- **A missing run** leaves that day NULL (never filled from another source); the rules already
  tolerate gaps (`WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS = 45`; a missing day breaks a dry streak).
  Layer states: `stale` when the newest day-1 row is older than its budget, `failed` on a fetch
  error, each with a reason.

## Keeping ≥120 days

- **Primary: `data/history/`.** P1 approved ~24 summary rows per run. Put that run's day-1 values
  (rain, Tmax, Tmin, wet-area share, `vsw1` at step 0) on the same row. **No extra rows**: ~8.8k/yr.
- **Backfill: AWS mirror, self-healing.** On each run, any of the trailing 120 days missing from the
  table is fetched from `s3://ecmwf-forecasts` (10 messages per missing run). First deploy: ~120
  runs, measured 32 s / 845 MB locally. Normal day: 0 runs. After a failed deploy: 1–2 runs.
- So the 30-day-deficit rule is **alive on the first deploy**, not 120 days later.
- Not an archive pull each run: the mirror has no permanence promise ("may retain older data
  conventions/versions over time") and the portal keeps only "the most recent 12 forecast runs".
- Caveat: an IFS cycle upgrade inside the window (50r1 landed 2026-05-13) can step the series,
  soil most of all.

## One footprint table?

Yes. One daily table keyed by footprint, valid date and lead day:

- lead day 1 rows are kept (and mirrored to history via the summary row);
- lead days 2–15 are kept for the latest run only (P1 rule);
- a row is "elapsed" when its valid date is before the latest run date — that maps onto the
  existing `is_forecast` flag, so `assess_region` reads it unchanged apart from the `basis` label.

## Traps for the build

- CDS (if ever used): one queued request per user per dataset; zip named `.nc`; partial last day
  (18 of 24 hours on 10-01) must never be summed.
- CPC rain files are rewritten two days after first posting.
- CHIRPS prelim for the newest pentad returns 404, not an empty file.
- `mx2t3` naming held for all 128 runs back to 2026-06-01; older cycles differ (R1).

## Not verified

- A positive deficit or dry-spell alert case (none occurred in the window).
- Any footprint beyond Iowa and Mato Grosso, and any season beyond Jun–Oct 2026.
- GitHub-runner bandwidth to the AWS mirror.
- ARCO-ERA5 as a queue-free ERA5T path (metadata read only).
- The ECMWF Terms of Use attribution clause; current CDS numeric cost limits.

## Verification pass (2026-10-07, stdlib recomputation from `results/series_*.csv`)

- ERA5T, CHIRPS and Tmax numbers reproduce exactly (Iowa ERA5T r 0.931 / 0.961, ratio 1.02; MT ratio 0.61; CPC Tmax MAE 1.89 °C in MT with 4 days > 38 °C).
- **CPC rain is window-sensitive.** The tables above score CPC on 126 days (to 10-04). On the 122-day
  window common to every source (06-01 → 09-30): daily r 0.541 (Iowa) / 0.410 (MT), 5-day r 0.844 / 0.937,
  total ÷ ECMWF **0.95 / 0.99**. So in Mato Grosso the gauge product and ECMWF day 1 agree on the
  season total to 1 %, while ERA5T stays at 0.61 — the ERA5T-outlier conclusion strengthens, the
  CPC daily correlation is weaker than stated above.
- The AWS mirror returned HTTP 000 (no response) on two consecutive probes, then 200 in 0.28 s.
  The backfill needs retries; a transient failure must not mark a day `failed`.
- Mirror `Last-Modified` for the 2026-10-07 00z run: 07:34:07 UTC — the 07:34 landing time holds.
- `data/history/` untouched (0 files vs the branch base). No secrets in the pushed files.
