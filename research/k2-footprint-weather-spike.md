# K2 · Spike: real footprint weather + hazard flags

Ticket: [#361](https://github.com/philipbergman6-glitch/Mirror-Market/issues/361) · Map: [#356](https://github.com/philipbergman6-glitch/Mirror-Market/issues/356) · Run 2026-10-05 against `main` @ `4258b56`. Throwaway code: [`spike/footprint-weather/`](../spike/footprint-weather/) on branch `spike/footprint-weather`, never merged. Raw outputs: [`spike/footprint-weather/results/`](../spike/footprint-weather/results/).

**Legend:** **[M]** measured in this spike · **[INF]** inference from measurements · **[REC]** recommendation for P1 to approve or amend.

---

## 0. Answer in one screen

| Question | Recommendation | Evidence (headline) |
|---|---|---|
| **Footprint shape** | Admin-1 polygon (Natural Earth), **SPAM 2020 production-weighted inside**. No county/município layer. Official stats only to weight *between* footprints if a belt total is ever wanted. | SPAM vs plain area: ≤6.2 % on MT 15-day rain over 4 runs. NASS county vs SPAM in Iowa: ≤0.2 mm (≤1 %) in all 4 runs. |
| **Beside vs replace** | **Replace the pin *forecast* with the footprint forecast. Keep the pin *observed* 30 days for now** (it feeds the observed-only agronomic alerts) until an observed footprint source is chosen (new ticket). | Pin 15-day rain vs its own footprint: Iowa −98 %…+55 %, MT −12 %…+48 % over 4 runs; the pin sat at the 33rd–95th percentile of its own state. Open-Meteo vs ECMWF *at the same pin* (days 2–7) is the smaller error: ≤2.6 mm, Tmax MAE 0.3–1.1 °C. |
| **Alert rules** | Forecast alerts are a **separate class**, labelled "forecast · ECMWF 00z", capped at `warning`, never merged with observed-row alerts. Reuse `WEATHER_DRY_SPELL_ALERT_DAYS`, `WEATHER_PRECIP_DEFICIT_ALERT_PCT`, `WEATHER_POD_FILL_HEAT_C`, `WEATHER_EXTREME_HEAT_C`, `WEATHER_HEAVY_RAIN_MM` but apply them to **share of production-weighted area**, not to the area mean. **No soil-moisture alert** in v1. | A dry day (< 1 mm) on an area mean is a different event from one at a point: Iowa pin dry-run 11 and 15 days on two runs, where only 35 % / 56 % of the soy area was dry ≥10 days. |
| **Hazard flags** | Flag a **port/pricing point** when it falls inside an NHC/JTWC forecast **34/50/64-kt quadrant radius** at any time ≤120 h (track + radii interpolated hourly). Band → severity: 64 kt = `alert`, 34/50 kt ≤72 h = `warning`, 34/50 kt 72–120 h = `info`. Growing areas get no radius flag (agency radii are "valid over open water only"); their storm rain is the footprint rain. South Atlantic places = `not_covered`. | Hurricane Francine 2024 replay: NOLA flagged on advisories 8–12, first TS-force wind 33 h → 11 h ahead. Today: 5 storms live, 0 rendered places within 2,200 km; 9 places `not_covered`. |
| **Attach-to-leg** | A `LEG_PLACES` registry keyed by the existing ledger `leg_id` (`"us_gulf:cif"` …), validated at load like `config.LEDGERS`; `_ledger_row` gets a `hazard` field; every ledger that includes the leg inherits it. | K1 matrix encoded as 18 legs → places in `storms.py`; leg roll-up computed. |
| **Row counts** | ~24 footprints × 15 days = **360 daily rows/run** + 24 summary rows/run; hazard rows = 2 source-status + 1 per live storm + 1 per flagged place. Persist **summary rows only** in `data/history/`; daily detail for the latest run only. | Daily-detail history would be ~94k rows/yr (~3× today's largest history CSV); summary-only ~6k rows/yr. |
| **CI runtime** | Add to the full 19:00 run, not `--fast`: **≈1–1.5 min** total. | 248 messages / 155–156 MB per run; fetch+decode 18–45 s (home link); decode CPU 1.8 s; aggregation 0.02 s for 2 footprints; storms 3.7–4.2 s; `pip install eccodes` 10–20 s (R1). |

---

## 1. What was run

| Step | What | Where |
|---|---|---|
| Footprints | Natural Earth 10m admin-1 `BR-MT`, `US-IA` (hard-fail unless exactly one polygon). SPAM 2020 V2r2 `P_SOYB_A` 5′ cells whose centre is inside → nearest ECMWF 0.25° point. Weights: `admin1_area` (cos lat), `admin1_soy` (SPAM tonnes), `county_nass` (Iowa: NASS 2021–25 mean county production spread over each county's cells; Census `cb_2024_us_county_500k`), `pin` (nearest point to `config.GROWING_REGIONS`). | `footprints.py`, `nass.py` |
| Forecast | ECMWF IFS 0.25° **00z** runs 2026-10-02/03/04/05. Live `.index` per step → HTTP Range per message → `eccodes` decode (grid keys asserted) → keep only footprint points. `tp` 24…360 h; `vsw` L1–4 0…360 h daily; `mx2t3/mn2t3` 3…144 h; `mx2t6/mn2t6` 150…360 h; `lsm` 0 h. | `fetch_ecmwf.py` |
| Aggregates | Daily (UTC) rain = step difference; Tmax/Tmin = max/min of the 3 h (d1–6) or 6 h (d7–15) windows per point, then weighted mean; `vsw` land-masked (`lsm > 0.5`, and `vsw > 0`). | `analyse_fc.py` |
| Pin comparison | Layer 5's own `fetch_region_weather` (unchanged; same call the pipeline makes) for the two pins. | `analyse_fc.py`, `results/openmeteo_pins.json` |
| Normal | ERA5 hourly `tp` + `2t` and ERA5-Land `swvl1`, 1991–2020, same UTC days (Oct 5–19), box per footprint. | `clim.py`, `anomaly.py` |
| Port rain | `tp` over a 3×3-cell box (~75 km) around P-PNG, P-NOLA, P-UPR, sea cells kept. | `port_rain.py` |
| Storms | NHC `CurrentStorms.json` → `_5day_latest` + `_fcst_latest` GIS; JTWC `jtwc.rss` → `<id>.tcw`. K1's 12 ports + 24 pins; K1 §3 leg matrix. Replay: NHC archive Francine `al062024` advisories 008–012. | `storms.py` |

## 2. Footprint averages, Mato Grosso and Iowa [M]

Run 2026-10-05 00z, valid 2026-10-05 00 UTC → 2026-10-20 00 UTC (15 UTC days). `results/fc_20261005.json`.

| | MT pin | MT area | **MT soy** | IA pin | IA area | **IA soy** | IA county |
|---|---|---|---|---|---|---|---|
| 15-day rain, mm | 65.6 | 72.8 | **74.1** | 15.4 | 10.5 | **10.6** | 10.6 |
| days 1–7 rain, mm | 21.2 | 21.7 | 21.4 | 0.1 | 1.3 | 1.3 | 1.3 |
| footprint p10 / p50 / p90 (15-day), mm | — | 51 / 72 / 95 | 52 / 73 / 100 | — | 5 / 10 / 16 | 5 / 10 / 17 | — |
| mean daily Tmax, °C | 33.9 | 33.5 | 33.3 | 23.3 | 23.0 | 23.0 | 23.0 |
| peak daily Tmax, °C | 36.8 | 36.2 | 36.0 | 26.9 | 26.9 | 26.8 | 26.8 |
| days with mean Tmax ≥ 34 °C | 7 | 7 | 7 | 0 | 0 | 0 | 0 |
| `vsw` L1 day 0 → day 15, m³/m³ | .323→.424 | .273→.393 | .283→.401 | .314→.271 | .299→.232 | .298→.234 | .295→.230 |
| lead dry days (< 1 mm, from day 1) | 0 | 0 | 0 | **9** | 4 | 4 | 4 |
| share of area dry ≥ 10 days | — | 0 % | 0 % | — | — | 13.4 % | — |

Footprint sizes [M]: MT 10,853 SPAM cells → **1,278** ECMWF points, 34.5 Mt SPAM soy, 70 % of the soy weight in the top quarter of points (concentrated). IA 2,276 cells → **274** points, 14.6 Mt, 36 % in the top quarter (spread). Sea points masked: MT 2, IA 0. Land points with `vsw == 0`: 0 in both.

**Across four runs** (`fc_202610{02,03,04,05}.json`), 15-day rain [M]:

| Run 00z | MT pin / soy | MT pin vs soy | MT soy vs area | MT pin pctile | IA pin / soy | IA pin vs soy | IA soy vs area | IA county vs soy | IA pin pctile |
|---|---|---|---|---|---|---|---|---|---|
| 10-02 | 78.0 / 70.1 | +11 % | +1.5 % | 73 | 21.5 / 26.3 | −18 % | −2.5 % | −0.2 mm | 34 |
| 10-03 | 73.9 / 49.8 | **+48 %** | −1.7 % | 95 | 0.0 / 1.9 | **−98 %** | +9.0 % (0.2 mm) | 0.0 | 35 |
| 10-04 | 75.1 / 71.8 | +5 % | **+6.2 %** | 54 | 26.9 / 17.3 | **+55 %** | −9.5 % (1.8 mm) | 0.0 | 77 |
| 10-05 | 65.6 / 74.1 | −12 % | +1.8 % | 33 | 15.4 / 10.6 | +45 % | +1.6 % | 0.0 | 85 |

MT `vsw` L1 day 0, pin vs soy footprint: +0.04, +0.08, +0.10, +0.04 m³/m³ — the pin sits on wetter-than-typical soil in every run [M]; soil level at a point is mostly soil texture, not weather [INF].

## 3. Against the Open-Meteo pins (Layer 5) [M]

Layer 5 asks for 7 forecast days with `timezone=auto`, so its days are **local** (MT UTC−4, IA UTC−5) and its first row is *today*, part-observed. ECMWF days here are UTC 00–24. Same 2026-10-05 window:

| | rain OM pin | rain EC pin | rain EC soy | Tmax MAE OM↔EC pin | Tmax MAE OM↔EC soy | worst day OM↔EC soy |
|---|---|---|---|---|---|---|
| MT day 1 (today) | 19.1 | 13.9 | 10.8 | 5.0 °C | 4.6 °C | 4.6 °C |
| MT days 2–7 | 9.9 | 7.3 | 10.6 | 0.3 °C | 0.7 °C | 1.9 °C |
| IA day 1 (today) | 0.0 | 0.0 | 0.0 | 1.5 °C | 1.1 °C | 1.1 °C |
| IA days 2–7 | 1.9 | 0.1 | 1.3 | 1.1 °C | 0.9 °C | 2.2 °C |

Where they disagree:
- **Day 1** is not comparable. Open-Meteo's "today" is a local-time row mixing what already happened with forecast; the 25.6 °C MT Tmax vs 30–31 °C in ECMWF is that, plus the 4 h offset. A footprint product that shows "today" must say which day boundary it uses.
- **Days 2–7** agree closely: model-vs-model at the same pin is ≤2.6 mm of rain and ≤1.1 °C MAE in Tmax.
- **The big disagreement is spatial, not model:** a single grid point vs the soy-weighted state (§2), up to −98 %/+55 % on 15-day rain.

## 4. 15-day anomaly vs ERA5 1991–2020 [M]

ANOMALY_SECTION

## 5. Port rain [M]

`results/port_rain.json`, 3×3-cell box, 2026-10-05 00z:

| Port | 15-day rain | days box-mean ≥ 1 mm | ≥ 5 mm |
|---|---|---|---|
| P-PNG Paranaguá | **208.2 mm** | 14 | 11 |
| P-NOLA | 119.5 mm | 5 | 5 |
| P-UPR Rosario/up-river | 23.6 mm | 4 | 1 |

Port rain uses the same `tp` messages as every footprint — **zero extra download** in a combined fetch (the standalone probe cost 15 messages / 16 MB / 22.7 s) [M]. It needs **no distinct data contract**: a port footprint is one more `(points, weights)` set [INF]. Paranaguá's loading-halt signal is rain *hours*, not daily totals; ECMWF's 3-hourly `tp` steps (to 144 h) can count wet 3-hour windows if P1 wants that [INF]. No sourced "loading halts at X mm" threshold was found; none is invented here.

## 6. Storms vs the K1 place list [M]

`results/storms_today.json` (2026-10-05, NHC 15:00Z, JTWC 12Z issue; 4.2 s end to end):

| Storm | Source | Position t0 | Vmax | Used? |
|---|---|---|---|---|
| Rachel `ep182026` | NHC | 20.4N 116.2W | 75 kt | yes |
| Rachel `ep1826` | JTWC | 20.4N 115.8W | 75 kt | **dropped** (NHC duplicate) |
| Nolo `ep1526` | JTWC | 25.6N **176.2E** | 105 kt | **yes** — west of 180°, NHC no longer lists it |
| Choi-wan `wp2626` | JTWC | 27.3N 145.9E | 110 kt | yes (forecast ends 48 h: extratropical) |
| Koguma `wp2726` | JTWC | 11.5N 167.9E | 35 kt | yes |

Result: **no rendered place is flagged or on watch.** Nearest approaches: G-HL Heilongjiang 2,224 km (Choi-wan, +31 h), P-NCN 2,488 km, P-NOLA 2,828 km (Rachel). **9 places `not_covered`** (P-PNG, P-UPR, G-MT, G-PR, G-RS, G-PAM, G-COR, G-BAS, G-PY). Leg roll-up: 12 legs `clear`; 5 legs wholly `not_covered` (CEPEA/ESALQ Paraná, ESALQ/B3 Paranaguá, AgRural Paranaguá FOB, MAGyP FOB soy/oil/meal, MAGyP FOB sunflower oil); DCE No.2 `clear | not covered: P-PNG, G-MT`; CZCE Rapeseed Oil has no places (K1 gap).

**Replay, Hurricane Francine 2024** (NHC archive `al062024`, advisories 008–012, i.e. 2024-09-10 15Z → 2024-09-11 15Z):

| Advisory | P-NOLA | first 34-kt arrival | closest approach | legs flagged |
|---|---|---|---|---|
| 008 | flag, TS-force | +33 h | 112 km at +38 h | CBOT Soybeans, US Gulf CIF, DCE No.2 (via P-NOLA) |
| 009 | flag, TS-force | +28 h | 104 km | same |
| 010 | flag, TS-force | +24 h | 107 km | same |
| 011 | flag, TS-force | +21 h | 99 km | same |
| 012 | flag, TS-force | +11 h | 106 km | same |

Two defects found and fixed in the spike rule [M]:
1. **Inland remnant noise.** A plain "centre within 500 km" watch flagged G-IL Illinois (292–498 km at +60…+96 h) from Francine's post-tropical remnant. Gating the watch on the storm being ≥34 kt at that hour removed it on every advisory.
2. **De-duplication by prefix is wrong.** R2 said "drop JTWC's East/Central Pacific duplicates". Nolo keeps its `ep15` id at JTWC after crossing 180°, where NHC no longer issues. Dedupe on *NHC currently lists the same basin + storm number*, never on the `ep`/`cp` prefix.

## 7. Recommendations

### 7.1 Footprint shape [REC]

- **Unit:** admin-1 polygon from Natural Earth (R3), joined by Wikidata id, hard-fail on no/duplicate match. Multi-unit footprints where K1 flagged ambiguity: G-PAM = AR-B + AR-S, G-PY = PY-10 + PY-7 + PY-14, G-RO = the Bărăgan județe, G-FR = dissolved `region_cod = FR-GES`.
- **Weights inside the unit:** SPAM 2020 production of the leg's crop (`SOYB`, `RAPE`, `SUNF`, `OILP`). It matters most where the crop is concentrated (MT: 70 % of weight in a quarter of the points; up to 6.2 % on 15-day rain) and costs nothing where it isn't (Iowa).
- **No admin-2 layer.** NASS county weights moved Iowa by ≤0.2 mm in 4 runs. Official sub-national stats (R3) are for *between-footprint* weights only, if P1 wants a single belt number per leg; otherwise render footprints separately (MT, PR, RS are different weather).
- **Weights are static reference data**, rebuilt yearly with an age budget (R3); store the point/weight table as a committed reference CSV so CI never reads SPAM or Natural Earth at run time.

### 7.2 Beside vs replace [REC]

Replace the **forecast** half of each pin card with its footprint; keep the pin's **observed** trailing 30 days until there is an observed footprint source.

- Two forecasts for one state disagree by up to 98 % on 15-day rain (§2). Rendering both "beside" puts two different rain numbers for Iowa on one page and two alert sets behind them.
- The gain is from area, not model: Open-Meteo vs ECMWF at the same pin is small (§3).
- Licence pushes the same way: Open-Meteo's free API is non-commercial ([#366](https://github.com/philipbergman6-glitch/Mirror-Market/issues/366)); ECMWF is CC BY 4.0, commercial OK (R1).
- The observed side cannot move yet: Layer 5's `past_days=30` rows drive the observed-only agronomic alerts (`config.py` "observed rows only — forecast rows are excluded"). ECMWF open data carries no history beyond ~3½ days. Graduated as a new ticket (§8).

### 7.3 Alert rules for footprint weather [REC]

All forecast alerts carry "forecast · ECMWF 00z {date}", are capped at `warning`, and stay a separate list from observed-row alerts (lands on the one rule set [#355](https://github.com/philipbergman6-glitch/Mirror-Market/issues/355) produces).

| Rule | Reuses | Test on the footprint | Measured on 4 runs |
|---|---|---|---|
| Dry spell | `WEATHER_DRY_SPELL_ALERT_DAYS = 10`, `WEATHER_DRY_THRESHOLD_MM = 1` | ≥ 50 % of production-weighted area has ≥10 consecutive forecast days < 1 mm from day 1, in a `WEATHER_GROWING_SEASON_MONTHS` month | IA 35 / 56 / 25 / 13 % → fires once (10-03); MT 0 % every run. The pin rule would have fired twice (11, 15 dry days). |
| Rain deficit | `WEATHER_PRECIP_DEFICIT_ALERT_PCT = 40` | 15-day production-weighted total ≤ 60 % of its ERA5 1991–2020 normal for the same days | DEFICIT_MEASURED |
| Heavy rain | `WEATHER_HEAVY_RAIN_MM = 20` | ≥ 25 % of area has any forecast day ≥ 20 mm → `info` (harvest/planting logistics, port loading) | not tuned; port boxes hit it (P-PNG 32.3, 33.9 mm days) |
| Pod-fill heat | `WEATHER_POD_FILL_HEAT_C = 34`, `WEATHER_SOY_POD_FILL_MONTHS` | ≥ 3 days with weighted Tmax ≥ 34 °C in a pod-fill month | MT has 4–9 such days in every run, but October is planting → gate holds, no alert |
| Extreme heat | `WEATHER_EXTREME_HEAT_C = 38` | any day with weighted Tmax ≥ 38 °C in season | peak 36.0 °C (MT), none |
| Soil moisture | — | **none in v1**: render level and 15-day change only | MT IFS day 0 ranks 3–5 of 31 vs ERA5-Land but moved 0.236 → 0.335 between consecutive runs; IFS and ERA5-Land soil schemes differ, so a cross-model anomaly is not trustworthy (§4) |

### 7.4 Hazard-flag rules and attach-to-leg [REC]

- **Sources:** NHC + JTWC only (R2). Dedupe by NHC's live list (basin + number), not prefix (§6).
- **What gets a radius flag:** ports and pricing points (`P-*`). Growing areas (`G-*`) get no radius flag — NHC/JTWC radii are "valid over open water only", and the Francine remnant showed the inland noise; storm rain on a footprint is already in the footprint rain.
- **Geometry:** track and 34/50/64-kt quadrant radii linearly interpolated hourly between forecast taus; quadrant from the bearing storm→place.
- **Look-ahead:** 0–120 h (both agencies' horizon). NHC 64-kt radii stop at 72 h, so a hurricane-force flag beyond 72 h cannot appear for NHC storms; say so rather than imply "clear".
- **Severity from the agencies' own bands:** inside 64-kt → `alert` ("hurricane/typhoon-force winds forecast"); inside 34/50-kt at ≤ 72 h → `warning`; at 72–120 h → `info`; centre ≤ 500 km while ≥ 34 kt but outside the radii → `info` ("watch"). Label with the agency's own class (Saffir-Simpson / JTWC TS-TY-STY) and "NHC" or "JTWC guidance", never a national signal.
- **States** (invariant 1): `flag`, `watch`, `clear` (asked, no storm reaches it), `not_covered` (South Atlantic, with R2's reason), `stale` (issuance > 12 h NH / 18 h SH during an active storm), `failed` (source fetch failed). Each place declares its basin in the place registry (`none` with a reason for inland places far from any basin), so "no cyclone exposure" is a stated fact, not an absence.
- **Attach-to-leg mechanism:**
  1. A place registry (`P-*`, `G-*`, lat/lon, kind, basin, **`effective_from`/`effective_to`** for K1's Randfontein → Driefontein case).
  2. `LEG_PLACES = {leg_id: (place ids…)}` keyed by the same `leg_id` strings `config.LEDGERS` uses, validated in `app/markets.py` like `_wire_ledgers` (unknown leg or place = `ValueError`), with a completeness test: every rendered leg has an entry; an empty tuple needs a stated reason (CZCE today).
  3. `_ledger_row` (`app/block_builders.py:719`) reads `ctx.leg_hazards(leg_id)` and sets `row["hazard"] = {state, level, storm, source, first_arrival_h, reason}`. A leg's hazard is the worst state among its covered places, plus the list of `not_covered` places.
  4. Because the flag rides the leg row, every ledger that lists the leg (US Gulf CIF appears on CBOT, Dalian, Brazil and Argentina ledgers) and the origins page via `ORIGIN_LEGS` inherit it with no per-page code. Price keys that are not ledger legs (DCE No.1, CBOT oil/meal) keep using the existing block-01 chip path (`app/block_builders.py:428`).

### 7.5 Row counts and CI runtime [M]/[INF]

| Item | Per run | Per year (260 weekday runs) |
|---|---|---|
| Footprint daily detail (~24 footprints × 15 days; one row = all variables) | 360 | 93,600 (~8 MB) if every run kept |
| Footprint summary (1 per footprint: 15-day totals, anomaly, dry/heat shares, `vsw` level + change) | ~24 | ~6,240 |
| Port-rain boxes (P-PNG, P-NOLA, P-UPR …) | 3–12 summary rows | ≤3,120 |
| Hazard: source status (NHC, JTWC) + 1 per live storm + 1 per flagged/watch place | 2 + 5 + 0 today; 2 + 1 + 1 for Francine | a few thousand |

For scale: the largest current history CSV is `contract_bars.csv`, 32,133 rows / 3.4 MB. **Recommend** history = summary rows only (system of record, forecast-vs-outcome possible later); daily detail = latest run only. ECMWF's AWS mirror (back to 2023-01-18, R1) can recompute daily detail if a verification study is ever wanted.

| Stage | Measured |
|---|---|
| `.index` fetch, 85 steps | 1.3–2.2 s |
| Range fetch + decode, 248 messages | 155.3–156.3 MB; 18.2–45.4 s wall (first run cold) |
| Decode CPU | 1.7–1.8 s |
| Aggregation, 2 footprints × 4 methods | 0.02 s |
| Footprint build from SPAM + Natural Earth (2 footprints) | 0.5 s (→ committed reference table; not in CI) |
| Storms, NHC + JTWC, 36 places | 3.7–4.2 s |
| `pip install eccodes` | 10–20 s (R1 estimate, wheel) |

Footprint count does not change the download: every field is one global message, so 2 or 24 footprints cost the same 156 MB [INF]. **≈1–1.5 min** added to the 19:00 full run; job timeout is 30 min. GitHub-runner bandwidth to `data.ecmwf.int` was not measured; the AWS mirror is the fallback route.

## 8. Effects on the map

- **New ticket (graduated):** *Observed side of footprint weather* — which source carries the trailing 30 observed days per footprint once the pin forecast is replaced (persisted ECMWF day-1, ERA5T via CDS, or CHIRPS), so the observed-only alerts can move off Open-Meteo. Blocked by P1 (only needed if P1 approves "replace").
- **Fog cleared:** *CI cost* (measured, §7.5). *Port rain* — no distinct data contract (§5); whether to render it goes to P1.
- **Fog sharpened into P1:** *History and storage shape* — row counts measured (§7.5); recommendation for P1 to approve.
- **Traps for the build:**
  - Accumulation differences go slightly negative from 16-bit packing: worst −0.0153 mm over 4 runs × 1,552 points. Clamp to 0 down to −0.05 mm, hard-fail below.
  - NHC 5-day `pts` attributes `LAT`/`LON` are rounded to whole degrees in the DBF too, so use the geometry.
  - Daily-statistics CDS datasets refuse 30 years in one request ("cost limits exceeded"). Hourly ERA5 1991–2020 for a footprint box took CLIM_TIME; ERA5-Land boxes took 391 s (IA) and 515 s (MT).
  - NASS withholds Adams County, IA (folded into county `998`, "other combined"); a county-weighted footprint must not read that as zero production.
  - `heat35` / Tmax from ERA5 hourly `2t` sampling runs ~0.3–0.5 °C below the IFS `mx2t*` window maxima [INF, not measured]; the anomaly label must name the normal's variable.

## Attribution

ECMWF open data, CC BY 4.0: "This service is based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF)." ERA5 / ERA5-Land: Copernicus Climate Change Service, CC BY 4.0. SPAM 2020 V2r2: "This data was provided by the International Food Policy Research Institute (IFPRI). IFPRI bears no responsibility for the analyses or interpretations of the data presented here." NASS: "This product uses the NASS API but is not endorsed or certified by NASS." Boundaries: Natural Earth (public domain); US Census Bureau cartographic boundary files. Storms: NOAA NHC; JTWC (public information).
