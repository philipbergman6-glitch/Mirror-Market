# R1 — ECMWF Open Data: variables, horizons, retention, ensemble, licence

**Issue:** [#357](https://github.com/philipbergman6-glitch/Mirror-Market/issues/357) (map #356, blocks #361)
**Date:** 2026-10-05
**Author:** research agent

Legend for confidence: **[OBS]** directly observed today on a live call (directory listing, `.index`
file, HEAD/Range response, or a GRIB message decoded with ecCodes) · **[SRC]** quoted from an ECMWF
primary source · **[INF]** logical inference from OBS/SRC · **[NV]** not verified — do not treat as fact.

Where the ECMWF docs and the live server disagree, **the live server wins** and the conflict is noted.

---

## 0. Answer in ten lines

1. **Rainfall:** `tp`, metres of water, **accumulated from run start** (GRIB `stepRange` `0-24`, `0-48`, …). A daily total is `tp(t) − tp(t−24h)`. Available out to 360 h on the 00z/12z runs. [OBS]
2. **Soil moisture:** `vsw`, m³/m³, 4 IFS soil layers (0–7, 7–28, 28–100, 100–289 cm). **It is 0.0 over sea with no bitmap**, so it must be masked with `lsm`. A 0 there means "not land", not "dry". [OBS]+[SRC]
3. **Heat:** the window flips at 144 h. **0–144 h carries only `mx2t3`/`mn2t3`** (3 h max/min); **150–360 h carries only `mx2t6`/`mn2t6`**. A daily Tmax is the max over 8 three-hour windows, or over 4 six-hour windows. [OBS]
4. **Runs:** 00z and 12z go to 360 h (3-hourly to 144 h, then 6-hourly). 06z and 18z go to 144 h only. A full run lands at once, **00z at ~07:34 UTC** (ENS 07:40, probabilities 08:01). [OBS]
5. **Retention:** the portal holds about **3½ days** (4 date folders; 2026-10-02 00z still there at 2026-10-05 14:05 UTC; 2026-10-01 returns 404). ECMWF documents it as "the most recent 12 forecast runs". [OBS]+[SRC]
6. **Backfill exists anyway:** the ECMWF-managed AWS mirror `s3://ecmwf-forecasts` (eu-central-1) uses the same paths and has history back to **2023-01-18**. It is anonymous, listable and current to 2026-10-05. [OBS]
7. **Ensemble: yes.** 50 perturbed members (`enfo/ef`, `pf` 1–50) carry `tp`, `vsw`, `mx2t3/6`, `mn2t3/6`, `ro`, `ssrd`, … to 360 h. **Ready-made probabilities** (`enfo/ep`) give `tpg1/5/10/20/25/50/100` (P[24 h rain ≥ X mm]) in 24 h windows every 12 h to 360 h. [OBS]
8. **Grid:** regular 0.25° lat/lon, 1440×721, lon −180→179.75. Each field is one global GRIB2 message of 0.2–1.3 MB, CCSDS-packed. **There is no server-side area subset:** each footprint average costs one full global message per field. [OBS]
9. **Licence:** CC BY 4.0 **plus** ECMWF Terms of Use. Commercial use is allowed. A public service must show the "based on data and products of ECMWF" statement, the CC BY link, the liability disclaimer and a modification note (exact text in §7). [SRC]
10. **Decoding:** `pip install eccodes` pulls the `eccodeslib` binary wheel (manylinux x86_64, cp311–cp314, 9.1 MB download), so CI needs no `apt-get`. Decoding was proven end-to-end today from Range-fetched messages. [OBS]

---

## 1. Variables for crop weather

Source: live `.index` for 2026-10-04 00z oper, steps 0/24/144/150/240/360
(e.g. <https://data.ecmwf.int/forecasts/20261004/00z/ifs/0p25/oper/20261004000000-240h-oper-fc.index>),
plus single messages decoded with ecCodes 2.49.0 (`scratch decode.py`; keys quoted below).

| Param | Units [OBS GRIB `units`] | Semantics | Steps present | ~bytes/msg |
|---|---|---|---|---|
| `tp` | `m` | `stepType=accum`, `stepRange=0-24`, `0-48`, … **from run start**. Step 0 is a 224-byte constant-zero field. `tp48 ≥ tp24` at every grid point (checked). [OBS] | every step | ~1.05 MB |
| `vsw` | `m**3 m**-3` | `typeOfLevel=soilLayer`, `level` 1–4, `stepType=instant`. Layer depths are not in the message: the GRIB carries soil-layer *numbers* 1–4. Depths are 0–7 / 7–28 / 28–100 / 100–289 cm per the IFS 4-layer scheme [SRC param-db 39–42]. Mapping that scheme onto `vsw` level n is **[INF]** (same model, same layer count). | every step | ~0.47 MB × 4 |
| `mx2t3` / `mn2t3` | `K` | `stepType=max/min`, `stepRange=21-24`: max/min 2 m T over the **previous 3 h** [OBS + SRC param-db 228026/228027] | **0–144 h only** (00z/12z); all steps on 06z/18z | ~0.65 MB |
| `mx2t6` / `mn2t6` | `K` | `stepRange=234-240`: max/min over the **previous 6 h** [OBS + SRC param-db 121/122] | **150–360 h only** | ~0.65 MB |
| `ro` | `m` | runoff, `accum` from run start (`stepRange=0-240`) [OBS] | every step | ~0.2 MB |
| `ssrd` | `J m**-2` | downward solar, `accum` from run start; divide the step difference by seconds to get W m⁻² [SRC param-db 169] | every step | ~1.15 MB |
| `sot` | K | soil temperature, 4 layers [OBS index] | every step | ~0.6 MB × 4 |
| `lsm` | 0–1 | land-sea mask, static [OBS] | every step | 0.19 MB |

Other surface fields in every oper file [OBS]: `2t 2d 10u 10v 100u 100v 10fg msl sp skt tcc tcw tcwv tprate ptype sf sd rsn asn ewss nsss ssr str strd ttr sve svn zos sithick mucape`. Pressure levels (13–14 levels): `d gh q r t u v vo w z`.

**Traps (all [OBS]):**

- **The `vsw` sea value is 0.0, not missing.** `bitmapPresent=0`, `numberOfMissing=0`. Of 687,048 points with `lsm ≤ 0.5`, 680,128 have `vsw == 0`. Another 3,688 land points are also exactly 0. [INF] These are probably ice sheets or other non-hydrological land, but that is unchecked. Under invariant 2, a footprint mean must be land-masked or it gets diluted toward "dry".
- **The temperature window changes at 144 h.** At steps ≤ 144 there is no `mx2t6` (24 h index: only `mx2t3/mn2t3`). At steps ≥ 150 there is no `mx2t3`. A fetcher that asks for `mx2t6` everywhere gets nothing for days 1–6.
- **`10fg` becomes `10fg3` on the 06z/18z runs** (144 h index). [OBS] Not crop-relevant; it is a sign that param names differ between cycles.
- **Accumulations start at the run, not each day.** Daily rain is a difference of two steps. Over 0–144 h every 3 h step exists; beyond 144 h only 6-hourly steps exist. Both cover 24 h boundaries (00/24/48…), so daily differences work at every horizon to 360 h. [INF from the step lists]
- **The ECMWF param-db pages render with JavaScript.** WebFetch sees an empty shell. The JSON API works: `https://codes.ecmwf.int/parameter-database/api/v1/param/<id>/?format=json` [OBS].

## 2. Runs, horizons, step spacing, publication time

**Live directory listings** (<https://data.ecmwf.int/forecasts/20261004/>), 2026-10-04 [OBS]:

| Run | `ifs/0p25/oper` (`fc`) | `ifs/0p25/enfo` (`ef`) | `ifs/0p25/enfo` (`ep`) |
|---|---|---|---|
| 00z | 85 steps: 0–144 by 3, 150–360 by 6 | 85 steps, same | files `240h` + `360h` |
| 06z | 49 steps: 0–144 by 3 | 49 steps: 0–144 by 3 | **none** |
| 12z | 85 steps: 0–360 (as 00z) | 85 steps | `240h` + `360h` |
| 18z | 49 steps: 0–144 by 3 | (not listed; assumed as 06z [INF]) | — |

**Docs disagree with the server.** The Confluence page (last updated 2026-09-04) says of the
Medium-range Control: "The steps available are 0h to 144h by 3h and 150h to 240h by 6h" and
"The steps available are 0h to 90h by 3h only" [SRC — <https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS>].
The live server serves 0–360 h at 00z/12z and 0–144 h at 06z/18z. The page also says ENS
probabilities are available "at all times 00, 06, 12 and 18 UTC", but no `ep` file exists in
`20261004/06z/ifs/0p25/enfo/` [OBS]. **Trust the listing, and have the fetcher check the step it needs is in the
directory or index. Never assume it.**

**IFS cycle 50r1 (13 May 2026) changes** [SRC — same Confluence page and
<https://www.ecmwf.int/en/forecasts/datasets/open-data>]:

- `oper` now means the **"IFS Medium-range Control"** forecast. "Prior to 50r1, this data used `scda` in both the path and filename"; "`scda` is deprecated". Today's 06z files sit under `oper/` [OBS].
- "Prior to 50r1, the ENS data contained both the control and perturbed members." Now `enfo/ef` carries **only `pf` members 1–50** (8,500 messages = 50 × 170, `type` counter `{pf: 8500}`) [OBS]. The control is the `oper` run.
- The `ecmwf-opendata` Python README still documents `scda` and `cf` as live values [SRC — <https://raw.githubusercontent.com/ecmwf/ecmwf-opendata/main/README.md>]. It is stale relative to 50r1, so check any client code against the live index.

**Publication time.** HTTP `Last-Modified` is identical across every step of a run, so a run appears as a unit [OBS]:

| Run (2026-10-04) | oper | enfo/ef | enfo/ep |
|---|---|---|---|
| 00z | 07:34 UTC (+7 h 34 m) | 07:40 | 08:01 |
| 06z | 12:27 (+6 h 27 m) | — | — |
| 12z | 19:34 (+7 h 34 m) | 19:40 | — |
| 18z | 00:27 next day (+6 h 27 m) | — | — |

The same 07:34 time held for the 2026-10-02 and 2026-10-05 00z oper files [OBS], and on AWS for
2026-09-01 (`2026-09-01T07:34:32Z`) [OBS]. ECMWF: IFS data are released "at the end of the
real-time dissemination schedule" [SRC — datasets page]. `ecmwf-opendata` README: "between 7 and 9 hours after the forecast starting date" [SRC].

**Fit with our CI** [INF]: `deploy-dashboard.yml` runs at `0 19 * * 1-5` (19:00 UTC, often late). At
19:00 the newest *360 h* run is 00z (07:34); 12z lands at 19:34, a race. **Pin to the 00z
run** for a deterministic 15-day horizon. Do not pin to "latest", which flips between 06z (144 h) and 12z (360 h)
depending on GitHub's delay.

## 3. Retention — can CI backfill?

**Portal.** The root <https://data.ecmwf.int/forecasts/> lists exactly `20261002/ 20261003/ 20261004/ 20261005/`, each folder created at 06:40 [OBS, 2026-10-05 14:05 UTC].
HEAD on `…/<date>/00z/ifs/0p25/oper/<date>000000-240h-oper-fc.index`: 2026-09-28, 09-29, 09-30, 10-01 → **404**, 10-02 → **200** [OBS].
ECMWF's wording: "rolling archive basis … the most recent 12 forecast runs, corresponding to approximately 2–3 days of forecasts" [SRC — datasets page].
Observed today: 14 runs (3 full days + today's 00z/06z), i.e. slightly more than documented.
**[INF]** The oldest date seems to roll off once a day, around 06:40 UTC. A weekday-only cron that misses
Friday to Monday can still reach Friday's 00z on Monday evening, but only just. That is not a backfill strategy.

**AWS mirror** (managed by ECMWF) [OBS + SRC — <https://registry.opendata.aws/ecmwf-forecasts/>]:

- Bucket `ecmwf-forecasts`, region `eu-central-1`, anonymous HTTPS listing works: `https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/?list-type=2&delimiter=/`. The first prefixes are `20230118/ 20230119/ …` [OBS].
- Same path layout and `.index` files: `20260901/00z/ifs/0p25/oper/20260901000000-240h-oper-fc.grib2` (142,107,678 B) and `20261005/00z/ifs/0p25/enfo/20261005000000-240h-enfo-ep.grib2` (2026-10-05T08:01:02Z) [OBS].
- Registry text: the bucket "is maintained as a rolling archive and may retain older data conventions/versions over time" [SRC]. **No stated retention guarantee [NV]**. Old files are in `INTELLIGENT_TIERING` storage [OBS].
- Licence is unchanged on the mirror: "published under a Creative Commons Attribution 4.0 International license (CC-BY-4.0) and the ECMWF Terms of Use" [SRC].

**Conclusion [INF]:** CI must not rely on the portal for anything older than ~3 days. AWS covers repair and backfill
in practice, with the same code path (switch the base URL; `ecmwf-opendata` has `source="aws"`). AWS makes no
retention promise, though. A forecast-vs-outcome history therefore has to be **persisted by us** in
`data/history/`: footprint aggregates, not GRIB. AWS is the recovery route, not the system of record. Pre-50r1
files on AWS (before 2026-05-13) use `scda` and carry `cf` inside `enfo/ef`, so a backfill across that date needs both layouts.

## 4. Grid, sizes, `.index` format, byte ranges, rate limits

**Grid** [OBS, decoded]: `gridType=regular_ll`, `Ni=1440`, `Nj=721`, 0.25°, first point lat 90 / lon 180
(i.e. −180), last point lat −90 / lon 179.75. `packingType=grid_ccsds`, 16 bits per value. CCSDS needs ecCodes
built with libaec; the pip wheel handles it (decoded fine).

**File sizes** [OBS, HEAD `Content-Length`]:

| File | Size |
|---|---|
| oper `fc` one step (0 h / 144 h / 240 h / 360 h) | 136.5 / 145.6 / 144.9 / 141.3 MB |
| enfo `ef` one step (0 h / 144 h / 360 h) | 6.22 / 6.61 / 6.68 **GB** |
| enfo `ep` `240h` / `360h` file | 701.6 / 260.8 MB |
| oper `.index` (one step) | ~40 KB, 184–187 lines |

**Per-message sizes** (from `_length` in the index): see §1. A global field is 0.2–1.3 MB.

**`.index` format** [OBS + SRC]: JSON Lines, one record per GRIB message, e.g.
`{"domain": "g", "date": "20261005", "time": "0000", "expver": "0001", "class": "od", "type": "fc", "stream": "oper", "levtype": "sfc", "step": "240", "param": "sf", "_offset": 0, "_length": 445248}`.
Soil and pressure records add `levelist`; ENS records add `number`; `ep` records carry window steps like `"step": "0-24"`.
Served as `Content-Type: application/json` [OBS]. Docs: "start_bytes = _offset", "end_bytes = _offset + _length - 1" [SRC Confluence].

**Byte ranges** [OBS]: `Accept-Ranges: bytes`. `Range: bytes=0-99` on the 240 h grib2 returned **206**,
`Content-Range: bytes 0-99/144894884`, body starting `GRIB`. Range-fetching a single `tp`/`vsw` message
and decoding it worked end-to-end.

**Rate limits** [SRC]: "access to the open-data portal is limited to 500 simultaneous connections" (Confluence).
Portal access guide: "Each parallel connection counts as one session against your limit" (portal root page).
No per-day volume cap is published [NV, meaning none was found].

**Download budget per daily run** [INF, from index `_length`, global messages, one run]:

| Bundle (00z) | Messages | ≈ MB |
|---|---|---|
| `tp` at 24, 48 … 360 h (+ step 0 is free) | 15 | ~16 |
| `mx2t3/mn2t3` 3…144 h + `mx2t6/mn2t6` 150…360 h | 48×2 + 36×2 | ~110 |
| `vsw` 4 layers at daily steps | 60 | ~28 |
| `lsm` once | 1 | 0.2 |
| **Deterministic subtotal** | | **~155 MB** |
| ENS `ep` `tpg*` 7 thresholds × 29 windows | 203 | ~95 |
| ENS raw `tp`, 50 members × 15 daily steps | 750 | ~800 |

All of these are full-globe downloads; footprint cropping happens after decode. The deterministic + `ep` bundle
is ~250 MB/run, comfortable for a GitHub runner. Raw-member `tp` is the expensive option.

## 5. Ensemble (ENS) in open data

**Available: yes** [OBS]. `ifs/0p25/enfo/` at 00z/12z (to 360 h) and 06z (to 144 h).

- **`ef` files**: `type=pf`, `number` 1–50. Surface params match oper (`tp vsw mx2t3/6 mn2t3/6 ro ssrd sot 2t …`). Pressure levels: `d gh q r t u v vo w`. No control: since 50r1 the control is the `oper` run [SRC].
- **`ep` files** (00z/12z only) [OBS from `…-240h-enfo-ep.index`, `…-360h-enfo-ep.index`]:
  - `tpg1 tpg5 tpg10 tpg20 tpg25 tpg50 tpg100`: "the probability (in %) that total precipitation will be [X] mm or above" [SRC param-db 131060/131062]. **24 h windows every 12 h**: `0-24, 12-36, …, 216-240` (19) in the 240h file and `228-252 … 336-360` (10) in the 360h file.
  - `10fgg10/15/25` (gust probabilities), `ptsa_gt/lt_{1,1.5,2}stdev` at 850 hPa (temperature-anomaly probabilities, 12-hourly).
  - Ensemble mean/stdev (`em`/`es`) for `gh t ws` on pressure levels and `msl` only. **No `em/es` for `tp`, `2t`, `vsw`.**
- **Gaps [OBS]:** no multi-day accumulation probability (e.g. P[7-day rain ≥ 25 mm]) and no heat-probability product at 2 m. Those need computing from the 50 `pf` members ourselves (see the cost in §4).

**AIFS ENS** (data-driven model, also open) [OBS]: `aifs-ens/0p25/enfo/` has `cf` + `pf` 1–50 at **every** run
(00/06/12/18z) to 360 h, **6-hourly only**. `pf` carries `tp cp sf 2t ssrd vsw sot …` but **only soil layers 1–2 and no `mx2t*/mn2t*`**.
Its `ep` adds `tpl01 tprg3 tprg5 tprl1 2tl273 10spg10/15` alongside `tpg*`, plus `em/es` for `2t`.
Publication is earlier: AIFS-ENS 240 h `pf` at 07:20, AIFS-single at 06:55 UTC for 00z [OBS]; "AIFS data … released as soon the data are produced" [SRC datasets page].
[INF] It is a possible fallback, or an earlier source, for the rain probability. It cannot serve heat or deep soil moisture.

## 6. Directory layout (for the fetcher)

`https://data.ecmwf.int/forecasts/{yyyymmdd}/{HH}z/{model}/0p25/{stream}/{yyyymmdd}{HH}0000-{step}h-{stream}-{type}.{grib2|index}`

[SRC Confluence: "[ROOT]/[yyyymmdd]/[HH]z/[model]/[resol]/[stream]/[yyyymmdd][HH]0000-[step][U]-[stream]-[type].[format]"]

- model ∈ `ifs`, `aifs-single`, `aifs-ens`; stream ∈ `oper wave enfo waef`; type ∈ `fc ef ep` (IFS), `fc cf pf ep` (AIFS). [OBS]
- Each data folder carries `LICENCE.txt` ("The licence to the herein provided data can be found under https://apps.ecmwf.int/datasets/licences/general") and `README.txt` pointing to the datasets page and Confluence. [OBS]
- Cyclone tracks: `…-{360h|144h}-{stream}-tf.bufr` in every run folder [OBS]. Possibly relevant to the cyclone-flag half of map #356; out of R1's scope.

## 7. Licence and required wording

Source: <https://apps.ecmwf.int/datasets/licences/general/> (linked from the portal's "Terms of Use" and from every `LICENCE.txt`). Verbatim [SRC]:

> Terms of Use for ECMWF Open Data Products/Advanced Web Services, applied in addition to the Creative Commons CC-4.0-BY licence.
>
> ECMWF must be acknowledged (attributed) as the source. Attribution must be displayed prominently and include 'this document/data/output/Results is/are based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF)'. Users must remove attribution if requested by ECMWF.
>
> ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use.
>
> ECMWF makes no warranty as to the accuracy or completeness of its data products or the uninterrupted provision of such data products. All data products are provided on an "as is" basis. […]
>
> ECMWF shall not be liable should ECMWF discontinue the provision of its data products at any time.
>
> You are free to: Share […] Adapt — remix, transform, and build upon the material for any purpose, even commercially.
>
> The following wording shall be attached to the services created with this ECMWF data product:
> Copyright statement: Copyright "This service is based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF)".
> Source www.ecmwf.int
> Licence Statement: This ECMWF data is published under a Creative Commons Attribution 4.0 International (CC BY 4.0). https://creativecommons.org/licenses/by/4.0/
> Disclaimer: ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use.
> Where applicable, an indication if the material has been modified and an indication of previous modifications

(For direct data redistribution, as opposed to a service, the copyright line is instead `Copyright "© [year] European Centre for Medium-Range Weather Forecasts (ECMWF)"`.)

**What this means for the public site [INF]:** Mirror-Market is a *service created with* the data, so it should
render the **services** block: the statement, `www.ecmwf.int`, the CC BY 4.0 link, the disclaimer, and a
modification note such as "area-averaged over footprint polygons and converted to mm/°C by Mirror-Market".
"Displayed prominently" suggests a footer line on every page that shows footprint weather. Collapsing it into a
global credits page is weaker. **No commercial-use restriction** [SRC: "for any purpose, even commercially"]. Under invariant 9 this source is
**licence-clear to publish**.

**One clause to flag [NV, needs a human read]:** "In order to receive services related to the provision of
this dataset, users are required to sign a service agreement with ECMWF"
(<https://www.ecmwf.int/sites/default/files/2026-01/ecmwf-service-agreement.pdf>). Anonymous download needs no
account or agreement [OBS: no auth on any request]. [INF] The clause most plausibly covers *services
related to provision* (support, SLAs, push delivery), not anonymous download, but I have not read the
agreement PDF and I am not confident. Worth one look before go-live.

## 8. Python decoding path and CI install cost

Measured in a throwaway venv (macOS arm64, Python 3.14) [OBS]:

```
pip install eccodes cfgrib xarray ecmwf-opendata   → real 73 s (user 4 s: network-bound)
eccodes 2.49.0 · eccodeslib 2.49.0.30 (47 MB installed) · cfgrib 0.9.15.1 · ecmwf-opendata 0.3.34 · xarray 2026.9.0
```

- **`eccodes` (ECMWF's own binding) is enough.** Since the `eccodeslib` binary wheel, `pip install eccodes` ships the C library. PyPI lists `eccodeslib-2.49.0.30-cp3{11,12,13,14}-manylinux_2_28_x86_64.whl`, 9.1 MB each [OBS, PyPI JSON]. CI runs `ubuntu-latest` with Python 3.12 [OBS, `.github/workflows`], so this is a wheel install with no `apt-get libeccodes`. [INF] Expect ~10–20 s on a runner, plus pip cache.
- Proven path: fetch the `.index` → pick the record → HTTP Range → `eccodes.codes_new_from_message(bytes)` → `codes_get_values` (numpy array of 1,038,240 points). It needs no temporary files and no xarray (`decode.py` in the scratch run).
- `cfgrib`/`xarray` are optional conveniences, and heavy (pandas, xarray). [INF] Not needed for footprint averaging: a boolean mask over a 1440×721 array is enough.
- `pygrib` was not tested. [INF] It would need its own ecCodes linkage, so it adds nothing over `eccodes`.
- `ecmwf-opendata` (ECMWF's client) wraps index + Range + mirror choice (`source="ecmwf"|"aws"|"azure"|"google"`) [SRC README]. Its README predates 50r1 (§2), so pin a version and test against the live index.
- **Type-check note [INF]:** `eccodes` has no type stubs that I found. The repo's mypy perimeter would need an `ignore_missing_imports` entry for it.

## 9. Things that change map #356

1. **The ticket's `mx2t6` assumption is half-right.** Days 1–6 use `mx2t3/mn2t3`, days 7–15 use `mx2t6/mn2t6`. The spec must name both.
2. **`vsw` sea = 0, not NULL.** The footprint spec must land-mask (with `lsm`), or invariant 2 is broken in a way that hides itself.
3. **Retention is ~3½ days on the portal**, but the ECMWF-managed **AWS mirror holds history since 2023-01-18** (no stated guarantee). The "History and storage shape" fog shifts: we still persist our own aggregates (system of record), but a missed run is recoverable, and forecast-vs-outcome history can be backfilled once if wanted.
4. **The ensemble is real and cheap in its pre-computed form.** `tpg*` gives P[24 h rain ≥ 1/5/10/20/25/50/100 mm] to 360 h for ~95 MB/run. Multi-day or heat probabilities would need raw members (~800 MB/run for `tp` alone). The presentation ticket can offer the 24 h probability at no real cost.
5. **Pin the 00z run.** It is published 07:34 UTC and is the only run safely complete for the 19:00 UTC deploy with a 360 h horizon. 06z/18z stop at 144 h, and 12z races the deploy.
6. **The CI cost fog mostly clears.** `pip install eccodes` is a wheel with no system package. ~155–250 MB downloaded per run. This argues for the full run only, not `--fast` [INF].
7. **Licence is clear for public, commercial use.** The rendered attribution block is the "services" wording in §7. One service-agreement clause is flagged for a human read.
8. **Docs drift.** The Confluence step tables and the `ecmwf-opendata` README lag the live 50r1 layout. The fetcher should verify step presence from the live directory/index (invariant 1), not hard-code the documented tables.

## Sources

- Live portal root: <https://data.ecmwf.int/forecasts/> (listing + access guide, OpenECPDS 8.2.0)
- Live index (ticket example): <https://data.ecmwf.int/forecasts/20261005/00z/ifs/0p25/oper/20261005000000-240h-oper-fc.index>
- Live folders probed: `…/20261004/{00z,06z,12z,18z}/ifs/0p25/{oper,enfo}/`, `…/20261004/00z/{aifs-ens,aifs-single}/0p25/…`, `…/20261002/…`
- ECMWF Confluence: <https://confluence.ecmwf.int/display/DAC/ECMWF+open+data%3A+real-time+forecasts+from+IFS+and+AIFS> (last updated 2026-09-04)
- ECMWF datasets page: <https://www.ecmwf.int/en/forecasts/datasets/open-data>
- Licence / Terms of Use: <https://apps.ecmwf.int/datasets/licences/general/>
- ECMWF Parameter DB API: `https://codes.ecmwf.int/parameter-database/api/v1/param/{228,39,40,41,42,121,122,228026,228027,205,169,131060,131062}/?format=json`
- AWS registry: <https://registry.opendata.aws/ecmwf-forecasts/>; bucket listing <https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com/?list-type=2&delimiter=/>
- `ecmwf-opendata` README: <https://raw.githubusercontent.com/ecmwf/ecmwf-opendata/main/README.md>
- PyPI `eccodeslib` JSON: <https://pypi.org/pypi/eccodeslib/json>
