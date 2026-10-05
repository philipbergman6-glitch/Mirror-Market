# R2 — Global tropical-cyclone sources by basin + their alert conventions

**Issue:** [#358](https://github.com/philipbergman6-glitch/Mirror-Market/issues/358) (map #356, blocks #361)
**Date:** 2026-10-05 (probes run 13:00–14:30 UTC)
**Author:** research agent

Confidence legend (same as earlier `research/` notes): **[OBS]** seen in a live curl response today ·
**[SRC]** quoted from the agency's own page · **[INF]** inference from OBS/SRC · **[NV]** not verified.

---

## 0. Answer in one screen

- **NHC + JTWC cover every basin in scope except the South Atlantic.** Both are keyless US-government
  public information, both use **1-minute winds** and **34/50/64 kt quadrant radii** out to 120 h. So
  together they give one severity vocabulary for the whole globe.
- **Every non-US RSMC/TCWC except JMA is blocked for publishing** by its own terms:
  CMA, IMD, BoM, Fiji, MetService NZ, and Météo-France's website. That makes them invariant 9 territory:
  fine to build with, not to render.
- **The South Atlantic has no publishable machine-readable source.** That covers Santos, Paranaguá and
  Up-River. No RSMC exists there. The Brazilian Navy (CHM) site sits behind Cloudflare (403), and
  GDACS, JTWC, IBTrACS and SWIC all miss recent South Atlantic storms. **This changes the map**:
  the flag must withhold there with a stated reason, not render "no storm".
- **GDACS** is a keyless global aggregator of NOAA, JTWC and RSMC La Réunion, licensed CC BY 4.0, and
  serves GeoJSON with wind swaths. It is a good **cross-check and fallback**, not the primary:
  - it lags about 5 h;
  - its terms say it "should not be used for decision making without prior confirmation";
  - its green/orange/red levels measure **population impact**, not port hazard.

**Recommended minimal set:** **NHC** (`CurrentStorms.json` + `_latest` GIS) for the Atlantic and
East/Central Pacific, plus **JTWC** (RSS → `.tcw`/`web.txt`) for everything else. Add **GDACS** as an
optional cross-check. Withhold the **South Atlantic**. Details in §3.

---

## 1. Basin × source matrix

Ports and footprints come from ticket #358. Primary RSMC = the WMO-designated official centre.
✅ = machine-readable, keyless and publishable. ⚠️ = machine-readable but licence unclear or the URL is
fragile. ⛔ = licence-blocked or not accessible. — = does not cover the basin.

| Basin (rendered ports/footprints) | Official RSMC / TCWC | NHC/CPHC | JTWC | JMA | GDACS | Other |
|---|---|---|---|---|---|---|
| **North Atlantic** (US Gulf; Europe via post-tropical; Nigeria ≈ nil climatology) | NHC Miami | ✅ primary | — | — | ✅ (src NOAA) | NWS CAP ✅ (US zones only) |
| **East / Central Pacific** (no rendered port directly; PNW gets only extratropical remnants) | NHC / CPHC Honolulu | ✅ primary | ✅ duplicate (drop) | — | ✅ (src NOAA) | — |
| **West Pacific + South China Sea** (Dalian/Qingdao/Rizhao; Malaysia palm) | JMA RSMC Tokyo (national: CMA) | — | ✅ | ✅ CC BY 4.0-compatible | ✅ (src JTWC) | CMA ⛔ |
| **North Indian Ocean** (Indian west-coast ports) | IMD RSMC New Delhi | — | ✅ | — | ✅ (src JTWC) | IMD ⛔ |
| **South-West Indian Ocean** (Richards Bay/Durban, Mozambique Channel) | Météo-France RSMC La Réunion | — | ✅ | — | ✅ (src RSMC + JTWC) | MF website ⛔; WTIO30 via NOAA mirror ⚠️ |
| **Australian region** (Indonesia southern edge) | BoM TCWCs Perth/Darwin/Brisbane (+ Jakarta) | — | ✅ | — | ✅ (src JTWC) | BoM ⛔ |
| **South Pacific** (no rendered port) | Fiji RSMC Nadi (+ TCWC Wellington) | — | ✅ | — | ✅ (src JTWC) | Fiji ⛔, MetService ⛔ |
| **South Atlantic** (Santos, Paranaguá, Up-River) | **none** (Brazilian Navy CHM names storms) | — | — | — | **misses** | CHM ⛔ (403); METAREA V text ⚠️ |

GDACS source column **[OBS]**: I pulled
`gdacsapi/api/events/geteventlist/SEARCH?eventlist=TC&fromdate=2025-10-01&todate=2026-10-05&alertlevel=green;orange;red`
(100 features, which may be a page cap) and bucketed them by centroid:

| Basin | Source | Events |
|---|---|---|
| North Atlantic | NOAA | 14 |
| East Pacific | NOAA | 20 |
| West Pacific | JTWC | 34 |
| North Indian Ocean | JTWC | 3 |
| South-West Indian Ocean | RSMC | 8 |
| South-West Indian Ocean | JTWC | 2 |
| Australian region | JTWC | 12 |
| South Pacific | JTWC | 6 |

There were **0** South Atlantic events. My basin bucketing is a rough longitude/latitude split **[INF]**.

### Per-source attribute matrix

| Source | Format (endpoint) | Track / horizon | Wind radii | Wind avg | Cadence / delay | Keyless | Licence for a public site | URL stability |
|---|---|---|---|---|---|---|---|---|
| **NHC/CPHC** | JSON index, shapefile/KMZ, ArcGIS REST→GeoJSON, RSS, TCM text | 0–120 h (12…120) | 34/50/64 kt quadrants (64 kt to 72 h) | 1-min | 03/09/15/21 UTC; files posted ≈ H−20 min (09Z advisory up 08:37–08:41) | ✅ | Public domain, with attribution and non-modification conditions | Fixed index + `_latest` aliases |
| **JTWC** | RSS index, WTPN text, `.tcw` (JMV 3.0), KMZ | to 120 h | 34/50/64 kt quadrants, every tau | 1-min | NH 6-hourly; SH 12-hourly; ≤3 h after synoptic (≤4 h for East Pacific) | ✅ | "Public information… may be freely distributed" | Per-storm filename, overwritten; missing file = 403 |
| **JMA** | bosai JSON, JMA XML Atom, CAP (experimental) | 0–120 h (12/24/48/72/96/120) + 70 % circle | 30 kt (analysis) / 50 kt (to 96 h) | 10-min | 3-hourly; forecast 00/06/12/18; <1 h | ✅ | Public Data Licence 1.0 = CC BY 4.0-compatible, with attribution | bosai fixed but **unofficial**; XML resolve-from-feed |
| **GDACS** | GeoJSON API, RSS, CAP | Inherits upstream (≈120 h) + cone | 60/90/120 km/h swath polygons | 1-min (Saffir-Simpson-equivalent) | Per advisory; ~5 h lag | ✅ | CC BY 4.0 (CAP header) + strong disclaimer | Fixed; event/episode IDs stable |
| **CMA/NMC** | JSONP (`typhoon.nmc.cn`) | to 120 h | 30/50/64 kt (in km) | 2-min [secondary] | 3-hourly; ~1.3 h | ✅ | ⛔ "未经授权禁止下载使用" | Opaque ID, resolve each run |
| **IMD** | Rotating PDFs; empty GeoJSON layers; CAP (no TC items seen) | to 120 h | 28/34/50/64 kt | **3-min** | 3-hourly from CS stage | ✅ | ⛔ "may not be reproduced… without due permission" | Rotating hash URLs |
| **Météo-France La Réunion** | Website = internal token API (401); text via NOAA GTS mirror | ≥72 h (old doc), longer now [NV] | 28/34/48(/64) kt | 10-min | 00/06/12/18 UTC, +30 min | API ⛔ / mirror ✅ | Website ⛔; WTIO30 = WMO "core" warning ⚠️ | Mirror URL fixed |
| **BoM** | Anon FTP: CXML, GML, CAP, text | 72 h track map; bulletin 6-hourly | 34/48/64 kt [secondary] | 10-min | 3–6-hourly | ✅ | ⛔ "may not supply… to any other person or use it for any commercial purpose" | Fixed product IDs (slot-based) |
| **Fiji RSMC Nadi** | RSS (empty), JS-rendered maps, rotating PDFs, GTS text | 12/24 h + 36/48 h outlook | ">33 kt within N nm" by quadrant | 10-min | 6-hourly (3-hourly if Fiji threatened) | ✅ | ⛔ "All Rights Reserved", no terms page | Rotating |
| **Brazil CHM** | HTML + SVG; METAREA V text via NOAA mirror | none structured | none structured | — | — | site 403 | No licence text found | Rotating on WMO WWMIWS |
| **WMO SWIC** | Undocumented JSON (`/json/tc_inforce.json`) | track + forecast points | `wind_radii: null` (NHC storm) | per-agency | ~live | ✅ | "free for re-use by media or other websites", with attribution to issuing RSMC | **Undocumented**, stability unknown |
| **IBTrACS ACTIVE** | CSV | **no forecast** | — | mixed | 3×/week; newest obs 4.5 days old | ✅ | WDC open access; "not to be used for decisions… in real time" | Fixed |

---

## 2. Per-source evidence

### 2.1 NOAA NHC / CPHC (North Atlantic, East and Central Pacific)

**Endpoints [OBS]** (all keyless):
- `https://www.nhc.noaa.gov/CurrentStorms.json` → 200 `application/json`, 4.6 KB. Lists `ep182026` (Rachel, 80 kt).
  - Each storm carries `forecastTrack`, `trackCone`, `forecastWindRadiiGIS`, `windSpeedProbabilitiesGIS`,
    `windWatchesWarnings`, each with `advNum` and `issuance`.
  - Today there is no Atlantic storm. The outlook gives a SW Gulf system "Formation chance through 7 days...high...70 percent".
- RSS: `https://www.nhc.noaa.gov/index-at.xml`, `index-ep.xml`, `index-cp.xml`, `gtwo.xml` → all 200.
- GIS: per-advisory `https://www.nhc.noaa.gov/gis/forecast/archive/ep182026_5day_033.zip`. The fixed alias
  `ep182026_5day_latest.zip` returns the identical file, and the same goes for `_fcst_latest.zip`,
  `EP182026_CONE_latest.kmz` and `EP182026_WW_latest.kmz`.
- ArcGIS REST: `https://mapservices.weather.noaa.gov/tropical/rest/services/tropical/NHC_tropical_weather/MapServer?f=json`
  → 200, 400 layers in fixed slots AT1–5/EP1–5/CP1–5.
  - Layer 188 `query?...&f=geojson` returned 9 forecast points, tau 0–120.
  - Layer 198 (forecast wind radii) returned 23 features with `radii` 34/50/64 and `ne/se/sw/nw`.
- **Traps [OBS]:**
  - The JSON reference PDF ([NHC_Tropical_Cyclone_Status_JSON_File_Reference.pdf](https://www.nhc.noaa.gov/productexamples/NHC_Tropical_Cyclone_Status_JSON_File_Reference.pdf))
    has drifted from the live file. The live file uses `latitudeNumeric`, not `latitude_numeric`, and
    `intensity` is the **string** `"80"`, not a number. Parse defensively and hard-fail on shape change.
  - MapServer `lat`/`lon` attributes are rounded to whole degrees, so use the geometry.
  - `gis/kml/nhc_active.kml` is "an experimental product".

**Stability [SRC]:** "The JSON file will have a static (unchanging) file name… If there are no active storms, the
value of this property will be an empty array." (JSON reference PDF above). An empty array is a valid
"asked, got nothing" answer (invariant 2: 0, not NULL).

**Cadence [SRC]** ([aboutnhcprod](https://www.nhc.noaa.gov/aboutnhcprod.shtml)):
- "Forecast/Advisories are issued… every six hours at 0300, 0900, 1500, and 2100 UTC."
- "Intermediate public advisories are issued every 3 hours when coastal watches or warnings are in effect."

**Horizon and radii [SRC]:** "valid 12, 24, 36, 48, 60, 72, 96, and 120 h… Tropical storm and 50-kt wind radii are
forecast out to 120 h and hurricane-force wind radii are forecast out 72 h."

**Severity conventions [SRC]:**
- Saffir-Simpson ([aboutsshws](https://www.nhc.noaa.gov/aboutsshws.php)):

  | Cat 1 | Cat 2 | Cat 3 | Cat 4 | Cat 5 |
  |---|---|---|---|---|
  | 64–82 kt | 83–95 kt | 96–112 kt | 113–136 kt | ≥137 kt |

- Watches and warnings ([glossary](https://www.nhc.noaa.gov/aboutgloss.shtml)):
  - "Hurricane Warning: … sustained winds of 64 knots… or higher are expected somewhere within the specified area… issued 36 hours in advance"
  - "Tropical Storm Warning: … 34 to 63 knots… expected… within 36 hours"
- Classification codes [OBS]: HU/TS/TD/STS/STD/PTC/PC.
- Wind [SRC]: "the highest one-minute average wind".

**Licence [SRC]:**
- [NHC disclaimer](https://www.nhc.noaa.gov/disclaimer.shtml): "The information on government servers are in the public domain, unless specifically annotated otherwise, and may be used freely by the public. This information shall not be modified in content and then presented as offical government material."
- [NWS disclaimer](https://www.weather.gov/disclaimer): "may be used without charge for any lawful purpose so long as you do not: 1) claim it is your own… 2) use it in a manner that implies an endorsement or affiliation with NOAA/NWS, or 3) modify its content and then present it as official government material." It also requires a 17 U.S.C. § 403 notice when a work consists "predominantly" of NWS material.
- The shapefile metadata [OBS] adds: "Acknowledgement of… the National Hurricane Center would be appreciated."

**NWS CAP / `api.weather.gov` [OBS]:**
- `https://api.weather.gov/alerts/active?event=Hurricane%20Warning` → 200 `application/geo+json`, `features: []`.
- It covers US jurisdictions only. [SRC] ([API docs](https://www.weather.gov/documentation/services-web-api)): "A User Agent is required…"
- It is useful only for an official US Gulf watch/warning flag.

**ATCF decks [OBS/SRC]:** `https://ftp.nhc.noaa.gov/atcf/` is 200, but it is a cross-check only.
- The [README](https://ftp.nhc.noaa.gov/atcf/README) says: "users of the data are REQUIRED to perform quality control… can differ from information issued in official NHC products"
- The [NOTICE](https://ftp.nhc.noaa.gov/atcf/NOTICE) says the a-deck ECMWF data is CC BY 4.0, so the decks are not wholly public domain.

### 2.2 JTWC (West Pacific, North Indian Ocean, all Southern Hemisphere; duplicates East/Central Pacific)

**Endpoints [OBS]** (all keyless):
- `https://www.metoc.navy.mil/jtwc/rss/jtwc.rss` → 200 `application/rss+xml`. This is the stable index, and it links every product for every active storm.
- Per storm:
  - `…/jtwc/products/wp2626web.txt`: WTPN32 text, 120 h track, 34/50/64 kt quadrants.
  - `wp2626.tcw`: JMV 3.0 machine format, lines like `T012 300N 1461E 100 R064 060 NE QD …`.
  - `wp2626.kmz`: 448 KB, includes a "34 knot Danger Swath".
- Outlooks: `abpwweb.txt` (West/South Pacific) and `abioweb.txt` (Indian Ocean).
- Active today: Choi-wan 26W (110 kt), Koguma 27W, Nolo 15E.

**Traps [OBS]:**
- The HTML pages return 403 to a short User-Agent and 200 to a browser one. The products return 200 either way.
- **A missing file returns 403 S3 `AccessDenied`, not 404.** So resolve filenames from the RSS and treat 403 as
  "no such product", not as being blocked. This is invariant 1: 403 must map to a distinct state.
- RSS `<link>`s point to `s3.amazonaws.com/www.metoc.navy.mil/…`, which returns 403.

**Cadence [SRC]** ([notices](https://www.metoc.navy.mil/jtwc/html/notices.html), [FAQ](https://www.metoc.navy.mil/jtwc/html/faq.html)):
- "WTPN31-36 PGTW Tropical Cyclone Warning, Western North Pacific Ocean Issued at 0300, 0900, 1500 and 2100 GMT"
- "products are transmitted no later than 3 hours past the synoptic hour"
- "South Indian and South Pacific Ocean tropical cyclone warnings are routinely updated every twelve hours"

**Radii [SRC] (FAQ):** "Wind radii accompany the current analysis and all forecast positions through the entire forecast period (maximum 120 hours)… valid only over water."

**Severity conventions [SRC] (FAQ):**
- "less than 34 knots… 'Tropical Depression'… between 34 and 63 knots… 'Tropical Storm'… between 64 and 129 knots is called a 'Typhoon,'… 130 knots or greater… 'Super Typhoon.' In the Indian Ocean and South Pacific, JTWC labels ALL tropical cyclones as 'Tropical Cyclone'"
- A Tropical Cyclone Formation Alert (TCFA) means the system is "likely to become the subject of a JTWC tropical cyclone warning within the following 24 hour period."
- Wind [OBS] in the bulletin: "MAX SUSTAINED WINDS BASED ON ONE-MINUTE AVERAGE".
- TCCOR is set by installation commanders, not by JTWC (see [CFA Sasebo](https://cnrj.cnic.navy.mil/Installations/CFA-Sasebo/Departments/Emergency-Management/Tropical-Cyclone-Conditions-of-Readiness/)). Do not use it as a scale.

**Licence [SRC]** ([notices](https://www.metoc.navy.mil/jtwc/html/notices.html)):
> "This is a U.S. government website. JTWC products on this website are intended for use by U.S. government agencies. Please consult your national meteorological agency or the appropriate World Meteorological Organization Regional Specialized Meteorological Center for tropical cyclone products pertinent to your country, region and/or local area. The information on this site is considered public information unless specifically annotated and may be freely distributed or copied. This information should not be modified in content and then presented as copyrighted material. As required by 17 U.S. Code section 403, third parties producing works consisting predominantly of the material appearing in JTWC web sites must provide notice…"

**[INF] Authority caveat:** JTWC is DoD guidance, not the WMO RSMC for any basin. A flag sourced from it
must be labelled as "JTWC" guidance, never as "the official warning" for Chinese, Indian, South African
or Australian ports. JTWC storm numbers can differ from RSMC numbers [SRC, FAQ].

### 2.3 JMA RSMC Tokyo (West Pacific + South China Sea)

**Endpoints [OBS]** (all keyless):
- `https://www.jma.go.jp/bosai/typhoon/data/targetTc.json` → 200 JSON index (`TC2633`…`TC2635`).
- Per storm: `…/data/<TCID>/forecast.json` and `specifications.json`. Hosted on S3, `max-age=60`, CORS `*`.
- `https://www.data.jma.go.jp/developer/xml/feed/extra.xml`: the supported JMA XML Atom feed. Its data
  file names rotate, so resolve them from the feed (invariant 10).
- `https://www.data.jma.go.jp/cap-rsmctk/atom.xml` (CAP 1.2, English). [SRC]: "NOT intended for operational use", "[Experimental]".
- **[NV]** The bosai JSON is not a documented, supported API. JMA states URLs may change without notice ([coment.html](https://www.jma.go.jp/jma/kishou/info/coment.html)).

**Cadence and content [SRC]** ([advisory.html](https://www.jma.go.jp/jma/jma-eng/jma-center/rsmc-hp-pub-eg/advisory.html)):
- "Information is issued every three hours. Advisories at 0000, 0600, 1200 and 1800 UTC contain 24-…120-hour forecasts."
- Content includes "Maximum sustained wind speed (10-minute average)" and "Radii of wind areas over 50 and 30 knots".

**Delay [OBS]:** the 12 UTC fix appeared in CAP at 12:46 and in bosai at 12:50–13:00.

**Severity [SRC]** ([1-3.html](https://www.jma.go.jp/jma/kishou/know/typhoon/1-3.html)):
- Strong: 64–<85 kt. Very strong: 85–<105 kt. Violent: ≥105 kt.
- Size by radius of winds ≥15 m/s: large 500–800 km, very large ≥800 km.

**Licence [SRC]** ([copyright.html](https://www.jma.go.jp/jma/en/copyright.html)):
- "may be used in accordance with the terms and conditions of use stipulated in Public Data License (Version 1.0)"
- "The user must cite the source… Source: Japan Meteorological Agency website (URL…)"
- "The Terms of Use are compatible with the Creative Commons Attribution License 4.0"

**Caveat [SRC]** ([RSMC_HP.htm](https://www.jma.go.jp/jma/jma-eng/jma-center/rsmc-hp-pub-eg/RSMC_HP.htm)):
"information issued by the RSMC Tokyo - Typhoon Center represents neither official analysis/forecasts nor
warnings for the areas concerned." For Dalian, Qingdao and Rizhao the national warning authority is CMA, which is blocked (§2.5).

### 2.4 GDACS (JRC / UN OCHA aggregator)

**Endpoints [OBS]** (all keyless, fixed):
- `https://www.gdacs.org/gdacsapi/api/events/geteventlist/MAP?eventtype=TC` → 200 GeoJSON, 1.5 MB.
  - Contents: track, **forecast** points, `Poly_Cones`, and wind-swath polygons `Poly_Green` "60 km/h",
    `Poly_Orange` "90 km/h", `Poly_Red` "120 km/h".
- `…/geteventlist/SEARCH?eventlist=TC`: without `alertlevel` it returns only Orange and Red events. Pass `alertlevel=green;orange;red`.
- `https://www.gdacs.org/xml/gdacs_cap.xml` → 200, 5.7 MB. `https://www.gdacs.org/xml/rss.xml` → 200.

**Upstream [SRC]** ([models_TC.aspx](https://www.gdacs.org/Knowledge/models_TC.aspx)): "includes the TC bulletins
produced by… (NOAA) and the Joint Typhoon Warning Center (JTWC) into a single database". [OBS] South-West
Indian Ocean events carry `source:"RSMC"` (La Réunion), with JTWC listed as a comparison row.

**Lag [INF from OBS]:** Rachel's 09Z advisory showed up with `datemodified` 13:58Z, so about 5 h.

**Alert levels [SRC]** (same page):
- "using 1-min sustained winds", graded by Saffir-Simpson category.
- The level comes from a matrix of category × exposed population × vulnerability. For example, Red = Cat 3 with >100K people or >10 % of the population, at Medium–High vulnerability.
- "GDACS RED levels forecasted more than 3 days in advance are reduced to Orange levels."
- **[INF]** A Cat 4 over open sea stays Green. Do not use `alertlevel` as a port hazard; use the swaths and radii.

**Licence [SRC]**, three statements:
- [API quickstart](https://www.gdacs.org/Documents/2025/GDACS_API_quickstart_v1.pdf): "We only request to acknowledge the source as "Global Disaster Alert and Coordination System, GDACS"".
- CAP channel [OBS]: "Licensed under CC BY 4.0." RSS channel: "public domain".
- [Terms of use](https://www.gdacs.org/documents/2025/GDACS_Terms_of_use_Mar_25.pdf): "automatic, produced by algorithms and not reviewed by human experts… should not be used for decision making without prior confirmation of their validity."

### 2.5 CMA / National Meteorological Centre (national authority for Chinese ports) — ⛔

**Endpoints [OBS]** (keyless):
- `https://typhoon.nmc.cn/weatherservice/typhoon/jsons/list_default` → JSONP. Per storm `view_<id>` gives a
  3-hourly track, SuperTY…TD grades, 30/50/64 kt radii (in km), and BABJ forecasts to 120 h.
- `tcdata.typhoon.org.cn` returned HTTP 468.

**Licence [SRC]:**
- Footer: "本站所刊登的信息、数据和各种专栏材料，未经授权禁止下载使用" (information, data and materials on this site may not be downloaded or used without authorisation).
- [nmc.cn statement](http://www.nmc.cn/publish/cms/view/722665831f0a4e98800c41e691444963.html): without written authorisation, no third party may use any content "以任何方式（包括但不限于转载、链接、转帖或复制发表）用于商业" (in any way, including reposting, linking, forwarding or copying, for commercial purposes).
- [Regulation Art. 7](https://www.cma.gov.cn/gzk/202005/t20200528_1694399.html): "其他任何组织或者个人不得向社会发布预警信号" (no other organisation or person may issue warning signals to the public).

**Severity [SRC]** (GB/T 19201-2006, [cma.gov.cn](https://www.cma.gov.cn/2011xzt/20100728/2010072806/201007/t20100706_3092914.html)):

| TD | TS | STS | TY | STY | SuperTY |
|---|---|---|---|---|---|
| 10.8–17.1 m/s | 17.2–24.4 | 24.5–32.6 | 32.7–41.4 | 41.5–50.9 | ≥51.0 m/s |

- Warning signals run blue/yellow/orange/red. Only the blue text was verified verbatim
  ([page](https://www.cma.gov.cn/ztbd/2022zt/2022tf/yjxh/202209/t20220902_5070001.html)).
- Wind averaging is 2-min, from a secondary source ([Ying et al. 2014](https://journals.ametsoc.org/view/journals/atot/31/2/jtech-d-12-00119_1.pdf)).

**[INF]** Blocked for publishing. Re-displaying CMA warning signals is legally restricted in China. I'm not
confident how that applies to a site hosted outside China, but it is moot because the site terms already forbid it.

### 2.6 IMD RSMC New Delhi (North Indian Ocean; Indian west-coast ports) — ⛔

**Endpoints [OBS]:**
- `https://rsmcnewdelhi.imd.gov.in/` → 200 HTML. Its bulletin PDFs have random-hash, rotating names (invariant 10).
  - Today's Tropical Weather Outlook gives cyclogenesis probability "NIL" for every window out to 168 h.
- `https://dss.imd.gov.in/dwr_img/GIS/tout34.geojson` (and the 27/50/64 variants) → 200, **0 bytes**. These are undocumented internal map layers.
- CAP feeds `cap-sources.s3.amazonaws.com/in-imd-en/rss.xml` and the NDMA SACHET feed: live, but no TC items today.
  Their "public domain" tag looks like a hub template **[INF]**.

**Horizon and radii [SRC]** ([bulletins-products.php](https://rsmcnewdelhi.imd.gov.in/bulletins-products.php)):
- Forecasts at "+06… +120 hours… from the stage of deep depression onwards".
- Radii at R28/R34/R50/R64.

**Wind [SRC]** ([terminology.pdf](https://rsmcnewdelhi.imd.gov.in/images/pdf/terminology.pdf)): "Highest 3 minutes surface wind".

**Severity [SRC]** (same PDF):

| D | DD | CS | SCS | VSCS | ESCS | SuCS |
|---|---|---|---|---|---|---|
| 17–27 kt | 28–33 | 34–47 | 48–63 | 64–89 | 90–119 | ≥120 kt |

- Warning stages ([four-stage-warning.php](https://rsmcnewdelhi.imd.gov.in/four-stage-warning.php)): Pre-Cyclone Watch, then "Cyclone Alert — Yellow", "Cyclone Warning — Orange", "Post landfall out look — Red".
- **Port warning signals I–XI** ([port-warning.pdf](https://rsmcnewdelhi.imd.gov.in/images/pdf/port-warning.pdf)):
  - DC1: "Depression far at sea. Port NOT affected."
  - LW4: "Cyclone at sea. Likely to affect the port later."
  - D5–D7 Danger and GD8–GD10 Great Danger.
  - XI: communication failure.
  - These are the most port-relevant convention found anywhere, but I found no machine-readable per-port feed **[INF]**.

**Licence [SRC]** ([copyright-policy.php](https://rsmcnewdelhi.imd.gov.in/copyright-policy.php)):
> "This contents of this website may not be reproduced partially or fully, without due permission from Regional Specialised Meteorological Centre, Govt. of India."

### 2.7 Météo-France RSMC La Réunion (South-West Indian Ocean) — website ⛔, GTS text ⚠️

**Endpoints [OBS]:**
- `http://www.meteo.fr/temps/domtom/La_Reunion/webcmrs9.0/`: the real-time page is just a PNG.
- `https://meteofrance.re/fr/cyclone` runs on an internal API: `webservice.meteofrance.com/cyclone/list?basin=SWI` → **401** "you must provide a token".
- The Météo-France API portal is key-gated, and no cyclone API is listed.
- **Fixed-URL GTS mirror:** `https://tgftp.nws.noaa.gov/data/raw/wt/wtio30.fmee..txt` → 200 text, keyless.
  - The latest file is `WTIO30 FMEE 051305` of 2026-04-05; the basin is out of season.
  - Content: "MAX AVERAGE WIND SPEED (10 MN)" and 28/34/48 kt quadrant radii.

**Cadence [SRC]** ([aide_lecture](http://www.meteo.fr/temps/domtom/La_Reunion/webcmrs9.0/francais/activiteope/aide_lecture/cmrs.html), 2006-era):
"quatre fois par jour à 0000, 0600, 1200, 1800 UTC" (four times a day at 0000, 0600, 1200, 1800 UTC).

**Severity [SRC]** ([classif_syst.html](http://www.meteo.fr/temps/domtom/La_Reunion/webcmrs9.0/francais/cmrs/classif_syst.html)):
- Perturbation tropicale: ≤27 kt
- Dépression tropicale: 28–33 kt
- Tempête tropicale modérée: 34–47 kt
- Forte tempête tropicale: 48–63 kt
- Cyclone tropical: 64–89 kt
- Cyclone tropical intense: 90–115 kt
- Cyclone tropical très intense: >115 kt
- Winds are "moyennée sur 10 minutes" (averaged over 10 minutes).

**Licence:**
- [SRC] [meteofrance.com/droits-de-reproduction](https://meteofrance.com/droits-de-reproduction), Art. 2: "est interdite : Toute utilisation de l'un des éléments des sites Internet dans un environnement informatique en réseau ; … L'extraction répétée et systématique" (forbidden: any use of the site's content in a networked computing environment, and repeated, systematic extraction).
- **⚠️ WTIO30 text [INF/NV]:** it carries no licence text. WMO Res. 40 Annex I lists "Severe weather warnings and
  advisories for the protection of life and property" as exchanged "without charge and with no conditions on use"
  ([Res 40 Annex I](https://community.wmo.int/res-40-annex-i)). **I am not confident** that this grants a third-party
  website a right to republish. It governs exchange between WMO Members. Treat it as needing a licence check.
- The data.gouv.fr SWIO best-track history is under Licence Ouverte 2.0, but it is history only.

### 2.8 BoM (Australian region) — ⛔

**Endpoints [OBS]:**
- `ftp://ftp.bom.gov.au/anon/gen/fwo/` → 226, keyless. No TC products today (out of season).
- Product IDs come from `https://www.bom.gov.au/catalogue/data/SMSRPR09.json`:
  - technical bulletins IDW27600, IDD20020, IDQ20018…
  - CXML IDW60350…
  - track GML IDW60266…
  - CAP IDW24500…
- Correction to the ticket: IDW60280 is a track-map graphic, not the bulletin.
- HTTP returns 403 to the default curl User-Agent.

**Format and cadence [SRC]:**
- 10-min winds.
- Categories 1–5 ([categories](https://www.bom.gov.au/resources/learn-and-explore/tropical-cyclone-knowledge-centre/tropical-cyclone-categories)):
  - Cat 1: 63–88 km/h
  - Cat 3 ("severe"): 118–159 km/h
  - Cat 5: >200 km/h
- Technical bulletin "issued once every 6 hours"; track map covers "next 72 hours".
- The 34/48/64 kt radii come from a secondary repost only **[NV]**.

**Licence [SRC]:**
- [anon-ftp.shtml](https://www.bom.gov.au/catalogue/anon-ftp.shtml): "you may download, use and copy that material for personal use, or use within your organisation but you may not supply that material to any other person or use it for any commercial purpose. Users intending to publish Bureau data should do so as Registered Users."
- [copyright](https://www.bom.gov.au/copyright): "we do not allow you to use automated or manual techniques to hack, scrape or otherwise extract material from our site".

### 2.9 Fiji Meteorological Service RSMC Nadi (South Pacific) — ⛔

**Endpoints [OBS]:**
- `https://www.met.gov.fj/feed/warnings/` → 200 RSS with no items.
- The track and threat maps are JS-rendered.
- Outlook PDFs have rotating suffixes. The last one, 30 Apr 2026, reads "final Tropical Cyclone Outlook for this season".
- GTS text `tgftp…/wt/wtps1x.nffn` was last updated April 2026.

**Conventions [SRC]** ([track-map guide](https://www.met.gov.fj/documents/28169/How_to_Read_the_FMS_Tropical_Cyclone_Track_Map_Final.pdf)):
"uses the Australian Tropical Cyclone Intensity Scale", "uses 10-minute wind average".

**Licence [OBS]:**
- Footer: "© 2026 Fiji Meteorological Services. All Rights Reserved."
- No terms page exists (/terms, /copyright and /disclaimer all return 404).

**MetService NZ (TCWC Wellington), south of 25° S:** commercial use "except for open access data, is not permitted"
([web-terms-of-use](https://about.metservice.com/web-terms-of-use); page truncated, **[NV]** verbatim).

### 2.10 South Atlantic (Santos, Paranaguá, Up-River Argentina) — no source

**No WMO RSMC [SRC].** The Brazilian Navy's CHM names storms. For example, its 2026-03-02 notice named subtropical
storm "Caiobá" ([PDF](https://assets.marinha.mil.br/dhn/sites/www.marinha.mil.br.dhn/files/notasaimprensa/Nota_Imprensa_CHM_02MAR_RJ_TEMPESTADE_SUBTROPICAL.pdf)).

**[OBS] Access:**
- `https://www.marinha.mil.br/chm/` → **403** (Cloudflare challenge), even with a browser User-Agent. I re-checked this at 14:25 UTC.
- The cyclone-monitoring page also returns 403. The warnings page is HTML/SVG only, with no licence text.

**[OBS] Text relay:** `https://tgftp.nws.noaa.gov/data/raw/ww/wwst02.sbbr..txt` → 200.
- It holds METAREA V gale and rough-sea warnings in free text, not structured cyclone data.
- WWMIWS notes [SRC]: "Please refer to the respective NMHS… as the authoritative source."

**[OBS] Coverage gaps:**
- GDACS SEARCH has no AKARÁ (Feb 2024) and no CAIOBÁ (Feb–Mar 2026).
- IBTrACS ACTIVE has 0 South Atlantic rows.
- SWIC's centre list has no Brazil entry.
- JTWC adds names only "after the WMO-designated RSMC or TCWC names a cyclone" ([FAQ](https://www.metoc.navy.mil/jtwc/jtwc.html?faq=)).

**[NV]** A Wikipedia claim says CHM's cyclone monitoring moves to DECEA in Dec 2026. There is no primary confirmation.

### 2.11 Aggregators rejected as primary

- **WMO SWIC [OBS/SRC]:** `https://severeweather.wmo.int/json/tc_inforce.json` → 200.
  - This path is undocumented; it was found in the site JS, and `v2/json/` returns 403.
  - It gives `wind_radii: null` for the NHC storm.
  - Terms ([note.html](https://severeweather.wmo.int/note.html)): "free for re-use by media or other websites… it should be indicated that they are issued by the respective Tropical Cyclone RSMCs, TCWCs or NMHSs."
  - **[INF]** This is the only route that relays **official RSMC** advisories under explicit re-use terms. But the
    endpoint is undocumented and the radii are missing, so it is a candidate cross-check, not a foundation.
- **IBTrACS ACTIVE [OBS]:** the newest observation is 2026-10-01 00Z, about 4.5 days old, with no forecasts.
  Maintainer ([ibtracs-qa](https://groups.google.com/g/ibtracs-qa/c/oXYPQVXBTfA)): "IBTrACS is not to be used for decisions of life and property in real time." Use it for climatology only.
- **NOAA GTS mirror `tgftp.nws.noaa.gov/data/raw/wt/` [OBS]:** a keyless mirror of raw bulletins from RJTD, DEMS,
  FMEE, ABRF/ADRM/APRF, NFFN, NZKL and BABJ.
  - **Staleness trap:** old files persist indefinitely (e.g. `wtfj11.nffn` from 2010). The issue time must be
    parsed and an age budget enforced (invariant 10).
  - Licence is the issuing agency's, not NOAA's **[INF]**.

---

## 3. Recommendation: minimal source set with full coverage

| Role | Source | Basins | Why |
|---|---|---|---|
| **Primary A** | NHC `CurrentStorms.json` → `_latest` GIS (5-day track, `fcst` radii, cone, WW) | North Atlantic, East/Central Pacific | Public domain, keyless, fixed index. Official RSMC for these basins and source of official US watches/warnings. |
| **Primary B** | JTWC `jtwc.rss` → `<id>.tcw` (+ `web.txt`) | West Pacific, North Indian Ocean, South-West Indian Ocean, Australian region, South Pacific | Public information, keyless. Same 1-min / 34/50/64 kt / 120 h convention as NHC. Drop JTWC's East/Central Pacific duplicates. |
| Cross-check (optional) | GDACS `MAP?eventtype=TC` | all but South Atlantic | Detects a storm that one primary missed, and gives La Réunion's view for the South-West Indian Ocean. CC BY 4.0 + disclaimer. ~5 h lag. |
| Cross-check (optional) | JMA bosai JSON / JMA XML | West Pacific | The RSMC's own view, CC BY 4.0-compatible. 10-min winds, so **don't** merge into the 1-min thresholds. |
| **Withhold** | — | South Atlantic | No publishable machine-readable source. State "no RSMC; South Atlantic cyclones are not covered", never "no storm". |

**Severity conventions to reuse, not invent [INF]:**
- **Wind-radius thresholds:** 34/50/64 kt quadrant radii, shared by NHC and JTWC. A port inside a forecast 34-kt radius at
  tau ≤ T is the natural flag. 50 and 64 kt raise the level.
- **Intensity labels:**
  - Saffir-Simpson category for NHC storms.
  - JTWC's TD/TS/TY/STY or "Tropical Cyclone" label for the rest. Both are 1-min, so one scale works.
- **Official US warnings:** NHC watch/warning (`windWatchesWarnings`) where present, US Gulf only.
- **Not to use:**
  - GDACS `alertlevel`, because it is population-driven.
  - TCCOR, because it is a military base posture.
  - Cross-agency raw wind comparisons. NHC and JTWC use 1-min winds, CMA 2-min, IMD 3-min, and JMA, BoM, Fiji
    and La Réunion 10-min.
- **[INF]** The port conventions readers would expect (IMD port signals, CMA colour signals, BoM watch/warning)
  are licence-blocked. The rendered flag should say it is derived from NHC/JTWC radii, not mimic those signals.

**Failure states this implies for #361 (invariant 1) [INF]:**
- An empty `activeStorms` or RSS with no warnings = `ok`, with 0 storms.
- HTTP failure = `failed`.
- JTWC 403 on a specific product = product absent, not `failed`, *if* the RSS still lists it.
- A stale `issuance` beyond ~12 h (NH) or ~18 h (SH) during an active storm = `stale`.
- South Atlantic = `no_source` with a reason.

## 4. What changes the map (#356)

1. **South Atlantic has no publishable source.** That covers Santos, Paranaguá and Up-River. The hazard flag spec must
   carry a permanent "not covered" state for those legs. "Global reach" is global minus one basin.
2. **Authority caveat.** For every non-US port the flag comes from JTWC (US DoD guidance), not the WMO RSMC or national
   service. Those are licence-blocked: CMA for Chinese ports, IMD port signals for Indian ports, BoM for Australia. The
   presentation must attribute JTWC and must not call it an official warning.
3. **Licence asks, if ever wanted (pre-sale task, not pre-build):**
   - IMD written permission, which would unlock port signals.
   - BoM Registered User status.
   - Météo-France / La Réunion clarification on WTIO30.
   - None of these blocks the minimal set.
4. **Nigeria and Europe:** the Gulf of Guinea risk is effectively nil climatologically. Europe sees post-tropical
   remnants handed to national services, outside NHC's warning remit [SRC] ([NHC product
   descriptions](https://www.nhc.noaa.gov/pdf/NHC_Product_Description.pdf)). Whether to flag either at all is a
   #361 decision.

## 5. Open / unverified

- The current WTIO30 forecast horizon, and whether its 64 kt radii appear. Not observable off-season.
- Whether WMO "core data" status lets a third party republish RSMC warnings. This is a legal question.
- Long-term stability of the JMA bosai JSON and the SWIC JSON paths, both undocumented.
- The BoM radii thresholds come from a secondary source only.
- CMA's 2-min averaging comes from a secondary source only.
