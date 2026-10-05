# R3: Climatology for "normal", soy production weights, and boundary polygons per footprint

Research for [#359](https://github.com/philipbergman6-glitch/Mirror-Market/issues/359) (map [#356](https://github.com/philipbergman6-glitch/Mirror-Market/issues/356)); blocks spike #361.
Date: 2026-10-05. Branch `research/r3-climatology-weights` off `main` @ `1500bc3`.
All probes were live on 2026-10-05. **Observed** means fetched or curl'd from the cited primary URL. **(inference)** marks my own reasoning. Licence quotes are verbatim.

Footprints in scope come from `config.WEATHER_GROWING_SEASON_MONTHS` (`config.py:1443`):
- US: IA, IL, NE
- Brazil: MT, PR, RS
- Argentina: Buenos Aires (soy and sunflower), Córdoba
- Paraguay: Alto Paraná
- Rapeseed: FR Grand Est, DE Mecklenburg-Vorpommern, RO Bărăgan
- Canola: CA SK, AB
- Palm: ID Riau, MY Sabah
- India: Madhya Pradesh, Maharashtra
- China: Heilongjiang
- South Africa: Free State, Mpumalanga
- Nigeria: Benue, Kaduna

Three of them sit at 52–54°N: Saskatchewan, Alberta and Mecklenburg-Vorpommern. That latitude decides several of the choices below.

---

## Recommendation

| Need | Pick | Licence for a published derived anomaly | Fallback / cross-check |
|---|---|---|---|
| **Climatology ("normal")** | **ERA5 (0.25°) + ERA5-Land (0.1°, soil) from the Copernicus CDS, 1991–2020**, built once into a committed reference CSV. Columns: footprint × day-of-year × {15-day precip sum, tmax, soil water}. | CC BY 4.0 (observed in the CDS catalogue API) | CHIRPS v3 pentads as an observed-rain cross-check. CPC Unified ready-made 1991–2020 daily normals for the quickest prototype in spike #361. |
| **Production weights, between regions** | Official sub-national statistics, **trailing 5-year mean**, re-run yearly: NASS county (US), IBGE PAM município (BR), MAGyP departamento (AR), Eurostat NUTS2 (EU; NUTS1 for DE), StatCan province (CA), DES state/district (IN), BPS province (ID) | Public domain / CC BY 4.0 / OGL-Canada / GODL-India / BPS commercial-OK (quotes below) | — |
| **Production weights, within a region and where official terms block reuse** | **SPAM 2020 v2.0 r2** (IFPRI, 5′ grid, crops `soyb`/`rape`/`sunf`/`oilp`), used **alone** for China (NBS non-commercial), Malaysia (MPOB consent), Paraguay and Nigeria (weak or unlicensed sources) | CC BY 4.0 plus the IFPRI disclaimer sentence | GEOGLAM BACS soy mask (CC BY 4.0) |
| **Boundary polygons** | **Natural Earth 10m admin-1** for every footprint. If admin-2 is ever needed, use each country's own agency: Census `cb_2024_us_county_500k`, IBGE MMD 2025, StatCan, HDX COD-AB (PY/NG/ID). | Public domain (NE); attribution only for the others | geoBoundaries gbOpen (check the per-file licence; avoid ODbL files) |
| **Do not use** | GADM (non-commercial), Eurostat GISCO NUTS geometry (non-commercial), Open-Meteo free tier for the climatology build (non-commercial), EarthStat/Monfreda (permission needed) | — | — |

**Grid-to-polygon method:** weight each cell by the fraction of it inside the polygon (exactextract computes "the fraction of the cell covered by the polygon": <https://isciences.github.io/exactextract/background.html>). Multiply by cos(latitude) for cell area (standard practice; I did not fetch a source for it). Multiply by the SPAM crop share for production weighting.

**Label every anomaly "vs ERA5 1991–2020".** The forecast is raw ECMWF IFS and the normal is a reanalysis. That is not ECMWF's own re-forecast model climate, so model bias stays in the anomaly (see §1.6).

---

## 1. Climatology: what is "normal"?

### 1.1 Comparison

| | ERA5 / ERA5-Land (CDS) | CPC Unified (NOAA PSL) | CHIRPS v3 (UCSB CHC) | Open-Meteo archive | NASA POWER |
|---|---|---|---|---|---|
| Coverage | Global. ERA5-Land is land only. | Global land | **60°N–60°S** land. v2 was 50°S–50°N. | Global (it reprocesses ERA5/ERA5-Land) | Global |
| Resolution | 0.25° / 0.1° | 0.5° | 0.05° | Point (nearest cell) | ½°×⅝° |
| Record | 1940– / 1950– | 1979– | 1981– | Same as ERA5 | 1981– |
| Update lag | About 5–6 days (ERA5T) | About 1 day | 2 days after each pentad (preliminary); monthly (final) | About 1 day | "a few months" |
| Variables | Precipitation, tmax, soil moisture (4 layers in ERA5-Land) | Precipitation; tmax/tmin in a sister set | Precipitation only | Precipitation, tmax, soil moisture | Precipitation, tmax, soil wetness |
| Access | Free account + token; accept the terms per dataset, by hand | Anonymous HTTPS | Anonymous HTTPS | No key; 10k calls/day | No key |
| Commercial derived publishing | **Yes, CC BY 4.0** | Yes in practice: acknowledgement requested, no licence found | **Yes: public domain + CC BY 4.0** | **No on the free tier** | Attribution requested; licence not found |
| One-off build | About 2 GB for 22 footprint boxes (inference) | Lowest: ready-made 1991–2020 daily normal files | About 26 GB of pentads, less with windowed COG reads | About 780 calls per point for 30 years; points only | Moderate |

### 1.2 ERA5 / ERA5-Land via the Copernicus CDS: recommended

- **Licence, observed** with curl on 2026-10-05 against `https://cds.climate.copernicus.eu/api/catalogue/v1/collections/<id>`. All of these return `"license":"CC-BY-4.0"`:
  - `reanalysis-era5-single-levels`
  - `reanalysis-era5-land`
  - `derived-era5-single-levels-daily-statistics`

  The research agent also saw it on `reanalysis-era5-land-timeseries` and `derived-era5-land-daily-statistics`.
- **When the licence changed.** ECMWF: "on Wednesday 2nd July 2025, the License to use Copernicus Products in the Climate Data Store (CDS)… will be replaced with the Creative Commons Attribution License (CC-BY)" (<https://forum.ecmwf.int/t/cc-by-licence-to-replace-licence-to-use-copernicus-products-on-02-july-2025/13464>).
- **Attribution template** (<https://confluence.ecmwf.int/x/l-xCDQ>): "The results contain modified Copernicus Climate Change Service information 2020. Neither the European Commission nor ECMWF is responsible for any use that may be made of the Copernicus information or data it contains." Also cite the dataset DOI (ERA5 single levels: `10.24381/cds.adbb2d47`). Footprint pages already carry ECMWF CC BY attribution (map #356), so this adds one line.
- **Coverage and lag.**
  - Global, 0.25°, "1940 to present".
  - ERA5T has about a 5-day lag and "could be different from the final release 2 to 3 months later" (<https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels>).
  - The catalogue showed data to 2026-09-29 on 2026-10-05 (observed by the agent).
  - The lag does not matter for a fixed 1991–2020 normal.
- **Access.** Free account, token in `~/.cdsapirc`. The user must "agree to the Terms of Use of a dataset before downloading… manually from the dataset page" (<https://cds.climate.copernicus.eu/how-to-api>). **This is a one-time HITL step.**
- **ERA5-Land time-series dataset.** It takes "the nearest grid point", so it gives points, not area means. Not useful for footprints.
- **ARCO ERA5 on Google Cloud** (`gs://gcp-public-data-arco-era5`): readable anonymously. The README says the data carries "the terms of the Copernicus license". The store is chunked one time step per piece across the whole globe, so pulling 30 years for one area means reading every global chunk (inference). Prefer CDS area subsets.
- **Rainfall caveat, from the ARCO README** (<https://github.com/google-research/arco-era5>): "ERA5's precipitation variables aren't directly constrained by any observations… check ERA5 against observed precipitation". That is the reason for the CHIRPS cross-check.
- **Build sketch (inference).**
  - Use `derived-era5-single-levels-daily-statistics` (and its ERA5-Land counterpart for soil water), one bounding-box request per footprint.
  - Volume: about 640 cells × 10,958 days × 3 variables ≈ 85 MB per footprint, about 2 GB total, one-off.
  - Tools: `cdsapi` + `xarray` + exactextract-style fractional weights.
  - Compute 15-day running sums, then a day-of-year mean with smoothing, and commit as a CSV.

### 1.3 CPC Global Unified Gauge-Based Precipitation (NOAA PSL): prototype or fallback

- **Endpoints, observed with curl.**
  - `https://downloads.psl.noaa.gov/Datasets/cpc_global_precip/precip.2026.nc` → `HTTP/2 200`, 47 MB, `last-modified: Sun, 04 Oct 2026`.
  - **Ready-made daily normals:** `precip.day.ltm.1991-2020.nc` (541 MB) and `cpc_global_temp/tmax.day.ltm.1991-2020.nc` (263 MB).
- **Terms.** PSL asks: "If you acquire … data products from PSL, we ask that you acknowledge us… 'data provided by the NOAA PSL, Boulder, Colorado, USA, from their website at https://psl.noaa.gov'". This is an acknowledgement request, not a licence. NCAR's Climate Data Guide lists usage restrictions as "None". I found no explicit public-domain statement; US-government-work status is inference.
- **Quality, from NCAR's guide** (<https://climatedataguide.ucar.edu/climate-data/cpc-unified-gauge-based-analysis-global-daily-precipitation>): "Quality… is poor over tropical Africa"; the station count fell from about 30k to about 17k; the real-time version starts in 2006.
  - So the 1991–2020 base contains a methods break.
  - It is weak for Benue and Kaduna.
  - It is precipitation only (tmax is a sister set). No CPC soil-moisture file was found.

### 1.4 CHIRPS v3 (UC Santa Barbara Climate Hazards Center): rainfall cross-check

- **Coverage.** "It spans 60°N to 60°S… 0.05° gridded rainfall time series over land", "from 1981 to near-present" (<https://www.chc.ucsb.edu/data/chirps3>). v3 reaches the 52–54°N footprints.
- **v2 does not, and is ending.** v2 is 50°S–50°N, and "Production of CHIRPS v2 will end after December 2026" (<https://www.chc.ucsb.edu/data/chirps>).
- **Licence, verbatim** (re-observed with curl): "CHIRPS3 **is in the public domain, as registered with Creative Commons, and is licensed under a** Creative Commons Attribution 4.0 International License. To the extent possible under the law, the Climate Hazards Center has waived all copyright and related or neighboring rights to CHIRPS3."
- **Citation.** Funk et al., *Sci Data* 13, 718 (2026), <https://doi.org/10.1038/s41597-026-07096-4>; data DOI `10.15780/G2JQ0P`.
- **Daily values are not independent.** "CHIRPS is fundamentally a pentad and monthly product." Daily values are split from pentads using ERA5 or IMERG (<https://data.chc.ucsb.edu/products/CHIRPS/v3.0/daily/readme.txt>). A 15-day window is exactly 3 pentads, so the pentad product fits.
- **Endpoint, observed.** `…/v3.0/pentads/global/netcdf/chirps-v3.0.1991.pentads.nc` → 200, 865 MB per year (about 26 GB for 1991–2020). Windowed reads of the daily COG `.tif` files would cut that (inference).

### 1.5 Open-Meteo: do not use for the build

- **Data licence.** "API data are offered under Attribution 4.0 International (CC BY 4.0)" (<https://open-meteo.com/en/licence>).
- **Free-API terms are separate and stricter.** "You may **only use the free API services for non-commercial purposes**." (re-observed with curl). "Less than 10'000 API calls per day, 5'000 per hour and 600 per minute." Commercial use includes "Operating websites or apps that have subscriptions or display advertisements" and "Integrating our service into commercial products" (<https://open-meteo.com/en/terms>).
- **Call weighting.** "Requests… extending over a period of more than 2 weeks for a single location are considered multiple API calls" (<https://open-meteo.com/en/pricing>). That is about 780 calls per point for 30 years (inference).
- **Endpoint works (observed).** curl of `archive-api.open-meteo.com` for Iowa (42.0, −93.5), 1991-01-01..03, returned `precipitation_sum [0,0,0]`, `temperature_2m_max [-0.9,-5.7,-10.9]`, `soil_moisture_0_to_7cm_mean 0.384…`.
- **Verdict.** It serves the same ERA5 as the CDS, but as points only, and its free tier is barred for commercial use. Going to the CDS directly gets the same data under CC BY with no fee.

### 1.6 Base period and method

- **WMO normals.** WMO standard normals are "Averages of climatological data computed for… consecutive periods of 30 years: 1 January 1981 to 31 December 2010, 1 January 1991 to 31 December 2020, etc." (<https://community.wmo.int/site/knowledge-hub/programmes-and-initiatives/climate-services/wmo-climatological-normals>); the method guide is WMO-No. 1203. **1991–2020 is the defensible base.**
- **ECMWF's own anomalies use a model climate, not observations.** Quote: "Forecasts in terms of anomalies relative to a model climate (rather than relative to the observed climatology) mean that some calibration for model bias and drift into the products is incorporated." The re-forecast set is "20 years x 5 runs x 11 ensemble members" (<https://confluence.ecmwf.int/spaces/FCST/pages/462890763/Sub-seasonal+range+re-forecasts+configurations+changes>).
  - **I'm not confident whether re-forecasts are in ECMWF Open Data.** That belongs to R1 #357. If they are, they would be the bias-free "normal" for the forecast.
  - If they are not, our anomaly carries IFS bias and must be labelled as being against ERA5. Shared IFS lineage should make the bias smaller than against gauge data (inference).
- **Day-of-year smoothing.** I found no primary CPC/ECMWF document that prescribes a smoothed day-of-year climatology of 15-day windows. ECMWF's re-forecast model climate pools a window of start dates, which is the same idea. A running window or 3 harmonics is reasonable (inference), and the spike should state which.

---

## 2. Production weights

### 2.1 Per-market sources

| Market | Source | Granularity | Format / key | Latest | Licence (verbatim) | Join codes |
|---|---|---|---|---|---|---|
| **US soy** | NASS Quick Stats API <https://quickstats.nass.usda.gov/api> | State, county | JSON/CSV; **key needed**; ≤50k records per call | County corn/soy published each May (NASS kept May for 2026: [Apr-2026 proceedings](https://www.nass.usda.gov/Education_and_Outreach/Meeting/2026/Proceedings%20from%20the%20%20April%202026%20Meeting.pdf)) | "most of the information… is within the public domain… may be freely downloaded and reproduced" ([citation page](https://www.nass.usda.gov/Data_and_Statistics/Citation_Request/index.php)). API terms require: "This product uses the NASS API but is not endorsed or certified by NASS." | State FIPS + county ANSI. County `998` = "OTHER (COMBINED) COUNTIES"; small or confidential counties are withheld |
| US (grid alternative) | Cropland Data Layer ([FAQ](https://www.nass.usda.gov/Research_and_Science/Cropland/sarsfaqs2.php)) | 30 m up to 2023, 10 m from 2024 | COG; no key | 2025 layer released 2026-02-27 | "considered public domain and free to redistribute" | Raster |
| **Brazil soy** | IBGE PAM via SIDRA (table 5457), **curl-tested** | State (N3), município (N6) | JSON API; no key | **2025** | No IBGE-specific licence found. IBGE allows reuse with citation ([biblioteca](https://biblioteca.ibge.gov.br/biblioteca-servicos.html)). IBGE's boundary meshes are explicitly CC BY 4.0 (§3). | 7-digit município code; 2-digit state code (MT 51, PR 41, RS 43) |
| Brazil | CONAB série histórica (`portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt`, HTTP 200) | State | TXT; no key | 2025/26 | **No licence found.** CONAB says it is outside the open-data decree ([dados-abertos](https://www.conab.gov.br/dados-abertos-m)). | State letters. **Cross-check only.** |
| **Argentina soy + sunflower** | MAGyP Estimaciones agrícolas ([CKAN](https://datos.magyp.gob.ar/dataset/estimaciones-agricolas)) | Province, departamento | CSV; no key | 2024/25 (series from 1969) | CKAN `license_id`: **CC-BY-4.0** | 2-digit `provincia_id`, 5-digit `departamento_id` (INDEC style) |
| Argentina | BCR (Rosario grain exchange) | — | — | — | Not researched; unnecessary because MAGyP is CC BY | — |
| **Paraguay soy** | MAG/DCEA Síntesis ([PDF](https://informacionpublica.paraguay.gov.py/public/1152699-SINTESISESTADISTICAS_16principalescultivos_25082021-1pdf-SINTESISESTADISTICAS_16principalescultivos_25082021-1.pdf)); INE ([xlsx](https://estadisticasambientales.ine.gov.py/detalle.php?id=158)) | Department | PDF/XLSX | No 2024/25 table found | Not found | Names. **Use SPAM.** |
| **China soy** | NBS <https://data.stats.gov.cn/> | Province | Web database | Yearbook cadence | Reuse must be "以新闻性或资料性公共免费信息为使用目的" (news or free public reference information) and carry "来源：国家统计局" ([用户协议](https://data.stats.gov.cn/login.htm?m=toDisclimer)). **Blocks a commercial site.** | GB/T 2260 (230000). **Use SPAM.** |
| **India soy** | DES APY portal ([crops-apy](https://data.desagri.gov.in/website/crops-apy-report-web)); data.gov.in district set ([catalog](https://www.data.gov.in/catalog/district-wise-season-wise-crop-production-statistics-0), last updated 2021) | State, district | Excel/CSV; data.gov.in needs a key (`DATA_GOV_IN_API_KEY` is still absent locally) | State to 2024-25 | GODL-India: "worldwide, royalty-free, non-exclusive license to use, adapt, publish… for any lawful commercial or non-commercial purpose" (quoted via search summary; the data.gov.in page served only a redirect page, **not confirmed first-hand**) | Names; LGD codes not confirmed |
| India | SOPA survey ([sopa.org](https://sopa.org/all-india-state-wise-soybean-area-production-and-productivity/)) | State, division | HTML | Kharif 2025 | "All right reservered SOPA © 2025". Industry estimate. **Avoid.** | Names |
| **EU rapeseed** | Eurostat `apro_cpshr` (crop `I1110`, `AR_THS_HA`), **API-tested** | NUTS2. **Germany NUTS1 only**; DE NUTS2 empty in 2024. | JSON/SDMX; no key | 2000–2024; updated 2026-09-08 | "Reuse of statistical data… for commercial or non-commercial purposes is authorised provided the source is acknowledged" ([notice](https://ec.europa.eu/eurostat/help/copyright-notice)) | NUTS codes (FRF, DE80, RO22/RO31) |
| **Canada canola** | StatCan [32-10-0359-01](https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3210035901) | Province | CSV/API | 2026, released 2026-09-16 | "use, reproduce, publish, freely distribute, or sell the Information"; credit "Adapted from Statistics Canada…" ([Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence)) | Province codes |
| **Indonesia palm** | BPS *Statistik Kelapa Sawit / Tanaman Perkebunan 2024* ([pub](https://www.bps.go.id/en/publication/2025/08/29/8d2a6ab3510f9828daf73191/statistik-tanaman-perkebunan-tahunan-indonesia-2024--kelapa-sawit--kopi--kakao--karet--teh--dan-komoditas-perkebunan-unggulan-.html)) | Province | PDF/tables | 2024 data | Allows "penggunaan data, baik untuk kepentingan komersial maupun nonkomersial" (data use, commercial or non-commercial) with citation ([term-of-use](https://www.bps.go.id/en/term-of-use)) | BPS region codes |
| **Malaysia palm** | MPOB area summary ([2024 PDF](https://bepi.mpob.gov.my/images/area/2024/Area_summary2024.pdf)) | State | PDF | 2024 | No republishing "without prior consent of Director-General" ([copyright](https://bepi.mpob.gov.my/index.php/import/332-copyright)). **Use SPAM.** | Names |
| **South Africa soy** | Crop Estimates Committee via [SAGIS](https://www.sagis.org.za/crop-estimates-committee-2/) / [DoA](https://www.nda.gov.za/index.php/publication/320-crop-estimates) | Province | PDF | 2025 final | **Not found**; confirm before publishing | Names |
| **Nigeria soy** | NAERLS Agricultural Performance Survey ([2025 PDF](https://agriculture.gov.ng/wp-content/uploads/2025/10/2025-Wet-Season-Agricultural-Performance-in-Nigeria.pdf)) | State (annexes, unverified) | PDF | 2025 | Not found. **Use SPAM.** | Names |
| **Global grid** | **SPAM 2020 v2.0 r2** ([Dataverse](https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/SWPENT)) | 5′ (~10 km) grid | CSV/GeoTIFF zips (66–146 MB); **download needs a guestbook form** (the API refused without one) | Released 2026-05-05; circa 2020 (2019–21 mean) | **CC BY 4.0.** Adaptations must state: "This data was provided by the International Food Policy Research Institute (IFPRI). IFPRI bears no responsibility for the analyses or interpretations…" | Grid |
| Global | GEOGLAM BACS ([Zenodo](https://zenodo.org/records/7230863)) | 0.05° | 7 MB zip | v1.0.0 (2022) | CC BY 4.0 | Grid (soy, maize, wheat, rice only) |
| Global | EarthStat / Monfreda ([metadata](https://geodata.ucdavis.edu/geodata/crops/monfreda/METADATA_HarvestedAreaYield175Crops_June2018.pdf)) | ~10 km | GeoTIFF | circa 2000 | Commercial use or re-release only "with explicit permission". **Avoid.** | Grid |

**Precedent.** USDA FAS IPAD's own sub-national crop shares are "calculated using IFPRI-SPAM (CY2010)" ([Crop Explorer](https://ipad.fas.usda.gov/cropexplorer/cropview/commodityView.aspx?cropid=2222000)). USDA itself weights by a static SPAM share.

### 2.2 Is a static multi-year average defensible?

**Yes, provided it is a trailing 5-year mean, its vintage is stated, and it is refreshed yearly.** An older fixed vintage drifts too far. Evidence (the agent's calculations from primary data, so inference built on observed numbers):

- **Brazil** (SIDRA 5457, harvested-area shares, 2005 → 2025):
  - Paraná 18.1% → 12.3%; Rio Grande do Sul 16.3% → 14.3%.
  - Mato Grosso flat (26.6% → 26.9%).
  - Matopiba (MA+TO+PI+BA) 7.8% → 12.5%.
  - Against 2025 shares, a 2016–20 mean is off by **5.9 points** of total weight; a 2021–25 mean by **1.6 points**.
- **Argentina** (MAGyP departamentos):
  - A 2016–20 mean is off from 2024/25 by **7.8 points**; a 2020/21–2024/25 mean by **5.1 points**, about the same as one year-to-year swing (drought years such as 2022/23 distort the shares).
  - Sunflower: Buenos Aires 57.7% → 48.5%; Córdoba 1.9% → 11.6% (2015 → 2025). This matters for the "Argentina Buenos Aires (sunflower)" pin.
- **India.** Maharashtra 48.27% and Madhya Pradesh 35.27% of soybean output in 2024-25 ([Economic Survey 2025-26, Table 1.18](https://www.indiabudget.gov.in/economicsurvey/doc/stat/tab1.18.pdf), observed). The common "MP produces 54%" claim is stale. **Maharashtra now outweighs MP.**
- **China.** Heilongjiang produced 903.9万 t of a national 2,091万 t in 2025, about 43%. The national figure is from the [NBS 2025 communiqué](https://www.stats.gov.cn/sj/zxfbhjd/202602/t20260228_1962662.html); the provincial figure is from the [Heilongjiang government](https://www.hlj.gov.cn/hlj/c110602/list_left_tt.shtml) via a search summary and is not confirmed first-hand.
- **US northern plains.** North Dakota peaked at 7.25M planted acres in 2021 ([ND bulletin 2024](https://www.nass.usda.gov/Statistics_by_State/North_Dakota/Publications/Annual_Statistical_Bulletin/2024/ND-Annual-Bulletin24.pdf)). ERS: expansion "appears to displace sunflower and canola acreage" ([OCS-24d](https://ers.usda.gov/sites/default/files/_laserfiche/outlooks/108963/OCS-24d.pdf?v=87200)).

**Design implication (inference).** Every weight row carries `{source, licence, vintage_years}`. The table is rebuilt once a year after the NASS county (May), IBGE PAM (about September) and MAGyP releases. Under the repo's invariant 10, a fixed-URL reference also needs an age budget, so a weights table older than about 18 months should be flagged `stale`, not silently used.

---

## 3. Boundary polygons

### 3.1 Comparison

| Source | Levels | Licence (verbatim) | Commercial publish? | Join codes in file |
|---|---|---|---|---|
| **Natural Earth 10m admin-1** (latest release v5.1.2, 2022-05-13; [releases](https://github.com/nvkelso/natural-earth-vector/releases)) | Admin-1 (4,596 rows) | "All versions of Natural Earth raster + vector map data found on this website are in the public domain." / "No permission is needed to use Natural Earth. Crediting the authors is unnecessary." ([terms](https://www.naturalearthdata.com/about/terms-of-use/)) | **Yes** | `iso_3166_2`, `adm1_code`, `code_hasc`, `gn_id`, `wikidataid`, `region_cod`, names in 30+ languages |
| **geoBoundaries gbOpen** v4 ([API](https://www.geoboundaries.org/api/current/gbOpen/BRA/ADM2/) returns 200) | ADM1/ADM2 | Per-file licence. The site: "Our license requires an acknowledgement in any products you produce which use this data." | **Yes**, with attribution; **avoid the ODbL files** (IDN ADM1, MYS ADM1, IND ADM2: share-alike) | `shapeName`, `shapeISO`, `shapeID` only. BRA ADM2 has no IBGE code, no parent state, and 232 duplicate names. |
| **GADM 4.1** | 0–5 | "The data are freely available for academic use and other non-commercial use. Redistribution or commercial use is not allowed without prior permission." ([licence](https://gadm.org/license.html), re-observed with curl) | **No** | GID_1, HASC_1 |
| **US Census** CB 2024 / TIGER 2025 ([www2.census.gov](https://www2.census.gov/geo/tiger/GENZ2024/shp/)) | State, county | File metadata: "free to use in a product or publication, however acknowledgement must be given to the U.S. Census Bureau as the source." Also: "Cartographic boundary files should not be used for geographic analysis including area or perimeter calculation." | **Yes** (use 500k, not 20m, for area work) | GEOID = 5-digit FIPS (joins NASS directly) |
| **IBGE Malha Municipal 2025** ([geoftp](https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2025/)) | UF, município | "O IBGE disponibiliza a MMD e todos produtos e informações dela derivados sob licença compatível com 'Creative Commons BY 4.0'… mesmo para fins comerciais, desde que sejam atribuídos os devidos créditos" ([Nota Metodológica 01/2026](https://biblioteca.ibge.gov.br/visualizacao/livros/liv102268.pdf)) | **Yes** | CD_MUN (7-digit), same as SIDRA |
| **IGN Argentina** ([terms](https://www.ign.gob.ar/descargas/tyc1.html)) | Province, departamento | "Se permite su uso comercial únicamente en el caso de obras derivadas en que la información sea utilizada como insumo para generar un nuevo producto." (commercial use allowed only for derived works that use the data as input to a new product) | **Conditional.** A weather average computed from the shapes reads as a derived work (inference, not legal advice). | Not inspected |
| **Eurostat GISCO NUTS** ([page](https://ec.europa.eu/eurostat/web/gisco/geodata/administrative-units)) | NUTS 0–3 | "the data will not be used for commercial purposes … If you intend to use the data commercially, please contact EuroGeographics" | **No.** Use NUTS **codes** only, as join keys. | NUTS_ID |
| **StatCan** boundary files | Province, CD | "use, reproduce, publish, freely distribute, or sell the Information" ([Open Licence](https://www.statcan.gc.ca/en/terms-conditions/open-licence)) | **Yes** | PRUID/CDUID (unverified; the page blocked automated access) |
| **OCHA HDX COD-AB** (NGA, PRY, IDN; updated 2026) | ADM1/ADM2 | `cc-by-igo`: "Creative Commons Attribution for Intergovernmental Organisations" ([HDX API](https://data.humdata.org/api/3/action/package_show?id=cod-ab-nga)) | **Yes**, with attribution | P-codes |

### 3.2 Join pitfalls (do names and codes join cleanly? Mostly no)

- **Grand Est is not a single row in Natural Earth.** France appears as departments tagged `region_cod = "FR-GES"`; dissolve them.
- **Buenos Aires province (`AR-B`) is typed "Federal District"** in Natural Earth, the same as CABA (`AR-C`). Fix it explicitly; don't filter by type.
- **Natural Earth's `fips` column is not unique** (Aube and Marne are both `FRA4`). Never use it as a key.
- **Romanian Bărăgan is not an administrative unit.**
  - It spans NUTS RO22 (Sud-Est: Brăila RO221, Constanța RO223) and RO31 (Sud-Muntenia: Ialomița RO315, Călărași RO312).
  - **Ialomița's old FIPS 10-4 code is `RO22`, which collides with NUTS RO22.**
  - Eurostat's NUTS names use cedilla letters (ş, ţ); Natural Earth drops diacritics entirely. Normalise to NFC, map ş→ș and ţ→ț, then strip accents before any name comparison. Better still, join on codes only.
- **NUTS names have trailing spaces** ("Meuse "). Join on `NUTS_ID`.
- **Brazil:** never join municipalities by name (232 duplicate names). Join IBGE CD_MUN to SIDRA directly; the state code is the first 2 digits.
- **China:** Heilongjiang has two ISO codes, `CN-HL` (current, used by Natural Earth) and the legacy `CN-23`. GB/T 2260 is `230000`.
- **Codes not verified this session:** Argentina INDEC province codes (BA 06, Córdoba 14) ↔ `AR-B`/`AR-X`; India LGD/census codes (MP 23, MH 27) ↔ `IN-MP`/`IN-MH`. These need a hand-built crosswalk from a primary source.
- **Recommended crosswalk spine:** Natural Earth's `wikidataid`. Wikidata links each region to ISO, GB/T 2260 (`P442`), NUTS and SIRUTA codes (e.g. Heilongjiang Q19206, Grand Est Q18677983 → NUTS FRF, Ialomița Q193044). Commit the crosswalk as a small reference CSV and **hard-fail on any unmatched key** (invariant 1).
- **Boundary vintages drift.** IBGE 2025 has 5,573 municipality codes; geoBoundaries BRA ADM2 (2020) has 5,570. Store the boundary year next to the statistics year.
- **Resolution.** At 1:10m, Natural Earth boundary error is roughly a few km (inference), well under one 0.25° cell (~28 km). That is adequate for admin-1 footprints. The maintainers still call the theme "still in beta!" and note that some Brazilian boundaries lack points on straight segments ([page](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/10m-admin-1-states-provinces/)).

---

## 4. Findings that reach beyond this ticket

1. **Layer 5's Open-Meteo pins already use the free API**, and its terms say "only… for non-commercial purposes", with commercial use including "websites… that have subscriptions or display advertisements". `fetchers/weather.py:8` says only "Open-Meteo is free, requires no API key", and the repo has no licence note for it.
   - Today's free public site is non-commercial, so there is no breach now (inference).
   - It is a **pre-sale licence item** of the same kind as MATIF and JSE (invariant 9). The same pins served to a paid desk edition would need an Open-Meteo paid plan.
   - It is a soft argument for footprint weather on ECMWF Open Data (CC BY) eventually **replacing** rather than sitting beside the pins. This is relevant to K2 #361's "beside vs replace" question.
2. **The India pin weighting is stale.** Maharashtra produced 48% vs MP 35% in 2024-25. An India footprint should lean towards Maharashtra (about 58:42 between the two states); an MP-heavy footprint would be wrong.
3. **The Argentine sunflower footprint has moved.** Córdoba's sunflower share went from 2% to 12% in 10 years. A Buenos Aires-only sunflower footprint now covers under half the national crop (BA 48.5%). A BA + Córdoba footprint covers about 60% (inference from the shares).
4. **HITL steps the spike will hit:**
   - Create a CDS account and accept the ERA5 / ERA5-Land dataset terms by hand.
   - Fill in the SPAM Dataverse guestbook form to download it.
   - Get a NASS Quick Stats API key, unless one already exists in `.env`; the agent's hook blocked reading `.env`, so this is unverified.
5. **R1 #357 should answer:** are ECMWF re-forecasts (model climate) in Open Data? If so, they are a better "normal" for anomaly purposes than ERA5. If not, the "vs ERA5 1991–2020" label is mandatory.

## Not verified / open

- NASS Quick Stats was not called live (no key read).
- SPAM download (guestbook form).
- MPOB state PDF (did not respond).
- CONAB terms pages (load by JavaScript).
- The GODL-India quote (via search summary).
- The Heilongjiang 2025 provincial figure (via search summary).
- StatCan boundary field names.
- IGN Argentina attribute table.
- INDEC and India LGD code crosswalks.
- Whether publishing *weights derived from* NBS or MPOB figures counts as "republishing". That is a legal-interpretation question; using SPAM for those markets avoids it.
