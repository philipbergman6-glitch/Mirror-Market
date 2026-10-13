# A17 · Weather-to-yield: public outcome data, defensible first step, cost of the deferred track

**Ticket:** [#460](https://github.com/philipbergman6-glitch/Mirror-Market/issues/460) · Map: [#296](https://github.com/philipbergman6-glitch/Mirror-Market/issues/296) · Branch `research/a17-weather-to-yield` · Written 2026-10-13 against `main` @ `03c7e7b`.

**Premise (not re-decided here):** a quantified weather→yield model is out of scope for map #296 (ruled 2026-10-13, map "Out of scope"). This file costs the deferred track and names the one claim the product can make today without a model.

Legend: **[SRC]** primary external source, fetched · **[S]** primary source seen only via search extract (page itself 403/503/JS-gated) · **[OBS]** observed in this repository · **[INF]** inference · **[NV]** not verified.

---

## Executive finding

1. **Validation data exist, with open licences, for the US, Brazil, Argentina, Canada, EU and Ukraine footprints — and are weak or closed for India, China, South Africa, Paraguay.** The deferred track is feasible for 11 of the 20 footprints (the three US states, three Brazilian UFs, two Argentine, Paraguay only via MAG PDFs, three EU rapeseed via Eurostat NUTS-2). It is *not* feasible as a validated public claim for Heilongjiang (NBS: no reuse licence), Madhya Pradesh / Maharashtra (DES portals geo-blocked from the runner, same class as #449; SOPA all-rights-reserved), or Free State / Mpumalanga (CEC provincial, no licence statement). **[INF from §1]**
2. **No public source publishes a running numeric weather-index soybean yield for South America or the US that the product could link to instead of computing.** The ticket's example — "link to the JRC MARS bulletin for this footprint" — does not work: the MARS *Global outlook* series now covers Ukraine only, twice a year; the monthly *Crop monitoring in Europe* bulletin does carry a numeric EU soybean yield forecast, which serves the three EU rapeseed footprints and no soy footprint. **[SRC, §2.3]**
3. **The one claim the page can make today:** a per-footprint **link line to the GEOGLAM Crop Monitor for AMIS** (monthly, CC BY 4.0, qualitative soybean condition for US / Brazil / China / India / Canada / Ukraine) plus, for Argentina and Paraguay where Crop Monitor is silent on soy, the MAGyP monthly estimate PDF. This is a *link*, labelled "third-party condition assessment, not this project's observation" — the same shape as the #320/#328 TradingView exception, and `pricing.semantics` never sees it. **[INF]**
4. **What exists on `main` today is thinner than the ticket assumes.** The footprint build (#380–#386) is specified but every slice is still open; `pipeline/footprints.py` does not exist. Production-weighted exposure, ERA5 normals and "day-1 estimate" labels are *spec*, not product. The audit chain's "step one" on `main` is Layer 5 Open-Meteo pins plus six observed rules whose thresholds carry no citation. **[OBS, §3]**
5. **`ROADMAP.md` on `origin/main` has no weather-to-yield row** although map #296 says the deferred track is "recorded in `ROADMAP.md`". The follow-up docs PR must add the row sketched in §4. **[OBS]**

---

## 1. Public, reproducible yield-outcome data per footprint market

The audit's precondition for any published quantitative claim is validation against historical outcomes. Below, "earliest" is the first soybean yield year the source itself exposes; cadence is when a season's *final* number appears.

### 1.1 United States — footprints G-IA, G-IL, G-NE

| Dataset | Resolution | Cadence / timing | Earliest | Licence | Access | Status |
|---|---|---|---|---|---|---|
| **USDA NASS Quick Stats (SURVEY)** — county and state soybean yield. https://quickstats.nass.usda.gov/api | State annual; county via the federal county-estimates programme | State: monthly forecasts Aug–Nov in *Crop Production*, final in the January Annual Summary. County: NASS survey guide says federal row-crop county estimates publish "late February through mid-May"; 2024-crop soybean counties released 6 May 2025 **[S]** | State 1924 **[INF]**; county ≈1972 **[NV]** — no primary page states it; confirm with `get_param_values` under a key | NASS citation page: "within the public domain… may be freely downloaded and reproduced… requested that… USDA-NASS be given appropriate acknowledgment" https://www.nass.usda.gov/Data_and_Statistics/Citation_Request/index.php **[SRC]**; data.gov record licence CC0 **[SRC]**. API ToS: key by e-mail registration; must display "This product uses the NASS API but is not endorsed or certified by NASS" **[SRC]** | JSON/XML/CSV, 50,000 rows per call | Layer 2 already fetches this source at `agg_level_desc = NATIONAL` only (`fetchers/usda.py:98,201`) **[OBS]** |
| **NASS Crop Progress & Condition** https://www.nass.usda.gov/Surveys/Guide_to_NASS_Surveys/Crop_Progress_and_Condition/index.php | State (18 soybean states) | Weekly Apr–Nov, 4 pm ET Monday (Tuesday after holidays); monthly Dec–Mar **[SRC]** | Condition ≈1986 **[INF]** | As above | Quick Stats CONDITION / PROGRESS items | Layer 2b already fetches **[OBS]**. Subjective reporter ratings, revised the following week |
| **USDA RMA Cause of Loss** https://www.rma.usda.gov/tools-reports/summary-of-business/cause-loss ; files `colsom_YYYY.zip` | County × commodity × cause × month | Annual ZIP, current year updated through the season **[SRC]** | 1989 (1948–88 as one aggregate on the legacy page) **[SRC]** | data.gov record CC0 **[SRC]** | Pipe-delimited flat files | Indemnity is a loss-*event* proxy, not yield; layout breaks at 2001 |

**Traps [S]:** county suppression (< 100 harvested acres, too few reports, disclosure); the county programme was cut and reinstated March 2025 and the 2025-crop county count fell; CENSUS-programme county yields (every 5 years) are a different measure from SURVEY.

### 1.2 Brazil — footprints G-MT, G-PR, G-RS

| Dataset | Resolution | Cadence / timing | Earliest | Licence | Access | Status |
|---|---|---|---|---|---|---|
| **CONAB Série Histórica de Safras — Grãos** https://portaldeinformacoes.conab.gov.br/safra-serie-historica-graos.html ; flat file https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt | UF (27 states) | 12 monthly *levantamentos* ~mid-month 09:00 BRT (2026: 15 Jan … 15 Sep = 12th/final 2025/26; 15 Oct = 1st 2026/27) **[SRC]**; the flat file carries one row per crop-year = the *current* estimate, so there is **no vintage history inside the file** **[SRC]** | 1976/77 **[SRC]** | Site footer: "Todo o conteúdo deste site está publicado sob a licença Creative Commons Atribuição-SemDerivações 3.0 Não Adaptada" — **CC BY-ND 3.0** **[SRC]**. Link and quote; a derived yield series is a derivative — licence read needed before rendering any transformation | `;`-delimited, latin-1 | Layer 15 already fetches this exact file and aggregates UFs to national (`fetchers/conab.py:12–15`) **[OBS]** — the per-UF rows are already in hand |
| **IBGE PAM** (Produção Agrícola Municipal), SIDRA tables 1612 / 5457 https://sidra.ibge.gov.br/tabela/1612 | Municipality (5,564 soy rows for 2025) and UF **[SRC]** | Annual; 2025 already published **[SRC]**; typically ~Sep/Oct of the following year **[INF]** | 1974 (Brasil soja 1,531 kg/ha) **[SRC]** | IBGE terms page is Cloudflare-gated **[NV]**; Agência IBGE: "reprodução permitida desde que citada a fonte" **[S]**. No CC declared | `apisidra.ibge.gov.br/values/t/1612/n6/all/v/112/p/last 1/c81/2713?formato=json`, no key; root path 403s but `/values/` answers to a browser UA **[SRC]** | Calendar-year label (PAM "2025" = safra 2024/25) — the #403/#433 class of trap |
| **IBGE LSPA** table 6588 | Brasil / region / UF only | Monthly; 2026 calendar: ref 9/2026 → 15 Oct, 10/2026 → 13 Nov, 11/2026 → 15 Dec **[S]** | Sep 2006 **[SRC]** | As PAM | apisidra, variable 36 | In-season vintages, not outcomes |

**Trap [SRC]:** the CONAB file's `produtividade` column is truncated to integer thousands (MT 2024/25 reads `4`); recompute yield = produção / área.

### 1.3 Argentina and Paraguay — footprints G-PAM, G-COR, G-BAS, G-PY

| Dataset | Resolution | Cadence / timing | Earliest | Licence | Access | Status |
|---|---|---|---|---|---|---|
| **MAGyP Estimaciones Agrícolas (datos abiertos)** https://datos.magyp.gob.ar/dataset/estimaciones-agricolas | Departamento / partido (342 soy departments, 16 provinces) **[SRC]** | CKAN: semiannual; file dated 2026-03-05, latest campaign 2024/25 — departmental finals lag ≈1 year **[SRC]**. Monthly PDF 3rd Thursday + weekly Thursdays **[S]** | **1969/70** **[SRC]** | CKAN `license_title: Creative Commons Attribution 4.0` **[SRC]** | 15.5 MB CSV: `cultivo, anio, campania, provincia, departamento, superficie_sembrada_ha, superficie_cosechada_ha, produccion_tm, rendimiento_kgxha` | Best-licensed sub-national series of any origin. Trap: `soja 1ra` / `soja 2da` / `soja total` triple rows |
| BCBA PAS https://www.bolsadecereales.com/estimaciones-informes | ~15 BCBA zones, weekly | Thursday PDF | **[NV]** | Site Cloudflare-blocks both fetchers; no terms found **[NV]** → treat as *not licensed for reuse* | PDF | Estimates, not outcomes; zones ≠ departments |
| BCR GEA https://www.bcr.com.ar/es/mercados/gea | Región Núcleo + national | Weekly + monthly PDF **[SRC]** | n/a | Footer "Todos los derechos reservados" **[SRC]** — no reuse grant | PDF | Build-only under invariant 9 |
| Paraguay: CAPECO https://capeco.org.py/area-de-siembra-produccion-y-rendimiento/ ; MAG/DCEA annual Síntesis Estadística PDFs | CAPECO national only (1996/97+) **[SRC]**; DCEA departmental in PDF **[S]** | Annual | 1996/97 | datos.gov.py licence exists for MAG sets but **no soybean-by-department dataset** is catalogued **[SRC]**; mag.gov.py timed out **[NV]** | HTML / PDF | G-PY validation would need PDF parsing of DCEA tables |

### 1.4 India — footprints G-MP, G-MH

| Dataset | Resolution | Cadence / timing | Earliest | Licence | Access | Status |
|---|---|---|---|---|---|---|
| MoA&FW DES district-wise season-wise crop production https://www.data.gov.in/catalog/district-wise-season-wise-crop-production-statistics-0 ; https://aps.dac.gov.in/APY/Public_Report1.aspx ; https://data.desagri.gov.in/website/crops-apy-report-web | District × season (area, production; yield derivable) | Annual; Advance Estimates 1st (late Nov 2025 for 2025-26, late vs the usual Sep/Oct), 2nd ~Mar, 3rd ~late May **[S]**; soy 2025-26 revised 142.66 → 127.20 → 125.96 lakh t across vintages **[S]** | 1997 on data.gov.in **[S]** | GODL-India: "worldwide, royalty-free, non-exclusive license to use, adapt, publish… for all lawful commercial and non-commercial purposes", mandatory attribution, provider may terminate **[S]** | data.gov.in API (key); APS/DES portals interactive | **All three hosts returned 503/403/timeout from this network** **[NV]** — the same geo-block class as Agmarknet #449. A CI runner cannot be assumed to reach them |
| ICRISAT–TCI District Level Database http://data.icrisat.org/dld/ ; DOI 10.21421/D2/XFB1BZ | 592 districts, boundary-apportioned | Periodic; to 2017, extension to 2019-20 "in progress" **[S]** | 1966 apportioned / 1990 unapportioned (`SOYA_TA`, `SOYA_TQ`) **[S]** | Site: CC BY 4.0 with DOI acknowledgment **[SRC]**; Dataverse page: "ICRISAT Data Sharing Agreement" **[SRC]** — the two conflict | CSV per category | Lags several years; yield is derived |
| SOPA state/district estimate https://sopa.org/all-india-state-wise-soybean-area-production-and-productivity/ | State (+ division/district) | Annual kharif, ~Oct | 2007 **[SRC]** | T&C: SOPA "is the sole owner… All rights… reserved" **[SRC]** | HTML | Layer 33 already fetches it, **private edition only** **[OBS]**. Trade-body estimate, not an outcome |

### 1.5 Other footprints and cross-checks

| Origin / footprint | Dataset | Resolution | Cadence | Earliest soy | Licence | Status |
|---|---|---|---|---|---|---|
| China G-HL | NBS annual database https://data.stats.gov.cn/english/ ; Dec bulletin gives national soy 2,031.5 kg/ha (2025) and provincial *grain* only **[SRC]** | Province | Annual | **[NV]** (database 403) | Copyright NBS; reuse "reasonable, good-faith… news/informational", source mark required, linking needs consent **[S]** — not open | Build-only, never render (invariant 9) |
| EU G-FR, G-DE, G-RO (rapeseed; soya also present) | Eurostat `apro_cpshr` https://ec.europa.eu/eurostat/databrowser/view/apro_cpshr/default/table | NUTS 0/1/2 (649 geos; Germany NUTS 1 only) **[SRC]** | Annual; regional 2000–2024 updated 2026-09-08; national 2000–2026 early estimates **[SRC]** | 2000 (soya `I1110`, `YI_HU_EU`, t/ha) **[SRC]** | CC BY 4.0 **[SRC]** | JSON-stat API, no key; 429 rate-limit hit today. Many NUTS-2 soya cells empty |
| South Africa G-FS, G-MPU | CEC via SAGIS https://www.sagis.org.za/crop-estimates-committee-2/ | Province (9), area + production, yield derivable **[SRC]** | 8 summer-crop forecasts Feb–Sep; prior crop finalised 12 Feb (2026) **[SRC]** | SAGIS archive to 2007 **[SRC]** | No licence statement found; dalrrd.gov.za DNS failed **[NV]** | Layer 25 already fetches the national figure **[OBS]**; PDF only |
| Canada (not a footprint; canola pins dropped) | StatCan 32-10-0359-01 https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=3210035901 | Province | Annual; last release 2026-10-09 **[SRC]** | 1941 (Canada, Ontario) **[SRC]** | Statistics Canada Open Licence: "use, reproduce, publish, freely distribute, or sell" **[SRC]** | WDS REST, no key |
| Ukraine (not a footprint) | Derzhstat oblast table https://www.ukrstat.gov.ua/operativ/operativ2022/sg/pvzu/arch_pvxu_reg.htm | Oblast | Annual finals 2011–2024 **[SRC]** | 2011 on page; 1991+ in the Рослинництво compendium **[SRC]** | Page links CC BY 4.0 **[SRC]** | TLS certificate expired on 2026-10-13 **[SRC]**; 2022+ excludes occupied territories |
| Global gridded | **GDHY v1.2+1.3** (Iizumi) https://doi.org/10.1594/PANGAEA.909132 | 0.5°, 1981–2016, soybean included | One-off | 1981 | CC BY 4.0 **[SRC]** | The standard cross-check for a model's spatial skill; ends 2016 |
| Global national | FAOSTAT QCL https://www.fao.org/faostat/en/#data/QCL | National | Annual, 1961–2024, updated 2025-12-31 **[SRC]** | 1961 | CC BY 4.0 (FAO database terms) **[SRC]** | National only |
| Weights (already used) | SPAM 2020 v2.0 r2, Harvard Dataverse doi:10.7910/DVN/SWPENT, released 2026-05-05 **[SRC]** | 10 km, one year | One-off | — | CC BY 4.0 + IFPRI disclaimer | Not a time series — gives where the crop is, not how it yielded |

**Already in the pipeline [OBS]:** Layer 2 (NASS, national), 2b (Crop Progress), 6 (PSD), 12 (WASDE), 15 (CONAB per-UF file), 25 (CEC), 33 (SOPA, private). The per-UF CONAB rows and a one-parameter change to Layer 2's `agg_level_desc` would give state-level US and Brazilian outcome series with no new source. That is a statement about *building*, not about scope.

---

## 2. A defensible minimum first step, short of a calibrated model

### 2.1 Published stage-specific stress thresholds

| Source | What it gives | Basis | Status |
|---|---|---|---|
| **USDA RMA FCIC-25440 Soybean Loss Adjustment Standards Handbook, 2026 crop year** https://www.rma.usda.gov/sites/default/files/2025-12/2026-25440-Soybean-Loss-Adjustment-Standards-Handbook.pdf | Defoliation × stage yield-loss charts used by crop-insurance adjusters; issued 2025-12-16, effective 2026 and succeeding crop years; public domain | Federal loss-adjustment standard | **[SRC]** cover; chart pages not read |
| **Iowa State Extension — Partial Soybean Hail Chart** https://www.extension.iastate.edu/Pages/eccrops/hailsoychart.html | Expected % loss at 50 % / 100 % defoliation: R1 3/25, R3 17/68, R4 18/73, R5 19/77, R6 14/50, R6.5 4/23; "Total defoliation in the vegetative stages does not cause yield loss" | Reproduces the RMA handbook | **[SRC]**. One cell (R1 at 40 % = "14") is an evident typo; cite RMA for that cell |
| ISU/Purdue simulated-hail study, Agronomy J. 2025, doi 10.1002/agj2.70105 | 100 % defoliation: 67–79 % loss at R4, 80–83 % at R5; 25 % defoliation minimal at any stage | **Peer-reviewed** | **[S]** |
| **University of Minnesota Extension — Flooded soybean** https://extension.umn.edu/growing-soybean/flooded-soybean | Losses rare at ≤ 48 h submergence; ≥ 4 days stunts; ≥ 6 days "can significantly depress yields"; ≥ 1 week significant-to-total stand loss. Per-day loss (Scott et al. 1989): V4 1.8 bu/ac/day clay, 0.8 silt loam; R1 2.3 / 1.5; larger at R3–R5 | Extension summarising a peer-reviewed trial | **[S]** page 403 on fetch |
| UMN hail-damage page https://extension.umn.edu/growing-soybean/hail-damage-soybean-crops | 100 % defoliation at R1 −25 %, at R3 −69 % (3-year trial) | Extension trial | **[S]** |
| **UNL NebGuide G1367 "Irrigating Soybean"** https://extensionpubs.unl.edu/publication/g1367/na/html/view | "About 65 percent of total water use occurs during the R1 to R6+ reproductive stages"; peak ~0.32 in/day at R2–R3; "most important times… pod development (R3–R4) and seed fill (R5–R6)" | Extension, ET-research-based | **[SRC]** |
| **Science for Success (multi-university extension), "Heat Stress in Soybean", 2026-09-29** https://soybeanscienceforsuccess.org/2026/09/29/heat-stress-in-soybean/ | R1–R2 sustained > ~95 °F (35 °C) → flower drop; R3–R4 > ~90–95 °F → pod abortion; R5–R6 > ~86–93 °F with warm nights = highest risk, losses "moderate to over 50 %" | Extension synthesis citing peer-reviewed work | **[SRC]** |
| Schlenker & Roberts 2009, PNAS https://www.pnas.org/doi/10.1073/pnas.0906865106 | Yield rises to **30 °C** for soybean, steep decline above; US county panel 1950–2005 | **Peer-reviewed** | **[S]** |
| Zhao et al. 2026, *Nature Food* 7:194–205 https://www.nature.com/articles/s43016-026-01298-0 ; data https://zenodo.org/records/17142122 | Data-driven extreme-degree-day threshold for soybean **33.7 ± 3.9 °C**, 0.5°, > 130 k sub-national records | **Peer-reviewed**; dataset licence **[NV]** | **[S]** paywall |
| Purdue CL-11 https://www.extension.purdue.edu/extmedia/CL/CL-11.pdf | Premature death: early R5 70–80 % loss; R6 20–40 %; R6–R7 10–20 % | Extension | **[S]** |
| NDSU quick guide https://www.ndsu.edu/agriculture/extension/publications/soybean-growth-and-management-quick-guide | "R4 is the most crucial period"; most critical R4.5–R5.5 | Extension | **[S]** |
| **EMBRAPA/MAPA ZARC** — open data https://dados.agricultura.gov.br/dataset/tabua-de-risco-zoneamento-agricola-de-risco-climatico ; SISZARC https://sistemasweb.agricultura.gov.br/siszarc/base.action | Per municipality × crop × soil type × cultivar group: 10-day sowing windows at **20 / 30 / 40 % yield-loss risk**; "Tábua de Risco" CSVs per safra 2016/17 → 2026/27; **licence CC BY**; last updated 2026-10-13 | Official regulatory zoning built on Embrapa models | **[SRC]** catalogue page |
| INTA (Argentina) https://repositorio.inta.gob.ar/ ; SI.FE.SOJA http://sedici.unlp.edu.ar/handle/10915/153156 | Critical period "begins at R1 and ends 10–12 d after R5 (Board & Tan 1995) or to start of R6 (Egli 1997)" | Research reports; no single INTA threshold sheet found | **[S]** |

**Consensus across sources [INF]:** R4–R6 (pod set → seed fill) is the sensitive window for drought, heat and defoliation; vegetative-stage damage ≈ 0 yield loss; the only *tabulated* loss functions in the public domain are the RMA/ISU defoliation (hail) tables and UMN's submergence days. There is no public tabulated "mm of deficit → % yield" function for any footprint.

**What this says about `config.py` [OBS]:** `WEATHER_POD_FILL_HEAT_C = 34` carries only the comment "sustained days >34C abort pods" (`config.py:2066–2069`), `WEATHER_EXTREME_HEAT_C = 38` and `WEATHER_DRY_SPELL_ALERT_DAYS = 10` carry none. The 34 °C bar sits between Schlenker–Roberts' 30 °C and Science for Success's 35 °C flowering bar and Zhao's 33.7 °C — defensible, but **uncited**. The cheapest honest step is a citation comment per threshold, not a new number.

### 2.2 Crop calendars per footprint

| Source | Format | Soy | Licence | Status |
|---|---|---|---|---|
| **EMBRAPA/MAPA ZARC** (above) | CSV per municipality, decêndio windows | Yes (Brazil) | CC BY | **[SRC]** — the only per-footprint calendar that is both data-shaped and openly licensed |
| CONAB *Calendário de Plantio e Colheita de Grãos* 2022 https://www.gov.br/conab/pt-br/acesso-a-informacao/institucional/publicacoes/arquivos-de-paginas/calendariozplantiozezcolheitazjunz2022.pdf | PDF, month × crop × UF | Yes | CC BY-ND 3.0 (site footer) **[SRC]** — link, do not adapt | **[S]** no newer edition surfaced |
| USDA FAS commodity page crop calendars https://www.fas.usda.gov/data/production/2222000 ; IPAD https://ipad.fas.usda.gov/ogamaps/cropcalendar.aspx | GIF/PNG images | Yes (US, Brazil, Argentina) | US Gov public domain | FAS 403, IPAD 503 at fetch **[NV]** |
| FAO crop calendar datasets https://data.apps.fao.org/catalog/dataset/crop-calendar-by-country-crop-activity-and-stage ; GIEWS https://data.apps.fao.org/catalog/dataset/giews-crop-calendar | Data (BigQuery / CSV) | Soy coverage **[NV]** | CC BY 4.0 (newer entries); older entry CC BY-NC-SA 3.0 IGO | **[S]** |
| GEOGLAM Crop Monitor baseline calendars https://www.cropmonitor.org/baseline-data | AMIS set incl. soybeans; download "Coming Soon"; crop masks on Zenodo 7230863 | Yes | CC BY 4.0 | **[SRC]** |
| JRC ASAP crop calendars https://agricultural-production-hotspots.ec.europa.eu/download.php | Shapefile, FAO-derived | Crop names not listed | EC reuse Decision 2011/833 | **[SRC]** |
| MAGyP weekly/monthly *Informe* PDFs https://www.magyp.gob.ar/sitio/areas/estimaciones/ | Prose by delegación (2026: soy sowing ended 5 Mar, harvest Mar → mid-Jul) | Yes | No licence seen | **[S]** |

**Already in `config.py` [OBS]:** `WEATHER_GROWING_SEASON_MONTHS` (planting-order months per pin, M24 #271) and `WEATHER_SOY_POD_FILL_MONTHS` (US Jul–Aug; South America Jan–Feb). These are month-granular and desk-entered, not sourced to any of the above.

### 2.3 Public quantified estimates the product could link to

| Source | Cadence | Numeric soybean yield? | Resolution | Licence | Status |
|---|---|---|---|---|---|
| **JRC MARS Bulletin — Crop monitoring in Europe** https://joint-research-centre.ec.europa.eu/monitoring-agricultural-resources-mars/jrc-mars-bulletin_en ; archive since 2007 https://agri4cast.jrc.ec.europa.eu/BulletinsArchive/ | Monthly; 2026: 2 Mar, 23 Mar, 27 Apr, 18 May, 22 Jun, 27 Jul, 24 Aug, 28 Sep, **26 Oct, 23 Nov** | **Yes, EU** (Sep 2026: EU soybean −15 % vs 5-yr average) | EU member states + neighbours, country level | EC reuse Decision 2011/833 → CC BY 4.0 | **[SRC]** (independently re-fetched 2026-10-13) |
| **JRC MARS Bulletin — Global outlook** | "issued twice per year" (15 Jun, 21 Sep 2026) | Ukraine only ("reports on crop development in Ukraine") | Ukraine | As above | **[SRC]** — **no South America, US or Asia coverage**; the ticket's example link does not exist for any soy footprint |
| JRC ASAP hotspots https://agricultural-production-hotspots.ec.europa.eu/ | Monthly country assessment; 10-day warnings CSV since 2001 (admin-1/2) | No (generic crops, qualitative) | Africa / MENA / Asia / LAC | EC 2011/833, cite Rembold et al. 2018 | **[SRC]** |
| **GEOGLAM Crop Monitor for AMIS** https://www.cropmonitor.org/crop-monitor-for-amis | Monthly, first week; No. 143 published 1 Oct 2026, conditions as of 28 Sep | **No — qualitative** (favourable / mixed / below-average expected); production-share pie by condition | Soybean for US, Canada, China, India, Ukraine, Brazil. **Not Argentina or Paraguay** | **CC BY 4.0** "for any purpose, even commercially" https://www.cropmonitor.org/referencing-guidelines (footer says "All rights reserved"; guidelines page governs) | **[SRC]** (independently re-fetched) |
| AMIS Market Monitor https://www.amis-outlook.org/ | 10×/yr; 2026 remaining 6 Nov, 4 Dec | World soybean balance (FAO-AMIS / USDA / IGC), not yields by region | Global | No licence statement in PDF **[NV]** | **[SRC]** |
| USDA WASDE / NASS Crop Production https://esmis.nal.usda.gov/publication/crop-production | Same days; 2026 remaining **10 Nov, 10 Dec**, 12:00 ET | Yes — US and state soybean yield Aug–Nov; country production in WASDE | State (US) / country | Public domain, acknowledgment requested | **[S]**. Layer 12 already fetches WASDE **[OBS]** |
| USDA FAS Crop Explorer / WAP https://ipad.fas.usda.gov/cropexplorer/ | WAP monthly on WASDE day | Country yield (e.g. Brazil 2025/26 soy 3.63 t/ha) | Country; sub-national NDVI / soil-moisture maps | Public domain | **[S]** |
| **USDA ERS Westcott & Jewison 2013, FDS-13G-01** https://ers.usda.gov/publications/36652 | One-off method paper | The weather-adjusted trend-yield regression WAOB uses for May–July projections (Jun/Jul/Aug temperature and rain + mid-May planting progress) | US national | Public domain | **[S]**. There is **no live published output** — it is embedded in WASDE's pre-August yields |
| FAO GIEWS Crop Prospects and Food Situation https://www.fao.org/giews/reports/crop-prospects/en | Triannual (2026: 6 Mar, 3 Jul, 6 Nov) | No — cereals | Country | FAO CC BY 4.0 (recent) | **[S]** |
| CONAB *Acompanhamento da Safra* (§1.2) | Monthly | Yes — area, kg/ha, production by UF | UF | CC BY-ND 3.0 | **[SRC]** survey/agronomist estimate, not a weather index |
| BCBA PAS / BCR GEA (§1.3) | Weekly | Yes — national (2025/26 soy 31.3 and 32.1 qq/ha) | National / zone | Not licensed for reuse | **[S]** / **[NV]** |
| India DES Advance Estimates https://desagri.gov.in/statistics-type/advance-estimates/ | 1st AE ~Nov, 2nd ~Mar, 3rd ~May | State tonnage | State | GoI, no statement found | **[S]** |

### 2.4 Open models, if the track were ever activated

| Model / benchmark | Soy | Licence | Status |
|---|---|---|---|
| AgML **CY-Bench** (ESSD 2026) https://cybench.agml.org/ | **No** — maize and wheat only | EUPL-1.2 | **[S]** |
| SustainBench crop yield (US 857 counties, AR 135 departments, BR 32, 2005–16) https://sustainlab-group.github.io/sustainbench/docs/datasets/sdg2/crop_yield.html | Yes | Data CC BY-SA 4.0; code MIT | **[S]** |
| DSSAT-CSM CROPGRO-Soybean v4.8.6 https://github.com/DSSAT/dssat-csm-os | Yes (process model) | BSD-3-Clause | **[S]** |
| PCSE / WOFOST 6.x https://pcse.readthedocs.io/ | Via parameter sets | EUPL-1.2 | **[S]** |
| NASA Harvest GEOCIF / ARYA | Yes (operational for Crop Monitor) | No public code or licence found | **[S]** |
| GDHY (§1.5) | Yes | CC BY 4.0 | **[SRC]** |

**Reading [INF]:** the open statistical route (Schlenker–Roberts-style degree-day panel on NASS county + MAGyP department + IBGE municipality yields) is fully open-licensed and reproducible; the process-model route (DSSAT/WOFOST) is open-licensed but needs soil, cultivar and management inputs the project does not hold. Neither route has a maintained open soybean benchmark to grade against — CY-Bench excludes soy.

---

## 3. What the footprint build provides toward the chain, and what it does not

The audit's chain: *weather exposure → affected planted area → growth stage and soil conditions → estimated damage with uncertainty → production impact → supply-balance implications.*

**Status caveat [OBS]:** the footprint spec (`docs/specs/footprint-weather.md`, merged 2026-10-07) is build-ready, but build slices 0–6 ([#380](https://github.com/philipbergman6-glitch/Mirror-Market/issues/380)–[#386](https://github.com/philipbergman6-glitch/Mirror-Market/issues/386)) are **all open**; `pipeline/footprints.py` does not exist on `main`; the ERA5 1991–2020 download was started on the desk Mac 2026-10-07 (#380 body). Everything "provided" below is provided by the spec, and reaches the product only when the slices merge.

| Chain step | Spec provides | `main` provides today | Does not provide |
|---|---|---|---|
| Weather exposure | 20 crop footprints, each admin-1 units weighted by **SPAM 2020 production** of the priced crop (§1, §4.3); ECMWF IFS 00z 15-day forecast, day-1 values persisted and backfilled to 120 days (§5.6); labelled "day-1 estimate · ECMWF 00z", never "observed" | Layer 5 Open-Meteo pins (24 regions, one point each); six observed rules (`analysis/weather_alerts.py`), 30-day deficit withheld everywhere for lack of baseline | Any observation source (ERA5T, CPC, CHIRPS ruled out, §1 table); admin-2 weights (ruled out, ≤ 0.2 mm effect) |
| Normal / anomaly | ERA5 1991–2020 daily normal per footprint; 15-day rain tercile and rank of 31, Tmax mean/SD (§6.2) | Nothing — the 90-day self-baseline only | A soil normal ("no soil anomaly", §1) |
| Affected planted area | — | — | **Nothing.** SPAM 2020 is a single-year map; no planted area by season, no NASS/CONAB/MAGyP area by stage, no crop mask by year |
| Growth stage | Month-granular `WEATHER_GROWING_SEASON_MONTHS` and `WEATHER_SOY_POD_FILL_MONTHS` gate the heat rules (§7.3 `fc_pod_fill_heat`) | Same two config maps | Dated phenology per footprint-season (emergence, R1, R5); any link to Crop Progress stage percentages that Layer 2b already fetches |
| Soil conditions | `vsw` level 1 at step 0 and step 360: level and 15-day change only; no alert (§1, R4 soil disagreement 0.07 m³/m³) | Nothing | Soil-moisture **history** or anomaly |
| Damage with uncertainty | — | — | **Nothing.** No damage function, no loss table, no uncertainty |
| Production impact | — | — | Nothing |
| S&D implications | — | Layers 6/12/15/25 carry the official balance sheets, un-linked to weather | Any weather-conditioned balance |

**Reading [INF]:** once slices 0–4 merge, the product owns step one (exposure, with a 30-year normal) and a defensible stage gate; steps two through six are absent by design, and the §1 spec table says each absence is a decision. The nearest *free* additions that would still be step-one work: linking Crop Progress stage percentages (already fetched, Layer 2b) to the US footprint cards, and swapping the desk-entered month maps for a sourced calendar (ZARC for Brazil, CC BY).

---

## 4. Cost sketch for the deferred track

In `ROADMAP.md`'s "What it would require" form (the table at "What is on the expansion track"):

| Deferred | What it would require |
|---|---|
| Quantified weather→yield (exposure → planted area → stage → damage with uncertainty → production → S&D) | **Outcome data:** NASS county yields (CC0, ≈1972→; API key; county programme shrinking), CONAB per-UF (CC BY-ND — derivative rights to confirm; 1976/77→; no vintages), IBGE PAM municipality (cite-source, 1974→), MAGyP departamento (CC BY 4.0, 1969/70→), Eurostat NUTS-2 (CC BY 4.0, 2000→); India/China/South Africa footprints **cannot be validated from open, reachable data** today. **Inputs the project does not hold:** planted area by season and stage (Crop Progress for US only; nothing comparable for South America short of satellite masks), a daily reanalysis history per footprint beyond the 1991–2020 normal (ERA5 per-year pulls, CDS hours-to-days each), a sourced phenology calendar per footprint-season. **Validation window:** ≥ 20 seasons of hindcast per footprint, scored against GDHY (to 2016) and the official series above, before any number renders — the audit's own precondition. **Build:** a Schlenker–Roberts-class degree-day / deficit panel is the cheapest open route; process models (DSSAT/WOFOST, open-licensed) need soil and cultivar inputs. **Licence reads:** CONAB BY-ND for derived series; CPC/CHIRPS/ERA5T as observation sources. **Trigger:** a paying client whose contract covers the analyst time for the hindcast programme and names the footprints they need; reopens as a fresh effort with its own design, not a resumption |

Not costed in hours; the ROADMAP table does not use hours for the other rows.

---

## Recommendation

If the track were activated, the first step is not a model: it is **a 20-season hindcast dataset** — NASS county (needs the key and an `agg_level_desc` change), MAGyP departamento and IBGE municipality yields joined to a per-footprint daily ERA5 history, scored against GDHY — because the audit's precondition is validation, and nothing in §2.3 lets the product borrow a validated weather-index number for any soy footprint (MARS Global outlook is Ukraine-only; GEOGLAM is qualitative; CONAB/BCBA/BCR are survey estimates). **Today, without a model, the one claim the page can make is a link, not a number:** a per-footprint line reading "Crop condition: GEOGLAM Crop Monitor for AMIS, No. 143, conditions as of 28 Sep 2026 — third-party assessment, not this project's observation" for the US, Brazil, India and China footprints (CC BY 4.0, monthly, first week), with MAGyP's monthly *Informe* as the link for Argentina and Paraguay, and the MARS *Crop monitoring in Europe* bulletin (numeric EU soy and rapeseed yield, CC BY 4.0, next 26 Oct) for the three EU footprints. It has the same shape as the #320/#328 TradingView exception — a link to someone else's page, dated with their date — and `pricing.semantics` never sees it. Two cheaper honesty fixes belong alongside it and need no scope decision: cite each `WEATHER_*` threshold in `config.py` to the §2.1 sources (34 °C sits between Schlenker–Roberts 30 °C and Science for Success 35 °C), and add the missing weather→yield row to `ROADMAP.md`, which map #296 says exists and does not.
