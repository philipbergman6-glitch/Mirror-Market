# Research: Market Structure & Origination Intelligence for Physical Soy Buyers

- **Ticket:** #45 (Wayfinder research) — companion to #44 (India price sufficiency)
- **Date:** 2026-08-07
- **Method:** primary-source verification (live HTTP fetches against official sites/APIs/endpoints, cross-referenced ≥2 sources for key claims) + repo code inspection. Claims are tagged **[verified]** (directly observed), **[inferred]**, or **[unverified]**.

---

## 1. Headline answers

1. **FAS export sales BY DESTINATION: we already have it.** The repo's Layer 10 endpoint `/exports/commodityCode/{code}/allCountries/marketYear/{year}` returns per-destination-country weekly rows, and the `export_sales` table already keys on `(commodity, week_ending, country)` — destination detail is fetched and stored today. The gap is purely analysis/display (only "top 3 buyers" + China share are surfaced). No new fetcher needed. **[verified — repo code + API docs]**
2. **US inspections by destination: same file we already parse.** `wa_gr101.txt` (already fetched for Table C port flows) contains a "SOYBEANS INSPECTED AND/OR WEIGHED FOR EXPORT BY PORT AREA AND COUNTRY OF DESTINATION" table — port area × destination country, weekly, public domain. **[verified — live fetch]**
3. **Vessel lineups: national aggregation is NOT free; per-port is.** Cargonave and Williams gate their lineups behind client logins; the free public window is (a) ANEC's weekly PDF (port × product totals + destination shares, "Source: Cargonave") and (b) port-authority pages — Paranaguá APPA serves a real-time server-rendered HTML lineup table with vessel, commodity, operator, tonnage. **[verified]**
4. **Freight: Baltic is licence-walled; USDA AgTransport is the public-domain substitute.** Baltic Exchange data requires a paid redistribution licence (and P4TC panamax ceased publication 2026-01-30). USDA AgTransport Socrata endpoints serve weekly Gulf/PNW ocean-freight indices and grain basis (incl. PNW soybeans) as free JSON, no key. **[verified — live API hits]**
5. **Licence pattern:** everything US-government (FAS ESR, AMS WA_GR101, AgTransport, GTR) is public domain; everything trade-association or exchange (ANEC/Cargonave, ABIOVE, CIARA-CEC, BCR, SOPA, SEA, CAPPRO, Baltic, Williams) has **no explicit reuse grant** — red per the 2026-08 audit standard. Argentine government data (MAGyP DJVE, Boletín Oficial) is official and open-data-policy covered but page-level terms were not found **[unverified]**.

---

## 2. Landscape A — per-origin player map

**Verdict: static reference page, not a feed.** Nothing in the player structure changes faster than ~annually, and the one thing that does (exporter market share / who's shipping this week) is not published free anywhere (ANEC aggregates have no company names **[verified — grepped the PDF]**; Cargonave/Williams gate vessel-level charterer data). A hand-maintained markdown/dashboard reference with an "as of" date, refreshed opportunistically, is the right cost point.

Player facts below are assembled from association member pages **[verified where noted]** and general industry knowledge **[unverified — verify against company/association pages before publishing a reference page]**.

### Global ABCD+ footprint (all origins)
ADM, Bunge (now Bunge Global after Viterra merger), Cargill, Louis Dreyfus (LDC), COFCO International — all five originate in US, Brazil, Argentina, Paraguay; COFCO is the largest single buyer channel into China. Regional majors: Amaggi (Brazil), ACA & AGD & Molinos Agro (Argentina), CHS (US/global). **[unverified detail; CIARA-CEC member page links to ADM, Bunge, Cargill, COFCO, LDC, AGD, Molinos Agro, ACA, Amaggi, CHS — verified]**

### Brazil
- **Exporters:** ANEC members ship the overwhelming share of beans/meal. ANEC publishes weekly export stats but **no per-company rankings** **[verified]**.
- **Crushers:** ABIOVE members — member list at `https://abiove.org.br/associados/` is a logo grid (names not in alt-text; manual extraction needed) **[verified]**. ABIOVE publishes installed-crush-capacity xlsx (see §3).

### United States
- **Exporters / elevation:** Gulf (Mississippi River) export elevation is dominated by ADM, Bunge, Cargill, CHS, Zen-Noh (ZGC), LDC; PNW export terminals: EGT (Bunge/Itochu/STX JV, Longview), TEMCO (Cargill/CHS JV, Tacoma/Kalama/Portland), Kalama Export (Marubeni/Columbia), United Grain (Mitsui, Vancouver WA), Louis Dreyfus (Portland/Seattle). **[unverified — industry knowledge; no single free authoritative list exists. FGIS export-elevator directory could substantiate.]**

### Argentina / Paraguay
- **Crushers/exporters:** CIARA-CEC members (verified link list above). Up-river Rosario/San Lorenzo plants: AGD, Bunge, Cargill, COFCO, LDC, Molinos Agro, ACA, Viterra(-Bunge), Renova/T6 JVs. **[member links verified; plant detail unverified]**
- **Paraguay:** CAPPRO members (ADM, Bunge, Cargill, LDC, COFCO expected; member page contents **[unverified]**).

### India
- **Crushers / meal exporters:** SOPA's plant directory is free and public — `https://sopa.org/solvent-extraction-plant/` names Adani Wilmar, ADM Agro Industries India, BCL Industries, Betul Oils, etc. **[verified]**. Patanjali/Ruchi expected under other member categories **[unverified]**. SEA of India member lists at `https://seaofindia.com/all-members/` **[link verified, contents not fetched]**. Feed for #44: India's exporter set is public and stable — static reference suffices.

---

## 3. Landscape B — candidate data sources

Legend: **Lic** ✅ = public domain / explicit grant, ⚠️ = no reuse grant found (association/exchange — display derived numbers with attribution, don't rehost), ❌ = paid/prohibited. **Feas** = scrape/API feasibility for a no-JS daily CI job.

### Flow intelligence

| Source | What it publishes | Cadence | Lic | Feas | Notes |
|---|---|---|---|---|---|
| **FAS ESR API** (`api.fas.usda.gov/api/esr`) | Weekly export sales per destination country: `weeklyExports`, `accumulatedExports`, `outstandingSales`, `grossNewSales`, `currentMYNetSales`, `currentMYTotalCommitment`, `nextMY*` + `/countries`, `/regions`, `/datareleasedates` lookups | Weekly (Thu ~08:30 ET) | ✅ | Already integrated | **[verified]** Watch: new **ESRQS** launched 2026-03-26 (`apps.fas.usda.gov/esrqs/`), legacy query site retired 2026-04-02; FAS states "No registration or login is required in ESRQS to access… the API" — may eventually let us drop `FAS_API_KEY`. Current OpenData gateway still works with `X-Api-Key`. |
| **AMS WA_GR101** (`ams.usda.gov/mnreports/wa_gr101.txt`) | Soybeans inspected for export **by port area × destination country** (China, Japan, Indonesia, Egypt…); parallel by-region tables | Weekly (Mon ~11:00 ET) | ✅ | Same fixed-width .txt already parsed for Table C | **[verified — table present in live 2026-07-30 file]** Destination "as known at time of exportation". |
| **ANEC weekly stats** (`anec.com.br`) | PDF: weekly shipments by port (Santos, Paranaguá, Itaqui…) per product, actual + scheduled; monthly YoY; destination shares (e.g. soy "CHINA 71%"). Footed "Source: Cargonave" | ~Weekly (skips/doubles observed) | ⚠️ underlying data is Cargonave's | Medium: article list embedded in `__NEXT_DATA__` JSON (no JS needed), free PDF download; needs PDF table extraction | **[verified — wk29/2026 PDF downloaded]** No per-company exporter rankings. |
| **Paranaguá APPA lineup** (`appaweb.appa.pr.gov.br/appaweb/pesquisa.aspx?WCI=relLineUpRetroativo`) | Real-time vessel lineup: vessel/IMO/DWT, berth, agency, **operator, commodity, planned/realized tonnage**, ETA/berthing | Real-time | ⚠️ govt port authority, terms unverified | High: server-rendered plain HTML table, ~195 KB, no JS/anti-bot | **[verified live fetch, stamped 07/08/2026]** One port only — but it's *our* basis port. |
| Itaqui port "Mapa de Atracação" | Daily lineup PDF incl. shipper (AMAGGI, CHS…) and agent | Daily | ⚠️ | Low-medium: random filename suffixes, index page URL unverified | **[PDF verified; discovery path unverified]** |
| Williams (`williams.com.br`) | Public site is a news blog; raw lineup behind `extranet.williams.com.br` login. TLS cert expired | — | ❌ (gated) | Not feasible | **[verified]** |
| Cargonave (`cargonavegroup.com`) | Client-area app (`/ClientArea`, `/VesselTracking`…), no public data | — | ❌ (gated) | Not feasible; free window is the ANEC PDF | **[verified]** |
| **MAGyP DJVE** (Argentina declared export sales) | Cumulative DJVE registrations by campaign, product (SOJA, ACEITE, SUBP./meal) × shipment month, updated daily with 1-day lag | Daily | ✅-ish (official govt; page terms unverified) | High: server-rendered HTML tables, `pandas.read_html`, no key | **[verified — "al 06 de Agosto de 2026"]** URL: `magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/_archivos/000012_….php` |
| CIARA-CEC stats | Crush, exports by product/company, FOB — but public DB **frozen at 2022** (2023–2026 queries return empty) | Frozen | ⚠️ | CSRF+session POST dance; works but historical only | **[verified]** Monthly FX-liquidation press releases continue (Google Drive PDFs). |
| Tender news (GASC etc.) | **No free structured source.** GASC stripped of procurement Dec-2024; Mostakbal Misr buys bilaterally, no transparent tenders. Only paid wires (Reuters/AgriCensus) + unstructured relays | — | ❌ | Not feasible | **[verified via 2 independent sources]** Skip as a layer. |

### Origination decision support

| Source | What it publishes | Cadence | Lic | Feas | Notes |
|---|---|---|---|---|---|
| **USDA AgTransport — Grain Basis** (`agtransport.usda.gov/resource/v85y-3hep.json`) | `date, market_name, commodity, bid, contract_month, futures_price, basis` for **Pacific Northwest**, Louisiana Gulf, Texas Gulf, Atlantic — soybeans/corn/wheat | Weekly | ✅ | High: Socrata JSON, no key | **[verified live — fields quoted from response]** Fills the missing PNW leg. Companions: prices `g92w-8cn7`, spreads `an4w-mnp7`. |
| **USDA AgTransport — Transport Cost Indicators** (`…/resource/8uye-ieij.json`) | Weekly indices `gulf_vessel`, `pacific_vessel` (+truck/rail/barge), 2017=100; base rates Gulf→Japan $39.33/MT, PNW→Japan $21.05/MT; latest row 2026-08-05 | Weekly | ✅ | High: Socrata JSON, no key | **[verified live]** Barge-rate datasets `27ck-4bei`, `42t2-gvpa`. Gulf→China quarterly in GTOR. |
| USDA GTR PDF | Ocean freight tables (Gulf→Japan, PNW→Japan), barge %-of-tariff, vessel counts | Weekly PDF | ✅ | Low (PDF; HTML pages 403 non-browser UA) | Use the Socrata datasets instead. |
| **IGC GOFI** (`igc.int/en/markets/xx-marketinfo-freight.aspx`) | Grain & Oilseeds Freight Index + route sub-indices (Brazil, Argentina, US…) | Daily/weekly | ⚠️ "© IGC 2026", no reuse text | Medium: download is `.xlsb` (needs pyxlsb) | **[verified]** Backup if AgTransport insufficient. |
| **Baltic Exchange** | BDI/panamax TC | Daily | ❌ paid redistribution licence required; P4TC ceased 2026-01-30 | Not feasible | **[verified]** Do not scrape. |
| **BCR FOB/FAS up-river** (`bcr.com.ar/...fobfas-argentina/...`) | Daily FOB up-river soybean/oil/meal USD + FAS teórico, per-day pages + PDF, 700+ page archive | Daily | ⚠️ "Todos los derechos reservados" | Medium: free, no registration, per-day node URLs | **[verified one node]** Legally grey for republication — derived basis numbers w/ attribution only. |
| CAC/BCR pizarra (`cac.bcr.com.ar/es/precios-de-pizarra`) | Daily pizarra soy/wheat/corn ARS + BNA FX | Daily | ⚠️ | High: server-rendered | **[verified — 05/08/2026 Soja $505,000/tn]** |
| **Argentina export tax** — Decreto 423/2026 | Beans **24%**, meal/oil **22.5%**, biodiesel 21% (in force 04-Jun-2026); legislated monthly step-downs from Jan-2027 (beans →15%, products →14% by Dec-2028) | Static + scheduled | ✅ official (Boletín Oficial / InfoLeg) | Config constant, not a scraper | Rates cross-checked across 3+ legal summaries **[decree annex itself not parsed]**. |
| **ABIOVE statistics** (`abiove.org.br/abiove_content/Abiove/exp_YYYYMM.xlsx`) | Monthly Brazil soy-complex exports xlsx; crush-capacity xlsx; biodiesel xlsx. S&D balance page is a Power BI iframe (not scrapeable) | Monthly, ~1-mo lag | ⚠️ no terms found | Medium-high: direct xlsx, but site 406s non-browser UAs (needs full Chrome UA string) | **[verified — exp_202606.xlsx downloaded]** |
| **SOPA soymeal rate** (`sopa.org/soymeal-market-rate/`) | Daily-ish rows: FAS/FOB Bedi/Kandla $/MT, FOR Rs/MT, Ex-Factory Indore Rs/MT — **but $ FOB cell often blank in recent rows**; bean-rate page stale since 2020 | Daily (INR legs) | ⚠️ T&C page has no data-reuse grant | Medium | **[verified — rows through 31-Jul-2026]** |
| **SEA of India oilmeal circular** (`seaofindia.com/category/statistical-update/export-of-oilmeals/`) | Monthly oilmeal export volumes + Indian SBM FOB vs intl comparison (Apr-2026: $605 vs ~$430) | Monthly, ~mid-month +1 | ⚠️ "All Rights Reserved" | Medium: scrape category page for latest PDF link (filenames not predictable) | **[verified — EC-088 PDF fetched]** |
| CAPPRO (`cappro.org.py`) | Monthly boletín PDFs (crush, exports); JSON listing API `/posts/public` exists but params unverified | Monthly | ⚠️ | Medium | **[stats page + PDF store verified]** |

---

## 4. Recommendation — build-worthy features (value ÷ build cost)

The platform is a free, static, daily-refresh dashboard for physical buyers (cash/basis > real-time). Ranked:

### 1. Destination-flow analytics from data we already have (ESR destinations + WA_GR101 destination table)
- **Why #1:** zero licence risk (public domain), near-zero fetch cost (ESR rows already in the DB with `country` in the PK; WA_GR101 .txt already downloaded each run), and it directly answers "who is buying, and is China's share shifting."
- **Scope:** (a) new briefing/dashboard block: per-destination outstanding sales + accumulated exports, top-N destinations with WoW/YoY deltas, China-share trend (snapshot already computes a point-in-time China share); (b) extend the WA_GR101 parser with the "by port area and country of destination" soybean table → new `inspection_destinations` table + history CSV.
- **Success criteria:** dashboard shows top-5 destination commitments and China share history ≥8 weeks deep; inspections destination table populated weekly in CI with no new keys; briefing degrades gracefully when either is absent.

### 2. US origination layer via USDA AgTransport Socrata (PNW basis + Gulf/PNW freight)
- **Why #2:** fills two ticket gaps (PNW FOB leg, free freight indication) from one public-domain JSON API with no key; makes a true cross-origin table possible: Paranaguá FOB (L19) vs CIF NOLA (L20) vs PNW (new) ± Gulf/PNW vessel-freight indices.
- **Scope:** fetcher for `v85y-3hep` (soybean basis/bid, PNW + Gulf markets) and `8uye-ieij` (gulf_vessel/pacific_vessel indices); "Origination" dashboard section: FOB comparison in USD/MT + freight-adjusted indication; weekly cadence matches the buyer's needs.
- **Success criteria:** cross-origin table renders with ≥3 origins in USD/MT with as-of dates per leg; freight indices plotted 52w; layers keyless and CI-safe (history CSVs if snapshot-only).

### 3. Argentina DJVE declared export sales (daily, official)
- **Why #3:** Argentina is the #1 meal/oil exporter and we have zero Argentine flow data; the DJVE page is official, daily, server-rendered HTML → `pandas.read_html`; lowest-friction new origin signal ("is Argentina forward-selling meal aggressively?"). Pair with a static Decreto 423/2026 export-tax config (with the legislated 2027–28 step-down schedule) for the policy-tracker ask.
- **Success criteria:** daily DJVE campaign totals for soy bean/oil/meal stored with history; briefing line shows WoW registration pace; export-tax constants displayed alongside FOB comparisons.

**Honorable mention (build 4th, behind the above):** ANEC weekly Brazil port×product flow + destination shares — good buyer value, but PDF table extraction + unclear Cargonave licence (display derived aggregates with attribution only) push it down. The Paranaguá APPA lineup is a compelling high-feasibility add-on (vessel/commodity/operator at our basis port) but is realtime-operational rather than daily-analytic; prototype only if a concrete use (e.g. port congestion proxy = soy tonnage waiting) is defined.

**Player directory:** ship as a static, dated reference page (markdown → dashboard), sourced from association member pages; no feed.

---

## 5. NOT feasible without paid data (honest scope exclusions)

- **National vessel-level lineups with charterer/exporter names** — Cargonave and Williams are client-gated; free coverage is per-port authorities only.
- **Exporter rankings by company (Brazil or anywhere)** — not published free; ANEC aggregates carry no company names.
- **Baltic Exchange indices** (BDI, panamax TC) — paid redistribution licence; P4TC discontinued 2026-01-30 anyway.
- **Tender flow (GASC-style)** — transparent Egyptian tenders ended Dec-2024 (Mostakbal Misr bilateral deals); structured tender data is Reuters/AgriCensus paid territory.
- **Broker offers / private cash bids / trade rumors** — inherently private.
- **Live Argentina crush/export-by-company** — CIARA-CEC public stats frozen at 2022.
- **Daily India soybean FOB in USD** — SOPA's $ column is intermittently blank; NCDEX remains suspended (see #44).

## 6. Licence red flags (audit follow-up)

- **Safe (public domain):** FAS ESR, AMS WA_GR101/GTR, AgTransport, Boletín Oficial/InfoLeg. MAGyP DJVE near-safe (official; page-level terms unverified).
- **Grey — derived numbers + attribution only, never rehost files:** ANEC (data owned by Cargonave), ABIOVE, BCR/CAC, SOPA, SEA, CAPPRO, IGC GOFI, APPA. None publish a reuse grant; none showed an explicit prohibition either **[verified absence on pages fetched]**.
- **Red — do not use:** Baltic Exchange, Cargonave/Williams gated data, paid newswires.

## 7. Proposed follow-up implementation tickets (do not auto-create)

1. **feat(analysis): export-sales destination analytics** — per-destination commitments, top-N deltas, China-share trend from existing `export_sales` rows; briefing + dashboard block.
2. **feat(fetcher): WA_GR101 destination-country table → `inspection_destinations`** — extend existing parser, schema + history CSV + briefing line.
3. **feat(layer): USDA AgTransport basis + freight (Socrata)** — fetch `v85y-3hep` + `8uye-ieij`; new "Origination" cross-origin FOB/freight comparison section.
4. **feat(layer): Argentina DJVE daily declared export sales** — `read_html` fetcher, history CSV, briefing pace line; include Decreto 423/2026 export-tax constants in `config.py`.
5. **feat(content): static per-origin player directory page** — dated reference (ABCD+ / ANEC-ABIOVE / CIARA-CEC / CAPPRO / SOPA-SEA), verify the US elevation-ownership entries against FGIS directory before publish.
6. **spike: ANEC weekly PDF extraction** — `__NEXT_DATA__` article discovery + pdfplumber table pull; go/no-go on parse stability across 4 consecutive weeks; licence-posture: derived aggregates only.
7. **spike: Paranaguá APPA lineup as congestion proxy** — parse HTML table, define one derived metric (soy tonnage in lineup) before committing to a layer.
8. **chore: ESRQS migration watch** — confirm whether the keyless ESRQS API can replace `FAS_API_KEY` on the OpenData gateway before legacy deprecation.
