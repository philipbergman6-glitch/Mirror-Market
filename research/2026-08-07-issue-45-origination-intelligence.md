# Issue #45 — Market-structure & origination intelligence for physical soy buyers

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/45
- **Research date:** 2026-08-07 (all URLs accessed this date unless noted)
- **Question:** Who can a physical soy buyer buy from, at which origin, and what are the big players doing — and which parts of that can a FREE, static, daily-refresh dashboard actually cover?
- **Method:** Primary sources only (association sites, government APIs, publisher terms pages), fetched directly where possible. Four parallel research streams (Brazil; Argentina/Paraguay; India; US/freight/policy) + direct verification against the repo's existing fetchers. Every claim below is tagged **[observed]** (fetched/read directly), **[inferred]** (from consistent secondary evidence), or **[assumed]**; "couldn't verify" is stated where true.

---

## 1. Key finding first: two "new" datasets are already in (or adjacent to) the pipeline

1. **FAS export sales BY DESTINATION is already in the repo.** `fetchers/export_sales.py` hits `/exports/commodityCode/{code}/allCountries/marketYear/{yr}` on `api.fas.usda.gov/api/esr` and stores one row per `(commodity, week_ending, country)` (see `pipeline/schema.py` line ~141–147: PK `(commodity, week_ending, country)`). The briefing already surfaces top-3 buyers (`analysis/briefing/sections/export_sales.py`). **[observed — repo code].** No new sourcing needed; the gap is presentation (destination-trend views), not data. The FAS OpenData home page (`apps.fas.usda.gov/opendataweb/home`) is a JS app and only served a maintenance notice when fetched **[observed]** — but the live API behavior is proven daily by the pipeline itself. USDA data: public domain.

2. **US export inspections by destination country already exist inside the file the repo fetches.** `https://www.ams.usda.gov/mnreports/wa_gr101.txt` (the file `fetchers/usda.py` already downloads for Layers 14/14b) contains a table the parser currently skips **[observed — fetched 2026-08-07, week ending 2026-07-30]**:
   - Title: `GRAINS INSPECTED AND/OR WEIGHED FOR EXPORT BY REGION AND COUNTRY OF DESTINATION`
   - Header: `REGION  COUNTRY  WHEAT  YELLOW  WHITE  SORGHUM  SOYBEANS  TOTALS`
   - Sample rows (verbatim): `GULF  COLOMBIA  0  271,599  0  0  7,411  279,010`; `PACIFIC  JAPAN  0  230,709  0  9,001  0  239,710`; `INTERIOR  MEXICO  43,810  259,655  0  0  74,245  377,710`
   - Rows are grouped by region (LAKES, ATLANTIC, GULF, PACIFIC, INTERIOR) with a soybeans column — i.e., **weekly destination × region flows for free, public domain, from a file already being fetched.** Parsing it is a sibling of the existing `_parse_port_flows()`.

---

## 2. Per-origin player map

A **static reference page suffices** for the player map itself (ownership changes on a years cadence, not daily). Living feeds are only warranted for flows/prices (§3–4). India findings feed **issue #44** (flagged inline).

### Global ABCD+ (ADM, Bunge, Cargill, LDC, COFCO)

- **ADM** — verifiable via SEC 10-K, EDGAR CIK 0000007084 (US, Brazil, Argentina, Paraguay, India ops in segment/properties sections). **[inferred from filing structure; specific filing not read line-by-line this session.]** Open licence (SEC).
- **Bunge** — SEC CIK 1996862; FY2025 DEF 14A `https://www.sec.gov/Archives/edgar/data/1996862/000199686225000078/bg-20250404.htm` states leading South American soybean processor; traceability programs in Brazil, Argentina, Paraguay; elevators/crush/ports in Brazil, Argentina, US, Canada. **[observed via search excerpt of the filing.]**
- **Cargill** — private; cargill.com country pages are the primary. **Couldn't verify this session** (not fetched). Presence in US/Brazil/Argentina/Paraguay/India is well established **[assumed, cite company pages on build]**.
- **LDC** — `https://www.ldc.com/global-presence/` returns **403 to bots [observed]**; use the annual report PDF (manual download) as citation.
- **COFCO International** — `cofcointernational.com` global-presence page **403 to bots [observed]**; annual report PDF is the citable primary.
- Independent cross-checks (secondary, open): Trase (`trase.earth` — e.g. Paraguay: ABCD ≈64% of 2014 exports; Cargill+ADM ~40% by 2018, COFCO/Sodrugestvo ~20%) and Mighty Earth soy tracker (Brazil top exporters: ADM, ALZ, Amaggi, Bunge, Cargill, COFCO, LDC). **[observed via those sites/search.]**
- Caveat: none of these firms disclose origination volumes by origin — a static map can assert *presence*, not market share (except where Trase quantifies it).

### Brazil

- **ANEC (anec.com.br)** — **no public member list found**; `/associados` → 404, no "Associados" category in the CMS tree **[observed]**. But ANEC is the standout *flow* source (§3).
- **ABIOVE member crushers** — **public list [observed]** at `https://abiove.org.br/en/associates/`: 3 Tentos, ADM, Agrex, Amaggi, BTG Pactual, Bunge, Cargill, CHS, CJ Selecta, COFCO Intl, Fiagril, Imcopa, JBS, LDC, Olfar, et al. Statistics page `https://abiove.org.br/en/statistics/` publishes monthly soy-complex S&D and an exports report by trade partner and North Arch/South Arch port grouping; direct no-login xlsx confirmed: `https://abiove.org.br/abiove_content/Abiove/exp_202606.xlsx` (predictable `exp_YYYYMM.xlsx` pattern) **[observed]**. Licence: no terms statement found — **unclear**.

### US

- **FGIS Registered Grain Exporters Directory** (annual, as of Jan 1, 2026) — official entity-level exporter registry at `https://www.ams.usda.gov/services/fgis/international` → `https://fgisonline.ams.usda.gov/MyFGIS/RegisteredGrainExporters/Index`. The MyFGIS page is a Kendo/jQuery JS grid that renders "Please wait…" without JS — **not plain-HTTP scrapeable [observed]**; a waivers PDF is downloadable. Public domain. Lists *entities*, not elevator-by-elevator ownership.
- **Facility-level elevation ownership: no current primary directory exists.** The old GIPSA "Directory of Export Elevators Including Facility Data" last surfaced as a 2012 edition **[observed via a Scribd copy — couldn't verify any current edition]**. Gulf/PNW JV structure (EGT = Bunge/Itochu/STX; TEMCO = Cargill/CHS; Pacificor = ADM et al.) is common knowledge but was **not verified from primary sources this session — [assumed, needs per-company citation on build]**. USSEC pages (`ussec.org/about-ussec/ussec-members/`, buyer tools) are member/connection directories, not an ownership map **[observed via search]**.

### Argentina / Paraguay

- **CIARA-CEC (ciaracec.com.ar)** — stats hub intermittently HTTP 500 **[observed]**, but deep pages work: `https://www.ciaracec.com.ar/cec/Estadísticas/Exportación por producto` etc. Datasets include **export by company**, crushing, crush capacity, FOB prices, export-tariff evolution (query range 1970–2026) **[observed]**. Member list: **couldn't verify** (legacy image-nav homepage). Licence: unclear. Treat as fragile.
- **CAPPRO (cappro.org.py)** — `https://cappro.org.py/estadisticas`: monthly crush/export stats, server-rendered HTML, no login **[observed]**. Member list at `/camara` **[observed via search excerpt, not fetched]**: ~10 members — ADM, BISA, Bunge, CAIASA/CAHPSA, Cargill, ContiParaguay/Copagra, LDC, Merco, Oleaginosa Raatz, Pioneros del Chaco (two slightly different lists on site — verify on build). Licence: unclear.
- **Banco Central del Paraguay** — monthly foreign-trade reports with soy grain/oil/meal detail (`bcp.gov.py`) **[observed via search; pages not fetched]**. Effectively open (central bank) **[inferred]**.

### India *(→ feeds issue #44's verdict)*

- **SOPA (sopa.org)** — the standout. **[all observed 2026-08-07]:**
  - `https://sopa.org/solvent-extraction-plant/` — **public crusher directory, 14 paginated HTML pages, no login**, with installed crush + refining capacity per member (e.g., Adani Wilmar Rajasthan 450 TPD crush / 2,000 refining; ADM Agro Karnataka 800 TPD; Abis Export Chhattisgarh 500 TPD). This alone materially strengthens the issue-#44 case: India gets a capacity-annotated player map for free.
  - `https://sopa.org/soymeal-market-rate/` — Indore soymeal, Rs./MT, three bases incl. **FAS/FOB (Bedi/Kandla) in $/MT**; 526 pages of history. Caveat: on the page sampled, $/MT FOB cells were blank while Rs. ex-factory populated — **FOB fill rate unverified**.
  - `https://sopa.org/soybean-rate/` (222 pages history) and `https://sopa.org/statistics/` (crush, **port-wise/month-wise meal exports**, MP arrivals, member-wise crushing).
  - Server-rendered, no JS wall, no Cloudflare. Licence: **unclear** (no terms found) — same tier as CEPEA/Notícias Agrícolas already used.
- **SEA (seaofindia.com)** — public ~400-entry member table (`/all-members/ordinary-members/`), monthly oilmeal-export press releases **as PDFs** with destination tonnage (~2.5-month lag; inconsistent slugs), evergreen weekly comparative rates URL (`/weekly-comparative-rate-as-on-3rd-jan-2020/` always shows latest — data 2026-08-06 when fetched) **[observed]**. Daily rates by email are paid (Rs. 5,900/yr) **[observed via search]**. Licence: unclear.
- **Big processors** (static-page material): Patanjali Foods (ex-Ruchi Soya, ~11,000 TPD, 22 plants), AWL Agri Business (ex-Adani Wilmar), ADM India, Vippy Industries and Prestige Group (Dewas meal exporters), ITC Agri, Bunge/Cargill India **[inferred from cross-referenced secondary sources; capacities not all primary-verified]**.
- **APEDA AgriExchange** — both hosts failed from a US environment (DNS/conn refused) — **couldn't verify; doubtful for US-based CI [observed failure]**.
- Market context for #44: mid-2026 India soymeal FOB ~$605–695/MT vs ~$430 South America; exports heading to a 4-year low **[inferred, secondary]** — India is a *domestic* story right now, which the mandi feed already covers; SOPA adds structure (capacity map + meal stats), not tradeable export flow.

---

## 3. Flow intelligence

### Vessel lineups (Brazil + up-river Argentina)

- **Williams (williams.com.br)** — **no longer publishes free public lineups in 2026 [observed]**: expired TLS cert, old WordPress site, lineups behind `extranet.williams.com.br` client login. Their numbers reach the market via S&P/Reuters. **Restricted — not scrapeable.**
- **Cargonave** — `cargonave.com.br` **does not resolve (DNS NXDOMAIN) [observed]**. But **Cargonave's data is republished free inside the ANEC weekly PDF** (ANEC article body: "Estatísticas – Em colaboração com Cargonave") **[observed]**.
- **ANEC weekly export statistics** — the free proxy for Brazilian lineups. **[observed]:** Next.js site with all content embedded server-side in `__NEXT_DATA__` JSON (no JS execution, no login, no Cloudflare). Weekly articles "ANEC — Exportações Acumuladas NN.2026" (weeks 20–29 observed; May–Jul 2026), each with a no-auth PDF (e.g. week 29: `https://anec.com.br/uploads/cms6e2r1f0000ovtx25fh739y.pdf`, 873 KB): weekly shipments of soybeans/meal/corn/wheat **by port** (Santos, Paranaguá, S. Francisco do Sul, Itacoatiara, Rio Grande, Santarém, Barcarena…) + monthly projections; numbers revised retroactively at month-end. Archives to 2021. Licence: **no terms page anywhere on the site [observed] — unclear.** Backend caveat: the CMS JSON leaks admin emails + bcrypt hashes — fragile/unmaintained; could break or get walled without notice.
- **Alphamar** — free mobile app only; **not feasible** for a web pipeline **[observed]**. **SA Commodities/Unimar** — partner/subscriber platform; no free publication found **[observed]**. Some port authorities (e.g., Porto do Itaqui) publish daily berthing PDFs — per-port, inconsistent **[observed]**.
- **NABSA (Argentina up-river) — still free in 2026 [observed]:** `https://www.nabsa.com.ar/assets/vessel_update.pdf` — 5-page PDF created 2026-08-06, near-daily circulars, per-terminal lineups for Rosario/San Lorenzo incl. vessel, ETA, cargo, tonnage, destination, charterer (ADM, LDC…). Prior issues at `vessel_prior1..4.pdf`. Not linked from site nav; PDF metadata asserts copyright (`xmpRights:Marked=True`) — **licence unclear/restricted; free to download, trivial to fetch, PDF-parse required.**
- **Comex Stat** (authoritative monthly Brazil exports by NCM/state/destination/URF≈port): open API + bulk CSVs, CC-BY-ND-3.0 site licence **[observed on gov.br page]** — but both `api-comexstat.mdic.gov.br` and `balanca.economia.gov.br` returned **Cloudflare 403 from a US IP [observed]** — a real GitHub-Actions-runner risk; must be tested from CI before committing.

### US flows

- **FAS ESR by destination** — already in repo (§1). Public domain.
- **WA_GR101 destination table** — present in the already-fetched file (§1). Public domain.

### Tender news

- **GASC (Egypt)** — **no free structured feed exists (couldn't verify any official one) [observed by absence]**; announcements travel via paid wires; secondary sites (apk-inform, zawya) republish with lag, unstructured. Also, Egyptian strategic buying has partly shifted to direct deals via "Mostakbal Misr," outside the tender system — a GASC-only tracker would under-count **[inferred, secondary]**. **Not feasible.**
- **Indian meal tenders** — nothing free/structured found; paid newswires only (Reuters, AgriCensus, Platts) **[observed by absence]**. Closest free proxy: SEA monthly destination tonnage (PDF, ~2.5-month lag). **Not feasible as a live feed.**

---

## 4. Origination decision support

### FOB comparison across origins

- **Argentina — best find of this research: MAGyP official FOB prices have a free JSON API, no key. [observed — tested `Fecha=05/08/2026`]**
  `https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=dd/mm/aaaa` (mirror: `monitorsiogranos.magyp.gob.ar/ws/ssma/precios_fob.php`). Returns `fecha, circular, posicion, precio, mes/año desde-hasta` in USD/t, business days only. Products keyed by HS position (1201 beans / 1507 oil / 2304 meal chapters observed) — **code→product mapping is inferred, must be label-confirmed on first integration**. Landing page: `https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/fob_oficiales/`. Licence: open government data **[inferred — no explicit terms on page; datos.gob.ar convention is CC-BY]**. The datos.gob.ar mirror series is stale (ends 2025-01-21) — use the live API **[observed]**.
- **Brazil FOB** — already covered (Layer 19 AgRural Paranaguá).
- **US Gulf** — already covered (Layer 20, AMS 3147).
- **PNW — no free soybean FOB/basis exists. [observed]** AMS 3148 "Portland Daily Grain Bids" (`ams.usda.gov/mnreports/ams_3148.pdf`; `.txt` 404s) was read in full for 2026-08-06: wheat classes + oats only; soybeans appear only in the futures-settlement table. Whether soybean rows appear seasonally couldn't be verified (winter edition retrieval failed), but 2026 third-party summaries also show wheat-only. **Don't build this leg.** Nearest public proxies: GTR PNW shipment data + Gulf–PNW freight spread.
- **India meal FOB** — SOPA's FAS/FOB Bedi/Kandla $/MT column (fill rate unverified, §2); SEA weekly rates don't clearly quote soymeal FOB **[observed]**.
- **Rosario pizarra** (cash reference): `https://www.cac.bcr.com.ar/es` shows the daily pizarra free (Soja $505.000 / US$339,50 on 2026-08-06), server-rendered — but footer is "Todos los derechos reservados," **no open licence; redistribution rights unclear [observed]**.

### Freight

- **Baltic Exchange — confirmed restricted.** Terms pages themselves are behind a Cloudflare challenge **[observed — couldn't fetch the terms text]**; search snippets of balticexchange.com state data is licensed, redistribution needs a specific licence, FCA-regulated benchmark **[inferred]**. Also: panamax P4TC ceased publication 2026-01-30 (P5TC live). **Do not use.**
- **IGC GOFI** — `https://igc.int/en/markets/marketinfo-freight.aspx`, free to view (index 186 $/t on 2026-08-04), working data download `https://igc.int/en/_csv/igc_gofi.xlsb` **[observed, file verified]** — but IGC terms say "personal and non-commercial use… may not… distribute… publish" **[observed via policy snippet]**. **Fetchable but republication on a public dashboard is outside the terms — do not republish.**
- **USDA GTR — the freight answer. Public domain. [observed]** Weekly Grain Transportation Report + ~21 xlsx datasets at `https://www.ams.usda.gov/services/transportation-analysis/gtr-datasets`. `GTRFigure20.xlsx` was downloaded and parsed: monthly ocean rates **US Gulf→Japan and PNW→Japan + spread, back to Jan 1996** (Q2-2026: Gulf→Japan $69.27/MT, +49% YoY; PNW→Japan $36.25; spread $33.02). Weekly PDF adds actual fixtures incl. Gulf→China/PNW→China when fixed (source: Maritime Research Inc.). Also on Socrata with CSV/JSON API: `https://agtransport.usda.gov/Transportation-Costs/Grain-Transportation-Cost-Indicators/8uye-ieij`. Benchmark routes are to **Japan**, not China.

### Export taxes / quotas / policy

- **Argentina derechos de exportación — current (Aug 2026): Decreto 423/2026** (B.O. 2026-06-03, effective 2026-06-04): soybeans **24%** now, stepping −0.25 pp/month from Jan-2027 to 21% (Dec-2027), then −0.50 pp/month to **15% (Dec-2028)**; soy oil/meal/pellets **22.5%** → 19.5% → **14%** (refined oil ends 13.5%); applies by DJVE shipment date. **[inferred — cross-referenced across ≥4 independent sources (Infobae, La Nación, Beccar Varela, CIRA); the decree text itself was not fetched.]** **No machine-readable source** — build a manually-maintained reference table; the step-down schedule is computable from the Anexo.
- **Indonesia palm levy** — CPO levy **10% → 12.5%** of reference price effective 2026-03-01 under **PMK 9/2026** (July 2026: ref price $1,000.90/MT, duty $148, levy $125.11) **[inferred — consistent secondary sources; PMK text not fetched]**. Best English primary: USDA FAS GAIN Jakarta 2026 (public domain). Monthly manual update; no feed.
- **AMIS policy database** (`amis-outlook.org`) — best free multi-country policy tracker (export restrictions, tariffs) but the app is a JS SPA **[observed shell only]**; Market Monitor PDFs (~monthly) at stable Strapi URLs are scrapeable per issue. FAO licence terms **couldn't be verified**.

---

## 5. Candidate data-source table

| Source | Publisher | Content | Cadence | Licence verdict | Feasibility |
|---|---|---|---|---|---|
| ESR by destination (in repo) | USDA FAS | Weekly sales/shipments per buyer country | Weekly (Thu) | Public domain | Already fetched; presentation-only work |
| WA_GR101 destination table | USDA AMS/FGIS | Weekly inspections by region × destination country × grain | Weekly | Public domain | Parser sibling of `_parse_port_flows()`; file already fetched |
| ANEC weekly stats PDF | ANEC (data: Cargonave) | Brazil weekly exports by port + projections | Weekly (few days' lag) | **Unclear** (no terms page; publicly posted) | High: `__NEXT_DATA__` JSON → PDF table extraction; fragile backend |
| ABIOVE exports xlsx | ABIOVE | Monthly soy-complex exports by partner + port arch; member list | Monthly | Unclear (no terms) | High: predictable `exp_YYYYMM.xlsx`, no login |
| Comex Stat API/CSV | MDIC (Brazil gov) | Monthly exports by NCM/state/destination/port | Monthly (~1-mo lag) | Open, CC-BY-ND site licence | **Blocked 403 from US IP — must test from CI first** |
| MAGyP FOB API | Argentina SAGyP | Official daily FOB, soy complex, USD/t | Daily (bus. days) | Open (inferred) | Trivial JSON, no key; HS-code mapping to confirm |
| NABSA lineup PDF | NABSA (agency) | Up-river vessel lineup: vessel/ETA/tonnage/destination/charterer | ~Daily | Unclear (© asserted) | Easy fetch, PDF parse; fixed URL |
| CAC Rosario pizarra | Cámara Arbitral/BCR | Daily cash price Rosario | Daily | **Restricted** (all rights reserved) | Easy but don't republish without permission |
| CIARA-CEC stats | CIARA-CEC | Monthly crush, export by company, capacity | Monthly | Unclear | Fragile (intermittent 500s) |
| CAPPRO stats | CAPPRO (Paraguay) | Monthly crush/exports; 10-member list | Monthly | Unclear | Easy HTML; narrative format |
| SOPA directory + rates | SOPA (India) | Crusher directory w/ capacity; Indore bean/meal rates incl. nominal FOB; meal export stats | ~Daily rates; monthly stats | Unclear (no terms) | Easy server-rendered HTML; FOB fill rate unverified |
| SEA oilmeal exports | SEA (India) | Monthly meal exports by destination; member list; weekly rates | Monthly (~2.5-mo lag) | Unclear | HTML easy; stats need PDF parse, inconsistent slugs |
| GTR datasets | USDA AMS | Ocean freight Gulf/PNW→Japan monthly since 1996 + spread; Gulf vessel queue; fixtures in weekly PDF | Weekly/monthly | **Public domain** | Excellent: static xlsx + Socrata JSON API |
| IGC GOFI | Intl Grains Council | Grain freight index + origin sub-indices | Weekly | **Restricted for republication** (personal use only) | Fetchable xlsb — do not republish |
| Baltic Exchange | BEISL | Panamax/freight benchmarks | Daily | **Restricted — licensed only** | Not usable |
| AMS 3148 Portland | USDA AMS | PNW export bids — **wheat/oats only, no soybeans** | Daily | Public domain | N/A for soy |
| FGIS exporter directory | USDA AMS | Registered exporter entities | Annual | Public domain | JS grid — not plain-HTTP scrapeable; PDF fallback |
| GASC / Indian meal tenders | — | Tender flow | — | — | **Not feasible free** (paid wires only) |
| Argentina export tax | Boletín Oficial | Decree rates + step-down schedule | Ad hoc | Open (law text) | Manual reference table + computed schedule |
| Indonesia palm levy | Kemenkeu/BPDP | Levy %/reference price | Monthly | Open (regulation) | Manual reference; FAS GAIN as English primary |
| AMIS policy DB | FAO AMIS | Export restrictions/tariffs | Irregular | Couldn't verify | JS SPA; Monitor PDFs scrapeable |

---

## 6. Ranked recommendations

Ranking = value-to-physical-buyer ÷ build cost, given a free static daily-refresh dashboard.

### #1 — Argentina official FOB leg → cross-origin FOB comparison board (build)
The missing third leg. Repo already has Brazil FOB (AgRural, L19) and US Gulf (AMS 3147, L20); the MAGyP JSON API adds official daily Argentina FOB for beans/oil/meal in USD/t with a no-key JSON endpoint. Result: a Gulf vs Paranaguá vs Up-river FOB board — the core "which origin is cheap" question — entirely from open sources.
**Success criteria:** daily fetch of beans/oil/meal FOB with confirmed HS-code→product mapping; new table + history CSV round-trip (snapshot-source, like AgRural); briefing/dashboard section showing three-origin FOB spread in USD/MT; hard-fail if the API shape changes.
**Risks:** HS mapping unverified (one-time confirmation); licence inferred-open, not explicit.

### #2 — US flow-by-destination, zero new sources (build)
Two presentation/parse tasks on public-domain data already flowing: (a) parse the WA_GR101 destination-country table (soybeans column, region-grouped) alongside the existing Table C parse; (b) surface ESR destination trends beyond top-3 (e.g., China share trend, "new/absent buyers this week"). Highest value-per-line-of-code in the whole ticket.
**Success criteria:** `inspection_destinations` table keyed (week_ending, region, country, commodity); briefing line "Soybeans by destination: Mexico X, China Y…"; ScraperShapeError on structural drift; history CSV if >3-week retention needed.

### #3 — Brazil weekly port-flow feed via ANEC (build, licence-gated)
The free proxy for vessel lineups: weekly Brazil soy exports by port (Cargonave-sourced) with monthly projections. Direct agency lineups (Williams/Unimar/Alphamar/Cargonave) are all gated — ANEC is the only free path, and it's technically easy (server-side JSON + PDF).
**Success criteria:** weekly discovery of the newest "Exportações Acumuladas" article, PDF parse into (week, port, commodity, mt); tolerate retroactive month-end revisions (INSERT OR REPLACE); degrade gracefully if the site changes.
**Gate:** no terms page exists — before shipping, send ANEC a permission email (the audit's licensing-RED standard); the fragile CMS also argues for treating this as best-effort with a freshness warning.

### #4 — Static reference pages (cheap, do alongside)
- **Player map page** (static HTML, quarterly manual refresh): ABIOVE associates (observed), SOPA crusher directory w/ capacities (observed), CAPPRO members, CIARA-CEC export-by-company, FGIS exporter registry, SEC filings for ADM/Bunge; mark Cargill/LDC/COFCO claims with per-company citations. Static suffices — ownership churns slowly.
- **Policy reference table** (manual + computed): Argentina Decreto 423/2026 step-down schedule (programmable to Dec-2028); Indonesia PMK 9/2026 levy. Update on decree events only.
- **Freight context** (if wanted later): GTR Figure 20 xlsx (public domain, monthly Gulf/PNW→Japan + spread) — trivial parse, but it's Japan-benchmark and monthly, so it's context, not a daily signal. Rank below #1–3.

### Not feasible without paid data (explicit list)
- **Broker offers / private cash bids** at any origin (incl. PNW soybean FOB/basis — AMS 3148 has no soy; confirmed).
- **Direct vessel lineups** from Williams (extranet), SA Commodities/Unimar (subscriber), Alphamar (app-only), Cargonave (site dead).
- **Tender flow**: GASC Egypt, Indian meal tenders — paid wires only; GASC additionally under-counts due to Mostakbal Misr direct deals.
- **Baltic freight indices** (licensed benchmark; terms wall) and **IGC GOFI republication** (personal-use terms).
- **Real market-share/origination volumes for ABCD+** (not disclosed; only Trase's dated estimates).
- **Rosario pizarra republication** (all-rights-reserved; viewable but not licensable for a public page without permission).

---

## 7. Follow-up implementation ticket outlines

### Ticket A — Layer 14 extension: WA_GR101 inspections by destination country
- Add `_parse_destinations()` to `fetchers/usda.py` for the `BY REGION AND COUNTRY OF DESTINATION` table (anchor on title; columns WHEAT/YELLOW/WHITE/SORGHUM/SOYBEANS; rows grouped LAKES/ATLANTIC/GULF/PACIFIC/INTERIOR; drop TOTALS).
- New table `inspection_destinations(week_ending, region, country, commodity, inspections_mt)`; add to `pipeline/history.py` if retention beyond re-fetch window matters.
- Briefing: add destination lines to section 12; snapshot_json block.
- ESR presentation: destination-trend summary (China share, new buyers) in section 11 — data already in `export_sales`.
- Est: S. Licence: public domain — no gate.

### Ticket B — Layer 21: Argentina official FOB (MAGyP)
- New `fetchers/magyp_fob.py`: GET `precios_fob.php?Fecha=dd/mm/yyyy` (retry via `_backoff`), map HS posiciones → Soybeans/Soybean Oil/Soybean Meal (confirm labels on first run; hard-fail on unknown codes), skip non-business days.
- Table `argentina_fob(date, product, position, price_usd_mt)`; history CSV (snapshot-ish; API supports date param so backfill is possible — verify depth).
- Briefing: extend section 4 into a three-origin FOB board (Gulf CIF vs Paranaguá FOB vs Up-river FOB, USD/MT); market-drivers hook for origin-switch signal.
- Est: M. Licence: inferred open — cite MAGyP on the page.

### Ticket C — Layer 22: ANEC weekly Brazil port flows (licence-gated)
- Precondition: permission email to ANEC; record outcome in the licensing register.
- `fetchers/anec.py`: fetch `anec.com.br` search page for the current-year stats category, parse `__NEXT_DATA__` for the newest weekly article + `articleMediaFiles` PDF cuid; download `uploads/<cuid>.pdf`; extract per-port weekly table (pdfplumber).
- Table `brazil_port_flows(week, port, commodity, mt)`; INSERT OR REPLACE (month-end revisions); history CSV.
- Health: freshness warning >10 days; ScraperShapeError on layout drift.
- Est: M–L (PDF table variability).

### Ticket D — Static pages: player map + policy reference
- `docs/players.html` generated from a hand-maintained YAML (`data/reference/players.yml`) with per-claim citation URLs + accessed dates; sections per origin (§2). Optional: small scraper to refresh the SOPA directory (14 pages) and ABIOVE associates into the YAML.
- `data/reference/export_taxes.yml`: Argentina Decreto 423/2026 schedule (computed monthly rate to 2028), Indonesia PMK 9/2026; render on the policy card with "as of" dates.
- Est: S–M. No live pipeline risk.

### Ticket E (stretch) — GTR ocean-freight context card
- Parse `GTRFigure20.xlsx` (or Socrata API) monthly: Gulf→Japan, PNW→Japan, spread; render as context chart. Public domain. Est: S.

---

## 8. Cross-reference

- **Issue #44 (India sufficiency):** SOPA findings here (capacity-annotated crusher directory, meal-rate history with nominal FOB column, port-wise meal-export stats — all free and scrape-friendly; licence unclear) are the strongest new evidence. India remains a domestic-price story in 2026 (FOB uncompetitive, exports at 4-yr low), so the mandi feed + a SOPA-derived static player map likely suffices without a new live layer; SOPA meal rate is the one candidate live series if #44 wants a second leg.
- **2026-08 audit licensing register:** new RED/unclear entries to log — ANEC (no terms), ABIOVE (no terms), SOPA/SEA (no terms), NABSA (© asserted), CAC Rosario (ARR), IGC (personal-use), Baltic (licensed). GREEN: all USDA (ESR, WA_GR101, GTR, 3147/3148), MAGyP (inferred), Comex Stat (CC-BY-ND, geo-block caveat), SEC EDGAR.
