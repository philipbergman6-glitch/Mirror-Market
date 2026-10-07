# A14 — Daily intelligence inputs that are datasets, not news

**Issue:** [#408](https://github.com/philipbergman6-glitch/Mirror-Market/issues/408) (map #296)
**Date:** 2026-10-07
**Author:** research agent (four parallel source-group sub-agents; synthesis by the parent)

Legend: **[SRC]** quoted from a primary source fetched today (URL given) · **[SNIP]** seen only in a
search-engine snippet or Wayback copy of the primary page (page blocked direct fetch) · **[SRC-2nd]**
secondary/press source · **[INF]** inference · **[NV]** not verified — do not treat as fact.

Standing constraints applied (map #296 Notes): no cloud DB; licensing gates *publishing*, not building;
every trap recorded in `LAYERS.md` style so a builder inherits it. Map #142's ruling that free-text news
is out of scope is **not reopened** — every item below is a dataset with a publisher.

---

## 0. Summary of recommendations

| # | Source | Feed | Licence | Recommendation |
|---|---|---|---|---|
| 1 | USDA FAS daily export-sales announcements ("flash sales") | **None** — HTML press releases only; fas.usda.gov 403s non-browser clients | US gov public domain | **list-with-link-only** |
| 2 | CFTC Disaggregated COT | Text/zip on cftc.gov + Socrata SODA API | Public domain, attribution requested | **ingest-as-layer** (alongside Legacy, not replacing) |
| 3a | WASDE release dates (OCE) | HTML prose sentence; no ICS/CSV | Public domain | **list-with-link-only** (12 hand-keyed dates/yr + on-time check) |
| 3b | NOPA crush release dates | PDF file on nopa.org | Dates: unstated; data: paid, LSEG sole distributor | **list-with-link-only** for dates; **do-not-pursue** the data |
| 3c | CONAB grain-survey calendar | PDF infographic, link rotates | gov.br CC BY-ND 3.0; data portal non-profit clause | **ingest-as-reference (annual)** — 13 dates, resolve PDF from hub each run |
| 3d | Crop Progress / Fats & Oils (NASS calendar) | PDF grid + HTML by-release-day page + ESMIS "Upcoming releases" | Public domain | **ingest-as-reference (annual)** |
| 3e | Weekly Export Sales / Export Inspections schedule | FAS calendar page (403 to scripts); AMS lists day only | Public domain | **list-with-link-only** (rule already in `events.py`) |
| 4a | EPA RFS rulemakings | **Federal Register JSON API + RSS**, keyless | Public domain | **ingest-as-layer** (metadata rows only) |
| 4b | Argentina export-duty decrees (Boletín Oficial) | HTML only; OTS hash index is the only pollable manifest | "libre y gratuita", official; no stated terms | **list-with-link-only** (hand-maintained rate table) |
| 4c | China Tariff Commission notices (MOF/MOFCOM) | HTML only; unstable IDs | MOF "版权所有 … 请注明来源" / "All rights reserved" | **list-with-link-only** (tariff-stack table + 2026-11-10 cliff row) |
| 4d | Indonesia palm levy / reference price | HTML + PDF decrees; BPDP RSS = news only; Kemenkeu JDIH refused connection | No open-data statement found | **ingest-as-layer, narrow** (12-row/yr HR/BK/PE table; detection automated, numbers entered) |

Builds, if any, are separate issues. Calendar-only items go to `analysis/futures/events.py`'s owner; its
docstring already says WASDE is "the exception worth naming" because the schedule file is not ingested —
§3 below gives that owner the exact source and its traps.

---

## 1. USDA FAS daily export-sales announcements ("flash sales")

**Publisher.** USDA FAS, Export Sales Reporting Staff. Each announcement is a newsroom press release of
type "Export Sales Announcement", numbered `FAS-ESR-NNN-YY` [SRC via Wayback: e.g.
`https://www.fas.usda.gov/newsroom/export-sales-unknown-destinations-129`, "March 30, 2026 | Export Sales
Announcement | FAS-ESR-025-26"].

**URL / API.**
- Listing: homepage "Daily Export Sales → View All Daily Sales" links to `/newsroom/search`; filtered
  listing `https://www.fas.usda.gov/newsroom/search?f[0]=field_news_type:Export+Sales+Announcements` [SNIP]
  — a paginated Drupal search, not a single static page [INF].
- Release slugs: `/newsroom/export-sales-<destination>-<n>` (2023→), earlier
  `/newsroom/private-exporters-report-sales-activity-<destination>-<n>` (2013–~2023) [SRC: Wayback CDX, 471 + 14 URLs].
- **No RSS, JSON, CSV or API.** `rss.xml` → 403; no feed href in homepage HTML [SRC]. The ESR OpenData
  swagger (`https://apps.fas.usda.gov/OpenData/swagger/docs/v1`) lists exactly seven ESR paths — regions,
  countries, commodities, unitsOfMeasure, datareleasedates, two `/exports/...` — none daily [SRC, fetched].
  The new ESRQS SPA (`https://apps.fas.usda.gov/esrqs/`, launched 2026-03-26) has `"daily"` **0 hits** in its
  3.4 MB bundle; probes of `/esrqs/api/{dailysales,DailyExportSales,swagger}` → 404 [SRC].
- Push channels: GovDelivery email (`https://public.govdelivery.com/accounts/USDAFAS/subscriber/new`) and
  X `@USDAForeignAg` [SRC, About page via Wayback].
- Weekly roll-up: ESRQS `StaticReports/WeeklyHighlightsReport.pdf` carries "SUMMARY OF EXPORT TRANSACTIONS
  Reported Under the Daily Reporting System For Period Ending …" [SRC, pdftotext]. PDF, weekly.

**Format.** One templated sentence per item: "Private exporters reported sales of 792,000 metric tons of
soybeans for delivery to China during the 2025/2026 marketing year." (FAS-ESR-072-25) [SRC]. MY split
variant: "Of the total, 60,000 metric tons is for delivery during the 2025/2026 marketing year, and 312,000
… 2026/2027" [SNIP]. Fields: commodity, MT, destination (incl. "unknown destinations"), MY(s). **No price,
no exporter, no sale date.** Variant releases: cancellations, destination changes, CORRECTIONs [SNIP].
Daily-reportable commodities [SRC, ESR Instructions §3.102]: "Wheat, corn, grain sorghum, barley, oats,
soybeans, soybean cake and meal, and soybean oil."

**Rule, cadence, time.**
- Thresholds [SRC §3.103]: ≥100,000 MT one commodity, one destination, one calendar day, or ≥200,000 MT in
  one Fri–Thu reporting period; **soybean oil 20,000 / 40,000 MT**.
- Exporter deadline [SRC 7 CFR 20.11(a)]: "no later than 3 p.m., E.s.t., on the next business day
  following the calendar day of the sale."
- USDA release [SRC §3.108 + About page]: "9 a.m. ET on the business day after the exporter reports to
  FAS." = **13:00 UTC (US DST) / 14:00 UTC (standard)** [INF].
- **Lag sale→announcement: 1–2 business days** (D+2 if the exporter uses the full window); up to ~4
  calendar days across a weekend/holiday [INF]. Sale date is not printed.

**Historical depth.** Daily announcements since 1977-04-14 [SRC, Top-10 page]; programme established
1974-09-12 [SRC §3.101]. Online newsroom archive from 2013 [SNIP]; pre-2013 online [NV]. Every daily sale is
compiled into the weekly ESR [SRC §3.108] which we already ingest (Layer 10).

**Licence.** USDA: "Most information presented on the USDA Web site is considered public domain
information … Attribution may be cited as follows: 'U.S. Department of Agriculture.'" [SRC, Wayback 2025].
data.gov catalog entry labels ESR CC-BY-4.0 [SRC]. FAS-specific statement [NV] (page blocked).

**Traps.**
1. **Bot wall**: every `www.fas.usda.gov` URL returned Akamai `Access Denied` (403) to curl and WebFetch
   with and without browser headers [SRC]. A CI scraper hits the same wall [INF] — a silent 403 → stale
   panel is exactly invariant 1's failure.
2. Shutdown gaps: "all Daily Sales … have not been published since September 30, 2025 … list of Daily Sales
   … October 1 through November 13, 2025 … released on Friday, November 14, 2025, at 12:00 noon ET" [SRC,
   Stakeholder Notice], followed by a CORRECTION on 2025-11-17 [SNIP]. Absence ≠ no sales.
3. Corrections / cancellations / destination changes are separate releases; "sum of flashes" overstates.
4. "Unknown destinations" are reassigned only in the *weekly* report; the daily release never updates.
5. Optional-origin sales are reportable [SRC §3.104(a)] — a flash may not be US-origin cargo.
6. Homepage teaser abbreviates "MT"/"MY"; release body spells "metric tons"/"marketing year".
7. Legacy `apps.fas.usda.gov/export-sales/*` and `esrquery` now 404; data.gov still points at them.

**Recommendation: list-with-link-only.** Public-domain and genuinely relevant, but there is no
machine-readable source, the host blocks scripts, and the information lands in the weekly ESR we ingest
*with* cancellations and unknown-destination reassignments netted. At our ~13:00–14:00 UTC refresh a
09:00 ET post reaches the reader next run (effectively D+2/D+3 vs the sale), losing the same-morning
value. Ship a link + "09:00 ET / 13–14 UTC" timing note on the ESR card. Reopen only if ESRQS exposes a
daily endpoint (ask `esr@usda.gov`) or a GovDelivery-email parser becomes acceptable infrastructure.

---

## 2. CFTC Disaggregated Commitments of Traders

**Publisher.** CFTC Division of Market Oversight; open-data mirror `publicreporting.cftc.gov` (Socrata).

**URL / API** [SRC, all fetched 2026-10-07, HTTP 200].
- Current week, no header row: `https://www.cftc.gov/dea/newcot/f_disagg.txt` (futures only),
  `.../c_disagg.txt` (fut+opt combined). Last-Modified `Fri, 02 Oct 2026 19:27:4x GMT`.
- Yearly zips (header row present): `https://www.cftc.gov/files/dea/history/fut_disagg_txt_YYYY.zip` →
  `f_year.txt`; `com_disagg_txt_YYYY.zip` → `c_year.txt` (2026: 17.6 MB / 1.95 MB zipped); bundles
  `*_disagg_txt_hist_2006_2016.zip` (`C_Disagg06_16.txt`, 119 MB, CRLF).
- Socrata: Disagg Futures Only `72hh-3qpy` (185,631 rows, 2006-06-13 → 2026-09-29); Disagg Combined
  `kh3c-gbw2` (194 cols, 191,743 rows). `https://publicreporting.cftc.gov/resource/kh3c-gbw2.json?cftc_contract_market_code=005602&$order=report_date_as_yyyy_mm_dd DESC&$limit=1` verified. Legacy = `6dca-aqww` / `jun7-fc8e`; CIT = `4zgm-a668`.
  Token optional; "we do not throttle API requests that are using an application token" [SRC
  dev.socrata.com]; `$limit` default 1,000, max 50,000 on 2.x (headers were `X-SODA2-*`) [SRC/INF].
- `cot_reports` 0.1.3 (already in `.venv`, MIT) supports `disaggregated_fut` / `disaggregated_futopt`;
  `cot_year(year, "disaggregated_futopt")` hits the `com_disagg_txt_<year>.zip` URL above [SRC, README +
  installed source]. Switch is one parameter at `fetchers/cot.py:61-63` (`config.COT_REPORT_TYPE`), but
  `_COL_MAP` there is legacy-specific and must be remapped.

**Format.** 191 columns, **identical between fut-only and combined** (`diff` IDENTICAL) — distinguisher is
the last column `FutOnly_or_Combined` [SRC]. Blocks per category (Prod_Merc / Swap / M_Money / Other_Rept
/ Tot_Rept / NonRept): `_Positions_Long_All`, `_Short_All`, `_Spread_All` (Swap, M_Money, Other_Rept only
— **Prod_Merc has no spread column**), repeated `_Old`/`_Other` crop-year, `Change_in_*`, `Pct_of_OI_*`,
`Traders_*`, concentration ratios. Double-underscore quirk `Swap__Positions_Short_All` [SRC]. Socrata:
lowercase, `_1`/`_2` suffix for Old/Other, numerics as strings, dates `2026-09-29T00:00:00.000`.

**What it adds** [SRC, Disaggregated Explanatory Notes]: "increases transparency from the legacy COT
reports by separating traders into the following four categories of traders: Producer/Merchant/Processor/
User; Swap Dealers; Managed Money; and Other Reportables. The legacy COT report separates reportable traders
only into 'commercial' and 'non-commercial' categories." Soybeans 2026-09-29 [SRC]: Prod/Merc 390,455 L /
665,843 S; Swap 132,216 L / 81,960 S; MM 276,133 L / 34,969 S / 178,208 spread; 653 traders. For a
physical buyer the value is that Legacy "commercial" lumps crusher/exporter hedges with swap-dealer index
hedges [INF].

**Cadence / time** [SRC Release Schedule page]: "released at 3:30 p.m. Eastern time … usually released on
Friday … data from the previous Tuesday … Federal holidays may delay release by one or two days." 2026
delayed dates: Jan 05, Jun 22, Jul 06, Nov 16, Nov 30, Dec 28. = 19:30 UTC (EDT) / 20:30 UTC (EST). Observed
write 19:27 UTC; Socrata `rowsUpdatedAt` 19:30:09 → ~2.5 min behind the text file (one observation) [SRC].

**Historical depth.** "dated back to June 13, 2006" [SRC, CFTC]; Socrata min date 2006-06-13 confirms. The
HistoricalCompressed page's "from September 2009" wording is the *publication* start, not the data start
[SRC, inspected]. Legacy: 1986 (fut) / 1995 (combined), still published in parallel. Agriculture + natural
resources only; financials live in TFF [SRC].

**Symbols** (from `c_disagg.txt`) [SRC]: Soybeans 005602 · Soybean Oil 007601 · Soybean Meal 026603 · Corn
002602 · Wheat-SRW 001602 · Cotton No. 2 033661 · Sugar No. 11 080732 · Live Cattle 057642 · Lean Hogs
054642 · Canola 135731.

**Licence** [SRC `https://www.cftc.gov/WebPolicy/index.htm`]: "Government information at the CFTC website
is in the public domain. Public domain information may be freely distributed and copied, but it is
requested that in any subsequent use the CFTC be given appropriate acknowledgement." Socrata metadata
`license: None`. publicreporting.cftc.gov separate terms [NV].

**Traps.**
1. Name-prefix matching pulls in `SOYBEAN CSO` (005606), `CORN CSO` (00260C), `CORN CONSECUTIVE CSO` —
   **key on `CFTC_Contract_Market_Code`, never name**.
2. Renames: `WHEAT - CHICAGO BOARD OF TRADE` → `WHEAT-SRW …` 2013-12-17; sugar/cotton `NEW YORK BOARD OF
   TRADE` → `ICE FUTURES U.S.` 2007-09-04; canola exists only from **2018-07-31** (`CANOLA OIL` one week, then
   `CANOLA`) [SRC].
3. Current-week txt has no header; yearly zips do; the 2006–16 bundle is CRLF.
4. Fut-only vs combined headers are identical — mixing them silently double-counts.
5. Supplemental CIT (`cit_positions_*`) is a different report; do not confuse with `swap_*`.
6. Yearly zip is rewritten weekly (~2 MB re-download each run; idempotent under `INSERT OR REPLACE`).
7. `As_of_Date_In_Form_YYMMDD` is an integer; `Report_Date_as_YYYY-MM-DD` is text.
8. Trader counts <4 render as "·" in HTML; txt/Socrata representation [NV].
9. Holiday-week Monday releases break any "Friday" assumption — use the schedule above.

**Recommendation: ingest-as-layer alongside Legacy (do not replace).** Zero licence risk, same weekly
cadence and latency vocabulary as Layer 4, 20-year history, package already supports it. Keep Legacy for
its longer history and because the existing page depends on it. Label it weekly positioning context, not a
daily signal.

---

## 3. Authoritative release calendars

Access note that governs every build here [SRC]: `usda.gov/oce/*`, `nass.usda.gov/Publications/Calendar/`
(directory), every `fas.usda.gov/*` page and `cmegroup.com` returned **HTTP 403** to curl and WebFetch under
browser, curl and Googlebot UAs. What did serve: the usda.gov WASDE report page, NASS calendar PDFs,
`data.nass.usda.gov`, `esmis.nal.usda.gov`, `nopa.org`, `gov.br/conab`.

### 3a. WASDE (USDA OCE / WAOB)

- URL: `https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report` (200). `https://www.usda.gov/oce/commodity/wasde` → 403 to scripts.
- Format: **HTML prose only** [SRC]: "2026 WASDE Release Dates (12:00pm ET) In 2026 the WASDE report will be
  released on Jan. 12, Feb. 10, Mar. 10, Apr. 9, May 12, Jun. 11, Jul. 10, Aug. 12, Sep. 11, Oct. 9, Nov. 10,
  and Dec. 10." No ICS/CSV/JSON schedule (link scan). The *report* is machine-readable (`wasdeMMYY.xml`,
  Excel, CSV "updated each month, the day after the WASDE release") — the *schedule* is not.
- Remaining 2026: **Oct 9, Nov 10, Dec 10**; cross-checked against WASDE-673/674 back page and NASS Crop
  Production dates [SRC/SRC-2nd]. **2027: [NV] not published** as of today.
- Time: 12:00 ET = 16:00 UTC (EST) / 17:00 UTC (EDT).
- Licence: US gov public domain [INF, not stated on page].
- Traps: Oct 2025 WASDE **cancelled** under the shutdown; Nov 2025 moved Nov 10 → Nov 14 (NASS notice
  "USDA Reschedules Reports Affected by Lapse in Federal Funding") [SRC-2nd/SRC] — a fixed-date list is
  silently wrong under a shutdown and needs an on-time existence check against `wasdeMMYY.xml`, not a
  calendar. `/oce/` 403s scripts.
- **Recommendation: list-with-link-only** — 12 desk-keyed dates per year (re-keyed each December) plus an
  on-time check at 16:00/17:00 UTC. This directly resolves the `events.py` docstring's named exception.

### 3b. NOPA monthly crush

- URL: `https://www.nopa.org/resources/nopa-monthly-crush-report/` (200). Schedule **PDF file**:
  `https://www.nopa.org/wp-content/uploads/2026/02/NOPA-ONLY-Crush-Reporting-Release-Dates-2026-FINAL.pdf`
  [SRC]: "LSEG Release Dates (at Noon Eastern): Thursday, January 15 · Tuesday, February 17 · Monday, March
  16 · Wednesday, April 15 · Friday, May 15 · Monday, June 15 · Wednesday, July 15 · Monday, August 17 ·
  Tuesday, September 15 · Thursday, October 15 · Monday, November 16 · Tuesday, December 15."
- Remaining 2026: **Oct 15, Nov 16, Dec 15**. 2027 [NV]. Rule is 15th-or-next-business-day [INF from list;
  never stated]. Time: 12:00 ET (= 11:00 CT) = 16:00/17:00 UTC. Page says "12:00 noon (EST)" year-round [SRC].
- Access [SRC]: "Refinitiv is the sole distributor of the National Oilseed Processors Association (NOPA)
  monthly Crush Report." "A 12-month subscription to the NOPA Crush Report is US $1,200." No free summary
  on nopa.org — headline numbers reach the public via wires only.
- Licence/redistribution terms: **[NV]** — no terms page found; sole-distributor + paywall ⇒ contractual
  with LSEG [INF]. Invariant 9 blocks rendering the data.
- Traps: paywall; "NOPA-ONLY" filename implies a members' variant; "EST" literal in summer.
- **Recommendation: list-with-link-only** for dates (re-keyed from the PDF yearly); **do-not-pursue** the
  data. NASS Fats & Oils (free, ~2 weeks later) is the public substitute already ingested.

### 3c. CONAB grain survey

- Old `conab.gov.br/info-agro/safras/graos` → 301 → `https://www.gov.br/conab/pt-br` (domain dead). Hub:
  `https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras` → "Acesse aqui o calendário de
  divulgação de safras" → view page `.../safras/calendario-de-safras-e-prohort-2026/view` → **PDF
  infographic** at `https://www.gov.br/conab/pt-br/assuntos/imagens/calendario-de-safras-e-prohort-2026-novo`
  ("Atualizado em 12/05/2026 11h58 … 60 KB") [SRC].
- Dates [SRC, PDF]: 2025/26 12º 15/set/2026; **2026/27: 1º 15/out/2026, 2º 13/nov/2026, 3º 15/dez/2026, 4º
  14/jan/2027**. Weekly "Progresso de Safra" every Monday 19h BRT (22:00 UTC). Hub listing shows every
  bulletin at "09h00" [SRC] → **09:00 BRT = 12:00 UTC** (no DST since 2019). Full 2027 calendar [NV].
- Licence: gov.br footer "Creative Commons Atribuição-SemDerivações 3.0 Não Adaptada" (CC BY-ND) [SRC];
  data portal: "reprodução total ou parcial sem fins lucrativos é autorizada, desde que citada a fonte"
  [SRC-2nd] — **non-profit-only** clause on the Série Histórica; Conab states it is outside Decreto
  8.777/2016 open-data scope [SRC-2nd]. Pre-sale licence question for a commercial desk product.
- Traps: PDF re-issued mid-year ("-novo", May 2026) — **resolve from the view page each run** (invariant
  10); dates drift off the 15th with no rule (13/nov, 13/mar, 12/fev); some gov.br news pages redirect
  scripted fetches to `require_login`; the PDF carries no times.
- **Recommendation: ingest-as-reference (annual)** — store the 13 grain dates + 09:00 BRT; verify bulletin
  landing at 12:00 UTC.

### 3d. NASS calendar — Crop Progress, Fats & Oils / Oilseed Crushings, Grain Stocks, Crop Production

- Web list: `https://data.nass.usda.gov/Publications/Calendar/reports_by_date.php?view=l&js=1&month=MM&year=YYYY` (200; each entry carries "3:00 pm ET"/"12:00 pm ET"). `year=2027` → **zero entries** [SRC].
- PDF file: `https://www.nass.usda.gov/Publications/Calendar/2026/2026ReleaseCalendar_12Months_11x17_Color.pdf`
  (200). Legend [SRC]: "◆ 12:00 p.m. Release · ⬤ 12:00 p.m. PFEI Release · ◼ 3:00 p.m. PFEI Release · ▲
  4:00 p.m. Release · Remaining reports are issued at 3 p.m." **Timezone not stated on the PDF.** 2025 PDF
  200; 2024 and 2027 404.
- By title: `https://data.nass.usda.gov/Publications/Reports_by_Release_Day/index.php` (200). Fats & Oils /
  Grain Crushings 2026: Jan 2, Feb 2, Mar 2, Apr 1, May 1, Jun 1, Jul 1, Aug 3, Sep 1, Oct 1, **Nov 2, Dec
  1**, 15:00 ET [SRC; ESMIS "Upcoming releases Nov 02 2026 3:00 PM, Dec 01 2026 3:00 PM"].
- Crop Progress [SRC, ESMIS]: "released after 4:00 pm ET on the first business day of the week";
  Dec–Mar monthly "State Stories". Explicit Tuesday shifts in 2026: May 26, Sep 8, **Oct 13** [SRC]. Remaining
  2026: Oct 13, 19, 26, Nov 2, 9, 16, 23, 30 — 20:00 UTC until Nov 1, **21:00 UTC after**.
- RSS: `http://www.nass.usda.gov/rss/reports.xml` ("Today's Reports") — notification, not schedule [SRC].
- Licence: public domain [INF].
- **Recommendation: ingest-as-reference (annual)** from the by-release-day page (ET-labelled) with ESMIS as
  cross-check; not a live layer.

### 3e. Weekly Export Sales (FAS) and Export Inspections (AMS/FGIS)

- FAS rule [SRC-2nd, all fas.usda.gov blocked]: "every Thursday at 8:30 a.m. ET … If the preceding Friday
  or Monday is a national holiday, the reporting deadline moves to Tuesday and the weekly summary is
  published Friday." Calendar `https://www.fas.usda.gov/data/scheduled-reports` offers **per-event ICS**
  ("Add to Calendar: iCalendar"), no bulk feed [SRC-2nd]. Remaining 2026 Friday shifts: **Oct 16, Nov 13, Nov
  27**. 08:30 ET = 12:30 UTC → **13:30 UTC from Nov 2**. Shutdown catch-up page documents Sep 25 → Nov 2025 gap.
- AMS: National Grain Reports page lists "Weekly Grains Inspected For Export (Mon)" — **day only, no time**
  [SRC]; 10:00 CT / 11:00 ET is trade-press only [SRC-2nd]; official holiday rule [NV].
- **Recommendation: list-with-link-only** — the rule is already encoded in `events.py`; add the three 2026
  Friday shifts and the DST flip as notes. Export Inspections stays a rule date with an [NV] time source.

---

## 4. Official policy releases

### 4a. US EPA — RFS / RVO rulemakings and SRE decisions

- Authoritative text: Office of the Federal Register. Set 2 final rule = doc **2026-06275**, 91 FR 16388,
  published 2026-04-01, docket EPA-HQ-OAR-2024-0505, effective 2026-06-15 [SRC
  `https://www.federalregister.gov/api/v1/documents/2026-06275.json`].
- **Structured feed, live, keyless** [SRC, fetched]:
  `https://www.federalregister.gov/api/v1/documents.json?conditions[agencies][]=environmental-protection-agency&conditions[term]="Renewable Fuel Standard"&order=newest`
  → 247 docs; fields `document_number, type, publication_date, docket_ids, abstract, html_url, pdf_url,
  raw_text_url, full_text_xml_url`. Same query `.rss` → valid RSS 2.0 (auto-windowed ~30 days — backfill via
  JSON). Public-inspection JSON `public-inspection-documents/current.json` gives next-day documents.
  regulations.gov v4 (api.data.gov key) for dockets/comments.
- EPA's own pages: no RSS; `epa.gov/newsreleases/search/rss` → 405 [SRC]. federalregister.gov **HTML** 302s
  to `unblock.federalregister.gov` (bot wall) — the API does not [SRC].
- Language English. Licence 17 U.S.C. §105 [SRC govinfo]. API quota [NV] (third parties cite a 10,000-result
  ceiling per filter).
- Cadence irregular: 2026 so far Set 2 (announced 03-27, FR 04-01), SRE batches 08-03 and 08-31 (34
  petitions), compliance-deadline rule 09-04; EPA says SRE reallocation proposal "before the end of October
  2026" [SRC]. FR publishes 06:00 ET daily [NV]; RSS `pubDate` stamps 04:00 GMT [SRC].
- Traps: press release precedes gazette by 1–5 days (market moves on the press); litigation (AFPM + NGOs,
  D.C. Cir., June 2026) can change status with no new FR doc; free-text `term` matches ICR renewals — filter
  on `type` and `docket_ids` prefix `EPA-HQ-OAR`; public-inspection docs carry a future `publication_date`.
- **Recommendation: ingest-as-layer** (metadata rows: title, type, docket, dates, URL; never auto-summarise
  rule content). Complements Layer 29 (EMTS RIN volumes) with no overlap.

### 4b. Argentina — export duties (derechos de exportación) on soy complex

- Boletín Oficial: `https://www.boletinoficial.gob.ar/seccion/primera`; items
  `/detalleAviso/primera/<ID>/<YYYYMMDD>`; daily PDF `https://s3.arsat.com.ar/cdn-bo-001/pdf-del-dia/primera.pdf`;
  timestamped hash index `https://otslist.boletinoficial.gob.ar/ots/` (Fecha/Sección/SHA-256/PDF, ~95 pages
  back) [SRC]. `/rss` → HTML homepage + "Hubo un error en el sistema" [SRC]. **No RSS/API.** The OTS index is
  the only pollable manifest (new hash = new edition/supplement) [INF].
- Consolidated text: argentina.gob.ar/normativa (Decreto 682/2025: "publicada BO 22-09-2025, N° 35754, p.
  3; 'LA PRESENTE NORMA FUE PUBLICADA COMO SUPLEMENTO DE BOLETIN OFICIAL'") [SRC]. Slugs need the numeric ID
  suffix — `decreto-423-2026` without it resolved to a 1989 decree [SRC].
- Decree chain (BO dates, each ≥2 sources) [SRC-2nd]: 38/2025 (temp. cut to 26 %); 439/2025 (27-06-2025,
  back to 33 %); 526/2025 (31-07-2025, 26 %, subproducts 24.5 %); 682/2025 (22-09-2025 supplement, 0 % until
  31-10-2025 or USD 7 bn DJVE cap — ARCA closed it 24-09-2025); 877/2025 (Dec 2025, beans 24 %, subproducts
  22.5 %); **423/2026 (BO 03-06-2026)**: beans 24 % through 2026 → monthly step-down from Jan 2027 → 21 % Dec
  2027 → 15 % Dec 2028; crude oil 22.5 → 19.5 → 14 %. **Current (Oct 2026): beans 24 %, oil/meal 22.5 %**
  [SRC-2nd: ARCA Jun 2026 recaudación PDF + Decreto 423 schedule].
- Language Spanish only. Licence: site says access is "libre y gratuita" and the electronic edition
  "reviste carácter de oficial y auténtica" (Decreto 207/2016) [SRC]; no terms-of-use or Ley 11.723
  statement found [NV] — treat as "no stated restriction", not "public domain confirmed".
- Clock [NV]: regular edition early morning ART (UTC-3) [INF]; supplements any time (682/2025 "antes de la
  apertura del mercado") [SRC-2nd].
- Traps: press/X precedes gazette; supplements outside the daily edition; **ARCA, not a decree, ends a
  cap-based regime** (DJVE closure announced on X); rates live in PDF annexes per NCM position, not the HTML
  body; Spanish-only against an English-only pipeline.
- **Recommendation: list-with-link-only** — hand-maintained rate table (rate, decree, BO date, link to
  detalleAviso + ARCA). If ever automated, ingest *event detection* (new decree mentioning "derecho de
  exportación" + NCM 1201/1507/2304), never rate extraction (invariant 11).

### 4c. China — Tariff Commission notices (MOF), MOFCOM lists, GACC schedule

- Authoritative: 国务院关税税则委员会 公告 published by MOF Tariff Dept at
  `https://gss.mof.gov.cn/gzdt/zhengcefabu/` (pattern `./YYYYMM/tYYYYMMDD_<id>.htm`) [SRC]. Soy chain: 2025年第2号
  (04-03-2025, +10 % US soybeans from 10-03); 第4号 (04-04-2025, +34 %); 第7号 (13-05-2025, 34→10 %, 24 %
  suspended 90 d); **第9号 (05-11-2025)**: "自2025年11月10日13时01分起，停止实施…（税委会公告2025年第2号）规定的加征关税措施"
  [SRC policy.mofcom.gov.cn id=104127]; 第10号 (05-11-2025) keeps 10 %, suspends 24 % one year.
- **Official English exists on MOF, not MOFCOM**: `https://www.mof.gov.cn/en/Policies/` carries same-day
  English versions (2025-04-04/09/11, 05-13, 11-05 ×2, 2026-04-28) [SRC]. `english.mofcom.gov.cn` reset the
  connection [SRC]. MOFCOM's `policy.mofcom.gov.cn/claw/` mirror disclaims accuracy ("网站不对法规内容的准确性负责") [SRC].
- Status Oct 2026 [SRC/SRC-2nd]: US soybeans 3 % MFN + 10 % = 13 % vs 3 % Brazil; 24 % suspended to
  ~2026-11-10; 2026-09-28 MOFCOM "300亿对300亿" list covers soy oil/meal but **excludes whole soybeans**; no
  implementing 税委会 公告 yet. gss.mof.gov.cn shows no US-related 公告 in 2026 (latest 2026-04-28, Africa zero-tariff).
- **No RSS/API** on gss.mof.gov.cn or mof.gov.cn/en [SRC]. Stale deep link 302'd to `mof.gov.cn/404.htm` —
  **IDs not guessable; resolve from the listing each run** (invariant 10). Fetched fine from a US address;
  `customs.gov.cn` failed on "self signed certificate in certificate chain" (TLS, not geo) [SRC].
- Licence [SRC]: MOF Chinese "中华人民共和国财政部版权所有，如需转载，请注明来源"; MOF English "All rights reserved."
  Quoting 公告 number/date/effective time with attribution is within stated terms; wholesale republication is not [INF].
- Cadence event-driven (~10 US-related 公告 in 2025, 0 in 2026). Effective times stated to the minute
  Beijing (13:01 CST = 05:01 UTC); publication clock [NV].
- GACC context [SRC-2nd, 海关总署公告2025年第240号]: 2026 快讯 dates e.g. Oct 14, Nov 10; 月刊 18th; online 20th;
  no Feb release. Soybean-by-origin detail is in the 月刊/online query, not the 快讯. Clock ~11:00 CST [INF].
- Traps: three layers (税委会 legal / MOFCOM political / White House claims); 公告 numbering collisions between
  bodies; the 2025-11 action removed the soy-specific 10 % but the across-the-board 10 % remained — easy to
  mis-state; **2026-11-10 suspension expiry is a cliff with no document until it happens**.
- **Recommendation: list-with-link-only** — hand-maintained US-soy tariff-stack table (MFN 3 % + 10 %
  retained + 24 % suspended-to-date), each cell citing 公告 number + MOF Chinese + MOF English URL, plus a dated
  "next cliff 2026-11-10" row sourced to 第10号.

### 4d. Indonesia — palm export levy (PE), export duty (BK), CPO reference price (HR)

- HR + BK: Kemendag. Monthly Kepmendag on JDIH Kemendag `https://jdih.kemendag.go.id/`; press release at
  `https://www.kemendag.go.id/berita/siaran-pers` (2026-10-01 "HR CPO dan HPE Biji Kakao Naik…") [SRC].
  **October 2026: HR USD 1,042.15/MT, BK USD 178/MT, PE 12.5 % = USD 130.269/MT; Kepmendag 1921/2026 signed
  29-09-2026; press 02-10-2026** [SRC-2nd ≥3 agree]. Formula: 20 Aug–19 Sep average of Bursa CPO Indonesia /
  Bursa Malaysia / Rotterdam, Permendag 35/2025 (in force 22-10-2025) [SRC-2nd].
- PE rate: Kemenkeu PMK. **PMK 9/2026** (27-02-2026, effective ~1 Mar 2026) raised CPO levy 10 → **12.5 % of
  HR**, olein/stearin 9.5 → 12 % [SRC-2nd + BPDP PDF exists]. Prior PMK 30/2025 (May 2025) 7.5 → 10 %. BK
  bracket table PMK 38/2024 jo. 68/2025 Annex C.
- Feeds: Kemendag/JDIH Kemendag HTML + PDF, no RSS/API [SRC]; BPDP (`https://www.bpdp.or.id/rss-feeds`) RSS
  = news only, English site exists [SRC]; **JDIH Kemenkeu `ECONNREFUSED` twice** (geo/IP block [INF]);
  national JDIHN API (`api-jdih.perpusnas.go.id`, bearer token) coverage of Kemenkeu/Kemendag [NV].
  Third-party cross-check: `https://gimni.org/harga-cpo` (industry body; monthly HR/BK/PE back to Oct 2024
  with "Dasar hukum" column, CSV/XLSX export) [SRC].
- Language Bahasa Indonesia; English only via wires. Licence: "© Hak Cipta 2026 … Kementerian Perdagangan"
  and "© 2022 - 2026 Biro Hukum Kementerian Perdagangan RI"; **no open-data licence statement on any of the
  three sites** [NV].
- Cadence: HR monthly since 1 Feb 2024 (twice-monthly Aug 2022–Jan 2024) [SRC]. Kepmendag signed
  ~29th–31st, press 1–3 days later — the rate is live before public notice [INF]. PE % changes irregular.
  Clock [NV]; Jakarta UTC+7.
- Traps: two ministries, three instruments (Kepmendag HR → BK bracket from PMK 38/2024; PE % from PMK
  69/2025 as amended); cadence changed twice by directorate regulation; PE is % of HR, BK is fixed USD from
  a bracket — do not multiply the wrong base; Kemenkeu JDIH block; HR Kepmendag number differs from the
  same-day olein brand-list Kepmendag (1921 vs 1922/2026) — media mix them.
- **Recommendation: ingest-as-layer, narrow scope** — a 12-row/yr reference table (HR, BK, PE %, PE USD,
  Kepmendag no., PMK basis) entered from the Kemendag press release with GIMNI as cross-check, hard-failing
  on disagreement; automate only *detection* (new siaran pers title containing "HR CPO"); link out to the
  JDIH PDF, never store decree text. Payload is numbers + decree IDs, so the English-only pipeline is not a
  blocker. Licence to be confirmed pre-sale.

---

## 5. Open items [NV] — for whoever builds

- FAS-specific licence statement; whether ESRQS will ever expose a daily endpoint (ask `esr@usda.gov`).
- 2027 schedules for WASDE, NOPA, CONAB, NASS — none published as of 2026-10-07.
- NOPA redistribution terms; official AMS Export Inspections release-time / holiday statement.
- Federal Register API quota; FR daily publication hour.
- Boletín Oficial daily publication hour; any Ley 11.723 / terms statement.
- Chinese 公告 publication clock; publicreporting.cftc.gov separate terms page.
- Indonesian JDIH open-data licence; JDIHN API coverage; how "·" (<4 traders) renders in CFTC txt/Socrata.

## 6. Evidence log (direct fetches, HTTP 200 unless noted)

CFTC: `/dea/newcot/{f,c}_disagg.txt`, `/files/dea/history/*_disagg_txt_2026.zip`, `com_disagg_txt_hist_2006_2016.zip`,
HistoricalCompressed index, Disaggregated Explanatory Notes, Release Schedule, WebPolicy; Socrata
`/api/views/{72hh-3qpy,kh3c-gbw2,jun7-fc8e,4zgm-a668}.json` + `/resource/kh3c-gbw2.json` queries; dev.socrata.com
app-tokens + $limit; cot_reports GitHub + installed 0.1.3.
FAS: `apps.fas.usda.gov/OpenData/swagger/docs/v1`; `apps.fas.usda.gov/esrqs/` + `main-WLKZ33AV.js`;
`esrqs/StaticReports/WeeklyHighlightsReport.pdf`; `api.fas.usda.gov/api/esr/commodities` (403 API_KEY_MISSING);
govinfo CFR-2025 title 7 part 20 XML; catalog.data.gov export-sales-reporting; Wayback copies of fas.usda.gov
homepage / about ESR / top-10 / three releases / stakeholder notice / instructions.html. All live
`www.fas.usda.gov` URLs → 403 Akamai.
Calendars: usda.gov WASDE report page; NASS 2026 release-calendar PDF (2025 200; 2024/2027 404);
`data.nass.usda.gov` reports_by_date (2027 = 0 entries) + Reports_by_Release_Day + Help/RSS; ESMIS crop-progress
and fats-and-oils pages; nopa.org crush page + 2026 dates PDF; gov.br/conab safras hub, calendar view page, PDF,
CC BY-ND footer; ams.usda.gov national-grain-reports. Blocked: usda.gov/oce/*, NASS Calendar dir, fas.usda.gov,
cmegroup, usda.gov agency-reports, Conab portal (JS).
Policy: FR API documents.json/.rss/public-inspection/2026-06275.json; govinfo policies; open.gsa.gov
regulations.gov; epa.gov/renewable-fuel-standard; boletinoficial.gob.ar (home, /rss, /seccion/primera, FAQ,
prodyserv), otslist.boletinoficial.gob.ar; gss.mof.gov.cn/gzdt/zhengcefabu/; mof.gov.cn/en/Policies/;
policy.mofcom.gov.cn id=104127; news.cn 2026-09-28; jdih.kemendag.go.id; kemendag.go.id siaran-pers; bpdp.or.id;
gimni.org/harga-cpo; api-jdih.perpusnas.go.id/docs. Failed: federalregister.gov HTML (bot wall),
english.mofcom.gov.cn (reset), customs.gov.cn (TLS), jdih.kemenkeu.go.id (refused), epa.gov RSS (405),
gss 20251105 deep link (404).
