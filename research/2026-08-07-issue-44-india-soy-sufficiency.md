# Issue #44 — India soy data sufficiency for physical trading

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/44
- **Date:** 2026-08-07 (all URLs accessed this date unless noted)
- **Question:** Is the Layer 16 mandi feed (MP median modal soybean, data.gov.in) enough for physical soy buyers, or does the India leg need plant/meal/import-parity series?

**Evidence tiers used below:** **[D]** directly observed (API response, page content fetched today) · **[I]** inference from observed data · **[A]** assumption / secondary source · **[?]** couldn't verify.

---

## RQ1 — What does a physical soy trader dealing with India actually need?

### 1a. Plant-delivered vs mandi auction prices

- **[D]** SEA of India's weekly comparative rate page (2026-08-06 quotes) lists **"Soyabean seed (Indore) Rs 69,500/MT"** — a processor-level (plant/Indore trade) quote. Source: https://seaofindia.com/weekly-comparative-rate-as-on-3rd-jan-2020/ (permalink reused; page content is current — titled "as of 7th Aug 2026").
- **[D]** Same-day (07/08/2026) MP mandi modal prices from the data.gov.in API sample: FAQ modal quotes 6,580–7,001 INR/quintal across mandis (≈ 65,800–70,010 INR/MT), typical FAQ rows ~6,640–6,725 (≈ 66,400–67,250 INR/MT).
- **[I]** Plant-level Indore quote sits roughly **3–5% above the MP mandi modal median** on this one observed day. This is a single-day observation, not a measured typical spread. **[?]** Couldn't verify a published long-run mandi↔plant spread series from any primary source.
- **[I]** For import/export switching, the decision price is the plant-delivered bean cost (crush economics) and the meal FOB realisation — not the raw mandi auction print. The mandi median is a good *direction/level* proxy (mandis feed the plants) but is quoted pre-transport, mixed-grade.

### 1b. Soy meal (SBM) export prices — the substitution signal

India's soy relevance to world trade is mostly **meal-side** (exports to Bangladesh, SE Asia, Middle East) — and both trade associations publish exactly this:

- **[D]** SOPA "Soymeal market rate" page publishes **daily** (business days): FAS/FOB (Bedi/Kandla), F.O.R. (Bedi/Kandla), Ex-Factory (Indore), all in Rs/MT, with READY and forward (e.g. "DEC") conditions. Latest observed: 2026-07-31 — FAS/FOB Rs 59,000–59,500/MT DEC; Ex-Factory Indore Rs 57,000–57,500/MT READY. Source: https://sopa.org/soymeal-market-rate/
- **[D]** SOPA also publishes monthly FAS(FOB) average rates for soybean meal in USD (values 359–392 $/MT visible on the index page): https://sopa.org/price_information_category/fob_soybean_meal/
- **[D]** SEA weekly comparative rates include "Soya Ext. (EX-Indore) 48/2.5: Rs 58,500/MT" and a "Soyabean Ext. (FAS) Bulk (Ex-Kandla)" line in US$/MT. **[?]** The US$ figure extracted ($253/MT) is implausibly low vs SOPA's Rs 59,000/MT (≈ $670 at ~88 INR/USD) and vs the ~$605/MT FOB India cited from SEA data in Apr-2026 press ([Investing.in/Kedia](https://in.investing.com/news/commodities-news/indias-oilmeal-exports-decline-as-soybean-shipments-weaken-sharply-5500772)); likely a row-misread (rapeseed meal FAS Kandla trades ≈$250). Treat the SEA USD meal line as needing table-position verification before use.
- **[A]** Context (secondary, Kedia Advisory on SEA data, Apr-2026): Indian SBM FOB ≈ $605/MT vs ~$430 CIF Rotterdam — India priced out; soybean-meal exports fell to 62,844 t in Apr-2026 from 230k t a year earlier. The **India-vs-CBOT/South-America meal premium is the actual substitution signal** for Middle East/African buyers, which is what the platform's briefing narrative claims to track.

### 1c. Import parity (oil side)

- **[D]** SEA publishes **"Soya Degum (Crude) CIF Mumbai: 1,285 US$/MT"** and domestic **"Solvent Ext. Soyabean Oil (Indore): Rs 139,000/MT"** in the same weekly table — both legs of an import-parity calculation from one primary source.
- **Duty structure (primary-anchored):**
  - **[D/A]** BCD on crude soybean/palm/sunflower oil cut 20%→**10%** effective 2025-05-30; effective duty (BCD + AIDC + Social Welfare Surcharge, SWS cut 2.5%→1.5%) = **16.5%**. Refined oils unchanged at 32.5% BCD / 35.75% effective. Corroborated by USDA FAS GAIN ("India Cuts Import Tax on Crude Edible Oils", https://www.fas.usda.gov/data/india-india-cuts-import-tax-crude-edible-oils-opportunities-us-soybean-oil) and NewsOnAir/PIB reporting (https://www.newsonair.gov.in/centre-halves-customs-duty-on-crude-edible-oils-to-10). **[?]** I did not retrieve the CBIC notification text itself; Budget-2026 customs notifications (01/2026, 02/2026 of 2026-02-01) do not appear to alter edible-oil rates, and mid-2026 press reports a duty *hike under consideration* (SOPA lobbying for +10%) but **no decision** — so 16.5% is the standing rate as of 2026-08-07, flagged as politically live.
- **[I]** Illustrative parity check from the observed SEA quotes: $1,285 × 1.165 ≈ $1,497/MT landed ≈ Rs 131.7k/MT at ~88 INR/USD, vs Rs 139k domestic solvent oil → imports favored (ignores port/finance costs). This *is* a computable, briefing-grade switching signal — currently absent from the platform.
- **[I]** India imports ~55–60% of edible oil needs; bean imports are negligible (GM restrictions), so oil-side parity + meal-side FOB competitiveness, not bean arbitrage, are the two real cross-border signals.

**RQ1 answer:** A physical buyer needs (1) the mandi/plant bean price (have, proxy-level), (2) SBM FOB Kandla / ex-Indore (don't have), (3) crude oil CIF vs domestic with the 16.5% duty (don't have). The current CBOT-vs-mandi bean premium line is the weakest of the three as a trade signal because beans barely cross India's border.

---

## RQ2 — Is the MP median modal price representative?

### 2a. State coverage

- **[D]** SOPA "All India state-wise soybean area, production and productivity" (https://sopa.org/all-india-state-wise-soybean-area-production-and-productivity/), Kharif 2025 estimates: **Maharashtra 52.229, Madhya Pradesh 43.247, Rajasthan 6.394**, total 110.267 (units: lakh tonnes — **[I]** the page renderer said "million tonnes" but the 110.27 total matches SOPA's published 11.03 mln-t national estimate, so lakh tonnes).
- **[I]** ⇒ **MP is now ~39% of production; Maharashtra ~47% and is the #1 state in 2025-26.** The Layer-16 docstring's premise ("Madhya Pradesh is the soy belt … the domestic benchmark") is stale for this crop year. Indore/MP remains the crush-industry pricing hub (Agriwatch and SOPA both quote Indore as the benchmark), but an MP-only median ignores the largest producing state (Latur, Vidarbha mandis).
- **Cheap fix [I]:** the same data.gov.in resource covers all states — adding `Maharashtra` is a one-line filter change plus aggregation choice; no new source, same GODL licence.

### 2b. Reporting lag — checked against the API itself

- **[D]** Query run 2026-08-07: `total: 122`, records with `arrival_date: 07/08/2026`, response `updated_date: 2026-08-07T13:00:49Z` (18:30 IST). **Same-day publication, intraday update — lag is not a problem.**
- **[D]** Resource description: "Current Daily Price of Various Commodities from Various Markets (Mandi)" — and filtering `arrival_date=01/08/2026` still returned today's 122 rows. **[I]** The resource is a **current-day snapshot only** (or the date filter is ignored); history must be accumulated by the pipeline run-by-run. This matches how Layer 16 + `data/history` already work, but means **any missed pipeline day is an unrecoverable gap** in the India series — worth stating in the repo docs.

### 2c. Quality/moisture basis

- **[D]** The feed carries `variety` (e.g. "Yellow", "Soyabeen") and `grade` (**FAQ / Non-FAQ**); `fetchers/mandi.py` medians across all of them. Observed same-mandi FAQ vs Non-FAQ spread (Sarangpur, 07/08/2026): 6,724 vs 6,651 INR/qtl (~1%).
- **[A]** Mandi "Fair Average Quality" is looser than processor contract specs (the old NCDEX spec was ~10% moisture, 2% FM basis); plant-delivered quotes embed quality claims mandi modal prices don't. **[?]** Couldn't verify a published FAQ↔contract-spec basis series anywhere.
- **[I]** Net: the median-modal series is fine as a *level/trend proxy* (robust, ±1–5% of trade-grade quotes) but should not be presented as a tradeable price.

---

## RQ3 — Candidate supplementary sources

| Source | What it publishes | Cadence | Licence / terms | Scrape/API feasibility |
|---|---|---|---|---|
| **data.gov.in mandi API** (current Layer 16) — https://api.data.gov.in/resource/9ef84268-… | Per-mandi bean min/max/modal, all states, variety+grade fields | Daily, same-day (updated ~18:30 IST) **[D]** | **GODL-India** (open, attribution) via NDSAP — the one GREEN licence in this space **[D/A]** | Already integrated; adding Maharashtra = filter change |
| **SOPA** (sopa.org) | Daily: soybean rate (Indore, Rs/qtl range, history to 2020, 222 pages), soymeal market rate (FAS/FOB Bedi/Kandla, FOR, Ex-Factory Indore, Rs/MT, READY+forward), solvent/refined oil (Indore, Rs/10kg); monthly USD FOB meal averages; state-wise production stats **[D]** | Daily (business days); monthly averages | Disclaimer: no accuracy guarantee, no liability; **no reuse/redistribution licence granted** — copyright default. AMBER/RED, same class as CEPEA-via-NA **[D]** | WordPress, server-rendered, no anti-bot observed (WebFetch worked). GOOD feasibility, licence caveat |
| **SEA of India** (seaofindia.com) | Weekly comparative rates: soybean seed Indore, soymeal EX-Indore 48/2.5, meal FAS Ex-Kandla US$/MT, **Soya Degum CIF Mumbai US$/MT**, solvent+refined oil, in Rs/MT + US$/MT; monthly oilmeal-export press releases; daily-rate PDFs (e.g. "DR260807") **[D]** | Weekly page + monthly stats (+ daily PDFs) | "© Solvent Extractors' Association … All Rights Reserved"; no open licence found. RED/AMBER **[D]** | Rates page is server-rendered (fetched fine today) but lives on a *reused 2020 permalink* — URL stability is a real risk; PDF parsing needed for daily series. MEDIUM |
| **Agriwatch** (agriwatch.com) | Mandi + market commentary, Indore benchmark framing | Daily | © Indian Agribusiness Systems Ltd, All Rights Reserved; freemium — data behind registration/paywall **[D]** | POOR — paywalled, price tables didn't render without login. Not a candidate |
| **data.gov.in variants (soy oil/meal)** | Mandi resource: `Soybean Meal` → **0 records [D]**; `Soyabean Oil` filter returned coconut-oil rows (filter unreliable for that value) **[D]**. DoCA Price Monitoring (fcainfoweb.nic.in / data.gov.in archives) has daily retail/wholesale packed soya oil across 75 centres | Daily (DoCA) | GODL for data.gov.in archives; fcainfoweb is a .nic.in reports portal, no API | Consumer-level packaged-oil prices — wrong grade for trade parity. Not worth adding |

Licensing note vs the 2026-08 audit: only the data.gov.in feed is licence-GREEN. SOPA/SEA would join CEPEA/Notícias Agrícolas in the "republished trade-association quotes, internal-use" bucket — acceptable for a personal/internal dashboard, not for redistribution.

---

## RQ4 — Verdict

**Mandi-only Layer 16 is NOT sufficient for physical buyers, but it is the right backbone. Do not restructure around meal parity — add to it.**

Reasoning:
1. The mandi feed is the only open-licensed, API-stable, same-day source — keep it as the primary bean leg.
2. Its MP-only scope now misses the #1 producing state (Maharashtra ~47% in 2025-26) — fixable inside the existing fetcher for free.
3. The signals a physical buyer actually trades — **SBM FOB Kandla competitiveness** and **crude-oil CIF import parity vs the 16.5% duty** — exist only at SOPA (daily) and SEA (weekly). Without at least the meal leg, the emerging-markets section's "meal buyers switch suppliers" narrative has no data behind it.

### Success criteria for an "adequate India leg for physical buyers"
- [ ] Bean price covering ≥80% of national production (MP + Maharashtra) — daily, open licence
- [ ] SBM export-parity line: India FOB (Kandla, USD/MT) vs CBOT SBM (ZM, USD/MT), updated ≥ weekly
- [ ] Oil import-parity line: CIF crude soy oil × (1 + effective duty, config-driven, currently 0.165) vs domestic solvent oil Indore — flagged when parity flips
- [ ] Duty rate stored in `config.py` with source + date, reviewed when Indian budget/notification news hits (hike actively under consideration mid-2026)
- [ ] Docstring premise corrected (MP no longer sole "soy belt"); series labelled as mandi-grade proxy, not tradeable price

### Proposed follow-up ticket outline (if accepted)
**Title:** India leg phase 2 — Maharashtra mandi + SOPA soymeal series + oil import parity
1. **16a (trivial):** extend `fetchers/mandi.py` to fetch MP + Maharashtra (config list of states); either keep separate series or production-weighted median; update docstring/CLAUDE.md premise.
2. **16b (new scraper):** `fetchers/sopa.py` → daily soymeal market rate table (FAS/FOB Kandla Rs/MT READY, Ex-Factory Indore) + optionally daily soybean Indore range; WordPress server-rendered, same `FetchResult`/history-CSV pattern as Layers 17–20; licence documented as internal-use (SOPA disclaimer).
3. **Analysis:** replace/augment "India bean vs CBOT premium" with (a) India SBM FOB vs CBOT ZM (USD/MT), (b) oil import-parity indicator using SEA CIF Mumbai (weekly) or a CBOT ZL + freight assumption; duty constant in config.
4. **Out of scope:** Agriwatch (paywalled), DoCA retail oils (wrong grade), NCDEX (suspended ≥2027-03).

### Top uncertainties carried
- SEA USD meal quote table-position ($253/MT line likely rapeseed meal — verify against the DR PDF before coding anything against it).
- CBIC notification text for the 16.5% duty not read directly (corroborated via USDA FAS GAIN + PIB-adjacent reporting); duty hike under active government consideration mid-2026.
- Mandi↔plant spread quantified from a single day (~3–5%); no published series found.
- SOPA state-table units inferred (lakh tonnes) from total-vs-national-estimate consistency.
