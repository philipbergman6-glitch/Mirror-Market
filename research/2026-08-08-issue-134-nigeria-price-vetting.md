# Issue #134 — X4: Nigeria domestic soybean price leg — AFEX / FEWS NET / NBS vetting

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/134 (part of umbrella #130)
- **Research date:** 2026-08-08 (all URLs/API calls accessed this date unless noted)
- **Question:** Can a `nigeria_domestic` soybean price leg (NGN/MT → USD/MT, basis vs CBOT, mirroring SAFEX/mandi/CEPEA) be built from a FREE, licensable, machine-readable source — vetting AFEX, FEWS NET, NBS, plus any other candidates surfaced?
- **Method:** Primary sources fetched directly (FEWS NET FDW API pulls, NBS Excel download, WFP/HDX CSV download, AFEX site pages and terms). Every claim tagged **[observed]** (fetched/parsed directly), **[inferred]**, or **[assumed]**. Cross-source price-level checks run on actual downloaded data where soybean or overlapping products exist.
- **Success criteria (set before work):** per-source GREEN/GRAY/RED licence verdict + cadence/format/access; sample data pulled from ≥2 sources with a cross-check; explicit go/no-go.

---

## 1. Headline finding

**NO-GO (for now).** Nigeria has no free, machine-readable domestic soybean price series as of 2026-08:

- **FEWS NET** and **WFP** — the two open, API/CSV-accessible Nigerian market-price feeds — both cover ~20–43 staples and **neither carries soybean at all** (verified by enumerating every product in both feeds, not just by name-filtering). **[observed]**
- **NBS** Selected Food Prices Watch is retail-only, monthly, and its full 43-item table has **no soybean**. **[observed]**
- **AFEX** — the only entity publishing a true Nigerian soybean market series (exchange-traded, NGN/MT) — has moved all price data behind a **paid portal (XIP, ≥ ₦1.5M/yr)**; its public "Daily/Weekly Reports" pages are empty shells. **[observed]**
- **CBN**'s "Commodity Price: Soyabeans" series (the one CEIC republishes in USD/MT) is an index of **world** prices of Nigeria's export commodities sourced from the World Bank Pink Sheet — not a domestic price; it duplicates Layer 8. **[inferred — strong, see §2.4]**

The absence is the finding: unlike SA (SAFEX), India (Agmarknet), and Brazil (CEPEA), Nigeria has no official/open domestic soy benchmark. Fallback paths in §5.

---

## 2. Per-source vetting

### 2.1 AFEX Commodities Exchange — licence **RED (paywalled)**; coverage would be excellent

| Aspect | Finding |
|---|---|
| Coverage | Soybean is a core listed commodity (SSBS); AFEX quotes NGN/MT exchange-traded prices — the highest-quality candidate series. **[observed via AFEX-attributed republication, see cross-check §3]** |
| Access | **Paid.** `africaexchange.com/reports/daily` 307-redirects to the AFEX Intelligence Portal `xip.afex.africa` **[observed]** — login + subscription: Basic ₦1,500,000/yr, Premium ₦1,800,000/yr, Enterprise ₦2,500,000/yr; datasets sold as one-time purchases or subscriptions, PDF/XLSX download after payment **[observed on xip.afex.africa]**. `africaexchange.com/reports/weekly` and `/reports` render "No latest report" / "Showing 0 - 0 of 0 entries" **[observed]**. Homepage has no public price ticker **[observed]**. Daily "price stickers" exist only inside the AfricaExchange mobile app **[observed via Google Play listing description]**. |
| Format | XIP: PDF + XLSX behind checkout. No public API found. **[observed]** |
| Cadence | Daily (exchange sessions); XIP history 3–10 years depending on commodity. **[observed — XIP marketing copy]** |
| Licence | Terms at `africaexchange.com/terms` contain no explicit redistribution grant; price-data accuracy is disclaimed and separate "platform rules" are referenced but not public. **[observed]** Republishing purchased data on a public dashboard would need explicit permission. |
| Verdict | **RED** for pipeline use: no free access path; scraping the app or a paid portal is out. The only route is a commercial/permission agreement with AFEX (`support@afex.africa` / `contactus@afexnigeria.com`). |

URLs: https://xip.afex.africa/ · https://africaexchange.com/reports/daily · https://africaexchange.com/reports/weekly · https://africaexchange.com/terms

### 2.2 FEWS NET (FDW API) — licence **GREEN (attribution)**; coverage **zero soybean** → no-go

| Aspect | Finding |
|---|---|
| Access | Open API, no key: `https://fdw.fews.net/api/marketpricefacts.csv?...` (CSV/JSON/XML). Docs: https://help.fews.net/fdw/fews-net-api. **[observed — multiple successful pulls]** |
| Format | Clean tidy rows: market, admin1/2, product, CPC code, period_date, value, currency, unit, lat/lon. `data_usage_policy` field = `"Public"` on every Nigeria row pulled. **[observed]** |
| Cadence | Nigeria collection is **weekly** (source_document: "FEWS NET, Nigeria, Price (weekly)"). **[observed]** |
| Coverage | **No soybean.** Evidence: (a) `country=NG&product=soybean&start_date=2025-01-01` → 0 rows; (b) `product=Soybeans` since 2020 → 0 rows; (c) `cpcv2=R01412` all-time → 0 rows; (d) decisively, filter-independent enumeration of **all** NG price facts across all datasets since 2026-05-01 (594 rows) and of the staple-food dataset since 2026-03-01 (5,049 rows) returns exactly 20 products: bread, cattle, cowpeas (brown/white), diesel, gari (white/yellow), gasoline, goats, groundnuts, maize (white/yellow), millet, palm oil, rice (5% broken/milled), sheep, sorghum (brown/white), yams. **[all observed 2026-08-08]** |
| Licence | FEWS NET's own primary series: reusable with mandatory, prominent attribution ("you must include attribution for the data you use in the manner indicated in the metadata"); third-party-sourced series in FDW "may not be redistributed or reused without the consent of the original data provider" — the NG price series are `source_organization = FEWS NET`, i.e. first-party. Policy: https://help.fews.net/fdp/data-and-information-use-and-attribution-policy. **[observed]** Note: policy states content "is not official United States Government information" — so *not* blanket public domain; GREEN-with-attribution, not GREEN-unconditional. |
| Verdict | Licence/tech **GREEN**, but **no-go on coverage** — nothing to fetch. Worth a periodic re-check (products are added over time); the maize/sorghum/cowpea series would be usable context if a West-Africa feed-grain line is ever wanted. |

### 2.3 NBS Selected Food Prices Watch — coverage **zero soybean** → no-go; licence GRAY

| Aspect | Finding |
|---|---|
| Access | e-library at `nigerianstat.gov.ng/elibrary` (flaky; listing page served nothing newer than Oct 2024 when fetched, though press coverage confirms an April 2026 edition was released June 2026 — Punch/ThisDay/Vanguard, 2026-06-02). **[observed both]** |
| Format | Per-edition PDF + Excel. Direct file pattern observed: `https://nigerianstat.gov.ng/resource/selected_food_oct_2024.xlsx` (downloaded, 23 KB, 2 sheets). **[observed]** |
| Cadence | Monthly, national + 37-state granularity, retail prices per kg. **[observed]** |
| Coverage | Full item list parsed from the Excel: **43 retail items — zero soy matches** (`beans brown`, `beans: white black eye` are cowpeas; oils are groundnut/palm/vegetable). 2026 press summaries likewise mention no soybean. **[observed]** |
| Licence | Site footer has a "Terms of Use" link; terms text not vetted further since coverage already disqualifies the source. **[observed link only — GRAY]** |
| Verdict | **No-go on coverage** (and retail-per-kg is the wrong market level for a basis leg anyway). |

### 2.4 CBN "Commodity Price: Soyabeans" (extra candidate) — **not a domestic price** → no-go

- CEIC republishes "Nigeria Commodity Price: Soyabeans", monthly USD/MT, Jan 2009–Sep 2025 (403.96 USD/MT Sep 2025), source: Central Bank of Nigeria. **[observed via search excerpts; ceicdata.com itself 403'd]**
- CBN's own Economic Report (Jan 2025, `cbn.gov.ng/Out/2025/RSD/January 2025 Economic Report.pdf`) describes its commodity-price section as **world** commodity prices sourced from the **World Bank Pink Sheet** (soya bean −2.08% MoM in an index of average world prices); the Quarterly Statistical Bulletin's Table C.11 is "Indices of Average World Prices of Nigeria's Major Agricultural Export Commodities". **[observed via search excerpts of the PDFs]**
- Level check corroborates: 403–407 USD/MT (Aug–Sep 2025) tracks the world price (World Bank/CBOT ~$380–440), not Nigerian domestic (~$440–480, §3). **[inferred]**
- Access would be via `statistics.cbn.gov.ng` ("LiveShop" data store; free/paid status unverified). **[observed shell page only]**
- Verdict: **no-go — duplicates Layer 8 (World Bank CMO)**; adds nothing domestic.

### 2.5 WFP food prices via HDX (extra candidate) — licence **GREEN**; coverage **zero soybean** → no-go

- `https://data.humdata.org/dataset/wfp-food-prices-for-nigeria` → CSV `wfp_food_prices_nga.csv` downloaded: **87,729 rows, current to 2026-07-15**, 43 commodities, monthly, per-market with lat/lon, NGN + USD columns, HXL-tagged. **[observed]**
- `df['commodity'].str.contains('soy')` → **0 rows**; full commodity list has no soy item. **[observed]**
- Licence: HDX WFP datasets are CC-BY (standard for this dataset family) **[assumed — dataset licence field not re-verified this session]**; machine-readable and CI-friendly either way.
- Verdict: **no-go on coverage**; best-in-class plumbing, wrong product list. Same periodic re-check note as FEWS NET.

---

## 3. Sample-data cross-checks

### 3.1 Feed-vs-feed consistency (the two open feeds, overlapping products)

Since neither open feed has soybean, the consistency check ran on overlapping staples — June 2026, NGN/kg, national averages across markets **[observed — computed from the two downloaded datasets]**:

| Product (Jun 2026) | WFP (retail, → per kg) | FEWS NET (per kg) | Gap |
|---|---|---|---|
| Sorghum | 1,006.8 per 2.7 kg = **372.9** | brown 402.9 / white 399.0 | ~7% |
| Millet | 1,162.8 per 2.6 kg = **447.2** | pearl 473.4 | ~6% |
| Cowpeas | 3,047.5 per 2.5 kg = **1,219.0** | brown 941.1 / white 924.5 | ~25% (retail vs market mix; different market panels) |

Grains agree within single digits — both feeds are real and internally sane; the gap is purely the missing product.

### 3.2 Soybean price-level triangulation (secondary sources only — this is the point)

No two independent *machine-readable* soybean series exist to cross-check; the best available triangulation, all converted at the repo's observed NGN rate (₦1,361/USD on 2026-08-07, `currencies` table **[observed — local DB]**):

| Source | Quote | ≈ USD/MT |
|---|---|---|
| CBOT front-month (repo DB, 2026-08-07) | 1,181.25 ¢/bu | **$434** **[observed]** |
| Kano wholesale, Jan 2026 (bestsales.ng) | ₦600,000–650,000/MT | $441–478 (at current rate) **[observed via search; secondary]** |
| Selina Wamucii farmgate, Jul 2026 | $0.445/kg | $445 **[observed via search; secondary — their "205 NGN/kg" companion figure is internally inconsistent with their own USD figure (would be ~₦690/kg at ₦1,550/USD) and is discarded]** |
| AFEX exchange, Apr 2025 low (via Nairametrics quoting AFEX) | ~₦843,000/MT | — (2025 rate; ≈$520–560 at 2025 rates) **[observed via search; secondary]** |
| AFEX exchange, Q3 2024 peak (via AFEX-attributed reporting) | ₦1,180,000/MT | — **[observed via search; secondary]** |

Reading: Nigerian domestic soybeans currently sit roughly at parity to a modest premium vs CBOT (~$435–480 vs $434) after the 2024 spike (>2× CBOT) unwound — consistent with the 2026 narrative of India/US traders active in West Africa. Levels are *plausible and mutually consistent*, but every domestic point is a one-off press/aggregator quote, not a series — exactly why the leg can't be built yet.

---

## 4. Verdict table

| Source | Soybean coverage | Cadence | Format/access | Licence | Leg-feasibility |
|---|---|---|---|---|---|
| AFEX / XIP | **Yes — the series** (SSBS, NGN/MT) | Daily | Paid portal, PDF/XLSX; app-only free stickers | **RED** (paywall; no redistribution grant) | Only via commercial agreement |
| FEWS NET FDW | **None** (20 products enumerated) | Weekly | Open API, CSV/JSON — excellent | **GREEN** (attribution required) | No-go: nothing to fetch |
| NBS Food Prices Watch | **None** (43 items enumerated) | Monthly, retail | PDF + Excel; flaky e-library | GRAY (terms unvetted) | No-go: coverage + wrong market level |
| CBN "Soyabeans" | World price index, not domestic | Monthly | statistics.cbn.gov.ng (unvetted) | GRAY | No-go: duplicates Layer 8 |
| WFP / HDX | **None** (43 commodities enumerated) | Monthly | CSV on HDX — excellent | GREEN (CC-BY assumed) | No-go: nothing to fetch |

## 5. Recommendation

**NO-GO on a `nigeria_domestic` price leg in 2026-08.** Do not build a fetcher; the Nigeria block stays PSD + NGN/USD + Benue/Kaduna weather + players prose.

What *is* actionable:

1. **AFEX permission email** (the audit's licensing-RED standard, same as the ANEC gate): ask whether a single daily soybean benchmark price may be republished with attribution on a free dashboard. This is the only path to a real leg. Cheap to send; record outcome in the licensing register.
2. **Quarterly re-check of FEWS NET + WFP product lists** (two one-line API calls — §2.2(d)/§2.5) in case soybean is added; both feeds are GREEN and CI-ready the day that happens.
3. **Narrative fallback (no pipeline):** the pitch can honestly say "Nigeria has no open domestic soy benchmark — we triangulate from AFEX-attributed reporting and CBOT parity" — itself a differentiating observation for the West-Africa demand story (#130 yardstick).
4. Do **not** add the CBN series (world-price duplicate) or an NBS scrape (no soy, retail level).

## 6. Cross-reference

- Umbrella #130: X4 resolves as research-complete/no-build; does not block X5/X6.
- Licensing register additions: AFEX (RED — paywalled, terms silent on redistribution), FEWS NET (GREEN w/ attribution — first-party series only; third-party FDW series need provider consent), WFP/HDX (GREEN, CC-BY assumed), NBS (GRAY, unvetted terms, moot), CBN (GRAY, moot).
- Pattern precedent: SAFEX (Layer 18) / mandi (Layer 16) / CEPEA (Layer 17) all exist because an official or exchange publisher offers free structured data. Nigeria's equivalent publisher (AFEX) chose a paid-data model — the structural difference, not a scraping gap.
