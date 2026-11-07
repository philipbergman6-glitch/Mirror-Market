# Africa → Europe soy: what origin-side data exists, and where it comes from

- **Date:** 2026-11-06 (every URL fetched this date; clock stamps UTC). Checkout: `main` @ `2515168`.
- **Question:** for a physical buyer moving soybeans (HS 1201) or soymeal (HS 2304) from Nigeria or West/Southern Africa to Europe, what *origin-side* data exists — export rules, export volumes, freight, and the NGN rate the AFEX leg converts through — and is any of it free, primary, and machine-readable?
- **Prior notes:** `research/2026-08-10-nigeria-price-source.md` (AFEX found, 15 candidates probed); `LAYERS.md` Layer 36 (AFEX, published 2026-11-05 under the A18 stance); Layer 7 (NGN/USD from Yahoo `NGNUSD=X`).
- **Method:** four parallel probes (rules/prices, volumes, freight, FX), primary sources only, every claim quoted from the response. No emails sent, nothing signed up for. Raw downloads kept under the session scratchpad `…/scratchpad/dl/` (not in the repo).

**Evidence tiers:** **[D]** directly observed today · **[I]** inference from observed data · **[A]** assumption or secondary source · **[?]** could not verify.

---

## Verdict (up front)

| # | Item | Verdict | One line |
|---|---|---|---|
| 1a | Nigeria soybean export ban | **No ban found — but the 2026 list is unread** | Official NSW trade portal lists 8 prohibited export items, no soya [D]; the 2026 Fiscal Policy Measures carry a "Revised Export Prohibition List" on `customs.gov.ng`, which is behind Cloudflare and unreadable from here [?]. Owner should open it in a browser. |
| 1b | Export levy / docs | **Verified from CBN's own manual** | NESS levy 0.5% of FOB (non-oil); Form NXP via bank, 90-day validity, 90-day repatriation; CCI mandatory to load; NEPC registration ₦13,500 [D]. No export duty found [I]. |
| 1c | Nigerian FOB / export price | **No FOB series exists anywhere official** | New find: **NEPC monthly "LICPL" PDF** carries state-level soya beans (₦/100 kg) and **Lagos soymeal ₦/MT** [D] — buildable to store, owner-decision to publish (NEPC terms bar republication). AFEX has no FOB field. LCFE one row, NBS Food Watch no soybean. |
| 2a | Export volumes — UN Comtrade | **Buildable-free (keyless preview), licence = owner-decision** | 500 rows / 1 period per call, 500 calls/day, 1/s [D]; "provided for internal use only … may not be re-disseminated" [D]. |
| 2b | Export volumes — FAOSTAT | **Buildable-free, CC BY 4.0** | Bulk zip 11.9 MB, through 2024, updated Jul-2026 [D]; keyless API dead (401) [D]. |
| 2c | Mirror divergence | **Nigeria under-reports HS 2304 ~9×** | Nigeria says 17.7 kt to France (2024); France says **159.3 kt** from Nigeria [D]. Togo 1201 to EU: self 22 kt vs EU-mirror 66.9 kt [D]. Use EU-mirror for EU-bound legs. |
| 2d | ITC Trade Map | **Login** | "available only to users with MAT Pro access" [D]. |
| 3 | WA → Europe freight | **Nothing free and machine-readable** | Baltic Akamai-walled + paid; FBX/Drewry/Xeneta have no Africa lane; SCFI Lagos lane is image-only; CCFI is China→Africa index points. Only primary Lagos→Europe print: **Nigerian Shippers' Council quarterly indicative PDF** (Q2-26: 20' $1,404 / 40' $2,004 export) [D]. |
| 4a | NGN official rate | **Buildable-free, keyless JSON** | `https://www.cbn.gov.ng/api/GetAllNFEM_Rates` — NFEM VWAP + close + hi/lo + turnover, daily, 2024-12-02 → 2026-11-05 [D]. No terms page exists on cbn.gov.ng [D]. |
| 4b | NGN parallel rate | **Not feedable publicly** | abokifx paywalled (API £100/yr) + no-redistribution clause; ABCON publishes nothing; aggregators disagree ₦35 same morning [D]. |
| 4c | Yahoo `NGNUSD=X` | **Replace** | ICE Data Services composite; prints ₦2–36 *below* NFEM VWAP and ₦40–80 below parallel [D]. Not the official rate, not the street rate. |

**The one actionable build:** swap the NGN leg's FX source from Yahoo to the CBN NFEM endpoint. It is the official rate by CBN's own definition ("NFEM rate … stands as the official exchange rate for the day"), keyless, daily, and the thing a Nigerian seller actually prices against. Everything else here is either a reference table (rules), an annual series (volumes), or a quarterly PDF (freight).

---

## 1. Nigeria — export rules, charges, prices

### 1a. Export prohibition status

| Source | URL | Observed | Tier |
|---|---|---|---|
| **Nigeria Customs Service** export prohibition list | `https://customs.gov.ng/?page_id=3079`; `https://customs.gov.ng/publications/circulars/customs-excise-tariff-etc-variation-order2026` | curl `HTTP/2 403`, body "Just a moment…" (Cloudflare). Playwright: challenge never cleared after 20 s. WebFetch 403. | [?] primary unreadable |
| **Nigerian Trade Information Portal** (National Single Window, official) | `https://tip.nsw.gov.ng/trade-information/prohibited-items` | `200 text/html`. Under `EXPORT PROHIBITION LIST NCS`: "1. Maize; 2. Timber (rough or sawn); 3. Raw hides and skin … HS 4101.2000.00 – 4108.9200.00; 4. Scrap Metals; 5. Unprocessed rubber latex and rubber lumps; 6. Artifacts and Antiquities; 7. Wildlife animals classified as endangered species…; 8. Goods imported can be re-exported". **No soya.** No edition date on the list. | [D] |
| **CBN Foreign Exchange Manual, 4th ed. (2026)** | `https://www.cbn.gov.ng/Out/2026/CCD/The%20new%20Forex%20Manual%20-%204th%20Edition.pdf` (5.3 MB) | Exports memo item xvi: "Goods prohibited for exportation from Nigeria, as contained in the annual Fiscal Policy measures, shall not be inspected by Pre-shipment Inspection Agents." Appendix V: "PROHIBITED EXPORTS: Please refer to the Federal Ministry of Finance for guidiance [sic]." Zero occurrences of "soy". | [D] |
| **FMINO press** — NCS implements 2026 Fiscal Policy Measures | `https://fmino.gov.ng/nigeria-customs-service-implements-2026-fiscal-policy-measures-and-tariff-amendments/` | 29 Jul 2026. Amendments include "the Revised Import Prohibition List (Trade) and the Revised Export Prohibition List"; names no goods; points to the customs.gov.ng URL above. | [D] that a revised list exists; [?] its contents |
| **FMINO press** — National Soybean Production Expansion Policy | `https://fmino.gov.ng/fg-launches-national-soyabean-production-policy-and-strategy-to-achieve-food-sovereignty-others/` | 23 Jul 2025. Minister on becoming "a leading supplier of raw grains and value added products"; "export earnings". No ban language. | [D] |
| USDA FAS GAIN NI2024-0007 | `apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Review+of+Agricultural+Import+and+Export+Restrictions_Lagos_Nigeria_NI2024-0007.pdf` | "'Prohibited Items List During Export' containing seven product categories … corn, timber, and hides and skins." Export (Prohibition) Act 1989 bars beans, cassava, maize, rice, yam and "all imported food items" — "does not appear to be closely enforced". No soybean. | [A] |
| USDA FAS GAIN NI2026-0010 (22 May 2026) | same host, `…Nigeria+Announces+2026+Fiscal+and+Trade+Policy+Changes_Lagos_Nigeria_NI2026-0010.pdf` | Import-side only. Soy lines: `1507100000 Crude soyabean oil … Prohibited → NOT Prohibited`; refined stays prohibited. No export-list change discussed. | [A] |
| 2024 press (BusinessDay/allAfrica) | via search | Poultry Association of Nigeria "appealing to the federal government to ban exportation of soybeans and maize". | [A] |

**Reading [I]:** the "2024/25 soybean export ban" is a poultry-industry lobbying ask, not an instrument; the 2025 raw shea-nut export ban is the measure it gets conflated with. The governing instrument is the annual Fiscal Policy Measures; the 2026 edition's revised export list is the one document to read, and it was not readable from this machine. **Residual risk stated:** soya could have been added in the 2026 revision; nothing observed suggests it, and the May-2026 GAIN review of the same FPM discusses no export-list change.

### 1b. Levies, documentation, duty

| Item | Source | Observed | Tier |
|---|---|---|---|
| **NESS levy (non-oil)** | CBN FX Manual 4th ed., Memorandum on Exports, A. Non-Oil, item viii | "The exporter shall be required to pay the assessed Nigerian Export Supervision Scheme (NESS) Levy on the exported non-oil products (0.5% of the FOB value) through a designated bank after inspection of the non-oil export and deliver the receipt to the PIA." Item ix: PIA issues the Clean Certificate of Inspection "after receipt of Form NXP and NESS Levy receipt". Item xiii: NCS "shall ensure that any cargo without a CCI … is not loaded". | [D] |
| **Form NXP** | same | "The Form NXP shall be utilised within ninety (90) days from the date of registration"; proceeds "repatriated and credited to their export domiciliary account in the bank where the NXP was established, within 90 days from the Bill of Lading date". "TRMS" does not appear in the 4th edition; RFI goes "through the SVP" (Verification Portal). | [D] |
| NXP on CBN TRMS | 2019 CBN circular via Mondaq / Banwo & Ighodalo | Electronic NXP on TRMS; TIN required. Primary PDF not fetched. | [A] |
| **NEPC registration** | `https://nepc.gov.ng/get-started/e-registration/` | "New certificate registration: N13,500; Expired certificate renewal: N7,500; Late renewal (>3 months): N12,500"; valid 24 months then yearly. Portal `nepc.gov.ng/ereg/exporter`. | [D] |
| NEPC documents page | `https://nepc.gov.ng/get-started/export-documents-procedures/` | NXP "issued by a commercial bank"; CCI "ascertains the quality, quantity and price competitiveness of Nigerian exports". | [D] |
| **Export duty on soybeans** | — | Not found in the FX Manual, NEPC, TIP or GAIN. Nigeria's tariff is import-side. | [I] none; [?] customs tariff unreadable |
| Latest NESS circular (TED/FEM/FPC/PUB/001/005, 29 Oct 2026) | press (Leadership, Nairametrics, Vanguard) | Reallocates **oil/gas** pre-shipment agents from 2 Oct 2026; not a rate change. `cbn.gov.ng/Documents/circulars.html` is JS-rendered, `Last-Modified: 02 Apr 2025`. | [A] |
| Trade portals | `nigeriatradeportal.fmiti.gov.ng`, `trade.gov.ng`, `ness.gov.ng`, `nxp.cbn.gov.ng`, `trms.cbn.gov.ng` | all curl `000` (DNS/connect failure). `nigeriatradehub.gov.ng` → bare "Index of /". `tip.nsw.gov.ng` is the live official portal. | [D] |

### 1c. Price series at origin

| Source | Access | Observed | Cadence / format | Licence | Verdict |
|---|---|---|---|---|---|
| **NEPC Local Commodity Price List (LICPL) — new** | `https://nepc.gov.ng/indicative-market-prices/` → monthly PDFs, e.g. `https://nepc.gov.ng/cms/wp-content/uploads/2026/10/SEPTEMBER-LICPL.pdf` (200, 432 KB, 8 pp, Excel-generated 5 Oct 2026) | `SOYA BEANS (UNIT:100KG PER BAG)` by state × 4 weeks (Sep 2026: Jigawa ₦110,000; Kano ₦76,000–79,000; Benue ₦145,000; Osun ₦183,900) and `SOYAMEAL (UNIT: METRIC TONS)`: "LAGOS ₦1,000,000 / 1,000,000 / 1,240,000 / 1,260,000" weeks 1–4. Footer "SOURCE: NEPC (REGIONAL & STATE) OFFICES". No FOB column. Weekly columns mostly flat ("% PER WEEK 0%") → looks like a monthly collection spread over weeks [I]. | Monthly PDF, irregular (gaps Jun–Aug 2025, Oct–Dec 2025); filenames unstable → rotating-URL trap, resolve from the listing page | NEPC Terms: "print and download extracts … for your own personal use"; "may not modify, alter, republish, redistribute, resend, sell or broadcast any material … without our prior written permission" | **Buildable-free to store; owner-decision to publish** (same A18 shape as AFEX). The only official Nigerian **soymeal** price found. |
| **AFEX** (Layer 36) | `api-md.afexnigeria.com/AFEXMD/api/v1/securities/price?security_code=SSBS&date_from=…&date_to=…` + `Origin: https://africaexchange.com` | 200: `{"responseCode":"100","data":[{"SSBS":570.87,"date":"2026-11-05"},…]}` — still live. `/securities` list → 404. `africaexchange.com/markets` shows Best Sell / Best Buy / Closing / Market Value only. Disclaimer PDF names an "AFEX EXPORT INDEX (AEI)" — no public series found. | Daily JSON | as Layer 36 | Already ingested; **no FOB field** [D]. AEI = question for the unsent AFEX email. |
| **LCFE** (re-check) | `https://lcfe.ng/market-data.php` (HEAD 406; GET with UA 200) | Changed since Aug: `02/10/2026 | SOYA BEANS | NIGER/NASARAWA/KEBBI | KG | 790`; chart 790 (02/10) vs 770 (18/09). One row, no archive, no terms page. | Ad hoc, ~fortnightly | none found | Dead for a series |
| **NCX** | `https://ncx.com.ng/` | "LIVE DATA … Soya (SOYKAD) ₦695.00 … Soya (SOYNAS) ₦687.00" — undated, unit absent. | undated | — | Dead for a dated series |
| **NBS Selected Food Price Watch** (re-check) | `https://microdata.nigerianstat.gov.ng/index.php/catalog/162/download/1427` (200, zip 5.3 MB: May-2026 PDF + xlsx); `nigerianstat.gov.ng` itself `000` | xlsx shared strings: **zero** "soy"; PDF grep "soy" = 0. Retail items only. | Monthly, ~1 mo lag | not checked | Dead for soybeans — confirmed out of scope |
| NBS Food Price Tracking CSV | not re-fetched (host unreachable) | — | — | — | [?] assumed still frozen at 2025-06 |
| FMAFS/FMARD price data | `fmafs.gov.ng` → 404 | — | — | — | Dead |
| Nigerian port FOB | — | **None** from any official body; only vendor indicatives on the open web (e.g. "FOB Lagos $480–580/MT") [A]. | — | — | Gap — withhold with reason |

---

## 2. Export volumes — Nigeria, Ghana, Benin, Togo, Burkina Faso, Zambia, Malawi, Uganda, Ethiopia, South Africa

### 2a. Headline table (UN Comtrade preview, reporter-declared exports, flow X)

Base call [D]: `https://comtradeapi.un.org/public/v1/preview/C/A/HS?reporterCode=<code>&period=<yyyy>&cmdCode=1201,2304&flowCode=X`. EU-27 mirror [D]: `…reporterCode=97&period=2024&cmdCode=1201,2304&flowCode=M&partnerCode=566,288,204,768,854,894,454,800,231,710` (97 = "European Union", `isGroup: true`). Tonnes = `netWgt`/1000; "World" = `partnerCode=0`. 2024 is the last complete year; 2025 annual is already released for 8 of 10 reporters but thin.

| Origin (code) | HS 1201 t 2024 → 2025 | HS 2304 t 2024 → 2025 | Top-3 partners 2024 (1201 / 2304) | EU-27 mirror import 2024 (1201 / 2304) |
|---|---|---|---|---|
| Nigeria (566) | 472,022 → 73,111 | 18,167 → no rows | Pakistan 239,460 · India 190,884 · Canada 10,876 / France 17,726 · India 229 · Türkiye 61 | 501 / **161,353** |
| Ghana (288) | 55,627 → 2,986 | 2,092 → World blank (USA 12,490) | USA 21,006 · Pakistan 17,933 · India 9,913 / UK 580 · Ireland 564 · France 480 | 0.2 / 914 |
| Benin (204) | 184,817 → no rows | 134,811 → no rows | China 79,626 · Pakistan 56,683 · India 43,046 / Pakistan 74,421 · USA 30,980 · France 6,615 | 3,729 / 9,256 |
| Togo (768) | 49,196 → 48,226 | 111,177 → 137,161 | India 17,240 · **France 8,583 · Italy 7,012** / USA 59,537 · Pakistan 17,131 · UK 6,330 | **66,864** / 15,421 |
| Burkina Faso (854) | 14,599 → 1,448 | 6,470 → 1,606 | Togo 11,215 · Ghana 2,827 · France 498 / Senegal 6,311 · Mali 104 · Ghana 55 | 6,642 / 0 |
| Zambia (894) | 13,658 → 2,513 | 114,252 → 55,090 | Zimbabwe 10,242 · India 1,334 · Uganda 848 / Zimbabwe 37,262 · Tanzania 34,530 · Kenya 26,330 | 0.02 / 0 |
| Malawi (454) | 797 → 1,641 | 24,508 → 45,541 | Zimbabwe 300 · Zambia 208 · Tanzania 150 / Tanzania 13,912 · Kenya 4,482 · Mozambique 1,981 | 0 / 0 |
| Uganda (800) | 10,549 → 14,040 | 22,693 → 25,325 | Kenya 7,489 · Rwanda 3,032 · Sierra Leone 16 / Kenya 13,311 · USA 4,034 · Rwanda 2,554 | 0.2 / 4,050 |
| Ethiopia (231) | 2023: 207,604 (2024/25 no rows) | 2023: 8,703 | India 92,059 · Viet Nam 27,125 · Singapore 26,601 / USA 6,221 · India 1,832 · Oman 450 | 0.04 / 264 |
| South Africa (710) | 2024 World null (Zimbabwe 87,973 · Viet Nam 28,158 · Eswatini 7,385); 2023: 688,969 → 2025: 220,112 | 181,109 → 215,758 | see left / Botswana 41,921 · Zimbabwe 40,339 · Eswatini 38,954 | 0.04 / 0.02 (2023: **29,923** 1201 to Portugal vs SA-reported 29,807) |

Quoted row (Nigeria 2024 world, HS 1201) [D]:
```
{"period":"2024","reporterCode":566,"flowCode":"X","partnerCode":0,"cmdCode":"1201","netWgt":472022404.0,"isNetWgtEstimated":false,"primaryValue":118011498.884,"isReported":false,"isAggregate":true}
```
Traps [D]: `reporterDesc`/`partnerDesc`/`cmdDesc` are **null in preview** — join from `https://comtradeapi.un.org/files/v1/app/reference/partnerAreas.json` (200, 96 KB; **France = 251, not 250**; Italy 380). `reporterAreas.json` → `BlobNotFound`; use `Reporters.json`. South Africa 2024 World `netWgt` null while partner rows exist; Ghana 2025 2304 World row has no `netWgt` but the USA leg does → treat World as withheld, never sum partners into it.

### 2b. Reported-vs-mirror divergence — the finding that matters for a Europe-bound buyer

- **Nigeria HS 2304 is self-reported ~9× too low.** Nigeria → France: 2022 106,859 t · 2023 56,620 t · 2024 17,726 t. France (251) imports from Nigeria: 2023 **158,585 t** ($89.6 M) · 2024 **159,269 t** ($83.6 M) [D]. EU-27: 2023 161,699 · 2024 161,353 · 2025 2,464 t (partial-year or collapse — [?]). Call: `…reporterCode=251&period=2024&cmdCode=1201,2304&flowCode=M&partnerCode=566,…` → `"2304 from Nigeria: netWgt=159,268,899 kg value=$83,576,954"`.
- Nigeria HS 1201 2023 "France 33,985 t" vs France's 12 kg import [D] → [I] Nigerian partner coding is unreliable.
- **Togo HS 1201 to EU:** Togo says France 8,583 + Italy 7,012 + Belgium 6,368 (2024); EU-27 mirror **66,864 t** (France 13,042; Italy 16,263; Belgium 12,904; Spain 2,350; Germany 2,244; NL 1,021) [D]. 2025 EU mirror: 83,592 t 1201 + 37,185 t 2304 from Togo [D].
- **Burkina Faso:** reports France 498 t; EU mirror 6,642 t (Belgium 5,416) [D].
- **South Africa 2023 Portugal leg reconciles within 0.4%** (29,807 vs 29,922 t) [D] — SA customs is the one African reporter here that mirrors cleanly.
- **FAOSTAT already replaces Nigeria with external figures** (flag `X`): Nigeria 2304 2024 = 371,268 t (X), 1201 = 283,379 t (X), vs Comtrade self-report 18,167 / 472,022 [D]. The two disagree in *both directions*; neither is a customs record. **[I] For Nigeria, use the EU mirror for EU destinations and treat the self-declared world total as unreliable. Store both, never net them.**

### 2c. Source verdicts

**A. UN Comtrade public preview — buildable-free, licence = owner-decision**
- Keyless `comtradeapi.un.org/public/v1/preview/...` → 200 with data; no `subscription-key` needed [D].
- Limits [D]: `{"error":"Maximum number of periods for preview is 1"}` (400) when `period=2022,2023,2024`; `{"statusCode":429,"message":"Rate limit is exceeded. Try again in 1 seconds."}` with `Retry-After: 1`. Two `cmdCode`s in one call worked despite the docs' "1 product" wording.
- Docs (`https://uncomtrade.org/docs/subscriptions/`, 200) [D]: "API Preview data API Quota Limited to 500 records and 1 single period and product per call 500 calls/day … API Rate Limit 1 call per second". Basic free key: 100K records/call, 500 calls/day.
- Policy (`https://uncomtrade.org/docs/policy-on-use-and-re-dissemination/`) [D]: "UN Comtrade data are provided for internal use only and may not be re-disseminated in any form without UNSD's permission. The exemption (e.g., a small amount of data) is explained in the section on re-dissemination." → publishing full tables is a licence question (invariant 9); a few cited figures are the stated exemption.
- Availability (`…/public/v1/getDA/C/A/HS?reporterCode=…`, 200, 298 rows) [D]: 2025 annual released Nigeria 2026-08-07, Zambia 2026-11-03, SA 2026-10-02, Togo 2026-06-03, Ghana 2026-06-30, Uganda 2026-09-29, Malawi 2026-09-24, Burkina 2026-10-30, Benin 2026-10-01 (3,090 records → [I] partial); Ethiopia latest 2023.
- Cadence annual, 6–11 months after year end, revised irregularly. 10 reporters × 1 period ≈ 10–20 calls/day — well inside 500/day. `comtradedeveloper.un.org` is a 5 KB JS shell [D].

**B. FAOSTAT — buildable-free, CC BY 4.0 (not the NC-SA licence the brief assumed)**
- Bulk `https://bulks-faostat.fao.org/production/Trade_CropsLivestock_E_Africa.zip` → 200, `content-length: 11868740`, `last-modified: Fri, 24 Jul 2026` [D]. Columns Y1961…**Y2024**; items `236 Soya beans`, `238 Cake of soya beans`; element `5910 Export quantity` (t) [D].
- API `https://faostatservices.fao.org/api/v1/en/data/TCL?area=159&item=236&element=5910&year=2023` → **401 "Missing Authorization Header"**; `fenixservices.fao.org/faostat/api/v1/…` → 521 [D]. Keyless API is dead; use the zip.
- Licence (`https://www.fao.org/contact-us/terms/db-terms-of-use/en`) [D]: "all datasets disseminated through FAO corporate statistical databases … are licensed under the Creative Commons Attribution-4.0 International licence (CC BY 4.0)". Redistribution permitted with the stated citation.
- FAO 2024 export t (flag) [D]: Nigeria 1201 283,379 (X) / 2304 371,268 (X); Ghana 87,684 (X) / 14,065 (X); Benin — / 134,811 (A); Togo 49,196 / 111,177 (A); Burkina 14,599 / 6,470 (A); Zambia 13,658 / 114,252 (A); Malawi 12,548 (X) / 24,508 (A); Uganda 10,549 / 22,693 (A); Ethiopia 105,028 (X) / 49,337 (X); South Africa 150,412 (I) / 224,659 (I). Flags: `A` official, `I` imputed, `X` external. Where `A`, FAO equals Comtrade to the tonne → [I] FAO is Comtrade re-hosted plus imputation. No partner breakdown in TCL (`Trade_DetailedTradeMatrix` not tested).

**C. ITC Trade Map — login**
- `https://www.trademap.org` → Angular shell; `Index.aspx` → `legacy.trademap.org`: "Trade Map has moved to the new version. The legacy Trade Map is now available only to users with MAT Pro access. … Login / Register" [D]. Not probed further.

**D. National statistics**
- **SARS trade stats portal** `https://tools.sars.gov.za/tradestatsportal/` and `data_download.aspx` (200, 246 KB): ASP.NET postback form "Trade Data Download — Trade Type: Exports/Imports; Focus Area: Tariffs/Countries" [D]. Monthly HS8 is there behind a ViewState POST — a separate spike; licence unstated [?].
- **Nigeria NBS** `nigerianstat.gov.ng` (197.159.69.92): every HTTPS fetch timed out at 45–90 s, three attempts [D]. Whether the quarterly Foreign Trade PDF lists soybeans is [?]. Retry from another network before writing off.
- SAGIS: already Layers 23/24, not re-tested.

---

## 3. Freight — West/Southern Africa → Europe

**Headline [D/I]:** no index publisher offers a free, machine-readable Africa→Europe lane. Every Africa leg found is China→Africa (SCFI/CCFI/NCFI), bot-walled and paid (Baltic), or absent (FBX, Drewry WCI, Xeneta). The single primary Lagos→Europe export print is a quarterly government PDF.

| Source | URL | Observed | Cadence / access | Licence | Verdict |
|---|---|---|---|---|---|
| **Baltic Exchange** (BSI/BHSI) | `balticexchange.com/en/data-services/routes.html`, `/terms-conditions.html`, `/pricing.html`, `/api.html` | Every URL → 200 with a 1,946-byte body `<title>Challenge Validation</title>` (Akamai proof-of-work, `"difficulty":5000`); Playwright still "Processing your request…" after 12 s; PDFs and `robots.txt` → 403 [D]. Routes [A, not observed]: BSI63 has S3 N.China→WAF, S5 WAF via ECSA→N.China, **S9 WAF via ECSA→Skaw-Passero** ($/day TC, not $/MT); BHSI HS1–HS7 has no Africa route (HS3 Recalada→Skaw-Passero, HS4 USG→Skaw-Passero nearest Atlantic proxies). | Subscription | unreadable | **Paid**; terms not quotable; S9 is a weak proxy even if licensed [I] |
| **Freightos FBX** | `fbx.freightos.com` → `freightos.com/enterprise/terminal/freightos-baltic-index-global-container-pricing-index/` (200, 430 KB) | Lanes in embedded JSON: FBX01–04, 11–14, 21–22, 24, 26 — e.g. `{"label":"FBX01","value":"$8,340","change":"-0.33%"}`, FBX11 $3,595, FBX24 $1,574. **No Africa lane.** Daily 06:00/14:00 UTC, weekly Friday. API (`apidocs.freightos.com`) needs `apikey` + `secret-key` and is the FAX **air** index [D]. | Daily; HTML-embedded JSON (scrapable) | `freightos.com/freightos-data-terms-conditions/`: free parts "may only be reproduced with full credit to Freightos"; "does not include the right to resell"; AI-training bar [D] | Buildable-free with attribution, **useless for Africa** |
| **Drewry WCI** | `drewry.co.uk/supply-chain-advisors/…/world-container-index-assessed-by-drewry` | curl → `HTTP 429 Too Many Requests(6)` first hit; cached fetch: composite "US$4,389 per 40ft, down 1%… 05 Nov 2026", Shanghai–Rotterdam 3,615, –Genoa 4,166; route table is an image; "To access this content please login". 8 lanes, **no Africa** [A]. | Weekly; login | `drewry.co.uk/terms`: "may not copy, reproduce, republish, distribute…"; "must not use any part of the Content for commercial purposes" [D] | **Login / redistribution barred**; no Africa anyway |
| **Xeneta XSI-C** | `https://xsi.xeneta.com/` (200) | 8 lanes (Far East↔N.Europe, FE→USWC, N.Europe↔USEC, →SAEC…). **No Africa.** "The fair-use of XSI-C indices data as it appears on this website is free of charge for internal viewing purposes only." Data via `index@xeneta.com` / Compass; `compassft.com/?s=xsi` lists the same 8, no CSV [D]. | Daily 16:15 London | viewing-only | **Paid for data**; no Africa |
| **SCFI** (Shanghai Shipping Exchange) | `https://en.sse.net.cn/currentIndex?indexName=scfi` (keyless JSON) | `{"data":{"currentDate":"2026-11-06","lastDate":"2026-10-30","lineDataList":[{"lineName_EN":"Comprehensive Index","currentContent":3633.4483,…`; lanes incl. `SCFI_L7 "West Africa 20ft (Lagos)"`, `SCFI_L8 "South Africa 20ft (Durban)"`, `SCFI_L26 "East Africa (Mombasa)"`. **Trap:** every lane's `currentContent` is `null`; lane values served only as an image (`sse.net.cn/index/indexImg?name=scfi&type=english`, 12.6 KB). History `/singleIndex/scfi?date=…` → 500 "cannot be cast to sse.security.bean.SubscribeUser" (subscriber only) [D]. | Weekly Friday | footer © only; no terms page [?] | **Not buildable** (image), and China→Africa direction |
| **CCFI** | `…/currentIndex?indexName=ccfi` | Numeric for all lanes: `CCFI_S12 SOUTH AFRICA 1946.38 (prev 1839.54)`, `CCFI_S11 WEST EAST AFRICA 1190.95 (prev 1134.75)`, composite 1924.69, dated 2026-11-06 [D]. Index points (base 1000), China-export direction. | Weekly; keyless JSON | unstated [?] | Buildable-free, **wrong direction**, index not USD — trend indicator at most |
| Ningbo NCFI | `nbse.net.cn/col/hsslzs/index.html` (200), data on `msri.cn` SPA | 21 routes incl. Africa [A]; no values, no download; guessed API 404 [D]. | Weekly | © only | Dead for our purpose |
| Clarksons SIN | `clarksons.net` → 403 | — | — | — | **Paid** (UNCTAD's Africa charts come from it) |
| Signal Ocean | redirects to `thesignalgroup.com/…` | no pricing, nothing public [D] | — | — | Owner-decision |
| Maersk / Hapag-Lloyd quote tools | `/instant-spot-rates` 404; Quick Quotes 403 | account-gated per-shipment quotes [A] | — | — | Not a series |
| **Nigerian Shippers' Council — Indicative Freight Rates** | `https://shipperscouncil.gov.ng/freight-and-haulage-rates/` (200) → `…/uploads/2026/07/INDICATIVE-FREIGHT-RATES-OF-LADEN-CONTAINERS-OF-MAJOR-TRADING-ROUTES-TO-NIGERIA-1-1-1.pdf` (200, `application/pdf`, 867 KB, text-extractable) | "**April – June, 2026** … EUROPE/MIDDLE EAST — 20FT IMPORT $2455 / **EXPORT $1404**; 40FT IMPORT $3241 / **EXPORT $2004**; 45FT $3855/$2355". Jan–Mar 2026: export 20' $902 / 40' $1,213; Oct–Dec 2025: $723 / $888. Basket ports "London, Liverpool, Dublin, **Rotterdam, Valencia**, Napoli, Marseille, Lisbon, Dubai…" (Europe + Middle East blended). "NOTE: This 'Indicative Freight Rates' serves as a guide to Business Plans as it could be negotiated by Shippers or agents with the respective Shipping Companies." Companion docx gives Q2 composition: base ocean 20' $1,959 / 40' $2,508; OTHC $189/$245; DTHC $153/$209; BAF $383/$755 [D]. `NSC PORT TARIFF.pdf` → 404. | Quarterly, ~1 month after quarter; PDF; filename changes each release → resolve from page (invariant 10) | none stated; government publication [?] | **Buildable-free (PDF parse)** — quarterly, labelled `indicative`, container only, Europe+ME blended. The honest fallback. |
| UNCTAD RMT 2025 ch.3 | `unctad.org/publication/review-maritime-transport-2025` → `rmt2025ch3_en.pdf` (1.5 MB) | Africa content = SCFI Lagos/Durban/Mombasa charts "Source: UNCTAD, based on data from Clarksons Shipping Intelligence Network" [D]. Annual. | Annual PDF | not on page [?] | Context only |
| **IMF PortWatch** | `portwatch.imf.org` (ArcGIS Hub); feature service `services9.arcgis.com/weJ1QsnbMYJlCHdG/…/PortWatch_ports_database/FeatureServer/0/query` | Four attempts over ~5 min → `{"error":{"code":429,"message":"Unable to perform query. Too many requests.","details":["API calls quota exceeded (6585 request units)! maximum allowed request units (6000) per Minute"]}}` → [I] shared global quota, flaky unattended. Gives daily port calls + tonnage *estimates* (Lagos/Tema/Durban), weekly Tuesdays [A]. **No freight rates.** | Weekly; keyless but quota-limited | `imf.org/en/About/copyright-and-terms`: may "publish, distribute, and use Data… with attribution", but "systematic" downloading and "commercial reuse" need written permission [D] | Free, machine-readable, **wrong variable** |

**Freight verdict:** carry the NSC quarterly indicative Lagos→Europe/ME container rate as a labelled, quarterly, `indicative` cost (never a fixture, never a settlement); optionally CCFI South Africa / West-East Africa as a direction-mismatched trend line. A daily WA→Rotterdam number would have to be desk-entered under `data/reference/` or licensed (Baltic/Clarksons/Xeneta) — owner decision under invariant 9. Until then the landed-cost leg is **withheld with reason: freight to Europe is not observed.**

---

## 4. Nigeria FX — official (NFEM) vs parallel

### 4a. CBN official rate — buildable-free, keyless JSON

Old `.asp` URLs are dead (`rates/ExchRateByCurrency.asp` → 404, `rates/exrate.asp` → 404) [D]. Live: `https://www.cbn.gov.ng/rates/exchratebycurrency.html` (200). The page is a Kendo grid fed by keyless JSON (quoted from inline JS [D]): `read: { url: "/api/GetAllExchangeRates" }`, `/api/GetAllNFEM_RatesGRAPH`, `/api/GetAllExchangeRatesGRAPH`, `/api/GetAllMonthlyAvgExchRatesGRAPH`; dropping the suffix gives undocumented `/api/GetAllNFEM_Rates` (200). "Export to Excel" is client-side xlsx.js. Page legend [D]: **"NFEM rate is derived at Volume Weighted Average and stands as the official exchange rate for the day."**

**Endpoint A — `https://www.cbn.gov.ng/api/GetAllExchangeRates`** [D]: `200 application/json; charset=utf-8`, `server: cloudflare`, 8,190,880 bytes, 62,579 rows, all currencies, full history; query params ignored.
```
{"id":65366,"currency":"US DOLLAR","ratedate":"2026-11-05","buyingrate":"1347.7416","centralrate":"1348.2416","sellingrate":"1348.7416"}
```
USD: 6,100 rows, 6,094 distinct dates, 2001-12-10 → 2026-11-05. Traps: 6 duplicate USD dates with *different* values (2024-02-22: selling 1489.3960 vs 1440.6210; 2024-07-02: 1511.8590 vs 1508.5360) → hard-fail, never pick one; currency labels inconsistent (`"US DOLLAR "`, `"EURO\t"`) → strip.

**Endpoint B — `https://www.cbn.gov.ng/api/GetAllNFEM_Rates`** [D]: 200, 135,700 bytes, 483 rows, **2024-12-02 → 2026-11-05** (EFEMS go-live onward). Date format `"November-05-2026"` (parse `%B-%d-%Y`). 6 duplicated dates, all exact duplicates (`noOfDeals` 0) → dedupe by id.
```
ratedate          vwap       close      hi        lo        deals  turnover(US$)
October-23-2026   1361.8394  1370.0000  1395.00   1340.00   609    3,238,998,691.59
October-26-2026   1367.6057  1365.0000  1380.00   1359.00   429    1,253,972,967.41
November-04-2026  1347.7983  1348.0000  1349.50   1346.00   287      434,535,593.40
November-05-2026  1348.7416  1350.4500  1350.50   1348.00   250      380,567,734.35
```
**Which number is "the official rate" [D→I]:** across all 477 overlapping dates, Endpoint A `sellingrate` == NFEM `weightedAvgRate` on 452 (the 25 misses are 2024-12/2025-01 two-decimal rounding); `centralrate = VWAP − 0.50`, `buyingrate = VWAP − 1.00`. So the CBN "official USD rate" **is the NFEM VWAP**, and the buy/central/sell triplet is a cosmetic ±0.5 spread, not a market quote. The NFEM *closing* rate is a different number (last-trades VWAP per FMDQ methodology) and lives only in Endpoint B.

Cadence [D]: one row per business day; the 11-05 row was present at 10:05 UTC on 11-06 (D−1 by morning). EFEMS FAQ (`Out/2024/FMD/…EFEMS .pdf`) Q19: "Real-time rate and other key data from EFEMS will be published on the selected platform as well as the CBN official websites … The rates would serve as reference rate for USDNGN." Revised NFEM Guidelines (29 Nov 2024) PDF not located on cbn.gov.ng [?].

Licence [?]: **no terms-of-use, disclaimer or copyright text found** anywhere on cbn.gov.ng (`/termsofuse.html`, `/Disclaimer.html`, `/AboutCBN/terms.html` all 404). Treat as public central-bank statistics with attribution — an owner stance, not a proven licence.

### 4b. FMDQ — paid; not needed

`fmdqgroup.com/exchange/` 200; no machine-readable endpoint; data behind the "e-Markets Portal" [D]: "Subscribe to FMDQ Exchange's datasets via access to e-Markets Portal in three (3) simple steps: 1. Complete/Execute the FMDQ e-Markets Portal Subscription Order Form and Agreement. 2. Pay the relevant subscription fee … Data Redistribution: … available for redistribution upon obtaining a licence from FMDQ Exchange." 2026 Fees & Dues Framework PDF [D]: redistribution licence "Closing Prices/Rates 13,437,500.00 NGN / 13,437.50 USD", "FMDQ FX Data 6,718,750.00 / 6,718.75"; DQL subscription 580,500 (member) / 645,000 NGN. Terms of use (`fmdqgroup.com/terms-of-use/`): "may not be copied, reproduced, transmitted, displayed … distributed … without the prior written consent of FMDQ Group". The free, useful document is the **FMDQ FX Spot Rates Methodology (Dec 2024)** [D]: window 09:00–16:00, publication 17:00; §5.2.1 "Level I – Volume Weighted Average Price ('VWAP') of the rates for the last trades, up to ten (10) (subject to a minimum of two (2) trades)" → this is CBN's `closingrate` [I]. CBN republishes the same NFEM numbers free, so FMDQ is not needed.

### 4c. Parallel market — not feedable publicly

| Source | Observed [D] | Verdict |
|---|---|---|
| **abokifx.com** (200) | Live, © 2026, rates paywalled: "Subscribe to abokiFX to see rates" (£1/wk, £25/yr, **API £100/yr**, "Bloomberg Ticker ABOX"). Disclaimer: "You agree not to copy, modify, reformat, download, store, reproduce, reprocess, transmit or redistribute any data … or use any such data or information in a commercial enterprise without obtaining prior written consent." | Paid + no-redistribution → owner-decision, desk-only at most |
| **ABCON** `abconng.org` (200; `abcon.org.ng` NXDOMAIN) | Home page carries no rates. | Not a rate source |
| monierate.com/fx/parallel (200) | "1 USD = ₦1,425 Buy ₦1,425 · Sell ₦1,410 as of 6 November 2026 … updated up to three times a day"; provenance unstated. | Secondary, scrape-only |
| abokiforex.app (200) | "₦1,410 black market · ₦1,348.74 CBN official rate"; "average black market rates collected from multiple dealers … Lagos, Abuja, Kano, and Port Harcourt"; "not connected to AbokiFx". Advertises a Rates API [?]. | Secondary |
| nairarates.com | SSL handshake failure | Dead |

No primary parallel series exists. Aggregators disagree by ₦15–35 on the same morning (1,390 / 1,400 / 1,410 / 1,425). The only official BDC-adjacent number is `bdcDollar` in `GetAllMonthlyAvgExchRatesGRAPH` (e.g. `"period":"July 2020","ifemDollar":"377.19","bdcDollar":"464.71"`) — monthly; latest populated month not checked [?].

### 4d. What Yahoo `NGNUSD=X` (Layer 7) actually is

Yahoo help SLN2310 [D]: "Global | Currency Rates | =X | Real-time | ICE Data Services" and "must not redistribute information displayed on or provided by Yahoo Finance." Chart meta: `exchangeName: 'CCY', instrumentType: 'CURRENCY', exchangeTimezoneName: 'Europe/London'`.

USD/NGN comparison (Yahoo from `USDNGN=X`; 1/`NGNUSD=X` agrees to 0.01) [D]:

| date | CBN sell = NFEM VWAP | NFEM close | NFEM hi/lo | Yahoo close | Yahoo hi/lo | Yahoo − VWAP |
|---|---|---|---|---|---|---|
| 10-26 | 1367.61 | 1365.00 | 1380 / 1359 | **1331.56** | 1352.38 / 1339.29 | **−36.05** |
| 10-27 | 1352.87 | 1357.00 | 1368.5 / 1344 | 1339.99 | 1347.00 / 1337.68 | −12.88 |
| 10-28 | 1347.53 | 1349.00 | 1363.5 / 1342 | 1340.16 | 1348.02 / 1340.16 | −7.37 |
| 10-29 | 1346.91 | 1346.00 | 1363.5 / 1343.5 | 1344.66 | 1346.93 / 1343.66 | −2.25 |
| 10-30 | 1346.26 | 1346.00 | 1363.5 / 1344.5 | 1346.21 | 1346.65 / 1345.59 | −0.05 |
| 11-02 | 1347.94 | 1346.01 | 1349.5 / 1346 | 1345.97 | 1344.48 / 1340.43 (close > high) | −1.97 |
| 11-03 | 1346.93 | 1347.75 | 1349.5 / 1344 | 1343.83 | 1344.87 / 1340.42 | −3.10 |
| 11-04 | 1347.80 | 1348.00 | 1349.5 / 1346 | 1343.45 | 1346.09 / 1340.66 | −4.35 |
| 11-05 | 1348.74 | 1350.45 | 1350.5 / 1348 | 1345.92 | 1346.41 / 1343.79 | −2.82 |

Parallel the same week: 1,384–1,425 [D/A]. **[I] Yahoo `NGNUSD=X` is an ICE-contributed offshore interbank indicative, not the NFEM official rate and not the street rate.** It sits near the official rate in calm weeks and decoupled by ₦36 on the 10-22/23 EFEMS spike (NFEM hi 1,395); the close-outside-range defect (#447) recurs (11-02). It should be replaced by the CBN NFEM series for the AFEX NGN→USD conversion; Yahoo's redistribution ban is a second reason.

### 4e. Alternatives checked

| Source | Observed [D] | Verdict |
|---|---|---|
| World Bank `PA.NUS.FCRF` (keyless JSON) | 200; **annual only**: 2025 1518.38, 2024 1478.97, 2023 645.19 | Useless for daily |
| IMF Representative Rates (`rms_mth.aspx…&tsvflag=Y`) | curl → 403 Akamai; naira not in the "selected currencies" table | Not available |
| `open.er-api.com/v6/latest/USD` (keyless) | 200; `NGN 1348.023447`, updated `Fri, 06 Nov 2026 00:02:31`, once/day; terms "this license does not permit re-distribution of our data" | Re-serves the official rate ±1; redistribution barred |
| exchangerate.host | 401 `missing_access_key` | Keyed/paid |
| Open Exchange Rates | `app_id` required; "blended together" from unnamed providers | Keyed, provenance opaque |
| ECB | no NGN | n/a |

---

## 5. What this means for the product

1. **Build (small):** a CBN NFEM FX layer — `GET /api/GetAllNFEM_Rates` for Dec-2024→, `GetAllExchangeRates` filtered `currency.strip()=="US DOLLAR"` / `sellingrate` for pre-2024 backfill (hard-fail on the 6 conflicting duplicate dates). Fields: `weightedAvgRate` (= official rate), `closingrate`, `highestrate`, `lowestrate`, `nfemTotalTurnover`, `noOfDeals`. Unit NGN per USD. Fixed URL → `LAYER_MAX_DATA_AGE_DAYS` budget (invariant 10, other side). Label "NFEM VWAP (CBN official)"; not a settlement (invariant 3). Then route the AFEX conversion through it via `align_fx` (invariant 7) and retire Yahoo `NGNUSD=X` for NGN. Licence stance: public central-bank statistics with attribution — owner to accept explicitly, since no terms page exists.
2. **Build (small, store-only until owner says publish):** NEPC LICPL monthly PDF → state soya ₦/100 kg + Lagos soymeal ₦/MT. Resolve the current PDF from `nepc.gov.ng/indicative-market-prices/` each run. NEPC's terms bar republication; same gate shape as `AFEX_PUBLISH_RAW`.
3. **Build (annual, cheap):** FAOSTAT bulk zip (CC BY 4.0) as the publishable export-volume series for the ten origins, flags carried through (`A`/`I`/`X` never collapsed). Comtrade preview as the desk-side partner breakdown + EU/France mirror — store reporter and mirror side by side, never net; publishing more than "a small amount" needs UNSD permission (owner).
4. **Reference, not a layer:** Nigeria export rules — no soya on the readable prohibition list; NESS 0.5% FOB; NXP 90/90 days; CCI mandatory; NEPC ₦13,500. One HITL item: read the 2026 variation order on `customs.gov.ng` from a browser that clears Cloudflare and confirm the revised export list.
5. **Withhold with reason:** WA→Europe freight. The NSC quarterly indicative container rate is the only primary print and could be carried as a labelled `indicative` quarterly cost; a bulk or daily number does not exist free.

## 6. Could not verify [?]

- `customs.gov.ng` content (Cloudflare 403 via curl, WebFetch and Playwright) — the 2026 Revised Export Prohibition List text.
- CBN 29-Oct-2026 NESS circular PDF; Revised NFEM Guidelines (Nov 2024) PDF; any CBN terms-of-use page (none exists at the guessed paths).
- TRMS/NXP portal hosts (DNS dead from here); NBS main host (`nigerianstat.gov.ng` timed out three times) — NBS Food Price Tracking CSV not re-checked; NBS quarterly Foreign Trade PDF soybean rows.
- Baltic Exchange route definitions and terms (Akamai wall) — route list above is secondary.
- SCFI / CCFI / NSC / UNCTAD redistribution terms (none published).
- Comtrade EU-27 2025 Nigeria 2304 = 2,464 t — partial year vs real collapse.
- SARS trade portal download flow (ViewState POST untested).
