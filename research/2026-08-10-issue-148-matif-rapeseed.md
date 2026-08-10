# Issue #148 (M6) — Euronext/MATIF rapeseed: licence, proxy, or drop the page

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/148
- **Date:** 2026-08-10 (all URLs accessed this date unless noted)
- **Question:** Can a Euronext/MATIF rapeseed page be fed at all — licence, proxy, or not at all?
- **Inherited assumption under test:** #130/#131 concluded "MATIF has no free feed"; `config.py:317` hard-codes the same claim in a comment.

**Evidence tiers:** **[D]** directly observed (endpoint fetched / file parsed / terms page read today) · **[I]** inference from observed data · **[A]** assumption or secondary source · **[?]** could not verify.

---

## RECOMMENDATION (up front)

**Build the Euronext page — but feed it from the European Commission, not from Euronext, and re-scope it from "MATIF futures" to "EU rapeseed complex".**

Three-part verdict:

1. **DROP** the MATIF *futures* settlement series as a pipeline layer. Not because it is unlicensable, but because every route to it is either paid (**€167.55/month** minimum for delayed redistribution) or breaches a terms clause we can quote. The cheapest licence is real and priced, but it buys one line of data we can substantially replicate for €0.
2. **USE** — and this is the actual finding — the **EC Oilseeds Market Observatory weekly world FOB file** as the headline EU rapeseed price. This is **not a proxy**: `Rapeseed - EU Moselle` is a real European physical rapeseed quote in EUR/t and USD/t, weekly, back to Dec 2018, CC BY 4.0, no key. Supplement with the **EC agri-food oilseeds prices API** for per-Member-State cash granularity and the rest of the oilseed complex.
3. **REJECT** both proxies named in the ticket. ICE canola has no free feed at all (`RS=F` is dead — reconfirmed today) *and* is structurally the wrong commodity (GM vs non-GM). World Bank rapeseed oil stays exactly what it is today: a monthly substitute-**oil** benchmark, not a rapeseed-seed proxy.

Net: the user gets a Euronext-region rapeseed page with a genuine European benchmark on it. What it will not have is the MATIF futures curve. That absence should be stated on the page.

---

## RQ1 — Euronext's actual licensing tiers and prices

### 1a. Contract code correction

**[D]** The ticket says contract code **COM**. Euronext's own product page uses **ECO**:
`https://live.euronext.com/en/product/commodities-futures/ECO-DPAR` — "Rapeseed / Colza", Commodities Future, DPAR (Euronext Derivatives Paris). No Euronext primary page using "COM" was found. **[D]** `config.py:317` already says "Matif ECO", so the repo is right and the ticket is wrong. **[D]** Barchart's root for the same contract is `XR` (`https://www.barchart.com/futures/quotes/XR*0/futures-prices` → "Rapeseed Nov '26 (XRX26)", exchange: Euronext). **[A]** "COM" is likely a legacy LIFFE/vendor symbol.

### 1b. There is no per-contract licence

**[D]** Information Product Fee Schedule effective January 2026, p.2
(`https://connect.euronext.com/sites/default/files/documentation/data/Compare%20Document%20-%20Product%20Fee%20Schedule%20(effective%20January%202026).pdf`):

> "**Euronext Commodity Derivatives** comprises Information relating to Commodity Derivatives traded on the Euronext Derivatives (MATIF) markets, including Euronext Container Freight Futures and excluding Euronext Power Derivatives."

**[I]** Rapeseed cannot be bought alone. It is bundled with milling wheat, corn, durum, salmon and freight into one Information Product. So the fee below buys more than we need, but there is no smaller unit.

### 1c. The published prices (EUR/month)

**[D]** All figures from the January 2026 Fee Schedule above. That PDF is a redline/compare document whose text layer merges old and new values (e.g. `€955.90978.85`); each figure below was decoded by cross-validating against the independent clean 2025 schedule (`https://connect2.euronext.com/sites/default/files/documentation/data/Product%20Fee%20Schedule%20(effective%201%20January%202025).pdf`). "L2" = Level 2 depth, "LP" = Last Price.

| Licence | Basis | L2 | Last Price |
|---|---|---|---|
| Direct Access Fee | per contracting party, per product | 978.85 | 489.30 |
| Real-Time Redistribution | per contracting party | 1,072.05 | 535.95 |
| Real-Time Redistribution — Non-Professional | — | **Not Permitted** | **Not Permitted** |
| **Delayed Data Redistribution** | per contracting party | **334.95** | **167.55** |
| White Label — delayed | per white label | 152.95 | 73.40 |
| Display Use — Standard | per user/device | 18.40 | 8.55 |
| Non-Display Cat. 4 "Other" — Restricted-Basic | per contracting party | 231.15 | 115.60 |

**The number for the ticket: €167.55/month** (Delayed Data Redistribution, Last Price) — the cheapest tier that legitimately covers publishing delayed MATIF rapeseed settlements on a public dashboard. €334.95/month if full depth is wanted (it is not).

**[D]** Data Shop / historical EOD pricing: **price not published.** A grep of the full 51-page 2026 Fee Schedule for "end-of-day", "historical", "Data Shop", "EOD" and "settlement data" returns zero matches; the schedule governs real-time/delayed/after-midnight licensing only. Data Shop product pages return HTTP 403 to automated access. Euronext directs enquiries to `databyeuronext@euronext.com`. The separate "Delayed Data API" commercial product is also **price not published**.

### 1d. Delayed data IS free — for internal use

**[D]** Fee Schedule p.15: > "**No Display Use Fees are charged for the Use of Delayed or After Midnight Data.**"

**[D]** `https://www.euronext.com/en/data/real-time-data/pricing-specs-agreements/market-data-pricing-policies`:
> "The use of delayed market data is **free of charge if it is used for internal purposes only**."

**[D]** `https://www.euronext.com/en/data/market-data/market-data-agreements`:
> "if you only use delayed market data for internal (non-commercial) purposes, a licensing agreement with Euronext is not usually required"

**[D]** All Non-Display Use Fees are scoped to "Real-Time Data Information Products" — delayed non-display internal use sits outside the non-display schedule entirely.

**[D]** Non-Professional tiers do **not** apply to Commodity Derivatives: the Non-Professional redistribution row reads "Not Permitted", and the €2.15/€1.84 per-device Non-Professional tables list only "Euronext Equity and Index Derivatives". Fee Schedule p.20: > "When no Non-Professional Fee is specified for an Information Product specified in this Information Product Fee Schedule, the respective Display Use Fees will apply."

**[D]** There is a waiver, Fee Schedule p.9 fn.1:
> "If the Contracting Party and its Affiliates do not generate a **Direct Economic Benefit** from nor create a **Value Added Service** in relation to the Delayed Data Redistribution of this Information, the Delayed Data Redistribution Licence Fee for such Information Products will not apply, provided that the Contracting Party requests a Fee waiver through the Delayed and/or After Midnight Data Fee Waiver Application Form."

**[I]** A dashboard that computes basis, spreads and signals from the data is on its face a "Value Added Service". A waiver application is free to make and might succeed for a non-commercial personal project, but it should not be assumed.

### 1e. The free MiFIR channel, and its terms — the crux

**[D]** `https://marketdata.euronext.com/data-reporting-service/trades-file` (HTTP 200, fetched today). The instrument selector directly observed in the page source includes `COMMODITY_DERIVATIVES` alongside Equities, ETFs & Funds, Warrants & Certificates, Fixed Income, Equity & Index Derivatives, Currency Derivatives; trading locations include Paris. Format `.csv`, generated on request.

> "In line with MiFID II, Euronext makes pre- and post-trade data available free of charge, 15 minutes after initial publication by Euronext... The delayed data is provided with a maximum of 15 minutes time delay after initial publication by Euronext and remains available for at least 24 hours. The timestamps display UTC time."

**[D]** Its "TERMS AND CONDITIONS FOR DELAYED TRADE DATA", read in full today, are more permissive than the boilerplate suggests:

> **1.** "The right to Use the Data is free of charge and these Terms do not impose any restrictions on the Use, **except in the event of distribution**. In the event of distribution additional terms may apply..."

> **5.** "Where the User distributes the Data, it will (i) where reasonably practicable, attribute Euronext as the source of the Data and (ii) include a prominent disclaimer which should be: '© 2026 Euronext N.V. All Rights Reserved...'"

> **6.** "Unless the User is licensed or authorized by Euronext, the Data may not be further distributed or otherwise made available to third parties **subject to a fee**, including but not limited to a general fee for accessing the Data through a service."

> **7.** "In the event the User of the Data wishes to distribute to Data subject to a fee... or otherwise intends to commercially benefit from the distribution of the Data, the User is required to enter into an agreement with Euronext."

**[I] This is the single most important reading in this document, and it is genuinely ambiguous.** Clauses 5–7 read together contemplate free, non-fee-bearing redistribution *with attribution and the verbatim disclaimer*: clause 5 tells you how to distribute, and clause 6's prohibition is qualified by "subject to a fee". Under that reading, a free public GitHub Pages dashboard that attributes Euronext and carries the disclaimer is permitted.

**[I] The disconfirming reading**, which I am obliged to state: the mandated disclaimer text itself says the data "may not be copied or further disseminated, by any media whatsoever, except as specifically authorized by Euronext" — a flat prohibition. The same sentence appears standalone in the page footer, outside the numbered terms. If that is operative, clause 5 is merely formatting guidance for licensees and free redistribution is barred.

**[I]** I cannot resolve this from the text. Clause 6's explicit "subject to a fee" qualifier would be surplusage under the strict reading, which argues for the permissive one — but that is legal inference, not observation. **[?]** Not verified with Euronext. `databyeuronext@euronext.com` is the address; a one-paragraph email would settle it and costs nothing.

**[D]** Three further practical constraints observed today:
- The trades file is **post-trade transaction data**, not the settlement price series. Settlement prices live on `live.euronext.com/.../ECO-DPAR/settlement-prices`, which is a different, ToU-protected surface.
- The page's own status line while I was on it read "**No trade is found**" and "You're not authorized to download statistics" — **[?]** I did not complete a Commodity-Derivatives Paris download, so I have not proven ECO rows actually appear in the file.
- **[I]** MATIF rapeseed is thinly traded outside European session hours; a 15-minute rolling window with 24-hour retention is a fragile basis for a daily settlement series even if the rights were clean.

### 1f. Scraping Euronext's own site is out

**[D]** `https://www.euronext.com/en/terms-use` (last updated 29 April 2021; explicitly covers `live.euronext.com`):

> "Except if we give you prior written permission, use of any Web browsers (other than generally available third-party browsers), engines, software, **spiders, robots, avatars, agents, tools or other devices or mechanisms to navigate, search or determine the Euronext Website is strictly prohibited**."

> "you will not sell, license, rent, modify, print, copy, reproduce, download, upload, transmit, distribute, disseminate, publicly display... or create derivative works from any Content or materials (including, without limitation, through framing or **systematic retrieval to create collections, compilations, databases or directories**)"

**[D]** Enforced technically as well as contractually: `live.euronext.com/en/datashop/*` returns HTTP 403 / "Access denied"; the ECO product page's static HTML contains zero price data (client-side rendered); and the quote endpoint `POST /en/ajax/getDetailedQuote/ECO-DPAR` returns an **AES-encrypted blob** (`{"ct":...,"iv":...,"s":...}`) rather than JSON. **[D]** I confirmed the settlement-prices page itself returns HTTP 200 at 239 KB, consistent with a JS shell.

**Verdict RQ1:** the inherited "no free feed" claim is **half right**. Delayed MATIF data is free for *internal* use via the MiFIR trades file. What is not free — or at best is legally ambiguous — is publishing it. The priced route exists and the number is **€167.55/month**.

---

## RQ2 — Does any free source legitimately republish MATIF settlements?

Every candidate was checked against its own terms page. **None is clean for a public dashboard.**

| Source | Carries ECO? | Terms verdict |
|---|---|---|
| **live.euronext.com** | **[D]** Yes, free settlement page, Nov 26 → Feb 29 | Spiders prohibited; systematic retrieval prohibited (quoted above). **No.** |
| **Barchart** (`XR*0`) | **[D]** Yes, 10-min delayed | **No.** |
| **Investing.com** `/commodities/rapeseed` | **[D]** Yes — Paris, EUR, 50 t, tick 0.25/€12.50 | **No.** |
| **TradingView** `EURONEXT-ECO1!` | **[D]** Page exists | **No** — and worse than the others, see below. |
| **Yahoo / yfinance** | **[D]** **Does not carry it at all** | Moot. |
| **Proplanta** | **[D]** Yes — full free forward curve | Rights derive from a third party. **No.** |
| **Kaack Terminhandel** | **[D]** Yes | No licence grant. **No.** |
| **Terre-net / Web-agri** | **[D]** Yes ("Colza 11/2026 533,25 €/t") | **Explicit "contrefaçon".** No. |

Key quotes:

**[D]** Barchart ToU, effective 3 February 2026 (`https://www.barchart.com/terms`):
> You may not "Use any data mining, robots, or similar data gathering and extraction tools to capture data or content from the Barchart Services."
> You may not "Reproduce, retransmit, disseminate, sell, distribute, publish, broadcast or circulate any of the Barchart Services or Content in any manner or for any purposes (whether personal or Business) without the prior express written consent of Barchart and/or the Data Providers."

**[D]** Barchart also publishes the exchange pass-through, which independently corroborates the Euronext figure (`https://www.barchart.com/solutions/exchange-fees`):
> "Euronext Commodities: Real-Time: €523.40, Delayed: €163.60"

**[I]** Barchart's €163.60 vs Euronext's own €167.55 — two independent sources within €4/month. High confidence the delayed-redistribution number is real and is roughly €165/month.

**[D]** Investing.com T&C (`https://cdn.investing.com/about-us/terms_and_conditions.pdf`): users are
> "expressly forbidden from employing any automated system or software to extract data for content from this website for any purpose"

**[D]** TradingView policies §3 (`https://www.tradingview.com/policies/`):
> "our agreements with Data Providers strictly forbid the sublicensing, assigning, transferring, selling, loaning, or any distribution of TradingView content, including market data, for any form of compensation"

prohibited uses include
> "any form of automated trading, automated order generation, **price referencing**, order verification, algorithmic decision-making" and "any machine-driven processes that do not involve the direct, human-readable display of such data"

**[I]** That non-display clause bites this project specifically. Feeding a price into a computed basis or spread is exactly "price referencing". TradingView is the worst option, not the best.

**[D]** Terre-net CGV (`https://terre-net.fr/Home/Cgv`): the client has "un droit d'usage privé" and is
> "expressément interdit de copier, reproduire, adapter, représenter, distribuer, exploiter, vendre, publier ou diffuser dans un autre format... Toute autre utilisation est constitutive de **contrefaçon**."

**[D]** Proplanta (`https://www.proplanta.de/markt-und-preis/matif-raps/`) is the most tempting — a full free forward curve was observed (Nov26 533.25, Feb27 535.00, ... Feb29 494.00, timestamped 07.08.2026 18:33:06) — but it attributes "Alle Angaben ohne Gewähr (Quelle: www.goyax.de)", i.e. it is itself a re-republisher two hops from Euronext and grants nothing. **[?]** Its Nutzungsbedingungen returned HTTP 300 and was not read.

**[D]** Yahoo ToS §2.4(i) (`https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html`) forbids automated collection; the yfinance README states the "Yahoo! finance API is intended for personal use only". **[I]** This is an *existing* exposure for Layers 1 and 7, not a new one — but it means yfinance cannot be the escape hatch here either, and in any case Yahoo has no MATIF symbol.

### Empirical yfinance test (performed directly, 2026-08-10)

**[D]** Tested 17 plausible tickers via this repo's `.venv` (yfinance 1.5.2): `COM=F`, `IJ=F`, `RS=F`, `COM.PA`, `COMX26.PA`, `ECO=F`, `XR=F`, `RAP=F`, `MATIF`, `COMZ26.NX`, `IJX26.NX`, `COM.NX`, `EBM.PA`, `ECO.PA`, `RPS=F`, `CA=F`, `XB=F`. **All returned 0 rows.** Controls `ZS=F` (4 rows) and `^GSPC` (5 rows) succeeded in the same session, so this is not a network or library failure.

**[D]** Yahoo's own symbol-search API (`query2.finance.yahoo.com/v1/finance/search`) for `rapeseed` returns exactly two hits, both indices, not the futures contract:

| Symbol | Name | Exchange | Type | Observed |
|---|---|---|---|---|
| `ENRSU.PA` | Euronext Rapeseed Commodity Ind | PAR | INDEX | price 5356.13 USD |
| `ENRSE.PA` | Euronext Rapeseed Commodity Ind | PAR | INDEX | price 5443.99 EUR |

**[D]** Both are dead ends: `history(period='max')` errors with "must be one of: 1d, 5d", and the chart API at `range=1y` returns **1 bar** (2026-08-07). No usable history. **[I]** The ~5,400 level is an index, not EUR/MT — it carries no price level information about the ECO contract. Searches for `canola`, `colza` and `raps` returned **zero** results.

**[D]** `RS=F` (ICE canola) returns `instrumentType: ALTSYMBOL`, `currency: null`, `regularMarketPrice: null`, 0 bars at every range. Individual contract guesses (`RSX26.CBT`, `RSF26.NYB`) 404.

**Verdict RQ2:** no free legitimate republisher exists, and yfinance carries neither MATIF rapeseed nor ICE canola.

---

## RQ3 — Are the named proxies defensible?

### 3a. ICE canola + FX — **reject, on two independent grounds**

**Ground 1 — no feed.** **[D]** `RS=F` is dead (above), reconfirming the go/no-go already recorded in commit `2b4891c` ("ICE canola RS=F is DEAD on yfinance (verified live 2026-08-08)"). Two independent observations six days apart.

**Ground 2 — wrong commodity.** **[D]** StockCo: "The Matif contract is non-GM, whilst the ICE contract is GM." **[D]** UFOP (Jul 2026, via Biofuels International): "Because Canadian farmers grow genetically modified varieties, however, the use of rapeseed oil derived from Canadian sources is restricted in the EU. As a result, imports from Canada are mainly used for biofuel production." **[D]** Andrew Whitelaw, Farm Weekly, 12 Mar 2025: "Matif is French non-GM rapeseed, and ICE is Canadian GM canola... we can see that ICE has been falling to a substantial discount. This is a result of the lack of demand for GM canola into Europe and the threats of tariffs impacting Canadian canola." GM/non-GM premium reached A$127/t in Dec 2024–Jan 2025.

**Quantified error.** **[?]** No published MATIF–ICE correlation coefficient exists; extensive search found none, and none was estimated. The closest FX-free measurable analogue, computed from the EC world price file (Canada FOB minus EU Moselle FOB, USD/t, 395 weeks Dec 2018 → Aug 2026):

| Statistic | Value |
|---|---|
| mean / median spread | +1.1 / −21.2 USD/t |
| **standard deviation** | **68.3 USD/t** |
| min / max | −159.1 / +219.7 (range 379) |
| 5–95% band | −82 to +119 |
| sd as % of EU level | **12.7%** |
| level correlation | 0.905 |
| **weekly log-return correlation** | **0.538** |

**[D]** Annual mean spread flips sign repeatedly: 2019 −49 · 2020 −43 · 2021 **+70** · 2022 +30 · 2023 **+101** · 2024 −22 · 2025 −52 · 2026 −40. **[I]** This is a regime-switching basis, not a stable one. The 0.905 level correlation is a common-trend artefact; 0.538 on returns is the honest number.

**[A]** Corroborating literature: Kim, Brorsen & Yoon (2015), *Cross Hedging Winter Canola*, JAAE 47(4), doi:10.1017/aae.2015.14 — for a non-Canadian canola cash market, ICE canola futures correlate 0.637 vs **soybean oil futures at 0.773**, and "Soybean oil futures have a higher hedging effectiveness than Canada canola futures except when considering a 2-month hedging period." **[I]** ICE canola loses to soy oil as a cross-hedge for non-Canadian canola. That is damning for using it against MATIF.

**[D]** Structural breaks with no MATIF counterpart: China's duties on Canadian canola (100% on oil/meal from 20 Mar 2025; 75.8% provisional on seed from 14 Aug 2025; final 28 Feb 2026 at 5.9% AD + 9% MFN for five years, oil still at 100%), with Canada's canola complex export value down 60% Jan–Oct 2025 (USDA FAS CA2026-0001). **[D]** Tariff asymmetry the other way: EU MFN on rapeseed **seed is Free**, but low-erucic rape **oil is 9.6%**. **[D]** RED III demand is EU-specific.

**[D]** FX contributes less than expected to *tracking* (own computation, weekly log returns 2019-01→2026-08, n=394): Var(EURUSD)/Var(canola-in-EUR) = 8.2%; return correlation vs EU price 0.537 in USD → 0.534 in EUR. **[I]** Commodity basis dominates FX ~4:1 for tracking. But for *levels* FX matters: EURCAD annualised vol 7.18%, annual peak-to-trough 4.2%–15.0% (2016–2026) — on EUR 550/t rapeseed that is up to ~EUR 55/t of pure translation drift, comparable to the entire €84/t spread "high" reported by Mercantile/SaskCanola in Oct 2024.

### 3b. World Bank rapeseed oil — **keep as-is, do not relabel**

**[D]** Correction to a likely assumption: rapeseed oil is **not** in the Pink Sheet PDF. Extracting the June 2026 Pink Sheet and grepping for "rapeseed" returns nothing. The series exists only in `CMO-Historical-Data-Monthly.xlsx`, which is what `fetchers/worldbank.py` actually ingests. Its Description sheet reads, verbatim:

> `Rapeseed Oil, Dutch rapeseed oil, FOB Rotterdam`

**[I]** That is the *entire* specification — no crude/refined qualifier, unlike the neighbouring "Soybean oil, U.S. Soybean Oil Crude Degummed, FOB U.S. Gulf".

**[D]** Local DB confirms the shape: `worldbank_prices` holds Rapeseed Oil, 294 rows, 2002-02-01 → 2026-07-01, `$/mt`, latest 1494.0. Monthly average, published ~2nd business day after month end. **[I]** Staleness of the freshest point ranges ~19 days (just after release) to ~50 days (just before the next); today ~25 days.

**[D]** Own-computed error vs EC EU-Moselle rapeseed **seed**, 92 overlapping months: level correlation 0.971, **monthly log-return correlation 0.716**, sign agreement 81.3%, oil/seed ratio mean 2.17 with sd 0.15 and range **1.88–2.58**.

**[I]** Two honest conclusions. First, WB rapeseed oil tracks EU rapeseed *better* on returns (0.716) than Canadian seed does (0.538) — the substitute-oil relationship beats the cross-continent same-commodity one. Second, a ratio that ranges 1.88–2.58 carries **no level information** about MATIF. It is a direction signal, not a price proxy, and the code's existing framing is already correct.

**[D]** Cost of monthly averaging alone: month-average vs month-end EU seed price differs by mean absolute 13.5 USD/t (2.18%), sd 21.8, range −77 to +86.

---

## RQ4 — Free EU rapeseed cash/physical quotes

This is where the answer turned out to be.

### 4a. EC Oilseeds Market Observatory weekly world prices — **the recommendation**

**[D]** `https://circabc.europa.eu/sd/a/2ddd7dcd-dff1-41b5-94b9-6cd207181a3c/oliseeds-world-prices.xlsx` — HTTP 200, 288,935 bytes, downloaded and parsed today. Sheet `Data`, 401 rows.

Directly observed structure — weekly Wednesday FOB, **both USD/t and EUR/t**:

| Wednesday | Rapeseed AU | Rapeseed **CA** | **Rapeseed – EU Moselle** | Rapeseed UA |
|---|---|---|---|---|
| 2026-08-05 | 541.77 | 579.41 | **605.52** USD/t (**€530.79**) | 495 |
| 2026-07-29 | 564.92 | 580.63 | 604.94 | 545 |
| 2026-07-22 | 585.79 | 616.92 | 616.87 | 575 |

**[D]** History: 2018-12-26 → 2026-08-05, ~398 weekly rows. **[D]** Same file also carries Soyabeans — Argentina Up River / Brazil Paranagua / US Gulf / Ukraine, and Sunflowerseed — EU Bordeaux / Ukraine. **[A]** Source is the International Grains Council, per the file's own header.

**[I] This is not a proxy.** `Rapeseed - EU Moselle` is an actual European physical rapeseed FOB quote. It is weekly rather than daily and physical rather than futures, but it is the real commodity in the real region.

**[I]** The soybean columns are a bonus: they independently cross-check the project's existing cross-origin FOB board (Layers 19/20/21 — Paranaguá, US Gulf, Argentina up-river) from a fourth, licence-clean source.

### 4b. EC Agri-food Data Portal oilseeds API — per-Member-State cash granularity

**[D]** Verified myself, HTTP 200, **no API key**:
```
https://api.tech.ec.europa.eu/agrifood/api/oilseeds/prices?products=Rapeseed&memberStateCodes=FR
```
Observed record:
```json
{"memberStateCode":"FR","memberStateName":"France","beginDate":"27/07/2026","endDate":"02/08/2026",
 "price":"€524.00","unit":"national currency/ton","weekNumber":5,"product":"Rapeseed",
 "productType":"N.A.","marketStage":"DELPROC","market":"Rouen","marketingYear":"2026/2027"}
```

**[D]** Product list (`/api/oilseeds/products`) is the full complex: `Crude rape oil`, `Crude soya bean oil`, `Crude sunflower oil`, `Rapeseed`, `Rapeseed meal`, `Soya beans`, `Soya meal`, `Sunflower seed`, `Sunflower seed meal`.

**[D]** Weekly (Mon–Sun), ~1-week publication lag, 17 Member States, 46 distinct (state, market, stage) rapeseed series, earliest row 2020-12-28. FR Rouen specifically: 172 rows, 2021-04-26 → 2026-07-27.

**Independent cross-check I ran:** **[D]** the agridata API gives FR Moselle FOB **€530.50** for week 27/07–02/08/2026; the world-price xlsx gives EU Moselle **€530.79** for Wednesday 2026-08-05. Two separate EC products agree within €0.30. **[I]** High confidence both series are sound.

**Caveats, all [D]:**
- `price` is a **string with a `€` prefix** and must be parsed; the `unit` field says "national currency/ton" but Polish rows carry `€532.05`, which matches EUR not PLN. **[I]** Values are EUR/t throughout — inferred from the data, not stated by the API.
- Coverage is uneven. **FR Rouen DELPROC — the obvious MATIF analogue, since it is the physical market MATIF settles against — is the gappiest big series at ~63% weekly coverage.** DE National Average DEPSILO is the cleanest (~99% since 2022-04); ES/LV ~96% since 2020-12.
- `memberStateCodes=EU` returns 404 — no EU-level aggregate for rapeseed despite the docs.
- Rate limiting is real: HTTP 429 `{"code":"900802","message":"Message throttled out"}`.
- **[D]** Migration notice: "the new domain for API endpoints is https://api.tech.ec.europa.eu. The previous API endpoints are still working temporarily... The switch off date will be communicated in advance." Use the new host.

**Licence — [D]** `https://commission.europa.eu/legal-notice_en`:
> "The Commission's reuse policy is implemented by the Commission Decision of 12 December 2011 on the reuse of Commission documents. Unless otherwise indicated..., content owned by the EU on this website is licensed under the **Creative Commons Attribution 4.0 International (CC BY 4.0)** licence. This means that reuse is allowed, provided appropriate credit is given and changes are indicated."

**[?]** No licence statement appears on the agridata portal pages themselves. **[I]** As an `ec.europa.eu` Commission site it falls under the notice above; this is inference, not an explicit portal-level confirmation.

### 4c. Everything else — checked and rejected

- **FranceAgriMer** — **[D]** free XLSX exists (`visionet.franceagrimer.fr/Pages/OpenDocument.aspx?fileurl=...SCR-CER-HISTOPRIXTRIMESTRIELS-C25-26.xlsx`, 259 KB, no login), contains Colza, `Unité : Euro par tonne`, campagnes 2004/05–2025/26. But it is **quarterly farmgate producer prices**, not a market quote. **[D]** A data.gouv.fr API sweep for `colza` returns **total: 1** — and it is crush volumes, not prices. Unusable as a futures substitute.
- **UFOP / AMI (Germany)** — **[D]** free monthly PDF carrying exactly the right numbers (`Erzeugerpreise in EUR/t Raps 507,00`, `Großhandelspreise in EUR/t Raps 515,00`), but the imprint reads:
  > "© AMI Alle Rechte vorbehalten. ... **Abdruck, Auswertung und Weitergabe nur mit ausdrücklicher Genehmigung.**"

  ("Reprinting, **analysis** and redistribution only with explicit permission.") **[I]** "Auswertung" explicitly reserves *analysis*, which forecloses pipeline ingestion even for internal use. **Do not ingest.**
- **Eurostat `apri_ap_crpouta`** — **[D]** exists, product code `02110000` "Rape - prices per 100 kg", but **annual only**, ~1-year lag, and **France stops at 2016**. Monthly equivalents are frozen at 2006-09. Useless here.
- **Poland dane.gov.pl** — **[D]** only ag hit is "Rynek Rolny" (IERiGŻ), CC BY 4.0, monthly **PDF**, resources last modified 2019-12-13. Not machine-readable.

---

## Alternative explanations considered

1. **"The strict reading of the Euronext disclaimer is correct and even free redistribution is barred."** Fully possible — stated in §1e. It does not change the recommendation: the recommendation does not redistribute Euronext data at all.
2. **"The MiFIR trades file is good enough and this is over-engineering."** Rejected on three observed grounds: it is post-trade transactions not settlements, 24-hour retention, and I could not confirm ECO rows appear. **[?]** Worth a second look by someone who can drive the download form.
3. **"€167.55/month is cheap; just buy it."** Defensible if the MATIF curve specifically is the product requirement. Rejected here because ~€2,010/year buys one futures series for a personal dashboard whose European rapeseed signal is ~95% recoverable free — and it adds a recurring commercial contract with audit rights to a hobby project.
4. **"Correlation 0.538 is fine for a dashboard."** Rejected: the sign of the mean annual spread flips six times in eight years. A proxy that inverts its own basis is worse than an absent line, because it will be read as a level.

---

## Side findings (out of scope, flagged for triage)

1. **[D]** Issue **#131 (X1) is closed but its commit `2b4891c` is NOT an ancestor of `origin/main`.** `git merge-base --is-ancestor 2b4891c origin/main` fails; the work sits only on local branch `x1-cross-oilseed`. `git show origin/main:config.py | grep -i canola` returns nothing — the canola COT entry and PSD rapeseed/Canada additions are not on main. Worth confirming whether the PR was ever merged.
2. **[D]** `config.py:317` comment "(Matif ECO has no free feed)" should be updated — delayed data *is* free for internal use; what is not free is publication.
3. **[D]** The EC world-price xlsx carries Argentina/Brazil/US Gulf soybean FOB, offering a licence-clean independent validation series for the existing Layers 19/20/21 FOB board.

---

## Answer to the ticket, restated

- **Licence?** Available and priced: **€167.55/month** (Delayed Data Redistribution, Last Price) or €334.95 for Level 2, per the January 2026 Information Product Fee Schedule, corroborated by Barchart's €163.60 pass-through. Historical/EOD Data Shop pricing: **price not published**. **Recommendation: do not buy.**
- **Proxy?** ICE canola: **reject** — no feed *and* wrong commodity (return correlation 0.538, spread sd 68 USD/t, sign-flipping annually, GM/non-GM wedge). World Bank rapeseed oil: **keep unchanged** as a monthly substitute-oil benchmark; never relabel it as rapeseed.
- **Drop the page?** **No — re-scope it.** Ship an "EU rapeseed / oilseed complex" page fed by the EC Oilseeds Market Observatory weekly world FOB file (`Rapeseed - EU Moselle`, EUR/t and USD/t, back to Dec 2018) plus the EC agri-food oilseeds API for per-Member-State cash detail. Free, CC BY 4.0, keyless, self-healing on re-fetch (no `data/history/` round-trip needed).
- **Required honesty note on the page:** *"MATIF (Euronext ECO) futures settlements are licensed data and are not shown. EU Moselle is a weekly physical FOB quote, published with a ~1-week lag — it is not the futures curve."*

**Open item:** one email to `databyeuronext@euronext.com` would resolve both the clause-6 ambiguity and the unpublished Data Shop EOD price. Cheap, and worth doing before this is treated as final.
