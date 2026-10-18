# V1 — Hand-verification table, edition 2026-10-17

**Issue:** [#474](https://github.com/philipbergman6-glitch/Mirror-Market/issues/474) (map [#472](https://github.com/philipbergman6-glitch/Mirror-Market/issues/472))
**Date of check:** 2026-10-18, 08:01–08:09 UTC (≈ 31 h after the edition was generated; inside the 48 h window)
**Author:** verification agent (Claude Opus 5.5), one sitting, no `/grilling` (owner away — open questions at the end)

Legend: **[O]** observed in this sitting (page fetched, PDF text extracted, browser fetch read) · **[I]** inference
from [O] · **[NV]** not verified. Arithmetic is shown so every delta can be recomputed by hand.

## Method

- **Edition.** Live site `https://philipbergman6-glitch.github.io/Mirror-Market/`, every page carrying
  `<meta name="mirror-market-generated-at" content="2026-10-17T01:24:28.247705+00:00">` [O] (players page
  `01:24:40`), built by the green `Refresh Prices` run of `2026-10-17T01:23:23Z` [O: `gh run list`]. Masthead
  "SATURDAY 17 OCTOBER 2026". Every number checked is "as of 2026-10-16" (Friday's session). SHA-256 of the HTML
  read (fetched 08:01 UTC 18 Oct):

  ```
  7f051b62ebf8904932fcb6c6ee5255aebc3870152ae0ef92fda43898594933b2  markets/cbot.html
  f5bb15bcc6afb55e40e3ea457ad04386d34d96fca45b8698c466d150ee369600  markets/dalian.html
  a67cb6de85581ad4fc2010a72cb1feeeb4c5132c70d2dcfe27d40aa017112713  markets/brazil.html
  f4a838cbbb1d67fd3c03b3ebdc53e332bf6914f7e1e01b94543c4647fc56d6f0  markets/argentina.html
  3a742201c594296b8425207f0adec47277572aedc92c021f66f0f0f1f0b182df  markets/south_africa.html
  d67e9093110ac40288d97d27849d926c32281b51bb6284ddee3e342e5205ea3f  workstation.html
  88c479099275ed53799ae34f23469b03997739870064f11e82fd020e8de04324  origins.html
  853e524bfa501105e66bd91c672790b818554decb8081870ef4225b0a441eb40  index.html
  ```
- **Independence.** No fetcher code was opened or run. Sources were reached with `curl` and, for DCE only, a
  Playwright browser. Displayed values were read from the rendered page text; FX was additionally read at full
  precision from the page's own embedded `fx_series` chart data (the visible text rounds to 4 dp).
- **Not touched.** cmegroup.com in any form (Data Terms forbid automated reading; owner note on #474) and
  agrural.com.br (owner: record *unverifiable*, do not search).
- **Minutes** are this agent's elapsed wall-clock per check, from a timestamp log. They are **not** human minutes;
  a human estimate is given separately (§ "One sitting?") and is an inference, not a measurement.
- **Conversions** (exact constants): ZS 1 MT = 36.74371 bu (27.2155422 kg/bu); ZL 1 MT = 2,204.62262 lb;
  ZM 1 short ton = 0.90718474 MT; CEPEA/AgRural 1 MT = 16.66667 sacas of 60 kg. ECB crosses = (CCY per EUR) ÷
  (USD per EUR) from the same 2026-10-16 fixing.

## The table

| # | Stratum | Displayed value | Unit | Label (as shown) | Obs. date | Contract / shipment window | FX date | Source reached (tier) | URL / document | Publication vintage | Source value & unit | Conversion applied | Δ native | Δ USD/MT | Classification | Min | Blocker |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1 CBOT | 1,260.50 (= 463.15 USD/MT) | ¢/bu | "CBOT Soybeans … delayed close" (workstation 02); "settlement not proven" (CBOT 03) | 2026-10-16 | ZSX26, Nov 2026 | n/a (USD) | **Tier 2** — USDA AMS republication of CBOT settlement | `ams.usda.gov/mnreports/ams_3147.pdf` and `ams_3046.pdf` (identical table) | "Grain Report for 10/16/2026 - Final"; PDF last-modified Fri 16 Oct 2026 18:50:10 / 18:47:56 GMT | 1261.25 (Nov 26) ¢/bu "Closing Settlement Prices (¢/bu) as of 10/16/2026" = 463.43 USD/MT | ¢/bu × 36.74371 ÷ 100 | −0.75 ¢/bu (3 ticks) | −0.28 | **Value delta, explained** (displayed is a provider close, publisher figure is the settlement — the page already says "settlement not proven"); exceeds tick tolerance, below any plausible economic threshold | 0.5 | none for ZS |
| 2 | 1 CBOT | 67.10 (= 1,479.30 USD/MT) | ¢/lb | "CBOT Soybean Oil … delayed close" | 2026-10-16 | ZLZ26, Dec 2026 | n/a | **Unverifiable (this sitting)** | CME Daily Bulletin Section 03 — not opened | — | — | — | — | — | Unverifiable: only publisher is CME, human-read only; no free second provider (V2) | 0 | CME Data Terms forbid agent reading; needs a human before the bulletin rolls. ZMZ26 356.70 $/st (393.19 USD/MT) identical status |
| 3 | 2 DCE | 3,978.00 (= 595.8 USD/MT) | CNY/MT | "DCE Soybean No.2 · headline · board · delayed close" | 2026-10-16 | **none shown**; inferred **b2611** | 2026-10-16 (CNY/USD 0.149772) | **Tier 1** — DCE 日行情 | `http://www.dce.com.cn/frontend/dcereport/#/zh/dayFuturesQuotation?variety=b` → in-page `POST /dcereport/publicweb/dailystat/dayQuotes` `{varietyId:"b",tradeDate:"20261016"}` | trade date 20261016; publication time not stated by DCE | b2611 `close 3978` (`clearPrice 3992`) CNY/MT | none (CNY/MT both sides) | 0 | 0 | **Pass** (value). Month inferred uniquely (no other b month closed at 3978); block 01 shows no month — the map's accepted weakness | 0.7 | needs a JS browser (瑞数 anti-bot) |
| 4 | 3 Brazil | 2,479.50 (= 489.1 USD/MT) | BRL/MT | "Soybean (CEPEA) · headline · physical assessment" | 2026-10-16 | spot (none applies) | 2026-10-16 (BRL/USD 0.197266) | **Tier 1** — CEPEA/Esalq | `https://www.cepea.org.br/br/indicador/soja.aspx`, table "INDICADOR DA SOJA CEPEA/ESALQ - PARANÁ" (Fonte: Cepea) | row 16/10/2026; publication hour not stated for Paraná | R$ 148,77 /sc 60 kg = 2,479.50 R$/MT | R$/sc × 16.66667 | 0.00 | 0.00 (BRL leg) | **Pass**. USD leg uses our FX: CEPEA's own US$ 29,31/sc = 488.50 USD/MT vs displayed 489.1 (+0.6, FX source/time — see row 8) | 0.3 | Chrome UA needed (plain curl UA → Cloudflare 403 per V2) |
| 5 | 3 Brazil | 2,575.00 (= 508.0 USD/MT) | BRL/MT | "Brazil Paranaguá FOB · physical assessment" (ledger row, CBOT/Dalian pages) | 2026-10-16 | none shown | 2026-10-16 | **Unverifiable** | — (AgRural; no publisher of record) | — | — (implies 154.50 R$/sc) | R$/MT ÷ 16.66667 | — | — | Unverifiable by design (map #472; B21 #483: an R$/sc "sobre rodas" port bid, not FOB) | 0 | not attempted, per owner |
| 6 | 4 Argentina | 483.0 | USD/MT | "Soybeans · headline · administered minimum · mean of 3 quotes" | 2026-10-16 | **none shown** (mean of 3) | n/a (USD) | **Unverifiable at Tier 1 at check time**; transport check only | DINEM index `dinem.magyp.gob.ar/dinem_fob.wp_fob_consall.aspx` lists circulars only through **2079 (14/10/2026)**; `…conslista.aspx?20261016,2081,193,1` renders an empty "NCirc0" shell. BCR homepage "SAGyP Cerc." shows 15/10 only | circular 2081 (16/10) absent from DINEM HTML at 08:04 UTC 18 Oct | (not counted) alt-host JSON circular 2081, 12019000190C: Oct-26 **493**, Nov-26–Apr-27 **495**, May–Sep-27 **461** USD/t | arithmetic mean (493+495+461)/3 = 483.0 | 0 vs transport | 0 vs transport | **Metadata fail, explained**: the headline averages three shipment windows incl. new-crop May–Sep 2027 and states none; no published price equals 483. Value itself unverified at Tier 1 | 1.4 | publisher HTML lags its JSON ≥ 40 h (lag unmeasured) |
| 7 | 5 US Gulf | +41.6 | USD/MT | "US Gulf CIF over CBOT · premium to the board" (CBOT 04) | 2026-10-16 | none shown; the local leg is "13.74 USD/bu · mean of 6 quotes" | n/a | **Tier 3** on a **Tier 1** leg | `ams.usda.gov/mnreports/ams_3147.pdf` "US #1 Soybeans (Bulk) … Gulf Coast Ports - LA … CIF-B" | "Grain Report for 10/16/2026 - Final" | Averages 13.5325 (Current, 87–97X), 13.7175 (Nov¹), 13.7375 (Nov²), 13.7775 (Dec, 100–104F), 13.8225 (Jan, 105–108F), 13.8375 (Feb, 99–100H) USD/bu | mean = 13.7375 USD/bu × 36.74371 = 504.77; − ZS close 463.15 = 41.61 | local leg 0.0000 USD/bu (13.74 ✓); Current-window basis mid 92 ¢/bu | recompute 0.0; vs AMS Current basis mid (33.80) **+7.80** | **Metadata fail, explained**: basis is a mean over six delivery windows (Current→Feb) priced over three CBOT months (X, F, H), shown against Nov board, no window stated. Arithmetic passes. Origins page's "Current (prompt)" leg $497.2 = 13.5325 × 36.74371 ✓ | 0.3 | none |
| 8 | 6 FX | USD per BRL 0.1973 (full: 0.197266) / "BRL per USD 5.07" | USD/BRL | "BRL/USD … every USD/MT figure on this page is struck at this rate" | 2026-10-16 | — | 2026-10-16 | **Tier 1** BCB PTAX + **Tier 2** ECB cross | BCB Olinda `CotacaoDolarDia(dataCotacao='10-16-2026')`; ECB `eurofxref-daily.xml` | PTAX `2026-10-16 13:04:14` BRT; ECB Cube time 2026-10-16 (14:10 CET concertation) | PTAX venda 5.08260; ECB 5.7197/1.1253 = 5.08282 BRL/USD | 1/0.197266 = 5.06930 | −0.0135 BRL/USD (−0.266 % vs ECB; −0.262 % vs PTAX) | +1.30 on CEPEA Paraná (489.12 vs 487.82) | **Pass, explained band** — different snapshot (Yahoo London-day bar vs 12:10 UTC fix), inside the V3 BRL 2σ band 1.4 % | 0.5 | none |
| 9 | 6 FX | USD per CNY 0.1498 (full: 0.149772) / "CNY per USD 6.68" | USD/CNY | "CNY/USD … every USD/MT figure on this page is struck at this rate" | 2026-10-16 | — | 2026-10-16 | **Tier 2** ECB cross | ECB `eurofxref-daily.xml`, `eurofxref-hist-90d.xml` | Cube time 2026-10-16 | 7.5240/1.1253 = 6.68622 CNY/USD | 1/0.149772 = 6.67682 | −0.0094 CNY/USD (−0.141 %) | +0.84 on DCE No.2 (595.79 vs 594.96) | **Value delta, unexplained (pending)** — outside the V3 CNY 2σ band (0.1 %). Context [O]: our series moved −0.236 % d/d in CNY terms (6.69259→6.67682) while ECB moved −0.072 % (6.69102→6.68622); on 15 Oct the two agreed to +0.023 %. The volatile-day rule needs the provider bar's High/Low, which this sitting did not read | 0.5 | volatile-day rule needs Yahoo H/L (the fetcher's own provider) |
| 10 | 7 Crush | +96.4 | USD/MT | "CBOT 03 Crush margin · Nov 2026 beans / Dec 2026 products"; headline: "mixed delivery months … embeds a calendar spread, so no margin verdict" | 2026-10-16 | ZSX26 / ZLZ26 / ZMZ26 (shown) | n/a | **Tier 3** (arithmetic) — legs: ZS Tier 2, ZL/ZM unverifiable | displayed legs on the same page | — | recompute with displayed 3-dp yields: 0.183×1,479.30 + 0.733×393.19 − 463.15 = **95.77**; with 11/60 and 44/60: **96.39** | yields × leg USD/MT | — | −0.63 vs displayed (with displayed yields); 0.00 with 11/60, 44/60 | **Value delta, explained** (displayed yields 0.183/0.733 are rounded 11/60, 44/60; a reader recomputing from the page misses by 0.6). Not verified overall: ZL/ZM legs unverifiable. With the AMS ZS settlement the margin is 96.11 (−0.28) | 0.2 | ZL/ZM legs (row 2) |
| 11 | 7 Crush | +7.4 (CNY 49) | USD/MT (CNY/MT) | "Dalian 03 Crush margin … Continuous main-contract series … names no contract" | 2026-10-16 | none shown; legs inferred **b2611 / y2701 / m2701** | 2026-10-16 | **Tier 3** on **Tier 1** legs | DCE dayQuotes 20261016 for b, m, y (as row 3) | trade date 20261016 | legs: b2611 close 3978, y2701 close 8844, m2701 close 3281 — all = displayed legs ✓ | 11/60×8844 + 44/60×3281 − 3978 = 49.47 CNY/MT | same-month Jan crush (b2701 4028): −0.53 CNY → Δ −50.0 CNY; same-month Nov (b/y/m2611): 9.13 CNY → Δ −40.3 CNY | Jan: −7.49; Nov: −6.04 | **Metadata fail (contract month), explained**: bean leg is Nov 2026, product legs Jan 2027 — an undisclosed calendar spread, unlike the CBOT crush which states it. Cause: provider continuous series roll on different schedules. Arithmetic passes | 0.2 | none |
| 12 | 8 Landed | **no number displayed** | — | Origins 01: "Not comparable … Destination port costs, Elevation, Financing / carry, Marine insurance, Ocean freight, Quality adjustment are not entered" | 2026-10-16 | windows Oct-2026 … Apr-2027 offered | — | **Unverifiable** — nothing to verify | origins page | — | — | — | — | — | No discrepancy: the site withholds the landed number with a reason (invariant 2). Public FOB-eq. legs seen: Argentina 2026-10 $493.0 (= circular 2081 Oct window, transport), Gulf Current $497.2 (✓ AMS), Paranaguá $508.0 (AgRural) | 0.3 | assumption stack empty in the public edition; private edition not in scope |
| 13 | 9 Cash oil/meal | 1,535.5 (range 1,458.4–1,612.7; board ZLZ26 1,480.4; premium −22.0 to +132.3) | USD/MT | "Crude soybean oil · Illinois · FOB · truck/rail · weekly processor ask" | week 2026-10-12–16, "published 2026-10-16 13:51:26" | board ZLZ26 (10-15 session per page) | n/a | **Tier 1** (AMS PDF; same AMS store as the MARS API — transport-independent, not store-independent) | `ams.usda.gov/mnreports/ams_3511.pdf` (fixed URL exists — V2 had it [NF]) | "Report for 10/12/2026 - 10/16/2026"; PDF last-modified 19:51:28 GMT; MARS `publishedDate 2026-10-16 13:51:26 MDT`, final | Illinois Ask −100.00Z to 600.00Z ¢/lb basis; 66.15–73.15 ¢/lb; avg 69.65; FOB-T/R | ¢/lb × 22.0462262 | 0.00 | avg 1,535.52 ✓; range 1,458.36–1,612.68 ✓; board 67.15 ¢/lb = 1,480.40 ✓; premium −22.05/+132.28 ✓ | **Pass** (value, terms, week). Observation: displayed publish time has no zone; MARS says MDT (= 14:51 CT) | 0.4 | none |

Total measured agent time: **5.8 min** elapsed (08:01:12 → 08:07:02 UTC) for capture plus all checks; edition capture
and page reading ≈ 1.6 min of that.

## Supplementary checks (not counted in the 12 numbers)

| Displayed | Source | Result |
|---|---|---|
| ZMZ26 356.70 $/st (393.19 USD/MT) | — | Unverifiable, same reason as row 2 |
| CEPEA Paranaguá 2,576.83 BRL/MT ($508.3), labelled "Soybean (ESALQ/B3 Paranaguá)" | CEPEA "INDICADOR DA SOJA CEPEA/ESALQ - PARANAGUÁ" 16/10/2026 R$ 154,61/sc | ✓ 154.61 × 16.66667 = 2,576.83. Label: CEPEA does not use "ESALQ/B3" (V2) — naming only |
| DCE m 3,281 / y 8,844 / a 5,449 CNY/MT | DCE closes m2701 3281, y2701 8844, a2701 5449 | ✓ all exact (settlements 3299 / 8834 / 5422 differ — the site shows closes, as labelled) |
| ZAR per USD 16.48 (USD per ZAR 0.0607) | ECB 18.4930/1.1253 = 16.43384 | +0.28 % (16.48) / +0.25 % (1/0.0607); inside V3 band 0.8 %. No full-precision series embedded on the South Africa page |
| Gulf local leg 13.74 USD/bu ($504.8) | AMS 3147 six window averages, mean 13.7375 | ✓ value; window blend as row 7 |
| AMS 3511 meal Illinois FOB-R $418.7; Minnesota $386.7 (369.1–429.7) | AMS 379.80 $/Ton; 350.80 (334.80–389.80) | ✓ 379.80 × 1.1023113 = 418.66; 386.69 / 369.05–429.68 (short-ton reading reproduces) |
| ZSF27 implied by "Nov 2026 -> Jan 2027: +14.75" = 1,275.25 ¢/bu | AMS 1275.75 (Jan 27) | −0.50 ¢/bu (2 ticks), same sign as row 1 |
| MAGyP circular 2080 (15/10) Oct window 494 USD/t | BCR homepage "SAGyP Cerc. U$S 494,00 15/10" | ✓ Tier 2 match — for 15 Oct, not the displayed 16 Oct |

## Counts

**By tier reached (12 numbers):** Tier 1 — 4 (rows 3, 4, 8 [+Tier 2], 13) · Tier 2 — 2 (rows 1, 9) ·
Tier 3 — 3 (rows 7, 10, 11) · Unverifiable — 3 (rows 2, 5, 6). Row 12 has no number (withheld by the site).

**By discrepancy class:**

| Class | Rows |
|---|---|
| Pass (no discrepancy) | 3, 4, 8, 13 — 4 |
| Value delta, explained | 1 (close vs settlement, −0.28 USD/MT), 10 (yield display rounding, −0.63) — 2 |
| Value delta, unexplained | 9 (CNY −0.141 %, +0.84 USD/MT) — 1, pending the volatile-day rule |
| Metadata fail, explained | 6 (shipment-window blend), 7 (window + reference-month blend), 11 (undisclosed Nov/Jan calendar spread) — 3 |
| Metadata fail, unexplained | 0 |
| Unverifiable (no comparison made) | 2, 5 — 2 (row 6's value is also unverified, but it is counted once, as a metadata fail) |
| Nothing to verify | 12 |

**Material** under the map's three-part rule: all three metadata fails are material by definition (window / contract
mismatch fails at any value delta). No value delta here is material unless the economic threshold is set below
0.3 USD/MT (row 1) or below 0.9 USD/MT (row 9).

## One sitting?

- **Agent:** 7 of 9 strata reached in 5.8 min. Two hard stops no amount of time fixes: CME (stratum 1 ZL/ZM, and
  Tier 1 for ZS) is human-read only, and the MAGyP DINEM HTML circular for the displayed day was not published
  ~40 h after the session (the JSON service already had it). Stratum 8 has no public number to check.
- **Human (estimate, [I], not measured):** with the URLs in this file a human would need roughly 5 min for the CME
  bulletin, 5 for DCE (browser, Chinese UI), 3 for CEPEA, 5–10 for MAGyP (postback HTML), 3 for AMS 3147, 5 for
  AMS 3511, 5–10 for the ECB crosses and the CNY/BRL arithmetic, 5 for the two crush recomputations, 2 for landed:
  **≈ 45–60 min.** Feasible in one sitting **only if** the sitting falls inside both the CME bulletin's single-day
  window and after MAGyP posts the HTML circular — which this sitting (Sunday) could not satisfy.

## Tolerances that look realistic (proposals — one sample each, not a decision)

| Dataset | Tick / rounding tolerance | What the sample showed | Proposed economic threshold |
|---|---|---|---|
| CBOT ZS/ZL/ZM vs settlement | 0 vs CME "Last"/close if a human can read it; vs settlement a definitional band ≥ 3 ticks | ZS −3 ticks, ZSF27 −2 ticks | 0.5 USD/MT; compare against the bulletin's settlement only with the "not a settlement" label kept |
| DCE close per named month | 0 CNY (exact) | 4/4 exact | n/a — any non-zero delta is a defect |
| CEPEA indicator | ±0.005 R$/sc (= 0.083 R$/MT) | exact | n/a — exact |
| MAGyP official FOB | 0 USD/t per window | (transport only) | n/a — but the window must be shown or the row fails |
| AMS 3147 / 3511 | conversion rounding ≤ 0.05 USD/MT | exact | n/a — exact; window/terms must match |
| FX vs ECB cross | display precision (4 dp shown; 6 dp embedded) | BRL −0.27 %, ZAR +0.28 %, CNY −0.14 % | band per pair from V3 (BRL 1.4 %, ZAR 0.8 %) — **CNY 0.1 % looks too tight**: one ordinary day breached it; propose ≥ 0.3 % or the volatile-day rule, and state the band in USD/MT on the leg it moves (≈ 1.8 USD/MT on a 600 USD/MT bean) |
| Crush (Tier 3) | ±1 USD/MT when recomputed from displayed 3-dp yields | −0.63 | 1 USD/MT; or display 11/60, 44/60 |

## Findings worth a ticket (owner to decide; none filed)

1. **DCE crush mixes Nov beans with Jan products and does not say so** (row 11) — the CBOT crush discloses its
   calendar spread; the Dalian one does not. The displayed +49 CNY/MT matches neither same-month crush (Jan −0.5,
   Nov +9.1).
2. **Two headline physical numbers are means over shipment windows with no window shown** — Argentina soybeans
   (row 6, includes new-crop May–Sep 2027) and the US Gulf ledger leg/basis (row 7, Current→Feb, over X/F/H).
   The origins page already uses the single-window values ($493.0 Oct, $497.2 Current); the market pages do not.
3. **MAGyP's HTML circular lags its JSON service** (row 6): DINEM listed circulars only to 2079 (14 Oct) on 18 Oct.
   The Tier 1 path the ledger plans to use may not exist for the displayed day inside 48 h.
4. Minor: the AMS 3511 publish time is shown without a zone (it is MDT); CBOT crush yields display as 0.183/0.733.

## Open questions for the owner

- CME: please open `Section03_Agricultural_Futures.pdf` in a normal browser and record the 16 Oct ZLZ26/ZMZ26
  settlements and the Last column for ZSX26 — it shows one trade date only, and I am **not confident** when the
  16 Oct bulletin is replaced (likely at Monday's preliminary, ~00:00 CT Tuesday 20 Oct).
- Is a blended-window headline (rows 6, 7) a metadata fail, as the map's rule reads, or an accepted labelled
  aggregate ("mean of N quotes")? The ledger's verdict on two strata depends on this.
- CNY band: accept the volatile-day rule reading the provider's own High/Low (same provider as the fetcher), or
  widen the band?
