# Accuracy V4–V6 — evidence-backed proposals for the owner's decisions

**Map:** [#472](https://github.com/philipbergman6-glitch/Mirror-Market/issues/472) · **Tickets:**
[V4 #477](https://github.com/philipbergman6-glitch/Mirror-Market/issues/477) materiality ·
[V5 #478](https://github.com/philipbergman6-glitch/Mirror-Market/issues/478) ledger page contract ·
[V6 #479](https://github.com/philipbergman6-glitch/Mirror-Market/issues/479) recurring regime
**Date:** 2026-11-25 · **Author:** research session (Claude Opus 5.5), AFK, no grilling held

**Status: proposals only.** All three tickets are the owner's grilling tickets. Nothing here is decided, and nothing
here changes code, config or the glossary. Each section ends with the questions the owner must answer, with a
recommended answer first.

Legend, as in V1: **[O]** observed in this session (file read, query run, page or PDF read), **[I]** inference from
[O], **UNVERIFIED** = not confirmed against a primary source in this session (memory, a search snippet, or a
secondary page). Every UNVERIFIED fact is marked where it is used.

Inputs: `research/v1-hand-verification-2026-10-17.md` (V1, edition 2026-10-17); V2 #475 and V3 #476 resolutions
(the V3 findings file lives on branch `research/v3-fx-publishers`, not on `main`); `CONTEXT.md` → Verification;
`DESIGN.md`; `config.py`; `pipeline/units.py`; `LAYERS.md`; the private DB copy `data/storage/mirror_market.db`
(newest rows 2026-11-17); the git history of `data/history/*.csv`. Publisher facts are in §A, gathered from
primary sources by a separate research pass. Scripts used for every [O] number are in §B, so each number can be
recomputed.

---

## 0. What changed since V1 — the basket V4–V6 will grade is not the one V1 saw

V1 checked edition 2026-10-17. Several of its findings have been fixed in code since then, and one fix changes
which tier a stratum can reach.

| V1 finding | Fix since | Effect on V4–V6 |
|---|---|---|
| Row 11: the DCE crush mixed Nov beans with Jan products and did not say so | B29 [#503](https://github.com/philipbergman6-glitch/Mirror-Market/issues/503): crush struck on named months from `dce_contract_bars` [O `config.py` Dalian `crush.table`] | Stratum 7 (DCE) can now be checked month by month at Tier 1 per leg |
| Rows 6–7: Argentina and Gulf headlines were means across shipment windows, with no window shown | B30 [#504](https://github.com/philipbergman6-glitch/Mirror-Market/issues/504) removed both blends; B39 [#540](https://github.com/philipbergman6-glitch/Mirror-Market/issues/540) compares a print by loading month; B41 [#550](https://github.com/philipbergman6-glitch/Mirror-Market/issues/550) withholds sessions whose windows have all sailed [O `DESIGN.md` decision log; `CONTEXT.md` "Labelled blend": "none is rendered today"] | Strata 4–5 compare one window against one window. The "labelled blend" rule is still a proposal, but nothing renders a blend for it to grade |
| Row 7: Gulf basis struck over the front month | #512: struck over the named contract the AMS slot quotes [O `config.py` `AVAILABILITY_BASKET_BLOCKS` comment] | Stratum 5 has a contract identity check that can pass |
| Row 9: CNY −0.141 % against the ECB cross, "unexplained (pending)" | B25 [#495](https://github.com/philipbergman6-glitch/Mirror-Market/issues/495): **CNY/USD is now fetched from the ECB** [O `config.FX_ECB_CROSS_PAIRS = {"CNY/USD": "CNY"}`, `fetchers/ecb_fx.py`] | For CNY the ECB is now the publisher of record. A check against it is **Tier 1 with an exact tolerance**, not a Tier 2 band. The 0.1 % / 0.3 % band debate in V1 is moot for CNY. BRL and ZAR are still Yahoo [O `CURRENCY_TICKERS`] |
| Row 3: DCE month inferred | Not fixed for block 01: the Dalian price block still reads the continuous `dce_futures` table [O `config.py` Dalian `price.table = "dce_futures"`] | Stratum 2 stays "contract month inferred". The map has this as an accepted weakness |
| Row 7: AMS 3147 checked against the AMS PDF | — | The Gulf fetcher **reads that same PDF** [O `LAYERS.md` Layer 20: "AMS report 3147 … daily PDF"]. V1's check was independent code reading the same document. That is a legitimate Tier 1 check when the document *is* the publisher of record, but it catches our parsing, selection and conversion errors, not a different observation. The same holds for CEPEA, MAGyP and now ECB-CNY. The ledger should say so (§2.2 field `transport`) |

---

## 1. V4 — materiality numbers per dataset

### 1.1 The rule as charted, and three sharpenings

Charted rule (map #472): a discrepancy is **material** if it exceeds *both* the dataset's tick/rounding tolerance
*and* an economic threshold in USD/MT, or if it is a metadata fail (date, contract, grade, incoterm, window, FX
date) at any value delta.

Three things the evidence adds:

1. **Comparisons come in three kinds, and the rule needs a name for the middle one.** The glossary's Tier 2 is "a
   second provider publishing *the same definition*". But two of the basket's planned checks compare *different
   definitions*: the CBOT stratum compares a provider close against a CME settlement, and BRL/ZAR compare a Yahoo
   London-day bar against an ECB 14:10 CET fix. Neither can ever match exactly. Proposed vocabulary (for
   `/domain-modeling`):
   - **exact**: same definition. Tolerance = the publisher's rounding, plus our display rounding and conversion
     residue (point 2). DCE, CEPEA, MAGyP, AMS 3147/3511, ECB-CNY.
   - **band**: different definition, compared inside a stated **explained band**. Outside the band, the
     discrepancy is unexplained until someone explains it. CBOT close vs settlement, BRL/ZAR vs ECB or PTAX.
   - **recompute**: Tier 3 arithmetic from verified legs. Tolerance = the propagated rounding of the inputs *as
     displayed*.
2. **The tolerance must include our own rounding.** The site prints USD/MT to 1 dp in places (`595.8`, `489.1`
   [O V1 rows 3–4]), so an exact check has at least ±0.05 USD/MT of display rounding. `pipeline/units.py` uses
   rounded constants (36.7437 bu/MT, 2204.62 lb/MT, 0.907185 MT/st [O lines 31–80]). Against the exact values
   (36.74371, 2204.62262, 0.90718474) the residue is ≤ 1.2 × 10⁻⁶ relative. That is ≤ 0.002 USD/MT on a
   1,480 USD/MT oil leg [I, arithmetic]. Negligible, but a verifier that carries its own exact constants (§3.5)
   will see it, so the tolerance has to say it is allowed.
3. **Material and explained are separate axes.** The V7 gate is "zero *unexplained material*". An explained band
   does not make a delta immaterial, and an economic threshold does not explain a delta. A BRL bar 0.27 % off
   the ECB fix moves a 489 USD/MT CEPEA leg by 1.30 USD/MT [O V1 row 8]. That is above any threshold proposed
   below, so it is *material*, and it sits inside the BRL band, so it is *explained*. The ledger shows it as
   "material · explained", and it does not block V7. Folding the two axes into one would hide FX timing gaps
   from readers.

### 1.2 Publisher tick and rounding per stratum

Sources are in §A. The native tolerance is **0** when the page displays the publisher's own printed number, and
**half the last published unit** when the displayed figure is our conversion or re-rounding of it.

| # | Publisher / series | Native unit | Published grid (tick or last decimal) | Grid in USD/MT [I] | Rounding / definition note | Status |
|---|---|---|---|---|---|---|
| 1 | CBOT ZS (5,000 bu) | ¢/bu | ¼ ¢/bu | 0.092 | Lead-month settlement = VWAP of outright trades 13:14:00–13:15:00 CT, **rounded to the nearest tradable tick**, ties to the tick nearer the prior settle; deferred months settle off calendar-spread VWAP in the same window | tick: VERIFIED-snippet (rulebook ch. 11); window: VERIFIED (CME client wiki, updated 2025-12-31); rounding clause: VERIFIED-snippet |
| 1 | CBOT ZL (60,000 lb) | ¢/lb | 0.01 ¢/lb | 0.220 | same settlement procedure | VERIFIED-snippet (ch. 12) |
| 1 | CBOT ZM (100 short tons) | $/short ton | $0.10/st | 0.110 | same | VERIFIED-snippet (ch. 13) |
| 1 | ZS settlement republished in AMS 3147 | ¢/bu | ¼ ¢ (all 7 settles of 2026-11-24 on tick) | 0.092 | "Closing Settlement Prices (¢/bu)" | VERIFIED (AMS PDF read) |
| 2 | DCE No.1 / No.2 / meal (10 MT lots) | CNY/MT | 1 CNY/MT | ≈ 0.150 | settlement = **whole trading day's volume-weighted average** (Clearing Rules art. 40, 2024 rev.), not a closing window; the site displays the close [O V1 row 3]; no rounding-to-tick clause found | tick: VERIFIED-snippet (DCE rules pages); settlement: VERIFIED (rules text via a mirror) |
| 2 | DCE oil | CNY/MT | **2 CNY/MT** | ≈ 0.300 | whether an oil settlement can land off its 2-CNY tick: UNVERIFIED | VERIFIED-snippet |
| 3 | CEPEA/ESALQ Paraná and Paranaguá | R$/sc 60 kg (US$/sc on the same row) | 0.01 R$/sc | ≈ 0.028 (at 5.86 BRL/USD) | US$ column converted at the 16:30 commercial dollar; published after 18:01 on business days; no rounding rule stated. **Licence CC BY-NC 4.0** | VERIFIED (page + methodology PDFs) |
| 3 | AgRural Paranaguá | R$/sc 60 kg | prints 2 dp, but every value seen ends ,00 or ,50 → effective **R$ 0.50/sc** | ≈ 1.42 | no rounding statement; **reproduction of price tables forbidden without written permission** | page VERIFIED; grid INFERRED from ~40 values |
| 4 | MAGyP official FOB (Ley 21.453 circulars) | USD/t | **whole dollars** | 1.00 | the circular PDF prints no unit; daily page states USD/t | VERIFIED (circular PDF + daily page) |
| 5 | AMS 3147 Louisiana & Texas Export Bids (daily, Preliminary + Final editions) | basis ¢/bu; price $/bu | basis printed 2 dp but **every value a whole cent**; price 4 dp | basis 0.367; price ≈ 0.004 | "Price & Basis Values quoted are for Current Delivery, unless otherwise noted" | VERIFIED (2026-11-24 Final) |
| 9 | AMS 3511 processor feedstuff (weekly) — meal | $/short ton | 0.01 $/st; basis moves in $5 steps | 0.011 | — | VERIFIED |
| 9 | AMS 3511 — oil | price ¢/lb; basis **points** (1/100 ¢/lb) though headed "¢/Lb" | 0.01 ¢/lb | 0.220 | the repo already reads the basis as points [O `fetchers/processor_cash.py` docstring, `basis_unit = points_per_lb`] | VERIFIED (agent arithmetic) + [O] |
| 6 | ECB euro reference rates (USD, BRL, CNY, INR, ZAR legs) | units per EUR | **fixed 4 dp** for these five (not a significant-digit rule: GBP 5 dp, JPY 2 dp) | cross rounding ≤ ~5 × 10⁻⁵ relative (V3) | concertation ~14:10 CET, published ~16:00 CET, "for information purposes only" | VERIFIED (2026-11-24 XML + ECB page) |
| 6 | BCB PTAX | BRL per USD | 4 dp effective (API returns a 5th digit that is always 0) | — | 4 polls 10–13h BRT, trim 2 high/2 low, mean | data VERIFIED; method VERIFIED-snippet; a formal 4-dp rule UNVERIFIED |
| — | Conversion constants | — | 1 MT = 36.7437 bu (soy); 1 MT = 2,204.622 lb; 1 st = 0.907185 MT (USDA ERS AH-697); exact 0.90718474 and 1 lb = 0.45359237 kg (NIST HB44 App. C) → exact 36.74371 bu/MT, 2,204.62262 lb/MT | — | `pipeline/units.py` uses the AH-697 rounded values [O] | VERIFIED (AH-697 downloaded); NIST VERIFIED-snippet |

[I] Every publisher grid is **≤ 0.3 USD/MT except MAGyP (1.00) and AgRural (≈ 1.42, unverifiable anyway)**. So
option A's 0.5 USD/MT (§1.4) sits above every verifiable grid but one: on MAGyP *any* non-zero delta is material,
which is the right answer for an exact, whole-dollar administered price.

### 1.3 What a buyer would call material: measured day-over-day moves

An economic threshold should sit well under the move a buyer acts on, so that an error large enough to change a
decision cannot pass. It should also sit above the noise of honest rounding. The distributions below come from the
desk DB copy, over the 12 months to 2026-11-17. Each move is the absolute change between consecutive sessions on
the **same** contract, window or slot, converted to USD/MT at that session's FX rate where needed [O, §B script
`moves.py`].

| Stratum | Series | n | p25 | **median** | p75 | p90 | share of moves < 0.5 USD/MT |
|---|---|---|---|---|---|---|---|
| 1 | CBOT ZS named contracts | 1,694 | 0.83 | **1.75** | 3.31 | 5.33 | 15 % |
| 1 | CBOT ZL named contracts | 1,927 | 4.63 | **10.58** | 19.84 | 31.39 | 4 % |
| 1 | CBOT ZM named contracts | 1,927 | 0.99 | **2.09** | 3.75 | 5.62 | 13 % |
| 2 | DCE No.2 named contracts | 772 | 1.04 | **2.24** | 4.75 | 7.50 | 15 % |
| 2 | DCE meal named contracts | 781 | 0.75 | **1.79** | 3.29 | 6.11 | 16 % |
| 2 | DCE oil named contracts | 780 | 2.66 | **5.79** | 10.78 | 15.66 | 6 % |
| 3 | CEPEA Paraná | 182 | 0.78 | **1.68** | 2.85 | 4.81 | 14 % |
| 3 | CEPEA Paranaguá | 182 | 0.99 | **2.06** | 3.88 | 5.65 | 13 % |
| 3 | AgRural Paranaguá (unverifiable) | 61 | 1.62 | **3.05** | 3.34 | 6.62 | 16 % |
| 4 | MAGyP soybeans, same window and route | 1,006 | 1.00 | **3.00** | 5.00 | 8.00 | 13 % (all of them zero moves: MAGyP prints whole dollars) |
| 5 | AMS 3147 Gulf "Current" basis mid, same reference month | 225 | 0.00 | **0.00** | 0.37 | 2.20 | 76 % — **71 % of reports leave the basis unchanged** |
| 5 | same, non-zero moves only | 61 | 0.92 | **1.65** | 2.94 | 4.04 | 13 % |
| 6 | BRL/USD daily % → on a 500 USD/MT leg | 12 mo | — | **0.42 % → 2.10** | — | 1.21 % | — |
| 6 | CNY/USD | 12 mo | — | **0.056 % → 0.28** | — | 0.19 % | — |
| 6 | ZAR/USD | 12 mo | — | **0.40 % → 2.00** | — | 1.09 % | — |
| 9 | AMS 3511 Illinois meal, week over week | 94 | 4.35 | **8.10** | 13.59 | 18.67 | 4 % |
| 9 | AMS 3511 Illinois oil, week over week | 47 | 23.70 | **35.71** | 58.64 | 91.36 | 0 % |

Readings [I]:
- For beans, meal, DCE and the Brazil indicators, the typical day moves **1.7–2.2 USD/MT**. Gulf basis moves
  less often than that: most days it does not move at all, and when it does it moves about 1.65 USD/MT, which is
  about 4.5 ¢/bu.
- **1 ¢/bu = 0.367 USD/MT** (36.7437 / 100). US basis and Brazil FOB premiums are quoted in whole cents per
  bushel (AMS 3147 prints basis as `95.00` to `118.00` ¢/bu in its 2026-11-02 → 11-13 reports [O `gulf_bids` rows]). The smallest unit a buyer
  negotiates basis in is therefore about **0.37 USD/MT**.
- Oil trades at about three times the price per tonne of beans (≈1,480 vs ≈463 USD/MT [O V1 rows 1–2]), and it
  moves about five times as much per day. A flat USD/MT threshold that suits beans is very tight for oil.

### 1.4 Proposed thresholds: three options, one recommended

| Option | Rule | Beans / meal / indicators | Oil legs | Spreads, basis, crush, landed |
|---|---|---|---|---|
| **A (recommended)** | Flat **0.5 USD/MT** for every bean-scale number and every derived number; **1.5 USD/MT** for oil legs (≈ 0.1 % of level) | 0.5 ≈ 1.4 ¢/bu, ≈ 25 % of a median day | 1.5 ≈ 14 % of a median day | 0.5 (a derived number's level is near zero, so a relative rule has no meaning there) |
| B | **0.1 % of the leg's own USD/MT level**, with a floor of 0.25 | ≈ 0.46 beans, 0.39 meal | ≈ 1.48 | floor 0.25, or the legs' propagated thresholds |
| C | Flat **1.0 USD/MT** everywhere | ≈ 2.7 ¢/bu, ≈ 55 % of a median day | 1.0 | 1.0 |

Why A: it is one number a buyer can hold in their head ("half a dollar a tonne"), and it sits just above the
negotiating unit of 1 ¢/bu (0.37). Any error that would move a basis negotiation by more than a cent therefore
counts as material. B is equivalent in effect but cannot be stated in one line. C would let a 2 ¢/bu basis error
pass as immaterial, and 2 ¢/bu is almost half the typical non-zero Gulf basis move (1.65 USD/MT ≈ 4.5 ¢/bu),
so C passes almost half a typical move.

**Per-stratum proposal under option A**

| # | Stratum | Kind | Native tolerance (§1.2) | Economic threshold | Explained band (band kind only) | Identity checks that fail at any delta |
|---|---|---|---|---|---|---|
| 1 | CBOT ZS/ZL/ZM named contracts | **band** (provider close vs CME settlement) | 1 tick + display rounding | 0.5 ZS/ZM · 1.5 ZL | **hypothesis: ≤ 4 ticks** (ZS 1 ¢/bu = 0.37; ZL 0.04 ¢/lb = 0.88; ZM 0.4 $/st = 0.44 USD/MT). Must be measured on the first full pass (V1 sample: ZS 3, ZL 3, ZM 4 ticks [O V1 row 2]) | contract month, session date |
| 2 | DCE No.2/meal/oil | **exact** (close vs DCE close, inferred month) | 0 CNY (grid 1 CNY; oil 2 CNY, §1.2) + display rounding | 0.5 · oil 1.5 | — | session date; month *inferred*, shown as a weakness, never a pass of the month check |
| 3 | CEPEA Paraná / Paranaguá | **exact** in BRL; the USD leg is graded under stratum 6 | ±0.005 R$/sc = ±0.083 R$/MT | 0.5 | — | indicator name (Paraná ≠ Paranaguá), date |
| 3 | AgRural Paranaguá | **unverifiable** (B21 #483) | — | — | — | — |
| 4 | MAGyP official FOB | **exact** per window | 0 USD/MT (whole dollars, §1.2) | 0.5 (in practice any nonzero delta is ≥ 1, so it is always material) | — | window/band, product position code, date |
| 5 | AMS 3147 Gulf basis + local leg | **exact** on the leg; **recompute** on the basis | leg: AMS's own decimals (4 dp USD/bu) ±0.00005 USD/bu; basis: our recompute ±0.05 USD/MT | 0.5 | — | delivery slot, the CBOT contract the slot quotes over, report date |
| 6 | FX | CNY: **exact** vs ECB (fixed 4 dp per EUR leg, §1.2); BRL/ZAR: **band** vs ECB and BRL vs PTAX | CNY: cross rounding ≤ 5 × 10⁻⁵ relative [V3] | grade on the leg it moves: 0.5 on the converted leg | BRL/ZAR: V3's √-time 2σ bands (BRL 1.4 %, ZAR 0.8 %), **or the volatile-day rule: the fix lies inside the displayed bar's [Low, High]** — see 1.5 | FX date = price date, or a labelled prior close ≤ 3 days old (A1) |
| 7 | Board crush (CBOT named) + DCE crush (named) | **recompute** | propagated: displayed 3-dp yields ±0.0005 × (oil + meal USD/MT) ≈ ±0.94 [I, from V1 legs]. Or display 11/60 and 44/60 and the tolerance falls to ±0.05 | 0.5 | — | all legs on one session (invariant 8); named months as labelled |
| 8 | One landed row | **recompute** against the entered assumption stack | ±0.05 per summed leg | 0.5 | — | route, window, assumption vintage. Today: nothing to verify, because the public stack is empty [O V1 row 12] |
| 9 | AMS 3511 cash oil/meal | **exact** | AMS decimals (¢/lb 2 dp; $/st 2 dp) + conversion ±0.05 USD/MT | 0.5 meal · 1.5 oil | — | location, freight/terms (FOB truck/rail), week, board contract |

### 1.5 Explained bands — what the data supports

- **CBOT close vs settlement.** The CME settles grain futures inside a closing window (§1.2). A provider
  "close" can be a last trade, or a bar later back-filled. The repo has **no stored settlement series**, so the
  band cannot be measured from history. The only measurements are V1's three numbers (3, 3 and 4 ticks). The
  "≤ 4 ticks" figure above is a hypothesis for the first full pass to confirm or widen, the same status V3's FX
  bands had.
- **New evidence: our stored CBOT closes are revised after first capture.** Across the 30 days to 2026-11-25, the
  committed `data/history/contract_bars.csv` changed `Close` **347** times on **284** distinct already-stored soy-complex contract-sessions (one session can be revised more than once) [O,
  §B `rev_cb.py`; git diffs of daily snapshot commits].

  | Product | revisions | median lag (session → commit) | max lag | median \|Δ\| USD/MT | p90 | max | share ≥ 0.5 USD/MT |
  |---|---|---|---|---|---|---|---|
  | ZS | 111 | 1 day | 10 days | 0.37 | 0.92 | 1.75 | 29 % |
  | ZL | 119 | 1 day | 10 days | 1.54 | 6.61 | 10.80 | 77 % |
  | ZM | 117 | 1 day | 10 days | 0.44 | 1.54 | 3.53 | 42 % |

  Layer 11b re-fetches each listed contract at `period="max"` every run [O `LAYERS.md` 11b]. A Yahoo
  back-correction is therefore absorbed silently whenever it is under the 20 % quarantine threshold
  (`quarantined_revisions` is empty [O]). [I] Consequences:
  1. An edition's displayed close can differ from what history later stores for the same session. The verifier
     must capture the **displayed** value at edition time (page text plus page SHA, as V1 did) and never re-read
     it from the DB afterwards.
  2. The ZL band may need to be wider than 4 ticks, or the ZL threshold may be crossed routinely by the
     provider's own revisions.
  3. This is the "revised source publication" event class V6 asks for, and it occurs every day.

  **Not yet triaged.** These figures count Close changes on the same (ticker, Date) key across consecutive daily
  commits. The commits were not examined one by one, and some changes could be same-day re-captures before the
  bar was final (60 % have a lag of ≤ 1 day). A `backlog:integrity` ticket to characterise them is a candidate
  (question V4-Q6).
- **BRL/ZAR vs ECB.** V3 derived the bands from annualised volatility (BRL 1.4 %, ZAR 0.8 %). Measured
  day-over-day here: BRL median 0.42 %, p90 1.21 %; ZAR median 0.40 %, p90 1.09 % [O]. V3's bands are therefore
  about p90 of a whole day's move. On a 500 USD/MT leg, a 1.4 % band is **±7 USD/MT** [I], fourteen times
  option A's threshold. A band that wide explains almost any FX error. The **volatile-day rule**, applied every
  day (the fix must lie inside the displayed bar's [Low, High]), is a far tighter structural test. It reads the
  displayed bar's own range, so it adds no new provider. Third option, outside V4's scope: move BRL and ZAR onto
  the ECB cross as B25 did for CNY. That turns stratum 6 into an exact Tier 1 check and removes the B20
  London-day issue for those pairs.
- **Republication lag** (Notícias Agrícolas, MAGyP HTML vs JSON). Under the charted rule a labelled D−1 is not a
  discrepancy: the verifier compares the value for the *displayed* date. Lag is therefore an availability fact
  for V6 ("publication not yet visible at check time → retry within the window"), not a value band. V1 row 6:
  the MAGyP DINEM HTML lagged its JSON by ≥ 40 h [O V1].

### 1.6 V1's observations graded under option A

| V1 row | Observed | Kind | Within tolerance? | Material (≥ threshold)? | Explained? | Grade under A |
|---|---|---|---|---|---|---|
| 1 ZSX26 vs AMS-republished settlement | −0.75 ¢/bu, −0.28 USD/MT | band | no (3 ticks > 1) | no (0.28 < 0.5) | yes, inside the ≤ 4-tick band | **discrepancy · immaterial · explained** |
| 2 ZLZ26 vs CME bulletin | −0.03 ¢/lb (3 ticks), −0.66 | band | no | no (0.66 < 1.5) | yes | **immaterial · explained** |
| 2 ZMZ26 vs CME bulletin | −0.40 $/st (4 ticks), −0.44 | band | no | no | yes (at the band edge) | **immaterial · explained** |
| 3 DCE No.2 | 0 | exact | yes | — | — | **pass** (month inferred, shown as a weakness) |
| 4 CEPEA Paraná | 0 | exact | yes | — | — | **pass** |
| 5 AgRural | — | — | — | — | — | **unverifiable** |
| 6 MAGyP 483.0 blend | metadata fail | — | — | material by rule | yes (blend; since fixed by B30) | V1 then: **material · explained**. Today: re-check one window [I] |
| 7 Gulf basis blend | metadata fail; recompute 0.0 | — | — | material by rule | yes (since fixed by B30/#512) | as row 6 |
| 8 BRL vs ECB | −0.266 %, +1.30 on the CEPEA leg | band | no | **yes** (1.30 ≥ 0.5) | yes (inside the 1.4 % band); volatile-day rule not run | **material · explained**. Not blocking |
| 9 CNY vs ECB | −0.141 %, +0.84 | band then; **exact now** (B25) | — | yes (0.84 ≥ 0.5) | under a 0.3 % band yes; under 0.1 % no | V1 then: **material · unexplained (pending)**. Today: an exact Tier 1 check [I] |
| 10 CBOT crush recompute | −0.63 | recompute | **yes** (±0.94 propagated) | — | — | **pass** |
| 11 DCE crush month mix | metadata fail | — | — | material by rule | yes (since fixed by B29) | then **material · explained**; today re-check |
| 12 Landed | nothing displayed | — | — | — | — | **nothing to verify** |
| 13 AMS 3511 oil | 0 | exact | yes | — | — | **pass** |

Under option A, V1 would have read: 4 pass · 3 immaterial or explained · 4 material-explained (three metadata,
one FX) · **1 material-unexplained (CNY, which B25 has since removed by construction)** · 1 unverifiable · 1
nothing to verify. Under option C, row 8 (1.30 USD/MT) stays material and row 9 (0.84) becomes immaterial. Under
B, the result is the same as A.

### 1.7 Where tolerances live, and how a change is recorded

The ticket offers `config.py` or the trust registry. **Both are product code, and the verifier must not import
product code (§3.5).** If tolerances lived in `config.py`, a commit that loosened a tolerance would be
indistinguishable from any other config change. It would also put the verifier's thresholds inside the thing it
verifies.

Recommendation:
- Tolerances live in one versioned data file **inside the verifier package** (e.g. `verify/tolerances.toml`),
  one block per stratum: kind, native tolerance and unit, economic threshold, band, identity checks, and a
  `since` date.
- Every verification record stamps the `tolerance_version` it was graded under (the file's content hash plus a
  human version number). Past records are **never re-graded** when the file changes.
- A tolerance change is itself a ledger row in a **"Tolerance changes"** table: date, stratum, old → new, reason,
  link to the PR. Loosening any tolerance requires a reason that cites evidence (a measured band), not a failing
  sample.
- The product (`app/`) reads the verifier's *output* records to render the ledger page, never its tolerances
  directly.

### 1.8 V4 — questions for the owner (recommended answer first)

1. **Economic threshold.** *Rec: option A, 0.5 USD/MT for bean-scale and derived numbers and 1.5 for oil legs.*
   Alternatives: B (0.1 % of level) or C (flat 1.0).
2. **Should the three comparison kinds (exact / band / recompute) be added to the glossary, and does a band
   comparison count as Tier 2?** *Rec: yes, add them. Record a band check as "Tier 2 (definition differs: close
   vs settlement)" so the reader sees the definitional gap rather than an unqualified Tier 2.*
3. **CBOT close-vs-settlement band.** *Rec: start at ≤ 4 ticks as a hypothesis, measured on the first full pass,
   and widen only with a measured distribution, recorded as a tolerance change.*
4. **BRL/ZAR band.** *Rec: the volatile-day rule every day (the fix inside the displayed bar's [Low, High]), with
   V3's % band kept only as an upper cap.* Alternative: open a ticket to move BRL/ZAR to the ECB cross, as B25
   did for CNY, which makes stratum 6 exact.
5. **An immaterial discrepancy on an exact dataset** (say DCE 1 CNY off). *Rec: list it in the ledger and open a
   `backlog:integrity` defect, but do not count it against V7.* On an exact dataset any non-zero delta means a
   parse or selection fault, even when it is small.
6. **The CBOT close revisions in §1.5** (347 in 30 days, ZL median 1.54 USD/MT). *Rec: file a
   `backlog:integrity` ticket to characterise them (same-day recapture vs later back-correction) before the CBOT
   band is fixed.*
7. **Where tolerances live.** *Rec: a versioned file inside the verifier package, not `config.py` or the trust
   registry, with every change recorded as a ledger row (§1.7).*

---

## 2. V5 — accuracy ledger page contract

### 2.1 What the page answers

Three questions, in scan order, matching DESIGN.md's "numbered sections in scan order":
1. *Can I trust the numbers on this edition?* This is the summary.
2. *Which numbers were checked, against what, and how close were they?* This is the strata and the samples.
3. *What went wrong, and what was done about it?* These are the discrepancies, the tolerance changes and the
   method.

### 2.2 Field list

**Sample record** (one row per checked number; this is also the verifier's output schema, so the page and the
record cannot drift apart). It includes the fourth audit's separate fields: source identity, session,
contract/window, conversion, discrepancy and disposition.

| Field | Example | Notes |
|---|---|---|
| `ledger_edition` | `L-2026-12-02-1` | §2.6 |
| `site_edition` | `2026-12-01T19:24:28Z` | the page's `mirror-market-generated-at` |
| `page` / `page_sha256` | `markets/dalian.html` / `7f05…` | what the reader saw |
| `stratum` | `2 DCE` | 1–9 |
| `series` | `DCE Soybean No.2` | as labelled on the page |
| `displayed_value`, `displayed_unit` | `3978`, `CNY/MT` | **withheld for licence-restricted series** (§2.3) |
| `observation_date` | `2026-12-01` | the date the page shows |
| `contract_or_window` | `b2701 (inferred)` / `Dec loading (Current (prompt))` | inference is stated |
| `fx_date` | `2026-12-01` or `2026-11-28 (3d prior)` | A1 vocabulary |
| `event_class` | `ordinary` / `contract_roll` / `window_change` / `fx_boundary` / `revision` | §3.4 |
| `tier` | `1` / `2 (definition differs: …)` / `3` / `unverifiable` | |
| `kind` | `exact` / `band` / `recompute` | §1.1 |
| `source_identity` | `DCE 日行情 dayQuotes` | the publisher and the document |
| `source_url` | — | |
| `source_vintage` | `trade date 20261201; publication time not stated` | "publication time unknown" is printed where it is unknown (map Notes) |
| `transport` | `same document as fetcher, independent parser` / `independent document` | §0 |
| `source_value`, `source_unit` | `3978`, `CNY/MT` | **withheld where the verifying publisher's terms forbid it** (§2.3) |
| `conversion` | `¢/bu × 36.74371 ÷ 100` | the verifier's own constants |
| `delta_native`, `delta_usd_mt` | `0`, `0.00` | always shown, including for restricted series |
| `tolerance`, `threshold`, `band` | `0 CNY`, `0.5`, `—` | as graded |
| `tolerance_version` | `v1 · 3a7c…` | §1.7 |
| `result` | `pass` / `discrepancy` / `metadata fail` / `unverifiable` / `not yet published` | |
| `material` | `yes` / `no` | |
| `disposition` | `explained` / `unexplained` | |
| `explanation`, `owner`, `issue` | `close vs settlement; inside ≤4-tick band` · `owner` · `#NNN` | an unexplained row links its issue |
| `verifier` | `verify/dce.py@<sha>` or `human (initials)` | never an LLM |
| `verified_at` | `2026-12-02T08:04Z` | shown beside `site_edition`, so the gap between them is visible |

**Stratum row** (section 02, one per stratum): stratum, tier reached, kind, samples this pass / rolling 13 weeks,
pass · immaterial · material-explained · material-unexplained · unverifiable counts, tolerance and threshold,
last checked, and "95 % upper bound on the failure rate" (§3.3), e.g. "0 failures in 39 → under 7.4 %".

### 2.3 Licence-safe row rule

There are two values per row, and they need two different rules:
- **Displayed value.** It is already on the site, and under A18 [#468](https://github.com/philipbergman6-glitch/Mirror-Market/issues/468)
  every rendered series is published with the risk accepted. The map nevertheless fixes "never the displayed
  value" for licence-restricted series. *Proposal:* keep the map's rule, and give the row a link to the block
  where the value already renders. The ledger then adds no new copy, and the reader still reaches the number in
  one click.
- **Verifying value.** It is *not* on the site today. Showing it publishes a second dataset. The rule should key
  on the **verifying publisher's** terms, not the displayed series':

| Verifying source | Show the value? | Basis |
|---|---|---|
| CME Daily Bulletin (stratum 1) | **no**: pass/fail + delta only | V2: CME terms forbid automated reading. Redistribution of settlements is licensed (A5 [#302](https://github.com/philipbergman6-glitch/Mirror-Market/issues/302) licence menu; fee figure UNVERIFIED here) |
| DCE 日行情 | no (default) | terms not established; UNVERIFIED |
| AgRural | n/a — unverifiable, nothing to show | AgRural forbids reproducing its price tables without written permission (§A); the *displayed* series is published under A18's accepted risk |
| CEPEA/ESALQ | yes, with "CEPEA (CC BY-NC 4.0)" — **only while the project is non-commercial**; at sale this flips to no | CEPEA page licence line (§A). Owner call: the NC clause and A18's pre-sale stance |
| MAGyP circular | yes | Argentine government publication; terms UNVERIFIED, so confirm before showing |
| USDA AMS 3147 / 3511 | yes | US federal government work; public-domain status UNVERIFIED in this session |
| ECB reference rates | yes, with "Source: ECB" and "derived cross" | V3: free reuse with citation, modification stated |
| BCB PTAX | yes, with attribution | V3: ODbL |
| CFETS, SARB, FBIL | no | V3: no publication without a licence |

Wording for a withheld cell (12px muted `.caption`, the A1 caption pattern): *"Value withheld — the verifying
publisher's terms do not allow republication. Pass/fail and the USD/MT difference are shown."*

### 2.4 Unverifiable, staleness and the pre-buyer label: proposed wording

- **Pre-buyer label** (summary line under the section head): *"Basket v1 · pre-buyer. The project owner chose
  these nine strata before any physical buyer reviewed them. A buyer session may revise the basket (A12)."*
- **Unverifiable row:** *"Unverifiable — no independent source. AgRural publishes no record of this Paranaguá
  port bid (B21 #483). The site shows it as a physical assessment; it has not been checked."* It is never counted
  as a pass, and its count is shown separately.
- **Not yet published:** *"Not checked — the publisher had not posted 16 Oct by the end of the check window."*
  This is distinct from unverifiable, because it can resolve later.
- **Staleness:** if the newest pass is older than **2 × the cadence** (14 days at weekly), the summary shows the
  warning badge with *"Regime lapsed: last pass 2 Dec 2026, 16 days ago. The results below describe that pass,
  not today's edition."* Old results are never hidden or greyed out. Before any pass has run, the page shows the
  block empty-state pattern: *"No verification pass has run yet."*

### 2.5 Placement: options

| Option | Where | For | Against |
|---|---|---|---|
| **A (recommended)** | Its own page `accuracy.html`. Nav: a third dim utility beside Briefing and How to read. Headline section 12 "About the Data" gains one `<details>` line: "Accuracy ledger: last pass …, 0 unexplained material" | DESIGN.md: "a small fixed set of single scrolling editorial pages", and utilities already exist for cross-cutting pages (Briefing). Citable URL. Does not lengthen the headline | One more page in the promotion contract and smoke (`trust.site_promotion`) |
| B | A section on the headline (a new 12, with About the Data moving to 13) | Seen without a click | The headline is already the heaviest page (wayfinding pass PR #393/#401). A sample table is long |
| C | On `origins` | — | Origins is a DESK task page about one decision. The ledger spans all strata. Rejected |
| A + phase 2 | As A, plus a `.caption` on each basket-grade block (`AVAILABILITY_BASKET_BLOCKS`): "checked 2 Dec · pass" linking to the stratum row | Puts the evidence beside the number | A per-block caption needs its own design approval. Defer until the page exists |

### 2.6 Edition identity

*Proposal:* `L-YYYY-MM-DD-n`. `n` increments if a pass is re-published the same day after a correction. The id
is printed in the page's masthead meta, with the sha256 of the records file (the same practice as V1's page
hashes). Each record cites the `site_edition` it checked, so a reader can say "Accuracy ledger L-2026-12-02-1
found the 1 Dec DCE close exact". The ledger keeps every past edition (a table of contents of passes); it never
overwrites one. This also gives the map's "past-edition replay" (Not yet specified) its edition identity.

### 2.7 DESIGN.md addendum: draft text for the decision log (not applied)

> | 2026-MM-DD | **The accuracy ledger is its own page, a dim utility in the nav, and reuses existing components** | V5 #478. `accuracy.html`, same masthead and nav, sections 01 Summary · 02 Strata · 03 Discrepancies · 04 Samples (collapsed `<details>`, `.dtable`) · 05 Tolerance changes · 06 Method. Verification results get their **own** pill set, because a reused propagation pill would carry two meanings (the drift the key exists to prevent): `pass` outlined `--green`, `explained` neutral badge, `unexplained` solid `--bearish`, `unverifiable` outlined grey like `out of cadence`, `not yet published` neutral. All of them enter `app.wayfinding.how_to_read_vocabulary()`, and `tests/test_wayfinding.py` pins them. Numbers use `.num`. A withheld value is the 12px muted `.caption` "Value withheld — …". A lapsed regime is the warning badge on the summary line, never a page banner. No new colour, no chart. |

### 2.8 V5 — questions for the owner (recommended answer first)

1. **Placement.** *Rec: option A, its own `accuracy.html` as a dim nav utility, with a one-line pointer in
   headline section 12.* Per-block captions are deferred to phase 2.
2. **Withholding rule.** *Rec: key the verifying value on the verifying publisher's terms (table in 2.3), and for
   restricted displayed series link to the block instead of repeating the value.* Alternative: the map's literal
   rule of pass/fail plus delta only for every A18-listed series.
3. **The field list in 2.2.** Is it complete? *Rec: adopt it as the verifier's output schema too.* `transport`
   and `event_class` are the two fields the audits asked for that the map did not list.
4. **Staleness rule.** *Rec: "regime lapsed" after 2 × cadence, results kept visible.*
5. **Edition identity.** *Rec: `L-YYYY-MM-DD-n` plus the records-file sha256, with every pass kept.*
6. **Pill set.** *Rec: a separate verification pill set (2.7), not reuse of the propagation pills.* This is a
   DESIGN.md decision and needs explicit approval.
7. **Counts per stratum.** Should the page show a failure-rate bound ("0 failures in 39 → under 7.4 % at 95 %")?
   *Rec: yes.* It is the honest form of "we checked 39".

---

## 3. V6 — recurring verification regime

### 3.1 Script or human, per stratum

| # | Stratum | Verifier | Independent path | History reachable later? |
|---|---|---|---|---|
| 1 | CBOT ZS/ZL/ZM | **human** (CME Daily Bulletin in a browser, T+1 from 10:00 CT); **script** for ZS only, via the AMS 3147/3046 settlement table (Tier 2, definition: settlement) | CME terms forbid scripting (V2) | CME: **current trade date only** (V1). AMS PDF fixed URL: current only (archive path UNVERIFIED) |
| 2 | DCE | **script**, headless browser (Playwright; DCE has anti-bot, V1) | dce.com.cn ≠ fetcher's Sina/AKShare | yes, by `tradeDate` (V1) |
| 3 | CEPEA | **script** (CEPEA XLS series, V2) | same publisher, independent download | yes, XLS 1997→ (V2) |
| 3 | AgRural | — unverifiable | — | — |
| 4 | MAGyP | **script** (DINEM HTML, or the JSON service V1 saw) | same publisher | by circular number (V1); the HTML lags ≥ 40 h |
| 5 | AMS 3147 | **script** | same PDF as fetcher, independent parser (§0); MARS as an alternative transport (V2: same store) | fixed URL is the current report |
| 6 | FX | **script**: ECB SDMX (CNY exact; BRL/ZAR band), BCB PTAX (BRL Tier 1) | for CNY, same publisher as the fetcher | ECB full history; PTAX by date |
| 7 | Crushes | **script** recompute from the *displayed* legs | page text only | — |
| 8 | Landed | **script** recompute | page text plus the public assumption stack | — |
| 9 | AMS 3511 | **script** (PDF; fetcher uses MARS) | different transport, same store (V1 row 13) | MARS by week |

[I] Eight of nine strata can be scripted at near-zero marginal cost. Only stratum 1 needs a person, and it fits in
a five-minute window each morning (V1's human estimate, UNVERIFIED). The person cannot catch up later, because the
CME page shows the current day only.

### 3.2 Cadence

*Recommendation: census, not sampling, wherever it is free.*
- **Scripted strata: every edition, every displayed basket number** (a census). The order of 50–80 numbers per
  edition is [I, not counted]. Sampling exists to save human time, so there is no reason to sample a script.
  Run after the 19:00 UTC deploy (`deploy-dashboard.yml` cron `0 19 * * 1-5` [O]), and re-run inside each
  source's window (DCE and CEPEA next morning; MAGyP retried until the circular posts, capped at 72 h, then
  "not yet published").
- **Human stratum 1: weekly**, Tuesday–Friday morning, T+1 after 10:00 CT. Each sitting covers every displayed
  ZS/ZL/ZM named month for one session. The ZS rows are also checked daily by script against AMS.
- **Full pass** (all strata, human-read CME, all event classes present): the first one, then monthly. This keeps
  the map's "first pass manual and full".
- **Ledger publication: weekly**, as one `L-` edition summarising the week's census plus the human sitting.

### 3.3 Sample size: the arithmetic

With zero failures observed in *n* independent checks, the 95 % upper bound on the true failure rate is
1 − 0.05^(1/n) (≈ 3/n, the "rule of three") [O, computed]:

| n | 3 | 9 | 13 | 26 | 39 | 52 | 59 | 156 | 299 |
|---|---|---|---|---|---|---|---|---|---|
| bound | 63 % | 28 % | 20.6 % | 10.9 % | **7.4 %** | 5.6 % | 4.95 % | 1.9 % | 1.0 % |

- **Human stratum 1 at weekly × 3 numbers** (ZS, ZL, ZM front named month): 39 per rolling 13 weeks gives
  **< 7.4 %**. At 6 numbers per sitting (two months each) it is 78 per quarter, **< 3.8 %** [I, arithmetic].
- **Scripted strata on a census**: at roughly 250 sessions a year and several numbers each, a stratum passes 299
  checks inside a quarter. Zero failures then support **< 1 %**. The caveat [I]: numbers from one edition are
  not independent of each other (one parser bug hits them all), so the honest unit of *n* is **sessions**, not
  numbers. 63 sessions a quarter gives **< 4.7 %** per stratum.
- **Minimum per pass, per stratum:** ≥ 1 number in every stratum every week, and ≥ 1 per event class when one
  occurred (§3.4).

### 3.4 Event-class strata: how often each event happens, and how to sample it

Frequencies are measured where the repo can measure them [O §B `events.py`, `revisions.py`, `rev_cb.py`].

| Event class | Definition | Measured or known frequency | Sampling proposal |
|---|---|---|---|
| **Contract roll** | a session where the displayed named contract changes (CBOT), or the DCE crush's main month changes | DCE main-by-OI switches in the last 12 months, within the 5 captured contracts per product: No.2 6, meal 1, oil 1 [O; capture window limits this]. CBOT: ZS lists 7 months a year and ZL/ZM 8 (UNVERIFIED in this session — contract calendars not re-read), so roughly 7–8 roll sessions per product per year [I] | **event-triggered census**: every roll session is checked, inside its source window. Roughly 2 a month on CBOT alone, so almost every weekly pass includes one |
| **Shipment-band change** | (a) a band change within a loading month (like-for-like), (b) a loading-month roll | MAGyP rolls loading month **11.8× a year** [O DESIGN.md B39 row]; the AMS "Current" slot rolls monthly [I]; within-month band changes are more frequent (MAGyP's exact nearest band held a median of 2 sessions [O DESIGN.md B39 row]) | (a) census, because it happens most weeks; (b) every roll checked, and the check includes that the day-over-day move is withheld with its reason |
| **FX-date boundary** | the displayed price date has no same-date FX row (A1's prior-close fallback), or a London-day edge (B20) | CNY: **3 sessions in 12 months** (2025-12-25, 2025-12-26, 2026-04-03) [O]. BRL vs CEPEA: **0** in 183 sessions [O] | **too rare for a weekly quota.** Event-triggered census, plus **replay**: the first full pass re-verifies the three known 2025–26 boundary sessions from committed history. Each check confirms the caption "struck at FX … (Nd prior)" and the ≤ 3-day cap |
| **Revised publication** | the publisher (or provider) re-prints an already-stored session | `contract_bars` soy: **347 Close revisions in 30 days**, median lag 1 day, max 10 [O]; `gulf_bids`: 59 changed rows across 53 snapshot commits since 2026-08-25, concentrated on 2026-10-06/07/19, none in the last 30 days [O]; `brazil_spot_prices`, `argentina_fob`: 0 [O]; CONAB/USDA revisions are outside the price basket | **detector**: diff consecutive `data/history/` snapshot commits for same-key value changes (the method in §B). Every changed basket row becomes a mandatory check of the *edition that displayed the old value* against the publisher's later print. The record carries both prints |

[I] Of the four classes, only FX boundaries fail to occur most weeks. For that class a weekly quota would mostly
be unfillable, and the honest rule is "every event, when it happens" plus replay of past events.

### 3.5 Verifier independence boundary

*Proposal:*
- A separate top-level package, `verify/`. It may import the **standard library** plus third-party HTTP, parsing
  and browser libraries (requests, pandas, openpyxl, a PDF text extractor, Playwright). It may import **nothing**
  from this repo: not `fetchers/`, `pipeline/`, `pricing/`, `trust/`, `analysis/`, `app/`, and not `config`.
- **Displayed values come from the rendered page** (`docs/` HTML or the live site), never from the DB. The
  ledger checks what readers saw. This is provider-agnostic, as the map locked, and it survives the CBOT revision
  effect in §1.5.
- **Conversion constants:** the verifier carries its own exact constants (36.74371 bu/MT; 2204.62262 lb/MT;
  0.90718474 MT/st; 16.6667 sc/MT), each citing its primary source (§A). A test in `tests/`, which may import
  both sides, pins them to `pipeline/units.py` within 2 × 10⁻⁶ relative. That test is itself a small
  verification of the product's unit table. **Invariant 7 ("unit conversion has two sites only") would need a
  one-line amendment:** two sites in the product, plus the verifier's independent copy, pinned equal by test.
  This is an owner decision (V6-Q4).
- **Enforcement:** a pytest walks `verify/**/*.py` with `ast`. It fails on any `import` or `from` whose top-level
  name is a repo package, and on any import of an LLM SDK (`anthropic`, `openai`, …). No new dev dependency:
  `import-linter` is not in `requirements-dev.txt` [O], and the repo already enforces structure with
  stdlib-only tests (e.g. `tests/test_connection.py` forbids Turso symbols [O]).
- **No LLM in the path:** no model call in `verify/`, and the human step is a person reading a page. An agent
  may *run* the scripts (as V1's agent did), but it may not decide a result.

### 3.6 The human part: who does it, and where results go

- **Who:** the owner, or a named delegate, initialled in `verifier`. No trading book is needed: the CME
  read is public-page reading.
- **Where:** a CSV per sitting under `verification/records/YYYY-MM-DD-human.csv`, in the §2.2 schema, committed
  **by PR** and reviewed. It must **not** live under `data/history/`: the `history-guard` CI job fails any PR
  that edits that directory, and only the deploy workflow writes there (invariant 6). Script records go to the
  same directory, from a scheduled workflow that commits like the history snapshot does.
- **Aid:** a small `verify/cme_entry.py` that prints the displayed values to read off, and writes the CSV from
  typed input. It is a form, not a checker, and it does no grading of its own beyond the arithmetic.

### 3.7 What a failure triggers

| Outcome | Ledger | Signal | Deploy | Issue |
|---|---|---|---|---|
| pass / immaterial / material-explained | row | — | — | — (immaterial on an exact dataset → `backlog:integrity` defect, V4-Q5) |
| **material · unexplained** | red row, linked issue | `warning` in the next briefing | **not blocked** | auto-opened `backlog:integrity` + `accuracy-discrepancy`, assigned to the owner. **Explain or fix within 5 business days**, or the row's block gets a `.caption` "accuracy check failed on <date>, under investigation" linking to the row |
| same stratum material-unexplained on **2 consecutive passes** | as above | `alert` | not blocked | the block's number is withheld with that reason until it re-verifies (invariant 11: a wrong number is worse than a gap) |
| a pass did not run (script crashed, source down) | "not checked" with the reason | workflow fails loudly (invariant 1) | not blocked | rolled into `ci-dead-source` if it persists for 3 sessions (B34 convention) |
| no pass in 2 × cadence | "regime lapsed" banner on the summary line (§2.4) | — | — | — |

Why not block deploy: a failed check is evidence about the *checked* edition, not the next one. A9's bar is
"drift is reported, never paged" [O `config.py` reliability comment]. The withholding step at the second
consecutive failure is what protects readers.

### 3.8 V6 — questions for the owner (recommended answer first)

1. **Cadence.** *Rec: a census of every displayed basket number by script on every edition; a weekly human CME
   sitting; a monthly full pass; a weekly ledger edition.* Alternative: the map's starting guess, a weekly
   stratified sample for everything.
2. **Unit of n.** *Rec: sessions, not numbers, for the failure-rate bound.* Target ≥ 39 sessions per stratum
   per rolling quarter (< 7.4 %). Does the owner want a tighter target?
3. **Event classes.** *Rec: an event-triggered census for every class, a history-diff detector for revisions,
   and replay of the three known FX-boundary sessions in the first full pass.* A weekly quota cannot be filled
   for FX boundaries (3 a year).
4. **The independence boundary and invariant 7.** *Rec: a `verify/` package with no repo imports, enforced by an
   AST test; its own exact constants, pinned to `pipeline/units.py` by a test; and a one-line amendment to
   invariant 7.* Alternative: let `verify/` import `pipeline/units.py`. That is simpler, but a wrong constant
   would then never be caught.
5. **Where the human writes results.** *Rec: `verification/records/` via PR, never under `data/history/`.*
6. **Failure response.** *Rec: the table in 3.7: never block deploy; 5 business days to explain or fix; withhold
   the number after two consecutive unexplained-material passes.*
7. **Who reads CME.** *Rec: the owner, weekly, Tue–Fri after 10:00 CT.* If no human is available in a given
   week, the stratum reads "not checked (human step not run)". This must never be filled with Tier 2 AMS data
   for ZL/ZM, which AMS does not republish (V2).

---

## A. Publisher facts (primary sources)

Gathered 2026-11-25 by a separate research pass from primary sources; **no cmegroup.com URL was fetched**
(terms; V2). Labels: VERIFIED = primary text or data read; VERIFIED-snippet = wording from a search snippet of the
primary document (used for the CME rulebook PDFs, which could not be fetched); UNVERIFIED = secondary or memory;
[I] = arithmetic. The per-item results are in the §1.2 table; sources:

| Item | Source(s) |
|---|---|
| CBOT ticks and sizes | CME rulebook CBOT ch. 11, 12, 13 — `cmegroup.com/rulebook/CBOT/II/{11,12,13}/{11,12,13}.pdf` (VERIFIED-snippet; no revision date visible) |
| CBOT settlement window and rounding | CME Client Systems Wiki, Soybeans and Soybean Oil pages, "Updated Dec 31, 2025" — `cmegroupclientsite.atlassian.net/wiki/spaces/EPICSANDBOX/pages/457090434/Soybeans` (window VERIFIED; VWAP + tick-rounding wording VERIFIED-snippet). Expiring contract settles on 12:00:00–12:01:00 CT |
| On-tick cross-check | `ams.usda.gov/mnreports/ams_3147.pdf`, "Grain Report for 11/24/2026 - Final": ZS settles 1234.00, 1237.25, 1243.00, 1247.50, 1228.75, 1191.50, 1184.50 ¢/bu — all multiples of ¼ |
| Conversion factors | USDA ERS Agricultural Handbook 697 — `ers.usda.gov/media/7780/ah-697.pdf` (VERIFIED; note it prints 2,204.622 in one table and 2,204.623 in another); NIST Handbook 44 Appendix C (2023) (VERIFIED-snippet) |
| DCE ticks | DCE trading rules pages for 黄大豆2号 (第八/十条 "最小变动价位为1元/吨"), 豆粕, and the English contract pages for No.1 and soybean oil (VERIFIED-snippet; dce.com.cn anti-bot blocks fetches) |
| DCE price limit | Contract pages say 4 % (the floor); DCE notice 大商所发〔2026〕347号 (2026-09-18): 8 % / margin 10 % over the National Day holiday, then "恢复至国庆节前标准" = **6 % / 7 %** — read via Sina Finance's republication (secondary mirror of a primary notice). No later notice found |
| DCE settlement | 《大连商品交易所结算管理办法》第四十条 (2024-03 revision), via an Eastmoney mirror of the DCE file: "当日成交价格按照成交量的加权平均价" |
| CEPEA | `cepea.org.br/br/indicador/soja.aspx` (browser UA; plain fetch 403), Paranaguá methodology PDF `cepea.org.br/upload/kceditor/files/paranagua(1).pdf`, Paraná methodology page; licence line "CEPEA (CC BY-NC 4.0)" |
| AgRural | `agrural.com.br/precossojaemilho/` — unit line, and "Fica vedada … a reprodução total ou parcial das tabelas de preços … salvo mediante autorização prévia e por escrito" |
| MAGyP | Circular PDF `argentina.gob.ar/sites/default/files/circular-precios-fob-oficiales17-01-2025.pdf` (whole numbers per NCM and window); daily page `magyp.gob.ar/mercadosagropecuarios/precios.php` ("FOB: 482 USD/t" on 24/11/2026) |
| AMS 3511 | `ams.usda.gov/mnreports/ams_3511.pdf`, report for 11/16–11/20/2026 ("no report November 27, 2026") |
| ECB | ECB reference-rates page and `eurofxref-daily.xml` for 2026-11-24 (USD 1.0963, BRL 6.4349, CNY 7.3063, INR 101.8670, ZAR 19.2333) |
| BCB PTAX | Olinda `CotacaoDolarPeriodo` 17–24 Nov 2026 (e.g. compra 5.85670 / venda 5.85730 at 2026-11-24 13:07:40); BCB EE042 study and Circ. 3.506 (VERIFIED-snippet) |
| Basis moves (cross-check of §1.3) | USDA AgTransport "Grain Basis" dataset `v85y-3hep`, Soybeans, Gulf–Louisiana, 30-day to arrive, **weekly**, 2007-01-05 → 2026-11-13: median \|week-over-week\| change 6.8 ¢/bu = **2.50 USD/MT** (p90 9.26); last five years 10.0 ¢ = **3.67 USD/MT** (p90 13.0), roll weeks excluded [I; the dataset's futures column looks averaged, so part of that "basis" noise is futures timing]. No primary source states a "typical daily basis move" |

Consistency check [I]: the AgTransport weekly Gulf median (2.5–3.7 USD/MT) is larger than this note's
daily non-zero AMS 3147 move (1.65 USD/MT, §1.3), as a weekly-vs-daily horizon should be. Both put option A's 0.5
USD/MT well under a typical move and option C's 1.0 at a third to two-thirds of one. The research pass itself
suggested ~1 USD/MT as a judgement (above every grid except AgRural's); this note recommends 0.5 because the
basis negotiating unit (1 ¢/bu = 0.37 USD/MT) is the economically meaningful floor and MAGyP is the only
verifiable grid above it.

Not established: whether DCE rounds settlements to tick; the DCE night session hours (broker sites only); a formal
PTAX 4-dp rule; CME rulebook revision dates; reuse terms for DCE, MAGyP and USDA AMS (assumed open for the US
federal and Argentine government documents — UNVERIFIED).

## B. Reproduction

All queries are read-only against the private desk DB copy (`data/storage/mirror_market.db`, newest rows
2026-11-17) or against `git` history of `data/history/`. Run with `.venv/bin/python -I <script> <db-or-repo>`.
The scripts were run from a scratch directory and are not committed. Their logic, in full:

- `moves.py`: for each basket table, sort by the identity key (`contract_bars.ticker`;
  `dce_contract_bars.contract`; `brazil_spot_prices.commodity`; `argentina_fob (position, ship_from, ship_to)`;
  `gulf_bids` slot `Current` with `futures_month` unchanged; `us_processor_cash (commodity, freight,
  trans_mode)` at Illinois). Take `diff()` of the value column, convert with the exact constants above (DCE and
  CEPEA at that session's `currencies` close), and report |Δ| quantiles. Window: `Date >= 2025-11-17`, except
  `brazil_spot_prices` (all rows, from 2026-02-20 or 2026-07-27).
- `events.py`: DCE main contract = argmax `Open_Interest` per (commodity, Date) in `dce_contract_bars`, counting
  changes. FX boundary = CEPEA / DCE session dates with no same-date `currencies` row for BRL/USD and CNY/USD.
  2026-11-17 was excluded as the DB's last day.
- `revisions.py`, `rev_cb.py`: for each commit touching `data/history/<table>.csv` since the date given, `git
  diff -U0 <c>^ <c>`. Parse `-` and `+` rows, match on the table's primary key, and count keys whose value column
  changed by more than 10⁻⁶ relative. Lag = commit date − row session date.
