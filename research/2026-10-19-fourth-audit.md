# Mirror Market — fourth external audit (findings as received)

Date received: 2026-10-19. Author: external reviewer (shared by the owner).
Checkout audited: main at 2005720 (after PRs #505–#511). Charted onto map
#296 the same day; verification notes at the end are the charting session's.

## The most important remaining findings

1. **Argentina's market-page crush still combines different shipment horizons.**
   The live Argentina page displays a +$35.2/MT crush using beans at $483.0,
   oil at $1,209.4, and meal at $404.3. Those are averages across different
   sets of shipment windows. Meanwhile, the same page's corrected headline bean
   price is $493.0 for October. The arithmetic can be correct while the
   resulting margin represents no coherent shipment period. Confirmed the live
   output and the code path: `Crush.as_source()` does not preserve the
   shipment-window specification. Already tracked in #507. Priority: highest.
   Use a common applicable shipment period across all legs, or withhold the
   margin.

2. **The Gulf basis calculation still depends on an unnamed continuous futures
   contract.** The current Gulf figure looks consistent for the checked
   session, but the implementation subtracts the provider's continuous front
   month rather than the named contract used by AMS for that delivery slot.
   Around a roll these can diverge. A confirmed implementation weakness, not
   evidence that today's displayed basis is numerically wrong. See #512.
   Related historical averages, ranges and indicators still cross
   shipment-window changes without consistently accounting for them: #513.

3. **The hedge workflow contains an incorrect blanket assumption about
   fixed-price physical positions.** Reproduced for a long, fixed-price
   physical exposure: "the price is already fixed, so a futures leg does not
   offset anything". Not generally true for a merchant: inventory purchased at
   a fixed price remains exposed to falling resale prices. The model needs to
   distinguish purchase pricing, sale pricing, inventory ownership and existing
   hedges. CME's hedging guide describes merchants protecting grain between
   purchase and resale, and processors protecting inventory values. Lives in
   `analysis/futures/hedge.py:471`; an existing test requires the warning. An
   example of why 4,000 passing tests do not establish commercial correctness.
   The warning needs correction, and the exposure model needs scenario-based
   review before a buyer trial.

4. **Production success currently hides meaningful coverage degradation unless
   you inspect layer states.** The published October 18 edition reports 34
   successful layers and five failed:

   | Failed layer | Observed failure in the production log |
   |---|---|
   | CONAB crop estimates | TLS certificate hostname mismatch |
   | CONAB prices | Same certificate problem |
   | India mandi prices | API requires captcha/token |
   | SAFEX | Source page returned HTTP 404 |
   | Brazil Comex Stat exports | HTTP 429 rate limiting |

   These failures do not mean retained historical data is wrong or
   unavailable. They do mean a green workflow badge cannot establish fresh,
   complete coverage. The latest eight refresh runs succeeded — a real
   improvement over the October 13 capability table — but a longer,
   source-specific reliability record is required before an A.

5. **Independent accuracy verification is not yet an operating discipline.**
   The hand-verification exercise is the right work. Recurring verification,
   materiality thresholds and the published accuracy ledger remain unfinished.
   The trusted ledger is not serving the production pages. Agreement between
   two parsers reading the same upstream proves implementation consistency,
   not that the upstream observation is correct. This distinction should
   govern the accuracy score.

6. **The physical workflow is implemented more fully than it is proven.** The
   public origins page correctly blocks landed totals because commercial
   inputs are missing — good behaviour — but it cannot yet answer: which cargo
   is cheaper at my destination, for my shipment window and specifications?
   The private workflow has synthetic tests, but no demonstrated buyer-entered
   book or complete real route. Recruitment remains open in #411. Language
   inconsistency: the propagation ledger describes its rows as competing
   physical offers and an arbitrage a cargo can close, while those rows
   include administered values and differently situated price references. The
   briefing is more careful than that ledger caption.

## How close is this to the Bloomberg ambition?

Lack of real-time ticks is not the main obstacle; daily observations can be
entirely appropriate for the stated use. Bloomberg's relevant benchmark is its
combination of supply-demand information, pricing, margins, research and
workflow integration, not refresh speed. Mirror-Market can realistically become
excellent at a narrower job: making the daily physical soy decision
understandable, traceable and reproducible. The remaining distance:

- Consistent commodity, quality, location, shipment-window and contract identities.
- Reliable acquisition of the critical sources.
- Independently demonstrated accuracy.
- Complete private commercial inputs.
- Evidence that buyers make better or faster decisions with it.

Free public information can support much of that. It cannot supply every
desk's freight quote, financing terms, quality adjustment or counterparty
offer; those enter as owned private inputs with dates and expiry, never
estimated silently. The analytical layer also needs development: the briefing
withholds China pace analysis for want of WASDE vintage data; the weather page
reports insufficient history for some deficit assessments; footprint weather
remains specified rather than implemented.

## What would raise the grade most (in order)

1. Close the economic-meaning defects: Argentina crush windows, named-contract
   Gulf basis, window-aware history, hedge exposure semantics, misleading
   comparability language.
2. Stabilise a critical soy basket (beans/oil/meal benchmarks, required FX,
   core physical references, key fundamentals); track completeness and
   recovery over at least 30 trading sessions.
3. Operationalise independent verification: source identity, session,
   contract/window, conversion, discrepancy, disposition; separate explained
   differences from errors.
4. Complete one real private cargo workflow: one destination, one shipment
   period, comparable origins, buyer-owned cost inputs, an independently
   checked hedge interpretation.
5. Run the buyer trial: preparation time, missed material changes, incorrect
   interpretations, unusable alerts, decision reproducibility.
6. Then deepen analytics: release-vintage comparisons, seasonal export pace,
   production-weighted weather exposure, a concise "what changed / why it
   matters / what to check next" screen.

Favour those over additional countries, more technical indicators or faster
refreshes.

## Checks performed by the reviewer

- Local suite: 4,092 passed; one loopback-socket test failed under sandbox
  restrictions and passed outside it. Coverage 87.91 % of the measured
  surface. Ruff passed. Mypy passed across 159 files within configured
  exclusions. Latest production CI: 4,093 passed.
- Public smoke with browser checks failed solely on generation age: the
  checker applies a six-hour promotion window to an edition ~20.6 h old on a
  weekend. Deployment verification and routine weekend monitoring need
  distinguishing.
- No project files changed.

Recommendation: shift the next phase from feature expansion to demonstrated
trading correctness.

## Charting session verification (2026-10-19)

- Finding 1–2: #507, #512, #513 existed but did not block the readiness gate;
  wired natively to #314.
- Finding 3: confirmed at `analysis/futures/hedge.py:471`; `PhysicalExposure`'s
  docstring (lines 145–152) defines LONG as owning the goods, so the alert
  contradicts the model's own definition. Tests pinning it:
  `tests/test_futures_hedge.py:254`, `tests/test_futures_scenarios.py:227`.
  → B31 #514 (defect) + exposure-state question posted on A7 #304.
- Finding 4: SAFEX — `www.grainsa.co.za/pages/industry-reports/safex-feeds`
  301→404; new page `grainsa.co.za/applied-economics/safex-feeds/` answers 200;
  failed every run since 2026-10-14 (#398). → B32 #515. CONAB — port 443 of
  `portaldeinformacoes.conab.gov.br` answers a non-TLS response locally
  (`WRONG_VERSION_NUMBER`), hostname mismatch in CI; upstream, → #517 (bug, not
  gate-blocking). Comex Stat 429 — fetcher already waits out 429s on a
  schedule (`fetchers/comexstat.py:208`); one run, transient, no ticket. India
  → #449 (owner decision pending). Reliability record → A19 #518.
- Finding 5: routed to map #472 (V4–V7); comment posted there.
- Finding 6: caption confirmed at `app/markets.py:164-167`
  (`LEDGER_RULE_STATEMENTS["origin"]`) → B33 #516. Real cargo workflow →
  A15 #411 / A12 #406 (A13 delivered the synthetic route).
- "Deepen analytics" (item 6) is feature work: map #142 and the footprint
  build #380–#386, not this map.
