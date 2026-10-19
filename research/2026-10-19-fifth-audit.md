# Mirror Market — fifth external audit (findings as received)

Date received: 2026-10-19. Author: external reviewer (shared by the owner).
Checkout audited: main at 4d56a4c (after PRs #519–#529). Charted onto map
#296 the same day; verification notes at the end are the charting session's.

## Findings as received

The completed edition I inspected reported 37 successful layers, weather partially available, and India failed, compared with five failed layers during the previous assessment. That is encouraging recovery evidence, though one edition does not establish sustained reliability.

### Standards

The independent standards review found one correctness defect, two workflow risks, and one minor convention issue.

1. The "shared" crush window is not necessarily shared by all three products. The selector independently checks whether oil and meal overlap the bean period. That does not guarantee oil and meal overlap each other. Replayed over the stored Argentina observations: six of 462 returned selections had no common three-way intersection. Example 2026-07-22: beans July–September, oil July, meal August–September — no period shared by all three, yet a usable crush selection is returned. The live October example does have a common intersection. Fix: select a compatible combination with a nonempty three-way intersection, label the period it actually supports, withhold otherwise; add the July example as a regression case (`shared_crush_band`, `analysis/origins/crush.py:315`).

2. Reliability recording has two conditional workflow risks. (a) The earlier history-commit step can rebase the working checkout onto newly merged code, so the recorder can use a different layer roster from the code that performed ingestion. (b) API keys are scoped to the pipeline step; the recorder does not receive that configuration evidence, so an ungraded keyed layer can be classified `skipped_unconfigured` when it was configured but never reached. Neither established as a live incident. Preserve the original run's roster and configuration explicitly.

Minor: a new signal threshold outside `config.py`; can wait behind the correctness work.

### Spec

1. The reliability basket omits a feed now required by Gulf basis: it excludes `contract_bars`, but the corrected Gulf basis reads its named-contract close from that table. The basket can pass while an important decision number is unavailable. Add `contract_bars`, then distinguish source availability (feed returned usable data), calculation availability (basis or crush computed with compatible dates, contracts and windows), and desk readiness (required private inputs present and current). Successful feeds alone do not establish a usable cargo comparison.

2. The 30-session continuity test can pass before the required restart period finishes. The spec says a gap longer than two sessions restarts the evidence window; a three-session gap followed by only 28 logged sessions still returned `T1_continuous.met == True` — the rolling window trims the gap instead of enforcing the restart (`pipeline/reliability.py:145`).

Hedge fix status: the misleading warning is corrected; a complete model of purchases, sales, inventory and existing hedges remains deferred — not yet a validation of net exposure for a real merchant book.

### Recommended next

1. Finish this correctness round (three-way crush intersection, reliability restart, basket dependency, recorder evidence).
2. Collect trustworthy operational evidence: verify the first committed run records against production logs, let the 30-session history accumulate, keep the capability-status table current (much still describes 2026-10-13).
3. Complete independent accuracy verification — materiality thresholds, recurring process, accuracy ledger — covering economic identity (contract, shipment period, terms, FX date) as well as numerical agreement.
4. Validate one real cargo workflow with an experienced buyer before expanding the desk workflow.
5. Resolve the Argentina window-selection policy deliberately (#531): a stable economic comparison rule, original quotes preserved, disclosure when comparable observations disappear — not smoothing for the chart.
6. Then improve the trader's first screen: can a buyer quickly see what changed, which cargo economics it affects, what evidence supports it, what to check next.

## Charting session's verification (2026-10-19, checkout 4d56a4c)

| Finding | Verified | Evidence | Filed |
|---|---|---|---|
| Three-way crush intersection | Confirmed in code | `crush.py` 340–360: oil and meal each tested against `bean_window` only | B35 #532 → gate #314 |
| T1 restart boundary | Confirmed in code | `reliability.py`: `sessions = every[-window:]` then gap measured inside the cut | B36 #533 → gate #314 |
| Basket omits `contract_bars` | Confirmed in code | block 04 → `open_provider().close_history` → `providers.py:302` reads `contract_bars`; `AVAILABILITY_BASKET` lists `forward_curve` (separate table, `store.py:1002` vs `:1064`) | B37 #534 → gate #314 |
| Recorder roster / keys | Confirmed in workflow | `deploy-dashboard.yml`: `git pull --rebase` at ~289 precedes the record step at ~434; keys are `env:` on the pipeline step only; `layer_configured()` reads `os.environ` | B38 #535 → gate #314 |
| Threshold outside `config.py` | Not located | grep of B34 and #513 follow-up diffs found no new constant | note on B38 |
| Argentina window policy | Owner decision | #531 measurement | A20 #536 (HITL) → A11 |
| First-record verification | Task | B34 merged 2026-10-19, records accumulating | A21 #537 (AFK) → A11 |
| Three measures; first screen; accuracy identity; one cargo; hedge status; table currency | Routed | — | #406, #472, #411, #304, #315 |
