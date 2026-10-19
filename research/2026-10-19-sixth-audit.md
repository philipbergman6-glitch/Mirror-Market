# Mirror Market — sixth external audit (recommendations as received)

Date received: 2026-10-19 (evening). Author: external reviewer (shared by the
owner). Checkout audited: main at e9d21a0 (after the fifth round's PRs
#539, #543, #544, #545, #547, #548). Charted onto map #296 the same day; the
owner passed on the four recommendations only — no findings table came with
them.

## Recommendations as received

1. Close the two bounded calculation issues. Repair expired-period selection
   and briefing snapshot collisions. These need focused fixes, not another
   architectural expansion.
2. Verify the new production edition and recorder evidence. Confirm the latest
   build serves the intended changes, the migration preserves rows, and the
   new ingest-time evidence reaches committed records. Update the capability
   table. The run I checked supports part of #537, but does not complete its
   full verification.
3. Let the reliability record accumulate while completing accuracy
   verification. Finish materiality thresholds, the recurring sample and the
   published ledger. Include contract rolls, shipment-band changes, FX-date
   boundaries and revised source publications — not just ordinary days.
4. Start one real private cargo workflow in parallel. Choose one destination
   and shipment period, enter owned commercial inputs, and have a physical
   buyer independently reproduce the landed comparison and hedge
   interpretation. This is now a more valuable product test than adding
   another market.

## Charting session's disposition (2026-10-19, checkout e9d21a0)

| Recommendation | Where it landed | Notes |
|---|---|---|
| 1 · briefing snapshot collisions | #549 (existing, `backlog:integrity`, blocks #314) — relabelled ready-for-agent | `analysis/briefing/snapshot.py::_argentina_fob_block` keys `curve` on `ship_from`; B39 has merged so the file is free. |
| 1 · expired-period selection | B41 #550 (`backlog:integrity`, blocks #314) | **Pointer inferred, not cited.** The one deliberate selection of an ended period is `nearest_window_key`'s all-sailed fallback (`analysis/origins/sources.py:224`), pinned by `tests/test_window_headlines.py:237`. Alternatives ruled out in the ticket; fixer confirms first. |
| 2 · edition + recorder evidence | A22 #551 (AFK task, child of #296, blocks A11 #315); comment on A21 #537 | First `layer_runs.csv` rows exist (commits `2007832`, `f98a4ea`); `ARCHITECTURE.md` reliability row still says "no run has recorded yet". |
| 3 · accuracy verification | Map #472 — V6 #479 body amended with event-class strata; V4/V5 unchanged; V7 #480 already blocks #315 | No new ticket. |
| 4 · real cargo workflow | A23 #552 (HITL task, child of #296, blocked by A15 #411); pointers on A7 #304 and A8 #305 | Owner has no book; inputs are the recruited buyer's, under A6 terms. |

Not adopted as new work: nothing — every recommendation mapped onto an
existing ticket or one new one. No grilling session (autonomous charting on
the second→fifth audit precedent).
