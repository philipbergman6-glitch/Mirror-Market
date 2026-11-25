# Trusted path: go/kill decision record

**Decided 2026-11-25** by the owner on [#239](https://github.com/philipbergman6-glitch/Mirror-Market/issues/239) ([decision comment](https://github.com/philipbergman6-glitch/Mirror-Market/issues/239#issuecomment-7171599007)). The owner adopted an independent advisory review of the AI-written decision pack on the same issue. Built by [#312](https://github.com/philipbergman6-glitch/Mirror-Market/issues/312) (re-scoped to this one PR) and [#656](https://github.com/philipbergman6-glitch/Mirror-Market/issues/656). Sits beside the RFC `docs/plans/2026-08-10-data-trust-foundation.md` and is referenced from `CLAUDE.md`'s `trust.registry` line, as #239's acceptance asks.

**Late.** #239's acceptance asked for this record "written before the go date, not after it". The go date the original ticket proposed was **2026-09-08**. That date was missed: no record existed until 2026-11-25.

## The decision

**The reader cutover does not proceed for now. The `reconcile-trusted` job stays on probation as a drift detector. At the 2027-03-01 checkpoint the default is excise.**

| #239 question | Answer |
|---|---|
| 1 · Go date | None. The reader cutover is not proceeding. It is reopened only by the `ROADMAP.md` trigger (below), not by a date. |
| 2 · What "go" authorises | The alarm only: a divergence opens or comments on a rolling `ci-reconcile-divergence` issue, on state change. It does **not** redden the Update Dashboard run, and `reconcile-trusted` does not gate the publish. No reader moves. No hard-source pilot 4. |
| 3 · Kill criterion, and what kill means | The checkpoint text below. Kill means **excise**: delete `trust/` and its tests, the reconcile job and scripts, and the `open_provider` switch branch, then tag the commit. `trust/site_promotion.py` is load-bearing (the promotion gate in both deploy workflows) and moves out of `trust/` first. Ideas already harvested into v1 stay: the same-PK quarantine in `_save()`, `clean_ohlcv` OHLC logging, the success-state vocabulary, `fetchers.yfinance.download_bars`. |
| 4 · Where recorded | This file. |

### Checkpoint, 2027-03-01

Text as decided:

> excise on 2027-03-01 unless (a) the alarm surfaced a real defect that no v1 guard, `ci-alert` or the accuracy verifier surfaced, or (b) the ROADMAP trigger fired.

### What was built (#312, #656)

1. **Alarm by a rolling issue.** `scripts/ci_reconcile_alert.py` follows the `scripts/ci_dead_source.py` pattern and comments only on a state change. A red deploy run must keep meaning "the edition did not go out". The job and its steps stay `continue-on-error`. `ARCHITECTURE.md` → "Trusted path — status and alarm" covers the state model.
2. **CNY is left out of the alarm** (G5). `scripts/reconcile_fx.py` runs it but reports it as "not compared: v1 source differs", from the same set that refuses its cutover: `trust.read_path.V1_SOURCE_DIFFERS`.
3. **#656, raise.** With the switch on, an absent `data/v2/` or a ledger with no accepted revision for a soy leg makes `open_provider` raise `TrustedLedgerEmptyError` with a named reason. Before this it served empty curves silently. Observed with the switch on: the workstation page tombstones with that reason and `generate_site` exits 1.
4. **This record**, and a `ROADMAP.md` entry for the reader cutover with its trigger.

### Owner answers to the pack's questions

- **Q1 · Ratify #481: yes.** `ohlc.relationship` is a WARNING by deliberate policy, on the #447 evidence (12,033 of ~124k Yahoo bars have the close outside their own range; PYG 52 %). `ohlc.complete` stays at REJECT. This was a raised finding, not a rule weakened to make reconciliation pass, so it meets #194's clause.
- **Q2 · Keep the DT-16 cutover criterion**, including "at least one quarantine", attached to the ROADMAP trigger. It guards a cutover, and no cutover is authorised. It may never fire naturally (0 quarantines in 124 retained logs).
- **Q3 · Reconcile when `check` is red: no.** `needs: check` stays. A commit that fails its tests deploys nothing, and reconciling it would invite false alarms. The cost is G2: a missed session is never reconciled later.

### Reversal

- **Toward a real go:** the ROADMAP trigger fires.
- **Toward kill now:** the alarm PR needed more than a workflow edit plus the #656 guard, or `trust/` breaks an unrelated PR's CI before March.

### Later, not urgent

Move `trust/site_promotion.py` out of `trust/` before the checkpoint, once the open PRs have drained, so a kill becomes a pure delete. That move is not part of this PR.

## Evidence: every `reconcile-trusted` run, 2026-08-11 → 2026-11-25

Recorded here because the evidence expires (G1). Reports are CI artifacts only (`data/reconciliation/` is gitignored), and GitHub deletes logs after 90 days, so everything before 2026-08-27 is already exit-code-only. This table is from the owner pack on #239. The pack classified each run from `gh run list --workflow deploy-dashboard.yml` (276 runs), the jobs API, the retained logs (124) and the `trusted-reconciliation` artifacts. The 276 per-run rows stayed outside the repo. The independent review the owner adopted re-counted that per-run file to these totals; it did not re-pull the runs from GitHub.

| Pilot | First ran | Eligible runs | Step ran | Reconciled (log) | Exit 0, log expired | **Diverged** | Unavailable (exit 2) | Not run |
|---|---|---|---|---|---|---|---|---|
| MAGyP FOB | 2026-08-11 | 257 | 176 | 119 | 52 | **0** | 5 | 81 |
| CBOT named contracts (DT-16) | 2026-08-19 | 227 | 151 | 122 | 27 | **2** | 0 | 76 |
| Required FX (4 pairs) | 2026-08-23 | 202 | 132 | 114 | 8 | **10** | 0 | 70 |

- **"Exit 0, log expired"** covers runs from 08-11 to 08-26. Exit 0 rules out a divergence and an outage, but it cannot tell "reconciled" from "no session". All 17 failing steps have retained logs.
- **"Not run"**: the test suite failed, so the job was skipped (`needs: check`): 27–28 runs. Job cancelled: 24–30. Superseded push cancelled: 10–13. Run cancelled before the job existed: 9–10.
- **Distinct sessions** in the retained window (08-27 to 11-24): MAGyP 48 sessions, all clean. FX 47 sessions, 3 ever diverged. CBOT 49 run-dates, 2 ever diverged.
- **Since #481** (2026-10-13 13:55 UTC to 2026-11-25): CBOT 72/72 reconciled, FX 72/72 reconciled (26 sessions), MAGyP 69 reconciled and 3 unavailable (27 sessions).
- **All 12 divergences fall on 5 sessions between 08-28 and 10-13.** 4 of those 5 sessions have one cause: Yahoo candles with the close outside their own range (#447), cleared by #481.

| # | Run (date) | Pilot / session | What | Cause |
|---|---|---|---|---|
| 1 | 33449099517 (08-31) | FX CNY/USD 08-28 | v1 1 row, trusted 0 | Close < Low; `ohlc.relationship` REJECT before #481 (#447 class) |
| 2 | 37702094754 (10-07) | FX ZAR/USD 10-07 | v1 1, trusted 0 | Close > High; same class |
| 3–10 | 8 runs, 10-12 09:41 → 10-13 13:42 | FX ZAR/USD 10-09 | v1 1, trusted 0 | Yahoo revised a closed bar to Close < Low (#447); fixed by #481 |
| 11 | 38335926439 (10-13) | CBOT meal, session 10-12 | ZMZ26 only in v1 | `rejected_by_rule: {ohlc.relationship: 1}`, before #481 |
| 12 | 37543135239 (10-06) | CBOT meal, session 10-06 | ZMV26, ZMF27, ZMK27 only in v1 | **Not recoverable**: the report predates `rejected_by_rule` (#427) and the ledger was never uploaded |

Exit-2 runs were MAGyP only. Two were upstream transport failures (09-04, 11-19). Three hit a pilot-side non-JSON abort (#527) that PR #615 fixed on 2026-11-25.

**What the green proves is small (G6).** Both paths parse one shared download, and since #481 they apply the same severity to the rule behind 4 of 5 divergent sessions. "Reconciled" shows the adapters are still wired alike. It does not show the number is right. That is the accuracy verifier's job (#639 → V7).
