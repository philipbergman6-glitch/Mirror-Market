# Source reliability record for the A11 re-audit

**A19 · [#518](https://github.com/philipbergman6-glitch/Mirror-Market/issues/518)** on map "The narrowed A" [#296](https://github.com/philipbergman6-glitch/Mirror-Market/issues/296). Input to **A11 · engineering re-audit** [#315](https://github.com/philipbergman6-glitch/Mirror-Market/issues/315), next to `ops-requirements-a11.md` (A9). Evidence as of **2026-10-19**, `main` at a392717. Counts come from code, config, git history and the tracker, never from prose.

This is a requirements note. It changes no code or workflow. The store and the computation are one `backlog:integrity` ticket (§2), which blocks the Integrity readiness gate (#314).

**Why it exists.** The fourth audit (2026-10-19, finding 4) found the 2026-10-18 edition published with 34 of 39 layers successful under a green deploy badge, and SAFEX dead for five runs. A3/B3 made the masthead amber. Nothing tells a one-run blip from a dead source, and no availability or recovery figure exists over time. The audit asks for a 30-trading-session record before an A.

**Bar inherited from A9, not reopened:** drift is reported on the page, never paged. A dead source is an outage, not drift, so the bar allows it to reach the owner, but only through the existing rolling-issue channel (§4).

## 1. The availability basket

**Decision.** The availability basket is the set of layers that **accuracy basket v1** (map #472, charted 2026-10-13) reads from. One basket, two questions: is the number right (#472), and was it there (here). A layer whose absence makes a page thinner but leaves every cargo, basis, origin or hedge decision possible is outside the basket. Its availability is reported, but it never decides A11.

**Labelled pre-buyer**, owner-defined, the same way basket v1 is. A12 (#406) gets a **revise-the-basket hook, not a block**: when buyers name a leg they cannot decide without, or one they never use, the basket changes and the 30-session window restarts for any layer that joined.

| Basket v1 stratum | Layer (`config.PRODUCTION_LAYERS` key) | Cadence |
|---|---|---|
| 1 CBOT ZS/ZL/ZM named-contract closes | `forward_curve` | Daily |
| 2 DCE beans/meal/oil | `dce` | Daily |
| 3 AgRural Paranaguá + CEPEA indicator | `agrural`, `cepea` | Daily |
| 4 MAGyP official FOB | `magyp_fob` | Daily |
| 5 US Gulf basis (AMS 3147) | `gulf_bids` | Daily |
| 6 FX BRL/CNY/INR/ZAR | `currencies` | Daily |
| 7 Board crush (CBOT named) + DCE crush | none. Available when strata 1 and 2 are | — |
| 8 One landed origin→destination row | none. Available when strata 1, 3–6 are (the assumption stack is desk-entered, not a layer) | — |
| 9 US processor cash oil/meal (AMS 3511) | `us_processor_cash` | Weekly |

**Eight layers.** Derived strata (7, 8) get no availability figure of their own. A derived number is available exactly when every leg is, which follows from invariant 8 (one session's number, every leg printed).

**Deliberately outside**, each with its reason:

- `prices` (CBOT continuous front month): not in basket v1 because named contracts carry a month. It is already in `main.CRITICAL_LAYERS`, so its failure fails the pipeline's exit code. That is a stronger rule than this one.
- `contract_bars`: named-contract history for charts and technicals. Today's decision prices come from `forward_curve`.
- India (`india_domestic`, `sea_india`, `sopa_crop`): out of v1 for the same reason as accuracy (private edition, geo-block, Agmarknet dead #449). They join when #472's India strata do.
- `safex`, CONAB, Comex Stat, weather, macro, balance sheets, freight, rivers, cyclones: each degrades a market page or the context frame. None is a leg of a basket number.

**There is already a second list.** `config.TRIAL_CRITICAL_LAYERS` (config.py:4368, 12 layers, read by `analysis/trial/metrics.py` and `scripts/trial.py day`) measures "critical source availability" for the Phase 5 trial. It holds `fred`, `export_sales`, `psd`, `weather` and `prices`, which are context rather than legs, and it lacks `us_processor_cash`. Its comment still says "all 27". Two availability baskets is the parallel-vocabulary drift invariant 3 warns about. **Requirement 1.1:** one tuple, `config.AVAILABILITY_BASKET`, holding the eight layers above. The trial reads it in place of `TRIAL_CRITICAL_LAYERS`, which is deleted, so the trial and A11 measure one thing. A test pins it to basket v1's strata.

## 2. Where the record comes from

**What is persisted today:**

| Source | What it holds | Usable as the record? |
|---|---|---|
| `data/history/data_freshness.csv` | **Current state only**: one row per layer (PK `layer_name`, `pipeline/history.py:52`), overwritten each run. Has `status`, `observed_at`, `last_success`, `keys_returned/expected` | No as a table. **Yes as git history**: 68 snapshot commits since 2026-08-23 can be replayed (§3, preliminary record) |
| `data/history/layer_partial_streak.csv` | The current open `usable_partial` streak per layer (A3 #300 §6), in `AUTHORITATIVE_STATE_TABLES` | No. It is a current-state table, not a log. **Empty means no streak is open now.** It does not mean nothing was recorded |
| `manifest.json` `coverage` | Per-layer `status` + `observed_at` per edition (`app/manifest.py:65`) | No. Overwritten each deploy, not archived (A9 2.2 gap) |
| `ci-alert` issue comments (#398 and 16 predecessors) | Failing layers per run, with run URL | No. Lists hard failures only (no success, partial or `no_publication` rows), and is split across issues closed by each green run. It is the right **surfacing check** (§3 T4), not the store |
| `scripts/trial.py day` | Critical-layer count per day, written to the private trial dir | No. Manual, private, success-only (counts `usable_partial` as down) |

**Why the git replay is not enough:**

1. It captures **the day's final state only**. On 2026-10-13 the deploy ran 7 times and `gulf_bids` failed in two of them, but the final snapshot shows `success`.
2. A run that died before the history commit leaves no trace. That is where "no edition today" ought to show up.
3. It depends on `[skip ci]` snapshot commits never being squashed, and it needs git archaeology for every computation.

**Decision: yes.** The store is a committed, **append-only** `data/history/layer_runs.csv`, written only by the workflows that already write `data/history/` (deploy and retry), per invariant 6. One row per (run, layer):

| Column | Meaning |
|---|---|
| `run_id`, `workflow`, `run_started_at` | GitHub run identity. Shared with A9 2.1's manifest SHA/run-id requirement, so one id joins edition to record |
| `session_date` | The CBOT session the run's edition serves (the date the settlement guard resolved to), not the runner's calendar day |
| `layer_name` | A key in `config.PRODUCTION_LAYER_KEYS` |
| `status` | The `pipeline/grading.py` state as graded, unchanged: `success` / `usable_partial` / `incomplete` / `stale` / `failed` / `no_publication`, plus `skipped_unconfigured` and `not_run`. No new state words (A9 §4) |
| `observed_at` | The publication date observed (the row's `data_freshness.observed_at`) |
| `rendered_at` | That edition's manifest `generated_at`, blank if no edition was promoted |

**Derived, never stored:** age at render (`rendered_at − observed_at`), streaks, recovery sessions. Storing a derived value next to its inputs invites the two to disagree (invariant 2).

**Constraints on the build:**

- **Privacy:** layer states are public (the Layer Freshness table already publishes them). Private layers (`sea_india`, `sopa_crop`) carry only `layer_name` + `status`, the same as the public freshness table does today. No client record touches this table (invariant 4).
- **Not a snapshot table.** `HISTORY_TABLES` export is "PK-keyed snapshot". This needs an append path that refuses to rewrite an existing `(run_id, layer_name)` row.
- **A missing run must not look like a quiet day.** A weekday session with no row at all is `no_run` in the computation, counted as unavailable for every basket layer. Absence never becomes an assumption (invariant 2).
- **Size:** 39 rows × ~2 runs/day × ~260 sessions ≈ 20k rows/year. Fine for a committed CSV.
- **Backfill:** the git replay may seed the store, but every backfilled row carries `run_id = reconstructed:<commit>` so it is never mistaken for a logged run.

**Graduated as [B34 · Source reliability record](https://github.com/philipbergman6-glitch/Mirror-Market/issues/520)** (`backlog:integrity`), natively blocking #314.

## 3. The evidence A11 must see

The computation is one function over `layer_runs.csv` (A9 §4's rule: computed once, existing states only). Its output goes in `manifest.json` beside `coverage` and `latency`, so A11 reads it from the edition and not from a side script.

**Window:** the last **30 weekday CBOT sessions** that ended before the audit, counted on `session_date`. Weekend dispatches add runs, never sessions.

**Per basket layer:**

| Figure | Definition |
|---|---|
| Logged sessions | Sessions in the window with at least one row for the layer |
| Availability % | Sessions whose **last** run graded `success`, `usable_partial` or `no_publication` ÷ 30. `stale`, `failed`, `incomplete`, `skipped_unconfigured`, `not_run` and `no_run` count as unavailable |
| First-run availability % | The same, on each session's **first** run. Shows how much the retry is carrying |
| Longest unavailable streak | Consecutive sessions unavailable |
| Median recovery | Median length, in sessions, of the closed unavailable streaks |
| Open at window end | Whether the layer is unavailable on the last session |
| Surfaced within one run | Share of failing runs (any layer) with a `ci-alert` comment from that same run naming the layer |

Non-basket layers get the same figures, reported, never gating. Edition level: share of weekday sessions with a promoted edition, reported (A9 0.4 already declines a same-day threshold).

**Thresholds below which A11 cannot pass** (pre-buyer, owner-revisable at A12, same status as basket v1):

| # | Threshold | Why this number |
|---|---|---|
| T1 | The record is **continuous**: 30 consecutive weekday sessions logged by the store, with **no gap of more than 2 sessions without a run**, else the window restarts. Shorter gaps stay in the window as `no_run` and count against T2 | A record with holes is not a record. The 2026-09-08→09-18 deploy outage would have reset it |
| T2 | Every basket layer **≥ 90 %** available (≤ 3 of 30 sessions down) | One bad session a fortnight on a decision leg, retries included. Today's replay sits at 93–100 % for the basket, so this is a floor to hold, not a stretch |
| T3 | Every basket layer's **longest streak ≤ 5 sessions**, and every streak of ≥ 3 sessions raised a dead-source alert (§4) | Five sessions is one desk week. Past that a buyer has priced a week of cargoes without the leg. The alert clause tests that the dead-source rule fires, not just exists |
| T4 | **Surfaced within one run = 100 %**, all 39 layers | Invariant 1. One unsurfaced failure is a silent failure |

No paging SLO. Nothing here wakes anyone or reddens a run (A9 bar).

### Preliminary record (git replay, not the A11 evidence)

`git log` over `data/history/data_freshness.csv`, last snapshot per UTC day, last 30 snapshot days (2026-08-25 → 2026-10-18). That covers **40 weekday sessions, 12 of which have no snapshot at all** (2026-08-27, 09-08→09-18 deploy outage, 10-01, 10-02), and one Sunday dispatch (10-18). **It fails T1 by construction.** It is shown to calibrate T2/T3, and as the backfill seed.

| Basket layer | Snapshots | Avail. % | Longest streak | Down on |
|---|---|---|---|---|
| `forward_curve` | 30 | 100 | 0 | — |
| `dce` | 30 | 100 | 0 | — |
| `agrural` | 30 | 97 | 1 | 10-06 |
| `cepea` | 30 | 100 | 0 | — |
| `magyp_fob` | 30 | 97 | 1 | 09-04 |
| `gulf_bids` | 30 | 93 | 1 | 09-24, 10-16 (and two intraday failures on 10-13 the final-state replay hides) |
| `currencies` | 30 | 100 | 0 | — |
| `us_processor_cash` | 11 (layer added 2026-10-05) | 100 | 0 | — |

Outside the basket: `india_domestic` 27 % (open streak 7, #449), `safex` 80 % (open streak 4 snapshot days / 7 runs, B32 #515), `conab`/`conab_precos` 90 % (open, #517), `comexstat` 82 % of 11. `weather` is 77 % `success` but 100 % available (7 `usable_partial` days).

**T4 check, done by hand for the #398 era:** every snapshot from 2026-10-08 on that carried a failed layer (19 runs) had a same-day #398 comment naming every failed layer. 19/19, nothing unsurfaced. The automated figure must reproduce this per run, not per day.

## 4. Does a dead source need a second rule?

**Yes.** The per-run #398 comment fired correctly on every SAFEX failure (§3). The failure was not silence. It was **sameness**: a dead source and a blip produce identical comments, and the only way to see "dead for 5 sessions" is to diff a thread by eye. A3 already solved the same problem for partials. `ci-catalog-drift` opens a distinct rolling issue after `USABLE_PARTIAL_ESCALATION_RUNS = 3` consecutive `usable_partial` runs (config.py:2538, `scripts/ci_layer_alert.py`). Outages need the same treatment.

**Rule (requirement 4.1):**

- **Trigger:** a layer unavailable (§3 definition) on **3 consecutive sessions**. Counted in sessions, not runs: 2026-10-13 had 7 runs, so a run count would call a one-morning blip dead.
- **Scope:** **all 39 layers**, with basket layers marked *decision-impairing* in the issue. The motivating case (SAFEX) is outside the basket. The rule costs the same either way, and a dead non-basket source is still an outage someone has to fix.
- **Channel:** one rolling issue, label `ci-dead-source`, distinct from `ci-alert`. **Commented only on a state change**: a layer crosses 3 sessions, or a dead layer recovers (with its streak length). It is closed when no layer is dead. This adds no per-run notification, so on a bad week the owner gets fewer messages, not more.
- **Computed from `layer_runs.csv`**, not from a second streak table. The streak is a derived value (§2). `layer_partial_streak` stays as A3 built it, and folding it into the record is the build ticket's call, not this one's.
- **Never reddens a run.** It is a relay, like `ci_layer_alert.py`: `continue-on-error`, so a broken alerter never blocks a deploy.

The per-run #398 comment stays as it is. It is T4's evidence.

## Checklist for A11 (one line each)

- [ ] 1.1 one `AVAILABILITY_BASKET` tuple (8 layers, basket v1 strata); `TRIAL_CRITICAL_LAYERS` retired into it; test pins it
- [ ] 2 `data/history/layer_runs.csv` append-only, deploy/retry only, one row per (run, layer), `no_run` sessions detected; backfill rows labelled `reconstructed`
- [ ] 3 reliability figures computed once into `manifest.json`; 30 weekday sessions logged
- [ ] T1 record continuous (no gap > 2 sessions)
- [ ] T2 every basket layer ≥ 90 % available
- [ ] T3 every basket layer's longest streak ≤ 5 sessions; every streak ≥ 3 raised a dead-source alert
- [ ] T4 every failing run surfaced in the same run (100 %)
- [ ] 4.1 `ci-dead-source` rolling issue, 3-session trigger, state-change comments only, never a red run
