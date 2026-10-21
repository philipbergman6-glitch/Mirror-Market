# A21 · layer_runs record vs deploy run logs

**A21 · [#537](https://github.com/philipbergman6-glitch/Mirror-Market/issues/537)** on map [#296](https://github.com/philipbergman6-glitch/Mirror-Market/issues/296). Feeds A11 [#315](https://github.com/philipbergman6-glitch/Mirror-Market/issues/315). Checked on 2026-10-21 against `main` at f3a5bc8.

This is a read-only comparison. `data/history/` was not edited. These are working notes; the resolution comment on #537 is the summary.

## Scope

The record holds six runs, all `Update Dashboard` on `main`, each with 39 rows:

| run_id | event | `run_started_at` | session | snapshot commit | record commit | B38 era |
|---|---|---|---|---|---|---|
| 39250830567-2 | push | 2026-10-19T12:46:52Z | 2026-10-16 | a8218af | 2007832 | pre |
| 39256327033 | push | 2026-10-19T13:10:04Z | 2026-10-16 | 749389c | f98a4ea | pre |
| 39270866701 | push | 2026-10-19T14:49:54Z | 2026-10-16 | d8118d7 | 85ba758 | post |
| 39299083914 | push | 2026-10-19T18:15:33Z | 2026-10-16 | 4bc25e5 | a864c19 | post (not promoted) |
| 39336923844 | schedule | 2026-10-19T22:54:40Z | 2026-10-19 | 7ecbba1 | 3417652 | post |
| 39504764196 | schedule | 2026-10-20T22:11:16Z | 2026-10-20 | 45d56ff | f3a5bc8 | post (bonus 6th run) |

Each record commit's parent is that run's own snapshot commit. The record step's log shows `HEAD is now at <snapshot>`.

**Every run that reached the pipeline has a record.** `gh run list --workflow deploy-dashboard.yml --branch main` lists 16 runs (17 attempts) between B34's merge (PR #529, 2026-10-19T12:16:52Z) and this check:

- **Recorded:** the six runs above, the only ones not cancelled.
- **Cancelled, not recorded:** the other 11 attempts: 10 runs, plus attempt 1 of 39250830567, which was cancelled in `check`. 39268582661 and 39297389160 were cancelled mid-pipeline. The rest never reached the pipeline. None of the 11 attempts reached *Commit history snapshots* or *Deploy*, and the record step's `!cancelled()` guard skips a cancelled run by design.
- **No other workflow wrote to the record.** No `repository_dispatch` retry ran in the window: 39514703224 *Retry Failed Pipeline* logged `No retry: too late in the UTC day (23:23)`.

## Sources per run

- **Log:** `gh run view <id> --log`, 0.5 MB each. The checks used:
  - `PIPELINE_STARTED_AT`
  - `Run evidence written to`
  - the *Verification Summary* block (`Succeeded (n/39)`, `Usable partial`, `Upstream/ingest failure`, `Incomplete key coverage`)
  - `wrote manifest.json`
  - `PUBLICATION LATENCY: generated …` (the public smoke reading the live manifest)
  - `layer_runs: run …` (the recorder's own tally)
  - `DEPLOYMENT_OUTCOME`
- **Steps:** `gh run view --json jobs`; attempt 2 of 39250830567 via `actions/runs/39250830567/attempts/2/jobs`.
- **`pipeline-status` artefact** (`pipeline_status.json`), for all six runs. It carries `run_evidence` from 39270866701 onwards.
- **The run's own `data/history/data_freshness.csv`** at its snapshot commit.
- **`github-pages` artefact:** only 39504764196's survives (12303553529), so `manifest.json` was compared row by row for that run only. For the other promoted runs, `rendered_at` is checked against the public smoke's `PUBLICATION LATENCY: generated <stamp>` line. That line is the `generated_at` of the manifest actually served.
- **`ci-alert` [#398](https://github.com/philipbergman6-glitch/Mirror-Market/issues/398) comments** since 2026-10-19, matched by their `Run:` URL.

Comparison script: compare each record row with the snapshot's `data_freshness` row (status, `observed_at`, and `last_attempt` ≥ `run_started_at`), with the `pipeline_status` classification, with the evidence roster and with `layers_requested`.

## Row-level results

| Run | Rows | Status vs summary + freshness | `observed_at` vs freshness | `rendered_at` | `session_date` | `skipped_unconfigured` / `not_run` | Alert receipts vs #398 |
|---|---|---|---|---|---|---|---|
| 39250830567-2 | 39 | 39/39 (37 success, weather `usable_partial`, india_domestic `failed`) | 37/37 public match | `12:55:20.891647` = served (smoke) | 2026-10-16 ✓ | 0 / 0 | `india_domestic` = comment 6267709003 |
| 39256327033 | 39 | 39/39 (same shape) | 37/37 | `13:18:42.811304` = served | 2026-10-16 ✓ | 0 / 0 | `india_domestic` = 6268176199 |
| 39270866701 | 39 | 39/39 (same shape) | 37/37 | `15:00:33.036209` = served | 2026-10-16 ✓ | 0 / 0 | `india_domestic` = 6270325856 |
| 39299083914 | 39 | 39/39 (38 success incl. weather, india_domestic `failed`) | 37/37 | blank × 39, not promoted (see below) | 2026-10-16 ✓ | 0 / 0 | `india_domestic` = 6274169897 |
| 39336923844 | 39 | 39/39 (34 success, weather `usable_partial`, agrural/india_domestic/sopa_crop `failed`, epa_rfs `incomplete`) | 37/37 | `23:12:36.124001` = served | 2026-10-19 ✓ | 0 / 0 | 4 = 6279402485 (agrural, india_domestic, sopa_crop, epa_rfs) |
| 39504764196 | 39 | 39/39 (36 success, weather `usable_partial`, india_domestic `failed`, epa_rfs `incomplete`) | 37/37; manifest `coverage` 38/39 (see B46) | `22:23:09.277433` = served = artefact `edition.generated_at` | 2026-10-20 ✓ | 0 / 0 | 2 = 6303107997 (india_domestic, epa_rfs) |

**Totals:** 234 rows (195 for the ticket's five runs) and 10 alert-receipt rows. Every one of the 39 rows per run matches on status, `observed_at`, `rendered_at` and `session_date`.

**Identity and shape:**

- **No duplicates.** 0 duplicate `(run_id, layer_name)` in `layer_runs.csv` and 0 in `layer_alerts.csv`.
- **Content is append-only.** Each record commit's rows are a prefix of the final file's.
- **`run_started_at`** = the logged `PIPELINE_STARTED_AT` in all six runs.
- **`workflow`** = `Update Dashboard/<event>` in all six runs.
- **The attempt suffix is right.** `-2` appears only on the re-run.

**`session_date`.** Recomputed with `pipeline.layer_runs.session_date_for`. The guard is CBOT close 14:30 America/Chicago = 19:30 UTC on 2026-10-19. The four Monday runs before 19:30Z serve Friday 2026-10-16, and the log's CBOT `latest close … date: 2026-10-16` agrees. The 22:54Z Monday run serves 2026-10-19, and the 22:11Z Tuesday run serves 2026-10-20.

**Alerts.** Receipts are exactly the `failed` + `incomplete` rows of each run. `usable_partial` is not alerted per run, by design (A3 escalation). *Flag a missing record* was `skipped` in all six runs, and *Record layer runs* was `success` in all six.

**Dead-source relay.** The relay fired on 39504764196: [#566](https://github.com/philipbergman6-glitch/Mirror-Market/issues/566) reads `india_domestic: unavailable 3 consecutive sessions (2026-10-16 → 2026-10-20)`. That is consistent with the record, where india_domestic is `failed` on sessions 10-16, 10-19 and 10-20. agrural (1 session) and epa_rfs (2 sessions) correctly did not trigger it.

## B38 caveat (pre-B38 runs 39250830567-2, 39256327033)

Neither failure mode B38 names could have produced a wrong row in these two runs:

1. **Rebased roster.** No code commit landed between the run's head and the record step's checkout:
   - `git log ebd8a37..a8218af` is only the run's own snapshot commit.
   - `git log ad6eab7..749389c` is the same.
   - Each run's 39 rows equal its own `layers_requested` from `pipeline_status.json`, in order.
2. **Keyless environment.** It only mislabels a layer the run asked for but never graded. Every row of both runs has `last_attempt` at or after `run_started_at` in that run's own `data_freshness` snapshot, so every layer, keyed ones included, was graded by the run itself. That leaves 0 `skipped_unconfigured` and 0 `not_run`.

So the caveat is vacuous for these two runs. For the four post-B38 runs, `run_evidence` was present in each, with a 39-layer roster equal to the rows in order. All five catalog keys show `configured: true` and `private` = [sea_india, sopa_crop].

## 39299083914: the edition that was not promoted

The steps ran as follows:

- *Verify candidate promotion contract* → `FAIL: browser timed out at 1440px: index.html`.
- *Deploy* was skipped, and *Classify deployment outcome* gave `DEPLOYMENT_OUTCOME: skipped`.
- *Enforce publication outcome* failed, so the run went red. The `ci-deploy-alert` issue [#565](https://github.com/philipbergman6-glitch/Mirror-Market/issues/565) opened on this run and closed on 39336923844.

The record labels this honestly. `rendered_at` is blank on all 39 rows, the contract's "no edition promoted", while statuses are the run's own grades.

Two consequences follow, both by A19 §3's definitions rather than discrepancies:

- **The last run of session 2026-10-16 is this non-promoted run**, so session-level availability for 10-16 is read from a run whose pages never served. weather is `success` here, but `usable_partial` in the served 39270866701 edition, and both count as available. Source availability (did the layer arrive?) and edition share (`pipeline/reliability.py:201`, sessions with any `rendered_at`) are deliberately separate figures. Session 10-16 still counts as an edition session through 39250830567-2, 39256327033 and 39270866701.
- **The run's history snapshot 4bc25e5 was committed before the smoke failed.** That is the existing save-first rule, and the served edition stayed 39270866701's until 39336923844.

## Discrepancies

| # | What | Disposition | Traces to B38? |
|---|---|---|---|
| 1 | **Private-layer stamps.** The record blanks `sea_india`'s `observed_at`, as `PRIVATE_LAYERS` requires. The public surfaces publish it: manifest `coverage` has `observed_at: 2026-10-16T00:00:00`, the `index.html` Layer Freshness row reads `sea_india fresh 2026-10-16 23:59Z 2026-10-20 22:23Z`, and `data_freshness.csv` has the same. `config.py:171-173` ("…the same as the public freshness table") and A19 §2 are therefore false. | **Filed [B46 #567](https://github.com/philipbergman6-glitch/Mirror-Market/issues/567)** (`backlog:integrity`, sub-issue of #296, blocks #314, owner decision: dates public or private) | No |
| 2 | **First three record commits show deletions.** 2007832 → f98a4ea → 85ba758 have numstat `79 40` and `78 39`. This is the CRLF→LF rewrite. | **Already filed and fixed.** B42 [#555](https://github.com/philipbergman6-glitch/Mirror-Market/issues/555) shipped as PR #559 (467ad68, 17:00Z). The three post-fix commits a864c19, 3417652 and f3a5bc8 show `39 0` and keep the parent's bytes as an exact prefix. The file holds 39 CRLF + 196 LF lines; the content is unchanged, per [#555](https://github.com/philipbergman6-glitch/Mirror-Market/issues/555) | No |
| 3 | **Five public layers carry a blank `observed_at` on `success`:** `usda`, `psd`, `wasde`, `crush_inspections`, `conab`. | **Explained.** Their `data_freshness.observed_at` is NULL in every one of the last 60 snapshots, and manifest `coverage` shows `None`. The record copies a NULL faithfully (invariant 2), and none is a basket layer. Age at render is undefined for them; that is a property of the layers, not of the record | No |
| 4 | **weather sits in both `usable_partial` and `catalog_drift`** in the 39256327033 and 39270866701 summaries. | **Explained.** `catalog_drift` is the A3 escalation after `consecutive_runs` ≥ 3 (3 and 4 here), not a grade. The graded word is `usable_partial`, as recorded | No |
| 5 | **Fast-refresh editions are served but not recorded.** The live manifest at check time was `generated_at 2026-10-21T01:48:55Z`, Refresh Prices 39529314195. | **Explained.** A19 §2 scopes the store to the deploy and retry workflows. `rendered_at` names the full edition the run promoted | No |
| 6 | **11 cancelled attempts have no rows.** | **Explained.** Superseded by the next push, with no snapshot and no deploy. The sessions they would have served all have rows from later runs | No |

**Nothing traces to B38.**

## Result

The record matches the deploy logs on every row of all six runs: 234 rows and 10 alert receipts. One new discrepancy, outside the record's own correctness, was filed as B46 #567. The 30-session clock for A11 can keep accumulating on this record. Two things remain unobserved:

- a run whose pipeline crashed, the `not_run` / evidence-file path
- a run with an unset key, the `skipped_unconfigured` path
