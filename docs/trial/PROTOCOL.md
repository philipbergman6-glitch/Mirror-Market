# Mirror Market — trader validation protocol v1.0.0

*This document is generated from `analysis/trial/` by `python scripts/trial.py protocol`. Do not edit it by hand: the task definitions, issue classes and thresholds below are the same objects the metrics are computed from, and editing the prose would put the instructions and the measurement out of step without either one complaining.*

## What this trial measures

One question: **does Mirror Market reduce external terminal, broker and spreadsheet use without increasing decision risk?**

Two halves, and both must hold. A product that halves the lookups while producing one wrong number a trader would have sized off has failed, and the metrics are built so that it cannot pass by trading one against the other — correctness rates are graded separately from lookup counts and neither is blended into a single score.

## Shape of the trial

- **30 trading days.**
- **At least 2 professional soy traders.** One trader's habits are not a finding.
- **At least 10 sessions** before any metric is graded at all; below that the metric reports `insufficient` rather than a number.
- **Ten recurring tasks**, listed below. Each is framed as a *decision*, not as a page to look at: the test is whether the trader can answer the question, not whether the page loaded.
- **One session record per task attempt**, including the attempts that fail. An abandoned session is data; a session nobody logged is not.
- **One day observation per trading day**, whether or not anyone ran a session. This is the only way availability and deployment reliability get measured, because the days the product broke are exactly the days nobody logs a session.

## The ten tasks

### 1. Pre-open / morning brief

- **Cadence**: every trading day
- **Time target**: 10 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** What moved overnight, what repriced, what has not printed yet, and which of those changes anything I hold or intend to do today?

**Success.** The trader can state the overnight move in the soy complex, name at least one market that has NOT yet repriced, and say whether the day's plan changes — without opening a second tool first.

### 2. Origin comparison for a real shipment window

- **Cadence**: at least twice a week
- **Time target**: 20 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** For a named destination and a real shipment window, which origin is cheapest landed, by how much, and what would have to be true for that to flip?

**Success.** A ranked landed cost with every cost component visible, the shipment window stated, and the trader able to name the one input that most moves the ranking.

### 3. Physical crush and hedge scenario

- **Cadence**: at least twice a week
- **Time target**: 25 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Given a physical crush position, what is the board margin, what hedge would cover it, and what does the position lose under a stated shock?

**Success.** A sized hedge in whole contracts against a stated tonnage, a named contract month with its expiry and first notice day, and a shocked P&L the trader can reproduce from the numbers on the page.

### 4. USDA / WASDE event response

- **Cadence**: each WASDE release day inside the window
- **Time target**: 20 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** What did the report change versus the prior month, and is the board's reaction consistent with the revision?

**Success.** Month-over-month revisions for the US and world balance sheets, with stocks-to-use, read off the product within the session; the trader can say whether the move looks over- or under-done.

### 5. China demand and shipment reconciliation

- **Cadence**: weekly, on the export-sales release
- **Time target**: 15 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Do committed sales, actual inspections and the Dalian board agree about Chinese demand, and where do they disagree?

**Success.** Outstanding sales and shipped-to-date for China stated with their own report dates, set beside the DCE import parity, with any disagreement named rather than averaged away.

### 6. Weather-risk response

- **Cadence**: at least twice a week
- **Time target**: 10 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Which growing region has moved outside its own normal, and does the board already carry a premium for it?

**Success.** A named region with an anomaly stated against its own history, and an explicit judgement on whether price has already responded.

### 7. Counterparty / opportunity identification

- **Cadence**: at least twice a week
- **Time target**: 15 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Is there a lane worth working today, who would be on the other side of it, and what is stopping it?

**Success.** A ranked lane with named candidate counterparties, its blockers stated, and the trader able to say whether it is workable today or merely worth a phone call.

### 8. Deliberate data-source and deployment failure drill

- **Cadence**: five drills across the window, one per failure mode
- **Time target**: 15 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** When a source dies, a payload freezes, a page fails to build or a deploy fails, does the product say so — and is the last good edition still what the trader sees?

**Success.** The trader, shown a degraded edition without being told which drill ran, can name what is missing and say whether they would still trade off the page.

### 9. Price and calculation audit

- **Cadence**: at least three times a week, on a number chosen by the trader
- **Time target**: 15 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Pick a displayed number at random: what exactly is it, where did it come from, and can the trader reproduce it?

**Success.** The number's product, venue, price type, currency, unit, contract or window, and observation date are all recoverable from the product, and any derived figure reproduces by hand from its stated inputs.

### 10. Proposed hedge / trade-ticket review

- **Cadence**: at least twice a week
- **Time target**: 10 minutes (a target, not a benchmark — this project has no instrumented terminal session to compare against)

**The decision.** Is this proposed ticket one the trader would send to a broker, and if not, what is wrong with it?

**Success.** Every leg carries a named contract, a side, a quantity in whole contracts and a price basis; the trader states accept, amend or reject with a reason.

## What to record, every session

| Field | Meaning |
|---|---|
| trader | Your handle. Never a full name. See confidentiality below. |
| task | One of the ten above. |
| trading_day / start / end | The session's own clock. Timestamps must carry a timezone. |
| outcome | `completed`, `abandoned`, `blocked` |
| decision or output | What you concluded. Required when the outcome is `completed`. |
| pages used | Which Mirror Market pages you actually opened. |
| external lookups | Every time you left the product — see below. |
| missing / stale / wrong | Logged as issues, classified. |
| false and missed alerts | Logged as issues, classified. |
| confidence | 1–5. How much you trust the answer you reached. |
| would act | Would you place, size or price a real trade off this? |
| notes and evidence | Free text. Private. |

A session that did **not** complete must carry at least one issue saying why. That rule is enforced by the record type, not by review: an unexplained failure is the one record that teaches nothing.

## External lookups

Every time you go outside Mirror Market to finish a task, log it — with **the question the product could not answer**. That question is the single most valuable output of this trial, and the record refuses to be saved without one. A lookup count tells us a trader left; the question tells us why, and it is the input to the backlog.

Tools: `bloomberg`, `broker`, `spreadsheet`, `exchange`, `refinitiv`, `news`, `colleague`, `other`. Use `other` only with the tool named in text.

## Issue classification

| Class | Meaning |
|---|---|
| `numerical_error` | A displayed number is arithmetically wrong, or disagrees with the source it claims to come from. |
| `semantic_mismatch` | The number is right but is not what it is labelled as — a farmgate price shown as FOB, a last trade shown as a settlement, a bid shown as a price. |
| `stale_data` | The value is past its own source's cadence and the product did not say so. A value labelled stale is not this; that is the product working. |
| `missing_coverage` | The question is a reasonable one for this product and it has no answer at all. The most common issue class, and the one that becomes the roadmap. |
| `misleading_ux` | The number is correct and the trader read it wrongly anyway. Treated as a product defect, not a user error. |
| `workflow_friction` | The answer was there and took too many steps, too many pages, or a manual calculation to reach. |
| `false_alert` | An alert or signal fired and the trader, having checked, judged there was nothing there. |
| `missed_alert` | Something happened that this product should have flagged and did not. Only recordable against a stated expectation, never in hindsight alone. |
| `upstream_outage` | A source was genuinely down or dark. Recorded to measure availability, and explicitly not counted as this product being wrong. |
| `requested_enhancement` | Nothing is broken; the trader wants something that does not exist. Kept separate from missing coverage, which is a gap in what is already claimed. |

The first three are **correctness** classes. One occurrence promotes straight to the backlog with no corroboration needed, because being wrong once is already the finding. `upstream_outage` is deliberately *not* one of them: a source being down is measured as availability, and counting it as this product being wrong would make an honest outage look like a defect.

## Severity

| Severity | Meaning |
|---|---|
| `blocker` | A trader could place or size a real trade wrongly off this. Stop the trial for this surface until it is fixed. |
| `major` | The task cannot be completed in the product, or the answer needs an external check every time. Fix inside the window. |
| `minor` | Friction, polish, or a gap the trader routed around without risk. |

A correctness issue may not be filed as `minor`. The record type refuses it: if a number was wrong, the question is how wrong, not whether it mattered.

## Confidentiality — binding

- **Do not publish trader names, positions, counterparties, contact notes or commercial decisions.** Anywhere, in any form.
- Use a **handle**, not a name, in the `trader` field. Three characters minimum. Pick something that is not a substring of ordinary English — the leak guard searches free text for it.
- Trial records live in `data/reference/trial` and are **gitignored**. They are YAML files, not database rows, specifically because every table in this project round-trips through `data/history/*.csv`, which is committed to a public repository. A trial table would publish trader identity by construction.
- Generated private output goes to `data/workspace/trial` — outside `docs/`, and absent from the site promotion contract, so it can never reach GitHub Pages.
- Anything shared outside the desk is the **aggregate** projection, which does not build the private fields at all and is checked by a recursive guard before it is written.
- Do not put a position, a cargo, a price you were shown, or a counterparty name in a `summary` field. Those go in `notes` and `evidence`, which never leave the private record.

## Decision thresholds

These are the bars. They live in `config.TRIAL_DECISION_THRESHOLDS` so that "why did it say no-go" is a lookup rather than an argument.

| Metric | Go at | No-go at | Direction |
|---|---|---|---|
| `task_completion_rate` | 0.9 | 0.7 | higher is better |
| `external_lookups_per_task` | 1.0 | 2.5 | lower is better |
| `wrong_or_stale_rate` | 0.02 | 0.1 | lower is better |
| `false_alert_rate` | 0.05 | 0.2 | lower is better |
| `missed_alert_rate` | 0.05 | 0.2 | lower is better |
| `would_act_rate` | 0.75 | 0.5 | higher is better |
| `median_confidence` | 4.0 | 3.0 | higher is better |
| `deployment_reliability` | 0.95 | 0.85 | higher is better |
| `critical_source_availability` | 0.95 | 0.85 | higher is better |

A metric between the two bars is `hold`. Two overrides sit above the arithmetic: **any open blocker is a no-go** whatever the rates say, and a window with fewer than 2 traders or 10 sessions returns `insufficient` rather than a verdict.

## Weekly and final output

- **Weekly**: what worked, what failed, the top unmet questions, the metric trend against the prior week, recommended changes, and a go/no-go for wider use. `python scripts/trial.py review`.
- **Final**: a 30-day scorecard across precision, accuracy, reliability, timeliness, physical usefulness, futures usefulness, opportunity usefulness, UX and trader trust. Every dimension states the arithmetic behind it, and a dimension without enough observations scores nothing at all rather than a default. `python scripts/trial.py scorecard`.

## Reproducibility

Every session is stamped with the git commit and a fingerprint of the layer freshness table at the moment it ran. `python scripts/trial.py reproduce <session-id>` reports whether that result can be produced again, and what has moved if not. A session run against uncommitted changes is recorded as **not reproducible** rather than refused — a hotfix session is a legitimate session — and the share of findings arriving on unreproducible builds is itself a reported metric.

## Failure drills

Five drills simulate a critical source outage, partial key coverage, a stale payload, a page-generation failure and a deployment failure, then check that the product degrades the way it claims to. Run them with `python scripts/trial.py drills`. They touch no production database, write nothing into `docs/`, and make no network call.

Each drill carries a **trader prompt**: show the degraded surface to a trader who has not been told what broke, and record what they can tell. That answer is the drill's real result; the assertions only prove the mechanism fired.

---

*Protocol v1.0.0. Generated from `analysis/trial/domain.py` and `config.py`.*
