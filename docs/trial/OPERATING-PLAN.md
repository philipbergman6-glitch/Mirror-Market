# Trader validation — operating plan

Companion to `docs/trial/PROTOCOL.md`. The protocol is generated from
`analysis/trial/` and defines *what* is measured; this file is hand-written and
defines *who does it, on which day, and what the desk does each morning*. It
changes no threshold, no task definition and no issue class — if anything here
disagrees with the protocol, the protocol wins.

## Status at time of writing (2026-08-19)

| Piece | State |
|---|---|
| Protocol v1.0.0 | generated, current |
| Record harness (`scripts/trial.py`) | verified: `check`, `metrics`, `scorecard` all refuse to produce a number with zero records |
| Five failure drills | **mechanism half passed 5/5** (`scripts/trial.py drills`) — see below |
| Session records | **none** |
| Day observations | **none** |
| Traders onboarded | **none** |

The trial has **not started**. Days 1–30 begin on the first trading day after
two professional soy traders are onboarded and have handles.

## The window

30 trading days on the CBOT calendar (`analysis.futures.domain.is_business_day`,
which excludes Labor Day, Mon 2026-09-07). Anchored to the earliest possible
start:

| # | Day | # | Day | # | Day |
|---|---|---|---|---|---|
| 1 | Thu 2026-08-20 | 11 | Thu 2026-09-03 | 21 | Fri 2026-09-18 |
| 2 | Fri 2026-08-21 | 12 | Fri 2026-09-04 | 22 | Mon 2026-09-21 |
| 3 | Mon 2026-08-24 | 13 | Tue 2026-09-08 | 23 | Tue 2026-09-22 |
| 4 | Tue 2026-08-25 | 14 | Wed 2026-09-09 | 24 | Wed 2026-09-23 |
| 5 | Wed 2026-08-26 | 15 | Thu 2026-09-10 | 25 | Thu 2026-09-24 |
| 6 | Thu 2026-08-27 | 16 | Fri 2026-09-11 | 26 | Fri 2026-09-25 |
| 7 | Fri 2026-08-28 | 17 | Mon 2026-09-14 | 27 | Mon 2026-09-28 |
| 8 | Mon 2026-08-31 | 18 | Tue 2026-09-15 | 28 | Tue 2026-09-29 |
| 9 | Tue 2026-09-01 | 19 | Wed 2026-09-16 | 29 | Wed 2026-09-30 |
| 10 | Wed 2026-09-02 | 20 | Thu 2026-09-17 | 30 | Thu 2026-10-01 |

If the start slips, shift the whole grid — never compress it. Thirty trading
days is the sample, not a deadline.

**One WASDE falls inside this window: Fri 2026-09-11 (day 16).** Task 4 is
therefore a single occurrence, and it is the only day in the trial that cannot
be rescheduled. A window that starts later than 2026-09-11 must be checked
against the next release (Fri 2026-10-09) or task 4 scores nothing.

## Who

Minimum two professional soy traders (`config.TRIAL_MIN_TRADERS = 2`). What
"professional" has to mean for a finding to count:

- prices, hedges or executes physical soybean, meal or oil business as their job;
- currently pays for at least one of the tools this product claims to displace
  (terminal, broker portal, subscription assessment) — otherwise the external
  lookup count measures habit rather than substitution;
- can commit ~45 min/day for six weeks.

They must be **independent of each other** — same desk is acceptable, same book
is not, because two traders reading one position produce one opinion twice.

Each picks a handle: 3+ characters, not a substring of ordinary English
(`assert_no_identifiers` greps shared output for it — `art` would fire on
"chart"). Record the handle→person mapping nowhere in this repository.

## Per-week load

Straight from the protocol's cadences, per trader per week:

| Task | Per week |
|---|---|
| 1 morning brief | 5 (daily) |
| 2 origin comparison | 2 |
| 3 crush + hedge | 2 |
| 5 China reconciliation | 1 (export-sales release, Thu) |
| 6 weather | 2 |
| 7 counterparty/opportunity | 2 |
| 9 price/calculation audit | 3 |
| 10 ticket review | 2 |
| **total** | **19** |

Task 4 is event-driven (day 16 only). Task 8 is the five drills, below.

Over six weeks and two traders that is roughly 230 sessions against a floor of
10 — the floor is not the binding constraint, attendance is. **A missed day is
recorded as a missed day**; it is never backfilled from memory, because a
session reconstructed after the fact cannot honestly report its own external
lookups.

## Task 9 — random audits

The number is chosen **by the trader, at the moment of the session**, from
whatever page they happen to have open. Do not pre-select the numbers and do not
let the desk suggest one: an audit list assembled by the people who built the
product tests the numbers they already trust. Three per trader per week, ~36 per
trader over the window.

## Task 8 — the five drills

`scripts/trial.py drills` runs all five with no network, no production database
and no write into `docs/`. Run on 2026-08-19 against `feat/gtr-transport`:

| Drill | Mechanism | Trader half |
|---|---|---|
| `critical_source_outage` | **pass** — `status='failed'`, no fresh `last_success` | open |
| `partial_key_coverage` | **pass** — 14/19 records coverage, 13/19 demotes to `incomplete` | open |
| `stale_payload` | **pass** — `status='stale'`, rows stored, `last_success` held back | open |
| `page_generation_failure` | **pass** — dated tombstone, promotion contract rejects candidate | open |
| `deployment_failure` | **pass** — candidate refused, last good edition stays live, no private path in the contract | open |

The assertions only prove the mechanism fired. **The drill's actual result is
the trader's blind read** — show the degraded surface to a trader who has not
been told what broke and record what they can name. All five trader halves are
outstanding, and no drill counts as run until its trader half is recorded.

Place one drill per failure mode across the window, avoiding day 16 (WASDE):

| Drill day | Mode |
|---|---|
| 4 (Tue 2026-08-25) | `critical_source_outage` |
| 10 (Wed 2026-09-02) | `partial_key_coverage` |
| 15 (Thu 2026-09-10) | `stale_payload` |
| 21 (Fri 2026-09-18) | `page_generation_failure` |
| 28 (Tue 2026-09-29) | `deployment_failure` |

Record the day with `--drill <name>` so it leaves the reliability metrics: a
deliberate outage counted as downtime would understate the product's real
availability, which is the mirror image of the mistake the protocol refuses in
the other direction (`upstream_outage` is not a correctness class).

Do not tell the traders which day is a drill day, or the blind read is not blind.

## Daily desk runbook

Every trading day, in order, after the pipeline lands (~20:00–24:00 UTC):

```bash
python main.py                                   # or confirm the CI run landed
python scripts/generate_site.py
python scripts/trial.py day --edition-current    # drop the flag if it did not rebuild
python scripts/trial.py check                    # must exit clean
```

`trial.py day` is required **every** trading day, including days nobody ran a
session — those are exactly the days the product broke, and skipping them is how
availability quietly measures itself only on the days it worked.

On a drill day, add `--drill <name>`.

## Weekly

Mondays, on the prior week:

```bash
python scripts/trial.py review --week-start <Mon>
python scripts/trial.py backlog
```

Review Mondays: 2026-08-24, 08-31, 09-07, 09-14, 09-21, 09-28, and a final on
2026-10-05 covering days 27–30.

The weekly output is: what worked, what failed, the top unmet questions, the
metric trend against the prior week, recommended changes, and a go/no-go for
wider use. Findings promote to a ranked backlog through `trial.py backlog`;
anything reaching a public tracker goes through
`issue_body(item, audience="aggregate")`, which refuses until the item is
cleared.

## The stop rule

**Any `blocker`-severity numerical or semantic issue stops the affected surface
until it is fixed.** Concretely: the page comes down or the block renders an
empty state with its reason, sessions against that surface are suspended, and
the fix ships before they resume. The suspension days are still recorded as day
observations. An open blocker at the end of the window is a **no-go whatever
the rates say** — that override sits above the arithmetic in
`config.TRIAL_DECISION_THRESHOLDS` and is not negotiable against a good score
elsewhere.

A correctness issue may not be filed `minor`; the record type refuses it.

## Confidentiality

Binding, and restated here because this file is public while the records are
not:

- Records live in gitignored `data/reference/trial/`; private output in
  `data/workspace/trial/`, outside `docs/` and absent from
  `trust.site_promotion.expected_site_paths()`.
- Handles only. No names, positions, counterparties, cargoes, prices shown, or
  commercial decisions in any field that leaves the desk.
- Only the `aggregate` projection is shareable — it does not filter the private
  fields out, it never builds them.
- Nothing about this trial goes in a `data/history/*.csv`. Every table in this
  project round-trips through those files and they are committed publicly, which
  is why the trial is files rather than a table.

## Final output

After day 30:

```bash
python scripts/trial.py scorecard
```

Nine dimensions — precision, accuracy, reliability, timeliness, physical
usefulness, futures usefulness, opportunity usefulness, UX, trader trust — each
stating its own arithmetic, each scoring nothing rather than a default where the
observations are short. Then the go/hold/no-go against the $20,000/year
single-client question, with the two overrides applied: an open blocker is a
no-go, and fewer than 2 traders or 10 sessions returns `insufficient` rather
than a verdict.
