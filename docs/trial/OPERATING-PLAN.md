# validation — operating plan

Companion to `docs/trial/PROTOCOL.md`. The protocol is generated from
`analysis/trial/` and defines *what* is measured; this file is hand-written and
defines *who does it, on which day, and what the desk does each morning*. It
changes no threshold, no task definition and no issue class — if anything here
disagrees with the protocol, the protocol wins.

## Status checked 2026-10-08

The trial has **not started**. No local JSON/JSONL session or day records were
present at inspection. No participant onboarding or commercial outcome is claimed.
Protocol v2 remains the authority; recruitment is tracked in #411 and the
buyer-approved first-screen contract in #406.

All five technical failure drills passed again with `python scripts/trial.py
drills`. The participant halves below remain open. The synthetic private-route
integration test in `tests/test_origins_page_operational.py` also passed: US Gulf
to North China, September 2026 shipment, CIF barge to FOB vessel bridge, freight,
insurance, duty/VAT, destination port, finance and quality, landed ranking and
sensitivity, plus a separately assumed named-contract hedge reference. Every
input is a fixture. This validates software mechanics, not a cargo decision.

## The window

Start the 30-trading-day window only after two independent physical buyers have
committed, signed participation letters, chosen private handles, and one has
approved the first-screen contract. Set the actual dates then; the old August
2026 grid was a planning example and did not record attendance.

Use the CBOT business-day calendar for trading days and the publisher calendar
for release days. These are different calendars. The workstation links the
[USDA WASDE schedule](https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report).
Recheck it when setting the window and after any publisher rescheduling notice.
Do not infer publication from a scheduled date or invent a task-4 session if a
release falls outside the actual trial window.

## Private-route onboarding before day 1

Supply the actual origin and destination, product grade and quality specification,
shipment and pricing windows, delivery terms/carrier and cargo quantity. Enter
owned, dated, expiring freight, elevation, insurance, destination port, finance
(rate and days) and quality assumptions in `data/reference/assumptions/private/`.
Use explicit zero only when confirmed, never to fill an unknown. Verify public
policy rates and their expiry separately. Keep records and rendered desk output
outside `docs/`; never copy the test fixture set into production assumptions.

For the hedge reference, separately state physical exposure direction, pricing
convention, named contract, hedge ratio/rounding and any FX exposure. A landed
comparison alone cannot establish these. Review residual tonnes, basis and FX
risk alongside the contract count. The drill's 10,000 MT and 73 ZSX26 contracts
are invented examples, not suggested inputs.

## Who

Minimum two **participants** at the decision floor (`config.TRIAL_MIN_PARTICIPANTS = 2`;
terms decided by A6, #303). A participant is a **physical buyer**:

- prices or books physical soybean, meal or oil product at least weekly —
  procurement, origination, or the physical desk at an importer, crusher, feed
  miller or trading house; futures-only traders, brokers and analysts do not
  qualify;
- currently pays for at least one of the tools this product claims to displace
  (terminal, broker portal, subscription assessment) — otherwise the external
  lookup count measures habit rather than substitution;
- can commit ≤15 min/day of form-filling plus one 30-min weekly debrief.

Unpaid in both directions. They get a private desk edition for the trial and six
months after, and a vote on the first-screen contract (A12); the warmest recruit
is the **design partner** who settles that contract before day 1. Each signs a
one-page **participation letter** (what is recorded, where it lives, aggregate
only is shared, right to withdraw and be deleted). No NDA. Recruitment is A15
(#411); names and firms never appear on the tracker.

They must be **independent of each other** — same desk is acceptable, same book
is not, because two participants reading one position produce one opinion twice.

Each picks a handle: 3+ characters, not a substring of ordinary English
(`assert_no_identifiers` greps shared output for it — `art` would fire on
"chart"). Record the handle→person mapping nowhere in this repository.

## Per-week load

Straight from the protocol's cadences, per participant per week:

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

Task 4 is event-driven on the verified release day. Task 8 is the five drills, below.

Over six weeks that is roughly 115 sessions per participant against the
**decision floor** of 20 sessions / 5 tasks / 8 real decisions per participant
(`config.TRIAL_DECISION_FLOOR`) — the floor is not the binding constraint,
attendance is. **A missed day is recorded as a missed day**; it is never
backfilled from memory, because a session reconstructed after the fact cannot
honestly report its own external lookups.

Sessions are captured on a ~2-minute form and transcribed weekly by the desk
with `python scripts/trial.py transcribe <export.csv>`; participants never touch
the repository. A transcribed session takes its release stamp from that day's
`trial.py day` observation, so **the day record must exist before the week's
transcription** or those rows are refused.

## Task 9 — random audits

The number is chosen **by the participant, at the moment of the session**, from
whatever page they happen to have open. Do not pre-select the numbers and do not
let the desk suggest one: an audit list assembled by the people who built the
product tests the numbers they already trust. Three per participant per week, ~36 per
participant over the window.

## Task 8 — the five drills

`scripts/trial.py drills` runs all five with no network, no production database
and no write into `docs/`. Mechanisms rerun successfully on 2026-10-08;
participant validation remains outstanding:

| Drill | Mechanism | Participant half |
|---|---|---|
| `critical_source_outage` | **pass** — `status='failed'`, no fresh `last_success` | open |
| `partial_key_coverage` | **pass** — 14/19 records coverage, 13/19 demotes to `incomplete` | open |
| `stale_payload` | **pass** — `status='stale'`, rows stored, `last_success` held back | open |
| `page_generation_failure` | **pass** — dated tombstone, promotion contract rejects candidate | open |
| `deployment_failure` | **pass** — candidate refused, last good edition stays live, no private path in the contract | open |

The assertions only prove the mechanism fired. **The drill's actual result is
the participant's blind read** — show the degraded surface to a participant who has not
been told what broke and record what they can name. All five participant halves are
outstanding, and no drill counts as run until its participant half is recorded.

Place one drill per failure mode across the actual window, avoiding verified
release days. Assign dates only after the trial start is known.

Record the day with `--drill <name>` so it leaves the reliability metrics: a
deliberate outage counted as downtime would understate the product's real
availability, which is the mirror image of the mistake the protocol refuses in
the other direction (`upstream_outage` is not a correctness class).

Do not tell the participants which day is a drill day, or the blind read is not blind.

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
usefulness, futures usefulness, opportunity usefulness, UX, participant trust — each
stating its own arithmetic, each scoring nothing rather than a default where the
observations are short. Then the go/hold/no-go against the $20,000/year
single-client question, with the two overrides applied: an open blocker is a
no-go, and fewer than 2 participants at the decision floor returns
`insufficient` rather than a verdict. A participant below the floor is listed
in the private review with each shortfall — reported, not graded.
