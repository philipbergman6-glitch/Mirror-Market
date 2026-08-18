# Trial records — private, never committed

This directory holds the trader-validation trial (Phase 5). **Everything in it
except this README is gitignored**, and that is not a convenience: session
records carry a trader's handle, the decisions they reached, their notes and
their evidence, and this repository is public.

```
data/reference/trial/
  sessions/YYYY-MM-DD.yml   one file per trading day, a list of sessions
  days/YYYY-MM-DD.yml       one file per trading day, one observation
```

## Why YAML and not a table

Every persisted table in this project round-trips through `data/history/*.csv`
via `pipeline/history.py`, and those CSVs are **committed to this public
repository** by the daily deploy workflow. A `trial_sessions` table would
therefore publish trader identity by construction, on the first green pipeline
run, with nothing in the schema to suggest it was about to. So trial records are
files this repository never reads and git never sees.

The same reasoning explains the two subdirectories: a session is written by a
trader during the day, a day observation is computed once by the desk after the
pipeline runs. Different authors, different times, different files.

## Getting started

```bash
python scripts/trial.py protocol                        # generate the protocol
python scripts/trial.py start --interactive             # begin a session
python scripts/trial.py day --edition-current           # once per trading day
python scripts/trial.py check                           # validate everything
python scripts/trial.py review --week-start 2026-08-17  # the weekly read
```

Read `docs/trial/PROTOCOL.md` first. It is generated from the same code the
metrics are computed from, so it cannot describe a task differently from the way
the task is measured.

## Rules that are enforced, not just asked for

- **A malformed file raises.** It does not render as an empty trial. A silently
  skipped record is a metric computed over the wrong denominator.
- **An unknown field raises.** A typo'd key would otherwise be dropped and the
  value it carried lost without a word.
- **A naive timestamp raises.** Sessions are logged across continents; a
  timestamp without a timezone cannot be ordered against one that has one.
- **A session that did not complete must carry an issue.** An unexplained
  failure teaches nothing.
- **An external lookup must name the question we could not answer.** That field
  is the point of the trial, and the record refuses to be built without it.

## Use a handle, not a name

Three characters minimum, and pick something that is not a substring of ordinary
English — `assert_no_identifiers` searches all shared output for your handle, so
a handle like `art` would fire on the word "chart" and a handle of two letters
would make the guard useless while appearing to run.

## What may be shared

The **aggregate** projection: counts, rates, classes, grades. It does not filter
the private fields out — it never builds them. Anything you paste anywhere else
should have come from `to_dict(audience="aggregate")` or from
`analysis.trial.sanitize.aggregate()`, which builds it and proves the projection
held in one call.

Never share a `summary`, `evidence`, `notes`, `decision` or
`unanswered_question` field. If a finding needs to reach a public tracker, run
it through the backlog (`scripts/trial.py backlog`) and clear the specific item
first — `issue_body(item, audience="aggregate")` refuses until you do.
