# Clearing statements

The **official** P&L, as printed on your clearing or broker statement. It sits
beside the figure this project computes and is never merged with it.

Files here are **gitignored**. A statement carries an account number, a broker
name and somebody's position; nothing in this directory but this README is
tracked, and nothing here is written to `docs/`.

## Why two numbers

Two numbers describe the same book every evening and they are not the same
number:

* the **clearer** marks against the exchange settlement and reports what the
  account actually owes;
* **this project** marks against a delayed daily close and reports what the
  desk thinks it made.

Both are worth having. Averaging them, or letting the statement quietly
overwrite the mark, throws away the only thing that makes either trustworthy —
the difference, and the reason for it. So the reconciliation shows both columns
and their difference, and there is no third, "reconciled" figure anywhere: it
would belong to neither desk and would be acted on as both.

A settlement printed on a statement **you** supplied is classified
`ATTESTED_SETTLEMENT`: authoritative for that account, and still not proven by
anything this project ingests. It does not make `PROVEN_SETTLEMENT_SOURCES`
non-empty and it never reaches `Confidence.EXECUTABLE`.

## File shape

One document per statement, named however you like — `2026-08-19.yml` reads
well because they are loaded newest first by `statement_date`.

```yaml
account: ACCT-000123          # required; an unattributed statement reconciles against nothing
broker: "Your Clearing LLC"
statement_date: 2026-08-19    # required; never defaulted to the day it was read
statement_ref: "DAILY-20260819"
currency: USD

lines:
  - symbol: ZSX26             # required
    description: SOYBEAN NOV26
    quantity: -68             # required; signed, short negative
    settlement_price: 1150.00 # required
    realised_usd: 0.0
    unrealised_usd: -78000.0
```

Nothing is defaulted. A line with no `settlement_price` is **refused** rather
than marked at the board, because marking it at the board would turn the
official number into ours. A missing `unrealised_usd` yields `None`, not zero,
and the row reports "could not be compared" instead of "agrees".

## What the reconciliation reports

* **Per contract**: their lots and ours, their settlement and our mark, their
  unrealised and ours, the difference, and whether it is inside
  `config.CLEARING_RECONCILIATION_TOLERANCE_USD` (default USD 25).
* **A quantity mismatch** is its own finding, not a price difference. If the
  clearer says 70 lots and the book says 68, that is not a P&L question.
* **On the statement, not in the book** — either a position nobody recorded, or
  a statement line for another desk. Both are worth knowing about.
* **In the book, not on the statement** — the clearer does not think this
  position exists.
* **A date mismatch** warns. Two dates are two markets, so part of any
  difference is just the session that moved in between.

Physical positions are **not** reconciled and the report says so. A clearer
holds futures, not beans; scoring a bean length against a futures statement
would manufacture a discrepancy out of a category error.

## Where it is rendered

Section 10 of the **private** workstation edition,
`data/workspace/workstation.html`. The public page renders that section
`absent` with a reason. It is never uploaded.
