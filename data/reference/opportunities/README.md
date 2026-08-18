# Opportunities — the private working file

**Deliberately empty, and gitignored.** This project ingests no CRM, no mailbox
and no deal system, so "we called them on Tuesday" can only come from you. An
empty directory is the correct state for a clone that has worked nothing, and
the page says so rather than showing a blank status column.

Every `*.yml` file here is read by `analysis/opportunities/workflow.py` at site
build time. A missing directory is an empty workflow; a **present but
malformed** file raises and fails the build, because "nothing recorded" and
"something recorded wrongly" are different states and only one of them is safe
to render as blank. Same contract, and the same reasoning, as
`data/reference/positions/`.

## The privacy boundary

Everything in this directory is private, and the boundary is enforced in four
independent places rather than by one `{% if %}` in a template:

1. `*.yml` here is **gitignored** — the file never leaves your machine.
2. It loads into `WorkflowRecord`, a **separate object**;
   `Opportunity.to_dict(audience="public")` does not build the key at all.
3. Any opportunity carrying a record is excluded from
   `EngineResult.public` **entirely** — not hidden, absent. The fact that a desk
   is working a lane is itself commercial information, whatever the status says.
4. `pipeline.store.save_opportunity_detections` **rejects** the private field
   names, so the git-committed detection archive cannot hold them either.

The private edition of the page is rendered to
`data/workspace/opportunities.html` — outside `docs/`, because `docs/` is what
the Pages deploy uploads. Do not move it.

## Shape

```yaml
# One document is a YAML list of records, one per opportunity id.
# The id comes from the board: OPP-<first-seen date>-<hash>. It is stable
# across runs as long as the lane, product, window and rule are the same.

- opportunity_id: OPP-20260812-2ff181
  status: contacted          # detected | reviewing | actionable | contacted |
                             # negotiating | won | lost | expired | dismissed
  owner: pb
  contacted_on: 2026-08-13
  counterparty: "Aceitera General Deheza"   # who you actually spoke to
  next_action: "Re-check the Sep band once the next circular lands"
  next_action_due: 2026-08-20
  notes:
    - "Interested in Sep, not Aug. Wants the meal leg quoted separately."
  feedback:
    # Every entry needs a reason. A dismissal with no reason teaches nobody
    # anything, which is the only purpose this record has.
    - kind: progressed       # dismissed | false_signal | contacted_no_interest |
                             # progressed | won | lost
      recorded_on: 2026-08-13
      reason: "They asked for an offer on the Sep band."
      by: pb
  audit:
    - {on: 2026-08-13, by: pb, what: "status detected -> contacted"}
```

Unknown fields raise rather than being ignored — a typo'd `note` where `notes`
was meant would silently drop the one thing this file exists to record.

## What a status does

`contacted` and `negotiating` promote the opportunity to **proposed trade**;
`won` and `lost` promote it to **completed business**. Those two rungs cannot be
reached by any detector — they are statements about what a person did, and this
file is the only place they can come from.

A status never *lowers* the detected rung. A contacted lead is still, in market
terms, a lead; the ladder says what the thing is, and the status says what was
done about it.

## What the feedback is for

It is counted and reported back to you, per rule, in section 08 of the private
page. It does **not** re-weight anything. A screen that quietly retuned itself
on a handful of dismissals would be a model nobody trained, evaluated or can
turn off — and at this sample size it would be fitting noise.
