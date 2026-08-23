# ROADMAP — what is deferred, and what would reopen it

The canonical mission lives in `CLAUDE.md`:

> Public-data soy intelligence and private desk decision support for physical buyers making daily cargo, basis, origin, and hedge decisions.

This document holds the other half of that decision: the ambition the mission
**stopped** claiming, why it was stopped rather than dropped, and the single
condition under which it comes back.

## Decision · 2026-08-23 (#297, map #296)

**The Bloomberg-terminal-class product is deferred to a commercially-triggered
expansion track. It is not the standard this repo is graded against.**

### Context

`CLAUDE.md` opened with "Bloomberg-terminal-style market intelligence platform
for physical soy traders" from the project's first commit. `AUDIT-2026-08-22.md`
graded the system against exactly that sentence and returned **C** — not
because the engineering is a C, but because the claim promises an execution
substrate the system does not have and was never building:

> "The single largest gap is not Python quality: the system lacks the
> executable physical-market substrate—firm bids/offers, licensed freight,
> contract-quality adjustments, counterparty terms, and authoritative
> settlements—from which a cargo value is actually made."
> — `AUDIT-2026-08-22.md:5`

The same audit found the narrower product sound: "It is not fatal to the
narrower 'public-data morning intelligence and decision-support workspace'
product" (`AUDIT-2026-08-22.md:225`). `DESIGN.md` had already conceded the
point in its own vocabulary — "A daily read, not a trading terminal" — which
means the codebase had been building the narrow product while the mission
statement advertised the wide one. Grading the first against the second is a
benchmark error, and most of the C→B− delta is that error, not a defect.

### Decision

Narrow the claim. Keep the ambition on a documented track.

Two failure modes were available and both were rejected:

- **Quietly delete the ambition.** Loses the reason half the architecture
  exists — the price-type vocabulary, the trust registry, the same-session
  join rule and the licence gate were all built *because* someone intends to
  sell this. Deleting the destination makes those look like over-engineering.
- **Keep the claim and build toward it.** Costs money before there is anyone to
  charge. Every item below needs paid data (CME/DCE/MATIF settlements, Platts,
  Baltic) or a counterparty network. Invariant 9 already says licensing gates
  publishing, not building; this is the same rule pointed at scope.

### What is on the expansion track

Deferred wholesale, per map #296's "Out of scope". None of it is a bug, a gap,
or a backlog item, and none of it should be partially built to look closer:

| Deferred | What it would require |
|---|---|
| Intraday / live execution | Exchange entitlements; an order path; SLOs this project does not staff |
| Live options chains and vol surfaces | Licensed derivatives feed |
| Executable bids/offers with counterparty terms | A broker/counterparty network, not an API |
| Full CTRM lifecycle | L/Cs, GAFTA/FOSFA clause handling, washouts, arbitration, hedge accounting |
| Terminal-grade SLOs and paging | An on-call rota |

### The trigger

**A paying client whose contract covers the data licences and the operational
burden of the item they are activating.** Until then: cost the menu, build
licence-ready interfaces, buy nothing speculatively (map #296, standing
constraints).

If the trigger ever fires, the expansion is a **fresh effort with its own
design** — not a resumption of this one. Nothing in this repo is holding a
seam open for it, and nothing should be.

### How to tell this decision is being violated

- A doc, page, or pitch calls the product a terminal, real-time, or a Bloomberg
  alternative. `tests/test_docs_claims.py` pins the mission sentence and
  guards `CLAUDE.md` against the rejected phrasing.
- A speculative data licence is purchased before a client covers it.
- A rendered number is presented as a settlement.
  `pricing.semantics.PROVEN_SETTLEMENT_SOURCES` is empty and invariant 3 keeps
  it that way.

## Prior benchmark research

`research/2026-08-14-soy-trader-bloomberg-replacement-benchmark.md` costs the
Bloomberg comparison in detail and reaches the same conclusion from the data
side: the free stack cannot claim to replace the exchange-grade intraday feed
or the end-to-end trading workflow until it buys entitlements. It is retained
as the costing input for this track, not as a target.
