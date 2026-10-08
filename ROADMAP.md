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

## Audit continuation — 2026-10-08

Implemented locally after reproducing the four findings at `4a49b5b`: origin
crush FX provenance through the rendered caption; missing commitment components
withheld rather than summed as zero; China pace withheld until original WASDE
release availability is stored; eligible-only trial performance with all-record
standing and blocker reporting. Prior-FX limits and financial quality gates are
unchanged. No push, merge, deployment, participant outreach or paid subscription
was authorized by the handoff.

### Operations evidence and remaining work

- [Refresh 37800738125](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/37800738125)
  served the 02:49 edition throughout the 240-second wait for the 15:27 candidate.
  The smoke now explicitly fails a propagation timeout and checks every served
  page against the candidate stamp, even if an older edition is still inside the
  ordinary age budget. Recovery is logged only after the served stamp catches up.
  This improves diagnosis; it does not establish why that CDN edge stayed old.
- [Deploy 37702094754](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/37702094754)
  failed browser checks (desktop timeout, mobile overflow marker still pending).
  Browser timeouts now remain named failures while other pages are checked.
  The exact browser timing cause remains unresolved; no viewport gate was relaxed.
- The same run's saved FX report contains ZAR close `0.060509610921144485` above
  high `0.06050887703895569` on October 7. Replaying that row reproduces rejection
  by the trusted OHLC gate and acceptance by v1. Rejected revision IDs now travel
  into the JSON report and log. This is a real divergence, not proof of recovery
  or grounds for enabling the trusted reader (#239 / #312).
- COT failed all three attempts for both annual files with non-ZIP responses.
  Its open outage is #398. Weather returned 23/24 pins after three timeouts for
  Germany Mecklenburg-Vorpommern; #435 previously named Canada Alberta. Changing
  missing pins undermine a catalog-rename diagnosis. The alert now calls for
  transport/parse investigation before a catalog edit. Neither source is declared
  repaired. The weather licence decision remains #366.
- The deploy log also reports a forward-curve history shrink guard. That guard
  remains intact; no CI-owned history was edited.

### One private workflow before more features

`tests/test_origins_page_operational.py` now has a complete synthetic route
regression spanning cost inputs, September shipment, CIF barge delivery terms,
quality, landed ranking, sensitivity and an explicitly separate hedge assumption.
The five technical failure drills passed. The operating plan now removes its
obsolete proposed August start grid and lists the actual private inputs needed.
No local session/day JSON records were present. Real validation still requires
#411's two independent physical buyers, participation letters, private handles,
owned commercial inputs and their blind reads of the degraded surfaces.

### Narrow change digest — scoped, not activated

Keep the reserved News block under map #142 until a text-source contract is
accepted. The first digest should contain at most three source-linked changes
since the prior edition, limited to soy sales, applicable official policy, and
route logistics. Candidate sources:

- USDA FAS daily export-sale announcements. Resolve and validate the current
  primary listing before building an adapter; the attempted listing URL was
  inaccessible during this audit. Do not substitute newswire summaries or add
  daily announcements to weekly commitments (double counting).
- [USDA AMS Grain Transportation Report](https://www.ams.usda.gov/services/transportation-analysis/gtr),
  already relevant to #75: factual transport changes with the issue date and
  applicable route; never represent its context rates as a desk freight quote.
- [FAS policy notices](https://www.federalregister.gov/agencies/foreign-agricultural-service):
  follow the official document/PDF, record publication and effective dates
  separately, and distinguish proposed from final measures. Scope other agencies
  only when a specific soy route or oil-demand decision requires them.

Each item must carry publisher URL, publication time (unknown when absent),
retrieval time, affected commodity/route, concise factual change, separately
labelled decision implication, next verification/catalyst, and uncertainty.
Deduplicate by publisher document ID/URL, retain corrections, and distinguish
successful empty retrieval from source failure. Summarize facts and link out;
exclude copied articles, licensed report values and private desk content. No
synthetic example is to become a published news item.

### First-screen proposal for #406 — approval still outstanding

Use the existing editorial typography and numbered-section pattern. Proposed
reading order: edition timestamp and missing critical inputs; three dated changes
with source links and one sentence each on the physical decision affected;
next verified catalyst with schedule basis; route readiness and the private
workspace link. Each change exposes its observation separately from its
interpretation and uncertainty. The private edition may add landed sensitivity
and a named hedge reference only when its inputs are complete. The public edition
must keep the honest blocked state where desk inputs are absent.

Acceptance with the design partner: in a timed morning read, the buyer can name
what changed, why it matters to the cargo/basis decision, what they will check
next, and which missing input prevents action. No first-screen layout has been
changed; #406 and `DESIGN.md` require the buyer/design approval first.
