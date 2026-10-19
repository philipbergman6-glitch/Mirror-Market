# Mirror-Market — Domain Glossary

## Weather

**Pin**
A single named forecast point (one lat/lon) whose weather prices a rendered leg — e.g. "Brazil Mato Grosso".
_Avoid_: region (when meaning the point), station

**Belt**
The named set of pins that prices one rendered leg — e.g. Palm = Riau + Sabah.
_Avoid_: footprint (different concept), zone

**Footprint**
An area outline (admin-1 or county polygons, optionally production-weighted) whose weather is averaged as a whole — e.g. "the Mato Grosso footprint".
_Avoid_: belt, region, polygon (in prose)

## Hazards

**Hazard flag**
A dated warning attached to a rendered leg because a hazard (e.g. a tropical cyclone) threatens a place that prices it — a port or a footprint.
_Avoid_: signal (signals are price-derived), alert (an alert is a severity level, not a thing)

## Landed cost

**Desk assumption**
A hand-entered cost input (freight, elevation, port costs, financing, quality differential) that a named owner signed for, with a basis and an expiry. It is a client record: it never enters the public repository or the committed-CSV path.
_Avoid_: default, estimate, parameter

**Policy rate**
A published administered rate (import duty, VAT) entered with its provenance. Public by nature; still carries an expiry because policy moves.
_Avoid_: assumption (when meaning a published rate)

**Lapsed input**
A desk assumption or policy rate past its expiry. It blocks every landed total that needs it; the private edition shows the dead value, its date and owner so the owner knows what to renew. It is never used, labelled or not.
_Avoid_: stale (reserved for data-layer freshness), expired-but-usable

**Renewal**
Re-signing a lapsed or lapsing input with a new expiry, possibly unchanged in value. A decision with a person's name on it, not an automatic extension.

**Drill route**
A route costed end to end with the desk's own inputs standing in for a buyer's, labelled as such. It proves the chain, not the decision; only a buyer's confirmation makes it a decision.

## Trial

**Participant**
A physical buyer taking part in the validation trial: someone who prices or books physical soy product (beans, meal, oil) at least weekly — procurement, origination, or the physical desk at an importer, crusher, feed miller, or trading house. Futures-only traders, brokers and analysts are not participants.
_Avoid_: trader (the protocol's old word), user, client

**Handle**
A participant's pseudonym in every trial record; three characters minimum, never a name. The only identity the repo ever holds.
_Avoid_: username, id

**Session**
One logged attempt at one trial task by one participant, completed or abandoned. Captured on a form, transcribed into the private record by the desk.
_Avoid_: visit, decision (a session may reach no decision)

**Decision floor**
The minimum a participant must log before their sessions are graded: 20 sessions over the 30-trading-day window, across at least 5 tasks, including at least 8 real cargo or basis decisions. A trial with fewer than two participants at the floor returns `insufficient`.
_Avoid_: quota, target

**Design partner**
The participant who decides the first-screen contract with the desk before the trial starts. One of the recruits, not a separate person.

**Participation letter**
The one-page confidentiality instrument a participant signs: what is recorded, where it lives, that only the aggregate projection is ever shared, and the right to withdraw and have records deleted.
_Avoid_: NDA (a vendor instrument this effort does not use), consent form

## Relationships

- A **desk assumption** and a **policy rate** are both inputs to a landed total; a **lapsed input** of either kind blocks it until a **renewal**.
- A **belt** has one or more **pins**; a **footprint** is independent of pins and may contain several.
- A **hazard flag** attaches to a leg via a place — a port or a **footprint**.

## Flagged ambiguities

- "trader" in `docs/trial/PROTOCOL.md` and `analysis/trial/` vs "physical buyer" in the mission — resolved 2026-10-07 (A6): the canonical term is **participant**, a physical buyer; code and protocol v2 follow it (#412).

- "belt" was about to mean an area outline (2026-10-05 God's Eye View map) — resolved: it keeps its existing meaning (set of pins); the outline is a **footprint**.

## Publishing

**Publication disposition**
What the public may see of a source's series, fixed by that source's terms and decided per series (A18, 2026-10-13). One of three states: **open** (raw levels public), **derived** (raw levels desk-only; the public sees only our arithmetic on them — spreads against another series, rebased indices, ratios, ranks, percentiles), **withheld** (nothing of the series is public; at most a labelled link to the publisher's own page). An absolute change is not derived — it reconstructs the level. The committed history CSVs are a public surface, so a `derived` or `withheld` series is never exported there. Pre-sale every series is `open` (owner decision 2026-10-13); the other two states exist for the licence flip at sale. The private desk edition is internal use and sees raw levels under every disposition.
_Avoid_: licence-blocked (a reason, not a state), redacted, hidden

## Verification

**Critical basket**
The named set of displayed numbers (by product, venue, contract, origin, FX pair, crush, landed row) whose accuracy the product is judged on. Membership is a decision, labelled "pre-buyer" until physical buyers have revised it.
_Avoid_: sample (the sample is drawn from the basket), panel

**Basket-grade block**
A market-page block whose number is a critical-basket stratum (`config.AVAILABILITY_BASKET_BLOCKS`). Every table it reads must be written by a layer in the availability basket, so the basket cannot read available while the block is withheld (B37 #534). A spread struck over the provider front month is not basket-grade, even where its local leg is a basket number.
_Avoid_: critical block, basket block

**Verified**
A displayed number whose value, unit, observation date and contract month were checked against a source independent of the fetcher that produced it: the publisher of record (Tier 1) or a second provider publishing the same definition (Tier 2). A derived number is verified only when its legs are verified and its arithmetic recomputed (Tier 3).
_Avoid_: reconciled (see below), validated, tested

**Reconciled**
One provider's payload run through two of our own code paths with no field-level disagreement. Confirms persistence and parsing, never truth.
_Avoid_: verified, cross-checked

**Unverifiable**
A basket number for which no independent source is reachable. A legal published state carried with its reason, never a pass.
_Avoid_: skipped, n/a

**Discrepancy**
A verified comparison that fails: a value outside tolerance, or a date, contract, grade, incoterm or FX-date mismatch at any value delta (a metadata fail). Each is explained (cause and owner named) or unexplained.
_Avoid_: error (until unexplained and material), drift (reserved for reconciliation)

**Material**
A discrepancy that exceeds both the dataset's tick/rounding tolerance and the economic threshold in USD/MT, or is a metadata fail. A labelled D−1 against the publisher's D is not a discrepancy.
_Avoid_: significant, large

**Shipment window** _(proposal for V4 #477 — not yet adopted by the ledger)_
The loading/delivery period a physical quote is priced for — an AMS 3147 delivery slot ("Current", "Nov¹") or a MAGyP circular band ("2026-11..2027-04"). A venue that prints several windows in one session has published several prices. A displayed physical number names its window inline; one that does not fails the window check of **Verified** as a metadata fail.
_Avoid_: shipment period (on its own), month, slot (except for AMS's own term)
See **Loading-month identity** for how a band is compared across sessions.

**Loading-month identity**
The comparison key for a venue that prints several shipment windows per session: the start month of the nearest open window. A change of window within one loading month (a single month and the multi-month band starting on it) is like-for-like — the day-over-day change is shown, with the printed window named. A change of loading month is a **roll**: the day's figure shows with its label and the day-over-day change is withheld with a reason, never bridged. A crush struck across bands takes the start month of the period all three legs share.
_Avoid_: window (on its own, meaning the identity), contract month, prompt

**Labelled blend** _(proposal for V4 #477 — not yet adopted by the ledger)_
A displayed mean across shipment windows of one session, stating inline "mean of N windows (first–last)" with each window's own quote one click away on the same page. Proposed rule: a labelled blend **passes** when every window's quote verifies and the stated mean is recomputed exactly from them (Tier 3 arithmetic); the label is what makes it checkable. An unlabelled blend, or one whose label omits the count or the window range, **fails** as a metadata fail whatever its value — no published price equals it (V1 #474 rows 6–7). A mean of places quoting *one* window ("mean of 2 quotes" under a named window) is not a blend. B30 #504 removed the only two blends the site rendered; none is rendered today.
_Avoid_: average price, composite, "mean of N quotes" for windows

**Accuracy ledger**
The published, dated record of a verification pass: sample count per stratum, tolerances, and every discrepancy with its explanation. Shows pass/fail and delta only for licence-restricted series.
_Avoid_: scorecard, accuracy rate
