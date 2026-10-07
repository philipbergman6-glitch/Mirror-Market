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

- "trader" in `docs/trial/PROTOCOL.md` and `analysis/trial/` vs "physical buyer" in the mission — resolved 2026-10-07 (A6): the canonical term is **participant**, a physical buyer; the code wording is a pending backlog change.

- "belt" was about to mean an area outline (2026-10-05 God's Eye View map) — resolved: it keeps its existing meaning (set of pins); the outline is a **footprint**.
