# Data Trust Foundation RFC and Implementation Package

Status: approved for implementation planning
Date: 2026-08-10
Product: Mirror Market
Audience: professional global soy commodity traders
Infrastructure constraint: Git is the durable source of truth; SQLite is a rebuildable cache

## How to use this document

This is the authoritative handoff for the Data Trust Foundation milestone. Before implementing it:

1. Record the starting commit and inspect changes made after this RFC.
2. Reconcile each ticket with the current code. Mark work that already satisfies an acceptance criterion; do not recreate it under a second interface.
3. Implement tickets in dependency order. A ticket may be split into smaller commits, but unrelated tickets must not be combined.
4. Keep the current dashboard operable until the edition-promotion path replaces it.
5. Finish each ticket only when every acceptance criterion and named test behavior passes.

This RFC prepares ticket text. It does not authorize filing issues, changing source rights, or publishing restricted data.

## Problem Statement

Mirror Market has become a credible specialist research prototype, but a professional soy trader cannot yet tell, for every displayed number:

- exactly what market observation it represents;
- which source record produced it;
- whether it is a settlement, reference price, bid, offer, spot, FOB, CIF, or farmgate quote;
- which contract, delivery window, currency, unit, location, and market date apply;
- when the source published it and when Mirror Market ingested it;
- which validation rules it passed;
- whether it is current, stale, legacy, quarantined, corrected, or superseded;
- which revision was used in a dashboard or briefing;
- whether Mirror Market has the right to retain and publish it.

The current pipeline writes normalized rows before all dataset-level checks finish, overwrites matching keys, retains only part of production history, and can publish a green build with missing critical content. Git CSV persistence is useful, but it does not yet form an append-only observation ledger or a reproducible edition history.

The problem is not primarily insufficient market coverage. The next milestone must make the existing core trustworthy before adding more sources or features.

## Product Trust Contract

Mirror Market is decision support for a professional global soy trader. It is not the sole execution source.

Every promoted edition must satisfy these guarantees:

1. Every critical displayed number resolves to one accepted observation revision or one derived observation whose inputs resolve recursively to accepted revisions.
2. Every observation identifies its native market meaning: product, venue or location, price type, currency, unit, contract or delivery window, and effective date.
3. Every observation carries source, ingestion, quality, freshness, and publication-rights metadata.
4. Stale data remains visible only as last known good data with an explicit as-of date.
5. Derived comparisons are unavailable when their required inputs are stale beyond contract, quarantined, or not aligned on an allowed market date.
6. Quarantined, rejected, rights-prohibited, and unreviewed legacy observations are ineligible for critical published analytics.
7. Failed attempts are durable and observable but cannot replace the current promoted edition.
8. Corrections append a revision; they never erase the prior value or the record of editions that used it.
9. A promoted edition is reproducible from its run manifest, accepted revision identifiers, code revision, and generated artifact hashes.
10. Publication rights are fail-closed: unknown rights are treated as ineligible for public output.

## Definition of complete soy coverage

Coverage is measured against trader decisions, not source count.

### Tier 1 — Daily decision core

- Soybean, soybean meal, and soybean oil benchmark prices
- Named contracts and confirmed settlement state
- Currencies needed for every published conversion
- Crush economics
- Physical and export prices
- Basis and cross-origin spreads
- Input alignment, freshness, provenance, and quality state

### Tier 2 — Market explanation

- Production, area, yield, stocks, use, and trade balances
- Export sales and inspections
- Crush volumes
- Weather and crop progress
- Positioning
- Biofuel and energy demand
- Policy and official forecasts

### Tier 3 — Origination intelligence

- Freight and port logistics
- Vessel lineups and river conditions
- Domestic cash markets and farmer selling
- Processing capacity and utilization
- Counterparties, destinations, and trade relationships

A domain is complete only after its trader questions, required observations, source rights, cadence, validation rules, and failure behavior are documented.

## Agreed operating model

- Initial users are invited professional traders, leading to a free public beta.
- The product is source-native and daily. Exchange data is settlement-anchored; weekly and monthly publications retain their true cadence.
- The public site remains a static build artifact during this milestone.
- No new external infrastructure is required.
- Git is the durable source of truth.
- SQLite remains a local and CI query cache rebuilt from durable records.
- Routine publication is automated. Human review handles exceptions but is not required for a normal edition.
- During beta, the owner may be the sole correction approver.
- Source repair and new source work remain separate from the trust foundation.

## Ubiquitous language

Use these terms consistently in code, tests, logs, tickets, and trader-facing status text.

### Source

The legal and operational origin of data. A source owns cadence, rights, transport, and attribution facts.

### Dataset

A defined family of observations from one source with one market meaning and one data contract. One source can expose several datasets.

### Raw artifact

The immutable response or document obtained from a source, plus retrieval metadata. Where content retention is prohibited or impractical, a metadata-only artifact records the fetch without persisting content.

### Candidate observation

A parsed record that has not completed validation and is never publishable.

### Observation

A market fact in native units with a stable logical identity. An observation can have multiple revisions.

### Observation revision

One append-only assertion about an observation, including value, source artifact, ingestion time, parser version, quality state, and supersession relationship.

### Finding

The result of applying one quality rule. Severity is `warning`, `quarantine`, or `reject`. Informational metrics belong in run statistics rather than findings.

### Quality state

The lifecycle state of a revision: `legacy`, `accepted`, `quarantined`, `rejected`, or `superseded`. A warning does not create a separate state; an accepted revision may carry warnings.

### Freshness state

The time relationship between an accepted observation and its dataset contract: `current`, `stale`, or `unavailable`.

### Run

One attempted collection and validation cycle. Every run completes with a durable manifest, including failed runs.

### Dataset result

The complete outcome for one dataset in one run: artifact references, candidate count, accepted revision identifiers, findings, coverage, freshness, and status.

### Edition

A candidate or promoted trader-facing publication assembled from specific accepted revisions and derived outputs.

### Promotion

The operation that makes one verified edition the public current edition. Promotion changes a pointer; it does not mutate the edition.

### Legacy

Data preserved from the existing pipeline whose provenance or validations do not yet satisfy the new contract. Legacy is a known state, not an accusation that the value is wrong.

## Quality model

Finding severity and revision state are separate dimensions.

### Reject

Use when the record cannot represent the claimed market fact:

- missing observation identity;
- invalid or ambiguous unit/currency;
- unparseable effective date;
- invalid contract or delivery window;
- impossible OHLC relationships;
- non-finite or prohibited value;
- parser/source mismatch;
- public-retention or publication prohibition.

Rejected records are durable for audit but never accepted automatically.

### Quarantine

Use when a record may be valid but needs evidence or review:

- extreme price or FX move;
- unexpected source coverage decline;
- stale source payload;
- conflicting duplicate;
- cross-source divergence outside contract;
- observation timestamp inconsistent with venue schedule;
- settlement status not confirmed when settlement is required.

Quarantined records never feed critical analytics until an append-only approval or replacement revision resolves the finding.

### Warning

Use for unusual but publishable conditions:

- plausible value near a configured limit;
- optional field absent;
- source publication later than normal but inside tolerance;
- accepted fallback explicitly allowed by the dataset contract.

Warnings travel with accepted revisions and appear in edition health.

## Canonical observation identity

Every observation type defines which fields form its stable logical identity. The model must be able to represent at least:

- commodity and product form;
- source and dataset;
- market venue or geographic location;
- price type;
- currency and native unit;
- contract identity or physical delivery window;
- effective market date;
- source record identity where provided.

Revision metadata must include:

- revision identifier;
- value and optional OHLC/volume fields;
- raw artifact reference;
- source publication timestamp when known;
- observation timestamp when genuinely observed;
- ingestion timestamp;
- parser version;
- quality state and finding identifiers;
- superseded revision identifier;
- correction type and reason when applicable;
- public publication eligibility.

Dates inferred from fetch time must be marked as inferred; they must not masquerade as source-observed timestamps.

Named futures contracts are canonical observations. Continuous front-month series are derived observations with a documented roll method and input contracts.

## Rights model

Every dataset contract records independent decisions for:

- raw-content retention;
- normalized historical retention;
- internal display;
- public display;
- derived publication;
- commercial use;
- redistribution;
- required attribution;
- expiry or review date;
- evidence supporting the decision.

Each decision is `allowed`, `prohibited`, or `unknown`. `Unknown` is fail-closed for public publication.

The artifact repository supports:

- content-retained artifacts for permitted sources;
- metadata-only artifacts for prohibited or oversized content;
- content hashes calculated during ingestion even when content is not retained, where permitted;
- source-specific retention instructions.

## Git-native durable layout

The durable v2 root contains these logical collections:

```text
data/v2/
  registry/        source and dataset contracts
  artifacts/       permitted content-addressed raw artifacts and metadata
  observations/    append-only observation revisions, partitioned by dataset and year
  findings/        immutable quality findings, partitioned by run
  runs/            immutable run manifests
  editions/        immutable edition manifests
  corrections/     append-only operator decisions and evidence references
  current-edition.json
```

Rules:

- Every record declares a schema version.
- Durable writes use temporary files and atomic replacement.
- Append operations are idempotent by stable identifier.
- Content-addressed artifacts are not duplicated.
- Existing v1 history remains intact until migrated and verified.
- CI owns generated durable records; pull requests cannot casually rewrite them.
- A shrinking export or disappearing field is rejected unless an explicit reviewed migration permits it.
- Failed runs may add manifests, artifacts, findings, and quarantined records, but cannot update `current-edition.json`.
- Large, reproducible public files may be metadata-only when retaining every copy would create unreasonable Git growth.
- Restricted raw content never enters the public repository.

## Architecture and module interfaces

### Trusted ingestion module

This is the primary deep module. Its interface accepts a dataset identifier and run context, and returns one dataset result.

Callers need not coordinate fetching, hashing, raw retention, parsing, generic validation, dataset validation, revision creation, finding persistence, or manifest bookkeeping. Those are implementation details behind the interface.

The interface guarantees:

- no candidate observation escapes as accepted before all required rules run;
- dataset-level coverage is evaluated before accepted revisions become visible to the candidate edition;
- repeated ingestion of the same artifact is idempotent;
- every returned accepted revision resolves to an artifact and findings;
- external failures return a typed dataset result rather than an unstructured exception or ambiguous empty mapping.

Source adapters form an internal seam because behavior varies by true external source and tests need deterministic adapters.

### Durable trust repository

Its interface stores and reads artifacts, revisions, findings, runs, editions, corrections, and the promoted-edition pointer.

The production adapter writes Git-native files. Tests use an in-memory or temporary-directory adapter. Callers do not construct paths or coordinate atomic writes.

### Quality module

Its interface evaluates candidates plus dataset context and returns findings and a deterministic disposition. It has no storage or network behavior.

Rules are registered by stable rule identifier and version. Re-running a fixed rule against the same candidate and context produces the same result.

### Observation query module

Its interface provides accepted current revisions, point-in-time revisions, and edition-pinned revisions. The SQLite cache adapter serves production analysis during CI; a direct in-memory adapter supports tests.

Analytics callers do not filter quality states themselves. The interface enforces publication eligibility.

### Edition module

Its interface builds a candidate edition from dataset results, evaluates the critical contract, records generated artifact hashes, and returns a promotion decision.

Promotion is a separate operation. A failed candidate remains durable without changing the current edition.

### Rights policy module

Its interface answers whether a proposed retention or publication action is allowed for a dataset. Ingestion, persistence, derivation, and edition building use the same policy so rights rules do not drift across callers.

## Critical edition contract

An edition cannot be promoted unless all of the following are true:

- soybean, soybean meal, and soybean oil benchmark observations are accepted and use named contracts;
- settlement-required observations have confirmed settlement status;
- every FX rate required by a published conversion is accepted and aligned according to contract;
- at least one approved Brazil physical or export price is current;
- every critical derived value resolves to eligible aligned inputs;
- critical dataset freshness states are current;
- the briefing is non-empty and contains required sections;
- the dashboard is generated successfully and passes semantic assertions;
- no rights policy prohibits a displayed critical value;
- durable run and edition manifests were written successfully;
- generated artifact hashes match the edition manifest.

If the contract fails, the prior promoted edition stays public. The failed attempt produces an alert and durable diagnostics.

## Migration strategy

### Additive migration

The v1 pipeline remains operational while v2 is introduced. V2 initially dual-writes pilot datasets and produces reconciliation reports. Readers move only after acceptance criteria pass.

### Existing history

- Preserve current history unchanged.
- Import rows into v2 as `legacy` revisions.
- Record the original table/file and key as provenance.
- Re-fetch and validate where upstream history is available and rights permit.
- Create accepted revisions rather than mutating legacy revisions.
- Keep known-bad revisions quarantined or rejected for audit.
- Exclude unapproved legacy data from critical analytics.

### Pilot datasets

1. Argentina MAGyP official FOB proves artifact capture, structured parsing, delivery windows, provenance, and dual-write reconciliation.
2. Named soy benchmark contracts prove settlement state, OHLC validation, anomaly quarantine, and roll-aware derivation.
3. Required FX datasets prove currency identity, anomaly handling, and input-date alignment.
4. USD/MT derived observations prove recursive provenance and publication eligibility.

## Epics and tickets

Each ticket is independently mergeable. Dependencies are explicit. Acceptance criteria describe observable behavior, not internal structure.

### Epic A — Trust vocabulary and contracts

#### DT-01 — Introduce the trust-domain vocabulary

Dependencies: none

Goal: establish schema-versioned value objects for source, dataset, artifact reference, observation identity, revision, finding, run, dataset result, edition, and correction.

Acceptance criteria:

- Every term in the ubiquitous language has one canonical representation.
- Severity, quality state, freshness state, run status, and edition status reject unknown values.
- Identifiers are stable and deterministic from their documented identities.
- Serialization round-trips without losing numeric, date, timestamp, or optional-field meaning.
- The new vocabulary has no dependency on pandas, SQLite, HTTP, or filesystem paths.

Tests:

- Round-trip every value object.
- Reject invalid enum values and incomplete identities.
- Prove identifier stability for equivalent inputs.
- Prove distinct contracts/delivery windows produce distinct identities.

Suggested commits:

1. Add states and identifiers with tests.
2. Add observation and artifact values with tests.
3. Add run, finding, edition, and correction values with tests.

#### DT-02 — Add source and dataset contracts

Dependencies: DT-01

Goal: define one registry that owns cadence, required identity fields, units, criticality, validation policy, raw retention, and publication rights.

Acceptance criteria:

- A dataset cannot be registered without a source, cadence, identity contract, freshness contract, and explicit rights decisions.
- Rights decisions distinguish retention, public display, derived publication, commercial use, and redistribution.
- Unknown rights make public publication ineligible.
- Registry validation detects duplicate identifiers, missing required decisions, and contradictory configuration.
- Initial contracts exist for MAGyP FOB, soy benchmark prices, and required FX pairs.

Tests:

- Validate the real pilot contracts.
- Reject incomplete and contradictory contracts.
- Prove source cadence and rights can differ between datasets from one source.

Suggested commits:

1. Add registry validation and temporary test contracts.
2. Register MAGyP.
3. Register price and FX pilots.

#### DT-03 — Define run and edition manifests

Dependencies: DT-01, DT-02

Goal: specify reproducible run and edition records before persistence or workflow behavior is added.

Acceptance criteria:

- A run manifest records code revision, start/end times, dataset results, parser versions, findings summary, and terminal status.
- An edition manifest pins accepted revision identifiers and derived outputs.
- Edition status distinguishes candidate, verified, promoted, deployment-failed, and superseded.
- Manifests calculate deterministic content hashes excluding only explicitly non-deterministic transport metadata.
- A failed run manifest is valid without an edition.

Tests:

- Round-trip successful and failed manifests.
- Prove content hash changes when a pinned revision or generated artifact changes.
- Reject promotion records lacking verification evidence.

Suggested commits:

1. Add run manifest behavior.
2. Add edition manifest and promotion-state behavior.

#### DT-04 — Define the trusted-ingestion result interface

Dependencies: DT-01, DT-02, DT-03

Goal: replace ambiguous dict/empty/exception orchestration semantics with one dataset result contract for v2 pilot datasets.

Acceptance criteria:

- Dataset result distinguishes success, legitimate empty publication, external failure, contract failure, and quarantined dataset.
- It exposes artifact references, row counts, revision identifiers, findings, coverage, freshness, and eligibility.
- Successful status cannot coexist with unresolved reject findings.
- A legitimate empty result is only valid when the dataset contract permits it.
- No live fetcher migration is included in this ticket.

Tests:

- Construct and validate every result state.
- Reject contradictory combinations.
- Prove systemic and critical-status evaluation can consume results without inspecting DataFrames.

Suggested commits:

1. Add the interface and replace-or-extend result contract tests without changing production callers.

### Epic B — Git-native durable trust repository

#### DT-05 — Establish the versioned Git repository interface

Dependencies: DT-01, DT-03

Goal: create one repository interface that hides paths, schema files, atomic writes, locking assumptions, and serialization.

Acceptance criteria:

- A temporary-directory adapter and production Git-directory adapter satisfy the same interface.
- Repository initialization is idempotent.
- Every durable record includes a schema version.
- Writes use temporary files and atomic replacement where records are mutable pointers.
- Immutable records reject conflicting rewrites.
- Callers never build storage paths.

Tests:

- Run the same repository behavior suite against both adapters.
- Prove initialization, idempotence, atomic pointer replacement, and immutable conflict detection.

Suggested commits:

1. Add repository interface and temporary adapter.
2. Add Git-directory adapter and shared contract tests.

#### DT-06 — Persist content-addressed raw artifacts

Dependencies: DT-02, DT-05

Goal: retain permitted source material without duplication and support metadata-only artifacts.

Acceptance criteria:

- Content-retained artifacts are addressed by cryptographic hash.
- Re-storing identical content creates no duplicate payload.
- Metadata records source, dataset, URL, fetch time, response status, media type, size, hash, and retention decision.
- Metadata-only mode stores no prohibited bytes.
- Rights policy is checked before content is written.
- Interrupted writes cannot leave a valid-looking truncated artifact.

Tests:

- Store and retrieve permitted content.
- Deduplicate repeated content.
- Prove metadata-only mode writes no payload.
- Reject prohibited retention.
- Simulate interrupted or conflicting writes.

Suggested commits:

1. Add artifact metadata persistence.
2. Add content retention and deduplication.
3. Add rights and failure-path tests.

#### DT-07 — Persist an append-only observation ledger

Dependencies: DT-01, DT-05, DT-06

Goal: store observation revisions without destructive replacement.

Acceptance criteria:

- Appending the same revision twice is idempotent.
- A different value for the same observation identity creates a new revision.
- Supersession links are explicit and acyclic.
- Current accepted, point-in-time, and all-revision queries return deterministic results.
- Rejected, quarantined, superseded, and legacy revisions remain queryable but are excluded from current accepted queries.
- Every non-legacy revision references an artifact and parser version.

Tests:

- Append, deduplicate, revise, supersede, and query observations.
- Reject conflicting revision identifiers and cycles.
- Prove current accepted queries cannot leak ineligible states.

Suggested commits:

1. Add append and all-revision queries.
2. Add supersession and current accepted queries.
3. Add point-in-time and eligibility behavior.

#### DT-08 — Persist findings, runs, corrections, editions, and the current pointer

Dependencies: DT-03, DT-05, DT-07

Goal: make every operational and publication decision durable.

Acceptance criteria:

- Findings and run manifests are immutable.
- Corrections append operator, reason, evidence, prior revision, and replacement/approval decision.
- Editions are immutable; status transitions append records or create a new manifest version without rewriting history.
- The current-edition pointer updates atomically and only to a verified/promoted edition.
- Failed runs and rejected editions do not change the pointer.

Tests:

- Persist and reload each record type.
- Reject correction without evidence/reason.
- Reject pointer update to candidate or failed edition.
- Prove prior current pointer survives a failed update.

Suggested commits:

1. Add findings and runs.
2. Add corrections.
3. Add editions and pointer promotion.

#### DT-09 — Import v1 history as legacy revisions

Dependencies: DT-02, DT-07, DT-08

Goal: preserve existing Git history without granting it unearned trust.

Acceptance criteria:

- Import is read-only with respect to v1 files.
- Every imported row becomes an idempotent legacy revision with original file/table/key provenance.
- Unsupported or ambiguous rows produce findings rather than disappearing.
- Re-running the importer adds no duplicates.
- Import summaries reconcile input, imported, skipped, and finding counts.
- No critical query treats legacy as accepted by default.

Tests:

- Import representative history fixtures.
- Re-run idempotently.
- Preserve blank/null distinctions required by identities.
- Surface malformed and unsupported rows.
- Prove existing files are byte-for-byte unchanged.

Suggested commits:

1. Add dry-run reconciliation.
2. Add legacy revision writes.
3. Add malformed-row findings and complete summary.

#### DT-10 — Build the SQLite query cache from trusted records

Dependencies: DT-07, DT-08, DT-09

Goal: retain pandas/SQL analysis ergonomics while making Git records authoritative.

Acceptance criteria:

- A clean SQLite cache can be built from accepted and edition-pinned revisions.
- Rebuilding twice yields equivalent query results.
- The cache stores revision identifiers and provenance needed by analytics.
- Cache deletion loses no durable information.
- Legacy inclusion is an explicit option and defaults off for critical analysis.
- Existing query consumers can be adapted incrementally.

Tests:

- Build, query, delete, and rebuild.
- Compare cache results with repository queries.
- Prove quarantined/rejected records are absent.
- Prove edition-pinned rebuilds remain stable after newer revisions arrive.

Suggested commits:

1. Add cache schema and accepted-revision build.
2. Add edition-pinned and optional legacy modes.
3. Add reconciliation tests against repository queries.

### Epic C — Validation, freshness, and derivation

#### DT-11 — Implement the deterministic quality-rule engine

Dependencies: DT-01, DT-04

Goal: centralize rule execution and disposition instead of logging warnings inside individual cleaners.

Acceptance criteria:

- Rules have stable identifier, version, scope, severity, and evidence payload.
- Rule order cannot change final disposition.
- Reject outranks quarantine; quarantine outranks accepted-with-warning.
- The engine returns findings and disposition without performing persistence.
- Duplicate findings from identical rule/evidence inputs collapse deterministically.

Tests:

- Good, warning, quarantine, and reject examples.
- Mixed-severity ordering.
- Determinism across rule registration order.
- Rule version appears in findings.

Suggested commits:

1. Add rule and finding evaluation.
2. Add disposition aggregation and determinism tests.

#### DT-12 — Add generic identity, unit, temporal, and numeric validators

Dependencies: DT-02, DT-11

Goal: enforce invariants common to every dataset before source-specific rules run.

Acceptance criteria:

- Required identity fields are enforced from the dataset contract.
- Dates and timestamps distinguish observed, published, ingested, and inferred values.
- Units/currencies must be contract-recognized.
- Non-finite, impossible-sign, and configured-range behavior is deterministic.
- Unknown extra fields do not silently alter identity.

Tests:

- Parameterized tests across pilot dataset contracts.
- Time-zone and date-boundary cases.
- Inferred timestamp labeling.
- Unit/currency mismatch rejection.

Suggested commits:

1. Add identity and unit validators.
2. Add temporal and numeric validators.

#### DT-13 — Add dataset coverage and freshness evaluation

Dependencies: DT-02, DT-04, DT-11, DT-12

Goal: evaluate whole-dataset health before accepted revisions become edition-eligible.

Acceptance criteria:

- Expected keys, minimum coverage, publication calendar, and freshness tolerance come from the dataset contract.
- Partial coverage is evaluated before the dataset is eligible.
- Forecast rows cannot make observed history appear current.
- Legitimate holidays/quiet publications are distinguishable from failure.
- Last known good state is returned with explicit as-of date.

Tests:

- Complete, partial, stale, legitimate-empty, and failed datasets.
- Weekly/monthly cadence and holiday examples.
- Forecast masking regression test.
- Prove partial datasets cannot partially leak into a candidate edition.

Suggested commits:

1. Add coverage evaluation.
2. Add cadence/freshness evaluation.
3. Add last-known-good result behavior.

#### DT-14 — Gate derived observations on eligible aligned inputs

Dependencies: DT-07, DT-11, DT-13

Goal: provide one interface for trusted derived calculations.

Acceptance criteria:

- A derived revision records calculation identifier/version and exact input revision identifiers.
- Input eligibility, rights, quality, freshness, currency, unit, and date alignment are checked before calculation.
- Missing or misaligned required input returns an unavailable derived result, not a partial number.
- Re-running the same calculation with the same inputs is idempotent.
- New input revisions produce a new derived revision without erasing the prior result.

Tests:

- Valid aligned calculation.
- Stale, quarantined, rights-prohibited, missing, and disjoint-date inputs.
- Recursive provenance and revision behavior.

Suggested commits:

1. Add derived-input eligibility interface.
2. Add derived revision persistence and provenance tests.

### Epic D — Trusted vertical slices

#### DT-15 — Migrate MAGyP FOB through trusted ingestion

Dependencies: DT-02, DT-04, DT-06, DT-07, DT-11, DT-12, DT-13

Goal: prove the full trusted path with an official physical-market source.

Acceptance criteria:

- The source adapter returns a raw artifact before parsing.
- Parsed candidates retain product, position, price type, USD/MT unit, effective date, and shipment window.
- Source shape drift produces typed findings and no accepted revisions.
- The old and new outputs reconcile for a fixed real fixture and a controlled live run.
- Dual-write can run without changing the current dashboard.
- A reviewed switch makes trusted MAGyP queries available while retaining the old path for rollback.

Tests:

- Existing parser behaviors continue through the new interface.
- Raw artifact replay requires no network.
- Duplicate ingestion is idempotent.
- Shape drift, malformed positions, holiday empty, and revision cases.
- Reconciliation report accounts for every row and field difference.

Suggested commits:

1. Wrap existing fetch/parse behavior in an artifact-replay adapter.
2. Add trusted validation and ledger writes.
3. Add dual-write reconciliation.
4. Add trusted read path and rollback switch.

#### DT-16 — Model named soy benchmark contracts and settlement state

Dependencies: DT-02, DT-11, DT-12, DT-13, DT-15

Goal: replace ambiguous daily front-month observations in the trusted path with named contracts.

Acceptance criteria:

- Soybean, meal, and oil observations identify exchange, contract, native unit, market date, and settlement state.
- An unfinished current-session bar cannot be accepted as settlement.
- OHLC invariants reject impossible candles.
- Extreme moves quarantine rather than merely log.
- Contract rolls do not rewrite named-contract history.
- Continuous series, if produced, are derived and document roll methodology.

Tests:

- Before/after settlement examples across daylight-saving periods.
- Impossible OHLC, missing contract, duplicate, extreme move, and valid settlement.
- Roll-day behavior and derived-series provenance.
- Network-free source replay.

Suggested commits:

1. Add named-contract candidate mapping.
2. Add settlement and OHLC rules.
3. Add anomaly quarantine and dual-write reconciliation.
4. Add trusted contract queries and optional derived continuous series.

#### DT-17 — Migrate required FX observations

Dependencies: DT-02, DT-11, DT-12, DT-13, DT-16

Goal: make currency conversions depend on trustworthy, correctly oriented FX observations.

Acceptance criteria:

- Pair orientation and quote convention are explicit in identity.
- Zero, non-finite, unit-flipped, and implausible moves are rejected or quarantined according to contract.
- Historical known anomaly fixtures cannot become accepted silently.
- Accepted rates retain market date and source provenance.
- Required FX coverage is evaluated before an edition can publish conversions.

Tests:

- Correct and inverted pair examples.
- Historical NGN, IDR, ARS, and ZAR anomaly fixtures.
- Missing-day alignment and last-known-good behavior.
- Dual-write reconciliation for recent valid periods.

Suggested commits:

1. Add explicit FX identity and orientation.
2. Add anomaly rules and historical fixtures.
3. Add trusted reads and reconciliation.

#### DT-18 — Produce trusted USD/MT derived observations

Dependencies: DT-14, DT-16, DT-17

Goal: migrate the central cross-market comparison unit through the derived-observation interface.

Acceptance criteria:

- Each conversion records native-price and FX revision identifiers plus conversion version.
- Commodity unit conversions use the correct contract-specific native unit.
- FX and price dates follow the dataset alignment policy.
- Unavailable inputs produce no numeric output.
- Current display values reconcile with known-good unit tests and accepted recent observations.

Tests:

- Beans, meal, and oil conversions.
- Missing/misaligned/quarantined FX.
- Unit-direction and double-conversion regressions.
- Revision propagation when one input changes.

Suggested commits:

1. Move conversion math behind derived-observation eligibility.
2. Add persisted provenance and reconciliation.

### Epic E — Reproducible editions and safe promotion

#### DT-19 — Build and verify candidate editions

Dependencies: DT-03, DT-08, DT-10, DT-13, DT-14, DT-15, DT-16, DT-17, DT-18

Goal: turn one run into a reproducible candidate publication and semantic verdict.

Acceptance criteria:

- Candidate analysis uses an edition-pinned cache.
- Critical dataset and derived-observation requirements are evaluated before rendering.
- Rendering writes to a candidate location rather than replacing public output.
- Semantic checks assert non-empty briefing, required sections, correct edition identifier, freshness/quality summaries, and generated artifact hashes.
- Failed semantic checks create a rejected edition manifest and preserve the prior current edition.
- The valid word `failed` inside briefing content cannot cause the briefing to disappear.

Tests:

- Fully valid candidate.
- Missing core price, FX, Brazil physical price, briefing, section, or manifest.
- Stale and quarantined critical inputs.
- Valid degraded contextual datasets.
- Briefing failure-word regression.

Suggested commits:

1. Add critical contract evaluation.
2. Add edition-pinned analysis and candidate rendering.
3. Add semantic artifact verification and hashes.

#### DT-20 — Promote editions safely and expose provenance

Dependencies: DT-19

Goal: publish only verified editions while making trader-facing trust state visible.

Acceptance criteria:

- Verification failure skips deployment and leaves the prior public edition intact.
- Successful deployment is followed by an atomic current-pointer update.
- Deployment or pointer-update failure raises a durable alert and records the edition state.
- Public pages show edition ID, generated time, critical freshness, and degraded datasets.
- Critical numbers expose source, as-of date, quality state, and observation/revision reference through a concise details interaction or download.
- Failed attempts are visible in operational status without replacing the current edition.
- Promotion behavior is idempotent under workflow retry.

Tests:

- Verification failure, deployment failure, pointer failure, retry, and success.
- Prior edition preservation.
- Public provenance fields for every pilot critical number.
- Static output contains no prohibited raw data.

Suggested commits:

1. Gate deployment on verified edition.
2. Add promotion/pointer workflow and retry behavior.
3. Add trader-facing provenance and health display.
4. Add end-to-end static artifact test.

## Dependency order

```text
DT-01
  ├─ DT-02 ─ DT-04
  └─ DT-03
       │
DT-05 ─┼─ DT-06 ─ DT-07 ─ DT-08
       │              ├─ DT-09 ─ DT-10
       │              └─ DT-14
DT-11 ─ DT-12 ─ DT-13 ─┘
                         │
                         DT-15 ─ DT-16 ─ DT-17
                                      └────┬──── DT-18
                                           │
                                           DT-19 ─ DT-20
```

Parallel work is safe only when ticket dependencies are satisfied and agents do not edit shared generated data. The implementation session should prefer sequential work through the foundations; vertical source work may parallelize only after repository and quality interfaces stabilize.

## Testing decisions

### What makes a good test

- It crosses the module interface used by callers.
- It asserts observable records, findings, eligibility, query results, or publication decisions.
- It survives internal refactoring.
- It is deterministic, network-free, and fast.
- It proves both the safe path and the failure that would mislead a trader.

### Required suites

- Value-object and serialization contract tests
- Source/dataset registry validation tests
- Shared repository adapter contract tests
- Artifact replay tests
- Observation revision and point-in-time query tests
- Quality-rule and disposition tests
- Dataset coverage/freshness tests
- Derived-input eligibility tests
- V1 legacy import reconciliation tests
- Pilot source dual-write reconciliation tests
- Edition critical-contract tests
- Static artifact semantic tests
- Workflow promotion/failure tests

### Existing prior art

- History import/export atomicity and regression guards
- Typed fetch-result state tests
- MAGyP parser shape tests
- Storage/query round-trip tests
- Snapshot JSON-native tests
- Main pipeline exit-code tests
- Freshness timestamp tests
- Scraper fixture tests

### Test isolation rule

No automated test may reach a live source. Source adapters accept injected transport or replay stored fixtures/artifacts. Orchestrator tests must stub every external adapter through one registry fixture so a newly registered source cannot accidentally perform network I/O.

### Coverage rule

The milestone must report coverage for trusted ingestion, source adapters migrated to v2, quality rules, repository adapters, cache building, edition building, and promotion logic. A percentage that omits these modules is not a sufficient gate. The numeric threshold may remain incremental, but every critical interface and rule branch must have named behavior tests.

## Commit discipline

- One behavior change per commit.
- Introduce interfaces before switching production callers.
- Add contract tests with each interface.
- Add adapters behind existing behavior before changing output.
- Run dual-write and reconciliation before cutover.
- Commit generated durable data only through the CI-owned workflow.
- Keep rollback possible until a pilot dataset has completed multiple successful scheduled runs.
- Delete superseded shallow tests only after equivalent interface tests exist.

## Beta service objectives

- At least 95% of expected weekday editions promoted.
- 100% of promoted editions satisfy the critical contract.
- Zero silent stale or date-misaligned substitutions.
- 100% traceability for displayed critical numbers.
- Alert within 15 minutes after a failed edition attempt completes.
- Recovery never depends on reconstructing history from ephemeral logs.

## Milestone definition of done

The Data Trust Foundation milestone is complete only when:

- all DT-01 through DT-20 acceptance criteria pass;
- MAGyP, named soy benchmark contracts, required FX, and USD/MT conversions use the trusted path;
- at least one successful and one deliberately rejected edition have durable manifests;
- a rejected critical edition demonstrably leaves the prior public edition intact;
- every critical displayed pilot number resolves to accepted revisions and artifacts/metadata;
- v1 history is preserved and importable as legacy;
- the dashboard remains usable throughout migration;
- rights-prohibited content is absent from Git and public artifacts;
- the implementation session records deviations from this RFC as explicit decisions.

## Out of scope

- New market sources
- Repairing every currently broken source
- New dashboard pages or redesign
- Accounts, authentication, and customer entitlements
- Public data interfaces
- Intraday streaming
- User-configurable alerts
- Portfolio, order, or execution workflows
- Large analytics rewrites unrelated to provenance
- Full migration of every existing dataset
- Managed database or object-store adoption
- Commercial launch before source rights are resolved

## Risks and controls

### Repository growth

Control with content addressing, metadata-only artifacts, source-specific retention, partitioned ledgers, and periodic measured repository-size checks. External object storage becomes a future decision only when measured Git limits are reached.

### Public repository rights

Control through the shared rights policy. Restricted content is metadata-only or excluded; derived publication remains fail-closed unless explicitly allowed.

### Concurrent bot and human changes

Control with CI ownership guards, workflow concurrency, atomic files, idempotent identifiers, and branch reconciliation before bot pushes.

### Dual-path drift

Control with field-level reconciliation reports and an explicit cutover/rollback switch per dataset. Dual-write is temporary.

### Excessive interface surface

Keep trusted ingestion, repository, quality, query, and edition modules deep. Source-specific parsing and rules remain internal. Callers consume dataset results and accepted queries rather than coordinating substeps.

### Planning staleness

The branch is changing quickly. Each ticket begins by comparing its acceptance criteria to the current baseline. Existing work is credited; incompatible new work becomes an explicit RFC deviation rather than a duplicate implementation.

## Ready-to-paste implementation-session prompt

> Implement the Mirror Market Data Trust Foundation using `docs/plans/2026-08-10-data-trust-foundation.md` as the authoritative RFC. Start by recording the current commit and reconciling DT-01 through DT-20 with changes made since the RFC; credit existing behavior and do not duplicate it. Work in dependency order, one tiny green commit at a time. Do not file or modify unrelated tickets, add new sources, redesign the dashboard, or introduce external infrastructure. Git is the durable source of truth and SQLite is a rebuildable query cache. Preserve the v1 pipeline until each pilot dataset passes dual-write reconciliation and has a rollback switch. Tests must be deterministic and network-free. Complete only the first not-yet-satisfied ticket, verify its full acceptance criteria, report the exact evidence, and stop for review before starting the next ticket.
