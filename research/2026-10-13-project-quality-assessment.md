# Mirror Market — project quality assessment

Date: 2026-10-13. Author: Codex. Assignment: assess integrity, reliability,
accuracy and the path to leading daily soy intelligence for physical buyers.

## Verdict and grading method

**B overall for the stated daily public-data intelligence mission.** Strong
engineering safeguards and breadth; insufficient evidence for an A in numerical
accuracy, sustained operations or buyer usefulness. This is an ordinal expert
assessment, not an accuracy percentage, certification, or measured competitor
ranking. A means independently demonstrated performance over time; B means
substantial working capability with material evidence or coverage gaps; C means
partial capability or weak validation. Unknown is not the same as defective.

Do not grade this project against execution, CTRM or real-time terminal promises
it explicitly does not make. Conversely, narrowing its mission cannot excuse
incorrect numbers, stale critical inputs or unusable cargo comparisons.

| Dimension | Grade | Evidence and limitation |
|---|---|---|
| Integrity and honest semantics | A− | Shared price types; missing inputs withheld; unfinished bars dropped; native-unit storage; privacy boundary. Enforcement is substantial, but legacy/trusted paths still disagree. |
| Calculation correctness controls | B+ | Central unit/FX rules, named-contract crush and shipment-window checks; extensive tests. No complete independent source-to-screen numerical sample performed in this audit. |
| Actual numerical accuracy | Not certified | A percentage requires sampled comparisons against source publications, explicit tolerances and a denominator. Coverage and passing tests cannot supply that percentage. |
| Provenance and revision reproducibility | B− | Revision ledger, quarantine and pilot reconciliation exist. Legacy reads remain the default; FX pilot currently fails. Full historical edition replay was not demonstrated. |
| Operational reliability | B− | Successful deployment and live browser check; 37/39 sources successful, one partial, one failed. No fresh longitudinal availability/recovery calculation here. |
| Timeliness | B | Venue guards, source age budgets and scheduled refreshes exist. Latest CI observed publication within 100 seconds of generation; this excludes provider delay and is not a long-run SLO. |
| Soy coverage | B+ | Broad beans/oil/meal, origins, supply-demand, positioning, trade, energy and logistics coverage. Layer count does not measure buyer-critical coverage; India failed this run. |
| Physical cargo decision support | B− | Comparable delivery terms, cost stacks, dated private assumptions and named hedges are modeled. Real freight/quality/finance inputs and buyer outcomes remain prerequisites. |
| Weather decision value | C+ | Point rules and cyclone exposure exist; recent missingness/timezone fixes deployed. Point data remain partial; production-weighted footprint weather is specified, not built. |
| Public UX and interpretability | B provisional | 14 URL promotion/browser checks pass; shared explanations and blocked states exist. No fresh accessibility audit or timed buyer usability study. |
| Software quality and maintainability | B+ | Latest deployed CI: 3,925 tests passed; 87.65% measured coverage. Large central modules, selected mypy suppressions and scripts outside coverage remain limitations. |
| Privacy/security controls | B provisional | Static public surface, structural separation of private files, template autoescape. No penetration test, complete dependency audit or account-permissions audit performed. |
| Rights and source governance | B− provisional | Explicit rights gates and source semantics. No fresh legal clearance of every published source; delayed/EOD exchange data still requires use-specific rights review. |
| Buyer/commercial validation | C | Public operating plan last checked Oct 8 says trial has not started. No private participants or commercial records inspected; current owner confirmation still needed. |

## What was actually checked

Local checkout: `03c7e7b`, with pre-existing uncommitted onboarding changes and
`data/soy.db` preserved. Reviewed mission/invariants, architecture and source
reference, roadmap, latency/design structure, CI/deploy/refresh configuration,
cleaning, quarantine, units, FX alignment, trusted-read switches, promotion,
named-contract crush, origin comparison and public trial operating plan.
This is a system-wide risk review with targeted code inspection, not a line-by-line
proof of every module or every source observation.

Production advanced independently to `4616ded`. GitHub compare showed seven
commits after the local baseline: history snapshot, scope/docs changes, verified
cache separation, weather provenance/timezone correction, and weather gap/window
corrections. The old weather and cache defects are therefore not reported as
unfixed production defects. Full remote source was not checked out over local work.

Primary operational evidence:
[deployment 38398229567](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/38398229567).

- CI passed 3,925 tests with 87.65% coverage (display rounds to 88%).
- At 12:50 UTC the pipeline reported 37/39 successful layers, usable-partial
  weather, and failed `india_domestic`. Agmarknet returned HTTP 400 for the
  September requests for both states. Cyclone agencies and COT succeeded in this run.
- MAGyP reconciliation matched 18 rows; CBOT matched six legs for each soy product.
  This checks reader agreement, not independent exchange correctness.
- The separate `reconcile-trusted` job failed: ZAR/USD on October 9 had one
  row only in v1, none in trusted; two FX revisions were rejected. BRL, CNY
  and INR reconciled. This job is deliberately nonblocking, so workflow green
  does not imply reconciliation green. Do not relax quality gates to obtain green.
- Candidate and served-site promotion passed 14 URLs. CI observed the 12:51:01
  UTC edition publicly readable within 100 seconds, an upper bound.
- Independent local public browser smoke also passed 14 URLs for that edition.
  Its 674-second reported bound includes the later time this audit asked;
  it is not evidence that publication took 674 seconds.

Local validation:

- `.venv/bin/ruff check .`: passed.
- `.venv/bin/mypy analysis pipeline fetchers`: passed, 158 source files;
  configured ignored modules and untyped function bodies limit that claim.
- `.venv/bin/python -m pytest -q`: 3,904 passed, two failed, 88% coverage.
  `test_loopback_is_not_the_network` failed on sandbox socket binding and
  passed alone outside the sandbox. The remaining failure is real integration:
  `test_every_production_renderer_uses_the_shared_environment` detects the
  pre-existing uncommitted `scripts/project_context.py` environment constructor.
  Decide the correct offline/public renderer boundary and add a narrow test;
  do not disable the production guard. No production defect is inferred from
  this uncommitted-file conflict.
- Public smoke required network/browser sandbox escalation; the initial DNS
  denial was an environment restriction, not a service outage.

Logs for this session are under `/tmp/mirror-grade-deploy.log`,
`/tmp/mirror-project-grade-tests.log`, `/tmp/mirror-grade-smoke.log`.
They are temporary; the linked run and this summary provide durable pointers.

## Highest-impact findings

1. **Accuracy evidence is the largest missing asset.** Existing gates are good,
   but no current sampled accuracy rate was established. Build an independently
   recalculated benchmark basket before advertising exceptional accuracy.
2. **Health has several independent meanings.** Current delivery works while
   India is failed, weather partial and FX migration divergent. Report delivery,
   source completeness, economic comparability and reconciliation separately.
3. **Finish provenance where decisions depend on it.** Earn cutover per dataset
   with explained differences, accepted revision identity and rollback drills.
   Re-fetching the same provider twice confirms persistence, not independent truth;
   quarantine release should retain this distinction for material corrections.
4. **Cargo usefulness depends on comparable commercial inputs.** The origin
   module appropriately refuses unknown shipment windows and incomplete cost
   stacks. Improve input availability and onboarding without weakening those rules.
5. **Weather breadth exceeds demonstrated agronomic skill.** Implement the
   accepted footprint spec before claiming area-wide crop insight. A rolling
   recent baseline must not be presented as long-term climatology. Quantified
   weather-to-yield modelling was explicitly deferred in remote roadmap #461;
   do not reopen it as an incidental audit fix.
6. **Coordination is now an integration risk.** Remote #465 introduces AGENTS.md
   and an architecture capability table while local onboarding introduces its
   own AGENTS.md and state generator. Reconcile those concrete differences in
   an isolated integration change before committing; avoid duplicate authorities.
7. **Test coverage needs interpretation.** `scripts/` is excluded, and selected
   trust/site modules suppress mypy errors. Prioritize executable tests around
   publication, history recovery and numeric lineage rather than inflating count.

## Parameters for the quality dashboard

All targets below are proposals, not existing measured results or industry
standards. Agree the critical basket and morning cutoff with actual buyers.

| Parameter | Definition / acceptance evidence |
|---|---|
| Critical-input availability | Eligible, current required inputs by buyer cutoff / all expected input-mornings; report each feed and bundle, including missed mornings. Proposed initial target ≥99% over a rolling 60 trading days. |
| Numerical agreement | Independently verified published values / sampled values, stratified by product, venue, units, FX, contract and revision. Require zero unexplained material errors in the critical basket; publish counts and tolerances. |
| Materiality | Dataset-specific tick/rounding tolerance plus economic significance in USD/MT; a date, contract, grade or incoterm mismatch fails even if its value is close. |
| Provenance coverage | Critical displayed values with source identifier, native unit, observation/publication/retrieval times, revision ID and transformation/input lineage / all critical values. Target 100%; unknown publication time remains explicitly unknown. |
| Temporal correctness | No future FX; no cross-session spread/crush; correct timezone, release availability and contract mapping. Replay both holiday and roll boundaries. |
| Completeness | Required keys, regions, contracts and window days present / expected; distinguish missing, stale, no publication and confirmed zero. |
| Latency | Provider release→fetch, fetch→eligible, eligible→published, published→reader; median/p95/max by source and release cycle, not one pooled number. |
| Revision handling | Detection delay, affected outputs identified, correction delay and reproducibility of superseded editions. Test revised/removed source rows. |
| Cross-source discrepancies | Same-definition comparisons only, with cause/owner/age; disagreement between USDA and CONAB vintages is not automatically error. |
| Recovery | Clean reconstruction from retained artifacts; corrupt/missing cache and failed-deploy drills; record observed RPO/RTO. Proposed initial recovery target: before next agreed morning cutoff. |
| Analytical validity | Compare signals to simple baselines with time-separated evaluation; record false alerts, missed events and withheld results. Avoid hindsight from revised reports. |
| Commercial comparability | Product/grade/location/incoterm/shipment/pricing window/FX/cost expiry all present; show break-even freight and quality sensitivity before ranking actionable routes. |
| Rights | Current source-use evidence for retrieval, storage, display, redistribution and derived output; renewal/review owner and dates. |
| Privacy/security | No private artifacts in publication bundle; scoped workflow permissions; secret/dependency scanning and dependency provenance; backup access review. These require a separate verification pass. |
| Usability/accessibility | Timed buyer tasks, keyboard/navigation/screen-reader checks, mobile overflow, number/date readability and successful interpretation of degraded states. |
| Buyer value | Existing protocol's time saved, external lookups, critical errors, real decisions and task success; use all eligible sessions and predeclared thresholds. |
| Maintainability/cost | Incident recurrence, time to repair a source change, build duration, source maintenance hours and cost per reliable critical input. |

## Sequenced improvement plan

### First 1–2 weeks: establish a defensible baseline

- Owner: pipeline maintainer. Reproduce India HTTP 400, fix the source contract
  with representative fixtures, then record successful runner recovery. Investigate
  weather transport failures separately from the now-fixed missingness rules.
- Owner: trust maintainer. Inspect the exact ZAR rejected revisions, trace effects
  on current conversions, and resolve source/quality-policy discrepancy with evidence.
  Keep the trusted switch off until its cutover conditions pass.
- Owner: integration maintainer. Reconcile local onboarding with remote #465;
  resolve the renderer test conflict without broad guard exemptions.
- Owner: analyst plus independent reviewer. Define a small critical soy basket;
  independently trace native quote→unit conversion→FX→derived value→rendered label.
  Include revisions, holidays, rolls, thin days, missing inputs and November crush.
  Save discrepancies and reviewer signoff. Do not fabricate an accuracy percentage.

### Following 2–6 weeks: make every important answer auditable

- Complete critical-dataset lineage and revision history; retain allowed raw
  evidence or source hashes/identifiers according to rights. Reproduce an old
  morning edition with only information available at that time.
- Instrument buyer-cutoff availability and source-release-aware freshness.
  Annual/crop-year tables need release-vintage checks beyond their year keys.
- Add a plain-language correction ledger and an incident runbook with clear
  owner, affected numbers, detection time and recovery evidence.
- Rehearse loss of DB/cache, corrupted history and rejected publication using
  isolated fixtures; preserve the repository's no-cloud-DB constraint.
- Review dependency locking/hashes, workflow action pinning and narrower type
  suppressions in a dedicated engineering pass. These are hardening proposals,
  not claims that exploitable vulnerabilities were found.

### Next 30-trading-day buyer trial: prove decision value

- Use the existing protocol, two independent physical buyers and private inputs.
  The owner supplies participants and commercial assumptions; agents can validate
  software but cannot manufacture participants or field results.
- Perfect one real origin/destination/product workflow: matching shipment windows,
  landed costs, quality, FX and a separately specified hedge reference.
- Test whether the first screen answers: what changed, why it affects my cargo,
  what I should verify next and what prevents action. Buyer/design approval
  remains necessary before changing the established visual contract.
- Prioritize a narrow official-source change digest, release-vintage comparisons,
  China demand reconciliation and route context only against unmet buyer tasks.
  The news block remains reserved until a text-source contract is accepted.

### Then: selective differentiation

Implement production-weighted footprint weather under its accepted specification;
validate it before stronger scientific claims. Expand physical pricing or source
rights only where a demonstrated workflow needs them and commercial funding covers
the commitment. Do not spend first on additional countries, generic indicators,
execution infrastructure or unsupported yield predictions.

## What would justify an A

An independently reconciled critical basket; 100% critical lineage; sustained
measured by-cutoff availability; zero unexplained material numerical discrepancies;
demonstrated correction and recovery procedures; and a successful buyer trial
under the existing protocol. These are independent gates—excellent UI cannot
compensate for a wrong contract month, and excellent tests cannot establish demand.

External benchmark and primary-source references are in
[the companion research](2026-10-13-soy-platform-benchmark.md). USDA's
[WASDE](https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report)
and [transportation report](https://www.ams.usda.gov/services/transportation-analysis/gtr)
support release-aware fundamentals and logistics context; the
[CME policy center](https://www.cmegroup.com/market-data/license-data/market-data-policy-education-center.html)
is the primary reference for exchange data-use policy, including delayed/EOD use.

No product code, source history, private desk records, production settings or
deployment was changed by this assessment. No exhaustive number reconciliation,
penetration test, legal clearance or competitor trial was performed.
