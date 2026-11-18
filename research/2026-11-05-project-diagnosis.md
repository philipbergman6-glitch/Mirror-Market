# Mirror-Market diagnosis — 2026-11-05

This is a dated audit and recommendation, not a replacement capability-status table. `ARCHITECTURE.md` remains the canonical status record. Assessment: a substantial, carefully engineered public-data soy intelligence product, with a credible physical-buyer specialization, but without demonstrated real-desk adoption or a complete operational accuracy record. The next milestone should be a buyer reproducing a real cargo comparison, supported by a dependable critical data basket.

## Scope and evidence boundary

Inspected local checkout `d0e8835`, current GitHub main `40140d38` in an isolated temporary copy, project instructions, architecture, source contracts, design, latency, trial protocol, relevant readers and workflows, current issues, production logs, the public manifest and rendered pages. Current main is **36 commits ahead** of the local checkout: 34 data/record commits and two code changes, #568 and #570. Did not change the branch, deploy, alter private records, contact buyers, or change publication policy.

Production references:

- [Daily deploy 41945879263](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/41945879263), November 5.
- [Fast refresh 41962511521](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/41962511521), November 5.
- [DCE refresh 42011317690](https://github.com/philipbergman6-glitch/Mirror-Market/actions/runs/42011317690), cancelled during candidate refresh; subsequent publication steps skipped. Cancellation cause not established.
- Captured [live manifest](https://philipbergman6-glitch.github.io/Mirror-Market/manifest.json): edition **2026-11-05T03:35:59.754351+00:00**. Findings below attach to that edition; live URLs will change.
- [Current source snapshot](https://github.com/philipbergman6-glitch/Mirror-Market/tree/40140d38).

This was not an exhaustive verification of every price, a penetration test, a commercial data-rights opinion, or a buyer trial. Competitors were researched through primary sources, not installed and benchmarked. [Separate competitive review](2026-11-05-physical-soy-competitive-review.md).

## Verified engineering numbers

| Measure | Evidence and limitation |
|---|---|
| 39 production layers | Counted `config.PRODUCTION_LAYERS`; 35 numbered groups plus four sub-layers. This is not 39 independent providers. |
| 8 market descriptors | CBOT, Dalian, Brazil, Argentina, India, Europe, South Africa, Nigeria. Page presence does not imply equal coverage. |
| 13 generated main pages plus briefing | Manifest enumerates 13; promotion contract also checks briefing. |
| 192 test modules on current main | Files matching `tests/test_*.py`. |
| 4,393 passed, 1 skipped in production CI | November 5 deploy's check job; 553.69 seconds, coverage **88.21%**. |
| Local current-main validation | 4,390 passed, one skipped; three failures caused by the downloaded archive lacking Git metadata. After reconstructing the temporary index from the archive's exact file list, those three tests passed. Coverage **88.20%**. Local Python 3.14 versus CI Python 3.12. No code change needed. |
| Lint/type checks | Current main: Ruff passed; mypy found no issues in **162 source files**. An untyped-body note remains. Explicit mypy suppression excludes several app/trust modules from full checking. |
| 28 history CSVs on current main | Versus 26 locally, reflecting SEA/SOPA publication. Current layer-run record: 780 rows, 20 runs across 14 session dates through November 4. |
| Source community footprint | GitHub metadata at audit: public repository, **3 stars, 0 forks**, license field null. These are adoption signals, not a quality score. |

Coverage measures configured product modules, excluding `scripts/`; it is neither complete-system coverage nor proof that live market observations are correct. Initial local-checkout suite: 4,381 passed, one skipped and one sandbox-blocked loopback test; the isolated failing test passed outside the sandbox.

## What is useful today

The public product joins daily futures/FX, physical assessments, government supply-and-demand data, export sales and inspections, positioning, competing oils, river conditions and cyclone guidance. Its strongest property is the explicit meaning of each number: native units retained in storage, USD/MT conversion, bounded historical FX alignment, named shipment periods, same-session spread/crush rules and distinct failure/empty states.

Specific buyer-relevant capabilities exist in code and rendered pages:

- Market propagation and basis comparison, with quote kind and observation dates.
- Named-contract curves, contract calendars, chart panels and whole-contract hedge calculations.
- Crush calculations with leg identity and warnings when delivery months differ.
- Origin costing and sensitivity machinery, with route-specific costs, expiry and ownership of assumptions.
- Public opportunity screens that name blockers rather than imply executable arbitrage.
- Private book, limits, clearing, manually entered options and opportunity workflow, kept outside public output.
- A formal buyer-trial protocol with separate correctness and convenience measures.

The adjusted continuous-series capability has progressed beyond its status-table entry: the live headline explicitly says all three soy technical series use the project's **ratio-adjusted named-contract series**. The workstation carries approximately 500 sessions in the inspected soy contract chart panels. This supplies the required history for more defensible technical indicators; it does not make adjusted historical levels executable prices.

The November 5 deployment also ran the briefing failure relay and logged **“Every briefing block built”**. The canonical table still says no production run executed it. Its reliability row likewise understates the current evidence: the live manifest now contains reliability figures. These are documentation synchronization failures, not missing implementations.

## Current production weaknesses

The captured edition reports **33 successful, one usable-partial and five failed layers**: 84.6% fully successful, or 87.2% including the partial layer. These are a single-edition layer tally, not uptime or accuracy.

| Layer | Observed failure | Last success / implication |
|---|---|---|
| CONAB crop estimates | TLS certificate hostname mismatch on the configured information portal; all three attempts failed | October 20. Brazil crop intelligence has a persistent gap. Resolve the official endpoint/certificate path; do not disable TLS verification. |
| CONAB farmgate prices | Same portal certificate mismatch | October 20. Two layer failures share one upstream dependency. |
| India domestic | `TOKEN_OR_CAPTCHA_REQUIRED`; unattended API path refused | October 8. This is a source-access problem, not something more blind retries solve. Existing #449. |
| EC oilseeds | Expected XLSX failed with “File is not a zip file” | November 4. Need response/content and download-link diagnosis; the log alone does not prove the upstream cause. |
| SOPA crop | Header schema changed to include units: `Sowing Area (Lakh Ha)`, `Expected Yield (Kg per Ha)`, `Estimated Production (Lakh MT)` | October 22. Preserve unit validation while supporting the verified schema. |
| Weather | 23/24 regions returned; Malaysia Sabah failed all three attempts | Partial, not complete. Other recent runs name different missing regions. Do not remove a region from the catalog to make a timeout disappear. |

The successful deploy is compatible with these failures because the system publishes a degraded edition with explicit warnings. That is sensible continuity, but the green workflow badge must not be interpreted as a healthy physical decision basket.

The manifest's completed reliability window is **13 sessions, October 16–November 3**, against a **30-session target**. It reports **9/13 sessions with an edition, 69.2%**. This is its recorded edition-generation metric, not measured public website uptime, and the currently served edition can remain available between builds. The continuous-evidence threshold is false. Availability counts usable partial and legitimate no-publication states as available.

Within that window: FX, DCE, curves, contract bars, CEPEA and MAGyP have 100% recorded layer availability; AgRural, Gulf bids and US processor cash have 92.3%. India domestic is 0%; both CONAB layers 15.4%; SOPA 30.8%. The critical-basket thresholds passing do not certify all 39 layers. A short window and multiple layers sharing providers limit any reliability claim.

## Accuracy and freshness: validated examples

### Independent physical-price spot check

Fetched the November 4 [USDA AMS 3147 PDF](https://www.ams.usda.gov/mnreports/ams_3147.pdf) separately from the project's fetchers. It reports Current Gulf soybean bids, **CIF-B**, average **$13.0575/bushel**. Conversion using 36.74371 bushels/MT gives **$479.78099/MT**. The live CBOT page prints **$479.8/MT**, Current/November loading and NOLA barge terms: this spot check passes value, date and delivery-term interpretation.

The PDF's November soybean settlement is 1193.25 cents/bushel; the workstation's named ZSX26 delayed close is 1198.00. That is **$1.7453/MT** difference. The site does not claim settlement authority, and these are different price types. Its Gulf basis of approximately $39.6/MT is against its named delayed close; do not describe it as the publisher's settlement-based quoted basis. One passed spot check does not validate the whole board.

### Same edition, different soybean reference prices

All inspected pages carry the same edition stamp. The headline shows soybeans **$440.19/MT**, matching workstation ZSX26 at **1198.00 cents/bushel**. The CBOT market page shows **$442.9/MT**, explicitly labelled provider front-month, with **1205.25 cents/bushel**. Native-price difference: 7.25 cents/bushel, approximately **$2.66/MT** before display rounding.

`analysis/loaders.py:enrich_with_technicals` returns the named adjusted series when available, including its Close column. The market-page reference remains the provider front-month. This explains the difference without proving either observation wrong. The headline's main price card does not make its contract identity as obvious as the workstation. Recommendation: carry series/contract identity beside every prominent price, and explicitly distinguish a provider reference from the desk's chosen hedge month. This is a presentation/semantic consistency finding, not a demonstrated conversion defect.

### Stale values need local labels

The homepage correctly warns of source outages in the masthead. Nevertheless its India cards still show a **₹62,000/MT October 8** mandi observation and **239,319 MT arrivals through October 7**, alongside current material. Dates are present; a feed-failed/last-known state is not equally prominent on each card.

A direct in-memory replay of current committed SOPA and freshness CSVs returns `india_crop(..., today=2026-11-05)` as **state=ok**, crop year **2025**, read **October 23**, while the stored layer status is **failed**. The reader checks its **400-day reading-age budget**, not the latest ingestion state. The page labels kharif 2025 correctly, so the value is not fabricated or inherently invalid. But an annual reading's age budget cannot tell the reader that newer crop-year data is currently inaccessible. Surface the failure beside retained historical content and distinguish “last known” from “current checked”.

### Latency needs a buyer deadline

The fast edition's own report gives board acquisition about **8h20m** and DCE about **18h18m**, exceeding the declared six-hour acquisition objective. The successful catch-up run gates **FX only**, intentionally; board breaches remain printed. It would be incorrect to call this a bypass defect without reading the slot policy.

Daily deployment became publicly readable within **104 seconds of generation** in the observed check, an upper bound. Much of the age is therefore upstream/cadence/queue timing rather than HTML generation. The manifest itself has null public-read timestamps, so its pipeline verdict remains unknown; the workflow log provides the separate publication observation.

Choose a specific buyer's morning deadline and measure critical-basket availability by that deadline. A successful late refresh is not proof the morning decision was served. Monthly/weekly observation ages also must not be interpreted as polling outages without considering release cadence.

### Weather is context, not crop-loss prediction

The public page fires point-based dry-condition alerts but repeatedly withholds precipitation-deficit assessments for lack of baseline observations. The fetcher's default is 30 past days; weather is not among the git-history tables, while the rule needs a preceding baseline. The inspected live text reports **0 of 45 required baseline days**. Footprint-weather work remains unmerged/open, and no validated weather-to-yield model exists. Do not market this as production-loss forecasting. Test durable baseline availability before investing in more weather display.

## What the buyer cannot yet rely on

The live Origins page states **“Not comparable”** because freight, insurance, financing, destination costs, quality adjustments and origin-specific charges are absent. This is correct withholding, not a usable delivered-price ranking. US Gulf barge quotes need elevation to vessel; Paranaguá truck-at-terminal indications need origin port costs. Brazil's undated shipment indication cannot automatically rank against dated offers even after costs are entered. Argentina's official FOB is administered, not a firm tradable offer.

There is no verified buyer book or real costed cargo workflow in the reviewed evidence. [#552](https://github.com/philipbergman6-glitch/Mirror-Market/issues/552) remains open and blocked by recruitment [#411](https://github.com/philipbergman6-glitch/Mirror-Market/issues/411). This does not prove nobody has ever used it privately; it means the project cannot support a product-validation claim from its records.

Other boundaries are explicit: no order execution, live counterparty offers, authoritative settlements, live licensed options surface or CTRM contract lifecycle. The trusted ledger has reconciliation tests and production comparisons but no enabled page-reader cutover. CNY trusted ingestion still differs from the ECB-based public reader. Reconciliation of two parsers over shared data is not independent market verification. The independent accuracy-ledger readiness work remains open (#472, #477–#480).

The Players directory is research context, not a verified reachable counterparty network; contact and activity work remains open. A bank account, approved credit line, callable broker or executable offer cannot be inferred from a company entry.

## Competition, open-source readiness and economics

The [competitive review](2026-11-05-physical-soy-competitive-review.md) documents OpenBB/OpenBQ and FINOS Workspace's current open-source position, agricultural tools/public reports, and commercial physical-grain products. The useful competitive frame is the daily buyer task, not stock-terminal feature count. Barchart cmdtyView is a closer physical-grain workflow benchmark; AMIS and USDA plus a spreadsheet are practical public-data substitutes. No evidence establishes that Mirror-Market is uniquely capable or more accurate than these alternatives.

Mirror-Market's potential advantage is the integration of soy-specific quote semantics, shipment windows, consistent units, same-session arithmetic, reasons for withholding and private desk assumptions. These can reduce a buyer's manual reconciliation work. They become a defensible advantage only when buyers demonstrably save time or avoid mistakes.

There is **no root software LICENSE** in either audited snapshot; GitHub reports `license: null`. A public repository alone does not supply an explicit reuse license. An owner-selected license and contributor setup are prerequisites to a clear open-source distribution proposition. Do not add a license automatically as part of an audit.

Data rights are separate. [#570](https://github.com/philipbergman6-glitch/Mirror-Market/pull/570) records your October 21 decision to publish SEA/SOPA despite absent reproduction permission; this audit respects that decision and did not change flags. It is accepted risk, not a granted license. `PRIVATE_LAYERS=()` concerns source data; client records remain structurally private. Open-Meteo commercial-use work is still tracked in #366. “No paid data” is not evidence that every downstream commercial use is cleared.

No current official Bloomberg price was verified. Treat $30,000/year as your comparison assumption, not a verified quote or automatic saving. Illustrative arithmetic only: on a **25,000 MT cargo**, $1/MT is **$25,000** and $30,000 is **$1.20/MT**. A small basis, freight or quality error can outweigh annual software cost. Saving **15 minutes on 220 working days** would save **55 hours/year**; this is a target to measure, not a result achieved. Track actual hosting, maintenance, source-repair and desk-support costs before claiming an all-in cost advantage.

## Buyer experience

The live desktop first screen is legible and disciplined, with dates and an outage banner. Its first signals are RSI oversold warnings, while the physical decisions require navigating to origin costs, shipment terms and next catalysts. My product inference: a physical-buyer morning read should prioritize what changed, which route/window it affects, the next catalyst and the missing input preventing a comparison. Existing #406 already asks for the buyer to decide that contract; no redesign was made here.

The public origins document contains roughly 191,000 extracted text characters, including repeated onboarding commands. That counts hidden/conditional HTML content too and is not a reading-time measurement, but it reinforces the need for a guided private input workflow. A buyer should not need repository knowledge to supply a freight quote or renew an assumption.

The existing public smoke check failed the promotion-age window because it was invoked hours after deployment; that is not evidence of a stale daily edition. It also returned workstation mobile marker `pending` at the 1.5-second browser budget. Repeating that page at five seconds returned `qa-overflow=ok`. No persistent mobile overflow was demonstrated. Do not weaken production checks based on this result; distinguish incomplete probes from measured overflow in diagnostics.

## Claude Code / Codex synchronization

The shared instruction design is good: `AGENTS.md` explicitly binds all models to `CLAUDE.md`. I read that source and used its invariants. GitHub issues and PRs preserve decisions made with Claude; #570 supplied a concrete decision absent from this local checkout. There is no direct live Claude-session communication channel established in this audit, and I do not automatically share Claude's conversation memory or know its uncommitted reasoning.

There are **10 registered local worktrees**, including Claude-associated audit/fix worktrees and other feature branches. Their existence is not proof of active work or errors. The demonstrated synchronization problems are the 36-commit local lag and dated capability entries, not an inability of the models to read the same repository.

Before building a coordination platform, use the existing issue/PR system consistently: every agent records the issue and base SHA, owns a separate branch/worktree, reads current remote decisions before judging status, records exact checks and deployment evidence in its handoff, and updates the canonical capability row when evidence changes. One agent implements; another can independently verify against the spec and external source. Preserve unresolved questions rather than treating a successful merge as successful operation. This matches the role of durable repository instructions described in [official OpenAI workflow documentation](https://developers.openai.com/cookbook/examples/codex/iterating-development-workflows-with-codex).

If a custom coordinator is later justified, it should track task ownership, base SHA, changed files, decisions, test evidence and deployment IDs. It should link the existing issue and status table, not create another authoritative status database. This is a recommendation, not an integration built or tested here.

## Recommended sequence and proof of completion

1. **Restore and clearly label the critical basket.** Diagnose CONAB endpoint/certificate, SOPA schema and EC payload; make an explicit supported-source decision for India domestic. Put last-known/failed state on the affected cards. Completion: fixtures replay actual failures, repaired sources pass production runs, and a failed fetch remains visibly degraded where its number is consumed.
2. **Close quote-identity and independent-verification gaps.** Label headline contracts/series, preserve named loading windows and FX dates, and implement the existing critical-basket verification plan. Completion: independent comparisons include value, units, session, reference contract, shipment terms and documented tolerances; no unexplained material discrepancies in the agreed sample. Parser parity alone does not count.
3. **Run one real cargo exercise now.** Recruit one buyer for #552, with one destination, product and open shipment period, using their own private commercial inputs. Have the buyer reproduce landed costs and hedge interpretation independently before seeing the site's answer. Completion: a sanitized comparison record, timed attempts, every difference explained or filed, and no invented assumptions. Recruitment requires you or an authorized commercial contact.
4. **Run the existing two-buyer trial.** Protocol already specifies 30 trading days, at least two eligible buyers, at least 20 sessions each, five task types and eight real cargo/basis decisions per buyer. Completion: measured decision correctness, external lookups, time and failure recognition, including unsuccessful attempts; no premature replacement claim.
5. **Make repeat use easy and the distribution proposition clear.** Buyer-approved first screen, guided private assumption entry/expiry, installation and recovery instructions, owner-selected software license and an explicit source-rights inventory. Establish a morning service objective and an accountable source-repair routine. Completion: a second desk can set up and use it without an engineer interpreting every field.

Do not spend the next cycle adding markets to increase a source count. The highest-value milestone is a reproducible real cargo comparison delivered reliably before the buyer's decision deadline. The engineering foundation supports pursuing that milestone; the available evidence does not yet show it has been achieved.
