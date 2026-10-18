# Benchmark for dependable daily soy intelligence

**Date:** 2026-10-13  
**Author:** Codex research agent  
**Scope:** Physical buyers making daily cargo, basis, origin and hedge decisions; public primary sources. This is a benchmark and proposed acceptance criteria, not a numerical certification of Mirror Market or a full competitor trial. No private desk records were inspected.

## Finding

The strongest achievable product is a reproducible daily answer to: **what changed, what does that do to comparable cargo economics, and how trustworthy is that answer?** This recommendation follows the existing mission in [CLAUDE.md](../CLAUDE.md). Adding more feeds alone does not establish accuracy. A top-tier claim needs measured numerical agreement, dated operational reliability and actual buyer validation.

## Source-backed benchmark

| Area | Primary-source evidence | Implication for this product (recommendation) |
|---|---|---|
| Fundamentals and event timing | USDA describes WASDE as monthly annual supply/use forecasts and publishes a release calendar, report files and historical revisions. Its current page identifies October 9, 2026 as the October release. [USDA WASDE](https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report) | Attach report vintage, crop/marketing year and expected next release to each number. Show change from the previous release, and distinguish revisions to history from a new forecast. Score freshness against the release calendar, not merely crop-year labels. |
| Sales versus physical movement | USDA's export-sales program separately describes weekly exporter reporting and daily reporting obligations for large transactions. [USDA FAS export sales](https://www.fas.usda.gov/programs/export-sales-reporting-program) | Preserve sales, outstanding commitments, shipments, inspections and customs flows as distinct measures. Never add unlike flow measures into an apparent total. A source publication delay is distinct from an ingestion outage. |
| Brazil crop intelligence | CONAB describes ongoing crop monitoring and monthly grain-crop and agricultural-monitoring bulletins, including soy. [CONAB grain crops](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/safra-de-graos) | Compare USDA and CONAB using explicit vintages and crop definitions; explain disagreements rather than averaging them. Link planting/harvest and production context to origin comparisons. |
| Argentina crop intelligence | Argentina's agricultural open-data portal lists weekly/monthly reports and historical series for major crops including soy. Its crop monitor covers area, production, planting, harvest and condition. [Open data](https://www.argentina.gob.ar/economia/agricultura/datos-abiertos), [monitor description](https://www.argentina.gob.ar/noticias/agricultura-actualiza-el-monitor-de-estimaciones-agricolas) | Evaluate Argentina crop/harvest coverage separately from Argentina FOB prices. A price feed is not evidence that the country's supply intelligence is complete. |
| Trade detail | MDIC provides the official foreign-trade statistics portal and open datasets used for Brazil's trade balance and Comex Stat. [MDIC statistics](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas) | Preserve product codes, destination, reporting period and revision behavior. A monthly customs unit value is not a current offer or independently observed spot price. Check the existing Brazil layer before proposing duplicate ingestion. |
| Freight and logistics | USDA's weekly Grain Transportation Report covers volume and price information for barges, railroads, trucks and ocean vessels; on inspection its latest linked issue was October 8, 2026. [USDA GTR](https://www.ams.usda.gov/services/transportation-analysis/gtr) | Use weekly route benchmarks as dated context. A delivered-cost calculation needs route, shipment window, freight basis and costs with comparable meaning; a broad ocean index does not establish a cargo's bookable freight. |
| Exchange authority and publishing | CME's policy center describes licensing of real-time, delayed and end-of-day data, distributor agreements, historical distribution, non-display use and entitlements. It links June 2026 agreements and an August 2026 Schedule 5. [CME policy center](https://www.cmegroup.com/market-data/license-data/market-data-policy-education-center.html) | Maintain explicit rights evidence per source and use. Do not upgrade a vendor daily close to an exchange-authoritative settlement. Buying approved daily settlement access may be valuable if buyer validation justifies it; real-time infrastructure is not required by this mission. |

The source facts above establish the benchmark's ingredients, not a claim that every source must be added. Source comparison, publishing rights and practical usability require dataset-specific verification. The FAS full-page open returned HTTP 403 in this research tool; its official search result supplied the narrow reporting-program description. No report values were independently reconciled here.

## What to measure before calling it top tier

These are proposed acceptance criteria, not observed results or external industry standards.

1. **Numerical correctness:** a stratified reconciliation sample covering each important product, venue, unit, month, FX convention and publication revision. Record comparisons, tolerances, exceptions and denominator. Require zero unexplained material discrepancies in the critical daily panels; do not report a global accuracy percentage from unit tests.
2. **Reproducibility:** each critical displayed number resolves to source document/payload, observation time, publication time when known, retrieval time, native units, transformation version and input identities. Rebuild an earlier briefing using only information available then.
3. **Decision availability:** publish the fraction of mornings on which the required buyer basket was available by an agreed cutoff. Report critical-data coverage separately from HTTP uptime and workflow success. Count stale carry-forward honestly.
4. **Correction handling:** measure detection and correction times; publish a readable correction record; test amended releases, removed rows, duplicate rows and source schema changes. A previously published number must remain auditable.
5. **Comparable economics:** require grade, location, incoterm, shipment period, exact futures month, FX date and cost assumptions for actionable origin comparison. Withhold a rank when a missing input could reverse it. Show sensitivity to uncertain freight and quality costs.
6. **Weather validity:** separate observation, forecast and climatology; require completeness and production/crop-stage relevance. Evaluate alerts against historical outcomes and a simple baseline before publishing quantified yield impact.
7. **Buyer usefulness:** run the existing trial with real physical buyers. Measure time to answer recurring questions, decisions influenced, false alerts, withheld answers and explanations buyers cannot understand. More indicators are not a success metric.
8. **Continuity:** demonstrate recovery from a clean environment and lost cache, source failure, corrupted artifact and failed deployment. Set recovery objectives, rehearse them, and retain dated evidence.

## Practical order of investment

**First:** establish the critical morning basket; resolve current operational defects; reconcile sample numbers end to end; expose unavailable or stale inputs clearly. Existing semantics, missingness and licensing invariants are appropriate foundations.

**Second:** complete provenance and revision handling for the critical basket; add source-release-aware freshness; prove recovery and 30 consecutive days of measured service. Thirty days is a proposed observation window, not a guarantee of long-term reliability.

**Third:** validate comparable origin/landed-cost workflows with buyers; prioritize Argentina crop context, China demand or route detail only where those trials reveal unanswered material questions. Repository documentation already lists substantial trade, freight and crop coverage, so validate usability before expanding.

**Fourth:** implement the accepted production-weighted weather specification and evaluate its alerts. Commission additional settlement or physical-price rights only against a clear need and budget. Keep execution, CTRM and speculative real-time expansion outside this benchmark.

## Local evidence and limits

Reviewed [CLAUDE.md](../CLAUDE.md), [LAYERS.md](../LAYERS.md), [collaboration/state.yaml](../collaboration/state.yaml), and the earlier [August benchmark](2026-08-14-soy-trader-bloomberg-replacement-benchmark.md) at checkout `03c7e7b`, with existing uncommitted work preserved. The dated state says trusted-reader migration and buyer validation remain partial and footprint weather is specified; these are handoff claims for the main audit to verify, not fresh production findings. This research changed only this note. No tests are needed for an evidence-only document; the parent audit owns session handoff/context checks.
