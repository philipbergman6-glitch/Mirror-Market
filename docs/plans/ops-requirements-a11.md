# Ops requirements for the A11 re-audit

**A9 · [#306](https://github.com/philipbergman6-glitch/Mirror-Market/issues/306)** on map "The narrowed A" [#296](https://github.com/philipbergman6-glitch/Mirror-Market/issues/296). Input to **A11 · engineering re-audit** [#315](https://github.com/philipbergman6-glitch/Mirror-Market/issues/315), whose checklist is "the 11 audit dimensions rescoped to the narrowed claim + the ops bar from #306". Evidence as of **2026-10-13**, `main` at 4616ded. Every count below comes from code or config at that commit, not from prose.

This file is a requirements list, not a build. It changes no code or workflow. Each item names what A11 checks, the state today, and where the gap is. The implementation tickets graduate from the map's "Operations implementation work" fog.

## The bar (decided 2026-10-13 by the owner on #306, not re-opened here)

1. **The settlement guard stays.** It sets the ≈1 h 15 m floor (`LATENCY.md` §9). An unfinished bar is never published (invariant 11).
2. **Same day, honestly stamped.** An edition that lands late because GitHub's cron drifted is not a breach, as long as every stamp on the page is truthful. There is no fixed HH:MM deadline.
3. **Drift is reported on the page, never paged.**

Glossary. **Paged** means any notification that reaches the owner: a red workflow run (GitHub's failure email) or a comment on a rolling alert issue. **Reported** means shown on the published edition: the masthead, the Layer Freshness table, or `manifest.json`.

## 0. Bar conformance: what A11 checks first

| # | Requirement | Check | State today |
|---|---|---|---|
| 0.1 | Settlement guard drops unfinished bars on every path (daily, fast, dce dispatch) | `fetchers/_settlement.py` tests pass; both pipelines call it | Met. Fast refresh is "the same code … with two arguments different" (`ARCHITECTURE.md` "Fast refresh") |
| 0.2 | Every stamp is truthful | For ≥5 sampled live editions, compare masthead generation time, "priced from data N old", and Layer Freshness Observed/Fetched/Age/Last Success against that edition's `manifest.json` `coverage` + `latency` | Surfaces exist (`LATENCY.md` §6). No automated stamp-vs-manifest consistency check exists. `tests/test_one_edition_stamp.py` (#444) pins the generation stamp across pages, not the data-age stamps against the manifest. **Gap: A11 runs this check by hand, or a test is added** |
| 0.3 | No red run is caused by scheduler drift alone | List every red `Refresh Prices` / `Update Dashboard` run in the A11 window and classify each by cause | **Partly met.** The 20:07 slot passes `--late-start-is-not-a-breach` and the 23:30 slot is ungated (`refresh-prices.yml` "Select layers"). **Gap:** the dispatched `dce` run (cron-job.org, 10:30 UTC) is gated with no late-start allowance ("Nor does a manual dispatch"), so a late fire from the outside scheduler is a red run, which pages drift. The 08:00 backstop's red is a missing trigger, not drift, and is allowed under the bar |
| 0.4 | "Same day" is measured and reported, not gated | Share of weekday sessions whose first edition carrying session D's CBOT close was published on UTC day D, over the A11 window | **Not measured today.** No report computes it. `LATENCY.md` §3 still states 6 h objectives that redden runs. Under the bar they become reporting targets, except a stale bar or a pipeline breach, which still fails. **Assumption, flagged for the owner:** "same day" means UTC calendar day D for the edition carrying D's close. Treat it as a reported rate with no pass threshold |
| 0.5 | Breach and drift are visible on the page | Masthead shows the data age; manifest carries each verdict + basis string | Met (`LATENCY.md` §6) |

## 1. Frozen-source detection

**Question.** Does every fixed-URL and rotating-URL source have an age budget today?

**Answer, from config.** Yes at layer level, with 7 written exemptions. `config.validate_age_budgets()` runs at import and refuses to load if any of the 39 `PRODUCTION_LAYER_KEYS` is in neither map, or in both. Counts at 4616ded: **32 budgeted** (`LAYER_MAX_DATA_AGE_DAYS`), **7 exempt** (`LAYER_AGE_BUDGET_EXEMPT`), 0 unlisted.

A layer past its budget is graded `stale` (`main._mark_stale`). That stops `last_success` advancing, adds the layer to `_HARD_FAILURES`, and lists it under "Stale last-known-good data" in the rolling `ci-alert` issue (`scripts/ci_layer_alert.py`). The stale state is reported on the page, and it also triggers the same-day retry (`retry-failed-pipeline.yml` reads the same `hard_failures`). The `ci-alert` comment does reach the owner. But a frozen feed is an outage, not drift, so the bar allows it. Requirement: frozen feeds stay in that one rolling issue and add no new paging channel.

URL kind is not a config attribute. The fixed/rotating split exists only in `LAYERS.md` prose, so A11 cannot check invariant 10 from config. (See requirement 1.4.)

### 1.1 Exempt layers: no frozen-source detector at all

| Layer | Exemption reason (config) | Can it freeze silently? | Requirement |
|---|---|---|---|
| `wasde` | "keyed by marketing year with no date column" | Yes: an ESMIS release list or xls that stops updating | **The exemption reason looks stale.** Each row carries `reference_period`, the release month as an ISO date (`fetchers/wasde.py` `_assign_reference_periods`; schema PK). It is not in `main._DATE_COLUMNS`, so it is not read for dating. A budget of ~45 days (monthly release + slack) looks feasible. Unverified until built. |
| `psd` | "keyed by marketing year with no date column (verified against the live schema)" | Yes: bulk CSV zips at a fixed URL (`apps.fas.usda.gov/psdonline/downloads/…`), the fixed-filename trap | The bulk CSV carries a `Month` vintage column (`fetchers/psd.py` comment at line ~101) that is not stored. Storing it would make PSD datable. **Gap.** |
| `usda` | NASS annual, keyed by crop year | Low risk: QuickStats API query, not a file | Accept the exemption. Note it in A11 as low risk. |
| `crop_progress` | Seasonal: nothing published Dec–Mar | Yes, in season. `week_ending` is a real date column | An **in-season budget** (e.g. 14 days, April–November, `no_publication` otherwise) would close it. **Gap.** |
| `sopa_crop` | One kharif estimate per crop year | Yes, and its revision path is also lost (§2) | Private layer, one annual estimate. Accept the exemption. Low decision weight. |
| `cyclones_nhc`, `cyclones_jtwc` | Status frame dated today by construction; advisory age graded in hours by `analysis/hazards.py` | Covered by another mechanism | Not a gap. A11 checks that `hazards.py` grades advisory age (spec `docs/specs/cyclone-hazard-flags.md`). |

### 1.2 Per-key freezes hidden by a layer-wide budget

`main._latest_observation_date` dates a layer by its **newest row across all keys**. One frozen key passes behind a fresh one. `LAYERS.md` (Layer 29) states the trap: "a frozen price feed would otherwise hide behind a fresh generation file". Two mechanisms partly close it:

- **`epa_rfs`**: per-key budgets enforced inside the fetcher (`config.EPA_RFS_KEY_MAX_AGE_DAYS`). Graded.
- **`analysis.health`**: per-key age **report**, not graded. `_build_commodity_status` covers 10 tables: `prices`, `cot`, `weather`, `currencies`, `dce_futures`, `worldbank_prices`, `forward_curve`, `india_domestic_prices`, `brazil_spot_prices` (cepea, agrural, conab_precos), `safex_prices`. `_check_flat_prices` (same Close 3 sessions running) covers `prices` only.

Multi-key layers with **no per-key age check of either kind**:

| Layer | Keys | Independent upstreams per key? | Risk |
|---|---|---|---|
| `fred` | 9 series (`FRED_SERIES`) | Yes: each series is published separately and can be discontinued alone | **High.** A discontinued series freezes behind 8 live ones |
| `contract_bars` | per named contract, 9 commodities | Yes: Yahoo can stop updating one listed ticker | **High.** The config comment says the budget catches "a run of listed tickers all going quietly stale", not one |
| `river_us` | 2 gauges (Memphis, St. Louis) | Yes: separate USACE/USGS readings republished by NWPS | **Medium.** `LAYER_MIN_KEYS=2` catches an absent gauge, not a frozen one. Memphis is the leg the basis trades off |
| `sagis` | soybeans, sunflower | Yes: one DT export file per commodity | Medium |
| `eia` | 3 series | Yes | Medium |
| `forward_curve` | 9 curves | Health covers `forward_curve` per commodity by `fetched_date`, which is fetch time, not observation date | **Medium.** The layer budget reads `observation_date` correctly, but the per-curve health report reads fetch date, so it calls a curve fresh when its legs are old |
| `gulf_bids`, `magyp_fob`, `us_processor_cash`, `sea_india`, `export_sales`, `comexstat`, `gtr_*`, `conab`, `cec` | several | No: every key arrives in one document per run, so they freeze together | Low. The layer budget is sufficient |

**Requirement 1.2.** Every multi-key layer whose keys come from independent upstreams carries a per-key age verdict that reaches the page: the five High/Medium rows (`fred`, `contract_bars`, `river_us`, `sagis`, `eia`) plus the `forward_curve` dating fix. Graded per key like `epa_rfs`, or reported per key like `analysis.health`. Under the bar, **reported is enough**. A11 checks the list above is empty or each remaining row has a written acceptance.

### 1.3 Identical-value repeats (a frozen page that keeps its date fresh)

A2 #299 deferred frozen-source *repeats* to A3/invariant 10. Today only `prices` has a flat-value check (`_check_flat_prices`). Stale-serving pages that re-serve the previous session's rows are dated correctly and caught by budget, for example SAFEX (#157). A page that re-stamps today's date on yesterday's number is not caught anywhere. **Requirement 1.3**: A11 records, per daily physical leg (`cepea`, `agrural`, `gulf_bids`, `magyp_fob`, `safex`, `india_domestic`), whether that failure mode is possible for its upstream. Where it is, a flat-value report like `_check_flat_prices` is extended to that table. Unverified per source. No incident observed.

### 1.4 URL kind is prose only

**Requirement 1.4.** Each layer's URL kind (`api` | `fixed_url` | `rotating_url`) is declared in config, next to the layer roster. A test then asserts that every `rotating_url` layer resolves its link from a landing page and every `fixed_url` layer has a budget. The prose today names rotating: `worldbank`, `ec_oilseeds`, `sagis`, `sagis_smd`, `epa_rfs` (rin_generation), `cec` (listing-resolved). It names fixed: `gtr_ocean_freight`, `gtr_vessels`, `river_us`, `river_ar`, `psd` (bulk zip), `conab`/`conab_precos` (`portaldeinformacoes.conab.gov.br/downloads/arquivos/*.txt`), `gulf_bids` (`ams_3147.pdf`), `crush_inspections` (`wa_gr101.txt`). All of them except `psd` carry a budget today.

## 2. Point-in-time reproducibility

**Question.** Which rendered numbers cannot be rebuilt from committed history + code at a given date?

**What is committed.** `pipeline/history.py` `HISTORY_TABLES` holds 25 tables, including three edition snapshots: `briefings` (with `snapshot_json`, one raw-number block per briefing section), `origin_rankings`, `opportunity_detections`. Code is in git.

**What is not.** 24 of the schema's tables are not in `HISTORY_TABLES` (`pipeline/schema.py`, 49 real tables). The generated site is not tracked: `.gitignore` ignores `docs/*.html`, `docs/markets/`, `docs/manifest.json`, `docs/assets/`. GitHub Pages serves only the newest edition.

### 2.1 Editions carry no build identity

No edition records the commit it was built from. `app/manifest.py` writes mode, `generated_at`, layers, coverage and latency, with no SHA. No workflow reads `GITHUB_SHA`. `trust.domain.Run.code_revision` holds hardcoded strings (`"mirror-market-fx-trusted-v1"`). The only `git rev-parse HEAD` capture is in `analysis/trial/release.py`. **Requirement 2.1**: `manifest.json` carries the commit SHA, the workflow run id, and the verified-seed cache key the edition was built on. A11 checks a live manifest for all three.

### 2.2 Published editions are not archived

Pages overwrites the previous edition. `manifest.json` lives only on Pages and in the short-lived Pages upload artifact. **Requirement 2.2**: the per-edition `manifest.json` is retained in a durable place. The candidate is a `data/history/`-style committed file written by the deploy workflow only. That respects invariant 6 (no cloud DB) and the history-guard. Without it, "what did the page say on the 12th" has an answer only for the briefing and the two snapshot tables.

### 2.3 Rendered tables rebuilt each run from an upstream that revises

These tables self-heal, but only to **today's vintage**. A rebuild at date D reproduces what the upstream says now about D, not what the page showed on D.

| Table (layer) | Rendered by | Revises? (evidence) | Per-run window |
|---|---|---|---|
| `prices`, `currencies` (1, 7) | `analysis/loaders.py`, `app/block_builders.py` | **Yes, observed.** #447: yfinance revised the closed 2026-10-09 ZAR bar. The quarantine guard is inert on CI's empty DB by construction (`ARCHITECTURE.md` capability table) | 15 y daily build, `1mo` fast refresh |
| `economic` (3, FRED) | `analysis/briefing/sections/economic.py` | Yes for revised series such as CPI. Daily market rates rarely revise. ALFRED serves vintages. Unverified per series | from 2020-01-01 |
| `psd` (6) | `app/block_builders.py` | Yes, monthly. The `Month` vintage column is not stored | full bulk CSV |
| `usda` (2) | briefing `usda.py`, `nass_crush.py` | Yes. NASS forecasts revise to final | crop years 2020+ |
| `cec_estimates` (25) | `soy_analytics.py` emerging markets | Yes, monthly in season (`fetchers/cec.py` docstring) | releases since 2025-01-01 |
| `rin_generation` (29) | `analysis/rfs.py` | Yes. EPA revises; the newest month is flagged `preliminary` | full since 2010-07 |
| `weather` (5) | `app/sections.py`, weather alerts | Yes. Past days start as model estimates (B15 #455 `weather.basis`), forecast rows are replaced | `past_days=30` + 7-day forecast |
| `cot`, `crop_progress`, `eia_energy`, `dce_futures`, `worldbank_prices`, `ocean_freight_rates`, `port_vessel_activity`, `us_processor_cash`, `rin_prices`, `rfs_rvo`, `ec_oilseed_prices` | various | No revision note found in fetchers or `LAYERS.md`. Unverified, not proven absent | full or multi-year |
| `cyclone_track_points` (34/35) | `app/block_builders.py` | Advisories are superseded. Deliberately excluded: NHC's archive can replay (`pipeline/history.py` comment) | active advisories only |

**Requirement 2.3.** For each table above, A11 records one of two things. Either the rendered number on date D can be rebuilt (snapshot stored, or upstream serves vintages and the fetcher can ask for one), or the gap is **accepted in writing**. The likely minimum: `prices`/`currencies` for the CBOT/FX legs that every crush, spread and landed number reads. #447 is direct evidence that the provider rewrites closed bars. `briefings.snapshot_json` already captures the briefing's own `prices`/`currencies` blocks, which may be enough. A11 decides. A full raw-table snapshot is not assumed necessary.

### 2.4 Private tables

`sea_india_rates` and `sopa_crop_estimates` are private (`config.SEA_PUBLISH = False`, `config.SOPA_PUBLISH = False`; `LAYERS.md` Layers 32–33). Committed CSVs are public by construction, so these **must not** enter `HISTORY_TABLES` (invariant 4). `sopa_crop_estimates`' revision path is lost on every CI run (`fetchers/sopa.py` docstring). **Requirement 2.4**: A11 confirms that loss is accepted, because a private edition is desk-local, and that the desk-side store, not CI, is where private point-in-time lives. That ties to the map's "Private desk store implementation" fog.

### 2.5 Fast-refresh editions depend on a cache, not on git

A Refresh Prices edition is the 3 fetched layers plus every other layer **from the verified-seed DB cache** (`mirror-market-db-verified-<run_id>`, `ARCHITECTURE.md` "Fast refresh"; #453). Actions cache entries are evicted after 7 days unused (GitHub's documented policy, not measured here). So a fast-refresh edition's non-fast layers cannot be rebuilt once its seed is evicted, except through the history CSVs and self-healing re-download (§2.3 caveats apply). Requirement 2.1's seed key in the manifest makes that dependency visible. A11 checks the live manifest names it.

### 2.6 Doc claim to correct

`CLAUDE.md` invariant 4 says "Every table round-trips through committed CSVs". In fact 25 of 49 do. The sentence is about why a client-record table would leak, and its intent holds: any table added to `HISTORY_TABLES` publishes. A11 flags the wording. This ticket does not edit `CLAUDE.md`.

## 3. Redundant providers

**Question.** Which critical prices have a free second source worth cross-checking, and which do not? There is no budget.

**Today: no rendered price is cross-checked against a second provider.** The trust registry knows three sources (`yahoo-finance`, `magyp`, `agrural`; `trust/registry.py`). The `reconcile-trusted` job (`deploy-dashboard.yml`, `continue-on-error`) compares the v1 parse with the trusted parse **of the same download** for CBOT named contracts, required FX (BRL, CNY, INR, ZAR) and MAGyP FOB. That checks *rules*, not *providers*. #447 is the proof: the ZAR divergence was the trusted OHLC rule catching a Yahoo candle with Close outside its own High/Low, not a second provider disagreeing.

**Licence frame.** A cross-check that is computed and never rendered is internal use. Invariant 9 gates publishing, not building, so a second source needs access rights, not redistribution rights. "Critical" here means the price feeds a rendered buyer number: crush, spread, landed cost, basis or hedge.

| Price | Provider today | Second provider in code? | Free second source | Verdict |
|---|---|---|---|---|
| **FX: BRL, CNY, INR, ZAR** (every landed/crush conversion; A1 3-day cap) | Yahoo (`config.CURRENCY_TICKERS`) | No (trusted = same Yahoo bars) | **ECB euro reference rates** (`eurofxref`, daily, free with citation; covers all four via EUR cross). For BRL also **BCB PTAX** (OData, no key, ODbL) | **Worth it, first.** Cheapest, broadest, and #447 shows the feed is fragile. Fixing-time mismatch: ECB 14:15 CET, PTAX 13:00 BRT, Yahoo NY close. So compare within a tolerance band, never as equality |
| FX: ARS | Yahoo | No | BCRA A 3500 API (no key) | Worth it, second. Terms page not found. **Unverified** |
| FX: MYR, IDR, THB | Yahoo | No | ECB | Low priority (palm context only) |
| FX: NGN, PYG | Yahoo | No | CBN NFEM page (NGN); none found for PYG | Not worth it now |
| **CBOT ZS/ZL/ZM closes + named contracts** (crush, hedge, every basis) | Yahoo (`prices`, `forward_curve`, `contract_bars`) | No | **CME DataMine settlement file** (free T+1 top-day). cmegroup.com pages refuse scripts. Stooq, Barchart and Investing.com bar automated use | **Worth it if DataMine's terms allow internal use: unverified, terms not read.** It is the only path to a real settlement, and so the only thing that could ever put a source in `PROVEN_SETTLEMENT_SOURCES` (invariant 3). Until verified, record "no free second source confirmed" |
| DCE soy/meal/oil | Sina via akshare | No | dce.com.cn daily quotes (scripts got an empty page; DCE rules bar unauthorised dissemination, read on a third-party repost) | None usable. Record as accepted single-source |
| BMD palm | Yahoo `CPO=F`; DCE `P0`; World Bank monthly | Three different instruments, so no like-for-like check | MPOB, Bursa: all rights reserved / robots-forbidden | None usable |
| AgRural Paranaguá FOB (Brazil basis) | AgRural | **Already stored:** `Soybean (ESALQ/B3 Paranaguá)` (Layer 17, `config.NOTICIAS_AGRICOLAS_URLS`, commented "cross-check for AgRural FOB") in the same `brazil_spot_prices` table | — | **Worth it, no new fetch.** A daily gap check between two stored series. No comparison code exists. The nearest is a `TODO Phase 2.1` in `analysis/health.py` (AgRural vs CEPEA Paraná wedge) |
| CEPEA indicators | Notícias Agrícolas republication | Weak: CONAB PR farmgate band check (`analysis/soy_analytics.py`, weekly, different level) | IMEA republishes through the same site, so it is not independent | Accept the existing weak check. CEPEA's CC BY-NC licence is already on record (2026-08 audit). That is a pre-sale licensing item, not ops |
| MAGyP FOB (Argentina) | MAGyP | No (trusted = same service) | CAC pizarra is a Rosario *cash* price (not FOB) and all rights reserved | None. MAGyP is itself the administered authority |
| US Gulf bids (AMS 3147) | AMS PDF | Same report over the MARS API: a second *channel*, not a second source | None independent (GTR quotes 3147) | Channel check only (PDF vs MARS). Already used by the backfill (`scripts/backfill_gulf_bids_mars.py`) |
| US processor cash (AMS 3511) | MARS | No | None found | None |
| India mandi (Indore) | Agmarknet (dead since 2026-10-09, #449) | SEA's weekly sheet carries an Indore bean rate (private layer). No comparison code | eNAM: all rights reserved | Worth an internal SEA vs mandi check once #449 resolves. Private only |
| EU rapeseed Moselle | EC xlsx | No | EC agri-food API (same publisher, CC BY 4.0) | Channel check only. Cheap, low value |

**Requirement 3.1.** A11 checks that every critical price above sits in exactly one of two states: **cross-checked** (a reconcile job compares it against an independent provider and reports agreement or divergence) or **accepted single-source in writing**. A reconcile of two parsers of one feed counts as neither.

**Requirement 3.2 (minimum for "A-ready").** FX for BRL, CNY, INR and ZAR is cross-checked against ECB reference rates in the existing non-blocking `reconcile-trusted` job, with a tolerance band. Divergence is reported, never paged, the same posture as #194/#447. The other rows may be accepted single-source.

**Requirement 3.3.** The AgRural vs ESALQ/B3 Paranaguá gap is reported from data already stored.

**Requirement 3.4.** CME DataMine's terms are read before any CBOT second source is built. If internal use is allowed, it is the CBOT cross-check. If not, CBOT is recorded as accepted single-source, and the go/kill on the trusted path (#239) is decided knowing that its CBOT pilot can only ever check rules.

Verification note: the ECB, BCB PTAX and FBIL findings rest on the sources' own pages. The DCE, BCRA, CFETS and DataMine terms were **not** confirmed against two primary sources. Treat those verdicts as provisional.

## 4. The "one operational-status result"

The third audit's item (#306 comment, 2026-10-13). Six producers each answer a different question, and none says which **buyer decisions** are supported or impaired right now:

| Producer | Question it answers |
|---|---|
| `main.py --doctor` | Which API keys are set and which layers each gates |
| `analysis.health` | Which commodity rows inside which tables are stale or missing |
| `pipeline/grading.py` (`data_freshness`) | What each layer's last run graded: success / usable_partial / stale / failed / no_publication |
| `reconcile-trusted` job | Do v1 and the trusted path agree (MAGyP, CBOT, FX; #447 non-blocking) |
| `scripts/latency_report.py` | Is each layer within its latency objective |
| `trust/site_promotion.py` | Is this candidate edition sound, and no thinner than the live one |

**Recommendation: it is A12's first-screen quadrant, not an ops surface. A9 sets one requirement on how it is computed.**

Reasons:

1. **The bar puts the reader on the page.** "Drift is reported on the page, never paged" makes the published edition the place operational state is read. A separate ops surface has no reader. The owner holds no book, and the operator already has the `ci-alert` and `ci-deploy-alert` rolling issues, `latency_report.py` and `pipeline_status.json`.
2. **A12 already has the slot.** #406's proposed fifth section is "what is missing, stale, disputed or uncertain", and it already asks how to tell "quiet tape" from "degraded signal coverage". That is the same question. Two homes for one answer is the "build both" the audit warned against.
3. **"Which decisions" is a buyer question.** Mapping a cargo/basis/origin/hedge decision to the legs it needs is domain modelling, decided with a physical buyer in the room. That is how A12 is set up (HITL, `/grilling` + `/domain-modeling`). An AFK ops ticket would have to invent that mapping, which breaks invariant 2.
4. **A new surface would be a seventh status vocabulary.** The audit's complaint is that six already exist. The fix is one roll-up over them, not one more.

**A9 requirement (the only part A11 checks before A12 lands).** When built, the result is computed **once**, by one function, from the existing states only: grading statuses, health per-key ages, reconcile verdicts, latency verdicts, key gates. It is carried in `manifest.json` next to `coverage` and `latency`, and the first screen renders from it. It adds no new state words. Every impaired decision names the leg and the state that impaired it (`{state, reason, data}` envelope). A11 checks that the derivation exists and that its inputs are those six producers. It does not grade layout, which is A12's.

**Sequencing consequence.** A12 is blocked on buyer recruitment (A15 #411). If A11 must wait for the decision map, A11 waits on recruitment. A11 checks the derivation's *shape* (single function, existing inputs, carried in the manifest). The decision list itself is graded in the trial, not the re-audit. Posted to #406 for A12 to confirm or overturn.

## Checklist for A11 (one line each)

- [ ] 0.2 stamps on ≥5 sampled editions match their manifest
- [ ] 0.3 no red run in the window is scheduler drift alone. Fix or accept the dispatched-`dce` gate
- [ ] 0.4 same-day landing rate reported (assumption: UTC day D)
- [ ] 1.1 `wasde` and `psd` exemptions re-examined (date columns exist). `crop_progress` in-season budget decided
- [ ] 1.2 per-key age verdict for `fred`, `contract_bars`, `river_us`, `sagis`, `eia`. `forward_curve` health dated by `observation_date`
- [ ] 1.3 repeat-value risk recorded per daily physical leg
- [ ] 1.4 URL kind declared in config and tested
- [ ] 2.1 manifest carries commit SHA, run id, seed key
- [ ] 2.2 per-edition manifest retained durably (committed by the deploy workflow only)
- [ ] 2.3 every revising rendered table either reproducible at D or accepted in writing, `prices`/`currencies` first
- [ ] 2.4 private-table revision loss accepted, desk store owns it
- [ ] 3.1 every critical price cross-checked or accepted single-source in writing (two parsers of one feed is neither)
- [ ] 3.2 BRL/CNY/INR/ZAR reconciled against ECB, tolerance band, non-blocking
- [ ] 3.3 AgRural vs ESALQ/B3 Paranaguá gap reported
- [ ] 3.4 CME DataMine terms read; CBOT cross-checked or accepted single-source
- [ ] 4 one status roll-up: one function, existing inputs, in the manifest. Rendering is A12's
