# Changelog

Format: human-readable summaries grouped by "run" — a discrete refactor or
feature push. Each run notes the why, the user-visible behaviour change (if
any), and the test/coverage impact.

## Unreleased — M15: world stocks-to-use from PSD (2026-08-22)

The world total was already inside the bulk CSVs Layer 6 downloads; the
country filter destroyed it. Summing *before* that filter reconstructs USDA's
own R00 row — verified attribute-by-attribute on 197 (commodity, marketing-year)
pairs 1960→2026 with zero mismatches, and against WASDE-673's printed world
lines (#210 research). Zero extra HTTP, no schema change: the `psd` primary key
already accommodates `country='World'` and `'World Less China'`.

* **The world denominator is Domestic Consumption only.** The repo's US formula
  (consumption + exports) is right for one country and wrong for the world,
  where an export is already inside some importer's consumption — it moves
  soybean stocks-to-use from 29.19% to 20.33%. Both are plausible-looking
  percentages, which is why the denominator now switches on the region instead
  of being a module constant, and why the test pins 29.19% *and* rejects 20.33%.
  PSD ships an attr-195 Stocks-to-Use whose World value makes exactly this
  mistake and lands 17.2 pp from the 63.1% USDA prints for cotton.
* **Rate attributes are gated on the unit, not on a name blocklist.** Summing
  country Yield gives 95.39 MT/HA against USDA's 3.01. A blocklist would miss
  the next rate attribute; only `1000 MT` / `1000 HA` / `1000 480 lb. Bales`
  are summed, and an unrecognised unit is withheld.
* **World Less China carries the *world* import/export gap**, not one
  recomputed from its own trade legs. WASDE subtracts China from the already
  adjusted world use — PSD-derived less-China wheat use is 669,541 and WASDE
  prints 674.61, the same +5,071 as the world row. Recomputing adds China's own
  net import position (+9,430) and yields a figure matching neither WASDE nor
  raw PSD. The two regions also carry two Basis lines: the closed-system
  argument for a consumption-only denominator is the world's, and 113 MMT of
  soybeans leave the less-China region every year.
* **Two guards stand between a partial parse and a plausible world number.**
  Duplicate `(commodity, country, year, attribute)` rows hard-fail — the
  country path survives them on its upsert, a sum would silently double. And a
  group missing countries its siblings report is withheld: `min_count=1` only
  catches an all-NULL group, while 10 of 100 countries still sum to something
  that looks like a world total. `scripts/reconcile_psd_world.py` re-checks the
  sum against USDA's own R00 row on demand (needs `FAS_API_KEY`; deliberately
  not in the pipeline, which is key-free).
* **Cotton ships nothing.** Its consumption line is attr 142 `Domestic Use`,
  which `PSD_TARGET_ATTRIBUTES` does not request, so no cotton ratio — US or
  world — is computable at all today. Pre-existing, surfaced by this run,
  tracked separately.
* Rendered on the briefing's stocks-to-use section and in `snapshot_json`
  (`world_stocks_to_use`), each stating region, denominator, and whether the
  grain adjustment was applied. Tests: +39.

## Unreleased — M16: cross-market crush board, headline section 04 (2026-08-21)

The headline said whether *CBOT* processing was paying, twice, in two shapes:
a "Crush Spread" tile in the key-metrics grid and a chart sub-block below it —
plus a "DCE Board Crush" tile computed a third way. None of the three said what
kind of margin it was. This run replaces them with one board.

* **Four markets side by side, each labelled by kind.** CBOT (board, named
  contracts) · Dalian (board, continuous series) · Brazil · Argentina
  (administered, Ley 21.453), in `config.CRUSH_BOARD` order, each card linking
  to the market page whose block 03 carries the depth. Every card calls the
  same `crush_block` that page does, so the headline and the page cannot print
  two different margins for one market (M7 #149's one engine, enforced by
  reuse rather than by convention).
* **Brazil ships without a number.** No Brazilian oil or meal cash quote is
  ingested — an unbuilt scrape, and additional work rather than a gap this
  ticket could close — so the card carries the registry's own reason in the
  `absent` empty state. Dropping it would read as "Brazil has no crush
  industry".
* **Argentina ships as a full leg.** #162 verified the meal NCM position
  `23040010100B` against dataset 358's labelled series (52 of 52 business days
  exact), so the margin M7 flagged as provisional is no longer caveated.
* **A range is struck by the engine that struck the level, or not at all.**
  Dalian and Argentina restrike their own margin for every stored session —
  one session for all three legs, converted at that row's own date's rate and
  never a later one. CBOT's named-contract margin gets no range: a mean off the
  continuous front-month series would be a range around a different number, and
  the card says so. Under 20 sessions a leg prints its level with "no range
  yet" and the count.
* **Housekeeping.** Headline sections renumber 04→05 … 11→12; the old section
  04 loses its crush sub-block and becomes "Relative Value"; the surviving
  key-metrics crush tile is renamed "CBOT Front-Month Crush" and now declares
  `derived: "crush"` rather than being recognised by its display label, so the
  promotion contract's alignment probe cannot be broken by a rename;
  `.empty-state` CSS moves into `_base.html.j2` beside `.kind`.
* **Tests.** `tests/test_crush_board.py` — 20 tests over kind labelling, engine
  agreement with block 03, the Brazil empty state, both range states, the FX
  rule, and the rendered section (including a guard that the index nav's
  section numbers match the page's).

## Unreleased — Phase 4: opportunity and counterparty engine (2026-08-18)

The players base was a reference document and the market layers were a
briefing. This run joins them into a screen: `analysis/opportunities/` plus
`docs/opportunities.html`, answering who might buy or sell what, where, in
which window, why now, on what evidence, and what to do next. It originates no
data — every detector reads a table another layer already fills, and every
counterparty is a researched players-base entry. Nothing is invented.

* **Six deterministic rules, each reusing an existing module.** Landed-origin
  advantage over a configured threshold (Phase 2's `origins.comparison`), an
  unusual destination share of inspections, an unusual share of outstanding
  export commitments, a buyer region whose stocks-to-use has fallen below its
  own history (`analysis.stocks_to_use`), a favourable origin crush
  (`origins.crush`), and an FX move that changes an origin's competitiveness.
  Every rule's threshold, validity window and stated question lives in
  `config.OPPORTUNITY_RULES`; each detector is isolated, so one raising is
  reported in the run's coverage rather than blanking the page.
* **A price difference is not an arbitrage.** Policy, freight, quality, window,
  liquidity, staleness, our-own-ingest-outage and no-counterparty blockers ride
  on the row. A hard blocker zeroes feasibility *and* caps the rung, and
  ranking sorts by rung before composite — so India's genuine +284 USD/MT mandi
  premium, uncloseable behind the GM import ban, sits in the leads with its
  caveat instead of heading the board. That caveat is reused verbatim from the
  market registry's `basis` descriptor rather than restated.
* **Five scores, shown separately, no single number.** Economic, evidence,
  freshness, counterparty, feasibility — each 0–100 with a note reproducible
  from the figures printed next to it, weighted by
  `config.OPPORTUNITY_SCORE_WEIGHTS` (economics deliberately not on top). One
  design fault was caught in the demo and fixed: the evidence component read
  the *row's* confidence, which is already dragged to `provisional` by
  `inferred` counterparty research — charging one weakness twice and pinning
  the component at 40 almost everywhere. It now reads the evidence's own
  confidences.
* **Private desk workflow, structurally separated.** Status, owner, contact
  dates, notes, feedback and audit come from gitignored YAML under
  `data/reference/opportunities/` and render only into
  `data/workspace/opportunities.html`. Four independent guards: the public
  serialiser never builds the `workflow` key, `EngineResult.public` drops any
  row that has one, `save_opportunity_detections` raises on those column names,
  and the private file is written outside `docs/` and is absent from
  `trust.site_promotion.expected_site_paths()`. Six sentinel strings in the
  demo fixture appear in the private edition and in neither the public HTML nor
  the archived table.
* **Stable identity and expiry.** `identity_key` carries no number at all, so
  re-detecting yesterday's lane keeps its id and first-seen date; the new
  `opportunity_detections` table archives the public projection and
  round-trips through `data/history/`, which is what makes ids survive CI's
  empty database. Lapsed rows are re-stamped `expired` and demoted, then listed
  from the archive as rows rather than re-rendered with stale numbers.
* **A stated ladder.** market signal → lead → actionable → proposed trade →
  completed business. Only the first three are detectable; the last two need a
  human and are private by construction. No routing, no contact channel —
  the output is a next action for a person.
* **Feedback is counted, never learned from.** Dismissals and outcomes are
  reported on the private edition and do not re-weight a rule.
* Tests: 129 new (`tests/test_opportunity_{domain,signals,rules,scoring,`
  `registry,workflow}.py`, `tests/test_opportunities_page.py`, shared
  `tests/opportunity_fixtures.py`), including suppression cases for stale
  evidence, a policy barrier, unknown freight and incompatible windows, and
  end-to-end leak checks over both rendered editions. Suite: 2036 passed,
  5 skipped, 85.6% coverage; ruff clean; the promotion contract passes at 13
  URLs with the 1440/390 viewport pass.

## Unreleased — Phase 3 follow-up: closing four stated gaps (2026-08-18)

The Phase 3 write-up listed what the workstation could not do. Four of those
turned out to be fixable without inventing anything, and this run fixes them.
The fifth — a stitched continuous series below 10 sessions of stored
named-contract history — is only time passing and is left alone.

* **ICE Sugar No. 11 and Cotton No. 2 now have documented expiry rules.** They
  were `NOT_ENCODED` because this project had not read the rules, not because
  none are published. Encoded off the **rulebook**, not a summary page: Sugar
  Rule 11.06(a) (last full trading day of the month preceding delivery, plus a
  January carve-out no listed month reaches) and Cotton Rule 10.02(a) (last
  trading day = 10 business days before last delivery day; last delivery day =
  the 7th-last business day of the month). Cotton's summary page states the
  same rule as "seventeen business days from end of spot month" — 10 + 7 — and
  it is the *pair* that proves the counting convention runs from the last
  business day as 1. Pinned to dated examples: CTZ24 last trades 6 Dec 2024,
  first notice 22 Nov 2024. All nine products are now `DOCUMENTED`, and the
  `NOT_ENCODED` degradation keeps its tests against a purpose-built spec.
* **A contract with no notice day is now distinguishable from one we have not
  encoded.** Sugar No. 11 has no notice-day mechanism at all — Rule 11.06(b)
  attaches the delivery obligation to the close of the last trading day, which
  is *stricter* than an FND. Rendering that as "not encoded" would send a
  hedger looking for a date that does not exist. New `NO_NOTICE_DAY` sentinel,
  rendered with its own explanation.
* **Whole-product open interest is now shown.** Per-contract open interest
  remains NULL and is still never inferred — nothing publishes it per delivery
  month. But `cot.total_open_interest` has been stored all along: the CFTC's
  weekly all-months-combined figure. It is surfaced as its own type
  (`AggregateOpenInterest`) beside the curve, carrying its own report Tuesday,
  never as a field on a quote — that would claim one month holds the product's
  whole open interest, and would date a Tuesday number to the price session.
* **The manual options workflow now has an entry point.** The module has always
  described "type in your broker's quote"; nothing could be typed in.
  `data/reference/options/*.yml` now loads on the same terms as the position
  book (missing directory = empty, malformed = raises). Each row needs a
  `source` naming who quoted it and exactly one of `premium` /
  `implied_volatility`; the other is derived. Also records the measurement
  behind the absent chain: `yfinance.Ticker(t).options` returns `()` for all
  seven tickers checked, including named contracts.
* **`scripts/prune_curve_snapshots.py`** — the operator cleanup for
  forward-curve legs left by a same-day re-run. Dry-run by default. It cannot
  be a PR change: the `history-guard` CI job fails any PR touching
  `data/history/`, so the committed CSV is rewritten by the ordinary export
  under `MIRROR_HISTORY_ALLOW_SHRINK=1` afterwards. The rule is deliberately
  narrow — a group whose legs are *all* undated is legacy pre-column history,
  not duplicates, and is left untouched, because a curve leg cannot be
  re-fetched once deleted.

Tests: 2014 passed, 5 skipped; coverage 85.27% (gate 70%). `ruff` and `mypy`
clean on the changed files.

## Unreleased — Phase 3: futures workstation for delayed-data hedging (2026-08-18)

Turns the futures side of the stack from a price display into something a
physical trader can model a hedge with. New package `analysis/futures/`
(11 modules), new page `docs/workstation.html`, ~180 new tests.

### The gap this closes

The stack had prices but no **contract**. `prices` stores a continuous
front-month series under the label "Soybeans" with no contract column at all;
`forward_curve` stored a Yahoo ticker string and a close. Neither said which
contract a number belonged to, when it stopped trading, or whether it was a
settlement — so nothing downstream could size a hedge, and
`analysis.signals.is_near_roll` existed only to demote indicators a silent
provider roll may have faked.

### What was built

* **Contract identity** (`domain.py`) — exchange, root, month/year, contract
  size, native unit, tick, published expiry rule, first notice day, price type,
  observation date, provider, freshness. Named contracts and continuous series
  are different types and neither substitutes for the other.
* **Curve analytics** (`curve.py`) — calendar spreads, carry annualised on
  business days between *last trade dates* (not month labels), structure,
  spread history with percentiles above a 20-observation floor, hedge-month
  candidates.
* **Hedge calculator** (`hedge.py`) — long/short physical, unit and contract-size
  conversion, hedge ratio, rounding policy, residual exposure, basis and FX
  exposure, meal/oil crush cross-hedge at metric-native yields, and ten warning
  codes including `fnd_inside_pricing_window` and `expiry_not_encoded`.
* **Scenarios** (`scenarios.py`) — futures, basis, FX, crush-yield and
  value-share shocks, combined and attributed separately.
* **Ticket** (`ticket.py`), **positions and P&L** (`positions.py`), **release
  calendar** (`events.py`), **options** (`options.py`), **exposure alerts**
  (`alerts.py`).

### Two data-integrity fixes

* **`forward_curve` accumulated legs across runs on one `fetched_date`.** The PK
  is `(commodity, contract_month, fetched_date)` and nothing deleted, so a
  second run the same day left the earlier run's legs standing beside the new
  ones. Visible in committed history: seven Soybean legs on 2026-08-11, six
  stamped that session and `ZSN27.CBT` undated. Fixed at write time
  (`_replace_curve_snapshot` clears the day's rows before rewriting them) *and*
  re-checked at read time, because the stale leg has a valid key and a plausible
  price.
* **`volume` and `open_interest` columns** added to `forward_curve`. Volume is
  now captured from yfinance and is `None` — not `0.0` — when absent; open
  interest is always NULL, because no ingested source publishes it and a zero
  would read as "nothing open".
* A position document naming an unsizable commodity raised `UnknownContract`,
  which the page's loader did not catch as a position error and rendered as an
  **empty book** — "nothing entered" standing in for "entered wrongly". It now
  raises `PositionError` and fails the page.

### Deliberately not built

No order routing, and no seam for one. No invented settlement, volume, open
interest or option values: the options chain reports *unavailable* with its
reason rather than rendering an empty ladder, and Black-76 refuses to run
without a named human source for its volatility. Sugar and Cotton expiry rules
are left un-encoded. Licensing was out of scope; provider substitution is a
single class behind `QuoteProvider`.

## Unreleased — History persistence: briefings, export sales, regression guards (2026-08-10)

Audit of what actually survives the ephemeral CI runner. `HISTORY_TABLES`
covered 11 of 26 tables; two of the omissions were genuine, unrecoverable
losses, and `export_history()` could silently truncate good history.

### Two tables added to `HISTORY_TABLES`

* **`briefings`** — written every run by `analysis/briefing/orchestrator.py`
  and destroyed with the runner. No upstream serves it: a past day's `text`
  and `snapshot_json` cannot be reconstructed from any source. This was the
  single largest ongoing data loss in the pipeline.
* **`export_sales`** — `fetchers/export_sales` requests only the *current*
  marketing year (`_current_market_year`), so the outgoing year is never
  re-fetched. At the next rollover (Sep 1 for soybeans) the prior year's
  weekly series would have disappeared from CI.

Deliberately **not** added: `crop_progress` (NASS re-fetched from 2020),
`dce_futures` (akshare serves 2005→now), `worldbank_prices` (CMO xlsx serves
1960→now), `prices`/`cot`/`weather`/`psd`/`currencies`/`usda`/`eia_energy`
(all self-healing).

### Workflow ordering — without this the briefings table stays empty forever

Registering `briefings` in `HISTORY_TABLES` was necessary but not sufficient.
`main.py` never generates a briefing; `scripts/generate_html.py` does, via
`generate_briefing()` → `generate_briefing_data(archive=True)` →
`save_briefing()`. In `deploy-dashboard.yml` that step ran *after* both
`main.py` (which calls `export_history()`) and the commit step — so today's
briefing was written to a DB that was already exported and about to be
destroyed, and the CSV would never have gained a row.

No seed CSV ships here: `data/history/` is written only by the daily deploy
workflow (enforced by the `history-guard` job in `ci.yml`), so
`briefings.csv` and `export_sales.csv` are created on the first run after
merge.

Dashboard generation now runs before the data commit, followed by a second
`export_history()` pass that picks up the briefing. The commit step gains
`!cancelled()` so a failed dashboard build no longer costs the day's data
as well. The export is idempotent and its guards make the re-run safe.

### Two export regression guards

The existing empty-table guard was not sufficient. Both new checks log an
error and leave the CSV untouched:

* **Shrink guard** — refuses an export with fewer rows than the committed
  CSV. Triggered in practice by running `export_history()` from a local DB
  without `import_history()` first; would have truncated `brazil_estimates`
  (5970 → 5373), `forward_curve` (486 → 432), `gulf_bids` (107 → 86),
  `argentina_fob` (27 → 14) and `brazil_spot_prices` (294 → 291).
* **Column guard** — refuses an export missing a column present in the
  committed CSV. `CREATE TABLE IF NOT EXISTS` never adds columns to an
  existing table, so a DB predating a schema change exports narrower rows at
  an *unchanged row count*, invisible to the shrink guard. Caught
  `safex_prices.contract` being dropped.

`MIRROR_HISTORY_ALLOW_SHRINK=1` overrides both for a deliberate prune.

Tests: 4 added to `tests/test_history.py` (shrink refused, shrink-with-
override permitted, dropped column refused, briefings round-trip).

### `tests/test_main_exit_code.py` — two fixture defects found while verifying

* **Real history CSVs written from tests.** The fixture patched
  `STORAGE_DIR`/`DB_PATH` to a tmp dir but not `HISTORY_DIR`, and `run()`
  ends with `export_history()` — so every run of this module appended stub
  rows to the repo's *committed* `data/history/*.csv`. Now patched to tmp.
  The new shrink guard is what surfaced it: it started refusing exports that
  should never have been requested in the first place.
* **No guard against the next unstubbed fetcher.** #127 fixed three fetchers
  that had been added to `main.py` without stubs (the "no network" docstring
  had stopped being true; the live data.gov.in call could hang the suite for
  25+ minutes). That was a one-time fix — this makes it permanent with an
  assertion that every `fetch_*` symbol on `main` appears in the patch dict,
  so the same bug cannot return with the next layer.

## Unreleased — Upstream source repairs (2026-05)

Two layers were silently failing on every pipeline run; both are now fixed
or explicitly disabled with diagnostic logging.

### Layer 15 (CONAB) — schema rewrite

CONAB's `SerieHistoricaGraos.txt` switched to a semicolon-separated,
per-UF schema (`ano_agricola; dsc_safra_previsao; uf; produto; id_produto;
area_plantada_mil_ha; producao_mil_t; produtividade_mil_ha_mil_t`) and
the old fetcher silently parsed it as a 1-column DataFrame, so every row
was discarded by the commodity filter. `fetchers/conab.py` now:

* Parses with `sep=";"` directly (no `\t` fallback).
* Targets `{soja, milho, trigo, algodao em pluma}` (no accents, lowercase).
* Aggregates the 27 UF rows to national totals per (year, commodity).
* Recomputes yield in kg/ha from aggregated production/area instead of
  averaging per-state yields.
* Drops coffee — `SerieHistoricaGraos.txt` is grains+oilseeds+cotton only.
* Hard-fails (empty return + `logger.error` listing the columns it got)
  if the required schema columns are missing again.

Self-test now returns 597 rows / 199 (year, commodity) pairs; latest
2025/26 figures match published CONAB totals (Soybeans 179.2 MMT, Corn
139.6 MMT, Cotton lint 3.8 MMT).

### Layer 16 (NCDEX India) — disabled

`ncdex.com` now serves a JavaScript fingerprint interstitial
(`__hd_fingerprint` cookie issued via POST to `/__verify/fp`) on every
URL, including the homepage. Plain `requests.get()` returns a 6.5 KB
HTML error page with `Content-Type=text/html`, regardless of URL. The
silent skip in `fetchers/india_domestic.py` masked this — there's no
URL update that fixes it.

* Diagnostic logging added: status code, Content-Type, and size are now
  logged on every failed fetch attempt, so the anti-bot wall is visible.
* `main.py` short-circuits the layer with `_mark_empty("india_domestic")`
  and an explanatory log line.
* Fetcher code, config templates, and downstream consumers
  (`analysis/briefing/sections/emerging_markets.py`, the dashboard) are
  preserved — they already degrade gracefully when the table is empty.

Re-enabling requires either an alternate India spot-soy source
(AgMarknet, SOPA, NSE) or a Playwright-based bypass that executes the
fingerprint JS and captures the cookie.

### Tests

No test changes — the CONAB output shape `(source, commodity, crop_year,
attribute, value, unit, report_date)` is unchanged, so `clean_conab`,
`save_brazil_estimates`, and the briefing section all keep working.
Full suite still passes: 111 tests green.

## Unreleased — Run 7: Trader-grade analytics

Goal: layer trader-grade signals on top of the existing pipeline —
stocks-to-use tightness, statistical-extreme positioning/weather,
Brazil export basis, and the persistence groundwork to backtest what
the briefing said on any given day.

### Stocks-to-use ratios (US, from PSD)

`analysis/stocks_to_use.py` — computes the classic ending-stocks /
total-use ratio per commodity, with `detect_tight_supply()` flagging
the bottom-decile years that historically precede price spikes. The
ratios are sourced from the PSD frame rather than direct WASDE numbers
because NASS QuickStats does not expose the "ending stocks" and
"total use" line items in a stable, queryable shape — PSD carries
both, country-keyed, with the same definitions used in the WASDE
report. `analysis/briefing/sections/stocks_to_use.py` renders the
section. 12 tests in `tests/test_stocks_to_use.py`.

### Shared z-score helpers

`analysis/zscore.py` — `zscore(series, window)` and
`format_zscore(z)` factored out of two callers that were each rolling
their own. Now used by:

* `analysis/briefing/sections/cot.py` — flags COT positioning at
  statistical extremes (|z| ≥ 2) rather than the previous fixed
  net-position threshold, which missed regime shifts in low-OI
  contracts.
* `analysis/briefing/sections/weather.py` — annotates anomaly
  z-scores alongside threshold breaches so "+5°F" and "+5°F at z=3.1"
  read differently.

8 tests across `tests/test_zscore.py` and
`tests/test_zscore_sections.py`.

### Brazil export basis

`analysis/spreads.py` gained two additions:

* `oil_value_share` — a new column on the crush spread output: the
  fraction of total crush gross revenue contributed by soy oil.
  Crosses the 50% line during biofuel-pull regimes and is the
  cleanest single number for "is the board paying for beans or
  paying for oil right now".
* `compute_brazil_basis()` — Brazil basis as CBOT (USD/MT) vs
  CEPEA/AgRural (USD/MT after BRL FX conversion). Negative basis
  means Brazilian beans are trading below CBOT in dollar terms,
  i.e. export-competitive discount; positive basis means the US
  origin is the cheaper alternative. Pulls FX from
  `analysis/loaders.load_currencies()`.

`analysis/briefing/sections/basis.py` is the new section module that
renders the result, wired into `orchestrator.py`. 12 tests in
`tests/test_brazil_basis.py`.

### Layer 19: AgRural Paranaguá port FOB

`fetchers/agrural.py` — new fetcher scraping Paranaguá port FOB
quotes in BRL/60kg and converting to BRL/MT. Feeds the Brazil basis
section above. The CEPEA/ESALQ index (Layer 17) covers the interior
spot, but the export-basis question is about FOB-at-port, which the
two sources differ on by the inland-freight wedge.

### `fetchers/wasde.py`: extracted from `fetchers/usda.py`

WASDE fetching (Layer 12) moved into its own module. The USDA fetcher
had grown three distinct responsibilities (crop, crop progress,
WASDE) and the WASDE branch had its own `source_desc=FORECAST` query
shape, its own MoM revision logic, and its own retry/empty handling.
Splitting it makes the per-layer ownership obvious and lets each
side evolve independently. Called from `main.py:440`. Tests in
`tests/test_fetcher_wasde.py`.

### Briefing archive

The briefing is now persisted on every run, not just printed.

* `pipeline/schema.py` — new `briefings` table keyed by run timestamp,
  with `text` and `snapshot_json` columns.
* `pipeline/store.py` — `save_briefing(text, snapshot_json)`.
* `pipeline/query.py` — `read_briefing(timestamp)` for a single run,
  `read_briefings(limit, since)` for a range.
* `analysis/briefing/snapshot.py` — distills `BriefingData` into a
  compact structured payload for `snapshot_json`. Initial shape
  covers prices+technicals, crush, and COT positioning; future
  sections plug in incrementally without a schema migration.

Why: enables backtesting questions like "what did we say last
Tuesday when the dollar broke 105?" without re-running the pipeline
against a database that has since moved on. Also unblocks the
"signal recall@N" analysis we want to run before promoting new
signal rules to `alert` severity.

### Tests, coverage, types

* **314 → 394 passing (+80).** New: `test_stocks_to_use.py` (12),
  `test_brazil_basis.py` (12), `test_zscore.py` + `test_zscore_sections.py`
  (8), `test_fetcher_wasde.py` and the new briefing-archive query
  paths.
* **Coverage 72% → 72.93%.** New code is fully covered; the headline
  number is flat because the codebase grew at roughly the same rate
  as the tests.
* `mypy analysis/ pipeline/` and `ruff` remain clean.

### Deferred to v3.1 — continuous contract roll

`scripts/spike_back_contracts.py` is the Phase 2 spike: validates
that yfinance returns usable history for the back contracts we'd
need to construct a Panama-adjusted series. Phase 3 (the actual
implementation) requires an `adj_close` schema column on `prices`
and the dual-column read pattern documented in `CLAUDE.md` (Known
Limitations) — technicals read `adj_close`, spreads/basis stay on
raw `Close`. Not in this run; see the plan file referenced in
`CLAUDE.md`.

---

## Unreleased — Run 6: Organization & Polish

Goal: make the codebase navigable, remove duplication between the daily
briefing and the dashboard, and bring documentation back in line with the
code.

### `analysis/loaders.py`: shared, cached price/currency loaders

Extracted the two zero-arg loaders that both `analysis/briefing/` and
`analysis/soy_analytics.py` were maintaining their own copies of:

* `load_prices(*, with_technicals=False)` — `read_prices()` + DatetimeIndex
  setup, optionally applies `compute_all_technicals` so the dashboard's
  cached technicals call stays a single pass.
* `load_currencies()` — `read_currencies()` + DatetimeIndex setup.

Both are `@lru_cache`d (two slots for `load_prices` — with and without
technicals). `clear_loader_cache()` resets both between pipeline runs.
`analysis/soy_analytics._load_soy_prices` and `_load_currency_data` are now
thin filters over the shared loaders.

### `analysis/briefing/`: package split

The 1,492-LOC `analysis/briefing.py` monolith is now a package:

```
analysis/briefing/
    __init__.py             # exposes generate_briefing, generate_briefing_data, BriefingData
    orchestrator.py         # loads shared data, calls sections, assembles text
    types.py                # BriefingData dataclass
    sections/
        freshness.py        prices.py        crush.py
        economic.py         usda.py          crop_progress.py
        wasde.py            export_sales.py  inspections.py
        dce.py              forward_curve.py eia.py
        conab.py            currencies.py    cot.py
        weather.py          psd.py           worldbank.py
        emerging_markets.py correlations.py  seasonal.py
        market_drivers.py   signals.py
```

Each section module exposes a `format(...)` function and is independently
testable. The largest section (`market_drivers.py`) is 261 LOC; most are
under 50.

### Typed structured output: `BriefingData`

New `analysis.briefing.generate_briefing_data() -> BriefingData`.
`BriefingData` is a frozen dataclass with:

* `text` — the joined briefing text (what `generate_briefing()` returns)
* `section_texts` — `dict[name, str]` for consumers that want to render
  one section at a time
* `signals`, `enriched`, `price_data`, `currency_data` — the structured
  pieces the orchestrator already passes between sections

`generate_briefing()` is now a thin wrapper that returns
`BriefingData.text`. Existing callers continue to work unchanged.

### Documentation pass

* `README.md` — new "Required vs Optional Layers" matrix; explicit "What
  runs with zero API keys" (11 of 18 layers); updated Project Structure
  for the `briefing/` package and `loaders.py`; corrected the
  zero-key layer count in the "How to Run" section.
* `CLAUDE.md` — updated Analysis Layer section to reflect the new package
  structure and the shared loaders.
* `ARCHITECTURE.md` — new single-page diagram of fetch → clean → store →
  analyze → render, with the module dependency graph and the "stores
  native units, converts at display" rule called out.
* `CHANGELOG.md` — this file; formalised the per-run structure.

### Tests

Total: 314 passing (was 301 at start of run), 5 skipped (Turso integration
suite, gated on env vars). New tests:

* `tests/test_loaders.py` — 7 tests covering the shared loaders
  (empty DB, populated DB, with/without technicals, cache slot isolation,
  `clear_loader_cache()` semantics).
* `tests/test_briefing.py` — 6 tests covering the orchestrator wiring:
  empty-DB run emits every expected section header, populated-DB runs
  exercise non-empty paths across every briefing section,
  `generate_briefing()` agrees with `generate_briefing_data().text`,
  `BriefingData.section()` returns "" for unknown names.

### Coverage & types

* Project test coverage: **38% → 72%** (target was 60%). The shared
  loaders, the briefing orchestrator, every section module, and the
  `BriefingData` API are all covered.
* `mypy analysis/ pipeline/` is **clean** (0 errors, 45 source files).
  Fixed: missing type annotations on `signals: list[dict]` /
  `issues: list[dict]` / etc.; `dict[str, Any]` annotations on aggregate
  result dicts in `analysis/soy_analytics.py`; `str | None` annotation on
  `prior_success` in `pipeline/store.save_freshness`; None-narrowing
  guard on RSI divergence operands in `analysis/signals.py`.

### Bug fix uncovered by mypy

`analysis/soy_analytics.emerging_markets_analysis` was calling
`compute_crush_spread(prices_df)` with a single long-format DataFrame
when the function expects three indexed frames (beans, oil, meal). The
call was wrapped in a try/except so the error was silently swallowed —
the India CBOT-crush comparison block never actually ran. The call now
splits `prices_df` into the three legs before invoking
`compute_crush_spread`.

---

## Unreleased — Run 5: Scraper hardening + cleaner test coverage

Goal: stop letting HTML-scraper sources fail silently, and lock in
cleaner behaviour with regression tests so future refactors can't
quietly drift.

### `ScraperShapeError` adoption across HTML scrapers

`fetchers/cepea.py`, `fetchers/india_domestic.py`, `fetchers/safex.py`,
and the USDA inspections parser in `fetchers/usda.py` now raise
`ScraperShapeError` (from `pipeline/results.py`, see Run 3) instead
of returning empty DataFrames when the upstream page no longer
matches the expected structure — missing required header column,
zero rows beneath a valid header, no `<table>` elements (typical for
JS-rendered pages), etc. The fetcher wrappers catch the error,
log it as "the upstream site changed", and continue; the dashboard
can now distinguish "no data this week" from "the scraper is broken".

### `pipeline/connection.py`: opt-in strict Turso mode

New `MIRROR_REQUIRE_TURSO=1` env flag. When set, a failed Turso
connection raises `TursoUnavailableError` instead of silently
falling back to local SQLite. Default behaviour is unchanged.
Intended for CI/production deploys where the local fallback would
mask a config bug. `tests/test_connection.py` covers both modes.

### `pipeline/query.py`: narrowed exception handlers

Replaced bare `except Exception:` blocks in five `read_*` functions
with explicit `(sqlite3.OperationalError, pd.errors.DatabaseError)`,
so unrelated bugs (e.g. a typo in a column name) raise instead of
being swallowed as "table doesn't exist".

### `pipeline/schema.py`: freshness schema extended

`data_freshness` table grew two columns: `last_attempt` (timestamp
of most recent run regardless of outcome) and `status` (`'success'`
or `'failed'`, default `'success'`). `last_success` is now nullable.
This is what enables the dashboard's "last good run was X days ago"
display when a layer has been failing for a while.

### Tests

* `tests/test_scrapers.py` — 12 tests against committed HTML
  fixtures in `tests/fixtures/` (no network). Parser-level coverage
  for SAFEX, CEPEA, India NCDEX, and USDA inspections. Each suite
  includes a "header renamed → `ScraperShapeError`" test so the
  alert path is exercised, not just the happy path.
* `tests/test_clean.py` — full cleaner coverage: happy path +
  no-mutation invariants for every cleaner (`clean_ohlcv`,
  `clean_fred_series`, `clean_cot`, `clean_weather`, `clean_psd`,
  `clean_dce_futures`, `clean_export_sales`, `clean_forward_curve`,
  `clean_wasde`, `clean_eia`, `clean_inspections`, `clean_conab`,
  `clean_india_domestic`, `clean_brazil_spot`, `clean_safex`,
  `clean_worldbank`).
* `tests/test_query.py` — 13 tests: empty table, missing DB file,
  missing table, datetime hydration on `Date`/`week_ending`, and
  the `status`-column backfill path for older databases that
  pre-date the freshness schema change.
* `tests/test_connection.py` — 8 tests covering `is_cloud()`,
  `_require_turso()`, fallback-to-SQLite without Turso env, and
  the `MIRROR_REQUIRE_TURSO` raise path.

---

## Unreleased — Run 4: Performance & Storage refactor

### `pipeline/store.py`: executemany + generic upsert helper

Replaced 19 per-row `iterrows` writers with a single batch `executemany`
helper. Each `save_*` function now reshapes its DataFrame and delegates
to `_save`, which calls `upsert_dataframe` inside a transaction.

**Microbenchmark** (10,000 rows on a temp SQLite, median of 3 runs):

| save_* function    | Before     | After    | Speedup |
|--------------------|-----------:|---------:|--------:|
| `save_price_data`  | 236.7 ms   | 31.6 ms  | **7.5×** |
| `save_cot_data`    | 264.6 ms   | 31.1 ms  | **8.5×** |
| → throughput       | ~40k rps   | ~320k rps | ~8× |

Reproduce with `python scripts/benchmark_store.py`.

**LOC reduction**: `pipeline/store.py` 979 → 471 (**52% smaller**).
The remaining ~120 lines beyond the bare migration are the freshness
functions (`save_freshness`, `update_commodity_freshness`) and the
`save_wasde` defaulting logic — both intentionally kept verbatim
because they have non-trivial business rules.

### Index discipline

Added explicit `CREATE UNIQUE INDEX IF NOT EXISTS` for every PK column
set across all 22 tables (`pipeline/schema.py:UNIQUE_INDEXES`). Wired
into `init_database()`. SQLite already creates an implicit unique
index for each declared `PRIMARY KEY`, but the explicit indexes are
belt-and-suspenders for any older user DB that may pre-date the
current PK constraints. Migration-safe — the `IF NOT EXISTS` guard
means existing databases are upgraded in place on next `init_database()`.

### Cached analyst loaders

`analysis/soy_analytics._load_soy_prices` and `_load_currency_data` are
now `@lru_cache(maxsize=1)`. Each is called by 5+ analysts per dashboard
run; the cache collapses 5× redundant `read_prices()` + 25× redundant
`compute_all_technicals` invocations into one. A new
`clear_loader_cache()` helper resets both caches between runs.
(Run 6 hoisted these into `analysis/loaders.py` to dedupe with briefing.)

### Forward-fill behaviour pinned by tests

`clean_ohlcv`'s `ffill(limit=3)` is preserved. Two new tests
(`tests/test_clean.py`) lock the documented behaviour in:

1. Rows with all-NaN OHLC are **dropped** before the ffill step, so
   weekend/holiday bars are never silently filled.
2. Rows with partial-NaN OHLC are ffilled, and RSI computed on a
   filled bar treats it as a zero-delta day (does not skip it). This
   is intentional — keeps index alignment stable — but a future change
   to reorder dropna/ffill or to skip-on-fill would now break the test.

### Turso integration test

New `tests/integration/test_turso.py`. Skipped unless both
`TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` are set in env. Exercises
the full `save_* → read_*` path against a real libsql connection:

* `save_price_data` → `read_prices` roundtrip
* INSERT OR REPLACE upsert semantics
* `save_cot_data` numeric values
* `save_psd_data` INTEGER NOT NULL key column
* `save_freshness` failed-status preserves prior `last_success`

All test rows use sentinel commodity names (`TEST_TURSO_<uuid>`) and
clean up in `finally` blocks, so the integration suite is safe to run
against a shared DB.

### Test suite

All 298 existing tests + 53 new (51 clean, 5 Turso skipped, plus
existing) pass. Run with `pytest tests/`.

---

## Unreleased — Run 3: Typed fetch results & retry hygiene

Goal: give every fetcher one consistent vocabulary for "succeeded /
returned-nothing / actually-failed", and stop each fetcher rolling
its own retry-sleep formula.

### `pipeline/results.py`: `FetchResult` + `ScraperShapeError`

New `FetchResult(data, status, error)` frozen dataclass with a
`Literal["ok", "empty", "failed"]` status. Three constructors —
`FetchResult.ok(data)`, `.empty(reason)`, `.failed(error)` — plus
`total_rows` / `has_rows` properties. `ScraperShapeError` subclasses
`ValueError` so existing `except ValueError` blocks still catch it,
but callers can distinguish a shape problem from a generic value
error. The three states matter because they each demand a different
freshness row: `success` with rows, `success` with zero rows, or
`failed`. Conflating them was the original sin behind "the
dashboard says everything is green but no exports loaded".

### `fetchers/_backoff.py`: shared retry-sleep helper

`retry_sleep(attempt)` — exponential backoff capped at 30s, plus
0–1s of jitter, sourced from `config.RETRY_DELAY`. Single formula
across all fetchers replaces nine copies of `time.sleep(RETRY_DELAY)`
with no jitter. Jitter matters because the 18-layer pipeline
otherwise produces synchronised retry storms against shared
upstreams (USDA, FRED, EIA).

### Fetcher rollout

`akshare.py`, `conab.py`, `cot.py`, `eia.py`, `export_sales.py`,
`fred.py`, `psd.py`, `weather.py`, `worldbank.py`, `yfinance.py`,
and `usda.py` all now import `retry_sleep`. Bonus cleanup as part
of the same pass: each fetcher's broad `except Exception:` was
narrowed to the actual transport+parse exceptions
(`requests.RequestException`, `ValueError`, `KeyError`,
`AttributeError`) so unrelated bugs (e.g. a `NameError` from a
refactor) surface as crashes instead of silent empty DataFrames.

### Tests

* `tests/test_results.py` — 5 tests covering each constructor's
  status/error/data invariants, `has_rows` when every frame is
  empty, and `ScraperShapeError` IS-A `ValueError`.

---

## Unreleased — Run 2: Critical-layer exit code + empty/failed freshness

Goal: make the pipeline's exit code meaningful for CI, and make
freshness tracking honest about what actually happened.

### Critical-layer exit code

`main.py` now declares `CRITICAL_LAYERS = ("prices", "fred")` and
returns `1` if either fails. Non-critical layer failures continue
to be logged but no longer poison the deploy. The script is now
invoked via `sys.exit(run())` so the exit code propagates to
GitHub Actions / cron.

### `_mark_empty` vs `_mark_failed`

Two new helpers in `main.py`:

* `_mark_failed(layer)` writes a freshness row with
  `status='failed'`, preserving any prior `last_success`. This
  is what powers the dashboard's "last good run was N days ago"
  badge.
* `_mark_empty(layer)` writes a freshness row with
  `status='success', rows_fetched=0`. This is the legitimately-empty
  case (no inspection report this week, no contract trades) — it
  is NOT a failure and should not show a stale-data warning.

Before Run 2, the only signal was "row exists in freshness" — empty
was indistinguishable from never-ran, and failed was
indistinguishable from succeeded.

### Tests

* `tests/test_main_exit_code.py` — 4 tests: exit 0 when both critical
  layers succeed, exit 1 when `prices` fails, exit 1 when `fred`
  fails, and a failed layer writes a `status='failed'` freshness
  row that preserves the prior `last_success`. Fetchers are stubbed
  with `monkeypatch`, so the test runs in ~50ms with no network.

---

## Unreleased — Run 1: Dev tooling, test harness, fast wins

Goal: get the project a real dev loop — type checker, linter,
tests with coverage — so the larger refactors that follow can move
with confidence. Plus a handful of standalone correctness fixes.

### Tooling

* `pyproject.toml` — pytest config (`pythonpath = ["."]`,
  `testpaths = ["tests"]`), coverage scoped to `pipeline`,
  `analysis`, `app`, ruff (E/F/I/UP/B/SIM/DTZ003, line-length 120,
  py310 target), mypy (`strict_equality`, `warn_unused_ignores`,
  `ignore_missing_imports`, py310 target, scripts/data/docs excluded).
* `requirements-dev.txt` — `pytest`, `pytest-cov`, `responses`,
  `ruff`, `mypy`.
* `.github/workflows/ci.yml` — runs on every push to `main` and
  every PR: lint + type-check (`analysis pipeline`) + tests with
  `--cov-fail-under=60`. Concurrency group cancels superseded runs.

### `tests/conftest.py`: shared fixtures

* `synthetic_ohlcv` — 300 business days of seeded random-walk OHLCV.
  Long enough for 200-day MA, MACD warm-up, and the 120-day
  Bollinger squeeze lookback. Seed is fixed so tests are deterministic.
* `tmp_db` — temp on-disk SQLite with every project table created.
* `patched_db` — temp DB plus monkeypatched `get_connection`,
  `DB_PATH`, `STORAGE_DIR`, and `is_cloud` in both `pipeline.store`
  and `pipeline.query`, so `save_*`/`read_*` transparently target
  the temp DB without the test having to do any plumbing.

### Initial test modules

`test_units.py`, `test_spreads.py`, `test_signals.py`,
`test_technical.py`, `test_usda_year_range.py` — covering unit
conversion (cents/bu → USD/MT etc.), the soybean crush spread, MA
crossovers / volume spikes / RSI extremes / divergence / MACD
crossovers / Bollinger squeeze, MA + RSI + MACD + Bollinger +
volatility math, and the dynamic-year-end behaviour of the USDA
fetchers.

### Correctness fixes shipped alongside

* **Seasonal sample-size guard** — `monthly_seasonal` now requires
  `SEASONAL_MIN_YEARS_PER_MONTH=5` observations per calendar month
  before reporting an average; otherwise returns empty. To make
  that bar achievable, `DEFAULT_HISTORY_PERIOD` bumped from `"2y"`
  to `"15y"`. Reason: a 2-year mean confounds trend with seasonality
  and was being reported as a "seasonal norm".
* **Mutable-default-arg cleanup** — `analysis/technical.py`'s
  `add_moving_averages` and `calculate_volatility` had
  `windows=[20, 50, 200]` / `[20, 60]` mutable defaults; now
  `windows: list[int] | None = None` with an explicit `if None`
  guard.
* **Dead-code removal** — `OPTIONS_COMMODITIES` in `config.py` and
  the `options_sentiment` table in `pipeline/schema.py` were never
  wired into the pipeline. Both deleted.
