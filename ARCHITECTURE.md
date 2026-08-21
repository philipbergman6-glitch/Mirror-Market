# Architecture

The goal is for a contributor to read this and know which file to open for any
given task — and to know the load-bearing rules of each subsystem before
touching it. Per-source detail and data traps live in `LAYERS.md`; project
invariants and commands in `CLAUDE.md`; visual rules in `DESIGN.md`; the
data-age vocabulary in `LATENCY.md`.

## The pipeline

```
┌────────────┐    ┌────────────┐    ┌────────────┐    ┌────────────┐    ┌────────────┐
│   FETCH    │──▶ │   CLEAN    │──▶ │   STORE    │──▶ │  ANALYZE   │──▶ │   RENDER   │
│ fetchers/  │    │ pipeline/  │    │ pipeline/  │    │ analysis/  │    │ scripts/   │
│            │    │  clean.py  │    │  store.py  │    │            │    │ generate_  │
└────────────┘    └────────────┘    └────────────┘    └────────────┘    │  html.py   │
                                          │                  ▲          └────────────┘
                                          ▼                  │                  ▲
                                    ┌──────────────────────────────┐            │
                                    │      data/storage/...db      │            │
                                    │  (SQLite local OR Turso       │            │
                                    │   cloud — see                 │            │
                                    │   pipeline/connection.py)     │            │
                                    └──────────────────────────────┘            │
                                          ▲                                     │
                                          │                                     │
                                    ┌──────────────────────────────┐            │
                                    │      pipeline/query.py        │           │
                                    │      (read_* functions)       │ ──────────┘
                                    └──────────────────────────────┘
```

The five stages are independent — a fetch failure does not block analysis
of previously stored data, and the dashboard reads from the DB so it can
be re-generated without re-fetching. `main.py` orchestrates the
fetch→clean→store stages; `scripts/generate_site.py` owns the complete page
list and delegates the headline to `scripts/generate_html.py`.

## Stage detail

### Fetch (`fetchers/`)

One module per data source. Each fetcher returns
`dict[str, pd.DataFrame]` keyed by commodity/region/pair, and uses
`fetchers/_backoff.py` for retry policy. Failures raise — they don't
return empty silently — so `main.py` can record the layer as `failed` in
the freshness table. The layer catalog, run-state grading rules
(`failed` / `no_publication` / `stale` / `incomplete` / skipped), and every
source's traps are documented in `LAYERS.md`.

### Clean (`pipeline/clean.py`)

Normalises raw frames: parses `Date` to datetime, sets index, forward-fills
gaps with `limit=3`, drops all-NaN rows, warns on >10% daily moves and
zero/negative volume. Returns copies — originals are never mutated.
Contains `_check_nan_gaps()` used by `clean_ohlcv()` and
`clean_dce_futures()`, plus `clean_india_domestic()`, `clean_brazil_spot()`,
`clean_safex()`, `clean_sagis_deliveries()`.

### Store (`pipeline/store.py` + `schema.py`)

Tables are defined in `pipeline/schema.py` as `CREATE TABLE IF NOT EXISTS`
strings. `pipeline/store.py` exposes `save_*()` functions that batch-upsert
via `executemany` (INSERT OR REPLACE — the pipeline is safe to re-run).
`pipeline/connection.py` returns a Turso cloud connection when
`TURSO_DATABASE_URL` is set, a local SQLite connection otherwise — call
sites don't care which (the cloud path is dormant; see `LAYERS.md`).

- Database: `data/storage/mirror_market.db` (SQLite, gitignored)
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `wasde`, `inspections`, `inspection_port_flows`, `inspection_destinations`, `gulf_bids`, `argentina_fob`, `eia_energy`, `brazil_estimates`, `data_freshness`, `commodity_freshness`, `india_domestic_prices`, `brazil_spot_prices`, `safex_prices`, `sagis_deliveries`, `sagis_supply_demand`, `cec_estimates`, `ec_oilseed_prices`, `ocean_freight_rates`, `port_vessel_activity`, `briefings`
- `forward_curve` keys on `(commodity, contract_month, fetched_date)` — one full curve per run accumulates term-structure history; `read_forward_curve()` returns only each commodity's latest snapshot.
- V1 pipeline config lives in `config.py` (tickers, API URLs, region coordinates, thresholds). V2 source/dataset cadence, identity, freshness, validation, retention, rights, and criticality policy lives in the authoritative `trust.registry` contract registry.

#### Git-based history persistence (`pipeline/history.py`)

CI runs on an ephemeral runner with an empty DB each day. Most layers self-heal by re-downloading full history, but snapshot-only sources don't: AgRural (1 row/day — the Brazil basis source), SAFEX, forward curve, CONAB survey revisions, inspections (>3 weeks) incl. port/destination breakdowns, Gulf bids, Argentina FOB (MAGyP serves history but re-fetch depth is unproven), CEPEA (>~10 sessions), WASDE (>12 months), India mandi (current-day snapshot), **export sales** (ESR is fetched for the current marketing year only, so the outgoing year vanishes at each MY rollover), **briefings** (generated from that run's DB; a past day's `text`/`snapshot_json` is reconstructible from no source at all), **SAGIS monthly supply & demand** (only the current season plus the two most recent finals are ever listed; older seasons exist only as per-month announcement files this layer does not fetch, so a season scrolling off the page is unrecoverable), and **SAGIS deliveries** (the DT export *does* serve history, but only a fixed 9-season window — nothing upstream promises it grows, so a season that rolls off is unrecoverable from an ephemeral CI DB). These tables round-trip through CSVs in `data/history/` (committed to git): `main.py` calls `import_history()` after `init_database()` (INSERT OR IGNORE — DB rows win over CSVs) and `export_history()` after the layers (atomic per-table writes, PK-sorted for stable diffs). The deploy workflow commits `data/history/` back to `main` with `[skip ci]`. A failed import aborts the run so a bad seed can never be exported over good history. Cloud DB (Turso/Supabase) was explicitly rejected for this — do not reintroduce it as a CI requirement.

`export_history()` refuses three kinds of regression rather than committing them: an **empty** table (fetch layer failed), a **shrinking** row count (DB behind the committed CSV — e.g. a local run that skipped `import_history()`), and a **dropped column** (`CREATE TABLE IF NOT EXISTS` never adds columns, so a DB predating a schema change exports narrower rows at an unchanged row count). Each logs an error and leaves the CSV untouched. `MIRROR_HISTORY_ALLOW_SHRINK=1` overrides the latter two when a prune or column removal is genuinely intended.

### Analyze (`analysis/`)

Pure functions over the DataFrames returned by `pipeline/query.read_*()`:

- `analysis/technical.py` — SMA (20/50/200), RSI (Wilder smoothing), MACD (12/26/9), Bollinger Bands, historical volatility, price changes
- `analysis/signals.py` — 20/50 and 50/200 MA crossovers, volume spikes, RSI extremes/divergence, MACD crossovers, Bollinger squeeze
- `analysis/spreads.py` — Soybean crush spread (Oil*11 + Meal*2.2 - Beans)
- `analysis/correlations.py` — Cross-commodity matrix, commodity-vs-currency, rolling correlation
- `analysis/seasonal.py` — Monthly seasonal averages, current vs historical norm
- `analysis/forward_curve.py` — Contango/backwardation, curve slope, calendar spreads
- `analysis/stocks_to_use.py` — Stocks-to-use ratios from PSD; tight-supply alerts
- `analysis/zscore.py` — Shared z-score helper used by COT and weather sections
- `analysis/health.py` — Per-commodity data health checks (stale data, flat prices, missing commodities)

Two consumer layers sit on top of these primitives, both pulling
price/currency frames from `analysis/loaders.py` (shared, cached;
`clear_loader_cache()` resets between pipeline runs) so they stay in sync:

- `analysis/briefing/` — daily text briefing. Each section lives in its own module under `analysis/briefing/sections/` (prices, crush, economic, usda, crop_progress, wasde, export_sales, inspections, gulf_basis, transport, dce, forward_curve, eia, conab, currencies, cot, weather, psd, worldbank, emerging_markets, basis, stocks_to_use, correlations, seasonal, market_drivers, signals, freshness). `orchestrator.py` joins them; `types.py` defines the typed `BriefingData` returned by `generate_briefing_data()`; `generate_briefing()` is a thin wrapper returning `BriefingData.text`. `snapshot.py` distills `BriefingData` into structured `snapshot_json` for the briefings archive (schema v2, marked by a top-level `schema_version`; rows without it are v1) — raw numbers and components, never display labels; every block degrades to None/{} on failure.
- `analysis/soy_analytics.py` — 9 analyst functions that produce page-shaped dicts for the dashboard: command_center, supply, demand, technicals, relative_value, risk, seasonal, forward_curve, emerging_markets.

Briefing section order: 1 Data Freshness Warnings · 2 Prices · 3 Crush Spread · 4 Brazil Basis (Paranaguá FOB vs CBOT) · 4b US Gulf Basis (CIF NOLA barge) · 4c Cross-Origin FOB Board (Gulf × Paranaguá × Argentina) · 4d Transport (ocean freight + vessel lineups) · 5 Economic Context · 6 USDA Fundamentals · 7 Crop Conditions · 8 Yield Curve · 9 WASDE · 10 Stocks-to-Use · 11 Export Sales · 12 Export Inspections · 13 DCE vs CBOT · 14 Forward Curve · 15 Biofuel & Energy · 16 Brazil Crop Estimates (CONAB vs USDA) · 17 Currencies · 18 COT · 19 Weather Alerts · 20 Global Supply (PSD) · 21 World Bank Prices · 22 Emerging Markets (SA SAFEX + SAGIS pace + SAGIS S&D + CEC revisions + Brazil farmgate + India mandi vs CBOT + Nigeria) · 23 Correlations · 24 Seasonal · 25 Market Drivers · 26 Signals (by severity).

### Render (`scripts/generate_html.py` + `app/`)

`scripts/generate_site.py` renders the headline, Players, and eight market
URLs into a private candidate directory. It calls analyst functions, builds
Plotly figures via `app/charts.py`, and embeds the archived daily text briefing
in the headline. A page failure may create a dated tombstone inside that
candidate for diagnosis, but the candidate is not public yet.
The full static-site contract is in "The static site contract" below.

### Verify and promote (`trust/site_promotion.py` + `scripts/smoke_site.py`)

Normal publication uses the existing candidate/verification/promotion seam.
The v1 bridge verifies the complete static candidate while named-contract v2
coverage continues to expand: all expected URLs and links, current core soy
benchmarks, briefing presence, aligned crush inputs, no tombstones, valid
generation/observation timestamps, the authoritative layer count, and
desktop/mobile viewport fit. Only a verified candidate is uploaded. If render
or verification fails, GitHub Pages is not called and the previous trustworthy
edition remains public. After deployment, the same smoke contract reads the
real Pages URLs; alerting identifies page-generation, contract, deployment,
and post-deployment failures separately. The semantic-contract `scan()`
(see "Semantic contract" below) also runs inside
`trust.site_promotion.verify_site_candidate`, so a misleading edition is
refused at the promotion gate rather than only in CI.

## The static site contract

A small fixed set of static pages deployed to GitHub Pages: the headline page, the players map, and one page per market. **The market is a parameter, never a code path** — `if market == "india"` in a builder is the drift the whole contract exists to prevent; per-market variation belongs in the registry descriptor.

Key files:
- `config.py` → `MARKETS` — the market registry, keyed by slug. **Pointers, never values**: table/column/key *names*, never a price, a date or a tier. Key order is nav order is the headline ledger's row order (`cbot · dalian · brazil · argentina · india · europe · south_africa · nigeria` — role in the trade), declared once so the two consumers cannot disagree.
- `config.py` → `CRUSH_BOARD` — the four markets the headline's crush board compares, in registry order. Registry data for the same reason `LEDGERS` is: a builder that derived the set ("every market with a crush descriptor") would drop Brazil, whose empty card is the point.
- `config.py` → `LEDGER_LEGS` / `LEDGERS` — the propagation ledger's leg catalog and each page's counterpart set (M12 #161). A **second id space**: `LEDGER_LEGS` is keyed by leg id (`us_gulf:cif`), `MARKETS` by market slug, and a leg carries only what its owning market's descriptor cannot say — which key it is, what to call it, and how its print is proved. Everything else (table, unit, FX pair, home currency) is read from the owner, never restated.
- `app/markets.py` — the typed view of that registry (`load_markets()`), plus `compute_tiers()`. It lives outside `config.py` because it reads the DB and `config` is imported *by* `pipeline`. **`quote_kind` is required on every leg whose unit is a price** (`PRICE_UNITS`), rejected at load; `tonnes` and `observation` legs have none and must not invent one. The general trap: **a descriptor reused by a second consumer may be missing labels the first never needed**, so the label belongs to the descriptor and the check sits at load rather than in each renderer. A header chip that labels one animal also cannot label two — block 04 and the ledger table both state the kind per row.
- `app/blocks.py` — the nine-block set and the `{state, reason, data}` envelope every block builder returns. `Block.__post_init__` **raises** when a non-`ok` state carries no reason, so "every empty state names its reason" is enforced by the type rather than by nine builders remembering.
- `app/block_builders.py` — the nine builders that fill that envelope, all **generic SQL over the registry descriptor**: `Source` names the table, date column, key column, value column and unit, so a tenth market is a `config.MARKETS` entry and no code. `Source.to_usd_mt` is the site's **only** conversion site — `native_exchange` (cents/bu, cents/lb, $/short ton), `usd_per_bushel` (AMS 3147 prints flat CIF bids in $/bu), `home_per_mt` (× the `<CCY>/USD` rate **of that row's own date**) or `usd_per_mt`. A `home_per_mt` leg with no FX rate renders its USD/MT as blank, never the local number relabelled. Crush and basis are struck on a session **all** legs printed — no cross-day arithmetic — and a home-currency crush margin is computed only where the legs share one per-MT currency. Several rows for one key on one date (AMS barge locations, MAGyP shipment windows) are averaged and the count is rendered.
- `app/sections.py` — the same `{state, reason, data}` envelope for the five headline sections (emerging markets, relative value, risk monitor, forward curves, seasonal). The two registry-driven headline sections — the propagation ledger and the crush board — live in `app/block_builders.py` instead, beside the per-market builders they reuse. Also owns the **chart budget**: `clip()` cuts each series to the window the figure actually reads (`CHART_WINDOW_SESSIONS`), applied where the series meets the figure so the stats above the chart keep their full history.
- `app/templates/blocks/NN_<id>.html.j2` — one partial per block, numbered so file order on disk is block order on the page; `app/templates/sections/*.html.j2` the same for the headline. Markup lives here and nowhere else: an f-string builder cannot enforce "same block, same treatment, eight markets", which is why M8 made this a contract.
- `app/templates/_base.html.j2` — owns `<head>` entirely: fonts, the DESIGN.md palette, the masthead and both nav bars. Every page extends it, `players.html.j2` included, or it becomes the page that drifts.
- `app/templates/market_page.html.j2` / `market_brief.html.j2` / `market_stub.html.j2` — one real template per tier (a brief is not a page with more hatching), plus `tombstone.html.j2`.
- `scripts/generate_site.py` — the orchestrator: owns the page list and the failure-isolation policy.
- `scripts/generate_html.py` — the **headline page's** renderer, one entry in that list.
- `.github/workflows/deploy-dashboard.yml` — daily pipeline run + site generation + Pages deploy.

**Tier is computed from the DB every run, never hard-coded** (M1 #143): a daily price leg plus ≥3 of {ledger, crush, basis, weather} → `page`; less than that, or no daily leg with ≥2 → `brief`; otherwise `stub`. "Present" means *current within that layer's own `LAYER_MAX_DATA_AGE_DAYS` budget*, not "the descriptor names a table" — a market whose scraper died a fortnight ago demotes itself. The ledger is not probed separately: it is daily-only, so it is present exactly when a daily leg is. **The URL never changes with the tier** — `docs/markets/india.html` exists in all three tiers, because anything else means yesterday's link 404s when a scraper breaks.

**A demotion caused by our own outage must say so.** Age alone cannot tell a rate limit from a market where nobody publishes the series: both surface as `no rows`. `_ingest_status()` consults `data_freshness` — which records `status='failed'` with `last_success` held back — and appends "our `<layer>` ingest failed; last good run `<date>`" to the note. Without it the page states our outage in the same words it uses for a genuine market absence, which reads as a judgement about the market (#212).

### The propagation ledger (block 02, headline section 03)

Answers one question — who has repriced, and who has not printed. Shape from M3 #145: settlement-ordered rows, each dual-quoting USD/MT over the venue's own print, both moves with an `FX` tag when the currency did the work, and a state pill so silence can never read as flat. Counterpart sets from M12 #161, which are **registry data** (`config.LEDGER_LEGS` + `config.LEDGERS`), not a code path.

Five things about it are load-bearing:

- **A row is a *leg*, not a market.** `us_gulf:cif` is the AMS CIF NOLA bid living on CBOT's `basis` descriptor and has no market key of its own, so the registry carries **two id spaces**. Every leg id resolves to (market, sub-block, key) or `load_markets()` hard-fails — an unresolvable leg would render an empty row, which reads as "that market has not printed", the ledger's most important statement made by accident.
- **A row stamp is not a print.** Grain SA re-dates a carried SAFEX price with `Volume 0` (#157), so a leg may name a `trade_proof_column`: a row with `<= 0` there is not a print. NULL keeps the row — that is proof of nothing, and dropping it would invent an outage. Assessments (CEPEA, AgRural, AMS, MAGyP, mandi) publish no volume by nature and are not asked for one.
- **Being behind is judged per leg.** `FRESHNESS_WARNING_DAYS = 7` lets a daily leg go six days silent (M4 §3.4 trap 5), so each leg carries its own `expected_gap_days` (default 4 — a weekend plus a holiday). `dark` still uses the layer's own `LAYER_MAX_DATA_AGE_DAYS`, the number `main.py` grades on, so the site and the pipeline cannot disagree about whether a source is alive.
- **A spread is one session's number.** Counterpart rows carry a spread against the pinned own leg, struck on the most recent session *both* printed and stamped with that date; where there is no common session the cell is blank with the reason. Two dates subtracted would manufacture an arbitrage out of a calendar gap.
- **A dual quote needs two observations.** Only a `home_per_mt` leg has a second currency; a `usd_per_mt` leg (Argentina) prints one number and a dash, never the same figure twice. This is the EC lesson from the other side (#163): that workbook's EUR column is its USD one divided by an ECB rate, so a dual quote there would be our own arithmetic dressed as the venue's second opinion.

Sets are **fixed, never seasonal**; every ledger is **one good** (the soybean — a meal ledger would be a second block); row counts are **4 or 5, never padded** (Dalian and Argentina have three genuine counterparts and the fifth candidates are filler). CBOT is not pinned everywhere — dropped from India, where the GM import ban makes the +66% uncloseable, and demoted to a labelled reference row, last, on South Africa. India's ledger is **two domestic state medians and no foreign leg at all**. Europe and Nigeria get **no ledger block** — `absent` with a reason, a legal page configuration rather than a degraded one. The headline's eight rows are **markets**, with the market cell as the link and **no spread column** (there is no pinned leg to spread against); Europe carries no value (`out of cadence`, not `dark` — its weekly leg is not an outage) and Nigeria is `dark`.

**A basis must say whether trade connects its two legs** (M19 #222). Every `basis` descriptor declares `arbitrage`: `open` (cargoes move, so freight/quality/duty bound the spread — Gulf, Paranaguá, Argentina FOB, DCE import parity) or `policy_blocked`, which additionally **requires a `caveat`** and fails the build without one. India is the only `policy_blocked` leg: its mandi bean prints ~+66% over CBOT because GM imports are banned behind a tariff wall, it reached ~2× in 2021, and nothing closes it — rendered with the same treatment as Paranaguá FOB it would invite a trade that cannot be taken. That basis line is also what makes India a `page` rather than a `brief`: mandi is bean-only, so ledger + basis + weather is the whole of its three.

### The crush board (headline section 04)

Answers the second headline question — is processing paying, and where. Four markets side by side (`config.CRUSH_BOARD`, decided by M2 #144, built by M16 #208), each card linking to the market page whose block 03 carries the depth. Three things about it are load-bearing:

- **The level is block 03's number, not a second calculation.** Every card calls the same `crush_block` its market page does, so the headline and the page cannot print two different margins for one market. That is M7 #149's finding turned into code: one engine serves every market, and only yields, FX and the kind label vary. The DCE tile in the key-metrics grid and the CBOT crush spread that opened the relative-value section were the two surfaces this replaces — a third and a fourth crush, each computed its own way, neither saying what kind of margin it was.
- **The kinds do not collapse.** Four numbers in one row of cards is the easiest place on the site for a board close, an administered minimum and a physical assessment to read as one "crush" line, so each card carries its own kind chip (M2 constraint 3 / M3 #145 constraint 4). Argentina's is an administered Ley 21.453 minimum and is no longer provisional — #162 cross-checked NCM `23040010100B` against dataset 358's labelled meal series, 52 of 52 business days exact.
- **A range is struck by the engine that struck the level, or not at all** (#208 sub-question 4). Dalian and Argentina restrike the same generic margin for every stored session — one session for all three legs, and that row's own date's rate, never a later one, so a session older than every stored rate is dropped rather than converted forward. CBOT's level is ZSU26/ZMU26/ZLU26 out of `forward_curve`; the continuous front-month series beside it would produce a mean around a *different* margin, so that card states it has no range instead. Below `CRUSH_RANGE_MIN_OBS` (20) sessions a leg prints its level with "no range yet" and the count.

Brazil sits on the board **without a number**: no Brazilian oil or meal cash quote is ingested (an unbuilt scrape, M7 #149), so the card carries the registry's own reason in the same `absent` empty state the market blocks use. Dropping the card would say something quite different — that Brazil has no crush industry.

**Failure isolation, three levels.** A block that raises renders as an empty state with reason `generation error` — the same *shape* as a missing source, a deliberately different *reason*. A page that fails becomes a **dated tombstone inside the private candidate**, never a silently retained old file. The headline failing fails generation outright. The promotion contract rejects every tombstoned candidate before Pages upload, so the last trustworthy public edition remains available while the failure is loud.

## Hedge (`analysis/futures/` + `app/workstation_page.py`, Phase 3)

A second consumer layer beside the briefing and the analyst functions, and the
only one that speaks in *named contracts*. Everything else in this repo works
on the continuous front-month series `prices` holds; a hedge cannot, because
ZSX26 and ZSF27 are different instruments with different termination dates.
Output: `docs/workstation.html`, built by `app/workstation_page.py`.

```
analysis/futures/domain.py       vocabulary — specs, contract identity, expiry
                                 rules, unit conversion (stdlib only, no SQL)
              ▲
              │
analysis/futures/providers.py    the only SQL-aware module here; reads
                                 forward_curve / prices / currencies and hands
                                 back NamedContract quotes with a coherence
                                 verdict and a freshness state
              ▲
   ┌──────────┼─────────────┬────────────────┬──────────────┐
curve.py   continuous.py   hedge.py       positions.py    events.py
(spreads,  (stitched      (sizing,        (entered book,  (release
 carry,     series, roll   coverage,       marks, P&L,     calendar
 percentile) method)       warnings)       limits)         from our
              │              │                 │           own rows)
              │        scenarios.py            │
              │        (futures/basis/FX/      │
              │         yield shocks)          │
              │              │                 │
              │        ticket.py           options.py
              │        (proposal —         (Black-76 +
              │         not routed)         no chain)
              └──────────────┴─────────┬───────┴─────────────┘
                                       ▼
                              alerts.py (exposure alerts)
                                       ▼
                            app/workstation_page.py  →  docs/workstation.html
```

`domain.py` holds `CONTRACT_SPECS` (size, native unit, tick, published expiry
rule, first notice rule) for nine products, `NamedContract`, `ContractQuote`,
`ContinuousSeries`, the exchange holiday calendar and the business-day
arithmetic. Its MT factors are pinned against `pipeline/units.py` by test —
one table of densities, not two. `QuoteProvider` is a Protocol and
`SqliteQuoteProvider` its one implementation: an authoritative feed replaces
that class and nothing else.

The load-bearing rules, each enforced by a type or a test rather than by
reviewer memory:

- **A named contract is not a continuous series.** Different types, neither accepted where the other is expected; `ContinuousSeries.is_hedgeable` is always `False`. Where the stored named-contract history is shorter than `MIN_SESSIONS`, a stitched series is **withheld** rather than padded with the provider's own front month — the silent substitution this phase exists to prevent. The provider series is still shown, labelled `provider_front_month` and carrying "the provider does not publish its roll dates".
- **Nothing here is a settlement.** Every quote is `PriceType.DELAYED_CLOSE` and `is_settlement_proven` is `False` on all of them. `PriceType.SETTLEMENT` exists for the day an authoritative provider is substituted and is never constructed today. The word "settlement" appears on the page only in denials.
- **Expiry is a published rule or it is absent.** ZS/ZM/ZL/ZC/ZW use the CBOT grain rule (business day before the 15th), LE the last business day, HE the 10th business day. The two ICE softs were `NOT_ENCODED` until their rules were read off the **rulebook** rather than a summary page: Sugar No. 11 Rule 11.06(a) — the last full trading day of the month preceding delivery, plus a January carve-out that no listed month reaches — and Cotton No. 2 Rule 10.02(a), where Last Trading Day is the 10th business day before Last Delivery Day and Last Delivery Day is the 7th-last business day of the month. 10 + 7 is exactly the "seventeen business days from end of spot month" the contract summary states, and it is the *pair* of statements that proves the counting convention (last business day = 1); one alone would have left an off-by-one nobody could see. Both are checked against dated examples in the tests — CTZ24 last trades 6 Dec 2024. The `NOT_ENCODED` machinery stays and stays tested (on a spec built for it, `tests.test_futures_hedge.unencoded_contract`) because the next product added may arrive without a rule: no days-to-expiry, no annualised carry, no roll window, no expiry alert, `hedge_month_candidates` returns nothing, the hedge reports `no_hedge_month`, and a leg named by hand reports `expiry_not_encoded` because silence there would read as safety.
- **A missing first notice day has two different reasons, and they are different states.** `first_notice_rule = None` means *this project* has not encoded the rule (Live Cattle, Lean Hogs). `NO_NOTICE_DAY` means the contract runs no notice-day mechanism at all — Sugar No. 11 is the one such product here, where Rule 11.06(b) attaches the delivery obligation to the close of the **last trading day itself**. That is stricter than an FND, not looser, so rendering it as "not encoded" would send a hedger looking for a date that does not exist and imply room they do not have. Cotton is the opposite edge: its FND (5 business days before the first business day of the delivery month) falls a fortnight *before* its last trade, which is the whole reason this package keys roll alerts on FND.
- **A curve is one session, re-checked at read time.** `forward_curve` is keyed `(commodity, contract_month, fetched_date)` and, until this phase, nothing deleted from it — so two runs on one day left the earlier run's legs standing (2026-08-11: seven Soybean legs, six stamped that session and `ZSN27.CBT` undated). Fixed at write time (`_replace_curve_snapshot`) *and* at read time (`_coherence`), because a leftover leg has a valid key and a plausible price. Legs off the newest observation date are dropped and named; the verdict rides on the analysis, and an incoherent curve suppresses the inversion reading in favour of a data alert. Neither fix cleans up rows already written, and the committed history holds some — `scripts/prune_curve_snapshots.py` is the operator tool for that, dry-run by default. It cannot run in a PR: the `history-guard` CI job fails any PR touching `data/history/`, so the cleanup is a deliberate action against a database followed by an ordinary export under `MIRROR_HISTORY_ALLOW_SHRINK=1`. The rule it applies is narrow on purpose — within one `(commodity, fetched_date)` group it keeps the legs on the newest *non-null* observation date, and a group where **every** leg is null is left entirely alone. Those are legacy rows predating the column, not duplicates, and a curve leg is unrecoverable once deleted.
- **First notice day, not last trade, is the hedger's date.** A merchant long past FND is exposed to delivery, so `roll_alerts` fires on FND and `fnd_inside_pricing_window` warns when the pricing period runs past it.

Volume is captured from yfinance and is `None` — never `0.0` — when absent. **Per-contract open interest is always NULL**, because no source publishes it per delivery month and a zero reads as "nothing open". A *whole-product* figure does exist and is shown beside the curve: `cot.total_open_interest`, the CFTC's weekly all-months-combined number, carried as its own type (`AggregateOpenInterest`) with its own report Tuesday rather than as a field on a quote. Putting it on a contract row would assert two false things at once — that one month holds the product's whole open interest, and that a Tuesday figure belongs to the price session. The join is by name and needs no mapping table: `config.COT_COMMODITIES` keys are the same nine strings `CONTRACT_SPECS` uses, which a test pins.

There is still **no options chain**, and that is a measured fact rather than an assumption — `yfinance.Ticker(t).options` returns `()` for every ticker here including named contracts, and no layer carries a strike, premium or implied volatility. So `fetch_chain` returns `ChainUnavailable` with its reason rather than an empty ladder. `data/reference/options/` is the manual entry point: a directory of YAML documents on exactly the terms `data/reference/positions/` uses — missing directory is an empty ladder, a present but malformed one raises — where each row carries a mandatory `source` naming who quoted it and **exactly one** of `premium` or `implied_volatility`, the other being derived (bisection one way, Black-76 the other). Supplying both is refused, because two inconsistent numbers on one row leave nothing saying which was believed. An option whose underlying has no board price that session is reported unvalued with its reason rather than priced against an invented forward, and every valued row is stamped `PriceType.MANUAL` with the American early-exercise caveat riding on it. The discount rate is a stated page constant (`OPTION_DISCOUNT_RATE`), not a number lifted from the `economic` layer — rho is reported per rate point so the choice stays visible.

**No routing, and no seam for one.** Every ticket carries `PROPOSAL — NOT ROUTED` in text, JSON and HTML. Positions come only from a YAML document under `data/reference/positions/` or a CSV import — this project ingests no account, broker or clearing feed, so a book can only come from the user, and a *present but malformed* file raises rather than rendering as an empty book. With no book entered the hedge section shows a labelled 1,000 MT **reference calculation** so the arithmetic stays inspectable; it says on the row that it is not a position.

## Positions, limits and options workflow (Phase 6)

The supervised desk workflow on top of the workstation: a book that can be imported rather than retyped, exposure decomposed into the views a mandate is written in, limits that are visible when crossed, the official clearing figure beside ours, and options the desk supplies itself. Six modules, and one boundary that runs through all of them.

- `analysis/futures/privacy.py` — the client-record boundary, and the reason the phase exists in this shape. Four guards: a **key** guard (`CLIENT_RECORD_FIELDS`), a **provenance** guard (any string naming `reference/{positions,options,clearing,import_profiles}`), a **path** guard (`assert_private_path` refuses anything under `docs/` or on the promotion contract), and **section redaction** (`redact_for_public`).
- `analysis/futures/exposure.py` — the seven views (flat price, basis, crush, FX, contract month, first notice, residual) and the metrics every limit key resolves through.
- `analysis/futures/limits.py` — `DeskLimit` over eleven exposure-backed keys with an optional `warn_at`, and `ok`/`warn`/`breach` with headroom.
- `analysis/futures/clearing.py` — `ClearingStatement`/`ClearingLine` from `data/reference/clearing/`, `PnlBasis`, and `reconcile()`.
- `analysis/futures/imports.py` — profile-driven import of a broker, clearing or ERP export: a dry-run `ImportReport` first, `apply_import` second.
- `analysis/futures/options.py` — extended with tz-aware `quoted_at`, `chain_from_csv`/`value_chain` for an externally supplied ladder, and `BLACK76_LIMITATIONS`.

Seven rules are load-bearing, each pinned by a test:

- **The public artifact never contains a book, and that is structural rather than remembered.** `build_view(audience=...)` defaults to **public** — the only default a privacy boundary may have — and the five client sections (`book`, `exposure`, `limits`, `clearing`, `options_entered`) render `absent` with a reason. `absent`, not `empty`: "nothing entered" and "not shown to you" are different states and a public reader is owed the second. The private edition goes to `data/workspace/workstation.html`, outside `docs/` and deliberately absent from `trust.site_promotion.expected_site_paths()`, and `assert_no_client_records` runs at write time in `scripts/generate_site.py` so a leak fails the page — a tombstone, blocking promotion — rather than being published and noticed. This closed a **live leak**: `_book_section` wrote `valuation.to_dict()` and the positions file's absolute path straight into `docs/workstation.html`, quiet only because CI's positions directory is empty. Two things the fix surfaced that a section-level guard alone would not have: the hedge, scenario and ticket sections are *sized from the book*, so the public edition always works the 1,000 MT reference example; and the valuation-derived **alerts** name a limit key and an observed tonnage, which is the book in one sentence, so the public edition is built without the valuation rather than built and filtered. The guard's own list is narrow on purpose — `exposure` was removed from `CLIENT_RECORD_FIELDS` because a public reference hedge honestly has one, and a key belongs there only when *no* public payload could contain it.
- **The official P&L and ours are two numbers and stay two numbers.** A reconciliation reports both columns, labelled by `PnlBasis`, with their difference against `CLEARING_RECONCILIATION_TOLERANCE_USD` — and there is no `total_usd`, `reconciled_usd` or `net_usd` anywhere in the payload, because a single figure would belong to neither desk and be acted on as both. A quantity mismatch is its own finding, not a price difference. A contract on the statement but not in the book is a finding in its own right — it is a position nobody recorded. Physical positions are not reconciled and the report says why: a clearer holds futures, not beans.
- **A settlement on a client's statement is `ATTESTED_SETTLEMENT`.** A seventh `PriceType`: official for that account, and still not proven by anything this project ingests, so `PROVEN_SETTLEMENT_SOURCES` stays empty and its confidence ceiling is `BOARD_REFERENCE`, not `EXECUTABLE`.
- **Importing is two steps and the first writes nothing.** `read_import` returns accepted rows, rejected rows with reasons, unclaimed columns and the file's sha256; `apply_import` refuses while anything was rejected unless `allow_partial=True`. Every refusal is one rule — *nothing is guessed*: a missing required column refuses the **whole file** before any row is read (a per-row failure there reads as bad data rather than a bad mapping); a sign convention is declared (`signed` or `side_column`), never inferred, because a short 68 lots read as a long 68 is a 136-lot error that looks like a position; a date format is tried once, since a fallback is how one file's March becomes another's April; an unmapped product code is rejected, because `SOJA` is *probably* soybeans and probably is not a book; a blank is never a zero. Re-import is idempotent by construction — every row's reference is `<sha256[:8]>:<row number>`, derived from the bytes.
- **A limit that cannot be measured produces no row, not a passing one.** A green line nobody checked is the most dangerous output here, so the page reports configured-vs-measured. `warn_at` must sit *below* `maximum` or it is refused — a warning that fires only after the breach is misconfiguration that looks like safety. An unknown key **raises** (it used to log and skip, which left a desk believing a mandate was being checked). Limits are reported, never enforced.
- **Hedging moves tonnes between views; it does not remove them.** Basis exposure is `max(unfixed, hedged)` rather than a sum, so tonnes that are both count once. The pricing convention (`pricing:`) decides which view a cargo lands in; omitting it is legal and means *not stated*, in which case the tonnes are counted at their **most exposed** reading and every line built from them says the convention was a default. A wrong value is refused — it would move tonnes silently between views, which is a risk report saying something untrue.
- **An option input carries a source and a moment.** `quoted_at` is timezone-aware or refused (whose local time — the desk's, the broker's, or the runner's?), and must fall on `quoted_on`. An imported ladder is refused when a row has neither premium nor vol, when it has both, when nothing anywhere carries a timestamp, and when the rows carry **two** timestamps — one chain is one moment, and a Greeks table struck across two sessions is not a Greeks table. Every imported row is `PriceType.MANUAL`: a file somebody sent us is not a feed. `BLACK76_LIMITATIONS` states the model's limits as **data with a direction** — a caveat that does not say which way it bites cannot be acted on. The American early-exercise one is `understates`: the Black-76 number is a *floor* for an American option, not a value for one.

Surfaces: workstation sections 07–10 and 13 (private), 12 (public — the chain's absence, the model, its limits); `scripts/import_positions.py` (dry-run default, exit 1 on any rejection so a nightly import can gate on it); `scripts/worked_book_example.py`, the end-to-end worked synthetic position. Docs: `data/reference/{positions,options,clearing,import_profiles}/README.md`. Client records are **files, never tables** — every table here round-trips through the committed `data/history/*.csv`, so a positions table would publish the book by construction — and all four directories are gitignored but for their READMEs.

## Screen (`analysis/opportunities/` + `app/opportunities_page.py`, Phase 4)

The third consumer layer. Where the briefing says *what moved* and the
workstation says *how to hedge it*, this one asks a commercial question: who
might buy or sell what, where, in which window, why now, how strong is the
evidence, and what should the desk do next. It **originates no data** — every
detector reads tables other layers already fill, and the counterparties come
from `data/reference/players/` as researched, never invented.

```
analysis/opportunities/domain.py   vocabulary — Ladder, Opportunity, Blocker,
                                   Evidence, ScoreCard, the audience split
                                   (stdlib only, no SQL)
              ▲
              │
analysis/opportunities/signals.py  the only SQL-aware module here; six
                                   detectors over landed cost, destination
                                   flows, commitments, stocks-to-use, crush
                                   and FX → Detection + per-detector coverage
              ▲
        rules.py  (blockers, counterparties, ladder rung, next action)
              ▲
       scoring.py  (five components, declared weights, rank)
              ▲
   ┌──────────┴───────────┬────────────────┬──────────────────┐
registry.py          workflow.py       sensitivity.py         │
(identity, dupes,   (the private       (threshold headroom,   │
 expiry, archive)    desk file)         score swings,         │
                                        landed scenarios)     │
   └──────────┬───────────┴────────────────┴──────────────────┘
              ▼
          engine.py  →  EngineResult(public, private, expired, coverage, …)
              ▼
   app/opportunities_page.py  ──▶ docs/opportunities.html          (public)
                              └─▶ data/workspace/opportunities.html (private,
                                                              gitignored)
```

The six detectors each reuse an existing analysis module rather than
recomputing it: landed advantage (`analysis.origins.comparison`), destination
flow shift (`inspection_destinations`), commitment shift
(`export_sales.outstanding_sales`), buyer-region tight stocks
(`analysis.stocks_to_use`), crush margin (`analysis.origins.crush`), and an
origin-competitiveness FX move (`currencies`). Each is isolated: one detector
raising is reported in `coverage`, never a blank page.

Six rules are load-bearing, each enforced by a type or a test:

- **A price difference is not an opportunity.** `rules.py` attaches policy, freight, quality, window, liquidity, staleness, ingest-outage and no-counterparty blockers. A **hard** blocker sets `feasibility = 0` and caps the rung below `actionable`; `rank()` sorts by rung *before* composite, so the India mandi row — a real +284 USD/MT over CBOT, and uncloseable behind the GM import ban — can never head a board titled "what to work today". Its `policy_blocked` caveat is reused verbatim from the market registry's own `basis` descriptor, not restated here.
- **Unknown stays unknown.** `Volume` requires a stated `basis`; a total with no volume raises. A missing ocean freight is a *hard* blocker (there is no landed number without it); a missing quality adjustment is soft. Absence never becomes an assumption.
- **Five components, shown separately.** Economic, evidence, freshness, counterparty and feasibility are each 0–100 with a note a reader can reproduce from the numbers printed beside it; the composite is only a sort key. `evidence` reads the *evidence's* confidence, not the row's — the row's is already dragged down by `inferred` counterparty research, which has its own component, and scoring it twice pinned this component at 40 for nearly every row. Freshness is judged per item against its own layer's `LAYER_MAX_DATA_AGE_DAYS` — the number `main.py` grades on — so a weekly source is not punished for being four days old.
- **The privacy boundary is structural.** Desk status, owner, contact dates, notes, feedback and audit live only on `Opportunity.workflow`, loaded from gitignored YAML under `data/reference/opportunities/`. Four independent guards: the public serialiser never builds the `workflow` key, `EngineResult.public` excludes any opportunity that has one, `save_opportunity_detections` raises on those column names, and the private edition is written to `data/workspace/` — outside `docs/` and deliberately absent from `trust.site_promotion.expected_site_paths()`, so it can never be uploaded to Pages. A *present but malformed* desk file raises rather than rendering as an empty book, on the same terms `data/reference/positions/` uses.
- **Identity excludes every number.** `identity_key` is `(rule_id, product, origin, destination, window_start)` — no price, no edge, no score — so today's re-detection of yesterday's lane is the *same* opportunity with its original id and first-seen date, not a new one. Ids survive an ephemeral CI database because `opportunity_detections` archives the public projection and round-trips through `data/history/`. Expiry is the signal's own validity plus `OPPORTUNITY_EXPIRY_GRACE_DAYS`; a lapsed row is re-stamped `expired` and demoted, and past the grace it is listed from the archive as a row, never re-rendered with stale numbers.
- **Nothing here learns.** Feedback (`dismissed`, `false_signal`, `no_interest`, `progressed`, `won`, `lost`) is counted and reported; it never re-weights a rule. Retuning on five dismissals would be a model nobody trained, evaluated or can turn off.

The ladder is stated on the page and is the reason the board is honest about what it is: **market signal** (something moved) → **lead** (a lane, but something is missing or blocked) → **actionable** (workable today) → **proposed trade** → **completed business**. Only the first three are ever detected; the last two require a human and are therefore private by construction. There is **no routing and no contact channel** — the output is a next action for a person to take.

## Landed-cost onboarding (`analysis/origins/`, Phase 6)

The origin page is fail-closed by design: with nothing entered, every landed total blocks and says so. Three modules make that a workflow rather than a wall, and none relaxes the blocking rule.

- `validation.py` — the faults **one entry cannot see about itself**. An ambiguous pair (same component, same scope, overlapping windows *and* overlapping lifetimes) is two answers to one question; a freight with no `origin` prices all three legs off one indication; `us-gulf` for `us_gulf` matches no route and reads on the page as "never entered". Those are `Severity.ERROR` and `load_assumptions()` **raises** on them, because at lookup time the complaint surfaces mid-page-build for whichever route asked first. Expiry is `Severity.WARNING` and is reported, never raised: the lapsed record is the audit trail, and deleting it to quiet a loader would destroy it. A renewal chain (old entry's `expires_on` shortened to before the new one's `entered_at`) is explicitly legal; an overlapping renewal is not.
- `readiness.py` — **database-free** route onboarding. What a route requires is the incoterm bridge plus `config.LANDED_STACK`, both derived, so US Gulf asks for elevation and an ex-works leg would ask for inland haulage with no edit here. Status per input (`satisfied` / `expiring` / `expired` / `missing`) is resolved through the same `AssumptionSet.lookup` the calculation uses, so the checklist and the page cannot disagree. Every command carries a `<VALUE>` placeholder — a suggested default is a fabricated default with an extra step. `expiry_review` answers "what lapses, who owns it, which routes go dark", and resolves the last part by re-running readiness with the entry removed rather than by matching scope: an entry shadowed by a more specific one takes nothing down with it.
- `scenarios.py` — `input_flip_moves` generalises the freight break-even to **every** input. Solved, not searched: each rung is linear in its own value, so `marginal_landed_per_unit` differentiates the row's own waterfall (a flat dollar compounds through the ad-valorem rungs after it; a duty point is worth the CIF base it is charged on; a financing point recovers its carry period as `amount / rate`, and reports `None` at a zero rate rather than inventing one). Rows sort by the move **as a percentage of the input's own value**, because a dollar of freight and a point of duty are not comparable as written. An input **both** origins share moves both totals together and mostly cannot flip anything — said in words, never as a large number that reads as "possible".

Surfaces: page sections **02 Route readiness** and **09 Renewals due**, plus the flip table in **05 Sensitivity**; `scripts/enter_assumption.py --onboarding`, `--review` and `--check` (exit 1 on file faults, 0 on world faults). What must be entered per route is documented in `data/reference/assumptions/ONBOARDING.md`.

The shipped directory contains **no invented number** — only the two China policy rates. The success path is rendered in tests from `tests/fixtures/assumptions_complete/`, reached through `MIRROR_ASSUMPTIONS_DIR` and never set in CI. One rendering trap is pinned by test: the site renders with `autoescape=False`, so `<VALUE>` must be escaped explicitly or a browser eats the placeholder as a tag. A second is pinned by name: a template key called `clear` resolves to `dict.clear` — a truthy bound method — so the renewals verdict is keyed `nothing_due`.

## The crush (`analysis/futures/crush.py`)

**One crush calculation, four surfaces, and it names its contracts.** Until Phase 6 every "board crush" here was three *provider front-month* series — Yahoo's `ZS=F`/`ZM=F`/`ZL=F` out of `prices`, which carries no contract column at all. That number named no delivery month, so it could not be reproduced; Yahoo rolled each leg on its own unannounced schedule, so a roll-day print moved for reasons nobody earned and the artifact did *not* cancel across a spread; and a crusher acting on it would have had no month to place the three orders in. `named_board_crush(provider, as_of=...)` replaces it, reading the `forward_curve` layer through the `QuoteProvider` seam and returning either a `NamedCrush` or a `CrushWithheld` carrying the reason.

Five things are load-bearing:

- **`CrushLevel` is one closed vocabulary of four**, imported by `analysis.origins.crush` rather than redefined: `board_reference` (named contracts, delayed closes), `board_settlement` (proven settlements — *not constructible today*, because `PROVEN_SETTLEMENT_SOURCES` is empty), `gross_physical`, `net_plant`. The two board levels are computed here, the two physical ones in `analysis/origins/crush.py`.
- **The month convention is derived, documented and rendered.** ZS lists Jan/Mar/May/Jul/Aug/Sep/Nov; ZM and ZL list Jan/Mar/May/Jul/Aug/Sep/Oct/Dec. Six bean months pair with the products' own month; **November beans crush into December**, the first listed product month after it. `SOY_CRUSH_PRODUCT_MONTH` is derived from the two listed-month sets and pinned against the literal table by test. `propose_crush_hedge` uses the same mapping, so the hedge's product legs *follow* its bean month instead of each choosing its own nearest.
- **Every leg carries its defence**: symbol, delivery month, observation date, price type, provider, and `settlement_proven`. `NamedCrush.workings()` prints the lines that reproduce the margin by hand, and they are on the page.
- **Coherence is checked and withheld, never patched.** `no_curve`, `no_crush_month` (nothing listed with ≥5 sessions left), `mixed_sessions`, `mixed_price_types`, `mixed_providers` (a trusted bean and a v1 oil are two provenances in one number), `unsupported_price_type`, `settlement_unproven` (a settlement is a claim about the *provider*), and `expiry_not_encoded`. A withheld crush is a different type from a computed one, so it cannot be read as a margin.
- **`ContractBasis` states what the legs structurally are** and is registry data, not a code path: `MARKETS[...]["crush"]["contracts"]` is `named` (CBOT), `continuous` (Dalian's akshare main-contract series — arithmetically fine, structurally unhedgeable, and the block says so) or `administered` (Argentina). A descriptor that omits it fails the build. `ContinuousSeries` has no path to a crush at all, and `continuous_withheld()` is the answer a surface gets when that is all it holds.

Consumers: `app/block_builders.crush_block` (block 03), `app/origins_page` section 06, `app/workstation_page` section 04, `analysis/opportunities/signals.crush_margin_detections`, `analysis/briefing/sections/crush` and the briefings archive's `crush_spread` block. `tests/test_named_crush.py` pins the convention and every refusal, including the roll-period, missing-leg, mixed-date and mixed-price-type boundaries.

## Price semantics (`pricing/semantics.py`)

**One classification of what a stored number is, shared by every surface.** `PriceType` has six members — `settlement`, `delayed_close`, `last_trade`, `assessment`, `administered`, `manual` — and it is the *same object* wherever it appears: `analysis/futures/domain.py`, `analysis/origins/domain.py`, `app/markets.py` and `trust` all import it rather than defining their own. Four parallel vocabularies is how the stack came to call one yfinance daily bar a `DELAYED_CLOSE` on the workstation, "three exchange settlements" on Origins, and `Confidence.EXECUTABLE` (scored 100/100, ranked on) on Opportunities.

Three rules are load-bearing:

- **A settlement is a claim about the provider, not the number.** `PROVEN_SETTLEMENT_SOURCES` is empty and that emptiness is the finding: no layer here ingests an authoritative settlement feed, so nothing may be rendered as one. Adding a name to that frozenset is the single edit that turns a delayed close into a settlement across the whole site.
- **Confidence is derived from the price type, never asserted beside it.** `CONFIDENCE_CEILING` grants `EXECUTABLE` only to a settlement-proven type; `CONFIDENCE_BY_QUOTE_KIND` is built from it rather than restated. A CBOT/DCE board leg is `BOARD_REFERENCE` — above every assessment because it is the venue's own daily print, below `EXECUTABLE` because no provider proves it. The board crush and the board basis are kept in full; only the claim about them changed.
- **The chip names the animal.** `quote_kind_label` renders `board · delayed close`, not `board`, everywhere a quote kind appears (block headers, block 04, both ledgers). "Board" alone reads as the exchange's own settlement.

`tests/test_price_semantics.py` pins the vocabulary and the derivation; `tests/test_price_semantics_rendering.py` renders Origins, Opportunities, every market block and the Workstation through their real builders and fails on an unnegated "exchange settlement" claim or on the word "executable" reaching a page.

## Semantic contract (`pricing/policy.py`)

**`semantics.py` says what a number is; `policy.py` says what may then be said and done with it.** One module, imported by `analysis`, `app`, `trust` and the tests, because a list of forbidden words living in a test file protects only the surfaces that test happens to render. The failure it guards is not a template typo — it is a *future* feature reusing an existing number correctly and describing it wrongly: an assessment as a firm offer, an administered minimum as a traded market price, a delayed consumer-endpoint bar as the official close. Each of those parses, prices and looks right.

`ClaimKind` is five members — `settlement`, `official_close`, `executable`, `firm_offer`, `traded_price` — and `CLAIM_SUPPORTED_BY` is the whole policy: which `PriceType` can support each. `EXECUTABLE` needs a proven `SETTLEMENT` (and `PROVEN_SETTLEMENT_SOURCES` is empty, so nothing ingested reaches it); `FIRM_OFFER` is supported by **nothing**, because no layer here carries a counterparty quote; `TRADED_PRICE` is the discriminating case — a board close and a last trade came out of a trade, an assessment and an administered minimum did not.

Enforcement is in two places because the claims fail in two ways.

- **Language.** `scan()` reads the tag-stripped text of a page (script and style payloads dropped — a Plotly blob is not prose) against `FORBIDDEN_CLAIMS`, and a claim **negated in its own sentence is not a claim**: "delayed daily closes, not proven exchange settlements" has to stay sayable, and a check that banned the word would delete the sentence that tells the truth. Two narrownesses are deliberate: "last-traded price" is excluded (SAFEX publishes one, and a pattern that fired on the honest label would be switched off within a week), and `price_types=` lets the *one* private surface carrying an attested clearing statement say "settlement" while no public page can.
- **Structure.** Some claims are made by arithmetic rather than by words. `require_hedgeable` is **opt-in**: `NamedContract`/`ContractQuote` declare `is_hedgeable = True` and everything else — a `ContinuousSeries`, a bare price, the next research artifact nobody has written yet — is refused, at `size_leg`, `build_hedge`, `build_ticket` and `named_board_crush`. `require_traded_price` refuses to size a futures leg off an administered or assessed number, because doing so asserts it is a market price whatever the caption says. `assert_confidence_supported` runs in `Evidence.__post_init__`, so an over-claimed row cannot be constructed, let alone ranked on — judged on `AUTHORITY_TIER`, where `indicative` and `administered` are peers (they differ in *kind*, not strength; `CONFIDENCE_RANK`'s worst-wins ordering is a display concern and is not reused here). A quote kind that is not a price at all (`NON_PRICE_QUOTE_KINDS` — a tonnage, a ratio) has no ceiling to exceed, while an *unknown* kind still raises: "not a price" and "nobody classified this" are different facts.

The same scan runs inside `trust.site_promotion.verify_site_candidate`, so a misleading edition is refused at the promotion gate rather than only in CI — a page that is wrong and looks right is a worse thing to publish than a tombstone.

`tests/test_semantic_contract.py` generates the **whole site** off a seeded database and scans all thirteen public pages, both private workspace editions (`data/workspace/opportunities.html`, `workstation.html`), the private trial dashboard and the briefing text, plus every structural refusal above.

## Trusted ledger — CBOT named contracts (`trust/cbot_benchmarks.py`, DT-16)

The second pilot dataset in the Data Trust Foundation migration (`docs/plans/2026-08-10-data-trust-foundation.md`) and the first **critical** one. MAGyP (`trust/magyp_fob.py`) proved artifact capture and structured parsing against an official physical source; this proves the four things a *board* price needs, and it is the only trusted adapter that actually runs the quality engine (MAGyP accepts every candidate unconditionally).

Three registry datasets, one per soy leg: `cbot-soybean-named-contracts`, `cbot-soybean-meal-named-contracts`, `cbot-soybean-oil-named-contracts`.

- **A named contract, not a front month.** Every observation identifies exchange, contract code and delivery month. A symbol that does not resolve to a contract of the dataset's own product is refused, not carried as an anonymous price. The ticker set comes from `fetchers.forward_curve._build_contract_tickers` — the v1 builder itself, not a second copy of the month rules — so a reconciliation difference can only mean "different parse", never "different contracts".
- **The settlement claim is about the session, not the number.** Yahoo publishes no settlement and `price_type` stays `delayed-close`; what `settlement.confirmed` checks is that the bar is a *finished* session. The same defect `fetchers/_settlement.py` prevents at fetch time, restated as a quality rule so a provider substitution cannot lose it.
- **The candle has to be possible.** `ohlc.relationship` rejects a bar whose high is below its open/low/close, or whose low is above them. Such a frame parses cleanly and is simply not a candle.
- **An extreme move quarantines rather than overwrites.** A day-over-day move past `DAILY_MOVE_QUARANTINE_THRESHOLD` (20% — deliberately above CBOT's own *expanded* limits, so a move past it cannot be a legitimate session) is appended as a quarantined revision: durable, auditable, never reachable by an accepted query, and it does **not** displace the accepted history it disagrees with. The same-session re-print is the sharper case and is pinned by test — the previous-value lookup prefers the observation's own accepted revision before falling back to the latest earlier session.
- **Corrections append.** `append_benchmark_correction` writes a superseding accepted revision linked to its predecessor; the superseded value stays queryable and any edition that pinned it is still reproducible.

Two RFC deviations, both deliberate:

- **`EligibilityScope`** (`trust/domain.py`). `public_eligible` was the only gate on every accepted-revision query, so a dataset whose `public-display` right is `unknown` — which Yahoo's is, and which must stay fail-closed — was unreadable and therefore *unreconcilable*. `revision_is_eligible(revision, scope)` adds `INTERNAL`, which uses the rights model's own already-recorded `internal-display: allowed`. No rights decision changed and `PUBLIC` (the default everywhere) is unchanged. **This is not a licensing change** and does not authorise publishing anything.
- **Dataset-scope vs candidate-scope findings.** `DatasetResult` refuses `success` while carrying a quarantine or reject finding, which is right for a stale payload or a coverage shortfall — facts about the whole dataset that dropping a row cannot resolve. A candidate-scope finding *is* resolved by its own disposition: the record it complains about was quarantined or rejected and is not among the accepted revisions the result exposes. So those findings travel on the revision (`finding_ids`), on `BenchmarkDatasetIngestion`, and in the run manifest's `findings_summary`; only dataset-scope findings reach `evaluate_dataset_health`. Passing them all in would not make the result stricter, it would make the evaluator raise instead of returning a verdict.

**The read path is still v1 and switches per dataset.** `trust/read_path.py` reads `MIRROR_TRUSTED_READ_DATASETS`, a comma-separated list of registry dataset keys. Unset, empty, `none` or `off` all mean v1; an unknown key **raises**, because a typo that silently meant "still on v1" is the one failure a cutover switch must not have; there is deliberately no `all`. `analysis/futures/providers.open_provider` returns `SqliteQuoteProvider` unless **all three** soy keys are named — a crush struck from a trusted bean and a v1 oil is two provenances in one number — and falls back, logging, on any ledger problem, because a storage migration must not be able to take the workstation down. With the switch on, `TrustedNamedContractProvider` serves curves from accepted current revisions at `INTERNAL` scope and delegates the still-v1 reads (continuous series, FX, aggregate open interest) unchanged. Moving the bytes into a ledger proves provenance, not authority: `settlement_authoritative` stays `False` and the price type stays `DELAYED_CLOSE`.

**Reconciliation.** `trust/reconciliation.py` holds the shared `reconcile_frames` (deliberately dumb — no tolerance windows, no fuzzy matching); MAGyP now uses it too. `scripts/reconcile_cbot_benchmarks.py` runs daily in the `reconcile-trusted` CI job beside the MAGyP report. It downloads each ticker **once** through a memoising downloader shared by both parsers: the settlement guard means a run straddling the cutoff would otherwise hand one path a session the other never saw, and a divergence that might mean "different download" is worthless as cutover evidence. Exit 0 reconciled or no session published, 1 diverged, 2 upstream unavailable. Quarantined revision ids are *reported, never graded* — v1 has no such state, so a held-back leg is not a divergence, but a cutover should not be enabled on a day the ledger is holding legs back.

Tests: `tests/test_trust_cbot_benchmarks.py` (ingestion, quarantine, rejection, corrections, point-in-time), `tests/test_trust_read_path.py` (the switch), `tests/test_trust_cutover.py` (the provider and the reconciler). All network-free.

## Latency (`latency/`, LATENCY.md)

One vocabulary for "how old is the number on the page, and which part of that age did we cause". `domain.py` is stdlib-only (stage chain, five `LatencyClass`es and their objectives, per-layer `ObservationClock` and declared `provider_delay`), `clock.py` the per-run instrumentation, `measure.py` the single DB-aware seam, `report.py` the rendering.

- **Acquisition** (observation → fetched) contains the provider's delay *and* our cadence wait, and cannot be split by observing ourselves; `provider_delay` is therefore **declared per layer with a stated basis**, never inferred from our own timings, and `cadence_wait = acquisition − provider_delay` is the share our schedule chose. **Pipeline** (fetched → publicly readable) is wholly ours.
- **A missing stamp is `Verdict.UNKNOWN`, never `MEETS`.** `data_freshness` gained `observed_at`, `fetch_started_at`, `fetch_completed_at`, `stored_at`; `save_freshness` looks the run clock up by layer name at the single write choke point, so the eight `_mark_*` paths need no knowledge of it, and a write with no fetch behind it records NULLs rather than `now` — a fabricated fetch stamp would make a slow fetch look instant.
- **Observation is an instant where the venue hour is known and `DAY`-granular where it is not.** No invented hours; a day-granular age is a stated lower bound.
- `observed_at` is stamped on **every** status, not just success — a stale layer's newest observation is what sizes the hole.

Surfaces: the masthead's "Board and FX priced from data N old" (generation time is not observation time, and here they are routinely a day apart), four separate Observed/Fetched/Age/Last Success columns in the Layer Freshness table, and `docs/manifest.json`.

## Fast refresh (`main.py --fast`, `scripts/refresh_prices.py`)

`config.FAST_REFRESH_LAYERS` (`prices`, `currencies`, `forward_curve`) over `FAST_REFRESH_HISTORY_PERIOD` (`1mo`) — the **same code** as the daily build with two arguments different, so the settlement guard, cleaners, `LAYER_MIN_KEYS` floor and freshness grading cannot drift between the paths. Measured 53 s end to end against the daily build's 6 m 02 s; the lever is `DEFAULT_HISTORY_PERIOD`, since a 15-year yfinance pull benchmarks at 24–32 s/ticker against 1–3 s for a short window. Every scraped physical leg is deliberately excluded — they publish once a day, and doubling the request rate on unfriendly upstreams trades reliability for freshness that is not there. DCE is excluded from the default set but has its own 08:00 UTC slot (`--layers dce` on `refresh_prices.py`): Dalian closes 15:00 CST = 07:00 UTC, so the evening-only fetch was 13–17h late against the 6h board objective. Each refresh slot ends with `scripts/latency_report.py --fail-on-breach-layers <its own layers>` so a breach is a red run — scoped, because some layers breach acquisition structurally (COT's 3-day provider delay; `dce` re-stamped late by the evening daily build).

**A fast refresh's failure mode is not a crash — it is a structurally perfect site that knows less than the one it replaces.** It inherits 26 layers from whatever database it sits on, and on an unseeded runner that is only what `data/history/*.csv` carries, so PSD, weather, COT and crop progress would silently vanish behind legal empty states. `trust.site_promotion.verify_refresh_is_not_a_regression` compares the candidate's `manifest.json` against the published edition's: no layer's observation may go backwards or vanish, no page that rendered may tombstone. A missing *candidate* manifest is a refusal (being unable to compare is being unable to pass); a missing *published* one passes loudly (the first build has nothing to compare against). Refusal is a **no-op** — `scripts/refresh_prices.py` exits before the Pages upload and the live edition is untouched. `docs/manifest.json` is a published **asset**, not a page: it is in `trust.site_promotion.PUBLISHED_ASSETS` (link-checked for existence) rather than `expected_site_paths` (crawled and timestamp-checked as HTML).

## Module dependency graph

```
  fetchers/*  (incl. fetchers/agrural)  ───┐
                                            ▼
  pipeline/clean ───▶ pipeline/store ───▶ DB (incl. briefings table)
                                            │
                       pipeline/query ◀─────┘
                            │
                            ▼
              ┌─────── analysis/loaders ────────┐
              │                                  │
              ├──▶ analysis/zscore               │
              ├──▶ analysis/stocks_to_use        │
              ▼                                  ▼
       analysis/briefing/                analysis/soy_analytics
              │                                  │
              └──────────┬───────────────────────┘
                         ▼
             scripts/generate_html (renders dashboard)
```

Direction is one-way: nothing in `fetchers/` imports from `pipeline/` or
`analysis/`; nothing in `pipeline/` imports from `analysis/`. The
analysis layer imports from `pipeline/query` and `pipeline/units` only.

The "trader-grade signals" flow (basis, stocks-to-use, z-score-based
COT/weather thresholds) runs the same fetch → clean → store → analyze →
render path — no new architectural pattern. `analysis/zscore.py` and
`analysis/stocks_to_use.py` are pure-function primitives consumed by the
briefing sections; `fetchers/agrural.py` is one more fetcher feeding the
existing store stage; the `briefings` table is one more row in the
storage layer.

## The "native units, convert at display" rule

The DB stores values in **native exchange units** — cents/bushel for
soybeans, cents/pound for soybean oil, $/short ton for meal, etc. This
is what the fetchers receive and what the analysis math is calibrated
to. Conversion to `USD/MT` happens **at the display layer only** via
`pipeline/units.to_metric_tons()` and `pipeline/units.mt_label()`
(and, for the static site, `Source.to_usd_mt` in `app/block_builders.py`).

Why: round-tripping through MT and back introduces rounding error and
makes downstream math source-of-truth ambiguous. Keeping the storage
unit fixed means every analysis function and every test has one obvious
input space.

Where the rule is enforced:

* Fetchers store native units (`save_price_data` just dumps OHLCV).
* `compute_all_technicals`, `compute_crush_spread`, etc. operate on
  native units.
* The briefing's `prices` section calls `to_metric_tons` only when
  building the display string.
* The dashboard's chart builders convert at the figure-construction
  layer.

If you find yourself converting before storage or before analysis,
that's the bug — back it out and convert at render instead.

## Where to add new things

| You want to add...                       | Edit...                                  |
|------------------------------------------|------------------------------------------|
| A new data source                        | `fetchers/<source>.py` + `pipeline/schema.py` for its table + `pipeline/store.py:save_<source>()` + `pipeline/query.py:read_<source>()` + wire into `main.py` |
| A new analysis primitive                 | `analysis/<name>.py` with a pure function over DataFrames |
| A new briefing section                   | `analysis/briefing/sections/<name>.py` with `format(...)`, then wire into `analysis/briefing/orchestrator.py` |
| A new dashboard page                     | New analyst function in `analysis/soy_analytics.py` + chart in `app/charts.py` + block in `app/templates/dashboard.html.j2` |
| A new market page                        | A `config.MARKETS` entry — no code; the builders are generic over the descriptor |
| A new threshold (RSI level, weather)     | Constant in `config.py` — never inline |
| A new hedgeable product                  | `CONTRACT_SPECS` entry in `analysis/futures/domain.py` (with its expiry rule, or `None` to leave it un-encoded) + its `config.FORWARD_CURVE_CONTRACTS` months |
| A new exposure alert                     | A check function in `analysis/futures/alerts.py` + wire into `build_alerts` |
| A new scheduled release on the calendar  | `EVENT_SOURCES` entry in `analysis/futures/events.py` — only if a layer here ingests it |
| A new opportunity rule                   | Detector in `analysis/opportunities/signals.py` + its `config.OPPORTUNITY_RULES` entry (threshold, validity, question) + wire into `DETECTORS`; blockers/next action in `rules.py` |
| A new blocker reason                     | `BlockerCode` member in `analysis/opportunities/domain.py` (+ `HARD_BLOCKERS` if it stops the trade) + the check that raises it in `rules.py` |
| A hand-entered option quote              | A `*.yml` document in `data/reference/options/` — see its README; never code |
