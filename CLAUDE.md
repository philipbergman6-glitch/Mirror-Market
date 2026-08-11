

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mirror Market is a commodity market intelligence platform focused on the soy complex (Soybeans, Soybean Oil, Soybean Meal) with supporting data for competing crops. It pulls data from 21 source layers (covering 10 commodity futures, 10 currency pairs including ZAR/NGN, 19 weather regions including SA/Nigeria, 28 countries in PSD supply/demand, weekly export sales, forward curves, WASDE monthly forecasts, EIA biofuel/energy, USDA crush/inspections incl. port-area and destination-country flows, CONAB Brazil estimates, domestic spot prices for India/Brazil/South Africa, AgRural Paranaguá FOB, AMS CIF Gulf export bids, and Argentina MAGyP official FOB) into a SQLite database (local or Turso cloud). All prices are displayed in **USD/MT** (metric tons) for international comparability. The analysis engine includes an emerging markets deep dive (South Africa, India, Nigeria). A static HTML dashboard (deployed via GitHub Pages) provides 9 pages of visual analysis.

## Commands

```bash
# Install runtime dependencies (uses a .venv virtual environment with Python 3.10+)
pip install -r requirements.txt
# For development (tests, lint, type-check):
pip install -r requirements-dev.txt

# Run the full data pipeline (fetches, cleans, validates, stores all layers)
python main.py

# Generate the daily market briefing
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"

# Generate the static HTML dashboard
python scripts/generate_html.py
# Open docs/index.html in your browser

# Run a single analysis module standalone
python -m analysis.briefing
```

## Required Environment Variables

- `USDA_API_KEY` — USDA NASS QuickStats API key (Layers 2, 14)
- `FRED_API_KEY` — Federal Reserve Economic Data API key (Layer 3)
- `FAS_API_KEY` — USDA FAS OpenData API key (Layer 10 — export sales)
- `EIA_API_KEY` — Energy Information Administration API key (Layer 13 — biofuel/energy)

Layers 1, 4, 5, 6, 7, 8, 9, 11, 12, 15, 16, 17, 18, 19, 20, 21 work without API keys. Layer 16 uses the published data.gov.in sample key by default; set `DATA_GOV_IN_API_KEY` (optional) for a personal key with higher row limits. **api.data.gov.in blackholes any Python-identifying User-Agent** — it accepts the connection and never answers, so it reads as a timeout, not a 403. `fetchers/mandi.py` sends an explicit project UA; dropping it silently darkens the layer (#155).

### Optional (Cloud Database — dormant)

- `TURSO_DATABASE_URL` — Turso database URL (e.g., `libsql://your-db.turso.io`)
- `TURSO_AUTH_TOKEN` — Turso authentication token

**Decision 2026-07-30: no cloud DB.** CI persistence uses git-committed CSVs instead (see "Git-based history persistence" below). The Turso code path in `pipeline/connection.py` remains as dormant optional code for local use — it requires `pip install libsql` (deliberately not in `requirements.txt`) plus both env vars. Nothing in CI sets them.

## Architecture

The project follows a three-stage pipeline: **Fetch -> Clean/Validate -> Store**, with an analysis layer on top.

### Data Pipeline (20 Layers + sub-layers)

`main.py` orchestrates the pipeline. Layers 1–13 are driven by a table of `DictLayer` entries run through a shared `_run_dict_layer()` (fetch → clean → save → `_finalize_layer`, which applies the `LAYER_MIN_KEYS` partial-outage floor); Layers 17–20 share a `_run_scraper_layer()` for `FetchResult` sources; Layers 14–16 keep custom blocks. Each layer is independent and wrapped in try/except — if one fails, the rest still run (graceful degradation). After each successful layer, a freshness timestamp is recorded.

1. **Commodity prices** — `fetchers/yfinance.py` (10 futures: soy complex, palm oil CME `CPO=F` (settlement-marked, zero volume by design), corn, wheat, sugar, cotton, cattle, hogs)
2. **USDA crop data** — `fetchers/usda.py` (production, yield, area harvested)
   - **2b. Crop progress/condition** — weekly USDA ratings (% good/excellent, % planted/harvested)
3. **FRED economic data** — `fetchers/fred.py` (dollar index, CPI, Fed funds, Treasury 2Y/10Y/30Y, Ethanol PPI, Soybean Oil PPI, Diesel Price)
4. **COT positioning** — `fetchers/cot.py` (10 commodities including corn, wheat, sugar, cotton, cattle, hogs, ICE canola)
5. **Weather** — `fetchers/weather.py` (19 regions: US, Brazil, Argentina, Paraguay, Ivory Coast, Indonesia, Malaysia, India, Thailand, China, South Africa, Nigeria)
6. **PSD global supply/demand** — `fetchers/psd.py` (10 commodities x 28 countries incl. rapeseed complex + Canada, oilseeds + grains + cotton)
7. **Currencies** — `fetchers/yfinance.py` (10 pairs: BRL, ARS, PYG, CNY, IDR, MYR, INR, THB, ZAR, NGN)
8. **World Bank monthly prices** — `fetchers/worldbank.py` (Palm Oil, Rapeseed Oil, Sunflower Oil, etc. — current xlsx link resolved from the CMO landing page each run; the GUID deep link rotates yearly and stale links serve frozen data with HTTP 200)
9. **Chinese futures** — `fetchers/akshare.py` (8 contracts: 6 DCE incl. Corn + CZCE Rapeseed Oil/Meal — the only free daily rapeseed benchmark). Two bean contracts, not interchangeable: **No.1 (`A0`)** is the domestic non-GMO food bean (tofu/soymilk, ~700–1,100 CNY/MT premium, never crushed) and **No.2 (`B0`)** is the imported/GMO crush bean. The board crush and the vs-CBOT import-parity premium both key off No.2; No.1 is carried as a standalone China food-demand level with no CBOT counterpart (#152).
10. **Export sales** — `fetchers/export_sales.py` (weekly USDA FAS demand data — requires `FAS_API_KEY`)
11. **Forward curves** — `fetchers/forward_curve.py` (individual contract months via yfinance — contango/backwardation)
12. **WASDE monthly estimates** — `fetchers/wasde.py` (USDA OCE monthly XLS — `wasdeMMYY.xls`, no API key required)
13. **EIA biofuel/energy** — `fetchers/eia.py` (ethanol production, biodiesel production, diesel prices — requires `EIA_API_KEY`)
14. **USDA crush + inspections** — `fetchers/usda.py` (monthly soybean crush volumes + weekly AMS export inspections, incl. the WA_GR101 Table C port-area breakdown → `inspection_port_flows`)
15. **CONAB Brazil estimates** — `fetchers/conab.py` (Brazil's official crop agency — production, area, yield; aggregates 27 UFs to national totals for Soybeans, Corn, Wheat, Cotton lint; coffee is in a separate CONAB file and not tracked here)
   - **15b. CONAB weekly farmgate prices** — `fetchers/conab_precos.py` (`PrecosSemanalUF.txt` — Paraná soybean producer price, R$/kg → BRL/MT). Cross-check for the CEPEA Paraná wholesale indicator (a ~10–14% wholesale-over-farmgate spread is the expected band). Own commodity key in `brazil_spot_prices` — never spliced into the CEPEA series.
16. **India domestic soy prices** — `fetchers/mandi.py` (data.gov.in Mandi Price API — official Agmarknet feed; per-state median `modal_price` series for Madhya Pradesh (Indore hub — headline benchmark) and Maharashtra (#1 producing state since 2025-26) → INR/MT, bean-only; never pooled across states). Rebuilt 2026-08 after NCDEX became unusable (SEBI derivatives suspension to ≥2027-03-31 + fingerprint wall on the spot pages; `fetchers/india_domestic.py` kept on disk as a dormant fallback). Uses the published sample key by default; set `DATA_GOV_IN_API_KEY` for a personal key. No meal/oil legs, so the old India crush margin is retired — the cross-market line is India bean vs CBOT bean premium (USD/MT).
17. **Brazil domestic soy spot** — `fetchers/noticias_agricolas.py` (CEPEA/ESALQ Paraná + ESALQ/B3 Paranaguá indicators republished server-rendered by Notícias Agrícolas — BRL/MT, no API key). Re-enabled 2026-07-30; cepea.org.br itself is still Cloudflare-Turnstile-walled and `fetchers/cepea.py` stays on disk only as a fallback. Historical gap backfill: `scripts/backfill_cepea_gap.py` (one session per `/YYYY-MM-DD` archive page).
18. **South Africa domestic soy** — `fetchers/safex.py` (JSE SAFEX settlement — ZAR/MT, no API key)
19. **AgRural Paranaguá FOB** — `fetchers/agrural.py` (Brazil port-side soy FOB scraper — BRL/MT, no API key)
20. **US Gulf export bids** — `fetchers/gulf_bids.py` (AMS report 3147 "Louisiana and Texas Export Bids" daily PDF — CIF NOLA-barge soybean/corn/wheat bids, basis in cents/bu over the named CBOT contract; no API key)
21. **Argentina official FOB** — `fetchers/magyp_fob.py` (MAGyP "Precios FOB Oficiales" JSON web service — daily official minimum FOB export values in USD/MT for soybean beans/oil/meal, bulk NCM positions, with shipment-window forward curve; no API key. Position→product mapping cross-verified against the labelled datos.gob.ar series — see `MAGYP_FOB_POSITIONS` in config.py. Feeds the cross-origin FOB board with Layers 19/20.)

### Pipeline Layer

- `pipeline/clean.py` — Normalizes raw data (forward-fill gaps, datetime indices, drop NaN rows). Runs sanity checks (warns on >10% daily moves, zero/negative volume). Contains `_check_nan_gaps()` helper used by `clean_ohlcv()` and `clean_dce_futures()`. Also has `clean_india_domestic()`, `clean_brazil_spot()`, `clean_safex()`.
- `pipeline/schema.py` — All 24 `CREATE TABLE IF NOT EXISTS` SQL definitions. No functions — just the table blueprints used by `store.py`.
- `pipeline/store.py` — All `save_*()` write functions. INSERT OR REPLACE upserts, transaction safety, freshness tracking. Uses `get_connection()` from `connection.py`.
- `pipeline/query.py` — All `read_*()` query functions. Returns DataFrames; used by the analysis layer and dashboard.
- `pipeline/connection.py` — Database connection abstraction. Returns Turso cloud connection when `TURSO_DATABASE_URL` is set, local SQLite otherwise.
- `pipeline/units.py` — Metric ton conversion utilities. Converts native exchange units (cents/bu, cents/lb, $/short ton) to USD/MT at the display layer.

### Analysis Layer

- `analysis/technical.py` — SMA (20/50/200), RSI (Wilder smoothing), MACD (12/26/9), Bollinger Bands, historical volatility, price changes
- `analysis/signals.py` — 20/50 and 50/200 MA crossovers, volume spikes, RSI extremes/divergence, MACD crossovers, Bollinger squeeze
- `analysis/spreads.py` — Soybean crush spread (Oil*11 + Meal*2.2 - Beans)
- `analysis/correlations.py` — Cross-commodity matrix, commodity-vs-currency, rolling correlation
- `analysis/seasonal.py` — Monthly seasonal averages, current vs historical norm
- `analysis/forward_curve.py` — Forward curve analysis: contango/backwardation, curve slope, calendar spreads
- `analysis/loaders.py` — Shared, cached price and currency loaders. Used by both `analysis/briefing/` and `analysis/soy_analytics.py` so the two consumers don't drift. `clear_loader_cache()` resets between pipeline runs.
- `analysis/stocks_to_use.py` — Stocks-to-use ratios from PSD; tight-supply alerts.
- `analysis/zscore.py` — Shared z-score helper used by COT and weather sections.
- `analysis/briefing/` — Daily briefing package. Each section of the briefing lives in its own module under `analysis/briefing/sections/` (prices, crush, economic, usda, crop_progress, wasde, export_sales, inspections, gulf_basis, dce, forward_curve, eia, conab, currencies, cot, weather, psd, worldbank, emerging_markets, basis, stocks_to_use, correlations, seasonal, market_drivers, signals, freshness). `analysis/briefing/orchestrator.py` joins them; `analysis/briefing/types.py` defines the typed `BriefingData` returned by `generate_briefing_data()`. `generate_briefing()` is a thin wrapper that returns `BriefingData.text`.
- `analysis/briefing/snapshot.py` — Distills `BriefingData` into structured `snapshot_json` for the briefings archive (schema v2, marked by a top-level `schema_version`; rows without it are v1). Captures every quantitative section output: technicals, crush, Brazil basis, FRED + yield curve, USDA YoY, crop progress, WASDE revisions, stocks-to-use, export sales (incl. China share), inspections, DCE, forward curve (incl. slope), EIA, CONAB legs (no derived gap — units unreconciled), currencies (session-based `chg_5d_pct`/`chg_21d_pct`), COT + 3y z-scores, weather + 90d z-scores, PSD highlights, World Bank, emerging markets (verbatim), correlations, seasonal, and data health. Stores raw numbers and components, never display labels; every block degrades to None/{} on failure.
- `analysis/soy_analytics.py` — 9 analyst functions for the soy dashboard: command_center, supply, demand, technicals, relative_value, risk, seasonal, forward_curve, emerging_markets. Pulls price/currency dicts from `analysis/loaders.py`.
- `analysis/health.py` — Per-commodity data health checks (stale data, flat prices, missing commodities)

### Storage

- Database: `data/storage/mirror_market.db` (SQLite, gitignored)
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `wasde`, `inspections`, `inspection_port_flows`, `inspection_destinations`, `gulf_bids`, `argentina_fob`, `eia_energy`, `brazil_estimates`, `data_freshness`, `commodity_freshness`, `india_domestic_prices`, `brazil_spot_prices`, `safex_prices`, `briefings`
- `forward_curve` keys on `(commodity, contract_month, fetched_date)` — one full curve per run accumulates term-structure history; `read_forward_curve()` returns only each commodity's latest snapshot.
- All config lives in `config.py` (tickers, API URLs, region coordinates, thresholds)

### Git-based history persistence (`pipeline/history.py`)

CI runs on an ephemeral runner with an empty DB each day. Most layers self-heal by re-downloading full history, but snapshot-only sources don't: AgRural (1 row/day — the Brazil basis source), SAFEX, forward curve, CONAB survey revisions, inspections (>3 weeks) incl. port/destination breakdowns, Gulf bids, Argentina FOB (MAGyP serves history but re-fetch depth is unproven), CEPEA (>~10 sessions), WASDE (>12 months), India mandi (current-day snapshot), **export sales** (ESR is fetched for the current marketing year only, so the outgoing year vanishes at each MY rollover), and **briefings** (generated from that run's DB; a past day's `text`/`snapshot_json` is reconstructible from no source at all). These tables round-trip through CSVs in `data/history/` (committed to git): `main.py` calls `import_history()` after `init_database()` (INSERT OR IGNORE — DB rows win over CSVs) and `export_history()` after the layers (atomic per-table writes, PK-sorted for stable diffs). The deploy workflow commits `data/history/` back to `main` with `[skip ci]`. A failed import aborts the run so a bad seed can never be exported over good history. Cloud DB (Turso/Supabase) was explicitly rejected for this — do not reintroduce it as a CI requirement.

`export_history()` refuses three kinds of regression rather than committing them: an **empty** table (fetch layer failed), a **shrinking** row count (DB behind the committed CSV — e.g. a local run that skipped `import_history()`), and a **dropped column** (`CREATE TABLE IF NOT EXISTS` never adds columns, so a DB predating a schema change exports narrower rows at an unchanged row count). Each logs an error and leaves the CSV untouched. `MIRROR_HISTORY_ALLOW_SHRINK=1` overrides the latter two when a prune or column removal is genuinely intended.

### Briefing Sections (in order)

1. Data Freshness Warnings
2. Prices (10 commodities with MA, RSI, MACD, volatility)
3. Crush Spread
4. Brazil Basis (Paranaguá FOB vs CBOT, USD/MT — Layer 19 × Layer 1)
4b. US Gulf Basis (CIF NOLA barge — AMS export bids, Layer 20)
4c. Cross-Origin FOB Board (US Gulf CIF vs Brazil Paranaguá FOB vs Argentina up-river FOB, USD/MT — Layers 19 × 20 × 21)
5. Economic Context (FRED — dollar index, CPI, rates, ethanol PPI)
6. USDA Fundamentals (YoY production/yield)
7. Crop Conditions (weekly USDA % good/excellent, progress)
8. Yield Curve (2Y/10Y spread with recession signal)
9. WASDE Estimates (monthly USDA supply/demand forecasts with MoM revisions)
10. Stocks-to-Use (US balance-sheet tightness from PSD; tight-supply alerts)
11. Export Sales (weekly USDA FAS demand data, top buyers)
12. Export Inspections (actual shipments vs committed sales)
13. DCE Chinese Futures (vs CBOT comparison)
14. Forward Curve (contango/backwardation per commodity)
15. Biofuel & Energy (EIA — ethanol, biodiesel production, diesel prices)
16. Brazil Crop Estimates (CONAB vs USDA comparison)
17. Currencies (10 pairs with trade impact)
18. COT Positioning (10 commodities)
19. Weather Alerts (19 regions)
20. Global Supply — PSD (28 countries)
21. World Bank Prices
22. Emerging Markets (South Africa SAFEX + Brazil CEPEA/CONAB farmgate + India mandi bean vs CBOT + Nigeria deep dive)
23. Correlations (cross-commodity + commodity-vs-currency)
24. Seasonal Analysis
25. Market Drivers (BRL + exports, COT + RSI crowding, weather + price premium, dollar impact, corn/soy acreage competition, livestock demand, export sales pace, forward curve structure, palm oil vs soy oil, CZCE rapeseed oil vs soy oil, biofuel pull, CONAB vs USDA divergence)
26. Signals (sorted by severity)

## Key Patterns

- All fetchers return `dict[str, pd.DataFrame]` (keyed by commodity/region name)
- All cleaners return copies — originals are never mutated
- Database uses `INSERT OR REPLACE` so the pipeline is safe to re-run
- Analysis functions expect DataFrames with a `Close` column and DatetimeIndex
- Logging throughout — configured once in `config.setup_logging()`
- Configurable thresholds in `config.py` (RSI, volume spike, weather, freshness)
- Signals have severity levels: `alert` > `warning` > `info`

## Static HTML Dashboard

The dashboard is a static HTML page deployed to GitHub Pages.

Key files:
- `app/charts.py` — Shared Plotly figure builders
- `app/templates/dashboard.html.j2` — Jinja2 template with CSS from DESIGN.md
- `scripts/generate_html.py` — Generation script: calls analysts → builds charts → renders template → writes `docs/index.html`
- `.github/workflows/deploy-dashboard.yml` — GitHub Actions: daily pipeline run + HTML generation + Pages deploy

```bash
# Generate the static dashboard locally
python scripts/generate_html.py
# Output: docs/index.html
```

## Design System
Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.

## Known Limitations

### Settlement guard — no unsettled bars (Layers 1, 7, 11)

yfinance emits a row for the session **in progress**, so a run landing mid-session used to store an unfinished bar as the day's close (observed: ZS=F 2026-08-07 stored at 1181.25 against a 1156.50 settlement — 2.1% wrong, well under `pipeline/clean.py`'s >10% warning). It self-healed on the next run, but the dashboard published on day D carried day D's partial print.

`fetchers/_settlement.py` drops the current session's row until the venue has settled, applied at `fetchers.yfinance.fetch_one` — the single choke point for every yfinance frame (Layer 1 prices, Layer 7 currencies, Layer 11 forward-curve contracts, which took `Close.iloc[-1]` off the same partial bar). The cutoff is `SETTLEMENT_CUTOFF_LOCAL = (14, 30)` in `SETTLEMENT_TIMEZONE = "America/Chicago"` — one time clearing CBOT 13:15 CT, CME livestock/palm 13:05 CT, ICE cotton 13:20 CT and sugar 12:00 CT, expressed in venue-local time so US DST is handled by zoneinfo. A dropped bar logs a WARNING; the missing day is visible, and today's close lands on the next run.

Consequence: a run landing before the cutoff publishes a dashboard whose newest price row is D−1. That is the intended trade — a gap over a wrong number.

The daily schedule (`.github/workflows/deploy-dashboard.yml`) targets a landing window of ~20:00–24:00 UTC (cron `0 19`, plus GitHub's observed +64 to +298 min scheduler delay), which is after CBOT settlement year-round and picks up Argentina MAGyP and AMS Gulf bids same-day. Brazil CEPEA publishes after 21:01 UTC and is caught on later landings only. Correctness does not depend on the cron — the guard does.

### Front-month roll-day discontinuities (Layer 1)

`fetchers/yfinance.py` pulls front-month commodity tickers (`ZS=F`, `ZL=F`, etc.). yfinance silently switches the underlying contract as expirations approach, which introduces artificial price gaps on roll days. These gaps do not represent economic moves.

**Affected analyses:**
- `analysis/technical.py` — SMA crossovers, RSI (Wilder), MACD (EMA-based), Bollinger Bands, historical volatility. All compute on `Close` and propagate the discontinuity.
- `analysis/signals.py` — MA/MACD crossovers, RSI extremes, RSI divergence, Bollinger squeeze. Mitigated: signals within ±3 business days of an estimated roll date (first business day on/after the 15th of an active delivery month — CME expiry; validated against 15y of ZS=F gaps — see `analysis.signals.is_near_roll`) are demoted to `info` severity and tagged `(near-roll)` in both the briefing and the dashboard.

**Not affected:**
- `analysis/spreads.py` — crush spread, oil/meal ratio, bean/corn ratio. Computed on raw active-contract closes; the artifact appears on each leg simultaneously and largely cancels.
- `analysis/soy_analytics.py` emerging-markets basis — CEPEA/SAFEX/India crush vs CBOT compare raw `Close` values, level-sensitive but consistent.
- Layers 2–19 (USDA fundamentals, FRED, weather, PSD, currencies, forward curve, COT, WASDE, EIA, CONAB, domestic spot prices, AgRural FOB). None depend on a continuous front-month price.

**Future work (deferred):** a Panama-adjusted `adj_close` column on the `prices` table with the dual-column read pattern (technicals use adjusted, spreads/basis stay on raw). See plan `~/.claude/plans/ontinuous-contract-roll-for-glowing-kernighan.md` for the Phase 2 spike gate and Phase 3 scope.
