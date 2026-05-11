

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mirror Market is a commodity market intelligence platform focused on the soy complex (Soybeans, Soybean Oil, Soybean Meal) with supporting data for competing crops. It pulls data from 15 source layers (covering 11 commodity futures, 13 currency pairs including ZAR/NGN, 24 weather regions including SA/Nigeria, 27 countries in PSD supply/demand, weekly export sales, forward curves, WASDE monthly forecasts, EIA biofuel/energy, USDA crush/inspections, CONAB Brazil estimates, and key economic indicators) into a SQLite database (local or Turso cloud). All prices are displayed in **USD/MT** (metric tons) for international comparability. The analysis engine includes an emerging markets deep dive (South Africa, India, Nigeria). A static HTML dashboard (deployed via GitHub Pages) provides 9 pages of visual analysis.

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
python analysis/briefing.py
```

## Required Environment Variables

- `USDA_API_KEY` — USDA NASS QuickStats API key (Layers 2, 12, 14)
- `FRED_API_KEY` — Federal Reserve Economic Data API key (Layer 3)
- `FAS_API_KEY` — USDA FAS OpenData API key (Layer 10 — export sales)
- `EIA_API_KEY` — Energy Information Administration API key (Layer 13 — biofuel/energy)

Layers 1, 4, 5, 6, 7, 8, 9, 11, 15 work without API keys.

### Optional (Cloud Database)

- `TURSO_DATABASE_URL` — Turso database URL (e.g., `libsql://your-db.turso.io`)
- `TURSO_AUTH_TOKEN` — Turso authentication token

If not set, uses local SQLite (default). Set both to enable persistent cloud storage via Turso.

## Architecture

The project follows a three-stage pipeline: **Fetch -> Clean/Validate -> Store**, with an analysis layer on top.

### Data Pipeline (15 Layers + sub-layers)

`main.py` orchestrates the pipeline. Each layer is independent and wrapped in try/except — if one fails, the rest still run (graceful degradation). After each successful layer, a freshness timestamp is recorded.

1. **Commodity prices** — `fetchers/yfinance.py` (11 futures: soy complex, palm oil BMD, corn, wheat, sugar, cotton, cattle, hogs)
2. **USDA crop data** — `fetchers/usda.py` (production, yield, area harvested)
   - **2b. Crop progress/condition** — weekly USDA ratings (% good/excellent, % planted/harvested)
3. **FRED economic data** — `fetchers/fred.py` (dollar index, CPI, Fed funds, Treasury 2Y/10Y/30Y, Ethanol PPI, Soybean Oil PPI, Diesel Price)
4. **COT positioning** — `fetchers/cot.py` (10 commodities including corn, wheat, sugar, cotton, cattle, hogs)
5. **Weather** — `fetchers/weather.py` (24 regions: US, Brazil, Argentina, Paraguay, Colombia, Ethiopia, Ivory Coast, Vietnam, Indonesia, Malaysia, India, Thailand, China, South Africa, Nigeria)
6. **PSD global supply/demand** — `fetchers/psd.py` (8 commodities x 27 countries, oilseeds + grains + coffee + cotton)
7. **Currencies** — `fetchers/yfinance.py` (13 pairs: BRL, ARS, COP, PYG, CNY, IDR, MYR, VND, INR, THB, ETB, ZAR, NGN)
8. **World Bank monthly prices** — `fetchers/worldbank.py` (Robusta, Palm Oil, etc.)
9. **DCE Chinese futures** — `fetchers/akshare.py` (5 contracts including DCE Corn)
10. **Export sales** — `fetchers/export_sales.py` (weekly USDA FAS demand data — requires `FAS_API_KEY`)
11. **Forward curves** — `fetchers/forward_curve.py` (individual contract months via yfinance — contango/backwardation)
12. **WASDE monthly estimates** — `fetchers/usda.py` (USDA monthly supply/demand forecasts — `source_desc=FORECAST`)
13. **EIA biofuel/energy** — `fetchers/eia.py` (ethanol production, biodiesel production, diesel prices — requires `EIA_API_KEY`)
14. **USDA crush + inspections** — `fetchers/usda.py` (monthly soybean crush volumes + weekly AMS export inspections)
15. **CONAB Brazil estimates** — `fetchers/conab.py` (Brazil's official crop agency — production, area, yield)
16. **India domestic soy prices** — `fetchers/india_domestic.py` (NCDEX Bhav Copy — INR/MT, no API key)
17. **Brazil domestic soy spot** — `fetchers/cepea.py` (CEPEA/ESALQ index — BRL/MT, no API key)
18. **South Africa domestic soy** — `fetchers/safex.py` (JSE SAFEX settlement — ZAR/MT, no API key)

### Pipeline Layer

- `pipeline/clean.py` — Normalizes raw data (forward-fill gaps, datetime indices, drop NaN rows). Runs sanity checks (warns on >10% daily moves, zero/negative volume). Contains `_check_nan_gaps()` helper used by `clean_ohlcv()` and `clean_dce_futures()`. Also has `clean_india_domestic()`, `clean_brazil_spot()`, `clean_safex()`.
- `pipeline/schema.py` — All 22 `CREATE TABLE IF NOT EXISTS` SQL definitions. No functions — just the table blueprints used by `store.py`.
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
- `analysis/briefing/` — Daily briefing package. Each section of the briefing lives in its own module under `analysis/briefing/sections/` (prices, crush, fred, usda, crop_progress, wasde, export_sales, inspections, dce, forward_curve, eia, conab, currencies, cot, weather, psd, worldbank, emerging_markets, basis, stocks_to_use, correlations, seasonal, market_drivers, signals, freshness). `analysis/briefing/orchestrator.py` joins them; `analysis/briefing/types.py` defines the typed `BriefingData` returned by `generate_briefing_data()`. `generate_briefing()` is a thin wrapper that returns `BriefingData.text`.
- `analysis/briefing/snapshot.py` — Distills `BriefingData` into structured `snapshot_json` for the briefings archive. Currently captures prices/technicals, crush, and COT; basis, stocks-to-use, and z-scores are not yet in the snapshot — flagged as a backtesting follow-up.
- `analysis/soy_analytics.py` — 9 analyst functions for the soy dashboard: command_center, supply, demand, technicals, relative_value, risk, seasonal, forward_curve, emerging_markets. Pulls price/currency dicts from `analysis/loaders.py`.
- `analysis/health.py` — Per-commodity data health checks (stale data, flat prices, missing commodities)

### Storage

- Database: `data/storage/mirror_market.db` (SQLite, gitignored) — or Turso cloud when configured
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `wasde`, `inspections`, `eia_energy`, `brazil_estimates`, `options_sentiment`, `data_freshness`, `commodity_freshness`, `india_domestic_prices`, `brazil_spot_prices`, `safex_prices`
- All config lives in `config.py` (tickers, API URLs, region coordinates, thresholds)

### Briefing Sections (in order)

1. Data Freshness Warnings
2. Prices (10 commodities with MA, RSI, MACD, volatility)
3. Crush Spread
4. Economic Context (FRED — dollar index, CPI, rates, ethanol PPI)
5. USDA Fundamentals (YoY production/yield)
6. Crop Conditions (weekly USDA % good/excellent, progress)
7. Yield Curve (2Y/10Y spread with recession signal)
8. WASDE Estimates (monthly USDA supply/demand forecasts with MoM revisions)
9. Export Sales (weekly USDA FAS demand data, top buyers)
10. Export Inspections (actual shipments vs committed sales)
11. DCE Chinese Futures (vs CBOT comparison)
12. Forward Curve (contango/backwardation per commodity)
13. Biofuel & Energy (EIA — ethanol, biodiesel production, diesel prices)
14. Brazil Crop Estimates (CONAB vs USDA comparison)
15. Currencies (11 pairs with trade impact)
16. COT Positioning (10 commodities)
17. Weather Alerts (20 regions)
18. Global Supply — PSD (27 countries)
19. World Bank Prices
20. Correlations (cross-commodity + commodity-vs-currency)
21. Seasonal Analysis
22. Market Drivers (BRL + exports, COT + RSI crowding, weather + price premium, dollar impact, corn/soy acreage competition, livestock demand, export sales pace, forward curve structure, palm oil vs soy oil, biofuel pull, CONAB vs USDA divergence)
23. Signals (sorted by severity)

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

### Front-month roll-day discontinuities (Layer 1)

`fetchers/yfinance.py` pulls front-month commodity tickers (`ZS=F`, `ZL=F`, etc.). yfinance silently switches the underlying contract as expirations approach, which introduces artificial price gaps on roll days. These gaps do not represent economic moves.

**Affected analyses:**
- `analysis/technical.py` — SMA crossovers, RSI (Wilder), MACD (EMA-based), Bollinger Bands, historical volatility. All compute on `Close` and propagate the discontinuity.
- `analysis/signals.py` — MA/MACD crossovers, RSI extremes, RSI divergence, Bollinger squeeze. Mitigated: signals within ±3 business days of an estimated roll date (first business day of an active delivery month — see `analysis.signals.is_near_roll`) are demoted to `info` severity and tagged `(near-roll)` in both the briefing and the dashboard.

**Not affected:**
- `analysis/spreads.py` — crush spread, oil/meal ratio, bean/corn ratio. Computed on raw active-contract closes; the artifact appears on each leg simultaneously and largely cancels.
- `analysis/soy_analytics.py` emerging-markets basis — CEPEA/SAFEX/India crush vs CBOT compare raw `Close` values, level-sensitive but consistent.
- Layers 2–18 (USDA fundamentals, FRED, weather, PSD, currencies, forward curve, COT, WASDE, EIA, CONAB, domestic spot prices). None depend on a continuous front-month price.

**Future work (deferred):** a Panama-adjusted `adj_close` column on the `prices` table with the dual-column read pattern (technicals use adjusted, spreads/basis stay on raw). See plan `~/.claude/plans/ontinuous-contract-roll-for-glowing-kernighan.md` for the Phase 2 spike gate and Phase 3 scope.
