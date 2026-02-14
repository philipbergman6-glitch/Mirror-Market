

/# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mirror Market is a commodity market intelligence platform that monitors the global soybean, coffee, palm oil, corn, wheat, sugar, cotton, and livestock markets. It pulls data from 11 free source layers (covering 10 commodity futures, 11 currency pairs, 20 weather regions, 27 countries in PSD supply/demand, weekly export sales, forward curves, and key economic indicators including the yield curve) into a local SQLite database. The analysis engine produces a daily briefing with technical indicators (MACD, Bollinger Bands, RSI divergence), cross-market correlations, seasonal patterns, crop condition tracking, forward curve analysis, and a Market Drivers narrative that connects dots across data sources. An interactive Streamlit dashboard provides 7 pages of visual analysis.

## Commands

```bash
# Install dependencies (uses a .venv virtual environment with Python 3.10+)
pip install -r requirements.txt

# Run the full data pipeline (fetches, cleans, validates, stores all layers)
python main.py

# Generate the daily market briefing
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"

# Launch the interactive dashboard
streamlit run app/dashboard.py

# Run a single analysis module standalone
python analysis/briefing.py
```

## Required Environment Variables

- `USDA_API_KEY` — USDA NASS QuickStats API key (Layer 2 + crop progress)
- `FRED_API_KEY` — Federal Reserve Economic Data API key (Layer 3)
- `FAS_API_KEY` — USDA FAS OpenData API key (Layer 10 — export sales)

Layers 1, 4, 5, 6, 7, 8, 9, 11 work without API keys.

## Architecture

The project follows a three-stage pipeline: **Fetch -> Clean/Validate -> Store**, with an analysis layer on top.

### Data Pipeline (11 Layers + sub-layers)

`main.py` orchestrates the pipeline. Each layer is independent and wrapped in try/except — if one fails, the rest still run (graceful degradation). After each successful layer, a freshness timestamp is recorded.

1. **Commodity prices** — `data/fetchers/yfinance_fetcher.py` (10 futures: soy complex, corn, wheat, sugar, cotton, cattle, hogs)
2. **USDA crop data** — `data/fetchers/usda_fetcher.py` (production, yield, area harvested)
   - **2b. Crop progress/condition** — weekly USDA ratings (% good/excellent, % planted/harvested)
3. **FRED economic data** — `data/fetchers/fred_fetcher.py` (dollar index, CPI, Fed funds, Treasury 2Y/10Y/30Y, Ethanol PPI)
4. **COT positioning** — `data/fetchers/cot_fetcher.py` (10 commodities including corn, wheat, sugar, cotton, cattle, hogs)
5. **Weather** — `data/fetchers/weather_fetcher.py` (20 regions across US, Brazil, Argentina, Paraguay, Colombia, Ethiopia, Ivory Coast, Vietnam, Indonesia, Malaysia, India, Thailand, China)
6. **PSD global supply/demand** — `data/fetchers/psd_fetcher.py` (8 commodities x 27 countries, oilseeds + grains + coffee + cotton)
7. **Currencies** — `data/fetchers/yfinance_fetcher.py` (11 pairs: BRL, ARS, COP, PYG, CNY, IDR, MYR, VND, INR, THB, ETB)
8. **World Bank monthly prices** — `data/fetchers/worldbank_fetcher.py` (Robusta, Palm Oil, etc.)
9. **DCE Chinese futures** — `data/fetchers/akshare_fetcher.py` (5 contracts including DCE Corn)
10. **Export sales** — `data/fetchers/export_sales_fetcher.py` (weekly USDA FAS demand data — requires `FAS_API_KEY`)
11. **Forward curves** — `data/fetchers/forward_curve_fetcher.py` (individual contract months via yfinance — contango/backwardation)

### Processing Layer

- `processing/cleaner.py` — Normalizes raw data (forward-fill gaps, datetime indices, drop NaN rows). Runs sanity checks (warns on >10% daily moves, zero/negative volume).
- `processing/combiner.py` — SQLite storage layer. 13 tables, INSERT OR REPLACE upserts, `read_*()` query functions, freshness tracking.

### Analysis Layer

All 7 modules are actively used in the briefing:

- `analysis/technical.py` — SMA (20/50/200), RSI (Wilder smoothing), MACD (12/26/9), Bollinger Bands, historical volatility, price changes
- `analysis/signals.py` — 20/50 and 50/200 MA crossovers, volume spikes, RSI extremes/divergence, MACD crossovers, Bollinger squeeze
- `analysis/spreads.py` — Soybean crush spread (Oil*11 + Meal*2.2 - Beans)
- `analysis/correlations.py` — Cross-commodity matrix, commodity-vs-currency, rolling correlation
- `analysis/seasonal.py` — Monthly seasonal averages, current vs historical norm
- `analysis/forward_curve.py` — Forward curve analysis: contango/backwardation, curve slope, calendar spreads
- `analysis/briefing.py` — ALL data layers + all analysis into a daily text briefing with Market Drivers narrative

### Storage

- Database: `data/storage/mirror_market.db` (SQLite, gitignored)
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `data_freshness`
- All config lives in `config.py` (tickers, API URLs, region coordinates, thresholds)

### Briefing Sections (in order)

1. Data Freshness Warnings
2. Prices (10 commodities with MA, RSI, MACD, volatility)
3. Crush Spread
4. Economic Context (FRED — dollar index, CPI, rates, ethanol PPI)
5. USDA Fundamentals (YoY production/yield)
6. Crop Conditions (weekly USDA % good/excellent, progress)
7. Yield Curve (2Y/10Y spread with recession signal)
8. Export Sales (weekly USDA FAS demand data, top buyers)
9. DCE Chinese Futures (vs CBOT comparison)
10. Forward Curve (contango/backwardation per commodity)
11. Currencies (11 pairs with trade impact)
12. COT Positioning (10 commodities)
13. Weather Alerts (20 regions)
14. Global Supply — PSD (27 countries)
15. World Bank Prices
16. Correlations (cross-commodity + commodity-vs-currency)
17. Seasonal Analysis
18. Market Drivers (BRL + exports, COT + RSI crowding, weather + price premium, dollar impact, corn/soy acreage competition, livestock demand, export sales pace, forward curve structure)
19. Signals (sorted by severity)

## Key Patterns

- All fetchers return `dict[str, pd.DataFrame]` (keyed by commodity/region name)
- All cleaners return copies — originals are never mutated
- Database uses `INSERT OR REPLACE` so the pipeline is safe to re-run
- Analysis functions expect DataFrames with a `Close` column and DatetimeIndex
- Logging throughout — configured once in `config.setup_logging()`
- Configurable thresholds in `config.py` (RSI, volume spike, weather, freshness)
- Signals have severity levels: `alert` > `warning` > `info`
