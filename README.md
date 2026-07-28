# Mirror Market

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-2D6A4F?style=for-the-badge)](https://philipbergman6-glitch.github.io/Mirror-Market/)

Commodity market intelligence for the soy complex (soybeans, soybean oil, soybean meal) and competing crops. A Python pipeline pulls 19 free data source layers across 27 countries into SQLite, an analysis engine turns them into signals and a daily text briefing, and a static dashboard — a light, editorial "morning scan" page — deploys automatically to GitHub Pages every weekday.

All prices are shown in **USD per metric ton** for international comparability.

## The Dashboard

One scrolling page in scan order, every section collapsible:

**01 Overnight** prices · **02 Signals** · **03 Crush & Relative Value** · **04 Supply & Demand** (WASDE, CONAB, exports, emerging markets) · **05 Risk** (currencies, COT, weather) · **06 Forward Curves** · **07 Seasonal** · **08 Technicals** · **09 Full Briefing** · **10 About**

## Data Sources (all free)

| # | Source | Coverage |
|--:|--------|----------|
| 1 | Yahoo Finance | 11 commodity futures (daily) |
| 2 | USDA NASS* | US production, yield, weekly crop conditions |
| 3 | FRED* | Dollar index, CPI, rates, yield curve |
| 4 | CFTC | COT positioning, 10 commodities (weekly) |
| 5 | Open-Meteo | Weather in 24 growing regions, 6 continents |
| 6 | USDA PSD | Supply/demand, 8 commodities × 27 countries |
| 7 | Yahoo Finance | 13 currency pairs |
| 8 | World Bank | Monthly benchmark prices (Robusta, palm oil) |
| 9 | AKShare | DCE Chinese futures, 5 contracts |
| 10 | USDA FAS* | Weekly export sales + top buyers |
| 11 | Yahoo Finance | Forward curves (contango/backwardation) |
| 12 | USDA WASDE | Monthly supply/demand forecasts |
| 13 | EIA* | Ethanol, biodiesel, diesel |
| 14 | USDA* | Monthly crush + weekly export inspections |
| 15 | CONAB | Brazil official crop estimates |
| 16 | NCDEX | India domestic soy *(currently disabled — anti-bot wall)* |
| 17 | CEPEA | Brazil farm-gate soy *(currently disabled — Cloudflare)* |
| 18 | JSE SAFEX | South Africa soy settlements |
| 19 | AgRural | Paranaguá FOB — Brazil port basis |

\* needs a free API key (`USDA_API_KEY`, `FRED_API_KEY`, `FAS_API_KEY`, `EIA_API_KEY`). **14 of 19 layers run with no keys at all.** Any layer can fail and the rest still run.

## Analysis

- **Technicals** — SMA 20/50/200, RSI (Wilder), MACD, Bollinger Bands, volatility
- **Signals** — MA/MACD crossovers, RSI extremes and divergence, volume spikes, Bollinger squeeze, ranked by severity
- **Trader metrics** — crush spread and soy-oil value share, Brazil basis (Paranaguá vs CBOT), stocks-to-use tightness, COT and weather z-scores, bean/corn acreage ratio
- **Market Drivers** — a narrative that connects layers: BRL + exports, positioning + RSI crowding, weather premiums, dollar impact, China buying pace
- **Daily briefing** — 26-section text report, archived to the database with a structured snapshot for backtesting

## Run It

```bash
pip install -r requirements.txt

python main.py                      # fetch → clean → validate → store (19 layers)
python scripts/generate_html.py     # build the dashboard → docs/index.html
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"
```

## Deployment

GitHub Actions runs the pipeline and redeploys the dashboard to Pages every weekday at 12:00 UTC (and on every push to `main`). API keys live in repository secrets. Optional Turso cloud storage via `TURSO_DATABASE_URL` + `TURSO_AUTH_TOKEN`; defaults to local SQLite.

## Layout

```
main.py            pipeline orchestrator (19 layers, graceful degradation)
fetchers/          one module per data source
pipeline/          schema, store, query, clean, unit conversion, DB connection
analysis/          technicals, signals, spreads, briefing package, dashboard analytics
app/               Plotly chart builders + Jinja2 template
scripts/           static dashboard generator
docs/index.html    the generated dashboard (GitHub Pages)
```

Design system in `DESIGN.md` · architecture details in `ARCHITECTURE.md` and `CLAUDE.md`.

---

*A project by Philip Bergman. Free public data, delayed, provided as-is — not financial advice.*
