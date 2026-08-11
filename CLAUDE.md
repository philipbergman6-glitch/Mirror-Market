

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mirror Market is a commodity market intelligence platform focused on the soy complex (Soybeans, Soybean Oil, Soybean Meal) with supporting data for competing crops. It pulls data from 25 source layers (covering 10 commodity futures, 10 currency pairs including ZAR/NGN, 19 weather regions including SA/Nigeria, 28 countries in PSD supply/demand, weekly export sales, forward curves, WASDE monthly forecasts, EIA biofuel/energy, USDA crush/inspections incl. port-area and destination-country flows, CONAB Brazil estimates, domestic spot prices for India/Brazil/South Africa, AgRural Paranaguá FOB, AMS CIF Gulf export bids, Argentina MAGyP official FOB, and SAGIS South Africa weekly producer deliveries plus its monthly soybean supply & demand balance, and EU rapeseed from the European Commission) into a SQLite database (local or Turso cloud). All prices are displayed in **USD/MT** (metric tons) for international comparability. The analysis engine includes an emerging markets deep dive (South Africa, India, Nigeria). A static HTML dashboard (deployed via GitHub Pages) provides 9 pages of visual analysis.

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

Layers 1, 4, 5, 6, 7, 8, 9, 11, 12, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25 work without API keys. Layer 16 uses the published data.gov.in sample key by default; set `DATA_GOV_IN_API_KEY` (optional) for a personal key with higher row limits. **api.data.gov.in blackholes any Python-identifying User-Agent** — it accepts the connection and never answers, so it reads as a timeout, not a 403. `fetchers/mandi.py` sends an explicit project UA; dropping it silently darkens the layer (#155).

### Optional (Cloud Database — dormant)

- `TURSO_DATABASE_URL` — Turso database URL (e.g., `libsql://your-db.turso.io`)
- `TURSO_AUTH_TOKEN` — Turso authentication token

**Decision 2026-07-30: no cloud DB.** CI persistence uses git-committed CSVs instead (see "Git-based history persistence" below). The Turso code path in `pipeline/connection.py` remains as dormant optional code for local use — it requires `pip install libsql` (deliberately not in `requirements.txt`) plus both env vars. Nothing in CI sets them.

## Architecture

The project follows a three-stage pipeline: **Fetch -> Clean/Validate -> Store**, with an analysis layer on top.

### Data Pipeline (25 Layers + sub-layers)

`main.py` orchestrates the pipeline. Layers 1–13 and 22 are driven by a table of `DictLayer` entries run through a shared `_run_dict_layer()` (fetch → clean → save → `_finalize_layer`, which applies the `LAYER_MIN_KEYS` partial-outage floor); Layers 17–21 and 23–25 share a `_run_scraper_layer()` for `FetchResult` sources; Layers 14–16 keep custom blocks. Each layer is independent and wrapped in try/except — if one fails, the rest still run (graceful degradation). After each successful layer, a freshness timestamp is recorded.

**"Success" requires recency.** `_finalize_layer` gates a stamped `last_success` on three things: the `LAYER_MIN_KEYS` shape floor, then the `LAYER_MAX_DATA_AGE_DAYS` recency budget (how old the *newest observation received* may be), then success. Rows arriving is not the same as new rows arriving — without the recency gate an upstream that answers 200 OK with last month's file every day stays green forever. A stale layer is recorded `status='failed'`, which preserves the previous `last_success`: the timestamp stops advancing, so the layer ages out of its `FRESHNESS_WARNING_DAYS_BY_LAYER` window and shows stale on every surface that already reads freshness. **Not listed in `LAYER_MAX_DATA_AGE_DAYS` = not checked** — `psd`/`wasde`/`usda` are keyed by marketing year with no date column, `forward_curve` is dated by contract month, and `crop_progress` is seasonally silent; all four are covered by the run-cadence window instead. World Bank enforces the same 100-day budget inside the fetcher and returns `{}` rather than storing a frozen Pink Sheet vintage (`DictLayer.empty_fails` stops that empty from recording as an empty-*success*). The scraper layers run the same gate: `_run_scraper_layer` used to call `save_freshness` directly on its success path, so Layers 17–21 could stamp a fresh `last_success` off a frozen upstream no matter what the config said — a live risk for a page that stale-serves rather than emptying (#157). Both paths now save first and grade second, so stale rows are still stored and only the verdict changes.

**"Success" also requires rows.** A layer returning *nothing* used to record an empty-success, stamping a fresh `last_success` against a table that got no data — so severity inverted: 1 of 10 keys was below the `LAYER_MIN_KEYS` floor and recorded `failed`, while 0 of 10 recorded `success`. `_empty_is_failure` derives the grading from the floor instead of a second hand-kept list: **a floor of 2+ means the layer has that many independent keys, so zero of them is an outage** (verified per layer — yfinance and DCE/Sina return full history, PSD is keyed by marketing year, so a holiday or off-week never empties them). `DictLayer.empty_fails` remains as an explicit override for layers with no floor to derive from — `True` for `worldbank`, `wasde` and `ec_oilseeds`, whose upstreams always carry history; the default `None` derives. Layers that legitimately publish nothing keep empty-success: `crop_progress` (seasonally silent), `crush_inspections` (AMS skips report weeks), and the `_run_scraper_layer` layers that pass `empty_fails=False` (`safex` on JSE holidays, `india_domestic` on mandi closures). Empty-failures join `_HARD_FAILURES` and count toward `MAX_FAILED_LAYERS`, so a quiet outage pages CI like a loud one.

**"Skipped" means unconfigured, never "the upstream died."** The two API-key-gated dict layers (`export_sales`, `eia`) write no freshness row when their key is unset — the layer genuinely never ran, and either a `success` or a `failed` row there would be a lie. That skip is decided by `DictLayer.run_if`, a predicate consulted **before** `fetch()` — `fetchers.export_sales.is_configured` / `fetchers.eia.is_configured`, each reading its own module's key constant so the pipeline's answer and the fetcher's own check cannot disagree. It has to be asked of the config, because inferring it from the *result* (the old `if not data and layer.skip_msg`) conflates the two meanings of an empty return: "no key, never ran" and "key set, upstream had nothing", where recording nothing at all is right for the first and hides an outage in the second (#180). That the second case never actually fired is an accident of two fetchers' loop bodies — `fetch_all_export_sales`/`fetch_all_eia` assign `results[name] = df` unconditionally, so a live outage returns empty *frames*, not a bare `{}` — which nothing was pinning. With `run_if` ahead of the fetch, a configured layer always reaches `_finalize_layer` whatever shape its emptiness takes; both carry a `LAYER_MIN_KEYS` floor of 2+, so an all-empty run grades as a failure by the rules above.

**How much came back is recorded too.** Every freshness write carries the real `rows_fetched` plus a `(keys_returned, keys_expected)` pair, on the failure and stale paths as well — a partial outage that returned 3 of 10 keys used to record `0`. `keys_expected` is `len()` of the config catalog the layer iterates (`config.LAYER_KEY_CATALOGS`), never the payload's own length: per-key fetchers only insert *after* a successful fetch, so a weather run that lost 5 of 19 regions returns a 14-key dict and would self-report 14/14. Layers with no catalog record NULL, as do transport failures that never had a payload — NULL means "never learned", `0` means "asked and got nothing". Coverage **describes, it does not grade**: `LAYER_MIN_KEYS` stays the sole verdict, and coverage is rendered only when below full — the briefing freshness block prints `NOTE:` (or `WARNING:` if the layer also failed) and the dashboard Layer Freshness table has its own Keys column.

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
10. **Export sales** — `fetchers/export_sales.py` (weekly USDA FAS demand data — requires `FAS_API_KEY`). One marketing year per commodity, and MY starts are staggered (soy complex + corn 1 Sep, wheat 1 Jun, cotton 1 Aug). ESR answers a not-yet-started MY with HTTP 200 and an empty array, so on 1 September four of six commodities emptied at once and the layer dropped below its `LAYER_MIN_KEYS` floor for the ~8–10 days until the first in-MY report (#181). A **successfully empty** year now falls back once to `market_year - 1` (logged WARNING, never chained); a **failed** request (`_fas_get` returns `None`, not `[]`) does not fall back, so a real FAS outage still hard-fails. The recency budget bounds the fallback — prior-MY rows are frozen in the past, so a commodity that stays empty ages the layer out anyway.
11. **Forward curves** — `fetchers/forward_curve.py` (individual contract months via yfinance — contango/backwardation)
12. **WASDE monthly estimates** — `fetchers/wasde.py` (USDA OCE monthly XLS — `wasdeMMYY.xls`, no API key required)
13. **EIA biofuel/energy** — `fetchers/eia.py` (ethanol production, biodiesel production, diesel prices — requires `EIA_API_KEY`)
14. **USDA crush + inspections** — `fetchers/usda.py` (monthly soybean crush volumes + weekly AMS export inspections, incl. the WA_GR101 Table C port-area breakdown → `inspection_port_flows`)
15. **CONAB Brazil estimates** — `fetchers/conab.py` (Brazil's official crop agency — production, area, yield; aggregates 27 UFs to national totals for Soybeans, Corn, Wheat, Cotton lint; coffee is in a separate CONAB file and not tracked here)
   - **15b. CONAB weekly farmgate prices** — `fetchers/conab_precos.py` (`PrecosSemanalUF.txt` — Paraná soybean producer price, R$/kg → BRL/MT). Cross-check for the CEPEA Paraná wholesale indicator (a ~10–14% wholesale-over-farmgate spread is the expected band). Own commodity key in `brazil_spot_prices` — never spliced into the CEPEA series.
16. **India domestic soy prices** — `fetchers/mandi.py` (data.gov.in Mandi Price API — official Agmarknet feed; per-state median `modal_price` series for Madhya Pradesh (Indore hub — headline benchmark) and Maharashtra (#1 producing state since 2025-26) → INR/MT, bean-only; never pooled across states). Rebuilt 2026-08 after NCDEX became unusable (SEBI derivatives suspension to ≥2027-03-31 + fingerprint wall on the spot pages; `fetchers/india_domestic.py` kept on disk as a dormant fallback). Uses the published sample key by default; set `DATA_GOV_IN_API_KEY` for a personal key. No meal/oil legs, so the old India crush margin is retired — the cross-market line is India bean vs CBOT bean premium (USD/MT). **The level is validated and the premium is real** (#206): 2026-08-11 MP median ₹6,725/quintal = $705/MT against CBOT $425/MT, +66%, confirmed by SOPA's own Indore oil (₹1,400/10kg) and meal (₹57,000–57,500/MT) quotes — which are *not* Agmarknet-derived and imply a ~+4.7% gross crush on that bean — plus two mandi aggregators. India bans GM soybean imports behind a tariff wall, so nothing arbitrages the domestic bean toward CBOT and a large premium is its normal state. Variety/grade mixing was measured and is immaterial (≤0.6%), so no filter is applied. Three traps pinned by tests: **`modal_price` is ₹/quintal** and a switch to ₹/kg or ₹/MT would parse cleanly while restating the level 10–100×, so a day's median outside `MANDI_MODAL_MIN/MAX_INR_QUINTAL` hard-fails; **offset paging must be sorted** (`MANDI_SORT_FIELD`) — unsorted, the walk served 20 of 115 MP rows twice and never served 20 others, inflating the mandi count; and the shared sample key's throttle answers **HTTP 200 with `{"error": "Rate limit exceeded"}`**, which is retried as transport rather than read as a schema break. **No High/Low is stored** — Agmarknet's per-mandi min/max are lot extremes (₹800/qtl against a ₹6,750 modal) and produced a ₹1,010/MT "low" on a ₹67,250/MT day; a cross-sectional median has no range, so Open/High/Low are NaN.
17. **Brazil domestic soy spot** — `fetchers/noticias_agricolas.py` (CEPEA/ESALQ Paraná + ESALQ/B3 Paranaguá indicators republished server-rendered by Notícias Agrícolas — BRL/MT, no API key). Re-enabled 2026-07-30; cepea.org.br itself is still Cloudflare-Turnstile-walled and `fetchers/cepea.py` stays on disk only as a fallback. Historical gap backfill: `scripts/backfill_cepea_gap.py` (one session per `/YYYY-MM-DD` archive page).
18. **South Africa domestic soy** — `fetchers/safex.py` (JSE SAFEX via Grain SA — ZAR/MT, no API key). The stored number is the **last traded price, not settlement/MTM**: the free Grain SA table has no settlement column, and the JSE's own MTM file is behind its Client Portal under terms barring commercial use (#157). The contract shown is the **most-liquid** one that session (largest Volume, ties broken by nearest expiry) — nearest-expiry alone rode the contract into expiry as liquidity rolled away, and could not exclude zero-volume rows whose carried-forward prices the page re-stamps with the current date. The page **stale-serves** on non-trading days rather than emptying, which is why Layer 18 carries a `LAYER_MAX_DATA_AGE_DAYS` budget: without it a frozen upstream would return the same rows forever and stay green.
19. **AgRural Paranaguá FOB** — `fetchers/agrural.py` (Brazil port-side soy FOB scraper — BRL/MT, no API key)
20. **US Gulf export bids** — `fetchers/gulf_bids.py` (AMS report 3147 "Louisiana and Texas Export Bids" daily PDF — CIF NOLA-barge soybean/corn/wheat bids, basis in cents/bu over the named CBOT contract; no API key). A ranged basis quote can span **two different** contracts (`95.00Q to 100.00X` — Aug low leg, Nov high leg; ~3% of cells over 2021-06→2026-08), so both codes are stored (`futures_month`, `futures_month_high`) and the briefing labels each leg with its own month; storing only the low leg priced the high end of the spread against the wrong futures (#196).
21. **Argentina official FOB** — `fetchers/magyp_fob.py` (MAGyP "Precios FOB Oficiales" JSON web service — daily official minimum FOB export values in USD/MT for soybean beans/oil/meal, bulk NCM positions, with shipment-window forward curve; no API key. Position→product mapping cross-verified against the labelled datos.gob.ar series — see `MAGYP_FOB_POSITIONS` in config.py. Feeds the cross-origin FOB board with Layers 19/20.)
23. **SAGIS South Africa weekly producer deliveries** — `fetchers/sagis.py` (tonnage delivered by producers into commercial storage each week, soybeans + sunflower seed, MT; no API key). South Africa's first **physical flow** series and the reason the SA page is a flow page rather than a price page: SAFEX (Layer 18) is capped at a licence-limited last-traded print, while SAGIS grants reproduction with acknowledgement (`SAGIS_ATTRIBUTION` must be rendered wherever these numbers appear). Takes the machine-readable `DT-SWP-<Commodity>_<season>_<week>.xlsx` export, not the `ProdProgressive-*` presentation workbook — flat 9-column table, no header sniffing, and **every season in one file** (2018–2026 as of week 22/2026, 440 rows/commodity). The URL is week-stamped and must be re-resolved from the listing page each run; a hardcoded deep link serves a frozen week at HTTP 200, the same trap as the World Bank CMO GUID in Layer 8. `SeasonYear` is the *start* year of a March–February season, so 2026 = 2026/27; comparisons are made at the same **week number**, which is SAGIS's own convention (a season's week 1 can start in February or March). Components are stored (`first_published`, `adjustments`, `week_total`); the progressive total is derived at read time. SAGIS revises past weeks for months — including in closed seasons — so the `(commodity, season_year, week_number)` upsert legitimately rewrites stored rows every run. Layer 22 is reserved for the in-flight Nigeria AFEX leg (PR #166).
24. **SAGIS South Africa monthly soybean supply & demand** — `fetchers/sagis.py` (`fetch_sagis_supply_demand`; the SA balance sheet in MT — opening stock, deliveries, imports, tonnes processed, whole-bean exports split border-posts vs harbours, the soybean equivalent of product exports, and closing stock split storers-vs-processors; no API key). Filed as SA2 (#203) for SAGIS's *weekly* imports/exports and its 8-week forward intentions — **those two products are published for maize and wheat only** (verified live 2026-08-12), so no soybean trade series exists at weekly cadence and the monthly SMD is the only one there is. `processed_oil_oilcake` is South Africa's **crush volume**; M7's finding that SA has no honest crush *margin* still holds (SAFEX is seed-only, the JSE meal/oil contracts are cash-settled CBOT), and this line must never be rendered as one. Takes the season-progressive workbook `Sojabone<season><season+1>_<pubdate>[_F].xlsx` — all twelve months of one March–February season in one sheet — rather than the per-month announcement file, which holds two months and would cost one request per month of history. **Both the URL and the season set rotate**: the current season is re-published monthly under a new filename and only three seasons are ever listed, so links are resolved from the landing page each run and the table round-trips through `data/history/`. Unreported months are printed as a hard `0`, not left blank, so the frame is cut at the workbook's own `SMD-MMYYYY` vintage tag — without that cut every season would open with a fabricated collapse in crush, trade and stocks. The sheet repeats line-item labels across sections with *different* numbers ((c) commercial-use crush 204,103 t vs (i) local-market 190,651 t for Jun 2026; (h) restates "Opening stock"/"Closing stock" for excluded transit tonnage), so rows are matched by section letter *and* label prefix, and every stored month is checked against the report's own balance, (a)+(b)−(c)−(d)−(e) = (f) — a row read from the wrong section parses fine and is simply wrong, which only that arithmetic can catch. Section totals are not stored; each is the sum of components that are. Keyed `(commodity, season_year, month_number)` with month_number 1 = March, and revised for a year or more after first publication, so the upsert legitimately rewrites stored rows.

22. **EU rapeseed (European Commission)** — `fetchers/ec_oilseeds.py` (EC Oilseeds Market Observatory weekly world FOB xlsx — `Rapeseed - EU Moselle`, ~400 weekly rows back to 2018-12-26, CC BY 4.0, no API key). The Europe page's only price leg. **Euronext MATIF (ECO) settlements are deliberately not ingested**: delayed data is free for internal use but redistribution costs EUR 167.55/month and this project publishes, so the futures curve is licence-blocked (#148). This is a **weekly physical FOB assessment**, not a futures price — no term structure, no daily print — and every stored row carries `cadence` and `quote_kind` so a consumer cannot collapse it into a daily board series. Same rotating-GUID trap as Layer 8: the CIRCABC deep link is resolved from the observatory landing page each run by **link text** ("World oilseed prices"), never by filename — the published filename carries an upstream typo (`oliseeds-world-prices.xlsx`). The workbook's **EUR/t block is derived, not independently quoted** (EUR = USD ÷ the row's ECB rate for 391 of 393 rows; five rows read `n.q.`, and the two newest convert at a two-week-stale rate, ~1.3% off), so **USD/t is authoritative** and EUR is stored exactly as published — NULL where `n.q.`, never recomputed. Self-healing: full history re-downloads every run, so no `data/history/` round-trip.

25. **CEC South Africa official crop estimates** — `fetchers/cec.py` (the Crop Estimates Committee's monthly area/production estimate for soybeans + sunflower seed, ha and MT; no API key). Structurally the SA analogue of Layer 15 (CONAB), but **not an independent second opinion**: USDA's PSD carries the CEC's final figure verbatim at PSD year = CEC season − 1 (2,770,000 / 1,848,000 / 2,800,000 t for the 2023 / 2024 / 2025 crops are exact ties), so no "CEC vs USDA" divergence line exists anywhere — what renders is the in-season revision path plus a **lag** read on PSD (#204). Fetched from the SAGIS mirror because the issuer's own domain, `dalrrd.gov.za`, no longer resolves and its replacement (nda.gov.za) publishes no parseable listing; a mirror that stops republishing is caught by the `LAYER_MAX_DATA_AGE_DAYS` budget, since every release carries its own date. PDF-only, four filename conventions, and `.doc` before 2025 — so `CEC_HISTORY_START` bounds the window at 2025-01-01, inside which **every** PDF must parse. The window is ~23 files / ~18 MB and re-downloads in seconds, so the layer re-reads it whole each run and self-heals on an empty CI database; nothing here needs `data/history/`. It is a **revision series** keyed `(commodity, season_year, release_date)`, never overwritten to a current estimate: the season is forecast nine times, finalised in November, and then *re-finalised* by the CELC in February against SAGIS's actual deliveries (2,771,225 → 2,800,000 t for 2025) — that last figure is the one later reports quote as "final crop". `season_year` is the CEC's calendar-year convention (2026 = the 2025/26 season). Area-only releases (January's preliminary area, October's intentions to plant) store `production_t` NULL, never 0. Two header rows in the source are known-wrong and must not be anchored on — `CEC_2026-02-26.pdf` labels its tons column "Ha", `CEC-27-Feb-2025.pdf` declares a change formula it doesn't use — so columns are matched by **label** and the parse is validated against the source's own arithmetic (the printed Change % must be reproducible from the row) plus an implied-yield band. Yield is derived at read time; the release date is read from the document body, since two filename conventions carry no day and the names that do carry the *upload* date.

### Pipeline Layer

- `pipeline/clean.py` — Normalizes raw data (forward-fill gaps, datetime indices, drop NaN rows). Runs sanity checks (warns on >10% daily moves, zero/negative volume). Contains `_check_nan_gaps()` helper used by `clean_ohlcv()` and `clean_dce_futures()`. Also has `clean_india_domestic()`, `clean_brazil_spot()`, `clean_safex()`, `clean_sagis_deliveries()`.
- `pipeline/schema.py` — All 27 `CREATE TABLE IF NOT EXISTS` SQL definitions. No functions — just the table blueprints used by `store.py`.
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
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `wasde`, `inspections`, `inspection_port_flows`, `inspection_destinations`, `gulf_bids`, `argentina_fob`, `eia_energy`, `brazil_estimates`, `data_freshness`, `commodity_freshness`, `india_domestic_prices`, `brazil_spot_prices`, `safex_prices`, `sagis_deliveries`, `sagis_supply_demand`, `cec_estimates`, `ec_oilseed_prices`, `briefings`
- `forward_curve` keys on `(commodity, contract_month, fetched_date)` — one full curve per run accumulates term-structure history; `read_forward_curve()` returns only each commodity's latest snapshot.
- V1 pipeline config lives in `config.py` (tickers, API URLs, region coordinates, thresholds).
  V2 source/dataset cadence, identity, freshness, validation, retention, rights,
  and criticality policy lives in the authoritative `trust.registry` contract registry.

### Git-based history persistence (`pipeline/history.py`)

CI runs on an ephemeral runner with an empty DB each day. Most layers self-heal by re-downloading full history, but snapshot-only sources don't: AgRural (1 row/day — the Brazil basis source), SAFEX, forward curve, CONAB survey revisions, inspections (>3 weeks) incl. port/destination breakdowns, Gulf bids, Argentina FOB (MAGyP serves history but re-fetch depth is unproven), CEPEA (>~10 sessions), WASDE (>12 months), India mandi (current-day snapshot), **export sales** (ESR is fetched for the current marketing year only, so the outgoing year vanishes at each MY rollover), **briefings** (generated from that run's DB; a past day's `text`/`snapshot_json` is reconstructible from no source at all), **SAGIS monthly supply & demand** (only the current season plus the two most recent finals are ever listed; older seasons exist only as per-month announcement files this layer does not fetch, so a season scrolling off the page is unrecoverable), and **SAGIS deliveries** (the DT export *does* serve history, but only a fixed 9-season window — nothing upstream promises it grows, so a season that rolls off is unrecoverable from an ephemeral CI DB). These tables round-trip through CSVs in `data/history/` (committed to git): `main.py` calls `import_history()` after `init_database()` (INSERT OR IGNORE — DB rows win over CSVs) and `export_history()` after the layers (atomic per-table writes, PK-sorted for stable diffs). The deploy workflow commits `data/history/` back to `main` with `[skip ci]`. A failed import aborts the run so a bad seed can never be exported over good history. Cloud DB (Turso/Supabase) was explicitly rejected for this — do not reintroduce it as a CI requirement.

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
22. Emerging Markets (South Africa SAFEX price + SAGIS delivery pace + SAGIS monthly S&D — crush volume, trade, stocks + CEC official crop estimate with its in-season revision + Brazil CEPEA/CONAB farmgate + India mandi bean vs CBOT + Nigeria deep dive)
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

## Static HTML Site

A small fixed set of static pages deployed to GitHub Pages: the headline page, the players map, and one page per market. **The market is a parameter, never a code path** — `if market == "india"` in a builder is the drift the whole contract exists to prevent; per-market variation belongs in the registry descriptor.

Key files:
- `config.py` → `MARKETS` — the market registry, keyed by slug. **Pointers, never values**: table/column/key *names*, never a price, a date or a tier. Key order is nav order is the headline ledger's row order (`cbot · dalian · brazil · argentina · india · europe · south_africa · nigeria` — role in the trade), declared once so the two consumers cannot disagree.
- `app/markets.py` — the typed view of that registry (`load_markets()`), plus `compute_tiers()`. It lives outside `config.py` because it reads the DB and `config` is imported *by* `pipeline`.
- `app/blocks.py` — the nine-block set and the `{state, reason, data}` envelope every block builder returns. `Block.__post_init__` **raises** when a non-`ok` state carries no reason, so "every empty state names its reason" is enforced by the type rather than by nine builders remembering.
- `app/block_builders.py` — the nine builders that fill that envelope, all **generic SQL over the registry descriptor**: `Source` names the table, date column, key column, value column and unit, so a tenth market is a `config.MARKETS` entry and no code. `Source.to_usd_mt` is the site's **only** conversion site — `native_exchange` (cents/bu, cents/lb, $/short ton), `usd_per_bushel` (AMS 3147 prints flat CIF bids in $/bu), `home_per_mt` (× the `<CCY>/USD` rate **of that row's own date**) or `usd_per_mt`. A `home_per_mt` leg with no FX rate renders its USD/MT as blank, never the local number relabelled. Crush and basis are struck on a session **all** legs printed — no cross-day arithmetic — and a home-currency crush margin is computed only where the legs share one per-MT currency (a board quoted in three different native units has no such number). Several rows for one key on one date (AMS barge locations, MAGyP shipment windows) are averaged and the count is rendered.
- `app/sections.py` — the same `{state, reason, data}` envelope for the five headline sections that used to assemble ~600 lines of HTML in f-strings inside `generate_html.py` (emerging markets, relative value, risk monitor, forward curves, seasonal). Also owns the **chart budget**: `clip()` cuts each series to the window the figure actually reads (`CHART_WINDOW_SESSIONS`), applied where the series meets the figure so the stats above the chart keep their full history.
- `app/templates/blocks/NN_<id>.html.j2` — one partial per block, numbered so file order on disk is block order on the page; `app/templates/sections/*.html.j2` the same for the headline. Markup lives here and nowhere else: an f-string builder cannot enforce "same block, same treatment, eight markets", which is why M8 made this a contract.
- `app/templates/_base.html.j2` — owns `<head>` entirely: fonts, the DESIGN.md palette, the masthead and both nav bars. Every page extends it, `players.html.j2` included, or it becomes the page that drifts.
- `app/templates/market_page.html.j2` / `market_brief.html.j2` / `market_stub.html.j2` — one real template per tier (a brief is not a page with more hatching), plus `tombstone.html.j2`.
- `scripts/generate_site.py` — the orchestrator: owns the page list and the failure-isolation policy.
- `scripts/generate_html.py` — the **headline page's** renderer, one entry in that list.
- `.github/workflows/deploy-dashboard.yml` — daily pipeline run + site generation + Pages deploy.

**Tier is computed from the DB every run, never hard-coded** (M1 #143): a daily price leg plus ≥3 of {ledger, crush, basis, weather} → `page`; less than that, or no daily leg with ≥2 → `brief`; otherwise `stub`. "Present" means *current within that layer's own `LAYER_MAX_DATA_AGE_DAYS` budget*, not "the descriptor names a table" — a market whose scraper died a fortnight ago demotes itself. The ledger is not probed separately: it is daily-only, so it is present exactly when a daily leg is. **The URL never changes with the tier** — `docs/markets/india.html` exists in all three tiers, because anything else means yesterday's link 404s when a scraper breaks.

**The ledger block (02) is deliberately unbuilt.** M3 #145 settled what a ledger row *is*; which 3–4 counterpart legs sit under a given market's own leg is [M12 #161](https://github.com/philipbergman6-glitch/Mirror-Market/issues/161) and still open, so the block renders a reasoned empty state naming that dependency rather than a set chosen inside a build ticket.

**Failure isolation, three levels.** A block that raises renders as an empty state with reason `generation error` — the same *shape* as a missing source, a deliberately different *reason*. A page that fails is replaced by a **tombstone carrying the error, never left as yesterday's file**: Pages ships the whole `docs/` artifact, so a silently retained page is the same stale-serving failure #157 caught in the SAFEX scraper. The headline failing fails the run outright. Any tombstone reds CI *after* the upload, so the site stays usefully up while the failure is loud.

```bash
# Generate the whole site locally
python scripts/generate_site.py
# One page, for the dev loop (headline | players | <market slug>)
python scripts/generate_site.py --only cbot
# Output: docs/index.html, docs/players.html, docs/markets/<slug>.html
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
