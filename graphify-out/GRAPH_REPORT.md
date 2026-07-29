# Graph Report - /Users/philipbergman/Documents/Coding_Projects/Mirror-Market  (2026-07-28)

## Corpus Check
- 117 files · ~105,195 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 1328 nodes · 3031 edges · 77 communities (74 shown, 3 thin omitted)
- Extraction: 97% EXTRACTED · 3% INFERRED · 0% AMBIGUOUS · INFERRED: 78 edges (avg confidence: 0.74)
- Token cost: 146,729 input · 0 output

## Community Hubs (Navigation)
- Signal Detection
- Basis & Crush Sections
- Config & DCE Fetch
- WASDE Fetcher
- Technical Indicators
- COT Positioning
- Dashboard Pages
- SAFEX Fetcher
- AgRural Scraper
- Briefing Orchestrator
- Seasonal Analysis
- Dashboard Charts
- Stocks-to-Use Signal
- USDA Fetcher
- DB Upsert Writers
- Freshness & Health
- DB Save Functions
- Energy & Drivers Sections
- Shared Data Loaders
- DB Schema & Benchmarks
- Unit Conversion USD/MT
- Crop Conditions & Connection
- NCDEX India Fetcher
- CEPEA Scraper
- Price Read Tests
- Turso Connection Layer
- Supply & Dashboard Build
- Save-Function Tests
- Forward Curve Fetcher
- Design Rationale (Changelog)
- Turso Roundtrip Tests
- FRED & Export Cleaning
- OHLCV Cleaning
- FetchResult Contract
- Correlations
- Exit-Code Smoke Tests
- Freshness Markers
- Data Cleaning Core
- CONAB Fetcher
- Briefing Archive
- Global Supply Sections
- Export Sales Sections
- Emerging Markets
- Prices Section & Signals
- World Bank Pink Sheet
- Database Store Init
- Platform Architecture Docs
- README PDF Generator
- Freshness Regression Tests
- USDA Year-Range Tests
- Export Sales Fetcher
- PSD Fetcher
- Back-Contract Probe
- DCE Section
- USDA Section
- WASDE Section
- Brazil Spot Cleaning
- CONAB Cleaning
- COT Cleaning
- EIA Cleaning
- Forward Curve Cleaning
- India Cleaning
- Inspections Cleaning
- PSD Cleaning
- SAFEX Cleaning
- WASDE Cleaning
- Weather Cleaning
- World Bank Cleaning
- CSV Data URIs
- Crush & Roll Concepts
- Sections Package
- Storage Rationale
- Dependency Rule

## God Nodes (most connected - your core abstractions)
1. `run()` - 61 edges
2. `get_connection()` - 44 edges
3. `generate_briefing_data()` - 40 edges
4. `retry_sleep()` - 36 edges
5. `is_cloud()` - 36 edges
6. `_bday_index()` - 31 edges
7. `generate()` - 27 edges
8. `save_price_data()` - 25 edges
9. `_save()` - 24 edges
10. `to_metric_tons()` - 23 edges

## Surprising Connections (you probably didn't know these)
- `Mirror Market Static Dashboard (Soy Complex Desk)` --semantically_similar_to--> `Interactive Streamlit Dashboard (app/dashboard.py, 7 pages)`  [INFERRED] [semantically similar]
  docs/index.html → Mirror_Market_Overview.pdf
- `Mirror Market Static Dashboard (Soy Complex Desk)` --implements--> `Mirror Market Platform`  [INFERRED]
  docs/index.html → Mirror_Market_Overview.pdf
- `Risk Monitor page` --conceptually_related_to--> `Layer 4 — COT Positioning (CFTC via cot_reports, commercials vs speculators)`  [AMBIGUOUS]
  docs/index.html → Mirror_Market_Overview.pdf
- `Forward Curves page` --conceptually_related_to--> `Forward Curve Analysis (analysis/forward_curve.py: contango/backwardation detection, curve slope, calendar spreads)`  [INFERRED]
  docs/index.html → Mirror_Market_Overview.pdf
- `Full Briefing page` --conceptually_related_to--> `Daily Briefing (analysis/briefing.py)`  [INFERRED]
  docs/index.html → Mirror_Market_Overview.pdf

## Import Cycles
- None detected.

## Hyperedges (group relationships)
- **Trader-Grade Signals Suite** — changelog_stocks_to_use_ratio, changelog_brazil_basis, changelog_zscore_helpers, changelog_briefing_archive, readme_trader_grade_signals [EXTRACTED 1.00]
- **Scraper Fixture Regression Testing** — tests_fixtures_readme_fixture_snapshot_policy, tests_fixtures_agrural_paranagua_agrural_fixture, tests_fixtures_ams_inspections_ams_inspections_fixture, tests_fixtures_cepea_soybean_cepea_fixture, tests_fixtures_safex_grainsa_safex_fixture, changelog_scrapershapeerror [EXTRACTED 1.00]
- **Silent-Failure Elimination Pattern** — changelog_fetchresult_pattern, changelog_scrapershapeerror, changelog_mark_empty_vs_failed, changelog_conab_schema_rewrite, claude_antibot_layer_disablement [INFERRED 0.85]
- **Eleven-Layer Free Data Source Architecture** — mirror_market_overview_layer_1_futures_prices, mirror_market_overview_layer_2_usda_crop_fundamentals, mirror_market_overview_layer_2b_crop_progress, mirror_market_overview_layer_3_fred_economic_context, mirror_market_overview_layer_4_cot_positioning, mirror_market_overview_layer_5_weather_data, mirror_market_overview_layer_6_psd_supply_demand, mirror_market_overview_layer_7_currency_rates, mirror_market_overview_layer_8_world_bank_prices, mirror_market_overview_layer_9_dce_chinese_futures, mirror_market_overview_layer_10_export_sales, mirror_market_overview_layer_11_forward_curves [EXTRACTED 1.00]
- **Analysis Engine Feature Set feeding the Daily Briefing** — mirror_market_overview_technical_indicators, mirror_market_overview_trading_signals, mirror_market_overview_crush_spread, mirror_market_overview_forward_curve_analysis, mirror_market_overview_correlations, mirror_market_overview_seasonal_patterns, mirror_market_overview_yield_curve_analysis, mirror_market_overview_daily_briefing [EXTRACTED 1.00]
- **Static Dashboard Nine-Page Navigation** — docs_index_command_center, docs_index_technicals, docs_index_supply_demand, docs_index_relative_value, docs_index_risk_monitor, docs_index_forward_curves, docs_index_seasonal_patterns, docs_index_full_briefing, docs_index_about_mirror_market [EXTRACTED 1.00]

## Communities (77 total, 3 thin omitted)

### Community 0 - "Signal Detection"
Cohesion: 0.06
Nodes (67): format(), SIGNALS section — sorted by severity (alert > warning > info). Signals within…, demote_near_roll_signals(), detect_bollinger_squeeze(), detect_ma_crossovers(), detect_macd_crossover(), detect_rsi_divergence(), detect_rsi_extremes() (+59 more)

### Community 1 - "Basis & Crush Sections"
Cohesion: 0.07
Nodes (53): _basis_for_source(), format(), _format_basis_line(), DataFrame, BRAZIL BASIS section — Paranaguá FOB (primary) + CEPEA Paraná (secondary) vs…, format(), DataFrame, CRUSH SPREAD section — Soybeans crush margin. (+45 more)

### Community 2 - "Config & DCE Fetch"
Cohesion: 0.07
Nodes (44): Mirror Market configuration. Ticker symbols, API keys, and shared settings live…, Configure the root logger with a clean, timestamped format. Every module that…, setup_logging(), fetch_dce_futures(), fetch_one(), DataFrame, Layer 9 — DCE (Dalian Commodity Exchange) futures via AKShare. AKShare is an…, Download daily futures data for a single DCE contract. Parameters ----------… (+36 more)

### Community 3 - "WASDE Fetcher"
Cohesion: 0.07
Nodes (53): _assign_reference_periods(), _build_url(), _cell_str(), _clean_label(), _coerce_float(), _detect_unit(), _download(), fetch_wasde_estimates() (+45 more)

### Community 4 - "Technical Indicators"
Cohesion: 0.11
Nodes (43): add_moving_averages(), add_price_changes(), add_rsi(), calculate_bollinger(), calculate_macd(), calculate_volatility(), compute_all_technicals(), DataFrame (+35 more)

### Community 5 - "COT Positioning"
Cohesion: 0.08
Nodes (38): format(), _format_side(), DataFrame, Timestamp, COT POSITIONING section — commercials vs. specs with z-scores vs 3-year…, Z-score of the latest value in `column` vs the trailing 3-year baseline., _zscore_for(), _annotate() (+30 more)

### Community 6 - "Dashboard Pages"
Cohesion: 0.07
Nodes (41): About Mirror Market page, Command Center page, Data Freshness panel, Forward Curves page, Full Briefing page, Mirror Market Static Dashboard (Soy Complex Desk), Relative Value page, Risk Monitor page (+33 more)

### Community 7 - "SAFEX Fetcher"
Cohesion: 0.07
Nodes (38): fetch_safex(), _find_settlement_table(), _normalize_header(), _parse_safex_table(), BeautifulSoup, DataFrame, Locate the settlement table and return (header_cells, body_rows). Raises…, Parse a numeric cell, returning None for blanks or non-numerics. (+30 more)

### Community 8 - "AgRural Scraper"
Cohesion: 0.08
Nodes (35): fetch_agrural(), _fetch_agrural_page(), _find_header_columns(), _find_paranagua_price(), _find_soy_table(), _parse_agrural_table(), _parse_banner_date(), BeautifulSoup (+27 more)

### Community 9 - "Briefing Orchestrator"
Cohesion: 0.09
Nodes (28): Daily market briefing package. The package replaces the previous monolithic…, _assemble_text(), generate_briefing(), generate_briefing_data(), Orchestrator — loads shared data and joins section output into the daily…, Return the formatted briefing text. Thin wrapper around…, Build the full briefing as structured data + the joined text block. If…, format() (+20 more)

### Community 10 - "Seasonal Analysis"
Cohesion: 0.10
Nodes (31): format(), DataFrame, SEASONAL ANALYSIS section — current vs. historical norms., calendar_spread(), Compute the calendar spread between two contract months. Parameters ----------…, current_vs_seasonal(), monthly_seasonal(), DataFrame (+23 more)

### Community 11 - "Dashboard Charts"
Cohesion: 0.12
Nodes (31): build_basis_chart(), build_bean_corn_ratio_chart(), build_correlations_chart(), build_cot_chart(), build_crush_spread_chart(), build_forward_curve_chart(), build_oil_meal_ratio_chart(), build_seasonal_chart() (+23 more)

### Community 12 - "Stocks-to-Use Signal"
Cohesion: 0.14
Nodes (29): format(), STOCKS-TO-USE section — US balance sheet from PSD. PSD (Layer 6) is the…, Return (text, signals) — orchestrator extends the briefing signal list., compute_stocks_to_use(), detect_tight_supply(), DataFrame, Stocks-to-use ratio computation and tight-supply signal detection. The ratio…, Return stocks-to-use ratios per (commodity, marketing year). Parameters… (+21 more)

### Community 13 - "USDA Fetcher"
Cohesion: 0.10
Nodes (30): _current_crop_year_end(), fetch_all_crop_progress(), fetch_crop_progress(), fetch_crush_data(), fetch_export_inspections(), fetch_soybean_overview(), fetch_usda(), _parse_inspections() (+22 more)

### Community 14 - "DB Upsert Writers"
Cohesion: 0.11
Nodes (31): DataFrame, Open a connection and run a transactional upsert. Logs result., In-place: ensure each `cols` column exists and is string-typed., Write crop progress → 'crop_progress'. Source 'statisticcat_desc' →…, Write PSD → 'psd'. Drops rows with NaN year (INTEGER NOT NULL key)., Write World Bank monthly prices → 'worldbank_prices'., Write weekly export sales → 'export_sales'., Write forward curve → 'forward_curve'. Stamps fetched_date with today UTC. (+23 more)

### Community 15 - "Freshness & Health"
Cohesion: 0.12
Nodes (28): format(), Data freshness warnings — shown at the top of the briefing., Stale-layer warnings + per-commodity health summary., _build_commodity_status(), _check_brazil_spot(), _check_cot(), _check_currencies(), _check_dce() (+20 more)

### Community 16 - "DB Save Functions"
Cohesion: 0.10
Nodes (29): _date(), Series, ISO-format a date column. NaT becomes NaT and is NULLed at write., Write FRED Series → 'economic'., Write COT positioning → 'cot'., Write weather → 'weather'., Write DCE futures → 'dce_futures'., Write WASDE forecast → 'wasde'. commodity_key (e.g., 'SOYBEANS/PRODUCTION') is… (+21 more)

### Community 17 - "Energy & Drivers Sections"
Cohesion: 0.12
Nodes (24): format(), BIOFUEL & ENERGY (EIA) section — ethanol, biodiesel, diesel prices., format(), FORWARD CURVE section — contango/backwardation per commodity., format(), DataFrame, MARKET DRIVERS section — cross-data narrative. This is the most cross-cutting…, analyze_curve() (+16 more)

### Community 18 - "Shared Data Loaders"
Cohesion: 0.13
Nodes (24): clear_loader_cache(), load_currencies(), load_prices(), DataFrame, Shared data loaders for the analysis layer. Both the daily briefing…, Return a dict[commodity] -> DataFrame indexed by Date. Args: with_technicals:…, Return a dict[pair] -> DataFrame indexed by Date., Reset both loader caches. Call between pipeline runs or in tests. (+16 more)

### Community 19 - "DB Schema & Benchmarks"
Cohesion: 0.11
Nodes (22): Connection, MonkeyPatch, SQL schema strings for Mirror Market database tables. Contains all CREATE TABLE…, main(), DataFrame, Microbenchmark for pipeline.store save_* functions. Measures rows/second for…, Run fn() `repeats` times, return median seconds., Spin up a fresh temp SQLite and rebind get_connection to it. (+14 more)

### Community 20 - "Unit Conversion USD/MT"
Cohesion: 0.14
Nodes (23): convert_df_to_mt(), native_label(), DataFrame, Return the native exchange unit label for a commodity. Useful for tooltips or…, Convert a single price value from native exchange units to USD/MT. Parameters…, Convert OHLC price columns in a DataFrame from native units to USD/MT. Creates…, to_metric_tons(), _make_price_df() (+15 more)

### Community 21 - "Crop Conditions & Connection"
Cohesion: 0.12
Nodes (21): format(), CROP CONDITIONS section — weekly USDA condition/progress ratings., format(), WORLD PRICES (World Bank Monthly) section., get_connection(), Get a database connection — cloud (Turso) or local (SQLite). If…, Database read (query) functions for Mirror Market. All read_* functions query…, Read crop progress/condition data from SQLite. (+13 more)

### Community 22 - "NCDEX India Fetcher"
Cohesion: 0.12
Nodes (23): _date_str(), _extract_soy_prices(), fetch_india_domestic(), _first_match(), DataFrame, Layer 16 — NCDEX India domestic soy prices. NCDEX (National Commodity and…, Return the first column name that matches any candidate alias., Convert a value to float, returning None on failure or NaN. (+15 more)

### Community 23 - "CEPEA Scraper"
Cohesion: 0.12
Nodes (20): fetch_cepea(), _fetch_cepea_page(), _find_price_table(), _normalize(), _parse_brl_price(), _parse_cepea_tables(), BeautifulSoup, DataFrame (+12 more)

### Community 24 - "Price Read Tests"
Cohesion: 0.17
Nodes (21): Read price data back from SQLite. Parameters ---------- commodity : str or None…, read_prices(), _currency_df(), _datetime_index(), _fetchall(), _price_df(), DatetimeIndex, parametrize (+13 more)

### Community 25 - "Turso Connection Layer"
Cohesion: 0.12
Nodes (19): is_cloud(), Database connection abstraction for Mirror Market. Provides a single…, Raised when Turso is required (MIRROR_REQUIRE_TURSO=1) but unreachable., Check if we're configured to use Turso cloud database., _require_turso(), TursoUnavailableError, Scan data tables, record per-commodity last_date + row count., update_commodity_freshness() (+11 more)

### Community 26 - "Supply & Dashboard Build"
Cohesion: 0.14
Nodes (20): Build the supply-side picture: WASDE, CONAB, PSD, crop progress. Returns dict…, supply_analysis(), delta_str(), Format a number as +X.X% or -X.X%., _build_briefing_text(), _build_command_center(), _build_demand(), _build_emerging_markets() (+12 more)

### Community 27 - "Save-Function Tests"
Cohesion: 0.16
Nodes (19): Write OHLCV → 'prices'., Write currency OHLC → 'currencies'., save_currency_data(), save_price_data(), _price_df(), DataFrame, parametrize, Smoke tests for pipeline.query. Covers the three behaviours every read_*… (+11 more)

### Community 28 - "Forward Curve Fetcher"
Cohesion: 0.17
Nodes (16): _build_contract_tickers(), fetch_all_forward_curves(), fetch_forward_curve(), DataFrame, Layer 11 — Forward curve data via yfinance. Instead of just the front-month…, Fetch forward curves for all configured commodities. Returns ------- dict…, Build a list of upcoming contract tickers for a commodity. Parameters…, Fetch the forward curve for a single commodity. Downloads the latest close… (+8 more)

### Community 29 - "Design Rationale (Changelog)"
Cohesion: 0.12
Nodes (17): Native Units, Convert at Display Rule, Brazil Export Basis (compute_brazil_basis), Briefing Archive (briefings table + snapshot_json), CONAB Fetcher Schema Rewrite (Layer 15), FetchResult Typed Fetch Outcome (ok/empty/failed), _mark_empty vs _mark_failed Freshness Distinction, ScraperShapeError Pattern, Stocks-to-Use Ratio Signal (+9 more)

### Community 30 - "Turso Roundtrip Tests"
Cohesion: 0.15
Nodes (16): _cleanup(), fixture, End-to-end Turso roundtrip test. Skipped unless both `TURSO_DATABASE_URL` and…, save_cot_data → read_cot via Turso preserves rows + values., PSD year is INTEGER NOT NULL — must survive the libsql roundtrip., Failed status keeps the prior last_success stamp (cloud variant)., Ensure the Turso schema exists. Returns the test-run UUID prefix., Best-effort delete of test rows. (+8 more)

### Community 31 - "FRED & Export Cleaning"
Cohesion: 0.23
Nodes (14): clean_export_sales(), clean_fred_series(), Series, Clean a FRED time series. Forward-fills gaps (FRED often publishes monthly, so…, Clean USDA FAS export sales data. Steps: 1. Ensure week_ending is datetime. 2.…, _make_export_sales(), _make_fred_series(), Series (+6 more)

### Community 32 - "OHLCV Cleaning"
Cohesion: 0.17
Nodes (15): clean_ohlcv(), Clean a raw OHLCV DataFrame from yfinance. Steps: 1. Drop rows where ALL price…, _make_ohlcv(), DataFrame, parametrize, A single-day NaN row is forward-filled from the prior bar., A row where every OHLC value is NaN is dropped, not ffilled.…, RSI computed on a partial-NaN ffilled bar treats it as zero-delta. When at… (+7 more)

### Community 33 - "FetchResult Contract"
Cohesion: 0.17
Nodes (9): FetchResult, DataFrame, Typed return value for every fetcher. ``data`` is the per-key DataFrame mapping…, Tests for the FetchResult / ScraperShapeError types., test_empty_constructor_signals_zero_rows_but_no_error(), test_failed_constructor_carries_error_string(), test_has_rows_is_false_when_all_frames_are_empty(), test_ok_constructor_marks_status_and_keeps_data() (+1 more)

### Community 34 - "Correlations"
Cohesion: 0.21
Nodes (12): format(), DataFrame, CORRELATIONS section — cross-commodity + commodity-vs-currency., commodity_correlation_matrix(), commodity_vs_currency(), DataFrame, Series, Cross-market correlation analysis. Correlations tell you how two markets move… (+4 more)

### Community 35 - "Exit-Code Smoke Tests"
Cohesion: 0.22
Nodes (12): RuntimeError, _make_ohlcv(), DataFrame, fixture, Smoke tests for the pipeline's critical-layer exit code logic. We don't run the…, A failed layer leaves a status='failed' row so the dashboard can render it., Replace every fetcher with a no-op that returns the right shape. By default…, stub_fetchers() (+4 more)

### Community 36 - "Freshness Markers"
Cohesion: 0.20
Nodes (12): _mark_empty(), _mark_failed(), Best-effort 'failed' freshness row — never crashes the pipeline itself., Record a successful run that returned zero rows. Distinct from _mark_failed:…, run(), Write export inspections → 'inspections'., Record a freshness row. Success stamps last_success; failed preserves it., save_freshness() (+4 more)

### Community 37 - "Data Cleaning Core"
Cohesion: 0.23
Nodes (11): _check_nan_gaps(), clean_dce_futures(), DataFrame, Data cleaning utilities. Raw data from external APIs is messy — missing days…, Run sanity checks on price data and log warnings for suspicious values. Checks:…, Clean DCE futures data from AKShare. AKShare returns lowercase columns: date,…, Warn if any column has >5 consecutive NaN values., _validate_price_data() (+3 more)

### Community 38 - "CONAB Fetcher"
Cohesion: 0.29
Nodes (10): _aggregate_national(), fetch_conab_estimates(), _melt_to_long(), _parse_csv(), DataFrame, Layer 15 — CONAB (Companhia Nacional de Abastecimento) Brazil crop estimates.…, Reshape aggregated DataFrame into the long format the pipeline expects., Fetch CONAB historical series data for Brazilian crop estimates. Returns a… (+2 more)

### Community 39 - "Briefing Archive"
Cohesion: 0.20
Nodes (11): Any, Read one archived briefing by date. Returns None if absent. Returns a dict with…, read_briefing(), Any, Archive a generated briefing. INSERT OR REPLACE keyed on briefing_date.…, save_briefing(), _scenario_briefings(), test_read_briefing_missing_returns_none() (+3 more)

### Community 40 - "Global Supply Sections"
Cohesion: 0.27
Nodes (8): format(), BRAZIL CROP ESTIMATES (CONAB) section — compares to USDA PSD., format(), GLOBAL SUPPLY (USDA PSD) section., Read PSD global supply/demand data from SQLite., Read CONAB Brazil estimates from SQLite., read_brazil_estimates(), read_psd()

### Community 41 - "Export Sales Sections"
Cohesion: 0.27
Nodes (8): format(), EXPORT SALES section — weekly USDA FAS demand data., format(), EXPORT INSPECTIONS section — actual shipments vs commitments., Read export sales data from SQLite., Read export inspections data from SQLite., read_export_sales(), read_inspections()

### Community 42 - "Emerging Markets"
Cohesion: 0.28
Nodes (8): format(), _format_india_domestic(), EMERGING MARKETS section — South Africa, India, Nigeria, Brazil deep dive.…, Inline India NCDEX domestic prices + crush margin block., emerging_markets_analysis(), Deep dive on emerging soybean markets: South Africa, India, Nigeria. Returns…, Read currency data from SQLite., read_currencies()

### Community 43 - "Prices Section & Signals"
Cohesion: 0.25
Nodes (8): format(), DataFrame, PRICES section — also collects signals and returns enriched DataFrames. This…, Format the PRICES section. Returns: (text, signals, enriched) where: text — the…, detect_all_signals(), Run all signal detectors on a single commodity's DataFrame. Parameters…, Full technical analysis for all 3 soy legs. Returns dict with: per_leg: dict of…, technicals_analysis()

### Community 44 - "World Bank Pink Sheet"
Cohesion: 0.31
Nodes (8): _download_pink_sheet(), fetch_worldbank_prices(), _parse_pink_sheet(), DataFrame, Layer 8 — World Bank Pink Sheet monthly commodity prices. Downloads the CMO…, Download Pink Sheet xlsx, extract monthly prices for target commodities.…, Download the Pink Sheet xlsx file, return raw bytes., Parse the Pink Sheet xlsx into per-commodity DataFrames. The Monthly Prices…

### Community 45 - "Database Store Init"
Cohesion: 0.31
Nodes (8): clear_database(), _ensure_storage_dir(), init_database(), _migrate_data_freshness(), Database write functions for Mirror Market. `upsert_dataframe` factors all…, Add status/last_attempt to data_freshness if absent. Idempotent., Create tables + unique indexes if missing. Idempotent., Drop all tables. Manual-only utility.

### Community 46 - "Platform Architecture Docs"
Cohesion: 0.25
Nodes (8): CI Workflow (lint + mypy + pytest), Update Dashboard Workflow (pipeline + Pages deploy), Five-Stage Pipeline (Fetch, Clean, Store, Analyze, Render), Shared retry_sleep Backoff with Jitter, 19-Layer Data Pipeline, Mirror Market CLAUDE.md Project Guide, Mirror Market Design System (Industrial Dark), Mirror Market Platform

### Community 47 - "README PDF Generator"
Cohesion: 0.36
Nodes (6): FPDF, make_pdf(), PDF, One-time script to convert README.md to a clean PDF., Replace Unicode chars that latin-1 can't encode., sanitize()

### Community 48 - "Freshness Regression Tests"
Cohesion: 0.25
Nodes (7): Regression: ISSUE-002 — freshness sidebar must show non-negative ages. Found by…, A timestamp 3 hours in the past must render with a positive age string. Before…, Whether the stored timestamp parses as tz-aware or naive, age must be non-…, The exact failure mode from ISSUE-002: writer stamps naive UTC, reader runs in…, test_freshness_age_handles_aware_and_naive_timestamps(), test_freshness_age_is_non_negative_for_past_timestamp(), test_freshness_handles_writer_format_against_non_utc_local_clock()

### Community 49 - "USDA Year-Range Tests"
Cohesion: 0.25
Nodes (4): fixture, Verify USDA fetchers use a dynamic year_end (not a hardcoded year). Catches the…, Pretend USDA_API_KEY is set and stub requests.get to capture params., stub_usda()

### Community 50 - "Export Sales Fetcher"
Cohesion: 0.38
Nodes (7): _current_market_year(), fetch_all_export_sales(), fetch_export_sales(), DataFrame, Fetch weekly export sales for all commodities in config. Returns ------- dict…, Return the current USDA marketing year. Most grain marketing years start in…, Fetch weekly export sales for a single commodity. Parameters ----------…

### Community 51 - "PSD Fetcher"
Cohesion: 0.38
Nodes (7): fetch_psd_all(), fetch_psd_commodity_group(), _filter_psd(), DataFrame, Fetch oilseeds + coffee PSD data, filter to target…, Download a PSD bulk zip, extract the CSV, return raw DataFrame. Parameters…, Filter raw PSD data to just the commodities, countries, and attributes we…

### Community 52 - "Back-Contract Probe"
Cohesion: 0.43
Nodes (6): ContractReport, _longest_inner_bday_gap(), main(), probe(), DatetimeIndex, Phase 2 spike — yfinance back-contract availability probe. Standalone, read-…

### Community 53 - "DCE Section"
Cohesion: 0.40
Nodes (5): format(), DataFrame, DCE CHINESE FUTURES section — Dalian futures vs CBOT comparison., Read DCE futures data from SQLite., read_dce_futures()

### Community 54 - "USDA Section"
Cohesion: 0.50
Nodes (4): format(), USDA FUNDAMENTALS section — year-over-year production/yield., Read USDA data from SQLite., read_usda()

### Community 55 - "WASDE Section"
Cohesion: 0.50
Nodes (4): format(), WASDE ESTIMATES section — monthly USDA supply/demand forecasts., Read WASDE forecast data from SQLite., read_wasde()

### Community 56 - "Brazil Spot Cleaning"
Cohesion: 0.50
Nodes (5): clean_brazil_spot(), Clean CEPEA Brazil domestic spot price data. Steps: 1. Copy first. 2. Parse…, _make_brazil_spot(), test_clean_brazil_spot_does_not_mutate_input(), test_clean_brazil_spot_happy_path()

### Community 57 - "CONAB Cleaning"
Cohesion: 0.50
Nodes (5): clean_conab(), Clean CONAB Brazil crop estimate data. Steps: 1. Ensure value is numeric. 2.…, _make_conab(), test_clean_conab_does_not_mutate_input(), test_clean_conab_happy_path()

### Community 58 - "COT Cleaning"
Cohesion: 0.50
Nodes (5): clean_cot(), Clean COT (Commitment of Traders) data. Steps: 1. Ensure Date column is…, _make_cot(), test_clean_cot_does_not_mutate_input(), test_clean_cot_happy_path()

### Community 59 - "EIA Cleaning"
Cohesion: 0.50
Nodes (5): clean_eia(), Clean EIA energy data. Same pattern as clean_fred_series but for DataFrames: 1.…, _make_eia(), test_clean_eia_does_not_mutate_input(), test_clean_eia_happy_path()

### Community 60 - "Forward Curve Cleaning"
Cohesion: 0.50
Nodes (5): clean_forward_curve(), Clean forward curve data. Steps: 1. Ensure contract_month is a date string…, _make_forward_curve(), test_clean_forward_curve_does_not_mutate_input(), test_clean_forward_curve_happy_path()

### Community 61 - "India Cleaning"
Cohesion: 0.50
Nodes (5): clean_india_domestic(), Clean NCDEX India domestic price data. Steps: 1. Copy first (never mutate…, _make_india(), test_clean_india_domestic_does_not_mutate_input(), test_clean_india_domestic_happy_path()

### Community 62 - "Inspections Cleaning"
Cohesion: 0.50
Nodes (5): clean_inspections(), Clean export inspections data from AMS. Steps: 1. Ensure week_ending is…, _make_inspections(), test_clean_inspections_does_not_mutate_input(), test_clean_inspections_happy_path()

### Community 63 - "PSD Cleaning"
Cohesion: 0.50
Nodes (5): clean_psd(), Clean PSD (Production, Supply & Distribution) data. Steps: 1. Standardise…, _make_psd(), test_clean_psd_does_not_mutate_input(), test_clean_psd_happy_path()

### Community 64 - "SAFEX Cleaning"
Cohesion: 0.50
Nodes (5): clean_safex(), Clean JSE SAFEX South Africa settlement price data. Steps: 1. Copy first. 2.…, _make_safex(), test_clean_safex_does_not_mutate_input(), test_clean_safex_happy_path()

### Community 65 - "WASDE Cleaning"
Cohesion: 0.50
Nodes (5): clean_wasde(), Clean WASDE forecast data from USDA NASS. Steps: 1. Ensure year is a string. 2.…, _make_wasde(), test_clean_wasde_does_not_mutate_input(), test_clean_wasde_happy_path()

### Community 66 - "Weather Cleaning"
Cohesion: 0.50
Nodes (5): clean_weather(), Clean weather data from Open-Meteo. Steps: 1. Ensure Date column is datetime.…, _make_weather(), test_clean_weather_does_not_mutate_input(), test_clean_weather_happy_path()

### Community 67 - "World Bank Cleaning"
Cohesion: 0.50
Nodes (5): clean_worldbank(), Clean World Bank monthly price data. Steps: 1. Ensure Date column is datetime.…, _make_worldbank(), test_clean_worldbank_does_not_mutate_input(), test_clean_worldbank_happy_path()

### Community 68 - "CSV Data URIs"
Cohesion: 0.40
Nodes (5): _csv_data_uri(), DataFrame, Encode text as a base64 data URI for download links., Encode a DataFrame as a CSV data URI., _to_data_uri()

### Community 69 - "Crush & Roll Concepts"
Cohesion: 0.67
Nodes (3): Front-Month Roll-Day Discontinuity (Known Limitation), Soybean Crush Spread, Market Drivers Narrative

## Ambiguous Edges - Review These
- `Risk Monitor page` → `Layer 4 — COT Positioning (CFTC via cot_reports, commercials vs speculators)`  [AMBIGUOUS]
  docs/index.html · relation: conceptually_related_to

## Knowledge Gaps
- **13 isolated node(s):** `CI Workflow (lint + mypy + pytest)`, `Turso-or-SQLite Storage Abstraction`, `Briefing Package (orchestrator + 26 sections + BriefingData)`, `Market Drivers Narrative`, `USDA AMS Export Inspections Fixture (wa_gr101.txt)` (+8 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **3 thin communities (<3 nodes) omitted from report** — run `graphify query` to explore isolated nodes.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **What is the exact relationship between `Risk Monitor page` and `Layer 4 — COT Positioning (CFTC via cot_reports, commercials vs speculators)`?**
  _Edge tagged AMBIGUOUS (relation: conceptually_related_to) - confidence is low._
- **Why does `run()` connect `Freshness Markers` to `Config & DCE Fetch`, `WASDE Fetcher`, `SAFEX Fetcher`, `AgRural Scraper`, `USDA Fetcher`, `DB Upsert Writers`, `Freshness & Health`, `DB Save Functions`, `Price Read Tests`, `Turso Connection Layer`, `Save-Function Tests`, `Forward Curve Fetcher`, `FRED & Export Cleaning`, `OHLCV Cleaning`, `Data Cleaning Core`, `CONAB Fetcher`, `World Bank Pink Sheet`, `Database Store Init`, `Export Sales Fetcher`, `PSD Fetcher`, `Brazil Spot Cleaning`, `CONAB Cleaning`, `COT Cleaning`, `EIA Cleaning`, `Forward Curve Cleaning`, `Inspections Cleaning`, `PSD Cleaning`, `SAFEX Cleaning`, `WASDE Cleaning`, `Weather Cleaning`, `World Bank Cleaning`?**
  _High betweenness centrality (0.061) - this node is a cross-community bridge._
- **Why does `read_prices()` connect `Price Read Tests` to `Config & DCE Fetch`, `Freshness Markers`, `Seasonal Analysis`, `Emerging Markets`, `Dashboard Charts`, `Energy & Drivers Sections`, `Shared Data Loaders`, `Crop Conditions & Connection`, `Turso Connection Layer`, `Supply & Dashboard Build`, `Save-Function Tests`, `Turso Roundtrip Tests`?**
  _High betweenness centrality (0.056) - this node is a cross-community bridge._
- **Why does `run_health_check()` connect `Freshness & Health` to `Config & DCE Fetch`, `Freshness Markers`, `Dashboard Charts`, `Turso Connection Layer`, `Supply & Dashboard Build`?**
  _High betweenness centrality (0.045) - this node is a cross-community bridge._
- **What connects `CI Workflow (lint + mypy + pytest)`, `Turso-or-SQLite Storage Abstraction`, `Briefing Package (orchestrator + 26 sections + BriefingData)` to the rest of the system?**
  _13 weakly-connected nodes found - possible documentation gaps or missing edges._
- **Should `Signal Detection` be split into smaller, more focused modules?**
  _Cohesion score 0.06459627329192547 - nodes in this community are weakly interconnected._
- **Should `Basis & Crush Sections` be split into smaller, more focused modules?**
  _Cohesion score 0.07231638418079096 - nodes in this community are weakly interconnected._