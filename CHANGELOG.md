# Changelog

Format: human-readable summaries grouped by "run" — a discrete refactor or
feature push. Each run notes the why, the user-visible behaviour change (if
any), and the test/coverage impact.

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
