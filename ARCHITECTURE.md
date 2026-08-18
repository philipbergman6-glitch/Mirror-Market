# Architecture

Single page. The goal is for a new contributor to read this once and
know which file to open for any given task.

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
the freshness table.

### Clean (`pipeline/clean.py`)

Normalises raw frames: parses `Date` to datetime, sets index, forward-fills
gaps with `limit=3`, drops all-NaN rows, warns on >10% daily moves and
zero/negative volume. Returns copies — originals are never mutated.

### Store (`pipeline/store.py` + `schema.py`)

Tables are defined in `pipeline/schema.py` as `CREATE TABLE IF NOT EXISTS`
strings. `pipeline/store.py` exposes `save_*()` functions that batch-upsert
via `executemany`. `pipeline/connection.py` returns a Turso cloud
connection when `TURSO_DATABASE_URL` is set, a local SQLite connection
otherwise — call sites don't care which.

### Analyze (`analysis/`)

Pure functions over the DataFrames returned by `pipeline/query.read_*()`:
technicals, signals, spreads, correlations, seasonality, forward-curve
shape. Two consumer layers sit on top of these primitives:

* `analysis/briefing/` — daily text briefing. Orchestrator joins ~23
  section modules into one text block + a typed `BriefingData`.
* `analysis/soy_analytics.py` — 9 analyst functions that produce
  page-shaped dicts for the dashboard.

Both consumers pull price/currency frames from `analysis/loaders.py`
(shared, cached) to stay in sync.

### Render (`scripts/generate_html.py` + `app/`)

`scripts/generate_site.py` renders the headline, Players, and eight market
URLs into a private candidate directory. It calls analyst functions, builds
Plotly figures via `app/charts.py`, and embeds the archived daily text briefing
in the headline. A page failure may create a dated tombstone inside that
candidate for diagnosis, but the candidate is not public yet.

### Verify and promote (`trust/site_promotion.py` + `scripts/smoke_site.py`)

Normal publication uses the existing candidate/verification/promotion seam.
The v1 bridge verifies the complete static candidate while named-contract v2
coverage continues to expand: all expected URLs and links, current core soy
benchmarks, briefing presence, aligned crush inputs, no tombstones, valid
generation/observation timestamps, the authoritative 27-layer count, and
desktop/mobile viewport fit. Only a verified candidate is uploaded. If render
or verification fails, GitHub Pages is not called and the previous trustworthy
edition remains public. After deployment, the same smoke contract reads the
real Pages URLs; alerting identifies page-generation, contract, deployment,
and post-deployment failures separately.

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
`pipeline/units.to_metric_tons()` and `pipeline/units.mt_label()`.

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
| A new threshold (RSI level, weather)     | Constant in `config.py` — never inline |
