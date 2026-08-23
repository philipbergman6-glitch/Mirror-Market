# CLAUDE.md

Guidance for Claude Code working in this repository.

## Mission

**Public-data soy intelligence and private desk decision support for physical buyers making daily cargo, basis, origin, and hedge decisions.**

That sentence is canonical (map #296). Everything below narrows it; nothing widens it.

The soy complex (beans, oil, meal) priced across every venue that matters to a cargo: CBOT, Dalian, Brazil, Argentina, India, Europe, South Africa, Nigeria. Competing crops (palm, rapeseed, sunflower, corn) are carried *because* soy buyers price against them, not as scope creep.

**Primary users: physical buyers, working at a daily cargo/basis cadence.** They open this before the desk day to see what a cargo is worth, where it moves cheapest, and who has repriced — then act on it elsewhere. The cadence is the product: a daily refresh with honest timestamps, not a screen that ticks.

**What this product does not claim to be.** Each of these is a real capability that real traders use, deliberately out of scope — say so plainly rather than half-building it:

| Not | Because |
|---|---|
| An **execution terminal** | Nothing here is orderable. No blotter, no ladder, no executable quote state. |
| A **real-time market feed** [^tv] | Measured: the fastest possible CBOT-settlement-to-reader path is **1 h 15 m**, floored by the settlement guard (`LATENCY.md` §9). No part of this is intraday-authoritative. |
| A **CTRM** | No contract lifecycle: no L/Cs, GAFTA/FOSFA clause handling, washouts, arbitration, or hedge accounting. |
| An **authoritative settlement service** | `PROVEN_SETTLEMENT_SOURCES` is empty by design — see invariant 3. Nothing rendered is a settlement. |
| A **live counterparty market** | No firm bids/offers, no counterparty terms, no broker network. Desk-entered records are one desk's notes, not a market. |

[^tv]: **One labelled exception, decided 2026-08-23 ([#320](https://github.com/philipbergman6-glitch/Mirror-Market/issues/320)), narrowed the same day by [#328](https://github.com/philipbergman6-glitch/Mirror-Market/issues/328).** A workstation contract row expands into a panel of two territories (`DESIGN.md` → "Contract-row chart panel"): our own close-history chart for that contract month, drawn from this project's stored snapshots on our timestamps, above **links out** to the same contract on TradingView's and Barchart's own sites. Nothing third-party renders on this site — the embedded TradingView widget #320 originally approved was removed by #328 after their free embed proved to refuse CME symbols on any plan — so the exception is now only that the page points at intraday-capable third-party pages. What a reader sees after clicking is theirs, on their pages: not our observation, not a settlement, and invariant 3 is untouched because `pricing.semantics` never sees it. This narrows nothing else in this table — the product still has no order path, no CTRM lifecycle, and no settlement authority.

The Bloomberg-class ambition is not abandoned, it is **deferred and priced**: `ROADMAP.md` records it as a commercially-triggered expansion track. It is not the standard this repo is graded against, and it is not a reason to build toward it speculatively.

The build: 32 operational data layers across 28 numbered groups (fetch → clean/validate → store, SQLite) → analysis → a 13-page static site on GitHub Pages + a daily briefing. `config.PRODUCTION_LAYERS` is the authoritative roster — count from it, never from prose. All prices display in **USD/MT**. Private desk editions (opportunity board, workstation book) are written to `data/workspace/`, outside `docs/`, and are never published.

## Reference docs — read before touching the relevant area

- **`LAYERS.md`** — all 32 data layers: units, cadence, API keys, and every source's known traps. **Mandatory before editing any fetcher.**
- **`ARCHITECTURE.md`** — pipeline, analysis, storage, history persistence, site contract, and the product phases (origins, workstation, desk workflow, opportunities, crush, price semantics, trust ledger, latency, fast refresh).
- **`DESIGN.md`** — all visual/UI decisions. Read before any visual change; never deviate without explicit user approval. In QA mode, flag code that doesn't match it.
- **`LATENCY.md`** — the data-age vocabulary and objectives.
- **`ROADMAP.md`** — what is deliberately deferred, and the commercial trigger that would reopen it.
- `data/reference/*/README.md` — desk-entered files (positions, options, clearing, assumptions, players).

## Commands

```bash
pip install -r requirements.txt          # runtime (.venv, Python 3.10+)
pip install -r requirements-dev.txt      # tests, lint, type-check

python main.py                           # full pipeline (all layers)
python main.py --fast                    # prices/FX/curve only, ~1 min

python -m analysis.briefing              # daily market briefing

python scripts/generate_site.py          # whole static site → docs/
python scripts/generate_site.py --only cbot   # one page (headline | players |
                                         # origins | workstation | opportunities | <slug>)
```

## Environment variables

`USDA_API_KEY`, `FRED_API_KEY`, `FAS_API_KEY`, `EIA_API_KEY`, `DATA_GOV_IN_API_KEY` (required in CI; degraded fallback locally). Most layers need no key. Details, degraded modes, and the User-Agent trap: `LAYERS.md` → "API keys".

## Invariants — never break these

1. **Silent failures are worse than crashes.** Hard-fail on invalid/ambiguous input. Save first, grade second: rows are stored, but only a fully-answered, recent, complete fetch stamps `last_success`. `stale` ≠ `failed` ≠ `no_publication` ≠ skipped-unconfigured — each is a distinct state with a distinct meaning.
2. **Nothing is invented.** NULL means "never learned", 0 means "asked and got nothing", a blank is never a zero, absence never becomes an assumption. Withhold with a reason rather than patch, pad, or substitute.
3. **Price semantics are one shared vocabulary** (`pricing/semantics.py` + `policy.py`). Nothing here is a settlement — `PROVEN_SETTLEMENT_SOURCES` is empty; confidence is derived from price type, never asserted. Don't create parallel vocabularies or over-claim what a number is.
4. **The privacy boundary is structural.** Client records (positions, options, clearing, desk workflow) are gitignored files, never DB tables; private editions go to `data/workspace/`, never `docs/`; public is the only legal default audience. Every table round-trips through committed CSVs, so a client-record table would publish the book by construction.
5. **The market is a parameter, never a code path.** Per-market variation lives in the `config.MARKETS` registry descriptor. `if market == "india"` in a builder is the exact drift the contract exists to prevent.
6. **No cloud DB.** CI persistence is git-committed CSVs in `data/history/` (decision 2026-07-30; do not reintroduce Turso/Supabase as a CI requirement). **Never edit `data/history/` in a PR** — the `history-guard` CI job fails it; only the deploy workflow writes there.
7. **Unit conversion has two sites only**: `pipeline/units.py` and `Source.to_usd_mt`. A `home_per_mt` leg converts at *that row's own date's* FX rate or renders blank. Labelling has one site too — `Source.quote_unit`: a leg carries the **venue's** unit, never the market's `home_currency`, and a `usd_per_mt` leg has no home-currency line at all because there is no second observation to print (#230).
8. **Spreads and crushes are one session's number.** All legs struck on a session where every leg printed — no cross-day arithmetic, ever.
9. **Licensing gates publishing, not building.** MATIF futures and JSE MTM are licence-blocked because this project *publishes*; internal use of the same data would be fine. Check rights before adding a rendered source.
10. **Rotating-URL trap** (World Bank, CIRCABC, SAGIS): resolve download links from the landing page each run — a stale deep link serves frozen data at HTTP 200. Fixed-URL sources need a `LAYER_MAX_DATA_AGE_DAYS` budget for the same reason, from the other side.
11. **A wrong number is worse than a gap.** The settlement guard drops unfinished bars; a run before the venue close publishes D−1. That trade is intended.

## Key patterns

- Fetchers return `dict[str, pd.DataFrame]`; cleaners return copies, never mutate.
- `INSERT OR REPLACE` upserts — the pipeline is safe to re-run.
- Analysis expects DataFrames with a `Close` column and DatetimeIndex.
- Every block/section builder returns a `{state, reason, data}` envelope; a non-`ok` state must carry a reason (enforced by type).
- Signal severity: `alert` > `warning` > `info`.
- Thresholds live in `config.py`; v2 source policy in the `trust.registry` contract registry.
- Technicals never compute on raw front-month `Close`: `analysis.loaders.enrich_with_technicals` is the one seam — named ratio-adjusted series first, labelled provider fallback second, near-roll signals suppressed on the fallback. See `LAYERS.md` → roll-day discontinuities.
- Tests are sandboxed by `tests/_guards.py`: a write inside `data/history/` and any outbound connect both raise. Point `pipeline.history.HISTORY_DIR` at `tmp_path`, stub the fetcher, or mark a test `@pytest.mark.network` if it genuinely needs the internet.
