# Latency

What the numbers on this site are worth in time: how old they are, which
part of that age we caused, and what we promise.

Every figure below is measured, and says where. Nothing here is an advertised
provider figure — those are the numbers this document exists to replace.

---

## 1. The decomposition

Four intervals. Three are measured end to end; one is declared, with its basis
recorded beside it.

| Interval | From → To | Whose |
|---|---|---|
| **acquisition** | observation → fetch completed | provider's delay **and** our cadence wait, inseparable by observing ourselves |
| **processing** | fetch completed → stored | ours |
| **publication** | stored → publicly readable | ours (analysis, generation, deploy) |
| **pipeline** | fetch completed → publicly readable | ours — `processing + publication`, the budget a code change moves |

Plus one declared quantity:

| | |
|---|---|
| **provider_delay** | the floor under `acquisition`, per layer, with a stated basis |
| **cadence_wait** | `acquisition − provider_delay`, floored at zero — the staleness *we chose*, by deciding how often to run |

That last subtraction is the point. A trader looking at a four-hour-old board
price cannot tell whether Yahoo was slow or we simply had not run yet, and the
two have completely different fixes: one is a provider problem nothing in this
repo can solve, the other is a cron line.

**Fetch-time provider delay vs display-time application delay.** The first is
`provider_delay` — imposed on us, quoted with its evidence, never inferred from
our own timings. The second is `cadence_wait + pipeline` — entirely ours, and
the only part any change here can move. Code is `latency/domain.py`;
`latency/measure.py` is the single DB-aware seam.

**Observation is an instant, not a date.** Almost every source publishes a
date. Calling a daily bar "0 days old" the moment the date rolls over would
understate it by most of a day, so each layer declares the venue-local hour its
observation for day D came into being (CBOT settlement 13:15 CT, the FX 17:00
New York rollover, AMS 3147 at ~13:48 CT). Where that hour is genuinely
unknown the layer is marked `DAY` granular and its age is a stated lower bound
— never converted to a false hour.

---

## 2. Measured baseline

### Provider side (live probes, 2026-08-19)

| Measurement | Result |
|---|---|
| Settled CBOT daily bar available on Yahoo | ZS=F carried a complete 2026-08-18 session (settled 18:15 UTC) at the 03:45 UTC probe → **≤ 9h30m**, one observation, no SLA published |
| Yahoo FX daily bar | The bar labelled D is **in progress until 17:00 New York on D**. Measured directly: `BRL=X` returned a bar labelled 2026-08-19 at 03:45 UTC with `High == Open` and `Low == Close` |
| Transient throttling | `ZL=F` returned empty once and succeeded 2.8 s later on retry; `ZS=F` failed outright on a 15-year request and succeeded on a 7-day one. `MAX_RETRIES = 3` covers it |

The working provider floor used to split `cadence_wait` out of `acquisition` is
**30 minutes** for Yahoo — deliberately tighter than the 9h30m bound, because
understating the provider's share attributes the remainder to us, which is the
conservative direction for judging our own schedule.

### Fetch cost (this machine, home broadband, 2026-08-19)

| Fetch | Time |
|---|---|
| yfinance, one ticker, `period="15y"` | **24–32 s** |
| yfinance, one ticker, `period="1mo"` | **1.1–2.0 s** |
| yfinance, one ticker, `period="5d"` | **1.8–3.2 s** |
| Layer 1 prices, 10 tickers @ 1mo | **3.0 s** |
| Layer 7 currencies, 10 pairs @ 1mo | **3.0 s** |
| Layer 11 forward curve, 9 commodities / ~50 contracts @ 5d | **42.8 s** |

The 15-year history pull is the dominant cost of the daily build and the entire
reason a fast path is possible: the same twenty tickers cost ~8 minutes at 15y
and ~6 seconds at 1mo.

### Pipeline and publication

| Stage | Time | Source |
|---|---|---|
| Full daily pipeline, all 32 layers | **6 m 02 s** | production CI 2026-08-18, first to last freshness stamp (13:53:22 → 13:59:24) in `data/history/data_freshness.csv` |
| Fast pipeline (3 layers, fetch + clean + store + history round-trip) | **51.2 s** | local, isolated DB copy |
| Site generation, 13 pages | **3.4–3.6 s** | local |
| Fast refresh end to end, both gates included | **53.1 s** | local, `scripts/refresh_prices.py` |
| GitHub Pages deploy (stored → publicly readable) | **not yet measured** | now instrumented — see §6 |

### Schedule

| | Cron | Observed landing |
|---|---|---|
| Daily build | `0 19 * * 1-5` | 20:00–24:00 UTC (workflow's own note: +64 to +298 min over 30 runs, median +138) |
| DCE refresh | `0 8 * * 1-5` | expected 09:04–12:58 UTC by the same delay envelope; not yet observed |
| Fast refresh | `30 21 * * 1-5` | 22:34–02:28 UTC |
| Catch-up refresh | `30 23 * * 1-5` | expected 00:34–04:28 UTC; a second shot for FX and the board on a day the 21:30 run failed |

---

## 3. Objectives

Per class, because their acceptable staleness differs by two orders of
magnitude. A weekly CFTC report four days old is on time; a board price four
days old is an outage.

| Class | acquisition | pipeline | end to end |
|---|---|---|---|
| **Board price** (CBOT, curve, DCE) | ≤ 6 h | ≤ 25 min | ≤ 6 h 25 m |
| **FX** | ≤ 6 h | ≤ 25 min | ≤ 6 h 25 m |
| **Physical origin** (CEPEA, AgRural, Gulf, MAGyP, SAFEX, mandi, EC) | ≤ 8 h | ≤ 25 min | ≤ 8 h 25 m |
| **Fundamentals** (USDA, CFTC, FAS, EIA, GTR, FRED) | ≤ 24 h | ≤ 25 min | ≤ 24 h 25 m |
| **Weather** | ≤ 12 h | ≤ 25 min | ≤ 12 h 25 m |

Each carries a `basis` string in `latency/domain.py` explaining how it was
arrived at, and a test asserts none of them is empty — a target with no basis
is a number somebody made up.

Two of these deserve their reasoning stated here:

- **Board price is 6 h, not 4 h.** The settlement guard puts a hard floor at
  ~1h15m (14:30 CT cutoff after a 13:15 CT settlement). Four hours would have
  been the tightest achievable and would have been missed on roughly a third of
  runs — not for anything in this code, but because GitHub's scheduler varies by
  nearly four hours. A target missed that often is one readers learn to ignore.
- **One pipeline budget for all five classes.** The work after a fetch does not
  know what class the number belongs to: the same clean, store, generate and
  deploy path carries all of them. Five copies of one fact would be five things
  to keep in step.

---

## 4. The fast refresh

`python main.py --fast` — `config.FAST_REFRESH_LAYERS` (`prices`, `currencies`,
`forward_curve`) over `FAST_REFRESH_HISTORY_PERIOD` (`1mo`) instead of all 32
layers over `15y`.

It is the **same code** as the daily build with two arguments different, not a
second pipeline. The settlement guard, the cleaners, the `LAYER_MIN_KEYS`
floor, the recency gate and the freshness grading are all unchanged and
unbypassable on this route.

**What is in it, and what is not.** The three layers that move intraday and
that everything else prices off — FX is in because every `home_per_mt` leg on
the site converts through it, so a stale rate is a stale landed cost on every
physical origin, not merely a stale FX cell. Deliberately excluded:

- **DCE** — closes 15:00 CST = 07:00 UTC, hours before either evening slot,
  so it gets its own morning run instead: the 08:00 UTC cron passes
  `--layers dce` and fetches nothing else. Without it the only DCE fetch was
  the evening daily build, 13–17h after the close — a guaranteed daily breach
  of the 6h board objective. Probed live 2026-08-19: Sina's daily endpoint
  carried the day's settled bar by evening CST and did **not** emit a
  next-trade-date row from the 21:00 CST night session, so a late landing
  risks only a breach verdict, never a partial bar. When the first row
  appears after the 15:00 CST close is not yet measured; an early landing
  that misses it stores D−1, passes the gates, and reads as a dce breach in
  the latency gate — the signal to shift the cron later.
- **The scraped physical legs** (CEPEA, AgRural, Gulf bids, MAGyP, mandi) —
  they publish once a day, and doubling the request rate on unfriendly
  upstreams trades reliability for freshness that is not there. The 2026-08-11
  data.gov.in throttle blackout is the standing example.

**The latency gate.** Every refresh slot ends by reading the published
edition's `manifest.json` through `scripts/latency_report.py
--fail-on-breach-layers <the layers that slot fetched>` — a breach in a layer
the run was responsible for is a red run, not a log line. The gate is scoped
on purpose: an unscoped `--fail-on-breach` would be red every day on layers
whose acquisition breaches structurally (COT carries a 3-day provider delay
against a 24h target; the evening daily build re-stamps `dce` 13–17h after
the Dalian close, overwriting the morning run's on-time stamps until the next
morning).

The forward curve is 43 of the 53 seconds. It stays in because a hedger reads
the curve as a board price, and 53 s is still an order of magnitude under the
daily build.

---

## 5. Why a failed fast refresh cannot replace the last trusted edition

A fast refresh fetches three layers and inherits the other twenty-six from
whatever database it is sitting on. On a fresh CI runner that database is
seeded only from `data/history/*.csv`, which carries the snapshot-only tables
and nothing else — so an unseeded fast refresh would render a **complete,
structurally valid, internally consistent site with PSD, weather, COT and crop
progress simply gone**. Every block would show a legal empty state naming its
reason. Every page would pass the existing promotion contract. Nothing in the
HTML would say the edition knows less than the one it is replacing.

No amount of checking a candidate against itself finds that. So there are two
gates, and the second is new:

1. `trust.site_promotion.verify_site_candidate` — is this candidate internally
   sound? (unchanged)
2. `trust.site_promotion.verify_refresh_is_not_a_regression` — does it know at
   least as much as the edition it would replace? It compares the candidate's
   `manifest.json` against the published one: no layer's observation may go
   backwards or vanish, and no page that rendered may tombstone.

Failure is a **no-op**. `scripts/refresh_prices.py` exits non-zero before the
Pages upload step, so the live site is untouched. Exit codes are distinct so CI
can tell the four failure kinds apart: `1` pipeline, `2` generation, `3`
promotion contract, `4` regression.

Three supporting decisions:

- A **missing candidate** manifest is a refusal. Being unable to make the
  comparison is being unable to clear it.
- A **missing published** manifest passes, loudly. The first build after this
  ships has nothing to compare against, and blocking there would wedge the
  deploy permanently.
- A layer whose run **failed but whose observation is unchanged** is not a
  regression. `last_success` is preserved by design and the surfaces already
  report the failure; refusing here would let one flaky upstream freeze the
  site.

The daily build caches its populated database (`actions/cache`), and the
refresh restores it. A cache miss is not a failure — the refresh runs, is
refused by gate 2, and the live site stays as it was.

> **Ramp-up caveat.** Gate 2 compares `observed_at`, which only exists on
> freshness rows written by an instrumented run. Until the first full daily
> build after this ships, most layers carry NULL on both sides and the gate has
> less to compare. It can only under-refuse, never over-refuse, and it becomes
> fully effective after that first build.

---

## 6. What is exposed

| Surface | Shows |
|---|---|
| Masthead (every page) | generation time, plus **"Board and FX priced from data N old"** — the worst board or FX observation age, because generation time alone reads as observation time and on this site they are routinely a day apart |
| Layer Freshness table | **Observed**, **Fetched**, **Age** and **Last Success** as four separate columns. "Last Success" answers *when we ran*; "Observed" answers *what the number is dated*. One column alone read "0h ago" for a day-old price |
| `docs/manifest.json` | the machine-readable edition record: mode, generation time, per-layer coverage, and the full latency chain with every verdict and basis string |
| `scripts/latency_report.py` | operator report from the live DB, or from a published manifest with `--manifest URL`; `--fail-on-breach` for CI |
| `scripts/smoke_site.py --url` | prints `PUBLICATION LATENCY` — generation → publicly readable, the one interval no process can observe from inside itself. Reported, never failed on: a slow Pages deploy is an operational fact, not a reason to alert on a site that published correctly |

---

## 7. Operational cost

| | Daily build | Fast refresh |
|---|---|---|
| Runs per weekday | 1 (+ pushes to `main`) | 1 |
| Layers fetched | 29 | 3 |
| Measured runtime | ~6 min pipeline + ~4 s generation | ~53 s total |
| Billed runner minutes | ~10–15 (incl. CI, smoke, deploy) | ~4–6 |
| Upstream requests added | — | 20 yfinance tickers + ~50 curve contracts |

**~5 extra runner-minutes per weekday, ~110/month.** Public repositories on
GitHub Actions have no minute charge, so the real cost is upstream request
volume, and it is concentrated on Yahoo — which is not rate-limit-fragile in
the way `data.gov.in` is, and is why no scraped layer was added to the fast
path. The database cache is ~19 MB against a 10 GB repository allowance.

---

## 8. A defect this work found and fixed

Measuring the provider surfaced a live bug rather than confirming a design.

At 03:45 UTC on 2026-08-19 — 22:45 Chicago, 23:45 New York — `BRL=X` returned a
bar labelled 2026-08-19 with `High == Open` and `Low == Close`: an FX day under
four hours old. Chicago local time was past the 14:30 CT cutoff, so the
settlement guard declared the session settled and **stored that partial bar as
the day's FX close**. Every `home_per_mt` leg on the site converts at that
row's own date, so it was a wrong landed cost on every physical origin, not
merely a wrong FX cell.

The guard asked one question — *has Chicago settled?* — and answered it for
spot FX, which has no settlement at all. The same root cause broke the futures
side in the other direction: the guard dropped rows *equal to* the Chicago
date, so once the CME overnight session opened at 19:00 CT carrying the **next**
trade date, the bar it produced compared unequal and survived.

`fetchers/_settlement.py` now asks **which session date is the newest one that
has finished**, and drops every row labelled after it. Two `SessionRule`
instances, one function: `EXCHANGE_SESSION` (14:30 America/Chicago) and
`FX_SESSION` (17:00 America/New_York). Both holes close with one comparison.

Confirmed fixed against live data the same evening: a fast refresh at 04:27 UTC
stored FX and board prices for the **2026-08-18** session and logged the
2026-08-19 overnight curve legs being dropped. Pinned by
`tests/test_settlement_guard.py`.

---

## 9. The product's latency

Not the provider's. This is what a reader gets.

**Board price (CBOT, forward curve).** A settlement at 18:15 UTC is publicly
readable in **1 h 45 m at best, 5 h 45 m typical worst case** via the daily
build, with the 21:30 UTC refresh as a backstop. Never fresher than 1 h 15 m,
because the settlement guard will not publish an unfinished bar. Meets the 6 h
objective on the observed scheduler envelope.

**FX.** The 17:00 New York close is publicly readable in **0 h 34 m to 5 h 28 m**
via the fast refresh — where before this work it was **either D−1 or, worse,
a partial bar mislabelled as the close**. Meets the 6 h objective.

**Physical origin legs.** Fetched once a day by the daily build. AMS Gulf bids
(~18:48 UTC) and Argentina MAGyP (~15:05 UTC) are same-day, within about
2–6 h. **CEPEA publishes after 21:01 UTC and is therefore D−1 on most days**,
about 24–30 h — deliberately, because re-scraping it more often is a
reliability trade we declined. Stated, not hidden.

**Fundamentals** (USDA, CFTC, FAS, EIA, GTR, FRED). Within one build cycle of
release: under 24 h.

**Weather.** Under 12 h for observed rows; forecast rows have negative age by
construction, and the measurement reports the negative rather than clamping it.

**The whole product.** Excluding CEPEA, **a number observed anywhere in the
world is publicly readable on this site within six and a half hours, and the
share of that we control — fetch to public page — is under twenty-five
minutes.** CEPEA is the one named exception at D−1.

Two things this is not. It is not real-time and no part of it is: the fastest
possible path from a CBOT settlement to a reader is 1 h 15 m, floored by the
settlement guard, and nothing here should be used for execution. And it is not
a claim about a *provider*: `pricing.semantics.PROVEN_SETTLEMENT_SOURCES` is
still empty, so every board number remains a delayed close, never a settlement,
however fast it arrives.
