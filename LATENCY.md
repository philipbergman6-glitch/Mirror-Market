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
observation for day D came into being (CBOT settlement 13:15 CT, the London
midnight that ends an FX bar's day, AMS 3147 at ~13:48 CT). Where that hour is genuinely
unknown the layer is marked `DAY` granular and its age is a stated lower bound
— never converted to a false hour.

---

## 2. Measured baseline

### Provider side (live probes, 2026-08-19)

| Measurement | Result |
|---|---|
| Settled CBOT daily bar available on Yahoo | **At 00:00 New York on D+1** — 04:00 UTC in EDT, 05:00 UTC in EST — **9h45m after the 13:15 CT settlement** (B47 #622). Before it the row labelled D holds the last trade, then the overnight session's trades. Measured over all 89 committed snapshots of `contract_bars`: 1,912 CBOT/CME values last served before that midnight were later changed, 0 of 27,835 served after it. The first entry here — "ZS=F carried a complete 2026-08-18 session at the 03:45 UTC probe, ≤ 9h30m" — read a live bar: 03:45 UTC was 23:45 in New York |
| Yahoo FX daily bar | The bar labelled D is the **London calendar day D**, in progress until London midnight ending D — **23:00 UTC in BST, 00:00 UTC in GMT**. Chart API, 2026-10-13 and 2026-10-18, `BRLUSD=X` `ZARUSD=X` `CNYUSD=X` `INRUSD=X`: `exchangeTimezoneName: Europe/London`, `currentTradingPeriod.regular` 23:00Z→22:59Z (00:00→23:59 London), bars stamped 23:00Z, which yfinance labels with the London date (2026-10-15T23:00Z → `2026-10-16`). A year of history carries Monday–Friday labels only. First recorded here (2026-08-19) as a 17:00 New York session — wrong, B20 #482. The original partial-bar observation stands: `BRL=X` returned a bar labelled 2026-08-19 at 03:45 UTC with `High == Open` and `Low == Close` |
| Transient throttling | `ZL=F` returned empty once and succeeded 2.8 s later on retry; `ZS=F` failed outright on a 15-year request and succeeded on a 7-day one. `MAX_RETRIES = 3` covers it |

The working provider floor used to split `cadence_wait` out of `acquisition` is
**30 minutes** for the Yahoo **FX** bar after London midnight — deliberately
tight, because understating the provider's share attributes the remainder to
us, which is the conservative direction for judging our own schedule. The
board's provider delay is no longer a working figure: since B47 (#622) it is
the measured 9h45m above, pinned to the guard's rule by `tests/test_latency.py`.

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
| Full daily pipeline (that day's roster: 32 layers) | **6 m 02 s** | production CI 2026-08-18, first to last freshness stamp (13:53:22 → 13:59:24) in `data/history/data_freshness.csv` |
| Fast pipeline (3 layers, fetch + clean + store + history round-trip) | **51.2 s** | local, isolated DB copy |
| Site generation, 13 pages | **3.4–3.6 s** | local |
| Fast refresh end to end, both gates included | **53.1 s** | local, `scripts/refresh_prices.py` |
| GitHub Pages deploy (stored → publicly readable) | **not yet measured** | now instrumented — see §6 |

### Schedule

| | Cron | Observed landing |
|---|---|---|
| Daily build | `0 19 * * 1-5` | ~21:10 to ~04:10 UTC: +130 to +549 min over the 61 scheduled runs since 2026-09-01, median +227; +224 to +549, median +304, over the 22 since 10-26 (re-measured 2026-11-25, B47 #622; first measured +64 to +298 over 30 runs). Every one landed before Yahoo finishes the CBOT bar, so under the corrected guard all 61 carry CBOT D−1 |
| DCE refresh | none — `workflow_dispatch` with `layers=dce`, fired at **10:30 UTC** weekdays by cron-job.org | on time by construction; GitHub's scheduler is not in the path. Also the **board backstop**: on a Tue–Fri where no `board` dispatch delivered since 00:00 New York, it fetches `prices,forward_curve` too and goes red on its own step (§4) |
| **Board refresh** (#622) | none — `workflow_dispatch` with `slot=board`, fired at **01:00 America/New_York, Tue–Sat** by cron-job.org (job timezone `America/New_York`, so DST is handled there) | 05:00 UTC (EDT) / 06:00 UTC (EST), on time by construction; 1 h after Yahoo finishes the CBOT bar. **Not yet operational**: the cron-job.org job is created by `scripts/setup_cbot_board_trigger.sh` after merge |
| DCE backstop | `0 8 * * 1-5` | 13:15–16:38 UTC (+5h15 to +8h38, seven runs 2026-09-21 to 09-29) — past the 13:00 UTC deadline every time, which is why it is only a backstop |
| Fast refresh | `7 20 * * 1-5` | 22:01–23:42 UTC expected, from the +1h54 to +3h35 delay this slot showed over thirteen runs, 2026-09-11 to 09-29, while it was still `30 21`. Before the London FX close in GMT always, in BST mostly: carries D−1 FX and, since B47 #622, D−1 CBOT; its latency gate covers nothing — the board is proven on the `board` slot |
| Catch-up refresh | `30 23 * * 1-5` | 01:11–02:38 UTC; past the London FX close in both seasons, so **the slot where FX day D first arrives** and the one whose gate covers FX. For CBOT it still lands before New York midnight on nearly every run (3 of 46 evening refreshes since 10-26 landed after it) |

---

## 3. Objectives

Per class, because their acceptable staleness differs by two orders of
magnitude. A weekly CFTC report four days old is on time; a board price four
days old is an outage.

| Class | acquisition | pipeline | end to end |
|---|---|---|---|
| **Board price** (CBOT, curve, DCE) | ≤ 12 h | ≤ 25 min | ≤ 12 h 25 m |
| **FX** | ≤ 6 h | ≤ 25 min | ≤ 6 h 25 m |
| **Physical origin** (CEPEA, AgRural, Gulf, MAGyP, SAFEX, mandi, EC) | ≤ 8 h | ≤ 25 min | ≤ 8 h 25 m |
| **Fundamentals** (USDA, CFTC, FAS, EIA, GTR, FRED) | ≤ 24 h | ≤ 25 min | ≤ 24 h 25 m |
| **Weather** | ≤ 12 h | ≤ 25 min | ≤ 12 h 25 m |

Each carries a `basis` string in `latency/domain.py` explaining how it was
arrived at, and a test asserts none of them is empty — a target with no basis
is a number somebody made up.

Two of these deserve their reasoning stated here:

- **Board price is 12 h** (owner decision on #622, 2026-11-25, Option A). It
  was 6 h, set when the guard's floor was ~1h15m (a 14:30 CT cutoff after a
  13:15 CT settlement) plus GitHub's scheduler spread. B47 (#622) measured that
  cutoff wrong: Yahoo finishes the CBOT bar at 00:00 New York on D+1, so the
  floor for `prices` and `forward_curve` is **9h45m**, above 6 h — a breach on
  every run. The board now has its own slot, a cron-job.org dispatch at
  **01:00 New York, Tue–Sat**: 10h45m after the settlement, ~10h50m once the
  run has started and fetched. Twelve hours is that plus ~70 minutes for a
  queued Pages deploy or a late dispatch; the 10:30 UTC backstop (~16 h) still
  breaches, as it must. **Why 01:00 and not 00:15:** NY-midnight finality is
  inferred from committed captures, few of which landed in the first minutes
  after midnight, and a slot fifteen minutes past a thinly sampled boundary is
  where the rule could print a wrong number every day.
  `scripts/measure_cbot_bar_finality.py` samples ZS/ZL/ZM at 23:50, 00:00,
  00:05, 00:15, 00:30 and 01:00 New York; only a week with no Close change
  after 00:00 moves the slot to 00:15 (and lets this target tighten to about
  11 h). If the board job misses more than about 1 run in 20, the fallback is
  to fire the daily build itself from cron-job.org after New York midnight.
  **DCE shares the class**, so it is held to 12 h as well: its 10:30 UTC
  dispatch lands ~3h30m after the 15:00 CST close, far inside either number.
  The one thing the 6 h number did for DCE — turning the 08:00 backstop's
  13:15–16:38 UTC landing red — the backstop now does explicitly (§4).
  `tests/test_refresh_slot.py` checks the fire time against this target for
  every weekday of 2026 in both seasons.
- **One pipeline budget for all five classes.** The work after a fetch does not
  know what class the number belongs to: the same clean, store, generate and
  deploy path carries all of them. Five copies of one fact would be five things
  to keep in step.

**A fourth verdict: `closed`** (2026-10-07, #399). A board-price objective
counts hours after a session's close; on a day the venue held no session there
is nothing to be late for, and the newest bar we hold is the newest bar that
exists. Golden Week 2026 showed the gap: every scoped DCE refresh from 2 to 7
October was red on correct data. So a venue may carry a declared closure
calendar (`latency/calendars.py`, DCE 2026 from the exchange's own notice), and
an acquisition breach becomes `closed` only when all three hold: the calendar
covers the days in question, the observed bar is the newest session that had
closed by the fetch, and at least one declared weekday holiday lies between
them. Weekends are not closures (every schedule already runs around them); a
pipeline breach is ours and is never excused; a year with no calendar entered
falls back to `breaches`, because an unknown calendar must not excuse anything.
The gate (`--fail-on-breach-layers`) does not fail on `closed`, and both reports
print the layer with the calendar's source line. Entering the next year's
notice is a maintenance act: the DCE publishes it each December.

---

## 4. The fast refresh

`python main.py --fast` — `config.FAST_REFRESH_LAYERS` (`prices`, `currencies`,
`forward_curve`) over `FAST_REFRESH_HISTORY_PERIOD` (`1mo`) instead of all 41
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
  so it gets its own morning run instead, which passes `--layers dce` and
  fetches nothing else. Without it the only DCE fetch was the evening daily
  build, 13–17h after the close — a guaranteed daily breach of the board
  objective as it then stood (6 h; 12 h since #622). That run is
  **dispatched from outside GitHub**, not scheduled inside it: the fetch had
  to land in 07:00–13:00 UTC, and the `0 8` cron
  was started 13:15–16:38 UTC on all seven weekdays from 2026-09-21 to
  09-29 — a delay wider than the window, so no cron time fixes it.
  cron-job.org calls the `workflow_dispatch` API with `layers=dce` at
  **10:30 UTC** on weekdays, using a fine-grained token that can do nothing
  but read and trigger this repository's Actions. The `0 8` cron stays as
  the backstop and as the alarm on that outside dependency: it looks for a
  successful `Refresh Prices (dce)` run today, stands down if there is one
  (a second fetch hours later would re-stamp an on-time number as a late
  one), and otherwise fetches DCE itself and goes red on the breach —
  which says, correctly, that the outside trigger did not fire.
  Probed live 2026-08-19: Sina's daily endpoint carried the day's settled
  bar by evening CST and did **not** emit a next-trade-date row from the
  21:00 CST night session, so a late landing risks only a breach verdict,
  never a partial bar. **Why 10:30 and not straight after the close:**
  measured 2026-09-30, polling all eight contracts every three to five
  minutes, the first day-D row appeared between 08:11 and 08:16 UTC, and
  until 08:57 the endpoint served day D and day D−1 for the same contract
  on alternating requests; every contract carried day D on every poll from
  09:00 UTC. That is one day's measurement. A fetch inside that window
  stores a mixed set — some contracts D, some D−1 — which the gates pass,
  because the layer's `observed_at` is the newest date the run received.
  10:30 is ninety minutes clear of it and two and a half hours inside the
  13:00 UTC deadline.
- **The scraped physical legs** (CEPEA, AgRural, Gulf bids, MAGyP, mandi) —
  they publish once a day, and doubling the request rate on unfriendly
  upstreams trades reliability for freshness that is not there. The 2026-08-11
  data.gov.in throttle blackout is the standing example.

**The board slot** (owner decision on #622, 2026-11-25). Yahoo's CBOT bar
labelled D is final only at 00:00 New York on D+1, and every GitHub-scheduled
slot lands before that on nearly every run, so the board gets its own
dispatch from outside GitHub, like DCE: cron-job.org calls the
`workflow_dispatch` API with `slot=board` at **01:00 New York, Tuesday to
Saturday** (Monday's bar finishes on Tuesday, Friday's on Saturday). **The
cron-job.org job must be scheduled in the `America/New_York` timezone**, not
UTC: the bar finishes at New York midnight, so the trigger follows New York's
clock changes, landing at 05:00 UTC in EDT and 06:00 UTC in EST. The run is
titled `Refresh Prices (board)`, fetches `prices,forward_curve` only and gates
on exactly those against the 12 h objective (§3). It does not refetch
`currencies`: FX day D already arrived on the 23:30 slot, and a second fetch
hours later would re-stamp an on-time number as a later one. The routing is
`scripts/refresh_slot.py`, tested in `tests/test_refresh_slot.py`.

- **Backstop.** The 10:30 UTC dce dispatch is also the board's backstop, the
  same pattern the 08:00 cron is for DCE. On a New York Tue–Sat at least one
  hour past 01:00, if no `Refresh Prices (board)` run *delivered* since 00:00
  New York, it fetches `dce,prices,forward_curve`, gates dce as usual, and a
  separate final step, **Gate the board backstop**, prints the board report and
  goes red with `CBOT board not delivered on time` — the trigger did not
  fire, or its run failed its own gate (a board run after a CBOT holiday
  does, until CBOT has a calendar: one red for the breach, one for the
  backstop). "Delivered" means the run's
  own `Gate the published edition's latency` step succeeded — not the run's
  conclusion — so a dce run that was red only on the board half still counts
  as dce delivered, and the 08:00 backstop does not refetch dce late.
  Saturday has no 10:30 run, so a Saturday miss is not backstopped: Friday's
  session then waits for Monday evening's daily build. The red Saturday run
  (or cron-job.org's failure e-mail) is the alarm.
- **Set up and diagnosis.** `scripts/setup_cbot_board_trigger.sh` (a wizard:
  token, API key, creates the job through cron-job.org's API so the timezone
  cannot be mistyped) runs once after merge; it refuses to start until main
  has the `slot` input, because GitHub answers an unknown input with 422.
  `scripts/check_cbot_board_trigger.sh` reads the job and its execution
  history, rotates the token and re-saves the canonical job.
- **Known exposures.** CBOT has no closure calendar in `latency/calendars.py`,
  so the board run after a CBOT holiday (e.g. Friday 27 November 2026, after
  Thanksgiving) holds the previous session and reads as a breach — red on
  correct data, the same gap DCE had before #399. A board run queued behind a
  running deploy is in the `pages` concurrency group, where a newer pending run
  replaces it; the 10:30 backstop then catches it.

**The latency gate.** Every refresh slot ends by reading the published
edition's `manifest.json` through `scripts/latency_report.py
--fail-on-breach-layers <the layers that slot fetched>` — a breach in a layer
the run was responsible for is a red run, not a log line. So is a gated layer
the run cannot show an age for (no observation, no fetch stamp, or no row in
the manifest): `UNMEASURED` fails the gate, because `unknown` never silently
meets. The test is the acquisition leg, not the overall verdict, which on a
published manifest is always `unknown` — the manifest is written before its
own deploy (review of #630: a failed curve fetch otherwise passed the board
gate and the backstop read the run as delivered). The gate is scoped
on purpose: an unscoped `--fail-on-breach` would be red every day on layers
whose acquisition breaches structurally (COT carries a 3-day provider delay
against a 24h target; the evening daily build re-stamps `dce` 13–17h after
the Dalian close, overwriting the morning run's on-time stamps until the next
morning). One slot is ungated: the 23:30 UTC catch-up. It lands after 00:15
UTC on every observed run (01:11–02:38, 2026-09-05 to 2026-09-29), which is
past settlement-plus-six-hours by construction, so its gate could never be
green and was red every night after a deploy that had succeeded. The report
still prints for that slot; only the exit code is ungated.

**A late start is reported, not failed — on the 20:07 slot only** (2026-10-12).
Four of the fifteen reds from 2026-09-28 to 2026-10-09 were the 20:07 slot
starting at 00:16–01:16 UTC: past settlement-plus-six-hours before a byte was
fetched, on the right bar, after a deploy that had succeeded. That slot — the cron, never a manual dispatch — passes
`--late-start-is-not-a-breach`, under which a gated breach prints `LATE START`
instead of failing when all of these hold: the held bar is the newest session
as of the fetch start (`latency.domain.started_after_objective` — a stale
upstream bar is still a breach), the fetch began after the acquisition target
had lapsed, the clock has a session close to measure from, and our own
pipeline leg did not breach. (In CI that last condition is vacuous: the gate
reads the published manifest, which is written before its own deploy and so
carries the pipeline leg as `unknown`. The first two conditions carry the
weight.) The edition *was* late; the line says so and
names the cause. The 08:00 DCE backstop never opts in: it fetches only when no
dispatched `Refresh Prices (dce)` run delivered that day, so its red is the
alarm that the cron-job.org trigger did not fire. Since #622 that red is
explicit — the gate step exits 1 after an `::error` annotation, whatever the
report says — because under the 12 h objective the backstop's 13:15–16:38 UTC
landing would otherwise meet it and pass the miss silently.

**The post-deploy smoke waits for the CDN.** `deploy-pages` reports done
before GitHub's CDN serves the new edition everywhere: on 2026-09-17 the
public index still carried the previous night's stamp 37 s after the deploy
returned, and the promotion-window check failed a deploy that had succeeded.
`scripts/smoke_site.py --url ... --candidate <dir>` now polls the public
index until it carries the candidate's own generation stamp (bounded, 240 s
default) before grading. On timeout it grades whatever is served — the wait
removes a false red, never a real one.

**An unfinished viewport probe is its own failure (#617).** Headless Chrome
exits 0 when a page fails to load or is served an error page. So from
2026-11-20 the smoke reported probes that never ran the page's reporting script
as `viewport overflow … (missing)` / `(pending)`, though the same pages measured
`ok` on re-run. Only a measured `overflow:<px>` now says "viewport overflow".
A failed load, a page with no marker, or a marker still `pending` at the
1500 ms virtual-time budget says "viewport probe incomplete" and names the
cause. An `ok` measured on an older edition than the candidate is "measured
another edition". All of them still fail the run: a probe that did not
measure is never a pass, and the budget is unchanged.

**Both deploying workflows retry the Pages deploy.** `deploy-pages` lists the
run's artifacts ~0.5 s after the upload finalizes, and the listing can lag
("No artifacts named github-pages"): four Refresh Prices runs (11-17, 3× 11-24)
went red on that alone. Refresh Prices now waits 30 s / 60 s and retries, as the
daily build already did. It also names its artifact per run attempt, so a manual
re-run no longer meets the first attempt's artifact ("Multiple artifacts").

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
| Masthead (headline and briefing) | generation time, plus **"Board and FX priced from data N old"** — the worst board or FX observation age, because generation time alone reads as observation time and on this site they are routinely a day apart — and beside it **"CBOT session Tue 24 Nov"**, the CBOT board's session date (oldest of `prices` and `forward_curve`; #622), because an age alone does not say which close a buyer is reading and an evening edition holds D−1 by design |
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
`FX_SESSION` — first 17:00 America/New_York, corrected by B20 #482 to 00:00
Europe/London on the day after the label, because the Yahoo bar is a London
calendar day. Both holes close with one comparison.

**The FX calendar was itself wrong (B20 #482, 2026-10-18).** Between 17:00 New
York and London midnight — 21:00→23:00 UTC in summer, 22:00→00:00 UTC in
winter — the 17:00 rule kept a bar with up to two hours still to run. It
happened routinely: `data/history/data_freshness.csv` records currencies
stored with that day's `observed_at` at 21:21 UTC on 2026-10-07, 22:13 on
10-14, 22:06 on 10-15 and 22:02 on 10-16, each before the 23:00 UTC London
close. A later run re-fetched and overwrote each one (`INSERT OR REPLACE`),
but the edition published from the earlier run carried the partial bar.

Confirmed fixed against live data the same evening: a fast refresh at 04:27 UTC
stored FX and board prices for the **2026-08-18** session and logged the
2026-08-19 overnight curve legs being dropped. Pinned by
`tests/test_settlement_guard.py`.

**The exchange clock was wrong too (B47 #622, 2026-11-25).** 14:30 CT assumed
the overnight session opens a separately labelled D+1 row at 19:00 CT. It does
not until New York midnight: before it Yahoo's row labelled D holds D's last
trade (14:30–19:00 CT) and then live trades of trade date D+1. So the guard
stored a last trade or an overnight print as D's close on every evening run —
855 soy values a later run changed, worst ZLZ26 2026-08-27 at +31.97 USD/MT,
first published in 11 daily editions. (The 04:27 UTC confirmation above was
after New York midnight, which is why it was right.) `EXCHANGE_SESSION` now
closes the bar at 00:00 America/New_York on D+1; the trusted CBOT path reads
the same rule; stored `forward_curve` legs are repaired from the final bar
(`LAYERS.md` → "Settlement guard").

---

## Desk cutoff report

```bash
python scripts/market_board.py --cutoff 08:00 --timezone Asia/Jerusalem --sessions 30
```

Add `--db /path/to/verified.db` to include the current market board from
a SQLite snapshot. It reads CBOT beans, meal and oil by named contract,
physical prices by their published terms and shipment windows, BRL and CNY
FX, and the GTR Japan route benchmarks. U.S. Gulf basis requires the
physical quote's named contract close on the same session. The benchmark
freight rows carry their own cadence and age budget. Cargo freight comes
from a quote for the buyer's route.

The cutoff belongs to the next weekday after each recorded market session.
Use `--through` to end the window on a historical market session and
`--layers` to name a different public source basket. The default is the
existing availability basket. The JSON includes the promoted run, render
timestamp, individual source grades and observation stamps, plus sessions
with no promotion or a late or incomplete basket. A ready session requires
the full basket in one qualifying edition. Dates before the record starts
are reported separately and excluded from the percentage. This report is
per promoted edition, so the reliability record's session rule (available
if any run succeeded, #518) does not apply to it.

This measures generation time for editions that eventually passed promotion.
Public delivery time requires an independent URL observation before the
cutoff. The report uses the daily deployment record and a weekday calendar.
Compare individual price identity, units and shipment terms separately when
validating a source with the desk.

**Session filing moved one session back for evening runs (B47 #622).** A run
is filed under `pipeline.layer_runs.session_date_for` — the CBOT session the
edition serves, which is the settlement guard's own answer. Since B47 that
answer changes at 00:00 New York, not 14:30 CT, so a run that starts before
New York midnight (every scheduled evening build and refresh measured so far)
is filed under the previous weekday session, and a retry that starts after it
is filed under the next one — a scheduled run and its retry can now sit on two
sessions. Three consequences, all read from `data/history/layer_runs.csv`:

- **The merge boundary.** Rows written before B47 use the 14:30 CT filing.
  The first evening after the merge files under the same session as the
  evening before it, so that one session holds two days' runs. No marker
  column separates the two meanings.
- **This report is stricter for non-CBOT layers.** Same-day physical and FX
  sources fetched on evening D are now filed under D−1, and their edition
  renders after D−1's cutoff, so they count as late for D−1. That is true of
  the CBOT leg the session is named for. It is not true of a same-day physical
  leg. Replaying the 48 recorded runs under the new filing (20-session window,
  08:00 Asia/Jerusalem) moves `basket_ready` from 13 to 1, with `late` going
  from 1 to 13. This was measured in review of #627 and is not pinned by a
  test.
- **The meaning is the owner's call.** Should `session_date` mean "CBOT
  session served" (today's definition) or "edition date"? That question is
  open on #622 together with the schedule.

## 9. The product's latency

Not the provider's. This is what a reader gets.

**Board price (CBOT, forward curve).** Yahoo finishes the bar for a 13:15 CT
settlement only at 00:00 New York the next day (B47 #622), so a CBOT close is
**never publicly readable sooner than 9 h 45 m** after it settles, plus our
fetch and publish. The `board` slot (§4, owner decision on #622) fetches at
01:00 New York, Tue–Sat: **about 11 h** after the settlement, against a 12 h
objective, before the Europe, Africa and India desk mornings. Every evening
build and refresh still lands before that midnight and carries **D−1**, so
from the evening refresh until the next 01:00 run the page shows the previous
session; the masthead names it ("CBOT session …"). Until the cron-job.org job
exists, the board reaches a reader on the next evening's build, about 26–34 h
after it settles — accepted by the owner as invariant 11's intended trade. A
wrong number is worse than a gap: before B47 the page showed day D sooner, but
the number was the last trade or an overnight print, not the settlement.

**FX.** The London-day close (23:00 UTC in BST, 00:00 UTC in GMT) is publicly
readable in **1 h 11 m to 3 h 38 m** via the 23:30 catch-up (a 01:11–02:38 UTC
landing). The 20:07 slot reaches it only on a BST landing after 23:00 UTC;
otherwise it publishes D−1, correctly. Never fresher than the London close
itself, because the settlement guard will not publish an unfinished bar —
where before this work it was **either D−1 or, worse, a partial bar
mislabelled as the close**, and until B20 #482 the guard closed the bar at
17:00 New York, up to two hours early. Meets the 6 h objective.

**Physical origin legs.** Fetched once a day by the daily build. AMS Gulf bids
(~18:48 UTC) and Argentina MAGyP (~15:05 UTC) are same-day, within about
2–6 h. **CEPEA publishes after 21:01 UTC and is therefore D−1 on most days**,
about 24–30 h — deliberately, because re-scraping it more often is a
reliability trade we declined. Stated, not hidden.

**Fundamentals** (USDA, CFTC, FAS, EIA, GTR, FRED). Within one build cycle of
release: under 24 h.

**Weather.** Under 12 h for observed rows; forecast rows have negative age by
construction, and the measurement reports the negative rather than clamping it.

**The whole product.** Excluding CEPEA and the CBOT board, **a number
observed anywhere in the world is publicly readable on this site within six
and a half hours, and the share of that we control — fetch to public page — is
under twenty-five minutes.** CEPEA is the named exception at D−1; the CBOT
board is the other, at about 11 h on the `board` slot because its provider
floor alone is 9 h 45 m.

Two things this is not. It is not real-time and no part of it is: the fastest
possible path from a CBOT settlement to a reader is 9 h 45 m, floored by when
Yahoo finishes the bar (00:00 New York on D+1) and enforced by the settlement
guard, and nothing here should be used for execution. And it is not
a claim about a *provider*: `pricing.semantics.PROVEN_SETTLEMENT_SOURCES` is
still empty, so every board number remains a delayed close, never a settlement,
however fast it arrives.
