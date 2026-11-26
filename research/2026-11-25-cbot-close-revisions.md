# Why stored CBOT soy closes change after capture

**Question.** PR [#619](https://github.com/philipbergman6-glitch/Mirror-Market/pull/619) found **347 soy-complex
`Close` revisions in 30 days** of `data/history/contract_bars.csv` (V4 question 6, [#477](https://github.com/philipbergman6-glitch/Mirror-Market/issues/477)).
Are they same-session recaptures before the value is final, provider back-revisions days later, roll artefacts, or
something else? The answer has to come before the CBOT tolerance band is set.
**Date:** 2026-11-25 · **Author:** research session (Claude Opus 5.5), read-only on data.
**Defect filed:** [#622](https://github.com/philipbergman6-glitch/Mirror-Market/issues/622) (B47, `backlog:integrity`).

Legend: **[O]** observed here (git snapshots, run metadata, run logs, provider responses, the desk DB) ·
**[I]** inference from [O] · **[NV]** not verified.

## Answer

**Every revision is class (a): a value captured before Yahoo had finished the bar.** None is a provider correction
made after the bar was final, and none is a roll artefact.

| Class | Count, all history (sessions 08-24 → 11-24) | Count, 30-day window (#619's 347) |
|---|---|---|
| (a1) **overnight leak**: captured 19:00 CT → NY midnight; the row labelled D held trade date D+1's live overnight price | **260** | **180** |
| (a2) **last trade, not the final close**: captured 14:30 → 19:00 CT; the row held D's last trade (sampled: 307 of 391, Friday sessions not established, below), later replaced by Yahoo's final daily close | **595** | **167** |
| (a0) captured before the 14:30 CT guard cutoff | **0** | **0** |
| (b) provider back-revision of a bar already final (captured after NY midnight, changed later) | **0** | **0** |
| (c) roll / contract-identity artefact | **0**, by construction | **0** |
| (d) other | **0** | **0** |
| **Total** | **855** revisions over 776 (ticker, session) rows | **347** (reproduces #619 exactly) |

[O] for every count (`classify.py` and `derive.py`, appendix). The classes are **capture-time windows**: the count in
each is observed, the cause its name gives is [I], checked on samples since 2026-09-28 (Evidence). The 30-day window
is revisions whose replacing commit is on or after 2026-10-26.

The settlement guard (`fetchers/_settlement.py`, `EXCHANGE_SESSION`) calls bar D finished at **14:30 CT on D**
(`config.SETTLEMENT_CUTOFF_LOCAL = (14, 30)`, `config.py:310`) [O]. Yahoo's bar D behaves as final from **00:00
America/New_York on D+1** [I: no value captured after it was ever replaced (O); the exact hour between the last
revised and the first never-revised capture was not bounded]. Every revision falls inside that gap. "Final" below
means Yahoo's daily close as it stands after NY midnight: a delayed close (`pricing/semantics.py`), shown to equal the
settlement-implied board only for meal on 7 sessions (a2 evidence) [I for ZS and ZL].

## Method

1. **Which tables.** `pipeline/history.py` `HISTORY_TABLES` carries `contract_bars` (`("ticker", "Date")`) and
   `forward_curve` (`("fetched_date", "commodity", "contract_month")`). Front-month `prices` is **not** a history
   table [O], so it has no git record and is out of reach of this method.
2. **Snapshots.** All 89 commits touching `data/history/contract_bars.csv` (first `6904a96`, 2026-08-23), every one
   authored by `github-actions[bot]` [O]. Each was read with `git show <sha>:<path>`, filtered to Soybeans, Soybean Oil
   and Soybean Meal, and each change in `Close` per (ticker, Date) was recorded with its commit time.
3. **Run mapping.** Only `deploy-dashboard.yml` commits history: its "Commit history snapshots" step. `refresh-prices.yml`
   commits none, and 11b is excluded from it (`LAYERS.md` 11b) [O]. Each commit was matched to the one deploy run whose
   `createdAt…updatedAt` contains it (`gh run list --workflow deploy-dashboard.yml`). The 855 replaced values were
   captured by 55 commits: schedule 758, repository_dispatch (retry) 79, push 18 values. Their replacements came
   from schedule 549, push 131, repository_dispatch 112, workflow_dispatch 63. The first capture of each of the 776
   revised rows: schedule 758, push 18 [O].
4. **Windows.** Each capture time was placed against its own session date D: before 14:30 CT, 14:30–19:00 CT (the CBOT
   overnight session opens at 19:00 CT), 19:00 CT to 00:00 ET, and after 00:00 ET [O, `window()` in the appendix].
5. **Ground truth, two independent checks.**
   - *What the pre-final value is.* Yahoo 15-minute bars (`yfinance`, 60-day reach) for every listed soy contract.
     Each pre-NY-midnight capture since 2026-09-28 was compared with (i) the close of the last day-session 15-min bar
     and (ii) the overnight 15-min range from 19:00 CT up to the capture minute [O].
   - *What the final value is.* USD/MT price minus basis in USDA AMS report 3511 (`us_processor_cash`, Layer 30). That
     is the CBOT board AMS struck its basis against, which `LAYERS.md` Layer 30 shows reproduces CBOT closes to the
     cent. It was compared with our first and final ZMZ26 value on each Thursday strike session [O].
   - CME's own settlements endpoint refused automated access (HTTP 403, "This IP address is blocked due to suspected
     web scraping activity") [O]. It was not retried, so no CME-published settlement was read [NV].

## Evidence

### Values captured after NY midnight never change

Of 15,168 soy (ticker, session) rows, **no value captured after 00:00 ET on D+1 was ever replaced**. All 855 replaced
values were captured before it: 595 between 14:30 and 19:00 CT, 260 between 19:00 CT and NY midnight [O]. The
"10-day lag" in #619 is not a late correction. The 11-06 session's last-trade values (captured 17:51 CT, commit
`b24fc8f`) were replaced on the next **successful** run, 11-16 09:35 UTC (`8c1eade`). Every deploy from 11-10 to 11-14
failed [O, `gh run list`].

### (a1) The overnight leak

- **2026-11-24, all 17 listed soy contracts moved between two runs 47 minutes apart on the same evening**: the schedule
  run at 21:09 CT (`7f67908`, run 45536043506) and its retry at 21:56 CT (`cee32b6`, run 45542017650). ZL moved −11
  to −17 ticks across every month, ZM +10 to +19, ZS +5 to +7 [O]. A settlement does not move in parallel at 21:00 CT.
  - ZLF27: stored **57.99**, then **57.82**. Yahoo's 15-min overnight bars at those minutes close at 57.99 (20:00,
    20:15) and 57.79–57.82 (21:45–22:15). The last day-session bar closed at 58.11, and Yahoo's daily bar for 11-24
    now reads **58.32** [O]. The first published value was 0.33 ¢/lb (7.3 USD/MT) off, the second 0.50 ¢/lb
    (11.0 USD/MT).
  - ZMF27: stored 348.4, then 350.2. The overnight 19:00 bar closes at 348.4 and the 21:15/21:30 bars at 350.2.
    Yahoo now reads 351.1 [O].
- **The guard dropped no CBOT bar on that run.** The 21:05 CT log carries `Dropping 1 unsettled bar(s)` lines for the
  nine FX pairs only [O, run 45536043506 log]. Yahoo had served no 11-25 row, so the overnight trades were inside the
  row labelled 11-24 [I]. On the 11-17 push run at 04:00 CT (after NY midnight) the guard did drop `[ZS=F]`, `[ZL=F]`
  and `[ZM=F]` 11-17 rows [O, run 44058875066]: by then the D+1 row exists. Yahoo labels these futures bars in
  `America/New_York` (index `00:00:00-05:00`) [O]. That fits a live row keyed by the New York calendar date [I].
- Across the 243 pre-final captures since 09-28 taken between 19:00 CT and NY midnight, **all 243** lie inside the
  overnight 15-min range traded up to the capture minute. Only 22 equal the day-session last trade [O].
- **Error of the overnight value against the final close** (rows whose final value was captured after NY midnight) [O]:

  | Product | n | p50 ticks | p90 | max | p50 USD/MT | p90 | max | share ≥ 0.5 USD/MT |
  |---|---|---|---|---|---|---|---|---|
  | ZS (¼ ¢/bu) | 79 | 6 | 32 | 42 | 0.55 | 2.94 | 3.86 | 54 % |
  | ZL (0.01 ¢/lb) | 81 | 17 | 42 | 145 | 3.75 | 9.26 | **31.97** | 90 % |
  | ZM (0.10 $/st) | 83 | 8 | 28.6 | 45 | 0.88 | 3.15 | 4.96 | 61 % |

  Worst case: 2026-08-27, 21:37 CT (`2e7f843`). ZLZ26 was stored as 69.96 against a final 68.51: **+145 ticks,
  +31.97 USD/MT**, and the curve moved about the same amount in every month.
- **Editions affected.** 11 daily editions first published overnight-window closes, all `schedule` runs. Their sessions
  were 08-27, 08-28, 10-05, 10-12, 11-02, 11-16, 11-17, 11-18, 11-19, 11-23 and 11-24. Six retries re-captured inside
  the same window [O]. The cause is cron lateness. The workflow header says "+64 to +298 min … ~20:00–24:00 UTC"
  [O `deploy-dashboard.yml:5-7`], but since 2026-10-26 the median schedule delay is **304 min** (range 224–549), and
  7 of 22 scheduled runs started after 19:00 CT [O].

### (a2) Last trade, then the final close

- **307 of 391** captures since 09-28 taken between 14:30 and 19:00 CT equal the close of Yahoo's last 15-min
  day-session bar, which is the last trade [O]. The 84 that do not are **all Friday sessions** (10-09, 10-16, 10-23,
  10-30, 11-06, 11-20; 14 contracts each). On those days Yahoo's intraday last bar already equals the final daily
  close, so what the Friday capture held is not established [NV].
- **The final value is the settlement-consistent one.** On Thursday strike sessions, the board implied by USDA AMS
  3511 (Illinois / Portland / Indiana-Ohio meal, single-price single-basis rows over the December contract) is [O]:

  | Thu session | AMS-implied ZMZ26 | our first capture (window) | our final |
  |---|---|---|---|
  | 09-24 | 372.4 | 371.2 (14:30–19:00) | **372.4** |
  | 10-01 | 353.3 | — | **353.3** |
  | 10-08 | 357.6 | 357.2 (14:30–19:00) | **357.6** |
  | 10-22 | 347.3 | 347.4 (14:30–19:00) | **347.3** |
  | 10-29 | 329.3 | 329.2 (14:30–19:00) | **329.3** |
  | 11-05 | 317.2 | 317.4 (14:30–19:00) | **317.2** |
  | 11-12 | 325.4 | — | **325.4** |

  7 of 7 final values match; all 5 first captures from the 14:30–19:00 window miss by 1–12 ticks. AMS is a second
  publisher (USDA), so this is independent of Yahoo [I: AMS strikes basis against settlement, the convention for cash
  basis; Layer 30's reconciliation is the observed support].
- **Error of the last-trade value against the final close** [O]:

  | Product | n | p50 ticks | p90 | max | p50 USD/MT | p90 | max | share ≥ 0.5 USD/MT |
  |---|---|---|---|---|---|---|---|---|
  | ZS | 204 | 4 | 6.7 | 25 | 0.37 | 0.62 | 2.30 | 20 % |
  | ZL | 196 | 4 | 12 | 55 | 0.88 | 2.65 | 12.13 | 67 % |
  | ZM | 195 | 3 | 10 | 38 | 0.33 | 1.10 | 4.19 | 34 % |

  The tails are expiring delivery-month contracts: ZLU26 in September (+55 ticks on 09-02), ZLV26 and ZMV26 in October
  (−49, +38). There a thin last trade and the settlement part furthest [O].

### (b), (c), (d): none found

- (b): 0. No value captured after NY midnight was ever replaced (above).
- (c): 0. `contract_bars` is keyed by ticker, so a row cannot change contract. The front-month `prices` table, where a
  roll could switch contract, is not a history table and could not be measured [O `HISTORY_TABLES`].
- Float noise and sub-tick changes: 0. Every delta is a whole number of ticks [O].

### The snapshot tables keep the pre-final value for good

`forward_curve` is keyed by `fetched_date`, so a row is written once and never revisited. Of 1,037 soy curve rows with
an `observation_date` and a matching `contract_bars` session, **671 differ from that session's final close** [O].
Example: the 2026-11-25 row for ZLF27 holds 57.84 at `observation_date` 2026-11-24, with `volume` 4,244 against a
day-session volume around 80k. That is an overnight print stamped as the 11-24 session [O]. #619's materiality and
band numbers for the curve, crush and spreads should be read with this in mind [I].

Expired contracts cannot be repaired by re-fetch either. 47 rows outside the pending 11-24 session still hold a
pre-NY-midnight value. Most are probably a last trade that equalled the settlement. Those on contracts Yahoo has since
delisted (e.g. ZLV26/ZMV26 2026-10-12; ZSX26 11-03 and 11-06) will never be re-served, so they cannot be checked [O/NV].

### Other paths with the same exposure (inferred)

- **Trusted CBOT path.** `trust/cbot_benchmarks.py:431` stamps `SettlementState.SETTLED` from
  `fetchers._settlement.session_is_settled(now)`, so `settlement.confirmed` (`trust/quality.py:577`) passes the same
  bars [O code, I effect].
- **Fast refresh.** `Refresh Prices` runs fetch `prices` and `forward_curve` through the same guarded `fetch_one`.
  Since 10-26, 31 of the 44 evening-band scheduled runs (created 20:00–08:00 UTC: the 20:07 and 23:30 UTC crons,
  late landings included) started between 19:00 and 24:00 CT, 26 of them on Mon–Thu [O run times, `derive.py`]. They commit no history, so the values they published are **not measured** [NV].

## What this means for the CBOT tolerance band (#477)

**Proposals for the owner, not decisions.** Item 2 bears on #477 question 3, items 3–4 on the V4/V6 grading
grilling (#479), item 5 on #622 and the canonical latency figure in `CLAUDE.md`. Each is the owner's call there.

1. **The revision distribution is not a provider-noise band, so it must not set the CBOT tolerance.** #619 §1.5's
   ZL "median 1.54 USD/MT" mixes two of our own capture defects. The overnight leak is a different trade date: a
   wrong number, not a tolerance question. Last trade vs settlement is a different price type: a definition question.
   Widening the band to absorb either would hide a defect the verifier exists to catch.
2. **Once bars are read after NY midnight (#622), the measured revision rate is zero.** On this evidence the CBOT
   stratum can stay **exact**: native tolerance 0 ticks against the publisher's print, plus display and conversion
   rounding only (#619 §1.2). There is no measured case for a ≤ 4-tick band on final bars.
3. **Until #622 lands, editions are graded against what they displayed** (#619 §1.5 consequence 1 stands). An edition
   stamped before NY midnight carries a pre-final value by construction. The verifier should record the edition's
   capture window and grade such rows **material-unexplained** when the published value ≠ the final, not
   "explained". The cause is ours, and V6's "revised publication" event class does not describe it.
4. The `revision` event class in #619's V6 table should be split. *Pre-final capture* (ours; 347 per 30 days) is
   distinct from *provider back-revision* (0 observed for CBOT). Only the second is a property of the source.
5. The 1 h 15 m CBOT floor in `LATENCY.md` §9 and `CLAUDE.md`'s mission table is measured to a cutoff the provider does
   not honour. That is #622's to correct. The honest figure is roughly 13:15 CT → 23:00 CT.

## Limits

- No CME-published settlement was read (403). "Final = settlement" rests on the AMS meal match (7 sessions, one
  product) and on Yahoo's own convention [I]. ZS and ZL finals were not checked against a second publisher [NV].
- The 15-min intraday comparison covers only sessions since 2026-09-28 (Yahoo's 60-day reach) and listed contracts
  (expired ZLV26/ZMV26/ZSX26 return no intraday) [O].
- Capture time is the history commit's time, which lands a few minutes after the fetch. No window boundary was within
  a few minutes of a capture in the examples above, but the count near the 19:00 CT edge was not re-checked [NV].
- `prices` (front month) and fast-refresh editions were not measured (see above).

## Appendix: the script

The Yahoo and AMS checks were short ad-hoc reads over its output (`captures.csv`). The script is not added under
`scripts/`: #619's V6 "history-diff detector" will need a tested, general version, and that belongs to the verifier.

```python
"""Classify every Close revision of a soy-complex contract_bars row across the
committed snapshots of data/history/contract_bars.csv. Read-only (git show).
usage: python classify.py runs.json [since]   # runs.json = gh run list --workflow
       deploy-dashboard.yml --limit 400 --json databaseId,event,createdAt,updatedAt
"""
import io, json, os, subprocess, sys
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import pandas as pd

PATH = "data/history/contract_bars.csv"
SOY = {"Soybeans": (0.25, 36.7437 / 100), "Soybean Oil": (0.01, 22.0462),
       "Soybean Meal": (0.10, 1.10231)}  # (tick, native -> USD/MT), pipeline/units.py
CT, ET = ZoneInfo("America/Chicago"), ZoneInfo("America/New_York")

def window(session, t):
    cut = datetime.combine(session, time(14, 30), tzinfo=CT)   # config.SETTLEMENT_CUTOFF_LOCAL
    reopen = datetime.combine(session, time(19, 0), tzinfo=CT)  # overnight session (next trade date)
    et_midnight = datetime.combine(session + timedelta(days=1), time(0, 0), tzinfo=ET)
    if t < cut: return "W0 before guard cutoff"
    if t < reopen: return "W1 cutoff..overnight open"
    if t < et_midnight: return "W2 overnight open..NY midnight"
    return "W3 after NY midnight"

runs = json.load(open(sys.argv[1]))
since = sys.argv[2] if len(sys.argv) > 2 else "2000-01-01"
for r in runs:
    r["c"] = datetime.fromisoformat(r["createdAt"].replace("Z", "+00:00"))
    r["u"] = datetime.fromisoformat(r["updatedAt"].replace("Z", "+00:00"))
def run_of(t):
    hit = [r for r in runs if r["c"] <= t <= r["u"]]
    return (hit[0]["event"], hit[0]["databaseId"]) if len(hit) == 1 else ("unmatched", None)

log = subprocess.run(["git", "log", "--reverse", "--format=%H %cI", "--", PATH],
                     capture_output=True, text=True, check=True).stdout.split()
commits = [(log[i], datetime.fromisoformat(log[i + 1]).astimezone(timezone.utc))
           for i in range(0, len(log), 2)]
seq = {}  # (ticker, Date, commodity) -> [(commit_i, Close)] on each value change
for i, (sha, _) in enumerate(commits):
    df = pd.read_csv(io.BytesIO(subprocess.run(["git", "show", f"{sha}:{PATH}"],
                     capture_output=True, check=True).stdout), dtype={"Date": str})
    df = df[df.commodity.isin(SOY)]
    for tk, d, c, com in zip(df.ticker, df.Date, df.Close, df.commodity):
        s = seq.setdefault((tk, d, com), [])
        if not s or s[-1][1] != c:
            s.append((i, c))

rows = []
for (tk, d, com), s in seq.items():
    sess, (tick, usd), final = pd.Timestamp(d).date(), SOY[com], s[-1][1]
    for k, (i, c) in enumerate(s):
        t = commits[i][1]
        ev, rid = run_of(t)
        rows.append(dict(ticker=tk, commodity=com, session=d, k=k, commit=commits[i][0][:7],
                         time_utc=t, window=window(sess, t), event=ev, run=rid, close=c,
                         replaced_by=s[k + 1][1] if k + 1 < len(s) else None,
                         err_vs_final_ticks=round((c - final) / tick, 2),
                         err_vs_final_usd=(c - final) * usd))
out = pd.DataFrame(rows)
out = out[out.session >= since]
out.to_csv(os.path.join(os.path.dirname(os.path.abspath(sys.argv[1])), "captures.csv"), index=False)
rev = out[out.replaced_by.notna()]
print("revisions:", len(rev), "over", rev.groupby(["ticker", "session"]).ngroups, "rows")
print(rev.groupby(["window", "commodity"]).size().unstack(fill_value=0))
```

`derive.py` produces every other figure that does not need a network read: the 30-day split, the capture/replacement
events, the error tables (rows whose last value was captured in W3, so that "final" is known), the worst case, the
11 editions and 6 later W2 captures, the 47 pre-final rows, the curve 671 of 1,037, and both run-timing figures.
Run it from the repo root after `classify.py`, on the `captures.csv` that wrote:

```python
"""Every derived figure of the note from classify.py's captures.csv, the two run
lists and the committed history CSVs. Read-only.
usage: python derive.py captures.csv runs.json refresh.json
       refresh.json = gh run list --workflow refresh-prices.yml --limit 300 --json createdAt,event
"""
import json, sys
from zoneinfo import ZoneInfo
import pandas as pd

CT = ZoneInfo("America/Chicago")
c = pd.read_csv(sys.argv[1], parse_dates=["time_utc"]).sort_values(["ticker", "session", "k"])
g = c.groupby(["ticker", "session"])
c["repl_t"], c["repl_ev"] = g.time_utc.shift(-1), g.event.shift(-1)
rev = c[c.replaced_by.notna()]
print("rows:", g.ngroups, "| revised values:", len(rev), "| by window:", rev.window.str[:2].value_counts().to_dict())
w30 = rev[rev.repl_t >= pd.Timestamp("2026-10-26", tz="UTC")]
print("30-day (replacing commit >= 10-26):", len(w30), w30.window.str[:2].value_counts().to_dict())
print("replaced values by capturing event:", rev.event.value_counts().to_dict(),
      "| distinct capturing commits:", rev.commit.nunique())
print("replaced values by replacing event:", rev.repl_ev.value_counts().to_dict())
print("first captures (k=0) of revised rows by event:",
      c[(c.k == 0) & c.set_index(["ticker", "session"]).index.isin(rev.set_index(["ticker", "session"]).index)]
      .event.value_counts().to_dict())
print("non-integer tick deltas:", int((rev.err_vs_final_ticks % 1 != 0).sum()))

last = g.tail(1)
final_known = set(map(tuple, last[last.window.str.startswith("W3")][["ticker", "session"]].values))
c["final_known"] = [(t, s) in final_known for t, s in zip(c.ticker, c.session)]
for w in ("W2", "W1"):
    print(f"error vs final, {w} (final captured after NY midnight):")
    sub = c[c.replaced_by.notna() & c.window.str.startswith(w) & c.final_known]
    for com, x in sub.groupby("commodity"):
        t, u = x.err_vs_final_ticks.abs(), x.err_vs_final_usd.abs()
        print(f"  {com}: n={len(x)} ticks p50={t.median()} p90={t.quantile(.9):.1f} max={t.max()} "
              f"usd p50={u.median():.2f} p90={u.quantile(.9):.2f} max={u.max():.2f} share>=0.5={(u >= 0.5).mean():.0%}")
w2 = c[c.replaced_by.notna() & c.window.str.startswith("W2")]
print("worst W2:", w2.loc[w2.err_vs_final_usd.abs().idxmax(), ["ticker", "session", "commit", "err_vs_final_ticks", "err_vs_final_usd"]].to_dict())
first_w2 = c[(c.k == 0) & c.window.str.startswith("W2")]
print("editions first published in W2:", sorted(first_w2.session.unique()), first_w2.event.unique().tolist())
later_w2 = c[(c.k > 0) & c.window.str.startswith("W2")].drop_duplicates("commit")
print("later captures still in W2 (commits):", len(later_w2), later_w2[["commit", "session", "event"]].values.tolist())
pending = last[~last.window.str.startswith("W3") & (last.session != "2026-11-24")]
print("rows outside 11-24 whose last value is pre-NY-midnight:", len(pending))

fc = pd.read_csv("data/history/forward_curve.csv")
fc = fc[fc.commodity.isin(["Soybeans", "Soybean Oil", "Soybean Meal"]) & fc.observation_date.notna()]
cb = pd.read_csv("data/history/contract_bars.csv", dtype={"Date": str})
m = fc.merge(cb[["ticker", "Date", "Close"]], left_on=["ticker", "observation_date"], right_on=["ticker", "Date"])
print("curve rows with observation_date and a bar:", len(m), "| differ from the bar:", int(((m.close - m.Close).abs() > 1e-6).sum()))
print(m[(m.ticker == "ZLF27.CBT") & (m.fetched_date == "2026-11-25")][["close", "observation_date", "volume"]].to_dict("records"))

def evening_ct(runs):
    r = pd.DataFrame(runs)
    r["c"] = pd.to_datetime(r.createdAt)
    return r[(r.event == "schedule") & (r.c >= pd.Timestamp("2026-10-26", tz="UTC"))].copy()

d = evening_ct(json.load(open(sys.argv[2])))
d["delay"] = ((d.c - d.c.dt.normalize()).dt.total_seconds() / 60 - 19 * 60) % 1440   # cron 0 19 UTC
ct = d.c.dt.tz_convert(CT)
print("deploy schedule runs since 10-26:", len(d), "delay min median/range:", d.delay.median(), d.delay.min(), d.delay.max(),
      "| started 19:00-06:00 CT:", int(((ct.dt.hour >= 19) | (ct.dt.hour < 6)).sum()))
r = evening_ct(json.load(open(sys.argv[3])))
r = r[(r.c.dt.hour >= 20) | (r.c.dt.hour < 8)]          # the 20:07 and 23:30 UTC crons, late-landing included
rct = r.c.dt.tz_convert(CT)
night = r[(rct.dt.hour >= 19)]
print("refresh evening-band schedule runs:", len(r), "| started 19:00-24:00 CT:", len(night),
      "| of them Mon-Thu (CT):", int(night.c.dt.tz_convert(CT).dt.dayofweek.isin([0, 1, 2, 3]).sum()))
```

Re-run 2026-11-25 by an independent review on `contract_bars.csv` at `cee32b6`: every figure above reproduced
(the evening-band denominator is 44 under the definition in the script; an earlier ad-hoc count said 43).

**Not reproducible from this appendix** [O, ad hoc, not scripted]: the Yahoo 15-minute comparisons (the 11-24
ZLF27/ZMF27 bar closes, "243 of 243" inside the overnight range and "22" equal to the last trade, "307 of 391"
and the Friday 84). Yahoo's 15-minute reach is 60 days, so the sessions they cover start to fall out of reach from
2026-11-27. Also the USDA AMS 3511 table and its 7/7 match, and the guard log lines of runs 45536043506 and
44058875066 (`gh run view --log`). Their denominators (243, 391) are the W2 and W1 captures since 09-28 on contracts
Yahoo still lists, and those come from `captures.csv`.
