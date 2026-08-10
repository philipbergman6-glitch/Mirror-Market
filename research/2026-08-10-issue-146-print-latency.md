# M4 — Per-market print times, ingest lag, timestamp fidelity

**Issue:** [#146](https://github.com/philipbergman6-glitch/Mirror-Market/issues/146) (parent #142, blocks #145)
**Date:** 2026-08-10
**Author:** research agent

Legend for confidence: **[OBS]** directly observed in this repo's code/DB/CSVs or a live call made
today · **[SRC]** quoted from a primary external source · **[INF]** logical inference from OBS/SRC ·
**[NV]** not verified — could not be confirmed, do not treat as fact.

---

## 0. The one number everything hangs off: when do we actually fetch?

`.github/workflows/deploy-dashboard.yml:5-6`:

```yaml
    # Weekdays at 12:00 UTC (after US market open, captures prior day settlement)
    - cron: '0 12 * * 1-5'
```

That comment is wrong on both counts, and the cron is not when the run happens.

**[OBS]** Actual start times of the last 30 `schedule`-triggered runs (`gh run list --workflow=deploy-dashboard.yml --event=schedule --limit 30`):

```
2026-08-07T13:04:49Z  2026-08-06T14:11:14Z  2026-08-05T14:10:40Z  2026-08-04T14:13:24Z
2026-08-03T14:40:42Z  2026-07-31T14:08:27Z  2026-07-30T14:03:58Z  2026-07-29T14:11:37Z
2026-07-20T14:05:51Z  2026-07-17T13:37:32Z  2026-07-16T13:50:37Z  2026-07-15T13:43:23Z
2026-07-14T13:43:31Z  2026-07-13T14:34:15Z  2026-07-10T14:26:52Z  2026-07-09T15:10:02Z
2026-07-08T14:07:51Z  2026-07-07T14:34:17Z  2026-07-06T15:33:30Z  2026-07-03T14:04:41Z
2026-07-02T14:04:07Z  2026-07-01T14:36:20Z  2026-06-30T14:19:33Z  2026-06-29T15:47:45Z
2026-06-26T14:25:11Z  2026-06-25T14:33:52Z  2026-06-24T14:30:56Z  2026-06-23T14:49:29Z
2026-06-22T16:58:26Z  2026-06-19T15:18:41Z
```

GitHub's scheduler delay ranges **+64 min to +298 min** past the 12:00 UTC cron; median ≈ **+138 min**.
So the **effective fetch window is 13:04–16:58 UTC, median ≈ 14:15 UTC** on weekdays only.
The pipeline itself then runs several minutes; the dashboard deploys ~6–35 min after start **[OBS]**.

Two corrections to the comment:
- 12:00 UTC (and even 14:15 UTC) is **before** the CBOT day-session open at 13:30 UTC / after it by
  only ~45 min — see §1. It is nowhere near "prior day settlement": the last CBOT settlement is
  ~20 h old and the row we write for *today* is an unfinished bar.
- No run happens Sat/Sun (`* * 1-5`), so Monday's dashboard is Friday-anchored for every
  business-day source.

**[OBS]** The schedule is also not reliable: no scheduled run fired at all between 2026-07-20 and
2026-07-29 (six missing weekdays: 07-21, 22, 23, 24, 27, 28), and four consecutive scheduled runs
failed (07-15, 07-16, 07-17, 07-20).

**[OBS]** Nothing in the schema stores a time of day for any market datum. Every price/report table
keys on a bare `TEXT` date (`pipeline/schema.py:11-297`); the only time-bearing columns in the whole
database are `data_freshness.last_success/last_attempt`, `commodity_freshness.checked_at` and
`briefings.generated_at` — all of which are *our clock*, not the market's
(`pipeline/store.py:692`, `:718`, `:666`, all `datetime.now(timezone.utc)`).

---

## 1. Deliverable table

"Our lag" = age of the newest datum in the DB at the moment the scheduled run writes it
(median start 14:15 UTC on weekday D).

| Market | Session / publication time (UTC) | Days | Our pipeline lag | Timestamp fidelity | Staleness notes |
|---|---|---|---|---|---|
| **CBOT** (ZS/ZL/ZM + corn/wheat, via yfinance) | Globex 00:00–12:45; day session 13:30–18:20; **settlement 18:15** (CDT; +1 h in CST) **[SRC]** | Sun eve – Fri | Row for day **D is written mid-session, ~4 h before D's settlement**. Last *settled* value in the DB is D−1, ~20 h old. **[OBS]** | True session date, but the newest row is an **unsettled partial bar** **[OBS]** | Self-heals: next run re-downloads and `INSERT OR REPLACE`s D with the settled close. Observed error on one day: stored ZS 08-07 close 1181.25 vs true settle 1156.50 (−2.1%). Roll-day gaps (see CLAUDE.md). |
| **Dalian DCE + CZCE** (via AKShare/Sina) | Day 01:00–03:30 & 05:30–07:00; night 13:00–15:00 (belongs to next trading day). **Close 07:00 UTC**; settlement = post-close VWAP, no clock time published **[SRC]** | Mon–Fri excl. PRC holidays | **Same day, ~7¼ h after the close.** Settle for D confirmed available at 08:23 UTC on D **[OBS]** | True session date **and** a real `Settle` column **[OBS]** | Freshest leg in the stack. PRC statutory holidays (Spring Festival, Golden Week) produce multi-day gaps. Whether Sina emits a partial next-day bar during the 13:00–15:00 UTC night session — i.e. exactly when our run fires — is **[NV]**. |
| **India (mandi / Agmarknet)** | No official clock time published **[NV]**; Agmarknet allows entry until 23:30 IST the next day, Fri/Sat data until Mon 23:30 IST (unconfirmed extract) **[NV]** | Mandi days; Sunday gap | **Infinite — we hold zero rows.** | `arrival_date` from the API would be a true market date **[OBS]** | `india_domestic_prices` is **empty**; no `data/history/india_domestic_prices.csv` exists. Layer status `failed`, last "success" 2026-07-30 with 0 rows **[OBS]**. Live check today: read-timeout at 45 s, then **HTTP 502 ×2** from `api.data.gov.in` **[OBS]**. |
| **Brazil — CEPEA/ESALQ Paranaguá & Paraná** | Collection 12:00–20:00 UTC; indicator computed 20:00–21:00 UTC; **published after 21:01 UTC**, business days **[SRC]** (Paranaguá methodology; the Paraná indicator's clock time is **[NV]**) | Business days | **D−1 at fetch time**, i.e. ~17 h after that indicator published, ~41 h after its collection window opened **[OBS]** | True indicator date, parsed from the page **[OBS]** | Our 14:15 UTC run is ~7 h *before* the day's indicator exists. Arbitrated days (≤5 quotes → prior day folded into the sample) are not flagged in our DB **[SRC]/[OBS]**. |
| **Brazil — AgRural Paranaguá FOB** | No published schedule **[NV]** | Business days (observed) | **D−1** **[OBS]** | True banner date, hard-failing if unparseable (`fetchers/agrural.py:156-172`) **[OBS]** | 1 row/day, snapshot-only; git CSV is the only history. Observed gap: no AgRural row for 2026-08-04 **[OBS]**. |
| **Brazil — CONAB grain survey** | Monthly, **12:00 UTC (09h BRT)** on published calendar dates (2026: 15 Jan, 12 Feb, 13 Mar, 14 Apr, 14 May, 11 Jun, 14 Jul, **13 Aug**, 15 Sep, 15 Oct, 13 Nov, 15 Dec) **[SRC]** | Monthly | Same-day if the run is after 12:00 UTC | **BROKEN — `report_date` is our UTC fetch date, not the survey date** (`fetchers/conab.py:182`: `today = datetime.now(timezone.utc)...`) **[OBS]** | DB holds one `report_date` per *pipeline run* (2026-07-30, 07-31, 08-02 … 08-07) **[OBS]**, so a single unchanged survey appears as a daily stream of "revisions". |
| **Brazil — CONAB weekly farmgate (15b)** | No official schedule; page says only "atualizados periodicamente" **[NV]**. One observed `Last-Modified: Sun, 09 Aug 2026 11:00:04 GMT` **[OBS]** | Weekly (unstated day) | Up to ~7 days | True week-end date parsed from `data_inicial_final_semana` (`fetchers/conab_precos.py:77-83`) **[OBS]** | Latest row in DB 2026-07-31 while CEPEA is at 08-06 — a normal ~1-week offset **[OBS]**. |
| **Argentina — MAGyP official FOB** | Business days; price is "vigente para operaciones … concretadas en el día de su publicación" **[SRC]**. **Clock time not published [NV]**; bounded by observation to **after ~15:05 UTC** **[OBS]** | Business days (weekends skipped in code) | **D−1** **[OBS]** | True circular date from the `?Fecha=` query (`fetchers/magyp_fob.py:107-120`) **[OBS]** | Fetcher walks back up to `MAGYP_FOB_LOOKBACK_DAYS = 7` (`config.py:625`), skipping Sat/Sun, so an Argentine holiday silently yields an older date under its own true date. Live at 08:24 UTC today: `Fecha=10/08/2026` → 0 posts; `07/08/2026` → 125 posts **[OBS]**. |
| **South Africa — SAFEX (JSE, via Grain SA)** | JSE ag session 07:00–10:00 UTC; MTM struck on 09:55–10:00 UTC snapshots, **MTM released 10:45 UTC** **[SRC]** | Business days | **Same day, ~3½ h after MTM release** **[OBS]** | True trade date, parsed from `LastTradedTime` and hard-failing rather than stamping today (`fetchers/safex.py:268-273`) **[OBS]** | Value is `LastTradedPrice`, **not** the JSE official MTM — the module docstring saying "daily settlement prices" (`fetchers/safex.py:5`) contradicts the parser **[OBS]**. Thin volumes (151 lots on 08-07) mean the "last trade" can be hours before the close. Observed gap: no 2026-07-29 row. Live scrape today failed: `Grain SA SAFEX: no <table> elements on page` **[OBS]**, cause undetermined **[NV]**. |
| **Nigeria** | — | — | — | **No price series exists.** | Nigeria appears only as two weather regions (`config.py:212-213`), a PSD country (`config.py:255`) and a Yahoo FX pair `NGNUSD=X` (`config.py:288`) **[OBS]**. There is no Nigerian cash or futures print in the stack. |
| **Euronext / MATIF** | Session 08:45–18:15 UTC (CEST) / 09:45–19:15 UTC (CET); **DSP = VWAP of the 2 min before 18:30 CET** = 16:28–16:30 UTC summer, 17:28–17:30 UTC winter **[SRC]** | Mon–Fri | **Not ingested — no layer, no table, no rows.** If added at the current cron it would be **D−1**, since the DSP is struck ~2¼ h after our median fetch **[INF]** | n/a | `config.py:317` comment: `"Matif ECO has no free feed"` — deliberate omission; the only other repo mention of Euronext is an ownership string in `data/reference/players/europe.yml:410` **[OBS]**. |
| *(US cash — AMS Gulf bids, Layer 20, feeds the CBOT column)* | Published after the CBOT close; observed 2026-08-07 edition published **18:48 UTC (13:48 CT)**, another at 16:28 UTC — **variable, no schedule [NV]** | Business days | **D−1** **[OBS]** | True `report_date` parsed from the PDF header, with a 7-day staleness hard-fail (`fetchers/gulf_bids.py:80,93-108`) **[OBS]** | Permanent hole at **2026-08-05** — the report was never captured and no later run backfilled it **[OBS]**. |

---

## 2. Evidence per market

### 2.1 CBOT (Layer 1, `fetchers/yfinance.py`)

**Publication.** CME Group grain & oilseed fact card **[SRC]**
(`https://www.cmegroup.com/trading/agricultural/files/grain-and-oilseed-futures-options-fact-card.pdf`;
CME's live host is Akamai-blocked from the research environment, so the CME-hosted PDF was read via an
Internet Archive replay of that same URL, snapshot 2025-03-28 — **no 2026-dated revision confirmed [NV]**):

> "Trading Hours   CME Globex: 7:00 p.m. – 7:45 a.m. CT, Sun – Fri and 8:30 a.m. – 1:20 p.m. CT, Mon – Fri (Settlement remains 1:15)"

In August 2026 (CDT, UTC−5): Globex 00:00–12:45 UTC, day session 13:30–18:20 UTC, **settlement 18:15 UTC**.
The finer 13:14:00–13:15:00 CT VWAP window is **[NV]** (search-index sourced only).

**What we hold.** Live pull today **[OBS]**:

```
now UTC 2026-08-10T08:23:21Z            # ZS=F, yf.download(period='10d')
2026-08-05  close 1151.50  volume     70
2026-08-06  close 1157.25  volume    135
2026-08-07  close 1156.50  volume    135
2026-08-10  close 1182.00  volume  12295   <-- today, mid-Globex, incomplete
index tz: None
```

A row for the current date exists while the session is still running. Compare the stored value written
by a local run at 11:23 UTC on 2026-08-07 (`data_freshness.last_attempt = 2026-08-07 11:23:08`, UTC per
`pipeline/store.py:718`) **[OBS]**:

```
sqlite> SELECT * FROM prices WHERE commodity='Soybeans' ORDER BY Date DESC LIMIT 2;
Soybeans|2026-08-07|1177.75|1185.0|1175.5|1181.25|15478.0
Soybeans|2026-08-06|1151.75|1157.5|1151.25|1157.25|70.0
```

Stored 2026-08-07 close = **1181.25**; the true settled close for that date is **1156.50**. The stored
number was an unfinished overnight bar, 24.75 ¢ (2.1 %) away from settlement. The 08-06 row matches
exactly, confirming the value self-heals on the *next* run — but the dashboard published on day D carries
day D's partial print labelled as day D. **[OBS]**

`pipeline/store.py:276-278` writes `Date` straight through `_date()` (`:255`,
`pd.to_datetime(...).dt.strftime("%Y-%m-%d")`); yfinance's index is tz-naive exchange session dates,
so fidelity is a true session date — the problem is completeness, not labelling. **[OBS]**

Side note **[OBS], unexplained [NV]**: ZS=F daily volumes of 70/135 for 2026-08-05..07 are implausibly
low for a front-month grain contract. Not investigated here; flagged because a propagation strip that
weights by volume would be misled.

### 2.2 Dalian DCE + CZCE (Layer 9, `fetchers/akshare.py`)

**Publication.** DCE contract spec (dce.com.cn returns HTTP 412 to non-browser clients; read via archive
replay of the DCE URL) **[SRC]**:

> "Trading Hours | 9:00 - 11:30 a.m., 1:30 - 3:00 p.m., Beijing Time, Monday to Friday, and other trading hours announced by DCE"

DCE's *Measures for Trading Management* splits the day into a night period whose hours are "separately
notified by the Exchange" **[SRC]** — DCE's own statement of the night hours is **[NV]**. CZCE, however,
publishes them for rapeseed meal in a 2026-approved rule
(`https://www.czce.com.cn/cn/content_file/flfg/zcjywgz/pzxz/2026/5/338bbcd6027246f792a776ca7c59f5f4.pdf`,
Art. 8) **[SRC]**:

> 夜盘交易时间为下午21:00—23:00。日盘交易时间为上午9:00—11:30，下午13:30—15:00；其中，上午10:15—10:30为休息时间。

CZCE rapeseed **oil** (OI0, one of our two CZCE tickers) has no equivalent 2026 rule PDF located — its
night session is inferred from RM and is **[NV]**. Neither DCE nor CZCE publishes a settlement *clock
time*; both compute a volume-weighted average after the 15:00 Beijing close **[SRC]**.

China is UTC+8 with no DST, so these UTC mappings never move: close **07:00 UTC**, night session
**13:00–15:00 UTC**.

**What we hold.** Live AKShare pull today **[OBS]**:

```
now UTC 2026-08-10T08:23:41Z            # ak.futures_zh_daily_sina('M0')
2026-08-07  ... close 3154.0  volume 721055  hold 2078470  settle 3144.0
2026-08-10  ... close 3148.0  volume 603209  hold 2160574  settle 3146.0
```

The completed 2026-08-10 session, with `settle`, was available **1 h 23 min after the 07:00 UTC close**.
Our run at ~14:15 UTC therefore gets same-day Chinese settlement, ~7¼ h old — the only leg in the stack
that carries a same-day *settled* print. `dce_futures` stores `Date` + `Settle`
(`pipeline/schema.py:110-120`, `pipeline/store.py:445-449`) **[OBS]**.

**Open risk [NV]:** 14:15 UTC falls inside the 13:00–15:00 UTC night session, which at DCE/CZCE belongs
to the *next* trading day. Whether Sina emits an in-progress D+1 bar during that window (which we would
then store as a settled-looking row) was not tested — it can only be tested by running the fetcher
between 13:00 and 15:00 UTC on a trading day.

### 2.3 India — mandi (Layer 16, `fetchers/mandi.py`)

**We hold nothing.** **[OBS]**

```
sqlite> SELECT commodity, COUNT(*), MIN(Date), MAX(Date) FROM india_domestic_prices GROUP BY commodity;
(no rows)

$ ls data/history/
argentina_fob.csv  brazil_estimates.csv  brazil_spot_prices.csv  forward_curve.csv
gulf_bids.csv  inspection_destinations.csv  inspection_port_flows.csv  inspections.csv
safex_prices.csv  wasde.csv
```

`india_domestic_prices` *is* registered in `HISTORY_TABLES` (`pipeline/history.py:47`), but
`export_history()` never writes an empty table, so no CSV was ever created. On CI's ephemeral runner
that means India has no persistence path at all: even a successful fetch would give exactly one day's
snapshot, discarded at the end of the run — the API resource itself is a current-snapshot resource with
no date filter (`fetchers/mandi.py:93-97` sends only `filters[commodity]` and `filters[state]`). **[OBS]**

Layer status **[OBS]**:

```
india_domestic | 2026-07-30 16:00:32 | 2026-08-07 11:23:03 | 0 | failed
```

Live check today, using the exact URL and sample key from `config.py:546-548` **[OBS]**:

```
ERR ReadTimeout HTTPSConnectionPool(host='api.data.gov.in', port=443): read timeout=45
HTTP 502 / JSONDecodeError    (retried twice, unfiltered, 90 s timeout)
```

Publication timing is moot while the source is down. For the record: no official refresh clock time or
SLA is published for the resource **[NV]**; an Agmarknet data-entry page reportedly allows updates until
23:30 IST the next day and rolls Fri/Sat into Monday, which would imply a ≥1-day lag and a Sunday hole —
but agmarknet.gov.in returned 403 to the research agent, so this is **[NV]**.

### 2.4 Brazil (Layers 15, 15b, 17, 19)

**CEPEA/ESALQ publication [SRC]** — Paranaguá indicator methodology (cepea.org.br is Cloudflare-walled;
the identical PDF was read from ESALQ's own mirror `economiaflorestal.esalq.usp.br`):

> §3.2.2.1 "Coleta: Diariamente, das 09h00 às 17h00, hora oficial de Brasília/DF."
> §3.2.2.2 "Fechamento do Indicador: … são realizados a partir das 17h00 e finalizam às 18h00"
> §3.4.1 "…é divulgado todos os Dias Úteis nos sites da BM&FBOVESPA e do Cepea, após as 18h01"
> §3.3.1 (≤5 valid quotes) the previous day's indicator is added to the sample and the release is tagged "o Indicador foi Arbitrado"

BRT = UTC−3, so publication is **after 21:01 UTC**. The separate *Paraná* CEPEA indicator's page states
only "Periodicidade: diária" — its clock time is **[NV]**.

**Observed lag.** Max `Date` in `data/history/brazil_spot_prices.csv` at each CI history commit **[OBS]**:

| CI commit (UTC) | brazil_spot max Date |
|---|---|
| 2026-07-31T14:13:56Z | 2026-07-30 |
| 2026-08-03T14:45:31Z | 2026-07-31 |
| 2026-08-04T14:19:19Z | 2026-08-03 |
| 2026-08-05T14:20:35Z | 2026-08-04 |
| 2026-08-06T14:19:28Z | 2026-08-05 |
| 2026-08-07T13:22:24Z | 2026-08-06 |
| 2026-08-08T08:10:20Z | 2026-08-07 |

Consistently D−1, exactly as the 21:01 UTC publication vs 14:15 UTC fetch predicts. Confirmed live today
at 08:25 UTC (Monday) **[OBS]**:

```
agrural   ok  {'Soybean (AgRural Paranaguá FOB)': '2026-08-07'}
cepea/NA  ok  {'Soybean (CEPEA)': '2026-08-07', 'Soybean (ESALQ/B3 Paranaguá)': '2026-08-07'}
```

**Fidelity.** `fetchers/noticias_agricolas.py:137,150` parses `DD/MM/YYYY` from the table and stores the
ISO date; `fetchers/agrural.py:156-172` parses the SOJA banner date and raises rather than guess. Both
are true indicator dates. **[OBS]**

**Observed gaps [OBS]:** no AgRural row for 2026-08-04 (CEPEA and ESALQ/B3 both present that day).

**CONAB survey [SRC]** — 2026 calendar PDF
(`https://www.gov.br/conab/pt-br/assuntos/imagens/calendario-de-safras-e-prohort-2026-interativo-2026-novo.pdf`):
levantamentos on 15 Jan, 12 Feb, 13 Mar, 14 Apr, 14 May, 11 Jun, 14 Jul, **13 Aug**, 15 Sep, 15 Oct,
13 Nov, 15 Dec 2026; CONAB's aviso de pauta gives "Horário: 9h" BRT = 12:00 UTC. The same calendar also
states "TODA SEGUNDA-FEIRA • ÀS 19H A CONAB DIVULGA O PROGRESSO DE SAFRA".

**CONAB fidelity is broken [OBS].** `fetchers/conab.py:182-183`:

```python
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
result = _melt_to_long(grouped, report_date=today)
```

`report_date` is the fetch date, and it is part of the `brazil_estimates` primary key
(`pipeline/schema.py:261`). The DB therefore contains:

```
sqlite> SELECT DISTINCT report_date FROM brazil_estimates ORDER BY report_date DESC LIMIT 8;
2026-08-07 2026-08-06 2026-08-05 2026-08-04 2026-08-03 2026-08-02 2026-07-31 2026-07-30
```

— one "report" per pipeline run against a source that publishes monthly. Any consumer treating
`report_date` as a survey date, or counting distinct `report_date`s as revisions, is wrong.
(It does still let you diff values across runs to *detect* when a revision landed, which is presumably
why nothing has broken.)

**CONAB weekly farmgate (15b)** carries a true week-end date parsed from
`data_inicial_final_semana` (`fetchers/conab_precos.py:77-83`) **[OBS]**. No official update schedule
exists — the download page says only "Os dados são atualizados periodicamente" **[NV]**; one observed
`Last-Modified: Sun, 09 Aug 2026 11:00:04 GMT` is a single sample, not a schedule **[OBS]**.

### 2.5 Argentina — MAGyP official FOB (Layer 21)

**Publication [SRC].** Resolución 65/2025 (BO 30/04/2025):

> Art. 2: "Los precios FOB oficiales serán los vigentes para las operaciones con fecha de cierre de venta concretadas en el día de su publicación."
> Art. 3: "se ajustarán periódicamente para los días hábiles…"

So: business days, effective for the publication day itself. **No official clock time is published [NV].**

**Observed bound [OBS].** Live probe today of `MAGYP_FOB_URL` (`config.py:614`):

```
now UTC 2026-08-10T08:24:47Z
10/08/2026  HTTP 200  posts 0
07/08/2026  HTTP 200  posts 125
```

and from the history CSV: the 2026-08-07 **15:05 UTC** run still wrote `argentina_fob` max date
2026-08-06; only the 2026-08-08 08:10 UTC run had 2026-08-07. That bounds publication to **after
~15:05 UTC (12:05 ART)** on the circular's own date — later than our median 14:15 UTC fetch, hence the
permanent D−1.

**Fidelity [OBS].** `fetchers/magyp_fob.py:107-120` takes the date from the circular record itself, not
from the clock; the walk-back loop (`:144-152`, `MAGYP_FOB_LOOKBACK_DAYS = 7`, `config.py:625`) skips
weekends and unpublished days. Consequence: on an Argentine holiday the strip silently shows an older
circular — correctly dated, but a reader looking only at "we have Argentina" will not notice it is stale
unless the date is rendered.

### 2.6 South Africa — SAFEX (Layer 18)

**Publication [SRC].** JSE "Commodity Derivatives Market Operating Hours"
(`https://clientportal.jse.co.za/Content/JSE%20Trading%20Dates%20and%20Calendars%20Items/CommodityDerivativesTradingHours.pdf`),
Agricultural Derivatives column, "All times South African Standard Time":
pre-open 08h50–08h59, open 09h00, **close 12h00**, **Mark-to-Market release 12h45**.
JSE "Detailed Agricultural Contract Specifications" §2.3: "Trading on a trading day commences at 09h00
and ends at 12h00"; Appendix I: MTM is the arithmetic average of five randomised snapshots across the
last 5 minutes of the session. SAST = UTC+2, no DST → close **10:00 UTC**, **MTM released 10:45 UTC**.
The operating-hours PDF is undated, so "current 2026 revision" is **[NV]**.

**Observed lag [OBS].** SAFEX max `Date` per CI history commit is always the same calendar day as the run
(2026-07-31→07-31, 08-03→08-03, 08-04→08-04, 08-05→08-05, 08-06→08-06, 08-07→08-07). Same-day,
~3½ h after the MTM release.

**Fidelity caveat [OBS].** We do not read the JSE MTM. `fetchers/safex.py` parses Grain SA's
`LastTradedPrice` / `LastTradedTime` columns (`:58-60`, `:243-276`) and refuses to store a row whose
`LastTradedTime` has no parseable calendar date:

```python
if trade_date is None:
    raise ScraperShapeError(
        f"Grain SA SAFEX: no parseable trade date for {instrument_code} ... — refusing to stamp today")
```

That is a good guard, but the module docstring at `fetchers/safex.py:5` calls these "daily settlement
prices", which the parser contradicts. A last trade on a thin contract (2026-08-07 SOYB volume: 151
lots) can be hours stale relative to the 12h00 close even though the date is right.

**Observed gaps [OBS]:** `data/history/safex_prices.csv` has no 2026-07-29 row (07-28 → 07-30).
Live scrape today failed outright:

```
FetchResult(data={}, status='failed', error='Grain SA SAFEX: no <table> elements on page')
```

Whether that is a weekend page state, a transient render, or a fresh break is **[NV]**.

### 2.7 Nigeria

There is no Nigerian price print anywhere in the stack. **[OBS]** Grep of `config.py` and the analysis
layer returns only:

- `config.py:212-213` — weather regions "Nigeria Benue", "Nigeria Kaduna"
- `config.py:255` — Nigeria in the PSD country list (annual/quarterly USDA balance sheet)
- `config.py:288` — `"NGN/USD": "NGNUSD=X"` (Yahoo FX)
- `analysis/briefing/sections/emerging_markets.py:1` — narrative section

Neither PSD (annual attributes) nor an FX rate is a market print. The Nigeria "deep dive" is a
derived/narrative read, not an observed quote, and cannot carry a latency claim.

### 2.8 Euronext / MATIF

Not ingested. **[OBS]** A repo-wide grep for `Euronext|MATIF` matches exactly two files: `docs/players.html`
(generated) and `data/reference/players/europe.yml:410`, where "Euronext Amsterdam: FFARM" is a company's
stock listing. The deliberate exclusion is documented at `config.py:316-318`:

> `# CZCE rapeseed complex — same Sina feed, different exchange. The only`
> `# free *daily* rapeseed benchmark (Matif ECO has no free feed);`

If it were added, the numbers are known **[SRC]** (`live.euronext.com` contract specs + DSP page):

> "10:45 – 20:15 CET (UTC+1) (and 10:45 - 18:30 CET the last three days of the expiry)"
> EBM and ECO DSP: "the 2 minutes immediately preceding 18:30 CET, i.e. before the end of the main trading session."

DSP = 16:28–16:30 UTC in CEST (summer), 17:28–17:30 UTC in CET (winter) — **2¼ h after our median fetch**,
so a Euronext leg on the current cron would be permanently D−1. The 18:30–20:15 evening block was added
13 April 2026; the notice PDF itself was not read **[NV]**.

Note for any cross-market timing logic: CBOT and Euronext change DST on different dates (US 2nd Sun Mar /
1st Sun Nov; EU last Sun Mar / last Sun Oct), so the Chicago↔Paris offset is off its usual value for
~3 weeks a year **[SRC/INF]**.

### 2.9 US cash — AMS Gulf export bids (Layer 20, supports the CBOT leg)

Report AMS_3147 is business-day, labelled "Grain Report for 8/7/2026 - Final", and includes same-day CBOT
settlements — so it necessarily publishes after 18:15 UTC **[SRC]**. Observed publish timestamps from the
public MARS feed vary (2026-08-07 edition at 12:48 MDT = 18:48 UTC; a 2026-08-05 entry at 10:28 MDT), so
there is **no fixed time [NV]**.

Observed lag: D−1 at every CI commit **[OBS]** (07-31 run → 07-30; 08-03 → 07-31; 08-04 → 08-03;
08-06 → 08-04; 08-07 → 08-06; 08-08 → 08-07).

Observed permanent hole **[OBS]**:

```
sqlite> SELECT report_date, COUNT(*) FROM gulf_bids GROUP BY report_date ORDER BY report_date DESC;
2026-08-07 | 2026-08-06 | 2026-08-04 | 2026-08-03 | 2026-07-31 | 2026-07-30 | 2026-07-29
```

2026-08-05 is missing and was never backfilled — the fetcher only ever reads the current PDF, so a missed
run is a permanent gap. `_MAX_AGE_DAYS = 7` (`fetchers/gulf_bids.py:80`) prevents stamping a stale report
but does nothing about the hole.

---

## 3. Implications for the propagation strip (#145)

### 3.1 The hard constraint

**The stack stores no time of day for any market datum.** Every leg is a bare date
(`pipeline/schema.py`), and the only clocks in the DB are our own run timestamps. A strip that shows
"last print time" per market can only honestly show a **date**, plus a *statically known* venue
publication time attached in the presentation layer — never an observed timestamp. If the strip renders
a clock time, that time is a constant we typed in, not something we measured, and should be labelled as
the venue's scheduled print time, not as "when this number arrived".

### 3.2 Which legs can carry a "has it repriced yet" read

**Yes — same-day settled print, honest:**
- **Dalian DCE + CZCE.** Same-day settlement, ~7 h old at fetch, with a real `Settle` column. This is
  the only leg where "China has repriced today" is a statement we can defend. Fitting, given #145's 6am
  Dalian-silo scenario is precisely the case where China is the *originating* market.
- **South Africa SAFEX**, with a caveat: the date is same-day and correct, but the number is a last
  trade on a thin book, not the JSE MTM. "SAFEX has traded today at X" is honest; "SAFEX settled at X"
  is not. Ideally re-source to the JSE MTM (released 10:45 UTC, comfortably before our run) before this
  leg carries a repricing claim.

**Yes, but only as "yesterday's print" — must be rendered as D−1, never as today:**
- **Brazil (CEPEA / ESALQ-B3 / AgRural).** Structurally D−1: the indicator for day D does not exist
  until 21:01 UTC, ~7 h after our run. On the day a Dalian story breaks, Brazil *cannot* have responded
  in our data — its absence of movement is an artifact of the cron, not a market judgement. This is the
  single most dangerous leg for a naive strip.
- **Argentina MAGyP FOB.** Same shape (published after ~15:05 UTC), plus the walk-back can silently
  serve a several-day-old circular over a holiday. Render the circular date.
- **US Gulf cash (AMS 3147).** D−1, with observed permanent holes. Never interpolate across a hole.

**Qualified — do not present as a settled print:**
- **CBOT.** The row we publish for day D is an *unfinished* bar captured ~4 h before settlement
  (observed error 2.1 % on one day). Two honest options: (a) show the **D−1 settlement** and label it
  as such — correct but 20 h stale, or (b) show the D partial and label it "intraday, unsettled". What
  the strip must not do is show today's date next to a number implied to be a settlement. If CBOT is the
  reference leg the whole strip is measured against, this choice propagates into every gap number.
  Moving the cron to ≥18:30 UTC would make CBOT a same-day settled leg *and* pick up Brazil, Argentina
  and AMS 3147 on the same day — the single highest-leverage change available.

**No — cannot appear:**
- **India mandi.** Zero rows, source returning 502 today, no persistence path even if it recovered, and
  the underlying data is a next-day-reported physical arrival price. #145 already flags India as the
  extreme case; the evidence is stronger than "slow" — it is currently **absent**. If it appears at all
  it should be an explicit empty state, not a line.
- **Nigeria.** No price series exists. It can carry weather and balance-sheet context, never a
  repricing read.
- **Euronext.** Not ingested. If added on the current cron it would be D−1 anyway.

### 3.3 Specific traps to design around

1. **Weekends and Monday.** No run fires Sat/Sun. Monday's dashboard is built ~14:15 UTC Monday and
   holds Friday for every D−1 leg and Monday for DCE/SAFEX. A strip that says "3 days since Brazil
   printed" on a Monday is technically right and practically noise; a strip that says "Brazil has not
   moved" is wrong.
2. **Scheduler jitter is ±4 h.** Observed run starts span 13:04–16:58 UTC. On a late run (>16:30 UTC in
   winter) a Euronext leg would flip from D−1 to D, and CBOT gets closer to settlement. Any lag figure
   baked into the UI must be a range, not a constant.
3. **Missed runs are permanent holes** for snapshot-only sources (gulf_bids 2026-08-05, AgRural
   2026-08-04, SAFEX 2026-07-29 are all observed). Distinguish "no row" from "unchanged row" visually —
   these are different claims and the DB cannot tell them apart without the date.
4. **`brazil_estimates.report_date` is our fetch date, not CONAB's survey date.** Do not source any
   "CONAB last updated" line from it. Fixing `fetchers/conab.py:182` to parse the actual survey date is
   a separate, small ticket worth filing.
5. **Freshness thresholds are layer-level, not market-level.** `FRESHNESS_WARNING_DAYS = 7`
   (`config.py:704`) means a daily leg can be 6 days stale without warning. That is far too loose to
   back a propagation strip; the strip needs its own per-market expected-cadence table.

### 3.4 Unresolved / worth a follow-up ticket

- **[NV]** Does the Sina/AKShare feed emit a partial D+1 bar during the 13:00–15:00 UTC DCE/CZCE night
  session — exactly our run window? Testable by running `fetchers/akshare.py` at 14:00 UTC on a trading
  day. If yes, DCE has the same partial-bar problem as CBOT and the "freshest leg" claim weakens.
- **[NV]** The Grain SA SAFEX page returned no tables at 08:25 UTC today. Is this a weekend state, a
  transient, or a break?
- **[NV]** ZS=F daily volumes of 70–135 lots for 2026-08-05..07 from yfinance. If real, the front-month
  mapping is wrong; if a Yahoo artifact, any volume-weighted logic must not use it.
- **[NV]** CEPEA *Paraná* indicator publication clock time (only the Paranaguá methodology documents
  18h01 BRT).
- **[NV]** DCE's own statement of night-session hours; CZCE rapeseed **oil** night session.
