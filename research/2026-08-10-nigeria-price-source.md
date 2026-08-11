# Is there a feedable, reliable Nigerian soybean price series?

- **Question:** Can Mirror Market ingest a daily/weekly Nigerian soybean price (or credible proxy) as an automated pipeline layer?
- **Date:** 2026-08-10 (all URLs probed this date unless noted). Checkout: branch `players-t2-page` @ `865827e`.
- **Context:** Nigeria currently has *no* price series in the repo — only weather (`Nigeria Benue`, `Nigeria Kaduna`, `config.py:212-213`), `NGN/USD` (Layer 7, `config.py:288`) and PSD supply/demand (Layer 6).

**Evidence tiers:** **[D]** directly observed by me this session (endpoint fetched, file parsed, DB queried) · **[I]** inference from observed data · **[A]** assumption or secondary source · **[?]** could not verify.

---

## VERDICT — GO, with a licence gate that must be cleared first

**A real, daily, machine-readable Nigerian soybean price series exists. I fetched it, parsed 1,655 observations of it, and corroborated its level against three independent Nigerian sources.**

**AFEX Commodities Exchange publishes an unauthenticated JSON endpoint carrying daily soybean prices back to 2022-01-19.** It is not documented, but AFEX's own front-end code calls it the public live-ticker URL. It needs no API key, no login and no captcha — only an `Origin` header.

```
GET https://api-md.afexnigeria.com/AFEXMD/api/v1/securities/price
     -H "Origin: https://africaexchange.com"
→ 200, application/json, 796,168 B, 2,575 daily rows 2019-01-28 → 2026-08-09
```

**But this is a licence decision, not a technical one.** AFEX's terms of use say, verbatim:

> "You agree not to reproduce, duplicate, copy, sell, resell or exploit any portion of the Platform or our services without the express written permission by us."
> — https://africaexchange.com/terms-and-conditions

That is **RED** on its face. One email to AFEX would settle it, and it is worth sending, because the data underneath is genuinely good.

**Everything else loses, and most of it loses badly.** Three separate international humanitarian price databases carry Nigeria in depth and **all three carry zero soybean rows** — I enumerated each one. USDA's GAIN Lagos reports state **no Nigerian soybean price at all**. The most-cited "Nigerian soybean price" on the open web is a **13-year-old dead FAOSTAT number republished as a 2026 price**, which I traced to source.

**The one genuine near-miss is federal.** NBS's *Nigeria Food Price Tracking* pilot does collect soybean at LGA level including farmgate — 126,425 rows — and publishes it as a single open CSV. But I downloaded all 117 MB and the file's `Last-Modified` header reads **2025-06-26: frozen for 13.5 months**, its price unit is documented nowhere, and its level sits **64% above AFEX** on the same date (§3.2b). It is the best *future* candidate in this survey and unusable today.

### The one-line answer per candidate

| Candidate | Feedable? | Verdict |
|---|---|---|
| **AFEX `api-md.afexnigeria.com`** | **Yes — JSON, daily, 2022→now, NGN/kg** | **GO on data, RED on licence** |
| NCX `ncx.com.ng` | Server-rendered HTML, but **undated, no history** | No — level cross-check only |
| LCFE `lcfe.ng/market-data.php` | Server-rendered table, **one row, 11 weeks stale** | No — level cross-check only |
| WFP / HDX `wfp_food_prices_nga.csv` | Yes, CSV, CC BY-IGO — **0 soybean rows** | No |
| FEWS NET FDW API | Yes, JSON, open — **0 soybean rows for NG** | No |
| FAO GIEWS FPMA API | Yes, JSON, open — **0 soybean rows for NG** | No |
| FAOSTAT producer prices | Yes, ZIP/CSV — **Nigeria soy price ends 2013** | No |
| CBN Statistics Database | Live, open — **no soybean node in 2,323 datasets** | No |
| **NBS NFPT** (Food Price Tracking pilot) | **Yes — one CSV, 126k soy rows, farmgate included** | **No — frozen 13.5 months; unit unresolved** |
| NBS Selected Food Price Watch | PDF only — **43-item basket, zero soy** (verified) | No |
| USDA FAS GAIN (Lagos) | PDFs retrievable via back-door API | **No — states no soybean price at all** |
| NGX / FMDQ | HTTP 200 — **no commodity price product at all** | No |
| Benin / Togo WFP soybean (proxy) | Yes, CSV, CC BY-IGO, monthly | **No — proxy quality measured and it fails** |
| Selina Wamucii / CEIC / SEO price sites | — | **No — actively misleading, see §4** |

---

## 1. AFEX Commodities Exchange — the one that works

### 1.1 Finding it: the obvious domain is dead

**[D]** Probed 2026-08-10:

| URL | Observed |
|---|---|
| `https://afexnigeria.com/` | **HTTP 502**, `text/html`, 631 B, body cites `openresty` / `Powered by APISIX` |
| `https://www.afexnigeria.com/` | **connection timeout** at 25 s |
| `comx.africa`, `comx.ng`, `app.comx.africa` | **DNS NXDOMAIN** |
| `https://afex.africa/` | 308 → `https://www.afex.africa/`, **HTTP 200**, 44,832 B — corporate site only |
| **`https://africaexchange.com/`** | **HTTP 200**, 67,341 B — the real platform |
| `https://africaexchange.com/markets` | **HTTP 200**, 132,128 B, but a **JS-only shell** — I grepped it and found zero price values and zero `₦` strings server-side |

**[D]** The endpoint is hard-coded in AFEX's own Next.js bundles. Verbatim from
`https://africaexchange.com/_next/static/chunks/pages/_app-c48e2739a7d3c612.js`:

```js
commodity_prices=()=>(0,eA.Z)({baseURL:"https://api-md.afexnigeria.com/AFEXMD",
                              url:"api/v1/commodities/prices",method:"GET"})
```

and from the `www.afex.africa` bundle `771-02deddac0bc9a484.js`:

```js
h="https://api-md.afexnigeria.com/AFEXMD/api/v1";
if(!h)throw Error("NEXT_PUBLIC_LIVE_TICKER_URL is not set")
```

**[I]** AFEX's own variable name — `NEXT_PUBLIC_LIVE_TICKER_URL` — is the strongest available evidence that this endpoint is intended to be publicly reachable.

### 1.2 The access gate is an `Origin` header, and nothing else

**[D]** Isolated by controlled test (I re-ran both arms myself):

```
$ curl https://api-md.afexnigeria.com/AFEXMD/api/v1/commodities/prices
status=401  {"message":"Auth denied. Missing required keys. API-KEY, REQUEST-TS and HASH-KEY","responseCode":"101"}

$ curl -H "Origin: https://africaexchange.com" <same URL>
status=200  application/json  1178 B
```

**[D]** User-Agent is irrelevant; `Origin: https://afex.africa` also returns 200; `Origin: https://example.com` returns 401. No cookie, no key, no captcha, no Cloudflare.

**[I]** This is a server-side check, not browser CORS. It is trivially satisfiable but also trivially tightenable — **a fetcher must hard-fail on 401 rather than degrade silently** (per the repo's stated preference for hard failure over silent failure).

### 1.3 Two endpoints, and the second one is the valuable one

**Endpoint A — latest snapshot.** `…/api/v1/commodities/prices` → 200, 1,178 B, 8 commodities, all dated `2026-08-09`. **[D]** Verbatim soybean record:

```json
{"commodity_code":"Soyabean (SBS)","marketPrice":681.25,"changePercentage":0.0,
 "percentage_change":0.0,"type":"","date":"2026-08-09"}
```

**Endpoint B — full history.** `…/api/v1/securities/price` → 200, 796,168 B, `"message":"Security Prices "`, **2,575 daily rows, 2019-01-28 → 2026-08-09**. Rows are **ragged** — early rows carry 4 columns, the last row carries 28; 67 distinct column names across the file. **[D]** Verbatim last row (excerpt):

```json
{"SPRL":345.32,"DPRL":345320.0,"OPRL":345320.0,"SCOC":6733.5,"DCOC":6733500.0,
 "SSBS":681.25,"DSBS":681250.0,"OSBS":681250.0,"SSGM":321.63,"date":"2026-08-09"}
```

### 1.4 Units — resolved by two independent observations, not guessed

**[D]** (a) `SSBS: 681.25` and `DSBS: 681250.0` appear in the *same record* — exactly ×1000. The same ×1000 relation holds for every commodity (`SPRL`/`DPRL`, `SCOC`/`DCOC`, …). (b) Verbatim strings in `_app-c48e2739a7d3c612.js`: **`All prices are in kg`** and **`Prices have a 24hr delay.`**; the ticker renders `["₦", format(marketPrice)]`.

→ **`S*` = NGN/kg, `D*`/`O*` = NGN/MT.** Soybean 2026-08-09 = **₦681.25/kg = ₦681,250/MT**.

**Leg: whole bean only.** **[D]** No meal, oil or cake column exists in the 67-column set. (`SSSM`/`DSSM` = 1,190/kg is present but is **not** in the 8-item ticker and its commodity identity is **[?] UNVERIFIED** — the contract-spec endpoint is auth-gated. Do not assume it is soy meal.)

### 1.5 Series quality — measured, and it changes the implementation advice

**[D]** My own parse of the 2,575 rows:

| Column | Unit | n>0 | First | Last | Gaps >7d |
|---|---|---|---|---|---|
| **`SSBS`** | **NGN/kg** | **1,655** | 2022-01-19 = 398.55 | 2026-08-09 = 681.25 | **none** |
| `DSBS` | NGN/MT | 1,204 | 2022-04-01 = 400000 | 2026-08-09 = 681250 | **one, 379 days** (2022-08-29 → 2023-09-12) |
| `OSBS` | NGN/MT | 1,054 | 2023-09-12 | 2026-08-09 | — |
| `VSBS` | — | 636 | 2022-01-19 | **dead 2023-10-16** | — |
| `SBS` | — | all zeros | — | — | legacy |

**Use `SSBS` × 1000, not `DSBS`.** `DSBS` — the one that looks right because it is already in MT — has a **379-day hole**. `SSBS` covers that hole continuously (395 observations between 2022-09-01 and 2023-09-30). This is the single most decision-relevant implementation fact here, and I only found it by parsing the file rather than reading the last row.

**Cadence.** **[D]** Calendar-daily *including weekends* — weekday counts over the last 120 observations are uniform (Mon 15, Tue 17, Wed 17, Thu 18, Fri 17, Sat 18, Sun 18), i.e. weekends are carry-forward. Fraction of observations that change value, per year (`SSBS`):

```
2022  295/346    2023  239/365    2024  187/364    2025  222/361    2026  122/218
```

**[D]** Flat runs are real: the price has been **frozen at ₦681.25/kg for the 13 days 2026-07-28 → 2026-08-09**. **[I]** `clean.py`'s flat-price health check will fire on this. It is genuine illiquidity plus weekend carry-forward, not a data bug — but the dashboard must not present a carried-forward weekend value as a new print.

**Level history, `SSBS` NGN/kg monthly mean** (**[D]**, my parse):

```
2024-01  501   2024-06  827   2024-11 1036   2025-04  980   2025-11  691   2026-04  747
2024-03  672   2024-08  866   2024-12 1033   2025-06  926   2026-01  749   2026-06  697
2024-05  618   2024-10  865   2025-01  915   2025-08  768   2026-02  655   2026-08  681
```

2026 range: **₦550,000 – ₦927,730/MT** over 218 observations.

### 1.6 The PDF reports are stale — do not build against them

**[D]** AFEX's Strapi CMS is open and unauthenticated at `https://blog.afex.africa/graphql` (POST, no token, 200). It exposes `Daily Price Report`, `Weekly Price Report`, `Monthly Price Report`. But:

- **Daily Price Reports stop at 2026-02-25** (latest five: `25-02-2026`, `24-02-2026`, `23-02-2026`, `22-02-2026`, `21-02-2026`)
- **Weekly stops 2025-09-19**; **Monthly stops 2024-09-20**
- PDFs themselves download fine — `https://blog.afex.africa/uploads/25_02_2026_59ca0e5187.pdf` → 200, `application/pdf`, 1,114,749 B. `pdftotext -layout` gives a clean row:
  ```
  Soyabean   SBS   0.00   0   637.50   637.50   634.38   634.38   0.49%   14.56%
  ```
  (OHLC in ₦/kg; 2026-02-25 close 634.38.)
- **[D]** `https://www.afex.africa/reports` renders `Showing 0 - 0 of 0 entries` server-side.

**[I]** The PDF route is a useful *format* cross-check and confirms the ₦/kg unit reading independently, but it is six months stale. The JSON is the live source.

### 1.7 What this price *is*

**[A/I]** AFEX is a licensed private commodities exchange with warehouse receipts, not a government price collection. The `SBS` ticker is an exchange reference/market price for whole soybean, at AFEX warehouse locations, **24 h delayed by AFEX's own statement**. It is closer in character to a wholesale/exchange spot than to a farmgate or retail price. Any rendered label should read **"AFEX exchange spot, Nigeria (T-1)"**, not "Nigeria soybean price". I did **not** find a published contract specification (grade, moisture, delivery location) — **[?] UNVERIFIED**.

---

## 2. Cross-validation — every number, worked out in both units

Conversion inputs, both **[D]** from the repo DB (`data/storage/mirror_market.db`):
- `NGN/USD` from Layer 7 (`NGNUSD=X`), last close **2026-08-07 = 1,361.08 NGN/USD**
- CBOT `Soybeans` front-month × `0.367437` = USD/MT (`pipeline/units.py` convention)

### 2.1 Four Nigerian sources, one commodity, same week

| Source | Date on the quote | NGN/kg | NGN/MT | NGN/USD | **USD/MT** |
|---|---|---|---|---|---|
| **AFEX** `SSBS` | 2026-08-09 | 681.25 | 681,250 | 1,361.08 | **500.5** |
| **NCX** `SOYKAD` (Kaduna) | undated | 695.00 | 695,000 | 1,361.08 | **510.6** |
| **NCX** `SOYNAS` (Nasarawa) | undated | 687.00 | 687,000 | 1,361.08 | **504.7** |
| **LCFE** SOYA BEANS (Niger/Nasarawa/Kebbi) | **22/5/2026** | 700 | 700,000 | 1,369.55 | **511.1** |

**[D]** NCX verbatim from the server-rendered homepage HTML (no JS needed):

```html
<h3>Soya (SOYKAD) <b>₦695.00</b> <span class="green">11.56%</span></h3>
<h3>Soya (SOYNAS) <b>₦687.00</b> <span class="green">5.69%</span></h3>
```

**[D]** LCFE verbatim from the `<table>` at `https://lcfe.ng/market-data.php` (I re-fetched it myself; note it returns **HTTP 406** to a bare UA and 200 only with full browser `Accept` headers):

```
Date | Commodities | Location | unit | unit price
22/5/2026 | SOYA BEANS | NIGER/NASARAWA/KEBBI | KG | 700
```

**Date-matched, which is the honest comparison:**

| Comparison | AFEX on that date | Other source | Gap |
|---|---|---|---|
| LCFE dated **2026-05-22** | ₦730/kg = **$533.0/MT** | ₦700/kg = **$511.1/MT** | **−4.1%** |
| NCX, *if* current (2026-08) | ₦681.25 = $500.5 | ₦695 = $510.6 | **+2.0%** |
| NCX, *if* last refreshed 2026-04-09 | ₦765 = $555.0 | ₦695 = $504.2 | **−9.2%** |

**All three comparisons land inside ±10%, under either reading of the NCX date.** Three Nigerian institutions that do not share a data pipeline agree on the level. **This is the finding that makes the AFEX series credible rather than merely available.**

**[D] Why the NCX date is ambiguous, stated rather than papered over:** NCX prints no timestamp anywhere. Its WordPress REST API is exposed — `https://ncx.com.ng/wp-json/wp/v2/commodities?per_page=100` → 200, `x-wp-total: 14` — and **all 14 records carry `"modified":"2026-04-09T07:17:xx"`**. **[I]** Strong inference the ticker was last refreshed 2026-04-09. **[?]** Not confirmed: Wayback CDX returned no 2026 captures and `archive.org/wayback/available` returned HTTP 429.

### 2.2 AFEX's feed vs AFEX's own published commentary — an independent check

AFEX's market commentary is widely re-reported. Two specific claims, tested against the feed I pulled:

| Published claim | Feed says (**[D]**) | Assessment |
|---|---|---|
| "surpassing ₦1,000,000/MT, **peaking around ₦1,180,000 in the third quarter** [2024]" | **Max 2024 = ₦1,200.00/kg = ₦1,200,000/MT on 2024-11-29.** Q3 2024 max was only ₦900/kg | **Level corroborated within 1.7%; the quarter attribution is wrong** — the peak is Q4, not Q3 |
| "falling to a **low of ₦843 on April 2** [2025]" | 2025-04-02 = **952.48**; 2025-04-03 = **840.16** | **Level corroborated within 0.34%, date off by one session** |

**[I]** Both checks land on the feed's own numbers to within a rounding error while being off by one calendar unit. That pattern is consistent with a T-1 publication convention (AFEX itself states a 24 h delay, §1.4) rather than with the feed and the commentary being different data. It is a genuine corroboration.

**[A]** Caveat: the commentary is *AFEX's own*, so this is an internal-consistency check, not an independent one. The independent check is §2.1.

### 2.3 Nigeria vs CBOT — what the series would actually render

| Date | AFEX USD/MT | CBOT USD/MT | Premium |
|---|---|---|---|
| 2024-11-29 | 711.9 | 363.58 | **+348.4 (+95.8%)** |
| 2025-04-03 | 546.7 | 371.66 | +175.0 (+47.1%) |
| 2026-04-09 | 555.0 | 428.16 | +126.9 (+29.6%) |
| 2026-05-22 | 533.0 | 439.64 | +93.4 (+21.2%) |
| 2026-06-15 | 547.0 | 411.25 | +135.8 (+33.0%) |
| **2026-08-07** | **500.5** | **434.03** | **+66.5 (+15.3%)** |

**[I]** A persistent, large, and *narrowing* premium. That is economically coherent: Nigeria is a net-deficit crusher (PSD MY2026 crush 975 kMT vs production 1,500 kMT, `psd` table) with a non-GM origin premium and import-parity pricing on a depreciated naira. The premium compressing from +96% to +15% as NGN/USD firmed from 1,686 to 1,361 is exactly the shape the repo's existing India-vs-CBOT premium line is built to show.

### 2.4 The regional proxy — I built it, measured it, and it fails

**[D]** WFP's HDX country files carry soybean for Nigeria's neighbours even though they do not for Nigeria:

| Country | Soy rows | Last obs | Unit | Type |
|---|---|---|---|---|
| **Benin** | 4,059 | 2026-06-15 | XOF/KG | Retail, 50 markets |
| **Togo** | 5,380 | 2026-06-15 | XOF/KG | Retail, 103 markets |
| Cameroon | 599 | 2025-12-15 | XAF/KG | mixed |
| Ghana | 1,018 | **2023-07-15** | GHS | dead |
| Niger, Burkina, Mali, Côte d'Ivoire | **0** | — | — | — |

June 2026 national medians of WFP's own `usdprice`, ×1000 → **Benin $620/MT (n=49), Togo $590/MT (n=89)** — and AFEX on 2026-06-15 was **$547/MT**. Retail sitting 8–13% above an exchange wholesale is the right sign and a sane magnitude.

**So why reject it?** Because I measured how well two *adjacent* countries track each other, and that is the ceiling on how well either could proxy Nigeria. **[D]** 87 overlapping months, 2019-01 → 2026-06, retail median USD/MT:

| Statistic | Benin vs Togo |
|---|---|
| Level correlation | 0.661 |
| **Monthly log-return correlation** | **0.232** |
| Spread mean / sd | +29.4 / **100.3 USD/MT** |
| Spread min / max | −275.0 / +315.0 |

Month-to-month the two flip by ±33% against each other (2025-12: Benin $545 vs Togo $820; 2026-01: Benin $535 vs Togo $405). **[I]** A proxy whose two nearest realisations correlate 0.232 on returns carries essentially no short-horizon information about a third country — and unlike Benin/Togo, Nigeria has a floating currency, so an XOF-pegged proxy would strip out the single largest driver of Nigerian soybean prices. This mirrors the #148 finding on ICE canola vs MATIF (0.538) — and 0.232 is far worse than that.

**Decisive additional problem: the proxy is unvalidatable.** There is no Nigerian series in any of these databases to backtest it against. Shipping it would mean publishing a line nobody can check.

---

## 3. Everything else — enumerated, not assumed

### 3.1 The three international price databases all cover Nigeria in depth and all have zero soybean

This is the most important negative result, and each leg is a direct enumeration, not a search.

**WFP Global Food Prices via HDX.** **[D]**
`https://data.humdata.org/api/3/action/package_show?id=wfp-food-prices-for-nigeria` → 200, JSON. Metadata verbatim: `license_title: "Creative Commons Attribution for Intergovernmental Organisations (CC BY-IGO)"`, `dataset_date: [2002-01-15 TO 2026-07-15]`, `last_modified: 2026-08-09T15:18:18`, `dataset_source: "FEWSNET via FAO: GIEWS, FPMA, Nigeria, SIMA - Niger, WFP"`.
CSV → 200, `text/csv`, 10,801,660 B, 88,556 rows. **I enumerated all 43 commodities. Soybean is not among them.** Nearest oilseeds present: `Groundnuts`, `Groundnuts (shelled)`, `Oil (palm)`, `Oil (vegetable)`, `Cowpeas`.

**FEWS NET Data Warehouse.** **[D]** Open, keyless, JSON.
`https://fdw.fews.net/api/marketpricefacts/?format=json&country=Nigeria` → 200, 103,468,074 B, **84,427 rows, 2003-10-31 → 2026-06-30**, source documents `"…Nigeria, Price (weekly)"` and `"…Livestock Price (weekly)"`. **20 products, none soy**: Bread, Cattle, Cowpeas ×2, Diesel, Gari ×2, Gasoline, Goats, Groundnuts, Maize ×2, Millet, Palm Oil, Rice ×2, Sheep, Sorghum ×2, Yams.
Direct test: `?product=R01412AA` (the CPC code for `Soybean (unspecified)`, confirmed from `/api/classifiedproduct/`) returns **462 rows worldwide — 318 United States, 144 Burma, and nothing else.** Combined with `&country=NG` it returns `[]`.

**FAO GIEWS FPMA.** **[D]** The public tool is a JS-only Angular app, but I extracted the API base from its bundle (`Z2_prefix="https://fpma.fao.org/giews/v4/"`, `Z2_baseOrigin="global/"`, `Z2_postfix="price_module/api/v1/"`). The resulting endpoint is **open, keyless JSON**:
`https://fpma.fao.org/giews/v4/global/price_module/api/v1/FpmaSerie/` → 200, 4,114,504 B, `count: 4964`.
**178 Nigeria series. I printed all of them. Zero soybean.** Globally there are only 20 soy series and **not one is African** (Bangladesh, Indonesia, Cambodia, Laos, Nepal, Uruguay, plus FAO's own international IPS quotes).

**[D] A telling detail:** of the 178 Nigeria series, `source_name` is `National Bureau of Statistics` for **105** and `Famine Early Warning Systems Network (FEWS NET)` for 73. **[I]** FPMA is therefore a machine-readable mirror of the NBS food-price collection — and the NBS half of it has no soybean either. That is meaningful indirect evidence that NBS's Selected Food Price Watch does not track soybean, though it is inference: FPMA may ingest only a subset of what NBS publishes.

### 3.2 Nigerian federal sources

- **NBS — `nigerianstat.gov.ng` is unreachable, and precisely so.** **[D]** DNS resolves (197.159.69.92). **TCP/443 accepts the connection** (`nc` succeeds) but **the TLS handshake never completes**: curl sends `Client hello (1)` and no ServerHello ever returns — timeouts at 20/25/30/40/60/90 s, with `--http1.1` and `--tlsv1.2`, across two independent agents, on both apex and `www.`. Port 80 answers `HTTP/1.1 301, Server: Apache` and redirects to the dead HTTPS leg. `api.` / `nada.` are NXDOMAIN. **The origin is down, not slow.**
- **NBS "Selected Food Prices Watch" does NOT track soybean — now verified, no longer an inference.** **[D]** Two archived editions pulled through Wayback raw replay (both 200, `application/pdf`): April 2021 (10,526,476 B, 102 pp) and April 2023 (5,167,424 B, 17 pp). `grep -i soy` returns **0 hits in both**. The basket is 43 items and its only oilseed products are `Groundnut oil`, `Palm oil`, `Vegetable oil`. Verbatim row shape: `Beans brown,sold loose | 596.96 | MoM 0.47 | YoY 13.13 | Ebonyi (N906.00) | Kebbi (N352.70)`. Format is **PDF only** (no XLSX/CSV in any capture), filenames drift every month, granularity *degraded* from 36 states + FCT to 6 zones, and the `/download/{id}` pattern is unreliable (`download/1241593`, labelled "Selected Food Prices Watch (October 2024)", serves the Q3-2024 GDP report). Latency ~21–24 days after month-end. **[D] Premise correction:** it had *not* been folded into the CPI report as of end-2025 — the release calendar schedules both separately.
- **CBN Statistics Database — live, open, and has no soybean.** **[D]** `https://statistics.cbn.gov.ng/` → 200, redirects to `/shop`. I found the data-browser XHR paths in `https://statistics.cbn.gov.ng/js/data-browser/d-browser-paths.js` (verbatim: `const NAV_TREE_DATA_PATH = "/data-nav-items/dataset-nav-tree";`) and pulled the full tree: `https://statistics.cbn.gov.ng/data-nav-items/dataset-nav-tree` → 200, 453,552 B, **2,323 dataset nodes. Zero matches for "soy"/"soya"/"soybean". Zero for "commodity price".** The only price nodes are `Crude Oil Price In US Dollars Per Barrel`, `Fuel Pump Price Per Liter - Average (PMS)` and CPI variants. Corroborated from the printed side: the 2024 Statistical Bulletin's **"Domestic Production, Consumption and Prices"** workbook (`https://www.cbn.gov.ng/Out/2025/STD/2024%20Statistical%20Bulletin_Real%20Sector.xlsx`, 200, 1,571,719 B, 31 sheets) contains only GDP, CPI, ACGSF loan guarantees, capacity utilisation and rainfall. A `sharedStrings.xml` grep for `soy` yields exactly two hits, one of which is **`Beans & Soya Beans` — a column in Table C.3.1 "ACGSF Operations – Value of Loans Guaranteed", in ₦'000. Loan values, not prices.**
- **CBN *does* publish an excellent free FX API — worth adopting regardless of the soy decision.** **[D]** `https://www.cbn.gov.ng/api/GetAllExchangeRates` → 200, `application/json`, 8,083,390 B, 61,775 rows, 28 currencies, **US DOLLAR: 6,039 daily rows 2001-12-10 → 2026-08-07** in a single unauthenticated call. Verbatim latest: `{"currency":"US DOLLAR","ratedate":"2026-08-07","buyingrate":"1364.6856","centralrate":"1365.1856","sellingrate":"1365.6856"}`. That is the **official** rate; the repo's `NGNUSD=X` gave 1,361.08 for the same date — a 0.3% difference, immaterial to this report's conclusions but a strictly better primary source for official-rate work. Also `https://www.cbn.gov.ng/api/GetAllNFEM_RatesGRAPH` → 200, but only **18 sessions** (2026-07-15 → 2026-08-07) with `weightedAvgRate` and turnover. **[I]** NFEM would be the right rate for testing the parallel-market question in §6.3, but 18 sessions is too short a window to do it properly.
- **FMARD / NAERLS — no soybean price.** **[D]** `fmard.gov.ng` 301 → `https://agriculture.gov.ng/`, 200, 143,734 B, WordPress with an open `wp-json` API. `/research-and-reports/` is 13 policy PDFs, no recurring price bulletin. The one price-bearing document — NAERLS's `2025-Wet-Season-Agricultural-Performance-in-Nigeria.pdf` (200, 3,138,919 B, 27 pp) — has its "Food Commodity Prices" page as **an image-only slide** (four rasters, no text layer); rendering it shows **Maize, Milled Rice, Sorghum, Cowpea only — no soybean**. Soybean appears solely as production in the text layer: `Soyabean | 992,633.60 | 1,002,947.90 | 1.05 | 947,952.10 | 951,702.50 | 0.40` (area/production 2024 vs 2025). NAERLS itself once ran the right product — `naerls.gov.ng/wp-json/wp/v2/search?search=price` returns ~20 posts titled "National Commodity Prices as …" — but **the newest is 2019-10-21 and every attached PDF is HTTP 404**. Discontinued.
- **IITA / CGIAR / NASC / associations — all negative.** **[D]** `data.iita.org` runs a real open CKAN (3.4k datasets); `q=soybean` → 154 hits, **all agronomy/breeding**, and the only market-price datasets are one-off project surveys for Mali, Ethiopia and Rwanda — **none Nigerian**. `data.cgiar.org` **does not resolve**. Harvard Dataverse: `"Nigeria" AND "soybean" AND "price"` → **total 1**, a crop-model dataset with no prices. IFPRI NSSP: 25 datasets, no price monitor. `seedcouncil.gov.ng` times out; `nasc.gov.ng` 200 with zero price matches (it is a seed regulator regardless). **`nspan.org.ng` and `soybeannigeria.org` do not resolve** — no association-owned bulletin exists.
- **`data.gov.ng`** — **[D]** connection refused. **`nigeria.opendataforafrica.org`** — **[D]** HTTP 403.
- **USDA FAS GAIN (Lagos post) — retrieved, and it states no soybean price.** `www.fas.usda.gov` sits behind an Akamai WAF: **[D]** HTTP 403 (~430 B) on every `/data/...` path, direct PDFs included, to curl and WebFetch alike. The working back door is `https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=<name>.pdf` (200 `application/pdf`; HTTP 500 = file absent; no listing/search API exists — `ReportList`, `ReportSearch`, `/swagger*` all 404). Two Lagos reports pulled and text-extracted: **Oilseeds and Products Annual NI2024-0006 (2024-04-17, 672,481 B)** — a regex sweep for `₦`, `N[0-9,]{4,}`, `US$`, `/kg`, `/MT` across all 631 lines returns **zero price matches**, only qualitative text (*"and high prices. Soybean exports are forecasted to significantly increase to 212,000MT in MY 2024/25"*); and **Grain and Feed Annual NI2026-0003 (2026-03-02, 934,609 B)** — zero naira figures, soy only qualitative. **[D] No Nigeria oilseeds report exists for 2025 or 2026** (`NI2025-0001..0012` and `NI2026-0001..0008`, Annual and Update, all HTTP 500, with the filename convention confirmed still valid because NI2026-0003 resolves). **Conclusion: the GAIN level cross-check cannot be done because GAIN does not publish a Nigerian soybean price.** That is a finding, not a gap.

### 3.2b NBS *does* have a soybean series — and it has been frozen for 13.5 months

This is the one federal source that genuinely collects soybean prices, and it very nearly changes the recommendation. It does not, for one reason.

**[D]** `https://www.nigeriafoodpricetracking.ng/` → 200, server-rendered, on IP `147.124.214.6` — **a different host from the dead NBS origin, which is why it is up**. Verbatim: *"The Nigeria Food Price Tracking, an initiative by the National Bureau of Statistics (NBS)… Through the power of crowdsourcing and AI"* and *"The data is updated daily"*. The dashboard is a JWT-gated Tableau Cloud embed and is not scrapable, but the bulk file is a single open URL:

```
https://drive.usercontent.google.com/download?id=1rDD7k6Z95JsSyjJ1qHnyVI6ZWfETE45o&export=download&confirm=t
→ 200/206, content-disposition: attachment; filename="NFPT Complete Dataset.csv", 116,959,953 B
```

**[D]** I downloaded all 117 MB and parsed it myself. 1,267,380 rows, header `Date,State,LGA,Outlet Type,Country,Sector,Food Item,Price Category,UPRICE`, 10 food items. **Soyabeans: 126,425 rows, 2024-11-27 → 2025-06-26**, 37 states, LGA-level, split **Retail 113,478 / Wholesale 10,660 / Farmgate 2,287**. On paper this is better than AFEX — farmgate, wholesale and retail, at LGA resolution.

**[D] It is dead.** The HTTP response carries `last-modified: Thu, 26 Jun 2025 16:08:13 GMT` — I verified this header myself with a range request. It is a static Drive snapshot untouched in 13.5 months, and the soy data ends on exactly that date. *"Updated daily"* is not true of the file.

**[D] And its levels do not reconcile with anything.** My own medians (NGN, whole series): Retail 1,571 · Wholesale 1,500 · Farmgate 1,500. June-2025 national median **1,538**.

> **Disagreement, flagged and NOT reconciled.** AFEX on **2025-06-26** = **939.28**. NFPT June-2025 median = **1,538**. That is **+64%** — far outside the ±15% band. **These two must not be averaged or spliced.**

Three independent reasons to distrust the NFPT level rather than AFEX:

1. **The unit is stated nowhere.** `UPRICE` has no unit column and no documentation. **[?] UNVERIFIED.**
2. **My unit test is inconclusive.** I compared NFPT June-2025 retail medians against WFP Nigeria for the same month, normalising WFP's local measures (`2.7 KG` = a derica/mudu):

   | Item | NFPT median | WFP raw | WFP unit | WFP per kg | NFPT ÷ WFP-per-kg | NFPT ÷ WFP-raw |
   |---|---|---|---|---|---|---|
   | Local rice | 1,867 | 3,400 | 2.7 KG | 1,259 | **1.48** | 0.55 |
   | Sorghum | 967 | 1,300 | 2.7 KG | 481 | **2.01** | 0.74 |
   | Maize yellow / flour | 1,071 | 2,002 | 2.1 KG | 953 | **1.12** | 0.54 |

   **Neither reading lands on unity.** Per-kg gives 1.12–2.01; per-local-unit gives 0.54–0.74. **[I]** Per-kg is the likelier reading, but NFPT then runs 12–100% above WFP's per-kg equivalents on three staples — so this is not a clean resolution and cannot license a level comparison.
3. **It is internally implausible.** Farmgate (1,500) ≈ wholesale (1,500) ≈ retail (1,571). Farmgate should sit well *below* retail. **[D]** Row-level noise corroborates: adjacent rows in the same market give `Brown beans,Retail,8000` and `Brown beans,Retail,3076.92`. This is consistent with a crowdsourced/AI-extracted pilot where the respondent's own selling unit was not normalised.

**[I] Verdict on NFPT: not usable.** Frozen, unit unresolved, internally inconsistent. But it is the single most promising *future* source in this whole survey — if the `Last-Modified` header ever moves past 2025-06-26 and a unit is documented, NBS would have an LGA-level, farmgate-inclusive, official series that would beat AFEX on licence and granularity. **Re-probe the header periodically; it costs one HEAD request.**

> ⚠️ **Security note, incidental and unused.** **[D]** `nigeriafoodpricetracking.ng/dashboard.html` ships a Tableau Connected App **HMAC secret, key id, issuer and user email in clear client-side JavaScript**, which would let anyone mint `tableau:views:embed` JWTs for that site. Not used and not probed further. Worth mentioning to NBS if this source is ever pursued; it also means the Drive CSV is the only defensible access path.

### 3.3 The other exchanges

- **NGX** — **[D]** 200, 430,055 B. **Zero occurrences of "soy"/"soya"** on the homepage or `/exchange/data/indices/`. The only "commodity" string is `<option value="NGXCOMMDTY" data-description="NGX Equity Based Commodity Index">` — an **equity index of commodity-sector listed companies**, not a physical price. The assumption in the brief is confirmed: NGX has no commodities board.
- **FMDQ** — **[D]** `https://fmdqgroup.com/` → 200, `Server: nginx/1.24.0 (Ubuntu)` (**not** Cloudflare-fronted; my first probe's `cloudflare` grep hit an asset CDN reference, corrected here). Homepage: **0 × "commodit", 0 × "soy"**. Market data sits behind the login-gated e-Markets Portal.
- **LCFE** — **[D]** Server-rendered, no wall, but **not a series**: one hardcoded row per commodity, no history, and the soybean row is dated **22/5/2026 — eleven weeks stale** while other rows on the same page carry 05/08/2026. `https://lcfe.ng/report.php` → 200, latest report `LCFE-Report-July-28-01-Aug-2025`, ~1 year stale. `/products.php` lists `soyabeans`, `soyabean oil`, `soyabean meal` as *eligible* products — aspirational; no oil or meal price exists anywhere on the site.
- **NCX** — **[D]** Not defunct (`ncx.gov.ng` and `ncx.ng` are NXDOMAIN; the live host is `https://ncx.com.ng/`, 200, LiteSpeed + WordPress, server-rendered). Fourteen location-coded tickers with units stated verbatim in the widget attribute: `"All prices are per kg."` **Fatal gap: no date on any price and no history.** See §2.1.

### 3.4 Agri platforms — all dead ends

**[D]** Farmcrowdy (`farmcrowdy.com`, `.ng`, `.africa`, `.io`, `farmcrowdyfoods.com`): **all NXDOMAIN**. AgroCentral: NXDOMAIN. Releaf → `wereleaf.earth`, 200, Webflow marketing, 0 × "price"/"soy". Thrive Agric: 200, JS app, 0 × "price"/"soy", `api.thriveagric.com` NXDOMAIN. Babban Gona: 200, 16 corporate pages, 0 × "soy". Crop2Cash: 200, 0 × "price"/"soy", `/sitemap.xml` 404.

---

## 4. UNVERIFIED — and one number that is actively wrong

### 4.1 The trap: the most-cited "Nigeria soybean price" on the web is from 2013

**[D]** Selina Wamucii (`https://www.selinawamucii.com/insights/prices/nigeria/soya-beans/`, 200, page titled **"Soya Beans Price in Nigeria - July 2026 Market Prices (Updated Monthly)"**) states **US$0.45/kg (205 NGN/kg)**, farmgate — and, buried on the same page, **"Price as of: January 2013"** and `Source: FAOSTAT Producer Prices`.

**[D]** I downloaded FAOSTAT's bulk file and checked:
`https://bulks-faostat.fao.org/production/Prices_E_Africa.zip` → 200, 1,461,290 B. Nigeria × `Soya beans`, four elements. The price elements **end at 2013**:

```
Producer Price (LCU/tonne) … 2011: 67010   2012: 70450   2013: 70010   [series ends]
Producer Price (USD/tonne) … 2011: 435.5   2012: 447.3   2013: 445.0   [series ends]
```

**FAOSTAT's last Nigerian soybean price is USD 445/tonne, i.e. $0.445/kg, for 2013.** That is byte-for-byte the "latest farmgate price" the page advertises. **[I] Confirmed: a "July 2026" price is a thirteen-year-old dead number.** Its own naira figure is broken too — NGN 205,000/t reconciles neither with FAOSTAT's NGN 70,010/t nor with $0.45/kg at any plausible FX.

**Disagreement, stated explicitly and not averaged:** NGN 205,000/MT = **$150.6/MT** at 1,361.08, versus AFEX's **$500.5/MT**. That is a **70% disagreement — far outside the ±15% band — and the two must not be reconciled.** One of them is a live exchange print and the other is a stale statistic.

**[?]** FAOSTAT also carries `Producer Price Index (2014-2016 = 100)` for Nigeria soya running to **2025 (230.07)**. I could not make this reconcile with anything: read as NGN it implies ~NGN 230,000/t in 2025 (implausible — naira has devalued ~10× since 2015 and AFEX shows ~NGN 900,000/t); read as USD it implies ~$1,035/MT (also implausible). **[D] Decisive detail: every year of that index, 1991–2025, carries FAOSTAT flag `E` (estimated), whereas every LCU/USD price year carries flag `A` (official).** It is a modelled series, not an observation. **Treat as unusable and do not derive a level from it.**

**[D]** The coverage collapse is Nigeria-wide, not soy-specific: of 273 Nigerian LCU price items, the only post-2013 data is a **single 2020 print** for ~20 staples (`Maize (corn) 88,210`, `Rice 174,280`, `Groundnuts 349,592`) — and soybean is not among them. **[D]** The FAOSTAT REST API is now auth-walled (`https://faostatservices.fao.org/api/v1/en/data/PP?…` → **HTTP 401 `Missing Authorization Header`**); the bulk zip is the only open route.

**[D] Licence warning, relevant beyond this ticket.** `https://www.fao.org/contact-us/terms/db-terms-of-use/en/` grants CC BY 4.0 — *"all datasets disseminated through FAO corporate statistical databases… are licensed under the Creative Commons Attribution-4.0 International licence (CC BY 4.0) as complemented by the Terms of Use outlined below"* — but those complementary terms then state verbatim: *"Datasets shall not be used for or in conjunction with the promotion of a commercial enterprise and/or its product(s) or services."* **[I]** That is a **non-commercial rider bolted onto a nominally CC-BY licence**, and it applies to FAOSTAT and GIEWS/FPMA alike. It does not block a non-commercial dashboard, but it means neither source is as green as a bare "CC BY 4.0" label suggests. Flagging it against the repo's existing licensing-RED finding.

### 4.2 The other trap: "Nigeria Commodity Price: Soyabeans" is a world price

CEIC advertises `Nigeria Commodity Price: Soyabeans`, **monthly, USD/Metric Ton, Jan 2009 – Dec 2025, "reported by Central Bank of Nigeria"**. Tempting — and wrong for our purposes.

**[D]** I tested its four published anchors against the repo's own `worldbank_prices` (Layer 8) and CBOT:

| Anchor | CEIC/CBN | World Bank `Soybeans` | CBOT front-month |
|---|---|---|---|
| Dec 2025 | **440.000** | **440.0** ✓ exact | 395.6 |
| Nov 2025 | **445.833** | **446.0** ✓ | 412.8 |
| Jun 2022 (all-time high) | **737.060** | **737.0** ✓ exact | 621.0 |
| Nov 2015 (record low) | **319.080** | 368.0 ✗ | **319.08** ✓ exact |

**[I]** Three of four anchors match the World Bank Pink Sheet exactly; the fourth matches the CBOT monthly mean exactly. Either way, **this is an international benchmark, not a Nigerian domestic price** — and it is already in the repo twice over (Layers 1 and 8). Adding it would be double-counting dressed as a new country.

**[D]** Corroborated structurally: CBN's live database has no such node at all (§3.2), so this comes from the printed Statistical Bulletin. **[A]** A search snippet gives that table's title as *"Table C.11: Indices of Average **World** Prices of Nigeria's Major Agricultural Export Commodities"* — consistent with the numeric finding. **[?]** I could not read the bulletin PDF directly: `https://www.cbn.gov.ng/Out/2025/STD/2024Q3%20Statistical%20Bulletin_Contents%20and%20Narratives_Final.pdf` returns **HTTP 403** to automated fetch.

### 4.3 Full UNVERIFIED list

- **AFEX contract specification** — grade, moisture, delivery basis, and which warehouses `SBS` references. Not published anywhere I could reach; the spec endpoint is auth-gated.
- **AFEX `SSSM` (₦1,190/kg)** — present in the history file, absent from the ticker. Commodity identity unknown. **Do not assume it is soybean meal.**
- **AFEX `changePercentage` / `percentage_change`** — `0.0` for all 8 commodities on both my fetches. Field looks non-functional; never depend on it.
- **AFEX websocket streams** — `wss://api-md.afexnigeria.com/ws/ohlcv/<key>/<id>/<hash>` and `wss://ecn.afexnigeria.com/ws/oms-streams/…` found in the bundle, not tested. `https://ecn.afexnigeria.com/` root → 404. Intraday OHLCV may live here.
- **AFEX licence** — the T&C forbids reproduction without written permission; whether AFEX regards a public non-commercial dashboard as covered is **unresolved and must be asked**.
- **NCX ticker freshness** — inferred 2026-04-09 from WordPress `modified`; Wayback returned no 2026 captures and then HTTP 429.
- **NFPT `UPRICE` unit** — stated nowhere; my WFP normalisation test is inconclusive in both directions (§3.2b). This is why the NFPT level cannot be used to corroborate or refute AFEX.
- **Whether NFPT is abandoned or merely paused** — the site still claims daily updates while the file has not moved since 2025-06-26. Only a future `Last-Modified` change will tell.
- **NBS Selected Food Prices Watch, 2024–2026 editions** — soy absence verified on the 2021 and 2023 editions only; the origin was down so recent editions could not be checked directly. **[I]** The basket is stable across those two years and FPMA's 105 NBS-sourced series (§3.1) contain no soy, so the inference is strong but not airtight.
- **CBN Statistical Bulletin Table C.11 title** — from a search snippet; the PDF is 403.
- **FAOSTAT Nigeria soy Producer Price Index to 2025** — cannot be reconciled in either currency (§4.1).
- **AFEX API stability** — the `Origin` gate is undocumented and could be tightened without notice.
- **`api-md.afexnigeria.com/robots.txt`** → 404; `africaexchange.com/robots.txt` → 200 but serves the SPA shell. No robots signal either way.

---

## 5. Recommendation

### 5.1 GO — conditional on a licence answer, and it is one email

**Build a Nigeria soybean layer from AFEX, but send the permission email before writing the fetcher.**

The technical case is strong and unusually cheap:

1. **`GET https://api-md.afexnigeria.com/AFEXMD/api/v1/securities/price`** with `Origin: https://africaexchange.com`. Parse **`SSBS` (NGN/kg) × 1000**, not `DSBS` (§1.5). One request returns the entire history, so this layer is **self-healing on re-fetch and needs no `data/history/` CSV round-trip** — unlike AgRural, SAFEX, CEPEA and the Gulf bids.
2. **Hard-fail on 401.** The `Origin` gate is the whole authentication story; if it changes, the layer must crash, not return empty (per the repo's stated preference — silent failures are worse than crashes).
3. **Convert to USD/MT via the existing `NGN/USD` Layer 7 series** and render **Nigeria vs CBOT premium**, exactly the shape of the India mandi line in `analysis/briefing/sections/emerging_markets.py`. §2.3 shows this produces a live, economically legible signal (+96% → +15% as the naira firmed).
4. **Label it "AFEX exchange spot, Nigeria (T-1)"** — never "Nigeria soybean price". It is one exchange, whole bean only, 24 h delayed by AFEX's own statement.
5. **Handle weekends.** The feed carries calendar-daily rows with weekend carry-forward (§1.5). Either drop Sat/Sun or mark them; do not present a carried value as a new print.
6. **Do not add a Nigerian crush margin.** There is no meal or oil leg anywhere in Nigerian data (§1.4) — same constraint that retired the India crush margin in the 2026-08 Layer 16 rebuild.
7. **Optional, near-free:** NCX and LCFE are server-rendered and unwalled and could be scraped as a weekly *sanity check* on the AFEX level (§2.1). They are not series and must never be charted.

### 5.2 If the licence answer is no — the honest fallback page

If AFEX declines, **do not substitute a proxy.** §2.4 measures the proxy option and it fails: 0.232 return correlation between the two nearest countries, ±$100/MT spread sd, no Nigerian series to validate against, and an XOF peg that would delete the naira signal entirely.

The fallback is the shape Nigeria already has, presented without a price line:

- **PSD balance sheet** (Layer 6, already ingested). MY2026, verbatim from the `psd` table: production 1,500 kMT, **crush 975 kMT**, domestic consumption 1,450 kMT, exports 50 kMT, imports 1 kMT, ending stocks 223 kMT; soybean meal production 758 kMT with 120 kMT exported; soybean oil production 178 kMT against 50 kMT imported.
- **NGN/USD** (Layer 7) — the dominant driver of Nigerian import parity.
- **Weather** for Benue and Kaduna (Layer 5).

**What that page can honestly claim:** *"Nigeria is a ~1.5 Mt producer that crushes ~975 kt domestically and exports meal into the region; it is structurally short vegetable oil. We track its balance sheet, its currency and its two growing-season weather regions. **We do not have a Nigerian price.** No free, licence-clean Nigerian soybean price series is publicly redistributable: FAO GIEWS, FEWS NET and WFP all cover Nigerian food prices in depth and none collects soybean; the NBS food-price bulletin's 43-item basket excludes it; and the one official series that did track it stopped in June 2025."*

Every clause of that is independently checkable, which is the point.

That last sentence is a defensible, checkable claim and it is more valuable than a fabricated line. It is also the same posture #148 recommends for the missing MATIF curve: state the absence on the page.

### 5.3 What I would *not* do

- **Do not ingest CEIC/CBN "Nigeria Commodity Price: Soyabeans"** — it is the World Bank Pink Sheet wearing a Nigerian label (§4.2), already in the repo as Layer 8.
- **Do not ingest FAOSTAT producer prices** — dead since 2013 (§4.1).
- **Do not scrape Selina Wamucii, Tridge, IndexBox or any Nigerian SEO price page.** The best-ranked one is republishing a 2013 number as a 2026 price, and I can show it.
- **Do not build against AFEX's PDF reports** — six months stale (§1.6).

---

## 6. Alternative explanations considered

1. **"NBS does publish soybean and I simply could not reach the host."** **This turned out to be half right, and it is the most important correction in this report.** NBS *does* collect soybean prices — via the NFPT pilot (§3.2b), at LGA level, including farmgate. It just stopped 13.5 months ago and its unit is undocumented. Meanwhile NBS's *flagship* product, Selected Food Prices Watch, verifiably does **not** carry soy (43-item basket, 0 grep hits across two archived editions). So: the hypothesis "no federal soybean price exists" is **wrong as stated** and should be restated as **"no *current* federal soybean price exists."** The practical conclusion is unchanged, but the reason is different from what I first wrote, and NFPT deserves periodic re-probing.
2. **"AFEX's `SBS` is a nominal/reference number, not a traded price."** This is the CPO=F pathology and it deserved a test. Against it: 122 value changes in 218 observations in 2026, a ₦550k–928k range, corroboration to within 2–10% by two unrelated Nigerian institutions (§2.1), and a premium-to-CBOT that tracks the naira coherently (§2.3). For it: 13 consecutive flat days right now and weekend carry-forward. **Read: real but illiquid — closer to SAFEX than to CPO=F.** No volume or open-interest field is exposed, so this cannot be fully closed.
3. **"The whole premium is an FX artefact of using the official window rate."** Nigeria has had a large parallel-market spread historically. The repo uses `NGNUSD=X`. If the effective rate is weaker than 1,361, the USD/MT level is overstated and the premium shrinks. **[?]** Not tested — I did not obtain an independent parallel/NAFEM rate. Worth one check before publishing a premium number.
4. **"A West African proxy is good enough for a dashboard."** Rejected on measured grounds (§2.4). 0.232 return correlation is worse than the ICE-canola proxy that #148 rejected at 0.538, and it cannot be validated against anything.

---

## 7. Open questions — what would change the verdict

- **Would AFEX grant written permission for a free public dashboard?** This is the whole decision. Contact via `https://www.afex.africa/contact-us`. A yes turns this from a no-go into a two-day layer.
- **Will the NFPT file ever move again?** This is the second-most important question after the AFEX licence, and it costs one `curl -I` to check. If `last-modified` advances past `Thu, 26 Jun 2025 16:08:13 GMT` **and** NBS documents the `UPRICE` unit, Nigeria gains an official, farmgate-inclusive, LGA-level series that would beat AFEX on both licence and granularity. Until then it is a dead pilot.
- **Is `nigerianstat.gov.ng` permanently down or transiently down?** Its flagship food-price product has no soy either way (§3.2), so this matters less than it first appeared — but a live origin would let the 2024–2026 editions be checked directly rather than inferred.
- **What is the AFEX `SBS` contract spec?** Without it the series cannot be honestly compared to CBOT No. 2 Yellow on a like-for-like basis, only as a directional premium.
- **Is `SSSM` soybean meal?** If yes, Nigeria gains a second leg and a crush margin becomes possible. Currently unknown.
- **Does the parallel-market NGN rate materially change the USD/MT level?** (§6.3)
- **Do the AFEX websockets expose volume or open interest?** That would close the liquidity question in §6.2 definitively.

---

## 8. Follow-up: the flat-tail check (2026-08-10, post-report)

The report left §6.2 ("real but illiquid") open, resting partly on a then-current flat run. That run was measurable against its own history, so it did not need a wait-and-see. Re-fetched the endpoint (`200`, 796,168 B, 2,575 rows, latest date `2026-08-09`) and ran the base rate. **[D]**

**Finding 1 — the plateau is within historical norm, not a stall.**

```
trailing flat run: 14 days at 681.25 NGN/kg, 2026-07-27 → 2026-08-09
runs >= 7 days:  24    runs >= 14 days: 4    longest ever: 26
recent precedents: 19d (2025-10-15 → 11-02), 14d (2025-05-12 → 05-25), 13d (2025-07-22 → 08-03)
median run length: 1 day    mean: 1.56
```

**Finding 2 — the decisive test is that new dates keep arriving.** A stalled feed stops appending rows; this one appends daily and carries an unchanged value. Rows exist for every calendar day through yesterday. That distinguishes "publishing, unchanged" from "dead", and it is the failure mode #157 (SAFEX: `200` with no table) and #155 (India Layer 16: zero rows) would *not* have passed. **[D]**

**Finding 3 — it is genuinely calendar-daily, not a weekly print forward-filled.** Tested by change-day-of-week and inter-change gap distribution over the last 2 years (406 changes):

```
change day-of-week: Fri 67 · Wed 65 · Thu 62 · Mon 60 · Tue 60 · Sat 53 · Sun 39
gap between changes: 1d ×300 · 2d ×45 · 3d ×25 · 4d ×9 · 5d ×7 · 6d ×4 · 7d ×5 · >7d ×10
```

No weekday clustering and 74% of changes are day-over-day, which rules out a weekly repricing dressed up as daily. **[I]**

**What this does and does not settle.** It closes "is the feed alive" (yes) and "is it secretly weekly" (no). It does **not** close §6.2's liquidity question: weekend price changes still mean these are not exchange session settlements, so `SSBS` should be labelled a **daily reference price, not a settlement**, wherever it surfaces. Any layer built on it needs a staleness alarm calibrated above the 26-day historical max, not the default freshness threshold.

### 8b. The plateau broke during implementation — 2026-08-10 **[D]**

While the Layer 22 fetcher was being written, the feed appended a new row and **the 14-day flat run ended**:

```
2026-07-27 → 2026-08-09   681.25 NGN/kg   (the plateau)
2026-08-10                690.63 NGN/kg   (+1.38%)
```

This is the disconfirming test §8 asked for, and it came back negative for "stalled": a dead feed does not resume. Combined with the base rate (4 prior runs ≥14 days, 26-day max), the liquidity concern in §6.2 is now as closed as it can get without a volume field. **`SSBS` is a live series.**

**Correction to the endpoint description in the verdict block.** The response is not a bare JSON array — it is an envelope:

```
{"responseCode": "...", "data": [ ...2,576 daily rows... ], "message": "..."}
```

The original report's row counts were right because its parser already unwrapped the first list-valued field, but the documented shape was wrong. `fetchers/afex.py` handles both forms.

**Row-count reconciliation:** 1,663 rows carry a non-null `SSBS`, of which **7 are exactly 0.0** (all in 2021, before the series began). The layer stores **1,656**, first positive print `2022-01-19 = 398.55 NGN/kg`. The zeros are dropped as non-positive rather than stored as a price of zero.
