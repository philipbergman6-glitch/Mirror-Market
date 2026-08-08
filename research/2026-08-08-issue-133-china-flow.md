# Issue #133 (X3) — China flow: GACC customs / reserve auctions / hog demand — go/no-go

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/133 (umbrella #130)
- **Research date:** 2026-08-08 (all URLs accessed this date unless noted)
- **Question:** The structural China gap is *flow, not price*. Can (1) monthly GACC soybean import volumes, (2) Sinograin reserve-auction results, and (3) a hog/feed-demand proxy be sourced free, machine-readable, and licence-clean?
- **Method:** Three parallel research streams with live endpoint tests from a US IP (throwaway scripts in the scratchpad; nothing written to the repo). Every claim tagged **[observed]** (fetched/parsed directly this session), **[inferred]**, or **[assumed]**; "couldn't verify" stated where true. ≥2 independent sources per load-bearing claim.
- **CI caveat (applies throughout):** all access tests ran from a US residential IP + Anthropic US fetch infra, not a GitHub Actions runner. Every GO below carries a "smoke-test from CI first" precondition (the Comex Stat 403 lesson from issue #45).

## Verdict summary

| # | Candidate | Best route | Licence | Go/no-go |
|---|---|---|---|---|
| 1a | GACC monthly soybean imports — **total** | GACC English "Major Imports" HTML table | GRAY | **GO** |
| 1b | GACC monthly imports — **by origin** | UN Comtrade (China monthly, HS 1201, by partner) | GRAY | **NO-GO as live feed**; optional one-shot backfill to 2024 |
| 2 | Sinograin reserve auction results | grainmarket.com.cn JSON API (offers only) / esinograin.com | **RED** | **NO-GO** (licence + results not public); manual/HITL fallback |
| 3 | Hog/feed demand proxy | akshare (Layer 9 infra): DCE `LH0` + soozhu hog/corn spot + NDRC ratio | GRAY (same as Layer 9) | **GO** |

---

## 1. GACC customs — monthly China soybean imports

### 1a. GACC English site, monthly "Major Imports" table — **GO (total tonnage only)** — GRAY

- **[observed]** Index: http://english.customs.gov.cn/statics/report/preliminary.html → per-month GUID article, e.g. Jun 2026 = http://english.customs.gov.cn/Statics/e821dd30-c0f5-4455-981a-d59f307f3237.html — "(6) China's Major Imports by Quantity and Value (in USD)". Plain server-rendered HTML table, fetched with bare `curl` from a US IP: no captcha, no JS, no key.
- **[observed]** Verified row: `Soya beans | 10,000 Tons | 1,354.7 (month) | 6,337.0 (YTD)` = 13.547 MMT June / 63.37 MMT Jan–Jun 2026. Independent cross-check: eFeedLink 2026-07-16 "record June soybean imports of 13.55 million tonnes" per customs data (https://www.efeedlink.com/contents/07-16-2026/fd974b43-20ba-4e2c-8e9d-f2338a4e09c7-0001.html). Two sources agree.
- **Cadence/lag:** monthly, ~2–3 weeks after month end **[inferred]** — index showed Feb–Jun 2026 posted, Jul absent on 2026-08-08; Jan is folded into the Feb release (lunar-new-year practice) **[observed/inferred]**.
- **Format quirks [observed]:** units are 万吨 ("10,000 Tons" → ×10,000 = MT); GUID article URLs force scraping the index each run (index is static HTML with a year `<select>`, 2018–2026); table markup is old ASP.NET nbsp-soup — parse by row label "Soya beans". HTTPS cert chain is self-signed — fetch via plain http or `curl -k` equivalent; a CI note, not a blocker.
- **Access walls [observed]:** the interactive stats portal `stats.customs.gov.cn` (incl. `/indexEn`) returns **HTTP 412** to curl — hostile anti-bot wall; the English static articles do not.
- **Licence: GRAY.** No usage terms found on the statement pages **[observed]**; PRC government statistical publications carry no explicit open licence; the figures themselves are facts **[inferred]**. Same class as other GRAY scraped layers (CEPEA-via-NA, SAFEX).
- **Fragility: MEDIUM** — GUID indirection + possibility of later geo-blocking (couldn't verify from a GH runner).
- **Not in this table:** origin split. The English Monthly Bulletin's country tables are country × HS-division only, not soybean-by-origin **[observed]**.

### 1b. By-origin routes — no free machine-readable live feed exists (2026)

- **UN Comtrade** — **[observed, tested]** `https://comtradeapi.un.org/public/v1/preview/C/M/HS?reporterCode=156&cmdCode=1201&flowCode=M&period=202412` returns full partner detail (World 7.94 MMT; US 4.255; Brazil 2.935; Uruguay, Canada, Russia, Argentina, Ethiopia, Chile — netWgt kg), no key, 500 records/call (free registered key: 500 calls/day — https://uncomtrade.org/docs/how-to-create-an-account/, https://docs.ropensci.org/comtradr/articles/comtradr.html). **Lag is fatal [observed]:** the data-availability endpoint lists China monthly datasets 201001→**202412 max** (~20-month lag, with gaps; 2025–26 queries return empty). **Licence GRAY [observed, quoted]:** https://uncomtrade.org/docs/policy-on-use-and-re-dissemination/ — "internal use only… may not be re-disseminated… without UNSD's permission", but free data-visualization/analytics applications are exempt from licence fees if they offer no significant bulk-download. A dashboard chart likely fits the exemption; **committing the raw series as a public `data/history/` CSV is arguably re-dissemination** — resolve before adopting. **Verdict: one-shot historical by-origin backfill (2010–2024) only.**
- **Detailed GACC by-origin releases** land ~18th–27th of the following month but reach the public as **prose** via Reuters/S&P/eFeedLink (e.g. https://www.spglobal.com/energy/en/news-research/latest-news/agriculture/021926-analysis-tariff-gap-likely-to-keep-chinas-soybean-imports-anchored-to-brazil) **[observed via search]** — licence RED for scraping; manual cross-check only.
- **NBS** (`data.stats.gov.cn`) — English path 404, root redirects to a JS-heavy portal **[observed]**; republishes customs aggregates, not soybean-by-origin. NO-GO.
- **Trading Economics** — terms grant a "limited, personal, nontransferable, revocable license" (https://tradingeconomics.com/terms.aspx) **[observed via search]** — **RED**. TDM/Wind/Mysteel — paid, **RED [assumed]**.
- **USDA FAS GAIN** China Oilseeds Annual (Mar 2026, CH2026-0032) + ~2×/yr updates (https://www.fas.usda.gov/data/china-oilseeds-and-products-annual-10) — public domain (**GREEN**), narrative PDF, cites GACC — validation/annotation source, not a feed **[observed via search]**.
- **Already in repo (mirror-side, keep as complement):** PSD marketing-year China imports (Layer 6, annual with WASDE-cycle revisions) and ESR weekly US-origin-only China commitments (Layer 10) — blind to the Brazil/Argentina legs **[observed — repo code]**.

**Recommendation:** new small fetcher on the GACC English preliminary index → monthly soybean row (month + YTD MT). Snapshot-ish source → `data/history/` CSV round-trip. Frame dashboard copy as "total observed monthly; origin split annual (USDA/press-cited)". Precondition: CI smoke test for geo-blocking.

---

## 2. Sinograin reserve auctions — **NO-GO** as a feed — RED

Key structural finding **[observed]**: the *imported*-soybean reserve auctions Reuters reports are run on the **National Grain Trade Center** (www.grainmarket.com.cn), not Sinograin's corporate site; *domestic* soybean purchases/sales run on Sinograin's own platform **www.esinograin.com** **[inferred from title patterns]**.

### 2a. grainmarket.com.cn — offers are machine-readable; results are not public; licence RED

- **[observed, tested]** Undocumented JSON API: `POST https://www.grainmarket.com.cn/getData` with `param={"m":"tradeCenterDaoDuNewsByPlateType","plateType":"1","articleTypeTagID":"1",...}` (announcement/lot-list/result listings, paginated to 2020; discovered in the site's own `info.js`), plus `{"m":"tradeCenterNewsDetail","articleID":...}` for article HTML + attachment URLs. Anonymous, no cookie/referer check; worked from this host and Anthropic US fetch infra.
- **[observed]** The 2026-07-31 auction (the 504k t event already in `china.yml`): announcement `articleID=1082110` — HTML table with 进口大豆 50.4万吨, crop years 2022–2025, 8 provinces, bid increment, deposit, buyer-eligibility rules; lot list `articleID=1082113` → attachment `…/ArtAccessory/20260729090826281.xlsx` — a real 50-row xlsx (lot no., depot, warehouse, crop year, quantity, province), total 503,694.083 t, parsed with pandas — matches the 504k t in newswires. Aug 5 (501,158 t) and Aug 12 auctions follow the same announcement+lot-list pattern **[observed]**.
- **The gap [observed]:** the 交易结果 (results) category contains **zero** soybean items for 2025-06→2026-08 — only weekly rice results, published as **JPG screenshots**. Soybean result articles exist only from 2020, also JPGs. Reuters' "sold ~half at avg 4,033 yuan/t" therefore comes from trade participants/paid vendors, not any public page **[inferred]**.
- **Licence: RED [observed, verbatim]** — https://www.grainmarket.com.cn/about/bqsm: "未经本网站同意…不得以任何形式擅自摘编、转载…" and "不得使用爬虫程序等非法方式抓取本站数据，不得买卖本站数据或将其用于任何商业目的" — explicit scraping + commercial-use ban. Under this project's licence discipline that is RED, not GRAY.

### 2b. esinograin.com — thin results, gated archive, licence RED

- **[observed]** Homepage lists latest 交易结果; detail API `POST https://www.esinograin.com/api/others/getTransactionAnnouncementDetail` (`jyggId=<32-hex>`) works anonymously — but the sampled result table (Aug 7 Guangdong corn) contained only 销售单位/成交率 ("合计 20%") — **no volume, no price**. Richness varies by branch (a Dec-2025 domestic-soy result republished by Mysteel did carry 5,000 t / 底价 3,950 / 均价 4,110 yuan/t **[observed via Mysteel]**); couldn't verify any 2026 油脂公司大豆 result table directly — homepage exposes only ~6 latest items, archive is member-gated.
- **Licence: RED [observed]** — legal statement (https://www.esinograin.com/resources/static/index/legalstatement.jsp) prohibits copying/scraping/republication without written authorization. Fragility HIGH (random GUID keys, homepage-only window, per-branch free-form tables).

### 2c. Republication routes — none viable

USDA GAIN: aggregates only, annual (**GREEN**, sanity-check use — e.g. the corn analogue "2023–Sep 2025: 25 MMT offered, <8 MMT sold"). dimsums.blogspot.com: no auction-results coverage in Aug 2026 posts **[observed]**. Mysteel: full numbers but opaque hash URLs, no listing/RSS, paid product — **RED**. JCI chinajci.com: broken TLS chain **[observed]** — itself a CI hazard. 粮信网 chinagrain.cn: structured auction-results section exists but detail is VIP-gated — **RED**. boyar.cn/沪粮网: announcements only, no results **[observed]**.

**Recommendation:** NO-GO for any fetcher. The cleared-volume/price signal does not exist in public structured form in 2025–26, and the offer-side portal that *is* structured carries an explicit anti-scraping clause. Honest options: (a) keep the current curated-prose approach in `china.yml`/players activity notes — manual entry of Reuters-reported figures, ~2–4 events/quarter, matching the players-spec "activity is curated dated notes, no scraping (licence RED)" decision; (b) a licensed vendor (out of scope for a free stack).

---

## 3. Hog/feed-demand proxy — **GO** via akshare (Layer 9 infra) — GRAY

Live tests **[observed, 2026-08-08, akshare 1.18.83 in a scratchpad venv, US IP]**:

| akshare function | Result | Cadence/freshness | Upstream |
|---|---|---|---|
| `futures_zh_daily_sina("LH0")` — DCE live hog futures | 1,352 rows OHLCV+settle; last 2026-08-07 settle 12,060 CNY/t | Daily, fresh | Sina Finance — **same function/upstream Layer 9 already runs daily in CI** (`fetchers/akshare.py`) |
| `spot_hog_soozhu()` / `spot_hog_year_trend_soozhu()` | National avg 10.42 CNY/kg, 2026-08-08 | Daily, fresh | soozhu.com (commercial pig-industry site) |
| `spot_corn_price_soozhu()` | 2.31 CNY/kg, 2026-08-08 | Daily, fresh | soozhu.com |
| `futures_hog_supply("猪粮比价")` — official hog:corn ratio | 305 weekly rows; last 2026-06-17 = 3.96 | Weekly, ~7 wks stale | xt.yangzhu.vip republishing NDRC |
| `futures_hog_supply("生猪产能")` — breeding-sow herd | Monthly; last 2025-10 = 39.90M head | Monthly, ~10 mo stale | xt.yangzhu.vip republishing MARA/NBS |
| `futures_hog_core("外三元")` | Daily hog price, fresh 2026-08-08 | Daily | xt.yangzhu.vip |
| `futures_pig_info` / `futures_pig_rank` | **Removed** in current akshare (AttributeError) | — | — evidence of function churn |

- **Cross-validation [observed, 3 independent sources agree ~4.0]:** akshare ratio 3.96 (Jun 17) ≈ NDRC Price Monitoring Center page 4.11 (May 13; https://www.jgjcndrc.org.cn/detail?clmId=1836667772799598593&tId=2056293101422559233 — server-rendered HTML table: hog 9.78 CNY/kg, corn 2.38, ratio, WoW) ≈ MARA April monthly 4.03 (https://www.nahs.org.cn/jcyj/scxs/202605/t20260519_472251.htm). Ratio ~4 vs 5.5–5.8 breakeven = deep-loss zone, consistent with 2026 state pork-reserve purchase news **[inferred]**.
- **Direct-official routes, for completeness:**
  - **MARA portal** (zdscxx.moa.gov.cn:8080) — reachable from US (HTTP 200) **[observed]**, self-described machine-readable, but the JSON endpoints are undocumented (probes 404'd) and some paths WeChat-gated — **GRAY, no-go as primary; manual reference**.
  - **NBS** (data.stats.gov.cn easyquery) — **403 from US IP, both CN and EN endpoints, two attempts [observed]**; third-party guides confirm non-China-network instability (https://chinadata.live/nbs-data-api/). Geo-block = disqualifying. **RED on access.**
  - **NDRC weekly ratio page** (jgjcndrc.org.cn, above) — HTTP 200 from US, HTML table, weekly; footer requires source attribution for republication **[observed]** — **GRAY-GREEN**, a viable direct fetcher if akshare's ~7-week-lagged republication is too stale. Fragility medium (ID-based URLs need a listing walk).
  - **USDA GAIN Livestock** Semi-Annual CH2026-0018 (Feb 2026) + Annual CH2025-0165 — public domain PDFs via the `newgainapi` download endpoint, carry NBS sow-herd numbers (40.78M head end-2024) — **GREEN**, semi-annual → annotation layer only **[observed]**.
  - zhuwang.com.cn republishes the NDRC ratio **as a PNG chart** with a restrictive reprint notice **[observed]** — RED. Trading Economics — RED (terms, §1b). pig333/Genesus — commentary, no machine series.
- **Licence posture:** identical to existing Layer 9 GRAY (akshare republication of Sina/commercial upstreams; tracked on #71). Fine for derived briefing analysis under the project's current discipline.
- **Fragility:** `LH0` LOW (CI-proven Sina endpoint). soozhu/yangzhu functions MEDIUM-HIGH — they churn between akshare versions (`futures_pig_info` already removed); pin-and-smoke-test needed. Reachability from GH runners unverified for soozhu/yangzhu (Sina is proven).
- **Policy anchor for framing [observed, 2 sources]:** MARA cut the normal breeding-sow target to **37.5M head, May 2026** (https://www.news.cn/politics/20260514/6c94d00ea8734c5597de32092133182a/c.html; https://www.agri.cn/zx/hxgg/202605/t20260514_8836581.htm).

**Recommendation (if a ticket is cut):**
1. Add `LH0` to the Layer 9 contract list — near-zero code, daily market-priced China hog signal; enables a DCE-native hog/corn-futures ratio and hog-vs-meal feed-margin line next to the X2 crush margin.
2. Optionally extend with soozhu daily hog + corn spot → computed hog:corn ratio vs the 5.5–5.8 band, cross-checked monthly against the official NDRC/akshare ratio.
3. Sow herd = slow context only (GAIN annual + akshare monthly with known lag); do not build a fetcher around it.

---

## 4. Candidate source table (licensing-register entries)

| Source | Content | Cadence | Format/access | Licence | Go/no-go |
|---|---|---|---|---|---|
| GACC English preliminary "Major Imports" | Monthly soybean import total + YTD (MT) | Monthly, ~2–3 wk lag | Static HTML via GUID index scrape; http/self-signed cert | **GRAY** (no terms) | **GO** (CI smoke test first) |
| stats.customs.gov.cn portal | Full by-origin queries | Monthly | **HTTP 412 anti-bot wall** | RED (hostile) | No |
| UN Comtrade API | China monthly HS 1201 by partner | Monthly, **~20-mo lag** | Free JSON API, keyless preview | GRAY (internal-use terms; viz exemption; public CSV commit questionable) | Backfill-only |
| grainmarket.com.cn JSON API | Auction announcements + lot-level xlsx (offers); results absent/JPG | Per-auction (irregular) | Undocumented POST API, anonymous | **RED** (explicit anti-scraping + commercial ban) | No |
| esinograin.com API | Domestic-soy auction results (thin: 成交率 only in sample) | Per-auction | Undocumented POST API; archive member-gated | **RED** (legal statement) | No |
| akshare `LH0` (Sina) | DCE live hog futures daily | Daily | Existing Layer 9 function | GRAY (as Layer 9, #71) | **GO** |
| akshare soozhu/yangzhu functions | Daily hog + corn spot; weekly NDRC ratio; monthly sow herd | Daily/weekly/monthly (official series lag) | pip lib; function churn risk | GRAY | GO (secondary) |
| NDRC jgjcndrc.org.cn | Weekly hog/corn prices + ratio | Weekly | Server-rendered HTML; attribution-required | GRAY-GREEN | Fallback fetcher |
| NBS data.stats.gov.cn | Quarterly hog/sow | Quarterly | **403 from US IP** | RED (access) | No |
| MARA zdscxx portal | Monthly hog data | Monthly | Undocumented JSP/JSON, partly WeChat-gated | GRAY | No (manual ref) |
| USDA FAS GAIN (Oilseeds/Livestock China) | GACC-cited imports; NBS sow herd | Annual/semi-annual | Public-domain PDF via newgainapi | **GREEN** | Annotation only |
| Trading Economics / Mysteel / JCI / 粮信网 / zhuwang | Republication | Various | Paid/paywalled/PNG/broken TLS | RED | No |

## 5. Cross-reference

- **X2 (#132):** the hog GO here composes directly with X2's DCE crush margin — `LH0` + existing corn/meal contracts give a demand-side companion line in the same DCE section, same GRAY posture.
- **#71 (akshare licensing):** LH0/soozhu additions inherit that tracking item; no new licence class introduced.
- **Players (#123/#125):** Sinograin NO-GO validates the existing design — auction activity stays curated dated notes in `china.yml` with citations; the grainmarket announcement pages found here are good *citation targets* for those manual notes (public URLs with offered tonnage) even though scraping them is off-limits.
- **2026-08 audit licensing register — new entries:** GRAY: GACC English statics, UN Comtrade (viz-exemption nuance), akshare soozhu/yangzhu upstreams, MARA portal. GRAY-GREEN: NDRC jgjcndrc. RED: stats.customs.gov.cn (412 wall), grainmarket.com.cn (anti-scraping clause), esinograin.com (legal statement), NBS-from-US (403), Trading Economics, Mysteel, 粮信网, zhuwang (PNG + reprint notice). GREEN: USDA GAIN.

## 6. Known unknowns

- GitHub Actions runner reachability: GACC English site, soozhu/yangzhu — untested from CI (Sina is CI-proven). A one-run CI smoke test is the gate for both GOs.
- Whether 2026 Sinograin 油脂公司 domestic-soy result tables ever contain volume/price (only a thin corn sample + a Mysteel-republished Dec-2025 rich sample observed).
- UN Comtrade: whether committing a backfilled by-origin CSV to a public repo falls under the visualization exemption — needs a decision (or a permission email) before any backfill ships.
- GACC July 2026 article timing — lag estimate (~2–3 wks) is inferred from one index snapshot, not a measured distribution.
