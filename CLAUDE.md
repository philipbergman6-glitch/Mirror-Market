

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Mirror Market is a commodity market intelligence platform focused on the soy complex (Soybeans, Soybean Oil, Soybean Meal) with supporting data for competing crops. It has 29 operational layers arranged in 26 numbered source groups (the split groups are 2b, 15b and 26b), covering 10 commodity futures, 10 currency pairs including ZAR/NGN, 19 weather regions including SA/Nigeria, 28 countries in PSD supply/demand, weekly export sales, forward curves, WASDE monthly forecasts, EIA biofuel/energy, USDA crush/inspections incl. port-area and destination-country flows, CONAB Brazil estimates, domestic spot prices for India/Brazil/South Africa, AgRural Paranaguá FOB, AMS CIF Gulf export bids, Argentina MAGyP official FOB, SAGIS South Africa weekly producer deliveries plus its monthly soybean supply & demand balance, EU rapeseed from the European Commission, and the USDA AMS Grain Transportation Report's ocean freight and grain-vessel lineups. Data is stored in SQLite (local or Turso cloud). All prices are displayed in **USD/MT** (metric tons) for international comparability. The public static site has 13 pages: the headline, the players map, the origin-comparison page, the futures workstation, the opportunity board, and eight market pages. A fourteenth, private edition of the opportunity board is written outside `docs/` and is never published.

## Commands

```bash
# Install runtime dependencies (uses a .venv virtual environment with Python 3.10+)
pip install -r requirements.txt
# For development (tests, lint, type-check):
pip install -r requirements-dev.txt

# Run the full data pipeline (fetches, cleans, validates, stores all layers)
python main.py

# Generate the daily market briefing
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"

# Generate the static HTML dashboard
python scripts/generate_html.py
# Open docs/index.html in your browser

# Run a single analysis module standalone
python -m analysis.briefing
```

## Required Environment Variables

- `USDA_API_KEY` — USDA NASS QuickStats API key (Layers 2, 14)
- `FRED_API_KEY` — Federal Reserve Economic Data API key (Layer 3)
- `FAS_API_KEY` — USDA FAS OpenData API key (Layer 10 — export sales)
- `EIA_API_KEY` — Energy Information Administration API key (Layer 13 — biofuel/energy)
- `DATA_GOV_IN_API_KEY` — data.gov.in personal key (Layer 16 — India mandi). **Required in CI**, optional locally. Unset, `fetchers/mandi.py` falls back to the published sample key, which is capped at 10 rows/page and shares a *global* throttle across every anonymous caller — ~19 requests per pipeline run. On 2026-08-11 four PRs merged inside 15 minutes, the four resulting runs exhausted the throttle, and Layer 16 went dark on HTTP 429 (#212). A personal key raises the page limit to `MANDI_PAGE_LIMIT_PERSONAL` (100), cutting the request count and the 429 exposure ~10×. The deploy workflow passes it through; an unset secret resolves to empty and falls back, so the fallback is a degraded mode, not a break.

Layers 1, 4, 5, 6, 7, 8, 9, 11, 12, 15, 17, 18, 19, 20, 21, 22, 23, 24, 25 work without API keys. Layer 16 works without one but only in the degraded, rate-limited mode described above. **api.data.gov.in blackholes any Python-identifying User-Agent** — it accepts the connection and never answers, so it reads as a timeout, not a 403. `fetchers/mandi.py` sends an explicit project UA; dropping it silently darkens the layer (#155).

### Optional (Cloud Database — dormant)

- `TURSO_DATABASE_URL` — Turso database URL (e.g., `libsql://your-db.turso.io`)
- `TURSO_AUTH_TOKEN` — Turso authentication token

**Decision 2026-07-30: no cloud DB.** CI persistence uses git-committed CSVs instead (see "Git-based history persistence" below). The Turso code path in `pipeline/connection.py` remains as dormant optional code for local use — it requires `pip install libsql` (deliberately not in `requirements.txt`) plus both env vars. Nothing in CI sets them.

## Architecture

The project follows a three-stage pipeline: **Fetch -> Clean/Validate -> Store**, with an analysis layer on top.

### Data Pipeline (27 Operational Layers)

`main.py` orchestrates 29 independently graded operational layers across 26 numbered groups (2b crop progress, 15b CONAB prices and 26b vessel lineups are separate run units). `config.PRODUCTION_LAYERS` is the authoritative inventory used by pipeline summaries, the masthead, About Data, health rows, and promotion smoke checks. Dict-shaped sources use `_run_dict_layer()`; scraper sources use `_run_scraper_layer()`; Layers 14–16 retain custom orchestration. Each layer is isolated so contextual failures degrade visibly without hiding healthy results.

Run state is explicit: `failed` means upstream/transport/parse failure, `no_publication` means the source ran successfully on a legitimate quiet day, `stale` means a fetched payload exceeded its observation-age budget, and `incomplete` means key coverage missed its floor. Only `success` advances `last_success`; all other states preserve the last known good timestamp. `data_freshness` round-trips through `data/history/` so a fresh CI runner does not falsely say a layer “never succeeded.”

**"Success" requires recency.** `_finalize_layer` gates a stamped `last_success` on the `LAYER_MIN_KEYS` shape floor and then the `LAYER_MAX_DATA_AGE_DAYS` observation-age budget. A stale payload is stored but classified `status='stale'`; the prior `last_success` is preserved and every public surface names the stale last-known-good state. **Not listed in `LAYER_MAX_DATA_AGE_DAYS` = not checked** — `psd`/`wasde`/`usda` are keyed by marketing year with no date column, `forward_curve` is dated by contract month, and `crop_progress` is seasonally silent. World Bank retains its own 100-day frozen-file guard. Dict and scraper paths both save first and grade second, so useful rows survive without a partial or frozen run being called complete.

**"Success" also requires rows.** `_empty_is_failure` derives whether zero rows is an outage from the configured key floor, with explicit overrides for full-history sources. Layers that legitimately publish nothing use `status='no_publication'` (crop progress out of season, a skipped inspections week, JSE holidays, mandi closures). That state advances `last_attempt` but never `last_success`, and alerts label it informational rather than confusing market silence with an upstream failure.

**"Success" also requires *every* key to have been asked.** `_run_scraper_layer` keyed its verdict on `result.has_rows` alone, so a fetch where one key returned a full set and another never answered took the success path and stamped a fresh `last_success` over a half-dark layer (#212, Layer 16 — and invisible there because `india_domestic` has no `LAYER_MIN_KEYS` floor to fall below). `FetchResult.partial(data, error)` is the fix: status `failed`, but carrying its rows. It is the same "save first, grade second" shape #157 established for recency, applied to key coverage instead — the rows are stored (for a current-day-only source, dropping them punches a permanent hole in history) and only the verdict changes. A fetcher must distinguish the two emptinesses to use it: a key that returned **zero rows** was asked and answered (a holiday) and is not an error; a key that **failed transport** was never answered, so its absence says nothing about whether data existed, and treating those alike is the silent failure this project prefers to crash over.

**"Skipped" means unconfigured, never "the upstream died."** The two API-key-gated dict layers (`export_sales`, `eia`) write no freshness row when their key is unset — the layer genuinely never ran, and either a `success` or a `failed` row there would be a lie. That skip is decided by `DictLayer.run_if`, a predicate consulted **before** `fetch()` — `fetchers.export_sales.is_configured` / `fetchers.eia.is_configured`, each reading its own module's key constant so the pipeline's answer and the fetcher's own check cannot disagree. It has to be asked of the config, because inferring it from the *result* (the old `if not data and layer.skip_msg`) conflates the two meanings of an empty return: "no key, never ran" and "key set, upstream had nothing", where recording nothing at all is right for the first and hides an outage in the second (#180). That the second case never actually fired is an accident of two fetchers' loop bodies — `fetch_all_export_sales`/`fetch_all_eia` assign `results[name] = df` unconditionally, so a live outage returns empty *frames*, not a bare `{}` — which nothing was pinning. With `run_if` ahead of the fetch, a configured layer always reaches `_finalize_layer` whatever shape its emptiness takes; both carry a `LAYER_MIN_KEYS` floor of 2+, so an all-empty run grades as a failure by the rules above.

**How much came back is recorded too.** Every freshness write carries the real `rows_fetched` plus a `(keys_returned, keys_expected)` pair, on the failure and stale paths as well — a partial outage that returned 3 of 10 keys used to record `0`. `keys_expected` is `len()` of the config catalog the layer iterates (`config.LAYER_KEY_CATALOGS`), never the payload's own length: per-key fetchers only insert *after* a successful fetch, so a weather run that lost 5 of 19 regions returns a 14-key dict and would self-report 14/14. Layers with no catalog record NULL, as do transport failures that never had a payload — NULL means "never learned", `0` means "asked and got nothing". Coverage **describes, it does not grade**: `LAYER_MIN_KEYS` stays the sole verdict, and coverage is rendered only when below full — the briefing freshness block prints `NOTE:` (or `WARNING:` if the layer also failed) and the dashboard Layer Freshness table has its own Keys column.

1. **Commodity prices** — `fetchers/yfinance.py` (10 futures: soy complex, palm oil CME `CPO=F` (settlement-marked, zero volume by design), corn, wheat, sugar, cotton, cattle, hogs)
2. **USDA crop data** — `fetchers/usda.py` (production, yield, area harvested)
   - **2b. Crop progress/condition** — weekly USDA ratings (% good/excellent, % planted/harvested)
3. **FRED economic data** — `fetchers/fred.py` (dollar index, CPI, Fed funds, Treasury 2Y/10Y/30Y, Ethanol PPI, Soybean Oil PPI, Diesel Price)
4. **COT positioning** — `fetchers/cot.py` (10 commodities including corn, wheat, sugar, cotton, cattle, hogs, ICE canola)
5. **Weather** — `fetchers/weather.py` (19 regions: US, Brazil, Argentina, Paraguay, Ivory Coast, Indonesia, Malaysia, India, Thailand, China, South Africa, Nigeria)
6. **PSD global supply/demand** — `fetchers/psd.py` (10 commodities x 28 countries incl. rapeseed complex + Canada, oilseeds + grains + cotton)
7. **Currencies** — `fetchers/yfinance.py` (10 pairs: BRL, ARS, PYG, CNY, IDR, MYR, INR, THB, ZAR, NGN)
8. **World Bank monthly prices** — `fetchers/worldbank.py` (Palm Oil, Rapeseed Oil, Sunflower Oil, etc. — current xlsx link resolved from the CMO landing page each run; the GUID deep link rotates yearly and stale links serve frozen data with HTTP 200)
9. **Chinese futures** — `fetchers/akshare.py` (8 contracts: 6 DCE incl. Corn + CZCE Rapeseed Oil/Meal — the only free daily rapeseed benchmark). Two bean contracts, not interchangeable: **No.1 (`A0`)** is the domestic non-GMO food bean (tofu/soymilk, ~700–1,100 CNY/MT premium, never crushed) and **No.2 (`B0`)** is the imported/GMO crush bean. The board crush and the vs-CBOT import-parity premium both key off No.2; No.1 is carried as a standalone China food-demand level with no CBOT counterpart (#152).
10. **Export sales** — `fetchers/export_sales.py` (weekly USDA FAS demand data — requires `FAS_API_KEY`). One marketing year per commodity, and MY starts are staggered (soy complex + corn 1 Sep, wheat 1 Jun, cotton 1 Aug). ESR answers a not-yet-started MY with HTTP 200 and an empty array, so on 1 September four of six commodities emptied at once and the layer dropped below its `LAYER_MIN_KEYS` floor for the ~8–10 days until the first in-MY report (#181). A **successfully empty** year now falls back once to `market_year - 1` (logged WARNING, never chained); a **failed** request (`_fas_get` returns `None`, not `[]`) does not fall back, so a real FAS outage still hard-fails. The recency budget bounds the fallback — prior-MY rows are frozen in the past, so a commodity that stays empty ages the layer out anyway.
11. **Forward curves** — `fetchers/forward_curve.py` (individual contract months via yfinance — contango/backwardation). Two rules keep the front of the curve honest (#61). **The current delivery month is a candidate, not a skip**: a CBOT grain contract trades until the business day before the 15th of its own delivery month, so skipping the current month started the curve one contract out for the first half of every delivery month and erased the leg that carries the structure (2026-08-12: Lean Hogs Aug 95.93 vs Oct 83.30, a 12.6-point front inversion the curve could not see). Whether that contract is still alive is decided by the data, not by an expiry-rule estimate — the nine commodities span four expiry conventions, and Yahoo *delists* an expired contract rather than serving a stale bar (verified live on ZSN26/ZCN26/LEM26/ZSK26/SBN26), so expiry arrives as an empty frame. The cost is one wasted ticker (three retries) per commodity for the back half of its delivery month. **Every leg of a curve carries the same `observation_date`**, and legs observed earlier are dropped with a WARNING: yfinance answers each contract with its own last bar, so thin deferreds and expiring fronts run days behind the liquid months (2026-08-12: every soy-oil leg on 08-11 except the expiring Aug contract, stuck on 08-10), and a curve stitched from two sessions reports yesterday's move as term structure. It is a single date, not a tolerance window — one day of drift is exactly the case that fakes the structure. `observation_date` is the session the curve was read at and is what dates it on the dashboard; `fetched_date` is only when the pipeline ran (they differ on any run landing before settlement). Legacy rows predate the column and are NULL.
12. **WASDE monthly estimates** — `fetchers/wasde.py` (USDA OCE monthly XLS — `wasdeMMYY.xls`, no API key required)
13. **EIA biofuel/energy** — `fetchers/eia.py` (ethanol production, biodiesel production, diesel prices — requires `EIA_API_KEY`)
14. **USDA crush + inspections** — `fetchers/usda.py` (monthly soybean crush volumes + weekly AMS export inspections, incl. the WA_GR101 Table C port-area breakdown → `inspection_port_flows`)
15. **CONAB Brazil estimates** — `fetchers/conab.py` (Brazil's official crop agency — production, area, yield; aggregates 27 UFs to national totals for Soybeans, Corn, Wheat, Cotton lint; coffee is in a separate CONAB file and not tracked here)
   - **15b. CONAB weekly farmgate prices** — `fetchers/conab_precos.py` (`PrecosSemanalUF.txt` — Paraná soybean producer price, R$/kg → BRL/MT). Cross-check for the CEPEA Paraná wholesale indicator (a ~10–14% wholesale-over-farmgate spread is the expected band). Own commodity key in `brazil_spot_prices` — never spliced into the CEPEA series.
16. **India domestic soy prices** — `fetchers/mandi.py` (data.gov.in Mandi Price API — official Agmarknet feed; per-state median `modal_price` series for Madhya Pradesh (Indore hub — headline benchmark) and Maharashtra (#1 producing state since 2025-26) → INR/MT, bean-only; never pooled across states). Rebuilt 2026-08 after NCDEX became unusable (SEBI derivatives suspension to ≥2027-03-31 + fingerprint wall on the spot pages; `fetchers/india_domestic.py` kept on disk as a dormant fallback). Uses the published sample key by default; set `DATA_GOV_IN_API_KEY` for a personal key. No meal/oil legs, so the old India crush margin is retired — the cross-market line is India bean vs CBOT bean premium (USD/MT). **The level is validated and the premium is real** (#206): 2026-08-11 MP median ₹6,725/quintal = $705/MT against CBOT $425/MT, +66%, confirmed by SOPA's own Indore oil (₹1,400/10kg) and meal (₹57,000–57,500/MT) quotes — which are *not* Agmarknet-derived and imply a ~+4.7% gross crush on that bean — plus two mandi aggregators. India bans GM soybean imports behind a tariff wall, so nothing arbitrages the domestic bean toward CBOT and a large premium is its normal state. Variety/grade mixing was measured and is immaterial (≤0.6%), so no filter is applied. Three traps pinned by tests: **`modal_price` is ₹/quintal** and a switch to ₹/kg or ₹/MT would parse cleanly while restating the level 10–100×, so a day's median outside `MANDI_MODAL_MIN/MAX_INR_QUINTAL` hard-fails; **offset paging must be sorted** (`MANDI_SORT_FIELD`) — unsorted, the walk served 20 of 115 MP rows twice and never served 20 others, inflating the mandi count; and the shared sample key's throttle answers **HTTP 200 with `{"error": "Rate limit exceeded"}`**, which is retried as transport rather than read as a schema break. **No High/Low is stored** — Agmarknet's per-mandi min/max are lot extremes (₹800/qtl against a ₹6,750 modal) and produced a ₹1,010/MT "low" on a ₹67,250/MT day; a cross-sectional median has no range, so Open/High/Low are NaN. **A partial walk is a hard failure, not a warning** (#212): because the daily number is a *median across the reporting mandis*, a truncated walk does not yield a missing number — it yields a plausible wrong one, computed over whichever pages survived, with nothing in its shape marking it partial. Both silent truncations now raise — the `MANDI_MAX_PAGES` cap, and a page answering zero records before `total` is reached. Likewise, transport exhaustion on **any** state grades the whole layer failed, because `india_domestic` has no `LAYER_MIN_KEYS` floor and nothing downstream would otherwise notice half of it going dark. The rows that did arrive are still saved (`FetchResult.partial`) — the resource serves the current day only, so a discarded day is a permanent hole — but `last_success` does not advance. An *empty* state stays an ordinary success: MP and MH keep different local holiday calendars, and empty means asked-and-answered whereas failed means never-answered. Layer 16's `LAYER_MAX_DATA_AGE_DAYS` budget is **7 days** (set by M19 #222), with one risk accepted knowingly: India's closure calendar is the longest of any daily leg here, and the Diwali stretch (Dhanteras→Bhai Dooj, ~5 days, a Sunday at each end) could exceed 7 and demote the India page over an ordinary festival week. It is kept because a full blackout needs *every* one of ~115 reporting mandis per state shut, not just the Indore hub. Nothing settles it from our own data yet — history starts 2026-08-10 and the resource serves only the current day — so revisit at the first Diwali (Oct 2026) with a real gap in hand.
17. **Brazil domestic soy spot** — `fetchers/noticias_agricolas.py` (CEPEA/ESALQ Paraná + ESALQ/B3 Paranaguá indicators republished server-rendered by Notícias Agrícolas — BRL/MT, no API key). Re-enabled 2026-07-30; cepea.org.br itself is still Cloudflare-Turnstile-walled and `fetchers/cepea.py` stays on disk only as a fallback. Historical gap backfill: `scripts/backfill_cepea_gap.py` (one session per `/YYYY-MM-DD` archive page).
18. **South Africa domestic soy** — `fetchers/safex.py` (JSE SAFEX via Grain SA — ZAR/MT, no API key). The stored number is the **last traded price, not settlement/MTM**: the free Grain SA table has no settlement column, and the JSE's own MTM file is behind its Client Portal under terms barring commercial use (#157). The contract shown is the **most-liquid** one that session (largest Volume, ties broken by nearest expiry) — nearest-expiry alone rode the contract into expiry as liquidity rolled away, and could not exclude zero-volume rows whose carried-forward prices the page re-stamps with the current date. The page **stale-serves** on non-trading days rather than emptying, which is why Layer 18 carries a `LAYER_MAX_DATA_AGE_DAYS` budget: without it a frozen upstream would return the same rows forever and stay green.
19. **AgRural Paranaguá FOB** — `fetchers/agrural.py` (Brazil port-side soy FOB scraper — BRL/MT, no API key)
20. **US Gulf export bids** — `fetchers/gulf_bids.py` (AMS report 3147 "Louisiana and Texas Export Bids" daily PDF — CIF NOLA-barge soybean/corn/wheat bids, basis in cents/bu over the named CBOT contract; no API key). A ranged basis quote can span **two different** contracts (`95.00Q to 100.00X` — Aug low leg, Nov high leg; ~3% of cells over 2021-06→2026-08), so both codes are stored (`futures_month`, `futures_month_high`) and the briefing labels each leg with its own month; storing only the low leg priced the high end of the spread against the wrong futures (#196).
21. **Argentina official FOB** — `fetchers/magyp_fob.py` (MAGyP "Precios FOB Oficiales" JSON web service — daily official minimum FOB export values in USD/MT for soybean beans/oil/meal **plus crude sunflower oil**, bulk NCM positions, with shipment-window forward curve; no API key. Feeds the cross-origin FOB board with Layers 19/20, and is the **only daily sunflower benchmark the stack can have** — no exchange anywhere lists a sunflower contract on a free feed, so the four-oil veg-oil board's fourth oil is an administered FOB, never a traded price (#147/#162). Sunflower enters on the **oil leg only**: the seed (1206.00.90) and meal (2306.30) positions are administered step-functions that render as levels, not lines. **The service publishes no description field — a position is a bare NCM code — so every mapping is cross-checked numerically against the labelled datos.gob.ar mirror (sspm dataset 358), never read off the nomenclature**, after 3 of 4 inferred meal codes turned out wrong in #147. The check is *membership in the day's shipment-window set*, not equality with the nearest window: the mirror tracks a campaign window, so on 2024-11-04 its bean price 390 was the 2025-03/2025-10 window while the nearest printed 416 — the naive test scores a correct code 15/52. All four positions matched on 312 of 312 product-days over 2024-11-01→2025-01-21, which also **discharged the long-open `23040010100B` meal risk**, so the Argentina crush is no longer `provisional`. Crude sunflower oil publishes under **three SIM lines carrying one price** (`15121110310E`/`911P`/`919G`, identical on all 66 circulars sampled 2026-05→08); only `310E` is stored, because `position` is part of both the primary key and the git-committed history CSV. Two guards keep that honest: `_check_every_product_present` hard-fails when a published circular drops a product — the case that would otherwise go **silently dark**, since the surviving positions keep the layer green — and `_parse_posts` hard-fails if the unmapped siblings ever quote a different price.)
23. **SAGIS South Africa weekly producer deliveries** — `fetchers/sagis.py` (tonnage delivered by producers into commercial storage each week, soybeans + sunflower seed, MT; no API key). South Africa's first **physical flow** series and the reason the SA page is a flow page rather than a price page: SAFEX (Layer 18) is capped at a licence-limited last-traded print, while SAGIS grants reproduction with acknowledgement (`SAGIS_ATTRIBUTION` must be rendered wherever these numbers appear). Takes the machine-readable `DT-SWP-<Commodity>_<season>_<week>.xlsx` export, not the `ProdProgressive-*` presentation workbook — flat 9-column table, no header sniffing, and **every season in one file** (2018–2026 as of week 22/2026, 440 rows/commodity). The URL is week-stamped and must be re-resolved from the listing page each run; a hardcoded deep link serves a frozen week at HTTP 200, the same trap as the World Bank CMO GUID in Layer 8. `SeasonYear` is the *start* year of a March–February season, so 2026 = 2026/27; comparisons are made at the same **week number**, which is SAGIS's own convention (a season's week 1 can start in February or March). Components are stored (`first_published`, `adjustments`, `week_total`); the progressive total is derived at read time. SAGIS revises past weeks for months — including in closed seasons — so the `(commodity, season_year, week_number)` upsert legitimately rewrites stored rows every run. Layer 22 is reserved for the in-flight Nigeria AFEX leg (PR #166).
24. **SAGIS South Africa monthly soybean supply & demand** — `fetchers/sagis.py` (`fetch_sagis_supply_demand`; the SA balance sheet in MT — opening stock, deliveries, imports, tonnes processed, whole-bean exports split border-posts vs harbours, the soybean equivalent of product exports, and closing stock split storers-vs-processors; no API key). Filed as SA2 (#203) for SAGIS's *weekly* imports/exports and its 8-week forward intentions — **those two products are published for maize and wheat only** (verified live 2026-08-12), so no soybean trade series exists at weekly cadence and the monthly SMD is the only one there is. `processed_oil_oilcake` is South Africa's **crush volume**; M7's finding that SA has no honest crush *margin* still holds (SAFEX is seed-only, the JSE meal/oil contracts are cash-settled CBOT), and this line must never be rendered as one. Takes the season-progressive workbook `Sojabone<season><season+1>_<pubdate>[_F].xlsx` — all twelve months of one March–February season in one sheet — rather than the per-month announcement file, which holds two months and would cost one request per month of history. **Both the URL and the season set rotate**: the current season is re-published monthly under a new filename and only three seasons are ever listed, so links are resolved from the landing page each run and the table round-trips through `data/history/`. Unreported months are printed as a hard `0`, not left blank, so the frame is cut at the workbook's own `SMD-MMYYYY` vintage tag — without that cut every season would open with a fabricated collapse in crush, trade and stocks. The sheet repeats line-item labels across sections with *different* numbers ((c) commercial-use crush 204,103 t vs (i) local-market 190,651 t for Jun 2026; (h) restates "Opening stock"/"Closing stock" for excluded transit tonnage), so rows are matched by section letter *and* label prefix, and every stored month is checked against the report's own balance, (a)+(b)−(c)−(d)−(e) = (f) — a row read from the wrong section parses fine and is simply wrong, which only that arithmetic can catch. Section totals are not stored; each is the sum of components that are. Keyed `(commodity, season_year, month_number)` with month_number 1 = March, and revised for a year or more after first publication, so the upsert legitimately rewrites stored rows.

22. **EU rapeseed (European Commission)** — `fetchers/ec_oilseeds.py` (EC Oilseeds Market Observatory weekly world FOB xlsx — `Rapeseed - EU Moselle`, ~400 weekly rows back to 2018-12-26, CC BY 4.0, no API key). The Europe page's only price leg. **Euronext MATIF (ECO) settlements are deliberately not ingested**: delayed data is free for internal use but redistribution costs EUR 167.55/month and this project publishes, so the futures curve is licence-blocked (#148). This is a **weekly physical FOB assessment**, not a futures price — no term structure, no daily print — and every stored row carries `cadence` and `quote_kind` so a consumer cannot collapse it into a daily board series. Same rotating-GUID trap as Layer 8: the CIRCABC deep link is resolved from the observatory landing page each run by **link text** ("World oilseed prices"), never by filename — the published filename carries an upstream typo (`oliseeds-world-prices.xlsx`). The workbook's **EUR/t block is derived, not independently quoted** (EUR = USD ÷ the row's ECB rate for 391 of 393 rows; five rows read `n.q.`, and the two newest convert at a two-week-stale rate, ~1.3% off), so **USD/t is authoritative** and EUR is stored exactly as published — NULL where `n.q.`, never recomputed. Self-healing: full history re-downloads every run, so no `data/history/` round-trip.

25. **CEC South Africa official crop estimates** — `fetchers/cec.py` (the Crop Estimates Committee's monthly area/production estimate for soybeans + sunflower seed, ha and MT; no API key). Structurally the SA analogue of Layer 15 (CONAB), but **not an independent second opinion**: USDA's PSD carries the CEC's final figure verbatim at PSD year = CEC season − 1 (2,770,000 / 1,848,000 / 2,800,000 t for the 2023 / 2024 / 2025 crops are exact ties), so no "CEC vs USDA" divergence line exists anywhere — what renders is the in-season revision path plus a **lag** read on PSD (#204). Fetched from the SAGIS mirror because the issuer's own domain, `dalrrd.gov.za`, no longer resolves and its replacement (nda.gov.za) publishes no parseable listing; a mirror that stops republishing is caught by the `LAYER_MAX_DATA_AGE_DAYS` budget, since every release carries its own date. PDF-only, four filename conventions, and `.doc` before 2025 — so `CEC_HISTORY_START` bounds the window at 2025-01-01, inside which **every** PDF must parse. The window is ~23 files / ~18 MB and re-downloads in seconds, so the layer re-reads it whole each run and self-heals on an empty CI database; nothing here needs `data/history/`. It is a **revision series** keyed `(commodity, season_year, release_date)`, never overwritten to a current estimate: the season is forecast nine times, finalised in November, and then *re-finalised* by the CELC in February against SAGIS's actual deliveries (2,771,225 → 2,800,000 t for 2025) — that last figure is the one later reports quote as "final crop". `season_year` is the CEC's calendar-year convention (2026 = the 2025/26 season). Area-only releases (January's preliminary area, October's intentions to plant) store `production_t` NULL, never 0. Two header rows in the source are known-wrong and must not be anchored on — `CEC_2026-02-26.pdf` labels its tons column "Ha", `CEC-27-Feb-2025.pdf` declares a change formula it doesn't use — so columns are matched by **label** and the parse is validated against the source's own arithmetic (the printed Change % must be reproducible from the row) plus an implied-yield band. Yield is derived at read time; the release date is read from the document body, since two filename conventions carry no day and the names that do carry the *upload* date.

26. **USDA AMS ocean freight (Grain Transportation Report)** — `fetchers/gtr.py` (`fetch_gtr_ocean_freight`; monthly bulk-grain vessel rates US Gulf → Japan and PNW → Japan, USD/MT, back to Jan 1996; no API key). The stack's first **freight** leg: it could price a cargo at both ends and had no number for moving it. The Gulf-minus-PNW spread is the decision — which US coast is the cheaper way out — and it is **derived at read time**, never stored, because the workbook publishes its own spread column and that column is used as the *parse check* instead (see below). Two things it is not. It is **not a USDA measurement**: USDA republishes an assessment by O'Neil Commodity Consulting, which is why `attribution` is stamped on every row rather than resolved at display time — rendered beside the AMS Gulf bids (Layer 20) an unattributed row credits both to the same author. And it is **not a quote for any cargo here**: Japan is a benchmark route, so this must never be substituted for the route-specific ocean freight that `analysis/origins/` requires (a missing one there is a *hard* blocker, and filling it with a benchmark would convert a visible gap into an invisible wrong answer). Four traps are pinned by test. The period column carries **seven layouts** — six string formats (`96-Jan`, `July_99`, `Jan. 02`, `May  02` with a doubled space, `June 02`, `Aug '17`) plus real datetimes — and its month token has **three spellings**, of which `Sept` (every September 2002-2016) is accepted by neither `%b` nor `%B`. None of that raises: a parser handling a subset returns a *shorter series* and logs success, and the first cut of this module stored 128 of 367 months that way. Months are therefore matched by **unambiguous prefix** against the full month names rather than by strptime. The file also contains a plain data-entry error — seven 2019 months stored as **1919**, between `May '19` and 2020-01 — where the sequence proves the intent but rewriting a published year would be inventing data and storing 1919 would put a century-old rate at the front of every chart; the row is dropped and named, so 360 of 367 months survive and the gap is documented rather than silent. The sheet **ends in a summary block that parses as data**: year-on-year *ratios* (0.33, 0.21) printed under the rate columns, which stored are a freight market that collapsed by two orders of magnitude. And because the sheet has no single header row, columns are addressed by index and therefore **checked against the sheet's own arithmetic on every row** — the published spread must equal gulf − pnw, since a shifted column is a wrong number rather than a missing one. A row with no published spread is still accepted: a missing check is not a failed check. And because the check detects *our* drift rather than auditing the publisher, the verdict rides on the failure **rate** (`GTR_MAX_ARITHMETIC_FAILURE_RATE`, 5%): a handful of contradictory rows in thirty years drops only those rows, while a rate above the threshold means the mapping moved and the whole workbook is discarded — under a shifted mapping the rows that happen to reconcile are no more trustworthy than the ones that do not. Measured live 2026-08-19: 0 of 367 freight months and 8 of 1,649 vessel weeks fail, the latter scattered across 2018-2026 and off by 1-12 vessels. **One route parsing alone discards the whole workbook** — both come out of one download of one sheet, so a lone survivor means the mapping moved and is exactly as suspect as the casualty. The workbook carries the full series on every fetch, so the layer self-heals on an empty CI database and needs no `data/history/` round-trip; the URL is a **fixed filename**, which is the World Bank / CIRCABC trap from the other side — nothing rotates, so a file that stops being refreshed answers 200 forever and only `LAYER_MAX_DATA_AGE_DAYS` (75 days) catches it.
   - **26b. Grain vessel lineups** — `fetch_gtr_vessel_activity` (weekly counts of vessels in port, loaded in the last 7 days, and due in the next 10, for the US Gulf and the Pacific Northwest, back to 1995). A separate run unit from 26 on the 2b/15b convention, because a weekly cadence and a monthly one cannot share one recency budget (21 days vs 75). Counts of **vessels**, not tonnes and not a lineup by ship — no names, no cargoes, no berths. `in_port` is **stored rather than derived** even though it equals loading + waiting-to-load wherever all three print: the 1990s rows publish only the total, so deriving it would delete a decade — and where all three *are* present the identity is enforced instead, which is what pins the column mapping (the same role the spread plays in 26). Verified against the workbook's own printed presentation sheet, a different sheet from the one parsed: week ending 2026-08-06 reads Gulf 22 in port / 33 loaded / 30 due and PNW 14 in port on both. A region whose columns are entirely blank is a series that did not exist that week — Vancouver's whole tail, which is why Vancouver is deliberately **not** an expected key — and is never read as a zero; an individual blank count is preserved as NULL through the cleaner for the same reason. A **truncated download served as HTTP 200** is a live failure mode here, observed against these files on 2026-08-19 (634,667 bytes arrived, the zip central directory did not, and the result still passed `file(1)` as a workbook): openpyxl raises `zipfile.BadZipFile`, which is **not** an OSError subclass and escapes a naive handler as a mid-run crash.

### Pipeline Layer

- `pipeline/clean.py` — Normalizes raw data (forward-fill gaps, datetime indices, drop NaN rows). Runs sanity checks (warns on >10% daily moves, zero/negative volume). Contains `_check_nan_gaps()` helper used by `clean_ohlcv()` and `clean_dce_futures()`. Also has `clean_india_domestic()`, `clean_brazil_spot()`, `clean_safex()`, `clean_sagis_deliveries()`.
- `pipeline/schema.py` — All 27 `CREATE TABLE IF NOT EXISTS` SQL definitions. No functions — just the table blueprints used by `store.py`.
- `pipeline/store.py` — All `save_*()` write functions. INSERT OR REPLACE upserts, transaction safety, freshness tracking. Uses `get_connection()` from `connection.py`.
- `pipeline/query.py` — All `read_*()` query functions. Returns DataFrames; used by the analysis layer and dashboard.
- `pipeline/connection.py` — Database connection abstraction. Returns Turso cloud connection when `TURSO_DATABASE_URL` is set, local SQLite otherwise.
- `pipeline/units.py` — Metric ton conversion utilities. Converts native exchange units (cents/bu, cents/lb, $/short ton) to USD/MT at the display layer.

### Analysis Layer

- `analysis/technical.py` — SMA (20/50/200), RSI (Wilder smoothing), MACD (12/26/9), Bollinger Bands, historical volatility, price changes
- `analysis/signals.py` — 20/50 and 50/200 MA crossovers, volume spikes, RSI extremes/divergence, MACD crossovers, Bollinger squeeze
- `analysis/spreads.py` — Soybean crush spread (Oil*11 + Meal*2.2 - Beans)
- `analysis/correlations.py` — Cross-commodity matrix, commodity-vs-currency, rolling correlation
- `analysis/seasonal.py` — Monthly seasonal averages, current vs historical norm
- `analysis/forward_curve.py` — Forward curve analysis: contango/backwardation, curve slope, calendar spreads
- `analysis/loaders.py` — Shared, cached price and currency loaders. Used by both `analysis/briefing/` and `analysis/soy_analytics.py` so the two consumers don't drift. `clear_loader_cache()` resets between pipeline runs.
- `analysis/stocks_to_use.py` — Stocks-to-use ratios from PSD; tight-supply alerts.
- `analysis/zscore.py` — Shared z-score helper used by COT and weather sections.
- `analysis/briefing/` — Daily briefing package. Each section of the briefing lives in its own module under `analysis/briefing/sections/` (prices, crush, economic, usda, crop_progress, wasde, export_sales, inspections, gulf_basis, transport, dce, forward_curve, eia, conab, currencies, cot, weather, psd, worldbank, emerging_markets, basis, stocks_to_use, correlations, seasonal, market_drivers, signals, freshness). `analysis/briefing/orchestrator.py` joins them; `analysis/briefing/types.py` defines the typed `BriefingData` returned by `generate_briefing_data()`. `generate_briefing()` is a thin wrapper that returns `BriefingData.text`.
- `analysis/briefing/snapshot.py` — Distills `BriefingData` into structured `snapshot_json` for the briefings archive (schema v2, marked by a top-level `schema_version`; rows without it are v1). Captures every quantitative section output: technicals, crush, Brazil basis, FRED + yield curve, USDA YoY, crop progress, WASDE revisions, stocks-to-use, export sales (incl. China share), inspections, transport (both ocean-freight legs and both vessel regions, each stamped with its own as-of because the cadences differ), DCE, forward curve (incl. slope), EIA, CONAB legs (no derived gap — units unreconciled), currencies (session-based `chg_5d_pct`/`chg_21d_pct`), COT + 3y z-scores, weather + 90d z-scores, PSD highlights, World Bank, emerging markets (verbatim), correlations, seasonal, and data health. Stores raw numbers and components, never display labels; every block degrades to None/{} on failure.
- `analysis/soy_analytics.py` — 9 analyst functions for the soy dashboard: command_center, supply, demand, technicals, relative_value, risk, seasonal, forward_curve, emerging_markets. Pulls price/currency dicts from `analysis/loaders.py`.
- `analysis/health.py` — Per-commodity data health checks (stale data, flat prices, missing commodities)

### Storage

- Database: `data/storage/mirror_market.db` (SQLite, gitignored)
- Tables: `prices`, `economic`, `usda`, `crop_progress`, `cot`, `weather`, `psd`, `currencies`, `worldbank_prices`, `dce_futures`, `export_sales`, `forward_curve`, `wasde`, `inspections`, `inspection_port_flows`, `inspection_destinations`, `gulf_bids`, `argentina_fob`, `eia_energy`, `brazil_estimates`, `data_freshness`, `commodity_freshness`, `india_domestic_prices`, `brazil_spot_prices`, `safex_prices`, `sagis_deliveries`, `sagis_supply_demand`, `cec_estimates`, `ec_oilseed_prices`, `ocean_freight_rates`, `port_vessel_activity`, `briefings`
- `forward_curve` keys on `(commodity, contract_month, fetched_date)` — one full curve per run accumulates term-structure history; `read_forward_curve()` returns only each commodity's latest snapshot.
- V1 pipeline config lives in `config.py` (tickers, API URLs, region coordinates, thresholds).
  V2 source/dataset cadence, identity, freshness, validation, retention, rights,
  and criticality policy lives in the authoritative `trust.registry` contract registry.

### Git-based history persistence (`pipeline/history.py`)

CI runs on an ephemeral runner with an empty DB each day. Most layers self-heal by re-downloading full history, but snapshot-only sources don't: AgRural (1 row/day — the Brazil basis source), SAFEX, forward curve, CONAB survey revisions, inspections (>3 weeks) incl. port/destination breakdowns, Gulf bids, Argentina FOB (MAGyP serves history but re-fetch depth is unproven), CEPEA (>~10 sessions), WASDE (>12 months), India mandi (current-day snapshot), **export sales** (ESR is fetched for the current marketing year only, so the outgoing year vanishes at each MY rollover), **briefings** (generated from that run's DB; a past day's `text`/`snapshot_json` is reconstructible from no source at all), **SAGIS monthly supply & demand** (only the current season plus the two most recent finals are ever listed; older seasons exist only as per-month announcement files this layer does not fetch, so a season scrolling off the page is unrecoverable), and **SAGIS deliveries** (the DT export *does* serve history, but only a fixed 9-season window — nothing upstream promises it grows, so a season that rolls off is unrecoverable from an ephemeral CI DB). These tables round-trip through CSVs in `data/history/` (committed to git): `main.py` calls `import_history()` after `init_database()` (INSERT OR IGNORE — DB rows win over CSVs) and `export_history()` after the layers (atomic per-table writes, PK-sorted for stable diffs). The deploy workflow commits `data/history/` back to `main` with `[skip ci]`. A failed import aborts the run so a bad seed can never be exported over good history. Cloud DB (Turso/Supabase) was explicitly rejected for this — do not reintroduce it as a CI requirement.

`export_history()` refuses three kinds of regression rather than committing them: an **empty** table (fetch layer failed), a **shrinking** row count (DB behind the committed CSV — e.g. a local run that skipped `import_history()`), and a **dropped column** (`CREATE TABLE IF NOT EXISTS` never adds columns, so a DB predating a schema change exports narrower rows at an unchanged row count). Each logs an error and leaves the CSV untouched. `MIRROR_HISTORY_ALLOW_SHRINK=1` overrides the latter two when a prune or column removal is genuinely intended.

### Briefing Sections (in order)

1. Data Freshness Warnings
2. Prices (10 commodities with MA, RSI, MACD, volatility)
3. Crush Spread
4. Brazil Basis (Paranaguá FOB vs CBOT, USD/MT — Layer 19 × Layer 1)
4b. US Gulf Basis (CIF NOLA barge — AMS export bids, Layer 20)
4c. Cross-Origin FOB Board (US Gulf CIF vs Brazil Paranaguá FOB vs Argentina up-river FOB, USD/MT — Layers 19 × 20 × 21)
4d. Transport (Gulf and PNW ocean freight to Japan with the Gulf-over-PNW spread, plus Gulf/PNW vessel lineups — Layers 26 × 26b)
5. Economic Context (FRED — dollar index, CPI, rates, ethanol PPI)
6. USDA Fundamentals (YoY production/yield)
7. Crop Conditions (weekly USDA % good/excellent, progress)
8. Yield Curve (2Y/10Y spread with recession signal)
9. WASDE Estimates (monthly USDA supply/demand forecasts with MoM revisions)
10. Stocks-to-Use (US balance-sheet tightness from PSD; tight-supply alerts)
11. Export Sales (weekly USDA FAS demand data, top buyers)
12. Export Inspections (actual shipments vs committed sales)
13. DCE Chinese Futures (vs CBOT comparison)
14. Forward Curve (contango/backwardation per commodity)
15. Biofuel & Energy (EIA — ethanol, biodiesel production, diesel prices)
16. Brazil Crop Estimates (CONAB vs USDA comparison)
17. Currencies (10 pairs with trade impact)
18. COT Positioning (10 commodities)
19. Weather Alerts (19 regions)
20. Global Supply — PSD (28 countries)
21. World Bank Prices
22. Emerging Markets (South Africa SAFEX price + SAGIS delivery pace + SAGIS monthly S&D — crush volume, trade, stocks + CEC official crop estimate with its in-season revision + Brazil CEPEA/CONAB farmgate + India mandi bean vs CBOT + Nigeria deep dive)
23. Correlations (cross-commodity + commodity-vs-currency)
24. Seasonal Analysis
25. Market Drivers (BRL + exports, COT + RSI crowding, weather + price premium, dollar impact, corn/soy acreage competition, livestock demand, export sales pace, forward curve structure, palm oil vs soy oil, CZCE rapeseed oil vs soy oil, biofuel pull, CONAB vs USDA divergence)
26. Signals (sorted by severity)

## Key Patterns

- All fetchers return `dict[str, pd.DataFrame]` (keyed by commodity/region name)
- All cleaners return copies — originals are never mutated
- Database uses `INSERT OR REPLACE` so the pipeline is safe to re-run
- Analysis functions expect DataFrames with a `Close` column and DatetimeIndex
- Logging throughout — configured once in `config.setup_logging()`
- Configurable thresholds in `config.py` (RSI, volume spike, weather, freshness)
- Signals have severity levels: `alert` > `warning` > `info`

## Static HTML Site

A small fixed set of static pages deployed to GitHub Pages: the headline page, the players map, and one page per market. **The market is a parameter, never a code path** — `if market == "india"` in a builder is the drift the whole contract exists to prevent; per-market variation belongs in the registry descriptor.

Key files:
- `config.py` → `MARKETS` — the market registry, keyed by slug. **Pointers, never values**: table/column/key *names*, never a price, a date or a tier. Key order is nav order is the headline ledger's row order (`cbot · dalian · brazil · argentina · india · europe · south_africa · nigeria` — role in the trade), declared once so the two consumers cannot disagree.
- `config.py` → `LEDGER_LEGS` / `LEDGERS` — the propagation ledger's leg catalog and each page's counterpart set (M12 #161). A **second id space**: `LEDGER_LEGS` is keyed by leg id (`us_gulf:cif`), `MARKETS` by market slug, and a leg carries only what its owning market's descriptor cannot say — which key it is, what to call it, and how its print is proved. Everything else (table, unit, FX pair, home currency) is read from the owner, never restated.
- `app/markets.py` — the typed view of that registry (`load_markets()`), plus `compute_tiers()`. It lives outside `config.py` because it reads the DB and `config` is imported *by* `pipeline`. **`quote_kind` is required on every leg whose unit is a price** (`PRICE_UNITS`), rejected at load; `tonnes` and `observation` legs have none and must not invent one. It was optional while block 01 was the only reader — one number of one animal, stamped in the block header — so the four `basis` descriptors shipped without one. Two later consumers put those same descriptors beside a board price (the ledger's shared USD/MT column, then block 04's Local-vs-CBOT pair) and each had to rediscover the omission. The general trap: **a descriptor reused by a second consumer may be missing labels the first never needed**, so the label belongs to the descriptor and the check sits at load rather than in each renderer. A header chip that labels one animal also cannot label two — block 04 and the ledger table both state the kind per row.
- `app/blocks.py` — the nine-block set and the `{state, reason, data}` envelope every block builder returns. `Block.__post_init__` **raises** when a non-`ok` state carries no reason, so "every empty state names its reason" is enforced by the type rather than by nine builders remembering.
- `app/block_builders.py` — the nine builders that fill that envelope, all **generic SQL over the registry descriptor**: `Source` names the table, date column, key column, value column and unit, so a tenth market is a `config.MARKETS` entry and no code. `Source.to_usd_mt` is the site's **only** conversion site — `native_exchange` (cents/bu, cents/lb, $/short ton), `usd_per_bushel` (AMS 3147 prints flat CIF bids in $/bu), `home_per_mt` (× the `<CCY>/USD` rate **of that row's own date**) or `usd_per_mt`. A `home_per_mt` leg with no FX rate renders its USD/MT as blank, never the local number relabelled. Crush and basis are struck on a session **all** legs printed — no cross-day arithmetic — and a home-currency crush margin is computed only where the legs share one per-MT currency (a board quoted in three different native units has no such number). Several rows for one key on one date (AMS barge locations, MAGyP shipment windows) are averaged and the count is rendered.
- `app/sections.py` — the same `{state, reason, data}` envelope for the five headline sections that used to assemble ~600 lines of HTML in f-strings inside `generate_html.py` (emerging markets, relative value, risk monitor, forward curves, seasonal). Also owns the **chart budget**: `clip()` cuts each series to the window the figure actually reads (`CHART_WINDOW_SESSIONS`), applied where the series meets the figure so the stats above the chart keep their full history.
- `app/templates/blocks/NN_<id>.html.j2` — one partial per block, numbered so file order on disk is block order on the page; `app/templates/sections/*.html.j2` the same for the headline. Markup lives here and nowhere else: an f-string builder cannot enforce "same block, same treatment, eight markets", which is why M8 made this a contract.
- `app/templates/_base.html.j2` — owns `<head>` entirely: fonts, the DESIGN.md palette, the masthead and both nav bars. Every page extends it, `players.html.j2` included, or it becomes the page that drifts.
- `app/templates/market_page.html.j2` / `market_brief.html.j2` / `market_stub.html.j2` — one real template per tier (a brief is not a page with more hatching), plus `tombstone.html.j2`.
- `scripts/generate_site.py` — the orchestrator: owns the page list and the failure-isolation policy.
- `scripts/generate_html.py` — the **headline page's** renderer, one entry in that list.
- `.github/workflows/deploy-dashboard.yml` — daily pipeline run + site generation + Pages deploy.

**Tier is computed from the DB every run, never hard-coded** (M1 #143): a daily price leg plus ≥3 of {ledger, crush, basis, weather} → `page`; less than that, or no daily leg with ≥2 → `brief`; otherwise `stub`. "Present" means *current within that layer's own `LAYER_MAX_DATA_AGE_DAYS` budget*, not "the descriptor names a table" — a market whose scraper died a fortnight ago demotes itself. The ledger is not probed separately: it is daily-only, so it is present exactly when a daily leg is. **The URL never changes with the tier** — `docs/markets/india.html` exists in all three tiers, because anything else means yesterday's link 404s when a scraper breaks.

**A demotion caused by our own outage must say so.** One data.gov.in rate limit costs India both its daily leg and, since the ledger is daily-only, the ledger with it — two blocks at once, enough to move the tier. The demotion is correct (we genuinely don't have the number), but age alone cannot tell a rate limit from a market where nobody publishes the series: both surfaced as `no rows`. `_ingest_status()` consults `data_freshness` — which records `status='failed'` with `last_success` held back — and appends "our `<layer>` ingest failed; last good run `<date>`" to the note. Without it the page states our outage in the same words it uses for a genuine market absence, which reads as a judgement about the market (#212).

**The propagation ledger (block 02, and headline section 03)** answers one question — who has repriced, and who has not printed. Shape from M3 #145: settlement-ordered rows, each dual-quoting USD/MT over the venue's own print, both moves with an `FX` tag when the currency did the work, and a state pill so silence can never read as flat. Counterpart sets from [M12 #161](https://github.com/philipbergman6-glitch/Mirror-Market/issues/161), which are **registry data** (`config.LEDGER_LEGS` + `config.LEDGERS`), not a code path.

Five things about it are load-bearing:

- **A row is a *leg*, not a market.** `us_gulf:cif` is the AMS CIF NOLA bid living on CBOT's `basis` descriptor and has no market key of its own, so the registry now carries **two id spaces**. Every leg id resolves to (market, sub-block, key) or `load_markets()` hard-fails — an unresolvable leg would render an empty row, which reads as "that market has not printed", the ledger's most important statement made by accident.
- **A row stamp is not a print.** Grain SA re-dates a carried SAFEX price with `Volume 0` (#157), so a leg may name a `trade_proof_column`: a row with `<= 0` there is not a print. NULL keeps the row — that is proof of nothing, and dropping it would invent an outage. Assessments (CEPEA, AgRural, AMS, MAGyP, mandi) publish no volume by nature and are not asked for one.
- **Being behind is judged per leg.** `FRESHNESS_WARNING_DAYS = 7` lets a daily leg go six days silent (M4 §3.4 trap 5), so each leg carries its own `expected_gap_days` (default 4 — a weekend plus a holiday). `dark` still uses the layer's own `LAYER_MAX_DATA_AGE_DAYS`, the number `main.py` grades on, so the site and the pipeline cannot disagree about whether a source is alive.
- **A spread is one session's number.** Counterpart rows carry a spread against the pinned own leg, struck on the most recent session *both* printed and stamped with that date; where there is no common session the cell is blank with the reason. Two dates subtracted would manufacture an arbitrage out of a calendar gap.
- **A dual quote needs two observations.** Only a `home_per_mt` leg has a second currency; a `usd_per_mt` leg (Argentina) prints one number and a dash, never the same figure twice. This is the EC lesson from the other side (#163): that workbook's EUR column is its USD one divided by an ECB rate, so a dual quote there would be our own arithmetic dressed as the venue's second opinion.

Sets are **fixed, never seasonal**; every ledger is **one good** (the soybean — a meal ledger would be a second block); row counts are **4 or 5, never padded** (Dalian and Argentina have three genuine counterparts and the fifth candidates are filler). CBOT is not pinned everywhere — dropped from India, where the GM import ban makes the +66% uncloseable, and demoted to a labelled reference row, last, on South Africa. India's ledger is **two domestic state medians and no foreign leg at all**. Europe and Nigeria get **no ledger block** — `absent` with a reason, a legal page configuration rather than a degraded one. The headline's eight rows are **markets**, with the market cell as the link and **no spread column** (there is no pinned leg to spread against); Europe carries no value (`out of cadence`, not `dark` — its weekly leg is not an outage) and Nigeria is `dark`.

**A basis must say whether trade connects its two legs** (M19 #222). Every `basis` descriptor declares `arbitrage`: `open` (cargoes move, so freight/quality/duty bound the spread — Gulf, Paranaguá, Argentina FOB, DCE import parity) or `policy_blocked`, which additionally **requires a `caveat`** and fails the build without one. India is the only `policy_blocked` leg: its mandi bean prints ~+66% over CBOT because GM imports are banned behind a tariff wall, it reached ~2× in 2021, and nothing closes it — rendered with the same treatment as Paranaguá FOB it would invite a trade that cannot be taken. That basis line is also what makes India a `page` rather than a `brief`: mandi is bean-only, so ledger + basis + weather is the whole of its three.

**Failure isolation, three levels.** A block that raises renders as an empty state with reason `generation error` — the same *shape* as a missing source, a deliberately different *reason*. A page that fails becomes a **dated tombstone inside the private candidate**, never a silently retained old file. The headline failing fails generation outright. The promotion contract rejects every tombstoned candidate before Pages upload, so the last trustworthy public edition remains available while the failure is loud.

```bash
# Generate the whole site locally
python scripts/generate_site.py
# One page, for the dev loop
# (headline | players | origins | workstation | opportunities | <market slug>)
python scripts/generate_site.py --only cbot
# Output: docs/index.html, docs/players.html, docs/workstation.html,
#         docs/opportunities.html, docs/markets/<slug>.html
#         plus data/workspace/opportunities.html — the private desk edition,
#         gitignored and never in the promotion contract
```

## Landed-cost onboarding (`analysis/origins/`, Phase 6)

The origin page is fail-closed by design: with nothing entered, every landed
total blocks and says so. That is correct, and on its own it is a wall rather
than a workflow — the next question is always "so what do I enter, and who
enters it". Three modules answer it, and none of them relaxes the blocking rule.

- `validation.py` — the faults **one entry cannot see about itself**. An
  ambiguous pair (same component, same scope, overlapping windows *and*
  overlapping lifetimes) is two answers to one question; a freight with no
  `origin` prices all three legs off one indication; `us-gulf` for `us_gulf`
  matches no route and reads on the page as "never entered". Those are
  `Severity.ERROR` and `load_assumptions()` **raises** on them, because at
  lookup time the complaint surfaces mid-page-build for whichever route asked
  first. Expiry is `Severity.WARNING` and is reported, never raised: the lapsed
  record is the audit trail, and deleting it to quiet a loader would destroy it.
  A renewal chain (old entry's `expires_on` shortened to before the new one's
  `entered_at`) is explicitly legal; an overlapping renewal is not.
- `readiness.py` — **database-free** route onboarding. What a route requires is
  the incoterm bridge plus `config.LANDED_STACK`, both derived, so US Gulf asks
  for elevation and an ex-works leg would ask for inland haulage with no edit
  here. Status per input (`satisfied` / `expiring` / `expired` / `missing`) is
  resolved through the same `AssumptionSet.lookup` the calculation uses, so the
  checklist and the page cannot disagree. Every command carries a `<VALUE>`
  placeholder — a suggested default is a fabricated default with an extra step.
  `expiry_review` answers "what lapses, who owns it, which routes go dark",
  and resolves the last part by re-running readiness with the entry removed
  rather than by matching scope: an entry shadowed by a more specific one takes
  nothing down with it.
- `scenarios.py` — `input_flip_moves` generalises the freight break-even to
  **every** input. Solved, not searched: each rung is linear in its own value,
  so `marginal_landed_per_unit` differentiates the row's own waterfall (a flat
  dollar compounds through the ad-valorem rungs after it; a duty point is worth
  the CIF base it is charged on; a financing point recovers its carry period as
  `amount / rate`, and reports `None` at a zero rate rather than inventing one).
  Rows sort by the move **as a percentage of the input's own value**, because a
  dollar of freight and a point of duty are not comparable as written. An input
  **both** origins share moves both totals together and mostly cannot flip
  anything — said in words, never as a large number that reads as "possible".

Surfaces: page sections **02 Route readiness** and **09 Renewals due**, plus the
flip table in **05 Sensitivity**; `scripts/enter_assumption.py --onboarding`,
`--review` and `--check` (exit 1 on file faults, 0 on world faults). What must
be entered per route is documented in `data/reference/assumptions/ONBOARDING.md`.

The shipped directory still contains **no invented number** — only the two
China policy rates. The success path is rendered in tests from
`tests/fixtures/assumptions_complete/`, reached through `MIRROR_ASSUMPTIONS_DIR`
and never set in CI. One rendering trap is pinned by test: the site renders with
`autoescape=False`, so `<VALUE>` must be escaped explicitly or a browser eats
the placeholder as a tag. A second is pinned by name: a template key called
`clear` resolves to `dict.clear` — a truthy bound method — so the renewals
verdict is keyed `nothing_due`.

## Futures Workstation (`analysis/futures/`, Phase 3)

The hedging surface: `docs/workstation.html`, built by `app/workstation_page.py`
from the `analysis/futures/` package. It is the only part of this repo that
speaks in **named contracts** — everything else works on the continuous
front-month series `prices` holds, which no hedge can be placed on.

- `domain.py` — the vocabulary, standard library only: `CONTRACT_SPECS` (size,
  native unit, tick, published expiry rule, first notice rule) for nine
  products, `NamedContract`, `ContractQuote`, `ContinuousSeries`, the exchange
  holiday calendar and the business-day arithmetic. Its MT factors are pinned
  against `pipeline/units.py` by test — one table of densities, not two.
- `providers.py` — the **only** SQL-aware module here, and the substitution
  seam: `QuoteProvider` is a Protocol and `SqliteQuoteProvider` its one
  implementation. An authoritative feed replaces this class and nothing else.
- `curve.py` / `continuous.py` / `hedge.py` / `scenarios.py` / `ticket.py` /
  `positions.py` / `events.py` / `options.py` / `alerts.py` — term structure,
  stitched series, sizing, shocks, the proposal, the entered book, the release
  calendar, Black-76, and exposure alerts.

Five rules are load-bearing, each enforced by a type or a test rather than by
reviewer memory:

- **A named contract is not a continuous series.** Different types, neither
  accepted where the other is expected; `ContinuousSeries.is_hedgeable` is
  always `False`. Where the stored named-contract history is shorter than
  `MIN_SESSIONS`, a stitched series is **withheld** rather than padded with the
  provider's own front month — the silent substitution this phase exists to
  prevent. The provider series is still shown, labelled `provider_front_month`
  and carrying "the provider does not publish its roll dates".
- **Nothing here is a settlement.** Every quote is `PriceType.DELAYED_CLOSE`
  and `is_settlement_proven` is `False` on all of them. `PriceType.SETTLEMENT`
  exists for the day an authoritative provider is substituted and is never
  constructed today. The word "settlement" appears on the page only in denials.
- **Expiry is a published rule or it is absent.** ZS/ZM/ZL/ZC/ZW use the CBOT
  grain rule (business day before the 15th), LE the last business day, HE the
  10th business day. The two ICE softs were `NOT_ENCODED` until their rules
  were read off the **rulebook** rather than a summary page: Sugar No. 11 Rule
  11.06(a) — the last full trading day of the month preceding delivery, plus a
  January carve-out that no listed month reaches — and Cotton No. 2 Rule
  10.02(a), where Last Trading Day is the 10th business day before Last
  Delivery Day and Last Delivery Day is the 7th-last business day of the month.
  10 + 7 is exactly the "seventeen business days from end of spot month" the
  contract summary states, and it is the *pair* of statements that proves the
  counting convention (last business day = 1); one alone would have left an
  off-by-one nobody could see. Both are checked against dated examples in the
  tests — CTZ24 last trades 6 Dec 2024. The `NOT_ENCODED` machinery stays and
  stays tested (on a spec built for it, `tests.test_futures_hedge.
  unencoded_contract`) because the next product added may arrive without a
  rule: no days-to-expiry, no annualised carry, no roll window, no expiry
  alert, `hedge_month_candidates` returns nothing, the hedge reports
  `no_hedge_month`, and a leg named by hand reports `expiry_not_encoded`
  because silence there would read as safety.
- **A missing first notice day has two different reasons, and they are
  different states.** `first_notice_rule = None` means *this project* has not
  encoded the rule (Live Cattle, Lean Hogs). `NO_NOTICE_DAY` means the
  contract runs no notice-day mechanism at all — Sugar No. 11 is the one such
  product here, where Rule 11.06(b) attaches the delivery obligation to the
  close of the **last trading day itself**. That is stricter than an FND, not
  looser, so rendering it as "not encoded" would send a hedger looking for a
  date that does not exist and imply room they do not have. Cotton is the
  opposite edge: its FND (5 business days before the first business day of the
  delivery month) falls a fortnight *before* its last trade, which is the whole
  reason this package keys roll alerts on FND.
- **A curve is one session, re-checked at read time.** `forward_curve` is keyed
  `(commodity, contract_month, fetched_date)` and, until this phase, nothing
  deleted from it — so two runs on one day left the earlier run's legs standing
  (2026-08-11: seven Soybean legs, six stamped that session and `ZSN27.CBT`
  undated). Fixed at write time (`_replace_curve_snapshot`) *and* at read time
  (`_coherence`), because a leftover leg has a valid key and a plausible price.
  Legs off the newest observation date are dropped and named; the verdict rides
  on the analysis, and an incoherent curve suppresses the inversion reading in
  favour of a data alert. Neither fix cleans up rows already written, and the
  committed history holds some — `scripts/prune_curve_snapshots.py` is the
  operator tool for that, dry-run by default. It cannot run in a PR: the
  `history-guard` CI job fails any PR touching `data/history/`, so the cleanup
  is a deliberate action against a database followed by an ordinary export
  under `MIRROR_HISTORY_ALLOW_SHRINK=1`. The rule it applies is narrow on
  purpose — within one `(commodity, fetched_date)` group it keeps the legs on
  the newest *non-null* observation date, and a group where **every** leg is
  null is left entirely alone. Those are legacy rows predating the column, not
  duplicates, and a curve leg is unrecoverable once deleted.
- **First notice day, not last trade, is the hedger's date.** A merchant long
  past FND is exposed to delivery, so `roll_alerts` fires on FND and
  `fnd_inside_pricing_window` warns when the pricing period runs past it.

Volume is captured from yfinance and is `None` — never `0.0` — when absent.
**Per-contract open interest is always NULL**, because no source publishes it
per delivery month and a zero reads as "nothing open". A *whole-product* figure
does exist and is now shown beside the curve: `cot.total_open_interest`, the
CFTC's weekly all-months-combined number, carried as its own type
(`AggregateOpenInterest`) with its own report Tuesday rather than as a field on
a quote. Putting it on a contract row would assert two false things at once —
that one month holds the product's whole open interest, and that a Tuesday
figure belongs to the price session. The join is by name and needs no mapping
table: `config.COT_COMMODITIES` keys are the same nine strings
`CONTRACT_SPECS` uses, which a test pins.

There is still **no options chain**, and that is a measured fact rather than an
assumption — `yfinance.Ticker(t).options` returns `()` for every ticker here
including named contracts, and no layer carries a strike, premium or implied
volatility. So `fetch_chain` returns `ChainUnavailable` with its reason rather
than an empty ladder. What was missing was the other half of that sentence: the
manual workflow was described and had no entry point. `data/reference/options/`
is now a directory of YAML documents on exactly the terms
`data/reference/positions/` uses — missing directory is an empty ladder, a
present but malformed one raises — where each row carries a mandatory `source`
naming who quoted it and **exactly one** of `premium` or `implied_volatility`,
the other being derived (bisection one way, Black-76 the other). Supplying both
is refused, because two inconsistent numbers on one row leave nothing saying
which was believed. An option whose underlying has no board price that session
is reported unvalued with its reason rather than priced against an invented
forward, and every valued row is stamped `PriceType.MANUAL` with the American
early-exercise caveat riding on it. The discount rate is a stated page constant
(`OPTION_DISCOUNT_RATE`), not a number lifted from the `economic` layer — rho
is reported per rate point so the choice stays visible.

**No routing, and no seam for one.** Every ticket carries
`PROPOSAL — NOT ROUTED` in text, JSON and HTML. Positions come only from a
YAML document under `data/reference/positions/` or a CSV import — this project
ingests no account, broker or clearing feed, so a book can only come from the
user, and a *present but malformed* file raises rather than rendering as an
empty book. With no book entered the hedge section shows a labelled 1,000 MT
**reference calculation** so the arithmetic stays inspectable; it says on the
row that it is not a position.

## Positions, limits and options workflow (Phase 6)

The supervised desk workflow on top of the workstation: a book that can be
imported rather than retyped, exposure decomposed into the views a mandate is
written in, limits that are visible when crossed, the official clearing figure
beside ours, and options the desk supplies itself. Six modules, and one
boundary that runs through all of them.

- `analysis/futures/privacy.py` — the client-record boundary, and the reason
  the phase exists in this shape. Four guards: a **key** guard
  (`CLIENT_RECORD_FIELDS`), a **provenance** guard (any string naming
  `reference/{positions,options,clearing,import_profiles}`), a **path** guard
  (`assert_private_path` refuses anything under `docs/` or on the promotion
  contract), and **section redaction** (`redact_for_public`).
- `analysis/futures/exposure.py` — the seven views (flat price, basis, crush,
  FX, contract month, first notice, residual) and the metrics every limit key
  resolves through.
- `analysis/futures/limits.py` — `DeskLimit` over eleven exposure-backed keys
  with an optional `warn_at`, and `ok`/`warn`/`breach` with headroom.
- `analysis/futures/clearing.py` — `ClearingStatement`/`ClearingLine` from
  `data/reference/clearing/`, `PnlBasis`, and `reconcile()`.
- `analysis/futures/imports.py` — profile-driven import of a broker, clearing
  or ERP export: a dry-run `ImportReport` first, `apply_import` second.
- `analysis/futures/options.py` — extended with tz-aware `quoted_at`,
  `chain_from_csv`/`value_chain` for an externally supplied ladder, and
  `BLACK76_LIMITATIONS`.

Seven rules are load-bearing, each pinned by a test:

- **The public artifact never contains a book, and that is structural rather
  than remembered.** `build_view(audience=...)` defaults to
  **public** — the only default a privacy boundary may have — and the five
  client sections (`book`, `exposure`, `limits`, `clearing`, `options_entered`)
  render `absent` with a reason. `absent`, not `empty`: "nothing entered" and
  "not shown to you" are different states and a public reader is owed the
  second. The private edition goes to `data/workspace/workstation.html`,
  outside `docs/` and deliberately absent from
  `trust.site_promotion.expected_site_paths()`, and
  `assert_no_client_records` runs at write time in `scripts/generate_site.py`
  so a leak fails the page — a tombstone, blocking promotion — rather than
  being published and noticed. This closed a **live leak**: `_book_section`
  wrote `valuation.to_dict()` and the positions file's absolute path straight
  into `docs/workstation.html`, quiet only because CI's positions directory is
  empty. Two things the fix surfaced that a section-level guard alone would
  not have: the hedge, scenario and ticket sections are *sized from the book*,
  so the public edition always works the 1,000 MT reference example; and the
  valuation-derived **alerts** name a limit key and an observed tonnage, which
  is the book in one sentence, so the public edition is built without the
  valuation rather than built and filtered. The guard's own list is narrow on
  purpose — `exposure` was removed from `CLIENT_RECORD_FIELDS` because a public
  reference hedge honestly has one, and a key belongs there only when *no*
  public payload could contain it.
- **The official P&L and ours are two numbers and stay two numbers.** A
  reconciliation reports both columns, labelled by `PnlBasis`, with their
  difference against `CLEARING_RECONCILIATION_TOLERANCE_USD` — and there is no
  `total_usd`, `reconciled_usd` or `net_usd` anywhere in the payload, because a
  single figure would belong to neither desk and be acted on as both. A
  quantity mismatch is its own finding, not a price difference. A contract on
  the statement but not in the book is a finding in its own right — it is a
  position nobody recorded. Physical positions are not reconciled and the
  report says why: a clearer holds futures, not beans.
- **A settlement on a client's statement is `ATTESTED_SETTLEMENT`.** A seventh
  `PriceType`: official for that account, and still not proven by anything this
  project ingests, so `PROVEN_SETTLEMENT_SOURCES` stays empty and its
  confidence ceiling is `BOARD_REFERENCE`, not `EXECUTABLE`.
- **Importing is two steps and the first writes nothing.** `read_import`
  returns accepted rows, rejected rows with reasons, unclaimed columns and the
  file's sha256; `apply_import` refuses while anything was rejected unless
  `allow_partial=True`. Every refusal is one rule — *nothing is guessed*: a
  missing required column refuses the **whole file** before any row is read (a
  per-row failure there reads as bad data rather than a bad mapping); a sign
  convention is declared (`signed` or `side_column`), never inferred, because a
  short 68 lots read as a long 68 is a 136-lot error that looks like a
  position; a date format is tried once, since a fallback is how one file's
  March becomes another's April; an unmapped product code is rejected, because
  `SOJA` is *probably* soybeans and probably is not a book; a blank is never a
  zero. Re-import is idempotent by construction — every row's reference is
  `<sha256[:8]>:<row number>`, derived from the bytes.
- **A limit that cannot be measured produces no row, not a passing one.** A
  green line nobody checked is the most dangerous output here, so the page
  reports configured-vs-measured. `warn_at` must sit *below* `maximum` or it is
  refused — a warning that fires only after the breach is misconfiguration that
  looks like safety. An unknown key **raises** (it used to log and skip, which
  left a desk believing a mandate was being checked). Limits are reported,
  never enforced.
- **Hedging moves tonnes between views; it does not remove them.** Basis
  exposure is `max(unfixed, hedged)` rather than a sum, so tonnes that are both
  count once. The pricing convention (`pricing:`) decides which view a cargo
  lands in; omitting it is legal and means *not stated*, in which case the
  tonnes are counted at their **most exposed** reading and every line built
  from them says the convention was a default. A wrong value is refused — it
  would move tonnes silently between views, which is a risk report saying
  something untrue.
- **An option input carries a source and a moment.** `quoted_at` is
  timezone-aware or refused (whose local time — the desk's, the broker's, or
  the runner's?), and must fall on `quoted_on`. An imported ladder is refused
  when a row has neither premium nor vol, when it has both, when nothing
  anywhere carries a timestamp, and when the rows carry **two** timestamps —
  one chain is one moment, and a Greeks table struck across two sessions is not
  a Greeks table. Every imported row is `PriceType.MANUAL`: a file somebody
  sent us is not a feed. `BLACK76_LIMITATIONS` states the model's limits as
  **data with a direction** — a caveat that does not say which way it bites
  cannot be acted on. The American early-exercise one is `understates`: the
  Black-76 number is a *floor* for an American option, not a value for one.

Surfaces: workstation sections 07–10 and 13 (private), 12 (public — the
chain's absence, the model, its limits); `scripts/import_positions.py`
(dry-run default, exit 1 on any rejection so a nightly import can gate on it);
`scripts/worked_book_example.py`, the end-to-end worked synthetic position.
Docs: `data/reference/{positions,options,clearing,import_profiles}/README.md`.
Client records are **files, never tables** — every table here round-trips
through the committed `data/history/*.csv`, so a positions table would publish
the book by construction — and all four directories are gitignored but for
their READMEs.

## Opportunity engine (`analysis/opportunities/`, Phase 4)

The commercial surface: `docs/opportunities.html`, built by
`app/opportunities_page.py` from the `analysis/opportunities/` package. It
turns the players base and the market layers into ranked, blocked, evidenced
leads. It **originates no data**: every detector reads a table another layer
already fills, and every counterparty comes from `data/reference/players/` as
researched. Nothing is invented — not a name, not a volume, not a trade.

- `domain.py` — the vocabulary, standard library only: `Ladder`,
  `OpportunityStatus`, `BlockerCode`/`HARD_BLOCKERS`, `Evidence`,
  `MarketSignal`, `Counterparty`, `Volume`, `Economics`, `ScoreCard`,
  `Feedback`, `WorkflowRecord`, `Opportunity`, and the identity/id functions.
- `signals.py` — the **only** SQL-aware module here, and the substitution seam.
  Six detectors, each reusing an existing analysis module rather than
  recomputing it: landed advantage (`analysis.origins.comparison`), destination
  flow shift (`inspection_destinations`), commitment shift
  (`export_sales.outstanding_sales`), buyer-region tight stocks
  (`analysis.stocks_to_use`), crush margin (`analysis.origins.crush`), and an
  origin-competitiveness FX move (`currencies`). Each is isolated: one
  detector raising is reported in `coverage`, never a blank page.
- `rules.py` / `scoring.py` / `registry.py` / `workflow.py` / `sensitivity.py`
  / `engine.py` — blockers and counterparty match, the five scores, identity
  and expiry, the private desk file, calculation lineage, and the run.

Six rules are load-bearing, each enforced by a type or a test:

- **A price difference is not an opportunity.** `rules.py` attaches policy,
  freight, quality, window, liquidity, staleness, ingest-outage and
  no-counterparty blockers. A **hard** blocker sets `feasibility = 0` and caps
  the rung below `actionable`; `rank()` sorts by rung *before* composite, so
  the India mandi row — a real +284 USD/MT over CBOT, and uncloseable behind
  the GM import ban — can never head a board titled "what to work today". Its
  `policy_blocked` caveat is reused verbatim from the market registry's own
  `basis` descriptor, not restated here.
- **Unknown stays unknown.** `Volume` requires a stated `basis`; a total with
  no volume raises. A missing ocean freight is a *hard* blocker (there is no
  landed number without it); a missing quality adjustment is soft. Absence
  never becomes an assumption.
- **Five components, shown separately.** Economic, evidence, freshness,
  counterparty and feasibility are each 0–100 with a note a reader can
  reproduce from the numbers printed beside it; the composite is only a sort
  key. `evidence` reads the *evidence's* confidence, not the row's — the row's
  is already dragged down by `inferred` counterparty research, which has its
  own component, and scoring it twice pinned this component at 40 for nearly
  every row. Freshness is judged per item against its own layer's
  `LAYER_MAX_DATA_AGE_DAYS` — the number `main.py` grades on — so a weekly
  source is not punished for being four days old.
- **The privacy boundary is structural.** Desk status, owner, contact dates,
  notes, feedback and audit live only on `Opportunity.workflow`, loaded from
  gitignored YAML under `data/reference/opportunities/`. Four independent
  guards: the public serialiser never builds the `workflow` key,
  `EngineResult.public` excludes any opportunity that has one,
  `save_opportunity_detections` raises on those column names, and the private
  edition is written to `data/workspace/` — outside `docs/` and deliberately
  absent from `trust.site_promotion.expected_site_paths()`, so it can never be
  uploaded to Pages. A *present but malformed* desk file raises rather than
  rendering as an empty book, on the same terms `data/reference/positions/`
  uses.
- **Identity excludes every number.** `identity_key` is
  `(rule_id, product, origin, destination, window_start)` — no price, no edge,
  no score — so today's re-detection of yesterday's lane is the *same*
  opportunity with its original id and first-seen date, not a new one. Ids
  survive an ephemeral CI database because `opportunity_detections` archives
  the public projection and round-trips through `data/history/`. Expiry is the
  signal's own validity plus `OPPORTUNITY_EXPIRY_GRACE_DAYS`; a lapsed row is
  re-stamped `expired` and demoted, and past the grace it is listed from the
  archive as a row, never re-rendered with stale numbers.
- **Nothing here learns.** Feedback (`dismissed`, `false_signal`,
  `no_interest`, `progressed`, `won`, `lost`) is counted and reported;
  it never re-weights a rule. Retuning on five dismissals would be a model
  nobody trained, evaluated or can turn off.

The ladder is stated on the page and is the reason the board is honest about
what it is: **market signal** (something moved) → **lead** (a lane, but
something is missing or blocked) → **actionable** (workable today) →
**proposed trade** → **completed business**. Only the first three are ever
detected; the last two require a human and are therefore private by
construction. There is **no routing and no contact channel** — the output is a
next action for a person to take.

## The crush (`analysis/futures/crush.py`)

**One crush calculation, four surfaces, and it names its contracts.** Until
Phase 6 every "board crush" here was three *provider front-month* series —
Yahoo's `ZS=F`/`ZM=F`/`ZL=F` out of `prices`, which carries no contract column
at all. That number named no delivery month, so it could not be reproduced;
Yahoo rolled each leg on its own unannounced schedule, so a roll-day print
moved for reasons nobody earned and the artifact did *not* cancel across a
spread; and a crusher acting on it would have had no month to place the three
orders in. `named_board_crush(provider, as_of=...)` replaces it, reading the
`forward_curve` layer through the `QuoteProvider` seam and returning either a
`NamedCrush` or a `CrushWithheld` carrying the reason.

Five things are load-bearing:

- **`CrushLevel` is one closed vocabulary of four**, imported by
  `analysis.origins.crush` rather than redefined: `board_reference` (named
  contracts, delayed closes), `board_settlement` (proven settlements — *not
  constructible today*, because `PROVEN_SETTLEMENT_SOURCES` is empty),
  `gross_physical`, `net_plant`. The two board levels are computed here, the
  two physical ones in `analysis/origins/crush.py`.
- **The month convention is derived, documented and rendered.** ZS lists
  Jan/Mar/May/Jul/Aug/Sep/Nov; ZM and ZL list Jan/Mar/May/Jul/Aug/Sep/Oct/Dec.
  Six bean months pair with the products' own month; **November beans crush
  into December**, the first listed product month after it.
  `SOY_CRUSH_PRODUCT_MONTH` is derived from the two listed-month sets and
  pinned against the literal table by test. `propose_crush_hedge` uses the same
  mapping, so the hedge's product legs *follow* its bean month instead of each
  choosing its own nearest.
- **Every leg carries its defence**: symbol, delivery month, observation date,
  price type, provider, and `settlement_proven`. `NamedCrush.workings()` prints
  the lines that reproduce the margin by hand, and they are on the page.
- **Coherence is checked four ways and withheld, never patched.**
  `no_curve`, `no_crush_month` (nothing listed with ≥5 sessions left),
  `mixed_sessions`, `mixed_price_types`, `mixed_providers` (a trusted bean and
  a v1 oil are two provenances in one number), `unsupported_price_type`,
  `settlement_unproven` (a settlement is a claim about the *provider*), and
  `expiry_not_encoded`. A withheld crush is a different type from a computed
  one, so it cannot be read as a margin.
- **`ContractBasis` states what the legs structurally are** and is registry
  data, not a code path: `MARKETS[...]["crush"]["contracts"]` is `named`
  (CBOT), `continuous` (Dalian's akshare main-contract series — arithmetically
  fine, structurally unhedgeable, and the block says so) or `administered`
  (Argentina). A descriptor that omits it fails the build. `ContinuousSeries`
  has no path to a crush at all, and `continuous_withheld()` is the answer a
  surface gets when that is all it holds.

Consumers: `app/block_builders.crush_block` (block 03), `app/origins_page`
section 06, `app/workstation_page` section 04, `analysis/opportunities/signals.
crush_margin_detections`, `analysis/briefing/sections/crush` and the briefings
archive's `crush_spread` block. `tests/test_named_crush.py` pins the convention
and every refusal, including the roll-period, missing-leg, mixed-date and
mixed-price-type boundaries.

## Price semantics (`pricing/semantics.py`)

**One classification of what a stored number is, shared by every surface.**
`PriceType` has six members — `settlement`, `delayed_close`, `last_trade`,
`assessment`, `administered`, `manual` — and it is the *same object* wherever it
appears: `analysis/futures/domain.py`, `analysis/origins/domain.py`,
`app/markets.py` and `trust` all import it rather than defining their own. Four
parallel vocabularies is how the stack came to call one yfinance daily bar a
`DELAYED_CLOSE` on the workstation, "three exchange settlements" on Origins, and
`Confidence.EXECUTABLE` (scored 100/100, ranked on) on Opportunities.

Three rules are load-bearing:

- **A settlement is a claim about the provider, not the number.**
  `PROVEN_SETTLEMENT_SOURCES` is empty and that emptiness is the finding: no
  layer here ingests an authoritative settlement feed, so nothing may be
  rendered as one. Adding a name to that frozenset is the single edit that turns
  a delayed close into a settlement across the whole site.
- **Confidence is derived from the price type, never asserted beside it.**
  `CONFIDENCE_CEILING` grants `EXECUTABLE` only to a settlement-proven type;
  `CONFIDENCE_BY_QUOTE_KIND` is built from it rather than restated. A CBOT/DCE
  board leg is `BOARD_REFERENCE` — above every assessment because it is the
  venue's own daily print, below `EXECUTABLE` because no provider proves it.
  The board crush and the board basis are kept in full; only the claim about
  them changed.
- **The chip names the animal.** `quote_kind_label` renders `board · delayed
  close`, not `board`, everywhere a quote kind appears (block headers, block 04,
  both ledgers). "Board" alone reads as the exchange's own settlement.

`tests/test_price_semantics.py` pins the vocabulary and the derivation;
`tests/test_price_semantics_rendering.py` renders Origins, Opportunities, every
market block and the Workstation through their real builders and fails on an
unnegated "exchange settlement" claim or on the word "executable" reaching a
page.

## Semantic contract (`pricing/policy.py`)

**`semantics.py` says what a number is; `policy.py` says what may then be said
and done with it.** One module, imported by `analysis`, `app`, `trust` and the
tests, because a list of forbidden words living in a test file protects only the
surfaces that test happens to render. The failure it guards is not a template
typo — it is a *future* feature reusing an existing number correctly and
describing it wrongly: an assessment as a firm offer, an administered minimum as
a traded market price, a delayed consumer-endpoint bar as the official close.
Each of those parses, prices and looks right.

`ClaimKind` is five members — `settlement`, `official_close`, `executable`,
`firm_offer`, `traded_price` — and `CLAIM_SUPPORTED_BY` is the whole policy:
which `PriceType` can support each. `EXECUTABLE` needs a proven `SETTLEMENT`
(and `PROVEN_SETTLEMENT_SOURCES` is empty, so nothing ingested reaches it);
`FIRM_OFFER` is supported by **nothing**, because no layer here carries a
counterparty quote; `TRADED_PRICE` is the discriminating case — a board close
and a last trade came out of a trade, an assessment and an administered minimum
did not.

Enforcement is in two places because the claims fail in two ways.

- **Language.** `scan()` reads the tag-stripped text of a page (script and style
  payloads dropped — a Plotly blob is not prose) against `FORBIDDEN_CLAIMS`, and
  a claim **negated in its own sentence is not a claim**: "delayed daily closes,
  not proven exchange settlements" has to stay sayable, and a check that banned
  the word would delete the sentence that tells the truth. Two narrownesses are
  deliberate: "last-traded price" is excluded (SAFEX publishes one, and a pattern
  that fired on the honest label would be switched off within a week), and
  `price_types=` lets the *one* private surface carrying an attested clearing
  statement say "settlement" while no public page can.
- **Structure.** Some claims are made by arithmetic rather than by words.
  `require_hedgeable` is **opt-in**: `NamedContract`/`ContractQuote` declare
  `is_hedgeable = True` and everything else — a `ContinuousSeries`, a bare
  price, the next research artifact nobody has written yet — is refused, at
  `size_leg`, `build_hedge`, `build_ticket` and `named_board_crush`.
  `require_traded_price` refuses to size a futures leg off an administered or
  assessed number, because doing so asserts it is a market price whatever the
  caption says. `assert_confidence_supported` runs in `Evidence.__post_init__`,
  so an over-claimed row cannot be constructed, let alone ranked on — judged on
  `AUTHORITY_TIER`, where `indicative` and `administered` are peers (they differ
  in *kind*, not strength; `CONFIDENCE_RANK`'s worst-wins ordering is a display
  concern and is not reused here). A quote kind that is not a price at all
  (`NON_PRICE_QUOTE_KINDS` — a tonnage, a ratio) has no ceiling to exceed, while
  an *unknown* kind still raises: "not a price" and "nobody classified this" are
  different facts.

The same scan runs inside `trust.site_promotion.verify_site_candidate`, so a
misleading edition is refused at the promotion gate rather than only in CI — a
page that is wrong and looks right is a worse thing to publish than a tombstone.

`tests/test_semantic_contract.py` generates the **whole site** off a seeded
database and scans all thirteen public pages, both private workspace editions
(`data/workspace/opportunities.html`, `workstation.html`), the private trial
dashboard and the briefing text, plus every structural refusal above.

## Trusted ledger — CBOT named contracts (`trust/cbot_benchmarks.py`, DT-16)

The second pilot dataset in the Data Trust Foundation migration
(`docs/plans/2026-08-10-data-trust-foundation.md`) and the first **critical**
one. MAGyP (`trust/magyp_fob.py`) proved artifact capture and structured
parsing against an official physical source; this proves the four things a
*board* price needs, and it is the only trusted adapter that actually runs the
quality engine (MAGyP accepts every candidate unconditionally).

Three registry datasets, one per soy leg: `cbot-soybean-named-contracts`,
`cbot-soybean-meal-named-contracts`, `cbot-soybean-oil-named-contracts`.

- **A named contract, not a front month.** Every observation identifies
  exchange, contract code and delivery month. A symbol that does not resolve
  to a contract of the dataset's own product is refused, not carried as an
  anonymous price. The ticker set comes from
  `fetchers.forward_curve._build_contract_tickers` — the v1 builder itself, not
  a second copy of the month rules — so a reconciliation difference can only
  mean "different parse", never "different contracts".
- **The settlement claim is about the session, not the number.** Yahoo
  publishes no settlement and `price_type` stays `delayed-close`; what
  `settlement.confirmed` checks is that the bar is a *finished* session. The
  same defect `fetchers/_settlement.py` prevents at fetch time, restated as a
  quality rule so a provider substitution cannot lose it.
- **The candle has to be possible.** `ohlc.relationship` rejects a bar whose
  high is below its open/low/close, or whose low is above them. Such a frame
  parses cleanly and is simply not a candle.
- **An extreme move quarantines rather than overwrites.** A day-over-day move
  past `DAILY_MOVE_QUARANTINE_THRESHOLD` (20% — deliberately above CBOT's own
  *expanded* limits, so a move past it cannot be a legitimate session) is
  appended as a quarantined revision: durable, auditable, never reachable by an
  accepted query, and it does **not** displace the accepted history it
  disagrees with. The same-session re-print is the sharper case and is pinned
  by test — the previous-value lookup prefers the observation's own accepted
  revision before falling back to the latest earlier session.
- **Corrections append.** `append_benchmark_correction` writes a superseding
  accepted revision linked to its predecessor; the superseded value stays
  queryable and any edition that pinned it is still reproducible.

Two RFC deviations, both deliberate:

- **`EligibilityScope`** (`trust/domain.py`). `public_eligible` was the only
  gate on every accepted-revision query, so a dataset whose `public-display`
  right is `unknown` — which Yahoo's is, and which must stay fail-closed —
  was unreadable and therefore *unreconcilable*. `revision_is_eligible(revision,
  scope)` adds `INTERNAL`, which uses the rights model's own already-recorded
  `internal-display: allowed`. No rights decision changed and `PUBLIC` (the
  default everywhere) is unchanged. **This is not a licensing change** and does
  not authorise publishing anything.
- **Dataset-scope vs candidate-scope findings.** `DatasetResult` refuses
  `success` while carrying a quarantine or reject finding, which is right for a
  stale payload or a coverage shortfall — facts about the whole dataset that
  dropping a row cannot resolve. A candidate-scope finding *is* resolved by its
  own disposition: the record it complains about was quarantined or rejected
  and is not among the accepted revisions the result exposes. So those findings
  travel on the revision (`finding_ids`), on `BenchmarkDatasetIngestion`, and in
  the run manifest's `findings_summary`; only dataset-scope findings reach
  `evaluate_dataset_health`. Passing them all in would not make the result
  stricter, it would make the evaluator raise instead of returning a verdict.

**The read path is still v1 and switches per dataset.**
`trust/read_path.py` reads `MIRROR_TRUSTED_READ_DATASETS`, a comma-separated
list of registry dataset keys. Unset, empty, `none` or `off` all mean v1; an
unknown key **raises**, because a typo that silently meant "still on v1" is the
one failure a cutover switch must not have; there is deliberately no `all`.
`analysis/futures/providers.open_provider` returns `SqliteQuoteProvider` unless
**all three** soy keys are named — a crush struck from a trusted bean and a v1
oil is two provenances in one number — and falls back, logging, on any ledger
problem, because a storage migration must not be able to take the workstation
down. With the switch on, `TrustedNamedContractProvider` serves curves from
accepted current revisions at `INTERNAL` scope and delegates the still-v1 reads
(continuous series, FX, aggregate open interest) unchanged. Moving the bytes
into a ledger proves provenance, not authority: `settlement_authoritative`
stays `False` and the price type stays `DELAYED_CLOSE`.

**Reconciliation.** `trust/reconciliation.py` holds the shared
`reconcile_frames` (deliberately dumb — no tolerance windows, no fuzzy
matching); MAGyP now uses it too. `scripts/reconcile_cbot_benchmarks.py` runs
daily in the `reconcile-trusted` CI job beside the MAGyP report. It downloads
each ticker **once** through a memoising downloader shared by both parsers: the
settlement guard means a run straddling the cutoff would otherwise hand one
path a session the other never saw, and a divergence that might mean "different
download" is worthless as cutover evidence. Exit 0 reconciled or no session
published, 1 diverged, 2 upstream unavailable. Quarantined revision ids are
*reported, never graded* — v1 has no such state, so a held-back leg is not a
divergence, but a cutover should not be enabled on a day the ledger is holding
legs back.

Tests: `tests/test_trust_cbot_benchmarks.py` (ingestion, quarantine,
rejection, corrections, point-in-time), `tests/test_trust_read_path.py` (the
switch), `tests/test_trust_cutover.py` (the provider and the reconciler). All
network-free.

## Design System
Always read DESIGN.md before making any visual or UI decisions.
All font choices, colors, spacing, and aesthetic direction are defined there.
Do not deviate without explicit user approval.
In QA mode, flag any code that doesn't match DESIGN.md.

## Known Limitations

### Settlement guard — no unsettled bars (Layers 1, 7, 11)

yfinance emits a row for the session **in progress**, so a run landing mid-session used to store an unfinished bar as the day's close (observed: ZS=F 2026-08-07 stored at 1181.25 against a 1156.50 settlement — 2.1% wrong, well under `pipeline/clean.py`'s >10% warning). It self-healed on the next run, but the dashboard published on day D carried day D's partial print.

`fetchers/_settlement.py` drops unfinished bars, applied at `fetchers.yfinance.fetch_one` — the single choke point for every yfinance frame (Layer 1 prices, Layer 7 currencies, Layer 11 forward-curve contracts, which took `Close.iloc[-1]` off the same partial bar). A dropped bar logs a WARNING; the missing day is visible, and today's close lands on the next run.

**The question is "which session date is the newest one that has finished", not "has Chicago settled".** A `SessionRule` is a (timezone, close-time) pair and answers the first; every row labelled after that answer is dropped. Two rules:

- `EXCHANGE_SESSION` — `SETTLEMENT_CUTOFF_LOCAL = (14, 30)` in `SETTLEMENT_TIMEZONE = "America/Chicago"`, one time clearing CBOT 13:15 CT, CME livestock/palm 13:05 CT, ICE cotton 13:20 CT and sugar 12:00 CT, in venue-local time so US DST is handled by zoneinfo.
- `FX_SESSION` — `FX_SESSION_CLOSE_LOCAL = (17, 0)` in `FX_SESSION_TIMEZONE = "America/New_York"`. Spot FX has **no settlement**: it runs continuously Sunday 17:00 NY to Friday 17:00, and Yahoo labels the bar that *closes* at 17:00 on day D with day D's date. `fetch_currencies` passes this rule; everything else takes the exchange one.

Asking the older question — "has Chicago settled? then keep everything" — was wrong in both directions and **measured wrong live on 2026-08-19 at 03:45 UTC**: `BRL=X` returned a bar labelled 2026-08-19 with `High == Open` and `Low == Close`, an FX day under four hours old, and Chicago was past 14:30, so it was stored as that day's FX close. Every `home_per_mt` leg converts at that row's own date, making it a wrong landed cost on every physical origin rather than one wrong FX cell. The same root cause broke the futures side oppositely: the old rule dropped rows *equal to* the Chicago date, so once the CME overnight session opened (19:00 CT, carrying the **next** trade date) its bar compared unequal and survived. One comparison closes both.

Consequence, unchanged: a run landing before the relevant close publishes a dashboard whose newest row for that venue is D−1. That is the intended trade — a gap over a wrong number.

The daily schedule (`.github/workflows/deploy-dashboard.yml`) targets a landing window of ~20:00–24:00 UTC (cron `0 19`, plus GitHub's observed +64 to +298 min scheduler delay), which is after CBOT settlement year-round and picks up Argentina MAGyP and AMS Gulf bids same-day. Brazil CEPEA publishes after 21:01 UTC and is caught on later landings only. The 21:30 UTC fast refresh (`refresh-prices.yml`) lands after the FX close on both sides of US DST — a slot only safe because of the overnight fix above. Correctness does not depend on either cron — the guard does.

### Latency (`latency/`, LATENCY.md)

One vocabulary for "how old is the number on the page, and which part of that age did we cause". `domain.py` is stdlib-only (stage chain, five `LatencyClass`es and their objectives, per-layer `ObservationClock` and declared `provider_delay`), `clock.py` the per-run instrumentation, `measure.py` the single DB-aware seam, `report.py` the rendering.

- **Acquisition** (observation → fetched) contains the provider's delay *and* our cadence wait, and cannot be split by observing ourselves; `provider_delay` is therefore **declared per layer with a stated basis**, never inferred from our own timings, and `cadence_wait = acquisition − provider_delay` is the share our schedule chose. **Pipeline** (fetched → publicly readable) is wholly ours.
- **A missing stamp is `Verdict.UNKNOWN`, never `MEETS`.** `data_freshness` gained `observed_at`, `fetch_started_at`, `fetch_completed_at`, `stored_at`; `save_freshness` looks the run clock up by layer name at the single write choke point, so the eight `_mark_*` paths need no knowledge of it, and a write with no fetch behind it records NULLs rather than `now` — a fabricated fetch stamp would make a slow fetch look instant.
- **Observation is an instant where the venue hour is known and `DAY`-granular where it is not.** No invented hours; a day-granular age is a stated lower bound.
- `observed_at` is stamped on **every** status, not just success — a stale layer's newest observation is what sizes the hole.

Surfaces: the masthead's "Board and FX priced from data N old" (generation time is not observation time, and here they are routinely a day apart), four separate Observed/Fetched/Age/Last Success columns in the Layer Freshness table, and `docs/manifest.json`.

### Fast refresh (`main.py --fast`, `scripts/refresh_prices.py`)

`config.FAST_REFRESH_LAYERS` (`prices`, `currencies`, `forward_curve`) over `FAST_REFRESH_HISTORY_PERIOD` (`1mo`) — the **same code** as the daily build with two arguments different, so the settlement guard, cleaners, `LAYER_MIN_KEYS` floor and freshness grading cannot drift between the paths. Measured 53 s end to end against the daily build's 6 m 02 s; the lever is `DEFAULT_HISTORY_PERIOD`, since a 15-year yfinance pull benchmarks at 24–32 s/ticker against 1–3 s for a short window. DCE and every scraped physical leg are deliberately excluded — they publish once a day, and doubling the request rate on unfriendly upstreams trades reliability for freshness that is not there.

**A fast refresh's failure mode is not a crash — it is a structurally perfect site that knows less than the one it replaces.** It inherits 26 layers from whatever database it sits on, and on an unseeded runner that is only what `data/history/*.csv` carries, so PSD, weather, COT and crop progress would silently vanish behind legal empty states. `trust.site_promotion.verify_refresh_is_not_a_regression` compares the candidate's `manifest.json` against the published edition's: no layer's observation may go backwards or vanish, no page that rendered may tombstone. A missing *candidate* manifest is a refusal (being unable to compare is being unable to pass); a missing *published* one passes loudly (the first build has nothing to compare against). Refusal is a **no-op** — `scripts/refresh_prices.py` exits before the Pages upload and the live edition is untouched. `docs/manifest.json` is a published **asset**, not a page: it is in `trust.site_promotion.PUBLISHED_ASSETS` (link-checked for existence) rather than `expected_site_paths` (crawled and timestamp-checked as HTML).

### Front-month roll-day discontinuities (Layer 1)

`fetchers/yfinance.py` pulls front-month commodity tickers (`ZS=F`, `ZL=F`, etc.). yfinance silently switches the underlying contract as expirations approach, which introduces artificial price gaps on roll days. These gaps do not represent economic moves.

**Affected analyses:**
- `analysis/technical.py` — SMA crossovers, RSI (Wilder), MACD (EMA-based), Bollinger Bands, historical volatility. All compute on `Close` and propagate the discontinuity.
- `analysis/signals.py` — MA/MACD crossovers, RSI extremes, RSI divergence, Bollinger squeeze. Mitigated: signals within ±3 business days of an estimated roll date (first business day on/after the 15th of an active delivery month — CME expiry; validated against 15y of ZS=F gaps — see `analysis.signals.is_near_roll`) are demoted to `info` severity and tagged `(near-roll)` in both the briefing and the dashboard.

**Not affected:**
- `analysis/spreads.py` — crush spread, oil/meal ratio, bean/corn ratio. Computed on raw active-contract closes; the artifact appears on each leg simultaneously and largely cancels.
- `analysis/soy_analytics.py` emerging-markets basis — CEPEA/SAFEX/India crush vs CBOT compare raw `Close` values, level-sensitive but consistent.
- Layers 2–19 (USDA fundamentals, FRED, weather, PSD, currencies, forward curve, COT, WASDE, EIA, CONAB, domestic spot prices, AgRural FOB). None depend on a continuous front-month price.

**Future work (deferred):** a Panama-adjusted `adj_close` column on the `prices` table with the dual-column read pattern (technicals use adjusted, spreads/basis stay on raw). See plan `~/.claude/plans/ontinuous-contract-roll-for-glowing-kernighan.md` for the Phase 2 spike gate and Phase 3 scope.
