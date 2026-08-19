# Does USDA PSD aggregate to a clean world stocks-to-use?

Research for M15 (parent #142).
Date: 2026-08-12. Checkout: branch `main` @ `1396870`; DB `data/storage/mirror_market.db`
(`data_freshness.layer_name='psd'` → `last_success 2026-08-07 11:21:49`, `rows_fetched 72617`, `status success`).
Live probes run 2026-08-12 ~02:00–02:30 local against `api.fas.usda.gov` (with the repo's own `FAS_API_KEY`)
and the three `apps.fas.usda.gov/psdonline/downloads/*.zip` bulk files. Current PSD/WASDE vintage at probe
time = **July 2026** (`month: "07"`, WASDE-673); the August WASDE had not yet published.

---

## Verdict — YES, and it costs zero new fetches

**A defensible world stocks-to-use is computable from PSD, and the cleanest route does not need the API at all.**

The recommended definition, per commodity per marketing year:

```
world S/U  =  Ending Stocks (attr 176)  /  Domestic Consumption (attr 125)
                                            [cotton: Domestic Use (attr 142)]
```

evaluated on the **World** aggregate — and the World aggregate is **exactly the unfiltered sum of every
country row already inside the bulk CSVs Layer 6 downloads**. Verified on 197 (commodity, marketing-year)
pairs — soybeans 1964–2026, corn 1960–2026, cotton 1960–2026 — with **zero discrepancies on any
*extensive* attribute** (the ratio attributes `184` Yield and `195` Stocks-to-Use do **not** sum and must be
excluded — see §3.2 and §8.4)
against USDA's own World row from the API (§3). The repo throws that total away in `fetchers/psd.py:127`
when it filters to 28 countries; summing *before* the filter reconstructs it bit-for-bit.

Three things must be right or the number is wrong:

1. **Denominator = Domestic Consumption only.** Not `Domestic Consumption + Exports` (what
   `analysis/stocks_to_use.py:83` correctly does for the *US*), and not Total Distribution. World exports are
   already inside some importer's domestic consumption; adding them double-counts trade and moves world
   soybean S/U from 29.19% to 20.33% (§5.1). USDA's own published world ratio uses consumption only —
   WASDE-673 cotton text: world ending stocks 75.7 mb over "Global consumption … 120.0 million bales" =
   **63.1 percent**, which is the number WASDE prints (§4.3).
2. **Region set = World, or World-less-China, or World.** Not "our 28 countries". The 28-country sum is
   not an approximation of the world for the grains: it captures 41.3% of world wheat imports and 67.3% of
   world wheat domestic consumption, and produces a wheat S/U of 39.74% against a true 34.05% — a 5.7 pp
   error in the same direction every year (§5.2).
3. **Grains carry a WASDE adjustment PSD does not apply.** For corn/wheat/coarse grains, WASDE's world
   "Domestic Total" = PSD world Domestic Consumption **+ (world exports − world imports)**. Verified to the
   last decimal (§4.2). Oilseed tables apply no such adjustment. So a grain S/U built on raw PSD will differ
   from WASDE's printed table by ~0.2–0.4 pp; a soy one will not.

**World-less-China is not our invention — USDA publishes it**, as a `World Less China` line in every WASDE
international supply-and-use table, and it is a *pure subtraction* of the China row (verified, §4.4). So we
can reproduce it exactly.

Recommended shipping set: **world S/U** and **world-less-China S/U** for Soybeans, Soybean Meal, Soybean Oil,
Palm Oil, Corn, Wheat, Rapeseed complex. **Cotton needs a new attribute before any S/U at all — including the
US one the briefing already claims to print** (§6.3). Major-exporter S/U is computable but is a different
statistic and needs care (§7.3).

---

## 1. What the repo has today

| Thing | Where | State |
|---|---|---|
| Source | `config.py:263-266` — three bulk zips, `psd_oilseeds_csv.zip` / `psd_grains_pulses_csv.zip` / `psd_cotton_csv.zip` | Live; all three returned HTTP 200 on 2026-08-12. Zip members dated **Jul 10 11:33** — one file per month, no API key |
| Commodity filter | `config.py:269-286` (10 codes), applied `fetchers/psd.py:117` | Codes are stored unpadded (`"813100"`) because pandas reads `Commodity_Code` as int. Fine for the CSV; **the API rejects unpadded codes** — `/commodity/813100/...` → HTTP 404, `/commodity/0813100/...` → 200 |
| Country filter | `config.py:290-306` (28 names), applied `fetchers/psd.py:126-127` | **This is where the world total is destroyed.** The CSVs carry 67–125 countries per commodity |
| Attribute filter | `config.py:308-312` (9 names), applied `fetchers/psd.py:133` | Missing cotton's consumption line (§6.3) |
| Table | `pipeline/schema.py:76-84` | `PRIMARY KEY (commodity, country, year, attribute)` — a `country='World'` row needs **no schema change** |
| Write / read | `pipeline/store.py:394-407`, `pipeline/query.py:121-123` | Pass-through; no country whitelist at write time |
| US ratio | `analysis/stocks_to_use.py:38,83,88` | `country="United States"`, `total_use = Domestic Consumption + Exports`, `ratio = ending_stocks / total_use` — correct for one country |
| Briefing | `analysis/briefing/sections/stocks_to_use.py:22-31` | Prints six US balance sheets incl. `"Cotton"` |
| Trust registry | `trust/registry.py:838-840` | `PILOT_REGISTRY` covers AgRural, MAGyP, Yahoo/CBOT, FX only. **There is no PSD dataset contract** — grep for `psd` across `trust/` returns nothing |
| Live DB | `data/storage/mirror_market.db` | 84,699 rows, **7 commodities** (no rapeseed complex), **27 countries** (no Canada), years 1960–2026 — i.e. this local DB predates the rapeseed/Canada config additions. CI's DB will differ |

`analysis/stocks_to_use.py:20-22` already reasons about the denominator:

> `# Total use is Domestic Consumption + Exports. PSD's "Total Distribution"`
> `# is NOT usable here: it equals Total Supply (Beginning Stocks + Production`
> `# + Imports), which understates the ratio's denominator meaning.`

That comment is **correct and confirmed** (§3.3) — and it is exactly the reasoning that must be *inverted* at
world level, because at world level "+ Exports" becomes the double-count.

---

## 2. What PSD actually publishes

### 2.1 There is an explicit World region, and its code is `00`

`GET https://api.fas.usda.gov/api/psd/regions` (header `X-Api-Key`), observed verbatim:

```json
[{"regionCode": "R00", "regionName": "World"}, {"regionCode": "R01", "regionName": "North America"},
 {"regionCode": "R02", "regionName": "Caribbean"}, {"regionCode": "R03", "regionName": "Central America"},
 {"regionCode": "R04", "regionName": "South America"}, {"regionCode": "R05", "regionName": "European Union"},
 {"regionCode": "R07", "regionName": "Former Soviet Union - 12"}, {"regionCode": "R09", "regionName": "Middle East"},
 {"regionCode": "R10", "regionName": "North Africa"}, {"regionCode": "R11", "regionName": "Sub-Saharan Africa"},
 {"regionCode": "R12", "regionName": "South Asia"}, {"regionCode": "R14", "regionName": "Oceania"},
 {"regionCode": "R16", "regionName": "Other Europe"}, {"regionCode": "R17", "regionName": "Southeast Asia"},
 {"regionCode": "R18", "regionName": "East Asia"}]
```

The **data** endpoint that serves it is `/api/psd/commodity/{7-digit-code}/world/year/{marketYear}`, and the
rows come back with `"countryCode": "00"`. Observed, soybeans MY2025, July vintage (values in 1000 MT except
attr 4 in 1000 HA):

| attributeId | name | value |
|---|---|---|
| 4 | Area Harvested | 142,840 |
| 20 | Beginning Stocks | 125,933 |
| 28 | Production | 429,461 |
| 57 | Imports | 186,342 |
| 86 | Total Supply | 741,736 |
| 88 | Exports | 187,078 |
| 7 | Crush | 372,486 |
| 149 | Food Use Dom. Cons. | 25,801 |
| 161 | Feed Waste Dom. Cons. | 31,046 |
| 125 | Domestic Consumption | 429,333 |
| 176 | Ending Stocks | 125,325 |
| 178 | Total Distribution | 741,736 |
| 184 | Yield | (present) |

Endpoint shapes that do **not** exist (all HTTP 404, observed): `/commodity/{code}/world` (no year),
`/commodity/{code}/world/allYears`, `/commodity/{code}/region/R00/year/{y}`, `/api/psd/releaseDates`,
`/api/psd/dataReleaseDates`. `apps.fas.usda.gov/PSDOnlineDataServices/api/*` does not resolve. So the API is
**one request per (commodity, year)** — ~630 requests to backfill 10 commodities × 63 years. That alone is a
reason to prefer the CSV sum.

*Caveat on documentation:* the FAS OpenData docs site (`apps.fas.usda.gov/opendataweb/home`) is a JavaScript
SPA (1,510-byte shell; its `main.*.js` bundle contains no `api/psd/...` route literals), so I could **not**
retrieve a rendered, human-readable endpoint reference. Every endpoint fact above is from a live probe, not
from a doc page. Confidence in the endpoints: **high** (200s with correct payloads). Confidence that no
better/bulk world endpoint exists: **medium** — I probed five plausible shapes, not an exhaustive set.

### 2.2 The World row is NOT in the bulk CSVs

Directly observed on `psd_oilseeds.csv` (801,669 rows, columns
`Commodity_Code, Commodity_Description, Country_Code, Country_Name, Market_Year, Calendar_Year, Month,
Attribute_ID, Attribute_Description, Unit_ID, Unit_Description, Value`):

- rows with `Country_Code == "00"`: **0**
- 168 distinct `Country_Name`, none of them `World`
- the only aggregate-looking labels are `European Union`, `EU-15`, `Union of Soviet Socialist Repu`, and
  `Other` (`ZZ`, 377 rows, **palm oil only** in this file)

So the World row must be either fetched from the API or **reconstructed by summation** — and summation is
exact (§3).

### 2.3 Attribute IDs that matter

Observed from the CSVs and `/api/psd/commodityAttributes` (85 attributes total):

| ID | Name | Soy complex | Corn/Wheat | Cotton |
|---|---|---|---|---|
| 20 | Beginning Stocks | ✓ | ✓ | ✓ |
| 28 | Production | ✓ | ✓ | ✓ |
| 57 | Imports | ✓ | ✓ | ✓ |
| 86 | Total Supply | ✓ | ✓ | ✓ |
| 88 | Exports | ✓ | ✓ | ✓ |
| 7 | Crush | ✓ | — | — |
| 125 | Domestic Consumption | ✓ | ✓ | **—** |
| 142 | **Domestic Use** | — | — | **✓** (this is cotton's consumption) |
| 130 / 192 | Feed Dom. Consumption / FSI Consumption | — | ✓ | — |
| 149 / 161 | Food Use / Feed Waste Dom. Cons. | ✓ | — | — |
| 150 | Loss | — | — | ✓ (can be negative: −188 world MY2025) |
| 176 | Ending Stocks | ✓ | ✓ | ✓ |
| 178 | Total Distribution | ✓ | ✓ | ✓ |
| 81 / 113 | TY Imports / TY Exports (trade year) | — | ✓ | — |
| **195** | **Stocks-to-Use (PERCENT)** | — | — | **✓ cotton only** (7,777 rows) |
| 4 | Area Harvested | ✓ | ✓ | ✓ |
| 184 | Yield **(non-additive)** | ✓ | ✓ | ✓ |

Three traps live in that table. **Attr 195 is a percent and must never be summed** across countries. **Attr
184 Yield is a rate and must not be summed either** — verified 2026-08-12: summing country Yield for
soybeans MY2025 gives 95.39 MT/HA against USDA's World 3.01 MT/HA (wheat 253.41 vs 3.83; cotton 43,915 vs
903.90 KG/HA). Both are present in the CSVs, so an "aggregate everything" implementation produces two
silently absurd rows. And **attr 195's World value disagrees with USDA's own published world ratio** —
see §4.3.

---

## 3. Does summing the countries equal the World total? Yes — exactly

### 3.1 The sweep

For each marketing year, I summed every country row in the bulk CSV (no country filter, no exclusions) and
compared attribute-by-attribute against the API World row:

```
Soybeans (2222000): years 1964-2026 n=63 world_rows_checked=63 discrepancies=[]
Corn     (0440000): years 1960-2026 n=67 world_rows_checked=67 discrepancies=[]
Cotton   (2631000): years 1960-2026 n=67 world_rows_checked=67 discrepancies=[]
```

Attributes compared: 20, 28, 57, 86, 88, 7, 125, 142, 150, 176, 178. **197 year-commodity pairs, zero
mismatches** — including cotton's negative `Loss`. Cross-sectionally, MY2025 for all 10 tracked commodities
(soybeans, soy oil, soy meal, palm oil, corn, wheat, cotton, rapeseed, rape oil, rape meal): **zero
mismatches** on every attribute.

Example, soybeans MY2025 — `bulk_sum` vs `world`, per attribute:

```
Beginning Stocks     world= 125933.0  bulk_sum= 125933
Production           world= 429461.0  bulk_sum= 429461
Imports              world= 186342.0  bulk_sum= 186342
Exports              world= 187078.0  bulk_sum= 187078
Crush                world= 372486.0  bulk_sum= 372486
Domestic Consumption world= 429333.0  bulk_sum= 429333
Ending Stocks        world= 125325.0  bulk_sum= 125325
Total Supply         world= 741736.0  bulk_sum= 741736
Total Distribution   world= 741736.0  bulk_sum= 741736
```

### 3.2 There is no Rest-of-World residual to worry about — and no double count

Both worries turn out to be non-issues, for observable reasons:

- **Residual.** Only palm oil carries an explicit `Other` (`ZZ`) country row, and it is *inside* the file, so
  the sum already contains it. For soybeans, `Country_Name == "Other"` has **0 rows**. Countries with no
  reported ending stocks (Uganda, Burma, Guatemala, Ecuador, Switzerland, Bosnia, Nicaragua, Syria on MY2025
  soybeans) are present with an explicit `0.0`, not absent — so they neither inflate nor deflate the total.
- **Double count of EU / defunct entities.** The aggregate and its members **never coexist in the same
  marketing year**. Observed on soybeans: `France` last appears MY1990; `EU-15` spans MY1991–1998;
  `European Union` starts MY1999 and runs to MY2026; `Union of Soviet Socialist Repu` appears through the
  1970s–80s and `Russia` starts MY1990. MY2025 contains the `European Union` row and **zero** member-state
  rows. This is *why* the per-year sum can equal the world for all 197 pairs — but note the safety comes from
  USDA's file layout, not from anything we control, so it should be **asserted in code, not assumed** (§7.6).

### 3.3 What the balance identities actually are

Both hold identically on the World row (observed, soybeans MY2025):

```
Total Supply       = Beginning Stocks + Production + Imports = 125,933 + 429,461 + 186,342 = 741,736 ✓
Total Distribution = Domestic Consumption + Exports + Ending Stocks = 429,333 + 187,078 + 125,325 = 741,736 ✓
```

So `Total Distribution == Total Supply` at world level, exactly as `analysis/stocks_to_use.py:20-22` says. It
is an accounting closure, not a "use", and it is useless as a denominator. Note also that it closes *despite*
world imports (186,342) ≠ world exports (187,078) — because each country's own balance closes by construction
and summation preserves that. The trade imbalance is absorbed in stocks and consumption, country by country.

---

## 4. Cross-check against USDA's own published world numbers (WASDE-673, July 2026)

This is the strongest available grounding: the same vintage, printed by USDA, in a document that is not PSD.

### 4.1 PSD's World row IS the WASDE world line (oilseeds)

WASDE-673 p.28, *World Soybean Supply and Use*, 2025/26 Est., million MT — transcribed verbatim:

```
World 2/          125.93   429.46   186.34   372.49   429.33   187.08   125.33
  World Less China  81.44   408.56    73.34   263.49   295.43   186.96    80.96
```

(columns: Beginning Stocks, Production, Imports, Domestic Crush, Domestic Total, Exports, Ending Stocks)

Against the PSD API World row: 125,933 / 429,461 / 186,342 / 372,486 / 429,333 / 187,078 / 125,325.
**Identical to the rounding.** Same for soybean meal (WASDE p.29 world 17.94 / 292.43 / 80.84 / 287.27 /
84.62 / 19.33 vs PSD 17,944 / 292,431 / 80,841 / 287,270 / 84,616 / 19,330).

Oilseed-table footnotes, verbatim:

> `1/ Data based on local marketing years except Argentina and Brazil which are adjusted to an
> October-September year.`
> `2/ World imports and exports may not balance due to differences in local marketing years and to time lags
> between reported exports and imports. Therefore, world supply may not equal world use.`

### 4.2 Grains DO carry an adjustment PSD's world row does not

WASDE grain-table footnotes (p.20 coarse grains, p.18 wheat, p.22 corn), verbatim:

> `1/ Aggregate of local marketing years. 2/ Total foreign and world use adjusted to reflect the differences
> in world imports and exports. 3/ World imports and exports may not balance due to differences in marketing
> years, grain in transit, and reporting discrepancies in some countries.`

That footnote is arithmetically visible:

| | PSD world Domestic Consumption | + (world exports − world imports) | = | WASDE world "Domestic Total 2/" |
|---|---|---|---|---|
| Wheat 2025/26 | 819,541 | +5,071 (227,084 − 222,013) | 824,612 | **824.61** ✓ |
| Corn 2025/26 | 1,301,298 | +23,937 (220,662 − 196,725) | 1,325,235 | **1,325.24** ✓ |
| Soybeans 2025/26 | 429,333 | *(not applied)* | — | **429.33** ✓ |

Every other column (beginning stocks, production, imports, exports, ending stocks) matches PSD exactly for
wheat and corn too. **Only the consumption column is adjusted, and only in the grain tables.** This is the
single most likely source of a "why doesn't our number match WASDE" complaint.

### 4.3 USDA's own world S/U for cotton uses consumption only — and PSD's attr 195 does not

WASDE-673 p.5, cotton narrative, verbatim:

> `For 2025/26, world production is lowered by 750,000 bales following a like reduction for Brazil. Global
> consumption is reduced modestly to 120.0 million bales … Ending stocks are reduced by about 900,000 bales,
> mostly due to the smaller Brazilian crop, lowering the stocks-to-use ratio to 63.1 percent.`

PSD world MY2025 cotton: Ending Stocks 75,723, Domestic Use 119,953 (1000 480-lb bales).
`75,723 / 119,953 = 63.13%` → **matches USDA's printed 63.1 percent.**

But the PSD **attribute 195 "Stocks-to-Use (PERCENT)" on the World row reads 45.9495**, which is
`75,723 / (119,953 + 44,843) = 45.95%` — ending stocks over domestic use *plus exports*. That is the correct
per-*country* formula, and *FAS defines it that way* — Cotton: World Markets and Trade, July 2026, p.4-5,
verbatim:

> `Loss: Cotton that has been destroyed or cannot be accounted for. Loss may also include "negative loss"
> when the estimates of Ending Stocks exceed the difference between Total Supply and total use (total use is
> equal to Exports plus Domestic Use.`

(sanity check on a country: China cotton MY2025, `36,562 / (41,000 + 75) = 89.01%` = the stored attr 195 value
89.01 ✓.)

**So PSD ships a stocks-to-use attribute whose World value contradicts USDA's own published world ratio by
17 pp, because the per-country formula was applied mechanically to an aggregate row.** Do not use attr 195
for the world. This is the cleanest single piece of evidence for the whole verdict.

### 4.4 World-less-China is published by USDA and is a pure subtraction

Every WASDE international table carries a `World Less China` line — observed at 21 distinct places in
WASDE-673 (wheat, coarse grains, corn, rice, cotton, soybeans, soybean meal, soybean oil, …). It reconciles
as plain subtraction of the China row:

| Soybeans 2025/26 | World | China | World − China | WASDE "World Less China" |
|---|---|---|---|---|
| Ending Stocks | 125.33 | 44.37 | 80.96 | **80.96** ✓ |
| Production | 429.46 | 20.90 | 408.56 | **408.56** ✓ |
| Imports | 186.34 | 113.00 | 73.34 | **73.34** ✓ |
| Domestic Total | 429.33 | 133.90 | 295.43 | **295.43** ✓ |
| Exports | 187.08 | 0.12 | 186.96 | **186.96** ✓ |

Same for soybean meal (PSD-derived less-China ES 18,059 / DC 202,120 vs WASDE 18.06 / 202.12 ✓). For **wheat
and corn the subtraction must be done on the *adjusted* consumption**: PSD-derived less-China wheat DC is
669,541, WASDE prints 674.61 — the 5,071 import/export adjustment again; add it back and it matches.

**On the "China stocks problem" itself I will not overclaim.** What is *observed* is that USDA publishes a
World-Less-China aggregate for every crop, and that China holds a third to nearly two-thirds of world stocks in
these tables — so any world ratio is dominated by one country's balance sheet.

| MY2025 ending stocks | World | China | China share |
|---|---|---|---|
| Soybeans | 125,325 | 44,369 | **35.4%** |
| Corn | 298,670 | 177,148 | **59.3%** |
| Wheat | 279,035 | 122,645 | **44.0%** |

What I could **not** verify from a primary source is USDA's stated *rationale* or the date the line was introduced: `www.usda.gov`
returns HTTP 403 to both WebFetch and curl (browser UA included), so `usda.gov/previous-changes-wasde` was
unreachable. A web search attributed the addition to the May 2019 WASDE; **that is secondary and I am not
relying on it.** Treat "USDA publishes World Less China" as observed fact, and "USDA does so because Chinese
stocks are less transparent" as **unverified inference**.

### 4.5 Marketing-year alignment — USDA's own words

There is no single global marketing year. The primary statements are the WASDE footnotes quoted above:
oilseeds are *"based on local marketing years except Argentina and Brazil which are adjusted to an
October-September year"*; grains are an *"Aggregate of local marketing years"* whose imports and exports
*"may not balance due to differences in marketing years, grain in transit, and reporting discrepancies in some
countries."* PSD also carries separate **trade-year** attributes for grains (81 `TY Imports`, 113 `TY Exports`)
and separate `(Local)` commodity codes — e.g. `0813101 "Meal, Soybean (Local)"` alongside `0813100
"Meal, Soybean"`; the world `0813101` row for MY2025 returns a *different* crush (104,500 vs 372,486) and its
own attribute set, so the two are not interchangeable. **I did not find a PSD documentation page defining the
local-vs-international convention** (the psdonline app is a SPA and `fas.usda.gov/data/...psd-online` returns
403), so beyond the WASDE footnotes I record: *I don't know* precisely how each `(Local)` series is
constructed. It does not block the recommendation, because we would use the same `2222000`/`0813100`/`4232000`
codes USDA itself aggregates in WASDE.

---

## 5. How wrong the naive answers are (MY2025, July 2026 vintage)

### 5.1 Denominator choice

| Commodity | **ES / DomCons** (recommended) | ES / (DomCons + Exports) (US formula applied to world) | WASDE-adjusted ES / (DC + X − M) | ES / Total Distribution |
|---|---|---|---|---|
| Soybeans | **29.19%** | 20.33% | 29.14% | 16.90% (=125,325/741,736) |
| Soybean Oil | **8.88%** | 7.38% | 8.71% | 6.87% |
| Soybean Meal | **6.73%** | 5.20% | 6.64% | 4.94% |
| Palm Oil | **19.82%** | 12.52% | 19.21% | 11.13% |
| Corn | **22.95%** | 19.62% | **22.54%** ← matches WASDE table | 16.40% |
| Wheat | **34.05%** | 26.66% | **33.84%** ← matches WASDE table | 21.05% |
| Cotton | **63.13%** (=75,723/119,953, matches WASDE text) | 45.95% (= PSD attr 195 World) | n/a | 31.51% |

### 5.2 Region-set choice — the 28-country sum is not the world

Repo-country share of the world total, MY2025 (observed):

| Commodity | ES | Domestic Cons. | Imports | Exports | 28-country S/U | **true world S/U** |
|---|---|---|---|---|---|---|
| Soybeans | 97.6% | 91.9% | 88.1% | 97.5% | 31.01% | **29.19%** |
| Soybean Meal | 73.2% | 83.1% | 63.8% | 94.5% | 5.92% | **6.73%** |
| Soybean Oil | 81.8% | 89.6% | 68.0% | 83.9% | 8.10% | **8.88%** |
| Palm Oil | 81.4% | 81.4% | 70.4% | 92.2% | 19.83% | **19.82%** |
| Corn | 93.7% | 85.7% | 57.3% | 85.4% | 25.11% | **22.95%** |
| Wheat | 78.5% | 67.3% | 41.3% | 60.8% | 39.74% | **34.05%** |
| Rapeseed | 93.8% | 87.8% | 85.0% | 81.3% | — | **12.40%** |

The error is not small and not stable: palm oil happens to land within 0.01 pp by coincidence of offsetting
coverage, wheat is off by 5.7 pp. Countries absent from the 28-list entirely (they exist in PSD, we just don't
ask for them) include **Russia, Ukraine, Egypt, Turkey, Kazakhstan, Iran, Saudi Arabia, Philippines, Burma,
Benin, Zambia** and ~40–100 others depending on commodity. Also observed: `Cote d'Ivoire`, `Tanzania`,
`Kenya`, `Uganda` are in our list but have **no rows at all** for several commodities (they get silently
dropped by `isin`), and `European Union` has no cotton row.

---

## 6. Concrete gaps — what to change

### 6.1 The World row (the only real work)

Two options; **Option A is recommended.**

**Option A — sum before filtering, inside `fetchers/psd.py`.** The world total is already in the bytes we
download. Add, in `_filter_psd` *before* the country filter at line 126, a per-`(commodity, year, attribute,
unit)` sum over **all** countries, emitted as synthetic rows with `country="World"`, then let the existing
country filter run on the rest. Cost: zero extra HTTP, full 1960–2026 history, exact by the §3 proof. Must
exclude non-additive attributes (`195` Stocks-to-Use, `184` Yield, `181` and any `(PERCENT)`/`(RATIO)`/
`(MT/HA)`/`(KG/HA)` unit) — the safe rule is *sum only where `Unit_Description` is a quantity unit*, not a
name blocklist.

**Option B — fetch `/commodity/{code}/world/year/{y}` from the API.** Authoritative and self-documenting, but
needs `FAS_API_KEY` (which would newly gate Layer 6, today key-free — see `config.py:263` comment "no API
key"), 7-digit **zero-padded** codes, and one request per commodity-year (~630 for full history, ~10/run if
only the current + prior MY). Worth adding later as a **reconciliation assert** against Option A rather than
as the fetch path.

Storage needs nothing: `country='World'` fits `pipeline/schema.py:76-84` as-is, and `read_psd()` returns it
untouched. Downstream must then be explicit — `compute_stocks_to_use(psd, country="United States")` already
takes a country, but anything that aggregates or ranks countries (`analysis/soy_analytics.py`,
`analysis/briefing/sections/psd.py`) would start seeing a `World` row and must exclude it.

### 6.2 A world-aware denominator

`analysis/stocks_to_use.py:83` hard-codes `Domestic Consumption + Exports`. It needs a mode switch, not an
edit: keep the current formula for single countries, use consumption-only for `World` /
`World less China` / any multi-country aggregate. Getting this wrong is silent — both formulas produce a
plausible-looking percentage.

### 6.3 Cotton is broken today, world or not

`config.py:308-312` lists `"Domestic Consumption"`, which **cotton does not have** — cotton's consumption
attribute is `142 "Domestic Use"`. Consequence, observed in the live DB:

```
sqlite> select attribute, count(*) from psd where commodity='Cotton' group by 1;
Beginning Stocks 1726 | Ending Stocks 1726 | Exports 1726 | Imports 1726
Production 1726 | Total Distribution 1726 | Total Supply 1726
```

No consumption row at all → `compute_stocks_to_use` drops every cotton row at its `dropna` → the briefing's
`_S2U_COMMODITIES` entry `"Cotton"` (`analysis/briefing/sections/stocks_to_use.py:28`) can only ever print
`Cotton: No data`. **This is a pre-existing bug that M15 happens to surface.** Fix = add `"Domestic Use"`
(and, if wanted, `"Loss"`) to `PSD_TARGET_ATTRIBUTES`.

### 6.4 Grain adjustment, if we want to match WASDE's printed table

Store nothing new — it's computable from stored attributes: `DC + Exports − Imports`, applied **only** to
world/less-China aggregates of corn, wheat (and coarse grains if ever added). Decide once and label the chart:
"PSD world balance" (unadjusted) or "WASDE world use" (adjusted). Both are defensible; mixing them is not.

### 6.5 Trust registry

There is no PSD dataset contract in `trust/registry.py` (`PILOT_REGISTRY` at :838-840 lists AgRural, MAGyP,
Yahoo, FX only). If a world S/U becomes a published surface, PSD arguably graduates from "legacy layer" to a
registered dataset — cadence monthly, identity `(commodity, country, year, attribute)`, rights: US Government
work, and the bulk zips carry no licence wall. **Rights assessment is out of scope here and I have not done
it.**

---

## 7. Pitfalls that would make a naive aggregation wrong

1. **Summing the country rows we happen to store.** Ranges from −5.7 pp (wheat) to +1.8 pp (soybeans) error,
   with the sign flipping by commodity (§5.2).
2. **Putting exports in a world denominator.** Double-counts every traded tonne; −8.9 pp on soybeans. PSD's
   own attr 195 makes exactly this mistake on the World row (§4.3).
3. **Using `Total Distribution` (or `Total Supply`) as "use".** They are equal to each other at world level,
   by construction (§3.3).
4. **Summing percentage/ratio attributes.** Cotton attr 195 is `(PERCENT)`; `Yield` is `(MT/HA)`/`(KG/HA)`.
   Blocklisting names is fragile — gate on `Unit_Description`.
5. **Mixing units across a "world oilseed" total.** Cotton is `1000 480 lb. Bales`, everything else `1000 MT`,
   area is `1000 HA`. The `psd` table's `unit` column must be part of the aggregation key, never averaged.
6. **Assuming EU/member-state and USSR/Russia rows never overlap.** True for every year I checked (§3.2) and
   the reason the sum works — but it is USDA's editorial choice, not a contract. A pipeline that sums all
   countries should **assert** `sum(countries) == API world row` for the latest MY on each run, or at minimum
   assert no aggregate-plus-member coexistence, and hard-fail rather than publish a double-counted world.
7. **Expecting PSD's world consumption to equal WASDE's printed world use for grains.** It won't, by the
   documented ±(exports − imports) adjustment (§4.2).
8. **Cross-year marketing-year comparability.** A world row is an "aggregate of local marketing years"; the
   Argentina/Brazil October–September re-basing means the world soybean line is not a single calendar window.
   Fine for a level and a trend; **do not** narrate an intra-year world S/U as if it were a synchronised
   snapshot.
9. **Vintage mixing.** The bulk zips carried `month = 07` (files stamped Jul 10) at probe time, i.e. one
   vintage per file; the API returns the same `"month": "07"`. But WASDE-673 also *prints two vintages*
   (Jun and Jul) for the new crop year. Any stored world series is a **latest-vintage-only** series —
   `psd`'s PK `(commodity, country, year, attribute)` has no vintage dimension, so monthly revisions
   overwrite silently and no revision history is recoverable. Same limitation as today, just more visible on
   a headline world number.
10. **Zero vs missing.** Non-reporting countries appear with an explicit `0.0`, so a "we have every country"
    check on row counts will pass while carrying an unknown amount of unmeasured stock. USDA's world total has
    the same property; we inherit it, we don't create it.
11. **The current MY is a forecast, not a measurement.** MY2026 world rows already exist (soybeans:
    beginning 125,325 / production 441,701). A world S/U chart must mark estimate/projection years.

---

## 8. Recommended definitions, arithmetic spelled out (soybeans MY2025, 1000 MT, July 2026 vintage)

**(a) World stocks-to-use** — the headline.

```
ES_world = 125,325        (attr 176, country 00)
DC_world = 429,333        (attr 125)
S/U      = 125,325 / 429,333 = 29.19%
```

**(b) World-less-China stocks-to-use** — reproduces USDA's own `World Less China` line.

```
ES = 125,325 − 44,369 (China attr 176) =  80,956
DC = 429,333 − 133,900 (China attr 125) = 295,433
S/U = 80,956 / 295,433 = 27.40%
```

For corn/wheat, add the adjustment if matching WASDE's printed table:
`DC_adj = DC + Exports − Imports`, e.g. corn world `1,301,298 + 220,662 − 196,725 = 1,325,235`
→ `298,670 / 1,325,235 = 22.54%` (WASDE prints 1,325.24). China's own export/import gap is ~0, so the same
adjustment carries into less-China: `121,522 / 1,004,235 = 12.10%` (WASDE less-China DC 1,004.24).

**(c) Major-exporter stocks-to-use** — a *different* statistic; use with a different label.

```
Exporters (soy, USDA's own list: US, Brazil, Argentina, Paraguay, Uruguay)
ES  = sum of exporter attr 176
Use = sum of exporter (attr 125 + attr 88)   ← exports DO belong here
```
Exports belong in the denominator here because from an exporter-bloc perspective a shipment out of the bloc
*is* an offtake. WASDE prints the bloc as `Major Exporters 3/` (US + `Major Exporters` incl. Uruguay);
2025/26: Major Exporters ES 62.32, domestic total 118.78, exports 134.30 → `62.32 / 253.08 = 24.62%`, plus the
US separately (8.98 / (75.16 + 41.37) = 7.71%). **Caveat (inference):** the exporter bloc's ending stocks are
seasonally shaped by South American harvest timing under the Oct–Sep re-basing, so this series is more
seasonal than the world one; it is a "how tight is exportable supply" gauge, not a comparable to (a).

**Cotton, once attr 142 is stored:** `75,723 / 119,953 = 63.13%` (matches WASDE's printed 63.1%).
Do **not** use the stored attr 195 for the World row.

---

## 9. Confidence register

| Claim | Basis | Confidence |
|---|---|---|
| PSD exposes a World region, `regionCode R00` / `countryCode 00`, via `/api/psd/commodity/{code}/world/year/{y}` | live probe, HTTP 200 payloads quoted | **High — observed** |
| The bulk CSVs contain no World row (`Country_Code == "00"` → 0 rows) | direct file inspection | **High — observed** |
| Sum of all country rows == USDA's World row, every attribute | 197 (commodity, year) pairs, 0 mismatches; 10 commodities cross-section | **High — observed** |
| PSD World row == WASDE-673 world line (oilseeds, all columns) | side-by-side, matches to rounding | **High — observed** |
| WASDE grain world "Domestic Total" = PSD DC + (exports − imports) | exact arithmetic on wheat (+5,071) and corn (+23,937), and USDA footnote 2/ states it | **High — observed + documented** |
| USDA's published world S/U denominator is consumption, not consumption+exports | WASDE-673 cotton text 63.1% reproduced from PSD as ES/DomUse | **High — observed** (one commodity; the only place WASDE prints a *world* ratio I could find) |
| PSD attr 195 World value (45.95%) contradicts USDA's own 63.1% | computed both from the same vintage | **High — observed** |
| USDA publishes `World Less China`, and it is a pure subtraction of the China row | 21 occurrences in WASDE-673; subtraction verified on soybeans + meal (+ grains once adjusted) | **High — observed** |
| USDA's *rationale* for World-Less-China (Chinese stock opacity) and its May-2019 introduction | `usda.gov` 403 to curl/WebFetch; only a web-search summary | **Low — unverified inference; do not cite** |
| The `(Local)` commodity codes (e.g. `0813101`) encode the local-vs-international MY distinction | code name + a differing world payload; no doc found | **Low — I don't know how they are constructed** |
| No richer world/bulk API endpoint exists (all-years, region-code form) | 5 probed shapes all 404 | **Medium — absence of evidence, not exhaustive** |
| Cotton has no consumption attribute stored, so US *and* world cotton S/U are both impossible today | DB query + `config.py:308-312` | **High — observed** |
| Layer 6 has no `trust/` dataset contract | grep across `trust/` returns nothing for `psd` | **High — observed** |
| Palm oil's 28-country S/U matching world to 0.01 pp is coincidence | offsetting coverage gaps (ES 81.4%, DC 81.4%) | **Medium — inference** |
| The recommended change needs no new HTTP request and no schema change | follows from §3 + `pipeline/schema.py:76-84` | **High — inference from observed** |

## 10. Sources

- PSD bulk CSVs (no key): `https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip`,
  `…/psd_grains_pulses_csv.zip`, `…/psd_cotton_csv.zip` — downloaded 2026-08-12, members stamped
  2026-07-10 11:33 (801,669 / grains 51.6 MB / cotton 8.7 MB rows-and-sizes as observed).
  *Note: `apps.fas.usda.gov` refused ~50% of connections during probing (`curl (28)` connect timeouts at
  20–75 s); every fetch succeeded on retry. Layer 6's `MAX_RETRIES` loop is load-bearing.*
- FAS PSD API (`X-Api-Key`, the repo's existing `FAS_API_KEY`): `https://api.fas.usda.gov/api/psd/regions`,
  `/api/psd/countries` (251), `/api/psd/commodities` (63), `/api/psd/commodityAttributes` (85),
  `/api/psd/unitsOfMeasure` (42), `/api/psd/commodity/{code}/world/year/{year}`,
  `/api/psd/commodity/{code}/country/{cc}/year/{year}`.
- **WASDE-673, July 2026** — `https://www.usda.gov/oce/commodity/wasde/wasde0726.pdf` (890,823 bytes;
  requires a browser User-Agent, else 403). Pages used: p.5 cotton narrative, p.18 world wheat, p.20 coarse
  grains, p.22 world corn, p.28 world soybeans, p.29 world soybean meal.
- **Cotton: World Markets and Trade, July 2026** — `https://apps.fas.usda.gov/psdonline/circulars/cotton.pdf`
  (PSD attribute definitions, p.4-5). Also fetched `oilseeds.pdf`, `grain.pdf` (no world S/U or
  world-less-China text found in either).
- Unreachable (recorded as gaps, not used): `https://www.fas.usda.gov/data/production-supply-and-distribution-online-psd-online`
  (403), `https://www.usda.gov/previous-changes-wasde` (403), `https://apps.fas.usda.gov/opendataweb/home`
  (JS SPA, no endpoint reference rendered).
- Repo: `config.py:263-312`, `fetchers/psd.py:117-133`, `pipeline/schema.py:76-84`,
  `pipeline/store.py:394-407`, `pipeline/query.py:121-123`, `analysis/stocks_to_use.py:20-88`,
  `analysis/briefing/sections/stocks_to_use.py:22-31`, `trust/registry.py:838-840`,
  `data/storage/mirror_market.db`.
