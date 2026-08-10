# Is there a free, daily, licence-clean sunflower benchmark?

Research for [#147](https://github.com/philipbergman6-glitch/Mirror-Market/issues/147) (M5, parent #142).
Date: 2026-08-10. Checkout: branch `players-t2-page` @ `865827e`.
Live probes run 2026-08-10; latest published MAGyP circular at probe time = **2033, dated 2026-08-07**.

---

## Verdict — GO, on one leg only

**A daily sunflower line IS achievable, and it is nearly free to build.**

**Argentina's MAGyP "Precios FOB Oficiales" — the exact JSON service Layer 21 already fetches every
day — carries a complete sunflower complex (seed, crude oil, refined oil, meal) in USD/MT that
`fetchers/magyp_fob.py` currently discards.** Adding it is a config-only change to an
already-scheduled, already-licence-cleared fetcher. No new host, no new scraper, no new failure mode.

But the three legs do **not** move at the same cadence, and that decides what can be rendered:

| Leg | Position | 2026-08-07 | Price changes in last 45 sessions | Verdict |
|---|---|---|---|---|
| **Crude sunflower oil, bulk** | `15121110310E` | **1364** | **39 / 44** | **Daily line — GO.** Same cadence as soy oil (41/44) |
| Sunflower seed, industry, bulk | `12060090910Y` | 532 | 4 / 44 | Step function. Level only, not a daily line |
| Sunflower meal, extraction pellets | `23063010310V` | 188 | 1 / 44 | Effectively static. Level only |

So the honest answer is narrower than "sunflower goes daily": **sunflower OIL goes daily; sunflower
seed and meal are obtainable daily but are administered step-functions that will render as flat
lines.** The correct product is a daily **sun-oil vs soy-oil vs palm-oil vegoil spread** off a single
origin (Argentina, up-river) — a relative-value line, which is the repo's stated preference — with
the seed and meal legs stored and shown as levels, not charted as series.

Everything else surveyed loses. **Nothing free, daily and licence-green exists outside Argentina.**
The runner-up (APK-Inform Ukraine) is weekly. The only *traded* sunflower futures curve on earth
(JSE SAFEX SUNS) is licence-**RED**.

### What this source is, and is not

**It is an administered price.** MAGyP publishes an *official minimum FOB export value* under
Ley 21.453 — the base on which Argentina's export duty (derechos de exportación) is levied
(`fetchers/magyp_fob.py:9-16`; the FOB landing page cites "Granos, Aceites y Subproductos Ley 21.453",
https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/fob_oficiales/). It tracks the up-river
market and, for oil, re-marks daily — but it is **not an exchange settlement and not a traded print**.
Any rendered label must carry the word "official". Do not present it as a market benchmark.

The cadence table above is itself evidence for this: an administered price is re-struck only where the
underlying flow justifies it. Argentina crushes essentially all its sunseed domestically and exports
oil, so the oil position is marked daily and the seed/meal positions are not.

---

## 1. Argentina MAGyP official FOB — direct observation

### 1.1 The sunflower positions exist in the circular the fetcher already hits

Endpoint (unchanged from `config.py:614-617`):
`https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=07/08/2026`
→ HTTP 200, `posts` array of 125 rows, 43 distinct NCM positions. Verbatim first record:

```json
{"fecha": "2026-08-07 00:00:00.000", "circular": "2033", "posicion": "10011900110H",
 "precio": 275, "mesDesde": 8, "añoDesde": 2026, "mesHasta": 8, "añoHasta": 2026}
```

Sunflower rows in that same response (23 of the 125 posts), verbatim `posicion` / `precio` /
shipment window:

```
12060090290L   772   8/2026->7/2027
12060090910Y   532   8/2026->7/2027
12060090929W   552   8/2026->7/2027
15121110310E  1364   8/2026->9/2026      (also 1356 for 10/2026, 1343 for 11/2026->7/2027)
15121110911P  1364   ...same three windows, identical prices
15121110919G  1364   ...same
15121919110H  1582   8/2026->9/2026      (1573, 1558 forward)
15121919121N  1692   8/2026->9/2026      (1683, 1668 forward)
23063010100F   190   8/2026->7/2027
23063010200L   186   8/2026->7/2027
23063010310V   188   8/2026->7/2027
23063010320Y   133   8/2026->7/2027
23063090100J   190   8/2026->7/2027
```

All 13 positions are present unchanged on every date sampled: 2026-08-07, 2026-01-02, 2025-07-01,
2024-01-03, 2022-01-04, 2020-01-02, 2018-01-03. Nomenclature is stable over ≥8 years. Dates further
back respond too (2015-01-05 → 117 posts; 2015-01-02 → 0, a holiday). The labelled mirror (§1.2)
declares history from **1993-01-04**.

**Independent corroboration of the seed leg from a second MAGyP page.** The "Resumen Diario de
Cotizaciones" HTML page prints `GIRASOL … FOB Oficial 532,00 / 507,00 (prev) / 502,00 (month-ago)
USD/t` and `ACEITE DE GIRASOL CRUDO … FOB 1.364,00 USD/t` — byte-matching the JSON above and the
prior session. (The cross-finding relayed from the #149 agent quoted 507 for the seed; that is the
**2026-08-06** value, not 2026-08-07. Both are correct for their date; I verified 532 on the 7th.)

### 1.2 Position → product mapping, cross-verified against a labelled independent source

Two independent checks, both passed.

**Check A — the official NCM nomenclator for Ley 21.453** (PDF at
`https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/djve/_archivos/000001_NCM-LEY-21.453.pdf`,
extracted with `pdftotext -layout`), verbatim rows:

```
1206-00-90   Semilla de Girasol. Únicamente para industria, Los Demás, A granel con hasta un 15 %
1206-00-90   Semilla de Girasol. Únicamente para industria, Los Demás, Más del 15 % embolsado
1206-00-90   Semilla de Girasol. Los Demás, Girasol, tipo confitería
1512-11-10   Aceite de Girasol, a granel
1512-19-19   Aceite de Girasol, Los Demás, Refinado a granel
1512-19-19   Aceite de Girasol, Los Demás, Refinado en tambores de mas de 200 litros
2306-30-10   Tortas, "Expellers", "Pellets" y Harinas de girasol, Tortas
2306-30-10   Tortas, "Expellers", "Pellets" y Harinas de girasol, "Pellets", de harina de extracción
2306-30-10   Tortas, "Expellers", "Pellets" y Harinas de girasol, "Pellets", integral
2306-30-10   Tortas, "Expellers", "Pellets" y Harinas de girasol, Harinas de tortas
2306-30-90   Tortas, "Expellers", "Pellets" y Harinas de girasol, "Expellers"
2306-30-90   Tortas, "Expellers", "Pellets" y Harinas de girasol, "Expellers", integral
```

**Check B — numeric match against the labelled `datos.gob.ar` series** (dataset 358 "Precios FOB
oficiales", the same cross-reference CLAUDE.md records was used for the soy positions). On
**2025-01-21**, the labelled API and the raw web service return identical values:

| datos.gob.ar series id | Verbatim `description` | Value 2025-01-21 | WS position | WS value |
|---|---|---|---|---|
| `358.1_SEMILLA_GIBOL__56` | Semilla de Girasol. Unicamente para industria, Los Demás, A granel con hasta un 15 % embolsado | 445.0 | `12060090910Y` | 445 |
| `358.1_SEMILLA_GIADO__52` | …Más del 15 % embolsado | 465.0 | `12060090929W` | 465 |
| `358.1_SEMILLA_GIRIA__55` | …Los Demás, Girasol, tipo confitería | 690.0 | `12060090290L` | 690 |
| `358.1_ACEITE_GIRNEL__21` | Aceite de Girasol, a granel | 1080.0 | `15121110310E` (and `911P`, `919G`) | 1080 |
| `358.1_ACEITE_GIRNEL__36` | Aceite de Girasol, Los Demás, Refinado a granel | 1252.8 | `15121919110H` | 1253 |
| `358.1_ACEITE_GIRROS__53` | …Refinado en tambores de mas de 200 litros | 1600.0 | `15121919121N` | 1600 |
| `358.1_TORTAS_EXPTAS__47` | Tortas, "Expellers", "Pellets" y Harinas de girasol, Tortas | 225.0 | `23063010100F` | 225 |
| `358.1_TORTAS_EXPTAS__55` | …Harinas de tortas | 221.0 | `23063010200L` | 221 |
| `358.1_TORTAS_EXPXTR__53` | …"Pellets", de harina de extracción | 223.0 | `23063010310V` | 223 |
| `358.1_TORTAS_EXPRAL__57` | …"Pellets", integral | 165.0 | `23063010320Y` | 165 |

Every mapping is **directly observed**, not inferred. Units confirmed verbatim by the series
metadata: `"units": "Dolares por tonelada"` — **USD/MT as published, no conversion needed**, same as
the existing soy positions.

**This cross-check materially changed the answer, which is why it was worth doing.** My own first
pass inferred the meal suffixes in numeric order (`…100F` = Tortas, `…200L` = pellets extracción,
`…310V` = pellets integral, `…320Y` = harinas). Three of those four were **wrong**. The labelled
series shows `…200L` = *harinas de tortas* and `…320Y` = *pellets integral*. Wiring the meal leg by
suffix intuition would have silently poisoned a rendered line.

**Two consequences for whoever implements this:**

1. **The recommended meal leg is `23063010310V` ("Pellets", de harina de extracción, 188), not
   `23063010100F` ("Tortas", 190).** The cross-finding relayed from the #149 agent proposed `100F`.
   `Tortas` is expeller/press-cake; the extraction-meal *pellet* is the direct analogue of the soy
   position this repo already uses (`23040010100B` = "pellets de soja") and is what actually moves in
   the export trade. The two differ by ~1% today so nothing dramatic turns on it, but the label
   would be wrong.
2. **`12060090290L` (772) is confectionery sunflower, not the crush seed.** It is ~$240/MT above the
   industrial bulk seed and belongs to a different market entirely. Using it as "the" seed leg would
   be a serious error.

Recommended additions to `MAGYP_FOB_POSITIONS`:

```
"12060090910Y": "Sunflower Seed",       # semilla de girasol, industria, a granel
"15121110310E": "Sunflower Oil",        # aceite de girasol en bruto — granel
"23063010310V": "Sunflower Meal",       # pellets de harina de extracción
```

Note `15121110911P` and `15121110919G` carry an **identical** price to `310E` on every date sampled
(1364 on 2026-08-07; 1080 on 2025-01-21). Only one should be stored; storing all three would
triple-count the same official value.

### 1.3 Cadence — measured, not assumed

45 published sessions walked back from 2026-08-07 (span 2026-06-03 → 2026-08-07), first shipment
window per position:

```
SunSeed bulk           n=45  distinct=5   changes=4    range 482-532
SunOil crude bulk      n=45  distinct=29  changes=39   range 1299-1364
SunMeal pellets extr   n=45  distinct=2   changes=1    range 188-194
Soybeans  (baseline)   n=45  distinct=28  changes=36   range 410-475
SoyOil    (baseline)   n=45  distinct=31  changes=41   range 1148-1217
```

Crude sunflower oil is re-marked at the **same rate as soy oil**. Seed and meal are not. This is the
single most decision-relevant fact in the report and it is directly observed.

### 1.4 Licence read — **GREEN**

The identical data is republished on Argentina's national open-data portal with an explicit licence.
`https://apis.datos.gob.ar/series/api/series?ids=358.1_ACEITE_GIRNEL__21&metadata=full` returns:

```
dataset_title:     "Precios FOB oficiales"
dataset_source:    "Secretaría de Agricultura, Ganadería y Pesca, Ministerio de Economía"
dataset_publisher: "Subsecretaría de Programación Macroeconómica"
license:           "Creative Commons Attribution 4.0"
```

CC-BY 4.0, commercial use permitted, attribution required. The API doc page
(`.../fob_oficiales/_archivos/000021_Precios%20Fob%20Api.php`) states no rate limit and no
attribution requirement of its own; it documents only `Fecha` (dd/mm/aaaa) and warns *"No se calculan
Precios Fob para los días, sabados, domingos y/o feriados"*. **Caveat (inference):** the CC-BY 4.0
grant is observed on the datos.gob.ar mirror of this dataset, not on the magyp.gob.ar WS page itself,
which carries no notice at all. Given they are the same dataset from the same publisher this is a
sound read — and per the ticket context, this source is already in production as Layer 21 and was
already treated as clean, so **the licensing question here is settled, not open**.

**The datos.gob.ar mirror is stale** — newest value 2025-01-21, verified by `sort=desc`. Use it for
labels and for a one-off 1993→2025 backfill
(`https://infra.datos.gob.ar/catalog/sspm/dataset/358/distribution/358.1/download/precios-fob-oficiales-valores-diarios.csv`);
use the live WS going forward. The two agree exactly on overlapping dates, so splicing is safe.

### 1.5 On crush yields — the relayed 0.42/0.40 figures are unsourced; here are sourced ones

The #149 agent's sunflower crush yields (0.42 seed→oil, 0.40 seed→meal) are **its own assumption and
are not sourced** — recording that explicitly as requested. Sourced replacements, computed directly
from the USDA PSD oilseeds bulk CSV already downloaded by Layer 6
(`https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip`, Production ÷ Crush,
codes 2224000 / 4236000 / 813500):

| Origin | MY2026 oil yield | MY2026 meal yield | Stability |
|---|---|---|---|
| **Argentina** | **0.438** | **0.441** | 0.435–0.445 / 0.440–0.448 across MY2021-26 |
| Ukraine | 0.430 | 0.413 | flat |
| Russia | 0.413 | 0.411 | flat |
| European Union | 0.423 | **0.541** | flat |

For an Argentina-origin margin use **oil 0.438 / meal 0.441**. The relayed 0.42 oil is ~2 pp low and
the 0.40 meal is ~4 pp low. Note the EU meal yield (0.541) diverges sharply — that is
undecorticated meal versus Argentina's dehulled product, which is exactly why the meal yield must be
paired with the specific NCM position chosen (§1.2). Treat any sunflower crush margin as
**illustrative**, not tradeable: the seed and meal legs are step-functions (§1.3), so a computed
sun-crush would be an oil series with two constants subtracted.

### 1.6 What is NOT worth adding from Argentina

- **Matba Rofex `GIR.ROS.P/DIS26`** — the open, keyless API `https://apicem.matbarofex.com.ar/api/v2/`
  does carry a girasol product, but the whole 2026 YTD pull (40,846 rows) yields 145 sunflower rows
  with **volume 0, openInterest 0, tradeCount 0 on every one**. It is a settlement-marked ARS
  disponible quote — the CPO=F pathology. Also: the `product=`/`symbol=` filters silently return
  empty, so a naive integration would look like a clean no-data result. Licence **GRAY** (terms page
  404, `/estadisticas` 403). **No.**
- **Cámara Arbitral de Cereales pizarra** (`https://www.cac.bcr.com.ar/es/precios-de-pizarra`) — the
  girasol row on 06/08/2026 reads `Girasol | S/C | (E) $745.250,00 | US$ (E) 500,00`, i.e.
  *sin cotización* with an estimative value only. Current-day HTML scrape, no history, footer
  "Todos los derechos reservados" → **RED/GRAY**. **No.**
- **BCR `/mercado-fisico-de-rosario/precios-2026`** — returns 200 but the content is frozen at
  2017/2018 despite the `2026` slug. Actively misleading. **No.**
- **Bolsa de Cereales de Buenos Aires** — HTTP 403 Cloudflare challenge to both WebFetch and curl
  with a browser UA. Same wall class as cepea.org.br. **Unverified, blocked.**

---

## 2. Everything else — what exists and why it lost

### 2.1 Ukraine — APK-Inform is the best non-Argentine source, but it is WEEKLY

`https://www.apk-inform.com/en/prices` (200, no login). An undocumented JSON endpoint discovered in
the page's own `<form action="/en/prices/get-prices">` works anonymously. Verified live response:

```json
{"chartColumns":["Sunflower oil, crude (offer, FOB), USD, Ukraine",
                 "Sunflower oil, crude (bid, FOB), USD, Ukraine"],
 "chartData":{"2019-01-04":[640,630], ... ,"2026-08-07":[1390,1380]}}
```

- **Coverage is excellent**: crude sunoil offer/bid FOB USD (ids 109/113), sunoil bid CPT-port (112),
  sunseed offer FOB (135), sunseed CPT-port (471), sunmeal offer/bid DAP (136/2157), plus UAH
  domestic EXW legs. All three derivative legs present.
- **History: 1370 weekly points back to 2000-01-07.** Continuous through the invasion —
  `2022-02-18: 1445`, `2022-02-25: 1540`, `2022-03-18: 2220`, unbroken to 2026-08-07.
- **Frequency: WEEKLY.** Every timestamp is a Friday. APK-Inform states verbatim that the daily range
  is *"published in the daily issues 'Novosti Agrorynka' and 'AGROden' … and can also be provided
  upon additional request"* — i.e. paid.
- Hard-fail note if ever implemented: `monthlyCommodities` is a **mandatory** parameter even for a
  weekly query; omitting it returns `400 {"monthlyCommodities":"This value should not be blank."}`.
- **LICENCE: GRAY-GREEN.** Footer verbatim: *"© 2001-2026 APK-Inform Information Agency … Any copying,
  full or partial reproduction of the site materials is possible only with indication of hyperlink to
  the source."* That is an attribution-conditional grant, and `robots.txt` disallows only
  `*/admin`, `*/search?`, `*/register`. But there is **no ToS addressing automated access to an
  undocumented endpoint** — recommend written permission before production use.

**Why it loses:** weekly beats monthly, but the ticket asks for daily, and Argentina delivers daily
on the oil leg for zero new infrastructure. Keep APK-Inform on file as the **second origin** if the
map ever needs Black Sea sunflower — it is the only credible one.

**Ukrainian state sources all fail.**
- **Agrarian Exchange** (`agrex.gov.ua/monitoring/grain/index.php`) — parsed the inline amCharts
  series: only `Пшениця-2кл/3кл/4кл` and `Кукурудза-3кл`. **No sunflower at all**, and the data
  **stops 2023-10-03** (verified on two oblasts independently) — war-related discontinuation.
- **data.gov.ua** CKAN — `q=соняшник` returns **count 2**, both non-price (production, sown area).
- **ukrstat.gov.ua** — annual/monthly farmgate only, running ~1 year late (1996–2024 series published
  2026-04-17).
- **Minagro МДЕЦ minimum export prices** — free and legally mandated (KMU res. 944/2024) but
  **monthly** and deliberately administered: Aug 2026 applies a 0.714 coefficient after Black Sea
  shelling. Not a market quote. Individual August 2026 order URL **unverified**.
- **customs.gov.ua** — HTTP 403, **unverified**.
- Structural caveat beyond any single source: quoting has migrated from FOB Black Sea toward
  Danube/CPT, so a "FOB Ukraine" series is not economically continuous even where the numbers are.

### 2.2 Exchange-listed sunflower — exactly one exists, and it is licence-RED

**JSE / SAFEX South Africa `SUNS` is the only traded sunflower futures contract on earth with a free
daily file.** And this repo **already ingests it** (Layer 18, `fetchers/safex.py:54`,
`config.py:636` → `"Sunflower (SAFEX)"`).

- Liquid, not nominal: JSE daily MTM file (200, .xls,
  `https://www.jse.co.za/_layouts/15/DownloadHandler.ashx?FileName=/Safex/amdmtm/NEW%20DAYAGR.xls`),
  header "DOMESTIC FUTURES PRICES 07-Aug-2026" — SUNFLOWER SEEDS FUTURE Dec-26 MTM 10380, volume 240,
  **open interest 6114**. Repo DB corroborates: `2026-08-07, Sunflower (SAFEX), 10210.0, 283.0, ZAR/MT`.
  Dated archive free back to **2009** (`https://www.jse.co.za/downloadable-files?RequestNode=/Safex/agriculture.stats/2026`).
- Units ZAR/MT; **seed only** — JSE lists no local sun oil or sun meal contract.
- **LICENCE: RED.** JSE verbatim: *"You may not copy, reproduce, modify, reformat, download, store,
  distribute, publish or transmit any data and information, except for your personal use… you may not
  develop or create any product that uses, is based on, or is developed in connection with any of the
  data… You are not permitted… to use the data and information for commercial gain."* The Grain SA
  mirror this repo scrapes is **GRAY** (copyright footer only, no reuse terms) but the underlying data
  is still JSE's. This is a pre-existing exposure on #71, not something #147 creates.

> **Live incident, flagged in passing (out of scope for #147):** `fetch_safex()` run against the live
> Grain SA page on 2026-08-10 returns **`failed — Grain SA SAFEX: no <table> elements on page`**. The
> page returns HTTP 200 / 28 KB and still credits BVG, but the settlement table is absent from the
> HTML; no JSON/iframe replacement found. Last good DB row is 2026-08-07. May be transient. Worth its
> own ticket.

**No sunflower contract exists at all** on Euronext/MATIF (oilseed = rapeseed ECO only), MOEX (free
ISS API enumerated 470 futures; agri = SUGAR + WHEAT only), Bursa Malaysia (palm only), DCE, or CZCE.
**CME's Black Sea Sunflower Oil** contract (USD/t, 25 t) is **suspended** (SER-9233, Aug 2023) —
*unverified against cmegroup.com directly, which timed out*. **NCDEX SUNOIL** is listed
(`https://www.ncdex.com/products/SUNOIL`) but effectively untraded; SEBI's suspension extension is
real (PR 21/2026, 2026-03-27) but sunflower oil is not on the suspended list — it is simply illiquid.
Borsa Istanbul **unverified** (site timed out).

### 2.3 EU — free, green-licenced, all three legs, but weekly and methodologically messy

The Commission's agri-food data REST API is keyless and works:

```
GET https://api.tech.ec.europa.eu/agrifood/api/oilseeds/products
→ ["Crude rape oil","Crude soya bean oil","Crude sunflower oil","Rapeseed","Rapeseed meal",
   "Soya beans","Soya meal","Sunflower seed","Sunflower seed meal"]

GET .../oilseeds/prices?products=Sunflower%20seed&beginDate=01/06/2026&endDate=10/08/2026
→ {"memberStateCode":"BG","beginDate":"27/07/2026","price":"€486.00",
   "unit":"national currency/ton","weekNumber":5,"product":"Sunflower seed",
   "productType":"Standard","marketStage":"FGATE","market":"National Average",
   "marketingYear":"2026/2027"}
```

- **Weekly**, per-Member-State, **no EU aggregate**. History from **2020-12-28** (2019/20 queries 404).
  Full pull: seed 4,238 rows, oil 1,484, meal 2,241.
- **Two traps.** (1) `unit` claims "national currency/ton" but `price` is a **€-prefixed string even
  for Hungary** — values look EUR-converted and the label is wrong; must strip `€`. (2) `marketStage`
  is heterogeneous (FGATE / DEPSILO / DEPPROC / DELFIRST / FOB / **Not Defined**) — this is a bag of
  national quotes at mixed value-chain stages, **not one comparable benchmark**. Continuous series
  only for ES, HU, RO, IT-Milano, NL, BG.
- **LICENCE: GREEN.** `https://commission.europa.eu/legal-notice_en` verbatim: *"content owned by the
  EU on this website is licensed under the Creative Commons Attribution 4.0 International (CC BY 4.0)
  licence… reuse is allowed, provided appropriate credit is given and changes are indicated."*
  **Caveat (inference):** there is no agridata-specific legal notice (`legalNotice.html` and variants
  all 404), so applying the Commission-wide grant to this portal is an inference.
- **Hamburg/Rotterdam CIF sunmeal is not free.** agridata exposes `markets=Hamburg` and stage
  `CIF GEX`, but `Sunflower seed meal` + `Hamburg` returns **exactly one row in all history**
  (30/01/2023, €560.00). Not a series. Oil World (ISTA Mielke), Fastmarkets (which absorbed
  AgriCensus), Argus, Platts and LSEG are all **paid, enterprise-licence, no free tier** — stated
  plainly rather than proposed as a scrape.
- Eurostat has sunflower **seed only, annual**, per 100 kg (`apri_ap_crpouta`, item `02120000`).
  Weaker than agridata.

**Why it loses:** weekly, no single benchmark, and it would render as a fan of national lines rather
than one price. Genuinely useful later as a *demand-side* EU cash panel, not as the benchmark.

### 2.4 India — no green option is usable, and no usable option is green

- **Agmarknet via the data.gov.in API this repo already uses** (Layer 16 endpoint
  `9ef84268-d588-465a-a308-a864a43d0070`): querying `filters[commodity]=Sunflower` returns
  **`"total": 5`** — five records, Karnataka only (Lingasugur APMC), commodity `Sunflower/Sunflower
  Seed`, modal 8250 Rs/quintal. **Seed only, no oil, five mandis, no backfill.** Run twice
  independently with byte-identical output. Licence GREEN (GODL-India: *"worldwide, royalty-free,
  non-exclusive license to use, adapt, publish … for all lawful commercial and non-commercial
  purposes"*) — and useless. *(My own re-run from this session hit read timeouts / non-JSON responses
  from api.data.gov.in and could not independently reproduce it; I am relying on two agent runs.)*
- **DoCA Price Monitoring System** (`https://fcainfoweb.nic.in/`) — genuinely good: daily
  **Sunflower Oil (Packed)** across 579 centres, retail and wholesale. But `report_menu_web.aspx`
  carries a **mandatory captcha**, and POST emulation with valid `__VIEWSTATE` + cookies returned
  HTTP 404 `"The specified URL is inaccessible at this time"` — an NIC WAF block. Internal API
  `PMSAPI/api/values` → 401. Units and archive depth **could not be verified**. Licence **GRAY**:
  *"Material featured on this website may be reproduced free of charge after taking proper permission
  by sending a mail to us."* Permission-gated.
- **data.gov.in sunflower-oil price mirrors are dead** — catalog
  `dailyweekly-retail-prices-sunflower-oil-packed` shows "Updated On: 21/09/2015" with zero
  API-enabled resources; the one queryable resource is frozen at 2022, 33 rows.
- **SEA of India** — weekly `Sunflower Oil (Crude) CIF Mumbai` US$/MT plus monthly country-wise import
  volumes. Licence **RED/GRAY** *and* **robots-disallowed** — the robots block is the harder stop.
- **TRADESTAT (DGCI&S)** — free, no key, no captcha, works: HS `15121110` crude sunflower oil,
  May-2026 = 338,473,270 **KGS** (vs 167,356,987 May-2025). Monthly, ~2–3 month lag, Laravel POST with
  a scraped CSRF `_token`. Licence **GRAY**: *"The data refrenced in the system do not have any legal
  sanctity and is for general refrence only."* This is a **volume** series, not a price. Warning on
  the site: ITC HS codes may be re-allocated and units changed **from April 2026** — expect a break.
- **NCDEX polled spot `UndrlygPric`** (INR/MT ex-tank Chennai, daily, 2024-07 onward) is the only live
  daily Indian oil price; licence **RED/GRAY**, and NCDEX's own terms page was unreachable so the read
  is inferred from a footer copyright line. **Unverified.**

**Why it loses:** India is a *demand* signal, not a benchmark, and every leg that would work is
licence-blocked. Consistent with the M4/#149 conclusion that India's cross-market line is a
premium-vs-CBOT comparison, not a domestic complex.

---

## 3. Adjacent cheap win found while investigating: PSD sunflower fundamentals

Not asked for, but it costs two config lines and it is the same pattern X1 (#131) used for rapeseed.
The sunflower rows are **already inside the oilseeds ZIP Layer 6 downloads every run** — just not
whitelisted. Verified by downloading and filtering it:

```
Commodity_Code  Commodity_Description
        813500    Meal, Sunflowerseed
       4236000     Oil, Sunflowerseed
       2224000 Oilseed, Sunflowerseed
years 1964 - 2026
```

MY2026 sunseed production, top origins (1000 MT): Russia 20,700 · Ukraine 13,000 · EU 9,800 ·
Argentina 8,000 · Kazakhstan 2,500 · China 2,230 · Turkey 1,675 · US 1,050.

Caveat: **Russia, Ukraine, Kazakhstan and Turkey are absent from `PSD_TARGET_COUNTRIES`**
(`config.py:244-259`), so whitelisting the three commodity codes alone would capture Argentina, the
EU, China and the US but miss the world's two largest producers. Adding Russia/Ukraine was
**explicitly deferred** in #130 ("Deferred explicitly: … Ukraine/Russia PSD"), so this is a decision
to re-open, not a free lunch.

---

## 4. Recommendation

**Ship the Argentine sunflower oil line. Skip everything else.**

1. **Add three positions to `MAGYP_FOB_POSITIONS`** — `15121110310E` (oil), `12060090910Y` (seed),
   `23063010310V` (meal). Zero new fetch cost; the rows are already in the response and are being
   thrown away. Note this touches `config.py`, which #130 already flags as an X1/X2 conflict point.
2. **Render one line: crude sunflower oil vs soybean oil, both Argentina official FOB, USD/MT.**
   Both legs come from the same circular on the same day at the same basis — no FX, no unit
   conversion, no cross-origin apples-to-oranges. That is a clean vegoil substitution spread and it is
   the only genuinely daily sunflower thing available for free anywhere. Palm can join it from Layer 1
   `CPO=F` with the usual settlement-marked caveat.
3. **Show seed and meal as levels, not lines** (§1.3). Do not chart a step function, and do not
   compute a headline sunflower crush margin from them (§1.5).
4. **Label it "Argentina official FOB (girasol)"** everywhere. It is administered, not traded (§0).
5. **Do not** build the Ukrainian, Indian or EU scrapes for this ticket. If a second origin is ever
   needed, it is APK-Inform, weekly, with written permission first.
6. **Sunflower stays monthly-only in one respect and that is fine:** World Bank Pink Sheet sunflower
   oil (Layer 8) remains the long-history global level. The comment in `fetchers/worldbank.py:43-46`
   — *"no free daily futures feed exists for Matif rapeseed or sunflower oil"* — stays literally true
   (there is no futures feed) but should be amended to note that a free daily *official FOB* does.

### Unverified / open items

- CME Black Sea Sunflower Oil suspension — read from SER-9233 via secondary route; cmegroup.com timed out.
- Matba Rofex terms of use — `/estadisticas` 403, `/terminos-y-condiciones` 404.
- Bolsa de Cereales de Buenos Aires — 403 Cloudflare, cannot confirm or deny free girasol quotes.
- Borsa Istanbul sunflower listing — site timed out.
- Whether APK-Inform considers programmatic use of `/en/prices/get-prices` within its footer grant.
- agridata portal-specific legal notice — 404; CC-BY 4.0 applied by inference from the Commission-wide policy.
- India Agmarknet sunflower result — reproduced by two agent runs, not by me (api.data.gov.in timed out from this session).
- MAGyP WS earliest available date — confirmed working at 2015-01-05; datos.gob.ar declares the series from 1993-01-04, not probed directly on the WS.
