# Which markets can show a genuine local crush margin, and on what legs?

Research for [#149](https://github.com/philipbergman6-glitch/Mirror-Market/issues/149) (M7, parent #142).
Date: 2026-08-10. Checkout: branch `players-t2-page` @ `865827e`; DB `data/storage/mirror_market.db`
(last pipeline write 2026-08-08, prices through 2026-08-07).

Note on baseline: the DCE board crush from X2 (#132) **is merged on `main`** (`92c0208`) but is **not on this
branch** — `players-t2-page` predates it. All DCE code references below are to `92c0208:analysis/spreads.py`
and `92c0208:analysis/briefing/sections/dce.py`, not to the working tree.

---

## 1. Verdict table

| Market / complex | Verdict | Board or physical | Legs (seed / oil / meal) | Grounding |
|---|---|---|---|---|
| **CBOT (US)** | **Computable — shipped** | Board | `prices` × 3 | `analysis/spreads.py:22` `compute_crush_spread`; DB `prices` has Soybeans/Soybean Oil/Soybean Meal through 2026-08-07 |
| **DCE (China)** | **Computable — shipped, but wrong bean leg** | Board | `dce_futures` A0/M0/Y0 | `92c0208:analysis/spreads.py:89` `compute_dce_crush_margin`; **A0 = No.1 non-GMO food bean.** Correct leg is **B0 (No.2, imported/GMO)** — not in `DCE_CONTRACTS` (`config.py:70-82`). Evidence in §3.2 |
| **Argentina — soy** | **Computable now, zero new sources** | Physical (official FOB) | `argentina_fob` × 3 | `config.py:618-622`; DB rows for all three NCM positions on 2026-08-06 (§3.3). Needs a compute function + history depth |
| **Argentina — sunflower** | **Computable with 3 named config lines** | Physical (official FOB) | Same already-fetched MAGyP circular, **unmapped positions** | Live circular 2026-08-06 carries `12060090910Y` (seed 507), `15121110310E` (crude oil 1364), `23063010100F` (meal 190) — dropped by `fetchers/magyp_fob.py:100-103` because they are not in `MAGYP_FOB_POSITIONS` |
| **Brazil (Paranaguá)** | **Computable with one named scrape**: Notícias Agrícolas Paranaguá **premium** trio | Physical (FOB basis over CBOT) | none in DB today; all three live on the host already scraped by Layer 17 | `brazil_spot_prices` is bean-only (§3.5); NA `/cotacoes/soja` publishes Prêmio Soja + Prêmio farelo + Prêmio óleo Paranaguá, all dated 07/08/2026 |
| **South Africa** | **Not computable** (local margin) | — | `safex_prices` has **seed only**: `Soybean (SAFEX)`, `Sunflower (SAFEX)` | `config.py:634-637`; `fetchers/safex.py:53-54`. JSE lists no local oil/meal contract; its soy meal/oil products are cash-settled **CBOT** references in ZAR — that is the CBOT crush in rand, not a South African margin |
| **Nigeria** | **Not computable** | — | **no price series of any kind** | Nigeria appears only as weather grid points (`config.py:212-213`), FX `NGN/USD` (`config.py:288`) and PSD (`config.py:255`) |
| **India** | **Not computable** (correctly retired) | — | bean only, and `india_domestic_prices` is **empty (0 rows)** in this DB | `config.py:530-545`; `select count(*) from india_domestic_prices` → `0` |
| **Rapeseed — China (CZCE)** | **Computable with named leg: a liquid seed price** | Board (impaired) | oil `OI0` ✅, meal `RM0` ✅, seed `RS0` exists but **dead** | `config.py:319-320`; live probe: `RS0` 2026-08-10 volume **5**, open interest **43** vs `RM0` volume 213,699 (§3.6) |
| **Rapeseed — Euronext** | **Not computable** (no feed) | Board | ECO / RSO / RSM all *exist as contracts*; **no free feed, no rows in any table** | `config.py:315-318` comment: "Matif ECO has no free feed"; RSO/RSM liquidity **not verified** |
| **Sunflower — South Africa** | **Not computable** | — | SUNS seed only | as above |
| **Sunflower — Argentina** | see Argentina sunflower row | | | |

Monthly-only sources are excluded by the ticket's "daily frequency" bar: `worldbank_prices` has Soybeans /
Soybean Oil / Soybean Meal / Rapeseed Oil / Sunflower Oil but is **monthly** (`min 1960-01-01, max 2026-07-01`)
and has **no oilseed-meal leg for rape or sun** — no crush margin is constructible from it.

---

## 2. Board vs physical — which each market yields

Directly observed distinction, to be preserved in any UI:

- **Board crush** = exchange futures, all three legs on the same exchange, one deliverable spec.
  CBOT and DCE only. It is a *hedgeable* margin: a processor can put it on.
- **Physical / cash margin** = cash or administered quotes for a named delivery point.
  Argentina (MAGyP official FOB, up-river) and Brazil (Paranaguá premium-over-CBOT) only.
  It is a *replacement-cost* margin: it says what an exporter's product mix is worth at the port,
  and it embeds export tax, freight and origin-differential effects that a board crush cannot show.

Two further caveats, stated as **inference not observation**:

- Argentina's MAGyP number is an **official minimum FOB export value** used as the export-duty base
  (`fetchers/magyp_fob.py:9-16`). It tracks the market and moves daily, but it is *administered*, so it
  can be stickier than the traded Rosario FAS/FOB market. Labelling it "Argentina crush margin" without
  the word "official" would overstate it.
- Brazil's premium trio is FOB Paranaguá; a true Brazilian *crusher* margin would be interior bean cost
  (CEPEA/IMEA) against interior meal/oil, not port values. The port version is an **export-arbitrage
  margin**, which is the more useful one for a physical-buyer client, but it is not the crusher's P&L.

---

## 3. Per-market detail

### 3.1 CBOT — shipped, board

`analysis/spreads.py:63-66`:

```
oil_value = combined["oil_close"] * CRUSH_OIL_FACTOR      # 11.0
meal_value = combined["meal_close"] * CRUSH_MEAL_FACTOR   # 2.2
combined["crush_spread"] = oil_value + meal_value - combined["soybeans_close"]
```

`config.py:666-667`: `CRUSH_OIL_FACTOR = 11.0`, `CRUSH_MEAL_FACTOR = 2.2`.
Display conversion is `to_metric_tons(latest_cents, "Soybeans")` (`92c0208:analysis/briefing/sections/crush.py:54`).

Observed 2026-08-07 (`prices`): Soybeans 1181.25 c/bu, Soybean Oil 67.45 c/lb, Soybean Meal 316.60 $/st.
→ 741.95 + 696.52 − 1181.25 = **257.22 c/bu = 94.51 USD/MT**.

Worth recording: the cents/bu formulation and the metric-native formulation agree exactly.
Metric-native, using the same mass balance —
oil 67.45 × 22.0462 = 1487.0, meal 316.60 / 0.907185 = 349.0, bean 1181.25 × 0.367437 = 434.0 →
1487.0 × 0.18333 + 349.0 × 0.73333 − 434.0 = **94.5 USD/MT**. Same number. So one shared metric-native
engine can serve every market; no market needs its own algebra, only its own yields and its own FX.

### 3.2 DCE — shipped, but the bean leg is the wrong bean

`92c0208:analysis/spreads.py:28-34`:

```
CRUSH_OIL_YIELD_MT  = CRUSH_OIL_FACTOR / 60.0          # ≈0.1833 MT oil/MT beans
CRUSH_MEAL_YIELD_MT = (CRUSH_MEAL_FACTOR * 20.0) / 60.0 # ≈0.7333 MT meal/MT beans
_DCE_BEAN = "DCE Soybean"   # A0
```

Observed `dce_futures` 2026-08-07: DCE Soybean (A0) 4875, DCE Soybean Oil 8443, DCE Soybean Meal 3154 CNY/MT.
→ 8443 × 0.18333 + 3154 × 0.73333 − 4875 = **−1014 CNY/MT ≈ −$150.5/MT** at CNY/USD 0.148436 (`currencies`, 2026-08-07).

A −$150/MT sustained crush margin is not a market state; it is a leg mismatch. **A0 is Soybean No.1 —
domestic non-GMO food-grade beans**, which trade at a large premium to the imported beans crushers actually
run. Live probe of the same Sina feed already used by `fetchers/akshare.py:47`:

```
B0  4458 rows  2026-08-07  close 3820.0  volume 127,214  open interest 114,889
A0  (DB)       2026-08-07  close 4875.0  volume 318,430
RS0 2702 rows  2026-08-10  close 5903.0  volume 5        open interest 43
```

Substituting **B0 (Soybean No.2 — imported/GMO, the crush bean)**:
8443 × 0.18333 + 3154 × 0.73333 − 3820 = **+40.8 CNY/MT ≈ +$6.1/MT** — an economically plausible Chinese
board crush. **Inference (high confidence):** the shipped DCE crush should key on B0, or at minimum print
both. B0 is one line in `DCE_CONTRACTS` (`config.py:70-82`) plus one constant in `spreads.py`.
Caveat carried over from the merged docstring (`92c0208:analysis/spreads.py:98-102`): the DCE legs are
continuous main-contract series that need not roll on the same day.

### 3.3 Argentina soy — computable now, physical

`config.py:618-622`:

```
MAGYP_FOB_POSITIONS = {
    "12019000190C": "Soybeans",      # habas de soja, las demás — granel
    "15071000100Q": "Soybean Oil",   # aceite de soja en bruto — granel
    "23040010100B": "Soybean Meal",  # pellets de soja
}
```

CLAUDE.md's claim is **confirmed** against both the code and the stored data. `argentina_fob` rows,
2026-08-06 (nearest shipment window, `ship_from = 2026-08`), all in USD/MT as published — no conversion:

```
Soybeans     12019000190C  2026-08  451.0
Soybean Oil  15071000100Q  2026-08  1191.0
Soybean Meal 23040010100B  2026-08  351.0
```

Formula (metric-native, no unit conversion — the source is already USD/MT):

```
crush_usd_mt = oil_usd_mt × 0.1833 + meal_usd_mt × 0.7333 − bean_usd_mt
             = 1191 × 0.18333 + 351 × 0.73333 − 451
             = 218.35 + 257.40 − 451 = +24.75 USD/MT
```

Two implementation notes, both **directly observed**:

- **Shipment-window alignment is mandatory.** The three legs publish different curve depths on the same
  day: beans 4 windows, meal 4, oil 6 (`select product, count(*) ... group by`). A margin must inner-join
  on `ship_from`, not take `iloc[0]` per product. A forward crush curve across the common windows
  (2026-08, 09, 10) is a free bonus and is genuinely differentiating.
- **History depth is the open risk.** `argentina_fob` holds **exactly one date** (2026-08-06, 28 rows in
  `data/history/argentina_fob.csv` including header). `fetch_magyp_fob()` (`fetchers/magyp_fob.py:139-160`)
  walks *back* only to find the latest published circular and returns the first hit — it never backfills.
  `config.py:606` says the `?Fecha=` parameter "also serves historical dates (backfill-capable)", which
  the live probe is consistent with but which is **not verified for depth**. Without a backfill script the
  Argentina margin is a single point, not a series.

### 3.4 Argentina sunflower — three config lines, already being downloaded and thrown away

Live probe, `GET .../precios_fob.php?Fecha=06/08/2026` → 122 posts, of which
`_parse_posts` (`fetchers/magyp_fob.py:100-103`) keeps 14 and discards 108:

```
if product is None:
    continue
```

Among the discarded, a complete sunflower complex (prices verbatim from the circular, USD/MT):

| NCM position | Product (HS) | Price | Sibling |
|---|---|---|---|
| `12060090910Y` | 1206.00.90 sunflower seed, bulk | **507** | `12060090929W` 527 (= bagged, +20, same +20 pattern as soy `12019000299C` 471 vs granel 451) |
| `15121110310E` | 1512.11.10 crude sunflower oil | **1364** | `15121919110H` 1582 (refined) |
| `23063010100F` | 2306.30.10 sunflower oilcake/pellets | **190** | `...200L` 186, `...310V` 188, `...320Y` 133 |

HS headings 1206.00.90 (sunflower seed), 1512.11.10 (crude sunflower oil) and 2306.30 (sunflower oilcake)
are confirmed externally; 1512.11.10.00 is labelled "Aceite de girasol" in Argentina's own export
nomenclature (Cancillería product sheet). **Not verified:** which of the three same-priced
`1512.11.10` sub-positions is the bulk benchmark, and what distinguishes the four `2306.30` meal
sub-positions. Resolve exactly as the soy mapping was resolved — cross-check one historical date against
the labelled `datos.gob.ar` sspm dataset 358 (`config.py:607-612`) before committing the mapping.
My `apis.datos.gob.ar/series` search did not surface the FOB series by keyword; use the dataset directly.

**Sunflower yields are not soy yields — do not reuse `CRUSH_OIL_FACTOR`.** Sunflower is roughly 42% oil /
40% meal by mass out of a ~44%-oil seed, against soy's 18/73. Illustrative only, with those yields:

```
1364 × 0.42 + 190 × 0.40 − 507 = 572.9 + 76.0 − 507 = +141.9 USD/MT
```

That number is an **assumption**, not an observation — a sunflower-specific yield pair needs sourcing
(FEDIOL/USDA extraction rates) before it ships. The *availability* of the three legs is observed.

Also discarded by the same filter and worth knowing about: corn `1005.90`, wheat `1001.19`, sorghum
`1007.90`, barley `1003`, wheat flour `1101`, refined soy oil `1507.90.19`.

### 3.5 Brazil — one named scrape away, physical

`brazil_spot_prices` is bean-only. Observed:

```
Soybean (AgRural Paranaguá FOB)    7 rows  2026-07-27 → 2026-08-06  BRL/MT
Soybean (CEPEA)                  114 rows  2026-02-20 → 2026-08-06  BRL/MT
Soybean (CONAB PR farmgate)       56 rows  2025-07-11 → 2026-07-31  BRL/MT
Soybean (ESALQ/B3 Paranaguá)     114 rows  2026-02-20 → 2026-08-06  BRL/MT
```

`NOTICIAS_AGRICOLAS_URLS` (`config.py:579-590`) maps exactly two keys, both beans. **No oil or meal leg
exists anywhere in the DB for Brazil.**

The same host already scraped by Layer 17 publishes the missing legs. Observed on
`https://www.noticiasagricolas.com.br/cotacoes/soja`, all three **dated 07/08/2026**:

| Table | Front window | Value | Unit |
|---|---|---|---|
| Prêmio Soja Paranaguá/PR | Agosto/26 | +1,45 | US$/bu over CBOT |
| Prêmio farelo de soja - Paranaguá/PR | Agosto/26 | +0,08 | US$/short ton over CBOT |
| Prêmio óleo de soja - Paranaguá/PR | Agosto/26 | +0,10 | cents/lb over CBOT |

Each premium is quoted in its leg's **native CBOT unit**, so the existing conversion factors in
`pipeline/units.py:34-46` apply unchanged. Formula:

```
bean_fob_usd_mt = (cbot_bean_c_bu  + prem_bean_c_bu ) × 0.367437
oil_fob_usd_mt  = (cbot_oil_c_lb   + prem_oil_c_lb  ) × 22.0462
meal_fob_usd_mt = (cbot_meal_usd_st + prem_meal_usd_st) / 0.907185
crush_usd_mt    = oil × 0.1833 + meal × 0.7333 − bean
```

Worked, using CBOT 2026-08-07 from `prices` and the premiums above:

```
bean = (1181.25 + 145) × 0.367437 = 487.3
oil  = (67.45 + 0.10)  × 22.0462  = 1489.2
meal = (316.60 + 0.08) / 0.907185 =  349.1
crush = 1489.2 × 0.18333 + 349.1 × 0.73333 − 487.3 = +41.7 USD/MT
```

vs CBOT board 94.5 and Argentina official-FOB 24.75 on adjacent dates — a three-origin export-margin
board, which is the natural extension of the existing cross-origin FOB board (Layers 19 × 20 × 21).

Two cautions:

- **Not verified:** whether the dedicated sub-pages render server-side. `/cotacoes/soja/farelo-de-soja`
  returned a live daily domestic **meal** table (07/08/2026: Rio Grande do Sul 1.800,00 R$/t; Mato Grosso
  IMEA 1.665,87; Rondonópolis/MT BCSP 1.740,00), but
  `/cotacoes/soja/premio-oleo-de-soja-paranagua-pr` fetched as a **2017** page while the aggregate
  `/cotacoes/soja` showed the same premium dated 07/08/2026. Scrape the aggregate page, or spike the
  sub-pages first. `fetchers/noticias_agricolas.py` already enforces a `"soja"` title/heading guard
  (`:57-58, :109-129`) that would need per-table extension.
- There is a live daily **domestic** meal cash quote (BRL/t, regional) but **no domestic oil cash quote
  was found** — the site's "Óleo de Soja" entry is CBOT, not Brazilian physical. So an *interior*
  Brazilian crusher margin is **not** available; only the port premium version is.

### 3.6 Rapeseed / canola

- **China (CZCE):** oil `OI0` and meal `RM0` are stored daily and liquid (2026-08-07: oil 10,144 CNY/MT
  on 136,066 lots; meal 2,173 on 213,699 lots — `config.py:319-320`). The **seed leg is the gap**. `RS0`
  is served by the same Sina feed (2,702 rows, close 5,903 on 2026-08-10) but with **volume 5 and open
  interest 43** — a dead contract. A margin built on it would be a printing artifact.
  **Named leg required:** a real rapeseed/canola seed price. Candidates not investigated here: ICE Canada
  canola (RS, Winnipeg), Euronext ECO, or a Chinese imported-rapeseed CIF quote.
- **Euronext:** Euronext lists all three (ECO rapeseed, RSO rapeseed oil, RSM rapeseed meal per its own
  rapeseed brochure), so unlike South Africa the *contracts* exist. But there is **no free feed** for any
  of them (`config.py:315-318`) and **zero rows** in any table. RSO/RSM liquidity is **not verified** and
  is the thing to check first — the MATIF oil and meal contracts are widely believed to be thin, and a
  board crush off illiquid legs is worse than none. Verdict: not computable, and not cheap to make so.
- **World Bank:** Rapeseed Oil exists monthly from 2002; there is **no rapeseed meal series**. Dead end.

### 3.7 Markets with no path

- **South Africa.** `safex_prices` carries `Soybean (SAFEX)` and `Sunflower (SAFEX)` only, both seed,
  both ZAR/MT (`config.py:634-637`, `fetchers/safex.py:53-54`, DB: 8 rows each, 2026-07-28 → 2026-08-07).
  The JSE's local physically-delivered suite is grains and oilseeds only (WMAZ/YMAZ/WEAT/SUNS/SOYA/sorghum);
  its soybean meal and soybean oil products are **cash-settled against CBOT settlements in rand**. Using
  them would produce the CBOT crush denominated in ZAR — a currency translation, not a South African
  margin. **Verdict: not computable, and no leg purchase fixes it** — the local oil/meal price simply is
  not published on any daily feed reachable here.
- **Nigeria.** No price series of any kind. Nigeria exists in the repo as two weather grid points, an FX
  pair and a PSD country. A crush margin is several sourcing problems away, not one leg away.
- **India.** Correctly retired. `config.py:530-545` records why: the Agmarknet resource "has no soy meal
  commodity and its soy-oil rows carry inconsistent units across mandis". Independently, this DB's
  `india_domestic_prices` table is **empty (0 rows)** — so even the bean leg is absent from this snapshot.
  DoCA daily retail edible-oil prices are a *retail* series and would not give a crush margin.

---

## 4. Recommended order of work

Ranked by (value × confidence) / cost. Steps 1–2 add no new source at all.

1. **Fix the DCE bean leg (B0).** One config line, one constant. Turns a −$150/MT artifact into a
   credible +$6/MT China board crush. Highest value per line of code in this whole report.
2. **Argentina soy official-FOB margin** from `argentina_fob` as it stands. Inner-join on `ship_from`;
   emit the nearby margin plus the forward crush curve. Pair it with a **backfill script** for
   `?Fecha=` history (mirroring `scripts/backfill_cepea_gap.py`) — without it there is one data point.
3. **Argentina sunflower**, after the position mapping is cross-checked against datos.gob.ar dataset 358
   and a sourced sunflower yield pair replaces my assumed 0.42/0.40.
4. **Brazil Paranaguá premium trio** — one new scrape on an already-trusted host, and it completes a
   three-origin export-margin board alongside CBOT and Argentina.
5. **Rapeseed seed leg** — scoping only. Do not build on `RS0`.
6. **Stop.** South Africa, Nigeria, India and Euronext should be recorded as *deliberately not covered*,
   with the reason, rather than left as open work items.

## 5. Design constraint for whatever ships

Every market above resolves to the same metric-native expression once each leg is in USD/MT:

```
crush_usd_mt = oil_usd_mt × oil_yield + meal_usd_mt × meal_yield − seed_usd_mt
```

What varies is only: (a) the yields — soy 0.1833/0.7333, sunflower different and unsourced; (b) the FX
step; (c) **whether the result is a board or a physical margin.** (a) and (b) are parameters. (c) is not —
it is a different quantity, and the CBOT/DCE board numbers must never be drawn on one line with the
Argentina and Brazil physical numbers without a label saying so.

---

## Sources

- [JSE Agricultural Derivatives — grain futures & options](https://www.jse.co.za/trade/derivitive-market/interest-rate-derivatives-market/grain-futures-options)
- [JSE Grain Contract Specifications (2022)](https://www.jse.co.za/sites/default/files/media/documents/commodities/Grain%20Contract%20Specifications_0.pdf)
- [Euronext Rapeseed futures & options brochure (ECO / RSO / RSM)](https://www.euronext.com/sites/default/files/2019-04/RAPESEED%20FUTURES%20AND%20OPTIONS%20BROCHURE.pdf)
- [Euronext Rapeseed (ECO) contract specification](https://live.euronext.com/en/product/commodities-futures/ECO-DPAR/contract-specification)
- [Cancillería Argentina — posición 1512.11.10.00 "Aceite de girasol"](https://exportaciones.cancilleria.gob.ar/Estadistica/imagen_producto/4483)
- [NCM 1206.00.90 — sementes de girassol](https://www.ncm.net.br/ncm/120600/sementes-de-girassol-mesmo-trituradas/)
- [Notícias Agrícolas — cotações soja (premium tables, 07/08/2026)](https://www.noticiasagricolas.com.br/cotacoes/soja)
- [Notícias Agrícolas — farelo de soja](https://www.noticiasagricolas.com.br/cotacoes/soja/farelo-de-soja)
- MAGyP Precios FOB Oficiales web service, circular 2032, `?Fecha=06/08/2026` (fetched live 2026-08-10)
