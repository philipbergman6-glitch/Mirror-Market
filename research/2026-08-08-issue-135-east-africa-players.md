# Issue #135 — X5: East Africa soy players (Ethiopia + Uganda + Kenya, Tanzania stretch)

- **Issue:** https://github.com/philipbergman6-glitch/Mirror-Market/issues/135 (part of #130)
- **Research date:** 2026-08-08 (all URLs accessed this date)
- **Question:** Who are the commercial players in the Ethiopia/Uganda/Kenya soy complexes — crushers, feed millers, importers/aggregators, exporters — and which are tier-1 by the T1 (#122) conventions? Plus: flag any usable official domestic price source (feeds X6 scope decisions).
- **Method:** Three parallel web-research streams (one per country, Tanzania folded into the Kenya stream). Primary/high-trust sources preferred: USDA FAS GAIN, IFC disclosures, the Competition Authority of Kenya's 2024 Animal Feed Market Inquiry (PDF downloaded and parsed directly), company sites, UN Comtrade-derived series, peer-reviewed trade studies. Claims tagged **[observed]** (read directly in the source or a search-index snapshot of it) vs **[inferred]**. Access caveat: several primaries 403/ECONNREFUSED to bots (fas.usda.gov pages, ecx.com.et, monitor.co.ug, ipad.fas.usda.gov) — where a quote comes from a search-engine digest of the cited page rather than a direct fetch it is marked **[digest]**; verbatim wording of [digest] quotes should be re-verified before being quoted downstream.
- **Output:** 12 new entries in `data/reference/players/africa.yml` (5 ET, 3 UG, 4 KE; 1 tier-1), validator-clean.

---

## 1. The block in one paragraph

East Africa is a **non-GM soy corridor with Kenya as the demand sink**. Ethiopia genuinely exports beans and meal (India-dominant historically, Pakistan/Kenya/UAE rising, China opened for Ethiopian meal Jul 2025). Uganda is a small producer but a regional net exporter — and its **meal** flow to Kenya (~$18.9m, 2023) is ~4x its bean flow by value, because the Lira crushers (Mukwano, Mount Meru) ship product, not raws. Kenya produces almost nothing, has **no large-scale soy crush** (Kenyan competition authority, verbatim), imports ~100k t/yr of meal — and its GE-import ban (re-blocked by the Court of Appeal in Mar 2025 after the 2022 lift) pins it to Zambia/Malawi/Uganda/Tanzania origins, cleared largely through regional crusher-traders (ETG, Mount Meru). No functioning futures market exists anywhere in the block.

## 2. Ethiopia

### Market facts

- **Production scale is poorly pinned.** No dedicated USDA FAS Ethiopia Oilseeds Annual since Mar 2021; the 2025 Grain and Feed Annual (ET2025-0011) touches soy only via land competition ("sorghum harvested area is expected to decrease… due to ongoing land competition from higher-value crops such as sesame seed, soybeans, and cotton") **[digest]** — https://apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Grain+and+Feed+Annual_Addis+Ababa_Ethiopia_ET2025-0011.pdf. Third-party estimates disagree: ~220k MT 2023 (IndexBox) vs ~179k MT 2026 projection (Reportlinker) vs 177k MT ("1.77 million quintals", Milling MEA 2023, fetched directly — https://www.millingmea.com/a-surging-soybean-market-stirring-tensions-between-edible-oil-processors-and-exporters-in-ethiopia/). **Gap:** exact USDA PSD 2025/26 number (query the repo's own PSD layer — Ethiopia is in `PSD_TARGET_COUNTRIES`).
- **Exporter status verified, ≥2 sources.** Peer-reviewed study (2004–2022 data): "92.63% of the grain is exported… the value of exports totaled $44.7 million in 2022", top-5 destinations 2022: **India 60%, USA 12%, UAE 5.24%, Canada, Spain** **[observed digest]** — https://onlinelibrary.wiley.com/doi/10.1155/2024/9979892. Milling MEA (fetched): "130,000 tonnes exported in last fiscal year (valued at $86 million)". 2024: ~29,408 t beans to China (~$18m) **[digest]** — https://ukragroconsult.com/en/news/china-approves-ethiopian-soybean-meal-imports-to-diversify-supplies/. 2024–25 shipment data shifts beans toward Pakistan (44%), Kenya (26%), UAE (7%) **[digest, Volza — shipment counts, not tonnes]**.
- **China meal access, 2025-07-03** **[observed, ≥3 independent]**: GACC authorized Ethiopian soybean meal imports under a phytosanitary protocol; plants must be MoA-approved + GACC-registered. People's Daily — https://en.people.cn/n3/2025/0709/c90000-20337934.html ("soybean meal… produced in the new fiscal year starting from July 8 will be exported to the Chinese market"); GACC requirements — https://www.foodgacc.com/china-gacc-approved-list-registration-ciqcode-cifer-singlewindow-soybean-meal-from-ethiopia; Milling MEA — https://millingmea.com/china-opens-market-to-ethiopian-soybean-meal-in-strategic-trade-shift/; corroborated by USDA Beijing Oilseeds Annual CH2026-0032. JCI: "volumes are expected to be small."
- **No soybean export ban (negative finding).** The edible-oil manufacturers' association *requested* a ban in 2023; State Minister for Industry Tarekegn Bululta deemed it "isn't feasible" **[observed — Milling MEA, fetched]**. Ethiopia's cereal-grain export bans do not list soy; no 2024–2026 source reverses this. Moderately confident (trade press covers this space closely).
- **ECX lists soybean but liquidity is thin.** Tickers by grade/origin (SBAS2/SBAS3 Assosa, SBGJ2 Gojjam; Birr/quintal) **[observed digest]** — https://commodity.com/trading/exchanges/ethiopia/. USDA FAS 2020: "soybeans are actively traded on the ECX trading floor" (dated). Two sampled 2025 daily sheets (via 2merkato, fetched) had **zero** soybean rows; grains trade Wednesdays only. ecx.com.et was unreachable (TLS ECONNREFUSED — possible geo/IP restriction).
- Domestic tension: processors vs exporters compete for beans; 2023 spike to "4,400 Br (US$889) per quintal… increase of nearly 1,000 Br within two weeks" **[observed — Milling MEA, fetched]**.

### Players entered (5)

| Entry | Why |
|---|---|
| **Alema Koudijs Feed PLC** | #1 Ethiopian feed miller (De Heus JV, Bishoftu, 12 t/hr; ~14k t/yr soy intake per **IFC disclosure 43214** — primary; US$10m IFC expansion loan 2024) |
| **Rich Land Biochemical (Richland)** | Largest disclosed soy-crush figure in Ethiopia — projected 96k t/yr meal at Bure IAIP + stated international meal exports. **Single-source, company-projected; ownership undisclosed** |
| **MSA Trading PLC** | #1 meal exporter by shipment count (Volza); company-stated top markets India/Russia/US; $46.56m turnover Mar 2025–Feb 2026 (shipment-derived) |
| **PhiBela / Belayneh Kindie Group** | Largest edible-oil refinery (1.5m L/day Bure, 2 independent sources) — two-sided: local bean sourcing + crude soy oil **imports**; FX-impaired, not fully operational per 2025 Reporter Ethiopia (https://www.thereporterethiopia.com/22305/) |
| **Ethiopia Commodity Exchange** | Market-access analogue of AFEX (which has a Nigeria entry): official venue, thin soy liquidity, honestly flagged |

### Rejected / not entered

- **Tsehay Industry S.C.** — steel/coffee, no soy role (negative).
- **WA Oil Factory, Shemu Group** — palm-dominant refiners, both halted production >6 months in 2024–25 (tax/FX distress, Reporter Ethiopia #37152); marginal soy relevance.
- **Ethio Agri-CEFT (MIDROC)** — upstream commercial soybean *grower* (Ayehu/Bir farms), no crush/trading evidence; context only.
- **Jagdish Agro Production** — #2 meal exporter per Volza only; no website/capacity found — too thin to enter (gap).
- **HCFM, FAFA Foods, BBZ, Guts Agro** — soy-food processors (CGIAR value-chain report ~2021); food-grade, not trade-relevant scale.
- "Health Care Food Manufacturing = Alema" lead was wrong — separate company (Samanu/54 Capital).

## 3. Uganda

### Market facts

- **USDA FAS coverage is thin** — no Uganda oilseeds GAIN report; FAS IPAD numbers internally inconsistent (2.17 vs 0.7 MT/ha on different pages) **[digest]** — treat FAS Uganda soy data as low-confidence. No authoritative current production tonnage found (UBOS census figure is 2008/09-vintage). **[gap]**
- **Kenya is the dominant destination, ≥3 sources.** 2024 bean exports: Kenya $4.26m, Rwanda $2.04m (OEC — https://oec.world/en/profile/bilateral-product/soya-beans/reporter/uga **[digest]**). Oil-cake/meal to Kenya **$18.9m in 2023** (UN Comtrade via https://tradingeconomics.com/uganda/exports/kenya/soybean-oilcake-solid-residue-ground **[digest]**) — the meal flow is ~4x beans by value. Qualitative: "Whilst Uganda is a net exporter of soya bean, both Kenya and Tanzania are significant net importers" (Gatsby Africa, fetched — https://www.gatsbyafrica.org.uk/insight/leveraging-east-africas-soya-bean-opportunity/).
- **Policy:** National Oilseeds Project (NOSP) — ~$160m GoU/IFAD, Jul 2021–Sep 2028, soybean among 4 target oilseeds; Nov-2025 IFAD supervision: "moderately satisfactory (4)… 48,853 households reached (40% of target)" **[digest — IFAD supervision report PDF]**. MAAIF page: https://www.agriculture.go.ug/nosp/.
- **Exchange status:** old Uganda Commodity Exchange defunct; UWRSA merged into the Trade Ministry as a WRS department; successor **UNCE** (80% private / 20% government via UDC) described in official language as *to be* complemented by the WRS — i.e. **not yet operational** [inferred from phrasing; worth one recheck before hard-coding]. WRS Act 2006 under Law Reform Commission review — https://ulrc.go.ug/projects/review-of-the-warehouse-receipt-system-act-no-14-of-2006/.
- Trade is spot/aggregator: smallholders → aggregators/cooperatives → Lira processors (90k+ outgrower schemes) or cross-border traders at Busia (Kenyan traders buy at the border — Farmgain bulletins **[digest]**).

### Players entered (3)

| Entry | Why |
|---|---|
| **Mukwano Group (AK Oils & Fats)** | Anchor crusher: Lira solvent-extraction complex, 90k+ outgrowers, fortified 100%-Ugandan soybean-oil brand. Crush capacity unpublished (gap) |
| **Mount Meru Millers Uganda** | The other Lira anchor; group context from CAK 2024 (Malawi 150k + Zambia 128k t/yr crush) makes the group regionally price-relevant; the group's dedicated soy crusher (Soyco) is in **Rwanda**, not Uganda — recorded honestly |
| **Biyinzika Poultry International** | Demand anchor with **in-house** soybean processing (expelled/extruded soya cake) — more than a meal buyer (https://www.biyinzika.co.ug/our-facilities/feed-mill/) |

### Rejected / not entered

- **Bidco Uganda (BUL)** — 75k t/yr oilseed line exists but BUL is palm-dominant (only oil-palm plantation operator in EA); soy-specific volumes unconfirmed. Covered inside the Bidco Africa (Kenya) entry instead.
- **Ugachick** — real feed miller, but soy usage wholly inferred (standard ration); no direct evidence — doc only.
- **Aponye (U) Ltd** — financially distressed post-founder-death (UGX 38bn debt; Museveni-ordered bailout effort Apr 2025 **[digest]**) — recorded in the YAML comment block, not entered as a healthy incumbent.
- **AgroWays (Jinja)** — maize-focused warehouser; soy handling plausible, not evidenced.
- **RECO, Sesaco, Maganjo** — soy-foods (RUTF/beverages/flours), not trade-relevant scale. **Soybean Africa Ltd, Equator Seeds** — seed companies, upstream of scope.
- **Chinese-owned soy processor lead: dead.** Chinese agro investment centers on the Kehong industrial park (Luwero — pineapple/chili/layers), no soy plant found (negative).
- **Afgri Uganda** — nothing surfaced (gap/negative).

## 4. Kenya

### Market facts (the strongest-sourced country — CAK PDF parsed directly)

- **No large-scale crush, import-dependent** **[observed, PDF-verbatim]**: "Kenya has very little production of soybean and sunflower… relies heavily on imports of soybean meal and sunflower cake"; "As there are no large-scale processors in Kenya, imports are largely of the already processed meal and cake products." — Competition Authority of Kenya, *Animal Feed Market Inquiry Report 2024*, https://cak.go.ke/arch/sites/default/files/ANIMAL%20FEED%20MARKET%20INQUIRY%20REPORT,%202024.pdf (p.34).
- **Meal import scale:** USDA PSD series (via IndexMundi, fetched — https://www.indexmundi.com/agriculture/?country=ke&commodity=soybean-meal&graph=imports): 47k t (2022) → 100k t (2023, 2024).
- **Origins:** "Zambia accounted for just over 70% of soymeal imports into Kenya" (2021–22) **[PDF-verbatim, CAK]**; beans 2024: Tanzania 79%, Uganda 11% **[digest, IndexBox]**. Stress episodes push sourcing to India/Ethiopia/"Eastern Europe" (The Star 2024-08-06; feedbusinessmea.com Feb 2026 — feed prices +45% **[digest]**).
- **Feed industry size:** formal commercial ~600k t/yr, top-4 >50% of sales **[PDF-verbatim, CAK]**; AKEFEMA claims 2.5m t/yr including informal/home-mixing **[digest]** — different scopes, not a contradiction. Soy = ~25% of feed volume, **44% of value** **[PDF-verbatim, CAK]**.
- **GMO whipsaw (critical, decision-grade):** Oct 2022 Cabinet lifted the ban → 2023 High Court dismissed challenges (VOA) → **2025-03-07 Court of Appeal conservatory order blocks GE imports** pending appeal ("the precautionary principle militates in favor of granting conservatory orders" **[digest]** — https://www.nationofchange.org/2025/03/25/kenya-court-of-appeal-blocks-governments-import-of-gmos/) → USDA FAS *Kenya FAIRS Annual* (Jun 2026): "Kenya continues to enforce its long-standing ban on all genetically engineered imports" **[digest — page 403'd; re-verify exact wording]** — https://www.fas.usda.gov/data/gain/2026/06/kenya-fairs-country-report-annual. Practical effect **[PDF-verbatim, CAK]**: "Kenya can source non-GMO inputs from countries including Uganda, Tanzania, Malawi, and Zambia but is not able to import from international producers which are GMO producers." **Gap:** final appeal ruling.
- **Tariffs:** EAC/COMESA preferences → regional sourcing effectively duty-free; EAC CET applies to third countries (trade.gov guide); recurring ad-hoc duty waivers on feed inputs (2021-12, 2022-06 — 577,050 t across 18–30 named millers; Aug 2023 zero-rating; Apr 2025 yellow-corn waiver not yet gazetted per FAS KE2025-0021). **Gap:** exact HS 2304 CET line rate.
- **Aquafeed is the growth edge:** Victory Farms (2nd + 3rd Homa Bay farms 2026, AgDevCo $15m), DiscoverAqua Athi River plant (Q3 2026, >20 t/hr), Maxim Agri/Samakgro Naivasha — all soybean-meal-dependent; company-level soy procurement unpublished (gap).

### Players entered (4)

| Entry | Why |
|---|---|
| **Export Trading Group (ETG)** — **tier 1** | The regional soy ABCD-analogue: ">at least 500 thousand tonnes of non-GM soybeans across sub-Saharan Africa" + ETG/Parrogate crush Malawi 100k + Zambia 240k t/yr — both **from the CAK inquiry PDF [observed]**; ~5m t/yr commodities, Patel/Pembani Remgro/Mitsui shareholders (Daily Nation). Founded Kenya 1967. Tier-1 justified: multi-country crush + trading network that the Kenyan competition authority says "can set the terms" of regional trade |
| **De Heus Kenya** | Largest single feed mill in EA: greenfield KSh3bn Athi River, 200k t/yr expandable 260k (De Heus + Feed Strategy, 2 independent). Cross-listing with De Heus Vietnam (asia_importers.yml); no tier on either |
| **Unga Group Plc** | Listed incumbent; Seaboard ~47% effective interest in Unga Holdings; Fugo feeds; Tunga Nutrition 50/50 Nutreco JV (KE+UG). Seaboard is the quiet US thread through African feed (also 30% of RussellStone Protein, SA) |
| **Bidco Africa** | Consumer soy-oil (SoyaGold) + feeds demand anchor; honestly bounded — refiner/blender, NOT a crusher (CAK verbatim) |

### Rejected / not entered

- **De Heus–Unga acquisition lead: FALSE.** De Heus Kenya was greenfield (negative finding, 2 sources).
- **LDC Kenya** — Panjiva shipment data shows coffee/tea, not soy (negative).
- **Kapa, Pwani, Menengai** — palm refiners, no soy-crush evidence (negative).
- **Cargill Kenya** — Nairobi office trades "wheat, maize, barley, and soybean meal" per company country page **[digest]** — real but small; Cargill's global entry already exists in global.yml; no Kenya-specific entry warranted.
- **Sigma/Isinya, Belfast, Pembe, Treasure, Farmers Choice** — mid-tier feed millers on the 2022 duty-waiver list; capacity data thin (Sigma ~120 t/day); doc-only.
- **Agventure** — nothing found (gap).

## 5. Tanzania (stretch — no entries, per ticket)

Tanzania is a **bean exporter but meal importer**. 2023 bean exports $92.7m — China $37.4m, India $33.4m, Pakistan $19.8m, Kenya only $282k (OEC via https://kilimokwanza.org/tanzanias-soybean-sector-a-sleeping-giant-awakens/ **[digest]**). Yet IndexBox shows Tanzania as Kenya's #1 **bean** supplier (79% by value) in 2024 — **unresolved inconsistency** (different years/HS scope suspected; do not build on either number without reconciliation). USDA FAS Tanzania Grain & Feed TZ2025-0003 **[digest]**: feed sector "needs approximately 135,000 mt of soybean meal… largest imported supplies coming from Zambia, followed by India and Malawi"; crushing capacity limited and mostly sunflower. Named players: **Silverlands Tanzania** (integrated poultry: ~21m day-old chicks, 72k t/yr feed, pioneered local soybean processing, 32k t storage — Tridge/company **[digest]**); **Mount Meru** (the only sizeable sunflower processor — CAK **[PDF-verbatim]**; supplies Kenya sunflower cake, not soymeal); **ETG** operates from Dar es Salaam. If Tanzania is ever promoted from stretch to entries, Silverlands and the ETG Dar hub are the starting points.

## 6. X6 flags — official domestic price sources found in passing

Ranked by plausibility as a Mirror-Market layer (data layers explicitly out of scope here):

1. **Kenya — KAMIS** (https://kamis.kilimo.go.ke/, Ministry of Agriculture & Livestock Development): wholesale/retail/farmgate prices, markets across 47 counties, **soybean is a tracked commodity**, searchable UI + Excel export (https://kamis.kilimo.go.ke/site/market_search). Official, government-owned. No documented API; scrape feasibility unprobed. **Best candidate in the block.**
2. **Regional — RATIN/EAGC** (https://ratin.net/home/data): daily/weekly grain prices + cross-border flows for 5 EA countries incl. all three targets; bulletins + downloadable datasets. Semi-official (industry council). Soybean granularity per-market unverified.
3. **Uganda — Farmgain Africa** (https://farmgainafrica.org/market-data/): weekly prices ~16 markets incl. Lira and Busia border; soybean in bulletins; the detailed terminal is **paywalled** — only the public pages would be usable.
4. **Uganda — Infotrade/AGMIS** (http://agmis.infotradeuganda.com): national MIS with a soybean commodity page; freshness/format unprobed.
5. **Ethiopia — ECX** (https://www.ecx.com.et): official soybean tickers, Birr/quintal — but site unreachable from outside (ECONNREFUSED), soy rows absent from sampled 2025 sheets, grains trade Wednesdays only. **Not viable** without confirming reachability + row frequency. 2merkato's republished daily sheets (HTML, fetched successfully) are the fallback, but soy sparsity remains.
6. **Ethiopia — NBE "Price of Commodities"** (https://nbe.gov.et/exchange/price-of-commodities/): existence observed; soybean coverage unverified — one direct probe needed.

Currency note for X6: ETB/UGX/KES pairs ship only with rendered East-Africa content (umbrella #130 decision); nothing here changes that.

## 7. Cross-country synthesis for the players page

- **The corridor is the story, not any single company.** Ethiopia/Uganda sell; Kenya buys; the GE ban routes Kenyan demand through non-GM regional origins; ETG-class crusher-traders arbitrage the corridor. This is why ETG is the block's only tier-1.
- **Capacity numbers are near-absent.** The only audited-adjacent figures in the whole block: De Heus Kenya 200k t/yr (2 sources), CAK's regional crush table (ETG/Parrogate 340k, Mount Meru 278k across Malawi+Zambia), Alema Koudijs 14k t/yr soy intake (IFC). Everything else is company-projected (Richland 96k) or stale (Mount Meru Uganda, 2017). Entries flag this per the "no fabricated numbers" rule.
- **Seaboard thread:** Seaboard Corp now touches African feed via RussellStone Protein (SA, 30%) and Unga Holdings (KE, ~47% effective) — a cross-file pattern worth a future activity note.
- **Distress watch:** PhiBela (FX-starved, below nameplate), Shemu/WA (halted), Aponye (insolvency proceedings) — East African soy capacity on paper overstates operating reality.

## 8. Gaps (explicitly not resolved)

1. USDA PSD 2025/26 soybean rows for ET/UG/KE — retrievable from the repo's own PSD layer; not re-queried here.
2. Richland ownership + any independent confirmation of its 96k t/yr projection.
3. The MoA/GACC-registered Ethiopian plant list for China meal exports; first shipment date/tonnage.
4. Mukwano and Mount Meru Uganda current crush capacities (the two biggest Ugandan gaps).
5. Kenya: final GMO appeal ruling; exact HS 2304 CET rate; Mombasa soy-specific port volumes; FAIRS 2026 exact wording (fetch the PDF, not the landing page).
6. Tanzania↔Kenya bean-flow inconsistency (OEC vs IndexBox).
7. UNCE (Uganda) operational status — one recheck before X6 hard-codes "no exchange".
