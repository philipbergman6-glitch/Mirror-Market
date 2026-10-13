# A5 · Costed licence menu

Research for [A5 #302](https://github.com/philipbergman6-glitch/Mirror-Market/issues/302) on map [The narrowed A #296](https://github.com/philipbergman6-glitch/Mirror-Market/issues/296). Absorbs [Track L #71](https://github.com/philipbergman6-glitch/Mirror-Market/issues/71) (L1–L4) and [T-EUX #156](https://github.com/philipbergman6-glitch/Mirror-Market/issues/156) (Euronext email).
Date: 2026-10-13. Checkout: `research/a5-licence-menu` @ `4616ded`. Web sources read 2026-10-13 unless dated otherwise.

**Evidence tiers:** **[D]** read on the publisher's own page / fee schedule today · **[2]** confirmed by a second independent source · **[I]** inference · **[?]** UNVERIFIED (could not reach, single secondary source, or conflicting).

## Trigger (decided, not reopened here)

Owner decision 2026-10-13 on #302: **a signed paying client whose contract covers the source. Spend before that: zero.** Free permission emails may be sent at any time. Consequence for this menu: each row's "acceptable spend" is simply *the line that client's contract must cover*; there is no separate revenue threshold.

## Bottom line

1. **The cheapest licence-clean CBOT leg is US$487/month** (CME "Public Website" fee, delayed + historical only) [D][2], plus a vendor's feed fee (quote only). Direct licensing is ≈ US$27.7k/yr. CME removed its "Delayed No Charge Website" packages on 2025-04-01 — **there is no free CME website tier any more** [D][2].
2. **Euronext MATIF may cost €0 for the public site.** €167.55/month is confirmed in the 1 Jul 2026 schedule [D][2], but the schedule carries a fee waiver for redistribution that earns no direct economic benefit, and ESMA Guideline 19 says free website publication of delayed data "should not be considered as data re-distribution". The #156 email is worth sending now; text below. Once MATIF enters a *paid* desk product, €167.55/month applies.
3. **Everything else priced is expensive or quote-only:** ICE US softs US$3k/yr public display (+US$30k/yr redistribution direct) [D]; DCE delayed L1 CNY 70k/yr via CIIS [D][2]; JSE, CEPEA, AgRural, Platts, Argus, Baltic, SEA, SOPA and CME DataMine options/IV are **quote only**. Platts, Argus and Baltic **prohibit public-website display outright**; Baltic's only route is a negotiated Media Partner licence.
4. **Cheap at the trigger:** Open-Meteo €29/month [D] (or ECMWF Open Data, free CC BY 4.0, per #366).
5. **Finding the menu cannot leave silent — today's public site already displays series whose terms forbid it**, independent of any sale (detail in [§ Present exposure](#present-exposure-not-a-purchase-question)): Yahoo-sourced CBOT/ICE closes (Yahoo ToS §2(d)/(h); CME has no free website tier), CEPEA indicators (CEPEA forbids "transmissão de séries de preços"), raw DCE/CZCE closes via akshare/Sina (DCE Trading Rules require consent), and **raw AgRural BRL quotes in the committed `data/history/brazil_spot_prices.csv`** (AgRural forbids table reproduction; `config.py` claims they are "stored locally only"). The zero-spend trigger governs purchases; whether to keep publishing these meanwhile is a separate owner decision, surfaced as a new grilling ticket.

## The menu

One row per source. "On the page today" is from `config.MARKETS` / fetchers at `4616ded`. Prices exclude VAT/GST.

| # | Source | On the page today | Cost to publish on a public site | What it unlocks | Redistribution terms (public site) | Free licence-ready work now |
|---|---|---|---|---|---|---|
| 1 | **CME/CBOT** (ZS/ZL/ZM/ZC, also CME livestock + CPO swap) | Raw closes via Yahoo (`prices`, `contract_bars`, forward curve, workstation chart) | **US$487/mo per website** (vendor route, +vendor fee, quote only) · direct: +US$21,840/yr delayed distribution · historical *feed* distribution US$35,220/yr | Licence-clean CBOT board, curve, workstation; named-contract settlements from a licensed channel (still not "proven settlement" until reconciled) | All licensee websites fee-liable since 2025-04-01; delayed = 10 min–8 h, historical ≥ 8 h | Pick vendor at trigger (Databento/Barchart); registry rights row; nothing to build |
| 2 | **CME options / IV** (CBOT soy options) | Not ingested | DataMine EOM Summary: quote only (Premium Plus US$2,173/mo [?], 2024 letter) · Databento Standard US$199/mo (internal; redistribution = CME fees on top) | Options/vol context — **out of scope** (ROADMAP expansion track) | CME fees as row 1 | None (expansion track) |
| 3 | **ICE Futures US** (sugar, cotton) | Raw closes via Yahoo (`prices`, `contract_bars`) | Public Display **US$3,000/yr** · redistribution licence US$30,000/yr non-member (real-time, delayed, historical) | Licence-clean softs rows | Licence required; no free delayed tier found for IFUS (IFEU states delayed pricing free of *fee*, licence still needed) [?] | Decide whether softs earn a row at all (they are context, not soy) |
| 4 | **DCE** (No.1/No.2, meal, oil) + **CZCE** (rapeseed oil/meal) | Raw closes via akshare/Sina (`dce_futures`) on the Dalian page; derived crush/parity | DCE delayed L1 via CIIS **CNY 70,000/yr** (≈ US$9.8k at 7.1, illustrative); EOD: quote only; CZCE: quote only [?] | Licence-clean Dalian board | DCE Trading Rules: no publication or commercial use without DCE consent; akshare (MIT) conveys no data rights, README "academic research purpose" | None until exposure decision |
| 5 | **Euronext MATIF** (ECO rapeseed, wheat, corn) | Not ingested (EC Moselle physical instead) | **€0 if waiver granted** · else **€167.55/mo** delayed redistribution (Last Price), €334.95 L2 | A real EU futures leg + EU basis on the Europe page | Free MiFIR trades file: free unless distributed "subject to a fee" (cl. 6) vs disclaimer's flat bar — ambiguity; waiver form exists | **Send the #156 email (below)** |
| 6 | **CEPEA/ESALQ** (Paraná, Paranaguá) | Brazil headline price, via Notícias Agrícolas; raw BRL in committed history CSV | **Quote only** (R$60 buys historical series for own use [?], not republication) | Licence-clean Brazil price leg | CC BY-NC label *and* copyright policy barring "transmissão de séries de preços" — conflicting even for non-commercial | **Send the L3 email (below)** |
| 7 | **AgRural** (Paranaguá FOB) | Derived USD basis rendered; **raw BRL committed to `data/history/`** | **Quote only** (reader plans from R$275/mo [?], not redistribution) | Raw Paranaguá FOB line | "Fica vedada, sob qualquer forma, a reprodução total ou parcial das tabelas … salvo mediante autorização prévia e por escrito" | Stop committing raw BRL (exposure ticket); permission email optional |
| 8 | **JSE/SAFEX** (soy, sunflower) | Last-traded price via Grain SA (`safex_prices`, committed) | **Quote only** (data licensing behind marketdataconnect.jse.co.za) | MTM/settlement instead of last-traded | Delayed may go to uncontrolled environments *under a Data Agreement*; free public-display policy excludes websites; MTM file: no reproduction, no commercial use | None; SAGIS flow leg (#202) is the clean SA content |
| 9 | **B3** (Brazil soy futures) | Not ingested | Delayed snapshot distribution **US$160/mo per dataset** (international), from 2026-11-01 policy | A Brazil futures/basis leg | Delayed snapshot on open websites allowed under licence; EOD/history only for informational charts/tables; **no download** | None (not on roadmap) |
| 10 | **Platts** (S&P Global Energy) soy FOB, meal, oil, Panamax | Not ingested | **Quote only** | Licensed Brazil/Argentina FOB + freight | No publication or derivative works without written consent; extra uses fee-bearing | None |
| 11 | **Argus** soy/vegoil/freight | Not ingested | **Quote only** | As Platts | No copying "single prices, graphs" without written consent | None |
| 12 | **Baltic Exchange** (BDI, P1A/P2A/P6/P8) | Not ingested (GTR public-domain freight instead) | **Quote only** (no published fee list; £3,420/yr investor indices [?] is not a redistribution fee) | Daily ocean freight on the FOB/landed board | Restricted and full licences both: "shall NOT … display Baltic Data on any intranet or public website"; derived only with permission and underlying data hidden; public route = Media Partner channel licence | None |
| 13 | **Open-Meteo** (Layer 5 weather) | 24 pins, free API | **€29/mo** Standard (1M calls), €99 Professional, €160 10M | Commercial use of current pins | Free tier non-commercial only; "promotional activities" counted commercial; data CC BY 4.0 with link | At trigger: €29/mo or ECMWF Open Data (#366, #357) |
| 14 | **CONAB** (estimates, PR farmgate 15b) | Rendered, committed | Free | — | CC BY-ND 3.0 site footer; ND binds copyrightable expression, arguably not facts → USD conversion / YoY are ours [I] | Optional confirming email via Fala.BR |
| 15 | **Comex Stat / MDIC** (Layer 31) | Rendered; unit value gated | Free | Customs unit value (`COMEXSTAT_PUBLISH_UNIT_VALUE`) | CC BY-ND 3.0 [D] | Same ND email unlocks unit value |
| 16 | **SEA India** (Layer 32) | Private (`SEA_PUBLISH=False`) | **Quote only** (membership ₹32,450/yr foreign associate; daily-rate email ₹5,900/yr — neither grants republication) | Public India meal FAS Kandla / CIF Mumbai oil | "All Rights Reserved"; no reuse grant | Email pending per memory (seaofindia1963@gmail.com) |
| 17 | **SOPA India** (Layer 33) | Private (`SOPA_PUBLISH`); derived only | **Quote only** (membership ₹12–75k/yr; no republication grant) | Raw state crop table | "All rights not otherwise claimed … reserved" | Optional email (sopa@sopa.org [?]) |
| 18 | **Yahoo Finance** (transport) | Carries rows 1 and 3 | **No licence offered** — replace, never license | — | ToS §2(d) no automated collection; §2(h) no redistribution without written permission | Replacement = row 1 vendor at trigger |
| 19 | **Notícias Agrícolas** (transport) | Carries row 6 | None | — | "© direitos reservados"; no grant; carries Safras & Mercado data too | Replacement = CEPEA licence |
| 20 | **Clean public sources** — USDA (AMS/FAS/NASS/OCE), NOAA/NHC, JTWC, EC Oilseeds, SAGIS, Agmarknet, FRED, EIA, EPA RFS, GTR, river gauges | Rendered | Free | — | Public domain / CC BY 4.0 / reproduce with acknowledgement / GODL-India. EPA's disclaimer limits *commercial* reuse of documents [?]; RFS figures arguably 17 USC §105 [I]. MAGyP rights review pending (`trust/registry.py` `_MAGYP_RIGHTS` UNKNOWN) | Keep attribution strings rendered |

### Bundles at the trigger (what a first client contract would need to cover)

| Bundle | Lines | Monthly | Notes |
|---|---|---|---|
| **Core — keep the public site as it is, licence-clean** | CBOT website fee (vendor) + vendor feed + Open-Meteo Standard + Euronext (waiver or €167.55) | **≈ US$487 + vendor + €29 (+€0–167.55)** | DCE/CEPEA/ICE rows withheld or quoted separately |
| **+ China board** | DCE delayed L1 | +≈ US$820 (CNY 70k/yr) | EOD-only price not published |
| **+ Brazil cash** | CEPEA + AgRural | quote only | Or keep CONAB farmgate + derived basis only |
| **+ Freight** | Baltic Media Partner or Platts | quote only | GTR public-domain monthly freight stays the free substitute (#75) |
| **Private desk edition only** (client sees CBOT, public site does not) | CME delayed via vendor | vendor fee; CME delayed display device $0 [I] | Website fee would not apply to a non-public edition [I] — confirm with CME/vendor |

## Per-source evidence

### 1–2. CME Group (CBOT) and options

- **[D][2] Fee list effective 1 Jan 2026** (fees "assessed per exchange"): read via Databento's mirror `https://api.databento.com/static/licensing/cme/cme-market-data-fee-list.pdf` (original `https://www.cmegroup.com/market-data/files/january-2026-market-data-fee-list.pdf` blocks scrapers). **Public Website US$487 per website per month**, footnote 13: "Delayed and Historical Information only. Real Time Information is not permitted." Delayed Distribution US$21,840/yr per Licensee Group; Historical Distribution US$35,220/yr (footnote 2: "Distribution of Historical Information via Data Feed"); delayed display device $0; delayed data feed $304/mo. Second source: Barchart exchange-fees page `https://www.barchart.com/solutions/exchange-fees` — "$21,840" delayed redistribution and "CBOT Delayed: $487/mo" website hosting.
- **[D][2] No free website tier since 2025-04-01:** CME notice `https://www.cmegroup.com/market-data/files/fee-and-policy-adjustments-futures-and-options-september-2025.pdf` — "all references to Delayed No Charge Website packages are removed … all Licensee Websites will be fee liable at the prevailing Website Fee"; EOD licences terminated into delayed. Corroborated by WatersTechnology, 2026-02-24, `https://www.waterstechnology.com/data-management/7952957/cme-rankles-market-data-users-with-licensing-changes`.
- **[?] Conflict:** Barchart says the CME website fee is "waived when placing the CME logo and link". That contradicts the April 2025 termination, and the same Barchart page still shows Euronext's 2025 price — treated as stale.
- **[?]** Whether publishing only settlements ≥ 8 h old (CME "historical") avoids any distribution fee beyond the website fee — footnote 2 suggests the historical fee is feed-only. Ask `marketdata@cmegroup.com` at the trigger.
- Prior repo research agrees: `research/2026-08-14-soy-trader-bloomberg-replacement-benchmark.md:166-188` ("licensed CBOT pricing as a non-waivable" commercial gate).
- **Options/IV:** DataMine publishes no price list; "End of Market Summary – Standard" is the IV-bearing tier (CBOT IV from 2008-07). Premium Plus US$2,173/mo appears only in HealthyMarkets' 2024-11-19 letter to the CFTC [?]. Databento CME plans: Standard **US$199/mo** from 2026-06-22, Plus US$1,750, Unlimited US$4,500 (`https://databento.com/pricing`, `https://databento.com/blog/updates-to-subscription-pricing`) [D][2: earlier $179 blog]; Databento: "Your redistribution rights depend on the dataset … we pass through these license fees with no upcharge" — CME fees still apply. Options sit on ROADMAP's expansion track; costed, not recommended.

### 3. ICE Futures US

- **[D]** "ICE Futures Technology & Market Data Fees 2026 v1", `https://www.ice.com/publicdocs/Fee_Overview.pdf` p.1, *Annual Redistribution License Fees*, IFUS column: Redistribution License Fee (Non Member) "Real Time, Delayed and Historical" **$30,000**; Member $25,000; S2F $50,000; **Public Display $3,000**. Derived data (index use) EOD $10,000 per product (p.3) — not our use. Single primary source; not cross-checked.
- **[?]** No IFUS statement that delayed data is free was found; ICE Futures Europe's policy says "There is no charge for delayed pricing data" yet still requires a licence for public website display.

### 4. DCE / CZCE

- **[D][2]** CIIS (Hong Kong, DCE's overseas licensing channel) `https://www.ciis.com.hk/hongkong/en/productsandservices/whatdoweprovide/index.shtml`: **Delayed Level-1 (≥10 min) CNY 70,000/yr**; real-time L1 CNY 300,000/yr + per-subscriber; L2 CNY 400,000/yr. Same CNY 70,000 in the 2018 CIIS schedule. No EOD or website category → quote only. Domestic licensing via DFITC (`marketdata@dce.com.cn` [?], from search snippets; site refused connections).
- **[D]** DCE Trading Rules (Art. 103; Art. 73 in the Jan 2026 revision): no publication or commercial use of DCE quotes without DCE consent.
- **[D]** akshare is MIT-licensed code; README: "All data provided by AKShare is just for academic research purpose". #71's 2026-08-08 note had ruled DCE/CZCE **derived-only**, "not raw redistribution" — but the Dalian page's price block renders raw `dce_futures.Close` (`config.MARKETS["dalian"]["price"]`). That note is no longer what the page does.
- CZCE not separately researched — quote only [?].

### 5. Euronext MATIF

- **[D][2] €167.55/month** Delayed Data Redistribution, Last Price (€334.95 L2), unchanged across schedules effective 1 Jan 2026 (v18.0), 1 Apr 2026 (v18.1) and **1 Jul 2026 (v20.0, p.10)**: `https://connect.euronext.com/sites/default/files/documentation/data/Product%20Fee%20Schedule%20(effective%201%20July%202026).pdf`. Corroborated by Barchart's pass-through "Delayed: €163.60" (2025 figure) — see `research/2026-08-10-issue-148-matif-rapeseed.md:42-56,141-144`.
- **[D] Waiver** (schedule footnote 1): "If the Contracting Party and its Affiliates do not generate a Direct Economic Benefit from nor create a Value Added Service in relation to the Delayed Data Redistribution of this Information, the Delayed Data Redistribution Licence Fee for such Information Products will not apply, provided that the Contracting Party requests a Fee waiver through the Delayed and/or After Midnight Data Fee Waiver Application Form." EMDA policy 2.4: Euronext decides "in its sole discretion"; an EMDA must still be signed.
- **[D] ESMA Guideline 19** (ESMA70-156-4263, which the EMDA uses for both defined terms): "Where a delayed data user publishes delayed data on its website, but does not charge for that access, it should not be considered as data re-distribution … including where the data user generates indirect revenue (for example via advertisement)"; a value-added service is chargeable only if "sold as a product for a fee to third parties." `https://www.esma.europa.eu/sites/default/files/library/esma70-156-4263_guidelines_mifid_ii_mifir_obligations_on_market_data.pdf`. **[I]** This answers #148's worry that computing basis/spreads makes the site a "Value Added Service" — under ESMA's definition it does not while the site charges nothing.
- **[D]** EMDA 7.11/7.12: delayed data must be labelled delayed with a timestamp and attributed to Euronext.
- **[D]** euronext.com terms prohibit spiders and systematic retrieval (`research/2026-08-10-issue-148-matif-rapeseed.md:106-114`) — scraping the settlement page stays out regardless of the answer; the data path would be the MiFIR trades file or a licensed feed.
- Contact `databyeuronext@euronext.com` [D] (single source).

### 6–7. CEPEA and AgRural

- **[D] CEPEA licence page** `https://www.cepea.org.br/br/licenca-de-uso-de-dados.aspx`: content under "Creative Commons (CC BY NC …)"; "São proibidas reproduções, armazenamentos, transmissões, cópias, distribuições ou quaisquer outras formas de utilização para fins comerciais … salvo quando expressamente autorizado pelo CEPEA."
- **[D] CEPEA copyright policy** `https://www.cepea.org.br/br/politica-de-direitos-autorais.aspx`: "devem solicitar autorização formal da equipe Cepea" (widget content excepted); "É obrigatória a citação do Centro de Estudos Avançados em Economia Aplicada (Cepea/Esalq)"; "**Não é permitida** a vinculação de página inteira do site do Cepea no interior de outros sites **nem a transmissão de séries de preços**." The two texts conflict; the stricter one bites a non-commercial site too. Contact `cepea@usp.br`, +55 19 3429 8800.
- **[?]** `onetoone.cepea.org.br/indicadores1/` "Séries Históricas … R$60,00" — search snippet only (page 403); a data purchase, not a republication right. Commercial licence: no published price anywhere.
- **[D] Notícias Agrícolas:** "© direitos reservados"; no reuse grant found; it is a transport, not a licensor.
- **[D] AgRural** `https://agrural.com.br/precossojaemilho/` "Aviso Legal": "É permitida a citação pontual de dados, desde que de forma isolada e acompanhada de referência expressa à AgRural, por veículos de imprensa … Fica vedada, sob qualquer forma, a reprodução total ou parcial das tabelas de preços disponibilizadas neste site, salvo mediante autorização prévia e por escrito da AgRural." Plans "a partir de R$275/mês" from a search-indexed copy of `/boletins` [?]. Contact `contato@agrural.com.br`.
- **[D] Repo contradiction:** `fetchers/agrural.py:33-39` and `config.py` ("raw BRL/saca quotes are stored locally only") promise raw AgRural stays local, but `pipeline/history.py:63-65` exports `brazil_spot_prices`, and `data/history/brazil_spot_prices.csv` holds 43 `Soybean (AgRural Paranaguá FOB)` rows in BRL/MT plus 159 CEPEA and 159 ESALQ/B3 rows — committed to a public repo. Under invariant 4's own logic ("a committed CSV is a publication"), this publishes the table AgRural forbids reproducing.

### 8. JSE / SAFEX

- **[D]** 2026 JSE price list covers trading/colocation only; data licensing sits behind `https://marketdataconnect.jse.co.za` → **quote only**.
- **[D]** JSE Market Data Policies (`https://www.jse.co.za/sites/default/files/jse_document_manager/RW/Internal/JSE%20Data%20Agreements/Equities,%20Derivatives%20and%20Interest%20Rates/01.%20JSE%20Market%20Data%20Policies.pdf`): §7.5.1/§12.2 delayed (≥15 min) "may be distributed in an Uncontrolled User Environment, subject to the Data Agreement"; §7.8.2 EOD statistics distributors "must enter into the required data agreement … and pay the relevant data fees"; §11.1.5 free public display excludes websites; §17 historical (> 24 h) self-built history may pass to external users "at no additional charge" — still under an agreement. Website terms `https://www.jse.co.za/terms-and-conditions`: "reproduction, redistribution … strictly prohibited". Parts dated 2014.
- Grain SA, the transport for the last-traded price, was not re-checked in this pass [?]; #157 graded SAFEX RED.

### 9. B3

- **[D]** Política Comercial de Market Data B3 2026 v1.1 (from 2026-11-01): delayed snapshot distribution **US$160/R$320 per dataset per month**; delayed continuous US$960/R$1,920; real-time US$4,000/R$6,000. Política de Consumo §3.3.2: delayed snapshot "em websites abertos" limited to min/max/open/EOD prices, volume, OI; EOD/historical only "para construção de gráficos e tabelas informacionais", **no download**. Single primary source per doc.

### 10–12. Platts, Argus, Baltic

- **[D] Platts** (Daily Grains & Oilseeds notice, 2025-11-26): "You may not publish, reproduce, extract, distribute, retransmit, resell, create any derivative work from and/or otherwise provide access to the Data … without S&P Global Commodity Insights' prior written consent … Any use or distribution of the Data beyond the express uses authorized … is subject to the payment of additional fees". Quote only; `ci.support@spglobal.com`. Derived-data policy unreachable (403) [?].
- **[D] Argus** sitewide notice: no copying "(including, but not limited to, single prices, graphs or news content) in any form or for any purpose whatsoever without the prior written consent of the publisher"; `https://www.argusmedia.com/en/policies/copyright-policy`. Quote only.
- **[D] Baltic** Data Policy `https://www.balticexchange.com/en/site-services/data-policy.html`: 2.2.3 restricted licence and 2.3.3(b) full licence — subscriber "shall NOT … display Baltic Data on any intranet or public website"; derived data only with written permission "so long as the underlying Baltic Data is not visible"; banned: derived data "which seeks to track or match the values". Public route = Channel Partner licence as a "Media Partner". `data.partners@balticexchange.com`. Consistent with #75 (Baltic and IGC GOFI ruled out; GTR public-domain freight is the substitute).

### 13. Open-Meteo (#366)

- **[D][2]** `https://open-meteo.com/en/pricing` (rendered 2026-10-13): API Standard **€29/mo** (1M calls, commercial licence), Professional €99, Professional 10M €160, Enterprise quote. Third-party listings say "$29/mo" — currency may be locale-dependent [?].
- **[D]** `https://open-meteo.com/en/terms`: free tier non-commercial; non-commercial example "private or non-profit websites or apps that do not have subscriptions or advertising"; commercial includes "Integrating our service into commercial products or promotional activities". Data `https://open-meteo.com/en/licence`: CC BY 4.0, link "next to any location Open-Meteo data are displayed".
- **[I]** Today's public site fits non-commercial. The A6 trial (unpaid, private edition) is arguably still non-commercial; using the site as a *sales pitch* edges toward "promotional activities". Under the decided trigger: no spend until a signed client; the free fallback at the trigger is ECMWF Open Data (CC BY 4.0, commercial allowed — #357).

### 14–15. CONAB, Comex Stat (CC BY-ND)

- **[D]** CONAB (`https://www.gov.br/conab/pt-br`) and MDIC bulk-data page footers: "Todo o conteúdo deste site está publicado sob a licença Creative Commons Atribuição-SemDerivações 3.0 Não Adaptada."
- **[I]** CC FAQ: licences "are operative only when applied to material in which a copyright exists" — facts (tonnes, BRL/MT) are arguably outside ND, so USD conversion, YoY and unit value are our derived numbers; reproducing CONAB/MDIC tables or charts as designed is what ND forbids. Decreto 8.777/2016 art. 2 III defines open data as free to use and cross-reference, crediting the source. Not legal advice; a Fala.BR email to each agency settles it for €0 and would open `COMEXSTAT_PUBLISH_UNIT_VALUE`.

### 16–17. SEA, SOPA

- **[D]** SEA footer "Copyright © 2018 Solvent Extractors' Association of India . All Rights Reserved." Membership (w.e.f. 2026-04-01) Associate (Foreign) ₹32,450/yr; daily rates by email ₹5,900/yr (`https://seaofindia.com/contact/join-us/`). No republication product → quote only. Contact `seaofindia1963@gmail.com`.
- **[D]** SOPA terms `https://sopa.org/terms-conditions/`: "All rights not otherwise claimed under this Agreement or by the Soybean Processors Association of India are hereby reserved." Membership ₹12,000–75,000/yr; Foreign Affiliate US$500/yr (`https://sopa.org/membership-levels/`). Contact `sopa@sopa.org` [?] (2016 FAO deck).
- Both are correctly private today (`config.SEA_PUBLISH = False`, `SOPA_PUBLISH`); nothing to change.

### 18. Yahoo Finance

- **[D]** Yahoo Terms (updated 2026-08-04) `https://legal.yahoo.com/us/en/yahoo/terms/otos/index.html`: §2(d) no collecting data "using any automated means … scrapers … for any purpose without our express, prior permission"; §2(h) "Unless you have explicit written permission, you must not reproduce, modify, rent, lease, sell, trade, distribute, transmit, broadcast, publicly perform, create derivative works based on … any portion or use of, or access to, the Services". yfinance README: "intended for personal use only". Matches `trust/registry.py` `_YAHOO_MARKET_RIGHTS` (raw + redistribution PROHIBITED). Yahoo licenses nothing; the fix is the row-1 vendor.

## Present exposure (not a purchase question)

The trigger decides *when to spend*. It does not decide whether the free site may keep publishing series whose terms already forbid it while non-commercial. What is published today:

| Series | Where published | Term it meets | Commercial-only restriction? |
|---|---|---|---|
| CBOT/CME closes + bars | CBOT page, workstation, `contract_bars.csv`, `forward_curve.csv` | Yahoo §2(d)/(h); CME: no free website tier since 2025-04 | **No** — applies non-commercially |
| ICE sugar/cotton bars | `contract_bars.csv`, workstation | Yahoo; ICE public display licence | No |
| DCE/CZCE closes | Dalian page | DCE consent rule; akshare "academic research" | No |
| CEPEA + ESALQ/B3 indicators | Brazil page, `brazil_spot_prices.csv` | CEPEA "nem a transmissão de séries de preços" | No (policy text); CC BY-NC label alone would allow |
| AgRural raw BRL | `brazil_spot_prices.csv` (43 rows) | AgRural table-reproduction ban; contradicts `config.py` | No |
| JSE/SAFEX last traded | South Africa page, `safex_prices.csv` | JSE website terms; Grain SA unverified | — |

Options the owner can choose between (no recommendation forced here; each is a policy call, not a fact): keep publishing pre-sale and accept the risk; withhold raw levels and publish only derived numbers (the AgRural/SOPA pattern); or pay the US$487/mo CBOT website fee early. The AgRural CSV is the one case where the repo contradicts its *own* stated rule. → New HITL ticket on map #296.

## Free licence-ready work worth doing now (zero spend)

1. **Send the permission emails** — Euronext (#156, below), CEPEA (L3, below); optionally CONAB/MDIC ND confirmation (unlocks the Comex Stat unit value), AgRural, ANEC (#73). All free; the owner sends them, an agent never does.
2. **Rights rows for every rendered source.** `trust/registry.py` carries `RightsPolicy` for only MAGyP, Yahoo and AgRural (review date `_RIGHTS_REVIEW_ON = 2026-11-10`), and no renderer reads `PUBLIC_DISPLAY`. Adding rows with this file as `evidence` is cheap and makes the menu machine-checkable. Wiring a render gate should wait for the exposure decision, since its default depends on it.
3. **Nothing else.** Vendor selection (Databento vs Barchart for CBOT) is a trigger-time task and needs no seam today. ROADMAP's rule holds: nothing should be held open for the expansion track.

## Disposition of Track L (#71)

| Item | Disposition |
|---|---|
| **L1** Open-Meteo paid plan (~$29/mo, "do first") | **Deferred to the trigger** (zero-spend decision supersedes "do first"). Price verified: €29/mo Standard. Current use fits the non-commercial wording; free alternative at the trigger: ECMWF Open Data (#366, #357). |
| **L2** Agmarknet replaces NCDEX | **Done** — Layer 16 on Agmarknet (PR #367), licence clean (reproduce with acknowledgement). Separately broken since 2026-10-09 by a captcha (#449) — an availability problem, not a licence one. |
| **L3** CEPEA commercial inquiry (cepea@usp.br) | **Not yet sent; send now** — free, and sharper than in August: CEPEA's own policy bars transmitting price series even non-commercially. Draft below. |
| **L4** Budget a licensed CBOT delayed feed; AgRural/SAFEX replace-or-agreement | **Costed:** CBOT US$487/mo website fee + vendor (≈ US$27.7k/yr direct). AgRural: agreement quote only; meanwhile only the derived basis may render and the raw BRL should leave the committed CSV (exposure ticket). SAFEX: JSE quote only; SA content leans on SAGIS flows (#202), which are licence-clean. |

## Disposition of #156 — Euronext email

**Send it.** It costs nothing and could flip the Europe page from EC-physical-only to a real futures leg at €0. It is stronger than in August: the fee waiver and ESMA Guideline 19 both point toward a free route for a non-charging site. Not sent by this session. Draft:

> **To:** databyeuronext@euronext.com
> **Subject:** Delayed MATIF rapeseed settlements on a free public website — licence, fee waiver, or not permitted?
>
> Dear Euronext Market Data team,
>
> I run a small, free, publicly accessible website (GitHub Pages; no subscriptions, no advertising, no login) that publishes a once-daily read of the soybean and oilseed complex for physical buyers. I would like to show the **delayed, end-of-day settlement prices for Euronext Rapeseed futures (ECO, Euronext Derivatives Paris)** — front months only, published the following morning, labelled as delayed with a timestamp, attributed to Euronext, carrying your required disclaimer, and alongside our own derived figures (a USD/t conversion and a spread against other oilseed prices). The data would not be offered as a download or feed.
>
> Before doing so I want to be sure I am licensed correctly. Could you tell me:
>
> 1. **Is a licence needed at all?** Your Terms and Conditions for Delayed Trade Data say the data may not be distributed "subject to a fee" without authorisation (clause 6), while the mandated disclaimer says it "may not be copied or further disseminated, by any media whatsoever, except as specifically authorized by Euronext". ESMA's Guideline 19 (ESMA70-156-4263) states that publishing delayed data on a website without charging for access "should not be considered as data re-distribution". Which governs a free website like mine?
> 2. **If a licence is needed, does the fee waiver apply?** Your Product Fee Schedule (effective 1 July 2026) waives the Delayed Data Redistribution Licence Fee where the contracting party generates no Direct Economic Benefit and creates no Value Added Service, on application through the Delayed and/or After Midnight Data Fee Waiver Application Form. Would a free site of this kind qualify, and could you send the form and the EMDA I would need to sign?
> 3. **Is there a non-commercial or personal tier** below the €167.55/month Delayed Data Redistribution (Last Price) licence for Euronext Commodity Derivatives?
> 4. **Which channel** should the data come from under that permission (e.g. the MiFIR delayed trades file, or another end-of-day product)? I will not scrape live.euronext.com.
>
> If the site later becomes part of a paid service, I understand the standard Delayed Data Redistribution licence would apply, and I would contact you before that happens.
>
> Kind regards,
> Philip Bergman
> [project URL]

Record the reply verbatim on #156 (closed) or #302; a "yes, free with disclaimer" or "waiver granted" graduates a MATIF layer into map #142 (per-market feature work).

## L3 — CEPEA email (draft, not sent)

> **To:** cepea@usp.br
> **Assunto / Subject:** Autorização para exibir indicadores CEPEA de soja em site público gratuito / Permission to display CEPEA soybean indicators on a free public website
>
> Prezada equipe Cepea,
>
> Mantenho um site público e gratuito (sem assinaturas nem publicidade) com uma leitura diária do complexo soja para compradores físicos. Gostaria de exibir os indicadores **CEPEA/ESALQ Soja Paraná** e **ESALQ/B3 Soja Paranaguá** (valor diário em R$/saca e convertido para US$/t), com citação obrigatória "Cepea/Esalq" e link para o site do Cepea.
>
> A licença de uso de dados (CC BY-NC) permite uso não comercial com atribuição, mas a política de direitos autorais diz que não é permitida "a transmissão de séries de preços". Poderiam esclarecer:
>
> 1. A exibição do indicador diário e de um histórico curto (gráfico de ~12 meses) em site gratuito é permitida com atribuição, ou requer autorização formal?
> 2. Se requer, como solicitar a autorização, e há condições (por exemplo, apenas o valor do dia, sem histórico)?
> 3. Caso o site passe a integrar um serviço pago, qual é o processo e o custo do licenciamento comercial dos indicadores?
>
> (English summary: free public site; may we show the daily Paraná and Paranaguá indicators with a short history and mandatory attribution, given the CC BY-NC licence vs the copyright policy's bar on transmitting price series; and what would commercial licensing cost later?)
>
> Atenciosamente,
> Philip Bergman
> [URL do projeto]

## Gaps and confidence

- **UNVERIFIED:** CEPEA R$60 and AgRural R$275 (search-indexed only); DataMine Premium Plus (one 2024 letter); Baltic £3,420; CZCE and Grain SA terms; SOPA email; IFUS delayed-data treatment; whether CME's ≥ 8 h "historical" display escapes distribution fees; whether a private desk edition avoids the CME website fee.
- **Single primary source** (no second found): ICE fee table, B3 policy, JSE policies, Euronext contact email.
- **Not researched:** MAGyP reuse rights (registry says review pending), Bursa Malaysia (palm enters via the CME CPO swap, so row 1 covers it), A3/BCR Rosario (quote only; no licence text found) — none is rendered from its own licensor today except MAGyP.
