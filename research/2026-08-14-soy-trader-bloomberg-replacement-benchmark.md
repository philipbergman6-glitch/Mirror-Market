# Soy trader intelligence benchmark: what a credible Bloomberg alternative must cover

**Date:** 2026-08-14  
**Purpose:** external, primary-source benchmark for grading Mirror Market as a replacement for the
information workflow of a global physical/derivatives soy trader  
**Scope note:** the approximately USD 20,000 subscription cost is the user's premise. Bloomberg's
public product pages invite prospects to order/contact sales and do not publish a list price, so this
research does not independently validate that number.

Legend: **[SRC]** primary external source · **[OBS]** directly observed in this repository · **[INF]**
inference from SRC/OBS · **[GAP]** no free, first-party equivalent established

---

## Executive finding

Mirror Market should be graded against **three different replacement claims**, not one:

1. **Daily soy intelligence replacement:** a consolidated daily briefing, normalized fundamentals,
   cross-origin comparisons and alerts. Public first-party data can cover most of this job well. **[INF]**
2. **Front-office market-data replacement:** exchange-authoritative real-time/delayed prices, depth,
   options volatility, executable FX and consistent intraday curves. This cannot be supplied lawfully
   and reliably for free. CME says real-time, delayed and end-of-day access is licensed, and its June
   2026 website-publication schedule constrains which snapshots and next-day fields may be published.
   **[SRC]** [CME market-data policy center](https://www.cmegroup.com/market-data/license-data/market-data-policy-education-center.html),
   [June 2026 license changes](https://www.cmegroup.com/market-data/files/market-data-license-agreement-updates-june-2026.pdf)
3. **Full Terminal/workflow replacement:** proprietary news/research, messaging/counterparty network,
   mobile continuity, execution/order management and pre/post-trade risk. Free APIs do not reproduce
   this. Bloomberg itself describes the Terminal as data + proprietary/third-party research, alerts,
   charting, news, collaboration with 350,000+ professionals and multi-asset execution; its commodities
   offering adds spot/forward pricing, intraday fair-value curves, transformation-margin calculators,
   options volatility surfaces and integrated risk/execution. **[SRC]**
   [Bloomberg Terminal](https://professional.bloomberg.com/products/bloomberg-terminal/),
   [Bloomberg commodities](https://professional.bloomberg.com/institutions/corporations/commodities/),
   [Instant Bloomberg](https://professional.bloomberg.com/products/bloomberg-terminal/collaboration-tools/instant-bloomberg/),
   [Bloomberg trading](https://professional.bloomberg.com/products/trading/)

Bloomberg's agriculture-specific page also says it aggregates supply, demand, pricing and margin data
from thousands of government and industry sources, adding proprietary scenarios and analyst research.
That is the closest official statement of the benchmark Mirror Market is trying to meet. **[SRC]**
[Bloomberg agriculture](https://professional.bloomberg.com/institutions/corporations/agriculture/)

**Defensible product claim:** Mirror Market can aspire to be a **better soy-specific daily research
cockpit** than a general-purpose terminal for a particular trader's recurring questions. It should not
claim to replace Bloomberg's exchange-grade intraday feed or end-to-end trading workflow until it buys
the relevant licenses and adds the missing workflow components. **[INF]**

---

## 1. The job a top-tier soy trader needs done

The following is a workflow decomposition, not a claim that every trader uses identical screens.
Each row is necessary because it changes origin choice, crush economics, hedge selection, timing or
risk. **[INF]**

| Decision/workflow | Minimum information needed | Professional standard |
|---|---|---|
| Mark the soy complex | ZS/ZM/ZL outright prices, official settlement, volume/open interest, exact contract and roll state | Never mix last trade, partial daily bar, close and settlement; carry exchange/session timestamp and entitlement |
| Read structure | All liquid contract months, calendar spreads, carry, days to expiry, continuous-series roll method | Same-session curve legs; no stale deferred leg stitched to a fresh front |
| Price optionality | Option chain, implied vol/skew/term structure, Greeks and scenarios | Live or clearly delayed surface from licensed exchange data, with model and rate/forward assumptions |
| Compare origins | US Gulf/PNW, Brazil Paranaguá/Santos/northern arc, Argentina Up River FOB/basis; China landed parity | Comparable incoterm, grade, shipment window, currency, unit and timestamp |
| Compute crush | Bean, meal and oil legs; plant yields; energy/credit/logistics; board versus physical margin | Explicit yield and cost assumptions; aligned shipment/contract months; sensitivity rather than one magic number |
| Follow supply | Area, yield, production, beginning/ending stocks, crush/use and stocks-to-use for US/Brazil/Argentina/China/world | Preserve marketing-year definitions, estimate vintage and revisions |
| Follow demand/flows | Export sales, shipments/inspections, customs by origin/destination, China arrivals, domestic crush | Separate commitments, shipments and customs arrivals; state reporting lag and revision policy |
| Follow crops/weather | Planting/condition/harvest, precipitation, soil moisture, temperature, forecast anomalies by production-weighted region | Forecast vs observation clearly separated; crop calendar and acreage weights; provider/run timestamp |
| Follow logistics | Barge/rail/truck/ocean freight, river levels, port queues/lineups, strikes and closures | Route- and date-specific landed-cost effects; weekly proxies must not be presented as live freight |
| Follow macro/substitutes | BRL/CNY/ARS/USD, rates/dollar, crude/diesel/biofuel, palm/rapeseed/sunflower/corn | Timestamped FX convention and explicit cross-currency conversion |
| Follow positioning | CFTC producer/merchant, managed money, swap dealer, index positioning and changes | Respect Tuesday observation / Friday publication lag and classification caveats |
| Act on events | Release calendar, fast news, policy/tariff/export-tax/biofuel changes, threshold alerts | Event-time alert, source link, impact hypothesis, acknowledgement/escalation |
| Execute and control | Broker/counterparty communication, orders, fills, position/P&L, limits, hedge ratios, audit trail | Permissioned, resilient, logged workflow; not merely an informational dashboard |

The public-data case for this decomposition is strong: USDA explains that WASDE combines NASS
surveys, AMS market news, Commerce trade, FAS attaché/satellite information, weather, models and expert
judgment; AMS's Grain Transportation Report covers barge, rail, truck and ocean volume/rates; CFTC's
disaggregated report separates producer/merchant, swap dealer, managed money and other reportables.
**[SRC]** [USDA outlook process](https://www.ers.usda.gov/topics/farm-economy/commodity-outlook/usda-outlook-process),
[USDA Grain Transportation Report](https://www.ams.usda.gov/services/transportation-analysis/gtr),
[CFTC COT](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)

---

## 2. What Bloomberg relevantly provides

Bloomberg's own pages support this comparison (marketing claims, not an independent performance
audit):

| Capability | Bloomberg's public description | Free-stack implication |
|---|---|---|
| Coverage and normalization | Terminal coverage across asset classes; B-PIPE claims normalized coverage of 35 million instruments from 330+ exchanges and 5,000+ contributors | A soy product can be deeper in its niche, but must build its own identifiers, symbology, units, calendars and QA |
| Prices/curves | Comprehensive commodity spot and forward pricing; fair-value curves updated throughout the day with time-aligned legs | Daily public observations are not equivalent to intraday price discovery |
| Options | Real-time implied-volatility surfaces with filtering/interpolation/extrapolation | No complete zero-cost substitute established |
| Analytics | Transformation-margin calculators, charts, portfolio and trade analytics | Mirror Market can differentiate here if assumptions are transparent and tested |
| News/research | Bloomberg News plus proprietary and third-party research; Bloomberg Intelligence | Government releases cover scheduled facts, not fast editorial news or analyst access |
| Alerts/workspace/mobile | Launchpad monitors, alerts, charts/news and mobile account access | A static daily site is a different latency and continuity class |
| Collaboration | Instant Bloomberg network for ideas, research, inquiries, indications and pricing | No free API recreates the counterparty network |
| Execution/risk | Listed-futures routing, FX, order/execution management and pre/post-trade analytics | Out of scope for an intelligence-only product unless explicitly built/licensed |

Sources: **[SRC]** [Bloomberg Terminal](https://professional.bloomberg.com/products/bloomberg-terminal/),
[commodities solution](https://professional.bloomberg.com/institutions/corporations/commodities/),
[B-PIPE](https://professional.bloomberg.com/products/data/enterprise-catalog/real-time-data-feed/),
[Instant Bloomberg](https://professional.bloomberg.com/products/bloomberg-terminal/collaboration-tools/instant-bloomberg/),
[trading solutions](https://professional.bloomberg.com/products/trading/).

Important comparison discipline: some Bloomberg enterprise data/trading products and exchange
entitlements may be separately contracted. The public pages establish capabilities in Bloomberg's
ecosystem; they do **not** prove that every capability is included in a base Terminal seat. **[INF]**

---

## 3. Free/public first-party coverage map

“Free to access” does not automatically mean “licensed for commercial redistribution,” “real time,”
“API-backed,” or “covered by an SLA.” Those are separate fields in the table.

| Need | High-trust public/first-party source | Cadence / caveat | Coverage verdict |
|---|---|---|---|
| US/global balance sheets | USDA WASDE (monthly, 12:00 ET release) and FAS PSD API | Official estimates; vintages/revisions must be retained. USDA provides PDF/XML/XLS/text and next-day consolidated CSV history | **Strong for fundamentals** |
| US production/crop progress/stocks/crush | USDA NASS Quick Stats and scheduled reports | Survey/statistical cadence, not live; API key; NASS documents 100 requests per five minutes | **Strong** |
| US export commitments | FAS Export Sales Reporting API | Weekly report Thursday 08:30 ET; qualifying daily sales next business day 09:00 ET; sales are exporter-reported commitments, not shipments; API key | **Strong** |
| US inspections/physical bids | USDA AMS My Market News API / FGIS reports | Daily/weekly by report; corrections endpoints exist; publication times vary | **Strong US physical layer** |
| US and Brazil logistics proxies | USDA AMS Grain Transportation Report datasets | Weekly US truck/rail/barge/ocean proxies; Brazil transport indicators are quarterly | **Useful, not live freight** |
| Spec/commercial positioning | CFTC Public Reporting Environment API | Tuesday positions generally published Friday 15:30 ET; aggregate classifications, no trader identities; historical disruptions occur | **Strong with lag disclosed** |
| Biofuel/energy | EIA Open Data plus EPA RFS public data | Weekly/monthly by series; key/API limits; distinguish biodiesel, renewable diesel and ethanol. EPA RIN records can change after remedial/resubmitted reports | **Strong fundamentals** |
| Macro/FX proxies | FRED API | Series-specific rights; key; FRED can change/limit/terminate API and requires attribution notice | **Useful, not an executable FX feed** |
| Global observed/forecast ag-weather | NASA POWER daily API; NOAA CDO v2; ECMWF Open Data | POWER runs 1981 to near-real-time, max 20 point parameters; NOAA token limit 5 req/s and 10,000/day. ECMWF made its real-time catalogue CC BY 4.0/no information charge in October 2025, although enhanced high-volume delivery can cost money. Forecast skill and uncertainty still need validation | **Strong public foundation, engineering-heavy** |
| Brazil crop | CONAB crop surveys, tables and soybean history | Grain/fiber survey monthly; official page says methodology targets reliability, consistency, timeliness, access, continuity and transparency | **Strong** |
| Brazil exports | MDIC/SECEX Comex Stat open CSV | Monthly, detailed by NCM/country/state/customs route, kg and FOB USD; latest raw files can lag current month | **Strong but lagged** |
| Brazil cash/basis | CONAB farmgate reports are useful; CEPEA/ESALQ and private origin assessments require separate rights review | Public display/scrape is not evidence of commercial redistribution rights; physical premiums are not a complete free API | **Partial / commercial gap** |
| Argentina official export values | Agriculture Secretariat daily official FOB prices and open-data portal; SIO Granos for registered trades | Official FOB is an administrative reference under Law 21.453, not necessarily an executable bid; endpoints are less API-friendly | **Strong reference, partial market depth** |
| China imports | GACC monthly bulletins/interactive tables | GACC warns latest monthly values may differ after verification; no stable public API established here | **Strong official fact, weak automation/latency** |
| China futures/crush proxy | DCE/CZCE official market pages can publish data, but no documented free redistribution/API entitlement established in this research | Third-party wrappers/scrapes do not inherit exchange redistribution rights | **Licensing/automation gate** |
| EU oilseed prices/flows | European Commission oilseeds dashboard/data portal | Weekly prices/customs surveillance; monthly Eurostat/COMEXT; official but slower than a live market | **Strong regional fundamentals** |
| World veg-oil reference prices | World Bank Commodity Price Data (“Pink Sheet”) | Monthly benchmark levels, not executable cash bids | **Useful context** |
| Multi-source global synthesis | FAO-hosted AMIS Market Monitor/database | Monthly synthesis and comparisons; not a trading-price feed | **Useful context** |
| Live ocean freight, vessel lineups/AIS, farmer selling, private crush bids | No complete free first-party source established | Weekly/quarterly government proxies do not replace broker/vessel intelligence | **[GAP]** |
| Fast news, broker chat, execution, position/P&L and limits | No free public API equivalent | Requires providers, brokers and internal systems | **[GAP]** |

Primary links: **[SRC]**

- [USDA WASDE](https://www.usda.gov/about-usda/general-information/staff-offices/office-chief-economist/commodity-markets/wasde-report)
- [USDA NASS Quick Stats developer portal](https://www.nass.usda.gov/developer/),
  [NASS API limit statement](https://data.nass.usda.gov/Education_and_Outreach/Meeting/2025/2025%20Spring%20Data%20Users%20Meeting%20Question%20and%20Answer%20Summary%20with%20Slides.pdf)
- [FAS databases and APIs](https://www.fas.usda.gov/data/databases-applications),
  [PSD API](https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/ui/index),
  [ESR/GATS/PSD API](https://apps.fas.usda.gov/opendata/swagger/ui/index),
  [Export Sales Reporting program](https://www.fas.usda.gov/programs/export-sales-reporting-program)
- [USDA My Market News API examples](https://mymarketnews.ams.usda.gov/mymarketnews-api/examples),
  [FGIS exports](https://fgisonline.ams.usda.gov/ExportGrainReport/default.aspx),
  [GTR datasets](https://www.ams.usda.gov/services/transportation-analysis/gtr-datasets)
- [CFTC COT/API/release caveats](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)
- [EPA Renewable Fuel Standard public data](https://www.epa.gov/fuels-registration-reporting-and-compliance-help/public-data-renewable-fuel-standard)
- [NOAA CDO API](https://www.ncdc.noaa.gov/cdo-web/webservices/v2),
  [NASA POWER daily API](https://power.larc.nasa.gov/docs/services/api/temporal/daily/),
  [ECMWF open-data status](https://www.ecmwf.int/en/about/media-centre/news/2025/ecmwf-achieve-fully-open-data-status-2025),
  [ECMWF Open Data catalogue](https://www.ecmwf.int/en/forecasts/datasets/open-data)
- [CONAB crop surveys](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras),
  [CONAB soybean history](https://www.gov.br/conab/pt-br/atuacao/informacoes-agropecuarias/safras/series-historicas/graos/soja)
- [Brazil Comex Stat raw open data](https://www.gov.br/mdic/pt-br/assuntos/comercio-exterior/estatisticas/base-de-dados-bruta)
- [Argentina agricultural open data](https://www.argentina.gob.ar/economia/agricultura/datos-abiertos),
  [official grain/FOB links](https://www.argentina.gob.ar/bioeconomia/mercados-agropecuarios-y-negociaciones-internacionales/granos)
- [China Customs monthly statistics](https://english.customs.gov.cn/statics/report/monthly.html)
- [European Commission oilseeds statistics](https://agriculture.ec.europa.eu/data-and-analysis/markets/overviews/market-observatories/crops/oilseeds-and-protein-crops_en)

---

## 4. Licensing, latency and reliability: hard commercial gates

### 4.1 Exchange price rights are the decisive gate

CME defines real-time as within 10 minutes, delayed as more than 10 minutes but less than 8 hours,
and historical as first accessed at least 8 hours after transmission. It requires licensing for
distribution, including API/feed use. Its June 2026 website policy permits only specified hourly delayed
last-trade snapshots and delays daily settlement/OHLC/volume/open-interest website publication until
07:00 CT the next day. **[SRC]**
[CME historical-information guidelines](https://www.cmegroup.com/market-data/distributor/files/cme-group-data-licensing-policy-guidelines-historical-information-distribution.pdf),
[June 2026 publication restrictions](https://www.cmegroup.com/market-data/files/market-data-license-agreement-updates-june-2026.pdf)

Consequences:

- A dashboard can be technically delayed and still require a license. **[INF]**
- A third-party library returning CME prices does not convey CME redistribution rights. **[INF]**
- “We refresh once daily” is not a licensing defense; price type, access time, use and distribution
  channel matter. **[INF]**
- CME's official settlement FAQ says delayed settlement data is available through licensed channels;
  DataMine end-of-market datasets have their own delivery cadence. **[SRC]**
  [CME settlement-data FAQ](https://www.cmegroup.com/articles/faqs/access-to-cme-group-settlement-data-faq.html)

Therefore a commercial Mirror Market launch should treat **licensed CBOT pricing as a non-waivable
dependency**, not a polish item. The same rights review is needed for DCE/CZCE, JSE/SAFEX, CEPEA and
every private origin assessment. **[INF]**

### 4.2 Public APIs have availability risk, not Bloomberg-like SLAs

- CFTC documents that COT normally reports Tuesday positions on Friday and records publication
  interruptions, including the 2025 federal appropriations lapse. **[SRC]**
  [CFTC COT and special announcements](https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm)
- FRED's terms allow it to change, suspend, limit or discontinue API access, and require applications
  to display a specified attribution/disclaimer and pass terms to users. Some underlying series have
  third-party rights. **[SRC]**
  [FRED API terms](https://fred.stlouisfed.org/docs/api/terms_of_use.html)
- NOAA CDO applies per-token request limits; NASA POWER warns repetitive whole-catalog requests can be
  blocked. **[SRC]** [NOAA CDO](https://www.ncdc.noaa.gov/cdo-web/webservices/v2),
  [NASA POWER](https://power.larc.nasa.gov/docs/services/api/temporal/daily/)
- GACC explicitly says later monthly releases may revise previously disseminated figures after
  verification. **[SRC]** [GACC monthly bulletin](https://english.customs.gov.cn/statics/report/monthly.html)

The appropriate architecture is therefore cache + immutable vintages + retry/backoff + secondary
source where legally available + freshness/coverage alarms + explicit degraded mode. **[INF]**

### 4.3 Scraping and API availability are not licenses

For every source, retain a machine-readable rights record:

`owner | dataset | acquisition method | permitted use | internal/display/redistribution | attribution |
retention | latency class | expiry/review date | evidence URL`

No documented permission means **commercial hold**, even if the endpoint is public and technically
scrapable. **[INF]** Government/open-data sources outside the United States are not automatically public
domain; follow each owner's terms and dataset-specific license.

---

## 5. Scoring rubric for Mirror Market (100 points plus gates)

A replacement score should measure whether a trader can make a correct decision on time, not count
APIs or dashboard cards.

| Dimension | Weight | Full-credit evidence |
|---|---:|---|
| Price and curve integrity | 15 | Licensed authoritative feed; contract/price type/session timestamp; coherent curves; roll QA; volume/OI |
| Physical price/origin coverage | 12 | Comparable basis/FOB/landed parity for US/Brazil/Argentina/China; grades, incoterms and shipment windows |
| Fundamentals and flows | 12 | US + Brazil + Argentina + China + world supply/use; sales/shipments/customs/crush; vintage/revision history |
| Analytics accuracy and usefulness | 12 | Transparent crush/parity/relative-value formulas, scenario analysis, validated yields/costs and backtests |
| Timeliness and event handling | 10 | Publication-aware schedules; release calendar; event alerts; no partial session bar labelled settlement |
| Reliability and observability | 10 | SLOs by source, retries/fallbacks, coverage floors, stale/degraded UI, deploy alerts, recovery tests |
| Data lineage, revisions and auditability | 8 | Raw payload hash/archive, source and observation/publication/ingest timestamps, transformation version, vintages |
| Weather/crop/logistics risk | 7 | Production-weighted observed + forecast weather, crop stage, river/freight/port signals with latency labels |
| Risk/options/position workflow | 6 | Options surface and Greeks, hedge ratios, positions/P&L/limits or explicit integration boundary |
| UX, alerts and decision compression | 4 | Trader-specific watchlists, exception-first briefing, mobile continuity, drill-through to source |
| Security and operational controls | 2 | Secrets, roles, audit logs, backups, dependency/data-contract controls |
| Licensing and claims accuracy | 2 | Rights registry and approved use for every commercial datum; honest replacement/latency claims |

### Non-compensating gates

Regardless of point score, commercial/front-office readiness is **blocked** if any is true:

1. Core displayed price lacks commercial display/redistribution rights.
2. Current/partial/last-trade data can be labelled or used as official settlement.
3. Observation, publication and ingestion times cannot be distinguished.
4. A partial upstream response can silently pass as complete.
5. A failed pipeline/deploy can leave an apparently current product without an external alert.
6. Unit, currency, grade, contract, incoterm or marketing-year semantics are implicit.
7. Material model assumptions (crush yields, freight, FX side, basis mapping) are hidden or untested.

Suggested interpretation:

- **90–100:** top-tier soy decision platform; full Bloomberg replacement only if execution/news/comms
  scope is also met.
- **75–89:** strong professional daily soy intelligence replacement; licensed terminal still needed for
  intraday prices/execution or specific gaps.
- **60–74:** valuable analyst dashboard, not yet dependable as the trader's primary decision surface.
- **40–59:** research prototype; material coverage/integrity/operations gaps.
- **<40:** cannot safely support professional trading decisions.

---

## 6. How to test the product rather than grade the brochure

Run a 30-trading-day shadow trial beside the trader's existing tools and score these tasks:

1. **Pre-open brief:** did the product identify overnight soy-complex moves, source/session and the
   two or three decision-relevant drivers?
2. **Origin switch:** can the trader compare US/Brazil/Argentina landed China economics for one real
   shipment window without leaving the product or fixing units manually?
3. **Crush/hedge:** does a change in ZS/ZM/ZL, basis or FX flow through a reproducible margin and
   hedge scenario using aligned dates/contracts?
4. **USDA event:** are pre-release expectations clearly separate from the official vintage, does the
   official print arrive at the promised latency, and is the revision/delta correct?
5. **China demand:** do export commitments, inspections, Brazil/US exports and GACC arrivals reconcile
   at their different lags without presenting false precision?
6. **Weather shock:** does an alert identify affected production, crop stage and forecast/observation
   source rather than merely display a city temperature?
7. **Failure drill:** deliberately break one key/endpoint and one deployment; confirm the product turns
   visibly degraded, retains last-good data with age, and alerts an operator.
8. **Price audit:** sample 20 contracts/market dates and compare price type, contract, settlement,
   volume and timestamp with licensed/official truth; target zero semantic mismatches.

Record: completion time, external lookups needed, wrong/stale answers, missed alerts, false alerts and
whether the trader would act on the result. The real success metric is **fewer paid-terminal lookups
without increased decision or operational risk**, not maximum source count. **[INF]**

---

## 7. Mirror Market-specific implications observed in the repo

The repository describes a daily, soy-focused, 25-source pipeline with futures, USDA/FAS/NASS,
weather, COT, PSD/WASDE, curves, DCE/CZCE, EIA, CONAB, physical spot/FOB and regional pages. It also
describes graceful degradation, freshness/coverage gates and git-persisted snapshot history. **[OBS]**
See `README.md`, `CLAUDE.md`, `ARCHITECTURE.md` and `docs/audits/2026-08-audit.md`.

That is a strong match for tier 1, daily intelligence. The existing audit already identifies the most
important external-benchmark gaps: licensing of core displayed prices; physical Argentina/ocean freight/
China customs depth; and historical incidents involving timestamp/settlement semantics, CI source
availability and silent stale deployment. **[OBS]** `docs/audits/2026-08-audit.md`.

Priority order implied by this research:

1. **Clear the commercial price-rights gate** and replace unofficial price acquisition with licensed,
   contract-identified data.
2. **Prove temporal integrity end to end:** market observation, official publication, ingestion,
   analysis and page-generation times; explicit price type.
3. **Complete the physical decision loop:** route/shipment-window origin parity, freight, China flows
   and Argentina domestic/FOB context.
4. **Run the trader shadow trial** and tune alerts/briefing around real decisions.
5. Only then decide whether to add licensed intraday/options data, news and execution integrations, or
   position the product honestly as a superior soy intelligence companion rather than a whole Terminal.

---

## Bottom line

The free/public ecosystem is good enough to build a top-tier **scheduled fundamental intelligence and
analytics layer** for soy, and specialization can make that layer more useful than a generic terminal
for recurring soy decisions. It is not good enough to produce a lawful, reliable, zero-cost clone of
Bloomberg's **real-time market data, options, news, communication and execution network**. The best
solution is therefore hybrid and explicit: make Mirror Market the trader's primary soy research cockpit,
license the narrow set of price data it truly needs, and integrate rather than pretend away the
remaining execution/news/counterparty boundary. **[INF]**
