# V2 — Publisher of record and independent access path per basket stratum

**Issue:** [#475](https://github.com/philipbergman6-glitch/Mirror-Market/issues/475) (map [#472](https://github.com/philipbergman6-glitch/Mirror-Market/issues/472); blocks #477, #478)
**Date:** 2026-10-13
**Author:** research agent (five parallel source-research subagents + synthesis)

Legend: **[O]** directly observed today (page fetched, header read, PDF text extracted, browser DOM read) ·
**[S]** from a search-result snippet or secondary page, not re-opened · **[I]** inference from O/S ·
**[NF]** not found — could not be verified; do not treat as fact. No URL or time below is invented; where a
publisher states no time, it says "not found".

The question is what the **independent publisher** offers, not what our fetchers do. Fetcher behaviour is
cited only to establish independence (what the verifier must *not* share).

---

## Method caveats that are themselves findings

- **cmegroup.com blocked this machine's IP after two curl requests** (HTTP 403: "This IP address is blocked
  due to suspected web scraping activity … strictly prohibited by CME Group's website Data Terms of Use …
  contact gcc@cmegroup.com") [O]. Every cmegroup.com fact below is from Wayback Machine 2026 captures or CME's
  Confluence client wiki. A scripted CME verifier is blocked at the edge *and* prohibited by the terms.
- **dce.com.cn** sits behind 瑞数 (RiverSecurity) JS anti-bot: curl gets HTTP 412; headless Chromium passes
  without a CAPTCHA [O]. The old `publicweb/quotesdata/*` endpoints akshare targets return HTTP 500 [O].
- **mymarketnews.ams.usda.gov** and **marsapi** time out for curl/WebFetch; a real browser works [O].
- **datos.gob.ar** returned HTTP 502 all day; metadata came from `infra.datos.gob.ar` catalog JSON [O].

---

## Stratum 1 — CBOT ZS/ZL/ZM named-contract closes (fetcher: yfinance `forward_curve`/`contract_history`)

**Publisher of record:** CME Group (CBOT). Settlement is defined by CBOT Rule 813.

| Item | Finding |
|---|---|
| Settlements web page | `https://www.cmegroup.com/markets/agriculture/oilseeds/soybean.settlements.html` (redirects to `…/soybean/settlements`); `…/soybean-oil/settlements`; `…/soybean-meal/settlements` [O, Wayback + one Playwright load]. Columns per CME "About Settlements" (Wayback 2026-09-04): Month, Open, High, Low, Last, Change, Settle, Volume, Open Interest. Lists "every contract that is eligible to trade on a particular trade date" [O]. Page is client-rendered. |
| Underlying JSON | `https://www.cmegroup.com/CmeWS/mvc/Settlements/Futures/Settlements/<productId>/FUT?strategy=DEFAULT&tradeDate=MM/DD/YYYY`; productIds **320 (ZS), 312 (ZL), 310 (ZM)** confirmed from embedded product objects [O]. Endpoint response shape **[NF]** (403 to curl; no Wayback capture). |
| What is published | **Settlement** (plus O/H/L/Last). "Settlement prices on instruments without open interest or volume are provided for web users only … not based on market activity" [O]. OI shown is the *previous* day's [O]. |
| Unit / quoting | ZS ¢/bu (Rule 11102.C, ¼¢ tick); ZL ¢/lb (12102.C, 1/100¢); ZM USD/**short** ton (13102.B "2,000 pounds per ton", 10¢ tick) — CBOT Rulebook PDFs `rulebook/CBOT/II/11/11.pdf`, `/12/12.pdf`, `/13/13.pdf` [O]. Daily Bulletin prints ZS in eighths (`1297'4` = 1297.50) [O]. |
| Publication time | Settlement window grains/oilseeds **13:14:00–13:15:00 CT**; "Settlement details are available on the cmegroup.com website at midnight, 12:00 a.m. CT" (Confluence "Daily Settlement Time Details", updated 2025-01-03) [O]. Daily Bulletin: Preliminary ~00:00 CT T+1, **Final 10:00 a.m. CT T+1** [O]. Preliminary-vs-final flag on the web settlements page: [NF]. |
| Shows obs. date + month | Yes — per trade date, every month [O]. |
| Format | HTML (JS) + JSON (gated); **Daily Bulletin Section 03 PDF** `https://www.cmegroup.com/daily_bulletin/current/Section03_Agricultural_Futures.pdf` — header "FINAL … AGRICULTURAL FUTURES Wed, Oct 07, 2026", columns Globex O/H/L, Sett.Price & Pt.Chge, RTH/Globex volume, OI, year-ago [O]. |
| History depth | Web dropdown depth [NF]. Bulletin and settlement files: "available top day only … overwrite the prior day's files. Historical … can be purchased from CME DataMine" (Settlements FAQ, 2025-06-17) [O]. FTP `/settle` removed (Chadv23-352) [O/S]. Wayback holds captures of the current-bulletin URL (e.g. 2026-10-09) [O]. |
| Terms on public delta/pass-fail | Website Data Terms (last updated 2023-09-07): "access content only for your personal use for non-commercial purposes"; prohibits "publicly displaying, publishing … creating derivative works, electronically extracting or scrubbing"; bots prohibited [O]. DS-45 (June 2026): website display of settlements under a Public Website licence is embargoed until **07:00 CT T+1 from 2027-01-01**; "there are no circumstances in which Delayed Information or Historical Information may be deemed to have entered the public domain"; ILA §1.2(c) prohibits "analytical reference figures or values calculated from Information" [O]. Fee list 2026: "Public Website — $487" (A5 memory: per month) [O]. **[I] No carve-out for a public match/delta statement; a delta plus our value reconstructs the settlement. Compliant routes named by the documents: licence or written consent (marketdata@cmegroup.com).** |
| Free machine-readable path sharing nothing with Yahoo | **USDA AMS daily grain reports** republish CBOT **ZS** settlements with month: e.g. AMS 3046 Minneapolis `https://www.ams.usda.gov/mnreports/ams_3046.pdf` — "Closing Settlement Prices (¢/bu) as of 10/12/2026: 1287.75 (Nov 26) 1304.75 (Jan 27) … 1272.25 (Sep 27)" [O]; cross-check AMS Illinois 10/07 Nov26 1297.50 = Bulletin FINAL `1297'4` ✓ [O]. Public domain. **No AMS table of ZL/ZM settlements found [NF]** — AMS 3511 carries only cash/basis. Barchart ZSX26 price-history (settlement-suffixed, ~2 yrs) is independent of Yahoo but JS/paywall-ish [S]. Yahoo itself is a CME licensee via ICE Data Services, 10-min delayed, "Close" not labelled settlement [O]. |

**Verdict:** Tier 1 reachable only by a **human reading the Daily Bulletin / web page** (terms forbid scripting) and only for the current day unless captured daily. **ZS** has a free, public-domain, independent machine path (AMS PDFs). **ZL/ZM have no free independent machine-readable settlement path found.**

---

## Stratum 2 — DCE a/b/m/y closes (fetcher: akshare continuous A0/B0/M0/Y0 via Sina, no contract month)

**Publisher of record:** Dalian Commodity Exchange.

| Item | Finding |
|---|---|
| Page | zh: `http://www.dce.com.cn/frontend/dcereport/#/zh/dayFuturesQuotation?variety=m&tradeType=1` (iframed from `/dce/channel/list/168.html`); en: `…/#/en/dayFuturesQuotation?variety=m&tradeType=1` [O]. The English nav page still iframes the dead `publicweb/quotesdata/dayQuotesEn.html` (HTTP 500) [O]. `https://` timed out; `http://` works [O]. |
| JSON API | `POST http://www.dce.com.cn/dcereport/publicweb/dailystat/dayQuotes`, headers `content-type: application/json`, `clientid: web`, body `{"varietyId":"m","tradeDate":"20261013","tradeType":"1","contractId":"","lang":"en","optionSeries":"","statisticsType":0}` → rows with `contractId, open, high, low, close, lastClear, clearPrice, diff, diff1, volumn, openInterest, turnover` [O]. Works only from inside the page context (瑞数 token). |
| What is published | **Both close and settlement** (`收盘价` / `结算价`), per named month (e.g. `m2701 close 3361 clearPrice 3373` on 2026-10-13) [O]. |
| Unit | 元(人民币)/吨 — product pages `/dce/channel/list/120.html` (a), `121` (b), `122` (m), `123` (y) [O]; page note "价格：元/吨" [O]. |
| Publication time | "每个交易日结算完成后更新数据" (after settlement each trading day); day session closes 15:00 CST; night 21:00–23:00 for a/b/m/y [O]. **Wall-clock time: not found.** Trap: after 21:00 Beijing `maxTradeDate` already returns the *next* trading day with an empty table — always query an explicit date [O]. |
| Shows obs. date + month | Yes — "查询日期：YYYYMMDD" and contract codes; note (9) "合约后四位数字即为交割月份" [O]. |
| Format | HTML (Vue SPA); client-side export `导出文本` (tab-separated txt) / `Csv(.csv)`; yearly bulk xlsx/zip via `/frontend/dcereport/#/zh/datadownload?year=2026&type=1&variety=all` (years 2006–2026; sheet `日历史行情`, columns incl. 收盘价, 前结算价, 结算价) [O]. Bulk file caveats: numbers are strings with thousands separators; volume is 双边 vs daily page 单边; turnover CNY vs 万元 [O/I]. |
| History depth | API answered 20050104 (`m0501 settle 2298`) [O]; bulk 2006→ [O]. |
| Terms | 法律声明 2025-02-28 clause 三: "任何机构或者个人可基于非商业目的浏览、下载本网站的内容。未经…书面许可，任何机构或者个人不得以向他人出售牟利为目的，使用本网站的任何内容…" [O]. English disclaimer mirrors it; Chinese prevails [O]. Deep-linking rule: links must point to the homepage, text-only [O]. **[I] A non-sold pass/fail or delta is not within the enumerated prohibition (all conditioned on sale for profit); publishing DCE values in a commercial edition is the risk. Not legal advice.** |
| Free machine path sharing nothing with Sina | **Eastmoney** `https://push2his.eastmoney.com/api/qt/stock/kline/get?secid=114.m2701&fields1=f1,f2,f3,f4,f5,f6&fields2=f51,f52,f53,f54,f55,f56,f57&klt=101&fqt=0&end=20500101&lmt=3` → `2026-10-13,3390,3361,3395,3356,973868,32859043584` — O/H/L/C/volume identical to DCE, turnover matches (×10⁴) [O]. **No settlement field**; serves a partial next-day bar during the night session (settlement-guard trap) [O]. DCE's own API via headless browser is the Tier-1 path. |

**Verdict:** Tier 1 fully reachable (named months, both close and settle, deep history) — but **only through a JS-capable browser**, and the comparison exposes the basket's weakness: our series has no contract month, so the month must be inferred (map "Named DCE contracts" item).

---

## Stratum 3 — Brazil: CEPEA indicators + AgRural Paranaguá (fetchers: noticiasagricolas.com.br scrape; agrural.com.br scrape)

### 3a. CEPEA/ESALQ Paraná and Paranaguá indicators — publisher of record: CEPEA (Esalq/USP)

| Item | Finding |
|---|---|
| Page | `https://www.cepea.org.br/br/indicador/soja.aspx` — two tables `INDICADOR DA SOJA CEPEA/ESALQ - PARANAGUÁ` and `… - PARANÁ`; columns date, `Valor R$*`, `Var./Dia`, `Var./Mês`, `Valor US$*`; footnote "Reais por saca de 60 kg."; 15 dated rows server-rendered; **no Cloudflare challenge** on this page (HTTP 200 to python-requests UA) [O]. (CEPEA does not use the "ESALQ/B3" label; that is Notícias Agrícolas' naming [O].) |
| Full-history download | `https://www.cepea.org.br/br/indicador/series/soja.aspx?id=12` (Paraná, **7,275 rows 1997-07-29→2026-10-09**) and `?id=92` (Paranaguá, **5,132 rows 2006-03-13→2026-10-09**) — HTTP 200, `application/vnd.ms-excel`, no login, columns `Data | À vista R$ | À vista US$` [O]. xlrd needs `ignore_workbook_corruption=True` [O]. Only `consultas-ao-banco-de-dados-do-site.aspx` is Cloudflare-challenged (403 `cf-mitigated: challenge`) [O]. |
| What is published | Daily **indicator** (not a trade, not a settlement) in R$/sc and US$/sc; US$ converted at "taxa de dólar comercial de venda das 16h30" [O]. |
| Unit | saca de 60 kg — stated on methodology pages (`metodologia-soja-cepea-esalq-parana.aspx`: "Unidade de medida: saca de 60kg (R$/sc)"; Paranaguá: "Dólares americanos por saca de 60 kg") and indicator footnote [O]. R$/MT = R$/sc × 16.6667 [arithmetic]. |
| Publication time | Paranaguá methodology PDF (`cepea.org.br/upload/kceditor/files/paranagua(1).pdf`, mirrored on B3): collection 09:00–17:00 BRT, closing 17:00–18:00, "divulgado … após as 18h01, hora oficial de Brasília" [O]. **Paraná indicator time: not found** [NF]; same team/page, likely ≥18:00 BRT [I]. Notícias Agrícolas shows "Última atualização: 13:08 (09/10)" for a CEPEA block — timing relation unresolved [NF]. |
| Shows obs. date | Yes, per row [O]. Contract month n/a (spot). |
| Format | HTML + XLS (BIFF). |
| Terms | Política de Direitos Autorais: "É obrigatória a citação do … (Cepea/Esalq) como fonte"; "Interessados em retransmitir conteúdo … devem solicitar autorização formal"; "Não é permitida … a transmissão de séries de preços" [O]. Licença de uso: **CC BY-NC 4.0** — commercial use requires written authorisation [O]. **[I] A single dated delta/pass-fail with "Fonte: Cepea/Esalq" on a non-commercial public page fits; the series does not; a paid desk edition is commercial (A18).** |
| Independent machine path | CEPEA's own xls endpoints (above) share nothing with Notícias Agrícolas [O]. Other mirrors: B3 has no current public page carrying the indicator (BDI PDFs contain no "esalq"/"paranagu") [O]; Canal Rural/Brasil61 prose only [S]; Agrolink quotes a different number and disclaims auditing [O]. |

### 3b. AgRural "Paranaguá FOB" — publisher: AgRural (private consultancy)

| Item | Finding |
|---|---|
| Page | `https://agrural.com.br/precossojaemilho/` (`/cotacoes/` is 404) — table `Estado | Praça | Compra | Variação hoje | 1 semana | 1 mês`, date label `9-Oct-26`, row `PR | Paranaguá | 154,00 | -1,00 | -4,3% | -4,0%` [O]. |
| What is published | Footnote verbatim: "Em R$ nominais por saca de 60 kg. Valores brutos, FOB praças, 'sobre rodas' portos e CIF onde indicado." → an **R$/sc buyer bid (Compra), gross, "sobre rodas" at the port** — FOB-*praça*, **not** FOB-vessel USD and **not** a ¢/bu premium [O]. |
| Time | None shown [O]. |
| Format / history | HTML (WordPress); current table only; no series download [O]. |
| Terms | Aviso Legal: "É permitida a citação pontual de dados … por veículos de imprensa"; "Fica vedada … a reprodução total ou parcial das tabelas de preços … salvo mediante autorização prévia e por escrito" [O]. **[I] Daily republication of a row is "reprodução parcial"; a press-style single citation is the only carve-out.** |
| Independent source for a Paranaguá premium | **None is a publisher of record.** Notícias Agrícolas "Prêmio Soja Paranaguá" (US$/bu by month, no source line) [O]; Sistema FAEP republishes **CMA** premiums in ¢/bu by month (e.g. NOV.26 135→140) with "Última atualização: 13/10/2026 10:30:00" [O] — differs from NA's by ~15–25 ¢/bu same day [O]. CONAB/Safras/T&F: no free table [NF]. |

**Verdict:** CEPEA = genuine Tier 1 with a free, deep, server-side XLS path. **AgRural FOB has no publisher of record and is mislabelled as FOB** if stored as a vessel price — this is a basket-definition finding, not a verification result.

---

## Stratum 4 — MAGyP official FOB (fetcher: `monitorsiogranos…/ws/ssma/precios_fob.php` JSON)

**Publisher of record:** Subsecretaría de Mercados Agroalimentarios e Inserción Internacional (SAGyP, Ministerio de Economía), under Res. SAGyP 65/2025 (signed Sergio Iraeta) implementing Ley 21.453 / Decreto 1.177/1992 [O: `https://www.argentina.gob.ar/normativa/nacional/norma-412287/texto`].

| Item | Finding |
|---|---|
| Daily instrument | A numbered **circular** (e.g. `DIM_Precio Fob_NCirc2077` for 2026-10-09, with "N° de Comunicación" 189), published via DINEM, not the Boletín Oficial [O]. Valid "para las operaciones con fecha de cierre de venta concretadas en el día de su publicación" (Art. 2); business days only [O]. |
| HTML of record | Index `https://dinem.magyp.gob.ar/dinem_fob.wp_fob_consall.aspx` → `dinem_fob.wp_fob_conslista.aspx?YYYYMMDD,<circular>,<comunicación>,1` — ASP.NET; columns **Producto, Descripción, Precio (Dls/Ton), Emb.Desde, Emb.Hasta**, "Exportar Información" [O header; S columns — table is postback-rendered]. Landing: `https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/fob_oficiales/` [O]. |
| JSON service | Documented at the landing's "Precios Fob Api" page: primary `https://monitorsiogranos.magyp.gob.ar/ws/ssma/precios_fob.php?Fecha=dd/mm/aaaa` (**HTTP 403 today**) and alternate `https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios/ws/ssma/precios_fob.php?Fecha=dd/mm/aaaa` (worked) → `{"posts":[{"fecha":"2026-10-09 00:00:00.000","circular":"2077","posicion":"10011900110H","precio":289,"mesDesde":10,"añoDesde":2026,"mesHasta":10,"añoHasta":2026},…]}` — no product name, no unit field, time 00:00 [O]. |
| What is published | **Administered official FOB** (USD/t) per NCM/SIM position and shipment window — not a traded price. |
| Unit | "Dls/Ton" on the HTML; "Dolares por tonelada" on datos.gob.ar [O/S]. |
| Publication time | **Not found** in any norm or page [NF]. |
| Shows obs. date + window | Yes (date, Emb.Desde/Hasta) [O/S]. |
| Format | HTML (postback) · JSON · datos.gob.ar CSV. |
| History depth | DINEM index lists historical circulars ("Actual y Series Históricas") [O]; "Precios por Producto y por Mes" page `dinem_fob.WP_ConsProdf.aspx` [O]. |
| Terms | argentina.gob.ar: contents CC BY 4.0 "excepto cuando se declare lo contrario" [S]; datos.gob.ar dataset 358 licence "Creative Commons Attribution 4.0" [O]; magyp.gob.ar/DINEM/JSON: **no licence text found** [O]. Public delta/pass-fail with attribution is within CC BY for the mirror; for the live service, unlicensed-by-text but low risk [I]. |
| Tier 2 mirror (datos.gob.ar sspm 358) | CSV `https://infra.datos.gob.ar/catalog/sspm/dataset/358/distribution/358.1/download/precios-fob-oficiales-valores-diarios.csv` — 59 named product columns (e.g. `aceite_soja_granel`, `habas_soja_demas_granel_hasta_15_porciento_embolsado`, `tortas_expellers_pellets_harina_soja_…`), first row **1993-01-04**, **last row 2025-01-21** — the "R/P1D" dataset has been **stale ~9 months** [O]. Collapses the shipment-window dimension to one value per product/day (which window is undocumented) [I]. Dataset page itself HTTP 502 today [O]. |
| Other republishers | BCR "Precios FOB/FAS Argentina" has a "MAGyP" column, spot only, no oil/meal MAGyP column; page showed 23/10/2025 (stale/cached) [O]; Bolsa de Cereales BA "Precios Oficiales" by position Oct26…Sep27 incl. soja/aceite/pellets — **403 bot challenge**, snippet only [S]; Agrofy shows value without date/position [O]; CIARA-CEC: not found [NF]. |

**Verdict:** Tier 1 reachable (DINEM HTML, a human can read it; JSON alt host works today but the primary 403s). The planned **Tier 2 datos.gob.ar mirror is dead since 2025-01-21** — useful for backfill cross-checks only. The HTML path is independent of the JSON service in rendering but **[I] almost certainly the same database**, so it is a transport check, not a Tier 2.

---

## Stratum 5 — US Gulf CIF basis vs AMS 3147 (fetcher: `ams.usda.gov/mnreports/ams_3147.pdf` daily PDF; MARS API backfill)

**Publisher of record:** USDA AMS Livestock, Poultry & Grain Market News (Portland, OR office).

| Item | Finding |
|---|---|
| Pages | `https://mymarketnews.ams.usda.gov/viewReport/3147` (Slug AMS_3147, Market "Grain - Gulf Export", Point of Sale) [O]; fixed-URL latest PDF `https://www.ams.usda.gov/mnreports/ams_3147.pdf` [O]; dated PDFs `mymarketnews.ams.usda.gov/filerepo/sites/default/files/3147/YYYY-MM-DD/<id>/ams_3147_NNNNN.pdf` (prelim `_pre01`) [O/S]; Public Data `https://mymarketnews.ams.usda.gov/public_data?slug_id=3147` [O]; legacy `jo_gr112.txt` frozen at 2020-08-14 [O]. |
| What is published | Export **bids**: `Basis (¢/Bu)` over a named CBOT month (`86.00X to 87.00X`), `Price($/Bu)`, `CIF-B`, "US #1 Soybeans (Bulk)", "Gulf Coast Ports - LA"; month-code legend printed; "Grain Report for 10/12/2026 - Final"; also "Closing Settlement Prices (¢/bu) as of 10/12/2026" [O]. |
| Publication time | **No AMS-stated clock time** [NF]. Evidence: legacy text "Bids as of 2:00 Central time"; file-list stamps 17:47–18:27 vs Public Data "Published Date 10/12/2026 13:27:12" ⇒ [I] stamps are UTC, publish ≈12:45–13:30 CT (matches repo's 12:48:13 observation). |
| Shows obs. date + month | Yes, both, plus Final flag [O]. |
| Format | PDF; API JSON/CSV; Public Data HTML (`ajax-search-data-by-report/3147?q=report_begin_date=…`) [O]. |
| History depth | viewReport "Previous Releases" back to **2017-10-02** (TXT until Aug 2020, PDF after) [O]; Public Data years 2020–2026 [O]; ESMIS archive `https://esmis.nal.usda.gov/concern/publications/9z902z86w` to 2025-08-26 then **stale** [O]. |
| Terms | USDA: "Most information … is considered public domain … may be freely distributed or copied, but use of appropriate … credits is requested. Attribution … 'U.S. Department of Agriculture.'" [S; identical text verified on fsa.usda.gov [O]]. MPR DataMart: "There are no restrictions on who may obtain this data or how it may be used … if … identified as USDA data, it cannot be altered" [O]. Public delta/pass-fail: permitted. |
| Independent path vs our PDF fetch | **MARS API** `https://marsapi.ams.usda.gov/services/v1.2/reports/3147` (free key via eAuth, Basic auth key-as-username; unregistered sample key 5,000 rows; no rate limit stated) [O]. **Caveat [I]:** Public Data page proxies the same MARS store — different transport, not a different data store. PDF vs API diverged once (2026-07-30 prelim reissue, LAYERS.md L87). |
| Independent of AMS altogether | Reuters daily "CIF/FOB Gulf Grain" (trader-sourced; 2026-10-08 "86 cents over November" vs AMS "86.00 to 87.00 … November") — free via aggregators, body paywalled on TradingView [S]. GTR weekly republishes AMS [O]. Platts/DTN/Barchart Gulf basis paid [S]. |

**Verdict:** Tier 1 reachable, public domain, machine-readable via MARS (independent *transport* from the PDF). Fully independent Tier 2 (Reuters) is human-read only.

---

## Stratum 6 — FX BRL/CNY/INR/ZAR (fetcher: Yahoo `USDBRL=X` etc., 17:00 NY close) — Tier 2 only by definition

**No publisher of record exists for a spot FX "close".** Candidate reference-rate publishers:

| Publisher | Coverage | Time | Machine path | Terms | Status |
|---|---|---|---|---|---|
| **ECB euro reference rates** `https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html` | BRL, CNY, INR, ZAR all present (2026-10-12: 5.5819 / 7.4832 / 107.0860 / 18.4945) [O] | Concertation ~14:10 CET, published ~16:00 CET; SDMX title says "2.15 pm (C.E.T.)"; not on TARGET closing days (2026: 1 Jan, 3 Apr, 6 Apr, 1 May, 25–26 Dec) [O] | `…/stats/eurofxref/eurofxref-daily.xml`; `eurofxref-hist.zip` (CSV 1999-01-04→; ZAR 1999, CNY 2005-04-01, BRL 2008-01-02, INR 2009-01-02); `https://data-api.ecb.europa.eu/service/data/EXR/D.BRL.EUR.SP00.A?format=csvdata` (flaky: 503 ×4 to WebFetch, OK to curl) [O] | "users … may make free use of the information … the ECB must be cited as the source"; modified data must be stated [O] | **Usable Tier 2.** USD cross = BRL/EUR ÷ USD/EUR from the same snapshot [I]. Snapshot is **~8h45m before** Yahoo's 17:00 NY — tolerance must absorb intraday vol; label "different-snapshot check" [I]. |
| Fed H.10 `https://www.federalreserve.gov/releases/h10/current/` | BRL, CNY, INR, ZAR [O] | Noon NY buying rates; **weekly** release Mondays 16:15 ET [O] | DDP CSV (DDP being retired → FRED DEXBZUS etc.; FRED CSV not fetched [NF]) [O/S] | US govt | Weekly lag; usable for recurring sample, not daily. |
| BCB PTAX (BRL) | BRL | ≈13:10 BRT closing PTAX [O] | `https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/CotacaoDolarPeriodo(…)?$format=json` [O] | ODbL [O] | Usable. |
| PBOC/CFETS central parity (CNY) | CNY | 09:15 Beijing (银发[2014]18号) [O] | chinamoney.com.cn JS-rendered; **no download URL found** [NF] | — | Human-read only. |
| FBIL (INR) | INR | ~13:00 IST since 2025-01-06 [O] | fbil.org.in JS; no API/CSV verified [NF]; NSE mirror [S] | — | Human-read only. |
| SARB (ZAR) | ZAR | **time not stated** [NF] | JS form, no static URL [O] | Disclaimer: content "may not be copied, reproduced, adapted, published or distributed … without the prior written consent" [O] | **Not usable** for a published check. |

**Verdict:** ECB is the one source covering all four with a free machine path and permissive terms; it is a *different observation*, not the same number — a tolerance is a design decision for the prototype ticket.

---

## Stratum 7 — Board crush (CBOT named) + DCE crush — Tier 3 + legs

No publisher publishes a crush margin of record. Legs: stratum 1 (CME ZS/ZL/ZM) and stratum 2 (DCE b/m/y). Tier 3 recomputation is pure arithmetic on verified legs; the stratum inherits **every** limitation above — notably that ZL/ZM have no free independent machine path, and DCE legs need a browser. CME ILA §1.2(c) names "ratios, curves … calculated from Information" as prohibited derived use for licensed data [O] — relevant to publishing a DCE-vs-CBOT crush delta, and [I] to the board-crush verification display.

## Stratum 8 — One landed origin→destination row — Tier 3 against the entered assumption stack

Legs: an FOB (stratum 3/4/5), FX (6), ocean freight + elevation + financing (**desk-entered, gitignored** — no publisher by design, `data/reference/assumptions/README.md`), and China duty/VAT (committed `china_import_policy.yml`).

| Leg | Publisher of record | Finding |
|---|---|---|
| Ocean freight, elevation, financing | None (Baltic/Platts licensed; "nobody publishes a NOLA elevation spread") [O: repo README] | **Unverifiable** by construction; Tier 3 recomputation only. |
| China MFN duty 3% / import VAT 9% | 国务院关税税则委员会 (Customs Tariff Commission) — 2026 plan 税委会公告2025年第11号 `https://gss.mof.gov.cn/gzdt/zhengcefabu/202512/t20251229_3980625.htm` (attachments: 进口商品暂定税率表 etc.) [O: search + page summary]; 9% VAT for agricultural goods (FAS GAIN CH2026-0016 context) [S] | **The repo's yml cites only itself** ("source: repo-established (config.MARKETS…)", confidence indicative) — no publisher citation [O]. The 1201 line's 3% MFN in the 2026 schedule was **not verified** here [NF]. |
| US-origin additional duty | Customs Tariff Commission announcements (Nov 2025) | Press (Reuters via FMT/Tribune/Farm Policy News, 2026-09-28): US soybeans **still carry an additional 10%** (≈13% total); excluded from the Sept 2026 tariff-cut list [S]. **Not modelled** in the committed file (header says so) — a US Gulf→China landed row today understates duty unless an origin-scoped entry exists [O/I]. |

**Verdict:** Partially verifiable at best — the only public legs are policy rates whose official line items need a citation added; the freight stack is unverifiable by design and must be published as such.

---

## Stratum 9 — Cash oil/meal vs AMS 3511 (fetcher: MARS API `Report Detail`)

**Publisher of record:** USDA AMS (Minneapolis office).

| Item | Finding |
|---|---|
| Pages | `https://mymarketnews.ams.usda.gov/viewReport/3511` — prints "This Report: https://marsapi.ams.usda.gov/services/v1.2/reports/3511?q&allSections=true"; dated PDFs `filerepo/sites/default/files/3511/2026-10-05/1357413/ams_3511_00234.pdf`; Public Data `?slug_id=3511` [O]. Fixed `mnreports/ams_3511.pdf`: **not verified to exist** [NF]. |
| What is published | Processor **ask** ranges: "Soybean Meal 46.5-48%" `Basis ($/Ton)` / `Price ($/Ton)` e.g. `Indiana-Ohio Ask 30.00V to 50.00V … 298.60-318.60 … FOB-T`; "Soybean Oil" `Basis (¢/Lb)` / `Price (¢/Lb)` e.g. `Illinois Ask -25.00V UNCH 49.49` [O]. "$/Ton" **unqualified** on the document (short ton is the repo's arithmetic inference); oil basis printed as ¢/Lb though it behaves as points (LAYERS.md L98) — a verifier must apply the same reading or it will "fail" every row. |
| Time | Weekly, "Published on: Fri"; file stamps ~18:44–18:47 ⇒ [I] ~13:45 CT Friday; **no stated time** [NF]. |
| Shows obs. date + month | Yes: "Report for 9/22/2025 - 9/26/2025", month codes per quote [O]. |
| Format | PDF, API JSON/CSV, Public Data HTML [O]. |
| History | Weekly PDFs from **2022-02-07** (merger note) [O]; ESMIS `publications/qb98nh77f` to 2025-09-26 then stale [O]. |
| Terms | Public domain (as stratum 5). |
| Independent path vs our API fetch | The **dated PDF** on the filerepo (rendered document) — different artefact, same AMS origin [O]. Public Data HTML proxies MARS [I]. |
| Independent of AMS | **None free found** [NF] (Jacobsen paid). |

---

## Summary table

| # | Stratum | Publisher of record | Independent free machine path (shares nothing with our fetcher) | Pub. time stated? | Terms allow public delta/pass-fail? | Tier reachable |
|---|---|---|---|---|---|---|
| 1 | CBOT ZS/ZL/ZM closes | CME Group (Rule 813) | ZS: USDA AMS daily grain PDFs (public domain). ZL/ZM: **none found**. CME page/JSON/Bulletin: scripting prohibited + IP-blocked | Yes: web 00:00 CT; Bulletin Final 10:00 CT T+1 | **No carve-out**; Public Website licence $487 (7 AM CT embargo from 2027) or written consent | T1 human-read (today only); T2 machine for ZS only |
| 2 | DCE a/b/m/y | Dalian Commodity Exchange | DCE JSON via headless browser (close + settle, named months, 2005→); Eastmoney kline (OHLC only, no settle) | Only "after settlement"; clock [NF] | Non-commercial use permitted; sale-for-profit prohibited; delta not addressed | T1 (browser), with month-inference weakness on our side |
| 3a | CEPEA Paraná / Paranaguá | CEPEA/Esalq | `series/soja.aspx?id=12\|92` XLS, 1997/2006→, no login | Paranaguá: after 18:01 BRT; Paraná [NF] | CC BY-NC + mandatory "Fonte: Cepea"; series retransmission forbidden; commercial needs consent | T1 |
| 3b | AgRural Paranaguá "FOB" | AgRural (consultancy; not a benchmark) | None; number is an R$/sc port bid, not FOB-vessel | None shown | Press-citation only; table reproduction forbidden | **Unverifiable** (no publisher of record for a Paranaguá premium) |
| 4 | MAGyP FOB oficial | SAGyP Subsecretaría (Res. 65/2025) | DINEM HTML (postback) · alt-host JSON · datos.gob.ar CSV (**stale since 2025-01-21**) | [NF] | CC BY 4.0 (argentina.gob.ar/datos.gob.ar); magyp.gob.ar no text | T1 human/HTML; T2 mirror dead |
| 5 | Gulf CIF basis (AMS 3147) | USDA AMS | MARS API vs PDF (same store, different transport); Reuters column (human) | Not stated; ≈12:45–13:30 CT inferred | Public domain, attribution requested | T1 |
| 6 | FX BRL/CNY/INR/ZAR | none (reference rates only) | ECB XML/CSV/SDMX (all four; 14:15 CET snapshot); PTAX OData (BRL); Fed H.10 weekly | ECB yes (~16:00 CET) | ECB: free with citation; **SARB forbids** | T2 (different snapshot) |
| 7 | Board + DCE crush | none | inherits 1 & 2 | — | CME ILA names ratios as derived use | T3 + legs (ZL/ZM gap) |
| 8 | Landed row | none for freight; Tariff Commission for duty/VAT | gss.mof.gov.cn 2026 plan (PDF attachments); 1201 line unverified | n/a | gov publication | T3 only; freight unverifiable by design |
| 9 | Cash oil/meal (AMS 3511) | USDA AMS | dated filerepo PDF vs API; no non-AMS source | Not stated; Fri ≈13:45 CT inferred | Public domain | T1 (transport-independent) |

## Strata with no reachable independent source (the *unverifiable* finding)

1. **3b AgRural Paranaguá "FOB"** — no publisher of record exists for a Paranaguá FOB premium; every free quote is a broker indication (AgRural, CMA via FAEP, Insoy via NA) and they disagree by 15–25 ¢/bu on the same day. The stored number is an R$/sc "sobre rodas" bid, not FOB-vessel. Recommend the basket treat this stratum as *unverifiable* or replace it with CEPEA Paranaguá (verifiable) and label the premium gap.
2. **1 (ZL/ZM part)** — CME is the only publisher; its terms prohibit scripted reading and public derived figures, no free third party republishes ZL/ZM settlements by month. Verifiable only by a human reading the Daily Bulletin the same day, and the publishability of even a pass/fail is undetermined without CME's consent.
3. **8 freight/elevation/financing legs** — desk-entered by design; no publisher. Tier 3 only.
4. **6 ZAR via SARB** — publisher terms forbid reproduction; ECB remains the only usable ZAR reference.
5. **9 / 5 beyond AMS** — no free non-AMS publisher of processor cash oil/meal; Gulf basis has Reuters (human-read).

## Facts not found (explicit)

- Clock time of: DCE 日行情 publication; MAGyP daily circular; AMS 3147 and 3511 releases; CEPEA Paraná indicator; SARB rates.
- CME settlements web dropdown history depth; CmeWS JSON response shape; prelim/final flag on the web page.
- MAGyP JSON product-name mapping; Bolsa de Cereales positions table (403); CIARA-CEC republication.
- Existence of `ams.usda.gov/mnreports/ams_3511.pdf`; FRED CSV endpoints for DEXCHUS/DEXINUS/DEXSFUS.
- CFETS central-parity download URL; FBIL API.
- China 2026 tariff schedule line for 1201 soybeans at 3% MFN (official PDF not opened).
- DCE `/dcereport/quote/history/download` query-parameter names.

## Implications handed to #477 / #478

- A verifier script is realistic for: CEPEA (XLS), MAGyP (alt-host JSON or DINEM HTML), AMS 3147/3511 (MARS vs PDF), ECB (XML/CSV), PTAX (OData), DCE (headless browser only), AMS-republished ZS settlements.
- A human is required for: CME ZL/ZM (and ZS Tier 1), Reuters Gulf column, CFETS/FBIL, any CEPEA/AgRural series statement.
- Licence exposure (A18 #468): CME (no carve-out; derived figures named), CEPEA (BY-NC, series ban), AgRural (table ban), SARB (reproduction ban). DCE and all US/Argentine government sources are permissive.
- Two planned Tier 2 paths are weaker than charted: datos.gob.ar is stale; AMS Public Data/API share one store with the PDF.
- Basket definition issues surfaced: AgRural is not FOB; `china_import_policy.yml` has no publisher citation and omits the live 10% US-origin duty.
