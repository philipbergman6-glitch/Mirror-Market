# Euronext MATIF rapeseed (ECO): a free, keyless, machine-readable daily path for private desk use

- **Date:** 2026-11-05 (every URL fetched this date; clock stamps are UTC)
- **Owner decision under test:** "Ingest MATIF delayed settlements, private edition only" (2026-11-05)
- **Prior note:** `research/2026-08-10-issue-148-matif-rapeseed.md` (#148) — found the paid tiers (€167.55/month delayed redistribution), found that `live.euronext.com` is terms-protected, and did not complete a MiFIR trades-file download.
- **Question:** is there a path to ECO daily prices *by contract month* that is free, needs no key, is machine-readable, and is not behind a login or a terms-prohibited route?

**Evidence tiers:** **[D]** directly observed today · **[I]** inference from observed data · **[A]** assumption or secondary source · **[?]** could not verify.

---

## Verdict (up front)

**A free, keyless, machine-readable, Euronext-own path exists — but it carries trades, not settlements.** Euronext's MiFIR Article 13 delayed post-trade file for Paris commodity derivatives downloads as a zip of CSV with no key and no login, under Euronext's own "Terms and Conditions for Delayed Trade Data", whose clause 1 says the right to *Use* is free of charge and unrestricted except for distribution. Each ECO contract month's trades are in it, identified by ISIN, and the ISIN resolves to "Rapeseed / Colza FUTR PHYS <expiry month>" plus an expiry date through ESMA's public FIRDS register, also keyless. Measured publication delay: about 15 minutes.

**The daily settlement price (DSP) itself is not on that path.** The DSP renders only on `live.euronext.com` through an XHR route (`POST /en/ajax/getTabPrices/ECO-DPAR/...`) that the Euronext Terms of Use forbid reaching by "spiders, robots, ... tools or other devices" and by "systematic retrieval to create ... databases". That route was located and is documented below but was **not** scraped and must not be built against.

So what can honestly be built is **Layer 37: per-contract-month session aggregates of on-book trades — open, high, low, last, VWAP, lots, count — in EUR/t, labelled `board_last_traded`, never a settlement.** This matches the SAFEX precedent (Layer 18: Grain SA last-traded, never JSE MTM). It is a departure from the owner's word "settlements", recorded as an assumed decision in the PR.

---

## RQ1 — Euronext's own surfaces

### 1a. `live.euronext.com` product pages: a JS shell, antibot, encrypted quote route — and a terms prohibition

**[D]** `GET https://live.euronext.com/en/product/commodities-futures/ECO-DPAR` → 200, 248,564 bytes, `text/html`; `.../ECO-DPAR/settlement-prices` → 200, 240,736 bytes. Both open with Drupal `antibot` markup and contain no price data: the only "settlement" strings in the HTML are the route name, `settlementFilter` and `settlement-prices` link variants.

**[D]** `POST https://live.euronext.com/en/ajax/getDetailedQuote/ECO-DPAR` → 200, 246 bytes, `application/json`, body `{"ct":"...","iv":"...","s":"..."}` — an AES envelope, as in August.

**[D]** `POST /en/ajax/getPriceList/ECO-DPAR` → 404. `GET /en/ajax/getFactsheetInfoBlock/COMMODITIES_FUTURES/ECO-DPAR/{fs_settlement_prices_block,fs_quote_block,fs_derivatives_settlement_block,fs_contracts_block}` → 200 with **0 bytes** each.

**[D]** The real route, read from the page's own bundle `js_h162G7pZqYzBO1ZGLQ09IAaPcIj9RbjnvzBx8pSeQ98…js` (source credit in the bundle: `modules/custom/awl_settlement_prices_page/js/awl-settlement-prices-page.js`):

```
jQuery.ajax({type:"POST",
  url:"/"+drupalSettings.path.pathPrefix+"ajax/getTabPrices/"+drupalSettings.instrument+"/"+drupalSettings.param,
  dataType:'html',
  data:{mt:$("#maturityTypeFilter").val(), md:$("#maturityFilter").val(), td:$("#tradingFilter").val()}, ...})
```
with `drupalSettings.instrument == "ECO-DPAR"`, refreshed every 30 s (`setInterval(settlementTabPrices,3e4)`). It returns an HTML table fragment. **It was not called.**

**[D]** `https://www.euronext.com/en/terms-use` (updated 29 April 2021; names `live.euronext.com` among the covered sites): "Except if we give you prior written permission, use of any ... spiders, robots, avatars, agents, tools or other devices or mechanisms to navigate, search or determine the Euronext Website is strictly prohibited" and "you will not ... download ... or create derivative works from any Content ... (including ... through framing or systematic retrieval to create collections, compilations, databases or directories)". **[D]** `https://live.euronext.com/robots.txt` additionally disallows `/product/` for `ClaudeBot`, `GPTBot`, `PerplexityBot` and other named AI agents; `User-agent: *` disallows only Drupal internals.

**[I]** The `getTabPrices` route is the DSP source and is terms-prohibited for automated retrieval. Out.

### 1b. The DSP methodology and contract facts (for the guard)

**[D]** `https://live.euronext.com/en/products/commodities/dsp`: for Milling Wheat (EBM) and Rapeseed (ECO) the Settlement Range is "2 minutes before the end of the Trading session preceding 18:30 CET". **[D]** Extended-hours Q&A (`live.euronext.com/en/media/1932/download`): after the 13 April 2026 go-live "the daily settlement price (DSP) remains fixed at 18:30 CET for all the commodity contracts". **[D]** Contract specification (`.../ECO-DPAR/contract-specification`): trading hours "10:45 – 20:15 CET (UTC+1) (and 10:45 – 18:30 CET the last three days of the expiry)"; delivery months "February, May, August, November such that ten delivery months are available"; unit 50 tonnes; "Euro and euro cents per tonne"; last trading day "18:30 on the last business day of the calendar month immediately preceding the delivery month".

**[D]** In the 2026-11-04 trade file the newest XMAT trade is stamped `19:14:59Z` = 20:14:59 CET, consistent with a 20:15 CET session end. So a session labelled D is finished at **20:15 Europe/Paris on D**; that is the Layer 37 settlement-guard rule.

### 1c. Static files under `live.euronext.com/sites/default/files/commodities_reporting/`

**[D]** `.../2026/11/04/en/cdwpr_ECO_20261104.csv` → 200, 3,229 bytes, `text/csv`: the **weekly MiFID II position report** (COT by participant category, report date 2026-10-30, published 04/11/2026; `COMB` and `FUTR` blocks). No price column. **[D]** Directory listings → 403 (S3 `AccessDenied`); guessed daily-settlement siblings (`cddpr_`, `cdsp_`) → 403. Useful later for a MATIF COT leg; not a price source.

### 1d. The MiFIR delayed trade file — the path

**[D]** `https://marketdata.euronext.com/data-reporting-service/trades-file` → 200. Its form posts nothing; the page's `downloadFile()` builds
```
/data-reporting-service/trades-file/download/{fileType}/{timeSelection}/{tradingLocation}
```
from radio values `fileType ∈ {EQUITIES, ETFS_FUNDS, WARRANT_CERTIFICATES, FIXED_INCOME, EQUITY_INDEX_DERIVATIVES, COMMODITY_DERIVATIVES}`, `timeSelection ∈ {LAST_15_MINUTES, LAST_HOUR, CURRENT_TRADING_DAY, SINCE_PREVIOUS_TRADING_DAY, PREVIOUS_TRADING_DAY}`, `tradingLocation ∈ {AMS, BRU, DUB, LIS, MIL, OSL, PAR}`, then `fetch(url, {method:'HEAD'})` and treats a 404 as "empty file". No login, no token.

**[D]** `GET .../download/COMMODITY_DERIVATIVES/PREVIOUS_TRADING_DAY/PAR` → 200, 589,877 bytes, `application/zip`, one member `Trades_CommodityDerivatives.csv` (10.1 MB, 46,374 trades, all `Venue == XMAT`, all dated 2026-11-04, 09:18:00Z → 19:14:59Z). `.../CURRENT_TRADING_DAY/PAR` at 15:52:35Z → 200, 326,723 bytes, 26,451 trades to `15:37:33Z` — **~15 minutes behind the clock**, matching the page's "maximum of 15 minutes time delay"; the file "remains available for at least 24 hours".

**[D]** Shape: line 1 is a copyright/terms preamble (`(c) 2025 Euronext N.V. ... Terms and Conditions for Delayed Trade Data available at: https://www.euronext.com/delayed-data-terms-conditions`), line 2 the header:
`TradingDateTime,PublicationDateTime,MifidInstrumentID,MifidPrice,MifidQuantity,MifidPriceNotation,MifidCurrency,MifidQtyInMeasurementUnitNotation,MifidQuantityMeasurementUnit,MifidNotionalAmount,NotionalCurrency,MifidClearingFlag,MmtPostTradeDeferral,NumberOfTransactions,MmtMarketMechanism,MmtNegotiationIndicator,MmtModificationIndicator,MmtBenchMarkIndicator,MmtContributionToPrice,MmtAlgorithmicIndicator,MmtPublicationMode,Venue,ThirdCountryTradingVenueExecution,TradeUniqueIdentifier,MissingPrice,MmtContingentTransactionIndicator,VenueOfPublication`.
All rows `MifidPriceNotation = MONE`, `MifidCurrency = EUR`, `MissingPrice` empty. `MmtMarketMechanism` on 11-04: `1` × 46,247 (central limit order book), `4` × 69, `5` × 31, `8` × 27. One `MmtModificationIndicator = CANC` row. 100 distinct `MifidInstrumentID`s that day; 45 on 11-05 to 15:37Z.

**[D]** The file carries **no product code and no maturity** — only the ISIN (and, for 38 of the 100 ids, a bare numeric id such as `4224099884` with up to 2,028 trades, lot size 50; **[I]** strategy/spread instruments without an ISIN, left unresolved and never priced).

**[D]** Terms (`https://www.euronext.com/sites/default/files/stld/terms_and_conditions_for_delayed_data.pdf`, one page, read in full): "1. The right to Use the Data is free of charge and these Terms do not impose any restrictions on the Use, except in the event of distribution." Clause 5 (attribution + the "© Euronext N.V. ... may not be copied or further disseminated ... except as specifically authorized" disclaimer on distribution), 6 (no distribution to third parties subject to a fee unless licensed), 7–8 (fee-bearing or commercial distribution needs an agreement/EMDA fees). "Use" is defined in the preamble to include "downloading".

**[I]** Private desk use — fetch, store, print on the desk — is "Use" and is free and unrestricted under clause 1. Rendering it on the public site would be distribution and is out (`MATIF_PUBLISH = False`, enforced). **[I] Residual risk, stated:** `marketdata.euronext.com` is not in the 2021 Terms of Use's list of covered sites, and its own page carries these specific Terms that contemplate downloading; the alternative reading is that the general "no robots" clause reaches every `euronext.com` host. **[?]** Not confirmed with Euronext; `databyeuronext@euronext.com` is the address the prior note named.

### 1e. ESMA FIRDS — the ISIN → contract map

**[D]** `https://registers.esma.europa.eu/solr/esma_registers_firds/select?q=isin:FREN27722858&wt=json&fl=*` → 200, one doc: `gnr_full_name = "Rapeseed / Colza FUTR PHYS JAN 2027"`, `gnr_cfi_code = FCAPSX`, `mic = XMAT`, `drv_expiry_date = 2027-01-29T00:00:00Z`, `mrkt_trdng_trmination_date = 2027-01-29T17:30:00Z`, `drv_price_multiplier = 50`, `drvcmd_base_product = AGRI`, `drvcmd_sub_product = GROS`, `drvcmd_further_sub_product = RPSD`, `drvcmd_transaction_type = FUTR`, `drv_delivery_type = PHYS`, `latest_received_flag = 1`.

**[D]** Batched `isin:A OR isin:B ...` queries of 15 ISINs with `fq=mic:XMAT` answered 200 in 0.8–7.1 s each; a broad `q=mic:XMAT AND drvcmd_further_sub_product:RPSD` listing timed out twice (504 from the Azure gateway at ~20 s). Per-ISIN resolution is the workable shape. Options come back as two docs per ISIN (`latest_received_flag` 1 and 0); the flag-1 doc is the current one.

**[D]** The 62 ISINs resolved from the 2026-11-04 file: RPSD futures × 6 (`JAN 2027, APR 2027, JUL 2027, OCT 2027, JAN 2028, APR 2028` — i.e. six of the ten listed maturities traded), MWHT futures × 10 (one named `Milling Wheat TAS FUTR PHYS NOV 2026` — a **Trading-At-Settlement** instrument, priced as a differential), CORN futures × 5, options (CFI `OCAFPS`/`OPAFPS`) × 40, one `European SMP Fut FUTR CASH`.

**[I]** FIRDS names the **expiry month**; the ECO delivery month is the month after (expiry 2027-01-29 → Feb 2027 delivery, consistent with the spec's "last business day of the month preceding the delivery month" and the 2026–2040 calendar's Nov 2026 row: last trading 30 OCT 2026). Layer 37 stores the delivery month as `contract_month = YYYY-MM` and hard-fails if an expiry month is not Jan/Apr/Jul/Oct.

**[D]** ESMA legal notice (`https://www.esma.europa.eu/legal-notice`): reproduction of material "is permitted, except as otherwise stated, provided the source is acknowledged". No statement about automated access. **[A]** A public regulatory register whose purpose is machine consumption (daily full/delta files exist); the per-ISIN query is a few requests a day.

### 1f. The settlement-range window is visible in the trades

**[D]** On 2026-11-04, 2,091 trades across 20 instruments fall in 17:28–17:30Z (18:28–18:30 CET, the ECO/EBM settlement range). **[I]** A VWAP of that window would *approximate* the DSP; it is not published as such and would be an invented settlement. **Not computed.** Layer 37 stores last trade and a whole-session VWAP, both labelled as what they are.

---

## RQ2 — Republishers and libraries

- **yfinance [D]** (re-run today in this repo's `.venv`, yfinance current): `ECO=F`, `ECOX26.PA`, `ECO.PA`, `ECOF27.NX`, `ECOF27.PA`, `COM=F`, `XR=F`, `XRX26.NX` → 0 rows each; control `ZS=F` → 4 rows (2026-11-05 close 1204.25). Yahoo still does not carry MATIF rapeseed.
- **Barchart [D]** `XR*0` pages exist; `GET https://www.barchart.com/terms` answered **202 with an empty body** today (bot gate). The prior note quotes the ToU of 3 Feb 2026: no "data mining, robots, or similar data gathering" and no reproduction/redistribution without consent. Out.
- **Investing.com [D]** the terms page fetch returned only the footer/risk disclosure: "It is prohibited to use, store, reproduce, display, modify, transmit or distribute the data contained in this website without the explicit prior written permission of Fusion Media and/or the data provider." Download needs an account. Out.
- **Stooq [D]** `https://stooq.com/q/d/l/?s=xr.f&i=d` (and `eco.f`, `rs.f`, control `zs.f`) → 200 with a 796-byte "This site requires JavaScript to verify your browser" page. No keyless CSV. Out.
- **TradingView [A]** carries `EURONEXT:` commodity futures since Aug 2024 (their blog) — a display product, no file; and #328 already found their embed refuses CME symbols. Out as a source.
- **Web search (2026) [D]** found no Euronext-hosted bulk DSP file for Paris commodities; the "Derivatives Settlement Prices" download page at `athens.euronext.com/en/market-data/instruments/derivatives/settlement-prices` is the ATHEX section. **[?]** Not probed for XMAT coverage; its host is in the Terms-of-Use family in any case.

---

## What Layer 37 is, and is not

| | |
|---|---|
| Source | Euronext MiFIR delayed post-trade file, `COMMODITY_DERIVATIVES` / `PAR`, both `PREVIOUS_TRADING_DAY` and `CURRENT_TRADING_DAY` each run |
| Instrument map | ESMA FIRDS per-ISIN lookups, cached in the stored rows (`isin`, `firds_name`, `expiry_date`), so a FIRDS outage only blocks never-seen ISINs |
| Row | one per (session date, delivery month): open/high/low/last/VWAP of **on-book** trades (`MmtMarketMechanism == 1`), lots, trade count, `last_trade_at`; off-book trade and lot counts and cancelled-trade counts carried as columns; EUR/t; `quote_kind = board_last_traded` |
| Guard | session D finished at 20:15 Europe/Paris on D, Monday–Friday — via `fetchers._settlement.SessionRule`, the one guard site |
| Excluded | options (CFI `O…`), TAS instruments (`TAS` in the FIRDS name), non-RPSD products, numeric non-ISIN ids |
| Not a settlement | the DSP is on a terms-prohibited route; nothing here reaches `pricing.semantics` as anything but last-traded |
| Retention | Euronext keeps the files "at least 24 hours": a missed desk run is a permanent hole for that session; the two selections give D−1 and D so one missed run self-heals |
| Privacy | `MATIF_PUBLISH = False`; `matif_futures` is not in `pipeline/history.py`'s export; no reader under `app/` or `scripts/generate_site.py`; the layer runs only where `MATIF_PRIVATE_FETCH` is set (the desk), so the CI database and the MCP image never hold a row; `mirror_mcp` refuses a database that does |
| USD/MT | **not printed.** No USD/EUR series is stored (`CURRENCY_TICKERS` has no EUR pair; the ECB cross fetcher uses USD/EUR only as a leg and does not store it), and inventing a second conversion site is forbidden (invariant 7). `python -m analysis.matif_curve` prints EUR/t and says so. |

## Live run of the built layer (2026-11-06, ~00:10Z, outside pytest)

**[D]** `fetch_matif_trades()` end to end against Euronext and FIRDS: `PREVIOUS_TRADING_DAY` → the 2026-11-05 file, 41,769 trades; `CURRENT_TRADING_DAY` → 404 (pre-open, treated as "no file"); 29 non-ISIN ids left unresolved; result `ok` with five ECO session rows for 2026-11-05 — Feb-27 open 526.50 / high 529.75 / low 523.50 / **last 525.00** / VWAP 526.98 / 15,946 on-book lots / 8,611 trades / last print 19:14:59Z / 3 off-book prints; May-27 last 527.75 (6,398 lots); Aug-27 507.50; Nov-27 513.75 (last print 17:50:44Z); Feb-28 514.75 (51 lots). Nothing was stored (no `MATIF_PRIVATE_FETCH` on this machine; the call was made directly to the fetcher). **[?]** Not checked against the Euronext DSP for 2026-11-05 — doing so would mean reading the terms-protected page; the only honest comparison is a human reading it once and noting the gap between last trade and DSP.

## Alternative explanations considered

1. **"The trades file is incomplete — some ECO prints never reach it."** **[?]** Possible for pre-trade-deferred or off-venue prints; `MmtPostTradeDeferral` is `-` on every row seen. On-book volume on 11-04 (19,123 lots on the Feb-27 contract alone) is of the order of a normal ECO day, but no independent volume figure was checked against it.
2. **"Strategy legs are reported only under the numeric strategy id, so outright last trades under-count."** **[I]** Plausible; it changes lots and counts, not the last on-book price's honesty. Recorded, not corrected.
3. **"The settlement-range VWAP is good enough to call a settlement."** Rejected — invariant 3: nothing is a settlement unless proven; the DSP has fallbacks (calendar VWAP, last traded, absolute change) the file cannot see.
