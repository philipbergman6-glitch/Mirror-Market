# EU destination-side data for African soy into Europe — and why `ec_oilseeds` is failing

- **Date:** 2026-11-06 (every URL fetched this date; clock stamps are UTC)
- **Question:** for a physical buyer moving soybeans (HS 1201) / soymeal (HS 2304) from Nigeria or West/Southern Africa into Europe, what EU destination-side data exists, where exactly does it come from, and what can this project build on it for free?
- **Scope note:** primary sources only (official sites, first-party APIs). No sign-ups, no emails. Items 1–6 below were verified by six parallel research agents against the live sites; §0 was verified directly in this session.
- **Related:** `fetchers/ec_oilseeds.py` (Layer 22), `LAYERS.md` §22, `research/2026-11-05-matif-rapeseed-source.md` (Layer 37), invariant 10 (rotating-URL trap).

**Evidence tiers:** **[D]** directly observed today · **[I]** inference from observed data · **[A]** assumption or secondary source · **[?]** could not verify.

---

## 0. Why `ec_oilseeds` (Layer 22) is failing in CI — CIRCABC is down, not moved

**[D]** `data/history/layer_runs.csv`: Layer 22 was `success` on every run through `2026-11-04T00:00:43Z` (session 2026-11-03) and `failed` on every run from `2026-11-05T00:14:05Z` onward (5 consecutive: 11-05 00:14, 11-05 01:14, 11-06 00:04, 11-06 06:23, 11-06 06:42).

**[D]** Deploy log, run 42178421042 (2026-11-06 07:16 UTC):
```
INFO  — fetchers.ec_oilseeds — Downloading EC oilseeds world prices ...
INFO  — fetchers.ec_oilseeds — EC world prices downloaded (39 KB)
ERROR — fetchers.ec_oilseeds — Failed to parse EC world-prices xlsx: File is not a zip file
WARNING — __main__ — [ec_oilseeds] returned no data
```

**[D]** Reproduced locally 2026-11-06 10:05 UTC. The landing page `https://agriculture.ec.europa.eu/data-and-analysis/markets/overviews/market-observatories/crops/oilseeds-and-protein-crops_en` → 200 (100,893 bytes) and still carries the **same** anchor the fetcher resolves — `href="https://circabc.europa.eu/sd/a/2ddd7dcd-dff1-41b5-94b9-6cd207181a3c/oliseeds-world-prices.xlsx">World oilseed prices` — so the GUID has **not** rotated. But every CIRCABC route answers the same way, with and without a User-Agent:

```
GET https://circabc.europa.eu/sd/a/2ddd7dcd-.../oliseeds-world-prices.xlsx   → 307 Location: https://sorry.ec.europa.eu/
GET https://circabc.europa.eu/rest/download/2ddd7dcd-...                     → 307 → sorry.ec.europa.eu
GET https://circabc.europa.eu/rest/download/205f3b77-...?ticket=             → 307 → sorry.ec.europa.eu
GET https://circabc.europa.eu/d/a/workspace/SpacesStore/8df1b7d8-.../OILSEEDS%20TAXUD_Surv-DATA.xlsx → 307 → sorry
GET https://circabc.europa.eu/sd/a/4d9aeffa-.../oilseeds-monthly_trade-eurostat_en.xlsx             → 307 → sorry
GET https://circabc.europa.eu/ui/welcome                                     → 307 → sorry
GET https://circabc.europa.eu/                                               → 307 → sorry
```
Following the redirect gives **HTTP 200, `text/html`, 40,691 bytes**: `<title>Server temporarily unavailable</title> … "We apologise for any inconvenience. Please try again later."` — that is the 39 KB "workbook" the fetcher tried to unzip.

**[I]** Diagnosis: a **platform-wide CIRCABC outage** (every path on the host, not just our GUID) that began between 2026-11-04 00:00 UTC and 2026-11-05 00:14 UTC. It is not the rotating-GUID trap (the landing page anchor is unchanged) and not a source move (the landing page was not edited). The fetcher behaved exactly as designed: `requests` follows the 307, the HTML is not a zip, `zipfile.BadZipFile` is caught, the layer returns `{}` and is graded `failed`, nothing stale is stored. Hypothesis to disconfirm: a migration rather than an outage — see §4 for what the DG AGRI agent found about the host's status and whether the same workbooks exist on `agridata.ec.europa.eu`.

**[D]** Wayback: the xlsx GUID was last archived 2023-08-23 (`archive.org/wayback/available` → `20230823013808`); `circabc.europa.eu/ui/welcome` has no snapshot. No offline copy of the current workbook is available from the archive.

**[I]** Blast radius in this repo: `grep -rln circabc fetchers/ config.py` → only `fetchers/ec_oilseeds.py` and `config.py`. The other four CIRCABC workbooks on the landing page (production/area/yield, customs-surveillance trade ×2, Eurostat monthly trade) are the ones §4 evaluates for the Africa→EU lane — all are on the same dead host today.

**Small hardening worth filing (not done here):** `_download_world_prices()` could check `resp.url` host / `Content-Type` before returning bytes and log "CIRCABC redirected to sorry.ec.europa.eu" instead of the generic "File is not a zip file" — same outcome (failed), clearer alert text. No change to grading.

## 5. Eurostat Comext — monthly EU imports of 1201/2304 by partner — **buildable-free, keyless** ✔

Verified by the Comext agent with curl, keyless, 2026-11-06.

**[D]** API: `https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data/DS-045409?freq=A&reporter=EU27_2020&partner=NG&product=1201&flow=1&indicators=VALUE_IN_EUROS&indicators=QUANTITY_IN_100KG&time=2024&time=2025` → HTTP 200, JSON-stat 2.0, `"label":"EU trade since 1988 by HS2-4-6 and CN8"`, `"updated":"2026-10-16T11:00:00+0200"`, `"value":{"2":5006.51,"3":13196.39,"0":304529,"1":925529}` — NG→EU27 soybeans 2024 ≈ 501 t / €304,529; 2025 ≈ 1,320 t / €925,529. Params: `freq=M|A`, `reporter`, `partner` (repeatable), `product` (HS4 `1201` and CN8 `12019000` both accepted), `flow=1` import, `indicators=…`, `time=` or `sinceTimePeriod=2024-01`. Default curl UA, empty UA and `python-requests/2.31` all 200 on the API; the Liferay *website* 429s after ~15 rapid hits.

**[D]** Dataset codes (from `…/sdmx/2.1/dataflow/ESTAT/all`): **DS-045409** (since 1988, HS2-4-6 + CN8) is the one to use; DS-059341 (since 2002 HS, indicators `VALUE_EUR`/`QUANTITY_KG`) cross-checks (NG 1201 2025 = 1,319,639 kg / €925,529). **DS-059322** (CN8 since 2002) → 404 `ERR_NOT_FOUND_4 … not available for dissemination`. Partner aggregates `EXT_EU27_2020` works (EU27 2025 bean imports ≈ 14.2 Mt); `WORLD`/`LDC`/`ACP`/`AFRICA` return empty — sum countries yourself.

**[D]** Bulk: `https://ec.europa.eu/eurostat/api/dissemination/files/?sort=1&dir=comext%2FCOMEXT_DATA%2FPRODUCTS` → 200, 393 `full_v2_YYYYMM.7z` files; latest `full_v2_202608.7z` 24.56 MB dated 2026-10-20 (file URL `…/files/?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202608.7z` → 200, `Content-Length: 25760599`). **Trailing slash after `files` is required** (without it: 302 to an internal 10.x address). The old `BulkDownloadListing` URL → 404; `fullYYYYMM.7z` naming → 404.

**[D]** Lag: latest populated month today = **2026-08**; policy page (`…/web/international-trade-in-goods/information-data`): "extra-EU data are published … 46 days after the end of the reference month; intra-EU data … around 2.5 months after". So ≈ T+7 weeks for extra-EU (what this lane needs). Site notice: API unavailable Sat 7 Nov 08:00–18:00.

**[D]** Licence: `https://ec.europa.eu/eurostat/web/main/help/copyright-notice` (the `/about-us/policies/copyright` URL is 404): "Reuse of statistical data … for commercial or non-commercial purposes is authorised provided the source is acknowledged … implemented by the Decision of 12 December 2011"; modifications must be stated; a non-responsibility disclaimer must be included. Clears invariant 9.

**[D]** Does the lane exist? EU27 imports, annual, hundred-kg (÷10 = t):

| Partner | 1201 beans 2023 / 2024 / 2025 | 2304 meal 2023 / 2024 / 2025 |
|---|---|---|
| NG | 1,278 / 5,007 / 13,196 | **1,616,991 / 1,613,534 / 24,645** (€83.8M / €78.8M / €1.79M) |
| TG | **954,771 / 673,164 / 835,923** (€66.1M / €38.8M / €52.2M) | 6,714 / 154,214 / 371,840 |
| BJ | 43,525 / 37,289 / 0.19 | 1.32 / 92,560 / 57,640 |
| GH | 9.73 / 1.69 / 32,114 | 863 / 9,136 / 20,982 |
| ZA | 299,226 / 0.37 / 1.61 | ~0 |
| ZM | 0 / 0 / – | ~0 |
| MW | no rows | no rows |

**[I]** Nigeria is a **soymeal** lane (~161 kt/yr 2023–24, collapsed to 2.5 kt in 2025, back to ~70 kt Jan–Aug 2026, almost all into **FR**: FR/NG 2304 2026-02 = 198,884 hkg); Nigerian beans are negligible. Togo is the **bean** lane (~84 kt 2025; reporters FR/BE/IT/ES). South Africa: one 30 kt bean cargo in 2023, nothing since. Zambia/Malawi: no lane in official data. Monthly NG 2304 2025 (8,266 + 7,349 + 9,030 = 24,645) reconciles to the annual row.

**Verdict: buildable-free.** Fixed-URL API + `updated` stamp → needs a `LAYER_MAX_DATA_AGE_DAYS` budget (~75 d). These are customs quantities/values, not prices; an implied €/t unit value is a trade statistic and must stay out of `pricing.semantics` as a quote. Use: lane-existence, destination member state, monthly volume, unit-value cross-check.

## 2. EUDR (Regulation (EU) 2023/1115) — legal fact, not a feed; **no public data API** ✘

Verified by the EUDR agent. EUR-Lex blocks curl/WebFetch (HTTP 202 + AWS-WAF challenge stub), so every EUR-Lex quote was read through a real browser on the `EN/TXT/HTML` endpoint; other pages via curl with status shown.

**Application date as of Nov 2026.** **[D]** Consolidated text `CELEX:02023R1115-20260918` ("Consolidated TEXT … 18.09.2026") lists three amendments: M1 Reg (EU) 2024/3234 (OJ L 23.12.2024), **M2 Reg (EU) 2025/2650 of 19.12.2025 (OJ L 23.12.2025)**, M3 Delegated Reg (EU) 2026/2102 of 13.7.2026 (OJ L 17.9.2026, scope only). **[D]** 2025/2650 Art 1(8) new Art 38: "Articles 3 to 13, Articles 16 to 24 and Articles 26, 31 and 32 shall apply from **30 December 2026**" and, for micro/small operators established by 31 Dec 2024 (except timber), "from **30 June 2027**". Entry into force 26 Dec 2025; EUTR 995/2010 repealed "with effect from 30 December 2026". New Art 34(1a): "By 30 April 2026, the Commission shall carry out a simplification review". **[D]** Review outcome: press release IP/26/941 (4 May 2026, `…/presscorner/api/files/document/print/en/ip_26_941/IP_26_941_EN.pdf` → 200): report + guidance/FAQ + draft delegated act on scope + draft implementing act on the IS, "reduce annual compliance costs … by about 75%" — **no new legislative proposal, no further delay.** Commission EUDR page (200) status box: "Large & medium-sized operators and traders, 30 December 2026"; "micro & small … 30 June 2027"; "Newly added products … 30 Dec 2027".

**Soy in Annex I.** **[D]** Consolidated Annex I: "Soya — 1201 90 00 Soya beans … other / 1208 10 Soya bean flour and meal / 1507 Soya-bean oil … / 2304 Oilcake … resulting from the extraction of soya-bean oil". 2026/2102 recital 12 narrowed `1201` to `1201 90 00` (seed for sowing out of scope). So beans and meal from Nigeria are both in scope.

**Country benchmarking.** **[D]** Implementing Reg (EU) 2025/1093 of 22 May 2025 (`CELEX:32025R1093`, OJ L 23.5.2025) Annex: high risk = Belarus, DPRK, Myanmar, Russia; low-risk list (119 names) **includes Ghana, Togo, South Africa**; **Nigeria, Benin, Zambia, Malawi are absent → standard risk** (Art 29(1)(c): "'standard risk' refers to countries … which do not fall in either … 'high risk' or … 'low risk'"). No 2026 revision of the list found on Commission pages. Practical consequence **[D→I]**: the "micro or small primary operator" simplified declaration (Art 4a / Art 2(15a)) is defined for low-risk countries only, so Ghana/Togo/SA growers could use it; Nigerian growers cannot.

**Who files.** **[D]** Art 7: "Where a natural or legal person established outside the Union places relevant products on the market, the first natural or legal person established in the Union who makes such relevant products available on the market shall be deemed to be an operator" — the **EU importer** is the operator of record, never the Nigerian seller. Art 6(1): an authorised representative (must be EU-established, Art 2(22)) may submit, operator retains responsibility. Art 4(2): no placing on the market "without prior submission of a due diligence statement … through the information system referred to in Article 33". Post-2025/2650 simplification: Art 4(7) the operator passes DDS reference numbers downstream; Art 5(3)(a) downstream operators/traders collect reference numbers "only in the event that their supplier is an operator"; Art 5(2) only **non-SME** downstream operators and traders must register; recital: "The obligation to collect and keep reference numbers should apply only to the first downstream operator or trader". **So only the first operator files; SME traders file nothing.** Art 9(1)(d): geolocation of "all plots of land where the relevant commodities … were produced" (GeoJSON RFC 7946, WGS84, point allowed with default 4 ha, 25 MB per DDS — acceptance IS docs, 200). Art 26(4): the DDS reference number "shall be made available to customs authorities before the release for free circulation"; single-window interface by 1 Dec 2029 (Art 28).

**Data surfaces.** **[D]** Information System = TRACES module, `https://eudr.webcloud.ec.europa.eu/tracesnt/login` (200). API docs `https://acceptance.eudr.webcloud.ec.europa.eu/tracesnt/help/eudr-documentation/operator/index.html` (200): **SOAP**, WS-Security UsernameToken, WSDL `/ws/EUDRDueDiligenceStatementServiceV4?wsdl`, operations submitDds / amendDds / withdrawDds / getDds / getDdsByInternalReference; `getDdsByIdentifiers` "discontinued over API, UI-only". Credentials require a validated operator account. The CIRCABC API folder and competent-authority list → 307 `sorry.ec.europa.eu` today [?].

**Verdict: owner-decision (static config), not a layer.** Encode as desk-side facts with a verified-on stamp: application 30 Dec 2026 (large/medium) / 30 Jun 2027 (micro/small); Nigeria/Benin/Zambia/Malawi = standard risk, Ghana/Togo/SA = low risk; Annex I = 1201 90 00, 1208 10, 1507, 2304; EU importer files the DDS; reference number needed at customs. No anonymous read endpoint exists, so nothing here is fetchable. Unverified: 2026/2102 entry-into-force article [?]; whether a non-EU seller registering voluntarily as operator is tolerated in practice (Commission guidance not read) [?].

## 3. GMO status — Nigeria has no GM soy in cultivation; EU non-GM premium has a free daily table (licence unclear)

Verified by the GMO agent, 2026-11-06.

**Nigeria.** **[D]** USDA FAS GAIN **NI2025-0021** "Biotechnology and Other New Production Technologies Annual, Lagos, Nigeria" (2025-12-29; `apps.fas.usda.gov/newgainapi/api/Report/DownloadReportByFileName?fileName=Biotechnology+and+Other+New+Production+Technologies+Annual_Lagos_Nigeria_NI2025-0021.pdf` → 200, 640 KB): *"Bt Cotton, PBR Cowpea, and TELA Maize are currently the only approved crops for commercial release. The government has approved imported biotech corn and soybean varieties for feed and oil processing."* **[D]** ISAAA GM Approval Database, Nigeria (`isaaa.org/gmapprovaldatabase/approvedeventsin/default.asp?CountryID=NG` → 200; note `gmapprovalsdatabase` with an "s" is 404): 33 events, of which **11 soybean events** (A2704-12, A5547-127, DP356043, FG72, FG72×A5547-127, GTS 40-3-2, MON87701, MON87705, MON87708, MON87769, MON89788), source "Nigeria Biosafety Clearing House". Drilled GTS 40-3-2 (EventID=174): Food = "2019 *", Feed = "2019 *", **Cultivation = empty**; tooltip "Authorization period: 10th April 2019 to 31st December 2022" — renewal after 2022 **[?]**. **[D]** NBMA GMO database `gmdatabase.nbma.gov.ng` (TLS cert expired; `curl -k` → 200) lists 2021 TELA maize, 2022 HB4 wheat/potato/cassava; 2023–2025 pages read "Information … Will Be Available Soon"; no soybean entry. BCH Nigeria page is an Angular SPA; its API → 401 [?]. **[A]** Peer-reviewed 2025 paper hosted on nbma.gov.ng: "permits for the import of GM soy and wheat for use as food, feed and industrial processing, no approval … for their cultivation." A 2024 Guardian NG claim that soybean was "approved for commercialisation" conflicts with FAS/ISAAA — treat as conflation with FFP.

**[I] Conclusion:** Nigerian-origin soy is **non-GM by default** (no GM soy cultivated), but there is no identity-preservation scheme behind that, so a European non-GM buyer still needs a certified IP chain (Donau Soja / ProTerra / VLOG) to earn the premium — a desk fact, not a feed.

**Neighbours (brief).** **[D]** South Africa: ISAAA GTS 40-3-2 Cultivation = "2001"; 16 soy events; **[A]** GM ≈ 95% of SA soy area (AgbioInvestor May 2026). **[D]** Ghana: GTS 40-3-2 food/feed = 2024, cultivation empty; **[A]** PBR cowpea only. **[A]** Malawi Bt cotton 2018; Zambia still bans GM cultivation/import as of late 2025 — no primary source fetched for either [?].

**EU non-GM soy premium — who publishes, free vs paid.**

| Source | URL / status | Observed [D] | Cadence | Free? | Verdict |
|---|---|---|---|---|---|
| **Donau Soja daily DS/ES meal prices** | `https://www.donausoja.org/ds-es-soya-bean-meal-prices/` → 200 | "updated every weekday … typically between 1:00 and 2:00 PM (Vienna time)… based on 44% protein… net values"; the table is an **iframe** of `https://www.proteinmarkt.de/charts/tagesaktuelle_oelschrotpreise/non-gmo-sojaschrot_en.php` (200) | Daily (weekdays) | Yes, no login | **buildable-free, licence = owner-decision** |
| **proteinmarkt.de non-GMO table** | `…/non-gmo-sojaschrot.php` → 200 | "Stand: 05.11.2026"; Non-GMO SES ES/DS 44%: Norddeutschland 633/632/628/629/633 €/t (Nov26…Mai/Jul27, "Fracht bezahlt bis"); Süddeutschland 606…; Linz 600; Budapest 594; Mailand 583; Brake Übersee-Ware 648/638/622/622/622; conventional 44% Deutschland 410/411/411/409/407; 48% 436/438. "Quelle: Agrarmarkt Informations-Gesellschaft, Bonn / Verein Donausoja" | Daily | Yes | same; no terms text on page, imprint page "Inhalt nicht gefunden" → licence [?] — **AMI-sourced, redistribution must be cleared before publishing (invariant 9)** |
| **Donau Soja Market Report** | `https://www.donausoja.org/news/market-report/` → 200 (`/market-report/` → member login) | "GM and non-GM soya prices/premiums… updated on a monthly basis… available for our members. A yearly subscription can be purchased by non-members"; Oct 2026 = No. 74; public blog posts give headline levels only (Sep 2026: non-GM S. Germany "above 530 EUR/t") | Monthly | Members/paid | **paid** (headlines free) |
| **ENGA Non-GMO Market Report** (with Donau Soja + ProTerra) | `https://www.enga.org/pdf/Market_Report_Edition_11.pdf` → 200, 10.6 MB | "Edition 11 / July 2026": Bologna non-GM beans >450 €/t; S. Germany >480 €/t; HP non-GM meal N. Germany ~530 €/t; Q1-2025 ed.: "Non-GM soymeal premiums ranged from €90 to €130 per tonne" | ~Quarterly | Free PDF | **buildable-free** narrative only, not a series; licence [?] |
| **CLAL/TESEO Bologna** | `https://teseo.clal.it/en/?section=BIO-far-soia-bol` → 200 | "NON-GMO Wholegrain Roasted Soybean Meal – Italian, 44% stq", source CCIAA Bologna; weekly Thursday; 05 Nov 2026: 573–575 €/t | Weekly | Free view | **buildable-free**, terms [?] (CLAL re-publishes the Bologna chamber quote) |
| **agrarheute Markt (dlv)** | `https://markt.agrarheute.com/futtermittel-3/sojaschrot-16` → 200 | Producer prices frei Hof, Oct 2026: Sojaschrot 43/44% 498.18; GMO 48% 523.16; **Non-GMO 44% 629.08 €/t**; "Quelle: AMI"; "© 2026 agrarheute.com Alle Rechte vorbehalten" | Monthly | Free to read | **owner-decision** (AMI data, all rights reserved) |
| **AMI** | `https://www.ami-informiert.de/ami-maerkte` → 200 | Shop; free soy content = headlines | — | Paid | **paid** (the upstream of the two above) |
| **VLOG Marktreport** | 200 | May 2026 ed., free PDF — market shares, **no prices** | Irregular | Free | not a price source |
| **Vesper** | `https://vespertool.com/pricing/` → 200 | "Contact us for pricing", free trial; no non-GM soymeal on pricing page | — | Paid | **paid** |
| **Expana (ex-Mintec)** | `expana.com` DNS fail; `mintecglobal.com` 200 | no non-GM soymeal page surfaced | — | Paid [A] | **paid / unverified** |
| **S&P Global Platts** | not fetched | [A] FOB Netherlands & EXW Spain soymeal assessments launched 2025 (GM, not non-GM) | Daily | Paid | **paid** |
| **EU Commission oilseeds observatory** | landing 200 | **No GM / non-GM mention anywhere on the page**; weekly dashboard is JS (contents [?]) | Weekly | Free | no non-GM quotation |
| **USDA FAS EU Oilseeds Annual E42026-0029** (2026-04-10) | 200, 1.9 MB | grep "non-gm|gm-free|premium" = **0 hits** | Annual | Free | no premium data |

**[I] Premium arithmetic:** agrarheute Oct-26 farm-gate same basis: 629.08 − 498.18 = **+131 €/t**. proteinmarkt 05-11-2026 Nov-26: Norddeutschland non-GM 633 (freight paid) vs conventional 44% Deutschland 410 → ~+223 €/t, but the delivery bases differ, so it is not a clean spread. ENGA's "€90 to €130" (Q1-2025) brackets the same-basis number.

**Verdict:** the only daily, free, keyless non-GM series is the proteinmarkt/Donau Soja table (7 locations, forward months) — buildable technically, **owner-decision on licence** because the numbers are AMI's. CLAL Bologna weekly is the cleanest public-chamber quote. Everything that labels itself "premium" (Donau Soja Market Report, AMI, Vesper, Expana, Platts) is paid.

## 1. EU import duty — 0 % MFN on both codes for all seven origins; **buildable-free** via an undocumented Access2Markets JSON route ✔

Verified by the duty agent, 2026-11-06 (raw captures in the session scratchpad `taric/`).

**Headline.** **[D]** MFN "Third country duty" on **1201 90 00** and **2304 00 00** = **0 %**, regulation R2416520 (= Council Reg (EU) 2024/1652), valid 01/07/2024 → open-ended. The measure is expressed as a condition: B1 "Y155 Products exported directly or indirectly from the Russian Federation or Belarus → 50.00 %"; B2 (no cert) → "0 %"; footnote CD775. **[D]** For 2304 the measure lists are byte-identical across NG/GH/TG/BJ/ZM/MW/ZA; for 1201 only GH adds "Ghana | Tariff preference | 15/12/2016–**31/12/2026** | D1618500 (Decision (EU) 2016/1850, Ghana stepping-stone EPA) | 0 %". No SADC-EPA / GSP / EBA row renders for the others **[I]** because preferences are not coded where MFN is already free. **So the preference question is moot for duty on beans and meal.** Also on 1201 (erga omnes, from 01/11/2026): "Low-value consignment customs duty 3.00 EUR" (R2603820) and "Union handling fee 2.00 EUR" (R2621080), conditioned on Y126 distance sales — irrelevant to cargo.

**Per-origin regime (read off a dutiable sibling, 1507 10 10, same API).** **[D]** NG → "GSP - General arrangements | 0 % | R1209781" — Nigeria is a **standard-GSP** beneficiary, confirmed in Reg (EU) 2026/1395 Annex I "NG | Nigeria | Standard GSP"; not EBA. TG, BJ, ZM, MW → "GSP-EBA … Everything But Arms | R1209782". ZA → "South Africa | Tariff preference | 10/10/2016– | D1616230" (SADC EPA; DG TRADE: parties Botswana, Lesotho, Mozambique, Namibia, South Africa, Eswatini). GH → interim EPA "entered into provisional application on 15 December 2016" (DG TRADE West Africa page; the `/ghana_en` page is 404); TARIC preference row **ends 31/12/2026** — re-check in Jan 2027; regional ECOWAS EPA "yet to enter into force". Proof of origin for dutiable lines: GSP/EBA = REX statement (Form A not accepted after 30 June 2020) [D]; EPA proofs not read [?]. **[D]** GSP law: Reg 2023/2663 extends 978/2012 to 2027; Reg (EU) 2026/1395 (signed 18 June 2026, OJ 22 June 2026) "shall apply from 1 January 2027", repealing 978/2012 the same day.

**Non-duty conditions rendered in TARIC.** **[D]** Both codes: "Import control of organic products (01-01-2022–) CD808, R2306/21" (C644 certificate or Y929 "not concerned by Reg 2018/848"). 2304 only: "Import control – Waste (21-05-2026–) CD572/573/574/577, R1157/24" (Y-code exemption unless declared as waste). **No GMO or EUDR measure renders in TARIC** for either code.

**Sources.**

| Source / URL | HTTP | Observed | Access | Verdict |
|---|---|---|---|---|
| **Access2Markets JSON** `https://trade.ec.europa.eu/access-to-markets/api/tariffs/get/{TARIC10}/{origin}/{dest}?lang=EN` | 200 `application/json` | full measure/condition/footnote objects, e.g. `"type":"Third country duty","regulationId":"R2416520"… "tariffFormula":"0%"`; route recovered from the site's `main.js` (`tariffsUrl`, `getTariffsEU`); wrong arg order → 401 or `[]` | **keyless GET, no cookie** | **buildable-free — preferred**; undocumented, version string "2026.10.2.4", stability [?] |
| TARIC DDS2 `…/dds2/taric/measures.jsp?Lang=en&Taric=1201900000&Area=NG&Expand=true` | 200, 71 KB | template only ("The reference date is 06-11-2026 / Last TARIC update: 05-11-2026"); measures are an iframe `measures_details.jsp?…&Sid=…` needing the JSESSIONID + Sid from the outer page; without them → **500** | keyless, 2-step session | buildable-free, fragile fallback |
| Access2Markets search page `…/en/search?product=1201&origin=NG&destination=DE` | 200 | Angular shell, "a match was identified with an HS code. Please continue browsing" — no numbers without JS | — | dead for scraping |
| EUR-Lex `/eli/reg/2023/2663/oj` etc. | **202** "we need to verify that you're not a robot. This requires JavaScript" | bot-shield | — | dead for automation |
| Publications Office CELLAR `http://publications.europa.eu/resource/celex/32026R1395` (content-negotiated, `Accept: application/xhtml+xml`) | 200 → `…/cellar/<uuid>.0006.03/DOC_1` | full legal text | keyless | buildable-free for legal text |

Cadence: TARIC updates daily ("Last TARIC update: 05-11-2026" on a 06-11 query); A2M mirrors it (same regulation IDs/SIDs) [I]. Licence **[D]**: Commission legal notice → Decision 2011/833/EU, CC BY 4.0 "provided appropriate credit is given and changes are indicated"; publishing duty data with attribution is permitted [I, high confidence].

**Verdict: buildable-free**, but low value as a *daily* feed — the answer is a constant (0 %) with one dated exception (Ghana preference to 31/12/2026) and one surcharge (RU/BY 50 %). A static desk fact with a re-verify trigger (Jan 2027: Ghana row + GSP 2026/1395 switch) is the honest form; the A2M API is the re-verification tool.

## 6. EU / Rotterdam / Hamburg price references — two free series, everything else paid or licence-blocked

Verified by the Rotterdam agent, 2026-11-06 (raw captures in session scratchpad `dl/`).

**Free, keyless, machine-readable.**

- **EC Agridata oilseeds API** — `https://www.ec.europa.eu/agrifood/api/oilseeds/prices?memberStateCodes=DE,NL,ES&products=Soya%20meal&years=2026` → 200 JSON (600 KB). **[D]** Product list served: `Crude rape oil, Crude soya bean oil, Crude sunflower oil, Rapeseed, Rapeseed meal, Soya beans, Soya meal, Sunflower seed, Sunflower seed meal`. Soya-meal rows: **DE Hamburg FOB 40-50 %** (249 rows, last `03/08–09/08/2026 €356.00` — **stale ~3 months, reason unknown [?]**), DE Mainz FOB, DE Brake FOB, Bremen/Mannheim/Köln/Würzburg/München/Hannover DEPSILO, DE Average DEPPROC; **ES Average DEPPROC `01/11/2026 €424.73`**; **NL (product type `N.A.`) `18/10/2026 €418.21`**. Unit "national currency/ton". Weekly. Dashboard `https://agridata.ec.europa.eu/extensions/DashboardCereals/OilseedPrices.html` → 200 (JS). → **buildable-free**; this is the *only* keyless weekly EU soymeal series. Full member-state list not enumerated (query limited to DE,NL,ES) — see §4.
- **World Bank Pink Sheet** — `https://thedocs.worldbank.org/en/doc/74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/CMO-Pink-Sheet-November-2026.pdf` → 200 (dated "November 3, 2026"); monthly xlsx `CMO-Historical-Data-Monthly.xlsx` under the same doc id. **[D]** Oct-2026: Soybean meal **453**, Soybean oil 1,546, Soybeans 515 $/mt. **Definitions page (p.2): "Soybean meal, U.S. Soybean Meal, 48% protein, average weekly price, FOB Rotterdam." / "Soybean oil, U.S. Soybean Oil Crude Degummed, FOB U.S. Gulf." / "Soybeans, U.S. Soybeans, FOB U.S. Gulf."** with the note "January 2025 … Some oilseed series have been replaced." **[I]** The old "Argentine 45/46 % CIF Rotterdam" and "Soybeans CIF Rotterdam" definitions (still echoed by IndexMundi/cbonds) are superseded — the repo's Layer 8 label must say *US 48 % FOB Rotterdam*, not CIF. Doc-id rotates monthly → resolve from `https://www.worldbank.org/en/research/commodity-markets` (200) each run (invariant 10; Layer 8 already does this). → buildable-free (already Layer 8).

**Free to read, licence-blocked for publishing (private edition = owner-decision).**

| Source | URL / status | Observed [D] | Cadence | Terms |
|---|---|---|---|---|
| web-agri.fr (Terre-net) | `https://www.web-agri.fr/marches-agricoles/tourteaux` → 200 | "Tourteaux de soja 48%, pellets Montoir Départ, €/t 05/11/2026 … nov. 430,00 +6"; also Brest, Gand, Ottmarsheim | Daily | mentions légales: "Toute reproduction ou représentation non autorisée … est interdite … Bases de données" |
| pleinchamp.com | `…/les-cours/aliments-du-betail/42-tourteaux-de-soja` → 200 | "Délivré Montoir 05/11/2026 430 €/t +6,00 €/t Source : Argus Media"; analysis subscriber-only | Daily | Argus rights [I] |
| agrarheute (dlv) | `https://markt.agrarheute.com/futtermittel-3/sojaschrot-16` → 200 | AMI monthly producer prices (Oct 2026: 43/44 % 498.18; GMO 48 % 523.16; Non-GMO 44 % 629.08 €/t); "Großhandelspreise … Hamburg" exists but JS-rendered via `api.dlv.de` (region id 120030) | Monthly | dlv AGB reserves TDM rights (EU 2019/790) |

**Paid.** Argus (methodology PDF "LAST UPDATED: OCTOBER 2026" → 200: "Soybean meal (SBM) delivered Montoir", Brazil fob Paranaguá; **no Rotterdam/Hamburg soymeal assessment in the guide**). Fastmarkets `AG-SYB-0077` "Soymeal hi-pro 49% CIF Argentina/Rotterdam, $/t, launch 20 November 2000" (200, no value). S&P Platts FOB Netherlands (`EUPMQ00`/`EUPMR00`, effective 1 May 2025) — subscriber URLs **403**, facts [A] only. AMI (shop). agrarzeitung Produktenbörsen ("Von Hamburg über Mannheim bis Wien", 9 €/1st month then 54 €/mo). Kaack → cash prices moved to `kassapreise.de` ("Sojaschrot in Deutschland und Benelux … Abonnement"; terms forbid redistribution). lafranceagricole (weekly, subscriber; 30/10/26 "445 €/t délivré Montoir"). Vesper (trial). LSEG / Expana [?].

**Dead / none.** IGC GOI (`igc.int/en/public-site/markets/marketinfo-goi.aspx` → 200; `igc__goi.xlsb` → 200, 7.1 MB) is **index-only** ("Soyabeans sub-Index 245 … 4 Nov"), no $/t soymeal; no ToS page found [?]. AMIS Market Monitor (FAO PDF → 200) reproduces the GOI, no Rotterdam series. USDA FAS E42026-0029 (EU Oilseeds Annual, 2026-04-10): zero "Rotterdam"/"Hamburg" hits. Comité van Graanhandelaren `graan.com/beursnoteringen` → 200 but no own notering; Beurs Groningen last notering 07-02-2023 (grains only); FEGRA/Synagra JS/login [?]; Agrimatie no soymeal series found [?].

**Verdict:** for a Rotterdam/EU soymeal reference the project can publish exactly two things: Agridata weekly member-state prices (ES/NL current; DE Hamburg FOB stale since Aug) and the World Bank monthly US-48 % FOB Rotterdam. A daily CIF Rotterdam/Montoir number exists only behind Argus/Fastmarkets/Platts/AMI or the French farm-press pages whose terms forbid reproduction — owner-decision for a private desk edition, never `docs/`.

