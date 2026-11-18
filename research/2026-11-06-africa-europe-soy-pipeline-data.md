# Africa → Europe soy pipeline: what a buyer needs, what we have, where the rest comes from

Date: 2026-11-06. Owner brief: "big opportunities in that pipeline, Nigeria → Europe or Africa → Europe; I need the data."

This is the synthesis. Evidence lives in two companion notes written the same day, each claim tiered [D] observed / [I] inferred / [A] secondary / [?] unverified:

- `research/2026-11-06-africa-europe-soy-origin-side.md` — Nigeria export rules, African export volumes, freight, naira FX.
- `research/2026-11-06-africa-europe-soy-eu-side.md` — EU duty, EUDR, GMO/non-GM premium, DG AGRI, Eurostat, EU price references, and why Layer 22 is failing.

Success criteria set before research: for each data item a buyer needs to price an Africa → Europe cargo, name what the site has, and for every gap a verified source with URL, cadence, access and licence, or an honest "no source".

## 1. The trade as the data actually shows it [D]

- **The lane is real and it is mostly meal, not beans.** Eurostat Comext, EU-27 imports from Nigeria of HS 2304 soymeal: 1,616,991 t (2023), 1,613,534 t (2024), then 24,645 t in 2025 to date. France alone booked 159,269 t from Nigeria in 2024 per UN Comtrade mirror. Beans from Nigeria to the EU are small: 13,196 t in 2025.
- **Togo is the bean origin for Europe.** EU-27 imports of HS 1201 from Togo: 954,771 t (2023), 673,164 t (2024), 835,923 t (2025 to date). Benin and Burkina Faso are secondary.
- **Nigeria's own customs numbers are unusable for the EU leg.** Nigeria reports 17,726 t of meal to France in 2024; France reports 159,269 t received. Rule: use the EU-side mirror for anything Europe-bound, store both sides, never net them.
- **The 2025 collapse in Nigerian meal to the EU is unexplained [?]**: partial-year data or a real stop. This is the first question the data must answer, and Comext monthly will answer it.

## 2. What a buyer needs, what we have, what is missing

| # | Decision input | Have today | Gap | Best source found | Verdict |
|---|---|---|---|---|---|
| 1 | Origin price, Nigeria beans | **Yes** — AFEX daily reference, Layer 36, live since 2026-11-06 | — | AFEX API | done |
| 2 | Origin price, Nigeria meal (the product Europe buys) | No | **Lagos soymeal ₦/MT** | NEPC monthly LICPL PDF, `nepc.gov.ng/indicative-market-prices/` (Sep-26: ₦1.0–1.26 M/MT) | buildable, store-only; NEPC terms bar republication → owner publish call under A18 |
| 3 | Naira conversion | Yahoo `NGNUSD=X` — tracks neither official nor street [D] | **Replace** | CBN keyless JSON `cbn.gov.ng/api/GetAllNFEM_Rates` (NFEM VWAP, daily since 2024-12) | buildable-free |
| 4 | Nigerian FOB / export price | No | No official series exists anywhere | — | withhold with reason |
| 5 | Export rules, levies, documents | No | NESS levy 0.5% FOB, Form NXP 90/90 days, CCI mandatory, NEPC ₦13,500; no soya on the readable export-prohibition list | CBN FX Manual 4th ed. 2026; NSW trade portal | reference facts, not a layer; one HITL read of the 2026 revised list on `customs.gov.ng` (Cloudflare-walled) |
| 6 | Africa → EU trade flows, monthly, by origin and member state | No | the lane's own volume | **Eurostat Comext API**, keyless, HS 1201/2304 by partner, ~2-month lag | buildable-free |
| 7 | Same, weekly | No | arrivals as they clear customs | **EU agrifood API TAXUD weekly surveillance** `api.tech.ec.europa.eu/agrifood/api/taxud/weeklyData/import` — rows exist for partner NG, TG, GH; ~1-week lag; throttled (429) and backend was SUSPENDED (500) during probing | buildable-free, fragile; CC BY 4.0 |
| 8 | Annual export volumes, all ten African origins | No | — | FAOSTAT bulk zip, CC BY 4.0, through 2024; UN Comtrade keyless preview (500 rows/call, "internal use only") | FAOSTAT buildable-free; Comtrade desk-side only |
| 9 | EU import duty | No | — | 0% MFN on 1201 and 2304 from all seven origins; Ghana preference dated to 31/12/2026; Access2Markets JSON route for re-verification | static desk fact with a re-verify date |
| 10 | EUDR | No | legal gate on soy into the EU | Application 30 Dec 2026 (large/medium operators), 30 Jun 2027 (micro/small); Nigeria/Benin/Zambia/Malawi standard risk, Ghana/Togo/SA low risk; EU importer files the due-diligence statement; no public API | static desk fact |
| 11 | GMO status and the non-GM premium | No | the premium is the margin in this trade | Nigeria has no GM soy in cultivation [D]. Free daily non-GM meal table at proteinmarkt.de / Donau Soja (7 locations, forward months, Non-GMO 44% ~€629/t Oct-26 vs GM ~€498/t), numbers are AMI's; CLAL Bologna weekly chamber quote €573–575/t | buildable; licence is owner call; CLAL cleanest |
| 12 | EU destination meal price | Layer 22 EC world prices — **down since 2026-11-05** (CIRCABC platform outage, not a move) | Rotterdam daily | EU agrifood API weekly member-state prices (NL soymeal €418/t Oct-26, ES, FR Montoir, DE); World Bank monthly US 48% FOB Rotterdam; daily Montoir via Argus-sourced farm press (terms forbid reproduction) | weekly buildable-free; daily = private edition only |
| 13 | Freight West Africa → Europe | No | — | Nothing free and machine-readable for a bulk lane. Only primary print: Nigerian Shippers' Council quarterly indicative container rate (Q2-26 Lagos→Europe/ME 20' $1,404, 40' $2,004). CCFI West-East Africa index points (China lanes) | carry NSC as labelled quarterly `indicative`; otherwise withhold |
| 14 | India (context market) | Agmarknet captcha-blocked since 2026-10; SEA + SOPA published | — | unchanged; India-hosted relay is the only path (#566) | owner |

## 3. Why Europe's existing layer is dark

Layer 22 (`ec_oilseeds`) resolves the "World oilseed prices" workbook from the DG AGRI landing page. Since between 2026-11-04 and 2026-11-05 every path on `circabc.europa.eu` answers 307 → `sorry.ec.europa.eu` "Server temporarily unavailable". The landing page is unchanged, so this is an outage, not the rotating-URL trap. The fetcher fails correctly ("File is not a zip file"). No copy of that workbook exists outside CIRCABC; its content is IGC FOB prices republished by DG AGRI. Wait it out; one hardening: detect the `sorry.ec.europa.eu` redirect host and say so in the alert.

CIRCABC notice captured 2026-10-17 [A]: interest groups outside EU institutions become read-only April 2027 and are deleted July 2027. The DG AGRI workbooks may or may not be affected [?]. The agrifood API is the durable replacement for everything except the IGC world FOB sheet.

## 4. Build order (what turns this into a desk screen)

Ordered by value to a Nigeria → Europe meal decision per unit of work. All free, keyless, publishable under A18 unless noted.

1. **Eurostat Comext monthly imports** of 1201/2304 by partner and member state, ten African origins. Answers "is the lane alive" and the 2025 collapse. Fixed URL, needs a `LAYER_MAX_DATA_AGE_DAYS` budget (~75 d). Trade statistic, never a quote.
2. **CBN NFEM FX layer** replacing Yahoo NGN for the AFEX conversion. Daily VWAP, close, high/low, turnover. Pre-2024 backfill from the older CBN endpoint with hard-fail on its six duplicate dates.
3. **NEPC LICPL monthly**: Lagos soymeal ₦/MT and state soya ₦/100 kg. Store-only behind a publish gate shaped like `AFEX_PUBLISH_RAW`; owner flips it.
4. **EU agrifood API weekly prices**: soymeal NL/ES/FR Montoir/DE and soya beans DE/IT. Replaces the Rotterdam gap with the Commission's own weekly series. Throttle-aware fetcher, one request per product per run.
5. **EU TAXUD weekly imports by origin** from the same API. Same fetcher family as 4; keep as a separate layer because the backend was suspended mid-probe and the two must fail independently.
6. **Non-GM premium**: CLAL Bologna weekly (public chamber quote) publishable; proteinmarkt/Donau Soja daily table store-only pending the AMI licence call.
7. **Desk reference file** (`data/reference/`), not a layer: EU duty 0% with Ghana expiry 31/12/2026, EUDR dates and risk tiers, Nigeria NESS/NXP/CCI/NEPC facts, each with a verified-on stamp.
8. **FAOSTAT annual** export volumes for the ten origins, flags carried through.
9. **NSC quarterly indicative freight** as a labelled cost line. Never a fixture.

Not buildable: Nigerian FOB (none exists), bulk freight on the lane (paid only), naira parallel rate (paywalled and non-redistributable), India mandi without a relay.

## 5. Owner decisions embedded above

- Publish NEPC soymeal and the proteinmarkt non-GM table, or keep them desk-only. Both buildable either way.
- Treat UN Comtrade as desk-side only (its terms say internal use), FAOSTAT as the public volume series.
- Read the 2026 Revised Export Prohibition List on `customs.gov.ng` from a browser once, to close the "no ban" finding.

## 6. Could not verify [?]

- Cause of the 2025 drop in Nigeria → EU meal (partial year vs stop).
- The 2026 Nigerian export prohibition list text (Cloudflare).
- Agrifood API quota size and return-to-service time; CIRCABC outage end.
- EUDR Regulation 2026/2102 entry-into-force article; practical tolerance of a non-EU seller registering as operator.
- Redistribution terms for SCFI/CCFI/NSC/CBN (none published).
