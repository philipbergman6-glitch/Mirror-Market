# K1 · Place list — ports and growing areas for every rendered leg

Ticket: [#360](https://github.com/philipbergman6-glitch/Mirror-Market/issues/360) · Map: [#356](https://github.com/philipbergman6-glitch/Mirror-Market/issues/356) · Compiled 2026-10-05 against `main` @ `bec74aa`.

**Standing rule applied:** a place is listed only because a rendered leg needs it. Every leg→place link is tagged with *how we know*:

- **config** — stated in `config.py` (line cited)
- **layers** — stated in `LAYERS.md` (line cited)
- **data** — observed in the stored rows
- **ext** — stated by the venue's own rulebook/spec (URL cited)
- **inferred** — a reasoned link with no source saying it; the review at sign-off (P1) should confirm or drop it

**Coordinates:** each port/pricing point carries two independent sources where both exist — UN/LOCODE (UNECE release via the `datasets/un-locode` mirror, commit 2026-07-27; unece.org itself returned a Cloudflare block) and Wikidata P625. The NGA World Port Index download refused the request. Growing-area coordinates are the existing `GROWING_REGIONS` pins (`config.py:367-411`, set by M14 #207 / M24 #271). They are **pins, not footprints**. Footprint outlines belong to K2/R3; the admin-1 code is given here as the likely outline unit, and the ISO 3166-2 codes come from memory. **Verify them at build.**

---

## 1. Ports and pricing points

| id | Place | Kind | lat, lon (used) | UN/LOCODE | Wikidata | Δ km | Notes |
|---|---|---|---|---|---|---|---|
| P-NOLA | US Gulf — NOLA / lower Mississippi elevator range | export port range | 29.9369, −90.0619 | USMSY (no coords) | Q773908 Port of New Orleans | — (single source) | Range runs Baton Rouge → Gulf. Range anchors: Port of South Louisiana Q2316930 (30.0510, −90.5000); Baton Rouge USBTR 30.450, −91.133 / Q28218 30.4475, −91.1786, Δ 4.3 km |
| P-PNG | Paranaguá | export port | −25.5047, −48.5108 | BRPNG −25.500, −48.517 | Q10351931 | 0.8 | |
| P-UPR | Argentina up-river — Rosario / San Lorenzo / Timbúes | export port range | −32.9575, −60.6394 | ARROS −32.950, −60.650 | Q52535 Rosario | 1.3 | San Lorenzo: ARQBR −32.650, −60.717 / Q52546 −32.7500, −60.7333, Δ 11.2 (the LOCODE entry is the Quebracho terminal north of town). Timbúes ARTIM −32.633, −60.783 (single source) |
| P-NCN | North China discharge range — Qingdao / Rizhao / Dalian | import port range | 36.0833, 120.3170 | — | Q1068325 Port of Qingdao | — | Rizhao CNRZH 35.383, 119.533 / Q16898718, Δ 2.2. Dalian Q1703582 38.9167, 121.6833 (single source: the CNDLC LOCODE row is the airport). The config itself calls this "one discharge range, not one berth" |
| P-DUR | Durban | import-parity port | −29.8737, 31.0232 | ZADUR −29.850, 31.017 | Q7231133 | 2.7 | |
| P-MOS | Moselle barge ports — Metz (+ Frouard) | inland FOB barge point | 49.1197, 6.1769 | FRMZM 49.133, 6.167 | Q22690 | 1.7 | Frouard FROXA 48.767, 6.133 / Q195098, Δ 0.7. Euronext rapeseed delivery ports on the Moselle: Belleville, Metz, Frouard. Belleville not located |
| P-CBOT | CBOT soybean delivery territory — Chicago/Burns Harbor at par, Illinois Waterway down to St. Louis | inland delivery territory | 41.6259, −87.1334 (Burns Harbor) | USBNB 41.617, −87.133 | Q2680659 | 1.0 | Chicago Q1297 41.8819, −87.6278 (LOCODE USCHI has no coords). St. Louis USSTL 38.617, −90.183 / Q38022, Δ 1.8. Territory per CBOT Rule 11106 |
| P-RFT | Randfontein — JSE single reference point | inland pricing point | −26.1797, 27.7042 | ZARFT −26.150, 27.700 | Q744437 | 3.3 | **Time-bound:** the JSE has proposed Driefontein from the season starting 2027-03-01; Grain SA is contesting it. Not located |
| P-IDR | Indore — mandi MP hub | inland pricing hub | 22.7186, 75.8550 | INIDR 22.717, 75.833 | Q66616 | 2.2 | |
| P-PKG | Port Klang | FCPO delivery port | 3.0000, 101.4000 | MYPKG 3.000, 101.400 | Q111387192 | 0.0 | |
| P-PGU | Pasir Gudang (Johor) | FCPO delivery port | 1.4500, 103.8900 | MYPGU (no coords) | Q1018693 | — (single source) | |
| P-PEN | Penang / Butterworth | FCPO delivery port | 5.4157, 100.3660 | MYPEN 5.417, 100.317 | Q46349553 Port of Penang | 5.5 | |

Sources for the delivery-point rows:

- P-CBOT: [CBOT Rulebook ch. 11, Rules 11105–11106](https://www.cmegroup.com/rulebook/CBOT/II/11/11.pdf)
- P-PKG/PGU/PEN: [Bursa FCPO contract spec](https://www.bursamalaysia.com/sites/5d809dcf39fba22790cad230/assets/605322fb5b711a61ee8be2ae/BURSA_FCPO_Contract_Spec_EN_digital.pdf) and [list of PTIs](https://www.bursamalaysia.com/trade/trading_resources/brokers_for_derivatives/non_broker_participants/list_of_port_tank_installations)
- P-MOS: [Euronext rapeseed spec](https://live.euronext.com/en/product/commodities-futures/ECO-DPAR/contract-specification)
- P-RFT: [JSE market notice 28426](https://clientportal.jse.co.za/Content/JSENoticesandCircularsItems/JSE%20Market%20Notice%2028426%20CDM%20-%20Outcome%20of%20Multiple%20Reference%20Points%20Model.pdf) and [Engineering News, 2026-07-21](https://www.engineeringnews.co.za/article/grain-sa-dismayed-about-jses-maintained-single-reference-point-approach-for-soybeans-2026-07-21)

The search summary was read; the PDFs themselves were not opened, so treat exact clause numbers as unverified.

**Existing located places, not new.** The river gauges already carry provider IDs: Memphis NWPS `MEMT1`, St. Louis NWPS `EADM7`, Rosario INA series 34 (`config.py:470-523`). Their coordinates live in provider metadata and were not re-fetched here.

## 2. Growing areas

All pins come from `config.GROWING_REGIONS`; each one's role comes from that market's `weather_regions`.

| id | Pin (config key) | lat, lon | Likely footprint unit (ISO 3166-2, unverified) | Crop |
|---|---|---|---|---|
| G-IA | US Midwest (Iowa) | 42.03, −93.47 | US-IA | soy |
| G-IL | US Illinois | 40.12, −89.30 | US-IL | soy |
| G-NE | US Nebraska | 40.90, −98.40 | US-NE | soy |
| G-MT | Brazil Mato Grosso | −12.64, −55.42 | BR-MT | soy |
| G-PR | Brazil Parana | −24.04, −51.46 | BR-PR | soy |
| G-RS | Brazil Rio Grande do Sul | −28.50, −53.50 | BR-RS | soy |
| G-PAM | Argentina Pampas | −33.95, −60.33 | **ambiguous**: the pin sits in N. Buenos Aires prov. (AR-B) at the Santa Fe (AR-S) border; "Pampas" spans several provinces | soy |
| G-COR | Argentina Cordoba | −31.42, −64.18 | AR-X | soy |
| G-BAS | Argentina Buenos Aires (sunflower) | −38.40, −60.30 | AR-B (SE) | sunflower |
| G-PY | Paraguay Alto Parana | −25.90, −55.30 | PY-10 (+ Itapúa PY-7, Canindeyú PY-14 per the config comment) | soy |
| G-FR | France Champagne (Grand Est) | 48.70, 4.30 | FR-GES | rapeseed |
| G-DE | Germany Mecklenburg-Vorpommern | 53.60, 12.70 | DE-MV | rapeseed |
| G-RO | Romania Baragan (Danube plain) | 44.60, 27.00 | **ambiguous**: the Bărăgan spans several județe | rapeseed |
| G-SK | Canada Saskatchewan (Saskatoon) | 52.10, −106.70 | CA-SK | canola |
| G-AB | Canada Alberta (central) | 53.00, −112.80 | CA-AB | canola |
| G-RIAU | Indonesia Riau (Sumatra) | 0.29, 101.71 | ID-RI | palm |
| G-SBH | Malaysia Sabah (Borneo) | 5.42, 116.80 | MY-12 | palm |
| G-MP | India Madhya Pradesh | 22.72, 75.86 | IN-MP | soy |
| G-MH | India Maharashtra | 19.75, 75.71 | IN-MH | soy |
| G-HL | China Heilongjiang | 47.36, 127.76 | CN-HL | soy |
| G-FS | South Africa Free State | −29.12, 26.21 | ZA-FS | soy |
| G-MPU | South Africa Mpumalanga | −25.47, 30.00 | ZA-MP | soy |
| G-BEN | Nigeria Benue | 7.73, 8.52 | NG-BE | soy |
| G-KAD | Nigeria Kaduna | 10.52, 7.43 | NG-KD | soy |

## 3. Leg → places

| Rendered leg (where it renders) | Ports / pricing points | Growing areas | Basis |
|---|---|---|---|
| CBOT Soybeans `cbot:board` (CBOT page; every ledger except India) | P-CBOT (delivery territory); P-NOLA as its export outlet | G-IA, G-IL, G-NE | P-CBOT **ext**. P-NOLA **inferred** (`LEDGERS["cbot"]` note: "the board against its own physical — the Gulf basis"). Pins **config** `:1904` |
| CBOT Soybean Oil, Soybean Meal (CBOT price keys + named crush) | **gap**: oil/meal delivery points not looked up | G-IA, G-IL, G-NE | pins **config** (market-level) |
| US Gulf CIF `us_gulf:cif` (CBOT basis; CBOT, Dalian, Brazil, Argentina ledgers; origins) | P-NOLA | G-IA, G-IL, G-NE; river: Memphis, St. Louis | **config** `:2671`, `:2713`; **data**: all 9,891 soybean rows in `gulf_bids.csv` carry `location="Gulf Coast Ports"`, `freight="CIF-B"`. AMS 3147 is titled "Louisiana and Texas" (**layers** L80), but no Texas location appears in the rows |
| DCE Soybean No.2 `dalian:board` + DCE oil, meal (Dalian page; CBOT ledger) | P-NCN; import-origin ports P-PNG, P-NOLA | G-MT (import origin), G-HL | P-NCN **config** `:2694` (`market: "dalian"`). Origin ports **config** via `LEDGERS["dalian"]` legs. G-MT role "import origin — prices No.2 and meal" **config**. DCE delivery warehouses are a **gap** |
| DCE Soybean No.1 (Dalian price key, no ledger row) | **none**: domestic food bean | G-HL ("prices No.1") | **config** |
| CEPEA/ESALQ Paraná `brazil:cepea` (Brazil page) | **none**: in-state wholesale indicator, not a port price | G-PR (the index's own state), G-MT, G-RS | **layers** L75/L77; pins **config** `:2035` |
| ESALQ/B3 Paranaguá (Brazil price key) | P-PNG | G-MT, G-PR, G-RS | name **layers** L77 |
| AgRural Paranaguá FOB `brazil:paranagua` (Brazil, CBOT, Dalian, Argentina, SA ledgers; origins) | P-PNG | G-MT, G-PR, G-RS | **config** `:2681`, `:2745` |
| MAGyP FOB Soybeans/Oil/Meal `argentina:fob` (Argentina page + crush; CBOT, Brazil, SA ledgers; origins) | P-UPR | G-PAM, G-COR, G-PY; river: Rosario | **config** `:2045` venue "(up-river)", `:2686`, `:2761` |
| MAGyP FOB Sunflower Oil (Argentina page; headline oil board) | P-UPR per the config venue; **gap**: whether sun-oil's official FOB references up-river or the southern ports | G-BAS | venue **config**; sun-oil port **inferred** only |
| India mandi MP `india:mandi_mp` | P-IDR (hub); **no port**: GM import ban, no trade route (`LEDGERS["india"]` note) | G-MP | hub **layers** L76 |
| India mandi MH `india:mandi_mh` | **none**: state-wide median, no hub stated | G-MH | **layers** L76 |
| EU Rapeseed (Moselle) (Europe page) | P-MOS | G-FR, G-DE, G-RO | Moselle **layers** L83 + **ext** |
| SAFEX Soybean `south_africa:safex` (SA page) | P-RFT; P-DUR (import-parity port) | G-FS, G-MPU | P-RFT **ext**; P-DUR **config** `:2516` |
| SAFEX Sunflower (SA price key) | P-RFT | **gap**: no sunflower pin; G-FS/G-MPU are labelled "domestic crop" (soy) | |
| SAGIS deliveries — soybeans, sunflower seed (SA flows) | none (national tonnage) | G-FS, G-MPU (soy); sunflower gap as above | **config** `:2248` |
| Palm Oil (CME CPO=F, off Bursa FCPO) (headline Soy Oil vs Palm Oil) | P-PKG, P-PGU, P-PEN | G-RIAU, G-SBH | ports **ext**; pins **config** `:1487` |
| CZCE Rapeseed Oil (headline Soy Oil vs CZCE Rapeseed Oil) | **gap**: CZCE delivery warehouses not looked up | **gap — mismatch**, see §4.1 | |

## 4. Findings for K2 / P1. Gaps listed, not filled

1. **Canola-prairie belt prices a leg that does not render.** `COMPETING_OIL_WEATHER_BELTS` pairs Saskatchewan/Alberta with the `"ICE canola"` leg (`config.py:1499-1503`). But ICE canola has had no feed since 2026-08-08 (`config.py:223-228`), and the rendered rapeseed leg is CZCE Rapeseed Oil (`app/sections.py:136-147`). Under the standing rule, either the belt re-points to CZCE OI with a stated link (Canadian canola is a Chinese crush input; that link is **inferred**), or the pins have no rendered leg. Decide at P1.
2. **Nigeria has pins but no price leg** (`MARKETS["nigeria"]["price"] is None`, `config.py:2273`). G-BEN and G-KAD exist and the weather-mapping test passes only because it counts `weather_regions`, not price legs. The extended rule ("weather **and hazards** for rendered legs") would give Nigeria hazard flags on no leg. Decide at P1.
3. **The Brazil port leg is Paranaguá only.** Santos (BRSSZ −23.933, −46.317 / Q16460) and the Arco Norte ports carry much of Mato Grosso's export flow, but no rendered leg prices there. They are excluded by the rule, which means a cyclone or port-rain event at Santos raises no flag. This ties into the map's "Port rain" fog.
4. **US PNW renders as a declared-absent origin row** (`ORIGIN_LEGS["us_pnw"]`), and its GTR vessel count is a rendered non-price row. It has no price leg, so it is excluded here. P1 should say whether a hazard flag can attach to a non-price row.
5. **Places that change over time:** the JSE reference point may move Randfontein → Driefontein on 2027-03-01. The place list needs an effective-date field, or this row goes silently wrong.
6. **Unverified items, to resolve at build rather than guess:**
   - CBOT oil/meal delivery points
   - DCE No.2 and CZCE OI delivery warehouses
   - Euronext's Belleville port
   - the MAGyP sun-oil port basis
   - the admin-1 codes above
7. **Cyclone relevance at a glance** (for R2; *inferred* from geography, not checked against R2's basin maps):
   - Atlantic basin: P-NOLA
   - W. Pacific basin: P-NCN
   - S.-W. Indian Ocean: P-DUR
   - N. Indian Ocean: G-MP/G-MH (monsoon depressions)
   - Little to no cyclone exposure: P-PNG and P-UPR (S. Atlantic is near-cyclone-free); the FCPO ports (near-equatorial)
