"""
Mirror Market configuration.

Ticker symbols, API keys, and shared settings live here so every
module can import them from one place.
"""

import logging
import os
from datetime import date as _date
from typing import Any


# ---------------------------------------------------------------------------
# Logging — call setup_logging() once at startup (in main.py)
# ---------------------------------------------------------------------------
def setup_logging(level=logging.INFO):
    """
    Configure the root logger with a clean, timestamped format.

    Every module that does `logger = logging.getLogger(__name__)` will
    inherit this format automatically — no per-file setup needed.

    Levels (from most to least verbose):
        DEBUG   → fine-grained diagnostic info
        INFO    → confirmation that things are working
        WARNING → something unexpected but not fatal
        ERROR   → something failed
    """
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s — %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# ---------------------------------------------------------------------------
# Network settings
# ---------------------------------------------------------------------------
REQUEST_TIMEOUT = 30    # seconds — used by every fetcher's HTTP calls
MAX_RETRIES = 3         # how many times to retry a failed request

# Minimum non-empty keys for a multi-key layer to count as fully healthy.
# Below the floor the layer is recorded as failed freshness ("partial") —
# 1 of 13 currencies is an outage, not a success. Layers not listed here
# succeed with any non-empty key.
# Counts in the trailing comments are derived, not authoritative — the
# denominator each layer is actually graded against lives in
# LAYER_KEY_CATALOGS below, and tests/test_layer_coverage.py asserts every
# floor here fits inside its catalog.
LAYER_MIN_KEYS = {
    "prices": 8,       # of 10 tickers
    "currencies": 8,   # of 10 pairs
    "fred": 8,         # of 9 series
    "weather": 14,     # of 19 regions
    "cot": 7,          # of 10 commodities
    "psd": 5,          # of 10 commodities
    "dce": 3,          # of 8 contracts (6 DCE + 2 CZCE rapeseed)
    "usda": 2,         # of 3 stats (production, area harvested, yield)
    "export_sales": 4,  # of 6 commodities
    "forward_curve": 7,  # of 9 commodities
    "eia": 2,          # of 3 series
}

# Systemic-outage backstop: exit non-zero when more than this many active
# (non-disabled) layers fail in one run, even if the critical layers passed.
MAX_FAILED_LAYERS = 5
RETRY_DELAY = 2         # seconds between retries

# Authoritative operational inventory. The public masthead, About Data table,
# pipeline summary, and smoke contract all consume this catalog so their
# denominator cannot drift. Numbered groups 2 and 15 each have an independently
# runnable sub-layer, hence 27 operational layers across 25 numbered groups.
PRODUCTION_LAYERS = (
    ("prices", "1", "Yahoo Finance (CME/CBOT/ICE)", "Daily", "10 commodity futures"),
    ("usda", "2", "USDA NASS QuickStats", "Annual", "US production, area and yield"),
    ("crop_progress", "2b", "USDA NASS QuickStats", "Weekly/seasonal", "US crop progress and condition"),
    ("fred", "3", "Federal Reserve (FRED)", "Daily/Monthly", "Dollar, CPI, rates and yields"),
    ("cot", "4", "CFTC", "Weekly", "10 commodities positioning"),
    ("weather", "5", "Open-Meteo", "Daily + forecast", "19 growing regions"),
    ("psd", "6", "USDA FAS (PSD)", "Monthly", "10 commodities × 28 countries"),
    ("currencies", "7", "Yahoo Finance (FX)", "Daily", "10 currency pairs"),
    ("worldbank", "8", "World Bank Pink Sheet", "Monthly", "Palm and rapeseed oil benchmarks"),
    ("dce", "9", "AKShare (DCE/CZCE)", "Daily", "Chinese oilseed futures"),
    ("export_sales", "10", "USDA FAS (Export Sales)", "Weekly", "6 commodities and buyers"),
    ("forward_curve", "11", "Yahoo Finance (Contracts)", "Daily", "9 commodity forward curves"),
    ("wasde", "12", "USDA WASDE", "Monthly", "Supply and demand forecasts"),
    ("eia", "13", "EIA", "Weekly/Monthly", "Ethanol, biodiesel and diesel"),
    ("crush_inspections", "14", "USDA NASS + AMS", "Monthly/Weekly", "Crush and export inspections"),
    ("conab", "15", "CONAB", "Monthly", "Brazil crop estimates"),
    ("conab_precos", "15b", "CONAB", "Weekly", "Paraná farmgate prices"),
    ("india_domestic", "16", "data.gov.in (Agmarknet)", "Daily", "MP and Maharashtra mandi soy"),
    ("cepea", "17", "CEPEA via Notícias Agrícolas", "Daily", "Brazil soy indicators"),
    ("safex", "18", "JSE SAFEX via Grain SA", "Daily", "South Africa soy futures"),
    ("agrural", "19", "AgRural", "Daily", "Paranaguá FOB soy"),
    ("gulf_bids", "20", "USDA AMS", "Daily", "CIF NOLA Gulf bids"),
    ("magyp_fob", "21", "Argentina MAGyP", "Daily", "Official FOB beans, oil and meal"),
    ("ec_oilseeds", "22", "European Commission", "Weekly", "EU Moselle rapeseed FOB"),
    ("sagis", "23", "SAGIS", "Weekly", "South Africa producer deliveries"),
    ("sagis_smd", "24", "SAGIS", "Monthly", "South Africa soy supply and demand"),
    ("cec", "25", "Crop Estimates Committee (SA)", "Monthly", "Official crop estimates"),
)
PRODUCTION_LAYER_KEYS = tuple(layer[0] for layer in PRODUCTION_LAYERS)

# ---------------------------------------------------------------------------
# Layer 1 — yfinance ticker symbols (data sourced from CME / ICE / CBOT)
#
# Core commodities (soybean complex) PLUS competing/rotation
# crops and downstream demand. Without corn, wheat, sugar, and livestock
# the analysis would be misleading — these directly drive soybean acreage
# decisions and feed demand.
# ---------------------------------------------------------------------------
COMMODITY_TICKERS = {
    # ── Soybean complex (core) ──
    "Soybeans":     "ZS=F",   # CME/CBOT — benchmark global soybean price
    "Soybean Oil":  "ZL=F",   # CME/CBOT — cooking oil + biodiesel
    "Soybean Meal": "ZM=F",   # CME/CBOT — animal feed protein

    # ── Competing/rotation crops ──
    # Corn is THE #1 driver of soybean acreage: when corn is more profitable,
    # farmers plant less soy. Missing corn = missing the biggest supply signal.
    "Corn":         "ZC=F",   # CBOT — largest US crop, soybean rotation partner
    "Wheat":        "ZW=F",   # CBOT — competes for acreage, food inflation proxy
    "Sugar":        "SB=F",   # ICE — competes with ethanol, affects biofuel demand
    "Cotton":       "CT=F",   # ICE — competes for acreage in US South, Brazil, India

    # ── Downstream demand (feed) ──
    # Soybean meal IS animal feed. Not tracking livestock = blind to demand side.
    "Live Cattle":  "LE=F",   # CME — beef herd expansion = more meal demand
    "Lean Hogs":    "HE=F",   # CME — hog cycle drives meal consumption globally

    # ── Substitute oils ──
    # Palm oil is the #1 substitute for soy oil — daily tracking shows real-time competition
    # CME USD Malaysian Crude Palm Oil Calendar swap — marked off Bursa FCPO
    # settlements, natively USD/MT, settlement-marked (volume ≈ 0 by design)
    "Palm Oil (CME)": "CPO=F",

    # ICE canola (RS=F) — verified DEAD on yfinance 2026-08-08 (X1 go/no-go):
    # zero rows on every period (1mo/6mo/2y/max) and individual contract
    # tickers (RSF26.NYB etc.) 404. The daily rapeseed benchmark is CZCE
    # Rapeseed Oil (Layer 9, CNY/MT). Revisit if Yahoo restores ICE
    # Canada coverage — a CAD/USD pair + pipeline/units.py leg would then
    # be needed (canola quotes CAD/MT).
}

# How far back to pull historical data (yfinance period strings)
# 15y gives ~15 monthly observations per calendar month — enough to extract
# real soy seasonality (US harvest pressure, SA harvest, summer weather rallies)
# without bleeding into structural regime changes (pre-2010 ethanol ramp).
DEFAULT_HISTORY_PERIOD = "15y"

# Minimum observations per calendar month for a seasonal average to be reported.
# Below this we return None rather than a noisy short-window mean.
SEASONAL_MIN_YEARS_PER_MONTH = 5

# Settlement guard (see fetchers/_settlement.py).
#
# yfinance returns a row for the *current* session as soon as trading opens.
# Before the venue settles, that row is an unfinished bar — a real observed
# case stored ZS=F 2026-08-07 at 1181.25 when the settlement was 1156.50, a
# 2.1% error published as that day's close. Anything below the >10% move
# warning in pipeline/clean.py passes silently, so the guard is structural:
# the current session's bar is dropped until the venue has settled.
#
# One cutoff covers every venue we pull through yfinance, expressed in
# Chicago local time so US DST is handled by the zoneinfo database rather
# than by a hand-maintained UTC offset:
#   CBOT grains/oilseeds (ZS/ZL/ZM/ZC/ZW)   settle 13:15 CT
#   CME livestock (LE/HE), CME palm (CPO)   settle 13:05 CT
#   ICE US cotton (CT)                      settle 14:20 ET = 13:20 CT
#   ICE US sugar (SB)                       settle 13:00 ET = 12:00 CT
# 14:30 CT clears the latest of them with ~70 min of headroom for Yahoo to
# publish the settled bar.
SETTLEMENT_TIMEZONE = "America/Chicago"
SETTLEMENT_CUTOFF_LOCAL = (14, 30)  # (hour, minute) in SETTLEMENT_TIMEZONE

# ---------------------------------------------------------------------------
# Layer 2 — USDA NASS QuickStats API
# Sign up: https://quickstats.nass.usda.gov/api
# ---------------------------------------------------------------------------
USDA_API_KEY = os.getenv("USDA_API_KEY", "")
USDA_BASE_URL = "https://quickstats.nass.usda.gov/api/api_GET/"

# Commodities to fetch crop condition/progress for (weekly USDA data)
USDA_CROP_PROGRESS_COMMODITIES = ["SOYBEANS", "CORN"]

# ---------------------------------------------------------------------------
# Layer 3 — FRED (Federal Reserve Economic Data)
# Sign up: https://fred.stlouisfed.org/docs/api/api_key.html
#
# The yield curve (2Y/10Y/30Y) matters because:
#   - Rising rates strengthen the dollar (headwind for commodities)
#   - Rising rates increase storage/carry costs for physical commodities
#   - An inverted yield curve signals recession (demand destruction)
#   - Ethanol PPI tracks biofuel costs (soybean oil competes with ethanol)
# ---------------------------------------------------------------------------
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

FRED_SERIES = {
    # ── Macro context ──
    "US Dollar Index": "DTWEXBGS",   # Trade-weighted dollar index (broad)
    "CPI":             "CPIAUCSL",   # Consumer Price Index
    "Fed Funds Rate":  "FEDFUNDS",   # Federal funds effective rate

    # ── Yield curve ──
    "Treasury 2Y":     "DGS2",       # 2-year Treasury yield
    "Treasury 10Y":    "DGS10",      # 10-year Treasury yield
    "Treasury 30Y":    "DGS30",      # 30-year Treasury yield

    # ── Energy/biofuel ──
    "Ethanol PPI":     "WPU06140341",  # Producer Price Index: Ethanol
    # WPU0612 was discontinued by BLS (FRED 400s "series does not exist").
    # Replaced 2026-07 with the industry PPI for crude soybean oil, degummed
    # (monthly, history from 1988-05). NOTE: different index base than
    # WPU0612 — store.save_fred_data wipes old rows for this display name
    # so stored history never mixes the two bases.
    "Soybean Oil PPI": "PCU31122431122431",  # PPI Industry: Soybean/Oilseed Processing — Crude Soybean Oil, Degummed
    "Diesel Price":    "GASDESW",       # US diesel retail (biodiesel competition)
}

# ---------------------------------------------------------------------------
# Layer 4 — CFTC Commitment of Traders (COT)
# No API key needed — uses the cot_reports library to fetch from CFTC.gov
# Published weekly (Fridays), data from previous Tuesday
# ---------------------------------------------------------------------------
COT_REPORT_TYPE = "legacy_futopt"   # Legacy Futures-and-Options Combined

# CFTC contract market names (must match exactly what CFTC uses)
COT_COMMODITIES = {
    # ── Soybean complex ──
    "Soybeans":     "SOYBEANS - CHICAGO BOARD OF TRADE",
    "Soybean Oil":  "SOYBEAN OIL - CHICAGO BOARD OF TRADE",
    "Soybean Meal": "SOYBEAN MEAL - CHICAGO BOARD OF TRADE",

    # ── Competing crops ──
    "Corn":         "CORN - CHICAGO BOARD OF TRADE",
    "Wheat":        "WHEAT-SRW - CHICAGO BOARD OF TRADE",
    "Sugar":        "SUGAR NO. 11 - ICE FUTURES U.S.",
    "Cotton":       "COTTON NO. 2 - ICE FUTURES U.S.",

    # ── Livestock ──
    "Live Cattle":  "LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE",
    "Lean Hogs":    "LEAN HOGS - CHICAGO MERCANTILE EXCHANGE",

    # ── Cross-oilseed ──
    # ICE canola positioning — the price feed itself has no free daily
    # source (RS=F dead on yfinance), but CFTC still publishes the COT.
    "Canola":       "CANOLA - ICE FUTURES U.S.",
}

# ---------------------------------------------------------------------------
# Layer 5 — Weather data via Open-Meteo (free, no API key)
#
# Every major growing region for soybeans and palm oil worldwide.
# Missing a region means missing a weather event that could move prices.
# ---------------------------------------------------------------------------
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

GROWING_REGIONS = {
    # ── US Soybean Belt ──
    "US Midwest (Iowa)":          {"lat": 42.03,  "lon": -93.47},
    "US Illinois":                {"lat": 40.12,  "lon": -89.30},   # #1 soybean state

    # ── South America ──
    "Brazil Mato Grosso":         {"lat": -12.64, "lon": -55.42},   # #1 Brazil soy state
    "Brazil Parana":              {"lat": -24.04, "lon": -51.46},   # #2 Brazil soy state
    "Argentina Pampas":           {"lat": -33.95, "lon": -60.33},   # Soy belt
    "Argentina Cordoba":          {"lat": -31.42, "lon": -64.18},   # #2 Argentina soy province
    "Paraguay Chaco":             {"lat": -22.35, "lon": -59.95},   # Expanding soy frontier

    # ── Africa ──
    "Ivory Coast (Cocoa)":        {"lat": 6.83,   "lon": -5.29},   # Cross-reference

    # ── Asia ──
    "Indonesia Riau (Sumatra)":   {"lat": 0.29,   "lon": 101.71},  # #1 palm oil belt
    "Malaysia Sabah (Borneo)":    {"lat": 5.42,   "lon": 116.80},  # #2 palm oil state
    "India Madhya Pradesh":       {"lat": 22.72,  "lon": 75.86},   # India soybean capital
    "India Maharashtra":          {"lat": 19.75,  "lon": 75.71},   # #2 India soybean state
    "Thailand Surat Thani":       {"lat": 9.14,   "lon": 99.33},   # #3 global palm oil
    "China Heilongjiang":         {"lat": 47.36,  "lon": 127.76},  # China domestic soybean belt
    "China Jilin":                {"lat": 43.87,  "lon": 125.32},  # China corn belt (hog/feed demand)

    # ── Emerging Markets (soy deep dive) ──
    "South Africa Free State":    {"lat": -29.12, "lon": 26.21},   # SA #1 soy province
    "South Africa Mpumalanga":    {"lat": -25.47, "lon": 30.00},   # SA #2 soy province
    "Nigeria Benue":              {"lat": 7.73,   "lon": 8.52},    # Nigeria soy belt
    "Nigeria Kaduna":             {"lat": 10.52,  "lon": 7.43},    # Nigeria soy belt
}

WEATHER_DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"

# ---------------------------------------------------------------------------
# Layer 6 — USDA FAS PSD (global supply/demand, bulk CSV, no API key)
# Covers: soybeans, soybean oil, soybean meal, palm oil — every country
#
# Added corn and cotton for rotation crop tracking, and grains category
# for wheat coverage.
# ---------------------------------------------------------------------------
PSD_URLS = {
    "oilseeds": "https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip",
    "grains":   "https://apps.fas.usda.gov/psdonline/downloads/psd_grains_pulses_csv.zip",
    "cotton":   "https://apps.fas.usda.gov/psdonline/downloads/psd_cotton_csv.zip",
}

PSD_TARGET_COMMODITIES = {
    # ── Core ──
    "Soybeans":     "2222000",
    "Soybean Oil":  "4232000",
    "Soybean Meal": "813100",
    "Palm Oil":     "4243000",
    # ── Competing crops ──
    "Corn":         "440000",
    "Wheat":        "410000",
    "Cotton":       "2631000",
    # ── Cross-oilseed (rapeseed complex — Canada is the #1 canola origin) ──
    # Codes verified against the 2026 oilseeds bulk CSV. pandas reads
    # Commodity_Code as int, so leading zeros are stripped — Rapeseed Meal
    # is 0813600 ("Meal, Rapeseed") in the raw file, "813600" after read
    # (same convention as Soybean Meal 0813100 → "813100").
    "Rapeseed":      "2226000",
    "Rapeseed Oil":  "4239100",
    "Rapeseed Meal": "813600",
}

# Every country that materially affects our tracked commodities
PSD_TARGET_COUNTRIES = [
    # ── Americas ──
    "United States", "Brazil", "Argentina", "Paraguay",
    "Uruguay", "Bolivia", "Colombia", "Mexico",
    "Canada",  # #1 canola (rapeseed) origin
    # ── Asia ──
    "China", "India", "Indonesia", "Malaysia",
    "Thailand", "Vietnam", "Japan", "South Korea",
    "Pakistan", "Bangladesh",
    # ── Europe ──
    "European Union",
    # ── Africa ──
    "Ethiopia", "Nigeria", "South Africa",
    "Ivory Coast", "Tanzania", "Uganda", "Kenya",
    # ── Oceania ──
    "Australia",
]

PSD_TARGET_ATTRIBUTES = [
    "Production", "Imports", "Exports", "Crush",
    "Ending Stocks", "Domestic Consumption",
    "Beginning Stocks", "Total Supply", "Total Distribution",
]

# ---------------------------------------------------------------------------
# Layer 7 — Currency pairs via yfinance (export competitiveness)
#
# Every major producer/consumer currency. Without these, you can't tell
# whether a price move is a real commodity move or just a currency effect.
# ---------------------------------------------------------------------------
CURRENCY_TICKERS = {
    # ── South America ──
    "BRL/USD": "BRLUSD=X",   # Brazilian Real — THE most important soybean currency
    "ARS/USD": "ARSUSD=X",   # Argentine Peso — #3 soybean exporter
    "PYG/USD": "PYGUSD=X",   # Paraguayan Guarani — #4 soybean exporter

    # ── Asia ──
    "CNY/USD": "CNYUSD=X",   # Chinese Yuan — #1 soybean importer
    "IDR/USD": "IDRUSD=X",   # Indonesian Rupiah — #1 palm oil producer
    "MYR/USD": "MYRUSD=X",   # Malaysian Ringgit — #2 palm oil producer
    "INR/USD": "INRUSD=X",   # Indian Rupee — major soybean/palm oil consumer
    "THB/USD": "THBUSD=X",   # Thai Baht — #3 palm oil producer

    # ── Africa ──
    "ZAR/USD": "ZARUSD=X",   # South African Rand — emerging soy producer
    "NGN/USD": "NGNUSD=X",   # Nigerian Naira — emerging soy market (may have limited data)
}

# ---------------------------------------------------------------------------
# Layer 8 — World Bank Pink Sheet (monthly Palm Oil, Rapeseed Oil, etc.)
# The xlsx deep link contains a GUID that rotates yearly, and stale links
# keep returning HTTP 200 with frozen data (the 2025 GUID silently served
# Dec-2025 data through mid-2026). The fetcher therefore resolves the
# current link from the landing page first; WORLDBANK_PRICES_URL is only
# the fallback when the landing page is unreachable.
# ---------------------------------------------------------------------------
WORLDBANK_CMO_LANDING_URL = "https://www.worldbank.org/en/research/commodity-markets"
WORLDBANK_PRICES_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "74e8be41ceb20fa0da750cda2f6b9e4e-0050012026/related/"
    "CMO-Historical-Data-Monthly.xlsx"
)

# ---------------------------------------------------------------------------
# Layer 22 — European Commission Oilseeds Market Observatory (no API key)
#
# The EU rapeseed leg of the Europe market page. Euronext MATIF (ECO)
# settlements are licensed — free for internal use, EUR 167.55/month to
# redistribute, and this project publishes — so the futures curve is not
# ingested (#148). `Rapeseed - EU Moselle` is the Commission's weekly
# physical FOB assessment for the same commodity in the same region,
# CC BY 4.0, sourced by the EC from the International Grains Council.
#
# Same GUID trap as the Pink Sheet above: the CIRCABC deep link is opaque
# and a rotated-away link serves a frozen workbook with HTTP 200, so the
# fetcher resolves it from the landing page by *link text* each run. The
# pinned URL is only the fallback — note the upstream filename typo
# ("oliseeds"), which is exactly why the resolver does not match on it.
# ---------------------------------------------------------------------------
EC_OILSEEDS_LANDING_URL = (
    "https://agriculture.ec.europa.eu/data-and-analysis/markets/overviews/"
    "market-observatories/crops/oilseeds-and-protein-crops_en"
)
EC_OILSEEDS_WORLD_PRICES_URL = (
    "https://circabc.europa.eu/sd/a/"
    "2ddd7dcd-dff1-41b5-94b9-6cd207181a3c/oliseeds-world-prices.xlsx"
)

# Workbook column header → our series label.
#
# Scope is deliberately one series (#163). The same sheet also carries
# Soyabeans Argentina/Brazil/US Gulf/Ukraine, Rapeseed AU/CA/UA and
# Sunflowerseed EU Bordeaux/Ukraine — all parsed by the same code, so
# widening is an edit to this dict alone. They are left out under the
# standing preference carried from #130: only add a series that feeds a
# rendered line. (The soybean FOB columns are a licence-clean independent
# check on Layers 19/20/21 and are logged as fog on map #142, not built
# here. Sunflower enters the stack on the oil leg only, per #147, and this
# is seed.)
EC_OILSEEDS_SERIES = {
    "Rapeseed - EU Moselle": "EU Rapeseed (Moselle)",
}

# Every row in this layer is a weekly physical FOB assessment. Stored on the
# rows themselves rather than kept in a display-layer lookup, because map
# #142 carries a standing risk that board / physical / administered /
# assessment quotes get collapsed into one "price" line — a label that
# travels with the data cannot be lost by a consumer that forgets to look
# it up.
EC_OILSEEDS_CADENCE = "weekly"
EC_OILSEEDS_QUOTE_KIND = "physical FOB assessment"

# ---------------------------------------------------------------------------
# Layer 9 — DCE (Dalian Commodity Exchange) futures via AKShare (no API key)
# China is the world's largest soybean importer; DCE is the main exchange
# ---------------------------------------------------------------------------
# Two DCE bean contracts, and they are NOT interchangeable. No.1 (A) is the
# domestic non-GMO, food-grade bean (tofu/soymilk) and carries a ~700-1,100
# CNY/MT food premium; Chinese crushers do not crush it. No.2 (B) is the
# imported/GMO deliverable bean — the crush bean, and the only honest
# counterpart to CBOT for an import-parity comparison. The board crush and
# the vs-CBOT premium both key off No.2 (see analysis/spreads.py:_DCE_BEAN).
DCE_CONTRACTS = {
    "DCE Soybean No.1": "A0",   # Soybean No.1 continuous — domestic non-GMO food bean
    "DCE Soybean No.2": "B0",   # Soybean No.2 continuous — imported/GMO crush bean
    "DCE Soybean Meal": "M0",   # Soybean Meal continuous
    "DCE Soybean Oil":  "Y0",   # Soybean Oil continuous
    "DCE Palm Oil":     "P0",   # Palm Oil continuous
    "DCE Corn":         "C0",   # Corn continuous — China feed demand
    # CZCE rapeseed complex — same Sina feed, different exchange. The only
    # free *daily* rapeseed benchmark (Matif ECO has no free feed);
    # substitute-oil signal for the soy oil book.
    "CZCE Rapeseed Oil":  "OI0",  # Rapeseed Oil continuous
    "CZCE Rapeseed Meal": "RM0",  # Rapeseed Meal continuous
}

# ---------------------------------------------------------------------------
# Layer 10 — USDA FAS Export Sales Reporting (ESR)
# Sign up: https://apps.fas.usda.gov/opendataweb/home
# Weekly export sales — the #1 indicator of Chinese buying pace
# ---------------------------------------------------------------------------
FAS_API_KEY = os.getenv("FAS_API_KEY", "")
# New api.data.gov-fronted host (2026 ESRQS migration). The legacy
# apps.fas.usda.gov/OpenData host returns 500 for every key.
FAS_BASE_URL = "https://api.fas.usda.gov/api/esr"

# USDA FAS commodity codes for Export Sales Reporting
# Codes sourced from /api/esr/commodities endpoint
# ESR uses its own small-integer commodity codes (see /api/esr/commodities),
# NOT the 7-digit GATS/PSD codes. The API returns 500 on unknown codes.
EXPORT_SALES_COMMODITIES = {
    "Soybeans":     "801",
    "Soybean Oil":  "902",
    "Soybean Meal": "901",   # ESR name: "Soybean cake & meal"
    "Corn":         "401",
    "Wheat":        "107",   # ESR name: "All Wheat"
    "Cotton":       "1404",  # ESR name: "All Upland Cotton"
}

# Marketing-year start month per ESR commodity. Requesting every commodity
# with the September grain year is wrong for part of the calendar: wheat's
# MY starts Jun 1 and cotton's Aug 1, so from June/August onward those two
# must roll to the next market year before the soy complex does.
EXPORT_SALES_MY_START_MONTH = {
    "Wheat": 6,
    "Cotton": 8,
}
EXPORT_SALES_DEFAULT_MY_START = 9

# ---------------------------------------------------------------------------
# Layer 11 — Forward Curve (individual contract months via yfinance)
#
# The forward curve shows contango (future > spot = oversupply) vs
# backwardation (spot > future = tight supply). Essential for understanding
# carry costs and market sentiment.
#
# Ticker format: {root}{month_code}{2-digit year}.{exchange}
# Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
#              N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
# ---------------------------------------------------------------------------
MONTH_CODES = {
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

# root symbol, exchange suffix, and which calendar months trade
FORWARD_CURVE_CONTRACTS = {
    "Soybeans":     {"root": "ZS", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 11]},
    "Soybean Oil":  {"root": "ZL", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 10, 12]},
    "Soybean Meal": {"root": "ZM", "exchange": "CBT", "months": [1, 3, 5, 7, 8, 9, 10, 12]},
    "Corn":         {"root": "ZC", "exchange": "CBT", "months": [3, 5, 7, 9, 12]},
    "Wheat":        {"root": "ZW", "exchange": "CBT", "months": [3, 5, 7, 9, 12]},
    "Sugar":        {"root": "SB", "exchange": "NYB", "months": [3, 5, 7, 10]},
    "Cotton":       {"root": "CT", "exchange": "NYB", "months": [3, 5, 7, 10, 12]},
    "Live Cattle":  {"root": "LE", "exchange": "CME", "months": [2, 4, 6, 8, 10, 12]},
    "Lean Hogs":    {"root": "HE", "exchange": "CME", "months": [2, 4, 5, 6, 7, 8, 10, 12]},
}

# ---------------------------------------------------------------------------
# Layer 12 — WASDE Monthly Estimates (USDA OCE — oce-wasde-report-data.xls)
# THE most market-moving USDA report — monthly supply/demand projections.
# NASS QuickStats does not serve WASDE forecast rows, so this layer pulls
# the canonical XLS artifact directly from USDA OCE.
# ---------------------------------------------------------------------------
WASDE_COMMODITIES = ["SOYBEANS", "CORN", "WHEAT", "COTTON"]

# URL template — USDA OCE publishes one .xls per month as
# https://www.usda.gov/oce/commodity/wasde/wasdeMMYY.xls
WASDE_URL_TEMPLATE = "https://www.usda.gov/oce/commodity/wasde/wasde{mm:02d}{yy:02d}.xls"

# Fallback archive — USDA-ESMIS at the National Agricultural Library.
# usda.gov keeps only the last few months at the canonical path (older files
# 404), and reissued reports get a "v2" filename (wasde0526v2.xls) the
# template cannot construct. This keyless JSON API indexes every release with
# its exact per-release file URLs; one page (25 releases) covers ~2 years.
WASDE_ESMIS_API_URL = (
    "https://esmis.nal.usda.gov/api/v1/release/findByIdentifier/wasde?latest=false"
)

# How many monthly XLS files to attempt on first run. After backfill the
# pipeline still re-downloads the latest file every run, but historical
# rows are idempotent (INSERT OR REPLACE on the wasde PK).
WASDE_BACKFILL_MONTHS = 12

# Where each commodity's balance-sheet table lives inside the XLS.
# Sheets are named "Page N" (matching the PDF page numbers). Some sheets
# contain multiple sub-tables stacked vertically; `header_text` is the
# col-0 label that marks the start of the section. None means "the entire
# sheet is one table" (Wheat and Cotton each have their own page).
# "sheet" is only a fast-path hint — the parser locates each table by its
# "title" text (e.g. "U.S. Wheat Supply and Use"), so a USDA repagination
# (tables drifting to a different "Page N") doesn't silently break parsing.
WASDE_LAYOUT = {
    "WHEAT": {
        "sheet": "Page 11", "header_text": None,
        "title": "U.S. Wheat Supply and Use",
    },
    "CORN": {
        "sheet": "Page 12", "header_text": "CORN",
        "title": "U.S. Feed Grain and Corn Supply and Use",
    },
    "SOYBEANS": {
        "sheet": "Page 15", "header_text": "SOYBEANS",
        "title": "U.S. Soybeans and Products Supply and Use",
    },
    "SOYBEAN_OIL": {
        "sheet": "Page 15", "header_text": "SOYBEAN OIL",
        "title": "U.S. Soybeans and Products Supply and Use",
    },
    "SOYBEAN_MEAL": {
        "sheet": "Page 15", "header_text": "SOYBEAN MEAL",
        "title": "U.S. Soybeans and Products Supply and Use",
    },
    "COTTON": {
        "sheet": "Page 17", "header_text": None,
        "title": "U.S. Cotton Supply and Use",
    },
}

# ---------------------------------------------------------------------------
# Layer 13 — EIA Biofuel/Energy Data
# Sign up: https://www.eia.gov/opendata/register.php
# Soybean oil increasingly goes to renewable diesel (~40% of US soy oil demand)
# ---------------------------------------------------------------------------
EIA_API_KEY = os.getenv("EIA_API_KEY", "")
EIA_BASE_URL = "https://api.eia.gov/v2/"

EIA_SERIES = {
    "Ethanol Production": {
        "route": "petroleum/sum/sndw/data",
        "series": "W_EPOOXE_YOP_NUS_MBBLD",
        "frequency": "weekly",
    },
    "Biodiesel Production": {
        # EIA retired the sndm route; monthly S&D now lives under snd
        # EPOORDB is biodiesel (renewable diesel is the sibling EPOORDO) —
        # verified against EIA's series page 2026-08
        "route": "petroleum/sum/snd/data",
        "series": "M_EPOORDB_YNP_NUS_MBBLD",
        "frequency": "monthly",
    },
    "Diesel Retail Price": {
        "route": "petroleum/pri/gnd/data",
        "series": "EMD_EPD2D_PTE_NUS_DPG",
        "frequency": "weekly",
    },
}

# ---------------------------------------------------------------------------
# Layer 14 — USDA Crush/Processing + Export Inspections
# Crush = domestic demand, Inspections = actual export shipments
# ---------------------------------------------------------------------------
INSPECTIONS_URL = "https://www.ams.usda.gov/mnreports/wa_gr101.txt"

# ---------------------------------------------------------------------------
# Layer 15 — CONAB Brazil Crop Estimates
# Brazil's official crop agency — often differs from USDA by millions of tonnes
# ---------------------------------------------------------------------------
CONAB_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt"

# ---------------------------------------------------------------------------
# Layer 15b — CONAB weekly producer (farmgate) prices
# Cross-check series for the CEPEA/ESALQ Paraná indicator (Layer 17).
# Semicolon-separated, latin-1, comma decimal, R$/kg by UF and week.
# Stored under its own commodity key — NEVER spliced into the CEPEA series
# (farmgate vs wholesale; a ~10-14% spread is expected and is the signal).
# ---------------------------------------------------------------------------
CONAB_PRECOS_URL = "https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt"
CONAB_FARMGATE_PRODUCT = "SOJA"
# dsc_nivel_comercializacao is truncated to 20 chars in the file; this prefix
# selects "producer price received" (farmgate) rows.
CONAB_FARMGATE_LEVEL_PREFIX = "PREÇO RECEBIDO"
CONAB_FARMGATE_UF = "PR"          # Paraná — same state as the CEPEA indicator
CONAB_FARMGATE_SERIES = "Soybean (CONAB PR farmgate)"

# ---------------------------------------------------------------------------
# Layer 16 (retired source) — NCDEX India domestic soy prices
# NCDEX Bhav Copy: daily settlement prices in INR/quintal or INR/MT
#
# Dormant since 2026-05 (fingerprint anti-bot wall on the spot pages) and
# superseded 2026-08 by the data.gov.in mandi API below: NCDEX soy
# derivatives are SEBI-suspended to at least 2027-03-31, so the bhavcopy
# carries no soy contracts even where it downloads. Constants kept for
# fetchers/india_domestic.py, the dormant fallback module.
# ---------------------------------------------------------------------------
NCDEX_BHAVCOPY_URL_TEMPLATES = [
    "https://www.ncdex.com/bdocuments/bhavcopy/bhavcopy_{date}.csv",
    "https://www.ncdex.com/Downloads/Bhavcopy/ncdex_bhavcopy_{date}.csv",
]

# Maps our commodity name → list of NCDEX symbol aliases to search for in CSV
NCDEX_SOY_SYMBOLS = {
    "Soybean (NCDEX)":     ["SYBEANIDR", "SOYBEAN", "SOYBEANIDR"],
    "Soybean Oil (NCDEX)": ["SYOIL", "REFSOLOIL", "SOYOIL"],
    "Soybean Meal (NCDEX)":["SOYMEAL", "SYBEANMEAL"],
}

# Multiplier to convert NCDEX native unit → INR/MT
# Soybeans/meal are typically Rs/quintal (100 kg) → ×10 = INR/MT
# Soy oil is typically Rs/10kg → ×100 = INR/MT
NCDEX_UNIT_MULTIPLIER = {
    "Soybean (NCDEX)":     10.0,
    "Soybean Oil (NCDEX)": 100.0,
    "Soybean Meal (NCDEX)": 10.0,  # Rs/quintal → INR/MT, same as beans
}

# ---------------------------------------------------------------------------
# Layer 16 (2026-08 rebuild) — India domestic soy spot via data.gov.in
# Mandi Price API (official Agmarknet feed, Ministry of Agriculture).
# Bean-only: the resource has no soy meal commodity and its soy-oil rows
# carry inconsistent units across mandis, so the old NCDEX oil/meal legs
# (and the India crush margin built on them) are retired until NCDEX
# derivatives return (SEBI suspension runs to at least 2027-03-31).
#
# The published sample key is officially for testing, caps responses at
# 10 rows/request, and shares a global throttle (occasional 429s) — but
# ~8 paginated requests/day cover the full Madhya Pradesh soybean set.
# A personal key via the DATA_GOV_IN_API_KEY env var is a drop-in upgrade.
# ---------------------------------------------------------------------------
MANDI_API_URL = "https://api.data.gov.in/resource/9ef84268-d588-465a-a308-a864a43d0070"
# Published in the data.gov.in API docs — a public testing credential, not a secret.
MANDI_SAMPLE_API_KEY = "579b464db66ec23bdd000001cdd3946e44ce4aad7209ff7b23ac571b"  # public-sample-key: not a secret
MANDI_COMMODITY = "Soyabean"      # Agmarknet's spelling
MANDI_PAGE_LIMIT = 10             # sample-key hard cap per request
MANDI_PAGE_LIMIT_PERSONAL = 100   # personal keys allow bigger pages → fewer throttle hits
MANDI_MAX_PAGES = 30              # safety stop: 30 × 10 rows ≫ any single-state daily set
# Elasticsearch-style offset paging over an *unsorted* index is not stable:
# without this the same mandi comes back on two pages while another is never
# served at all (verified 2026-08-12: 115 MP rows fetched, only 95 distinct,
# so ~20 real mandis were silently missing and Volume over-counted by 21%).
# ``market.keyword`` is an exposed keyword field — see the resource's
# ``field_exposed`` block — and gives a total order across pages.
MANDI_SORT_FIELD = "market.keyword"
# Unit guard, ₹/quintal. ``modal_price`` is quoted per quintal (100 kg) and
# multiplied by 10 into INR/MT; a source that switched to ₹/kg (~67) or
# ₹/MT (~67,000) would still parse cleanly and silently restate the level
# 10–100×. Validated 2026-08-11 at ₹6,725/qtl MP against three independent
# quotes (#206), and the band is wide enough for any real market: India's
# 2021 record was ~₹10,000/qtl and the 2008 low ~₹2,200/qtl.
MANDI_MODAL_MIN_INR_QUINTAL = 1_000
MANDI_MODAL_MAX_INR_QUINTAL = 20_000
# Fresh series keys — mandi farmgate spot is a different instrument from the
# retired NCDEX futures series and must never be spliced onto it.
MANDI_SERIES = "Soybean (Mandi MP)"     # headline: Indore is the crush-industry pricing hub
MANDI_SERIES_MH = "Soybean (Mandi MH)"  # Maharashtra — #1 producing state since 2025-26
# One median series per state, fetched from the same resource. SOPA Kharif
# 2025 estimates: Maharashtra 52.2 vs Madhya Pradesh 43.2 lakh tonnes —
# MP alone is ~39% of the crop; MP + MH cover ~86% (issue #44 research).
MANDI_STATES = {
    "Madhya Pradesh": MANDI_SERIES,
    "Maharashtra": MANDI_SERIES_MH,
}

# ---------------------------------------------------------------------------
# Layer 17 — CEPEA/ESALQ Brazil domestic soy spot price (free, no API key)
# Farm-gate reference indicator for Paraná state in BRL per 60kg bag.
# This is NOT the Paranaguá port FOB — see Layer 19 (AgRural) for that.
# ---------------------------------------------------------------------------
CEPEA_SOYBEAN_URL = "https://www.cepea.org.br/en/indicator/soybean.aspx"
CEPEA_COMMODITIES = ["Soybean (CEPEA)"]

# cepea.org.br itself sits behind a Cloudflare Turnstile challenge
# (2026-05). Notícias Agrícolas republishes the same CEPEA/ESALQ
# indicators server-rendered — this is the active Layer 17 source.
# Appending /YYYY-MM-DD to a URL returns that date's page (~10 sessions
# per page), which is how the backfill script walks the gap.
# Commodity keys reuse the historical names so stored history continues.
NOTICIAS_AGRICOLAS_URLS = {
    # CEPEA/ESALQ Paraná — the classic farm-gate CEPEA soy indicator
    "Soybean (CEPEA)": (
        "https://www.noticiasagricolas.com.br/cotacoes/soja/"
        "indicador-cepea-esalq-soja-parana"
    ),
    # ESALQ/B3 Paranaguá — port-side indicator, cross-check for AgRural FOB
    "Soybean (ESALQ/B3 Paranaguá)": (
        "https://www.noticiasagricolas.com.br/cotacoes/soja/"
        "soja-indicador-cepea-esalq-porto-paranagua"
    ),
}

# ---------------------------------------------------------------------------
# Layer 20 — US Gulf export basis bids (USDA AMS report 3147, no API key)
# Daily "Louisiana and Texas Export Bids" PDF — CIF Gulf (NOLA barge)
# export-elevator bids for soybeans/corn/wheat, basis in cents/bu over the
# named CBOT contract. The legacy mnreports .txt grain bid endpoints froze
# in 2020-2022 after the MARS migration; this PDF is the live keyless feed.
# ---------------------------------------------------------------------------
AMS_GULF_BIDS_URL = "https://www.ams.usda.gov/mnreports/ams_3147.pdf"

# ---------------------------------------------------------------------------
# Layer 21 — Argentina official FOB prices (MAGyP, free JSON, no API key)
# Daily "Precios FOB Oficiales" web service of the Secretaría de Agricultura
# (SAGyP/MAGyP): official minimum FOB export values in USD/t, published per
# NCM tariff position with shipment-window columns. Business days only; the
# ?Fecha=dd/mm/yyyy parameter also serves historical dates (backfill-capable).
#
# POSITION → PRODUCT MAPPING IS CROSS-CHECKED NUMERICALLY, NEVER INFERRED.
# The service publishes no `descripcion` field — only the NCM code — and 3 of
# 4 meal codes inferred from the nomenclature were wrong the first time this
# was checked (#147), which invalidated part of M7. Every code below is
# verified against the *labelled* datos.gob.ar mirror of the same circular
# (sspm dataset 358, "Precios FOB oficiales"), which carries a Spanish
# description per series.
#
# Verification method (2026-08-12, re-run over 2024-11-01 → 2025-01-21, the
# window before the mirror stops at 2025-01-21):
#   For each of 52 business days, the labelled value must equal one of the
#   *shipment-window* prices this position published that day. Result: 312 of
#   312 product-days matched exactly, zero exceptions.
#
# THE MIRROR IS NOT WINDOW-ALIGNED WITH US, so a naive "labelled == nearest
# window" test under-reports badly and must not be used: the mirror tracks a
# campaign window, which is the nearest one only in-season. On 2024-11-04 the
# labelled bean price 390 was the 2025-03/2025-10 window while the nearest
# window printed 416; that test scores beans 15/52 and looks like a bad code.
# Membership in the day's window set is the honest check — hence 312/312.
#
# Bulk ("granel") positions are the benchmark legs; the bagged ("embolsado")
# sub-positions run ~$20 over and are not stored.
# ---------------------------------------------------------------------------
MAGYP_FOB_URL = (
    "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios"
    "/ws/ssma/precios_fob.php"
)
# Verified against these labelled dataset-358 series:
#   12019000190C → 358.1_HABAS_SOJAADO__52 "Habas de soja ... A granel"
#   15071000100Q → 358.1_ACEITE_SOJNEL__18 "Aceite de soja, a granel"
#   23040010100B → 358.1_TORTAS_EXPXTR__56 "... Harina de soja, Pellets, de
#                  harina de extracción"  (was the open unverified code; the
#                  Argentina crush is no longer provisional — see MARKETS)
#   15121110310E → 358.1_ACEITE_GIRNEL__21 "Aceite de Girasol, a granel"
#
# Sunflower enters on the OIL LEG ONLY (#147): the seed (1206.00.90) and meal
# (2306.30) positions are administered step-functions that sat unchanged for
# months, so they render as levels, not lines, and are deliberately absent.
# Refined sunflower oil (1512.19) is a different good and is not the veg-oil
# board's leg — 15121919110H (refined granel) ran ~$170/MT over crude and
# maps to a separate labelled series, 358.1_ACEITE_GIRNEL__36.
#
# CRUDE SUNFLOWER OIL IS PUBLISHED UNDER THREE SIM LINES CARRYING ONE PRICE:
# 15121110310E, 15121110911P and 15121110919G were byte-identical on all 66
# published circulars 2026-05-01 → 2026-08-11 and on the 2024-11 → 2025-01
# window. Only 310E is mapped, so one economic number stores one row (the
# `position` column is part of both the primary key and the git-committed
# history CSV — mapping all three would triple the row and a later change of
# canonical code would fork the series). `_parse_posts` guards both halves of
# that assumption: it hard-fails if a mapped product goes missing from a
# published circular, and if the unmapped siblings ever quote a *different*
# price than the mapped line.
MAGYP_FOB_POSITIONS = {
    "12019000190C": "Soybeans",      # habas de soja, las demás — granel
    "15071000100Q": "Soybean Oil",   # aceite de soja en bruto — granel
    "23040010100B": "Soybean Meal",  # pellets de soja, harina de extracción
    "15121110310E": "Sunflower Oil",  # aceite de girasol en bruto — granel
}
# The other two SIM lines carrying the crude-sunflower-oil price. Not stored;
# watched, so a split between them cannot pass silently as agreement.
MAGYP_SUNFLOWER_OIL_SIBLINGS = ("15121110911P", "15121110919G")
# Walk back this many calendar days to find the latest published circular
# (weekends + Argentine holidays publish nothing).
MAGYP_FOB_LOOKBACK_DAYS = 7

# ---------------------------------------------------------------------------
# Layer 18 — SAFEX/JSE South Africa domestic soy prices (free, no API key)
# JSE agricultural *last traded* prices in ZAR/MT — not settlement/MTM. The
# Grain SA table carries no settlement column and the JSE's own MTM file is
# licensed (#157); see fetchers/safex.py for the full note.
# ---------------------------------------------------------------------------
SAFEX_STATS_URL = "https://www.grainsa.co.za/pages/industry-reports/safex-feeds"
# Display names stored by fetchers/safex.py; the JSE contract-code mapping
# (SOYB/SUNS) lives in the fetcher itself.
SAFEX_COMMODITIES = (
    "Soybean (SAFEX)",
    "Sunflower (SAFEX)",
)

# ---------------------------------------------------------------------------
# Layer 19 — AgRural Paranaguá FOB soy quote (free, no API key)
# Trade-convention Brazil basis benchmark: daily Paranaguá port buy price
# in BRL per 60kg bag, converted to BRL/MT downstream.
#
# Redistribution constraint: raw BRL/saca quotes are stored locally only.
# The public dashboard publishes only the derived USD/MT basis.
# ---------------------------------------------------------------------------
AGRURAL_URL = "https://agrural.com.br/precossojaemilho/"
AGRURAL_COMMODITIES = ["Soybean (AgRural Paranaguá FOB)"]

# ---------------------------------------------------------------------------
# Layer 23 — SAGIS weekly producer deliveries (free, no API key)
#
# South Africa's first *physical flow* series: tonnage delivered by producers
# into commercial storage each week — the SA analogue of USDA export
# inspections (Layer 14), and a better fit for a physical-buyer product than
# the licence-capped SAFEX price leg (#157 → #202).
#
# Licence: "SAGIS' information may be reproduced with the acknowledgement of
# the source." An explicit reproduction grant, materially better than SAFEX
# (RED). SAGIS_ATTRIBUTION below is the string every surface must render.
#
# We take the machine-readable `DT-SWP-<Commodity>_<year>_<week>.xlsx` export,
# not the `ProdProgressive-*` presentation workbook: the DT file is a flat
# 9-column table needing no header sniffing, and it carries *all* seasons
# (2018–2026 as of week 22/2026, 440 rows) rather than one season per file.
#
# The URL is week-stamped, never rolling, so it MUST be resolved from the
# listing page on every run. A hardcoded deep link would serve a frozen week
# at HTTP 200 forever — the same trap as the World Bank CMO GUID in Layer 8.
# ---------------------------------------------------------------------------
SAGIS_WEEKLY_DATA_URL = "https://www.sagis.org.za/sagis-weekly-data/"
SAGIS_ATTRIBUTION = "Source: SAGIS (South African Grain Information Service)"
# Filename commodity token → the commodity key stored in `sagis_deliveries`.
# Renaming a key here forks the series in data/history/sagis_deliveries.csv,
# where the old key would be re-seeded forever — treat these as frozen.
SAGIS_COMMODITIES = {
    "Soybeans": "Soybeans (SAGIS)",
    "Sunflower": "Sunflower Seed (SAGIS)",
}

# ---------------------------------------------------------------------------
# Layer 24 — SAGIS monthly soybean supply & demand (free, no API key)
#
# SA2 (#203) was filed for SAGIS's *weekly* imports/exports and its 8-week
# forward intentions. Verified live 2026-08-12: those two products exist for
# **maize and wheat only** — the weekly page's own section header reads
# "Weekly Imports & Exports … MAIZE | WHEAT", and the only files are
# `Intended-{MAIZE,WHEAT}-WeekEnding_*.xlsx` and
# `IMP-EXP_Progressive_{Mielies,Koring}_*`. There is no soybean or sunflower
# trade file on the weekly page at all, so the forward series the ticket was
# filed for does not exist for the soy complex.
#
# South African soybean trade *does* exist, monthly, on the SMD (Supply and
# Demand) page — and it carries more than trade: imports, exports split
# border-posts vs harbours, **tonnes processed** (SA's crush volume) and
# closing stock split storers-vs-processors. M7 ruled out an SA crush
# *margin* (SAFEX is seed-only and the JSE meal/oil contracts are
# cash-settled CBOT); a crush *volume* is a different quantity and is not
# barred by that finding.
#
# Which file: the **season-progressive** workbook
# `Sojabone<season><season+1>_<pubdate>[_F].xlsx`, which carries all twelve
# months of one March–February season in one sheet. The per-month
# announcement files (`Sojabone<YYYYMMDD>.xlsx`) hold only two months each
# and would need one request per month of history.
#
# Both the URL *and* the set of published seasons rotate: the current
# season's file is re-published every month under a new filename, and only
# the current season plus the two most recent finals are listed. Resolve
# from the landing page every run (Layer 8 / Layer 23 trap), and round-trip
# the table through data/history/ so a season that rolls off the page is not
# lost from an ephemeral CI database.
#
# Licence: the same SAGIS reproduction grant as Layer 23 —
# SAGIS_ATTRIBUTION must be rendered wherever these numbers appear.
# ---------------------------------------------------------------------------
SAGIS_MONTHLY_DATA_URL = "https://www.sagis.org.za/sagis-monthly-data/"
# Filename commodity token (Afrikaans on this page) → stored commodity key.
# Deliberately the same key string Layer 23 stores, so the two SAGIS tables
# join on commodity. Frozen for the same history-CSV reason as above.
SAGIS_SMD_COMMODITIES = {
    "Sojabone": "Soybeans (SAGIS)",
}

# ---------------------------------------------------------------------------
# Layer 25 — Crop Estimates Committee (CEC), South Africa (free, no API key)
#
# South Africa's official area/production estimate, revised monthly through
# the season. Structurally the SA analogue of Layer 15 (CONAB) — but *not* an
# independent second opinion: USDA's PSD carries the CEC's final figure
# verbatim at PSD year = CEC year − 1 (2,770,000 / 1,848,000 / 2,800,000 t for
# the 2023 / 2024 / 2025 crops are exact ties). What the layer buys is the
# in-season revision path and the lead on the PSD number, not a divergence
# between two agencies, so nothing here renders as "CEC vs USDA" (#204).
#
# Issuer: the Crop Estimates Committee of the national Department of
# Agriculture. Fetched from the SAGIS mirror because the issuer is the weaker
# host — dalrrd.gov.za no longer resolves (2026-08-12) and its replacement,
# nda.gov.za, publishes no parseable CEC listing. A mirror that silently
# stops updating is caught by the recency budget below, since every release
# carries its own date.
#
# Licence: no copyright notice on the releases (an official government
# statistic), reproduced under SAGIS's "may be reproduced with the
# acknowledgement of the source". CEC_ATTRIBUTION names both.
# ---------------------------------------------------------------------------
CEC_REPORTS_URL = "https://www.sagis.org.za/crop-estimates-committee-2/"
CEC_ATTRIBUTION = (
    "Source: Crop Estimates Committee, Department of Agriculture (South "
    "Africa), via SAGIS"
)
# Crop label as printed in the CEC summary table → the stored commodity key.
# Renaming a key here forks the series wherever it is already stored.
CEC_CROPS = {
    "Soybeans": "Soybeans (CEC)",
    "Sunflower seed": "Sunflower Seed (CEC)",
}
# Releases before this date are legacy binary .doc files (2013–2024) or
# layouts from a different decade. Inside the window every PDF must parse.
CEC_HISTORY_START = _date(2025, 1, 1)
# CEC commodity → the PSD commodity holding USDA's view of the same crop,
# and the year offset between the two calendars. The CEC dates a crop by the
# calendar year it is harvested in (2026 = the 2025/26 season); PSD keys the
# same crop to the marketing year that starts in the *previous* calendar year,
# so PSD year = CEC season − 1. Verified on three exact ties of the CEC final
# crop against PSD Production (2,770,000 / 1,848,000 / 2,800,000 t for the
# 2023 / 2024 / 2025 crops).
#
# Sunflower seed has no PSD counterpart in PSD_TARGET_COMMODITIES, so it
# carries no USDA comparison — which is correct rather than a gap: the line
# only exists where both agencies publish the same crop.
CEC_PSD_COUNTERPARTS = {"Soybeans (CEC)": "Soybeans"}
CEC_PSD_YEAR_OFFSET = -1
# Implied-yield sanity band (t/ha) for the tracked crops. Observed 2024–2026:
# soybeans 1.57–2.51, sunflower seed 1.28–1.53. Wide enough not to fire on a
# drought or a record, tight enough to catch a column mix-up.
CEC_YIELD_BAND_T_HA = (0.3, 5.0)

# ---------------------------------------------------------------------------
# Analysis thresholds — configurable per-commodity where appropriate
# ---------------------------------------------------------------------------

# RSI levels (industry standard 70/30, but can be tuned)
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# Volume spike: multiple of 20-day average volume to flag as unusual
VOLUME_SPIKE_MULTIPLIER = 2.0

# Soybean crush yield factors — one 60-lb bushel of beans yields ~11 lbs
# of oil (priced in cents/lb) and ~44 lbs of meal (priced in $/short ton,
# hence 44/2000*100 = 2.2 in cents/bu terms). Board-crush convention;
# shared by the CBOT crush spread and every domestic crush-margin
# comparison so a yield-assumption change is a one-line edit.
CRUSH_OIL_FACTOR = 11.0
CRUSH_MEAL_FACTOR = 2.2

# Weather alert thresholds
WEATHER_HEAVY_RAIN_MM = 20      # mm precipitation to flag
WEATHER_EXTREME_HEAT_C = 38     # degrees C to flag as crop stress
WEATHER_DRY_THRESHOLD_MM = 1    # below this = "dry conditions"

# Agronomic alerting (observed rows only — forecast rows are excluded)
# Pod-fill heat stress: soy yield loss starts well below the generic 38C
# extreme-heat bar. During pod fill, sustained days >34C abort pods.
WEATHER_POD_FILL_HEAT_C = 34
# Soy regions with their pod-fill months (US: Jul-Aug; South America: Jan-Feb).
# Keys must match GROWING_REGIONS names.
WEATHER_SOY_POD_FILL_MONTHS = {
    "US Midwest (Iowa)":   (7, 8),
    "US Illinois":         (7, 8),
    "Brazil Mato Grosso":  (1, 2),
    "Brazil Parana":       (1, 2),
    "Argentina Pampas":    (1, 2),
    "Argentina Cordoba":   (1, 2),
    "Paraguay Chaco":      (1, 2),
}
# Consecutive-dry-day spell: days with precip < WEATHER_DRY_THRESHOLD_MM.
WEATHER_DRY_SPELL_ALERT_DAYS = 10
# 30-day precipitation deficit vs the region's trailing norm. The norm is the
# mean daily precip over the baseline window immediately preceding the 30-day
# window; we require a minimum number of baseline days before trusting it.
WEATHER_PRECIP_DEFICIT_WINDOW_DAYS = 30
WEATHER_PRECIP_DEFICIT_BASELINE_DAYS = 90
WEATHER_PRECIP_DEFICIT_MIN_BASELINE_OBS = 45
WEATHER_PRECIP_DEFICIT_ALERT_PCT = 40   # alert when 30d total ≥40% below norm

# Data freshness: warn if a layer hasn't updated in this many days.
# FRESHNESS_WARNING_DAYS is the default for daily layers; layers with a
# slower publication cadence get their own threshold below — otherwise
# weekly COT (Friday release of Tuesday data) and monthly WASDE/PSD would
# warn permanently and train the reader to ignore the freshness block.
FRESHNESS_WARNING_DAYS = 7
FRESHNESS_WARNING_DAYS_BY_LAYER = {
    # Weekly publications — allow a missed week before warning.
    "cot": 12,
    "export_sales": 12,
    "crop_progress": 12,
    "crush_inspections": 12,
    "sagis": 12,
    "ec_oilseeds": 12,
    # Monthly publications — allow ~6 weeks.
    "sagis_smd": 42,
    "wasde": 42,
    "psd": 42,
    "conab": 42,
    "cec": 42,
    "worldbank": 42,
    "eia": 42,
    "usda": 400,  # annual NASS crop data
}


def freshness_limit_days(layer: str) -> int:
    """Days a layer may go without a successful run before it reads stale.

    Single source for the three surfaces that judge run-freshness — the
    briefing warning block, the dashboard Layer Freshness table, and
    `analysis.health` — so a cadence added above reaches all of them at
    once. The sidebar drifted onto a hardcoded 7 and spent months calling
    healthy monthlies "old" (issue #176).
    """
    return FRESHNESS_WARNING_DAYS_BY_LAYER.get(layer, FRESHNESS_WARNING_DAYS)

# Recency budget for "success" (audit F3). FRESHNESS_WARNING_DAYS_BY_LAYER
# above measures when we last *ran*; this measures how old the newest
# observation we *received* is allowed to be. Without it a frozen upstream
# — one that answers 200 OK with last month's file every day — stamps a
# fresh last_success forever and no surface downstream can tell.
#
# A layer whose newest observation exceeds its budget is recorded with
# status='failed', which preserves the previous last_success. That is the
# mechanism: last_success stops advancing, so the layer ages out of its
# FRESHNESS_WARNING_DAYS_BY_LAYER window on its own and shows up stale on
# every surface that already reads freshness.
#
# Budgets are calendar days from the newest observation date and are sized
# to survive the layer's normal quiet periods (weekends, exchange holidays,
# publication lag) — a threshold that cries wolf gets ignored, which is the
# failure mode this whole block exists to avoid.
#
# NOT LISTED = NOT CHECKED. A layer is only listed when "newest observation
# date" is a meaningful quantity for it:
#   psd, wasde, usda  — keyed by marketing year / crop year, no date column
#                       at all (verified against the live schema), so there
#                       is nothing to measure.
#   forward_curve     — rows are dated by *contract month*, i.e. months in
#                       the future; a frozen curve stays "recent" for a year.
#   crop_progress     — seasonal by design; NASS publishes nothing between
#                       roughly December and March, so any budget short
#                       enough to be useful fires every winter.
# These four are covered by the run-cadence window, not by data recency.
LAYER_MAX_DATA_AGE_DAYS = {
    # Daily exchange/market data — a long weekend plus a holiday.
    "prices": 7,
    "currencies": 7,
    # SAFEX is a *stale-serving* page: on a non-trading day Grain SA re-serves
    # the previous session's rows rather than emptying (verified 2026-08-02 and
    # 2026-08-08). So "rows came back" says nothing about whether the JSE/BVG
    # feed is still moving — if it froze, the same rows would return forever
    # and the layer would stay green. Same long-weekend-plus-holiday budget as
    # the other daily exchange legs (#157).
    "safex": 7,
    # Agmarknet mandi spot. The API serves the *current day only*, so history
    # exists only as rows we already stored — a frozen or silently-emptied
    # upstream leaves the newest Date standing still while the layer keeps
    # returning 200. Same long-weekend-plus-holiday budget as the other daily
    # legs. Two enforcement points off one number: main.py stops stamping
    # last_success, and app/markets.py's tier probe demotes the India page to a
    # brief within the week rather than on the 14-day default (M19 #222).
    #
    # Known risk, deliberately accepted (#212): India's closure calendar is the
    # longest of any daily leg here, and the Diwali stretch (Dhanteras through
    # Bhai Dooj, ~5 days — 17–23 Oct in 2026) with a Sunday at each end could
    # exceed 7 and fire a false stale, demoting the India page over an ordinary
    # festival week. 7 is kept anyway because a *full* blackout needs every one
    # of ~115 reporting mandis per state shut, not just the Indore hub, which
    # makes the real worst case very likely shorter. Nothing settles this from
    # our own data yet: history starts 2026-08-10 and the resource serves only
    # the current day, so there is no observed multi-day gap to measure.
    # Revisit at the first Diwali (Oct 2026) with a real gap in hand.
    "india_domestic": 7,
    "fred": 10,        # 1-day publication lag on the daily series
    "weather": 10,     # includes forecast rows, so age is normally negative
    "dce": 21,         # Spring Festival / Golden Week close the DCE for ~2 weeks
    # Weekly publications — observation lag on top of the weekly cadence,
    # plus room for one missed release.
    "cot": 18,         # Friday release reports the *previous Tuesday*
    "export_sales": 21,
    "eia": 21,
    # SAGIS publishes producer deliveries at 12:00 on the 3rd working day of
    # each week, and the observation it stamps is the *previous* week's end
    # date — so the newest week_end is already ~5 days old the moment it
    # lands, and ~12 days old the day before the next release. 21 days is
    # that normal worst case plus one missed publication; anything tighter
    # fires on an ordinary public-holiday week. The DT export is also a
    # candidate for freezing (a week-stamped URL that keeps resolving), which
    # is exactly what this budget is here to catch.
    "sagis": 21,
    # SAGIS's monthly SMD lands around the 24th–27th and reports through the
    # *previous* calendar month, so the newest month_end is ~25 days old at
    # publication and ~55 days old the day before the next release. 90 days
    # is that worst case plus one missed publication. The season file's URL
    # rotates monthly, so a frozen link is a live risk here too.
    "sagis_smd": 90,
    # Wednesday-dated assessment published the following day, so the newest
    # row is normally 1-8 days old. 21 tolerates exactly one missed release
    # and fails on two — and the cadence has never actually missed one:
    # all 397 gaps across 2018-12-26 → 2026-08-05 are exactly 7 days.
    "ec_oilseeds": 21,
    # The CEC publishes monthly, but only Jan-Nov carry a summer-crop table:
    # the December release covers winter cereals alone, so the soybean series
    # has one legitimate ~61-day gap a year (27 Nov 2025 -> 27 Jan 2026). 70
    # days is that gap with a little slack; anything tighter would fail the
    # layer every January for a source behaving exactly as it should.
    "cec": 70,
    # Monthly publication. 100 days matches the identical guard inside
    # fetchers/worldbank.py — the CMO deep link rotates yearly and the old
    # GUID keeps serving a frozen file with HTTP 200. One number, one
    # meaning, two enforcement points.
    "worldbank": 100,
}

# Key coverage (#182): which config catalog each multi-key layer iterates.
# `keys_expected` is len(catalog), so adding a ticker/region/series moves a
# layer's expected count with no second edit — the trailing counts in
# LAYER_MIN_KEYS are comments and two of them had already drifted.
#
# The denominator cannot come from the payload: every per-key fetcher
# inserts into its results dict only *after* a successful fetch, so a
# weather run that lost 5 of 19 regions returns a 14-key dict and would
# self-report 14/14 — exactly the outage coverage exists to expose. A layer
# absent from this map records NULL coverage rather than falling back to
# that self-reported length. Single-key and scraper layers are deliberately
# absent: 1/1 is noise, not information.
LAYER_KEY_CATALOGS: dict[str, dict] = {
    "prices": COMMODITY_TICKERS,
    "currencies": CURRENCY_TICKERS,
    "fred": FRED_SERIES,
    "weather": GROWING_REGIONS,
    "cot": COT_COMMODITIES,
    "psd": PSD_TARGET_COMMODITIES,
    "dce": DCE_CONTRACTS,
    "export_sales": EXPORT_SALES_COMMODITIES,
    "forward_curve": FORWARD_CURVE_CONTRACTS,
    "eia": EIA_SERIES,
}


def layer_expected_keys(layer: str) -> int | None:
    """How many keys `layer` should return, or None if coverage is undefined.

    None means "not a coverage-bearing layer" and is recorded as NULL —
    distinct from 0, which would read as "expected nothing".
    """
    catalog = LAYER_KEY_CATALOGS.get(layer)
    return len(catalog) if catalog is not None else None


# analysis/health.py runs the same cadence question one level down: per
# commodity/region row inside a stored table rather than per layer. Tables
# whose source publishes slower than daily map to their layer above so the
# two checks can never disagree — weekly COT and monthly World Bank rows
# would otherwise be flagged STALE on every single run. Tables absent from
# this map are daily and keep the tighter health default.
HEALTH_TABLE_LAYERS = {
    "cot": "cot",
    "worldbank_prices": "worldbank",
}

# Which pipeline layer(s) write each table analysis/health.py inspects.
# The dashboard masthead counts layers, health counts commodities inside
# tables; without this map a layer that wrote *something* reads "fresh"
# while DATA HEALTH lists its commodities as MISSING. A health critical
# demotes every layer that writes the offending table (issue #58).
# brazil_spot_prices has three writers (Layers 15b/17/19) — a critical
# there can't be pinned to one, so all three lose the fresh badge.
HEALTH_TABLE_WRITER_LAYERS = {
    "prices": ("prices",),
    "cot": ("cot",),
    "weather": ("weather",),
    "currencies": ("currencies",),
    "dce_futures": ("dce",),
    "forward_curve": ("forward_curve",),
    "worldbank_prices": ("worldbank",),
    "india_domestic_prices": ("india_domestic",),
    "brazil_spot_prices": ("cepea", "agrural", "conab_precos"),
    "safex_prices": ("safex",),
}

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "data", "storage")
DB_PATH = os.path.join(STORAGE_DIR, "mirror_market.db")

# Git-committed CSV snapshots of snapshot-only tables (AgRural, SAFEX,
# forward curve, ...). CI runs on an ephemeral DB; these files are the
# persistence layer — imported at pipeline start, exported at pipeline end,
# committed back to the repo by the workflow. See pipeline/history.py.
HISTORY_DIR = os.path.join(os.path.dirname(__file__), "data", "history")

# ---------------------------------------------------------------------------
# Cloud Database (Turso — hosted SQLite)
# Set these env vars to use Turso instead of local SQLite.
# Sign up: https://turso.tech (free tier: 9GB, 500 databases)
# ---------------------------------------------------------------------------
TURSO_DATABASE_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_AUTH_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")

# ---------------------------------------------------------------------------
# MARKETS — the site's market registry (M8 #150, built by M17 #213)
#
# Market identity was previously scattered across COMMODITY_TICKERS,
# GROWING_REGIONS, CURRENCY_TICKERS, DCE_CONTRACTS, MANDI_STATES,
# MAGYP_FOB_POSITIONS, SAFEX_COMMODITIES and NOTICIAS_AGRICOLAS_URLS, and
# nothing in the repo named "Dalian" or "Argentina" as a *market* at all.
# This dict is the union of those: one place to read what a market IS.
#
# THREE RULES, all load-bearing:
#
# 1. POINTERS, NEVER VALUES. Every entry names a table, a column and a key —
#    never a price, a tier or a date. M1 #143 constraint 3 computes the tier
#    from what the DB holds on the day of generation; a hard-coded answer
#    here would freeze today's outage into the site forever.
# 2. KEY ORDER IS NAV ORDER IS LEDGER ORDER. Declared once (M8), consumed by
#    the masthead nav and by M2's eight-row headline ledger, so the two can
#    never disagree. The order is role in the trade: the board everything is
#    priced off, the buyer, the two exporters, then the domestic markets.
# 3. THE MARKET IS A PARAMETER, NEVER A CODE PATH. Anything a block builder
#    would otherwise branch on (`if market == "india"`) belongs in the
#    descriptor. See app/markets.py for the typed view and the tier rule.
#
# `quote_kind` is M3 #145 constraint 4 / M7 #149 finding 5: this stack spans
# traded board prices, physical export assessments, administered official
# minimums and weekly assessments. Collapsing those into one "price" label is
# the easiest way to make the product dishonest, so the kind rides on the
# descriptor and is rendered on the block.
#
# `cadence` gates the propagation ledger: it is daily-only (M10 #151), so a
# weekly leg renders in a cadence-stamped context band instead of a strip row
# that would read as an outage.
#
# `arbitrage` is required on every basis descriptor (M19 #222). A basis is a
# number a trader can work only where trade actually connects the two legs;
# India's mandi bean prints +66% over CBOT because GM imports are banned
# behind a tariff wall, and there is no cargo that closes it. Rendering that
# beside Paranaguá FOB with the same treatment invites a trade that cannot be
# taken. `policy_blocked` therefore *requires* a `caveat` — enforced at load in
# app/markets.py, so the spread cannot ship unlabelled.
# ---------------------------------------------------------------------------
MARKETS: dict[str, dict[str, Any]] = {
    "cbot": {
        "name": "CBOT",
        "venue": "CME Group / CBOT (Chicago)",
        "home_currency": "USD",
        "currency_pair": None,          # the numeraire — no conversion
        "price": {
            "layer": "prices",
            "table": "prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybeans", "Soybean Oil", "Soybean Meal"],
            "headline_key": "Soybeans",
            "cadence": "daily",
            "quote_kind": "board",
            "value_column": "Close",
            "unit": "native_exchange",
        },
        "crush": {
            "kind": "board",
            "yield_set": "soy_board",
            "table": "prices",
            "date_column": "Date",
            "key_column": "commodity",
            "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
            "value_column": "Close",
            "unit": "native_exchange",
        },
        # CIF NOLA barge bids over the named CBOT contract (Layer 20).
        "basis": {
            "layer": "gulf_bids",
            "table": "gulf_bids",
            "date_column": "report_date",
            "key_column": "commodity",
            "keys": ["Soybeans"],
            "reference": "cbot",
            "label": "US Gulf CIF over CBOT",
            # A cash bid for physical barge freight-on-board, not a board
            # settlement. Required because this descriptor is also the
            # `us_gulf:cif` ledger leg, and M3 #145 constraint 4 does not let an
            # unlabelled quote onto a surface beside a board price.
            "quote_kind": "physical",
            # AMS 3147 prints a flat CIF price in $/bu (the basis column is
            # cents/bu over the named contract); several barge locations and
            # delivery windows print on one report date.
            "value_column": "average",
            "unit": "usd_per_bushel",
            "arbitrage": "open",
        },
        "weather_regions": ["US Midwest (Iowa)", "US Illinois"],
        "psd_country": "United States",
        "players_country": "US",
    },
    "dalian": {
        "name": "Dalian",
        "venue": "Dalian Commodity Exchange (DCE)",
        "home_currency": "CNY",
        "currency_pair": "CNY/USD",
        # No.2 (B0) is the imported/GMO crush bean. No.1 (A0) is the domestic
        # non-GMO food bean and is NOT this market's benchmark — #152 shipped
        # the wrong one into both the crush and the vs-CBOT premium.
        "price": {
            "layer": "dce",
            "table": "dce_futures",
            "date_column": "Date",
            "key_column": "commodity",
            # No.1 last (M13a #249): the crush triplet stays contiguous and the
            # standalone food bean wraps onto its own grid row. It gets no
            # ledger row and no A0−B0 spread — a China food-demand level only.
            "keys": [
                "DCE Soybean No.2",
                "DCE Soybean Oil",
                "DCE Soybean Meal",
                "DCE Soybean No.1",
            ],
            "headline_key": "DCE Soybean No.2",
            "key_labels": {
                "DCE Soybean No.2": "imported/GMO crush bean",
                "DCE Soybean No.1": "domestic non-GMO food bean",
            },
            "cadence": "daily",
            "quote_kind": "board",
            "value_column": "Close",
            "unit": "home_per_mt",
        },
        "crush": {
            "kind": "board",
            "yield_set": "soy_board",
            "table": "dce_futures",
            "date_column": "Date",
            "key_column": "commodity",
            "legs": {
                "bean": "DCE Soybean No.2",
                "oil": "DCE Soybean Oil",
                "meal": "DCE Soybean Meal",
            },
            "value_column": "Close",
            "unit": "home_per_mt",
        },
        # Import parity vs CBOT, computed from the two price legs above.
        "basis": {
            "layer": "dce",
            "table": "dce_futures",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["DCE Soybean No.2"],
            "reference": "cbot",
            "label": "DCE No.2 over CBOT (import parity)",
            # Board against board — the one basis on the map whose two legs are
            # the same animal, and it is stated for exactly that reason.
            "quote_kind": "board",
            "value_column": "Close",
            "unit": "home_per_mt",
            # China imports the bean this contract deliveries against, so the
            # spread is worked by real cargoes — bounded by freight, tariff
            # (3%) and VAT (9%), not closed to zero.
            "arbitrage": "open",
        },
        "weather_regions": ["China Heilongjiang", "China Jilin"],
        "psd_country": "China",
        "players_country": "CN",
    },
    "brazil": {
        "name": "Brazil",
        "venue": "CEPEA/ESALQ Paraná · Paranaguá FOB",
        "home_currency": "BRL",
        "currency_pair": "BRL/USD",
        "price": {
            "layer": "cepea",
            "table": "brazil_spot_prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybean (CEPEA)", "Soybean (ESALQ/B3 Paranaguá)"],
            "headline_key": "Soybean (CEPEA)",
            "cadence": "daily",
            "quote_kind": "physical",
            "value_column": "price_brl",
            "unit": "home_per_mt",
        },
        # M7 #149: computable only with one named Paranaguá oil/meal scrape
        # that does not exist yet. Absent, not broken — the block says so.
        "crush": None,
        "crush_absent_reason": (
            "no Brazil oil/meal cash quote is ingested — the Paranaguá premium "
            "trio is an unbuilt scrape (M7 #149)"
        ),
        "basis": {
            "layer": "agrural",
            "table": "brazil_spot_prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybean (AgRural Paranaguá FOB)"],
            "reference": "cbot",
            "label": "Paranaguá FOB over CBOT",
            "quote_kind": "physical",
            "value_column": "price_brl",
            "unit": "home_per_mt",
            "arbitrage": "open",
        },
        "weather_regions": ["Brazil Mato Grosso", "Brazil Parana"],
        "psd_country": "Brazil",
        "players_country": "BR",
    },
    "argentina": {
        "name": "Argentina",
        "venue": "MAGyP official FOB (up-river)",
        "home_currency": "ARS",
        "currency_pair": "ARS/USD",
        # Ley 21.453 official minimum export values: natively USD/MT and
        # administered, not traded. Never label this a market price.
        "price": {
            "layer": "magyp_fob",
            "table": "argentina_fob",
            "date_column": "date",
            "key_column": "product",
            # Sunflower oil is an Argentine leg like the other three, not a
            # complex page of its own (#151) — it renders here, and feeds the
            # headline veg-oil board as the fourth oil.
            "keys": ["Soybeans", "Soybean Oil", "Soybean Meal", "Sunflower Oil"],
            "headline_key": "Soybeans",
            "cadence": "daily",
            "quote_kind": "administered",
            # Natively USD/MT, and several shipment windows print per day.
            "value_column": "price_usd_mt",
            "unit": "usd_per_mt",
        },
        "crush": {
            "kind": "administered",
            "yield_set": "soy_board",
            "table": "argentina_fob",
            "date_column": "date",
            "key_column": "product",
            "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
            "value_column": "price_usd_mt",
            "unit": "usd_per_mt",
            # No longer provisional (#162). The meal position 23040010100B was
            # the inferred leg M7 #149 flagged; it is now cross-checked against
            # dataset 358's labelled "Harina de soja, Pellets, de harina de
            # extracción" — 52 of 52 business days matched exactly. All three
            # crush legs are verified, so the margin is no longer caveated.
        },
        "basis": {
            "layer": "magyp_fob",
            "table": "argentina_fob",
            "date_column": "date",
            "key_column": "product",
            "keys": ["Soybeans"],
            "reference": "cbot",
            "label": "Argentina official FOB over CBOT",
            # The sharpest case on the map for naming the kind: a Ley 21.453
            # minimum export value is set by decree. Beside the CBOT board on
            # block 04 an unlabelled one reads as a traded Argentine price,
            # which does not exist.
            "quote_kind": "administered",
            "value_column": "price_usd_mt",
            "unit": "usd_per_mt",
            # Administered minimum, but on physical cargoes that do move —
            # export duty and the FX regime widen it, they don't block it.
            "arbitrage": "open",
        },
        "weather_regions": ["Argentina Pampas", "Argentina Cordoba"],
        "psd_country": "Argentina",
        "players_country": "AR",
    },
    "india": {
        "name": "India",
        "venue": "Agmarknet mandi spot (MP · MH)",
        "home_currency": "INR",
        "currency_pair": "INR/USD",
        # Bean-only. NCDEX derivatives are suspended to >=2027-03-31, so there
        # is no Indian futures curve and no India crush margin.
        "price": {
            "layer": "india_domestic",
            "table": "india_domestic_prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybean (Mandi MP)", "Soybean (Mandi MH)"],
            "headline_key": "Soybean (Mandi MP)",
            "cadence": "daily",
            "quote_kind": "physical",
            "value_column": "Close",
            "unit": "home_per_mt",
        },
        "crush": None,
        "crush_absent_reason": (
            "mandi is bean-only — no Indian oil or meal quote exists at daily "
            "cadence (NCDEX suspended to >=2027-03-31)"
        ),
        # Unblocked by #206: the +66% is real, cross-checked against SOPA's own
        # Indore oil and meal quotes (not Agmarknet-derived). Struck on the MP
        # median alone — the price block's own headline key, and the Indore hub
        # is the benchmark. MH is carried as the second price leg, not as a
        # second basis: two lines 0.2% apart would imply a choice that isn't one.
        "basis": {
            "layer": "india_domestic",
            "table": "india_domestic_prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybean (Mandi MP)"],
            "headline_key": "Soybean (Mandi MP)",
            "reference": "cbot",
            "label": "Mandi MP over CBOT (policy spread)",
            # An Agmarknet cash median, like the price leg it is struck on.
            "quote_kind": "physical",
            # Same stored column and unit as the price leg it is struck on —
            # INR/MT through the INR/USD rate, converted only by to_usd_mt.
            "value_column": "Close",
            "unit": "home_per_mt",
            # The whole reason this line needed a decision (#222): no trade
            # route connects these two legs, so the spread is not a basis a
            # trader can work — see ARBITRAGE_KINDS.
            "arbitrage": "policy_blocked",
            "caveat": (
                "India bans GM soybean imports behind a tariff wall, so no "
                "arbitrage connects this bean to CBOT — this is a policy "
                "spread, not a freight-and-quality one. It reached ~2x in 2021 "
                "and a wide print is its normal state, not a data error."
            ),
        },
        "weather_regions": ["India Madhya Pradesh", "India Maharashtra"],
        "psd_country": "India",
        "players_country": "IN",
    },
    "europe": {
        "name": "Europe",
        "venue": "EC Oilseeds Observatory (EU Moselle rapeseed)",
        "home_currency": "EUR",
        # EUR is absent from CURRENCY_TICKERS; the EC workbook publishes USD
        # itself (and derives its own EUR column from it), so the page quotes
        # the authoritative USD leg rather than a rate we do not hold.
        "currency_pair": None,
        # Weekly physical assessment, not the MATIF futures curve — those
        # settlements are licensed at EUR 167.55/month and are not shown.
        "price": {
            "layer": "ec_oilseeds",
            "table": "ec_oilseed_prices",
            "date_column": "Date",
            "key_column": "series",
            # Stored under the display label from EC_OILSEEDS_SERIES, not the
            # workbook's own column header.
            "keys": ["EU Rapeseed (Moselle)"],
            "headline_key": "EU Rapeseed (Moselle)",
            "cadence": "weekly",
            "quote_kind": "weekly_assessment",
            # USD is the authoritative column; the workbook derives its own EUR
            # from it at a sometimes-stale ECB rate (Layer 22).
            "value_column": "price_usd",
            "unit": "usd_per_mt",
        },
        "crush": None,
        "crush_absent_reason": "no EU rapeseed oil or meal assessment is ingested",
        "basis": None,
        "basis_absent_reason": "no EU futures board to take a basis against (MATIF is licensed)",
        "weather_regions": [],
        "weather_absent_reason": "no European growing region is in GROWING_REGIONS",
        "psd_country": "European Union",
        "players_country": None,
    },
    "south_africa": {
        "name": "South Africa",
        "venue": "JSE/SAFEX (Grain SA) · SAGIS deliveries",
        "home_currency": "ZAR",
        "currency_pair": "ZAR/USD",
        # LAST TRADED, not settlement: the free Grain SA table has no
        # settlement column and JSE MTM is licensed (#157). The contract shown
        # is the most-liquid one that session, and it must be named on the page.
        "price": {
            "layer": "safex",
            "table": "safex_prices",
            "date_column": "Date",
            "key_column": "commodity",
            "keys": ["Soybean (SAFEX)", "Sunflower (SAFEX)"],
            "headline_key": "Soybean (SAFEX)",
            "cadence": "daily",
            "quote_kind": "board_last_traded",
            "value_column": "Close",
            "unit": "home_per_mt",
        },
        # M7 #149 finding 4: SAFEX is seed-only and the JSE meal/oil products
        # are cash-settled CBOT in rand, so a "margin" off them is an FX
        # translation, not local crush economics. Established dead end.
        "crush": None,
        "crush_absent_reason": (
            "SAFEX is seed-only; JSE meal/oil are cash-settled CBOT in rand, so "
            "a margin off them is an FX translation, not crush economics (M7 #149)"
        ),
        "basis": None,
        "basis_absent_reason": "no South African export assessment against a board is ingested",
        # SA's physical-flow leg — the reason this is a flow page, not a price
        # page (#157 -> #202). SAGIS_ATTRIBUTION must render wherever it shows.
        "flows": {
            "layer": "sagis",
            "table": "sagis_deliveries",
            "date_column": "week_end",
            "key_column": "commodity",
            "keys": ["Soybeans", "Sunflower Seed"],
            "cadence": "weekly",
            # Tonnage, not a price — no currency conversion applies.
            "value_column": "week_total",
            "unit": "tonnes",
        },
        "weather_regions": ["South Africa Free State", "South Africa Mpumalanga"],
        "psd_country": "South Africa",
        "players_country": "ZA",
    },
    "nigeria": {
        "name": "Nigeria",
        "venue": "no domestic price venue ingested",
        "home_currency": "NGN",
        "currency_pair": "NGN/USD",
        # The only market on the map with no price leg of any kind. AFEX is
        # in flight (PR #166 / X4 #134) and pending a licence answer.
        "price": None,
        "price_absent_reason": (
            "no Nigerian soybean price source is ingested — AFEX is in flight "
            "and pending a licence answer (X4 #134)"
        ),
        "crush": None,
        "crush_absent_reason": "no Nigerian price leg to build a margin from",
        "basis": None,
        "basis_absent_reason": "no Nigerian price leg to take a basis from",
        "weather_regions": ["Nigeria Benue", "Nigeria Kaduna"],
        "psd_country": "Nigeria",
        "players_country": "NG",
    },
}


# ---------------------------------------------------------------------------
# LEDGER_LEGS — the propagation ledger's leg catalog (M12 #161, built by #223)
#
# A ledger row names a LEG, not a market. That is M12's first decision and the
# reason this catalog exists at all: `us_gulf:cif` is the AMS CIF NOLA barge
# bid, which lives on the CBOT market's `basis` descriptor and has no market
# key of its own. Forcing Brazil to compare against a Chicago board rather
# than against a competing physical answers the wrong question.
#
# So there are now TWO id spaces in this registry, and they must not be
# confused: `MARKETS` is keyed by market slug, this is keyed by leg id. Every
# leg id resolves to (market slug, descriptor sub-block, key within it) and
# `app/markets.py` hard-fails at load when it does not — the same treatment
# `quote_kind`, `arbitrage` and the weather regions already get, because a typo
# that ships an empty row instead of failing the build is exactly the defect
# nobody notices in review.
#
# Everything else about a leg — its table, date column, unit, FX pair, home
# currency — is READ FROM THE OWNING MARKET'S DESCRIPTOR, never restated here.
# A leg entry carries only what the owner cannot say: which of its keys this
# leg is, what to call it (the owner's name is not the leg's name), and how
# the print is proved.
#
# `trade_proof_column` is M4/#157's finding turned into a rule. SAFEX re-dates
# a carry-forward row with Volume 0 and High/Low 0.00, so the date beside the
# price is a ROW stamp, not evidence that anything traded — and a ledger whose
# whole job is "who has repriced" cannot read a row stamp as a reprice. Where a
# venue publishes a field that proves a trade, it is named here and a row that
# fails it is not a print. Assessments (CEPEA, AgRural, AMS, MAGyP, mandi) have
# no such field by nature: an assessment is not a trade, and demanding volume
# of one is a category error — their `quote_kind` already says which animal
# they are.
#
# `expected_gap_days` is M4 section 3.4 trap 5. FRESHNESS_WARNING_DAYS = 7 lets
# a daily leg go six days stale without a word, so the ledger cannot derive
# "overdue" from the freshness window; each leg states the largest gap between
# prints that is NORMAL for it. Four calendar days clears a Friday-to-Monday
# weekend plus one public holiday, which is the shape of every leg below.
LEDGER_DEFAULT_EXPECTED_GAP_DAYS = 4

LEDGER_LEGS = {
    "cbot:board": {
        "market": "cbot",
        "block": "price",
        "key": "Soybeans",
        "label": "CBOT board (ZS front)",
        # yfinance carries the session's volume; a settlement printed against
        # zero volume is not a session this board traded.
        "trade_proof_column": "Volume",
    },
    "us_gulf:cif": {
        # The one leg with no market of its own — it is `cbot.basis`, the AMS
        # 3147 flat CIF NOLA barge bid in $/bu. Its page link therefore lands
        # on the CBOT page, which is where that number is already explained.
        "market": "cbot",
        "block": "basis",
        "key": "Soybeans",
        "label": "US Gulf CIF (NOLA barge)",
    },
    "brazil:cepea": {
        "market": "brazil",
        "block": "price",
        "key": "Soybean (CEPEA)",
        "label": "Brazil CEPEA/ESALQ Paraná",
    },
    "brazil:paranagua": {
        "market": "brazil",
        "block": "basis",
        "key": "Soybean (AgRural Paranaguá FOB)",
        "label": "Brazil Paranaguá FOB",
    },
    "argentina:fob": {
        "market": "argentina",
        "block": "price",
        "key": "Soybeans",
        "label": "Argentina official FOB",
    },
    "dalian:board": {
        "market": "dalian",
        "block": "price",
        "key": "DCE Soybean No.2",
        "label": "Dalian No.2 (crush bean)",
        "trade_proof_column": "Volume",
    },
    "india:mandi_mp": {
        "market": "india",
        "block": "price",
        "key": "Soybean (Mandi MP)",
        "label": "India mandi — Madhya Pradesh",
    },
    "india:mandi_mh": {
        "market": "india",
        "block": "price",
        "key": "Soybean (Mandi MH)",
        "label": "India mandi — Maharashtra",
    },
    "south_africa:safex": {
        "market": "south_africa",
        "block": "price",
        "key": "Soybean (SAFEX)",
        "label": "SAFEX soybean (last traded)",
        # #157: the whole reason this field is in the contract. Grain SA
        # re-stamps a non-trading day's carried price with the current date.
        "trade_proof_column": "Volume",
    },
}


# ---------------------------------------------------------------------------
# LEDGERS — which legs sit on which page (M12 #161)
#
# Two selection rules, not one, and that is the load-bearing decision. Origin
# pages ask "who else is offering this cargo" — a genuine, closable arbitrage
# between competing FOBs. Destination pages ask "what does landed supply cost
# from the origins this market actually buys from" — the SPECIFIC origins, not
# all of them. One rule fits neither, so each ledger declares its own and the
# block renders it.
#
# Four constraints this data has to keep saying out loud:
#
# 1. ONE COMMODITY PER LEDGER. Every ledger below is the soybean. M3 #145's
#    "kinds do not mix" has a twin in *goods* do not mix: in one USD/MT column
#    a per-row label is not strong enough to stop the eye reading five numbers
#    as one price in five places. The straw man's DCE-meal-against-bean row is
#    out for exactly that reason; a meal ledger would be a second block.
# 2. FOUR OR FIVE ROWS, NEVER PADDED. Dalian and Argentina have three
#    counterparts genuinely connected by trade and a fourth candidate that is
#    filler (Argentine beans to China; a second Argentine leg). A layout
#    constant is not a reason to render an economic relationship, so M3's
#    "5 rows" becomes "the legs that exist".
# 3. CBOT IS NOT PINNED EVERYWHERE. M4 found it is our LEAST reliable same-day
#    leg while Dalian is the freshest settled one, so reflexively putting it on
#    every page encodes the assumption the ledger was built to correct. It is
#    dropped from India — the GM import ban means no cargo closes that +66%,
#    so the row would invite a trade that cannot be taken — and demoted to a
#    labelled reference row, last, on South Africa.
# 4. FIXED, NEVER SEASONAL. A row set that changes under the reader has to be
#    re-learned each visit, and picking counterparts by harvest window is
#    lead-lag inference wearing a layout hat — explicitly out of scope on the
#    map that produced this.
#
# The first leg is the page's own, pinned; the rest are counterparts, and each
# counterpart carries a spread against the pinned leg. `reference_legs` names
# the rows that are a flat-price yardstick rather than a peer offer.
LEDGER_RULES = ("origin", "destination")

LEDGERS = {
    "cbot": {
        "rule": "origin",
        "legs": [
            "cbot:board",
            "us_gulf:cif",
            "brazil:paranagua",
            "argentina:fob",
            "dalian:board",
        ],
        "note": (
            "The board against its own physical — the Gulf basis, made visible as "
            "a row — then the two competing origins, and the buyer whose bid "
            "ultimately clears the cargo."
        ),
    },
    "dalian": {
        "rule": "destination",
        "legs": ["dalian:board", "brazil:paranagua", "us_gulf:cif", "cbot:board"],
        "note": (
            "China's bean comes from Brazil (dominant) and the US Gulf; CBOT is "
            "the flat price import parity is struck against. Argentina is "
            "excluded — it crushes its beans rather than exporting them, so an "
            "Argentine bean row here would imply a flow that barely exists."
        ),
    },
    "brazil": {
        "rule": "origin",
        "legs": [
            "brazil:cepea",
            "brazil:paranagua",
            "us_gulf:cif",
            "argentina:fob",
            "cbot:board",
        ],
        "note": (
            "Domestic against own port is the internal-freight and export-premium "
            "gap — the most actionable line on this page. Then the two competing "
            "offers, and the reference."
        ),
    },
    "argentina": {
        "rule": "origin",
        "legs": ["argentina:fob", "brazil:paranagua", "us_gulf:cif", "cbot:board"],
        "note": (
            "The competing FOBs plus the reference; nothing else is connected. "
            "The pinned leg is an administered legal minimum, not an offer — the "
            "kind stays on the row."
        ),
    },
    "india": {
        "rule": "destination",
        "legs": ["india:mandi_mp", "india:mandi_mh"],
        # Required by app/markets.py because this ledger names no foreign leg:
        # a destination ledger with no origin in it has to say why, or it reads
        # as a set someone forgot to finish.
        "note": (
            "No origin qualifies. India bans GM soybean imports behind a tariff "
            "wall, so no foreign bean is connected to this one by trade and a "
            "counterpart row would invite an arbitrage that cannot be worked "
            "(#206). What remains is two independent state medians — a real "
            "check that catches one state's mandis being shut. CBOT appears on "
            "this page only in the basis block, labelled the policy spread it is."
        ),
    },
    "south_africa": {
        "rule": "destination",
        "legs": [
            "south_africa:safex",
            "argentina:fob",
            "brazil:paranagua",
            "cbot:board",
        ],
        "reference_legs": ["cbot:board"],
        "note": (
            "SAFEX seed trades inside an import/export parity band anchored on "
            "South American FOBs delivered Durban, so those two are the peers. "
            "CBOT rides last as the flat price both FOBs are quoted against, not "
            "as a peer."
        ),
    },
    # Europe and Nigeria get NO ledger block — `absent` with a reason, which is
    # a LEGAL page configuration and not a degraded one (M1 #143 / M10 #151).
    # Five rows of markets a country has no trade relationship with is worse
    # than nothing: it implies one.
    "europe": None,
    "nigeria": None,
}

LEDGER_ABSENT_REASONS = {
    "europe": (
        "the ledger is daily-only (M10 #151) and Europe's only leg is the EC's "
        "weekly Moselle rapeseed assessment — wrong cadence and wrong good. The "
        "straw man's ICE canola counterpart is not available either: that feed "
        "was verified dead on 2026-08-08"
    ),
    "nigeria": (
        "no Nigerian price leg of any kind is ingested, and rows of markets "
        "Nigeria has no trade relationship with would imply one (M12 #161)"
    ),
}


# ---------------------------------------------------------------------------
# ORIGIN COMPARISON — landed-cost economics (Phase 2)
#
# The trader question this registry serves: "for a named shipment window, which
# origin — United States, Brazil or Argentina — is economically preferable for
# delivery to a named destination?"
#
# Same three rules as MARKETS above, and one more that only applies here:
#
# 4. AN ORIGIN LEG NAMES ITS INCOTERM AND ITS CARRIER. Two export offers are
#    comparable numbers only when they are the same delivery term at the same
#    kind of loading point. AMS report 3147 quotes CIF onto a *barge* in the
#    New Orleans area; AgRural and MAGyP quote FOB into a *vessel*. Treating
#    those as one number understates the US origin by the elevation spread —
#    always in the same direction, and invisibly. So the term rides on the leg
#    and `analysis/origins/landed_cost.py` bridges it explicitly, at a cost
#    somebody entered and signed for.
#
# Everything else about a leg — table, date column, value column, unit,
# quote_kind, FX pair — is READ FROM THE OWNING MARKET'S DESCRIPTOR, exactly as
# LEDGER_LEGS does. A leg entry carries only what MARKETS cannot say.
# ---------------------------------------------------------------------------

# Hand-entered cost inputs (ocean freight, elevation, port charges, processing).
# See analysis/origins/assumptions.py — an entered number with an owner and an
# expiry beats a fabricated one with neither.
#
# MIRROR_ASSUMPTIONS_DIR overrides the location. It exists for the dev loop and
# for tests — rendering the page against a populated fixture set is the only way
# to look at the success path on a clone whose real assumptions are (correctly)
# empty. It is deliberately NOT set anywhere in CI: production reads the
# committed directory, so a fixture freight number cannot reach a published page.
ASSUMPTIONS_DIR = os.getenv("MIRROR_ASSUMPTIONS_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "assumptions"
)

# Bumped whenever the arithmetic or the component order changes. Stored on every
# ranking, so a historical row can be read against the method that produced it
# rather than against today's.
LANDED_COST_METHOD_VERSION = "1.0.0"

# How many days apart the origin quotes in one ranking may be observed before
# the comparison is refused. Three clears a weekend: Argentina's circular lands
# same-day, AgRural is same-day, AMS is same-day, but any one of them can miss
# a session. Beyond that the spread between two origins is measuring the
# calendar, and the ranking says so instead of publishing it.
ORIGIN_MAX_OBSERVATION_SPREAD_DAYS = 3

# Pricing locations. `key` is the id used by every assumption's `origin` and
# `destination` field, so a typo in a freight entry fails to match rather than
# matching the wrong route.
ORIGIN_PORTS: dict[str, dict[str, Any]] = {
    "us_gulf": {
        "name": "US Gulf (NOLA / Mississippi)",
        "country": "United States",
        "country_iso": "US",
    },
    "us_pnw": {
        "name": "US Pacific Northwest (Columbia River)",
        "country": "United States",
        "country_iso": "US",
    },
    "br_paranagua": {
        "name": "Paranaguá",
        "country": "Brazil",
        "country_iso": "BR",
    },
    "ar_up_river": {
        "name": "Up-river (Rosario / San Lorenzo)",
        "country": "Argentina",
        "country_iso": "AR",
    },
}

DESTINATION_PORTS: dict[str, dict[str, Any]] = {
    "cn_north": {
        "name": "North China (Qingdao / Rizhao / Dalian range)",
        "country": "China",
        "country_iso": "CN",
        "market": "dalian",          # the destination's own market page
        "players_country": "CN",
        "note": (
            "One discharge range, not one berth: Chinese crush capacity is "
            "concentrated on the northern coast and freight is quoted to the "
            "range rather than to a named terminal."
        ),
    },
}

# Which origin legs exist, and which are declared-but-unavailable. PNW is
# listed with no source on purpose: it is a real origin a trader compares
# against, this stack ingests no PNW price, and the honest rendering of that is
# a named unavailable row rather than a silently three-origin board.
ORIGIN_LEGS: dict[str, dict[str, Any]] = {
    "us_gulf": {
        "port": "us_gulf",
        "label": "US Gulf CIF (NOLA barge)",
        # AMS 3147 is a cash bid for barge-delivered beans at the NOLA area —
        # one elevation short of being on a vessel. See rule 4 above.
        "incoterm": "CIF",
        "carrier": "barge",
        "market": "cbot",
        "block": "basis",
        "key": "Soybeans",
        # AMS quotes several barge locations and delivery slots on one report
        # date; the slot IS the shipment window and must not be averaged across.
        "window_column": "delivery",
        "window_scheme": "ams_delivery",
        "contract_column": "futures_month",
        "grade": "US No. 2 Yellow Soybeans",
        "hedge_exchange": "cbot",
        "hedge_code": "ZS",
    },
    "us_pnw": {
        "port": "us_pnw",
        "label": "US PNW FOB (Columbia River)",
        "incoterm": "FOB",
        "carrier": "vessel",
        "absent_reason": (
            "no PNW price series is ingested — AMS report 3147 covers Louisiana "
            "and Texas only, and the PNW export bid tables that exist are behind "
            "paid feeds. The row is declared rather than dropped: a three-origin "
            "board that silently omits the PNW reads as a complete comparison"
        ),
        "grade": "US No. 2 Yellow Soybeans",
    },
    "br_paranagua": {
        "port": "br_paranagua",
        "label": "Brazil Paranaguá FOB",
        "incoterm": "FOB",
        "carrier": "vessel",
        "market": "brazil",
        "block": "basis",
        "key": "Soybean (AgRural Paranaguá FOB)",
        # AgRural publishes a port-side FOB level with no shipment period
        # attached. That absence is data: the row is priced, but it cannot be
        # said to be priced FOR October, so it is never ranked against one.
        "window_scheme": "none",
        "grade": "Brazilian soybeans, contract standard",
        "hedge_exchange": "cbot",
        "hedge_code": "ZS",
    },
    "ar_up_river": {
        "port": "ar_up_river",
        "label": "Argentina official FOB (up-river)",
        "incoterm": "FOB",
        "carrier": "vessel",
        "market": "argentina",
        "block": "price",
        "key": "Soybeans",
        # MAGyP publishes a genuine shipment-window curve: each circular row
        # carries ship_from/ship_to as YYYY-MM bounds. This is the only origin
        # here that can answer "which window" from its own data.
        "window_columns": ("ship_from", "ship_to"),
        "window_scheme": "magyp_months",
        "grade": "Argentine soybeans, Ley 21.453 reference quality",
        "hedge_exchange": "cbot",
        "hedge_code": "ZS",
    },
}

# What has to be paid to turn a leg's own delivery term into FOB-vessel at its
# own port — the common footing every landed cost is built from. An entry of ()
# means the term already IS FOB vessel and nothing is owed.
#
# Keyed by (incoterm, carrier) rather than by origin: the bridge is a property
# of the delivery term, so a second CIF-barge origin needs no new code and no
# new entry.
INCOTERM_BRIDGE_TO_FOB_VESSEL = {
    ("FOB", "vessel"): (),
    # Barge-delivered at NOLA -> loaded into an ocean vessel. The elevation
    # spread is real, non-trivial (trade press puts it in the tens of dollars
    # per tonne) and published nowhere free, so it is an entered assumption and
    # its absence blocks the US row rather than defaulting to zero.
    ("CIF", "barge"): ("elevation",),
    ("FCA", "truck"): ("inland_transport", "origin_port_costs"),
    ("FCA", "rail"): ("inland_transport", "origin_port_costs"),
    ("EXW", "gate"): ("inland_transport", "origin_port_costs"),
}

# The landed stack applied after FOB vessel, in order. Ad-valorem rungs are
# applied against the running total at their own position — duty on the CIF
# value, VAT on the duty-paid value — which is why this is a sequence and not
# a set. Components a route does not incur are entered as an explicit zero
# assumption, never omitted: "no marine insurance on this route" is a decision
# somebody made, and it should have their name on it.
LANDED_STACK = (
    "ocean_freight",
    "marine_insurance",
    "import_duty",
    "import_vat",
    "destination_port_costs",
    "financing",
    "quality_adjustment",
)

# Shipment windows the page offers. Generated relative to the run date rather
# than hard-coded, so the selector never offers a window that has sailed; see
# analysis/origins/comparison.py:offered_windows.
ORIGIN_WINDOW_MONTHS_AHEAD = 6

# Physical crush: what has to be entered before a net plant margin exists.
# Board crush needs nothing (it is three board prices); gross physical crush
# needs three physical legs; net plant margin needs the conversion cost of
# actually running the plant, which no free source publishes for any origin.
NET_PLANT_MARGIN_COMPONENTS = (
    "processing_cost",
    "energy_cost",
    "plant_freight_in",
    "working_capital",
)


# ---------------------------------------------------------------------------
# PHYSICAL_CRUSH — the cash-market legs of a crush margin, per market (Phase 2)
#
# A board crush is three futures settlements and tells a processor what the
# *paper* margin is. It is not what the plant earns. The plant buys a physical
# bean delivered to its gate and sells physical oil and meal ex-works, and the
# gap between those two margins is the whole reason a crusher has a trading
# desk.
#
# So this is a second, deliberately separate descriptor set. It is NOT derived
# from MARKETS[...]["crush"], because that entry answers a different question
# for CBOT (three board legs) than it does for Argentina (three administered
# FOB legs), and collapsing them would publish a board margin under a physical
# label on six pages out of eight.
#
# Every leg that does not exist says so, by name. "No Brazilian cash oil quote
# is ingested" is a fact a reader can act on; a crush block that quietly falls
# back to the board is not.
# ---------------------------------------------------------------------------
PHYSICAL_CRUSH: dict[str, dict[str, Any]] = {
    "argentina": {
        "label": "Argentina up-river, official FOB legs",
        # Ley 21.453 minimum export values for all three products on one
        # circular — the only market on this map where a complete physical
        # triplet is published daily and free. All three positions were
        # cross-checked numerically against the labelled datos.gob.ar mirror
        # (#162), so none of the legs is inferred.
        "kind": "administered",
        "table": "argentina_fob",
        "date_column": "date",
        "key_column": "product",
        "value_column": "price_usd_mt",
        "unit": "usd_per_mt",
        "layer": "magyp_fob",
        "quote_kind": "administered",
        "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
        # The circular quotes every product for several shipment bands at once,
        # so the crush has to be struck on a band all three share. Averaging a
        # product across its bands first produced a margin for a cargo nobody
        # can ship — 21.2 USD/MT against the 24.1 an actual prompt August cargo
        # earned on 2026-08-11, from the same rows on the same day.
        "window_columns": ("ship_from", "ship_to"),
        "yield_set": "soy_board",
        "note": (
            "An administered FOB triplet, not a plant's own buy and sell. It is a "
            "physical margin in the sense that all three legs are physical goods at "
            "one location on one day; it is not an offer anybody made."
        ),
    },
    "cbot": {
        "label": "US Gulf physical legs",
        "kind": "physical",
        "absent_reason": (
            "only the bean leg is ingested. AMS report 3147 gives a CIF NOLA barge "
            "soybean bid (Layer 20), but no free daily US cash soybean oil or meal "
            "assessment is ingested by this stack, so the oil and meal legs of a US "
            "physical crush do not exist here. The board crush above is a paper "
            "margin and is labelled as one."
        ),
        "missing_legs": ("oil", "meal"),
    },
    "brazil": {
        "label": "Paranaguá physical legs",
        "kind": "physical",
        "absent_reason": (
            "the Paranaguá premium trio is an unbuilt scrape (M7 #149) — a bean FOB "
            "level exists, the oil and meal cash quotes do not"
        ),
        "missing_legs": ("oil", "meal"),
    },
    "dalian": {
        "label": "China cash legs",
        "kind": "physical",
        "absent_reason": (
            "DCE settlements are a board, not a cash market. Chinese port-side cash "
            "bean, oil and meal assessments are commercial products and none is "
            "ingested here"
        ),
        "missing_legs": ("bean", "oil", "meal"),
    },
}
