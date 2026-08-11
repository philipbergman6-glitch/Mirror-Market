"""
Mirror Market configuration.

Ticker symbols, API keys, and shared settings live here so every
module can import them from one place.
"""

import logging
import os


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
LAYER_MIN_KEYS = {
    "prices": 8,       # of 10 tickers
    "currencies": 8,   # of 10 pairs
    "fred": 8,         # of 10 series
    "weather": 14,     # of 18 regions
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
# Position → product mapping cross-verified 2026-08-07 against the labelled
# datos.gob.ar series (sspm dataset 358, "precios-fob-oficiales"): values for
# 2025-01-20 matched exactly (beans granel 412, crude oil granel 1044, meal
# pellets 322). Bulk ("granel") positions are the benchmark legs; the bagged
# ("embolsado") sub-positions run ~$20 over and are not stored.
# ---------------------------------------------------------------------------
MAGYP_FOB_URL = (
    "https://www.magyp.gob.ar/sitio/areas/ss_mercados_agropecuarios"
    "/ws/ssma/precios_fob.php"
)
MAGYP_FOB_POSITIONS = {
    "12019000190C": "Soybeans",      # habas de soja, las demás — granel
    "15071000100Q": "Soybean Oil",   # aceite de soja en bruto — granel
    "23040010100B": "Soybean Meal",  # pellets de soja
}
# Walk back this many calendar days to find the latest published circular
# (weekends + Argentine holidays publish nothing).
MAGYP_FOB_LOOKBACK_DAYS = 7

# ---------------------------------------------------------------------------
# Layer 18 — SAFEX/JSE South Africa domestic soy prices (free, no API key)
# JSE agricultural settlement prices in ZAR/MT
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
    # Monthly publications — allow ~6 weeks.
    "wasde": 42,
    "psd": 42,
    "conab": 42,
    "worldbank": 42,
    "eia": 42,
    "usda": 400,  # annual NASS crop data
}

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
    "fred": 10,        # 1-day publication lag on the daily series
    "weather": 10,     # includes forecast rows, so age is normally negative
    "dce": 21,         # Spring Festival / Golden Week close the DCE for ~2 weeks
    # Weekly publications — observation lag on top of the weekly cadence,
    # plus room for one missed release.
    "cot": 18,         # Friday release reports the *previous Tuesday*
    "export_sales": 21,
    "eia": 21,
    # Monthly publication. 100 days matches the identical guard inside
    # fetchers/worldbank.py — the CMO deep link rotates yearly and the old
    # GUID keeps serving a frozen file with HTTP 200. One number, one
    # meaning, two enforcement points.
    "worldbank": 100,
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
