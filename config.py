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
RETRY_DELAY = 2         # seconds between retries

# ---------------------------------------------------------------------------
# Layer 1 — yfinance ticker symbols (data sourced from CME / ICE / CBOT)
#
# Core commodities (soybeans complex + coffee) PLUS competing/rotation
# crops and downstream demand. Without corn, wheat, sugar, and livestock
# the analysis would be misleading — these directly drive soybean acreage
# decisions and feed demand.
# ---------------------------------------------------------------------------
COMMODITY_TICKERS = {
    # ── Soybean complex (core) ──
    "Soybeans":     "ZS=F",   # CME/CBOT — benchmark global soybean price
    "Soybean Oil":  "ZL=F",   # CME/CBOT — cooking oil + biodiesel
    "Soybean Meal": "ZM=F",   # CME/CBOT — animal feed protein
    "Coffee":       "KC=F",   # ICE — Arabica coffee

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
}

# How far back to pull historical data (yfinance period strings)
DEFAULT_HISTORY_PERIOD = "2y"

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
    "Coffee":       "COFFEE C - ICE FUTURES U.S.",

    # ── Competing crops ──
    "Corn":         "CORN - CHICAGO BOARD OF TRADE",
    "Wheat":        "WHEAT-SRW - CHICAGO BOARD OF TRADE",
    "Sugar":        "SUGAR NO. 11 - ICE FUTURES U.S.",
    "Cotton":       "COTTON NO. 2 - ICE FUTURES U.S.",

    # ── Livestock ──
    "Live Cattle":  "LIVE CATTLE - CHICAGO MERCANTILE EXCHANGE",
    "Lean Hogs":    "LEAN HOGS - CHICAGO MERCANTILE EXCHANGE",
}

# ---------------------------------------------------------------------------
# Layer 5 — Weather data via Open-Meteo (free, no API key)
#
# Every major growing region for soybeans, coffee, and palm oil worldwide.
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
    "Brazil Minas Gerais":        {"lat": -19.47, "lon": -46.05},   # Coffee capital of Brazil
    "Brazil Bahia":               {"lat": -12.97, "lon": -38.51},   # Cacao + coffee
    "Argentina Pampas":           {"lat": -33.95, "lon": -60.33},   # Soy belt
    "Argentina Cordoba":          {"lat": -31.42, "lon": -64.18},   # #2 Argentina soy province
    "Paraguay Chaco":             {"lat": -22.35, "lon": -59.95},   # Expanding soy frontier
    "Colombia Coffee Region":    {"lat": 4.81,   "lon": -75.68},

    # ── Africa ──
    "Ethiopia Sidama (Coffee)":   {"lat": 6.74,   "lon": 38.46},   # #1 Africa coffee, Arabica origin
    "Ivory Coast (Cocoa)":        {"lat": 6.83,   "lon": -5.29},   # Cross-reference

    # ── Asia ──
    "Vietnam Central Highlands":  {"lat": 14.35,  "lon": 108.00},  # #2 global Robusta
    "Indonesia Riau (Sumatra)":   {"lat": 0.29,   "lon": 101.71},  # #1 palm oil belt
    "Malaysia Sabah (Borneo)":    {"lat": 5.42,   "lon": 116.80},  # #2 palm oil state
    "India Madhya Pradesh":       {"lat": 22.72,  "lon": 75.86},   # India soybean capital
    "India Maharashtra":          {"lat": 19.75,  "lon": 75.71},   # #2 India soybean state
    "Thailand Surat Thani":       {"lat": 9.14,   "lon": 99.33},   # #3 global palm oil
    "China Heilongjiang":         {"lat": 47.36,  "lon": 127.76},  # China domestic soybean belt
}

WEATHER_DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"

# ---------------------------------------------------------------------------
# Layer 6 — USDA FAS PSD (global supply/demand, bulk CSV, no API key)
# Covers: soybeans, soybean oil, soybean meal, palm oil, coffee — every country
#
# Added corn and cotton for rotation crop tracking, and grains category
# for wheat coverage.
# ---------------------------------------------------------------------------
PSD_URLS = {
    "oilseeds": "https://apps.fas.usda.gov/psdonline/downloads/psd_oilseeds_csv.zip",
    "coffee":   "https://apps.fas.usda.gov/psdonline/downloads/psd_coffee_csv.zip",
    "grains":   "https://apps.fas.usda.gov/psdonline/downloads/psd_grains_pulses_csv.zip",
    "cotton":   "https://apps.fas.usda.gov/psdonline/downloads/psd_cotton_csv.zip",
}

PSD_TARGET_COMMODITIES = {
    # ── Core ──
    "Soybeans":     "2222000",
    "Soybean Oil":  "4232000",
    "Soybean Meal": "813100",
    "Palm Oil":     "4243000",
    "Coffee":       "711100",
    # ── Competing crops ──
    "Corn":         "440000",
    "Wheat":        "410000",
    "Cotton":       "2631000",
}

# Every country that materially affects our tracked commodities
PSD_TARGET_COUNTRIES = [
    # ── Americas ──
    "United States", "Brazil", "Argentina", "Paraguay",
    "Uruguay", "Bolivia", "Colombia", "Mexico",
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
    "COP/USD": "COPUSD=X",   # Colombian Peso — #3 Arabica coffee producer
    "PYG/USD": "PYGUSD=X",   # Paraguayan Guarani — #4 soybean exporter

    # ── Asia ──
    "CNY/USD": "CNYUSD=X",   # Chinese Yuan — #1 soybean importer
    "IDR/USD": "IDRUSD=X",   # Indonesian Rupiah — #1 palm oil producer
    "MYR/USD": "MYRUSD=X",   # Malaysian Ringgit — #2 palm oil producer
    "VND/USD": "VNDUSD=X",   # Vietnamese Dong — #2 Robusta coffee producer
    "INR/USD": "INRUSD=X",   # Indian Rupee — major soybean/palm oil consumer
    "THB/USD": "THBUSD=X",   # Thai Baht — #3 palm oil producer

    # ── Africa ──
    "ETB/USD": "ETBUSD=X",   # Ethiopian Birr — #1 Africa coffee producer
}

# ---------------------------------------------------------------------------
# Layer 8 — World Bank Pink Sheet (monthly Robusta, Palm Oil, etc.)
# ---------------------------------------------------------------------------
WORLDBANK_PRICES_URL = (
    "https://thedocs.worldbank.org/en/doc/"
    "18675f1d1639c7a34d463f59263ba0a2-0050012025/related/"
    "CMO-Historical-Data-Monthly.xlsx"
)

# ---------------------------------------------------------------------------
# Layer 9 — DCE (Dalian Commodity Exchange) futures via AKShare (no API key)
# China is the world's largest soybean importer; DCE is the main exchange
# ---------------------------------------------------------------------------
DCE_CONTRACTS = {
    "DCE Soybean":      "A0",   # Soybean No.1 continuous
    "DCE Soybean Meal": "M0",   # Soybean Meal continuous
    "DCE Soybean Oil":  "Y0",   # Soybean Oil continuous
    "DCE Palm Oil":     "P0",   # Palm Oil continuous
    "DCE Corn":         "C0",   # Corn continuous — China feed demand
}

# ---------------------------------------------------------------------------
# Layer 10 — USDA FAS Export Sales Reporting (ESR)
# Sign up: https://apps.fas.usda.gov/opendataweb/home
# Weekly export sales — the #1 indicator of Chinese buying pace
# ---------------------------------------------------------------------------
FAS_API_KEY = os.getenv("FAS_API_KEY", "")
FAS_BASE_URL = "https://apps.fas.usda.gov/opendataweb/api/esr"

# USDA FAS commodity codes for Export Sales Reporting
# Codes sourced from /api/esr/commodities endpoint
EXPORT_SALES_COMMODITIES = {
    "Soybeans":     "2222000",
    "Soybean Oil":  "4232000",
    "Soybean Meal": "0813100",
    "Corn":         "0440000",
    "Wheat":        "0410000",
    "Cotton":       "2631000",
}

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
    "Coffee":       {"root": "KC", "exchange": "NYB", "months": [3, 5, 7, 9, 12]},
    "Sugar":        {"root": "SB", "exchange": "NYB", "months": [3, 5, 7, 10]},
    "Cotton":       {"root": "CT", "exchange": "NYB", "months": [3, 5, 7, 10, 12]},
    "Live Cattle":  {"root": "LE", "exchange": "CME", "months": [2, 4, 6, 8, 10, 12]},
    "Lean Hogs":    {"root": "HE", "exchange": "CME", "months": [2, 4, 5, 6, 7, 8, 10, 12]},
}

# ---------------------------------------------------------------------------
# Analysis thresholds — configurable per-commodity where appropriate
# ---------------------------------------------------------------------------

# RSI levels (industry standard 70/30, but can be tuned)
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# Volume spike: multiple of 20-day average volume to flag as unusual
VOLUME_SPIKE_MULTIPLIER = 2.0

# Weather alert thresholds
WEATHER_HEAVY_RAIN_MM = 20      # mm precipitation to flag
WEATHER_EXTREME_HEAT_C = 38     # degrees C to flag as crop stress
WEATHER_DRY_THRESHOLD_MM = 1    # below this = "dry conditions"

# Data freshness: warn if a layer hasn't updated in this many days
FRESHNESS_WARNING_DAYS = 7

# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------
STORAGE_DIR = os.path.join(os.path.dirname(__file__), "data", "storage")
DB_PATH = os.path.join(STORAGE_DIR, "mirror_market.db")
