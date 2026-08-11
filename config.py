"""
Mirror Market configuration.

Ticker symbols, API keys, and shared settings live here so every
module can import them from one place.
"""

import logging
import os
from datetime import date as _date


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
# ---------------------------------------------------------------------------
MARKETS = {
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
        },
        "crush": {
            "kind": "board",
            "yield_set": "soy_board",
            "table": "prices",
            "date_column": "Date",
            "key_column": "commodity",
            "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
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
            "keys": ["DCE Soybean No.2", "DCE Soybean Oil", "DCE Soybean Meal"],
            "headline_key": "DCE Soybean No.2",
            "cadence": "daily",
            "quote_kind": "board",
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
            "keys": ["Soybeans", "Soybean Oil", "Soybean Meal"],
            "headline_key": "Soybeans",
            "cadence": "daily",
            "quote_kind": "administered",
        },
        "crush": {
            "kind": "administered",
            "yield_set": "soy_board",
            "table": "argentina_fob",
            "date_column": "date",
            "key_column": "product",
            "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
            # M7 #149 / M5 #147: the meal position 23040010100B is inferred,
            # not cross-checked against datos.gob.ar series 358, and 3 of 4
            # inferred meal codes were wrong last time this was checked. The
            # margin is provisional until that check lands.
            "provisional": True,
        },
        "basis": {
            "layer": "magyp_fob",
            "table": "argentina_fob",
            "date_column": "date",
            "key_column": "product",
            "keys": ["Soybeans"],
            "reference": "cbot",
            "label": "Argentina official FOB over CBOT",
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
        },
        "crush": None,
        "crush_absent_reason": (
            "mandi is bean-only — no Indian oil or meal quote exists at daily "
            "cadence (NCDEX suspended to >=2027-03-31)"
        ),
        # M1 #143 caught the mandi level printing +66% over CBOT, against a
        # source row carrying Low = INR 1,010/MT beside High = INR 71,500. The
        # basis line stays absent until #206 validates the level.
        "basis": None,
        "basis_absent_reason": (
            "mandi level fails validation against CBOT (+66%; unit mix in the "
            "source) — blocked on #206"
        ),
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
