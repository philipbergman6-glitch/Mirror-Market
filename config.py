"""
Mirror Market configuration.

Ticker symbols, API keys, and shared settings live here so every
module can import them from one place.
"""

import logging
import os
from datetime import date as _date
from pathlib import Path
from typing import Any, TypedDict

from dotenv import load_dotenv


# ---------------------------------------------------------------------------
# API keys — load the local env file before anything reads os.getenv
# ---------------------------------------------------------------------------
# Every key below is an `os.getenv(...)` at import time, so the file has to be
# in the environment *before* this module finishes importing — a later
# load_dotenv() would be a no-op against constants already bound to "".
#
# `override=False` is the point: a variable already in the environment wins.
# CI sets its keys from GitHub secrets and has no env file at all, so this is
# inert there; locally it is the difference between a run that fetches and a
# run that skips every keyed layer while looking healthy. Before this, nothing
# in the repo loaded the file, so a populated .env still left FAS_API_KEY ==
# "" and Layers 2/3/10/13/16 quietly degraded (#237 follow-up).
def _load_env_file(path: Path) -> bool:
    """Put `path`'s variables in the environment. True if the file existed."""
    if not path.is_file():
        return False
    load_dotenv(path, override=False)
    return True


ENV_FILE = Path(__file__).resolve().parent / ".env"
ENV_FILE_LOADED = _load_env_file(ENV_FILE)


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
    "weather": 18,     # of 24 regions (M14 #207 remapped the set)
    "cot": 7,          # of 10 commodities
    "psd": 5,          # of 10 commodities
    "dce": 3,          # of 8 contracts (6 DCE + 2 CZCE rapeseed)
    "usda": 2,         # of 3 stats (production, area harvested, yield)
    "export_sales": 4,  # of 6 commodities
    "forward_curve": 7,  # of 9 commodities
    "eia": 2,          # of 3 series
    # Both GTR legs demand *every* key. Each layer has exactly two, they come
    # out of one workbook in one download, and there is no such thing as one
    # route publishing while the other does not — so a missing key is a parse
    # fault, never an off-day, and a floor of 1 would let half a layer go dark
    # while the run stayed green.
    "gtr_ocean_freight": 2,  # of 2 routes
    "gtr_vessels": 2,        # of 2 port regions
    # Both Mississippi gauges come out of the same NWPS API on the same run.
    # One gauge answering while the other does not is our transport or a
    # renamed LID, never "that stretch of river had nothing to report" — a
    # river always has a level. A floor of 1 would let the Memphis leg, which
    # is the one the basis trades off, go dark behind a green St. Louis.
    "river_us": 2,           # of 2 gauges
}

# Systemic-outage backstop: exit non-zero when more than this many active
# (non-disabled) layers fail in one run, even if the critical layers passed.
MAX_FAILED_LAYERS = 5
RETRY_DELAY = 2         # seconds between retries

# Authoritative operational inventory. The public masthead, About Data table,
# pipeline summary, and smoke contract all consume this catalog so their
# denominator cannot drift. Numbered groups 2 and 15 each have an independently
# runnable sub-layer, hence 31 operational layers across 28 numbered groups.
PRODUCTION_LAYERS = (
    ("prices", "1", "Yahoo Finance (CME/CBOT/ICE)", "Daily", "10 commodity futures"),
    ("usda", "2", "USDA NASS QuickStats", "Annual", "US production, area and yield"),
    ("crop_progress", "2b", "USDA NASS QuickStats", "Weekly/seasonal", "US crop progress and condition"),
    ("fred", "3", "Federal Reserve (FRED)", "Daily/Monthly", "Dollar, CPI, rates and yields"),
    ("cot", "4", "CFTC", "Weekly", "10 commodities positioning"),
    ("weather", "5", "Open-Meteo", "Daily + forecast", "19 growing regions"),
    ("psd", "6", "USDA FAS (PSD)", "Monthly", "10 commodities × 28 countries + World"),
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
    ("gtr_ocean_freight", "26", "USDA AMS (Grain Transport Report)", "Monthly", "Gulf and PNW to Japan ocean freight"),
    ("gtr_vessels", "26b", "USDA AMS (Grain Transport Report)", "Weekly", "Gulf and PNW grain vessel lineups"),
    ("river_us", "27", "NOAA NWPS (stage via USACE/USGS)", "Daily + forecast", "Mississippi at Memphis and St. Louis"),
    ("river_ar", "28", "Argentina INA (Prefectura reading)", "Daily", "Paraná at Rosario"),
)
PRODUCTION_LAYER_KEYS = tuple(layer[0] for layer in PRODUCTION_LAYERS)

# ---------------------------------------------------------------------------
# Fast refresh — the price-only path (`python main.py --fast`)
#
# The daily build re-downloads all 31 layers, and the measured cost of that is
# dominated by one thing: DEFAULT_HISTORY_PERIOD is 15 years, and a 15-year
# yfinance pull benchmarked at 24-32 s per ticker against 1-3 s for a short
# window (2026-08-19, see LATENCY.md). Twenty tickers of history is most of the
# 6m02s the 2026-08-18 production run spent inside its layers.
#
# The fast path exists because the numbers a trader reprices on intraday are a
# small, cheap subset of that: the board, its curve, and the FX every
# `home_per_mt` leg converts through. Everything else on the site — a weekly
# CFTC report, a monthly WASDE, a crop estimate — cannot have changed between
# two runs on the same day, so re-fetching it would buy nothing and cost the
# whole runtime.
#
# Three deliberate exclusions, each of which looks like it belongs here:
#   dce           — the DCE closes 15:00 CST, i.e. before any refresh we would
#                   schedule; a second fetch the same day re-reads one file.
#   the physical  — cepea/agrural/gulf_bids/magyp_fob publish once a day. They
#     origin legs   would be legitimate additions to a *second* daily build,
#                   but they are scrapers against unfriendly upstreams and
#                   running them more often trades their reliability for
#                   freshness they do not have (Layer 16's 2026-08-11 rate-limit
#                   blackout is the standing example).
#   india_domestic— shares that hazard and the shared-key throttle explicitly.
#
# So: what moves, and only what moves.
FAST_REFRESH_LAYERS = ("prices", "currencies", "forward_curve")

# History is already stored and re-downloading it is the entire cost above. A
# month covers any weekend-plus-holiday gap the fast path could need to backfill
# while staying inside the cheap window; INSERT OR REPLACE makes the overlap
# free. It is NOT used by the daily build, which still pulls DEFAULT_HISTORY_PERIOD
# so a fresh CI database is seeded with real history.
FAST_REFRESH_HISTORY_PERIOD = "1mo"

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

# Spot FX has no settlement, so the exchange cutoff above is the wrong
# question for Layer 7 and answering it there stored a partial bar as an FX
# close — measured live on 2026-08-19 at 03:45 UTC, when BRL=X returned a
# bar labelled 2026-08-19 with High == Open and Low == Close, an FX day less
# than four hours old, while Chicago local time was already past 14:30.
#
# The FX market runs continuously from Sunday 17:00 New York to Friday
# 17:00, and Yahoo labels the bar that *closes* at 17:00 on day D with day
# D's date. So the bar labelled D is unfinished until 17:00 New York on D.
# Expressed in venue-local time for the same reason as the cutoff above:
# US DST moves it against UTC twice a year.
FX_SESSION_TIMEZONE = "America/New_York"
FX_SESSION_CLOSE_LOCAL = (17, 0)  # (hour, minute) in FX_SESSION_TIMEZONE

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
# M14 #207 (built by M24 #271) replaced "every major growing region" with one
# standing rule: **every rendered price leg gets the weather that prices it,
# and weather with no rendered price leg downstream is not fetched.** Every pin
# below is therefore traceable to a leg on some page — a market page's own
# regions, or the headline's competing-oil strip (palm, canola).
#
# Dropped by that rule in M14: Ivory Coast (cocoa, no leg), China Jilin (its
# own comment said corn/feed proxy, not soy) and Thailand Surat Thani
# (Indonesia + Malaysia ≈ 85% of world palm and both are pinned). Their
# historical rows stop accruing; that is the intended cost.
# ---------------------------------------------------------------------------
OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

GROWING_REGIONS = {
    # ── US Soybean Belt (CBOT) ──
    "US Midwest (Iowa)":          {"lat": 42.03,  "lon": -93.47},
    "US Illinois":                {"lat": 40.12,  "lon": -89.30},   # #1 soybean state
    # IA+IL is ~30% of the crop and one climate zone; the classic US
    # divergence is east vs west (2023: IL fine, western belt burning).
    "US Nebraska":                {"lat": 40.90,  "lon": -98.40},   # western-belt pin

    # ── South America ──
    "Brazil Mato Grosso":         {"lat": -12.64, "lon": -55.42},   # #1 Brazil soy state
    "Brazil Parana":              {"lat": -24.04, "lon": -51.46},   # #2 Brazil soy state
    # ~20 Mt swing state with a failure mode MT does not share: La Niña
    # drought (-41% 2019/20, >50% loss 2021/22) and the May 2024 floods.
    "Brazil Rio Grande do Sul":   {"lat": -28.50, "lon": -53.50},
    "Argentina Pampas":           {"lat": -33.95, "lon": -60.33},   # Soy belt
    "Argentina Cordoba":          {"lat": -31.42, "lon": -64.18},   # #2 Argentina soy province
    # Sunflower, not soy — it prices the MAGyP sun-oil leg the Argentina page
    # renders (M5 #162). 2025/26 was a record 6.6 Mt crop centred here.
    "Argentina Buenos Aires (sunflower)": {"lat": -38.40, "lon": -60.30},
    # M14 #207: the old "Paraguay Chaco" pin (-22.35,-59.95) was in Boquerón —
    # cattle country. The soy belt is Alto Paraná–Itapúa–Canindeyú (>80% of
    # the crop), and 75–85% of its exports barge to the Rosario crush.
    "Paraguay Alto Parana":       {"lat": -25.90, "lon": -55.30},

    # ── Europe (rapeseed, not soy — the EC Moselle leg) ──
    "France Champagne (Grand Est)":    {"lat": 48.70, "lon": 4.30},  # ~4.6 Mt, EU #1
    "Germany Mecklenburg-Vorpommern":  {"lat": 53.60, "lon": 12.70},  # ~4.0 Mt, EU #2
    "Romania Baragan (Danube plain)":  {"lat": 44.60, "lon": 27.00},  # ~2.8 Mt, fastest-growing

    # ── Canada (canola — the ICE canola leg on the headline strip) ──
    "Canada Saskatchewan (Saskatoon)": {"lat": 52.10, "lon": -106.70},
    "Canada Alberta (central)":        {"lat": 53.00, "lon": -112.80},

    # ── Asia ──
    "Indonesia Riau (Sumatra)":   {"lat": 0.29,   "lon": 101.71},  # #1 palm oil belt
    "Malaysia Sabah (Borneo)":    {"lat": 5.42,   "lon": 116.80},  # #2 palm oil state
    "India Madhya Pradesh":       {"lat": 22.72,  "lon": 75.86},   # India soybean capital
    "India Maharashtra":          {"lat": 19.75,  "lon": 75.71},   # #2 India soybean state
    "China Heilongjiang":         {"lat": 47.36,  "lon": 127.76},  # China domestic soybean belt

    # ── Emerging Markets (soy deep dive) ──
    "South Africa Free State":    {"lat": -29.12, "lon": 26.21},   # SA #1 soy province
    "South Africa Mpumalanga":    {"lat": -25.47, "lon": 30.00},   # SA #2 soy province
    "Nigeria Benue":              {"lat": 7.73,   "lon": 8.52},    # Nigeria soy belt
    "Nigeria Kaduna":             {"lat": 10.52,  "lon": 7.43},    # Nigeria soy belt
}

WEATHER_DAILY_VARS = "temperature_2m_max,temperature_2m_min,precipitation_sum"

# ---------------------------------------------------------------------------
# Layers 27 / 28 — river gauges (M25 #272, M26 #273)
#
# River water is tradeable weather, which is why these sit beside
# GROWING_REGIONS rather than in a transport section: the number is a stage
# reading, and the trade it moves is the barge freight inside a cash bid.
#
#   27  Mississippi (NOAA NWPS)  — Memphis prices the barge freight inside the
#       `us_gulf:cif` ledger leg. The 2022 low took St. Louis barge rates from
#       ~$20 to ~$106/ton and US Gulf soybean basis to a record +$3.00/bu, and
#       it repeated in 2023 and 2024.
#   28  Paraná at Rosario (INA)  — ~80% of Argentine ag exports move on it. The
#       2021 low (0.06 m against a 2.92 m 24-year median, a 77-year record) cut
#       cargo sizes ~5,500-7,000 t and Rosario soy exports by more than two
#       thirds.
#
# TWO PROVIDERS, ONE SHAPE. Every gauge lands in one `river_levels` table with
# one set of columns, so the market stays a parameter (invariant 5) — but the
# providers are graded as two layers because a quiet Argentine endpoint must
# not take the Mississippi leg down with it, and vice versa.
#
# `unit` is `ft` or `m` and is stored on every row. This is the one series in
# the stack that is not a price: it never passes through `to_usd_mt`, and the
# two rivers must never share a unit label (invariant 7's neighbourhood, from
# the other side — a metre rendered as a foot parses perfectly).
#
# `timezone` is declared here rather than read off the payload. A stage series
# is bucketed into days in the *gauge's* local time — NWPS stamps every reading
# in UTC, so a 02:00Z reading belongs to the previous river day, and bucketing
# by UTC date would file half of every evening under tomorrow. NWPS does
# publish its own zone, but as a POSIX string (`CST6CDT`) rather than an IANA
# name, and mapping one to the other is a guess this file would rather state.
#
# `low_water` is a DECLARED trade threshold with a named basis, never an
# inferred one. St. Louis carries None deliberately: it is here as the
# barge-rate reference, and no threshold for it is sourced — inventing one to
# fill the column is exactly what invariant 2 forbids. A gauge with no
# threshold renders its level and its direction and raises no flag.
# ---------------------------------------------------------------------------
NWPS_GAUGE_URL = "https://api.water.noaa.gov/nwps/v1/gauges/{gauge_id}/stageflow"
INA_OBSERVATIONS_URL = "https://alerta.ina.gob.ar/a5/obs/puntual/series/{series_id}/observaciones"

# How much history each provider is asked for on every run. NWPS serves a
# rolling ~30-day observed window and nothing older, so the Mississippi legs
# depend on the `data/history/` round-trip; INA serves back to 1884, so the
# Paraná leg self-heals on an empty CI database for the window asked for here.
RIVER_NWPS_LOOKBACK_DAYS = 30
RIVER_INA_LOOKBACK_DAYS = 730

# Sanity band per unit. A stage outside it is a parse fault (a flow value read
# as a stage, a sentinel that escaped), not a river — NWPS prints -999/-9999
# for "no value" in the very same field.
RIVER_STAGE_BOUNDS = {"ft": (-60.0, 80.0), "m": (-5.0, 25.0)}

RIVER_GAUGES: dict[str, dict[str, Any]] = {
    "Mississippi at Memphis": {
        "provider": "nwps",
        "gauge_id": "MEMT1",
        "river": "Mississippi",
        "unit": "ft",
        "timezone": "America/Chicago",
        # DTN treats Memphis below -5 ft as the basis-moving regime. This is a
        # *trade* threshold and deliberately not NWPS's own `lowThreshold`
        # (-8 ft), which answers a navigation question, not a basis one.
        "low_water": -5.0,
        "low_water_basis": "DTN's basis-moving regime for the Memphis gauge",
        "attribution": "NOAA/NWS National Water Prediction Service — stage courtesy of USACE and USGS",
        "url": "https://water.noaa.gov/gauges/MEMT1",
        "note": (
            "the trade-watched gauge; stage is on the local Memphis gauge datum, "
            "which is why the record low reads -10.81 ft rather than an elevation"
        ),
    },
    "Mississippi at St. Louis": {
        "provider": "nwps",
        "gauge_id": "EADM7",
        "river": "Mississippi",
        "unit": "ft",
        "timezone": "America/Chicago",
        "low_water": None,
        "low_water_basis": None,
        "attribution": "NOAA/NWS National Water Prediction Service — stage courtesy of USACE and USGS",
        "url": "https://water.noaa.gov/gauges/EADM7",
        "note": "the barge-rate reference point, upstream of the Ohio confluence",
    },
    "Paraná at Rosario": {
        "provider": "ina",
        # INA a5 series 34 — station "Rosario" (id_externo 280, PNA), table
        # `alturas_prefe`: the daily Prefectura Naval reading, 53,677 values
        # back to 1884-01-02 (probed live 2026-08-21).
        "gauge_id": "34",
        "river": "Paraná",
        "unit": "m",
        "timezone": "America/Argentina/Buenos_Aires",
        # INA's own `nivel_aguas_bajas` for this station, read off the series
        # metadata rather than chosen here.
        "low_water": 1.64,
        "low_water_basis": "INA's own nivel de aguas bajas for the Rosario station",
        "attribution": "Instituto Nacional del Agua (INA) — reading by Prefectura Naval Argentina",
        "url": "https://alerta.ina.gob.ar/pub/mapa",
        "note": (
            "observed only — INA's forecast trace for this station (series 3387) "
            "answered empty on every probe, so the Paraná leg carries no forecast"
        ),
    },
}

# Per-layer catalogs. Keyed by provider so each layer is graded on its own
# gauges; the merged registry above is what the site reads.
RIVER_GAUGES_NWPS = {
    name: spec for name, spec in RIVER_GAUGES.items() if spec["provider"] == "nwps"
}
RIVER_GAUGES_INA = {
    name: spec for name, spec in RIVER_GAUGES.items() if spec["provider"] == "ina"
}

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
# Layer 26 / 26b — USDA AMS Grain Transportation Report (no API key)
#
# The transport legs the stack was missing: what it costs to move a cargo
# (26, ocean freight) and whether the boats are actually moving (26b, vessel
# lineups). Both come off the same weekly AMS publication, whose supporting
# tables are posted as standalone workbooks carrying the *whole* series —
# 1996 for freight, 1995 for vessels — so each run re-reads the full history
# and the layer self-heals on an empty CI database. Nothing here needs a
# data/history/ round-trip.
#
# The two are separate run units (the 2b/15b convention) because their
# cadences differ by an order of magnitude: freight is monthly, vessels are
# weekly, so one recency budget cannot grade both.
#
# The links are stable filenames under /sites/default/files/media/ rather
# than rotating GUIDs, so there is no landing-page resolution step here —
# but a frozen workbook is still the live risk, and LAYER_MAX_DATA_AGE_DAYS
# is what catches it.
# ---------------------------------------------------------------------------
GTR_DATASETS_LANDING_URL = (
    "https://www.ams.usda.gov/services/transportation-analysis/gtr-datasets"
)
GTR_OCEAN_FREIGHT_URL = (
    "https://www.ams.usda.gov/sites/default/files/media/GTRFigure20.xlsx"
)
GTR_VESSEL_ACTIVITY_URL = (
    "https://www.ams.usda.gov/sites/default/files/media/GTRTable19_Figure19.xlsx"
)

# Column index → route, read off the Figure 20 `Data` sheet. Indices are used
# because that sheet has no single header row (the labels are split across two
# banner rows), but they are never *trusted*: _parse_ocean_freight checks the
# published spread column against gulf - pnw on every row, so a column shift
# fails the parse instead of restating one route as the other.
GTR_OCEAN_ROUTES = {
    1: "US Gulf to Japan",
    3: "PNW to Japan",
}
GTR_OCEAN_SPREAD_COLUMN = 5

# Bulk-grain vessel freight, assessed by a broker and republished by USDA —
# it is not a USDA measurement and not an exchange print.
GTR_OCEAN_CADENCE = "monthly"
GTR_OCEAN_QUOTE_KIND = "freight assessment"
GTR_OCEAN_ATTRIBUTION = (
    "USDA AMS Grain Transportation Report (rates: O'Neil Commodity Consulting)"
)
# Ocean bulk grain freight has traded roughly $10-$120/MT since 1996. The band
# is deliberately wide: it is here to catch a column shift or a unit change,
# not to have an opinion on the freight market.
GTR_OCEAN_MIN_USD_MT = 3.0
GTR_OCEAN_MAX_USD_MT = 250.0

# The Figure 20 workbook contains a data-entry error its own sequence
# exposes: seven 2019 months are stored as datetimes in **1919** (Jun-Dec),
# sitting between "May '19" and 2020-01. The neighbours prove the intent, but
# rewriting a published year is inventing data, and storing 1919 would put a
# century-old row at the front of every chart and every "earliest
# observation" read. So an implausible year is rejected and named: seven
# months of 2019 are a visible, documented gap rather than a wrong number.
GTR_MIN_OBSERVATION_YEAR = 1990

# The published-arithmetic checks (Figure 20's spread, Table 19's in-port
# identity) exist to detect a **column shift**, not to audit the publisher.
# Measured 2026-08-19 against the live files: 0 of 367 freight months and 8
# of 1,649 vessel weeks fail, the latter scattered across 2018-2026 and off
# by 1-12 vessels — upstream noise, not a mapping fault. So an individual
# failure drops that row (its own components contradict it, and we cannot
# know which side is right) while a failure *rate* above this threshold
# means the mapping moved and the whole workbook is discarded.
GTR_MAX_ARITHMETIC_FAILURE_RATE = 0.05

# Table 19 columns, same deal: pinned by index, verified by the sheet's own
# arithmetic (in port = loading + waiting to load) wherever all three print.
# Vancouver (columns 15-17) is deliberately absent — it stopped being
# reported and every recent row is blank, so demanding it would fail the
# layer over a discontinued series.
GTR_PORT_REGIONS = {
    "US Gulf": {"loading": 2, "waiting_to_load": 3, "in_port": 4,
                "loaded_7day": 6, "due_10day": 8},
    "Pacific Northwest": {"loading": 10, "waiting_to_load": 11, "in_port": 12,
                          "loaded_7day": 13, "due_10day": 14},
}
GTR_VESSEL_CADENCE = "weekly"
GTR_VESSEL_UNIT = "vessels"
GTR_VESSEL_ATTRIBUTION = "USDA AMS Grain Transportation Report"
# A count of ships in a port region. Zero is legal (a holiday week); a
# hundred-plus is a parse fault, not a queue.
GTR_VESSEL_MAX_COUNT = 200

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
class CurveSpec(TypedDict):
    root: str
    exchange: str
    months: list[int]


FORWARD_CURVE_CONTRACTS: dict[str, CurveSpec] = {
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
WASDE_LAYOUT: dict[str, dict[str, str | None]] = {
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

# The same report over USDA's MARS API (#283): structured rows back to
# 2020-02-24, where the PDF above is only ever *today's* report — the archive
# is the whole reason this second transport exists. HTTP Basic with the key as
# the username; the prices live in the "Report Detail" section, since the base
# /reports/<slug> endpoint answers with report metadata only.
#
# Deliberately not the live path. The PDF is keyless, so an absent or rotated
# MARS_API_KEY costs the backfill and never the daily leg — the same degraded
# contract Layer 16 has, read at call time via
# fetchers.gulf_bids.is_api_configured().
# Annotated rather than bare: `NAME_API_KEY = <call>` is the literal-assignment
# shape the pre-commit secret scanner blocks on, and this reads an env var.
MARS_API_KEY: str = os.getenv("MARS_API_KEY", "")


# ---------------------------------------------------------------------------
# Key visibility — a degraded run must say so at the top, not only per layer
# ---------------------------------------------------------------------------
# Every env var any layer reads, with the layers it gates. Read at call time
# rather than off the constants above, because fetchers.mandi reads its key
# from os.environ directly and has no constant here.
API_KEY_LAYERS: dict[str, str] = {
    "USDA_API_KEY": "Layers 2, 14",
    "FRED_API_KEY": "Layer 3",
    "FAS_API_KEY": "Layer 10",
    "EIA_API_KEY": "Layer 13",
    "DATA_GOV_IN_API_KEY": "Layer 16 (degrades to the shared sample key)",
    "MARS_API_KEY": "Layer 20b backfill only",
}


def missing_api_keys() -> dict[str, str]:
    """The keys that are unset, as {name: the layers it gates}."""
    return {
        name: layers
        for name, layers in API_KEY_LAYERS.items()
        if not os.getenv(name)
    }


MARS_BASE_URL = "https://marsapi.ams.usda.gov/services/v1.2/reports"
MARS_GULF_BIDS_SLUG = 3147
# First report date slug 3147's *metadata* lists (probed 2026-08-21, #253) —
# the walk floor, not a promise of data. `Report Detail` answers HTTP 200 with
# zero rows for the early ones: 1,498 dates carry detail against 1,671 listed,
# and the first date that carries any is 2020-08-17 (measured 2026-08-22 over
# a full archive pull — every listed date before it answers empty).
# A per-date walk from here therefore logs a run of non-publications that are
# really the archive not reaching back as far as its own index claims.
MARS_GULF_BIDS_ARCHIVE_START = "2020-02-24"
# One pull of 6.5 years is tens of megabytes assembled server-side, which the
# 30s REQUEST_TIMEOUT every daily fetcher uses would cut off mid-archive —
# a timeout is the right answer for a daily leg and the wrong one here.
MARS_ARCHIVE_TIMEOUT = 600

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
    "US Midwest (Iowa)":         (7, 8),
    "US Illinois":               (7, 8),
    "US Nebraska":               (7, 8),
    "Brazil Mato Grosso":        (1, 2),
    "Brazil Parana":             (1, 2),
    "Brazil Rio Grande do Sul":  (1, 2),
    "Argentina Pampas":          (1, 2),
    "Argentina Cordoba":         (1, 2),
    "Paraguay Alto Parana":      (1, 2),
}

# Growing-season calendar (M14 #207, built by M24 #271). Same key shape as
# WEATHER_SOY_POD_FILL_MONTHS: keys must match GROWING_REGIONS names, and the
# completeness of the map is pinned by a test — a pin with no season would
# silently render as "in season" all year.
#
# Months are declared **in planting order**, so `months[0]` is the planting
# month and the out-of-season tag can name it ("planting ~Oct") without a
# second field. Southern-hemisphere seasons are therefore the wrap-around
# ones. Perennials (palm) and the Aug-sown/Jul-harvested EU rapeseed crop are
# in the ground all twelve months by construction, not by omission.
#
# What the calendar does NOT do: hide or collapse a card. September Iowa rain
# is real data that prices harvest logistics; the tag prices it, the card
# still renders. Agronomic alerts stay gated where they already were.
_ALL_YEAR = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
WEATHER_GROWING_SEASON_MONTHS = {
    # US soy: planted May, harvested Oct.
    "US Midwest (Iowa)":         (5, 6, 7, 8, 9, 10),
    "US Illinois":               (5, 6, 7, 8, 9, 10),
    "US Nebraska":               (5, 6, 7, 8, 9, 10),
    # Brazil soy: sown from Oct, harvested Feb–Mar.
    "Brazil Mato Grosso":        (10, 11, 12, 1, 2, 3),
    "Brazil Parana":             (10, 11, 12, 1, 2, 3),
    "Brazil Rio Grande do Sul":  (10, 11, 12, 1, 2, 3),
    # Argentina soy: sown Nov, harvested Apr–May.
    "Argentina Pampas":          (11, 12, 1, 2, 3, 4, 5),
    "Argentina Cordoba":         (11, 12, 1, 2, 3, 4, 5),
    # Argentine sunflower runs ahead of soy: sown Oct, harvested Feb–Mar.
    "Argentina Buenos Aires (sunflower)": (10, 11, 12, 1, 2, 3),
    # Paraguay sows ahead of Argentina, from September.
    "Paraguay Alto Parana":      (9, 10, 11, 12, 1, 2),
    # EU rapeseed: sown Aug, harvested the following Jul — always in ground.
    "France Champagne (Grand Est)":    _ALL_YEAR,
    "Germany Mecklenburg-Vorpommern":  _ALL_YEAR,
    "Romania Baragan (Danube plain)":  _ALL_YEAR,
    # Prairie canola: seeded May, harvested Sep.
    "Canada Saskatchewan (Saskatoon)": (5, 6, 7, 8, 9),
    "Canada Alberta (central)":        (5, 6, 7, 8, 9),
    # Oil palm is perennial — it is never out of season, it is only stressed.
    "Indonesia Riau (Sumatra)":  _ALL_YEAR,
    "Malaysia Sabah (Borneo)":   _ALL_YEAR,
    # India soy is a kharif crop: sown with the monsoon in Jun, harvested Oct.
    "India Madhya Pradesh":      (6, 7, 8, 9, 10),
    "India Maharashtra":         (6, 7, 8, 9, 10),
    # Heilongjiang: sown May, harvested Sep–Oct.
    "China Heilongjiang":        (5, 6, 7, 8, 9, 10),
    # South African soy: sown Nov, harvested Apr–May.
    "South Africa Free State":   (11, 12, 1, 2, 3, 4, 5),
    "South Africa Mpumalanga":   (11, 12, 1, 2, 3, 4, 5),
    # Nigerian soy follows the middle-belt rains: sown Jun, harvested Oct–Nov.
    "Nigeria Benue":             (6, 7, 8, 9, 10, 11),
    "Nigeria Kaduna":            (6, 7, 8, 9, 10, 11),
}

# Competing-oil weather strip (M14 #207) — the headline's Oilseed Complex
# section. Palm and canola price legs render on the four-oil board and nowhere
# else, so under M14's standing rule their weather belongs there too: **a
# strip, one line per belt, not a block and not region cards.** M2 #144 took
# the region cards off the headline and this does not put them back.
COMPETING_OIL_WEATHER_BELTS = (
    {
        "belt": "Palm",
        "leg": "palm oil",
        "regions": ("Indonesia Riau (Sumatra)", "Malaysia Sabah (Borneo)"),
        # Stated on the strip, always: palm weather does not price palm today.
        "note": (
            "palm yield lags weather by 9–12 months — today's stress prices "
            "next year's crop, not this week's board"
        ),
    },
    {
        "belt": "Canola prairies",
        "leg": "ICE canola",
        "regions": ("Canada Saskatchewan (Saskatoon)", "Canada Alberta (central)"),
        "note": "prairie weather moves the ICE canola leg within the season",
    },
)
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
    "gtr_vessels": 12,
    # Monthly publications — allow ~6 weeks.
    "gtr_ocean_freight": 42,
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
    # River gauges. Both are fixed-URL sources — nothing rotates, so a feed
    # that stops being refreshed answers 200 forever with the same stage and
    # nothing else in the payload marks it frozen (invariant 10 from the other
    # side). A river has a level every single day, so unlike a market leg
    # there is no weekend, no holiday and no quiet day to tolerate: 7 days is
    # already generous and exists only to absorb a run of failed fetches.
    #
    # The NWPS frame also carries forecast rows, and `_latest_observation_date`
    # drops them before dating the layer — otherwise a dead observed feed would
    # pass this budget on a 14-day forecast trace alone.
    "river_us": 7,
    "river_ar": 7,
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
    # GTR vessel lineups. The report lands Thursday and the newest week it
    # carries is the one that ended the *previous* Thursday, so the freshest
    # possible row is already ~7 days old and is ~14 days old the day before
    # the next release (measured 2026-08-19: newest week_ending 2026-08-06).
    # 21 is that worst case plus one missed publication. The workbook lives at
    # a fixed filename, which is exactly the World Bank / CIRCABC trap — a
    # file that stops being refreshed keeps answering 200 forever — so this
    # budget is the only thing standing between a frozen link and a layer
    # that reports success on 2026 numbers in 2027.
    "gtr_vessels": 21,
    # GTR ocean freight is monthly and the month is stamped to its first day,
    # so the newest row is ~30 days old the moment it publishes and ~60 the
    # day before the next one. 75 is that worst case plus a little slack, and
    # it is the same frozen-workbook guard as above rather than an opinion
    # about how often freight moves.
    "gtr_ocean_freight": 75,
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
# weather run that lost 6 of 24 regions returns an 18-key dict and would
# self-report 18/18 — exactly the outage coverage exists to expose. A layer
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
    "gtr_ocean_freight": GTR_OCEAN_ROUTES,
    "gtr_vessels": GTR_PORT_REGIONS,
    # river_ar is deliberately absent: one gauge, and 1/1 is noise.
    "river_us": RIVER_GAUGES_NWPS,
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

# How far a price may be revised, against the value already stored under the
# *same primary key*, before the store layer quarantines it instead of letting
# INSERT OR REPLACE overwrite (T19 · F9, #67). See pipeline/divergence.py.
#
# 20% matches the trust ledger's DAILY_MOVE_QUARANTINE_THRESHOLD, and the
# choice is conservative twice over: that number is already above CBOT's own
# *expanded* daily limits, and this rule compares two readings of the SAME
# session rather than two consecutive sessions — a re-print of one day's close
# that moves a fifth is not a correction any venue publishes. Erring high is
# deliberate: a threshold that catches real revisions would suppress the
# corrections this project depends on, and a suppressed correction is the same
# silent wrongness the guard exists to prevent (invariant 11 cuts the other
# way here — the stored value is not a gap).
SAME_PK_DIVERGENCE_QUARANTINE_THRESHOLD = 0.20

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
        # NAMED CONTRACTS, not the continuous front-month series. `prices`
        # holds Yahoo's ZS=F/ZM=F/ZL=F, which name no delivery month and roll
        # silently on the provider's own schedule — three of them make a margin
        # nobody can reproduce and nobody can hedge. `contracts: "named"` routes
        # this descriptor to `analysis.futures.crush`, which strikes the margin
        # on ZSU26/ZMU26/ZLU26 out of `forward_curve` and withholds it when
        # those cannot be had on one session.
        "crush": {
            "kind": "board",
            "contracts": "named",
            "layer": "forward_curve",
            "yield_set": "soy_board",
            "table": "forward_curve",
            "date_column": "observation_date",
            "key_column": "commodity",
            "legs": {"bean": "Soybeans", "oil": "Soybean Oil", "meal": "Soybean Meal"},
            "value_column": "close",
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
        # (region, role) — the role says why this pin is on this page. M14 #207
        # / M24 #271: a label, never a code path.
        "weather_regions": [
            ("US Midwest (Iowa)", "domestic crop"),
            ("US Illinois", "domestic crop"),
            ("US Nebraska", "domestic crop — western belt"),
        ],
        # M25 #272. Water is this page's freight leg: the Memphis gauge prices
        # the barge freight inside the `us_gulf:cif` ledger leg, and St. Louis
        # is the barge-rate reference point above the Ohio confluence. Rendered
        # inside block 06 rather than as a tenth block — the nine ids are the
        # contract, and river water is tradeable weather.
        "river_gauges": ["Mississippi at Memphis", "Mississippi at St. Louis"],
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
        # Continuous main-contract series (akshare A0/M0/Y0): the underlying
        # contract changes silently and is not published, so this margin is a
        # China board *reference* and never a hedgeable one. Declared rather
        # than inferred — that is the whole point of the field.
        "crush": {
            "kind": "board",
            "contracts": "continuous",
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
        # Dalian is the one market whose weather is half somebody else's: the
        # domestic crop prices No.1, and Brazil (73.6% of China's 111.8 Mt
        # 2025 imports) prices No.2 and meal. Jilin was dropped — corn/feed.
        "weather_regions": [
            ("China Heilongjiang", "domestic crop — prices No.1"),
            ("Brazil Mato Grosso", "import origin — prices No.2 and meal"),
        ],
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
        "weather_regions": [
            ("Brazil Mato Grosso", "domestic crop"),
            ("Brazil Parana", "domestic crop"),
            ("Brazil Rio Grande do Sul", "domestic crop — the La Niña swing state"),
        ],
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
            "contracts": "administered",
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
        "weather_regions": [
            ("Argentina Pampas", "domestic crop"),
            ("Argentina Cordoba", "domestic crop"),
            ("Paraguay Alto Parana", "crushed in Rosario — 75–85% of the crop barges down"),
            ("Argentina Buenos Aires (sunflower)", "sunflower — prices the sun-oil leg"),
        ],
        # M26 #273. ~80% of Argentine ag exports move down the Paraná, and the
        # draft at Rosario sets how much of a cargo each vessel can lift.
        "river_gauges": ["Paraná at Rosario"],
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
        "weather_regions": [
            ("India Madhya Pradesh", "domestic crop — kharif, monsoon-timed"),
            ("India Maharashtra", "domestic crop — kharif, monsoon-timed"),
        ],
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
        # M14 #207 filled the empty block on the page whose leg is the most
        # locally weather-driven one on the map. These pins are RAPESEED, not
        # soy, and the role label is what says so on the page.
        "weather_regions": [
            ("France Champagne (Grand Est)", "rapeseed, not soy — EU #1"),
            ("Germany Mecklenburg-Vorpommern", "rapeseed, not soy — EU #2"),
            ("Romania Baragan (Danube plain)", "rapeseed, not soy — the risk FR/DE miss"),
        ],
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
        "weather_regions": [
            ("South Africa Free State", "domestic crop"),
            ("South Africa Mpumalanga", "domestic crop"),
        ],
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
        "weather_regions": [
            ("Nigeria Benue", "domestic crop"),
            ("Nigeria Kaduna", "domestic crop"),
        ],
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
# the rows that are a flat-price yardstick rather than a peer offer. Rows
# render in DECLARED ORDER (M20 #236 — position is role in the trade, never
# recency), so a reference leg must be declared last; app/markets.py refuses
# the set otherwise.
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
# CRUSH_BOARD — which markets sit on the headline crush board (M16 #208)
#
# Registry data for the same reason LEDGERS is: which markets are compared
# side by side is a decision about the trade, and a builder that derived it
# ("every market with a crush descriptor") would answer a different question —
# it would drop Brazil, whose empty card is the point.
#
# Four, decided by M2 #144, ordered as MARKETS is (role in the trade):
#
# * CBOT and Dalian — the two boards, one on named contracts, one on
#   continuous main-contract series, and the cards say which is which.
# * Brazil — the largest exporter, and the one leg that does NOT compute: no
#   oil or meal cash quote is ingested, so the card carries the registry's own
#   reason (an unbuilt scrape, M7 #149). A board of the three that happen to
#   work would read as though Brazil had no crush industry.
# * Argentina — the largest crusher of the four, struck off Ley 21.453
#   administered FOB minimums. Verified, not provisional, since #162.
#
# Kinds are never collapsed: board, physical and administered are three
# different claims and every card labels its own (M2 constraint 3).
CRUSH_BOARD = ("cbot", "dalian", "brazil", "argentina")


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

# Hand-entered or imported positions for the Phase 3 workstation. See
# analysis/futures/positions.py — this project ingests no account, broker
# statement or clearing feed, so a position can only come from the user, and a
# missing directory is a legitimately empty book rather than a fault.
#
# MIRROR_POSITIONS_DIR overrides the location, for the same dev-loop and test
# reason ASSUMPTIONS_DIR does, and is likewise never set in CI: a fixture
# position must not be able to reach a published page.
POSITIONS_DIR = os.getenv("MIRROR_POSITIONS_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "positions"
)

# Hand-entered option quotes, on exactly the same terms as POSITIONS_DIR above
# and for exactly the same reason: no layer here publishes an option chain
# (verified against the incumbent provider — see data/reference/options/
# README.md), so a premium or an implied volatility can only come from the
# user's own broker screen.
OPTIONS_DIR = os.getenv("MIRROR_OPTIONS_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "options"
)

# Official clearing / broker statements, on the same terms again. These are the
# client's *authoritative* numbers — the settlement prices and the realised and
# unrealised P&L their clearer margined the account at — and they exist here for
# one reason: so a management estimate marked to delayed closes can be shown
# beside the official figure and reconciled, never merged into it.
# See analysis/futures/clearing.py.
CLEARING_DIR = os.getenv("MIRROR_CLEARING_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "clearing"
)

# Column-mapping profiles for broker / clearing / ERP CSV exports. A profile
# names somebody's broker and their column conventions, so it is a client record
# too and is gitignored with the rest. See analysis/futures/imports.py.
IMPORT_PROFILE_DIR = os.getenv("MIRROR_IMPORT_PROFILE_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "import_profiles"
)

# How close a contract's first notice day has to be before an open position in
# it is reported as first-notice risk. Ten business days is the window a
# merchant rolls in: past FND a long is exposed to delivery, and the alert has
# to arrive with enough time to roll rather than on the day.
FIRST_NOTICE_WARNING_DAYS = 10

# How far a management mark-to-market may sit from the clearer's official
# unrealised P&L before the reconciliation calls the line a difference rather
# than a rounding artefact. In USD per position line. A delayed close against an
# official settlement is genuinely a different number, so this is not zero; it
# is small enough that a wrong contract size or a missing fill cannot hide in it.
CLEARING_RECONCILIATION_TOLERANCE_USD = 25.0

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


# ---------------------------------------------------------------------------
# OPPORTUNITY ENGINE (Phase 4)
#
# Turning the Players knowledge base and the ingested market layers into
# candidate physical business. Every threshold that decides whether something is
# worth a trader's attention lives here rather than in a rule body, so the
# answer to "why did this fire" is a number a reader can look up and change.
#
# The rules themselves are in analysis/opportunities/rules.py. What is
# configurable is the *sensitivity*, never the *logic*: a rule that could be
# switched from "landed advantage" to "price difference" by a config edit would
# put the one distinction this phase exists to keep behind a YAML key.
# ---------------------------------------------------------------------------

# Bumped whenever a rule's arithmetic, a score component's formula, or the
# component weights change. Stored on every detection, so an archived
# opportunity can be read against the method that produced it.
OPPORTUNITY_METHOD_VERSION = "1.0.0"

# Where the local, private trader workflow lives: status, owner, notes, contact
# dates, outcomes. This directory is NEVER read by the public page builder and
# is gitignored — see data/reference/opportunities/README.md.
#
# MIRROR_OPPORTUNITY_DIR overrides it, for the dev loop and for tests, the same
# reason ASSUMPTIONS_DIR and POSITIONS_DIR carry an override. It is deliberately
# not set in CI: a fixture note must not be able to reach a rendered page.
OPPORTUNITY_WORKFLOW_DIR = os.getenv("MIRROR_OPPORTUNITY_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "opportunities"
)

# Where the *private* render goes. Outside docs/ on purpose: docs/ is what the
# Pages deploy uploads, so anything carrying a trader's own notes must not be
# able to land in it by a path mistake. Gitignored.
OPPORTUNITY_PRIVATE_OUTPUT_DIR = os.getenv("MIRROR_PRIVATE_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "workspace"
)

# Score component weights. They sum to 1.0 (checked in ScoreCard.__post_init__).
#
# Economic attractiveness is deliberately NOT the largest weight. A big number
# on stale evidence with no counterparty and an unbridged incoterm is the single
# most common false positive in this domain, and weighting the money above
# everything else is how a screen fills up with them.
OPPORTUNITY_SCORE_WEIGHTS: dict[str, float] = {
    "economic": 0.30,
    "evidence": 0.20,
    "freshness": 0.15,
    "counterparty": 0.15,
    "feasibility": 0.20,
}

# The edge, in USD/MT, that scores 100 on economic attractiveness. Above it the
# component saturates rather than running away: the difference between a 40 and
# an 80 dollar advantage is not twice as interesting, it is "both are enormous,
# go and check the inputs".
OPPORTUNITY_ECONOMIC_FULL_SCALE_USD_MT = 25.0

# Per-rule thresholds and validity horizons. `validity_days` is how long the
# observation stays meaningful, and it is a property of the SOURCE's cadence,
# not of the rule's importance: a weekly inspections number is not stale in
# three days just because prices are.
OPPORTUNITY_RULES: dict[str, dict[str, Any]] = {
    "landed_advantage": {
        "label": "Origin landed advantage",
        "min_advantage_usd_mt": 5.0,
        "validity_days": 3,
        "question": "which origin is cheapest delivered, and by enough to matter",
    },
    "destination_flow_shift": {
        "label": "Destination flow shift",
        # Share of a week's US inspections going to one destination, against its
        # own trailing mean. Two sigma on a 26-week baseline: below that a
        # single large vessel moves the number.
        "min_z": 2.0,
        "baseline_weeks": 26,
        "min_weeks": 12,
        "min_share": 0.05,
        "validity_days": 10,
        "question": "who has started taking cargo they were not taking",
    },
    "commitment_shift": {
        "label": "Export commitment shift",
        # Outstanding sales (sold, not yet shipped) to one destination against
        # its own trailing mean. This is forward demand, which is why it is a
        # separate rule from shipped inspections.
        "min_z": 2.0,
        "baseline_weeks": 26,
        "min_weeks": 12,
        "min_share": 0.05,
        "validity_days": 10,
        "question": "who has bought forward and not yet shipped",
    },
    "supply_deficit": {
        "label": "Buyer-region tight stocks",
        # Stocks-to-use below its own prior-window low, on PSD. Reuses
        # analysis/stocks_to_use.py rather than restating the ratio.
        "validity_days": 40,
        "question": "which importing region is running its balance sheet thin",
    },
    "crush_margin": {
        "label": "Favourable crush margin",
        "min_margin_usd_mt": 15.0,
        "validity_days": 5,
        "question": "which crusher is earning enough to bid up for beans",
    },
    "currency_shift": {
        "label": "Currency move changes origin competitiveness",
        # A move in the origin's own currency against the dollar over the
        # lookback. Five percent in twenty sessions is a real repricing of a
        # local seller's incentive to ship, not noise.
        "min_move_pct": 5.0,
        "lookback_sessions": 20,
        "validity_days": 5,
        "question": "whose farmer just got paid more, or less, for the same cargo",
    },
}

# How many days past its expiry an opportunity stays visible, marked expired,
# before it drops off entirely. A screen that silently deletes yesterday's items
# cannot be checked against yesterday's decisions.
OPPORTUNITY_EXPIRY_GRACE_DAYS = 7

# Two candidates on the same lane and product from different rules are not
# duplicates — they are corroboration — but they should be linked rather than
# read as two independent findings. This is the lane-level grouping key's scope.
OPPORTUNITY_RELATED_ON = ("product", "origin", "destination")

# Counterparty candidates carried per side. Six is what the origins page uses;
# beyond that the list stops being a shortlist.
OPPORTUNITY_COUNTERPARTY_LIMIT = 6


# ---------------------------------------------------------------------------
# Phase 5 — the trader validation trial
#
# A 30-trading-day shadow trial measuring one thing: does Mirror Market reduce
# a professional soy trader's reliance on an external terminal, a broker call or
# a spreadsheet, WITHOUT increasing decision risk. Both halves matter. A tool
# that answers everything and is wrong twice is worse than one that answers half
# and says so.
#
# What is configurable here is the *protocol*: the window, the thresholds a
# go/no-go is read against, and where the records live. What is NOT configurable
# is whether a record may be published — that is structural (analysis/trial/
# sanitize.py) for the same reason the opportunity workflow's boundary is.
# ---------------------------------------------------------------------------

# Bumped whenever the session-record schema, a metric's formula, or a decision
# threshold changes. Stamped on every captured session, so a result recorded in
# week 1 can be read against the protocol that was in force when it was taken.
TRIAL_PROTOCOL_VERSION = "1.0.0"

# The trial window, in *trading* days — weekends and CBOT holidays do not count,
# because a session on a day the board is shut measures nothing.
TRIAL_WINDOW_TRADING_DAYS = 30

# The minimum number of independent traders. Two is the floor stated in the
# brief; one trader's preference is a taste, not a finding.
TRIAL_MIN_TRADERS = 2

# Where the private trial records live: trader ids, session notes, decisions,
# counterparties, and every commercial judgement made during the window. This
# directory is gitignored and is NEVER read by any builder that writes into
# docs/ — see data/reference/trial/README.md.
#
# MIRROR_TRIAL_DIR overrides it, for the dev loop and for tests, the same reason
# OPPORTUNITY_WORKFLOW_DIR and POSITIONS_DIR carry an override. It is
# deliberately not set in CI: a fixture session must not be able to reach a
# rendered page or a published metric.
TRIAL_RECORD_DIR = os.getenv("MIRROR_TRIAL_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "reference", "trial"
)

# Where the private trial dashboard is rendered. Outside docs/ on purpose, and
# for the same reason the private opportunity edition is: docs/ is what the
# Pages deploy uploads, so anything carrying a trader's own words must not be
# able to land in it by a path mistake. Gitignored.
TRIAL_PRIVATE_OUTPUT_DIR = os.getenv("MIRROR_TRIAL_PRIVATE_DIR") or os.path.join(
    os.path.dirname(__file__), "data", "workspace", "trial"
)

# Trader confidence is recorded on a 1-5 scale. It is an ordinal opinion, not a
# measurement, and is reported as a median and a distribution — never a mean,
# which would invent a precision the scale does not carry.
TRIAL_CONFIDENCE_SCALE = (1, 2, 3, 4, 5)

# Decision thresholds. These are read by analysis/trial/review.py to produce the
# go / no-go recommendation, and they are stated here rather than in the rule
# body so that the answer to "why did it say no-go" is a number a reader can
# look up — and so that moving a goalpost is a visible diff.
#
# `go` is the bar for recommending broader use. `no_go` is the bar below which
# broader use is recommended against. Between them is `hold` — extend the trial,
# fix what the issues log names, and re-measure. Three outcomes, because a
# binary forces a verdict the evidence may not support.
TRIAL_DECISION_THRESHOLDS: dict[str, dict[str, float]] = {
    # Share of started sessions that reached a decision or output.
    "task_completion_rate": {"go": 0.90, "no_go": 0.70},
    # External lookups per completed task. The headline number of the whole
    # trial: it is the reduction in terminal reliance, measured.
    "external_lookups_per_task": {"go": 1.0, "no_go": 2.5},
    # Share of sessions in which the trader hit a number that was wrong or past
    # its own cadence without the page saying so. This is the risk half, and its
    # bar is deliberately the strictest on the board.
    "wrong_or_stale_rate": {"go": 0.02, "no_go": 0.10},
    # Alerts that fired on nothing, or failed to fire on something.
    "false_alert_rate": {"go": 0.05, "no_go": 0.20},
    "missed_alert_rate": {"go": 0.05, "no_go": 0.20},
    # Would the trader act on the output, unaided.
    "would_act_rate": {"go": 0.75, "no_go": 0.50},
    # Median trader confidence, 1-5.
    "median_confidence": {"go": 4.0, "no_go": 3.0},
    # Share of the 30 windows in which the promoted edition was the day's, and
    # every critical source was inside its own cadence budget.
    "deployment_reliability": {"go": 0.95, "no_go": 0.85},
    "critical_source_availability": {"go": 0.95, "no_go": 0.85},
}

# Metrics where a SMALLER number is the better one. Kept as its own set rather
# than a flag inside the threshold dict, because the direction of a metric is a
# property of the metric, not of the bar it is being read against — and a bar
# can be moved without changing which way is good.
TRIAL_LOWER_IS_BETTER = frozenset({
    "external_lookups_per_task",
    "wrong_or_stale_rate",
    "false_alert_rate",
    "missed_alert_rate",
})

# A metric with fewer than this many observations reports `insufficient` rather
# than a rate. Three sessions do not make a completion rate, and a go/no-go read
# off one is worse than no go/no-go at all.
TRIAL_MIN_OBSERVATIONS = 10

# The nine rubric dimensions of the final scorecard, each scored 0-5 against the
# same strict professional rubric the earlier audits used. Order is the order a
# trader would ask them in.
TRIAL_SCORECARD_DIMENSIONS = (
    "precision",
    "accuracy",
    "reliability",
    "timeliness",
    "physical_usefulness",
    "futures_usefulness",
    "opportunity_usefulness",
    "ux",
    "trader_trust",
)


# The layers a soy trader's daily decisions actually rest on. Availability is
# measured against THIS set rather than all 27, because a CEC release slipping a
# week is not the same event as the CBOT board going dark, and averaging them
# produces an availability number that stays green through an outage that
# matters. main.CRITICAL_LAYERS is a different and narrower list — it decides
# the pipeline's exit code, not what a trader needs on screen.
TRIAL_CRITICAL_LAYERS = (
    "prices",         # the board itself
    "currencies",     # every non-USD leg is unreadable without it
    "fred",           # the dollar, and the macro frame
    "forward_curve",  # nothing can be hedged off a continuous series
    "export_sales",   # the demand side of every China question
    "psd",            # the balance sheet behind stocks-to-use
    "weather",        # the supply risk the board prices first
    "dce",            # the destination market
    "gulf_bids",      # US origin, physical
    "agrural",        # Brazil origin, physical
    "magyp_fob",      # Argentina origin, physical
    "cepea",          # Brazil domestic, the basis leg
)
