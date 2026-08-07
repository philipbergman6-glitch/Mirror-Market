"""
Soy Complex Analytics Team.

This module is the "analytics desk" for a professional soy complex trader.
Each function represents an analyst who processes raw data into tradeable
intelligence across Soybeans (ZS=F), Soybean Oil (ZL=F), and Soybean Meal (ZM=F).

The analysts:
    1. Command Center   — snapshot of all 3 legs + crush + key metrics
    2. Supply Analyst    — WASDE balance sheet, CONAB vs USDA, PSD global stocks
    3. Demand Analyst    — China buying pace, crush volumes, biodiesel pull, inspections
    4. Technicals Analyst — technicals + signals for all 3 soy legs
    5. Relative Value    — soy oil vs palm oil, crush margin, inter-leg ratios
    6. Risk Analyst      — BRL/USD, COT crowding, weather threats
    7. Seasonal Analyst  — current vs historical norms for all 3 legs
    8. Forward Curve     — term structure for all 3 soy contracts

Key concepts for learning:
    - Each analyst returns a dict of structured data (not display strings)
    - The dashboard renders the data; the analyst just computes it
    - This separation means you could use the same analysts for
      email reports, Slack bots, or algorithmic signals
"""

import logging
from typing import Any

import pandas as pd

from analysis.forward_curve import analyze_curve, calendar_spread
from analysis.loaders import load_currencies, load_prices
from analysis.nass_crush import latest_crush
from analysis.seasonal import current_vs_seasonal, monthly_seasonal
from analysis.signals import demote_near_roll_signals, detect_all_signals
from analysis.spreads import (
    compute_brazil_basis,
    compute_crush_spread,
    compute_domestic_basis,
)
from analysis.stocks_to_use import compute_stocks_to_use, detect_tight_supply
from config import (
    CONAB_FARMGATE_SERIES,
    CRUSH_MEAL_FACTOR,
    CRUSH_OIL_FACTOR,
    MANDI_SERIES,
    MANDI_SERIES_MH,
    WEATHER_DRY_THRESHOLD_MM,
    WEATHER_EXTREME_HEAT_C,
    WEATHER_HEAVY_RAIN_MM,
)
from pipeline.query import (
    read_brazil_estimates,
    read_brazil_spot,
    read_cot,
    read_crop_progress,
    read_dce_futures,
    read_economic,
    read_eia_data,
    read_export_sales,
    read_forward_curve,
    read_india_domestic,
    read_inspections,
    read_psd,
    read_safex,
    read_wasde,
    read_weather,
)
from pipeline.units import convert_df_to_mt, mt_label, to_metric_tons

logger = logging.getLogger(__name__)

# The 3 soy legs — everything in this module focuses on these
SOY_LEGS = ["Soybeans", "Soybean Oil", "Soybean Meal"]

# Minimum observations before 1Y basis distribution stats are reported.
_BASIS_STATS_MIN_OBS = 20

# Key growing regions for soy
SOY_WEATHER_REGIONS = [
    "US Midwest (Iowa)", "US Illinois",
    "Brazil Mato Grosso", "Brazil Parana",
    "Argentina Pampas", "Argentina Cordoba",
    "Paraguay Chaco",
    "India Madhya Pradesh", "India Maharashtra",
    "China Heilongjiang",
    "South Africa Free State", "South Africa Mpumalanga",
    "Nigeria Benue", "Nigeria Kaduna",
]

# Key currencies for soy trade
SOY_CURRENCIES = ["BRL/USD", "CNY/USD", "ARS/USD", "ZAR/USD", "NGN/USD", "INR/USD"]

# Emerging market countries for deep dive
EMERGING_MARKET_COUNTRIES = ["South Africa", "India", "Nigeria", "Brazil"]
EMERGING_MARKET_CURRENCIES = {
    "South Africa": "ZAR/USD",
    "India": "INR/USD",
    "Nigeria": "NGN/USD",
    "Brazil": "BRL/USD",
}
EMERGING_MARKET_WEATHER = {
    "South Africa": ["South Africa Free State", "South Africa Mpumalanga"],
    "India": ["India Madhya Pradesh", "India Maharashtra"],
    "Nigeria": ["Nigeria Benue", "Nigeria Kaduna"],
    "Brazil": ["Brazil Mato Grosso", "Brazil Parana"],
}


# ---------------------------------------------------------------------------
# Helper: filter the shared loaders down to the soy-relevant subset.
# ---------------------------------------------------------------------------

_SOY_PRICE_TARGETS = SOY_LEGS + ["Palm Oil (CME)", "Corn"]


def _load_soy_prices() -> dict[str, pd.DataFrame]:
    """Soy-relevant slice of `load_prices(with_technicals=True)`."""
    all_prices = load_prices(with_technicals=True)
    return {c: all_prices[c] for c in _SOY_PRICE_TARGETS if c in all_prices and not all_prices[c].empty}


def _load_currency_data() -> dict[str, pd.DataFrame]:
    """Soy-relevant slice of `load_currencies()`."""
    all_currencies = load_currencies()
    return {p: all_currencies[p] for p in SOY_CURRENCIES if p in all_currencies and not all_currencies[p].empty}


# Session-count conventions for percentage changes: iloc[-1] vs
# iloc[-1 - sessions], so 5 sessions ≈ one trading week, 21 ≈ one month.
_WEEKLY_SESSIONS = 5
_MONTHLY_SESSIONS = 21


def _asof(ts) -> str | None:
    """ISO date string for an observation timestamp (F15 as-of), or None.

    Every trader-facing figure carries the date it was observed so a stale
    print can never masquerade as today's market.
    """
    if ts is None or pd.isna(ts):
        return None
    return str(pd.Timestamp(ts).date())


def _pct_chg(series: pd.Series, sessions: int) -> float | None:
    """% change over the last `sessions` trading sessions, or None if the
    series is too short (or the base value is zero)."""
    if len(series) <= sessions:
        return None
    prev = series.iloc[-1 - sessions]
    if pd.isna(prev) or prev == 0:
        return None
    return float((series.iloc[-1] - prev) / prev * 100)


# ---------------------------------------------------------------------------
# Analyst 1: Command Center — the top-level snapshot
# ---------------------------------------------------------------------------

def command_center() -> dict:
    """
    Build the command center snapshot — everything a trader glances at first.

    Returns dict with:
        legs: list of dicts, one per soy leg with price/change/RSI/MACD/vol
        crush: dict with current spread, trend, profitability
        signals: list of active signals across all 3 legs
        key_metrics: dict of headline numbers (BRL, China exports, etc.)
    """
    prices = _load_soy_prices()
    currencies = _load_currency_data()

    # --- Leg summaries ---
    legs = []
    all_signals = []

    for leg in SOY_LEGS:
        df = prices.get(leg)
        if df is None or df.empty:
            legs.append({"name": leg, "available": False})
            continue

        latest = df.iloc[-1]

        # Convert prices to USD/MT for display
        close_mt = to_metric_tons(latest["Close"], leg)
        ma_50_mt = to_metric_tons(latest.get("MA_50", 0), leg) if pd.notna(latest.get("MA_50")) else None
        ma_200_mt = to_metric_tons(latest.get("MA_200", 0), leg) if pd.notna(latest.get("MA_200")) else None
        bb_upper_mt = to_metric_tons(latest.get("BB_Upper", 0), leg) if pd.notna(latest.get("BB_Upper")) else None
        bb_lower_mt = to_metric_tons(latest.get("BB_Lower", 0), leg) if pd.notna(latest.get("BB_Lower")) else None

        leg_info = {
            "name": leg,
            "available": True,
            "as_of": _asof(df.index[-1]),
            "close": close_mt,
            "close_native": latest["Close"],
            "unit": mt_label(leg),
            "daily_chg": latest.get("daily_pct_change", 0),
            "weekly_chg": latest.get("weekly_pct_change", 0),
            "rsi": latest.get("RSI"),
            "macd_hist": latest.get("MACD_Histogram"),
            "ma_50": ma_50_mt,
            "ma_200": ma_200_mt,
            "hv_20": latest.get("HV_20"),
            "bb_upper": bb_upper_mt,
            "bb_lower": bb_lower_mt,
            "volume": latest.get("Volume"),
        }

        # Trend assessment — compare in native units (Close vs the raw MA
        # columns); mixing the native Close with the MT-converted MA made
        # the verdict a unit artifact, not a trend.
        if pd.notna(latest.get("MA_200")):
            leg_info["trend"] = "Bullish" if latest["Close"] > latest["MA_200"] else "Bearish"
        elif pd.notna(latest.get("MA_50")):
            leg_info["trend"] = "Bullish" if latest["Close"] > latest["MA_50"] else "Bearish"
        else:
            leg_info["trend"] = "N/A"

        legs.append(leg_info)

        # Collect signals for this leg
        signals = detect_all_signals(df, leg)
        all_signals.extend(signals)

    # --- Crush spread ---
    crush_info: dict[str, Any] = {"available": False}
    beans = prices.get("Soybeans")
    oil = prices.get("Soybean Oil")
    meal = prices.get("Soybean Meal")

    if (
        beans is not None and oil is not None and meal is not None
        and not beans.empty and not oil.empty and not meal.empty
    ):
        try:
            spread = compute_crush_spread(beans, oil, meal)
            if not spread.empty:
                latest_cents = spread.iloc[-1]["crush_spread"]
                # Convert crush spread to USD/MT
                # Crush spread is in cents/bu; same conversion as soybeans
                crush_mt = to_metric_tons(latest_cents, "Soybeans")
                crush_info = {
                    "available": True,
                    "as_of": _asof(spread.iloc[-1].get("Date")),
                    "value_usd_mt": crush_mt,
                    "value_dollars_bu": latest_cents / 100,
                    "profitable": latest_cents > 0,
                    "spread_series": spread,
                }
                if len(spread) >= 6:
                    prev_cents = spread.iloc[-6]["crush_spread"]
                    crush_info["trend"] = "widening" if latest_cents > prev_cents else "narrowing"
                    crush_info["weekly_chg"] = latest_cents - prev_cents
        except Exception:
            logger.warning("Command-center crush computation failed", exc_info=True)

    # --- Key metrics ---
    key_metrics = {}

    # BRL/USD
    if "BRL/USD" in currencies and not currencies["BRL/USD"].empty:
        brl = currencies["BRL/USD"]
        key_metrics["brl_usd"] = brl["Close"].iloc[-1]
        key_metrics["brl_usd_date"] = _asof(brl.index[-1])
        brl_weekly = _pct_chg(brl["Close"], _WEEKLY_SESSIONS)
        if brl_weekly is not None:
            key_metrics["brl_weekly_chg"] = brl_weekly

    # CNY/USD
    if "CNY/USD" in currencies and not currencies["CNY/USD"].empty:
        cny = currencies["CNY/USD"]
        key_metrics["cny_usd"] = cny["Close"].iloc[-1]
        key_metrics["cny_usd_date"] = _asof(cny.index[-1])

    # Dollar index
    econ = read_economic()
    if not econ.empty:
        dollar = econ[econ["series_name"] == "US Dollar Index"].sort_values("Date")
        if not dollar.empty:
            key_metrics["dollar_index"] = dollar.iloc[-1]["value"]
            key_metrics["dollar_index_date"] = _asof(dollar.iloc[-1]["Date"])

    # Sort signals by severity
    all_signals = demote_near_roll_signals(all_signals)
    severity_order = {"alert": 0, "warning": 1, "info": 2}
    all_signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    return {
        "legs": legs,
        "crush": crush_info,
        "signals": all_signals,
        "key_metrics": key_metrics,
    }


# ---------------------------------------------------------------------------
# Analyst 2: Supply — balance sheet and production
# ---------------------------------------------------------------------------

def supply_analysis() -> dict:
    """
    Build the supply-side picture: WASDE, CONAB, PSD, crop progress.

    Returns dict with:
        wasde: dict per commodity with latest estimates + MoM revisions
        conab_vs_usda: comparison of Brazil soy production estimates
        psd_highlights: key global supply numbers
        crop_progress: latest US crop condition/progress
    """
    # --- WASDE ---
    wasde_data = read_wasde()
    wasde_summary = {}

    if not wasde_data.empty:
        for commodity in wasde_data["commodity"].unique():
            subset = wasde_data[wasde_data["commodity"] == commodity]
            attrs = {}
            for attribute in subset["attribute"].unique():
                attr_rows = subset[subset["attribute"] == attribute]
                if attr_rows.empty:
                    continue
                # WASDE rows include multiple marketing years per release (e.g. 2024/25
                # Est. and 2025/26 Proj. in the same April report). Pin to the latest
                # MY so the MoM revision math compares like with like.
                latest_my = attr_rows["year"].max()
                attr_rows = attr_rows[attr_rows["year"] == latest_my]
                attr_rows = attr_rows.sort_values("reference_period")
                latest = attr_rows.iloc[-1]
                entry: dict[str, Any] = {
                    "value": latest.get("value"),
                    "unit": latest.get("unit", ""),
                    "period": latest.get("reference_period", ""),
                    "marketing_year": latest_my,
                }
                if len(attr_rows) >= 2:
                    prev = attr_rows.iloc[-2]
                    if pd.notna(prev.get("value")) and pd.notna(latest.get("value")):
                        entry["revision"] = latest["value"] - prev["value"]
                        entry["prev_value"] = prev["value"]
                attrs[attribute] = entry
            if attrs:
                wasde_summary[commodity] = attrs

    # --- CONAB vs USDA ---
    conab_vs_usda = {}
    brazil = read_brazil_estimates()
    psd = read_psd()

    if not brazil.empty:
        soy_conab = brazil[
            (brazil["commodity"] == "Soybeans") & (brazil["attribute"] == "Production")
        ]
        if not soy_conab.empty:
            latest_year = soy_conab["crop_year"].max()
            conab_prod = soy_conab[soy_conab["crop_year"] == latest_year]["value"].iloc[0]
            conab_vs_usda["conab_production"] = conab_prod
            conab_vs_usda["crop_year"] = latest_year

            if not psd.empty:
                usda_brazil = psd[
                    (psd["commodity"] == "Soybeans") &
                    (psd["country"] == "Brazil") &
                    (psd["attribute"] == "Production")
                ]
                if not usda_brazil.empty:
                    usda_val = usda_brazil[usda_brazil["year"] == usda_brazil["year"].max()]["value"]
                    if not usda_val.empty:
                        conab_vs_usda["usda_production"] = usda_val.iloc[0]
                        conab_vs_usda["gap"] = conab_prod - usda_val.iloc[0]

    # --- PSD global highlights ---
    psd_highlights = []
    if not psd.empty:
        key_rows = [
            ("Soybeans", "Brazil", "Production"),
            ("Soybeans", "United States", "Production"),
            ("Soybeans", "Argentina", "Production"),
            ("Soybeans", "China", "Imports"),
            ("Soybeans", "China", "Crush"),
            ("Soybeans", "United States", "Ending Stocks"),
            ("Soybean Oil", "United States", "Production"),
            ("Soybean Meal", "United States", "Production"),
            # Argentina — #1 soymeal/oil exporter.
            ("Soybean Meal", "Argentina", "Production"),
            ("Soybean Meal", "Argentina", "Exports"),
            ("Soybean Oil", "Argentina", "Production"),
            ("Soybean Oil", "Argentina", "Exports"),
        ]
        latest_year = psd["year"].max()
        for commodity, country, attribute in key_rows:
            match = psd[
                (psd["commodity"] == commodity) &
                (psd["country"] == country) &
                (psd["attribute"] == attribute) &
                (psd["year"] == latest_year)
            ]
            if not match.empty:
                psd_highlights.append({
                    "commodity": commodity,
                    "country": country,
                    "attribute": attribute,
                    "value": match.iloc[0]["value"],
                    "unit": match.iloc[0].get("unit", "1000 MT"),
                    "year": latest_year,
                })

    # --- Stocks-to-use ratio (US, from PSD) ---
    stu_summary: dict[str, dict[str, Any]] = {}
    if not psd.empty:
        stu_df = compute_stocks_to_use(psd, country="United States")
        # WASDE row crops + the soy product balance sheets (PSD-only).
        wasde_psd_names = ("Soybeans", "Corn", "Wheat", "Cotton", "Soybean Meal", "Soybean Oil")
        tight = {
            s["commodity"]
            for s in detect_tight_supply(stu_df, commodities=list(wasde_psd_names))
        }
        for name in wasde_psd_names:
            rows = stu_df[stu_df["commodity"] == name].sort_values("year")
            if rows.empty:
                continue
            current = rows.iloc[-1]
            history = rows.iloc[-6:-1]  # prior 5 marketing years
            entry = {
                "marketing_year": int(current["year"]),
                "ending_stocks": float(current["ending_stocks"]),
                "total_use": float(current["total_use"]),
                "current_ratio": float(current["ratio"]),
                "is_tight": name in tight,
            }
            if len(history) >= 3:
                entry["prior_low"] = float(history["ratio"].min())
                entry["prior_high"] = float(history["ratio"].max())
            stu_summary[name] = entry

    # --- Crop progress ---
    crop_data = read_crop_progress()
    crop_summary: dict[str, Any] = {}
    if not crop_data.empty:
        soy_crop = crop_data[crop_data["commodity"] == "SOYBEANS"]
        if not soy_crop.empty:
            # Latest condition
            condition = soy_crop[soy_crop["stat_category"] == "CONDITION"]
            if not condition.empty:
                latest_week = condition["week_ending"].max()
                latest_cond = condition[condition["week_ending"] == latest_week]
                crop_summary["condition_week"] = _asof(latest_week)
                crop_summary["condition"] = []
                for _, row in latest_cond.iterrows():
                    desc = str(row.get("short_desc", ""))
                    val = row.get("Value", "")
                    if any(kw in desc.upper() for kw in ["GOOD", "EXCELLENT", "POOR", "VERY POOR"]):
                        crop_summary["condition"].append({"desc": desc, "value": val})

            # Latest progress
            progress = soy_crop[soy_crop["stat_category"] == "PROGRESS"]
            if not progress.empty:
                latest_week = progress["week_ending"].max()
                latest_prog = progress[progress["week_ending"] == latest_week]
                crop_summary["progress_week"] = _asof(latest_week)
                crop_summary["progress"] = []
                for _, row in latest_prog.iterrows():
                    desc = str(row.get("short_desc", ""))
                    val = row.get("Value", "")
                    if val:
                        crop_summary["progress"].append({"desc": desc, "value": val})

    return {
        "wasde": wasde_summary,
        "stocks_to_use": stu_summary,
        "conab_vs_usda": conab_vs_usda,
        "psd_highlights": psd_highlights,
        "crop_progress": crop_summary,
    }


# ---------------------------------------------------------------------------
# Analyst 3: Demand — who's buying, crushing, and burning soy
# ---------------------------------------------------------------------------

def demand_analysis() -> dict:
    """
    Build the demand-side picture: export sales, inspections, crush, biodiesel.

    Returns dict with:
        export_sales: latest weekly export sales for soy complex
        inspections: latest weekly actual shipments
        china_buying: China-specific demand signal
        biofuel: EIA biodiesel/ethanol data (soy oil demand driver)
        dce_prices: DCE Chinese futures vs CBOT
    """
    # --- Export sales ---
    es = read_export_sales()
    export_summary = {}
    china_buying = {}

    if not es.empty:
        for commodity in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
            subset = es[es["commodity"] == commodity]
            if subset.empty:
                continue

            latest_week = subset["week_ending"].max()
            week_data = subset[subset["week_ending"] == latest_week]

            total_net = week_data["net_sales"].sum() if "net_sales" in week_data.columns else 0
            total_exports = week_data["weekly_exports"].sum() if "weekly_exports" in week_data.columns else 0

            # Top 3 buyers
            top = week_data.nlargest(3, "net_sales") if "net_sales" in week_data.columns else pd.DataFrame()
            buyers = []
            for _, row in top.iterrows():
                if pd.notna(row.get("net_sales")) and row["net_sales"] != 0:
                    buyers.append({"country": row.get("country", ""), "mt": row["net_sales"]})

            export_summary[commodity] = {
                "week_ending": latest_week,
                "net_sales": total_net,
                "exports": total_exports,
                "top_buyers": buyers,
            }

            # China-specific
            china = week_data[week_data["country"].str.contains("China", case=False, na=False)]
            if not china.empty and "net_sales" in china.columns:
                china_net = china["net_sales"].sum()
                china_pct = (china_net / total_net * 100) if total_net > 0 else 0
                china_buying[commodity] = {
                    "net_sales": china_net,
                    "pct_of_total": china_pct,
                }

    # --- Inspections ---
    insp = read_inspections()
    inspection_summary = {}
    if not insp.empty:
        for commodity in ["Soybeans", "Corn", "Wheat"]:
            subset = insp[insp["commodity"] == commodity].sort_values("week_ending")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            inspection_summary[commodity] = {
                "week_ending": latest["week_ending"],
                "volume_mt": latest.get("inspections_mt", 0),
            }

    # --- NASS actual crush (monthly, soybeans processed into meal/oil) ---
    crush_summary: dict[str, Any] = {}
    try:
        crush = latest_crush()
        if crush:
            crush_summary = crush
    except Exception as exc:
        logger.debug("NASS crush summary failed: %s", exc)

    # --- Biofuel (EIA) ---
    eia = read_eia_data()
    biofuel = {}
    if not eia.empty:
        for series in ["Ethanol Production", "Biodiesel Production", "Diesel Retail Price"]:
            subset = eia[eia["series_name"] == series].sort_values("Date")
            if len(subset) >= 2:
                latest = subset.iloc[-1]
                prev = subset.iloc[-2]
                chg = 0
                if pd.notna(prev["value"]) and prev["value"] != 0:
                    chg = ((latest["value"] - prev["value"]) / prev["value"]) * 100
                biofuel[series] = {
                    "value": latest["value"],
                    "unit": latest.get("unit", ""),
                    "date": latest["Date"],
                    "chg_pct": chg,
                }

    # --- DCE vs CBOT ---
    dce = read_dce_futures()
    dce_comparison = {}
    prices = _load_soy_prices()

    dce_map = {
        "DCE Soybean": "Soybeans",
        "DCE Soybean Meal": "Soybean Meal",
        "DCE Soybean Oil": "Soybean Oil",
    }

    if not dce.empty:
        for dce_name, cbot_name in dce_map.items():
            dce_sub = dce[dce["commodity"] == dce_name].sort_values("Date")
            if dce_sub.empty:
                continue
            latest_dce = dce_sub.iloc[-1]
            entry = {"dce_close": latest_dce["Close"], "dce_date": latest_dce["Date"]}
            if cbot_name in prices and not prices[cbot_name].empty:
                entry["cbot_close"] = prices[cbot_name]["Close"].iloc[-1]
            dce_comparison[dce_name] = entry

    return {
        "export_sales": export_summary,
        "inspections": inspection_summary,
        "china_buying": china_buying,
        "crush": crush_summary,
        "biofuel": biofuel,
        "dce_comparison": dce_comparison,
    }


# ---------------------------------------------------------------------------
# Analyst 4: Technicals — price action on all 3 legs
# ---------------------------------------------------------------------------

def technicals_analysis() -> dict:
    """
    Full technical analysis for all 3 soy legs.

    Returns dict with:
        per_leg: dict of DataFrames with full technicals computed
        signals: list of all detected signals
    """
    prices = _load_soy_prices()

    per_leg = {}
    per_leg_mt = {}
    all_signals = []

    for leg in SOY_LEGS:
        df = prices.get(leg)
        if df is None or df.empty:
            continue
        per_leg[leg] = df
        per_leg_mt[leg] = convert_df_to_mt(df, leg)
        signals = detect_all_signals(df, leg)
        all_signals.extend(signals)

    all_signals = demote_near_roll_signals(all_signals)
    severity_order = {"alert": 0, "warning": 1, "info": 2}
    all_signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    return {
        "per_leg": per_leg,
        "per_leg_mt": per_leg_mt,
        "signals": all_signals,
    }


# ---------------------------------------------------------------------------
# Analyst 5: Relative Value — inter-leg and cross-commodity
# ---------------------------------------------------------------------------

def relative_value_analysis() -> dict:
    """
    Relative value analysis: crush margin, oil/meal ratio, soy oil vs palm oil.

    Returns dict with:
        crush: full crush spread DataFrame + current value
        basis: dict with keys primary (str), sources (per-source stats dict),
            and wedge_usd_mt (Paranaguá FOB − CEPEA Paraná in USD/MT, None if
            only one source is available).
        oil_meal_ratio: soy oil / soy meal price ratio (tracks protein vs oil demand)
        oil_vs_palm: soybean oil vs palm oil comparison
        bean_corn_ratio: soybean/corn ratio (acreage competition signal)
        soy_oil_share: soy oil as % of total crush value
    """
    prices = _load_soy_prices()
    currencies = _load_currency_data()

    beans = prices.get("Soybeans")
    oil = prices.get("Soybean Oil")
    meal = prices.get("Soybean Meal")
    palm = prices.get("Palm Oil (CME)")
    corn = prices.get("Corn")

    result: dict[str, Any] = {}

    # --- Crush spread ---
    if (
        beans is not None and oil is not None and meal is not None
        and not beans.empty and not oil.empty and not meal.empty
    ):
        try:
            spread = compute_crush_spread(beans, oil, meal)
            if not spread.empty:
                # Convert crush spread (cents/bu) to USD/MT
                crush_mt = spread["crush_spread"].apply(lambda x: to_metric_tons(x, "Soybeans"))
                last_252 = crush_mt.iloc[-252:] if len(crush_mt) >= 252 else crush_mt
                result["crush"] = {
                    "series": spread,
                    "as_of": _asof(spread.iloc[-1].get("Date")),
                    "current_usd_mt": crush_mt.iloc[-1],
                    "current_dollars_bu": spread.iloc[-1]["crush_spread"] / 100,
                    "profitable": spread.iloc[-1]["crush_spread"] > 0,
                    "avg_1y": last_252.mean(),
                    "min_1y": last_252.min(),
                    "max_1y": last_252.max(),
                }
        except Exception:
            logger.warning("Relative-value crush computation failed", exc_info=True)

    # --- Brazil basis: AgRural Paranaguá FOB (primary) + CEPEA Paraná (secondary) ---
    # Paranaguá port FOB is the export-relevant number; CEPEA Paraná farm-gate is the
    # widely-cited reference. When both are available, the difference reveals how much
    # margin the export chain is capturing between farm and port.
    brl_usd = currencies.get("BRL/USD")
    if (
        beans is not None and not beans.empty
        and brl_usd is not None and not brl_usd.empty
    ):
        sources: dict[str, dict[str, Any]] = {}
        for label, commodity in (
            ("Paranaguá FOB", "Soybean (AgRural Paranaguá FOB)"),
            ("CEPEA Paraná", "Soybean (CEPEA)"),
        ):
            try:
                spot = read_brazil_spot(commodity)
            except Exception as exc:
                logger.debug("Brazil spot read failed for %s: %s", commodity, exc)
                continue
            if spot.empty:
                continue
            try:
                basis_df = compute_brazil_basis(beans, spot, brl_usd)
            except Exception as exc:
                logger.debug("Brazil basis computation failed for %s: %s", commodity, exc)
                continue
            if basis_df.empty:
                continue
            basis_series = basis_df["basis_usd_mt"]
            last_252 = basis_series.iloc[-252:] if len(basis_series) >= 252 else basis_series
            current = float(basis_series.iloc[-1])
            entry: dict[str, Any] = {
                "series": basis_df,
                "as_of": _asof(basis_df["Date"].iloc[-1]),
                "current_usd_mt": current,
                "direction": "discount" if current < 0 else "premium",
                "n_obs": int(len(last_252)),
                "avg_1y": None,
                "min_1y": None,
                "max_1y": None,
                "percentile_1y": None,
            }
            # Distributional stats need real history — computed over a
            # handful of rows they read as context ("1Y avg", "0th pctile")
            # while actually restating the current print.
            if len(last_252) >= _BASIS_STATS_MIN_OBS:
                rank_count = (last_252 < current).sum()
                entry["avg_1y"] = float(last_252.mean())
                entry["min_1y"] = float(last_252.min())
                entry["max_1y"] = float(last_252.max())
                entry["percentile_1y"] = float(rank_count / len(last_252) * 100)
            sources[label] = entry

        if sources:
            primary = "Paranaguá FOB" if "Paranaguá FOB" in sources else "CEPEA Paraná"
            wedge = None
            if "Paranaguá FOB" in sources and "CEPEA Paraná" in sources:
                wedge = (
                    sources["Paranaguá FOB"]["current_usd_mt"]
                    - sources["CEPEA Paraná"]["current_usd_mt"]
                )
            result["basis"] = {
                "primary": primary,
                "sources": sources,
                "wedge_usd_mt": wedge,
            }

    # --- Oil/Meal ratio ---
    if oil is not None and meal is not None and not oil.empty and not meal.empty:
        combined = pd.DataFrame({
            "oil": oil["Close"],
            "meal": meal["Close"],
        }).dropna()
        if not combined.empty:
            combined["ratio"] = combined["oil"] / combined["meal"]
            last_252 = combined["ratio"].iloc[-252:] if len(combined) >= 252 else combined["ratio"]
            result["oil_meal_ratio"] = {
                "series": combined["ratio"],
                "as_of": _asof(combined.index[-1]),
                "current": combined["ratio"].iloc[-1],
                "avg_60d": combined["ratio"].iloc[-60:].mean() if len(combined) >= 60 else combined["ratio"].mean(),
                "min_1y": last_252.min(),
                "max_1y": last_252.max(),
            }

    # --- Soy oil vs Palm oil ---
    if oil is not None and palm is not None and not oil.empty and not palm.empty:
        oil_latest = oil["Close"].iloc[-1]
        palm_latest = palm["Close"].iloc[-1]
        result["oil_vs_palm"] = {
            "soy_oil": to_metric_tons(oil_latest, "Soybean Oil"),
            "soy_oil_unit": mt_label("Soybean Oil"),
            "soy_oil_as_of": _asof(oil.index[-1]),
            "palm_oil": to_metric_tons(palm_latest, "Palm Oil (CME)"),
            "palm_oil_unit": mt_label("Palm Oil (CME)"),
            "palm_oil_as_of": _asof(palm.index[-1]),
        }
        if len(oil) >= 6 and len(palm) >= 6:
            result["oil_vs_palm"]["soy_oil_weekly_chg"] = (
                (oil["Close"].iloc[-1] - oil["Close"].iloc[-6]) / oil["Close"].iloc[-6]
            ) * 100
            result["oil_vs_palm"]["palm_oil_weekly_chg"] = (
                (palm["Close"].iloc[-1] - palm["Close"].iloc[-6]) / palm["Close"].iloc[-6]
            ) * 100

    # --- Bean/Corn ratio ---
    if beans is not None and corn is not None and not beans.empty and not corn.empty:
        combined = pd.DataFrame({
            "beans": beans["Close"],
            "corn": corn["Close"],
        }).dropna()
        if not combined.empty:
            combined["ratio"] = combined["beans"] / combined["corn"]
            last_252 = combined["ratio"].iloc[-252:] if len(combined) >= 252 else combined["ratio"]
            result["bean_corn_ratio"] = {
                "series": combined["ratio"],
                "as_of": _asof(combined.index[-1]),
                "current": combined["ratio"].iloc[-1],
                "avg_1y": last_252.mean(),
                "min_1y": last_252.min(),
                "max_1y": last_252.max(),
            }

    # --- Soy oil share of crush value ---
    if (
        oil is not None and meal is not None and beans is not None
        and not oil.empty and not meal.empty
    ):
        oil_val = oil["Close"].iloc[-1] * CRUSH_OIL_FACTOR
        meal_val = meal["Close"].iloc[-1] * CRUSH_MEAL_FACTOR
        total_product = oil_val + meal_val
        if total_product > 0:
            result["soy_oil_share"] = (oil_val / total_product) * 100
            result["soy_oil_share_as_of"] = _asof(
                min(oil.index[-1], meal.index[-1])
            )

    return result


# ---------------------------------------------------------------------------
# Analyst 6: Risk Monitor — threats and positioning
# ---------------------------------------------------------------------------

def risk_analysis() -> dict:
    """
    Risk factors: BRL/USD, COT extremes, weather threats.

    Returns dict with:
        currencies: BRL, CNY, ARS latest + changes
        cot: COT positioning for soy complex
        weather_alerts: active weather threats in soy regions
    """
    # --- Currencies ---
    currencies_data = _load_currency_data()
    currency_summary = {}
    for pair, df in currencies_data.items():
        if df.empty:
            continue
        latest = df.iloc[-1]
        entry = {"close": latest["Close"], "as_of": _asof(df.index[-1])}
        if len(df) >= 6:
            entry["weekly_chg"] = (
                (df["Close"].iloc[-1] - df["Close"].iloc[-6]) / df["Close"].iloc[-6]
            ) * 100
        if len(df) >= 22:
            entry["monthly_chg"] = (
                (df["Close"].iloc[-1] - df["Close"].iloc[-22]) / df["Close"].iloc[-22]
            ) * 100
        currency_summary[pair] = entry

    # --- COT ---
    cot = read_cot()
    cot_summary = {}
    if not cot.empty:
        for leg in SOY_LEGS:
            subset = cot[cot["commodity"] == leg].sort_values("Date")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            entry = {
                "date": latest["Date"],
                "commercial_net": latest.get("commercial_net"),
                "spec_net": latest.get("noncommercial_net"),
                "total_oi": latest.get("total_open_interest"),
            }
            # Week-over-week change in spec positioning
            if len(subset) >= 2:
                prev = subset.iloc[-2]
                if pd.notna(latest.get("noncommercial_net")) and pd.notna(prev.get("noncommercial_net")):
                    entry["spec_net_chg"] = latest["noncommercial_net"] - prev["noncommercial_net"]
            cot_summary[leg] = entry

    # --- Weather ---
    weather = read_weather()
    weather_alerts = []
    if not weather.empty:
        for region in SOY_WEATHER_REGIONS:
            subset = weather[weather["region"] == region].sort_values("Date")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            precip = latest.get("precipitation", 0)
            temp_max = latest.get("temp_max")
            temp_min = latest.get("temp_min")

            # Heat outranks rain conditions; elif chain keeps one alert per
            # region without a later check silently overwriting an earlier one.
            alert_type = None
            if pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C:
                alert_type = "Extreme Heat"
            elif pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM:
                alert_type = "Heavy Rain"
            elif pd.notna(precip) and precip < WEATHER_DRY_THRESHOLD_MM:
                alert_type = "Dry"

            entry = {
                "region": region,
                "temp_max": temp_max,
                "temp_min": temp_min,
                "precip": precip,
                "date": latest["Date"],
                "alert": alert_type,
            }
            if alert_type:
                weather_alerts.append(entry)

    return {
        "currencies": currency_summary,
        "cot": cot_summary,
        "weather_alerts": weather_alerts,
    }


# ---------------------------------------------------------------------------
# Analyst 7: Seasonal — where are we vs history
# ---------------------------------------------------------------------------

def seasonal_analysis() -> dict:
    """
    Seasonal patterns for all 3 soy legs.

    Returns dict per leg with:
        monthly_avg: DataFrame of avg/min/max by month
        current_vs_avg: deviation from seasonal norm
    """
    prices = _load_soy_prices()
    result = {}

    for leg in SOY_LEGS:
        df = prices.get(leg)
        if df is None or df.empty:
            continue

        # Compute seasonal on MT-converted data
        df_mt = convert_df_to_mt(df, leg)
        monthly = monthly_seasonal(df_mt)
        vs_seasonal = current_vs_seasonal(df_mt)

        result[leg] = {
            "monthly": monthly,
            "vs_seasonal": vs_seasonal,
            "unit": mt_label(leg),
            "as_of": _asof(df.index[-1]),
        }

    return result


# ---------------------------------------------------------------------------
# Analyst 8: Forward Curve — term structure for soy complex
# ---------------------------------------------------------------------------

def forward_curve_analysis() -> dict:
    """
    Forward curve analysis for all 3 soy legs.

    Returns dict per leg with:
        curve_data: raw forward curve DataFrame
        analysis: contango/backwardation assessment
        calendar_spreads: front-month spreads
    """
    fc = read_forward_curve()
    result: dict[str, Any] = {}

    if fc.empty:
        return result

    for leg in SOY_LEGS:
        subset = fc[fc["commodity"] == leg].sort_values("contract_month")
        if len(subset) < 2:
            continue

        curve_analysis = analyze_curve(subset)
        cal_spread = calendar_spread(subset, 0, 1) if len(subset) >= 2 else {}

        # Convert forward curve prices to USD/MT
        subset_mt = subset.copy()
        if "close" in subset_mt.columns:
            from pipeline.units import CONVERSION_FACTORS
            factor = CONVERSION_FACTORS.get(leg)
            if factor:
                subset_mt["close"] = subset_mt["close"] * factor

        result[leg] = {
            "curve_data": subset,
            "curve_data_mt": subset_mt,
            "analysis": curve_analysis,
            "calendar_spread": cal_spread,
            "unit": mt_label(leg),
            "as_of": _asof(subset["fetched_date"].max())
            if "fetched_date" in subset.columns else None,
        }

    return result


# ---------------------------------------------------------------------------
# Analyst 9: Emerging Markets — SA, India, Nigeria deep dive
# ---------------------------------------------------------------------------

def _latest_aligned_usd(
    domestic_df: pd.DataFrame | None,
    fx_df: pd.DataFrame | None,
    price_col: str = "Close",
) -> tuple[float, pd.Timestamp] | None:
    """Latest date-aligned local→USD conversion.

    Inner-joins the domestic price series with the FX series on Date and
    returns (usd_per_mt, date) for the last common row, or None. Used as
    the display fallback when the full three-leg basis join (which also
    needs CBOT) has no common date.
    """
    if domestic_df is None or fx_df is None or domestic_df.empty or fx_df.empty:
        return None
    dom = domestic_df.copy()
    if "Date" in dom.columns:
        dom["Date"] = pd.to_datetime(dom["Date"])
        dom = dom.set_index("Date")
    joined = pd.DataFrame({
        "local": pd.to_numeric(dom[price_col], errors="coerce").dropna(),
        "fx": pd.to_numeric(fx_df["Close"], errors="coerce").dropna(),
    }).dropna()
    joined = joined[joined["fx"] > 0]
    if joined.empty:
        return None
    last = joined.iloc[-1]
    return float(last["local"] * last["fx"]), joined.index[-1]


def _latest_aligned_basis(
    soybeans_df: pd.DataFrame | None,
    domestic_df: pd.DataFrame | None,
    fx_df: pd.DataFrame | None,
    price_col: str = "Close",
) -> pd.Series | None:
    """Last row of the date-aligned domestic-vs-CBOT basis, or None.

    All emerging-markets basis math routes through here (F1): the three
    legs — CBOT close, domestic price, FX — are inner-joined on Date by
    `compute_domestic_basis`, the same engine behind the briefing's
    `compute_brazil_basis`, so the briefing and the EM card can never
    print different basis numbers for the same day.
    """
    if soybeans_df is None or domestic_df is None or fx_df is None:
        return None
    if soybeans_df.empty or domestic_df.empty or fx_df.empty:
        return None
    basis_df = compute_domestic_basis(
        soybeans_df, domestic_df, fx_df, price_col=price_col
    )
    if basis_df.empty:
        return None
    return basis_df.iloc[-1]


def emerging_markets_analysis() -> dict:
    """
    Deep dive on emerging soybean markets: South Africa, India, Nigeria.

    Returns dict with:
        countries: dict per country with PSD production/stocks/trade,
                   currency trends, and weather alerts
    """
    psd = read_psd()
    weather = read_weather()
    # Shared cached loaders (Date-indexed, sorted) — one DB read for the
    # whole function instead of one per country block.
    prices = load_prices()
    currencies = load_currencies()

    countries: dict[str, dict[str, Any]] = {}

    for country in EMERGING_MARKET_COUNTRIES:
        entry: dict[str, Any] = {"name": country}

        # --- PSD production data ---
        if not psd.empty:
            country_psd = psd[
                (psd["commodity"] == "Soybeans") & (psd["country"] == country)
            ]
            if not country_psd.empty:
                latest_year = country_psd["year"].max()
                latest = country_psd[country_psd["year"] == latest_year]

                psd_data: dict[str, dict[str, Any]] = {}
                for _, row in latest.iterrows():
                    attr = row.get("attribute", "")
                    val = row.get("value")
                    if pd.notna(val):
                        psd_data[attr] = {
                            "value": val,
                            "unit": row.get("unit", "1000 MT"),
                        }

                # YoY comparison
                if latest_year > 0:
                    prev_year = latest_year - 1
                    prev = country_psd[country_psd["year"] == prev_year]
                    for attr in psd_data:
                        prev_match = prev[prev["attribute"] == attr]
                        if not prev_match.empty:
                            prev_val = prev_match.iloc[0].get("value")
                            if pd.notna(prev_val) and prev_val != 0:
                                psd_data[attr]["yoy_pct"] = (
                                    (psd_data[attr]["value"] - prev_val) / prev_val
                                ) * 100

                entry["psd"] = psd_data
                entry["psd_year"] = latest_year

        # --- Currency ---
        pair = EMERGING_MARKET_CURRENCIES.get(country)
        pair_df = currencies.get(pair) if pair else None
        if pair_df is not None and not pair_df.empty:
            currency_info: dict[str, Any] = {"pair": pair, "close": pair_df["Close"].iloc[-1]}
            weekly = _pct_chg(pair_df["Close"], _WEEKLY_SESSIONS)
            if weekly is not None:
                currency_info["weekly_chg"] = weekly
            monthly = _pct_chg(pair_df["Close"], _MONTHLY_SESSIONS)
            if monthly is not None:
                currency_info["monthly_chg"] = monthly
            entry["currency"] = currency_info

        # --- Weather ---
        regions = EMERGING_MARKET_WEATHER.get(country, [])
        weather_alerts: list[dict[str, Any]] = []
        if not weather.empty:
            for region in regions:
                w_subset = weather[weather["region"] == region].sort_values("Date")
                if w_subset.empty:
                    continue
                w_latest = w_subset.iloc[-1]
                precip = w_latest.get("precipitation", 0)
                temp_max = w_latest.get("temp_max")

                alert_type = None
                if pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C:
                    alert_type = "Extreme Heat"
                elif pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM:
                    alert_type = "Heavy Rain"
                elif pd.notna(precip) and precip < WEATHER_DRY_THRESHOLD_MM:
                    alert_type = "Dry"

                weather_entry = {
                    "region": region,
                    "temp_max": temp_max,
                    "temp_min": w_latest.get("temp_min"),
                    "precip": precip,
                    "date": w_latest["Date"],
                    "alert": alert_type,
                }
                weather_alerts.append(weather_entry)

        entry["weather"] = weather_alerts

        # --- India mandi domestic bean price + CBOT bean premium ---
        # Bean-only since the 2026-08 Layer 16 rebuild: the mandi source
        # carries no meal and unreliable oil, so the old NCDEX crush margin
        # and crush premium retired with the exchange (SEBI suspension).
        if country == "India":
            india_domestic_entry: dict[str, Any] = {}
            try:
                inr_df = read_india_domestic(MANDI_SERIES)
                inr_rows = currencies.get("INR/USD")

                if not inr_df.empty:
                    mandi_rows = inr_df.sort_values("Date")
                    latest_close = float(mandi_rows["Close"].iloc[-1])
                    india_domestic_entry["soybean_mandi_inr"] = latest_close
                    india_domestic_entry["soybean_mandi_date"] = _asof(
                        mandi_rows["Date"].iloc[-1]
                    )

                    # India bean vs CBOT bean premium (USD/MT) — mandi,
                    # INR/USD and CBOT joined on a common date (F1).
                    soy_rows = prices.get("Soybeans", pd.DataFrame())
                    basis_row = _latest_aligned_basis(soy_rows, mandi_rows, inr_rows)
                    if basis_row is not None:
                        india_domestic_entry["soybean_mandi_usd"] = round(
                            float(basis_row["domestic_usd_mt"]), 2
                        )
                        india_domestic_entry["cbot_bean_usd"] = round(
                            float(basis_row["cbot_usd_mt"]), 2
                        )
                        india_domestic_entry["bean_premium_usd"] = round(
                            float(basis_row["basis_usd_mt"]), 2
                        )
                        india_domestic_entry["basis_date"] = str(
                            pd.Timestamp(basis_row["Date"]).date()
                        )
                    else:
                        aligned = _latest_aligned_usd(mandi_rows, inr_rows)
                        if aligned is not None:
                            india_domestic_entry["soybean_mandi_usd"] = round(
                                aligned[0], 2
                            )

                    weekly = _pct_chg(mandi_rows["Close"], _WEEKLY_SESSIONS)
                    if weekly is not None:
                        india_domestic_entry["weekly_chg_pct"] = round(weekly, 2)

                # Maharashtra — #1 producing state since 2025-26; secondary
                # series alongside the MP (Indore hub) headline benchmark.
                mh_df = read_india_domestic(MANDI_SERIES_MH)
                if not mh_df.empty:
                    mh_rows = mh_df.sort_values("Date")
                    india_domestic_entry["soybean_mandi_mh_inr"] = float(
                        mh_rows["Close"].iloc[-1]
                    )
                    india_domestic_entry["soybean_mandi_mh_date"] = _asof(
                        mh_rows["Date"].iloc[-1]
                    )
                    aligned = _latest_aligned_usd(mh_rows, inr_rows)
                    if aligned is not None:
                        india_domestic_entry["soybean_mandi_mh_usd"] = round(
                            aligned[0], 2
                        )

            except Exception as exc:
                logger.warning("India domestic analytics failed: %s", exc)

            if india_domestic_entry:
                entry["india_domestic"] = india_domestic_entry

        # --- Brazil CEPEA domestic price + CBOT basis ---
        if country == "Brazil":
            brazil_domestic_entry: dict[str, Any] = {}
            try:
                brl_df = read_brazil_spot()
                brl_rows = currencies.get("BRL/USD")
                soy_rows = prices.get("Soybeans", pd.DataFrame())

                if not brl_df.empty:
                    cepea_rows = brl_df[brl_df["commodity"] == "Soybean (CEPEA)"].sort_values("Date")
                    if not cepea_rows.empty:
                        latest_brl = float(cepea_rows["price_brl"].iloc[-1])
                        brazil_domestic_entry["cepea_soy_brl"] = round(latest_brl, 2)
                        brazil_domestic_entry["cepea_soy_date"] = _asof(
                            cepea_rows["Date"].iloc[-1]
                        )

                        # CEPEA vs CBOT — three legs joined on a common
                        # date (F1). Same engine as the briefing's BRAZIL
                        # BASIS section (`compute_brazil_basis` delegates
                        # to it), so the two pages cannot print different
                        # basis numbers.
                        basis_row = _latest_aligned_basis(
                            soy_rows, cepea_rows, brl_rows, price_col="price_brl"
                        )
                        if basis_row is not None:
                            brazil_domestic_entry["cepea_soy_usd"] = round(
                                float(basis_row["domestic_usd_mt"]), 2
                            )
                            brazil_domestic_entry["cbot_usd"] = round(
                                float(basis_row["cbot_usd_mt"]), 2
                            )
                            brazil_domestic_entry["brazil_cbot_basis_usd"] = round(
                                float(basis_row["basis_usd_mt"]), 2
                            )
                            brazil_domestic_entry["basis_date"] = str(
                                pd.Timestamp(basis_row["Date"]).date()
                            )
                        else:
                            aligned = _latest_aligned_usd(
                                cepea_rows, brl_rows, price_col="price_brl"
                            )
                            if aligned is not None:
                                brazil_domestic_entry["cepea_soy_usd"] = round(
                                    aligned[0], 2
                                )

                        # Weekly % change
                        weekly = _pct_chg(cepea_rows["price_brl"], _WEEKLY_SESSIONS)
                        if weekly is not None:
                            brazil_domestic_entry["weekly_chg_pct"] = round(weekly, 2)

                    # CONAB weekly PR farmgate — sanity cross-check for the
                    # CEPEA wholesale indicator; a ~10-14% wholesale premium
                    # over farmgate is the expected band.
                    farmgate_rows = brl_df[
                        brl_df["commodity"] == CONAB_FARMGATE_SERIES
                    ].sort_values("Date")
                    if not farmgate_rows.empty:
                        farmgate_brl = float(farmgate_rows["price_brl"].iloc[-1])
                        brazil_domestic_entry["conab_farmgate_brl"] = round(farmgate_brl, 2)
                        brazil_domestic_entry["conab_farmgate_date"] = _asof(
                            farmgate_rows["Date"].iloc[-1]
                        )
                        cepea_brl = brazil_domestic_entry.get("cepea_soy_brl")
                        if cepea_brl and farmgate_brl > 0:
                            brazil_domestic_entry["cepea_vs_farmgate_pct"] = round(
                                (cepea_brl - farmgate_brl) / farmgate_brl * 100, 1
                            )

                    # AgRural Paranaguá FOB — USD/MT only (raw BRL is not publishable
                    # under AgRural redistribution rules; the derived USD basis is).
                    agrural_rows = brl_df[
                        brl_df["commodity"] == "Soybean (AgRural Paranaguá FOB)"
                    ].sort_values("Date")
                    if not agrural_rows.empty:
                        # AgRural vs CBOT — same date-aligned join (F1).
                        agrural_row = _latest_aligned_basis(
                            soy_rows, agrural_rows, brl_rows, price_col="price_brl"
                        )
                        if agrural_row is not None:
                            brazil_domestic_entry["agrural_soy_usd"] = round(
                                float(agrural_row["domestic_usd_mt"]), 2
                            )
                            brazil_domestic_entry["agrural_cbot_basis_usd"] = round(
                                float(agrural_row["basis_usd_mt"]), 2
                            )
                            brazil_domestic_entry["agrural_basis_date"] = str(
                                pd.Timestamp(agrural_row["Date"]).date()
                            )
                        else:
                            aligned = _latest_aligned_usd(
                                agrural_rows, brl_rows, price_col="price_brl"
                            )
                            if aligned is not None:
                                brazil_domestic_entry["agrural_soy_usd"] = round(
                                    aligned[0], 2
                                )

            except Exception as exc:
                logger.warning("Brazil domestic analytics failed: %s", exc)

            if brazil_domestic_entry:
                entry["brazil_domestic"] = brazil_domestic_entry

        # --- South Africa SAFEX domestic price + CBOT basis ---
        if country == "South Africa":
            sa_domestic_entry: dict[str, Any] = {}
            try:
                safex_df = read_safex()
                zar_rows = currencies.get("ZAR/USD")
                cbot_rows = prices.get("Soybeans", pd.DataFrame())

                if not safex_df.empty:
                    soy_rows = safex_df[safex_df["commodity"] == "Soybean (SAFEX)"].sort_values("Date")
                    sun_rows = safex_df[safex_df["commodity"] == "Sunflower (SAFEX)"].sort_values("Date")

                    if not soy_rows.empty:
                        latest_zar = float(soy_rows["Close"].iloc[-1])
                        sa_domestic_entry["soybean_safex_zar"] = round(latest_zar, 2)
                        sa_domestic_entry["soybean_safex_date"] = _asof(
                            soy_rows["Date"].iloc[-1]
                        )

                        # SAFEX vs CBOT — SAFEX, ZAR/USD and CBOT joined
                        # on a common date (F1).
                        basis_row = _latest_aligned_basis(cbot_rows, soy_rows, zar_rows)
                        if basis_row is not None:
                            sa_domestic_entry["soybean_safex_usd"] = round(
                                float(basis_row["domestic_usd_mt"]), 2
                            )
                            sa_domestic_entry["cbot_usd"] = round(
                                float(basis_row["cbot_usd_mt"]), 2
                            )
                            sa_domestic_entry["safex_cbot_basis_usd"] = round(
                                float(basis_row["basis_usd_mt"]), 2
                            )
                            sa_domestic_entry["basis_date"] = str(
                                pd.Timestamp(basis_row["Date"]).date()
                            )
                        else:
                            aligned = _latest_aligned_usd(soy_rows, zar_rows)
                            if aligned is not None:
                                sa_domestic_entry["soybean_safex_usd"] = round(
                                    aligned[0], 2
                                )

                        weekly = _pct_chg(soy_rows["Close"], _WEEKLY_SESSIONS)
                        if weekly is not None:
                            sa_domestic_entry["weekly_chg_pct"] = round(weekly, 2)

                    if not sun_rows.empty:
                        sa_domestic_entry["sunflower_safex_zar"] = round(
                            float(sun_rows["Close"].iloc[-1]), 2
                        )
                        sa_domestic_entry["sunflower_safex_date"] = _asof(
                            sun_rows["Date"].iloc[-1]
                        )

            except Exception as exc:
                logger.warning("South Africa SAFEX analytics failed: %s", exc)

            if sa_domestic_entry:
                entry["south_africa_domestic"] = sa_domestic_entry

        countries[country] = entry

    return {"countries": countries}
