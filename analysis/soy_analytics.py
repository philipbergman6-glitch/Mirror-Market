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
    6. Risk Analyst      — BRL/USD, COT crowding, weather threats, options sentiment
    7. Seasonal Analyst  — current vs historical norms for all 3 legs
    8. Forward Curve     — term structure for all 3 soy contracts

Key concepts for learning:
    - Each analyst returns a dict of structured data (not display strings)
    - The dashboard renders the data; the analyst just computes it
    - This separation means you could use the same analysts for
      email reports, Slack bots, or algorithmic signals
"""

import logging
from datetime import datetime

import pandas as pd

from processing.combiner import (
    read_prices,
    read_cot,
    read_currencies,
    read_weather,
    read_economic,
    read_usda,
    read_wasde,
    read_crop_progress,
    read_export_sales,
    read_inspections,
    read_eia_data,
    read_brazil_estimates,
    read_options_sentiment,
    read_psd,
    read_forward_curve,
    read_dce_futures,
    read_freshness,
)
from analysis.technical import compute_all_technicals
from analysis.spreads import compute_crush_spread
from analysis.signals import detect_all_signals
from analysis.correlations import commodity_vs_currency, rolling_correlation
from analysis.seasonal import monthly_seasonal, current_vs_seasonal
from analysis.forward_curve import analyze_curve, calendar_spread

logger = logging.getLogger(__name__)

# The 3 soy legs — everything in this module focuses on these
SOY_LEGS = ["Soybeans", "Soybean Oil", "Soybean Meal"]

# Key growing regions for soy
SOY_WEATHER_REGIONS = [
    "US Midwest (Iowa)", "US Illinois",
    "Brazil Mato Grosso", "Brazil Parana",
    "Argentina Pampas", "Argentina Cordoba",
    "Paraguay Chaco",
    "India Madhya Pradesh", "India Maharashtra",
    "China Heilongjiang",
]

# Key currencies for soy trade
SOY_CURRENCIES = ["BRL/USD", "CNY/USD", "ARS/USD"]


# ---------------------------------------------------------------------------
# Helper: load soy price data with technicals
# ---------------------------------------------------------------------------

def _load_soy_prices() -> dict[str, pd.DataFrame]:
    """Load price data for all 3 soy legs + Palm Oil, with technicals computed."""
    all_prices = read_prices()
    result = {}

    targets = SOY_LEGS + ["Palm Oil (BMD)", "Corn"]

    if not all_prices.empty:
        for commodity in targets:
            subset = all_prices[all_prices["commodity"] == commodity].copy()
            if subset.empty:
                continue
            subset["Date"] = pd.to_datetime(subset["Date"])
            subset = subset.set_index("Date").sort_index()
            subset = compute_all_technicals(subset)
            result[commodity] = subset

    return result


def _load_currency_data() -> dict[str, pd.DataFrame]:
    """Load currency data relevant to soy trade."""
    all_currencies = read_currencies()
    result = {}

    if not all_currencies.empty:
        for pair in SOY_CURRENCIES:
            subset = all_currencies[all_currencies["pair"] == pair].copy()
            if subset.empty:
                continue
            subset["Date"] = pd.to_datetime(subset["Date"])
            subset = subset.set_index("Date").sort_index()
            result[pair] = subset

    return result


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
        prev = df.iloc[-2] if len(df) >= 2 else latest

        leg_info = {
            "name": leg,
            "available": True,
            "close": latest["Close"],
            "daily_chg": latest.get("daily_pct_change", 0),
            "weekly_chg": latest.get("weekly_pct_change", 0),
            "rsi": latest.get("RSI"),
            "macd_hist": latest.get("MACD_Histogram"),
            "ma_50": latest.get("MA_50"),
            "ma_200": latest.get("MA_200"),
            "hv_20": latest.get("HV_20"),
            "bb_upper": latest.get("BB_Upper"),
            "bb_lower": latest.get("BB_Lower"),
            "volume": latest.get("Volume"),
        }

        # Trend assessment
        if pd.notna(leg_info["ma_200"]):
            leg_info["trend"] = "Bullish" if latest["Close"] > leg_info["ma_200"] else "Bearish"
        elif pd.notna(leg_info["ma_50"]):
            leg_info["trend"] = "Bullish" if latest["Close"] > leg_info["ma_50"] else "Bearish"
        else:
            leg_info["trend"] = "N/A"

        legs.append(leg_info)

        # Collect signals for this leg
        signals = detect_all_signals(df, leg)
        all_signals.extend(signals)

    # --- Crush spread ---
    crush_info = {"available": False}
    beans = prices.get("Soybeans")
    oil = prices.get("Soybean Oil")
    meal = prices.get("Soybean Meal")

    if beans is not None and oil is not None and meal is not None:
        if not beans.empty and not oil.empty and not meal.empty:
            try:
                spread = compute_crush_spread(beans, oil, meal)
                if not spread.empty:
                    latest_cents = spread.iloc[-1]["crush_spread"]
                    crush_info = {
                        "available": True,
                        "value_dollars": latest_cents / 100,
                        "profitable": latest_cents > 0,
                        "spread_series": spread,
                    }
                    if len(spread) >= 6:
                        prev_cents = spread.iloc[-6]["crush_spread"]
                        crush_info["trend"] = "widening" if latest_cents > prev_cents else "narrowing"
                        crush_info["weekly_chg"] = latest_cents - prev_cents
            except Exception:
                pass

    # --- Key metrics ---
    key_metrics = {}

    # BRL/USD
    if "BRL/USD" in currencies and not currencies["BRL/USD"].empty:
        brl = currencies["BRL/USD"]
        key_metrics["brl_usd"] = brl["Close"].iloc[-1]
        if len(brl) >= 6:
            key_metrics["brl_weekly_chg"] = (
                (brl["Close"].iloc[-1] - brl["Close"].iloc[-6]) / brl["Close"].iloc[-6]
            ) * 100

    # CNY/USD
    if "CNY/USD" in currencies and not currencies["CNY/USD"].empty:
        cny = currencies["CNY/USD"]
        key_metrics["cny_usd"] = cny["Close"].iloc[-1]

    # Dollar index
    econ = read_economic()
    if not econ.empty:
        dollar = econ[econ["series_name"] == "US Dollar Index"].sort_values("Date")
        if not dollar.empty:
            key_metrics["dollar_index"] = dollar.iloc[-1]["value"]

    # Sort signals by severity
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
                attr_rows = subset[subset["attribute"] == attribute].sort_values("reference_period")
                if attr_rows.empty:
                    continue
                latest = attr_rows.iloc[-1]
                entry = {
                    "value": latest.get("value"),
                    "unit": latest.get("unit", ""),
                    "period": latest.get("reference_period", ""),
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

    # --- Crop progress ---
    crop_data = read_crop_progress()
    crop_summary = {}
    if not crop_data.empty:
        soy_crop = crop_data[crop_data["commodity"] == "SOYBEANS"]
        if not soy_crop.empty:
            # Latest condition
            condition = soy_crop[soy_crop["stat_category"] == "CONDITION"]
            if not condition.empty:
                latest_week = condition["week_ending"].max()
                latest_cond = condition[condition["week_ending"] == latest_week]
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
                crop_summary["progress"] = []
                for _, row in latest_prog.iterrows():
                    desc = str(row.get("short_desc", ""))
                    val = row.get("Value", "")
                    if val:
                        crop_summary["progress"].append({"desc": desc, "value": val})

    return {
        "wasde": wasde_summary,
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
    all_signals = []

    for leg in SOY_LEGS:
        df = prices.get(leg)
        if df is None or df.empty:
            continue
        per_leg[leg] = df
        signals = detect_all_signals(df, leg)
        all_signals.extend(signals)

    severity_order = {"alert": 0, "warning": 1, "info": 2}
    all_signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    return {
        "per_leg": per_leg,
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
        oil_meal_ratio: soy oil / soy meal price ratio (tracks protein vs oil demand)
        oil_vs_palm: soybean oil vs palm oil comparison
        bean_corn_ratio: soybean/corn ratio (acreage competition signal)
        soy_oil_share: soy oil as % of total crush value
    """
    prices = _load_soy_prices()

    beans = prices.get("Soybeans")
    oil = prices.get("Soybean Oil")
    meal = prices.get("Soybean Meal")
    palm = prices.get("Palm Oil (BMD)")
    corn = prices.get("Corn")

    result = {}

    # --- Crush spread ---
    if beans is not None and oil is not None and meal is not None:
        if not beans.empty and not oil.empty and not meal.empty:
            try:
                spread = compute_crush_spread(beans, oil, meal)
                if not spread.empty:
                    result["crush"] = {
                        "series": spread,
                        "current_dollars": spread.iloc[-1]["crush_spread"] / 100,
                        "profitable": spread.iloc[-1]["crush_spread"] > 0,
                    }
            except Exception:
                pass

    # --- Oil/Meal ratio ---
    if oil is not None and meal is not None:
        if not oil.empty and not meal.empty:
            combined = pd.DataFrame({
                "oil": oil["Close"],
                "meal": meal["Close"],
            }).dropna()
            if not combined.empty:
                combined["ratio"] = combined["oil"] / combined["meal"]
                result["oil_meal_ratio"] = {
                    "series": combined["ratio"],
                    "current": combined["ratio"].iloc[-1],
                    "avg_60d": combined["ratio"].iloc[-60:].mean() if len(combined) >= 60 else combined["ratio"].mean(),
                }

    # --- Soy oil vs Palm oil ---
    if oil is not None and palm is not None:
        if not oil.empty and not palm.empty:
            oil_latest = oil["Close"].iloc[-1]
            palm_latest = palm["Close"].iloc[-1]
            result["oil_vs_palm"] = {
                "soy_oil": oil_latest,
                "palm_oil": palm_latest,
            }
            if len(oil) >= 6 and len(palm) >= 6:
                result["oil_vs_palm"]["soy_oil_weekly_chg"] = (
                    (oil["Close"].iloc[-1] - oil["Close"].iloc[-6]) / oil["Close"].iloc[-6]
                ) * 100
                result["oil_vs_palm"]["palm_oil_weekly_chg"] = (
                    (palm["Close"].iloc[-1] - palm["Close"].iloc[-6]) / palm["Close"].iloc[-6]
                ) * 100

    # --- Bean/Corn ratio ---
    if beans is not None and corn is not None:
        if not beans.empty and not corn.empty:
            combined = pd.DataFrame({
                "beans": beans["Close"],
                "corn": corn["Close"],
            }).dropna()
            if not combined.empty:
                combined["ratio"] = combined["beans"] / combined["corn"]
                result["bean_corn_ratio"] = {
                    "series": combined["ratio"],
                    "current": combined["ratio"].iloc[-1],
                    "avg_1y": combined["ratio"].mean(),
                }

    # --- Soy oil share of crush value ---
    if oil is not None and meal is not None and beans is not None:
        if not oil.empty and not meal.empty:
            oil_val = oil["Close"].iloc[-1] * 11  # cents per bushel from oil
            meal_val = meal["Close"].iloc[-1] * 2.2  # cents per bushel from meal
            total_product = oil_val + meal_val
            if total_product > 0:
                result["soy_oil_share"] = (oil_val / total_product) * 100

    return result


# ---------------------------------------------------------------------------
# Analyst 6: Risk Monitor — threats and positioning
# ---------------------------------------------------------------------------

def risk_analysis() -> dict:
    """
    Risk factors: BRL/USD, COT extremes, weather threats, options sentiment.

    Returns dict with:
        currencies: BRL, CNY, ARS latest + changes
        cot: COT positioning for soy complex
        weather_alerts: active weather threats in soy regions
        options: put/call ratios and IV for soy legs
    """
    # --- Currencies ---
    currencies_data = _load_currency_data()
    currency_summary = {}
    for pair, df in currencies_data.items():
        if df.empty:
            continue
        latest = df.iloc[-1]
        entry = {"close": latest["Close"]}
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

            alert_type = None
            if pd.notna(precip) and precip > 20:
                alert_type = "Heavy Rain"
            elif pd.notna(precip) and precip < 1:
                alert_type = "Dry"
            if pd.notna(temp_max) and temp_max > 38:
                alert_type = "Extreme Heat"

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

    # --- Options ---
    options = read_options_sentiment()
    options_summary = {}
    if not options.empty:
        for leg in SOY_LEGS:
            subset = options[options["commodity"] == leg].sort_values("Date")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            options_summary[leg] = {
                "put_call_ratio": latest.get("put_call_ratio"),
                "total_call_oi": latest.get("total_call_oi"),
                "total_put_oi": latest.get("total_put_oi"),
                "avg_call_iv": latest.get("avg_call_iv"),
                "avg_put_iv": latest.get("avg_put_iv"),
            }

    return {
        "currencies": currency_summary,
        "cot": cot_summary,
        "weather_alerts": weather_alerts,
        "options": options_summary,
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

        monthly = monthly_seasonal(df)
        vs_seasonal = current_vs_seasonal(df)

        result[leg] = {
            "monthly": monthly,
            "vs_seasonal": vs_seasonal,
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
    result = {}

    if fc.empty:
        return result

    for leg in SOY_LEGS:
        subset = fc[fc["commodity"] == leg].sort_values("contract_month")
        if len(subset) < 2:
            continue

        curve_analysis = analyze_curve(subset)
        cal_spread = calendar_spread(subset, 0, 1) if len(subset) >= 2 else {}

        result[leg] = {
            "curve_data": subset,
            "analysis": curve_analysis,
            "calendar_spread": cal_spread,
        }

    return result
