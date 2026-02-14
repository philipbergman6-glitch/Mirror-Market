"""
Daily market briefing generator.

Reads ALL data from the database, runs analysis, and returns a formatted
text summary combining: prices, technicals, crush spread, currencies,
COT positioning, weather alerts, PSD global supply, World Bank prices,
FRED economic context, USDA fundamentals, DCE Chinese futures,
correlations, seasonal patterns, and a Market Drivers narrative.

Key concepts for learning:
    - This module ties everything together — it's the "output" layer
    - It imports from the processing layer (combiner.read_*) for data
      and the analysis layer (technical, spreads, signals, etc.) for insights
    - The briefing is just a string — you can print it, email it, or
      display it in a dashboard
"""

import logging
from datetime import date, datetime, timedelta

import pandas as pd

from config import (
    FRESHNESS_WARNING_DAYS,
    RSI_OVERBOUGHT,
    WEATHER_HEAVY_RAIN_MM,
    WEATHER_EXTREME_HEAT_C,
    WEATHER_DRY_THRESHOLD_MM,
)
from processing.combiner import (
    read_prices,
    read_cot,
    read_currencies,
    read_psd,
    read_weather,
    read_worldbank_prices,
    read_economic,
    read_usda,
    read_dce_futures,
    read_crop_progress,
    read_export_sales,
    read_forward_curve,
    read_freshness,
)
from analysis.technical import compute_all_technicals
from analysis.spreads import compute_crush_spread
from analysis.signals import detect_all_signals
from analysis.correlations import commodity_correlation_matrix, commodity_vs_currency
from analysis.seasonal import current_vs_seasonal
from analysis.forward_curve import analyze_curve

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper to load price data (used by multiple sections)
# ---------------------------------------------------------------------------

def _load_price_data() -> dict[str, pd.DataFrame]:
    """Load all price data from DB and set up DatetimeIndex."""
    all_prices = read_prices()
    price_data = {}
    if not all_prices.empty:
        for commodity in all_prices["commodity"].unique():
            subset = all_prices[all_prices["commodity"] == commodity].copy()
            subset["Date"] = pd.to_datetime(subset["Date"])
            subset = subset.set_index("Date").sort_index()
            price_data[commodity] = subset
    return price_data


def _load_currency_data() -> dict[str, pd.DataFrame]:
    """Load all currency data from DB and set up DatetimeIndex."""
    all_currencies = read_currencies()
    currency_data = {}
    if not all_currencies.empty:
        for pair in all_currencies["pair"].unique():
            subset = all_currencies[all_currencies["pair"] == pair].copy()
            subset["Date"] = pd.to_datetime(subset["Date"])
            subset = subset.set_index("Date").sort_index()
            currency_data[pair] = subset
    return currency_data


# ---------------------------------------------------------------------------
# Section formatters
# ---------------------------------------------------------------------------

def _format_freshness_warnings() -> str:
    """Check data freshness and return warnings for stale layers."""
    freshness = read_freshness()
    if freshness.empty:
        return ""

    warnings = []
    now = datetime.utcnow()
    threshold = timedelta(days=FRESHNESS_WARNING_DAYS)

    for _, row in freshness.iterrows():
        last = row["last_success"]
        if pd.notna(last):
            age = now - last
            if age > threshold:
                days_old = age.days
                warnings.append(
                    f"  WARNING: {row['layer_name']} data is {days_old} days old"
                )

    if not warnings:
        return ""

    return "DATA FRESHNESS WARNINGS:\n" + "\n".join(warnings)


def _format_price_section(price_data: dict[str, pd.DataFrame]) -> tuple[str, list[dict], dict[str, pd.DataFrame]]:
    """Format the PRICES section, collect signals, and return enriched DataFrames."""
    lines = ["PRICES:"]
    all_signals = []
    enriched = {}

    for commodity, df in price_data.items():
        if df.empty:
            lines.append(f"  {commodity}: No data")
            continue

        df = compute_all_technicals(df)
        enriched[commodity] = df
        latest = df.iloc[-1]
        close = latest["Close"]
        daily_chg = latest.get("daily_pct_change", 0)
        rsi = latest.get("RSI", None)

        # Build description parts
        parts = [f"{close:,.2f}"]
        if pd.notna(daily_chg):
            sign = "+" if daily_chg >= 0 else ""
            parts.append(f"({sign}{daily_chg:.1f}%)")

        # MA context
        ma50 = latest.get("MA_50", None)
        ma200 = latest.get("MA_200", None)
        if pd.notna(ma200):
            if close > ma200:
                parts.append("Above 200-day MA")
            else:
                parts.append("Below 200-day MA")
        elif pd.notna(ma50):
            if close > ma50:
                parts.append("Above 50-day MA")
            else:
                parts.append("Below 50-day MA")

        # RSI context
        if pd.notna(rsi):
            if rsi > RSI_OVERBOUGHT:
                parts.append(f"RSI {rsi:.0f} (overbought)")
            elif rsi < 30:
                parts.append(f"RSI {rsi:.0f} (oversold)")

        # MACD context
        macd_hist = latest.get("MACD_Histogram", None)
        if pd.notna(macd_hist):
            parts.append(f"MACD {'positive' if macd_hist > 0 else 'negative'}")

        # Volatility context
        hv20 = latest.get("HV_20", None)
        if pd.notna(hv20):
            parts.append(f"Vol {hv20:.0f}%")

        lines.append(f"  {commodity + ':':16s} {('  '.join(parts))}")

        # Collect signals
        signals = detect_all_signals(df, commodity)
        all_signals.extend(signals)

    return "\n".join(lines), all_signals, enriched


def _format_crush_spread(price_data: dict[str, pd.DataFrame]) -> str:
    """Format the CRUSH SPREAD section."""
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    oil = price_data.get("Soybean Oil", pd.DataFrame())
    meal = price_data.get("Soybean Meal", pd.DataFrame())

    if soybeans.empty or oil.empty or meal.empty:
        return "CRUSH SPREAD: Insufficient data"

    try:
        spread = compute_crush_spread(soybeans, oil, meal)
        if spread.empty:
            return "CRUSH SPREAD: No overlapping dates"

        latest_cents = spread.iloc[-1]["crush_spread"]
        latest_dollars = latest_cents / 100
        if len(spread) >= 6:
            prev = spread.iloc[-6]["crush_spread"]
            trend = "widening" if latest_cents > prev else "narrowing"
            profitability = "processors profitable" if latest_cents > 0 else "margin squeeze"
            return f"CRUSH SPREAD: ${latest_dollars:.2f}/bu ({trend} — {profitability})"
        else:
            return f"CRUSH SPREAD: ${latest_dollars:.2f}/bu"
    except Exception as exc:
        logger.debug("Crush spread error: %s", exc)
        return "CRUSH SPREAD: Calculation error"


def _format_fred() -> str:
    """Format the ECONOMIC CONTEXT (FRED) section — dollar index, CPI, rates."""
    lines = ["ECONOMIC CONTEXT (FRED):"]
    econ_data = read_economic()

    if econ_data.empty:
        return "ECONOMIC CONTEXT (FRED): No data"

    for series_name in econ_data["series_name"].unique():
        subset = econ_data[econ_data["series_name"] == series_name].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        value = latest["value"]

        # Compute trend vs prior observation
        comment = ""
        if len(subset) >= 2:
            prev = subset.iloc[-2]
            if pd.notna(prev["value"]) and prev["value"] != 0:
                chg = value - prev["value"]
                chg_pct = (chg / prev["value"]) * 100
                direction = "up" if chg > 0 else "down"

                if "Dollar" in series_name:
                    impact = "headwind for commodities" if chg > 0 else "tailwind for commodities"
                    comment = f"({direction} {abs(chg_pct):.1f}% — {impact})"
                elif "CPI" in series_name:
                    comment = f"({direction} {abs(chg_pct):.1f}%)"
                elif "Fed Funds" in series_name:
                    comment = f"({value:.2f}% — {'tightening' if chg > 0 else 'easing'})"

        if "Fed Funds" in series_name:
            lines.append(f"  {series_name}: {value:.2f}% {comment}")
        elif "CPI" in series_name:
            lines.append(f"  {series_name}: {value:.1f} {comment}")
        else:
            lines.append(f"  {series_name}: {value:.2f} {comment}")

    if len(lines) == 1:
        lines.append("  Data available but no series matched")

    return "\n".join(lines)


def _format_usda() -> str:
    """Format the USDA FUNDAMENTALS section — year-over-year production/yield."""
    lines = ["USDA FUNDAMENTALS:"]
    usda_data = read_usda()

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No data"

    # Get the two most recent years for YoY comparison
    usda_data["year_int"] = pd.to_numeric(usda_data["year"], errors="coerce")
    usda_data = usda_data.dropna(subset=["year_int"])

    if usda_data.empty:
        return "USDA FUNDAMENTALS: No valid year data"

    years = sorted(usda_data["year_int"].unique())
    if len(years) < 2:
        # Only one year — just show latest values
        latest_year = years[-1]
        latest = usda_data[usda_data["year_int"] == latest_year]
        for _, row in latest.head(5).iterrows():
            desc = row.get("short_desc", "")
            val = row.get("Value", "")
            lines.append(f"  {desc}: {val}")
        return "\n".join(lines)

    latest_year = years[-1]
    prev_year = years[-2]
    latest = usda_data[usda_data["year_int"] == latest_year]
    prev = usda_data[usda_data["year_int"] == prev_year]

    for _, row in latest.iterrows():
        desc = row.get("short_desc", "")
        val_str = str(row.get("Value", "")).replace(",", "")
        unit = row.get("unit_desc", "")

        try:
            val = float(val_str)
        except (ValueError, TypeError):
            continue

        # Find matching row in previous year
        prev_match = prev[prev["short_desc"] == desc]
        if prev_match.empty:
            lines.append(f"  {desc}: {val:,.0f} {unit} ({int(latest_year)})")
            continue

        prev_val_str = str(prev_match.iloc[0].get("Value", "")).replace(",", "")
        try:
            prev_val = float(prev_val_str)
        except (ValueError, TypeError):
            lines.append(f"  {desc}: {val:,.0f} {unit} ({int(latest_year)})")
            continue

        if prev_val != 0:
            yoy_pct = ((val - prev_val) / prev_val) * 100
            sign = "+" if yoy_pct >= 0 else ""
            lines.append(f"  {desc}: {val:,.0f} {unit} ({sign}{yoy_pct:.1f}% YoY)")
        else:
            lines.append(f"  {desc}: {val:,.0f} {unit}")

    if len(lines) == 1:
        lines.append("  Data available but no production/yield data found")

    return "\n".join(lines)


def _format_crop_conditions() -> str:
    """Format the CROP CONDITIONS section — weekly USDA condition/progress."""
    lines = ["CROP CONDITIONS (USDA Weekly):"]
    progress_data = read_crop_progress()

    if progress_data.empty:
        return "CROP CONDITIONS (USDA Weekly): No data"

    for commodity in progress_data["commodity"].unique():
        subset = progress_data[progress_data["commodity"] == commodity]
        if subset.empty:
            continue

        lines.append(f"  {commodity}:")

        # Show latest condition ratings (good/excellent %)
        condition = subset[subset["stat_category"] == "CONDITION"]
        if not condition.empty:
            # Get most recent week
            latest_week = condition["week_ending"].max()
            latest = condition[condition["week_ending"] == latest_week]
            for _, row in latest.iterrows():
                desc = str(row.get("short_desc", ""))
                val = row.get("Value", "")
                # Show key condition ratings
                if any(kw in desc.upper() for kw in ["GOOD", "EXCELLENT", "POOR"]):
                    lines.append(f"    {desc}: {val}%")

        # Show latest progress milestone
        progress = subset[subset["stat_category"] == "PROGRESS"]
        if not progress.empty:
            latest_week = progress["week_ending"].max()
            latest = progress[progress["week_ending"] == latest_week]
            for _, row in latest.iterrows():
                desc = str(row.get("short_desc", ""))
                val = row.get("Value", "")
                if val:
                    lines.append(f"    {desc}: {val}%")

    if len(lines) == 1:
        lines.append("  No crop condition data available")

    return "\n".join(lines)


def _format_yield_curve() -> str:
    """Format yield curve context from FRED Treasury data."""
    econ_data = read_economic()

    if econ_data.empty:
        return ""

    t2y = econ_data[econ_data["series_name"] == "Treasury 2Y"].sort_values("Date")
    t10y = econ_data[econ_data["series_name"] == "Treasury 10Y"].sort_values("Date")

    if t2y.empty or t10y.empty:
        return ""

    latest_2y = t2y.iloc[-1]["value"]
    latest_10y = t10y.iloc[-1]["value"]

    if pd.isna(latest_2y) or pd.isna(latest_10y):
        return ""

    spread = latest_10y - latest_2y
    if spread < 0:
        assessment = "INVERTED — recession signal, demand destruction risk for commodities"
    elif spread < 0.5:
        assessment = "flat — economic uncertainty"
    else:
        assessment = "normal — growth environment"

    return (
        f"YIELD CURVE:\n"
        f"  2Y: {latest_2y:.2f}%  |  10Y: {latest_10y:.2f}%  |  "
        f"Spread: {spread:+.2f}% ({assessment})"
    )


def _format_dce(price_data: dict[str, pd.DataFrame]) -> str:
    """Format the DCE CHINESE FUTURES section alongside CBOT prices."""
    lines = ["DCE CHINESE FUTURES:"]
    dce_data = read_dce_futures()

    if dce_data.empty:
        return "DCE CHINESE FUTURES: No data"

    # Map DCE names to CBOT names for comparison
    dce_to_cbot = {
        "DCE Soybean": "Soybeans",
        "DCE Soybean Meal": "Soybean Meal",
        "DCE Soybean Oil": "Soybean Oil",
    }

    for dce_name in dce_data["commodity"].unique():
        subset = dce_data[dce_data["commodity"] == dce_name].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        dce_close = latest["Close"]
        dce_date = latest["Date"]

        parts = [f"CNY {dce_close:,.0f}"]

        # Show CBOT comparison if available
        cbot_name = dce_to_cbot.get(dce_name)
        if cbot_name and cbot_name in price_data:
            cbot_df = price_data[cbot_name]
            if not cbot_df.empty:
                cbot_close = cbot_df["Close"].iloc[-1]
                parts.append(f"vs CBOT {cbot_close:,.2f} USD")

        lines.append(f"  {dce_name}: {' | '.join(parts)} (as of {dce_date.date() if hasattr(dce_date, 'date') else dce_date})")

    return "\n".join(lines)


def _format_correlations(price_data: dict[str, pd.DataFrame], currency_data: dict[str, pd.DataFrame]) -> str:
    """Format the CORRELATIONS section."""
    lines = ["CORRELATIONS:"]

    # Cross-commodity correlation matrix
    if len(price_data) >= 2:
        corr_matrix = commodity_correlation_matrix(price_data)
        if not corr_matrix.empty:
            lines.append("  Cross-commodity (Close prices):")
            # Show notable correlations (|r| > 0.5 between different commodities)
            shown = set()
            for i, row_name in enumerate(corr_matrix.index):
                for j, col_name in enumerate(corr_matrix.columns):
                    if i >= j:
                        continue
                    pair_key = tuple(sorted([row_name, col_name]))
                    if pair_key in shown:
                        continue
                    r = corr_matrix.iloc[i, j]
                    if pd.notna(r) and abs(r) > 0.5:
                        strength = "strong" if abs(r) > 0.7 else "moderate"
                        direction = "positive" if r > 0 else "negative"
                        lines.append(f"    {row_name} vs {col_name}: {r:.2f} ({strength} {direction})")
                        shown.add(pair_key)

    # Commodity vs currency correlations
    key_pairs = [
        ("Soybeans", "BRL/USD", "BRL weakening → cheaper Brazil exports → soy pressure"),
        ("Coffee", "COP/USD", "COP weakening → cheaper Colombia exports"),
        ("Coffee", "BRL/USD", "BRL weakening → cheaper Brazil exports"),
    ]

    currency_corrs = []
    for commodity_name, pair_name, note in key_pairs:
        if commodity_name in price_data and pair_name in currency_data:
            r = commodity_vs_currency(
                price_data[commodity_name],
                currency_data[pair_name],
                commodity_name,
                pair_name,
            )
            if pd.notna(r):
                currency_corrs.append(f"    {commodity_name} vs {pair_name}: {r:.2f} ({note})")

    if currency_corrs:
        lines.append("  Commodity-currency:")
        lines.extend(currency_corrs)

    if len(lines) == 1:
        lines.append("  Insufficient data for correlation analysis")

    return "\n".join(lines)


def _format_seasonal(price_data: dict[str, pd.DataFrame]) -> str:
    """Format the SEASONAL ANALYSIS section."""
    lines = ["SEASONAL ANALYSIS:"]

    for commodity, df in price_data.items():
        if df.empty:
            continue

        result = current_vs_seasonal(df)
        if result:
            lines.append(f"  {commodity}: {result['assessment']}")

    if len(lines) == 1:
        lines.append("  Insufficient history for seasonal comparison")

    return "\n".join(lines)


def _format_currencies(currency_data: dict[str, pd.DataFrame]) -> str:
    """Format the CURRENCIES section."""
    lines = ["CURRENCIES:"]

    if not currency_data:
        return "CURRENCIES: No data"

    for pair, subset in currency_data.items():
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        close = latest["Close"]

        comment = ""
        if len(subset) >= 6:
            prev = subset.iloc[-6]["Close"]
            if pd.notna(prev) and prev != 0:
                chg_pct = ((close - prev) / prev) * 100
                if "BRL" in pair:
                    direction = "Real weakening" if chg_pct < 0 else "Real strengthening"
                    impact = "Brazil exports cheaper" if chg_pct < 0 else "Brazil exports dearer"
                    comment = f"({direction} — {impact})"
                elif "CNY" in pair:
                    direction = "Yuan weakening" if chg_pct < 0 else "Yuan stable"
                    comment = f"({direction})"
                elif "ARS" in pair:
                    direction = "Peso weakening" if chg_pct < 0 else "Peso stable"
                    comment = f"({direction})"
                elif "IDR" in pair:
                    direction = "Rupiah weakening" if chg_pct < 0 else "Rupiah stable"
                    comment = f"({direction})"
                elif "MYR" in pair:
                    direction = "Ringgit weakening" if chg_pct < 0 else "Ringgit stable"
                    comment = f"({direction})"

        lines.append(f"  {pair}: {close:.4f} {comment}")

    return "\n".join(lines)


def _format_cot() -> str:
    """Format the COT POSITIONING section."""
    lines = ["COT POSITIONING:"]
    cot_data = read_cot()

    if cot_data.empty:
        return "COT POSITIONING: No data"

    for commodity in cot_data["commodity"].unique():
        subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        comm_net = latest.get("commercial_net", None)
        spec_net = latest.get("noncommercial_net", None)

        parts = []
        if pd.notna(comm_net):
            parts.append(f"Commercials net {'long' if comm_net > 0 else 'short'} {abs(comm_net):,.0f}")
        if pd.notna(spec_net):
            parts.append(f"Specs net {'long' if spec_net > 0 else 'short'} {abs(spec_net):,.0f}")

        lines.append(f"  {commodity}: {', '.join(parts)}")

    return "\n".join(lines)


def _format_weather() -> str:
    """Format the WEATHER ALERTS section using configurable thresholds."""
    lines = ["WEATHER ALERTS:"]
    weather_data = read_weather()

    if weather_data.empty:
        return "WEATHER ALERTS: No data"

    has_alert = False
    for region in weather_data["region"].unique():
        subset = weather_data[weather_data["region"] == region].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        precip = latest.get("precipitation", 0)
        temp_max = latest.get("temp_max", None)

        alerts = []
        if pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM:
            alerts.append(f"Heavy rain ({precip:.0f}mm) — harvest delays possible")
        elif pd.notna(precip) and precip < WEATHER_DRY_THRESHOLD_MM:
            alerts.append("Dry conditions — watch soil moisture")

        if pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C:
            alerts.append(f"Extreme heat ({temp_max:.0f}C) — crop stress risk")

        if alerts:
            has_alert = True
            for alert in alerts:
                lines.append(f"  {region}: {alert}")

    if not has_alert:
        lines.append("  No significant weather alerts")

    return "\n".join(lines)


def _format_psd() -> str:
    """Format the GLOBAL SUPPLY (PSD) section."""
    lines = ["GLOBAL SUPPLY (USDA PSD):"]
    psd_data = read_psd()

    if psd_data.empty:
        return "GLOBAL SUPPLY (USDA PSD): No data"

    latest_year = psd_data["year"].max()
    latest = psd_data[psd_data["year"] == latest_year]

    highlights = [
        ("Soybeans", "Brazil", "Production"),
        ("Soybeans", "China", "Imports"),
        ("Soybeans", "United States", "Production"),
        ("Palm Oil", "Indonesia", "Production"),
    ]

    for commodity, country, attribute in highlights:
        row = latest[
            (latest["commodity"] == commodity) &
            (latest["country"] == country) &
            (latest["attribute"] == attribute)
        ]
        if not row.empty:
            value = row.iloc[0]["value"]
            unit = str(row.iloc[0].get("unit", "1000 MT")).strip("() ")
            lines.append(f"  {country} {commodity.lower()} {attribute.lower()}: {value:,.0f} ({unit})")

    if len(lines) == 1:
        lines.append("  Data available but no key highlights matched")

    return "\n".join(lines)


def _format_worldbank() -> str:
    """Format the WORLD PRICES (World Bank) section."""
    lines = ["WORLD PRICES (World Bank Monthly):"]
    wb_data = read_worldbank_prices()

    if wb_data.empty:
        return "WORLD PRICES (World Bank Monthly): No data"

    for commodity in wb_data["commodity"].unique():
        subset = wb_data[wb_data["commodity"] == commodity].sort_values("Date")
        if len(subset) < 2:
            continue

        latest = subset.iloc[-1]
        prev = subset.iloc[-2]
        price = latest["price"]
        unit = latest.get("unit", "")

        if pd.notna(prev["price"]) and prev["price"] != 0:
            chg_pct = ((price - prev["price"]) / prev["price"]) * 100
            sign = "+" if chg_pct >= 0 else ""
            price_str = f"${price:,.0f}/mt" if "mt" in str(unit).lower() else f"{price:,.2f} {unit}"
            lines.append(
                f"  {commodity}: {price_str} ({sign}{chg_pct:.1f}% vs last month)"
            )
        else:
            price_str = f"${price:,.0f}/mt" if "mt" in str(unit).lower() else f"{price:,.2f} {unit}"
            lines.append(f"  {commodity}: {price_str}")

    return "\n".join(lines)


def _format_signals(signals: list[dict]) -> str:
    """Format the SIGNALS section."""
    if not signals:
        return "SIGNALS:\n  No active signals"

    lines = ["SIGNALS:"]
    # Sort by severity: alert > warning > info
    severity_order = {"alert": 0, "warning": 1, "info": 2}
    signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    for s in signals:
        severity_tag = f"[{s.get('severity', 'info').upper()}]"
        lines.append(f"  {severity_tag:10s} {s['description']}")

    return "\n".join(lines)


def _format_export_sales() -> str:
    """Format the EXPORT SALES section — weekly USDA FAS demand data."""
    lines = ["EXPORT SALES (USDA Weekly):"]
    es_data = read_export_sales()

    if es_data.empty:
        return "EXPORT SALES (USDA Weekly): No data (set FAS_API_KEY to enable)"

    for commodity in es_data["commodity"].unique():
        subset = es_data[es_data["commodity"] == commodity]
        if subset.empty:
            continue

        # Get the most recent week
        latest_week = subset["week_ending"].max()
        week_data = subset[subset["week_ending"] == latest_week]

        # Total net sales and exports across all destinations
        total_net_sales = week_data["net_sales"].sum() if "net_sales" in week_data.columns else 0
        total_exports = week_data["weekly_exports"].sum() if "weekly_exports" in week_data.columns else 0

        # Top 3 destinations by net sales
        top_buyers = week_data.nlargest(3, "net_sales") if "net_sales" in week_data.columns else pd.DataFrame()
        buyer_parts = []
        for _, row in top_buyers.iterrows():
            country = row.get("country", "Unknown")
            sales = row.get("net_sales", 0)
            if pd.notna(sales) and sales != 0:
                buyer_parts.append(f"{country} ({sales:,.0f} MT)")

        parts = [f"Net sales: {total_net_sales:,.0f} MT"]
        if total_exports:
            parts.append(f"Exports: {total_exports:,.0f} MT")
        if buyer_parts:
            parts.append(f"Top buyers: {', '.join(buyer_parts)}")

        week_str = latest_week.strftime("%m/%d") if hasattr(latest_week, "strftime") else str(latest_week)
        lines.append(f"  {commodity} (w/e {week_str}): {' | '.join(parts)}")

    if len(lines) == 1:
        lines.append("  Data available but no sales data found")

    return "\n".join(lines)


def _format_forward_curve() -> str:
    """Format the FORWARD CURVE section — market term structure."""
    lines = ["FORWARD CURVE:"]
    fc_data = read_forward_curve()

    if fc_data.empty:
        return "FORWARD CURVE: No data"

    for commodity in fc_data["commodity"].unique():
        subset = fc_data[fc_data["commodity"] == commodity]
        if subset.empty or len(subset) < 2:
            continue

        result = analyze_curve(subset)
        if result:
            lines.append(f"  {commodity}: {result['summary']}")

    if len(lines) == 1:
        lines.append("  Insufficient contracts for curve analysis")

    return "\n".join(lines)


def _format_market_drivers(
    price_data: dict[str, pd.DataFrame],
    enriched: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    """
    Format the MARKET DRIVERS section — cross-data narrative insights.

    This is where we connect the dots between different data sources to
    surface insights that no single section shows on its own.
    """
    lines = ["MARKET DRIVERS:"]
    drivers = []

    # --- BRL + Soybean export competitiveness ---
    if "BRL/USD" in currency_data and not currency_data["BRL/USD"].empty:
        brl = currency_data["BRL/USD"]
        if len(brl) >= 6:
            brl_chg = ((brl["Close"].iloc[-1] - brl["Close"].iloc[-6]) / brl["Close"].iloc[-6]) * 100
            if brl_chg < -1:
                drivers.append(
                    f"Brazil export competitiveness improving: BRL weakened {abs(brl_chg):.1f}% "
                    f"this week — makes Brazilian soy/coffee cheaper on world markets"
                )
            elif brl_chg > 1:
                drivers.append(
                    f"Brazil export competitiveness declining: BRL strengthened {brl_chg:.1f}% "
                    f"this week — Brazilian exports getting more expensive"
                )

    # --- COT extremes + RSI = crowded trade ---
    cot_data = read_cot()
    if not cot_data.empty:
        for commodity in cot_data["commodity"].unique():
            cot_subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
            if cot_subset.empty:
                continue

            spec_net = cot_subset.iloc[-1].get("noncommercial_net", None)
            if pd.notna(spec_net) and commodity in enriched:
                rsi_val = enriched[commodity]["RSI"].iloc[-1] if "RSI" in enriched[commodity].columns else None

                if pd.notna(rsi_val):
                    if spec_net > 0 and rsi_val > RSI_OVERBOUGHT:
                        drivers.append(
                            f"Crowded long in {commodity}: Specs net long {spec_net:,.0f} contracts "
                            f"AND RSI at {rsi_val:.0f} — reversal risk elevated"
                        )
                    elif spec_net < 0 and rsi_val < 30:
                        drivers.append(
                            f"Crowded short in {commodity}: Specs net short {abs(spec_net):,.0f} contracts "
                            f"AND RSI at {rsi_val:.0f} — short squeeze risk"
                        )

    # --- Weather + price movement = weather premium ---
    weather_data = read_weather()
    if not weather_data.empty:
        active_alerts = []
        for region in weather_data["region"].unique():
            subset = weather_data[weather_data["region"] == region].sort_values("Date")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            precip = latest.get("precipitation", 0)
            temp_max = latest.get("temp_max", None)

            if (pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM) or \
               (pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C):
                active_alerts.append(region)

        if active_alerts:
            # Check if any related commodity is rising
            for commodity in ["Soybeans", "Coffee"]:
                if commodity in enriched:
                    weekly_chg = enriched[commodity].get("weekly_pct_change", pd.Series())
                    if not weekly_chg.empty and pd.notna(weekly_chg.iloc[-1]) and weekly_chg.iloc[-1] > 1:
                        drivers.append(
                            f"Weather premium building in {commodity}: price up "
                            f"{weekly_chg.iloc[-1]:.1f}% this week with active weather alerts "
                            f"in {', '.join(active_alerts[:3])}"
                        )

    # --- Corn/Soy acreage competition ---
    if "Corn" in enriched and "Soybeans" in enriched:
        corn_weekly = enriched["Corn"].get("weekly_pct_change", pd.Series())
        soy_weekly = enriched["Soybeans"].get("weekly_pct_change", pd.Series())
        if (not corn_weekly.empty and not soy_weekly.empty
                and pd.notna(corn_weekly.iloc[-1]) and pd.notna(soy_weekly.iloc[-1])):
            corn_chg = corn_weekly.iloc[-1]
            soy_chg = soy_weekly.iloc[-1]
            # Corn outperforming soybeans = farmers may plant more corn next season
            if corn_chg - soy_chg > 3:
                drivers.append(
                    f"Corn outperforming soybeans ({corn_chg:+.1f}% vs {soy_chg:+.1f}% this week): "
                    f"if sustained, farmers may shift acreage to corn next planting season"
                )
            elif soy_chg - corn_chg > 3:
                drivers.append(
                    f"Soybeans outperforming corn ({soy_chg:+.1f}% vs {corn_chg:+.1f}% this week): "
                    f"soybean acreage may expand next season"
                )

    # --- Livestock demand for soybean meal ---
    for livestock in ["Live Cattle", "Lean Hogs"]:
        if livestock in enriched:
            lv_weekly = enriched[livestock].get("weekly_pct_change", pd.Series())
            if not lv_weekly.empty and pd.notna(lv_weekly.iloc[-1]) and lv_weekly.iloc[-1] > 3:
                drivers.append(
                    f"{livestock} prices rising ({lv_weekly.iloc[-1]:+.1f}% this week): "
                    f"expanding herds = more soybean meal demand"
                )

    # --- Export sales demand signal ---
    es_data = read_export_sales()
    if not es_data.empty:
        for commodity in ["Soybeans", "Corn", "Wheat"]:
            es_subset = es_data[es_data["commodity"] == commodity]
            if es_subset.empty:
                continue
            # Check if China is a top buyer (key demand signal)
            latest_week = es_subset["week_ending"].max()
            week_data = es_subset[es_subset["week_ending"] == latest_week]
            china_sales = week_data[week_data["country"].str.contains("China", case=False, na=False)]
            if not china_sales.empty and "net_sales" in china_sales.columns:
                china_net = china_sales["net_sales"].sum()
                total_net = week_data["net_sales"].sum()
                if total_net > 0 and china_net > 0:
                    china_pct = (china_net / total_net) * 100
                    if china_pct > 30:
                        drivers.append(
                            f"China buying pace strong for {commodity}: "
                            f"{china_net:,.0f} MT net sales ({china_pct:.0f}% of total) — "
                            f"demand signal bullish"
                        )

    # --- Forward curve structure ---
    fc_data = read_forward_curve()
    if not fc_data.empty:
        for commodity in ["Soybeans", "Corn", "Wheat"]:
            fc_subset = fc_data[fc_data["commodity"] == commodity]
            if len(fc_subset) >= 2:
                result = analyze_curve(fc_subset)
                if result and "backwardation" in result.get("structure", ""):
                    drivers.append(
                        f"{commodity} in backwardation ({result['spread_pct']:+.1f}%): "
                        f"market signals tight supply / strong nearby demand"
                    )
                elif result and result.get("spread_pct", 0) > 5:
                    drivers.append(
                        f"{commodity} in steep contango ({result['spread_pct']:+.1f}%): "
                        f"market expects adequate supply, carrying costs elevated"
                    )

    # --- Dollar strength + commodities ---
    econ_data = read_economic()
    if not econ_data.empty:
        dollar = econ_data[econ_data["series_name"] == "US Dollar Index"].sort_values("Date")
        if len(dollar) >= 2:
            latest_val = dollar.iloc[-1]["value"]
            prev_val = dollar.iloc[-2]["value"]
            if pd.notna(latest_val) and pd.notna(prev_val) and prev_val != 0:
                dollar_chg = ((latest_val - prev_val) / prev_val) * 100
                if abs(dollar_chg) > 0.5:
                    direction = "strengthening" if dollar_chg > 0 else "weakening"
                    impact = "headwind" if dollar_chg > 0 else "tailwind"
                    drivers.append(
                        f"Dollar {direction} ({dollar_chg:+.1f}%): "
                        f"generally a {impact} for USD-denominated commodities"
                    )

    if not drivers:
        lines.append("  No cross-market signals detected this session")
    else:
        for i, driver in enumerate(drivers, 1):
            lines.append(f"  {i}. {driver}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main briefing generator
# ---------------------------------------------------------------------------

def generate_briefing() -> str:
    """
    Read ALL data from database, run analysis, return formatted text briefing.

    This is the main function you call to get the daily market summary.
    It combines every data source and analysis module into one text block.

    Returns
    -------
    str
        The complete market briefing as a formatted text string.
    """
    today = date.today().strftime("%Y-%m-%d")
    sections = [f"=== Mirror Market Daily Briefing — {today} ===", ""]

    # Data freshness warnings (show at top so stale data is immediately visible)
    freshness = _format_freshness_warnings()
    if freshness:
        sections.append(freshness)
        sections.append("")

    # Load shared data
    price_data = _load_price_data()
    currency_data = _load_currency_data()

    # PRICES + collect signals + enriched DataFrames
    price_section, signals, enriched = _format_price_section(price_data)
    sections.append(price_section)
    sections.append("")

    # CRUSH SPREAD
    sections.append(_format_crush_spread(price_data))
    sections.append("")

    # ECONOMIC CONTEXT (FRED) — previously dead code
    sections.append(_format_fred())
    sections.append("")

    # USDA FUNDAMENTALS — previously dead code
    sections.append(_format_usda())
    sections.append("")

    # CROP CONDITIONS — weekly USDA condition/progress
    sections.append(_format_crop_conditions())
    sections.append("")

    # YIELD CURVE — recession/growth signal
    yield_curve = _format_yield_curve()
    if yield_curve:
        sections.append(yield_curve)
        sections.append("")

    # EXPORT SALES — weekly USDA demand data
    sections.append(_format_export_sales())
    sections.append("")

    # DCE CHINESE FUTURES — previously dead code
    sections.append(_format_dce(price_data))
    sections.append("")

    # FORWARD CURVE — market term structure
    sections.append(_format_forward_curve())
    sections.append("")

    # CURRENCIES
    sections.append(_format_currencies(currency_data))
    sections.append("")

    # COT
    sections.append(_format_cot())
    sections.append("")

    # WEATHER
    sections.append(_format_weather())
    sections.append("")

    # PSD
    sections.append(_format_psd())
    sections.append("")

    # WORLD BANK
    sections.append(_format_worldbank())
    sections.append("")

    # CORRELATIONS — previously dead code
    sections.append(_format_correlations(price_data, currency_data))
    sections.append("")

    # SEASONAL ANALYSIS — previously dead code
    sections.append(_format_seasonal(price_data))
    sections.append("")

    # MARKET DRIVERS — new cross-data narrative
    sections.append(_format_market_drivers(price_data, enriched, currency_data))
    sections.append("")

    # SIGNALS (sorted by severity)
    sections.append(_format_signals(signals))

    return "\n".join(sections)


# -- Quick self-test -------------------------------------------------------
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()
    print(generate_briefing())
