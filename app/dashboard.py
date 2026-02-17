"""
Mirror Market — Soy Complex Trading Dashboard.

A Streamlit + Plotly dashboard built for a professional soy complex trader.
Every page focuses exclusively on Soybeans (ZS=F), Soybean Oil (ZL=F),
and Soybean Meal (ZM=F) — plus the key drivers that move them.

Pages:
    1. Command Center  — at-a-glance snapshot of all 3 legs + crush + signals
    2. Technicals       — candlestick charts with RSI/MACD/BBands for each leg
    3. Supply/Demand    — WASDE balance sheet, CONAB, exports, China, biodiesel
    4. Relative Value   — crush spread, oil/meal ratio, oil vs palm, bean/corn (with 1Y overlays)
    5. Risk Monitor     — BRL/USD, COT positioning, weather, options, correlations
    6. Forward Curves   — term structure for all 3 soy contracts
    7. Seasonal         — monthly avg patterns vs current price for each leg
    8. Briefing         — full text briefing + data health

Run with:  streamlit run app/dashboard.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st

# Bridge Streamlit Cloud secrets to environment variables
for key in ("USDA_API_KEY", "FRED_API_KEY", "FAS_API_KEY", "EIA_API_KEY",
            "TURSO_DATABASE_URL", "TURSO_AUTH_TOKEN"):
    if key not in os.environ:
        try:
            os.environ[key] = st.secrets[key]
        except (KeyError, FileNotFoundError):
            pass

import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# ---------------------------------------------------------------------------
# Page config (must be first Streamlit command)
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Soy Complex Desk",
    page_icon="🫘",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Auto-fetch ONCE per session if DB is missing or new tables are empty
# ---------------------------------------------------------------------------
from config import DB_PATH
from processing.database import get_connection, is_cloud


def _get_empty_tables() -> list[str]:
    """Return list of important tables that are empty or missing."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return ["prices"]  # DB doesn't exist at all
    from processing.combiner import init_database
    init_database()
    empty = []
    try:
        conn = get_connection()
        for table in ["prices", "wasde", "inspections", "brazil_estimates"]:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            if count == 0:
                empty.append(table)
        conn.close()
    except Exception:
        return ["prices"]
    return empty


# Only check once per session — not on every page navigation
if "data_checked" not in st.session_state:
    empty = _get_empty_tables()
    if empty:
        # If we have NO data at all, auto-fetch core layers only (fast)
        if "prices" in empty:
            with st.spinner("First launch — fetching core price data..."):
                try:
                    from processing.combiner import init_database, save_price_data, save_currency_data, save_freshness
                    from data.fetchers.yfinance_fetcher import fetch_all as fetch_prices, fetch_currencies
                    from processing.cleaner import clean_ohlcv, clean_currencies
                    init_database()
                    # Prices only — fast, no API keys needed
                    price_data = fetch_prices()
                    for name in price_data:
                        price_data[name] = clean_ohlcv(price_data[name])
                    for name, df in price_data.items():
                        save_price_data(name, df)
                    save_freshness("prices", sum(len(df) for df in price_data.values()))
                    # Currencies — also fast, no key needed
                    currency_data = fetch_currencies()
                    for name in currency_data:
                        currency_data[name] = clean_currencies(currency_data[name])
                    for pair, df in currency_data.items():
                        save_currency_data(pair, df)
                    save_freshness("currencies", sum(len(df) for df in currency_data.values()))
                except Exception as e:
                    st.warning(f"Auto-fetch failed: {e}")
        # Show notice about missing tables (don't block the app)
        remaining = [t for t in _get_empty_tables() if t != "prices"]
        if remaining:
            st.info(
                f"Some data tables are empty ({', '.join(remaining)}). "
                "Click **Refresh Data** in the sidebar to run the full pipeline."
            )
    st.session_state["data_checked"] = True

# ---------------------------------------------------------------------------
# Professional color theme
# ---------------------------------------------------------------------------
COLORS = {
    "primary": "#1B4332",      # Dark green
    "secondary": "#2D6A4F",    # Forest green
    "accent": "#40916C",       # Medium green
    "bullish": "#2DC653",      # Bright green
    "bearish": "#E63946",      # Red
    "neutral": "#6C757D",      # Gray
    "bg_light": "#F8F9FA",     # Light background
    "text": "#212529",         # Dark text
    "soybean": "#DAA520",      # Goldenrod
    "soy_oil": "#FF8C00",      # Dark orange
    "soy_meal": "#8B4513",     # Saddle brown
}

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------
PAGES = [
    "Command Center",
    "Technicals",
    "Supply & Demand",
    "Relative Value",
    "Risk Monitor",
    "Forward Curves",
    "Seasonal",
    "Briefing",
    "About",
]

st.sidebar.markdown("## Soy Complex Desk")
page = st.sidebar.radio("Navigate", PAGES, label_visibility="collapsed")

st.sidebar.divider()
if st.sidebar.button("Refresh Data"):
    with st.sidebar:
        with st.spinner("Fetching latest data..."):
            from main import run as run_pipeline
            run_pipeline()
    st.cache_data.clear()
    st.rerun()

# Sidebar freshness indicators
st.sidebar.divider()
st.sidebar.markdown("**Data Freshness**")
try:
    from processing.combiner import read_freshness
    freshness = read_freshness()
    if not freshness.empty:
        from datetime import datetime, timedelta
        now = datetime.utcnow()
        for _, row in freshness.iterrows():
            layer = row["layer_name"]
            last = row["last_success"]
            if pd.notna(last):
                age = now - last
                if age < timedelta(hours=24):
                    dot = "🟢"
                elif age < timedelta(days=7):
                    dot = "🟡"
                else:
                    dot = "🔴"
                age_str = f"{age.days}d" if age.days > 0 else f"{age.seconds // 3600}h"
                st.sidebar.caption(f"{dot} {layer}: {age_str} ago")
    else:
        st.sidebar.caption("No freshness data yet")
except Exception:
    st.sidebar.caption("Freshness data unavailable")

st.sidebar.divider()
st.sidebar.caption("Soybeans | Soybean Oil | Soybean Meal")
st.sidebar.caption("v2.0 — Mirror Market")

# ---------------------------------------------------------------------------
# Cached analytics loaders
# ---------------------------------------------------------------------------
from analysis.soy_analytics import (
    command_center,
    supply_analysis,
    demand_analysis,
    technicals_analysis,
    relative_value_analysis,
    risk_analysis,
    seasonal_analysis,
    forward_curve_analysis,
    emerging_markets_analysis,
)


def _check_error(data, page_name):
    """Check if analyst returned an error and display it. Returns True if error."""
    if isinstance(data, dict) and "error" in data:
        st.error(f"{page_name}: {data['error']}")
        st.info("Try clicking **Refresh Data** in the sidebar, or run `python main.py` first.")
        return True
    return False


def _safe_call(fn, label):
    """Wrap an analyst function with error handling."""
    try:
        return fn()
    except Exception as e:
        return {"error": f"{label} failed: {str(e)}"}


@st.cache_data(ttl=300)
def load_command_center():
    return _safe_call(command_center, "Command Center")


@st.cache_data(ttl=300)
def load_supply():
    return _safe_call(supply_analysis, "Supply")


@st.cache_data(ttl=300)
def load_demand():
    return _safe_call(demand_analysis, "Demand")


@st.cache_data(ttl=300)
def load_technicals():
    return _safe_call(technicals_analysis, "Technicals")


@st.cache_data(ttl=300)
def load_relative_value():
    return _safe_call(relative_value_analysis, "Relative Value")


@st.cache_data(ttl=300)
def load_risk():
    return _safe_call(risk_analysis, "Risk")


@st.cache_data(ttl=300)
def load_seasonal():
    return _safe_call(seasonal_analysis, "Seasonal")


@st.cache_data(ttl=300)
def load_forward_curves():
    return _safe_call(forward_curve_analysis, "Forward Curves")


@st.cache_data(ttl=300)
def load_emerging_markets():
    return _safe_call(emerging_markets_analysis, "Emerging Markets")


@st.cache_data(ttl=600)
def load_briefing():
    from analysis.briefing import generate_briefing
    try:
        return generate_briefing()
    except Exception as e:
        return f"Briefing generation failed: {str(e)}"


# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------
def _chg_color(val):
    """Return green/red color based on positive/negative value."""
    if pd.isna(val):
        return "gray"
    return "green" if val >= 0 else "red"


def _delta_str(val):
    """Format a number as +X.X% or -X.X%."""
    if pd.isna(val):
        return "N/A"
    return f"{val:+.1f}%"


# ---------------------------------------------------------------------------
# Page 1: Command Center
# ---------------------------------------------------------------------------
def page_command_center():
    st.title("Soy Complex — Command Center")

    data = load_command_center()
    if _check_error(data, "Command Center"):
        return
    legs = data["legs"]
    crush = data["crush"]
    signals = data["signals"]
    metrics = data["key_metrics"]

    # --- Top row: 3 soy legs as metric cards ---
    cols = st.columns(3)
    for i, leg in enumerate(legs):
        with cols[i]:
            if not leg.get("available"):
                st.metric(leg["name"], "No data")
                continue

            daily = leg.get("daily_chg", 0)
            unit = leg.get("unit", "USD/MT")
            st.metric(
                f"{leg['name']} ({unit})",
                f"{leg['close']:,.1f}",
                delta=_delta_str(daily),
            )

            # Sub-metrics
            sub_cols = st.columns(3)
            rsi = leg.get("rsi")
            if pd.notna(rsi):
                rsi_label = "RSI"
                if rsi > 70:
                    rsi_label = "RSI (OB)"
                elif rsi < 30:
                    rsi_label = "RSI (OS)"
                sub_cols[0].metric(rsi_label, f"{rsi:.0f}")

            trend = leg.get("trend", "N/A")
            sub_cols[1].metric("Trend", trend)

            hv = leg.get("hv_20")
            if pd.notna(hv):
                sub_cols[2].metric("Vol 20d", f"{hv:.0f}%")

    st.divider()

    # --- Second row: Crush + Key Metrics ---
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if crush.get("available"):
            val_mt = crush.get("value_usd_mt")
            trend_label = crush.get("trend", "")
            st.metric(
                "Crush Spread (USD/MT)",
                f"${val_mt:,.1f}" if val_mt else "N/A",
                delta=trend_label if trend_label else None,
            )
        else:
            st.metric("Crush Spread", "N/A")

    with col2:
        brl = metrics.get("brl_usd")
        brl_chg = metrics.get("brl_weekly_chg")
        if brl:
            st.metric("BRL/USD", f"{brl:.4f}", delta=_delta_str(brl_chg) if brl_chg else None)
        else:
            st.metric("BRL/USD", "N/A")

    with col3:
        dollar = metrics.get("dollar_index")
        if dollar:
            st.metric("Dollar Index", f"{dollar:.1f}")
        else:
            st.metric("Dollar Index", "N/A")

    with col4:
        cny = metrics.get("cny_usd")
        if cny:
            st.metric("CNY/USD", f"{cny:.4f}")
        else:
            st.metric("CNY/USD", "N/A")

    st.divider()

    # --- Signals ---
    if signals:
        st.subheader(f"Active Signals ({len(signals)})")
        for s in signals[:10]:
            severity = s.get("severity", "info")
            icon = {"alert": "🔴", "warning": "🟡", "info": "🔵"}.get(severity, "⚪")
            st.markdown(f"{icon} **[{severity.upper()}]** {s['description']}")
    else:
        st.info("No active signals across soy complex")

    st.caption("Source: CME/CBOT/ICE via yfinance, FRED, CFTC COT")


# ---------------------------------------------------------------------------
# Page 2: Technicals
# ---------------------------------------------------------------------------
def _render_technical_chart(df, leg_name, signals_list):
    """Render a candlestick+RSI+MACD chart for one soy leg."""
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=(leg_name, "RSI", "MACD"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df.index, open=df["Open"], high=df["High"],
            low=df["Low"], close=df["Close"], name="Price",
        ),
        row=1, col=1,
    )

    # Moving averages
    for ma, color in [("MA_20", "orange"), ("MA_50", "blue"), ("MA_200", "red")]:
        if ma in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df[ma], name=ma, line=dict(width=1, color=color)),
                row=1, col=1,
            )

    # Bollinger Bands
    if "BB_Upper" in df.columns and "BB_Lower" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["BB_Upper"], name="BB Upper",
                       line=dict(width=1, dash="dot", color="gray")),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=df.index, y=df["BB_Lower"], name="BB Lower",
                       line=dict(width=1, dash="dot", color="gray"),
                       fill="tonexty", fillcolor="rgba(128,128,128,0.1)"),
            row=1, col=1,
        )

    # Volume
    if "Volume" in df.columns:
        colors = ["green" if c >= o else "red" for c, o in zip(df["Close"], df["Open"])]
        fig.add_trace(
            go.Bar(x=df.index, y=df["Volume"], name="Volume", marker_color=colors, opacity=0.3),
            row=1, col=1,
        )

    # RSI
    if "RSI" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["RSI"], name="RSI", line=dict(color="purple")),
            row=2, col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    # MACD
    if "MACD" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["MACD"], name="MACD", line=dict(color="blue")),
            row=3, col=1,
        )
        if "MACD_Signal" in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df["MACD_Signal"], name="Signal", line=dict(color="orange")),
                row=3, col=1,
            )
        if "MACD_Histogram" in df.columns:
            hist_colors = ["green" if v >= 0 else "red" for v in df["MACD_Histogram"]]
            fig.add_trace(
                go.Bar(x=df.index, y=df["MACD_Histogram"], name="Histogram", marker_color=hist_colors),
                row=3, col=1,
            )

    fig.update_layout(
        height=900,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        yaxis_title="USD/MT",
    )
    st.plotly_chart(fig, use_container_width=True)

    # Signals for this leg
    leg_signals = [s for s in signals_list if s.get("commodity") == leg_name]
    if leg_signals:
        st.subheader(f"Signals — {leg_name}")
        for s in leg_signals:
            severity = s.get("severity", "info")
            icon = {"alert": "🔴", "warning": "🟡", "info": "🔵"}.get(severity, "⚪")
            st.markdown(f"{icon} {s['description']}")


def page_technicals():
    st.title("Soy Complex — Technical Analysis")

    data = load_technicals()
    per_leg = data["per_leg"]
    per_leg_mt = data.get("per_leg_mt", {})

    if not per_leg:
        st.warning("No price data. Run `python main.py` first.")
        return

    # Use tabs for all 3 legs instead of dropdown
    available_legs = [l for l in ["Soybeans", "Soybean Oil", "Soybean Meal"] if l in per_leg]
    tabs = st.tabs(available_legs)

    for tab, leg_name in zip(tabs, available_legs):
        with tab:
            df = per_leg_mt.get(leg_name, per_leg[leg_name])
            _render_technical_chart(df, leg_name, data["signals"])

            # CSV download
            csv = df.reset_index().to_csv(index=False)
            st.download_button(
                f"Download {leg_name} Data (CSV)",
                data=csv,
                file_name=f"{leg_name.lower().replace(' ', '_')}_technicals.csv",
                mime="text/csv",
            )

    st.caption("Source: CME/CBOT via yfinance | Prices in USD/MT")


# ---------------------------------------------------------------------------
# Page 3: Supply & Demand
# ---------------------------------------------------------------------------
def page_supply_demand():
    st.title("Soy Complex — Supply & Demand")

    supply = load_supply()
    demand = load_demand()

    tab1, tab2, tab3 = st.tabs(["Soy Complex", "Competing Crops", "Emerging Markets"])

    # ── SOY COMPLEX TAB ──
    with tab1:
        # Supply side
        st.subheader("WASDE Monthly Estimates")
        wasde = supply.get("wasde", {})
        if wasde:
            for commodity, attrs in wasde.items():
                if "SOYBEAN" not in commodity.upper():
                    continue
                st.markdown(f"**{commodity}**")
                for attr_name, info in attrs.items():
                    val = info.get("value")
                    if pd.isna(val):
                        continue
                    rev = info.get("revision")
                    unit = info.get("unit", "")
                    if rev is not None and rev != 0:
                        direction = "UP" if rev > 0 else "DOWN"
                        st.markdown(
                            f"- {attr_name}: **{val:,.0f}** {unit} "
                            f"(revised {direction} {abs(rev):,.0f} vs prior month)"
                        )
                    else:
                        st.markdown(f"- {attr_name}: **{val:,.0f}** {unit}")
        else:
            st.info("No WASDE data available")

        st.divider()

        # CONAB vs USDA
        st.subheader("Brazil: CONAB vs USDA")
        conab = supply.get("conab_vs_usda", {})
        if conab.get("conab_production"):
            cols = st.columns(3)
            cols[0].metric("CONAB (Brazil)", f"{conab['conab_production']:,.0f} 1000 MT")
            if conab.get("usda_production"):
                cols[1].metric("USDA (Brazil)", f"{conab['usda_production']:,.0f} 1000 MT")
                gap = conab.get("gap", 0)
                cols[2].metric("Gap", f"{gap:+,.0f} 1000 MT")
        else:
            st.info("No CONAB data available")

        st.divider()

        # Crop Progress
        st.subheader("US Crop Conditions")
        crop = supply.get("crop_progress", {})
        if crop.get("condition"):
            for item in crop["condition"]:
                st.markdown(f"- {item['desc']}: **{item['value']}%**")
        if crop.get("progress"):
            for item in crop["progress"]:
                st.markdown(f"- {item['desc']}: **{item['value']}%**")
        if not crop:
            st.info("No crop progress data available")

        st.divider()

        # Demand side
        st.subheader("China Buying Pace")
        china = demand.get("china_buying", {})
        if china:
            cols = st.columns(len(china))
            for i, (commodity, info) in enumerate(china.items()):
                with cols[i]:
                    st.metric(
                        f"{commodity}",
                        f"{info['net_sales']:,.0f} MT",
                        delta=f"{info['pct_of_total']:.0f}% of total",
                    )
        else:
            st.info("No China buying data")

        st.divider()

        st.subheader("Weekly Export Sales")
        es = demand.get("export_sales", {})
        if es:
            for commodity, info in es.items():
                week_str = info["week_ending"].strftime("%m/%d") if hasattr(info["week_ending"], "strftime") else str(info["week_ending"])
                st.markdown(f"**{commodity}** (w/e {week_str})")
                st.markdown(f"- Net sales: **{info['net_sales']:,.0f} MT** | Exports: **{info['exports']:,.0f} MT**")
                if info.get("top_buyers"):
                    buyers = ", ".join(f"{b['country']} ({b['mt']:,.0f})" for b in info["top_buyers"])
                    st.markdown(f"- Top buyers: {buyers}")
        else:
            st.info("No export sales data")

        st.divider()

        st.subheader("Biofuel & Energy (Soy Oil Demand Driver)")
        bio = demand.get("biofuel", {})
        if bio:
            cols = st.columns(len(bio))
            for i, (name, info) in enumerate(bio.items()):
                with cols[i]:
                    st.metric(
                        name,
                        f"{info['value']:,.2f}",
                        delta=_delta_str(info.get("chg_pct")),
                    )
        else:
            st.info("No EIA data (set EIA_API_KEY to enable)")

    # ── COMPETING CROPS TAB ──
    with tab2:
        st.subheader("Competing Crops (WASDE)")
        if wasde:
            for commodity, attrs in wasde.items():
                if "SOYBEAN" in commodity.upper():
                    continue
                st.markdown(f"**{commodity}**")
                for attr_name, info in attrs.items():
                    val = info.get("value")
                    if pd.isna(val):
                        continue
                    rev = info.get("revision")
                    unit = info.get("unit", "")
                    if rev is not None and rev != 0:
                        direction = "UP" if rev > 0 else "DOWN"
                        st.markdown(f"- {attr_name}: **{val:,.0f}** {unit} (revised {direction} {abs(rev):,.0f})")
                    else:
                        st.markdown(f"- {attr_name}: **{val:,.0f}** {unit}")
        else:
            st.info("No WASDE data")

        st.divider()

        st.subheader("Global Supply (PSD)")
        psd = supply.get("psd_highlights", [])
        if psd:
            for item in psd:
                st.markdown(
                    f"- {item['country']} {item['commodity']} {item['attribute']}: "
                    f"**{item['value']:,.0f}** {item.get('unit', '')}"
                )
        else:
            st.info("No PSD data available")

        st.divider()

        st.subheader("DCE China vs CBOT")
        dce = demand.get("dce_comparison", {})
        if dce:
            for name, info in dce.items():
                parts = [f"DCE: CNY {info['dce_close']:,.0f}"]
                if info.get("cbot_close"):
                    parts.append(f"CBOT: {info['cbot_close']:,.2f} USD")
                st.markdown(f"- {name}: {' | '.join(parts)}")
        else:
            st.info("No DCE data")

        st.divider()

        st.subheader("Export Inspections")
        insp = demand.get("inspections", {})
        if insp:
            for commodity, info in insp.items():
                st.markdown(f"- {commodity}: **{info['volume_mt']:,.0f} MT** inspected")
        else:
            st.info("No inspections data")

    # ── EMERGING MARKETS TAB ──
    with tab3:
        st.subheader("Emerging Soybean Markets")
        em_data = load_emerging_markets()
        countries = em_data.get("countries", {})

        if not countries:
            st.info("No emerging market data. Run `python main.py` to fetch PSD data.")
        else:
            for country_name, info in countries.items():
                st.markdown(f"### {country_name}")

                # PSD production data
                psd_em = info.get("psd", {})
                if psd_em:
                    year = info.get("psd_year", "")
                    cols = st.columns(min(len(psd_em), 4))
                    key_attrs = ["Production", "Imports", "Exports", "Ending Stocks"]
                    col_idx = 0
                    for attr in key_attrs:
                        if attr in psd_em:
                            vals = psd_em[attr]
                            yoy = vals.get("yoy_pct")
                            with cols[col_idx % len(cols)]:
                                st.metric(
                                    f"{attr} ({year})",
                                    f"{vals['value']:,.0f} {vals.get('unit', '')}",
                                    delta=f"{yoy:+.1f}% YoY" if yoy is not None else None,
                                )
                            col_idx += 1

                # Currency
                currency = info.get("currency", {})
                if currency:
                    pair = currency["pair"]
                    close = currency["close"]
                    wk = currency.get("weekly_chg")
                    mo = currency.get("monthly_chg")
                    col1, col2 = st.columns(2)
                    col1.metric(pair, f"{close:.4f}", delta=_delta_str(wk) if wk is not None else None)
                    if mo is not None:
                        col2.metric("30d Change", _delta_str(mo))

                # Weather alerts
                weather_list = info.get("weather", [])
                active_alerts = [w for w in weather_list if w.get("alert")]
                if active_alerts:
                    for w in active_alerts:
                        icon = {"Heavy Rain": "🌧️", "Dry": "☀️", "Extreme Heat": "🔥"}.get(w["alert"], "⚠️")
                        st.markdown(f"{icon} **{w['region']}**: {w['alert']} — Max {w.get('temp_max', 'N/A')}C, Precip {w.get('precip', 0):.0f}mm")
                elif weather_list:
                    st.success(f"No active weather alerts in {country_name}")

                st.divider()


# ---------------------------------------------------------------------------
# Page 4: Relative Value
# ---------------------------------------------------------------------------
def page_relative_value():
    st.title("Soy Complex — Relative Value")

    data = load_relative_value()

    # --- Crush Spread Chart ---
    st.subheader("Crush Spread")
    crush = data.get("crush")
    if crush:
        spread_df = crush["series"]
        spread_df["Date"] = pd.to_datetime(spread_df["Date"])

        # Convert crush spread to USD/MT for charting
        from processing.units import to_metric_tons, CONVERSION_FACTORS
        soy_factor = CONVERSION_FACTORS.get("Soybeans", 1)
        spread_mt = spread_df["crush_spread"] * soy_factor

        col1, col2 = st.columns([1, 3])
        with col1:
            st.metric(
                "Current (USD/MT)",
                f"${crush['current_usd_mt']:,.1f}",
                delta="Profitable" if crush["profitable"] else "Negative",
            )

        with col2:
            fig = go.Figure()
            # 1Y range shading
            if crush.get("min_1y") is not None:
                fig.add_hline(y=crush["avg_1y"], line_dash="dot", line_color="blue",
                              annotation_text=f"1Y avg: ${crush['avg_1y']:,.1f}")
                fig.add_hrect(y0=crush["min_1y"], y1=crush["max_1y"],
                              fillcolor="rgba(100,149,237,0.1)", line_width=0,
                              annotation_text="1Y range")
            fig.add_trace(
                go.Scatter(
                    x=spread_df["Date"],
                    y=spread_mt,
                    mode="lines", name="Crush Spread",
                    line=dict(color="black", width=2),
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=spread_df["Date"],
                    y=[max(0, v) for v in spread_mt],
                    fill="tozeroy", fillcolor="rgba(0,128,0,0.2)",
                    line=dict(width=0), name="Profitable",
                )
            )
            fig.add_trace(
                go.Scatter(
                    x=spread_df["Date"],
                    y=[min(0, v) for v in spread_mt],
                    fill="tozeroy", fillcolor="rgba(255,0,0,0.2)",
                    line=dict(width=0), name="Negative",
                )
            )
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            fig.update_layout(height=350, xaxis_title="", yaxis_title="USD/MT")
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Crush spread needs Soybeans + Oil + Meal data")

    st.divider()

    # --- Oil/Meal Ratio + Soy Oil Share ---
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Oil/Meal Ratio")
        omr = data.get("oil_meal_ratio")
        if omr:
            st.metric("Current", f"{omr['current']:.3f}", delta=f"60d avg: {omr['avg_60d']:.3f}")
            fig = go.Figure()
            if omr.get("min_1y") is not None:
                fig.add_hrect(y0=omr["min_1y"], y1=omr["max_1y"],
                              fillcolor="rgba(255,165,0,0.08)", line_width=0,
                              annotation_text="1Y range")
            fig.add_trace(
                go.Scatter(x=omr["series"].index, y=omr["series"], mode="lines",
                           name="Oil/Meal Ratio", line=dict(color="darkorange"))
            )
            fig.add_hline(y=omr["avg_60d"], line_dash="dash", line_color="gray",
                          annotation_text="60d avg")
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need Oil + Meal data")

    with col2:
        st.subheader("Soy Oil Share of Crush")
        share = data.get("soy_oil_share")
        if share:
            st.metric("Oil % of Product Value", f"{share:.1f}%")
            st.caption("Higher = biodiesel demand pulling oil; Lower = feed demand pulling meal")
        else:
            st.info("Need all 3 legs")

    st.divider()

    # --- Soy Oil vs Palm Oil ---
    st.subheader("Soy Oil vs Palm Oil")
    ovp = data.get("oil_vs_palm")
    if ovp:
        cols = st.columns(2)
        soy_unit = ovp.get("soy_oil_unit", "USD/MT")
        palm_unit = ovp.get("palm_oil_unit", "MYR/MT")
        cols[0].metric(
            f"Soybean Oil ({soy_unit})",
            f"{ovp['soy_oil']:,.1f}",
            delta=_delta_str(ovp.get("soy_oil_weekly_chg")),
        )
        cols[1].metric(
            f"Palm Oil ({palm_unit})",
            f"{ovp['palm_oil']:,.1f}",
            delta=_delta_str(ovp.get("palm_oil_weekly_chg")),
        )
    else:
        st.info("Need Soybean Oil + Palm Oil data")

    st.divider()

    # --- Bean/Corn Ratio ---
    st.subheader("Soybean/Corn Ratio (Acreage Signal)")
    bcr = data.get("bean_corn_ratio")
    if bcr:
        col1, col2 = st.columns([1, 3])
        with col1:
            st.metric("Current", f"{bcr['current']:.2f}")
            st.metric("1Y Average", f"{bcr['avg_1y']:.2f}")
            if bcr["current"] > bcr["avg_1y"]:
                st.caption("Above avg = soybeans relatively expensive vs corn = may attract more soy acres")
            else:
                st.caption("Below avg = corn relatively expensive = may lose soy acres to corn")
        with col2:
            fig = go.Figure()
            if bcr.get("min_1y") is not None:
                fig.add_hrect(y0=bcr["min_1y"], y1=bcr["max_1y"],
                              fillcolor="rgba(139,69,19,0.08)", line_width=0,
                              annotation_text="1Y range")
            fig.add_trace(
                go.Scatter(x=bcr["series"].index, y=bcr["series"], mode="lines",
                           name="Bean/Corn Ratio", line=dict(color="saddlebrown"))
            )
            fig.add_hline(y=bcr["avg_1y"], line_dash="dash", line_color="gray",
                          annotation_text="1Y avg")
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Need Soybeans + Corn data")


# ---------------------------------------------------------------------------
# Page 5: Risk Monitor
# ---------------------------------------------------------------------------
def page_risk_monitor():
    st.title("Soy Complex — Risk Monitor")

    data = load_risk()

    # --- Currencies ---
    st.subheader("Key Currencies")
    currencies = data.get("currencies", {})
    if currencies:
        # Split into rows of 3 to avoid cramming too many columns
        pairs = list(currencies.items())
        for row_start in range(0, len(pairs), 3):
            row_pairs = pairs[row_start:row_start + 3]
            cols = st.columns(len(row_pairs))
            for i, (pair, info) in enumerate(row_pairs):
                with cols[i]:
                    st.metric(
                        pair,
                        f"{info['close']:.4f}",
                        delta=_delta_str(info.get("weekly_chg")),
                    )
                    if info.get("monthly_chg") is not None:
                        st.caption(f"30d: {info['monthly_chg']:+.1f}%")
    else:
        st.info("No currency data")

    st.divider()

    # --- COT Positioning ---
    st.subheader("COT Positioning — Soy Complex")
    cot = data.get("cot", {})
    if cot:
        # Bar chart
        fig = go.Figure()
        commodities = list(cot.keys())
        comm_nets = [cot[c].get("commercial_net", 0) or 0 for c in commodities]
        spec_nets = [cot[c].get("spec_net", 0) or 0 for c in commodities]

        fig.add_trace(go.Bar(x=commodities, y=comm_nets, name="Commercials (net)", marker_color="steelblue"))
        fig.add_trace(go.Bar(x=commodities, y=spec_nets, name="Speculators (net)", marker_color="coral"))
        fig.update_layout(barmode="group", height=400, yaxis_title="Net Contracts")
        st.plotly_chart(fig, use_container_width=True)

        # Week-over-week changes
        for leg, info in cot.items():
            chg = info.get("spec_net_chg")
            spec = info.get("spec_net", 0) or 0
            direction = "long" if spec > 0 else "short"
            parts = [f"Specs net {direction} {abs(spec):,.0f}"]
            if chg is not None:
                parts.append(f"(WoW: {chg:+,.0f})")
            st.markdown(f"- **{leg}**: {' '.join(parts)}")
    else:
        st.info("No COT data")

    st.divider()

    # --- Weather Alerts ---
    st.subheader("Weather — Soy Growing Regions")
    alerts = data.get("weather_alerts", [])
    if alerts:
        for a in alerts:
            icon = {"Heavy Rain": "🌧️", "Dry": "☀️", "Extreme Heat": "🔥"}.get(a["alert"], "⚠️")
            st.markdown(
                f"{icon} **{a['region']}**: {a['alert']} — "
                f"Max {a['temp_max']:.0f}C, Precip {a['precip']:.0f}mm"
            )
    else:
        st.success("No active weather alerts in soy regions")

    st.divider()

    # --- Options Sentiment ---
    st.subheader("Options Sentiment")
    options = data.get("options", {})
    if options:
        cols = st.columns(len(options))
        for i, (leg, info) in enumerate(options.items()):
            with cols[i]:
                pc = info.get("put_call_ratio")
                if pd.notna(pc):
                    sentiment = "Bearish" if pc > 1.2 else ("Bullish" if pc < 0.7 else "Neutral")
                    st.metric(leg, f"P/C: {pc:.2f}", delta=sentiment)
                else:
                    st.metric(leg, "N/A")
    else:
        st.info("No options data (experimental — may not be available for ag futures)")

    st.divider()

    # --- Correlations ---
    st.subheader("Rolling Correlations")
    try:
        from analysis.correlations import rolling_correlation, commodity_vs_currency
        from processing.combiner import read_prices, read_currencies

        all_prices = read_prices()
        all_currencies = read_currencies()

        # Build price series for soy legs + corn
        _corr_series = {}
        for name in ["Soybeans", "Soybean Oil", "Corn"]:
            subset = all_prices[all_prices["commodity"] == name].copy() if not all_prices.empty else pd.DataFrame()
            if not subset.empty:
                subset["Date"] = pd.to_datetime(subset["Date"])
                subset = subset.set_index("Date").sort_index()
                _corr_series[name] = subset["Close"]

        # BRL
        brl_df = pd.DataFrame()
        if not all_currencies.empty:
            brl_sub = all_currencies[all_currencies["pair"] == "BRL/USD"].copy()
            if not brl_sub.empty:
                brl_sub["Date"] = pd.to_datetime(brl_sub["Date"])
                brl_df = brl_sub.set_index("Date").sort_index()

        pairs = []
        if "Soybeans" in _corr_series and not brl_df.empty:
            pairs.append(("Soybeans vs BRL/USD", _corr_series["Soybeans"], brl_df["Close"]))
        if "Soybeans" in _corr_series and "Soybean Oil" in _corr_series:
            pairs.append(("Soybeans vs Soy Oil", _corr_series["Soybeans"], _corr_series["Soybean Oil"]))
        if "Soybeans" in _corr_series and "Corn" in _corr_series:
            pairs.append(("Soybeans vs Corn", _corr_series["Soybeans"], _corr_series["Corn"]))

        if pairs:
            fig = go.Figure()
            colors = ["crimson", "darkorange", "steelblue"]
            for i, (label, sa, sb) in enumerate(pairs):
                rc = rolling_correlation(sa, sb, window=60)
                if not rc.empty:
                    fig.add_trace(
                        go.Scatter(x=rc.index, y=rc, mode="lines", name=label,
                                   line=dict(color=colors[i % len(colors)], width=2))
                    )
            fig.add_hline(y=0, line_dash="dash", line_color="gray")
            fig.update_layout(height=400, yaxis_title="60d Rolling Correlation",
                              yaxis_range=[-1, 1])
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Need price + currency data for correlations")
    except Exception:
        st.info("Correlation data unavailable")


# ---------------------------------------------------------------------------
# Page 6: Forward Curves
# ---------------------------------------------------------------------------
def page_forward_curves():
    st.title("Soy Complex — Forward Curves")

    data = load_forward_curves()

    if not data:
        st.warning("No forward curve data. Run `python main.py` first.")
        return

    for leg in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
        if leg not in data:
            continue

        leg_data = data[leg]
        curve_df_mt = leg_data.get("curve_data_mt", leg_data["curve_data"])
        analysis = leg_data.get("analysis", {})
        cal = leg_data.get("calendar_spread", {})
        unit = leg_data.get("unit", "USD/MT")

        st.subheader(leg)

        # Metrics row — convert front/back prices to MT
        if analysis:
            from processing.units import to_metric_tons
            front_mt = to_metric_tons(analysis.get("front_price", 0), leg)
            back_mt = to_metric_tons(analysis.get("back_price", 0), leg)
            cols = st.columns(4)
            cols[0].metric("Structure", analysis.get("structure", "N/A").title())
            cols[1].metric("Front", f"{front_mt:,.1f}" if front_mt else "N/A")
            cols[2].metric("Back", f"{back_mt:,.1f}" if back_mt else "N/A")
            spread_pct = analysis.get("spread_pct", 0)
            cols[3].metric("Spread", f"{spread_pct:+.1f}%")

        # Curve chart (MT-converted)
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=curve_df_mt["label"], y=curve_df_mt["close"],
                mode="lines+markers", name=leg,
                line=dict(width=3), marker=dict(size=10),
            )
        )
        front_price_mt = curve_df_mt.iloc[0]["close"]
        fig.add_hline(y=front_price_mt, line_dash="dash", line_color="gray",
                      annotation_text=f"Front: {front_price_mt:,.1f}")
        fig.update_layout(height=350, xaxis_title="Contract", yaxis_title=unit)
        st.plotly_chart(fig, use_container_width=True)

        # Calendar spread
        if cal:
            st.caption(
                f"Front spread: {cal.get('near_label', '')} → {cal.get('far_label', '')}: "
                f"{cal.get('spread', 0):+.2f} ({cal.get('spread_pct', 0):+.1f}%)"
            )

        st.divider()


# ---------------------------------------------------------------------------
# Page 7: Seasonal
# ---------------------------------------------------------------------------
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def page_seasonal():
    st.title("Soy Complex — Seasonal Patterns")

    data = load_seasonal()

    if not data:
        st.warning("No seasonal data. Run `python main.py` first.")
        return

    for leg in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
        if leg not in data:
            continue

        leg_data = data[leg]
        monthly = leg_data.get("monthly")
        vs_seasonal = leg_data.get("vs_seasonal", {})

        st.subheader(leg)

        # Current vs seasonal metric
        unit = leg_data.get("unit", "USD/MT")
        if vs_seasonal:
            cols = st.columns(3)
            cols[0].metric(f"Current ({unit})", f"{vs_seasonal['current_price']:,.1f}")
            cols[1].metric(f"Seasonal Avg ({unit})", f"{vs_seasonal['seasonal_avg']:,.1f}")
            dev = vs_seasonal.get("deviation_pct", 0)
            cols[2].metric("vs Seasonal", f"{dev:+.1f}%",
                           delta="Above" if dev > 0 else "Below")

        # Monthly average bar chart
        if monthly is not None and not monthly.empty:
            labels = [MONTH_NAMES[m - 1] for m in monthly["month"]]
            fig = go.Figure()
            # Min/max range as error bars
            fig.add_trace(
                go.Bar(
                    x=labels,
                    y=monthly["avg_close"],
                    name="Avg Close",
                    marker_color="steelblue",
                    error_y=dict(
                        type="data",
                        symmetric=False,
                        array=monthly["max_close"] - monthly["avg_close"],
                        arrayminus=monthly["avg_close"] - monthly["min_close"],
                        color="rgba(70,130,180,0.4)",
                    ),
                )
            )
            # Highlight current month
            if vs_seasonal:
                from datetime import datetime
                current_month_idx = datetime.now().month - 1
                if current_month_idx < len(labels):
                    fig.add_trace(
                        go.Scatter(
                            x=[labels[current_month_idx]],
                            y=[vs_seasonal["current_price"]],
                            mode="markers",
                            name="Current",
                            marker=dict(color="red", size=14, symbol="diamond"),
                        )
                    )

            fig.update_layout(height=350, yaxis_title=unit,
                              xaxis_title="Month", showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()


# ---------------------------------------------------------------------------
# Page 8: Briefing
# ---------------------------------------------------------------------------
def page_briefing():
    st.title("Soy Complex — Full Briefing")

    briefing = load_briefing()
    st.text(briefing)

    # Download briefing as text
    st.download_button(
        label="Download Briefing (TXT)",
        data=briefing,
        file_name="mirror_market_briefing.txt",
        mime="text/plain",
    )

    # Data health
    with st.expander("Data Health"):
        try:
            from analysis.health import run_health_check
            health = run_health_check()
            issues = health.get("issues", [])
            if not issues:
                st.success("All systems green")
            else:
                for issue in issues:
                    sev = issue.get("severity", "info")
                    if sev == "critical":
                        st.error(f"[{issue['table']}] {issue['commodity']}: {issue['message']}")
                    else:
                        st.warning(f"[{issue['table']}] {issue['commodity']}: {issue['message']}")
        except Exception:
            st.info("Health check unavailable")

    st.caption("Source: All 16 data layers — see About page for details")


# ---------------------------------------------------------------------------
# Page 9: About & Methodology
# ---------------------------------------------------------------------------
def page_about():
    st.title("About Mirror Market")

    st.markdown("""
### Overview

Mirror Market is a commodity market intelligence platform focused on the
global soybean complex (Soybeans, Soybean Oil, Soybean Meal). It pulls
data from 16 source layers into a database, runs technical and fundamental
analysis, and presents actionable intelligence through this dashboard.

### Data Sources

| Layer | Source | Frequency | API Key Required |
|-------|--------|-----------|-----------------|
| Commodity Prices | CME/CBOT/ICE via yfinance | Daily | No |
| USDA Crop Data | USDA NASS QuickStats | Annual | Yes |
| Economic Data | FRED (Federal Reserve) | Varies | Yes |
| COT Positioning | CFTC via cot_reports | Weekly | No |
| Weather | Open-Meteo | Daily | No |
| Global Supply/Demand | USDA FAS PSD | Monthly | No |
| Currencies | yfinance | Daily | No |
| World Bank Prices | World Bank Pink Sheet | Monthly | No |
| DCE Chinese Futures | AKShare | Daily | No |
| Export Sales | USDA FAS ESR | Weekly | Yes |
| Forward Curves | yfinance | Daily | No |
| WASDE Estimates | USDA NASS | Monthly | Yes |
| Biofuel/Energy | EIA | Weekly/Monthly | Yes |
| Crush/Inspections | USDA | Weekly/Monthly | Yes |
| Brazil Estimates | CONAB | Monthly | No |
| Options Sentiment | yfinance | Daily | No |

### Price Units

All prices are displayed in **USD per Metric Ton (USD/MT)** for international
comparability. Raw exchange data is stored in native units (cents/bushel,
cents/lb, etc.) and converted at the display layer only.

**Key conversion factors:**
- Soybeans: 1 MT = 36.7437 bushels
- Soybean Oil: 1 MT = 2,204.62 lbs
- Soybean Meal: 1 short ton = 0.907185 MT
- Palm Oil (BMD): Already in MYR/MT

### Methodology

**Technical Analysis:** SMA (20/50/200), RSI (Wilder smoothing), MACD (12/26/9),
Bollinger Bands (20-day, 2 std dev), historical volatility.

**Crush Spread:** Oil price x 11 + Meal price x 2.2 - Soybean price (CBOT board crush).

**Signals:** MA crossovers, volume spikes (2x 20-day avg), RSI extremes/divergence,
MACD crossovers, Bollinger squeeze. Severity: alert > warning > info.

**Emerging Markets:** South Africa, India, Nigeria — PSD production data, local
currency tracking (ZAR, INR, NGN), and regional weather monitoring.

### Disclaimer

This platform is for **informational and educational purposes only**. It should
not be used as the sole basis for trading or investment decisions. All data is
sourced from public APIs and may be delayed, incomplete, or contain errors.
Past performance does not guarantee future results.
""")

    st.caption("Mirror Market v2.0 — Built with Python, Streamlit, and Plotly")


# ---------------------------------------------------------------------------
# Page router
# ---------------------------------------------------------------------------
if page == "Command Center":
    page_command_center()
elif page == "Technicals":
    page_technicals()
elif page == "Supply & Demand":
    page_supply_demand()
elif page == "Relative Value":
    page_relative_value()
elif page == "Risk Monitor":
    page_risk_monitor()
elif page == "Forward Curves":
    page_forward_curves()
elif page == "Seasonal":
    page_seasonal()
elif page == "Briefing":
    page_briefing()
elif page == "About":
    page_about()
