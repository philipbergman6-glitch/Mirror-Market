"""
Mirror Market — Interactive Dashboard.

A Streamlit + Plotly dashboard with 7 pages for visualising all market data.
Run with:  streamlit run app/dashboard.py

Key concepts for learning:
    - Streamlit turns Python scripts into web apps — no HTML/JS needed
    - Plotly creates interactive charts (zoom, pan, hover)
    - st.sidebar for navigation between pages
    - @st.cache_data caches database reads so pages load fast
    - All data comes from the same read_*() functions used by the briefing
"""

import sys
import os

# Ensure the project root is on the Python path so imports work
# regardless of where streamlit is launched from
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import streamlit as st
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

from processing.combiner import (
    read_prices,
    read_cot,
    read_currencies,
    read_weather,
    read_economic,
    read_forward_curve,
    read_export_sales,
    read_freshness,
)
from analysis.technical import compute_all_technicals
from analysis.spreads import compute_crush_spread
from analysis.correlations import commodity_correlation_matrix
from analysis.forward_curve import analyze_curve
from analysis.briefing import generate_briefing

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Mirror Market",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Sidebar navigation
# ---------------------------------------------------------------------------
PAGES = [
    "Overview",
    "Price Charts",
    "Forward Curve",
    "Crush Spread",
    "COT Positioning",
    "Weather",
    "Correlations",
]

page = st.sidebar.radio("Navigate", PAGES)

st.sidebar.divider()
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()


# ---------------------------------------------------------------------------
# Cached data loaders
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_prices():
    return read_prices()


@st.cache_data(ttl=300)
def load_cot():
    return read_cot()


@st.cache_data(ttl=300)
def load_forward_curves():
    return read_forward_curve()


@st.cache_data(ttl=300)
def load_weather():
    return read_weather()


@st.cache_data(ttl=300)
def load_freshness():
    return read_freshness()


@st.cache_data(ttl=600)
def load_briefing():
    return generate_briefing()


# ---------------------------------------------------------------------------
# Page 1: Overview
# ---------------------------------------------------------------------------
def page_overview():
    st.title("Mirror Market — Overview")

    # Data freshness status
    freshness = load_freshness()
    if not freshness.empty:
        st.subheader("Data Freshness")
        display = freshness.copy()
        if "last_success" in display.columns:
            display["last_success"] = pd.to_datetime(display["last_success"]).dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(display, width="stretch", hide_index=True)

    # Full text briefing
    st.subheader("Daily Briefing")
    briefing = load_briefing()
    st.text(briefing)


# ---------------------------------------------------------------------------
# Page 2: Price Charts
# ---------------------------------------------------------------------------
def page_price_charts():
    st.title("Price Charts")

    all_prices = load_prices()
    if all_prices.empty:
        st.warning("No price data available. Run `python main.py` first.")
        return

    commodities = sorted(all_prices["commodity"].unique())
    selected = st.selectbox("Select Commodity", commodities)

    df = all_prices[all_prices["commodity"] == selected].copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.set_index("Date").sort_index()

    # Compute technicals
    df = compute_all_technicals(df)

    # Create subplot figure: candlestick + volume, RSI, MACD
    fig = make_subplots(
        rows=3, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        row_heights=[0.6, 0.2, 0.2],
        subplot_titles=(selected, "RSI", "MACD"),
    )

    # Candlestick
    fig.add_trace(
        go.Candlestick(
            x=df.index,
            open=df["Open"],
            high=df["High"],
            low=df["Low"],
            close=df["Close"],
            name="Price",
        ),
        row=1, col=1,
    )

    # Moving averages
    for ma, color in [("MA_20", "orange"), ("MA_50", "blue"), ("MA_200", "red")]:
        if ma in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df.index, y=df[ma],
                    name=ma, line=dict(width=1, color=color),
                ),
                row=1, col=1,
            )

    # Bollinger Bands
    if "BB_Upper" in df.columns and "BB_Lower" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["BB_Upper"],
                name="BB Upper", line=dict(width=1, dash="dot", color="gray"),
            ),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["BB_Lower"],
                name="BB Lower", line=dict(width=1, dash="dot", color="gray"),
                fill="tonexty", fillcolor="rgba(128,128,128,0.1)",
            ),
            row=1, col=1,
        )

    # Volume as bars on price chart
    if "Volume" in df.columns:
        colors = ["green" if c >= o else "red"
                  for c, o in zip(df["Close"], df["Open"])]
        fig.add_trace(
            go.Bar(
                x=df.index, y=df["Volume"],
                name="Volume", marker_color=colors, opacity=0.3,
            ),
            row=1, col=1,
        )

    # RSI
    if "RSI" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["RSI"],
                name="RSI", line=dict(color="purple"),
            ),
            row=2, col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color="red", row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color="green", row=2, col=1)

    # MACD
    if "MACD" in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df.index, y=df["MACD"],
                name="MACD", line=dict(color="blue"),
            ),
            row=3, col=1,
        )
        if "MACD_Signal" in df.columns:
            fig.add_trace(
                go.Scatter(
                    x=df.index, y=df["MACD_Signal"],
                    name="Signal", line=dict(color="orange"),
                ),
                row=3, col=1,
            )
        if "MACD_Histogram" in df.columns:
            colors = ["green" if v >= 0 else "red" for v in df["MACD_Histogram"]]
            fig.add_trace(
                go.Bar(
                    x=df.index, y=df["MACD_Histogram"],
                    name="Histogram", marker_color=colors,
                ),
                row=3, col=1,
            )

    fig.update_layout(
        height=900,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Page 3: Forward Curve
# ---------------------------------------------------------------------------
def page_forward_curve():
    st.title("Forward Curve")

    fc_data = load_forward_curves()
    if fc_data.empty:
        st.warning("No forward curve data. Run `python main.py` first.")
        return

    commodities = sorted(fc_data["commodity"].unique())
    selected = st.selectbox("Select Commodity", commodities)

    subset = fc_data[fc_data["commodity"] == selected].sort_values("contract_month")
    if len(subset) < 2:
        st.info(f"Not enough contracts for {selected} curve.")
        return

    # Analyze
    result = analyze_curve(subset)

    # Summary metrics
    if result:
        cols = st.columns(4)
        cols[0].metric("Structure", result["structure"].title())
        cols[1].metric("Front", f"{result['front_price']:.2f}")
        cols[2].metric("Back", f"{result['back_price']:.2f}")
        cols[3].metric("Spread", f"{result['spread']:+.2f} ({result['spread_pct']:+.1f}%)")

    # Line chart
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=subset["label"],
            y=subset["close"],
            mode="lines+markers",
            name=selected,
            line=dict(width=3),
            marker=dict(size=10),
        )
    )

    # Color fill to show contango (green below) / backwardation (red above)
    front_price = subset.iloc[0]["close"]
    fig.add_hline(
        y=front_price,
        line_dash="dash",
        line_color="gray",
        annotation_text=f"Front month: {front_price:.2f}",
    )

    fig.update_layout(
        title=f"{selected} Forward Curve",
        xaxis_title="Contract Month",
        yaxis_title="Price",
        height=500,
    )
    st.plotly_chart(fig, width="stretch")

    # Raw data
    with st.expander("Raw data"):
        st.dataframe(subset[["label", "ticker", "close"]], hide_index=True)


# ---------------------------------------------------------------------------
# Page 4: Crush Spread
# ---------------------------------------------------------------------------
def page_crush_spread():
    st.title("Soybean Crush Spread")

    all_prices = load_prices()
    if all_prices.empty:
        st.warning("No price data. Run `python main.py` first.")
        return

    def get_commodity_df(name):
        subset = all_prices[all_prices["commodity"] == name].copy()
        if subset.empty:
            return pd.DataFrame()
        subset["Date"] = pd.to_datetime(subset["Date"])
        return subset.set_index("Date").sort_index()

    soybeans = get_commodity_df("Soybeans")
    oil = get_commodity_df("Soybean Oil")
    meal = get_commodity_df("Soybean Meal")

    if soybeans.empty or oil.empty or meal.empty:
        st.warning("Need Soybeans, Soybean Oil, and Soybean Meal data for crush spread.")
        return

    spread = compute_crush_spread(soybeans, oil, meal)
    if spread.empty:
        st.warning("No overlapping dates for crush spread calculation.")
        return

    spread["Date"] = pd.to_datetime(spread["Date"])

    # Crush spread chart with profitability shading
    fig = go.Figure()

    # Split into profitable (green) and unprofitable (red)
    positive = spread[spread["crush_spread"] >= 0]
    negative = spread[spread["crush_spread"] < 0]

    fig.add_trace(
        go.Scatter(
            x=spread["Date"],
            y=spread["crush_spread"] / 100,  # Convert cents to dollars
            mode="lines",
            name="Crush Spread",
            line=dict(color="black", width=2),
        )
    )

    # Fill above zero green, below zero red
    fig.add_trace(
        go.Scatter(
            x=spread["Date"],
            y=[max(0, v / 100) for v in spread["crush_spread"]],
            fill="tozeroy",
            fillcolor="rgba(0,128,0,0.2)",
            line=dict(width=0),
            name="Profitable",
        )
    )

    fig.add_trace(
        go.Scatter(
            x=spread["Date"],
            y=[min(0, v / 100) for v in spread["crush_spread"]],
            fill="tozeroy",
            fillcolor="rgba(255,0,0,0.2)",
            line=dict(width=0),
            name="Negative margin",
        )
    )

    fig.add_hline(y=0, line_dash="dash", line_color="gray")

    fig.update_layout(
        title="Soybean Crush Spread ($/bushel)",
        xaxis_title="Date",
        yaxis_title="Spread ($/bu)",
        height=500,
    )
    st.plotly_chart(fig, width="stretch")

    # Current value
    latest = spread.iloc[-1]["crush_spread"] / 100
    st.metric("Current Crush Spread", f"${latest:.2f}/bu",
              delta="Profitable" if latest > 0 else "Negative")


# ---------------------------------------------------------------------------
# Page 5: COT Positioning
# ---------------------------------------------------------------------------
def page_cot():
    st.title("COT Positioning")

    cot_data = load_cot()
    if cot_data.empty:
        st.warning("No COT data. Run `python main.py` first.")
        return

    # Get latest data for each commodity
    commodities = sorted(cot_data["commodity"].unique())

    # Summary bar chart: commercial vs speculator net positions
    latest_rows = []
    for commodity in commodities:
        subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
        if subset.empty:
            continue
        latest = subset.iloc[-1]
        latest_rows.append({
            "commodity": commodity,
            "commercial_net": latest.get("commercial_net", 0),
            "noncommercial_net": latest.get("noncommercial_net", 0),
        })

    if not latest_rows:
        st.info("No recent COT data found.")
        return

    summary = pd.DataFrame(latest_rows)

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            x=summary["commodity"],
            y=summary["commercial_net"],
            name="Commercials (net)",
            marker_color="steelblue",
        )
    )
    fig.add_trace(
        go.Bar(
            x=summary["commodity"],
            y=summary["noncommercial_net"],
            name="Speculators (net)",
            marker_color="coral",
        )
    )

    fig.update_layout(
        title="Latest COT Net Positions by Commodity",
        barmode="group",
        xaxis_title="Commodity",
        yaxis_title="Net Contracts",
        height=500,
    )
    st.plotly_chart(fig, width="stretch")

    # Detailed view per commodity
    selected = st.selectbox("Detail view", commodities)
    detail = cot_data[cot_data["commodity"] == selected].sort_values("Date")
    if not detail.empty:
        fig2 = go.Figure()
        fig2.add_trace(
            go.Scatter(
                x=detail["Date"], y=detail["commercial_net"],
                name="Commercial net", line=dict(color="steelblue"),
            )
        )
        fig2.add_trace(
            go.Scatter(
                x=detail["Date"], y=detail["noncommercial_net"],
                name="Speculator net", line=dict(color="coral"),
            )
        )
        fig2.update_layout(
            title=f"{selected} — COT Net Positions Over Time",
            height=400,
        )
        st.plotly_chart(fig2, width="stretch")


# ---------------------------------------------------------------------------
# Page 6: Weather
# ---------------------------------------------------------------------------
def page_weather():
    st.title("Weather Alerts")

    weather_data = load_weather()
    if weather_data.empty:
        st.warning("No weather data. Run `python main.py` first.")
        return

    # Build summary table for latest day per region
    rows = []
    for region in weather_data["region"].unique():
        subset = weather_data[weather_data["region"] == region].sort_values("Date")
        if subset.empty:
            continue
        latest = subset.iloc[-1]
        temp_max = latest.get("temp_max", None)
        temp_min = latest.get("temp_min", None)
        precip = latest.get("precipitation", None)

        # Determine alert level
        alert = "Normal"
        if pd.notna(precip) and precip > 20:
            alert = "Heavy Rain"
        elif pd.notna(precip) and precip < 1:
            alert = "Dry"
        if pd.notna(temp_max) and temp_max > 38:
            alert = "Extreme Heat"

        rows.append({
            "Region": region,
            "Temp Max (C)": f"{temp_max:.1f}" if pd.notna(temp_max) else "N/A",
            "Temp Min (C)": f"{temp_min:.1f}" if pd.notna(temp_min) else "N/A",
            "Precip (mm)": f"{precip:.1f}" if pd.notna(precip) else "N/A",
            "Alert": alert,
            "Date": latest["Date"],
        })

    if not rows:
        st.info("No weather data to display.")
        return

    summary = pd.DataFrame(rows)

    # Color code by alert status
    def highlight_alerts(row):
        if row["Alert"] == "Extreme Heat":
            return ["background-color: #ffcccc"] * len(row)
        elif row["Alert"] == "Heavy Rain":
            return ["background-color: #cce5ff"] * len(row)
        elif row["Alert"] == "Dry":
            return ["background-color: #fff3cd"] * len(row)
        return [""] * len(row)

    st.dataframe(
        summary.style.apply(highlight_alerts, axis=1),
        width="stretch",
        hide_index=True,
    )


# ---------------------------------------------------------------------------
# Page 7: Correlations
# ---------------------------------------------------------------------------
def page_correlations():
    st.title("Correlation Matrix")

    all_prices = load_prices()
    if all_prices.empty:
        st.warning("No price data. Run `python main.py` first.")
        return

    # Build price_data dict
    price_data = {}
    for commodity in all_prices["commodity"].unique():
        subset = all_prices[all_prices["commodity"] == commodity].copy()
        subset["Date"] = pd.to_datetime(subset["Date"])
        subset = subset.set_index("Date").sort_index()
        price_data[commodity] = subset

    if len(price_data) < 2:
        st.info("Need at least 2 commodities for correlation analysis.")
        return

    corr_matrix = commodity_correlation_matrix(price_data)
    if corr_matrix.empty:
        st.warning("Could not compute correlation matrix.")
        return

    # Plotly heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=corr_matrix.values,
            x=corr_matrix.columns.tolist(),
            y=corr_matrix.index.tolist(),
            colorscale="RdBu_r",
            zmin=-1,
            zmax=1,
            text=[[f"{v:.2f}" for v in row] for row in corr_matrix.values],
            texttemplate="%{text}",
            textfont=dict(size=10),
        )
    )

    fig.update_layout(
        title="Cross-Commodity Correlation (Close Prices)",
        height=600,
        width=800,
    )
    st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Page router
# ---------------------------------------------------------------------------
if page == "Overview":
    page_overview()
elif page == "Price Charts":
    page_price_charts()
elif page == "Forward Curve":
    page_forward_curve()
elif page == "Crush Spread":
    page_crush_spread()
elif page == "COT Positioning":
    page_cot()
elif page == "Weather":
    page_weather()
elif page == "Correlations":
    page_correlations()
