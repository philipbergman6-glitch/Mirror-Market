"""Shared Plotly figure builders for Mirror Market dashboard.

All functions return go.Figure objects, used by the static HTML generator
(scripts/generate_html.py).
"""

from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ---------------------------------------------------------------------------
# Design tokens (from DESIGN.md)
# ---------------------------------------------------------------------------
COLORS = {
    "primary": "#1B4332",
    "secondary": "#2D6A4F",
    "accent": "#2D6A4F",
    "bullish": "#1F7A3D",
    "bearish": "#C23B2E",
    "neutral": "#5D6660",
    "bg": "#FAFAF7",
    "surface": "#FFFFFF",
    "card": "#FFFFFF",
    "border": "#D8DCD8",
    "text": "#191C1A",
    "text_muted": "#5D6660",
    "text_dim": "#9BA39E",
    "soybean": "#8B6914",
    "soy_oil": "#B05E10",
    "soy_meal": "#8A5A2B",
    "info": "#2F5D8F",
    "warning": "#A8730A",
}

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Shared light editorial layout defaults applied to every figure
_BASE_LAYOUT = dict(
    paper_bgcolor=COLORS["card"],
    plot_bgcolor=COLORS["surface"],
    font=dict(color=COLORS["text"], family="Inter, sans-serif", size=12),
    xaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
    yaxis=dict(gridcolor=COLORS["border"], zerolinecolor=COLORS["border"]),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=11)),
    margin=dict(l=50, r=20, t=40, b=40),
)


# ---------------------------------------------------------------------------
# Color helpers
# ---------------------------------------------------------------------------
def chg_color(val: float | None) -> str:
    """Return bullish/bearish color based on positive/negative value."""
    if pd.isna(val):
        return COLORS["neutral"]
    return COLORS["bullish"] if val >= 0 else COLORS["bearish"]


def delta_str(val: float | None) -> str:
    """Format a number as +X.X% or -X.X%."""
    if pd.isna(val):
        return "N/A"
    return f"{val:+.1f}%"


# ---------------------------------------------------------------------------
# Chart builders
# ---------------------------------------------------------------------------

def build_technical_chart(df: pd.DataFrame, leg_name: str) -> go.Figure:
    """Candlestick + RSI + MACD subplots for one soy leg.

    Expects df with DatetimeIndex and columns: Open, High, Low, Close,
    Volume, MA_20, MA_50, MA_200, BB_Upper, BB_Lower, RSI, MACD,
    MACD_Signal, MACD_Histogram.
    """
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
            increasing_line_color=COLORS["bullish"],
            decreasing_line_color=COLORS["bearish"],
        ),
        row=1, col=1,
    )

    # Moving averages
    ma_colors = {"MA_20": COLORS["soy_oil"], "MA_50": COLORS["info"], "MA_200": COLORS["bearish"]}
    for ma, color in ma_colors.items():
        if ma in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df[ma], name=ma, line=dict(width=1, color=color)),
                row=1, col=1,
            )

    # Bollinger Bands
    if "BB_Upper" in df.columns and "BB_Lower" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["BB_Upper"], name="BB Upper",
                       line=dict(width=1, dash="dot", color=COLORS["text_dim"])),
            row=1, col=1,
        )
        fig.add_trace(
            go.Scatter(x=df.index, y=df["BB_Lower"], name="BB Lower",
                       line=dict(width=1, dash="dot", color=COLORS["text_dim"]),
                       fill="tonexty", fillcolor="rgba(155,163,158,0.12)"),
            row=1, col=1,
        )

    # Volume
    if "Volume" in df.columns:
        vol_colors = [COLORS["bullish"] if c >= o else COLORS["bearish"]
                      for c, o in zip(df["Close"], df["Open"], strict=False)]
        fig.add_trace(
            go.Bar(x=df.index, y=df["Volume"], name="Volume",
                   marker_color=vol_colors, opacity=0.3),
            row=1, col=1,
        )

    # RSI
    if "RSI" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["RSI"], name="RSI",
                       line=dict(color=COLORS["info"])),
            row=2, col=1,
        )
        fig.add_hline(y=70, line_dash="dash", line_color=COLORS["bearish"], row=2, col=1)
        fig.add_hline(y=30, line_dash="dash", line_color=COLORS["bullish"], row=2, col=1)

    # MACD
    if "MACD" in df.columns:
        fig.add_trace(
            go.Scatter(x=df.index, y=df["MACD"], name="MACD",
                       line=dict(color=COLORS["info"])),
            row=3, col=1,
        )
        if "MACD_Signal" in df.columns:
            fig.add_trace(
                go.Scatter(x=df.index, y=df["MACD_Signal"], name="Signal",
                           line=dict(color=COLORS["soy_oil"])),
                row=3, col=1,
            )
        if "MACD_Histogram" in df.columns:
            hist_colors = [COLORS["bullish"] if v >= 0 else COLORS["bearish"]
                           for v in df["MACD_Histogram"]]
            fig.add_trace(
                go.Bar(x=df.index, y=df["MACD_Histogram"], name="Histogram",
                       marker_color=hist_colors),
                row=3, col=1,
            )

    base = {**_BASE_LAYOUT}
    base["legend"] = dict(orientation="h", yanchor="bottom", y=1.02, bgcolor="rgba(0,0,0,0)", font=dict(size=11))
    fig.update_layout(
        height=900,
        xaxis_rangeslider_visible=False,
        showlegend=True,
        yaxis_title="USD/MT",
        **base,
    )
    return fig


def build_crush_spread_chart(
    spread_df: pd.DataFrame,
    spread_mt: pd.Series,
    crush: dict,
) -> go.Figure:
    """Crush spread time series with 1Y range shading and profit/loss zones.

    Args:
        spread_df: DataFrame with 'Date' column
        spread_mt: Series of crush spread values in USD/MT
        crush: dict with keys avg_1y, min_1y, max_1y
    """
    fig = go.Figure()

    if crush.get("min_1y") is not None:
        fig.add_hline(y=crush["avg_1y"], line_dash="dot", line_color=COLORS["info"],
                      annotation_text=f"1Y avg: ${crush['avg_1y']:,.1f}",
                      annotation_font_color=COLORS["text_muted"])
        fig.add_hrect(y0=crush["min_1y"], y1=crush["max_1y"],
                      fillcolor="rgba(47,93,143,0.07)", line_width=0,
                      annotation_text="1Y range",
                      annotation_font_color=COLORS["text_dim"])

    fig.add_trace(
        go.Scatter(
            x=spread_df["Date"], y=spread_mt,
            mode="lines", name="Crush Spread",
            line=dict(color=COLORS["text"], width=2),
        )
    )
    fig.add_trace(
        go.Scatter(
            x=spread_df["Date"],
            y=[max(0, v) for v in spread_mt],
            fill="tozeroy", fillcolor="rgba(31,122,61,0.12)",
            line=dict(width=0), name="Profitable",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=spread_df["Date"],
            y=[min(0, v) for v in spread_mt],
            fill="tozeroy", fillcolor="rgba(194,59,46,0.10)",
            line=dict(width=0), name="Negative",
        )
    )
    fig.add_hline(y=0, line_dash="dash", line_color=COLORS["text_dim"])
    fig.update_layout(height=350, xaxis_title="", yaxis_title="USD/MT", **_BASE_LAYOUT)
    return fig


def build_basis_chart(
    basis_payload: pd.DataFrame | dict,
    stats: dict,
) -> go.Figure:
    """Brazil basis time series with discount/premium zones.

    Negative basis (Brazilian discount = export-competitive) is shaded in
    the bullish-for-trade green; positive basis (domestic premium = import
    pull) is shaded in the bearish red. Zero is the meaningful threshold.

    Args:
        basis_payload: either
            - a DataFrame with 'Date' + 'basis_usd_mt' (single-source legacy path), or
            - the multi-source basis dict from `relative_value_analysis()`:
              {"primary": str, "sources": {label: {"series": df, ...}}, "wedge_usd_mt": ...}.
              The primary source renders as a solid line; any secondary source
              renders as a dashed muted line.
        stats: dict with keys avg_1y, min_1y, max_1y for the primary source
            (used for the 1Y range backdrop and average horizontal line).
    """
    if isinstance(basis_payload, dict) and "sources" in basis_payload:
        primary_label = basis_payload["primary"]
        sources = basis_payload["sources"]
        primary_df = sources[primary_label]["series"]
        secondary_label = next(
            (lbl for lbl in sources if lbl != primary_label), None
        )
        secondary_df = sources[secondary_label]["series"] if secondary_label else None
    else:
        primary_label = "Brazil Basis"
        primary_df = basis_payload
        secondary_label = None
        secondary_df = None

    fig = go.Figure()

    if stats.get("min_1y") is not None:
        fig.add_hline(
            y=stats["avg_1y"], line_dash="dot", line_color=COLORS["info"],
            annotation_text=f"1Y avg: ${stats['avg_1y']:+,.1f}",
            annotation_font_color=COLORS["text_muted"],
        )
        fig.add_hrect(
            y0=stats["min_1y"], y1=stats["max_1y"],
            fillcolor="rgba(47,93,143,0.07)", line_width=0,
            annotation_text="1Y range",
            annotation_font_color=COLORS["text_dim"],
        )

    primary_series = primary_df["basis_usd_mt"]
    fig.add_trace(
        go.Scatter(
            x=primary_df["Date"], y=primary_series,
            mode="lines", name=primary_label,
            line=dict(color=COLORS["text"], width=2),
        )
    )
    if secondary_df is not None:
        fig.add_trace(
            go.Scatter(
                x=secondary_df["Date"], y=secondary_df["basis_usd_mt"],
                mode="lines", name=secondary_label,
                line=dict(color=COLORS["text_muted"], width=1.5, dash="dash"),
            )
        )
    # Discount zone (negative basis = export-competitive Brazil) — primary only.
    fig.add_trace(
        go.Scatter(
            x=primary_df["Date"],
            y=[min(0, v) for v in primary_series],
            fill="tozeroy", fillcolor="rgba(31,122,61,0.12)",
            line=dict(width=0), name="Brazilian discount",
            showlegend=False,
        )
    )
    # Premium zone (positive basis = domestic pull) — primary only.
    fig.add_trace(
        go.Scatter(
            x=primary_df["Date"],
            y=[max(0, v) for v in primary_series],
            fill="tozeroy", fillcolor="rgba(194,59,46,0.10)",
            line=dict(width=0), name="Brazilian premium",
            showlegend=False,
        )
    )
    fig.add_hline(y=0, line_dash="dash", line_color=COLORS["text_dim"])
    fig.update_layout(height=350, xaxis_title="", yaxis_title="USD/MT", **_BASE_LAYOUT)
    return fig


def build_oil_meal_ratio_chart(omr: dict) -> go.Figure:
    """Oil/Meal ratio time series with 60d average and 1Y range.

    Args:
        omr: dict with keys series (pd.Series), avg_60d, min_1y, max_1y
    """
    fig = go.Figure()
    if omr.get("min_1y") is not None:
        fig.add_hrect(y0=omr["min_1y"], y1=omr["max_1y"],
                      fillcolor="rgba(176,94,16,0.07)", line_width=0,
                      annotation_text="1Y range",
                      annotation_font_color=COLORS["text_dim"])
    fig.add_trace(
        go.Scatter(x=omr["series"].index, y=omr["series"], mode="lines",
                   name="Oil/Meal Ratio", line=dict(color=COLORS["soy_oil"]))
    )
    fig.add_hline(y=omr["avg_60d"], line_dash="dash", line_color=COLORS["text_dim"],
                  annotation_text="60d avg", annotation_font_color=COLORS["text_muted"])
    fig.update_layout(height=300, **_BASE_LAYOUT)
    return fig


def build_bean_corn_ratio_chart(bcr: dict) -> go.Figure:
    """Bean/Corn ratio with 1Y average and range.

    Args:
        bcr: dict with keys series (pd.Series), avg_1y, min_1y, max_1y
    """
    fig = go.Figure()
    if bcr.get("min_1y") is not None:
        fig.add_hrect(y0=bcr["min_1y"], y1=bcr["max_1y"],
                      fillcolor="rgba(138,90,43,0.07)", line_width=0,
                      annotation_text="1Y range",
                      annotation_font_color=COLORS["text_dim"])
    fig.add_trace(
        go.Scatter(x=bcr["series"].index, y=bcr["series"], mode="lines",
                   name="Bean/Corn Ratio", line=dict(color=COLORS["soy_meal"]))
    )
    fig.add_hline(y=bcr["avg_1y"], line_dash="dash", line_color=COLORS["text_dim"],
                  annotation_text="1Y avg", annotation_font_color=COLORS["text_muted"])
    fig.update_layout(height=300, **_BASE_LAYOUT)
    return fig


def build_cot_chart(cot: dict) -> go.Figure:
    """COT positioning grouped bar chart — commercials vs speculators.

    Args:
        cot: dict keyed by commodity name, each value has commercial_net, spec_net
    """
    fig = go.Figure()
    commodities = list(cot.keys())
    comm_nets = [cot[c].get("commercial_net", 0) or 0 for c in commodities]
    spec_nets = [cot[c].get("spec_net", 0) or 0 for c in commodities]

    fig.add_trace(go.Bar(x=commodities, y=comm_nets, name="Commercials (net)",
                         marker_color=COLORS["info"]))
    fig.add_trace(go.Bar(x=commodities, y=spec_nets, name="Speculators (net)",
                         marker_color=COLORS["soy_oil"]))
    fig.update_layout(barmode="group", height=400, yaxis_title="Net Contracts", **_BASE_LAYOUT)
    return fig


def build_correlations_chart(
    pairs: list[tuple[str, pd.Series, pd.Series]],
    rolling_correlation_fn,
    window: int = 60,
) -> go.Figure:
    """Rolling correlation line chart for multiple pairs.

    Args:
        pairs: list of (label, series_a, series_b) tuples
        rolling_correlation_fn: the rolling_correlation function from analysis.correlations
        window: rolling window size
    """
    fig = go.Figure()
    pair_colors = [COLORS["bearish"], COLORS["soy_oil"], COLORS["info"]]
    for i, (label, sa, sb) in enumerate(pairs):
        rc = rolling_correlation_fn(sa, sb, window=window)
        if not rc.empty:
            fig.add_trace(
                go.Scatter(x=rc.index, y=rc, mode="lines", name=label,
                           line=dict(color=pair_colors[i % len(pair_colors)], width=2))
            )
    fig.add_hline(y=0, line_dash="dash", line_color=COLORS["text_dim"])
    fig.update_layout(height=400, yaxis_title="60d Rolling Correlation",
                      yaxis_range=[-1, 1], **_BASE_LAYOUT)
    return fig


def build_forward_curve_chart(
    curve_df_mt: pd.DataFrame,
    leg: str,
    unit: str = "USD/MT",
) -> go.Figure:
    """Forward curve line+markers chart for one commodity.

    Args:
        curve_df_mt: DataFrame with columns 'label' and 'close' (in MT units)
        leg: commodity name
        unit: y-axis unit label
    """
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=curve_df_mt["label"], y=curve_df_mt["close"],
            mode="lines+markers", name=leg,
            line=dict(width=3, color=COLORS["accent"]),
            marker=dict(size=10, color=COLORS["accent"]),
        )
    )
    front_price_mt = curve_df_mt.iloc[0]["close"]
    fig.add_hline(y=front_price_mt, line_dash="dash", line_color=COLORS["text_dim"],
                  annotation_text=f"Front: {front_price_mt:,.1f}",
                  annotation_font_color=COLORS["text_muted"])
    fig.update_layout(height=350, xaxis_title="Contract", yaxis_title=unit, **_BASE_LAYOUT)
    return fig


def build_seasonal_chart(
    monthly: pd.DataFrame,
    vs_seasonal: dict,
    leg: str,
    unit: str = "USD/MT",
) -> go.Figure:
    """Monthly seasonal bar chart with error bars and current price marker.

    Args:
        monthly: DataFrame with columns month, avg_close, min_close, max_close
        vs_seasonal: dict with current_price key
        leg: commodity name
        unit: y-axis unit label
    """
    labels = [MONTH_NAMES[m - 1] for m in monthly["month"]]
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=labels,
            y=monthly["avg_close"],
            name="Avg Close",
            marker_color=COLORS["info"],
            error_y=dict(
                type="data",
                symmetric=False,
                array=monthly["max_close"] - monthly["avg_close"],
                arrayminus=monthly["avg_close"] - monthly["min_close"],
                color="rgba(47,93,143,0.3)",
            ),
        )
    )

    if vs_seasonal:
        current_month_idx = datetime.now().month - 1
        if current_month_idx < len(labels):
            fig.add_trace(
                go.Scatter(
                    x=[labels[current_month_idx]],
                    y=[vs_seasonal["current_price"]],
                    mode="markers",
                    name="Current",
                    marker=dict(color=COLORS["bearish"], size=14, symbol="diamond"),
                )
            )

    fig.update_layout(height=350, yaxis_title=unit, xaxis_title="Month",
                      showlegend=True, **_BASE_LAYOUT)
    return fig
