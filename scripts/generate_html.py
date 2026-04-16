"""Generate a static HTML dashboard from Mirror Market data.

Usage:
    python scripts/generate_html.py

Reads from the SQLite database (populated by main.py), calls the same
analyst functions as the Streamlit dashboard, builds Plotly charts,
and renders a single index.html via Jinja2.
"""

import base64
import html as html_lib
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.charts import (
    COLORS,
    build_bean_corn_ratio_chart,
    build_correlations_chart,
    build_cot_chart,
    build_crush_spread_chart,
    build_forward_curve_chart,
    build_oil_meal_ratio_chart,
    build_seasonal_chart,
    build_technical_chart,
    chg_color,
    delta_str,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "docs"
OUTPUT_FILE = OUTPUT_DIR / "index.html"
TEMPLATE_DIR = PROJECT_ROOT / "app" / "templates"

# Page definitions for sidebar nav
PAGES = [
    {"id": "command-center", "name": "Command Center", "icon": "\u25A0"},
    {"id": "technicals", "name": "Technicals", "icon": "\u25B2"},
    {"id": "supply-demand", "name": "Supply & Demand", "icon": "\u2584"},
    {"id": "relative-value", "name": "Relative Value", "icon": "\u2300"},
    {"id": "risk-monitor", "name": "Risk Monitor", "icon": "\u26A0"},
    {"id": "forward-curves", "name": "Forward Curves", "icon": "\u2192"},
    {"id": "seasonal", "name": "Seasonal", "icon": "\u2605"},
    {"id": "briefing", "name": "Briefing", "icon": "\u270E"},
    {"id": "about", "name": "About", "icon": "\u2139"},
]

LEG_COLORS = {
    "Soybeans": COLORS["soybean"],
    "Soybean Oil": COLORS["soy_oil"],
    "Soybean Meal": COLORS["soy_meal"],
}


def _safe_call(fn, label: str):
    """Call fn(), returning None on error."""
    try:
        result = fn()
        if isinstance(result, str) and "failed" in result.lower():
            log.warning("  %s returned error string", label)
            return None
        return result
    except Exception as e:
        log.warning("  %s failed: %s", label, e)
        return None


def _fig_to_html(fig) -> str:
    """Convert a Plotly figure to an embeddable HTML div."""
    return fig.to_html(full_html=False, include_plotlyjs=False)


def _to_data_uri(text: str, mime: str = "text/plain") -> str:
    """Encode text as a base64 data URI for download links."""
    b64 = base64.b64encode(text.encode("utf-8")).decode("ascii")
    return f"data:{mime};base64,{b64}"


def _csv_data_uri(df: pd.DataFrame) -> str:
    """Encode a DataFrame as a CSV data URI."""
    csv_str = df.to_csv(index=True)
    return _to_data_uri(csv_str, "text/csv")


def _esc(text) -> str:
    """HTML-escape a string."""
    if text is None:
        return ""
    return html_lib.escape(str(text))


# ---------------------------------------------------------------------------
# Freshness indicators
# ---------------------------------------------------------------------------
def _build_freshness_items() -> list[dict]:
    """Build data freshness sidebar items."""
    try:
        from pipeline.query import read_freshness
        freshness = read_freshness()
    except Exception:
        return []

    if freshness.empty:
        return []

    now = datetime.now(tz=None)
    items = []
    for _, row in freshness.iterrows():
        layer = row["layer_name"]
        last = row["last_success"]
        if pd.notna(last):
            last_dt = pd.to_datetime(last)
            age = now - last_dt
            if age < timedelta(days=1):
                status = "fresh"
                age_str = f"{int(age.total_seconds() // 3600)}h ago"
            elif age < timedelta(days=7):
                status = "stale"
                age_str = f"{age.days}d ago"
            else:
                status = "old"
                age_str = f"{age.days}d ago"
        else:
            status = "old"
            age_str = "never"
        items.append({"name": layer, "status": status, "age": age_str})
    return items


# ---------------------------------------------------------------------------
# Command Center context
# ---------------------------------------------------------------------------
def _build_command_center(data: dict) -> dict | None:
    if not data:
        return None

    legs = []
    for leg_info in data.get("legs", []):
        name = leg_info.get("name", "")
        price = leg_info.get("close")
        daily = leg_info.get("daily_chg")
        rsi = leg_info.get("rsi")
        trend = leg_info.get("trend", "N/A")
        vol = leg_info.get("hv_20")

        rsi_class = ""
        if rsi and rsi > 70:
            rsi_class = "down"
        elif rsi and rsi < 30:
            rsi_class = "up"

        legs.append({
            "name": name,
            "color": LEG_COLORS.get(name, COLORS["text"]),
            "price": f"{price:,.2f}" if price else "N/A",
            "daily_chg": delta_str(daily),
            "chg_class": "up" if daily and daily >= 0 else "down" if daily else "muted",
            "rsi": f"{rsi:.1f}" if rsi else "N/A",
            "rsi_class": rsi_class,
            "trend": trend,
            "trend_class": "up" if trend == "Bullish" else "down" if trend == "Bearish" else "muted",
            "volatility": f"{vol:.1f}%" if vol else "N/A",
        })

    # Key metrics
    crush = data.get("crush", {})
    km = data.get("key_metrics", {})
    key_metrics = []

    crush_val = crush.get("value_usd_mt")
    key_metrics.append({
        "label": "Crush Spread",
        "value": f"${crush_val:,.1f}" if crush_val else "N/A",
        "val_class": "up" if crush.get("profitable") else "down" if crush_val else "",
        "delta": "Profitable" if crush.get("profitable") else "Negative" if crush_val else "",
        "delta_class": "up" if crush.get("profitable") else "down",
    })

    # key_metrics is a flat dict: brl_usd, brl_weekly_chg, dollar_index, cny_usd
    brl_val = km.get("brl_usd")
    brl_chg = km.get("brl_weekly_chg")
    key_metrics.append({
        "label": "BRL/USD",
        "value": f"{brl_val:.4f}" if brl_val else "N/A",
        "val_class": "",
        "delta": delta_str(brl_chg) if brl_chg is not None else "",
        "delta_class": "up" if brl_chg and brl_chg >= 0 else "down" if brl_chg else "muted",
    })

    dollar_val = km.get("dollar_index")
    key_metrics.append({
        "label": "Dollar Index",
        "value": f"{dollar_val:.2f}" if dollar_val else "N/A",
        "val_class": "",
        "delta": "",
        "delta_class": "muted",
    })

    cny_val = km.get("cny_usd")
    key_metrics.append({
        "label": "CNY/USD",
        "value": f"{cny_val:.4f}" if cny_val else "N/A",
        "val_class": "",
        "delta": "",
        "delta_class": "muted",
    })

    # Signals
    signals = []
    for sig in data.get("signals", []):
        sev = sig.get("severity", "info")
        signals.append({
            "severity": sev,
            "severity_label": sev.upper(),
            "commodity": sig.get("commodity", ""),
            "message": sig.get("message", ""),
        })

    return {"legs": legs, "key_metrics": key_metrics, "signals": signals}


# ---------------------------------------------------------------------------
# Technicals context
# ---------------------------------------------------------------------------
def _build_technicals(data: dict) -> list[dict] | None:
    if not data:
        return None

    per_leg_mt = data.get("per_leg_mt", data.get("per_leg", {}))
    all_signals = data.get("signals", [])
    items = []

    for name in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
        df = per_leg_mt.get(name)
        if df is None or df.empty:
            continue

        fig = build_technical_chart(df, name)
        chart_html = _fig_to_html(fig)

        leg_signals = [s for s in all_signals if s.get("commodity") == name]
        sig_items = [{
            "severity": s.get("severity", "info"),
            "severity_label": s.get("severity", "info").upper(),
            "message": s.get("message", ""),
        } for s in leg_signals]

        # CSV download (last 252 trading days)
        csv_df = df.tail(252)[["Open", "High", "Low", "Close"]].copy()
        csv_uri = _csv_data_uri(csv_df)

        items.append({
            "name": name,
            "chart_html": chart_html,
            "signals": sig_items,
            "csv_uri": csv_uri,
        })

    return items if items else None


# ---------------------------------------------------------------------------
# Supply & Demand HTML snippets
# ---------------------------------------------------------------------------
def _build_supply(data: dict) -> dict | None:
    if not data:
        return None
    out = {}

    # WASDE (soy only)
    wasde = data.get("wasde", {})
    if wasde:
        lines = []
        for commodity, attrs in wasde.items():
            if "SOYBEAN" not in commodity.upper():
                continue
            lines.append(f'<div class="subhdr" style="font-size:14px; margin-top:12px;">{_esc(commodity)}</div>')
            for attr_name, info in attrs.items():
                val = info.get("value")
                if pd.isna(val):
                    continue
                rev = info.get("revision")
                unit = info.get("unit", "")
                rev_str = ""
                if rev is not None and rev != 0:
                    direction = "UP" if rev > 0 else "DOWN"
                    rev_str = f' <span class="{"up" if rev > 0 else "down"}">(revised {direction} {abs(rev):,.0f})</span>'
                lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(attr_name)}: <strong style="color:var(--text)">{val:,.0f}</strong> {_esc(unit)}{rev_str}</div>')
        out["wasde_html"] = "\n".join(lines) if lines else ""

    # Competing crops WASDE
    if wasde:
        lines = []
        for commodity, attrs in wasde.items():
            if "SOYBEAN" in commodity.upper():
                continue
            lines.append(f'<div class="subhdr" style="font-size:14px; margin-top:12px;">{_esc(commodity)}</div>')
            for attr_name, info in attrs.items():
                val = info.get("value")
                if pd.isna(val):
                    continue
                unit = info.get("unit", "")
                lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(attr_name)}: <strong style="color:var(--text)">{val:,.0f}</strong> {_esc(unit)}</div>')
        # PSD highlights
        psd = data.get("psd_highlights", [])
        if psd:
            lines.append('<hr class="divider"><div class="subhdr">Global Supply (PSD)</div>')
            for item in psd:
                lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(item["country"])} {_esc(item["commodity"])} {_esc(item["attribute"])}: <strong style="color:var(--text)">{item["value"]:,.0f}</strong> {_esc(item.get("unit", ""))}</div>')
        out["competing_html"] = "\n".join(lines) if lines else ""

    # CONAB
    conab = data.get("conab_vs_usda", {})
    if conab.get("conab_production"):
        cp = conab["conab_production"]
        up = conab.get("usda_production")
        gap = conab.get("gap", 0)
        html_parts = [f'<div class="grid grid-3">']
        html_parts.append(f'<div class="mc"><div class="mc-label">CONAB (Brazil)</div><div class="mc-val">{cp:,.0f}</div><div class="mc-delta muted">1000 MT</div></div>')
        if up:
            html_parts.append(f'<div class="mc"><div class="mc-label">USDA (Brazil)</div><div class="mc-val">{up:,.0f}</div><div class="mc-delta muted">1000 MT</div></div>')
            gc = "up" if gap > 0 else "down"
            html_parts.append(f'<div class="mc"><div class="mc-label">Gap</div><div class="mc-val {gc}">{gap:+,.0f}</div><div class="mc-delta muted">1000 MT</div></div>')
        html_parts.append('</div>')
        out["conab_html"] = "\n".join(html_parts)

    # Crop progress
    crop = data.get("crop_progress", {})
    if crop:
        lines = []
        for item in crop.get("condition", []):
            lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(item["desc"])}: <strong style="color:var(--text)">{item["value"]}%</strong></div>')
        for item in crop.get("progress", []):
            lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(item["desc"])}: <strong style="color:var(--text)">{item["value"]}%</strong></div>')
        out["crop_progress_html"] = "\n".join(lines) if lines else ""

    return out if out else None


def _build_demand(data: dict) -> dict | None:
    if not data:
        return None
    out = {}

    # China buying
    china = data.get("china_buying", {})
    if china:
        cards = ['<div class="grid grid-3">']
        for commodity, info in china.items():
            cards.append(f'<div class="mc"><div class="mc-label">{_esc(commodity)}</div><div class="mc-val">{info["net_sales"]:,.0f}</div><div class="mc-delta muted">MT | {info["pct_of_total"]:.0f}% of total</div></div>')
        cards.append('</div>')
        out["china_html"] = "\n".join(cards)

    # Export sales
    es = data.get("export_sales", {})
    if es:
        lines = []
        for commodity, info in es.items():
            we = info["week_ending"]
            week_str = we.strftime("%m/%d") if hasattr(we, "strftime") else str(we)
            lines.append(f'<div style="margin-bottom:12px;"><strong style="color:var(--text)">{_esc(commodity)}</strong> <span class="muted">(w/e {week_str})</span>')
            lines.append(f'<div style="font-size:13px; color:var(--text-muted);">Net sales: <strong style="color:var(--text)">{info["net_sales"]:,.0f} MT</strong> | Exports: <strong style="color:var(--text)">{info["exports"]:,.0f} MT</strong></div>')
            if info.get("top_buyers"):
                buyers = ", ".join(f'{b["country"]} ({b["mt"]:,.0f})' for b in info["top_buyers"])
                lines.append(f'<div style="font-size:12px; color:var(--text-dim);">Top buyers: {buyers}</div>')
            lines.append('</div>')
        out["export_sales_html"] = "\n".join(lines)

    # Biofuel
    bio = data.get("biofuel", {})
    if bio:
        cards = [f'<div class="grid grid-{min(len(bio), 4)}">']
        for name, info in bio.items():
            chg = info.get("chg_pct")
            dc = "up" if chg and chg >= 0 else "down" if chg else "muted"
            cards.append(f'<div class="mc"><div class="mc-label">{_esc(name)}</div><div class="mc-val">{info["value"]:,.2f}</div><div class="mc-delta {dc}">{delta_str(chg)}</div></div>')
        cards.append('</div>')
        out["biofuel_html"] = "\n".join(cards)

    return out if out else None


# ---------------------------------------------------------------------------
# Emerging Markets HTML
# ---------------------------------------------------------------------------
def _build_emerging_markets(data: dict) -> str:
    if not data:
        return ""

    countries = data.get("countries", {})
    if not countries:
        return ""

    parts = []
    for country_name, info in countries.items():
        parts.append(f'<div class="subhdr">{_esc(country_name)}</div>')

        # PSD
        psd_em = info.get("psd", {})
        if psd_em:
            year = info.get("psd_year", "")
            cards = ['<div class="grid grid-4">']
            for attr in ["Production", "Imports", "Exports", "Ending Stocks"]:
                if attr in psd_em:
                    vals = psd_em[attr]
                    yoy = vals.get("yoy_pct")
                    yoy_str = f'{yoy:+.1f}% YoY' if yoy is not None else ""
                    yoy_class = "up" if yoy and yoy >= 0 else "down" if yoy else "muted"
                    cards.append(f'<div class="mc"><div class="mc-label">{_esc(attr)} ({year})</div><div class="mc-val">{vals["value"]:,.0f}</div><div class="mc-delta {yoy_class}">{yoy_str} {_esc(vals.get("unit", ""))}</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        # Currency
        currency = info.get("currency", {})
        if currency:
            pair = currency["pair"]
            close = currency["close"]
            wk = currency.get("weekly_chg")
            wk_class = "up" if wk and wk >= 0 else "down" if wk else "muted"
            parts.append(f'<div class="grid grid-2"><div class="mc"><div class="mc-label">{_esc(pair)}</div><div class="mc-val">{close:.4f}</div><div class="mc-delta {wk_class}">{delta_str(wk)}</div></div></div>')

        # Weather
        weather_list = info.get("weather", [])
        active_alerts = [w for w in weather_list if w.get("alert")]
        if active_alerts:
            for w in active_alerts:
                parts.append(f'<div class="alert alert-warn">{_esc(w.get("region", ""))}: {_esc(w["alert"])} — Max {w.get("temp_max", "N/A")}C, Precip {w.get("precip", 0):.0f}mm</div>')
        elif weather_list:
            parts.append(f'<div class="alert alert-ok">No active weather alerts in {_esc(country_name)}</div>')

        # India domestic
        dom_india = info.get("india_domestic", {})
        if dom_india:
            parts.append('<div class="subhdr" style="font-size:14px;">NCDEX Domestic Prices</div>')
            cards = ['<div class="grid grid-3">']
            for key, label in [("soybean_ncdex_inr", "Soybean"), ("oil_ncdex_inr", "Soy Oil"), ("meal_ncdex_inr", "Soy Meal")]:
                v = dom_india.get(key)
                if v:
                    cards.append(f'<div class="mc"><div class="mc-label">{label}</div><div class="mc-val">\u20B9{v:,.0f}</div><div class="mc-delta muted">INR/MT</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        # Brazil domestic
        dom_brazil = info.get("brazil_domestic", {})
        if dom_brazil:
            parts.append('<div class="subhdr" style="font-size:14px;">CEPEA Farm-Gate Price</div>')
            brl = dom_brazil.get("cepea_soy_brl")
            usd = dom_brazil.get("cepea_soy_usd")
            basis = dom_brazil.get("brazil_cbot_basis_usd")
            cards = ['<div class="grid grid-3">']
            if brl:
                cards.append(f'<div class="mc"><div class="mc-label">CEPEA Soybean</div><div class="mc-val">R${brl:,.2f}</div><div class="mc-delta muted">BRL/MT</div></div>')
            if usd:
                cards.append(f'<div class="mc"><div class="mc-label">CEPEA (USD)</div><div class="mc-val">${usd:,.1f}</div><div class="mc-delta muted">USD/MT</div></div>')
            if basis is not None:
                bc = "up" if basis > 0 else "down"
                cards.append(f'<div class="mc"><div class="mc-label">Brazil-CBOT Basis</div><div class="mc-val {bc}">${basis:+,.1f}</div><div class="mc-delta muted">{"premium" if basis > 0 else "discount"}</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        # South Africa SAFEX
        dom_sa = info.get("south_africa_domestic", {})
        if dom_sa:
            parts.append('<div class="subhdr" style="font-size:14px;">SAFEX Settlement Prices</div>')
            zar = dom_sa.get("soybean_safex_zar")
            usd = dom_sa.get("soybean_safex_usd")
            basis = dom_sa.get("safex_cbot_basis_usd")
            cards = ['<div class="grid grid-3">']
            if zar:
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX Soybean</div><div class="mc-val">R{zar:,.0f}</div><div class="mc-delta muted">ZAR/MT</div></div>')
            if usd:
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX (USD)</div><div class="mc-val">${usd:,.1f}</div><div class="mc-delta muted">USD/MT</div></div>')
            if basis is not None:
                bc = "up" if basis > 0 else "down"
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX-CBOT Basis</div><div class="mc-val {bc}">${basis:+,.1f}</div><div class="mc-delta muted">{"premium" if basis > 0 else "parity"}</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        parts.append('<hr class="divider">')

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Relative Value HTML
# ---------------------------------------------------------------------------
def _build_relative_value(data: dict) -> str:
    if not data:
        return ""

    parts = []

    # Crush spread
    crush = data.get("crush", {})
    spread_df = crush.get("series")
    if spread_df is not None and not spread_df.empty:
        parts.append('<div class="subhdr">Crush Spread</div>')
        try:
            from pipeline.units import to_metric_tons
            spread_mt = spread_df["crush_spread"].apply(lambda x: to_metric_tons(x, "Soybeans"))
            fig = build_crush_spread_chart(spread_df, spread_mt, crush)
            cur = crush.get("current_usd_mt", 0)
            prof = crush.get("profitable", False)
            parts.append(f'<div class="grid grid-2"><div class="mc"><div class="mc-label">Current (USD/MT)</div><div class="mc-val {"up" if prof else "down"}">${cur:,.1f}</div><div class="mc-delta {"up" if prof else "down"}">{"Profitable" if prof else "Negative"}</div></div><div class="chart-box">{_fig_to_html(fig)}</div></div>')
        except Exception as e:
            log.warning("  Crush spread chart failed: %s", e)

        parts.append('<hr class="divider">')

    # Oil/Meal ratio
    omr = data.get("oil_meal_ratio")
    if omr and omr.get("series") is not None:
        parts.append('<div class="subhdr">Oil/Meal Ratio</div>')
        fig = build_oil_meal_ratio_chart(omr)
        parts.append(f'<div class="grid grid-2"><div class="mc"><div class="mc-label">Current</div><div class="mc-val">{omr["current"]:.3f}</div><div class="mc-delta muted">60d avg: {omr["avg_60d"]:.3f}</div></div><div class="chart-box">{_fig_to_html(fig)}</div></div>')

    # Soy oil share
    share = data.get("soy_oil_share")
    if share:
        parts.append(f'<div class="mc" style="margin-bottom:24px;"><div class="mc-label">Soy Oil Share of Crush</div><div class="mc-val">{share:.1f}%</div><div class="caption">Higher = biodiesel demand pulling oil; Lower = feed demand pulling meal</div></div>')

    # Oil vs Palm
    ovp = data.get("oil_vs_palm")
    if ovp:
        parts.append('<hr class="divider"><div class="subhdr">Soy Oil vs Palm Oil</div>')
        cards = ['<div class="grid grid-2">']
        so = ovp.get("soy_oil")
        po = ovp.get("palm_oil")
        if so:
            swk = ovp.get("soy_oil_weekly_chg")
            sc = "up" if swk and swk >= 0 else "down" if swk else "muted"
            cards.append(f'<div class="mc"><div class="mc-label">Soy Oil ({_esc(ovp.get("soy_oil_unit", "USD/MT"))})</div><div class="mc-val">{so:,.2f}</div><div class="mc-delta {sc}">{delta_str(swk)}</div></div>')
        if po:
            pwk = ovp.get("palm_oil_weekly_chg")
            pc = "up" if pwk and pwk >= 0 else "down" if pwk else "muted"
            cards.append(f'<div class="mc"><div class="mc-label">Palm Oil ({_esc(ovp.get("palm_oil_unit", "MYR/MT"))})</div><div class="mc-val">{po:,.2f}</div><div class="mc-delta {pc}">{delta_str(pwk)}</div></div>')
        cards.append('</div>')
        parts.append("\n".join(cards))

    # Bean/Corn ratio
    bcr = data.get("bean_corn_ratio")
    if bcr and bcr.get("series") is not None:
        parts.append('<hr class="divider"><div class="subhdr">Soybean/Corn Ratio (Acreage Signal)</div>')
        fig = build_bean_corn_ratio_chart(bcr)
        label = "Above avg = soybeans expensive vs corn" if bcr["current"] > bcr["avg_1y"] else "Below avg = corn expensive vs soy"
        parts.append(f'<div class="grid grid-2"><div><div class="mc" style="margin-bottom:16px;"><div class="mc-label">Current</div><div class="mc-val">{bcr["current"]:.2f}</div></div><div class="mc"><div class="mc-label">1Y Average</div><div class="mc-val">{bcr["avg_1y"]:.2f}</div><div class="caption">{label}</div></div></div><div class="chart-box">{_fig_to_html(fig)}</div></div>')

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Risk Monitor HTML
# ---------------------------------------------------------------------------
def _build_risk_monitor(data: dict) -> str:
    if not data:
        return ""

    parts = []

    # Currencies
    currencies = data.get("currencies", {})
    if currencies:
        parts.append('<div class="subhdr">Key Currencies</div>')
        pairs = list(currencies.items())
        for row_start in range(0, len(pairs), 3):
            row = pairs[row_start:row_start + 3]
            parts.append(f'<div class="grid grid-{len(row)}">')
            for pair, info in row:
                wk = info.get("weekly_chg")
                wc = "up" if wk and wk >= 0 else "down" if wk else "muted"
                mo_str = f'<div class="caption">30d: {info["monthly_chg"]:+.1f}%</div>' if info.get("monthly_chg") is not None else ""
                parts.append(f'<div class="mc"><div class="mc-label">{_esc(pair)}</div><div class="mc-val">{info["close"]:.4f}</div><div class="mc-delta {wc}">{delta_str(wk)}</div>{mo_str}</div>')
            parts.append('</div>')
        parts.append('<hr class="divider">')

    # COT
    cot = data.get("cot", {})
    if cot:
        parts.append('<div class="subhdr">COT Positioning</div>')
        fig = build_cot_chart(cot)
        parts.append(f'<div class="chart-box">{_fig_to_html(fig)}</div>')

        # WoW changes
        for leg, info in cot.items():
            wow = info.get("spec_wow")
            if wow:
                wc = "up" if wow >= 0 else "down"
                parts.append(f'<div style="font-size:13px; padding:2px 0;"><span class="muted">{_esc(leg)}</span> spec WoW: <span class="{wc}">{wow:+,.0f}</span></div>')
        parts.append('<hr class="divider">')

    # Weather
    weather = data.get("weather_alerts", [])
    if weather:
        parts.append('<div class="subhdr">Weather Alerts</div>')
        for w in weather:
            parts.append(f'<div class="alert alert-warn">{_esc(w.get("region", ""))}: {_esc(w.get("alert", ""))} — Max {w.get("temp_max", "N/A")}C, Precip {w.get("precip", 0):.0f}mm</div>')
        parts.append('<hr class="divider">')

    # Options
    options = data.get("options", {})
    if options:
        parts.append('<div class="subhdr">Options Sentiment</div>')
        cards = [f'<div class="grid grid-{min(len(options), 3)}">']
        for leg, info in options.items():
            pcr = info.get("put_call_ratio")
            if pcr:
                sentiment = "Bearish" if pcr > 1 else "Bullish" if pcr < 0.7 else "Neutral"
                sc = "down" if pcr > 1 else "up" if pcr < 0.7 else "muted"
                cards.append(f'<div class="mc"><div class="mc-label">{_esc(leg)}</div><div class="mc-val">{pcr:.2f}</div><div class="mc-delta {sc}">{sentiment}</div></div>')
        cards.append('</div>')
        parts.append("\n".join(cards))
        parts.append('<hr class="divider">')

    # Correlations
    try:
        from analysis.correlations import rolling_correlation
        from pipeline.query import read_currencies, read_prices

        all_prices = read_prices()
        all_currencies = read_currencies()

        corr_series = {}
        for name in ["Soybeans", "Soybean Oil", "Corn"]:
            subset = all_prices[all_prices["commodity"] == name].copy() if not all_prices.empty else pd.DataFrame()
            if not subset.empty:
                subset["Date"] = pd.to_datetime(subset["Date"])
                subset = subset.set_index("Date").sort_index()
                corr_series[name] = subset["Close"]

        brl_df = pd.DataFrame()
        if not all_currencies.empty:
            brl_sub = all_currencies[all_currencies["pair"] == "BRL/USD"].copy()
            if not brl_sub.empty:
                brl_sub["Date"] = pd.to_datetime(brl_sub["Date"])
                brl_df = brl_sub.set_index("Date").sort_index()

        pairs = []
        if "Soybeans" in corr_series and not brl_df.empty:
            pairs.append(("Soybeans vs BRL/USD", corr_series["Soybeans"], brl_df["Close"]))
        if "Soybeans" in corr_series and "Soybean Oil" in corr_series:
            pairs.append(("Soybeans vs Soy Oil", corr_series["Soybeans"], corr_series["Soybean Oil"]))
        if "Soybeans" in corr_series and "Corn" in corr_series:
            pairs.append(("Soybeans vs Corn", corr_series["Soybeans"], corr_series["Corn"]))

        if pairs:
            parts.append('<div class="subhdr">Rolling Correlations</div>')
            fig = build_correlations_chart(pairs, rolling_correlation)
            parts.append(f'<div class="chart-box">{_fig_to_html(fig)}</div>')
    except Exception:
        pass

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Forward Curves HTML
# ---------------------------------------------------------------------------
def _build_forward_curves(data: dict) -> str:
    if not data:
        return ""

    parts = []
    for leg in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
        if leg not in data:
            continue
        leg_data = data[leg]
        curve_df_mt = leg_data.get("curve_data_mt", leg_data.get("curve_data"))
        analysis = leg_data.get("analysis", {})
        cal = leg_data.get("calendar_spread", {})
        unit = leg_data.get("unit", "USD/MT")

        if curve_df_mt is None or curve_df_mt.empty:
            continue

        parts.append(f'<div class="subhdr">{_esc(leg)}</div>')

        # Metrics
        if analysis:
            try:
                from pipeline.units import to_metric_tons
                front_mt = to_metric_tons(analysis.get("front_price", 0), leg)
                back_mt = to_metric_tons(analysis.get("back_price", 0), leg)
            except Exception:
                front_mt = analysis.get("front_price", 0)
                back_mt = analysis.get("back_price", 0)
            spread_pct = analysis.get("spread_pct", 0)
            parts.append('<div class="grid grid-4">')
            parts.append(f'<div class="mc"><div class="mc-label">Structure</div><div class="mc-val">{_esc(analysis.get("structure", "N/A").title())}</div></div>')
            parts.append(f'<div class="mc"><div class="mc-label">Front</div><div class="mc-val">{front_mt:,.1f}</div></div>')
            parts.append(f'<div class="mc"><div class="mc-label">Back</div><div class="mc-val">{back_mt:,.1f}</div></div>')
            parts.append(f'<div class="mc"><div class="mc-label">Spread</div><div class="mc-val">{spread_pct:+.1f}%</div></div>')
            parts.append('</div>')

        # Chart
        fig = build_forward_curve_chart(curve_df_mt, leg, unit)
        parts.append(f'<div class="chart-box">{_fig_to_html(fig)}</div>')

        # Calendar spread
        if cal:
            parts.append(f'<div class="caption">Front spread: {_esc(cal.get("near_label", ""))} -> {_esc(cal.get("far_label", ""))}: {cal.get("spread", 0):+.2f} ({cal.get("spread_pct", 0):+.1f}%)</div>')

        parts.append('<hr class="divider">')

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Seasonal HTML
# ---------------------------------------------------------------------------
def _build_seasonal(data: dict) -> str:
    if not data:
        return ""

    parts = []
    for leg in ["Soybeans", "Soybean Oil", "Soybean Meal"]:
        if leg not in data:
            continue
        leg_data = data[leg]
        monthly = leg_data.get("monthly")
        vs_seasonal = leg_data.get("vs_seasonal", {})
        unit = leg_data.get("unit", "USD/MT")

        parts.append(f'<div class="subhdr">{_esc(leg)}</div>')

        # Metrics
        if vs_seasonal:
            dev = vs_seasonal.get("deviation_pct", 0)
            dc = "up" if dev > 0 else "down"
            parts.append('<div class="grid grid-3">')
            parts.append(f'<div class="mc"><div class="mc-label">Current ({unit})</div><div class="mc-val">{vs_seasonal["current_price"]:,.1f}</div></div>')
            parts.append(f'<div class="mc"><div class="mc-label">Seasonal Avg</div><div class="mc-val">{vs_seasonal["seasonal_avg"]:,.1f}</div></div>')
            parts.append(f'<div class="mc"><div class="mc-label">vs Seasonal</div><div class="mc-val {dc}">{dev:+.1f}%</div><div class="mc-delta {dc}">{"Above" if dev > 0 else "Below"}</div></div>')
            parts.append('</div>')

        # Chart
        if monthly is not None and not monthly.empty:
            fig = build_seasonal_chart(monthly, vs_seasonal, leg, unit)
            parts.append(f'<div class="chart-box">{_fig_to_html(fig)}</div>')

        parts.append('<hr class="divider">')

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Briefing + Health
# ---------------------------------------------------------------------------
def _build_briefing_text(text: str) -> str:
    """HTML-escape and add color hints to the briefing text."""
    escaped = _esc(text)
    # Colorize directional numbers
    import re
    escaped = re.sub(r'(\+\d+\.\d+%)', r'<span style="color:var(--bullish)">\1</span>', escaped)
    escaped = re.sub(r'(-\d+\.\d+%)', r'<span style="color:var(--bearish)">\1</span>', escaped)
    # Section headers (lines starting with ---)
    escaped = re.sub(r'^(--- .+ ---)$', r'<span style="color:var(--green-light);font-weight:600">\1</span>', escaped, flags=re.MULTILINE)
    return escaped


def _build_health_html(health: dict) -> str:
    if not health:
        return ""
    issues = health.get("issues", [])
    if not issues:
        return '<div class="alert alert-ok">All data sources healthy</div>'
    parts = []
    for issue in issues:
        sev = issue.get("severity", "warning")
        cls = "alert-err" if sev == "critical" else "alert-warn"
        parts.append(f'<div class="alert {cls}">{_esc(issue.get("table", ""))} / {_esc(issue.get("commodity", ""))}: {_esc(issue.get("message", ""))}</div>')
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main generation
# ---------------------------------------------------------------------------
def generate():
    """Generate the static HTML dashboard."""
    log.info("Starting HTML generation...")

    # Load analysts
    from analysis.soy_analytics import (
        command_center,
        demand_analysis,
        emerging_markets_analysis,
        forward_curve_analysis,
        relative_value_analysis,
        risk_analysis,
        seasonal_analysis,
        supply_analysis,
        technicals_analysis,
    )
    from analysis.briefing import generate_briefing
    from analysis.health import run_health_check

    # Call all analysts
    log.info("Calling analysts...")
    cc_data = _safe_call(command_center, "command_center")
    tech_data = _safe_call(technicals_analysis, "technicals")
    supply_data = _safe_call(supply_analysis, "supply")
    demand_data = _safe_call(demand_analysis, "demand")
    rv_data = _safe_call(relative_value_analysis, "relative_value")
    risk_data = _safe_call(risk_analysis, "risk")
    seasonal_data = _safe_call(seasonal_analysis, "seasonal")
    fc_data = _safe_call(forward_curve_analysis, "forward_curves")
    em_data = _safe_call(emerging_markets_analysis, "emerging_markets")

    log.info("Generating briefing...")
    briefing_text = _safe_call(generate_briefing, "briefing") or ""

    log.info("Running health check...")
    health = _safe_call(run_health_check, "health")

    # Build template context
    log.info("Building template context...")
    context = {
        "pages": PAGES,
        "generated_at": datetime.now(tz=None).strftime("%Y-%m-%d %H:%M UTC"),
        "freshness_items": _build_freshness_items(),
        "command_center": _build_command_center(cc_data),
        "technicals": _build_technicals(tech_data),
        "supply": _build_supply(supply_data),
        "demand": _build_demand(demand_data),
        "emerging_markets": _build_emerging_markets(em_data),
        "relative_value": _build_relative_value(rv_data),
        "risk_monitor": _build_risk_monitor(risk_data),
        "forward_curves": _build_forward_curves(fc_data),
        "seasonal": _build_seasonal(seasonal_data),
        "briefing_text": _build_briefing_text(briefing_text) if briefing_text else "",
        "briefing_uri": _to_data_uri(briefing_text) if briefing_text else "",
        "health_html": _build_health_html(health) if health else "",
    }

    # Render template
    log.info("Rendering template...")
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    template = env.get_template("dashboard.html.j2")
    html_output = template.render(**context)

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(html_output, encoding="utf-8")

    size_kb = OUTPUT_FILE.stat().st_size / 1024
    log.info("Generated %s (%.0f KB)", OUTPUT_FILE, size_kb)


if __name__ == "__main__":
    generate()
