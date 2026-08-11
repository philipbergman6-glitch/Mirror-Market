"""Generate a static HTML dashboard from Mirror Market data.

Usage:
    python scripts/generate_html.py

Reads from the SQLite database (populated by main.py), calls the same
analyst functions from the analysis layer, builds Plotly charts,
and renders a single index.html via Jinja2.
"""

import base64
import html as html_lib
import logging
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

# Ensure project root is on sys.path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.charts import (  # noqa: E402  (must follow sys.path.insert above)
    COLORS,
    build_basis_chart,
    build_bean_corn_ratio_chart,
    build_correlations_chart,
    build_cot_chart,
    build_crush_spread_chart,
    build_forward_curve_chart,
    build_oil_meal_ratio_chart,
    build_seasonal_chart,
    build_technical_chart,
    delta_str,
)
from config import (  # noqa: E402
    HEALTH_TABLE_WRITER_LAYERS,
    freshness_limit_days,
)
from scripts.validate_players import validate_players  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "docs"
OUTPUT_FILE = OUTPUT_DIR / "index.html"
TEMPLATE_DIR = PROJECT_ROOT / "app" / "templates"

# Numbered sections for the index nav (scan order)
SECTIONS = [
    {"id": "overnight", "no": "01", "name": "Overnight"},
    {"id": "signals", "no": "02", "name": "Signals"},
    {"id": "relative-value", "no": "03", "name": "Crush & Value"},
    {"id": "supply-demand", "no": "04", "name": "Supply & Demand"},
    {"id": "risk", "no": "05", "name": "Risk"},
    {"id": "forward-curves", "no": "06", "name": "Curves"},
    {"id": "seasonal", "no": "07", "name": "Seasonal"},
    {"id": "technicals", "no": "08", "name": "Technicals"},
    {"id": "briefing", "no": "09", "name": "Briefing"},
    {"id": "about", "no": "10", "name": "About"},
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
def _coverage_label(row) -> str | None:
    """Render "14/19" when a layer ran below full key coverage, else None (#182).

    Its own field, deliberately not appended to the age string: that string
    already carries status prose ("failed · last good 3d ago", "never",
    "disabled"), and overloading it further is how the per-layer-cadence bug
    arose. Full-coverage layers render nothing — a badge shown every day on
    every healthy layer stops being read.
    """
    returned, expected = row.get("keys_returned"), row.get("keys_expected")
    if pd.isna(returned) or pd.isna(expected) or not expected:
        return None
    returned, expected = int(returned), int(expected)
    return f"{returned}/{expected}" if returned < expected else None


def _build_freshness_items() -> list[dict]:
    """Build data freshness sidebar items."""
    try:
        from pipeline.query import read_freshness
        freshness = read_freshness()
    except Exception:
        return []

    if freshness.empty:
        return []

    now = datetime.now(timezone.utc)
    items = []
    for _, row in freshness.iterrows():
        layer = row["layer_name"]
        last = row["last_success"]
        row_status = str(row.get("status") or "success")

        # An intentionally disabled layer must not read as fresh or as an
        # outage — it gets its own bucket and is excluded from counts.
        if row_status == "disabled":
            items.append({"name": layer, "status": "disabled", "age": "disabled",
                          "coverage": None})
            continue

        coverage = _coverage_label(row)

        if pd.notna(last):
            last_dt = pd.to_datetime(last, utc=True)
            age = now - last_dt
            if row_status == "failed":
                # Last run failed: show the age of the last GOOD run, never
                # a green badge — the old code rendered a dead layer "0h ago".
                status = "old"
                age_str = f"failed · last good {age.days}d ago"
            elif age < timedelta(days=1):
                status = "fresh"
                age_str = f"{int(age.total_seconds() // 3600)}h ago"
            elif age <= timedelta(days=freshness_limit_days(layer)):
                # Inside the layer's own publication cadence: not today's
                # data, but not a problem either. A literal 7 here painted
                # weekly COT and the monthlies "old" every day of their
                # normal cycle — the exact badge-blindness the per-layer
                # policy exists to prevent (issue #176).
                status = "stale"
                age_str = f"{age.days}d ago"
            else:
                status = "old"
                age_str = f"{age.days}d ago"
        else:
            status = "old"
            age_str = "failed · never succeeded" if row_status == "failed" else "never"
        items.append({"name": layer, "status": status, "age": age_str,
                      "coverage": coverage})
    return items


_HEALTH_CRITICAL_NOTE = "data health critical"

# Buckets that mean "this layer is doing what it is supposed to do".
# `fresh` is sub-day, `stale` is older than a day but still inside the
# layer's own publication cadence (`config.freshness_limit_days`) — a
# weekly COT four days after its Friday release is not a problem, and the
# masthead must not call it one (#179).
_ON_SCHEDULE_STATUSES = ("fresh", "stale")


def _apply_health_criticals(freshness_items: list[dict], health: dict | None) -> dict:
    """Demote, in place, every layer a health critical is attributed to.

    `data_freshness` only records that a layer ran; `analysis.health`
    checks whether the rows it wrote are actually there and current. A
    layer can satisfy the first and fail the second, which is how
    "17/17 layers fresh" used to render above a DATA HEALTH section full
    of criticals (issue #58). Demoted layers get their own `degraded`
    status so the sidebar badge agrees with the masthead count.

    Criticals on a table no layer claims (`table` is "all" when the DB is
    missing entirely) can't be pinned to a layer; they are counted and
    reported instead of silently dropped.

    Returns {"degraded_layers", "critical_count", "unmapped_critical_tables"}.
    """
    issues = (health or {}).get("issues") or []
    criticals = [i for i in issues if i.get("severity") == "critical"]

    degraded: set[str] = set()
    unmapped: set[str] = set()
    for issue in criticals:
        table = issue.get("table", "")
        writers = HEALTH_TABLE_WRITER_LAYERS.get(table)
        if not writers:
            unmapped.add(table)
            continue
        degraded.update(writers)

    if unmapped:
        log.error(
            "Health criticals on unmapped table(s) %s — masthead count cannot "
            "attribute them; add the table to config.HEALTH_TABLE_WRITER_LAYERS",
            ", ".join(sorted(unmapped)),
        )

    # Demote, never downgrade: `old` (the layer is past its cadence, or its
    # last run failed) is a stronger statement than `degraded`, and a layer
    # that already lost its on-schedule badge doesn't need to lose it twice.
    # `stale` does get demoted — it counts as on-schedule (#179), so leaving
    # it would let a health-critical layer stay inside the masthead count.
    for item in freshness_items:
        if item["name"] not in degraded or item["status"] == "disabled":
            continue
        if item["status"] in _ON_SCHEDULE_STATUSES:
            item["status"] = "degraded"
        if _HEALTH_CRITICAL_NOTE not in item["age"]:
            item["age"] = f"{item['age']} · {_HEALTH_CRITICAL_NOTE}"

    return {
        "degraded_layers": sorted(degraded),
        "critical_count": len(criticals),
        "unmapped_critical_tables": sorted(unmapped),
    }


def _build_masthead(freshness_items: list[dict], now: datetime,
                    health: dict | None = None) -> dict:
    """Masthead meta: date line, on-schedule counts, late-layer note.

    The headline claim is "on schedule", not "ran today" (#179): the count
    is over `_ON_SCHEDULE_STATUSES`, so a weekly or monthly layer inside
    its own cadence is not held against it. Counting sub-day freshness
    here made the number structurally unreachable and put healthy layers
    in the warning-coloured late note every day of their normal cycle —
    the sub-day distinction survives, in the Layer Freshness table where
    `fresh` and `stale` are still separate badges.

    Disabled layers are excluded from both numerator and denominator so
    "N/M layers on schedule" only describes layers that are supposed to
    run. Health criticals demote their writing layers first — see
    `_apply_health_criticals`.
    """
    health_summary = _apply_health_criticals(freshness_items, health)
    active = [i for i in freshness_items if i["status"] != "disabled"]
    on_schedule = [i for i in active if i["status"] in _ON_SCHEDULE_STATUSES]
    late = [i for i in active if i["status"] not in _ON_SCHEDULE_STATUSES]
    return {
        "day_line": now.strftime("%A · %-d %B %Y"),
        "on_schedule_count": len(on_schedule),
        "total_layers": len(active),
        "late_layers": late,
        **health_summary,
    }


def _build_public_trust_metadata(trust_state) -> dict | None:
    """Format DT-20 public trust state for static dashboard display."""

    if trust_state is None:
        return None

    public_state = trust_state.to_public_dict() if hasattr(trust_state, "to_public_dict") else dict(trust_state)

    critical_freshness = public_state.get("critical_freshness") or {}
    degraded_dataset_ids = tuple(public_state.get("degraded_dataset_ids") or ())
    critical_numbers = tuple(public_state.get("critical_numbers") or ())
    return {
        "edition_id": public_state["edition_id"],
        "generated_at": public_state["generated_at"],
        "critical_freshness": [
            {"dataset_id": dataset_id, "freshness": freshness}
            for dataset_id, freshness in sorted(critical_freshness.items())
        ],
        "degraded_dataset_ids": degraded_dataset_ids,
        "critical_numbers": critical_numbers,
    }


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
            "as_of": leg_info.get("as_of") or "",
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
        "as_of": crush.get("as_of") or "",
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
        "as_of": km.get("brl_usd_date") or "",
    })

    dollar_val = km.get("dollar_index")
    key_metrics.append({
        "label": "Dollar Index",
        "value": f"{dollar_val:.2f}" if dollar_val else "N/A",
        "val_class": "",
        "delta": "",
        "delta_class": "muted",
        "as_of": km.get("dollar_index_date") or "",
    })

    cny_val = km.get("cny_usd")
    key_metrics.append({
        "label": "CNY/USD",
        "value": f"{cny_val:.4f}" if cny_val else "N/A",
        "val_class": "",
        "delta": "",
        "delta_class": "muted",
        "as_of": km.get("cny_usd_date") or "",
    })

    # DCE board crush (China story) — CNY/MT, USD/MT beneath when available.
    dce_crush = km.get("dce_crush_cny_mt")
    dce_crush_usd = km.get("dce_crush_usd_mt")
    key_metrics.append({
        "label": "DCE Board Crush",
        "value": f"CNY {dce_crush:+,.0f}" if dce_crush is not None else "N/A",
        "val_class": (
            "up" if dce_crush is not None and dce_crush > 0
            else "down" if dce_crush is not None else ""
        ),
        "delta": f"${dce_crush_usd:+,.0f}/MT" if dce_crush_usd is not None else "",
        "delta_class": "muted",
        "as_of": km.get("dce_crush_date") or "",
    })

    # Signals
    signals = []
    for sig in data.get("signals", []):
        sev = sig.get("severity", "info")
        signals.append({
            "severity": sev,
            "severity_label": sev.upper(),
            "commodity": sig.get("commodity", ""),
            "message": sig.get("description") or sig.get("message", ""),
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
            "message": s.get("description") or s.get("message", ""),
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
                period = info.get("period", "")
                period_str = f' <span style="color:var(--text-dim);">· {_esc(period)}</span>' if period else ""
                lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(attr_name)}: <strong style="color:var(--text)">{val:,.0f}</strong> {_esc(unit)}{rev_str}{period_str}</div>')
        out["wasde_html"] = "\n".join(lines) if lines else ""

    # Stocks-to-use (US balance sheet, from PSD)
    stu = data.get("stocks_to_use", {})
    if stu:
        cards = ['<div class="grid grid-4">']
        for commodity, info in stu.items():
            ratio_pct = info["current_ratio"] * 100
            my = info["marketing_year"]
            is_tight = info.get("is_tight", False)
            lo = info.get("prior_low")
            hi = info.get("prior_high")
            range_str = ""
            if lo is not None and hi is not None:
                range_str = f"Prior 5-yr: {lo * 100:.1f}%–{hi * 100:.1f}%"
            delta_cls = "down" if is_tight else "muted"
            delta_text = "[TIGHT] below 5-yr low" if is_tight else range_str
            val_cls = ' class="down"' if is_tight else ""
            cards.append(
                f'<div class="mc">'
                f'<div class="mc-label">{_esc(commodity)} (MY {my})</div>'
                f'<div class="mc-val"{val_cls}>{ratio_pct:.1f}%</div>'
                f'<div class="mc-delta {delta_cls}">{_esc(delta_text)}</div>'
                f'</div>'
            )
        cards.append('</div>')
        out["stocks_to_use_html"] = "\n".join(cards)

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
                period = info.get("period", "")
                period_str = f' <span style="color:var(--text-dim);">· {_esc(period)}</span>' if period else ""
                lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(attr_name)}: <strong style="color:var(--text)">{val:,.0f}</strong> {_esc(unit)}{period_str}</div>')
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
        crop_year = conab.get("crop_year", "")
        year_str = f" · {_esc(crop_year)}" if crop_year else ""
        html_parts = ['<div class="grid grid-3">']
        html_parts.append(f'<div class="mc"><div class="mc-label">CONAB (Brazil)</div><div class="mc-val">{cp:,.0f}</div><div class="mc-delta muted">1000 MT{year_str}</div></div>')
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
        cond_week = crop.get("condition_week")
        if cond_week and crop.get("condition"):
            lines.append(f'<div class="caption">Condition — week ending {_esc(cond_week)}</div>')
        for item in crop.get("condition", []):
            lines.append(f'<div style="font-size:13px; color:var(--text-muted); padding:2px 0;">- {_esc(item["desc"])}: <strong style="color:var(--text)">{item["value"]}%</strong></div>')
        prog_week = crop.get("progress_week")
        if prog_week and crop.get("progress"):
            lines.append(f'<div class="caption">Progress — week ending {_esc(prog_week)}</div>')
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
    es_weeks = data.get("export_sales", {})
    if china:
        cards = ['<div class="grid grid-3">']
        for commodity, info in china.items():
            we = es_weeks.get(commodity, {}).get("week_ending")
            week_str = ""
            if we is not None:
                week_str = f' · w/e {we.strftime("%m/%d") if hasattr(we, "strftime") else _esc(we)}'
            if info["net_sales"] == 0:
                cards.append(f'<div class="mc"><div class="mc-label">{_esc(commodity)}</div><div class="mc-val">—</div><div class="mc-delta muted">no net Chinese purchases{week_str}</div></div>')
            else:
                cards.append(f'<div class="mc"><div class="mc-label">{_esc(commodity)}</div><div class="mc-val">{info["net_sales"]:,.0f}</div><div class="mc-delta muted">MT | {info["pct_of_total"]:.0f}% of total{week_str}</div></div>')
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
            bio_date = info.get("date")
            date_str = ""
            if bio_date is not None:
                date_str = f'<div class="caption">as of {bio_date.strftime("%Y-%m-%d") if hasattr(bio_date, "strftime") else _esc(bio_date)}</div>'
            cards.append(f'<div class="mc"><div class="mc-label">{_esc(name)}</div><div class="mc-val">{info["value"]:,.2f}</div><div class="mc-delta {dc}">{delta_str(chg)}</div>{date_str}</div>')
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

        # India domestic \u2014 bean-only mandi series since the 2026-08 rebuild
        dom_india = info.get("india_domestic", {})
        if country_name == "India" and not dom_india:
            # An empty card reads as a bug; say what the feed is and that
            # it is pending rather than silently omitting the series.
            parts.append('<div class="alert alert-warn">Mandi domestic price (Agmarknet via data.gov.in): awaiting session data \u2014 feed throttled or mandis closed</div>')
        if dom_india:
            parts.append('<div class="subhdr" style="font-size:14px;">Mandi Domestic Price (Agmarknet, MP median)</div>')
            mandi_date = dom_india.get("soybean_mandi_date")
            basis_date = dom_india.get("basis_date")
            cards = ['<div class="grid grid-3">']
            inr = dom_india.get("soybean_mandi_inr")
            if inr:
                d = f" \u00B7 {_esc(mandi_date)}" if mandi_date else ""
                cards.append(f'<div class="mc"><div class="mc-label">Soybean</div><div class="mc-val">\u20B9{inr:,.0f}</div><div class="mc-delta muted">INR/MT{d}</div></div>')
            usd = dom_india.get("soybean_mandi_usd")
            if usd:
                d = f" \u00B7 {_esc(basis_date or mandi_date)}" if (basis_date or mandi_date) else ""
                cards.append(f'<div class="mc"><div class="mc-label">Soybean (USD)</div><div class="mc-val">${usd:,.1f}</div><div class="mc-delta muted">USD/MT{d}</div></div>')
            premium = dom_india.get("bean_premium_usd")
            if premium is not None:
                pc = "up" if premium > 0 else "down"
                d = f" \u00B7 as of {_esc(basis_date)}" if basis_date else ""
                cards.append(f'<div class="mc"><div class="mc-label">vs CBOT Beans</div><div class="mc-val {pc}">${premium:+,.1f}</div><div class="mc-delta muted">{"premium" if premium > 0 else "discount"}{d}</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        # Brazil domestic
        dom_brazil = info.get("brazil_domestic", {})
        if dom_brazil:
            brl = dom_brazil.get("cepea_soy_brl")
            usd = dom_brazil.get("cepea_soy_usd")
            basis = dom_brazil.get("brazil_cbot_basis_usd")
            # Only render the CEPEA block when CEPEA actually has values —
            # AgRural populates the same dict, and an unconditional header
            # left "CEPEA Farm-Gate Price" hanging over nothing.
            if brl or usd or basis is not None:
                cepea_date = dom_brazil.get("cepea_soy_date")
                basis_date = dom_brazil.get("basis_date")
                parts.append('<div class="subhdr" style="font-size:14px;">CEPEA Farm-Gate Price</div>')
                cards = ['<div class="grid grid-3">']
                if brl:
                    d = f" · {_esc(cepea_date)}" if cepea_date else ""
                    cards.append(f'<div class="mc"><div class="mc-label">CEPEA Soybean</div><div class="mc-val">R${brl:,.2f}</div><div class="mc-delta muted">BRL/MT{d}</div></div>')
                if usd:
                    d = f" · {_esc(basis_date or cepea_date)}" if (basis_date or cepea_date) else ""
                    cards.append(f'<div class="mc"><div class="mc-label">CEPEA (USD)</div><div class="mc-val">${usd:,.1f}</div><div class="mc-delta muted">USD/MT{d}</div></div>')
                if basis is not None:
                    bc = "up" if basis > 0 else "down"
                    d = f" · as of {_esc(basis_date)}" if basis_date else ""
                    cards.append(f'<div class="mc"><div class="mc-label">Brazil-CBOT Basis</div><div class="mc-val {bc}">${basis:+,.1f}</div><div class="mc-delta muted">{"premium" if basis > 0 else "discount"}{d}</div></div>')
                cards.append('</div>')
                parts.append("\n".join(cards))

            # AgRural Paranaguá FOB — USD/MT only (raw BRL/saca is not redistributable
            # under AgRural terms; the derived USD basis is the publishable figure).
            agrural_usd = dom_brazil.get("agrural_soy_usd")
            agrural_basis = dom_brazil.get("agrural_cbot_basis_usd")
            if agrural_usd is not None or agrural_basis is not None:
                ag_date = dom_brazil.get("agrural_basis_date")
                ag_d = f" · {_esc(ag_date)}" if ag_date else ""
                parts.append('<div class="subhdr" style="font-size:14px;">AgRural Paranaguá FOB</div>')
                ag_cards = ['<div class="grid grid-3">']
                if agrural_usd is not None:
                    ag_cards.append(f'<div class="mc"><div class="mc-label">AgRural (USD)</div><div class="mc-val">${agrural_usd:,.1f}</div><div class="mc-delta muted">USD/MT{ag_d}</div></div>')
                if agrural_basis is not None:
                    ab_cls = "up" if agrural_basis > 0 else "down"
                    ag_cards.append(f'<div class="mc"><div class="mc-label">AgRural−CBOT Basis</div><div class="mc-val {ab_cls}">${agrural_basis:+,.1f}</div><div class="mc-delta muted">{"premium" if agrural_basis > 0 else "discount"}{" · as of " + _esc(ag_date) if ag_date else ""}</div></div>')
                ag_cards.append('</div>')
                parts.append("\n".join(ag_cards))

        # South Africa SAFEX
        dom_sa = info.get("south_africa_domestic", {})
        if dom_sa:
            # "Last Traded", not "Settlement": the free Grain SA table has no
            # settlement column and the JSE's official MTM is licensed (#157).
            parts.append('<div class="subhdr" style="font-size:14px;">SAFEX Last Traded Prices</div>')
            zar = dom_sa.get("soybean_safex_zar")
            usd = dom_sa.get("soybean_safex_usd")
            basis = dom_sa.get("safex_cbot_basis_usd")
            safex_date = dom_sa.get("soybean_safex_date")
            basis_date = dom_sa.get("basis_date")
            cards = ['<div class="grid grid-3">']
            if zar:
                d = f" · {_esc(safex_date)}" if safex_date else ""
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX Soybean</div><div class="mc-val">R{zar:,.0f}</div><div class="mc-delta muted">ZAR/MT{d}</div></div>')
            if usd:
                d = f" · {_esc(basis_date or safex_date)}" if (basis_date or safex_date) else ""
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX (USD)</div><div class="mc-val">${usd:,.1f}</div><div class="mc-delta muted">USD/MT{d}</div></div>')
            if basis is not None:
                bc = "up" if basis > 0 else "down"
                d = f" · as of {_esc(basis_date)}" if basis_date else ""
                cards.append(f'<div class="mc"><div class="mc-label">SAFEX-CBOT Basis</div><div class="mc-val {bc}">${basis:+,.1f}</div><div class="mc-delta muted">{"premium" if basis > 0 else "parity"}{d}</div></div>')
            cards.append('</div>')
            parts.append("\n".join(cards))

        # South Africa SAGIS weekly producer deliveries (physical flow).
        # The SA leg is a flow story, not a price story: SAFEX above is
        # licence-capped, while SAGIS grants reproduction with attribution,
        # which is rendered here and must not be dropped (#202).
        sa_flow = info.get("south_africa_deliveries", {})
        if sa_flow:
            parts.append(
                '<div class="subhdr" style="font-size:14px;">'
                'SAGIS Weekly Producer Deliveries</div>'
            )
            for key, label in (
                ("soybeans", "Soybeans"),
                ("sunflower", "Sunflower Seed"),
            ):
                pace = sa_flow.get(key)
                if not pace:
                    continue
                flow_cards = ['<div class="grid grid-3">']
                week_end = pace.get("week_end")
                wk = pace.get("week_number")
                sub = f"MT · wk {wk}" + (f" ending {_esc(week_end)}" if week_end else "")
                flow_cards.append(
                    f'<div class="mc"><div class="mc-label">{label} — Latest Week</div>'
                    f'<div class="mc-val">{pace["week_total_mt"]:,.0f}</div>'
                    f'<div class="mc-delta muted">{sub}</div></div>'
                )
                season = _esc(pace.get("season_label", ""))
                yoy = pace.get("yoy_pct")
                yoy_str = f"{yoy:+.1f}% YoY" if yoy is not None else "season-to-date"
                yoy_cls = "" if yoy is None else ("up" if yoy > 0 else "down")
                flow_cards.append(
                    f'<div class="mc"><div class="mc-label">{label} — Season {season}</div>'
                    f'<div class="mc-val">{pace["progressive_mt"]:,.0f}</div>'
                    f'<div class="mc-delta {yoy_cls}">{yoy_str}</div></div>'
                )
                vs_avg = pace.get("vs_avg3_pct")
                if vs_avg is not None:
                    avg_cls = "up" if vs_avg > 0 else "down"
                    flow_cards.append(
                        f'<div class="mc"><div class="mc-label">{label} — vs 3y Avg</div>'
                        f'<div class="mc-val {avg_cls}">{vs_avg:+.1f}%</div>'
                        f'<div class="mc-delta muted">same week number</div></div>'
                    )
                flow_cards.append('</div>')
                parts.append("\n".join(flow_cards))
            attribution = sa_flow.get("attribution")
            if attribution:
                parts.append(
                    f'<div class="muted" style="font-size:12px;">{_esc(attribution)}</div>'
                )

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
            crush_asof = crush.get("as_of")
            crush_d = f'<div class="caption">as of {_esc(crush_asof)}</div>' if crush_asof else ""
            parts.append(f'<div class="grid grid-2"><div class="mc"><div class="mc-label">Current (USD/MT)</div><div class="mc-val {"up" if prof else "down"}">${cur:,.1f}</div><div class="mc-delta {"up" if prof else "down"}">{"Profitable" if prof else "Negative"}</div>{crush_d}</div><div class="chart-box">{_fig_to_html(fig)}</div></div>')
        except Exception as e:
            log.warning("  Crush spread chart failed: %s", e)

        parts.append('<hr class="divider">')

    # Brazil basis — Paranaguá FOB (primary) + CEPEA Paraná (secondary) vs CBOT
    basis = data.get("basis", {})
    sources = basis.get("sources", {}) if basis else {}
    primary_label = basis.get("primary") if basis else None
    if sources and primary_label and primary_label in sources:
        primary_stats = sources[primary_label]
        secondary_label = next((lbl for lbl in sources if lbl != primary_label), None)
        secondary_stats = sources.get(secondary_label) if secondary_label else None

        subhdr_label = (
            f"Brazil Basis ({primary_label} · {secondary_label} vs CBOT)"
            if secondary_label else f"Brazil Basis ({primary_label} vs CBOT)"
        )
        parts.append(f'<div class="subhdr">{subhdr_label}</div>')

        try:
            fig = build_basis_chart(basis, primary_stats)
            cur = primary_stats.get("current_usd_mt", 0.0)
            direction = primary_stats.get("direction", "")
            avg = primary_stats.get("avg_1y")
            pct = primary_stats.get("percentile_1y")
            n_obs = primary_stats.get("n_obs", 0)
            # Negative basis (Brazilian discount) is export-competitive — bullish for trade flow.
            val_class = "up" if cur < 0 else "down"
            if avg is not None and pct is not None:
                window = "1Y" if n_obs >= 252 else f"{n_obs}-session"
                delta_label = f"{window} avg ${avg:+,.1f} · {pct:.0f}th pctile"
            else:
                delta_label = f"history building ({n_obs} obs — stats at 20)"
            primary_asof = primary_stats.get("as_of")
            if primary_asof:
                delta_label = f"as of {_esc(primary_asof)} · {delta_label}"

            tiles = [
                f'<div class="mc"><div class="mc-label">{primary_label} (USD/MT)</div>'
                f'<div class="mc-val {val_class}">${cur:+,.1f}</div>'
                f'<div class="mc-delta {val_class}">Brazilian {direction}</div>'
                f'<div class="caption">{delta_label}</div></div>'
            ]
            if secondary_stats is not None:
                sec_cur = secondary_stats.get("current_usd_mt", 0.0)
                sec_direction = secondary_stats.get("direction", "")
                sec_class = "up" if sec_cur < 0 else "down"
                sec_asof = secondary_stats.get("as_of")
                sec_d = f'<div class="caption">as of {_esc(sec_asof)}</div>' if sec_asof else ""
                tiles.append(
                    f'<div class="mc"><div class="mc-label">{secondary_label} (USD/MT)</div>'
                    f'<div class="mc-val {sec_class}">${sec_cur:+,.1f}</div>'
                    f'<div class="mc-delta {sec_class}">Brazilian {sec_direction}</div>{sec_d}</div>'
                )
            wedge = basis.get("wedge_usd_mt")
            if wedge is not None:
                wedge_class = "up" if wedge > 0 else "down"
                tiles.append(
                    f'<div class="mc"><div class="mc-label">Port − Farm wedge</div>'
                    f'<div class="mc-val {wedge_class}">${wedge:+,.1f}</div>'
                    f'<div class="mc-delta muted">Paranaguá minus CEPEA</div></div>'
                )

            parts.append(
                f'<div class="grid grid-2">'
                f'<div>{"".join(tiles)}</div>'
                f'<div class="chart-box">{_fig_to_html(fig)}</div>'
                f'</div>'
            )
        except Exception as e:
            log.warning("  Brazil basis chart failed: %s", e)

        parts.append('<hr class="divider">')

    # Oil/Meal ratio
    omr = data.get("oil_meal_ratio")
    if omr and omr.get("series") is not None:
        parts.append('<div class="subhdr">Oil/Meal Ratio</div>')
        fig = build_oil_meal_ratio_chart(omr)
        omr_d = f'<div class="caption">as of {_esc(omr["as_of"])}</div>' if omr.get("as_of") else ""
        parts.append(f'<div class="grid grid-2"><div class="mc"><div class="mc-label">Current</div><div class="mc-val">{omr["current"]:.3f}</div><div class="mc-delta muted">60d avg: {omr["avg_60d"]:.3f}</div>{omr_d}</div><div class="chart-box">{_fig_to_html(fig)}</div></div>')

    # Soy oil share
    share = data.get("soy_oil_share")
    if share:
        share_asof = data.get("soy_oil_share_as_of")
        share_d = f" · as of {_esc(share_asof)}" if share_asof else ""
        parts.append(f'<div class="mc" style="margin-bottom:24px;"><div class="mc-label">Soy Oil Share of Crush</div><div class="mc-val">{share:.1f}%</div><div class="caption">Higher = biodiesel demand pulling oil; Lower = feed demand pulling meal{share_d}</div></div>')

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
            so_asof = ovp.get("soy_oil_as_of")
            so_d = f'<div class="caption">as of {_esc(so_asof)}</div>' if so_asof else ""
            cards.append(f'<div class="mc"><div class="mc-label">Soy Oil ({_esc(ovp.get("soy_oil_unit", "USD/MT"))})</div><div class="mc-val">{so:,.2f}</div><div class="mc-delta {sc}">{delta_str(swk)}</div>{so_d}</div>')
        if po:
            pwk = ovp.get("palm_oil_weekly_chg")
            pc = "up" if pwk and pwk >= 0 else "down" if pwk else "muted"
            po_asof = ovp.get("palm_oil_as_of")
            po_d = f'<div class="caption">as of {_esc(po_asof)}</div>' if po_asof else ""
            cards.append(f'<div class="mc"><div class="mc-label">Palm Oil ({_esc(ovp.get("palm_oil_unit", "USD/MT"))})</div><div class="mc-val">{po:,.2f}</div><div class="mc-delta {pc}">{delta_str(pwk)}</div>{po_d}</div>')
        cards.append('</div>')
        parts.append("\n".join(cards))

    # Oil vs CZCE Rapeseed Oil (cross-oilseed — ICE canola has no free daily feed)
    ovr = data.get("oil_vs_rapeseed")
    if ovr:
        parts.append('<hr class="divider"><div class="subhdr">Soy Oil vs CZCE Rapeseed Oil</div>')
        cards = ['<div class="grid grid-2">']
        so = ovr.get("soy_oil")
        ro = ovr.get("rapeseed_oil")
        if so:
            swk = ovr.get("soy_oil_weekly_chg")
            sc = "up" if swk and swk >= 0 else "down" if swk else "muted"
            so_asof = ovr.get("soy_oil_as_of")
            so_d = f'<div class="caption">as of {_esc(so_asof)}</div>' if so_asof else ""
            cards.append(f'<div class="mc"><div class="mc-label">Soy Oil (USD/MT)</div><div class="mc-val">{so:,.2f}</div><div class="mc-delta {sc}">{delta_str(swk)}</div>{so_d}</div>')
        if ro:
            rwk = ovr.get("rapeseed_oil_weekly_chg")
            rc = "up" if rwk and rwk >= 0 else "down" if rwk else "muted"
            ro_caption_parts = []
            ro_cny = ovr.get("rapeseed_oil_cny")
            if ro_cny:
                ro_caption_parts.append(f"CNY {ro_cny:,.0f}/MT")
            ro_asof = ovr.get("rapeseed_oil_as_of")
            if ro_asof:
                ro_caption_parts.append(f"as of {_esc(ro_asof)}")
            ro_d = f'<div class="caption">{" · ".join(ro_caption_parts)}</div>' if ro_caption_parts else ""
            cards.append(f'<div class="mc"><div class="mc-label">CZCE Rapeseed Oil (USD/MT)</div><div class="mc-val">{ro:,.2f}</div><div class="mc-delta {rc}">{delta_str(rwk)}</div>{ro_d}</div>')
        cards.append('</div>')
        parts.append("\n".join(cards))
        spread = ovr.get("spread_usd_mt")
        if spread is not None:
            sp_class = "up" if spread > 0 else "down"
            parts.append(
                f'<div class="mc" style="margin-bottom:24px;"><div class="mc-label">Rapeseed − Soy Oil Spread</div>'
                f'<div class="mc-val {sp_class}">${spread:+,.1f}</div>'
                f'<div class="caption">CZCE rapeseed oil premium over CBOT soy oil, USD/MT — ICE canola (RS=F) has no free daily feed, CZCE is the rapeseed leg</div></div>'
            )

    # Bean/Corn ratio
    bcr = data.get("bean_corn_ratio")
    if bcr and bcr.get("series") is not None:
        parts.append('<hr class="divider"><div class="subhdr">Soybean/Corn Ratio (Acreage Signal)</div>')
        fig = build_bean_corn_ratio_chart(bcr)
        label = "Above avg = soybeans expensive vs corn" if bcr["current"] > bcr["avg_1y"] else "Below avg = corn expensive vs soy"
        bcr_d = f'<div class="caption">as of {_esc(bcr["as_of"])}</div>' if bcr.get("as_of") else ""
        parts.append(f'<div class="grid grid-2"><div><div class="mc" style="margin-bottom:16px;"><div class="mc-label">Current</div><div class="mc-val">{bcr["current"]:.2f}</div>{bcr_d}</div><div class="mc"><div class="mc-label">1Y Average</div><div class="mc-val">{bcr["avg_1y"]:.2f}</div><div class="caption">{label}</div></div></div><div class="chart-box">{_fig_to_html(fig)}</div></div>')

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
                mo_parts = []
                if info.get("monthly_chg") is not None:
                    mo_parts.append(f'30d: {info["monthly_chg"]:+.1f}%')
                if info.get("as_of"):
                    mo_parts.append(f'as of {_esc(info["as_of"])}')
                mo_str = f'<div class="caption">{" · ".join(mo_parts)}</div>' if mo_parts else ""
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
            wow = info.get("spec_net_chg")
            if wow is not None:
                wc = "up" if wow >= 0 else "down"
                cot_date = info.get("date")
                cot_d = ""
                if cot_date is not None:
                    cot_d = f' <span class="muted" style="font-size:12px;">(report {cot_date.strftime("%Y-%m-%d") if hasattr(cot_date, "strftime") else _esc(cot_date)})</span>'
                parts.append(f'<div style="font-size:13px; padding:2px 0;"><span class="muted">{_esc(leg)}</span> spec WoW: <span class="{wc}">{wow:+,.0f}</span>{cot_d}</div>')
        parts.append('<hr class="divider">')

    # Weather
    weather = data.get("weather_alerts", [])
    if weather:
        parts.append('<div class="subhdr">Weather Alerts</div>')
        for w in weather:
            w_date = w.get("date")
            w_d = ""
            if w_date is not None:
                w_d = f' ({w_date.strftime("%Y-%m-%d") if hasattr(w_date, "strftime") else _esc(w_date)})'
            parts.append(f'<div class="alert alert-warn">{_esc(w.get("region", ""))}: {_esc(w.get("alert", ""))} — Max {w.get("temp_max", "N/A")}C, Precip {w.get("precip", 0):.0f}mm{w_d}</div>')
        parts.append('<hr class="divider">')

    # Correlations
    try:
        from analysis.correlations import rolling_correlation
        from analysis.loaders import load_currencies, load_prices

        all_prices = load_prices()
        all_currencies = load_currencies()

        corr_series = {
            name: all_prices[name]["Close"]
            for name in ["Soybeans", "Soybean Oil", "Corn"]
            if name in all_prices and not all_prices[name].empty
        }
        brl_df = all_currencies.get("BRL/USD", pd.DataFrame())

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
        log.warning("Correlations section failed", exc_info=True)

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
        curve_asof = leg_data.get("as_of")
        if curve_asof:
            parts.append(f'<div class="caption">Curve snapshot as of {_esc(curve_asof)}</div>')

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
            seasonal_asof = leg_data.get("as_of")
            seasonal_d = f'<div class="caption">as of {_esc(seasonal_asof)}</div>' if seasonal_asof else ""
            parts.append('<div class="grid grid-3">')
            parts.append(f'<div class="mc"><div class="mc-label">Current ({unit})</div><div class="mc-val">{vs_seasonal["current_price"]:,.1f}</div>{seasonal_d}</div>')
            parts.append(f'<div class="mc"><div class="mc-label">Seasonal Avg</div><div class="mc-val">{vs_seasonal["seasonal_avg"]:,.1f}</div></div>')
            detrended = vs_seasonal.get("detrended_delta_pct")
            if detrended is not None:
                dc = "up" if detrended > 0 else "down"
                parts.append(f'<div class="mc"><div class="mc-label">vs Seasonal (detrended)</div><div class="mc-val {dc}">{detrended:+.1f}%</div><div class="mc-delta {dc}">{"Above" if detrended > 0 else "Below"} typical for month</div></div>')
            else:
                dev = vs_seasonal.get("deviation_pct", 0)
                dc = "up" if dev > 0 else "down"
                parts.append(f'<div class="mc"><div class="mc-label">vs 15y Avg Level</div><div class="mc-val {dc}">{dev:+.1f}%</div><div class="mc-delta {dc}">trend not removed</div></div>')
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
def generate(
    *,
    output_dir: str | Path = OUTPUT_DIR,
    public_trust_state=None,
    include_players: bool = True,
) -> dict[str, Path]:
    """Generate the static HTML dashboard."""
    log.info("Starting HTML generation...")

    # Hard-fail on players knowledge-base schema violations (issue #122) —
    # a bad country code must break the build, not ship as a dead filter.
    player_errors = validate_players()
    if player_errors:
        for err in player_errors:
            log.error("players: %s", err)
        raise SystemExit(f"players validation failed with {len(player_errors)} violation(s)")

    # Load analysts
    from analysis.briefing import generate_briefing
    from analysis.health import run_health_check
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
    now = datetime.now(timezone.utc)
    freshness_items = _build_freshness_items()
    context = {
        "sections": SECTIONS,
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        # Built before the sidebar reads `freshness_items` — the masthead
        # demotes health-critical layers in place so both agree.
        "masthead": _build_masthead(freshness_items, now, health),
        "public_trust": _build_public_trust_metadata(public_trust_state),
        "freshness_items": freshness_items,
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
    output_dir = Path(output_dir)
    output_file = output_dir / "index.html"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html_output, encoding="utf-8")

    size_kb = output_file.stat().st_size / 1024
    log.info("Generated %s (%.0f KB)", output_file, size_kb)
    artifacts = {"dashboard": output_file}

    # Players page (issue #123) — same deploy, own template. Validation above
    # already gates the knowledge base, so a failure here is a build bug and
    # must fail the run, not silently ship a dashboard without the page.
    if include_players:
        log.info("Generating players page...")
        from scripts.generate_players import generate_players_page
        artifacts["players"] = generate_players_page(output_dir / "players.html")
    return artifacts


def static_site_candidate_renderer(*, public_trust_state=None, include_players: bool = False):
    """Build a DT-20 CandidateRenderer for the existing static dashboard generator."""

    def _render_candidate_static_site(
        cache_path: Path,
        output_dir: Path,
        edition,
    ) -> dict[str, Path]:
        del cache_path, edition
        return generate(
            output_dir=output_dir,
            public_trust_state=public_trust_state,
            include_players=include_players,
        )

    return _render_candidate_static_site


def static_site_deployer(*, public_dir: str | Path = OUTPUT_DIR):
    """Build a DT-20 EditionDeployer that publishes rendered static artifacts."""

    def _deploy_static_site(edition, render) -> tuple[str, ...]:
        del edition
        public_dir_path = Path(public_dir)
        public_dir_path.mkdir(parents=True, exist_ok=True)
        evidence: list[str] = []
        for key, source_path in sorted(render.generated_artifact_paths.items()):
            target_name = "index.html" if key == "dashboard" else Path(source_path).name
            target_path = public_dir_path / target_name
            shutil.copy2(source_path, target_path)
            evidence.append(f"deployed.{key}.{target_name}")
        return tuple(evidence)

    return _deploy_static_site


if __name__ == "__main__":
    generate()
