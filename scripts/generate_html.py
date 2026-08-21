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
    build_technical_chart,
    delta_str,
)
from app.sections import (  # noqa: E402
    clip,
    emerging_markets_section,
    forward_curves_section,
    relative_value_section,
    risk_monitor_section,
    seasonal_section,
)
from config import (  # noqa: E402
    HEALTH_TABLE_WRITER_LAYERS,
    PRODUCTION_LAYERS,
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
    # M2 #144 put the eight-row ledger third — the headline's sole per-market
    # presence, and the way a reader gets to a market page (M19 #223).
    {"id": "propagation", "no": "03", "name": "Propagation"},
    # M2 #144 put the cross-market crush board fourth, built by M16 #208. The
    # CBOT-only crush spread that used to open the next section is its
    # predecessor, not its neighbour.
    {"id": "crush-board", "no": "04", "name": "Crush Board"},
    {"id": "relative-value", "no": "05", "name": "Relative Value"},
    {"id": "supply-demand", "no": "06", "name": "Supply & Demand"},
    {"id": "risk", "no": "07", "name": "Risk"},
    {"id": "forward-curves", "no": "08", "name": "Curves"},
    {"id": "seasonal", "no": "09", "name": "Seasonal"},
    {"id": "technicals", "no": "10", "name": "Technicals"},
    {"id": "briefing", "no": "11", "name": "Briefing"},
    {"id": "about", "no": "12", "name": "About"},
]

LEG_COLORS = {
    "Soybeans": COLORS["soybean"],
    "Soybean Oil": COLORS["soy_oil"],
    "Soybean Meal": COLORS["soy_meal"],
}


def _build_registry_sections() -> dict[str, dict]:
    """The two headline sections built from the market registry (03 and 04).

    One DB session for both: this renderer is also runnable standalone, and the
    market-page orchestrator's SiteContext does not reach here. Each builder
    fails on its own — a crush board that raises must not take the propagation
    ledger down with it, which is the same failure isolation the nine market
    blocks get.
    """
    from app.block_builders import SiteContext, headline_crush_board, headline_ledger
    from app.markets import load_markets

    builders = {"ledger": headline_ledger, "crush_board": headline_crush_board}
    out: dict[str, dict] = {}
    ctx = SiteContext.open()
    try:
        markets = load_markets()
        for name, build in builders.items():
            try:
                state, reason, data = build(markets, ctx)
            except Exception as e:  # noqa: BLE001 — one section, not the page
                log.warning("  headline %s failed: %s", name, e)
                state, reason, data = "empty", f"{name} could not be built for this render", {}
            out[name] = {"state": state, "reason": reason, "data": data}
    finally:
        ctx.close()
    return out


def _standalone_market_nav() -> list[dict]:
    """Market nav for a standalone `python scripts/generate_html.py` run."""
    from app.markets import compute_tiers, load_markets, nav_items

    markets = load_markets()
    return nav_items(compute_tiers(markets), markets=markets)


def _safe_call(fn, label: str):
    """Call fn(), returning None only when the callable raises.

    Rendered prose is data, not an error protocol. Briefings legitimately use
    words such as "failed" when reporting a degraded layer.
    """
    try:
        return fn()
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


def _latency_fields(layer: str, row, now: datetime) -> dict:
    """The three time facts a freshness row could never state before.

    "Last Success" answers *when we ran*. It has never answered *what the
    number is dated*, and the two come apart in exactly the case that
    matters: a run that lands before the CBOT settlement fetches
    successfully, stamps a fresh last_success, and carries yesterday's close.
    The table read "0h ago" for a price a day old, and it was not wrong — it
    was answering a different question than the one a reader was asking.

    So: ``observed`` is the newest observation date the layer received,
    ``fetched`` is when we asked, and ``data_age`` is how stale that
    observation is right now — the number a trader is actually deciding on.
    All three are blank rather than defaulted where the stamps are NULL,
    which is every row written before the instrumentation existed.
    """
    from latency.domain import LAYER_LATENCY_BY_KEY, format_delta
    from latency.measure import measure_from_rows

    if row is None:
        return {"observed": None, "fetched": None, "data_age": None, "latency_class": None}

    spec = LAYER_LATENCY_BY_KEY.get(layer)
    if spec is None:
        # Not a trader-critical layer: it carries no declared observation
        # hour, so render the date alone rather than inventing a chain for it.
        observed = row.get("observed_at")
        fetched = row.get("fetch_completed_at")
        return {
            "observed": None if pd.isna(observed) else pd.to_datetime(observed).strftime("%Y-%m-%d"),
            "fetched": None if pd.isna(fetched) else pd.to_datetime(fetched).strftime("%H:%MZ"),
            "data_age": None,
            "latency_class": None,
        }

    measurement = measure_from_rows({layer: dict(row)}, specs=(spec,))[0]
    observed_at = measurement.stamps.observed_at
    fetched_at = measurement.stamps.fetch_completed_at
    from latency.domain import age_at

    age = age_at(measurement, now)
    return {
        "observed": observed_at.strftime("%Y-%m-%d %H:%MZ") if observed_at else None,
        "fetched": fetched_at.strftime("%Y-%m-%d %H:%MZ") if fetched_at else None,
        "data_age": format_delta(age) if age is not None else None,
        "latency_class": spec.latency_class.value,
    }


def _build_freshness_items() -> list[dict]:
    """Build one freshness item for every operational production layer."""
    try:
        from pipeline.query import read_freshness
        freshness = read_freshness()
    except Exception:
        freshness = pd.DataFrame()

    now = datetime.now(timezone.utc)
    items = []
    rows = (
        freshness.set_index("layer_name").to_dict("index")
        if not freshness.empty and "layer_name" in freshness.columns
        else {}
    )
    for layer, number, source, cadence, scope in PRODUCTION_LAYERS:
        row = rows.get(layer)
        if row is None:
            items.append({
                "name": layer,
                "number": number,
                "source": source,
                "cadence": cadence,
                "scope": scope,
                "status": "not-run",
                "age": "not attempted · no recorded success",
                "coverage": None,
                **_latency_fields(layer, None, now),
            })
            continue
        last = row["last_success"]
        row_status = str(row.get("status") or "success")

        # An intentionally disabled layer must not read as fresh or as an
        # outage — it gets its own bucket and is excluded from counts.
        if row_status == "disabled":
            items.append({"name": layer, "number": number, "source": source,
                          "cadence": cadence, "scope": scope, "status": "disabled",
                          "age": "disabled", "coverage": None,
                          **_latency_fields(layer, None, now)})
            continue

        coverage = _coverage_label(row)

        if pd.notna(last):
            last_dt = pd.to_datetime(last, utc=True)
            age = now - last_dt
            if row_status in {"failed", "stale", "incomplete"}:
                status = "old"
                label = {
                    "failed": "upstream failure",
                    "stale": "stale last-known-good",
                    "incomplete": "incomplete key coverage",
                }[row_status]
                age_str = f"{label} · last good {age.days}d ago"
            elif row_status == "no_publication":
                status = "no-publication"
                age_str = f"no publication · last good {age.days}d ago"
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
            if row_status == "no_publication":
                status = "no-publication"
                age_str = "no publication · no prior observation"
            else:
                status = "old"
                labels = {
                    "failed": "upstream failure",
                    "stale": "stale last-known-good",
                    "incomplete": "incomplete key coverage",
                }
                age_str = f"{labels.get(row_status, 'never')} · no recorded success"
        items.append({"name": layer, "number": number, "source": source,
                      "cadence": cadence, "scope": scope, "status": status,
                      "age": age_str, "coverage": coverage,
                      **_latency_fields(layer, row, now)})
    return items


_HEALTH_CRITICAL_NOTE = "data health critical"

# Buckets that mean "this layer is doing what it is supposed to do".
# `fresh` is sub-day, `stale` is older than a day but still inside the
# layer's own publication cadence (`config.freshness_limit_days`) — a
# weekly COT four days after its Friday release is not a problem, and the
# masthead must not call it one (#179).
_ON_SCHEDULE_STATUSES = ("fresh", "stale", "no-publication")


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
        "price_age": _price_age_label(now),
        **health_summary,
    }


def _price_age_label(now: datetime) -> str | None:
    """How stale the oldest board or FX leg is, for the masthead.

    "Generated 14:02 UTC" says when the page was built, and a reader
    reasonably reads that as when the numbers are from. On this site those
    are routinely a day apart — the settlement guard means a build landing
    before 14:30 Chicago publishes the previous session's close, correctly
    and deliberately. Stating the generation time alone lets a correct
    behaviour read as a fresher product than it is.

    The worst of the board and FX legs, not an average: an average would let
    a current FX print cover for a board leg that has not moved in three
    days, which is the one thing a reader must not miss.
    """
    from latency.measure import measure, worst_observation_age

    try:
        age = worst_observation_age(measure(generated_at=now), now)
    except Exception:  # noqa: BLE001 — the masthead must never fail the build
        log.warning("could not measure price age for the masthead", exc_info=True)
        return None
    if age is None:
        return None
    from latency.domain import format_delta

    return format_delta(age)


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

    # The front-month crush over the three benchmark legs. Named for what it
    # is since M16 #208: the crush board one section down strikes CBOT's margin
    # on ZSU26/ZMU26/ZLU26, and two tiles both called "Crush Spread" would read
    # as one number printed twice. Its job here is also structural — the
    # promotion contract reads `data-derived="crush"` to prove the three
    # benchmark legs came from one session, which is why the tile survives the
    # retirement of the DCE one below.
    crush_val = crush.get("value_usd_mt")
    key_metrics.append({
        "label": "CBOT Front-Month Crush",
        "value": f"${crush_val:,.1f}" if crush_val else "N/A",
        "val_class": "up" if crush.get("profitable") else "down" if crush_val else "",
        "delta": "Profitable" if crush.get("profitable") else "Negative" if crush_val else "",
        "delta_class": "up" if crush.get("profitable") else "down",
        "as_of": crush.get("as_of") or "",
        # The alignment probe, declared rather than inferred from the label.
        "derived": "crush",
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

    # The DCE Board Crush tile that used to sit here is retired (M16 #208).
    # Section 04 renders Dalian's margin beside CBOT's, Brazil's and
    # Argentina's, off the same engine the Dalian page's block 03 uses — while
    # this tile was `analysis.spreads.compute_dce_crush_margin` over the
    # continuous series, a fifth surface computing its own crush. M2 #144's
    # rule for the grid: a metric rendered better one section down is how a
    # page rots.

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

        # M8's chart budget: the indicators are computed on the full series
        # (a 200-day MA needs its 200 days) and only the *drawn* window is
        # clipped. Technicals are 3 of the 15 inline figures that make
        # docs/index.html 7 MB, and nothing read past the last two years.
        fig = build_technical_chart(clip(df), name)
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
    include_players: bool = False,
    market_nav: list[dict] | None = None,
) -> dict[str, Path]:
    """Render the headline page (M2 #144).

    M8 #150 demoted this module to one entry in the site's page list; the page
    list, the market nav and the failure-isolation policy live in
    ``scripts/generate_site.py``. ``include_players`` stays for the DT-20
    trusted-render path and for a standalone run, but the orchestrator renders
    the players page itself so both pages get the same computed nav.
    """
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

    log.info("Building the registry sections (ledger, crush board)...")
    registry_sections = _safe_call(_build_registry_sections, "registry_sections") or {}

    def _section(name: str) -> dict:
        return registry_sections.get(name) or {
            "state": "empty",
            "reason": f"the headline {name.replace('_', ' ')} could not be built for this render",
            "data": {},
        }

    ledger = _section("ledger")
    crush_board = _section("crush_board")

    # Build template context
    log.info("Building template context...")
    now = datetime.now(timezone.utc)
    freshness_items = _build_freshness_items()
    context = {
        "sections": SECTIONS,
        # _base.html.j2 owns <head>, the masthead and the market nav. The nav
        # is normally passed down by generate_site.py so every page shares one
        # tier computation; a standalone run computes its own.
        "market_nav": market_nav if market_nav is not None else _standalone_market_nav(),
        "root": "",
        "current_page": "headline",
        "current_market": None,
        "generated_at": now.strftime("%Y-%m-%d %H:%M UTC"),
        "generated_at_iso": now.isoformat(),
        # Built before the sidebar reads `freshness_items` — the masthead
        # demotes health-critical layers in place so both agree.
        "masthead": _build_masthead(freshness_items, now, health),
        "public_trust": _build_public_trust_metadata(public_trust_state),
        "freshness_items": freshness_items,
        "production_layers": PRODUCTION_LAYERS,
        "command_center": _build_command_center(cc_data),
        "technicals": _build_technicals(tech_data),
        "supply": _build_supply(supply_data),
        "demand": _build_demand(demand_data),
        # M18 #214: these five return the {state, reason, data} envelope and
        # nothing else — their markup lives in app/templates/sections/.
        "emerging_markets": emerging_markets_section(em_data),
        "relative_value": relative_value_section(rv_data),
        "risk_monitor": risk_monitor_section(risk_data),
        "forward_curves": forward_curves_section(fc_data),
        "seasonal": seasonal_section(seasonal_data),
        # M19 #223: the eight-row ledger, built from the same registry and the
        # same builder the market pages use — one implementation, so the
        # headline and a page can never disagree about who has repriced.
        "ledger": ledger,
        # M16 #208: four markets' crush margins side by side, each card calling
        # the same `crush_block` its market page's block 03 does — so the
        # headline and the page cannot print two different margins.
        "crush_board": crush_board,
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
