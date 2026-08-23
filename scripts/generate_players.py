"""Build docs/players.html — the filterable global counterparty map (issue #123).

Reads the players knowledge base (data/reference/players/*.yml, validated by
scripts/validate_players.py), groups entries by home country, and renders
compact expandable cards behind a client-side filter bar (role / product /
origin / destination / tier). Country headers carry 2-4 always-on context
numbers built from the DB at build time (PSD supply/demand, US export-sales
trend, weather z-scores for the weather-covered origin countries); missing
data omits the block — never fabricates (spec decision #13).

Called from scripts/generate_html.py; also runnable standalone:
    python scripts/generate_players.py
"""

from __future__ import annotations

import datetime as dt
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml
from jinja2 import Environment, FileSystemLoader

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.validate_players import PLAYERS_DIR  # noqa: E402

log = logging.getLogger(__name__)

OUTPUT_FILE = PROJECT_ROOT / "docs" / "players.html"
TEMPLATE_DIR = PROJECT_ROOT / "app" / "templates"

# Staleness bars (spec decision #14): activity amber when the newest tier-1
# item is older than 2 quarters; contacts amber when `accessed` > 12 months.
ACTIVITY_STALE_DAYS = 183
CONTACTS_STALE_DAYS = 365

# Display names for every country/region code the dataset uses. Grouping and
# filters must never show a bare code; test_generate_players asserts coverage.
COUNTRY_NAMES = {
    "GLOBAL": "Global trading houses",
    "AE": "United Arab Emirates", "AR": "Argentina", "BE": "Belgium", "BO": "Bolivia",
    "BR": "Brazil", "CA": "Canada", "CL": "Chile", "CN": "China", "CO": "Colombia",
    "DZ": "Algeria", "EC": "Ecuador", "EG": "Egypt", "ES": "Spain",
    "ET": "Ethiopia", "FR": "France",
    "GB": "United Kingdom", "GT": "Guatemala", "ID": "Indonesia", "IN": "India",
    "IR": "Iran", "IT": "Italy", "JP": "Japan", "KE": "Kenya",
    "KR": "South Korea", "MA": "Morocco",
    "MX": "Mexico", "NG": "Nigeria", "NL": "Netherlands", "NO": "Norway",
    "PE": "Peru", "PY": "Paraguay", "RU": "Russia", "SA": "Saudi Arabia",
    "TH": "Thailand", "TN": "Tunisia", "TR": "Turkey", "TW": "Taiwan",
    "UA": "Ukraine", "UG": "Uganda", "US": "United States", "UY": "Uruguay",
    "VN": "Vietnam",
    "ZA": "South Africa",
    # destination-only codes seen in destinations_structured
    "BD": "Bangladesh", "CH": "Switzerland", "CI": "Ivory Coast", "DE": "Germany",
    "DK": "Denmark", "HK": "Hong Kong", "IE": "Ireland", "IL": "Israel",
    "KW": "Kuwait", "LK": "Sri Lanka", "MM": "Myanmar", "MY": "Malaysia",
    "NP": "Nepal", "OM": "Oman", "PA": "Panama", "PH": "Philippines",
    "PK": "Pakistan", "PL": "Poland", "PT": "Portugal", "QA": "Qatar",
    "SE": "Sweden", "SG": "Singapore", "SV": "El Salvador", "UZ": "Uzbekistan",
    "VE": "Venezuela",
    # region enum (destination vocabulary, spec decision #11)
    "EU": "European Union", "MENA": "Middle East & North Africa",
    "SE-ASIA": "Southeast Asia", "ASIA": "Asia", "LATAM": "Latin America",
    "AFRICA": "Africa",
}

# ISO code → country name as it appears in the psd table (config.PSD_TARGET_COUNTRIES).
PSD_COUNTRY_BY_ISO = {
    "US": "United States", "BR": "Brazil", "AR": "Argentina", "PY": "Paraguay",
    "UY": "Uruguay", "BO": "Bolivia", "CO": "Colombia", "MX": "Mexico",
    "CN": "China", "IN": "India", "ID": "Indonesia", "MY": "Malaysia",
    "TH": "Thailand", "VN": "Vietnam", "JP": "Japan", "KR": "South Korea",
    "PK": "Pakistan", "BD": "Bangladesh", "NG": "Nigeria", "ZA": "South Africa",
    "CI": "Ivory Coast",
}

# ISO code → uppercase substring matching the FAS ESR destination name in the
# export_sales table (e.g. "CHINA, PEOPLES REPUBLIC OF").
FAS_COUNTRY_PATTERNS = {
    "CN": "CHINA", "JP": "JAPAN", "KR": "KOREA", "TW": "TAIWAN", "VN": "VIETNAM",
    "ID": "INDONESIA", "TH": "THAILAND", "MX": "MEXICO", "EG": "EGYPT",
    "TR": "TURKEY", "IR": "IRAN", "DZ": "ALGERIA", "TN": "TUNISIA",
    "MA": "MOROCCO", "SA": "SAUDI ARABIA", "AE": "ARAB EMIRATES",
    "NL": "NETHERLANDS", "GB": "UNITED KINGDOM", "FR": "FRANCE", "BE": "BELGIUM",
    "ES": "SPAIN", "IT": "ITALY", "NO": "NORWAY", "DE": "GERMANY",
    "CO": "COLOMBIA", "PE": "PERU", "CL": "CHILE", "EC": "ECUADOR",
    "GT": "GUATEMALA", "IN": "INDIA", "NG": "NIGERIA", "ZA": "SOUTH AFRICA",
    "CA": "CANADA", "RU": "RUSSIA",
}

# ISO code → weather regions (config.GROWING_REGIONS names) — the
# weather-covered origin countries from the spec's weather/flows mapping.
# Every value must be a live GROWING_REGIONS key; a test pins that.
WEATHER_REGIONS_BY_ISO = {
    "US": ["US Midwest (Iowa)", "US Illinois"],
    "BR": ["Brazil Mato Grosso", "Brazil Parana"],
    "AR": ["Argentina Pampas", "Argentina Cordoba"],
    "PY": ["Paraguay Alto Parana"],
    "CN": ["China Heilongjiang"],
    "IN": ["India Madhya Pradesh", "India Maharashtra"],
    "ZA": ["South Africa Free State", "South Africa Mpumalanga"],
    "NG": ["Nigeria Benue", "Nigeria Kaduna"],
    "ID": ["Indonesia Riau (Sumatra)"],
    "MY": ["Malaysia Sabah (Borneo)"],
    # TH and CI came out with their pins (M14 #207 / M24 #271): Thailand's
    # palm is covered by ID+MY, and Ivory Coast was a cocoa relic. A key here
    # naming a region that GROWING_REGIONS no longer holds would read as a
    # weather outage for that country rather than as "not covered".
    "CA": ["Canada Saskatchewan (Saskatoon)", "Canada Alberta (central)"],
}

WEATHER_Z_ALERT = 2.0  # |z| at or above this gets alert styling
ES_RECENT_WEEKS = 4    # export-sales trend window


def display_name(code: str) -> str:
    return COUNTRY_NAMES.get(code, code)


def dest_name(code: str) -> str:
    """Destination label — GLOBAL means 'worldwide', not the origin group name."""
    if code == "GLOBAL":
        return "Global (unspecified)"
    return display_name(code)


def _as_date(value) -> dt.date | None:
    if isinstance(value, dt.datetime):
        return value.date()
    if isinstance(value, dt.date):
        return value
    if isinstance(value, str):
        try:
            return dt.date.fromisoformat(value)
        except ValueError:
            return None
    return None


def _date_str(value) -> str:
    d = _as_date(value)
    return d.isoformat() if d else str(value)


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------
def load_players(players_dir: Path = PLAYERS_DIR) -> list[dict]:
    """All player entries across the knowledge base (assumes validation passed)."""
    entries: list[dict] = []
    for path in sorted(players_dir.glob("*.yml")):
        entries.extend(yaml.safe_load(path.read_text(encoding="utf-8")))
    return entries


def build_card(entry: dict, today: dt.date) -> dict:
    """One template-ready card: filter attributes, staleness flags, activity feed."""
    tier = 1 if entry.get("tier") == 1 else 2

    dests = entry.get("destinations_structured") or []
    dest_codes = [d["code"] for d in dests]

    activity = sorted(
        entry.get("activity") or [],
        key=lambda a: _as_date(a.get("date")) or dt.date.min,
        reverse=True,
    )
    activity_stale = False
    if activity:
        newest = _as_date(activity[0].get("date"))
        activity_stale = newest is not None and (today - newest).days > ACTIVITY_STALE_DAYS
    activity = [
        {
            "date": _date_str(a.get("date")),
            "category": a.get("category", ""),
            "headline": a.get("headline", ""),
            "detail": a.get("detail", ""),
            "url": (a.get("citation") or {}).get("url", ""),
        }
        for a in activity[:3]  # page shows latest 3 (spec decision #10)
    ]

    contacts = entry.get("contacts")
    contacts_stale = False
    if contacts:
        accessed = _as_date(contacts.get("accessed"))
        contacts_stale = accessed is not None and (today - accessed).days > CONTACTS_STALE_DAYS
        contacts = {
            "website": contacts.get("website", ""),
            "contact_url": contacts.get("contact_url", ""),
            "offices": contacts.get("offices") or [],
            "trade_desks": contacts.get("trade_desks") or [],
            "accessed": _date_str(contacts.get("accessed")) if contacts.get("accessed") else "",
        }

    return {
        "name": entry["name"],
        "aka": ", ".join(entry.get("aka") or []),
        "scope": entry.get("scope", ""),
        "country": entry["country"],
        "side": entry.get("side", ""),
        "tier": tier,
        "roles": entry.get("roles") or [],
        "products": entry.get("products") or [],
        "ownership": entry.get("ownership", ""),
        "footprint": entry.get("footprint", ""),
        "destinations_prose": entry.get("destinations", ""),
        "destinations": [
            {
                "code": d["code"],
                "name": dest_name(d["code"]),
                "products": d.get("products") or [],
                "note": d.get("note", ""),
            }
            for d in dests
        ],
        "dest_codes": dest_codes,
        "size_evidence": entry.get("size_evidence", ""),
        "confidence": entry.get("confidence", ""),
        "as_of": _date_str(entry.get("as_of")) if entry.get("as_of") else "",
        "notes": entry.get("notes", ""),
        "citations": [
            {"url": c.get("url", ""), "supports": c.get("supports", "")}
            for c in (entry.get("citations") or [])
        ],
        "website": entry.get("website", ""),
        "contacts": contacts,
        "contacts_stale": contacts_stale,
        "activity": activity,
        "activity_stale": activity_stale,
    }


def group_cards(cards: list[dict]) -> list[dict]:
    """Country groups: GLOBAL first, then alphabetical; tier-1 leads each group."""
    by_country: dict[str, list[dict]] = {}
    for card in cards:
        by_country.setdefault(card["country"], []).append(card)

    def group_key(code: str):
        return (code != "GLOBAL", display_name(code))

    groups = []
    for code in sorted(by_country, key=group_key):
        group_cards_ = sorted(by_country[code], key=lambda c: (c["tier"], c["name"].lower()))
        groups.append({
            "code": code,
            "name": display_name(code),
            "anchor": f"c-{code.lower()}",
            "cards": group_cards_,
            "tier1_count": sum(1 for c in group_cards_ if c["tier"] == 1),
        })
    return groups


def build_filter_options(cards: list[dict]) -> dict:
    """Facet values actually present in the dataset — no dead options."""
    roles = sorted({r for c in cards for r in c["roles"]})
    products = sorted({p for c in cards for p in c["products"]})
    origin_codes = sorted({c["country"] for c in cards}, key=lambda x: (x != "GLOBAL", display_name(x)))
    dest_codes = sorted({d for c in cards for d in c["dest_codes"]}, key=dest_name)
    return {
        "roles": roles,
        "products": products,
        "origins": [{"code": x, "name": display_name(x)} for x in origin_codes],
        "destinations": [{"code": x, "name": dest_name(x)} for x in dest_codes],
    }


# ---------------------------------------------------------------------------
# Country context blocks (spec decision #13 — calm is information)
# ---------------------------------------------------------------------------
def psd_numbers(psd_df: pd.DataFrame, iso: str) -> list[dict]:
    """Top-2 nonzero soybean balance-sheet numbers for the latest PSD year."""
    country = PSD_COUNTRY_BY_ISO.get(iso)
    if not country or psd_df is None or psd_df.empty:
        return []
    rows = psd_df[psd_df["country"] == country]
    if rows.empty:
        return []
    latest_year = int(rows["year"].max())
    rows = rows[rows["year"] == latest_year]
    scored: list[tuple[float, dict]] = []
    for attribute, label in [
        ("Production", "Soybean production"),
        ("Imports", "Soybean imports"),
        ("Exports", "Soybean exports"),
    ]:
        match = rows[rows["attribute"] == attribute]
        if match.empty:
            continue
        value = float(match["value"].iloc[0])
        # `not value > 0` also catches NaN (NULL value column) — a NaN row
        # would otherwise render "nan MMT" (fabrication, spec decision #13)
        if not value > 0:
            continue
        scored.append((value, {
            "label": label,
            "value": f"{value / 1000:,.1f} MMT",
            "note": f"PSD {latest_year}",
            "cls": "",
        }))
    scored.sort(key=lambda t: t[0], reverse=True)
    return [item for _, item in scored[:2]]


def export_sales_trend(es_df: pd.DataFrame, iso: str) -> dict | None:
    """US soybean export-sales pace to this destination: last 4 wks vs prior 4."""
    pattern = FAS_COUNTRY_PATTERNS.get(iso)
    if not pattern or es_df is None or es_df.empty:
        return None
    df = es_df[es_df["commodity"] == "Soybeans"] if "commodity" in es_df.columns else es_df
    df = df[df["country"].str.upper().str.contains(pattern, regex=False, na=False)]
    if df.empty:
        return None
    df = df.assign(week=pd.to_datetime(df["week_ending"]))
    weekly = df.groupby("week")["net_sales"].sum().sort_index()
    recent = weekly.tail(ES_RECENT_WEEKS)
    if len(weekly) > ES_RECENT_WEEKS:
        prior = weekly.iloc[:-ES_RECENT_WEEKS].tail(ES_RECENT_WEEKS)
    else:
        prior = pd.Series(dtype=float)
    recent_sum = float(recent.sum())

    note_parts = [f"last {len(recent)} wks to {recent.index.max():%m/%d}"]
    if not prior.empty and float(prior.sum()) > 0:
        chg = (recent_sum - float(prior.sum())) / float(prior.sum()) * 100
        note_parts.append(f"{chg:+.0f}% vs prior {len(prior)}")
    return {
        "label": "US soybean sales (FAS)",
        "value": f"{recent_sum / 1e6:,.2f} MMT",
        "note": " · ".join(note_parts),
        # net cancellations are the loud case for a buyer country
        "cls": "warn" if recent_sum < 0 else "",
    }


def weather_anomalies(region_dfs: dict[str, pd.DataFrame], iso: str) -> list[dict]:
    """Per-region 90d temp/precip z-scores; |z| >= 2 gets alert styling."""
    from analysis.briefing.sections.weather import observed_only
    from analysis.zscore import trailing_zscore

    regions = WEATHER_REGIONS_BY_ISO.get(iso, [])
    lookback = pd.Timedelta(days=90)
    scored: list[tuple[float, dict]] = []
    for region in regions:
        df = region_dfs.get(region)
        if df is None or df.empty:
            continue
        observed = observed_only(df).sort_values("Date")
        if observed.empty:
            continue
        latest_date = observed["Date"].max()
        parts, worst = [], 0.0
        for column, tag in [("temperature_2m_max", "temp"), ("precipitation", "precip")]:
            z = trailing_zscore(observed, column, latest_date, lookback)
            if z is None:
                continue
            parts.append(f"{tag} {z:+.1f}σ")
            worst = max(worst, abs(z))
        if not parts:
            continue
        scored.append((worst, {
            "label": region,
            "value": " · ".join(parts),
            "note": f"90d z · {latest_date:%Y-%m-%d}",
            "cls": "alert" if worst >= WEATHER_Z_ALERT else "",
        }))
    # calm regions render too (calm is information), worst anomaly first
    scored.sort(key=lambda t: t[0], reverse=True)
    return [item for _, item in scored]


def build_country_contexts(codes: list[str]) -> dict[str, list[dict]]:
    """DB-backed context items per country code. Any failure → omission."""
    contexts: dict[str, list[dict]] = {}

    psd_df = es_df = None
    weather_cache: dict[str, pd.DataFrame] = {}
    try:
        from pipeline.query import read_export_sales, read_psd, read_weather
    except Exception as e:
        log.warning("players context: DB unavailable (%s)", e)
        return contexts
    # each source degrades independently — a dead export_sales table must not
    # take the PSD numbers down with it
    try:
        psd_df = read_psd("Soybeans")
    except Exception as e:
        log.warning("players context: PSD read failed (%s)", e)
    try:
        es_df = read_export_sales("Soybeans")
    except Exception as e:
        log.warning("players context: export sales read failed (%s)", e)

    for code in codes:
        items: list[dict] = []
        try:
            if psd_df is not None:
                items.extend(psd_numbers(psd_df, code))
        except Exception as e:
            log.warning("players context: PSD failed for %s: %s", code, e)
        try:
            trend = export_sales_trend(es_df, code) if es_df is not None else None
            if trend:
                items.append(trend)
        except Exception as e:
            log.warning("players context: export sales failed for %s: %s", code, e)
        try:
            region_dfs = {}
            for region in WEATHER_REGIONS_BY_ISO.get(code, []):
                if region not in weather_cache:
                    weather_cache[region] = read_weather(region)
                region_dfs[region] = weather_cache[region]
            items.extend(weather_anomalies(region_dfs, code))
        except Exception as e:
            log.warning("players context: weather failed for %s: %s", code, e)
        if items:
            contexts[code] = items
    return contexts


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------
def render_players_html(
    groups: list[dict],
    filters: dict,
    contexts: dict[str, list[dict]],
    generated_at: str,
    day_line: str,
    market_nav: list[dict] | None = None,
    generated_at_iso: str = "",
) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)
    template = env.get_template("players.html.j2")
    total = sum(len(g["cards"]) for g in groups)
    tier1 = sum(g["tier1_count"] for g in groups)
    return template.render(
        groups=groups,
        filters=filters,
        contexts=contexts,
        generated_at=generated_at,
        generated_at_iso=generated_at_iso,
        day_line=day_line,
        total_players=total,
        tier1_players=tier1,
        # _base.html.j2 owns the masthead and the market nav (M8 #150). The
        # nav is normally passed down by scripts/generate_site.py so every
        # page shares one computed tier set; standalone runs compute their own.
        market_nav=market_nav if market_nav is not None else _standalone_market_nav(),
        root="",
        current_page="players",
        current_market=None,
    )


def _standalone_market_nav() -> list[dict]:
    """Market nav for a standalone `python scripts/generate_players.py` run."""
    from app.markets import compute_tiers, load_markets, nav_items

    markets = load_markets()
    return nav_items(compute_tiers(markets), markets=markets)


def generate_players_page(
    output_file: Path = OUTPUT_FILE,
    *,
    market_nav: list[dict] | None = None,
) -> Path:
    """Load the knowledge base, build DB context, write docs/players.html."""
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc)
    players = load_players()
    cards = [build_card(e, now.date()) for e in players]
    groups = group_cards(cards)
    contexts = build_country_contexts([g["code"] for g in groups])
    html = render_players_html(
        groups=groups,
        filters=build_filter_options(cards),
        contexts=contexts,
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_iso=now.isoformat(),
        day_line=now.strftime("%A · %-d %B %Y"),
        market_nav=market_nav,
    )
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(html, encoding="utf-8")
    log.info("Generated %s (%.0f KB)", output_file, output_file.stat().st_size / 1024)
    return output_file


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    generate_players_page()
