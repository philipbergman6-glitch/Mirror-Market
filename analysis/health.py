"""
Data health check module.

Runs after the pipeline to detect silent data gaps, stale commodities,
and suspicious patterns that the per-layer freshness tracking misses.

Key concepts for learning:
    - Per-commodity monitoring vs per-layer monitoring
    - Detecting "silent failures" (data that stopped updating but nobody noticed)
    - Business day awareness (weekends/holidays aren't real gaps)
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd

from config import (
    COMMODITY_TICKERS,
    COT_COMMODITIES,
    CURRENCY_TICKERS,
    DB_PATH,
    DCE_CONTRACTS,
    FORWARD_CURVE_CONTRACTS,
    GROWING_REGIONS,
    HEALTH_TABLE_LAYERS,
    freshness_limit_days,
)
from pipeline.connection import get_connection, is_cloud, managed_connection

logger = logging.getLogger(__name__)


# How many business days old before we flag a daily table as stale
_STALE_THRESHOLD_DAYS = 3
# Weekend slack — Monday-morning data from Friday is 3 calendar days old
_WEEKEND_SLACK_DAYS = 2

# How many identical consecutive Close prices before flagging as "flat"
_FLAT_PRICE_DAYS = 3


def _stale_limit_days(table: str) -> int:
    """Calendar days a table's newest row may age before it counts as stale.

    Slower-than-daily tables defer to the per-layer freshness policy in
    config (which already builds in cadence slack); daily tables get the
    tight health threshold plus a weekend allowance.
    """
    layer = HEALTH_TABLE_LAYERS.get(table)
    if layer is not None:
        return freshness_limit_days(layer)
    return _STALE_THRESHOLD_DAYS + _WEEKEND_SLACK_DAYS


def _observed_filter(conn, table: str, date_col: str) -> tuple[str, list[str]]:
    """SQL WHERE clause + params restricting a table to observed rows.

    Two rules, both needed so a dead fetcher can't hide behind its own
    forecast horizon (weather writes ~7 days ahead of today):

    - future-dated rows never prove liveness, whatever their flag;
    - rows flagged ``is_forecast = 1`` are excluded once their date passes.

    NULL / missing ``is_forecast`` counts as observed — same rule as
    ``analysis.briefing.sections.weather.observed_only`` — so tables and
    DBs predating the flag behave exactly as before.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    # substr() keeps the comparison correct for rows carrying a time part.
    clauses = [f"substr({date_col}, 1, 10) <= ?"]
    params = [today]
    if "is_forecast" in _table_columns(conn, table):
        clauses.append("(is_forecast IS NULL OR is_forecast = 0)")
    return " WHERE " + " AND ".join(clauses), params


def _table_columns(conn, table: str) -> set[str]:
    """Column names of ``table``; empty set if it can't be introspected."""
    try:
        return {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    except Exception:
        logger.warning("Could not introspect columns of %s", table, exc_info=True)
        return set()


def run_health_check() -> dict:
    """
    Run a full health check across all data tables.

    Returns a dict with:
        "summary"    : str  — human-readable health report
        "issues"     : list[dict] — each issue with severity, table, commodity, message
        "commodity_status" : list[dict] — per-commodity status for dashboard display
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return {
            "summary": "DATABASE NOT FOUND — run 'python main.py' first.",
            "issues": [{"severity": "critical", "table": "all", "commodity": "all",
                        "message": "Database does not exist"}],
            "commodity_status": [],
        }

    issues = []
    commodity_status = []

    # --- Check each table for expected commodities ---
    issues.extend(_check_prices())
    issues.extend(_check_cot())
    issues.extend(_check_weather())
    issues.extend(_check_currencies())
    issues.extend(_check_dce())
    issues.extend(_check_forward_curve())
    issues.extend(_check_flat_prices())
    issues.extend(_check_india_domestic())
    issues.extend(_check_brazil_spot())
    issues.extend(_check_safex())

    # Build per-commodity status for the dashboard
    commodity_status = _build_commodity_status()

    # Build human-readable summary
    summary = _format_summary(issues)

    return {
        "summary": summary,
        "issues": issues,
        "commodity_status": commodity_status,
    }


def _check_table_freshness(table: str, key_col: str, date_col: str,
                           expected_keys: list[str],
                           stale_exempt: frozenset[str] = frozenset()) -> list[dict]:
    """
    Check a table for missing or stale commodities.

    Only observed rows count — see ``_observed_filter``; a region whose
    only rows are forecasts reads as MISSING, not fresh.

    ``stale_exempt`` keys skip the staleness loop only — for series whose
    normal cadence is slower than the daily threshold (e.g. weekly).

    Returns a list of issue dicts.
    """
    issues = []
    today = datetime.now(timezone.utc).date()
    limit_days = _stale_limit_days(table)

    with managed_connection(get_connection()) as conn:
        try:
            where, params = _observed_filter(conn, table, date_col)
            rows = conn.execute(
                f"SELECT {key_col}, MAX({date_col}) as last_date, COUNT(*) as cnt "
                f"FROM {table}{where} GROUP BY {key_col}",
                params,
            ).fetchall()
        except Exception:
            issues.append({
                "severity": "critical",
                "table": table,
                "commodity": "all",
                "message": f"Table '{table}' does not exist or is unreadable",
            })
            return issues

    found = {}
    for key, last_date, count in rows:
        found[key] = (last_date, count)

    # Check for completely missing commodities
    for expected in expected_keys:
        if expected not in found:
            issues.append({
                "severity": "critical",
                "table": table,
                "commodity": expected,
                "message": f"MISSING from {table} — no observed rows",
            })

    # Check for stale data
    for key, (last_date, _count) in found.items():
        if last_date is None or key in stale_exempt:
            continue
        try:
            last_dt = pd.to_datetime(last_date).date()
            age_days = (today - last_dt).days
            if age_days > limit_days:
                issues.append({
                    "severity": "warning",
                    "table": table,
                    "commodity": key,
                    "message": f"STALE in {table} — last date is {last_date} ({age_days} days ago)",
                })
        except Exception:
            logger.warning("Staleness check failed for %s/%s", table, key, exc_info=True)

    return issues


def _check_prices() -> list[dict]:
    expected = list(COMMODITY_TICKERS.keys())
    return _check_table_freshness("prices", "commodity", "Date", expected)


def _check_cot() -> list[dict]:
    expected = list(COT_COMMODITIES.keys())
    return _check_table_freshness("cot", "commodity", "Date", expected)


def _check_weather() -> list[dict]:
    expected = list(GROWING_REGIONS.keys())
    return _check_table_freshness("weather", "region", "Date", expected)


def _check_currencies() -> list[dict]:
    expected = list(CURRENCY_TICKERS.keys())
    return _check_table_freshness("currencies", "pair", "Date", expected)


def _check_dce() -> list[dict]:
    expected = list(DCE_CONTRACTS.keys())
    return _check_table_freshness("dce_futures", "commodity", "Date", expected)


def _check_forward_curve() -> list[dict]:
    expected = list(FORWARD_CURVE_CONTRACTS.keys())
    return _check_table_freshness("forward_curve", "commodity", "fetched_date", expected)


def _check_flat_prices() -> list[dict]:
    """
    Detect commodities where the Close price hasn't changed for 3+ consecutive days.
    This could mean the source is returning cached/stale data.
    """
    issues: list[dict] = []
    if not is_cloud() and not os.path.exists(DB_PATH):
        return issues

    with managed_connection(get_connection()) as conn:
        try:
            commodities = [r[0] for r in conn.execute(
                "SELECT DISTINCT commodity FROM prices"
            ).fetchall()]
        except Exception:
            return issues

        for commodity in commodities:
            try:
                df = pd.read_sql(
                    "SELECT Date, Close FROM prices WHERE commodity = ? ORDER BY Date DESC LIMIT ?",
                    conn,
                    params=(commodity, _FLAT_PRICE_DAYS + 1),
                )
            except Exception:
                continue

            if len(df) < _FLAT_PRICE_DAYS or "Close" not in df.columns:
                continue

            recent_closes = df["Close"].dropna().head(_FLAT_PRICE_DAYS)
            if len(recent_closes) >= _FLAT_PRICE_DAYS and recent_closes.nunique() == 1:
                issues.append({
                    "severity": "warning",
                    "table": "prices",
                    "commodity": commodity,
                    "message": f"FLAT — same Close price ({recent_closes.iloc[0]}) "
                               f"for last {_FLAT_PRICE_DAYS} days (possible stale data)",
                })

    return issues


def _check_india_domestic() -> list[dict]:
    """Check the India mandi bean series for freshness.

    Only the live mandi series is expected — the retired NCDEX rows stay
    in the table under their own keys and would emit permanent false
    CRITICALs if listed here.
    """
    from config import MANDI_STATES
    return _check_table_freshness(
        "india_domestic_prices", "commodity", "Date", list(MANDI_STATES.values())
    )


def _check_brazil_spot() -> list[dict]:
    """Check Brazil domestic soy prices for freshness (daily = >2 days stale).

    Expectations are AgRural Paranaguá FOB plus the CEPEA indicators
    (re-enabled 2026-07-30 via Notícias Agrícolas — see main.py Layer 17);
    both layers write to brazil_spot_prices. The CONAB farmgate series
    (Layer 15b) also lives there but is weekly — the daily staleness
    threshold would flag it constantly, so it is stale-exempt.
    """
    from config import AGRURAL_COMMODITIES, CEPEA_COMMODITIES, CONAB_FARMGATE_SERIES
    # TODO Phase 2.1: also flag when Paranaguá FOB (AgRural) vs CEPEA Paraná
    # diverges beyond the historical port-vs-farm wedge band — a structural break
    # there is a stronger trade signal than either source's absolute freshness.
    return _check_table_freshness(
        "brazil_spot_prices", "commodity", "Date",
        AGRURAL_COMMODITIES + CEPEA_COMMODITIES,
        stale_exempt=frozenset({CONAB_FARMGATE_SERIES}),
    )


def _check_safex() -> list[dict]:
    """Check JSE SAFEX South Africa prices for freshness (daily = >2 days stale)."""
    from config import SAFEX_COMMODITIES
    expected = list(SAFEX_COMMODITIES)
    return _check_table_freshness("safex_prices", "commodity", "Date", expected)


def _build_commodity_status() -> list[dict]:
    """
    Build a list of per-commodity status entries for dashboard display.

    Each entry: {commodity, table, last_date, rows, age_days, status}
    status is one of: "fresh", "aging", "stale", "missing"
    """
    status_list: list[dict] = []
    today = datetime.now(timezone.utc).date()

    table_specs = [
        ("prices",                "commodity", "Date"),
        ("cot",                   "commodity", "Date"),
        ("weather",               "region",    "Date"),
        ("currencies",            "pair",      "Date"),
        ("dce_futures",           "commodity", "Date"),
        ("worldbank_prices",      "commodity", "Date"),
        ("forward_curve",         "commodity", "fetched_date"),
        ("india_domestic_prices", "commodity", "Date"),
        ("brazil_spot_prices",    "commodity", "Date"),
        ("safex_prices",          "commodity", "Date"),
    ]

    if not is_cloud() and not os.path.exists(DB_PATH):
        return status_list

    with managed_connection(get_connection()) as conn:
        for table, key_col, date_col in table_specs:
            limit_days = _stale_limit_days(table)
            try:
                where, params = _observed_filter(conn, table, date_col)
                rows = conn.execute(
                    f"SELECT {key_col}, MAX({date_col}) as last_date, COUNT(*) as cnt "
                    f"FROM {table}{where} GROUP BY {key_col}",
                    params,
                ).fetchall()
            except Exception:
                continue

            for key, last_date, count in rows:
                age_days = None
                status = "unknown"
                if last_date:
                    try:
                        last_dt = pd.to_datetime(last_date).date()
                        age_days = (today - last_dt).days
                        if age_days <= 1:
                            status = "fresh"
                        elif age_days <= limit_days:
                            status = "aging"
                        else:
                            status = "stale"
                    except Exception:
                        status = "unknown"
                else:
                    status = "missing"

                status_list.append({
                    "commodity": key,
                    "table": table,
                    "last_date": last_date,
                    "rows": count,
                    "age_days": age_days,
                    "status": status,
                })

    return status_list


def _format_summary(issues: list[dict]) -> str:
    """Format issues into a human-readable health report."""
    if not issues:
        return "DATA HEALTH: All systems green — no issues detected."

    critical = [i for i in issues if i["severity"] == "critical"]
    warnings = [i for i in issues if i["severity"] == "warning"]

    lines = []
    lines.append(f"DATA HEALTH: {len(critical)} critical, {len(warnings)} warnings")
    lines.append("")

    if critical:
        lines.append("CRITICAL:")
        for issue in critical:
            lines.append(f"  [{issue['table']}] {issue['commodity']}: {issue['message']}")
        lines.append("")

    if warnings:
        lines.append("WARNINGS:")
        for issue in warnings:
            lines.append(f"  [{issue['table']}] {issue['commodity']}: {issue['message']}")

    return "\n".join(lines)
