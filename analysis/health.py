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
import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from config import (
    COMMODITY_TICKERS,
    COT_COMMODITIES,
    CURRENCY_TICKERS,
    DB_PATH,
    DCE_CONTRACTS,
    FORWARD_CURVE_CONTRACTS,
    GROWING_REGIONS,
)
from processing.database import get_connection, is_cloud

logger = logging.getLogger(__name__)


# How many business days old before we flag as stale
_STALE_THRESHOLD_DAYS = 3

# How many identical consecutive Close prices before flagging as "flat"
_FLAT_PRICE_DAYS = 3


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
                           expected_keys: list[str]) -> list[dict]:
    """
    Check a table for missing or stale commodities.

    Returns a list of issue dicts.
    """
    issues = []
    today = datetime.utcnow().date()

    with get_connection() as conn:
        try:
            rows = conn.execute(
                f"SELECT {key_col}, MAX({date_col}) as last_date, COUNT(*) as cnt "
                f"FROM {table} GROUP BY {key_col}"
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
                "message": f"MISSING from {table} — no rows at all",
            })

    # Check for stale data
    for key, (last_date, count) in found.items():
        if last_date is None:
            continue
        try:
            last_dt = pd.to_datetime(last_date).date()
            age_days = (today - last_dt).days
            # Skip weekend check: if today is Monday, data from Friday is only 3 days old
            if age_days > _STALE_THRESHOLD_DAYS + 2:  # +2 for weekends
                issues.append({
                    "severity": "warning",
                    "table": table,
                    "commodity": key,
                    "message": f"STALE in {table} — last date is {last_date} ({age_days} days ago)",
                })
        except Exception:
            pass

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
    issues = []
    if not is_cloud() and not os.path.exists(DB_PATH):
        return issues

    with get_connection() as conn:
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


def _build_commodity_status() -> list[dict]:
    """
    Build a list of per-commodity status entries for dashboard display.

    Each entry: {commodity, table, last_date, rows, age_days, status}
    status is one of: "fresh", "aging", "stale", "missing"
    """
    status_list = []
    today = datetime.utcnow().date()

    table_specs = [
        ("prices",          "commodity", "Date"),
        ("cot",             "commodity", "Date"),
        ("weather",         "region",    "Date"),
        ("currencies",      "pair",      "Date"),
        ("dce_futures",     "commodity", "Date"),
        ("worldbank_prices","commodity", "Date"),
        ("forward_curve",   "commodity", "fetched_date"),
    ]

    if not is_cloud() and not os.path.exists(DB_PATH):
        return status_list

    with get_connection() as conn:
        for table, key_col, date_col in table_specs:
            try:
                rows = conn.execute(
                    f"SELECT {key_col}, MAX({date_col}) as last_date, COUNT(*) as cnt "
                    f"FROM {table} GROUP BY {key_col}"
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
                        elif age_days <= _STALE_THRESHOLD_DAYS + 2:
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
