"""
Database read (query) functions for Mirror Market.

All read_* functions query the SQLite/Turso database and return DataFrames.
Used by the analysis layer and dashboard.

Extracted from the original processing/combiner.py.
"""

import json
import logging
import os
import sqlite3
from typing import Any

import pandas as pd

from config import DB_PATH
from pipeline.connection import get_connection, is_cloud

logger = logging.getLogger(__name__)


def _read_table(
    table: str,
    filter_col: str | None = None,
    filter_value: str | None = None,
    *,
    date_cols: tuple[str, ...] = ("Date",),
    missing_ok: bool = True,
    sql: str | None = None,
    where_prefix: str = "",
) -> pd.DataFrame:
    """Shared body for the read_* functions.

    Parameters
    ----------
    table : str
        Table name (also used in the warning log line).
    filter_col, filter_value : str or None
        Optional equality filter appended as a parameterised WHERE clause.
    date_cols : tuple of str
        Columns parsed to datetime when present.
    missing_ok : bool
        True: a missing/unmigrated table logs a warning and returns empty
        (newer tables that may not exist in a pre-migration DB).
        False: the error propagates — core tables that init_database always
        creates, where a read failure means something is genuinely broken.
    sql : str or None
        Override for the base SELECT (e.g. the forward-curve latest-snapshot
        join). Defaults to ``SELECT * FROM {table}``.
    where_prefix : str
        Table alias prefix for the WHERE column when ``sql`` uses one.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    base = sql or f"SELECT * FROM {table}"  # noqa: S608 — table names are literals below
    with get_connection() as conn:
        try:
            if filter_value is not None:
                df = pd.read_sql(
                    f"{base} WHERE {where_prefix}{filter_col} = ?",
                    conn,
                    params=(filter_value,),
                )
            else:
                df = pd.read_sql(base, conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            if not missing_ok:
                raise
            logger.warning("Read failed for %s: %s", table, exc)
            return pd.DataFrame()

    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    return df


def read_prices(commodity: str | None = None) -> pd.DataFrame:
    """
    Read price data back from SQLite.

    Parameters
    ----------
    commodity : str or None
        If given, filter to just that commodity.  Otherwise return all.
    """
    return _read_table("prices", "commodity", commodity, missing_ok=False)


def read_economic(series_name: str | None = None) -> pd.DataFrame:
    """Read economic (FRED) data from SQLite."""
    return _read_table("economic", "series_name", series_name, missing_ok=False)


def read_usda(stat_category: str | None = None) -> pd.DataFrame:
    """Read USDA data from SQLite."""
    return _read_table(
        "usda", "stat_category", stat_category, date_cols=(), missing_ok=False
    )


def read_cot(commodity: str | None = None) -> pd.DataFrame:
    """Read COT data from SQLite."""
    return _read_table("cot", "commodity", commodity, missing_ok=False)


def read_weather(region: str | None = None) -> pd.DataFrame:
    """Read weather data from SQLite."""
    return _read_table("weather", "region", region, missing_ok=False)


def read_crop_progress(commodity: str | None = None) -> pd.DataFrame:
    """Read crop progress/condition data from SQLite."""
    return _read_table("crop_progress", "commodity", commodity, date_cols=())


def read_psd(commodity: str | None = None) -> pd.DataFrame:
    """Read PSD global supply/demand data from SQLite."""
    return _read_table("psd", "commodity", commodity, date_cols=(), missing_ok=False)


def read_currencies(pair: str | None = None) -> pd.DataFrame:
    """Read currency data from SQLite."""
    return _read_table("currencies", "pair", pair, missing_ok=False)


def read_worldbank_prices(commodity: str | None = None) -> pd.DataFrame:
    """Read World Bank monthly price data from SQLite."""
    return _read_table("worldbank_prices", "commodity", commodity, missing_ok=False)


def read_export_sales(commodity: str | None = None) -> pd.DataFrame:
    """Read export sales data from SQLite."""
    return _read_table(
        "export_sales", "commodity", commodity, date_cols=("week_ending",)
    )


def read_forward_curve(commodity: str | None = None) -> pd.DataFrame:
    """Read the latest forward-curve snapshot per commodity.

    The table accumulates one full curve per fetched_date (history for
    term-structure analysis); every current consumer wants only the most
    recent curve, so this filters to each commodity's latest fetched_date.
    Query the table directly for history.
    """
    latest_sql = (
        "SELECT fc.* FROM forward_curve fc "
        "JOIN (SELECT commodity, MAX(fetched_date) AS max_fd "
        "      FROM forward_curve GROUP BY commodity) latest "
        "ON fc.commodity = latest.commodity AND fc.fetched_date = latest.max_fd"
    )
    return _read_table(
        "forward_curve",
        "commodity",
        commodity,
        date_cols=(),
        sql=latest_sql,
        where_prefix="fc.",
    )


def read_dce_futures(commodity: str | None = None) -> pd.DataFrame:
    """Read DCE futures data from SQLite."""
    return _read_table("dce_futures", "commodity", commodity, missing_ok=False)


def read_wasde(commodity: str | None = None) -> pd.DataFrame:
    """Read WASDE forecast data from SQLite."""
    return _read_table("wasde", "commodity", commodity, date_cols=())


def read_inspections(commodity: str | None = None) -> pd.DataFrame:
    """Read export inspections data from SQLite."""
    return _read_table(
        "inspections", "commodity", commodity, date_cols=("week_ending",)
    )


def read_inspection_destinations(commodity: str | None = None) -> pd.DataFrame:
    """Read AMS destination-country export inspections from SQLite."""
    return _read_table(
        "inspection_destinations", "commodity", commodity, date_cols=("week_ending",)
    )


def read_argentina_fob(product: str | None = None) -> pd.DataFrame:
    """Read MAGyP official Argentina FOB prices from SQLite."""
    return _read_table(
        "argentina_fob", "product", product, date_cols=("date",)
    )


def read_port_flows(commodity: str | None = None) -> pd.DataFrame:
    """Read AMS port-area export inspections from SQLite."""
    return _read_table(
        "inspection_port_flows", "commodity", commodity, date_cols=("week_ending",)
    )


def read_gulf_bids(commodity: str | None = None) -> pd.DataFrame:
    """Read AMS CIF Gulf export bids from SQLite."""
    return _read_table(
        "gulf_bids", "commodity", commodity, date_cols=("report_date",)
    )


def read_eia_data(series_name: str | None = None) -> pd.DataFrame:
    """Read EIA energy data from SQLite."""
    return _read_table("eia_energy", "series_name", series_name)


def read_brazil_estimates(commodity: str | None = None) -> pd.DataFrame:
    """Read CONAB Brazil estimates from SQLite."""
    return _read_table("brazil_estimates", "commodity", commodity, date_cols=())


def read_india_domestic(commodity: str | None = None) -> pd.DataFrame:
    """Read NCDEX India domestic prices from SQLite."""
    return _read_table("india_domestic_prices", "commodity", commodity)


def read_brazil_spot(commodity: str | None = None) -> pd.DataFrame:
    """Read CEPEA Brazil domestic spot prices from SQLite."""
    return _read_table("brazil_spot_prices", "commodity", commodity)


def read_safex(commodity: str | None = None) -> pd.DataFrame:
    """Read JSE SAFEX South Africa settlement prices from SQLite."""
    return _read_table("safex_prices", "commodity", commodity)


def read_freshness() -> pd.DataFrame:
    """
    Read data freshness timestamps for all layers.

    Returns
    -------
    pd.DataFrame
        Columns: layer_name, last_success, last_attempt, rows_fetched, status,
        keys_returned, keys_expected
        (last_attempt and status default to NaN/'success' for legacy rows;
        the key-coverage columns are NULL wherever coverage is undefined or
        was never learned — see save_freshness.)
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            df = pd.read_sql("SELECT * FROM data_freshness", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for data_freshness: %s", exc)
            return pd.DataFrame()

    for col in ("last_success", "last_attempt"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    if "status" not in df.columns:
        df["status"] = "success"

    return df


def read_briefing(briefing_date: str) -> dict[str, Any] | None:
    """Read one archived briefing by date. Returns None if absent.

    Returns a dict with keys: briefing_date, text, signals (list),
    snapshot (dict), generated_at. JSON columns are decoded.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return None
    with get_connection() as conn:
        try:
            row = conn.execute(
                "SELECT briefing_date, text, signals_json, snapshot_json, generated_at "
                "FROM briefings WHERE briefing_date = ?",
                (briefing_date,),
            ).fetchone()
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for briefings: %s", exc)
            return None
    if row is None:
        return None
    return {
        "briefing_date": row[0],
        "text": row[1],
        "signals": json.loads(row[2]) if row[2] else [],
        "snapshot": json.loads(row[3]) if row[3] else {},
        "generated_at": row[4],
    }


def read_briefings(
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Read archived briefings, optionally filtered by date range (inclusive).

    Returns a DataFrame with columns:
        briefing_date, text, signals_json, snapshot_json, generated_at
    JSON columns are left as strings — callers can decode per-row.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()
    sql = "SELECT * FROM briefings"
    clauses: list[str] = []
    params: list[str] = []
    if start_date:
        clauses.append("briefing_date >= ?")
        params.append(start_date)
    if end_date:
        clauses.append("briefing_date <= ?")
        params.append(end_date)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY briefing_date"
    with get_connection() as conn:
        try:
            df = pd.read_sql(sql, conn, params=tuple(params) if params else None)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for briefings: %s", exc)
            return pd.DataFrame()
    return df


def read_commodity_freshness() -> pd.DataFrame:
    """Read per-commodity freshness data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            df = pd.read_sql("SELECT * FROM commodity_freshness", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for commodity_freshness: %s", exc)
            return pd.DataFrame()

    return df
