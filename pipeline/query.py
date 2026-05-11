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


def read_prices(commodity: str | None = None) -> pd.DataFrame:
    """
    Read price data back from SQLite.

    Parameters
    ----------
    commodity : str or None
        If given, filter to just that commodity.  Otherwise return all.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM prices WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM prices", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_economic(series_name: str | None = None) -> pd.DataFrame:
    """Read economic (FRED) data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if series_name:
            df = pd.read_sql(
                "SELECT * FROM economic WHERE series_name = ?",
                conn,
                params=(series_name,),
            )
        else:
            df = pd.read_sql("SELECT * FROM economic", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_usda(stat_category: str | None = None) -> pd.DataFrame:
    """Read USDA data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if stat_category:
            df = pd.read_sql(
                "SELECT * FROM usda WHERE stat_category = ?",
                conn,
                params=(stat_category,),
            )
        else:
            df = pd.read_sql("SELECT * FROM usda", conn)

    return df


def read_cot(commodity: str | None = None) -> pd.DataFrame:
    """Read COT data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM cot WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM cot", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_weather(region: str | None = None) -> pd.DataFrame:
    """Read weather data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if region:
            df = pd.read_sql(
                "SELECT * FROM weather WHERE region = ?",
                conn,
                params=(region,),
            )
        else:
            df = pd.read_sql("SELECT * FROM weather", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_crop_progress(commodity: str | None = None) -> pd.DataFrame:
    """Read crop progress/condition data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM crop_progress WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM crop_progress", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for crop_progress: %s", exc)
            return pd.DataFrame()

    return df


def read_psd(commodity: str | None = None) -> pd.DataFrame:
    """Read PSD global supply/demand data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM psd WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM psd", conn)

    return df


def read_currencies(pair: str | None = None) -> pd.DataFrame:
    """Read currency data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if pair:
            df = pd.read_sql(
                "SELECT * FROM currencies WHERE pair = ?",
                conn,
                params=(pair,),
            )
        else:
            df = pd.read_sql("SELECT * FROM currencies", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_worldbank_prices(commodity: str | None = None) -> pd.DataFrame:
    """Read World Bank monthly price data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM worldbank_prices WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM worldbank_prices", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_export_sales(commodity: str | None = None) -> pd.DataFrame:
    """Read export sales data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM export_sales WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM export_sales", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for export_sales: %s", exc)
            return pd.DataFrame()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"])

    return df


def read_forward_curve(commodity: str | None = None) -> pd.DataFrame:
    """Read forward curve data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM forward_curve WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM forward_curve", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for forward_curve: %s", exc)
            return pd.DataFrame()

    return df


def read_dce_futures(commodity: str | None = None) -> pd.DataFrame:
    """Read DCE futures data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM dce_futures WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM dce_futures", conn)

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_wasde(commodity: str | None = None) -> pd.DataFrame:
    """Read WASDE forecast data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM wasde WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM wasde", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for wasde: %s", exc)
            return pd.DataFrame()

    return df


def read_inspections(commodity: str | None = None) -> pd.DataFrame:
    """Read export inspections data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM inspections WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM inspections", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for inspections: %s", exc)
            return pd.DataFrame()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"])

    return df


def read_eia_data(series_name: str | None = None) -> pd.DataFrame:
    """Read EIA energy data from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if series_name:
                df = pd.read_sql(
                    "SELECT * FROM eia_energy WHERE series_name = ?",
                    conn,
                    params=(series_name,),
                )
            else:
                df = pd.read_sql("SELECT * FROM eia_energy", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for eia_energy: %s", exc)
            return pd.DataFrame()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_brazil_estimates(commodity: str | None = None) -> pd.DataFrame:
    """Read CONAB Brazil estimates from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM brazil_estimates WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM brazil_estimates", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for brazil_estimates: %s", exc)
            return pd.DataFrame()

    return df


def read_india_domestic(commodity: str | None = None) -> pd.DataFrame:
    """Read NCDEX India domestic prices from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM india_domestic_prices WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM india_domestic_prices", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for india_domestic_prices: %s", exc)
            return pd.DataFrame()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_brazil_spot(commodity: str | None = None) -> pd.DataFrame:
    """Read CEPEA Brazil domestic spot prices from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM brazil_spot_prices WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM brazil_spot_prices", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for brazil_spot_prices: %s", exc)
            return pd.DataFrame()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_safex(commodity: str | None = None) -> pd.DataFrame:
    """Read JSE SAFEX South Africa settlement prices from SQLite."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with get_connection() as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM safex_prices WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM safex_prices", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for safex_prices: %s", exc)
            return pd.DataFrame()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])

    return df


def read_freshness() -> pd.DataFrame:
    """
    Read data freshness timestamps for all layers.

    Returns
    -------
    pd.DataFrame
        Columns: layer_name, last_success, last_attempt, rows_fetched, status
        (last_attempt and status default to NaN/'success' for legacy rows.)
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
