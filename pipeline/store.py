"""
Database write (store) functions for Mirror Market.

All save_* functions write cleaned DataFrames into SQLite/Turso tables.
Uses INSERT OR REPLACE so the pipeline is safe to re-run.

Extracted from the original processing/combiner.py.
"""

import logging
import os
from datetime import datetime

import pandas as pd

from config import DB_PATH, STORAGE_DIR
from pipeline.connection import get_connection, is_cloud
from pipeline.schema import (
    _CREATE_PRICES, _CREATE_ECONOMIC, _CREATE_USDA, _CREATE_COT,
    _CREATE_WEATHER, _CREATE_PSD, _CREATE_CURRENCIES, _CREATE_WORLDBANK,
    _CREATE_DCE_FUTURES, _CREATE_CROP_PROGRESS, _CREATE_EXPORT_SALES,
    _CREATE_FORWARD_CURVE, _CREATE_WASDE, _CREATE_INSPECTIONS,
    _CREATE_EIA_ENERGY, _CREATE_BRAZIL_ESTIMATES, _CREATE_OPTIONS_SENTIMENT,
    _CREATE_DATA_FRESHNESS, _CREATE_COMMODITY_FRESHNESS,
    _CREATE_INDIA_DOMESTIC, _CREATE_BRAZIL_SPOT, _CREATE_SAFEX,
)

logger = logging.getLogger(__name__)


def _ensure_storage_dir():
    """Create the storage directory if it doesn't exist yet."""
    os.makedirs(STORAGE_DIR, exist_ok=True)


def init_database():
    """
    Create tables if they don't exist yet.

    Call this once at startup. It's safe to call repeatedly — the
    IF NOT EXISTS clause means it won't destroy existing data.
    """
    _ensure_storage_dir()
    with get_connection() as conn:
        conn.execute(_CREATE_PRICES)
        conn.execute(_CREATE_ECONOMIC)
        conn.execute(_CREATE_USDA)
        conn.execute(_CREATE_COT)
        conn.execute(_CREATE_WEATHER)
        conn.execute(_CREATE_PSD)
        conn.execute(_CREATE_CURRENCIES)
        conn.execute(_CREATE_WORLDBANK)
        conn.execute(_CREATE_DCE_FUTURES)
        conn.execute(_CREATE_CROP_PROGRESS)
        conn.execute(_CREATE_EXPORT_SALES)
        conn.execute(_CREATE_FORWARD_CURVE)
        conn.execute(_CREATE_WASDE)
        conn.execute(_CREATE_INSPECTIONS)
        conn.execute(_CREATE_EIA_ENERGY)
        conn.execute(_CREATE_BRAZIL_ESTIMATES)
        conn.execute(_CREATE_OPTIONS_SENTIMENT)
        conn.execute(_CREATE_DATA_FRESHNESS)
        conn.execute(_CREATE_COMMODITY_FRESHNESS)
        conn.execute(_CREATE_INDIA_DOMESTIC)
        conn.execute(_CREATE_BRAZIL_SPOT)
        conn.execute(_CREATE_SAFEX)
    logger.info("Database initialised (tables verified) at %s", DB_PATH)


def clear_database():
    """
    Drop all tables so we can do a fresh load.

    Manual-only utility — NOT called during the normal pipeline.
    Use from the Python REPL if you need a clean slate:

        >>> from pipeline.store import clear_database
        >>> clear_database()
    """
    _ensure_storage_dir()
    with get_connection() as conn:
        for table in ("prices", "economic", "usda", "cot", "weather",
                      "psd", "currencies", "worldbank_prices",
                      "dce_futures", "crop_progress", "export_sales",
                      "forward_curve", "wasde", "inspections",
                      "eia_energy", "brazil_estimates", "options_sentiment",
                      "data_freshness", "commodity_freshness",
                      "india_domestic_prices", "brazil_spot_prices", "safex_prices"):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    logger.info("Database cleared.")


def save_price_data(name: str, df: pd.DataFrame):
    """
    Write an OHLCV DataFrame to the 'prices' table in SQLite.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = name

    # Reset index so the Date becomes a normal column (SQLite-friendly)
    df = df.reset_index()

    # Convert Date to ISO string for consistent storage
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO prices
                       (commodity, Date, Open, High, Low, Close, Volume)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["commodity"],
                        row["Date"],
                        float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        float(row["High"]) if pd.notna(row.get("High")) else None,
                        float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        float(row["Close"]) if pd.notna(row.get("Close")) else None,
                        float(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for prices/%s — rolled back", name)
            raise

    logger.info("Saved %d rows for %s → prices table", len(df), name)


def save_fred_data(name: str, series: pd.Series):
    """
    Write a FRED Series to the 'economic' table.

    Uses INSERT OR REPLACE to avoid duplicates on re-run.
    """
    if series.empty:
        return

    df = series.reset_index()
    df.columns = ["Date", "value"]
    df["series_name"] = name
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO economic
                       (series_name, Date, value)
                       VALUES (?, ?, ?)""",
                    (row["series_name"], row["Date"], float(row["value"])),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for economic/%s — rolled back", name)
            raise

    logger.info("Saved %d rows for %s → economic table", len(df), name)


def save_usda_data(df: pd.DataFrame, stat_category: str):
    """
    Write a USDA DataFrame to the 'usda' table.

    Uses INSERT OR REPLACE to avoid duplicates on re-run.
    """
    if df.empty:
        return

    df = df.copy()
    df["stat_category"] = stat_category

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO usda
                       (stat_category, year, short_desc, Value,
                        unit_desc, state_name, reference_period_desc)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row.get("stat_category", ""),
                        row.get("year", ""),
                        row.get("short_desc", ""),
                        row.get("Value", ""),
                        row.get("unit_desc", ""),
                        row.get("state_name", ""),
                        row.get("reference_period_desc", ""),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for usda/%s — rolled back", stat_category)
            raise

    logger.info("Saved %d rows for USDA/%s → usda table", len(df), stat_category)


def save_crop_progress(commodity: str, df: pd.DataFrame):
    """
    Write crop progress/condition data to the 'crop_progress' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO crop_progress
                       (commodity, week_ending, year, short_desc,
                        Value, unit_desc, stat_category)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row.get("commodity", commodity),
                        row.get("week_ending", ""),
                        row.get("year", ""),
                        row.get("short_desc", ""),
                        row.get("Value", ""),
                        row.get("unit_desc", ""),
                        row.get("statisticcat_desc", ""),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for crop_progress/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → crop_progress table", len(df), commodity)


def save_freshness(layer_name: str, rows_fetched: int = 0):
    """
    Record the last successful fetch timestamp for a data layer.

    Called after each layer succeeds in the pipeline so the briefing
    can warn about stale data.
    """
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    with get_connection() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO data_freshness
               (layer_name, last_success, rows_fetched)
               VALUES (?, ?, ?)""",
            (layer_name, now, rows_fetched),
        )
    logger.debug("Freshness recorded for %s at %s (%d rows)", layer_name, now, rows_fetched)


def update_commodity_freshness():
    """
    Scan every data table and record per-commodity freshness.

    For each commodity/region/pair in each table, records:
      - The most recent date found in the DB
      - Total row count
      - When this check was performed

    This catches the case where one commodity silently stops updating
    while the rest of the layer succeeds.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    # Define which tables to scan and their key/date columns
    table_specs = [
        ("prices",          "commodity", "Date"),
        ("cot",             "commodity", "Date"),
        ("weather",         "region",    "Date"),
        ("currencies",      "pair",      "Date"),
        ("dce_futures",     "commodity", "Date"),
        ("worldbank_prices","commodity", "Date"),
        ("forward_curve",   "commodity", "fetched_date"),
    ]

    with get_connection() as conn:
        for table, key_col, date_col in table_specs:
            try:
                rows = conn.execute(
                    f"SELECT {key_col}, MAX({date_col}) as last_date, COUNT(*) as cnt "
                    f"FROM {table} GROUP BY {key_col}"
                ).fetchall()
            except Exception:
                continue

            for commodity, last_date, count in rows:
                conn.execute(
                    """INSERT OR REPLACE INTO commodity_freshness
                       (commodity, table_name, last_date_in_db, rows_total, checked_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (commodity, table, last_date, count, now),
                )

    logger.info("Commodity freshness updated at %s", now)


def save_cot_data(name: str, df: pd.DataFrame):
    """
    Write COT positioning data to the 'cot' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = name
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO cot
                       (commodity, Date, commercial_long, commercial_short,
                        commercial_net, noncommercial_long, noncommercial_short,
                        noncommercial_net, total_open_interest)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["commodity"],
                        row["Date"],
                        float(row["commercial_long"]) if pd.notna(row.get("commercial_long")) else None,
                        float(row["commercial_short"]) if pd.notna(row.get("commercial_short")) else None,
                        float(row["commercial_net"]) if pd.notna(row.get("commercial_net")) else None,
                        float(row["noncommercial_long"]) if pd.notna(row.get("noncommercial_long")) else None,
                        float(row["noncommercial_short"]) if pd.notna(row.get("noncommercial_short")) else None,
                        float(row["noncommercial_net"]) if pd.notna(row.get("noncommercial_net")) else None,
                        float(row["total_open_interest"]) if pd.notna(row.get("total_open_interest")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for cot/%s — rolled back", name)
            raise

    logger.info("Saved %d rows for %s → cot table", len(df), name)


def save_weather_data(region: str, df: pd.DataFrame):
    """
    Write weather data to the 'weather' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["region"] = region
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO weather
                       (region, Date, temp_max, temp_min, precipitation)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        row["region"],
                        row["Date"],
                        float(row["temp_max"]) if pd.notna(row.get("temp_max")) else None,
                        float(row["temp_min"]) if pd.notna(row.get("temp_min")) else None,
                        float(row["precipitation"]) if pd.notna(row.get("precipitation")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for weather/%s — rolled back", region)
            raise

    logger.info("Saved %d rows for %s → weather table", len(df), region)


def save_psd_data(commodity: str, df: pd.DataFrame):
    """
    Write PSD data to the 'psd' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO psd
                       (commodity, country, year, attribute, value, unit)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        str(row.get("commodity", commodity)),
                        str(row.get("country", "")),
                        int(row["year"]) if pd.notna(row.get("year")) else None,
                        str(row.get("attribute", "")),
                        float(row["value"]) if pd.notna(row.get("value")) else None,
                        str(row.get("unit", "")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for psd/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → psd table", len(df), commodity)


def save_currency_data(pair: str, df: pd.DataFrame):
    """
    Write currency OHLCV data to the 'currencies' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["pair"] = pair
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO currencies
                       (pair, Date, Open, High, Low, Close)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        row["pair"],
                        row["Date"],
                        float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        float(row["High"]) if pd.notna(row.get("High")) else None,
                        float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        float(row["Close"]) if pd.notna(row.get("Close")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for currencies/%s — rolled back", pair)
            raise

    logger.info("Saved %d rows for %s → currencies table", len(df), pair)


def save_worldbank_data(commodity: str, df: pd.DataFrame):
    """
    Write World Bank monthly prices to the 'worldbank_prices' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO worldbank_prices
                       (commodity, Date, price, unit)
                       VALUES (?, ?, ?, ?)""",
                    (
                        row["commodity"],
                        row["Date"],
                        float(row["price"]) if pd.notna(row.get("price")) else None,
                        str(row.get("unit", "")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for worldbank/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → worldbank_prices table", len(df), commodity)


def save_export_sales(commodity: str, df: pd.DataFrame):
    """
    Write export sales data to the 'export_sales' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO export_sales
                       (commodity, week_ending, country, net_sales,
                        weekly_exports, accumulated_exports, outstanding_sales)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row.get("commodity", commodity),
                        row.get("week_ending", ""),
                        str(row.get("country", "")),
                        float(row["net_sales"]) if pd.notna(row.get("net_sales")) else None,
                        float(row["weekly_exports"]) if pd.notna(row.get("weekly_exports")) else None,
                        float(row["accumulated_exports"]) if pd.notna(row.get("accumulated_exports")) else None,
                        float(row["outstanding_sales"]) if pd.notna(row.get("outstanding_sales")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for export_sales/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → export_sales table", len(df), commodity)


def save_forward_curve(commodity: str, df: pd.DataFrame):
    """
    Write forward curve data to the 'forward_curve' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity
    df["fetched_date"] = datetime.utcnow().strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO forward_curve
                       (commodity, contract_month, label, ticker, close, fetched_date)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        row.get("commodity", commodity),
                        str(row.get("contract_month", "")),
                        str(row.get("label", "")),
                        str(row.get("ticker", "")),
                        float(row["close"]) if pd.notna(row.get("close")) else None,
                        row.get("fetched_date", ""),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for forward_curve/%s — rolled back", commodity)
            raise

    logger.info("Saved %d contracts for %s → forward_curve table", len(df), commodity)


def save_dce_futures_data(commodity: str, df: pd.DataFrame):
    """
    Write DCE futures data to the 'dce_futures' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO dce_futures
                       (commodity, Date, Open, High, Low, Close,
                        Volume, Open_Interest, Settle)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["commodity"],
                        row["Date"],
                        float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        float(row["High"]) if pd.notna(row.get("High")) else None,
                        float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        float(row["Close"]) if pd.notna(row.get("Close")) else None,
                        float(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                        float(row["Open_Interest"]) if pd.notna(row.get("Open_Interest")) else None,
                        float(row["Settle"]) if pd.notna(row.get("Settle")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for dce_futures/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → dce_futures table", len(df), commodity)


def save_wasde(commodity_key: str, df: pd.DataFrame):
    """
    Write WASDE forecast data to the 'wasde' table.

    commodity_key is like "SOYBEANS/PRODUCTION" — we split it to get
    the commodity and attribute for storage.
    """
    if df.empty:
        return

    df = df.copy()

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                commodity = str(row.get("commodity_desc", commodity_key.split("/")[0]))
                attribute = str(row.get("statisticcat_desc", commodity_key.split("/")[-1]))
                conn.execute(
                    """INSERT OR REPLACE INTO wasde
                       (commodity, year, attribute, value, unit, reference_period)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    (
                        commodity,
                        str(row.get("year", "")),
                        attribute,
                        float(row["Value"]) if pd.notna(row.get("Value")) else None,
                        str(row.get("unit_desc", "")),
                        str(row.get("reference_period_desc", "")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for wasde/%s — rolled back", commodity_key)
            raise

    logger.info("Saved %d rows for %s → wasde table", len(df), commodity_key)


def save_inspections(commodity: str, df: pd.DataFrame):
    """Write export inspections data to the 'inspections' table."""
    if df.empty:
        return

    df = df.copy()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO inspections
                       (commodity, week_ending, inspections_mt)
                       VALUES (?, ?, ?)""",
                    (
                        row.get("commodity", commodity),
                        row.get("week_ending", ""),
                        float(row["inspections_mt"]) if pd.notna(row.get("inspections_mt")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for inspections/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → inspections table", len(df), commodity)


def save_eia_data(series_name: str, df: pd.DataFrame):
    """Write EIA energy data to the 'eia_energy' table."""
    if df.empty:
        return

    df = df.copy()
    df["series_name"] = series_name

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO eia_energy
                       (series_name, Date, value, unit)
                       VALUES (?, ?, ?, ?)""",
                    (
                        row["series_name"],
                        row["Date"],
                        float(row["value"]) if pd.notna(row.get("value")) else None,
                        str(row.get("unit", "")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for eia_energy/%s — rolled back", series_name)
            raise

    logger.info("Saved %d rows for %s → eia_energy table", len(df), series_name)


def save_brazil_estimates(df: pd.DataFrame):
    """Write CONAB Brazil crop estimates to the 'brazil_estimates' table."""
    if df.empty:
        return

    df = df.copy()

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO brazil_estimates
                       (source, commodity, crop_year, attribute, value, unit, report_date)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        str(row.get("source", "CONAB")),
                        str(row.get("commodity", "")),
                        str(row.get("crop_year", "")),
                        str(row.get("attribute", "")),
                        float(row["value"]) if pd.notna(row.get("value")) else None,
                        str(row.get("unit", "")),
                        str(row.get("report_date", "")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for brazil_estimates — rolled back")
            raise

    logger.info("Saved %d rows → brazil_estimates table", len(df))


def save_india_domestic(commodity: str, df: pd.DataFrame):
    """
    Write NCDEX India domestic price data to the 'india_domestic_prices' table.

    Prices are stored in INR/MT (native units — conversion to USD/MT happens
    in the analysis layer using the INR/USD rate from the currencies table).
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO india_domestic_prices
                       (Date, commodity, Open, High, Low, Close, Volume, unit)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["Date"],
                        row["commodity"],
                        float(row["Open"]) if pd.notna(row.get("Open")) else None,
                        float(row["High"]) if pd.notna(row.get("High")) else None,
                        float(row["Low"]) if pd.notna(row.get("Low")) else None,
                        float(row["Close"]) if pd.notna(row.get("Close")) else None,
                        float(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                        str(row.get("Unit", "INR/MT")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for india_domestic/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → india_domestic_prices table", len(df), commodity)


def save_brazil_spot(commodity: str, df: pd.DataFrame):
    """
    Write CEPEA Brazil domestic spot price to the 'brazil_spot_prices' table.

    Prices are stored in BRL/MT (native units — conversion to USD/MT happens
    in the analysis layer using the BRL/USD rate from the currencies table).
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO brazil_spot_prices
                       (Date, commodity, price_brl, unit)
                       VALUES (?, ?, ?, ?)""",
                    (
                        row["Date"],
                        row["commodity"],
                        float(row["price_brl_mt"]) if pd.notna(row.get("price_brl_mt")) else None,
                        str(row.get("Unit", "BRL/MT")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for brazil_spot/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → brazil_spot_prices table", len(df), commodity)


def save_safex(commodity: str, df: pd.DataFrame):
    """
    Write JSE SAFEX South Africa settlement prices to the 'safex_prices' table.

    Prices are stored in ZAR/MT (native units — conversion to USD/MT happens
    in the analysis layer using the ZAR/USD rate from the currencies table).
    """
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO safex_prices
                       (Date, commodity, Close, Volume, unit)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        row["Date"],
                        row["commodity"],
                        float(row["Close"]) if pd.notna(row.get("Close")) else None,
                        float(row["Volume"]) if pd.notna(row.get("Volume")) else None,
                        str(row.get("Unit", "ZAR/MT")),
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for safex/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → safex_prices table", len(df), commodity)


def save_options_sentiment(commodity: str, df: pd.DataFrame):
    """Write options sentiment data to the 'options_sentiment' table."""
    if df.empty:
        return

    df = df.copy()
    df["commodity"] = commodity

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with get_connection() as conn:
        conn.execute("BEGIN")
        try:
            for _, row in df.iterrows():
                conn.execute(
                    """INSERT OR REPLACE INTO options_sentiment
                       (commodity, Date, total_call_oi, total_put_oi,
                        put_call_ratio, avg_call_iv, avg_put_iv)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        row["commodity"],
                        row["Date"],
                        float(row["total_call_oi"]) if pd.notna(row.get("total_call_oi")) else None,
                        float(row["total_put_oi"]) if pd.notna(row.get("total_put_oi")) else None,
                        float(row["put_call_ratio"]) if pd.notna(row.get("put_call_ratio")) else None,
                        float(row["avg_call_iv"]) if pd.notna(row.get("avg_call_iv")) else None,
                        float(row["avg_put_iv"]) if pd.notna(row.get("avg_put_iv")) else None,
                    ),
                )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for options_sentiment/%s — rolled back", commodity)
            raise

    logger.info("Saved %d rows for %s → options_sentiment table", len(df), commodity)
