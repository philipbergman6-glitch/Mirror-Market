"""
Combine price data with economic context into a single SQLite database.

This is the "merge" step — we take the cleaned DataFrames from each
fetcher and store them in tables so the rest of the app can query them
without re-fetching.

Key concepts for learning:
    - sqlite3: Python's built-in database module (no install needed)
    - PRIMARY KEY constraints prevent duplicate rows
    - INSERT OR REPLACE (upsert): if a row with the same key exists,
      it gets replaced instead of creating a duplicate
    - Transactions: group writes so they either ALL succeed or ALL
      roll back — no half-written data
    - Context managers ("with" statements) for safe database connections
"""

import logging
import os
import sqlite3

import pandas as pd

from config import DB_PATH, STORAGE_DIR

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SQL schemas — explicit tables with PRIMARY KEY constraints
# ---------------------------------------------------------------------------
_CREATE_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    commodity   TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    Open        REAL,
    High        REAL,
    Low         REAL,
    Close       REAL,
    Volume      REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_ECONOMIC = """
CREATE TABLE IF NOT EXISTS economic (
    series_name TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    value       REAL,
    PRIMARY KEY (series_name, Date)
);
"""

_CREATE_USDA = """
CREATE TABLE IF NOT EXISTS usda (
    stat_category           TEXT,
    year                    TEXT,
    short_desc              TEXT,
    Value                   TEXT,
    unit_desc               TEXT,
    state_name              TEXT,
    reference_period_desc   TEXT,
    PRIMARY KEY (stat_category, year, short_desc)
);
"""

_CREATE_COT = """
CREATE TABLE IF NOT EXISTS cot (
    commodity           TEXT    NOT NULL,
    Date                TEXT    NOT NULL,
    commercial_long     REAL,
    commercial_short    REAL,
    commercial_net      REAL,
    noncommercial_long  REAL,
    noncommercial_short REAL,
    noncommercial_net   REAL,
    total_open_interest REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_WEATHER = """
CREATE TABLE IF NOT EXISTS weather (
    region          TEXT    NOT NULL,
    Date            TEXT    NOT NULL,
    temp_max        REAL,
    temp_min        REAL,
    precipitation   REAL,
    PRIMARY KEY (region, Date)
);
"""

_CREATE_PSD = """
CREATE TABLE IF NOT EXISTS psd (
    commodity   TEXT    NOT NULL,
    country     TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    attribute   TEXT    NOT NULL,
    value       REAL,
    unit        TEXT,
    PRIMARY KEY (commodity, country, year, attribute)
);
"""

_CREATE_CURRENCIES = """
CREATE TABLE IF NOT EXISTS currencies (
    pair    TEXT    NOT NULL,
    Date    TEXT    NOT NULL,
    Open    REAL,
    High    REAL,
    Low     REAL,
    Close   REAL,
    PRIMARY KEY (pair, Date)
);
"""

_CREATE_WORLDBANK = """
CREATE TABLE IF NOT EXISTS worldbank_prices (
    commodity   TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    price       REAL,
    unit        TEXT,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_DCE_FUTURES = """
CREATE TABLE IF NOT EXISTS dce_futures (
    commodity       TEXT NOT NULL,
    Date            TEXT NOT NULL,
    Open            REAL,
    High            REAL,
    Low             REAL,
    Close           REAL,
    Volume          REAL,
    Open_Interest   REAL,
    Settle          REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_CROP_PROGRESS = """
CREATE TABLE IF NOT EXISTS crop_progress (
    commodity       TEXT    NOT NULL,
    week_ending     TEXT    NOT NULL,
    year            TEXT,
    short_desc      TEXT    NOT NULL,
    Value           TEXT,
    unit_desc       TEXT,
    stat_category   TEXT,
    PRIMARY KEY (commodity, week_ending, short_desc)
);
"""

_CREATE_EXPORT_SALES = """
CREATE TABLE IF NOT EXISTS export_sales (
    commodity           TEXT    NOT NULL,
    week_ending         TEXT    NOT NULL,
    country             TEXT    NOT NULL,
    net_sales           REAL,
    weekly_exports      REAL,
    accumulated_exports REAL,
    outstanding_sales   REAL,
    PRIMARY KEY (commodity, week_ending, country)
);
"""

_CREATE_FORWARD_CURVE = """
CREATE TABLE IF NOT EXISTS forward_curve (
    commodity       TEXT    NOT NULL,
    contract_month  TEXT    NOT NULL,
    label           TEXT,
    ticker          TEXT,
    close           REAL,
    fetched_date    TEXT    NOT NULL,
    PRIMARY KEY (commodity, contract_month)
);
"""

_CREATE_DATA_FRESHNESS = """
CREATE TABLE IF NOT EXISTS data_freshness (
    layer_name      TEXT    NOT NULL PRIMARY KEY,
    last_success    TEXT    NOT NULL,
    rows_fetched    INTEGER
);
"""


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
    with sqlite3.connect(DB_PATH) as conn:
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
        conn.execute(_CREATE_DATA_FRESHNESS)
    logger.info("Database initialised (tables verified) at %s", DB_PATH)


def save_price_data(name: str, df: pd.DataFrame):
    """
    Write an OHLCV DataFrame to the 'prices' table in SQLite.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["commodity"] = name

    # Reset index so the Date becomes a normal column (SQLite-friendly)
    df = df.reset_index()

    # Convert Date to ISO string for consistent storage
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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

    _ensure_storage_dir()

    df = series.reset_index()
    df.columns = ["Date", "value"]
    df["series_name"] = name
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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

    _ensure_storage_dir()

    df = df.copy()
    df["stat_category"] = stat_category

    with sqlite3.connect(DB_PATH) as conn:
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

    _ensure_storage_dir()
    df = df.copy()
    df["commodity"] = commodity

    with sqlite3.connect(DB_PATH) as conn:
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


def read_crop_progress(commodity: str | None = None) -> pd.DataFrame:
    """Read crop progress/condition data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM crop_progress WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM crop_progress", conn)
        except Exception:
            return pd.DataFrame()

    return df


def save_freshness(layer_name: str, rows_fetched: int = 0):
    """
    Record the last successful fetch timestamp for a data layer.

    Called after each layer succeeds in the pipeline so the briefing
    can warn about stale data.
    """
    from datetime import datetime

    _ensure_storage_dir()
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO data_freshness
               (layer_name, last_success, rows_fetched)
               VALUES (?, ?, ?)""",
            (layer_name, now, rows_fetched),
        )
    logger.debug("Freshness recorded for %s at %s (%d rows)", layer_name, now, rows_fetched)


def read_freshness() -> pd.DataFrame:
    """
    Read data freshness timestamps for all layers.

    Returns
    -------
    pd.DataFrame
        Columns: layer_name, last_success, rows_fetched
    """
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        try:
            df = pd.read_sql("SELECT * FROM data_freshness", conn)
        except Exception:
            return pd.DataFrame()

    if "last_success" in df.columns:
        df["last_success"] = pd.to_datetime(df["last_success"])

    return df


def clear_database():
    """
    Drop all tables so we can do a fresh load.

    This is a manual-only utility — it is NOT called during the normal
    pipeline.  Use it from the Python REPL if you need a clean slate:

        >>> from processing.combiner import clear_database
        >>> clear_database()
    """
    _ensure_storage_dir()
    with sqlite3.connect(DB_PATH) as conn:
        for table in ("prices", "economic", "usda", "cot", "weather",
                      "psd", "currencies", "worldbank_prices",
                      "dce_futures", "crop_progress", "export_sales",
                      "forward_curve", "data_freshness"):
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    logger.info("Database cleared.")


def save_cot_data(name: str, df: pd.DataFrame):
    """
    Write COT positioning data to the 'cot' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["commodity"] = name
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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

    _ensure_storage_dir()

    df = df.copy()
    df["region"] = region
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


# ---------------------------------------------------------------------------
# Read functions — used by the dashboard and analysis modules
# ---------------------------------------------------------------------------

def read_prices(commodity: str | None = None) -> pd.DataFrame:
    """
    Read price data back from SQLite.

    Parameters
    ----------
    commodity : str or None
        If given, filter to just that commodity.  Otherwise return all.
    """
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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


# ---------------------------------------------------------------------------
# Layer 6 — PSD (global supply/demand)
# ---------------------------------------------------------------------------

def save_psd_data(commodity: str, df: pd.DataFrame):
    """
    Write PSD data to the 'psd' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()
    df = df.copy()

    with sqlite3.connect(DB_PATH) as conn:
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


def read_psd(commodity: str | None = None) -> pd.DataFrame:
    """Read PSD global supply/demand data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        if commodity:
            df = pd.read_sql(
                "SELECT * FROM psd WHERE commodity = ?",
                conn,
                params=(commodity,),
            )
        else:
            df = pd.read_sql("SELECT * FROM psd", conn)

    return df


# ---------------------------------------------------------------------------
# Layer 7 — Currencies
# ---------------------------------------------------------------------------

def save_currency_data(pair: str, df: pd.DataFrame):
    """
    Write currency OHLCV data to the 'currencies' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["pair"] = pair
    df = df.reset_index()
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


def read_currencies(pair: str | None = None) -> pd.DataFrame:
    """Read currency data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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


# ---------------------------------------------------------------------------
# Layer 8 — World Bank monthly prices
# ---------------------------------------------------------------------------

def save_worldbank_data(commodity: str, df: pd.DataFrame):
    """
    Write World Bank monthly prices to the 'worldbank_prices' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


def read_worldbank_prices(commodity: str | None = None) -> pd.DataFrame:
    """Read World Bank monthly price data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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


# ---------------------------------------------------------------------------
# Layer 10 — Export Sales (USDA FAS ESR)
# ---------------------------------------------------------------------------

def save_export_sales(commodity: str, df: pd.DataFrame):
    """
    Write export sales data to the 'export_sales' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["commodity"] = commodity

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


def read_export_sales(commodity: str | None = None) -> pd.DataFrame:
    """Read export sales data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM export_sales WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM export_sales", conn)
        except Exception:
            return pd.DataFrame()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"])

    return df


# ---------------------------------------------------------------------------
# Layer 11 — Forward Curve
# ---------------------------------------------------------------------------

def save_forward_curve(commodity: str, df: pd.DataFrame):
    """
    Write forward curve data to the 'forward_curve' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    from datetime import datetime

    df = df.copy()
    df["commodity"] = commodity
    df["fetched_date"] = datetime.utcnow().strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


def read_forward_curve(commodity: str | None = None) -> pd.DataFrame:
    """Read forward curve data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
        try:
            if commodity:
                df = pd.read_sql(
                    "SELECT * FROM forward_curve WHERE commodity = ?",
                    conn,
                    params=(commodity,),
                )
            else:
                df = pd.read_sql("SELECT * FROM forward_curve", conn)
        except Exception:
            return pd.DataFrame()

    return df


# ---------------------------------------------------------------------------
# Layer 9 — DCE futures (Chinese exchange)
# ---------------------------------------------------------------------------

def save_dce_futures_data(commodity: str, df: pd.DataFrame):
    """
    Write DCE futures data to the 'dce_futures' table.

    Uses INSERT OR REPLACE so re-running the pipeline updates existing
    rows instead of creating duplicates.
    """
    if df.empty:
        return

    _ensure_storage_dir()

    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")

    with sqlite3.connect(DB_PATH) as conn:
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


def read_dce_futures(commodity: str | None = None) -> pd.DataFrame:
    """Read DCE futures data from SQLite."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with sqlite3.connect(DB_PATH) as conn:
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
