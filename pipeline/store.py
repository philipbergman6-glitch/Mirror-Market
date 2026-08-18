"""Database write functions for Mirror Market.

`upsert_dataframe` factors all batch INSERT OR REPLACE writes into one
executemany call — replaces 19 iterrows loops with a single helper. Each
`save_*` function reshapes its input DataFrame (rename, date-format, add
key columns), then delegates to `_save` for the transactional write.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from config import (
    DB_PATH,
    EC_OILSEEDS_CADENCE,
    EC_OILSEEDS_QUOTE_KIND,
    STORAGE_DIR,
)
from pipeline.connection import get_connection, is_cloud, managed_connection, maybe_sync
from pipeline.schema import ALL_SCHEMAS, UNIQUE_INDEXES

logger = logging.getLogger(__name__)


# --- Lifecycle --------------------------------------------------------------


def _ensure_storage_dir():
    os.makedirs(STORAGE_DIR, exist_ok=True)


def _migrate_data_freshness(conn) -> None:
    """Add status/last_attempt/key-coverage to data_freshness if absent.

    Idempotent. CREATE TABLE IF NOT EXISTS never widens an existing table,
    so every column added after the original definition needs an entry here
    or a pre-existing local DB silently keeps the narrow shape.
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(data_freshness)").fetchall()}
    except Exception:
        return
    for col, ddl in (
        ("status", "ALTER TABLE data_freshness ADD COLUMN status TEXT NOT NULL DEFAULT 'success'"),
        ("last_attempt", "ALTER TABLE data_freshness ADD COLUMN last_attempt TEXT"),
        ("keys_returned", "ALTER TABLE data_freshness ADD COLUMN keys_returned INTEGER"),
        ("keys_expected", "ALTER TABLE data_freshness ADD COLUMN keys_expected INTEGER"),
    ):
        if col not in cols:
            try:
                conn.execute(ddl)
            except Exception as exc:
                logger.warning("Could not add %s column to data_freshness: %s", col, exc)


def _migrate_export_sales_unit(conn) -> None:
    """Add the unit column to export_sales if absent. Idempotent."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(export_sales)").fetchall()}
    except Exception:
        return
    if cols and "unit" not in cols:
        try:
            conn.execute("ALTER TABLE export_sales ADD COLUMN unit TEXT")
        except Exception as exc:
            logger.warning("Could not add unit column to export_sales: %s", exc)


def _migrate_safex_contract(conn) -> None:
    """Add the contract column to safex_prices if absent. Idempotent."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(safex_prices)").fetchall()}
    except Exception:
        return
    if cols and "contract" not in cols:
        try:
            conn.execute("ALTER TABLE safex_prices ADD COLUMN contract TEXT")
        except Exception as exc:
            logger.warning("Could not add contract column to safex_prices: %s", exc)


def _migrate_weather_is_forecast(conn) -> None:
    """Add the is_forecast column to weather if absent. Idempotent.

    NULL means the row predates the flag — treated as observed downstream.
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(weather)").fetchall()}
    except Exception:
        return
    if cols and "is_forecast" not in cols:
        try:
            conn.execute("ALTER TABLE weather ADD COLUMN is_forecast INTEGER")
        except Exception as exc:
            logger.warning("Could not add is_forecast column to weather: %s", exc)


def _migrate_gulf_bids_price_change(conn) -> None:
    """Add the price_change column to gulf_bids if absent. Idempotent.

    Ranged price changes are what dropped the headline soybean row (#190);
    the column is stored verbatim, both endpoints kept.
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(gulf_bids)").fetchall()}
    except Exception:
        return
    if cols and "price_change" not in cols:
        try:
            conn.execute("ALTER TABLE gulf_bids ADD COLUMN price_change TEXT")
        except Exception as exc:
            logger.warning("Could not add price_change column to gulf_bids: %s", exc)


def _migrate_gulf_bids_futures_month_high(conn) -> None:
    """Add the futures_month_high column to gulf_bids if absent. Idempotent.

    A ranged basis quote can span two CBOT contracts ("95.00Q to 100.00X");
    storing only the low leg's month labelled the high leg with the wrong
    futures contract (#196). Rows predating this column keep NULL, which
    readers treat as "same contract as the low leg".
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(gulf_bids)").fetchall()}
    except Exception:
        return
    if cols and "futures_month_high" not in cols:
        try:
            conn.execute("ALTER TABLE gulf_bids ADD COLUMN futures_month_high INTEGER")
        except Exception as exc:
            logger.warning(
                "Could not add futures_month_high column to gulf_bids: %s", exc
            )


def _migrate_usda_pk(conn) -> None:
    """Rebuild the usda table with reference_period_desc in the PK. Idempotent.

    The original 3-column key collapsed monthly NASS series (crush) to one
    surviving row per year. SQLite can't alter a PK in place, so detect the
    old shape and rebuild. The stale 3-column unique index is dropped in
    both cases — left behind, it would keep collapsing rows on upsert.
    """
    try:
        conn.execute("DROP INDEX IF EXISTS ux_usda_cat_year_desc")
        pk_cols = {r[1] for r in conn.execute("PRAGMA table_info(usda)").fetchall() if r[5]}
    except Exception:
        return
    if "reference_period_desc" in pk_cols or not pk_cols:
        return
    try:
        conn.execute("ALTER TABLE usda RENAME TO usda_old_pk")
        conn.execute(
            """CREATE TABLE usda (
                   stat_category           TEXT,
                   year                    TEXT,
                   short_desc              TEXT,
                   Value                   TEXT,
                   unit_desc               TEXT,
                   state_name              TEXT,
                   reference_period_desc   TEXT,
                   PRIMARY KEY (stat_category, year, short_desc, reference_period_desc)
               )"""
        )
        conn.execute(
            """INSERT OR REPLACE INTO usda
               SELECT stat_category, year, short_desc, Value,
                      unit_desc, state_name, reference_period_desc
               FROM usda_old_pk"""
        )
        conn.execute("DROP TABLE usda_old_pk")
        logger.info("Migrated usda table PK to include reference_period_desc")
    except Exception:
        logger.exception("usda PK migration failed — leaving table as-is")


def _migrate_forward_curve_pk(conn) -> None:
    """Rebuild forward_curve with fetched_date in the PK. Idempotent.

    The original (commodity, contract_month) key overwrote the whole curve
    every run, making term-structure history structurally impossible.
    SQLite can't alter a PK in place, so detect the old shape and rebuild.
    The stale 2-column unique index is dropped in both cases — left behind,
    it would keep collapsing one row per contract on upsert.
    """
    try:
        conn.execute("DROP INDEX IF EXISTS ux_forward_curve_commodity_contract")
        pk_cols = {r[1] for r in conn.execute("PRAGMA table_info(forward_curve)").fetchall() if r[5]}
    except Exception:
        return
    if "fetched_date" in pk_cols or not pk_cols:
        return
    try:
        conn.execute("ALTER TABLE forward_curve RENAME TO forward_curve_old_pk")
        conn.execute(
            """CREATE TABLE forward_curve (
                   commodity       TEXT    NOT NULL,
                   contract_month  TEXT    NOT NULL,
                   label           TEXT,
                   ticker          TEXT,
                   close           REAL,
                   fetched_date    TEXT    NOT NULL,
                   PRIMARY KEY (commodity, contract_month, fetched_date)
               )"""
        )
        conn.execute(
            """INSERT OR REPLACE INTO forward_curve
               SELECT commodity, contract_month, label, ticker, close, fetched_date
               FROM forward_curve_old_pk"""
        )
        conn.execute("DROP TABLE forward_curve_old_pk")
        logger.info("Migrated forward_curve table PK to include fetched_date")
    except Exception:
        logger.exception("forward_curve PK migration failed — leaving table as-is")


def _migrate_forward_curve_observation_date(conn) -> None:
    """Add the observation_date column to forward_curve if absent. Idempotent.

    Must run *after* _migrate_forward_curve_pk, which rebuilds the table at
    its pre-observation_date shape. Legacy rows keep NULL — they were stored
    with mixed leg dates, so there is no single date to backfill them with.
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(forward_curve)").fetchall()}
    except Exception:
        return
    if cols and "observation_date" not in cols:
        try:
            conn.execute("ALTER TABLE forward_curve ADD COLUMN observation_date TEXT")
        except Exception as exc:
            logger.warning("Could not add observation_date column to forward_curve: %s", exc)


def _migrate_forward_curve_liquidity(conn) -> None:
    """Add volume / open_interest to forward_curve if absent. Idempotent.

    Both stay NULL on legacy rows. That is the honest backfill: the fetcher
    discarded volume before Phase 3, and no source here has ever published open
    interest, so there is nothing to fill them with and a zero would read as a
    contract that did not trade.
    """
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(forward_curve)").fetchall()}
    except Exception:
        return
    for column in ("volume", "open_interest"):
        if cols and column not in cols:
            try:
                conn.execute(f"ALTER TABLE forward_curve ADD COLUMN {column} REAL")
            except Exception as exc:
                logger.warning("Could not add %s column to forward_curve: %s", column, exc)


def init_database():
    """Create tables + unique indexes if missing. Idempotent."""
    _ensure_storage_dir()
    with managed_connection(get_connection()) as conn:
        for ddl in ALL_SCHEMAS:
            conn.execute(ddl)
        _migrate_usda_pk(conn)
        _migrate_forward_curve_pk(conn)
        _migrate_forward_curve_observation_date(conn)
        _migrate_forward_curve_liquidity(conn)
        _migrate_export_sales_unit(conn)
        _migrate_weather_is_forecast(conn)
        _migrate_safex_contract(conn)
        _migrate_gulf_bids_price_change(conn)
        _migrate_gulf_bids_futures_month_high(conn)
        for index_sql in UNIQUE_INDEXES:
            conn.execute(index_sql)
        _migrate_data_freshness(conn)
        maybe_sync(conn)
    logger.info("Database initialised (tables verified) at %s", DB_PATH)


def clear_database():
    """Drop all tables. Manual-only utility."""
    _ensure_storage_dir()
    tables = [
        "prices", "economic", "usda", "cot", "weather", "psd",
        "currencies", "worldbank_prices", "dce_futures", "crop_progress",
        "export_sales", "forward_curve", "wasde", "inspections",
        "inspection_port_flows", "inspection_destinations", "gulf_bids",
        "argentina_fob",
        "eia_energy", "brazil_estimates", "data_freshness",
        "commodity_freshness", "india_domestic_prices",
        "brazil_spot_prices", "safex_prices", "sagis_deliveries", "briefings",
    ]
    with managed_connection(get_connection()) as conn:
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
    logger.info("Database cleared.")


# --- Generic helpers --------------------------------------------------------


def upsert_dataframe(conn, table: str, df: pd.DataFrame, key_cols: list[str]) -> int:
    """Bulk INSERT OR REPLACE a DataFrame into `table` via executemany.

    The DataFrame's columns must match the destination table's columns
    exactly — pre-shape the df before calling. NaN → NULL.

    `key_cols` is informational (used in debug log); uniqueness is
    enforced by the table's PRIMARY KEY / UNIQUE INDEX. Returns rows
    written. Empty df returns 0 without opening the DB.
    """
    if df.empty:
        return 0
    cols = list(df.columns)
    sql = (
        f"INSERT OR REPLACE INTO {table} ({','.join(cols)}) "
        f"VALUES ({','.join('?' * len(cols))})"
    )
    df_obj = df.astype(object).where(df.notna(), None)
    rows = list(df_obj.itertuples(index=False, name=None))
    conn.executemany(sql, rows)
    logger.debug("upsert %s: %d rows (keys=%s)", table, len(rows), ",".join(key_cols))
    return len(rows)


def _save(table: str, df: pd.DataFrame, key_cols: list[str], label: str) -> int:
    """Open a connection and run a transactional upsert. Logs result."""
    if df.empty:
        return 0
    with managed_connection(get_connection()) as conn:
        conn.execute("BEGIN")
        try:
            n = upsert_dataframe(conn, table, df, key_cols)
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            logger.error("Transaction failed for %s — rolled back", label)
            raise
        maybe_sync(conn)
    logger.info("Saved %d rows for %s → %s table", n, label, table)
    return n


def _date(s: pd.Series) -> pd.Series:
    """ISO-format a date column. NaT becomes NaT and is NULLed at write."""
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y-%m-%d")


def _str_cols(df: pd.DataFrame, *cols: str, default: str = "") -> pd.DataFrame:
    """In-place: ensure each `cols` column exists and is string-typed."""
    for c in cols:
        if c not in df.columns:
            df[c] = default
        df[c] = df[c].fillna(default).astype(str)
    return df


# --- save_* functions -------------------------------------------------------


def save_price_data(name: str, df: pd.DataFrame):
    """Write OHLCV → 'prices'."""
    if df.empty:
        return
    df = df.reset_index().copy()
    df["commodity"] = name
    df["Date"] = _date(df["Date"])
    df = df[["commodity", "Date", "Open", "High", "Low", "Close", "Volume"]]
    _save("prices", df, ["commodity", "Date"], f"prices/{name}")


# Series whose FRED id was replaced with one on a different index base.
# Stored history from the old id must be wiped before the new values land,
# otherwise the 'economic' table would mix two incompatible bases under one
# display name. ("Soybean Oil PPI": WPU0612 → PCU31122431122431, 2026-07.)
_ECONOMIC_SERIES_RESET = {"Soybean Oil PPI"}


def save_fred_data(name: str, series: pd.Series):
    """Write FRED Series → 'economic'."""
    if series.empty:
        return
    if name in _ECONOMIC_SERIES_RESET:
        with managed_connection(get_connection()) as conn:
            conn.execute("BEGIN")
            try:
                conn.execute("DELETE FROM economic WHERE series_name = ?", (name,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                logger.error("Failed to reset economic/%s — rolled back", name)
                raise
        logger.info("Reset stored history for economic/%s (series id changed)", name)
    df = series.reset_index()
    df.columns = ["Date", "value"]
    df["series_name"] = name
    df["Date"] = _date(df["Date"])
    _save("economic", df[["series_name", "Date", "value"]],
          ["series_name", "Date"], f"economic/{name}")


def save_usda_data(df: pd.DataFrame, stat_category: str):
    """Write USDA → 'usda'."""
    if df.empty:
        return
    df = df.copy()
    df["stat_category"] = stat_category
    cols = ["stat_category", "year", "short_desc", "Value",
            "unit_desc", "state_name", "reference_period_desc"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    _save("usda", df[cols], ["stat_category", "year", "short_desc"], f"usda/{stat_category}")


def save_crop_progress(commodity: str, df: pd.DataFrame):
    """Write crop progress → 'crop_progress'. Source 'statisticcat_desc' → 'stat_category'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "statisticcat_desc" in df.columns:
        df["stat_category"] = df["statisticcat_desc"]
    cols = ["commodity", "week_ending", "year", "short_desc",
            "Value", "unit_desc", "stat_category"]
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    _save("crop_progress", df[cols],
          ["commodity", "week_ending", "short_desc"], f"crop_progress/{commodity}")


def save_cot_data(name: str, df: pd.DataFrame):
    """Write COT positioning → 'cot'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = name
    df["Date"] = _date(df["Date"])
    _save("cot", df[[
        "commodity", "Date", "commercial_long", "commercial_short", "commercial_net",
        "noncommercial_long", "noncommercial_short", "noncommercial_net", "total_open_interest",
    ]], ["commodity", "Date"], f"cot/{name}")


def save_weather_data(region: str, df: pd.DataFrame):
    """Write weather → 'weather'."""
    if df.empty:
        return
    df = df.copy()
    df["region"] = region
    df["Date"] = _date(df["Date"])
    if "is_forecast" not in df.columns:
        df["is_forecast"] = None  # legacy callers: NULL = observed
    _save("weather",
          df[["region", "Date", "temp_max", "temp_min", "precipitation", "is_forecast"]],
          ["region", "Date"], f"weather/{region}")


def save_psd_data(commodity: str, df: pd.DataFrame):
    """Write PSD → 'psd'. Drops rows with NaN year (INTEGER NOT NULL key)."""
    if df.empty:
        return
    df = df.copy()
    if "commodity" not in df.columns:
        df["commodity"] = commodity
    df = _str_cols(df, "commodity", "country", "attribute", "unit")
    df = df.dropna(subset=["year"])
    if df.empty:
        return
    df["year"] = df["year"].astype(int)
    _save("psd", df[["commodity", "country", "year", "attribute", "value", "unit"]],
          ["commodity", "country", "year", "attribute"], f"psd/{commodity}")


def save_currency_data(pair: str, df: pd.DataFrame):
    """Write currency OHLC → 'currencies'."""
    if df.empty:
        return
    df = df.reset_index().copy()
    df["pair"] = pair
    df["Date"] = _date(df["Date"])
    _save("currencies", df[["pair", "Date", "Open", "High", "Low", "Close"]],
          ["pair", "Date"], f"currencies/{pair}")


def save_worldbank_data(commodity: str, df: pd.DataFrame):
    """Write World Bank monthly prices → 'worldbank_prices'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = _date(df["Date"])
    df = _str_cols(df, "unit")
    _save("worldbank_prices", df[["commodity", "Date", "price", "unit"]],
          ["commodity", "Date"], f"worldbank/{commodity}")


def save_ec_oilseed_prices(series: str, df: pd.DataFrame):
    """Write EC weekly world oilseed prices → 'ec_oilseed_prices'.

    cadence and quote_kind are stamped on every row rather than looked up at
    display time: this is a weekly *physical assessment*, and map #142 carries
    a standing risk that such quotes get read as daily board prices once they
    share a USD/MT axis with CBOT and Dalian.
    """
    if df.empty:
        return
    df = df.copy()
    df["series"] = series
    df["Date"] = _date(df["Date"])
    df["cadence"] = EC_OILSEEDS_CADENCE
    df["quote_kind"] = EC_OILSEEDS_QUOTE_KIND
    _save(
        "ec_oilseed_prices",
        df[["series", "Date", "price_usd", "price_eur", "cadence", "quote_kind"]],
        ["series", "Date"],
        f"ec_oilseeds/{series}",
    )


def save_export_sales(commodity: str, df: pd.DataFrame):
    """Write weekly export sales → 'export_sales'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "week_ending" in df.columns:
        df["week_ending"] = _date(df["week_ending"])
    df = _str_cols(df, "country", "unit")
    _save("export_sales", df[[
        "commodity", "week_ending", "country", "net_sales",
        "weekly_exports", "accumulated_exports", "outstanding_sales", "unit",
    ]], ["commodity", "week_ending", "country"], f"export_sales/{commodity}")


def save_forward_curve(commodity: str, df: pd.DataFrame):
    """Write forward curve → 'forward_curve'. Stamps fetched_date with today UTC.

    observation_date is the fetcher's — the session the whole curve was read
    at — and is never derived from the clock here.

    A curve **replaces** whatever this commodity already had under the same
    fetched_date rather than merging into it. INSERT OR REPLACE alone leaves a
    leg from an earlier run on the same day standing, because the key is
    (commodity, contract_month, fetched_date) and a leg the newer run did not
    fetch is never touched. That is not hypothetical: the committed history for
    2026-08-11 carries seven Soybean legs, six stamped that session by the run
    that added the current delivery month and ``ZSN27.CBT`` left over from the
    earlier one, undated. A curve is a snapshot of one moment, so a partial
    merge of two snapshots is not a curve — and the stale leg is invisible,
    since it has the right key and a plausible price (Phase 3).
    """
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    fetched_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["fetched_date"] = fetched_date
    df = _str_cols(df, "contract_month", "label", "ticker")
    # Kept nullable rather than "" — a curve stored without an observation
    # date must read as "never learned", not as a blank date.
    df["observation_date"] = (
        df["observation_date"].map(lambda v: None if pd.isna(v) else str(v))
        if "observation_date" in df.columns else None
    )
    # Volume is per-contract and the provider does give it; open interest is
    # not published by any source this project ingests and is written NULL
    # rather than zero — "never learned" and "none traded" are different facts.
    for column in ("volume", "open_interest"):
        if column not in df.columns:
            df[column] = None
    _replace_curve_snapshot(commodity, fetched_date)
    _save(
        "forward_curve",
        df[["commodity", "contract_month", "label", "ticker", "close",
            "observation_date", "volume", "open_interest", "fetched_date"]],
        ["commodity", "contract_month", "fetched_date"],
        f"forward_curve/{commodity}",
    )


def _replace_curve_snapshot(commodity: str, fetched_date: str) -> None:
    """Clear this commodity's rows for one fetched_date before rewriting them."""
    with managed_connection(get_connection()) as conn:
        cursor = conn.execute(
            "DELETE FROM forward_curve WHERE commodity = ? AND fetched_date = ?",
            (commodity, fetched_date),
        )
        removed = cursor.rowcount if cursor.rowcount is not None else 0
        if removed:
            logger.info(
                "forward_curve/%s: cleared %d leg(s) already stored for %s before rewriting "
                "this run's snapshot",
                commodity, removed, fetched_date,
            )
        maybe_sync(conn)


def save_dce_futures_data(commodity: str, df: pd.DataFrame):
    """Write DCE futures → 'dce_futures'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    df["Date"] = _date(df["Date"])
    _save("dce_futures", df[[
        "commodity", "Date", "Open", "High", "Low", "Close",
        "Volume", "Open_Interest", "Settle",
    ]], ["commodity", "Date"], f"dce_futures/{commodity}")


def save_wasde(commodity_key: str, df: pd.DataFrame):
    """Write WASDE forecast → 'wasde'.

    commodity_key (e.g., 'SOYBEANS/PRODUCTION') is used as a fallback when
    source columns commodity_desc / statisticcat_desc are missing or NaN.
    """
    if df.empty:
        return
    df = df.copy()
    parts = commodity_key.split("/")
    c_default = parts[0] if parts else ""
    a_default = parts[-1] if len(parts) > 1 else c_default
    df["commodity"] = (df["commodity_desc"] if "commodity_desc" in df.columns
                       else pd.Series([c_default] * len(df), index=df.index))
    df["attribute"] = (df["statisticcat_desc"] if "statisticcat_desc" in df.columns
                       else pd.Series([a_default] * len(df), index=df.index))
    df["commodity"] = df["commodity"].fillna(c_default).astype(str)
    df["attribute"] = df["attribute"].fillna(a_default).astype(str)
    df["year"] = df.get("year", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
    df["value"] = df.get("Value")
    df["unit"] = df.get("unit_desc", pd.Series([""] * len(df), index=df.index)).fillna("").astype(str)
    df["reference_period"] = df.get(
        "reference_period_desc", pd.Series([""] * len(df), index=df.index)
    ).fillna("").astype(str)
    _save("wasde", df[["commodity", "year", "attribute", "value", "unit", "reference_period"]],
          ["commodity", "year", "attribute", "reference_period"], f"wasde/{commodity_key}")


def save_inspections(commodity: str, df: pd.DataFrame):
    """Write export inspections → 'inspections'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "week_ending" in df.columns:
        df["week_ending"] = _date(df["week_ending"])
    _save("inspections", df[["commodity", "week_ending", "inspections_mt"]],
          ["commodity", "week_ending"], f"inspections/{commodity}")


def save_port_flows(df: pd.DataFrame):
    """Write AMS port-area export inspections → 'inspection_port_flows'."""
    if df.empty:
        return
    df = df.copy()
    if "week_ending" in df.columns:
        df["week_ending"] = _date(df["week_ending"])
    df = _str_cols(df, "region", "port_area", "commodity")
    _save(
        "inspection_port_flows",
        df[["week_ending", "region", "port_area", "commodity", "inspections_mt"]],
        ["week_ending", "region", "port_area", "commodity"],
        "inspection_port_flows",
    )


def save_inspection_destinations(df: pd.DataFrame):
    """Write AMS destination-country export inspections → 'inspection_destinations'."""
    if df.empty:
        return
    df = df.copy()
    if "week_ending" in df.columns:
        df["week_ending"] = _date(df["week_ending"])
    df = _str_cols(df, "region", "country", "commodity")
    _save(
        "inspection_destinations",
        df[["week_ending", "region", "country", "commodity", "inspections_mt"]],
        ["week_ending", "region", "country", "commodity"],
        "inspection_destinations",
    )


def save_argentina_fob(df: pd.DataFrame):
    """Write MAGyP official Argentina FOB prices → 'argentina_fob'."""
    if df.empty:
        return
    df = df.copy()
    if "date" in df.columns:
        df["date"] = _date(df["date"])
    df = _str_cols(df, "product", "position", "ship_from", "ship_to")
    _save(
        "argentina_fob",
        df[["date", "product", "position", "ship_from", "ship_to", "price_usd_mt"]],
        ["date", "position", "ship_from"],
        "argentina_fob",
    )


def save_gulf_bids(df: pd.DataFrame):
    """Write AMS CIF Gulf export bids → 'gulf_bids'."""
    if df.empty:
        return
    df = df.copy()
    if "report_date" in df.columns:
        df["report_date"] = _date(df["report_date"])
    df = _str_cols(df, "commodity", "location", "delivery", "sale_type", "freight")
    # AMS leaves the change columns blank on some deliveries (#190) — those
    # stay NULL rather than becoming an empty string that reads as a value.
    for col in ("basis_change", "price_change"):
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].where(df[col].notna(), None)
    # A frame predating the split-contract column (#196) quotes both legs
    # against the same contract by construction — carry the low leg across.
    if "futures_month_high" not in df.columns:
        df["futures_month_high"] = df.get("futures_month")
    _save(
        "gulf_bids",
        df[[
            "report_date", "commodity", "location", "delivery", "sale_type",
            "basis_low", "basis_high", "futures_month", "futures_month_high",
            "basis_change",
            "price_change", "price_low", "price_high", "average", "year_ago",
            "freight",
        ]],
        ["report_date", "commodity", "location", "delivery"],
        "gulf_bids",
    )


def save_eia_data(series_name: str, df: pd.DataFrame):
    """Write EIA energy → 'eia_energy'."""
    if df.empty:
        return
    df = df.copy()
    df["series_name"] = series_name
    if "Date" in df.columns:
        df["Date"] = _date(df["Date"])
    df = _str_cols(df, "unit")
    _save("eia_energy", df[["series_name", "Date", "value", "unit"]],
          ["series_name", "Date"], f"eia/{series_name}")


def save_brazil_estimates(df: pd.DataFrame):
    """Write CONAB estimates → 'brazil_estimates'."""
    if df.empty:
        return
    df = df.copy()
    df = _str_cols(df, "source", "commodity", "crop_year", "attribute", "unit", "report_date")
    df["source"] = df["source"].replace("", "CONAB")
    _save(
        "brazil_estimates",
        df[["source", "commodity", "crop_year", "attribute", "value", "unit", "report_date"]],
        ["source", "commodity", "crop_year", "attribute", "report_date"],
        "brazil_estimates",
    )


def save_india_domestic(commodity: str, df: pd.DataFrame):
    """Write India domestic soy prices (INR/MT) → 'india_domestic_prices'.

    Serves the mandi (Agmarknet) series since 2026-08; the retired NCDEX
    rows share the table under their own commodity keys, never spliced.
    """
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "Date" in df.columns:
        df["Date"] = _date(df["Date"])
    if "Unit" in df.columns:
        df["unit"] = df["Unit"].fillna("INR/MT").astype(str)
    else:
        df["unit"] = "INR/MT"
    _save("india_domestic_prices",
          df[["Date", "commodity", "Open", "High", "Low", "Close", "Volume", "unit"]],
          ["Date", "commodity"], f"india_domestic/{commodity}")


def save_brazil_spot(commodity: str, df: pd.DataFrame):
    """Write CEPEA spot (BRL/MT) → 'brazil_spot_prices'. Renames price_brl_mt → price_brl."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "Date" in df.columns:
        df["Date"] = _date(df["Date"])
    if "price_brl_mt" in df.columns:
        df["price_brl"] = df["price_brl_mt"]
    if "Unit" in df.columns:
        df["unit"] = df["Unit"].fillna("BRL/MT").astype(str)
    else:
        df["unit"] = "BRL/MT"
    _save("brazil_spot_prices", df[["Date", "commodity", "price_brl", "unit"]],
          ["Date", "commodity"], f"brazil_spot/{commodity}")


def save_safex(commodity: str, df: pd.DataFrame):
    """Write JSE SAFEX (ZAR/MT) → 'safex_prices'."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "Date" in df.columns:
        df["Date"] = _date(df["Date"])
    if "Unit" in df.columns:
        df["unit"] = df["Unit"].fillna("ZAR/MT").astype(str)
    else:
        df["unit"] = "ZAR/MT"
    if "Contract" in df.columns:
        df["contract"] = df["Contract"]
    elif "contract" not in df.columns:
        df["contract"] = None
    _save("safex_prices", df[["Date", "commodity", "Close", "Volume", "unit", "contract"]],
          ["Date", "commodity"], f"safex/{commodity}")


_SAGIS_COLUMNS = [
    "commodity", "season_year", "week_number", "week_end", "season_status",
    "first_published", "adjustments", "week_total", "unit",
]


def save_sagis_deliveries(commodity: str, df: pd.DataFrame):
    """Write SAGIS weekly producer deliveries (MT) → 'sagis_deliveries'.

    INSERT OR REPLACE on (commodity, season_year, week_number) is load-
    bearing here, not just re-run safety: SAGIS revises past weeks for
    months — a closed season's `adjustments` keep moving — so every run
    legitimately rewrites rows it has already stored.
    """
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "unit" not in df.columns:
        df["unit"] = "MT"
    if "week_end" in df.columns:
        # The cleaner hands back a Timestamp column; SQLite takes ISO text.
        df["week_end"] = _date(df["week_end"])
    missing = [c for c in _SAGIS_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"save_sagis_deliveries: frame missing columns {missing}")
    _save("sagis_deliveries", df[_SAGIS_COLUMNS],
          ["commodity", "season_year", "week_number"], f"sagis/{commodity}")


_SAGIS_SMD_COLUMNS = [
    "commodity", "season_year", "month_number", "month_end", "report_month",
    "opening_stock", "deliveries", "imports", "processed_total",
    "processed_human", "processed_feed", "processed_oil_oilcake",
    "withdrawn_by_producers", "released_to_end_consumers", "seed_for_planting",
    "exports_whole", "exports_border_posts", "exports_harbours",
    "sundries_net_dispatches", "sundries_surplus_deficit", "unutilised_stock",
    "stock_storers_traders", "stock_processors", "products_exported",
    "products_exported_africa", "products_exported_other", "unit",
]


def save_sagis_smd(commodity: str, df: pd.DataFrame):
    """Write SAGIS monthly soybean supply & demand (MT) → 'sagis_supply_demand'.

    INSERT OR REPLACE on (commodity, season_year, month_number) is load-
    bearing, as it is for Layer 23: SAGIS revises months for a year or more
    after the fact — a month first published as preliminary is restated in
    later reports, and closed seasons keep moving — so every run legitimately
    rewrites rows it has already stored. `report_month` records which vintage
    the stored numbers came from.
    """
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "unit" not in df.columns:
        df["unit"] = "MT"
    for col in ("month_end", "report_month"):
        if col in df.columns:
            # The cleaner hands back Timestamp columns; SQLite takes ISO text.
            df[col] = _date(df[col])
    missing = [c for c in _SAGIS_SMD_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"save_sagis_smd: frame missing columns {missing}")
    _save("sagis_supply_demand", df[_SAGIS_SMD_COLUMNS],
          ["commodity", "season_year", "month_number"], f"sagis_smd/{commodity}")


_CEC_COLUMNS = [
    "commodity", "season_year", "release_date", "estimate_kind",
    "forecast_seq", "forecast_label", "area_ha", "production_t", "unit",
    "source_url",
]


def save_cec_estimates(commodity: str, df: pd.DataFrame):
    """Write CEC South Africa crop estimates → 'cec_estimates'.

    One row per release per season, so the revision path is kept rather than
    collapsed to a current estimate. The upsert key includes release_date:
    re-reading the same PDF rewrites the same row, and a *new* release adds
    one — a report is never edited in place upstream.
    """
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    if "unit" not in df.columns:
        df["unit"] = "MT"
    missing = [c for c in _CEC_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"save_cec_estimates: frame missing columns {missing}")
    _save("cec_estimates", df[_CEC_COLUMNS],
          ["commodity", "season_year", "release_date"], f"cec/{commodity}")


# --- Briefing archive -------------------------------------------------------


def save_briefing(
    briefing_date: str,
    text: str,
    signals: list[dict] | None = None,
    snapshot: dict[str, Any] | None = None,
) -> None:
    """Archive a generated briefing. INSERT OR REPLACE keyed on briefing_date.

    `signals` and `snapshot` are serialized to JSON with `default=str` so
    pandas Timestamps and numpy scalars survive without a custom encoder.
    """
    if not briefing_date or not text:
        raise ValueError("briefing_date and text are required")
    signals_json = json.dumps(signals or [], default=str)
    snapshot_json = json.dumps(snapshot or {}, default=str)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with managed_connection(get_connection()) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO briefings
               (briefing_date, text, signals_json, snapshot_json, generated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (briefing_date, text, signals_json, snapshot_json, generated_at),
        )
        maybe_sync(conn)
    logger.info("Archived briefing for %s (%d signals)", briefing_date, len(signals or []))


def save_origin_ranking(rows: list[dict[str, Any]]) -> int:
    """Archive one origin comparison, one row per origin. Returns rows written.

    Stored rather than re-derived for the same reason the briefing is: the
    ranking is a function of that morning's database *and* of the assumption
    file as it stood at the time, and an expired freight assumption leaves the
    working set by design. "What did we say on the 12th, and what did we say it
    on" is unanswerable from any source once the day has passed.

    Unrankable rows are written with ``rank = NULL``. That is a result — an
    origin quoted for the wrong window is a fact about that day's market — and
    dropping them would leave the history claiming an origin was silent when it
    was merely offering something else.
    """
    if not rows:
        return 0
    required = {
        "run_date", "destination", "window_start", "window_end", "origin",
        "comparability", "confidence", "freshness", "method_version",
        "assumption_set_id", "input_digest",
    }
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise ValueError(f"origin ranking row missing {sorted(missing)}")
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    columns = (
        "run_date", "destination", "window_start", "window_end", "origin", "rank",
        "landed_usd_mt", "fob_usd_mt", "origin_usd_mt", "comparability", "confidence",
        "freshness", "observation_date", "shipment_window", "incoterm", "quote_kind",
        "method_version", "assumption_set_id", "input_digest", "snapshot_json",
        "generated_at",
    )
    payload = [
        tuple(
            json.dumps(row[column], default=str)
            if column == "snapshot_json" and not isinstance(row.get(column), (str, type(None)))
            else (generated_at if column == "generated_at" else row.get(column))
            for column in columns
        )
        for row in rows
    ]
    with managed_connection(get_connection()) as conn:
        conn.executemany(
            f"INSERT OR REPLACE INTO origin_rankings ({','.join(columns)}) "  # noqa: S608 - fixed column list
            f"VALUES ({','.join('?' * len(columns))})",
            payload,
        )
        maybe_sync(conn)
    logger.info("Archived origin ranking: %d row(s)", len(payload))
    return len(payload)


def save_opportunity_detections(rows: list[dict[str, Any]]) -> int:
    """Archive one run's opportunity detections, one row per identity.

    Written for the same reason the origin ranking is — a detection is a
    function of that morning's database and of an assumption file that expires
    by design — plus one that is specific to this phase: the archive is what
    makes an opportunity **id stable**. The id is derived from the identity and
    the date it was FIRST seen, so without a persisted history a fresh CI
    database would mint a new id every morning and silently break the link to
    the trader's own workflow file.

    Nothing a person typed is accepted here. ``status``, ``owner``, ``notes``,
    contact dates and outcomes live only in the local workflow directory; a row
    carrying any of them is a programming error and is rejected rather than
    stored, because a private note in a git-committed table is a leak that no
    template check would catch.
    """
    if not rows:
        return 0
    required = {
        "run_date", "identity", "opportunity_id", "rule_id", "ladder", "product",
        "first_detected_on", "expires_on", "confidence", "method_version",
    }
    forbidden = {"status", "owner", "notes", "contacted_on", "next_action", "feedback", "audit"}
    for row in rows:
        missing = required - row.keys()
        if missing:
            raise ValueError(f"opportunity detection row missing {sorted(missing)}")
        leaked = forbidden & row.keys()
        if leaked:
            raise ValueError(
                f"opportunity detection row carries private workflow field(s) {sorted(leaked)} — "
                "the detection archive is public by construction and must never hold them"
            )
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    columns = (
        "run_date", "identity", "opportunity_id", "rule_id", "ladder", "product",
        "origin", "destination", "window_start", "first_detected_on", "expires_on",
        "confidence", "composite_score", "blocker_codes", "score_json", "snapshot_json",
        "method_version", "generated_at",
    )
    payload = [
        tuple(
            json.dumps(row[column], default=str)
            if column in ("score_json", "snapshot_json")
            and not isinstance(row.get(column), (str, type(None)))
            else (generated_at if column == "generated_at" else row.get(column))
            for column in columns
        )
        for row in rows
    ]
    with managed_connection(get_connection()) as conn:
        conn.executemany(
            f"INSERT OR REPLACE INTO opportunity_detections ({','.join(columns)}) "  # noqa: S608 - fixed column list
            f"VALUES ({','.join('?' * len(columns))})",
            payload,
        )
        maybe_sync(conn)
    logger.info("Archived opportunity detections: %d row(s)", len(payload))
    return len(payload)


# --- Freshness tracking (special-case: bespoke SQL) -------------------------


def save_freshness(
    layer_name: str,
    rows_fetched: int = 0,
    status: str = "success",
    keys_returned: int | None = None,
    keys_expected: int | None = None,
) -> None:
    """Record a freshness row. Only success stamps last_success; other states preserve it.

    'disabled' marks a layer intentionally short-circuited (e.g. an upstream
    anti-bot wall) — distinct from 'failed' so it doesn't read as an outage,
    and distinct from 'success' so it doesn't fabricate freshness.

    keys_returned/keys_expected describe how much of a multi-key layer came
    back (#182). They describe, they never grade: LAYER_MIN_KEYS remains the
    sole verdict. None (the default) records NULL, meaning "never learned" —
    a transport failure before any payload existed, or a layer for which
    partial coverage is not a meaningful state.
    """
    valid_statuses = {
        "success", "failed", "disabled", "no_publication", "stale", "incomplete"
    }
    if status not in valid_statuses:
        raise ValueError(
            f"status must be one of {sorted(valid_statuses)}, got {status!r}"
        )
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with managed_connection(get_connection()) as conn:
        prior_success: str | None
        if status == "success":
            prior_success = now
        else:
            row = conn.execute(
                "SELECT last_success FROM data_freshness WHERE layer_name = ?",
                (layer_name,),
            ).fetchone()
            prior_success = row[0] if row else None
        conn.execute(
            """INSERT OR REPLACE INTO data_freshness
               (layer_name, last_success, last_attempt, rows_fetched, status,
                keys_returned, keys_expected)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (layer_name, prior_success, now, rows_fetched, status,
             keys_returned, keys_expected),
        )
        maybe_sync(conn)
    logger.debug("Freshness recorded for %s at %s (status=%s, %d rows, keys=%s/%s)",
                 layer_name, now, status, rows_fetched, keys_returned, keys_expected)


def update_commodity_freshness():
    """Scan data tables, record per-commodity last_date + row count."""
    if not is_cloud() and not os.path.exists(DB_PATH):
        return
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    table_specs = [
        ("prices", "commodity", "Date"),
        ("cot", "commodity", "Date"),
        ("weather", "region", "Date"),
        ("currencies", "pair", "Date"),
        ("dce_futures", "commodity", "Date"),
        ("worldbank_prices", "commodity", "Date"),
        ("forward_curve", "commodity", "fetched_date"),
    ]
    with managed_connection(get_connection()) as conn:
        for table, key_col, date_col in table_specs:
            try:
                rows = conn.execute(
                    f"SELECT {key_col}, MAX({date_col}), COUNT(*) "
                    f"FROM {table} GROUP BY {key_col}"
                ).fetchall()
            except Exception:
                continue
            if rows:
                conn.executemany(
                    """INSERT OR REPLACE INTO commodity_freshness
                       (commodity, table_name, last_date_in_db, rows_total, checked_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    [(c, table, ld, ct, now) for c, ld, ct in rows],
                )
        maybe_sync(conn)
    logger.info("Commodity freshness updated at %s", now)
