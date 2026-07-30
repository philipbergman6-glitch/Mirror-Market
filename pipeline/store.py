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

from config import DB_PATH, STORAGE_DIR
from pipeline.connection import get_connection, is_cloud, maybe_sync
from pipeline.schema import ALL_SCHEMAS, UNIQUE_INDEXES

logger = logging.getLogger(__name__)


# --- Lifecycle --------------------------------------------------------------


def _ensure_storage_dir():
    os.makedirs(STORAGE_DIR, exist_ok=True)


def _migrate_data_freshness(conn) -> None:
    """Add status/last_attempt to data_freshness if absent. Idempotent."""
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(data_freshness)").fetchall()}
    except Exception:
        return
    for col, ddl in (
        ("status", "ALTER TABLE data_freshness ADD COLUMN status TEXT NOT NULL DEFAULT 'success'"),
        ("last_attempt", "ALTER TABLE data_freshness ADD COLUMN last_attempt TEXT"),
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


def init_database():
    """Create tables + unique indexes if missing. Idempotent."""
    _ensure_storage_dir()
    with get_connection() as conn:
        for ddl in ALL_SCHEMAS:
            conn.execute(ddl)
        _migrate_usda_pk(conn)
        _migrate_export_sales_unit(conn)
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
        "eia_energy", "brazil_estimates", "data_freshness",
        "commodity_freshness", "india_domestic_prices",
        "brazil_spot_prices", "safex_prices", "briefings",
    ]
    with get_connection() as conn:
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
    with get_connection() as conn:
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
        with get_connection() as conn:
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
    _save("weather", df[["region", "Date", "temp_max", "temp_min", "precipitation"]],
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
    """Write forward curve → 'forward_curve'. Stamps fetched_date with today UTC."""
    if df.empty:
        return
    df = df.copy()
    df["commodity"] = commodity
    df["fetched_date"] = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df = _str_cols(df, "contract_month", "label", "ticker")
    _save(
        "forward_curve",
        df[["commodity", "contract_month", "label", "ticker", "close", "fetched_date"]],
        ["commodity", "contract_month"],
        f"forward_curve/{commodity}",
    )


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


def save_gulf_bids(df: pd.DataFrame):
    """Write AMS CIF Gulf export bids → 'gulf_bids'."""
    if df.empty:
        return
    df = df.copy()
    if "report_date" in df.columns:
        df["report_date"] = _date(df["report_date"])
    df = _str_cols(df, "commodity", "location", "delivery", "sale_type",
                   "basis_change", "freight")
    _save(
        "gulf_bids",
        df[[
            "report_date", "commodity", "location", "delivery", "sale_type",
            "basis_low", "basis_high", "futures_month", "basis_change",
            "price_low", "price_high", "average", "year_ago", "freight",
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
    """Write NCDEX India domestic (INR/MT) → 'india_domestic_prices'."""
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
    _save("safex_prices", df[["Date", "commodity", "Close", "Volume", "unit"]],
          ["Date", "commodity"], f"safex/{commodity}")


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
    with get_connection() as conn:
        conn.execute(
            """INSERT OR REPLACE INTO briefings
               (briefing_date, text, signals_json, snapshot_json, generated_at)
               VALUES (?, ?, ?, ?, ?)""",
            (briefing_date, text, signals_json, snapshot_json, generated_at),
        )
        maybe_sync(conn)
    logger.info("Archived briefing for %s (%d signals)", briefing_date, len(signals or []))


# --- Freshness tracking (special-case: bespoke SQL) -------------------------


def save_freshness(layer_name: str, rows_fetched: int = 0, status: str = "success") -> None:
    """Record a freshness row. Success stamps last_success; failed preserves it."""
    if status not in ("success", "failed"):
        raise ValueError(f"status must be 'success' or 'failed', got {status!r}")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with get_connection() as conn:
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
               (layer_name, last_success, last_attempt, rows_fetched, status)
               VALUES (?, ?, ?, ?, ?)""",
            (layer_name, prior_success, now, rows_fetched, status),
        )
        maybe_sync(conn)
    logger.debug("Freshness recorded for %s at %s (status=%s, %d rows)",
                 layer_name, now, status, rows_fetched)


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
    with get_connection() as conn:
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
