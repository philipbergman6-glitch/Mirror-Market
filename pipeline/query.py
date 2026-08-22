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
from pipeline.connection import get_connection, is_cloud, managed_connection

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
    with managed_connection(get_connection()) as conn:
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


def read_river_levels(gauge: str | None = None) -> pd.DataFrame:
    """Read river stage from SQLite (Layers 27/28).

    A level, not a price: `stage` is feet on the Mississippi and metres on the
    Paraná, and `unit` says which on every row. Rows flagged
    ``is_forecast = 1`` are NWPS model output dated ahead of today — filter
    them out before treating the newest row as an observation.
    """
    return _read_table("river_levels", "gauge", gauge, date_cols=("Date",))


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


def read_ec_oilseed_prices(series: str | None = None) -> pd.DataFrame:
    """Read EC weekly world oilseed prices from SQLite.

    Rows carry `cadence` and `quote_kind` — this is a weekly physical FOB
    assessment, not a daily board price, and must not be plotted on a daily
    axis as if a flat week were a flat market.
    """
    return _read_table("ec_oilseed_prices", "series", series, date_cols=("Date",))


def read_ocean_freight_rates(route: str | None = None) -> pd.DataFrame:
    """Read GTR monthly bulk grain ocean freight rates from SQLite.

    A benchmark route (Japan) assessed by a broker and republished by USDA —
    every row carries the attribution that says so. It is a read on the level
    of freight, never a quote for a cargo, and must not be substituted for a
    route-specific rate in a landed-cost stack.
    """
    return _read_table("ocean_freight_rates", "route", route, date_cols=("Date",))


def read_port_vessel_activity(port_region: str | None = None) -> pd.DataFrame:
    """Read GTR weekly grain vessel activity by US port region from SQLite.

    Counts of vessels, not tonnes: in port, loaded in the last 7 days, due in
    the next 10.
    """
    return _read_table(
        "port_vessel_activity", "port_region", port_region,
        date_cols=("week_ending",),
    )


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
    """Read JSE SAFEX South Africa last-traded prices from SQLite.

    Not settlement/MTM — the free source carries no settlement column (#157).
    """
    return _read_table("safex_prices", "commodity", commodity)


def read_sagis_deliveries(commodity: str | None = None) -> pd.DataFrame:
    """Read SAGIS South Africa weekly producer deliveries (MT) from SQLite.

    `week_end` is parsed to datetime; `season_year` is the *start* year of
    the March–February marketing season. Rows are physical flow, not price.
    Any surface rendering these must carry `config.SAGIS_ATTRIBUTION`.
    """
    return _read_table(
        "sagis_deliveries", "commodity", commodity, date_cols=("week_end",)
    )


def read_sagis_supply_demand(commodity: str | None = None) -> pd.DataFrame:
    """Read SAGIS South Africa monthly soybean supply & demand (MT) from SQLite.

    `month_end` and `report_month` are parsed to datetime; `season_year` is
    the *start* year of the March–February marketing season and
    `month_number` its position in that season (1 = March). Rows are physical
    flow and stock, not price — `processed_oil_oilcake` is South Africa's
    crush volume, not a margin. Any surface rendering these must carry
    `config.SAGIS_ATTRIBUTION`.
    """
    return _read_table(
        "sagis_supply_demand", "commodity", commodity,
        date_cols=("month_end", "report_month"),
    )


def read_cec_estimates(commodity: str | None = None) -> pd.DataFrame:
    """Read CEC South Africa official crop estimates from SQLite.

    A revision series: one row per release per season, so a caller wanting
    "the current estimate" takes the newest `release_date` for the season.
    Yield is not stored — it is `production_t / area_ha` where both are
    present. Any surface rendering these must carry `config.CEC_ATTRIBUTION`.
    """
    return _read_table(
        "cec_estimates", "commodity", commodity, date_cols=("release_date",)
    )


def read_origin_rankings(destination: str | None = None) -> pd.DataFrame:
    """Read archived origin comparisons.

    A ``rank`` of NULL is meaningful: the row was rendered that day but was not
    comparable (wrong shipment window, missing freight assumption, stale
    quote). Callers wanting the published ordering filter on ``rank`` being
    present; callers asking "was this origin offering at all" must not.
    """
    return _read_table(
        "origin_rankings",
        "destination",
        destination,
        date_cols=("run_date", "window_start", "window_end", "observation_date"),
    )


def read_opportunity_detections(rule_id: str | None = None) -> pd.DataFrame:
    """Read archived opportunity detections (Phase 4).

    One row per identity per run. ``identity`` — not ``opportunity_id`` — is the
    stable join key: the id is derived from the identity plus its first-seen
    date, so grouping by identity is what recovers "when did we first see this".

    The table holds no trader notes, owner, status or outcome by construction;
    those live only in the local workflow directory.
    """
    return _read_table(
        "opportunity_detections",
        "rule_id",
        rule_id,
        date_cols=("run_date", "first_detected_on", "expires_on", "window_start"),
    )


def read_freshness() -> pd.DataFrame:
    """
    Read data freshness timestamps for all layers.

    Returns
    -------
    pd.DataFrame
        Columns: layer_name, last_success, last_attempt, rows_fetched, status,
        keys_returned, keys_expected, observed_at, fetch_started_at,
        fetch_completed_at, stored_at
        (last_attempt and status default to NaN/'success' for legacy rows;
        the key-coverage columns are NULL wherever coverage is undefined or
        was never learned — see save_freshness. The four latency stamps are
        NULL on every row written before the instrumentation existed, which
        `latency.measure` reports as an unmeasured chain rather than a fast
        one.)
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    with managed_connection(get_connection()) as conn:
        try:
            df = pd.read_sql("SELECT * FROM data_freshness", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for data_freshness: %s", exc)
            return pd.DataFrame()

    for col in (
        "last_success", "last_attempt", "observed_at",
        "fetch_started_at", "fetch_completed_at", "stored_at",
    ):
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
    with managed_connection(get_connection()) as conn:
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
    with managed_connection(get_connection()) as conn:
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

    with managed_connection(get_connection()) as conn:
        try:
            df = pd.read_sql("SELECT * FROM commodity_freshness", conn)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for commodity_freshness: %s", exc)
            return pd.DataFrame()

    return df


def read_quarantined_revisions(table_name: str | None = None) -> pd.DataFrame:
    """Read held-back revisions from the quarantine (T19 · F9, #67).

    An inspection seam, not a display source. Nothing in `analysis/` or
    `scripts/generate_site.py` may read this: a quarantined value is a
    number the store layer refused to believe, and rendering one anywhere
    would publish exactly what the guard exists to keep out of the data.
    It answers "what did the guard hold back, and against what" after an
    alert.
    """
    if not is_cloud() and not os.path.exists(DB_PATH):
        return pd.DataFrame()

    sql = "SELECT * FROM quarantined_revisions"
    params: tuple = ()
    if table_name is not None:
        sql += " WHERE table_name = ?"
        params = (table_name,)
    sql += " ORDER BY detected_at DESC, table_name, row_key"

    with managed_connection(get_connection()) as conn:
        try:
            df = pd.read_sql(sql, conn, params=params or None)
        except (sqlite3.OperationalError, pd.errors.DatabaseError) as exc:
            logger.warning("Read failed for quarantined_revisions: %s", exc)
            return pd.DataFrame()

    return df
