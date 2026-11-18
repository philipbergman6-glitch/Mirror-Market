"""Read-only access to the edition database.

The pipeline's ``pipeline.query`` binds ``config.DB_PATH`` at import, opens
read-write, and does unbounded ``SELECT *`` — fine for a one-shot site build,
wrong for a server. This module opens its own ``mode=ro`` connection and reads
only an allow-list of tables, bounded.

The allow-list mirrors the tables ``pipeline.query``'s ``read_*`` functions
expose, minus the quarantine inspection seam (its docstring: never rendered)
and minus any table owned by a private layer. A parity test holds the two
together so a new read function forces a decision here.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import NamedTuple

import config
from pipeline import schema


class TableSpec(NamedTuple):
    key_column: str | None  # the one equality filter the table takes, if any
    date_column: str  # what "newest first" and a start/end range mean here


ALLOWED: dict[str, TableSpec] = {
    "prices": TableSpec("commodity", "Date"),
    "economic": TableSpec("series_name", "Date"),
    "usda": TableSpec("stat_category", "year"),
    "cot": TableSpec("commodity", "Date"),
    "weather": TableSpec("region", "Date"),
    "river_levels": TableSpec("gauge", "Date"),
    "crop_progress": TableSpec("commodity", "week_ending"),
    "psd": TableSpec("commodity", "year"),
    "currencies": TableSpec("pair", "Date"),
    "worldbank_prices": TableSpec("commodity", "Date"),
    "ec_oilseed_prices": TableSpec("series", "Date"),
    "brazil_exports": TableSpec("product", "month_end"),
    "sea_india_rates": TableSpec("series", "Date"),
    "sopa_crop_estimates": TableSpec("crop_year", "crop_year"),
    "cyclone_source_status": TableSpec("source", "Date"),
    "cyclone_storms": TableSpec("source", "issued_at"),
    "cyclone_track_points": TableSpec("source", "issued_at"),
    "ocean_freight_rates": TableSpec("route", "Date"),
    "port_vessel_activity": TableSpec("port_region", "week_ending"),
    "export_sales": TableSpec("commodity", "week_ending"),
    "forward_curve": TableSpec("commodity", "observation_date"),
    "contract_bars": TableSpec("commodity", "Date"),
    "dce_futures": TableSpec("commodity", "Date"),
    "wasde": TableSpec("commodity", "year"),
    "inspections": TableSpec("commodity", "week_ending"),
    "inspection_destinations": TableSpec("commodity", "week_ending"),
    "argentina_fob": TableSpec("product", "date"),
    "inspection_port_flows": TableSpec("commodity", "week_ending"),
    "gulf_bids": TableSpec("commodity", "report_date"),
    "us_processor_cash": TableSpec("commodity", "week_end"),
    "eia_energy": TableSpec("series_name", "Date"),
    "rin_generation": TableSpec("d_code", "Date"),
    "rin_prices": TableSpec("d_code", "Date"),
    "rfs_rvo": TableSpec(None, "compliance_year"),
    "brazil_estimates": TableSpec("commodity", "report_date"),
    "india_domestic_prices": TableSpec("commodity", "Date"),
    "brazil_spot_prices": TableSpec("commodity", "Date"),
    "safex_prices": TableSpec("commodity", "Date"),
    # Layer 37, private: listed for parity with pipeline.query and subtracted
    # from servable_tables() by PRIVATE_LAYER_TABLES below.
    "matif_futures": TableSpec("contract_month", "Date"),
    "nigeria_spot_prices": TableSpec("commodity", "Date"),
    "sagis_deliveries": TableSpec("commodity", "week_end"),
    "sagis_supply_demand": TableSpec("commodity", "month_end"),
    "cec_estimates": TableSpec("commodity", "release_date"),
    "origin_rankings": TableSpec("destination", "run_date"),
    "opportunity_detections": TableSpec("rule_id", "run_date"),
    "data_freshness": TableSpec("layer_name", "last_attempt"),
    "commodity_freshness": TableSpec("commodity", "checked_at"),
    "briefings": TableSpec(None, "briefing_date"),
}

# Read by pipeline.query but deliberately not served.
EXCLUDED: frozenset[str] = frozenset({
    "quarantined_revisions",  # inspection seam — "never rendered" (query.py)
})

# Tables whose rows belong to a layer that can be declared private
# (config.PRIVATE_LAYERS). Every private layer must appear here: an unmapped
# one is a startup error, never a served table.
PRIVATE_LAYER_TABLES: dict[str, str] = {
    "sea_india": "sea_india_rates",
    "sopa_crop": "sopa_crop_estimates",
    "matif": "matif_futures",
}

MAX_LIMIT = 1000
DEFAULT_LIMIT = 250


class PrivacyRefusal(RuntimeError):
    """The database holds something this server must never serve."""


def private_tables() -> frozenset[str]:
    unmapped = sorted(layer for layer in config.PRIVATE_LAYERS if layer not in PRIVATE_LAYER_TABLES)
    if unmapped:
        raise PrivacyRefusal(
            f"private layers with no table mapping in mirror_mcp.db.PRIVATE_LAYER_TABLES: {unmapped}"
        )
    return frozenset(PRIVATE_LAYER_TABLES[layer] for layer in config.PRIVATE_LAYERS)


def servable_tables() -> frozenset[str]:
    return frozenset(ALLOWED) - private_tables()


def roster_tables() -> frozenset[str]:
    """Every table the pipeline's schema creates — the only tables a CI DB may hold."""
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    conn.close()
    return frozenset(names)


def readable_tables() -> frozenset[str]:
    """What any tool may touch at all: the roster minus private tables minus the excluded seams.

    Wider than ``servable_tables`` because the site's block builders read
    tables that have no ``series`` entry (hazard flags, named-month bars).
    """
    return roster_tables() - private_tables() - EXCLUDED


def open_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def assert_public_roster(conn: sqlite3.Connection) -> None:
    """Invariant 4 at startup: refuse a DB with a table outside the roster or owned by a private layer."""
    present = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    }
    unknown = sorted(present - roster_tables())
    if unknown:
        raise PrivacyRefusal(f"tables outside the schema roster: {unknown}")
    # The schema creates every table in every database, so a private layer's
    # *empty* table is DDL, not a leak; a row in it is Euronext data on a
    # machine that serves third parties, and the server does not start.
    leaked = sorted(
        table for table in present & private_tables()
        if conn.execute(f'SELECT 1 FROM "{table}" LIMIT 1').fetchone() is not None  # noqa: S608 — roster
    )
    if leaked:
        raise PrivacyRefusal(
            f"private-layer tables hold rows: {leaked} (config.PRIVATE_LAYERS={config.PRIVATE_LAYERS})"
        )


def _check_table_and_limit(table: str, limit: int) -> TableSpec:
    if table not in servable_tables():
        raise ValueError(f"table {table!r} is not served; allowed: {', '.join(sorted(servable_tables()))}")
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be 1..{MAX_LIMIT}, got {limit}")
    return ALLOWED[table]


def bounded_rows(
    conn: sqlite3.Connection,
    table: str,
    *,
    key: str | None = None,
    start: str | None = None,
    end: str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> list[dict]:
    """Newest-first rows from one allow-listed table, never more than MAX_LIMIT."""
    spec = _check_table_and_limit(table, limit)
    clauses: list[str] = []
    params: list[object] = []
    if key is not None:
        if spec.key_column is None:
            raise ValueError(f"table {table!r} takes no key")
        clauses.append(f'"{spec.key_column}" = ?')
        params.append(key)
    if start is not None:
        clauses.append(f'"{spec.date_column}" >= ?')
        params.append(start)
    if end is not None:
        clauses.append(f'"{spec.date_column}" <= ?')
        params.append(end)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f'SELECT * FROM "{table}"{where} ORDER BY "{spec.date_column}" DESC LIMIT ?'  # noqa: S608 — from ALLOWED
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params)]


def river_rows(conn: sqlite3.Connection, *, gauge: str | None, per_gauge: int) -> list[dict]:
    """Observed stages only (forecast rows excluded in SQL, before any limit), newest first, ``per_gauge`` each."""
    _check_table_and_limit("river_levels", per_gauge)
    sql = (
        'SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "gauge" ORDER BY "Date" DESC) AS rn '
        'FROM "river_levels" WHERE "is_forecast" = 0'
        + (' AND "gauge" = ?' if gauge is not None else "")
        + ') WHERE rn <= ? ORDER BY "gauge", "Date" DESC'
    )
    params: list[object] = [gauge, per_gauge] if gauge is not None else [per_gauge]
    return [{k: v for k, v in dict(r).items() if k != "rn"} for r in conn.execute(sql, params)]


def latest_session(conn: sqlite3.Connection) -> str | None:
    row = conn.execute('SELECT MAX("Date") FROM prices').fetchone()
    return row[0] if row and row[0] else None
