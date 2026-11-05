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

import config
from pipeline import schema

# table -> (filter column, date column)
ALLOWED: dict[str, tuple[str | None, str]] = {
    "prices": ("commodity", "Date"),
    "economic": ("series_name", "Date"),
    "usda": ('stat_category', 'year'),
    "cot": ("commodity", "Date"),
    "weather": ("region", "Date"),
    "river_levels": ("gauge", "Date"),
    "crop_progress": ('commodity', 'week_ending'),
    "psd": ('commodity', 'year'),
    "currencies": ("pair", "Date"),
    "worldbank_prices": ("commodity", "Date"),
    "ec_oilseed_prices": ("series", "Date"),
    "brazil_exports": ('product', 'month_end'),
    "sea_india_rates": ("series", "Date"),
    "sopa_crop_estimates": ("crop_year", "crop_year"),
    "cyclone_source_status": ('source', 'Date'),
    "cyclone_storms": ('source', 'issued_at'),
    "cyclone_track_points": ('source', 'issued_at'),
    "ocean_freight_rates": ("route", "Date"),
    "port_vessel_activity": ('port_region', 'week_ending'),
    "export_sales": ('commodity', 'week_ending'),
    "forward_curve": ('commodity', 'observation_date'),
    "contract_bars": ("commodity", "Date"),
    "dce_futures": ("commodity", "Date"),
    "wasde": ('commodity', 'year'),
    "inspections": ('commodity', 'week_ending'),
    "inspection_destinations": ('commodity', 'week_ending'),
    "argentina_fob": ('product', 'date'),
    "inspection_port_flows": ('commodity', 'week_ending'),
    "gulf_bids": ("commodity", "report_date"),
    "us_processor_cash": ('commodity', 'week_end'),
    "eia_energy": ("series_name", "Date"),
    "rin_generation": ("d_code", "Date"),
    "rin_prices": ("d_code", "Date"),
    "rfs_rvo": (None, "compliance_year"),
    "brazil_estimates": ('commodity', 'report_date'),
    "india_domestic_prices": ("commodity", "Date"),
    "brazil_spot_prices": ("commodity", "Date"),
    "safex_prices": ("commodity", "Date"),
    "sagis_deliveries": ('commodity', 'week_end'),
    "sagis_supply_demand": ('commodity', 'month_end'),
    "cec_estimates": ('commodity', 'release_date'),
    "origin_rankings": ("destination", "run_date"),
    "opportunity_detections": ('rule_id', 'run_date'),
    "data_freshness": ("layer_name", "last_attempt"),
    "commodity_freshness": ('commodity', 'checked_at'),
    "briefings": (None, "briefing_date"),
}

# Read by pipeline.query but deliberately not served.
EXCLUDED: frozenset[str] = frozenset({
    "quarantined_revisions",  # inspection seam — "never rendered" (query.py)
})

# Tables whose rows belong to a private layer (config.PRIVATE_LAYERS).
PRIVATE_LAYER_TABLES: dict[str, str] = {
    "sea_india": "sea_india_rates",
    "sopa_crop": "sopa_crop_estimates",
}

MAX_LIMIT = 1000
DEFAULT_LIMIT = 250


class PrivacyRefusal(RuntimeError):
    """The database holds something this server must never serve."""


def private_tables() -> frozenset[str]:
    return frozenset(PRIVATE_LAYER_TABLES[layer] for layer in config.PRIVATE_LAYERS if layer in PRIVATE_LAYER_TABLES)


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
    leaked = sorted(present & private_tables())
    if leaked:
        raise PrivacyRefusal(f"private-layer tables present: {leaked} (config.PRIVATE_LAYERS={config.PRIVATE_LAYERS})")


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
    if table not in servable_tables():
        raise ValueError(f"table {table!r} is not served; allowed: {', '.join(sorted(servable_tables()))}")
    if not 1 <= limit <= MAX_LIMIT:
        raise ValueError(f"limit must be 1..{MAX_LIMIT}, got {limit}")
    filter_col, date_col = ALLOWED[table]
    clauses: list[str] = []
    params: list[object] = []
    if key is not None:
        if filter_col is None:
            raise ValueError(f"table {table!r} takes no key")
        clauses.append(f'"{filter_col}" = ?')
        params.append(key)
    if start is not None:
        clauses.append(f'"{date_col}" >= ?')
        params.append(start)
    if end is not None:
        clauses.append(f'"{date_col}" <= ?')
        params.append(end)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    sql = f'SELECT * FROM "{table}"{where} ORDER BY "{date_col}" DESC LIMIT ?'  # noqa: S608 — table from ALLOWED
    params.append(limit)
    return [dict(r) for r in conn.execute(sql, params)]


def latest_session(conn: sqlite3.Connection) -> str | None:
    row = conn.execute('SELECT MAX("Date") FROM prices').fetchone()
    return row[0] if row and row[0] else None
