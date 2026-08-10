"""Git-based history persistence for snapshot-only pipeline tables.

CI runs on an ephemeral runner with an empty SQLite DB each day. Most
layers re-download full history and self-heal, but a few sources only
publish "today's" values — without persistence their history is lost on
every run. Instead of a cloud DB (Turso was evaluated and rejected —
account friction, extra secrets, network dependency for a few KB/day),
these tables round-trip through CSV files committed to the repo:

    import_history()  — pipeline start: seed the empty DB from CSVs
    export_history()  — pipeline end:   dump the tables back to CSVs

The workflow commits data/history/ back to the repo after each run.

Import uses INSERT OR IGNORE so rows already in the DB (a local dev DB is
always fresher than the last committed CSV) are never clobbered; the
fetch layers that follow use INSERT OR REPLACE and overwrite with live
values where they overlap. Export rewrites each CSV atomically, sorted by
primary key, so the daily git diff is stable and append-only.

A failed import must abort the run: exporting from a DB that failed to
seed would overwrite the committed CSVs with today-only data — the exact
history loss this module exists to prevent.
"""

import csv
import logging
import os

from config import HISTORY_DIR
from pipeline.connection import get_connection

logger = logging.getLogger(__name__)

# Table → primary-key columns (export sort order). Only snapshot-only
# tables belong here: sources whose upstream does not serve history, so
# the committed CSV is the only record. Self-healing layers (prices,
# weather, COT, PSD, ...) re-download their own history and stay out.
HISTORY_TABLES: dict[str, tuple[str, ...]] = {
    # AgRural Paranaguá FOB (1 row/day, the Brazil basis source) + CEPEA
    # via Notícias Agrícolas (~10 sessions deep — only recent self-heals).
    "brazil_spot_prices": ("Date", "commodity"),
    # Grain SA settlement page carries only the current session.
    "safex_prices": ("Date", "commodity"),
    # data.gov.in mandi resource is a current-day snapshot — the arrival_date
    # filter is ignored upstream, so history exists only where we keep it.
    "india_domestic_prices": ("Date", "commodity"),
    # One full curve per fetched_date — term-structure history.
    "forward_curve": ("fetched_date", "commodity", "contract_month"),
    # WA_GR101 report carries only the trailing three weeks.
    "inspections": ("commodity", "week_ending"),
    "inspection_port_flows": ("week_ending", "region", "port_area", "commodity"),
    "inspection_destinations": ("week_ending", "region", "country", "commodity"),
    "argentina_fob": ("date", "position", "ship_from"),
    # CONAB survey values are stamped with the fetch date; prior surveys
    # are unrecoverable from the source file.
    "brazil_estimates": ("source", "commodity", "crop_year", "attribute", "report_date"),
    # AMS 3147 publishes a daily PDF; only the current report is served.
    "gulf_bids": ("report_date", "commodity", "location", "delivery"),
    # WASDE self-heals 12 months; the CSV preserves revisions beyond that.
    "wasde": ("commodity", "year", "attribute", "reference_period"),
    # The briefing is generated from that run's DB and stored by
    # analysis/briefing/orchestrator.py. Nothing upstream serves it: a past
    # day's text + snapshot_json cannot be reconstructed from any source, so
    # without this the CI runner writes one every day and deletes it.
    "briefings": ("briefing_date",),
    # ESR is fetched for the *current* marketing year only
    # (fetchers/export_sales._current_market_year), so the prior year is never
    # re-requested. At each MY rollover (Sep 1 for soybeans) the outgoing
    # year's weekly series would vanish from an ephemeral DB.
    "export_sales": ("commodity", "week_ending", "country"),
}


class HistoryImportError(RuntimeError):
    """A history CSV exists but could not be loaded into its table."""


def _csv_path(table: str) -> str:
    return os.path.join(HISTORY_DIR, f"{table}.csv")


def import_history() -> int:
    """Seed the DB from committed history CSVs. Returns rows inserted.

    Missing directory or missing per-table files are normal (first run,
    or a table that has never exported). A present-but-unloadable CSV
    raises HistoryImportError — the caller must abort before export runs.
    """
    if not os.path.isdir(HISTORY_DIR):
        logger.info("History import: %s not present — nothing to seed", HISTORY_DIR)
        return 0

    total = 0
    with get_connection() as conn:
        for table in HISTORY_TABLES:
            path = _csv_path(table)
            if not os.path.exists(path):
                continue
            try:
                with open(path, newline="", encoding="utf-8") as fh:
                    reader = csv.reader(fh)
                    header = next(reader, None)
                    if not header:
                        logger.warning("History import: %s is empty — skipped", path)
                        continue
                    table_cols = {
                        r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()
                    }
                    unknown = set(header) - table_cols
                    if unknown:
                        raise HistoryImportError(
                            f"{path}: columns {sorted(unknown)} not in table {table}"
                        )
                    sql = (
                        f"INSERT OR IGNORE INTO {table} ({','.join(header)}) "
                        f"VALUES ({','.join('?' * len(header))})"
                    )
                    rows = [[cell if cell != "" else None for cell in row] for row in reader]
                conn.execute("BEGIN")
                try:
                    conn.executemany(sql, rows)
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                total += len(rows)
                logger.info("History import: %s ← %d rows", table, len(rows))
            except HistoryImportError:
                raise
            except Exception as exc:
                raise HistoryImportError(f"{path}: {exc}") from exc
    logger.info("History import complete: %d rows across %d tables", total, len(HISTORY_TABLES))
    return total


def _csv_shape(path: str) -> tuple[int, list[str]]:
    """(data rows excluding header, header columns) of an existing CSV.

    Returns (0, []) when the file is absent — a first export has nothing
    to regress against.
    """
    if not os.path.exists(path):
        return 0, []
    with open(path, newline="", encoding="utf-8") as fh:
        reader = csv.reader(fh)
        header = next(reader, None) or []
        return sum(1 for _ in reader), header


def export_history() -> int:
    """Dump every history table to data/history/<table>.csv. Returns rows.

    Each file is written to a temp path and atomically renamed so a
    mid-write crash never leaves a truncated CSV for git to commit.
    Empty tables are skipped (never overwrite a populated CSV with an
    empty file — a table can be empty because its fetch layer failed).

    A *shrinking* export is refused for the same reason. The empty guard
    alone is not enough: a local DB that is merely behind the committed
    CSVs (import_history() not run first, or a layer that fetched fewer
    rows than last time) would otherwise silently replace good history
    with a shorter file. Set MIRROR_HISTORY_ALLOW_SHRINK=1 to override
    when a shrink is genuinely intended (a deliberate prune or a schema
    change that drops rows).
    """
    allow_shrink = os.getenv("MIRROR_HISTORY_ALLOW_SHRINK", "").strip() == "1"
    os.makedirs(HISTORY_DIR, exist_ok=True)
    total = 0
    with get_connection() as conn:
        for table, pk_cols in HISTORY_TABLES.items():
            cursor = conn.execute(
                f"SELECT * FROM {table} ORDER BY {', '.join(pk_cols)}"
            )
            rows = cursor.fetchall()
            if not rows:
                logger.warning("History export: %s is empty — CSV left untouched", table)
                continue
            header = [d[0] for d in cursor.description]
            path = _csv_path(table)
            existing_rows, existing_header = _csv_shape(path)
            if len(rows) < existing_rows and not allow_shrink:
                logger.error(
                    "History export: %s would shrink %d → %d rows — CSV left "
                    "untouched. Run import_history() first, or set "
                    "MIRROR_HISTORY_ALLOW_SHRINK=1 if the shrink is intended.",
                    table,
                    existing_rows,
                    len(rows),
                )
                continue
            # A DB older than the schema (CREATE TABLE IF NOT EXISTS never
            # adds columns to an existing table) exports fewer columns and
            # would drop a whole field from the committed history. Row count
            # is unchanged in that case, so it needs its own guard.
            dropped = [c for c in existing_header if c not in header]
            if dropped and not allow_shrink:
                logger.error(
                    "History export: %s would drop column(s) %s — CSV left "
                    "untouched. The local DB predates the current schema; "
                    "recreate it, or set MIRROR_HISTORY_ALLOW_SHRINK=1 if the "
                    "columns were removed on purpose.",
                    table,
                    dropped,
                )
                continue
            tmp_path = path + ".tmp"
            with open(tmp_path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(header)
                writer.writerows(rows)
            os.replace(tmp_path, path)
            total += len(rows)
            logger.info("History export: %s → %d rows", table, len(rows))
    logger.info("History export complete: %d rows across %d tables", total, len(HISTORY_TABLES))
    return total
