"""Tests for pipeline.history (git-based CSV persistence) and the
forward_curve history schema (fetched_date in the PK, latest-snapshot
reads, old-PK migration).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from pipeline import history, query, store
from pipeline.history import HistoryImportError, export_history, import_history


@pytest.fixture
def history_env(patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """patched_db plus pipeline.history wired to a temp data/history dir."""
    history_dir = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(history_dir))
    monkeypatch.setattr(
        "pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db))
    )
    return history_dir


def _insert_spot(db_path: Path, date: str, price: float) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT OR REPLACE INTO brazil_spot_prices (Date, commodity, price_brl, unit) "
        "VALUES (?, 'Soybean (AgRural Paranaguá FOB)', ?, 'BRL/MT')",
        (date, price),
    )
    conn.commit()
    conn.close()


def _spot_rows(db_path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(
            "SELECT Date, price_brl FROM brazil_spot_prices ORDER BY Date"
        ).fetchall()
    finally:
        conn.close()


def test_import_missing_dir_is_noop(history_env: Path) -> None:
    assert import_history() == 0


def test_export_import_roundtrip(history_env: Path, patched_db: Path) -> None:
    _insert_spot(patched_db, "2026-07-29", 2500.0)
    _insert_spot(patched_db, "2026-07-30", 2510.0)
    assert export_history() == 2
    assert (history_env / "brazil_spot_prices.csv").exists()

    # Simulate the ephemeral CI runner: wipe the table, re-seed from CSV.
    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM brazil_spot_prices")
    conn.commit()
    conn.close()

    assert import_history() == 2
    assert _spot_rows(patched_db) == [("2026-07-29", 2500.0), ("2026-07-30", 2510.0)]


def test_import_never_clobbers_db_rows(history_env: Path, patched_db: Path) -> None:
    """Insert-if-absent: a fresher DB row wins over the committed CSV."""
    _insert_spot(patched_db, "2026-07-30", 2510.0)
    export_history()
    _insert_spot(patched_db, "2026-07-30", 9999.0)  # corrected value in DB
    import_history()
    assert _spot_rows(patched_db) == [("2026-07-30", 9999.0)]


def test_export_empty_table_leaves_csv_untouched(
    history_env: Path, patched_db: Path
) -> None:
    """A failed fetch layer (empty table) must not wipe committed history."""
    _insert_spot(patched_db, "2026-07-30", 2510.0)
    export_history()
    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM brazil_spot_prices")
    conn.commit()
    conn.close()
    export_history()
    csv_text = (history_env / "brazil_spot_prices.csv").read_text()
    assert "2026-07-30" in csv_text


def test_import_unknown_column_hard_fails(history_env: Path, patched_db: Path) -> None:
    history_env.mkdir()
    (history_env / "safex_prices.csv").write_text("Date,commodity,not_a_column\nx,y,z\n")
    with pytest.raises(HistoryImportError):
        import_history()


def test_india_mandi_roundtrips_through_history(
    history_env: Path, patched_db: Path
) -> None:
    """India is snapshot-only and MUST stay in the round-trip set.

    The data.gov.in resource serves the current day only — its
    arrival_date filter is ignored upstream — so the committed CSV is the
    single record of India's history. Drop this table from HISTORY_TABLES
    and every CI fetch is discarded at the end of the run, leaving India
    permanently unable to build a series (#155).
    """
    assert "india_domestic_prices" in history.HISTORY_TABLES

    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT OR REPLACE INTO india_domestic_prices "
        "(Date, commodity, Close, Unit) VALUES (?, ?, ?, ?)",
        ("2026-08-10", "Soybean (Mandi MP)", 66_875.0, "INR/MT"),
    )
    conn.commit()
    conn.close()

    export_history()
    assert (history_env / "india_domestic_prices.csv").exists()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM india_domestic_prices")
    conn.commit()
    conn.close()

    import_history()
    conn = sqlite3.connect(str(patched_db))
    rows = conn.execute(
        "SELECT Date, commodity, Close FROM india_domestic_prices"
    ).fetchall()
    conn.close()
    assert rows == [("2026-08-10", "Soybean (Mandi MP)", 66_875.0)]


def test_history_tables_all_exist(history_env: Path, patched_db: Path) -> None:
    """Every table in HISTORY_TABLES must exist in the schema (guards typos)."""
    conn = sqlite3.connect(str(patched_db))
    existing = {
        r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    }
    conn.close()
    missing = set(history.HISTORY_TABLES) - existing
    # gulf_bids/wasde etc. are created by conftest's schema list; anything
    # missing there is a conftest gap, not a history bug — but PK columns
    # must match the real schema either way.
    assert not missing, f"HISTORY_TABLES references unknown tables: {missing}"


def test_export_shrinking_table_leaves_csv_untouched(
    history_env: Path, patched_db: Path
) -> None:
    """A DB behind the committed CSV must not truncate it.

    The empty-table guard misses this: a local run without import_history()
    first has *some* rows, just fewer than the CSV.
    """
    _insert_spot(patched_db, "2026-07-29", 2500.0)
    _insert_spot(patched_db, "2026-07-30", 2510.0)
    export_history()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM brazil_spot_prices WHERE Date = '2026-07-29'")
    conn.commit()
    conn.close()

    export_history()
    csv_text = (history_env / "brazil_spot_prices.csv").read_text()
    assert "2026-07-29" in csv_text, "shrinking export overwrote committed history"


def test_export_shrink_allowed_with_override(
    history_env: Path, patched_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MIRROR_HISTORY_ALLOW_SHRINK=1 permits a deliberate prune."""
    _insert_spot(patched_db, "2026-07-29", 2500.0)
    _insert_spot(patched_db, "2026-07-30", 2510.0)
    export_history()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM brazil_spot_prices WHERE Date = '2026-07-29'")
    conn.commit()
    conn.close()

    monkeypatch.setenv("MIRROR_HISTORY_ALLOW_SHRINK", "1")
    export_history()
    csv_text = (history_env / "brazil_spot_prices.csv").read_text()
    assert "2026-07-29" not in csv_text


def test_export_dropped_column_leaves_csv_untouched(
    history_env: Path, patched_db: Path
) -> None:
    """A DB predating a schema column must not drop that column from history.

    CREATE TABLE IF NOT EXISTS never adds columns to an existing table, so an
    older local DB exports a narrower row set at an unchanged row count — the
    shrink guard alone would let it through.
    """
    history_env.mkdir()
    (history_env / "safex_prices.csv").write_text(
        "Date,commodity,Close,Volume,unit,contract\n"
        "2026-08-07,Soybean (SAFEX),7930.0,151.0,ZAR/MT,AUG26\n"
    )
    conn = sqlite3.connect(str(patched_db))
    conn.execute("DROP TABLE safex_prices")
    conn.execute(
        "CREATE TABLE safex_prices (Date TEXT NOT NULL, commodity TEXT NOT NULL, "
        "Close REAL, Volume REAL, unit TEXT, PRIMARY KEY (Date, commodity))"
    )
    conn.execute(
        "INSERT INTO safex_prices VALUES "
        "('2026-08-07', 'Soybean (SAFEX)', 7930.0, 151.0, 'ZAR/MT')"
    )
    conn.commit()
    conn.close()

    export_history()
    csv_text = (history_env / "safex_prices.csv").read_text()
    assert "contract" in csv_text, "export dropped a column from committed history"


def test_briefings_roundtrip(history_env: Path, patched_db: Path) -> None:
    """The briefing archive is point-in-time — nothing upstream can rebuild it."""
    conn = sqlite3.connect(str(patched_db))
    conn.execute(
        "INSERT INTO briefings (briefing_date, text, signals_json, snapshot_json, "
        "generated_at) VALUES ('2026-08-08', 'body', '[]', '{}', '2026-08-08T12:00:00')"
    )
    conn.commit()
    conn.close()

    export_history()
    assert (history_env / "briefings.csv").exists()

    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM briefings")
    conn.commit()
    conn.close()

    import_history()
    conn = sqlite3.connect(str(patched_db))
    try:
        rows = conn.execute("SELECT briefing_date, text FROM briefings").fetchall()
    finally:
        conn.close()
    assert rows == [("2026-08-08", "body")]


# --- the ""↔NULL boundary (T20 · F10 #68) -----------------------------------


def _exec(db_path: Path, sql: str, params: tuple = ()) -> None:
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _read(db_path: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(str(db_path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


def _gulf_row(db_path: Path, delivery: str, sale_type: str | None = None) -> None:
    _exec(
        db_path,
        "INSERT OR REPLACE INTO gulf_bids "
        "(report_date, commodity, location, delivery, sale_type, basis_low) "
        "VALUES ('2026-08-20', 'Soybeans', 'Gulf Coast Ports', ?, ?, 101.0)",
        (delivery, sale_type),
    )


_WASDE_ROW = (
    "INSERT INTO wasde (commodity, year, attribute, reference_period, value) "
    "VALUES ('Soybeans', '2026/27', 'Ending Stocks', NULL, 300.0)"
)


def test_blank_not_null_pk_roundtrips_as_empty_string(
    history_env: Path, patched_db: Path
) -> None:
    """gulf_bids.delivery is TEXT NOT NULL and store._str_cols blanks it.

    CSV cannot tell "" from NULL, so a blanket ""→NULL on import turned a
    storable empty string into a NOT NULL violation that aborted the whole
    run. A NOT NULL column can only ever have held "", so that is what the
    blank cell must be read back as.
    """
    _gulf_row(patched_db, "")
    assert export_history() >= 1
    _exec(patched_db, "DELETE FROM gulf_bids")

    import_history()
    assert _read(patched_db, "SELECT delivery, basis_low FROM gulf_bids") == [
        ("", 101.0)
    ]


def test_blank_not_null_pk_import_is_idempotent(
    history_env: Path, patched_db: Path
) -> None:
    """Re-importing the same CSV must not duplicate the blank-PK row."""
    _gulf_row(patched_db, "")
    export_history()
    import_history()
    import_history()

    assert _read(patched_db, "SELECT COUNT(*) FROM gulf_bids") == [(1,)]


def test_null_pk_import_is_idempotent(history_env: Path, patched_db: Path) -> None:
    """wasde.reference_period is a *nullable* PK column.

    SQLite lets NULL into such a column, and NULL != NULL in the implicit
    unique index — so a dedupe by equality never sees the existing row and
    the import appends a fresh duplicate on every run.
    """
    _exec(patched_db, _WASDE_ROW)

    export_history()
    import_history()
    import_history()

    assert _read(patched_db, "SELECT reference_period, value FROM wasde") == [
        (None, 300.0)
    ]


def test_nullable_pk_import_is_idempotent_for_brazil_estimates(
    history_env: Path, patched_db: Path
) -> None:
    """The second live nullable-PK column, on a different table.

    `wasde.reference_period` and `brazil_estimates.report_date` are the only
    two in HISTORY_TABLES; the plan is built per table from that table's own
    PRAGMA, so each one is its own path worth exercising.
    """
    _exec(
        patched_db,
        "INSERT INTO brazil_estimates "
        "(source, commodity, crop_year, attribute, report_date, value) "
        "VALUES ('CONAB', 'Soybeans', '2025/26', 'Production', NULL, 169.5)",
    )

    export_history()
    import_history()
    import_history()

    assert _read(
        patched_db, "SELECT report_date, value FROM brazil_estimates"
    ) == [(None, 169.5)]


def test_null_pk_import_never_clobbers_db_rows(
    history_env: Path, patched_db: Path
) -> None:
    """The NULL-safe path keeps the fresher-DB-wins rule."""
    _exec(patched_db, _WASDE_ROW)
    export_history()
    _exec(patched_db, "UPDATE wasde SET value = 999.0")

    import_history()
    assert _read(patched_db, "SELECT value FROM wasde") == [(999.0,)]


def test_blank_in_nullable_column_returns_null_not_empty_string(
    history_env: Path, patched_db: Path
) -> None:
    """The limit of what the CSV boundary can resolve — pinned deliberately.

    A NOT NULL column proves its own blank was "". A *nullable* one proves
    nothing, and the blank comes back as NULL, so a stored "" does not
    survive the round trip. No layer can reach this today (gulf_bids
    hard-fails an unknown sale_type before it is ever stored), and the fix
    if one ever could is at the write end — store NULL for "not given" per
    invariant 2, not a guess here. This test exists so that stops being
    silent: it fails the day the behaviour changes in either direction.
    """
    _gulf_row(patched_db, "Aug", sale_type="")
    export_history()
    _exec(patched_db, "DELETE FROM gulf_bids")

    import_history()
    assert _read(patched_db, "SELECT sale_type FROM gulf_bids") == [(None,)]


def test_blank_in_not_null_numeric_column_hard_fails(
    history_env: Path, patched_db: Path
) -> None:
    """Our own export can never produce this — a corrupt CSV must crash.

    Keeping "" for a NOT NULL column is only provable for TEXT affinity;
    an empty string in a NOT NULL numeric column would land as the text ""
    and poison the series silently.
    """
    history_env.mkdir()
    (history_env / "sagis_deliveries.csv").write_text(
        "commodity,season_year,week_number,week_end\nSoybeans,2025,,2025-03-08\n"
    )
    with pytest.raises(HistoryImportError):
        import_history()


# --- forward_curve history schema -------------------------------------------


def _fc_df(closes: dict[str, float]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"contract_month": month, "label": month, "ticker": "T", "close": close}
            for month, close in closes.items()
        ]
    )


def test_forward_curve_accumulates_history(patched_db: Path) -> None:
    """Two runs on different days keep both curves; read returns latest."""
    conn = sqlite3.connect(str(patched_db))
    for fetched, close in (("2026-07-29", 1200.0), ("2026-07-30", 1210.0)):
        conn.execute(
            "INSERT OR REPLACE INTO forward_curve "
            "(commodity, contract_month, label, ticker, close, fetched_date) "
            "VALUES ('Soybeans', '2026-03', 'Mar 26', 'ZSH26.CBT', ?, ?)",
            (close, fetched),
        )
    conn.commit()
    n_rows = conn.execute("SELECT COUNT(*) FROM forward_curve").fetchone()[0]
    conn.close()

    assert n_rows == 2  # history retained, not overwritten
    out = query.read_forward_curve("Soybeans")
    assert len(out) == 1  # but reads see only the latest snapshot
    assert out["fetched_date"].iloc[0] == "2026-07-30"
    assert out["close"].iloc[0] == 1210.0


def test_forward_curve_old_pk_migration(tmp_path: Path) -> None:
    """Old (commodity, contract_month) PK tables are rebuilt with
    fetched_date in the key and the stale unique index dropped."""
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """CREATE TABLE forward_curve (
               commodity TEXT NOT NULL, contract_month TEXT NOT NULL,
               label TEXT, ticker TEXT, close REAL, fetched_date TEXT NOT NULL,
               PRIMARY KEY (commodity, contract_month))"""
    )
    conn.execute(
        "CREATE UNIQUE INDEX ux_forward_curve_commodity_contract "
        "ON forward_curve (commodity, contract_month)"
    )
    conn.execute(
        "INSERT INTO forward_curve VALUES "
        "('Soybeans', '2026-03', 'Mar 26', 'ZSH26.CBT', 1200.0, '2026-07-29')"
    )
    conn.commit()

    store._migrate_forward_curve_pk(conn)

    pk_cols = {
        r[1] for r in conn.execute("PRAGMA table_info(forward_curve)").fetchall() if r[5]
    }
    assert pk_cols == {"commodity", "contract_month", "fetched_date"}
    # existing row survived and a second fetched_date now coexists
    conn.execute(
        "INSERT INTO forward_curve VALUES "
        "('Soybeans', '2026-03', 'Mar 26', 'ZSH26.CBT', 1210.0, '2026-07-30')"
    )
    assert conn.execute("SELECT COUNT(*) FROM forward_curve").fetchone()[0] == 2
    conn.close()
