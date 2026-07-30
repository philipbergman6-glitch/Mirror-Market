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
    """INSERT OR IGNORE: a fresher DB row wins over the committed CSV."""
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
