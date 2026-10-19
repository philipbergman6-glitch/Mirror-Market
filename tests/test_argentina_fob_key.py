"""argentina_fob is keyed on the whole shipment band (B40 #541).

MAGyP prints one price per (position, ship_from, ship_to) band. The old key
(date, position, ship_from) let two bands with the same start month on one
day — ``2026-09`` and ``2026-09..2026-10`` — INSERT OR REPLACE each other,
and the history CSV round-trip kept only the last. These tests pin the new
key end to end and the migration that brings an old-shape table onto it.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import pytest

from pipeline import store
from pipeline.history import export_history, import_history
from pipeline.query import read_argentina_fob

_OLD_SHAPE = """
CREATE TABLE argentina_fob (
    date          TEXT NOT NULL,
    product       TEXT NOT NULL,
    position      TEXT NOT NULL,
    ship_from     TEXT NOT NULL,
    ship_to       TEXT,
    price_usd_mt  REAL,
    PRIMARY KEY (date, position, ship_from)
)
"""

_SAME_START_BANDS = pd.DataFrame(
    [
        {"date": "2026-09-15", "product": "Soybeans", "position": "12019000190C",
         "ship_from": "2026-09", "ship_to": "2026-09", "price_usd_mt": 401.0},
        {"date": "2026-09-15", "product": "Soybeans", "position": "12019000190C",
         "ship_from": "2026-09", "ship_to": "2026-10", "price_usd_mt": 398.0},
    ]
)


def _pk(conn: sqlite3.Connection) -> list[str]:
    info = conn.execute("PRAGMA table_info(argentina_fob)").fetchall()
    return [r[1] for r in sorted(info, key=lambda r: r[5]) if r[5]]


def _rows(conn: sqlite3.Connection) -> list[tuple]:
    return conn.execute(
        "SELECT date, product, position, ship_from, ship_to, price_usd_mt "
        "FROM argentina_fob ORDER BY date, position, ship_from, ship_to"
    ).fetchall()


@pytest.fixture
def history_env(patched_db: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    history_dir = tmp_path / "history"
    monkeypatch.setattr("pipeline.history.HISTORY_DIR", str(history_dir))
    monkeypatch.setattr(
        "pipeline.history.get_connection", lambda: sqlite3.connect(str(patched_db))
    )
    return history_dir


@pytest.fixture
def old_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A DB whose argentina_fob predates the band key, wired into store."""
    db_path = tmp_path / "old.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(_OLD_SHAPE)
    conn.commit()
    conn.close()
    monkeypatch.setattr("pipeline.store.get_connection", lambda: sqlite3.connect(str(db_path)))
    monkeypatch.setattr("pipeline.store.DB_PATH", str(db_path))
    monkeypatch.setattr("pipeline.store.STORAGE_DIR", str(tmp_path))
    return db_path


def test_same_start_bands_survive_store_csv_reload(
    history_env: Path, patched_db: Path
) -> None:
    store.save_argentina_fob(_SAME_START_BANDS)
    assert len(read_argentina_fob("Soybeans")) == 2

    assert export_history() == 2
    exported = pd.read_csv(history_env / "argentina_fob.csv", dtype=str)
    assert sorted(exported["ship_to"]) == ["2026-09", "2026-10"]

    # The ephemeral CI runner: an empty table re-seeded from the CSV.
    conn = sqlite3.connect(str(patched_db))
    conn.execute("DELETE FROM argentina_fob")
    conn.commit()
    conn.close()
    assert import_history() == 2

    back = read_argentina_fob("Soybeans").sort_values("ship_to")
    assert list(back["ship_to"]) == ["2026-09", "2026-10"]
    assert list(back["price_usd_mt"]) == [401.0, 398.0]
    # And the re-seed is idempotent under the new key.
    assert import_history() == 0


def test_save_rejects_a_band_without_an_end() -> None:
    """ship_to is part of the key now: a blank one is not a band."""
    bad = _SAME_START_BANDS.iloc[[0]].assign(ship_to=[None])
    with pytest.raises(ValueError, match="ship_to"):
        store.save_argentina_fob(bad)


def test_migration_rebuilds_old_key_and_keeps_every_row(old_db: Path) -> None:
    conn = sqlite3.connect(str(old_db))
    conn.executemany(
        "INSERT INTO argentina_fob VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-09", 401.0),
            ("2026-09-15", "Soybeans", "12019000190C", "2026-10", "2026-12", 395.0),
            ("2026-09-15", "Soybean Oil", "15071000100Q", "2026-09", "2026-10", 1150.0),
            ("2026-09-16", "Soybeans", "12019000190C", "2026-09", "2026-09", 402.0),
        ],
    )
    conn.commit()
    before = _rows(conn)
    conn.close()

    store.init_database()

    conn = sqlite3.connect(str(old_db))
    assert _pk(conn) == ["date", "position", "ship_from", "ship_to"]
    assert _rows(conn) == before
    # The point of the key: a same-start band now coexists.
    conn.execute(
        "INSERT INTO argentina_fob VALUES "
        "('2026-09-15', 'Soybeans', '12019000190C', '2026-09', '2026-10', 398.0)"
    )
    assert conn.execute("SELECT COUNT(*) FROM argentina_fob").fetchone()[0] == 5
    conn.commit()
    conn.close()

    store.init_database()  # idempotent: second pass is a no-op
    conn = sqlite3.connect(str(old_db))
    assert conn.execute("SELECT COUNT(*) FROM argentina_fob").fetchone()[0] == 5
    conn.close()


def test_migration_hard_fails_on_rows_that_collide_under_the_new_key(
    old_db: Path,
) -> None:
    """A keyless legacy table can hold two copies of one band. The migration
    must not pick one — it stops, and leaves the table exactly as it was."""
    conn = sqlite3.connect(str(old_db))
    conn.execute("DROP TABLE argentina_fob")
    conn.execute(
        "CREATE TABLE argentina_fob (date TEXT NOT NULL, product TEXT NOT NULL, "
        "position TEXT NOT NULL, ship_from TEXT NOT NULL, ship_to TEXT, "
        "price_usd_mt REAL)"
    )
    conn.executemany(
        "INSERT INTO argentina_fob VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-10", 398.0),
            ("2026-09-15", "Soybeans", "12019000190C", "2026-09", "2026-10", 399.0),
        ],
    )
    conn.commit()
    before = _rows(conn)
    conn.close()

    with pytest.raises(store.MigrationError, match="collide"):
        store.init_database()

    conn = sqlite3.connect(str(old_db))
    assert _rows(conn) == before
    assert _pk(conn) == []
    conn.close()


def test_migration_hard_fails_on_a_null_ship_to(old_db: Path) -> None:
    """A NULL end month cannot be a key part, and guessing one (ship_from)
    would invent a band MAGyP never printed."""
    conn = sqlite3.connect(str(old_db))
    conn.execute(
        "INSERT INTO argentina_fob VALUES "
        "('2026-09-15', 'Soybeans', '12019000190C', '2026-09', NULL, 401.0)"
    )
    conn.commit()
    conn.close()

    with pytest.raises(store.MigrationError, match="ship_to"):
        store.init_database()

    conn = sqlite3.connect(str(old_db))
    assert _pk(conn) == ["date", "position", "ship_from"]
    assert conn.execute("SELECT COUNT(*) FROM argentina_fob").fetchone()[0] == 1
    conn.close()
