"""End-to-end Turso roundtrip test.

Skipped unless both `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN` env vars
are set. When running, exercises the full save_* → read_* path against a
real libsql connection and asserts the data round-trips correctly.

Run locally:
    TURSO_DATABASE_URL=libsql://... TURSO_AUTH_TOKEN=... \
        pytest tests/integration/test_turso.py -v

Caveats
-------
* Uses sentinel commodity names prefixed with `TEST_TURSO_` to avoid
  polluting any real data in the target DB. Each test cleans up its
  rows in a `finally` block.
* Schema is created via `init_database()` — assumes the test user has
  CREATE TABLE permission (true for the default Turso owner token).
"""

from __future__ import annotations

import os
import sys
import uuid
from pathlib import Path

import pandas as pd
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


# Skip the whole module unless Turso is configured. Reading the env var
# at module collection time means CI without credentials never tries to
# import libsql.
pytestmark = pytest.mark.skipif(
    not (os.getenv("TURSO_DATABASE_URL") and os.getenv("TURSO_AUTH_TOKEN")),
    reason="TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set — Turso integration skipped",
)


@pytest.fixture(scope="module")
def turso_db():
    """Ensure the Turso schema exists. Returns the test-run UUID prefix."""
    from pipeline import connection, store

    if not connection.is_cloud():
        pytest.skip("is_cloud() returned False — not pointing at Turso")

    store.init_database()
    run_id = uuid.uuid4().hex[:8]
    yield f"TEST_TURSO_{run_id}"


def _cleanup(table: str, where: str, params: tuple) -> None:
    """Best-effort delete of test rows."""
    from pipeline.connection import get_connection

    try:
        with get_connection() as conn:
            conn.execute(f"DELETE FROM {table} WHERE {where}", params)
    except Exception:
        pass


def test_prices_roundtrip(turso_db):
    """save_price_data → read_prices via Turso preserves rows + values."""
    from pipeline import query, store

    sentinel = f"{turso_db}_PRICES"
    dates = pd.to_datetime(["2026-01-01", "2026-01-02", "2026-01-03"])
    df = pd.DataFrame(
        {
            "Open":   [100.0, 101.0, 102.0],
            "High":   [101.5, 102.5, 103.5],
            "Low":    [99.5,  100.5, 101.5],
            "Close":  [101.0, 102.0, 103.0],
            "Volume": [10_000.0, 10_500.0, 11_000.0],
        },
        index=dates,
    )
    df.index.name = "Date"

    try:
        store.save_price_data(sentinel, df)
        out = query.read_prices(sentinel)

        assert len(out) == 3
        assert sorted(out["Close"].tolist()) == [101.0, 102.0, 103.0]
        assert sorted(out["Volume"].tolist()) == [10_000.0, 10_500.0, 11_000.0]
    finally:
        _cleanup("prices", "commodity = ?", (sentinel,))


def test_prices_upsert_replaces_existing_row(turso_db):
    """A second save_price_data for the same (commodity, date) overwrites."""
    from pipeline import query, store

    sentinel = f"{turso_db}_UPSERT"
    dates = pd.to_datetime(["2026-01-01"])
    df_v1 = pd.DataFrame(
        {"Open": [100.0], "High": [101.0], "Low": [99.0],
         "Close": [100.5], "Volume": [1000.0]},
        index=dates,
    )
    df_v1.index.name = "Date"
    df_v2 = df_v1.copy()
    df_v2["Close"] = [999.0]

    try:
        store.save_price_data(sentinel, df_v1)
        store.save_price_data(sentinel, df_v2)
        out = query.read_prices(sentinel)
        assert len(out) == 1
        assert out["Close"].iloc[0] == 999.0
    finally:
        _cleanup("prices", "commodity = ?", (sentinel,))


def test_cot_roundtrip(turso_db):
    """save_cot_data → read_cot via Turso preserves rows + values."""
    from pipeline import query, store

    sentinel = f"{turso_db}_COT"
    df = pd.DataFrame(
        {
            "Date": pd.to_datetime(["2026-01-07", "2026-01-14"]),
            "commercial_long":     [100_000.0, 105_000.0],
            "commercial_short":    [80_000.0,  82_000.0],
            "commercial_net":      [20_000.0,  23_000.0],
            "noncommercial_long":  [200_000.0, 210_000.0],
            "noncommercial_short": [150_000.0, 155_000.0],
            "noncommercial_net":   [50_000.0,  55_000.0],
            "total_open_interest": [600_000.0, 620_000.0],
        }
    )

    try:
        store.save_cot_data(sentinel, df)
        out = query.read_cot(sentinel)
        assert len(out) == 2
        assert out["commercial_net"].sum() == pytest.approx(43_000.0)
    finally:
        _cleanup("cot", "commodity = ?", (sentinel,))


def test_psd_roundtrip_preserves_integer_year(turso_db):
    """PSD year is INTEGER NOT NULL — must survive the libsql roundtrip."""
    from pipeline import query, store

    sentinel = f"{turso_db}_PSD"
    df = pd.DataFrame(
        [
            {"commodity": sentinel, "country": "Brazil", "year": 2025,
             "attribute": "Production", "value": 169_000.0, "unit": "1000 MT"},
            {"commodity": sentinel, "country": "United States", "year": 2025,
             "attribute": "Production", "value": 122_000.0, "unit": "1000 MT"},
        ]
    )

    try:
        store.save_psd_data(sentinel, df)
        out = query.read_psd(sentinel)
        assert len(out) == 2
        assert set(out["country"]) == {"Brazil", "United States"}
        assert (out["year"] == 2025).all()
    finally:
        _cleanup("psd", "commodity = ?", (sentinel,))


def test_freshness_failed_preserves_prior_success(turso_db):
    """Failed status keeps the prior last_success stamp (cloud variant)."""
    from pipeline import store
    from pipeline.connection import get_connection

    layer = f"{turso_db}_LAYER"
    try:
        store.save_freshness(layer, rows_fetched=42, status="success")
        store.save_freshness(layer, rows_fetched=0, status="failed")

        with get_connection() as conn:
            row = conn.execute(
                "SELECT last_success, last_attempt, status FROM data_freshness "
                "WHERE layer_name = ?",
                (layer,),
            ).fetchone()

        assert row is not None
        last_success, last_attempt, status = row
        assert last_success is not None
        assert last_attempt is not None
        assert status == "failed"
    finally:
        _cleanup("data_freshness", "layer_name = ?", (layer,))
