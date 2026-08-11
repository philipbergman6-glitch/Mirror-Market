"""Backfill brazil_spot_prices from archived briefing snapshots.

The briefings table archives a snapshot_json per day; its
brazil_basis.paranagua_fob block stores the AgRural spot in USD/MT together
with the BRL/USD rate used that day. Because the snapshot layer never stored
the raw BRL price, we reconstruct it:

    price_brl = spot_usd_mt / brl_usd        (brl_usd is USD-per-BRL)

Only dates missing from brazil_spot_prices are written, so the script is
safe to re-run. Run it once after wiring up a persistent (Turso) database
to seed basis history from the archive:

    python scripts/backfill_brazil_spot.py
"""

import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging  # noqa: E402
from pipeline.connection import get_connection, managed_connection, maybe_sync  # noqa: E402

logger = logging.getLogger(__name__)

# snapshot key → commodity name used by save_brazil_spot / read_brazil_spot
SNAPSHOT_SOURCES = {
    "paranagua_fob": "Soybean (AgRural Paranaguá FOB)",
    "cepea_parana": "Soybean (CEPEA)",
}


def backfill() -> int:
    """Reconstruct BRL/MT spot rows from briefing snapshots. Returns rows written."""
    with managed_connection(get_connection()) as conn:
        rows = conn.execute(
            "SELECT briefing_date, snapshot_json FROM briefings ORDER BY briefing_date"
        ).fetchall()
        existing = {
            (r[0], r[1])
            for r in conn.execute("SELECT Date, commodity FROM brazil_spot_prices").fetchall()
        }

        to_write: list[tuple[str, str, float, str]] = []
        for briefing_date, snapshot_json in rows:
            if not snapshot_json:
                continue
            try:
                snapshot = json.loads(snapshot_json)
            except json.JSONDecodeError:
                logger.warning("Unparseable snapshot_json for %s — skipped", briefing_date)
                continue
            basis = (snapshot or {}).get("brazil_basis") or {}
            for key, commodity in SNAPSHOT_SOURCES.items():
                block = basis.get(key) or {}
                spot_usd = block.get("spot_usd_mt")
                brl_usd = block.get("brl_usd")
                if spot_usd is None or not brl_usd:
                    continue
                if (briefing_date, commodity) in existing:
                    continue
                price_brl = float(spot_usd) / float(brl_usd)
                to_write.append((briefing_date, commodity, price_brl, "BRL/MT"))

        if to_write:
            conn.executemany(
                """INSERT OR REPLACE INTO brazil_spot_prices
                   (Date, commodity, price_brl, unit) VALUES (?, ?, ?, ?)""",
                to_write,
            )
            maybe_sync(conn)

    logger.info(
        "Backfilled %d brazil_spot rows from %d archived briefings", len(to_write), len(rows)
    )
    return len(to_write)


if __name__ == "__main__":
    setup_logging()
    n = backfill()
    print(f"Backfilled {n} rows into brazil_spot_prices.")
