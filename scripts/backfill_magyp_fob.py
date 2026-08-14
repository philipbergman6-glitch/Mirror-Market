"""One-time backfill of Argentina MAGyP official FOB history (#252).

The precios_fob.php web service answers ``?Fecha=`` for any past date —
probed live 2026-08-12 back to March 2021, same JSON shape as today's
circular — so the archive is walked one business day at a time through
the existing Layer 21 parser and save path. INSERT OR REPLACE makes
re-runs safe.

The walk is floored at 2024-11-01: the four NCM position mappings were
verified numerically against the labelled datos.gob.ar mirror over
2024-11-01 → 2025-01-21 only (#147), so an earlier date would store rows
under a product name the mapping evidence does not cover. Extending the
floor means re-running that cross-check first, not editing the constant.

Guard policy for historical vintages: `_parse_posts` and
`_check_every_product_present` hard-fail on circulars whose position set
drifted, exactly as they do live. A day they reject is skipped and
logged, never stored partially and never used as a reason to widen the
guard — on an old vintage a firing guard is the mapping evidence running
out, which is the floor's argument in miniature.

History CSVs are written by CI (`export_history` + the snapshot commit),
never by hand — run this through .github/workflows/backfill-magyp-fob.yml
to persist the result; a local run only fills the local DB.

    python scripts/backfill_magyp_fob.py [START [END]]   # ISO dates
"""

import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging  # noqa: E402
from fetchers.magyp_fob import _check_every_product_present, _fetch_day, _parse_posts  # noqa: E402
from pipeline.results import ScraperShapeError  # noqa: E402
from pipeline.store import init_database, save_argentina_fob  # noqa: E402

logger = logging.getLogger(__name__)

# Start of the #147 mirror cross-check window — the earliest date the
# NCM-position → product mapping is verified for. Hard floor, not a default.
VALIDATED_FLOOR = "2024-11-01"
_POLITE_DELAY_S = 1.0


def backfill(start: str, end: str) -> int:
    """Walk circulars from `end` back to `start`. Returns rows written."""
    if start < VALIDATED_FLOOR:
        raise SystemExit(
            f"start {start} predates {VALIDATED_FLOOR}, the first date the "
            "position mapping is verified for (#147). Re-run the datos.gob.ar "
            "cross-check for the earlier window before extending the floor."
        )
    checkpoints = pd.bdate_range(start=start, end=end)[::-1]
    total = 0
    unpublished = 0
    guard_skipped: list[str] = []
    transport_failed: list[str] = []
    for ts in checkpoints:
        day = ts.date()
        try:
            posts = _fetch_day(day)
            if not posts:
                unpublished += 1  # holiday — asked and answered
                continue
            df = _parse_posts(posts)
            if df.empty:
                unpublished += 1
                continue
            _check_every_product_present(df)
        except ScraperShapeError as exc:
            guard_skipped.append(day.isoformat())
            logger.warning("Guard rejected %s, day skipped: %s", day, exc)
            continue
        except requests.RequestException as exc:
            transport_failed.append(day.isoformat())
            logger.error("Transport failed for %s, day skipped: %s", day, exc)
            continue
        finally:
            time.sleep(_POLITE_DELAY_S)
        save_argentina_fob(df)
        total += len(df)
        logger.info("Stored %s — %d rows", day, len(df))

    logger.info(
        "Backfill walk done: %d rows stored, %d unpublished days, "
        "%d guard-skipped, %d transport failures",
        total, unpublished, len(guard_skipped), len(transport_failed),
    )
    if guard_skipped:
        logger.warning("Guard-skipped days (mapping drift?): %s", guard_skipped)
    if transport_failed:
        logger.warning("Transport-failed days — re-run these: %s", transport_failed)
    return total


if __name__ == "__main__":
    setup_logging()
    init_database()
    start = sys.argv[1] if len(sys.argv) > 1 else VALIDATED_FLOOR
    end = sys.argv[2] if len(sys.argv) > 2 else pd.Timestamp.today().date().isoformat()
    n = backfill(start, end)
    print(f"Backfilled {n} rows into argentina_fob ({start} → {end}).")
    if n == 0:
        sys.exit(1)
