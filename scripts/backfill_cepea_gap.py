"""One-time backfill of the CEPEA gap via Notícias Agrícolas archive pages.

The direct CEPEA scraper died on 2026-02-20 behind Cloudflare; Layer 17 was
re-enabled 2026-07-30 through Notícias Agrícolas, whose indicator pages
accept a /YYYY-MM-DD suffix. Each archive page carries exactly one session
(unlike the live page's ten), so the gap is walked one business day at a
time — ~110 requests for Feb→Jul at a polite 2s delay. INSERT OR REPLACE
makes re-runs safe.

    python scripts/backfill_cepea_gap.py [START [END]]   # ISO dates
"""

import logging
import sys
import time
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging  # noqa: E402
from fetchers.noticias_agricolas import fetch_noticias_agricolas  # noqa: E402
from pipeline.clean import clean_brazil_spot  # noqa: E402
from pipeline.store import save_brazil_spot  # noqa: E402

logger = logging.getLogger(__name__)

DEFAULT_START = "2026-02-20"
_POLITE_DELAY_S = 2.0


def backfill(start: str, end: str) -> int:
    """Walk archive pages from `end` back to `start`. Returns rows written."""
    checkpoints = pd.bdate_range(start=start, end=end)[::-1]
    total = 0
    for ts in checkpoints:
        day = ts.date().isoformat()
        logger.info("Backfilling archive page for %s ...", day)
        result = fetch_noticias_agricolas(history_date=day)
        if not result.has_rows:
            logger.warning("No data for %s: %s", day, result.error)
            continue
        for name, df in result.data.items():
            df = clean_brazil_spot(df)
            save_brazil_spot(name, df)
            total += len(df)
        time.sleep(_POLITE_DELAY_S)
    return total


if __name__ == "__main__":
    setup_logging()
    start = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_START
    end = sys.argv[2] if len(sys.argv) > 2 else pd.Timestamp.today().date().isoformat()
    n = backfill(start, end)
    print(f"Backfilled {n} rows into brazil_spot_prices ({start} → {end}).")
