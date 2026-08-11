"""
Layer 22 — European Commission Oilseeds Market Observatory weekly world prices.

The EU rapeseed leg of the Europe market page. Euronext MATIF (contract ECO)
settlements are *licensed* data — free for internal use, EUR 167.55/month to
redistribute, and this project publishes to GitHub Pages — so the futures
curve is deliberately not ingested (see issue #148). The Commission publishes
a weekly physical FOB assessment instead: `Rapeseed - EU Moselle`, sourced by
the EC from the International Grains Council, CC BY 4.0, no API key.

What this is NOT:
    - Not a futures price. There is no term structure, no open interest, and
      no daily print. A page rendering this must say so rather than implying
      MATIF coverage.
    - Not a proxy. EU Moselle is a real European physical rapeseed FOB quote
      in the real region; it is simply weekly rather than daily.

Two traps this module exists to survive:

1.  **The xlsx deep link is a CIRCABC GUID.** The World Bank Pink Sheet taught
    the lesson (fetchers/worldbank.py): a rotated-away GUID keeps serving a
    frozen file with HTTP 200, so the layer stays green on stale data forever.
    The link is therefore resolved from the observatory landing page each run,
    anchored on the link *text* ("World oilseed prices") rather than the
    filename — the published filename contains an upstream typo
    ("oliseeds-world-prices.xlsx") that a tidy-up would silently break.

2.  **The EUR/t block is derived, not independently quoted.** Verified against
    all 398 published rows on 2026-08-11: EUR equals USD divided by that row's
    ECB reference rate for 391 of the 393 rows that carry one. The other five
    rows read `n.q.`, and the two newest rows are converted at a rate two
    weeks stale (2026-08-05 quotes 605.52 USD / 530.79 EUR, an implied 1.1408
    against the 1.1554 printed on its own row — a 1.3% discrepancy). So the
    USD column is the authoritative series: 398 of 398 rows, no gaps, a
    perfect 7-day Wednesday cadence back to 2018-12-26. EUR is stored exactly
    as published — NULL where `n.q.` — and never recomputed by us, because
    inventing the missing conversions would mean picking a side in an upstream
    inconsistency and publishing the result as if it were the Commission's.
"""

import io
import logging
import re
import zipfile

import pandas as pd
import requests

from config import (
    EC_OILSEEDS_LANDING_URL,
    EC_OILSEEDS_SERIES,
    EC_OILSEEDS_WORLD_PRICES_URL,
    LAYER_MAX_DATA_AGE_DAYS,
    MAX_RETRIES,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# The landing page links several oilseed workbooks (production, trade,
# customs surveillance). Anchor on the link text, which names the dataset,
# rather than on the href, which is an opaque GUID plus a misspelled filename.
_WORLD_PRICES_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(https://circabc\.europa\.eu/[^"]+\.xlsx)"[^>]*>\s*'
    r'World oilseed prices\s*</a>',
    re.IGNORECASE,
)

# Row index of the header inside the `Data` sheet (0-based, as read with
# header=None). Rows 0-1 are the title band; row 2 carries the series names.
_HEADER_ROW = 2

# The workbook lays out the same series twice: a USD/t block, then a spacer,
# an ECB rate column, another spacer, and an EUR/t block. pandas disambiguates
# the repeated headers by appending ".1", which is how the EUR column is found.
_EUR_SUFFIX = ".1"

# "not quoted" — the Commission's sentinel for a week with no assessment.
_NOT_QUOTED = {"n.q.", "-", ""}


def _resolve_world_prices_url() -> str:
    """Resolve the current world-prices xlsx link from the observatory page.

    Falls back to the pinned GUID when the landing page is unreachable or has
    been restructured. The fallback is deliberately a last resort: a pinned
    CIRCABC link that has rotated away answers 200 with a frozen workbook, so
    the stale-file guard in fetch_ec_oilseed_prices() is what actually catches
    it — this only avoids a hard outage when the landing page moves first.
    """
    try:
        resp = requests.get(
            EC_OILSEEDS_LANDING_URL,
            timeout=60,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        match = _WORLD_PRICES_ANCHOR_RE.search(resp.text)
        if match:
            url = match.group(1)
            if url != EC_OILSEEDS_WORLD_PRICES_URL:
                logger.info(
                    "EC world-prices link resolved from landing page: %s", url
                )
            return url
        logger.warning(
            "EC oilseeds landing page had no 'World oilseed prices' link — "
            "using pinned URL. Check %s",
            EC_OILSEEDS_LANDING_URL,
        )
    except requests.RequestException as exc:
        logger.warning(
            "EC oilseeds landing page unreachable (%s) — using pinned URL", exc
        )
    return EC_OILSEEDS_WORLD_PRICES_URL


def _download_world_prices() -> bytes:
    """Download the world-prices xlsx, return raw bytes (b"" on failure)."""
    url = _resolve_world_prices_url()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading EC oilseeds world prices ...")
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            logger.info(
                "EC world prices downloaded (%d KB)", len(resp.content) // 1024
            )
            return resp.content

        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for EC world prices: %s",
                attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for EC world-prices download", MAX_RETRIES)
    return b""


def _numeric(value) -> float | None:
    """Parse a price cell, mapping the Commission's sentinels to None.

    `n.q.` ("not quoted") and `-` are real published values meaning the week
    carries no assessment — distinct from a parse failure, but both must land
    as NULL rather than as a number.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str) and value.strip().lower() in _NOT_QUOTED:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_world_prices(raw_bytes: bytes) -> dict[str, pd.DataFrame]:
    """Parse the `Data` sheet into one DataFrame per configured series.

    Returns
    -------
    dict
        {series_label: DataFrame} with columns Date, price_usd, price_eur.
        Empty when the workbook is unreadable or the configured series are
        absent — both are failures, never "nothing published this week".
    """
    try:
        df = pd.read_excel(
            io.BytesIO(raw_bytes),
            sheet_name="Data",
            header=_HEADER_ROW,
            engine="openpyxl",
        )
    except (ValueError, KeyError, OSError, zipfile.BadZipFile) as exc:
        # openpyxl raises ValueError on a malformed workbook and KeyError when
        # the `Data` sheet is missing. Non-zip bytes — a CIRCABC error page or
        # a truncated download served as 200 — raise zipfile.BadZipFile, which
        # is NOT an OSError subclass and would otherwise escape as a crash.
        logger.error("Failed to parse EC world-prices xlsx: %s", exc)
        return {}

    if df.empty:
        logger.warning("EC world-prices Data sheet is empty")
        return {}

    date_col = df.columns[0]
    dates = pd.to_datetime(df[date_col], errors="coerce")
    df = df[dates.notna()].copy()
    df["_date"] = dates[dates.notna()]

    if df.empty:
        logger.error("EC world-prices Data sheet has no parseable dates")
        return {}

    results: dict[str, pd.DataFrame] = {}
    for source_col, label in EC_OILSEEDS_SERIES.items():
        if source_col not in df.columns:
            # Hard-fail rather than skip: a renamed column is a silent
            # amputation of the only leg the Europe page has.
            logger.error(
                "EC world prices has no column %r — available: %s",
                source_col, [c for c in df.columns if not str(c).startswith("Unnamed")],
            )
            continue

        eur_col = f"{source_col}{_EUR_SUFFIX}"
        if eur_col not in df.columns:
            logger.warning(
                "EC world prices has no EUR column %r — storing USD only",
                eur_col,
            )

        out = pd.DataFrame({
            "Date": df["_date"].values,
            "price_usd": [_numeric(v) for v in df[source_col]],
            "price_eur": (
                [_numeric(v) for v in df[eur_col]] if eur_col in df.columns
                else [None] * len(df)
            ),
        })
        out = out.dropna(subset=["price_usd"])
        if out.empty:
            logger.error("EC series %r parsed to zero priced rows", source_col)
            continue

        out = out.sort_values("Date").reset_index(drop=True)
        results[label] = out
        logger.info(
            "  %s: %d weekly quotes, %s → %s (%d without a EUR conversion)",
            label, len(out),
            out["Date"].min().strftime("%Y-%m-%d"),
            out["Date"].max().strftime("%Y-%m-%d"),
            int(out["price_eur"].isna().sum()),
        )

    return results


def fetch_ec_oilseed_prices() -> dict[str, pd.DataFrame]:
    """
    Download and parse the EC weekly world oilseed prices.

    Returns
    -------
    dict
        {series_label: DataFrame} with columns Date, price_usd, price_eur.

        Empty when the download failed, the parse failed, **or** the workbook
        that came back is older than the stale-file budget. An empty return is
        graded as a layer failure by main.py (DictLayer.empty_fails), never as
        an empty-success — the workbook carries ~400 rows of history on every
        fetch, so "nothing came back" can only mean the source broke.
    """
    raw_bytes = _download_world_prices()
    if not raw_bytes:
        return {}

    results = _parse_world_prices(raw_bytes)
    if not results:
        return {}

    # Stale-file guard, mirroring fetchers/worldbank.py: a CIRCABC GUID that
    # has rotated away keeps serving the old workbook with HTTP 200. Detecting
    # that and storing the file anyway would be worse than not detecting it,
    # so the whole payload is discarded.
    latest = max(df["Date"].max() for df in results.values())
    age_days = (pd.Timestamp.today().normalize() - latest.normalize()).days
    budget = LAYER_MAX_DATA_AGE_DAYS["ec_oilseeds"]
    if age_days > budget:
        logger.error(
            "EC world prices end %s (%d days ago, budget %d) — the CIRCABC "
            "link is probably a stale GUID; discarding the workbook rather "
            "than storing it. Check %s",
            latest.strftime("%Y-%m-%d"), age_days, budget,
            EC_OILSEEDS_LANDING_URL,
        )
        return {}

    return results
