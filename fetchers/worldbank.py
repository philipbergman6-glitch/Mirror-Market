"""
Layer 8 — World Bank Pink Sheet monthly commodity prices.

Downloads the CMO Historical Data Monthly xlsx file from the World Bank.
This gives us monthly prices for Palm oil, Soybeans, Soybean oil,
Soybean meal, and substitute oils — going back to 1960.

Key concepts for learning:
    - pd.read_excel() with openpyxl engine for .xlsx files
    - The Pink Sheet has a specific layout: the "Monthly Prices" sheet
      has commodity names in the header rows and dates in the first column
    - We parse the header to find column positions for our target commodities
    - Monthly data is sufficient for trend/seasonal analysis (daily isn't
      available for free for the substitute-oil benchmarks)
"""

import logging
import re

import pandas as pd
import requests

from config import (
    LAYER_MAX_DATA_AGE_DAYS,
    MAX_RETRIES,
    WORLDBANK_CMO_LANDING_URL,
    WORLDBANK_PRICES_URL,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# Map from Pink Sheet column header substrings to our commodity names.
# The Pink Sheet headers are multi-row and use descriptive names — we
# search for these substrings to find the right columns.
#
# Layer 8b — Rotterdam CIF benchmark:
# "Soybean meal" in the World Bank Pink Sheet is defined as
# "United States origin, CIF Rotterdam" — this IS the global trade benchmark
# used by international crushers and traders.  So this layer already covers
# the Rotterdam CIF data gap described in the plan.
_WB_COMMODITY_MAP = {
    "Palm oil":         "Palm Oil",
    "Soybeans":         "Soybeans",
    "Soybean oil":      "Soybean Oil",
    "Soybean meal":     "Soybean Meal",
    # Substitute-oil benchmarks — no free daily futures feed exists for
    # Matif rapeseed or sunflower oil, so the Pink Sheet monthly $/mt
    # series are the implementable route.
    "Rapeseed oil":     "Rapeseed Oil",
    "Sunflower oil":    "Sunflower Oil",
}

_CMO_XLSX_LINK_RE = re.compile(
    r"https://thedocs\.worldbank\.org/[^\"' ]*CMO-Historical-Data-Monthly\.xlsx"
)


def _resolve_pink_sheet_url() -> str:
    """Resolve the current Pink Sheet xlsx link from the CMO landing page.

    The deep link's GUID rotates yearly and old links keep serving stale
    data with HTTP 200, so the landing page is the only reliable pointer.
    Falls back to the pinned WORLDBANK_PRICES_URL when unreachable.
    """
    try:
        resp = requests.get(
            WORLDBANK_CMO_LANDING_URL,
            timeout=60,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        resp.raise_for_status()
        match = _CMO_XLSX_LINK_RE.search(resp.text)
        if match:
            url = match.group(0)
            if url != WORLDBANK_PRICES_URL:
                logger.info("Pink Sheet link resolved from landing page: %s", url)
            return url
        logger.warning("CMO landing page had no Monthly xlsx link — using pinned URL")
    except requests.RequestException as exc:
        logger.warning("CMO landing page unreachable (%s) — using pinned URL", exc)
    return WORLDBANK_PRICES_URL


def _download_pink_sheet() -> bytes:
    """Download the Pink Sheet xlsx file, return raw bytes."""
    url = _resolve_pink_sheet_url()
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading World Bank Pink Sheet ...")
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            logger.info("Pink Sheet downloaded (%d KB)", len(resp.content) // 1024)
            return resp.content

        except requests.RequestException as exc:
            logger.warning(
                "Attempt %d/%d failed for Pink Sheet: %s",
                attempt, MAX_RETRIES, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for Pink Sheet download", MAX_RETRIES)
    return b""


def _parse_pink_sheet(raw_bytes: bytes) -> dict[str, pd.DataFrame]:
    """
    Parse the Pink Sheet xlsx into per-commodity DataFrames.

    The Monthly Prices sheet layout:
        - First few rows are headers with commodity names
        - First column contains dates like "1960M01" (year + month)
        - Data values are prices in USD

    Returns
    -------
    dict
        {commodity_name: DataFrame} with columns: Date, price, unit
    """
    import io

    # Read the Monthly Prices sheet — skip nothing, we'll parse headers manually
    try:
        # Try reading with header rows to find column names
        # The sheet typically has a few header rows then data
        raw_df = pd.read_excel(
            io.BytesIO(raw_bytes),
            sheet_name="Monthly Prices",
            header=None,
            engine="openpyxl",
        )
    except (ValueError, KeyError, OSError) as exc:
        # openpyxl raises ValueError on a malformed workbook and OSError
        # when the bytes aren't a valid zip; KeyError fires if the
        # "Monthly Prices" sheet is missing.
        logger.error("Failed to parse Pink Sheet xlsx: %s", exc)
        return {}

    if raw_df.empty:
        logger.warning("Pink Sheet Monthly Prices sheet is empty")
        return {}

    # Find the header row — look for a row containing commodity-like strings
    header_row_idx = None
    for idx in range(min(10, len(raw_df))):
        row_text = " ".join(str(v) for v in raw_df.iloc[idx].values)
        # The header row typically contains multiple commodity names
        matches = sum(1 for key in _WB_COMMODITY_MAP if key.lower() in row_text.lower())
        if matches >= 2:
            header_row_idx = idx
            break

    if header_row_idx is None:
        # Try a broader search across more rows
        for idx in range(min(20, len(raw_df))):
            row_text = " ".join(str(v) for v in raw_df.iloc[idx].values)
            if "palm" in row_text.lower() or "soybean" in row_text.lower():
                header_row_idx = idx
                break

    if header_row_idx is None:
        logger.error("Could not find header row in Pink Sheet")
        return {}

    logger.info("Found Pink Sheet header at row %d", header_row_idx)

    # Map column indices to commodity names
    col_map = {}  # column_index → commodity_name

    for col_idx in range(len(raw_df.columns)):
        cell_value = str(raw_df.iloc[header_row_idx, col_idx])
        for search_str, commodity_name in _WB_COMMODITY_MAP.items():
            if search_str.lower() in cell_value.lower():
                col_map[col_idx] = commodity_name
                break

    if not col_map:
        logger.error("No target commodities found in Pink Sheet headers")
        return {}

    logger.info("Found columns for: %s", list(col_map.values()))

    # Find the data start row — first row after header with a date-like value
    data_start = header_row_idx + 1
    for idx in range(header_row_idx + 1, min(header_row_idx + 10, len(raw_df))):
        cell = str(raw_df.iloc[idx, 0])
        if "M" in cell and cell[:4].isdigit():
            data_start = idx
            break

    # Extract data rows
    results = {}
    for col_idx, commodity_name in col_map.items():
        dates = []
        prices = []

        for row_idx in range(data_start, len(raw_df)):
            date_cell = str(raw_df.iloc[row_idx, 0])

            # Parse dates like "1960M01" → "1960-01-01"
            if "M" not in date_cell or not date_cell[:4].isdigit():
                continue

            try:
                parts = date_cell.split("M")
                year = int(parts[0])
                month = int(parts[1])
                date = pd.Timestamp(year=year, month=month, day=1)
            except (ValueError, IndexError):
                continue

            price_val = raw_df.iloc[row_idx, col_idx]
            try:
                price = float(price_val)
            except (ValueError, TypeError):
                continue

            dates.append(date)
            prices.append(price)

        if dates:
            df = pd.DataFrame({
                "Date": dates,
                "price": prices,
                "unit": "$/mt",
            })
            results[commodity_name] = df
            logger.info(
                "  %s: %d monthly prices, %s → %s",
                commodity_name, len(df),
                df["Date"].min().strftime("%Y-%m"),
                df["Date"].max().strftime("%Y-%m"),
            )

    return results


def fetch_worldbank_prices() -> dict[str, pd.DataFrame]:
    """
    Download Pink Sheet xlsx, extract monthly prices for target commodities.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — e.g. {"Palm Oil": DataFrame, ...}
        Each DataFrame has columns: Date, price, unit

        Empty when the download failed **or** when the file that came back
        is older than the stale-file budget. An empty return is treated as
        a layer failure by main.py (DictLayer.empty_fails), not as an
        empty-success, so it can never stamp a fresh last_success.
    """
    raw_bytes = _download_pink_sheet()
    if not raw_bytes:
        return {}

    results = _parse_pink_sheet(raw_bytes)

    # Stale-file guard: a rotated-away GUID keeps serving old data with
    # HTTP 200, so check the newest month actually present.
    latest = max(
        (df["Date"].max() for df in results.values() if not df.empty),
        default=None,
    )
    if latest is not None:
        age_days = (pd.Timestamp.today() - latest).days
        budget = LAYER_MAX_DATA_AGE_DAYS["worldbank"]
        if age_days > budget:
            # Audit F3a: this used to log the error and return the frozen
            # data anyway, which stored a stale vintage and stamped the
            # layer green. Detecting the trap and walking into it is worse
            # than not detecting it — the log line was the only evidence,
            # and nothing reads the logs.
            logger.error(
                "Pink Sheet data ends %s (%d days ago, budget %d) — the "
                "download link is probably a stale GUID; discarding the file "
                "rather than storing it. Check WORLDBANK_CMO_LANDING_URL",
                latest.strftime("%Y-%m"), age_days, budget,
            )
            return {}
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_worldbank_prices()
    for name, df in data.items():
        logger.info("%s: %d rows, latest: %s", name, len(df), df["Date"].max())
