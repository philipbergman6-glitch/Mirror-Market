"""
Layer 6 — USDA FAS PSD (Production, Supply & Distribution) global data.

Downloads bulk CSV zips from the USDA Foreign Agricultural Service.
These contain production, imports, exports, crush, and ending stocks
for every country — back to 1960.  Updated monthly.  No API key needed.

Key concepts for learning:
    - zipfile + io.BytesIO: extract files in memory without writing temp files
    - Filtering large DataFrames: the raw CSV has ~200K rows, but we only
      keep the commodities/countries/attributes we care about
    - The PSD data gives you the GLOBAL picture — who produces, who imports,
      who has stocks — which CBOT prices alone can't tell you
"""

import io
import logging
import time
import zipfile

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    PSD_TARGET_ATTRIBUTES,
    PSD_TARGET_COMMODITIES,
    PSD_TARGET_COUNTRIES,
    PSD_URLS,
    REQUEST_TIMEOUT,
    RETRY_DELAY,
)

logger = logging.getLogger(__name__)


def fetch_psd_commodity_group(group_name: str) -> pd.DataFrame:
    """
    Download a PSD bulk zip, extract the CSV, return raw DataFrame.

    Parameters
    ----------
    group_name : str
        Key in PSD_URLS, e.g. "oilseeds" or "coffee".

    Returns
    -------
    pd.DataFrame
        Raw CSV contents — tens of thousands of rows before filtering.
    """
    url = PSD_URLS[group_name]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading PSD %s data (%s) ...", group_name, url)
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # Extract CSV from the zip archive in memory
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    logger.warning("No CSV found in %s zip", group_name)
                    return pd.DataFrame()

                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(f, low_memory=False)

            logger.info(
                "PSD %s: downloaded %d rows, %d columns",
                group_name, len(df), len(df.columns),
            )
            return df

        except Exception as exc:
            logger.warning(
                "Attempt %d/%d failed for PSD %s: %s",
                attempt, MAX_RETRIES, group_name, exc,
            )
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for PSD %s", MAX_RETRIES, group_name)
    return pd.DataFrame()


def _filter_psd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter raw PSD data to just the commodities, countries, and attributes
    we track.

    The PSD CSV columns vary slightly between files, but generally include:
        Commodity_Code, Commodity_Description, Country_Name,
        Attribute_Description, Market_Year, Value, Unit_Description
    """
    if df.empty:
        return df

    # Build a set of target commodity codes for fast lookup
    target_codes = set(PSD_TARGET_COMMODITIES.values())

    # Filter by commodity code
    code_col = "Commodity_Code"
    if code_col not in df.columns:
        logger.warning("PSD CSV missing '%s' column — skipping filter", code_col)
        return pd.DataFrame()

    # Convert commodity code to string for matching
    df[code_col] = df[code_col].astype(str).str.strip()
    df = df[df[code_col].isin(target_codes)]

    # Filter by country
    country_col = "Country_Name"
    if country_col in df.columns:
        df = df[df[country_col].isin(PSD_TARGET_COUNTRIES)]

    # Filter by attribute
    attr_col = "Attribute_Description"
    if attr_col in df.columns:
        df = df[df[attr_col].isin(PSD_TARGET_ATTRIBUTES)]

    # Build a reverse lookup: code → commodity name
    code_to_name = {v: k for k, v in PSD_TARGET_COMMODITIES.items()}

    # Standardise output columns
    result = pd.DataFrame({
        "commodity": df[code_col].map(code_to_name),
        "country":   df[country_col],
        "year":      pd.to_numeric(df["Market_Year"], errors="coerce"),
        "attribute": df[attr_col],
        "value":     pd.to_numeric(df["Value"], errors="coerce"),
        "unit":      df.get("Unit_Description", "1000 MT"),
    })

    return result.dropna(subset=["commodity", "year"])


def fetch_psd_all() -> dict[str, pd.DataFrame]:
    """
    Fetch oilseeds + coffee PSD data, filter to target commodities/countries/attributes.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — e.g. {"Soybeans": DataFrame, ...}
        Each DataFrame has columns: commodity, country, year, attribute, value, unit
    """
    all_filtered = []

    for group_name in PSD_URLS:
        raw = fetch_psd_commodity_group(group_name)
        if raw.empty:
            continue
        filtered = _filter_psd(raw)
        if not filtered.empty:
            all_filtered.append(filtered)
            logger.info(
                "PSD %s: kept %d rows after filtering", group_name, len(filtered),
            )

    if not all_filtered:
        logger.warning("No PSD data collected from any group")
        return {}

    combined = pd.concat(all_filtered, ignore_index=True)

    # Split by commodity name
    results = {}
    for commodity in combined["commodity"].unique():
        results[commodity] = combined[combined["commodity"] == commodity].reset_index(drop=True)

    logger.info(
        "PSD total: %d rows across %d commodities",
        len(combined), len(results),
    )
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_psd_all()
    for name, df in data.items():
        logger.info(
            "%s: %d rows, countries: %s",
            name, len(df), sorted(df["country"].unique()),
        )
