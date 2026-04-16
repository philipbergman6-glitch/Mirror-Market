"""
Layer 15 — CONAB (Companhia Nacional de Abastecimento) Brazil crop estimates.

Brazil's official crop agency publishes monthly production, area, and yield
estimates that often differ from USDA by millions of tonnes. Getting both
gives you the range of uncertainty.

Source: CONAB data portal — historical series download.
No API key required.

Key concepts for learning:
    - CONAB publishes in Portuguese — column headers need translation.
    - The data is a tab-separated text file with crop-year rows.
    - We parse it defensively since the format may change between updates.
"""

import io
import logging
import time
from datetime import datetime

import requests
import pandas as pd

from config import CONAB_URL, REQUEST_TIMEOUT, MAX_RETRIES, RETRY_DELAY

logger = logging.getLogger(__name__)

# Portuguese → English column mapping for common CONAB headers
_CONAB_COLUMNS = {
    "produto": "commodity",
    "safra": "crop_year",
    "área plantada": "area",
    "area plantada": "area",
    "área (mil ha)": "area",
    "produtividade": "yield",
    "produtividade (kg/ha)": "yield",
    "produção": "production",
    "producao": "production",
    "produção (mil t)": "production",
}

# Commodities we care about
_TARGET_COMMODITIES = {"soja", "milho", "algodão", "trigo", "café"}


def fetch_conab_estimates() -> pd.DataFrame:
    """
    Fetch CONAB historical series data for Brazilian crop estimates.

    Returns a DataFrame with columns:
        source, commodity, crop_year, attribute, value, unit, report_date

    Empty DataFrame if the download fails or can't be parsed.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching CONAB historical series (attempt %d) ...", attempt)
            resp = requests.get(CONAB_URL, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d for CONAB", resp.status_code)
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)
                    continue
                return pd.DataFrame()

            # Try to parse as tab-separated text
            text = resp.text
            try:
                df = pd.read_csv(
                    io.StringIO(text),
                    sep="\t",
                    encoding="utf-8",
                    on_bad_lines="skip",
                )
            except Exception:
                # Try semicolon separator (common in Brazilian data)
                try:
                    df = pd.read_csv(
                        io.StringIO(text),
                        sep=";",
                        encoding="utf-8",
                        on_bad_lines="skip",
                    )
                except Exception as exc:
                    logger.warning("Could not parse CONAB data: %s", exc)
                    return pd.DataFrame()

            if df.empty:
                logger.info("CONAB data parsed but empty.")
                return pd.DataFrame()

            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]

            # Rename known Portuguese columns
            rename = {}
            for col in df.columns:
                for pt_name, en_name in _CONAB_COLUMNS.items():
                    if pt_name in col:
                        rename[col] = en_name
                        break
            if rename:
                df = df.rename(columns=rename)

            # Filter to target commodities if 'commodity' column exists
            if "commodity" in df.columns:
                df["commodity_lower"] = df["commodity"].str.strip().str.lower()
                df = df[df["commodity_lower"].isin(_TARGET_COMMODITIES)]
                df = df.drop(columns=["commodity_lower"])

            # Melt into long format: commodity, crop_year, attribute, value
            rows = []
            today = datetime.utcnow().strftime("%Y-%m-%d")

            # Map commodity names to English
            commodity_map = {
                "soja": "Soybeans",
                "milho": "Corn",
                "algodão": "Cotton",
                "trigo": "Wheat",
                "café": "Coffee",
            }

            for _, row in df.iterrows():
                crop_year = str(row.get("crop_year", row.get("safra", "")))
                raw_commodity = str(row.get("commodity", "")).strip().lower()
                en_commodity = commodity_map.get(raw_commodity, raw_commodity.title())

                for attr in ["production", "area", "yield"]:
                    if attr in row.index and pd.notna(row[attr]):
                        try:
                            val = float(str(row[attr]).replace(",", ".").replace(" ", ""))
                        except (ValueError, TypeError):
                            continue

                        unit = "1000 MT" if attr == "production" else (
                            "1000 HA" if attr == "area" else "KG/HA"
                        )

                        rows.append({
                            "source": "CONAB",
                            "commodity": en_commodity,
                            "crop_year": crop_year,
                            "attribute": attr.title(),
                            "value": val,
                            "unit": unit,
                            "report_date": today,
                        })

            if rows:
                result = pd.DataFrame(rows)
                logger.info("Parsed %d CONAB estimate rows.", len(result))
                return result
            else:
                logger.info("CONAB data parsed but no target commodities found.")
                return pd.DataFrame()

        except requests.RequestException as exc:
            logger.warning("CONAB attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_DELAY)

    logger.error("All %d attempts failed for CONAB", MAX_RETRIES)
    return pd.DataFrame()


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_conab_estimates()
    if data.empty:
        logger.info("CONAB: no data returned")
    else:
        logger.info("CONAB: %d rows", len(data))
        logger.info("\n%s", data.head(10).to_string(index=False))
