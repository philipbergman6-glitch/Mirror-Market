"""
Layer 17 — CEPEA/ESALQ Brazil domestic soy spot price.

CEPEA (Centro de Estudos Avançados em Economia Aplicada) at ESALQ/USP
is Brazil's leading agricultural commodity price research center.  Their
Soybean Indicator tracks the farm-gate price at Paranaguá port (Brazil's
main soy export terminal) in BRL per 60kg bag.

Why it matters:
    Brazilian farm-gate prices drive planting decisions.  When BRL weakens,
    Brazilian farmers effectively receive MORE BRL for their soy even at
    the same USD CBOT price — making them more competitive exporters.
    Tracking CEPEA in BRL reveals when Brazilian farmers are "happy" to sell
    vs holding back (which tightens global supply).

Unit conversion:
    CEPEA publishes BRL / 60kg bag.
    BRL/MT = (BRL per bag) / 60 × 1000

USD/MT conversion happens at the analysis layer using the BRL/USD rate from
the currencies table — NOT in this fetcher (display-layer-only conversion rule).

Source: CEPEA/ESALQ indicator page — no API key required.

Key concepts for learning:
    - pd.read_html() extracts tables from HTML pages automatically
    - The CEPEA page may have JavaScript — we try requests first, then
      describe what to do if it fails
    - Parsing mixed-format date strings from Brazilian pages
"""

import logging
import time
from datetime import datetime

import pandas as pd
import requests

from config import CEPEA_SOYBEAN_URL, MAX_RETRIES, REQUEST_TIMEOUT, RETRY_DELAY

logger = logging.getLogger(__name__)


def _parse_brl_price(val: str) -> float | None:
    """
    Convert a Brazilian-format price string to float.

    Brazilian number format uses period as thousands separator and comma
    as decimal: "1.234,56" → 1234.56

    Examples:
        "1.234,56" → 1234.56
        "123,45"   → 123.45
        "1234.56"  → 1234.56  (US format — also handled)
    """
    if not val or str(val).strip() in ("", "-", "N/A", "nan"):
        return None
    try:
        val = str(val).strip()
        # Brazilian format: remove thousands periods, replace decimal comma
        if "," in val and "." in val:
            # "1.234,56" format
            val = val.replace(".", "").replace(",", ".")
        elif "," in val:
            # "1234,56" format
            val = val.replace(",", ".")
        return float(val)
    except (ValueError, TypeError):
        return None


def _fetch_cepea_page() -> str:
    """Download the CEPEA soybean indicator page HTML."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                CEPEA_SOYBEAN_URL,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.text
            logger.warning("CEPEA: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("CEPEA: Request failed (attempt %d): %s", attempt, exc)

        if attempt < MAX_RETRIES:
            time.sleep(RETRY_DELAY)

    return ""


def _parse_cepea_tables(html: str) -> pd.DataFrame:
    """
    Extract the price table from CEPEA HTML using pd.read_html().

    CEPEA's soybean indicator page has a table with columns like:
        Date | Value (BRL/60kg bag) | ...

    We search all tables found for one that contains date + price data.

    Returns a DataFrame with columns: Date, price_brl_mt, Unit
    or an empty DataFrame if parsing fails.
    """
    if not html:
        return pd.DataFrame()

    try:
        tables = pd.read_html(html, decimal=",", thousands=".")
    except Exception as exc:
        logger.warning("CEPEA: pd.read_html() failed: %s", exc)
        return pd.DataFrame()

    if not tables:
        logger.warning("CEPEA: No HTML tables found on page")
        return pd.DataFrame()

    logger.info("CEPEA: Found %d HTML tables — searching for price data", len(tables))

    # Search each table for one that looks like price data
    for i, tbl in enumerate(tables):
        tbl.columns = [str(c).strip().lower() for c in tbl.columns]

        # Look for a date-like column and a numeric price column
        date_col = None
        price_col = None

        for col in tbl.columns:
            if "date" in col or "data" in col or "dia" in col:
                date_col = col
            if "value" in col or "valor" in col or "price" in col or "preco" in col or "preço" in col:
                price_col = col

        if date_col is None or price_col is None:
            # Try positional: first column as date, second as price
            if len(tbl.columns) >= 2:
                date_col = tbl.columns[0]
                price_col = tbl.columns[1]

        if date_col is None or price_col is None:
            continue

        logger.debug("CEPEA: Table %d — date_col=%s, price_col=%s", i, date_col, price_col)

        # Parse dates and prices
        rows = []
        for _, row in tbl.iterrows():
            date_val = str(row.get(date_col, "")).strip()
            price_val = str(row.get(price_col, "")).strip()

            # Try parsing the date
            parsed_date = None
            for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%m/%d/%Y"):
                try:
                    parsed_date = datetime.strptime(date_val, fmt).date()
                    break
                except ValueError:
                    continue

            if parsed_date is None:
                continue

            # Parse the price (BRL per 60kg bag)
            price_per_bag = _parse_brl_price(price_val)
            if price_per_bag is None or price_per_bag <= 0:
                continue

            # Convert: BRL/60kg → BRL/MT
            price_brl_mt = (price_per_bag / 60.0) * 1000.0

            rows.append({
                "Date": str(parsed_date),
                "price_brl_mt": round(price_brl_mt, 2),
                "Unit": "BRL/MT",
            })

        if rows:
            result = pd.DataFrame(rows)
            result = result.drop_duplicates("Date").sort_values("Date", ascending=False)
            logger.info(
                "CEPEA: Parsed %d price rows, latest = %.2f BRL/MT (%s)",
                len(result),
                result["price_brl_mt"].iloc[0],
                result["Date"].iloc[0],
            )
            return result

    logger.warning(
        "CEPEA: No price table found in %d HTML tables. "
        "Page may use JavaScript rendering — check %s manually.",
        len(tables), CEPEA_SOYBEAN_URL,
    )
    return pd.DataFrame()


def fetch_cepea() -> dict[str, pd.DataFrame]:
    """
    Fetch CEPEA/ESALQ Brazil soybean domestic price.

    Returns
    -------
    dict
        {"Soybean (CEPEA)": DataFrame}
        DataFrame columns: Date, price_brl_mt, Unit
        Returns {} if fetch/parse fails.
    """
    logger.info("Fetching CEPEA/ESALQ Brazil soybean price ...")
    html = _fetch_cepea_page()

    if not html:
        logger.warning(
            "CEPEA: Could not download page. "
            "Returning empty — pipeline continues without CEPEA data."
        )
        return {}

    df = _parse_cepea_tables(html)

    if df.empty:
        logger.warning("CEPEA: Parsed empty DataFrame — check page structure.")
        return {}

    return {"Soybean (CEPEA)": df}


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_cepea()
    if not data:
        logger.info("CEPEA: No data returned. Check URL or page structure.")
    else:
        for name, df in data.items():
            logger.info(
                "%s: %d rows\n%s",
                name, len(df), df.head(5).to_string(index=False),
            )
