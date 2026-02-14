"""
Layer 11 — Forward curve data via yfinance.

Instead of just the front-month continuous contract (ZS=F), this fetcher
builds the full forward curve by fetching individual contract months.
The forward curve reveals market structure:
    - Contango (future > spot) = market expects oversupply / carry costs
    - Backwardation (spot > future) = tight supply / immediate demand

Key concepts for learning:
    - Futures contracts have expiration months — ZSN26 = Soybeans Jul 2026
    - Month codes: F=Jan, G=Feb, H=Mar, J=Apr, K=May, M=Jun,
                   N=Jul, Q=Aug, U=Sep, V=Oct, X=Nov, Z=Dec
    - Ticker format: {root}{month_code}{2-digit year}.{exchange}
    - We only fetch contracts that are still in the future (no expired ones)
    - Reuses fetch_one() from yfinance_fetcher for retry logic
"""

import logging
from datetime import date

import pandas as pd

from config import FORWARD_CURVE_CONTRACTS, MONTH_CODES
from data.fetchers.yfinance_fetcher import fetch_one

logger = logging.getLogger(__name__)


def _build_contract_tickers(root: str, exchange: str, trading_months: list[int],
                            num_contracts: int = 6) -> list[dict]:
    """
    Build a list of upcoming contract tickers for a commodity.

    Parameters
    ----------
    root : str
        Root symbol (e.g. "ZS" for soybeans).
    exchange : str
        Exchange suffix (e.g. "CBT").
    trading_months : list[int]
        Calendar months this commodity trades (e.g. [1,3,5,7,8,9,11]).
    num_contracts : int
        How many future contracts to fetch (default 6).

    Returns
    -------
    list[dict]
        Each dict has: ticker, contract_month (datetime), label (e.g. "Jul 2026")
    """
    today = date.today()
    contracts = []
    year = today.year
    max_year = year + 3  # look up to 3 years ahead

    while len(contracts) < num_contracts and year <= max_year:
        for month in sorted(trading_months):
            if len(contracts) >= num_contracts:
                break

            # Skip months in the past
            if year == today.year and month <= today.month:
                continue

            month_code = MONTH_CODES[month]
            yr_2digit = str(year)[-2:]
            ticker = f"{root}{month_code}{yr_2digit}.{exchange}"
            contract_date = date(year, month, 1)
            label = contract_date.strftime("%b %Y")

            contracts.append({
                "ticker": ticker,
                "contract_month": contract_date,
                "label": label,
            })

        year += 1

    return contracts


def fetch_forward_curve(commodity: str) -> pd.DataFrame:
    """
    Fetch the forward curve for a single commodity.

    Downloads the latest close price for each upcoming contract month,
    building a picture of the term structure.

    Parameters
    ----------
    commodity : str
        Commodity name matching a key in FORWARD_CURVE_CONTRACTS.

    Returns
    -------
    pd.DataFrame
        Columns: commodity, contract_month, label, ticker, close
        Sorted by contract_month (nearest first).
        Empty DataFrame if the commodity isn't configured or no data found.
    """
    spec = FORWARD_CURVE_CONTRACTS.get(commodity)
    if not spec:
        logger.warning("No forward curve config for %s", commodity)
        return pd.DataFrame()

    contracts = _build_contract_tickers(
        root=spec["root"],
        exchange=spec["exchange"],
        trading_months=spec["months"],
    )

    rows = []
    for contract in contracts:
        ticker = contract["ticker"]
        logger.debug("Fetching %s curve contract %s ...", commodity, ticker)

        # Fetch just 5 days of data — we only need the latest close
        df = fetch_one(ticker, period="5d")
        if df.empty:
            logger.debug("  No data for %s — contract may not be active yet", ticker)
            continue

        latest_close = df["Close"].iloc[-1]
        if pd.notna(latest_close):
            rows.append({
                "commodity": commodity,
                "contract_month": contract["contract_month"].isoformat(),
                "label": contract["label"],
                "ticker": ticker,
                "close": float(latest_close),
            })

    if not rows:
        logger.warning("No forward curve data for %s", commodity)
        return pd.DataFrame()

    result = pd.DataFrame(rows)
    result = result.sort_values("contract_month").reset_index(drop=True)
    return result


def fetch_all_forward_curves() -> dict[str, pd.DataFrame]:
    """
    Fetch forward curves for all configured commodities.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — one entry per commodity.
    """
    results = {}

    for commodity in FORWARD_CURVE_CONTRACTS:
        logger.info("Fetching forward curve for %s ...", commodity)
        df = fetch_forward_curve(commodity)
        results[commodity] = df
        if not df.empty:
            logger.info(
                "  Got %d contracts for %s: %s → %s",
                len(df), commodity,
                df["label"].iloc[0], df["label"].iloc[-1],
            )

    return results


# ── Quick self-test ────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging(logging.DEBUG)

    data = fetch_all_forward_curves()
    for name, df in data.items():
        if df.empty:
            logger.info("%s: NO DATA", name)
        else:
            logger.info("%s curve: %d contracts", name, len(df))
            for _, row in df.iterrows():
                logger.info("  %s: %.2f", row["label"], row["close"])
