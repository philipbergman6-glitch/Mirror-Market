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
    - The *current* delivery month is part of the curve until it expires —
      see _build_contract_tickers
    - Every leg of one curve must carry the same observation date —
      see _enforce_single_observation_date
    - Reuses fetch_one() from yfinance_fetcher for retry logic
"""

import logging
from datetime import date

import pandas as pd

from config import FORWARD_CURVE_CONTRACTS, MONTH_CODES
from fetchers.yfinance import fetch_one

logger = logging.getLogger(__name__)


def _build_contract_tickers(root: str, exchange: str, trading_months: list[int],
                            num_contracts: int = 6,
                            today: date | None = None) -> list[dict]:
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
    today : date | None
        Clock injection point for tests; defaults to today.

    The current delivery month is a *candidate*, not a skip. A CBOT grain
    contract trades until the business day before the 15th of its own
    delivery month, so for the first half of every delivery month the real
    front of the curve is the current month — dropping it started the curve
    one contract out and erased the nearby leg of the term structure
    (observed 2026-08-12: Lean Hogs Aug 95.93 vs Oct 83.30, a 12.6-point
    front inversion invisible to the old skip; Soybeans Aug 1147.25 vs Sep
    1150.25).

    Whether that contract is still alive is decided by the *data*, not by a
    per-venue expiry-rule estimate: the nine commodities here span four
    expiry conventions (CBOT grains, ICE softs, CME livestock), and Yahoo
    delists an expired contract outright — verified 2026-08-12, every one of
    ZSN26/ZCN26/LEM26/ZSK26/SBN26 returns an empty frame, not a stale bar.
    An expired month therefore drops out at the fetch, and anything that
    lingers is caught by the single-observation-date rule below. The cost is
    one wasted ticker (three retries) per commodity for the back half of its
    delivery month, paid to avoid encoding four expiry calendars.

    Returns
    -------
    list[dict]
        Each dict has: ticker, contract_month (datetime), label (e.g. "Jul 2026")
    """
    today = today or date.today()
    contracts = []
    year = today.year
    max_year = year + 3  # look up to 3 years ahead

    while len(contracts) < num_contracts and year <= max_year:
        for month in sorted(trading_months):
            if len(contracts) >= num_contracts:
                break

            # Skip months already past — the current month still trades
            if year == today.year and month < today.month:
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


def _observation_date(df: pd.DataFrame, ticker: str) -> str:
    """ISO date of the last bar in ``df``.

    Hard-fails on a non-datetime index rather than coercing: the whole
    single-observation-date rule rests on this value, and a silently wrong
    date would let a stale leg pass as current.
    """
    try:
        stamp = pd.Timestamp(df.index[-1])
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{ticker}: forward-curve frame has a non-datetime index "
            f"({df.index[-1]!r}) — cannot date the leg"
        ) from exc
    if pd.isna(stamp):
        raise ValueError(f"{ticker}: forward-curve frame has an undated last bar")
    return stamp.date().isoformat()


def _latest_volume(df: pd.DataFrame) -> float | None:
    """Volume on the leg's last bar, or None when the provider gave none.

    None rather than 0.0: a deferred contract that genuinely did not trade and
    a provider that simply omitted the field look identical at zero, and the
    first is a fact about liquidity while the second is a fact about us.
    """
    if "Volume" not in df.columns:
        return None
    value = df["Volume"].iloc[-1]
    return None if pd.isna(value) else float(value)


def _enforce_single_observation_date(df: pd.DataFrame, commodity: str) -> pd.DataFrame:
    """Keep only the legs observed on the curve's newest observation date.

    A forward curve is a snapshot of one moment. yfinance answers each
    contract with its own last bar, and thin deferreds (and expiring fronts)
    can be days behind the liquid months — observed 2026-08-12: every
    Soybean Oil leg printed 08-11 except the expiring Aug contract, stuck on
    08-10. Reading a curve out of legs dated differently invents structure:
    after a big session the stale legs still carry the previous day's level,
    so the spread against them is yesterday's move mislabelled as
    contango/backwardation.

    The rule is a single date, not a tolerance window — one day of drift is
    exactly the case that fakes the structure, so there is no N of stale
    days worth keeping. A dropped leg shortens the curve, which is honest;
    ``analyze_curve`` needs two legs and simply returns nothing below that.
    """
    curve_date = df["observation_date"].max()
    stale = df[df["observation_date"] != curve_date]
    if not stale.empty:
        logger.warning(
            "%s curve dated %s — dropping %d leg(s) observed earlier: %s",
            commodity, curve_date, len(stale),
            ", ".join(
                f"{row.label} ({row.ticker} @ {row.observation_date})"
                for row in stale.itertuples()
            ),
        )
    return df[df["observation_date"] == curve_date]


def fetch_forward_curve(commodity: str, today: date | None = None) -> pd.DataFrame:
    """
    Fetch the forward curve for a single commodity.

    Downloads the latest close price for each upcoming contract month,
    building a picture of the term structure.

    Parameters
    ----------
    commodity : str
        Commodity name matching a key in FORWARD_CURVE_CONTRACTS.
    today : date | None
        Clock injection point for tests; defaults to today.

    Returns
    -------
    pd.DataFrame
        Columns: commodity, contract_month, label, ticker, close,
        observation_date. Sorted by contract_month (nearest first), every
        row carrying the same observation_date.
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
        today=today,
    )

    rows = []
    for contract in contracts:
        ticker = contract["ticker"]
        logger.debug("Fetching %s curve contract %s ...", commodity, ticker)

        # Fetch just 5 days of data — we only need the latest close
        df = fetch_one(ticker, period="5d")
        if df.empty:
            # Not yet listed, or already expired and delisted by Yahoo.
            logger.debug("  No data for %s — contract not listed", ticker)
            continue

        latest_close = df["Close"].iloc[-1]
        if pd.notna(latest_close):
            rows.append({
                "commodity": commodity,
                "contract_month": contract["contract_month"].isoformat(),
                "label": contract["label"],
                "ticker": ticker,
                "close": float(latest_close),
                "observation_date": _observation_date(df, ticker),
                # Per-contract session volume, where the provider gave one.
                # It was previously discarded, which left the workstation
                # unable to say anything about which month is liquid — the
                # question a hedger asks before choosing a hedge month.
                # Open interest is not published by any source this project
                # ingests, so the column is written NULL rather than derived
                # from volume (Phase 3).
                "volume": _latest_volume(df),
                "open_interest": None,
            })

    if not rows:
        logger.warning("No forward curve data for %s", commodity)
        return pd.DataFrame()

    result = _enforce_single_observation_date(pd.DataFrame(rows), commodity)
    if result.empty:
        logger.warning("No forward curve data for %s", commodity)
        return pd.DataFrame()

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
                "  Got %d contracts for %s as of %s: %s → %s",
                len(df), commodity, df["observation_date"].iloc[0],
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
