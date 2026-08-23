"""
Layer 11b — Named-contract daily bars via yfinance.

Layer 11 reads each contract's *latest* close to build today's forward
curve. This layer captures each contract's *full daily history* — the raw
material for a continuous series whose roll dates are our own written rule
(A4, #301) rather than Yahoo's unannounced front-month switching.

Why capture, not recompute: Yahoo serves deep per-contract history while a
contract is listed (measured 2026-08-23: ZSX26.CBT — 946 rows back to
2022-11-14) and then drops the symbol on a schedule nobody publishes.
ZSN26.CBT, expired July 2026, already answers 404; ZSX25.CBT, expired four
months earlier, still serves 1007 rows. Retention is not an age window, so
coverage can only be established by asking, and what is reachable today is
a one-time capture — which is why `contract_bars` round-trips through
`data/history/` (invariant 6) instead of relying on re-download.

Two ticker rosters per commodity, treated differently:

- **Listed contracts** (the same forward-looking roster Layer 11 uses):
  fetched every run with the full retry budget, `period="max"`, through the
  settlement guard. These self-heal; the daily re-fetch also extends each
  contract's history by one bar.
- **Expired candidates** (trading months from the recent past): probed only
  while the store holds no rows for them, with a single attempt — an empty
  answer here usually means "delisted", an expected daily outcome, not a
  transient worth three backoff sleeps. A probe that lands before Yahoo
  drops the symbol banks the whole history; one that lands after records
  nothing, and the near-roll overlap that contract would have provided is
  simply absent — the stitcher withholds rather than bridges (invariant 2).
"""

import logging
from datetime import date
from typing import Any

import pandas as pd

from config import FORWARD_CURVE_CONTRACTS, MONTH_CODES
from fetchers.forward_curve import _build_contract_tickers
from fetchers.yfinance import fetch_one

logger = logging.getLogger(__name__)

#: How many calendar months back the expired-contract probe reaches.
#: Beyond this, measurement says the symbols are gone (ZSX24 and older:
#: nothing), so probing further buys requests, not data.
EXPIRED_LOOKBACK_MONTHS = 6

#: Forward roster size — matches Layer 11's default so the two layers
#: always agree on what "listed" means.
NUM_LISTED_CONTRACTS = 6


def _expired_candidates(root: str, exchange: str, trading_months: list[int],
                        today: date) -> list[dict]:
    """Contracts whose delivery month fell in the recent past.

    Walks EXPIRED_LOOKBACK_MONTHS calendar months back from today and keeps
    the ones this commodity trades. The current month is *not* here — it is
    a listed candidate (it trades into its own delivery month) and belongs
    to the forward roster.
    """
    candidates: list[dict] = []
    year, month = today.year, today.month
    for _ in range(EXPIRED_LOOKBACK_MONTHS):
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        if month not in trading_months:
            continue
        month_code = MONTH_CODES[month]
        contract_date = date(year, month, 1)
        candidates.append({
            "ticker": f"{root}{month_code}{str(year)[-2:]}.{exchange}",
            "contract_month": contract_date,
            "label": contract_date.strftime("%b %Y"),
        })
    return candidates


def _bars_rows(df: pd.DataFrame, commodity: str, contract: dict) -> list[dict]:
    """Long-form rows for one contract's daily bars. NaN closes are skipped
    here — a bar with no close is not a session this contract priced."""
    rows: list[dict] = []
    for stamp, bar in df.iterrows():
        close = bar.get("Close")
        if pd.isna(close):
            continue
        stamp_any: Any = stamp
        day = pd.Timestamp(stamp_any)
        if pd.isna(day):
            continue
        volume = bar.get("Volume")
        rows.append({
            "commodity": commodity,
            "ticker": contract["ticker"],
            "contract_month": contract["contract_month"].isoformat(),
            "Date": day.date().isoformat(),
            "Close": float(close),
            # None, not 0.0 — "the provider gave no volume" and "none
            # traded" are different facts (same rule as Layer 11).
            "Volume": None if pd.isna(volume) else float(volume),
        })
    return rows


def fetch_contract_bars(commodity: str, *, already_captured: frozenset[str] = frozenset(),
                        today: date | None = None) -> pd.DataFrame:
    """Daily bar history for one commodity's named contracts.

    Parameters
    ----------
    commodity : str
        Key in FORWARD_CURVE_CONTRACTS.
    already_captured : frozenset[str]
        Tickers the store already holds rows for. Expired candidates in this
        set are skipped — their history cannot change — while listed
        contracts are always re-fetched, because theirs grows daily.
    today : date | None
        Clock injection point for tests; defaults to today.

    Returns
    -------
    pd.DataFrame
        Columns: commodity, ticker, contract_month, Date, Close, Volume.
        One row per (ticker, session). Empty if unconfigured or nothing
        answered.
    """
    spec = FORWARD_CURVE_CONTRACTS.get(commodity)
    if not spec:
        logger.warning("No contract config for %s", commodity)
        return pd.DataFrame()

    today = today or date.today()
    listed = _build_contract_tickers(
        root=spec["root"], exchange=spec["exchange"],
        trading_months=spec["months"], num_contracts=NUM_LISTED_CONTRACTS,
        today=today,
    )
    expired = _expired_candidates(
        spec["root"], spec["exchange"], spec["months"], today,
    )

    rows: list[dict] = []
    for contract in listed:
        df = fetch_one(contract["ticker"], period="max")
        if df.empty:
            # Not yet listed, or expired mid-roster — Layer 11's rule: the
            # data decides, not an expiry-calendar estimate.
            logger.debug("  %s: no bars — not listed", contract["ticker"])
            continue
        rows.extend(_bars_rows(df, commodity, contract))

    for contract in expired:
        if contract["ticker"] in already_captured:
            continue
        df = fetch_one(contract["ticker"], period="max", max_retries=1)
        if df.empty:
            logger.debug(
                "  %s: expired and not served — history not capturable", contract["ticker"]
            )
            continue
        logger.info(
            "  %s: captured %d bars from an expired contract while Yahoo still serves it",
            contract["ticker"], len(df),
        )
        rows.extend(_bars_rows(df, commodity, contract))

    if not rows:
        logger.warning("No contract bars for %s", commodity)
        return pd.DataFrame()
    return pd.DataFrame(rows)


def fetch_all_contract_bars(
    already_captured: frozenset[str] = frozenset(),
) -> dict[str, pd.DataFrame]:
    """{commodity: bars frame} for every configured commodity."""
    results: dict[str, pd.DataFrame] = {}
    for commodity in FORWARD_CURVE_CONTRACTS:
        logger.info("Fetching named-contract history for %s ...", commodity)
        df = fetch_contract_bars(commodity, already_captured=already_captured)
        results[commodity] = df
        if not df.empty:
            logger.info(
                "  Got %d bars across %d contracts (%s → %s)",
                len(df), df["ticker"].nunique(), df["Date"].min(), df["Date"].max(),
            )
    return results
