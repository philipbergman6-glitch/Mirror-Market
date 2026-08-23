"""
Layer 11b — per-contract daily close history via yfinance (#332).

Layer 11 stores one curve snapshot per run, so a contract's stored history
only reaches back to the day snapshots began (2026-07-30). This layer asks
Yahoo for each active contract's *own* daily series — months of session
closes per named month — which is what the workstation's contract-row chart
draws.

Why this is a separate layer rather than a deeper Layer 11 fetch: the curve
is a one-session snapshot with a single-observation-date rule; a history is
the opposite shape (many sessions, one contract). Mixing them would put two
grading regimes in one table.

Key properties:
    - Same ticker grammar as Layer 11 (``{root}{month_code}{yy}.{exchange}``)
      via the one builder in fetchers/forward_curve.py — never re-derived.
    - Same settlement-guarded path (fetch_one → drop_unsettled_session), so
      today's unfinished bar never lands (invariant 11).
    - A named contract has no roll-day discontinuity — that trap is specific
      to continuous front-month tickers (LAYERS.md, Layer 1).
    - Yahoo delists an expired contract outright (empty frame, not stale
      bars), so an expired month drops out at the fetch and its history
      leaves the ephemeral CI database with it. Accepted: the chart only
      renders contracts on the *current* curve, which are alive by
      construction.
    - Known cost, accepted: ``download_bars`` treats an empty answer as
      possible throttling and burns all three retries with backoff on a
      delisted ticker — up to one dead ticker per commodity for the back
      half of its delivery month (~20s of sleep per run at worst), *on top
      of* Layer 11 paying the same toll for the same tickers minutes
      earlier. The alternative — retrying delisted and throttled tickers
      differently — would need to tell two identical empty frames apart.
"""

import logging
from datetime import date

import pandas as pd

from config import (
    CONTRACT_HISTORY_COMMODITIES,
    CONTRACT_HISTORY_PERIOD,
    FORWARD_CURVE_CONTRACTS,
)
from fetchers.forward_curve import _build_contract_tickers
from fetchers.yfinance import fetch_one

logger = logging.getLogger(__name__)


def fetch_contract_history(commodity: str, today: date | None = None) -> pd.DataFrame:
    """Daily close history for every active contract of ``commodity``.

    Returns a long frame — one row per (ticker, session) — with columns:
    ticker, contract_month (ISO first-of-month), label, date (ISO session),
    close, volume. Empty frame when no contract answered.
    """
    spec = FORWARD_CURVE_CONTRACTS[commodity]
    contracts = _build_contract_tickers(
        spec["root"], spec["exchange"], spec["months"], today=today
    )
    frames: list[pd.DataFrame] = []
    for contract in contracts:
        ticker = contract["ticker"]
        bars = fetch_one(ticker, period=CONTRACT_HISTORY_PERIOD)
        if bars.empty or "Close" not in bars.columns:
            # An expired month is delisted (empty), and the current delivery
            # month is a candidate that may already have died mid-month —
            # either way the absence is the answer, not an error.
            logger.info("%s: no bars returned — expired or delisted", ticker)
            continue
        frames.append(pd.DataFrame({
            "ticker": ticker,
            "contract_month": contract["contract_month"].isoformat(),
            "label": contract["label"],
            "date": [pd.Timestamp(ts).strftime("%Y-%m-%d") for ts in bars.index],
            "close": bars["Close"].to_numpy(),
            "volume": (
                bars["Volume"].to_numpy() if "Volume" in bars.columns
                else [None] * len(bars)
            ),
        }))
    if not frames:
        logger.warning("%s: no contract returned any history", commodity)
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    logger.info(
        "%s: %d sessions across %d contracts", commodity, len(result), len(frames)
    )
    return result


def fetch_all_contract_history() -> dict[str, pd.DataFrame]:
    """Per-contract histories for the chart roster (soy complex)."""
    out: dict[str, pd.DataFrame] = {}
    for commodity in CONTRACT_HISTORY_COMMODITIES:
        df = fetch_contract_history(commodity)
        if not df.empty:
            out[commodity] = df
    return out
