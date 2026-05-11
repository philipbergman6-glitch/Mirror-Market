"""Phase 2 spike — yfinance back-contract availability probe.

Standalone, read-only. Validates whether yfinance returns clean OHLCV history
for individual ag futures contracts (ZSN26.CBT etc.) so we can decide whether
to proceed with a Panama-adjusted continuous series (Phase 3) or pivot to a
paid data source.

Run:
    python scripts/spike_back_contracts.py

Output: per-contract availability table + a single-line verdict per contract
(pass / fail) + an overall decision (green-light / narrow / stop).

Decision rule:
    All 6 pass         → green-light Phase 3 (soy complex roll implementation)
    1-2 fail           → narrow Phase 3 scope to whichever legs work
    >= 3 fail          → stop; revisit data sourcing before any further work
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

import pandas as pd
import yfinance as yf

SPIKE_TICKERS = [
    "ZSF26.CBT",   # Soybeans Jan 2026
    "ZSH26.CBT",   # Soybeans Mar 2026
    "ZSK26.CBT",   # Soybeans May 2026
    "ZSN26.CBT",   # Soybeans Jul 2026
    "ZSX25.CBT",   # Soybeans Nov 2025 (recently expired)
    "ZLZ25.CBT",   # Soybean Oil Dec 2025 (different root + recently expired)
]

MIN_LIQUID_ROWS = 120        # ~6 months of trading days
MAX_INNER_GAP_BDAYS = 5      # tolerated gap within liquid window


@dataclass
class ContractReport:
    ticker: str
    row_count: int
    pct_null_close: float
    pct_nonzero_volume: float
    longest_inner_gap_bdays: int
    open_interest_field_present: bool
    verdict: str  # "pass" or "fail"
    reason: str


def probe(ticker: str) -> ContractReport:
    try:
        t = yf.Ticker(ticker)
        df = t.history(period="1y", auto_adjust=False)
    except Exception as exc:  # noqa: BLE001 — probe must never crash
        return ContractReport(
            ticker=ticker, row_count=0, pct_null_close=100.0, pct_nonzero_volume=0.0,
            longest_inner_gap_bdays=0, open_interest_field_present=False,
            verdict="fail", reason=f"history() raised: {exc.__class__.__name__}: {exc}",
        )

    if df is None or df.empty:
        return ContractReport(
            ticker=ticker, row_count=0, pct_null_close=100.0, pct_nonzero_volume=0.0,
            longest_inner_gap_bdays=0, open_interest_field_present=False,
            verdict="fail", reason="empty DataFrame returned",
        )

    df = df.copy()
    row_count = len(df)
    pct_null_close = 100.0 * df["Close"].isna().mean() if "Close" in df else 100.0
    pct_nonzero_volume = (
        100.0 * (df["Volume"].fillna(0) > 0).mean() if "Volume" in df else 0.0
    )

    longest_gap = _longest_inner_bday_gap(df.index)

    # yfinance occasionally exposes openInterest on .info; cheap to probe.
    oi_present = False
    try:
        info = t.info or {}
        oi_present = bool(info.get("openInterest"))
    except Exception:  # noqa: BLE001
        oi_present = False

    fail_reasons: list[str] = []
    if row_count < MIN_LIQUID_ROWS:
        fail_reasons.append(f"row_count={row_count} < {MIN_LIQUID_ROWS}")
    if pct_null_close > 5.0:
        fail_reasons.append(f"null_close={pct_null_close:.1f}% > 5%")
    if pct_nonzero_volume < 80.0:
        fail_reasons.append(f"nonzero_volume={pct_nonzero_volume:.1f}% < 80%")
    if longest_gap > MAX_INNER_GAP_BDAYS:
        fail_reasons.append(f"longest_gap={longest_gap}bd > {MAX_INNER_GAP_BDAYS}bd")

    verdict = "pass" if not fail_reasons else "fail"
    reason = "" if verdict == "pass" else "; ".join(fail_reasons)

    return ContractReport(
        ticker=ticker, row_count=row_count, pct_null_close=pct_null_close,
        pct_nonzero_volume=pct_nonzero_volume, longest_inner_gap_bdays=longest_gap,
        open_interest_field_present=oi_present, verdict=verdict, reason=reason,
    )


def _longest_inner_bday_gap(idx: pd.DatetimeIndex) -> int:
    if len(idx) < 2:
        return 0
    idx = idx.sort_values()
    full = pd.bdate_range(idx[0], idx[-1])
    present = set(idx.normalize())
    longest = 0
    current = 0
    for d in full:
        if d in present:
            longest = max(longest, current)
            current = 0
        else:
            current += 1
    longest = max(longest, current)
    return longest


def main() -> int:
    reports = [probe(t) for t in SPIKE_TICKERS]

    header = (
        f"{'ticker':<14} {'rows':>6} {'null%':>7} {'vol%':>7} "
        f"{'gap(bd)':>8} {'OI':>4} {'verdict':<8} reason"
    )
    print(header)
    print("-" * len(header))
    for r in reports:
        print(
            f"{r.ticker:<14} {r.row_count:>6} {r.pct_null_close:>6.1f}% "
            f"{r.pct_nonzero_volume:>6.1f}% {r.longest_inner_gap_bdays:>8} "
            f"{('yes' if r.open_interest_field_present else 'no'):>4} "
            f"{r.verdict:<8} {r.reason}"
        )

    failures = sum(1 for r in reports if r.verdict == "fail")
    print()
    if failures == 0:
        decision = "GREEN-LIGHT — proceed with Phase 3 (soy complex roll implementation)"
        exit_code = 0
    elif failures <= 2:
        failing = ", ".join(r.ticker for r in reports if r.verdict == "fail")
        decision = (
            f"NARROW — {failures} contract(s) failed ({failing}). "
            f"Phase 3 should drop those legs; soy complex coverage may be incomplete."
        )
        exit_code = 1
    else:
        decision = (
            f"STOP — {failures} of {len(reports)} contracts failed. "
            f"Do not start Phase 3; revisit data sourcing (Barchart / Nasdaq Data Link / CME direct)."
        )
        exit_code = 2

    print(f"DECISION: {decision}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
