"""STOCKS-TO-USE section — US balance sheet from PSD.

PSD (Layer 6) is the machine-readable form of WASDE's US balance
sheet, so we source ending stocks + total use from there instead of
the broken NASS WASDE fetch. The section also emits one signal per
commodity whose latest marketing-year ratio is below the prior 5-yr
low.
"""

from __future__ import annotations

from analysis.stocks_to_use import (
    HISTORY_WINDOW,
    MIN_HISTORY_YEARS,
    compute_stocks_to_use,
    detect_tight_supply,
)
from config import WASDE_COMMODITIES
from pipeline.query import read_psd

# config.WASDE_COMMODITIES uses NASS-style uppercase names; PSD uses
# title-case. Map between them so the briefing's commodity universe
# stays consistent with the rest of the project.
_NAME_MAP = {
    "SOYBEANS": "Soybeans",
    "CORN":     "Corn",
    "WHEAT":    "Wheat",
    "COTTON":   "Cotton",
}


def format() -> tuple[str, list[dict]]:  # noqa: A001
    """Return (text, signals) — orchestrator extends the briefing signal list."""
    psd = read_psd()
    stu = compute_stocks_to_use(psd, country="United States")
    if stu.empty:
        return "STOCKS-TO-USE (US): No data", []

    psd_names = [_NAME_MAP[c] for c in WASDE_COMMODITIES if c in _NAME_MAP]
    signals = detect_tight_supply(stu, commodities=psd_names)
    tight = {s["commodity"] for s in signals}

    lines = ["STOCKS-TO-USE (US balance sheet, source: PSD):"]
    for psd_name in psd_names:
        rows = stu[stu["commodity"] == psd_name].sort_values("year")
        if rows.empty:
            lines.append(f"  {psd_name}: No data")
            continue

        current = rows.iloc[-1]
        history = rows.iloc[-(1 + HISTORY_WINDOW):-1]

        current_ratio = float(current["ratio"])
        my = int(current["year"])
        parts = [f"{current_ratio * 100:.1f}% (MY {my})"]

        if len(history) >= MIN_HISTORY_YEARS:
            lo = float(history["ratio"].min())
            hi = float(history["ratio"].max())
            parts.append(
                f"prior {len(history)}-yr range: {lo * 100:.1f}%–{hi * 100:.1f}%"
            )

        prefix = "[TIGHT] " if psd_name in tight else ""
        lines.append(f"  {prefix}{psd_name}: {' | '.join(parts)}")

    return "\n".join(lines), signals
