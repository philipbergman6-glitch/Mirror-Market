"""STOCKS-TO-USE section — US and world balance sheets from PSD.

PSD (Layer 6) is the machine-readable form of WASDE's balance sheet, so
we source ending stocks + total use from there instead of the broken NASS
WASDE fetch. The section also emits one signal per commodity whose latest
marketing-year US ratio is below the prior 5-yr low.

The world block (M15 #237) is a *different statistic* printed in the same
units, so it carries its own footnote naming the region, the denominator,
and whether WASDE's grain adjustment was applied. Signals stay on the US
block: a world balance sheet moves once a month, not on a desk's timescale.
"""

from __future__ import annotations

from analysis.stocks_to_use import (
    HISTORY_WINDOW,
    MIN_HISTORY_YEARS,
    WORLD,
    WORLD_COMMODITIES,
    WORLD_GRAIN_ADJUSTMENT,
    WORLD_LESS_CHINA,
    compute_stocks_to_use,
    denominator_note,
    detect_tight_supply,
)
from pipeline.query import read_psd

# US balance sheets shown in the briefing (PSD title-case names). The
# first four mirror config.WASDE_COMMODITIES; Soybean Meal and Soybean
# Oil come from PSD only — WASDE's NASS-style list omits them but their
# US balance sheets matter as much to the soy complex as the beans.
_S2U_COMMODITIES = (
    "Soybeans",
    "Corn",
    "Wheat",
    "Cotton",
    "Soybean Meal",
    "Soybean Oil",
)


def _world_block(psd) -> list[str]:
    """The world / world-less-China ratios, with their own footnote."""
    world = compute_stocks_to_use(
        psd, country=WORLD, wasde_grain_adjustment=WORLD_GRAIN_ADJUSTMENT
    )
    if world.empty:
        return []
    less_china = compute_stocks_to_use(
        psd, country=WORLD_LESS_CHINA,
        wasde_grain_adjustment=WORLD_GRAIN_ADJUSTMENT,
    )

    lines = ["", "STOCKS-TO-USE (world balance sheet, source: PSD):"]
    for psd_name in WORLD_COMMODITIES:
        rows = world[world["commodity"] == psd_name].sort_values("year")
        if rows.empty:
            continue
        current = rows.iloc[-1]
        my = int(current["year"])
        parts = [f"{float(current['ratio']) * 100:.1f}% (MY {my})"]

        ex_china = less_china[
            (less_china["commodity"] == psd_name) & (less_china["year"] == my)
        ]
        if not ex_china.empty:
            parts.append(
                f"less China {float(ex_china.iloc[0]['ratio']) * 100:.1f}%"
            )

        history = rows.iloc[-(1 + HISTORY_WINDOW):-1]
        if len(history) >= MIN_HISTORY_YEARS:
            lo = float(history["ratio"].min())
            hi = float(history["ratio"].max())
            parts.append(
                f"prior {len(history)}-yr range: {lo * 100:.1f}%–{hi * 100:.1f}%"
            )
        lines.append(f"  {psd_name}: {' | '.join(parts)}")

    if len(lines) == 2:
        return []
    lines.append(
        f"  Basis: {denominator_note(WORLD, wasde_grain_adjustment=WORLD_GRAIN_ADJUSTMENT)}"
    )
    return lines


def format() -> tuple[str, list[dict]]:  # noqa: A001
    """Return (text, signals) — orchestrator extends the briefing signal list."""
    psd = read_psd()
    stu = compute_stocks_to_use(psd, country="United States")
    if stu.empty:
        world_only = _world_block(psd)
        if world_only:
            return "\n".join(["STOCKS-TO-USE (US): No data", *world_only]), []
        return "STOCKS-TO-USE (US): No data", []

    psd_names = list(_S2U_COMMODITIES)
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

    lines.extend(_world_block(psd))
    return "\n".join(lines), signals
