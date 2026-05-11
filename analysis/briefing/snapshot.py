"""Snapshot extractor — distills a BriefingData into a JSON-friendly dict.

The snapshot is what gets stored in the `briefings.snapshot_json` column.
It captures the structured numbers behind the briefing so future runs
can backtest which signals preceded large price moves without re-parsing
the formatted text. Shape:

    {
        "prices": {commodity: {close, daily_pct_change, rsi, macd_histogram,
                               ma_50, ma_200, hv_20}},
        "crush_spread": {value_cents, value_usd_mt} | None,
        "cot": {commodity: {date, commercial_net, noncommercial_net}},
    }

Signals are stored separately in `briefings.signals_json` — they already
come pre-typed off `BriefingData.signals`.
"""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from analysis.briefing.types import BriefingData
from analysis.spreads import compute_crush_spread
from pipeline.query import read_cot
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)


_TECH_FIELDS = (
    ("close", "Close"),
    ("daily_pct_change", "daily_pct_change"),
    ("rsi", "RSI"),
    ("macd_histogram", "MACD_Histogram"),
    ("ma_50", "MA_50"),
    ("ma_200", "MA_200"),
    ("hv_20", "HV_20"),
)


def _latest_technicals(enriched: dict[str, pd.DataFrame]) -> dict[str, dict[str, float | None]]:
    """Pull the latest-row technicals from each enriched price frame."""
    out: dict[str, dict[str, float | None]] = {}
    for commodity, df in enriched.items():
        if df.empty:
            continue
        latest = df.iloc[-1]
        row: dict[str, float | None] = {}
        for out_key, col in _TECH_FIELDS:
            val = latest.get(col, None)
            row[out_key] = None if val is None or pd.isna(val) else float(val)
        out[commodity] = row
    return out


def _latest_crush(price_data: dict[str, pd.DataFrame]) -> dict[str, float | None] | None:
    """Compute latest crush spread from raw price frames. None if inputs missing."""
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    oil = price_data.get("Soybean Oil", pd.DataFrame())
    meal = price_data.get("Soybean Meal", pd.DataFrame())
    if soybeans.empty or oil.empty or meal.empty:
        return None
    try:
        spread = compute_crush_spread(soybeans, oil, meal)
    except Exception as exc:
        logger.debug("Crush spread snapshot failed: %s", exc)
        return None
    if spread.empty:
        return None
    cents = float(spread.iloc[-1]["crush_spread"])
    usd_mt = to_metric_tons(cents, "Soybeans")
    return {
        "value_cents": cents,
        "value_usd_mt": None if usd_mt is None else float(usd_mt),
    }


def _latest_cot() -> dict[str, dict[str, Any]]:
    """Read COT from the DB and return the latest row per commodity."""
    try:
        cot = read_cot()
    except Exception as exc:
        logger.debug("COT snapshot read failed: %s", exc)
        return {}
    if cot.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in cot.groupby("commodity"):
        latest = subset.sort_values("Date").iloc[-1]
        commercial_net = latest.get("commercial_net")
        noncommercial_net = latest.get("noncommercial_net")
        out[str(commodity)] = {
            "date": str(latest["Date"].date()) if pd.notna(latest["Date"]) else None,
            "commercial_net": None if pd.isna(commercial_net) else float(commercial_net),
            "noncommercial_net": None if pd.isna(noncommercial_net) else float(noncommercial_net),
        }
    return out


def build_snapshot(data: BriefingData) -> dict[str, Any]:
    """Distill a BriefingData into the dict written to briefings.snapshot_json."""
    return {
        "prices": _latest_technicals(data.enriched),
        "crush_spread": _latest_crush(data.price_data),
        "cot": _latest_cot(),
    }
