"""Orchestrator — loads shared data and joins section output into the daily briefing.

Two public entry points:
  - `generate_briefing()`        returns the formatted text (back-compat)
  - `generate_briefing_data()`   returns a typed `BriefingData` with text,
                                  per-section text blocks, signals, and
                                  the shared price/currency DataFrames.

`generate_briefing()` is a thin wrapper that returns `BriefingData.text`.
"""

import logging
from datetime import date

from analysis.briefing.sections import (
    basis,
    conab,
    correlations,
    cot,
    crop_progress,
    crush,
    currencies,
    dce,
    economic,
    eia,
    emerging_markets,
    export_sales,
    forward_curve,
    freshness,
    inspections,
    market_drivers,
    prices,
    psd,
    seasonal,
    signals,
    stocks_to_use,
    usda,
    wasde,
    weather,
    worldbank,
)
from analysis.briefing.snapshot import build_snapshot
from analysis.briefing.types import BriefingData
from analysis.loaders import load_currencies, load_prices
from analysis.signals import demote_near_roll_signals
from pipeline.store import save_briefing

logger = logging.getLogger(__name__)


def generate_briefing_data(*, archive: bool = True) -> BriefingData:
    """Build the full briefing as structured data + the joined text block.

    If `archive` is True (default), the briefing and a structured
    snapshot are written to the `briefings` table keyed on today's date.
    Archive failures are logged but do not raise — the briefing is
    still returned to the caller.
    """
    today = date.today().strftime("%Y-%m-%d")
    header = (
        f"=== Mirror Market Daily Briefing — {today} ===\n"
        "NOTE: Data is informational only and should not be used as sole basis for trading decisions.\n"
        "Some data layers may be delayed or unavailable. Check Data Freshness below."
    )

    price_data = load_prices()
    currency_data = load_currencies()

    # Sections that depend on shared inputs run first so we can pass the
    # enriched price frames + signals on to the cross-cutting sections.
    price_text, signal_list, enriched = prices.format(price_data)

    stu_text, stu_signals = stocks_to_use.format()
    signal_list.extend(stu_signals)

    # Demote near-roll technicals ONCE, before the list fans out — the signals
    # section, BriefingData.signals, signals_json, and the snapshot must all
    # report the same severities.
    signal_list = demote_near_roll_signals(signal_list)

    section_texts: dict[str, str] = {
        "freshness": freshness.format(),
        "prices": price_text,
        "crush": crush.format(price_data),
        "basis": basis.format(price_data, currency_data),
        "fred": economic.format_fred(),
        "usda": usda.format(),
        "crop_progress": crop_progress.format(),
        "yield_curve": economic.format_yield_curve(),
        "wasde": wasde.format(),
        "stocks_to_use": stu_text,
        "export_sales": export_sales.format(),
        "inspections": inspections.format(),
        "dce": dce.format(price_data),
        "forward_curve": forward_curve.format(),
        "eia": eia.format(),
        "conab": conab.format(),
        "currencies": currencies.format(currency_data),
        "cot": cot.format(),
        "weather": weather.format(),
        "psd": psd.format(),
        "worldbank": worldbank.format(),
        "emerging_markets": emerging_markets.format(),
        "correlations": correlations.format(price_data, currency_data),
        "seasonal": seasonal.format(price_data),
        "market_drivers": market_drivers.format(price_data, enriched, currency_data),
        "signals": signals.format(signal_list),
    }

    text = _assemble_text(header, section_texts)

    data = BriefingData(
        text=text,
        section_texts=section_texts,
        signals=signal_list,
        enriched=enriched,
        price_data=price_data,
        currency_data=currency_data,
    )

    if archive:
        try:
            save_briefing(
                briefing_date=today,
                text=text,
                signals=signal_list,
                snapshot=build_snapshot(data),
            )
        except Exception:
            logger.exception("Failed to archive briefing for %s", today)

    return data


def generate_briefing(*, archive: bool = True) -> str:
    """Return the formatted briefing text. Thin wrapper around `generate_briefing_data()`."""
    return generate_briefing_data(archive=archive).text


# ---------------------------------------------------------------------------
# Section ordering — defines the layout of the final briefing.
# Sections that emit "" are skipped (freshness, yield_curve when no data).
# ---------------------------------------------------------------------------

_SECTION_ORDER: tuple[str, ...] = (
    "freshness",
    "prices",
    "crush",
    "basis",
    "fred",
    "usda",
    "crop_progress",
    "yield_curve",
    "wasde",
    "stocks_to_use",
    "export_sales",
    "inspections",
    "dce",
    "forward_curve",
    "eia",
    "conab",
    "currencies",
    "cot",
    "weather",
    "psd",
    "worldbank",
    "emerging_markets",
    "correlations",
    "seasonal",
    "market_drivers",
    "signals",
)

# These sections are optional — when their content is empty we drop the
# trailing blank line too. Every other section emits a header even with
# no data, so a blank line is always wanted after them.
_SKIP_WHEN_EMPTY = frozenset({"freshness", "yield_curve"})


def _assemble_text(header: str, section_texts: dict[str, str]) -> str:
    parts: list[str] = [header, ""]
    for name in _SECTION_ORDER:
        body = section_texts.get(name, "")
        if not body and name in _SKIP_WHEN_EMPTY:
            continue
        parts.append(body)
        if name != _SECTION_ORDER[-1]:  # no trailing blank after the final section
            parts.append("")
    return "\n".join(parts)


if __name__ == "__main__":
    from config import setup_logging

    setup_logging()
    print(generate_briefing())
