"""CRUSH SPREAD section — the named-contract board crush + NASS actual crush volume.

The line used to read ``compute_crush_spread`` over the continuous ``ZS=F`` /
``ZM=F`` / ``ZL=F`` series: three front months whose underlying contract Yahoo
changes silently, on three schedules that need not agree. A briefing sentence
built from that names no contract, and on a roll day it reports a move nobody
earned.

It now reads the same calculation Origins, the Workstation and the Opportunity
board read — ``analysis.futures.crush.named_board_crush`` — so the four
surfaces cannot print four different crushes, and it names the three contracts
in the line. When the named legs cannot be had on one session the line says so
instead of falling back to the series.
"""

import logging
from datetime import date, datetime, timezone

import pandas as pd

from analysis.futures.crush import CrushWithheld, named_board_crush
from analysis.nass_crush import latest_crush

logger = logging.getLogger(__name__)


def _nass_crush_line() -> str:
    """One line with the latest NASS monthly crush volume + YoY, '' if absent."""
    crush = latest_crush()
    if not crush:
        return ""
    line = (
        f"  US crush (NASS): {crush['value']:,.0f} {crush['unit']}".rstrip()
        + f" in {crush['period']} {crush['year']}"
    )
    if crush["yoy_pct"] is not None:
        sign = "+" if crush["yoy_pct"] >= 0 else ""
        line += f" ({sign}{crush['yoy_pct']:.1f}% YoY)"
    return line


def format(price_data: dict[str, pd.DataFrame] | None = None, *, today: date | None = None) -> str:  # noqa: A001
    """``price_data`` is accepted and unused — the crush no longer reads it.

    Kept in the signature because the orchestrator passes it positionally to
    every price-driven section, and dropping the parameter would make this the
    one section with a different shape. The argument being ignored is the
    point: the continuous series is no longer an input to this number.
    """
    del price_data
    spread_line = _spread_line(today or datetime.now(timezone.utc).date())
    try:
        nass_line = _nass_crush_line()
    except Exception as exc:
        logger.debug("NASS crush line error: %s", exc)
        nass_line = ""
    return "\n".join(part for part in (spread_line, nass_line) if part)


def _spread_line(today: date) -> str:
    # Through ``pipeline.query`` rather than ``pipeline.connection`` directly:
    # that is the module every other briefing section reads the database
    # through, so this section resolves to the same database they do.
    from pipeline.connection import managed_connection
    from pipeline.query import get_connection

    try:
        with managed_connection(get_connection()) as conn:
            from analysis.futures.providers import open_provider

            outcome = named_board_crush(open_provider(conn), as_of=today)
    except Exception as exc:
        logger.debug("Crush spread error: %s", exc)
        return "CRUSH SPREAD: Calculation error"

    if isinstance(outcome, CrushWithheld):
        return f"CRUSH SPREAD: withheld — {outcome.reason}"

    share = outcome.oil_value_share
    profitability = "processors profitable" if outcome.margin_usd_mt > 0 else "margin squeeze"
    return (
        f"CRUSH SPREAD: ${outcome.margin_usd_mt:,.1f}/MT on "
        f"{'/'.join(leg.symbol for leg in outcome.legs)} "
        f"({outcome.observation_date.isoformat()} — {profitability}"
        + (f", oil share {share:.0%}" if share is not None else "")
        + f"; {outcome.level.label.lower()})"
    )
