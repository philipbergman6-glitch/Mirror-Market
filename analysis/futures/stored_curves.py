"""The stored curve, read once, for every surface outside the workstation.

The workstation opens its own provider over the connection it renders with;
the briefing (drivers, forward-curve section, snapshot) and the headline
forward-curve block read through here. Both routes end in
:func:`analysis.futures.curve.analyse_curve` over
:func:`analysis.futures.providers.open_provider`, so a Jan–Mar reading cannot
differ between the desk page and the morning briefing (#671).
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime, timezone

from analysis.futures.curve import CurveAnalysis, analyse_curve
from analysis.futures.providers import known_commodities, open_provider


def read_stored_curves(
    commodities: Iterable[str] | None = None, *, as_of: date | None = None
) -> dict[str, CurveAnalysis]:
    """Each commodity's newest stored curve at ``as_of``, analysed.

    Commodities with no stored legs are left out — "we hold no curve" is the
    caller's empty state to render, not an analysis with nothing in it.
    Through ``pipeline.query.get_connection`` so it resolves to the same
    database every other briefing section reads.
    """
    from pipeline.connection import managed_connection
    from pipeline.query import get_connection

    as_of = as_of or datetime.now(timezone.utc).date()
    wanted = tuple(commodities) if commodities is not None else known_commodities()
    out: dict[str, CurveAnalysis] = {}
    with managed_connection(get_connection()) as conn:
        provider = open_provider(conn)
        for commodity in wanted:
            analysis = analyse_curve(provider.curve(commodity, as_of=as_of), as_of=as_of)
            if not analysis.is_empty:
                out[commodity] = analysis
    return out


__all__ = ["read_stored_curves"]
