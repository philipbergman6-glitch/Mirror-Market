"""BRAZIL BASIS section — Paranaguá FOB (primary) + CEPEA Paraná (secondary) vs CBOT in USD/MT."""

import logging

import pandas as pd

from analysis.spreads import compute_brazil_basis
from pipeline.query import read_brazil_spot

logger = logging.getLogger(__name__)


def _basis_for_source(
    soybeans: pd.DataFrame,
    brl_usd: pd.DataFrame,
    commodity: str,
) -> pd.DataFrame | None:
    try:
        spot = read_brazil_spot(commodity)
    except Exception as exc:
        logger.debug("Brazil spot read failed for %s: %s", commodity, exc)
        return None
    if spot.empty:
        return None
    try:
        df = compute_brazil_basis(soybeans, spot, brl_usd)
    except Exception as exc:
        logger.debug("Brazil basis computation failed for %s: %s", commodity, exc)
        return None
    return df if not df.empty else None


def _format_basis_line(label: str, basis_df: pd.DataFrame) -> str:
    latest = float(basis_df.iloc[-1]["basis_usd_mt"])
    direction = "discount" if latest < 0 else "premium"
    if len(basis_df) >= 6:
        prev = float(basis_df.iloc[-6]["basis_usd_mt"])
        trend = "widening" if abs(latest) > abs(prev) else "narrowing"
        return f"BRAZIL BASIS ({label} vs CBOT): ${latest:+,.1f}/MT (Brazilian {direction}, {trend})"
    return f"BRAZIL BASIS ({label} vs CBOT): ${latest:+,.1f}/MT (Brazilian {direction})"


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    brl_usd = currency_data.get("BRL/USD", pd.DataFrame())

    if soybeans.empty or brl_usd.empty:
        return "BRAZIL BASIS: Insufficient data"

    agrural_basis = _basis_for_source(soybeans, brl_usd, "Soybean (AgRural Paranaguá FOB)")
    cepea_basis = _basis_for_source(soybeans, brl_usd, "Soybean (CEPEA)")

    if agrural_basis is None and cepea_basis is None:
        return "BRAZIL BASIS: Insufficient data"

    if agrural_basis is None:
        return _format_basis_line("CEPEA Paraná", cepea_basis)

    headline = _format_basis_line("Paranaguá FOB", agrural_basis)
    if cepea_basis is None:
        return headline

    agrural_current = float(agrural_basis.iloc[-1]["basis_usd_mt"])
    cepea_current = float(cepea_basis.iloc[-1]["basis_usd_mt"])
    wedge = agrural_current - cepea_current
    secondary = (
        f"  └ Farm-gate (CEPEA Paraná): ${cepea_current:+,.1f}/MT "
        f"— port-vs-farm wedge ${wedge:+,.1f}/MT"
    )
    return f"{headline}\n{secondary}"
