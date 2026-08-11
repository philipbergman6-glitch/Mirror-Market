"""DCE CHINESE FUTURES section — Dalian futures vs CBOT comparison.

Both legs are converted to USD/MT before comparison: DCE quotes CNY/MT,
CBOT quotes cents/bu (beans), cents/lb (oil), or $/short ton (meal) —
printing the raw closes side by side is not a comparison in any currency.
"""

import logging

import pandas as pd

from analysis.spreads import compute_dce_crush_margin
from pipeline.query import read_dce_futures
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame] | None = None,
) -> str:
    lines = ["DCE CHINESE FUTURES (USD/MT):"]
    dce_data = read_dce_futures()

    if dce_data.empty:
        return "DCE CHINESE FUTURES: No data"

    # CNY/USD spot for converting CNY/MT → USD/MT.
    cny_usd = None
    if currency_data:
        cny_df = currency_data.get("CNY/USD")
        if cny_df is not None and not cny_df.empty:
            rate = cny_df["Close"].iloc[-1]
            if pd.notna(rate) and rate > 0:
                cny_usd = float(rate)

    # No.1 (A0) has deliberately no CBOT counterpart: it is the domestic
    # non-GMO food bean, so a premium over CBOT would price a food-grade
    # spread, not import parity. No.2 (B0) is the imported/GMO bean and is
    # the only honest vs-CBOT comparison (#152).
    dce_to_cbot = {
        "DCE Soybean No.2": "Soybeans",
        "DCE Soybean Meal": "Soybean Meal",
        "DCE Soybean Oil": "Soybean Oil",
    }

    for dce_name in dce_data["commodity"].unique():
        subset = dce_data[dce_data["commodity"] == dce_name].sort_values("Date")
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        dce_close = latest["Close"]
        dce_date = latest["Date"]

        if cny_usd is not None:
            parts = [f"{dce_close * cny_usd:,.0f} USD/MT (CNY {dce_close:,.0f})"]
        else:
            parts = [f"CNY {dce_close:,.0f}/MT"]

        cbot_name = dce_to_cbot.get(dce_name)
        if cbot_name and cbot_name in price_data:
            cbot_df = price_data[cbot_name]
            if not cbot_df.empty:
                cbot_mt = to_metric_tons(cbot_df["Close"].iloc[-1], cbot_name)
                parts.append(f"vs CBOT {cbot_mt:,.0f} USD/MT")
                if cny_usd is not None and cbot_mt and cbot_mt > 0:
                    premium = (dce_close * cny_usd) - cbot_mt
                    parts.append(f"premium {premium:+,.0f}")

        lines.append(
            f"  {dce_name}: {' | '.join(parts)} "
            f"(as of {dce_date.date() if hasattr(dce_date, 'date') else dce_date})"
        )

    try:
        lines.extend(_board_crush_lines(dce_data, cny_usd))
    except Exception as exc:
        logger.debug("DCE board crush failed: %s", exc)

    return "\n".join(lines)


def _board_crush_lines(dce_data: pd.DataFrame, cny_usd: float | None) -> list[str]:
    """China board crush margin from the B0/M0/Y0 continuous series.

    Continuous-series caveat: the three legs need not roll the underlying
    contract on the same day, so near-roll prints may embed roll gaps —
    hence the explicit tag on the line (same humility as Layer 1 signals).
    """
    crush = compute_dce_crush_margin(dce_data)
    if crush.empty:
        return []
    latest = crush.iloc[-1]
    value_cny = latest["crush_cny_mt"]

    if cny_usd is not None:
        parts = [f"CNY {value_cny:+,.0f}/MT ({value_cny * cny_usd:+,.0f} USD/MT)"]
    else:
        parts = [f"CNY {value_cny:+,.0f}/MT"]
    parts.append(f"oil share {latest['oil_value_share'] * 100:.0f}%")

    date = latest["Date"]
    date_str = date.date() if hasattr(date, "date") else date
    return [
        f"  DCE board crush: {' | '.join(parts)} "
        f"(as of {date_str}; continuous main-contract legs — roll gaps possible)"
    ]
