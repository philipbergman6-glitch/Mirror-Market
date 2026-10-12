"""DCE CHINESE FUTURES section — Dalian futures vs CBOT comparison.

Both legs are converted to USD/MT before comparison: DCE quotes CNY/MT,
CBOT quotes cents/bu (beans), cents/lb (oil), or $/short ton (meal) —
printing the raw closes side by side is not a comparison in any currency.

Each displayed leg converts at its own date's CNY/USD close under A1 (#298),
never the newest rate. The premium is one session's number (invariant 8):
struck on the latest session both venues printed, or withheld with the reason
— a Golden Week closure is a gap, not a premium against today's CBOT.
"""

import logging

import pandas as pd

from analysis.spreads import (
    compute_dce_crush_margin,
    daily_close,
    fx_close_on,
    strike_cross_venue_spread,
)
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

    cny_df = currency_data.get("CNY/USD") if currency_data else None

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

        dce_series = daily_close(subset.set_index("Date")["Close"])
        if dce_series.empty:
            continue
        dce_date = dce_series.index[-1]
        dce_close = float(dce_series.iloc[-1])

        dce_fx = fx_close_on(cny_df, dce_date, "CNY/USD")
        if dce_fx is not None:
            parts = [f"{dce_close * dce_fx[0]:,.0f} USD/MT (CNY {dce_close:,.0f})"]
        else:
            parts = [f"CNY {dce_close:,.0f}/MT"]

        cbot_name = dce_to_cbot.get(dce_name)
        cbot_df = price_data.get(cbot_name) if cbot_name else None
        cbot_series = (
            daily_close(cbot_df["Close"]) if cbot_df is not None and not cbot_df.empty
            else pd.Series(dtype=float)
        )
        if cbot_name and not cbot_series.empty:
            cbot_mt = to_metric_tons(float(cbot_series.iloc[-1]), cbot_name)
            if cbot_mt is not None:
                parts.append(
                    f"vs CBOT {cbot_mt:,.0f} USD/MT ({cbot_series.index[-1].date()})"
                )
            strike = strike_cross_venue_spread(
                own=cbot_series, own_commodity=cbot_name, own_label=f"CBOT {cbot_name}",
                other=dce_series, other_label=dce_name, fx=cny_df, fx_pair="CNY/USD",
            )
            if strike.spread_usd_mt is not None and strike.struck_on is not None:
                parts.append(
                    f"premium {strike.spread_usd_mt:+,.0f} "
                    f"(struck {strike.struck_on.date()})"
                )
            else:
                parts.append(f"premium withheld — {strike.reason}")

        lines.append(
            f"  {dce_name}: {' | '.join(parts)} "
            f"(as of {dce_date.date()})"
        )

    try:
        lines.extend(_board_crush_lines(dce_data, cny_df))
    except Exception as exc:
        logger.debug("DCE board crush failed: %s", exc)

    return "\n".join(lines)


def _board_crush_lines(dce_data: pd.DataFrame, cny_df: pd.DataFrame | None) -> list[str]:
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
    date = latest["Date"]

    # Converted at the crush session's own rate, never the newest (A1 #298).
    crush_fx = fx_close_on(cny_df, pd.Timestamp(date), "CNY/USD")
    if crush_fx is not None:
        parts = [f"CNY {value_cny:+,.0f}/MT ({value_cny * crush_fx[0]:+,.0f} USD/MT)"]
    else:
        parts = [f"CNY {value_cny:+,.0f}/MT"]
    parts.append(f"oil share {latest['oil_value_share'] * 100:.0f}%")

    date_str = date.date() if hasattr(date, "date") else date
    return [
        f"  DCE board crush: {' | '.join(parts)} "
        f"(as of {date_str}; continuous main-contract legs — roll gaps possible)"
    ]
