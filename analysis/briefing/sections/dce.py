"""DCE CHINESE FUTURES section — Dalian futures vs CBOT comparison.

Both legs are converted to USD/MT before comparison: DCE quotes CNY/MT,
CBOT quotes cents/bu (beans), cents/lb (oil), or $/short ton (meal) —
printing the raw closes side by side is not a comparison in any currency.
"""

import pandas as pd

from pipeline.query import read_dce_futures
from pipeline.units import to_metric_tons


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

    dce_to_cbot = {
        "DCE Soybean": "Soybeans",
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

    return "\n".join(lines)
