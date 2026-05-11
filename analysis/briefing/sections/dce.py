"""DCE CHINESE FUTURES section — Dalian futures vs CBOT comparison."""

import pandas as pd

from pipeline.query import read_dce_futures


def format(price_data: dict[str, pd.DataFrame]) -> str:  # noqa: A001
    lines = ["DCE CHINESE FUTURES:"]
    dce_data = read_dce_futures()

    if dce_data.empty:
        return "DCE CHINESE FUTURES: No data"

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

        parts = [f"CNY {dce_close:,.0f}"]

        cbot_name = dce_to_cbot.get(dce_name)
        if cbot_name and cbot_name in price_data:
            cbot_df = price_data[cbot_name]
            if not cbot_df.empty:
                cbot_close = cbot_df["Close"].iloc[-1]
                parts.append(f"vs CBOT {cbot_close:,.2f} USD")

        lines.append(
            f"  {dce_name}: {' | '.join(parts)} "
            f"(as of {dce_date.date() if hasattr(dce_date, 'date') else dce_date})"
        )

    return "\n".join(lines)
