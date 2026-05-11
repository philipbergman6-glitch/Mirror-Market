"""SEASONAL ANALYSIS section — current vs. historical norms."""

import pandas as pd

from analysis.seasonal import current_vs_seasonal


def format(price_data: dict[str, pd.DataFrame]) -> str:  # noqa: A001
    lines = ["SEASONAL ANALYSIS:"]

    for commodity, df in price_data.items():
        if df.empty:
            continue

        result = current_vs_seasonal(df)
        if result:
            lines.append(f"  {commodity}: {result['assessment']}")

    if len(lines) == 1:
        lines.append("  Insufficient history for seasonal comparison")

    return "\n".join(lines)
