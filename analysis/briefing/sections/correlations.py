"""CORRELATIONS section — cross-commodity + commodity-vs-currency."""

from typing import cast

import pandas as pd

from analysis.correlations import commodity_correlation_matrix, commodity_vs_currency


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    lines = ["CORRELATIONS:"]

    if len(price_data) >= 2:
        corr_matrix = commodity_correlation_matrix(price_data)
        if not corr_matrix.empty:
            lines.append("  Cross-commodity (daily returns):")
            shown = set()
            for i, row_name in enumerate(corr_matrix.index):
                for j, col_name in enumerate(corr_matrix.columns):
                    if i >= j:
                        continue
                    pair_key = tuple(sorted([row_name, col_name]))
                    if pair_key in shown:
                        continue
                    r_val = corr_matrix.iloc[i, j]
                    if pd.isna(r_val):
                        continue
                    r = cast(float, r_val)
                    if abs(r) > 0.5:
                        strength = "strong" if abs(r) > 0.7 else "moderate"
                        direction = "positive" if r > 0 else "negative"
                        lines.append(f"    {row_name} vs {col_name}: {r:.2f} ({strength} {direction})")
                        shown.add(pair_key)

    key_pairs = [
        ("Soybeans", "BRL/USD", "BRL weakening → cheaper Brazil exports → soy pressure"),
        ("Coffee", "COP/USD", "COP weakening → cheaper Colombia exports"),
        ("Coffee", "BRL/USD", "BRL weakening → cheaper Brazil exports"),
    ]

    currency_corrs = []
    for commodity_name, pair_name, note in key_pairs:
        if commodity_name in price_data and pair_name in currency_data:
            r = commodity_vs_currency(
                price_data[commodity_name],
                currency_data[pair_name],
                commodity_name,
                pair_name,
            )
            if pd.notna(r):
                currency_corrs.append(f"    {commodity_name} vs {pair_name}: {r:.2f} ({note})")

    if currency_corrs:
        lines.append("  Commodity-currency:")
        lines.extend(currency_corrs)

    if len(lines) == 1:
        lines.append("  Insufficient data for correlation analysis")

    return "\n".join(lines)
