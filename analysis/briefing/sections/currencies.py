"""CURRENCIES section — trade-impact narratives per pair."""

import pandas as pd


def format(currency_data: dict[str, pd.DataFrame]) -> str:  # noqa: A001
    lines = ["CURRENCIES:"]

    if not currency_data:
        return "CURRENCIES: No data"

    for pair, subset in currency_data.items():
        if subset.empty:
            continue

        latest = subset.iloc[-1]
        close = latest["Close"]

        comment = ""
        if len(subset) >= 6:
            prev = subset.iloc[-6]["Close"]
            if pd.notna(prev) and prev != 0:
                chg_pct = ((close - prev) / prev) * 100
                if "BRL" in pair:
                    direction = "Real weakening" if chg_pct < 0 else "Real strengthening"
                    impact = "Brazil exports cheaper" if chg_pct < 0 else "Brazil exports dearer"
                    comment = f"({direction} — {impact})"
                elif "CNY" in pair:
                    direction = "Yuan weakening" if chg_pct < 0 else "Yuan stable"
                    comment = f"({direction})"
                elif "ARS" in pair:
                    direction = "Peso weakening" if chg_pct < 0 else "Peso stable"
                    comment = f"({direction})"
                elif "IDR" in pair:
                    direction = "Rupiah weakening" if chg_pct < 0 else "Rupiah stable"
                    comment = f"({direction})"
                elif "MYR" in pair:
                    direction = "Ringgit weakening" if chg_pct < 0 else "Ringgit stable"
                    comment = f"({direction})"

        lines.append(f"  {pair}: {close:.4f} {comment}")

    return "\n".join(lines)
