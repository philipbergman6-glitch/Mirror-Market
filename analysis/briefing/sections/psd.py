"""GLOBAL SUPPLY (USDA PSD) section."""

import pandas as pd

from pipeline.query import read_psd

# Argentina is the #1 soybean meal/oil exporter — its product balance
# sheets move the soy complex as much as US/Brazil bean supply does.
_ARGENTINA_HIGHLIGHTS = [
    ("Soybeans", "Argentina", "Production"),
    ("Soybean Meal", "Argentina", "Exports"),
    ("Soybean Oil", "Argentina", "Exports"),
]


def _latest_with_yoy(
    psd_data: pd.DataFrame, commodity: str, country: str, attribute: str
) -> str | None:
    """'value (unit) (+x.x% YoY)' for the latest year, None when absent."""
    rows = psd_data[
        (psd_data["commodity"] == commodity)
        & (psd_data["country"] == country)
        & (psd_data["attribute"] == attribute)
    ].sort_values("year")
    if rows.empty:
        return None
    latest = rows.iloc[-1]
    value = latest["value"]
    if pd.isna(value):
        return None
    unit = str(latest.get("unit", "1000 MT")).strip("() ")
    text = f"{value:,.0f} ({unit})"
    prev = rows[rows["year"] == latest["year"] - 1]
    if not prev.empty and pd.notna(prev.iloc[0]["value"]) and prev.iloc[0]["value"] != 0:
        yoy = (value - prev.iloc[0]["value"]) / prev.iloc[0]["value"] * 100
        sign = "+" if yoy >= 0 else ""
        text += f" ({sign}{yoy:.1f}% YoY)"
    return text


def format() -> str:  # noqa: A001
    lines = ["GLOBAL SUPPLY (USDA PSD):"]
    psd_data = read_psd()

    if psd_data.empty:
        return "GLOBAL SUPPLY (USDA PSD): No data"

    latest_year = psd_data["year"].max()
    latest = psd_data[psd_data["year"] == latest_year]

    highlights = [
        ("Soybeans", "Brazil", "Production"),
        ("Soybeans", "China", "Imports"),
        ("Soybeans", "United States", "Production"),
        ("Palm Oil", "Indonesia", "Production"),
    ]

    for commodity, country, attribute in highlights:
        row = latest[
            (latest["commodity"] == commodity)
            & (latest["country"] == country)
            & (latest["attribute"] == attribute)
        ]
        if not row.empty:
            value = row.iloc[0]["value"]
            unit = str(row.iloc[0].get("unit", "1000 MT")).strip("() ")
            lines.append(f"  {country} {commodity.lower()} {attribute.lower()}: {value:,.0f} ({unit})")

    argentina_lines = []
    for commodity, country, attribute in _ARGENTINA_HIGHLIGHTS:
        text = _latest_with_yoy(psd_data, commodity, country, attribute)
        if text:
            argentina_lines.append(f"    {commodity} {attribute.lower()}: {text}")
    if argentina_lines:
        lines.append("  Argentina (top soymeal/oil exporter):")
        lines.extend(argentina_lines)

    if len(lines) == 1:
        lines.append("  Data available but no key highlights matched")

    return "\n".join(lines)
