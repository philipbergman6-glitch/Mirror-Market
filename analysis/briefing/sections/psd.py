"""GLOBAL SUPPLY (USDA PSD) section."""

from pipeline.query import read_psd


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

    if len(lines) == 1:
        lines.append("  Data available but no key highlights matched")

    return "\n".join(lines)
