"""Stocks-to-use ratio computation and tight-supply signal detection.

The ratio (ending stocks / total use) per marketing year is the single
most-watched soy fundamental. We source it from PSD (Layer 6) because
PSD is the machine-readable form of WASDE's balance sheet — USDA OCE
publishes both products together each month.

NASS QuickStats does not expose WASDE forecasts, so Layer 12's
`fetch_wasde_estimates` returns zero rows in practice. PSD covers the
same data with proper attribute coverage.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

# PSD attribute strings — see config.PSD_TARGET_ATTRIBUTES.
# Total use is the domestic consumption leg + Exports. PSD's "Total Distribution"
# is NOT usable here: it equals Total Supply (Beginning Stocks + Production
# + Imports), which understates the ratio's denominator meaning.
_ENDING_STOCKS = "Ending Stocks"
_DOMESTIC_CONSUMPTION = "Domestic Consumption"
_DOMESTIC_USE = "Domestic Use"
_EXPORTS = "Exports"

# The consumption leg is **named per commodity**, not once (#238). PSD calls
# cotton's line "Domestic Use" (attribute 142) and publishes no "Domestic
# Consumption" row for it at all, so the single old constant dropped cotton
# from the ratio frame entirely while the briefing kept advertising a cotton
# balance sheet it could never print. An explicit map rather than a coalesce
# over whichever column happens to exist: a coalesce would quietly absorb a
# PSD rename, which is the failure this ticket is.
PSD_CONSUMPTION_ATTRIBUTE = {
    "Soybeans":      _DOMESTIC_CONSUMPTION,
    "Soybean Oil":   _DOMESTIC_CONSUMPTION,
    "Soybean Meal":  _DOMESTIC_CONSUMPTION,
    "Palm Oil":      _DOMESTIC_CONSUMPTION,
    "Corn":          _DOMESTIC_CONSUMPTION,
    "Wheat":         _DOMESTIC_CONSUMPTION,
    "Cotton":        _DOMESTIC_USE,
    "Rapeseed":      _DOMESTIC_CONSUMPTION,
    "Rapeseed Oil":  _DOMESTIC_CONSUMPTION,
    "Rapeseed Meal": _DOMESTIC_CONSUMPTION,
}


def consumption_attribute(commodity: str) -> str:
    """Return the PSD attribute holding `commodity`'s domestic consumption.

    Raises `KeyError` for an unmapped commodity rather than defaulting: a
    default would silently sum the wrong (or no) demand leg, which is exactly
    how cotton's ratio went missing.
    """
    try:
        return PSD_CONSUMPTION_ATTRIBUTE[commodity]
    except KeyError:
        raise KeyError(
            f"No PSD consumption attribute mapped for commodity {commodity!r} — "
            "add it to analysis.stocks_to_use.PSD_CONSUMPTION_ATTRIBUTE "
            "(PSD names cotton's line 'Domestic Use', everything else's "
            "'Domestic Consumption')"
        ) from None


# Sample-size guard mirrors the SEASONAL_MIN_YEARS_PER_MONTH pattern in
# analysis/seasonal.py: we won't fire an alert without at least this
# many prior marketing years to compare against.
MIN_HISTORY_YEARS = 3

# How many prior marketing years count toward the "historical low".
HISTORY_WINDOW = 5


def compute_stocks_to_use(
    psd_df: pd.DataFrame,
    country: str = "United States",
) -> pd.DataFrame:
    """Return stocks-to-use ratios per (commodity, marketing year).

    Parameters
    ----------
    psd_df : pd.DataFrame
        Output of `pipeline.query.read_psd()` with columns
        commodity / country / year / attribute / value / unit.
    country : str
        Country to compute the ratio for. Defaults to "United States",
        which gives the WASDE-equivalent US balance-sheet view.

    Returns
    -------
    pd.DataFrame
        Columns: commodity, year, unit, ending_stocks, total_use, ratio.
        Rows where either component is missing or total_use<=0 are
        dropped. The ratio is a fraction (0.082 represents 8.2%).
        `unit` is PSD's own unit string for that commodity, carried
        through because `ending_stocks`/`total_use` are level figures
        and the commodities are not in one unit: cotton is in
        `1000 480 lb. Bales`, the rest in `1000 MT`. The ratio is
        unitless and cancels; the levels must never be pooled.

    Raises
    ------
    KeyError
        A commodity with no entry in `PSD_CONSUMPTION_ATTRIBUTE`.
    ValueError
        `psd_df` carries no `unit` column, or a commodity's mapped
        consumption attribute is absent from PSD entirely.
    """
    empty_cols = [
        "commodity", "year", "unit", "ending_stocks", "total_use", "ratio",
    ]
    if psd_df.empty:
        return pd.DataFrame(columns=empty_cols)

    if "unit" not in psd_df.columns:
        raise ValueError(
            "PSD frame has no 'unit' column — refusing to return level figures "
            "unlabelled (cotton is in bales, the rest in 1000 MT)"
        )

    consumption_names = set(PSD_CONSUMPTION_ATTRIBUTE.values())
    df = psd_df[
        (psd_df["country"] == country)
        & (psd_df["attribute"].isin({_ENDING_STOCKS, _EXPORTS} | consumption_names))
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    # Hard-fail on a commodity we have no consumption attribute for, rather
    # than dropping it — a missing balance sheet is invisible downstream.
    # Then hard-fail on the *other* direction: the attribute is mapped but PSD
    # publishes no such row for that commodity at all. That is what a rename
    # upstream looks like, and it is the failure this module was carrying —
    # cotton's consumption leg vanished into a dropna and the section printed
    # "Cotton: No data" for as long as it existed (#238). A per-year gap stays
    # an ordinary dropped row; only a commodity-wide absence is a break.
    for commodity in sorted(df["commodity"].dropna().unique()):
        attribute = consumption_attribute(str(commodity))
        rows = df[df["commodity"] == commodity]
        if not (rows["attribute"] == attribute).any():
            raise ValueError(
                f"PSD publishes no {attribute!r} row for {commodity!r} in "
                f"{country!r} — it carries "
                f"{sorted(rows['attribute'].unique())}. The consumption "
                "attribute was renamed upstream; fix "
                "analysis.stocks_to_use.PSD_CONSUMPTION_ATTRIBUTE and "
                "config.PSD_TARGET_ATTRIBUTES together."
            )

    # PSD's unit is a property of the commodity, not of a year, but read it per
    # (commodity, year) so a mid-series unit change surfaces as two rows rather
    # than one silently relabelled series.
    units = (
        df.groupby(["commodity", "year"], as_index=False)["unit"]
        .last()
        .assign(year=lambda u: u["year"].astype(int))
    )

    wide = df.pivot_table(
        index=["commodity", "year"],
        columns="attribute",
        values="value",
        aggfunc="last",
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={_ENDING_STOCKS: "ending_stocks"})

    for col in sorted({"ending_stocks", _EXPORTS} | consumption_names):
        if col not in wide.columns:
            wide[col] = pd.NA

    # Each row reads only *its own* commodity's consumption attribute.
    wide["consumption"] = [
        wide.at[i, consumption_attribute(str(wide.at[i, "commodity"]))]
        for i in wide.index
    ]

    wide = wide.dropna(subset=["ending_stocks", "consumption", _EXPORTS])
    wide["total_use"] = wide["consumption"] + wide[_EXPORTS]
    wide = wide[wide["total_use"] > 0]
    if wide.empty:
        return pd.DataFrame(columns=empty_cols)

    wide["ratio"] = wide["ending_stocks"] / wide["total_use"]
    wide["year"] = wide["year"].astype(int)
    wide = wide.merge(units, on=["commodity", "year"], how="left")
    return wide[empty_cols].reset_index(drop=True)


def detect_tight_supply(
    stu_df: pd.DataFrame,
    *,
    commodities: list[str] | None = None,
    today: str | None = None,
) -> list[dict]:
    """Return one signal per commodity whose latest ratio < prior-window low.

    Parameters
    ----------
    stu_df : pd.DataFrame
        Output of `compute_stocks_to_use`.
    commodities : list[str] | None
        Restrict to these PSD commodity names. Defaults to every
        commodity present in `stu_df`.
    today : str | None
        ISO date stamp to attach to emitted signals. Defaults to
        today (UTC).

    Returns
    -------
    list[dict]
        Signal dicts (`severity="alert"`) matching the schema used in
        `analysis/signals.py`: date, commodity, signal_type, severity,
        description.
    """
    if stu_df.empty:
        return []

    stamp = today or date.today().strftime("%Y-%m-%d")
    universe = (
        commodities
        if commodities is not None
        else sorted(stu_df["commodity"].unique())
    )
    out: list[dict] = []

    for commodity in universe:
        rows = (
            stu_df[stu_df["commodity"] == commodity]
            .sort_values("year")
            .reset_index(drop=True)
        )
        if len(rows) < MIN_HISTORY_YEARS + 1:
            continue

        current = rows.iloc[-1]
        history = rows.iloc[-(1 + HISTORY_WINDOW):-1]
        if len(history) < MIN_HISTORY_YEARS:
            continue

        current_ratio = float(current["ratio"])
        prior_low = float(history["ratio"].min())
        if current_ratio < prior_low:
            out.append({
                "date": stamp,
                "commodity": commodity,
                "signal_type": "tight_supply_wasde",
                "severity": "alert",
                "description": (
                    f"{commodity} stocks-to-use {current_ratio * 100:.1f}% "
                    f"(MY {int(current['year'])}) — below "
                    f"{len(history)}-yr prior low of {prior_low * 100:.1f}%"
                ),
            })
    return out
