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
# Total use is Domestic Consumption + Exports. PSD's "Total Distribution"
# is NOT usable here: it equals Total Supply (Beginning Stocks + Production
# + Imports), which understates the ratio's denominator meaning.
_ENDING_STOCKS = "Ending Stocks"
_DOMESTIC_CONSUMPTION = "Domestic Consumption"
_EXPORTS = "Exports"

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
        Columns: commodity, year, ending_stocks, total_use, ratio.
        Rows where either component is missing or total_use<=0 are
        dropped. The ratio is a fraction (0.082 represents 8.2%).
    """
    empty_cols = ["commodity", "year", "ending_stocks", "total_use", "ratio"]
    if psd_df.empty:
        return pd.DataFrame(columns=empty_cols)

    df = psd_df[
        (psd_df["country"] == country)
        & (psd_df["attribute"].isin([_ENDING_STOCKS, _DOMESTIC_CONSUMPTION, _EXPORTS]))
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_cols)

    wide = df.pivot_table(
        index=["commodity", "year"],
        columns="attribute",
        values="value",
        aggfunc="last",
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={_ENDING_STOCKS: "ending_stocks"})

    for col in ("ending_stocks", _DOMESTIC_CONSUMPTION, _EXPORTS):
        if col not in wide.columns:
            wide[col] = pd.NA

    wide = wide.dropna(subset=["ending_stocks", _DOMESTIC_CONSUMPTION, _EXPORTS])
    wide["total_use"] = wide[_DOMESTIC_CONSUMPTION] + wide[_EXPORTS]
    wide = wide[wide["total_use"] > 0]
    if wide.empty:
        return pd.DataFrame(columns=empty_cols)

    wide["ratio"] = wide["ending_stocks"] / wide["total_use"]
    wide["year"] = wide["year"].astype(int)
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
