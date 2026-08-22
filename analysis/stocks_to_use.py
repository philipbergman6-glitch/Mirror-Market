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
# For a single country, total use is Domestic Consumption + Exports: a cargo
# leaving the country is an offtake against its own balance sheet. PSD's
# "Total Distribution" is NOT usable here: it equals Total Supply (Beginning
# Stocks + Production + Imports), which understates the ratio's denominator
# meaning.
#
# That reasoning inverts at world level — see _AGGREGATE_REGIONS below.
_ENDING_STOCKS = "Ending Stocks"
_DOMESTIC_CONSUMPTION = "Domestic Consumption"
_EXPORTS = "Exports"
_IMPORTS = "Imports"

# The synthetic regions fetchers/psd.py writes (M15 #237). The denominator
# depends on the region, and getting it wrong is silent: both formulas
# produce a plausible-looking percentage.
#
# For an aggregate the denominator is Domestic Consumption ONLY. A world
# export is already inside some importer's domestic consumption, so adding
# exports double-counts every traded tonne — it moves world soybean
# stocks-to-use from 29.19% to 20.33%. USDA's own printed world ratio is
# consumption-only: WASDE-673 p.5 puts cotton at 63.1 percent, which is
# world ending stocks over world domestic use and nothing else.
#
# (PSD ships a per-country Stocks-to-Use attribute whose World value applies
# the country formula to the aggregate and lands 17 pp from USDA's own
# printed figure. That attribute is not stored, and must not be.)
WORLD = "World"
WORLD_LESS_CHINA = "World Less China"
_AGGREGATE_REGIONS = frozenset({WORLD, WORLD_LESS_CHINA})

# WASDE's grain tables carry an adjustment PSD's world row does not:
# "Total foreign and world use adjusted to reflect the differences in world
# imports and exports" (WASDE-673 footnotes, wheat p.18 / coarse grains p.20
# / corn p.22). Arithmetically exact — wheat 819,541 + (227,084 − 222,013)
# = 824,612, the figure WASDE prints. The oilseed tables apply no such
# adjustment, so the set is the grains only.
WASDE_USE_ADJUSTED_COMMODITIES = frozenset({"Corn", "Wheat"})

# Reproduce WASDE's printed grain table rather than the raw PSD balance on
# every world surface. Either is defensible; mixing them is not, so the
# choice is made once here and stated by `denominator_note`.
WORLD_GRAIN_ADJUSTMENT = True

# World balance sheets we publish. Cotton is absent on purpose: PSD's cotton
# consumption attribute is "Domestic Use" (142), which
# config.PSD_TARGET_ATTRIBUTES does not yet request, so no cotton ratio —
# US or world — is computable today. Padding or substituting a denominator
# to make the row appear is the exact failure the withhold-with-a-reason
# rule exists to prevent.
WORLD_COMMODITIES = (
    "Soybeans",
    "Soybean Meal",
    "Soybean Oil",
    "Palm Oil",
    "Corn",
    "Wheat",
    "Rapeseed",
    "Rapeseed Oil",
    "Rapeseed Meal",
)

# Sample-size guard mirrors the SEASONAL_MIN_YEARS_PER_MONTH pattern in
# analysis/seasonal.py: we won't fire an alert without at least this
# many prior marketing years to compare against.
MIN_HISTORY_YEARS = 3

# How many prior marketing years count toward the "historical low".
HISTORY_WINDOW = 5


def denominator_note(
    country: str, *, wasde_grain_adjustment: bool = False
) -> str:
    """One sentence naming the region, the denominator, and the adjustment.

    Every rendered stocks-to-use surface must carry this: the world ratio
    and the country ratio are different statistics that print as the same
    kind of percentage.
    """
    if country not in _AGGREGATE_REGIONS:
        return (
            f"{country} balance sheet — denominator = Domestic Consumption "
            f"+ Exports (a cargo leaving the country is an offtake)."
        )

    if country == WORLD:
        region = "every PSD country"
        # The world is a closed system: every export lands in some other
        # country's domestic consumption, so adding exports double-counts it.
        why = "a world export is already inside an importer's consumption"
    else:
        region = (
            "every PSD country except China — USDA's own World Less China "
            "balance sheet, a pure subtraction of the China row"
        )
        # This region is *open* (China imported 113 MMT of soybeans out of
        # it in MY2025), so the closed-system argument does not carry. The
        # denominator is consumption-only because that is the convention of
        # the world line it is subtracted from — the two are comparable only
        # if struck the same way. USDA publishes the balance sheet; the
        # ratio is ours.
        why = (
            "matching the world line it is subtracted from, so the two are "
            "comparable — not because the region is closed, it is not"
        )
    grain = (
        "corn/wheat use adjusted by the world export–import gap, "
        "reproducing WASDE's printed table (WASDE footnote 2/)"
        if wasde_grain_adjustment
        else "raw PSD, without WASDE's corn/wheat use adjustment"
    )
    return (
        f"{country} — region: {region}; denominator = Domestic Consumption "
        f"only ({why}); {grain}."
    )


def _pivot_region(psd_df: pd.DataFrame, country: str) -> pd.DataFrame | None:
    """One row per (commodity, year) for a region, one column per attribute.

    None when the region has no rows at all. Absent attributes are present
    as all-NULL columns so callers can decide per branch what is required.
    """
    df = psd_df[
        (psd_df["country"] == country)
        & (psd_df["attribute"].isin(
            [_ENDING_STOCKS, _DOMESTIC_CONSUMPTION, _EXPORTS, _IMPORTS]
        ))
    ]
    if df.empty:
        return None

    wide = df.pivot_table(
        index=["commodity", "year"],
        columns="attribute",
        values="value",
        aggfunc="last",
    ).reset_index()
    wide.columns.name = None
    wide = wide.rename(columns={_ENDING_STOCKS: "ending_stocks"})
    for col in ("ending_stocks", _DOMESTIC_CONSUMPTION, _EXPORTS, _IMPORTS):
        if col not in wide.columns:
            wide[col] = pd.NA
    return wide


def _world_trade_gap(psd_df: pd.DataFrame) -> pd.DataFrame:
    """World (Exports − Imports) per (commodity, year), as column `_gap`.

    This is WASDE's footnote-2/ adjustment term. It is read off the World
    row for every aggregate region, including World Less China — see the
    call site.
    """
    empty = pd.DataFrame(columns=["commodity", "year", "_gap"])
    world = _pivot_region(psd_df, WORLD)
    if world is None:
        return empty
    world = world.dropna(subset=[_EXPORTS, _IMPORTS])
    if world.empty:
        return empty
    world = world.copy()
    world["_gap"] = world[_EXPORTS] - world[_IMPORTS]
    return world[["commodity", "year", "_gap"]]


def compute_stocks_to_use(
    psd_df: pd.DataFrame,
    country: str = "United States",
    *,
    wasde_grain_adjustment: bool = False,
) -> pd.DataFrame:
    """Return stocks-to-use ratios per (commodity, marketing year).

    Parameters
    ----------
    psd_df : pd.DataFrame
        Output of `pipeline.query.read_psd()` with columns
        commodity / country / year / attribute / value / unit.
    country : str
        Region to compute the ratio for. Defaults to "United States",
        which gives the WASDE-equivalent US balance-sheet view. Pass
        `WORLD` or `WORLD_LESS_CHINA` for the aggregates Layer 6
        synthesises — those switch the denominator to consumption only.
    wasde_grain_adjustment : bool
        Aggregate regions only. Add (Exports − Imports) to corn/wheat use
        so the ratio reproduces WASDE's printed grain table instead of the
        raw PSD balance. Raises for a single country, where the adjustment
        has no meaning.

    Returns
    -------
    pd.DataFrame
        Columns: commodity, year, ending_stocks, total_use, ratio.
        Rows where any component is missing or total_use<=0 are
        dropped. The ratio is a fraction (0.082 represents 8.2%).
    """
    is_aggregate = country in _AGGREGATE_REGIONS
    if wasde_grain_adjustment and not is_aggregate:
        raise ValueError(
            f"wasde_grain_adjustment applies to an aggregate region "
            f"({sorted(_AGGREGATE_REGIONS)}), not to {country!r}: WASDE "
            f"adjusts world and foreign use, never a single country's."
        )

    empty_cols = ["commodity", "year", "ending_stocks", "total_use", "ratio"]
    if psd_df.empty:
        return pd.DataFrame(columns=empty_cols)

    wide = _pivot_region(psd_df, country)
    if wide is None:
        return pd.DataFrame(columns=empty_cols)

    if is_aggregate:
        wide = wide.dropna(subset=["ending_stocks", _DOMESTIC_CONSUMPTION])
        wide["total_use"] = wide[_DOMESTIC_CONSUMPTION]
        if wasde_grain_adjustment:
            # The gap is always the *world* one, even for World Less China.
            # WASDE carries its world (exports − imports) into the less-China
            # line unchanged: PSD-derived less-China wheat consumption is
            # 669,541 and WASDE prints 674.61 — the same +5,071 as the world
            # row (research §4.4, and §8 for corn's +23,937). Recomputing the
            # gap from the less-China legs instead adds China's own net import
            # position — wheat MY2025 becomes 9,430, not 5,071 — and the
            # printed figure then matches neither WASDE nor raw PSD.
            gap = _world_trade_gap(psd_df)
            wide = wide.merge(gap, on=["commodity", "year"], how="left")
            adjusted = wide["commodity"].isin(WASDE_USE_ADJUSTED_COMMODITIES)
            # A grain row with no world gap to carry cannot be adjusted, and
            # printing it unadjusted under an adjusted label would misstate
            # what the number is. Withhold it instead.
            wide = wide[~(adjusted & wide["_gap"].isna())].copy()
            adjusted = wide["commodity"].isin(WASDE_USE_ADJUSTED_COMMODITIES)
            if adjusted.any():
                wide.loc[adjusted, "total_use"] = (
                    wide.loc[adjusted, _DOMESTIC_CONSUMPTION]
                    + wide.loc[adjusted, "_gap"]
                )
    else:
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
