"""
Layer 6 — USDA FAS PSD (Production, Supply & Distribution) global data.

Downloads bulk CSV zips from the USDA Foreign Agricultural Service.
These contain production, imports, exports, crush, and ending stocks
for every country — back to 1960.  Updated monthly.  No API key needed.

Key concepts for learning:
    - zipfile + io.BytesIO: extract files in memory without writing temp files
    - Filtering large DataFrames: the raw CSV has ~200K rows, but we only
      keep the commodities/countries/attributes we care about
    - The PSD data gives you the GLOBAL picture — who produces, who imports,
      who has stocks — which CBOT prices alone can't tell you
"""

import io
import logging
import zipfile

import pandas as pd
import requests

from config import (
    MAX_RETRIES,
    PSD_TARGET_ATTRIBUTES,
    PSD_TARGET_COMMODITIES,
    PSD_TARGET_COUNTRIES,
    PSD_URLS,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep

logger = logging.getLogger(__name__)

# Dataset spelling → display name used everywhere else in the project.
# Verified against the 2026 bulk CSVs (191 distinct Country_Name values).
_PSD_COUNTRY_ALIASES = {
    "Korea, South": "South Korea",
    "Cote d'Ivoire": "Ivory Coast",
}

# ── The world aggregate (M15 #237) ──────────────────────────────────
#
# The bulk CSVs carry no World row (`Country_Code == "00"` matches nothing),
# but they contain every country, so the World total is the plain sum of
# them — verified attribute-by-attribute against USDA's own R00 row on 197
# (commodity, marketing-year) pairs spanning 1960→2026, zero mismatches
# (research/2026-08-12-m15-psd-world-stocks-to-use.md §3). The 28-country
# sum in PSD_TARGET_COUNTRIES is NOT an approximation of it: it is 67.3% of
# world wheat consumption and puts wheat stocks-to-use 5.7 pp too high.
#
# So the sum must happen *before* the country filter, which is what
# `_filter_psd` does below.
WORLD = "World"
WORLD_LESS_CHINA = "World Less China"

# USDA publishes the World-less-China line itself, in every WASDE
# international supply-and-use table, and it is a pure subtraction of the
# China row (§4.4). China holds 35% of world soybean stocks and 59% of world
# corn stocks, so the world ratio is dominated by one balance sheet.
_CHINA = "China"

# Only *extensive* attributes may be summed. PSD ships rate attributes in the
# same files — 184 Yield and 195 Stocks-to-Use — and they sum to nonsense
# (soybean world Yield 3.01 MT/HA against a country-sum of 95.39). The gate
# is the unit rather than an attribute-name blocklist: a new rate attribute
# would slip past a blocklist, but it cannot carry a quantity unit.
#
# The set is every Unit_Description observed across the three 2026 bulk CSVs
# (oilseeds: 1000 HA / 1000 MT / MT/HA / PERCENT / RATIO; grains: 1000 HA /
# 1000 MT / MT/HA; cotton: 1000 HA / KG/HA / PERCENT / 1000 480 lb. Bales).
# An unrecognised unit is withheld rather than guessed at.
_ADDITIVE_UNITS = frozenset({"1000 MT", "1000 HA", "1000 480 LB. BALES"})

# Aggregate keys: one world row per commodity, marketing year, attribute.
_AGG_KEYS = ["commodity", "year", "attribute"]


def _is_additive_unit(unit: object) -> bool:
    """True where a Unit_Description names a quantity that may be summed."""
    return str(unit).strip().strip("()").strip().upper() in _ADDITIVE_UNITS


def _world_aggregates(df: pd.DataFrame, code_to_name: dict[str, str]) -> pd.DataFrame:
    """Reconstruct USDA's World row — and World-less-China — by summation.

    `df` is the raw CSV already filtered to the target commodities and
    attributes but **not** to the target countries. Returns rows in the
    standardised output shape, or an empty frame when nothing is additive.
    """
    empty = pd.DataFrame(
        columns=["commodity", "country", "year", "attribute", "value", "unit"]
    )
    if df.empty:
        return empty

    # One row per (commodity, country, year, attribute) is what makes the sum
    # a sum. The country path survives a duplicate harmlessly — storage is an
    # INSERT OR REPLACE on exactly that key, so the last write wins — but a
    # summed world row would silently double instead. The bulk CSVs carry a
    # `Month` column (the vintage each country's estimate was last revised
    # in) and today ship one row per key; if that ever changes, every world
    # number in the stack is wrong by an amount nothing in its shape reveals.
    duplicated = df.duplicated(
        subset=["Commodity_Code", "Country_Name", "Market_Year",
                "Attribute_Description"],
        keep=False,
    )
    if duplicated.any():
        sample = df.loc[duplicated].head(3)
        raise ValueError(
            f"PSD bulk CSV carries {int(duplicated.sum())} duplicate "
            f"(commodity, country, year, attribute) rows — refusing to sum "
            f"them into a world total. First: "
            f"{sample[['Commodity_Code', 'Country_Name', 'Market_Year', 'Attribute_Description']].to_dict('records')}"
        )

    additive = df[df["Unit_Description"].map(_is_additive_unit)]
    skipped = sorted(set(df["Attribute_Description"]) - set(additive["Attribute_Description"]))
    if skipped:
        logger.info(
            "PSD world: not aggregating non-quantity attribute(s) %s", skipped,
        )
    if additive.empty:
        return empty

    work = pd.DataFrame({
        "commodity":     additive["Commodity_Code"].map(code_to_name),
        "country":       additive["Country_Name"],
        "year":          pd.to_numeric(additive["Market_Year"], errors="coerce"),
        "attribute":     additive["Attribute_Description"],
        "value":         pd.to_numeric(additive["Value"], errors="coerce"),
        "unit":          additive["Unit_Description"],
    }).dropna(subset=["commodity", "year"])
    if work.empty:
        return empty

    # Units are part of the identity, never averaged: cotton is in bales,
    # everything else in 1000 MT. Two units on one (commodity, year,
    # attribute) is a source change, not something to reconcile silently.
    units_per_key = work.groupby(_AGG_KEYS)["unit"].transform("nunique")
    if (units_per_key > 1).any():
        mixed = sorted(set(work.loc[units_per_key > 1, "attribute"]))
        logger.warning(
            "PSD world: attribute(s) %s carry more than one unit in a single "
            "marketing year — no world row emitted for them", mixed,
        )
        work = work[units_per_key == 1]
        if work.empty:
            return empty

    # Every attribute of a (commodity, marketing year) must be reported by
    # the same country roster. USDA files them that way — a country that
    # reports nothing still appears with an explicit 0.0 — and it holds on
    # all 642 (commodity, year) pairs across the three 2026 bulk zips, zero
    # exceptions. So a short roster is not a quiet USDA choice, it is our
    # own loss: a partial parse, a truncated download, or rows dropped by
    # the unit gate above because some of them arrived stamped (PERCENT).
    #
    # `min_count=1` alone does not cover this — it withholds an all-NULL
    # group, but 90 missing countries out of 100 still sum to a plausible
    # world total with nothing in its shape marking it partial. That is the
    # 28-country-sum failure this whole module exists to eliminate, arriving
    # through a different door, so the row is withheld rather than emitted.
    roster = work.groupby(["commodity", "year"])["country"].transform("nunique")
    per_attribute = work.groupby(_AGG_KEYS)["country"].transform("nunique")
    short = per_attribute < roster
    if short.any():
        incomplete = sorted({
            (str(c), int(y), str(a))
            for c, y, a in work.loc[short, _AGG_KEYS].itertuples(index=False)
        })
        logger.warning(
            "PSD world: %d (commodity, year, attribute) group(s) are missing "
            "countries their siblings report — no world row emitted for them. "
            "First: %s", len(incomplete), incomplete[:3],
        )
        work = work[~short]
        if work.empty:
            return empty

    # min_count=1: an all-NULL group stays NULL. A blank is never a zero.
    world = (
        work.groupby([*_AGG_KEYS, "unit"], as_index=False)["value"]
        .sum(min_count=1)
        .dropna(subset=["value"])
    )
    world["country"] = WORLD

    china = work[work["country"] == _CHINA]
    if china.empty:
        return world[empty.columns]

    china = (
        china.groupby([*_AGG_KEYS, "unit"], as_index=False)["value"]
        .sum(min_count=1)
        .dropna(subset=["value"])
        .rename(columns={"value": "china"})
    )
    # An inner join withholds the line where China has no row: absence is
    # not a China zero, and a World-less-China that silently equals World
    # would be the most plausible-looking wrong number in the set.
    less = world.merge(china, on=[*_AGG_KEYS, "unit"], how="inner")
    less["value"] = less["value"] - less["china"]
    less["country"] = WORLD_LESS_CHINA

    return pd.concat(
        [world[empty.columns], less[empty.columns]], ignore_index=True
    )


def fetch_psd_commodity_group(group_name: str) -> pd.DataFrame:
    """
    Download a PSD bulk zip, extract the CSV, return raw DataFrame.

    Parameters
    ----------
    group_name : str
        Key in PSD_URLS, e.g. "oilseeds" or "grains".

    Returns
    -------
    pd.DataFrame
        Raw CSV contents — tens of thousands of rows before filtering.
    """
    url = PSD_URLS[group_name]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Downloading PSD %s data (%s) ...", group_name, url)
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            # Extract CSV from the zip archive in memory
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
                if not csv_names:
                    logger.warning("No CSV found in %s zip", group_name)
                    return pd.DataFrame()

                with zf.open(csv_names[0]) as f:
                    df = pd.read_csv(f, low_memory=False)

            logger.info(
                "PSD %s: downloaded %d rows, %d columns",
                group_name, len(df), len(df.columns),
            )
            return df

        except (requests.RequestException, zipfile.BadZipFile,
                pd.errors.ParserError, ValueError, KeyError) as exc:
            logger.warning(
                "Attempt %d/%d failed for PSD %s: %s",
                attempt, MAX_RETRIES, group_name, exc,
            )
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for PSD %s", MAX_RETRIES, group_name)
    return pd.DataFrame()


def _filter_psd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filter raw PSD data to just the commodities, countries, and attributes
    we track.

    The PSD CSV columns vary slightly between files, but generally include:
        Commodity_Code, Commodity_Description, Country_Name,
        Attribute_Description, Market_Year, Value, Unit_Description
    """
    if df.empty:
        return df

    # Build a set of target commodity codes for fast lookup
    target_codes = set(PSD_TARGET_COMMODITIES.values())

    # Filter by commodity code
    code_col = "Commodity_Code"
    if code_col not in df.columns:
        logger.warning("PSD CSV missing '%s' column — skipping filter", code_col)
        return pd.DataFrame()

    # Convert commodity code to string for matching
    df[code_col] = df[code_col].astype(str).str.strip()
    df = df[df[code_col].isin(target_codes)]

    # Units differ per commodity (grains in 1000 MT, cotton in 1000 480-lb
    # bales) — never guess a missing column. Checked before the world
    # aggregate, which gates on this column.
    if "Unit_Description" not in df.columns:
        raise ValueError(
            "PSD response missing Unit_Description column — refusing to "
            "assume units (cotton is in bales, not 1000 MT)"
        )

    # Build a reverse lookup: code → commodity name
    code_to_name = {v: k for k, v in PSD_TARGET_COMMODITIES.items()}

    # Filter by attribute *before* the world aggregate so the synthetic rows
    # cover exactly the attributes we store, and nothing more.
    attr_col = "Attribute_Description"
    if attr_col in df.columns:
        df = df[df[attr_col].isin(PSD_TARGET_ATTRIBUTES)]

    # The world total lives in the rows we are about to throw away, so take
    # it first (M15 #237). Everything below narrows to the tracked countries.
    world = _world_aggregates(df, code_to_name)

    # Filter by country. The PSD dataset spells some countries differently
    # from our display names ("Korea, South", "Cote d'Ivoire") — filter on
    # both spellings, then normalise to the display name so downstream
    # consumers see one consistent label. Without the aliases these
    # countries were silently dropped (isin simply matched nothing).
    country_col = "Country_Name"
    if country_col in df.columns:
        accepted = set(PSD_TARGET_COUNTRIES) | set(_PSD_COUNTRY_ALIASES)
        df = df[df[country_col].isin(accepted)]
        df[country_col] = df[country_col].replace(_PSD_COUNTRY_ALIASES)

    result = pd.DataFrame({
        "commodity": df[code_col].map(code_to_name),
        "country":   df[country_col],
        "year":      pd.to_numeric(df["Market_Year"], errors="coerce"),
        "attribute": df[attr_col],
        "value":     pd.to_numeric(df["Value"], errors="coerce"),
        "unit":      df["Unit_Description"],
    })

    result = result.dropna(subset=["commodity", "year"])
    if world.empty:
        return result
    return pd.concat([result, world], ignore_index=True)


def fetch_psd_all() -> dict[str, pd.DataFrame]:
    """
    Fetch oilseeds + grains + cotton PSD data, filter to target commodities/countries/attributes.

    Returns
    -------
    dict
        {commodity_name: DataFrame} — e.g. {"Soybeans": DataFrame, ...}
        Each DataFrame has columns: commodity, country, year, attribute, value, unit
    """
    all_filtered = []

    for group_name in PSD_URLS:
        raw = fetch_psd_commodity_group(group_name)
        if raw.empty:
            continue
        filtered = _filter_psd(raw)
        if not filtered.empty:
            all_filtered.append(filtered)
            logger.info(
                "PSD %s: kept %d rows after filtering", group_name, len(filtered),
            )

    if not all_filtered:
        logger.warning("No PSD data collected from any group")
        return {}

    combined = pd.concat(all_filtered, ignore_index=True)

    # Split by commodity name
    results = {}
    for commodity in combined["commodity"].unique():
        results[commodity] = combined[combined["commodity"] == commodity].reset_index(drop=True)

    logger.info(
        "PSD total: %d rows across %d commodities",
        len(combined), len(results),
    )
    return results


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_psd_all()
    for name, df in data.items():
        logger.info(
            "%s: %d rows, countries: %s",
            name, len(df), sorted(df["country"].unique()),
        )
