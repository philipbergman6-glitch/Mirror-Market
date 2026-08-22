"""Reconcile the synthesised PSD World row against USDA's own R00 row.

Layer 6 reconstructs the world total by summing every country in the bulk
CSVs *before* the country filter (M15 #237). That is only correct as long as
USDA keeps publishing member states and their aggregates in disjoint
marketing years — their editorial choice, not a contract we control. This
script is how that assumption is re-checked on demand.

It is deliberately **not** part of the pipeline: the bulk zips need no key,
and wiring the API into Layer 6 would newly gate a key-free layer behind
`FAS_API_KEY` (research/2026-08-12-m15-psd-world-stocks-to-use.md §6.1).
The API is also one request per (commodity, marketing year), so a full
history sweep is ~630 calls.

Usage
-----
    FAS_API_KEY=... python scripts/reconcile_psd_world.py
    FAS_API_KEY=... python scripts/reconcile_psd_world.py --years 1990 2003 2025

Exit code is 1 on any mismatch, so it can be run as a check.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (  # noqa: E402  (must follow sys.path.insert above)
    FAS_API_KEY,
    PSD_TARGET_COMMODITIES,
    PSD_URLS,
    REQUEST_TIMEOUT,
    setup_logging,
)
from fetchers.psd import (  # noqa: E402  (must follow sys.path.insert above)
    WORLD,
    _filter_psd,
    fetch_psd_commodity_group,
)

logger = logging.getLogger(__name__)

_API = "https://api.fas.usda.gov/api/psd/commodity/{code}/world/year/{year}"
_ATTRIBUTES_API = "https://api.fas.usda.gov/api/psd/commodityAttributes"
_UNITS_API = "https://api.fas.usda.gov/api/psd/unitsOfMeasure"
_AUTH_HEADER = "X-Api-Key"

# The API answers in codes where the bulk CSVs answer in words: a row carries
# `attributeId` / `unitId`, never the descriptions `_filter_psd` stores. Both
# lookups are small, static, and fetched once per run.

# The API rejects the unpadded codes the bulk CSVs use: pandas reads
# Commodity_Code as an int, so "0813100" arrives as "813100" and
# /commodity/813100/... answers 404 while /commodity/0813100/... answers 200.
_CODE_WIDTH = 7

# Default probe set: the three commodities the ticket names, on marketing
# years that straddle the two layout changes the summation depends on —
# 1985 predates both the EU aggregate and the Soviet breakup, 1995 sits
# inside EU-15, 2003 is the first European Union year, 2025 is current.
_DEFAULT_COMMODITIES = ("Soybeans", "Corn", "Wheat")
_DEFAULT_YEARS = (1985, 1995, 2003, 2025)


def _get(url: str) -> list[dict]:
    resp = requests.get(
        url, headers={_AUTH_HEADER: FAS_API_KEY}, timeout=REQUEST_TIMEOUT
    )
    resp.raise_for_status()
    return resp.json()


def _lookup(url: str, id_field: str, name_field: str) -> dict[int, str]:
    """A PSD code table as {id: description}."""
    return {int(row[id_field]): str(row[name_field]).strip() for row in _get(url)}


def _api_world_row(
    code: str, year: int, attributes: dict[int, str], units: dict[int, str]
) -> dict[str, tuple[float, str]]:
    """USDA's own World row as {attribute name: (value, unit)}."""
    rows = _get(_API.format(code=code.zfill(_CODE_WIDTH), year=year))
    out: dict[str, tuple[float, str]] = {}
    for row in rows:
        if str(row.get("countryCode", "")).strip() != "00":
            continue
        attribute_id = int(row["attributeId"])
        if attribute_id not in attributes:
            # An id absent from the code table means the table moved under
            # us; guessing at the name would compare two different things.
            raise ValueError(
                f"PSD attributeId {attribute_id} is not in the API's own "
                f"attribute table — cannot name the row to compare it"
            )
        out[attributes[attribute_id]] = (
            float(row["value"]),
            units.get(int(row["unitId"]), ""),
        )
    return out


def _synthesised_world(commodities: tuple[str, ...]) -> pd.DataFrame:
    """The World rows Layer 6 would store, from the same bulk zips it uses."""
    frames = []
    for group in PSD_URLS:
        raw = fetch_psd_commodity_group(group)
        if raw.empty:
            raise SystemExit(f"PSD {group} download returned nothing — cannot reconcile")
        frames.append(_filter_psd(raw))
    combined = pd.concat(frames, ignore_index=True)
    return combined[
        (combined["country"] == WORLD) & (combined["commodity"].isin(commodities))
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commodities", nargs="+", default=list(_DEFAULT_COMMODITIES))
    parser.add_argument("--years", nargs="+", type=int, default=list(_DEFAULT_YEARS))
    args = parser.parse_args()

    setup_logging()
    if not FAS_API_KEY:
        # Hard-fail rather than skip: a reconciliation that quietly does
        # nothing is worse than no reconciliation at all.
        logger.error("FAS_API_KEY is not set — the R00 row cannot be fetched")
        return 1

    attributes = _lookup(_ATTRIBUTES_API, "attributeId", "attributeName")
    units = _lookup(_UNITS_API, "unitId", "unitDescription")
    world = _synthesised_world(tuple(args.commodities))
    mismatches = 0
    checked = 0

    for commodity in args.commodities:
        code = PSD_TARGET_COMMODITIES[commodity]
        for year in args.years:
            ours = world[(world["commodity"] == commodity) & (world["year"] == year)]
            theirs = _api_world_row(code, year, attributes, units)
            if ours.empty or not theirs:
                logger.warning(
                    "%s MY%d: no %s row — ours=%d attributes, USDA=%d",
                    commodity, year, "bulk" if ours.empty else "API",
                    len(ours), len(theirs),
                )
                continue
            for _, row in ours.iterrows():
                attribute = str(row["attribute"])
                if attribute not in theirs:
                    logger.warning(
                        "%s MY%d %s: USDA's World row does not carry it",
                        commodity, year, attribute,
                    )
                    continue
                checked += 1
                their_value, their_unit = theirs[attribute]
                our_unit = str(row["unit"]).strip()
                if our_unit and their_unit and our_unit != their_unit:
                    # Equal numbers in different units are not agreement.
                    mismatches += 1
                    logger.error(
                        "%s MY%d %s: bulk unit %s != USDA unit %s",
                        commodity, year, attribute, our_unit, their_unit,
                    )
                elif float(row["value"]) != their_value:
                    mismatches += 1
                    logger.error(
                        "%s MY%d %s: bulk sum %s != USDA World %s",
                        commodity, year, attribute, row["value"], their_value,
                    )

    logger.info(
        "Reconciled %d attribute values across %d commodities × %d years: "
        "%d mismatch(es)",
        checked, len(args.commodities), len(args.years), mismatches,
    )
    return 1 if mismatches or not checked else 0


if __name__ == "__main__":
    sys.exit(main())
