"""The verifier's tolerance file (``tolerances.json``): load, validate, stamp.

Tolerances live here, inside the verifier, never in the product's
``config.py`` (owner, 2026-11-25, #479). Every record stamps the version it was
graded under — the file's human ``version`` plus its content hash — and a
record, once written, is never re-graded.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_PATH = Path(__file__).with_name("tolerances.json")

# Basket v1 (spec #639): the file must carry a block for every stratum.
BASKET_V1_STRATA = ("1", "1b", "2", "3", "3u", "4", "5", "6", "7", "8", "9", "10")
KINDS = ("exact", "band", "recompute", "unverifiable")


class ToleranceFileError(ValueError):
    """The tolerance file is missing a stratum, or a field a grade needs."""


@dataclass(frozen=True)
class Tolerances:
    version: int
    sha256: str
    data: dict[str, Any]

    @property
    def stamp(self) -> str:
        return f"v{self.version} sha256:{self.sha256[:12]}"

    def stratum(self, key: str) -> dict[str, Any]:
        block: dict[str, Any] = self.data["strata"][key]
        return block

    def for_series(self, stratum: str, series: str) -> dict[str, Any]:
        """The stratum block with the series' own overrides applied (stratum 6's pairs)."""
        block = dict(self.stratum(stratum))
        pairs = block.pop("pairs", None)
        if pairs is not None:
            if series not in pairs:
                raise ToleranceFileError(f"stratum {stratum} has no tolerance block for {series}")
            block.update(pairs[series])
        return block

    def threshold_usd_mt(self, leg_label: str) -> float:
        """The economic threshold for a leg: the oil threshold for an oil leg, else bean-scale."""
        thresholds = self.data["economic_thresholds_usd_mt"]
        is_oil = any(word in leg_label for word in self.data["oil_leg_label_contains"])
        return float(thresholds["oil"] if is_oil else thresholds["bean_scale_and_derived"])


def load(path: Path = DEFAULT_PATH) -> Tolerances:
    raw = path.read_bytes()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ToleranceFileError(f"{path}: not JSON ({exc})") from exc
    version = data.get("version")
    if not isinstance(version, int) or version < 1:
        raise ToleranceFileError(f"{path}: 'version' must be a positive integer, got {version!r}")
    strata = data.get("strata")
    if not isinstance(strata, dict):
        raise ToleranceFileError(f"{path}: no 'strata' table")
    missing = [key for key in BASKET_V1_STRATA if key not in strata]
    if missing:
        raise ToleranceFileError(f"{path}: no block for basket stratum/strata {missing}")
    for key, block in strata.items():
        for field in ("kind", "tier", "since"):
            if field not in block:
                raise ToleranceFileError(f"{path}: stratum {key} has no {field!r}")
        if block["kind"] not in KINDS:
            raise ToleranceFileError(f"{path}: stratum {key} kind {block['kind']!r} is not one of {KINDS}")
    thresholds = data.get("economic_thresholds_usd_mt", {})
    for field in ("bean_scale_and_derived", "oil"):
        if not isinstance(thresholds.get(field), (int, float)):
            raise ToleranceFileError(f"{path}: economic_thresholds_usd_mt.{field} must be a number")
    if not data.get("oil_leg_label_contains"):
        raise ToleranceFileError(f"{path}: 'oil_leg_label_contains' is empty")
    return Tolerances(version=version, sha256=hashlib.sha256(raw).hexdigest(), data=data)
