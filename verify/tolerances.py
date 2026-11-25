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
STRATUM_KINDS = ("exact", "band", "recompute", "unverifiable")


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

    def threshold_usd_mt(self, block: dict[str, Any], leg_label: str) -> float:
        """The block's economic threshold for one leg: its ``oil`` entry for an oil leg, else ``default``."""
        threshold = block.get("threshold_usd_mt")
        if isinstance(threshold, (int, float)):
            return float(threshold)
        if not isinstance(threshold, dict) or not isinstance(threshold.get("default"), (int, float)):
            raise ToleranceFileError(f"threshold_usd_mt {threshold!r} names no numeric 'default'")
        is_oil = any(word.casefold() in leg_label.casefold() for word in self.data["oil_leg_label_contains"])
        if is_oil and isinstance(threshold.get("oil"), (int, float)):
            return float(threshold["oil"])
        return float(threshold["default"])

    @staticmethod
    def native_allowance(block: dict[str, Any], decimals: int) -> float:
        """The native tolerance, plus half a unit in the last displayed place when the block says so."""
        native = block.get("native_tolerance")
        if not isinstance(native, dict) or not isinstance(native.get("value"), (int, float)):
            raise ToleranceFileError(f"native_tolerance {native!r} names no numeric 'value'")
        display = 0.5 * 10 ** -decimals if "display rounding" in str(native.get("plus", "")) else 0.0
        return float(native["value"]) + display


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
        if block["kind"] not in STRATUM_KINDS:
            raise ToleranceFileError(f"{path}: stratum {key} kind {block['kind']!r} is not one of {STRATUM_KINDS}")
        if block["kind"] != "unverifiable" and block.get("threshold_usd_mt") is None:
            raise ToleranceFileError(f"{path}: stratum {key} has no threshold_usd_mt")
    if not data.get("oil_leg_label_contains"):
        raise ToleranceFileError(f"{path}: 'oil_leg_label_contains' is empty")
    return Tolerances(version=version, sha256=hashlib.sha256(raw).hexdigest(), data=data)
