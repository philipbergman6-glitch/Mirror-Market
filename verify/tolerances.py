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
# The products a threshold table may key, and the one annotation it may carry.
PRODUCTS = ("bean", "meal", "oil")
_THRESHOLD_NOTE_KEYS = ("applied_to",)


class ToleranceFileError(ValueError):
    """The tolerance file is missing a stratum, or a field a grade needs."""


class UnknownProductError(ValueError):
    """A leg names no product the tolerance file thresholds, or more than one."""


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _check_threshold(where: str, threshold: Any) -> None:
    """A threshold is one number, or a product table: numeric values, known keys only."""
    if _is_number(threshold):
        return
    if not isinstance(threshold, dict) or not threshold:
        raise ToleranceFileError(f"{where}: threshold_usd_mt {threshold!r} is neither a number nor a product table")
    for key, value in threshold.items():
        if key in _THRESHOLD_NOTE_KEYS:
            continue
        if key != "default" and key not in PRODUCTS:
            raise ToleranceFileError(f"{where}: threshold_usd_mt keys {key!r}, not a product {PRODUCTS} or 'default'")
        if not _is_number(value):
            raise ToleranceFileError(f"{where}: threshold_usd_mt[{key!r}] = {value!r} is not a number")


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

    def product_of(self, leg_label: str) -> str:
        """The one product a leg's displayed label names; none, or two, is refused."""
        table: dict[str, list[str]] = self.data["leg_products"]
        label = leg_label.casefold()
        named = [p for p in PRODUCTS if any(word.casefold() in label for word in table[p])]
        if len(named) != 1:
            raise UnknownProductError(
                f"leg {leg_label!r} names {'no product' if not named else 'products ' + ', '.join(named)}: "
                f"its label must contain the words of exactly one of {PRODUCTS}"
            )
        return named[0]

    @staticmethod
    def threshold_usd_mt(block: dict[str, Any], product: str) -> float:
        """The block's economic threshold for one product: its own entry, else ``default``, else refused."""
        if product not in PRODUCTS:
            raise UnknownProductError(f"product {product!r} is not one of {PRODUCTS}")
        threshold: Any = block.get("threshold_usd_mt")
        _check_threshold(str(block.get("name", "stratum")), threshold)
        if not isinstance(threshold, dict):  # _check_threshold proved it a number
            return float(threshold)
        value = threshold.get(product, threshold.get("default"))
        if value is None:
            raise ToleranceFileError(
                f"{block.get('name', 'stratum')}: threshold_usd_mt {threshold!r} names no {product!r} and no 'default'"
            )
        return float(value)

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
        if block["kind"] != "unverifiable":
            if block.get("threshold_usd_mt") is None:
                raise ToleranceFileError(f"{path}: stratum {key} has no threshold_usd_mt")
            _check_threshold(f"{path}: stratum {key}", block["threshold_usd_mt"])
    products = data.get("leg_products")
    if not isinstance(products, dict) or sorted(products) != sorted(PRODUCTS) or not all(
        isinstance(words, list) and words and all(isinstance(w, str) and w for w in words)
        for words in products.values()
    ):
        raise ToleranceFileError(f"{path}: 'leg_products' must map each of {PRODUCTS} to a non-empty list of words")
    return Tolerances(version=version, sha256=hashlib.sha256(raw).hexdigest(), data=data)
