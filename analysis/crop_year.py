"""Crop-year label ↔ USDA PSD Market_Year mapping (#403).

National crop agencies label a crop by a split agricultural year
(CONAB: ``"2025/26"``); USDA's PSD keys the same crop to an integer
``Market_Year`` — the *first* year of its own split label (``2025`` =
"2025/26"). Whether the two labels coincide is a property of the agency's
convention, so the offset lives on the market descriptor
(``config.MARKETS[...]["crop_estimates"]["psd_year_offset"]``) and this
module only does the arithmetic. A label that does not parse is a hard
failure: a guessed year would pair two different crops silently
(invariant 1).
"""

from __future__ import annotations

import re

_SPLIT_YEAR = re.compile(r"^(\d{4})/(\d{2})$")


def is_split_crop_year(label: object) -> bool:
    """True for an agency's split label (``"2025/26"``), False for anything else.

    CONAB labels some crops by calendar year (wheat: ``"2025"``). That is a
    different convention, not a malformed label, and no PSD mapping is
    defined for it — callers that compare across agencies skip the
    comparison and say why, rather than guess a Market_Year or crash the
    surface they are rendering.
    """
    return _SPLIT_YEAR.match(str(label).strip()) is not None


def psd_year_for_crop_year(label: str, offset: int) -> int:
    """PSD ``Market_Year`` for an agency's split crop-year label.

    ``"2025/26"`` with offset 0 → 2025. The two-digit half must be the
    first year + 1 (``1999/00`` is fine); anything else is malformed.
    """
    match = _SPLIT_YEAR.match(str(label).strip())
    if not match:
        raise ValueError(f"crop year label {label!r} is not of the form YYYY/YY")
    first = int(match.group(1))
    if int(match.group(2)) != (first + 1) % 100:
        raise ValueError(f"crop year label {label!r} does not span consecutive years")
    return first + offset


def psd_year_label(year: int) -> str:
    """USDA's split-year label for a PSD ``Market_Year`` (2025 → ``"2025/26"``)."""
    return f"{int(year)}/{(int(year) + 1) % 100:02d}"
