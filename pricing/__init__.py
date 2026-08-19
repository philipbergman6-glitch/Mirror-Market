"""What a price *is*, shared by every surface that renders one.

Deliberately a top-level package rather than a module inside ``analysis`` or
``app``: the classification is consumed by ``analysis`` (origins, futures,
opportunities), by ``app`` (the site registry and its templates) and by
``trust`` (the contract registry's identity fields), and any home inside one of
those three would have inverted somebody's dependency direction.
"""

from pricing.semantics import (
    CONFIDENCE_CEILING,
    CONFIDENCE_RANK,
    NON_PRICE_QUOTE_KINDS,
    PROVEN_SETTLEMENT_SOURCES,
    QUOTE_KIND_LABELS,
    QUOTE_KIND_PRICE_TYPE,
    Confidence,
    PriceType,
    is_price_quote_kind,
    is_settlement_proven_source,
    price_type_for_quote_kind,
    quote_kind_label,
    worst_confidence,
)

__all__ = [
    "CONFIDENCE_CEILING",
    "CONFIDENCE_RANK",
    "NON_PRICE_QUOTE_KINDS",
    "PROVEN_SETTLEMENT_SOURCES",
    "QUOTE_KIND_LABELS",
    "QUOTE_KIND_PRICE_TYPE",
    "Confidence",
    "PriceType",
    "is_price_quote_kind",
    "is_settlement_proven_source",
    "price_type_for_quote_kind",
    "quote_kind_label",
    "worst_confidence",
]
