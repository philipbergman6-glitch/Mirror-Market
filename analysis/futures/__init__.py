"""Futures workstation and hedge workflow (Phase 3).

The trader question this package answers: *I have a physical exposure. What
hedge does it call for, what risk is left after I place it, and what would have
to move before that matters?*

Read in this order:

``domain``      the vocabulary — named contracts, contract specifications,
                expiry rules, price types, providers, roll methods. Standard
                library only.
``providers``   the one module that knows SQL exists; turns stored rows into
                named contracts and checks a curve is a single session.
``curve``       term structure over named contracts: calendar spreads,
                annualised carry, days to expiry, spread percentiles.
``continuous``  a research series stitched from named contracts, with its roll
                method carried as data rather than documentation.
``hedge``       the sizing calculator: contracts, coverage, residual, basis and
                FX exposure, crush cross-hedge.
``scenarios``   combined futures, basis, FX and crush-yield moves, netted
                against the hedge.
``ticket``      the proposal export. Not routed, and it says so first.
``positions``   the entered book, its marks, its P&L attribution and its limits.
``events``      scheduled releases for the sources this project ingests.
``options``     the chain/vol/Greeks interface, an honest unavailable state,
                and Black-76 under stated assumptions.
``alerts``      exposure alerts — expiry, roll, slippage, basis, limits,
                inversion, staleness.

Three rules run through all of it:

**A named contract is not a continuous series**, and the two are different
types so the confusion cannot be made silently.

**A delayed reference price is not a settlement.** Everything here is stamped
with what it actually is, and no surface may upgrade it.

**Nothing is routed.** There is no order path, no venue connection, and no
intention to add one.
"""

from analysis.futures.domain import (
    METHOD_VERSION,
    ContinuousSeries,
    ContractQuote,
    ContractSpec,
    ExpiryConfidence,
    Freshness,
    NamedContract,
    PriceType,
    Provider,
    RollMethod,
    Side,
    named_contract,
    parse_symbol,
    spec_for,
)

__all__ = [
    "METHOD_VERSION",
    "ContinuousSeries",
    "ContractQuote",
    "ContractSpec",
    "ExpiryConfidence",
    "Freshness",
    "NamedContract",
    "PriceType",
    "Provider",
    "RollMethod",
    "Side",
    "named_contract",
    "parse_symbol",
    "spec_for",
]
