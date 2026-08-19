"""Origin comparison and landed-origin economics (Phase 2).

The trader question this package answers: *for a named shipment window, which
origin — United States, Brazil or Argentina — is economically preferable for
delivery to a named destination?*

Read in this order:

``domain``       the vocabulary — quotes, windows, incoterms, waterfalls,
                 comparability verdicts. Standard library only.
``assumptions``  the hand-entered input contract: who entered a freight
                 number, when, for which route and window, and when it lapses.
``validation``   the faults one entry cannot see about itself — an ambiguous
                 pair, a scope too wide to mean anything, a key that matches
                 no route. Errors are raised at load; expiry is reported.
``readiness``    the onboarding view: what each route still needs, the command
                 that supplies it, and the renewal queue. Reads no database.
``sources``      the one module that knows SQL exists; turns registry legs
                 into ``OriginQuote`` values already in USD/MT.
``landed_cost``  the canonical, versioned calculation. One order of
                 operations, used by every surface.
``comparison``   alignment rules and the ranking.
``scenarios``    sensitivity: what has to move before the answer changes.
``crush``        board, gross physical, and estimated net plant margins —
                 three numbers that are not the same number.
``players``      who could actually do the trade, with evidence and its age.
``history``      what was published, and whether it moved because the market
                 did or because an input was re-entered.

The rule running through all of it: a number that cannot be computed honestly
is not computed. Missing freight blocks a row, a mismatched shipment window
excludes it from the ranking, and both say so on the page. There are no
defaults, no fallbacks to a wider route, and no placeholder values anywhere in
production output.
"""

from analysis.origins.domain import (
    Comparability,
    Confidence,
    CostComponent,
    Incoterm,
    LandedCost,
    OriginQuote,
    OriginRanking,
    ShipmentWindow,
)

__all__ = [
    "Comparability",
    "Confidence",
    "CostComponent",
    "Incoterm",
    "LandedCost",
    "OriginQuote",
    "OriginRanking",
    "ShipmentWindow",
]
