"""Typed structured output of the briefing.

`BriefingData` captures everything the orchestrator produces: the
formatted text, per-section text blocks (for consumers that want to
render one section at a time), the list of detected signals, and the
DataFrame dicts that several sections share. The dashboard and other
downstream consumers can pull from BriefingData instead of re-running
the briefing pipeline.

The per-section `compute()` / `format(SectionData)` split mentioned in
the package docstring is an incremental refactor: today the orchestrator
calls each section's `format()` directly and stores the result in
`BriefingData.section_texts`. Splitting individual sections is a
follow-up — the current shape lets a consumer hold the typed output
even before that split happens.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class BriefingData:
    """Structured output of a single briefing run."""

    text: str
    section_texts: dict[str, str] = field(default_factory=dict)
    signals: list[dict] = field(default_factory=list)
    enriched: dict[str, pd.DataFrame] = field(default_factory=dict)
    price_data: dict[str, pd.DataFrame] = field(default_factory=dict)
    currency_data: dict[str, pd.DataFrame] = field(default_factory=dict)

    def section(self, name: str) -> str:
        """Return a single section by name (e.g. 'prices', 'market_drivers')."""
        return self.section_texts.get(name, "")
