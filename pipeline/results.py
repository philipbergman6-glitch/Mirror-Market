"""
Typed fetch result for the pipeline's fetch stage.

Distinguishes three states the main loop must treat differently:

* ``ok``      — fetch succeeded and produced rows.
* ``empty``   — fetch succeeded but the filter/window returned zero rows.
                This is normal (no inspection report this week, no contract
                trades that day) and must NOT be reported as a failure.
* ``failed``  — the underlying source/transport/parse failed. The freshness
                row should be recorded with ``status='failed'`` so the
                dashboard can show "last good run was X days ago".

``ScraperShapeError`` is raised by HTML parsers when the page no longer has
the expected structure (missing column, wrong row count, etc). It is the
canonical "the upstream site changed" signal — distinct from a network
error or empty result.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

import pandas as pd

FetchStatus = Literal["ok", "empty", "failed"]


class ScraperShapeError(ValueError):
    """Raised when scraped HTML does not match expected structure.

    Subclasses ``ValueError`` so existing ``except ValueError`` blocks
    continue to catch it, but callers can distinguish a shape problem
    from a generic value error when needed.
    """


@dataclass(frozen=True)
class FetchResult:
    """Typed return value for every fetcher.

    ``data`` is the per-key DataFrame mapping (commodity name → DataFrame).
    For ``status='failed'`` it is always an empty dict.
    For ``status='empty'`` it is empty *or* contains empty DataFrames.
    For ``status='ok'`` at least one DataFrame has rows.
    """

    data: dict[str, pd.DataFrame] = field(default_factory=dict)
    status: FetchStatus = "ok"
    error: str | None = None

    @classmethod
    def ok(cls, data: dict[str, pd.DataFrame]) -> FetchResult:
        return cls(data=data, status="ok")

    @classmethod
    def empty(cls, reason: str | None = None) -> FetchResult:
        return cls(data={}, status="empty", error=reason)

    @classmethod
    def failed(cls, error: str) -> FetchResult:
        return cls(data={}, status="failed", error=error)

    @property
    def total_rows(self) -> int:
        return sum(len(df) for df in self.data.values())

    @property
    def has_rows(self) -> bool:
        return any(not df.empty for df in self.data.values())
