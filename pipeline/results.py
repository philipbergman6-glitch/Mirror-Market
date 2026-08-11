"""
Typed fetch result for the pipeline's fetch stage.

Distinguishes three states the main loop must treat differently:

* ``ok``      — fetch succeeded and produced rows.
* ``empty``   — fetch succeeded but the filter/window returned zero rows.
                This is normal (no inspection report this week, no contract
                trades that day) and must NOT be reported as a failure.
* ``failed``  — the underlying source/transport/parse failed. The freshness
                row should be recorded with ``status='failed'`` so the
                dashboard can show "last good run was X days ago". A failed
                result may still carry rows (see ``FetchResult.partial``):
                the rows are saved, only the verdict changes.

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
    For ``status='failed'`` it is an empty dict, unless the result came from
    ``partial()`` — see there for why a failure may carry rows.
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

    @classmethod
    def partial(cls, data: dict[str, pd.DataFrame], error: str) -> FetchResult:
        """Some keys came back, others failed on transport/parse.

        Graded ``failed`` while still carrying its rows, which is the
        project's established "save first, grade second" shape (#157): the
        rows that *did* arrive are stored — for a source that serves only
        the current day, dropping them would punch a permanent hole in
        history — but ``last_success`` must not advance off a run that
        only half happened.

        Distinct from ``empty``: a key that returned zero rows was asked
        and answered (mandis closed, no report this week). A key that
        failed transport was never answered at all, so its absence says
        nothing about whether data existed. Recording that as a success
        is the silent failure this project prefers to crash over.
        """
        return cls(data=data, status="failed", error=error)

    @property
    def total_rows(self) -> int:
        return sum(len(df) for df in self.data.values())

    @property
    def has_rows(self) -> bool:
        return any(not df.empty for df in self.data.values())
