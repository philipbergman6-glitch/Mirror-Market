"""Per-layer run instrumentation. Standard library only.

Before this existed, ``data_freshness`` carried one timestamp per layer,
written after the save — so "when did we fetch" and "when did we store"
were the same number and the fetch/display split the whole latency question
turns on was unmeasurable. It was not *wrong*, it was one stamp doing three
jobs.

``LayerClock`` records the three instants a layer run actually has, and
nothing else. It does not decide anything, does not touch the database and
does not know what a latency objective is: the runner stamps it, the store
persists it, and ``latency.measure`` interprets it.

The registry is process-global because the pipeline's freshness writes are
scattered across eight ``_mark_*`` helpers in ``main.py``, each of which
would otherwise need the clock threaded through it. A global keyed by layer
is the smaller evil: the alternative is eight signatures carrying a
parameter that only one of them varies.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class LayerClock:
    """The instants one layer's run passed through.

    ``observed_on`` is the newest observation *date* the run received — the
    layer's own answer to "how new is the data I just got", filled in by the
    runner after cleaning, because that is the first point at which the
    frames speak the cleaners' date conventions.
    """

    layer: str
    fetch_started_at: datetime = field(default_factory=_utc_now)
    fetch_completed_at: datetime | None = None
    stored_at: datetime | None = None
    observed_on: date | None = None

    def fetched(self) -> None:
        self.fetch_completed_at = _utc_now()

    def stored(self) -> None:
        self.stored_at = _utc_now()

    def observed(self, observed_on: date | None) -> None:
        self.observed_on = observed_on


_CLOCKS: dict[str, LayerClock] = {}


def start(layer: str) -> LayerClock:
    """Begin timing ``layer``, replacing any previous clock for it."""
    clock = LayerClock(layer)
    _CLOCKS[layer] = clock
    return clock


def get(layer: str) -> LayerClock | None:
    return _CLOCKS.get(layer)


def reset() -> None:
    """Clear every clock. Called at the top of a pipeline run, and by tests."""
    _CLOCKS.clear()


def snapshot() -> dict[str, LayerClock]:
    return dict(_CLOCKS)
