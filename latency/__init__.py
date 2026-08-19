"""Latency — what the product's numbers are worth in time.

One vocabulary for a question every other package answers by accident:
**how old is the number on the page, and which part of that age did we
cause?** ``domain`` is the stdlib-only vocabulary, ``clock`` the
instrumentation the pipeline stamps, ``measure`` the single SQL-aware seam,
and ``report`` the rendering.

See ``LATENCY.md`` for the measured baseline and the objectives.
"""

from latency.domain import (
    LATENCY_CLASS_BY_LAYER,
    OBJECTIVES,
    LatencyClass,
    LatencyObjective,
    LayerLatency,
    Stage,
    StageStamps,
    Verdict,
)

__all__ = [
    "LATENCY_CLASS_BY_LAYER",
    "OBJECTIVES",
    "LatencyClass",
    "LatencyObjective",
    "LayerLatency",
    "Stage",
    "StageStamps",
    "Verdict",
]
