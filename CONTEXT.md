# Mirror-Market — Domain Glossary

## Weather

**Pin**
A single named forecast point (one lat/lon) whose weather prices a rendered leg — e.g. "Brazil Mato Grosso".
_Avoid_: region (when meaning the point), station

**Belt**
The named set of pins that prices one rendered leg — e.g. Palm = Riau + Sabah.
_Avoid_: footprint (different concept), zone

**Footprint**
An area outline (admin-1 or county polygons, optionally production-weighted) whose weather is averaged as a whole — e.g. "the Mato Grosso footprint".
_Avoid_: belt, region, polygon (in prose)

## Hazards

**Hazard flag**
A dated warning attached to a rendered leg because a hazard (e.g. a tropical cyclone) threatens a place that prices it — a port or a footprint.
_Avoid_: signal (signals are price-derived), alert (an alert is a severity level, not a thing)

## Relationships

- A **belt** has one or more **pins**; a **footprint** is independent of pins and may contain several.
- A **hazard flag** attaches to a leg via a place — a port or a **footprint**.

## Flagged ambiguities

- "belt" was about to mean an area outline (2026-10-05 God's Eye View map) — resolved: it keeps its existing meaning (set of pins); the outline is a **footprint**.
