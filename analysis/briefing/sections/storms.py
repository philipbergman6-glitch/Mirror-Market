"""STORMS — cyclone hazard flags on the port legs (S2 #374 §9, slice 5 #391).

The rules live in ``analysis.hazards``: this section calls the same
``assess_places`` and the same sentence builders as the site's ledger chip and
block 06, so the briefing and the page cannot disagree about a port. This
module only renders.

Every line is a statement about a *place* that prices a ledger leg, never a
market banner. Three things the wording guards (spec §6.4, §9):

- ``not_covered`` is never "no storm". The South Atlantic has no publishable
  cyclone source, so its ports print one line every day saying so.
- A failed or stale read prints ``NOT ASSESSED`` — upper case because it is a
  statement about us, in the manner of the weather section's "not assessed".
- Every time is absolute UTC (§8.1); a lead time only ever follows one,
  anchored to its advisory.

Hazard flags are not added to the ``signals`` section and not to
``market_drivers``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd

import config
from analysis.hazards import (
    PlaceHazard,
    active_places,
    approach,
    assess_places,
    current_storms,
    flag_sentence,
    format_check_time,
    hourly_track,
    newest_status,
    watch_sentence,
)
from pipeline.query import (
    read_cyclone_status,
    read_cyclone_storms,
    read_cyclone_track,
    read_freshness,
)

_SEVERITY_RANK = {"alert": 0, "warning": 1, "info": 2}
_NOT_COVERED_CODA = "Absence of a flag is not a clear reading."


def _now() -> datetime:
    """The page-generation clock — one seam, so tests pin it."""
    return datetime.now(timezone.utc)


def _layer_dicts(freshness: pd.DataFrame) -> tuple[dict[str, str | None], dict[str, str | None]]:
    """``data_freshness`` → (status per layer, last_success per layer)."""
    states: dict[str, str | None] = {}
    last: dict[str, str | None] = {}
    if freshness is None or freshness.empty or "layer_name" not in freshness.columns:
        return states, last
    for _, row in freshness.iterrows():
        layer = str(row["layer_name"])
        state = row.get("status")
        states[layer] = None if state is None or pd.isna(state) else str(state)
        stamp = row.get("last_success")
        last[layer] = None if stamp is None or pd.isna(stamp) else str(stamp)
    return states, last


def _header(status: pd.DataFrame) -> str:
    """``STORMS (NHC + JTWC, checked 19:04Z 6 Oct):`` — each source's own stamp when they differ."""
    sources = [str(b["source"]) for b in config.CYCLONE_BASINS.values() if b.get("source")]
    sources = list(dict.fromkeys(sources))
    newest = newest_status(status)
    stamps = {s: format_check_time(str(newest[s]["checked_at"])) if s in newest else None for s in sources}
    distinct = {v for v in stamps.values() if v is not None}
    if len(distinct) == 1 and all(v is not None for v in stamps.values()):
        return f"STORMS ({' + '.join(sources)}, checked {distinct.pop()}):"
    parts = [f"{s} checked {stamps[s]}" if stamps[s] else f"{s} never answered" for s in sources]
    return f"STORMS ({', '.join(parts)}):"


def _place_rank(p: PlaceHazard) -> tuple[int, int, int, int]:
    """Worst first: flags before watches, alert before info, earlier arrival, closer approach."""
    state = 0 if p.state == "flag" else 1
    severity = _SEVERITY_RANK.get(p.severity or "", 9)
    return (state, severity, p.first_arrival_tau_h or 10**6, p.closest_km or p.closest_ts_km or 10**6)


def _legs_for(place_id: str) -> list[str]:
    """Ledger legs priced at this place, in registry order, by their ledger label."""
    return [
        str(config.LEDGER_LEGS[leg_id]["label"])
        for leg_id, place_ids in config.LEG_PLACES.items()
        if place_id in place_ids
    ]


def _storm_entry(storm: pd.Series) -> str:
    name = storm.get("name")
    name = str(name) if name is not None and not pd.isna(name) else str(storm["storm_id"])
    vmax = storm.get("vmax_kt")
    if storm.get("track_state") != "ok" or vmax is None or pd.isna(vmax):
        return f"{name} ({storm['source']}, track absent)"
    return f"{name} ({storm['source']}, {int(vmax)} kt)"


def _nearest_to_a_covered_port(
    current: pd.DataFrame, track: pd.DataFrame, covered: list[PlaceHazard], places: dict[str, dict[str, Any]],
) -> str | None:
    """``Choi-wan, 2,488 km from North China (Qingdao)`` — the closest forecast
    approach of any current storm with a readable track to any port that was
    actually assessed (flag, watch or clear). None when nothing can be struck."""
    best: tuple[float, str, str] | None = None
    for _, storm in current.iterrows():
        if storm["track_state"] != "ok":
            continue
        rows = track[
            (track["source"] == storm["source"])
            & (track["storm_id"] == storm["storm_id"])
            & (track["issued_at"] == storm["issued_at"])
        ]
        if rows.empty:
            continue
        samples = hourly_track(rows)
        for p in covered:
            raw = places[p.place_id]
            a = approach(float(raw["lat"]), float(raw["lon"]), samples)
            if a.closest_km is not None and (best is None or a.closest_km < best[0]):
                name = storm.get("name")
                name = str(name) if name is not None and not pd.isna(name) else str(storm["storm_id"])
                best = (a.closest_km, name, p.short)
    if best is None:
        return None
    km, name, short = best
    return f"{name}, {round(km):,} km from {short}"


def _not_covered_line(not_covered: list[PlaceHazard]) -> str:
    groups: dict[str, list[str]] = {}
    for p in not_covered:
        groups.setdefault(str(p.reason), []).append(p.short)
    body = "; ".join(f"{', '.join(shorts)} — {reason}" for reason, shorts in groups.items())
    return f"  Not covered: {body}. {_NOT_COVERED_CODA}"


def format() -> str:  # noqa: A001
    status = read_cyclone_status()
    storms = read_cyclone_storms()
    track = read_cyclone_track()
    if status.empty and storms.empty and track.empty:
        return "STORMS: No data"

    now = _now()
    layer_states, layer_last_success = _layer_dicts(read_freshness())
    places = active_places(config.PLACES, now.date())
    results = assess_places(
        places, config.CYCLONE_BASINS, status, storms, track, layer_states, now,
        layer_last_success=layer_last_success,
    )
    current = current_storms(status, storms, layer_states, config.CYCLONE_BASINS)

    lines = [_header(status)]

    # 1. One line per flag, worst first, then per watch — each with its legs.
    hazards = sorted((p for p in results.values() if p.state in ("flag", "watch")), key=_place_rank)
    for p in hazards:
        lines.append("  " + (flag_sentence(p) if p.state == "flag" else watch_sentence(p)))
        legs = _legs_for(p.place_id)
        lines.append(f"    Legs: {', '.join(legs) if legs else 'none'}")

    # 2. The clear places, named — omitted when none is clear.
    clear = [p for p in results.values() if p.state == "clear"]
    if clear:
        lines.append(f"  No active storm threatens: {', '.join(p.short for p in clear)}")

    # 3. NOT ASSESSED — a statement about us, one line per failed or stale place.
    for p in results.values():
        if p.state in ("failed", "stale"):
            lines.append(f"  NOT ASSESSED: {p.short} — {p.reason}")

    # 4. Not covered — every day, while any place is.
    not_covered = [p for p in results.values() if p.state == "not_covered"]
    if not_covered:
        lines.append(_not_covered_line(not_covered))

    # 5. Current storms after de-duplication, then the nearest approach to a covered port.
    if current.empty:
        lines.append("  Active storms: none listed.")
    else:
        entries = ", ".join(_storm_entry(s) for _, s in current.iterrows())
        line = f"  Active storms: {entries}."
        nearest = _nearest_to_a_covered_port(current, track, hazards + clear, places)
        if nearest:
            line += f" Nearest to a covered port: {nearest}."
        lines.append(line)

    return "\n".join(lines)


__all__ = ["format"]
