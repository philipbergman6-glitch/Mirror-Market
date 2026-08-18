"""Archiving origin rankings, and telling a market move from a bookkeeping one.

Two jobs.

**Record what was published.** :func:`archive_ranking` writes one row per
origin per run, unrankable rows included. That last part is the useful part: a
history that only stored ranked origins would show Argentina vanishing for a
fortnight and give no way to tell "no offer" from "offered November while we
asked about September".

**Explain what moved.** A landed cost changes for two very different reasons —
the market repriced, or somebody re-entered a freight assumption — and they
look identical in a time series of one number. :func:`input_changes` separates
them by comparing the stored waterfalls component by component: a step whose
``assumption_id`` or ``value`` changed is a *bookkeeping* change; a change in
the origin price with every assumption identical is a *market* change. The
distinction is the whole reason the waterfall is stored rather than the total.

Nothing here re-computes a ranking. Reading history back through today's method
would be reporting what we would say now, dressed as what we said then; every
row therefore carries the ``method_version`` and ``assumption_set_id`` it was
produced under, and a comparison across a version change says so rather than
pretending the two are one series.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import date
from typing import Any

from analysis.origins.domain import LandedCost, OriginRanking, fingerprint

log = logging.getLogger(__name__)

# How much a component has to move before it is called out. One dollar a tonne
# on a ~450 USD/MT cargo: below that the line is rounding in a hand-entered
# number, above it somebody made a decision.
MATERIAL_MOVE_USD_MT = 1.0


def _row_payload(row: LandedCost, rank: int | None, ranking: OriginRanking) -> dict[str, Any]:
    steps = [step.to_dict() for step in row.steps]
    return {
        "run_date": ranking.as_of.isoformat(),
        "destination": ranking.destination.key,
        "window_start": ranking.requested_window.start.isoformat(),
        "window_end": ranking.requested_window.end.isoformat(),
        "origin": row.quote.origin.key,
        "rank": rank,
        "landed_usd_mt": row.landed_usd_mt,
        "fob_usd_mt": row.fob_equivalent.amount if row.fob_equivalent else None,
        "origin_usd_mt": row.quote.price.amount,
        "comparability": row.comparability.value,
        "confidence": row.confidence.value,
        "freshness": row.freshness.value,
        "observation_date": row.quote.observation_date.isoformat(),
        "shipment_window": (
            row.quote.shipment_window.describe() if row.quote.shipment_window else None
        ),
        "incoterm": f"{row.quote.incoterm.value} {row.quote.carrier.value}",
        "quote_kind": row.quote.quote_kind.value,
        "method_version": row.method_version,
        "assumption_set_id": ranking.assumption_set_id,
        # Fingerprints the row's own inputs, not its output: two runs with the
        # same digest and different landed costs would be an arithmetic bug,
        # and two runs with different digests and the same cost are a
        # coincidence worth being able to see.
        "input_digest": fingerprint(
            {
                "origin_price": row.quote.price.amount,
                "observation_date": row.quote.observation_date.isoformat(),
                "steps": [
                    {"c": step["component"], "a": step["assumption_id"], "v": step["amount_usd_mt"]}
                    for step in steps
                ],
            }
        ),
        "snapshot_json": json.dumps(
            {"steps": steps, "blockers": [b.to_dict() for b in row.blockers]}, default=str
        ),
    }


def ranking_rows(ranking: OriginRanking) -> list[dict[str, Any]]:
    """The storable form of one ranking — rankable rows first, ranked from 1."""
    ranks = {row.quote.origin.key: index for index, row in enumerate(ranking.rankable, start=1)}
    return [
        _row_payload(row, ranks.get(row.quote.origin.key), ranking)
        for row in ranking.rows
    ]


def archive_ranking(ranking: OriginRanking) -> int:
    """Persist one ranking. Returns rows written."""
    from pipeline.store import save_origin_ranking

    return save_origin_ranking(ranking_rows(ranking))


@dataclass(frozen=True)
class InputChange:
    """One component that moved between two archived runs."""

    origin: str
    component: str
    previous: float | None
    current: float | None
    delta: float | None
    previous_assumption_id: str | None
    current_assumption_id: str | None

    @property
    def kind(self) -> str:
        """``market`` when the number moved on its own; ``bookkeeping`` when the input did.

        A re-entered assumption is not news about soybeans, and a page that
        reports the two the same way trains its reader to ignore both.
        """
        if self.previous_assumption_id != self.current_assumption_id:
            return "bookkeeping"
        return "market"

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "component": self.component,
            "previous": self.previous,
            "current": self.current,
            "delta": self.delta,
            "kind": self.kind,
            "previous_assumption_id": self.previous_assumption_id,
            "current_assumption_id": self.current_assumption_id,
        }


def _steps_by_component(snapshot_json: Any) -> dict[str, dict]:
    if not snapshot_json:
        return {}
    payload = json.loads(snapshot_json) if isinstance(snapshot_json, str) else snapshot_json
    return {step["component"]: step for step in payload.get("steps", [])}


def input_changes(
    previous_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
    *,
    threshold: float = MATERIAL_MOVE_USD_MT,
) -> tuple[InputChange, ...]:
    """Component-level differences between two archived runs of the same lane.

    Rows are matched on origin. A component present in one run and absent from
    the other is reported with a ``None`` on the missing side — an ocean-freight
    line that disappeared because the assumption lapsed is precisely the change
    worth surfacing, and a diff that only compared shared components would drop
    it.
    """
    previous_by_origin = {row["origin"]: row for row in previous_rows}
    changes: list[InputChange] = []
    for row in current_rows:
        before = previous_by_origin.get(row["origin"])
        if before is None:
            continue
        before_steps = _steps_by_component(before.get("snapshot_json"))
        after_steps = _steps_by_component(row.get("snapshot_json"))
        for component in sorted(set(before_steps) | set(after_steps)):
            was = before_steps.get(component)
            now = after_steps.get(component)
            previous_value = was["amount_usd_mt"] if was else None
            current_value = now["amount_usd_mt"] if now else None
            if previous_value is not None and current_value is not None:
                delta = current_value - previous_value
                if abs(delta) < threshold:
                    continue
            else:
                delta = None
            changes.append(
                InputChange(
                    origin=row["origin"],
                    component=component,
                    previous=previous_value,
                    current=current_value,
                    delta=delta,
                    previous_assumption_id=was.get("assumption_id") if was else None,
                    current_assumption_id=now.get("assumption_id") if now else None,
                )
            )
    return tuple(changes)


@dataclass(frozen=True)
class RankChange:
    origin: str
    previous_rank: int | None
    current_rank: int | None

    @property
    def description(self) -> str:
        if self.previous_rank is None and self.current_rank is not None:
            return f"{self.origin} entered the ranking at {self.current_rank}"
        if self.current_rank is None and self.previous_rank is not None:
            return f"{self.origin} left the ranking (was {self.previous_rank})"
        return f"{self.origin} moved {self.previous_rank} → {self.current_rank}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "origin": self.origin,
            "previous_rank": self.previous_rank,
            "current_rank": self.current_rank,
            "description": self.description,
        }


def rank_changes(
    previous_rows: list[dict[str, Any]],
    current_rows: list[dict[str, Any]],
) -> tuple[RankChange, ...]:
    """Origins whose position changed, including entries and exits."""
    before = {row["origin"]: row.get("rank") for row in previous_rows}
    after = {row["origin"]: row.get("rank") for row in current_rows}
    return tuple(
        RankChange(origin=origin, previous_rank=before.get(origin), current_rank=after.get(origin))
        for origin in sorted(set(before) | set(after))
        if before.get(origin) != after.get(origin)
    )


def previous_run(rows_by_run: dict[date, list[dict[str, Any]]], before: date) -> date | None:
    earlier = [run for run in rows_by_run if run < before]
    return max(earlier) if earlier else None


__all__ = [
    "MATERIAL_MOVE_USD_MT",
    "InputChange",
    "RankChange",
    "archive_ranking",
    "input_changes",
    "previous_run",
    "rank_changes",
    "ranking_rows",
]
