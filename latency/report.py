"""Rendering for latency measurements — text for an operator, JSON for a machine.

The JSON shape is the one published as ``docs/manifest.json`` and the one
the fast-refresh promotion gate compares editions with, so it is a contract
rather than a debug dump: keys here are read by
``trust.site_promotion.verify_refresh_is_not_a_regression``.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from latency.domain import (
    OBJECTIVES,
    Granularity,
    LatencyClass,
    LayerMeasurement,
    Verdict,
    age_at,
    format_delta,
)


def _seconds(value: timedelta | None) -> float | None:
    return None if value is None else round(value.total_seconds(), 1)


def measurement_to_dict(measurement: LayerMeasurement, now: datetime) -> dict[str, Any]:
    stamps = measurement.stamps
    return {
        "layer": measurement.layer,
        "class": measurement.spec.latency_class.value,
        "status": measurement.status,
        "granularity": measurement.granularity.value,
        "observed_at": stamps.observed_at.isoformat() if stamps.observed_at else None,
        "fetch_started_at": (
            stamps.fetch_started_at.isoformat() if stamps.fetch_started_at else None
        ),
        "fetch_completed_at": (
            stamps.fetch_completed_at.isoformat() if stamps.fetch_completed_at else None
        ),
        "stored_at": stamps.stored_at.isoformat() if stamps.stored_at else None,
        "generated_at": stamps.generated_at.isoformat() if stamps.generated_at else None,
        "published_at": stamps.published_at.isoformat() if stamps.published_at else None,
        "acquisition_s": _seconds(measurement.acquisition),
        "fetch_duration_s": _seconds(measurement.fetch_duration),
        "processing_s": _seconds(measurement.processing),
        "publication_s": _seconds(measurement.publication),
        "pipeline_s": _seconds(measurement.pipeline),
        "end_to_end_s": _seconds(measurement.end_to_end),
        # The declared provider floor and the remainder we are accountable
        # for. Rendered as a pair so a reader can never see the derived
        # number without the claim it was derived from.
        "provider_delay_s": _seconds(measurement.spec.provider_delay),
        "provider_delay_basis": measurement.spec.provider_delay_basis,
        "cadence_wait_s": _seconds(measurement.cadence_wait),
        "age_s": _seconds(age_at(measurement, now)),
        "acquisition_verdict": measurement.acquisition_verdict.value,
        "pipeline_verdict": measurement.pipeline_verdict.value,
        "verdict": measurement.verdict.value,
    }


def objectives_to_dict() -> dict[str, Any]:
    return {
        cls.value: {
            "acquisition_target_s": objective.acquisition_target.total_seconds(),
            "pipeline_target_s": objective.pipeline_target.total_seconds(),
            "end_to_end_target_s": objective.end_to_end_target.total_seconds(),
            "basis": objective.basis,
        }
        for cls, objective in OBJECTIVES.items()
    }


def to_dict(measurements: list[LayerMeasurement], now: datetime) -> dict[str, Any]:
    return {
        "measured_at": now.isoformat(),
        "objectives": objectives_to_dict(),
        "layers": [measurement_to_dict(m, now) for m in measurements],
    }


_VERDICT_MARK = {
    Verdict.MEETS: "ok  ",
    Verdict.BREACHES: "MISS",
    Verdict.UNKNOWN: "?   ",
}


def to_text(measurements: list[LayerMeasurement], now: datetime) -> str:
    """An operator-readable table. One row per layer, grouped by class."""
    lines: list[str] = []
    lines.append("Mirror Market — measured latency")
    lines.append(f"measured at {now.isoformat()}")
    lines.append("")
    lines.append(
        "acquisition = observation -> fetched (provider delay + our cadence wait)"
    )
    lines.append("pipeline    = fetched -> publicly readable (entirely ours)")
    lines.append("age         = how stale the newest observation is right now")
    lines.append("")

    by_class: dict[LatencyClass, list[LayerMeasurement]] = {}
    for m in measurements:
        by_class.setdefault(m.spec.latency_class, []).append(m)

    header = (
        f"{'layer':<18}{'observed':<21}{'acquis.':>10}{'cadence':>10}"
        f"{'pipeline':>10}{'age':>10}  verdict"
    )
    for cls in LatencyClass:
        rows = by_class.get(cls)
        if not rows:
            continue
        objective = OBJECTIVES[cls]
        lines.append(
            f"[{cls.value}]  target: acquisition <= {format_delta(objective.acquisition_target)}, "
            f"pipeline <= {format_delta(objective.pipeline_target)}"
        )
        lines.append(header)
        for m in rows:
            observed = m.stamps.observed_at
            stamp = observed.strftime("%Y-%m-%d %H:%MZ") if observed else "—"
            if m.granularity is Granularity.DAY and observed:
                stamp = observed.strftime("%Y-%m-%d (day)")
            lines.append(
                f"{m.layer:<18}{stamp:<21}"
                f"{format_delta(m.acquisition):>10}"
                f"{format_delta(m.cadence_wait):>10}"
                f"{format_delta(m.pipeline):>10}"
                f"{format_delta(age_at(m, now)):>10}"
                f"  {_VERDICT_MARK[m.verdict]}"
            )
        lines.append("")

    breaches = [m for m in measurements if m.verdict is Verdict.BREACHES]
    unknown = [m for m in measurements if m.verdict is Verdict.UNKNOWN]
    lines.append(
        f"{len(measurements) - len(breaches) - len(unknown)} meeting objective, "
        f"{len(breaches)} breaching, {len(unknown)} unmeasured"
    )
    if breaches:
        lines.append("breaching: " + ", ".join(m.layer for m in breaches))
    if unknown:
        lines.append("unmeasured: " + ", ".join(m.layer for m in unknown))
    return "\n".join(lines)
