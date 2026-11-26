"""The grading rule shared by every exact check (spec #639, "Materiality and grading").

A delta inside the native tolerance is a pass. Outside it, it is a
discrepancy, and it is material when it also exceeds the economic threshold on
the leg it moves. Material and explained are separate axes: this module decides
only the first. A fresh discrepancy is unexplained until a person explains it.
"""

from __future__ import annotations

from dataclasses import dataclass

# Float slack only: a delta exactly at half a display unit is inside, and
# binary representation must not push it out.
_EPSILON = 1e-12


@dataclass(frozen=True)
class Impact:
    """The USD/MT effect of a delta on one leg, against that leg's threshold."""

    leg: str
    delta_usd_mt: float
    threshold_usd_mt: float

    @property
    def exceeds(self) -> bool:
        return abs(self.delta_usd_mt) > self.threshold_usd_mt


@dataclass(frozen=True)
class Grade:
    result: str            # "pass" | "discrepancy"
    material: bool
    binding: Impact        # the leg the delta moves furthest relative to its threshold


def within(delta_native: float, tolerance_native: float) -> bool:
    return abs(delta_native) <= tolerance_native + _EPSILON


def binding_impact(impacts: list[Impact]) -> Impact:
    if not impacts:
        raise ValueError("an exact grade needs at least one leg the delta moves")
    return max(impacts, key=lambda impact: abs(impact.delta_usd_mt) / impact.threshold_usd_mt)


def grade_exact(delta_native: float, tolerance_native: float, impacts: list[Impact]) -> Grade:
    binding = binding_impact(impacts)
    if within(delta_native, tolerance_native):
        return Grade(result="pass", material=False, binding=binding)
    return Grade(result="discrepancy", material=any(impact.exceeds for impact in impacts), binding=binding)
