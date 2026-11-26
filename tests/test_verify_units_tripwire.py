"""AL1 #640: the unit tripwire — the one test that may import both sides.

Invariant 7 (as amended, #479): the verifier holds its own constants, derived
independently and compared by test. This is that test. It pins

* each verifier constant to the figure its cited source prints, and
* each verifier constant to the product's — ``pipeline/units.py``, and for the
  60 kg sack ``fetchers/cepea.py`` — within the residue the product's rounded
  constants allow: 2 × 10⁻⁶ relative (36.7437 vs 36.74371 is 2.8 × 10⁻⁷;
  2,204.62 vs 2,204.62262 is 1.2 × 10⁻⁶).

A wrong constant on either side, beyond that residue, fails here.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from fetchers import cepea
from pipeline import units as product
from verify import units as verifier

RESIDUE = 2e-6


def relative_gap(a: float, b: float) -> float:
    return abs(a - b) / abs(b)


def _pairs(v=verifier, p=product, sack_kg: float = cepea._SACK_KG) -> dict[str, tuple[float, float]]:
    """verifier constant vs the product's, each as the same physical quantity."""
    return {
        "bu/MT soybeans": (v.BU_PER_MT_SOYBEANS, p.CONVERSION_FACTORS["Soybeans"] * 100),
        "bu/MT wheat (60 lb bushel)": (v.BU_PER_MT_SOYBEANS, p.CONVERSION_FACTORS["Wheat"] * 100),
        "lb/MT (commodity table)": (v.LB_PER_MT, p.CONVERSION_FACTORS["Soybean Oil"] * 100),
        "lb/MT (unit-named)": (v.LB_PER_MT, p.POUNDS_PER_MT),
        "MT/st (commodity table)": (v.MT_PER_SHORT_TON, 1 / p.CONVERSION_FACTORS["Soybean Meal"]),
        "MT/st (unit-named)": (v.MT_PER_SHORT_TON, p.MT_PER_SHORT_TON),
        "kg/MT": (v.KG_PER_MT, p.KG_PER_METRIC_TON),
        "sc/MT": (v.SACKS_PER_MT, p.KG_PER_METRIC_TON / sack_kg),
    }


def _breaches(pairs: dict[str, tuple[float, float]]) -> list[str]:
    return [f"{name}: {mine} vs {theirs}" for name, (mine, theirs) in pairs.items()
            if relative_gap(mine, theirs) > RESIDUE]


def test_the_verifier_and_the_product_agree_within_the_rounding_residue():
    assert not _breaches(_pairs())


@pytest.mark.parametrize("name, printed", [(name, figure) for name, (figure, _src) in sorted(verifier.PUBLISHED.items())])
def test_each_verifier_constant_rounds_to_its_cited_figure(name, printed):
    places = -Decimal(printed).as_tuple().exponent
    assert round(getattr(verifier, name), places) == float(printed), (
        f"verify.units.{name} = {getattr(verifier, name)} does not round to its cited {printed}"
    )


def test_the_exact_definitions():
    # NIST HB44 App. C, USDA ERS AH-697, CEPEA's saca: the inputs every
    # derived constant rests on.
    assert verifier.KG_PER_LB == 0.45359237
    assert verifier.LB_PER_SHORT_TON == 2000.0
    assert verifier.LB_PER_BUSHEL_SOYBEANS == 60.0
    assert verifier.KG_PER_SACK == 60.0
    assert pytest.approx(0.90718474, rel=1e-15) == verifier.MT_PER_SHORT_TON


class _Shifted:
    """A module stand-in with one constant moved."""

    def __init__(self, base, **changes):
        self._base, self._changes = base, changes

    def __getattr__(self, name):
        return self._changes.get(name, getattr(self._base, name))


@pytest.mark.parametrize("side, name, value", [
    ("verifier", "BU_PER_MT_SOYBEANS", 36.7438),        # 2.4e-6: past the residue
    ("verifier", "LB_PER_MT", 2204.6),                  # 1.0e-5
    ("verifier", "MT_PER_SHORT_TON", 0.9072),           # 1.7e-5
    ("verifier", "SACKS_PER_MT", 16.67),                # 2.0e-4
    ("product", "POUNDS_PER_MT", 2204.63),              # 3.4e-6
    ("product", "MT_PER_SHORT_TON", 0.90719),           # 5.8e-6
    ("product", "CONVERSION_FACTORS", {**product.CONVERSION_FACTORS, "Soybeans": 36.75 / 100}),
])
def test_a_perturbed_constant_on_either_side_trips_the_wire(side, name, value):
    if side == "verifier":
        pairs = _pairs(v=_Shifted(verifier, **{name: value}))
    else:
        pairs = _pairs(p=_Shifted(product, **{name: value}))
    assert _breaches(pairs), f"moving {side} {name} to {value} went unnoticed"


def test_a_perturbed_sack_weight_trips_the_wire():
    assert _breaches(_pairs(sack_kg=60.5))
