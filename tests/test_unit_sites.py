"""Invariant 7: unit conversion has two sites only (#665).

Three contracts:

1. **Byte-identical storage.** Every fetcher whose unit factor moved into
   ``pipeline/units.py`` still stores exactly what it stored before the move.
   ``tests/fixtures/unit_sites/golden.json`` was written by
   ``python -m tests._unit_sites_capture`` on the code *before* the move
   (commit history of #665), running each parser on its recorded fixture;
   floats are compared by ``repr``, so any last-bit drift fails.
2. **The units are the published ones**, from worked examples, not recomputed.
3. **No fetcher or pipeline module grows its own factor again** — a static scan.
"""

from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from pipeline import units
from tests._unit_sites_capture import CAPTURES, GOLDEN

ROOT = Path(__file__).resolve().parent.parent


# ── 1. stored values are identical before and after ─────────────────────────

@pytest.mark.parametrize("name", sorted(CAPTURES))
def test_stored_values_identical_to_the_pre_move_capture(name: str) -> None:
    golden = json.loads(GOLDEN.read_text(encoding="utf-8"))
    assert set(golden) == set(CAPTURES), "golden capture and CAPTURES roster drifted"
    # Round-trip through JSON so tuples/lists compare the way the golden stored them.
    now = json.loads(json.dumps(CAPTURES[name](), ensure_ascii=False))
    assert now == golden[name]


def _old_cepea(sack: float) -> float:
    return round(sack * 1000.0 / 60.0, 2)  # fetchers/cepea.py + noticias_agricolas.py before #665


def _old_agrural(bag: float) -> float:
    return round((bag / 60.0) * 1000.0, 2)  # fetchers/agrural.py before #665


def test_one_sack_conversion_reproduces_both_old_formulas_on_every_printed_price() -> None:
    """CEPEA and AgRural print R$/sc to the centavo. The two old formulas differ
    in the last bit on a third of inputs; after the 2-dp storage round they
    never do, so one function cannot move a stored AgRural row. Checked on
    every centavo price from R$0.01 to R$9,999.99 (the plausibility band is 60–300)."""
    for cents in range(1, 1_000_000):
        sack = cents / 100
        new = round(units.per_sack_to_per_metric_ton(sack), 2)
        assert new == _old_cepea(sack) == _old_agrural(sack), sack


# ── 2. the factors are the published units ──────────────────────────────────

def test_sack_is_sixty_kilograms() -> None:
    # CEPEA: "Reais por saca de 60 kg"; AgRural fixture row 129,50 R$/sc → 2158.33 R$/t.
    assert units.KG_PER_SACK == 60.0
    assert round(units.per_sack_to_per_metric_ton(129.50), 2) == 2158.33


def test_quintal_is_one_hundred_kilograms() -> None:
    # Agmarknet/MSAMB/NCDEX quote ₹/quintal; ₹7,250/qtl is ₹72,500/MT.
    assert units.KG_PER_QUINTAL == 100.0
    assert units.per_quintal_to_per_metric_ton(7250.0) == 72_500.0
    # 93,358 quintals of arrivals is 9,335.8 MT.
    assert units.quintals_to_metric_tons(93_358.0) == 9_335.8


def test_per_kg_price_to_per_metric_ton() -> None:
    # AFEX 681.25 NGN/kg is 681,250 NGN/MT; CONAB 2.07 R$/kg is 2,070 R$/MT.
    assert units.per_kg_to_per_metric_ton(681.25) == 681_250.0
    assert units.per_kg_to_per_metric_ton(2.07) == 2070.0


def test_tonnes_per_hectare_to_kg_per_hectare() -> None:
    # CONAB yield: production (mil t) / area (mil ha) is t/ha; 3.5 t/ha is 3,500 kg/ha.
    assert units.metric_tons_to_kg(3.5) == 3500.0


def test_conversions_accept_a_pandas_series() -> None:
    import pandas as pd

    out = units.per_quintal_to_per_metric_ton(pd.Series([7000.0, 7250.0]))
    assert list(out) == [70_000.0, 72_500.0]


def test_cepea_keeps_the_sack_alias_pr_663_pins() -> None:
    """PR #663's verifier tripwire reads ``fetchers.cepea._SACK_KG``. The alias
    is the units constant itself, so the tripwire still checks the product."""
    from fetchers import cepea

    assert cepea._SACK_KG is units.KG_PER_SACK


# ── 3. no factor outside the two sites ──────────────────────────────────────

# A module-level constant whose name says "unit factor". Bands (``_SACK_MIN_BRL``)
# and labels are not factors and do not match.
_FACTOR_NAME_PARTS = (
    "_KG", "KG_", "_TO_MT", "_PER_MT", "PER_SACK", "PER_QUINTAL", "QUINTALS_PER",
    "_PER_LB", "LBS_PER", "POUNDS_PER", "BU_PER", "BUSHELS_PER", "_PER_BU",
    "SHORT_TON", "PER_TON", "LAKH",
)


def _numeric_unit_constants(path: Path, root: Path = ROOT) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found = []
    for node in tree.body:
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        value = node.value
        if not (isinstance(value, ast.Constant) and isinstance(value.value, (int, float))
                and not isinstance(value.value, bool)):
            continue
        for target in targets:
            if isinstance(target, ast.Name) and any(p in target.id.upper() for p in _FACTOR_NAME_PARTS):
                found.append(f"{path.relative_to(root)}:{node.lineno} {target.id} = {value.value!r}")
    return found


def test_no_fetcher_or_pipeline_module_defines_its_own_unit_factor() -> None:
    offenders = [
        hit
        for folder in ("fetchers", "pipeline")
        for path in sorted((ROOT / folder).rglob("*.py"))
        if path != ROOT / "pipeline" / "units.py"
        for hit in _numeric_unit_constants(path)
    ]
    assert offenders == [], (
        "invariant 7: unit factors live in pipeline/units.py only — " + "; ".join(offenders)
    )


def test_the_scan_catches_the_constants_it_replaced(tmp_path: Path) -> None:
    sample = tmp_path / "sample.py"
    sample.write_text("_SACK_KG = 60.0\n_QUINTAL_TO_MT = 10.0\n_KG_PER_MT = 1000.0\n_SACK_MIN_BRL = 60.0\n")
    hits = [h.split(" ", 1)[1] for h in _numeric_unit_constants(sample, root=tmp_path)]
    assert hits == ["_SACK_KG = 60.0", "_QUINTAL_TO_MT = 10.0", "_KG_PER_MT = 1000.0"]
