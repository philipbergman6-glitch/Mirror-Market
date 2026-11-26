"""Owner ruling 2026-11-26 on #663 (#477 Q1): economic thresholds are per product.

Stratum 4 (MAGyP official FOB) renders bean, meal, soybean-oil and
sunflower-oil legs, so its threshold is ``{"default": 0.5, "oil": 1.5}``, like
strata 2 and 6. A leg is classified into exactly one product from its label;
a label that names no product, or more than one, is refused, never defaulted.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from verify import tolerances
from verify.grade import Impact, grade_exact

# config.MARKETS["argentina"]["price"]["keys"], as block 01 labels the legs.
MAGYP_LEGS = {"Soybeans": "bean", "Soybean Meal": "meal", "Soybean Oil": "oil", "Sunflower Oil": "oil"}
DCE_LEGS = {
    "DCE Soybean No.2 · headline": "bean",
    "DCE Soybean No.1": "bean",
    "DCE Soybean Meal": "meal",
    "DCE Soybean Oil": "oil",
}


def _magyp_grade(leg_label: str, delta_usd_mt: float):
    tol = tolerances.load()
    block = tol.stratum("4")
    threshold = tol.threshold_usd_mt(block, tol.product_of(leg_label))
    # MAGyP is natively USD/MT in whole dollars: the delta is its own USD/MT effect.
    impact = Impact(leg=leg_label, delta_usd_mt=delta_usd_mt, threshold_usd_mt=threshold)
    return grade_exact(delta_usd_mt, tol.native_allowance(block, 0), [impact])


def test_magyp_stratum_threshold_is_per_product():
    assert tolerances.load().stratum("4")["threshold_usd_mt"] == {"default": 0.5, "oil": 1.5}


@pytest.mark.parametrize("leg", ["Soybean Oil", "Sunflower Oil"])
def test_a_magyp_oil_leg_off_by_one_dollar_is_within_the_oil_threshold(leg):
    grade = _magyp_grade(leg, 1.0)
    assert grade.result == "discrepancy"  # whole dollars: any non-zero delta is outside native
    assert grade.material is False
    assert grade.binding.threshold_usd_mt == 1.5


@pytest.mark.parametrize("leg", ["Soybeans", "Soybean Meal"])
def test_a_magyp_bean_or_meal_leg_off_by_one_dollar_is_material(leg):
    grade = _magyp_grade(leg, 1.0)
    assert grade.result == "discrepancy"
    assert grade.material is True
    assert grade.binding.threshold_usd_mt == 0.5


@pytest.mark.parametrize(("label", "product"), [*MAGYP_LEGS.items(), *DCE_LEGS.items()])
def test_every_located_leg_label_names_exactly_one_product(label, product):
    assert tolerances.load().product_of(label) == product


@pytest.mark.parametrize("label", ["", "Sunflower Seed", "EU Rapeseed (Moselle)", "Corn", "Soybean Oil/Meal"])
def test_a_leg_label_naming_no_product_or_two_is_refused(label):
    with pytest.raises(tolerances.UnknownProductError):
        tolerances.load().product_of(label)


def test_an_unknown_product_is_refused_not_defaulted():
    tol = tolerances.load()
    with pytest.raises(tolerances.UnknownProductError, match="corn"):
        tol.threshold_usd_mt(tol.stratum("4"), "corn")


def test_a_product_with_no_entry_and_no_default_is_refused():
    tol = tolerances.load()
    # AMS 3511 prices cash oil and meal only: a bean leg has no threshold there.
    assert tol.threshold_usd_mt(tol.stratum("9"), "meal") == 0.5
    with pytest.raises(tolerances.ToleranceFileError, match="bean"):
        tol.threshold_usd_mt(tol.stratum("9"), "bean")


def test_cbot_thresholds_are_keyed_by_product():
    tol = tolerances.load()
    cbot = tol.stratum("1")
    assert [tol.threshold_usd_mt(cbot, p) for p in ("bean", "meal", "oil")] == [0.5, 0.5, 1.5]


def _with(tmp_path: Path, edit) -> Path:
    data = json.loads(tolerances.DEFAULT_PATH.read_text(encoding="utf-8"))
    edit(data)
    path = tmp_path / "tol.json"
    path.write_text(json.dumps(data), encoding="utf-8")
    return path


def test_a_threshold_keyed_by_an_unknown_product_is_refused_at_load(tmp_path):
    path = _with(tmp_path, lambda d: d["strata"]["4"].__setitem__("threshold_usd_mt", {"default": 0.5, "olive": 1.5}))
    with pytest.raises(tolerances.ToleranceFileError, match="olive"):
        tolerances.load(path)


def test_a_non_numeric_threshold_is_refused_at_load(tmp_path):
    path = _with(tmp_path, lambda d: d["strata"]["4"].__setitem__("threshold_usd_mt", {"default": "0.5"}))
    with pytest.raises(tolerances.ToleranceFileError, match="stratum 4"):
        tolerances.load(path)


def test_a_file_without_the_product_table_is_refused(tmp_path):
    path = _with(tmp_path, lambda d: d.pop("leg_products"))
    with pytest.raises(tolerances.ToleranceFileError, match="leg_products"):
        tolerances.load(path)
