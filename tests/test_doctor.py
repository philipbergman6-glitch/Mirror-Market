"""#354: `python main.py --doctor` says which keys are set and what each skips.

Invariant 1 names skipped-unconfigured as its own state, but until now
nothing surfaced it except a per-key warning scrolling past at the top of a
39-layer log. DATA_GOV_IN_API_KEY was absent for weeks without anyone
noticing. The doctor answers, without fetching anything:

* every env var any layer reads, set or missing — never its value;
* what a missing one does downstream: a `run_if`-gated layer is skipped
  with no freshness row, an un-gated one runs and grades as failed;
* a non-zero exit when a key the deploy workflow relies on is missing.

Everything here runs off `config.API_KEY_CATALOG`, and the tests below pin
that catalog against the things it describes: the production layer roster
and the `run_if` wiring in `main._build_dict_layers`. A key added to a
fetcher without a catalog entry, or a layer gated differently from what the
catalog claims, fails here rather than drifting.
"""

from __future__ import annotations

import io
import uuid

import pytest

import config
import main
from config import API_KEY_CATALOG, API_KEY_LAYERS, PRODUCTION_LAYER_KEYS, missing_api_keys
from pipeline import doctor

# Values that could only ever appear in output if the doctor leaked them.
_SENTINELS = {name: f"leak-{name.lower()}-{uuid.uuid4().hex}" for name in API_KEY_CATALOG}


def _set_all(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _SENTINELS.items():
        monkeypatch.setenv(name, value)


def _clear_all(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in API_KEY_CATALOG:
        monkeypatch.delenv(name, raising=False)


def _run(monkeypatch: pytest.MonkeyPatch) -> tuple[int, str]:
    stream = io.StringIO()
    code = doctor.run_doctor(stream=stream)
    return code, stream.getvalue()


# ── The catalog is the one source and it agrees with the code ───────────────


def test_the_legacy_layer_map_is_derived_from_the_catalog() -> None:
    """`missing_api_keys()` keeps working off the same catalog, never a copy."""
    assert set(API_KEY_LAYERS) == set(API_KEY_CATALOG)
    assert API_KEY_LAYERS["FAS_API_KEY"] == "Layer 10"
    # The USDA key gates crop progress (2b) too — the old string said "2, 14".
    assert API_KEY_LAYERS["USDA_API_KEY"] == "Layers 2, 2b, 14"


def test_every_layer_the_catalog_names_is_a_production_layer() -> None:
    for name, spec in API_KEY_CATALOG.items():
        for key in spec.skips + spec.degrades:
            assert key in PRODUCTION_LAYER_KEYS, f"{name} names unknown layer {key!r}"
        assert spec.skips or spec.degrades, f"{name} gates nothing"
        assert not set(spec.skips) & set(spec.degrades), f"{name} lists a layer as both"
        assert spec.signup.startswith("https://"), f"{name} has no signup URL"


def test_skipped_layers_are_run_if_gated_and_degraded_layers_are_not() -> None:
    """The catalog's skip/degrade split is read off main.py's own wiring.

    A `skips` layer must carry a `run_if` (it never fetches, no freshness
    row); a `degrades` layer that is a DictLayer must not (it runs, and its
    empty return is graded under the #175 rules). Wiring a layer the other
    way without updating the catalog fails here.
    """
    by_key = {layer.key: layer for layer in main._build_dict_layers()}
    for name, spec in API_KEY_CATALOG.items():
        for key in spec.skips:
            assert key in by_key, f"{name}: {key!r} is not a DictLayer, cannot be run_if-skipped"
            assert by_key[key].run_if is not None, f"{name}: {key!r} has no run_if but the catalog says it skips"
        for key in spec.degrades:
            if key in by_key:
                assert by_key[key].run_if is None, f"{name}: {key!r} is run_if-gated but the catalog says it degrades"


def test_the_deploy_workflow_passes_every_catalogued_key() -> None:
    """A key the catalog knows but the deploy job never exports is a key CI can never set."""
    workflow = (config.ENV_FILE.parent / ".github" / "workflows" / "deploy-dashboard.yml").read_text()
    for name in API_KEY_CATALOG:
        assert f"{name}: ${{{{ secrets.{name} }}}}" in workflow, name


# ── Output coverage ─────────────────────────────────────────────────────────


def test_output_covers_every_catalogued_key_and_the_layers_it_gates(monkeypatch) -> None:
    _clear_all(monkeypatch)
    _, out = _run(monkeypatch)
    for name, spec in API_KEY_CATALOG.items():
        assert name in out
        assert spec.unlocks in out
        for key in spec.skips + spec.degrades:
            assert f"Layer {config.LAYER_NUMBERS[key]}" in out, (name, key)


def test_a_missing_key_says_whether_its_layers_skip_or_degrade(monkeypatch) -> None:
    _clear_all(monkeypatch)
    reports = doctor.diagnose()
    by_name = {report.name: report for report in reports}
    assert set(by_name) == set(API_KEY_CATALOG)
    assert not by_name["FAS_API_KEY"].present
    assert by_name["FAS_API_KEY"].spec.skips == ("export_sales",)
    assert by_name["USDA_API_KEY"].spec.degrades == ("usda", "crop_progress", "crush_inspections")

    _, out = _run(monkeypatch)
    assert "skipped" in out
    assert "failed" in out


def test_a_set_key_reads_as_set_and_a_missing_one_as_missing(monkeypatch) -> None:
    _clear_all(monkeypatch)
    monkeypatch.setenv("FRED_API_KEY", _SENTINELS["FRED_API_KEY"])
    by_name = {report.name: report for report in doctor.diagnose()}
    assert by_name["FRED_API_KEY"].present
    assert not by_name["USDA_API_KEY"].present
    assert set(missing_api_keys()) == set(API_KEY_CATALOG) - {"FRED_API_KEY"}


def test_an_empty_key_is_missing(monkeypatch) -> None:
    _clear_all(monkeypatch)
    monkeypatch.setenv("EIA_API_KEY", "")
    by_name = {report.name: report for report in doctor.diagnose()}
    assert not by_name["EIA_API_KEY"].present


# ── Exit code ───────────────────────────────────────────────────────────────


def test_all_keys_set_exits_zero(monkeypatch) -> None:
    _set_all(monkeypatch)
    code, out = _run(monkeypatch)
    assert code == 0
    assert "MISSING" not in out


def test_a_missing_ci_required_key_exits_non_zero(monkeypatch) -> None:
    _set_all(monkeypatch)
    monkeypatch.delenv("FAS_API_KEY")
    assert API_KEY_CATALOG["FAS_API_KEY"].required_in_ci
    code, out = _run(monkeypatch)
    assert code == 1
    assert "FAS_API_KEY" in out and "MISSING" in out


def test_a_missing_optional_key_exits_zero_but_is_still_reported(monkeypatch) -> None:
    """MARS gates a degraded-never-broken layer (LAYERS.md): visible, not fatal."""
    _set_all(monkeypatch)
    monkeypatch.delenv("MARS_API_KEY")
    assert not API_KEY_CATALOG["MARS_API_KEY"].required_in_ci
    code, out = _run(monkeypatch)
    assert code == 0
    assert "MARS_API_KEY" in out and "MISSING" in out


def test_the_cli_flag_runs_the_doctor_without_the_pipeline(monkeypatch, capsys) -> None:
    _set_all(monkeypatch)
    monkeypatch.delenv("USDA_API_KEY")

    def _never(*args, **kwargs):
        raise AssertionError("--doctor must not start the pipeline")

    monkeypatch.setattr(main, "run", _never)
    assert main.main(["--doctor"]) == 1
    assert "USDA_API_KEY" in capsys.readouterr().out


# ── No value ever appears ───────────────────────────────────────────────────


def test_no_key_value_appears_in_output_or_in_the_report(monkeypatch) -> None:
    _set_all(monkeypatch)
    _, out = _run(monkeypatch)
    for value in _SENTINELS.values():
        assert value not in out
    for report in doctor.diagnose():
        assert not any(value in repr(report) for value in _SENTINELS.values())
