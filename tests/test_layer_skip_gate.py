"""F3c (issue #180): "skipped" must mean unconfigured, never "upstream died".

`_run_dict_layer` used to infer the skip from the *result*: a layer carrying
a `skip_msg` that returned falsy data was logged as skipped and returned
before `_finalize_layer`, writing no freshness row at all. That reads the
same for two opposite situations —

    no API key set        → the layer genuinely never ran   (correct)
    key set, upstream {}  → a FAS/EIA outage                (wrong)

— because `fetch_all_export_sales` / `fetch_all_eia` return a bare `{}` for
both. An outage was therefore invisible: no `failed` status, nothing in
`_HARD_FAILURES`, nothing for scripts/ci_layer_alert.py or the dashboard.

The fix moves the decision *ahead* of the fetch: `DictLayer.skip_gate` is a
predicate consulted before `fetch()` is called, so the two cases can never
be conflated by the shape of the return value. With no key we skip without
fetching; with a key we always reach `_finalize_layer`, where the existing
#175 rules apply (both layers carry a LAYER_MIN_KEYS floor of 2+, so
all-empty derives to a failure).

No network, no DB — `save_freshness` is captured and the fetchers are stubbed.
"""

from __future__ import annotations

import pandas as pd
import pytest

import main
from config import LAYER_MIN_KEYS

# The two API-key-gated dict layers. Third element is the fetcher module
# whose module-level key constant the layer's skip_gate reads.
GATED_LAYERS = [
    ("export_sales", "fetchers.export_sales", "FAS_API_KEY"),
    ("eia", "fetchers.eia", "EIA_API_KEY"),
]


@pytest.fixture
def freshness_calls(monkeypatch):
    """Capture every save_freshness call main.py makes, without touching a DB."""
    calls: list[dict] = []

    def _capture(layer_name, rows_fetched=0, status="success"):
        calls.append({"layer": layer_name, "rows": rows_fetched, "status": status})

    monkeypatch.setattr(main, "save_freshness", _capture)
    main._HARD_FAILURES.clear()
    yield calls
    main._HARD_FAILURES.clear()


def _layer(key: str) -> main.DictLayer:
    return {layer.key: layer for layer in main._build_dict_layers()}[key]


def _stub(monkeypatch, layer: main.DictLayer, *, key_set: bool, data: dict):
    """Point the layer at a canned fetch result and a canned key state.

    The DictLayer is frozen, so we patch the module the skip_gate reads and
    swap the fetch/clean/save callables via dataclasses.replace.
    """
    import dataclasses
    import importlib

    module_name, const = {k: (m, c) for k, m, c in GATED_LAYERS}[layer.key]
    monkeypatch.setattr(
        importlib.import_module(module_name), const, "test-key" if key_set else ""
    )
    return dataclasses.replace(
        layer,
        fetch=lambda: dict(data),
        clean=None,
        save=lambda n, d: None,
    )


@pytest.mark.parametrize("key,_module,_const", GATED_LAYERS)
def test_unconfigured_layer_writes_no_freshness_row(key, _module, _const, monkeypatch, freshness_calls):
    """The behaviour worth preserving: no key means the layer never ran.

    A freshness row here would be a lie in either direction — 'success'
    fabricates data that was never fetched, 'failed' pages CI about a
    layer the operator deliberately left unconfigured.
    """
    fetched = []
    layer = _stub(monkeypatch, _layer(key), key_set=False, data={})
    import dataclasses

    layer = dataclasses.replace(layer, fetch=lambda: fetched.append(1) or {})

    assert main._run_dict_layer(layer) is False

    assert freshness_calls == []
    assert key not in main._HARD_FAILURES
    assert fetched == [], "an unconfigured layer must not call its fetcher at all"


@pytest.mark.parametrize("key,_module,_const", GATED_LAYERS)
def test_configured_layer_returning_nothing_is_an_outage(key, _module, _const, monkeypatch, freshness_calls):
    """F3c: the key is set and the upstream answered with nothing.

    Both layers have a LAYER_MIN_KEYS floor of 2+, so #175's derivation
    grades all-empty as a failure — provided we get there at all, which is
    exactly what the old result-shaped skip prevented.
    """
    assert LAYER_MIN_KEYS[key] >= 2  # the derivation this test leans on

    layer = _stub(monkeypatch, _layer(key), key_set=True, data={})

    assert main._run_dict_layer(layer) is False

    assert [c["status"] for c in freshness_calls] == ["failed"]
    assert freshness_calls[0]["layer"] == key
    assert key in main._HARD_FAILURES, "a quiet outage must page CI like a loud one"


@pytest.mark.parametrize("key,_module,_const", GATED_LAYERS)
def test_configured_layer_with_empty_frames_is_also_an_outage(key, _module, _const, monkeypatch, freshness_calls):
    """The other empty shape: keys present, every frame empty.

    `fetch_all_*` builds one entry per configured commodity/series and only
    returns a bare {} on the key check, so a live outage actually looks
    like this. `not data` was never true here, so this path already reached
    _finalize_layer — pinned so the skip_gate rewrite doesn't regress it.
    """
    payload = {"a": pd.DataFrame(), "b": pd.DataFrame()}
    layer = _stub(monkeypatch, _layer(key), key_set=True, data=payload)

    assert main._run_dict_layer(layer) is False

    assert [c["status"] for c in freshness_calls] == ["failed"]
    assert key in main._HARD_FAILURES


def test_skip_gate_and_skip_msg_are_wired_together():
    """Pin the wiring: a message without a gate is the old bug restored.

    `skip_msg` alone can no longer suppress anything, so a layer carrying
    one but no `skip_gate` would silently grade its outages correctly and
    log nothing useful — and a gate with no message logs an empty line.
    """
    gated = {
        layer.key
        for layer in main._build_dict_layers()
        if layer.skip_gate is not None
    }
    messaged = {
        layer.key
        for layer in main._build_dict_layers()
        if layer.skip_msg is not None
    }

    assert gated == messaged == {"export_sales", "eia"}


@pytest.mark.parametrize("key,module_name,const", GATED_LAYERS)
def test_skip_gate_reads_the_fetchers_own_key(key, module_name, const, monkeypatch):
    """One source of truth for "is this layer configured?".

    The gate must agree with the fetcher's own check by construction. If
    the pipeline read config.<KEY> while the fetcher read its own imported
    copy, the two could disagree and the layer would be graded on a fetch
    it declined to make.
    """
    import importlib

    module = importlib.import_module(module_name)
    gate = _layer(key).skip_gate

    monkeypatch.setattr(module, const, "")
    assert gate() is True

    monkeypatch.setattr(module, const, "test-key")
    assert gate() is False
