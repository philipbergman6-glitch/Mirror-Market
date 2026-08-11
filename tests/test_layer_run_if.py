"""F3c (issue #180): "skipped" must mean unconfigured, never "upstream died".

`_run_dict_layer` used to infer the skip from the *result*: a layer carrying
a `skip_msg` that returned falsy data was logged as skipped and returned
before `_finalize_layer`, writing no freshness row at all. But an empty
return means two opposite things —

    no API key set        → the layer genuinely never ran   (correct)
    key set, upstream {}  → a FAS/EIA outage                (wrong)

— and a layer that records nothing at all is invisible: no `failed` status,
nothing in `_HARD_FAILURES`, nothing for scripts/ci_layer_alert.py or the
dashboard to read.

Scope, honestly stated: the second case was a *latent* hazard, not an
observed outage. `fetch_all_export_sales` and `fetch_all_eia` assign
`results[name] = df` unconditionally, so with a key set they return a dict
of empty frames rather than a bare `{}` — `not data` was already False and
the old code already reached `_finalize_layer`. Only the key check ever
produced the bare `{}`. What this change buys is that the two cases are now
distinguished by construction rather than by an incidental property of two
fetchers' loop bodies, which nothing was pinning.

The mechanism: `DictLayer.run_if` is a predicate consulted *before* fetch()
is called, so the decision is read off the config rather than the result.
With no key we skip without fetching; with a key we always reach
`_finalize_layer`, where the existing #175 rules apply (both layers carry a
LAYER_MIN_KEYS floor of 2+, so all-empty derives to a failure).

No network, no DB — the shared `freshness_calls` fixture captures
save_freshness and the fetchers are stubbed.
"""

from __future__ import annotations

import dataclasses
import importlib

import pandas as pd
import pytest

import main
from config import LAYER_MIN_KEYS

# The two API-key-gated dict layers → the fetcher module and key constant
# each layer's run_if predicate reads.
GATED_LAYERS = {
    "export_sales": ("fetchers.export_sales", "FAS_API_KEY"),
    "eia": ("fetchers.eia", "EIA_API_KEY"),
}


def _layer(key: str) -> main.DictLayer:
    return {layer.key: layer for layer in main._build_dict_layers()}[key]


def _set_key(monkeypatch, key: str, *, present: bool) -> None:
    """Set or clear the key constant the layer's run_if predicate reads."""
    module_name, const = GATED_LAYERS[key]
    monkeypatch.setattr(
        importlib.import_module(module_name), const, "test-key" if present else ""
    )


def _stubbed(layer: main.DictLayer, data: dict) -> tuple[main.DictLayer, list]:
    """The layer with a canned fetch result, plus a list recording each fetch.

    DictLayer is frozen, so the callables are swapped via dataclasses.replace.
    Cleaning and saving are stubbed out: what is under test is which grading
    path the layer reaches, not what it does with rows.
    """
    fetches: list[dict] = []

    def _fetch() -> dict:
        fetches.append(data)
        return dict(data)

    stubbed = dataclasses.replace(layer, fetch=_fetch, clean=None, save=lambda n, d: None)
    return stubbed, fetches


@pytest.mark.parametrize("key", sorted(GATED_LAYERS))
def test_unconfigured_layer_writes_no_freshness_row(key, monkeypatch, freshness_calls):
    """The behaviour worth preserving: no key means the layer never ran.

    A freshness row here would be a lie in either direction — 'success'
    fabricates data that was never fetched, 'failed' pages CI about a layer
    the operator deliberately left unconfigured.
    """
    _set_key(monkeypatch, key, present=False)
    layer, fetches = _stubbed(_layer(key), {})

    assert main._run_dict_layer(layer) is False

    assert freshness_calls == []
    assert key not in main._HARD_FAILURES
    assert fetches == [], "an unconfigured layer must not call its fetcher at all"


@pytest.mark.parametrize("key", sorted(GATED_LAYERS))
def test_configured_layer_returning_nothing_is_an_outage(key, monkeypatch, freshness_calls):
    """F3c: the key is set and the upstream answered with nothing at all.

    Both layers have a LAYER_MIN_KEYS floor of 2+, so #175's derivation
    grades all-empty as a failure — provided we get there at all, which is
    exactly what a result-shaped skip could prevent.
    """
    assert LAYER_MIN_KEYS[key] >= 2  # the derivation this test leans on

    _set_key(monkeypatch, key, present=True)
    layer, fetches = _stubbed(_layer(key), {})

    assert main._run_dict_layer(layer) is False

    assert [c["status"] for c in freshness_calls] == ["failed"]
    assert freshness_calls[0]["layer"] == key
    assert key in main._HARD_FAILURES, "a quiet outage must page CI like a loud one"
    assert len(fetches) == 1, "a configured layer must actually attempt the fetch"


@pytest.mark.parametrize("key", sorted(GATED_LAYERS))
def test_configured_layer_with_empty_frames_is_also_an_outage(key, monkeypatch, freshness_calls):
    """The empty shape a live outage actually produces.

    `fetch_all_*` builds one entry per configured commodity/series and only
    returns a bare {} on the key check, so keys-present-frames-empty is what
    a dead upstream looks like. `not data` was never true here, so this path
    already reached _finalize_layer — pinned so the rewrite doesn't regress
    the case that carries the real-world traffic.
    """
    _set_key(monkeypatch, key, present=True)
    layer, _ = _stubbed(_layer(key), {"a": pd.DataFrame(), "b": pd.DataFrame()})

    assert main._run_dict_layer(layer) is False

    assert [c["status"] for c in freshness_calls] == ["failed"]
    assert key in main._HARD_FAILURES


def test_run_if_and_skip_msg_are_wired_together():
    """Pin the co-occurrence: the two fields are one decision in two parts.

    A `run_if` with no message logs an empty line; a `skip_msg` with no
    predicate can no longer suppress anything, so it would be dead prose
    sitting next to a layer whose skip is silently unreachable.
    """
    layers = main._build_dict_layers()
    gated = {layer.key for layer in layers if layer.run_if is not None}
    messaged = {layer.key for layer in layers if layer.skip_msg is not None}

    assert gated == messaged == set(GATED_LAYERS)


@pytest.mark.parametrize("key", sorted(GATED_LAYERS))
def test_run_if_reads_the_fetchers_own_key(key, monkeypatch):
    """One source of truth for "is this layer configured?".

    The predicate must agree with the fetcher's own check by construction.
    If the pipeline read config.<KEY> while the fetcher read its own
    imported copy, the two could disagree and the layer would be graded on
    a fetch it declined to make.
    """
    module_name, const = GATED_LAYERS[key]
    module = importlib.import_module(module_name)
    run_if = _layer(key).run_if

    monkeypatch.setattr(module, const, "")
    assert run_if() is False

    monkeypatch.setattr(module, const, "test-key")
    assert run_if() is True
