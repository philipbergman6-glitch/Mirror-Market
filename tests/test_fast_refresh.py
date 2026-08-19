"""Tests for the price-only refresh path and the gate that makes it safe.

The gate is the interesting half. A fast refresh fetches three layers and
inherits the rest from whatever database it happens to be sitting on, so its
failure mode is not a crash — it is a beautifully rendered, structurally
valid site that knows less than the one it replaces. These tests pin that
the promotion refuses it, and that a refusal changes nothing about what is
already published.
"""

from __future__ import annotations

import json

import pytest

import main
from config import (
    FAST_REFRESH_HISTORY_PERIOD,
    FAST_REFRESH_LAYERS,
    PRODUCTION_LAYER_KEYS,
)
from trust.site_promotion import verify_refresh_is_not_a_regression

# ---------------------------------------------------------------------------
# What the fast path is
# ---------------------------------------------------------------------------


def test_fast_refresh_layers_are_real_layers():
    for layer in FAST_REFRESH_LAYERS:
        assert layer in PRODUCTION_LAYER_KEYS


def test_fast_refresh_covers_the_board_the_curve_and_fx():
    """The three things that move intraday and that everything else prices off.

    FX is in here because every `home_per_mt` leg on the site converts
    through it: a stale rate is a stale landed cost on every physical origin,
    not merely a stale FX cell.
    """
    assert set(FAST_REFRESH_LAYERS) == {"prices", "currencies", "forward_curve"}


def test_fast_refresh_excludes_the_scraped_physical_legs():
    """Doubling the request rate on an unfriendly upstream is not free."""
    for scraper in ("india_domestic", "cepea", "agrural", "gulf_bids", "magyp_fob"):
        assert scraper not in FAST_REFRESH_LAYERS


def test_fast_history_window_is_shorter_than_the_daily_one():
    from config import DEFAULT_HISTORY_PERIOD

    assert FAST_REFRESH_HISTORY_PERIOD != DEFAULT_HISTORY_PERIOD
    assert FAST_REFRESH_HISTORY_PERIOD.endswith(("d", "mo"))


def test_the_short_window_reaches_the_yfinance_layers():
    """The single change that makes the fast path cheap must actually be wired."""
    layers = {layer.key: layer for layer in main._build_dict_layers(history_period="5d")}
    captured: dict[str, str] = {}

    def fake_prices(period=None):
        captured["prices"] = period
        return {}

    def fake_currencies(period=None):
        captured["currencies"] = period
        return {}

    original = (main.fetch_prices, main.fetch_currencies)
    main.fetch_prices, main.fetch_currencies = fake_prices, fake_currencies
    try:
        layers["prices"].fetch()
        layers["currencies"].fetch()
    finally:
        main.fetch_prices, main.fetch_currencies = original
    assert captured == {"prices": "5d", "currencies": "5d"}


def test_unknown_layer_key_is_a_hard_failure(monkeypatch):
    """A typo must not silently run a shorter pipeline."""
    monkeypatch.setattr(main, "setup_logging", lambda: None)
    with pytest.raises(ValueError, match="unknown layer key"):
        main.run(layer_keys=("prices", "nonsuch"))


# ---------------------------------------------------------------------------
# The regression gate
# ---------------------------------------------------------------------------


def _manifest(coverage: dict[str, str | None], pages: dict[str, bool] | None = None,
              mode: str = "fast") -> dict:
    pages = pages if pages is not None else {"index.html": True, "origins.html": True}
    return {
        "schema_version": 1,
        "edition": {
            "mode": mode,
            "generated_at": "2026-08-19T22:40:00+00:00",
            "pages": [{"name": url, "url": url, "ok": ok} for url, ok in pages.items()],
        },
        "coverage": [
            {"layer": layer, "status": "success" if observed else "not-run",
             "observed_at": observed}
            for layer, observed in coverage.items()
        ],
        "latency": {"layers": []},
    }


_PUBLISHED = _manifest(
    {
        "prices": "2026-08-18T18:15:00+00:00",
        "psd": "2026-08-01T23:59:59+00:00",
        "weather": "2026-08-19T23:59:59+00:00",
    },
    mode="full",
)


def test_a_richer_candidate_is_promotable():
    candidate = _manifest({
        "prices": "2026-08-19T18:15:00+00:00",   # newer board
        "psd": "2026-08-01T23:59:59+00:00",      # unchanged
        "weather": "2026-08-19T23:59:59+00:00",
    })
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert verdict.promotable, verdict.failures


def test_an_identical_rebuild_is_promotable():
    """A template fix republishing the same data is a legitimate promotion."""
    verdict = verify_refresh_is_not_a_regression(_manifest(dict(
        prices="2026-08-18T18:15:00+00:00",
        psd="2026-08-01T23:59:59+00:00",
        weather="2026-08-19T23:59:59+00:00",
    )), _PUBLISHED)
    assert verdict.promotable, verdict.failures


def test_a_fast_refresh_on_an_unseeded_database_is_refused():
    """THE case this gate exists for.

    Fresh prices, and PSD and weather simply gone — because the runner had no
    restored database and the history CSVs do not carry them. Every page
    renders, every empty state names its reason, the structural contract
    passes. Only the comparison catches it.
    """
    candidate = _manifest({
        "prices": "2026-08-19T18:15:00+00:00",
        "psd": None,
        "weather": None,
    })
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert not verdict.promotable
    assert any("psd" in f for f in verdict.failures)
    assert any("weather" in f for f in verdict.failures)


def test_an_observation_going_backwards_is_refused():
    candidate = _manifest({
        "prices": "2026-08-11T18:15:00+00:00",
        "psd": "2026-08-01T23:59:59+00:00",
        "weather": "2026-08-19T23:59:59+00:00",
    })
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert not verdict.promotable
    assert any("backwards" in f for f in verdict.failures)


def test_a_layer_missing_entirely_from_coverage_is_refused():
    candidate = _manifest({"prices": "2026-08-19T18:15:00+00:00"})
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert not verdict.promotable
    assert any("absent here" in f for f in verdict.failures)


def test_a_tombstoned_page_is_refused_when_it_used_to_render():
    candidate = _manifest(
        {
            "prices": "2026-08-19T18:15:00+00:00",
            "psd": "2026-08-01T23:59:59+00:00",
            "weather": "2026-08-19T23:59:59+00:00",
        },
        pages={"index.html": True, "origins.html": False},
    )
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert not verdict.promotable
    assert any("origins.html" in f for f in verdict.failures)


def test_a_failed_run_that_kept_its_observation_is_not_a_regression():
    """last_success is preserved by design; the number is still there.

    A layer whose fetch failed but whose stored observation is unchanged has
    not cost the reader anything this build — the surfaces already report the
    failure. Refusing promotion here would mean one flaky upstream freezes
    the whole site.
    """
    candidate = _manifest({
        "prices": "2026-08-19T18:15:00+00:00",
        "psd": "2026-08-01T23:59:59+00:00",
        "weather": "2026-08-19T23:59:59+00:00",
    })
    candidate["coverage"][1]["status"] = "failed"
    verdict = verify_refresh_is_not_a_regression(candidate, _PUBLISHED)
    assert verdict.promotable, verdict.failures


# --- the failed-deploy family ---------------------------------------------


def test_a_missing_candidate_manifest_is_refused_not_waved_through():
    """Unable to make the comparison is unable to clear it."""
    assert not verify_refresh_is_not_a_regression(None, _PUBLISHED).promotable
    assert not verify_refresh_is_not_a_regression({}, _PUBLISHED).promotable


def test_an_unreadable_candidate_schema_is_refused():
    candidate = _manifest({"prices": "2026-08-19T18:15:00+00:00"})
    candidate["schema_version"] = 99
    assert not verify_refresh_is_not_a_regression(candidate, _PUBLISHED).promotable


def test_a_missing_public_manifest_passes_but_says_so():
    """The first build after this ships has nothing to compare against.

    Blocking there would wedge the deploy permanently, so it passes — and the
    absence is reported by the caller rather than being invisible.
    """
    candidate = _manifest({"prices": "2026-08-19T18:15:00+00:00"})
    verdict = verify_refresh_is_not_a_regression(candidate, None)
    assert verdict.promotable
    assert verdict.regressions == ()


def test_refresh_script_reports_a_missing_public_manifest_as_no_comparison(tmp_path, caplog):
    """A 404 must not be mistaken for a transport failure, or vice versa."""
    import logging

    from scripts.refresh_prices import _fetch_public_manifest

    with caplog.at_level(logging.ERROR, logger="refresh_prices"):
        assert _fetch_public_manifest(str(tmp_path / "nope.json")) is None
    assert "could not read the published manifest" in caplog.text


def test_refresh_script_exit_codes_are_distinct():
    """CI reads these; two failures sharing a code cannot be told apart."""
    from scripts.refresh_prices import (
        EXIT_CONTRACT_FAILED,
        EXIT_GENERATION_FAILED,
        EXIT_OK,
        EXIT_PIPELINE_FAILED,
        EXIT_REGRESSION,
    )

    codes = [
        EXIT_OK, EXIT_PIPELINE_FAILED, EXIT_GENERATION_FAILED,
        EXIT_CONTRACT_FAILED, EXIT_REGRESSION,
    ]
    assert len(set(codes)) == len(codes)
    assert EXIT_OK == 0
    assert all(code > 0 for code in codes[1:])


def test_a_failed_fast_pipeline_never_reaches_generation(monkeypatch, tmp_path):
    """The published edition must be untouched when the refresh dies early."""
    import scripts.refresh_prices as refresh_mod

    monkeypatch.setattr(refresh_mod, "setup_logging", lambda: None)
    monkeypatch.setattr(main, "run", lambda **_: 1)

    generated: list[object] = []

    def explode(**kwargs):
        generated.append(kwargs)
        raise AssertionError("generation must not run after a failed pipeline")

    monkeypatch.setattr("scripts.generate_site.generate_site", explode)

    code = refresh_mod.refresh(output_dir=tmp_path, public_manifest_source=None)
    assert code == refresh_mod.EXIT_PIPELINE_FAILED
    assert generated == []
    # Nothing was written where a candidate would go.
    assert not (tmp_path / "index.html").exists()


def test_a_tombstoned_candidate_stops_before_any_gate(monkeypatch, tmp_path):
    import scripts.refresh_prices as refresh_mod
    from scripts.generate_site import PageResult

    monkeypatch.setattr(refresh_mod, "setup_logging", lambda: None)
    monkeypatch.setattr(
        "scripts.generate_site.generate_site",
        lambda **_: [
            PageResult("headline", "index.html", tmp_path / "index.html", ok=True),
            PageResult("origins", "origins.html", tmp_path / "origins.html",
                       ok=False, error="boom"),
        ],
    )
    code = refresh_mod.refresh(
        output_dir=tmp_path, public_manifest_source=None, skip_pipeline=True
    )
    assert code == refresh_mod.EXIT_GENERATION_FAILED


def test_manifest_round_trips_through_json(tmp_path):
    """The gate reads a file written by another process; it must survive that."""
    from app.manifest import MANIFEST_FILENAME, write_manifest

    candidate = _manifest({"prices": "2026-08-19T18:15:00+00:00"})
    write_manifest(tmp_path, candidate)
    reloaded = json.loads((tmp_path / MANIFEST_FILENAME).read_text(encoding="utf-8"))
    assert verify_refresh_is_not_a_regression(reloaded, candidate).promotable


# ---------------------------------------------------------------------------
# Schedule-specific layer sets (--layers)
# ---------------------------------------------------------------------------


def test_refresh_threads_an_explicit_layer_set_into_the_pipeline(monkeypatch, tmp_path):
    """The 08:00 UTC slot passes ('dce',); it must reach main.run verbatim."""
    import scripts.refresh_prices as refresh_mod

    monkeypatch.setattr(refresh_mod, "setup_logging", lambda: None)
    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return 1  # stop before generation — the threading is the test

    monkeypatch.setattr(main, "run", fake_run)
    refresh_mod.refresh(output_dir=tmp_path, public_manifest_source=None,
                        layers=("dce",))
    assert captured["layer_keys"] == ("dce",)


def test_refresh_defaults_to_the_configured_fast_set(monkeypatch, tmp_path):
    import scripts.refresh_prices as refresh_mod

    monkeypatch.setattr(refresh_mod, "setup_logging", lambda: None)
    captured: dict[str, object] = {}

    def fake_run(**kwargs):
        captured.update(kwargs)
        return 1

    monkeypatch.setattr(main, "run", fake_run)
    refresh_mod.refresh(output_dir=tmp_path, public_manifest_source=None)
    assert captured["layer_keys"] == FAST_REFRESH_LAYERS


def test_refresh_cli_parses_a_comma_separated_layer_list(monkeypatch, tmp_path):
    import scripts.refresh_prices as refresh_mod

    captured: dict[str, object] = {}

    def fake_refresh(**kwargs):
        captured.update(kwargs)
        return 0

    monkeypatch.setattr(refresh_mod, "refresh", fake_refresh)
    refresh_mod.main(["--output-dir", str(tmp_path), "--layers", "dce, prices"])
    assert captured["layers"] == ("dce", "prices")

    captured.clear()
    refresh_mod.main(["--output-dir", str(tmp_path)])
    assert captured["layers"] is None
