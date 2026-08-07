"""
Mirror Market — main entry point.

Run this script to fetch, clean, and store all 20 data layers:
    commodity prices, USDA crop data + progress, FRED, COT, weather,
    PSD, currencies, World Bank, DCE futures, export sales, forward
    curves, WASDE, EIA, crush + inspections, CONAB (estimates +
    weekly farmgate prices), India mandi domestic, CEPEA via Notícias
    Agrícolas, SAFEX, AgRural FOB, and AMS Gulf export bids — then
    print a verification summary.

Usage:
    python main.py

Key concepts for learning:
    - Graceful degradation: if one layer fails, we still save the rest.
    - try/except per layer means a USDA outage doesn't lose your prices.
    - logging replaces print() for professional, filterable output.
"""

import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from config import LAYER_MIN_KEYS, MAX_FAILED_LAYERS, setup_logging
from fetchers.agrural import fetch_agrural
from fetchers.akshare import fetch_dce_futures
from fetchers.conab import fetch_conab_estimates
from fetchers.conab_precos import fetch_conab_farmgate
from fetchers.cot import fetch_cot_recent
from fetchers.eia import fetch_all_eia
from fetchers.export_sales import fetch_all_export_sales
from fetchers.forward_curve import fetch_all_forward_curves
from fetchers.fred import fetch_all_series
from fetchers.gulf_bids import fetch_gulf_bids
from fetchers.magyp_fob import fetch_magyp_fob
from fetchers.mandi import fetch_mandi_prices
from fetchers.noticias_agricolas import fetch_noticias_agricolas
from fetchers.psd import fetch_psd_all
from fetchers.safex import fetch_safex
from fetchers.usda import (
    fetch_all_crop_progress,
    fetch_crush_data,
    fetch_export_inspections,
    fetch_soybean_overview,
)
from fetchers.wasde import fetch_wasde_estimates
from fetchers.weather import fetch_all_regions
from fetchers.worldbank import fetch_worldbank_prices
from fetchers.yfinance import fetch_all as fetch_prices
from fetchers.yfinance import fetch_currencies
from pipeline.clean import (
    clean_brazil_spot,
    clean_conab,
    clean_cot,
    clean_dce_futures,
    clean_eia,
    clean_export_sales,
    clean_forward_curve,
    clean_fred_series,
    clean_india_domestic,
    clean_inspections,
    clean_ohlcv,
    clean_psd,
    clean_safex,
    clean_wasde,
    clean_weather,
    clean_worldbank,
)
from pipeline.history import HistoryImportError, export_history, import_history
from pipeline.query import read_prices
from pipeline.results import FetchResult
from pipeline.store import (
    init_database,
    save_brazil_estimates,
    save_argentina_fob,
    save_brazil_spot,
    save_cot_data,
    save_crop_progress,
    save_currency_data,
    save_dce_futures_data,
    save_eia_data,
    save_export_sales,
    save_forward_curve,
    save_fred_data,
    save_freshness,
    save_gulf_bids,
    save_india_domestic,
    save_inspection_destinations,
    save_inspections,
    save_port_flows,
    save_price_data,
    save_psd_data,
    save_safex,
    save_usda_data,
    save_wasde,
    save_weather_data,
    save_worldbank_data,
    update_commodity_freshness,
)

logger = logging.getLogger(__name__)

# Layers whose failure means the pipeline run is unusable for traders.
# main() exits non-zero if any of these fail so CI can fail the deploy.
CRITICAL_LAYERS = ("prices", "fred")

# Layers deliberately switched off (upstream anti-bot walls) — excluded from
# the failed-layer count so they don't trip the systemic-outage backstop.
# Empty since 2026-08: Layer 16 came back on the data.gov.in mandi API.
DISABLED_LAYERS: frozenset[str] = frozenset()


# Layers hard-failed during the current run() — transport/parse/shape
# failures, not quiet empties. Feeds the systemic-outage backstop.
_HARD_FAILURES: set[str] = set()


def _mark_failed(layer: str) -> None:
    """Best-effort 'failed' freshness row — never crashes the pipeline itself."""
    _HARD_FAILURES.add(layer)
    try:
        save_freshness(layer, status="failed")
    except Exception:
        logger.exception("Could not record failed-freshness row for %s", layer)


def _mark_empty(layer: str) -> None:
    """Record a successful run that returned zero rows.

    Distinct from _mark_failed: the layer ran to completion and the upstream
    legitimately had nothing to publish (no inspection report this week, no
    matching contracts traded, etc). Without this, an empty result is
    indistinguishable from "the layer never ran" on the dashboard.
    """
    try:
        save_freshness(layer, rows_fetched=0, status="success")
    except Exception:
        logger.exception("Could not record empty-success freshness row for %s", layer)


def _mark_disabled(layer: str) -> None:
    """Record an intentionally disabled layer without fabricating freshness.

    Unlike _mark_empty this does NOT stamp last_success, so a hard-disabled
    layer (anti-bot wall) never reads as freshly successful on the dashboard.
    """
    try:
        save_freshness(layer, rows_fetched=0, status="disabled")
    except Exception:
        logger.exception("Could not record disabled freshness row for %s", layer)


def _finalize_layer(layer: str, data: dict) -> bool:
    """Record freshness for a dict-of-frames layer; return overall success.

    Applies the per-layer expected-count floor from LAYER_MIN_KEYS: a layer
    where only 1 of 13 keys returned data is an outage, not a success, so
    below-floor runs are logged as partial and recorded as failed freshness
    (which preserves last_success for staleness display). All-empty is
    recorded as empty-success — unless the layer is critical, where empty
    and failed are equally unusable.
    """
    non_empty = sum(1 for v in data.values() if not v.empty)
    total_rows = sum(len(v) for v in data.values())
    floor = LAYER_MIN_KEYS.get(layer, 1)

    if non_empty == 0:
        logger.warning("[%s] returned no data", layer)
        if layer in CRITICAL_LAYERS:
            _mark_failed(layer)
        else:
            _mark_empty(layer)
        return False
    if non_empty < floor:
        logger.warning(
            "[%s] partial: only %d/%d keys returned data (floor %d) — recording as failed",
            layer, non_empty, len(data), floor,
        )
        _mark_failed(layer)
        return False
    save_freshness(layer, total_rows)
    return True


@dataclass(frozen=True)
class DictLayer:
    """One fetch → clean → save → finalize pass over a dict-of-frames layer.

    Every DictLayer goes through _finalize_layer, so the LAYER_MIN_KEYS
    partial-outage floor applies uniformly (previously six layers bypassed
    it with hand-rolled success checks).
    """

    key: str                                    # results/freshness key
    label: str                                  # "Layer 4" — for log prefixes
    desc: str                                   # human description for logs
    fetch: Callable[[], dict]
    save: Callable[[str, Any], None]            # (name, frame) -> None
    clean: Callable[[str, Any], Any] | None = None  # (name, frame) -> frame
    # API-key-gated layers: fetch() returns {} when the key isn't set.
    # Logged as skipped — no freshness row, matching "the layer never ran".
    skip_msg: str | None = None


def _run_dict_layer(layer: DictLayer) -> bool:
    try:
        logger.info("[%s] Fetching %s ...", layer.label, layer.desc)
        data = layer.fetch()

        if not data and layer.skip_msg:
            logger.info("[%s] %s", layer.label, layer.skip_msg)
            return False

        if layer.clean is not None and data:
            logger.info("[Cleaning] Processing %s data ...", layer.key)
            for name in data:
                data[name] = layer.clean(name, data[name])

        for name, df in data.items():
            layer.save(name, df)

        return _finalize_layer(layer.key, data)
    except Exception:
        logger.exception("[%s] %s failed — see error above", layer.label, layer.desc)
        _mark_failed(layer.key)
        return False


def _run_scraper_layer(
    key: str,
    label: str,
    desc: str,
    fetch: Callable[[], FetchResult],
    save: Callable[[str, Any], None],
    clean: Callable[[Any], Any] | None = None,
    empty_fails: bool = True,
) -> bool:
    """One FetchResult-based scraper layer (Layers 17-20).

    empty_fails: a daily quote source that returns zero rows is broken
    (CEPEA, AgRural, Gulf bids); SAFEX legitimately publishes nothing on
    JSE holidays, so its empty result records as empty-success instead.
    """
    try:
        logger.info("[%s] Fetching %s ...", label, desc)
        result = fetch()

        if result.has_rows:
            for name, df in result.data.items():
                if clean is not None:
                    df = clean(df)
                save(name, df)
            save_freshness(key, result.total_rows)
            logger.info("[%s] %s: %d rows saved", label, key, result.total_rows)
            return True

        if empty_fails or result.status == "failed":
            logger.error("[%s] %s failed: %s", label, desc, result.error)
            _mark_failed(key)
        else:
            logger.warning("[%s] %s empty: %s", label, desc, result.error)
            _mark_empty(key)
        return False
    except Exception:
        logger.exception("[%s] %s failed — see error above", label, desc)
        _mark_failed(key)
        return False


def run() -> int:
    setup_logging()
    _HARD_FAILURES.clear()

    logger.info("=" * 60)
    logger.info("  Mirror Market — Data Pipeline")
    logger.info("=" * 60)

    # Track which layers succeeded vs failed
    results = {
        "prices": False, "usda": False, "crop_progress": False,
        "fred": False, "cot": False, "weather": False,
        "psd": False, "currencies": False, "worldbank": False,
        "dce": False, "export_sales": False, "forward_curve": False,
        "wasde": False, "eia": False, "crush_inspections": False,
        "conab": False, "conab_precos": False,
        "india_domestic": False,
        "cepea": False, "safex": False,
        "agrural": False, "gulf_bids": False,
    }
    # Layers intentionally short-circuited (upstream anti-bot walls) —
    # reported separately so the Failed list only carries real outages.
    disabled = sorted(DISABLED_LAYERS)

    # ── Initialise database schema ─────────────────────────────────
    init_database()

    # ── Seed snapshot-only history from git-committed CSVs ─────────
    # Must hard-fail: exporting later from a DB that failed to seed
    # would overwrite the committed CSVs with today-only data.
    try:
        import_history()
    except HistoryImportError:
        logger.exception("History import failed — aborting before any export can clobber it")
        return 1

    # ── Layers 1-13: uniform dict-of-frames layers ────────────────
    # Built inside run() so tests that monkeypatch main.<fetcher> are
    # picked up — the lambdas resolve module globals at call time.
    dict_layers = [
        DictLayer(
            "prices", "Layer 1", "commodity futures prices",
            fetch=lambda: fetch_prices(),
            save=lambda n, d: save_price_data(n, d),
            clean=lambda n, d: clean_ohlcv(d, label=n),
        ),
        DictLayer(
            "usda", "Layer 2", "USDA soybean data",
            fetch=lambda: fetch_soybean_overview(),
            save=lambda n, d: save_usda_data(d, n),
        ),
        DictLayer(
            "crop_progress", "Layer 2b", "USDA crop progress/condition",
            fetch=lambda: fetch_all_crop_progress(),
            save=lambda n, d: save_crop_progress(n, d),
        ),
        DictLayer(
            "fred", "Layer 3", "FRED economic indicators",
            fetch=lambda: fetch_all_series(),
            save=lambda n, d: save_fred_data(n, d),
            clean=lambda n, d: clean_fred_series(d),
        ),
        DictLayer(
            "cot", "Layer 4", "CFTC Commitment of Traders data",
            fetch=lambda: fetch_cot_recent(),
            save=lambda n, d: save_cot_data(n, d),
            clean=lambda n, d: clean_cot(d),
        ),
        DictLayer(
            "weather", "Layer 5", "weather for growing regions",
            fetch=lambda: fetch_all_regions(),
            save=lambda n, d: save_weather_data(n, d),
            clean=lambda n, d: clean_weather(d),
        ),
        DictLayer(
            "psd", "Layer 6", "USDA FAS PSD global data",
            fetch=lambda: fetch_psd_all(),
            save=lambda n, d: save_psd_data(n, d),
            clean=lambda n, d: clean_psd(d),
        ),
        DictLayer(
            "currencies", "Layer 7", "currency pairs",
            fetch=lambda: fetch_currencies(),
            save=lambda n, d: save_currency_data(n, d),
            clean=lambda n, d: clean_ohlcv(d, label=n),
        ),
        DictLayer(
            "worldbank", "Layer 8", "World Bank Pink Sheet prices",
            fetch=lambda: fetch_worldbank_prices(),
            save=lambda n, d: save_worldbank_data(n, d),
            clean=lambda n, d: clean_worldbank(d),
        ),
        DictLayer(
            "dce", "Layer 9", "DCE futures (AKShare)",
            fetch=lambda: fetch_dce_futures(),
            save=lambda n, d: save_dce_futures_data(n, d),
            clean=lambda n, d: clean_dce_futures(d),
        ),
        DictLayer(
            "export_sales", "Layer 10", "USDA export sales",
            fetch=lambda: fetch_all_export_sales(),
            save=lambda n, d: save_export_sales(n, d),
            clean=lambda n, d: clean_export_sales(d),
            skip_msg="Export sales skipped (FAS_API_KEY not set)",
        ),
        DictLayer(
            "forward_curve", "Layer 11", "forward curves",
            fetch=lambda: fetch_all_forward_curves(),
            save=lambda n, d: save_forward_curve(n, d),
            clean=lambda n, d: clean_forward_curve(d),
        ),
        DictLayer(
            "wasde", "Layer 12", "WASDE monthly estimates",
            fetch=lambda: fetch_wasde_estimates(),
            save=lambda n, d: save_wasde(n, d),
            clean=lambda n, d: clean_wasde(d),
        ),
        DictLayer(
            "eia", "Layer 13", "EIA energy/biofuel data",
            fetch=lambda: fetch_all_eia(),
            save=lambda n, d: save_eia_data(n, d),
            clean=lambda n, d: clean_eia(d),
            skip_msg="EIA skipped (EIA_API_KEY not set)",
        ),
    ]
    for layer in dict_layers:
        results[layer.key] = _run_dict_layer(layer)

    # ── Layer 14: USDA Crush/Processing + Export Inspections ──────
    # Custom: two sources (QuickStats CRUSHED + AMS text report) sharing
    # one freshness key, with a per-commodity save split.
    try:
        logger.info("[Layer 14] Fetching USDA crush data + export inspections ...")
        total_14 = 0

        # Crush data (same USDA API, stat_category=CRUSHED)
        crush_df = fetch_crush_data()
        if not crush_df.empty:
            save_usda_data(crush_df, "CRUSHED")
            total_14 += len(crush_df)

        # Export inspections (AMS text report)
        insp_result = fetch_export_inspections()
        insp_df = insp_result.data.get("inspections")
        if insp_df is not None and not insp_df.empty:
            insp_df = clean_inspections(insp_df)
            for commodity in insp_df["commodity"].unique():
                subset = insp_df[insp_df["commodity"] == commodity]
                save_inspections(commodity, subset)
            total_14 += len(insp_df)

        # Port-area breakdown (Table C of the same report)
        flows_df = insp_result.data.get("port_flows")
        if flows_df is not None and not flows_df.empty:
            save_port_flows(flows_df)
            total_14 += len(flows_df)

        # Destination-country breakdown (same report)
        dest_df = insp_result.data.get("destinations")
        if dest_df is not None and not dest_df.empty:
            save_inspection_destinations(dest_df)
            total_14 += len(dest_df)

        if total_14 > 0:
            results["crush_inspections"] = True
            save_freshness("crush_inspections", total_14)
        elif insp_result.status == "failed":
            logger.error("[Layer 14] Inspections failed: %s", insp_result.error)
            _mark_failed("crush_inspections")
        else:
            logger.warning("[Layer 14] Crush/inspections returned no data")
            _mark_empty("crush_inspections")
    except Exception:
        logger.exception("[Layer 14] Crush/inspections failed — see error above")
        _mark_failed("crush_inspections")

    # ── Layer 15: CONAB Brazil Crop Estimates ─────────────────────
    # Custom: single national DataFrame, not a dict of frames.
    try:
        logger.info("[Layer 15] Fetching CONAB Brazil estimates ...")
        conab_df = fetch_conab_estimates()

        if not conab_df.empty:
            conab_df = clean_conab(conab_df)
            save_brazil_estimates(conab_df)
            results["conab"] = True
            save_freshness("conab", len(conab_df))
        else:
            logger.warning("[Layer 15] CONAB returned no data")
            _mark_empty("conab")
    except Exception:
        logger.exception("[Layer 15] CONAB failed — see error above")
        _mark_failed("conab")

    # ── Layer 15b: CONAB weekly farmgate prices ───────────────────
    # Paraná producer-price series — cross-checks the CEPEA/ESALQ Paraná
    # wholesale indicator (Layer 17). Own commodity key; never spliced.
    results["conab_precos"] = _run_scraper_layer(
        "conab_precos", "Layer 15b", "CONAB weekly farmgate prices",
        fetch=lambda: fetch_conab_farmgate(),
        save=lambda n, d: save_brazil_spot(n, d),
        clean=lambda d: clean_brazil_spot(d),
    )

    # ── Layer 16: India Mandi Domestic Soy Prices ─────────────────
    # Rebuilt 2026-08 on the data.gov.in Mandi Price API (official
    # Agmarknet feed) after NCDEX became unusable (SEBI derivatives
    # suspension to ≥2027-03 + fingerprint wall on the spot pages;
    # fetchers/india_domestic.py kept on disk as a dormant fallback).
    # Mandis close on Sundays/holidays, so an empty day is a normal
    # empty-success, not a failure.
    results["india_domestic"] = _run_scraper_layer(
        "india_domestic", "Layer 16", "India mandi soy prices (data.gov.in)",
        fetch=lambda: fetch_mandi_prices(),
        save=lambda n, d: save_india_domestic(n, d),
        clean=lambda d: clean_india_domestic(d),
        empty_fails=False,
    )

    # ── Layers 17-20: FetchResult scraper layers ──────────────────
    # Layer 17: re-enabled 2026-07-30 via Notícias Agrícolas, which
    # republishes the CEPEA/ESALQ indicators server-rendered
    # (cepea.org.br itself is still behind a Cloudflare Turnstile
    # challenge; fetchers/cepea.py kept on disk in case the direct
    # source ever reopens).
    results["cepea"] = _run_scraper_layer(
        "cepea", "Layer 17", "CEPEA indicators via Notícias Agrícolas",
        fetch=lambda: fetch_noticias_agricolas(),
        save=lambda n, d: save_brazil_spot(n, d),
        clean=lambda d: clean_brazil_spot(d),
    )
    results["safex"] = _run_scraper_layer(
        "safex", "Layer 18", "JSE SAFEX South Africa soy prices",
        fetch=lambda: fetch_safex(),
        save=lambda n, d: save_safex(n, d),
        clean=lambda d: clean_safex(d),
        empty_fails=False,
    )
    results["agrural"] = _run_scraper_layer(
        "agrural", "Layer 19", "AgRural Paranaguá FOB soy quote",
        fetch=lambda: fetch_agrural(),
        save=lambda n, d: save_brazil_spot(n, d),
        clean=lambda d: clean_brazil_spot(d),
    )
    results["gulf_bids"] = _run_scraper_layer(
        "gulf_bids", "Layer 20", "AMS Gulf export bids",
        fetch=lambda: fetch_gulf_bids(),
        save=lambda n, d: save_gulf_bids(d),
    )
    # Layer 21: walks back over weekends/holidays itself, so an empty
    # result genuinely means the source is broken → empty_fails stays on.
    results["magyp_fob"] = _run_scraper_layer(
        "magyp_fob", "Layer 21", "Argentina MAGyP official FOB prices",
        fetch=lambda: fetch_magyp_fob(),
        save=lambda n, d: save_argentina_fob(d),
    )

    # ── Export snapshot-only history back to git-committed CSVs ──
    # Failure exits non-zero so the workflow's commit step never runs
    # against half-written files (writes are atomic per table anyway).
    try:
        export_history()
    except Exception:
        logger.exception("History export failed — pipeline exiting with status 1")
        return 1

    # ── Update per-commodity freshness tracking ─────────────────
    try:
        update_commodity_freshness()
    except Exception:
        logger.exception("Per-commodity freshness update failed")

    # ── Run data health check ─────────────────────────────────
    try:
        from analysis.health import run_health_check
        health = run_health_check()
        if health["issues"]:
            logger.info("\n%s", health["summary"])
        else:
            logger.info("DATA HEALTH: All systems green")
    except Exception:
        logger.exception("Health check failed")

    # ── Verify ───────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("  Verification Summary")
    logger.info("=" * 60)

    all_prices = read_prices()
    if all_prices.empty:
        logger.warning("  No price data in database!")
    else:
        for commodity in all_prices["commodity"].unique():
            subset = all_prices[all_prices["commodity"] == commodity]
            latest = subset.sort_values("Date").iloc[-1]
            logger.info(
                "  %15s  |  rows: %4d  |  latest close: %10.2f  |  date: %s",
                commodity, len(subset), latest["Close"], latest["Date"].date(),
            )

    # ── Final summary ────────────────────────────────────────────
    succeeded = [name for name, ok in results.items() if ok]
    failed = [name for name, ok in results.items() if not ok]

    logger.info("-" * 60)
    if succeeded:
        logger.info("Succeeded: %s", ", ".join(succeeded))
    if disabled:
        logger.info("Disabled:  %s", ", ".join(disabled))
    if failed:
        logger.warning("Failed:    %s", ", ".join(failed))
    logger.info("Database saved to: data/storage/mirror_market.db")

    # ── Exit code ────────────────────────────────────────────────
    # Non-critical layer failures are logged but do not fail the run.
    # If a critical layer (prices or FRED economic) failed, exit 1 so
    # CI/deploy workflows can react.
    critical_failures = [name for name in CRITICAL_LAYERS if not results.get(name)]
    if critical_failures:
        logger.error(
            "Critical layer(s) failed: %s — pipeline exiting with status 1",
            ", ".join(critical_failures),
        )
        return 1

    # Systemic-outage backstop: no single non-critical layer fails the run,
    # but a broad sweep of hard failures (transport/parse — not quiet
    # empties) means the environment itself is broken (network, DNS,
    # expired keys) and the deploy should not look green.
    active_failures = sorted(_HARD_FAILURES - DISABLED_LAYERS)
    if len(active_failures) > MAX_FAILED_LAYERS:
        logger.error(
            "%d active layers failed (threshold %d): %s — pipeline exiting with status 1",
            len(active_failures), MAX_FAILED_LAYERS, ", ".join(active_failures),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(run())
