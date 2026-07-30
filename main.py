"""
Mirror Market — main entry point.

Run this script to:
    1. Fetch commodity prices     (yfinance — always works, includes CME palm oil)
    2. Fetch USDA crop data       (requires USDA_API_KEY)
    3. Fetch FRED economic data   (requires FRED_API_KEY)
    4. Fetch COT positioning      (CFTC — no key needed)
    5. Fetch weather data         (Open-Meteo — no key needed)
    6. Fetch PSD global supply/demand (USDA FAS — no key needed)
    7. Fetch currency pairs       (yfinance — no key needed)
    8. Fetch World Bank monthly prices (no key needed)
    9. Fetch DCE Chinese futures  (AKShare — no key needed)
   10. Fetch USDA export sales    (requires FAS_API_KEY)
   11. Fetch forward curves       (yfinance — no key needed)
   12. Fetch WASDE monthly estimates (requires USDA_API_KEY)
   13. Fetch EIA biofuel/energy   (requires EIA_API_KEY)
   14. Fetch USDA crush + inspections (requires USDA_API_KEY + AMS report)
   15. Fetch CONAB Brazil estimates (no key needed)
   16. Clean everything
   17. Store it all in a local SQLite database
   18. Print a verification summary

Usage:
    python main.py

Key concepts for learning:
    - Graceful degradation: if one layer fails, we still save the rest.
    - try/except per layer means a USDA outage doesn't lose your prices.
    - logging replaces print() for professional, filterable output.
"""

import logging
import sys

from config import LAYER_MIN_KEYS, MAX_FAILED_LAYERS, setup_logging
from fetchers.agrural import fetch_agrural
from fetchers.akshare import fetch_dce_futures
from fetchers.conab import fetch_conab_estimates
from fetchers.cot import fetch_cot_recent
from fetchers.eia import fetch_all_eia
from fetchers.export_sales import fetch_all_export_sales
from fetchers.forward_curve import fetch_all_forward_curves
from fetchers.fred import fetch_all_series
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
    clean_inspections,
    clean_ohlcv,
    clean_psd,
    clean_safex,
    clean_wasde,
    clean_weather,
    clean_worldbank,
)
from pipeline.query import read_prices
from pipeline.store import (
    init_database,
    save_brazil_estimates,
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
DISABLED_LAYERS = frozenset({"india_domestic"})


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
        "conab": False,
        "india_domestic": False, "cepea": False, "safex": False,
        "agrural": False,
    }

    # ── Initialise database schema ─────────────────────────────────
    init_database()

    # ── Layer 1: Commodity Prices ────────────────────────────────
    price_data = {}
    try:
        logger.info("[Layer 1] Fetching commodity futures prices ...")
        price_data = fetch_prices()

        logger.info("[Cleaning] Processing price data ...")
        for name in price_data:
            price_data[name] = clean_ohlcv(price_data[name], label=name)

        for name, df in price_data.items():
            save_price_data(name, df)

        results["prices"] = _finalize_layer("prices", price_data)
    except Exception:
        logger.exception("[Layer 1] Prices failed — see error above")
        _mark_failed("prices")

    # ── Layer 2: USDA Fundamentals ───────────────────────────────
    usda_data = {}
    try:
        logger.info("[Layer 2] Fetching USDA soybean data ...")
        usda_data = fetch_soybean_overview()

        for stat, df in usda_data.items():
            save_usda_data(df, stat)

        if any(not df.empty for df in usda_data.values()):
            results["usda"] = True
            total_rows = sum(len(df) for df in usda_data.values())
            save_freshness("usda", total_rows)
        else:
            logger.warning("[Layer 2] USDA returned no data (API key missing?)")
            _mark_empty("usda")
    except Exception:
        logger.exception("[Layer 2] USDA failed — see error above")
        _mark_failed("usda")

    # ── Layer 2b: USDA Crop Progress/Condition ─────────────────────
    crop_progress_data = {}
    try:
        logger.info("[Layer 2b] Fetching USDA crop progress/condition ...")
        crop_progress_data = fetch_all_crop_progress()

        for crop, df in crop_progress_data.items():
            save_crop_progress(crop, df)

        if any(not df.empty for df in crop_progress_data.values()):
            results["crop_progress"] = True
            total_rows = sum(len(df) for df in crop_progress_data.values())
            save_freshness("crop_progress", total_rows)
        else:
            logger.warning("[Layer 2b] Crop progress returned no data (API key missing?)")
            _mark_empty("crop_progress")
    except Exception:
        logger.exception("[Layer 2b] Crop progress failed — see error above")
        _mark_failed("crop_progress")

    # ── Layer 3: FRED Economic Context ───────────────────────────
    fred_data = {}
    try:
        logger.info("[Layer 3] Fetching FRED economic indicators ...")
        fred_data = fetch_all_series()

        logger.info("[Cleaning] Processing FRED data ...")
        for name in fred_data:
            fred_data[name] = clean_fred_series(fred_data[name])

        for name, series in fred_data.items():
            save_fred_data(name, series)

        results["fred"] = _finalize_layer("fred", fred_data)
    except Exception:
        logger.exception("[Layer 3] FRED failed — see error above")
        _mark_failed("fred")

    # ── Layer 4: COT Positioning ─────────────────────────────────
    cot_data = {}
    try:
        logger.info("[Layer 4] Fetching CFTC Commitment of Traders data ...")
        cot_data = fetch_cot_recent()

        logger.info("[Cleaning] Processing COT data ...")
        for name in cot_data:
            cot_data[name] = clean_cot(cot_data[name])

        for name, df in cot_data.items():
            save_cot_data(name, df)

        results["cot"] = _finalize_layer("cot", cot_data)
    except Exception:
        logger.exception("[Layer 4] COT failed — see error above")
        _mark_failed("cot")

    # ── Layer 5: Weather ─────────────────────────────────────────
    weather_data = {}
    try:
        logger.info("[Layer 5] Fetching weather for growing regions ...")
        weather_data = fetch_all_regions()

        logger.info("[Cleaning] Processing weather data ...")
        for name in weather_data:
            weather_data[name] = clean_weather(weather_data[name])

        for region, df in weather_data.items():
            save_weather_data(region, df)

        results["weather"] = _finalize_layer("weather", weather_data)
    except Exception:
        logger.exception("[Layer 5] Weather failed — see error above")
        _mark_failed("weather")

    # ── Layer 6: PSD Global Supply/Demand ────────────────────────
    psd_data = {}
    try:
        logger.info("[Layer 6] Fetching USDA FAS PSD global data ...")
        psd_data = fetch_psd_all()

        logger.info("[Cleaning] Processing PSD data ...")
        for name in psd_data:
            psd_data[name] = clean_psd(psd_data[name])

        for name, df in psd_data.items():
            save_psd_data(name, df)

        results["psd"] = _finalize_layer("psd", psd_data)
    except Exception:
        logger.exception("[Layer 6] PSD failed — see error above")
        _mark_failed("psd")

    # ── Layer 7: Currencies ──────────────────────────────────────
    currency_data = {}
    try:
        logger.info("[Layer 7] Fetching currency pairs ...")
        currency_data = fetch_currencies()

        logger.info("[Cleaning] Processing currency data ...")
        for name in currency_data:
            currency_data[name] = clean_ohlcv(currency_data[name], label=name)

        for pair, df in currency_data.items():
            save_currency_data(pair, df)

        results["currencies"] = _finalize_layer("currencies", currency_data)
    except Exception:
        logger.exception("[Layer 7] Currencies failed — see error above")
        _mark_failed("currencies")

    # ── Layer 8: World Bank Monthly Prices ───────────────────────
    wb_data = {}
    try:
        logger.info("[Layer 8] Fetching World Bank Pink Sheet prices ...")
        wb_data = fetch_worldbank_prices()

        logger.info("[Cleaning] Processing World Bank data ...")
        for name in wb_data:
            wb_data[name] = clean_worldbank(wb_data[name])

        for name, df in wb_data.items():
            save_worldbank_data(name, df)

        if any(not df.empty for df in wb_data.values()):
            results["worldbank"] = True
            total_rows = sum(len(df) for df in wb_data.values())
            save_freshness("worldbank", total_rows)
        else:
            logger.warning("[Layer 8] World Bank returned no data")
            _mark_empty("worldbank")
    except Exception:
        logger.exception("[Layer 8] World Bank failed — see error above")
        _mark_failed("worldbank")

    # ── Layer 9: DCE Chinese Futures ──────────────────────────────
    dce_data = {}
    try:
        logger.info("[Layer 9] Fetching DCE futures (AKShare) ...")
        dce_data = fetch_dce_futures()

        logger.info("[Cleaning] Processing DCE futures data ...")
        for name in dce_data:
            dce_data[name] = clean_dce_futures(dce_data[name])

        for name, df in dce_data.items():
            save_dce_futures_data(name, df)

        results["dce"] = _finalize_layer("dce", dce_data)
    except Exception:
        logger.exception("[Layer 9] DCE failed — see error above")
        _mark_failed("dce")

    # ── Layer 10: USDA Export Sales ─────────────────────────────
    export_sales_data = {}
    try:
        logger.info("[Layer 10] Fetching USDA export sales ...")
        export_sales_data = fetch_all_export_sales()

        if export_sales_data:
            logger.info("[Cleaning] Processing export sales data ...")
            for name in export_sales_data:
                export_sales_data[name] = clean_export_sales(export_sales_data[name])

            for name, df in export_sales_data.items():
                save_export_sales(name, df)

            if any(not df.empty for df in export_sales_data.values()):
                results["export_sales"] = True
                total_rows = sum(len(df) for df in export_sales_data.values())
                save_freshness("export_sales", total_rows)
            else:
                logger.warning("[Layer 10] Export sales returned no data (FAS_API_KEY missing?)")
                _mark_empty("export_sales")
        else:
            logger.info("[Layer 10] Export sales skipped (FAS_API_KEY not set)")
    except Exception:
        logger.exception("[Layer 10] Export sales failed — see error above")
        _mark_failed("export_sales")

    # ── Layer 11: Forward Curves ────────────────────────────────
    forward_curve_data = {}
    try:
        logger.info("[Layer 11] Fetching forward curves ...")
        forward_curve_data = fetch_all_forward_curves()

        logger.info("[Cleaning] Processing forward curve data ...")
        for name in forward_curve_data:
            forward_curve_data[name] = clean_forward_curve(forward_curve_data[name])

        for name, df in forward_curve_data.items():
            save_forward_curve(name, df)

        if any(not df.empty for df in forward_curve_data.values()):
            results["forward_curve"] = True
            total_rows = sum(len(df) for df in forward_curve_data.values())
            save_freshness("forward_curve", total_rows)
        else:
            logger.warning("[Layer 11] Forward curves returned no data")
            _mark_empty("forward_curve")
    except Exception:
        logger.exception("[Layer 11] Forward curves failed — see error above")
        _mark_failed("forward_curve")

    # ── Layer 12: WASDE Monthly Estimates ────────────────────────
    wasde_data = {}
    try:
        logger.info("[Layer 12] Fetching WASDE monthly estimates ...")
        wasde_data = fetch_wasde_estimates()

        if wasde_data:
            logger.info("[Cleaning] Processing WASDE data ...")
            for key in wasde_data:
                wasde_data[key] = clean_wasde(wasde_data[key])

            for key, df in wasde_data.items():
                save_wasde(key, df)

            if any(not df.empty for df in wasde_data.values()):
                results["wasde"] = True
                total_rows = sum(len(df) for df in wasde_data.values())
                save_freshness("wasde", total_rows)
            else:
                logger.warning("[Layer 12] WASDE returned no data — OCE archive may be unreachable")
                _mark_empty("wasde")
        else:
            logger.warning("[Layer 12] WASDE returned no files — OCE archive unreachable?")
            _mark_empty("wasde")
    except Exception:
        logger.exception("[Layer 12] WASDE failed — see error above")
        _mark_failed("wasde")

    # ── Layer 13: EIA Biofuel/Energy ──────────────────────────────
    eia_data = {}
    try:
        logger.info("[Layer 13] Fetching EIA energy/biofuel data ...")
        eia_data = fetch_all_eia()

        if eia_data:
            logger.info("[Cleaning] Processing EIA data ...")
            for name in eia_data:
                eia_data[name] = clean_eia(eia_data[name])

            for name, df in eia_data.items():
                save_eia_data(name, df)

            if any(not df.empty for df in eia_data.values()):
                results["eia"] = True
                total_rows = sum(len(df) for df in eia_data.values())
                save_freshness("eia", total_rows)
            else:
                logger.warning("[Layer 13] EIA returned no data")
                _mark_empty("eia")
        else:
            logger.info("[Layer 13] EIA skipped (EIA_API_KEY not set)")
    except Exception:
        logger.exception("[Layer 13] EIA failed — see error above")
        _mark_failed("eia")

    # ── Layer 14: USDA Crush/Processing + Export Inspections ──────
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

    # ── Layer 16: NCDEX India Domestic Soy Prices ─────────────────
    # DISABLED 2026-05: ncdex.com now serves a JavaScript fingerprint
    # interstitial (__hd_fingerprint cookie via /__verify/fp) on every URL,
    # including the Bhav Copy endpoints. requests.get() cannot pass it.
    # Re-enable when a new source is wired up (alternate India spot soy
    # feed or a headless-browser bypass). The fetcher's diagnostic logging
    # will confirm the wall is still in place if you run it standalone.
    logger.info("[Layer 16] NCDEX disabled — blocked by ncdex.com anti-bot wall")
    _mark_empty("india_domestic")

    # ── Layer 17: CEPEA Brazil Domestic Soy Spot ──────────────────
    # DISABLED 2026-05-12: cepea.esalq.usp.br is fronted by a Cloudflare
    # Turnstile JavaScript challenge that cannot be solved with header
    # tweaks (HTTP 403 on every URL). Layer last had real data on
    # 2026-02-20. Re-enable when an alternate Brazil spot source is wired
    # up (Infosimples, Playwright, or another index). fetchers/cepea.py
    # is intentionally kept on disk so re-enabling is a one-line revert.
    logger.info("[Layer 17] CEPEA disabled — Cloudflare Turnstile JS challenge")
    _mark_empty("cepea")

    # ── Layer 18: JSE SAFEX South Africa Soy Prices ───────────────
    try:
        logger.info("[Layer 18] Fetching JSE SAFEX South Africa soy prices ...")
        safex_result = fetch_safex()

        if safex_result.has_rows:
            for name, df in safex_result.data.items():
                df = clean_safex(df)
                save_safex(name, df)

            results["safex"] = True
            save_freshness("safex", safex_result.total_rows)
            logger.info("[Layer 18] SAFEX: %d rows saved", safex_result.total_rows)
        elif safex_result.status == "failed":
            logger.error("[Layer 18] SAFEX failed: %s", safex_result.error)
            _mark_failed("safex")
        else:
            logger.warning("[Layer 18] SAFEX empty: %s", safex_result.error)
            _mark_empty("safex")
    except Exception:
        logger.exception("[Layer 18] SAFEX failed — see error above")
        _mark_failed("safex")

    # ── Layer 19: AgRural Paranaguá FOB Soy Quote ────────────────
    try:
        logger.info("[Layer 19] Fetching AgRural Paranaguá FOB soy quote ...")
        agrural_result = fetch_agrural()

        if agrural_result.has_rows:
            for name, df in agrural_result.data.items():
                df = clean_brazil_spot(df)
                save_brazil_spot(name, df)

            results["agrural"] = True
            save_freshness("agrural", agrural_result.total_rows)
            logger.info("[Layer 19] AgRural: %d rows saved", agrural_result.total_rows)
        else:
            logger.error("[Layer 19] AgRural failed: %s", agrural_result.error)
            _mark_failed("agrural")
    except Exception:
        logger.exception("[Layer 19] AgRural failed — see error above")
        _mark_failed("agrural")

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
