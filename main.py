"""
Mirror Market — main entry point.

Run this script to:
    1. Fetch commodity prices     (yfinance — always works, includes BMD palm oil)
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
   16. Fetch options sentiment    (yfinance — experimental)
   17. Clean everything
   18. Store it all in a local SQLite database
   19. Print a verification summary

Usage:
    python main.py

Key concepts for learning:
    - Graceful degradation: if one layer fails, we still save the rest.
    - try/except per layer means a USDA outage doesn't lose your prices.
    - logging replaces print() for professional, filterable output.
"""

import logging

from config import setup_logging

from data.fetchers.yfinance_fetcher import fetch_all as fetch_prices
from data.fetchers.yfinance_fetcher import fetch_currencies
from data.fetchers.usda_fetcher import (
    fetch_soybean_overview, fetch_all_crop_progress,
    fetch_wasde_estimates, fetch_crush_data, fetch_export_inspections,
)
from data.fetchers.fred_fetcher import fetch_all_series
from data.fetchers.cot_fetcher import fetch_cot_recent
from data.fetchers.weather_fetcher import fetch_all_regions
from data.fetchers.psd_fetcher import fetch_psd_all
from data.fetchers.worldbank_fetcher import fetch_worldbank_prices
from data.fetchers.akshare_fetcher import fetch_dce_futures
from data.fetchers.export_sales_fetcher import fetch_all_export_sales
from data.fetchers.forward_curve_fetcher import fetch_all_forward_curves
from data.fetchers.eia_fetcher import fetch_all_eia
from data.fetchers.conab_fetcher import fetch_conab_estimates
from data.fetchers.options_fetcher import fetch_options_sentiment

from processing.cleaner import (
    clean_ohlcv, clean_fred_series, clean_cot, clean_weather,
    clean_psd, clean_currencies, clean_worldbank, clean_dce_futures,
    clean_export_sales, clean_forward_curve,
    clean_wasde, clean_eia, clean_inspections, clean_conab, clean_options,
)
from processing.combiner import (
    init_database,
    save_price_data,
    save_fred_data,
    save_usda_data,
    save_cot_data,
    save_weather_data,
    save_psd_data,
    save_currency_data,
    save_worldbank_data,
    save_dce_futures_data,
    save_crop_progress,
    save_export_sales,
    save_forward_curve,
    save_wasde,
    save_inspections,
    save_eia_data,
    save_brazil_estimates,
    save_options_sentiment,
    save_freshness,
    read_prices,
)

logger = logging.getLogger(__name__)


def run():
    setup_logging()

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
        "conab": False, "options": False,
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
            price_data[name] = clean_ohlcv(price_data[name])

        for name, df in price_data.items():
            save_price_data(name, df)

        if any(not df.empty for df in price_data.values()):
            results["prices"] = True
            total_rows = sum(len(df) for df in price_data.values())
            save_freshness("prices", total_rows)
        else:
            logger.warning("[Layer 1] All tickers returned empty data")
    except Exception:
        logger.error("[Layer 1] Prices failed — see error above", exc_info=True)

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
    except Exception:
        logger.error("[Layer 2] USDA failed — see error above", exc_info=True)

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
    except Exception:
        logger.error("[Layer 2b] Crop progress failed — see error above", exc_info=True)

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

        if any(not s.empty for s in fred_data.values()):
            results["fred"] = True
            total_rows = sum(len(s) for s in fred_data.values())
            save_freshness("fred", total_rows)
        else:
            logger.warning("[Layer 3] FRED returned no data (API key missing?)")
    except Exception:
        logger.error("[Layer 3] FRED failed — see error above", exc_info=True)

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

        if any(not df.empty for df in cot_data.values()):
            results["cot"] = True
            total_rows = sum(len(df) for df in cot_data.values())
            save_freshness("cot", total_rows)
        else:
            logger.warning("[Layer 4] COT returned no data")
    except Exception:
        logger.error("[Layer 4] COT failed — see error above", exc_info=True)

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

        if any(not df.empty for df in weather_data.values()):
            results["weather"] = True
            total_rows = sum(len(df) for df in weather_data.values())
            save_freshness("weather", total_rows)
        else:
            logger.warning("[Layer 5] Weather returned no data")
    except Exception:
        logger.error("[Layer 5] Weather failed — see error above", exc_info=True)

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

        if any(not df.empty for df in psd_data.values()):
            results["psd"] = True
            total_rows = sum(len(df) for df in psd_data.values())
            save_freshness("psd", total_rows)
        else:
            logger.warning("[Layer 6] PSD returned no data")
    except Exception:
        logger.error("[Layer 6] PSD failed — see error above", exc_info=True)

    # ── Layer 7: Currencies ──────────────────────────────────────
    currency_data = {}
    try:
        logger.info("[Layer 7] Fetching currency pairs ...")
        currency_data = fetch_currencies()

        logger.info("[Cleaning] Processing currency data ...")
        for name in currency_data:
            currency_data[name] = clean_currencies(currency_data[name])

        for pair, df in currency_data.items():
            save_currency_data(pair, df)

        if any(not df.empty for df in currency_data.values()):
            results["currencies"] = True
            total_rows = sum(len(df) for df in currency_data.values())
            save_freshness("currencies", total_rows)
        else:
            logger.warning("[Layer 7] Currencies returned no data")
    except Exception:
        logger.error("[Layer 7] Currencies failed — see error above", exc_info=True)

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
    except Exception:
        logger.error("[Layer 8] World Bank failed — see error above", exc_info=True)

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

        if any(not df.empty for df in dce_data.values()):
            results["dce"] = True
            total_rows = sum(len(df) for df in dce_data.values())
            save_freshness("dce", total_rows)
        else:
            logger.warning("[Layer 9] DCE returned no data")
    except Exception:
        logger.error("[Layer 9] DCE failed — see error above", exc_info=True)

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
        else:
            logger.info("[Layer 10] Export sales skipped (FAS_API_KEY not set)")
    except Exception:
        logger.error("[Layer 10] Export sales failed — see error above", exc_info=True)

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
    except Exception:
        logger.error("[Layer 11] Forward curves failed — see error above", exc_info=True)

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
                logger.warning("[Layer 12] WASDE returned no data (API key missing?)")
        else:
            logger.info("[Layer 12] WASDE skipped (USDA_API_KEY not set)")
    except Exception:
        logger.error("[Layer 12] WASDE failed — see error above", exc_info=True)

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
        else:
            logger.info("[Layer 13] EIA skipped (EIA_API_KEY not set)")
    except Exception:
        logger.error("[Layer 13] EIA failed — see error above", exc_info=True)

    # ── Layer 14: USDA Crush/Processing + Export Inspections ──────
    try:
        logger.info("[Layer 14] Fetching USDA crush data + export inspections ...")
        total_14 = 0

        # Crush data (same USDA API, stat_category=PROCESSING)
        crush_df = fetch_crush_data()
        if not crush_df.empty:
            save_usda_data(crush_df, "PROCESSING")
            total_14 += len(crush_df)

        # Export inspections (AMS text report)
        insp_df = fetch_export_inspections()
        if not insp_df.empty:
            insp_df = clean_inspections(insp_df)
            for commodity in insp_df["commodity"].unique():
                subset = insp_df[insp_df["commodity"] == commodity]
                save_inspections(commodity, subset)
            total_14 += len(insp_df)

        if total_14 > 0:
            results["crush_inspections"] = True
            save_freshness("crush_inspections", total_14)
        else:
            logger.warning("[Layer 14] Crush/inspections returned no data")
    except Exception:
        logger.error("[Layer 14] Crush/inspections failed — see error above", exc_info=True)

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
    except Exception:
        logger.error("[Layer 15] CONAB failed — see error above", exc_info=True)

    # ── Layer 16: Options Sentiment (experimental) ────────────────
    options_data = {}
    try:
        logger.info("[Layer 16] Fetching options sentiment (experimental) ...")
        options_data = fetch_options_sentiment()

        if options_data:
            for name in options_data:
                options_data[name] = clean_options(options_data[name])

            for name, df in options_data.items():
                save_options_sentiment(name, df)

            if any(not df.empty for df in options_data.values()):
                results["options"] = True
                total_rows = sum(len(df) for df in options_data.values())
                save_freshness("options", total_rows)
            else:
                logger.info("[Layer 16] Options returned no data (expected for ag futures)")
        else:
            logger.info("[Layer 16] Options skipped — no data available")
    except Exception:
        # Extra-defensive for experimental layer
        logger.info("[Layer 16] Options failed (experimental, non-critical): %s", exc_info=True)

    # ── Update per-commodity freshness tracking ─────────────────
    try:
        from processing.combiner import update_commodity_freshness
        update_commodity_freshness()
    except Exception:
        logger.error("Per-commodity freshness update failed", exc_info=True)

    # ── Run data health check ─────────────────────────────────
    try:
        from analysis.health import run_health_check
        health = run_health_check()
        if health["issues"]:
            logger.info("\n%s", health["summary"])
        else:
            logger.info("DATA HEALTH: All systems green")
    except Exception:
        logger.error("Health check failed", exc_info=True)

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


if __name__ == "__main__":
    run()
