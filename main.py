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

import json
import logging
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from config import (
    LAYER_MAX_DATA_AGE_DAYS,
    LAYER_MIN_KEYS,
    MAX_FAILED_LAYERS,
    layer_expected_keys,
    setup_logging,
)
from fetchers.agrural import fetch_agrural
from fetchers.akshare import fetch_dce_futures
from fetchers.conab import fetch_conab_estimates
from fetchers.conab_precos import fetch_conab_farmgate
from fetchers.cot import fetch_cot_recent
from fetchers.eia import fetch_all_eia
from fetchers.eia import is_configured as eia_configured
from fetchers.export_sales import fetch_all_export_sales
from fetchers.export_sales import is_configured as export_sales_configured
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
    save_argentina_fob,
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


def _mark_failed(
    layer: str,
    rows_fetched: int = 0,
    keys_returned: int | None = None,
    keys_expected: int | None = None,
) -> None:
    """Best-effort 'failed' freshness row — never crashes the pipeline itself.

    Callers that already computed the run's counts pass them through, so a
    partial outage records "3 of 10 keys" instead of a fabricated zero
    (#182). Callers with no payload at all — the transport-exception paths —
    leave the key counts None, which records NULL: never learned, as opposed
    to asked-and-got-nothing.
    """
    _HARD_FAILURES.add(layer)
    try:
        save_freshness(
            layer, rows_fetched=rows_fetched, status="failed",
            keys_returned=keys_returned, keys_expected=keys_expected,
        )
    except Exception:
        logger.exception("Could not record failed-freshness row for %s", layer)


def _mark_empty(layer: str) -> None:
    """Record a successful run that returned zero rows.

    Distinct from _mark_failed: the layer ran to completion and the upstream
    legitimately had nothing to publish (no inspection report this week, no
    matching contracts traded, etc). Without this, an empty result is
    indistinguishable from "the layer never ran" on the dashboard.

    Zero rows is the truth here, so rows_fetched=0 stands; key coverage is
    left NULL because the layers that reach this path have no key catalog
    to be partial against (#182).
    """
    try:
        save_freshness(layer, rows_fetched=0, status="success")
    except Exception:
        logger.exception("Could not record empty-success freshness row for %s", layer)


def _mark_stale(
    layer: str,
    latest: pd.Timestamp,
    age_days: int,
    budget: int,
    rows_fetched: int = 0,
    keys_returned: int | None = None,
    keys_expected: int | None = None,
) -> None:
    """Record a layer that fetched fine but delivered stale data (audit F3).

    Recorded as 'failed' rather than 'success' so last_success is preserved
    and stops advancing: the layer then ages out of its freshness window on
    its own, and every surface that already reads freshness reports it stale
    without needing to know about this check.

    Counted as a hard failure — a frozen upstream is an outage, just a quiet
    one, and the CI alerter should raise it like any other.
    """
    _HARD_FAILURES.add(layer)
    logger.error(
        "[%s] fetched OK but newest observation is %s (%d days old, budget %d) "
        "— upstream looks frozen; recording as failed rather than stamping "
        "a fresh last_success",
        layer, latest.strftime("%Y-%m-%d"), age_days, budget,
    )
    try:
        save_freshness(
            layer, rows_fetched=rows_fetched, status="failed",
            keys_returned=keys_returned, keys_expected=keys_expected,
        )
    except Exception:
        logger.exception("Could not record stale-freshness row for %s", layer)


# Column names that carry the observation date across the layers. The frames
# reaching _finalize_layer are post-clean, so these are the cleaners' own
# conventions (pipeline/clean.py), not the raw upstream ones.
_DATE_COLUMNS = ("Date", "date", "week_ending", "report_date")


def _latest_observation_date(data: dict) -> pd.Timestamp | None:
    """Newest observation date across a layer's frames, or None if undatable.

    Handles every shape the layers actually produce:
      - DataFrame with a DatetimeIndex   (prices, currencies — clean_ohlcv)
      - Series with a DatetimeIndex      (fred — clean_fred_series)
      - DataFrame with a date column     (cot, weather, dce, worldbank, eia)
      - DataFrame with 'week_ending'     (export_sales)

    Returns None when nothing datable is present. Callers must treat that as
    "cannot certify recency", never as "recent".
    """
    candidates: list[pd.Timestamp] = []

    for frame in data.values():
        if frame is None or frame.empty:
            continue

        if isinstance(frame.index, pd.DatetimeIndex):
            candidates.append(frame.index.max())
            continue

        columns = getattr(frame, "columns", None)
        if columns is None:  # a Series without a DatetimeIndex — undatable
            continue

        for col in _DATE_COLUMNS:
            if col in columns:
                values = pd.to_datetime(frame[col], errors="coerce")
                if values.notna().any():
                    candidates.append(values.max())
                break

    if not candidates:
        return None
    # tz-aware and naive timestamps cannot be compared; normalise to naive
    # UTC, which is what the rest of the freshness path uses.
    normalised = [
        ts.tz_convert("UTC").tz_localize(None) if ts.tz is not None else ts
        for ts in candidates
    ]
    return max(normalised)


def _check_layer_recency(
    layer: str,
    data: dict,
    rows_fetched: int = 0,
    keys_returned: int | None = None,
    keys_expected: int | None = None,
) -> bool:
    """True if the layer's data is recent enough for 'success' to be honest.

    Layers absent from LAYER_MAX_DATA_AGE_DAYS are not checked and always
    pass — see the config block for why each exclusion is deliberate.

    The counts are pass-through: both failure paths here run against a real
    payload, so they record what it contained rather than defaulting to zero.
    """
    budget = LAYER_MAX_DATA_AGE_DAYS.get(layer)
    if budget is None:
        return True

    latest = _latest_observation_date(data)
    if latest is None:
        # The layer is configured for a recency check but produced nothing
        # datable. That is a shape change, not a clean bill of health, so it
        # must not stamp a fresh last_success.
        logger.error(
            "[%s] is configured for a recency check but no observation date "
            "could be found in its frames — treating as unverifiable, not fresh",
            layer,
        )
        _mark_failed(layer, rows_fetched, keys_returned, keys_expected)
        return False

    age_days = (pd.Timestamp.now().normalize() - latest.normalize()).days
    if age_days > budget:
        _mark_stale(
            layer, latest, age_days, budget,
            rows_fetched=rows_fetched,
            keys_returned=keys_returned,
            keys_expected=keys_expected,
        )
        return False
    return True


def _mark_disabled(layer: str) -> None:
    """Record an intentionally disabled layer without fabricating freshness.

    Unlike _mark_empty this does NOT stamp last_success, so a hard-disabled
    layer (anti-bot wall) never reads as freshly successful on the dashboard.
    """
    try:
        save_freshness(layer, rows_fetched=0, status="disabled")
    except Exception:
        logger.exception("Could not record disabled freshness row for %s", layer)


def _write_pipeline_status(payload: dict) -> None:
    """Write the run summary JSON next to the DB (data/storage/ — gitignored).

    Resolves STORAGE_DIR at call time so tests that monkeypatch config see
    their tmp_path. A write failure propagates: better a non-zero exit than
    a CI alerter reading yesterday's status.
    """
    import config as _config

    path = Path(_config.STORAGE_DIR) / "pipeline_status.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    logger.info("Pipeline status written to: %s", path)


def _empty_is_failure(layer: str, empty_fails: bool | None) -> bool:
    """Can this layer's zero-row result only mean the fetch broke? (F3b, #175)

    Derived from the LAYER_MIN_KEYS floor rather than a second hand-kept
    list: a floor of 2+ says the layer has that many independent keys, so
    zero of them is an outage by construction. Without this, severity
    inverts — 1 of 10 keys is below floor and records failed, while 0 of 10
    records an empty-*success* that stamps a fresh last_success.

    empty_fails overrides in both directions for layers with no floor to
    derive from: True for a single-key source whose upstream always has
    history (worldbank, wasde), False for one that legitimately publishes
    nothing sometimes. None (the default) derives.
    """
    if empty_fails is not None:
        return empty_fails
    return LAYER_MIN_KEYS.get(layer, 1) >= 2


def _finalize_layer(layer: str, data: dict, empty_fails: bool | None = None) -> bool:
    """Record freshness for a dict-of-frames layer; return overall success.

    Three gates stand between a fetch and a stamped last_success:

    1. Shape — the per-layer expected-count floor from LAYER_MIN_KEYS. A
       layer where only 1 of 13 keys returned data is an outage, not a
       success, so below-floor runs are recorded as failed freshness (which
       preserves last_success for staleness display). All-empty is recorded
       as empty-success only for layers that can legitimately publish
       nothing — see _empty_is_failure; critical layers never can.
    2. Recency — LAYER_MAX_DATA_AGE_DAYS (audit F3). Rows arriving is not
       the same as *new* rows arriving; a frozen upstream clears gate 1
       every day forever.
    3. Only then does the run count as a success.
    """
    non_empty = sum(1 for v in data.values() if not v.empty)
    total_rows = sum(len(v) for v in data.values())
    floor = LAYER_MIN_KEYS.get(layer, 1)
    # keys_returned is the non-empty-key count — the same quantity the floor
    # is graded against — so displayed coverage can never contradict the
    # verdict it explains (#182).
    expected = layer_expected_keys(layer)
    # A layer with no catalog records NULL coverage rather than "n of
    # unknown" — 1/1 on a single-key layer is noise both surfaces would
    # then have to filter back out.
    returned = non_empty if expected is not None else None

    if non_empty == 0:
        logger.warning("[%s] returned no data", layer)
        if layer in CRITICAL_LAYERS or _empty_is_failure(layer, empty_fails):
            _mark_failed(layer, total_rows, returned, expected)
        else:
            _mark_empty(layer)
        return False
    if non_empty < floor:
        logger.warning(
            "[%s] partial: only %d/%d keys returned data (floor %d) — recording as failed",
            layer, non_empty, expected or len(data), floor,
        )
        _mark_failed(layer, total_rows, returned, expected)
        return False
    if not _check_layer_recency(layer, data, total_rows, returned, expected):
        return False
    save_freshness(
        layer, total_rows, keys_returned=returned, keys_expected=expected,
    )
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
    # API-key-gated layers (F3c, #180). run_if() is consulted *before*
    # fetch(); when it returns False the layer is logged with skip_msg and
    # no freshness row is written, matching "the layer never ran". The two
    # fields always travel together — test_layer_run_if.py pins that.
    #
    # The question has to be asked of the *config*, ahead of the fetch,
    # never inferred from the result: a fetcher's empty return means both
    # "no key, never ran" and "key set, upstream had nothing", and only the
    # second is an outage. Reading it off the result made those two
    # indistinguishable, so a dead upstream could record no status at all —
    # nothing for scripts/ci_layer_alert.py or the dashboard to see. Asked
    # this way, a configured layer always reaches _finalize_layer and its
    # empty result is graded under the #175 rules.
    run_if: Callable[[], bool] | None = None
    skip_msg: str | None = None
    # Override for _empty_is_failure's LAYER_MIN_KEYS-derived default (#175).
    # None derives; set it only for a layer with no floor to derive from.
    # True: World Bank returns {} on both download failure and its own
    # stale-file guard, and WASDE workbooks always carry 12+ months —
    # neither empty is "upstream had nothing to publish this month".
    empty_fails: bool | None = None


def _run_dict_layer(layer: DictLayer) -> bool:
    try:
        if layer.run_if is not None and not layer.run_if():
            logger.info("[%s] %s", layer.label, layer.skip_msg)
            return False

        logger.info("[%s] Fetching %s ...", layer.label, layer.desc)
        data = layer.fetch()

        if layer.clean is not None and data:
            logger.info("[Cleaning] Processing %s data ...", layer.key)
            for name in data:
                data[name] = layer.clean(name, data[name])

        for name, df in data.items():
            layer.save(name, df)

        return _finalize_layer(layer.key, data, empty_fails=layer.empty_fails)
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


def _build_dict_layers() -> list[DictLayer]:
    """The Layer 1-13 table: uniform dict-of-frames layers.

    Module-level rather than inline in run() so tests can inspect the
    wiring — DictLayer flags like empty_fails change behaviour and are
    otherwise only reachable by executing the whole pipeline.

    Every fetch/save/clean is a lambda, so main.<fetcher> is resolved
    from module globals at call time and monkeypatching still works
    regardless of when this table is built.
    """
    return [
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
            empty_fails=True,
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
            run_if=export_sales_configured,
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
            # No LAYER_MIN_KEYS floor to derive from, and every monthly
            # workbook carries 12+ months — empty means the download or
            # parse broke, never "nothing published".
            empty_fails=True,
        ),
        DictLayer(
            "eia", "Layer 13", "EIA energy/biofuel data",
            fetch=lambda: fetch_all_eia(),
            save=lambda n, d: save_eia_data(n, d),
            clean=lambda n, d: clean_eia(d),
            run_if=eia_configured,
            skip_msg="EIA skipped (EIA_API_KEY not set)",
        ),
    ]


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
    dict_layers = _build_dict_layers()
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
            # CONAB's file carries the whole survey history every fetch, so
            # an empty national frame means the download or the UF
            # aggregation broke — not "no estimate published" (#175).
            logger.error("[Layer 15] CONAB returned no data — recording as failed")
            _mark_failed("conab")
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

    # ── Machine-readable run summary ─────────────────────────────
    # Consumed by scripts/ci_layer_alert.py in CI: hard failures there
    # open/update a GitHub issue instead of vanishing into the logs.
    critical_failures = [name for name in CRITICAL_LAYERS if not results.get(name)]
    _write_pipeline_status({
        "succeeded": succeeded,
        "failed": failed,
        "disabled": disabled,
        "hard_failures": sorted(_HARD_FAILURES - DISABLED_LAYERS),
        "critical_failures": critical_failures,
    })

    # ── Exit code ────────────────────────────────────────────────
    # Non-critical layer failures are logged but do not fail the run.
    # If a critical layer (prices or FRED economic) failed, exit 1 so
    # CI/deploy workflows can react.
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
