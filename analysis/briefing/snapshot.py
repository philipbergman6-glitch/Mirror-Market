"""Snapshot extractor — distills a BriefingData into a JSON-friendly dict.

The snapshot is what gets stored in the `briefings.snapshot_json` column.
It captures the structured numbers behind the briefing so future runs
can backtest which signals preceded large price moves without re-parsing
the formatted text.

Schema v2 (see the approved shape on wayfinder issue #11): a top-level
``schema_version`` int plus one block per quantitative briefing section.
Rows written before v2 lack the ``schema_version`` key — consumers treat
its absence as v1. Design rules carried by the schema:

- raw numbers and components are stored, never display labels, regime
  strings, or derived values whose inputs are already present
- every block is nullable — a failed or empty layer yields None/{} so a
  partial pipeline day still archives everything else
- the whole dict is JSON-native (plain float/int/str/bool/None) so
  ``json.dumps`` never falls back to ``default=str``

Signals are stored separately in `briefings.signals_json` — they already
come pre-typed off `BriefingData.signals`.
"""

from __future__ import annotations

import logging
import math
from collections.abc import Callable
from functools import partial
from typing import Any

import pandas as pd

from analysis.briefing.sections.cot import _LOOKBACK as _COT_LOOKBACK
from analysis.briefing.sections.export_sales import (
    soy_marketing_year,
    wasde_soy_export_forecast,
    year_ago_accumulated,
)
from analysis.briefing.sections.weather import _LOOKBACK as _WEATHER_LOOKBACK
from analysis.briefing.sections.weather import (
    consecutive_dry_days,
    observed_only,
    precip_deficit_30d,
)
from analysis.briefing.types import BriefingData
from analysis.correlations import commodity_correlation_matrix, commodity_vs_currency
from analysis.forward_curve import analyze_curve, curve_slope
from analysis.health import run_health_check
from analysis.nass_crush import latest_crush
from analysis.seasonal import current_vs_seasonal
from analysis.spreads import compute_brazil_basis, compute_crush_spread
from analysis.stocks_to_use import (
    HISTORY_WINDOW,
    MIN_HISTORY_YEARS,
    compute_stocks_to_use,
    detect_tight_supply,
)
from analysis.zscore import trailing_zscore
from pipeline.query import (
    read_brazil_estimates,
    read_brazil_spot,
    read_cot,
    read_crop_progress,
    read_dce_futures,
    read_economic,
    read_eia_data,
    read_export_sales,
    read_forward_curve,
    read_gulf_bids,
    read_inspections,
    read_port_flows,
    read_psd,
    read_usda,
    read_wasde,
    read_weather,
    read_worldbank_prices,
)
from pipeline.units import to_metric_tons

logger = logging.getLogger(__name__)

SCHEMA_VERSION = 2

_BASIS_SOURCES = {
    "paranagua_fob": "Soybean (AgRural Paranaguá FOB)",
    "cepea_parana": "Soybean (CEPEA)",
}

_DCE_TO_CBOT = {
    "DCE Soybean": "Soybeans",
    "DCE Soybean Meal": "Soybean Meal",
    "DCE Soybean Oil": "Soybean Oil",
}

_PSD_HIGHLIGHTS = {
    "brazil_soy_production": ("Soybeans", "Brazil", "Production"),
    "china_soy_imports": ("Soybeans", "China", "Imports"),
    "us_soy_production": ("Soybeans", "United States", "Production"),
    "indonesia_palm_production": ("Palm Oil", "Indonesia", "Production"),
    # Argentina — #1 soymeal/oil exporter.
    "argentina_soy_production": ("Soybeans", "Argentina", "Production"),
    "argentina_meal_exports": ("Soybean Meal", "Argentina", "Exports"),
    "argentina_oil_exports": ("Soybean Oil", "Argentina", "Exports"),
}

_CORRELATION_CURRENCY_PAIRS = (
    ("Soybeans", "BRL/USD"),
)

_TECH_FIELDS = (
    ("close", "Close"),
    ("daily_pct_change", "daily_pct_change"),
    ("weekly_pct_change", "weekly_pct_change"),
    ("rsi", "RSI"),
    ("macd", "MACD"),
    ("macd_signal", "MACD_Signal"),
    ("macd_histogram", "MACD_Histogram"),
    ("ma_20", "MA_20"),
    ("ma_50", "MA_50"),
    ("ma_200", "MA_200"),
    ("bb_upper", "BB_Upper"),
    ("bb_middle", "BB_Middle"),
    ("bb_lower", "BB_Lower"),
    ("bb_width", "BB_Width"),
    ("hv_20", "HV_20"),
    ("hv_60", "HV_60"),
)


def _num(val: Any) -> float | None:
    """Coerce a scalar to a plain float, mapping NaN/None/unparseable to None."""
    if val is None:
        return None
    try:
        f = float(val)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else f


def _date_str(val: Any) -> str | None:
    """Render a date-like value as an ISO string, None if missing/invalid."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    try:
        ts = pd.Timestamp(val)
    except (TypeError, ValueError):
        return None
    if pd.isna(ts):
        return None
    return str(ts.date())


def _jsonify(obj: Any) -> Any:
    """Recursively convert to JSON-native types (numpy scalars, Timestamps)."""
    if isinstance(obj, dict):
        return {str(k): _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonify(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        return str(obj.date())
    if isinstance(obj, (bool, int, str)) or obj is None:
        return obj
    if hasattr(obj, "item"):  # numpy scalar
        return _jsonify(obj.item())
    if isinstance(obj, float):
        return None if math.isnan(obj) else obj
    return str(obj)


def _safe(name: str, builder: Callable[[], Any], default: Any) -> Any:
    """Run a block builder; a failure degrades to `default`, never raises."""
    try:
        return builder()
    except Exception as exc:
        logger.debug("Snapshot block %s failed: %s", name, exc)
        return default


# ---------------------------------------------------------------------------
# Block builders
# ---------------------------------------------------------------------------


def _prices_block(enriched: dict[str, pd.DataFrame]) -> dict[str, dict[str, float | None]]:
    out: dict[str, dict[str, float | None]] = {}
    for commodity, df in enriched.items():
        if df.empty:
            continue
        latest = df.iloc[-1]
        row: dict[str, float | None] = {}
        for out_key, col in _TECH_FIELDS:
            row[out_key] = _num(latest.get(col, None))
        close = row.get("close")
        row["close_usd_mt"] = _num(to_metric_tons(close, commodity)) if close is not None else None
        out[commodity] = row
    return out


def _crush_block(price_data: dict[str, pd.DataFrame]) -> dict[str, float | None] | None:
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    oil = price_data.get("Soybean Oil", pd.DataFrame())
    meal = price_data.get("Soybean Meal", pd.DataFrame())
    if soybeans.empty or oil.empty or meal.empty:
        return None
    spread = compute_crush_spread(soybeans, oil, meal)
    if spread.empty:
        return None
    cents = _num(spread.iloc[-1]["crush_spread"])
    chg_5d = None
    if cents is not None and len(spread) >= 6:
        prev = _num(spread.iloc[-6]["crush_spread"])
        chg_5d = cents - prev if prev is not None else None
    return {
        "value_cents": cents,
        "value_usd_mt": _num(to_metric_tons(cents, "Soybeans")) if cents is not None else None,
        "oil_value_share": _num(spread.iloc[-1]["oil_value_share"]),
        "chg_5d_cents": chg_5d,
    }


def _basis_block(
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    soybeans = price_data.get("Soybeans", pd.DataFrame())
    brl_usd = currency_data.get("BRL/USD", pd.DataFrame())
    if soybeans.empty or brl_usd.empty:
        return None
    out: dict[str, Any] = {}
    for key, source in _BASIS_SOURCES.items():
        def build(source: str = source) -> dict[str, float | None] | None:
            spot = read_brazil_spot(source)
            if spot.empty:
                return None
            basis = compute_brazil_basis(soybeans, spot, brl_usd)
            if basis.empty:
                return None
            latest = basis.iloc[-1]
            return {
                "basis_usd_mt": _num(latest["basis_usd_mt"]),
                "basis_pct": _num(latest["basis_pct"]),
                "spot_usd_mt": _num(latest["cepea_usd_mt"]),
                "cbot_usd_mt": _num(latest["cbot_usd_mt"]),
                "brl_usd": _num(latest["brl_usd"]),
            }

        out[key] = _safe(f"brazil_basis.{key}", build, None)
    agrural = out.get("paranagua_fob") or {}
    cepea = out.get("cepea_parana") or {}
    a, c = agrural.get("basis_usd_mt"), cepea.get("basis_usd_mt")
    out["port_farm_wedge_usd_mt"] = a - c if a is not None and c is not None else None
    if out["paranagua_fob"] is None and out["cepea_parana"] is None:
        return None
    return out


def _economic_block() -> dict[str, dict[str, Any]]:
    econ = read_economic()
    if econ.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for series, subset in econ.groupby("series_name"):
        subset = subset.sort_values("Date")
        latest = subset.iloc[-1]
        value = _num(latest["value"])
        chg = chg_pct = None
        if value is not None and len(subset) >= 2:
            prev = _num(subset.iloc[-2]["value"])
            if prev is not None:
                chg = value - prev
                chg_pct = chg / prev * 100 if prev != 0 else None
        out[str(series)] = {
            "date": _date_str(latest["Date"]),
            "value": value,
            "chg": chg,
            "chg_pct": chg_pct,
        }
    return out


def _yield_curve_block(economic: dict[str, dict[str, Any]]) -> dict[str, float | None] | None:
    t2y = (economic.get("Treasury 2Y") or {}).get("value")
    t10y = (economic.get("Treasury 10Y") or {}).get("value")
    if t2y is None and t10y is None:
        return None
    spread = t10y - t2y if t2y is not None and t10y is not None else None
    return {"t2y": t2y, "t10y": t10y, "spread": spread}


def _usda_block() -> dict[str, dict[str, Any]]:
    usda = read_usda()
    if usda.empty or "year" not in usda.columns:
        return {}
    usda = usda.copy()
    # Monthly CRUSHED rows live in the nass_crush block, not the annual table.
    if "stat_category" in usda.columns:
        usda = usda[usda["stat_category"] != "CRUSHED"]
    usda["year_int"] = pd.to_numeric(usda["year"], errors="coerce")
    years = sorted(usda["year_int"].dropna().unique())
    if not years:
        return {}
    latest_year = years[-1]
    prev_year = years[-2] if len(years) >= 2 else None
    out: dict[str, dict[str, Any]] = {}
    for _, row in usda[usda["year_int"] == latest_year].iterrows():
        desc = str(row["short_desc"])
        value = _num(str(row["Value"]).replace(",", ""))
        if value is None:
            continue
        yoy_pct = None
        if prev_year is not None:
            prev_rows = usda[(usda["year_int"] == prev_year) & (usda["short_desc"] == row["short_desc"])]
            if not prev_rows.empty:
                prev_val = _num(str(prev_rows.iloc[0]["Value"]).replace(",", ""))
                if prev_val:
                    yoy_pct = (value - prev_val) / prev_val * 100
        out[desc] = {
            "value": value,
            "unit": str(row.get("unit_desc", "")) or None,
            "year": int(latest_year),
            "yoy_pct": yoy_pct,
        }
    return out


def _crop_progress_block() -> dict[str, dict[str, Any]]:
    progress = read_crop_progress()
    if progress.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in progress.groupby("commodity"):
        entry: dict[str, Any] = {}
        for key, category in (("condition", "CONDITION"), ("progress", "PROGRESS")):
            rows = subset[subset["stat_category"] == category]
            rows = rows[rows["Value"].notna()]
            if rows.empty:
                entry[key] = None
                continue
            week = rows["week_ending"].max()
            latest = rows[rows["week_ending"] == week]
            values = {
                str(r["short_desc"]): _num(str(r["Value"]).replace(",", ""))
                for _, r in latest.iterrows()
            }
            entry[key] = {"week_ending": _date_str(week), "percent": values}
        if entry.get("condition") or entry.get("progress"):
            out[str(commodity)] = entry
    return out


def _wasde_block() -> dict[str, dict[str, Any]]:
    wasde = read_wasde()
    if wasde.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for (commodity, attribute), subset in wasde.groupby(["commodity", "attribute"]):
        latest_my = subset["year"].max()
        rows = subset[subset["year"] == latest_my].sort_values("reference_period")
        if rows.empty:
            continue
        latest = rows.iloc[-1]
        value = _num(latest["value"])
        if value is None:
            continue
        revision = None
        if len(rows) >= 2:
            prev = _num(rows.iloc[-2]["value"])
            revision = value - prev if prev is not None else None
        out.setdefault(str(commodity), {})[str(attribute)] = {
            "value": value,
            "unit": str(latest.get("unit", "")) or None,
            # WASDE marketing years are strings like "2025/26" — keep verbatim.
            "marketing_year": str(latest_my) if pd.notna(latest_my) else None,
            "reference_period": str(latest.get("reference_period", "")) or None,
            "mom_revision": revision,
        }
    return out


def _stocks_to_use_block() -> dict[str, dict[str, Any]]:
    psd = read_psd()
    if psd.empty:
        return {}
    stu = compute_stocks_to_use(psd, country="United States")
    if stu.empty:
        return {}
    tight = {s["commodity"] for s in detect_tight_supply(stu)}
    out: dict[str, dict[str, Any]] = {}
    for commodity, rows in stu.groupby("commodity"):
        rows = rows.sort_values("year")
        current = rows.iloc[-1]
        history = rows.iloc[-(1 + HISTORY_WINDOW):-1]
        prior_low = prior_high = None
        if len(history) >= MIN_HISTORY_YEARS:
            prior_low = _num(history["ratio"].min())
            prior_high = _num(history["ratio"].max())
        out[str(commodity)] = {
            "ratio": _num(current["ratio"]),
            "ending_stocks": _num(current["ending_stocks"]),
            "total_use": _num(current["total_use"]),
            "marketing_year": int(current["year"]) if pd.notna(current["year"]) else None,
            "prior_5y_low": prior_low,
            "prior_5y_high": prior_high,
            "tight": str(commodity) in tight,
        }
    return out


def _export_sales_block() -> dict[str, dict[str, Any]]:
    sales = read_export_sales()
    if sales.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in sales.groupby("commodity"):
        week = subset["week_ending"].max()
        week_data = subset[subset["week_ending"] == week]
        total_net = _num(week_data["net_sales"].sum())
        china = week_data[week_data["country"].astype(str).str.contains("china", case=False)]
        china_net = _num(china["net_sales"].sum()) if not china.empty else 0.0
        china_pct = None
        if total_net not in (None, 0) and china_net is not None:
            china_pct = china_net / total_net * 100
        buyers = week_data[week_data["net_sales"].notna()].nlargest(3, "net_sales")
        entry: dict[str, Any] = {
            "week_ending": _date_str(week),
            "total_net_sales_mt": total_net,
            "total_exports_mt": _num(week_data["weekly_exports"].sum()),
            "outstanding_sales_mt": _num(week_data["outstanding_sales"].sum()),
            "accumulated_exports_mt": _num(week_data["accumulated_exports"].sum()),
            "china_net_sales_mt": china_net,
            "china_pct": china_pct,
            "top_buyers": [
                {"country": str(r["country"]), "net_sales_mt": _num(r["net_sales"])}
                for _, r in buyers.iterrows()
            ],
        }
        if str(commodity) == "Soybeans":
            entry.update(
                _safe("export_sales.pace", partial(_soy_pace_fields, subset, week), {})
            )
        out[str(commodity)] = entry
    return out


def _soy_pace_fields(subset: pd.DataFrame, week: Any) -> dict[str, Any]:
    """Raw pace inputs for soybeans — commitments % stays derived downstream."""
    week_ts = pd.Timestamp(week)
    my = soy_marketing_year(week_ts)
    forecast = wasde_soy_export_forecast(my)
    yr_ago = year_ago_accumulated(subset, week_ts)
    return {
        "marketing_year": my,
        "wasde_export_forecast_mt": _num(forecast["value_mt"]),
        "wasde_export_forecast_raw": _num(forecast["raw_value"]),
        "wasde_export_forecast_unit": forecast["unit"],
        "yr_ago_week_ending": _date_str(yr_ago["week_ending"]),
        "yr_ago_accumulated_exports_mt": _num(yr_ago["accumulated_mt"]),
    }


def _gulf_bids_block() -> dict[str, Any]:
    bids = read_gulf_bids("Soybeans")
    if bids.empty:
        return {}
    latest_date = bids["report_date"].max()
    latest = bids[bids["report_date"] == latest_date]
    return {
        "report_date": str(latest_date.date()),
        "deliveries": {
            str(row["delivery"]): {
                "basis_low_cents_bu": _num(row["basis_low"]),
                "basis_high_cents_bu": _num(row["basis_high"]),
                "futures_month": int(row["futures_month"]),
                "avg_price_usd_bu": _num(row["average"]),
                "year_ago_usd_bu": _num(row["year_ago"]),
            }
            for _, row in latest.iterrows()
        },
    }


def _port_flows_block() -> dict[str, dict[str, Any]]:
    flows = read_port_flows()
    if flows.empty or "port_area" not in flows.columns:
        return {}
    subtotals = flows[flows["port_area"] == "SUBTOTAL"]
    if subtotals.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in subtotals.groupby("commodity"):
        weeks = sorted(subset["week_ending"].dropna().unique())
        if not weeks:
            continue

        def regions_mt(week: Any, *, subset: pd.DataFrame = subset) -> dict[str, float | None]:
            rows = subset[subset["week_ending"] == week]
            return {
                str(region): _num(rows_r["inspections_mt"].sum())
                for region, rows_r in rows.groupby("region")
            }

        latest, prev = weeks[-1], weeks[-2] if len(weeks) >= 2 else None
        out[str(commodity)] = {
            "week_ending": _date_str(latest),
            "regions_mt": regions_mt(latest),
            "prev_week_ending": _date_str(prev) if prev is not None else None,
            "prev_regions_mt": regions_mt(prev) if prev is not None else None,
        }
    return out


def _nass_crush_block() -> dict[str, Any] | None:
    crush = latest_crush()
    if not crush:
        return None
    return {
        "value": _num(crush["value"]),
        "unit": crush["unit"] or None,
        "year": crush["year"],
        "month": crush["month"],
        "yoy_pct": _num(crush["yoy_pct"]),
    }


def _inspections_block() -> dict[str, dict[str, Any]]:
    inspections = read_inspections()
    if inspections.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in inspections.groupby("commodity"):
        latest = subset.sort_values("week_ending").iloc[-1]
        out[str(commodity)] = {
            "week_ending": _date_str(latest["week_ending"]),
            "inspections_mt": _num(latest["inspections_mt"]),
        }
    return out


def _dce_block(price_data: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    dce = read_dce_futures()
    if dce.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in dce.groupby("commodity"):
        latest = subset.sort_values("Date").iloc[-1]
        cbot_name = _DCE_TO_CBOT.get(str(commodity))
        cbot_close = None
        if cbot_name:
            cbot_df = price_data.get(cbot_name, pd.DataFrame())
            if not cbot_df.empty:
                cbot_close = _num(cbot_df.iloc[-1].get("Close"))
        out[str(commodity)] = {
            "date": _date_str(latest["Date"]),
            "close_cny": _num(latest["Close"]),
            "cbot_commodity": cbot_name,
            "cbot_close": cbot_close,
        }
    return out


def _forward_curve_block() -> dict[str, dict[str, Any]]:
    curve = read_forward_curve()
    if curve.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in curve.groupby("commodity"):
        if len(subset) < 2:
            continue
        result = analyze_curve(subset)
        if not result:
            continue
        out[str(commodity)] = {
            "structure": result.get("structure"),
            "front_price": _num(result.get("front_price")),
            "back_price": _num(result.get("back_price")),
            "spread": _num(result.get("spread")),
            "spread_pct": _num(result.get("spread_pct")),
            "num_contracts": int(result["num_contracts"]) if result.get("num_contracts") else None,
            "slope_per_month": _num(curve_slope(subset)),
        }
    return out


def _eia_block() -> dict[str, dict[str, Any]]:
    eia = read_eia_data()
    if eia.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for series, subset in eia.groupby("series_name"):
        subset = subset.sort_values("Date")
        latest = subset.iloc[-1]
        value = _num(latest["value"])
        chg_pct = None
        if value is not None and len(subset) >= 2:
            prev = _num(subset.iloc[-2]["value"])
            if prev:
                chg_pct = (value - prev) / prev * 100
        out[str(series)] = {
            "date": _date_str(latest["Date"]),
            "value": value,
            "unit": str(latest.get("unit", "")) or None,
            "chg_pct": chg_pct,
        }
    return out


def _conab_block() -> dict[str, dict[str, Any]]:
    conab = read_brazil_estimates()
    if conab.empty:
        return {}
    psd = _safe("conab.psd", read_psd, pd.DataFrame())
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in conab.groupby("commodity"):
        crop_year = subset["crop_year"].max()
        rows = subset[subset["crop_year"] == crop_year]
        attributes = {
            str(r["attribute"]): {"value": _num(r["value"]), "unit": str(r.get("unit", "")) or None}
            for _, r in rows.iterrows()
            if _num(r["value"]) is not None
        }
        usda_production = None
        if not psd.empty:
            match = psd[
                (psd["commodity"] == commodity)
                & (psd["country"] == "Brazil")
                & (psd["attribute"] == "Production")
            ]
            if not match.empty:
                latest = match[match["year"] == match["year"].max()].iloc[0]
                usda_production = {
                    "value": _num(latest["value"]),
                    "unit": str(latest.get("unit", "")) or None,
                    "year": int(latest["year"]) if pd.notna(latest["year"]) else None,
                }
        out[str(commodity)] = {
            "crop_year": str(crop_year) if pd.notna(crop_year) else None,
            "attributes": attributes,
            "usda_production": usda_production,
        }
    return out


def _currencies_block(currency_data: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for pair, df in currency_data.items():
        if df.empty or "Close" not in df.columns:
            continue
        close = _num(df["Close"].iloc[-1])
        if close is None:
            continue

        def pct_back(
            sessions: int, *, df: pd.DataFrame = df, close: float = close
        ) -> float | None:
            if len(df) <= sessions:
                return None
            base = _num(df["Close"].iloc[-(sessions + 1)])
            if base in (None, 0):
                return None
            return (close - base) / base * 100

        out[str(pair)] = {
            "close": close,
            "chg_5d_pct": pct_back(5),
            "chg_21d_pct": pct_back(21),
        }
    return out


def _cot_block() -> dict[str, dict[str, Any]]:
    cot = read_cot()
    if cot.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in cot.groupby("commodity"):
        subset = subset.sort_values("Date")
        latest = subset.iloc[-1]
        latest_date = latest["Date"]
        oi = latest.get("total_open_interest")
        out[str(commodity)] = {
            "date": _date_str(latest_date),
            "commercial_net": _num(latest.get("commercial_net")),
            "noncommercial_net": _num(latest.get("noncommercial_net")),
            "total_open_interest": _num(oi),
            "commercial_z3y": trailing_zscore(subset, "commercial_net", latest_date, _COT_LOOKBACK),
            "noncommercial_z3y": trailing_zscore(
                subset, "noncommercial_net", latest_date, _COT_LOOKBACK
            ),
        }
    return out


def _weather_block() -> dict[str, dict[str, Any]]:
    weather = read_weather()
    if weather.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for region, subset in weather.groupby("region"):
        subset = subset.sort_values("Date")
        # Forecast rows are model output, not readings — latest values,
        # z-scores, and agronomic aggregates all run on observed rows only
        # (NULL is_forecast = observed, pre-flag data).
        observed = observed_only(subset)
        if observed.empty:
            continue
        latest = observed.iloc[-1]
        latest_date = latest["Date"]
        total_30d, deficit_pct = precip_deficit_30d(observed)
        out[str(region)] = {
            "date": _date_str(latest_date),
            "precipitation_mm": _num(latest.get("precipitation")),
            "temp_max_c": _num(latest.get("temp_max")),
            "temp_min_c": _num(latest.get("temp_min")),
            "precip_z90d": trailing_zscore(
                observed, "precipitation", latest_date, _WEATHER_LOOKBACK
            ),
            "temp_max_z90d": trailing_zscore(observed, "temp_max", latest_date, _WEATHER_LOOKBACK),
            "consecutive_dry_days": consecutive_dry_days(observed),
            "precip_30d_mm": _num(total_30d),
            "precip_30d_deficit_pct": _num(deficit_pct),
            "forecast_days": int(len(subset) - len(observed)),
        }
    return out


def _psd_highlights_block() -> dict[str, dict[str, Any] | None]:
    psd = read_psd()
    if psd.empty:
        return {}
    out: dict[str, dict[str, Any] | None] = {}
    for key, (commodity, country, attribute) in _PSD_HIGHLIGHTS.items():
        match = psd[
            (psd["commodity"] == commodity)
            & (psd["country"] == country)
            & (psd["attribute"] == attribute)
        ]
        if match.empty:
            out[key] = None
            continue
        latest = match[match["year"] == match["year"].max()].iloc[0]
        out[key] = {
            "value": _num(latest["value"]),
            "unit": str(latest.get("unit", "")) or "1000 MT",
            "year": int(latest["year"]) if pd.notna(latest["year"]) else None,
        }
    return out


def _worldbank_block() -> dict[str, dict[str, Any]]:
    wb = read_worldbank_prices()
    if wb.empty:
        return {}
    out: dict[str, dict[str, Any]] = {}
    for commodity, subset in wb.groupby("commodity"):
        subset = subset.sort_values("Date")
        latest = subset.iloc[-1]
        price = _num(latest["price"])
        mom_pct = None
        if price is not None and len(subset) >= 2:
            prev = _num(subset.iloc[-2]["price"])
            if prev:
                mom_pct = (price - prev) / prev * 100
        out[str(commodity)] = {
            "date": _date_str(latest["Date"]),
            "price": price,
            "unit": str(latest.get("unit", "")) or None,
            "mom_pct": mom_pct,
        }
    return out


def _emerging_markets_block() -> dict[str, Any] | None:
    # Lazy import mirrors the section — soy_analytics pulls in the whole
    # loader stack and must not become an import-time dependency here.
    from analysis.soy_analytics import emerging_markets_analysis

    data = emerging_markets_analysis()
    return data or None


def _correlations_block(
    price_data: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> dict[str, Any] | None:
    matrix = commodity_correlation_matrix(price_data)
    matrix_out: dict[str, dict[str, float | None]] = {}
    if not matrix.empty:
        for a in matrix.columns:
            for b in matrix.columns:
                if a >= b:
                    continue
                matrix_out.setdefault(str(a), {})[str(b)] = _num(matrix.loc[a, b])
    vs_currency: dict[str, float | None] = {}
    for commodity, pair in _CORRELATION_CURRENCY_PAIRS:
        price_df = price_data.get(commodity, pd.DataFrame())
        fx_df = currency_data.get(pair, pd.DataFrame())
        if price_df.empty or fx_df.empty:
            vs_currency[f"{commodity}|{pair}"] = None
            continue
        vs_currency[f"{commodity}|{pair}"] = _num(
            commodity_vs_currency(price_df, fx_df, commodity, pair)
        )
    if not matrix_out and not any(v is not None for v in vs_currency.values()):
        return None
    return {"matrix": matrix_out, "vs_currency": vs_currency}


def _seasonal_block(price_data: dict[str, pd.DataFrame]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for commodity, df in price_data.items():
        result = _safe(f"seasonal.{commodity}", partial(current_vs_seasonal, df), {})
        if not result:
            continue
        out[str(commodity)] = {
            "current_price": _num(result.get("current_price")),
            "seasonal_avg": _num(result.get("seasonal_avg")),
            "deviation_pct": _num(result.get("deviation_pct")),
            "n_years": int(result["n_years"]) if result.get("n_years") else None,
            "current_dev_pct": _num(result.get("current_dev_pct")),
            "seasonal_dev_pct": _num(result.get("seasonal_dev_pct")),
            "detrended_delta_pct": _num(result.get("detrended_delta_pct")),
        }
    return out


def _health_block() -> dict[str, Any] | None:
    health = run_health_check()
    if not health:
        return None
    issues = health.get("issues", [])
    return {
        "critical_count": sum(1 for i in issues if i.get("severity") == "critical"),
        "warning_count": sum(1 for i in issues if i.get("severity") == "warning"),
        "commodity_status": health.get("commodity_status", []),
    }


def build_snapshot(data: BriefingData) -> dict[str, Any]:
    """Distill a BriefingData into the dict written to briefings.snapshot_json."""
    economic = _safe("economic", _economic_block, {})
    snapshot: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "prices": _safe("prices", lambda: _prices_block(data.enriched), {}),
        "crush_spread": _safe("crush_spread", lambda: _crush_block(data.price_data), None),
        "brazil_basis": _safe(
            "brazil_basis", lambda: _basis_block(data.price_data, data.currency_data), None
        ),
        "economic": economic,
        "yield_curve": _safe("yield_curve", lambda: _yield_curve_block(economic), None),
        "usda": _safe("usda", _usda_block, {}),
        "crop_progress": _safe("crop_progress", _crop_progress_block, {}),
        "wasde": _safe("wasde", _wasde_block, {}),
        "stocks_to_use": _safe("stocks_to_use", _stocks_to_use_block, {}),
        "export_sales": _safe("export_sales", _export_sales_block, {}),
        "inspections": _safe("inspections", _inspections_block, {}),
        "port_flows": _safe("port_flows", _port_flows_block, {}),
        "gulf_bids": _safe("gulf_bids", _gulf_bids_block, {}),
        "nass_crush": _safe("nass_crush", _nass_crush_block, None),
        "dce": _safe("dce", lambda: _dce_block(data.price_data), {}),
        "forward_curve": _safe("forward_curve", _forward_curve_block, {}),
        "eia": _safe("eia", _eia_block, {}),
        "conab": _safe("conab", _conab_block, {}),
        "currencies": _safe("currencies", lambda: _currencies_block(data.currency_data), {}),
        "cot": _safe("cot", _cot_block, {}),
        "weather": _safe("weather", _weather_block, {}),
        "psd_highlights": _safe("psd_highlights", _psd_highlights_block, {}),
        "worldbank": _safe("worldbank", _worldbank_block, {}),
        "emerging_markets": _safe("emerging_markets", _emerging_markets_block, None),
        "correlations": _safe(
            "correlations",
            lambda: _correlations_block(data.price_data, data.currency_data),
            None,
        ),
        "seasonal": _safe("seasonal", lambda: _seasonal_block(data.price_data), {}),
        "health": _safe("health", _health_block, None),
    }
    return _jsonify(snapshot)
