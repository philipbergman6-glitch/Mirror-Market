"""MARKET DRIVERS section — cross-data narrative.

This is the most cross-cutting section: it reads several DB tables
directly and also relies on the `enriched` price frames (with technical
indicators applied) produced by the prices section.

Every driver is three separable parts (#404, invariant 2 in prose form):

- **observation** — the number, dated. Printed whenever the inputs exist.
- **interpretation** — labelled ``Reads as:``; or, when the inputs do not
  establish one, ``Not interpreted:`` with the reason. Never both.
- **falsifier** — labelled ``Confirm / refute:``; what would confirm or
  invalidate the reading, so the desk knows which print to watch.

A rule that cannot be assessed withholds the interpretation with a reason
rather than inferring one from a correlate (China *share* of one week is
not a *pace*; a hog price move is not a herd; biodiesel is not renewable
diesel; a price move beside a weather alert is not a weather premium; a
net long beside a high RSI is not crowded until it is a historical extreme).
"""

from __future__ import annotations

from dataclasses import dataclass
from math import isfinite

import pandas as pd

from analysis.briefing.sections.export_sales import (
    MT_PER_MILLION_BUSHELS_SOY,
    soy_marketing_year,
)
from analysis.forward_curve import analyze_curve
from analysis.weather_alerts import (
    RULE_HEAT,
    RULE_HEAVY_RAIN,
    RULE_POD_FILL_HEAT,
    assess_region,
)
from config import (
    CHINA_PACE_LOOKBACK_WEEKS,
    CHINA_PACE_MIN_WEEKS,
    COT_CROWDED_LOOKBACK_DAYS,
    COT_CROWDED_MIN_OBSERVATIONS,
    COT_CROWDED_PERCENTILE,
    RSI_OVERBOUGHT,
    RSI_OVERSOLD,
)
from pipeline.query import (
    read_brazil_estimates,
    read_cot,
    read_dce_futures,
    read_economic,
    read_eia_data,
    read_export_sales,
    read_forward_curve,
    read_psd,
    read_wasde,
    read_weather,
)
from pipeline.units import to_metric_tons

# Weather rules that can put a premium on the crop this week.
_PREMIUM_RULES = frozenset({RULE_HEAT, RULE_POD_FILL_HEAT, RULE_HEAVY_RAIN})

# Only the soy complex has a WASDE export-forecast mapping (million bushels,
# `export_sales.wasde_soy_export_forecast`); other commodities' pace is
# withheld with that reason rather than compared against nothing.
_WASDE_EXPORT_COMMODITY = {"Soybeans": "SOYBEANS"}
_WASDE_SOY_EXPORT_UNIT = "million bushels"
_YR_AGO_TOLERANCE_DAYS = 10

_OBS_LABEL = "Reads as:"
_WITHHELD_LABEL = "Not interpreted:"
_FALSIFIER_LABEL = "Confirm / refute:"


@dataclass(frozen=True)
class Driver:
    """One market driver: observation, labelled interpretation, falsifier.

    Exactly one of `interpretation` / `not_interpreted` is set — a driver
    either reads as something or says why it does not. A falsifier is
    mandatory: an interpretation nobody could refute is not one.
    """

    observation: str
    falsifier: str
    interpretation: str | None = None
    not_interpreted: str | None = None

    def __post_init__(self) -> None:
        if bool(self.interpretation) == bool(self.not_interpreted):
            raise ValueError(
                "Driver needs exactly one of interpretation / not_interpreted: "
                f"{self.observation!r}"
            )
        if not self.falsifier:
            raise ValueError(f"Driver has no falsifier: {self.observation!r}")

    def lines(self, index: int) -> list[str]:
        out = [f"  {index}. {self.observation}"]
        if self.interpretation:
            out.append(f"     {_OBS_LABEL} {self.interpretation}")
        else:
            out.append(f"     {_WITHHELD_LABEL} {self.not_interpreted}")
        out.append(f"     {_FALSIFIER_LABEL} {self.falsifier}")
        return out


def _asof(frame: pd.DataFrame) -> str:
    """Date of the last row, for stamping an observation. Blank if unknown."""
    if frame.empty:
        return ""
    last = frame.index[-1]
    if isinstance(last, pd.Timestamp):
        return last.strftime("%Y-%m-%d")
    return ""


def _dated(label: str, frame: pd.DataFrame) -> str:
    stamp = _asof(frame)
    return f"{label} to {stamp}" if stamp else label


def _latest_weekly(frame: pd.DataFrame) -> float | None:
    weekly = frame.get("weekly_pct_change", pd.Series(dtype=float))
    if weekly.empty or pd.isna(weekly.iloc[-1]):
        return None
    return float(weekly.iloc[-1])


# ── positioning ───────────────────────────────────────────────────────────

def spec_net_percentile(subset: pd.DataFrame, latest_date: pd.Timestamp) -> float | None:
    """Percentile rank of the latest spec net within the trailing window.

    The window is `[latest_date - lookback, latest_date)` — the latest
    observation is excluded from its own baseline. None under the
    observation floor: a short history has no extreme to rank against.
    """
    window = subset[
        (subset["Date"] >= latest_date - pd.Timedelta(days=COT_CROWDED_LOOKBACK_DAYS))
        & (subset["Date"] < latest_date)
    ]["noncommercial_net"].dropna()
    if len(window) < COT_CROWDED_MIN_OBSERVATIONS:
        return None
    latest = float(subset[subset["Date"] == latest_date]["noncommercial_net"].iloc[-1])
    return float((window <= latest).mean() * 100)


def _positioning_drivers(enriched: dict[str, pd.DataFrame]) -> list[Driver]:
    drivers: list[Driver] = []
    cot_data = read_cot()
    if cot_data.empty:
        return drivers
    for commodity in cot_data["commodity"].unique():
        if commodity not in enriched or "RSI" not in enriched[commodity].columns:
            continue
        subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
        if subset.empty:
            continue
        latest = subset.iloc[-1]
        spec_net = latest.get("noncommercial_net")
        rsi_val = enriched[commodity]["RSI"].iloc[-1]
        if spec_net is None or pd.isna(spec_net) or pd.isna(rsi_val):
            continue
        spec_net = float(spec_net)
        rsi_val = float(rsi_val)

        long_side = spec_net > 0 and rsi_val > RSI_OVERBOUGHT
        short_side = spec_net < 0 and rsi_val < RSI_OVERSOLD
        if not (long_side or short_side):
            continue
        pct = spec_net_percentile(subset, pd.Timestamp(latest["Date"]))
        if pct is None:
            continue  # no history to call an extreme against — not a driver
        extreme = pct >= COT_CROWDED_PERCENTILE if long_side else pct <= 100 - COT_CROWDED_PERCENTILE
        if not extreme:
            continue

        side = "long" if long_side else "short"
        years = COT_CROWDED_LOOKBACK_DAYS // 365
        observation = (
            f"{commodity} specs net {side} {abs(spec_net):,.0f} contracts "
            f"(COT {pd.Timestamp(latest['Date']).strftime('%Y-%m-%d')}), "
            f"{pct:.0f}th percentile of the trailing {years}-yr net position; "
            f"{_dated(f'RSI {rsi_val:.0f}', enriched[commodity])}"
        )
        if long_side:
            interpretation = (
                "crowded long — a historical-extreme net long with an overbought RSI; "
                "reversal risk elevated"
            )
            falsifier = (
                "refuted if the next COT adds to the net long without a price break; "
                "confirmed by net longs shrinking as RSI falls back under "
                f"{RSI_OVERBOUGHT}"
            )
        else:
            interpretation = (
                "crowded short — a historical-extreme net short with an oversold RSI; "
                "short-squeeze risk"
            )
            falsifier = (
                "refuted if the next COT adds to the net short without a price bounce; "
                "confirmed by net shorts covering as RSI climbs back over "
                f"{RSI_OVERSOLD}"
            )
        drivers.append(Driver(observation, falsifier, interpretation=interpretation))
    return drivers


# ── export sales: China observation + pace rule ───────────────────────────

def _wasde_export_forecast_mt(wasde: pd.DataFrame, marketing_year: str) -> float | None:
    """Latest WASDE export forecast (MT) for `marketing_year`; None if absent
    or stored in a unit this converter does not know (no bogus conversion)."""
    if wasde.empty:
        return None
    rows = wasde[
        (wasde["attribute"].astype(str).str.strip().str.lower() == "exports")
        & (wasde["year"].astype(str) == marketing_year)
    ].sort_values("reference_period")
    if rows.empty or pd.isna(rows.iloc[-1]["value"]):
        return None
    unit = str(rows.iloc[-1].get("unit", "") or "").strip().lower()
    if unit != _WASDE_SOY_EXPORT_UNIT:
        return None
    value = float(rows.iloc[-1]["value"]) * MT_PER_MILLION_BUSHELS_SOY
    return value if isfinite(value) and value > 0 else None


def _committed_mt(week_data: pd.DataFrame) -> float | None:
    """Accumulated exports + outstanding sales across all destinations."""
    if "accumulated_exports" not in week_data.columns or "outstanding_sales" not in week_data.columns:
        return None
    if week_data.empty:
        return None
    # ESR stores one nullable pair per destination, not a total row. Every
    # stored destination must contribute both components; NULL is not zero.
    components = week_data[["accumulated_exports", "outstanding_sales"]]
    if components.isna().any().any():
        return None
    values = components.to_numpy(dtype=float)
    if not all(isfinite(value) and value >= 0 for value in values.flat):
        return None
    total = float(values.sum())
    return total if isfinite(total) else None


def _year_ago_week(subset: pd.DataFrame, latest_week: pd.Timestamp) -> pd.Timestamp | None:
    target = latest_week - pd.Timedelta(days=364)
    candidates = [
        pd.Timestamp(w) for w in subset["week_ending"].dropna().unique()
        if abs((pd.Timestamp(w) - target).days) <= _YR_AGO_TOLERANCE_DAYS
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda w: abs((w - target).days))


def assess_china_pace(
    commodity: str, subset: pd.DataFrame, latest_week: pd.Timestamp, china_net: float
) -> tuple[bool | None, str]:
    """The pace rule: (passed, reason). None = could not be assessed.

    Strong requires BOTH the latest week's absolute China net sales above
    `CHINA_PACE_STRONG_MULTIPLE` × the trailing-week mean AND total
    commitments at or ahead of the year-ago share of the WASDE export
    forecast. Any missing input withholds with its reason.
    """
    is_china = subset["country"].astype(str).str.contains("china", case=False, na=False)
    china_weekly = (
        subset[is_china].groupby("week_ending")["net_sales"].sum(min_count=1).dropna().sort_index()
    )
    prior = china_weekly[china_weekly.index < latest_week].tail(CHINA_PACE_LOOKBACK_WEEKS)
    if len(prior) < CHINA_PACE_MIN_WEEKS:
        return None, (
            f"pace not assessed — {len(prior)} prior week(s) of China net sales, "
            f"rule needs {CHINA_PACE_MIN_WEEKS}"
        )
    trailing_mean = float(prior.mean())
    multiple = china_net / trailing_mean if trailing_mean > 0 else None
    abs_text = (
        f"latest week {multiple:.1f}× the trailing {len(prior)}-wk China mean "
        f"({trailing_mean:,.0f} MT)"
        if multiple is not None
        else f"trailing {len(prior)}-wk China mean {trailing_mean:,.0f} MT, not positive"
    )

    wasde_key = _WASDE_EXPORT_COMMODITY.get(commodity)
    if wasde_key is None:
        return None, f"pace not assessed — no WASDE export forecast mapping for {commodity}; {abs_text}"
    committed = _committed_mt(subset[subset["week_ending"] == latest_week])
    if committed is None:
        return None, f"pace not assessed — no accumulated/outstanding commitments stored; {abs_text}"
    wasde = read_wasde(wasde_key)
    my = soy_marketing_year(latest_week)
    forecast = _wasde_export_forecast_mt(wasde, my)
    if forecast is None:
        return None, f"pace not assessed — no WASDE export forecast in million bushels for MY {my}; {abs_text}"
    ya_week = _year_ago_week(subset, latest_week)
    if ya_week is None:
        return None, f"pace not assessed — no year-ago week within {_YR_AGO_TOLERANCE_DAYS}d stored; {abs_text}"
    ya_committed = _committed_mt(subset[subset["week_ending"] == ya_week])
    ya_forecast = _wasde_export_forecast_mt(wasde, soy_marketing_year(ya_week))
    if ya_committed is None or ya_forecast is None:
        return None, (
            f"pace not assessed — year-ago commitments or WASDE forecast for MY "
            f"{soy_marketing_year(ya_week)} missing; {abs_text}"
        )

    # The persisted WASDE contract has reference_period only. The fetcher
    # also synthesizes prior-month reference periods from revised workbook
    # columns; these are not publication/availability dates. Consequently an
    # as-of denominator cannot be recovered safely from these rows. Withhold
    # the interpretation until ingestion preserves actual release vintages.
    return None, (
        "pace not assessed — WASDE availability dates and original release "
        "vintages are not stored; latest revised estimates cannot establish "
        f"a comparable year-ago forecast; {abs_text}"
    )


def _export_sales_drivers() -> list[Driver]:
    drivers: list[Driver] = []
    es_data = read_export_sales()
    if es_data.empty or "net_sales" not in es_data.columns:
        return drivers
    for commodity in ["Soybeans", "Corn", "Wheat"]:
        subset = es_data[es_data["commodity"] == commodity]
        if subset.empty:
            continue
        latest_week = pd.Timestamp(subset["week_ending"].max())
        week_data = subset[subset["week_ending"] == latest_week]
        china_sales = week_data[week_data["country"].str.contains("China", case=False, na=False)]
        if china_sales.empty or china_sales["net_sales"].isna().all():
            continue
        china_net = float(china_sales["net_sales"].sum())
        total_net = float(week_data["net_sales"].sum(skipna=True))
        week_str = latest_week.strftime("%Y-%m-%d")
        if total_net > 0:
            share_text = f"{china_net / total_net * 100:.0f}% of {total_net:,.0f} MT total net sales"
        else:
            share_text = f"share not computed (total net sales {total_net:,.0f} MT)"
        observation = (
            f"{commodity} China net sales {china_net:,.0f} MT w/e {week_str} — {share_text}"
        )
        passed, reason = assess_china_pace(commodity, subset, latest_week, china_net)
        falsifier = (
            "refuted by next week's China net sales back under the trailing mean or by "
            "cancellations; confirmed if commitments keep running ahead of the year-ago share"
        )
        if passed:
            drivers.append(Driver(
                observation, falsifier, interpretation=f"China buying pace strong — {reason}",
            ))
        else:
            drivers.append(Driver(observation, falsifier, not_interpreted=reason))
    return drivers


# ── the section ──────────────────────────────────────────────────────────

def _brl_driver(currency_data: dict[str, pd.DataFrame]) -> Driver | None:
    brl = currency_data.get("BRL/USD")
    if brl is None or brl.empty or len(brl) < 6:
        return None
    brl_chg = ((brl["Close"].iloc[-1] - brl["Close"].iloc[-6]) / brl["Close"].iloc[-6]) * 100
    if pd.isna(brl_chg) or abs(brl_chg) <= 1:
        return None
    weaker = brl_chg < 0
    observation = _dated(
        f"BRL/USD {brl_chg:+.1f}% over the week (BRL {'weaker' if weaker else 'stronger'})", brl
    )
    interpretation = (
        "Brazilian soy cheaper in USD at an unchanged BRL price — export competitiveness improving"
        if weaker
        else "Brazilian soy dearer in USD at an unchanged BRL price — export competitiveness declining"
    )
    falsifier = (
        "refuted if Paranaguá FOB in USD moves in step with the currency (Brazil basis, section 4); "
        "confirmed by the Gulf–Paranaguá FOB spread widening the same way"
    )
    return Driver(observation, falsifier, interpretation=interpretation)


def _weather_driver(enriched: dict[str, pd.DataFrame]) -> Driver | None:
    weather_data = read_weather()
    if weather_data.empty or "Soybeans" not in enriched:
        return None
    # The shared rule set (#355), narrowed to the rules that put a
    # premium on a crop — heat and a downpour, as this line always read.
    active_alerts = []
    for region, subset in weather_data.groupby("region", sort=False):
        assessed = assess_region(str(region), subset)
        if assessed is not None and any(alert.rule in _PREMIUM_RULES for alert in assessed.alerts):
            active_alerts.append(str(region))
    if not active_alerts:
        return None
    weekly = _latest_weekly(enriched["Soybeans"])
    if weekly is None or weekly <= 1:
        return None
    observation = _dated(
        f"Soybeans {weekly:+.1f}% this week with heat / heavy-rain alerts active in "
        f"{', '.join(active_alerts[:3])}",
        enriched["Soybeans"],
    )
    return Driver(
        observation,
        "a weather premium would unwind when the alerts clear; price holding after they clear "
        "refutes it",
        not_interpreted="a price move beside an active alert does not establish a weather cause — "
        "the move is not decomposed by driver",
    )


def _acreage_driver(enriched: dict[str, pd.DataFrame]) -> Driver | None:
    if "Corn" not in enriched or "Soybeans" not in enriched:
        return None
    corn_chg = _latest_weekly(enriched["Corn"])
    soy_chg = _latest_weekly(enriched["Soybeans"])
    if corn_chg is None or soy_chg is None or abs(corn_chg - soy_chg) <= 3:
        return None
    corn_leads = corn_chg > soy_chg
    observation = _dated(
        f"Corn {corn_chg:+.1f}% vs Soybeans {soy_chg:+.1f}% this week "
        f"({'corn' if corn_leads else 'soybeans'} outperforming)",
        enriched["Corn"],
    )
    interpretation = (
        "a relative-price incentive toward corn acreage next planting season, if sustained"
        if corn_leads
        else "a relative-price incentive toward soybean acreage next planting season, if sustained"
    )
    return Driver(
        observation,
        "the new-crop soy/corn price ratio holding the move into planting, then March "
        "Prospective Plantings; a one-week divergence that reverts next week refutes it",
        interpretation=interpretation,
    )


def _livestock_drivers(enriched: dict[str, pd.DataFrame]) -> list[Driver]:
    drivers: list[Driver] = []
    for livestock in ["Live Cattle", "Lean Hogs"]:
        if livestock not in enriched:
            continue
        chg = _latest_weekly(enriched[livestock])
        if chg is None or chg <= 3:
            continue
        drivers.append(Driver(
            _dated(f"{livestock} {chg:+.1f}% this week", enriched[livestock]),
            "USDA Hogs & Pigs / Cattle on Feed inventory counts would establish feed demand; "
            "not carried here",
            not_interpreted="a price move does not establish inventory — liquidation lifts "
            "livestock prices as readily as expansion does",
        ))
    return drivers


def _curve_drivers() -> list[Driver]:
    drivers: list[Driver] = []
    fc_data = read_forward_curve()
    if fc_data.empty:
        return drivers
    for commodity in ["Soybeans", "Corn", "Wheat"]:
        fc_subset = fc_data[fc_data["commodity"] == commodity]
        if len(fc_subset) < 2:
            continue
        result = analyze_curve(fc_subset)
        if not result:
            continue
        stamp = ""
        for col in ("observation_date", "fetched_date"):
            if col in fc_subset.columns and fc_subset[col].notna().any():
                stamp = f" (observed {fc_subset[col].dropna().astype(str).max()})"
                break
        if "backwardation" in result.get("structure", ""):
            drivers.append(Driver(
                f"{commodity} curve in backwardation, front–next {result['spread_pct']:+.1f}%{stamp}",
                "refuted if the inversion narrows as nearby open interest rolls off; confirmed "
                "by nearby basis firming alongside (sections 4–4b)",
                interpretation="the market pays for prompt delivery — nearby supply tight "
                "relative to deferred",
            ))
        elif result.get("spread_pct", 0) > 5:
            drivers.append(Driver(
                f"{commodity} curve in steep contango, front–next {result['spread_pct']:+.1f}%{stamp}",
                "refuted if the carry collapses on a nearby bid; confirmed by stocks building "
                "in the next WASDE / stocks-to-use (sections 9–10)",
                interpretation="the market pays to store — adequate nearby supply, full carry",
            ))
    return drivers


def _oil_spread_driver(
    label_a: str, chg_a: float, label_b: str, chg_b: float, frame: pd.DataFrame,
    spread_txt: str = "",
) -> Driver | None:
    """A > B by more than 3 pts on the week → labelled substitution reading."""
    if chg_a - chg_b > 3:
        lead, lag, lead_chg, lag_chg = label_a, label_b, chg_a, chg_b
    elif chg_b - chg_a > 3:
        lead, lag, lead_chg, lag_chg = label_b, label_a, chg_b, chg_a
    else:
        return None
    observation = _dated(
        f"{lead} outperforming {lag} ({lead_chg:+.1f}% vs {lag_chg:+.1f}% this week){spread_txt}",
        frame,
    )
    return Driver(
        observation,
        f"refuted if the spread reverts within a week; confirmed by import mix shifting "
        f"toward {lag} at the buyers that switch (India SEA, Layer 32)",
        interpretation=f"{lead} premium widening — substitution may shift demand toward {lag}",
    )


def _palm_driver(enriched: dict[str, pd.DataFrame]) -> Driver | None:
    if "Palm Oil (CME)" not in enriched or "Soybean Oil" not in enriched:
        return None
    palm_chg = _latest_weekly(enriched["Palm Oil (CME)"])
    oil_chg = _latest_weekly(enriched["Soybean Oil"])
    if palm_chg is None or oil_chg is None:
        return None
    return _oil_spread_driver("Palm oil", palm_chg, "soybean oil", oil_chg, enriched["Soybean Oil"])


def _rapeseed_driver(
    enriched: dict[str, pd.DataFrame], currency_data: dict[str, pd.DataFrame]
) -> Driver | None:
    # Cross-oilseed: CBOT soy oil vs CZCE rapeseed oil (USD/MT). ICE canola
    # (RS=F) is dead on yfinance, so CZCE is the daily rapeseed leg —
    # CNY/MT converted at CNY/USD spot.
    if "Soybean Oil" not in enriched or enriched["Soybean Oil"].empty:
        return None
    cny_usd = None
    cny_df = currency_data.get("CNY/USD") if currency_data else None
    if cny_df is not None and not cny_df.empty:
        rate = cny_df["Close"].iloc[-1]
        if pd.notna(rate) and rate > 0:
            cny_usd = float(rate)
    rapeseed = read_dce_futures("CZCE Rapeseed Oil")
    oil_chg = _latest_weekly(enriched["Soybean Oil"])
    if cny_usd is None or len(rapeseed) < 6 or oil_chg is None:
        return None
    rapeseed = rapeseed.sort_values("Date")
    rape_chg = (
        (rapeseed["Close"].iloc[-1] - rapeseed["Close"].iloc[-6]) / rapeseed["Close"].iloc[-6]
    ) * 100
    if pd.isna(rape_chg):
        return None
    rape_usd = float(rapeseed["Close"].iloc[-1]) * cny_usd
    soy_oil_usd = to_metric_tons(enriched["Soybean Oil"]["Close"].iloc[-1], "Soybean Oil")
    spread_txt = ""
    if soy_oil_usd is not None:
        spread_txt = f" (CZCE {rape_usd:,.0f} vs CBOT {soy_oil_usd:,.0f} USD/MT)"
    return _oil_spread_driver(
        "CZCE rapeseed oil", float(rape_chg), "soybean oil", oil_chg,
        enriched["Soybean Oil"], spread_txt,
    )


def _biodiesel_driver() -> Driver | None:
    eia = read_eia_data()
    if eia.empty or "series_name" not in eia.columns:
        return None
    biodiesel = eia[eia["series_name"] == "Biodiesel Production"].sort_values("Date")
    if len(biodiesel) < 2:
        return None
    latest_bio = biodiesel.iloc[-1]["value"]
    prev_bio = biodiesel.iloc[-2]["value"]
    if pd.isna(latest_bio) or pd.isna(prev_bio) or prev_bio <= 0:
        return None
    bio_chg = ((latest_bio - prev_bio) / prev_bio) * 100
    if abs(bio_chg) <= 5:
        return None
    stamp = pd.Timestamp(biodiesel.iloc[-1]["Date"]).strftime("%Y-%m-%d")
    direction = "rising" if bio_chg > 0 else "falling"
    return Driver(
        f"EIA Biodiesel production {direction} {bio_chg:+.1f}% on the prior period, to {stamp} "
        f"(biodiesel only — renewable diesel is a separate EIA series not carried here)",
        "EIA's monthly feedstock table (soybean oil share of biodiesel inputs) and D4 RIN "
        "generation (Layer 29) would establish the soy oil pull",
        not_interpreted="production volume alone does not establish soy oil demand — the "
        "feedstock mix is not carried",
    )


def _conab_driver() -> Driver | None:
    brazil_data = read_brazil_estimates()
    if brazil_data.empty:
        return None
    soy_conab = brazil_data[
        (brazil_data["commodity"] == "Soybeans") & (brazil_data["attribute"] == "Production")
    ]
    if soy_conab.empty:
        return None
    latest_year = soy_conab["crop_year"].max()
    conab_val = soy_conab[soy_conab["crop_year"] == latest_year]["value"].iloc[0]
    psd = read_psd()
    if psd.empty or pd.isna(conab_val):
        return None
    usda_brazil = psd[
        (psd["commodity"] == "Soybeans")
        & (psd["country"] == "Brazil")
        & (psd["attribute"] == "Production")
    ]
    if usda_brazil.empty:
        return None
    usda_val = usda_brazil[usda_brazil["year"] == usda_brazil["year"].max()]["value"]
    if usda_val.empty or pd.isna(usda_val.iloc[0]):
        return None
    gap = float(conab_val) - float(usda_val.iloc[0])
    if abs(gap) <= 2000:
        return None
    direction = "higher" if gap > 0 else "lower"
    return Driver(
        f"CONAB Brazil soybean production ({latest_year}) {abs(gap):,.0f} thousand MT {direction} "
        f"than USDA PSD ({usda_brazil['year'].max()})",
        "the next WASDE Brazil production line and the next CONAB survey; convergence from "
        "either side resolves which estimate moved",
        not_interpreted="a gap says the agencies disagree, not which one revises",
    )


def _dollar_driver() -> Driver | None:
    econ = read_economic()
    if econ.empty:
        return None
    dollar = econ[econ["series_name"] == "US Dollar Index"].sort_values("Date")
    if len(dollar) < 2:
        return None
    latest_val = dollar.iloc[-1]["value"]
    prev_val = dollar.iloc[-2]["value"]
    if pd.isna(latest_val) or pd.isna(prev_val) or prev_val == 0:
        return None
    dollar_chg = ((latest_val - prev_val) / prev_val) * 100
    if abs(dollar_chg) <= 0.5:
        return None
    stamp = pd.Timestamp(dollar.iloc[-1]["Date"]).strftime("%Y-%m-%d")
    stronger = dollar_chg > 0
    return Driver(
        f"US Dollar Index {dollar_chg:+.1f}% on the session to {stamp}",
        "refuted if CBOT soy moves with the dollar this week rather than against it",
        interpretation=(
            "generally a headwind for USD-denominated commodities"
            if stronger
            else "generally a tailwind for USD-denominated commodities"
        ),
    )


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    enriched: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    lines = ["MARKET DRIVERS:"]
    drivers: list[Driver] = []

    brl = _brl_driver(currency_data or {})
    if brl:
        drivers.append(brl)
    drivers.extend(_positioning_drivers(enriched))
    weather = _weather_driver(enriched)
    if weather:
        drivers.append(weather)
    acreage = _acreage_driver(enriched)
    if acreage:
        drivers.append(acreage)
    drivers.extend(_livestock_drivers(enriched))
    drivers.extend(_export_sales_drivers())
    drivers.extend(_curve_drivers())
    palm = _palm_driver(enriched)
    if palm:
        drivers.append(palm)
    rapeseed = _rapeseed_driver(enriched, currency_data or {})
    if rapeseed:
        drivers.append(rapeseed)
    for maybe in (_biodiesel_driver(), _conab_driver(), _dollar_driver()):
        if maybe:
            drivers.append(maybe)

    if not drivers:
        lines.append("  No cross-market signals detected this session")
    else:
        for i, driver in enumerate(drivers, 1):
            lines.extend(driver.lines(i))

    return "\n".join(lines)
