"""MARKET DRIVERS section — cross-data narrative.

This is the most cross-cutting section: it reads several DB tables
directly and also relies on the `enriched` price frames (with technical
indicators applied) produced by the prices section.
"""

import pandas as pd

from analysis.forward_curve import analyze_curve
from config import (
    RSI_OVERBOUGHT,
    RSI_OVERSOLD,
    WEATHER_EXTREME_HEAT_C,
    WEATHER_HEAVY_RAIN_MM,
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
    read_weather,
)
from pipeline.units import to_metric_tons


def format(  # noqa: A001
    price_data: dict[str, pd.DataFrame],
    enriched: dict[str, pd.DataFrame],
    currency_data: dict[str, pd.DataFrame],
) -> str:
    lines = ["MARKET DRIVERS:"]
    drivers = []

    if "BRL/USD" in currency_data and not currency_data["BRL/USD"].empty:
        brl = currency_data["BRL/USD"]
        if len(brl) >= 6:
            brl_chg = ((brl["Close"].iloc[-1] - brl["Close"].iloc[-6]) / brl["Close"].iloc[-6]) * 100
            if brl_chg < -1:
                drivers.append(
                    f"Brazil export competitiveness improving: BRL weakened {abs(brl_chg):.1f}% "
                    f"this week — makes Brazilian soy cheaper on world markets"
                )
            elif brl_chg > 1:
                drivers.append(
                    f"Brazil export competitiveness declining: BRL strengthened {brl_chg:.1f}% "
                    f"this week — Brazilian exports getting more expensive"
                )

    cot_data = read_cot()
    if not cot_data.empty:
        for commodity in cot_data["commodity"].unique():
            cot_subset = cot_data[cot_data["commodity"] == commodity].sort_values("Date")
            if cot_subset.empty:
                continue

            spec_net = cot_subset.iloc[-1].get("noncommercial_net", None)
            if spec_net is not None and pd.notna(spec_net) and commodity in enriched:
                rsi_val = enriched[commodity]["RSI"].iloc[-1] if "RSI" in enriched[commodity].columns else None

                if rsi_val is not None and pd.notna(rsi_val):
                    if spec_net > 0 and rsi_val > RSI_OVERBOUGHT:
                        drivers.append(
                            f"Crowded long in {commodity}: Specs net long {spec_net:,.0f} contracts "
                            f"AND RSI at {rsi_val:.0f} — reversal risk elevated"
                        )
                    elif spec_net < 0 and rsi_val < RSI_OVERSOLD:
                        drivers.append(
                            f"Crowded short in {commodity}: Specs net short {abs(spec_net):,.0f} contracts "
                            f"AND RSI at {rsi_val:.0f} — short squeeze risk"
                        )

    weather_data = read_weather()
    if not weather_data.empty:
        active_alerts = []
        for region in weather_data["region"].unique():
            subset = weather_data[weather_data["region"] == region].sort_values("Date")
            if subset.empty:
                continue
            latest = subset.iloc[-1]
            precip = latest.get("precipitation", 0)
            temp_max = latest.get("temp_max", None)

            if (pd.notna(precip) and precip > WEATHER_HEAVY_RAIN_MM) or (
                pd.notna(temp_max) and temp_max > WEATHER_EXTREME_HEAT_C
            ):
                active_alerts.append(region)

        if active_alerts:
            for commodity in ["Soybeans"]:
                if commodity in enriched:
                    weekly_chg = enriched[commodity].get("weekly_pct_change", pd.Series())
                    if not weekly_chg.empty and pd.notna(weekly_chg.iloc[-1]) and weekly_chg.iloc[-1] > 1:
                        drivers.append(
                            f"Weather premium building in {commodity}: price up "
                            f"{weekly_chg.iloc[-1]:.1f}% this week with active weather alerts "
                            f"in {', '.join(active_alerts[:3])}"
                        )

    if "Corn" in enriched and "Soybeans" in enriched:
        corn_weekly = enriched["Corn"].get("weekly_pct_change", pd.Series())
        soy_weekly = enriched["Soybeans"].get("weekly_pct_change", pd.Series())
        if (
            not corn_weekly.empty
            and not soy_weekly.empty
            and pd.notna(corn_weekly.iloc[-1])
            and pd.notna(soy_weekly.iloc[-1])
        ):
            corn_chg = corn_weekly.iloc[-1]
            soy_chg = soy_weekly.iloc[-1]
            if corn_chg - soy_chg > 3:
                drivers.append(
                    f"Corn outperforming soybeans ({corn_chg:+.1f}% vs {soy_chg:+.1f}% this week): "
                    f"if sustained, farmers may shift acreage to corn next planting season"
                )
            elif soy_chg - corn_chg > 3:
                drivers.append(
                    f"Soybeans outperforming corn ({soy_chg:+.1f}% vs {corn_chg:+.1f}% this week): "
                    f"soybean acreage may expand next season"
                )

    for livestock in ["Live Cattle", "Lean Hogs"]:
        if livestock in enriched:
            lv_weekly = enriched[livestock].get("weekly_pct_change", pd.Series())
            if not lv_weekly.empty and pd.notna(lv_weekly.iloc[-1]) and lv_weekly.iloc[-1] > 3:
                drivers.append(
                    f"{livestock} prices rising ({lv_weekly.iloc[-1]:+.1f}% this week): "
                    f"expanding herds = more soybean meal demand"
                )

    es_data = read_export_sales()
    if not es_data.empty:
        for commodity in ["Soybeans", "Corn", "Wheat"]:
            es_subset = es_data[es_data["commodity"] == commodity]
            if es_subset.empty:
                continue
            latest_week = es_subset["week_ending"].max()
            week_data = es_subset[es_subset["week_ending"] == latest_week]
            china_sales = week_data[week_data["country"].str.contains("China", case=False, na=False)]
            if not china_sales.empty and "net_sales" in china_sales.columns:
                china_net = china_sales["net_sales"].sum()
                total_net = week_data["net_sales"].sum()
                if total_net > 0 and china_net > 0:
                    china_pct = (china_net / total_net) * 100
                    if china_pct > 30:
                        drivers.append(
                            f"China buying pace strong for {commodity}: "
                            f"{china_net:,.0f} MT net sales ({china_pct:.0f}% of total) — "
                            f"demand signal bullish"
                        )

    fc_data = read_forward_curve()
    if not fc_data.empty:
        for commodity in ["Soybeans", "Corn", "Wheat"]:
            fc_subset = fc_data[fc_data["commodity"] == commodity]
            if len(fc_subset) >= 2:
                result = analyze_curve(fc_subset)
                if result and "backwardation" in result.get("structure", ""):
                    drivers.append(
                        f"{commodity} in backwardation ({result['spread_pct']:+.1f}%): "
                        f"market signals tight supply / strong nearby demand"
                    )
                elif result and result.get("spread_pct", 0) > 5:
                    drivers.append(
                        f"{commodity} in steep contango ({result['spread_pct']:+.1f}%): "
                        f"market expects adequate supply, carrying costs elevated"
                    )

    if "Palm Oil (CME)" in enriched and "Soybean Oil" in enriched:
        palm = enriched["Palm Oil (CME)"]
        soy_oil = enriched["Soybean Oil"]
        if not palm.empty and not soy_oil.empty:
            palm_weekly = palm.get("weekly_pct_change", pd.Series())
            oil_weekly = soy_oil.get("weekly_pct_change", pd.Series())
            if (
                not palm_weekly.empty
                and not oil_weekly.empty
                and pd.notna(palm_weekly.iloc[-1])
                and pd.notna(oil_weekly.iloc[-1])
            ):
                palm_chg = palm_weekly.iloc[-1]
                oil_chg = oil_weekly.iloc[-1]
                if palm_chg - oil_chg > 3:
                    drivers.append(
                        f"Palm oil outperforming soybean oil ({palm_chg:+.1f}% vs {oil_chg:+.1f}%): "
                        f"palm premium widening — may shift demand toward soy oil"
                    )
                elif oil_chg - palm_chg > 3:
                    drivers.append(
                        f"Soybean oil outperforming palm oil ({oil_chg:+.1f}% vs {palm_chg:+.1f}%): "
                        f"soy oil premium building — demand may shift to palm"
                    )

    # Cross-oilseed: CBOT soy oil vs CZCE rapeseed oil (USD/MT). ICE canola
    # (RS=F) is dead on yfinance, so CZCE is the daily rapeseed leg —
    # CNY/MT converted at CNY/USD spot.
    if "Soybean Oil" in enriched and not enriched["Soybean Oil"].empty:
        cny_usd = None
        if currency_data:
            cny_df = currency_data.get("CNY/USD")
            if cny_df is not None and not cny_df.empty:
                rate = cny_df["Close"].iloc[-1]
                if pd.notna(rate) and rate > 0:
                    cny_usd = float(rate)

        rapeseed = read_dce_futures("CZCE Rapeseed Oil")
        oil_weekly = enriched["Soybean Oil"].get("weekly_pct_change", pd.Series())
        if (
            cny_usd is not None
            and len(rapeseed) >= 6
            and not oil_weekly.empty
            and pd.notna(oil_weekly.iloc[-1])
        ):
            rapeseed = rapeseed.sort_values("Date")
            rape_chg = (
                (rapeseed["Close"].iloc[-1] - rapeseed["Close"].iloc[-6])
                / rapeseed["Close"].iloc[-6]
            ) * 100
            oil_chg = oil_weekly.iloc[-1]
            if pd.notna(rape_chg):
                rape_usd = float(rapeseed["Close"].iloc[-1]) * cny_usd
                soy_oil_usd = to_metric_tons(
                    enriched["Soybean Oil"]["Close"].iloc[-1], "Soybean Oil"
                )
                spread_txt = ""
                if soy_oil_usd is not None:
                    spread_txt = (
                        f" (CZCE {rape_usd:,.0f} vs CBOT {soy_oil_usd:,.0f} USD/MT)"
                    )
                if rape_chg - oil_chg > 3:
                    drivers.append(
                        f"CZCE rapeseed oil outperforming soybean oil "
                        f"({rape_chg:+.1f}% vs {oil_chg:+.1f}%){spread_txt}: "
                        f"rapeseed premium widening — may shift demand toward soy oil"
                    )
                elif oil_chg - rape_chg > 3:
                    drivers.append(
                        f"Soybean oil outperforming CZCE rapeseed oil "
                        f"({oil_chg:+.1f}% vs {rape_chg:+.1f}%){spread_txt}: "
                        f"soy oil premium building — demand may shift to rapeseed oil"
                    )

    eia_data_local = read_eia_data()
    if not eia_data_local.empty:
        biodiesel = eia_data_local[eia_data_local["series_name"] == "Biodiesel Production"].sort_values("Date")
        if len(biodiesel) >= 2:
            latest_bio = biodiesel.iloc[-1]["value"]
            prev_bio = biodiesel.iloc[-2]["value"]
            if pd.notna(latest_bio) and pd.notna(prev_bio) and prev_bio > 0:
                bio_chg = ((latest_bio - prev_bio) / prev_bio) * 100
                if bio_chg > 5:
                    drivers.append(
                        f"Biodiesel production surging ({bio_chg:+.1f}%): "
                        f"renewable diesel pulling more soybean oil — bullish ZL=F"
                    )
                elif bio_chg < -5:
                    drivers.append(
                        f"Biodiesel production declining ({bio_chg:+.1f}%): "
                        f"reduced biofuel pull on soybean oil — bearish ZL=F"
                    )

    brazil_data = read_brazil_estimates()
    if not brazil_data.empty:
        psd_data_local = read_psd()
        soy_conab = brazil_data[
            (brazil_data["commodity"] == "Soybeans") & (brazil_data["attribute"] == "Production")
        ]
        if not soy_conab.empty:
            latest_year = soy_conab["crop_year"].max()
            conab_val = soy_conab[soy_conab["crop_year"] == latest_year]["value"].iloc[0]

            if not psd_data_local.empty and pd.notna(conab_val):
                usda_brazil = psd_data_local[
                    (psd_data_local["commodity"] == "Soybeans")
                    & (psd_data_local["country"] == "Brazil")
                    & (psd_data_local["attribute"] == "Production")
                ]
                if not usda_brazil.empty:
                    usda_val = usda_brazil[usda_brazil["year"] == usda_brazil["year"].max()]["value"]
                    if not usda_val.empty:
                        usda_val = usda_val.iloc[0]
                        if pd.notna(usda_val) and abs(conab_val - usda_val) > 2000:
                            gap = conab_val - usda_val
                            direction = "higher" if gap > 0 else "lower"
                            drivers.append(
                                f"CONAB vs USDA divergence: CONAB estimates Brazil soybean production "
                                f"{abs(gap):,.0f} 1000 MT {direction} than USDA — "
                                f"{'USDA may revise up' if gap > 0 else 'USDA may revise down'}"
                            )

    econ_data = read_economic()
    if not econ_data.empty:
        dollar = econ_data[econ_data["series_name"] == "US Dollar Index"].sort_values("Date")
        if len(dollar) >= 2:
            latest_val = dollar.iloc[-1]["value"]
            prev_val = dollar.iloc[-2]["value"]
            if pd.notna(latest_val) and pd.notna(prev_val) and prev_val != 0:
                dollar_chg = ((latest_val - prev_val) / prev_val) * 100
                if abs(dollar_chg) > 0.5:
                    direction = "strengthening" if dollar_chg > 0 else "weakening"
                    impact = "headwind" if dollar_chg > 0 else "tailwind"
                    drivers.append(
                        f"Dollar {direction} ({dollar_chg:+.1f}%): "
                        f"generally a {impact} for USD-denominated commodities"
                    )

    if not drivers:
        lines.append("  No cross-market signals detected this session")
    else:
        for i, driver in enumerate(drivers, 1):
            lines.append(f"  {i}. {driver}")

    return "\n".join(lines)
