"""EMERGING MARKETS section — South Africa, India, Nigeria, Brazil deep dive.

Pulls structured data from `analysis.soy_analytics.emerging_markets_analysis`
(lazy import to avoid a cycle — soy_analytics imports loaders that are
shared with briefing). The India NCDEX sub-section is rendered inline at
the bottom of the block.
"""


def _format_india_domestic(em_countries: dict) -> str:
    """Inline India NCDEX domestic prices + crush margin block."""
    india = em_countries.get("India", {})
    dom = india.get("india_domestic", {})
    if not dom:
        return ""

    lines = ["INDIA — NCDEX Domestic Prices:"]

    soy_inr = dom.get("soybean_ncdex_inr")
    oil_inr = dom.get("oil_ncdex_inr")
    meal_inr = dom.get("meal_ncdex_inr")

    if soy_inr:
        soy_usd = dom.get("soybean_ncdex_usd")
        usd_str = f" / ${soy_usd:,.1f}/MT" if soy_usd else ""
        lines.append(f"  Soybean: ₹{soy_inr:,.0f}/MT{usd_str}")
    if oil_inr:
        lines.append(f"  Soy Oil: ₹{oil_inr:,.0f}/MT")
    if meal_inr:
        lines.append(f"  Soy Meal: ₹{meal_inr:,.0f}/MT")

    crush_inr = dom.get("crush_margin_inr")
    crush_usd = dom.get("crush_margin_usd")
    cbot_crush = dom.get("cbot_crush_usd")
    premium = dom.get("crush_premium_usd")

    if crush_inr is not None:
        lines.append(
            f"  India Crush Margin: ₹{crush_inr:,.0f}/MT"
            + (f" (${crush_usd:,.1f}/MT)" if crush_usd else "")
        )
    if cbot_crush is not None:
        lines.append(f"  CBOT Crush:         ${cbot_crush:,.1f}/MT")
    if premium is not None:
        direction = "premium" if premium > 0 else "discount"
        note = (
            "India crushers have edge, may resist imports"
            if premium > 0
            else "CBOT-origin meal competitive in ME/Africa"
        )
        lines.append(
            f"  India vs CBOT:      ${premium:+,.1f}/MT {direction} — {note}"
        )

    weekly = dom.get("weekly_chg_pct")
    if weekly is not None:
        lines.append(f"  Weekly Chg (NCDEX Soybean): {weekly:+.1f}%")

    return "\n".join(lines)


def format() -> str:  # noqa: A001
    lines = ["EMERGING MARKETS (Soybeans):"]

    try:
        from analysis.soy_analytics import emerging_markets_analysis
        data = emerging_markets_analysis()
    except Exception:
        return "EMERGING MARKETS: Analysis unavailable"

    countries = data.get("countries", {})
    if not countries:
        return "EMERGING MARKETS: No data"

    for country_name, info in countries.items():
        lines.append(f"  {country_name}:")

        psd = info.get("psd", {})
        year = info.get("psd_year", "")
        if psd:
            for attr, vals in psd.items():
                val = vals["value"]
                unit = vals.get("unit", "1000 MT")
                yoy = vals.get("yoy_pct")
                if yoy is not None:
                    lines.append(f"    {attr}: {val:,.0f} {unit} ({yoy:+.1f}% YoY)")
                else:
                    lines.append(f"    {attr}: {val:,.0f} {unit} ({year})")

        currency = info.get("currency", {})
        if currency:
            pair = currency["pair"]
            close = currency["close"]
            parts = [f"{pair}: {close:.4f}"]
            wk = currency.get("weekly_chg")
            if wk is not None:
                parts.append(f"week: {wk:+.1f}%")
            lines.append(f"    Currency: {', '.join(parts)}")

        weather_list = info.get("weather", [])
        active_alerts = [w for w in weather_list if w.get("alert")]
        if active_alerts:
            for w in active_alerts:
                lines.append(f"    Weather: {w['region']} — {w['alert']}")

        dom_india = info.get("india_domestic", {})
        if dom_india:
            soy_inr = dom_india.get("soybean_ncdex_inr")
            crush_usd = dom_india.get("crush_margin_usd")
            premium = dom_india.get("crush_premium_usd")
            if soy_inr:
                soy_usd = dom_india.get("soybean_ncdex_usd")
                lines.append(
                    f"    NCDEX Soybean: ₹{soy_inr:,.0f}/MT"
                    + (f" / ${soy_usd:,.1f}/MT" if soy_usd else "")
                )
            if crush_usd is not None:
                lines.append(f"    India Crush Margin: ${crush_usd:,.1f}/MT")
            if premium is not None:
                lines.append(f"    vs CBOT Crush: {premium:+.1f} USD/MT")

        dom_brazil = info.get("brazil_domestic", {})
        if dom_brazil:
            cepea_brl = dom_brazil.get("cepea_soy_brl")
            cepea_usd = dom_brazil.get("cepea_soy_usd")
            basis = dom_brazil.get("brazil_cbot_basis_usd")
            weekly = dom_brazil.get("weekly_chg_pct")
            if cepea_brl:
                usd_str = f" / ${cepea_usd:,.1f}/MT" if cepea_usd else ""
                lines.append(f"    CEPEA: R${cepea_brl:,.2f}/MT{usd_str}")
            if basis is not None:
                lines.append(
                    f"    Brazil-CBOT Basis: ${basis:+.1f}/MT"
                    + (" (premium)" if basis > 0 else " (discount)")
                )
            if weekly is not None:
                lines.append(f"    Weekly Chg: {weekly:+.1f}%")

        dom_sa = info.get("south_africa_domestic", {})
        if dom_sa:
            safex_zar = dom_sa.get("soybean_safex_zar")
            safex_usd = dom_sa.get("soybean_safex_usd")
            basis = dom_sa.get("safex_cbot_basis_usd")
            if safex_zar:
                usd_str = f" / ${safex_usd:,.1f}/MT" if safex_usd else ""
                lines.append(f"    SAFEX Soybean: R{safex_zar:,.0f}/MT{usd_str}")
            if basis is not None:
                lines.append(
                    f"    SAFEX-CBOT Basis: ${basis:+.1f}/MT"
                    + (" (sub-Saharan premium)" if basis > 0 else " (import parity)")
                )

    india_section = _format_india_domestic(countries)
    if india_section:
        lines.append("")
        lines.append(india_section)

    return "\n".join(lines)
