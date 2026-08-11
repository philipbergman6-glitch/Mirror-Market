"""EMERGING MARKETS section — South Africa, India, Nigeria, Brazil deep dive.

Pulls structured data from `analysis.soy_analytics.emerging_markets_analysis`
(lazy import to avoid a cycle — soy_analytics imports loaders that are
shared with briefing). The India mandi sub-section is rendered inline at
the bottom of the block.
"""


def _format_india_domestic(em_countries: dict) -> str:
    """Inline India mandi domestic bean price + CBOT premium block.

    Bean-only since the 2026-08 Layer 16 rebuild — the Agmarknet mandi
    source has no meal leg, so the old NCDEX crush margin is gone.
    """
    india = em_countries.get("India", {})
    dom = india.get("india_domestic", {})
    if not dom:
        return ""

    lines = ["INDIA — Mandi Domestic Price (Agmarknet, state medians):"]

    soy_inr = dom.get("soybean_mandi_inr")
    if soy_inr:
        soy_usd = dom.get("soybean_mandi_usd")
        usd_str = f" / ${soy_usd:,.1f}/MT" if soy_usd else ""
        lines.append(f"  Soybean (MP, Indore hub): ₹{soy_inr:,.0f}/MT{usd_str}")

    mh_inr = dom.get("soybean_mandi_mh_inr")
    if mh_inr:
        mh_usd = dom.get("soybean_mandi_mh_usd")
        usd_str = f" / ${mh_usd:,.1f}/MT" if mh_usd else ""
        lines.append(f"  Soybean (MH, #1 state):   ₹{mh_inr:,.0f}/MT{usd_str}")

    cbot_usd = dom.get("cbot_bean_usd")
    if cbot_usd is not None:
        lines.append(f"  CBOT Soybean:       ${cbot_usd:,.1f}/MT")

    premium = dom.get("bean_premium_usd")
    if premium is not None:
        direction = "premium" if premium > 0 else "discount"
        note = (
            "domestic crop tight, import appetite builds"
            if premium > 0
            else "domestic beans cheap, import demand fades"
        )
        lines.append(
            f"  India vs CBOT:      ${premium:+,.1f}/MT {direction} — {note}"
        )

    weekly = dom.get("weekly_chg_pct")
    if weekly is not None:
        lines.append(f"  Weekly Chg (Mandi Soybean): {weekly:+.1f}%")

    return "\n".join(lines)


def _format_sa_deliveries(info: dict) -> list[str]:
    """South Africa physical-flow lines — SAGIS weekly producer deliveries.

    The SA leg is a flow page, not a price page: the SAFEX print above is
    licence-capped to a last-traded number, while these tonnages carry an
    explicit reproduction grant (#202). Comparisons are stated at the same
    *week number*, which is how SAGIS itself frames them.
    """
    deliveries = info.get("south_africa_deliveries", {})
    if not deliveries:
        return []

    lines: list[str] = []
    for key, label in (("soybeans", "Soybeans"), ("sunflower", "Sunflower Seed")):
        pace = deliveries.get(key)
        if not pace:
            continue
        week = pace.get("week_number")
        week_end = pace.get("week_end")
        lines.append(
            f"    SAGIS {label} Deliveries: {pace['week_total_mt']:,.0f} MT "
            f"(wk {week}, ending {week_end})"
        )

        prog_parts = [f"{pace['progressive_mt']:,.0f} MT season-to-date"]
        yoy = pace.get("yoy_pct")
        if yoy is not None:
            prog_parts.append(f"{yoy:+.1f}% YoY")
        avg = pace.get("vs_avg3_pct")
        if avg is not None:
            prog_parts.append(f"{avg:+.1f}% vs 3y avg")
        lines.append(
            f"      {pace.get('season_label', '')} "
            f"({pace.get('season_status', '').lower() or 'season'}): "
            + ", ".join(prog_parts)
            + " — same week number"
        )

    if lines:
        attribution = deliveries.get("attribution")
        if attribution:
            lines.append(f"      {attribution}")
    return lines


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
            soy_inr = dom_india.get("soybean_mandi_inr")
            premium = dom_india.get("bean_premium_usd")
            if soy_inr:
                soy_usd = dom_india.get("soybean_mandi_usd")
                lines.append(
                    f"    Mandi Soybean (MP): ₹{soy_inr:,.0f}/MT"
                    + (f" / ${soy_usd:,.1f}/MT" if soy_usd else "")
                )
            if premium is not None:
                lines.append(f"    vs CBOT Beans: {premium:+.1f} USD/MT")

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
            farmgate = dom_brazil.get("conab_farmgate_brl")
            if farmgate:
                wedge = dom_brazil.get("cepea_vs_farmgate_pct")
                wedge_str = (
                    f" (CEPEA {wedge:+.1f}% vs farmgate)" if wedge is not None else ""
                )
                lines.append(f"    CONAB PR Farmgate: R${farmgate:,.2f}/MT{wedge_str}")
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

        lines.extend(_format_sa_deliveries(info))

    india_section = _format_india_domestic(countries)
    if india_section:
        lines.append("")
        lines.append(india_section)

    return "\n".join(lines)
