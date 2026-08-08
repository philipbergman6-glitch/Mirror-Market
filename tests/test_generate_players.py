"""Tests for scripts/generate_players.py (issue #123 — players.html)."""

import datetime as dt

import pandas as pd

from scripts.generate_players import (
    COUNTRY_NAMES,
    build_card,
    build_filter_options,
    display_name,
    export_sales_trend,
    group_cards,
    load_players,
    psd_numbers,
    render_players_html,
    weather_anomalies,
)

TODAY = dt.date(2026, 8, 8)


def make_entry(**overrides) -> dict:
    entry = {
        "name": "Test Co",
        "scope": "brazil",
        "country": "BR",
        "roles": ["exporter", "crusher"],
        "products": ["beans", "meal"],
        "ownership": "Private",
        "footprint": "Santos terminal",
        "destinations": "China-dominant",
        "destinations_structured": [
            {"code": "CN", "products": ["beans"], "note": "dominant"},
            {"code": "EU", "products": ["meal"]},
        ],
        "size_evidence": "Trase 2022 top-3",
        "confidence": "observed",
        "as_of": "2026-08-07",
        "citations": [{"url": "https://example.com", "accessed": "2026-08-07", "supports": "all"}],
        "notes": "",
    }
    entry.update(overrides)
    return entry


# ---------------------------------------------------------------------------
# Cards
# ---------------------------------------------------------------------------
def test_card_carries_filter_attributes():
    card = build_card(make_entry(tier=1, side="seller"), TODAY)
    assert card["country"] == "BR"
    assert card["tier"] == 1
    assert set(card["roles"]) == {"exporter", "crusher"}
    assert card["products"] == ["beans", "meal"]
    assert card["dest_codes"] == ["CN", "EU"]


def test_card_without_optional_blocks():
    card = build_card(make_entry(), TODAY)
    assert card["tier"] == 2
    assert card["dest_codes"] == ["CN", "EU"]
    assert card["contacts"] is None
    assert card["activity"] == []
    assert card["activity_stale"] is False
    assert card["contacts_stale"] is False


def test_activity_latest_three_newest_first():
    activity = [
        {"date": f"2026-0{m}-01", "category": "trade", "headline": f"h{m}",
         "citation": {"url": "u", "accessed": "2026-08-08"}}
        for m in (1, 3, 2, 5)
    ]
    card = build_card(make_entry(tier=1, activity=activity), TODAY)
    assert [a["headline"] for a in card["activity"]] == ["h5", "h3", "h2"]
    assert card["activity"][0]["date"] == "2026-05-01"


def test_activity_staleness_flags_two_quarters():
    fresh = [{"date": "2026-05-01", "category": "trade", "headline": "x",
              "citation": {"url": "u", "accessed": "2026-08-08"}}]
    stale = [{"date": "2025-12-01", "category": "trade", "headline": "x",
              "citation": {"url": "u", "accessed": "2026-08-08"}}]
    assert build_card(make_entry(tier=1, activity=fresh), TODAY)["activity_stale"] is False
    assert build_card(make_entry(tier=1, activity=stale), TODAY)["activity_stale"] is True


def test_contacts_staleness_flags_twelve_months():
    fresh = {"website": "https://x", "accessed": "2026-01-01"}
    stale = {"website": "https://x", "accessed": "2025-07-01"}
    assert build_card(make_entry(contacts=fresh), TODAY)["contacts_stale"] is False
    assert build_card(make_entry(contacts=stale), TODAY)["contacts_stale"] is True


def test_contacts_without_accessed_not_flagged():
    card = build_card(make_entry(contacts={"website": "https://x"}), TODAY)
    assert card["contacts_stale"] is False


# ---------------------------------------------------------------------------
# Grouping + filters
# ---------------------------------------------------------------------------
def test_groups_global_first_then_alphabetical_tier1_leads():
    cards = [
        build_card(make_entry(name="Zeta BR", country="BR"), TODAY),
        build_card(make_entry(name="Alpha BR", country="BR", tier=1), TODAY),
        build_card(make_entry(name="US Co", country="US"), TODAY),
        build_card(make_entry(name="ADM", country="GLOBAL", scope="global", tier=1), TODAY),
        build_card(make_entry(name="AR Co", country="AR"), TODAY),
    ]
    groups = group_cards(cards)
    assert [g["code"] for g in groups] == ["GLOBAL", "AR", "BR", "US"]
    br = next(g for g in groups if g["code"] == "BR")
    assert [c["name"] for c in br["cards"]] == ["Alpha BR", "Zeta BR"]


def test_filter_options_reflect_dataset():
    cards = [
        build_card(make_entry(), TODAY),
        build_card(make_entry(name="B", country="US", roles=["importer"],
                              products=["oil"], tier=1,
                              destinations_structured=[{"code": "MENA", "products": ["oil"]}]), TODAY),
    ]
    opts = build_filter_options(cards)
    assert opts["roles"] == ["crusher", "exporter", "importer"]
    assert opts["products"] == ["beans", "meal", "oil"]
    assert [o["code"] for o in opts["origins"]] == ["BR", "US"]
    assert {o["code"] for o in opts["destinations"]} == {"CN", "EU", "MENA"}


def test_real_dataset_loads_and_every_country_named():
    players = load_players()
    assert len(players) == 193
    cards = [build_card(e, TODAY) for e in players]
    for card in cards:
        assert display_name(card["country"]) != card["country"] or card["country"] in COUNTRY_NAMES
    codes = {c["country"] for c in cards} | {d for c in cards for d in c["dest_codes"]}
    for code in codes:
        # every grouping/filter label must resolve to a human name
        assert display_name(code), code


# ---------------------------------------------------------------------------
# Country context blocks (DB-derived, pure functions on DataFrames)
# ---------------------------------------------------------------------------
def test_psd_numbers_latest_year_nonzero_top_two():
    df = pd.DataFrame([
        # older year must be ignored
        {"country": "China", "year": 2025, "attribute": "Imports", "value": 90000, "unit": "(1000 MT)"},
        {"country": "China", "year": 2026, "attribute": "Imports", "value": 112000, "unit": "(1000 MT)"},
        {"country": "China", "year": 2026, "attribute": "Production", "value": 20000, "unit": "(1000 MT)"},
        {"country": "China", "year": 2026, "attribute": "Exports", "value": 0, "unit": "(1000 MT)"},
        {"country": "Brazil", "year": 2026, "attribute": "Exports", "value": 105000, "unit": "(1000 MT)"},
    ])
    items = psd_numbers(df, "CN")
    assert len(items) == 2  # zero exports omitted, capped at two
    assert items[0]["label"].startswith("Soybean imports")
    assert "112.0" in items[0]["value"]
    assert "2026" in items[0]["note"]


def test_psd_numbers_nan_value_omitted():
    # NULL value column must not render "nan MMT" (spec: never fabricate)
    df = pd.DataFrame([
        {"country": "Brazil", "year": 2026, "attribute": "Exports", "value": float("nan"), "unit": "(1000 MT)"},
        {"country": "Brazil", "year": 2026, "attribute": "Production", "value": 186000, "unit": "(1000 MT)"},
    ])
    items = psd_numbers(df, "BR")
    assert len(items) == 1
    assert items[0]["label"] == "Soybean production"


def test_psd_numbers_unknown_country_empty():
    df = pd.DataFrame([{"country": "Brazil", "year": 2026, "attribute": "Exports",
                        "value": 105000, "unit": "(1000 MT)"}])
    assert psd_numbers(df, "EG") == []
    assert psd_numbers(pd.DataFrame(), "BR") == []


def test_export_sales_trend_sums_recent_weeks():
    weeks = pd.date_range("2026-05-28", periods=8, freq="7D")
    rows = []
    for i, w in enumerate(weeks):
        # older 4 weeks 100k each, recent 4 weeks 200k each
        rows.append({"commodity": "Soybeans", "week_ending": w.strftime("%Y-%m-%d"),
                     "country": "CHINA, PEOPLES REPUBLIC OF", "net_sales": 200_000 if i >= 4 else 100_000})
        rows.append({"commodity": "Soybeans", "week_ending": w.strftime("%Y-%m-%d"),
                     "country": "JAPAN", "net_sales": 50_000})
    item = export_sales_trend(pd.DataFrame(rows), "CN")
    assert item is not None
    assert "0.80" in item["value"]          # 4 × 200k = 0.80 MMT
    assert "+100" in item["note"]           # vs 0.40 MMT prior → +100%
    assert item["cls"] == ""


def test_export_sales_trend_negative_flagged():
    df = pd.DataFrame([
        {"commodity": "Soybeans", "week_ending": "2026-07-23",
         "country": "CHINA, PEOPLES REPUBLIC OF", "net_sales": -50_000},
    ])
    item = export_sales_trend(df, "CN")
    assert item["cls"] == "warn"


def test_export_sales_trend_no_match_omitted():
    df = pd.DataFrame([{"commodity": "Soybeans", "week_ending": "2026-07-23",
                        "country": "JAPAN", "net_sales": 1.0}])
    assert export_sales_trend(df, "GT") is None
    assert export_sales_trend(pd.DataFrame(), "CN") is None


def _weather_df(region: str, temps: list[float]) -> pd.DataFrame:
    dates = pd.date_range("2026-05-01", periods=len(temps), freq="D")
    return pd.DataFrame({
        "region": region, "Date": dates,
        "temperature_2m_max": temps,
        "precipitation": [5.0] * len(temps),
        "is_forecast": 0,
    })


def test_weather_anomalies_flags_heat_breach():
    # 89 flat days then a big spike → temp z-score breach
    df = _weather_df("Brazil Mato Grosso", [30.0 + (i % 3) * 0.5 for i in range(89)] + [45.0])
    items = weather_anomalies({"Brazil Mato Grosso": df}, "BR")
    assert items
    assert items[0]["cls"] == "alert"
    assert "σ" in items[0]["value"]


def test_weather_anomalies_calm_is_information():
    df = _weather_df("Brazil Mato Grosso", [30.0 + (i % 5) * 0.5 for i in range(90)])
    items = weather_anomalies({"Brazil Mato Grosso": df}, "BR")
    assert items  # always-on: calm z-scores still render
    assert items[0]["cls"] == ""


def test_weather_anomalies_non_weather_country_empty():
    assert weather_anomalies({}, "EG") == []


# ---------------------------------------------------------------------------
# Template smoke test — real dataset, no DB
# ---------------------------------------------------------------------------
def test_render_smoke_real_dataset():
    players = load_players()
    cards = [build_card(e, TODAY) for e in players]
    groups = group_cards(cards)
    html = render_players_html(
        groups=groups,
        filters=build_filter_options(cards),
        contexts={},  # DB empty → every context block omitted
        generated_at="2026-08-08 12:00 UTC",
        day_line="Saturday · 8 August 2026",
    )
    assert "players-grid" in html or "player-card" in html
    assert "Bunge Global SA (incl. Viterra)" in html
    assert html.count("player-card") >= 193
    # spec decision #13: DB empty → context blocks omitted, never fabricated
    assert 'class="ctx-item' not in html
    # facet tokens are |-joined so multi-word roles ("grain trader (reducing)")
    # survive the client-side split — space-joining broke the role filter
    assert 'data-roles="originator|' in html
