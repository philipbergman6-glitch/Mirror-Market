"""The M14 #207 weather mapping, built by M24 #271.

What these pin, in order of how expensive the bug would be:

1. **The standing rule.** A pin exists because some rendered price leg is
   priced by it. A pin nothing renders is a fetch we pay for and never read;
   a leg with no pin is the weather event we miss.
2. **The key shapes agree.** ``GROWING_REGIONS`` is the catalog three other
   maps key off — the season calendar, the pod-fill months and the players
   page. A rename that lands in one and not the others reads as a weather
   outage for a region that is fetching fine.
3. **Out of season is a tag, never a silence.** The card renders either way.
"""

from __future__ import annotations

from datetime import date

import pytest

import config
from app.block_builders import out_of_season_note
from app.markets import load_markets


@pytest.fixture(scope="module")
def markets():
    return load_markets()


# ---------------------------------------------------------------------------
# The mapping itself
# ---------------------------------------------------------------------------
def test_every_pin_prices_a_rendered_leg(markets):
    """M14's standing rule, as a set equality — in both directions."""
    on_a_page = {region for m in markets.values() for region in m.weather_regions}
    on_the_strip = {
        region
        for belt in config.COMPETING_OIL_WEATHER_BELTS
        for region in belt["regions"]
    }
    rendered = on_a_page | on_the_strip
    assert rendered == set(config.GROWING_REGIONS), (
        "pins with no rendered leg: "
        f"{sorted(set(config.GROWING_REGIONS) - rendered)}; "
        f"legs with no pin: {sorted(rendered - set(config.GROWING_REGIONS))}"
    )


def test_the_regions_m14_dropped_are_gone():
    """Ivory Coast (cocoa relic), Jilin (corn/feed) and Thailand (covered by
    ID+MY). Their history rows stop accruing; that is the intended cost."""
    for region in ("Ivory Coast (Cocoa)", "China Jilin", "Thailand Surat Thani"):
        assert region not in config.GROWING_REGIONS


def test_the_paraguay_pin_sits_in_the_soy_belt_not_the_chaco():
    """The old pin (-22.35,-59.95) was in Boquerón — cattle country. The crop
    is in Alto Paraná–Itapúa–Canindeyú."""
    assert "Paraguay Chaco" not in config.GROWING_REGIONS
    pin = config.GROWING_REGIONS["Paraguay Alto Parana"]
    assert -27.0 < pin["lat"] < -24.0
    assert -57.0 < pin["lon"] < -54.0


def test_every_pin_has_plausible_coordinates():
    for region, pin in config.GROWING_REGIONS.items():
        assert -90 <= pin["lat"] <= 90, region
        assert -180 <= pin["lon"] <= 180, region


def test_dalian_carries_its_import_origin_and_says_so(markets):
    """China's No.2 and meal are priced by Brazilian weather (73.6% of 2025
    imports). The pin is in Brazil, so the label has to carry that."""
    roles = markets["dalian"].weather_roles
    assert "Brazil Mato Grosso" in roles
    assert "import origin" in roles["Brazil Mato Grosso"]
    assert "prices No.1" in roles["China Heilongjiang"]


def test_europe_has_weather_and_its_pins_say_rapeseed(markets):
    europe = markets["europe"]
    assert europe.weather_regions, "M14 filled Europe's empty weather block"
    assert "weather" not in europe.absent_reasons
    for role in europe.weather_roles.values():
        assert "rapeseed" in role.lower()


def test_argentina_labels_its_sunflower_and_paraguay_pins(markets):
    roles = markets["argentina"].weather_roles
    assert "sunflower" in roles["Argentina Buenos Aires (sunflower)"].lower()
    assert "Rosario" in roles["Paraguay Alto Parana"]


# ---------------------------------------------------------------------------
# Key-shape agreement
# ---------------------------------------------------------------------------
def test_the_season_calendar_covers_exactly_the_pins():
    """A pin with no calendar entry would render as in season all year."""
    assert set(config.WEATHER_GROWING_SEASON_MONTHS) == set(config.GROWING_REGIONS)


def test_season_months_are_real_months():
    for region, months in config.WEATHER_GROWING_SEASON_MONTHS.items():
        assert months, region
        assert set(months) <= set(range(1, 13)), region
        assert len(set(months)) == len(months), region


def test_pod_fill_months_key_off_live_regions():
    assert set(config.WEATHER_SOY_POD_FILL_MONTHS) <= set(config.GROWING_REGIONS)


def test_the_players_page_keys_off_live_regions():
    from scripts.generate_players import WEATHER_REGIONS_BY_ISO

    named = {r for regions in WEATHER_REGIONS_BY_ISO.values() for r in regions}
    assert named <= set(config.GROWING_REGIONS), sorted(named - set(config.GROWING_REGIONS))


def test_the_soy_analysts_key_off_live_regions():
    from analysis.soy_analytics import EMERGING_MARKET_WEATHER, SOY_WEATHER_REGIONS

    live = set(config.GROWING_REGIONS)
    assert set(SOY_WEATHER_REGIONS) <= live, sorted(set(SOY_WEATHER_REGIONS) - live)
    for country, regions in EMERGING_MARKET_WEATHER.items():
        assert set(regions) <= live, country


def test_the_competing_oil_strip_keys_off_live_regions():
    for belt in config.COMPETING_OIL_WEATHER_BELTS:
        assert set(belt["regions"]) <= set(config.GROWING_REGIONS), belt["belt"]
        assert belt["note"].strip()


def test_the_palm_line_states_its_yield_lag():
    palm = next(b for b in config.COMPETING_OIL_WEATHER_BELTS if b["belt"] == "Palm")
    assert "9–12 month" in palm["note"]


# ---------------------------------------------------------------------------
# The out-of-season tag
# ---------------------------------------------------------------------------
def test_out_of_season_names_the_planting_month():
    # January in Iowa: nothing in the ground, planting from May.
    assert out_of_season_note("US Midwest (Iowa)", date(2026, 1, 15)) == (
        "out of season — planting ~May"
    )
    # July in Iowa: pod fill. No tag.
    assert out_of_season_note("US Midwest (Iowa)", date(2026, 7, 15)) is None


def test_a_southern_hemisphere_season_wraps_the_year():
    # Brazil sows in October and harvests in Feb–Mar.
    assert out_of_season_note("Brazil Mato Grosso", date(2026, 1, 15)) is None
    assert out_of_season_note("Brazil Mato Grosso", date(2026, 7, 15)) == (
        "out of season — planting ~Oct"
    )


def test_perennial_and_overwintering_crops_are_never_tagged():
    for region in ("Indonesia Riau (Sumatra)", "France Champagne (Grand Est)"):
        for month in range(1, 13):
            assert out_of_season_note(region, date(2026, month, 1)) is None


def test_india_is_tagged_outside_the_kharif_window():
    assert out_of_season_note("India Madhya Pradesh", date(2026, 8, 1)) is None
    assert out_of_season_note("India Madhya Pradesh", date(2026, 2, 1)) == (
        "out of season — planting ~Jun"
    )
