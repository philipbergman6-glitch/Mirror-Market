"""The place registry and the leg → place map (cyclone hazard flags, slice 2).

Spec: ``docs/specs/cyclone-hazard-flags.md`` §4. A hazard flag attaches to a
rendered ledger leg through the places that price it, so the map from leg to
place is load-time validated exactly like the ledger itself: a typo, an
orphaned place or a leg with no declared places fails ``load_markets()`` and
never renders as a silent "clear".

Every test here is offline and touches no database.
"""

from __future__ import annotations

from datetime import date

import pytest

import config
from app.markets import ledger_leg_id_for, load_markets
from app.origins_page import build_view
from tests.test_origins_page_operational import TODAY as ORIGINS_TODAY
from tests.test_origins_page_operational import _render as render_origins
from tests.test_origins_page_operational import db  # noqa: F401 — fixture


def _reload(monkeypatch, **overrides):
    for name, value in overrides.items():
        monkeypatch.setattr(config, name, value)
    return load_markets()


def test_every_ledger_leg_declares_its_places():
    """The completeness test: one entry per ledger leg, no more, no fewer."""
    assert set(config.LEG_PLACES) == set(config.LEDGER_LEGS)


def test_a_rendered_leg_carries_its_places():
    """K1 §3, ports only. Builders read ``leg.place_ids``, never the config dict."""
    legs = {
        leg.leg_id: leg.place_ids
        for market in load_markets().values()
        if market.ledger is not None
        for leg in market.ledger.legs
    }
    assert legs["cbot:board"] == ("P-CBOT", "P-NOLA")
    assert legs["us_gulf:cif"] == ("P-NOLA",)
    assert legs["dalian:board"] == ("P-NCN", "P-PNG", "P-NOLA")
    assert legs["south_africa:safex"] == ("P-RFT", "P-DUR")
    assert legs["brazil:cepea"] == ()
    assert legs["india:mandi_mh"] == ()


# ---------------------------------------------------------------------------
# Empty legs — an empty tuple is a statement, so it must carry its reason
# ---------------------------------------------------------------------------
def test_an_empty_leg_needs_a_reason(monkeypatch):
    reasons = {k: v for k, v in config.LEG_PLACES_ABSENT_REASONS.items() if k != "brazil:cepea"}
    with pytest.raises(ValueError, match="brazil:cepea"):
        _reload(monkeypatch, LEG_PLACES_ABSENT_REASONS=reasons)


def test_a_blank_reason_is_no_reason(monkeypatch):
    reasons = {**config.LEG_PLACES_ABSENT_REASONS, "brazil:cepea": "   "}
    with pytest.raises(ValueError, match="brazil:cepea"):
        _reload(monkeypatch, LEG_PLACES_ABSENT_REASONS=reasons)


def test_a_leg_with_places_must_not_also_carry_a_reason(monkeypatch):
    reasons = {**config.LEG_PLACES_ABSENT_REASONS, "us_gulf:cif": "contradicts its own places"}
    with pytest.raises(ValueError, match="us_gulf:cif"):
        _reload(monkeypatch, LEG_PLACES_ABSENT_REASONS=reasons)


def test_a_reason_for_a_leg_that_does_not_exist_raises(monkeypatch):
    reasons = {**config.LEG_PLACES_ABSENT_REASONS, "czce:rapeseed_oil": "not a ledger leg"}
    with pytest.raises(ValueError, match="czce:rapeseed_oil"):
        _reload(monkeypatch, LEG_PLACES_ABSENT_REASONS=reasons)


# ---------------------------------------------------------------------------
# Ids — a typo in either id space fails the build, in both directions
# ---------------------------------------------------------------------------
def test_unknown_leg_or_place_raises(monkeypatch):
    # A leg the ledger does not know.
    with pytest.raises(ValueError, match="typo:leg"):
        _reload(monkeypatch, LEG_PLACES={**config.LEG_PLACES, "typo:leg": ("P-NOLA",)})
    monkeypatch.undo()

    # A ledger leg the map forgot.
    places = {k: v for k, v in config.LEG_PLACES.items() if k != "argentina:fob"}
    with pytest.raises(ValueError, match="argentina:fob"):
        _reload(monkeypatch, LEG_PLACES=places)
    monkeypatch.undo()

    # A place id the registry does not hold.
    with pytest.raises(ValueError, match="P-SANTOS"):
        _reload(monkeypatch, LEG_PLACES={**config.LEG_PLACES, "brazil:paranagua": ("P-PNG", "P-SANTOS")})


def test_a_leg_naming_one_place_twice_raises(monkeypatch):
    with pytest.raises(ValueError, match="us_gulf:cif"):
        _reload(monkeypatch, LEG_PLACES={**config.LEG_PLACES, "us_gulf:cif": ("P-NOLA", "P-NOLA")})


def test_every_place_is_used_by_a_leg(monkeypatch):
    """The standing rule made structural: a place exists only where a rendered leg needs it."""
    used = {place for place_ids in config.LEG_PLACES.values() for place in place_ids}
    assert used == set(config.PLACES)

    orphan = {**config.PLACES, "P-MOS": {**config.PLACES["P-RFT"], "name": "Moselle", "short": "Moselle"}}
    with pytest.raises(ValueError, match="P-MOS"):
        _reload(monkeypatch, PLACES=orphan)


# ---------------------------------------------------------------------------
# Place field rules (spec §4.1)
# ---------------------------------------------------------------------------
def _with_place(place_id: str, **fields) -> dict:
    place = {**config.PLACES[place_id], **fields}
    for name, value in fields.items():
        if value is _DROP:
            del place[name]
    return {**config.PLACES, place_id: place}


_DROP = object()


def test_place_basin_rules(monkeypatch):
    """``none`` and a sourceless basin need a reason; a covered basin must not carry one."""
    # The shipped registry already reads this way — pin it so a later edit is deliberate.
    for place_id, place in config.PLACES.items():
        basin = place["basin"]
        sourceless = basin == "none" or config.CYCLONE_BASINS[basin]["source"] is None
        assert bool(place.get("basin_reason")) is sourceless, place_id

    with pytest.raises(ValueError, match="P-CBOT"):  # basin "none", reason removed
        _reload(monkeypatch, PLACES=_with_place("P-CBOT", basin_reason=_DROP))
    monkeypatch.undo()
    with pytest.raises(ValueError, match="P-PNG"):  # South Atlantic, reason blanked
        _reload(monkeypatch, PLACES=_with_place("P-PNG", basin_reason="  "))
    monkeypatch.undo()
    with pytest.raises(ValueError, match="P-NOLA"):  # covered basin, reason added
        _reload(monkeypatch, PLACES=_with_place("P-NOLA", basin_reason="should not be here"))


def test_the_inland_points_are_declared_not_exposed():
    """Spec §14.1: agency wind radii are valid only over water."""
    inland = {p for p, place in config.PLACES.items() if place["basin"] == "none"}
    assert inland == {"P-CBOT", "P-RFT", "P-IDR"}
    assert all(config.PLACES[p]["kind"] == "inland_pricing_point" for p in inland)


@pytest.mark.parametrize(
    ("fields", "match"),
    [
        ({"basin": "north_indian"}, "basin"),
        ({"kind": "growing_area"}, "kind"),
        ({"lat": 90.5}, "lat"),
        ({"lon": -180.5}, "lon"),
        ({"lat": None}, "lat"),
        ({"short": ""}, "short"),
        ({"name": _DROP}, "name"),
        ({"effective_to": _DROP}, "effective_to"),
        ({"effective_from": "1 March 2027"}, "ISO date"),
    ],
)
def test_a_place_that_breaks_a_field_rule_raises(monkeypatch, fields, match):
    with pytest.raises(ValueError, match=match) as excinfo:
        _reload(monkeypatch, PLACES=_with_place("P-NOLA", **fields))
    assert "P-NOLA" in str(excinfo.value)


def test_effective_from_after_effective_to_raises(monkeypatch):
    with pytest.raises(ValueError, match="P-NOLA"):
        _reload(
            monkeypatch,
            PLACES=_with_place("P-NOLA", effective_from="2027-03-01", effective_to="2027-02-28"),
        )


# ---------------------------------------------------------------------------
# Effective dates — K1 §4.5, the JSE reference-point move, exercised
# synthetically because nothing confirmed is entered today
# ---------------------------------------------------------------------------
def _reference_point_move(monkeypatch) -> None:
    """Randfontein closes 2027-02-28; a synthetic successor opens 2027-03-01."""
    old = {**config.PLACES["P-RFT"], "effective_to": "2027-02-28"}
    new = {**config.PLACES["P-RFT"], "name": "X-NEW", "short": "X-NEW", "effective_from": "2027-03-01"}
    monkeypatch.setattr(config, "PLACES", {**config.PLACES, "P-RFT": old, "X-NEW": new})
    monkeypatch.setattr(
        config, "LEG_PLACES", {**config.LEG_PLACES, "south_africa:safex": ("P-RFT", "X-NEW", "P-DUR")}
    )


@pytest.mark.parametrize("today", [date(2027, 2, 28), date(2027, 3, 1)])
def test_a_reference_point_move_is_a_data_edit_valid_on_both_sides(monkeypatch, today):
    _reference_point_move(monkeypatch)
    safex = load_markets(today=today)["south_africa"].ledger.own
    assert safex.place_ids == ("P-RFT", "X-NEW", "P-DUR")


def test_a_leg_with_no_place_active_today_raises(monkeypatch):
    """Rule 7: a closed place with no successor would silently drop the leg off every surface."""
    places = _with_place("P-NOLA", effective_to="2027-02-28")
    monkeypatch.setattr(config, "PLACES", places)
    assert load_markets(today=date(2027, 2, 28))  # still open on its last day
    with pytest.raises(ValueError, match="us_gulf:cif"):
        load_markets(today=date(2027, 3, 1))


def test_a_place_not_yet_open_does_not_count_as_active(monkeypatch):
    places = _with_place("P-NOLA", effective_from="2027-03-01")
    monkeypatch.setattr(config, "PLACES", places)
    with pytest.raises(ValueError, match="us_gulf:cif"):
        load_markets(today=date(2027, 2, 28))
    assert load_markets(today=date(2027, 3, 1))


# ---------------------------------------------------------------------------
# Origins — the explicit seam from an origin leg to its ledger leg (spec §7.3)
# ---------------------------------------------------------------------------
def test_origin_legs_resolve_to_ledger_legs():
    resolved = {
        origin: ledger_leg_id_for(leg["market"], leg["block"], leg["key"])
        for origin, leg in config.ORIGIN_LEGS.items()
        if not leg.get("absent_reason")
    }
    assert resolved == {
        "us_gulf": "us_gulf:cif",
        "br_paranagua": "brazil:paranagua",
        "ar_up_river": "argentina:fob",
    }


def test_a_triple_no_ledger_leg_reads_resolves_to_none():
    assert ledger_leg_id_for("cbot", "price", "Soybean Oil") is None


def test_the_pnw_row_gets_no_hazard(db):  # noqa: F811 — the imported fixture
    """K1 edge case (c): PNW is a declared-absent origin with no price, so no
    ledger leg, no place, and nothing for a flag to attach to. A flag needs a price.

    Two halves: the registry has nothing for it, and the rendered origins page
    carries no chip in its row even when every priced leg is flagged.
    """
    pnw = config.ORIGIN_LEGS["us_pnw"]
    assert pnw.get("absent_reason")
    assert not {"market", "block", "key"} & pnw.keys()
    assert not any("PNW" in p or "Pacific Northwest" in place["name"] for p, place in config.PLACES.items())

    flagged = {
        "state": "flag", "severity": "warning", "partial": False, "chip": "storm",
        "chip_class": "hz-warning", "text": "Synthetic (NHC): tropical-storm-force winds",
        "level": "ts_force", "storm": "Synthetic", "storm_id": "al992026", "source": "NHC",
        "first_arrival_h": 24, "first_arrival_at": "2026-08-19T00:00:00Z", "reason": None,
        "uncovered": [], "anchor": "markets/cbot.html#block-weather",
    }
    hazards = {leg_id: flagged for leg_id in ("us_gulf:cif", "brazil:paranagua", "argentina:fob")}
    view = build_view(db, today=ORIGINS_TODAY, hazards=hazards)
    soup = render_origins(view)
    decision = soup.select_one("#section-decision")
    assert decision is not None
    rows = {row["origin_key"]: row for row in view["views"][0]["decision"]["rows"]}
    assert "us_pnw" not in rows  # rendered from UnavailableOrigin, never _row_view
    assert all(rows[k]["hazard"] is flagged for k in ("us_gulf", "br_paranagua", "ar_up_river"))
    # One chip per flagged row, in every costed window the section renders.
    assert len(decision.select("a.hz-chip")) == 3 * len(view["views"])
    pnw_block = next(es for es in decision.select(".empty-state") if "PNW" in es.get_text())
    assert not pnw_block.select(".hz-chip")
    assert "hazard" not in next(u for u in view["views"][0]["decision"]["unavailable"]
                                if u["port"]["key"] == "us_pnw")


def test_a_priced_origin_leg_with_no_ledger_leg_fails_the_build(monkeypatch):
    origins = {
        **config.ORIGIN_LEGS,
        "us_gulf": {**config.ORIGIN_LEGS["us_gulf"], "key": "Soybean Meal"},
    }
    with pytest.raises(ValueError, match="us_gulf"):
        _reload(monkeypatch, ORIGIN_LEGS=origins)


def test_two_ledger_legs_on_one_triple_is_ambiguous(monkeypatch):
    legs = {**config.LEDGER_LEGS, "us_gulf:cif_copy": {**config.LEDGER_LEGS["us_gulf:cif"]}}
    monkeypatch.setattr(config, "LEDGER_LEGS", legs)
    with pytest.raises(ValueError, match="us_gulf:cif_copy"):
        ledger_leg_id_for("cbot", "basis", "Soybeans")


def test_a_basin_with_half_a_source_raises(monkeypatch):
    basins = {
        **config.CYCLONE_BASINS,
        "west_pacific": {**config.CYCLONE_BASINS["west_pacific"], "layer": None},
    }
    with pytest.raises(ValueError, match="west_pacific"):
        _reload(monkeypatch, CYCLONE_BASINS=basins)
