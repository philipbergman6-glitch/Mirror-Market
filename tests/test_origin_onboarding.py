"""The onboarding contract: set-level validation, and route readiness.

Every number here is a fixture. Nothing in this file may be copied into
``data/reference/assumptions/`` — a test freight rate that reached a shipped
file would be a fabricated default with a test's credibility behind it, which
is worse than one with none.

Two things are under test:

* **validation** — the faults a single entry cannot see about itself: an
  ambiguous pair, a scope too wide to mean anything, a key that matches
  nothing, a window that has already sailed.
* **readiness** — what a route still needs, resolved through the same lookup
  the calculation uses, so the checklist and the page cannot disagree.
"""

from __future__ import annotations

from datetime import date

import pytest
import yaml

from analysis.origins.assumptions import (
    Assumption,
    AssumptionError,
    AssumptionSet,
    load_assumptions,
)
from analysis.origins.domain import Confidence, CostComponent, ShipmentWindow
from analysis.origins.readiness import (
    STATUS_EXPIRED,
    STATUS_MISSING,
    STATUS_SATISFIED,
    assess_route,
    assess_routes,
    expiry_review,
    required_components,
)
from analysis.origins.validation import Severity, errors, structural_issues, validate_set

TODAY = date(2026, 8, 18)
OCT = ShipmentWindow(date(2026, 10, 1), date(2026, 10, 31), label="Oct 2026")
NOV = ShipmentWindow(date(2026, 11, 1), date(2026, 11, 30), label="Nov 2026")


def _assumption(component: str, value: float, unit: str, **kw) -> Assumption:
    return Assumption(
        id=kw.pop("id", f"{component}.{kw.get('origin') or 'any'}"),
        component=CostComponent(component),
        value=value,
        unit=unit,
        basis="fixture",
        source="test",
        entered_by="tests@example.com",
        entered_at=kw.pop("entered_at", date(2026, 8, 1)),
        expires_on=kw.pop("expires_on", date(2026, 12, 31)),
        confidence=Confidence.INDICATIVE,
        **kw,
    )


def _complete_route(origin: str = "br_paranagua", **overrides) -> AssumptionSet:
    """Every input one FOB-vessel leg into North China needs. Fixture values."""
    return AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin=origin, destination="cn_north", id=f"freight.{origin}"),
        _assumption("marine_insurance", 0.0012, "fraction", destination="cn_north"),
        _assumption("import_duty", 0.03, "fraction", destination="cn_north"),
        _assumption("import_vat", 0.09, "fraction", destination="cn_north"),
        _assumption("destination_port_costs", 6.0, "usd_per_mt", destination="cn_north"),
        _assumption("financing", 0.065, "rate_per_annum", destination="cn_north", days=45),
        _assumption("quality_adjustment", -3.0, "usd_per_mt",
                    origin=origin, destination="cn_north", id=f"quality.{origin}",
                    **overrides),
    ))


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def test_a_freight_with_no_origin_is_rejected_rather_than_applied_to_every_leg():
    wide = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt", destination="cn_north"),
    ))
    issue = errors(structural_issues(wide))
    assert [item.code for item in issue] == ["scope_too_wide"]
    assert "loading place" in issue[0].message


def test_a_destination_scoped_component_must_name_its_destination():
    wide = AssumptionSet(assumptions=(
        _assumption("destination_port_costs", 6.0, "usd_per_mt"),
    ))
    assert [item.code for item in errors(structural_issues(wide))] == ["scope_too_wide"]


def test_a_scope_key_that_matches_nothing_is_an_error_not_a_silent_miss():
    typo = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin="us-gulf", destination="cn_north"),
    ))
    codes = [item.code for item in errors(structural_issues(typo))]
    assert "unknown_scope_key" in codes


def test_two_overlapping_entries_of_the_same_scope_are_ambiguous():
    both = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", window=OCT, id="a"),
        _assumption("ocean_freight", 47.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", window=OCT, id="b"),
    ))
    issues = errors(structural_issues(both))
    assert [item.code for item in issues] == ["ambiguous_overlap"]
    assert set(issues[0].ids) == {"a", "b"}


def test_non_overlapping_windows_of_the_same_scope_are_not_ambiguous():
    sequential = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", window=OCT, id="a"),
        _assumption("ocean_freight", 47.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north", window=NOV, id="b"),
    ))
    assert errors(structural_issues(sequential)) == ()


def test_a_renewal_whose_lifetime_does_not_overlap_the_outgoing_entry_is_allowed():
    """The ordinary workflow: the old one lapses, the new one starts after it."""
    chained = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north",
                    entered_at=date(2026, 7, 1), expires_on=date(2026, 8, 1), id="old"),
        _assumption("ocean_freight", 47.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north",
                    entered_at=date(2026, 8, 2), expires_on=date(2026, 9, 2), id="new"),
    ))
    assert errors(structural_issues(chained)) == ()


def test_a_window_that_ended_before_the_entry_was_made_is_rejected():
    sailed = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt",
                    origin="br_paranagua", destination="cn_north",
                    window=ShipmentWindow(date(2025, 10, 1), date(2025, 10, 31)),
                    entered_at=date(2026, 8, 1)),
    ))
    assert [item.code for item in errors(structural_issues(sailed))] == ["window_already_sailed"]


def test_expiry_is_a_warning_not_an_error_because_the_record_is_the_audit_trail():
    lapsed = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt", origin="br_paranagua",
                    destination="cn_north", expires_on=date(2026, 8, 1)),
    ))
    issues = validate_set(lapsed, on=TODAY)
    assert [item.severity for item in issues] == [Severity.WARNING]
    assert issues[0].code == "expired"
    assert errors(structural_issues(lapsed)) == ()


def test_an_entry_lapsing_inside_the_horizon_is_reported_before_it_blocks():
    soon = AssumptionSet(assumptions=(
        _assumption("ocean_freight", 44.0, "usd_per_mt", origin="br_paranagua",
                    destination="cn_north", expires_on=date(2026, 8, 25)),
    ))
    codes = [item.code for item in validate_set(soon, on=TODAY, expiry_horizon_days=14)]
    assert codes == ["expiring"]


def test_the_loader_raises_on_an_unusable_file_rather_than_costing_a_route_with_it(tmp_path):
    (tmp_path / "bad.yml").write_text(
        yaml.safe_dump([{
            "id": "freight.everywhere",
            "component": "ocean_freight",
            "value": 44.0,
            "unit": "usd_per_mt",
            "destination": "cn_north",
            "basis": "fixture",
            "source": "test",
            "entered_by": "tests@example.com",
            "entered_at": "2026-08-01",
            "expires_on": "2026-12-31",
            "confidence": "indicative",
        }]),
        encoding="utf-8",
    )
    with pytest.raises(AssumptionError, match="scope_too_wide"):
        load_assumptions(tmp_path)


def test_the_shipped_assumption_directory_validates():
    """The committed files must load — a broken one blocks every route at once."""
    import config

    shipped = load_assumptions(config.ASSUMPTIONS_DIR)
    assert errors(structural_issues(shipped)) == ()


# ---------------------------------------------------------------------------
# Readiness
# ---------------------------------------------------------------------------
def test_the_us_gulf_leg_needs_elevation_and_the_fob_legs_do_not():
    """The bridge is derived from the delivery term, not listed per origin."""
    gulf = required_components("us_gulf")
    brazil = required_components("br_paranagua")
    assert CostComponent.ELEVATION in gulf
    assert CostComponent.ELEVATION not in brazil
    assert gulf[0] is CostComponent.ELEVATION  # the bridge comes before the stack
    assert brazil[0] is CostComponent.OCEAN_FREIGHT


def test_all_three_supported_routes_are_declared_into_north_china():
    routes = {route.leg_id: route for route in assess_routes(
        AssumptionSet(assumptions=()), window=OCT, today=TODAY, destination_key="cn_north"
    )}
    assert {"us_gulf", "br_paranagua", "ar_up_river"} <= set(routes)
    for leg_id in ("us_gulf", "br_paranagua", "ar_up_river"):
        assert routes[leg_id].destination_key == "cn_north"


def test_an_empty_set_names_every_input_the_route_needs_and_the_command_for_each():
    route = assess_route(
        AssumptionSet(assumptions=()),
        leg_id="us_gulf", destination_key="cn_north", window=OCT, today=TODAY,
    )
    assert not route.is_ready
    missing = {item.component for item in route.blocking}
    assert missing == set(required_components("us_gulf"))
    assert all(item.status == STATUS_MISSING for item in route.requirements)
    freight = next(i for i in route.requirements if i.component is CostComponent.OCEAN_FREIGHT)
    assert "--component ocean_freight" in freight.command
    assert "--origin us_gulf" in freight.command
    assert "--destination cn_north" in freight.command
    assert "--window 2026-10-01:2026-10-31" in freight.command


def test_no_requirement_ever_proposes_a_value():
    """A pre-filled number is a fabricated default with an extra step."""
    for route in assess_routes(AssumptionSet(assumptions=()), window=OCT, today=TODAY):
        for requirement in route.requirements:
            assert "--value <VALUE>" in requirement.command


def test_a_financing_requirement_asks_for_the_carry_period_too():
    route = assess_route(
        AssumptionSet(assumptions=()),
        leg_id="br_paranagua", destination_key="cn_north", window=OCT, today=TODAY,
    )
    financing = next(i for i in route.requirements if i.component is CostComponent.FINANCING)
    assert financing.unit == "rate_per_annum"
    assert "--days <CARRY_DAYS>" in financing.command


def test_a_complete_route_reads_ready_and_names_the_owner_of_every_input():
    route = assess_route(
        _complete_route(),
        leg_id="br_paranagua", destination_key="cn_north", window=OCT, today=TODAY,
    )
    assert route.is_ready
    assert route.blocking == ()
    assert all(item.status == STATUS_SATISFIED for item in route.requirements)
    assert all(item.entered_by == "tests@example.com" for item in route.requirements)
    assert all(item.expires_on is not None for item in route.requirements)


def test_an_input_lapsing_inside_the_horizon_is_expiring_not_yet_blocking():
    soon = AssumptionSet(assumptions=tuple(
        item if item.component is not CostComponent.OCEAN_FREIGHT
        else _assumption("ocean_freight", 44.0, "usd_per_mt", origin="br_paranagua",
                         destination="cn_north", expires_on=date(2026, 8, 24))
        for item in _complete_route().assumptions
    ))
    route = assess_route(
        soon, leg_id="br_paranagua", destination_key="cn_north", window=OCT, today=TODAY,
    )
    assert route.is_ready  # still costable today...
    assert [item.component for item in route.expiring] == [CostComponent.OCEAN_FREIGHT]
    assert route.expiring[0].days_to_expiry == 6


def test_an_expired_input_blocks_and_still_names_who_has_to_renew_it():
    lapsed = AssumptionSet(assumptions=tuple(
        item if item.component is not CostComponent.OCEAN_FREIGHT
        else _assumption("ocean_freight", 44.0, "usd_per_mt", origin="br_paranagua",
                         destination="cn_north", expires_on=date(2026, 8, 1))
        for item in _complete_route().assumptions
    ))
    route = assess_route(
        lapsed, leg_id="br_paranagua", destination_key="cn_north", window=OCT, today=TODAY,
    )
    assert not route.is_ready
    freight = next(i for i in route.requirements if i.component is CostComponent.OCEAN_FREIGHT)
    assert freight.status == STATUS_EXPIRED
    assert freight.entered_by == "tests@example.com"
    assert freight.expires_on == date(2026, 8, 1)


def test_a_leg_with_no_price_series_says_so_rather_than_listing_inputs_to_enter():
    route = assess_route(
        _complete_route(origin="us_pnw"),
        leg_id="us_pnw", destination_key="cn_north", window=OCT, today=TODAY,
    )
    assert not route.is_ready
    assert route.unavailable_reason and "no PNW price series" in route.unavailable_reason


def test_readiness_needs_no_database():
    """The checklist is answerable on a clone with no data at all."""
    routes = assess_routes(AssumptionSet(assumptions=()), window=OCT, today=TODAY)
    assert routes  # built entirely from config and the entered set


# ---------------------------------------------------------------------------
# The renewal queue
# ---------------------------------------------------------------------------
def test_the_review_reports_what_lapses_when_and_what_goes_dark_with_it():
    soon = AssumptionSet(assumptions=tuple(
        item if item.component is not CostComponent.OCEAN_FREIGHT
        else _assumption("ocean_freight", 44.0, "usd_per_mt", origin="br_paranagua",
                         destination="cn_north", expires_on=date(2026, 8, 24), id="freight.br")
        for item in _complete_route().assumptions
    ))
    review = expiry_review(soon, today=TODAY, horizon_days=30, window=OCT)
    assert not review["nothing_due"]
    lapsing = {row["id"]: row for row in review["expiring"]}
    assert lapsing["freight.br"]["days_to_expiry"] == 6
    assert lapsing["freight.br"]["routes_blocked"] == ["br_paranagua"]
    assert lapsing["freight.br"]["entered_by"] == "tests@example.com"


def test_an_entry_shadowed_by_a_more_specific_one_takes_no_route_down_with_it():
    """Which routes lapse is resolved by the real lookup, never by matching scope."""
    shadowed = AssumptionSet(assumptions=(
        *_complete_route().assumptions,
        _assumption("financing", 0.05, "rate_per_annum", origin="br_paranagua",
                    destination="cn_north", days=45, id="financing.br",
                    expires_on=date(2027, 6, 30)),
    ))
    # The destination-wide financing entry is the one about to lapse, but the
    # Brazil route reads the origin-scoped one, so nothing of Brazil's goes dark.
    aging = AssumptionSet(assumptions=tuple(
        item if item.id != "financing.any"
        else _assumption("financing", 0.065, "rate_per_annum", destination="cn_north",
                         days=45, id="financing.any", expires_on=date(2026, 8, 25))
        for item in shadowed.assumptions
    ))
    review = expiry_review(aging, today=TODAY, horizon_days=30, window=OCT)
    rows = {row["id"]: row for row in review["expiring"]}
    assert "br_paranagua" not in rows["financing.any"]["routes_blocked"]


def test_a_set_with_nothing_lapsing_reports_clear():
    review = expiry_review(_complete_route(), today=TODAY, horizon_days=7, window=OCT)
    assert review["nothing_due"] is True
    assert review["expired"] == [] and review["expiring"] == []
