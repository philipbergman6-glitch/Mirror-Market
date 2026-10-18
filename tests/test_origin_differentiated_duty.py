"""B22 #485: a US-origin → China landed row never renders on the MFN duty alone.

China charges US-origin soybeans an additional duty on top of the MFN rate
every other origin pays. A destination-scoped MFN entry is therefore not an
answer for that leg at any specificity: the duty rung resolves only an entry
that names the origin, and with none live the row is withheld as
``origin_duty_unmodelled``. These tests pin both outcomes, and pin the shipped
policy file to them.
"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import date, timedelta

import pytest

import config
from analysis.origins.assumptions import (
    ORIGIN_DUTY_UNMODELLED,
    Assumption,
    AssumptionMiss,
    AssumptionSet,
    load_assumptions,
    origin_differentiated_duty,
)
from analysis.origins.domain import Carrier, Comparability, CostComponent, Incoterm
from analysis.origins.landed_cost import compute_landed_cost
from analysis.origins.readiness import assess_route
from tests.test_landed_cost import CN, GULF, SEP, TODAY, UP_RIVER, _assumption, _full_set, _quote


def _gulf_quote():
    return _quote(GULF, 466.1919, incoterm=Incoterm.CIF, carrier=Carrier.BARGE)


def _without_us_duty(*extra: Assumption) -> AssumptionSet:
    """The full stack minus the US-origin duty: only the MFN entry is left."""
    return AssumptionSet(assumptions=tuple(
        a for a in _full_set().assumptions if a.id != "duty.us"
    ) + extra)


def _duty_step(result):
    return next(s for s in result.steps if s.component is CostComponent.IMPORT_DUTY)


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------
def test_us_origin_into_china_is_registered_as_origin_differentiated():
    assert ("US", "CN") in config.ORIGIN_DIFFERENTIATED_IMPORT_DUTY
    # Keyed by country, not berth: PNW beans pay what Gulf beans pay.
    assert origin_differentiated_duty("us_gulf", "cn_north")
    assert origin_differentiated_duty("us_pnw", "cn_north")
    assert origin_differentiated_duty("br_paranagua", "cn_north") is None
    assert origin_differentiated_duty("ar_up_river", "cn_north") is None


@pytest.mark.parametrize("origin,destination", [
    (None, "cn_north"), ("us_gulf", None), ("us-gulf", "cn_north"), ("us_gulf", "cn_sooth"),
])
def test_an_unresolvable_route_raises_rather_than_reading_as_no_extra_duty(origin, destination):
    with pytest.raises(ValueError):
        origin_differentiated_duty(origin, destination)


# ---------------------------------------------------------------------------
# Outcome 1 — the origin-scoped duty is applied
# ---------------------------------------------------------------------------
def test_the_us_row_carries_the_origin_scoped_duty_not_mfn():
    result = compute_landed_cost(_gulf_quote(), CN, SEP, _full_set(), today=TODAY)
    duty = _duty_step(result)
    assert result.landed is not None
    assert duty.assumption_id == "duty.us"
    assert duty.rate == pytest.approx(0.13)


def test_other_origins_still_pay_the_destination_mfn_rate():
    result = compute_landed_cost(_quote(UP_RIVER, 452.0), CN, SEP, _full_set(), today=TODAY)
    assert _duty_step(result).rate == pytest.approx(0.03)


# ---------------------------------------------------------------------------
# Outcome 2 — withheld when the origin-scoped entry is absent or expired
# ---------------------------------------------------------------------------
def test_without_an_origin_scoped_entry_the_us_row_is_withheld_not_priced_at_mfn():
    result = compute_landed_cost(_gulf_quote(), CN, SEP, _without_us_duty(), today=TODAY)
    assert result.landed is None
    assert result.comparability is Comparability.MISSING_INPUT
    assert CostComponent.IMPORT_DUTY in result.missing_inputs
    blocker = next(b for b in result.blockers if b.component is CostComponent.IMPORT_DUTY)
    assert blocker.code == ORIGIN_DUTY_UNMODELLED
    assert "MFN rate is deliberately not used" in blocker.message
    assert all(s.component is not CostComponent.IMPORT_DUTY for s in result.steps)


def test_an_expired_origin_scoped_entry_withholds_and_mfn_is_not_substituted():
    lapsed = _assumption(
        "import_duty", 0.13, "fraction", origin="us_gulf", destination="cn_north",
        id="duty.us.lapsed", expires_on=TODAY - timedelta(days=1),
    )
    result = compute_landed_cost(
        _gulf_quote(), CN, SEP, _without_us_duty(lapsed), today=TODAY
    )
    assert result.landed is None
    blocker = next(b for b in result.blockers if b.component is CostComponent.IMPORT_DUTY)
    assert blocker.code == ORIGIN_DUTY_UNMODELLED
    assert "expired" in blocker.message and "MFN rate is not substituted" in blocker.message


def test_a_caveat_is_printed_on_the_rung_it_qualifies_even_under_override():
    caveated = AssumptionSet(assumptions=tuple(
        replace(a, caveat="holds only with a tariff exclusion") if a.id == "duty.us" else a
        for a in _full_set().assumptions
    ))
    plain = compute_landed_cost(_gulf_quote(), CN, SEP, caveated, today=TODAY)
    shocked = compute_landed_cost(
        _gulf_quote(), CN, SEP, caveated, today=TODAY,
        overrides={CostComponent.IMPORT_DUTY: 0.20},
    )
    for result in (plain, shocked):
        assert _duty_step(result).basis.endswith("— holds only with a tariff exclusion")
    # Only the rung it belongs to.
    vat = next(s for s in plain.steps if s.component is CostComponent.IMPORT_VAT)
    assert "tariff exclusion" not in vat.basis


def test_an_uncaveated_set_keeps_its_fingerprint():
    """`caveat` is serialised only when present, so stored set_ids stay stable."""
    assert all("caveat" not in a.to_dict() for a in _full_set().assumptions)
    with_one = replace(_full_set().assumptions[0], caveat="x")
    assert with_one.to_dict()["caveat"] == "x"


def test_a_scenario_override_cannot_unblock_a_withheld_us_duty():
    result = compute_landed_cost(
        _gulf_quote(), CN, SEP, _without_us_duty(), today=TODAY,
        overrides={CostComponent.IMPORT_DUTY: 0.03},
    )
    assert result.landed is None


def test_the_onboarding_checklist_agrees_and_asks_for_an_origin_scoped_entry():
    route = assess_route(
        _without_us_duty(), leg_id="us_gulf", destination_key="cn_north",
        window=SEP, today=TODAY,
    )
    duty = next(r for r in route.requirements if r.component is CostComponent.IMPORT_DUTY)
    assert duty.is_blocking and not route.is_ready
    assert "--origin us_gulf" in duty.command
    # Argentina's duty is the destination's own rate and stays destination-scoped.
    ar = assess_route(
        _without_us_duty(), leg_id="ar_up_river", destination_key="cn_north",
        window=SEP, today=TODAY,
    )
    ar_duty = next(r for r in ar.requirements if r.component is CostComponent.IMPORT_DUTY)
    assert not ar_duty.is_blocking
    assert "--origin" not in ar_duty.command


# ---------------------------------------------------------------------------
# The shipped policy file
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module")
def shipped() -> AssumptionSet:
    # The committed tier only: the public audience never reads private/.
    return load_assumptions(config.ASSUMPTIONS_DIR)


def test_the_shipped_file_never_prices_a_us_row_on_mfn_on_any_day(shipped):
    """Every day from first entry to a year past the last expiry: origin-scoped, or withheld."""
    start = min(a.entered_at for a in shipped.assumptions)
    end = max(a.expires_on for a in shipped.assumptions) + timedelta(days=365)
    day = start
    while day <= end:
        found = shipped.lookup(
            CostComponent.IMPORT_DUTY, origin="us_gulf", destination="cn_north",
            window=SEP, on=day,
        )
        if isinstance(found, AssumptionMiss):
            assert found.code == ORIGIN_DUTY_UNMODELLED, day
        else:
            assert found.origin == "us_gulf", (day, found.id)
        day += timedelta(days=1)


def test_every_shipped_policy_rate_cites_its_publisher_and_a_dated_check(shipped):
    assert shipped.assumptions
    for assumption in shipped.assumptions:
        assert "repo-established" not in assumption.source, assumption.id
        assert re.search(r"checked \d{4}-\d{2}-\d{2}", assumption.source), assumption.id
        assert assumption.citation and "https://" in assumption.citation, assumption.id


def test_the_shipped_us_duty_renders_with_its_section_301_qualification(shipped):
    """The 13% holds only with a Section 301 exclusion; the rung must say so."""
    desk = tuple(
        a for a in _full_set().assumptions if a.component is not CostComponent.IMPORT_DUTY
        and a.component is not CostComponent.IMPORT_VAT
    )
    stack = AssumptionSet(assumptions=shipped.assumptions + desk)
    on = date(2026, 10, 18)
    result = compute_landed_cost(_gulf_quote(), CN, SEP, stack, today=on)
    duty = _duty_step(result)
    assert duty.assumption_id == "import_duty.us_gulf.cn_north"
    assert duty.rate == pytest.approx(0.13)
    assert "Section 301 exclusion" in duty.basis and "40.5%" in duty.basis


def test_every_shipped_origin_differentiated_rate_carries_a_caveat(shipped):
    for assumption in shipped.assumptions:
        if assumption.origin and origin_differentiated_duty(
            assumption.origin, assumption.destination
        ):
            assert assumption.caveat, assumption.id


def test_the_shipped_us_duty_does_not_claim_a_number_its_cited_page_omits(shipped):
    us = next(a for a in shipped.assumptions if a.id == "import_duty.us_gulf.cn_north")
    assert "number not shown" in us.source
    assert "第10号" not in us.source + us.basis


def test_the_shipped_us_duty_expires_no_later_than_the_policy_it_records(shipped):
    us = [a for a in shipped.assumptions if a.origin == "us_gulf"]
    for assumption in us:
        assert assumption.component is CostComponent.IMPORT_DUTY
        assert assumption.expires_on <= date(2026, 11, 10), assumption.id
