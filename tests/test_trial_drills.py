"""The five failure drills, and the two properties that make them safe to run.

Requirement 9 asks for rehearsals of a critical source outage, partial key
coverage, a stale payload, a page-generation failure and a deployment failure —
each verifying that degradation, alerts and last-good-edition behaviour work.

These are rehearsals against the *real* grading code, which is the only way they
prove anything: a drill that asserted against a mock would pass forever after
``main._finalize_layer`` changed. That makes two safety properties load-bearing,
and both are tested here. A drill must not write a failure into the real
freshness table — a rehearsal that took the product down to prove the product
goes down is not a rehearsal — and it must not leave anything in ``docs/``,
where the next deploy would publish it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from analysis.trial.domain import TrialError
from analysis.trial.drills import (
    DRILLS,
    DrillResult,
    drill_critical_source_outage,
    drill_deployment_failure,
    drill_page_generation_failure,
    drill_partial_key_coverage,
    drill_stale_payload,
    run_all_drills,
    run_drill,
)

REPO = Path(__file__).resolve().parents[1]


def test_the_protocol_defines_the_five_drills_the_brief_asked_for() -> None:
    assert set(DRILLS) == {
        "critical_source_outage",
        "partial_key_coverage",
        "stale_payload",
        "page_generation_failure",
        "deployment_failure",
    }


def test_a_critical_source_going_dark_grades_as_a_hard_failure() -> None:
    result = drill_critical_source_outage()
    assert result.passed, result.observed
    assert "failed" in result.observed


def test_partial_coverage_distinguishes_usable_from_demoted() -> None:
    # The distinction is the whole point: above the key floor the layer is still
    # usable and grades success, but the coverage pair must record what actually
    # came back rather than a fetcher self-reporting fourteen-of-fourteen.
    result = drill_partial_key_coverage()
    assert result.passed, result.observed
    assert "incomplete" in result.observed


def test_a_frozen_upstream_grades_stale_and_holds_last_success_back() -> None:
    result = drill_stale_payload()
    assert result.passed, result.observed
    assert "stale" in result.observed


def test_a_failed_page_leaves_a_tombstone_the_promotion_contract_rejects() -> None:
    result = drill_page_generation_failure()
    assert result.passed, result.observed
    assert "tombstone" in result.observed.lower()


def test_a_blocked_deploy_keeps_the_last_trustworthy_edition_live() -> None:
    result = drill_deployment_failure()
    assert result.passed, result.observed


def test_all_five_drills_pass_against_the_current_grading_code() -> None:
    results = run_all_drills()
    assert len(results) == 5
    failures = [r for r in results if not r.passed]
    assert not failures, [(r.drill, r.observed) for r in failures]


def test_every_drill_records_what_it_expected_and_what_it_saw() -> None:
    # A drill result that only says "pass" is unreadable six weeks later, and a
    # trader is meant to be shown the degraded product and asked what they can
    # tell — which needs the prompt.
    for result in run_all_drills():
        assert result.simulated.strip()
        assert result.expected.strip()
        assert result.observed.strip()
        assert result.trader_prompt.strip()
        assert result.verdict == "pass"


def test_a_drill_result_missing_its_narrative_is_refused() -> None:
    for field in ("simulated", "expected", "observed"):
        kwargs = {"simulated": "s", "expected": "e", "observed": "o"}
        kwargs[field] = "  "
        with pytest.raises(TrialError, match=field):
            DrillResult(drill="d", title="t", passed=True, **kwargs)  # type: ignore[arg-type]


# --- the two safety properties -------------------------------------------
def test_no_drill_writes_a_failure_into_the_real_freshness_table(monkeypatch) -> None:
    # A rehearsal that marked the real `prices` layer failed would take the whole
    # product down to prove that the product goes down.
    import main

    written: list[object] = []
    monkeypatch.setattr(main, "save_freshness", lambda *a, **k: written.append((a, k)))
    run_all_drills()
    assert written == []


def test_the_drills_leave_mains_failure_sets_clean_for_the_next_caller() -> None:
    # They are read together with the freshness row, so a leftover entry from a
    # previous drill would make the next one lie.
    import main

    run_all_drills()
    for name in ("prices", "weather", "safex"):
        assert name not in main._HARD_FAILURES
        assert name not in main._STALE_LAST_KNOWN_GOOD
        assert name not in main._INCOMPLETE_KEY_COVERAGE


def test_no_drill_writes_anything_into_the_published_directory() -> None:
    docs = REPO / "docs"
    before = {p for p in docs.rglob("*") if p.is_file()}
    run_all_drills()
    after = {p for p in docs.rglob("*") if p.is_file()}
    assert before == after


def test_the_tombstone_drill_writes_to_a_temporary_directory_not_the_repo(tmp_path: Path) -> None:
    drill_page_generation_failure(output_dir=tmp_path)
    written = list(tmp_path.rglob("*.html"))
    assert written
    assert all(REPO / "docs" not in p.parents for p in written)


def test_a_stale_drill_against_a_layer_with_no_budget_refuses_rather_than_asserting_nothing() -> None:
    # psd is keyed by marketing year and deliberately carries no age budget, so
    # a drill there would assert a check that does not exist.
    with pytest.raises(TrialError, match="LAYER_MAX_DATA_AGE_DAYS"):
        drill_stale_payload("psd")


def test_an_unknown_drill_name_lists_the_ones_that_exist() -> None:
    with pytest.raises(TrialError, match="unknown drill"):
        run_drill("unplug_the_router")


def test_a_crashing_drill_is_reported_as_a_failed_drill_not_a_lost_one(monkeypatch) -> None:
    # "The rehearsal crashed" is itself a finding about the degradation path.
    import analysis.trial.drills as drills_module

    def boom() -> DrillResult:
        raise RuntimeError("simulated drill crash")

    monkeypatch.setitem(drills_module.DRILLS, "stale_payload", boom)
    results = {r.drill: r for r in run_all_drills()}
    assert not results["stale_payload"].passed
    assert "simulated drill crash" in results["stale_payload"].observed
    assert len(results) == 5


def test_a_drill_result_serialises_for_the_private_record() -> None:
    payload = drill_critical_source_outage().to_dict()
    assert payload["verdict"] == "pass"
    assert payload["evidence"]
