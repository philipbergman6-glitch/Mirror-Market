"""The Opportunities page: what it builds, and what it must never carry.

The privacy tests at the bottom are the important ones. They render the real
template through the real builder against a workflow full of distinctive
strings, and grep the resulting HTML for every one — because the guarantee this
page makes is not "the template has an `if`", it is "the private data is not in
the context".
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pytest
import yaml
from jinja2 import Environment, FileSystemLoader
from opportunity_fixtures import (
    TODAY,
    seed_currency,
    seed_freshness,
    seed_weekly_flow,
)

import config
from analysis.opportunities import engine as engine_mod
from analysis.opportunities.domain import AUDIENCE_PRIVATE, AUDIENCE_PUBLIC, Ladder
from app.opportunities_page import SECTION_SPECS, build_view

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "app" / "templates"

#: Distinctive enough that a substring match cannot be a coincidence, and
#: shaped like the things a trader would actually type.
SENTINELS = {
    "owner": "ZZOWNER-Jan-Kowalski",
    "note": "ZZNOTE-they-only-want-Sep-and-will-pay-4-over",
    "next_action": "ZZACTION-call-back-Thursday-before-noon",
    "counterparty": "ZZPARTY-Acme-Crushing-Private-Limited",
    "feedback": "ZZFEEDBACK-they-are-already-covered-until-December",
    "audit": "ZZAUDIT-moved-to-negotiating-after-the-call",
}


@pytest.fixture
def board_db(tmp_db):
    """A database with enough in it that at least one rule fires."""
    seed_weekly_flow(tmp_db, last_week=TODAY - timedelta(days=2))
    seed_currency(tmp_db, pair="BRL/USD", last_date=TODAY - timedelta(days=1))
    seed_freshness(tmp_db, "crush_inspections")
    seed_freshness(tmp_db, "currencies")
    return tmp_db


def render(view: dict) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)
    return env.get_template("opportunities.html.j2").render(
        opportunities=view,
        root="",
        market_nav=[],
        current_page="opportunities",
        current_market=None,
        day_line="TUESDAY 18 AUGUST 2026",
        generated_at="2026-08-18 12:00 UTC",
        generated_at_iso="2026-08-18T12:00:00+00:00",
        production_layers=[],
    )


def workflow_dir(tmp_path, opportunity_id: str) -> str:
    (tmp_path / "desk.yml").write_text(yaml.safe_dump([{
        "opportunity_id": opportunity_id,
        "status": "negotiating",
        "owner": SENTINELS["owner"],
        "counterparty": SENTINELS["counterparty"],
        "next_action": SENTINELS["next_action"],
        "notes": [SENTINELS["note"]],
        "feedback": [{
            "kind": "contacted_no_interest",
            "recorded_on": "2026-08-17",
            "reason": SENTINELS["feedback"],
            "by": SENTINELS["owner"],
        }],
        "audit": [{"on": "2026-08-17", "by": SENTINELS["owner"], "what": SENTINELS["audit"]}],
    }]), encoding="utf-8")
    return str(tmp_path)


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------
def test_the_public_page_builds_and_renders(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    assert view["audience"] == AUDIENCE_PUBLIC
    assert view["is_private"] is False
    html = render(view)
    assert "<title>Mirror Market — Opportunities</title>" in html
    assert "PRIVATE EDITION" not in html


def test_public_and_private_have_different_section_sets():
    public_ids = [sid for sid, _, _, audiences in SECTION_SPECS if AUDIENCE_PUBLIC in audiences]
    private_ids = [sid for sid, _, _, audiences in SECTION_SPECS if AUDIENCE_PRIVATE in audiences]
    assert "workflow" not in public_ids
    assert "feedback" not in public_ids
    assert set(public_ids) < set(private_ids)


def test_sections_are_numbered_from_one_without_gaps(board_db):
    for audience in (AUDIENCE_PUBLIC, AUDIENCE_PRIVATE):
        view = build_view(board_db, today=TODAY, audience=audience, archive=False)
        numbers = [section["no"] for section in view["sections"]]
        assert numbers == [f"{n:02d}" for n in range(1, len(numbers) + 1)]


def test_every_empty_section_names_its_reason(tmp_db):
    """An empty database is the honest worst case, and must still explain itself."""
    view = build_view(tmp_db, today=TODAY, archive=False)
    for section in view["sections"]:
        if section["state"] != "ok":
            assert section["reason"].strip()
    board = next(s for s in view["sections"] if s["id"] == "board")
    assert board["state"] == "empty"
    assert "every rule ran and none of them fired" in board["reason"]
    render(view)  # must still render


def test_an_empty_board_says_when_a_rule_crashed(tmp_db, monkeypatch):
    from analysis.opportunities import signals as signals_mod

    def boom(conn, *, today):
        raise RuntimeError("upstream is on fire")

    monkeypatch.setattr(
        signals_mod, "DETECTORS", (("destination_flow_shift", boom, False),)
    )
    view = build_view(tmp_db, today=TODAY, archive=False)
    board = next(s for s in view["sections"] if s["id"] == "board")
    assert "did not run" in board["reason"]
    assert "our failure, not a quiet market" in board["reason"]


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
def test_every_filter_option_matches_at_least_one_row(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    board = next(s for s in view["sections"] if s["id"] == "board")
    assert board["state"] == "ok"
    rows = board["data"]["rows"]
    for facet, options in board["data"]["facets"].items():
        key = {
            "origin": "origin_iso", "destination": "destination_iso"
        }.get(facet, facet)
        for option in options:
            matching = [row for row in rows if row.get(key) == option["value"]]
            assert matching, f"{facet}={option['value']} matches nothing"
            assert len(matching) == option["count"]


def test_the_required_filters_are_all_offered(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    board = next(s for s in view["sections"] if s["id"] == "board")
    assert {"product", "origin", "destination", "role", "confidence", "status"} <= set(
        board["data"]["facets"]
    )


def test_filter_chips_reach_the_html(board_db):
    html = render(build_view(board_db, today=TODAY, archive=False))
    assert 'data-facet="product"' in html
    assert "data-opp" in html
    assert 'id="filter-clear"' in html


# ---------------------------------------------------------------------------
# Content the requirements name explicitly
# ---------------------------------------------------------------------------
def test_each_row_carries_why_now_evidence_blockers_and_a_next_action(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    rows = next(s for s in view["sections"] if s["id"] == "board")["data"]["rows"]
    for row in rows:
        assert row["why_now"].strip()
        assert row["evidence"]
        assert row["suggested_next_action"].strip()
        assert "score" in row and row["score"]["components"]


def test_the_detail_view_carries_lineage_and_sensitivity(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    cards = next(s for s in view["sections"] if s["id"] == "detail")["data"]["cards"]
    assert cards
    for card in cards:
        assert card["lineage"]
        for item in card["lineage"]:
            assert item["layer"] and item["table"] and item["observed_on"]
            assert item["budget_days"] > 0
        assert card["sensitivity"]["swings"]
        assert "no probability" in card["sensitivity"]["no_probability_note"].lower()


def test_the_ladder_section_states_all_five_rungs(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    ladder = next(s for s in view["sections"] if s["id"] == "ladder")["data"]
    assert [rung["key"] for rung in ladder["rungs"]] == [rung.value for rung in Ladder]
    reachable = {rung["key"] for rung in ladder["rungs"] if rung["detector_reachable"]}
    assert reachable == {"market_signal", "lead", "actionable"}


def test_the_method_section_publishes_every_formula(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    method = next(s for s in view["sections"] if s["id"] == "method")["data"]
    assert {entry["key"] for entry in method["formulas"]} == {
        "economic", "evidence", "freshness", "counterparty", "feasibility"
    }
    assert sum(entry["weight"] for entry in method["weights"]) == pytest.approx(1.0)
    assert method["privacy"]["public_statuses"] == ["detected", "expired"]


def test_rule_coverage_lists_every_configured_rule(board_db):
    view = build_view(board_db, today=TODAY, archive=False)
    coverage = next(s for s in view["sections"] if s["id"] == "coverage")["data"]
    assert {rule["rule_id"] for rule in coverage["rules"]} == set(config.OPPORTUNITY_RULES)
    for rule in coverage["rules"]:
        assert rule["question"]


# ---------------------------------------------------------------------------
# The privacy boundary
# ---------------------------------------------------------------------------
def _first_id(conn) -> str:
    result = engine_mod.run(conn, today=TODAY, archive=False)
    assert result.private, "the fixture must produce at least one opportunity"
    return result.private[0].opportunity.opportunity_id


def test_private_notes_cannot_reach_the_public_html(board_db, tmp_path):
    directory = workflow_dir(tmp_path, _first_id(board_db))
    result = engine_mod.run(board_db, today=TODAY, workflow_dir=directory, archive=False)

    public_html = render(build_view(
        board_db, today=TODAY, audience=AUDIENCE_PUBLIC, result=result
    ))
    for label, sentinel in SENTINELS.items():
        assert sentinel not in public_html, f"{label} leaked into the public page"
    # And the row itself is gone, not merely stripped: a worked lane is private.
    worked = result.private[0].opportunity.opportunity_id
    assert worked not in public_html


def test_the_private_html_does_carry_them(board_db, tmp_path):
    directory = workflow_dir(tmp_path, _first_id(board_db))
    result = engine_mod.run(board_db, today=TODAY, workflow_dir=directory, archive=False)
    private_html = render(build_view(
        board_db, today=TODAY, audience=AUDIENCE_PRIVATE, result=result
    ))
    assert "PRIVATE EDITION" in private_html
    for label, sentinel in SENTINELS.items():
        assert sentinel in private_html, f"{label} is missing from the private page"


def test_the_public_view_data_contains_no_workflow_key_anywhere(board_db, tmp_path):
    directory = workflow_dir(tmp_path, _first_id(board_db))
    result = engine_mod.run(board_db, today=TODAY, workflow_dir=directory, archive=False)
    view = build_view(board_db, today=TODAY, audience=AUDIENCE_PUBLIC, result=result)
    blob = repr(view)
    for sentinel in SENTINELS.values():
        assert sentinel not in blob
    assert "'workflow'" not in blob


def test_a_touched_opportunity_is_absent_from_the_public_set(board_db, tmp_path):
    worked = _first_id(board_db)
    directory = workflow_dir(tmp_path, worked)
    result = engine_mod.run(board_db, today=TODAY, workflow_dir=directory, archive=False)
    assert worked in {item.opportunity.opportunity_id for item in result.private}
    assert worked not in {item.opportunity.opportunity_id for item in result.public}


def test_the_orchestrator_writes_two_files_and_only_one_of_them_is_public(
    board_db, tmp_path, monkeypatch
):
    """End to end through the real renderer: what lands in docs/, and what does not."""
    from dataclasses import dataclass
    from datetime import datetime, timezone

    import scripts.generate_site as generate_site

    working = tmp_path / "wf"
    working.mkdir()
    monkeypatch.setattr(
        config, "OPPORTUNITY_WORKFLOW_DIR", workflow_dir(working, _first_id(board_db))
    )
    private_dir = tmp_path / "workspace"
    monkeypatch.setattr(config, "OPPORTUNITY_PRIVATE_OUTPUT_DIR", str(private_dir))

    @dataclass
    class Ctx:
        conn: object

    output_dir = tmp_path / "docs"
    path = generate_site._render_opportunities(
        output_dir, [], ctx=Ctx(conn=board_db),
        now=datetime(2026, 8, 18, 12, tzinfo=timezone.utc),
    )

    public_html = Path(path).read_text(encoding="utf-8")
    private_html = (private_dir / "opportunities.html").read_text(encoding="utf-8")
    assert path.parent == output_dir
    for sentinel in SENTINELS.values():
        assert sentinel not in public_html
        assert sentinel in private_html


def test_the_private_edition_is_written_outside_docs():
    assert "docs" not in Path(config.OPPORTUNITY_PRIVATE_OUTPUT_DIR).parts


def test_the_promotion_contract_carries_the_public_page_only():
    from trust.site_promotion import expected_site_paths

    paths = expected_site_paths()
    assert "opportunities.html" in paths
    assert not any("workspace" in path for path in paths)


def test_an_unknown_audience_is_refused(board_db):
    with pytest.raises(ValueError, match="unknown audience"):
        build_view(board_db, today=TODAY, audience="partner", archive=False)
