"""The private trial dashboard: it renders, and it cannot be published.

This page is the one surface in the repository that is *designed* to carry
everything — trader handles, their words, the decisions they reached — because
it is the desk's own read of the trial. That inverts the usual test: the risk
here is not that a field is missing, it is that the file ends up somewhere the
Pages deploy can see. So the assertions divide in two: the page renders what the
desk needs, and every path by which it could reach the public artifact is shut.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from analysis.trial.sanitize import PrivacyLeak
from app.trial_page import build_trial_page, build_view, render_trial_page
from tests.trial_fixtures import (
    BLOCKER_ISSUE,
    MARK,
    NUMERICAL_ISSUE,
    SYNTHETIC_TRADERS,
    TODAY,
    day_observation,
    full_window,
    lookup,
    session,
)

REPO = Path(__file__).resolve().parents[1]


def _rendered() -> str:
    sessions, days = full_window()
    sessions.append(session(hour=13, issues=(NUMERICAL_ISSUE,), lookups=(lookup(),)))
    return render_trial_page(build_view(sessions, days, today=TODAY))


# --- the view -------------------------------------------------------------
def test_the_view_carries_the_eight_sections_the_desk_reads() -> None:
    sessions, days = full_window()
    view = build_view(sessions, days, today=TODAY)
    assert set(view["sections"]) == {
        "verdict",
        "metrics",
        "scorecard",
        "coverage",
        "findings",
        "reliability",
        "reproducibility",
        "method",
    }


def test_every_section_carries_the_state_reason_data_envelope() -> None:
    sessions, days = full_window()
    for name, section in build_view(sessions, days, today=TODAY)["sections"].items():
        assert set(section) == {"state", "reason", "data"}, name
        if section["state"] != "ok":
            assert section["reason"].strip(), f"{name} is not ok and names no reason"


def test_an_empty_trial_says_so_rather_than_rendering_a_dashboard_of_zeros() -> None:
    # A dashboard of zeros reads like a result. "Nothing has been logged" does not.
    view = build_view([], [], today=TODAY)
    assert view["empty"]
    assert view["empty_reason"].strip()
    assert view["sections"] == {}
    assert "no trial sessions" in render_trial_page(view).lower()


def test_the_unmet_questions_are_read_over_the_whole_window_not_one_day() -> None:
    # Read from a one-day window this silently showed nothing on any day nobody
    # happened to log a lookup — an empty list that reads as "we answered
    # everything" rather than as "wrong window".
    sessions, days = full_window()
    sessions.append(session(trading_day=TODAY, hour=13, lookups=(lookup(),)))
    view = build_view(sessions, days, today=TODAY)
    assert view["sections"]["findings"]["data"]["unmet_questions"]


def test_a_blocker_reaches_the_verdict_section() -> None:
    sessions, days = full_window()
    sessions.append(
        session(hour=13, issues=(BLOCKER_ISSUE,), outcome=__import__(
            "analysis.trial.domain", fromlist=["Outcome"]
        ).Outcome.BLOCKED, would_act=False)
    )
    view = build_view(sessions, days, today=TODAY)
    assert view["sections"]["verdict"]["data"]["verdict"] == "no_go"


# --- the render -----------------------------------------------------------
def test_the_page_renders_and_carries_the_material_the_desk_needs() -> None:
    html = _rendered()
    assert len(html) > 5_000
    assert MARK in html  # the fixtures' free text is present, as it must be
    assert "<html" in html.lower()


def test_the_page_warns_on_its_face_that_it_is_not_for_publication() -> None:
    html = _rendered()
    assert "NOT FOR PUBLICATION" in html
    assert "PRIVATE" in html


def test_the_page_asks_not_to_be_indexed_if_it_is_ever_served() -> None:
    # Belt and braces behind the path guard: if this file were ever mis-served,
    # it should not also enter a search index.
    html = _rendered()
    assert 'name="robots"' in html
    assert "noindex" in html


def test_the_page_carries_no_external_reference_that_could_leak_by_request() -> None:
    # A remote font or script fetch from a private page tells a third party the
    # page was opened, which is the quietest disclosure available.
    html = _rendered()
    for marker in ("http://", "https://"):
        assert marker not in html, f"{marker} appears in a private page"


def test_the_page_uses_the_projects_own_palette() -> None:
    import re

    design = (REPO / "DESIGN.md").read_text(encoding="utf-8")
    html = _rendered()
    palette = set(re.findall(r"#[0-9a-fA-F]{6}", design))
    used = set(re.findall(r"#[0-9a-fA-F]{6}", html))
    assert used, "the page defines no colours at all"
    assert used & {c.lower() for c in palette} or used & palette


# --- the write ------------------------------------------------------------
def test_the_dashboard_is_written_outside_docs(tmp_path: Path) -> None:
    sessions, days = full_window()
    target = build_trial_page(sessions, days, output_dir=tmp_path)
    assert target.exists()
    assert (REPO / "docs").resolve() not in target.resolve().parents


def test_writing_the_dashboard_into_docs_is_refused_before_anything_renders() -> None:
    with pytest.raises(PrivacyLeak, match="docs/"):
        build_trial_page([session()], [day_observation()], output_dir=REPO / "docs")


def test_the_configured_destination_is_itself_outside_docs() -> None:
    import config

    directory = Path(config.TRIAL_PRIVATE_OUTPUT_DIR).resolve()
    assert (REPO / "docs").resolve() not in directory.parents


def test_the_trial_dashboard_is_absent_from_the_promotion_contract() -> None:
    # The last line of defence: even a file inside docs/ would not be uploaded
    # by name, and this asserts the contract never learns about it.
    from trust.site_promotion import expected_site_paths

    assert not any("trial" in url for url in expected_site_paths())


def test_the_builder_has_no_audience_switch_that_could_publish_it_by_argument() -> None:
    # One builder with an audience flag would make publishing this page a
    # one-argument mistake. There is deliberately no such argument.
    import inspect

    assert "audience" not in inspect.signature(build_view).parameters
    assert "audience" not in inspect.signature(build_trial_page).parameters


def test_the_written_file_carries_the_free_text_because_it_is_the_private_edition() -> None:
    # Proving the guards above are not passing against an empty page. Note what
    # is asserted: the *words* traders typed, not their handles. The page reports
    # findings and questions, and reaches participation only as a count — there
    # is no per-trader breakdown on it, and none is wanted, because a defect
    # attributed to a named trader invites the desk to read the person rather
    # than the product.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        sessions, days = full_window()
        sessions.append(session(hour=13, issues=(NUMERICAL_ISSUE,), lookups=(lookup(),)))
        html = build_trial_page(sessions, days, output_dir=tmp).read_text(encoding="utf-8")
    assert MARK in html
    assert not any(trader in html for trader in SYNTHETIC_TRADERS)
