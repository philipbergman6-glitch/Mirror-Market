"""Sensitive trial fields cannot enter public output.

This is the brief's explicit verification requirement, and it is checked from
both directions. The **structural** direction: every aggregate projection in the
package is built and grepped, so a field added to a record later without an
aggregate branch fails here rather than on a published page. The **behavioural**
direction: the guards themselves are shown to actually fire, because a guard
that silently passes everything is worse than no guard — it converts an unchecked
write into a checked one in the reader's mind and nowhere else.

The privacy boundary in this package is four independent things: the aggregate
projection never *builds* a private key, the key guard walks for one anyway, the
value guard searches free text for the participant's handle, and the path guard
refuses a destination that Pages would pick up. Any one of them could be
defeated by a plausible refactor; all four together is the design.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis.trial.backlog import draft_backlog, issue_body
from analysis.trial.domain import (
    AUDIENCE_AGGREGATE,
    PRIVATE_FIELD_NAMES,
    TrialError,
)
from analysis.trial.metrics import compute_metrics
from analysis.trial.review import scorecard, weekly_review
from analysis.trial.sanitize import (
    PrivacyLeak,
    aggregate,
    assert_no_identifiers,
    assert_private_path,
    assert_sanitized,
    private_output_dir,
    record_dir,
    write_private_json,
)
from tests.trial_fixtures import (
    BLOCKER_ISSUE,
    MARK,
    NUMERICAL_ISSUE,
    SYNTHETIC_TRADERS,
    TODAY,
    WINDOW_START,
    day_observation,
    full_window,
    issue,
    lookup,
    session,
)

REPO = Path(__file__).resolve().parents[1]


def _rich_window() -> tuple[list, list]:
    """The healthy window plus the two issues that carry the most free text."""
    sessions, days = full_window()
    sessions.append(session(trader=SYNTHETIC_TRADERS[1], hour=11, issues=(NUMERICAL_ISSUE,)))
    sessions.append(
        session(
            trader=SYNTHETIC_TRADERS[0],
            hour=12,
            issues=(BLOCKER_ISSUE,),
            lookups=(lookup(),),
        )
    )
    return sessions, days


# --- the structural direction --------------------------------------------
def test_no_aggregate_projection_in_the_package_builds_a_private_key() -> None:
    sessions, days = _rich_window()
    metrics = compute_metrics(sessions, days)
    review = weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY)
    card = scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY)
    backlog = draft_backlog(sessions)

    for name, obj in (
        ("session", sessions[0]),
        ("issue", NUMERICAL_ISSUE),
        ("lookup", lookup()),
        ("day", days[0]),
        ("metrics", metrics),
        ("review", review),
        ("scorecard", card),
        ("backlog", backlog),
    ):
        aggregate(obj, identifiers=SYNTHETIC_TRADERS, where=name)


def test_no_synthetic_free_text_survives_into_any_aggregate_projection() -> None:
    # Every free-text field in the fixtures carries the SYNTHETIC marker, so one
    # grep proves that none of them — summaries, evidence, notes, decisions,
    # questions — reached the shareable payload.
    sessions, days = _rich_window()
    payloads = [
        aggregate(compute_metrics(sessions, days), where="metrics"),
        aggregate(
            weekly_review(sessions, days, week_start=WINDOW_START, week_end=TODAY), where="review"
        ),
        aggregate(
            scorecard(sessions, days, window_start=WINDOW_START, window_end=TODAY), where="card"
        ),
        aggregate(draft_backlog(sessions), where="backlog"),
    ]
    blob = json.dumps(payloads, default=str)
    assert MARK not in blob
    for trader in SYNTHETIC_TRADERS:
        assert trader not in blob.lower()


def test_the_private_projection_by_contrast_does_keep_everything() -> None:
    # Proving the aggregate test above is not passing because the objects are
    # empty. The private edition is the one the desk reads, and it must be whole.
    payload = session(issues=(NUMERICAL_ISSUE,), lookups=(lookup(),)).to_dict()
    blob = json.dumps(payload, default=str)
    assert MARK in blob
    assert SYNTHETIC_TRADERS[0] in blob


# --- the key guard --------------------------------------------------------
@pytest.mark.parametrize("field", sorted(PRIVATE_FIELD_NAMES))
def test_the_key_guard_fires_on_every_private_field_name(field: str) -> None:
    with pytest.raises(PrivacyLeak, match=field):
        assert_sanitized({"metrics": [{"ok": 1}], field: "anything"}, where="test")


def test_the_key_guard_reaches_a_private_field_buried_several_levels_down() -> None:
    # The realistic regression is never a top-level `trader` key — it is one
    # surviving inside a nested item a new caller passed through unchanged.
    payload = {"weeks": [{"backlog": {"items": [{"evidence": "leaked"}]}}]}
    with pytest.raises(PrivacyLeak, match=r"\$\.weeks\[0\]\.backlog\.items\[0\]\.evidence"):
        assert_sanitized(payload, where="test")


def test_a_clean_payload_passes_the_key_guard() -> None:
    assert_sanitized({"metrics": [{"key": "would_act_rate", "value": 0.8}]}, where="test")


# --- the value guard ------------------------------------------------------
def test_the_value_guard_catches_a_handle_interpolated_into_free_text() -> None:
    # The leak a key guard cannot see: no private key anywhere, and a name in a
    # string. This is how the real disclosure happens.
    payload = {"worked": ["zephyr priced the Nov cargo without leaving the page"]}
    with pytest.raises(PrivacyLeak, match="zephyr"):
        assert_no_identifiers(payload, SYNTHETIC_TRADERS, where="test")


def test_the_value_guard_scans_bare_strings_inside_lists() -> None:
    # A walk that only descended through mappings never scanned these, which is
    # exactly where the review's `worked` and `recommendations` entries sit.
    with pytest.raises(PrivacyLeak):
        assert_no_identifiers(["fine", ["nested", "quartz was blocked"]], SYNTHETIC_TRADERS)


def test_the_value_guard_matches_regardless_of_case() -> None:
    with pytest.raises(PrivacyLeak):
        assert_no_identifiers({"note": "Zephyr"}, ("zephyr",))


def test_the_value_guard_also_checks_keys_because_a_handle_can_be_one() -> None:
    with pytest.raises(PrivacyLeak):
        assert_no_identifiers({"by_trader": {"zephyr": 4}}, SYNTHETIC_TRADERS)


def test_a_two_character_identifier_is_refused_rather_than_silently_useless() -> None:
    # It would match almost any text, making the guard meaningless while
    # appearing to run — the worst state a guard can be in.
    with pytest.raises(TrialError, match="three characters"):
        assert_no_identifiers({"note": "fine"}, ("jd",))


def test_no_identifiers_supplied_is_a_no_op_rather_than_a_false_pass() -> None:
    # Documented behaviour: this module cannot guess what a name looks like.
    assert_no_identifiers({"note": "zephyr"}, ())


# --- the path guard -------------------------------------------------------
def test_a_write_inside_docs_is_refused() -> None:
    with pytest.raises(PrivacyLeak, match="docs/"):
        assert_private_path(REPO / "docs" / "trial-records.json", where="test")


def test_a_write_nested_deep_inside_docs_is_also_refused() -> None:
    with pytest.raises(PrivacyLeak, match="docs/"):
        assert_private_path(REPO / "docs" / "markets" / "trial.html")


def test_a_path_on_the_promotion_contract_is_refused_by_name() -> None:
    from trust.site_promotion import expected_site_paths

    with pytest.raises(PrivacyLeak):
        assert_private_path(REPO / "docs" / next(iter(expected_site_paths())))


def test_the_configured_private_directories_are_outside_the_published_artifact() -> None:
    # If either of these ever moved under docs/, every trial record would be
    # published by the next deploy with nothing else complaining.
    for directory in (private_output_dir(), record_dir()):
        assert (REPO / "docs").resolve() not in directory.parents


def test_the_trial_page_is_not_on_the_promotion_contract() -> None:
    from trust.site_promotion import expected_site_paths

    published = set(expected_site_paths())
    assert not any("trial" in url for url in published)


# --- writes ---------------------------------------------------------------
def test_a_private_write_keeps_its_content_and_guards_only_its_destination(tmp_path: Path) -> None:
    target = write_private_json(tmp_path / "record.json", session().to_dict())
    assert SYNTHETIC_TRADERS[0] in target.read_text(encoding="utf-8")


def test_an_aggregate_write_refuses_a_payload_carrying_a_private_field(tmp_path: Path) -> None:
    with pytest.raises(PrivacyLeak):
        write_private_json(
            tmp_path / "share.json", {"trader": "zephyr"}, audience=AUDIENCE_AGGREGATE
        )


def test_an_aggregate_write_refuses_a_payload_naming_a_participant(tmp_path: Path) -> None:
    with pytest.raises(PrivacyLeak):
        write_private_json(
            tmp_path / "share.json",
            {"worked": ["zephyr completed every brief"]},
            audience=AUDIENCE_AGGREGATE,
            identifiers=SYNTHETIC_TRADERS,
        )


def test_no_write_of_any_audience_may_land_in_docs() -> None:
    # The thing that must never happen, regardless of how sanitised the payload.
    with pytest.raises(PrivacyLeak):
        write_private_json(REPO / "docs" / "x.json", {"ok": 1}, audience=AUDIENCE_AGGREGATE)


# --- the backlog's separate clearance ------------------------------------
def test_a_backlog_item_refuses_public_rendering_until_it_is_cleared() -> None:
    # Backlog items must carry evidence, and evidence is a trader's words. So
    # publishing one is a separate, explicit act rather than a projection.
    backlog = draft_backlog([session(issues=(NUMERICAL_ISSUE,)) for _ in range(3)])
    assert backlog.items, "the fixture must produce at least one item to clear"
    item = backlog.items[0]
    with pytest.raises(TrialError, match="cleared"):
        issue_body(item, audience=AUDIENCE_AGGREGATE)
    body = issue_body(item.cleared(), audience=AUDIENCE_AGGREGATE)
    assert_no_identifiers(body, SYNTHETIC_TRADERS, where="issue body")


def test_the_private_issue_body_needs_no_clearance_because_it_is_not_shared() -> None:
    backlog = draft_backlog([session(issues=(NUMERICAL_ISSUE,)) for _ in range(3)])
    assert issue_body(backlog.items[0]).strip()


# --- the record store's own boundary --------------------------------------
def test_the_record_directory_is_gitignored_so_no_record_can_be_committed() -> None:
    # This is the reason trial records are YAML files rather than a table: every
    # table here round-trips through data/history/*.csv, which is committed to a
    # public repository, so a trial table would publish identity by construction.
    ignored = (REPO / ".gitignore").read_text(encoding="utf-8")
    assert "data/reference/trial/sessions/*.yml" in ignored
    assert "data/reference/trial/days/*.yml" in ignored


def test_a_day_observations_operator_note_never_reaches_the_aggregate() -> None:
    payload = aggregate(day_observation(note=f"{MARK} the desk was down"), where="day")
    assert "note" not in payload
    assert json.dumps(payload, default=str).find(MARK) == -1


def test_an_object_without_an_audience_aware_to_dict_is_refused_not_shared() -> None:
    class Naive:
        def to_dict(self) -> dict[str, str]:
            return {"trader": "zephyr"}

    with pytest.raises(TrialError, match="audience"):
        aggregate(Naive(), where="naive")


def test_an_object_with_no_to_dict_at_all_is_refused() -> None:
    with pytest.raises(TrialError, match="to_dict"):
        aggregate(object(), where="bare")


def test_the_issue_aggregate_keeps_the_class_and_drops_the_words() -> None:
    payload = aggregate(issue(), where="issue")
    assert payload["classification"] == "misleading_ux"
    assert "summary" not in payload
    assert "evidence" not in payload
