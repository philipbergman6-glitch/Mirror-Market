"""Release stamps, and whether a finding from three weeks ago can be re-run.

Requirement 6 is only worth having if the stamp is honest in the *pessimistic*
direction. A stamp that wrongly says "reproducible" is worse than no stamp at
all: it invites someone to trust a re-run that never reproduced anything. So
every uncertainty here — no git, a failed status call, an unreadable freshness
table — resolves toward dirty, unknown, or a fingerprint that matches nothing.
"""

from __future__ import annotations

import subprocess
from dataclasses import replace
from pathlib import Path

import pandas as pd
import pytest

from analysis.trial.domain import ReleaseStamp, TrialError
from analysis.trial.release import (
    UNKNOWN_REVISION,
    capture_release_stamp,
    data_fingerprint,
    git_code_revision,
    reproduce,
)
from tests.trial_fixtures import CLEAN_STAMP, DIRTY_STAMP

FRESHNESS = pd.DataFrame(
    [
        {
            "layer_name": "prices",
            "last_success": "2026-08-18",
            "status": "success",
            "rows_fetched": 10,
            "last_attempt": "2026-08-18T19:04:00Z",
        },
        {
            "layer_name": "cepea",
            "last_success": "2026-08-15",
            "status": "stale",
            "rows_fetched": 0,
            "last_attempt": "2026-08-18T19:04:00Z",
        },
    ]
)


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=str(repo), check=True, capture_output=True)


def _repo(tmp_path: Path) -> Path:
    _git(tmp_path, "init", "-q")
    _git(tmp_path, "config", "user.email", "t@example.com")
    _git(tmp_path, "config", "user.name", "T")
    (tmp_path / "a.txt").write_text("one", encoding="utf-8")
    _git(tmp_path, "add", "a.txt")
    _git(tmp_path, "commit", "-qm", "first")
    return tmp_path


# --- code half ------------------------------------------------------------
def test_a_clean_checkout_reports_its_commit_and_is_not_dirty(tmp_path: Path) -> None:
    sha, dirty = git_code_revision(_repo(tmp_path))
    assert len(sha) == 40
    assert not dirty


def test_an_uncommitted_change_makes_the_stamp_dirty(tmp_path: Path) -> None:
    repo = _repo(tmp_path)
    (repo / "a.txt").write_text("two", encoding="utf-8")
    _, dirty = git_code_revision(repo)
    assert dirty


def test_an_untracked_file_also_makes_the_stamp_dirty(tmp_path: Path) -> None:
    # An untracked module a local run imported is exactly the change that makes
    # a finding unreproducible while leaving no trace in the commit.
    repo = _repo(tmp_path)
    (repo / "hotfix.py").write_text("x = 1", encoding="utf-8")
    _, dirty = git_code_revision(repo)
    assert dirty


def test_no_repository_reports_unknown_and_dirty_rather_than_raising(tmp_path: Path) -> None:
    # A trader mid-session must not lose a record because git is unavailable —
    # and "unknown" must never be confusable with a commit, nor reproducible.
    sha, dirty = git_code_revision(tmp_path / "not-a-repo")
    assert sha == UNKNOWN_REVISION
    assert dirty


# --- data half ------------------------------------------------------------
def test_the_fingerprint_is_stable_across_row_order() -> None:
    # Two runs of the same pipeline over unchanged upstream data must produce
    # one edition id, whatever order the query returned the rows in.
    first, count = data_fingerprint(FRESHNESS)
    second, _ = data_fingerprint(FRESHNESS.iloc[::-1].reset_index(drop=True))
    assert first == second
    assert count == 2


def test_the_fingerprint_ignores_last_attempt() -> None:
    # last_attempt changes on every run whether or not anything was fetched;
    # including it would make two identical editions fingerprint differently.
    moved = FRESHNESS.copy()
    moved["last_attempt"] = "2026-08-19T19:04:00Z"
    assert data_fingerprint(moved)[0] == data_fingerprint(FRESHNESS)[0]


def test_the_fingerprint_moves_when_a_layer_grades_differently() -> None:
    changed = FRESHNESS.copy()
    changed.loc[1, "status"] = "success"
    assert data_fingerprint(changed)[0] != data_fingerprint(FRESHNESS)[0]


def test_the_fingerprint_moves_when_a_layer_returns_a_different_row_count() -> None:
    changed = FRESHNESS.copy()
    changed.loc[0, "rows_fetched"] = 9
    assert data_fingerprint(changed)[0] != data_fingerprint(FRESHNESS)[0]


def test_an_empty_freshness_table_fingerprints_as_no_data_rather_than_raising() -> None:
    digest, count = data_fingerprint(pd.DataFrame())
    assert count == 0
    assert digest != data_fingerprint(FRESHNESS)[0]


# --- capture --------------------------------------------------------------
def test_a_captured_stamp_carries_both_halves_and_the_layer_count(tmp_path: Path) -> None:
    stamp = capture_release_stamp(repo=_repo(tmp_path), freshness=FRESHNESS)
    assert len(stamp.code_revision) == 40
    assert stamp.layer_count == 2
    assert stamp.captured_at.tzinfo is not None
    assert stamp.is_reproducible


def test_a_captured_stamp_invents_no_edition_id_when_there_is_none(tmp_path: Path) -> None:
    # The static site is not built through trust.edition, so there is no id to
    # inherit. None is honest; a synthesised id would look exactly like a fact.
    assert capture_release_stamp(repo=_repo(tmp_path), freshness=FRESHNESS).edition_id is None


# --- reproduce ------------------------------------------------------------
def test_an_unchanged_code_and_data_state_reproduces() -> None:
    check = reproduce(CLEAN_STAMP, current=CLEAN_STAMP)
    assert check.verdict == "reproducible"
    assert check.reproducible


def test_moved_data_reports_drifted_and_names_which_half_moved() -> None:
    moved = replace(CLEAN_STAMP, data_fingerprint="a" * 64)
    check = reproduce(CLEAN_STAMP, current=moved)
    assert check.verdict == "drifted"
    assert check.code_matches
    assert not check.data_matches
    assert "data moved" in check.reason


def test_moved_code_and_data_names_both() -> None:
    moved = replace(CLEAN_STAMP, code_revision="9" * 40, data_fingerprint="a" * 64)
    assert "code and data moved" in reproduce(CLEAN_STAMP, current=moved).reason


def test_a_stamp_taken_dirty_can_never_reproduce_even_against_itself() -> None:
    # No commit describes the code that produced it, so nothing can re-run it.
    check = reproduce(DIRTY_STAMP, current=DIRTY_STAMP)
    assert check.verdict == "not-reproducible"
    assert not check.reproducible
    assert "dirty working tree" in check.reason


def test_the_replay_command_is_printed_and_the_working_tree_is_untouched(tmp_path: Path) -> None:
    # A verification routine that checked out a commit mid-trial would be a
    # worse failure than the unreproducible finding it was investigating.
    repo = _repo(tmp_path)
    before = git_code_revision(repo)
    check = reproduce(CLEAN_STAMP, repo=repo, freshness=FRESHNESS)
    assert check.replay_command == f"git checkout {CLEAN_STAMP.code_revision}"
    assert git_code_revision(repo) == before


def test_reproduce_refuses_anything_that_is_not_a_release_stamp() -> None:
    with pytest.raises(TrialError, match="ReleaseStamp"):
        reproduce({"code_revision": "0" * 40})  # type: ignore[arg-type]


def test_the_check_serialises_both_stamps_so_the_comparison_is_inspectable() -> None:
    payload = reproduce(CLEAN_STAMP, current=CLEAN_STAMP).to_dict()
    assert payload["verdict"] == "reproducible"
    assert payload["stamp"]["code_revision"] == CLEAN_STAMP.code_revision
    assert payload["current"]["data_fingerprint"] == CLEAN_STAMP.data_fingerprint


def test_a_stamp_matches_on_code_and_data_and_ignores_when_it_was_taken() -> None:
    # Two traders stamping the same edition an hour apart stamped one edition.
    from datetime import datetime, timezone

    later = replace(CLEAN_STAMP, captured_at=datetime(2027, 1, 1, tzinfo=timezone.utc))
    assert CLEAN_STAMP.matches(later)


def test_an_unknown_revision_is_never_reproducible() -> None:
    stamp = ReleaseStamp(
        code_revision=UNKNOWN_REVISION,
        data_fingerprint="f" * 64,
        captured_at=CLEAN_STAMP.captured_at,
        dirty=True,
        layer_count=0,
    )
    assert not stamp.is_reproducible
