"""The client-record boundary: nothing a client entered may reach the public artifact.

A position, a fill, a clearing statement and an option quote are the four things
in this project that can only come from the client's own desk. They are also the
four things that would do the most damage if published — a book is a trading
intention, and a published one is front-runnable.

The boundary is checked the same four ways ``tests/test_trial_privacy.py``
checks the trial's: the public projection never *builds* a client key, a key
guard walks for one anyway, a value guard searches free text for the paths and
account names a book carries, and a path guard refuses a destination the Pages
deploy would pick up. Any one could be defeated by a plausible refactor; all
four together is the design.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis.futures.privacy import (
    AUDIENCE_PRIVATE,
    AUDIENCE_PUBLIC,
    CLIENT_RECORD_FIELDS,
    ClientDataLeak,
    assert_no_client_records,
    assert_private_path,
    client_record_dirs,
    private_output_dir,
    redact_for_public,
)

REPO = Path(__file__).resolve().parents[1]


# --- the key guard --------------------------------------------------------
@pytest.mark.parametrize("field", sorted(CLIENT_RECORD_FIELDS))
def test_the_key_guard_fires_on_every_client_field_name(field: str) -> None:
    with pytest.raises(ClientDataLeak, match=field):
        assert_no_client_records({"sections": [], field: "anything"}, where="test")


def test_the_key_guard_reaches_a_client_field_buried_several_levels_down() -> None:
    payload = {"sections": [{"id": "book", "data": {"valuation": {"positions": []}}}]}
    with pytest.raises(ClientDataLeak, match=r"\$\.sections\[0\]\.data\.valuation"):
        assert_no_client_records(payload, where="test")


def test_a_clean_payload_passes_the_key_guard() -> None:
    assert_no_client_records({"sections": [{"id": "curve", "data": {"legs": []}}]}, where="test")


# --- the value guard ------------------------------------------------------
def test_a_path_into_the_client_record_directory_is_a_leak() -> None:
    # The realistic regression: no client key anywhere, and the *filename* of
    # somebody's book rendered as provenance. It names the desk and it names the
    # machine.
    payload = {"sections": [{"data": {"source": "data/reference/positions/house.yml"}}]}
    with pytest.raises(ClientDataLeak, match="positions"):
        assert_no_client_records(payload, where="test")


def test_the_value_guard_scans_bare_strings_inside_lists() -> None:
    with pytest.raises(ClientDataLeak):
        assert_no_client_records(
            ["fine", ["nested", "loaded from data/reference/clearing/statement.yml"]],
            where="test",
        )


def test_an_absolute_path_to_a_record_directory_is_caught_too() -> None:
    payload = {"note": f"{REPO}/data/reference/options/broker.yml"}
    with pytest.raises(ClientDataLeak):
        assert_no_client_records(payload, where="test")


# --- the path guard -------------------------------------------------------
def test_a_write_inside_docs_is_refused() -> None:
    with pytest.raises(ClientDataLeak, match="docs/"):
        assert_private_path(REPO / "docs" / "book.json", where="test")


def test_a_write_nested_deep_inside_docs_is_also_refused() -> None:
    with pytest.raises(ClientDataLeak, match="docs/"):
        assert_private_path(REPO / "docs" / "markets" / "book.html")


def test_a_path_on_the_promotion_contract_is_refused_by_name() -> None:
    from trust.site_promotion import expected_site_paths

    with pytest.raises(ClientDataLeak):
        assert_private_path(REPO / "docs" / "workstation.html")
    assert "workstation.html" in expected_site_paths()


def test_every_client_record_directory_is_outside_the_published_artifact() -> None:
    for directory in (*client_record_dirs(), private_output_dir()):
        assert (REPO / "docs").resolve() not in directory.resolve().parents


# --- committed history ----------------------------------------------------
def test_every_client_record_directory_is_gitignored() -> None:
    """The reason these are YAML files and not a table.

    Every table in this project round-trips through ``data/history/*.csv``,
    which is committed to a public repository, so a positions table would
    publish the book by construction.
    """
    ignored = (REPO / ".gitignore").read_text(encoding="utf-8")
    for stem in ("positions", "options", "clearing"):
        assert f"data/reference/{stem}/*.yml" in ignored
        assert f"data/reference/{stem}/*.csv" in ignored


def test_no_client_record_table_exists_in_the_schema() -> None:
    from pipeline import schema

    text = Path(schema.__file__).read_text(encoding="utf-8").lower()
    for forbidden in ("create table if not exists positions", "create table if not exists fills",
                      "create table if not exists clearing"):
        assert forbidden not in text


# --- redaction ------------------------------------------------------------
def test_redaction_removes_the_client_keys_and_keeps_the_rest() -> None:
    payload = {
        "sections": [
            {"id": "curve", "state": "ok", "data": {"legs": [1, 2]}},
            {"id": "book", "state": "ok", "data": {"valuation": {"positions": [{"key": "ZSX26"}]}}},
        ]
    }
    public = redact_for_public(payload, section_ids=("book",))
    assert_no_client_records(public, where="redacted")
    assert public["sections"][0]["data"]["legs"] == [1, 2]
    assert public["sections"][1]["state"] == "absent"
    assert public["sections"][1]["data"] is None
    assert "private" in public["sections"][1]["reason"]


def test_redaction_does_not_mutate_what_it_was_given() -> None:
    payload = {"sections": [{"id": "book", "state": "ok", "data": {"valuation": {}}}]}
    redact_for_public(payload, section_ids=("book",))
    assert payload["sections"][0]["state"] == "ok"


def test_the_two_audiences_are_distinct_and_named() -> None:
    assert AUDIENCE_PUBLIC != AUDIENCE_PRIVATE
    assert json.dumps({"a": AUDIENCE_PUBLIC})
