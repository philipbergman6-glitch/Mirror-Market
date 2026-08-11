"""Tests for the MAGyP v1-vs-trusted reconciliation runner.

Network-free: every test replays stored artifact bytes through a stub
fetcher, so the job's own logic is exercised without touching MAGyP.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import pytest

from scripts.reconcile_magyp_fob import (
    EXIT_DIVERGED,
    EXIT_OK,
    EXIT_UNAVAILABLE,
    STATUS_DIVERGED,
    STATUS_NO_CIRCULAR,
    STATUS_RECONCILED,
    main,
    reconcile_once,
    resolve_published_circular,
    write_report,
)
from trust.magyp_fob import artifact_from_response

NOW = datetime(2026, 8, 11, 15, 0, tzinfo=timezone.utc)


def _post(posicion: str, precio: float, fecha: str = "2026-08-11 00:00:00.000") -> dict:
    return {
        "fecha": fecha,
        "circular": "2031",
        "posicion": posicion,
        "precio": precio,
        "mesDesde": 8,
        "añoDesde": 2026,
        "mesHasta": 8,
        "añoHasta": 2026,
    }


def _replay(posts: list[dict]):
    content = json.dumps({"posts": posts}, separators=(",", ":")).encode("utf-8")
    return artifact_from_response(content=content, retrieved_at=NOW)


SOY_POSTS = [
    _post("12019000190C", 450),
    _post("15071000100Q", 1186),
    _post("23040010100B", 350),
]


def _fetcher(by_day: dict[date, list[dict]]):
    """Serve stored posts per day; unpublished days answer with an empty body."""
    def fetch(day: date):
        return _replay(by_day.get(day, []))
    return fetch


# ---------------------------------------------------------------------------
# The core promise: both paths see the same circular
# ---------------------------------------------------------------------------
def test_identical_bytes_reconcile_across_both_paths() -> None:
    run = reconcile_once(
        today=date(2026, 8, 11),
        fetch_artifact=_fetcher({date(2026, 8, 11): SOY_POSTS}),
        now=NOW,
    )

    assert run.status == STATUS_RECONCILED
    assert run.report is not None and run.report.reconciled
    assert run.resolved_day == date(2026, 8, 11)
    assert run.legacy_rows == 3
    assert run.trusted_rows == 3
    assert run.report.matched_rows == 3
    assert run.exit_code == EXIT_OK


def test_both_paths_are_handed_one_resolved_day_not_two() -> None:
    """A divergence must mean "different parse", never "different circular"."""
    fetched: list[date] = []

    def fetch(day: date):
        fetched.append(day)
        return _replay(SOY_POSTS if day == date(2026, 8, 10) else [])

    run = reconcile_once(today=date(2026, 8, 12), fetch_artifact=fetch, now=NOW)

    assert run.resolved_day == date(2026, 8, 10)
    # 12th and 11th were empty, the 10th published — and once resolved, no
    # further fetch happens: the trusted path reuses those exact bytes.
    assert fetched == [date(2026, 8, 12), date(2026, 8, 11), date(2026, 8, 10)]
    assert run.status == STATUS_RECONCILED


def test_weekends_are_skipped_without_a_request() -> None:
    fetched: list[date] = []

    def fetch(day: date):
        fetched.append(day)
        return _replay(SOY_POSTS if day == date(2026, 8, 7) else [])

    # 2026-08-08 is a Saturday, 08-09 a Sunday.
    run = resolve_published_circular(today=date(2026, 8, 9), fetch_artifact=fetch)

    assert run is not None and run.day == date(2026, 8, 7)
    assert date(2026, 8, 8) not in fetched
    assert date(2026, 8, 9) not in fetched


# ---------------------------------------------------------------------------
# Grading
# ---------------------------------------------------------------------------
def test_holiday_is_not_graded_as_a_divergence() -> None:
    """Every Argentine holiday must not read as a defect in the trusted path."""
    run = reconcile_once(
        today=date(2026, 8, 11),
        fetch_artifact=_fetcher({}),
        now=NOW,
    )

    assert run.status == STATUS_NO_CIRCULAR
    assert run.report is None
    assert run.resolved_day is None
    assert run.exit_code == EXIT_OK


def test_field_difference_is_reported_and_fails() -> None:
    def fetch(day: date):
        return _replay(SOY_POSTS)

    # Divert the v1 parser so the two paths genuinely disagree on a price.
    import scripts.reconcile_magyp_fob as runner

    original = runner._parse_posts

    def skewed(posts):
        frame = original(posts).copy()
        frame.loc[frame["product"] == "Soybeans", "price_usd_mt"] = 999.0
        return frame

    runner._parse_posts = skewed
    try:
        run = reconcile_once(today=date(2026, 8, 11), fetch_artifact=fetch, now=NOW)
    finally:
        runner._parse_posts = original

    assert run.status == STATUS_DIVERGED
    assert run.exit_code == EXIT_DIVERGED
    assert run.report is not None
    differences = run.report.field_differences
    assert any(diff["field"] == "price_usd_mt" for diff in differences)
    assert any(diff["legacy"] == 999.0 for diff in differences)


def test_missing_row_on_one_path_is_reported() -> None:
    import scripts.reconcile_magyp_fob as runner

    original = runner._parse_posts

    def dropped(posts):
        frame = original(posts).copy()
        return frame[frame["product"] != "Soybean Oil"]

    runner._parse_posts = dropped
    try:
        run = reconcile_once(
            today=date(2026, 8, 11),
            fetch_artifact=_fetcher({date(2026, 8, 11): SOY_POSTS}),
            now=NOW,
        )
    finally:
        runner._parse_posts = original

    assert run.status == STATUS_DIVERGED
    assert run.report is not None
    assert len(run.report.missing_in_legacy) == 1
    assert run.report.missing_in_legacy[0]["product"] == "Soybean Oil"


# ---------------------------------------------------------------------------
# Report and CLI
# ---------------------------------------------------------------------------
def test_report_json_is_machine_readable(tmp_path) -> None:
    run = reconcile_once(
        today=date(2026, 8, 11),
        fetch_artifact=_fetcher({date(2026, 8, 11): SOY_POSTS}),
        now=NOW,
    )
    path = write_report(run, tmp_path / "nested" / "magyp_fob.json")

    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["status"] == STATUS_RECONCILED
    assert payload["resolved_day"] == "2026-08-11"
    assert payload["legacy_rows"] == 3
    assert payload["checked_at"] == NOW.isoformat()
    assert payload["reconciliation"]["reconciled"] is True
    assert payload["reconciliation"]["matched_rows"] == 3
    assert len(payload["content_hash"]) == 64


def test_explicit_day_skips_resolution(tmp_path, monkeypatch) -> None:
    fetched: list[date] = []

    def fetch(day: date):
        fetched.append(day)
        return _replay(SOY_POSTS)

    monkeypatch.setattr("scripts.reconcile_magyp_fob.fetch_magyp_fob_artifact", fetch)
    output = tmp_path / "report.json"

    code = main(["--day", "2026-08-11", "--output", str(output)])

    assert code == EXIT_OK
    assert fetched == [date(2026, 8, 11)]
    assert json.loads(output.read_text(encoding="utf-8"))["resolved_day"] == "2026-08-11"


def test_upstream_outage_exits_unavailable_not_diverged(monkeypatch, tmp_path) -> None:
    """MAGyP being down is not evidence about the trusted path."""
    import requests

    def dead(day: date):
        raise requests.RequestException("connection reset")

    monkeypatch.setattr("scripts.reconcile_magyp_fob.fetch_magyp_fob_artifact", dead)

    code = main(["--day", "2026-08-11", "--output", str(tmp_path / "report.json")])

    assert code == EXIT_UNAVAILABLE
    assert code != EXIT_DIVERGED


def test_schema_change_exits_unavailable(monkeypatch, tmp_path) -> None:
    def renamed(day: date):
        return artifact_from_response(content=b'{"renamed_posts":[]}', retrieved_at=NOW)

    monkeypatch.setattr("scripts.reconcile_magyp_fob.fetch_magyp_fob_artifact", renamed)

    code = main(["--day", "2026-08-11", "--output", str(tmp_path / "report.json")])

    assert code == EXIT_UNAVAILABLE


@pytest.mark.parametrize("status", [STATUS_RECONCILED, STATUS_NO_CIRCULAR])
def test_non_divergent_statuses_never_fail_the_job(status) -> None:
    """The job is observational — only a real disagreement is worth a red mark."""
    posts = SOY_POSTS if status == STATUS_RECONCILED else []
    run = reconcile_once(
        today=date(2026, 8, 11),
        fetch_artifact=_fetcher({date(2026, 8, 11): posts} if posts else {}),
        now=NOW,
    )
    assert run.status == status
    assert run.exit_code == EXIT_OK
