"""The inactivity-clock relay (#6): warn before GitHub disables the schedules."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import config
from scripts import ci_inactivity_clock as clock_mod

NOW = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
ACTIVE = [{"path": ".github/workflows/deploy-dashboard.yml", "state": "active"}]


def _commit(days_ago: float, login: str = "owner", kind: str = "User") -> dict:
    when = (NOW - timedelta(days=days_ago)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {"author": {"login": login, "type": kind}, "commit": {"committer": {"date": when}}}


def _bot(days_ago: float) -> dict:
    return _commit(days_ago, "github-actions[bot]", "Bot")


def test_recent_human_commit_is_ok_and_silent():
    clock = clock_mod.assess([_bot(0), _commit(5)], ACTIVE, NOW)

    assert (clock.state, clock.days) == ("ok", 5)
    assert clock_mod.annotation(clock) is None


def test_daily_bot_commits_do_not_hold_the_clock_off():
    # The question #6 cannot settle from docs: bot commits never count here.
    days = config.SCHEDULE_INACTIVITY_WARN_DAYS
    commits = [_bot(d) for d in range(days)] + [_commit(days)]

    clock = clock_mod.assess(commits, ACTIVE, NOW)

    assert clock.state == "warning"
    assert clock.deadline == NOW - timedelta(days=days) + timedelta(days=config.GITHUB_SCHEDULE_INACTIVITY_DAYS)
    line = clock_mod.annotation(clock)
    assert line is not None and line.startswith("::warning")
    assert f"{clock.deadline:%Y-%m-%d}" in line


def test_unlinked_author_does_not_count_as_human():
    unlinked = {"author": None, "commit": {"committer": {"date": "2026-11-24T00:00:00Z"}}}

    clock = clock_mod.assess([unlinked, _commit(50)], ACTIVE, NOW)

    assert clock.state == "warning"


def test_a_disabled_workflow_wins_over_any_commit_age():
    workflows = ACTIVE + [{"path": ".github/workflows/refresh-prices.yml", "state": "disabled_inactivity"}]

    clock = clock_mod.assess([_commit(1)], workflows, NOW)

    assert clock.state == "disabled"
    assert clock.disabled == [".github/workflows/refresh-prices.yml"]
    line = clock_mod.annotation(clock)
    assert line is not None and line.startswith("::error")


def test_manually_disabled_workflow_is_not_an_inactivity_alarm():
    workflows = ACTIVE + [{"path": ".github/workflows/probe.yml", "state": "disabled_manually"}]

    assert clock_mod.assess([_commit(1)], workflows, NOW).state == "ok"


@pytest.mark.parametrize("days_ago", [config.GITHUB_SCHEDULE_INACTIVITY_DAYS,
                                      config.GITHUB_SCHEDULE_INACTIVITY_DAYS + 1,
                                      config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS - 1])
def test_just_past_the_deadline_is_still_a_warning_not_evidence(days_ago):
    # GitHub's sweep is a background job: July's disable landed on day ~61, not
    # at day 60 sharp. Closing the warning on day 60 would record "harmless" a
    # day before the schedules die (#616 review).
    clock = clock_mod.assess([_commit(days_ago)], ACTIVE, NOW)

    assert clock.state == "warning"
    line = clock_mod.annotation(clock)
    assert line is not None and line.startswith("::warning")


def test_conclude_margin_is_past_the_deadline():
    assert config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS > config.GITHUB_SCHEDULE_INACTIVITY_DAYS
    assert clock_mod.LOOKBACK_DAYS > config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS


@pytest.mark.parametrize("commits", [[_commit(config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS)], [_bot(0)], []])
def test_past_the_deadline_with_everything_active_is_evidence(commits):
    clock = clock_mod.assess(commits, ACTIVE, NOW)

    assert clock.state == "past_deadline_active"
    assert "evidence" in clock_mod.build_body(clock, "RUN")
    # The guard's own issue traffic is activity too, so the notice must not
    # credit the bot commits alone.
    line = clock_mod.annotation(clock)
    assert line is not None and "bot commits count" not in line


def test_empty_workflow_listing_refuses_to_read_as_healthy():
    with pytest.raises(ValueError, match="no workflows"):
        clock_mod.assess([_commit(1)], [], NOW)


def test_malformed_commit_date_raises():
    bad = {"author": {"type": "User"}, "commit": {"committer": {"date": None}}}

    with pytest.raises(ValueError):
        clock_mod.assess([bad], ACTIVE, NOW)


def test_body_carries_a_state_marker_read_back_from_the_newest_entry():
    warning = clock_mod.assess([_commit(50)], ACTIVE, NOW)
    thread = {
        "body": clock_mod.build_body(warning, "RUN"),
        "comments": [{"body": "a human reply"}],
    }

    assert clock_mod.last_reported(thread) == "warning"
    assert clock_mod.last_reported({"body": "no marker", "comments": []}) is None


class _FakeGh:
    def __init__(self, open_issue: int | None, thread: dict | None = None) -> None:
        self.calls: list[tuple[str, ...]] = []
        self.open_issue = open_issue
        self.thread = thread or {"body": "", "comments": []}

    def __call__(self, *args: str) -> str:
        self.calls.append(args)
        if args[:2] == ("issue", "list"):
            return json.dumps([{"number": self.open_issue}] if self.open_issue else [])
        if args[:2] == ("issue", "view"):
            return json.dumps(self.thread)
        return ""

    def verbs(self) -> list[tuple[str, str]]:
        return [(a[0], a[1]) for a in self.calls]


def test_relay_opens_once_then_comments_only_on_state_change(monkeypatch):
    warning = clock_mod.assess([_commit(50)], ACTIVE, NOW)
    fake = _FakeGh(open_issue=None)
    monkeypatch.setattr(clock_mod, "_gh", fake)
    clock_mod.relay(warning, "RUN")
    assert ("issue", "create") in fake.verbs()

    same = _FakeGh(open_issue=7, thread={"body": clock_mod.build_body(warning, "RUN"), "comments": []})
    monkeypatch.setattr(clock_mod, "_gh", same)
    clock_mod.relay(warning, "RUN")
    assert ("issue", "comment") not in same.verbs()

    disabled = clock_mod.assess(
        [_commit(50)], [{"path": "x.yml", "state": "disabled_inactivity"}], NOW)
    changed = _FakeGh(open_issue=7, thread=same.thread)
    monkeypatch.setattr(clock_mod, "_gh", changed)
    clock_mod.relay(disabled, "RUN")
    assert ("issue", "comment") in changed.verbs()


@pytest.mark.parametrize("days_ago", [1, config.SCHEDULE_INACTIVITY_CONCLUDE_DAYS])
def test_relay_closes_an_open_issue_when_reset_or_proven_harmless(monkeypatch, days_ago):
    clock = clock_mod.assess([_commit(days_ago)], ACTIVE, NOW)
    fake = _FakeGh(open_issue=7)
    monkeypatch.setattr(clock_mod, "_gh", fake)

    clock_mod.relay(clock, "RUN")

    assert ("issue", "close") in fake.verbs()
    assert ("issue", "create") not in fake.verbs()
