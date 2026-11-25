"""Which layers a Refresh Prices run fetches, which it gates on, and whether it stands down.

The `slot` job of `.github/workflows/refresh-prices.yml` runs this before any
dependency is installed, so it is stdlib-only and imports nothing from the
repo. It was bash inside the YAML until the board slot (owner decision on
#622, 2026-11-25) added a second backstop; the routing is now tested
(`tests/test_refresh_slot.py`) instead of read.

Slots, by trigger:

  workflow_dispatch slot=board   the CBOT board, fired by cron-job.org at
                                 01:00 America/New_York Tue-Sat: fetches and
                                 gates `prices,forward_curve`.
  workflow_dispatch layers=dce   DCE, fired by cron-job.org at 10:30 UTC
                                 weekdays; gated on dce. Also the **board
                                 backstop**: if the board was due today and no
                                 board dispatch delivered it, this run fetches
                                 the board too, and a separate step goes red
                                 on it (no board run delivered today).
  workflow_dispatch layers=...   anything else: fetch and gate what was asked.
  schedule 0 8                   the DCE backstop: stands down if a dispatched
                                 dce run delivered today, else fetches dce and
                                 goes red.
  schedule 7 20                  the evening refresh; gates nothing.
  schedule 30 23                 the FX catch-up; gates currencies, excuses a
                                 scheduler-late start.

"Delivered" is read from the run's own gate step, not its conclusion: a dce
run that delivered dce on time and then went red on the board backstop must
not make the DCE backstop refetch dce hours later and re-stamp an on-time
number as a late one.

Usage (CI):
    python3 scripts/refresh_slot.py --event "$EVENT" --schedule "$SCHEDULE" \\
        --layers "$LAYERS" --slot "$SLOT" --github-output "$GITHUB_OUTPUT"
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

WORKFLOW = "refresh-prices.yml"

# Restated from latency.domain.CBOT_BOARD_LAYERS: this job runs before the
# repo's dependencies are installed. tests/test_refresh_slot.py pins them equal.
BOARD_LAYERS: tuple[str, ...] = ("prices", "forward_curve")
DCE_LAYERS: tuple[str, ...] = ("dce",)

SCHEDULE_DCE_BACKSTOP = "0 8 * * 1-5"
SCHEDULE_EVENING = "7 20 * * 1-5"
SCHEDULE_CATCHUP = "30 23 * * 1-5"

BOARD_SLOT = "board"
DCE_DISPATCH_TITLE = "Refresh Prices (dce)"
BOARD_DISPATCH_TITLE = f"Refresh Prices ({BOARD_SLOT})"

# The step whose success means "this run delivered its own layers on time".
# The workflow names it; tests/test_refresh_slot.py pins the name against it.
OWN_GATE_STEP = "Gate the published edition's latency"
REFRESH_JOB = "refresh"

# The board trigger: cron-job.org, in the job's own America/New_York zone so
# DST is handled there. Tue-Sat local, because the bar for Monday's session is
# final at 00:00 New York on Tuesday, and Friday's on Saturday.
BOARD_TIMEZONE = "America/New_York"
BOARD_FIRE_LOCAL = time(1, 0)
BOARD_FIRE_WEEKDAYS = frozenset({1, 2, 3, 4, 5})  # Mon=0: Tue..Sat
# How long after the fire time a missing board dispatch counts as missed. The
# 10:30 UTC DCE dispatch is 3h30m-4h30m past 01:00 New York; a manual dce
# dispatch inside the grace does not backstop a board run still on its way.
BOARD_BACKSTOP_GRACE = timedelta(hours=1)


class SlotError(ValueError):
    """An input this workflow cannot route. Fails the run: never a guess."""


@dataclass(frozen=True)
class SlotPlan:
    layers: str = ""          # comma-separated; empty = config.FAST_REFRESH_LAYERS
    gate: str = ""            # layers the own gate step fails on
    skip: bool = False
    late_start_ok: bool = False
    dce_backstop: bool = False
    board_backstop: bool = False
    board_gate: str = ""      # layers the board-backstop step fails on
    notices: tuple[str, ...] = field(default_factory=tuple)

    def outputs(self) -> dict[str, str]:
        return {
            "layers": self.layers,
            "gate": self.gate,
            "skip": _flag(self.skip),
            "late_start_ok": _flag(self.late_start_ok),
            "backstop": _flag(self.dce_backstop),
            "board_backstop": _flag(self.board_backstop),
            "board_gate": self.board_gate,
        }


def _flag(value: bool) -> str:
    return "true" if value else "false"


def _csv(layers: Iterable[str]) -> str:
    return ",".join(layers)


def board_due_at(now: datetime) -> datetime | None:
    """When today's board dispatch was due, in UTC, or None if none is due.

    "Today" is the New York calendar day of ``now``: the trigger is scheduled
    in that zone. A day outside Tue-Sat has no board dispatch to miss.
    """
    if now.tzinfo is None:
        raise SlotError("now must be timezone-aware")
    local = now.astimezone(ZoneInfo(BOARD_TIMEZONE))
    if local.weekday() not in BOARD_FIRE_WEEKDAYS:
        return None
    due = datetime.combine(local.date(), BOARD_FIRE_LOCAL, tzinfo=ZoneInfo(BOARD_TIMEZONE))
    return due.astimezone(timezone.utc)


def board_day_start(now: datetime) -> datetime:
    """00:00 New York of ``now``'s New York day, in UTC: the board lookup's floor."""
    local = now.astimezone(ZoneInfo(BOARD_TIMEZONE))
    start = datetime.combine(local.date(), time(0, 0), tzinfo=ZoneInfo(BOARD_TIMEZONE))
    return start.astimezone(timezone.utc)


def utc_day_start(now: datetime) -> datetime:
    """00:00 UTC of ``now``'s UTC day: the DCE lookup's floor (unchanged)."""
    day = now.astimezone(timezone.utc).date()
    return datetime.combine(day, time(0, 0), tzinfo=timezone.utc)


Delivered = Callable[[str, datetime], bool]
"""delivered(title, since) — did a dispatched run with that title deliver
its own layers on time since that instant?"""


def plan(
    *,
    event: str,
    schedule: str,
    layers_input: str,
    slot_input: str,
    now: datetime,
    delivered: Delivered,
) -> SlotPlan:
    """Route one run. Pure apart from ``delivered``, which tests stub."""
    layers_input = layers_input.strip()
    slot_input = slot_input.strip()

    if event == "workflow_dispatch":
        if slot_input == BOARD_SLOT:
            if layers_input:
                raise SlotError(
                    f"slot={BOARD_SLOT} fetches {_csv(BOARD_LAYERS)} by definition; "
                    f"layers={layers_input!r} as well is ambiguous"
                )
            return SlotPlan(layers=_csv(BOARD_LAYERS), gate=_csv(BOARD_LAYERS))
        if slot_input:
            raise SlotError(f"unknown slot {slot_input!r}; the only named slot is {BOARD_SLOT!r}")
        if layers_input == _csv(DCE_LAYERS):
            return _dce_dispatch(now, delivered)
        return SlotPlan(layers=layers_input, gate=layers_input)

    if event != "schedule":
        raise SlotError(f"Refresh Prices is not triggered by {event!r}")
    if slot_input or layers_input:
        raise SlotError("a scheduled run carries no inputs")

    if schedule == SCHEDULE_DCE_BACKSTOP:
        if delivered(DCE_DISPATCH_TITLE, utc_day_start(now)):
            return SlotPlan(
                layers=_csv(DCE_LAYERS), gate=_csv(DCE_LAYERS), skip=True,
                notices=("notice::dce was refreshed on time by a dispatched run today — "
                         "backstop stands down",),
            )
        return SlotPlan(
            layers=_csv(DCE_LAYERS), gate=_csv(DCE_LAYERS), dce_backstop=True,
            notices=("warning::no dispatched dce refresh delivered today — the outside "
                     "scheduler did not fire; fetching dce late",),
        )
    if schedule == SCHEDULE_CATCHUP:
        # Proves FX (the London-day bar closed 23:00/00:00 UTC); not the board,
        # which is final only at 00:00 New York — after this slot lands on
        # nearly every run. A scheduler-late start is reported, not failed.
        return SlotPlan(gate="currencies", late_start_ok=True)
    if schedule == SCHEDULE_EVENING:
        # Lands before both the London FX close (GMT) and New York midnight,
        # so it holds D-1 FX and D-1 CBOT by design; it gates nothing (#176).
        return SlotPlan()
    raise SlotError(f"unknown schedule {schedule!r}; add it to scripts/refresh_slot.py")


def _dce_dispatch(now: datetime, delivered: Delivered) -> SlotPlan:
    due = board_due_at(now)
    dce = _csv(DCE_LAYERS)
    if due is None:
        return SlotPlan(
            layers=dce, gate=dce,
            notices=("notice::no board dispatch is due today (it fires Tue-Sat New York) — "
                     "board backstop stands down",),
        )
    if now < due + BOARD_BACKSTOP_GRACE:
        return SlotPlan(
            layers=dce, gate=dce,
            notices=(f"notice::today's board dispatch is not yet overdue (due "
                     f"{due:%H:%M} UTC + {BOARD_BACKSTOP_GRACE}) — board backstop stands down",),
        )
    if delivered(BOARD_DISPATCH_TITLE, board_day_start(now)):
        return SlotPlan(
            layers=dce, gate=dce,
            notices=("notice::the board was refreshed on time by a dispatched run today — "
                     "board backstop stands down",),
        )
    board = _csv(BOARD_LAYERS)
    return SlotPlan(
        layers=_csv((*DCE_LAYERS, *BOARD_LAYERS)), gate=dce,
        board_backstop=True, board_gate=board,
        notices=("warning::no dispatched board refresh delivered today — the board trigger "
                 "did not fire or its run failed its gate; fetching the board late",),
    )


# ---------------------------------------------------------------------------
# The "delivered" lookup, over gh
# ---------------------------------------------------------------------------


def delivered_from_runs(
    runs: Iterable[Mapping[str, Any]],
    title: str,
    jobs_of: Callable[[int], list[Mapping[str, Any]]],
) -> bool:
    """True if any completed run titled ``title`` passed its own gate step.

    A green run passed it by construction. A red one may have too — the dce
    dispatch that went red only on the board backstop — so its jobs are read.
    """
    for run in runs:
        if run.get("displayTitle") != title or run.get("status") != "completed":
            continue
        if run.get("conclusion") == "success":
            return True
        if run.get("conclusion") != "failure":
            continue
        for job in jobs_of(int(run["databaseId"])):
            if job.get("name") != REFRESH_JOB:
                continue
            for step in job.get("steps") or ():
                if step.get("name") == OWN_GATE_STEP and step.get("conclusion") == "success":
                    return True
    return False


def _gh_json(args: list[str]) -> Any:
    # check=True: a failed lookup fails the run rather than guessing either way.
    out = subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def gh_delivered(title: str, since: datetime) -> bool:
    runs = _gh_json([
        "run", "list", "--workflow", WORKFLOW, "--event", "workflow_dispatch",
        "--created", f">={since.astimezone(timezone.utc):%Y-%m-%dT%H:%M:%SZ}",
        "--limit", "50", "--json", "databaseId,displayTitle,status,conclusion",
    ])

    def jobs_of(run_id: int) -> list[Mapping[str, Any]]:
        return list(_gh_json(["run", "view", str(run_id), "--json", "jobs"]).get("jobs") or [])

    return delivered_from_runs(runs, title, jobs_of)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--event", required=True)
    parser.add_argument("--schedule", default="")
    parser.add_argument("--layers", default="")
    parser.add_argument("--slot", default="")
    parser.add_argument("--github-output", help="append key=value outputs to this file")
    args = parser.parse_args(argv)

    try:
        result = plan(
            event=args.event, schedule=args.schedule, layers_input=args.layers,
            slot_input=args.slot, now=datetime.now(timezone.utc), delivered=gh_delivered,
        )
    except SlotError as exc:
        print(f"::error title=Unroutable Refresh Prices run::{exc}")
        return 1

    for notice in result.notices:
        print(f"::{notice}")
    outputs = result.outputs()
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            for key, value in outputs.items():
                fh.write(f"{key}={value}\n")
    print(
        f"fetching: {result.layers or 'default (prices,currencies,forward_curve)'} | "
        f"gating on: {result.gate or '-'} | skip: {_flag(result.skip)} | "
        f"board backstop: {_flag(result.board_backstop)}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
