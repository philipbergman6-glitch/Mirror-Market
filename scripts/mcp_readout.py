"""Two-week readout of the MCP edition's question log (#581).

Reads the JSONL the server appends on the host (one line per tool call:
timestamp, tool, arguments, outcome — never the answer), and prints the
inputs to the trial's pass/fail criteria on #578: usage days, calls per day,
tools used, failure rate, first and last call.

A malformed line is an error, not a skipped row: the log is the evidence and
a log that cannot be read in full cannot grade a trial.

Usage: python scripts/mcp_readout.py .scratch/questions.jsonl
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

REQUIRED = ("ts", "tool", "status")


class ReadoutError(RuntimeError):
    pass


def read_log(path: Path) -> list[dict]:
    if not path.is_file():
        raise ReadoutError(f"no log at {path}")
    records: list[dict] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise ReadoutError(f"line {number} is not JSON: {exc}") from exc
        missing = [k for k in REQUIRED if k not in record]
        if missing:
            raise ReadoutError(f"line {number} lacks {missing}")
        try:
            datetime.fromisoformat(record["ts"])
        except ValueError as exc:
            raise ReadoutError(f"line {number} has an unreadable timestamp {record['ts']!r}") from exc
        records.append(record)
    return records


def summarise(records: list[dict]) -> dict:
    if not records:
        return {"calls": 0, "usage_days": 0, "days": {}, "tools": {}, "failed": 0, "failure_rate": None,
                "first_call": None, "last_call": None}
    days = Counter(datetime.fromisoformat(r["ts"]).date().isoformat() for r in records)
    tools = Counter(r["tool"] for r in records)
    failed = sum(1 for r in records if r["status"] != "completed")
    stamps = sorted(r["ts"] for r in records)
    return {
        "calls": len(records),
        "usage_days": len(days),
        "days": dict(sorted(days.items())),
        "tools": dict(tools.most_common()),
        "failed": failed,
        "failure_rate": round(failed / len(records), 3),
        "first_call": stamps[0],
        "last_call": stamps[-1],
    }


def render(summary: dict) -> str:
    lines = [
        f"calls            {summary['calls']}",
        f"usage days       {summary['usage_days']}",
        f"first call       {summary['first_call']}",
        f"last call        {summary['last_call']}",
        f"failed calls     {summary['failed']}"
        + (f" ({summary['failure_rate']:.1%})" if summary["failure_rate"] is not None else ""),
        "",
        "calls per day",
    ]
    lines += [f"  {day}  {n}" for day, n in summary["days"].items()] or ["  (none)"]
    lines += ["", "tools used"]
    lines += [f"  {tool:<16}{n}" for tool, n in summary["tools"].items()] or ["  (none)"]
    return "\n".join(lines)


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        print(render(summarise(read_log(Path(argv[1])))))
    except ReadoutError as exc:
        print(f"readout: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
