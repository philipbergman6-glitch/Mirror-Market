#!/usr/bin/env python
"""Drop forward-curve legs left behind by a same-day re-run.

Why this exists as a one-off script rather than as pipeline code
----------------------------------------------------------------
``forward_curve`` is keyed ``(commodity, contract_month, fetched_date)`` and,
until Phase 3, nothing ever deleted from it. Two pipeline runs on one calendar
day therefore left the earlier run's legs standing beside the later run's: same
key space, different sessions, and nothing in a row's shape marking which run
it came from. ``pipeline.store._replace_curve_snapshot`` stops that happening
again, and ``analysis.futures.providers._coherence`` catches it at read time —
but neither cleans up rows already written, and the committed history CSV holds
some.

The committed rows cannot be fixed in a pull request: ``.github/workflows/
ci.yml`` has a ``history-guard`` job that fails any PR touching
``data/history/``, because those files are written by the daily deploy workflow
and a hand-edit there is indistinguishable from a fabricated observation. So
the cleanup is a deliberate operator action, run against a database, with the
CSV rewritten by the ordinary export path afterwards.

What counts as a leftover
-------------------------
Within one ``(commodity, fetched_date)`` group, the legs of a real curve all
carry the same ``observation_date`` — that is the fetcher's own rule (#61) and
the reason a curve is a single session. So:

* Legs on the group's **newest non-null** observation date are kept.
* Legs on an **older** observation date are leftovers from a run that the newer
  one did not re-fetch.
* Legs with a **NULL** observation date, in a group that also has a non-null
  one, are the same thing from before the column existed.
* A group where **every** leg is NULL is left completely alone. Those are
  legacy rows predating the ``observation_date`` column (everything before
  2026-08-11 here), and there is no evidence in them of anything being wrong.
  Deleting them would destroy real history to tidy a schema change.

Usage
-----
    # Look, change nothing. This is the default.
    python scripts/prune_curve_snapshots.py

    # Actually delete, against the default database.
    python scripts/prune_curve_snapshots.py --apply

    # Then rewrite the committed CSV. The shrink override is required and is
    # the point at which a human is asserting the smaller table is correct:
    MIRROR_HISTORY_ALLOW_SHRINK=1 python -c \
        "from pipeline.history import export_history; export_history()"

The export refuses a shrinking row count without that flag, which is exactly
the guard that should fire here — this is the rare case where fewer rows is the
intended outcome.
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass

logger = logging.getLogger("prune_curve_snapshots")


@dataclass(frozen=True)
class Leftover:
    """One leg that is not part of its group's newest snapshot."""

    commodity: str
    contract_month: str
    fetched_date: str
    observation_date: str | None
    kept_observation_date: str
    ticker: str

    def describe(self) -> str:
        stamped = self.observation_date or "NULL"
        return (
            f"{self.commodity} {self.fetched_date}: {self.ticker} "
            f"observed {stamped}, snapshot is {self.kept_observation_date}"
        )


def find_leftovers(conn: sqlite3.Connection) -> list[Leftover]:
    """Every leg that a same-day re-run should have replaced. Read-only."""
    rows = conn.execute(
        "SELECT commodity, contract_month, fetched_date, observation_date, ticker "
        "FROM forward_curve"
    ).fetchall()

    groups: dict[tuple[str, str], list[tuple]] = defaultdict(list)
    for row in rows:
        groups[(row[0], row[2])].append(row)

    leftovers: list[Leftover] = []
    for (commodity, fetched_date), legs in sorted(groups.items()):
        observed = {leg[3] for leg in legs if leg[3]}
        if not observed:
            # Wholly pre-column. Nothing here says anything is wrong.
            continue
        newest = max(observed)
        for leg in legs:
            if leg[3] == newest:
                continue
            leftovers.append(Leftover(
                commodity=commodity,
                contract_month=leg[1],
                fetched_date=fetched_date,
                observation_date=leg[3],
                kept_observation_date=newest,
                ticker=leg[4] or "",
            ))
    return leftovers


def prune(conn: sqlite3.Connection, leftovers: list[Leftover]) -> int:
    """Delete the given legs. Keyed on the primary key plus the ticker."""
    if not leftovers:
        return 0
    conn.executemany(
        "DELETE FROM forward_curve "
        "WHERE commodity = ? AND contract_month = ? AND fetched_date = ?",
        [(item.commodity, item.contract_month, item.fetched_date) for item in leftovers],
    )
    conn.commit()
    return len(leftovers)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument(
        "--apply", action="store_true",
        help="actually delete. Without it the script only reports.",
    )
    parser.add_argument(
        "--database", default=None,
        help="path to the SQLite database (default: the project's configured one)",
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.database:
        conn = sqlite3.connect(args.database)
    else:
        from pipeline.connection import get_connection
        conn = get_connection()

    try:
        leftovers = find_leftovers(conn)
        if not leftovers:
            logger.info("no leftover curve legs — every snapshot is one session")
            return 0

        logger.info("%d leftover leg(s):", len(leftovers))
        for item in leftovers:
            logger.info("  %s", item.describe())

        if not args.apply:
            logger.info("\nDry run. Re-run with --apply to delete these.")
            return 0

        removed = prune(conn, leftovers)
        logger.info("\ndeleted %d leg(s)", removed)
        logger.info(
            "Now rewrite the committed CSV:\n"
            "  MIRROR_HISTORY_ALLOW_SHRINK=1 python -c "
            '"from pipeline.history import export_history; export_history()"\n'
            "The flag is required because export_history refuses a shrinking table by "
            "default — here the shrink is the point."
        )
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
