"""Refuse to publish a database the hosted MCP edition must never serve (invariant 4, #580).

Run by the deploy workflow on the CI-built SQLite before ``fly deploy``. Exit 0
means every check passed; any other exit names the first failure. Four checks,
each a distinct reason:

1. every table is in the pipeline's schema roster — a desk table cannot be present;
2. no table belongs to a layer in ``config.PRIVATE_LAYERS``;
3. every stored ``assumption_set_id`` is the public tier's own id — a ranking
   costed on the desk's private assumptions cannot be present;
4. the client-record guard passes on the newest rows of every servable table,
   with stored JSON blobs parsed so a client key inside one is seen as a key.

Usage: python scripts/mcp_privacy_check.py data/storage/mirror_market.db
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analysis.futures.privacy import AUDIENCE_PUBLIC, ClientDataLeak, assert_no_client_records  # noqa: E402
from analysis.origins.assumptions import load_assumptions  # noqa: E402
from mirror_mcp import db  # noqa: E402

SAMPLE_ROWS = 50
SET_ID_TABLES = ("origin_rankings", "opportunity_detections")


class PrivacyCheckFailed(RuntimeError):
    pass


def _parse_json_text(value: Any) -> Any:
    """A stored JSON blob is walked as structure, not scanned as text, so a key inside it is seen as a key."""
    if isinstance(value, str) and value[:1] in "[{":
        try:
            return json.loads(value)
        except ValueError:
            return value
    return value


def _columns(conn, table: str) -> set[str]:
    return {r[1] for r in conn.execute(f'PRAGMA table_info("{table}")')}


def check(db_path: Path, *, public_set_id: str | None = None) -> list[str]:
    """Every check that passed, in order; raises PrivacyCheckFailed at the first that did not."""
    if not db_path.is_file():
        raise PrivacyCheckFailed(f"no database at {db_path}")
    conn = db.open_readonly(db_path)
    passed: list[str] = []
    try:
        try:
            db.assert_public_roster(conn)
        except db.PrivacyRefusal as exc:
            raise PrivacyCheckFailed(str(exc)) from exc
        passed.append("roster: every table is in the schema roster and none belongs to a private layer")

        expected = public_set_id if public_set_id is not None else load_assumptions(audience=AUDIENCE_PUBLIC).set_id
        present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for table in SET_ID_TABLES:
            if table not in present or "assumption_set_id" not in _columns(conn, table):
                continue
            foreign = sorted(
                r[0] for r in conn.execute(
                    f'SELECT DISTINCT "assumption_set_id" FROM "{table}" WHERE "assumption_set_id" != ?', (expected,)
                )
            )
            if foreign:
                raise PrivacyCheckFailed(
                    f"{table} holds rows costed on an assumption set that is not the public tier's "
                    f"({expected}): {foreign}"
                )
        passed.append(f"assumptions: every stored assumption_set_id is the public tier's ({expected})")

        for table in sorted(db.servable_tables() & present):
            spec = db.ALLOWED[table]
            rows = [
                {k: _parse_json_text(v) for k, v in dict(r).items()} for r in conn.execute(
                    f'SELECT * FROM "{table}" ORDER BY "{spec.date_column}" DESC LIMIT ?', (SAMPLE_ROWS,)
                )
            ]
            try:
                assert_no_client_records(rows, where=f"{table} newest {SAMPLE_ROWS} rows")
            except ClientDataLeak as exc:
                raise PrivacyCheckFailed(f"client-record guard: {exc}") from exc
        passed.append(f"client records: guard passed on the newest {SAMPLE_ROWS} rows of every servable table")
    finally:
        conn.close()
    return passed


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        for line in check(Path(argv[1])):
            print(f"ok   {line}")
    except PrivacyCheckFailed as exc:
        print(f"FAIL {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
