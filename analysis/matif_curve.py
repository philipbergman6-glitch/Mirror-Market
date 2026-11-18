"""The latest MATIF rapeseed (ECO) curve off Layer 37 — PRIVATE desk read.

What a buyer asks of the Euronext board, answered from the newest stored
session per contract month:

* **Where did each delivery month last trade?** The last on-book print,
  the session's VWAP, range and lots, in EUR/t as Euronext quotes it.
* **Is the curve one session's number?** Every month is reported with its
  own session date; a month whose newest session is older than the curve's
  newest session is flagged ``stale`` rather than silently mixed in
  (invariant 8 in spirit: a spread between two months is only offered when
  both printed on the same session).

What it does not do:

* It never prints USD/MT. No USD/EUR series is stored (Layer 7 carries no
  EUR pair; the ECB cross fetcher uses USD/EUR as a leg and stores only the
  CNY cross), and invariant 7 forbids a second conversion site. The print
  says EUR/t and says why.
* It never calls anything a settlement: ``last`` is the last on-book trade
  (``board_last_traded``). The DSP is not fetched.
* It never writes under ``docs/``: ``--write`` goes to ``data/workspace/``
  through ``assert_private_path``, the guard the private workstation and
  opportunity editions use. ``config.MATIF_PUBLISH`` is False and enforced.

Read it locally: ``python -m analysis.matif_curve`` (``--write`` for a
Markdown copy in data/workspace/matif_curve.md).
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from pathlib import Path
from typing import Any

import config
from analysis.futures.privacy import assert_private_path, private_output_dir

STATE_OK = "ok"
STATE_EMPTY = "empty"
STATE_WITHHELD = "withheld"

_LAYER = "matif"
_ATTRIBUTION = "Euronext delayed trade data (MiFIR Art. 13), internal use only; ESMA FIRDS for the instrument map"
_UNIT_NOTE = "EUR/t as quoted on Euronext; no USD/EUR series is stored, so no USD/MT is printed (invariant 7)"


def latest_curve(conn: sqlite3.Connection | None, *, today: date) -> dict[str, Any]:
    """``{state, reason, data}`` — the newest session per contract month."""
    if conn is None:
        return {"state": STATE_WITHHELD, "reason": "no database connection", "data": None}
    if config.MATIF_PUBLISH:
        # The read itself is fine; the *default* audience of every reader in
        # this repo is public, and this one may only ever be private. A True
        # flag is an owner decision that has to arrive with a licence, not
        # by someone flipping a constant — refuse until the gate says so.
        return {
            "state": STATE_WITHHELD,
            "reason": "config.MATIF_PUBLISH is True: this reader is private-only",
            "data": None,
        }
    try:
        rows = conn.execute(
            "SELECT Date, contract_month, isin, firds_name, expiry_date, open, high, low, last, vwap, "
            "lots, trade_count, last_trade_at, off_book_trades, quote_kind "
            "FROM matif_futures ORDER BY contract_month, Date"
        ).fetchall()
    except sqlite3.OperationalError as exc:
        return {"state": STATE_WITHHELD, "reason": f"matif_futures unreadable: {exc}", "data": None}
    if not rows:
        return {
            "state": STATE_EMPTY,
            "reason": "no ECO session stored — run the pipeline with MATIF_PRIVATE_FETCH set",
            "data": None,
        }

    newest_by_month: dict[str, tuple] = {}
    for row in rows:
        newest_by_month[row[1]] = row  # ordered by Date within month: last wins
    curve_session = max(date.fromisoformat(r[0]) for r in newest_by_month.values())
    months = []
    for month, r in sorted(newest_by_month.items()):
        session = date.fromisoformat(r[0])
        months.append({
            "contract_month": month,
            "session": session.isoformat(),
            "stale": session < curve_session,
            "isin": r[2],
            "firds_name": r[3],
            "expiry_date": r[4],
            "open": r[5], "high": r[6], "low": r[7], "last": r[8], "vwap": r[9],
            "lots": r[10], "trade_count": r[11], "last_trade_at": r[12],
            "off_book_trades": r[13],
            "quote_kind": r[14],
        })
    same_session = [m for m in months if not m["stale"]]
    spreads = [
        {
            "near": a["contract_month"], "far": b["contract_month"],
            "far_minus_near": round(float(b["last"]) - float(a["last"]), 2),
            "session": curve_session.isoformat(),
        }
        for a, b in zip(same_session, same_session[1:], strict=False)
    ]
    return {
        "state": STATE_OK,
        "reason": None,
        "data": {
            "contract": config.MATIF_CONTRACT,
            "curve_session": curve_session.isoformat(),
            "age_days": (today - curve_session).days,
            "unit": "EUR/t",
            "unit_note": _UNIT_NOTE,
            "quote_kind": config.MATIF_QUOTE_KIND,
            "attribution": _ATTRIBUTION,
            "months": months,
            "calendar_spreads": spreads,
        },
    }


def render_markdown(envelope: dict[str, Any]) -> str:
    """The desk print as Markdown — the same text ``main`` prints."""
    if envelope["state"] != STATE_OK:
        return f"MATIF {config.MATIF_CONTRACT}: withheld — {envelope['reason']}\n"
    d = envelope["data"]
    lines = [
        f"# MATIF {d['contract']} rapeseed — last traded, session {d['curve_session']} ({d['age_days']} days ago)",
        "",
        f"PRIVATE — {d['attribution']}. Not a settlement: every price is the last on-book trade.",
        f"Unit: {d['unit_note']}.",
        "",
        "| Delivery | Session | Last | VWAP | Low–High | Lots | Trades | Last print (UTC) | Note |",
        "|---|---|---:|---:|---|---:|---:|---|---|",
    ]
    for m in d["months"]:
        note = "stale: older session" if m["stale"] else ""
        vwap = f"{m['vwap']:.2f}" if m["vwap"] is not None else "—"
        lines.append(
            f"| {m['contract_month']} | {m['session']} | {m['last']:.2f} | {vwap} | "
            f"{m['low']:.2f}–{m['high']:.2f} | {m['lots']:.0f} | {m['trade_count']} | {m['last_trade_at']} | {note} |"
        )
    if d["calendar_spreads"]:
        lines += ["", "Calendar spreads (far − near, same session, EUR/t):"]
        lines += [f"- {s['near']} → {s['far']}: {s['far_minus_near']:+.2f}" for s in d["calendar_spreads"]]
    return "\n".join(lines) + "\n"


def private_curve_target(directory: str | Path | None = None) -> Path:
    """Where ``--write`` lands: data/workspace/matif_curve.md, proven outside docs/."""
    root = Path(directory) if directory is not None else private_output_dir()
    assert_private_path(root, where="private MATIF curve")
    return assert_private_path(root / "matif_curve.md", where="private MATIF curve")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--write", action="store_true", help="also write data/workspace/matif_curve.md")
    args = parser.parse_args(argv)

    from pipeline.connection import get_connection, managed_connection

    with managed_connection(get_connection()) as conn:
        envelope = latest_curve(conn, today=date.today())
    text = render_markdown(envelope)
    print(text, end="")
    if args.write:
        target = private_curve_target()
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
        print(f"written: {target}")
    return 0 if envelope["state"] == STATE_OK else 1


if __name__ == "__main__":
    raise SystemExit(main())
