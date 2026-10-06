"""India meal export premium and oil import parity, off SEA's weekly sheet (#72).

Two questions a soy buyer asks of India, each answered per SEA sheet date:

* **Meal: what is Indian meal over the board?** SEA's soymeal FAS Kandla
  (USD/MT) less the CBOT meal close on that same session. The board leg is
  the first contract not yet at first notice — on 1 Oct 2026 the roll rule
  still names ZMV26, which is already in delivery, and a cargo is not priced
  against a delivery month. No CBOT session on the sheet date withholds the
  premium (invariant 8): the previous close would put the intervening board
  move into the Indian number.

* **Oil: where does domestic oil sit against import parity?** SEA's
  degummed CIF Mumbai landed at the duty in force that week
  (``config.INDIA_CRUDE_SOY_OIL_DUTY``) against SEA's solvent-extracted soy
  oil Indore — the domestic crusher's oil the import competes with —
  converted at the sheet date's own INR/USD print or withheld (invariant 7).
  ``gap`` is domestic minus landed: above zero, domestic oil is dearer than
  landed imports and pulls them in; below, domestic crush oil undercuts
  them. ``flipped`` marks a week whose side differs from the week before.
  This is not an importer's margin (that would be the port price, not
  Indore's, against landed cost). Duty is applied to SEA's CIF, the
  conventional approximation of India's tariff-value assessment, and port
  and handling costs are not modelled: this is an indicator, not a cost
  sheet.

Private while ``config.SEA_PUBLISH`` is False — nothing renders it on the
site. Read it locally: ``python -m analysis.india_parity``.
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from typing import Any

import config
from analysis.futures.domain import contracts_from
from analysis.origins.sources import to_usd_per_mt

STATE_OK = "ok"
STATE_EMPTY = "empty"

_LAYER = "sea_india"
_MEAL = "Soybean Meal FAS Kandla"
_CIF = "Soybean Oil CIF Mumbai"
_DOMESTIC_OIL = "Soybean Oil SE Indore"
_SERIES = (_MEAL, _CIF, _DOMESTIC_OIL)


def _sheets(conn: sqlite3.Connection, weeks: int) -> list[tuple[date, dict[str, float]]]:
    """The newest ``weeks`` sheet dates, oldest first, with their three legs."""
    placeholders = ", ".join("?" for _ in _SERIES)
    rows = conn.execute(
        f"SELECT Date, series, value, unit FROM sea_india_rates "  # noqa: S608 - fixed identifiers
        f"WHERE series IN ({placeholders}) ORDER BY Date",
        _SERIES,
    ).fetchall()
    by_date: dict[date, dict[str, float]] = {}
    for raw_date, series, value, unit in rows:
        expected = config.SEA_SERIES[series]["unit"]
        if unit != expected:
            raise ValueError(f"sea_india_rates: {series} stored in {unit!r}, expected {expected!r}")
        by_date.setdefault(date.fromisoformat(str(raw_date)[:10]), {})[series] = float(value)
    return sorted(by_date.items())[-weeks:]


def duty_on(day: date) -> tuple[float, str] | None:
    """(effective crude soy oil import duty, its source) in force on ``day``."""
    in_force = [entry for entry in config.INDIA_CRUDE_SOY_OIL_DUTY if entry[0] <= day]
    if not in_force:
        return None
    _, rate, source = max(in_force, key=lambda entry: entry[0])
    return rate, source


def _board_contract(day: date):
    """The first CBOT meal contract not yet at first notice on ``day``."""
    for contract in contracts_from("Soybean Meal", day, count=6):
        if contract.first_notice is not None and contract.first_notice > day:
            return contract
    return None


def _meal(provider, day: date, fas: float | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "fas_usd_mt": fas, "board_contract": None, "board_usd_mt": None,
        "premium_usd_mt": None, "note": None,
    }
    if fas is None:
        out["note"] = "SEA did not quote meal FAS Kandla on this sheet (NQ)"
        return out
    contract = _board_contract(day)
    if contract is None:
        out["note"] = f"no CBOT meal contract with a first notice after {day} is encoded"
        return out
    out["board_contract"] = contract.symbol
    # Layer 11b's named-contract bars: one contract across sessions.
    closes = provider.close_history(contract, as_of=day, sessions=1)
    quote = closes[-1] if closes else None
    if quote is None or quote.observation_date != day:
        seen = f"its newest close is {quote.observation_date}" if quote else "no close is stored"
        out["note"] = f"no {contract.symbol} close on {day} ({seen}) — premium withheld"
        return out
    out["board_usd_mt"] = quote.usd_per_mt
    out["premium_usd_mt"] = fas - quote.usd_per_mt
    return out


def _oil(provider, day: date, cif: float | None, domestic_inr: float | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "cif_usd_mt": cif, "duty": None, "duty_source": None, "landed_usd_mt": None,
        "domestic_inr_mt": domestic_inr, "domestic_usd_mt": None, "gap_usd_mt": None,
        "above_parity": None, "flipped": False, "note": None,
    }
    notes: list[str] = []
    duty = duty_on(day)
    if duty is None:
        first = config.INDIA_CRUDE_SOY_OIL_DUTY[0][0]
        notes.append(f"the duty schedule starts {first}; no rate is modelled for {day}")
    elif cif is None:
        notes.append("SEA did not quote degum CIF Mumbai on this sheet (NQ)")
    else:
        out["duty"], out["duty_source"] = duty
        out["landed_usd_mt"] = cif * (1 + duty[0])

    pair = config.MARKETS["india"]["currency_pair"]
    if domestic_inr is None:
        notes.append("SEA did not quote SE soy oil Indore on this sheet (NQ)")
    else:
        fx = provider.fx_rate(pair, on=day)
        if fx is None or fx[0] != day:
            seen = f"newest is {fx[0]}" if fx else "none stored"
            notes.append(f"no {pair} print on {day} ({seen}) — domestic leg withheld")
        else:
            out["domestic_usd_mt"] = to_usd_per_mt(
                domestic_inr, unit="home_per_mt", key=_DOMESTIC_OIL, fx=fx[1],
            )

    if out["landed_usd_mt"] is not None and out["domestic_usd_mt"] is not None:
        out["gap_usd_mt"] = out["domestic_usd_mt"] - out["landed_usd_mt"]
        out["above_parity"] = out["gap_usd_mt"] > 0
    out["note"] = "; ".join(notes) or None
    return out


def india_parity(
    conn: sqlite3.Connection | None, *, today: date, provider=None, weeks: int = 13,
) -> dict[str, Any]:
    """``{state, reason, data}`` for the newest ``weeks`` SEA sheets.

    ``data.weeks`` is oldest first, one entry per sheet date with a ``meal``
    and an ``oil`` block; a leg that cannot be struck is None with a note.
    """
    def withheld(reason: str) -> dict[str, Any]:
        return {"state": STATE_EMPTY, "reason": reason, "data": {}}

    if conn is None:
        return withheld("no database connection")
    try:
        sheets = _sheets(conn, weeks)
    except sqlite3.OperationalError:
        return withheld("the sea_india_rates table does not exist in this database")
    if not sheets:
        return withheld("no SEA rate sheets are stored — Layer 32 (sea_india) has not run here")

    newest = sheets[-1][0]
    age = (today - newest).days
    budget = config.LAYER_MAX_DATA_AGE_DAYS[_LAYER]
    if age > budget:
        return withheld(
            f"the newest SEA sheet is dated {newest}, {age} days ago — past the layer's "
            f"{budget}-day budget, so the feed is treated as stopped, not quiet"
        )

    if provider is None:
        from analysis.futures.providers import open_provider

        provider = open_provider(conn)

    result: list[dict[str, Any]] = []
    previous_verdict: bool | None = None
    for day, legs in sheets:
        oil = _oil(provider, day, legs.get(_CIF), legs.get(_DOMESTIC_OIL))
        verdict = oil["above_parity"]
        oil["flipped"] = verdict is not None and previous_verdict is not None and verdict != previous_verdict
        if verdict is not None:
            previous_verdict = verdict
        result.append({
            "as_on": day.isoformat(),
            "meal": _meal(provider, day, legs.get(_MEAL)),
            "oil": oil,
        })

    return {
        "state": STATE_OK,
        "reason": "",
        "data": {
            "weeks": result,
            "newest": newest.isoformat(),
            "age_days": age,
            "attribution": config.SEA_ATTRIBUTION,
            "published": config.SEA_PUBLISH,
        },
    }


def _fmt(value: float | None, spec: str = ",.0f") -> str:
    return "—" if value is None else format(value, spec)


def main(argv: list[str] | None = None) -> int:
    """Print the parity table from the local database (the private read)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--weeks", type=int, default=13)
    args = parser.parse_args(argv)

    from pipeline.connection import get_connection, managed_connection

    with managed_connection(get_connection()) as conn:
        envelope = india_parity(conn, today=date.today(), weeks=args.weeks)
    if envelope["state"] != STATE_OK:
        print(f"withheld: {envelope['reason']}")
        return 1
    data = envelope["data"]
    print(f"India parity — {data['attribution']} — PRIVATE (SEA_PUBLISH={data['published']})")
    print(
        f"{'as on':<10}  {'meal FAS':>8} {'board':>14} {'premium':>8}   "
        f"{'CIF':>6} {'duty':>5} {'landed':>7} {'SE Indore':>9} {'gap':>6}  domestic vs import parity"
    )
    for week in data["weeks"]:
        meal, oil = week["meal"], week["oil"]
        board = (
            f"{meal['board_contract']} {_fmt(meal['board_usd_mt'])}"
            if meal["board_contract"] else "—"
        )
        verdict = {True: "above", False: "below", None: "—"}[oil["above_parity"]]
        if oil["flipped"]:
            verdict += "  ← flipped"
        print(
            f"{week['as_on']:<10}  {_fmt(meal['fas_usd_mt']):>8} {board:>14} "
            f"{_fmt(meal['premium_usd_mt'], '+,.0f'):>8}   {_fmt(oil['cif_usd_mt']):>6} "
            f"{_fmt(oil['duty'], '.1%'):>5} {_fmt(oil['landed_usd_mt']):>7} "
            f"{_fmt(oil['domestic_usd_mt']):>9} {_fmt(oil['gap_usd_mt'], '+,.0f'):>6}  {verdict}"
        )
        for note in (meal["note"], oil["note"]):
            if note:
                print(f"{'':<12}note: {note}")
    print("All values USD/MT. Landed = CIF × (1 + duty); port/handling costs not modelled.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
