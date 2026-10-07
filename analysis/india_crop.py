"""India's soybean crop off SOPA's state-wise estimate (Layer 33).

What a buyer asks of the Indian crop, answered from the newest reading of
SOPA's kharif estimate:

* **How big is it, and against last year?** The all-India estimated
  production, and its change against the prior kharif as SOPA currently
  publishes it (both sides are the newest reading of their year — SOPA
  revises in place, so "last year" is last year's revised figure, not its
  first estimate).

* **Where is it?** Each state's share of the national crop, which is what
  decides whether a Madhya Pradesh mandi price or a Maharashtra one is the
  one to watch (Layer 16 carries both).

* **Has SOPA moved its number?** The readings this database has kept of
  the current kharif, oldest first, where consecutive readings differ. The
  page is overwritten upstream, so this path exists only where a run
  snapshotted it — on a local DB, not the ephemeral CI one.

Licence boundary: SOPA reserves all rights, so while ``config.SOPA_PUBLISH``
is False the raw table never renders. What this module returns is the
public-safe derivation — percentages and shares, plus one headline total
that must carry ``data.attribution`` wherever it is shown. Read it locally:
``python -m analysis.india_crop``.
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import date
from typing import Any

import config
from pipeline.units import lakh_tonnes_to_metric_tons

STATE_OK = "ok"
STATE_EMPTY = "empty"

_LAYER = "sopa_crop"
_FIELDS = ("area_lakh_ha", "yield_kg_ha", "production_lakh_t")


def _readings(conn: sqlite3.Connection) -> dict[int, dict[date, dict[str, dict[str, float]]]]:
    """``{crop_year: {fetched_date: {state: {field: value}}}}`` for everything stored."""
    rows = conn.execute(
        "SELECT crop_year, fetched_date, state, area_lakh_ha, yield_kg_ha, production_lakh_t "
        "FROM sopa_crop_estimates ORDER BY crop_year, fetched_date",
    ).fetchall()
    out: dict[int, dict[date, dict[str, dict[str, float]]]] = {}
    for year, fetched, state, area, yld, prod in rows:
        reading = out.setdefault(int(year), {}).setdefault(date.fromisoformat(fetched), {})
        reading[state] = {"area_lakh_ha": area, "yield_kg_ha": yld, "production_lakh_t": prod}
    return out


def _pct(current: float, prior: float) -> float:
    return (current / prior - 1.0) * 100.0


def india_crop(conn: sqlite3.Connection | None, *, today: date) -> dict[str, Any]:
    """``{state, reason, data}`` for the newest kharif reading stored.

    ``data.headline`` is the one figure that may render verbatim (with
    ``data.attribution``); ``data.yoy`` and ``data.state_shares`` are
    derived and carry no SOPA number. ``data.revisions`` is the kept
    reading path of that kharif.
    """
    def withheld(reason: str) -> dict[str, Any]:
        return {"state": STATE_EMPTY, "reason": reason, "data": {}}

    if conn is None:
        return withheld("no database connection")
    try:
        readings = _readings(conn)
    except sqlite3.OperationalError:
        return withheld("the sopa_crop_estimates table does not exist in this database")
    if not readings:
        return withheld("no SOPA estimate is stored — Layer 33 (sopa_crop) has not run here")

    crop_year = max(readings)
    by_date = readings[crop_year]
    read_on = max(by_date)
    current = by_date[read_on]
    total = current.get(config.SOPA_ALL_INDIA)
    if total is None:
        return withheld(f"the {read_on} reading of kharif {crop_year} has no all-India total row")
    age = (today - read_on).days
    budget = config.FRESHNESS_WARNING_DAYS_BY_LAYER[_LAYER]
    if age > budget:
        return withheld(
            f"the newest SOPA reading is from {read_on}, {age} days ago — past the layer's "
            f"{budget}-day budget, so the feed is treated as stopped, not quiet"
        )

    states = {name: row for name, row in current.items() if name != config.SOPA_ALL_INDIA}
    share_of = {
        name: round(row["production_lakh_t"] / total["production_lakh_t"] * 100.0, 1)
        for name, row in states.items()
    }
    shares: list[dict[str, Any]] = [
        {"state": name, "share_pct": share}
        for name, share in sorted(share_of.items(), key=lambda item: -item[1])
    ]

    yoy: dict[str, Any] | None = None
    prior_year = crop_year - 1
    if prior_year in readings:
        prior_read_on = max(readings[prior_year])
        prior_total = readings[prior_year][prior_read_on].get(config.SOPA_ALL_INDIA)
        if prior_total is not None:
            yoy = {
                "prior_year": prior_year,
                "prior_read_on": prior_read_on.isoformat(),
                **{
                    f"{field}_pct": round(_pct(total[field], prior_total[field]), 1)
                    for field in _FIELDS
                },
                "note": "prior kharif as SOPA currently publishes it (revised), not its first estimate",
            }

    revisions: list[dict[str, Any]] = []
    for fetched in sorted(by_date):
        reading_total = by_date[fetched].get(config.SOPA_ALL_INDIA)
        if reading_total is None:
            continue
        if revisions and revisions[-1]["production_lakh_t"] == reading_total["production_lakh_t"]:
            continue
        revisions.append({
            "read_on": fetched.isoformat(),
            "production_lakh_t": reading_total["production_lakh_t"],
        })

    return {
        "state": STATE_OK,
        "reason": "",
        "data": {
            "crop_year": crop_year,
            "read_on": read_on.isoformat(),
            "age_days": age,
            "headline": {
                "production_lakh_t": total["production_lakh_t"],
                "production_mt": lakh_tonnes_to_metric_tons(total["production_lakh_t"]),
            },
            "yoy": yoy,
            "state_shares": shares,
            "revisions": revisions,
            "attribution": config.SOPA_ATTRIBUTION,
            "published": config.SOPA_PUBLISH,
        },
    }


def main(argv: list[str] | None = None) -> int:
    """Print the crop read from the local database (the private read)."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.parse_args(argv)

    from pipeline.connection import get_connection, managed_connection

    with managed_connection(get_connection()) as conn:
        envelope = india_crop(conn, today=date.today())
    if envelope["state"] != STATE_OK:
        print(f"withheld: {envelope['reason']}")
        return 1
    data = envelope["data"]
    headline = data["headline"]
    print(
        f"India kharif {data['crop_year']} — {data['attribution']} — "
        f"PRIVATE (SOPA_PUBLISH={data['published']})"
    )
    print(
        f"read {data['read_on']} ({data['age_days']} days ago): "
        f"{headline['production_lakh_t']:.3f} lakh t = {headline['production_mt']:,.0f} MT"
    )
    yoy = data["yoy"]
    if yoy is None:
        print("year-on-year: withheld — the prior kharif is not stored")
    else:
        print(
            f"vs kharif {yoy['prior_year']}: production {yoy['production_lakh_t_pct']:+.1f}%, "
            f"area {yoy['area_lakh_ha_pct']:+.1f}%, yield {yoy['yield_kg_ha_pct']:+.1f}%  "
            f"({yoy['note']})"
        )
    print("state shares of the crop:")
    for entry in data["state_shares"]:
        print(f"  {entry['state']:<18} {entry['share_pct']:>5.1f}%")
    if len(data["revisions"]) > 1:
        path = " → ".join(f"{r['production_lakh_t']:.3f} ({r['read_on']})" for r in data["revisions"])
        print(f"readings kept here: {path} lakh t")
    else:
        print("readings kept here: one — revisions accumulate only where runs snapshot the page")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
