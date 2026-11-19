"""Report the promoted market basket against a desk's next-weekday cutoff."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import date, datetime, time
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from analysis.futures.domain import UnknownContract, named_contract  # noqa: E402
from analysis.futures.providers import SqliteQuoteProvider  # noqa: E402
from analysis.origins.domain import OriginQuote  # noqa: E402
from analysis.origins.sources import read_origin_quotes  # noqa: E402
from config import (  # noqa: E402
    AVAILABILITY_BASKET,
    GTR_OCEAN_ROUTES,
    LAYER_MAX_DATA_AGE_DAYS,
    ORIGIN_LEGS,
    PRIVATE_LAYERS,
    PRODUCTION_LAYER_KEYS,
)
from pipeline.layer_runs import read_runs  # noqa: E402
from pipeline.reliability import decision_cutoff_report, previous_session  # noqa: E402


def _physical_row(quote: OriginQuote, provider: SqliteQuoteProvider, today: date) -> dict:
    row = {**quote.to_dict(), "state": quote.freshness(today).value}
    ref = quote.board_contract
    if ref is not None and ref.exchange == "cbot" and ref.code == "ZS":
        try:
            month = date.fromisoformat(f"{ref.delivery_month}-01")
            contract = named_contract("Soybeans", month.year, month.month)
        except (UnknownContract, ValueError):
            row["basis"] = {"state": "unavailable", "reason": "unrecognized CBOT soybean contract month"}
            return row
        closes = provider.close_history(contract, as_of=quote.observation_date, sessions=1)
        if closes and closes[-1].observation_date == quote.observation_date:
            close = closes[-1]
            row["basis"] = {
                "state": "observed",
                "usd_per_mt": quote.price.amount - close.usd_per_mt,
                "reference_close": close.to_dict(),
            }
        else:
            row["basis"] = {"state": "unavailable", "reason": f"no same-session close for {contract.symbol}"}
    elif quote.source.layer == "gulf_bids":
        row["basis"] = {"state": "unavailable", "reason": "physical bid has no single named board contract"}
    return row


def current_board(conn: sqlite3.Connection, *, today: date) -> dict:
    """Read the critical market observations on their own terms and dates."""
    provider = SqliteQuoteProvider(conn)
    curves = {}
    for commodity in ("Soybeans", "Soybean Meal", "Soybean Oil"):
        curve = provider.curve(commodity, as_of=today)
        curves[commodity] = {
            "state": curve.freshness.value if curve.legs else "unavailable",
            "coherent": curve.coherent,
            "reason": curve.coherence_note or ("no named contract closes" if not curve.legs else None),
            "contracts": [quote.to_dict() for quote in curve.legs],
        }

    origins = {}
    for leg, descriptor in ORIGIN_LEGS.items():
        reason = descriptor.get("absent_reason")
        quotes = () if reason else read_origin_quotes(conn, leg, today=today)
        rows = [
            _physical_row(quote, provider, today)
            for quote in quotes if quote.observation_date <= today
        ]
        origins[leg] = {
            "state": "observed" if rows else "unavailable",
            "reason": None if rows else reason or "no dated comparable-unit observation",
            "label": descriptor["label"],
            "quotes": rows,
        }

    fx = {}
    for pair in ("BRL/USD", "CNY/USD"):
        observed = provider.fx_rate(pair, on=today)
        fx[pair] = (
            {
                "state": "observed", "observed_on": observed[0].isoformat(),
                "usd_per_unit": observed[1], "source": "stored currencies close",
            }
            if observed else {"state": "unavailable", "reason": "no close within the FX alignment window"}
        )

    freight: dict[str, dict[str, object]] = {}
    has_freight = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'ocean_freight_rates'"
    ).fetchone()
    for route in GTR_OCEAN_ROUTES.values():
        row = conn.execute(
            "SELECT Date, rate_usd_mt, cadence, quote_kind, attribution "
            "FROM ocean_freight_rates WHERE route = ? AND Date <= ? "
            "ORDER BY Date DESC LIMIT 1",
            (route, today.isoformat()),
        ).fetchone() if has_freight else None
        if row is None:
            freight[route] = {"state": "unavailable", "reason": "no GTR benchmark observation"}
            continue
        observed_on = date.fromisoformat(row[0])
        age = (today - observed_on).days
        budget = LAYER_MAX_DATA_AGE_DAYS["gtr_ocean_freight"]
        freight[route] = {
            "state": "stale" if age > budget else "within_source_age_budget",
            "age_days": age,
            "source_age_budget_days": budget,
            "observed_on": row[0],
            "usd_per_mt": row[1],
            "cadence": row[2],
            "quote_kind": row[3],
            "source": row[4],
        }
    return {
        "as_of": today.isoformat(),
        "cbot": curves,
        "physical_origins": origins,
        "fx": fx,
        "freight_benchmarks": freight,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cutoff", required=True, type=time.fromisoformat, help="local time, for example 08:00")
    parser.add_argument("--timezone", required=True, help="IANA time zone, for example Asia/Jerusalem")
    parser.add_argument("--through", type=date.fromisoformat, help="last market session to include")
    parser.add_argument("--sessions", type=int, default=30)
    parser.add_argument("--layers", nargs="+", default=AVAILABILITY_BASKET, help="public production layer keys")
    parser.add_argument("--db", type=Path, help="include market observations from this SQLite snapshot")
    args = parser.parse_args(argv)
    try:
        zone = ZoneInfo(args.timezone)
    except (ValueError, ZoneInfoNotFoundError):
        parser.error("timezone must be an IANA time zone")
    if args.cutoff.tzinfo is not None:
        parser.error("cutoff must use local time without an offset")
    if args.sessions < 1 or len(args.layers) != len(set(args.layers)):
        parser.error("require a positive session count and unique layers")
    unknown = set(args.layers) - (set(PRODUCTION_LAYER_KEYS) - set(PRIVATE_LAYERS))
    if unknown:
        parser.error(f"unknown or private layers: {', '.join(sorted(unknown))}")

    now = datetime.now(zone)
    latest = previous_session(now.date())
    if now.weekday() >= 5 or now < datetime.combine(now.date(), args.cutoff, zone):
        latest = previous_session(latest)
    if args.through is not None and args.through > latest:
        parser.error(f"cutoff has not elapsed for that window; latest complete session is {latest}")
    report = decision_cutoff_report(
        read_runs(), through=args.through or latest, cutoff_time=args.cutoff,
        cutoff_zone=zone, basket=args.layers, window=args.sessions,
    )
    if args.db is not None:
        if not args.db.is_file():
            parser.error("database must be an existing SQLite snapshot")
        with closing(sqlite3.connect(f"{args.db.resolve().as_uri()}?mode=ro", uri=True)) as conn:
            conn.execute("BEGIN")
            report["market_board"] = current_board(conn, today=now.date())
    print(json.dumps(report, indent=2, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
