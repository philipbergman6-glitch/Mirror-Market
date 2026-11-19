"""Summarize fresh God’s Eye View AIS observations inside a desk-supplied port area."""

from __future__ import annotations

import argparse
import json
import math
import time
from collections import Counter

import requests


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    area = parser.add_mutually_exclusive_group(required=True)
    area.add_argument("--bounds", nargs=4, type=float, metavar=("S", "W", "N", "E"))
    area.add_argument("--area", choices=("us-gulf-coast",), help="US Gulf Coast regional context")
    parser.add_argument("--port", type=int, default=4173, help="God’s Eye View local server port")
    parser.add_argument("--max-age-minutes", type=int, default=30)
    args = parser.parse_args()
    # Regional coverage includes Texas through Florida. Use --bounds for an individual port.
    south, west, north, east = args.bounds or (25.0, -98.0, 31.0, -82.0)
    if not (-90 <= south < north <= 90 and -180 <= west < east <= 180):
        parser.error("bounds must be ordered south west north east and within coordinate limits")
    if not 1 <= args.port <= 65535 or args.max_age_minutes < 1:
        parser.error("use a valid local port and a positive maximum age")

    try:
        response = requests.get(
            f"http://localhost:{args.port}/api/vessels",
            timeout=10,
            allow_redirects=False,
        )
        response.raise_for_status()
        feed = response.json()
        if not isinstance(feed, dict) or not isinstance(feed.get("rows"), list):
            raise ValueError("unexpected vessel snapshot shape")
    except (requests.RequestException, ValueError) as exc:
        print(json.dumps({"state": "unavailable", "reason": str(exc)}))
        return 1

    if feed.get("status") != "live":
        print(json.dumps({"state": "unavailable", "reason": "AIS feed is not live"}))
        return 1

    now = time.time()
    vessels: dict[str, tuple[float, str | None]] = {}
    for row in feed["rows"]:
        if not isinstance(row, dict) or not row.get("mmsi"):
            continue
        try:
            latitude = float(row["lat"])
            longitude = float(row["lon"])
            epoch = float(row["last_position_epoch"])
        except (KeyError, TypeError, ValueError):
            continue
        if not math.isfinite(epoch) or not 0 <= now - epoch <= args.max_age_minutes * 60:
            continue
        if south <= latitude <= north and west <= longitude <= east:
            identity = str(row["mmsi"])
            if identity not in vessels or epoch > vessels[identity][0]:
                vessel_type = row.get("type")
                vessels[identity] = (epoch, str(vessel_type) if vessel_type else None)

    if not vessels:
        print(json.dumps({"state": "unavailable", "reason": "no fresh AIS observations inside bounds"}))
        return 1
    counts = Counter(kind or "unknown" for _, kind in vessels.values())
    print(json.dumps({
        "state": "ok",
        "source": "God’s Eye View / AISStream",
        "observed_vessels": len(vessels),
        "by_reported_type": dict(sorted(counts.items())),
        "max_age_minutes": args.max_age_minutes,
        "bounds": [south, west, north, east],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
