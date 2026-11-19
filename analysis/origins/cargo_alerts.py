"""Private alerts derived from saved cargo comparisons."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from analysis.futures.alerts import Alert
from analysis.futures.privacy import ClientDataLeak, assert_private_path

OFFER_WARNING_HORIZON = timedelta(hours=24)


def _aware(value: str) -> datetime:
    instant = datetime.fromisoformat(value)
    if instant.utcoffset() is None:
        raise ValueError("decision and expiry timestamps need a timezone")
    return instant


def _terms(case: dict[str, Any]) -> tuple[Any, ...]:
    """Snapshots can be compared when demand and the competing terms match."""
    offers = tuple(sorted(
        (offer["origin"], offer["grade"], offer["incoterm"], offer["carrier"])
        for offer in case["offers"]
    ))
    return (
        case["destination"], case["quantity_mt"], case["loading_start"], case["loading_end"], offers,
    )


def _winner_origin(document: dict[str, Any]) -> str | None:
    result = document["result"]
    if result["comparison_state"] != "complete" or result["winner"] is None:
        return None
    return next(
        (offer["origin"] for offer in document["case"]["offers"] if offer["id"] == result["winner"]),
        None,
    )


def saved_cargo_alerts(directory: Path, *, now: datetime) -> tuple[Alert, ...]:
    """Report expiring quotes and rank reversals from the latest saved decision per case."""
    cases: dict[str, list[tuple[datetime, dict[str, Any]]]] = defaultdict(list)
    alerts: list[Alert] = []
    for path in sorted(directory.glob("*.json")):
        try:
            if assert_private_path(path, where="cargo alert") != path.absolute():
                raise ValueError("decision path resolves outside its private location")
            document = json.loads(path.read_text(encoding="utf-8"))
            if document["schema_version"] != 1:
                raise ValueError("unsupported cargo decision schema")
            result = document["result"]
            stamp = _aware(result["decision_at"])
            if stamp <= now:
                _terms(document["case"])
                _winner_origin(document)
                for offer in result["offers"]:
                    _aware(offer["expires_at"])
                date.fromisoformat(document["case"]["loading_end"])
                cases[document["case_id"]].append((stamp, document))
        except (ClientDataLeak, OSError, KeyError, TypeError, ValueError) as exc:
            alerts.append(Alert(
                "cargo_evidence_unavailable", "warning", path.stem,
                f"Saved cargo decision could not be read: {type(exc).__name__}",
            ))

    for case_id, history in cases.items():
        history.sort(key=lambda revision: revision[0])
        stamp, latest = history[-1]
        if date.fromisoformat(latest["case"]["loading_end"]) < now.date():
            continue
        result = latest["result"]
        for offer in result["offers"]:
            expiry = _aware(offer["expires_at"])
            if expiry > now + OFFER_WARNING_HORIZON:
                continue
            expired = expiry <= now
            status = "expired" if expired else "expires within 24 hours"
            alerts.append(Alert(
                "cargo_offer_expiry", "alert" if expired else "warning", f"{case_id} / {offer['id']}",
                f"Offer {status} ({expiry.isoformat()})",
            ))

        if len(history) < 2:
            continue
        previous_stamp, previous = history[-2]
        before = _winner_origin(previous)
        after = _winner_origin(latest)
        if previous_stamp < stamp and before and after and before != after:
            if _terms(previous["case"]) != _terms(latest["case"]):
                continue
            alerts.append(Alert(
                "cargo_rank_reversal", "warning", case_id,
                f"Preferred origin moved from {before} to {after} between the two latest saved comparisons",
                {"previous_decision_at": previous_stamp.isoformat(), "decision_at": stamp.isoformat()},
            ))
    return tuple(alerts)
