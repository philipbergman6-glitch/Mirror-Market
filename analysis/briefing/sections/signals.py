"""SIGNALS section — sorted by severity (alert > warning > info).

Signals within ±3 business days of an estimated contract roll are demoted to
`info` and tagged `(near-roll)` — see analysis.signals.demote_near_roll_signals.
"""

from analysis.signals import demote_near_roll_signals


def format(signals: list[dict]) -> str:  # noqa: A001
    if not signals:
        return "SIGNALS:\n  No active signals"

    signals = demote_near_roll_signals(signals)

    lines = ["SIGNALS:"]
    severity_order = {"alert": 0, "warning": 1, "info": 2}
    signals.sort(key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    for s in signals:
        severity_tag = f"[{s.get('severity', 'info').upper()}]"
        lines.append(f"  {severity_tag:10s} {s['description']}")

    return "\n".join(lines)
