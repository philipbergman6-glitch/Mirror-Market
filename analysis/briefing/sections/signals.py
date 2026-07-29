"""SIGNALS section — sorted by severity (alert > warning > info).

Near-roll demotion happens upstream in the orchestrator (once, before the
signal list fans out to display, signals_json, and the snapshot), so this
section expects an already-demoted list.
"""


def format(signals: list[dict]) -> str:  # noqa: A001
    if not signals:
        return "SIGNALS:\n  No active signals"

    severity_order = {"alert": 0, "warning": 1, "info": 2}
    signals = sorted(signals, key=lambda s: severity_order.get(s.get("severity", "info"), 3))

    lines = ["SIGNALS:"]
    for s in signals:
        severity_tag = f"[{s.get('severity', 'info').upper()}]"
        lines.append(f"  {severity_tag:10s} {s['description']}")

    return "\n".join(lines)
