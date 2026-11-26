"""FORWARD CURVE section — each stored curve read by its named pairs.

One line per commodity: the structure, the nearby pair (front against the next
listed month, far − near, native units and USD/MT, the session) and where the
strip is in carry and where it is inverted, by month (#671). Read through
``analysis.futures.curve`` — the vocabulary the workstation uses — so the two
surfaces cannot give different Jan–Mar readings.
"""

from analysis.futures.stored_curves import read_stored_curves


def format() -> str:  # noqa: A001
    curves = read_stored_curves()
    if not curves:
        return "FORWARD CURVE: No data"

    lines = ["FORWARD CURVE:"]
    for commodity, analysis in curves.items():
        nearby = analysis.nearby
        if nearby is None:
            continue
        line = f"  {commodity}: {analysis.structure.value} — {nearby.sentence()}"
        if analysis.reading:
            line += f"; {analysis.reading}"
        line += f" — {analysis.structure.implication}"
        if not analysis.coherent:
            line += f" [off-session legs dropped: {analysis.coherence_note}]"
        lines.append(line)

    if len(lines) == 1:
        lines.append("  Insufficient contracts for curve analysis")

    return "\n".join(lines)
