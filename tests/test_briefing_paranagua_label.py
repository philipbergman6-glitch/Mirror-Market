"""B21 #483: the briefing never calls the AgRural Paranaguá bid FOB.

AgRural publishes a "sobre rodas" port bid — beans on a truck at the terminal —
not an FOB-vessel level. The basis headline and the cross-origin board both
print a name for that leg, and neither may claim a delivery term the source
does not publish.
"""

from __future__ import annotations

import pandas as pd

from analysis.briefing.sections import basis, fob_board


def _basis_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "Date": pd.to_datetime(["2026-10-16"]),
        "basis_usd_mt": [-12.0],
        "cepea_usd_mt": [440.0],
    })


def test_the_brazil_basis_headline_names_the_port_bid_not_fob(monkeypatch):
    def only_agrural(_soy, _fx, commodity):
        return _basis_frame() if "AgRural" in commodity else None

    monkeypatch.setattr(basis, "_basis_for_source", only_agrural)
    line = basis.format(
        {"Soybeans": pd.DataFrame({"Close": [1.0]})},
        {"BRL/USD": pd.DataFrame({"Close": [0.18]})},
    )
    assert line.startswith("BRAZIL BASIS (Paranaguá port bid vs CBOT)")
    assert "FOB" not in line


def test_the_cross_origin_board_labels_brazil_as_a_port_bid(monkeypatch):
    monkeypatch.setattr(fob_board, "_gulf_leg", lambda: (466.2, "2026-10-16"))
    monkeypatch.setattr(fob_board, "_brazil_leg", lambda _p, _c: (440.0, "2026-10-16"))
    monkeypatch.setattr(fob_board, "_argentina_leg", lambda: (452.0, "2026-10-16"))
    board = fob_board.format({}, {})
    brazil = next(line for line in board.splitlines() if "Brazil" in line)
    assert "port bid" in brazil and "FOB" not in brazil
    assert "only Argentina is FOB vessel" in board
