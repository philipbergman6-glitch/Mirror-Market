"""B21 #483: the briefing never calls the AgRural Paranaguá bid FOB.

AgRural publishes a "sobre rodas" port bid — beans on a truck at the terminal —
not an FOB-vessel level. The basis headline and the cross-origin board both
print a name for that leg, and neither may claim a delivery term the source
does not publish.
"""

from __future__ import annotations

import pandas as pd

import config
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


def _three_legs(monkeypatch) -> str:
    monkeypatch.setattr(fob_board, "_gulf_leg", lambda: (466.2, "2026-10-16"))
    monkeypatch.setattr(fob_board, "_brazil_leg", lambda _p, _c: (440.0, "2026-10-16"))
    monkeypatch.setattr(fob_board, "_argentina_leg", lambda: (452.0, "2026-10-16"))
    return fob_board.format({}, {})


def test_the_cross_origin_board_labels_brazil_as_a_port_bid(monkeypatch):
    board = _three_legs(monkeypatch)
    brazil = next(line for line in board.splitlines() if "Brazil" in line)
    assert "port bid" in brazil and "FOB" not in brazil


def test_each_board_row_names_its_own_delivery_term(monkeypatch):
    """B27 #501: three delivery terms, so each row carries its own inline."""
    rows = {
        line.split()[0]: line
        for line in _three_legs(monkeypatch).splitlines()
        if line.startswith("  ") and "$" in line
    }
    assert "CIF barge" in rows["US"]
    assert "DAP truck" in rows["Brazil"]
    assert "FOB vessel" in rows["Argentina"]


def test_board_labels_and_terms_come_from_the_origin_registry(monkeypatch):
    """One vocabulary: the origins page's label and incoterm+carrier, per leg."""
    board = _three_legs(monkeypatch)
    for leg_id in ("us_gulf", "br_paranagua", "ar_up_river"):
        leg = config.ORIGIN_LEGS[leg_id]
        row = next(line for line in board.splitlines() if leg["label"] + ":" in line)
        assert f"{leg['incoterm']} {leg['carrier']}" in row


def test_the_board_never_ranks_mixed_delivery_terms(monkeypatch):
    """B27 #501: a CIF-barge bid, an on-truck port bid and an FOB-vessel price
    are not one animal. The origins page refuses to rank them without a costed
    bridge to FOB vessel; the briefing withholds the same ranking with a reason
    instead of printing "Cheapest origin" over a footer disclaimer.
    """
    board = _three_legs(monkeypatch)
    assert "FOB BOARD" not in board
    assert "Cheapest" not in board and "cheapest" not in board
    assert "Ranking withheld" in board
    assert "no costed path" in board and "FOB vessel" in board


def test_an_insufficient_board_is_not_titled_fob(monkeypatch):
    monkeypatch.setattr(fob_board, "_gulf_leg", lambda: None)
    monkeypatch.setattr(fob_board, "_brazil_leg", lambda _p, _c: None)
    monkeypatch.setattr(fob_board, "_argentina_leg", lambda: (452.0, "2026-10-16"))
    line = fob_board.format({}, {})
    assert "Insufficient data" in line and "FOB BOARD" not in line
