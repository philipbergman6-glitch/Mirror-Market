"""B48 (#626): every briefing price row carries its own session date.

``prices.observed_at`` is the newest date across tickers and ICE US bars now
land one weekday after CBOT's, so a cotton or sugar row printed under the
edition header would read as a session it is not. The row says which.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analysis import loaders
from analysis.briefing.sections import forward_curve as forward_curve_section
from analysis.briefing.sections import prices as prices_section


def _bars(last: str, n: int = 30) -> pd.DataFrame:
    idx = pd.bdate_range(end=last, periods=n, name="Date")
    close = np.linspace(60.0, 66.0, n)
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close, "Volume": 1.0}, index=idx
    )


def _row(text: str, commodity: str) -> str:
    return next(line for line in text.splitlines() if line.strip().startswith(f"{commodity}:"))


def test_each_price_row_names_its_own_session(patched_db):
    """An empty temp DB: no named-contract series, so the row is the frame given."""
    loaders.clear_loader_cache()
    text, _signals, _enriched = prices_section.format(
        {"Soybeans": _bars("2026-11-24"), "Cotton": _bars("2026-11-23")}
    )
    assert "session 2026-11-24" in _row(text, "Soybeans")
    assert "session 2026-11-23" in _row(text, "Cotton")


def _seed_curve(db, rows):
    """rows: (commodity, contract_month, label, ticker, close, observation_date)."""
    import sqlite3

    conn = sqlite3.connect(str(db))
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES (?, ?, ?, ?, ?, ?, '2026-11-25')",
        rows,
    )
    conn.commit()
    conn.close()


def test_each_forward_curve_line_names_its_observation_date(patched_db):
    """Two curves on different sessions: each line carries its own (#626),
    read through the one curve vocabulary since #671."""
    _seed_curve(patched_db, [
        ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.0, "2026-11-24"),
        ("Soybeans", "2027-03-01", "Mar 2027", "ZSH27.CBT", 1191.25, "2026-11-24"),
        ("Corn", "2027-03-01", "Mar 2027", "ZCH27.CBT", 445.0, "2026-11-23"),
        ("Corn", "2027-05-01", "May 2027", "ZCK27.CBT", 452.5, "2026-11-23"),
    ])
    text = forward_curve_section.format()
    assert "observed 2026-11-24" in _row(text, "Soybeans")
    assert "observed 2026-11-23" in _row(text, "Corn")


def test_a_curve_without_an_observation_date_is_never_dated_by_guess(patched_db):
    """An undated leg is dropped by the provider's coherence check, so an
    all-undated curve prints no line at all — never a fetch date standing in
    for the session (invariant 2)."""
    _seed_curve(patched_db, [
        ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.0, None),
        ("Soybeans", "2027-03-01", "Mar 2027", "ZSH27.CBT", 1191.25, None),
    ])
    text = forward_curve_section.format()
    assert "2026-11-25" not in text
    assert not any(line.strip().startswith("Soybeans:") for line in text.splitlines())
