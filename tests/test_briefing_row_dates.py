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


def test_each_forward_curve_line_names_its_observation_date(monkeypatch):
    curve = pd.DataFrame({
        "commodity": ["Soybeans", "Soybeans", "Cotton", "Cotton"],
        "contract_month": ["2027-01-01", "2027-03-01", "2026-12-01", "2027-03-01"],
        "label": ["Jan 2027", "Mar 2027", "Dec 2026", "Mar 2027"],
        "close": [1183.0, 1191.25, 66.5, 67.9],
        "observation_date": pd.to_datetime(["2026-11-24", "2026-11-24", "2026-11-23", "2026-11-23"]),
    })
    monkeypatch.setattr(forward_curve_section, "read_forward_curve", lambda: curve)
    text = forward_curve_section.format()
    assert "observed 2026-11-24" in _row(text, "Soybeans")
    assert "observed 2026-11-23" in _row(text, "Cotton")


def test_a_curve_without_an_observation_date_says_so(monkeypatch):
    curve = pd.DataFrame({
        "commodity": ["Sugar", "Sugar"],
        "contract_month": ["2027-03-01", "2027-05-01"],
        "label": ["Mar 2027", "May 2027"],
        "close": [18.1, 17.5],
        "observation_date": [None, None],
    })
    monkeypatch.setattr(forward_curve_section, "read_forward_curve", lambda: curve)
    assert "observation date not recorded" in _row(forward_curve_section.format(), "Sugar")
