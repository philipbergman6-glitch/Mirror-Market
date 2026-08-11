"""Layer 18 — SAFEX parser (fetchers/safex.py).

The fetcher had no tests at all before #157, which is how the two defects
here survived: the wrong contract was selected, and a carried-forward price
on a zero-volume row could be stored under the page's current date.

Fixtures are trimmed from the real Grain SA page captured 2026-08-11.
"""

from __future__ import annotations

import pytest

from fetchers.safex import _parse_safex_table
from pipeline.results import ScraperShapeError


def _page(rows: str) -> str:
    """Wrap body rows in the Grain SA page's actual table shape."""
    return f"""
    <html><body><table>
      <tr><td>Last Updated:  11 Aug 2026 12:45:02</td></tr>
      <tr>
        <th>Instrument</th><th>Contract</th><th>LastTradedTime</th>
        <th>LastTradedPrice</th><th>Difference</th><th>HighPrice</th>
        <th>LowPrice</th><th>Volume</th><th>OpenInterest</th>
      </tr>
      {rows}
    </table></body></html>
    """


def _row(inst, contract, when, price, diff, high, low, vol):
    return (
        f"<tr><td>{inst}</td><td>{contract}</td><td>{when}</td>"
        f"<td>{price}</td><td>{diff}</td><td>{high}</td><td>{low}</td>"
        f"<td>{vol}</td><td>0.00</td></tr>"
    )


# Verbatim from the live page on 2026-08-11. AUG26 is the nearest contract
# but DEC26 is the liquid one — the case that motivated the change.
_LIVE_SOYB = (
    _row("SOYB", "DEC27", "2026-08-11", "8049.00", "0.00", "0.00", "0.00", "0")
    + _row("SOYB", "MAR27", "2026-08-11", "7980.00", "-20.00", "7980.00", "7979.00", "24")
    + _row("SOYB", "DEC26", "2026-08-11", "8039.80", "-63.20", "8140.80", "8030.20", "433")
    + _row("SOYB", "OCT26", "2026-08-11", "8013.00", "0.00", "0.00", "0.00", "0")
    + _row("SOYB", "SEP26", "2026-08-11", "7910.00", "-73.00", "8019.80", "7900.40", "271")
    + _row("SOYB", "AUG26", "2026-08-11", "7847.00", "-83.00", "7960.00", "7847.00", "163")
)


def test_picks_most_liquid_contract_not_nearest_expiry():
    """DEC26 (433 lots) wins over the nearer AUG26 (163 lots)."""
    out = _parse_safex_table(_page(_LIVE_SOYB))

    row = out["Soybean (SAFEX)"].iloc[0]
    assert row["Contract"] == "DEC26"
    assert row["Close"] == pytest.approx(8039.80)
    assert row["Volume"] == pytest.approx(433)
    assert row["Unit"] == "ZAR/MT"


def test_zero_volume_contract_is_never_selected():
    """A carried-forward price stamped with today's date must not be stored.

    OCT26 shows the current date with Volume 0, Difference 0.00 and
    High/Low 0.00 — it did not trade. Its 8013.00 is a leftover from an
    earlier session and would be a fabricated print for this date.
    """
    only_untraded_is_dearest = (
        _row("SOYB", "OCT26", "2026-08-11", "9999.00", "0.00", "0.00", "0.00", "0")
        + _row("SOYB", "DEC26", "2026-08-11", "8039.80", "-63.20", "8140.80", "8030.20", "433")
    )
    out = _parse_safex_table(_page(only_untraded_is_dearest))

    assert out["Soybean (SAFEX)"].iloc[0]["Contract"] == "DEC26"


def test_all_contracts_untraded_yields_no_rows_not_a_fabricated_price():
    """Nothing traded → store nothing. Empty, not a stale carry-forward."""
    out = _parse_safex_table(_page(
        _row("SOYB", "OCT26", "2026-08-11", "8013.00", "0.00", "0.00", "0.00", "0")
        + _row("SOYB", "DEC26", "2026-08-11", "8039.80", "0.00", "0.00", "0.00", "0")
    ))

    assert "Soybean (SAFEX)" not in out


def test_volume_ties_break_toward_nearest_expiry():
    out = _parse_safex_table(_page(
        _row("SOYB", "DEC26", "2026-08-11", "8039.80", "-1.00", "8040.00", "8030.00", "100")
        + _row("SOYB", "SEP26", "2026-08-11", "7910.00", "-1.00", "7920.00", "7900.00", "100")
    ))

    assert out["Soybean (SAFEX)"].iloc[0]["Contract"] == "SEP26"


def test_stale_served_row_keeps_its_own_date():
    """On a non-trading day the page re-serves the prior session verbatim.

    The row must land on the date it actually belongs to, so it dedupes
    against what is stored rather than being restamped as a new print.
    """
    out = _parse_safex_table(_page(
        _row("SOYB", "AUG26", "2026-08-07", "7930.00", "-5.00", "7940.00", "7920.00", "151")
    ))

    assert str(out["Soybean (SAFEX)"].iloc[0]["Date"]) == "2026-08-07"


def test_both_tracked_instruments_are_parsed():
    out = _parse_safex_table(_page(
        _LIVE_SOYB
        + _row("SUNS", "DEC26", "2026-08-11", "10542.00", "162.00", "10550.00", "10423.00", "297")
        + _row("SUNS", "AUG26", "2026-08-11", "10295.20", "104.20", "10310.00", "10240.00", "167")
    ))

    assert set(out) == {"Soybean (SAFEX)", "Sunflower (SAFEX)"}
    assert out["Sunflower (SAFEX)"].iloc[0]["Contract"] == "DEC26"


# ── Shape failures must raise, never return empty ────────────────────────────

def test_missing_table_raises():
    """The 2026-08-10 outage shape: HTTP 200, no table at all."""
    with pytest.raises(ScraperShapeError, match="no <table>"):
        _parse_safex_table("<html><body><p>Nothing here</p></body></html>")


def test_renamed_column_raises_naming_the_column():
    page = _page(_LIVE_SOYB).replace("<th>Volume</th>", "<th>Traded Lots</th>")
    with pytest.raises(ScraperShapeError, match="volume"):
        _parse_safex_table(page)


def test_unparseable_contract_code_raises():
    with pytest.raises(ScraperShapeError, match="unparseable contract code"):
        _parse_safex_table(_page(
            _row("SOYB", "SPOT", "2026-08-11", "8039.80", "-1.00", "8040.00", "8030.00", "433")
        ))


def test_row_without_a_calendar_date_raises_rather_than_stamping_today():
    """A bare time would let pandas silently fill in today's date."""
    with pytest.raises(ScraperShapeError, match="no parseable trade date"):
        _parse_safex_table(_page(
            _row("SOYB", "DEC26", "12:45", "8039.80", "-1.00", "8040.00", "8030.00", "433")
        ))
