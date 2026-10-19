"""#512: the Gulf basis is struck against the contract AMS quotes the slot over.

AMS 3147 prices each delivery slot as a basis over a *named* CBOT month
(``gulf_bids.futures_month``, ``futures_month_high`` when a range spans two,
#196). Block 04 used to subtract the provider's continuous front month
(``ZS=F``), which names no contract: the two agree only until AMS moves the
Current slot to the next month or the provider rolls on its own schedule.

Measured before building (``data/history`` CSVs, 2026-10-19): on every one of
the 31 Current-slot sessions whose named close Layer 11b holds, AMS's
``price − basis`` equals that same-session close to the tick. So the struck
figure *is* AMS's basis — V1 #474 row 7, 92 ¢/bu = 33.80 USD/MT on
2026-10-16 — and anything this module cannot strike on one session against one
named contract is withheld with a reason, never padded with the front month.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest
from jinja2 import Environment, FileSystemLoader

import config
from app import block_builders
from app.block_builders import SiteContext
from app.markets import _source, load_markets
from pipeline import schema
from pipeline.units import to_metric_tons

TEMPLATES = block_builders.__file__.rsplit("/", 1)[0] + "/templates"

# AMS 3147 "Grain Report for 10/16/2026 - Final" and 10/15, Gulf Coast Ports,
# CIF-B, Current slot over X; ZSX26.CBT's stored closes for the same sessions.
D0 = date(2026, 10, 16)
D1 = date(2026, 10, 15)
GULF_CURRENT = {D0: 13.5325, D1: 13.47}
ZSX26 = {D0: 1261.25, D1: 1263.0}
# The continuous front month, deliberately not ZSX26: a basis struck on it is
# the defect, and a number this far off cannot pass by coincidence.
FRONT_MONTH = 1050.0


def _usd(cents_per_bu: float) -> float:
    usd = to_metric_tons(cents_per_bu, "Soybeans")
    assert usd is not None
    return usd


def _gulf(conn, day: date, average: float, *, delivery="Current", low=11, high=11,
          location="Gulf Coast Ports") -> None:
    conn.execute(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month, futures_month_high) VALUES (?,?,?,?,?,?,?)",
        (day.isoformat(), "Soybeans", location, delivery, average, low, high),
    )


def _bar(conn, ticker: str, contract_month: str, day: date, close: float) -> None:
    conn.execute(
        "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Close, "
        "fetched_date) VALUES (?,?,?,?,?,?)",
        ("Soybeans", ticker, contract_month, day.isoformat(), close, day.isoformat()),
    )


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_CONTRACT_BARS,
    ):
        connection.execute(ddl)
    for day in (D0, D1):
        connection.execute(
            "INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
            ("Soybeans", day.isoformat(), FRONT_MONTH),
        )
        _gulf(connection, day, GULF_CURRENT[day])
        # A later slot over a later contract rides along, never the board leg.
        _gulf(connection, day, 13.8225, delivery="Jan", low=1, high=1)
        _bar(connection, "ZSX26.CBT", "2026-11-01", day, ZSX26[day])
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture
def registry():
    return load_markets()


def _basis(conn, registry, today: date = D0):
    return block_builders.basis_block(
        registry["cbot"], SiteContext(conn=conn, today=today), markets=registry
    )


# ---------------------------------------------------------------------------
# The number
# ---------------------------------------------------------------------------
def test_the_gulf_basis_is_struck_against_the_contract_ams_names(conn, registry):
    state, reason, data = _basis(conn, registry)
    assert state == "ok", reason
    assert data["board_contract"] == "ZSX26"
    assert data["board_usd_mt"] == pytest.approx(_usd(ZSX26[D0]))
    assert data["basis_usd_mt"] == pytest.approx(_usd(GULF_CURRENT[D0] * 100) - _usd(ZSX26[D0]))
    # V1 #474 row 7: AMS's own Current basis mid, (87 + 97) / 2 ¢/bu.
    assert data["basis_usd_mt"] == pytest.approx(33.80, abs=0.01)
    assert data["board_usd_mt"] != pytest.approx(_usd(FRONT_MONTH))
    assert data["as_of"] == D0.isoformat()
    assert data["window"] == "Current (prompt)"


def test_the_front_month_missing_the_session_does_not_withhold_the_named_strike(conn, registry):
    """The continuous series is no longer a leg of this basis at all."""
    conn.execute("DELETE FROM prices WHERE Date = ?", (D0.isoformat(),))
    conn.commit()
    state, reason, data = _basis(conn, registry)
    assert state == "ok", reason
    assert data["as_of"] == D0.isoformat()
    assert data["board_contract"] == "ZSX26"


def test_the_contract_year_rolls_into_next_year_for_an_earlier_month(conn, registry):
    """AMS prints a month code with no year. In December, F is next January."""
    december = date(2026, 12, 10)
    conn.execute("INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
                 ("Soybeans", december.isoformat(), FRONT_MONTH))
    _gulf(conn, december, 11.10, low=1, high=1)
    _bar(conn, "ZSF27.CBT", "2027-01-01", december, 1012.5)
    _bar(conn, "ZSF26.CBT", "2026-01-01", december, 999.0)  # never the right year
    conn.commit()
    state, reason, data = _basis(conn, registry, today=december)
    assert state == "ok", reason
    assert data["board_contract"] == "ZSF27"
    assert data["basis_usd_mt"] == pytest.approx(_usd(1110.0) - _usd(1012.5))


# ---------------------------------------------------------------------------
# Withheld, with the reason a reader is owed
# ---------------------------------------------------------------------------
def test_no_named_close_on_the_session_withholds_rather_than_borrowing_another(conn, registry):
    conn.execute("DELETE FROM contract_bars WHERE Date = ?", (D0.isoformat(),))
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "ZSX26" in reason
    assert D0.isoformat() in reason
    assert D1.isoformat() in reason  # the newest close it does hold, named


def test_a_contract_with_no_stored_bars_at_all_is_withheld(conn, registry):
    conn.execute("DELETE FROM contract_bars")
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "ZSX26" in reason and "no close is stored" in reason


def test_a_close_on_zero_volume_is_not_a_traded_session(conn, registry):
    """The CBOT board's trade proof (#157) holds for the named leg too."""
    conn.execute("UPDATE contract_bars SET Volume = 0 WHERE Date = ?", (D0.isoformat(),))
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "zero volume" in reason and "ZSX26" in reason


def test_a_null_volume_proves_nothing_and_still_strikes(conn, registry):
    conn.execute("UPDATE contract_bars SET Volume = NULL")
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "ok", reason


def test_a_range_over_two_contracts_has_no_single_board(conn, registry):
    """`95.00Q to 100.00X` (#196): each end is over its own month."""
    conn.execute("UPDATE gulf_bids SET futures_month = 8 WHERE report_date = ? "
                 "AND delivery = 'Current'", (D0.isoformat(),))
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "ZSQ27" in reason and "ZSX26" in reason


def test_two_places_quoting_the_window_over_different_contracts_is_withheld(conn, registry):
    _gulf(conn, D0, 13.60, location="Texas Gulf", low=1, high=1)
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "ZSF27" in reason and "ZSX26" in reason


def test_a_slot_with_no_futures_month_is_withheld(conn, registry):
    conn.execute("UPDATE gulf_bids SET futures_month = NULL, futures_month_high = NULL "
                 "WHERE report_date = ? AND delivery = 'Current'", (D0.isoformat(),))
    conn.commit()
    state, reason, _ = _basis(conn, registry)
    assert state == "empty"
    assert "names no CBOT contract" in reason


def test_a_basis_with_no_named_contract_still_strikes_on_the_front_month(conn, registry):
    """Only a source that declares its contract columns changes. India's mandi
    leg names no contract; it keeps the reference board, unlabelled by one."""
    conn.execute(schema._CREATE_INDIA_DOMESTIC)
    conn.execute("INSERT INTO india_domestic_prices (Date, commodity, Close, unit) "
                 "VALUES (?,?,?,?)", (D0.isoformat(), "Soybean (Mandi MP)", 67250.0, "INR/MT"))
    conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)",
                 ("INR/USD", D0.isoformat(), 0.0105))
    conn.commit()
    state, reason, data = block_builders.basis_block(
        registry["india"], SiteContext(conn=conn, today=D0), markets=registry
    )
    assert state == "ok", reason
    assert data["board_contract"] is None
    assert data["board_usd_mt"] == pytest.approx(_usd(FRONT_MONTH))


# ---------------------------------------------------------------------------
# On the page
# ---------------------------------------------------------------------------
def test_the_card_names_the_contract_beside_the_window(conn, registry):
    state, reason, data = _basis(conn, registry)
    assert state == "ok", reason
    env = Environment(loader=FileSystemLoader(TEMPLATES), autoescape=True)
    block = type("B", (), {"data": data})()
    html = env.get_template("blocks/04_basis.html.j2").render(root="../", block=block)
    assert "window Current (prompt) · over ZSX26" in html
    # The range below is still the front-month series (#513) and says so.
    assert "front month" in html


# ---------------------------------------------------------------------------
# The declaration
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    ("patch", "message"),
    [
        ({"contract_columns": ("futures_month",)}, "low and high"),
        ({"contract_board": None}, "both or neither"),
        ({"contract_board": "Not A Board"}, "Not A Board"),
        ({"window_scheme": None, "window_columns": ()}, "window_scheme"),
    ],
)
def test_half_a_contract_declaration_fails_at_load(patch, message):
    raw = {**config.MARKETS["cbot"]["basis"], **patch}
    raw = {k: v for k, v in raw.items() if v is not None}
    with pytest.raises(ValueError, match=message):
        _source(raw, slug="cbot", block="basis")


def test_a_contract_declaration_is_basis_only():
    raw = {**config.MARKETS["cbot"]["price"],
           "contract_board": "Soybeans", "contract_columns": ("a", "b")}
    with pytest.raises(ValueError, match="basis-only"):
        _source(raw, slug="cbot", block="price")
