"""The origin page as a workflow: readiness, renewals, and the flip table.

Rendered, not just built. The page is the only place a trader meets any of
this, and the two failures that matter are both rendering failures: a command
whose ``<VALUE>`` placeholder a browser eats as a tag (the site renders with
autoescape on since #313), and a blocked page that names no way forward.

Both the empty set and a complete one are exercised. The complete one comes
from ``tests/fixtures/assumptions_complete/`` — invented values, loaded through
``MIRROR_ASSUMPTIONS_DIR``, and never from the shipped directory, which is
correctly empty and would leave the success path unrendered forever.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timezone
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from analysis.origins.assumptions import AssumptionSet, load_assumptions
from app.origins_page import build_view
from pipeline import schema

TODAY = date(2026, 8, 18)
FIXTURE_SET = Path(__file__).parent / "fixtures" / "assumptions_complete"


@pytest.fixture
def db(tmp_path):
    """One realistic session of all three origin legs into North China."""
    conn = sqlite3.connect(str(tmp_path / "origins.db"))
    for ddl in (
        schema._CREATE_GULF_BIDS,
        schema._CREATE_ARGENTINA_FOB,
        schema._CREATE_BRAZIL_SPOT,
        schema._CREATE_CURRENCIES,
        schema._CREATE_ORIGIN_RANKINGS,
    ):
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month) VALUES (?,?,?,?,?,?)",
        [
            ("2026-08-17", "Soybeans", "Gulf Coast Ports", "Sep¹", 12.6875, 11),
            ("2026-08-17", "Soybeans", "Gulf Coast Ports", "Sep²", 12.6975, 11),
        ],
    )
    conn.executemany(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
        "price_usd_mt) VALUES (?,?,?,?,?,?)",
        [("2026-08-17", "Soybeans", "12019000190C", "2026-09", "2026-10", 452.0)],
    )
    conn.executemany(
        "INSERT INTO brazil_spot_prices (Date, commodity, price_brl, unit) VALUES (?,?,?,?)",
        [("2026-08-17", "Soybean (AgRural Paranaguá FOB)", 2433.33, "BRL/MT")],
    )
    conn.executemany(
        "INSERT INTO currencies (pair, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)",
        [("BRL/USD", "2026-08-17", 0.1958, 0.1959, 0.1957, 0.19584)],
    )
    conn.commit()
    yield conn
    conn.close()


def _render(view: dict) -> BeautifulSoup:
    """Through the site's own environment, exactly as deployed."""
    from scripts.generate_site import _env

    now = datetime(2026, 8, 18, 21, 0, tzinfo=timezone.utc)
    html = _env().get_template("origins.html.j2").render(
        origins=view,
        root="",
        market_nav=[],
        current_page="origins",
        current_market=None,
        day_line="TUESDAY 18 AUGUST 2026",
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_iso=now.isoformat(),
    )
    return BeautifulSoup(html, "html.parser")


def _section(soup: BeautifulSoup, section_id: str) -> str:
    node = soup.select_one(f"#section-{section_id}")
    assert node is not None, section_id
    return node.get_text("\n", strip=True)


# ---------------------------------------------------------------------------
# The blocked page still tells a trader what to do
# ---------------------------------------------------------------------------
def test_a_page_with_nothing_entered_names_every_input_and_the_command_for_it(db):
    view = build_view(db, today=TODAY, assumptions=AssumptionSet(assumptions=()))
    soup = _render(view)
    readiness = _section(soup, "readiness")

    assert "Route readiness" in readiness
    for component in ("Ocean freight", "Elevation", "Marine insurance",
                      "Destination port costs", "Financing / carry", "Quality adjustment"):
        assert component in readiness, component
    assert "python scripts/enter_assumption.py" in readiness


def test_the_placeholder_survives_rendering_rather_than_being_eaten_as_a_tag(db):
    """<VALUE> must survive rendering as text, never be eaten as a tag."""
    view = build_view(db, today=TODAY, assumptions=AssumptionSet(assumptions=()))
    readiness = _section(_render(view), "readiness")
    assert "--value <VALUE>" in readiness
    assert "--entered-by <you@example.com>" in readiness


def test_no_rendered_command_carries_a_suggested_number(db):
    view = build_view(db, today=TODAY, assumptions=AssumptionSet(assumptions=()))
    for window_view in view["views"]:
        for route in window_view["readiness"]["routes"]:
            for requirement in route["requirements"]:
                assert "--value <VALUE>" in requirement["command"]


def test_the_declared_leg_with_no_price_series_says_so_rather_than_listing_inputs(db):
    view = build_view(db, today=TODAY, assumptions=AssumptionSet(assumptions=()))
    readiness = _section(_render(view), "readiness")
    assert "no PNW price series is ingested" in readiness


# ---------------------------------------------------------------------------
# The complete page — the success path, from a fixture set
# ---------------------------------------------------------------------------
@pytest.fixture
def complete(monkeypatch):
    monkeypatch.setenv("MIRROR_ASSUMPTIONS_DIR", str(FIXTURE_SET))
    return load_assumptions(FIXTURE_SET)


def test_every_supported_route_reads_ready_once_its_inputs_are_entered(db, complete):
    view = build_view(db, today=TODAY, assumptions=complete)
    default = next(v for v in view["views"] if v["is_default"])
    routes = {r["leg_id"]: r for r in default["readiness"]["routes"]}
    for leg_id in ("us_gulf", "br_paranagua", "ar_up_river"):
        assert routes[leg_id]["is_ready"], leg_id
    assert routes["us_pnw"]["is_ready"] is False  # declared, no price series
    assert default["readiness"]["ready_count"] == 3
    assert default["readiness"]["route_count"] == 3


def test_the_flip_table_names_which_input_would_change_the_answer(db, complete):
    view = build_view(db, today=TODAY, assumptions=complete)
    default = next(v for v in view["views"] if v["is_default"])
    sensitivity = default["sensitivity"]
    assert sensitivity["flip_moves"], "a ranked board must produce a flip table"
    fragile = sensitivity["most_fragile"]
    assert fragile is not None
    assert fragile["move"] is not None
    assert fragile["leader"] != fragile["challenger"]

    rendered = _section(_render(view), "sensitivity")
    assert "Which input flips it" in rendered


def test_a_shared_input_is_rendered_as_unable_to_flip_rather_than_as_a_big_number(db, complete):
    view = build_view(db, today=TODAY, assumptions=complete)
    default = next(v for v in view["views"] if v["is_default"])
    shared = [m for m in default["sensitivity"]["flip_moves"] if m["shared"]]
    assert shared, "duty, VAT and discharge are entered once for the destination"
    assert all(m["move"] is None and m["reason"] for m in shared)


def test_the_renewals_section_reports_a_clear_queue_when_nothing_lapses(db, complete):
    view = build_view(db, today=TODAY, assumptions=complete)
    renewals = next(s for s in view["sections"] if s["id"] == "renewals")
    assert renewals["data"]["nothing_due"] is True
    assert "Nothing has expired" in _section(_render(view), "renewals")


def test_the_renewals_section_names_the_owner_and_what_goes_dark(db, complete):
    """A lapse is only actionable if it says who renews it and what it costs."""
    from dataclasses import replace

    aging = AssumptionSet(
        assumptions=tuple(
            replace(item, expires_on=date(2026, 8, 25))
            if item.id == "fixture.ocean_freight.br_paranagua" else item
            for item in complete.assumptions
        ),
        loaded_from=complete.loaded_from,
    )
    view = build_view(db, today=TODAY, assumptions=aging)
    renewals = next(s for s in view["sections"] if s["id"] == "renewals")
    lapsing = {row["id"]: row for row in renewals["data"]["expiring"]}
    assert lapsing["fixture.ocean_freight.br_paranagua"]["days_to_expiry"] == 7
    assert lapsing["fixture.ocean_freight.br_paranagua"]["routes_blocked"] == ["br_paranagua"]

    rendered = _section(_render(view), "renewals")
    assert "fixture.ocean_freight.br_paranagua" in rendered
    assert "br_paranagua" in rendered


def test_a_lapsed_input_blocks_the_route_and_the_page_says_which_one(db, complete):
    from dataclasses import replace

    lapsed = AssumptionSet(
        assumptions=tuple(
            replace(item, expires_on=date(2026, 8, 10))
            if item.id == "fixture.ocean_freight.br_paranagua" else item
            for item in complete.assumptions
        ),
        loaded_from=complete.loaded_from,
    )
    view = build_view(db, today=TODAY, assumptions=lapsed)
    default = next(v for v in view["views"] if v["is_default"])
    brazil = next(
        r for r in default["readiness"]["routes"] if r["leg_id"] == "br_paranagua"
    )
    assert brazil["is_ready"] is False
    assert brazil["blocking"] == ["ocean_freight"]
    freight = next(
        r for r in brazil["requirements"] if r["component"] == "ocean_freight"
    )
    assert freight["status"] == "expired"
    assert freight["entered_by"] == "fixture"

    # and the landed total is withheld rather than computed without it
    row = next(
        r for r in default["decision"]["rows"] if r["origin_key"] == "br_paranagua"
    )
    assert row["landed_usd_mt"] is None


def test_the_fixture_set_is_never_what_the_site_loads():
    """config points at the shipped directory; the override is a test-only door."""
    import config

    assert Path(config.ASSUMPTIONS_DIR).resolve() != FIXTURE_SET.resolve()
