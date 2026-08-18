"""The workstation page: every section present, every empty state explained.

The page is where a wrong label does the damage, so these tests are mostly
about words: what is called a settlement, what is called a position, and what
says it was not routed.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime

import pytest
import yaml

from analysis.futures import options as options_mod
from app.workstation_page import (
    REFERENCE_LABEL,
    SECTION_SPECS,
    WORKSTATION_COMMODITIES,
    build_view,
)
from pipeline import schema

AS_OF = date(2026, 8, 18)
GENERATED = datetime(2026, 8, 18, 21, 30)

CURVE_ROWS = [
    ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75),
    ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.00),
    ("Soybeans", "2027-03-01", "Mar 2027", "ZSH27.CBT", 1191.25),
    ("Soybean Meal", "2026-12-01", "Dec 2026", "ZMZ26.CBT", 310.80),
    ("Soybean Meal", "2027-01-01", "Jan 2027", "ZMF27.CBT", 313.30),
    ("Soybean Oil", "2026-12-01", "Dec 2026", "ZLZ26.CBT", 68.18),
    ("Soybean Oil", "2027-01-01", "Jan 2027", "ZLF27.CBT", 68.10),
]


@pytest.fixture
def conn():
    connection = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        connection.execute(ddl)
    connection.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES (?,?,?,?,?,'2026-08-18',4210,NULL,'2026-08-18')",
        CURVE_ROWS,
    )
    # The provider's own front-month series, so the page can show it labelled.
    connection.executemany(
        "INSERT INTO prices (commodity, Date, Open, High, Low, Close, Volume) "
        "VALUES (?,?,NULL,NULL,NULL,?,NULL)",
        [("Soybeans", f"2026-08-{day:02d}", 1100.0 + day) for day in range(3, 19)],
    )
    # The one open-interest number this stack has: weekly, whole product,
    # dated to the CFTC report Tuesday rather than to the price session.
    connection.executemany(
        "INSERT INTO cot (commodity, Date, total_open_interest) VALUES (?,?,?)",
        [("Soybeans", "2026-08-11", 812_345.0), ("Soybean Meal", "2026-08-11", 601_200.0)],
    )
    connection.commit()
    yield connection
    connection.close()


@pytest.fixture
def empty_positions(tmp_path):
    return str(tmp_path / "positions")


def view(conn, positions_dir):
    return build_view(conn, today=AS_OF, generated_at=GENERATED, positions_dir=positions_dir)


def section(page, section_id):
    return next(s for s in page["sections"] if s["id"] == section_id)


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


def test_every_declared_section_is_built_in_order(conn, empty_positions):
    page = view(conn, empty_positions)
    assert [s["id"] for s in page["sections"]] == [spec[0] for spec in SECTION_SPECS]
    assert [s["no"] for s in page["sections"]] == [f"{i:02d}" for i in range(1, 11)]


def test_a_non_ok_section_always_names_its_reason(conn, empty_positions):
    for block in view(conn, empty_positions)["sections"]:
        if block["state"] != "ok":
            assert block["reason"].strip(), block["id"]


def test_the_page_stamps_its_session_and_its_method(conn, empty_positions):
    page = view(conn, empty_positions)
    assert page["as_of"] == "2026-08-18"
    assert page["method_version"] == "phase3.futures.v1"
    assert page["generated_at"] == GENERATED.isoformat()


def test_the_working_set_is_the_soy_complex(conn, empty_positions):
    rows = section(view(conn, empty_positions), "contracts")["data"]["commodities"]
    assert [row["commodity"] for row in rows] == list(WORKSTATION_COMMODITIES)


# ---------------------------------------------------------------------------
# Nothing here is a settlement, and nothing here is a position
# ---------------------------------------------------------------------------


def test_every_price_on_the_page_is_labelled_a_delayed_close(conn, empty_positions):
    page = view(conn, empty_positions)
    for row in section(page, "contracts")["data"]["commodities"]:
        for leg in row["legs"]:
            assert leg["price_label"] == "delayed close"
            assert leg["settlement_proven"] is False
            assert leg["price_type"] == "delayed_close"


def test_the_provider_section_denies_settlement_authority_and_routing(conn, empty_positions):
    data = section(view(conn, empty_positions), "provider")["data"]
    assert data["provider"]["settlement_authoritative"] is False
    assert data["provider"]["delayed"] is True
    assert "no connection to any venue, broker or execution system" in data["no_routing_note"]


def test_open_interest_is_reported_unavailable_rather_than_zero(conn, empty_positions):
    page = view(conn, empty_positions)
    curve = section(page, "curve")["data"]
    assert all(row["open_interest_available"] is False for row in curve["commodities"])
    assert "no honest way to derive it" in curve["open_interest_note"]
    for row in section(page, "contracts")["data"]["commodities"]:
        for leg in row["legs"]:
            assert leg["open_interest"] is None


def test_whole_product_open_interest_comes_from_cot_and_says_it_is_not_per_month(
    conn, empty_positions
):
    """The one open-interest number this stack actually has, and its caveats.

    It is weekly, it covers every listed month at once, and it is dated to the
    CFTC report Tuesday (11 Aug) rather than to the price session (18 Aug).
    Each of those three is a way the number could be misread, so each is
    carried on the row rather than left to the reader.
    """
    curve = section(view(conn, empty_positions), "curve")["data"]
    beans = next(row for row in curve["commodities"] if row["commodity"] == "Soybeans")
    aggregate = beans["aggregate_open_interest"]
    assert aggregate["contracts"] == 812_345.0
    assert aggregate["report_date"] == "2026-08-11"      # not the 18th
    assert aggregate["scope"] == "all contract months combined"
    assert "CFTC" in aggregate["source"]
    # And it has not leaked onto a contract row, where it would claim to be
    # one month's open interest.
    assert beans["open_interest_available"] is False
    assert "a whole-product figure attributed to one month would be wrong" in (
        curve["open_interest_note"]
    )


def test_a_commodity_with_no_cot_row_reports_no_open_interest_rather_than_zero(
    conn, empty_positions
):
    """Soybean Oil has no COT row in this fixture. Absence stays absence."""
    curve = section(view(conn, empty_positions), "curve")["data"]
    oil = next(row for row in curve["commodities"] if row["commodity"] == "Soybean Oil")
    assert oil["aggregate_open_interest"] is None


def test_every_products_termination_rule_is_named_on_the_page(conn, empty_positions):
    """The page states each product's rule, and states which products have none.

    The ``not_encoded`` list is empty today because all nine are encoded — the
    list stays because it is what the page would say if a tenth arrived without
    a rule, and an absent list would leave that silence unlabelled.
    """
    data = section(view(conn, empty_positions), "provider")["data"]
    assert data["not_encoded"] == []
    rules = {row["commodity"]: row for row in data["expiry_rules"]}
    assert len(rules) == 9
    assert all(row["confidence"] == "documented" for row in rules.values())
    assert rules["Soybeans"]["first_notice_rule"] == "last_business_day_of_prior_month"
    assert rules["Sugar"]["rule"] == "ice_sugar_last_trading_day"
    assert rules["Cotton"]["rule"] == "ice_cotton_last_trading_day"
    # Sugar's FND absence is a fact about the contract, not a gap in this
    # project, and the page distinguishes the two.
    assert rules["Sugar"]["first_notice_rule"] == "no_notice_day_mechanism"
    assert "no notice day" in rules["Sugar"]["first_notice_note"]
    assert rules["Soybeans"]["first_notice_note"] == ""


def test_the_continuous_series_is_never_offered_as_hedgeable(conn, empty_positions):
    """The provider series is shown, labelled with the roll it does not disclose;
    a stitched series is withheld entirely until the stored history is long enough."""
    data = section(view(conn, empty_positions), "provider")["data"]["continuous"]
    beans = data["Soybeans"]["provider_series"]
    assert beans["available"] is True
    assert beans["hedgeable"] is False
    assert beans["roll_method"] == "provider_front_month"
    assert "does not publish its roll dates" in beans["adjustment_note"]
    # And our own stitched alternative is absent rather than padded.
    assert data["Soybeans"]["available"] is False
    assert "does not pad it" in data["Soybeans"]["reason"]


# ---------------------------------------------------------------------------
# The hedge, with and without a book
# ---------------------------------------------------------------------------


def test_with_no_book_the_hedge_is_a_labelled_reference_calculation(conn, empty_positions):
    page = view(conn, empty_positions)
    assert page["is_reference_calculation"] is True
    hedge = section(page, "hedge")
    assert hedge["state"] == "ok"
    assert hedge["data"]["is_reference"] is True
    assert "not a position and no position has been entered" in hedge["data"]["reference_label"]
    assert page["reference_label"] == REFERENCE_LABEL


def test_the_reference_hedge_sizes_a_thousand_tonnes_and_says_which_month(conn, empty_positions):
    proposal = section(view(conn, empty_positions), "hedge")["data"]["proposals"][0]
    assert proposal["exposure"]["quantity_mt"] == pytest.approx(1_000.0)
    # 1,000 / 136.0777 = 7.35 -> 7 contracts.
    assert proposal["legs"][0]["contracts"] == 7
    assert proposal["legs"][0]["symbol"].startswith("ZS")


def test_the_crush_hedge_is_offered_beside_it_with_its_own_note(conn, empty_positions):
    data = section(view(conn, empty_positions), "hedge")["data"]
    assert data["crush"] is not None
    symbols = [leg["symbol"][:2] for leg in data["crush"]["legs"]]
    assert set(symbols) == {"ZS", "ZM", "ZL"}
    assert "hedge one position once" in data["crush_note"]


def test_the_book_section_says_where_a_position_would_have_to_come_from(conn, empty_positions):
    block = section(view(conn, empty_positions), "book")
    assert block["state"] == "empty"
    assert "ingests no account or clearing feed" in block["reason"]


def test_an_entered_book_replaces_the_reference_calculation(conn, tmp_path):
    (tmp_path / "book.yml").write_text(yaml.safe_dump({
        "physical": [{
            "commodity": "Soybeans", "quantity": 20_000, "unit": "mt", "side": "long",
            "average_cost_usd_mt": 415.0, "mark_contract": "ZSX26",
            "current_basis_usd_mt": -12.5, "location": "NOLA",
        }],
        "limits": [{"key": "net_mt", "scope": "Soybeans", "maximum": 10_000}],
    }), encoding="utf-8")
    page = view(conn, str(tmp_path))
    assert page["is_reference_calculation"] is False
    hedge = section(page, "hedge")["data"]
    assert hedge["is_reference"] is False
    assert hedge["proposals"][0]["exposure"]["quantity_mt"] == pytest.approx(20_000)

    book = section(page, "book")
    assert book["state"] == "ok"
    assert book["data"]["valuation"]["positions"][0]["key"] == "Soybeans @ NOLA"
    assert "not proven exchange settlements" in book["data"]["valuation"]["mark_note"]
    # The limit is crossed twice over, and the page says so rather than blocking.
    assert book["data"]["valuation"]["breaches"]


def test_a_breached_limit_reaches_the_alerts_section(conn, tmp_path):
    (tmp_path / "book.yml").write_text(yaml.safe_dump({
        "physical": [{
            "commodity": "Soybeans", "quantity": 20_000, "unit": "mt", "side": "long",
            "average_cost_usd_mt": 415.0, "mark_contract": "ZSX26",
            "current_basis_usd_mt": -12.5,
        }],
        "limits": [{"key": "net_mt", "scope": "Soybeans", "maximum": 10_000}],
    }), encoding="utf-8")
    alerts = section(view(conn, str(tmp_path)), "alerts")
    assert alerts["state"] == "ok"
    assert any(a["kind"] == "limit_breach" for a in alerts["data"]["alerts"])


def test_a_malformed_position_file_fails_the_page_rather_than_rendering_an_empty_book(conn, tmp_path):
    """'Nothing entered' and 'entered wrongly' are different states, and only
    one of them is safe to render as an empty book."""
    from analysis.futures.positions import PositionError

    (tmp_path / "book.yml").write_text("physical:\n  - commodity: Rapeseed\n    quantity: 1\n",
                                       encoding="utf-8")
    with pytest.raises(PositionError):
        view(conn, str(tmp_path))


# ---------------------------------------------------------------------------
# Scenarios, ticket, calendar, options
# ---------------------------------------------------------------------------


def test_scenarios_run_against_the_sized_hedge(conn, empty_positions):
    panel = section(view(conn, empty_positions), "scenarios")["data"]["panels"][0]
    assert panel["commodity"] == "Soybeans"
    assert panel["quantity_mt"] == pytest.approx(1_000.0)
    assert len(panel["results"]) >= 3
    names = {result["scenario"]["name"] for result in panel["results"]}
    assert any("basis" in name.lower() for name in names)


def test_a_usd_exposure_gets_no_fx_scenarios(conn, empty_positions):
    panel = section(view(conn, empty_positions), "scenarios")["data"]["panels"][0]
    for result in panel["results"]:
        for shock in result["scenario"]["shocks"]:
            assert shock["kind"] != "fx"


def test_the_ticket_section_carries_the_banner_and_both_formats(conn, empty_positions):
    data = section(view(conn, empty_positions), "ticket")["data"]
    assert data["banner"] == "PROPOSAL — NOT ROUTED"
    assert "has not been sent to any venue" in data["disclaimer"]
    ticket = data["tickets"][0]
    assert data["banner"] in ticket["text"]
    assert "PROPOSAL" in ticket["json"]
    assert ticket["id"]


def test_the_calendar_lists_only_ingested_sources_and_says_the_dates_are_rules(conn, empty_positions):
    data = section(view(conn, empty_positions), "calendar")["data"]
    assert data["events"]
    assert all(event["confidence"] == "rule" for event in data["events"])
    assert "Only releases this project actually ingests are listed" in data["scope_note"]


def test_the_options_section_reports_no_chain_and_offers_the_manual_route(conn, empty_positions):
    data = section(view(conn, empty_positions), "options")["data"]
    assert data["available"] is False
    assert "No source ingested by this project publishes an option chain" in data["reason"]
    assert "Enter the premium or the implied volatility" in data["manual_workflow"]
    assert data["model"] == "black76"
    assert data["model_assumptions"]


# ---------------------------------------------------------------------------
# Degraded states
# ---------------------------------------------------------------------------


def test_an_empty_database_renders_every_section_as_a_reasoned_empty_state(empty_positions):
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    page = build_view(conn, today=AS_OF, generated_at=GENERATED, positions_dir=empty_positions)
    states = {s["id"]: s for s in page["sections"]}
    assert states["contracts"]["state"] == "empty"
    assert "Layer 11 fetch has not run" in states["contracts"]["reason"]
    assert states["hedge"]["state"] == "empty"
    assert states["ticket"]["state"] == "empty"
    # The calendar and the options statement need no market data at all.
    assert states["calendar"]["state"] == "ok"
    assert states["options"]["state"] == "ok"
    conn.close()


def test_a_curve_stitched_from_two_sessions_is_flagged_rather_than_used(empty_positions):
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, fetched_date) VALUES (?,?,?,?,?,?, '2026-08-18')",
        [
            ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75, "2026-08-18"),
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.00, "2026-08-17"),
        ],
    )
    conn.commit()
    page = build_view(conn, today=AS_OF, generated_at=GENERATED, positions_dir=empty_positions)
    row = next(
        r for r in section(page, "contracts")["data"]["commodities"] if r["commodity"] == "Soybeans"
    )
    assert row["coherent"] is False
    assert "2026-08-17" in row["coherence_note"]
    alerts = section(page, "alerts")["data"]["alerts"]
    assert any("not a single session" in a["message"] for a in alerts)
    conn.close()


# ---------------------------------------------------------------------------
# Options: the chain is still unavailable, and the hand-entered ladder works
# ---------------------------------------------------------------------------


def test_the_options_section_is_empty_but_says_why_when_nothing_is_entered(
    conn, empty_positions, tmp_path
):
    data = section(
        build_view(conn, today=AS_OF, generated_at=GENERATED,
                   positions_dir=empty_positions, options_dir=str(tmp_path / "none")),
        "options",
    )["data"]
    assert data["available"] is False                 # the chain, still
    assert data["entered"] == []                      # the ladder, empty
    assert "No options entered" in data["empty_note"]
    # And the two absences are stated separately: ours and the market's.
    assert "no source ingested by this project" in data["reason"].lower()


def test_a_hand_entered_option_is_valued_against_the_board_and_labelled_a_model_value(
    conn, empty_positions, tmp_path
):
    """The end-to-end fill: a broker quote in a file, priced off our own curve.

    ZSX26 prints 1167.75 in the fixture curve, so the 1200 call is out of the
    money and its delta must be below 0.5. Everything on the row is stamped
    manual and carries the source string from the file.
    """
    ladder = tmp_path / "options"
    ladder.mkdir()
    (ladder / "book.yml").write_text(
        "options:\n"
        "  - underlying: ZSX26\n    right: call\n    strike: 1200\n"
        "    expiry: 2026-10-23\n    quoted_on: 2026-08-18\n"
        "    source: Broker XYZ 15:40 CT\n    implied_volatility: 0.185\n",
        encoding="utf-8",
    )
    data = section(
        build_view(conn, today=AS_OF, generated_at=GENERATED,
                   positions_dir=empty_positions, options_dir=str(ladder)),
        "options",
    )["data"]
    row = data["entered"][0]
    assert row["valued"] is True
    assert row["forward"] == 1167.75
    assert 0.0 < row["greeks"]["delta"] < 0.5
    assert row["price_type"] == "manual"
    assert "Broker XYZ" in row["volatility_source"]
    assert data["empty_note"] == ""
    assert "4.0%" in data["rate_note"]


def test_a_malformed_options_document_fails_the_page_rather_than_rendering_empty(
    conn, empty_positions, tmp_path
):
    """Same rule as the position book: entered wrongly is not entered nothing."""
    ladder = tmp_path / "options"
    ladder.mkdir()
    (ladder / "bad.yml").write_text("options:\n  - right: call\n", encoding="utf-8")
    with pytest.raises(options_mod.OptionEntryError):
        build_view(conn, today=AS_OF, generated_at=GENERATED,
                   positions_dir=empty_positions, options_dir=str(ladder))
