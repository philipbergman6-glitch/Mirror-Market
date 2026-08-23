"""What the four published surfaces are allowed to *say* about a price.

The unit tests in ``test_price_semantics.py`` pin the vocabulary. These pin the
output, because the vocabulary is not what a reader sees — a template string is.
The origins page called a delayed yfinance bar "three exchange settlements" in
prose that no enum ever passed through, and the opportunities board printed the
tag ``executable`` beside it.

Two rules, applied to real rendered HTML from the real builders:

1. **No surface asserts "settlement" about a board leg.** A *denial* is fine and
   is in fact required — the workstation says "delayed daily closes, not proven
   exchange settlements" — so the check is for an unnegated claim, not for the
   word.
2. **No surface prints ``executable``.** Nothing this stack ingests is
   settlement-proven, so the word cannot legitimately reach a page today. When
   an authoritative feed is bought, this test is the thing that has to be
   changed on purpose.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from jinja2 import Environment, FileSystemLoader

from pipeline import schema
from pricing.policy import ClaimKind, scan, visible_text

#: `seeded` (a DB with one real row per market source) and `registry` come from
#: the block tests rather than being built again here — a second seeding of the
#: same eight markets is a second set of numbers to keep in step.
pytest_plugins = ["test_market_blocks"]

TEMPLATE_DIR = Path(__file__).resolve().parent.parent / "app" / "templates"

# The phrase catalogue, the negation rule and the tag-stripping all live in
# `pricing.policy` now. They were defined here when this file was the only
# reader; the promotion gate and `tests/test_semantic_contract.py` are the
# second and third, and three copies of a list of forbidden words is how one of
# them ends up out of date and quietly permissive.


def _text(html: str) -> str:
    """Rendered HTML with tags stripped — what a reader actually sees."""
    return visible_text(html)


def _claims(html: str, kind: ClaimKind, *, surface: str) -> tuple:
    return tuple(v for v in scan(html, surface=surface) if v.kind is kind)


def assert_no_unnegated_settlement_claim(html: str, *, surface: str) -> None:
    for kind in (ClaimKind.SETTLEMENT, ClaimKind.OFFICIAL_CLOSE):
        violations = _claims(html, kind, surface=surface)
        assert not violations, "\n".join(v.describe() for v in violations)


def assert_never_executable(html: str, *, surface: str) -> None:
    violations = _claims(html, ClaimKind.EXECUTABLE, surface=surface)
    assert not violations, (
        f"{surface} claims an executable price; nothing this stack ingests is "
        "settlement-proven, so no row can carry that confidence: "
        + "\n".join(v.describe() for v in violations)
    )


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True)


# ---------------------------------------------------------------------------
# Origins
# ---------------------------------------------------------------------------
@pytest.fixture
def origins_db(tmp_path):
    """One realistic session of all three origin legs plus a board crush."""
    conn = sqlite3.connect(str(tmp_path / "origins.db"))
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month) VALUES (?,?,?,?,?,?)",
        [
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Current", 12.5563, 8),
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Sep¹", 12.6875, 11),
            ("2026-08-11", "Soybeans", "Gulf Coast Ports", "Nov¹", 12.8275, 11),
        ],
    )
    conn.executemany(
        "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, "
        "price_usd_mt) VALUES (?,?,?,?,?,?)",
        [
            ("2026-08-11", "Soybeans", "12019000190C", "2026-08", "2026-08", 449.0),
            ("2026-08-11", "Soybeans", "12019000190C", "2026-09", "2026-10", 452.0),
            ("2026-08-11", "Soybean oil", "15071000900J", "2026-09", "2026-10", 1120.0),
            ("2026-08-11", "Soybean meal", "23040010100B", "2026-09", "2026-10", 385.0),
        ],
    )
    conn.executemany(
        "INSERT INTO brazil_spot_prices (Date, commodity, price_brl, unit) VALUES (?,?,?,?)",
        [("2026-08-11", "Soybean (AgRural Paranaguá FOB)", 2433.33, "BRL/MT")],
    )
    conn.executemany(
        "INSERT INTO currencies (pair, Date, Open, High, Low, Close) VALUES (?,?,?,?,?,?)",
        [("BRL/USD", "2026-08-11", 0.1958, 0.1959, 0.1957, 0.19584)],
    )
    conn.executemany(
        "INSERT INTO prices (commodity, Date, Open, High, Low, Close, Volume) VALUES (?,?,?,?,?,?,?)",
        [
            ("Soybeans", "2026-08-11", 1160.0, 1170.0, 1155.0, 1167.75, 90000.0),
            ("Soybean Oil", "2026-08-11", 67.0, 68.5, 66.9, 68.18, 60000.0),
            ("Soybean Meal", "2026-08-11", 308.0, 312.0, 307.0, 310.80, 50000.0),
        ],
    )
    conn.commit()
    yield conn
    conn.close()


def test_origins_page_never_claims_a_settlement_or_an_executable_price(origins_db):
    from app.origins_page import build_view

    view = build_view(origins_db, today=date(2026, 8, 18))
    html = _env().get_template("origins.html.j2").render(
        origins=view,
        root="",
        market_nav=[],
        current_page="origins",
        generated_at="2026-08-18 21:30",
    )
    assert_no_unnegated_settlement_claim(html, surface="origins")
    assert_never_executable(html, surface="origins")


def test_origins_board_crush_is_labelled_a_delayed_board_reference(origins_db):
    """The useful calculation is preserved; only the claim about it changes."""
    from analysis.origins.crush import LEVEL_MEANINGS, CrushLevel

    meaning = LEVEL_MEANINGS[CrushLevel.BOARD_REFERENCE].lower()
    assert "delayed" in meaning
    assert "settlement" not in meaning or "not" in meaning


# ---------------------------------------------------------------------------
# Opportunities
# ---------------------------------------------------------------------------
def test_opportunities_page_never_claims_a_settlement_or_an_executable_price(tmp_db):
    from opportunity_fixtures import TODAY, seed_currency, seed_freshness, seed_weekly_flow

    from analysis.opportunities.domain import AUDIENCE_PUBLIC
    from app.opportunities_page import build_view

    seed_weekly_flow(tmp_db, last_week=TODAY - timedelta(days=2))
    seed_currency(tmp_db, pair="BRL/USD", last_date=TODAY - timedelta(days=1))
    seed_freshness(tmp_db, "crush_inspections")
    seed_freshness(tmp_db, "currencies")

    view = build_view(
        tmp_db, today=TODAY, audience=AUDIENCE_PUBLIC, workflow_dir=None, archive=False,
    )
    html = _env().get_template("opportunities.html.j2").render(
        opportunities=view,
        root="",
        market_nav=[],
        current_page="opportunities",
        generated_at="2026-08-18 21:30",
    )
    assert_no_unnegated_settlement_claim(html, surface="opportunities")
    assert_never_executable(html, surface="opportunities")


# ---------------------------------------------------------------------------
# Market pages (blocks 01 price, 03 crush, 04 basis) + headline
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("slug", ["cbot", "brazil", "india", "south_africa", "argentina"])
def test_market_page_blocks_never_claim_a_settlement(slug, seeded, registry):
    from app.block_builders import build_blocks

    env = _env()
    for block in build_blocks(registry[slug], None, seeded, markets=registry):
        html = env.get_template("blocks/_block.html.j2").render(
            block=block.to_dict() if hasattr(block, "to_dict") else block,
            market=registry[slug],
            root="../",
        )
        assert_no_unnegated_settlement_claim(html, surface=f"{slug}:{block.id}")
        assert_never_executable(html, surface=f"{slug}:{block.id}")


def test_a_board_leg_is_chipped_as_a_delayed_close_not_a_settlement(seeded, registry):
    """The chip is the only thing on a market page that names the animal.

    ``board`` alone reads as "the exchange's number"; the reader has no way to
    know it is a consumer-endpoint daily bar. The label says so.
    """
    from app.markets import quote_kind_label

    assert quote_kind_label("board") == "board · delayed close"
    assert quote_kind_label("board_last_traded") == "board · last traded"

    env = _env()
    price = next(b for b in build_market_blocks("cbot", seeded, registry) if b.id == "price")
    html = env.get_template("blocks/_block.html.j2").render(
        block=price.to_dict() if hasattr(price, "to_dict") else price,
        market=registry["cbot"],
        root="../",
    )
    assert "delayed close" in _text(html).lower()


def build_market_blocks(slug, seeded, registry):
    from app.block_builders import build_blocks

    return build_blocks(registry[slug], None, seeded, markets=registry)


# ---------------------------------------------------------------------------
# Workstation — the surface that was already right, kept right
# ---------------------------------------------------------------------------
def test_workstation_still_denies_settlement_in_so_many_words(tmp_path):
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES (?,?,?,?,?,'2026-08-18',4210,NULL,'2026-08-18')",
        [
            ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1167.75),
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1183.00),
        ],
    )
    conn.commit()
    from app.workstation_page import build_view

    view = build_view(
        conn,
        today=date(2026, 8, 18),
        generated_at=datetime(2026, 8, 18, 21, 30),
        positions_dir=str(tmp_path / "positions"),
    )
    html = _env().get_template("workstation.html.j2").render(
        workstation=view,
        root="",
        market_nav=[],
        current_page="workstation",
        generated_at="2026-08-18 21:30",
    )
    assert_no_unnegated_settlement_claim(html, surface="workstation")
    assert_never_executable(html, surface="workstation")
    # The denial itself must survive: silence here would read as endorsement.
    assert "not proven exchange settlements" in _text(html).lower()


def test_price_type_is_a_shared_label_across_surfaces():
    """The same observation carries the same words wherever it is rendered."""
    from pricing.semantics import PriceType

    assert PriceType.DELAYED_CLOSE.label == "delayed close"
    assert "settlement" in PriceType.DELAYED_CLOSE.caveat
    assert PriceType.DELAYED_CLOSE.caveat.lower().startswith(("not", "settlement not", "the "))
