"""The public workstation must not contain the client's book. Rendered, not asserted.

``tests/test_book_privacy.py`` pins the guards. This file pins the *page*, and
it does it by rendering the real template through the real builder against a
synthetic desk whose every free-text field carries a grep marker — because the
leak this exists to catch was not a missing guard. It was a builder that put
``valuation.to_dict()`` and the absolute path of the positions file straight
into ``docs/workstation.html``, which is on the promotion contract and is
uploaded to GitHub Pages. Nothing was wrong with the guards; nothing called
them.

Two editions, and the difference between them is the whole point:

* the **public** edition renders the book, exposure, limits, clearing and
  entered-options sections as ``absent`` with a reason that says they are
  private — not as ``empty``, which would say the desk holds nothing;
* the **private** edition renders them in full, and is written outside
  ``docs/`` where the deploy cannot reach it.
"""

from __future__ import annotations

import re
import sqlite3

import pytest
from book_fixtures import ACCOUNT, BROKER, GENERATED_AT, MARK, TODAY
from jinja2 import Environment, FileSystemLoader

from analysis.futures.privacy import (
    AUDIENCE_PRIVATE,
    AUDIENCE_PUBLIC,
    PRIVATE_SECTION_IDS,
    ClientDataLeak,
    assert_no_client_records,
    assert_private_path,
)
from pipeline import schema

TEMPLATE_DIR = "app/templates"


@pytest.fixture
def curve_db():
    conn = sqlite3.connect(":memory:")
    for ddl in schema.ALL_SCHEMAS:
        conn.execute(ddl)
    conn.executemany(
        "INSERT INTO forward_curve (commodity, contract_month, label, ticker, close, "
        "observation_date, volume, open_interest, fetched_date) "
        "VALUES (?,?,?,?,?,'2026-08-19',4210,NULL,'2026-08-19')",
        [
            ("Soybeans", "2026-11-01", "Nov 2026", "ZSX26.CBT", 1150.00),
            ("Soybeans", "2027-01-01", "Jan 2027", "ZSF27.CBT", 1162.00),
            ("Soybean Meal", "2026-12-01", "Dec 2026", "ZMZ26.CBT", 305.00),
            ("Soybean Oil", "2026-12-01", "Dec 2026", "ZLZ26.CBT", 52.00),
        ],
    )
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def desk(tmp_path):
    """A positions directory holding the synthetic desk, and an option ladder."""
    positions = tmp_path / "positions"
    positions.mkdir()
    (positions / "desk.yml").write_text(
        f"""
physical:
  - commodity: Soybeans
    quantity: 12000
    unit: MT
    side: long
    average_cost_usd_mt: 402.50
    pricing: basis_over_futures
    mark_contract: ZSX26
    current_basis_usd_mt: -12.5
    location: Paranagua
    note: {MARK} Nov-Dec loading
futures:
  - contract: ZSX26
    account: {ACCOUNT}
    fills:
      - {{date: 2026-08-04, side: short, quantity: 60, price: 1172.25}}
      - {{date: 2026-08-07, side: short, quantity: 28, price: 1180.00}}
      - {{date: 2026-08-11, side: long, quantity: 20, price: 1165.50}}
limits:
  - {{key: flat_price_mt, scope: Soybeans, maximum: 6000, warn_at: 4000}}
  - {{key: residual_mt, scope: Soybeans, maximum: 1000}}
""",
        encoding="utf-8",
    )

    options = tmp_path / "options"
    options.mkdir()
    (options / "ladder.yml").write_text(
        f"""
options:
  - underlying: ZSX26
    right: call
    strike: 1200
    expiry: 2026-10-23
    quoted_on: 2026-08-19
    quoted_at: 2026-08-19T14:45:00+00:00
    premium: 24.25
    source: {BROKER}
""",
        encoding="utf-8",
    )

    clearing = tmp_path / "clearing"
    clearing.mkdir()
    (clearing / "2026-08-19.yml").write_text(
        f"""
account: {ACCOUNT}
broker: {BROKER}
statement_date: 2026-08-19
lines:
  - {{symbol: ZSX26, description: SOYBEAN NOV26, quantity: -68,
      settlement_price: 1150.0, realised_usd: 0.0, unrealised_usd: -78000.0}}
""",
        encoding="utf-8",
    )
    return {
        "positions_dir": str(positions),
        "options_dir": str(options),
        "clearing_dir": str(clearing),
    }


def view(conn, desk, *, audience):
    from app.workstation_page import build_view

    return build_view(
        conn, today=TODAY, generated_at=GENERATED_AT, audience=audience, **desk,
    )


def render(page_view) -> str:
    env = Environment(loader=FileSystemLoader(TEMPLATE_DIR), autoescape=True)
    return env.get_template("workstation.html.j2").render(
        workstation=page_view,
        root="",
        market_nav=[],
        current_page="workstation",
        generated_at="2026-08-19 21:30",
    )


def sections(page_view) -> dict[str, dict]:
    return {section["id"]: section for section in page_view["sections"]}


# ---------------------------------------------------------------------------
# The private edition sees everything
# ---------------------------------------------------------------------------
def test_the_private_edition_renders_the_book_the_desk_entered(curve_db, desk):
    private = sections(view(curve_db, desk, audience=AUDIENCE_PRIVATE))

    assert private["book"]["state"] == "ok"
    assert private["book"]["data"]["valuation"]["positions"]
    assert private["exposure"]["state"] == "ok"
    assert private["limits"]["state"] == "ok"
    assert private["clearing"]["state"] == "ok"
    assert private["options_entered"]["state"] == "ok"


def test_the_private_edition_html_shows_the_entered_tonnage(curve_db, desk):
    """The same string the public test greps for, on the edition that may have it."""
    html = render(view(curve_db, desk, audience=AUDIENCE_PRIVATE))
    assert "12,000" in html or "12000" in html


# ---------------------------------------------------------------------------
# The public edition sees none of it
# ---------------------------------------------------------------------------
def test_the_public_view_carries_no_client_record_field(curve_db, desk):
    assert_no_client_records(
        view(curve_db, desk, audience=AUDIENCE_PUBLIC), where="public workstation view",
    )


@pytest.mark.parametrize("section_id", PRIVATE_SECTION_IDS)
def test_every_private_section_is_absent_with_a_reason_not_empty(curve_db, desk, section_id):
    """`empty` would say the desk holds nothing. `absent` says it is not shown."""
    section = sections(view(curve_db, desk, audience=AUDIENCE_PUBLIC))[section_id]

    assert section["state"] == "absent"
    assert section["data"] is None
    assert "private" in section["reason"].lower()


def test_the_public_html_contains_no_marker_no_account_and_no_file_path(curve_db, desk):
    html = render(view(curve_db, desk, audience=AUDIENCE_PUBLIC))

    assert MARK not in html
    assert ACCOUNT not in html
    assert BROKER not in html
    assert "reference/positions" not in html
    assert "reference/options" not in html
    assert "reference/clearing" not in html
    assert desk["positions_dir"] not in html


def test_no_entered_number_survives_into_the_public_html(curve_db, desk):
    """The marks are public; the sizes and the costs are not."""
    html = render(view(curve_db, desk, audience=AUDIENCE_PUBLIC))
    text = re.sub(r"<[^>]+>", " ", html)

    for entered in ("12,000", "12000", "402.50", "1,172.25", "1172.25", "-68", "6,000"):
        assert entered not in text, entered


def test_a_limit_breach_never_reaches_the_public_alerts(curve_db, desk):
    """An alert naming a limit key and an observed tonnage *is* the book."""
    public = sections(view(curve_db, desk, audience=AUDIENCE_PUBLIC))
    alerts = public["alerts"]

    kinds = {a["kind"] for a in (alerts["data"] or {}).get("alerts", [])}
    assert "limit_breach" not in kinds

    private = sections(view(curve_db, desk, audience=AUDIENCE_PRIVATE))
    private_kinds = {a["kind"] for a in (private["alerts"]["data"] or {}).get("alerts", [])}
    assert "limit_breach" in private_kinds


def test_the_public_edition_is_the_default_so_forgetting_the_flag_is_safe(curve_db, desk):
    from app.workstation_page import build_view

    default = build_view(curve_db, today=TODAY, generated_at=GENERATED_AT, **desk)
    assert_no_client_records(default, where="default workstation view")


def test_the_public_options_section_keeps_the_model_and_loses_the_quotes(curve_db, desk):
    """The chain's absence and Black-76's limits are public facts about us."""
    public = sections(view(curve_db, desk, audience=AUDIENCE_PUBLIC))

    options = public["options"]
    assert options["state"] == "ok"
    assert options["data"]["limitations"]
    assert "entered" not in options["data"]
    assert "entered_from" not in options["data"]


def test_black76s_american_limitation_is_stated_on_the_public_page(curve_db, desk):
    html = render(view(curve_db, desk, audience=AUDIENCE_PUBLIC))
    text = re.sub(r"<[^>]+>", " ", html).lower()

    assert "american" in text
    assert "early" in text and "exercise" in text


# ---------------------------------------------------------------------------
# Where each edition may be written
# ---------------------------------------------------------------------------
def test_the_private_workstation_is_written_outside_docs():
    from analysis.futures.privacy import private_output_dir

    assert_private_path(private_output_dir() / "workstation.html", where="private workstation")


def test_writing_the_private_edition_into_docs_is_refused():
    with pytest.raises(ClientDataLeak):
        assert_private_path("docs/workstation.html", where="private workstation")


def test_the_private_edition_is_not_on_the_promotion_contract():
    from trust.site_promotion import expected_site_paths

    published = set(expected_site_paths())
    assert not any("workspace" in path for path in published)
    assert "workstation.html" in " ".join(published)      # the public one still is


def test_an_empty_desk_still_renders_both_editions(curve_db, tmp_path):
    """No positions entered is a legitimate state, and not the same as private."""
    empty = {
        "positions_dir": str(tmp_path / "none"),
        "options_dir": str(tmp_path / "none"),
        "clearing_dir": str(tmp_path / "none"),
    }
    private = sections(view(curve_db, empty, audience=AUDIENCE_PRIVATE))
    assert private["book"]["state"] == "empty"
    assert "no positions entered" in private["book"]["reason"].lower()

    public = sections(view(curve_db, empty, audience=AUDIENCE_PUBLIC))
    assert public["book"]["state"] == "absent"

    assert_no_client_records(view(curve_db, empty, audience=AUDIENCE_PUBLIC), where="empty")


def test_an_unknown_audience_is_refused_rather_than_defaulting_to_private(curve_db, desk):
    from app.workstation_page import build_view

    with pytest.raises(ValueError, match="audience"):
        build_view(curve_db, today=TODAY, generated_at=GENERATED_AT, audience="desk", **desk)
