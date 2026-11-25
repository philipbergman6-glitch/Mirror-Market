"""AL1 #640: the locator seam — the one product-side seam of the verifier.

The product marks each rendered basket number with stable ``data-verify-*``
attributes. They *locate* an element and say nothing about its value: the
verifier (`verify/`) reads value, unit and date from the element's visible
text, because a product-asserted attribute checked against itself would prove
nothing (spec #639, "Locator seam").

Two things are pinned here:

* every block in ``config.VERIFY_LOCATED_BLOCKS`` renders its locators — the
  declared set later strata tickets grow, always inside
  ``config.AVAILABILITY_BASKET_BLOCKS``;
* end to end, the verifier's own page reader finds, in the product's real
  rendered markup, the numbers the builder put there.
"""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

import config
from app.block_builders import build_blocks
from app.templating import site_environment
from tests.test_market_blocks import _OPEN_BAND, _day, registry, seeded  # noqa: F401 — fixtures
from verify import page as verify_page

# What a located block must carry inside its section, per block id.
_FIGURE_SELECTOR = {
    "currency": "[data-verify-figure='fx-rate'][data-verify-series]",
    "price": "[data-verify-leg]",
}


def _seed_every_basket_source(ctx) -> None:
    """The rows `seeded` leaves out that a located block needs to render ok."""
    for product in ("Soybeans", "Soybean Oil", "Soybean Meal"):
        ctx.conn.execute(
            "INSERT INTO argentina_fob (date, product, position, ship_from, ship_to, price_usd_mt) "
            "VALUES (?,?,?,?,?,?)",
            (_day(0), product, f"pos-{product}", _OPEN_BAND, _OPEN_BAND, 450.0),
        )
    for pair, rate in (("INR/USD", 0.012), ("ZAR/USD", 0.055)):
        ctx.conn.execute("INSERT INTO currencies (pair, Date, Close) VALUES (?,?,?)", (pair, _day(0), rate))
    ctx.conn.commit()


def _render(block) -> BeautifulSoup:
    html = site_environment().get_template("blocks/_block.html.j2").render(block=block)
    return BeautifulSoup(html, "html.parser")


def test_the_located_set_is_basket_grade_and_names_a_figure_rule():
    assert config.VERIFY_LOCATED_BLOCKS, "the FX blocks are located from AL1 on"
    outside = set(config.VERIFY_LOCATED_BLOCKS) - set(config.AVAILABILITY_BASKET_BLOCKS)
    assert not outside, f"{outside} are located but not basket-grade"
    for _slug, block_id in config.VERIFY_LOCATED_BLOCKS:
        assert block_id in _FIGURE_SELECTOR, f"no locator rule for block {block_id!r}"


def test_the_cny_block_is_located():
    # The tracer's own number: removing it from the set must be a decision.
    assert ("dalian", "currency") in config.VERIFY_LOCATED_BLOCKS


@pytest.mark.parametrize("slug, block_id", sorted(config.VERIFY_LOCATED_BLOCKS))
def test_a_located_block_renders_its_locators(seeded, registry, slug, block_id):  # noqa: F811
    _seed_every_basket_source(seeded)
    (block,) = build_blocks(registry[slug], None, seeded, markets=registry, block_ids=(block_id,))
    assert block.state == "ok", block.reason
    soup = _render(block)
    section = soup.select_one(f"section#block-{block_id}")
    assert section is not None
    assert section.get("data-verify-stratum") == str(config.AVAILABILITY_BASKET_BLOCKS[(slug, block_id)]), (
        f"{slug} block {block_id!r} renders without its stratum locator"
    )
    figures = section.select(_FIGURE_SELECTOR[block_id])
    assert figures, f"{slug} block {block_id!r} renders a basket number without its locator"
    for figure in figures:
        # A locator on an element with no visible number would locate nothing.
        assert any(ch.isdigit() for ch in figure.get_text()), figure


def test_the_located_set_is_the_switch(seeded, registry, monkeypatch):  # noqa: F811
    # Out of the set, a basket-grade block keeps its section stratum but marks
    # no number: the set, not the template, decides what the verifier reads.
    monkeypatch.setattr(config, "VERIFY_LOCATED_BLOCKS", config.VERIFY_LOCATED_BLOCKS - {("dalian", "currency")})
    (block,) = build_blocks(registry["dalian"], None, seeded, markets=registry, block_ids=("currency",))
    soup = _render(block)
    assert soup.select_one("section#block-currency").get("data-verify-stratum") == "6"
    assert not soup.select("[data-verify-figure]")


def test_a_block_outside_the_basket_carries_no_locator(seeded, registry):  # noqa: F811
    # Europe's EUR/USD is not a basket number: marking it would put an
    # unverified figure in the verifier's census.
    (block,) = build_blocks(registry["europe"], None, seeded, markets=registry, block_ids=("currency",))
    soup = _render(block)
    assert not soup.select("[data-verify-stratum], [data-verify-figure], [data-verify-leg]")


def test_the_verifier_reads_the_product_rendered_cny_block(seeded, registry):  # noqa: F811
    """The seam end to end: product markup in, the verifier's reading out."""
    blocks = build_blocks(registry["dalian"], None, seeded, markets=registry, block_ids=("price", "currency"))
    for block in blocks:
        assert block.state == "ok", (block.id, block.reason)
    body = "".join(
        site_environment().get_template("blocks/_block.html.j2").render(block=block) for block in blocks
    )
    html = (
        '<html><head><meta name="mirror-market-generated-at" content="2026-11-25T19:24:28+00:00">'
        f"</head><body>{body}</body></html>"
    )
    reading = verify_page.read_fx_block(html, series="CNY/USD")

    currency = next(b for b in blocks if b.id == "currency").data
    assert reading.site_edition == "2026-11-25T19:24:28+00:00"
    assert reading.stratum == "6"
    assert reading.observation_date == currency["as_of"]
    figures = {figure.unit: figure for figure in reading.figures}
    assert set(figures) == {"USD per CNY", "CNY per USD"}
    # The seeded 0.14 USD per CNY, as the page prints it (4 dp and 2 dp).
    assert figures["USD per CNY"].text == "0.1400"
    assert figures["CNY per USD"].text == "7.14"
    # Every leg the rate converts, read from its visible home print.
    legs = {leg.label.split(" · ")[0]: leg.home_value for leg in reading.legs}
    assert legs["DCE Soybean No.2"] == 3600.0
    assert legs["DCE Soybean Oil"] == 8000.0
    assert all(leg.home_unit == "CNY/MT" for leg in reading.legs)
