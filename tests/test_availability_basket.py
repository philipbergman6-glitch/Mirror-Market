"""B37 #534: every table a basket-grade block strikes on is written by a basket layer.

The basket (A19 #518 §1) was drawn from basket v1's strata by layer name, and
#512 then moved the Gulf basis's board leg from `prices` onto Layer 11b's
`contract_bars` without the basket following — so the basket could read 100 %
available while block 04 withheld for want of a close. These tests close that
hole from both ends: the descriptor-derived reads of every basket-grade block
stay inside the basket, and the Gulf basis builder, run against a real
database, reads no table its derivation omits.
"""

from __future__ import annotations

import sqlite3
from datetime import date

import pytest

import config
from app import block_builders
from app.block_builders import SiteContext
from app.blocks import absent_reason
from app.markets import block_reads, load_markets
from pipeline import schema
from tests.sqlite_reads import tables_read

D0 = date(2026, 10, 16)


@pytest.fixture(scope="module")
def registry():
    return load_markets()


# ---------------------------------------------------------------------------
# The declaration
# ---------------------------------------------------------------------------
def test_every_basket_grade_block_exists_and_has_a_source(registry):
    assert config.AVAILABILITY_BASKET_BLOCKS
    for (slug, block), stratum in config.AVAILABILITY_BASKET_BLOCKS.items():
        assert slug in registry, f"{slug!r} is not a registered market"
        assert absent_reason(registry[slug], block) is None, (
            f"{slug} block {block!r} is declared basket-grade but has no source"
        )
        assert stratum in range(1, 10), f"{slug} {block}: basket v1 has strata 1-9, not {stratum}"


@pytest.mark.parametrize("slug, block", sorted(config.AVAILABILITY_BASKET_BLOCKS))
def test_a_basket_grade_block_reads_only_tables_a_basket_layer_writes(registry, slug, block):
    reads = block_reads(registry[slug], block, registry)
    assert reads, f"{slug} block {block!r} derives no reads"
    outside = {table: layer for table, layer in reads.items() if layer not in config.AVAILABILITY_BASKET}
    assert not outside, (
        f"{slug} block {block!r} strikes on {outside} (table: writing layer), outside "
        "config.AVAILABILITY_BASKET — the basket can read available while this number is withheld"
    )


def test_the_named_contract_gulf_basis_reads_layer_11b_not_the_front_month(registry):
    # #512: block 04 strikes each AMS slot over the contract it names, through
    # `close_history` — `contract_bars`, never `prices` or `forward_curve`.
    assert block_reads(registry["cbot"], "basis", registry) == {
        "gulf_bids": "gulf_bids",
        "contract_bars": "contract_bars",
    }


def test_the_cbot_crush_block_carries_the_cash_legs_board_read(registry):
    # Stratum 9 rides in the crush block (#352), and its board leg is the same
    # `close_history` read as the Gulf basis.
    reads = block_reads(registry["cbot"], "crush", registry)
    assert reads["forward_curve"] == "forward_curve"
    assert reads["us_processor_cash"] == "us_processor_cash"
    assert reads["contract_bars"] == "contract_bars"


def test_a_front_month_basis_reads_prices_and_is_not_basket_grade(registry):
    # The derivation must see the dependency the declaration excludes —
    # otherwise the exclusion would be untested luck.
    reads = block_reads(registry["brazil"], "basis", registry)
    assert reads["prices"] == "prices"
    assert reads["currencies"] == "currencies"
    assert ("brazil", "basis") not in config.AVAILABILITY_BASKET_BLOCKS
    assert "prices" not in config.AVAILABILITY_BASKET


def test_a_block_with_no_source_is_refused(registry):
    with pytest.raises(ValueError, match="no basis source"):
        block_reads(registry["europe"], "basis", registry)


def test_an_unknown_block_id_is_refused(registry):
    with pytest.raises(KeyError):
        block_reads(registry["cbot"], "weather", registry)


# ---------------------------------------------------------------------------
# The builder agrees with the derivation
# ---------------------------------------------------------------------------
@pytest.fixture
def gulf_db():
    """One struck Gulf session — the #512 fixture's numbers, with the
    continuous front month and a forward curve present to prove neither is read."""
    conn = sqlite3.connect(":memory:")
    for ddl in (
        schema._CREATE_PRICES,
        schema._CREATE_CURRENCIES,
        schema._CREATE_GULF_BIDS,
        schema._CREATE_CONTRACT_BARS,
        schema._CREATE_FORWARD_CURVE,
    ):
        conn.execute(ddl)
    conn.execute("INSERT INTO prices (commodity, Date, Close) VALUES (?,?,?)",
                 ("Soybeans", D0.isoformat(), 1050.0))
    conn.execute(
        "INSERT INTO gulf_bids (report_date, commodity, location, delivery, average, "
        "futures_month, futures_month_high) VALUES (?,?,?,?,?,?,?)",
        (D0.isoformat(), "Soybeans", "Gulf Coast Ports", "Current", 13.5325, 11, 11),
    )
    conn.execute(
        "INSERT INTO contract_bars (commodity, ticker, contract_month, Date, Close, "
        "fetched_date) VALUES (?,?,?,?,?,?)",
        ("Soybeans", "ZSX26.CBT", "2026-11-01", D0.isoformat(), 1261.25, D0.isoformat()),
    )
    conn.commit()
    yield conn
    conn.close()


def test_the_gulf_basis_builder_reads_no_table_outside_its_derivation_or_the_basket(gulf_db, registry):
    (state, reason, data), read = tables_read(gulf_db, lambda: block_builders.basis_block(
        registry["cbot"], SiteContext(conn=gulf_db, today=D0), markets=registry,
    ))
    assert state == "ok", reason  # the strike ran end to end, so every read happened
    assert data["board_contract"] == "ZSX26"

    derived = block_reads(registry["cbot"], "basis", registry)
    assert read <= set(derived), f"block 04 read {sorted(read - set(derived))}, which block_reads omits"
    assert set(derived) <= read, f"block_reads names {sorted(set(derived) - read)}, which block 04 never read"
    assert {derived[table] for table in read} <= set(config.AVAILABILITY_BASKET)
