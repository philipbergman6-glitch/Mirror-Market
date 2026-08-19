"""Exposure views: what the book is actually short of, cut seven ways.

A net tonnage is not an exposure. 12,000 MT of beans bought basis and hedged
short 68 lots is *flat-price flat and basis long* — one number, two completely
different risks, and a desk that reads only the first will be surprised by the
second. Each view here answers one question a risk manager asks out loud.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import Side, named_contract
from analysis.futures.exposure import ExposureView, build_exposure
from analysis.futures.hedge import BasisConvention, PhysicalUnit
from analysis.futures.positions import (
    Book,
    Fill,
    FuturesPosition,
    PhysicalPosition,
    value_book,
)
from tests.book_fixtures import TODAY, fx_for, quote_for, synthetic_book

MT_PER_ZS = 136.0777  # 5,000 bu of 60-lb beans


def report(book: Book | None = None, *, as_of: date = TODAY):
    book = synthetic_book() if book is None else book
    valuation = value_book(book, as_of=as_of, quote_for=quote_for(as_of), fx_for=fx_for())
    return build_exposure(book, valuation, as_of=as_of)


def line(rep, view: ExposureView, key: str):
    matches = [entry for entry in rep.by_view(view) if entry.key == key]
    assert matches, f"no {view.value} line for {key!r}; have {[e.key for e in rep.by_view(view)]}"
    return matches[0]


# --- flat price -----------------------------------------------------------
def test_flat_price_exposure_nets_the_futures_hedge_against_the_physical() -> None:
    # 12,000 MT long, 68 lots short = 9,253.3 MT short. Net long ~2,747 MT.
    rep = report()
    beans = line(rep, ExposureView.FLAT_PRICE, "Soybeans")
    assert beans.quantity_mt == pytest.approx(12_000 - 68 * MT_PER_ZS, abs=1.0)
    # And the money: one dollar per tonne on the residual.
    assert beans.usd_per_unit_move == pytest.approx(beans.quantity_mt, abs=1.0)


def test_a_flat_priced_physical_carries_no_flat_price_exposure() -> None:
    """The convention is load-bearing, not decoration.

    A cargo whose price is already fixed against the board has no board
    exposure left; counting its tonnes would report a risk that was contracted
    away, and would invite a hedge that *creates* a position.
    """
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=5_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, basis_convention=BasisConvention.FLAT_PRICE,
        mark_contract="ZSX26", current_basis_usd_mt=0.0,
    ),))
    beans = line(report(book), ExposureView.FLAT_PRICE, "Soybeans")
    assert beans.quantity_mt == pytest.approx(0.0)
    assert "flat_price" in beans.note


def test_an_unstated_pricing_convention_is_warned_about_rather_than_assumed_away() -> None:
    """Absence never becomes an assumption — but it does become the loud reading.

    Without a stated convention the tonnes are counted as fully exposed (the
    most-exposed reading) *and* the line says the convention was not stated, so
    the number can be corrected rather than quietly trusted.
    """
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, mark_contract="ZSX26", current_basis_usd_mt=0.0,
        pricing_stated=False,
    ),))
    rep = report(book)
    beans = line(rep, ExposureView.FLAT_PRICE, "Soybeans")
    assert beans.quantity_mt == pytest.approx(1_000.0)
    assert any("not stated" in warning for warning in beans.warnings)


# --- basis ----------------------------------------------------------------
def test_hedging_converts_flat_price_risk_into_basis_risk() -> None:
    """The oldest sentence in merchandising, made a number.

    9,253 MT of the bean length is covered by futures, so those tonnes no
    longer move with the board — they move with the basis.
    """
    rep = report()
    beans = line(rep, ExposureView.BASIS, "Soybeans")
    assert beans.quantity_mt == pytest.approx(68 * MT_PER_ZS, abs=1.0)


def test_unpriced_tonnes_carry_basis_risk_even_with_no_futures_against_them() -> None:
    book = Book(physical=(PhysicalPosition(
        commodity="Soybean Oil", quantity=2_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=1_100.0, basis_convention=BasisConvention.UNPRICED,
        mark_contract="ZLZ26", current_basis_usd_mt=15.0,
    ),))
    assert line(report(book), ExposureView.BASIS, "Soybean Oil").quantity_mt == pytest.approx(2_000)


def test_basis_exposure_is_not_double_counted_when_a_position_is_both() -> None:
    # Unpriced *and* hedged: the tonnes carry basis risk once, not twice.
    book = Book(
        physical=(PhysicalPosition(
            commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
            average_cost_usd_mt=400.0, basis_convention=BasisConvention.UNPRICED,
            mark_contract="ZSX26", current_basis_usd_mt=-5.0,
        ),),
        futures=(FuturesPosition(
            contract=named_contract("Soybeans", 2026, 11),
            fills=(Fill(date(2026, 8, 4), Side.SHORT, 7, 1170.0),),
        ),),
    )
    assert line(report(book), ExposureView.BASIS, "Soybeans").quantity_mt == pytest.approx(
        1_000.0, abs=1.0
    )


# --- crush ----------------------------------------------------------------
def test_the_crush_view_reports_the_bean_equivalent_the_products_actually_cover() -> None:
    rep = report()
    crush = line(rep, ExposureView.CRUSH, "Soy complex")
    # Products are net short of what the bean length implies, so the covered
    # crush is bounded by the smaller product leg and the rest is flat beans.
    assert crush.quantity_mt is not None
    assert crush.quantity_mt >= 0
    assert "meal" in crush.note.lower() and "oil" in crush.note.lower()


def test_a_book_with_no_soy_complex_position_reports_no_crush_line() -> None:
    book = Book(futures=(FuturesPosition(
        contract=named_contract("Corn", 2026, 12),
        fills=(Fill(date(2026, 8, 4), Side.SHORT, 5, 430.0),),
    ),))
    assert not report(book).by_view(ExposureView.CRUSH)


# --- FX -------------------------------------------------------------------
def test_fx_exposure_is_reported_per_pair_in_usd_at_risk_per_one_percent() -> None:
    rep = report()
    brl = line(rep, ExposureView.FX, "BRL/USD")
    assert brl.usd_per_unit_move is not None and brl.usd_per_unit_move > 0
    assert "1%" in brl.unit_move_label


def test_a_non_usd_position_with_no_rate_is_reported_unquantified_not_zero() -> None:
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, currency="ZAR", fx_pair="ZAR/USD",
        basis_convention=BasisConvention.UNPRICED, mark_contract="ZSX26",
        current_basis_usd_mt=0.0,
    ),))
    valuation = value_book(book, as_of=TODAY, quote_for=quote_for(), fx_for=fx_for())
    rep = build_exposure(book, valuation, as_of=TODAY)
    zar = line(rep, ExposureView.FX, "ZAR/USD")
    assert zar.usd_per_unit_move is None
    assert any("no rate" in warning for warning in zar.warnings)


# --- contract month and first notice --------------------------------------
def test_the_contract_month_view_is_per_named_month_never_per_product() -> None:
    rep = report()
    zs = line(rep, ExposureView.CONTRACT_MONTH, "ZSX26")
    assert zs.contracts == pytest.approx(-68.0)
    assert zs.quantity_mt == pytest.approx(-68 * MT_PER_ZS, abs=1.0)


def test_first_notice_risk_fires_only_inside_the_window_and_names_the_date() -> None:
    # ZSX26's first notice is late October; from mid-August it is far away.
    assert not report().by_view(ExposureView.FIRST_NOTICE)
    late = report(as_of=date(2026, 10, 26))
    zs = line(late, ExposureView.FIRST_NOTICE, "ZSX26")
    assert zs.contracts == pytest.approx(-68.0)
    assert "notice" in zs.note.lower()


def test_a_contract_with_no_encoded_notice_rule_says_so_rather_than_reporting_safe() -> None:
    book = Book(futures=(FuturesPosition(
        contract=named_contract("Live Cattle", 2026, 10),
        fills=(Fill(date(2026, 8, 4), Side.LONG, 3, 180.0),),
    ),))
    rep = report(book, as_of=date(2026, 10, 20))
    lines = [entry for entry in rep.by_view(ExposureView.FIRST_NOTICE) if entry.key == "LEV26"]
    assert lines and any("not encoded" in warning for warning in lines[0].warnings)


# --- residual -------------------------------------------------------------
def test_residual_tonnes_are_the_physical_the_hedge_did_not_cover() -> None:
    residual = line(report(), ExposureView.RESIDUAL, "Soybeans")
    assert residual.quantity_mt == pytest.approx(12_000 - 68 * MT_PER_ZS, abs=1.0)


def test_an_over_hedge_shows_a_negative_residual_rather_than_zero() -> None:
    book = Book(
        physical=(PhysicalPosition(
            commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
            average_cost_usd_mt=400.0, basis_convention=BasisConvention.UNPRICED,
            mark_contract="ZSX26", current_basis_usd_mt=0.0,
        ),),
        futures=(FuturesPosition(
            contract=named_contract("Soybeans", 2026, 11),
            fills=(Fill(date(2026, 8, 4), Side.SHORT, 12, 1170.0),),
        ),),
    )
    assert line(report(book), ExposureView.RESIDUAL, "Soybeans").quantity_mt < 0


# --- the report itself ----------------------------------------------------
def test_every_view_the_brief_asks_for_is_present_in_the_enum() -> None:
    assert {view.value for view in ExposureView} == {
        "flat_price", "basis", "crush", "fx", "contract_month", "first_notice", "residual",
    }


def test_an_empty_book_produces_an_empty_report_rather_than_zeroes() -> None:
    rep = report(Book())
    assert rep.lines == ()
    assert rep.is_empty


def test_the_report_serialises_and_every_line_names_its_unit() -> None:
    payload = report().to_dict()
    assert payload["as_of"] == TODAY.isoformat()
    for entry in payload["lines"]:
        assert entry["unit_move_label"]
        assert entry["view"] in {view.value for view in ExposureView}


def test_metric_lookup_is_what_the_limits_module_reads() -> None:
    rep = report()
    assert rep.metric("flat_price_mt", "Soybeans") == pytest.approx(
        line(rep, ExposureView.FLAT_PRICE, "Soybeans").quantity_mt
    )
    assert rep.metric("flat_price_mt", "Corn") is None
    with pytest.raises(KeyError):
        rep.metric("not_a_metric", "*")
