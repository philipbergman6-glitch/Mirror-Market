"""Position workspace: lot accounting, marks, attribution, limits, loading.

Every P&L number asserted here is worked out in the docstring first. A book
that marks itself wrongly is worse than no book at all, because it looks like
one that marks itself rightly.
"""

from __future__ import annotations

from datetime import date

import pytest

from analysis.futures.domain import Side, named_contract, parse_symbol, spec_for
from analysis.futures.hedge import PhysicalUnit
from analysis.futures.limits import LimitError
from analysis.futures.positions import (
    Book,
    BookKind,
    Fill,
    FuturesPosition,
    Limit,
    LimitStatus,
    PhysicalPosition,
    PositionError,
    load_book,
    parse_book,
    positions_from_csv,
    run_weighted_average,
    value_book,
)
from tests.test_futures_hedge import AS_OF, quote


def fill(day: int, side: Side, quantity: float, price: float) -> Fill:
    return Fill(trade_date=date(2026, 8, day), side=side, quantity=quantity, price=price)


# ---------------------------------------------------------------------------
# Weighted-average lot accounting
# ---------------------------------------------------------------------------


def test_two_buys_average_by_quantity_not_by_count():
    """10 @ 1100 and 30 @ 1200 -> 40 @ 1175, not @ 1150."""
    lot = run_weighted_average([
        fill(3, Side.LONG, 10, 1100.0),
        fill(4, Side.LONG, 30, 1200.0),
    ])
    assert lot.net_quantity == 40
    assert lot.average_cost == pytest.approx(1175.0)
    assert lot.realised == 0.0
    assert lot.derived is True


def test_a_closing_fill_realises_against_the_running_average():
    """Long 40 @ 1175, sell 15 @ 1210 -> realised 15 x 35 = +525 price-units."""
    lot = run_weighted_average([
        fill(3, Side.LONG, 10, 1100.0),
        fill(4, Side.LONG, 30, 1200.0),
        fill(5, Side.SHORT, 15, 1210.0),
    ])
    assert lot.net_quantity == 25
    assert lot.average_cost == pytest.approx(1175.0)   # unchanged by a close
    assert lot.realised == pytest.approx(15 * (1210.0 - 1175.0))


def test_closing_a_short_realises_the_other_way_round():
    lot = run_weighted_average([
        fill(3, Side.SHORT, 20, 1200.0),
        fill(4, Side.LONG, 20, 1150.0),
    ])
    assert lot.net_quantity == 0
    assert lot.average_cost is None          # flat carries no cost basis
    assert lot.realised == pytest.approx(20 * 50.0)


def test_a_fill_that_crosses_through_flat_leaves_no_phantom_basis():
    """Long 10 @ 1100, sell 25 @ 1200: close 10 (+1000), open short 15 @ 1200."""
    lot = run_weighted_average([
        fill(3, Side.LONG, 10, 1100.0),
        fill(4, Side.SHORT, 25, 1200.0),
    ])
    assert lot.net_quantity == -15
    assert lot.average_cost == pytest.approx(1200.0)
    assert lot.realised == pytest.approx(10 * 100.0)


def test_fills_are_ordered_by_trade_date_not_by_file_order():
    scrambled = run_weighted_average([
        fill(5, Side.SHORT, 15, 1210.0),
        fill(3, Side.LONG, 10, 1100.0),
        fill(4, Side.LONG, 30, 1200.0),
    ])
    assert scrambled.realised == pytest.approx(15 * 35.0)


# ---------------------------------------------------------------------------
# Marking
# ---------------------------------------------------------------------------


ZSX26 = named_contract("Soybeans", 2026, 11)


def quotes(**by_symbol):
    def quote_for(commodity, symbol):
        return by_symbol.get(symbol)
    return quote_for


def test_a_futures_position_marks_through_the_contract_size():
    """Short 73 ZSX26 @ 1150.00, mark 1167.75.

    Per contract: (1167.75 - 1150.00) c/bu x 5,000 bu / 100 = $887.50
    Short 73:     -73 x 887.50 = -$64,787.50
    """
    book = Book(futures=(FuturesPosition(
        contract=ZSX26, fills=(fill(10, Side.SHORT, 73, 1150.0),),
    ),))
    valuation = value_book(
        book, as_of=AS_OF,
        quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    )
    position = valuation.positions[0]
    assert position.kind is BookKind.FUTURES
    assert position.net_quantity == -73
    assert position.net_mt == pytest.approx(-73 * spec_for("Soybeans").mt_per_contract)
    assert position.unrealised_usd == pytest.approx(-64_787.50)
    assert valuation.total_unrealised_usd == pytest.approx(-64_787.50)
    assert position.attribution["basis"] is None
    assert "no basis or FX component" in position.attribution_note


def test_realised_pnl_converts_through_the_same_contract_size():
    """Sell 15 @ 1210 out of a 1175 average = 525 c/bu x 5,000 / 100 = $26,250."""
    book = Book(futures=(FuturesPosition(contract=ZSX26, fills=(
        fill(3, Side.LONG, 10, 1100.0),
        fill(4, Side.LONG, 30, 1200.0),
        fill(5, Side.SHORT, 15, 1210.0),
    )),))
    valuation = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    )
    assert valuation.total_realised_usd == pytest.approx(26_250.0)


def test_a_stated_position_is_marked_but_says_the_software_did_not_derive_it():
    book = Book(futures=(FuturesPosition(
        contract=ZSX26, stated_contracts=-50, stated_average_price=1150.0,
        stated_realised_usd=1_234.0,
    ),))
    position = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    ).positions[0]
    assert position.unrealised_usd == pytest.approx(-50 * 887.50)
    assert position.realised_usd == pytest.approx(1_234.0)      # taken at its word
    assert any("not something this software derived" in w for w in position.warnings)


def test_a_position_with_neither_fills_nor_a_stated_quantity_raises():
    with pytest.raises(PositionError, match="nothing to mark"):
        _ = FuturesPosition(contract=ZSX26).lot


def test_an_unquotable_contract_is_unmarked_and_says_so_rather_than_marking_at_zero():
    book = Book(futures=(FuturesPosition(
        contract=ZSX26, fills=(fill(10, Side.SHORT, 73, 1150.0),),
    ),))
    position = value_book(book, as_of=AS_OF, quote_for=quotes()).positions[0]
    assert position.unrealised_usd is None
    assert position.mark is None
    assert position.mark_label == "unavailable"
    assert any("cannot mark this position" in w for w in position.warnings)


def test_a_physical_is_marked_at_the_board_plus_its_basis():
    """10,000 MT long at 415.00 USD/MT cost; ZSX26 1167.75 = 429.05 USD/MT, basis -12.50.

    Mark = 429.0530 - 12.50 = 416.5530 -> (416.5530 - 415.00) x 10,000 = +15,530
    """
    board = spec_for("Soybeans").native_to_usd_per_mt(1167.75)
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=10_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=415.0, mark_contract="ZSX26", current_basis_usd_mt=-12.5,
        location="NOLA",
    ),))
    position = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    ).positions[0]
    assert position.key == "Soybeans @ NOLA"
    assert position.mark == pytest.approx(board - 12.5)
    assert position.unrealised_usd == pytest.approx((board - 12.5 - 415.0) * 10_000)
    assert position.mark_label.endswith("+ basis")


def test_a_physical_with_no_basis_recorded_is_marked_flat_and_warned():
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=415.0, mark_contract="ZSX26",
    ),))
    position = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    ).positions[0]
    assert any("no current basis recorded" in w for w in position.warnings)
    assert position.mark == pytest.approx(spec_for("Soybeans").native_to_usd_per_mt(1167.75))


# ---------------------------------------------------------------------------
# Attribution — the part that is withheld more often than it is given
# ---------------------------------------------------------------------------


def test_attribution_splits_the_move_into_board_and_basis():
    """Entry board 420.00, entry basis -20.00, now 429.0530 / -12.50, 10,000 MT long.

    Futures component = (429.0530 - 420.00) x 10,000 = +90,530
    Basis component   = (-12.50 - -20.00) x 10,000   = +75,000
    """
    board = spec_for("Soybeans").native_to_usd_per_mt(1167.75)
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=10_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, mark_contract="ZSX26",
        entry_futures_usd_mt=420.0, entry_basis_usd_mt=-20.0, current_basis_usd_mt=-12.5,
    ),))
    position = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    ).positions[0]
    assert position.attribution["futures"] == pytest.approx((board - 420.0) * 10_000)
    assert position.attribution["basis"] == pytest.approx(75_000.0)
    assert position.attribution["fx"] == 0.0
    assert "decomposed against the recorded entry" in position.attribution_note


def test_attribution_is_withheld_when_an_entry_level_is_missing():
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=10_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, mark_contract="ZSX26", current_basis_usd_mt=-12.5,
        entry_futures_usd_mt=420.0,      # no entry basis
    ),))
    position = value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    ).positions[0]
    assert position.attribution == {"futures": None, "basis": None, "fx": None}
    assert "one or more is missing" in position.attribution_note


def test_a_non_usd_position_gets_an_fx_component_from_the_recorded_entry_rate():
    board = spec_for("Soybeans").native_to_usd_per_mt(1167.75)
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=10_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, currency="BRL", fx_pair="BRL/USD",
        mark_contract="ZSX26", entry_futures_usd_mt=420.0, entry_basis_usd_mt=-20.0,
        entry_fx_rate=0.2000, current_basis_usd_mt=-12.5,
    ),))
    position = value_book(
        book, as_of=AS_OF,
        quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
        fx_for=lambda pair: (date(2026, 8, 11), 0.1958),
    ).positions[0]
    home = 10_000 * (board - 12.5) / 0.2000
    assert position.attribution["fx"] == pytest.approx(home * (0.1958 - 0.2000))
    assert position.attribution["fx"] < 0
    assert "0.200000 ->" in position.attribution_note


def test_a_non_usd_position_with_no_current_rate_says_the_fx_leg_is_unquantified():
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=10_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, currency="BRL", fx_pair="BRL/USD",
        mark_contract="ZSX26", entry_futures_usd_mt=420.0, entry_basis_usd_mt=-20.0,
        entry_fx_rate=0.2000, current_basis_usd_mt=-12.5,
    ),))
    position = value_book(
        book, as_of=AS_OF,
        quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
        fx_for=lambda pair: None,
    ).positions[0]
    assert position.attribution["futures"] is not None
    assert position.attribution["fx"] is None
    assert "unquantified" in position.attribution_note


def test_a_physical_with_no_mark_contract_named_warns_at_book_level():
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=1_000, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=400.0, current_basis_usd_mt=-12.5,
    ),))
    valuation = value_book(book, as_of=AS_OF, quote_for=quotes())
    assert any("name one to make the mark reproducible" in w for w in valuation.warnings)


def test_the_mark_note_never_claims_to_be_a_settlement():
    valuation = value_book(Book(), as_of=AS_OF, quote_for=quotes())
    assert "not proven exchange settlements" in valuation.mark_note
    assert "not a margin calculation" in valuation.mark_note
    assert valuation.to_dict()["positions"] == []


# ---------------------------------------------------------------------------
# Limits — reported, never enforced
# ---------------------------------------------------------------------------


def marked_long(net_mt: float, mark: float = 400.0):
    book = Book(physical=(PhysicalPosition(
        commodity="Soybeans", quantity=net_mt, unit=PhysicalUnit.METRIC_TON, side=Side.LONG,
        average_cost_usd_mt=mark, mark_contract="ZSX26", current_basis_usd_mt=0.0,
    ),))
    return value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    )


def limited(net_mt: float, *limits, mark: float = 400.0):
    """Value a one-position book under a set of limits.

    Limits are now measured against the exposure views rather than against a
    dict of net tonnages handed in by the caller, so they are exercised through
    ``value_book`` — the same path the page uses. A limit checked by a private
    arithmetic the page does not run is a limit nobody is actually keeping.
    """
    book = Book(
        physical=(PhysicalPosition(
            commodity="Soybeans", quantity=abs(net_mt), unit=PhysicalUnit.METRIC_TON,
            side=Side.LONG if net_mt >= 0 else Side.SHORT,
            average_cost_usd_mt=mark, mark_contract="ZSX26", current_basis_usd_mt=0.0,
        ),),
        limits=limits,
    )
    return value_book(
        book, as_of=AS_OF, quote_for=quotes(ZSX26=quote("Soybeans", 2026, 11, 1167.75)),
    )


def test_a_net_mt_limit_breaches_on_absolute_size_either_way():
    long_side = limited(30_000, Limit("net_mt", "Soybeans", 25_000))
    assert len(long_side.breaches) == 1
    assert long_side.breaches[0].to_dict()["excess"] == pytest.approx(5_000.0)
    assert len(limited(-30_000, Limit("net_mt", "Soybeans", 25_000)).breaches) == 1


def test_a_limit_scoped_to_another_commodity_does_not_fire():
    assert limited(30_000, Limit("net_mt", "Corn", 1.0)).breaches == ()


def test_a_loss_limit_fires_on_negative_unrealised_only():
    # Bought at 500 and marked at ~429: a loss well past the line.
    assert limited(30_000, Limit("loss_usd", "*", 100_000), mark=500.0).breaches
    # Bought at 300: the same size, in profit. A profit cannot cross a loss limit.
    assert limited(30_000, Limit("loss_usd", "*", 100_000), mark=300.0).breaches == ()


def test_a_notional_limit_measures_the_physical_marked_value():
    breaches = limited(10_000, Limit("notional_usd", "*", 1_000_000)).breaches
    assert breaches and breaches[0].observed > 4_000_000


def test_an_unknown_limit_key_is_refused_at_construction_not_logged_and_skipped():
    """A change of behaviour, deliberately.

    The old code logged a warning and checked nothing, which leaves a desk
    believing a mandate is being kept while nothing keeps it. A limit key this
    software cannot measure is invalid input, and invalid input fails loudly.
    """
    with pytest.raises(LimitError, match="unknown limit key"):
        Limit("var_99", "*", 1.0)


def test_a_warning_level_fires_before_the_breach_and_is_reported_separately():
    valuation = limited(30_000, Limit("net_mt", "Soybeans", 40_000, warn_at=25_000))
    assert valuation.breaches == ()
    statuses = {check.status for check in valuation.limit_checks}
    assert statuses == {LimitStatus.WARN}


def test_a_warning_level_at_or_above_the_maximum_is_refused():
    with pytest.raises(LimitError, match="not below the maximum"):
        Limit("net_mt", "*", 1_000, warn_at=1_000)


def test_headroom_is_reported_for_a_limit_that_is_nowhere_near_crossed():
    check = limited(1_000, Limit("net_mt", "Soybeans", 25_000)).limit_checks[0]
    assert check.status is LimitStatus.OK
    assert check.headroom == pytest.approx(24_000.0)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


DOCUMENT = {
    "physical": [{
        "commodity": "Soybeans", "quantity": 5_000, "unit": "mt", "side": "long",
        "average_cost_usd_mt": 415.0, "mark_contract": "ZSX26",
        "current_basis_usd_mt": -12.5, "location": "Paranagua",
    }],
    "futures": [{
        "contract": "ZSX26",
        "fills": [{"date": "2026-08-10", "side": "sell", "quantity": 36, "price": 1150.0}],
    }],
    "limits": [{"key": "net_mt", "scope": "Soybeans", "maximum": 20_000}],
}


def test_a_document_round_trips_into_typed_positions():
    book = parse_book(DOCUMENT, where="doc")
    assert book.physical[0].quantity_mt == pytest.approx(5_000)
    assert book.futures[0].contract.symbol == "ZSX26"
    assert book.futures[0].net_contracts == -36
    assert book.limits[0].key == "net_mt"
    assert book.is_empty is False


def test_a_short_physical_is_signed_negative_in_tonnes():
    payload = {"physical": [dict(DOCUMENT["physical"][0], side="short")]}
    assert parse_book(payload, where="doc").physical[0].quantity_mt == pytest.approx(-5_000)


@pytest.mark.parametrize(("mutation", "message"), [
    ({"physical": [dict(DOCUMENT["physical"][0], side="maybe")]}, "neither long nor short"),
    ({"physical": [dict(DOCUMENT["physical"][0], unit="tonnes")]}, "is not one of"),
    ({"futures": [{"contract": "XXZ26", "fills": []}]}, "not a contract symbol"),
    ({"futures": [{"contract": "ZSX26"}]}, "give either"),
    ({"limits": [{"key": "gamma", "maximum": 1}]}, "unknown limit key"),
])
def test_a_malformed_document_raises_rather_than_loading_partially(mutation, message):
    with pytest.raises(PositionError, match=message):
        parse_book(mutation, where="doc")


def test_an_unknown_commodity_in_a_document_raises_a_position_error():
    """Typed as a PositionError, not an UnknownContract: callers catch the former
    to mean 'the file said something that cannot be true', and a differently
    typed escape gets swallowed into an empty book one level up."""
    payload = {"physical": [dict(DOCUMENT["physical"][0], commodity="Rapeseed")]}
    with pytest.raises(PositionError, match="no contract specification for 'Rapeseed'"):
        parse_book(payload, where="doc")


def test_an_unknown_commodity_in_a_csv_raises_the_same_way(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text(
        "kind,commodity,side,quantity,unit,price\nphysical,Rapeseed,long,10,mt,400\n",
        encoding="utf-8",
    )
    with pytest.raises(PositionError, match="Rapeseed"):
        positions_from_csv(path)


def test_a_missing_positions_directory_is_an_empty_book_not_an_error(tmp_path):
    book = load_book(tmp_path / "nope")
    assert book.is_empty
    assert book.loaded_from == ()


def test_a_present_but_malformed_file_raises(tmp_path):
    (tmp_path / "book.yml").write_text("physical:\n  - commodity: Soybeans\n", encoding="utf-8")
    with pytest.raises((PositionError, KeyError)):
        load_book(tmp_path)


def test_yaml_files_load_and_name_their_source(tmp_path):
    import yaml

    (tmp_path / "book.yml").write_text(yaml.safe_dump(DOCUMENT), encoding="utf-8")
    book = load_book(tmp_path)
    assert book.futures[0].contract.symbol == "ZSX26"
    assert book.loaded_from == (str(tmp_path / "book.yml"),)


def test_the_shipped_positions_directory_is_deliberately_empty():
    """A fresh clone has entered no positions; the workspace must say so."""
    import config

    assert load_book(config.POSITIONS_DIR).is_empty


def test_a_csv_export_accumulates_several_rows_into_one_position(tmp_path):
    path = tmp_path / "book.csv"
    path.write_text(
        "kind,commodity,contract,side,quantity,unit,price,trade_date,basis_usd_mt,location\n"
        "futures,,ZSX26,sell,40,,1150.0,2026-08-10,,\n"
        "futures,,ZSX26,sell,33,,1160.0,2026-08-11,,\n"
        "physical,Soybeans,,long,10000,mt,415.0,2026-08-05,-12.5,NOLA\n",
        encoding="utf-8",
    )
    book = positions_from_csv(path)
    assert len(book.futures) == 1
    position = book.futures[0]
    assert position.net_contracts == -73
    # 40 @ 1150 and 33 @ 1160 -> (40x1150 + 33x1160) / 73
    assert position.lot.average_cost == pytest.approx((40 * 1150.0 + 33 * 1160.0) / 73)
    assert book.physical[0].location == "NOLA"


def test_a_csv_row_of_an_unknown_kind_raises(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("kind,quantity\nspread,1\n", encoding="utf-8")
    with pytest.raises(PositionError, match="neither physical nor futures"):
        positions_from_csv(path)


def test_symbols_from_a_file_resolve_to_the_same_contract_identity():
    assert parse_symbol("zsx26.cbt").symbol == ZSX26.symbol
