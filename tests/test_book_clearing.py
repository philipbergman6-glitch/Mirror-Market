"""The official number and our number are two numbers, and stay two numbers.

``analysis/futures/positions.py`` marks the book against delayed daily closes.
That is a *management estimate*: it is reproducible, it is useful, and it will
not match what the clearer says, because the clearer used the settlement and we
did not. The temptation is to reconcile by adopting whichever figure looks
better, or by quietly overwriting the mark with the statement's price. Both
destroy the only thing a reconciliation is for — the difference.

So the rules under test are:

1. A clearing line is stamped ``ATTESTED_SETTLEMENT``: official for that
   account, and still not proven by anything this project ingests.
2. A reconciliation reports both bases, labelled, and never a merged total.
3. A difference outside tolerance is *reported*, never corrected.
4. A contract on the statement that is not in the book is a finding, not a
   silently dropped row — it is a position the desk did not know it had.
5. A statement struck on a different date than the valuation is compared with a
   warning, because two dates are two markets.
6. Nothing is inferred: a statement with no unrealised column yields ``None``,
   not zero.
"""

from __future__ import annotations

from datetime import date

import pytest
from book_fixtures import ACCOUNT, BROKER, TODAY, quote_for, synthetic_book

from analysis.futures.clearing import (
    CLEARING_BASIS,
    ClearingError,
    ClearingLine,
    ClearingStatement,
    PnlBasis,
    load_statements,
    parse_statement,
    reconcile,
)
from analysis.futures.positions import MANAGEMENT_BASIS, value_book

pytestmark = pytest.mark.filterwarnings("ignore::DeprecationWarning")


def statement(**overrides) -> ClearingStatement:
    payload = {
        "account": ACCOUNT,
        "broker": BROKER,
        "statement_date": TODAY,
        "lines": (
            ClearingLine(
                symbol="ZSX26",
                description="SOYBEAN NOV26",
                quantity=-68.0,
                settlement_price=1150.00,
                realised_usd=0.0,
                unrealised_usd=-15_300.0,
            ),
        ),
    }
    payload.update(overrides)
    return ClearingStatement(**payload)


def valued():
    return value_book(synthetic_book(), as_of=TODAY, quote_for=quote_for())


# ---------------------------------------------------------------------------
# What a clearing number is
# ---------------------------------------------------------------------------
def test_a_clearing_price_is_attested_but_not_settlement_proven():
    from pricing.semantics import PriceType

    line = statement().lines[0]
    assert line.price_type is PriceType.ATTESTED_SETTLEMENT
    assert line.price_type.is_settlement_proven is False
    assert "not proven" in line.price_type.caveat


def test_the_two_bases_are_different_constants_and_neither_is_the_default():
    assert CLEARING_BASIS != MANAGEMENT_BASIS
    assert PnlBasis.OFFICIAL_CLEARING.value == CLEARING_BASIS
    assert PnlBasis.MANAGEMENT_ESTIMATE.value == MANAGEMENT_BASIS


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------
def test_a_reconciliation_reports_both_bases_side_by_side_and_no_merged_total():
    result = reconcile(valued(), statement())
    payload = result.to_dict()

    assert payload["official"]["basis"] == CLEARING_BASIS
    assert payload["management"]["basis"] == MANAGEMENT_BASIS
    # There is no combined, netted or "reconciled" P&L anywhere in the payload:
    # a single number would be neither desk's and would be acted on as both.
    assert not any(
        key in payload for key in ("total_usd", "reconciled_usd", "combined_usd", "net_usd")
    )


def mirroring(valuation, *, drift: float = 0.0) -> ClearingStatement:
    """A statement that reports exactly what the book marked, plus ``drift``."""
    from analysis.futures.positions import BookKind

    return statement(lines=tuple(
        ClearingLine(
            symbol=position.key,
            description=f"{position.commodity.upper()} {position.key}",
            quantity=position.net_quantity,
            settlement_price=position.mark or 0.0,
            realised_usd=position.realised_usd,
            unrealised_usd=(position.unrealised_usd or 0.0) + drift,
        )
        for position in valuation.positions
        if position.kind is BookKind.FUTURES
    ))


def test_a_difference_inside_tolerance_agrees_without_either_number_moving():
    ours = valued()
    mine = next(p for p in ours.positions if p.key == "ZSX26")
    result = reconcile(ours, mirroring(ours, drift=5.0))
    row = result.row("ZSX26")

    assert row.agrees is True
    assert row.official_unrealised_usd == pytest.approx((mine.unrealised_usd or 0.0) + 5.0)
    assert row.management_unrealised_usd == pytest.approx(mine.unrealised_usd)
    assert result.agrees is True
    assert result.not_in_book == () and result.not_on_statement == ()


def test_a_difference_outside_tolerance_is_reported_and_nothing_is_corrected():
    ours = valued()
    mine = next(p for p in ours.positions if p.key == "ZSX26")
    theirs = statement(lines=(
        ClearingLine(
            symbol="ZSX26",
            description="SOYBEAN NOV26",
            quantity=mine.net_quantity,
            settlement_price=1150.00,
            realised_usd=mine.realised_usd,
            unrealised_usd=(mine.unrealised_usd or 0.0) - 9_000.0,
        ),
    ))
    result = reconcile(ours, theirs)
    row = result.row("ZSX26")

    assert row.agrees is False
    assert row.difference_usd == pytest.approx(-9_000.0, abs=1.0)
    assert result.agrees is False
    # The valuation we were handed is untouched — reconciliation reads, never writes.
    assert ours.positions[0].unrealised_usd == mine.unrealised_usd
    assert "difference" in result.summary.lower() or "differ" in result.summary.lower()


def test_a_quantity_mismatch_is_its_own_finding_not_a_price_difference():
    """The clearer says 70 lots, the book says 68. That is not a P&L question."""
    ours = valued()
    theirs = statement(lines=(
        ClearingLine(
            symbol="ZSX26", description="SOYBEAN NOV26", quantity=-70.0,
            settlement_price=1150.00, realised_usd=0.0, unrealised_usd=-15_300.0,
        ),
    ))
    row = reconcile(ours, theirs).row("ZSX26")

    assert row.quantity_agrees is False
    assert row.official_quantity == -70.0
    assert row.management_quantity == -68.0
    assert any("quantit" in note.lower() for note in row.notes)


def test_a_contract_on_the_statement_that_is_not_in_the_book_is_a_finding():
    ours = valued()
    theirs = statement(lines=statement().lines + (
        ClearingLine(
            symbol="ZCZ26", description="CORN DEC26", quantity=12.0,
            settlement_price=430.25, realised_usd=0.0, unrealised_usd=1_100.0,
        ),
    ))
    result = reconcile(ours, theirs)

    assert "ZCZ26" in result.not_in_book
    assert result.agrees is False
    row = result.row("ZCZ26")
    assert row.management_unrealised_usd is None
    assert any("not in the entered book" in note for note in row.notes)


def test_a_book_position_absent_from_the_statement_is_also_a_finding():
    ours = valued()
    theirs = statement(lines=())
    result = reconcile(ours, theirs)

    assert "ZSX26" in result.not_on_statement
    assert result.agrees is False
    assert reconcile(ours, theirs).row("ZSX26").official_unrealised_usd is None


def test_only_futures_are_reconciled_and_the_physical_says_why():
    """A clearer holds no beans. Comparing them would invent a discrepancy."""
    result = reconcile(valued(), statement())

    assert all(not row.key.startswith("Soybean") for row in result.rows)
    assert any("physical" in note.lower() for note in result.notes)


def test_comparing_two_dates_warns_rather_than_silently_comparing_two_markets():
    result = reconcile(valued(), statement(statement_date=date(2026, 8, 15)))

    assert any("2026-08-15" in warning for warning in result.warnings)
    assert any("date" in warning.lower() for warning in result.warnings)


def test_a_statement_with_no_unrealised_column_yields_none_not_zero():
    line = ClearingLine(
        symbol="ZSX26", description="SOYBEAN NOV26", quantity=-68.0,
        settlement_price=1150.00, realised_usd=0.0, unrealised_usd=None,
    )
    row = reconcile(valued(), statement(lines=(line,))).row("ZSX26")

    assert row.official_unrealised_usd is None
    assert row.difference_usd is None
    assert row.agrees is None          # unknown is not agreement
    assert any("did not state" in note for note in row.notes)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def test_a_statement_document_round_trips_through_the_parser(tmp_path):
    payload = {
        "account": ACCOUNT,
        "broker": BROKER,
        "statement_date": "2026-08-19",
        "currency": "USD",
        "lines": [
            {
                "symbol": "ZSX26",
                "description": "SOYBEAN NOV26",
                "quantity": -68,
                "settlement_price": 1150.0,
                "realised_usd": 0.0,
                "unrealised_usd": -15300.0,
            },
        ],
    }
    parsed = parse_statement(payload, where="test")
    assert parsed.account == ACCOUNT
    assert parsed.statement_date == date(2026, 8, 19)
    assert parsed.lines[0].quantity == -68.0


def test_a_statement_with_no_date_is_refused_rather_than_dated_today():
    with pytest.raises(ClearingError, match="statement_date"):
        parse_statement({"account": "X", "lines": []}, where="test")


def test_a_statement_with_no_account_is_refused():
    """An unattributed statement cannot be reconciled against anything."""
    with pytest.raises(ClearingError, match="account"):
        parse_statement({"statement_date": "2026-08-19", "lines": []}, where="test")


def test_a_line_with_no_settlement_price_is_refused_not_marked_at_the_board():
    with pytest.raises(ClearingError, match="settlement_price"):
        parse_statement(
            {
                "account": "X", "statement_date": "2026-08-19",
                "lines": [{"symbol": "ZSX26", "quantity": -68}],
            },
            where="test",
        )


def test_a_missing_clearing_directory_is_no_statements_not_an_error(tmp_path):
    assert load_statements(tmp_path / "absent") == ()


def test_a_present_but_malformed_statement_raises(tmp_path):
    (tmp_path / "aug.yml").write_text("- not a mapping\n", encoding="utf-8")
    with pytest.raises(ClearingError):
        load_statements(tmp_path)


def test_statements_load_newest_first_so_the_current_one_is_reconciled(tmp_path):
    for day in ("2026-08-14", "2026-08-19"):
        (tmp_path / f"{day}.yml").write_text(
            f"account: {ACCOUNT}\nstatement_date: {day}\nlines: []\n", encoding="utf-8",
        )
    loaded = load_statements(tmp_path)
    assert [s.statement_date.isoformat() for s in loaded] == ["2026-08-19", "2026-08-14"]


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------
def test_a_reconciliation_payload_is_refused_by_the_public_guard():
    """It carries an account number and a broker name. It is never public."""
    from analysis.futures.privacy import ClientDataLeak, assert_no_client_records

    payload = reconcile(valued(), statement()).to_dict()
    with pytest.raises(ClientDataLeak):
        assert_no_client_records(payload, where="public workstation")
