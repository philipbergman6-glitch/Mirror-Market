"""Importing a broker, clearing or ERP export without inventing anything.

A client's export is the one file this project cannot control the shape of.
Every column name, every sign convention and every product code belongs to
somebody else's system, and the failure mode is not a crash — it is a clean
parse of the wrong thing. A short 68 lots read as a long 68 is a 136-lot error
that looks like a position.

So the workflow here is deliberately two steps, and the first one writes
nothing:

1. :func:`read_import` reads the file against a **profile** — a named,
   reviewable mapping the desk wrote once for its own broker — and returns an
   :class:`ImportReport`: what was accepted, what was rejected and why, which
   columns the profile did not claim, and the sha256 of the bytes read.
2. :func:`apply_import` turns an accepted report into a :class:`Book`, and
   refuses to do it while anything was rejected unless the caller says
   ``allow_partial=True`` in so many words.

The rules under test are all one rule wearing different clothes: **nothing is
guessed**. Not a sign, not a product, not a date format, not a missing column.
"""

from __future__ import annotations

from datetime import date

import pytest
from book_fixtures import ACCOUNT

from analysis.futures.imports import (
    ImportError_,
    ImportProfile,
    apply_import,
    load_profile,
    load_profiles,
    read_import,
)

CSV = f"""TradeDate,Ticker,B/S,Lots,AvgPx,Acct
2026-08-04,ZSX26,S,60,1172.25,{ACCOUNT}
2026-08-07,ZSX26,S,28,1180.00,{ACCOUNT}
2026-08-11,ZSX26,B,20,1165.50,{ACCOUNT}
"""

PROFILE = ImportProfile(
    name="synthetic-broker",
    source="SYNTHETIC Clearing LLC",
    kind="futures",
    quantity_sign="side_column",
    columns={
        "trade_date": "TradeDate",
        "contract": "Ticker",
        "side": "B/S",
        "quantity": "Lots",
        "price": "AvgPx",
        "account": "Acct",
    },
    side_values={"b": "long", "s": "short"},
    date_format="%Y-%m-%d",
)


def write(tmp_path, text: str = CSV, name: str = "export.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# The dry run
# ---------------------------------------------------------------------------
def test_reading_an_export_is_a_dry_run_that_produces_a_report_not_a_book(tmp_path):
    report = read_import(write(tmp_path), PROFILE)

    assert report.accepted_count == 3
    assert report.rejected == ()
    assert not hasattr(report, "book")
    assert report.is_clean is True


def test_the_report_stamps_the_bytes_it_read_so_a_changed_file_is_visible(tmp_path):
    first = read_import(write(tmp_path), PROFILE)
    second = read_import(write(tmp_path, CSV + f"2026-08-12,ZSX26,S,5,1170.00,{ACCOUNT}\n"), PROFILE)

    assert len(first.sha256) == 64
    assert first.sha256 != second.sha256


def test_every_row_carries_a_reference_that_is_stable_across_re_imports(tmp_path):
    """Re-importing the same export twice must not double the book."""
    path = write(tmp_path)
    first = read_import(path, PROFILE)
    second = read_import(path, PROFILE)

    references = [row.reference for row in first.accepted]
    assert references == [row.reference for row in second.accepted]
    assert len(set(references)) == 3
    assert all(row.reference.startswith(first.sha256[:8] + ":") for row in first.accepted)


def test_a_column_the_profile_does_not_claim_is_reported_not_ignored(tmp_path):
    """It might be the P&L column, or a second account. Silence would hide it."""
    text = CSV.replace("Acct", "Acct,Commission").replace(f",{ACCOUNT}", f",{ACCOUNT},12.50")
    report = read_import(write(tmp_path, text), PROFILE)

    assert "Commission" in report.unmapped_columns
    assert any("Commission" in note for note in report.notes)


# ---------------------------------------------------------------------------
# Refusals — the whole point
# ---------------------------------------------------------------------------
def test_a_missing_required_column_refuses_the_file_before_any_row_is_read(tmp_path):
    """A per-row failure here would read as bad data rather than a bad mapping."""
    text = CSV.replace("AvgPx", "Price")
    with pytest.raises(ImportError_, match="AvgPx"):
        read_import(write(tmp_path, text), PROFILE)


def test_an_unreadable_sign_convention_is_refused_at_profile_construction():
    with pytest.raises(ImportError_, match="quantity_sign"):
        ImportProfile(
            name="x", source="y", kind="futures", quantity_sign="guess",
            columns=dict(PROFILE.columns),
        )


def test_a_physical_profile_that_names_no_unit_and_no_default_is_refused():
    """Twelve thousand *what* is not a question a row can answer for itself."""
    with pytest.raises(ImportError_, match="unit"):
        ImportProfile(
            name="x", source="y", kind="physical", quantity_sign="signed",
            columns={"trade_date": "d", "commodity": "c", "quantity": "q", "price": "p"},
        )


def test_a_side_column_convention_without_a_side_column_is_refused():
    with pytest.raises(ImportError_, match="side"):
        ImportProfile(
            name="x", source="y", kind="futures", quantity_sign="side_column",
            columns={k: v for k, v in PROFILE.columns.items() if k != "side"},
        )


def test_an_unrecognised_side_value_rejects_the_row_rather_than_picking_one(tmp_path):
    text = CSV.replace("ZSX26,S,60", "ZSX26,X,60")
    report = read_import(write(tmp_path, text), PROFILE)

    assert report.accepted_count == 2
    rejected = report.rejected[0]
    assert rejected.row_number == 2
    assert "X" in rejected.reason and "side" in rejected.reason.lower()
    assert report.is_clean is False


def test_a_signed_quantity_convention_takes_the_sign_and_never_a_side(tmp_path):
    text = "TradeDate,Ticker,Lots,AvgPx\n2026-08-04,ZSX26,-60,1172.25\n"
    profile = ImportProfile(
        name="signed", source="y", kind="futures", quantity_sign="signed",
        columns={
            "trade_date": "TradeDate", "contract": "Ticker",
            "quantity": "Lots", "price": "AvgPx",
        },
    )
    report = read_import(write(tmp_path, text), profile)

    assert report.accepted[0].side == "short"
    assert report.accepted[0].quantity == 60.0


def test_a_date_that_does_not_match_the_declared_format_is_rejected_not_reparsed(tmp_path):
    """Trying a second format is how 03/04 becomes March in one file and April in the next."""
    text = CSV.replace("2026-08-04", "04/08/2026")
    report = read_import(write(tmp_path, text), PROFILE)

    assert report.accepted_count == 2
    assert "04/08/2026" in report.rejected[0].reason
    assert "%Y-%m-%d" in report.rejected[0].reason


def test_an_unmapped_product_code_is_rejected_rather_than_guessed(tmp_path):
    text = "TradeDate,Product,Side,Qty,Px\n2026-08-04,SOJA,S,100,402.50\n"
    profile = ImportProfile(
        name="erp", source="y", kind="physical", quantity_sign="side_column",
        columns={
            "trade_date": "TradeDate", "commodity": "Product", "side": "Side",
            "quantity": "Qty", "price": "Px",
        },
        commodity_map={"SOJ": "Soybeans"},   # deliberately not SOJA
        default_unit="MT",
    )
    report = read_import(write(tmp_path, text), profile)

    assert report.accepted_count == 0
    assert "SOJA" in report.rejected[0].reason
    assert "commodity_map" in report.rejected[0].reason


def test_a_blank_price_is_rejected_and_never_defaulted_to_zero(tmp_path):
    text = CSV.replace("60,1172.25", "60,")
    report = read_import(write(tmp_path, text), PROFILE)

    assert report.accepted_count == 2
    assert "price" in report.rejected[0].reason.lower()


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------
def test_applying_a_clean_report_builds_the_book_the_export_described(tmp_path):
    book = apply_import(read_import(write(tmp_path), PROFILE))

    assert len(book.futures) == 1
    position = book.futures[0]
    assert position.contract.symbol == "ZSX26"
    assert position.lot.net_quantity == -68.0
    assert position.fills[0].trade_date == date(2026, 8, 4)
    assert position.fills[0].reference.startswith(("", ""))  # carried, not blank
    assert position.fills[0].reference


def test_applying_a_report_with_rejections_is_refused_unless_asked_in_so_many_words(tmp_path):
    text = CSV.replace("ZSX26,S,60", "ZSX26,X,60")
    report = read_import(write(tmp_path, text), PROFILE)

    with pytest.raises(ImportError_, match="1 row"):
        apply_import(report)

    book = apply_import(report, allow_partial=True)
    assert book.futures[0].lot.net_quantity == -8.0     # -28 +20, the accepted rows only


def test_a_partial_import_says_on_the_book_that_it_is_partial(tmp_path):
    text = CSV.replace("ZSX26,S,60", "ZSX26,X,60")
    book = apply_import(read_import(write(tmp_path, text), PROFILE), allow_partial=True)

    assert any("1" in note and "rejected" in note for note in book.loaded_from)


def test_a_physical_export_becomes_physical_positions(tmp_path):
    text = "Date,Product,Side,Qty,Unit,Px,Loc\n2026-08-04,SOJ,B,12000,MT,402.50,Paranagua\n"
    profile = ImportProfile(
        name="erp", source="y", kind="physical", quantity_sign="side_column",
        columns={
            "trade_date": "Date", "commodity": "Product", "side": "Side",
            "quantity": "Qty", "unit": "Unit", "price": "Px", "location": "Loc",
        },
        commodity_map={"SOJ": "Soybeans"},
    )
    book = apply_import(read_import(write(tmp_path, text), profile))

    assert len(book.physical) == 1
    assert book.physical[0].commodity == "Soybeans"
    assert book.physical[0].quantity_mt == 12_000.0


# ---------------------------------------------------------------------------
# Profiles on disk
# ---------------------------------------------------------------------------
def test_a_profile_round_trips_through_yaml(tmp_path):
    (tmp_path / "broker.yml").write_text(
        "name: synthetic-broker\n"
        "source: SYNTHETIC Clearing LLC\n"
        "kind: futures\n"
        "quantity_sign: side_column\n"
        "date_format: '%Y-%m-%d'\n"
        "columns:\n"
        "  trade_date: TradeDate\n"
        "  contract: Ticker\n"
        "  side: B/S\n"
        "  quantity: Lots\n"
        "  price: AvgPx\n"
        "side_values:\n"
        "  b: long\n"
        "  s: short\n",
        encoding="utf-8",
    )
    profile = load_profile("synthetic-broker", directory=tmp_path)
    assert profile.columns["contract"] == "Ticker"
    assert profile.side_values["s"] == "short"


def test_a_missing_profile_directory_is_no_profiles_not_an_error(tmp_path):
    assert load_profiles(tmp_path / "absent") == ()


def test_asking_for_a_profile_that_does_not_exist_names_the_ones_that_do(tmp_path):
    (tmp_path / "a.yml").write_text(
        "name: alpha\nsource: x\nkind: futures\nquantity_sign: signed\n"
        "columns: {trade_date: d, contract: c, quantity: q, price: p}\n",
        encoding="utf-8",
    )
    with pytest.raises(ImportError_, match="alpha"):
        load_profile("beta", directory=tmp_path)


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------
def test_an_import_report_is_refused_by_the_public_guard(tmp_path):
    from analysis.futures.privacy import ClientDataLeak, assert_no_client_records

    report = read_import(write(tmp_path), PROFILE)
    with pytest.raises(ClientDataLeak):
        assert_no_client_records(report.to_dict(), where="public workstation")
