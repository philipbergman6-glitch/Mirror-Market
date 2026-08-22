"""Layer 20's MARS API path — mapping, guards, and the PDF cross-check (#283).

The decisive test here is ``test_api_rows_match_the_pdf_report``: both
fixtures are the *same* AMS report of 2026-08-11 — one as the published
PDF, one as the API's structured rows — so the two parsers must produce
the same table, cell for cell. That is the cross-validation the backfill
rides on: 6.5 years of archive enter the same `gulf_bids` table as the
PDF-parsed rows and must be indistinguishable from them.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from fetchers.gulf_bids import (
    _extract_text,
    _map_api_rows,
    _parse_gulf_bids,
)
from pipeline.results import ScraperShapeError
from scripts.backfill_gulf_bids_mars import check_against_stored_rows

_FIXTURES = Path(__file__).parent / "fixtures"
_PDF = _FIXTURES / "ams_3147_2026-08-11.pdf"
_API = _FIXTURES / "ams_3147_2026-08-11_api.json"


def _api_rows() -> list[dict]:
    return json.loads(_API.read_text())["results"]


def _one_row(**overrides) -> dict:
    """A single soybean detail row, overridable field by field."""
    row = dict(_api_rows()[0])
    row.update(overrides)
    return row


# ── The cross-check ──────────────────────────────────────────────────────────


def test_api_rows_match_the_pdf_report() -> None:
    """Same report, two transports — one table (ticket #283 step 3)."""
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    api = _map_api_rows(_api_rows())

    key = ["report_date", "commodity", "location", "delivery"]
    pdf = pdf.sort_values(key).reset_index(drop=True)
    api = api.sort_values(key).reset_index(drop=True)[pdf.columns]

    assert len(api) == len(pdf) == 22
    pd.testing.assert_frame_equal(api, pdf, check_dtype=False)


def test_split_contract_legs_survive_the_api_mapping() -> None:
    """The 95.00Q-to-100.00X row keeps a month per leg (#196)."""
    api = _map_api_rows(_api_rows())
    row = api[(api["commodity"] == "Soybeans") & (api["delivery"] == "Current")].iloc[0]
    assert row["basis_low"] == 95.0
    assert row["basis_high"] == 100.0
    assert row["futures_month"] == 8  # August (Q)
    assert row["futures_month_high"] == 11  # November (X)
    assert row["price_change"] == "DN 0.1025-DN 0.1075"


# ── Delivery labelling ───────────────────────────────────────────────────────


def test_half_month_deliveries_carry_the_pdf_superscripts() -> None:
    api = _map_api_rows(_api_rows())
    soy = set(api[api["commodity"] == "Soybeans"]["delivery"])
    assert {"Current", "Sep¹", "Sep²", "Oct", "Nov¹", "Nov²", "Dec"} == soy


def test_current_row_ignores_an_absent_delivery_window() -> None:
    df = _map_api_rows([_one_row(**{"current": "Yes", "delivery_start": None,
                                    "delivery Start Half": None})])
    assert df["delivery"].iloc[0] == "Current"


def test_a_forward_row_without_a_delivery_window_raises() -> None:
    with pytest.raises(ScraperShapeError, match="delivery"):
        _map_api_rows([_one_row(**{"current": "No", "delivery_start": None})])


def test_a_delivery_window_spanning_two_months_raises() -> None:
    """One label cannot name two windows — drift, not a row to guess at."""
    with pytest.raises(ScraperShapeError, match="delivery"):
        _map_api_rows([_one_row(**{"delivery_start": "2026-09-01",
                                   "delivery_end": "2026-10-01"})])


def test_an_unknown_half_marker_raises() -> None:
    with pytest.raises(ScraperShapeError, match="half"):
        _map_api_rows([_one_row(**{"delivery Start Half": "Middle Third",
                                   "delivery End Half": "Middle Third"})])


# ── Futures months ───────────────────────────────────────────────────────────


def test_futures_month_label_disagreeing_with_its_code_raises() -> None:
    """"August (X)" is one of the two wrong — never silently pick a leg."""
    with pytest.raises(ScraperShapeError, match="futures month"):
        _map_api_rows([_one_row(**{"basis Min Futures Month": "August (X)"})])


def test_an_unparseable_futures_month_raises() -> None:
    with pytest.raises(ScraperShapeError, match="futures month"):
        _map_api_rows([_one_row(**{"basis Max Futures Month": "Nov"})])


# ── Change columns ───────────────────────────────────────────────────────────


def test_unchanged_legs_collapse_to_one_unch() -> None:
    api = _map_api_rows(_api_rows())
    assert (api["basis_change"] == "UNCH").all()


def test_a_blank_change_stays_null_not_zero() -> None:
    """AMS leaves the column blank on some deliveries (#190) — NULL, never 0."""
    df = _map_api_rows([_one_row(**{"basis Min Change": None,
                                    "basis Min Direction": None,
                                    "basis Max Change": None,
                                    "basis Max Direction": None})])
    assert df["basis_change"].iloc[0] is None


def test_a_half_blank_change_raises() -> None:
    """One leg quoted and the other blank is a shape we cannot render."""
    with pytest.raises(ScraperShapeError, match="change"):
        _map_api_rows([_one_row(**{"basis Max Change": None,
                                   "basis Max Direction": None})])


def test_an_unknown_direction_raises() -> None:
    with pytest.raises(ScraperShapeError, match="direction"):
        _map_api_rows([_one_row(**{"price Min Direction": "SIDEWAYS"})])


# ── Scope guards ─────────────────────────────────────────────────────────────


def test_commodities_outside_the_report_sections_are_dropped() -> None:
    """Sorghum prints on this report; Layer 20 carries the soy complex's three."""
    rows = _api_rows() + [_one_row(**{"commodity": "Sorghum",
                                      "trade_loc": "Gulf Coast Ports - TX",
                                      "freight": "Delivered",
                                      "trans_mode": "Truck/Rail"})]
    api = _map_api_rows(rows)
    assert "Sorghum" not in set(api["commodity"])
    assert len(api) == 22


def test_a_stored_commodity_at_an_unmapped_port_raises() -> None:
    """A TX soybean row would collide with the LA one under the stored key."""
    with pytest.raises(ScraperShapeError, match="trade_loc|location"):
        _map_api_rows([_one_row(**{"trade_loc": "Gulf Coast Ports - TX"})])


def test_an_unmapped_freight_term_raises() -> None:
    with pytest.raises(ScraperShapeError, match="freight"):
        _map_api_rows([_one_row(**{"trans_mode": "Truck/Rail"})])


def test_a_quote_that_is_not_a_basis_bid_raises() -> None:
    with pytest.raises(ScraperShapeError, match="quote_type|sale"):
        _map_api_rows([_one_row(**{"quote_type": "Flat Price"})])


def test_unexpected_units_raise() -> None:
    """¢/Bu basis over $/Bu price is the unit contract the mapping assumes."""
    with pytest.raises(ScraperShapeError, match="unit"):
        _map_api_rows([_one_row(**{"price_unit": "$ Per Cwt"})])


def test_report_dates_are_parsed_never_sorted_as_strings() -> None:
    """MM/DD/YYYY sorts lexically; the stored column must be ISO (#283 trap)."""
    api = _map_api_rows(_api_rows())
    assert (api["report_date"] == "2026-08-11").all()


def test_an_empty_result_set_maps_to_an_empty_frame() -> None:
    """A date the archive has no detail rows for is a non-publication."""
    df = _map_api_rows([])
    assert df.empty


# ── The backfill's pre-write cross-check ─────────────────────────────────────


def test_matching_transports_raise_no_disagreement() -> None:
    api = _map_api_rows(_api_rows())
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    assert check_against_stored_rows(api, pdf) == []


def test_a_changed_cell_is_reported() -> None:
    api = _map_api_rows(_api_rows())
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    pdf.loc[pdf.index[0], "basis_low"] += 1.0
    problems = check_against_stored_rows(api, pdf)
    assert len(problems) == 1
    assert "basis_low" in problems[0]


def test_a_column_the_stored_rows_predate_is_not_a_disagreement() -> None:
    """`futures_month_high` is NULL on rows stored before #196 — not a conflict."""
    api = _map_api_rows(_api_rows())
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    pdf["futures_month_high"] = None
    assert check_against_stored_rows(api, pdf) == []


def test_rows_only_one_transport_carries_are_not_a_disagreement() -> None:
    api = _map_api_rows(_api_rows())
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    assert check_against_stored_rows(api, pdf.iloc[:5]) == []


def test_dates_only_one_transport_carries_are_not_compared() -> None:
    api = _map_api_rows(_api_rows())
    pdf = _parse_gulf_bids(_extract_text(_PDF.read_bytes()), today=date(2026, 8, 11))
    pdf["report_date"] = "2024-01-02"
    pdf.loc[pdf.index[0], "basis_low"] += 1.0
    assert check_against_stored_rows(api, pdf) == []
