"""Parser tests for the AMS 3147 Gulf export bids and WA_GR101 port flows."""

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from fetchers.gulf_bids import _extract_text, _parse_gulf_bids
from fetchers.usda import _parse_destinations, _parse_port_flows
from pipeline.results import ScraperShapeError

_TODAY = date(2026, 7, 30)

# Trimmed from a real pypdf layout extraction of ams_3147.pdf (2026-07-29).
_GULF_TEXT = """
                        Louisiana and Texas Export Bids
                        OR Dept. of Ag Market News            July  29,  2026

                                    US #2 Yellow Corn  (Bulk)
Export Elevators - Conventional
 Region/Location  Sale Type  Basis (¢/Bu)  Basis Change  Price($/Bu)  Price Change  Average  Year Ago  Freight  Delivery
Gulf Coast Ports -   Bid   100.00U to 105.00U   UNCH   5.4900-5.5400   DN 0.0950   5.5150   4.7325   CIF-B   Current
         LA

                                    US #1  Soybeans  (Bulk)
Export Elevators - Conventional
Gulf Coast Ports -   Bid   120.00Q to 122.00Q   UNCH   12.9800-13.0000   DN 0.3400   12.9900   10.5275   CIF-B   Current
         LA
Gulf Coast Ports -   Bid   103.00X to 108.00X   UNCH   12.9575-13.0075   DN 0.2725   12.9825   10.5375   CIF-B   Sep
         LA

                                    US #2 Soft Red Winter Wheat  (Bulk)
Export Elevators - Conventional
Gulf Coast Ports -   Bid   Ordinary   50.00U   UNCH   7.1075   DN 0.0175   7.1075   5.6125   CIF-B   Current
         LA
"""


def test_gulf_bids_parses_all_sections() -> None:
    df = _parse_gulf_bids(_GULF_TEXT, today=_TODAY)
    assert sorted(df["commodity"].unique()) == ["Corn", "Soybeans", "Wheat"]
    assert (df["report_date"] == "2026-07-29").all()

    soy = df[(df["commodity"] == "Soybeans") & (df["delivery"] == "Current")].iloc[0]
    assert soy["basis_low"] == 120.0
    assert soy["basis_high"] == 122.0
    assert soy["futures_month"] == 8  # Q = August
    assert soy["price_high"] == 13.0
    assert soy["year_ago"] == 10.5275


def test_gulf_bids_wheat_single_value_quote() -> None:
    """Wheat rows carry a Protein column and may quote a single value."""
    df = _parse_gulf_bids(_GULF_TEXT, today=_TODAY)
    wheat = df[df["commodity"] == "Wheat"].iloc[0]
    assert wheat["basis_low"] == wheat["basis_high"] == 50.0
    assert wheat["price_low"] == wheat["price_high"] == 7.1075


def test_gulf_bids_ranged_change_column_parses() -> None:
    """AMS prints ranged change columns; both endpoints are preserved (#190)."""
    text = _GULF_TEXT.replace(
        "12.9800-13.0000   DN 0.3400", "12.9800-13.0000   DN 0.1025-DN 0.1075"
    ).replace("120.00Q to 122.00Q   UNCH", "120.00Q to 122.00Q   UP 1.00-DN 4.00")
    df = _parse_gulf_bids(text, today=_TODAY)
    soy = df[(df["commodity"] == "Soybeans") & (df["delivery"] == "Current")].iloc[0]
    assert soy["basis_change"] == "UP 1.00-DN 4.00"
    assert soy["price_change"] == "DN 0.1025-DN 0.1075"


def test_gulf_bids_blank_change_column_parses() -> None:
    """A blank change column yields None rather than dropping the row (#190)."""
    text = _GULF_TEXT.replace(
        "12.9800-13.0000   DN 0.3400   12.9900", "12.9800-13.0000      12.9900"
    ).replace("120.00Q to 122.00Q   UNCH", "120.00Q to 122.00Q       ")
    df = _parse_gulf_bids(text, today=_TODAY)
    soy = df[(df["commodity"] == "Soybeans") & (df["delivery"] == "Current")].iloc[0]
    assert pd.isna(soy["basis_change"])
    assert pd.isna(soy["price_change"])
    assert soy["price_low"] == 12.98
    assert soy["average"] == 12.99
    assert soy["year_ago"] == 10.5275


def test_gulf_bids_unreadable_row_hard_fails() -> None:
    """An unparseable bid row raises rather than being silently skipped (#190)."""
    text = _GULF_TEXT.replace("12.9800-13.0000", "TWELVE-NINETY-EIGHT")
    with pytest.raises(ScraperShapeError, match="unparseable bid row"):
        _parse_gulf_bids(text, today=_TODAY)


def test_gulf_bids_missing_year_ago_parses() -> None:
    """The Year Ago column is blank on some deliveries."""
    text = _GULF_TEXT.replace("12.9900   10.5275   CIF-B", "12.9900            CIF-B")
    df = _parse_gulf_bids(text, today=_TODAY)
    soy = df[(df["commodity"] == "Soybeans") & (df["delivery"] == "Current")].iloc[0]
    assert soy["average"] == 12.99
    assert pd.isna(soy["year_ago"])


def test_gulf_bids_stale_report_raises() -> None:
    with pytest.raises(ScraperShapeError, match="days old"):
        _parse_gulf_bids(_GULF_TEXT, today=date(2026, 9, 30))


def test_gulf_bids_missing_soy_section_raises() -> None:
    text = _GULF_TEXT.replace("Soybeans", "Sorghum")
    with pytest.raises(ScraperShapeError, match="soybean section"):
        _parse_gulf_bids(text, today=_TODAY)


# ── Live AMS 3147 PDF (#190) ─────────────────────────────────────────────────
# tests/fixtures/ams_3147_2026-08-11.pdf — live download of
# https://www.ams.usda.gov/mnreports/ams_3147.pdf on 2026-08-11. Its
# headline soybean Current row carries a *ranged* price change
# ("DN 0.1025-DN 0.1075") which the single-token regex dropped, and
# several rows have a blank Year Ago column.

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _live_gulf_text() -> str:
    return _extract_text((_FIXTURES_DIR / "ams_3147_2026-08-11.pdf").read_bytes())


def test_gulf_bids_live_pdf_keeps_headline_soybean_current_row() -> None:
    df = _parse_gulf_bids(_live_gulf_text(), today=date(2026, 8, 11))
    assert (df["report_date"] == "2026-08-11").all()

    soy = df[(df["commodity"] == "Soybeans") & (df["delivery"] == "Current")]
    assert len(soy) == 1
    row = soy.iloc[0]
    assert row["basis_low"] == 95.0
    assert row["basis_high"] == 100.0
    assert row["price_low"] == 12.4250
    assert row["price_high"] == 12.6875
    assert row["price_change"] == "DN 0.1025-DN 0.1075"
    assert row["average"] == 12.5563
    assert row["year_ago"] == 10.9775
    assert row["freight"] == "CIF-B"


def test_gulf_bids_live_pdf_keeps_every_bid_row() -> None:
    """No row in the real report is silently shed (7 corn, 7 soy, 8 wheat)."""
    df = _parse_gulf_bids(_live_gulf_text(), today=date(2026, 8, 11))
    counts = df["commodity"].value_counts().to_dict()
    assert counts == {"Corn": 7, "Soybeans": 7, "Wheat": 8}

    # Blank Year Ago cells survive as NULL rather than shifting the columns.
    corn_nov = df[(df["commodity"] == "Corn") & (df["delivery"] == "Nov")].iloc[0]
    assert corn_nov["average"] == 5.6100
    assert pd.isna(corn_nov["year_ago"])
    assert corn_nov["freight"] == "CIF-B"


# Trimmed from the real WA_GR101 report (week ending 2026-07-23).
_PORT_TEXT = """
                   GRAINS INSPECTED AND/OR WEIGHED FOR EXPORT BY REGION AND PORT AREA
                                  REPORTED IN WEEK ENDING JUL 23, 2026
                                            -- METRIC TONS --
--------------------------------------------------------------------------------------------------
                                           CORN      CORN
  REGION    PORT AREA       WHEAT    RYE  YELLOW     WHITE   SORGHUM  SOYBEANS  FLAXSEED   TOTALS


GULF      MISSISSIPPI R.  107,727    0    668,261  23,237        0   219,159      0     1,018,384
          N. TEXAS         79,055    0      6,727       0   38,279         0      0       124,061
            SUBTOTAL      186,782    0    674,988  23,237   38,279   219,159      0     1,142,445

PACIFIC   PUGET SOUND           0    0    123,836       0   10,687         0      0       134,523
          COLUMBIA R.     162,877    0    332,830       0        0         0      0       495,707
            SUBTOTAL      162,877    0    456,666       0   10,687         0      0       630,230

  TOTAL                   394,785    0  1,464,791  23,237   50,141   348,850     48     2,281,852
"""


def test_port_flows_parses_regions_and_subtotals() -> None:
    df = _parse_port_flows(_PORT_TEXT)
    assert (df["week_ending"] == "2026-07-23").all()

    gulf_soy = df[
        (df["region"] == "GULF")
        & (df["port_area"] == "SUBTOTAL")
        & (df["commodity"] == "Soybeans")
    ]
    assert gulf_soy["inspections_mt"].iloc[0] == 219_159.0

    # Region carries over to continuation rows
    ntx = df[(df["port_area"] == "N. TEXAS") & (df["commodity"] == "Wheat")]
    assert ntx["region"].iloc[0] == "GULF"

    # Grand TOTAL row is excluded
    assert "TOTAL" not in set(df["port_area"])


def test_port_flows_missing_table_raises() -> None:
    with pytest.raises(ScraperShapeError, match="PORT AREA"):
        _parse_port_flows("GRAIN 07/23/2026 07/16/2026 07/09/2026\nSOYBEANS 1 2 3")


# Trimmed from the real WA_GR101 report (week ending 2026-07-30).
_DEST_TEXT = """
                GRAINS INSPECTED AND/OR WEIGHED FOR EXPORT BY REGION AND COUNTRY OF DESTINATION
                                     REPORTED IN WEEK ENDING JUL 30, 2026
                                               -- METRIC TONS --

---------------------------------------------------------------------------------------------------------------

                                    CORN      CORN
  REGION    COUNTRY       WHEAT    YELLOW     WHITE    SORGHUM  SOYBEANS      TOTALS


GULF      COLOMBIA            0    271,599      0          0     7,411       279,010
          EGYPT               0          0      0          0    56,944        56,944
            SUBTOTAL          0    271,599      0          0    64,355       335,954

INTERIOR  MEXICO         43,810    259,655      0          0    74,245       377,710
          UN KINGDOM          0     20,471      0          0         0        20,471
            SUBTOTAL     43,810    280,126      0          0    74,245       398,181

  TOTAL                  43,810    551,725      0          0   138,600       734,135


TOTAL
SHIPMENTS TO CANADA*
                              0          0      0          0    11,865        11,865
"""


def test_destinations_parses_countries_and_subtotals() -> None:
    df = _parse_destinations(_DEST_TEXT)
    assert (df["week_ending"] == "2026-07-30").all()

    mex_soy = df[(df["country"] == "MEXICO") & (df["commodity"] == "Soybeans")]
    assert mex_soy["inspections_mt"].iloc[0] == 74_245.0
    assert mex_soy["region"].iloc[0] == "INTERIOR"

    # Multi-word country names survive the 2+-space field split
    assert "UN KINGDOM" in set(df["country"])

    # Grand TOTAL and Canada-footnote rows are excluded
    assert "TOTAL" not in set(df["country"])
    total_soy = df[(df["country"] == "SUBTOTAL") & (df["commodity"] == "Soybeans")]
    assert total_soy["inspections_mt"].sum() == 138_600.0


def test_destinations_missing_table_raises() -> None:
    with pytest.raises(ScraperShapeError, match="COUNTRY OF DESTINATION"):
        _parse_destinations("GRAIN 07/23/2026 07/16/2026 07/09/2026\nSOYBEANS 1 2 3")


# ── Live 6-column layout (#55) ──────────────────────────────────────────────
# tests/fixtures/ams_inspections_2026-08-06.txt — live download of
# https://www.ams.usda.gov/mnreports/wa_gr101.txt on 2026-08-11 (report
# week ending AUG 06, 2026). Table C now carries six grain columns —
# WHEAT, CORN YELLOW, CORN WHITE, SORGHUM, SOYBEANS, CANOLA — where the
# hardcoded order expected seven (RYE and FLAXSEED gone, CANOLA new).

_FIXTURES = Path(__file__).parent / "fixtures"


def _live_report() -> str:
    return (_FIXTURES / "ams_inspections_2026-08-06.txt").read_text(encoding="utf-8")


def test_port_flows_parses_live_six_column_layout() -> None:
    df = _parse_port_flows(_live_report())
    assert (df["week_ending"] == "2026-08-06").all()

    # Columns come from the report header, not a hardcoded order.
    assert set(df["commodity"]) == {
        "Wheat", "Corn Yellow", "Corn White", "Sorghum", "Soybeans", "Canola",
    }

    miss_soy = df[
        (df["port_area"] == "MISSISSIPPI R.") & (df["commodity"] == "Soybeans")
    ]
    assert miss_soy["region"].iloc[0] == "GULF"
    assert miss_soy["inspections_mt"].iloc[0] == 260_822.0

    # CANOLA occupies the column the old parser read as FLAXSEED.
    miss_canola = df[
        (df["port_area"] == "MISSISSIPPI R.") & (df["commodity"] == "Canola")
    ]
    assert miss_canola["inspections_mt"].iloc[0] == 30_128.0

    gulf_sub = df[
        (df["region"] == "GULF")
        & (df["port_area"] == "SUBTOTAL")
        & (df["commodity"] == "Soybeans")
    ]
    assert gulf_sub["inspections_mt"].iloc[0] == 260_822.0
    assert "TOTAL" not in set(df["port_area"])


def test_destinations_parses_live_six_column_layout() -> None:
    df = _parse_destinations(_live_report())
    assert (df["week_ending"] == "2026-08-06").all()
    assert set(df["commodity"]) == {
        "Wheat", "Corn Yellow", "Corn White", "Sorghum", "Soybeans", "Canola",
    }

    china_soy = df[
        (df["region"] == "GULF")
        & (df["country"] == "CHINA")
        & (df["commodity"] == "Soybeans")
    ]
    assert china_soy["inspections_mt"].iloc[0] == 65_935.0
    assert "TOTAL" not in set(df["country"])


def test_port_flows_unknown_header_column_hard_fails() -> None:
    """A genuinely new grain column is drift — hard-fail, never mislabel."""
    text = _live_report().replace("SOYBEANS   CANOLA", "TRITICALE  CANOLA", 1)
    with pytest.raises(ScraperShapeError, match="unrecognised column"):
        _parse_port_flows(text)


def test_port_flows_parses_earlier_real_vintage() -> None:
    """The 2026-05-11 capture (FLAXSEED column, no CANOLA) parses too.

    Two real vintages with different column sets prove the column order is
    read off the header rather than pinned to whatever last shipped.
    """
    text = (_FIXTURES / "ams_inspections.txt").read_text(encoding="utf-8")
    expected = {
        "Wheat", "Corn Yellow", "Corn White", "Sorghum", "Soybeans", "Flaxseed",
    }
    assert set(_parse_port_flows(text)["commodity"]) == expected
    assert set(_parse_destinations(text)["commodity"]) == expected


def test_port_flows_row_width_mismatch_hard_fails() -> None:
    text = _live_report().replace(
        "GULF      S. TEXAS              0           0       0    55,593         0        0     55,593",
        "GULF      S. TEXAS              0           0       0    55,593         0     55,593",
    )
    with pytest.raises(ScraperShapeError, match="numeric columns"):
        _parse_port_flows(text)
