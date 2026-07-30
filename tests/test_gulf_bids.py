"""Parser tests for the AMS 3147 Gulf export bids and WA_GR101 port flows."""

from datetime import date

import pytest

from fetchers.gulf_bids import _parse_gulf_bids
from fetchers.usda import _parse_port_flows
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


def test_gulf_bids_stale_report_raises() -> None:
    with pytest.raises(ScraperShapeError, match="days old"):
        _parse_gulf_bids(_GULF_TEXT, today=date(2026, 9, 30))


def test_gulf_bids_missing_soy_section_raises() -> None:
    text = _GULF_TEXT.replace("Soybeans", "Sorghum")
    with pytest.raises(ScraperShapeError, match="soybean section"):
        _parse_gulf_bids(text, today=_TODAY)


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
