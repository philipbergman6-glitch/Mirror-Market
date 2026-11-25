"""AL1 #640: the verification record and the licence-safe write (spec #639).

Committed records are a publication surface: for each verifying publisher whose
terms forbid it, no committed record carries the verifying value, and for each
licence-restricted displayed series none carries the displayed value. The
deltas and the result always survive.
"""

from __future__ import annotations

from dataclasses import fields, replace

import pytest

from verify import licence, record

SPEC_FIELDS = (
    "ledger_edition", "site_edition", "page", "page_sha256", "stratum", "series", "displayed_value",
    "displayed_unit", "observation_date", "contract_or_window", "fx_date", "event_class", "tier", "kind",
    "source_identity", "source_url", "source_vintage", "transport", "source_value", "source_unit", "conversion",
    "delta_native", "delta_usd_mt", "tolerance", "threshold", "band", "tolerance_version", "result", "material",
    "disposition", "explanation", "owner", "issue", "verifier", "verified_at",
)

BASE = record.Record(
    ledger_edition=None, site_edition="2026-11-24T19:24:28+00:00", page="markets/dalian.html",
    page_sha256="0" * 64, stratum="2", series="DCE Soybean Meal", displayed_value="3,081.00",
    displayed_unit="CNY/MT", observation_date="2026-11-24", contract_or_window="m2701 (inferred)",
    fx_date=None, event_class="ordinary", tier="1", kind="exact", source_identity="DCE 日行情",
    source_url=None, source_vintage="trade date 20261124; publication time unknown",
    transport="independent document", source_value=3082.0, source_unit="CNY/MT",
    conversion="CNY/MT as printed", delta_native=-1.0, delta_usd_mt=-0.15, tolerance="±0.5 CNY/MT",
    threshold=0.5, band=None, tolerance_version="v1 sha256:abcdefabcdef", result="discrepancy",
    material=False, disposition="unexplained", explanation="1 CNY under the DCE close", owner=None, issue=None,
    verifier="verify/dce.py@0123456789abcdef0123456789abcdef01234567", verified_at="2026-11-25T08:04:00Z",
    publisher="dce",
)


def test_the_schema_is_the_spec_field_list_in_order_plus_publisher_and_withheld_reason():
    names = tuple(f.name for f in fields(record.Record))
    assert names == (*SPEC_FIELDS, "publisher", "withheld_reason")


RESTRICTED = sorted(name for name, terms in licence.PUBLISHER_TERMS.items() if not terms.show_source_value)


def test_the_restricted_publishers_are_the_spec_table():
    assert set(RESTRICTED) == {"cme_bulletin", "dce", "magyp", "cfets", "sarb", "fbil", "grain_sa_jse"}


@pytest.mark.parametrize("publisher", RESTRICTED)
def test_a_restricted_publishers_value_is_never_committed(publisher):
    out = record.committed(replace(BASE, stratum="6", series="X/USD", publisher=publisher))
    assert out["source_value"] is None
    assert licence.WITHHELD_CAPTION in out["withheld_reason"]
    # What the reader is owed survives: the deltas and the grade.
    assert out["delta_native"] == -1.0 and out["delta_usd_mt"] == -0.15
    assert out["result"] == "discrepancy"


@pytest.mark.parametrize("stratum", sorted(licence.RESTRICTED_DISPLAYED_STRATA))
def test_a_restricted_displayed_series_value_is_never_committed(stratum):
    out = record.committed(replace(BASE, stratum=stratum, publisher="usda_ams"))
    assert out["displayed_value"] is None
    assert licence.DISPLAYED_WITHHELD in out["withheld_reason"]


def test_an_open_publisher_keeps_its_value():
    out = record.committed(replace(BASE, stratum="6", series="CNY/USD", publisher="ecb", source_value=0.15004859))
    assert out["source_value"] == 0.15004859
    assert out["withheld_reason"] is None


def test_a_withheld_value_leaking_through_free_text_is_refused():
    leaky = replace(BASE, conversion="DCE close 3082.00 CNY/MT")
    with pytest.raises(record.RecordError, match="conversion"):
        record.committed(leaky)


def test_a_withheld_displayed_value_leaking_through_free_text_is_refused():
    leaky = replace(BASE, publisher="usda_ams", explanation="page showed 3,081.00")
    with pytest.raises(record.RecordError, match="explanation"):
        record.committed(leaky)


def test_an_unknown_publisher_is_refused_rather_than_guessed():
    with pytest.raises(KeyError, match="refusing to guess"):
        record.committed(replace(BASE, publisher="somebody"))


@pytest.mark.parametrize("change, why", [
    ({"result": "fine"}, "result"),
    ({"event_class": "revision"}, "event_class"),
    ({"result": "metadata fail", "material": False}, "material at any delta"),
    ({"result": "pass", "material": True, "disposition": None}, "not material"),
    ({"result": "not checked", "explanation": None, "disposition": None, "material": None}, "reason"),
    ({"verifier": "claude"}, "verifier"),
    ({"ledger_edition": "2026-12-02"}, "ledger_edition"),
    ({"material": None}, "material"),
])
def test_an_invalid_record_is_refused(change, why):
    with pytest.raises(record.RecordError, match=why):
        record.committed(replace(BASE, **change))


def test_a_human_record_names_initials():
    out = record.committed(replace(BASE, verifier="human (PB)"))
    assert out["verifier"] == "human (PB)"
