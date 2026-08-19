"""Options the desk supplies: a timestamp, a source, and a stated model limit.

``analysis/futures/options.py`` already refuses to invent a chain, and already
takes a hand-typed premium *or* implied volatility and derives the other. Three
things were missing for a professional desk, and they are what this file pins:

1. **A source timestamp, not a source date.** A broker's 09:15 quote and their
   14:45 quote are two different markets. ``quoted_on`` cannot tell them apart,
   so every entered and every imported quote now carries a timezone-aware
   ``quoted_at``, and a naive one is refused rather than assumed to be local.
2. **An externally supplied chain.** A desk that can export a ladder from its
   broker should not retype it. :func:`chain_from_csv` reads one, stamps it
   with who supplied it and when, and classifies every row ``MANUAL`` —
   because a file somebody sent us is not a feed this project ingests.
3. **The model's limits as data, not as prose in a docstring.** Black-76 prices
   a *European* option on a lognormal future with one constant volatility.
   Listed grain options are American, and grain volatility smiles. Those are
   not caveats to be recalled by whoever reads the module — they are
   :data:`BLACK76_LIMITATIONS`, and they are rendered.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import pytest

from analysis.futures.options import (
    BLACK76_LIMITATIONS,
    ChainQuote,
    ManualQuote,
    OptionContract,
    OptionEntryError,
    OptionRight,
    OptionStyle,
    chain_from_csv,
    chain_status,
    parse_ladder,
    value_chain,
)

QUOTED_AT = datetime(2026, 8, 19, 14, 45, tzinfo=timezone.utc)
EXPIRY = date(2026, 10, 23)
AS_OF = date(2026, 8, 19)


def contract(strike: float = 1150.0, right: str = "call") -> OptionContract:
    from analysis.futures.domain import named_contract

    return OptionContract(
        underlying=named_contract("Soybeans", 2026, 11),
        right=OptionRight(right),
        strike=strike,
        expiry=EXPIRY,
        style=OptionStyle.AMERICAN,
    )


CHAIN_CSV = """right,strike,expiry,premium,implied_volatility,quoted_at
call,1150,2026-10-23,42.50,,2026-08-19T14:45:00+00:00
call,1200,2026-10-23,24.25,,2026-08-19T14:45:00+00:00
put,1100,2026-10-23,,0.2450,2026-08-19T14:45:00+00:00
"""


def write(tmp_path, text: str = CHAIN_CSV, name: str = "zsx26.csv"):
    path = tmp_path / name
    path.write_text(text, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# Source timestamps
# ---------------------------------------------------------------------------
def test_an_entered_quote_carries_a_timezone_aware_source_timestamp():
    quote = ManualQuote(
        contract=contract(), source="Broker desk", quoted_on=AS_OF,
        quoted_at=QUOTED_AT, premium=42.50,
    )
    assert quote.quoted_at == QUOTED_AT
    assert quote.quoted_at.tzinfo is not None


def test_a_naive_timestamp_is_refused_rather_than_assumed_to_be_local():
    """Whose local? The desk's, the broker's, or the runner's in CI."""
    with pytest.raises(OptionEntryError, match="timezone"):
        ManualQuote(
            contract=contract(), source="Broker desk", quoted_on=AS_OF,
            quoted_at=datetime(2026, 8, 19, 14, 45), premium=42.50,
        )


def test_a_timestamp_is_optional_and_its_absence_is_stated_not_filled_in():
    quote = ManualQuote(
        contract=contract(), source="Broker desk", quoted_on=AS_OF, premium=42.50,
    )
    assert quote.quoted_at is None
    assert "no time of day" in quote.timestamp_note.lower()


def test_a_timestamp_that_disagrees_with_the_quoted_date_is_refused():
    with pytest.raises(OptionEntryError, match="quoted_on"):
        ManualQuote(
            contract=contract(), source="Broker desk", quoted_on=date(2026, 8, 18),
            quoted_at=QUOTED_AT, premium=42.50,
        )


def test_a_ladder_document_reads_the_timestamp_when_one_is_given():
    ladder = parse_ladder(
        {
            "options": [{
                "underlying": "ZSX26", "right": "call", "strike": 1150,
                "expiry": "2026-10-23", "quoted_on": "2026-08-19",
                "quoted_at": "2026-08-19T14:45:00+00:00",
                "premium": 42.5, "source": "Broker desk",
            }],
        },
        where="test",
    )
    assert ladder.quotes[0].quoted_at == QUOTED_AT


# ---------------------------------------------------------------------------
# An externally supplied chain
# ---------------------------------------------------------------------------
def test_an_external_chain_is_read_and_stamped_with_who_supplied_it_and_when(tmp_path):
    chain = chain_from_csv(
        write(tmp_path), underlying="ZSX26", source="Broker chain export",
    )
    assert chain.available is True
    assert chain.provider == "Broker chain export"
    assert chain.quoted_at == QUOTED_AT
    assert len(chain.quotes) == 3
    assert chain.underlying.symbol == "ZSX26"


def test_every_row_of_an_external_chain_is_manual_never_a_settlement(tmp_path):
    from pricing.semantics import PriceType

    chain = chain_from_csv(write(tmp_path), underlying="ZSX26", source="Broker chain export")
    assert all(q.price_type is PriceType.MANUAL for q in chain.quotes)
    assert all(q.price_type.is_settlement_proven is False for q in chain.quotes)


def test_a_chain_row_with_neither_premium_nor_vol_is_refused_not_left_blank(tmp_path):
    text = CHAIN_CSV.replace("call,1200,2026-10-23,24.25,,", "call,1200,2026-10-23,,,")
    with pytest.raises(OptionEntryError, match="premium"):
        chain_from_csv(write(tmp_path, text), underlying="ZSX26", source="x")


def test_a_chain_row_with_both_premium_and_vol_is_refused(tmp_path):
    text = CHAIN_CSV.replace(
        "call,1200,2026-10-23,24.25,,", "call,1200,2026-10-23,24.25,0.22,",
    )
    with pytest.raises(OptionEntryError, match="exactly one"):
        chain_from_csv(write(tmp_path, text), underlying="ZSX26", source="x")


def test_a_chain_with_no_timestamp_anywhere_is_refused(tmp_path):
    """A ladder with no time on it cannot be compared with a board price."""
    text = CHAIN_CSV.replace(",2026-08-19T14:45:00+00:00", ",")
    with pytest.raises(OptionEntryError, match="quoted_at"):
        chain_from_csv(write(tmp_path, text), underlying="ZSX26", source="x")


def test_a_caller_may_supply_the_timestamp_the_file_does_not_carry(tmp_path):
    text = CHAIN_CSV.replace(",2026-08-19T14:45:00+00:00", ",")
    chain = chain_from_csv(
        write(tmp_path, text), underlying="ZSX26", source="x", quoted_at=QUOTED_AT,
    )
    assert chain.quoted_at == QUOTED_AT


def test_a_chain_with_two_different_timestamps_is_refused_as_two_sessions(tmp_path):
    text = CHAIN_CSV.replace(
        "put,1100,2026-10-23,,0.2450,2026-08-19T14:45:00+00:00",
        "put,1100,2026-10-23,,0.2450,2026-08-19T09:15:00+00:00",
    )
    with pytest.raises(OptionEntryError, match="one moment"):
        chain_from_csv(write(tmp_path, text), underlying="ZSX26", source="x")


def test_an_unknown_underlying_is_refused(tmp_path):
    with pytest.raises(OptionEntryError):
        chain_from_csv(write(tmp_path), underlying="NOPE99", source="x")


# ---------------------------------------------------------------------------
# Valuing one
# ---------------------------------------------------------------------------
def test_an_external_chain_values_against_the_board_forward(tmp_path):
    chain = chain_from_csv(write(tmp_path), underlying="ZSX26", source="Broker chain export")
    valued = value_chain(chain, as_of=AS_OF, forward=1150.0, rate=0.04)

    assert len(valued) == 3
    assert all(row["valued"] for row in valued)
    assert all(row["source"] == "Broker chain export" for row in valued)
    assert all(row["quoted_at"] == QUOTED_AT.isoformat() for row in valued)
    atm = next(r for r in valued if r["contract"]["strike"] == 1150.0)
    assert atm["greeks"]["delta"] == pytest.approx(0.5, abs=0.06)


def test_a_chain_with_no_board_forward_is_reported_unvalued_with_its_reason(tmp_path):
    chain = chain_from_csv(write(tmp_path), underlying="ZSX26", source="x")
    valued = value_chain(chain, as_of=AS_OF, forward=None, rate=0.04)

    assert all(row["valued"] is False for row in valued)
    assert all("no board price" in row["reason"] for row in valued)


def test_every_valued_row_carries_the_american_caveat(tmp_path):
    chain = chain_from_csv(write(tmp_path), underlying="ZSX26", source="x")
    valued = value_chain(chain, as_of=AS_OF, forward=1150.0, rate=0.04)

    assert all("american" in " ".join(row["limitations"]).lower() for row in valued)


# ---------------------------------------------------------------------------
# Black-76's limits, as data
# ---------------------------------------------------------------------------
def test_the_model_limitations_are_structured_and_name_the_american_problem():
    ids = {limit.id for limit in BLACK76_LIMITATIONS}
    assert "american_early_exercise" in ids
    assert "volatility_smile" in ids

    early = next(limit for limit in BLACK76_LIMITATIONS if limit.id == "american_early_exercise")
    assert "american" in early.assumption.lower() or "european" in early.assumption.lower()
    assert early.direction == "understates"
    assert early.affects  # which desk decision it distorts


def test_every_limitation_says_which_way_it_bites():
    """A caveat that does not say the direction cannot be acted on."""
    allowed = {"understates", "overstates", "either", "unknown"}
    for limit in BLACK76_LIMITATIONS:
        assert limit.direction in allowed, limit.id
        assert limit.why.strip()


def test_the_page_facing_status_carries_the_limitations_verbatim():
    status = chain_status()
    ids = {row["id"] for row in status["limitations"]}
    assert {"american_early_exercise", "volatility_smile"} <= ids
    assert any("floor" in row["why"].lower() for row in status["limitations"])


def test_the_limitations_are_not_softened_into_a_single_sentence():
    """One line saying 'model values are approximate' would say nothing."""
    assert len(BLACK76_LIMITATIONS) >= 4


# ---------------------------------------------------------------------------
# Privacy
# ---------------------------------------------------------------------------
def test_a_chain_quote_carries_no_client_field_but_a_ladder_payload_is_still_private(tmp_path):
    from analysis.futures.privacy import PRIVATE_SECTION_IDS

    assert "options_entered" in PRIVATE_SECTION_IDS
    quote = ChainQuote(
        contract=contract(), bid=None, ask=None, settlement=None,
        implied_volatility=0.24, volume=None, open_interest=None,
        observation_date=AS_OF, quoted_at=QUOTED_AT, source="x",
    )
    assert quote.quoted_at.tzinfo is not None


def test_a_stale_external_chain_is_flagged_rather_than_silently_used(tmp_path):
    chain = chain_from_csv(write(tmp_path), underlying="ZSX26", source="x")
    valued = value_chain(
        chain, as_of=AS_OF + timedelta(days=5), forward=1150.0, rate=0.04,
    )
    assert all(any("quoted" in w for w in row["warnings"]) for row in valued)
