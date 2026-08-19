"""Trusted ingestion tests for the CBOT named soy benchmark contracts (DT-16).

Every test is network-free: the source adapter is exercised through an injected
download, and everything downstream replays a captured artifact.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from trust import QualityState, TemporaryDirectoryTrustRepository
from trust.cbot_benchmarks import (
    DAILY_MOVE_QUARANTINE_THRESHOLD,
    BenchmarkShapeError,
    ProviderBar,
    append_benchmark_correction,
    artifact_from_bars,
    contract_for,
    dual_write_cbot_benchmarks,
    fetch_cbot_benchmark_artifact,
    ingest_cbot_benchmark_replays,
    parse_benchmark_candidates,
    reconcile_cbot_benchmarks,
    settlement_state_for,
    trusted_curve_frame,
)
from trust.domain import DatasetResultStatus, EligibilityScope, SettlementState
from trust.registry import RawRetention, RightsAction, RightsDecision

# 20:00 UTC is 15:00 in Chicago — past the 14:30 settlement cutoff, so a bar
# dated the same day is a finished session.
AFTER_SETTLEMENT = datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc)
BEFORE_SETTLEMENT = datetime(2026, 8, 13, 15, 0, tzinfo=timezone.utc)
SESSION = date(2026, 8, 12)


def bar(
    symbol: str,
    *,
    day: date = SESSION,
    close: str = "1150.25",
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
    volume: str | None = "9000",
) -> ProviderBar:
    """A plausible candle around ``close`` unless a test states otherwise.

    Derived rather than defaulted to fixed numbers: a shared default high
    silently makes a higher ``close`` an impossible candle, and a test meaning
    to exercise something else would then be exercising the OHLC rule.
    """
    value = Decimal(close)
    return ProviderBar(
        symbol=symbol,
        session_date=day,
        close=value,
        open=Decimal(open_) if open_ is not None else value - Decimal("5"),
        high=Decimal(high) if high is not None else value + Decimal("5"),
        low=Decimal(low) if low is not None else value - Decimal("8"),
        volume=None if volume is None else Decimal(volume),
    )


def repository(tmp_path):
    return TemporaryDirectoryTrustRepository(tmp_path)


def legacy_frame(rows: list[dict]) -> pd.DataFrame:
    """The shape ``fetchers.forward_curve.fetch_forward_curve`` returns."""
    return pd.DataFrame(
        rows,
        columns=[
            "commodity",
            "contract_month",
            "label",
            "ticker",
            "close",
            "observation_date",
            "volume",
            "open_interest",
        ],
    )


# ---------------------------------------------------------------------------
# Artifact capture and replay
# ---------------------------------------------------------------------------


def test_source_adapter_captures_an_artifact_before_parsing_and_needs_no_network() -> None:
    asked: list[str] = []

    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        asked.append(ticker)
        return pd.DataFrame(
            {"Open": [1145.0], "High": [1155.0], "Low": [1142.0], "Close": [1150.25], "Volume": [9000.0]},
            index=pd.DatetimeIndex([pd.Timestamp("2026-08-12")]),
        )

    replay = fetch_cbot_benchmark_artifact(
        "Soybeans", today=date(2026, 8, 13), download=download, retrieved_at=AFTER_SETTLEMENT
    )

    # Every leg the v1 fetcher would ask for, and nothing else.
    assert asked == ["ZSQ26.CBT", "ZSU26.CBT", "ZSX26.CBT", "ZSF27.CBT", "ZSH27.CBT", "ZSK27.CBT"]
    assert replay.artifact.response_status == 200
    assert replay.artifact.byte_size == len(replay.content)
    assert json.loads(replay.content)["commodity"] == "Soybeans"


def test_raw_artifact_is_metadata_only_and_retains_no_provider_bytes() -> None:
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)

    # The Yahoo rights position prohibits retaining raw content, so the
    # artifact carries a hash and a size and no payload at all.
    assert contract_for("Soybeans").raw_retention is RawRetention.METADATA_ONLY
    assert replay.artifact.content is None
    assert replay.artifact.reference.content_retained is False
    assert len(replay.artifact.reference.content_hash) == 64


def test_replay_content_that_does_not_match_its_hash_is_refused() -> None:
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)

    with pytest.raises(ValueError, match="does not match raw artifact hash"):
        type(replay)(commodity="Soybeans", artifact=replay.artifact, content=b"{}")


def test_a_delisted_or_unlisted_contract_is_absent_rather_than_priced() -> None:
    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        if ticker == "ZSQ26.CBT":
            return pd.DataFrame()  # expired and delisted by the provider
        return pd.DataFrame(
            {"Close": [1150.25]}, index=pd.DatetimeIndex([pd.Timestamp("2026-08-12")])
        )

    replay = fetch_cbot_benchmark_artifact(
        "Soybeans", today=date(2026, 8, 13), download=download, retrieved_at=AFTER_SETTLEMENT
    )

    symbols = {row["symbol"] for row in json.loads(replay.content)["bars"]}
    assert "ZSQ26.CBT" not in symbols
    assert "ZSU26.CBT" in symbols


# ---------------------------------------------------------------------------
# Candidate identity
# ---------------------------------------------------------------------------


def test_candidates_identify_exchange_contract_unit_and_market_date() -> None:
    replay = artifact_from_bars("Soybeans", [bar("ZSX26.CBT")], retrieved_at=AFTER_SETTLEMENT)

    parsed = parse_benchmark_candidates(replay, now=AFTER_SETTLEMENT)

    (candidate,) = parsed.candidates
    identity = candidate.identity
    assert identity.commodity == "soybean"
    assert identity.product_form == "beans"
    assert identity.venue == "cbot"
    assert identity.unit == "cents-bu"
    assert identity.currency == "USD"
    assert identity.effective_date == SESSION
    assert identity.contract is not None
    assert (identity.contract.exchange, identity.contract.code, identity.contract.delivery_month) == (
        "cbot",
        "ZSX26",
        "2026-11",
    )
    assert identity.source_record_id == "ZSX26.CBT"
    assert candidate.value == Decimal("1150.25")
    assert candidate.high_value == Decimal("1155.25")


def test_two_delivery_months_are_two_observations_not_two_revisions_of_one() -> None:
    replay = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT"), bar("ZSX26.CBT", close="1162.00")], retrieved_at=AFTER_SETTLEMENT
    )

    parsed = parse_benchmark_candidates(replay, now=AFTER_SETTLEMENT)

    observation_ids = {candidate.identity.observation_id for candidate in parsed.candidates}
    assert len(observation_ids) == 2


def test_meal_and_oil_carry_their_own_commodity_product_form_and_native_unit() -> None:
    meal = parse_benchmark_candidates(
        artifact_from_bars("Soybean Meal", [bar("ZMZ26.CBT", close="298.40")], retrieved_at=AFTER_SETTLEMENT),
        now=AFTER_SETTLEMENT,
    ).candidates[0]
    oil = parse_benchmark_candidates(
        artifact_from_bars(
            "Soybean Oil",
            [bar("ZLZ26.CBT", close="45.10", open_="44.80", high="45.60", low="44.70")],
            retrieved_at=AFTER_SETTLEMENT,
        ),
        now=AFTER_SETTLEMENT,
    ).candidates[0]

    assert (meal.identity.commodity, meal.identity.product_form, meal.identity.unit) == (
        "soybean-meal",
        "meal",
        "usd-short-ton",
    )
    assert (oil.identity.commodity, oil.identity.product_form, oil.identity.unit) == (
        "soybean-oil",
        "oil",
        "cents-lb",
    )


# ---------------------------------------------------------------------------
# Settlement state
# ---------------------------------------------------------------------------


def test_a_past_session_is_settled_and_the_session_in_progress_is_not() -> None:
    assert settlement_state_for(date(2026, 8, 12), BEFORE_SETTLEMENT) is SettlementState.SETTLED
    assert settlement_state_for(date(2026, 8, 13), BEFORE_SETTLEMENT) is SettlementState.OPEN
    assert settlement_state_for(date(2026, 8, 13), AFTER_SETTLEMENT) is SettlementState.SETTLED
    assert settlement_state_for(date(2026, 8, 14), AFTER_SETTLEMENT) is SettlementState.PRE_OPEN


def test_an_unfinished_session_bar_cannot_become_an_accepted_revision(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", day=date(2026, 8, 13))], retrieved_at=BEFORE_SETTLEMENT
    )

    ingestion = ingest_cbot_benchmark_replays(store, [replay], ingested_at=BEFORE_SETTLEMENT)

    dataset = ingestion.dataset("Soybeans")
    assert dataset.accepted_revision_ids == ()
    assert len(dataset.rejected_revision_ids) == 1
    assert dataset.result.status is DatasetResultStatus.CONTRACT_FAILURE
    # The refused bar is durable, not discarded: it is in the ledger, labelled.
    (stored,) = store.all_observation_revisions()
    assert stored.quality_state is QualityState.REJECTED
    assert stored.settlement_state is SettlementState.OPEN
    assert stored.finding_ids


# ---------------------------------------------------------------------------
# Ingestion, idempotence, refusals
# ---------------------------------------------------------------------------


def test_replayed_artifact_ingests_as_accepted_revisions_with_full_provenance(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT"), bar("ZSX26.CBT", close="1162.00")], retrieved_at=AFTER_SETTLEMENT
    )

    ingestion = ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)

    dataset = ingestion.dataset("Soybeans")
    assert dataset.result.status is DatasetResultStatus.SUCCESS
    assert dataset.result.coverage == Decimal("1")
    assert len(dataset.accepted_revision_ids) == 2
    for revision in store.all_observation_revisions():
        assert revision.quality_state is QualityState.ACCEPTED
        assert revision.artifact is not None
        assert revision.artifact.artifact_id == dataset.artifact_id
        assert revision.parser_version
        assert revision.settlement_state is SettlementState.SETTLED


def test_repeated_ingestion_of_the_same_artifact_adds_no_revisions(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)

    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)
    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)

    assert len(store.all_observation_revisions()) == 1


def test_an_impossible_candle_is_rejected_rather_than_stored_as_a_price(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans",
        [bar("ZSU26.CBT", close="1150.25", open_="1145.00", high="1100.00", low="1142.00")],
        retrieved_at=AFTER_SETTLEMENT,
    )

    ingestion = ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)

    dataset = ingestion.dataset("Soybeans")
    assert dataset.accepted_revision_ids == ()
    assert len(dataset.rejected_revision_ids) == 1
    assert trusted_curve_frame(store, "Soybeans").empty


def test_a_symbol_from_another_product_is_refused_with_a_finding_not_carried(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans", [bar("ZLZ26.CBT", close="45.10")], retrieved_at=AFTER_SETTLEMENT
    )

    parsed = parse_benchmark_candidates(replay, now=AFTER_SETTLEMENT)
    ingestion = ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)

    assert parsed.candidates == ()
    assert len(parsed.rejected_rows) == 1
    assert ingestion.dataset("Soybeans").finding_ids
    assert store.all_observation_revisions() == ()


def test_source_shape_drift_produces_a_typed_finding_and_no_accepted_revisions(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)
    drifted = type(replay)(
        commodity="Soybeans",
        artifact=_artifact_for(b'{"commodity":"Soybeans","renamed":[]}'),
        content=b'{"commodity":"Soybeans","renamed":[]}',
    )

    ingestion = ingest_cbot_benchmark_replays(store, [drifted], ingested_at=AFTER_SETTLEMENT)

    dataset = ingestion.dataset("Soybeans")
    assert dataset.accepted_revision_ids == ()
    assert dataset.result.status is DatasetResultStatus.EXTERNAL_FAILURE
    assert store.all_observation_revisions() == ()


def _artifact_for(content: bytes):
    """A metadata-only artifact whose hash matches arbitrary replay bytes."""
    import hashlib

    from trust.domain import ArtifactReference, RawArtifact, Timestamp

    contract = contract_for("Soybeans")
    reference = ArtifactReference(
        source_id=contract.dataset.source_id,
        dataset_id=contract.dataset.dataset_id,
        dataset_key=contract.dataset.key,
        content_hash=hashlib.sha256(content).hexdigest(),
        content_retained=False,
        media_type="application/json",
    )
    return RawArtifact(
        reference=reference,
        retrieval_url="yfinance:chart/soybeans?period=5d",
        retrieved_at=Timestamp(AFTER_SETTLEMENT),
        response_status=200,
        byte_size=len(content),
    )


def test_malformed_bars_raise_rather_than_being_dropped() -> None:
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)
    broken = json.dumps(
        {"commodity": "Soybeans", "provider": "yahoo-finance", "bars": [{"symbol": "ZSU26.CBT"}]},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()

    with pytest.raises(BenchmarkShapeError):
        parse_benchmark_candidates(
            type(replay)(commodity="Soybeans", artifact=_artifact_for(broken), content=broken)
        )


# ---------------------------------------------------------------------------
# Quarantine must not overwrite accepted history
# ---------------------------------------------------------------------------


def test_an_extreme_move_quarantines_and_leaves_the_prior_session_accepted(tmp_path) -> None:
    store = repository(tmp_path)
    day_one = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", day=SESSION, close="1150.00")], retrieved_at=AFTER_SETTLEMENT
    )
    later = datetime(2026, 8, 14, 20, 0, tzinfo=timezone.utc)
    day_two = artifact_from_bars(
        "Soybeans",
        [bar("ZSU26.CBT", day=date(2026, 8, 13), close="1650.00", open_="1645", high="1655", low="1642")],
        retrieved_at=later,
    )

    ingest_cbot_benchmark_replays(store, [day_one], ingested_at=AFTER_SETTLEMENT)
    ingestion = ingest_cbot_benchmark_replays(store, [day_two], ingested_at=later)

    dataset = ingestion.dataset("Soybeans")
    assert len(dataset.quarantined_revision_ids) == 1
    assert dataset.accepted_revision_ids == ()
    # The accepted history is untouched: the trusted curve still reads the last
    # session anyone vouched for, not the 43% move nobody has reviewed.
    frame = trusted_curve_frame(store, "Soybeans")
    assert list(frame["close"]) == [1150.0]
    assert list(frame["observation_date"]) == ["2026-08-12"]
    assert Decimal("0.43") > DAILY_MOVE_QUARANTINE_THRESHOLD


def test_a_quarantined_revision_of_the_same_session_does_not_displace_the_accepted_one(tmp_path) -> None:
    store = repository(tmp_path)
    prior = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", day=date(2026, 8, 11), close="1150.00")], retrieved_at=AFTER_SETTLEMENT
    )
    good = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", day=SESSION, close="1155.00")], retrieved_at=AFTER_SETTLEMENT
    )
    later = datetime(2026, 8, 13, 21, 0, tzinfo=timezone.utc)
    bad = artifact_from_bars(
        "Soybeans",
        [bar("ZSU26.CBT", day=SESSION, close="1800.00", open_="1795", high="1805", low="1790")],
        retrieved_at=later,
    )

    ingest_cbot_benchmark_replays(store, [prior], ingested_at=AFTER_SETTLEMENT)
    ingest_cbot_benchmark_replays(
        store, [good], ingested_at=datetime(2026, 8, 13, 20, 30, tzinfo=timezone.utc)
    )
    ingest_cbot_benchmark_replays(store, [bad], ingested_at=later)

    frame = trusted_curve_frame(store, "Soybeans")
    assert list(frame["close"]) == [1155.0]
    states = sorted(revision.quality_state.value for revision in store.all_observation_revisions())
    assert states == ["accepted", "accepted", "quarantined"]


# ---------------------------------------------------------------------------
# Corrections
# ---------------------------------------------------------------------------


def test_a_correction_appends_a_revision_and_preserves_the_one_it_replaces(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", close="1150.00")], retrieved_at=AFTER_SETTLEMENT
    )
    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)
    (original,) = store.all_observation_revisions()

    corrected = append_benchmark_correction(
        store,
        original.identity,
        corrected_value=Decimal("1151.25"),
        reason="exchange republished the session close",
        corrected_at=datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc),
    )

    assert corrected.supersedes_revision_id == original.revision_id
    assert corrected.correction_reason
    # Both revisions remain in the ledger; only one is current.
    revisions = store.observation_revisions(original.identity)
    assert {item.revision_id for item in revisions} == {original.revision_id, corrected.revision_id}
    head = store.current_accepted_revision(original.identity, scope=EligibilityScope.INTERNAL)
    assert head is not None and head.revision_id == corrected.revision_id
    assert list(trusted_curve_frame(store, "Soybeans")["close"]) == [1151.25]


def test_a_correction_is_reproducible_at_the_instant_before_it_was_made(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", close="1150.00")], retrieved_at=AFTER_SETTLEMENT
    )
    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)
    (original,) = store.all_observation_revisions()
    append_benchmark_correction(
        store,
        original.identity,
        corrected_value=Decimal("1151.25"),
        reason="exchange republished the session close",
        corrected_at=datetime(2026, 8, 14, 12, 0, tzinfo=timezone.utc),
    )

    before = store.revision_effective_at(
        original.identity,
        datetime(2026, 8, 14, 11, 0, tzinfo=timezone.utc),
        scope=EligibilityScope.INTERNAL,
    )

    assert before is not None and before.value == Decimal("1150.00")


# ---------------------------------------------------------------------------
# Reconciliation against v1
# ---------------------------------------------------------------------------


def test_dual_write_reconciles_with_the_v1_forward_curve_frame(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans",
        [bar("ZSU26.CBT", close="1150.25"), bar("ZSX26.CBT", close="1162.00")],
        retrieved_at=AFTER_SETTLEMENT,
    )
    legacy = legacy_frame(
        [
            {
                "commodity": "Soybeans",
                "contract_month": "2026-09-01",
                "label": "Sep 2026",
                "ticker": "ZSU26.CBT",
                "close": 1150.25,
                "observation_date": "2026-08-12",
                "volume": 9000.0,
                "open_interest": None,
            },
            {
                "commodity": "Soybeans",
                "contract_month": "2026-11-01",
                "label": "Nov 2026",
                "ticker": "ZSX26.CBT",
                "close": 1162.00,
                "observation_date": "2026-08-12",
                "volume": 9000.0,
                "open_interest": None,
            },
        ]
    )

    result = dual_write_cbot_benchmarks(
        store, [replay], {"Soybeans": legacy}, ingested_at=AFTER_SETTLEMENT
    )

    report = result.reconciliations["Soybeans"]
    assert result.reconciled
    assert report.matched_rows == 2
    assert report.field_differences == ()


def test_reconciliation_names_every_missing_row_and_differing_field(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "Soybeans",
        [bar("ZSU26.CBT", close="1150.25"), bar("ZSX26.CBT", close="1162.00")],
        retrieved_at=AFTER_SETTLEMENT,
    )
    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)
    legacy = legacy_frame(
        [
            {
                "commodity": "Soybeans",
                "contract_month": "2026-09-01",
                "label": "Sep 2026",
                "ticker": "ZSU26.CBT",
                "close": 1149.00,  # v1 disagrees on the price
                "observation_date": "2026-08-12",
                "volume": 9000.0,
                "open_interest": None,
            },
            {
                "commodity": "Soybeans",
                "contract_month": "2027-01-01",
                "label": "Jan 2027",
                "ticker": "ZSF27.CBT",
                "close": 1175.00,  # a leg the trusted path never saw
                "observation_date": "2026-08-12",
                "volume": 10.0,
                "open_interest": None,
            },
        ]
    )

    report = reconcile_cbot_benchmarks(legacy, trusted_curve_frame(store, "Soybeans"))

    assert not report.reconciled
    assert [row["ticker"] for row in report.missing_in_trusted] == ["ZSF27.CBT"]
    assert [row["ticker"] for row in report.missing_in_legacy] == ["ZSX26.CBT"]
    assert [(diff["field"], diff["legacy"], diff["trusted"]) for diff in report.field_differences] == [
        ("close", 1149.0, 1150.25)
    ]


# ---------------------------------------------------------------------------
# Rights and eligibility
# ---------------------------------------------------------------------------


def test_revisions_stay_fail_closed_for_public_display_while_yahoo_rights_are_unknown(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars("Soybeans", [bar("ZSU26.CBT")], retrieved_at=AFTER_SETTLEMENT)
    ingest_cbot_benchmark_replays(store, [replay], ingested_at=AFTER_SETTLEMENT)
    (revision,) = store.all_observation_revisions()

    rights = contract_for("Soybeans").rights
    assert rights is not None
    assert rights.decision(RightsAction.PUBLIC_DISPLAY) is RightsDecision.UNKNOWN
    assert revision.public_eligible is False
    # Fail-closed for anything published; readable by an internal consumer,
    # which is the right the registry does record.
    assert store.current_accepted_revision(revision.identity) is None
    assert store.current_accepted_revision(revision.identity, scope=EligibilityScope.INTERNAL) is not None
    assert trusted_curve_frame(store, "Soybeans", scope=EligibilityScope.PUBLIC).empty


def test_the_trusted_frame_returns_one_session_not_a_stitched_curve(tmp_path) -> None:
    store = repository(tmp_path)
    older = artifact_from_bars(
        "Soybeans",
        [bar("ZSX26.CBT", day=date(2026, 8, 11), close="1160.00")],
        retrieved_at=AFTER_SETTLEMENT,
    )
    newer = artifact_from_bars(
        "Soybeans", [bar("ZSU26.CBT", day=SESSION, close="1150.25")], retrieved_at=AFTER_SETTLEMENT
    )

    ingest_cbot_benchmark_replays(store, [older], ingested_at=AFTER_SETTLEMENT)
    ingest_cbot_benchmark_replays(
        store, [newer], ingested_at=datetime(2026, 8, 13, 20, 30, tzinfo=timezone.utc)
    )

    frame = trusted_curve_frame(store, "Soybeans")
    assert list(frame["observation_date"]) == ["2026-08-12"]
    assert list(frame["ticker"]) == ["ZSU26.CBT"]
