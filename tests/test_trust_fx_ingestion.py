"""Trusted ingestion tests for the required FX pairs (#194, pilot 3).

Every test is network-free: the source adapter is exercised through an
injected download, and everything downstream replays a captured artifact.

The pilot's own subject is the *cutoff*. Spot FX has no settlement, so the
contracts carry no ``settlement.confirmed`` rule and an unfinished bar would
be accepted rather than refused — diverging from a v1 path that drops it on
every pre-cutoff run. The adapter therefore drops the same bar, judged by the
same shared guard, and records what it dropped.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import date, datetime, timezone
from decimal import Decimal

import pandas as pd
import pytest

from config import CURRENCY_TICKERS
from fetchers._settlement import FX_SESSION
from trust import QualityState, TemporaryDirectoryTrustRepository
from trust.domain import DatasetResultStatus, EligibilityScope, Finding, FindingSeverity
from trust.fx_ingestion import (
    DAILY_MOVE_QUARANTINE_THRESHOLD,
    FX_DATASET_KEYS,
    FX_PAIRS,
    FxArtifactReplay,
    FxProviderBar,
    FxShapeError,
    artifact_from_bars,
    bars_from_replay,
    contract_for,
    dual_write_fx,
    fetch_fx_artifact,
    ingest_fx_replays,
    last_settled_fx_session,
    legacy_currency_frame,
    parse_fx_candidates,
    reconcile_fx,
    ticker_for,
    trusted_currency_frame,
)
from trust.registry import RawRetention

# 17:00 New York ends the FX bar Yahoo labels with that day's date; in August
# that is 21:00 UTC. 2026-08-13 is a Thursday.
BEFORE_FX_CLOSE = datetime(2026, 8, 13, 19, 0, tzinfo=timezone.utc)
AFTER_FX_CLOSE = datetime(2026, 8, 13, 22, 0, tzinfo=timezone.utc)
SESSION = date(2026, 8, 12)


def bar(
    pair: str = "BRL/USD",
    *,
    day: date = SESSION,
    close: str = "0.1840",
    open_: str | None = None,
    high: str | None = None,
    low: str | None = None,
) -> FxProviderBar:
    """A plausible candle around ``close`` unless a test states otherwise."""
    value = Decimal(close)
    step = value / Decimal("100")
    return FxProviderBar(
        pair=pair,
        session_date=day,
        close=value,
        open=Decimal(open_) if open_ is not None else value - step,
        high=Decimal(high) if high is not None else value + step,
        low=Decimal(low) if low is not None else value - step - step,
    )


def repository(tmp_path):
    return TemporaryDirectoryTrustRepository(tmp_path)


def _replaced_content(replay: FxArtifactReplay, content: bytes) -> FxArtifactReplay:
    """The same capture over different bytes, so shape drift can be exercised."""
    reference = replace(
        replay.artifact.reference,
        content_hash=hashlib.sha256(content).hexdigest(),
    )
    artifact = replace(replay.artifact, reference=reference, byte_size=len(content))
    return FxArtifactReplay(pair=replay.pair, artifact=artifact, content=content)


def provider_frame(rows: list[tuple[date, str]]) -> pd.DataFrame:
    """The shape yfinance returns for one FX ticker."""
    frame = pd.DataFrame(
        [
            {
                "Open": float(Decimal(close)) - 0.001,
                "High": float(Decimal(close)) + 0.001,
                "Low": float(Decimal(close)) - 0.002,
                "Close": float(Decimal(close)),
                "Volume": 0,
            }
            for _, close in rows
        ],
        index=pd.DatetimeIndex([pd.Timestamp(day) for day, _ in rows], name="Date"),
    )
    return frame


# ---------------------------------------------------------------------------
# Artifact capture and replay
# ---------------------------------------------------------------------------


def test_source_adapter_captures_an_artifact_before_parsing_and_needs_no_network() -> None:
    asked: list[str] = []

    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        asked.append(ticker)
        return provider_frame([(SESSION, "0.1840")])

    replay = fetch_fx_artifact("BRL/USD", download=download, retrieved_at=AFTER_FX_CLOSE)

    assert asked == ["BRLUSD=X"]
    assert replay.pair == "BRL/USD"
    assert bars_from_replay(replay) == (bar(close="0.184", open_="0.183", high="0.185", low="0.182"),)


def test_raw_artifact_is_metadata_only_and_retains_no_provider_bytes() -> None:
    replay = artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE)

    assert contract_for("BRL/USD").raw_retention is RawRetention.METADATA_ONLY
    assert replay.artifact.content is None
    assert replay.artifact.reference.content_retained is False
    assert replay.artifact.byte_size == len(replay.content)


def test_replay_content_that_does_not_match_its_hash_is_refused() -> None:
    replay = artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE)

    with pytest.raises(ValueError, match="replay content"):
        type(replay)(pair=replay.pair, artifact=replay.artifact, content=b"{}")


def test_the_cutover_names_are_the_registrys_own_dataset_keys() -> None:
    assert tuple(contract_for(pair).dataset.key for pair in FX_PAIRS) == FX_DATASET_KEYS


def test_every_required_pair_has_a_ticker_v1_actually_fetches() -> None:
    assert [ticker_for(pair) for pair in FX_PAIRS] == [CURRENCY_TICKERS[pair] for pair in FX_PAIRS]


def test_a_pair_the_registry_does_not_contract_is_refused() -> None:
    with pytest.raises(KeyError):
        artifact_from_bars("ARS/USD", [bar(pair="ARS/USD")], retrieved_at=AFTER_FX_CLOSE)


def test_malformed_bars_raise_rather_than_being_dropped() -> None:
    replay = artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE)
    payload = json.loads(replay.content.decode("utf-8"))
    payload["bars"][0]["close"] = "not-a-number"

    with pytest.raises(FxShapeError):
        FxProviderBar.from_dict(payload["bars"][0])
    with pytest.raises(FxShapeError, match="not a JSON object"):
        bars_from_replay(_replaced_content(replay, b"[]"))


# ---------------------------------------------------------------------------
# The cutoff — the thing this pilot exists to prove
# ---------------------------------------------------------------------------


def test_the_cutoff_is_the_shared_guards_and_not_a_second_constant() -> None:
    for now in (BEFORE_FX_CLOSE, AFTER_FX_CLOSE):
        assert last_settled_fx_session(now) == FX_SESSION.last_settled_session(now)

    assert last_settled_fx_session(BEFORE_FX_CLOSE) == date(2026, 8, 12)
    assert last_settled_fx_session(AFTER_FX_CLOSE) == date(2026, 8, 13)


def test_an_unfinished_fx_bar_is_dropped_with_a_durable_finding(tmp_path) -> None:
    replay = artifact_from_bars(
        "BRL/USD",
        [bar(day=date(2026, 8, 12)), bar(day=date(2026, 8, 13), close="0.1850")],
        retrieved_at=BEFORE_FX_CLOSE,
    )

    parsed = parse_fx_candidates(replay, now=BEFORE_FX_CLOSE)
    assert [candidate.identity.effective_date for candidate in parsed.candidates] == [date(2026, 8, 12)]
    assert parsed.unfinished_sessions == (date(2026, 8, 13),)

    store = repository(tmp_path)
    ingestion = ingest_fx_replays(store, [replay], ingested_at=BEFORE_FX_CLOSE)
    sessions = {
        revision.identity.effective_date for revision in store.all_observation_revisions()
    }
    assert sessions == {date(2026, 8, 12)}
    dataset = ingestion.dataset("BRL/USD")
    findings = [store.read(Finding, finding_id) for finding_id in dataset.finding_ids]
    unfinished = [
        finding for finding in findings if finding is not None and finding.rule_id == "fx.session-unfinished"
    ]
    assert len(unfinished) == 1
    assert unfinished[0].severity is FindingSeverity.WARNING
    assert unfinished[0].evidence["session_date"] == "2026-08-13"
    assert ingestion.dataset("BRL/USD").unfinished_sessions == (date(2026, 8, 13),)


def test_the_same_bar_is_ingested_once_its_session_has_closed(tmp_path) -> None:
    replay = artifact_from_bars(
        "BRL/USD",
        [bar(day=date(2026, 8, 13), close="0.1850")],
        retrieved_at=AFTER_FX_CLOSE,
    )
    store = repository(tmp_path)
    ingest_fx_replays(store, [replay], ingested_at=AFTER_FX_CLOSE)

    revisions = list(store.all_observation_revisions())
    assert [revision.identity.effective_date for revision in revisions] == [date(2026, 8, 13)]
    assert revisions[0].quality_state is QualityState.ACCEPTED


# ---------------------------------------------------------------------------
# Identity and ingestion
# ---------------------------------------------------------------------------


def test_candidates_carry_the_pair_its_orientation_and_the_market_date() -> None:
    replay = artifact_from_bars("INR/USD", [bar(pair="INR/USD", close="0.0115")], retrieved_at=AFTER_FX_CLOSE)

    candidate = parse_fx_candidates(replay, now=AFTER_FX_CLOSE).candidates[0]
    identity = candidate.identity

    assert identity.commodity == "foreign-exchange"
    assert identity.product_form == "inr-usd"
    assert identity.venue == "yahoo-finance"
    assert identity.price_type == "market-close"
    assert identity.currency == "USD"
    assert identity.unit == "usd-per-inr"
    assert identity.fx_pair is not None and identity.fx_pair.pair == "INR/USD"
    assert identity.effective_date == SESSION
    assert candidate.value == Decimal("0.0115")
    assert identity.source_record_id == "INRUSD=X"


def test_each_pair_is_its_own_dataset_not_one_fx_table(tmp_path) -> None:
    store = repository(tmp_path)
    replays = [
        artifact_from_bars(pair, [bar(pair=pair, close=close)], retrieved_at=AFTER_FX_CLOSE)
        for pair, close in (("BRL/USD", "0.1840"), ("ZAR/USD", "0.0560"))
    ]
    ingestion = ingest_fx_replays(store, replays, ingested_at=AFTER_FX_CLOSE)

    dataset_ids = {item.dataset_id for item in ingestion.datasets}
    assert len(dataset_ids) == 2
    assert ingestion.dataset("ZAR/USD").accepted_revision_ids


def test_repeated_ingestion_of_the_same_artifact_adds_no_revisions(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE)

    ingest_fx_replays(store, [replay], ingested_at=AFTER_FX_CLOSE)
    before = len(list(store.all_observation_revisions()))
    ingest_fx_replays(store, [replay], ingested_at=AFTER_FX_CLOSE)

    assert len(list(store.all_observation_revisions())) == before


def test_revisions_stay_fail_closed_for_public_display_while_yahoo_rights_are_unknown(tmp_path) -> None:
    store = repository(tmp_path)
    ingest_fx_replays(
        store,
        [artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE)],
        ingested_at=AFTER_FX_CLOSE,
    )

    assert all(revision.public_eligible is False for revision in store.all_observation_revisions())
    # Readable for reconciliation, unreadable for anything published.
    assert not trusted_currency_frame(store, "BRL/USD", scope=EligibilityScope.INTERNAL).empty
    assert trusted_currency_frame(store, "BRL/USD", scope=EligibilityScope.PUBLIC).empty


def test_source_shape_drift_produces_a_typed_finding_and_no_accepted_revisions(tmp_path) -> None:
    store = repository(tmp_path)
    drifted = _replaced_content(
        artifact_from_bars("BRL/USD", [bar()], retrieved_at=AFTER_FX_CLOSE),
        b'{"pair":"BRL/USD","renamed":[]}',
    )

    ingestion = ingest_fx_replays(store, [drifted], ingested_at=AFTER_FX_CLOSE)

    dataset = ingestion.dataset("BRL/USD")
    assert dataset.accepted_revision_ids == ()
    assert dataset.result.status is DatasetResultStatus.EXTERNAL_FAILURE
    assert store.all_observation_revisions() == ()
    findings = [store.read(Finding, finding_id) for finding_id in dataset.finding_ids]
    assert [finding.rule_id for finding in findings if finding is not None] == ["fx.shape"]


# ---------------------------------------------------------------------------
# Quality rules
# ---------------------------------------------------------------------------


def test_an_impossible_candle_is_rejected_rather_than_stored_as_a_rate(tmp_path) -> None:
    store = repository(tmp_path)
    replay = artifact_from_bars(
        "BRL/USD",
        [bar(close="0.1840", high="0.1800", low="0.1700")],  # high below the close
        retrieved_at=AFTER_FX_CLOSE,
    )
    ingest_fx_replays(store, [replay], ingested_at=AFTER_FX_CLOSE)

    revisions = list(store.all_observation_revisions())
    assert [revision.quality_state for revision in revisions] == [QualityState.REJECTED]
    assert trusted_currency_frame(store, "BRL/USD").empty


def test_an_inverted_rate_quarantines_instead_of_landing_as_a_conversion_input(tmp_path) -> None:
    store = repository(tmp_path)
    # 5.43 is USD per BRL — the pair upside down. It parses, it is finite and
    # positive, and it would silently multiply every Brazilian landed cost by
    # thirty.
    replay = artifact_from_bars(
        "BRL/USD",
        [bar(close="5.4300", open_="5.4200", high="5.4400", low="5.4100")],
        retrieved_at=AFTER_FX_CLOSE,
    )
    ingest_fx_replays(store, [replay], ingested_at=AFTER_FX_CLOSE)

    revisions = list(store.all_observation_revisions())
    assert [revision.quality_state for revision in revisions] == [QualityState.QUARANTINED]
    assert trusted_currency_frame(store, "BRL/USD").empty


def test_an_extreme_move_quarantines_and_leaves_the_prior_session_accepted(tmp_path) -> None:
    store = repository(tmp_path)
    ingest_fx_replays(
        store,
        [artifact_from_bars("BRL/USD", [bar(day=date(2026, 8, 11), close="0.1840")], retrieved_at=AFTER_FX_CLOSE)],
        ingested_at=AFTER_FX_CLOSE,
    )
    moved = Decimal("0.1840") * (1 + DAILY_MOVE_QUARANTINE_THRESHOLD + Decimal("0.02"))
    ingest_fx_replays(
        store,
        [
            artifact_from_bars(
                "BRL/USD",
                [bar(day=date(2026, 8, 12), close=str(moved))],
                retrieved_at=AFTER_FX_CLOSE,
            )
        ],
        # A later run: two collection cycles are two runs, and a repository
        # that let them share a run id would be losing one of them.
        ingested_at=AFTER_FX_CLOSE.replace(hour=23),
    )

    frame = trusted_currency_frame(store, "BRL/USD")
    assert list(frame["Date"]) == ["2026-08-11"]
    quarantined = [
        revision
        for revision in store.all_observation_revisions()
        if revision.quality_state is QualityState.QUARANTINED
    ]
    assert [revision.identity.effective_date for revision in quarantined] == [date(2026, 8, 12)]


# ---------------------------------------------------------------------------
# Reconciliation against v1
# ---------------------------------------------------------------------------


def test_the_legacy_frame_is_v1s_own_parse_of_the_same_bars() -> None:
    frame = legacy_currency_frame("BRL/USD", provider_frame([(SESSION, "0.1840")]))

    assert list(frame.columns) == ["pair", "Date", "Open", "High", "Low", "Close"]
    assert frame.iloc[0]["pair"] == "BRL/USD"
    assert frame.iloc[0]["Date"] == "2026-08-12"


def test_dual_write_reconciles_with_the_v1_currency_frame(tmp_path) -> None:
    """One provider frame, both parses — the only comparison worth making."""
    store = repository(tmp_path)
    frame = provider_frame([(SESSION, "0.184")])
    replay = fetch_fx_artifact("BRL/USD", download=lambda ticker, period="5d": frame, retrieved_at=AFTER_FX_CLOSE)

    result = dual_write_fx(
        store,
        [replay],
        {"BRL/USD": legacy_currency_frame("BRL/USD", frame)},
        ingested_at=AFTER_FX_CLOSE,
    )

    assert result.reconciled
    assert result.reconciliations["BRL/USD"].matched_rows == 1


def test_reconciliation_names_every_missing_row_and_differing_field(tmp_path) -> None:
    store = repository(tmp_path)
    trusted_frame = provider_frame([(SESSION, "0.1840")])
    ingest_fx_replays(
        store,
        [
            fetch_fx_artifact(
                "BRL/USD",
                download=lambda ticker, period="5d": trusted_frame,
                retrieved_at=AFTER_FX_CLOSE,
            )
        ],
        ingested_at=AFTER_FX_CLOSE,
    )
    legacy = legacy_currency_frame("BRL/USD", provider_frame([(date(2026, 8, 11), "0.1830"), (SESSION, "0.1855")]))

    report = reconcile_fx(legacy, trusted_currency_frame(store, "BRL/USD"))

    assert not report.reconciled
    assert [row["Date"] for row in report.missing_in_trusted] == ["2026-08-11"]
    assert sorted(diff["field"] for diff in report.field_differences) == ["Close", "High", "Low", "Open"]
