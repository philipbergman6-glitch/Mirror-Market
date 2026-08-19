"""The DT-16 read-path cutover: the trusted provider and the v1/v2 reconciler.

Two things are pinned here that the ingestion tests cannot see, because both
are about what a *consumer* gets.

The switch is the first: `open_provider` must return the v1 SQLite provider
until every soy dataset is named, and must fall back rather than fail when the
ledger is unreachable — a storage migration that can take the workstation down
is a worse trade than one that quietly stays on v1 and says so in the log.

The provider is the second: a quarantined leg is *absent* from the curve and a
corrected leg returns the correction. Those are the two answers that differ
from `forward_curve`, and they are the reason the cutover is worth making.

Every test is network-free.
"""

from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pandas as pd
import pytest

from analysis.futures.providers import SqliteQuoteProvider, open_provider
from analysis.futures.trusted_provider import (
    TRUSTED_LEDGER_YFINANCE,
    TrustedNamedContractProvider,
)
from trust import TemporaryDirectoryTrustRepository
from trust.cbot_benchmarks import (
    DAILY_MOVE_QUARANTINE_THRESHOLD,
    ProviderBar,
    append_benchmark_correction,
    artifact_from_bars,
    ingest_cbot_benchmark_replays,
)
from trust.read_path import CBOT_BENCHMARK_DATASET_KEYS, TRUSTED_READ_ENV_VAR

AFTER_SETTLEMENT = datetime(2026, 8, 13, 20, 0, tzinfo=timezone.utc)
SESSION = date(2026, 8, 12)
AS_OF = date(2026, 8, 13)


def bar(symbol: str, *, day: date = SESSION, close: str = "1150.25", volume: str = "9000") -> ProviderBar:
    value = Decimal(close)
    return ProviderBar(
        symbol=symbol,
        session_date=day,
        close=value,
        open=value - Decimal("5"),
        high=value + Decimal("5"),
        low=value - Decimal("8"),
        volume=Decimal(volume),
    )


def ingest(store, bars, *, at=AFTER_SETTLEMENT, commodity="Soybeans"):
    replay = artifact_from_bars(commodity, bars, retrieved_at=at)
    return ingest_cbot_benchmark_replays(store, [replay], ingested_at=at)


def trusted(tmp_path, bars, **kwargs):
    store = TemporaryDirectoryTrustRepository(tmp_path)
    ingestion = ingest(store, bars, **kwargs)
    return store, ingestion


def provider(store, conn: sqlite3.Connection) -> TrustedNamedContractProvider:
    return TrustedNamedContractProvider(repository=store, fallback=SqliteQuoteProvider(conn=conn))


@pytest.fixture()
def conn() -> sqlite3.Connection:
    """An empty v1 database — the fallback must never be the source of an answer."""
    connection = sqlite3.connect(":memory:")
    connection.execute(
        "CREATE TABLE forward_curve ("
        "commodity TEXT, contract_month TEXT, label TEXT, ticker TEXT, close REAL,"
        " observation_date TEXT, fetched_date TEXT, volume REAL, open_interest REAL)"
    )
    connection.commit()
    try:
        yield connection
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# The switch
# ---------------------------------------------------------------------------


def test_the_default_read_path_is_still_v1(conn, monkeypatch) -> None:
    monkeypatch.delenv(TRUSTED_READ_ENV_VAR, raising=False)

    assert isinstance(open_provider(conn), SqliteQuoteProvider)


def test_a_partial_cutover_does_not_move_the_curve_read(conn, monkeypatch) -> None:
    # Beans alone would put a trusted bean beside a v1 oil inside the crush.
    monkeypatch.setenv(TRUSTED_READ_ENV_VAR, "cbot-soybean-named-contracts")

    assert isinstance(open_provider(conn), SqliteQuoteProvider)


def test_naming_every_soy_dataset_moves_the_read_to_the_ledger(conn, tmp_path, monkeypatch) -> None:
    monkeypatch.setenv(TRUSTED_READ_ENV_VAR, ",".join(CBOT_BENCHMARK_DATASET_KEYS))
    store = TemporaryDirectoryTrustRepository(tmp_path)

    assert isinstance(open_provider(conn, repository=store), TrustedNamedContractProvider)


def test_an_unreadable_ledger_falls_back_instead_of_failing_the_build(conn, monkeypatch) -> None:
    monkeypatch.setenv(TRUSTED_READ_ENV_VAR, ",".join(CBOT_BENCHMARK_DATASET_KEYS))

    monkeypatch.setattr(
        "trust.repository.GitDirectoryTrustRepository",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("ledger directory is gone")),
    )

    # A storage migration must not be able to take the workstation down.
    assert isinstance(open_provider(conn), SqliteQuoteProvider)


def test_a_typo_in_the_switch_falls_back_rather_than_half_cutting_over(conn, monkeypatch) -> None:
    monkeypatch.setenv(TRUSTED_READ_ENV_VAR, "cbot-soybean-namd-contracts")

    assert isinstance(open_provider(conn), SqliteQuoteProvider)


# ---------------------------------------------------------------------------
# The provider
# ---------------------------------------------------------------------------


def test_the_curve_is_built_from_accepted_revisions_and_says_where_it_came_from(conn, tmp_path) -> None:
    store, _ = trusted(tmp_path, [bar("ZSU26.CBT", close="1150.25"), bar("ZSX26.CBT", close="1162.00")])

    observation = provider(store, conn).curve("Soybeans", as_of=AS_OF)

    assert [leg.contract.symbol for leg in observation.legs] == ["ZSU26", "ZSX26"]
    assert [leg.price for leg in observation.legs] == [1150.25, 1162.00]
    assert observation.observation_date == SESSION
    assert observation.provider is TRUSTED_LEDGER_YFINANCE
    assert observation.coherent is True


def test_the_ledger_does_not_upgrade_the_claim_about_the_number(conn, tmp_path) -> None:
    store, _ = trusted(tmp_path, [bar("ZSU26.CBT")])

    leg = provider(store, conn).curve("Soybeans", as_of=AS_OF).legs[0]

    # Provenance is proved; authority is not. A settlement is a claim about the
    # provider, and moving the bytes into a ledger does not change the provider.
    assert leg.price_type.value == "delayed_close"
    assert leg.provider.settlement_authoritative is False
    assert leg.is_settlement_proven is False


def test_a_quarantined_leg_is_absent_from_the_curve_rather_than_carried(conn, tmp_path) -> None:
    store = TemporaryDirectoryTrustRepository(tmp_path)
    ingest(store, [bar("ZSU26.CBT", close="1150.25"), bar("ZSX26.CBT", close="1162.00")])

    # A next-session print that moves further than the quarantine threshold.
    jump = Decimal("1150.25") * (1 + DAILY_MOVE_QUARANTINE_THRESHOLD * 2)
    later = AFTER_SETTLEMENT + timedelta(days=1)
    ingestion = ingest(
        store,
        [bar("ZSU26.CBT", day=date(2026, 8, 13), close=str(jump)),
         bar("ZSX26.CBT", day=date(2026, 8, 13), close="1163.00")],
        at=later,
    )
    assert ingestion.quarantined_revision_ids

    observation = provider(store, conn).curve("Soybeans", as_of=date(2026, 8, 14))

    # The curve is shorter and honest, not complete and carrying a number
    # nobody vouched for — and the accepted prior session is untouched.
    assert [leg.contract.symbol for leg in observation.legs] == ["ZSX26"]
    assert observation.observation_date == date(2026, 8, 13)

    # The quarantine did not overwrite accepted history: the prior session's
    # accepted close is still the answer when asked as of that day.
    prior = provider(store, conn).curve("Soybeans", as_of=SESSION)
    assert [leg.price for leg in prior.legs] == [1150.25, 1162.00]


def test_a_corrected_leg_returns_the_correction_not_the_value_it_replaced(conn, tmp_path) -> None:
    store, _ = trusted(tmp_path, [bar("ZSU26.CBT", close="1150.25")])
    identity = store.all_observation_revisions()[0].identity

    append_benchmark_correction(
        store,
        identity,
        corrected_value=Decimal("1151.75"),
        reason="provider republished the session close",
        corrected_at=AFTER_SETTLEMENT + timedelta(days=1),
    )

    leg = provider(store, conn).curve("Soybeans", as_of=AS_OF).legs[0]

    assert leg.price == 1151.75
    # And the superseded value is still in the ledger: a correction appends.
    assert len(store.all_observation_revisions()) == 2


def test_a_commodity_that_has_not_been_migrated_returns_nothing_from_the_ledger(conn, tmp_path) -> None:
    store, _ = trusted(tmp_path, [bar("ZSU26.CBT")])

    observation = provider(store, conn).curve("Corn", as_of=AS_OF)

    assert observation.legs == ()
    assert observation.coherent is False
    assert observation.coherence_note


def test_un_migrated_reads_are_delegated_to_v1_unchanged(conn, tmp_path) -> None:
    store, _ = trusted(tmp_path, [bar("ZSU26.CBT")])
    fallback = SqliteQuoteProvider(conn=conn)
    subject = TrustedNamedContractProvider(repository=store, fallback=fallback)

    # FX, the continuous research series and aggregate open interest are still
    # v1 datasets; answering them from a trusted-labelled provider would put an
    # un-migrated number behind a trusted label.
    assert subject.continuous("Soybeans", as_of=AS_OF) == fallback.continuous("Soybeans", as_of=AS_OF)
    assert subject.fx_rate("BRL/USD", on=AS_OF) == fallback.fx_rate("BRL/USD", on=AS_OF)
    assert subject.aggregate_open_interest("Soybeans", as_of=AS_OF) is None


def test_a_named_quote_resolves_through_the_same_accepted_curve(conn, tmp_path) -> None:
    from analysis.futures.domain import parse_symbol

    store, _ = trusted(tmp_path, [bar("ZSU26.CBT", close="1150.25"), bar("ZSX26.CBT", close="1162.00")])
    subject = provider(store, conn)

    assert subject.quote(parse_symbol("ZSX26.CBT"), as_of=AS_OF).price == 1162.00
    assert subject.quote(parse_symbol("ZSF27.CBT"), as_of=AS_OF) is None


# ---------------------------------------------------------------------------
# The reconciliation report
# ---------------------------------------------------------------------------


def frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(
        {"Open": [row["close"] - 5 for row in rows],
         "High": [row["close"] + 5 for row in rows],
         "Low": [row["close"] - 8 for row in rows],
         "Close": [row["close"] for row in rows],
         "Volume": [row.get("volume", 9000.0) for row in rows]},
        index=pd.DatetimeIndex([pd.Timestamp(row["day"]) for row in rows]),
    )


def test_both_paths_read_one_download_and_reconcile(tmp_path) -> None:
    import scripts.reconcile_cbot_benchmarks as script

    asked: list[str] = []

    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        asked.append(ticker)
        return frame([{"day": SESSION, "close": 1150.25}])

    run = script.reconcile_once(
        commodities=("Soybeans",),
        today=AS_OF,
        repository_root=tmp_path,
        download=download,
        now=AFTER_SETTLEMENT,
    )

    assert run.status == script.STATUS_RECONCILED
    assert run.commodities[0].legacy_rows == run.commodities[0].trusted_rows
    # One download per ticker, shared: a difference here could otherwise mean
    # "different download" rather than "different parse".
    assert len(asked) == len(set(asked))


def test_a_provider_that_publishes_nothing_is_not_graded_as_a_divergence(tmp_path) -> None:
    import scripts.reconcile_cbot_benchmarks as script

    run = script.reconcile_once(
        commodities=("Soybeans",),
        today=AS_OF,
        repository_root=tmp_path,
        download=lambda ticker, period="5d": pd.DataFrame(),
        now=AFTER_SETTLEMENT,
    )

    assert run.status == script.STATUS_NO_SESSION
    assert run.exit_code == script.EXIT_OK


def test_a_leg_the_ledger_refuses_is_reported_as_a_divergence(tmp_path) -> None:
    import scripts.reconcile_cbot_benchmarks as script

    def download(ticker: str, period: str = "5d") -> pd.DataFrame:
        if ticker == "ZSX26.CBT":
            # An impossible candle: v1 stores it, the ledger rejects it.
            return frame([{"day": SESSION, "close": 1162.0}]).assign(High=1100.0)
        return frame([{"day": SESSION, "close": 1150.25}])

    run = script.reconcile_once(
        commodities=("Soybeans",),
        today=AS_OF,
        repository_root=tmp_path,
        download=download,
        now=AFTER_SETTLEMENT,
    )

    assert run.status == script.STATUS_DIVERGED
    assert run.exit_code == script.EXIT_DIVERGED
    missing = run.commodities[0].report.missing_in_trusted
    assert any(row["ticker"] == "ZSX26.CBT" for row in missing)


def test_the_report_writes_a_json_file_ci_can_attach(tmp_path) -> None:
    import json

    import scripts.reconcile_cbot_benchmarks as script

    run = script.reconcile_once(
        commodities=("Soybeans",),
        today=AS_OF,
        repository_root=tmp_path / "ledger",
        download=lambda ticker, period="5d": frame([{"day": SESSION, "close": 1150.25}]),
        now=AFTER_SETTLEMENT,
    )
    path = script.write_report(run, tmp_path / "report.json")
    payload = json.loads(path.read_text())

    assert payload["status"] == script.STATUS_RECONCILED
    assert payload["dataset_keys"] == list(CBOT_BENCHMARK_DATASET_KEYS)
    assert payload["commodities"][0]["commodity"] == "Soybeans"
    assert payload["quarantined_revision_ids"] == []
