"""A :class:`~analysis.futures.providers.QuoteProvider` served by the trusted ledger.

The cutover half of DT-16. ``SqliteQuoteProvider`` reads ``forward_curve``, a
table written by the v1 fetcher where a row is whatever last came back; this
reads accepted, current observation revisions, where a row is a value that
passed every rule in its dataset contract and has not been superseded.

What changes for a caller: nothing in the interface, and three things in the
answers.

- A leg whose revision was **quarantined** (an implausible move) or
  **rejected** (an impossible candle, an unfinished session, a symbol that is
  not this product's contract) is absent. The curve is shorter and honest,
  rather than complete and carrying a number nobody vouched for.
- A **corrected** leg returns the correction, not the value it replaced, and
  the value it replaced stays in the ledger for audit.
- Every quote carries its ``revision_id``, so a displayed number resolves to
  one durable record.

What does *not* change: the price is still a delayed consumer daily bar and
still may not be called a settlement. The ledger proves provenance, not
authority — ``settlement_authoritative`` stays ``False`` and
``PriceType.DELAYED_CLOSE`` stays the price type. Anything else would let a
storage change look like a data-quality upgrade.

This provider is only reachable when :mod:`trust.read_path` names its datasets,
and only when it names *all three* soy legs: a crush struck from a trusted bean
and a v1 oil would be two provenances inside one number.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, replace
from datetime import date

from analysis.futures.domain import (
    YFINANCE_DELAYED,
    AggregateOpenInterest,
    ContinuousSeries,
    ContractQuote,
    Freshness,
    NamedContract,
    PriceType,
    Provider,
    UnknownContract,
    parse_symbol,
)
from analysis.futures.providers import CurveObservation, SqliteQuoteProvider, _freshness

log = logging.getLogger(__name__)

#: The same provider, said differently. The bytes still come from Yahoo and the
#: claim about them is unchanged; what the key records is that the number was
#: read back from the trusted ledger rather than from the v1 table, which is a
#: provenance fact a surface is entitled to state.
TRUSTED_LEDGER_YFINANCE: Provider = replace(
    YFINANCE_DELAYED,
    key="yfinance_delayed_trusted",
    display="Yahoo Finance (delayed, trusted ledger)",
    note=(
        YFINANCE_DELAYED.note
        + " Served from the trusted observation ledger: accepted, current revisions only, "
        "each resolving to a raw artifact and its quality findings."
    ),
)


@dataclass
class TrustedNamedContractProvider:
    """Named-contract curves from the ledger; everything else from ``fallback``.

    The narrow scope is the point. Only the named soy benchmark contracts have
    completed trusted ingestion, so only they are answered from the ledger. FX,
    the continuous research series and aggregate open interest are still v1
    datasets and are delegated unchanged — pretending otherwise would put an
    un-migrated number behind a trusted label.
    """

    repository: object
    fallback: SqliteQuoteProvider
    provider: Provider = field(default=TRUSTED_LEDGER_YFINANCE)
    price_type: PriceType = field(default=PriceType.DELAYED_CLOSE)
    max_age_days: int = 14

    # -- curve -------------------------------------------------------------
    def curve(self, commodity: str, *, as_of: date) -> CurveObservation:
        sessions = self._sessions(commodity, as_of=as_of)
        if not sessions:
            return self._empty(commodity, "no accepted trusted revisions for this commodity")
        newest = max(sessions)
        return self._observation(commodity, newest, sessions[newest], as_of=as_of)

    def curve_history(
        self, commodity: str, *, as_of: date, sessions: int = 60
    ) -> tuple[CurveObservation, ...]:
        by_session = self._sessions(commodity, as_of=as_of)
        ordered = sorted(by_session)[-int(sessions):] if sessions else []
        return tuple(
            self._observation(commodity, session, by_session[session], as_of=as_of)
            for session in ordered
        )

    def quote(self, contract: NamedContract, *, as_of: date) -> ContractQuote | None:
        return self.curve(contract.spec.name, as_of=as_of).leg(contract.symbol)

    # -- still v1 ----------------------------------------------------------
    def close_history(
        self, contract: NamedContract, *, as_of: date, sessions: int = 500
    ) -> tuple[ContractQuote, ...]:
        # Layer 11b has no trusted edition yet; the fallback's answer is the
        # honest one, already labelled with its own provider and price type.
        return self.fallback.close_history(contract, as_of=as_of, sessions=sessions)

    def continuous(self, commodity: str, *, as_of: date) -> ContinuousSeries | None:
        return self.fallback.continuous(commodity, as_of=as_of)

    def fx_rate(self, pair: str, *, on: date) -> tuple[date, float] | None:
        return self.fallback.fx_rate(pair, on=on)

    def aggregate_open_interest(self, commodity: str, *, as_of: date) -> AggregateOpenInterest | None:
        return self.fallback.aggregate_open_interest(commodity, as_of=as_of)

    # -- internals ---------------------------------------------------------
    def _sessions(self, commodity: str, *, as_of: date) -> dict[date, list[Mapping[str, object]]]:
        """Accepted current revisions for ``commodity``, grouped by session.

        Imported here rather than at module scope so ``analysis.futures`` keeps
        its "no dependency on ``trust``" property for every caller that never
        turns the switch on.
        """
        from trust.cbot_benchmarks import BENCHMARK_COMMODITIES, contract_for
        from trust.domain import EligibilityScope

        if commodity not in BENCHMARK_COMMODITIES:
            return {}
        repository = self.repository
        contract = contract_for(commodity)
        dataset_id = contract.dataset.dataset_id

        identities: dict[str, object] = {}
        for revision in repository.all_observation_revisions():  # type: ignore[attr-defined]
            identity = revision.identity
            if identity.dataset_id != dataset_id or identity.effective_date > as_of:
                continue
            identities.setdefault(identity.observation_id, identity)

        by_session: dict[date, list[Mapping[str, object]]] = {}
        for identity in identities.values():
            head = repository.current_accepted_revision(  # type: ignore[attr-defined]
                identity, scope=EligibilityScope.INTERNAL
            )
            if head is None:
                continue
            row = _row(head)
            if row is None:
                continue
            by_session.setdefault(head.identity.effective_date, []).append(row)
        return by_session

    def _observation(
        self,
        commodity: str,
        session: date,
        rows: list[Mapping[str, object]],
        *,
        as_of: date,
    ) -> CurveObservation:
        legs: list[ContractQuote] = []
        dropped: list[str] = []
        for row in rows:
            symbol = str(row["symbol"])
            try:
                contract = parse_symbol(symbol)
            except (UnknownContract, ValueError) as exc:
                # The `contract.named` rule means this cannot happen for a
                # revision this project wrote; it can happen for one imported
                # under an older parser, and dropping it named is better than
                # naming a contract that may not be the one stored.
                log.warning("trusted curve row %s/%s: %s", commodity, symbol, exc)
                dropped.append(symbol)
                continue
            freshness, _ = _freshness(session, as_of, self.max_age_days)
            legs.append(
                ContractQuote(
                    contract=contract,
                    price=float(row["close"]),  # type: ignore[arg-type]
                    price_type=self.price_type,
                    observation_date=session,
                    provider=self.provider,
                    freshness=freshness,
                    volume=row["volume"],  # type: ignore[arg-type]
                    # No source publishes open interest per delivery month, and
                    # the ledger does not invent one.
                    open_interest=None,
                )
            )
        kept = tuple(sorted(legs, key=lambda quote: (quote.contract.year, quote.contract.month)))
        freshness, age = _freshness(session if kept else None, as_of, self.max_age_days)
        return CurveObservation(
            commodity=commodity,
            legs=kept,
            observation_date=session if kept else None,
            fetched_date=None,
            # Coherent by construction, not by inspection: a session is the
            # grouping key, so legs from two sessions can never end up in one
            # observation the way they can in `forward_curve`.
            coherent=bool(kept),
            coherence_note="" if kept else "no resolvable legs on the newest accepted session",
            dropped_legs=tuple(dropped),
            provider=self.provider,
            freshness=freshness,
            age_days=age,
        )

    def _empty(self, commodity: str, note: str) -> CurveObservation:
        return CurveObservation(
            commodity=commodity,
            legs=(),
            observation_date=None,
            fetched_date=None,
            coherent=False,
            coherence_note=note,
            provider=self.provider,
            freshness=Freshness.UNKNOWN,
        )


def _row(revision) -> Mapping[str, object] | None:
    identity = revision.identity
    symbol = identity.source_record_id
    if not symbol:
        return None
    return {
        "symbol": str(symbol),
        "close": float(revision.value),
        "volume": None if revision.volume is None else float(revision.volume),
        "revision_id": revision.revision_id,
    }


__all__ = ["TRUSTED_LEDGER_YFINANCE", "TrustedNamedContractProvider"]
