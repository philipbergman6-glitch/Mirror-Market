"""The one module in ``analysis/futures`` that knows a database exists.

Phase 3's isolation seam. Everything downstream — curve analytics, the hedge
calculator, scenarios, tickets, P&L — takes :class:`~analysis.futures.domain`
values and never a DataFrame, a table name or a Yahoo ticker. Substituting an
authoritative provider means writing another class satisfying
:class:`QuoteProvider` and changing nothing else.

Three things this module is responsible for, and they are all *honesty*
responsibilities rather than plumbing ones:

**Resolving a stored row to a named contract.** ``forward_curve`` stores a
Yahoo ticker (``ZSX26.CBT``) and a first-of-delivery-month date. Neither is a
contract; both are parsed into one here, and a row that cannot be resolved is
dropped with a warning rather than carried as an anonymous price. The contract
month is cross-checked against the ticker — the two encode the same fact, and
a disagreement means the row is not what it claims.

**Stating what kind of price it is.** Everything this stack currently has is a
delayed consumer daily bar, so every quote is stamped
``PriceType.DELAYED_CLOSE`` against the ``YFINANCE_DELAYED`` provider. Nothing
here may return ``SETTLEMENT``; that value is reserved for a provider that
proves it.

**Same-session coherence, checked at read.** The fetcher enforces one
observation date per curve at *write* time, but the table is keyed
``(commodity, contract_month, fetched_date)`` with no delete, so two runs on
one date leave the earlier run's legs standing — observed in the committed
history for 2026-08-11, where Soybeans has seven legs, six stamped that
session and ``ZSN27.CBT`` stamped NULL from an earlier run that the newer one
did not re-fetch. A curve is therefore never assumed coherent because it came
back from the fetcher; it is checked here, and an incoherent curve is reported
as such rather than analysed.
"""

from __future__ import annotations

import logging
import sqlite3
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol, runtime_checkable

from analysis.futures.domain import (
    CONTRACT_SPECS,
    YFINANCE_DELAYED,
    AggregateOpenInterest,
    ContinuousSeries,
    ContractQuote,
    Freshness,
    NamedContract,
    PriceType,
    Provider,
    RollMethod,
    UnknownContract,
    named_contract,
    parse_symbol,
    spec_for,
)

log = logging.getLogger(__name__)

#: Curve legs are the same venue, the same provider and the same cadence as
#: Layer 1, so they are graded on Layer 1's recency budget rather than a second
#: number that could disagree with it. ``forward_curve`` itself is deliberately
#: absent from ``LAYER_MAX_DATA_AGE_DAYS`` (its rows are dated by *contract*
#: month, months in the future, so a frozen curve would stay "recent" for a
#: year) — that exemption is about the contract_month column, not about the
#: observation date, which is what this grades.
_CURVE_AGE_LAYER = "prices"

#: A daily leg may legitimately go a long weekend plus a holiday without a
#: print before it is worth remarking on. Same default the propagation ledger
#: uses for a daily leg (``expected_gap_days``).
_EXPECTED_GAP_DAYS = 4


class ProviderError(RuntimeError):
    """A provider could not answer a question it should have been able to."""


# ---------------------------------------------------------------------------
# What a provider must be able to answer
# ---------------------------------------------------------------------------


@runtime_checkable
class QuoteProvider(Protocol):
    """The substitution seam.

    Deliberately small. Everything richer (carry, spreads, percentiles) is
    computed in ``curve.py`` from these primitives, so an authoritative feed
    does not have to reimplement the analytics to be swapped in.
    """

    provider: Provider

    def curve(self, commodity: str, *, as_of: date) -> CurveObservation:
        """Every liquid month this provider has for ``commodity``."""

    def quote(self, contract: NamedContract, *, as_of: date) -> ContractQuote | None:
        """One named contract's latest quote at or before ``as_of``."""

    def curve_history(self, commodity: str, *, as_of: date, sessions: int) -> tuple[CurveObservation, ...]:
        """Past curve snapshots, oldest first — the input to spread percentiles."""

    def close_history(self, contract: NamedContract, *, as_of: date, sessions: int) -> tuple[ContractQuote, ...]:
        """One named contract's stored session closes, oldest first.

        The input to the workstation's contract-row chart. Distinct from
        :meth:`curve_history` on purpose: a curve snapshot is many contracts
        on one session, this is one contract across many sessions — Layer 11b
        against Layer 11.
        """

    def continuous(self, commodity: str, *, as_of: date) -> ContinuousSeries | None:
        """The research series, labelled with how it was rolled."""

    def fx_rate(self, pair: str, *, on: date) -> tuple[date, float] | None:
        """(observation date, rate) for the most recent print at or before ``on``."""

    def aggregate_open_interest(
        self, commodity: str, *, as_of: date
    ) -> AggregateOpenInterest | None:
        """Whole-product open interest, or None where the provider has none.

        Separate from :meth:`quote` on purpose: a provider that publishes open
        interest per contract month would fill ``ContractQuote.open_interest``
        instead, and this method is what a provider that only has the weekly
        aggregate can honestly answer.
        """


# ---------------------------------------------------------------------------
# A curve observation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CurveObservation:
    """Every leg a provider had for one commodity, plus the coherence verdict.

    ``legs`` is nearest-expiry first. ``coherent`` false means the legs do not
    share one session, which makes every spread struck across them a mixture of
    term structure and the intervening move — the failure the fetcher's
    single-observation-date rule exists to prevent, re-checked here because the
    table can accumulate legs the fetcher never saw together.
    """

    commodity: str
    legs: tuple[ContractQuote, ...]
    observation_date: date | None
    fetched_date: date | None
    coherent: bool
    coherence_note: str = ""
    dropped_legs: tuple[str, ...] = ()
    provider: Provider = YFINANCE_DELAYED
    freshness: Freshness = Freshness.UNKNOWN
    age_days: int | None = None

    @property
    def is_empty(self) -> bool:
        return not self.legs

    def leg(self, symbol: str) -> ContractQuote | None:
        for quote in self.legs:
            if quote.contract.symbol == symbol.upper():
                return quote
        return None

    def leg_for_month(self, year: int, month: int) -> ContractQuote | None:
        for quote in self.legs:
            if quote.contract.year == year and quote.contract.month == month:
                return quote
        return None

    @property
    def front(self) -> ContractQuote | None:
        return self.legs[0] if self.legs else None


# ---------------------------------------------------------------------------
# The SQLite implementation over the existing layers
# ---------------------------------------------------------------------------


def _as_date(raw) -> date | None:
    if raw in (None, "", "None"):
        return None
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    text = str(raw).strip()
    for cut in (10, 19):
        try:
            return date.fromisoformat(text[:cut])
        except ValueError:
            continue
    return None


def _resolve_contract(ticker: str | None, contract_month: str | None, commodity: str) -> NamedContract | None:
    """Turn a stored curve row into a named contract, or refuse it.

    The ticker and the contract-month column encode the same fact. When both
    are present they are checked against each other: a row where they disagree
    is not a contract this provider can name, and naming it anyway would put a
    wrong expiry on a hedge.
    """
    from_ticker: NamedContract | None = None
    if ticker:
        try:
            from_ticker = parse_symbol(str(ticker))
        except (UnknownContract, ValueError) as exc:
            log.warning("curve row %s/%s: cannot parse ticker — %s", commodity, ticker, exc)

    month_date = _as_date(contract_month)
    from_month: NamedContract | None = None
    if month_date is not None:
        try:
            from_month = named_contract(commodity, month_date.year, month_date.month)
        except (UnknownContract, ValueError) as exc:
            log.warning("curve row %s/%s: cannot name contract month — %s", commodity, contract_month, exc)

    if from_ticker and from_month:
        if (from_ticker.year, from_ticker.month, from_ticker.spec.root) != (
            from_month.year, from_month.month, from_month.spec.root
        ):
            log.warning(
                "curve row %s: ticker %s and contract_month %s disagree — dropping the leg",
                commodity, ticker, contract_month,
            )
            return None
        return from_ticker
    return from_ticker or from_month


def _freshness(observation: date | None, as_of: date, max_age_days: int) -> tuple[Freshness, int | None]:
    if observation is None:
        return Freshness.UNKNOWN, None
    age = (as_of - observation).days
    if age > max_age_days:
        return Freshness.STALE, age
    if age > _EXPECTED_GAP_DAYS:
        return Freshness.BEHIND, age
    return Freshness.CURRENT, age


@dataclass
class SqliteQuoteProvider:
    """Reads the layers this project already ingests.

    Takes a live connection rather than opening one, the same contract
    ``analysis/origins/sources.py`` keeps: the site renders eight market pages
    plus this one through a single connection, and a module that opened its own
    would read a different snapshot than the page beside it.
    """

    conn: sqlite3.Connection
    provider: Provider = field(default=YFINANCE_DELAYED)
    price_type: PriceType = field(default=PriceType.DELAYED_CLOSE)

    # -- curve ------------------------------------------------------------
    def curve(self, commodity: str, *, as_of: date) -> CurveObservation:
        rows, columns = self._curve_rows(commodity, as_of=as_of)
        return self._build_observation(commodity, rows, columns, as_of=as_of)

    def curve_history(
        self, commodity: str, *, as_of: date, sessions: int = 60
    ) -> tuple[CurveObservation, ...]:
        if not self._has_table("forward_curve"):
            return ()
        fetched = [
            str(row[0])
            for row in self.conn.execute(
                "SELECT DISTINCT fetched_date FROM forward_curve "
                "WHERE commodity = ? AND fetched_date <= ? "
                "ORDER BY fetched_date DESC LIMIT ?",
                (commodity, as_of.isoformat(), int(sessions)),
            )
        ]
        out = []
        for fetched_date in reversed(fetched):
            rows, columns = self._curve_rows(commodity, as_of=as_of, fetched_date=fetched_date)
            observation = self._build_observation(commodity, rows, columns, as_of=as_of)
            if observation.legs:
                out.append(observation)
        return tuple(out)

    def close_history(
        self, contract: NamedContract, *, as_of: date, sessions: int = 500
    ) -> tuple[ContractQuote, ...]:
        """This contract's Layer 11b daily closes, oldest first.

        Keyed by (commodity, contract month) rather than by ticker so the
        read does not have to rebuild Yahoo's suffix grammar — the fetcher
        already resolved it once. A database without the table answers with
        the same reasoned emptiness as one with no rows: "we hold no history
        for this contract"."""
        if not self._has_table("contract_history"):
            return ()
        rows = list(self.conn.execute(
            "SELECT date, close, volume, fetched_date, open, high, low "
            "FROM contract_history "
            "WHERE commodity = ? AND contract_month = ? AND date <= ? "
            "ORDER BY date DESC LIMIT ?",
            (
                contract.spec.name,
                contract.contract_month_date.isoformat(),
                as_of.isoformat(),
                int(sessions),
            ),
        ))
        max_age = self._max_age_days()
        # Freshness belongs to the series, not to each row: a two-year history
        # whose newest session printed yesterday is CURRENT, and stamping every
        # older session STALE would invite a future consumer to filter the
        # whole record away. Graded off the newest date the query returned
        # (rows arrive newest-first here).
        newest = _as_date(rows[0][0]) if rows else None
        freshness, _ = _freshness(newest, as_of, max_age)
        quotes: list[ContractQuote] = []
        for raw_date, close, volume, raw_fetched, bar_open, bar_high, bar_low in reversed(rows):
            day = _as_date(raw_date)
            if day is None or close is None:
                continue
            # The cleaner already enforced all-or-nothing on the trio; the
            # re-check here means a hand-edited row cannot smuggle a partial
            # candle past the read path.
            has_bar = bar_open is not None and bar_high is not None and bar_low is not None
            quotes.append(ContractQuote(
                contract=contract,
                price=float(close),
                price_type=self.price_type,
                observation_date=day,
                provider=self.provider,
                freshness=freshness,
                fetched_date=_as_date(raw_fetched),
                volume=None if volume is None else float(volume),
                session_open=float(bar_open) if has_bar else None,
                session_high=float(bar_high) if has_bar else None,
                session_low=float(bar_low) if has_bar else None,
            ))
        return tuple(quotes)

    def _curve_rows(
        self, commodity: str, *, as_of: date, fetched_date: str | None = None
    ) -> tuple[list[tuple], list[str]]:
        selected = ["contract_month", "label", "ticker", "close", "observation_date", "fetched_date"]
        if not self._has_table("forward_curve"):
            return [], selected
        # volume and open_interest are additive columns. A database predating
        # the migration does not answer those questions at all, which is a
        # different thing from answering them with zero.
        selected += [c for c in ("volume", "open_interest") if c in self._curve_columns()]
        if fetched_date is None:
            latest = self.conn.execute(
                "SELECT MAX(fetched_date) FROM forward_curve "
                "WHERE commodity = ? AND fetched_date <= ?",
                (commodity, as_of.isoformat()),
            ).fetchone()
            if not latest or latest[0] is None:
                return [], selected
            fetched_date = str(latest[0])
        rows = list(self.conn.execute(
            f"SELECT {', '.join(selected)} FROM forward_curve "  # noqa: S608 — columns come from PRAGMA
            "WHERE commodity = ? AND fetched_date = ? ORDER BY contract_month",
            (commodity, fetched_date),
        ))
        return rows, selected

    def _curve_columns(self) -> set[str]:
        return {row[1] for row in self.conn.execute("PRAGMA table_info(forward_curve)")}

    def _has_table(self, name: str) -> bool:
        """Whether this database has the table at all.

        A database that never ran the pipeline has no ``forward_curve`` — which
        is the same *fact* as having no rows in it ("we hold no curve"), and is
        rendered by the same reasoned empty state. It is caught here rather than
        left to raise so that one un-initialised table cannot take the whole
        page down; a table that exists and is empty still reports emptiness, so
        nothing is hidden either way.
        """
        return bool(self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1", (name,)
        ).fetchone())

    def _build_observation(
        self, commodity: str, rows: list[tuple], selected: list[str], *, as_of: date
    ) -> CurveObservation:
        if not rows:
            return CurveObservation(
                commodity=commodity, legs=(), observation_date=None, fetched_date=None,
                coherent=False, coherence_note="no curve rows stored for this commodity",
                provider=self.provider,
            )

        index = {name: position for position, name in enumerate(selected)}
        max_age = self._max_age_days()

        parsed: list[tuple[date | None, ContractQuote]] = []
        dropped: list[str] = []
        fetched_dates: set[date] = set()

        for row in rows:
            ticker = row[index["ticker"]]
            contract = _resolve_contract(ticker, row[index["contract_month"]], commodity)
            close = row[index["close"]]
            if contract is None or close is None:
                dropped.append(str(ticker or row[index["contract_month"]]))
                continue
            observation = _as_date(row[index["observation_date"]])
            fetched = _as_date(row[index["fetched_date"]])
            if fetched:
                fetched_dates.add(fetched)
            freshness, _ = _freshness(observation or fetched, as_of, max_age)
            parsed.append((observation, ContractQuote(
                contract=contract,
                price=float(close),
                price_type=self.price_type,
                # A leg with no observation date is dated by the run that
                # fetched it. That is weaker information, and the coherence
                # check below is what makes the weakness visible rather than
                # this silently upgrading it.
                observation_date=observation or fetched or as_of,
                provider=self.provider,
                freshness=freshness,
                fetched_date=fetched,
                volume=_optional_float(row, index, "volume"),
                open_interest=_optional_float(row, index, "open_interest"),
            )))

        coherent, note, coherent_legs = _coherence(parsed)
        kept = tuple(sorted(coherent_legs, key=lambda q: (q.contract.year, q.contract.month)))
        observation_date = kept[0].observation_date if kept else None
        curve_fetched = max(fetched_dates) if fetched_dates else None
        freshness, age = _freshness(observation_date, as_of, max_age)

        return CurveObservation(
            commodity=commodity,
            legs=kept,
            observation_date=observation_date,
            fetched_date=curve_fetched,
            coherent=coherent,
            coherence_note=note,
            dropped_legs=tuple(dropped),
            provider=self.provider,
            freshness=freshness,
            age_days=age,
        )

    def _max_age_days(self) -> int:
        import config

        return config.LAYER_MAX_DATA_AGE_DAYS.get(_CURVE_AGE_LAYER, 14)

    # -- single contract ---------------------------------------------------
    def quote(self, contract: NamedContract, *, as_of: date) -> ContractQuote | None:
        observation = self.curve(contract.spec.name, as_of=as_of)
        return observation.leg(contract.symbol)

    # -- continuous --------------------------------------------------------
    def continuous(self, commodity: str, *, as_of: date, sessions: int = 750) -> ContinuousSeries | None:
        """The stored front-month series, labelled for what it is.

        ``prices`` holds Yahoo's ``ZS=F``: the provider switches the underlying
        contract on its own schedule and does not publish which contract any
        bar belongs to, so ``contract_by_date`` and ``roll_dates`` are empty.
        That emptiness is the honest answer, and it is why
        ``analysis.signals.is_near_roll`` has to estimate roll days from a
        calendar. ``continuous.py`` builds the alternative, from named
        contracts, where history depth allows.
        """
        if commodity not in CONTRACT_SPECS or not self._has_table("prices"):
            return None
        rows = list(self.conn.execute(
            "SELECT Date, Close FROM prices WHERE commodity = ? AND Date <= ? "
            "ORDER BY Date DESC LIMIT ?",
            (commodity, as_of.isoformat(), int(sessions)),
        ))
        points = tuple(
            (day, float(close))
            for day, close in ((_as_date(raw_date), raw_close) for raw_date, raw_close in reversed(rows))
            if day is not None and close is not None
        )
        if not points:
            return None
        return ContinuousSeries(
            commodity=commodity,
            roll_method=RollMethod.PROVIDER_FRONT_MONTH,
            points=points,
            adjustment_note="unadjusted; the provider does not publish its roll dates",
        )

    # -- open interest -----------------------------------------------------
    def aggregate_open_interest(
        self, commodity: str, *, as_of: date
    ) -> AggregateOpenInterest | None:
        """Whole-product open interest from the stored CFTC COT report.

        The only open-interest figure anything in this stack ingests. It is a
        weekly Tuesday number covering every listed month at once, which is why
        it comes back as :class:`AggregateOpenInterest` and never as a field on
        a contract quote — see that class for what attaching it to one month
        would falsely assert.

        The COT commodity keys (``config.COT_COMMODITIES``) are the same nine
        strings ``CONTRACT_SPECS`` uses, so the join is by name with no mapping
        table to drift. Returns None where the layer has never run or the
        product is not in the report; a missing number stays missing.
        """
        if not self._has_table("cot"):
            return None
        row = self.conn.execute(
            "SELECT Date, total_open_interest FROM cot "
            "WHERE commodity = ? AND Date <= ? AND total_open_interest IS NOT NULL "
            "ORDER BY Date DESC LIMIT 1",
            (commodity, as_of.isoformat()),
        ).fetchone()
        if not row:
            return None
        report_date = _as_date(row[0])
        if report_date is None:
            return None
        return AggregateOpenInterest(
            commodity=commodity, contracts=float(row[1]), report_date=report_date,
        )

    # -- fx ----------------------------------------------------------------
    def fx_rate(self, pair: str, *, on: date) -> tuple[date, float] | None:
        """Most recent close for ``pair`` at or before ``on``.

        The stack's convention (``analysis.soy_analytics._latest_aligned_usd``)
        is that ``<CCY>/USD`` is USD per unit of the home currency, so callers
        *multiply*. Returned with its own observation date because a hedge
        struck at a rate from three days ago must say so.
        """
        if not self._has_table("currencies"):
            return None
        row = self.conn.execute(
            "SELECT Date, Close FROM currencies WHERE pair = ? AND Date <= ? AND Close IS NOT NULL "
            "ORDER BY Date DESC LIMIT 1",
            (pair, on.isoformat()),
        ).fetchone()
        if not row:
            return None
        observed = _as_date(row[0])
        if observed is None:
            return None
        return observed, float(row[1])


def _optional_float(row: Sequence, index: dict[str, int], column: str) -> float | None:
    position = index.get(column)
    if position is None:
        return None
    value = row[position]
    return None if value is None else float(value)


def _coherence(
    parsed: list[tuple[date | None, ContractQuote]],
) -> tuple[bool, str, list[ContractQuote]]:
    """Decide whether these legs are one session, and keep only those that are.

    The rule is a single date, not a tolerance window — the same rule the
    fetcher applies, for the same reason: one day of drift is exactly the case
    that fakes term structure, because a stale leg still carries the previous
    session's level and the spread against it is yesterday's move.

    A leg with no observation date at all is dropped and *counted against
    coherence*: it is the shape the 2026-08-11 stragglers take, and keeping it
    would put a price of unknown vintage into a spread.
    """
    if not parsed:
        return False, "no legs", []

    dated = [(observed, quote) for observed, quote in parsed if observed is not None]
    undated = [quote for observed, quote in parsed if observed is None]

    if not dated:
        return False, (
            f"{len(undated)} leg(s) carry no observation date — the session they belong to is unknown"
        ), []

    newest = max(observed for observed, _ in dated)
    on_session = [quote for observed, quote in dated if observed == newest]
    earlier = [(observed, quote) for observed, quote in dated if observed != newest]

    if not earlier and not undated:
        return True, "", on_session

    problems = []
    if earlier:
        problems.append(
            "dropped " + ", ".join(
                f"{quote.contract.symbol} observed {observed.isoformat()}"
                for observed, quote in sorted(earlier)
            )
        )
    if undated:
        problems.append(
            "dropped " + ", ".join(f"{quote.contract.symbol} (undated)" for quote in undated)
        )
    return False, (
        f"curve dated {newest.isoformat()}; " + "; ".join(problems)
        + " — legs from another session cannot be spread against these"
    ), on_session


def open_provider(conn: sqlite3.Connection, *, repository=None) -> QuoteProvider:
    """The provider for this deployment, honouring the trusted-read switch.

    Default is unchanged and always will be: ``SqliteQuoteProvider`` over the
    v1 tables. The trusted ledger is used only when
    ``MIRROR_TRUSTED_READ_DATASETS`` names **all three** soy benchmark datasets
    — a curve mixing a trusted bean with a v1 oil would put two provenances
    inside one crush — and the rollback is unsetting that variable.

    A switch that names the datasets but cannot open the ledger falls back to
    v1 with a loud warning rather than failing the build: the v1 answer is the
    one the site published yesterday, and a page that renders nothing is worse
    than a page that renders the pre-cutover number and says so.
    """
    fallback = SqliteQuoteProvider(conn=conn)
    try:
        from trust.read_path import CBOT_BENCHMARK_DATASET_KEYS, load_trusted_read_switch

        if not load_trusted_read_switch().enabled_for_all(CBOT_BENCHMARK_DATASET_KEYS):
            return fallback

        from analysis.futures.trusted_provider import TrustedNamedContractProvider
        from trust.repository import GitDirectoryTrustRepository

        store = repository if repository is not None else GitDirectoryTrustRepository(_project_root())
        return TrustedNamedContractProvider(
            repository=store,
            fallback=fallback,
            max_age_days=fallback._max_age_days(),
        )
    except Exception:  # noqa: BLE001 — any ledger problem must degrade, not fail the build
        log.exception("trusted read path unavailable — falling back to the v1 forward_curve tables")
        return fallback


def _project_root():
    from pathlib import Path

    return Path(__file__).resolve().parents[2]


def known_commodities() -> tuple[str, ...]:
    return tuple(CONTRACT_SPECS)


def describe_provider(provider: Provider) -> dict[str, object]:
    """What a surface must say about where its numbers came from."""
    return {
        "key": provider.key,
        "display": provider.display,
        "delayed": provider.delayed,
        "settlement_authoritative": provider.settlement_authoritative,
        "note": provider.note,
    }


__all__ = [
    "CurveObservation",
    "ProviderError",
    "QuoteProvider",
    "SqliteQuoteProvider",
    "describe_provider",
    "known_commodities",
    "open_provider",
    "spec_for",
]
