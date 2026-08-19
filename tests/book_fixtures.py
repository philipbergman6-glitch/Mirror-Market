"""A synthetic desk: one book, one clearing statement, one option ladder.

Synthetic and marked as such. Every account string carries :data:`MARK` so a
privacy test can grep for it, and the numbers are round rather than plausible
so nobody mistakes a fixture for somebody's real position.

The same fixtures back the worked example in ``scripts/worked_book_example.py``
— one synthetic desk, exercised by the tests and printed by the demo, so the
demo cannot drift away from what is actually verified.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

from analysis.futures.domain import (
    YFINANCE_DELAYED,
    ContractQuote,
    Freshness,
    PriceType,
    Side,
    named_contract,
    parse_symbol,
)
from analysis.futures.hedge import BasisConvention, PhysicalUnit
from analysis.futures.limits import DeskLimit
from analysis.futures.positions import Book, Fill, FuturesPosition, PhysicalPosition

#: Grep marker. Every free-text field a client would fill in carries it.
MARK = "SYNTHETIC"
ACCOUNT = "SYNTHETIC-DESK-001"
BROKER = "SYNTHETIC Clearing LLC"

TODAY = date(2026, 8, 19)
GENERATED_AT = datetime(2026, 8, 19, 21, 30, tzinfo=timezone.utc)

#: Board levels for the session, native units. Soybeans and meal in the Nov/Dec
#: months a Q4 shipment would price against.
PRICES: dict[str, float] = {
    "ZSX26": 1150.00,     # cents/bu
    "ZSF27": 1162.00,
    "ZMZ26": 305.00,      # USD/short ton
    "ZLZ26": 52.00,       # cents/lb
}


def quote(symbol: str, *, price: float | None = None, as_of: date = TODAY) -> ContractQuote:
    return ContractQuote(
        contract=parse_symbol(symbol),
        price=PRICES[symbol] if price is None else price,
        price_type=PriceType.DELAYED_CLOSE,
        observation_date=as_of,
        provider=YFINANCE_DELAYED,
        freshness=Freshness.CURRENT,
        volume=1_000.0,
    )


def quotes(as_of: date = TODAY) -> dict[str, ContractQuote]:
    return {symbol: quote(symbol, as_of=as_of) for symbol in PRICES}


def quote_for(as_of: date = TODAY):
    """The ``(commodity, symbol) -> quote`` callable ``value_book`` expects."""
    table = quotes(as_of)

    def lookup(commodity: str, symbol: str | None) -> ContractQuote | None:
        if symbol is None:
            return next(
                (q for q in table.values() if q.contract.spec.name == commodity), None
            )
        return table.get(symbol.upper())

    return lookup


def fx_for(rate: float = 0.185):
    """BRL/USD — USD per one BRL, on the same session."""

    def lookup(pair: str | None) -> tuple[date, float] | None:
        return (TODAY, rate) if pair == "BRL/USD" else None

    return lookup


def synthetic_book(*, limits: bool = True) -> Book:
    """12,000 MT of beans bought basis, hedged short, plus a BRL meal length."""
    return Book(
        physical=(
            PhysicalPosition(
                commodity="Soybeans",
                quantity=12_000.0,
                unit=PhysicalUnit.METRIC_TON,
                side=Side.LONG,
                average_cost_usd_mt=402.50,
                currency="USD",
                basis_convention=BasisConvention.BASIS_OVER_FUTURES,
                mark_contract="ZSX26",
                current_basis_usd_mt=-12.5,
                entry_futures_usd_mt=415.00,
                entry_basis_usd_mt=-8.0,
                location="Paranagua",
                note=f"{MARK} Nov-Dec loading",
            ),
            PhysicalPosition(
                commodity="Soybean Meal",
                quantity=3_000.0,
                unit=PhysicalUnit.METRIC_TON,
                side=Side.LONG,
                average_cost_usd_mt=330.00,
                currency="BRL",
                fx_pair="BRL/USD",
                basis_convention=BasisConvention.UNPRICED,
                mark_contract="ZMZ26",
                current_basis_usd_mt=4.0,
                entry_futures_usd_mt=336.0,
                entry_basis_usd_mt=2.0,
                entry_fx_rate=0.190,
                location="Ponta Grossa",
                note=f"{MARK} domestic meal length",
            ),
        ),
        futures=(
            FuturesPosition(
                contract=named_contract("Soybeans", 2026, 11),
                account=ACCOUNT,
                fills=(
                    Fill(date(2026, 8, 4), Side.SHORT, 60, 1172.25, reference=f"{MARK}-1"),
                    Fill(date(2026, 8, 7), Side.SHORT, 28, 1180.00, reference=f"{MARK}-2"),
                    Fill(date(2026, 8, 11), Side.LONG, 20, 1165.50, reference=f"{MARK}-3"),
                ),
            ),
            FuturesPosition(
                contract=named_contract("Soybean Meal", 2026, 12),
                account=ACCOUNT,
                stated_contracts=-20.0,
                stated_average_price=308.40,
                note=f"{MARK} stated, not derived",
            ),
        ),
        limits=synthetic_limits() if limits else (),
        loaded_from=("data/reference/positions/synthetic.yml",),
    )


def synthetic_limits() -> tuple[DeskLimit, ...]:
    return (
        DeskLimit(key="flat_price_mt", scope="Soybeans", maximum=6_000, warn_at=4_000,
                  note=f"{MARK} desk mandate"),
        DeskLimit(key="basis_mt", scope="*", maximum=20_000, warn_at=15_000),
        DeskLimit(key="residual_mt", scope="Soybeans", maximum=3_000),
        DeskLimit(key="fx_usd", scope="BRL/USD", maximum=500_000),
        DeskLimit(key="first_notice_contracts", scope="*", maximum=0,
                  note=f"{MARK} nothing open past first notice"),
        DeskLimit(key="loss_usd", scope="*", maximum=250_000),
        DeskLimit(key="notional_usd", scope="*", maximum=8_000_000),
    )
