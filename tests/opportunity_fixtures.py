"""Builders for the Phase 4 opportunity tests.

Hand-verifiable by construction: every number a test asserts on is set here in
one place, so the assertion can be checked against the fixture by eye rather
than by re-running the code that produced it.
"""

from __future__ import annotations

from datetime import date, timedelta

import config
from analysis.opportunities.domain import (
    Blocker,
    BlockerCode,
    Confidence,
    Counterparty,
    Dislocation,
    Economics,
    Evidence,
    Grade,
    Ladder,
    MarketSignal,
    Opportunity,
    OpportunityStatus,
    PartyRole,
    SignalKind,
    Volume,
    identity_key,
    opportunity_id,
)
from analysis.opportunities.signals import Detection, country_port
from analysis.origins.domain import Incoterm, Money, ShipmentWindow, SourceRef

TODAY = date(2026, 8, 18)

GULF = country_port("US", "United States", role="origin")
CHINA = country_port("CN", "China")


def source(layer: str = "gulf_bids", table: str = "gulf_bids", key: str = "Soybeans") -> SourceRef:
    return SourceRef(layer=layer, table=table, key=key, detail="fixture", href="index.html")


def make_evidence(
    *,
    label: str = "US Gulf CIF",
    value: float = 420.0,
    observed_on: date = TODAY,
    max_age_days: int = 7,
    layer: str = "gulf_bids",
    confidence: Confidence = Confidence.INDICATIVE,
    quote_kind: str | None = "physical",
) -> Evidence:
    return Evidence(
        label=label,
        value=value,
        unit="usd_per_mt",
        observed_on=observed_on,
        source=source(layer=layer, table=layer),
        max_age_days=max_age_days,
        quote_kind=quote_kind,
        confidence=confidence,
    )


def make_signal(
    *,
    signal_id: str = "fixture:1",
    kind: SignalKind = SignalKind.LANDED_ADVANTAGE,
    observed_on: date = TODAY,
    validity_days: int = 3,
    magnitude: float = 12.0,
    evidence: tuple[Evidence, ...] | None = None,
) -> MarketSignal:
    return MarketSignal(
        signal_id=signal_id,
        kind=kind,
        headline="a fixture signal",
        detail="built by tests/opportunity_fixtures.py",
        observed_on=observed_on,
        evidence=(make_evidence(observed_on=observed_on),) if evidence is None else evidence,
        validity_days=validity_days,
        magnitude=magnitude,
        magnitude_unit="usd_per_mt",
        subject="US->CN",
    )


def make_detection(
    *,
    rule_id: str = "landed_advantage",
    origin=GULF,
    destination=CHINA,
    window: ShipmentWindow | None = None,
    economics: Economics | None = None,
    signal: MarketSignal | None = None,
    blockers: tuple[Blocker, ...] = (),
    missing: tuple[str, ...] = (),
    role_wanted: PartyRole = PartyRole.SELLER,
    volume: Volume | None = None,
    # Explicit "leave it out" switches. `window=None` cannot mean that, because
    # None is also the "use the default" signal every other argument uses, and a
    # test that silently got the default instead of the omission would pass while
    # asserting nothing.
    without_window: bool = False,
    without_economics: bool = False,
) -> Detection:
    window = None if without_window else (window if window is not None else ShipmentWindow(
        date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026"
    ))
    if without_economics:
        economics = None
    elif economics is None:
        economics = Economics(
            per_mt=Money(12.0),
            method="fixture edge",
            method_version="1.0.0",
            struck_on=TODAY,
            components=(("cheapest", 430.0), ("runner-up", 442.0)),
        )
    return Detection(
        signal=signal or make_signal(),
        rule_id=rule_id,
        product="beans",
        role_wanted=role_wanted,
        origin=origin,
        destination=destination,
        window=window,
        incoterm=Incoterm.FOB,
        grade=Grade(product="soybeans", specification="US No. 2 Yellow"),
        dislocation=Dislocation(
            kind="landed_advantage", label="vs runner-up", value=12.0, unit="usd_per_mt"
        ),
        economics=economics,
        volume=volume,
        blockers=blockers,
        missing=missing,
        why_now="a fixture reason",
    )


def make_counterparty(
    name: str = "Fixture Trading SA",
    *,
    role: PartyRole = PartyRole.SELLER,
    tier: int = 1,
    lane_evidenced: bool = True,
    confidence: str = "observed",
    verified_days_ago: int = 10,
) -> Counterparty:
    return Counterparty(
        name=name,
        country="BR",
        role=role,
        roles=("originator", "exporter"),
        products=("beans",),
        tier=tier,
        lane_evidenced=lane_evidenced,
        lane_note="named in this entry's own destinations" if lane_evidenced else None,
        confidence=confidence,
        last_verified=TODAY - timedelta(days=verified_days_ago),
        citation="https://example.invalid/fixture",
    )


def make_opportunity(
    *,
    rule_id: str = "landed_advantage",
    ladder: Ladder = Ladder.ACTIONABLE,
    blockers: tuple[Blocker, ...] = (),
    sellers: tuple[Counterparty, ...] | None = None,
    buyers: tuple[Counterparty, ...] = (),
    evidence: tuple[Evidence, ...] | None = None,
    economics: Economics | None = None,
    confidence: Confidence = Confidence.INDICATIVE,
    first_detected_on: date = TODAY,
    detected_on: date = TODAY,
    expires_on: date | None = None,
    workflow=None,
    status: OpportunityStatus = OpportunityStatus.DETECTED,
    volume: Volume | None = None,
) -> Opportunity:
    signal = make_signal(evidence=evidence)
    identity = identity_key(
        rule_id=rule_id,
        product="beans",
        origin_key=GULF.key,
        destination_key=CHINA.key,
        window_start=date(2026, 9, 1),
    )
    if economics is None:
        economics = Economics(
            per_mt=Money(12.0),
            method="fixture edge",
            method_version="1.0.0",
            struck_on=detected_on,
        )
    return Opportunity(
        opportunity_id=opportunity_id(identity, first_detected=first_detected_on),
        identity=identity,
        rule_id=rule_id,
        rule_label="Origin landed advantage",
        ladder=ladder,
        status=status,
        product="beans",
        grade=Grade(product="soybeans"),
        origin=GULF,
        destination=CHINA,
        incoterm=Incoterm.FOB,
        shipment_window=ShipmentWindow(date(2026, 9, 1), date(2026, 9, 30), label="Sep 2026"),
        why_now="a fixture reason",
        signals=(signal,),
        evidence=signal.evidence,
        confidence=confidence,
        first_detected_on=first_detected_on,
        detected_on=detected_on,
        expires_on=expires_on or (detected_on + timedelta(days=3)),
        sellers=sellers if sellers is not None else (make_counterparty(),),
        buyers=buyers,
        volume=volume,
        economics=economics,
        blockers=blockers,
        suggested_next_action="ring the shortlist",
        workflow=workflow,
    )


def hard_blocker(code: BlockerCode = BlockerCode.POLICY_BARRIER) -> Blocker:
    return Blocker(
        code=code,
        message="a fixture policy barrier",
        remedy="nothing in the price clears this",
    )


def soft_blocker(code: BlockerCode = BlockerCode.SIZE_UNKNOWN) -> Blocker:
    return Blocker(code=code, message="a fixture soft blocker", remedy="size it yourself")


# ---------------------------------------------------------------------------
# Database seeding
# ---------------------------------------------------------------------------
def seed_weekly_flow(
    conn,
    *,
    table: str = "inspection_destinations",
    value_column: str = "inspections_mt",
    weeks: int = 30,
    last_week: date = date(2026, 8, 16),
    baseline_share: float = 0.30,
    spike_share: float = 0.75,
    weekly_total: float = 1_000_000.0,
    country: str = "CHINA",
    other: str = "MEXICO",
    commodity: str = "Soybeans",
) -> None:
    """A steady destination share with one spike in the newest week.

    Deliberately *slightly* noisy: a perfectly flat baseline has a standard
    deviation of zero, which is exactly the case the z-score guard refuses to
    divide by. Tests for that case build it explicitly.

    The two tables have different shapes — ``inspection_destinations`` keys on a
    region, ``export_sales`` does not — so the column list is per table rather
    than one INSERT with a spare column nobody notices is being dropped.
    """
    has_region = table == "inspection_destinations"
    rows = []
    for index in range(weeks):
        week = last_week - timedelta(weeks=weeks - 1 - index)
        share = spike_share if index == weeks - 1 else baseline_share + (index % 3) * 0.01
        for name, value in ((country, share), (other, 1 - share)):
            row = (week.isoformat(), "fixture", name, commodity, weekly_total * value)
            rows.append(row if has_region else (commodity, week.isoformat(), name, weekly_total * value))
    if has_region:
        columns = f"week_ending, region, country, commodity, {value_column}"
    else:
        columns = f"commodity, week_ending, country, {value_column}"
    placeholders = ",".join("?" * len(columns.split(",")))
    conn.executemany(
        f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})",  # noqa: S608
        rows,
    )
    conn.commit()


def seed_export_sales(conn, **kwargs) -> None:
    """The same weekly share shape, on the forward-sales table."""
    seed_weekly_flow(
        conn, table="export_sales", value_column="outstanding_sales", **kwargs
    )


def seed_currency(
    conn,
    *,
    pair: str = "BRL/USD",
    # One more than the detector's own lookback, so the whole seeded path IS the
    # window it measures and the percentage move is the fixture's two endpoints.
    sessions: int = config.OPPORTUNITY_RULES["currency_shift"]["lookback_sessions"] + 1,
    last_date: date = date(2026, 8, 17),
    start: float = 0.20,
    end: float = 0.18,
) -> None:
    """A linear FX path, so the percentage move is arithmetic a reader can redo."""
    rows = []
    for index in range(sessions):
        when = last_date - timedelta(days=sessions - 1 - index)
        value = start + (end - start) * index / (sessions - 1)
        rows.append((pair, when.isoformat(), value))
    conn.executemany(
        "INSERT OR REPLACE INTO currencies (pair, Date, Close) VALUES (?, ?, ?)", rows
    )
    conn.commit()


def seed_freshness(conn, layer: str, *, status: str = "success", last_success: date = TODAY) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO data_freshness (layer_name, last_success, status) VALUES (?, ?, ?)",
        (layer, last_success.isoformat(), status),
    )
    conn.commit()


PLAYER_ENTRIES = [
    {
        "name": "Fixture Brazil Exportadora",
        "country": "BR",
        "roles": ["originator", "exporter"],
        "products": ["beans"],
        "confidence": "observed",
        "as_of": "2026-08-01",
        "tier": 1,
        "destinations_structured": [{"code": "CN", "products": ["beans"], "note": "fixture lane"}],
        "citations": [{"url": "https://example.invalid/br", "accessed": "2026-08-01"}],
    },
    {
        "name": "Fixture China Crushers",
        "country": "CN",
        "roles": ["importer", "crusher"],
        "products": ["beans"],
        "confidence": "observed",
        "as_of": "2026-08-01",
        "citations": [{"url": "https://example.invalid/cn", "accessed": "2026-08-01"}],
    },
    {
        "name": "Fixture US Originator",
        "country": "US",
        "roles": ["originator", "exporter"],
        "products": ["beans"],
        "confidence": "observed",
        "as_of": "2026-08-01",
        "destinations_structured": [{"code": "CN", "products": ["beans"]}],
        "citations": [{"url": "https://example.invalid/us", "accessed": "2026-08-01"}],
    },
]
