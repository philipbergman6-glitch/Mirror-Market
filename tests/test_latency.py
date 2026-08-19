"""Tests for the latency vocabulary and its measurement.

Every clock is injected. A latency test that reads the wall clock measures
the test runner, not the product.

The scenarios below are the ones the phase was specified against:
early-session, post-settlement, weekend, holiday and stale-provider. Each is
expressed as a set of stamps, because that is exactly what the pipeline
records and what the surfaces read.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from latency.domain import (
    LATENCY_CLASS_BY_LAYER,
    LAYER_LATENCIES,
    LAYER_LATENCY_BY_KEY,
    OBJECTIVES,
    Granularity,
    LatencyClass,
    LayerMeasurement,
    ObservationClock,
    StageStamps,
    Verdict,
    age_at,
    format_delta,
)
from latency.measure import measure_from_rows, worst_observation_age

UTC = timezone.utc


def _row(**kwargs):
    """A freshness row, exactly as ``read_freshness`` hands one over.

    Unknown keys raise rather than being carried along: a typo'd stamp name
    would otherwise leave the field at None and the assertion under test
    would pass or fail for a reason that has nothing to do with the code.
    """
    base = {
        "status": "success",
        "observed_at": None,
        "fetch_started_at": None,
        "fetch_completed_at": None,
        "stored_at": None,
        "last_attempt": None,
    }
    unknown = set(kwargs) - set(base)
    if unknown:
        raise TypeError(f"not a freshness column: {', '.join(sorted(unknown))}")
    base.update(kwargs)
    return base


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------


def test_every_class_has_an_objective_with_a_stated_basis():
    for latency_class in LatencyClass:
        objective = OBJECTIVES[latency_class]
        assert objective.latency_class is latency_class
        assert objective.acquisition_target > timedelta(0)
        assert objective.pipeline_target > timedelta(0)
        # A target with no basis is a number somebody made up.
        assert len(objective.basis) > 40


def test_every_layer_spec_declares_its_provider_delay_basis():
    for spec in LAYER_LATENCIES:
        assert spec.provider_delay >= timedelta(0)
        assert len(spec.provider_delay_basis) > 30, spec.layer


def test_layer_specs_cover_only_real_production_layers():
    from config import PRODUCTION_LAYER_KEYS

    for layer in LATENCY_CLASS_BY_LAYER:
        assert layer in PRODUCTION_LAYER_KEYS, layer


def test_the_five_required_classes_are_all_populated():
    """The brief named five things to set objectives for. All five must exist."""
    populated = {spec.latency_class for spec in LAYER_LATENCIES}
    assert populated == set(LatencyClass)


def test_end_to_end_target_is_the_sum_of_its_parts():
    objective = OBJECTIVES[LatencyClass.BOARD_PRICE]
    assert objective.end_to_end_target == (
        objective.acquisition_target + objective.pipeline_target
    )


def test_observation_clock_without_an_hour_is_day_granular():
    assert ObservationClock().granularity is Granularity.DAY
    assert ObservationClock().instant(datetime(2026, 8, 18).date()) is None


def test_observation_clock_resolves_a_session_close():
    clock = ObservationClock("America/Chicago", (13, 15))
    instant = clock.instant(datetime(2026, 8, 18).date())
    assert instant.astimezone(UTC) == datetime(2026, 8, 18, 18, 15, tzinfo=UTC)


@pytest.mark.parametrize(
    "delta,expected",
    [
        (timedelta(minutes=30), "30m"),
        (timedelta(hours=4, minutes=5), "4h 5m"),
        (timedelta(days=2, hours=3), "2d 3h"),
        (timedelta(hours=-6), "-6h 0m"),
        (None, "unknown"),
    ],
)
def test_format_delta(delta, expected):
    assert format_delta(delta) == expected


def test_negative_age_is_preserved_not_clamped():
    """Weather carries forecast rows; a negative age is the truth about them."""
    spec = LAYER_LATENCY_BY_KEY["weather"]
    m = LayerMeasurement(
        spec=spec,
        stamps=StageStamps(observed_at=datetime(2026, 8, 25, tzinfo=UTC)),
        granularity=Granularity.DAY,
    )
    age = age_at(m, datetime(2026, 8, 19, tzinfo=UTC))
    assert age < timedelta(0)
    assert format_delta(age).startswith("-")


# ---------------------------------------------------------------------------
# The decomposition: what is ours vs what is the provider's
# ---------------------------------------------------------------------------


def _prices_measurement(observed_at, fetch_started, fetch_completed, stored, published):
    return measure_from_rows(
        {
            "prices": _row(
                observed_at=observed_at,
                fetch_started_at=fetch_started,
                fetch_completed_at=fetch_completed,
                stored_at=stored,
            )
        },
        published_at=published,
        specs=(LAYER_LATENCY_BY_KEY["prices"],),
    )[0]


def test_post_settlement_run_meets_every_objective():
    """Monday 18 Aug settles 18:15 UTC; a 22:30 UTC refresh publishes by 22:45.

    This is the fast refresh's intended landing, and every leg of the chain
    is asserted separately so a regression in one cannot be masked by slack
    in another.
    """
    m = _prices_measurement(
        observed_at=datetime(2026, 8, 18, tzinfo=UTC),
        fetch_started=datetime(2026, 8, 18, 22, 30, tzinfo=UTC),
        fetch_completed=datetime(2026, 8, 18, 22, 32, tzinfo=UTC),
        stored=datetime(2026, 8, 18, 22, 33, tzinfo=UTC),
        published=datetime(2026, 8, 18, 22, 45, tzinfo=UTC),
    )
    assert m.stamps.observed_at == datetime(2026, 8, 18, 18, 15, tzinfo=UTC)
    assert m.acquisition == timedelta(hours=4, minutes=17)
    assert m.fetch_duration == timedelta(minutes=2)
    assert m.processing == timedelta(minutes=1)
    assert m.pipeline == timedelta(minutes=13)
    assert m.end_to_end == timedelta(hours=4, minutes=30)
    assert m.acquisition_verdict is Verdict.MEETS   # 4h17m against a 6h target
    assert m.pipeline_verdict is Verdict.MEETS      # 13m against a 25m target
    assert m.verdict is Verdict.MEETS


def test_a_late_scheduler_landing_breaches_the_board_objective():
    """The envelope's far end. GitHub's scheduler runs up to ~5h late.

    6h is chosen to cover the *observed* landing window, not to be
    unfailable: a landing well outside it must still register as a miss, or
    the objective is decoration.
    """
    m = _prices_measurement(
        observed_at=datetime(2026, 8, 18, tzinfo=UTC),
        fetch_started=datetime(2026, 8, 19, 1, 0, tzinfo=UTC),
        fetch_completed=datetime(2026, 8, 19, 1, 2, tzinfo=UTC),
        stored=datetime(2026, 8, 19, 1, 3, tzinfo=UTC),
        published=datetime(2026, 8, 19, 1, 15, tzinfo=UTC),
    )
    assert m.acquisition == timedelta(hours=6, minutes=47)
    assert m.acquisition_verdict is Verdict.BREACHES
    assert m.pipeline_verdict is Verdict.MEETS  # our own leg was fine
    assert m.verdict is Verdict.BREACHES


def test_cadence_wait_separates_our_delay_from_the_providers():
    """Acquisition minus the declared provider floor is what our schedule cost."""
    m = _prices_measurement(
        observed_at=datetime(2026, 8, 18, tzinfo=UTC),
        fetch_started=datetime(2026, 8, 18, 21, 15, tzinfo=UTC),
        fetch_completed=datetime(2026, 8, 18, 21, 15, tzinfo=UTC),
        stored=datetime(2026, 8, 18, 21, 16, tzinfo=UTC),
        published=datetime(2026, 8, 18, 21, 25, tzinfo=UTC),
    )
    assert m.acquisition == timedelta(hours=3)
    # Declared provider floor for Yahoo is 30 minutes.
    assert m.spec.provider_delay == timedelta(minutes=30)
    assert m.cadence_wait == timedelta(hours=2, minutes=30)


def test_cadence_wait_floors_at_zero_rather_than_going_negative():
    """A fetch faster than the declared floor attributes nothing to us."""
    m = _prices_measurement(
        observed_at=datetime(2026, 8, 18, tzinfo=UTC),
        fetch_started=datetime(2026, 8, 18, 18, 20, tzinfo=UTC),
        fetch_completed=datetime(2026, 8, 18, 18, 20, tzinfo=UTC),
        stored=datetime(2026, 8, 18, 18, 21, tzinfo=UTC),
        published=datetime(2026, 8, 18, 18, 30, tzinfo=UTC),
    )
    assert m.acquisition == timedelta(minutes=5)
    assert m.cadence_wait == timedelta(0)


def test_early_session_run_carries_the_previous_session():
    """The settlement guard's own consequence, measured.

    A build landing 13:53 UTC — the 2026-08-18 production run — cannot have
    today's close, because the guard drops the unfinished bar. So the newest
    observation is the *previous* session and the acquisition interval is a
    day long. That is correct behaviour with a real latency cost, and the
    measurement must show the cost rather than hide it behind a fresh
    last_success.
    """
    m = _prices_measurement(
        observed_at=datetime(2026, 8, 17, tzinfo=UTC),  # Monday's session
        fetch_started=datetime(2026, 8, 18, 13, 52, tzinfo=UTC),
        fetch_completed=datetime(2026, 8, 18, 13, 53, tzinfo=UTC),
        stored=datetime(2026, 8, 18, 13, 53, 30, tzinfo=UTC),
        published=datetime(2026, 8, 18, 14, 5, tzinfo=UTC),
    )
    assert m.acquisition > timedelta(hours=19)
    assert m.acquisition_verdict is Verdict.BREACHES
    assert m.pipeline_verdict is Verdict.MEETS  # our own leg was fine


def test_weekend_age_is_measured_from_fridays_settlement():
    m = measure_from_rows(
        {"prices": _row(observed_at=datetime(2026, 8, 21, tzinfo=UTC))},
        specs=(LAYER_LATENCY_BY_KEY["prices"],),
    )[0]
    sunday_noon = datetime(2026, 8, 23, 17, 0, tzinfo=UTC)
    # Friday 21 Aug settled 18:15 UTC; Sunday 17:00 UTC is 46h45m later.
    assert age_at(m, sunday_noon) == timedelta(hours=46, minutes=45)


def test_holiday_gap_reads_as_age_not_as_a_missing_measurement():
    """A closed venue publishes nothing; the chain stays measurable and just ages."""
    m = measure_from_rows(
        {
            "prices": _row(
                observed_at=datetime(2026, 12, 24, tzinfo=UTC),
                fetch_completed_at=datetime(2026, 12, 25, 22, 0, tzinfo=UTC),
                stored_at=datetime(2026, 12, 25, 22, 1, tzinfo=UTC),
            )
        },
        published_at=datetime(2026, 12, 25, 22, 15, tzinfo=UTC),
        specs=(LAYER_LATENCY_BY_KEY["prices"],),
    )[0]
    assert m.acquisition is not None
    assert m.pipeline == timedelta(minutes=15)      # fetch -> publicly readable
    assert m.publication == timedelta(minutes=14)   # stored -> publicly readable
    assert m.pipeline_verdict is Verdict.MEETS
    assert m.status == "success"


def test_stale_provider_keeps_its_status_and_its_measured_age():
    """A frozen upstream must show BOTH the stale verdict and the size of the hole."""
    m = measure_from_rows(
        {
            "safex": _row(
                status="stale",
                observed_at=datetime(2026, 8, 4, tzinfo=UTC),
                fetch_completed_at=datetime(2026, 8, 19, 13, 0, tzinfo=UTC),
                stored_at=datetime(2026, 8, 19, 13, 1, tzinfo=UTC),
            )
        },
        specs=(LAYER_LATENCY_BY_KEY["safex"],),
    )[0]
    assert m.status == "stale"
    assert m.acquisition > timedelta(days=14)
    assert m.acquisition_verdict is Verdict.BREACHES


def test_missing_stamps_are_unknown_never_meets():
    """The whole point: an unmeasured chain must not read as a fast one."""
    m = measure_from_rows(
        {"prices": _row(observed_at=datetime(2026, 8, 18, tzinfo=UTC))},
        specs=(LAYER_LATENCY_BY_KEY["prices"],),
    )[0]
    assert m.acquisition is None
    assert m.pipeline is None
    assert m.acquisition_verdict is Verdict.UNKNOWN
    assert m.verdict is Verdict.UNKNOWN


def test_unknown_outranks_meets_in_the_overall_verdict():
    spec = LAYER_LATENCY_BY_KEY["prices"]
    m = LayerMeasurement(
        spec=spec,
        stamps=StageStamps(
            observed_at=datetime(2026, 8, 18, 18, 15, tzinfo=UTC),
            fetch_completed_at=datetime(2026, 8, 18, 19, 0, tzinfo=UTC),
            # no published_at -> pipeline unknown
        ),
        granularity=Granularity.SESSION_CLOSE,
    )
    assert m.acquisition_verdict is Verdict.MEETS
    assert m.pipeline_verdict is Verdict.UNKNOWN
    assert m.verdict is Verdict.UNKNOWN


def test_a_layer_with_no_freshness_row_is_not_run_not_fresh():
    m = measure_from_rows({}, specs=(LAYER_LATENCY_BY_KEY["prices"],))[0]
    assert m.status == "not-run"
    assert m.verdict is Verdict.UNKNOWN


# ---------------------------------------------------------------------------
# Granularity honesty
# ---------------------------------------------------------------------------


def test_day_granular_layers_are_marked_and_use_end_of_day():
    """No declared hour means no invented hour."""
    m = measure_from_rows(
        {"cot": _row(observed_at=datetime(2026, 8, 11, tzinfo=UTC))},
        specs=(LAYER_LATENCY_BY_KEY["cot"],),
    )[0]
    assert m.granularity is Granularity.DAY
    assert m.stamps.observed_at.hour == 23


def test_session_close_layers_report_the_venue_hour():
    m = measure_from_rows(
        {"dce": _row(observed_at=datetime(2026, 8, 18, tzinfo=UTC))},
        specs=(LAYER_LATENCY_BY_KEY["dce"],),
    )[0]
    assert m.granularity is Granularity.SESSION_CLOSE
    # 15:00 Shanghai = 07:00 UTC
    assert m.stamps.observed_at == datetime(2026, 8, 18, 7, 0, tzinfo=UTC)


def test_stored_at_falls_back_to_last_attempt_for_legacy_rows():
    """Pre-instrumentation rows still yield a publication leg."""
    m = measure_from_rows(
        {
            "prices": _row(
                observed_at=datetime(2026, 8, 18, tzinfo=UTC),
                last_attempt=datetime(2026, 8, 18, 22, 0, tzinfo=UTC),
            )
        },
        published_at=datetime(2026, 8, 18, 22, 20, tzinfo=UTC),
        specs=(LAYER_LATENCY_BY_KEY["prices"],),
    )[0]
    assert m.publication == timedelta(minutes=20)
    # ...but acquisition stays unknown: last_attempt is not a fetch time.
    assert m.acquisition is None


# ---------------------------------------------------------------------------
# The masthead number
# ---------------------------------------------------------------------------


def test_worst_observation_age_takes_the_oldest_board_or_fx_leg():
    now = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    measurements = measure_from_rows(
        {
            "prices": _row(observed_at=datetime(2026, 8, 18, tzinfo=UTC)),
            "currencies": _row(observed_at=datetime(2026, 8, 14, tzinfo=UTC)),
            "weather": _row(observed_at=datetime(2026, 8, 25, tzinfo=UTC)),
        },
        specs=(
            LAYER_LATENCY_BY_KEY["prices"],
            LAYER_LATENCY_BY_KEY["currencies"],
            LAYER_LATENCY_BY_KEY["weather"],
        ),
    )
    worst = worst_observation_age(measurements, now)
    # The stale FX leg, not the fresh board and not the forecast weather.
    assert worst > timedelta(days=4)


def test_worst_observation_age_ignores_non_price_classes():
    now = datetime(2026, 8, 19, 12, 0, tzinfo=UTC)
    measurements = measure_from_rows(
        {"cot": _row(observed_at=datetime(2026, 1, 1, tzinfo=UTC))},
        specs=(LAYER_LATENCY_BY_KEY["cot"],),
    )
    assert worst_observation_age(measurements, now) is None
