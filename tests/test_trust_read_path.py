"""The per-dataset trusted-read cutover switch and its rollback."""

from __future__ import annotations

import pytest

from trust.read_path import (
    CBOT_BENCHMARK_DATASET_KEYS,
    TRUSTED_READ_ENV_VAR,
    TrustedReadSwitchError,
    load_trusted_read_switch,
    trusted_read_enabled,
)


def test_an_unset_variable_leaves_every_dataset_on_the_v1_path() -> None:
    switch = load_trusted_read_switch({})

    assert switch.any_enabled is False
    assert switch.enabled_for("cbot-soybean-named-contracts") is False
    assert switch.enabled_for_all(CBOT_BENCHMARK_DATASET_KEYS) is False


@pytest.mark.parametrize("value", ["", "none", "off", " , "])
def test_explicitly_empty_values_also_mean_nothing_is_cut_over(value: str) -> None:
    assert load_trusted_read_switch({TRUSTED_READ_ENV_VAR: value}).any_enabled is False


def test_cutover_is_per_dataset_and_moves_nothing_it_was_not_told_to() -> None:
    switch = load_trusted_read_switch({TRUSTED_READ_ENV_VAR: "cbot-soybean-named-contracts"})

    assert switch.enabled_for("cbot-soybean-named-contracts") is True
    assert switch.enabled_for("cbot-soybean-oil-named-contracts") is False
    assert switch.enabled_for("official-soy-fob") is False


def test_a_partial_soy_complex_cutover_is_not_enough_for_a_curve_read() -> None:
    partial = load_trusted_read_switch(
        {TRUSTED_READ_ENV_VAR: "cbot-soybean-named-contracts,cbot-soybean-oil-named-contracts"}
    )
    whole = load_trusted_read_switch({TRUSTED_READ_ENV_VAR: ",".join(CBOT_BENCHMARK_DATASET_KEYS)})

    # A crush struck from a trusted bean and a v1 oil is two provenances in one
    # number, so the soy complex moves together or not at all.
    assert partial.enabled_for_all(CBOT_BENCHMARK_DATASET_KEYS) is False
    assert whole.enabled_for_all(CBOT_BENCHMARK_DATASET_KEYS) is True


def test_a_dataset_the_registry_does_not_have_raises_rather_than_doing_nothing() -> None:
    with pytest.raises(TrustedReadSwitchError, match="cbot-soybean-namd-contracts"):
        load_trusted_read_switch({TRUSTED_READ_ENV_VAR: "cbot-soybean-namd-contracts"})


def test_there_is_no_switch_value_that_cuts_everything_over_at_once() -> None:
    with pytest.raises(TrustedReadSwitchError):
        load_trusted_read_switch({TRUSTED_READ_ENV_VAR: "all"})


def test_values_are_case_and_whitespace_insensitive() -> None:
    switch = load_trusted_read_switch({TRUSTED_READ_ENV_VAR: " CBOT-Soybean-Named-Contracts , "})

    assert switch.enabled_for("cbot-soybean-named-contracts") is True


def test_the_single_dataset_helper_reads_the_same_variable() -> None:
    assert trusted_read_enabled("official-soy-fob", {TRUSTED_READ_ENV_VAR: "official-soy-fob"}) is True
    assert trusted_read_enabled("official-soy-fob", {}) is False


def test_every_benchmark_key_the_switch_names_exists_in_the_registry() -> None:
    # The names in CBOT_BENCHMARK_DATASET_KEYS are the operator-facing contract
    # of the switch; a rename in the registry that missed them would make the
    # documented cutover command raise on the morning it was needed.
    switch = load_trusted_read_switch({TRUSTED_READ_ENV_VAR: ",".join(CBOT_BENCHMARK_DATASET_KEYS)})

    assert switch.dataset_keys == set(CBOT_BENCHMARK_DATASET_KEYS)
