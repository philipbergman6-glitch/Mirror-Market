"""The per-dataset cutover switch, and the rollback it exists to make cheap.

Dual-write is temporary; the read path is the part that is dangerous to change.
This module is the one place that answers "does this dataset serve analytics
from the trusted ledger, or from the v1 tables?", so a cutover is one
environment variable and a rollback is unsetting it — not a revert, not a
redeploy of different code, and not a decision made independently by three
callers that can drift.

Three rules:

- **Default off.** An unset variable means every dataset reads v1. A switch
  that defaulted on would make the safe state the one you have to remember.
- **Per dataset, never global.** Cutover is earned by one dataset passing its
  own reconciliation. ``MIRROR_TRUSTED_READ_DATASETS=cbot-soybean-named-contracts``
  moves exactly that dataset and nothing else. There is deliberately no
  ``all``.
- **An unknown name is an error, not a no-op.** A typo in the variable would
  otherwise read as "still on v1" — the switch silently doing nothing while the
  operator believes the cutover happened. Unknown names raise.

The variable takes a comma-separated list of registry dataset *keys*::

    MIRROR_TRUSTED_READ_DATASETS=cbot-soybean-named-contracts,cbot-soybean-oil-named-contracts

An empty value, ``none``, or an unset variable all mean "nothing is cut over".
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from trust.registry import PILOT_REGISTRY

#: The environment variable read by every consumer.
TRUSTED_READ_ENV_VAR = "MIRROR_TRUSTED_READ_DATASETS"

#: Values that explicitly mean "nothing".
_EMPTY_VALUES = frozenset({"", "none", "off"})

#: The three CBOT named-contract datasets, as one name a caller can pass.
CBOT_BENCHMARK_DATASET_KEYS: tuple[str, ...] = (
    "cbot-soybean-named-contracts",
    "cbot-soybean-meal-named-contracts",
    "cbot-soybean-oil-named-contracts",
)

#: The four conversion-critical FX datasets, likewise. Named together because
#: they are cut over together or not at all: a landed cost struck from a
#: trusted Real and a v1 Rand would be two provenances in one comparison.
REQUIRED_FX_DATASET_KEYS: tuple[str, ...] = (
    "fx-brl-usd",
    "fx-cny-usd",
    "fx-inr-usd",
    "fx-zar-usd",
)


class TrustedReadSwitchError(ValueError):
    """The switch names a dataset the registry does not have."""


@dataclass(frozen=True)
class TrustedReadSwitch:
    """Which datasets are cut over to trusted reads in this process."""

    dataset_keys: frozenset[str]

    def __contains__(self, dataset_key: object) -> bool:
        return isinstance(dataset_key, str) and dataset_key in self.dataset_keys

    def enabled_for(self, dataset_key: str) -> bool:
        return dataset_key in self.dataset_keys

    def enabled_for_all(self, dataset_keys: tuple[str, ...]) -> bool:
        """True only when *every* named dataset is cut over.

        A curve read across the three soy legs must not take beans from the
        ledger and oil from ``forward_curve``: the crush struck across them
        would be two different provenances in one number.
        """
        return bool(dataset_keys) and all(key in self.dataset_keys for key in dataset_keys)

    @property
    def any_enabled(self) -> bool:
        return bool(self.dataset_keys)


def load_trusted_read_switch(env: Mapping[str, str] | None = None) -> TrustedReadSwitch:
    """Read the switch, refusing a dataset key the registry does not define."""

    environment = os.environ if env is None else env
    raw = environment.get(TRUSTED_READ_ENV_VAR, "")
    tokens = [token.strip().lower() for token in raw.split(",")]
    keys = {token for token in tokens if token not in _EMPTY_VALUES}
    known = {contract.dataset.key for contract in PILOT_REGISTRY.datasets}
    unknown = sorted(keys - known)
    if unknown:
        raise TrustedReadSwitchError(
            f"{TRUSTED_READ_ENV_VAR} names datasets that are not in the trust registry: "
            f"{', '.join(unknown)} (known: {', '.join(sorted(known))})"
        )
    return TrustedReadSwitch(dataset_keys=frozenset(keys))


def trusted_read_enabled(dataset_key: str, env: Mapping[str, str] | None = None) -> bool:
    """Convenience for a single-dataset caller."""

    return load_trusted_read_switch(env).enabled_for(dataset_key)


__all__ = [
    "CBOT_BENCHMARK_DATASET_KEYS",
    "REQUIRED_FX_DATASET_KEYS",
    "TRUSTED_READ_ENV_VAR",
    "TrustedReadSwitch",
    "TrustedReadSwitchError",
    "load_trusted_read_switch",
    "trusted_read_enabled",
]
