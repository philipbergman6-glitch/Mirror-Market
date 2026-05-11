"""Shared retry-backoff helper for fetchers.

Single source of truth for the wait-between-retries formula so every
fetcher applies the same exponential + jitter policy.

Formula: ``min(RETRY_DELAY * 2**(attempt-1), 30) + uniform(0, 1)``.
``attempt`` is 1-indexed so the first retry waits roughly the base
``RETRY_DELAY``, the second waits twice that, etc., capped at 30s
to avoid pathologically long sleeps on the third or later attempt.
The jitter prevents synchronised retry storms across the 18-layer
pipeline.
"""

from __future__ import annotations

import logging
import random
import time

from config import RETRY_DELAY

logger = logging.getLogger(__name__)

_MAX_DELAY_SECONDS = 30.0


def retry_sleep(attempt: int) -> None:
    """Sleep before the next retry. ``attempt`` is the just-failed attempt number (1-indexed)."""
    base = RETRY_DELAY * (2 ** max(attempt - 1, 0))
    delay = min(base, _MAX_DELAY_SECONDS) + random.uniform(0, 1)
    logger.debug("Backing off %.2fs before retry (attempt %d)", delay, attempt)
    time.sleep(delay)
