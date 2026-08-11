"""Settlement guard for yfinance daily bars.

yfinance emits a row for the *current* session the moment trading starts,
so a pipeline run that lands mid-session reads an unfinished bar as if it
were a close. It self-heals on the next run (``INSERT OR REPLACE``), but
the dashboard published on day D carries day D's partial print labelled as
day D's close — the number a trader actually reads.

This module drops that row until the venue has settled. It is applied at
``fetchers.yfinance.fetch_one``, the single choke point every yfinance
frame flows through (Layer 1 prices, Layer 7 currencies, Layer 11 forward
curve contracts).

The guard is deliberately time-based rather than value-based: there is no
field on a Yahoo bar that says "settled", and a heuristic on volume or
range would be the same class of silent guess the guard exists to remove.
A dropped bar is a visible gap plus a WARNING; a stored partial is a wrong
number that looks right.

See ``config.SETTLEMENT_TIMEZONE`` / ``config.SETTLEMENT_CUTOFF_LOCAL`` for
the cutoff and the per-venue settlement times it clears.
"""

from __future__ import annotations

import logging
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from config import SETTLEMENT_CUTOFF_LOCAL, SETTLEMENT_TIMEZONE

logger = logging.getLogger(__name__)

_CUTOFF = time(*SETTLEMENT_CUTOFF_LOCAL)


def session_is_settled(now: datetime | None = None) -> bool:
    """
    Has the current venue session settled at ``now``?

    ``now`` defaults to the wall clock and may be naive (treated as UTC) or
    aware. The comparison is made in ``SETTLEMENT_TIMEZONE`` so US DST is
    handled by zoneinfo rather than a hardcoded UTC offset.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now.astimezone(ZoneInfo(SETTLEMENT_TIMEZONE)).time() >= _CUTOFF


def drop_unsettled_session(
    df: pd.DataFrame, label: str = "", now: datetime | None = None
) -> pd.DataFrame:
    """
    Drop the current session's bar when the venue has not settled yet.

    Parameters
    ----------
    df : pd.DataFrame
        Daily OHLCV frame from yfinance, indexed by session date.
    label : str
        Ticker or commodity name — prefixes the warning log.
    now : datetime, optional
        Injectable clock for tests. Defaults to the current UTC time.

    Returns
    -------
    pd.DataFrame
        A copy without the current session's row, or the input unchanged
        when the venue has settled or no row for the current session exists.
        The original is never mutated.
    """
    if df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return df

    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if session_is_settled(now):
        return df

    # The session date is the venue's local date, not the UTC date: a run
    # landing at 00:30 UTC Saturday is still Friday evening in Chicago, and
    # Friday's bar is settled.
    local_today = pd.Timestamp(now.astimezone(ZoneInfo(SETTLEMENT_TIMEZONE)).date())

    # A tz-aware index has its tz dropped, not converted: the label on a
    # daily bar is already the session date, and pipeline/store.py stores it
    # the same way (strftime on the value as given). Converting would shift
    # a UTC-midnight label back a day and drop the wrong row.
    index = df.index
    if index.tz is not None:
        index = index.tz_localize(None)
    unsettled = index.normalize() == local_today
    if not unsettled.any():
        return df

    prefix = f"[{label}] " if label else ""
    logger.warning(
        "%sDropping unsettled %s bar — run landed before the %02d:%02d %s "
        "settlement cutoff. Today's close will appear on the next run.",
        prefix, local_today.date(), _CUTOFF.hour, _CUTOFF.minute, SETTLEMENT_TIMEZONE,
    )
    return df.loc[~unsettled]
