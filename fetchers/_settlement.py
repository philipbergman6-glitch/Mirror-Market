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

Two rules, one shape
--------------------
A ``SessionRule`` answers exactly one question: **which session date is the
newest one that has finished?** Every row labelled after that answer is an
unfinished bar and is dropped. Both rules here are the same function over a
different (timezone, close-time) pair:

``EXCHANGE_SESSION``
    CME/CBOT/ICE futures. ``config.SETTLEMENT_CUTOFF_LOCAL`` in
    ``config.SETTLEMENT_TIMEZONE`` — one cutoff clearing the latest venue
    settlement we pull, expressed in venue-local time so US DST is handled
    by zoneinfo rather than a hardcoded offset.

``FX_SESSION``
    Spot FX. There is no settlement at all: the market runs continuously
    from Sunday 17:00 New York to Friday 17:00, and Yahoo labels the bar
    that *closes* at 17:00 on day D with day D's date.

Splitting them is not tidiness. The guard used to ask one question —
"has Chicago settled?" — and answer it for FX too, which is wrong in both
directions and was **measured wrong live on 2026-08-19 at 03:45 UTC**
(22:45 Chicago, 23:45 New York): ``BRL=X`` returned a bar labelled
2026-08-19 whose High equalled its Open and whose Low equalled its Close —
an FX day less than four hours old. Chicago local time was past 14:30, so
the old guard declared the session settled and stored that partial bar as
the day's FX close. Every ``home_per_mt`` leg on the site converts at "that
row's own date", so a wrong FX close is a wrong landed cost on every
physical origin, not merely a wrong FX cell.

The old rule also failed on the futures side for the same underlying
reason. It dropped rows *equal to* the Chicago local date, so once the
overnight session opened (19:00 CT, carrying the **next** trade date) the
bar it produced was labelled D+1, compared unequal to D, and survived.
Asking for the newest finished session instead of the current local date
closes both holes with one comparison.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd

from config import (
    FX_SESSION_CLOSE_LOCAL,
    FX_SESSION_TIMEZONE,
    SETTLEMENT_CUTOFF_LOCAL,
    SETTLEMENT_TIMEZONE,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionRule:
    """When a venue's daily bar labelled D stops being an unfinished bar.

    ``name`` appears in the WARNING a dropped bar logs, so a reader can tell
    which rule made the call without opening this file.
    """

    name: str
    timezone: str
    close_local: tuple[int, int]

    @property
    def close_time(self) -> time:
        return time(*self.close_local)

    def local_now(self, now: datetime) -> datetime:
        return now.astimezone(ZoneInfo(self.timezone))

    def last_settled_session(self, now: datetime) -> date:
        """The newest session date whose bar has finished at ``now``.

        Today's bar counts once the venue's close has passed; before that
        the newest finished bar is the one labelled the previous calendar
        day. Calendar, not business, day: a weekend or holiday simply has
        no row to drop, so walking back over them would buy nothing and
        would need a holiday calendar the guard has no business owning.
        """
        local = self.local_now(now)
        if local.time() >= self.close_time:
            return local.date()
        return local.date() - timedelta(days=1)


# CME/CBOT/ICE futures. See config.SETTLEMENT_CUTOFF_LOCAL for the per-venue
# settlement times this one cutoff clears.
EXCHANGE_SESSION = SessionRule(
    name="exchange settlement",
    timezone=SETTLEMENT_TIMEZONE,
    close_local=SETTLEMENT_CUTOFF_LOCAL,
)

# Spot FX. Not a settlement — the 17:00 New York rollover that ends the bar
# Yahoo labels with that day's date.
FX_SESSION = SessionRule(
    name="FX 17:00 New York rollover",
    timezone=FX_SESSION_TIMEZONE,
    close_local=FX_SESSION_CLOSE_LOCAL,
)


def _as_aware(now: datetime | None) -> datetime:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return now


def session_is_settled(now: datetime | None = None) -> bool:
    """Has the current *exchange* session settled at ``now``?

    Kept because it is the question the daily schedule is reasoned about in
    (``.github/workflows/deploy-dashboard.yml``) and because the fast
    refresh path asks it to decide whether a board price can move at all.
    It is deliberately exchange-only: FX has no settlement, so there is no
    honest boolean of this shape for it.

    ``now`` defaults to the wall clock and may be naive (treated as UTC) or
    aware. The comparison is made in ``SETTLEMENT_TIMEZONE`` so US DST is
    handled by zoneinfo rather than a hardcoded UTC offset.
    """
    now = _as_aware(now)
    return EXCHANGE_SESSION.local_now(now).time() >= EXCHANGE_SESSION.close_time


def drop_unsettled_session(
    df: pd.DataFrame,
    label: str = "",
    now: datetime | None = None,
    rule: SessionRule = EXCHANGE_SESSION,
) -> pd.DataFrame:
    """Drop every row whose session has not finished at ``now``.

    Parameters
    ----------
    df : pd.DataFrame
        Daily OHLCV frame from yfinance, indexed by session date.
    label : str
        Ticker or commodity name — prefixes the warning log.
    now : datetime, optional
        Injectable clock for tests. Defaults to the current UTC time.
    rule : SessionRule
        Which venue clock decides. Defaults to the exchange settlement rule;
        ``fetchers.yfinance.fetch_currencies`` passes ``FX_SESSION``.

    Returns
    -------
    pd.DataFrame
        A copy without any unfinished session's row, or the input unchanged
        when every row is complete. The original is never mutated.
    """
    if df.empty or not isinstance(df.index, pd.DatetimeIndex):
        return df

    now = _as_aware(now)
    cutoff = pd.Timestamp(rule.last_settled_session(now))

    # A tz-aware index has its tz dropped, not converted: the label on a
    # daily bar is already the session date, and pipeline/store.py stores it
    # the same way (strftime on the value as given). Converting would shift
    # a UTC-midnight label back a day and drop the wrong row.
    index = df.index
    if index.tz is not None:
        index = index.tz_localize(None)
    unsettled = index.normalize() > cutoff
    if not unsettled.any():
        return df

    prefix = f"[{label}] " if label else ""
    logger.warning(
        "%sDropping %d unsettled bar(s) dated after %s — the newest finished "
        "session under the %s rule (%02d:%02d %s). Those sessions will appear "
        "on a later run.",
        prefix,
        int(unsettled.sum()),
        cutoff.date(),
        rule.name,
        rule.close_time.hour,
        rule.close_time.minute,
        rule.timezone,
    )
    return df.loc[~unsettled]
