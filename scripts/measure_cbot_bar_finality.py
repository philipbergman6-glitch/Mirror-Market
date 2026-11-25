"""Measure when Yahoo's CBOT daily bar stops changing around New York midnight (#622).

B47 (#622) set the settlement guard to keep Yahoo's CBOT bar labelled D only
from 00:00 New York on D+1 (``config.EXCHANGE_BAR_*``). That instant was
*inferred* from committed captures, and few of them landed in the first
minutes after midnight. The owner's decision on #622 therefore fires the
board slot at 01:00 New York, not 00:15, until a week of samples taken
across the boundary shows the bar final within seconds of 00:00 every night.
This script is that measurement; it changes nothing in the pipeline.

Each sample downloads ZS=F, ZL=F and ZM=F through the same unguarded
``fetchers.yfinance.download_bars`` the pipeline uses and appends the newest
few labelled bars per ticker, OHLCV, to a CSV (``.scratch/`` is gitignored).
Each row carries its offset from that label's boundary — 00:00 New York on
the label's next day — so ``--summary`` can say, per bar, whether its Close
moved *after* the boundary, and by when it had stopped moving.

    # One sample now:
    .venv/bin/python scripts/measure_cbot_bar_finality.py

    # The week the owner runs (keep the Mac awake; samples every night at
    # these New York times, then exits):
    caffeinate -i .venv/bin/python scripts/measure_cbot_bar_finality.py --nights 7

    # Which closes changed, relative to the boundary, and the slot verdict:
    .venv/bin/python scripts/measure_cbot_bar_finality.py --summary

The decision rule (owner, #622): move the board trigger to 00:15 New York only
if no boundary bar's Close changes after +00:00 on any night of the week.

Needs the network; never imported by the pipeline or by tests' fetch paths.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time as time_module
from collections.abc import Callable, Iterable
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import (  # noqa: E402
    EXCHANGE_BAR_CLOSE_DAY_OFFSET,
    EXCHANGE_BAR_CLOSE_LOCAL,
    EXCHANGE_BAR_TIMEZONE,
)
from fetchers._settlement import EXCHANGE_SESSION, drop_unsettled_session  # noqa: E402

TICKERS = ("ZS=F", "ZL=F", "ZM=F")
DEFAULT_OUTPUT = PROJECT_ROOT / ".scratch" / "cbot_bar_finality.csv"
DEFAULT_TIMES = "23:50,00:00,00:05,00:15,00:30,01:00"
CANDIDATE_SLOT = timedelta(minutes=15)  # the 00:15 the sampler is deciding on
BARS_PER_SAMPLE = 3
FIELDS = (
    "sampled_at_utc",
    "sampled_at_new_york",
    "ticker",
    "label",
    "boundary_new_york",
    "offset_minutes",
    "final_by_rule",
    "open",
    "high",
    "low",
    "close",
    "volume",
)

Download = Callable[[str], pd.DataFrame]
_NY = ZoneInfo(EXCHANGE_BAR_TIMEZONE)


def _default_download(ticker: str) -> pd.DataFrame:
    from fetchers.yfinance import download_bars  # imported late so --summary needs no network stack

    return download_bars(ticker, period="5d")


def boundary(label: date) -> datetime:
    """The instant the guard treats the bar labelled ``label`` as final."""
    return datetime.combine(
        label + timedelta(days=EXCHANGE_BAR_CLOSE_DAY_OFFSET),
        time(*EXCHANGE_BAR_CLOSE_LOCAL),
        tzinfo=_NY,
    )


def sample(now: datetime, download: Download, tickers: Iterable[str] = TICKERS) -> list[dict[str, str]]:
    """One row per (ticker, label) for the newest ``BARS_PER_SAMPLE`` labels.

    ``offset_minutes`` is the sample time minus that label's boundary:
    negative before 00:00 New York on D+1, positive after. ``final_by_rule``
    is what the guard would keep at ``now``. A ticker that returns nothing
    raises: a silent gap would read as "stable".
    """
    if now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    local = now.astimezone(_NY)
    rows: list[dict[str, str]] = []
    for ticker in tickers:
        frame = download(ticker)
        if frame.empty:
            raise RuntimeError(f"{ticker}: provider returned no bars at {now.isoformat()}")
        for column in ("Open", "High", "Low", "Close"):
            if column not in frame.columns:
                raise RuntimeError(f"{ticker}: no {column} column at {now.isoformat()}")
        kept = drop_unsettled_session(frame, label=ticker, now=now, rule=EXCHANGE_SESSION)
        index = pd.DatetimeIndex(frame.index)
        if index.tz is not None:
            index = index.tz_localize(None)
        settled = pd.DatetimeIndex(kept.index).max().date() if not kept.empty else None
        tail = frame.iloc[-BARS_PER_SAMPLE:]
        for position, stamp in enumerate(index[-BARS_PER_SAMPLE:]):
            bar = tail.iloc[position]
            label = stamp.date()
            edge = boundary(label)
            volume = bar["Volume"] if "Volume" in frame.columns else None
            rows.append(
                {
                    "sampled_at_utc": now.astimezone(timezone.utc).isoformat(timespec="seconds"),
                    "sampled_at_new_york": local.isoformat(timespec="seconds"),
                    "ticker": ticker,
                    "label": label.isoformat(),
                    "boundary_new_york": edge.isoformat(timespec="minutes"),
                    "offset_minutes": f"{(now - edge).total_seconds() / 60:.1f}",
                    "final_by_rule": str(settled is not None and label <= settled),
                    "open": repr(float(bar["Open"])),
                    "high": repr(float(bar["High"])),
                    "low": repr(float(bar["Low"])),
                    "close": repr(float(bar["Close"])),
                    # Blank, not 0, when the provider sent no volume (invariant 2).
                    "volume": "" if volume is None or volume != volume else repr(float(volume)),
                }
            )
    return rows


def append(rows: list[dict[str, str]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    new = not output.exists()
    with output.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS)
        if new:
            writer.writeheader()
        writer.writerows(rows)


def _offset(minutes: float) -> str:
    sign = "-" if minutes < 0 else "+"
    whole = int(round(abs(minutes)))
    return f"{sign}{whole // 60:02d}:{whole % 60:02d}"


def summarise(rows: Iterable[dict[str, str]]) -> list[str]:
    """Per (ticker, label) sampled on both sides of its boundary, then a verdict.

    For each bar: every Close change with the offsets it happened between, and
    ``final by`` — the earliest sampled offset from which every later sample
    equals the last one. The closing verdict says whether any bar's Close
    changed after +00:00, and whether a slot at +00:15 would have read the
    final value on every bar sampled.
    """
    by_bar: dict[tuple[str, str], list[tuple[float, str]]] = {}
    for row in rows:
        by_bar.setdefault((row["ticker"], row["label"]), []).append(
            (float(row["offset_minutes"]), row["close"])
        )
    lines: list[str] = []
    straddling = 0
    post_boundary_changes: list[str] = []
    slot_misreads: list[str] = []
    for (ticker, label), samples in sorted(by_bar.items()):
        samples.sort()
        before = [s for s in samples if s[0] < 0]
        after = [s for s in samples if s[0] >= 0]
        if not before or not after:
            continue
        straddling += 1
        final = samples[-1][1]
        final_by = samples[-1][0]
        for offset, close in reversed(samples):
            if close != final:
                break
            final_by = offset
        changes = []
        for (o1, c1), (o2, c2) in zip(samples, samples[1:], strict=False):
            if c1 == c2:
                continue
            changes.append(f"{c1} -> {c2} between {_offset(o1)} and {_offset(o2)}")
            if o2 > 0:
                post_boundary_changes.append(f"{ticker} {label} ({_offset(o1)}..{_offset(o2)})")
        at_slot = [c for o, c in samples if o >= CANDIDATE_SLOT.total_seconds() / 60]
        if at_slot and at_slot[0] != final:
            slot_misreads.append(f"{ticker} {label}")
        verdict = "; ".join(changes) if changes else f"stable at {final}"
        lines.append(
            f"{ticker} {label} ({len(samples)} samples, {_offset(samples[0][0])}..{_offset(samples[-1][0])}): "
            f"{verdict} — final by {_offset(final_by)}"
        )
    if not straddling:
        lines.append("VERDICT: no bar was sampled on both sides of its boundary yet.")
        return lines
    if post_boundary_changes:
        lines.append(
            f"VERDICT: {len(post_boundary_changes)} Close change(s) after +00:00 over {straddling} "
            f"boundary bar(s): {', '.join(post_boundary_changes)}. Midnight is NOT proven final."
        )
    else:
        lines.append(
            f"VERDICT: no Close changed after +00:00 over {straddling} boundary bar(s)."
        )
    if slot_misreads:
        lines.append(f"A slot at +00:15 would have read a non-final Close for: {', '.join(slot_misreads)}.")
    else:
        lines.append("A slot at +00:15 would have read the final Close on every boundary bar sampled.")
    return lines


def schedule(times: list[time], nights: int, start: datetime) -> list[datetime]:
    """Every sample instant for ``nights`` nights, in order, from ``start``.

    A night is the run of listed times that straddles one New York midnight:
    23:50 belongs with the 00:00..01:00 that follow it, never with the ones
    before it.
    """
    if nights < 1:
        raise ValueError("nights must be at least 1")
    evening = sorted(t for t in times if t >= time(12, 0))
    morning = sorted(t for t in times if t < time(12, 0))
    local = start.astimezone(_NY)
    groups: list[list[datetime]] = []
    for k in range(nights + 2):
        day = local.date() + timedelta(days=k)  # the midnight that begins ``day``
        group = [datetime.combine(day - timedelta(days=1), t, tzinfo=_NY) for t in evening]
        group += [datetime.combine(day, t, tzinfo=_NY) for t in morning]
        group = sorted(t for t in group if t > start)
        if group:
            groups.append(group)
        if len(groups) == nights:
            break
    return [t for group in groups for t in group]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--at",
        default=None,
        help=f"comma-separated New York HH:MM times (default with --nights: {DEFAULT_TIMES})",
    )
    parser.add_argument("--nights", type=int, help="sample this many New York midnights, then exit")
    parser.add_argument("--summary", action="store_true", help="report changed closes and the verdict")
    args = parser.parse_args(argv)

    if args.summary:
        with args.output.open(newline="") as handle:
            for line in summarise(csv.DictReader(handle)):
                print(line)
        return 0

    start = datetime.now(timezone.utc)
    if args.at or args.nights:
        times = [time.fromisoformat(v.strip()) for v in (args.at or DEFAULT_TIMES).split(",")]
        targets = schedule(times, args.nights or 1, start)
    else:
        targets = [start]

    for target in targets:
        delay = (target - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            print(f"waiting until {target.isoformat()} ({delay / 60:.0f} min)", flush=True)
            time_module.sleep(delay)
        try:
            rows = sample(datetime.now(timezone.utc), _default_download)
        except Exception as exc:  # noqa: BLE001 — one failed night must not end the week
            # Loud, and recorded nowhere as data: a missing sample is a gap the
            # summary shows by its absence, never a "stable" reading.
            print(f"SAMPLE FAILED at {datetime.now(timezone.utc).isoformat()}: {exc}", file=sys.stderr, flush=True)
            continue
        append(rows, args.output)
        for row in rows:
            if -60 <= float(row["offset_minutes"]) <= 120:
                print(
                    f"{row['sampled_at_new_york']} {row['ticker']} {row['label']} "
                    f"({_offset(float(row['offset_minutes']))}) close={row['close']}",
                    flush=True,
                )
    print(f"appended to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
