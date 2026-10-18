"""Measure whether Yahoo re-serves a changed FX close after the London day end (B28 #502).

``FX_SESSION`` closes the bar labelled D at 00:00 London ending D with **zero
margin**, so the 20:07 UTC slot can store day D minutes after it closed.
Nobody has measured whether Yahoo finalises the daily bar at that instant or
re-serves a different Close shortly after. This script is the measurement; it
changes nothing in the pipeline.

Each sample downloads the four pairs through the same unguarded
``fetchers.yfinance.download_bars`` the pipeline uses, and appends the newest
few labelled bars per pair to a CSV (``.scratch/`` is gitignored). Compare the
Close recorded for one (pair, label) across sample times: if it never moves
after 00:05 London, a zero margin is honest; if it does, the margin is the
time after which it stopped moving.

    # One sample now:
    .venv/bin/python scripts/measure_fx_bar_finalisation.py

    # Start before London midnight; samples at each listed London time, then exits:
    .venv/bin/python scripts/measure_fx_bar_finalisation.py --at 00:05,00:30,01:00,02:00

    # Which closes changed between samples:
    .venv/bin/python scripts/measure_fx_bar_finalisation.py --summary

Needs the network; never imported by the pipeline or by tests' fetch paths.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time as time_module
from collections.abc import Callable, Iterable
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import CURRENCY_TICKERS, FX_SESSION_TIMEZONE  # noqa: E402
from fetchers._settlement import FX_SESSION, drop_unsettled_session  # noqa: E402

PAIRS = ("BRL/USD", "ZAR/USD", "CNY/USD", "INR/USD")
DEFAULT_OUTPUT = PROJECT_ROOT / ".scratch" / "fx_bar_finalisation.csv"
BARS_PER_SAMPLE = 3
FIELDS = (
    "sampled_at_utc",
    "sampled_at_london",
    "pair",
    "ticker",
    "label",
    "newest_settled",
    "open",
    "high",
    "low",
    "close",
)

Download = Callable[[str], pd.DataFrame]


def _default_download(ticker: str) -> pd.DataFrame:
    from fetchers.yfinance import download_bars  # imported late so --summary needs no network stack

    return download_bars(ticker, period="5d")


def sample(now: datetime, download: Download, pairs: Iterable[str] = PAIRS) -> list[dict[str, str]]:
    """One row per (pair, label) for the newest ``BARS_PER_SAMPLE`` labels.

    ``newest_settled`` marks the newest label the v1 guard would keep at
    ``now`` (``FX_SESSION``: London-day clock and weekday rule) — the bar
    whose Close this measures. A pair that returns nothing raises: a silent
    gap would read as "stable".
    """
    london = now.astimezone(ZoneInfo(FX_SESSION_TIMEZONE))
    rows: list[dict[str, str]] = []
    for pair in pairs:
        ticker = CURRENCY_TICKERS[pair]
        frame = download(ticker)
        if frame.empty:
            raise RuntimeError(f"{pair} ({ticker}): provider returned no bars at {now.isoformat()}")
        kept = drop_unsettled_session(frame, label=ticker, now=now, rule=FX_SESSION)
        index = pd.DatetimeIndex(frame.index)
        if index.tz is not None:
            index = index.tz_localize(None)
        settled = pd.DatetimeIndex(kept.index).max().date() if not kept.empty else None
        tail = frame.iloc[-BARS_PER_SAMPLE:]
        for position, label in enumerate(index[-BARS_PER_SAMPLE:]):
            bar = tail.iloc[position]
            rows.append(
                {
                    "sampled_at_utc": now.astimezone(timezone.utc).isoformat(timespec="seconds"),
                    "sampled_at_london": london.isoformat(timespec="seconds"),
                    "pair": pair,
                    "ticker": ticker,
                    "label": label.date().isoformat(),
                    "newest_settled": str(label.date() == settled),
                    "open": repr(float(bar["Open"])),
                    "high": repr(float(bar["High"])),
                    "low": repr(float(bar["Low"])),
                    "close": repr(float(bar["Close"])),
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


def summarise(rows: Iterable[dict[str, str]]) -> list[str]:
    """One line per (pair, label) sampled more than once: stable, or each change."""
    by_bar: dict[tuple[str, str], list[tuple[str, str]]] = {}
    for row in rows:
        by_bar.setdefault((row["pair"], row["label"]), []).append((row["sampled_at_london"], row["close"]))
    lines = []
    for (pair, label), samples in sorted(by_bar.items()):
        if len(samples) < 2:
            continue
        samples.sort()
        changes = [
            f"{before[1]} -> {after[1]} between {before[0]} and {after[0]}"
            for before, after in zip(samples, samples[1:], strict=False)
            if before[1] != after[1]
        ]
        verdict = "; ".join(changes) if changes else f"stable at {samples[0][1]}"
        lines.append(f"{pair} {label} ({len(samples)} samples, {samples[0][0]} .. {samples[-1][0]}): {verdict}")
    return lines


def _next_london(at: time, now: datetime) -> datetime:
    zone = ZoneInfo(FX_SESSION_TIMEZONE)
    local = now.astimezone(zone)
    target = datetime.combine(local.date(), at, tzinfo=zone)
    if target <= local:
        target = datetime.combine(local.date() + timedelta(days=1), at, tzinfo=zone)
    return target


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--at",
        help="comma-separated London HH:MM times; sleeps until each upcoming one and samples",
    )
    parser.add_argument("--summary", action="store_true", help="report changed closes and exit")
    args = parser.parse_args(argv)

    if args.summary:
        with args.output.open(newline="") as handle:
            for line in summarise(csv.DictReader(handle)):
                print(line)
        return 0

    if args.at:
        times = [time.fromisoformat(value.strip()) for value in args.at.split(",")]
        start = datetime.now(timezone.utc)
        targets = sorted(_next_london(at, start) for at in times)
    else:
        targets = [datetime.now(timezone.utc)]

    for target in targets:
        delay = (target - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            print(f"waiting until {target.isoformat()} ({delay / 60:.0f} min)", flush=True)
            time_module.sleep(delay)
        rows = sample(datetime.now(timezone.utc), _default_download)
        append(rows, args.output)
        for row in rows:
            if row["newest_settled"] == "True":
                print(f"{row['sampled_at_london']} {row['pair']} {row['label']} close={row['close']}", flush=True)
    print(f"appended to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
