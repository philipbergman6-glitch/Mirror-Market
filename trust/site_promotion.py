"""Promotion contract for the generated v1 site candidate.

This is the bridge into the existing trusted-edition vocabulary while v2
dataset contracts are still being piloted. It verifies a private candidate;
the workflow promotes that directory only when this verdict is green.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import PurePosixPath
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

from app.markets import load_markets
from config import LAYER_MAX_DATA_AGE_DAYS, PRODUCTION_LAYERS

CORE_BENCHMARKS = ("Soybeans", "Soybean Oil", "Soybean Meal")


def expected_site_paths() -> tuple[str, ...]:
    return (
        "index.html",
        "players.html",
        # The Phase 2 origin comparison. In the contract rather than optional
        # for the same reason every market URL is: the masthead links to it from
        # every page, so a run that failed to build it would ship a site whose
        # nav 404s.
        "origins.html",
        *(market.url for market in load_markets().values()),
    )


@dataclass(frozen=True)
class SitePromotionVerdict:
    verified: bool
    failures: tuple[str, ...]
    generated_at: datetime | None = None


def _local_target(source: str, href: str) -> str | None:
    parsed = urlsplit(href)
    if parsed.scheme or parsed.netloc or href.startswith(("#", "data:", "mailto:")):
        return None
    raw = parsed.path
    if not raw:
        return source
    target = PurePosixPath(source).parent / raw
    parts: list[str] = []
    for part in target.parts:
        if part == "..":
            if parts:
                parts.pop()
        elif part not in ("", "."):
            parts.append(part)
    return "/".join(parts) or "index.html"


def verify_site_candidate(
    pages: dict[str, str],
    *,
    today: date | None = None,
    now: datetime | None = None,
) -> SitePromotionVerdict:
    """Verify a complete generated-site mapping keyed by relative URL."""
    now = now or datetime.now(timezone.utc)
    today = today or now.date()
    failures: list[str] = []
    expected = expected_site_paths()
    missing = [path for path in expected if path not in pages]
    failures.extend(f"missing expected URL: {path}" for path in missing)

    parsed_pages: dict[str, BeautifulSoup] = {}
    generated_values: list[datetime] = []
    for path in expected:
        html = pages.get(path)
        if html is None:
            continue
        soup = BeautifulSoup(html, "html.parser")
        parsed_pages[path] = soup
        if soup.select_one(".tomb") or "could not be generated today" in soup.get_text(" "):
            failures.append(f"unexpected tombstone: {path}")

        stamp = soup.select_one('meta[name="mirror-market-generated-at"]')
        raw_stamp = stamp.get("content", "") if stamp else ""
        try:
            generated = datetime.fromisoformat(raw_stamp)
            if generated.tzinfo is None:
                raise ValueError("timezone missing")
            generated_values.append(generated)
        except (TypeError, ValueError):
            failures.append(f"invalid generation timestamp: {path}")

        for link in soup.select("a[href]"):
            target = _local_target(path, str(link.get("href", "")))
            if target is not None and target not in pages:
                failures.append(f"broken internal link: {path} -> {target}")

    index = parsed_pages.get("index.html")
    if index is not None:
        briefing = index.select_one("#briefing .briefing")
        if briefing is None or not briefing.get_text(strip=True):
            failures.append("daily briefing is absent")
        if "No briefing data" in index.get_text(" "):
            failures.append("daily briefing fallback is visible")

        expected_count = len(PRODUCTION_LAYERS)
        count_meta = index.select_one('meta[name="mirror-market-layer-count"]')
        try:
            rendered_count = int(count_meta.get("content", "")) if count_meta else -1
        except ValueError:
            rendered_count = -1
        freshness_count = len(index.select("tr[data-layer]"))
        if rendered_count != expected_count or freshness_count != expected_count:
            failures.append(
                "source/layer count mismatch: "
                f"catalog={expected_count}, metadata={rendered_count}, health={freshness_count}"
            )

        benchmark_dates: dict[str, date] = {}
        for name in CORE_BENCHMARKS:
            card = index.select_one(f'[data-benchmark="{name}"]')
            raw = card.get("data-as-of", "") if card else ""
            try:
                observed = date.fromisoformat(raw)
                benchmark_dates[name] = observed
                age = (today - observed).days
                if age < 0 or age > LAYER_MAX_DATA_AGE_DAYS["prices"]:
                    failures.append(f"benchmark outside cadence: {name} as of {raw}")
            except (TypeError, ValueError):
                failures.append(f"benchmark observation timestamp missing: {name}")

        crush = index.select_one('[data-derived="crush"]')
        crush_date = crush.get("data-as-of", "") if crush else ""
        if crush is None or crush.get("data-aligned") != "true":
            failures.append("required crush calculation lacks aligned eligible inputs")
        elif {value.isoformat() for value in benchmark_dates.values()} != {crush_date}:
            failures.append("crush observation date is not aligned with core benchmark inputs")

    generated_at = min(generated_values) if generated_values else None
    if generated_values and (
        max(generated_values) > now + timedelta(minutes=5)
        or min(generated_values) < now - timedelta(hours=6)
    ):
        failures.append("generation timestamp is outside the promotion window")
    return SitePromotionVerdict(not failures, tuple(dict.fromkeys(failures)), generated_at)
