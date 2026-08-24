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
from pricing.policy import scan

CORE_BENCHMARKS = ("Soybeans", "Soybean Oil", "Soybean Meal")

# Published files that are NOT pages. They are uploaded with the site and may
# be linked from it, but they carry no <head>, no generation meta and no links
# of their own — so they belong in the link allow-list rather than in
# `expected_site_paths`, which is crawled and timestamp-checked as HTML. The
# masthead links the manifest so a reader can check the age of what they are
# pricing off; without this entry that link reads as broken and blocks every
# promotion.
PUBLISHED_ASSETS = (
    "manifest.json",
    # The vendored chart renderer (#332): TradingView's open-source
    # lightweight-charts bundle, copied from app/assets/ by generate_site.
    # It draws the workstation's contract-row chart from data already in the
    # page — a candidate without it would publish charts that silently fall
    # back to the plain SVG, so its absence fails promotion rather than
    # shipping quietly degraded. The licence file rides with it because
    # Apache-2.0 redistribution keeps the licence text alongside.
    "assets/lightweight-charts.standalone.production.js",
    "assets/LICENSE.lightweight-charts",
)


def expected_site_paths() -> tuple[str, ...]:
    return (
        "index.html",
        "players.html",
        # The Phase 2 origin comparison. In the contract rather than optional
        # for the same reason every market URL is: the masthead links to it from
        # every page, so a run that failed to build it would ship a site whose
        # nav 404s.
        "origins.html",
        # The Phase 3 futures workstation, in the contract for exactly the same
        # reason as origins above: the masthead links to it from every page.
        "workstation.html",
        # The Phase 4 opportunity board — the PUBLIC edition. Same reason again:
        # the masthead links to it from every page.
        #
        # The private edition is deliberately NOT here and never will be. It is
        # written outside docs/ (config.OPPORTUNITY_PRIVATE_OUTPUT_DIR), so the
        # promotion contract cannot see it and the Pages upload cannot carry it.
        # A private edition that appeared in this tuple would be a private
        # edition somebody eventually published.
        "opportunities.html",
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
    assets: set[str] | None = None,
) -> SitePromotionVerdict:
    """Verify a complete generated-site mapping keyed by relative URL.

    ``assets`` is the set of non-page published files the caller found (see
    ``PUBLISHED_ASSETS``). ``None`` means the caller did not look, and the
    check is skipped — the pre-existing callers that pass pages alone keep
    their exact behaviour. Passing a set turns the link allow-list into a
    real check: an asset that is linked but absent is a 404 on a live page,
    which the allow-list would otherwise hide.
    """
    now = now or datetime.now(timezone.utc)
    today = today or now.date()
    failures: list[str] = []
    expected = expected_site_paths()
    missing = [path for path in expected if path not in pages]
    failures.extend(f"missing expected URL: {path}" for path in missing)
    if assets is not None:
        failures.extend(
            f"missing published asset: {asset}"
            for asset in PUBLISHED_ASSETS
            if asset not in assets
        )

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

        # The semantic contract, at the gate rather than only in CI. A page
        # that claims a settlement, an official close, an executable price, a
        # firm offer or a traded price no source supports is a *worse* edition
        # to publish than a tombstoned one: it is wrong and it looks right.
        # The catalogue lives in `pricing.policy` so this check, the contract
        # tests and any future surface all read one list. No price type is
        # passed: nothing on a public page is a proven settlement or an
        # attested statement, and a denial in the same sentence still passes.
        for violation in scan(html, surface=path):
            failures.append(f"misleading claim: {violation.describe()}")

        for link in soup.select("a[href]"):
            target = _local_target(path, str(link.get("href", "")))
            if target is not None and target not in pages and target not in PUBLISHED_ASSETS:
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


# ---------------------------------------------------------------------------
# The second gate: a candidate must not know LESS than what it replaces
# ---------------------------------------------------------------------------
#
# ``verify_site_candidate`` above asks whether a candidate is internally
# sound. That was sufficient while there was one build a day rebuilding
# everything from an empty database: a candidate that generated at all had
# just fetched every layer.
#
# A price-only refresh breaks that assumption, and breaks it silently. Its
# database is seeded from the committed history CSVs, which cover the
# snapshot-only tables and nothing else — so on a fresh runner a fast refresh
# has fresh prices, fresh FX, a fresh curve, and **no PSD, no weather, no
# COT, no crop progress at all**. Every block over those layers renders as a
# legal empty state with a stated reason, every page passes the contract
# above, and the result is a structurally perfect edition that has thrown
# away two thirds of what the site knew an hour earlier.
#
# No amount of checking the candidate against itself finds that. It is only
# visible against the edition being replaced, which is what this gate does.


@dataclass(frozen=True)
class RefreshVerdict:
    """Whether a candidate may replace the currently published edition."""

    promotable: bool
    failures: tuple[str, ...]
    regressions: tuple[str, ...] = ()


_UNUSABLE_STATUSES = frozenset({"not-run", "failed"})


def verify_refresh_is_not_a_regression(
    candidate: dict | None,
    public: dict | None,
) -> RefreshVerdict:
    """Compare two edition manifests (``app.manifest``); refuse a regression.

    Three rules, in the order a failure would matter:

    1. **Both manifests must be readable and same-schema.** A missing or
       unparseable candidate manifest is a refusal, not a pass — the whole
       gate is the comparison, so being unable to make it is being unable to
       clear it. A missing *public* manifest is different and is allowed
       through: the first build after this ships has nothing to compare
       against, and blocking there would wedge the deploy permanently.

    2. **No layer may go backwards.** For every layer the public edition
       observed, the candidate must carry an observation at least as new. A
       null against a date is the failure mode described above and is the
       loudest form of it.

    3. **No page may go missing or tombstone** that the public edition had.

    Note what is *not* checked: that the candidate is newer. A rebuild of the
    same data is a legitimate promotion (a template fix, a rendering change),
    and requiring a newer observation would block it.
    """
    failures: list[str] = []
    regressions: list[str] = []

    if not candidate:
        return RefreshVerdict(False, ("candidate manifest is missing or unreadable",))
    if candidate.get("schema_version") != 1:
        return RefreshVerdict(
            False,
            (f"candidate manifest schema {candidate.get('schema_version')!r} is not readable",),
        )

    if not public:
        # Nothing to regress against. Stated, not silent: the log line is how
        # an operator tells "first build" from "the gate never ran".
        return RefreshVerdict(True, (), ())
    if public.get("schema_version") != 1:
        return RefreshVerdict(
            True, (), (f"public manifest schema {public.get('schema_version')!r} not comparable",)
        )

    def _coverage(manifest: dict) -> dict[str, dict]:
        return {row["layer"]: row for row in manifest.get("coverage", []) if "layer" in row}

    cand_cov, pub_cov = _coverage(candidate), _coverage(public)
    for layer, pub_row in pub_cov.items():
        pub_observed = pub_row.get("observed_at")
        if pub_observed is None:
            continue
        cand_row = cand_cov.get(layer)
        if cand_row is None:
            regressions.append(f"{layer}: present in the published edition, absent here")
            continue
        cand_observed = cand_row.get("observed_at")
        if cand_observed is None:
            regressions.append(
                f"{layer}: published edition observed {pub_observed}, this candidate has none"
            )
        elif cand_observed < pub_observed:
            regressions.append(
                f"{layer}: observation went backwards, {pub_observed} -> {cand_observed}"
            )
        # A layer that is present and current but whose *run* failed is not a
        # regression in what we know — last_success is preserved by design and
        # the surfaces already say so. Only lost observations count here.
        if (
            cand_row.get("status") in _UNUSABLE_STATUSES
            and pub_row.get("status") not in _UNUSABLE_STATUSES
            and cand_observed is None
        ):
            regressions.append(f"{layer}: {pub_row.get('status')} -> {cand_row.get('status')}")

    def _pages(manifest: dict) -> dict[str, bool]:
        return {
            page["url"]: bool(page.get("ok"))
            for page in manifest.get("edition", {}).get("pages", [])
            if "url" in page
        }

    cand_pages, pub_pages = _pages(candidate), _pages(public)
    for url, was_ok in pub_pages.items():
        if not was_ok:
            continue
        if url not in cand_pages:
            regressions.append(f"{url}: published edition has this page, this candidate does not")
        elif not cand_pages[url]:
            regressions.append(f"{url}: published edition rendered this page, this candidate did not")

    failures.extend(regressions)
    return RefreshVerdict(not failures, tuple(dict.fromkeys(failures)), tuple(regressions))
