"""Site orchestrator — owns the page list and the failure-isolation policy.

M8 #150 demoted ``scripts/generate_html.py`` to the *headline page's* renderer:
one entry in this list, not the thing that also builds everything else. This
module owns what is genuinely site-wide — which pages exist, what happens when
one of them fails, and the market nav every page carries.

Failure isolation, three levels (M8):

    block raises     -> empty state carrying reason "generation error"
                        (app/blocks.py; same *shape* as a missing source, a
                        deliberately different *reason*)
    page fails       -> a TOMBSTONE at that page's URL, never yesterday's file
    headline fails   -> the run fails. There is no product without it.

Why the tombstone matters: a failed page cannot simply leave yesterday's HTML
in a candidate artifact. Every requested page is regenerated or replaced with
a dated tombstone. The promotion contract rejects every tombstoned candidate,
so the last trustworthy public edition remains available while the failure is
reported.

Usage:
    python scripts/generate_site.py                # every page
    python scripts/generate_site.py --only cbot    # one market, for the dev loop
    python scripts/generate_site.py --only headline
"""

from __future__ import annotations

import argparse
import logging
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from app.block_builders import SiteContext, build_blocks  # noqa: E402
from app.blocks import BLOCK_IDS, BRIEF_BLOCK_IDS  # noqa: E402
from app.markets import (  # noqa: E402
    TIER_BRIEF,
    TIER_PAGE,
    TIER_STUB,
    compute_tiers,
    load_markets,
    nav_items,
    relative_root,
)
from config import setup_logging  # noqa: E402

log = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "docs"
TEMPLATE_DIR = PROJECT_ROOT / "app" / "templates"

# Per-page size budget (M8). docs/index.html is ~7 MB of inline Plotly series
# today; M2 moves most of that onto the CBOT page, so the budget is what stops
# it landing there unclipped. Asserted in tests/test_site_contract.py — a
# budget that is not tested is a wish.
PAGE_SIZE_BUDGET_BYTES = 1_500_000

TEMPLATE_BY_TIER = {
    TIER_PAGE: "market_page.html.j2",
    TIER_BRIEF: "market_brief.html.j2",
    TIER_STUB: "market_stub.html.j2",
}


@dataclass
class PageResult:
    name: str
    relpath: str
    path: Path
    ok: bool
    error: str | None = None


def _env() -> Environment:
    return Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=False)


def _write(output_dir: Path, relpath: str, html: str) -> Path:
    path = output_dir / relpath
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")
    size_kb = path.stat().st_size / 1024
    if path.stat().st_size > PAGE_SIZE_BUDGET_BYTES:
        log.warning(
            "%s is %.0f KB, over the %.0f KB per-page budget",
            relpath, size_kb, PAGE_SIZE_BUDGET_BYTES / 1024,
        )
    else:
        log.info("wrote %s (%.0f KB)", relpath, size_kb)
    return path


def _tombstone(output_dir: Path, relpath: str, page_name: str, error: str, nav: list[dict], now) -> Path:
    """Overwrite a failed page with a dated error — never leave yesterday's file."""
    html = _env().get_template("tombstone.html.j2").render(
        page_name=page_name,
        error=error,
        root=relative_root(relpath),
        market_nav=nav_items_at(nav, relative_root(relpath)),
        current_page=None,
        current_market=None,
        day_line=now.strftime("%A %d %B %Y").upper(),
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_iso=now.isoformat(),
    )
    return _write(output_dir, relpath, html)


def nav_items_at(nav: list[dict], root: str) -> list[dict]:
    """Re-root the shared nav for a page at a different depth."""
    return [dict(item, href=root + item["href"]) for item in nav]


# ---------------------------------------------------------------------------
# Page renderers
# ---------------------------------------------------------------------------
def _render_headline(output_dir: Path, nav: list[dict], *, public_trust_state=None, **_) -> Path:
    from scripts.generate_html import generate

    artifacts = generate(
        output_dir=output_dir,
        market_nav=nav,
        include_players=False,
        public_trust_state=public_trust_state,
    )
    return artifacts["dashboard"]


def _render_players(output_dir: Path, nav: list[dict], **_) -> Path:
    from scripts.generate_players import generate_players_page

    return generate_players_page(output_dir / "players.html", market_nav=nav)


def _render_origins(output_dir: Path, nav: list[dict], *, ctx, now, **_) -> Path:
    """The Phase 2 origin-comparison page.

    Reuses the site context so the eight market pages and this one read one
    database through one connection — and, more importantly, so a price this
    page ranks on is the same row the owning market page renders.
    """
    from app.origins_page import build_view

    relpath = "origins.html"
    root = relative_root(relpath)
    view = build_view(ctx.conn, today=now.date())
    html = _env().get_template("origins.html.j2").render(
        origins=view,
        root=root,
        market_nav=nav_items_at(nav, root),
        current_page="origins",
        current_market=None,
        day_line=now.strftime("%A %d %B %Y").upper(),
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_iso=now.isoformat(),
    )
    path = _write(output_dir, relpath, html)
    _archive_origin_rankings(view)
    return path


def _archive_origin_rankings(view: dict) -> None:
    """Persist what this run published, without letting a write failure kill the page.

    The archive is what makes "what changed since yesterday" possible at all,
    and it is genuinely unrecoverable later — the assumption set that produced
    a ranking expires out of the working files by design. But a page that
    rendered correctly must still be published if the write fails, so this is
    isolated rather than inline.
    """
    from analysis.origins.history import archive_ranking

    for ranking in view.get("rankings", ()):
        try:
            archive_ranking(ranking)
        except Exception:  # noqa: BLE001 — archiving must never fail a render
            log.warning(
                "could not archive the %s / %s origin ranking",
                ranking.destination.key,
                ranking.requested_window.describe(),
                exc_info=True,
            )


def _render_market(output_dir: Path, nav: list[dict], *, slug: str, markets, tiers, ctx, now) -> Path:
    market = markets[slug]
    tier = tiers[slug]
    relpath = market.url
    root = relative_root(relpath)

    block_ids = BLOCK_IDS if tier.tier == TIER_PAGE else BRIEF_BLOCK_IDS
    blocks = build_blocks(market, tier, ctx, markets=markets, block_ids=block_ids)

    html = _env().get_template(TEMPLATE_BY_TIER[tier.tier]).render(
        market=market,
        tier=tier,
        blocks=blocks,
        root=root,
        market_nav=nav_items_at(nav, root),
        current_market=slug,
        current_page="market",
        day_line=now.strftime("%A %d %B %Y").upper(),
        generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
        generated_at_iso=now.isoformat(),
    )
    return _write(output_dir, relpath, html)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------
def generate_site(
    *,
    output_dir: str | Path = OUTPUT_DIR,
    only: str | None = None,
    public_trust_state=None,
) -> list[PageResult]:
    """Render every page, isolating failures. Returns one result per page."""
    setup_logging()
    output_dir = Path(output_dir)
    now = datetime.now(timezone.utc)

    markets = load_markets()
    tiers = compute_tiers(markets)
    nav = nav_items(tiers, markets=markets)
    if only and only not in {"headline", "players", "origins", *markets}:
        names = ["headline", "players", "origins", *markets]
        raise SystemExit(f"--only {only!r} matches no page; known: {', '.join(names)}")
    # One connection and one cache for every market page: eight pages x nine
    # blocks would otherwise re-read the CBOT reference leg eight times.
    ctx = SiteContext.open(today=now.date())

    pages: list[tuple[str, str, callable, dict]] = [
        ("headline", "index.html", _render_headline, {"public_trust_state": public_trust_state}),
        ("players", "players.html", _render_players, {}),
        ("origins", "origins.html", _render_origins, {"ctx": ctx, "now": now}),
    ]
    for slug, market in markets.items():
        pages.append((
            f"market:{slug}",
            market.url,
            _render_market,
            {"slug": slug, "markets": markets, "tiers": tiers, "ctx": ctx, "now": now},
        ))

    if only:
        wanted = {only, f"market:{only}"}
        pages = [p for p in pages if p[0] in wanted]

    results: list[PageResult] = []
    try:
        for name, relpath, render, kwargs in pages:
            try:
                path = render(output_dir, nav, **kwargs)
                results.append(PageResult(name, relpath, path, ok=True))
            except Exception as exc:  # noqa: BLE001 — isolation is the point
                if name == "headline":
                    log.error("headline page failed — there is no site without it")
                    raise
                detail = "".join(traceback.format_exception_only(type(exc), exc)).strip()
                log.error("%s failed: %s", name, detail, exc_info=True)
                path = _tombstone(output_dir, relpath, name, detail, nav, now)
                results.append(PageResult(name, relpath, path, ok=False, error=detail))
    finally:
        ctx.close()

    failed = [r for r in results if not r.ok]
    log.info("generated %d page(s), %d tombstone(s)", len(results) - len(failed), len(failed))
    return results


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate the Mirror Market static site.")
    parser.add_argument(
        "--only",
        metavar="PAGE",
        help="render one page only: headline, players, or a market slug",
    )
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args(argv)

    results = generate_site(output_dir=args.output_dir, only=args.only)

    # Tombstones diagnose page failures inside the private candidate; the
    # promotion contract prevents them reaching Pages.
    failed = [r for r in results if not r.ok]
    for result in failed:
        log.error("TOMBSTONE %s at %s: %s", result.name, result.relpath, result.error)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
