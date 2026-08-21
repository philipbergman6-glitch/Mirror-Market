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
    python scripts/generate_site.py --only workstation
    python scripts/generate_site.py --only opportunities
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
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

# Market pages get a much tighter one (M21 #250). They ran 13–19 KB before the
# ledger drill-down shipped its series inline, and inline series are exactly the
# thing that grows without anyone noticing — the prototype went 26 KB → 89 KB on
# five unclipped legs. Asserted in tests/test_site_contract.py.
MARKET_PAGE_SIZE_BUDGET_BYTES = 150_000

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


def _render_opportunities(output_dir: Path, nav: list[dict], *, ctx, now, **_) -> Path:
    """The Phase 4 opportunity board — TWO artifacts from ONE engine run.

    The public edition goes to ``docs/opportunities.html``. The private edition,
    which carries the trader's own working file, is written to
    ``config.OPPORTUNITY_PRIVATE_OUTPUT_DIR`` — deliberately outside ``docs/``,
    because ``docs/`` is what the Pages deploy uploads and a private note must
    not be able to land there through a path mistake.

    One engine run for both: two runs would archive twice and, worse, could
    disagree — which on this page means the public edition showing a row the
    private one already knows was dismissed.
    """
    from analysis.opportunities import engine as engine_mod
    from analysis.opportunities.domain import AUDIENCE_PUBLIC
    from app.opportunities_page import build_view

    result = engine_mod.run(ctx.conn, today=now.date())

    def render(view: dict, relpath: str, root: str) -> str:
        return _env().get_template("opportunities.html.j2").render(
            opportunities=view,
            root=root,
            market_nav=nav_items_at(nav, root),
            current_page="opportunities",
            current_market=None,
            day_line=now.strftime("%A %d %B %Y").upper(),
            generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
            generated_at_iso=now.isoformat(),
        )

    relpath = "opportunities.html"
    root = relative_root(relpath)
    public = build_view(ctx.conn, today=now.date(), audience=AUDIENCE_PUBLIC, result=result)
    path = _write(output_dir, relpath, render(public, relpath, root))

    _write_private_opportunities(ctx, now, result, render)
    return path


def _write_private_opportunities(ctx, now, result, render) -> None:
    """The private edition. Isolated: it must never fail the public page.

    A workspace that cannot be written is a local inconvenience; a public page
    that fails is a tombstone in the candidate and a blocked deploy. They are
    not the same severity and are not treated as one.
    """
    import config as _config
    from analysis.opportunities.domain import AUDIENCE_PRIVATE
    from app.opportunities_page import build_view

    try:
        private_dir = Path(_config.OPPORTUNITY_PRIVATE_OUTPUT_DIR)
        private_dir.mkdir(parents=True, exist_ok=True)
        view = build_view(
            ctx.conn, today=now.date(), audience=AUDIENCE_PRIVATE, result=result
        )
        # Root is "" rather than a computed prefix: the private file does not
        # sit inside docs/, so its relative links back to the public site would
        # be wrong at any depth. They are left pointing at the site root, and
        # the page is explicitly not a published artifact.
        target = private_dir / "opportunities.html"
        target.write_text(render(view, "opportunities.html", ""), encoding="utf-8")
        log.info("wrote the private opportunity edition to %s", target)
    except Exception:  # noqa: BLE001 — the workspace must never fail the site
        log.warning("could not write the private opportunity edition", exc_info=True)


def _render_workstation(output_dir: Path, nav: list[dict], *, ctx, now, **_) -> Path:
    """The Phase 3 futures workstation — TWO artifacts, on the Phase 4 pattern.

    Shares the site context for the same reason the origins page does: a hedge
    sized on this page must be sized on the same curve row the CBOT market page
    renders, and two connections are two snapshots.

    The public edition goes to ``docs/workstation.html`` with the book,
    exposure, limits, clearing and entered-option sections rendered absent. The
    private edition — the one the desk actually works from — is written to
    ``config.OPPORTUNITY_PRIVATE_OUTPUT_DIR``, outside ``docs/``, because that
    directory is what the Pages deploy uploads and an entered position must not
    be able to land there through a path mistake.
    """
    from analysis.futures.privacy import AUDIENCE_PUBLIC, assert_no_client_records
    from app.workstation_page import build_view

    relpath = "workstation.html"
    root = relative_root(relpath)

    def render(view: dict, link_root: str) -> str:
        return _env().get_template("workstation.html.j2").render(
            workstation=view,
            root=link_root,
            market_nav=nav_items_at(nav, link_root),
            current_page="workstation",
            current_market=None,
            day_line=now.strftime("%A %d %B %Y").upper(),
            generated_at=now.strftime("%Y-%m-%d %H:%M UTC"),
            generated_at_iso=now.isoformat(),
        )

    public = build_view(
        ctx.conn, today=now.date(), generated_at=now, audience=AUDIENCE_PUBLIC,
    )
    # Checked here, at the last moment before the bytes are written, and not
    # only in a test: a leak must fail this page — which becomes a tombstone
    # and blocks the promotion contract — rather than be published and noticed.
    assert_no_client_records(public, where="docs/workstation.html")
    path = _write(output_dir, relpath, render(public, root))

    _write_private_workstation(ctx, now, render)
    return path


def _write_private_workstation(ctx, now, render) -> None:
    """The desk's edition. Isolated: it must never fail the public page.

    Same severity split as the private opportunity board — a workspace that
    cannot be written is a local inconvenience, while a public page that fails
    is a tombstone in the candidate and a blocked deploy.

    The destination is checked by :func:`analysis.futures.privacy.
    assert_private_path` rather than assumed. The check is cheap and the thing
    it prevents — a book written into ``docs/`` because a constant moved — is
    unrecoverable once the deploy has run.
    """
    from analysis.futures.privacy import (
        AUDIENCE_PRIVATE,
        assert_private_path,
        private_output_dir,
    )
    from app.workstation_page import build_view

    try:
        private_dir = private_output_dir()
        private_dir.mkdir(parents=True, exist_ok=True)
        target = assert_private_path(private_dir / "workstation.html", where="private workstation")
        view = build_view(
            ctx.conn, today=now.date(), generated_at=now, audience=AUDIENCE_PRIVATE,
        )
        # Root is "" rather than a computed prefix: the private file does not
        # sit inside docs/, so its relative links back to the public site would
        # be wrong at any depth.
        target.write_text(render(view, ""), encoding="utf-8")
        log.info("wrote the private workstation edition to %s", target)
    except Exception:  # noqa: BLE001 — the workspace must never fail the site
        log.warning("could not write the private workstation edition", exc_info=True)


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
    if only and only not in {
        "headline", "players", "origins", "workstation", "opportunities", *markets
    }:
        names = ["headline", "players", "origins", "workstation", "opportunities", *markets]
        raise SystemExit(f"--only {only!r} matches no page; known: {', '.join(names)}")
    # One connection and one cache for every market page: eight pages x nine
    # blocks would otherwise re-read the CBOT reference leg eight times.
    ctx = SiteContext.open(today=now.date())

    pages: list[tuple[str, str, callable, dict]] = [
        ("headline", "index.html", _render_headline, {"public_trust_state": public_trust_state}),
        ("players", "players.html", _render_players, {}),
        ("origins", "origins.html", _render_origins, {"ctx": ctx, "now": now}),
        ("workstation", "workstation.html", _render_workstation, {"ctx": ctx, "now": now}),
        ("opportunities", "opportunities.html", _render_opportunities, {"ctx": ctx, "now": now}),
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
    started = time.perf_counter()
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

    # The manifest describes the whole edition, so it is written only for a
    # whole edition. A `--only cbot` dev build would otherwise leave a
    # manifest claiming one page — and the fast-refresh gate reads page
    # coverage, so that manifest would block the next real promotion.
    if not only:
        _write_manifest(output_dir, now, results, time.perf_counter() - started)
    return results


def _write_manifest(
    output_dir: Path, now, results: list[PageResult], seconds: float
) -> None:
    """Write the edition manifest. Isolated: it must never fail a good build.

    The manifest is how the next run proves it is not a regression, so losing
    it is a real cost — but it is a cost paid *next* time. Failing this build
    over it would turn a reporting problem into an outage.
    """
    from app.manifest import build_manifest, write_manifest

    try:
        manifest = build_manifest(
            generated_at=now,
            pages=[{"name": r.name, "url": r.relpath, "ok": r.ok} for r in results],
            generation_seconds=round(seconds, 3),
        )
        path = write_manifest(output_dir, manifest)
        log.info("wrote %s (%s edition, %.2fs)", path.name, manifest["edition"]["mode"], seconds)
    except Exception:  # noqa: BLE001
        log.warning("could not write the edition manifest", exc_info=True)


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
