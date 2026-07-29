# Wayfinder map: Soy Trader App

Label: wayfinder:map
Tracker: local markdown (.scratch/soy-trader-app/, tickets in issues/)

**STATUS: ACTIVE (resumed 2026-07-28)** — briefly shelved earlier the same day (keep static dashboard, defer app until a buyer appears), then resumed by user invoking "work the map". IA, design direction, and stack research all still stand.

## Destination

A locked spec **and a working v1** of a local web app (run locally, opens in the browser, reads the existing SQLite/Turso database) that replaces the static GitHub Pages dashboard: everything a commodity soy trader needs — physical and futures, across all 19 pipeline layers — opening on a morning command-center screen with drill-down detail views.

## Notes

- Domain: commodity soy trading (soy complex + competing crops); data pipeline documented in CLAUDE.md (19 layers, `pipeline/query.py` read API, `analysis/` modules incl. `soy_analytics.py`'s 9 analyst functions).
- Skills to consult per ticket: `/grilling` + `/domain-modeling` for decisions, `/prototype` + `/design-an-interface` for the design/home-screen ticket, `/research` for the stack ticket.
- Standing decisions from charting: form factor = **local web app**; home = **morning command center** (5-minute scan: soy prices, overnight changes, signals, crush, basis, freshness); **fresh visual design** (evolve DESIGN.md, don't inherit blindly); static dashboard retired once app covers it, **text briefing stays** as its own artifact (may render inside the app).
- This effort carries execution: the map includes building v1, not just deciding.
- Layers 16 (NCDEX) and 17 (CEPEA) are disabled upstream — the app must handle their staleness gracefully, not assume live data.
- Audience (clarified 2026-07-28): business professionals — every design/scope call favors the most professional and simplest take over trader-terminal density.

## Decisions so far

<!-- one line per closed ticket -->

- [01 — Information architecture: layers → screens](issues/01-information-architecture.md) — 11 screens: Command Center home (charter + weather/stocks-to-use alerts) plus 10 drill-downs grouped PHYSICAL / FUTURES / CROSS; the 9 analyst screens reused, plus new Macro & FX (FRED + 13 currencies + World Bank) and Briefing (text + archive) screens; futures screens get a commodity picker; Brazil basis detail lives in Relative Value.
- [03 — Visual design direction + command-center prototype](issues/03-design-and-command-center-prototype.md) — chosen from 3 prototypes: **C — Morning Scan**, a light editorial front-page look (paper/ink/soy-green, Archivo + Inter + IBM Plex Mono, numbered scan-order sections); reference mockup at `assets/design-directions/direction-c-morning-scan.html`, seeds the updated DESIGN.md.

## Not yet specified

- How/whether the app can trigger a pipeline run (refresh button vs "run `main.py` separately") — depends on data-access decision.
- Retiring mechanics for the old surfaces: `scripts/generate_html.py`, `docs/`, the GitHub Actions Pages deploy — after v1 proves coverage.
- Alerting/notification ideas (signal severity surfacing beyond the command center) — after v1.

## Out of scope

- Packaging as an installable desktop app (Tauri/Electron wrap) — "eventually an app" is satisfied for this effort by the local web app; desktop packaging is a fresh effort after v1, though the stack ticket should note which candidates wrap cleanly.
- Hosted/public deployment replacing GitHub Pages with a server — this effort is local-first.
