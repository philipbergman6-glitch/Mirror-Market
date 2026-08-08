# Players Section Phase 2 — Spec (issue #111)

**Date:** 2026-08-08 · **Route:** grilled with Philip (5 rounds, all recommendations accepted) → this spec → tickets T1–T4. Do not implement from #111 directly.

## Goal

One place where a commodity soy trader sees the global counterparty map — sellers, buyers, exports, imports, worldwide, both sides symmetric — and can identify who to contact for business. Ships as `docs/players.html`, one more page in the existing static site (same nav, same daily CI build, DESIGN.md governs visuals). No separate app; the structured data layer is deliberately decoupled from the page so a future app reads the same files/DB.

## Decisions (settled in grilling)

| # | Decision |
|---|----------|
| 1 | Audience: commodity soy trader; **symmetric** buyer+seller UX, global |
| 2 | Ship order: counterparty filtering → contactability → recent activity |
| 3 | Activity is **curated dated notes** — no scraping pipeline (licence RED; users don't need real-time). Structured so clean automated sources (SEC/CVM filings, press-release RSS) can layer in later |
| 4 | Supersedes #74's page — one combined players page. #74's `export_taxes.yml` policy card stays a separate small ticket |
| 5 | Contacts: company-level always, desk-level only when the company publishes it, **never named individuals** |
| 6 | Destinations become queryable: ISO country codes + region enum, build-time validation, hard-fail on unknown values |
| 7 | Light DB integration: per-country context blocks from existing pipeline data (PSD, export sales, weather) at build time |
| 8 | Coverage: contacts for **all 193** players; curated activity for **tier-1 only** (~40–60 majors, `tier: 1` flag) |
| 9 | Contact re-verify: opportunistic + semi-annual tier-1 sweep |
| 10 | Activity: quarterly tier-1 sweep + ad-hoc on breaking news; accumulate newest-first; page shows latest 3 per player |
| 11 | Destination vocabulary: ISO-3166 alpha-2 where sources name countries; small region enum (`EU`, `MENA`, `SE-ASIA`, `LATAM`, `AFRICA`, `GLOBAL`, …) where they don't |
| 12 | Page shape: **single `players.html`** — filter bar (role / product / origin / destination / tier) driving client-side JS over compact expandable cards grouped by country |
| 13 | Country context blocks: 2–4 always-on numbers (PSD imports/exports, export-sales trend, weather z-scores for the 12 weather-covered countries), alert styling on threshold breaches. Calm is information |
| 14 | Staleness flags: activity amber when newest tier-1 item >2 quarters old; contacts amber when `accessed` >12 months; profile `as_of` unflagged for now |

## Data model (additive to `data/reference/players/*.yml`, 193 entries)

```yaml
tier: 1                     # majors only; absent = tier 2
destinations_structured:    # replaces prose-only `destinations` for querying
  - {code: CN, products: [beans], note: "57.95% of Brazil soy-complex FOB value H1-2026"}
  - {code: EU, products: [meal]}   # region enum allowed where sources don't name countries
contacts:
  website: "https://..."
  contact_url: "https://..."       # corporate contact / commercial page
  offices: ["Rosario", "Geneva"]
  trade_desks:                     # only when publicly listed
    - {label: "Oilseeds export desk", email: "...", phone: "...", office: "Rosario"}
  accessed: "2026-08-08"
activity:                          # tier-1 only
  - date: "2026-07-02"
    category: capacity             # enum: ma | capacity | trade | policy | distress
    headline: "..."
    detail: "..."
    citation: {url: "...", accessed: "2026-08-08"}
```

A validation script (build-time, wired into `scripts/generate_html.py` or standalone) hard-fails on: unknown country codes/regions, bad category enums, missing citation/accessed on activity items, malformed dates.

## Weather/flows mapping

Weather regions exist for US, Brazil, Argentina, Paraguay, China, India, South Africa, Nigeria, Indonesia, Malaysia, Thailand, Ivory Coast — all origin countries. Pure importer sections (MENA, most Europe/Asia/LatAm importers) show flows only, no weather. Blocks render at build time from the DB; missing data degrades to omission, never fabrication.

## Tickets

- **T1 — data model**: destinations vocabulary + `tier` flags across 13 files, validation script. Blocks T2.
- **T2 — `players.html`**: filter bar + client-side filtering, country context blocks (flows + weather), card design per DESIGN.md, staleness flags, nav link. Closes the page half of #74.
- **T3 — contacts pass**: research sweep, all 193 players, schema above.
- **T4 — activity sweep #1**: tier-1 selection (~40–60) + first curated activity pass; seeds the feed and sets the quarterly cadence.

T3/T4 are research work, parallel after T1 defines schema. Maintenance cadence (quarterly activity, semi-annual contacts) is a standing process, not a ticket.
