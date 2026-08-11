# Design System — Mirror Market

## Product Context
- **What this is:** Commodity market intelligence platform monitoring global agricultural markets with 19 data source layers across 27 countries
- **Who it's for:** Business professionals (managers, stakeholders) reviewing the daily state of the soy complex — plus engineers viewing the project from GitHub
- **Space/industry:** Commodity trading, agricultural markets, market research
- **Project type:** Data-heavy analytics dashboard (static HTML, generated from Python)

## Aesthetic Direction
- **Direction:** "Morning Scan" — a light editorial front page. The dashboard reads like the morning edition of a serious financial paper: a masthead, numbered sections in scan order, heavy rules, generous whitespace
- **Decoration level:** Minimal — typography, rules, and data do all the work
- **Mood:** Calm, authoritative, professional. A daily read, not a trading terminal
- **Reference:** chosen 2026-07 from three prototypes; canonical mockup at `.scratch/soy-trader-app/assets/design-directions/direction-c-morning-scan.html`

## Typography
- **Display/Headlines:** Archivo (700–800 weight) — masthead at 42px/-0.03em, big stat headlines at 30–32px/-0.02em, section heads at 15px uppercase 0.05em tracking
- **Body/UI:** Inter (400–600 weight) — 14px body, 13px dense rows, 12px captions
- **Data/Numbers:** IBM Plex Mono (400–500) with `font-variant-numeric: tabular-nums` — every numeric value uses the `.num` class
- **Loading:** Google Fonts CDN (`Archivo:wght@500;600;700;800`, `Inter:wght@400;500;600`, `IBM+Plex+Mono:wght@400;500`)
- **Scale:**
  - Masthead: 42px / Archivo 800 / tracking -0.03em / line-height 1
  - Big stat: 30–32px / Archivo 700 / tracking -0.02em
  - Section head: 15px / Archivo 700 / uppercase / tracking 0.05em, preceded by a mono section number in green
  - Body: 14px / Inter 400 / line-height 1.45
  - Dense row / label: 13px; caption: 12px muted
  - Card metric value: 32px / Archivo 700 (mono for the digits)

## Color
- **Approach:** Paper-and-ink light palette with the soy-green brand accent. No dark mode
- **Surfaces:**
  - Paper (page background): `#FAFAF7`
  - Card: `#FFFFFF`
  - Rule (light border): `#D8DCD8`
  - Rule heavy (masthead/section dividers): `#191C1A`
- **Text:**
  - Ink (primary): `#191C1A`
  - Muted: `#5D6660`
  - Dim: `#9BA39E`
- **Brand:**
  - Soy green: `#2D6A4F` — masthead accent word, section numbers, links, pills
- **Directional:**
  - Up/bullish: `#1F7A3D`
  - Down/bearish: `#C23B2E`
- **Commodity (ink-compatible, darker than the old dark-theme set):**
  - Soybean: `#8B6914`
  - Soy Oil: `#B05E10`
  - Soy Meal: `#8A5A2B`
- **Semantic:**
  - Warning: `#A8730A` (badge fill `#E8C983` with ink text `#4A3608`)
  - Error/alert: `#C23B2E` (badge fill solid, white text)
  - Info: `#2F5D8F` (neutral badge fill `#E4E8E4`, muted text)
- **Alert backgrounds:** semantic color at ~8% opacity with a 3px left border
- **CSS variable contract:** generated HTML snippets reference `--text`, `--text-muted`, `--text-dim`, `--bullish`, `--bearish`, `--green-light`, `--warning`, `--info` — the template must define all of these, mapped to the palette above

## Spacing
- **Base unit:** 8px; density comfortable
- **Page:** max-width 1180px, centered, 32px side padding, 64px bottom
- **Sections:** 22–28px vertical padding, separated by a 1px light rule
- **Cards:** 16–18px padding, 16px grid gap
- **Border radius:** 8px cards, 3px badges

## Layout
- **Approach:** Every page is a single scrolling editorial page in scan order; the site is a small fixed set of such pages, reachable from a masthead-level nav identical on all of them. Still **no sidebar** — that is what the 2026-07-28 redesign removed and this does not reinstate it
- **Masthead:** brand title (two-line, accent word in green) + right-aligned meta (day/date, pipeline run time, layer freshness count, units note), sitting on a 3px heavy rule
- **Stale note:** a single warning line directly under the masthead when any layer is stale
- **Market nav:** a second thin bar in the masthead, directly under it and *above* the index nav — two bars in the same visual family. Lists Headline + every market + Players, in registry key order (role in the trade: `CBOT · Dalian · Brazil · Argentina · India · Europe · South Africa · Nigeria`), declared once in `config.MARKETS` and shared with the headline ledger. Every market appears in every tier or the reader cannot tell it exists; **stubs are dimmed**, pages and briefs render identically. The nav does not teach the tier system — the tier is stamped on the page itself
- **Index nav:** slim sticky bar under the market nav with mono section numbers linking to anchors
- **Sections:** numbered (`01`, `02`, …) with an Archivo uppercase title and a right-aligned dim "why you're looking at this" note
- **Grids:** 3-up for soy leg cards, 4-up for metric rows, 2-up for headline+chart pairs; dotted-rule key/value rows for watchlists
- **Responsive:** grids collapse to 1 column below 768px; index nav scrolls horizontally

## Motion
- **Approach:** Minimal-functional. Instant tab toggles, 0.1s hover transitions, native smooth-scroll for anchor links. Plotly handles chart interactivity

## Component Patterns

### Leg card (price card)
White card, 1px rule border, 8px radius. Uppercase muted commodity name (in its commodity color), Archivo 32px mono price with small unit, directional change line (▲/▼ + % in up/down color), 12px sub-row (RSI · trend · vol).

### Section head
`<span class="sec-no">01</span>` mono green number + Archivo uppercase `<h2>` + optional right-aligned dim `.why` note.

### Signal row
Flex row: solid badge (alert red/white, warn `#E8C983`/dark, info neutral gray) + muted commodity name (min-width 96px) + message. Dotted rule between rows.

### Key/value watch row
`.kv` flex row, label muted left, mono value right, dotted bottom rule. Used for watchlists and dense stat lists.

### Big stat headline
Archivo 30px mono number in directional color + 14px dim unit suffix + 13px muted caption underneath. For crush spread, basis, and other single-number stories.

### Briefing block
IBM Plex Mono 12px / line-height 1.7, `white-space: pre-wrap`, on a white card. Section headers tinted green, directional percentages tinted up/down.

### Charts (Plotly)
White paper/plot background, `#D8DCD8` gridlines, Inter 12px font, ink text. Traces use the commodity/directional palette above. No chart borders beyond the card that contains them.

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-04-16 | GitHub dark palette, Geist, sidebar nav | Original terminal-style system (superseded) |
| 2026-07-28 | **Full redesign to "Morning Scan" light editorial system** | Chosen by the user from three prototyped directions; audience is business professionals — a calm front-page read beats terminal density. Single scrolling page in scan order replaces sidebar + hidden pages |
| 2026-07-28 | Keep the CSS variable contract (`--text`, `--bullish`, …) | Python HTML builders emit inline `var(...)` references; remapping variables to the light palette restyles all generated snippets without touching builder logic |
| 2026-08-12 | **Multi-page site with a masthead market nav** (amends the 2026-07-28 "no hidden pages" line) | Map [#142](https://github.com/philipbergman6-glitch/Mirror-Market/issues/142): five of the ten headline sections were CBOT front-month only, so one page could not carry eight markets without becoming a CBOT page in a complex-wide hat. The editorial single-page *reading* model is unchanged — each page is still one scrolling page in scan order; what changes is that there is now a small fixed set of them. Nav is a second masthead bar, explicitly not the sidebar the 2026-07-28 redesign removed. Contract in [M8 #150](https://github.com/philipbergman6-glitch/Mirror-Market/issues/150), built in [M17 #213](https://github.com/philipbergman6-glitch/Mirror-Market/issues/213); `app/templates/_base.html.j2` owns `<head>`, the palette and both nav bars for every page |
| 2026-08-12 | **Market pages carry a tier stamp and reasoned empty states** | [M1 #143](https://github.com/philipbergman6-glitch/Mirror-Market/issues/143): the nine blocks are numbered identically on every full page, a missing block renders a labelled empty state naming its reason (never a gap, never a renumber), and the computed tier (`page` / `brief` / `stub`) is printed on the page so a demotion is visible rather than inferred. `absent` (no source exists) reads calm; `empty` (a source exists and gave nothing) carries the warning rule |
| 2026-08-12 | **Ledger state pills, and the headline gains a Propagation section (03)** | [M19 #223](https://github.com/philipbergman6-glitch/Mirror-Market/issues/223): the propagation ledger's job is to make silence legible, so the state pill carries the colour and the number does not. `repriced` is the only affirmative fill (green tint); `no print since` is deliberately quiet grey and turns amber **only** when past the gap that is normal for that leg; `dark` is solid `--bearish`; `out of cadence` (Europe on the headline) is outlined rather than filled, because it is neither an outage nor a print and carries no value at all. The `FX` tag is an `--info` chip, not a colour on the number — a currency move must not read as a market move. `.kind` moves to `_base.html.j2` for the same reason the cards did: the headline now renders it too. Headline sections renumber 03→04 … 10→11 to seat the ledger third, where [M2 #144](https://github.com/philipbergman6-glitch/Mirror-Market/issues/144) put it |
| 2026-08-12 | **Card, alert and badge CSS moves into `_base.html.j2`** | [M18 #214](https://github.com/philipbergman6-glitch/Mirror-Market/issues/214): the metric card, alert, signal-badge and table rules were defined inside `dashboard.html.j2` while the dashboard was the only page with components. The nine market blocks render the same components, so a second copy in `_market_css.html.j2` would be the drift the one-`<head>`-owner rule exists to prevent. `_base` now owns the shared components alongside the palette; each page's own CSS stays with the page |
