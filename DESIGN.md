# Design System — Mirror Market

## Product Context
- **What this is:** Public-data soy intelligence and private desk decision support for physical buyers making daily cargo, basis, origin, and hedge decisions — 31 operational data layers across 28 numbered source groups (canonical mission: `CLAUDE.md`)
- **Who it's for:** Physical buyers working at a daily cargo/basis cadence, reading the state of the soy complex before the desk day — plus engineers viewing the project from GitHub. **Not** an execution terminal, a real-time feed, or a CTRM; the visual language must not imply any of the three
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

### Ledger row drill-down (inline SVG, market pages only)
An expansion **under** the clicked ledger row, one open at a time, never a modal — the ledger's claim is comparative and a modal covers the rows being compared against. Two white panes side by side (one column below 768px) on a `#F4F6F3` inset row: the leg in USD/MT with the page's own pinned leg overlaid as a dashed grey line, and the FX component for `home_per_mt` legs only. Soy green for the leg, `--text-dim` dashed for the overlay, `--info` for the FX line; dots are `r=2.2` in the series colour. Inline SVG in the block's palette, not Plotly — Plotly is loaded for the headline's figures and is not worth shipping to eight market pages for a sparkline. Window toggles are mono chips: **30d / 90d / 1y**, 90 default, never "all".

**Under 8 observations a window is dots with no connecting line; under 3 there is no chart at all** — the level, its print date and how few there are, as text. A line asserts a path between prints that was never observed. The rule binds every series drawn, the dashed overlay included: where the pinned leg is too sparse for the selected window the overlay is dropped and the note says why. Axis labels are the observed high and low, never the padded drawing bounds, at whatever precision makes the two labels different numbers; windows wider than half a year carry the year on the date labels.

Collapsed, the USD/MT cell carries a 78px range track with a green tick where today's print sits, and `352–378 since 29 Jul` beneath it — rendered server-side, so it survives script being off. Under 8 observations it reads `4 obs since 06 Aug — no range yet` rather than drawing a range across two points. The chevron and every click affordance are gated on a `.drill-ready` class the script adds: with script off the page loses the drill-down cleanly rather than offering a control that does nothing.

### Embedded third-party chart (workstation contract rows only)
The one place on this site where a live third-party surface is rendered, and the frame is what makes it a **labelled exception** rather than a blurring of the "not a real-time feed" line. Everything about the treatment is deliberately **outside** the Morning Scan palette, so a reader can see where our numbers stop and TradingView's begin.

An inline expansion **under** the clicked contract row — same shape as the ledger drill-down, one open at a time, never a modal. The panel row sits on a cool slate inset `#EEF1F4` with a 3px `--info` (`#2F5D8F`) left border: not paper `#FAFAF7`, not the soy green, not a white card. Above the chart, two lines, both written in `app/workstation_page.py` and never in the template: a mono 10px uppercase `--info` stamp — `Third party · TradingView · approximately 10-minute delayed exchange data` — and a 11.5px muted sentence saying the table's numbers are ours on our timestamps and the chart's are theirs on theirs. Chart frame 420px (320px under 768px), white, 1px `#C7CFD6`.

**The attribution link below the chart is a licence condition, not decoration** (invariant 9): TradingView's terms permit free embedding only with the attribution left as designed. No rule in this project may hide, shrink to nothing, or position `.tv-credit` off-screen, and a test asserts it renders.

The chevron and row affordances are gated on a `.tv-ready` class the script adds after wiring — with script off the page loses the expander cleanly rather than offering a control that does nothing, the same gate `.drill-ready` applies on market pages. The embed is **lazy**: the panel ships empty and hidden, and the TradingView script is injected on first expand, so a page listing a dozen contract months ships zero iframes. A row whose venue has no checked symbol gets no expander at all (`app/tradingview.py`) — a chart of the wrong contract beside a right price is the wrong-number-worse-than-a-gap trade invariant 11 refuses. A failed embed says so in `--bearish` text; an empty grey box beside a price would read as "no trading".

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
| 2026-08-21 | **Weather cards carry a role line and an off-season tag; the headline gains a competing-oil weather strip** | [M14 #207](https://github.com/philipbergman6-glitch/Mirror-Market/issues/207) built by [M24 #271](https://github.com/philipbergman6-glitch/Mirror-Market/issues/271). Two existing components, no new ones. The **role** ("import origin", "rapeseed, not soy") is a 12px muted caption inside the card, because it qualifies the reading rather than competing with it — Dalian's second pin is in Brazil and Europe's are rapeseed, and an unlabelled card reads as this market's own soy crop. **Out of season** is the neutral grey `.kind` chip beside the region name plus the reason in the caption ("out of season — planting ~Oct"), never a colour: nothing has breached, and an amber card would read as an alert. The card is never hidden or collapsed — September Iowa rain is real and prices harvest logistics. The headline's competing-oil weather is a **`.sig` strip**, one row per belt, sharing the Signals row pattern (belt badge + leg name + reading); it is deliberately *not* a metric-card grid, because [M2 #144](https://github.com/philipbergman6-glitch/Mirror-Market/issues/144) took the region cards off the headline and this is not their return. The badge goes `badge-warning` only when a pin in that belt has actually breached |
| 2026-08-21 | **The headline gains a Crush Board (04), and the empty-state CSS follows `.kind` into `_base.html.j2`** | [M16 #208](https://github.com/philipbergman6-glitch/Mirror-Market/issues/208): four markets' crush margins as a 4-up metric row — the existing grid pattern, no new component — each card a market link plus a `.kind` chip, because a board close, an administered minimum and a physical assessment side by side must not read as one "crush" line. A card with no margin keeps its slot as the *same* labelled empty state the market blocks use (`absent` calm, `empty` warm), which is why `.empty-state` moves out of `_market_css.html.j2` into the one `<head>` owner — the identical reasoning that moved `.kind` there. Headline sections renumber 04→05 … 11→12 to seat the board fourth, where [M2 #144](https://github.com/philipbergman6-glitch/Mirror-Market/issues/144) put it; the old section 04 loses its CBOT crush sub-block and becomes plain "Relative Value" |
| 2026-08-21 | **The ledger row opens inline, and a sparse leg is drawn as dots or not at all** | [M11 #160](https://github.com/philipbergman6-glitch/Mirror-Market/issues/160) decided the shape, [M21 #250](https://github.com/philipbergman6-glitch/Mirror-Market/issues/250) built it. Inline expansion under the row, never a modal: this table's claim is comparative, and a modal hides the rows being compared against. The sparse-history rule is by **observation count on the leg** (≥8 line + dots, 3–7 dots only, <3 no chart) and never by age of source — the snapshot legs are dense-but-short while a weekly leg is sparse-but-long, and a connecting line drawn through the holes of a once-a-day source asserts a path nobody observed. The overlay is the **page's own pinned leg**, dashed: CEPEA on Brazil, No.2 on Dalian, because CBOT everywhere would encode the assumption [M12 #161](https://github.com/philipbergman6-glitch/Mirror-Market/issues/161) corrected. **India draws none at all** — no foreign leg is on its ledger, and a dashed series under a mandi one would render the `policy_blocked` +66% as a gap that closes. The FX pane exists because SAFEX moved +0.03% locally and +0.44% in USD on one session and a single USD line cannot say which moved; a `usd_per_mt` leg gets one sentence instead, per M3's dual-quote rule. The headline ledger is untouched: its rows are markets and the market cell is already the affordance |
| 2026-08-23 | **The workstation's contract rows expand into an embedded TradingView chart, framed as foreign territory** | [#320](https://github.com/philipbergman6-glitch/Mirror-Market/issues/320). Owner decision: a third-party live widget is approved, overriding the default "not a real-time feed" stance as a *labelled, deliberate exception* — footnoted in the `CLAUDE.md` mission table so the two documents cannot drift. The visual treatment (slate `#EEF1F4` inset on an `--info` rule, mono third-party stamp, our-numbers-vs-theirs sentence) was chosen by the user from three options; the two rejected ones were a `--bearish` frame, which would read as an error state rather than an exception, and a plain white card in our own palette, which is exactly the blurring the mission table's honesty claim exists to prevent. Lazy-loaded on first expand — the page lists every listed month and a static site must not open with a dozen third-party iframes. CBOT only: `app/tradingview.py` maps venues it has *checked*, and other venues are a registry entry in the follow-up ticket, never a branch (invariant 5) |
