# 03 — Visual design direction + command-center prototype

Type: prototype
Status: closed (2026-07-28)
Assignee: philipbergman6-glitch (claimed 2026-07-28)
Blocked by: 01

## Question

What does the app look like? Produce 2–3 rough visual directions (fresh design, evolving DESIGN.md rather than inheriting it) applied to the morning command-center home screen — the 5-minute scan: soy complex prices + overnight changes, signals by severity, crush spread, Brazil basis, freshness warnings. Human reacts and picks a direction; the chosen mockup becomes the design reference for v1 and the seed for an updated DESIGN.md.

## Resolution (2026-07-28)

Three directions were prototyped against the command-center content (soy legs + overnight changes, signals by severity, crush headline, Brazil basis headline, freshness/weather/stocks-to-use alerts), all in `assets/design-directions/`:

- **A — The Tape** (`direction-a-the-tape.html`): dense dark terminal — near-black #0A0C10, amber accent, IBM Plex Mono data, hairline grid, zero radius, ticker-tape header.
- **B — Crush** (`direction-b-crush.html`): warm dark from soy's own materials — umber ground, harvest-gold/oil-orange/meal-tan, Fraunces serif display, crush-flow hero (beans → oil + meal = margin).
- **C — Morning Scan** (`direction-c-morning-scan.html`): light editorial front page — paper #FAFAF7, ink, retained soy-green #2D6A4F accent, Archivo masthead, sections numbered in scan order (01 the complex overnight / 02 what fired / 03 crush & basis / 04 on watch), IBM Plex Mono for data.

**Chosen: C — Morning Scan** (agent recommendation, human confirmed). Deciding factor — audience clarification from the human: the app is for **business professionals**, and should be the **most professional and simplest** take. C reads like a polished daily brief with zero learning curve; A is trader-dense, B too decorative.

**Design reference for v1:** `assets/design-directions/direction-c-morning-scan.html`. It seeds the updated DESIGN.md (written as part of the v1 spec/build): light theme, Archivo display + Inter body + IBM Plex Mono data, paper/ink/soy-green palette, numbered scan-order sections as the command-center structure, freshness stale-note under the masthead.
