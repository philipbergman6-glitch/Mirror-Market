# Tech stack comparison — Soy Trader App (ticket 02)

Researched 2026-07-28 via three parallel web-research agents (sources cited inline in the per-stack notes below).
Context: local, single-user, ~11-screen app; codebase already has 11 Plotly `go.Figure` builders (`app/charts.py`), 23 pandas `read_*()` functions (`pipeline/query.py`), and `jinja2` + `plotly` in requirements.

## Scorecard

| Criterion | FastAPI + Jinja2/HTMX + Plotly | Dash 4.x | Streamlit | Vite+React SPA + FastAPI API |
|---|---|---|---|---|
| App-like feel | Strong — `hx-boost` nav + partial swaps; sub-ms local latency makes it SPA-indistinguishable | Strong — client-side-routed SPA, targeted no-flicker callback updates | Weak — whole-script rerun, visible flicker/redraw, "notebook app" feel | Strongest — true SPA, instant transitions |
| Python-first reuse | **Best** — figure builders, query functions, existing `dashboard.html.j2` + CSS drop straight into routes; no new layer | Very good — `dcc.Graph(figure=fig)` native; queries slot into callbacks | Very good — `st.plotly_chart(fig)` one-liner | Good — figures pass as `fig.to_json()` → react-plotly.js (no chart rewrite), but 23 query functions need a JSON API layer |
| Speed to v1 (solo Python dev) | **~1–2 weeks** — no build step, no node, refactor of existing static generator into routes | ~1–2 weeks — modest callback boilerplate (~1/page), hand-written layout | Days to rough v1, but custom-design requirement erases the advantage via CSS-hack time | ~3–6 weeks — Node toolchain, React/Tailwind learning curve, API plumbing |
| Fresh visual design control | **Total** — you author all HTML/CSS | Full — own DOM via `className` + `/assets` CSS, no fragility | **Fails requirement** — theme tokens only; custom layout/chrome needs unsupported `data-testid` DOM hacks | Total — Tailwind/shadcn, unconstrained |
| Desktop wrap later (note only) | Clean — pywebview shell or Tauri v2 sidecar; app unchanged | Clean — plain Flask under the hood; pywebview pattern documented | Turnkey but fragile (`streamlit-desktop-app`, single maintainer, Python <3.13 pin) | Cleanest documented path — Tauri v2 + PyInstaller sidecar templates assume exactly this shape |

## Key findings per stack

### FastAPI + Jinja2/HTMX + Plotly.js — **recommended**
- Well-trodden 2025–26 pattern for internal tools/dashboards; HTMX 2.0.9 stable. Ergonomic key: `jinja2-fragments` (render one `{% block %}` as a partial — same template serves full page and HTMX fragment).
- Plotly embed: `fig.to_html(full_html=False, include_plotlyjs=False)` per chart; plotly.js loaded once in base head (vendored for offline). HTMX executes swapped `<script>` tags → picker-driven chart swap works with zero custom JS. Zoom/hover fully client-side.
- Alpine.js for client-only state (picker open, tabs). Minor known frictions: Alpine state on back-button history restore, chart containers best marked `hx-history="false"`.
- Unusually good codebase fit: v1 is essentially the current static generator refactored into ~11 routes + partial endpoints.

### Dash 4.4.x — strong runner-up
- Now on 4.x (4.4.1, Jul 2026; pin ≥4.4.1 for a background-callback security fix). Built-in Pages multipage, full CSS/DOM control via `/assets`, native `go.Figure`, no-flicker targeted updates. `dash_table` deprecated → use dash-ag-grid.
- Cost vs FastAPI+HTMX: layout trees composed in Python (`html.Div(...)`) instead of real templates, one callback per interaction, and it discards the existing Jinja template. Chooses framework structure over direct HTML authorship — no wall, but no reuse advantage either.

### Streamlit — eliminated
- Theming improved a lot (custom fonts, chart palettes via `config.toml`), but layout is fundamentally linear top-down; custom nav/chrome/layout requires brittle unsupported CSS hacks (even Microsoft maintains a hack-template repo). Whole-script rerun model flickers. Fails the fresh-custom-design hard requirement.

### Vite+React SPA + FastAPI JSON API — capable but 2–3× slower
- The usual dealbreaker doesn't apply: `fig.to_json()` → react-plotly.js v4 (revived Jul 2026) renders server-built figures with no JS chart rewrite. FastAPI 0.138 added native `app.frontend()` SPA serving.
- But: two runtimes, build tooling, 23 endpoints with Pydantic/NaN/date serialization, React+Tailwind learning curve — published 2026 estimates put custom FastAPI+React at 2–6 weeks vs days-to-2-weeks for server-rendered Python. Overkill for local single-user with no auth/SEO/concurrency needs. Best kept as the fallback if HTMX ever hits an interactivity wall (unlikely for read-mostly dashboards).

## Recommendation

**FastAPI + Jinja2 + HTMX (+ Alpine.js) + Plotly.js**, with **Dash 4.4.x** as the named fallback.

Rationale: it maximizes the dominant criterion — reuse of the existing Python assets (figure builders, query API, Jinja template, design CSS) — while giving total control for the fresh design, the fastest credible path to v1, and a clean later desktop wrap (pywebview or Tauri sidecar; vendor plotly.js locally for offline). The React SPA buys nothing this app needs at 2–3× the cost; Streamlit fails the design requirement; Dash is close but trades the direct-HTML/template reuse for framework structure without an offsetting gain.

Supporting libraries to adopt at build time: `jinja2-fragments`, HTMX 2.x, Alpine.js 3.x, vendored plotly.js.
