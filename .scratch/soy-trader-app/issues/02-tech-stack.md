# 02 — Tech stack selection

Type: research
Status: open — paused (stack confirmation deferred 2026-07-28)
Assignee: none

## Question

Which stack builds the local web app? Compare at least: FastAPI + server-rendered UI (Jinja/HTMX + Plotly), Streamlit or Dash, and a React/Next SPA + Python API. Criteria: app-like feel and interactivity, Python-first (reuse `pipeline/query.py`, `analysis/`, `app/charts.py`), speed to v1, control over a fresh visual design, and (noted only, not deciding) how cleanly each wraps into a desktop app later. Output: a markdown comparison with a recommendation; the human confirms the pick.

---

**Paused 2026-07-28.** Research is complete — see [`assets/02-tech-stack-comparison.md`](../assets/02-tech-stack-comparison.md), recommending FastAPI + Jinja2 + HTMX with Dash 4.4.x as fallback. The human deferred confirmation: the whole effort is shelved (static GitHub Pages dashboard stays; app only gets built if a buyer appears). On resume, this ticket needs only the confirmation step.
