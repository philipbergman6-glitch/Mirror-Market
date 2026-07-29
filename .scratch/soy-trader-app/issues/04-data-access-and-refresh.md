# 04 — Data access & refresh model

Type: grilling
Status: open
Blocked by: 02

## Question

How does the app read data and stay current? Decide: direct calls into `pipeline/query.py` / `analysis/` vs an API layer between UI and DB; whether the app can trigger `main.py` (refresh button, scheduler) or relies on external pipeline runs; caching (reuse/replace `analysis/loaders.py` cache); and how stale/disabled layers (NCDEX, CEPEA) and `data_freshness` surface to the UI contract.
