# 01 — Information architecture: layers → screens

Type: grilling
Status: closed (2026-07-28)
Assignee: philipbergman6-glitch (claimed 2026-07-28)

## Question

How do the 19 pipeline layers and the analysis outputs (briefing sections, `soy_analytics.py`'s 9 analyst functions) organize into the app's screens? Decide: the full screen/page list, what lives on the command center vs drill-downs, how "physical" (spot, basis, PSD, CONAB, weather, crush volumes) vs "futures" (prices, curves, COT, DCE, spreads) structure the navigation, and where the daily text briefing surfaces. Output: a screen inventory with each data source assigned to exactly one home.

## Resolution (2026-07-28)

**Nav model:** the 9 existing `soy_analytics.py` analyst screens plus two additions (Macro & FX, Briefing), grouped in the nav under trader-workflow headings PHYSICAL / FUTURES / CROSS. No re-slicing of analyst payloads.

**Screen inventory (11 screens, each layer has exactly one home):**

- **Command Center** (home) — 5-minute scan, states and alerts only, no exploratory charts. Contents: soy legs + overnight changes, signals by severity, crush headline, Brazil basis headline, freshness warnings, weather alerts, stocks-to-use tight-supply alerts. Note: existing `command_center()` returns legs/crush/signals/key_metrics — needs basis + freshness (+ weather/S2U alerts) added.
- **PHYSICAL**
  - **Supply** — L2 USDA crop data, L2b crop progress, L5 weather, L6 PSD, L12 WASDE, L15 CONAB, stocks-to-use detail
  - **Demand** — L10 export sales, L13 EIA biofuel, L14 crush + inspections
  - **Emerging Markets** — L16 India NCDEX, L17 CEPEA, L18 SAFEX, Nigeria deep dive (must render disabled/stale layers gracefully)
- **FUTURES** — each screen gets a commodity picker defaulting to the soy complex, covering all tracked commodities
  - **Technicals** — L1 prices, MA/RSI/MACD/Bollinger (all 11 futures via picker)
  - **Forward Curve** — L11 curves, contango/backwardation, calendar spreads (picker)
  - **Risk** — L4 COT (10 commodities via picker), volatility
- **CROSS**
  - **Relative Value** — crush detail, bean/corn + oil/meal + palm-vs-soy ratios, L9 DCE vs CBOT, **L19 Brazil basis detail** (basis is a cross-market comparison, so it lives here, not EM/Demand)
  - **Seasonal** — seasonal norms vs current
  - **Macro & FX** (new screen) — L3 FRED (DXY, CPI, Fed funds, 2s10s recession signal, PPIs, diesel), L7 all 13 currency pairs with trade impact, L8 World Bank monthly benchmarks
  - **Briefing** (new screen) — today's full text briefing + date-picker archive browsing the `briefings` table; briefing stays a distinct artifact, command center stays visual
