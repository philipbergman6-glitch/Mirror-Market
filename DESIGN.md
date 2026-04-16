# Design System — Mirror Market

## Product Context
- **What this is:** Commodity market intelligence platform monitoring global agricultural markets with 18 data source layers across 27 countries
- **Who it's for:** Defense tech hiring managers and engineers viewing the project from GitHub as a resume portfolio piece
- **Space/industry:** Commodity trading, agricultural markets, fintech dashboards
- **Project type:** Data-heavy analytics dashboard (static HTML, generated from Python)

## Aesthetic Direction
- **Direction:** Industrial/Utilitarian — Bloomberg Terminal meets modern developer tools
- **Decoration level:** Minimal — typography and data do all the work
- **Mood:** Serious, functional, competent. Should feel like a real tool built by someone who understands markets, not a tutorial project or Dribbble eye-candy
- **Reference sites:** Bloomberg Terminal, GitHub dark theme, Fortress (Bloomberg-inspired Next.js template)

## Typography
- **Display/Hero:** Geist (600-700 weight) — clean, modern, built for developer tools. Signals engineering craft over generic fintech
- **Body:** Geist (400 weight) — same family for visual cohesion
- **UI/Labels:** Geist (500 weight, 11px uppercase with 0.04em tracking)
- **Data/Numbers:** Geist with `font-variant-numeric: tabular-nums` — aligned decimal places in metric cards and tables
- **Code/Briefing:** JetBrains Mono (400-500 weight) — for the briefing text section and data freshness timestamps
- **Loading:** Google Fonts CDN (`https://fonts.googleapis.com/css2?family=Geist:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500`)
- **Scale:**
  - Page title: 28px / weight 700 / tracking -0.02em
  - Section head: 20px / weight 600 / tracking -0.01em
  - Card title: 13px / weight 600
  - Body: 14px / weight 400 / line-height 1.5
  - Label: 11px / weight 500 / uppercase / tracking 0.04em
  - Caption: 12px / weight 400 / muted color
  - Data value: 28px / weight 600 / tabular-nums / tracking -0.02em
  - Mono: 12-13px / weight 400 / line-height 1.7

## Color
- **Approach:** Restrained dark palette — GitHub dark as the foundation, green accent from original brand
- **Surfaces:**
  - Background: `#0D1117`
  - Surface: `#161B22`
  - Card: `#1C2128`
  - Card hover: `#21262D`
  - Border: `#30363D`
- **Text:**
  - Primary: `#E6EDF3`
  - Muted: `#7D8590`
  - Dim: `#484F58`
- **Brand:**
  - Green accent: `#2D6A4F` — nav active state, section headers, primary buttons
  - Green accent light: `#40916C` — hover states, tab underlines
- **Directional:**
  - Bullish: `#3FB950`
  - Bearish: `#F85149`
- **Commodity:**
  - Soybean: `#DAA520` (goldenrod)
  - Soy Oil: `#FF8C00` (dark orange)
  - Soy Meal: `#CD853F` (peru)
- **Semantic:**
  - Success: `#3FB950` (same as bullish)
  - Warning: `#D29922`
  - Error: `#F85149` (same as bearish)
  - Info: `#58A6FF`
- **Alert backgrounds:** Semantic color at 8% opacity (e.g., `rgba(63,185,80,0.08)`) with 3px left border
- **Badge backgrounds:** Semantic color at 15% opacity with text in the semantic color

## Spacing
- **Base unit:** 8px
- **Density:** Comfortable
- **Scale:** 2px, 4px, 8px, 12px, 16px, 20px, 24px, 32px, 48px
- **Card padding:** 16px vertical, 20px horizontal
- **Grid gap:** 16px between cards
- **Section margin:** 24px between sections

## Layout
- **Approach:** Grid-disciplined
- **Navigation:** Fixed left sidebar, 220px wide
  - Brand header with title + subtitle
  - Nav items: icon (16px) + label, 8px vertical padding, 16px horizontal
  - Active: green accent background, white text
  - Bottom section: data freshness indicators with colored dots
- **Grid:**
  - 3-column for soy leg metric cards
  - 4-column for key metrics row
  - Full-width for charts and signals
- **Max content width:** 1400px
- **Border radius:** 6px for cards/containers, 4px for buttons/inputs, 3px for badges, 10px for pills/counts, 2px for color dots
- **Responsive:** Cards stack to 1 column below 768px, sidebar collapses to top bar

## Motion
- **Approach:** Minimal-functional
- **Tab switches:** Instant `display` toggle (no animation)
- **Hover states:** `background` transition 0.1s, `color` transition 0.1s
- **Collapsible arrow:** `transform` rotate 0.15s
- **Charts:** Plotly handles all chart interactivity (hover, zoom, pan) natively

## Component Patterns

### Metric Card
```
.metric-card {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px 20px;
}
- Label: 11px uppercase muted, with optional color dot
- Value: 28px weight 600 tabular-nums, colored by commodity
- Delta: 13px weight 500, green/red by direction
- Sub-metrics: 12px muted, flex row with 16px gap
```

### Signal Item
```
- Flex row: badge (10px uppercase, colored background) + commodity name (100px, muted) + description text
- Separated by 1px border-bottom
- Count shown in green pill next to section header
```

### Data Table
```
- Headers: 11px uppercase dim, left-aligned (numbers right-aligned)
- Cells: 13px primary text, tabular-nums, 8px vertical padding
- Row hover: card-hover background
- No zebra striping — border-bottom separation only
```

### Briefing Block
```
- JetBrains Mono, 12px, line-height 1.7
- Section headers in green accent light, weight 600
- Bullish values in green, bearish in red
- White-space: pre-wrap
```

### Collapsible (details/summary)
```
- Native HTML <details><summary>
- Triangle arrow rotates on open
- Card background with border
```

## Decisions Log
| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-04-16 | GitHub dark palette over pure black | Proven readable for dense data, cohesive with where repo visitors see the project |
| 2026-04-16 | Geist typography over generic sans-serifs | Signals developer craft, distinguishes from template dashboards |
| 2026-04-16 | Dark theme only (no light mode toggle) | Trading desks use dark themes. Simplifies implementation. Consistent with product identity |
| 2026-04-16 | Minimal decoration | Data-first approach. Typography and color hierarchy do the visual work |
| 2026-04-16 | Fixed sidebar nav over top nav | Standard for data dashboards. Provides persistent navigation + data freshness at a glance |
