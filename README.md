# Mirror Market

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-2D6A4F?style=for-the-badge)](https://philipbergman6-glitch.github.io/Mirror-Market/)

## The Big Picture

Every morning, a soybean trader needs to answer one question: **"What moved overnight, and why?"**

Answering it properly means checking dozens of places — futures prices, USDA reports, Brazilian crop estimates, Chinese demand, weather in six continents, currency moves, what big funds are betting. Mirror Market does that checking automatically.

It is three things stacked on top of each other:

1. **A data collector** — a Python script that pulls from 19 free public sources every weekday and saves everything into one database.
2. **An analysis engine** — code that turns that raw data into plain conclusions: "prices are overbought," "Brazil is undercutting US beans," "funds are crowded into this trade."
3. **A morning dashboard** — a single scrolling web page, rebuilt daily and published free on GitHub Pages, that presents it all in the order a trader would scan it.

The focus is the **soy complex** — soybeans and the two products they're crushed into, soybean oil and soybean meal — plus the crops that compete with them (corn, palm oil, wheat). All prices are converted to **US dollars per metric ton** so a Chicago price, a Brazilian price, and a South African price can sit on the same chart.

## The Dashboard

One page, in the order you'd scan it each morning, every section collapsible:

**01 Overnight** — what prices did · **02 Signals** — anything unusual, ranked by urgency · **03 Crush & Relative Value** — is processing beans profitable, and how does soy compare to rivals · **04 Supply & Demand** — how much the world is growing, buying, and holding · **05 Risk** — currencies, fund positioning, weather · **06 Forward Curves** — what future months cost vs today · **07 Seasonal** — how this year compares to a normal year · **08 Technicals** — chart indicators · **09 Full Briefing** — the whole text report · **10 About**

## Where the Data Comes From (all free)

| # | Source | What it provides |
|--:|--------|------------------|
| 1 | Yahoo Finance | Daily prices for 11 commodity futures |
| 2 | USDA NASS* | US harvest sizes, yields, weekly crop health ratings |
| 3 | FRED* | The economic backdrop: dollar strength, inflation, interest rates |
| 4 | CFTC | What big speculators are betting (published weekly) |
| 5 | Open-Meteo | Weather in 24 growing regions across 6 continents |
| 6 | USDA PSD | Global supply and demand, 8 commodities × 27 countries |
| 7 | Yahoo Finance | 13 currency pairs (a weak Brazilian real makes Brazilian beans cheaper) |
| 8 | World Bank | Monthly benchmark prices (palm oil, robusta coffee) |
| 9 | AKShare | Chinese futures prices — what the biggest soy buyer is paying |
| 10 | USDA FAS* | Weekly US export sales and who bought |
| 11 | Yahoo Finance | Prices for future delivery months |
| 12 | USDA WASDE | The monthly USDA forecast the whole market trades on |
| 13 | EIA* | Ethanol, biodiesel, diesel — fuel demand for crops |
| 14 | USDA* | How many beans got crushed, how many got shipped |
| 15 | CONAB | Brazil's official crop estimates (their version of the USDA) |
| 16 | NCDEX | India domestic soy *(disabled — site blocks scrapers)* |
| 17 | CEPEA | Brazil farm-gate soy *(disabled — site blocks scrapers)* |
| 18 | JSE SAFEX | South Africa's soy futures exchange |
| 19 | AgRural | Soy prices at Brazil's main export port |

\* needs a free API key (`USDA_API_KEY`, `FRED_API_KEY`, `FAS_API_KEY`, `EIA_API_KEY`). **14 of 19 sources need no key at all**, and if any one source fails, the rest still run.

## What the Analysis Actually Tells You

In plain terms, each piece answers a question a trader would ask:

- **Technicals** — *"What does the chart say?"* Standard indicators (moving averages, RSI, MACD, Bollinger Bands) that flag momentum and overbought/oversold conditions.
- **Signals** — *"Did anything just change?"* Automatic alerts when indicators cross meaningful lines, ranked by severity so the urgent ones surface first.
- **Crush spread** — *"Is it profitable to process soybeans?"* Beans are bought, crushed, and sold as oil + meal. The spread is the processor's margin — when it's fat, demand for beans rises.
- **Brazil basis** — *"Is Brazil undercutting the US?"* The gap between the price at Brazil's export port and the Chicago benchmark. Brazil is the US's biggest competitor; this gap shows who's winning export business.
- **Stocks-to-use** — *"How tight is supply?"* Leftover inventory as a fraction of yearly consumption. Low number = little cushion = prices react violently to bad news.
- **Positioning z-scores** — *"Is the trade crowded?"* When speculative funds are all leaning the same way to a historically extreme degree, a snap-back gets more likely.
- **Market Drivers** — *"How does it fit together?"* A written narrative connecting the layers: the Brazilian real and exports, fund crowding, weather premiums, dollar impact, China's buying pace.
- **Daily briefing** — the full 26-section text report, archived to the database each day so past calls can be checked against what actually happened.

## Run It Yourself

```bash
pip install -r requirements.txt

python main.py                      # collect all 19 data sources into the database
python scripts/generate_html.py     # build the dashboard → docs/index.html
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"   # print today's briefing
```

## Deployment

GitHub Actions runs the whole thing unattended: every weekday at 12:00 UTC (and on every push to `main`) it collects the data, rebuilds the dashboard, and republishes it to GitHub Pages. API keys live in repository secrets. Storage is a local SQLite file by default; set `TURSO_DATABASE_URL` + `TURSO_AUTH_TOKEN` for cloud storage instead.

## How the Code Is Organized

```
main.py            runs the whole collection pipeline, source by source
fetchers/          one module per data source (the "collectors")
pipeline/          cleaning, validation, unit conversion, database read/write
analysis/          turns stored data into indicators, signals, and the briefing
app/               chart builders + the dashboard's HTML template
scripts/           the script that assembles the dashboard page
docs/index.html    the finished dashboard that GitHub Pages serves
```

Design system in `DESIGN.md` · architecture details in `ARCHITECTURE.md` and `CLAUDE.md`.

---

*A project by Philip Bergman. Free public data, delayed, provided as-is — not financial advice.*
