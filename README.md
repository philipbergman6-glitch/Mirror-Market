# Mirror Market

[![Live Dashboard](https://img.shields.io/badge/Live_Dashboard-GitHub_Pages-2D6A4F?style=for-the-badge)](https://philipbergman6-glitch.github.io/Mirror-Market/)

## The Big Picture

Every morning, a soybean trader needs to answer one question: **"What moved overnight, and why?"**

Answering it properly means checking dozens of places — futures prices, USDA reports, Brazilian crop estimates, Chinese demand, weather in six continents, currency moves, what big funds are betting. Mirror Market does that checking automatically.

It is three things stacked on top of each other:

1. **A data collector** — a Python script that runs 31 operational data layers (28 numbered source groups plus three independently graded sub-layers) every weekday and saves everything into one database.
2. **An analysis engine** — code that turns that raw data into plain conclusions: "prices are overbought," "Brazil is undercutting US beans," "funds are crowded into this trade."
3. **A morning site** — a headline page plus one page per market, rebuilt daily and published free on GitHub Pages, presented in the order a trader would scan it.

The focus is the **soy complex** — soybeans and the two products they're crushed into, soybean oil and soybean meal — plus the crops that compete with them (corn, palm oil, wheat). All prices are converted to **US dollars per metric ton** so a Chicago price, a Brazilian price, and a South African price can sit on the same chart.

## The Site

**The headline page**, in the order you'd scan it each morning:

**01 Overnight** — what prices did · **02 Signals** — anything unusual, ranked by urgency · **03 Propagation ledger** — who has repriced, and who has not printed · **04 Crush & Relative Value** — is processing beans profitable, and how does soy compare to rivals · **05 Supply & Demand** — how much the world is growing, buying, and holding · **06 Risk** — currencies, fund positioning, weather · **07 Forward Curves** — what future months cost vs today · **08 Seasonal** — how this year compares to a normal year · **09 Technicals** — deep charts · **10 Full Briefing** — the whole text report

**One page per market** — CBOT, Dalian, Brazil, Argentina, India, Europe, South Africa, Nigeria — each built from the same nine blocks (prices, propagation ledger, crush, basis, weather, supply & demand, flows, positioning, data health). The market is a parameter, not a code path: a new market is a registry entry, not new code.

Each market page is **tiered from the data every run** — a full page, a brief, or a stub — so a market whose source went dark demotes itself and says so, rather than publishing a confident-looking empty page. The URL never changes with the tier, so yesterday's link never 404s.

## Where the Data Comes From (all free)

| # | Source | What it provides |
|--:|--------|------------------|
| 1 | Yahoo Finance | Daily prices for 10 commodity futures |
| 2 | USDA NASS* | US harvest sizes and yields |
| 2b | USDA NASS* | Weekly/seasonal crop progress and condition |
| 3 | FRED* | The economic backdrop: dollar strength, inflation, interest rates |
| 4 | CFTC | What big speculators are betting (published weekly) |
| 5 | Open-Meteo | Weather in 19 growing regions across 6 continents |
| 6 | USDA PSD | Global supply and demand, 10 commodities × 28 countries |
| 7 | Yahoo Finance | 10 currency pairs (a weak Brazilian real makes Brazilian beans cheaper) |
| 8 | World Bank | Monthly benchmark prices (palm oil, rapeseed oil) |
| 9 | AKShare | Chinese futures prices — what the biggest soy buyer is paying |
| 10 | USDA FAS* | Weekly US export sales and who bought |
| 11 | Yahoo Finance | Prices for future delivery months |
| 12 | USDA WASDE | The monthly USDA forecast the whole market trades on |
| 13 | EIA* | Ethanol, biodiesel, diesel — fuel demand for crops |
| 14 | USDA* | How many beans got crushed, how many got shipped |
| 15 | CONAB | Brazil's official crop estimates (their version of the USDA) |
| 15b | CONAB | Weekly Paraná farmgate prices |
| 16 | data.gov.in / Agmarknet | India domestic bean prices at the mandis (Madhya Pradesh + Maharashtra) |
| 17 | CEPEA via Notícias Agrícolas | Brazil farm-gate and Paranaguá soy indicators |
| 18 | JSE SAFEX via Grain SA | South Africa's soy futures exchange |
| 19 | AgRural | Soy prices at Brazil's main export port |
| 20 | USDA AMS | Daily US Gulf export bids (CIF NOLA barge) |
| 21 | Argentina MAGyP | Official daily FOB export values, incl. the only free daily sunflower oil benchmark |
| 22 | European Commission | Weekly EU rapeseed FOB assessment |
| 23 | SAGIS | South Africa weekly producer deliveries — the country's physical flow series |
| 24 | SAGIS | South Africa monthly soybean supply & demand balance (incl. crush volume) |
| 25 | Crop Estimates Committee (SA) | South Africa's official monthly crop estimate, with its in-season revision path |

\* needs a free API key (`USDA_API_KEY`, `FRED_API_KEY`, `FAS_API_KEY`, `EIA_API_KEY`, and optionally `DATA_GOV_IN_API_KEY`). **25 of 31 operational layers need no private key**, and if any one contextual source fails, the rest still run and the degradation remains visible.

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

python main.py                      # run all 31 operational layers
python scripts/generate_site.py     # build the whole site → docs/
python scripts/generate_site.py --only india    # or one page, for the dev loop
python -c "from analysis.briefing import generate_briefing; print(generate_briefing())"   # print today's briefing
```

## Trust — how the numbers are kept honest

Free public data fails in quiet ways. Most of the engineering here is about refusing to publish a wrong number:

- **"Success" requires new rows, not just a 200 OK.** A source that serves last month's file every day is recorded as *failed*, ages out of its freshness window, and says so on every page. Same for a source that answers with nothing, or that answers for only half the keys it was asked for.
- **Half a walk is a wrong number, not a missing one.** India's daily price is a median across ~115 reporting markets — a truncated fetch yields a plausible wrong figure with nothing in its shape marking it partial, so it hard-fails instead.
- **Unsettled prices are dropped, not stored.** A run landing mid-session would otherwise save an unfinished bar as the day's close. The site prints a gap rather than a wrong close.
- **Our outage never reads as the market's silence.** When a page is thin because *our* ingest broke, it says so, with the date of the last good run.
- **A basis says whether trade can actually close it.** India's bean trades ~+66% over Chicago and nothing arbitrages it — GM imports are banned behind a tariff wall — so it renders as a labelled policy spread, never as a tradeable one.
- **A failure is isolated three ways**: a broken block renders an empty state with its reason, a broken page becomes a diagnostic tombstone only inside the private candidate, and a broken headline fails the candidate. A candidate with a tombstone never replaces the last trustworthy public edition.

## Deployment

GitHub Actions runs the whole thing unattended: daily (and on every push to `main`) it collects the data and builds a private candidate. The promotion contract requires current soy-complex benchmarks, a generated briefing, aligned required calculations, all expected pages without tombstones, valid timestamps, consistent 27-layer counts, valid internal links, and overflow-free desktop/mobile rendering. Only a passing candidate is uploaded to GitHub Pages; otherwise the previous trustworthy edition remains public. A second smoke test reads the deployed public URLs. The schedule targets a landing window after Chicago settlement, while settlement and freshness guards enforce correctness independently of cron timing. Snapshot-only observations and operational freshness state round-trip through committed CSVs so an ephemeral runner retains both history and the age of the last known good run.

### Trusted Static Preview

To preview a verified or candidate trust edition without changing the public dashboard, render it into a candidate directory first:

```bash
.venv/bin/python scripts/publish_trusted_static.py \
  --trust-repository data/v2 \
  --edition-id edn_... \
  --candidate-root build/trusted-static-candidates
```

By default this writes `build/trusted-static-candidates/<edition-id>/` and prints a JSON summary. It does not mutate `docs/`.

Only publish the rendered candidate explicitly:

```bash
.venv/bin/python scripts/publish_trusted_static.py \
  --trust-repository data/v2 \
  --edition-id edn_... \
  --candidate-root build/trusted-static-candidates \
  --deploy \
  --public-dir docs
```

## How the Code Is Organized

```
main.py            runs the whole collection pipeline, source by source
fetchers/          one module per data source (the "collectors")
pipeline/          cleaning, validation, unit conversion, database read/write
analysis/          turns stored data into indicators, signals, and the briefing
app/               the market registry, the nine block builders, and the HTML templates
scripts/           generate_site.py — the orchestrator that renders every page
docs/              the finished site that GitHub Pages serves
```

Design system in `DESIGN.md` · architecture details in `ARCHITECTURE.md` and `CLAUDE.md`.

---

*A project by Philip Bergman. Free public data, delayed, provided as-is — not financial advice.*
