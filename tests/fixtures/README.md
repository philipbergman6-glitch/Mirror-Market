# Scraper test fixtures

These files are the inputs the scraper unit tests use to verify HTML/CSV
parsing without hitting the network.

## Provenance

| File | Source | Captured |
|------|--------|----------|
| `ams_inspections.txt` | USDA AMS report `wa_gr101.txt` | live download 2026-05-11 |
| `safex_grainsa.html` | Grain SA SAFEX feeds page | live download 2026-05-11 |
| `cepea_soybean.html` | CEPEA/ESALQ soybean indicator | synthetic (live returned HTTP 403 anti-bot) |
| `ncdex_bhavcopy.csv` | NCDEX Bhav Copy | synthetic (live URL templates 404 — see config.py) |
| `agrural_paranagua.html` | AgRural soja+milho price page | live download 2026-05-11 |
| `noticias_agricolas_parana.html` | Notícias Agrícolas CEPEA/ESALQ Paraná soy indicator | live download 2026-08-07 |
| `noticias_agricolas_milho.html` | Notícias Agrícolas ESALQ/B3 corn indicator (corn-redirect fixture — must *fail* the soy parse) | live download 2026-08-07 |
| `cot_annualof_2026_sample.txt` | CFTC Legacy futures+options annual file `annualof.txt` from `https://cftc.gov/files/dea/history/deahistfo2026.zip` — real header + 4 real rows per tracked market (11,292 → 41 lines) | live download 2026-08-11 |
| `cot_annualof_2010_sample.txt` | Same report for 2010, `https://cftc.gov/files/dea/history/deahistfo2010.zip` — real header + 2 real rows per market. Exhibits a genuine CFTC market rename: SRW wheat is `WHEAT - CHICAGO BOARD OF TRADE` (today `WHEAT-SRW - CHICAGO BOARD OF TRADE`), canola is absent, and every name carries a trailing space | live download 2026-08-11 |
| `cftc_landing_200.html` | `https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm` — cftc.gov answering HTTP 200 with HTML where the fetcher expects a zip; first 8,192 bytes of the real response. Feeding it to `zipfile.ZipFile` raises a genuine `BadZipFile` | live download 2026-08-11 |

## Treat fixtures as snapshots

When the live site changes shape (column renamed, table moved, format
swapped), the parser will start raising `ScraperShapeError` and tests
will continue to pass against this frozen fixture. **That divergence
is the alert.** Re-capture the fixture only after the parser has been
updated for the new shape.

## Re-capturing live fixtures

```bash
.venv/bin/python -c "
import requests
ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36'
r = requests.get('https://www.ams.usda.gov/mnreports/wa_gr101.txt',
                 headers={'User-Agent': ua}, timeout=20)
open('tests/fixtures/ams_inspections.txt', 'wb').write(r.content)
"
```
