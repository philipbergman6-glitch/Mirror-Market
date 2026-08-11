# Scraper test fixtures

These files are the inputs the scraper unit tests use to verify HTML/CSV
parsing without hitting the network.

## Provenance

| File | Source | Captured |
|------|--------|----------|
| `ams_inspections.txt` | USDA AMS report `wa_gr101.txt` | live download 2026-05-11 |
| `ams_inspections_2026-08-06.txt` | USDA AMS report `wa_gr101.txt` (https://www.ams.usda.gov/mnreports/wa_gr101.txt), week ending AUG 06 2026 — 6-column Table C / destination layout: WHEAT, CORN YELLOW, CORN WHITE, SORGHUM, SOYBEANS, CANOLA (no RYE/FLAXSEED) | live download 2026-08-11 |
| `safex_grainsa.html` | Grain SA SAFEX feeds page | live download 2026-05-11 |
| `cepea_soybean.html` | CEPEA/ESALQ soybean indicator | synthetic (live returned HTTP 403 anti-bot) |
| `ncdex_bhavcopy.csv` | NCDEX Bhav Copy | synthetic (live URL templates 404 — see config.py) |
| `agrural_paranagua.html` | AgRural soja+milho price page | live download 2026-05-11 |
| `noticias_agricolas_parana.html` | Notícias Agrícolas CEPEA/ESALQ Paraná soy indicator | live download 2026-08-07 |
| `noticias_agricolas_milho.html` | Notícias Agrícolas ESALQ/B3 corn indicator (corn-redirect fixture — must *fail* the soy parse) | live download 2026-08-07 |
| `cot_annualof_2026_sample.txt` | CFTC Legacy futures+options annual file `annualof.txt` from `https://cftc.gov/files/dea/history/deahistfo2026.zip` — real header + 4 real rows per tracked market (11,292 → 41 lines) | live download 2026-08-11 |
| `cot_annualof_2010_sample.txt` | Same report for 2010, `https://cftc.gov/files/dea/history/deahistfo2010.zip` — real header + 2 real rows per market. Exhibits a genuine CFTC market rename: SRW wheat is `WHEAT - CHICAGO BOARD OF TRADE` (today `WHEAT-SRW - CHICAGO BOARD OF TRADE`), canola is absent, and every name carries a trailing space | live download 2026-08-11 |
| `cftc_landing_200.html` | `https://www.cftc.gov/MarketReports/CommitmentsofTraders/index.htm` — cftc.gov answering HTTP 200 with HTML where the fetcher expects a zip; first 8,192 bytes of the real response. Feeding it to `zipfile.ZipFile` raises a genuine `BadZipFile` | live download 2026-08-11 |
| `conab_serie_historica_graos.txt.gz` | CONAB `https://portaldeinformacoes.conab.gov.br/downloads/arquivos/SerieHistoricaGraos.txt` (gzip of the verbatim 2,722,833-byte body; latin-1) | live download 2026-08-11 |
| `conab_serie_historica_graos.headers.txt` | response headers of that same download (carries `Last-Modified: Tue, 11 Aug 2026 11:00:16 GMT` — the survey's publication date) | live download 2026-08-11 |
| `conab_precos_semanal_uf.txt.gz` | CONAB `PrecosSemanalUF.txt` — `https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt` | live download 2026-08-11 (HTTP 200, 13,608,731 bytes, 89,700 lines; gzipped verbatim for the repo, latin-1) |

The CONAB missing-Mato-Grosso case in `tests/test_fetcher_conab.py` is
derived at test time by deleting the real `MT` rows from a copy of that
file — real layout, one state removed. Nothing about the file is
hand-authored.

The `PrecosSemanalUF` fixture is likewise the *unmodified* live payload.
The download-gate failure cases in `tests/test_scrapers.py` are derived
from it in-memory (truncation, header drop, delimiter swap, kg→tonne
requote) — nothing is hand-authored, so a real upstream change shows up
as a fixture/parser divergence rather than as a test that was written to
match the regex.

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
