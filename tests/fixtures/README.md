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
| `conab_precos_semanal_uf.txt.gz` | CONAB `PrecosSemanalUF.txt` — `https://portaldeinformacoes.conab.gov.br/downloads/arquivos/PrecosSemanalUF.txt` | live download 2026-08-11 (HTTP 200, 13,608,731 bytes, 89,700 lines; gzipped verbatim for the repo, latin-1) |

The CONAB fixture is the *unmodified* live payload. The download-gate
failure cases in `tests/test_scrapers.py` are derived from it in-memory
(truncation, header drop, delimiter swap, kg→tonne requote) — nothing is
hand-authored, so a real upstream change shows up as a fixture/parser
divergence rather than as a test that was written to match the regex.

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
