"""
Layer 15b — CONAB weekly producer (farmgate) soybean prices.

CONAB's Portal de Informações publishes ``PrecosSemanalUF.txt``: weekly
commercialization prices by product, state, and level (~100k rows,
semicolon-separated, latin-1, comma decimal, R$/kg). We extract the
Paraná farmgate soybean series — "PREÇO RECEBIDO" (producer price
received) — as a cross-check for the CEPEA/ESALQ Paraná wholesale
indicator (Layer 17).

The two series are deliberately kept apart: farmgate sits ~10-14% below
wholesale, and that spread is the sanity signal. The CONAB series gets
its own commodity key in ``brazil_spot_prices`` and is never spliced
into the CEPEA series.

Terms: CONAB authorizes reproduction with source citation. ~15 MB
download per run; the file carries a rolling ~18 months, so the layer
self-heals within that window (deeper history persists via the
``data/history/`` CSV round-trip like the rest of ``brazil_spot_prices``).

Parser strategy: hard-fail with ScraperShapeError when expected columns
vanish or the SOJA/PR farmgate filter matches nothing — Paraná soybeans
are always quoted, so an empty filter means the file's vocabulary
changed, not a quiet week.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Sequence
from datetime import datetime

import pandas as pd
import requests

from config import (
    CONAB_FARMGATE_LEVEL_PREFIX,
    CONAB_FARMGATE_PRODUCT,
    CONAB_FARMGATE_SERIES,
    CONAB_FARMGATE_UF,
    CONAB_PRECOS_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

_KG_TO_MT = 1000.0  # R$/kg → BRL/MT

# Download sanity gate. Live file 2026-08-11: 13,608,731 bytes, 89,699 data
# rows, median valor_produto_kg 5.53 R$/kg (p25 1.83, p75 27.99 across all
# products). Floors sit an order of magnitude below the live values so a
# genuine shrink of the rolling window never trips them.
_MIN_DOWNLOAD_BYTES = 1_000_000
_MIN_DATA_ROWS = 10_000
_SAMPLE_ROWS = 500
_MIN_NUMERIC_SHARE = 0.9  # of the *quoted* (non-blank) sampled prices
_MIN_QUOTED_SHARE = 0.5  # live file: ~93% of rows carry a price
_PLAUSIBLE_MEDIAN_PRICE_KG = (0.1, 1_000.0)

_REQUIRED_COLUMNS = (
    "produto",
    "uf",
    "data_inicial_final_semana",
    "dsc_nivel_comercializacao",
    "valor_produto_kg",
)


def _download() -> str:
    """Download the weekly-price file. Returns '' after exhausted retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(CONAB_PRECOS_URL, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200 and resp.content:
                return resp.content.decode("latin-1")
            logger.warning("CONAB prices: HTTP %d (attempt %d)", resp.status_code, attempt)
        except requests.RequestException as exc:
            logger.warning("CONAB prices attempt %d failed: %s", attempt, exc)
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    return ""


def _validate_download(text: str) -> None:
    """Sanity-gate the raw download before it reaches the parser.

    Three independent checks, each a hard-fail (ScraperShapeError):

    * **size** — the live file is ~13.6 MB / ~90k data rows carrying a
      rolling ~18 months. A truncated or padded body (proxy error page,
      cut connection answered 200 OK) is rejected on bytes *and* on data
      rows, so neither a big-but-empty nor a small-but-dense payload slips
      through.
    * **shape** — the header line must carry every required column, and
      the sampled data rows must have the same field count as the header.
      Catches a dropped header (the first data row would otherwise become
      the column names) and a delimiter switch.
    * **magnitude** — the sampled ``valor_produto_kg`` values must be
      mostly numeric and their median must sit inside a plausible R$/kg
      band. A silent kg→tonne requote keeps every column name and row
      count intact; only the band catches it (observed: the whole file
      x1000 parses clean and ships 2,070,000 BRL/MT).

    The sample is strided across the whole file, not taken off the head:
    the file is sorted by product name, so a head sample only ever sees
    the NPK fertilisers and both the field-count and the magnitude check
    would then depend on CONAB's row ordering. Blank prices are a normal
    feature of the file (~7% of rows), so they are excluded from the
    numeric share rather than counted as format breakage.
    """
    size = len(text.encode("latin-1", errors="replace"))
    if size < _MIN_DOWNLOAD_BYTES:
        raise ScraperShapeError(
            f"CONAB prices: undersized download — {size} bytes "
            f"(< {_MIN_DOWNLOAD_BYTES} minimum)"
        )

    lines = [line for line in text.splitlines() if line.strip()]
    if len(lines) - 1 < _MIN_DATA_ROWS:
        raise ScraperShapeError(
            f"CONAB prices: undersized download — {max(len(lines) - 1, 0)} data rows "
            f"(< {_MIN_DATA_ROWS} minimum)"
        )

    header = [c.strip() for c in lines[0].split(";")]
    missing = [c for c in _REQUIRED_COLUMNS if c not in header]
    if missing:
        raise ScraperShapeError(
            f"CONAB prices: download header missing {missing} — got {header}"
        )

    rows = lines[1:]
    stride = max(1, len(rows) // _SAMPLE_ROWS)
    sample = rows[::stride][:_SAMPLE_ROWS]
    ragged = sum(1 for line in sample if len(line.split(";")) != len(header))
    if ragged:
        raise ScraperShapeError(
            f"CONAB prices: download header/row field-count mismatch — "
            f"{ragged}/{len(sample)} sampled rows are not {len(header)} fields wide"
        )

    value_idx = header.index("valor_produto_kg")
    values: list[float] = []
    quoted = 0
    for line in sample:
        raw = line.split(";")[value_idx].strip()
        if not raw:  # blank = price not collected that week, normal
            continue
        quoted += 1
        try:
            values.append(float(raw.replace(",", ".")))
        except ValueError:
            continue
    if not values or len(values) < _MIN_NUMERIC_SHARE * quoted:
        raise ScraperShapeError(
            f"CONAB prices: implausible download — only {len(values)}/{quoted} "
            "quoted valor_produto_kg values are numeric (decimal format changed?)"
        )
    if quoted < _MIN_QUOTED_SHARE * len(sample):
        raise ScraperShapeError(
            f"CONAB prices: implausible download — only {quoted}/{len(sample)} "
            "sampled rows carry a price at all (empty price column?)"
        )

    median = sorted(values)[len(values) // 2]
    low, high = _PLAUSIBLE_MEDIAN_PRICE_KG
    if not low <= median <= high:
        raise ScraperShapeError(
            f"CONAB prices: implausible download — median sampled price {median} "
            f"outside the {low}-{high} R$/kg band (unit change?)"
        )


def _week_end_date(week_range: str) -> str | None:
    """Parse '21-07-2025 - 25-07-2025' → ISO date of the week's last day."""
    parts = week_range.strip().split(" - ")
    if len(parts) != 2:
        return None
    try:
        return datetime.strptime(parts[1].strip(), "%d-%m-%Y").date().isoformat()
    except ValueError:
        return None


def _parse_farmgate(text: str) -> pd.DataFrame:
    """Extract the PR soybean farmgate series from the raw file text.

    Returns the ``clean_brazil_spot``/``save_brazil_spot`` shape:
    ``Date`` (ISO week-end), ``price_brl_mt``, ``Unit``. Raises
    ScraperShapeError on structural mismatch.
    """
    df = pd.read_csv(io.StringIO(text), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ScraperShapeError(
            f"CONAB prices: columns {missing} missing from {sorted(df.columns)}"
        )

    for col in ("produto", "uf", "dsc_nivel_comercializacao"):
        df[col] = df[col].str.strip()

    subset = df[
        (df["produto"] == CONAB_FARMGATE_PRODUCT)
        & (df["uf"] == CONAB_FARMGATE_UF)
        & df["dsc_nivel_comercializacao"].str.startswith(CONAB_FARMGATE_LEVEL_PREFIX)
    ]
    if subset.empty:
        raise ScraperShapeError(
            f"CONAB prices: no rows for {CONAB_FARMGATE_PRODUCT}/{CONAB_FARMGATE_UF}/"
            f"{CONAB_FARMGATE_LEVEL_PREFIX}* — product or level vocabulary changed"
        )

    rows: list[dict[str, object]] = []
    for _, rec in subset.iterrows():
        week_end = _week_end_date(str(rec["data_inicial_final_semana"]))
        price_kg = pd.to_numeric(
            str(rec["valor_produto_kg"]).replace(",", "."), errors="coerce"
        )
        if week_end is None or pd.isna(price_kg) or price_kg <= 0:
            continue
        rows.append({
            "Date": week_end,
            "price_brl_mt": round(float(price_kg) * _KG_TO_MT, 2),
            "Unit": "BRL/MT",
        })

    if not rows:
        raise ScraperShapeError(
            f"CONAB prices: {len(subset)} matching rows but none with a parseable "
            "week range/price — date or decimal format changed"
        )

    out = pd.DataFrame(rows).drop_duplicates(subset="Date").sort_values("Date")
    return out.reset_index(drop=True)


def fetch_conab_farmgate() -> FetchResult:
    """Fetch CONAB's weekly Paraná soybean farmgate price series."""
    logger.info("Fetching CONAB weekly farmgate prices ...")
    text = _download()
    if not text:
        return FetchResult.failed("CONAB prices: download failed")

    try:
        _validate_download(text)
        df = _parse_farmgate(text)
    except ScraperShapeError as exc:
        logger.error("CONAB prices: %s", exc)
        return FetchResult.failed(str(exc))

    logger.info(
        "CONAB farmgate: %d weekly rows, latest R$%.2f/MT (week ending %s)",
        len(df), df["price_brl_mt"].iloc[-1], df["Date"].iloc[-1],
    )
    return FetchResult.ok({CONAB_FARMGATE_SERIES: df})


__all__: Sequence[str] = (
    "_parse_farmgate",
    "_validate_download",
    "_week_end_date",
    "fetch_conab_farmgate",
)


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_conab_farmgate()
    if not result.has_rows:
        logger.info("CONAB farmgate: %s — %s", result.status, result.error)
    else:
        for name, frame in result.data.items():
            logger.info("%s: %d rows\n%s", name, len(frame), frame.tail(5).to_string(index=False))
