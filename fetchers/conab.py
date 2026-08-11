"""
Layer 15 — CONAB (Companhia Nacional de Abastecimento) Brazil crop estimates.

Brazil's official crop agency publishes monthly production, area, and yield
estimates that often differ from USDA by millions of tonnes. Getting both
gives you the range of uncertainty.

Source: CONAB data portal — historical series download (SerieHistoricaGraos.txt).
No API key required.

File format (verified live 2026-05):
    ano_agricola;dsc_safra_previsao;uf;produto;id_produto;
    area_plantada_mil_ha;producao_mil_t;produtividade_mil_ha_mil_t

The file is semicolon-separated and per-state (UF column), so we aggregate
across the 27 UFs to produce national totals comparable to USDA/PSD figures.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Mapping, Sequence
from datetime import timezone
from email.utils import parsedate_to_datetime

import pandas as pd
import requests

from config import CONAB_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep
from pipeline.results import ScraperShapeError

logger = logging.getLogger(__name__)

_REQUIRED_COLUMNS: tuple[str, ...] = (
    "ano_agricola",
    "uf",
    "produto",
    "area_plantada_mil_ha",
    "producao_mil_t",
)

# Portuguese commodity (normalized: lowercase, stripped) → English label
# SerieHistoricaGraos.txt is grains+oilseeds+cotton only; coffee is in a
# separate CONAB file and is not tracked here.
_COMMODITY_MAP: dict[str, str] = {
    "soja": "Soybeans",
    "milho": "Corn",
    "trigo": "Wheat",
    "algodao em pluma": "Cotton",
}


# Core producing states that must be present (with output) in the newest
# crop year of every tracked commodity. Mato Grosso alone is ~29% of the
# national soybean crop and ~40% of corn — dropping it silently understates
# the national total by tens of millions of tonnes, which is exactly the
# CONAB-vs-USDA divergence the briefing reads. Verified against the live
# file (2026-08-11): every listed UF reports positive production in each of
# the last three crop years of its commodity.
_REQUIRED_UFS: dict[str, frozenset[str]] = {
    "soja": frozenset({"MT", "PR", "RS", "GO", "MS"}),
    "milho": frozenset({"MT", "PR", "GO", "MS"}),
    "trigo": frozenset({"PR", "RS"}),
    "algodao em pluma": frozenset({"MT", "BA"}),
}


def _report_date_from_headers(headers: Mapping[str, str]) -> str:
    """Publication date of the survey file, taken from ``Last-Modified``.

    The historical series is a monthly survey, not a daily stream. Stamping
    it with ``now()`` writes one identical-valued row per pipeline run and
    turns the revision history into phantom revisions. There is no date
    column inside the file, so the response's ``Last-Modified`` is the only
    real publication date available — no header, no row.
    """
    raw = headers.get("Last-Modified") or headers.get("last-modified")
    if not raw:
        raise ScraperShapeError(
            "CONAB response carries no Last-Modified header — refusing to "
            "stamp the monthly survey with today's date."
        )
    try:
        published = parsedate_to_datetime(raw)
    except (TypeError, ValueError) as exc:
        raise ScraperShapeError(
            f"CONAB Last-Modified header unparseable: {raw!r}"
        ) from exc
    if published.tzinfo is None:
        published = published.replace(tzinfo=timezone.utc)
    return published.astimezone(timezone.utc).strftime("%Y-%m-%d")


def _parse_csv(text: str) -> pd.DataFrame:
    """Parse the CONAB semicolon-separated text into a DataFrame."""
    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        encoding="utf-8",
        on_bad_lines="skip",
        dtype=str,
    )


def _assert_states_present(df: pd.DataFrame) -> None:
    """Abort if a core producing state is missing from the newest crop year.

    ``df`` is the tracked-commodity subset, already stripped and numeric.
    Only the newest crop year per commodity is checked — that is the year
    the briefing compares against USDA, and it is also the tail of the file
    a truncated download would lose first. Older years are left alone
    because state coverage genuinely varies back through the 1970s.
    """
    for produto_norm, required in _REQUIRED_UFS.items():
        rows = df[df["produto_norm"] == produto_norm]
        if rows.empty:
            continue
        latest = max(rows["ano_agricola"])
        year_rows = rows[rows["ano_agricola"] == latest]
        present = set(
            year_rows.loc[year_rows["producao_mil_t"] > 0, "uf"].astype(str)
        )
        missing = sorted(required - present)
        if missing:
            raise ScraperShapeError(
                f"CONAB {produto_norm!r} {latest}: core producing state(s) "
                f"{missing} missing or zero — refusing to publish a national "
                f"total that silently omits them."
            )


def _aggregate_national(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate per-state rows to national totals.

    Sums area and production across UFs for each (crop_year, commodity).
    Yield is recomputed from the aggregated totals (kg/ha) rather than
    averaged across states.
    """
    df = df.copy()

    # Strip whitespace from string columns (CONAB pads with trailing spaces)
    for col in ("ano_agricola", "uf", "produto"):
        df[col] = df[col].astype(str).str.strip()

    # Normalize produto for matching
    df["produto_norm"] = df["produto"].str.lower()
    df = df[df["produto_norm"].isin(_COMMODITY_MAP)]

    if df.empty:
        return df

    # Numeric conversion — CONAB uses '.' decimal in the new format.
    # A cell we cannot read is NOT a zero: coercing it to 0.0 subtracts a
    # whole state from the national sum without a trace.
    for col in ("area_plantada_mil_ha", "producao_mil_t"):
        numeric = pd.to_numeric(df[col], errors="coerce")
        bad = numeric.isna()
        if bad.any():
            sample = df.loc[bad, ["ano_agricola", "uf", "produto", col]].head(5)
            raise ScraperShapeError(
                f"CONAB column {col!r} has {int(bad.sum())} non-numeric "
                f"cell(s) — refusing to zero them into the national sum. "
                f"Sample:\n{sample.to_string(index=False)}"
            )
        df[col] = numeric

    _assert_states_present(df)

    grouped = (
        df.groupby(["ano_agricola", "produto_norm"], as_index=False)
        .agg(
            area=("area_plantada_mil_ha", "sum"),
            production=("producao_mil_t", "sum"),
        )
    )

    # yield in kg/ha = (production[1000 t] / area[1000 ha]) * 1000
    grouped["yield_kg_ha"] = (
        grouped["production"].div(grouped["area"].replace(0, pd.NA)) * 1000.0
    )

    # Drop years where production is zero (commodity not grown that year)
    grouped = grouped[grouped["production"] > 0].reset_index(drop=True)
    return grouped


def _melt_to_long(grouped: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """Reshape aggregated DataFrame into the long format the pipeline expects."""
    rows: list[dict[str, object]] = []
    for _, row in grouped.iterrows():
        commodity_en = _COMMODITY_MAP[row["produto_norm"]]
        crop_year = row["ano_agricola"]

        for attr, value, unit in (
            ("Production", row["production"], "1000 MT"),
            ("Area", row["area"], "1000 HA"),
            ("Yield", row["yield_kg_ha"], "KG/HA"),
        ):
            if pd.isna(value):
                continue
            rows.append({
                "source": "CONAB",
                "commodity": commodity_en,
                "crop_year": crop_year,
                "attribute": attr,
                "value": float(value),
                "unit": unit,
                "report_date": report_date,
            })

    return pd.DataFrame(rows)


def fetch_conab_estimates() -> pd.DataFrame:
    """Fetch CONAB historical series data for Brazilian crop estimates.

    Returns a DataFrame with columns:
        source, commodity, crop_year, attribute, value, unit, report_date

    ``report_date`` is the file's publication date (HTTP ``Last-Modified``),
    not the run date — the series is a monthly survey.

    Empty DataFrame if the download fails or the file schema is unrecognized.

    Raises:
        ScraperShapeError: the payload parsed but is not trustworthy — a
            core producing state missing from the newest crop year, a
            non-numeric area/production cell, or no publication date on the
            response. Layer 15 in ``main.py`` records the layer as failed
            rather than storing a poisoned national total.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logger.info("Fetching CONAB historical series (attempt %d) ...", attempt)
            resp = requests.get(CONAB_URL, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                logger.warning("HTTP %d for CONAB", resp.status_code)
                if attempt < MAX_RETRIES:
                    retry_sleep(attempt)
                    continue
                return pd.DataFrame()

            try:
                df = _parse_csv(resp.text)
            except (pd.errors.ParserError, ValueError, UnicodeDecodeError) as exc:
                logger.warning("Could not parse CONAB data: %s", exc)
                return pd.DataFrame()

            if df.empty:
                logger.info("CONAB data parsed but empty.")
                return pd.DataFrame()

            # Normalize column names
            df.columns = [c.strip().lower() for c in df.columns]

            missing = [c for c in _REQUIRED_COLUMNS if c not in df.columns]
            if missing:
                logger.error(
                    "CONAB schema unrecognized — missing columns %s. Got: %s",
                    missing, list(df.columns),
                )
                return pd.DataFrame()

            grouped = _aggregate_national(df)
            if grouped.empty:
                logger.info(
                    "CONAB data parsed but no target commodities found "
                    "(looked for %s).", sorted(_COMMODITY_MAP),
                )
                return pd.DataFrame()

            report_date = _report_date_from_headers(resp.headers)
            result = _melt_to_long(grouped, report_date=report_date)
            logger.info(
                "Parsed %d CONAB estimate rows across %d (year, commodity) "
                "pairs, published %s.",
                len(result), len(grouped), report_date,
            )
            return result

        except requests.RequestException as exc:
            logger.warning("CONAB attempt %d failed: %s", attempt, exc)
            if attempt < MAX_RETRIES:
                retry_sleep(attempt)

    logger.error("All %d attempts failed for CONAB", MAX_RETRIES)
    return pd.DataFrame()


__all__: Sequence[str] = ("fetch_conab_estimates",)


# ── Quick self-test ─────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    data = fetch_conab_estimates()
    if data.empty:
        logger.info("CONAB: no data returned")
    else:
        logger.info("CONAB: %d rows", len(data))
        latest_year = data["crop_year"].max()
        logger.info("Latest crop year: %s", latest_year)
        latest = data[data["crop_year"] == latest_year]
        logger.info("\n%s", latest.to_string(index=False))
