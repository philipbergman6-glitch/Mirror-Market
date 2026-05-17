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
from collections.abc import Sequence
from datetime import datetime, timezone

import pandas as pd
import requests

from config import CONAB_URL, MAX_RETRIES, REQUEST_TIMEOUT
from fetchers._backoff import retry_sleep

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


def _parse_csv(text: str) -> pd.DataFrame:
    """Parse the CONAB semicolon-separated text into a DataFrame."""
    return pd.read_csv(
        io.StringIO(text),
        sep=";",
        encoding="utf-8",
        on_bad_lines="skip",
        dtype=str,
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

    # Numeric conversion — CONAB uses '.' decimal in the new format
    for col in ("area_plantada_mil_ha", "producao_mil_t"):
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

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

    Empty DataFrame if the download fails or the file schema is unrecognized.
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

            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            result = _melt_to_long(grouped, report_date=today)
            logger.info(
                "Parsed %d CONAB estimate rows across %d (year, commodity) pairs.",
                len(result), len(grouped),
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
