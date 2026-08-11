"""
Layer 21 — Argentina official FOB prices via the MAGyP web service.

The Secretaría de Agricultura (SAGyP/MAGyP) publishes official minimum
FOB export values ("Precios FOB Oficiales") every business day as a
free JSON web service — the up-river/Argentina leg of the cross-origin
FOB board (Brazil Paranaguá = Layer 19, US Gulf CIF = Layer 20).

Why it matters:
    Argentina is the world's #1 soymeal/oil exporter and #3 bean
    exporter. The official FOB is the reference price the export tax
    (derechos de exportación) is levied on, so it tracks the up-river
    (Rosario/San Lorenzo) export market a physical buyer would compare
    against Paranaguá FOB and Gulf CIF.

Series construction:
    One request per date (``?Fecha=dd/mm/yyyy``, business days only —
    the fetcher walks back up to ``MAGYP_FOB_LOOKBACK_DAYS`` calendar
    days to find the latest circular). Each response carries one row
    per NCM tariff position per shipment window; only the bulk
    ("granel") soy-complex positions in ``MAGYP_FOB_POSITIONS`` are
    stored, in USD/MT as published (no unit conversion needed).

Parser strategy:
    A response missing the ``posts`` key, or a published day where none
    of the mapped positions appear, raises ScraperShapeError — the
    nomenclature or schema changed and silence would be worse than a
    crash. An empty ``posts`` list is a normal holiday/weekend outcome
    and just walks back one more day.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from config import (
    MAGYP_FOB_LOOKBACK_DAYS,
    MAGYP_FOB_POSITIONS,
    MAGYP_FOB_URL,
    MAX_RETRIES,
    REQUEST_TIMEOUT,
)
from fetchers._backoff import retry_sleep
from pipeline.results import FetchResult, ScraperShapeError

logger = logging.getLogger(__name__)

MAGYP_SERIES = "Argentina FOB"


def _fetch_day(day: date) -> list[dict]:
    """Fetch one date's circular. Raises on exhausted retries or bad shape."""
    params = {"Fecha": day.strftime("%d/%m/%Y")}
    last_error = "no attempts made"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(MAGYP_FOB_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                payload = resp.json()
                if "posts" in payload:
                    return payload["posts"]
                if not payload:
                    # Unpublished date (holiday, or today's circular not out
                    # yet) responds with an empty JSON *array* — observed live
                    # 2026-08-11, body b"[]". This branch catches it only by
                    # accident: `"posts" in []` is a membership test that is
                    # False, and `not []` is True. Left as-is because it is
                    # correct in effect, but do not read the old "empty
                    # object" claim as the upstream contract.
                    return []
                raise ScraperShapeError(
                    "MAGyP FOB: response has keys but no 'posts' — schema "
                    f"changed (keys: {sorted(payload)[:10]})"
                )
            last_error = f"HTTP {resp.status_code}"
            logger.warning(
                "MAGyP FOB: %s for %s (attempt %d)", last_error, day, attempt
            )
        except ScraperShapeError:
            raise
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
            logger.warning(
                "MAGyP FOB attempt %d failed for %s: %s", attempt, day, exc
            )
        if attempt < MAX_RETRIES:
            retry_sleep(attempt)
    raise requests.RequestException(
        f"MAGyP FOB: {day} failed after {MAX_RETRIES} attempts ({last_error})"
    )


def _parse_posts(posts: list[dict]) -> pd.DataFrame:
    """Distill one circular's posts into the mapped soy-complex rows.

    Returns columns: date (ISO), product, position, ship_from, ship_to
    (both ``YYYY-MM``), price_usd_mt.
    """
    rows: list[dict[str, object]] = []
    for post in posts:
        position = str(post.get("posicion", ""))
        product = MAGYP_FOB_POSITIONS.get(position)
        if product is None:
            continue
        try:
            day = datetime.strptime(
                str(post["fecha"])[:10], "%Y-%m-%d"
            ).date().isoformat()
            price = float(post["precio"])
            ship_from = f"{int(post['añoDesde']):04d}-{int(post['mesDesde']):02d}"
            ship_to = f"{int(post['añoHasta']):04d}-{int(post['mesHasta']):02d}"
        except (KeyError, TypeError, ValueError) as exc:
            raise ScraperShapeError(
                f"MAGyP FOB: unparseable post for position {position}: {post!r}"
            ) from exc
        if price <= 0:
            continue
        rows.append({
            "date": day,
            "product": product,
            "position": position,
            "ship_from": ship_from,
            "ship_to": ship_to,
            "price_usd_mt": price,
        })

    if posts and not rows:
        raise ScraperShapeError(
            f"MAGyP FOB: {len(posts)} posts published but none matched the "
            "configured soy positions — NCM nomenclature changed"
        )
    return pd.DataFrame(rows)


def fetch_magyp_fob() -> FetchResult:
    """Fetch the latest published Argentina official FOB circular.

    Walks back from today across weekends/holidays until a published
    circular is found (``empty`` if none within the lookback window);
    transport exhaustion or a schema change is ``failed``.
    """
    logger.info("Fetching Argentina official FOB prices from MAGyP ...")
    today = date.today()
    try:
        for offset in range(MAGYP_FOB_LOOKBACK_DAYS + 1):
            day = today - timedelta(days=offset)
            if day.weekday() >= 5:  # Sat/Sun never publish
                continue
            posts = _fetch_day(day)
            if not posts:
                logger.info("MAGyP FOB: no circular for %s (holiday?)", day)
                continue
            df = _parse_posts(posts)
            if df.empty:
                continue
            logger.info(
                "MAGyP FOB: circular %s — %d soy rows, bean spot $%.0f/MT",
                day, len(df),
                df[df["product"] == "Soybeans"]["price_usd_mt"].iloc[0]
                if not df[df["product"] == "Soybeans"].empty else float("nan"),
            )
            return FetchResult.ok({MAGYP_SERIES: df})
    except ScraperShapeError as exc:
        logger.error("MAGyP FOB: %s", exc)
        return FetchResult.failed(str(exc))
    except requests.RequestException as exc:
        return FetchResult.failed(str(exc))

    return FetchResult.empty(
        f"no MAGyP FOB circular published in the last {MAGYP_FOB_LOOKBACK_DAYS} days"
    )


__all__: Sequence[str] = ("_parse_posts", "fetch_magyp_fob", "MAGYP_SERIES")


# ── Quick self-test ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from config import setup_logging
    setup_logging()

    result = fetch_magyp_fob()
    if not result.has_rows:
        logger.info("MAGyP FOB: %s — %s", result.status, result.error)
    else:
        for name, frame in result.data.items():
            logger.info("%s:\n%s", name, frame.to_string(index=False))
