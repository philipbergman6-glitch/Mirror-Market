"""Read displayed numbers from a built edition's rendered HTML.

The product marks each basket number with ``data-verify-*`` attributes. They
are used here to *find* an element and for nothing else: value, unit and date
come from the element's visible text, because a number the product asserted in
an attribute, checked against itself, would prove nothing (spec #639).

Anything this module cannot read unambiguously raises ``PageReadError``; the
caller turns that into a ``not checked`` record with the reason.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

from bs4 import BeautifulSoup, Tag

EDITION_META = "mirror-market-generated-at"

_NUMBER = re.compile(r"^-?\d{1,3}(?:,\d{3})*(?:\.(\d+))?$|^-?\d+(?:\.(\d+))?$")
_PER = re.compile(r"\b([A-Z]{3}) per ([A-Z]{3})\b")
_AS_OF = re.compile(r"\bas of (\d{4}-\d{2}-\d{2})\b")
_HOME_PRINT = re.compile(r"(-?\d[\d,]*(?:\.\d+)?) ([A-Z]{3})/MT\b")


class PageReadError(ValueError):
    """The page does not show what the check needs, unambiguously."""


@dataclass(frozen=True)
class Figure:
    """One displayed number, as printed."""

    text: str        # the digits exactly as shown, e.g. "0.1500"
    value: float
    decimals: int    # places shown: the display rounding is half a unit there
    unit: str        # the visible "X per Y" phrase, e.g. "USD per CNY"
    numerator: str   # X
    denominator: str  # Y


@dataclass(frozen=True)
class Leg:
    """A price leg on the same page, read from its visible home-currency print."""

    label: str
    home_value: float
    home_unit: str   # e.g. "CNY/MT"
    text: str


@dataclass(frozen=True)
class FxBlockReading:
    site_edition: str
    stratum: str
    series: str
    observation_date: str
    figures: tuple[Figure, ...]
    legs: tuple[Leg, ...]


def visible_text(element: Tag) -> str:
    """The element's text as a reader sees it: no script, style or hidden content."""
    clone = BeautifulSoup(str(element), "html.parser")
    for node in clone.find_all(["script", "style", "template"]):
        node.decompose()
    for node in clone.select("[hidden]"):
        node.decompose()
    return " ".join(clone.get_text(" ").split())


def parse_number(text: str) -> tuple[float, int]:
    """A displayed number and the places it shows. Raises on anything else."""
    cleaned = text.strip()
    match = _NUMBER.match(cleaned)
    if match is None:
        raise PageReadError(f"{cleaned!r} is not a displayed number")
    places = match.group(1) or match.group(2) or ""
    return float(cleaned.replace(",", "")), len(places)


def site_edition(soup: BeautifulSoup) -> str:
    meta = soup.find("meta", attrs={"name": EDITION_META})
    content = meta.get("content") if isinstance(meta, Tag) else None
    if not isinstance(content, str) or not content.strip():
        raise PageReadError(f"the page carries no {EDITION_META} stamp, so the edition it shows is unknown")
    return content.strip()


def read_fx_block(html: str, *, series: str) -> FxBlockReading:
    """The FX rate cards for ``series`` (e.g. "CNY/USD"), their date, and the legs on the page."""
    soup = BeautifulSoup(html, "html.parser")
    edition = site_edition(soup)

    cards = soup.select(f'[data-verify-figure="fx-rate"][data-verify-series="{series}"]')
    if not cards:
        raise PageReadError(f"no located {series} rate on the page (data-verify-figure=\"fx-rate\")")
    parents = [card.find_parent(attrs={"data-verify-stratum": True}) for card in cards]
    section = parents[0]
    if section is None or any(parent is None for parent in parents):
        raise PageReadError(f"a {series} rate card sits in no block carrying data-verify-stratum")
    if any(parent is not section for parent in parents):
        raise PageReadError(f"the {series} rate cards sit in more than one located block, expected one")
    stratum = str(section["data-verify-stratum"])

    dates = sorted(set(_AS_OF.findall(visible_text(section))))
    if len(dates) != 1:
        raise PageReadError(f"the {series} block shows {len(dates)} 'as of' dates {dates}, expected exactly one")

    figures = tuple(_read_rate_card(card, series) for card in cards)
    units = [figure.unit for figure in figures]
    if len(set(units)) != len(units):
        raise PageReadError(f"the {series} block shows the same unit twice: {units}")

    legs = tuple(leg for card in soup.select("[data-verify-leg]") if (leg := _read_leg(card)) is not None)
    return FxBlockReading(
        site_edition=edition,
        stratum=stratum,
        series=series,
        observation_date=dates[0],
        figures=figures,
        legs=legs,
    )


def _read_rate_card(card: Tag, series: str) -> Figure:
    values = card.select(".mc-val")
    if len(values) != 1:
        raise PageReadError(f"a {series} rate card shows {len(values)} values, expected one")
    text = visible_text(values[0])
    value, decimals = parse_number(text)
    pers = _PER.findall(visible_text(card))
    if len(pers) != 1:
        raise PageReadError(f"a {series} rate card states {len(pers)} 'X per Y' units, expected one: {pers}")
    numerator, denominator = pers[0]
    return Figure(
        text=text,
        value=value,
        decimals=decimals,
        unit=f"{numerator} per {denominator}",
        numerator=numerator,
        denominator=denominator,
    )


def _read_leg(card: Tag) -> Leg | None:
    """A leg with a visible home print, or None for one quoted only in USD/MT."""
    labels = card.select(".mc-label")
    if len(labels) != 1:
        raise PageReadError(f"a located leg card shows {len(labels)} labels, expected one")
    prints = _HOME_PRINT.findall(visible_text(card))
    if not prints:
        return None
    if len(prints) != 1:
        raise PageReadError(f"leg {visible_text(labels[0])!r} shows {len(prints)} home prints, expected one")
    number, currency = prints[0]
    value, _ = parse_number(number)
    return Leg(
        label=visible_text(labels[0]),
        home_value=value,
        home_unit=f"{currency}/MT",
        text=f"{number} {currency}/MT",
    )
