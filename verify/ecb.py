"""ECB euro foreign exchange reference rates, read from the eurofxref XML.

The product's fetcher (Layer 7, B25 #495) reads the ECB's SDMX API. This reads
a different document of the same publication — ``eurofxref-hist-90d.xml`` —
with its own parser, so a defect in the fetcher's request, parse or cross
cannot reappear here by construction.

Reuse: ECB statistics may be reused free of charge with the source cited
("Source: ECB"); a cross we compute from two legs is marked "derived cross".
"""

from __future__ import annotations

import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import date

HIST_90D_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist-90d.xml"
IDENTITY = "ECB euro foreign exchange reference rates, eurofxref XML"
# The XML stamps a reference date per day, never a publication time.
VINTAGE_NOTE = "publication time unknown"

_NS = {"gesmes": "http://www.gesmes.org/xml/2002-08-01", "e": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
# A generic client name: the verifier sends nothing that identifies the project's people.
_USER_AGENT = "soy-verifier/1 (+python-urllib)"
_TIMEOUT_S = 30


class SourceUnavailable(RuntimeError):
    """The ECB document could not be fetched."""


class EcbDocumentError(ValueError):
    """The ECB answered, but not with a reference-rates document this parser can trust."""


@dataclass(frozen=True)
class ReferenceRates:
    """Units of each currency per EUR, by reference date."""

    by_date: dict[date, dict[str, float]]

    @property
    def first(self) -> date:
        return min(self.by_date)

    @property
    def last(self) -> date:
        return max(self.by_date)


def fetch(url: str = HIST_90D_URL) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:  # noqa: S310 — https or a file: copy
            body: bytes = response.read()
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise SourceUnavailable(f"ECB document {url} could not be fetched: {exc}") from exc
    return body


def parse(document: bytes) -> ReferenceRates:
    try:
        root = ET.fromstring(document)
    except ET.ParseError as exc:
        raise EcbDocumentError(f"ECB document is not XML: {exc}") from exc
    subject = root.find("gesmes:subject", _NS)
    if root.tag != f"{{{_NS['gesmes']}}}Envelope" or subject is None or subject.text != "Reference rates":
        raise EcbDocumentError("ECB document is not a gesmes 'Reference rates' envelope")
    outer = root.find("e:Cube", _NS)
    if outer is None:
        raise EcbDocumentError("ECB document carries no Cube")
    by_date: dict[date, dict[str, float]] = {}
    for day in outer.findall("e:Cube", _NS):
        stamp = day.get("time")
        if stamp is None:
            raise EcbDocumentError("an ECB day Cube carries no time")
        try:
            when = date.fromisoformat(stamp)
        except ValueError as exc:
            raise EcbDocumentError(f"ECB reference date {stamp!r} is not ISO") from exc
        if when in by_date:
            raise EcbDocumentError(f"ECB document carries {stamp} twice")
        rates: dict[str, float] = {}
        for cube in day.findall("e:Cube", _NS):
            currency, rate = cube.get("currency"), cube.get("rate")
            if currency is None or rate is None:
                raise EcbDocumentError(f"an ECB rate on {stamp} lacks currency or rate")
            if currency in rates:
                raise EcbDocumentError(f"ECB document carries {currency} twice on {stamp}")
            try:
                value = float(rate)
            except ValueError as exc:
                raise EcbDocumentError(f"ECB {currency} on {stamp} is {rate!r}, not a number") from exc
            if not value > 0:
                raise EcbDocumentError(f"ECB {currency} on {stamp} is {rate}: rates must be positive")
            rates[currency] = value
        by_date[when] = rates
    if not by_date:
        raise EcbDocumentError("ECB document carries no reference dates")
    return ReferenceRates(by_date=by_date)
