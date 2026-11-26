"""What a committed record may carry, by the verifying publisher's terms (spec #639, owner #478 Q2).

Committed records are a publication surface (spec reading from A18 #468): the
repo is public, so a value written to ``verification/records/`` is published.
Withholding therefore happens when the record is written, never at render
time. A withheld record keeps its deltas, its result and a ``withheld_reason``.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PublisherTerms:
    show_source_value: bool
    attribution: str | None
    basis: str


PUBLISHER_TERMS: dict[str, PublisherTerms] = {
    "cme_bulletin": PublisherTerms(False, None, "CME terms: redistribution of settlements is licensed (A5 #302)"),
    "dce": PublisherTerms(False, None, "DCE terms not established"),
    # CC BY-NC 4.0: shown only while the project is non-commercial; flips to
    # False at sale (A5 trigger).
    "cepea": PublisherTerms(True, "CEPEA (CC BY-NC 4.0)", "CC BY-NC 4.0, non-commercial use only"),
    # Government publication, but the spec leaves its terms to be confirmed by
    # the stratum ticket (AL6 #645) before any value is shown.
    "magyp": PublisherTerms(False, None, "MAGyP terms not yet confirmed (AL6 #645 confirms before showing)"),
    "usda_ams": PublisherTerms(True, "USDA AMS", "US federal government publication"),
    "ecb": PublisherTerms(True, "Source: ECB · derived cross", "ECB statistics: free reuse with the source cited"),
    "bcb_ptax": PublisherTerms(True, "Banco Central do Brasil (PTAX)", "reuse with attribution"),
    "cfets": PublisherTerms(False, None, "no publication without a licence"),
    "sarb": PublisherTerms(False, None, "no publication without a licence"),
    "fbil": PublisherTerms(False, None, "no publication without a licence"),
    "grain_sa_jse": PublisherTerms(False, None, "JSE terms bar redistribution; JSE MTM is licence-blocked (#157)"),
}

# Licence-restricted *displayed* series (spec #639): CBOT, DCE, CEPEA, AgRural,
# SAFEX. Their displayed value is never repeated in a record; the ledger row
# links to the block where it already renders.
RESTRICTED_DISPLAYED_STRATA = frozenset({"1", "1b", "2", "3", "3u", "10"})

WITHHELD_CAPTION = (
    "Value withheld — the verifying publisher's terms do not allow republication. "
    "Pass/fail and the USD/MT difference are shown."
)
DISPLAYED_WITHHELD = "displayed value withheld: licence-restricted displayed series; the ledger links to its block"


def terms(publisher: str) -> PublisherTerms:
    if publisher not in PUBLISHER_TERMS:
        raise KeyError(f"no licence terms recorded for verifying publisher {publisher!r}: refusing to guess")
    return PUBLISHER_TERMS[publisher]
