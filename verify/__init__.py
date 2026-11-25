"""The verifier: independent checks of the numbers the site displays (AL1 #640, spec #639).

It sits **outside the product**. It reads each basket number as the reader saw it
on a built edition's rendered page, fetches the publisher of record (or a second
provider, or recomputes a derived number), converts with its own unit constants,
grades the difference against its own versioned tolerance file, and writes one
verification record per checked number to ``verification/records/``.

The independence boundary, enforced by ``tests/test_verify_boundary.py``:

* this package imports nothing from the repo — not ``config``, ``fetchers``,
  ``pipeline``, ``pricing``, ``trust``, ``analysis``, ``app``, ``main`` or
  ``mirror_mcp`` — only the standard library and third-party HTTP, parsing,
  PDF and browser libraries;
* it imports no LLM SDK. An agent may *run* it; no model decides a result.

What it shares with the product is one seam, the product's ``data-verify-*``
locators on rendered numbers (``config.VERIFY_LOCATED_BLOCKS``). They find an
element and nothing more: every value, unit and date is read from visible text.

Owner decision 2026-11-25 (#479): if this package ever imports product code,
its independence is nominal, and its tolerances move to ``config.py``.

Run it::

    python -m verify fx-cny --edition docs            # live ECB document
    python -m verify fx-cny --edition docs --ecb-xml saved.xml

Exit status: 0 when every displayed number was graded (a discrepancy is a
graded result, never a crash); 1 when any record is ``not checked`` — the
record is still written, with its reason; 2 on a usage or setup error, before
any record exists.
"""
