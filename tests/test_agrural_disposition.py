"""B19 #469 — the AgRural promise and the history export must agree.

The fetcher once said raw BRL/MT rows stayed local while the export put
them in the public repo. A18 #468 fixed the disposition at ``open``; this
test pins that code and data say the same thing, whichever way it flips.
"""

import config
from pipeline.history import HISTORY_TABLES


def test_disposition_is_a_known_state() -> None:
    assert config.AGRURAL_PUBLICATION_DISPOSITION in {"open", "derived", "withheld"}


def test_export_roster_matches_disposition() -> None:
    exported = "brazil_spot_prices" in HISTORY_TABLES
    if config.AGRURAL_PUBLICATION_DISPOSITION == "open":
        assert exported, "open disposition but raw rows are not exported"
    else:
        # A non-open flip must remove AgRural raw rows from the committed
        # CSV path; the table has three writers, so that fix is a filter,
        # not a roster edit — and this assertion is the reminder to write it.
        raise AssertionError(
            "AgRural disposition is not open: filter its rows out of the "
            "brazil_spot_prices history export before changing the flag"
        )


def test_fetcher_docstring_names_the_disposition_flag() -> None:
    import fetchers.agrural as agrural

    assert "AGRURAL_PUBLICATION_DISPOSITION" in (agrural.__doc__ or "")
    assert "locally (for our own analysis)" not in (agrural.__doc__ or "")
