"""Publisher-announced dates, checked 2026-10-08; schedules, never observations.

Renew from the linked publishers when exhausted. No extrapolation past the
published horizon; no claim that a scheduled report was actually released.
"""
from datetime import date

CHECKED_ON = date(2026, 10, 8)
WASDE_URL = (
    "https://www.usda.gov/about-usda/general-information/staff-offices/"
    "office-chief-economist/commodity-markets/wasde-report"
)
NOPA_URL = (
    "https://www.nopa.org/wp-content/uploads/2026/02/"
    "NOPA-ONLY-Crush-Reporting-Release-Dates-2026-FINAL.pdf"
)

PUBLISHED_DATES = {
    "wasde": tuple(
        date(year, month, day)
        for year, days in (
            (2026, (12, 10, 10, 9, 12, 11, 10, 12, 11, 9, 10, 10)),
            (2027, (12, 10, 10, 9, 12, 11, 9, 12, 10, 8, 10, 10)),
        )
        for month, day in enumerate(days, 1)
    ),
    "nopa": tuple(date(2026, month, day) for month, day in enumerate(
        (15, 17, 16, 15, 15, 15, 15, 17, 15, 15, 16, 15), 1
    )),
}
