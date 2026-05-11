"""Daily market briefing package.

The package replaces the previous monolithic `analysis/briefing.py`. Each
section of the briefing now lives in its own module under
`analysis/briefing/sections/` and is independently testable. The
orchestrator loads shared data and stitches the sections together.
"""

from analysis.briefing.orchestrator import generate_briefing, generate_briefing_data
from analysis.briefing.types import BriefingData

__all__ = ["BriefingData", "generate_briefing", "generate_briefing_data"]
