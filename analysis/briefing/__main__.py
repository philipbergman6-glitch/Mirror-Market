"""Run the daily briefing standalone: python -m analysis.briefing"""

from analysis.briefing.orchestrator import generate_briefing

if __name__ == "__main__":
    print(generate_briefing())
