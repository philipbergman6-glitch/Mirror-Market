"""CROP CONDITIONS section — weekly USDA condition/progress ratings."""

from pipeline.query import read_crop_progress


def format() -> str:  # noqa: A001
    lines = ["CROP CONDITIONS (USDA Weekly):"]
    progress_data = read_crop_progress()

    if progress_data.empty:
        return "CROP CONDITIONS (USDA Weekly): No data"

    for commodity in progress_data["commodity"].unique():
        subset = progress_data[progress_data["commodity"] == commodity]
        if subset.empty:
            continue

        lines.append(f"  {commodity}:")

        condition = subset[subset["stat_category"] == "CONDITION"]
        if not condition.empty:
            latest_week = condition["week_ending"].max()
            latest = condition[condition["week_ending"] == latest_week]
            for _, row in latest.iterrows():
                desc = str(row.get("short_desc", ""))
                val = row.get("Value", "")
                if any(kw in desc.upper() for kw in ["GOOD", "EXCELLENT", "POOR"]):
                    lines.append(f"    {desc}: {val}%")

        progress = subset[subset["stat_category"] == "PROGRESS"]
        if not progress.empty:
            latest_week = progress["week_ending"].max()
            latest = progress[progress["week_ending"] == latest_week]
            for _, row in latest.iterrows():
                desc = str(row.get("short_desc", ""))
                val = row.get("Value", "")
                if val:
                    lines.append(f"    {desc}: {val}%")

    if len(lines) == 1:
        lines.append("  No crop condition data available")

    return "\n".join(lines)
