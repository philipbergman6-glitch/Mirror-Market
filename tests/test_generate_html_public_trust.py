from __future__ import annotations

from datetime import date, datetime, timezone

from jinja2 import Environment, FileSystemLoader

from scripts import generate_html
from trust import CriticalNumberProvenance, EditionPublicTrustState, FreshnessState, QualityState


def test_dashboard_template_can_render_public_trust_metadata_without_raw_values() -> None:
    trust_state = EditionPublicTrustState(
        edition_id="edn_" + "1" * 64,
        generated_at=datetime(2026, 8, 10, 12, 33, tzinfo=timezone.utc),
        critical_freshness={
            "dst_" + "2" * 64: FreshnessState.CURRENT,
            "dst_" + "3" * 64: FreshnessState.STALE,
        },
        degraded_dataset_ids=("dst_" + "3" * 64,),
        critical_numbers=(
            CriticalNumberProvenance(
                label="agrural_paranagua.soybean.beans.fob",
                source_id="src_agrural",
                dataset_id="dst_" + "3" * 64,
                dataset_key="agrural_paranagua",
                as_of_date=date(2026, 8, 10),
                quality_state=QualityState.ACCEPTED,
                observation_id="obs_" + "4" * 64,
                revision_id="rev_" + "5" * 64,
            ),
        ),
    )
    public_trust = generate_html._build_public_trust_metadata(trust_state)
    template = Environment(
        loader=FileSystemLoader(str(generate_html.TEMPLATE_DIR)),
        autoescape=False,
    ).get_template("dashboard.html.j2")

    html = template.render(
        sections=[],
        generated_at="2026-08-10 12:33 UTC",
        masthead={
            "day_line": "Monday · 10 August 2026",
            "fresh_count": 1,
            "total_layers": 2,
            "stale_layers": [],
        },
        freshness_items=[],
        public_trust=public_trust,
    )

    assert "Public Trust Metadata" in html
    assert trust_state.edition_id in html
    assert "2026-08-10T12:33:00+00:00" in html
    assert "current" in html
    assert "stale" in html
    assert "dst_" + "3" * 64 in html
    assert "agrural_paranagua.soybean.beans.fob" in html
    assert "src_agrural" in html
    assert "obs_" + "4" * 64 in html
    assert "rev_" + "5" * 64 in html
    assert "499.50" not in html
