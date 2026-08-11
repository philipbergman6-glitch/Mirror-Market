from __future__ import annotations

from datetime import date, datetime, timezone

from jinja2 import Environment, FileSystemLoader

from scripts import generate_html
from trust import (
    CandidateEditionRender,
    CriticalNumberProvenance,
    EditionPublicTrustState,
    FreshnessState,
    QualityState,
    QueryCacheBuild,
)


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


def test_static_site_candidate_renderer_writes_candidate_dashboard_without_touching_public_docs(
    tmp_path,
    monkeypatch,
) -> None:
    trust_state = _public_trust_state()
    public_docs = tmp_path / "docs"
    public_docs.mkdir()
    public_index = public_docs / "index.html"
    public_index.write_text("current public dashboard", encoding="utf-8")
    candidate_dir = tmp_path / "candidates" / trust_state.edition_id
    cache_path = tmp_path / "trusted-query-cache.sqlite"
    cache_path.write_text("cache placeholder", encoding="utf-8")
    _stub_static_generation_dependencies(monkeypatch)

    renderer = generate_html.static_site_candidate_renderer(public_trust_state=trust_state)
    artifacts = renderer(cache_path, candidate_dir, object())

    dashboard = candidate_dir / "index.html"
    assert artifacts == {"dashboard": dashboard}
    assert dashboard.is_file()
    html = dashboard.read_text(encoding="utf-8")
    assert "Public Trust Metadata" in html
    assert trust_state.edition_id in html
    assert "499.50" not in html
    assert public_index.read_text(encoding="utf-8") == "current public dashboard"
    assert not (candidate_dir / "players.html").exists()


def test_static_site_deployer_copies_rendered_dashboard_to_public_index(tmp_path) -> None:
    public_dir = tmp_path / "public"
    candidate_dir = tmp_path / "candidate"
    candidate_dir.mkdir()
    rendered_dashboard = candidate_dir / "index.html"
    rendered_dashboard.write_text("<main>verified edition</main>", encoding="utf-8")
    render = CandidateEditionRender(
        edition_id="edn_" + "1" * 64,
        output_dir=candidate_dir,
        cache_build=QueryCacheBuild(
            cache_path=candidate_dir / "trusted-query-cache.sqlite",
            mode="edition",
            revision_count=0,
            edition_id="edn_" + "1" * 64,
        ),
        generated_artifact_paths={"dashboard": rendered_dashboard},
    )

    evidence = generate_html.static_site_deployer(public_dir=public_dir)(object(), render)

    assert evidence == ("deployed.dashboard.index.html",)
    assert (public_dir / "index.html").read_text(encoding="utf-8") == "<main>verified edition</main>"


def _public_trust_state() -> EditionPublicTrustState:
    return EditionPublicTrustState(
        edition_id="edn_" + "1" * 64,
        generated_at=datetime(2026, 8, 10, 12, 33, tzinfo=timezone.utc),
        critical_freshness={"dst_" + "2" * 64: FreshnessState.CURRENT},
        degraded_dataset_ids=(),
        critical_numbers=(
            CriticalNumberProvenance(
                label="agrural_paranagua.soybean.beans.fob",
                source_id="src_agrural",
                dataset_id="dst_" + "2" * 64,
                dataset_key="agrural_paranagua",
                as_of_date=date(2026, 8, 10),
                quality_state=QualityState.ACCEPTED,
                observation_id="obs_" + "4" * 64,
                revision_id="rev_" + "5" * 64,
            ),
        ),
    )


def _stub_static_generation_dependencies(monkeypatch) -> None:
    monkeypatch.setattr(generate_html, "validate_players", lambda: [])
    monkeypatch.setattr(
        generate_html,
        "_safe_call",
        lambda _fn, label: "" if label == "briefing" else None,
    )
