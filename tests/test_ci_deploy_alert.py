import pytest

from scripts import ci_deploy_alert


@pytest.mark.parametrize(
    ("details", "phrase"),
    [
        (("failure", "", "", ""), "page-generation step failed"),
        (("success", "failure", "", ""), "candidate promotion contract failed"),
        (("success", "success", "failure", ""), "GitHub Pages deployment failed"),
        (("success", "success", "success", "failure"), "post-deployment public smoke test failed"),
    ],
)
def test_deploy_alert_names_the_failed_stage(details, phrase):
    body = ci_deploy_alert.build_alert_body("failure", "success", *details)

    assert phrase in body


def test_check_gate_failure_is_distinct_from_publication_failures():
    body = ci_deploy_alert.build_alert_body("skipped", "failure")

    assert "CI check gate failed" in body


def test_pipeline_failure_before_generation_is_not_called_a_deployment_failure():
    body = ci_deploy_alert.build_alert_body(
        "failure", "success", "skipped", "skipped", "skipped", "skipped"
    )

    assert "build failed before page generation" in body
    assert "GitHub Pages deployment failed" not in body
