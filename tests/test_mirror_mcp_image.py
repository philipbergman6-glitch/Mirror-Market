"""The MCP image and Fly app definition (#579) say what the server needs — and nothing private."""

from __future__ import annotations

import re
from pathlib import Path

import tomllib

from mirror_mcp.server import HEALTH_PATH
from mirror_mcp.settings import ENV_DB_BUILT_AT, ENV_DB_PATH, ENV_LOG_PATH, ENV_PATH_TOKEN

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = (ROOT / "Dockerfile").read_text()
DOCKERIGNORE = (ROOT / ".dockerignore").read_text().splitlines()
FLY = tomllib.loads((ROOT / "fly.toml").read_text())

SECRET_SHAPES = (r"FlyV1", r"fm2_", r"[A-Za-z0-9_-]{40,}", r"token\s*=\s*['\"][^$'\"]{8,}")


def test_no_secret_literal_in_image_or_app_definition():
    for name in ("Dockerfile", "fly.toml", ".dockerignore"):
        text = (ROOT / name).read_text()
        for shape in SECRET_SHAPES:
            assert not re.search(shape, text), (name, shape)
    assert ENV_PATH_TOKEN not in DOCKERFILE
    assert ENV_PATH_TOKEN not in (ROOT / "fly.toml").read_text()


def test_fly_health_check_is_the_servers_health_route():
    checks = FLY["http_service"]["checks"]
    assert [c["path"] for c in checks] == [HEALTH_PATH]
    assert FLY["http_service"]["internal_port"] == int(FLY["env"]["MIRROR_MCP_PORT"])


def test_volume_holds_the_question_log_and_the_db_lives_in_the_image():
    assert FLY["mounts"]["destination"] == "/data"
    assert FLY["env"][ENV_LOG_PATH].startswith("/data/")
    assert FLY["env"][ENV_DB_PATH].startswith("/app/")


def test_one_machine_that_never_sleeps():
    svc = FLY["http_service"]
    assert svc["auto_stop_machines"] == "off" and svc["min_machines_running"] == 1
    assert FLY["vm"][0]["size"] == "shared-cpu-1x"


def test_image_installs_ca_certificates_and_bakes_the_db():
    assert "ca-certificates" in DOCKERFILE and "update-ca-certificates" in DOCKERFILE
    assert re.search(r"^COPY \$\{DB_PATH\} /app/db/mirror_market\.db$", DOCKERFILE, re.M)
    assert f"ENV {ENV_DB_BUILT_AT}=${{BUILT_AT}}" in DOCKERFILE
    assert 'test -n "${BUILT_AT}"' in DOCKERFILE
    assert "USER mirror" in DOCKERFILE
    assert "requirements-mcp.txt" in DOCKERFILE and "requirements-dev" not in DOCKERFILE


def test_private_directories_never_enter_the_build_context():
    from analysis.futures.privacy import client_record_dirs

    ignored = {line.strip() for line in DOCKERIGNORE if line.strip() and not line.startswith("#")}
    for required in (
        ".env", "data/workspace", "data/history", "data/reference/assumptions/private",
        "data/reference/positions", "data/reference/options", "data/reference/clearing",
        "data/reference/import_profiles", "docs", "tests",
    ):
        assert required in ignored, required
    for path in client_record_dirs():
        rel = Path(path).resolve().relative_to(ROOT).as_posix()
        assert any(rel == i or rel.startswith(i.rstrip("/") + "/") for i in ignored), rel
