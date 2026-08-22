"""The local env file reaches config, and never outranks the environment.

Nothing in the repo loaded `.env` before this. A populated file therefore
left every key constant at `""`, and Layers 2/3/10/13/16 skipped while the
run looked healthy — the silent-degradation failure invariant 1 exists to
prevent, arriving through the configuration rather than through a fetcher.
"""

from __future__ import annotations

from pathlib import Path

import config
from config import API_KEY_LAYERS, _load_env_file, missing_api_keys

_VAR = "MIRROR_MARKET_TEST_ONLY_KEY"


def _write(tmp_path: Path, body: str) -> Path:
    path = tmp_path / ".env"
    path.write_text(body)
    return path


def test_a_variable_in_the_file_reaches_the_environment(tmp_path, monkeypatch):
    monkeypatch.delenv(_VAR, raising=False)

    assert _load_env_file(_write(tmp_path, f"{_VAR}=from-the-file\n")) is True

    import os
    assert os.environ[_VAR] == "from-the-file"


def test_the_environment_outranks_the_file(tmp_path, monkeypatch):
    """CI sets its keys from GitHub secrets; a stray file must not win.

    `override=False` is the whole contract — a developer exporting a key for
    one run would otherwise be silently overruled by a stale file.
    """
    monkeypatch.setenv(_VAR, "from-the-environment")

    _load_env_file(_write(tmp_path, f"{_VAR}=from-the-file\n"))

    import os
    assert os.environ[_VAR] == "from-the-environment"


def test_a_missing_file_is_not_an_error(tmp_path, monkeypatch):
    # CI ships no env file at all. Absence is the normal case there, not a
    # degraded one, so it must not raise and must be reported as False.
    monkeypatch.delenv(_VAR, raising=False)

    assert _load_env_file(tmp_path / ".env") is False


def test_the_repo_env_file_is_loaded_at_import_when_it_exists():
    # ENV_FILE_LOADED is the honest record of which case this run is in;
    # it must agree with the filesystem rather than being assumed either way.
    assert config.ENV_FILE_LOADED is config.ENV_FILE.is_file()


def test_missing_api_keys_names_the_layers_each_key_gates(monkeypatch):
    for name in API_KEY_LAYERS:
        monkeypatch.delenv(name, raising=False)

    missing = missing_api_keys()

    assert set(missing) == set(API_KEY_LAYERS)
    assert missing["FAS_API_KEY"] == "Layer 10"


def test_a_set_key_is_not_reported_missing(monkeypatch):
    for name in API_KEY_LAYERS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("FAS_API_KEY", "anything-non-empty")

    assert "FAS_API_KEY" not in missing_api_keys()


def test_an_empty_key_is_missing_not_present(monkeypatch):
    # An unset secret in a workflow resolves to "", not to absence. Treating
    # that as configured would send an empty key to the API and read the 403
    # as an outage.
    monkeypatch.setenv("FAS_API_KEY", "")

    assert "FAS_API_KEY" in missing_api_keys()
