"""AL1 #640: the verifier's independence is enforced, not promised (spec #639).

An AST walk over every module in ``verify/``: an import whose top-level name is
a package of this repo, or an LLM SDK, fails. Prior art: tests/test_connection.py
(forbids Turso symbols) and tests/test_matif_private_gate.py.

The repo's packages are read from the repo root rather than listed by hand, so
a new top-level package is forbidden the day it appears.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
VERIFY = REPO / "verify"

LLM_SDKS = frozenset({
    "anthropic", "openai", "google.generativeai", "google.genai", "vertexai", "cohere", "mistralai",
    "langchain", "langchain_core", "langchain_community", "llama_index", "litellm", "groq", "transformers",
    "ollama", "together", "replicate",
})


def _repo_packages() -> frozenset[str]:
    names = set()
    for entry in REPO.iterdir():
        if entry.name.startswith(".") or entry.name in {"verify", "tests", "docs", "data"}:
            continue
        if entry.is_dir() and (entry / "__init__.py").exists():
            names.add(entry.name)
        elif entry.is_dir() and any(entry.glob("*.py")):
            names.add(entry.name)  # namespace-style dirs (scripts/) count too
        elif entry.suffix == ".py":
            names.add(entry.stem)
    return frozenset(names)


REPO_PACKAGES = _repo_packages()


def forbidden_imports(source: str) -> list[str]:
    """Every import in ``source`` that crosses the boundary, as 'module'."""
    found = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            if node.level:  # relative: stays inside verify/
                continue
            modules = [node.module or ""]
        elif (
            isinstance(node, ast.Call)
            and isinstance(node.func, (ast.Name, ast.Attribute))
            and getattr(node.func, "id", getattr(node.func, "attr", "")) in {"import_module", "__import__"}
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            modules = [node.args[0].value]
        else:
            continue
        for module in modules:
            top = module.split(".")[0]
            if top in REPO_PACKAGES or top in LLM_SDKS or module in LLM_SDKS or module.rsplit(".", 1)[0] in LLM_SDKS:
                found.append(module)
    return found


def test_the_known_product_packages_are_in_the_forbidden_set():
    assert {"config", "fetchers", "pipeline", "pricing", "trust", "analysis", "app", "main", "mirror_mcp",
            "scripts", "latency"} <= REPO_PACKAGES


@pytest.mark.parametrize("path", sorted(VERIFY.rglob("*.py")), ids=lambda p: str(p.relative_to(REPO)))
def test_no_verifier_module_imports_the_product_or_an_llm_sdk(path):
    assert not forbidden_imports(path.read_text(encoding="utf-8")), (
        f"{path.relative_to(REPO)} crosses the independence boundary (spec #639; reversal condition in CLAUDE.md "
        "invariant 7: a verifier that imports product code is no longer independent)"
    )


@pytest.mark.parametrize("planted", [
    "import config",
    "from pipeline import units",
    "from pipeline.units import POUNDS_PER_MT",
    "import fetchers.ecb_fx",
    "from app.blocks import Block",
    "import anthropic",
    "from openai import OpenAI",
    "import google.generativeai as genai",
    "importlib.import_module('pricing.semantics')",
    "__import__('trust')",
])
def test_a_planted_import_is_caught(planted):
    assert forbidden_imports(f"import json\n{planted}\n"), planted


@pytest.mark.parametrize("allowed", [
    "import json", "from bs4 import BeautifulSoup", "import urllib.request", "from verify import page",
    "from . import page", "import xml.etree.ElementTree as ET",
])
def test_stdlib_third_party_and_the_verifier_itself_are_allowed(allowed):
    assert not forbidden_imports(allowed)
