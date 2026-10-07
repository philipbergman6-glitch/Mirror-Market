"""The one Jinja environment factory for every rendered page.

Four renderers used to build their own ``Environment`` with the same loader
and the same autoescape stance (#313). That was tolerable while the templates
needed nothing from Python but the context each page passed in. The "How to
read" key changed that: ``_base.html.j2`` renders it on *every* page, and its
content is the code's own vocabulary (``app.wayfinding``), so the base
template needs a global every environment provides. One factory, or the key
silently vanishes from whichever page forgot to pass it.
"""

from __future__ import annotations

from pathlib import Path

from jinja2 import Environment, FileSystemLoader

TEMPLATE_DIR = Path(__file__).resolve().parent / "templates"


def site_environment(**options) -> Environment:
    """Autoescaping environment over ``app/templates`` with the shared globals.

    Autoescape is unconditional and not overridable here: it is the
    default-deny stance (#313) — externally derived text renders inert unless
    a template says ``| safe`` at the exact spot a trusted fragment is
    embedded. ``options`` passes through anything else (``trim_blocks`` …).
    """
    from app.wayfinding import how_to_read_vocabulary

    if "autoescape" in options:
        raise ValueError("autoescape is always on for site templates (#313)")
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=True, **options)
    env.globals["how_to_read"] = how_to_read_vocabulary
    return env


__all__ = ["TEMPLATE_DIR", "site_environment"]
