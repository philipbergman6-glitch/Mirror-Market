"""Parser tests for the MAGyP Argentina official FOB web service (Layer 21)."""

import pytest

from fetchers.magyp_fob import _parse_posts
from pipeline.results import ScraperShapeError


def _post(posicion: str, precio: float, mes_desde: int = 8, mes_hasta: int = 8) -> dict:
    return {
        "fecha": "2026-08-05 00:00:00.000",
        "circular": "2031",
        "posicion": posicion,
        "precio": precio,
        "mesDesde": mes_desde,
        "añoDesde": 2026,
        "mesHasta": mes_hasta,
        "añoHasta": 2026,
    }


def test_parse_posts_maps_soy_positions_and_ship_windows() -> None:
    posts = [
        _post("12019000190C", 450),          # beans granel — mapped
        _post("12019000299C", 470),          # beans embolsado — not stored
        _post("15071000100Q", 1186),         # crude oil granel — mapped
        _post("23040010100B", 350, 9, 11),   # meal pellets — mapped, fwd window
        _post("10011900110H", 275),          # wheat — not a soy position
    ]
    df = _parse_posts(posts)
    assert sorted(df["product"].unique()) == ["Soybean Meal", "Soybean Oil", "Soybeans"]
    assert (df["date"] == "2026-08-05").all()

    beans = df[df["product"] == "Soybeans"]
    assert len(beans) == 1  # bagged sub-position excluded
    assert beans["price_usd_mt"].iloc[0] == 450.0

    meal = df[df["product"] == "Soybean Meal"].iloc[0]
    assert meal["ship_from"] == "2026-09"
    assert meal["ship_to"] == "2026-11"


def test_parse_posts_raises_when_no_soy_position_matches() -> None:
    """A published circular with zero mapped positions means the NCM codes moved."""
    with pytest.raises(ScraperShapeError, match="none matched"):
        _parse_posts([_post("10011900110H", 275)])


def test_parse_posts_raises_on_malformed_mapped_post() -> None:
    bad = _post("12019000190C", 450)
    del bad["mesDesde"]
    with pytest.raises(ScraperShapeError, match="unparseable"):
        _parse_posts([bad])


def test_parse_posts_empty_input_returns_empty_frame() -> None:
    """Zero posts is a normal holiday outcome, not a shape error."""
    assert _parse_posts([]).empty
