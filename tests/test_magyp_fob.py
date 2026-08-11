"""Parser tests for the MAGyP Argentina official FOB web service (Layer 21)."""

import pytest

from config import MAGYP_FOB_POSITIONS
from fetchers.magyp_fob import (
    _check_every_product_present,
    _parse_posts,
)
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
        _post("15121110310E", 1376),         # crude sunflower oil granel — mapped
        _post("15121919110H", 1596),         # REFINED sunflower oil — not stored
        _post("12060090910Y", 532),          # sunflower SEED — not stored (#147)
        _post("23063010100F", 190),          # sunflower MEAL — not stored (#147)
        _post("10011900110H", 275),          # wheat — not a mapped position
    ]
    df = _parse_posts(posts)
    assert sorted(df["product"].unique()) == [
        "Soybean Meal", "Soybean Oil", "Soybeans", "Sunflower Oil",
    ]
    assert (df["date"] == "2026-08-05").all()

    # Sunflower enters on the crude oil leg only: refined oil is a different
    # good ~$220/MT away, and seed/meal are administered step-functions.
    sun = df[df["product"] == "Sunflower Oil"]
    assert len(sun) == 1
    assert sun["price_usd_mt"].iloc[0] == 1376.0
    assert sun["position"].iloc[0] == "15121110310E"

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


# ── The verified position mapping, and the guards that keep it honest ────────
# The service publishes no description field, so a wrong NCM code parses
# cleanly and is simply the wrong number (#147 found 3 of 4 inferred meal
# codes wrong). These pin what was cross-checked against dataset 358.

def test_crude_sunflower_oil_maps_to_the_verified_granel_position() -> None:
    """15121110310E = "Aceite de Girasol, a granel" (dataset 358, 52/52 days).

    A silent edit to a neighbouring 1512 code would swap crude for refined —
    a ~$220/MT error that no arithmetic downstream could catch.
    """
    assert MAGYP_FOB_POSITIONS["15121110310E"] == "Sunflower Oil"
    assert sum(p == "Sunflower Oil" for p in MAGYP_FOB_POSITIONS.values()) == 1
    # Refined oil, seed and meal are deliberately absent.
    for unmapped in ("15121919110H", "15121919121N", "12060090910Y", "23063010100F"):
        assert unmapped not in MAGYP_FOB_POSITIONS


def test_missing_product_in_a_published_circular_hard_fails() -> None:
    """A retired SIM line must not go silently dark.

    The other positions still match, so ``_parse_posts`` sees nothing wrong;
    only a per-product presence check catches it.
    """
    df = _parse_posts([
        _post("12019000190C", 450),
        _post("15071000100Q", 1186),
        _post("23040010100B", 350),
        # sunflower absent — its SIM line moved
    ])
    with pytest.raises(ScraperShapeError, match="Sunflower Oil"):
        _check_every_product_present(df)


def test_full_circular_passes_the_presence_check() -> None:
    df = _parse_posts([
        _post("12019000190C", 450),
        _post("15071000100Q", 1186),
        _post("23040010100B", 350),
        _post("15121110310E", 1376),
    ])
    _check_every_product_present(df)  # does not raise


def test_agreeing_sunflower_siblings_are_watched_but_not_stored() -> None:
    """Three SIM lines, one price, one stored row.

    `position` is part of the primary key and of the git-committed history
    CSV, so storing all three would triple the row for one economic number.
    """
    df = _parse_posts([
        _post("12019000190C", 450),
        _post("15121110310E", 1376),
        _post("15121110911P", 1376),
        _post("15121110919G", 1376),
    ])
    assert len(df[df["product"] == "Sunflower Oil"]) == 1


def test_diverging_sunflower_siblings_hard_fail() -> None:
    """If MAGyP ever prices the SIM lines apart, the stored line stops
    standing for the others — that is a decision for a human, not a
    silent pick of whichever code happens to be mapped."""
    with pytest.raises(ScraperShapeError, match="disagree"):
        _parse_posts([
            _post("12019000190C", 450),
            _post("15121110310E", 1376),
            _post("15121110911P", 1402),  # split
        ])


def test_sibling_check_compares_matching_shipment_windows_only() -> None:
    """A sibling quoting a *different window* is not a divergence."""
    df = _parse_posts([
        _post("12019000190C", 450),
        _post("15121110310E", 1376, 8, 8),
        _post("15121110911P", 1358, 9, 12),  # further window, lower — normal
    ])
    assert len(df[df["product"] == "Sunflower Oil"]) == 1
