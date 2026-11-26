"""Canonical serialization of every fetcher whose unit factor #665 moved.

Shared by ``tests/test_unit_sites.py`` and the one-off capture that wrote
``tests/fixtures/unit_sites/golden.json`` from the code *before* the move.
Each entry runs a fetcher's parse path on a recorded fixture and serializes
every stored column with ``repr`` floats, so equality is byte-identity.
"""

from __future__ import annotations

import gzip
import json
import math
from collections.abc import Callable
from datetime import date
from pathlib import Path

import pandas as pd

FIXTURES = Path(__file__).parent / "fixtures"


def _canon(value: object) -> object:
    if isinstance(value, float):
        return "nan" if math.isnan(value) else repr(value)
    if isinstance(value, (int, str)) or value is None:
        return value
    if hasattr(value, "item"):  # numpy scalar
        return _canon(value.item())
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return repr(value)


def _frame(df: pd.DataFrame) -> list[dict[str, object]]:
    return [{str(k): _canon(v) for k, v in row.items()} for row in df.reset_index().to_dict("records")]


def _cepea() -> object:
    from fetchers.cepea import _parse_indicator_tables
    data, errors = _parse_indicator_tables((FIXTURES / "cepea_soja_br.html").read_text(encoding="utf-8"))
    return {"errors": errors, **{k: _frame(v) for k, v in sorted(data.items())}}


def _agrural() -> object:
    from fetchers.agrural import _parse_agrural_table
    return _frame(_parse_agrural_table((FIXTURES / "agrural_paranagua.html").read_text(encoding="utf-8")))


def _noticias() -> object:
    from fetchers.noticias_agricolas import _parse_indicator_page
    return {
        name: _frame(_parse_indicator_page((FIXTURES / name).read_text(encoding="utf-8")))
        for name in ("noticias_agricolas_parana.html",)
    }


def _msamb_via_mandi_aggregate() -> object:
    from fetchers.mandi import _aggregate
    from fetchers.msamb import _completed, _parse_grid
    records = _parse_grid((FIXTURES / "msamb_soybean_2026-11-17.html").read_text())
    return {"records": [{k: _canon(v) for k, v in r.items()} for r in records],
            "aggregate": _frame(_aggregate(_completed(records, date(2026, 11, 17))))}


def _ncdex_spot() -> object:
    from fetchers.ncdex_spot import _parse
    payload = json.loads((FIXTURES / "ncdex_spot_sybeanidr_2026-11-17.json").read_text())
    return _frame(_parse(payload, "SYBEANIDR", "Indore"))


def _conab_precos() -> object:
    from fetchers.conab_precos import _parse_farmgate
    with gzip.open(FIXTURES / "conab_precos_semanal_uf.txt.gz", "rb") as fh:
        return _frame(_parse_farmgate(fh.read().decode("latin-1")))


def _conab_yield() -> object:
    from fetchers.conab import _aggregate_national, _parse_csv
    with gzip.open(FIXTURES / "conab_serie_historica_graos.txt.gz", "rb") as fh:
        df = _parse_csv(fh.read().decode("latin-1"))  # CONAB serves latin-1
    df.columns = [c.strip().lower() for c in df.columns]
    return _frame(_aggregate_national(df))


def _afex() -> object:
    # No recorded AFEX payload is committed; these are the NGN/kg levels the
    # layer's own tests use plus awkward decimals, run through the real parser.
    from fetchers.afex import _parse_series
    prices = [681.25, 690.0, 525.21, 333.33, 712.07, 1234.567]
    payload = [{"date": f"2026-08-{i + 1:02d}", "SSBS": p, "SMAZ": 336.99} for i, p in enumerate(prices)]
    return _frame(_parse_series(payload, "SSBS", "Soybean (AFEX Nigeria)"))


def _sopa() -> object:
    from fetchers.sopa import parse_crop_table
    estimate = parse_crop_table((FIXTURES / "sopa_2025.html").read_text(encoding="utf-8"), 2025)
    assert estimate is not None
    return repr(estimate)


CAPTURES: dict[str, Callable[[], object]] = {
    "cepea": _cepea,
    "agrural": _agrural,
    "noticias_agricolas": _noticias,
    "msamb_via_mandi_aggregate": _msamb_via_mandi_aggregate,
    "ncdex_spot": _ncdex_spot,
    "conab_precos": _conab_precos,
    "conab_yield": _conab_yield,
    "afex": _afex,
    "sopa": _sopa,
}

GOLDEN = FIXTURES / "unit_sites" / "golden.json"


if __name__ == "__main__":
    GOLDEN.write_text(json.dumps({k: f() for k, f in CAPTURES.items()}, indent=1, sort_keys=True, ensure_ascii=False) + "\n")
