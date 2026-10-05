"""Layer 29 — Brazil customs exports from MDIC/SECEX Comex Stat (#351).

Pins the things that can silently break this layer:

  1. **NCM scope.** Seven 8-digit codes map to three products. A wrong code
     parses cleanly and is simply the wrong cargo, so the set is pinned
     against MDIC's own NCM descriptions — and seed beans (12011000) are out.
  2. **The source's own statement of how far the numbers run.** The API says
     which month is published (`/general/dates/updated`); the fetch stops
     there. A row past it, or a published month with no soybean rows at all,
     is our fetch going wrong — Brazil has shipped soybeans every month on
     record — and hard-fails rather than storing a hole.
  3. **The rate limit.** HTTP 429 says "try again in 10 seconds" and means
     longer; the fetcher waits on a schedule and fails loudly when it runs
     out, never returning a partial window.
  4. **Strings, not numbers.** Every metric arrives as a string. A missing or
     negative metric is a shape break, not a zero.

Row values below are verbatim from the live 2026-10-05 API response
(`?language=en`), which matched the bulk EXP_2026.csv to the kilogram.
"""

from __future__ import annotations

import pandas as pd
import pytest

import config
from fetchers import comexstat
from pipeline.results import ScraperShapeError

# Verbatim live rows, 2026-10-05.
_CHINA_MT_AUG26 = {
    "coNcm": "12019000", "year": "2026", "monthNumber": "08",
    "ncm": "Soybeans, whether or not crushed, except for sowing",
    "country": "China", "state": "Mato Grosso",
    "metricFOB": "301478563", "metricKG": "681136736", "metricStatistic": "681142",
}
_CHINA_UNDECLARED_AUG26 = {
    "coNcm": "12019000", "year": "2026", "monthNumber": "08",
    "ncm": "Soybeans, whether or not crushed, except for sowing",
    "country": "China", "state": "Não Declarada",
    "metricFOB": "128240235", "metricKG": "287283875", "metricStatistic": "541050",
}
_MEAL_NL_AUG26 = {
    "coNcm": "23040090", "year": "2026", "monthNumber": "08",
    "ncm": "Soybean waste, solid",
    "country": "Netherlands", "state": "Mato Grosso",
    "metricFOB": "16413188", "metricKG": "46184865", "metricStatistic": "46185",
}
_OIL_INDIA_AUG26 = {
    "coNcm": "15071000", "year": "2026", "monthNumber": "08",
    "ncm": "Crude soya-bean oil, whether or not degummed",
    "country": "India", "state": "Paraná",
    "metricFOB": "62845464", "metricKG": "53468108", "metricStatistic": "53468",
}
# A real published row whose net weight rounds to zero kg.
_OIL_ZERO_KG_JUN26 = {
    "coNcm": "15079011", "year": "2026", "monthNumber": "06",
    "ncm": "Soya-bean oil, refined, in containers with capacity<= 5l",
    "country": "Greece", "state": "São Paulo",
    "metricFOB": "767", "metricKG": "0", "metricStatistic": "0",
}


def _soy_row(year: int, month: int, kg: int = 1_000_000_000, country: str = "China") -> dict:
    return {
        "coNcm": "12019000", "year": str(year), "monthNumber": f"{month:02d}",
        "ncm": "Soybeans, whether or not crushed, except for sowing",
        "country": country, "state": "Mato Grosso",
        "metricFOB": str(kg // 2), "metricKG": str(kg), "metricStatistic": str(kg // 1000),
    }


def _payload(rows: list[dict]) -> dict:
    return {"data": {"list": rows}, "success": True, "message": None,
            "processo_info": None, "language": "en"}


def _window(start: str, declared: tuple[int, int], extra: list[dict] | None = None) -> list[dict]:
    """One soybean row for every month in [start, declared], plus extras."""
    rows = []
    for period in pd.period_range(start, f"{declared[0]}-{declared[1]:02d}", freq="M"):
        rows.append(_soy_row(period.year, period.month))
    return rows + (extra or [])


# ---------------------------------------------------------------------------
# 1. NCM scope
# ---------------------------------------------------------------------------


def test_ncm_codes_are_pinned_to_the_three_soy_complex_products():
    assert config.COMEXSTAT_NCM == {
        "12019000": "Soybeans",        # Soja, mesmo triturada, exceto para semeadura
        "23040010": "Soybean Meal",    # Farinhas e pellets, da extração do óleo de soja
        "23040090": "Soybean Meal",    # Bagaços e outros resíduos sólidos, ...
        "15071000": "Soybean Oil",     # Óleo de soja, em bruto, mesmo degomado
        "15079011": "Soybean Oil",     # Refinado, recipientes <= 5 l
        "15079019": "Soybean Oil",     # Refinado, outros
        "15079090": "Soybean Oil",     # Outros óleos de soja
    }
    assert "12011000" not in config.COMEXSTAT_NCM  # seed for sowing — not cargo
    assert set(config.COMEXSTAT_NCM.values()) == set(config.COMEXSTAT_PRODUCTS)


# ---------------------------------------------------------------------------
# 2. Parsing at the source's declared month
# ---------------------------------------------------------------------------


def test_parse_keeps_published_rows_verbatim_by_product():
    rows = _window("2026-07", (2026, 8), extra=[
        _CHINA_MT_AUG26, _CHINA_UNDECLARED_AUG26, _MEAL_NL_AUG26, _OIL_INDIA_AUG26,
    ])
    frames = comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-07")

    assert set(frames) == {"Soybeans", "Soybean Meal", "Soybean Oil"}
    soy = frames["Soybeans"]
    china_mt = soy[(soy["state"] == "Mato Grosso") & (soy["kg"] == 681_136_736)]
    assert len(china_mt) == 1
    row = china_mt.iloc[0]
    assert row["month_end"] == pd.Timestamp("2026-08-31")
    assert row["ncm"] == "12019000"
    assert row["country"] == "China"
    assert row["fob_usd"] == 301_478_563
    assert row["qty_stat"] == 681_142
    # The undeclared-state row is a real published row and is kept as such.
    assert "Não Declarada" in set(soy["state"])
    assert frames["Soybean Meal"]["kg"].tolist() == [46_184_865]
    assert frames["Soybean Oil"]["fob_usd"].tolist() == [62_845_464]


def test_a_published_zero_kilogram_row_is_kept_as_zero_not_dropped():
    rows = _window("2026-06", (2026, 8), extra=[_OIL_ZERO_KG_JUN26])
    frames = comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-06")
    oil = frames["Soybean Oil"]
    assert oil["kg"].tolist() == [0]
    assert oil["fob_usd"].tolist() == [767]


def test_a_row_past_the_declared_month_hard_fails():
    rows = _window("2026-07", (2026, 8), extra=[_soy_row(2026, 9)])
    with pytest.raises(ScraperShapeError, match="2026-09"):
        comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-07")


def test_a_published_month_with_no_soybean_rows_hard_fails():
    rows = [_soy_row(2026, 6), _soy_row(2026, 8)]  # July missing
    with pytest.raises(ScraperShapeError, match="2026-07"):
        comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-06")


def test_an_unrequested_ncm_hard_fails():
    seed = dict(_CHINA_MT_AUG26, coNcm="12011000")
    rows = _window("2026-08", (2026, 8), extra=[seed])
    with pytest.raises(ScraperShapeError, match="12011000"):
        comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-08")


@pytest.mark.parametrize("field,value", [
    ("metricKG", None), ("metricFOB", ""), ("metricKG", "-5"), ("metricFOB", "12.5x"),
])
def test_a_missing_or_malformed_metric_is_a_shape_break_not_a_zero(field, value):
    bad = dict(_CHINA_MT_AUG26, **{field: value})
    rows = _window("2026-08", (2026, 8), extra=[bad])
    with pytest.raises(ScraperShapeError, match=field):
        comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-08")


@pytest.mark.parametrize("payload", [
    {"error": {"code": 429, "message": "Você excedeu o limite de solicitações."}},
    {"data": {"list": []}, "success": True},
    {"data": {}, "success": True},
    {"data": {"list": [_CHINA_MT_AUG26]}, "success": False, "message": "erro"},
])
def test_an_unusable_payload_hard_fails(payload):
    with pytest.raises(ScraperShapeError):
        comexstat.parse_general(payload, declared=(2026, 8), start="2026-08")


# ---------------------------------------------------------------------------
# 3. The fetch: declared month, request body, rate limit
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int, body: dict):
        self.status_code = status
        self._body = body
        self.text = str(body)

    def json(self):
        return self._body


_RATE_LIMITED = _Resp(429, {"error": {"code": 429, "message": "tente novamente em 10 segundos."}})
_UPDATED_AUG = _Resp(200, {"data": {"updated": "2026-09-04", "year": "2026", "monthNumber": "08"},
                           "success": True})


class _FakeRequests:
    """Stands in for the `requests` module: scripted responses, recorded calls."""

    RequestException = Exception

    def __init__(self, get: list[_Resp], post: list[_Resp]):
        self._get, self._post = list(get), list(post)
        self.posts: list[dict] = []

    def get(self, url, **kwargs):
        assert url == config.COMEXSTAT_UPDATED_URL
        return self._get.pop(0)

    def post(self, url, json=None, **kwargs):
        assert url == config.COMEXSTAT_GENERAL_URL
        self.posts.append(json)
        return self._post.pop(0)


@pytest.fixture
def sleeps(monkeypatch):
    waited: list[float] = []
    monkeypatch.setattr(comexstat, "_sleep", waited.append)
    return waited


def _install(monkeypatch, fake):
    monkeypatch.setattr(comexstat, "requests", fake)
    monkeypatch.setattr(config, "COMEXSTAT_START_MONTH", "2026-07")
    monkeypatch.setattr(comexstat, "COMEXSTAT_START_MONTH", "2026-07")


def test_fetch_requests_only_up_to_the_month_mdic_says_is_published(monkeypatch, sleeps):
    fake = _FakeRequests([_UPDATED_AUG], [_Resp(200, _payload(_window("2026-07", (2026, 8))))])
    _install(monkeypatch, fake)

    frames = comexstat.fetch_brazil_exports()

    body = fake.posts[0]
    assert body["flow"] == "export"
    assert body["monthDetail"] is True
    assert body["period"] == {"from": "2026-07", "to": "2026-08"}
    assert sorted(body["filters"][0]["values"]) == sorted(config.COMEXSTAT_NCM)
    assert set(body["details"]) == {"ncm", "country", "state"}
    assert frames["Soybeans"]["month_end"].max() == pd.Timestamp("2026-08-31")


def test_fetch_waits_out_a_rate_limit_then_succeeds(monkeypatch, sleeps):
    ok = _Resp(200, _payload(_window("2026-07", (2026, 8))))
    fake = _FakeRequests([_UPDATED_AUG], [_RATE_LIMITED, _RATE_LIMITED, ok])
    _install(monkeypatch, fake)

    frames = comexstat.fetch_brazil_exports()

    assert sleeps == list(config.COMEXSTAT_RATE_LIMIT_WAITS[:2])
    assert len(fake.posts) == 3
    assert not frames["Soybeans"].empty


def test_fetch_hard_fails_when_the_rate_limit_never_lifts(monkeypatch, sleeps):
    waits = config.COMEXSTAT_RATE_LIMIT_WAITS
    fake = _FakeRequests([_UPDATED_AUG], [_RATE_LIMITED] * (len(waits) + 1))
    _install(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="429"):
        comexstat.fetch_brazil_exports()
    assert sleeps == list(waits)


def test_fetch_hard_fails_on_a_non_rate_limit_http_error_without_retrying(monkeypatch, sleeps):
    fake = _FakeRequests([_UPDATED_AUG], [_Resp(403, {"cf": "blocked"})])
    _install(monkeypatch, fake)

    with pytest.raises(RuntimeError, match="403"):
        comexstat.fetch_brazil_exports()
    assert sleeps == []


@pytest.mark.parametrize("body", [
    {"data": {"updated": "2026-09-04", "year": "2026"}, "success": True},
    {"data": {"updated": "2026-09-04", "year": "2026", "monthNumber": "13"}, "success": True},
    {"data": {"updated": "2099-01-04", "year": "2099", "monthNumber": "01"}, "success": True},
    {"error": {"code": 500}},
])
def test_an_unreadable_or_future_declared_month_hard_fails(monkeypatch, sleeps, body):
    fake = _FakeRequests([_Resp(200, body)], [])
    _install(monkeypatch, fake)

    with pytest.raises(ScraperShapeError):
        comexstat.fetch_brazil_exports()
    assert fake.posts == []


# ---------------------------------------------------------------------------
# 4. Storage: the re-fetched window replaces what is stored
# ---------------------------------------------------------------------------


def _stored(frames: dict[str, pd.DataFrame]) -> None:
    from pipeline.clean import clean_brazil_exports
    from pipeline.store import save_brazil_exports

    for product, frame in frames.items():
        save_brazil_exports(product, clean_brazil_exports(frame))


def test_store_and_read_round_trip(patched_db):
    from pipeline.query import read_brazil_exports

    rows = _window("2026-08", (2026, 8), extra=[_CHINA_MT_AUG26, _MEAL_NL_AUG26])
    _stored(comexstat.parse_general(_payload(rows), declared=(2026, 8), start="2026-08"))

    out = read_brazil_exports()
    china = out[(out["country"] == "China") & (out["kg"] == 681_136_736)].iloc[0]
    assert china["fob_usd"] == 301_478_563
    assert china["state"] == "Mato Grosso"
    assert pd.Timestamp(china["month_end"]) == pd.Timestamp("2026-08-31")
    assert set(out["product"]) == {"Soybeans", "Soybean Meal"}


def test_a_revised_window_replaces_rows_mdic_has_revised_away(patched_db):
    """The current year is provisional. A destination MDIC reallocates (an
    undeclared-state row later assigned to its real state) must vanish from
    the store, not survive beside its replacement and double-count."""
    from pipeline.query import read_brazil_exports

    first = _window("2026-07", (2026, 8), extra=[_CHINA_UNDECLARED_AUG26])
    _stored(comexstat.parse_general(_payload(first), declared=(2026, 8), start="2026-07"))

    reassigned = dict(_CHINA_UNDECLARED_AUG26, state="Goiás")
    second = _window("2026-07", (2026, 8), extra=[reassigned])
    _stored(comexstat.parse_general(_payload(second), declared=(2026, 8), start="2026-07"))

    aug = read_brazil_exports()
    aug = aug[pd.to_datetime(aug["month_end"]) == pd.Timestamp("2026-08-31")]
    assert "Não Declarada" not in set(aug["state"])
    assert (aug["state"] == "Goiás").sum() == 1


def test_months_outside_the_incoming_window_are_left_alone(patched_db):
    from pipeline.query import read_brazil_exports

    old = _window("2026-05", (2026, 6))
    _stored(comexstat.parse_general(_payload(old), declared=(2026, 6), start="2026-05"))
    new = _window("2026-07", (2026, 8))
    _stored(comexstat.parse_general(_payload(new), declared=(2026, 8), start="2026-07"))

    months = sorted(pd.to_datetime(read_brazil_exports()["month_end"]).dt.month.unique())
    assert months == [5, 6, 7, 8]


def test_self_healing_so_not_a_history_table():
    """The API re-serves the whole window every run — nothing to persist."""
    from pipeline.history import HISTORY_TABLES

    assert "brazil_exports" not in HISTORY_TABLES


# ---------------------------------------------------------------------------
# 5. Grading: the publication lag is not an outage
# ---------------------------------------------------------------------------


def _frames_ending(days_ago: int) -> dict[str, pd.DataFrame]:
    end = pd.Timestamp.now().normalize() - pd.Timedelta(days=days_ago)
    out = {}
    for product, ncm in (("Soybeans", "12019000"), ("Soybean Meal", "23040090"),
                         ("Soybean Oil", "15071000")):
        out[product] = pd.DataFrame([{
            "month_end": end, "ncm": ncm, "product": product, "country": "China",
            "state": "Mato Grosso", "kg": 1_000, "fob_usd": 400, "qty_stat": 1,
        }])
    return out


def _run_layer(monkeypatch, fetch) -> bool:
    import main

    layer = {lay.key: lay for lay in main._build_dict_layers()}["comexstat"]
    monkeypatch.setattr(main, "fetch_brazil_exports", fetch)
    monkeypatch.setattr(main, "save_brazil_exports", lambda n, d: None)
    return main._run_dict_layer(layer)


def test_the_month_mdic_has_not_released_yet_grades_success(monkeypatch, freshness_calls):
    """5 Oct with August as the newest month: September is due on 6 Oct, so the
    newest month_end is ~35 days old and this is the ordinary state of the
    world, not a stale feed."""
    assert _run_layer(monkeypatch, lambda: _frames_ending(35)) is True
    assert freshness_calls[-1]["status"] == "success"


def test_a_missed_release_on_top_of_the_lag_still_grades_success(monkeypatch, freshness_calls):
    assert _run_layer(monkeypatch, lambda: _frames_ending(66)) is True
    assert freshness_calls[-1]["status"] == "success"


def test_two_missed_releases_grade_stale(monkeypatch, freshness_calls):
    assert _run_layer(monkeypatch, lambda: _frames_ending(100)) is False
    assert freshness_calls[-1]["status"] == "stale"


def test_a_transport_failure_grades_failed(monkeypatch, freshness_calls):
    def boom():
        raise RuntimeError("Comex Stat general: HTTP 429 (rate-limit retries left: 0)")

    assert _run_layer(monkeypatch, boom) is False
    assert freshness_calls[-1]["status"] == "failed"


def test_a_missing_product_fails_the_key_floor(monkeypatch, freshness_calls):
    frames = _frames_ending(35)
    del frames["Soybean Oil"]
    assert _run_layer(monkeypatch, lambda: frames) is False
    assert freshness_calls[-1]["status"] != "success"


def test_registered_as_a_production_layer():
    assert "comexstat" in config.PRODUCTION_LAYER_KEYS
    assert config.LAYER_MIN_KEYS["comexstat"] == len(config.COMEXSTAT_PRODUCTS)
