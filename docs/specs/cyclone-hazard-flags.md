# Spec — cyclone hazard flags (build-ready)

Ticket: [S2 · Spec: cyclone hazard flags](https://github.com/philipbergman6-glitch/Mirror-Market/issues/374) · Map: [God's Eye View findings](https://github.com/philipbergman6-glitch/Mirror-Market/issues/356) · Written 2026-10-06 against `main` @ `2024217`.

This is a plan, not a build. A build session should be able to execute it top to bottom without re-deciding anything. Line numbers are from `2024217` and will drift; the function and constant names are the reference.

**Where each decision comes from**

| Tag | Source |
|---|---|
| **[P1]** | [P1 · Presentation prototype + rules sign-off](https://github.com/philipbergman6-glitch/Mirror-Market/issues/362), signed off 2026-10-06, variant A |
| **[K2]** | [K2 · Spike](https://github.com/philipbergman6-glitch/Mirror-Market/issues/361) — findings `research/k2-footprint-weather-spike.md`, reference code `spike/footprint-weather/storms.py`, both on branch `spike/footprint-weather` |
| **[R2]** | [R2 · Cyclone sources](https://github.com/philipbergman6-glitch/Mirror-Market/issues/358) — `research/2026-10-05-issue-358-cyclone-sources.md` on branch `research/r2-cyclone-sources` |
| **[K1]** | [K1 · Place list](https://github.com/philipbergman6-glitch/Mirror-Market/issues/360) — `research/k1-place-list.md` on branch `research/k1-place-list` |
| **[S2]** | Decided in this spec, because the upstream tickets left it open or the code contradicted them. Every one is listed in §14 so the owner can overrule it in one place. |

---

## 1. What ships

A **hazard flag** is a dated warning attached to a rendered leg because a tropical cyclone threatens a place that prices it (`CONTEXT.md`). Version 1 ships:

1. Two new pipeline layers that read active-storm forecast tracks and wind radii from **NOAA NHC** and **JTWC**.
2. A **place registry** of the ports and pricing points the ledger legs price at, and a `LEG_PLACES` map from ledger `leg_id` to those places.
3. One assessment module that grades each place `flag / watch / clear / not_covered / stale / failed`.
4. Three surfaces: a chip in the ledger's State column (market pages and the origins page), a "Storms — the port leg" section in block 06, and a `STORMS` section in the daily briefing.

**Not in version 1** — each is a decision, not an omission:

| Not | Why |
|---|---|
| Flags on growing areas | Agency wind radii are "valid only over water" [R2 §2.2]. An un-gated rule flagged Illinois from Francine's remnant [K2 §6]. Storm rain on a growing area is the footprint's job (S1). [P1 #5] |
| A page banner | [P1 #4] |
| Anything on the headline, Emerging Markets or the competing-oil strip | [P1 #2]. See §14.3 for how this is applied to the headline ledger. |
| Risk Monitor rows or `signals` entries | [P1 #2] sends only *forecast weather alerts* there. A hazard flag is not a signal (`CONTEXT.md`: "signals are price-derived"). |
| National warning signals (IMD port signals, CMA colours, BoM) | Licence-blocked for publishing [R2 §2.5–2.8]. The flag is derived from NHC/JTWC radii and says so. |
| Cross-checks against GDACS or JMA | Optional in [R2 §3]; not needed for the approved rules. |
| Private editions (workstation, opportunity board) | Not in [P1]. |

---

## 2. Sources

| | NHC | JTWC |
|---|---|---|
| Layer key | `cyclones_nhc` | `cyclones_jtwc` |
| Basins | North Atlantic, East and Central Pacific | West Pacific, North Indian, all Southern Hemisphere; duplicates East/Central Pacific |
| Index (fixed URL) | `https://www.nhc.noaa.gov/CurrentStorms.json` | `https://www.metoc.navy.mil/jtwc/rss/jtwc.rss` |
| Per-storm product | `https://www.nhc.noaa.gov/gis/forecast/archive/{id}_5day_latest.zip` (track points) and `{id}_fcst_latest.zip` (wind radii) | `https://www.metoc.navy.mil/jtwc/products/{id}.tcw` |
| Storm id | `al062024` — basin(2) + number(2) + year(4) | `wp2626` — basin(2) + number(2) + year(2) |
| Winds | 1-minute, knots | 1-minute, knots |
| Radii | 34/50/64 kt by quadrant; 64 kt stops at 72 h | 34/50/64 kt by quadrant at every forecast time |
| Horizon | 120 h | 120 h |
| Cadence | 03/09/15/21 UTC | Northern Hemisphere 6-hourly, Southern Hemisphere 12-hourly |
| Key | none | none |
| Publishing rights | Public domain; must not be modified and presented as official [R2 §2.1] | "Public information… may be freely distributed or copied" [R2 §2.2] |
| Attribution string | `NOAA National Hurricane Center` | `Joint Typhoon Warning Center (JTWC) — US military guidance, not a national warning` |

Both are US-government sources with the same wind convention, so one threshold vocabulary works globally [R2 §3]. Never mix in an agency that averages winds differently.

**Two layers, not one.** The sources fail independently, and a JTWC outage must not turn the US Gulf reading `failed`. This is the Layer 27/28 precedent (`main.py` `_build_dict_layers`, the `river_us` / `river_ar` entries).

**Trust registry.** These layers are **not** registered in `trust.registry`. `PILOT_REGISTRY` (`trust/registry.py`) holds only the three pilot price sources (AgRural, MAGyP, Yahoo Finance), and Layers 27–31 are all outside it. Rights are recorded the way those layers record them: the `LAYERS.md` entry plus an `attribution` value stamped on every stored row. The table below is written in the registry's own `RightsAction` vocabulary so a later migration is a transcription.

| `RightsAction` | NHC | JTWC |
|---|---|---|
| `raw-content-retention` | allowed | allowed |
| `normalized-history-retention` | allowed | allowed |
| `internal-display` | allowed | allowed |
| `public-display` | allowed | allowed |
| `derived-publication` | allowed, never presented as official | allowed, never presented as official |
| `commercial-use` | allowed | allowed |
| `redistribution` | allowed | allowed |

### 2.1 De-duplication [K2 §6]

JTWC re-issues East and Central Pacific storms that NHC also carries. A JTWC storm is a duplicate **only when NHC's live list currently holds a storm with the same basin prefix, the same storm number and the same year**. Never drop by the `ep`/`cp` prefix alone: a storm that crosses 180° keeps its `ep` id at JTWC after NHC stops issuing on it (Nolo, `ep1526`, 2026-10-05).

- De-duplication happens in the **assessment**, not in the fetcher. The JTWC layer stores every storm it read.
- If the NHC layer's latest run is not `success`, nothing is dropped. No double-counting is possible then, because NHC contributed no storms.

---

## 3. Data contract

### 3.1 Layer registration

Add two rows to `config.PRODUCTION_LAYERS`, after `comexstat`:

```python
("cyclones_nhc", "<n>", "NOAA NHC", "Per advisory, read daily", "Tropical-cyclone forecast tracks and wind radii — Atlantic, East/Central Pacific"),
("cyclones_jtwc", "<n+1>", "JTWC", "Per advisory, read daily", "Tropical-cyclone forecast tracks and wind radii — West Pacific, Indian Ocean, Southern Hemisphere"),
```

`<n>` is the next free group number when the build starts. On `2024217` that is 32 and 33, but the footprint-weather build (S1) may take one first. Count from `config.PRODUCTION_LAYERS`, never from this prose.

Everything else a new layer must touch:

| File | Change |
|---|---|
| `fetchers/cyclones.py` (new) | `fetch_nhc_storms()` and `fetch_jtwc_storms()`, each returning `dict[str, pd.DataFrame]` with exactly the keys `"status"`, `"storms"`, `"track"` |
| `pipeline/schema.py` | The four tables in §3.2 |
| `pipeline/clean.py` | `clean_cyclone_frame(name, df)` — returns a copy; type coercion and the bounds checks in §3.4 |
| `pipeline/store.py` | `save_cyclone_frame(source, name, df)` — see §3.3 |
| `pipeline/query.py` | `read_cyclone_status()`, `read_cyclone_storms()`, `read_cyclone_track()` |
| `pipeline/history.py` | Three `HISTORY_TABLES` entries (§3.2) |
| `main.py` `_build_dict_layers` | Two `DictLayer` entries after `river_ar`, each with `empty_fails=True` |
| `latency/domain.py` | Two `LayerLatency` entries: `LatencyClass.WEATHER`, `ObservationClock()`, `timedelta(hours=12)`, note "the advisory in hand at the daily read; agencies re-issue every 6 h (12 h in the Southern Hemisphere)" |
| `requirements.txt` | `pyshp` with a `~=` pin on the version the build installs. Pure Python, reads the NHC shapefiles. The spike used it (`import shapefile`). |
| `LAYERS.md` | Two layer entries carrying the traps in §12; bump the layer counts |
| `ARCHITECTURE.md` | Tables list in "Store"; one paragraph under it modelled on the `river_levels` bullet; a "hazard flag" row in "Where to add new things" |
| `CLAUDE.md`, `README.md`, `tests/test_docs_claims.py` | Layer count 35 → 37, groups 31 → 33 (or whatever `PRODUCTION_LAYERS` then says) |
| `config.py` | Not added to `FAST_REFRESH_LAYERS`, `LAYER_MIN_KEYS`, `LAYER_KEY_CATALOGS` or `LAYER_MAX_DATA_AGE_DAYS` — reasons in §3.5 |

### 3.2 Tables

All times are UTC ISO-8601 text. All winds are knots, all radii nautical miles, exactly as the agencies publish. Nothing here is a price and nothing passes through `to_usd_mt`.

**`cyclone_source_status`** — one row per source per run that the source answered. History: yes.

| Column | Type | Meaning |
|---|---|---|
| `source` | TEXT NOT NULL | `NHC` or `JTWC` |
| `Date` | TEXT NOT NULL | UTC date of the check |
| `checked_at` | TEXT NOT NULL | UTC time the index was read |
| `storms_listed` | INTEGER NOT NULL | Storms the index named. `0` is a real answer: asked, none active. |
| `storms_parsed` | INTEGER NOT NULL | Storms whose track was read |
| `products_absent` | INTEGER NOT NULL | JTWC only: listed products that answered 403 (§12.3). `0` for NHC. |
| `index_last_modified` | TEXT | The index's HTTP `Last-Modified` header, or NULL if not served. Recorded, not graded (§3.5). |
| `attribution` | TEXT NOT NULL | The string from §2 |

Primary key `(source, Date)`.

**`cyclone_storms`** — one row per storm per advisory read. History: yes.

| Column | Type | Meaning |
|---|---|---|
| `source` | TEXT NOT NULL | `NHC` or `JTWC` |
| `storm_id` | TEXT NOT NULL | The source's own id, lower case |
| `issued_at` | TEXT NOT NULL | Advisory issue time |
| `checked_at` | TEXT NOT NULL | Matches the status row |
| `basin_prefix` | TEXT NOT NULL | First two letters of the id |
| `storm_number` | INTEGER NOT NULL | Digits 3–4 of the id |
| `season` | INTEGER NOT NULL | Four-digit year (JTWC's two digits + 2000) |
| `name` | TEXT | As published |
| `classification` | TEXT | NHC's code (`HU`, `TS`, `TD`, `STS`, `STD`, `PTC`, `PC`). NULL for JTWC, which publishes none in the `.tcw`; the label is derived at display (§8.4). |
| `advisory` | TEXT | NHC advisory number. NULL for JTWC. |
| `lat`, `lon` | REAL | Position at forecast hour 0. NULL only when `track_state = 'absent'`. |
| `vmax_kt` | INTEGER | Wind at forecast hour 0. NULL only when `track_state = 'absent'`. |
| `max_tau_h` | INTEGER | Last forecast hour the track carries. NULL only when `track_state = 'absent'`. |
| `track_state` | TEXT NOT NULL | `ok`, or `absent` when JTWC lists the storm but its `.tcw` answers 403 |
| `attribution` | TEXT NOT NULL | |

Primary key `(source, storm_id, issued_at)`. The four nullable columns are nullable in the schema because an absent track has no position to store (NULL = never learned); `save_cyclone_frame` enforces that they are all present when `track_state = 'ok'`. For an absent track, `issued_at` is the `checked_at` of the run, since no advisory time was read.

**`cyclone_track_points`** — the forecast track of the latest advisory only. History: **no**.

| Column | Type | Meaning |
|---|---|---|
| `source`, `storm_id`, `issued_at` | TEXT NOT NULL | Joins to `cyclone_storms` |
| `tau_h` | INTEGER NOT NULL | Forecast hour, 0–120 |
| `lat`, `lon` | REAL NOT NULL | Centre position |
| `vmax_kt` | INTEGER NOT NULL | |
| `r34_ne`, `r34_se`, `r34_sw`, `r34_nw` | REAL | 34-kt radius by quadrant, nm |
| `r50_ne` … `r50_nw` | REAL | 50-kt |
| `r64_ne` … `r64_nw` | REAL | 64-kt |
| `is_forecast` | INTEGER NOT NULL | `0` at `tau_h = 0`, else `1` |

Primary key `(source, storm_id, issued_at, tau_h)`. **NULL radius = the agency published no radius for that band at that hour** (NHC 64 kt beyond 72 h; any band above the storm's wind). `0` = published as zero. Invariant 2.

**`hazard_flags`** — what the site published, for places in `flag` or `watch` only. History: yes.

| Column | Type | Meaning |
|---|---|---|
| `run_date` | TEXT NOT NULL | UTC date the site was generated |
| `place_id` | TEXT NOT NULL | |
| `source`, `storm_id` | TEXT NOT NULL | The storm that set the state |
| `state` | TEXT NOT NULL | `flag` or `watch` |
| `severity` | TEXT NOT NULL | `alert`, `warning` or `info` |
| `band_kt` | INTEGER | 34, 50 or 64. NULL for `watch`. |
| `storm_name`, `advisory`, `issued_at` | TEXT | |
| `first_arrival_tau_h` | INTEGER | NULL for `watch` |
| `first_arrival_at` | TEXT | `issued_at` + `first_arrival_tau_h` |
| `closest_km` | INTEGER NOT NULL | |
| `closest_tau_h` | INTEGER NOT NULL | |
| `place_lat`, `place_lon` | REAL NOT NULL | The registry coordinates used, so a later registry edit cannot rewrite history |
| `legs` | TEXT NOT NULL | Comma-joined `leg_id`s that carried the flag |

Primary key `(run_date, place_id, source, storm_id)`.

`HISTORY_TABLES` entries, each with a comment in the file's own style:

```python
"cyclone_source_status": ("source", "Date"),
"cyclone_storms": ("source", "storm_id", "issued_at"),
"hazard_flags": ("run_date", "place_id", "source", "storm_id"),
```

Why these are history tables: neither agency's live index serves a past advisory, JTWC keeps no public per-advisory archive, and a published flag is a function of that day's advisory and that day's place registry. Why `cyclone_track_points` is not: [P1 #6] approved summary rows only. NHC's archive can replay an Atlantic or East Pacific track if a verification study is ever wanted.

`export_history()` leaves the CSV untouched and logs a warning when a table is empty (`pipeline/history.py`, the "is empty — CSV left untouched" branch). `cyclone_storms` and `hazard_flags` will be legitimately empty out of season. That warning is expected and is not a defect.

### 3.3 Write rules

`save_cyclone_frame(source, name, df)`:

- `status` → upsert into `cyclone_source_status`.
- `storms` → upsert into `cyclone_storms`.
- `track` → **window-replace**: delete every `cyclone_track_points` row for that `source`, then insert, in one transaction. Use `_save(..., clear=(sql, params))`, the `brazil_exports` precedent. This also runs when the frame is empty, so a source that answered "no storms" clears yesterday's tracks. The current `save_*` functions return early on an empty frame; this one must not for `track`.
- Hard-fail (`ValueError`) on a missing `attribution`, an unknown `source`, or a `track_state = 'ok'` row with a NULL position or wind.

`hazard_flags` is written by `pipeline.store.save_hazard_flags(rows)`, called from the site generator (§7.4), `INSERT OR REPLACE`.

### 3.4 Clean rules

`clean_cyclone_frame` returns a copy and:

- coerces types; drops nothing silently;
- hard-fails if a latitude is outside ±90, a longitude outside ±180 **after** normalising to that range, a wind outside 0–250 kt, a radius outside 0–1,000 nm, or a `tau_h` outside 0–240;
- hard-fails on a duplicate primary key within one frame.

### 3.5 How a run is graded

Each fetcher always returns a one-row `status` frame when its index answered. `storms` and `track` are empty when no storm is active.

| What happened | Layer run state | Why |
|---|---|---|
| Index answered, zero storms | `success` | The `status` frame is non-empty, so `_finalize_layer` passes its shape gate. "Asked, none active" is the answer a `clear` reading depends on, so it must advance `last_success`. |
| Index answered, every listed storm parsed | `success` | |
| Index answered, a JTWC product answered 403 | `success`, with that storm stored `track_state = 'absent'` | [R2 §3]: an absent product is not a source failure. The place-level consequence is in §6.2. |
| Transport error, non-200 index, unparsable index | `failed` | The fetcher raises; `_run_dict_layer` records it. |
| A listed NHC storm's zip is missing, unreadable, or its advisory number disagrees with the index | `failed` | A half-read basin must not stamp `last_success` (invariant 1). The 20:40 UTC retry is the second shot: `retry-failed-pipeline.yml` re-runs when `hard_failures` is non-empty, and any failed layer is in it (`main.py`, the `hard_failures` key of the run summary). |
| A JTWC `.tcw` that answers 200 but yields no track lines, or an unknown header shape | `failed` | Shape change; hard-fail [K2 `parse_jtwc`]. |
| Returned `{}` | `failed` | `empty_fails=True`. A source that answered always yields a `status` row. |

`no_publication` is never used by these layers. `incomplete` cannot occur (no key catalog).

**No `LAYER_MAX_DATA_AGE_DAYS` entry, and why that is not a gap.** That budget is in whole days and reads a frame's newest `Date`. The `status` frame's `Date` is today by construction, so the gate would pass trivially and prove nothing. Invariant 10's age budget is enforced where it means something — in hours, on each advisory's own issue time, as the `stale` place state (§6.2).

**One named limitation.** A frozen index that lists *no* storms cannot be told from a quiet basin: there is no advisory to age-check. `index_last_modified` is stored so this can be graded later, but it is not graded now, because nobody has measured whether the agencies re-stamp an unchanged file. Follow-up trigger: after 30 daily runs, read the column and decide (§15).

---

## 4. Place registry and `LEG_PLACES`

### 4.1 `config.PLACES`

A new registry in `config.py`, next to `ORIGIN_PORTS`. Version 1 holds **ports and pricing points only**. Growing areas are footprints and belong to S1's registry, not here.

```python
PLACES: dict[str, dict[str, Any]] = {
    "P-NOLA": {
        "name": "US Gulf — New Orleans / lower Mississippi",
        "short": "New Orleans",
        "kind": "export_port_range",
        "lat": 29.9369, "lon": -90.0619,
        "basin": "north_atlantic",
        "effective_from": None, "effective_to": None,
    },
    ...
}
```

| id | `short` | `kind` | lat, lon | `basin` | `basin_reason` |
|---|---|---|---|---|---|
| `P-NOLA` | New Orleans | `export_port_range` | 29.9369, −90.0619 | `north_atlantic` | — |
| `P-PNG` | Paranaguá | `export_port` | −25.5047, −48.5108 | `south_atlantic` | South Atlantic — no publishable cyclone source |
| `P-UPR` | Rosario (up-river) | `export_port_range` | −32.9575, −60.6394 | `south_atlantic` | South Atlantic — no publishable cyclone source |
| `P-NCN` | North China (Qingdao) | `import_port_range` | 36.0833, 120.3170 | `west_pacific` | — |
| `P-DUR` | Durban | `import_parity_port` | −29.8737, 31.0232 | `south_west_indian` | — |
| `P-CBOT` | Burns Harbor (CBOT delivery) | `inland_pricing_point` | 41.6259, −87.1334 | `none` | inland delivery territory — agency wind radii are valid only over water |
| `P-RFT` | Randfontein (JSE reference) | `inland_pricing_point` | −26.1797, 27.7042 | `none` | inland pricing point — agency wind radii are valid only over water |
| `P-IDR` | Indore (mandi hub) | `inland_pricing_point` | 22.7186, 75.8550 | `none` | inland pricing hub — agency wind radii are valid only over water |

Coordinates are [K1 §1], each cross-checked there between UN/LOCODE and Wikidata. All eight have `effective_from = None` and `effective_to = None` today.

Field rules:

- `kind` ∈ `PLACE_KINDS = ("export_port", "export_port_range", "import_port_range", "import_parity_port", "inland_pricing_point")`.
- `basin` ∈ the keys of `CYCLONE_BASINS` (§4.2) or the literal `"none"`.
- `basin_reason` is required when `basin` is `"none"` or a basin with no source, and forbidden otherwise.
- `effective_from` / `effective_to` are `None` or an ISO date string. `None` means open-ended. A place is **active on day `d`** when `(from is None or from <= d) and (to is None or d <= to)`.

**`basin: "none"` means "not exposed", and it is a stated fact.** Such a place is never assessed, produces no state, and never contributes to a leg's flag. Its reason is rendered (§8.3). This is [K2 §7.4]'s "`none` with a reason", applied to the three inland points by the same argument that excluded growing areas (§14.1).

**The effective-date case [K1 §4.5].** The JSE has *proposed* moving its reference point from Randfontein to Driefontein from 2027-03-01, and Grain SA is contesting it. Nothing is entered for that today: a proposed date is not a fact (invariant 2). When the JSE confirms, the edit is: set `P-RFT.effective_to = "2027-02-28"`, add a `P-DRF` row with `effective_from = "2027-03-01"` and coordinates from two sources, and add `P-DRF` to `LEG_PLACES["south_africa:safex"]`. No code changes. Both are inland, so no flag changes either — the mechanism is exercised by a synthetic test (§10.2), not by this case.

**Places deferred, with the reason** (§14.2): `P-MOS` (Moselle), `P-PKG`, `P-PGU`, `P-PEN` (FCPO delivery ports). None is priced by a ledger leg.

### 4.2 `config.CYCLONE_BASINS`

Only the basins a registered place declares. Add a basin when a place needs it.

```python
CYCLONE_BASINS = {
    "north_atlantic":    {"layer": "cyclones_nhc",  "source": "NHC",  "id_prefix": "al", "hemisphere": "N"},
    "west_pacific":      {"layer": "cyclones_jtwc", "source": "JTWC", "id_prefix": "wp", "hemisphere": "N"},
    "south_west_indian": {"layer": "cyclones_jtwc", "source": "JTWC", "id_prefix": "sh", "hemisphere": "S"},
    "south_atlantic":    {"layer": None, "source": None, "id_prefix": None, "hemisphere": "S"},
}
```

`south_atlantic` has no source by finding, not by neglect: no regional centre exists, the Brazilian Navy site is behind a Cloudflare challenge, and GDACS, IBTrACS and SWIC all missed Akará (2024) and Caiobá (2026) [R2 §2.10].

The `sh` prefix for JTWC Southern Hemisphere storms was **not observed** by R2 (the basin was out of season on 2026-10-05). It is the standard ATCF basin code. The fetcher hard-fails on a storm id whose prefix is outside `{al, ep, cp, wp, io, sh}`, naming the id, so a surprise is loud rather than a silently unmatched basin.

### 4.3 `config.LEG_PLACES`

Keyed by the same `leg_id` strings as `config.LEDGER_LEGS`.

```python
LEG_PLACES: dict[str, tuple[str, ...]] = {
    "cbot:board":         ("P-CBOT", "P-NOLA"),
    "us_gulf:cif":        ("P-NOLA",),
    "brazil:cepea":       (),
    "brazil:paranagua":   ("P-PNG",),
    "argentina:fob":      ("P-UPR",),
    "dalian:board":       ("P-NCN", "P-PNG", "P-NOLA"),
    "india:mandi_mp":     ("P-IDR",),
    "india:mandi_mh":     (),
    "south_africa:safex": ("P-RFT", "P-DUR"),
}
LEG_PLACES_ABSENT_REASONS: dict[str, str] = {
    "brazil:cepea": "an in-state wholesale indicator, not a port price — it has no port to flag (K1 §3)",
    "india:mandi_mh": "a state-wide mandi median with no stated hub or port (K1 §3)",
}
```

The mapping is [K1 §3], ports only. `cbot:board → P-NOLA` is K1's one *inferred* link ("the board against its own physical — the Gulf basis", from the `LEDGERS["cbot"]` note); [P1] approved it when it approved the Francine result (CBOT board, US Gulf CIF and Dalian No.2 flagged through New Orleans).

The ticket's example of an empty entry was CZCE Rapeseed Oil. CZCE is not a ledger leg, so it has no key here; the two legitimately empty ledger legs are the ones above.

### 4.4 Validation — `app/markets.py`

Add `_validate_leg_places()` and call it from `load_markets()` after `_wire_ledgers(markets)`. Same create-then-wire shape, same hard-fail style, every message naming the offending id. It raises `ValueError` when:

1. `set(config.LEG_PLACES) != set(config.LEDGER_LEGS)` — an unknown leg, or a ledger leg with no entry.
2. A leg names a place id not in `config.PLACES`.
3. A leg's tuple is empty and `LEG_PLACES_ABSENT_REASONS` has no non-blank reason for it; or a leg has both places and a reason.
4. A place in `config.PLACES` is named by no leg. This is the standing rule made structural: a place exists only where a rendered leg needs it.
5. A place breaks a field rule in §4.1, or its coordinates are outside ±90 / ±180.
6. `effective_from > effective_to`.
7. A leg with a non-empty tuple has no place active today.
8. A priced `config.ORIGIN_LEGS` entry (no `absent_reason`) does not resolve to exactly one ledger leg (§7.3).

Add to `LedgerLeg` (the frozen dataclass in `app/markets.py`) a field `place_ids: tuple[str, ...]`, set in `_ledger_leg` from `config.LEG_PLACES[leg_id]`. Builders read `leg.place_ids`; nothing downstream re-reads the config dict.

The market stays a parameter (invariant 5): no builder branches on a slug, and the South Atlantic is a property of a *place's basin*, never of a market.

---

## 5. Geometry and severity

All of this lives in one new module, **`analysis/hazards.py`**: pure functions over DataFrames and config, no SQL, no network. It is the single rule set, in the same role `analysis/weather_alerts.py` plays for weather. `storms.py` on `spike/footprint-weather` is the reference implementation; where this section and that file differ, the difference is stated.

### 5.1 Constants — `config.py`

```python
CYCLONE_LOOKAHEAD_HOURS = 120            # both agencies' horizon
CYCLONE_RADII_KT = (34, 50, 64)          # the agencies' own bands
CYCLONE_WARNING_MAX_TAU_H = 72           # 34/50 kt inside this is `warning`, beyond it `info`
CYCLONE_WATCH_KM = 500                   # centre this close, outside the radii
CYCLONE_WATCH_MIN_KT = 34                # ...and only while at least tropical-storm strength
CYCLONE_ADVISORY_MAX_AGE_HOURS = {"N": 12, "S": 18}   # R2 §3: one missed cycle
CYCLONE_CHECK_MAX_AGE_HOURS = 36         # a daily read, plus half a day of scheduler drift
```

Also in `analysis/hazards.py`, not config: `NM_TO_KM = 1.852`, `EARTH_RADIUS_KM = 6371`.

### 5.2 Hourly track [K2 `hourly`]

For each storm, from its forecast points sorted by `tau_h`:

1. Between each pair of consecutive points `a` and `b`, emit one sample per whole hour `h` from `a.tau_h` up to but not including `b.tau_h`, with `f = (h − a.tau_h) / (b.tau_h − a.tau_h)`.
2. Latitude and wind interpolate linearly. Longitude interpolates along the short way round: `dlon = ((b.lon − a.lon + 540) % 360) − 180`, `lon = a.lon + f · dlon`.
3. Each radius interpolates linearly per band and quadrant. **A NULL radius is treated as 0 nm for interpolation**: a radius the agency did not publish cannot flag anything. The stored value stays NULL.
4. Append the final point as its own sample.
5. Keep samples with `h ≤ CYCLONE_LOOKAHEAD_HOURS`.

Forecast hours count from the advisory's `issued_at`. They are not re-based to the time the page is read.

### 5.3 Place against storm [K2 `assess`]

For each sample, with `d` = haversine distance in km from the storm centre to the place:

- `q` = the quadrant of the initial great-circle bearing from the centre to the place: `int(bearing // 90)`, giving 0 = NE, 1 = SE, 2 = SW, 3 = NW.
- The place is **inside band `k`** at that hour when the interpolated radius `r_k[q] > 0` and `d ≤ r_k[q] · NM_TO_KM`.

Per place and storm, record:

- `first_tau[k]` — the earliest hour inside band `k`, for each of 34, 50, 64;
- `closest_km`, `closest_tau_h` — the minimum of `d` over all samples;
- `closest_ts_km`, `closest_ts_tau_h` — the minimum of `d` over samples where the interpolated wind is ≥ `CYCLONE_WATCH_MIN_KT`.

### 5.4 Severity bands [K2 §7.4, P1 #5]

| Condition, first match wins | State | Severity | `band_kt` |
|---|---|---|---|
| Inside the 64-kt radius at any hour ≤ 120 | `flag` | `alert` | 64 |
| Inside the 34- or 50-kt radius, first arrival ≤ 72 h | `flag` | `warning` | 50 if ever inside 50, else 34 |
| Inside the 34- or 50-kt radius, first arrival > 72 h | `flag` | `info` | 50 if ever inside 50, else 34 |
| Centre within 500 km while ≥ 34 kt, outside every radius | `watch` | `info` | — |
| None of the above | no hit from this storm | | |

- `first_arrival_tau_h` is the earliest hour inside **any** band. For the two middle rows that is what "first arrival" means.
- A place's result is the worst hit across all storms: `alert` > `warning` > `info`-flag > `watch`. Ties go to the earlier `first_arrival_tau_h`, then the smaller `closest_km`.
- NHC publishes no 64-kt radius beyond 72 h, so an `alert` for an NHC storm can only come from the first 72 h. The block-06 caption says so (§8.3) rather than implying the far end is clear.
- Severity uses the shared scale `alert > warning > info`. No new vocabulary.

The spike's `assess` tracked `first_tau_h` with an expression that could carry a stale value across bands. This spec replaces it with the per-band minimum above. The Francine results are identical either way (only the 34-kt band is entered) and the regression fixture pins them.

---

## 6. States and failure modes

### 6.1 The six place states

| State | Meaning | Severity |
|---|---|---|
| `flag` | Inside a forecast wind radius within 120 h | `alert`, `warning` or `info` |
| `watch` | A storm of at least 34 kt passes within 500 km, outside its radii | `info` |
| `clear` | The source was asked, answered, and no storm reaches the place | — |
| `not_covered` | The place's basin has no publishable source. **Never rendered as "no storm".** | — |
| `stale` | The reading exists but is too old to call the place clear | — |
| `failed` | The source could not be read | — |

A place with `basin: "none"` has **no state** (§4.1).

### 6.2 Decision order — `assess_places(...)`

```python
def assess_places(
    places: dict,            # config.PLACES rows active on `today`
    basins: dict,            # config.CYCLONE_BASINS
    status: pd.DataFrame,    # cyclone_source_status
    storms: pd.DataFrame,    # cyclone_storms
    track: pd.DataFrame,     # cyclone_track_points
    layer_states: dict[str, str | None],   # data_freshness.status per layer
    now: datetime,           # UTC
) -> dict[str, PlaceHazard]
```

For each active place with a basin other than `"none"`, the first matching rule sets the state:

1. **`not_covered`** — the basin's `source` is `None`. Reason: the place's `basin_reason`.
2. **`failed`** — the basin's layer has `data_freshness.status` other than `success`, or the source has no `cyclone_source_status` row at all. Reason names the source and the layer's `last_success`.
3. **`stale`** — the source's newest status row has `checked_at` older than `CYCLONE_CHECK_MAX_AGE_HOURS` before `now`. Reason: "last checked {checked_at}".
4. Otherwise, take the **current storms**: every `cyclone_storms` row whose `checked_at` equals its source's newest status row, from **both** sources, with JTWC duplicates dropped (§2.1). Geometry is basin-agnostic: every current storm is tested against every place.
   - a. **`flag`** — any storm hits per §5.4. If that storm's advisory was already older than its hemisphere's `CYCLONE_ADVISORY_MAX_AGE_HOURS` at `checked_at`, the flag still stands and carries `advisory_stale = True`. A known hazard on an old advisory is shown with its age, never hidden.
   - b. **`failed`** — a current storm in the place's own basin (same `id_prefix`) has `track_state = 'absent'`. Reason: "{source} lists {storm_id} but its track product is absent". We know a storm is there and cannot see where it is going.
   - c. **`stale`** — a current storm in the place's own basin has an advisory older than the limit at `checked_at`. Reason: "{name}: advisory {n} h old when read".
   - d. **`watch`** — per §5.4.
   - e. **`clear`** — with the nearest current storm's name, source and `closest_km` when any storm is active anywhere, else "no active storm listed".

`layer_states` is read from `data_freshness` — the same row `app.markets._ingest_status` reads — so the site cannot call a source alive that the pipeline has already failed.

`PlaceHazard` is a frozen dataclass: `place_id, state, severity, band_kt, source, storm_id, storm_name, advisory, issued_at, first_arrival_tau_h, first_arrival_at, closest_km, closest_tau_h, vmax_kt, storm_class_label, advisory_stale, checked_at, reason`. Its `__post_init__` rejects a state outside the six, and rejects `not_covered`, `stale` or `failed` with an empty `reason` — the same enforcement-by-type the block envelope uses.

### 6.3 Leg roll-up — `leg_hazard(place_ids, place_results)`

A leg's exposed places are its `place_ids` that are active today and have a basin other than `"none"`.

| Exposed places | Leg state |
|---|---|
| None (no places, or all `basin: "none"`) | **no hazard** — `leg_hazard` returns `None` |
| Any `flag` | `flag`, with that place's severity (worst first) |
| Else any `watch` | `watch` |
| Else any `failed` | `failed` |
| Else any `stale` | `stale` |
| Else all `not_covered` | `not_covered` |
| Else (≥ 1 `clear`) | `clear` |

`partial = True` when the leg state is `flag`, `watch` or `clear` and at least one exposed place is `not_covered`, `stale` or `failed`. The uncovered places are listed in `uncovered`.

`LegHazard` is a frozen dataclass: `state, severity, partial, primary: PlaceHazard | None, places: tuple[PlaceHazard, ...], uncovered: tuple[str, ...], text`. `text` is the one-line reason used as the chip's tooltip (§8.2).

### 6.4 What each failure looks like to a reader

| Failure | Ledger chip | Block 06 | Briefing |
|---|---|---|---|
| NHC fetch failed | `storm: no reading` on Gulf-priced legs | warm empty-state naming NHC and its last good read | a `NOT ASSESSED` line |
| JTWC fetch failed | same, on legs priced at North China or Durban | same, naming JTWC | same |
| Site built from a DB last filled > 36 h ago | `storm: stale` | "last checked …" | same |
| A storm's advisory is > 12 h (18 h south) old | flagged places keep the flag with the age; others in that basin `storm: stale` | same | same |
| South Atlantic | `storm: not covered` | muted caption | one line, every day |
| Both sources fine, no storm | no chip | explicit green line | one line |

---

## 7. Wiring into the site

### 7.1 `SiteContext` — `app/block_builders.py`

Add two methods beside `leg_prints`:

```python
def place_hazards(self) -> dict[str, PlaceHazard]:
    """Every exposed place graded once for the whole site."""
    return self.cached(("hazards",), lambda: _read_place_hazards(self.conn, self.today))

def leg_hazard(self, leg) -> LegHazard | None:
    return self.cached(("leg_hazard", leg.leg_id),
                       lambda: hazards.leg_hazard(leg.place_ids, self.place_hazards()))
```

`_read_place_hazards` reads the three tables plus the two `data_freshness` rows and calls `hazards.assess_places`. With `conn is None` it returns `{}`, and every leg hazard is then `None` — the same "no database" behaviour every other read has. `now` is the generation time already used for the page stamp, passed in; do not call `datetime.now()` inside the assessor.

### 7.2 `_ledger_row` → `row["hazard"]`

In `_ledger_row` (`app/block_builders.py`), add one key to the row dict, set for every row whether or not it has prints:

```python
"hazard": _hazard_view(ctx.leg_hazard(leg)),
```

`_hazard_view` returns `None` when the leg has no hazard, else:

```python
{
    "state": "flag",              # flag | watch | clear | not_covered | stale | failed
    "severity": "warning",        # alert | warning | info | None
    "partial": False,
    "chip": "storm",              # §8.2; None when nothing renders
    "chip_class": "hz-warning",   # §8.2
    "text": "Francine (NHC): tropical-storm-force winds at New Orleans from Thu 12 Sep 03:00Z",
    "level": "ts_force",          # ts_force | 50kt | hurricane_force | None
    "storm": "Francine",
    "storm_id": "al062024",
    "source": "NHC",
    "first_arrival_h": 24,
    "first_arrival_at": "2024-09-12T03:00:00Z",
    "reason": None,               # set for not_covered / stale / failed
    "uncovered": [],              # short names of not-covered / stale / failed places
    "anchor": leg.href + "#block-weather",        # block 06 of the leg's owning page
}
```

`_headline_placeholder` gets `"hazard": None`.

Because the flag rides the row, every ledger that lists the leg inherits it with no per-page code: `us_gulf:cif` appears on the CBOT, Dalian, Brazil and Argentina ledgers.

Both `ledger_block` and `headline_ledger` return a new flag in their data, **`has_hazard`**: `True` from `ledger_block`, `False` from `headline_ledger`. This is the `has_drilldown` pattern exactly — an explicit flag from the builder, never inferred — and it is how [P1 #2]'s "nothing on the headline in v1" is honoured while the two surfaces keep sharing one template (§14.3).

### 7.3 Origins page

[K2 §7.4] assumed the origins page inherits the flag "via `ORIGIN_LEGS`". It does not: that page is built by `app/origins_page.py` from `analysis/origins`, and never calls `_ledger_row`. `config.ORIGIN_LEGS` entries name a `(market, block, key)` triple, not a ledger `leg_id`. The seam is therefore explicit:

- Add to `app/markets.py`: `ledger_leg_id_for(market: str, block: str, key: str) -> str | None`, which returns the one `config.LEDGER_LEGS` id whose `market`, `block` and `key` all match, or `None`.
- Today's resolution, pinned by test: `us_gulf → us_gulf:cif`, `br_paranagua → brazil:paranagua`, `ar_up_river → argentina:fob`.
- `_validate_leg_places` rule 8 makes a priced origin leg that resolves to nothing a load-time error.
- In `app/origins_page.py`, `_row_view` gains `"hazard"`: resolve the row's origin leg through `ledger_leg_id_for` and look the id up in `hazards`. `build_view` gains a keyword argument `hazards: dict[str, dict] | None = None` — the precomputed `{leg_id: hazard view}` for the three resolved legs, built in `scripts/generate_site.py` from the run's one `SiteContext` so the assessment is computed once. `None` (the default, and what every existing caller and test passes) renders no chips.
- **[K1 edge case (c), P1 #9c]** `us_pnw` declares an `absent_reason` and has no price. It is rendered from `UnavailableOrigin`, never through `_row_view`, and gets **no** hazard key and no chip. A flag needs a price. There is no PNW place in the registry.

`analysis/origins/` is not touched. It cannot import `app/` (the cycle its own module docstring describes), and it does not need to.

### 7.4 Archiving what was published

In `scripts/generate_site.py`, after the market pages render, call `_archive_hazard_flags(ctx)`: one `hazard_flags` row per place in `flag` or `watch`, with `legs` = every ledger leg whose `place_ids` include it. Model it on `_archive_origin_rankings` in the same file: isolated in a `try`, a write failure logs a warning and never fails a render. The deploy workflow's "Export history after briefing" step runs after site generation, so the rows reach `data/history/` without a workflow change.

---

## 8. Rendering

Check every visual change against `DESIGN.md`. [P1] approved three new `DESIGN.md` rows, all on existing palette tokens with no new colours (§8.5).

### 8.1 Timestamps

The page is static and read hours after it is built. So:

- Every time shown is an **absolute UTC time** (`Wed 11 Sep 15:00Z`), computed as `issued_at + tau`. Never "in 24 h" on its own.
- The storms head always carries the check time: `checked 19:04Z 6 Oct`.
- A lead time may follow in brackets, anchored to the advisory: `(+24 h from the 03:00Z advisory)`.

The prototype showed bare `+24 h` because it replayed one advisory "as if today"; on a daily static page that would be wrong by the time it is read.

### 8.2 The State-column chip

In `app/templates/blocks/_ledger_table.html.j2`, inside the State cell, after the existing pill and its `state_detail` caption:

```jinja
{%- if d.has_hazard and row.hazard and row.hazard.chip %}
<div><a class="hz-chip {{ row.hazard.chip_class }}" href="{{ root }}{{ row.hazard.anchor }}" title="{{ row.hazard.text }}">{{ row.hazard.chip }}</a></div>
{%- endif %}
```

| Leg state | `partial` | `chip` | `chip_class` | Looks like |
|---|---|---|---|---|
| `flag`, `alert` | either | `storm` | `hz-alert` | solid `--bearish`, white text (the signal row's alert badge) |
| `flag`, `warning` | either | `storm` | `hz-warning` | `#E8C983` fill, `#4A3608` text (the warning badge) |
| `flag`, `info` | either | `storm: 3–5 days` | `hz-quiet` | `#E4E8E4` fill, `--text-muted` |
| `watch` | either | `storm watch` | `hz-quiet` | same |
| `failed` | — | `storm: no reading` | `hz-quiet` | same |
| `stale` | — | `storm: stale` | `hz-quiet` | same |
| `not_covered` | — | `storm: not covered` | `hz-quiet` | same |
| `clear` | `True` | `storm: part covered` | `hz-quiet` | same |
| `clear` | `False` | *(none)* | — | **nothing renders** [P1 #3] |

- The chip is silent only when the leg is fully clear. `watch`, `stale` and `failed` get a chip because silence there would read as clear (invariant 1).
- The tooltip (`text`) is one sentence. When `partial`, it ends with the uncovered places: `… · Paranaguá not covered`.
- The chip links to block 06 of the leg's **owning** page, where the full sentence lives. On a market page whose ledger lists the leg, block 06 on that same page carries the port too (§8.3). The block id is `weather` (`app/blocks.py`), so the fragment is `#block-weather`. If the owning page's tier renders no weather block, the link lands at the top of that page; that is acceptable and needs no special case.
- CSS: add `.hz-chip` and its three modifiers to `app/templates/_base.html.j2` beside `.pill` (the origins page renders it too — the one-`<head>`-owner rule). Shape copies `.pill`: display font, 10px, 700, uppercase, `letter-spacing: 0.06em`, `padding: 2px 8px`, `border-radius: 3px`, `white-space: nowrap`, `margin-top: 4px`, `text-decoration: none`.
- Origins page: in `app/templates/origins.html.j2`, the ranked table's State cell gets the same chip after the comparability pill. No gate flag there; a row with `hazard` `None` renders nothing.

### 8.3 Block 06 — "Storms — the port leg"

`weather_block` (`app/block_builders.py`) gains a `storms` key in its data, built by a new `_storm_rows(market, ctx)` beside `_river_rows`. The template `app/templates/blocks/06_weather.html.j2` renders it **after** the river section.

**Which places a page lists.** The exposed and unexposed places of **every leg in that market's ledger**, in declared leg order, each place once. So a chip on any row of the page's ledger has its explanation on the same page. A market with no ledger (Europe, Nigeria) has no storms section at all — a fact about the registry, like a market with no river gauge.

| Page | Port rows | "Not exposed" caption |
|---|---|---|
| CBOT | New Orleans, Paranaguá, Rosario, North China | Burns Harbor |
| Dalian | North China, Paranaguá, New Orleans | Burns Harbor |
| Brazil | Paranaguá, New Orleans, Rosario | Burns Harbor |
| Argentina | Rosario, Paranaguá, New Orleans | Burns Harbor |
| South Africa | Durban, Rosario, Paranaguá, New Orleans | Randfontein, Burns Harbor |
| India | *(none)* | Indore |

This table is derived from `config.LEDGERS` and `LEG_PLACES` as they stand; the code derives it, never hard-codes it.

`weather_block` currently returns `STATE_EMPTY` when it has no regions and no river reading. Extend that test: the block is `ok` when it has regions, **or** a river reading, **or** any storm row.

**Markup**, reusing the river section's classes (no new component):

```jinja
{% if d.storms %}
<div class="river">
  <div class="river-head">Storms — the port leg · NHC + JTWC · checked {{ d.storms.checked_label }}</div>
  {% for p in d.storms.ports %}
  <div class="river-row">
    <div class="lbl">{{ p.short }}
      {%- if p.port_rain %}<div class="caption">port rain {{ p.port_rain.tp15_mm }} mm over 15 days · {{ p.port_rain.wet_days }} days ≥ 5 mm (loading delays) · {{ p.port_rain.stamp }}</div>{% endif %}
    </div>
    <div class="val">{# one of the cells below #}</div>
  </div>
  {% endfor %}
</div>
{# then the lines below, in this order #}
{% endif %}
```

`checked_label` is the older of the two sources' `checked_at`, formatted `19:04Z 6 Oct`. If a source has never answered, the head reads `checked — see below`.

The value cell per place state:

| State | Cell |
|---|---|
| `flag` | the same chip as the ledger (`storm`, with its class) |
| `watch` | `storm watch` chip, `hz-quiet` |
| `clear` | `<span class="muted">no storm ≤ 120 h</span>` |
| `not_covered` | `<span class="muted">not covered</span>` |
| `stale` | `<span class="muted">stale</span>` |
| `failed` | `<span class="muted">no reading</span>` |

Lines under the rows, in this order:

1. **One per flagged place**, worst first. Class by severity: `alert alert-err` for `alert`, `alert alert-warn` for `warning`, `alert alert-info` for `info`. Text: the flag sentence (§8.4).
2. **One per `watch` place**: `alert alert-info`, the watch sentence.
3. **One per `failed` or `stale` place**: `<div class="empty-state state-empty"><span class="es-label">{{ 'no reading' if failed else 'stale' }}</span>{{ short }}: {{ reason }}</div>` — the warm empty state, because a source exists and gave us nothing.
4. **The explicit no-storm line**, only when no place on the page is `flag` or `watch` and at least one is `clear`:
   `<div class="alert alert-ok">No active storm threatens {{ clear ports, comma-joined }}</div>`
   It names only the `clear` ports. It never renders when no port is `clear`: "no storm threatens a covered port" on a page with no covered port would be an empty reassurance.
5. **The not-covered caption**, one per `not_covered` place:
   `<div class="caption">{{ short }}: {{ basin_reason }} — absence of a flag is not a clear reading</div>`
6. **The not-exposed caption**, one line when the page has any `basin: "none"` place:
   `<div class="caption">Not exposed, so not assessed: {{ short }} ({{ basin_reason }}); …</div>`
7. **The method caption**, always:
   `<div class="caption">Forecast wind radii (34, 50, 64 kt) out to 120 h, from NOAA National Hurricane Center and the Joint Typhoon Warning Center. JTWC is US military guidance, not a national warning. NHC forecasts hurricane-force radii to 72 h only. Ports and pricing points only — storm rain on a growing area is in its rainfall forecast.</div>`

**Port rain [P1 #7].** The caption on a port row is supplied by footprint weather (S1), which owns the ECMWF fetch; a port box is "one more `(points, weights)` set" there [K2 §5]. This build ships the slot: `_storm_rows` calls `ctx.port_rain(place_id)`, which returns `None` until S1 lands, and the row renders without the caption. `None` is correct, not a blank to fill: there is no second observation yet. The contract S1 fills is `{"tp15_mm": int, "wet_days": int, "stamp": "ECMWF 00z 5 Oct"}`, keyed by the place ids in §4.1. Which ports get a box is S1's call.

### 8.4 Sentences

Built in `analysis/hazards.py` so the site and the briefing say the same thing.

**Storm label**: `{Name} ({source} adv {n}, {class}, {vmax} kt)`. Drop `adv {n}` when the advisory is NULL (JTWC).

**Class**, from the agency's own vocabulary and never a national scale:

| Source | Rule | Label |
|---|---|---|
| NHC | `HU` | `Hurricane, category {1–5}` by Saffir-Simpson: 64–82, 83–95, 96–112, 113–136, ≥ 137 kt [R2 §2.1] |
| NHC | `TS` / `TD` / `STS` / `STD` / `PTC` / `PC` | Tropical Storm / Tropical Depression / Subtropical Storm / Subtropical Depression / Potential Tropical Cyclone / Post-tropical Cyclone |
| JTWC, `wp` | by wind | < 34 Tropical Depression · 34–63 Tropical Storm · 64–129 Typhoon · ≥ 130 Super Typhoon [R2 §2.2] |
| JTWC, any other prefix | — | Tropical Cyclone |

An unknown NHC classification code is rendered as the raw code and logged at warning; it is not a failure.

**Band words**: 34 → `tropical-storm-force winds`; 50 → `50-kt winds`; 64 → `hurricane-force winds` (NHC), `typhoon-force winds` (JTWC `wp`), `64-kt winds` (JTWC otherwise).

**Flag sentence**:
`{short}: {storm label} — {band words} forecast from {first_arrival_at} (+{h} h from the {issued HH:MM}Z advisory); closest approach {closest_km} km at {closest_at}. {source line}`

- `info` severity appends ` Three to five days out.`
- `advisory_stale` appends ` Advisory was {n} h old when read.`
- Source line: `NHC forecast.` or `JTWC guidance, not a national warning.`

**Watch sentence**:
`{short}: {storm label} forecast to pass within {closest_ts_km} km at {closest_ts_at}, outside its forecast wind radii. {source line}`

**Chip tooltip** (`LegHazard.text`), shorter: `{Name} ({source}): {band words} at {short} from {first_arrival_at}`.

### 8.5 The three `DESIGN.md` rows

[P1] approved three additions. Row 3 belongs to this build and lands with it. Rows 1 and 2 belong to the footprint-weather build (S1); they are written out here so both specs quote one text, and **this build does not add them**.

Add under "Component Patterns":

> **### Hazard chip (ledger State column, origins State column, block 06 storm rows)**
> A second chip under the ledger's state pill, present only when a leg's cyclone hazard is anything other than fully clear — silence is the clear reading, and a watch, a stale read or a failed read is never silent. Same shape as `.pill`. Three fills, all existing tokens: solid `--bearish` with white text for a hurricane-force flag, the warning badge (`#E8C983` / `#4A3608`) for a tropical-storm-force flag inside 72 h, and the neutral badge (`#E4E8E4` / muted) for everything else — a far-out flag, a watch, part covered, not covered, stale, no reading. The chip is a link to block 06 of the leg's own page and carries the one-line reason as its title. Never a page banner. Rendered on market-page ledgers and the origins board, not on the headline ledger.

Add to the Decisions Log (the build fills in its own PR number):

> | 2026-10-06 | **Cyclone hazard flags: a State-column chip and a "Storms — the port leg" section in block 06** | [P1 #362](https://github.com/philipbergman6-glitch/Mirror-Market/issues/362), spec [S2 #374](https://github.com/philipbergman6-glitch/Mirror-Market/issues/374). Variant A of three prototyped. The storms section reuses the river section's rows inside block 06 rather than a tenth block, for the river's own reason: the nine block ids are the contract. A flagged port gets an alert line in its severity's existing class; when the covered ports are clear the block says so in an explicit `alert-ok` line, because an empty section would read as "not checked"; South Atlantic ports get a muted caption — "absence of a flag is not a clear reading" — because no publishable source exists there and `not covered` must never read as `no storm`. No new colours. |

For S1, verbatim from [P1]:

> **Row 1 — the tercile track**: a sibling of the ledger's range track.
> **Row 2 — the forecast alert line**: a dashed left border plus a `forecast` kind chip.

---

## 9. Briefing wording

A new section, `analysis/briefing/sections/storms.py`, with `format() -> str`, wired into `analysis/briefing/orchestrator.py`: add `"storms": storms.format()` to `section_texts` and `"storms"` to `_SECTION_ORDER` **immediately after `"weather"`**. It is not added to `_SKIP_WHEN_EMPTY`: the section always prints its header.

It reads through `pipeline.query` and calls the same `analysis.hazards.assess_places` and sentence builders as the site, so the two cannot disagree. Leg labels come from `config.LEDGER_LEGS[leg_id]["label"]`.

Line order and exact wording:

```
STORMS (NHC + JTWC, checked 19:04Z 06 Oct):
  New Orleans: Francine (NHC adv 10, Hurricane, category 1, 80 kt) — tropical-storm-force winds forecast from Thu 12 Sep 03:00Z (+24 h from the 03:00Z advisory); closest approach 107 km at Thu 12 Sep 06:00Z. NHC forecast.
    Legs: CBOT board (ZS front), US Gulf CIF (NOLA barge), Dalian No.2 (crush bean)
  No active storm threatens: North China (Qingdao), Durban
  Not covered: Paranaguá, Rosario (up-river) — South Atlantic, no publishable cyclone source. Absence of a flag is not a clear reading.
  Active storms: Rachel (NHC, 75 kt), Nolo (JTWC, 105 kt), Choi-wan (JTWC, 110 kt), Koguma (JTWC, 35 kt). Nearest to a covered port: Choi-wan, 2,488 km from North China (Qingdao).
```

Rules:

1. One line per `flag` place, worst first, then one per `watch` place, each followed by an indented `Legs:` line.
2. `No active storm threatens: …` lists the `clear` places. Omitted when none is clear.
3. `NOT ASSESSED: {short} — {reason}` — one line per `failed` or `stale` place. Upper case because it is a statement about us, in the manner of the weather section's "not assessed" lines.
4. `Not covered: …` — one line, every day, while any place is `not_covered`.
5. `Active storms: …` — current storms after de-duplication, then the nearest approach to any covered port. When none: `Active storms: none listed.`
6. With no database rows at all: `STORMS: No data`.

The example shows format, not one real day. The Francine line uses advisory 010's lead times from the regression fixture (+24 h, 107 km at +27 h); its clock times assume the advisory was issued 2024-09-11 03:00Z (six-hourly from advisory 008 at 2024-09-10 15Z — an inference; the fixture's own issue time governs), and its class and wind are placeholders. The last three lines use the 2026-10-05 spike snapshot.

Hazard flags are **not** added to the briefing's `signals` section and not to `market_drivers`.

---

## 10. Tests

Every test is offline. `tests/_guards.py` blocks sockets and writes under `data/history/`; point `pipeline.history.HISTORY_DIR` at `tmp_path` where history is exercised.

### 10.1 Fixtures — `tests/fixtures/cyclones/`

**Francine, the regression fixture.** Hurricane Francine 2024, NHC storm `al062024`, advisories 008–012 (2024-09-10 15Z → 2024-09-11 15Z). Ten zips from the NHC archive:

```
https://www.nhc.noaa.gov/gis/forecast/archive/al062024_5day_{008..012}.zip
https://www.nhc.noaa.gov/gis/forecast/archive/al062024_fcst_{008..012}.zip
```

The spike read them from local copies that were **not** committed to `spike/footprint-weather`; the build downloads them once and commits them (public domain, a few KB each). The spike's results are on that branch as `spike/footprint-weather/results/storms_replay_{008..012}.json`.

Expected for `P-NOLA` (29.9369, −90.0619), from those JSON files:

| Advisory | State | Band | Severity | First 34-kt arrival | Closest approach | at |
|---|---|---|---|---|---|---|
| 008 | `flag` | 34 | `warning` | +33 h | 112 km | +38 h |
| 009 | `flag` | 34 | `warning` | +28 h | 104 km | +33 h |
| 010 | `flag` | 34 | `warning` | +24 h | 107 km | +27 h |
| 011 | `flag` | 34 | `warning` | +21 h | 99 km | +23 h |
| 012 | `flag` | 34 | `warning` | +11 h | 106 km | +16 h |

If the build's parse of the committed zips does not reproduce this table exactly, stop and reconcile against `storms.py` before changing an expectation.

**A JTWC `.tcw` and `jtwc.rss`.** The spike saved no raw JTWC payload. The build captures one live `.tcw` and the RSS that listed it (public information) and commits both. If no JTWC storm is active on the build day, write the fixture by hand from the line format in [R2 §2.2] (`T012 300N 1461E 100 R064 060 NE QD …`) and the regexes in `storms.py` `parse_jtwc`, and name the file `synthetic_*.tcw`.

**A `CurrentStorms.json`** — one captured live (or hand-written to the live shape in [R2 §2.1] if none is active), and one with `"activeStorms": []`.

### 10.2 Test list

`tests/test_fetcher_cyclones.py`

| Test | Asserts |
|---|---|
| `test_nhc_track_uses_geometry_not_the_rounded_dbf_columns` | Francine 010: parsed positions equal the shape geometry, and at least one differs from the DBF `LAT`/`LON` attribute by more than 0.01°. The whole-degree trap [K2 §8]. |
| `test_nhc_radii_join_by_tau` | 010: the 34-kt quadrant radii land on the matching track point. |
| `test_nhc_64kt_radius_beyond_72h_is_null_not_zero` | Invariant 2. |
| `test_nhc_intensity_string_is_parsed` | `"80"` → `80`; a non-numeric value raises [R2 §2.1]. |
| `test_nhc_empty_active_storms_is_a_successful_answer` | Returns a `status` row with `storms_listed == 0` and empty `storms` / `track`. |
| `test_nhc_advisory_mismatch_between_index_and_zip_raises` | |
| `test_jtwc_tcw_parses_track_and_radii` | |
| `test_jtwc_tcw_with_no_track_lines_raises` | |
| `test_jtwc_403_on_a_listed_product_is_absent_not_failed` | `track_state == 'absent'`, `products_absent == 1`, no exception [R2 §2.2]. |
| `test_jtwc_rss_with_no_products_is_a_successful_answer` | |
| `test_unknown_basin_prefix_raises` | |
| `test_track_is_window_replaced_and_cleared_on_an_empty_answer` | Save a storm, then save an empty `track` for the same source: the table holds no rows for it. |
| `test_layers_are_registered` | Both keys in `PRODUCTION_LAYERS`, `_build_dict_layers` and `latency`; neither in `FAST_REFRESH_LAYERS`. |
| `test_zero_storm_run_stamps_last_success` | Through `_finalize_layer`. |
| `test_cyclone_tables_round_trip_through_git_history` | With `HISTORY_DIR` at `tmp_path`; `cyclone_track_points` is absent from `HISTORY_TABLES`. |

`tests/test_hazards.py`

| Test | Asserts |
|---|---|
| `test_francine_replay_flags_new_orleans` | Parametrised over 008–012: the table in §10.1. **The regression fixture.** |
| `test_francine_remnant_does_not_put_illinois_on_watch` | A synthetic exposed place at 40.12, −89.30 is `clear` on all five advisories. Pins the ≥ 34-kt watch gate [K2 §6 defect 1]. |
| `test_francine_legs` | With the real registry: `cbot:board`, `us_gulf:cif`, `dalian:board` are `flag`; `dalian:board` is `partial` with Paranaguá uncovered; `brazil:paranagua` and `argentina:fob` are `not_covered`; `south_africa:safex` is `clear`; `brazil:cepea`, `india:mandi_mp`, `india:mandi_mh` have no hazard. |
| `test_dedupe_is_by_basin_number_and_year_not_prefix` | NHC lists `ep182026`; JTWC lists `ep1526`, `ep1826`, `wp2626`, `wp2726`. Only `ep1826` is dropped [K2 §6 defect 2]. |
| `test_no_dedupe_when_nhc_layer_failed` | |
| `test_severity_bands` | Synthetic tracks: inside 64 kt → `alert`; 34 kt at +60 h → `warning`; 34 kt at +90 h → `info`; centre 400 km at 40 kt, outside radii → `watch`; centre 400 km at 30 kt → `clear`. |
| `test_interpolation_crosses_the_antimeridian` | A track from 179°E to 179°W passes a place at 180°. |
| `test_quadrants` | A place due north-east of the centre reads the NE radius; a 100-nm NE radius and a 0 SW radius flag only the NE place. |
| `test_south_atlantic_is_not_covered_never_clear` | With zero storms active. |
| `test_failed_layer_makes_its_basin_failed_only` | NHC failed → `P-NOLA` `failed`; `P-NCN`, `P-DUR` unaffected. |
| `test_old_check_is_stale` | `checked_at` 37 h before `now`. |
| `test_stale_advisory_keeps_a_flag_and_ages_it` | `advisory_stale is True`, state still `flag`. |
| `test_stale_advisory_makes_an_unflagged_place_in_its_basin_stale` | 13 h for `al`/`wp`, 19 h for `sh`; 17 h for `sh` is not stale. |
| `test_absent_track_makes_its_basin_failed` | |
| `test_inland_places_are_never_assessed` | `P-CBOT`, `P-RFT`, `P-IDR` are absent from `assess_places` output, even with a synthetic storm directly over them. |
| `test_effective_dates_select_the_active_place` | Synthetic registry: `X-OLD` to 2027-02-28, `X-NEW` from 2027-03-01. On 2027-02-28 only `X-OLD` is assessed; on 2027-03-01 only `X-NEW`. |
| `test_state_without_a_reason_is_rejected` | `PlaceHazard(state="not_covered", reason="")` raises. |

`tests/test_leg_places.py`

| Test | Asserts |
|---|---|
| `test_every_ledger_leg_declares_its_places` | **The completeness test.** `set(LEG_PLACES) == set(LEDGER_LEGS)`. |
| `test_an_empty_leg_needs_a_reason` | Monkeypatched config: an empty tuple with no reason raises from `load_markets()`. |
| `test_unknown_leg_or_place_raises` | Both directions. |
| `test_every_place_is_used_by_a_leg` | |
| `test_place_basin_rules` | `none` and sourceless basins need a reason; others must not carry one. |
| `test_origin_legs_resolve_to_ledger_legs` | The three mappings in §7.3. |
| `test_the_pnw_row_gets_no_hazard` | **K1 edge case (c).** `us_pnw` resolves to no ledger leg, has no place, and the rendered origins page carries no hazard chip in its row. |

`tests/test_propagation_ledger.py` (extend)

| Test | Asserts |
|---|---|
| `test_ledger_row_carries_hazard` | With Francine 010 stubbed into `ctx`: `row["hazard"]["state"] == "flag"` on `us_gulf:cif`, on each of the four ledgers that list it. |
| `test_clear_leg_renders_no_chip` | No `hz-chip` in the row's HTML. |
| `test_not_covered_leg_renders_the_chip` | `storm: not covered` on `brazil:paranagua`. |
| `test_headline_ledger_renders_no_hazard_chip` | `has_hazard is False`; no `hz-chip` in the section HTML even with a flag active. |

Block 06 (`tests/test_market_blocks*.py`, wherever `weather_block` is tested today)

| Test | Asserts |
|---|---|
| `test_storm_rows_follow_the_ledger` | The page → ports table in §8.3, derived. |
| `test_flagged_port_renders_an_alert_line` | Francine 010 on the CBOT page: `alert-warn`, the flag sentence. |
| `test_explicit_no_storm_line` | No storms: CBOT page carries `No active storm threatens New Orleans, North China (Qingdao)`. |
| `test_no_storm_line_is_withheld_when_no_port_is_clear` | A synthetic ledger whose only exposed ports are South Atlantic (no real page is in that position today). |
| `test_not_covered_caption` | Contains `absence of a flag is not a clear reading`. |
| `test_not_exposed_caption` | India page: Indore, with its reason. |
| `test_port_rain_slot_is_empty_until_footprint_weather_lands` | Row renders, no `port rain` text. |
| `test_market_without_a_ledger_has_no_storms_section` | Europe. |
| `test_times_are_absolute_utc` | The alert line contains `Z` and no bare relative time. |

Briefing: `tests/test_briefing_storms.py` — the six line rules in §9, and a parity test that the flag sentence in the briefing equals the one in block 06 for the same fixture (the `tests/test_weather_alert_parity.py` idea).

Docs: `tests/test_docs_claims.py` — update the layer count.

---

## 11. CI cost

| Item | Cost | Source |
|---|---|---|
| Fetch and parse, both sources | **3.7–4.2 s**, measured with 5 storms live and 36 places | [K2 §7.5] |
| Assessment | Negligible: 5 exposed places × ≤ 121 hourly samples × a handful of storms | derived |
| Download size | A few KB per NHC storm (two zips), a few KB per JTWC storm, two small index files | [R2 §2.1–2.2] |
| New dependency | `pyshp`, pure Python, no compiled wheel | — |
| Where it runs | The full daily run only (the 19:00 UTC cron in `deploy-dashboard.yml`). Not in `FAST_REFRESH_LAYERS`. | [K2 §0] |
| History growth | 2 status rows/run (≈ 520/yr) + 1 row per live storm per run (5 on 2026-10-05) + 1 per flagged place. A few thousand rows a year. | [K2 §7.5] |
| Job timeout | 30 min (`deploy-dashboard.yml`); this adds seconds | — |

**Unmeasured:** whether GitHub-hosted runners can reach `www.metoc.navy.mil`. R2 probed from a home connection; the JTWC HTML pages answered 403 to a short User-Agent and the products answered 200 either way. Use the project's browser-style User-Agent convention (`LAYERS.md` → "API keys" → the User-Agent trap). If the first CI run shows JTWC unreachable, the layer reads `failed`, every JTWC-basin place reads `failed` on the site, and the 20:40 retry fires once a day. That is the honest state, but it is also a full pipeline re-run every weekday — so slice 1 ends with a check of the first deploy run, and a persistent block is escalated to the owner rather than left running.

---

## 12. Traps

For the `LAYERS.md` entries. Each one burned the spike or the research.

1. **NHC DBF coordinates are whole degrees.** The `LAT` / `LON` attribute columns in the 5-day points shapefile are rounded. Read the position from the shape geometry (`shape.points[0]`, which is `(lon, lat)`). The ArcGIS MapServer attributes have the same defect. [K2 §8, R2 §2.1]
2. **The NHC JSON has drifted from its reference PDF.** Live keys are camelCase (`latitudeNumeric`), and `intensity` is a string. Parse defensively; hard-fail on a shape change. [R2 §2.1]
3. **JTWC answers 403, not 404, for a product that does not exist.** Resolve product names from `jtwc.rss`; treat a 403 on a listed product as "absent", a distinct state. The RSS `<link>` values point at an S3 host that itself answers 403 — match the `www.metoc.navy.mil/jtwc/products/{id}.tcw` URLs in the feed text, as `storms.py` does. [R2 §2.2]
4. **Never de-duplicate by the `ep` prefix.** §2.1. [K2 §6]
5. **A centre-distance watch with no wind gate flags inland remnants.** §5.4's ≥ 34-kt condition is load-bearing. [K2 §6]
6. **NHC forecasts 64-kt radii to 72 h only.** A missing radius is NULL, not zero, and the page says the far end is not forecast. [R2 §2.1]
7. **The `_latest` zip is an alias.** Assert the advisory number inside the zip equals the index's `forecastTrack.advNum`, or a half-published advisory pairs a new index with an old track. The DBF field is expected to be `ADVISNUM`; the spike did not read it, so confirm the name against the Francine fixture's field list.
8. **Radii are valid only over water.** That is why inland places are not assessed at all. [R2 §2.2]
9. **Do not use NOAA's `tgftp` bulletin mirror.** It keeps years-old files at HTTP 200. Not a source here; noted so nobody adds it as a fallback without an age check. [R2 §2.11]
10. **One storm id, two formats.** NHC `al062024` (four-digit year), JTWC `wp2626` (two-digit). Normalise to a four-digit `season` before comparing.

---

## 13. Build slices, in order

Each slice is one PR that leaves `main` green and the site unchanged until slice 4. Never edit `data/history/` in a PR; the deploy workflow writes it.

| # | Slice | Contains | Done when |
|---|---|---|---|
| 1 | **Layers** | `fetchers/cyclones.py`, schema, clean, store, query, history entries, `main.py` wiring, latency entries, `pyshp`, fixtures, `tests/test_fetcher_cyclones.py`, `LAYERS.md` entries, layer-count updates | Tests pass; the first deploy run after merge shows both layers `success` and rows in the two history CSVs. Check JTWC reachability from the runner (§11). |
| 2 | **Registry** | `config.PLACES`, `CYCLONE_BASINS`, `LEG_PLACES`, `LEG_PLACES_ABSENT_REASONS`, the §5.1 constants, `_validate_leg_places`, `LedgerLeg.place_ids`, `ledger_leg_id_for`, `tests/test_leg_places.py` | `load_markets()` validates; nothing renders differently. Independent of slice 1 — may be built in parallel. |
| 3 | **Assessment** | `analysis/hazards.py`, `tests/test_hazards.py` with the Francine regression | Francine table reproduced exactly. Needs 1 and 2. |
| 4 | **Site** | `SiteContext` methods, `row["hazard"]`, `has_hazard`, the chip, block 06 storms section, origins chip, `hazard_flags` table and archive step, CSS, the `DESIGN.md` row and log entry, `ARCHITECTURE.md` | Render tests pass; `scripts/generate_site.py --only cbot` and `--only origins` reviewed against `DESIGN.md`; `scripts/smoke_site.py` passes. Needs 3. |
| 5 | **Briefing** | `sections/storms.py`, orchestrator wiring, tests | `python -m analysis.briefing` prints the section. Needs 3; may be built in parallel with 4. |

The port-rain caption is not a slice here. It lights up when footprint weather (S1) implements `ctx.port_rain`.

---

## 14. Decisions made in this spec

Each was left open upstream or forced by the code. All are reversible in the place named.

1. **Three inland pricing points are declared not exposed** (`P-CBOT`, `P-RFT`, `P-IDR`; `basin: "none"`). [K2] said inland places "far from any basin" declare `none` with a reason but did not say which. The spike assessed them like ports. The argument that removed growing areas — radii are valid only over water — applies equally to a pricing point hundreds of kilometres inland. *Reverse by* giving a place a basin in `config.PLACES`.
2. **Eight places, not K1's twelve.** `P-MOS` and the three FCPO ports are deferred: no ledger leg prices at them, [P1 #2] put nothing on the headline or the competing-oil strip, and the hazard attaches through the ledger row. K1 itself rates the FCPO ports "little to no cyclone exposure", and the Moselle is inland. *Reverse by* adding a second map for non-ledger price keys when a surface exists for them.
3. **No chip on the headline ledger.** [K2] said "every ledger that lists the leg inherits it"; [P1 #2] said "nothing on the headline in v1". The headline's rows are markets, not legs, so a leg's port flag would be attributed to a whole market. `has_hazard = False` there. *Reverse by* flipping one flag in `headline_ledger`.
4. **`brazil:cepea` gets no chip.** The prototype showed `storm: not covered` on it, because the spike's leg list still included growing areas. Under the approved ports-only rule CEPEA has no port. `dalian:board`'s uncovered list likewise drops Mato Grosso and keeps Paranaguá.
5. **Block 06 lists the ports of every leg in the page's ledger**, not only the page's own. The prototype's two pages used hand-picked lists that follow no single rule. This rule keeps every chip's explanation on the page that shows the chip.
6. **Chips for `watch`, `stale` and `failed`.** [P1 #3] named `flag`, `not_covered` and part-covered, and said "nothing when clear". The other three are not clear, and silence would read as clear.
7. **Three chip fills by severity.** The prototype drew one amber `storm` chip. A hurricane-force flag uses the existing alert badge and a 3–5-day flag the neutral one, so the chip follows the approved severity bands. All existing tokens.
8. **Absolute UTC times**, not the prototype's bare `+24 h` (§8.1).
9. **The assessment runs at render time**, from stored tracks, in one module shared by the site and the briefing. Only what was published is archived.
10. **Two layers rather than one**, on the Layer 27/28 precedent.
11. **A flag on a stale advisory stays a flag**, with its age. A listed storm whose track is unreadable makes its basin `failed`.
12. **The origins page gets an explicit seam** (`ledger_leg_id_for`), because [K2]'s assumed inheritance does not exist in the code.
13. **No hazard chip in block 01.** [K2] said non-ledger price keys "keep the block-01 chip path". No non-ledger key on a market page prices at an exposed, covered port, so that path would only ever print `not covered`. Read as: block 01 is unchanged.

## 15. Open after this spec

Nothing blocks the build. Three items are deliberately left for evidence:

- **JTWC reachability from GitHub runners** — measured by slice 1's first deploy run.
- **Grading `index_last_modified`** — after 30 daily runs, decide whether a frozen empty index can be detected (§3.5).
- **The `sh` id prefix** — confirmed when the first Southern Hemisphere storm of the season is read; a wrong guess fails loudly.

## Attribution

Storm forecasts: NOAA National Hurricane Center; Joint Typhoon Warning Center (US government public information). JTWC products are US military guidance and are not the official warning of any national meteorological service. Nothing derived here is presented as an official government product.
