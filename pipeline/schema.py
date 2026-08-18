"""
SQL schema strings for Mirror Market database tables.

Contains all CREATE TABLE IF NOT EXISTS statements used by pipeline/store.py.
Each string is a named constant — no imports, no functions, just the SQL.
"""

# SQL schemas — explicit tables with PRIMARY KEY constraints
# ---------------------------------------------------------------------------
_CREATE_PRICES = """
CREATE TABLE IF NOT EXISTS prices (
    commodity   TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    Open        REAL,
    High        REAL,
    Low         REAL,
    Close       REAL,
    Volume      REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_ECONOMIC = """
CREATE TABLE IF NOT EXISTS economic (
    series_name TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    value       REAL,
    PRIMARY KEY (series_name, Date)
);
"""

# reference_period_desc is part of the key: monthly series (NASS crush)
# publish one row per month under the same short_desc/year, and the old
# 3-column key collapsed them to a single surviving month.
_CREATE_USDA = """
CREATE TABLE IF NOT EXISTS usda (
    stat_category           TEXT,
    year                    TEXT,
    short_desc              TEXT,
    Value                   TEXT,
    unit_desc               TEXT,
    state_name              TEXT,
    reference_period_desc   TEXT,
    PRIMARY KEY (stat_category, year, short_desc, reference_period_desc)
);
"""

_CREATE_COT = """
CREATE TABLE IF NOT EXISTS cot (
    commodity           TEXT    NOT NULL,
    Date                TEXT    NOT NULL,
    commercial_long     REAL,
    commercial_short    REAL,
    commercial_net      REAL,
    noncommercial_long  REAL,
    noncommercial_short REAL,
    noncommercial_net   REAL,
    total_open_interest REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_WEATHER = """
CREATE TABLE IF NOT EXISTS weather (
    region          TEXT    NOT NULL,
    Date            TEXT    NOT NULL,
    temp_max        REAL,
    temp_min        REAL,
    precipitation   REAL,
    is_forecast     INTEGER,
    PRIMARY KEY (region, Date)
);
"""

_CREATE_PSD = """
CREATE TABLE IF NOT EXISTS psd (
    commodity   TEXT    NOT NULL,
    country     TEXT    NOT NULL,
    year        INTEGER NOT NULL,
    attribute   TEXT    NOT NULL,
    value       REAL,
    unit        TEXT,
    PRIMARY KEY (commodity, country, year, attribute)
);
"""

_CREATE_CURRENCIES = """
CREATE TABLE IF NOT EXISTS currencies (
    pair    TEXT    NOT NULL,
    Date    TEXT    NOT NULL,
    Open    REAL,
    High    REAL,
    Low     REAL,
    Close   REAL,
    PRIMARY KEY (pair, Date)
);
"""

_CREATE_WORLDBANK = """
CREATE TABLE IF NOT EXISTS worldbank_prices (
    commodity   TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    price       REAL,
    unit        TEXT,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_EC_OILSEED_PRICES = """
CREATE TABLE IF NOT EXISTS ec_oilseed_prices (
    series      TEXT    NOT NULL,
    Date        TEXT    NOT NULL,
    price_usd   REAL,
    price_eur   REAL,
    cadence     TEXT,
    quote_kind  TEXT,
    PRIMARY KEY (series, Date)
);
"""

_CREATE_DCE_FUTURES = """
CREATE TABLE IF NOT EXISTS dce_futures (
    commodity       TEXT NOT NULL,
    Date            TEXT NOT NULL,
    Open            REAL,
    High            REAL,
    Low             REAL,
    Close           REAL,
    Volume          REAL,
    Open_Interest   REAL,
    Settle          REAL,
    PRIMARY KEY (commodity, Date)
);
"""

_CREATE_CROP_PROGRESS = """
CREATE TABLE IF NOT EXISTS crop_progress (
    commodity       TEXT    NOT NULL,
    week_ending     TEXT    NOT NULL,
    year            TEXT,
    short_desc      TEXT    NOT NULL,
    Value           TEXT,
    unit_desc       TEXT,
    stat_category   TEXT,
    PRIMARY KEY (commodity, week_ending, short_desc)
);
"""

_CREATE_EXPORT_SALES = """
CREATE TABLE IF NOT EXISTS export_sales (
    commodity           TEXT    NOT NULL,
    week_ending         TEXT    NOT NULL,
    country             TEXT    NOT NULL,
    net_sales           REAL,
    weekly_exports      REAL,
    accumulated_exports REAL,
    outstanding_sales   REAL,
    unit                TEXT,
    PRIMARY KEY (commodity, week_ending, country)
);
"""

# fetched_date is part of the key: each pipeline run stores that day's full
# curve, so term-structure history accumulates instead of being overwritten.
# Readers wanting "the current curve" filter to the latest fetched_date
# (see pipeline/query.read_forward_curve).
#
# observation_date is the session every leg of that curve was observed on —
# the date a trader reads the structure at. fetched_date is only when the
# pipeline ran (they differ whenever the run lands before settlement, or on
# a holiday). Legacy rows predating the column are NULL.
_CREATE_FORWARD_CURVE = """
CREATE TABLE IF NOT EXISTS forward_curve (
    commodity        TEXT    NOT NULL,
    contract_month   TEXT    NOT NULL,
    label            TEXT,
    ticker           TEXT,
    close            REAL,
    observation_date TEXT,
    volume           REAL,
    open_interest    REAL,
    fetched_date     TEXT    NOT NULL,
    PRIMARY KEY (commodity, contract_month, fetched_date)
);
"""
# volume is per-contract and the provider publishes it; open_interest is
# published by no source this project ingests and is therefore always NULL.
# The column exists so an authoritative provider can fill it without a
# migration, and so consumers can distinguish "never learned" (NULL) from
# "none" (0) — which is the difference the whole Phase 3 workstation refuses
# to blur.

_CREATE_WASDE = """
CREATE TABLE IF NOT EXISTS wasde (
    commodity       TEXT NOT NULL,
    year            TEXT NOT NULL,
    attribute       TEXT NOT NULL,
    value           REAL,
    unit            TEXT,
    reference_period TEXT,
    PRIMARY KEY (commodity, year, attribute, reference_period)
);
"""

_CREATE_INSPECTIONS = """
CREATE TABLE IF NOT EXISTS inspections (
    commodity       TEXT NOT NULL,
    week_ending     TEXT NOT NULL,
    inspections_mt  REAL,
    PRIMARY KEY (commodity, week_ending)
);
"""

_CREATE_INSPECTION_PORT_FLOWS = """
CREATE TABLE IF NOT EXISTS inspection_port_flows (
    week_ending     TEXT NOT NULL,
    region          TEXT NOT NULL,
    port_area       TEXT NOT NULL,
    commodity       TEXT NOT NULL,
    inspections_mt  REAL,
    PRIMARY KEY (week_ending, region, port_area, commodity)
);
"""

_CREATE_INSPECTION_DESTINATIONS = """
CREATE TABLE IF NOT EXISTS inspection_destinations (
    week_ending     TEXT NOT NULL,
    region          TEXT NOT NULL,
    country         TEXT NOT NULL,
    commodity       TEXT NOT NULL,
    inspections_mt  REAL,
    PRIMARY KEY (week_ending, region, country, commodity)
);
"""

_CREATE_ARGENTINA_FOB = """
CREATE TABLE IF NOT EXISTS argentina_fob (
    date          TEXT NOT NULL,
    product       TEXT NOT NULL,
    position      TEXT NOT NULL,
    ship_from     TEXT NOT NULL,
    ship_to       TEXT,
    price_usd_mt  REAL,
    PRIMARY KEY (date, position, ship_from)
);
"""

_CREATE_GULF_BIDS = """
CREATE TABLE IF NOT EXISTS gulf_bids (
    report_date   TEXT NOT NULL,
    commodity     TEXT NOT NULL,
    location      TEXT NOT NULL,
    delivery      TEXT NOT NULL,
    sale_type     TEXT,
    basis_low     REAL,
    basis_high    REAL,
    futures_month INTEGER,
    futures_month_high INTEGER,
    basis_change  TEXT,
    price_change  TEXT,
    price_low     REAL,
    price_high    REAL,
    average       REAL,
    year_ago      REAL,
    freight       TEXT,
    PRIMARY KEY (report_date, commodity, location, delivery)
);
"""

_CREATE_EIA_ENERGY = """
CREATE TABLE IF NOT EXISTS eia_energy (
    series_name TEXT NOT NULL,
    Date        TEXT NOT NULL,
    value       REAL,
    unit        TEXT,
    PRIMARY KEY (series_name, Date)
);
"""

_CREATE_BRAZIL_ESTIMATES = """
CREATE TABLE IF NOT EXISTS brazil_estimates (
    source      TEXT NOT NULL,
    commodity   TEXT NOT NULL,
    crop_year   TEXT NOT NULL,
    attribute   TEXT NOT NULL,
    value       REAL,
    unit        TEXT,
    report_date TEXT,
    PRIMARY KEY (source, commodity, crop_year, attribute, report_date)
);
"""

_CREATE_INDIA_DOMESTIC = """
CREATE TABLE IF NOT EXISTS india_domestic_prices (
    Date        TEXT NOT NULL,
    commodity   TEXT NOT NULL,
    Open        REAL,
    High        REAL,
    Low         REAL,
    Close       REAL,
    Volume      REAL,
    unit        TEXT,
    PRIMARY KEY (Date, commodity)
);
"""

_CREATE_BRAZIL_SPOT = """
CREATE TABLE IF NOT EXISTS brazil_spot_prices (
    Date        TEXT NOT NULL,
    commodity   TEXT NOT NULL,
    price_brl   REAL,
    unit        TEXT,
    PRIMARY KEY (Date, commodity)
);
"""

# SAGIS weekly producer deliveries (Layer 23). Keyed by season + week
# number rather than by week_end: SAGIS's own comparison columns are built
# on week *number* ("The beginning and end dates of a week are therefore not
# the same for each year"), and a season's week 1 can start in either
# February or March. week_end is carried alongside as the observation date
# the recency gate and every chart read from.
_CREATE_SAGIS_DELIVERIES = """
CREATE TABLE IF NOT EXISTS sagis_deliveries (
    commodity       TEXT NOT NULL,
    season_year     INTEGER NOT NULL,   -- start year of the Mar-Feb season
    week_number     INTEGER NOT NULL,   -- 1..52 within that season
    week_end        TEXT NOT NULL,      -- ISO date of the week's last day
    season_status   TEXT,               -- 'Active' | 'Final'
    first_published REAL,               -- tonnage as first reported
    adjustments     REAL,               -- later revisions (signed)
    week_total      REAL,               -- first_published + adjustments
    unit            TEXT,
    PRIMARY KEY (commodity, season_year, week_number)
);
"""

# SAGIS monthly soybean supply & demand (Layer 24). Keyed by season +
# position-in-season for the same reason Layer 23 is keyed by week number:
# the source frames every comparison inside a March–February marketing
# season, and month_number 1 is always March. month_end carries the calendar
# date the recency gate and every chart read from.
#
# Only components are stored. The section totals the report prints —
# (b) Acquisition, (c) Utilisation, (e) Sundries — are sums of columns below
# and are re-derived at read time, which is also what makes the fetcher's
# balance check possible. `report_month` is the SMD-MMYYYY vintage that
# produced the row: SAGIS revises months for a year or more, so a row's
# numbers and the report they came from are two different facts.
_CREATE_SAGIS_SUPPLY_DEMAND = """
CREATE TABLE IF NOT EXISTS sagis_supply_demand (
    commodity                 TEXT NOT NULL,
    season_year               INTEGER NOT NULL,  -- start year of the Mar-Feb season
    month_number              INTEGER NOT NULL,  -- 1..12, 1 = March
    month_end                 TEXT NOT NULL,     -- ISO last day of the month
    report_month              TEXT,              -- ISO first-of-month of the SMD vintage
    opening_stock             REAL,
    deliveries                REAL,              -- direct from farms
    imports                   REAL,              -- destined for RSA
    processed_total           REAL,              -- commercial use, incl. export equivalent
    processed_human           REAL,
    processed_feed            REAL,
    processed_oil_oilcake     REAL,              -- SA crush volume
    withdrawn_by_producers    REAL,
    released_to_end_consumers REAL,
    seed_for_planting         REAL,
    exports_whole             REAL,              -- whole beans
    exports_border_posts      REAL,
    exports_harbours          REAL,
    sundries_net_dispatches   REAL,              -- signed
    sundries_surplus_deficit  REAL,              -- signed
    unutilised_stock          REAL,              -- closing stock
    stock_storers_traders     REAL,
    stock_processors          REAL,
    products_exported         REAL,              -- soybean equivalent of products
    products_exported_africa  REAL,
    products_exported_other   REAL,
    unit                      TEXT,
    PRIMARY KEY (commodity, season_year, month_number)
);
"""

# CEC South Africa official crop estimates (Layer 25). A *revision series*,
# not a current-value table: the committee re-forecasts the same season nine
# times and the CELC then finalises it, so every release is kept and keyed by
# (commodity, season_year, release_date). Overwriting to one current estimate
# would throw away the month-on-month path, which is the whole point of the
# layer — USDA's PSD carries the same final number (#204).
#
# season_year is the CEC's own calendar-year convention: 2026 is the crop
# harvested in 2026, i.e. the 2025/26 production season. area_ha and
# production_t are the two components; yield is derived at read time, never
# stored. production_t is NULL where a release carries no production number
# at all — January's preliminary area estimate and October's intentions to
# plant are area-only, which is different from a production of zero.
_CREATE_CEC_ESTIMATES = """
CREATE TABLE IF NOT EXISTS cec_estimates (
    commodity      TEXT NOT NULL,
    season_year    INTEGER NOT NULL,   -- calendar year of the harvest
    release_date   TEXT NOT NULL,      -- ISO date printed on the release
    estimate_kind  TEXT NOT NULL,      -- intentions | preliminary_area |
                                       -- forecast | final_estimate | final_crop
    forecast_seq   INTEGER,            -- 1..9 for forecasts, else NULL
    forecast_label TEXT,               -- the release's own wording
    area_ha        REAL,
    production_t   REAL,               -- NULL on area-only releases
    unit           TEXT,
    source_url     TEXT,
    PRIMARY KEY (commodity, season_year, release_date)
);
"""

_CREATE_SAFEX = """
CREATE TABLE IF NOT EXISTS safex_prices (
    Date        TEXT NOT NULL,
    commodity   TEXT NOT NULL,
    Close       REAL,
    Volume      REAL,
    unit        TEXT,
    contract    TEXT,                       -- JSE MMMYY code, e.g. 'AUG26'
    PRIMARY KEY (Date, commodity)
);
"""

_CREATE_DATA_FRESHNESS = """
CREATE TABLE IF NOT EXISTS data_freshness (
    layer_name      TEXT    NOT NULL PRIMARY KEY,
    last_success    TEXT,                              -- null if never succeeded
    last_attempt    TEXT,                              -- timestamp of most recent run
    rows_fetched    INTEGER,
    status          TEXT    NOT NULL DEFAULT 'success', -- run-state classification
    -- Key coverage (#182). NULL = never learned (transport failure, or a
    -- layer with no key catalog); 0 = asked and got nothing back.
    keys_returned   INTEGER,
    keys_expected   INTEGER
);
"""

_CREATE_COMMODITY_FRESHNESS = """
CREATE TABLE IF NOT EXISTS commodity_freshness (
    commodity       TEXT    NOT NULL,
    table_name      TEXT    NOT NULL,
    last_date_in_db TEXT,
    rows_total      INTEGER,
    checked_at      TEXT    NOT NULL,
    PRIMARY KEY (commodity, table_name)
);
"""

# Origin-comparison rankings (Phase 2). One row per (run, destination, window,
# origin) — the answer this stack published on a given day, not a re-derivable
# view of it.
#
# It has to be stored for the same reason `briefings` is: the ranking is
# computed from that run's DB *and* from the assumption file as it stood that
# morning, and neither is recoverable later. An expired freight number is gone
# from the working set by design, so "what did we say on 12 September, and what
# did we say it on" cannot be reconstructed from any source.
#
# `rank` is NULL for a row that was rendered but not rankable — which is a
# result, not a gap: "Argentina was quoting November, so it was not ranked" is
# exactly the history a reader needs to interpret a gap in the series.
# `input_digest` fingerprints the row's own waterfall, so an input change and a
# market move can be told apart without diffing two JSON blobs by eye.
_CREATE_ORIGIN_RANKINGS = """
CREATE TABLE IF NOT EXISTS origin_rankings (
    run_date          TEXT NOT NULL,
    destination       TEXT NOT NULL,
    window_start      TEXT NOT NULL,
    window_end        TEXT NOT NULL,
    origin            TEXT NOT NULL,
    rank              INTEGER,
    landed_usd_mt     REAL,
    fob_usd_mt        REAL,
    origin_usd_mt     REAL,
    comparability     TEXT NOT NULL,
    confidence        TEXT NOT NULL,
    freshness         TEXT NOT NULL,
    observation_date  TEXT,
    shipment_window   TEXT,
    incoterm          TEXT,
    quote_kind        TEXT,
    method_version    TEXT NOT NULL,
    assumption_set_id TEXT NOT NULL,
    input_digest      TEXT NOT NULL,
    snapshot_json     TEXT,
    generated_at      TEXT NOT NULL,
    PRIMARY KEY (run_date, destination, window_start, origin)
);
"""

_CREATE_BRIEFINGS = """
CREATE TABLE IF NOT EXISTS briefings (
    briefing_date   TEXT    NOT NULL PRIMARY KEY,
    text            TEXT    NOT NULL,
    signals_json    TEXT,
    snapshot_json   TEXT,
    generated_at    TEXT    NOT NULL
);
"""


# Bundle for callers that need every table's DDL in one iterable.
ALL_SCHEMAS = (
    _CREATE_PRICES,
    _CREATE_ECONOMIC,
    _CREATE_USDA,
    _CREATE_COT,
    _CREATE_WEATHER,
    _CREATE_PSD,
    _CREATE_CURRENCIES,
    _CREATE_WORLDBANK,
    _CREATE_EC_OILSEED_PRICES,
    _CREATE_DCE_FUTURES,
    _CREATE_CROP_PROGRESS,
    _CREATE_EXPORT_SALES,
    _CREATE_FORWARD_CURVE,
    _CREATE_WASDE,
    _CREATE_INSPECTIONS,
    _CREATE_INSPECTION_PORT_FLOWS,
    _CREATE_INSPECTION_DESTINATIONS,
    _CREATE_ARGENTINA_FOB,
    _CREATE_GULF_BIDS,
    _CREATE_EIA_ENERGY,
    _CREATE_BRAZIL_ESTIMATES,
    _CREATE_DATA_FRESHNESS,
    _CREATE_COMMODITY_FRESHNESS,
    _CREATE_INDIA_DOMESTIC,
    _CREATE_BRAZIL_SPOT,
    _CREATE_SAFEX,
    _CREATE_SAGIS_DELIVERIES,
    _CREATE_SAGIS_SUPPLY_DEMAND,
    _CREATE_CEC_ESTIMATES,
    _CREATE_ORIGIN_RANKINGS,
    _CREATE_BRIEFINGS,
)


# Belt-and-suspenders: explicit UNIQUE INDEXes on every PK column set.
# PRIMARY KEY already implies a unique index in SQLite, but defining them
# explicitly keeps the contract visible and protects any older user DBs
# that may pre-date the current PK constraints.
UNIQUE_INDEXES = (
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_prices_commodity_date "
    "ON prices (commodity, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_economic_series_date "
    "ON economic (series_name, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_usda_cat_year_desc_period "
    "ON usda (stat_category, year, short_desc, reference_period_desc);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_cot_commodity_date "
    "ON cot (commodity, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_weather_region_date "
    "ON weather (region, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_psd_commodity_country_year_attr "
    "ON psd (commodity, country, year, attribute);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_currencies_pair_date "
    "ON currencies (pair, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_worldbank_commodity_date "
    "ON worldbank_prices (commodity, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_ec_oilseed_prices_series_date "
    "ON ec_oilseed_prices (series, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_dce_futures_commodity_date "
    "ON dce_futures (commodity, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_crop_progress_commodity_week_desc "
    "ON crop_progress (commodity, week_ending, short_desc);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_export_sales_commodity_week_country "
    "ON export_sales (commodity, week_ending, country);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_forward_curve_commodity_contract_date "
    "ON forward_curve (commodity, contract_month, fetched_date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_wasde_commodity_year_attr_period "
    "ON wasde (commodity, year, attribute, reference_period);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_inspections_commodity_week "
    "ON inspections (commodity, week_ending);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_eia_energy_series_date "
    "ON eia_energy (series_name, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_brazil_estimates_keys "
    "ON brazil_estimates (source, commodity, crop_year, attribute, report_date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_commodity_freshness_keys "
    "ON commodity_freshness (commodity, table_name);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_india_domestic_date_commodity "
    "ON india_domestic_prices (Date, commodity);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_brazil_spot_date_commodity "
    "ON brazil_spot_prices (Date, commodity);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_safex_date_commodity "
    "ON safex_prices (Date, commodity);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_sagis_commodity_season_week "
    "ON sagis_deliveries (commodity, season_year, week_number);",
    "CREATE INDEX IF NOT EXISTS ix_sagis_week_end "
    "ON sagis_deliveries (week_end);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_sagis_smd_commodity_season_month "
    "ON sagis_supply_demand (commodity, season_year, month_number);",
    "CREATE INDEX IF NOT EXISTS ix_sagis_smd_month_end "
    "ON sagis_supply_demand (month_end);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_cec_commodity_season_release "
    "ON cec_estimates (commodity, season_year, release_date);",
    "CREATE INDEX IF NOT EXISTS ix_cec_release_date "
    "ON cec_estimates (release_date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_briefings_date "
    "ON briefings (briefing_date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_origin_rankings_keys "
    "ON origin_rankings (run_date, destination, window_start, origin);",
    "CREATE INDEX IF NOT EXISTS ix_origin_rankings_run_date "
    "ON origin_rankings (run_date);",
)
