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

_CREATE_USDA = """
CREATE TABLE IF NOT EXISTS usda (
    stat_category           TEXT,
    year                    TEXT,
    short_desc              TEXT,
    Value                   TEXT,
    unit_desc               TEXT,
    state_name              TEXT,
    reference_period_desc   TEXT,
    PRIMARY KEY (stat_category, year, short_desc)
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
    PRIMARY KEY (commodity, week_ending, country)
);
"""

_CREATE_FORWARD_CURVE = """
CREATE TABLE IF NOT EXISTS forward_curve (
    commodity       TEXT    NOT NULL,
    contract_month  TEXT    NOT NULL,
    label           TEXT,
    ticker          TEXT,
    close           REAL,
    fetched_date    TEXT    NOT NULL,
    PRIMARY KEY (commodity, contract_month)
);
"""

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
    basis_change  TEXT,
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

_CREATE_SAFEX = """
CREATE TABLE IF NOT EXISTS safex_prices (
    Date        TEXT NOT NULL,
    commodity   TEXT NOT NULL,
    Close       REAL,
    Volume      REAL,
    unit        TEXT,
    PRIMARY KEY (Date, commodity)
);
"""

_CREATE_DATA_FRESHNESS = """
CREATE TABLE IF NOT EXISTS data_freshness (
    layer_name      TEXT    NOT NULL PRIMARY KEY,
    last_success    TEXT,                              -- null if never succeeded
    last_attempt    TEXT,                              -- timestamp of most recent run
    rows_fetched    INTEGER,
    status          TEXT    NOT NULL DEFAULT 'success' -- 'success' | 'failed'
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
    _CREATE_DCE_FUTURES,
    _CREATE_CROP_PROGRESS,
    _CREATE_EXPORT_SALES,
    _CREATE_FORWARD_CURVE,
    _CREATE_WASDE,
    _CREATE_INSPECTIONS,
    _CREATE_INSPECTION_PORT_FLOWS,
    _CREATE_GULF_BIDS,
    _CREATE_EIA_ENERGY,
    _CREATE_BRAZIL_ESTIMATES,
    _CREATE_DATA_FRESHNESS,
    _CREATE_COMMODITY_FRESHNESS,
    _CREATE_INDIA_DOMESTIC,
    _CREATE_BRAZIL_SPOT,
    _CREATE_SAFEX,
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
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_usda_cat_year_desc "
    "ON usda (stat_category, year, short_desc);",
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
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_dce_futures_commodity_date "
    "ON dce_futures (commodity, Date);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_crop_progress_commodity_week_desc "
    "ON crop_progress (commodity, week_ending, short_desc);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_export_sales_commodity_week_country "
    "ON export_sales (commodity, week_ending, country);",
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_forward_curve_commodity_contract "
    "ON forward_curve (commodity, contract_month);",
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
    "CREATE UNIQUE INDEX IF NOT EXISTS ux_briefings_date "
    "ON briefings (briefing_date);",
)

