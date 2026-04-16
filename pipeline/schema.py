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

_CREATE_OPTIONS_SENTIMENT = """
CREATE TABLE IF NOT EXISTS options_sentiment (
    commodity       TEXT NOT NULL,
    Date            TEXT NOT NULL,
    total_call_oi   REAL,
    total_put_oi    REAL,
    put_call_ratio  REAL,
    avg_call_iv     REAL,
    avg_put_iv      REAL,
    PRIMARY KEY (commodity, Date)
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
    last_success    TEXT    NOT NULL,
    rows_fetched    INTEGER
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


