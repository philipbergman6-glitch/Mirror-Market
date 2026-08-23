"""
Data cleaning utilities.

Raw data from external APIs is messy — missing days (weekends/holidays),
occasional NaN values, inconsistent column names.  This module normalises
everything into a consistent format before it hits the database.

Prices are never forward-filled (B1, #307). A carried-forward close is an
invented observation: RSI/MACD/daily-change would read it as a real 0%
session, and a rendered candle would show a bar nobody printed. Partial
bars are dropped with a logged reason instead. Limited forward-fill
survives only where the series' own semantics allow carrying a published
observation across a gap — low-frequency macro series (FRED publishes
monthly; the value *is* in force between publications) and small weather
gaps — never for market prices.
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Commodities exempt from the zero-volume sanity warning. CPO=F (Palm Oil)
# is a settlement-marked calendar swap: volume is ~0 every day by design,
# so the warning would fire on every run without indicating a data gap.
ZERO_VOLUME_EXEMPT = {"Palm Oil (CME)"}


def _validate_price_data(df: pd.DataFrame, label: str = ""):
    """
    Run sanity checks on price data and log warnings for suspicious values.

    Checks:
        - Daily close change >10% (possible data corruption or extreme event)
        - Zero or negative volume (missing data) — skipped for commodities
          in ZERO_VOLUME_EXEMPT

    These are warnings only — they don't block the pipeline, and that is
    correct here: a 10% *session* is a fact about the market, and a cleaner
    that dropped it would delete the days a trader most needs. The move
    that is a fact about the *fetch* — a value disagreeing with what is
    already stored for the same date — is a different question, and it is
    answered at the write instead, where the stored value can be compared
    against: see `pipeline.divergence` (T19 · F9, #67).
    """
    if df.empty or "Close" not in df.columns:
        return

    prefix = f"[{label}] " if label else ""

    # Check for extreme daily price moves
    pct_change = df["Close"].pct_change().abs()
    extreme = pct_change[pct_change > 0.10]
    for idx in extreme.index:
        logger.warning(
            "%sLarge price move on %s: %.1f%% change (verify data integrity)",
            prefix, idx.date() if hasattr(idx, "date") else idx, extreme[idx] * 100,
        )

    # Check for zero or negative volume
    if "Volume" in df.columns and label not in ZERO_VOLUME_EXEMPT:
        bad_volume = df[df["Volume"] <= 0]
        if not bad_volume.empty:
            logger.warning(
                "%s%d rows with zero/negative volume (possible data gap)",
                prefix, len(bad_volume),
            )




def _drop_partial_bars(df: pd.DataFrame, price_cols: list[str], label: str = "") -> pd.DataFrame:
    """Drop any bar with a missing value in a present price column.

    A bar missing its Close would need a fabricated close to be usable, and
    a bar missing Open/High/Low is half an observation that would render as
    a broken candle — neither is filled, both are dropped with a reason
    (invariant 2: absence never becomes an assumption).

    TODO(#299): interim treatment. If A2 decides quarantine over rejection,
    dropped bars should be stored flagged instead of discarded here.
    """
    present = [c for c in price_cols if c in df.columns]
    if not present:
        return df
    partial = df[present].isna().any(axis=1)
    if partial.any():
        prefix = f"[{label}] " if label else ""
        # Dates live on the index for yfinance frames, in a column for DCE.
        date_values = df["Date"][partial] if "Date" in df.columns else df.index[partial]
        dates = ", ".join(
            str(d.date()) if hasattr(d, "date") else str(d)
            for d in list(date_values)[:10]
        )
        if int(partial.sum()) > 10:
            dates += f", … and {int(partial.sum()) - 10} more"
        logger.warning(
            "%sDropped %d partial bar(s) with missing price values (%s) — "
            "never filled; see #299 for quarantine semantics",
            prefix, int(partial.sum()), dates,
        )
        df = df[~partial]
    return df


def clean_ohlcv(df: pd.DataFrame, label: str = "") -> pd.DataFrame:
    """
    Clean a raw OHLCV DataFrame from yfinance.

    Steps:
        1. Ensure the index is a proper DatetimeIndex named "Date".
        2. Drop bars with any missing OHLC value — all-NaN weekend/holiday
           rows and partial bars alike. Nothing is forward-filled (#307).
        3. Run sanity checks (warnings only).

    A NaN Volume never disqualifies a bar and is left NaN: Volume is not a
    price, and never-learned is not zero.

    Parameters
    ----------
    df : pd.DataFrame
        Raw output from yfinance (Open, High, Low, Close, Volume).
    label : str
        Commodity/pair name — prefixes warning logs and drives per-commodity
        sanity-check exemptions (see ZERO_VOLUME_EXEMPT).

    Returns
    -------
    pd.DataFrame   — cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    # Guarantee a clean datetime index
    df.index = pd.to_datetime(df.index)
    df.index.name = "Date"

    price_cols = ["Open", "High", "Low", "Close"]
    present = [c for c in price_cols if c in df.columns]
    # Drop all-NaN rows silently (weekends/holidays are expected), then
    # partial bars with a logged reason — those are real data defects.
    df = df.dropna(subset=present, how="all")
    df = _drop_partial_bars(df, price_cols, label=label)

    # Run sanity checks (warnings only — doesn't block pipeline)
    _validate_price_data(df, label=label)

    return df


def clean_fred_series(series: pd.Series) -> pd.Series:
    """
    Clean a FRED time series.

    Forward-fills gaps (FRED often publishes monthly, so daily gaps are
    expected) and drops any remaining NaNs at the start.
    """
    if series.empty:
        return series

    series = series.copy()
    series.index = pd.to_datetime(series.index)
    series = series.sort_index()
    series = series.ffill(limit=5).dropna()
    return series


def clean_cot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean COT (Commitment of Traders) data.

    Steps:
        1. Ensure Date column is datetime.
        2. Sort by date.
        3. Drop rows where all position columns are NaN.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)

    position_cols = [
        "commercial_long", "commercial_short",
        "noncommercial_long", "noncommercial_short",
    ]
    present = [c for c in position_cols if c in df.columns]
    if present:
        df = df.dropna(subset=present, how="all")

    return df


def clean_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean weather data from Open-Meteo.

    Steps:
        1. Ensure Date column is datetime.
        2. Sort by date.
        3. Forward-fill small gaps (API occasionally has missing values).

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    # is_forecast is a flag, not a measurement — hold it out of the
    # forward-fill so a gap never inherits a neighboring day's flag.
    measure_cols = [c for c in df.columns if c != "is_forecast"]
    df[measure_cols] = df[measure_cols].ffill(limit=3)
    if "is_forecast" in df.columns:
        df["is_forecast"] = pd.to_numeric(df["is_forecast"], errors="coerce")
    return df


def clean_river_levels(df: pd.DataFrame) -> pd.DataFrame:
    """Clean river stage rows from either provider (Layers 27/28).

    Deliberately *not* clean_weather: weather forward-fills small gaps, and a
    river gauge must not. A missing stage is a reading nobody took, and the
    days it goes missing — a frozen sensor in a low-water event — are exactly
    the days a carried-forward level would be wrong in the direction that
    matters. NULL stays NULL (invariant 2).

    Returns a cleaned copy; the original is not mutated.
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df["stage"] = pd.to_numeric(df["stage"], errors="coerce")
    if "is_forecast" in df.columns:
        df["is_forecast"] = pd.to_numeric(df["is_forecast"], errors="coerce")
    # A row with no date cannot be keyed, and a row with no unit cannot be
    # read: feet and metres are both plausible numbers for either river.
    df = df.dropna(subset=["Date"])
    if "unit" in df.columns:
        df = df[df["unit"].notna() & (df["unit"].astype(str).str.strip() != "")]
    return df.sort_values("Date").reset_index(drop=True)


def clean_psd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean PSD (Production, Supply & Distribution) data.

    Steps:
        1. Standardise country name casing.
        2. Drop rows with missing values in key columns.
        3. Ensure year is integer.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    # Standardise country names (strip whitespace, title case)
    if "country" in df.columns:
        df["country"] = df["country"].str.strip()

    # Ensure year is integer
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce")
        df = df.dropna(subset=["year"])
        df["year"] = df["year"].astype(int)

    # Drop rows missing a value
    if "value" in df.columns:
        df = df.dropna(subset=["value"])

    return df.reset_index(drop=True)



def clean_dce_futures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DCE futures data from AKShare.

    AKShare returns lowercase columns: date, open, high, low, close,
    volume, hold, settle.  We rename them to match our project conventions
    (capitalized names, Open_Interest instead of hold).

    Steps:
        1. Rename columns to project conventions.
        2. Parse Date to datetime and sort by date.
        3. Drop bars with any missing OHLC value — nothing is
           forward-filled, same rule as clean_ohlcv (#307).

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    # Rename AKShare lowercase columns → project conventions
    rename_map = {
        "date": "Date",
        "open": "Open",
        "high": "High",
        "low": "Low",
        "close": "Close",
        "volume": "Volume",
        "hold": "Open_Interest",
        "settle": "Settle",
    }
    df = df.rename(columns=rename_map)

    # Parse and sort by date
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)

    # Drop all-NaN price rows silently, then partial bars with a reason —
    # never filled (#307).
    price_cols = ["Open", "High", "Low", "Close"]
    present = [c for c in price_cols if c in df.columns]
    df = df.dropna(subset=present, how="all")
    df = _drop_partial_bars(df, price_cols, label="DCE")

    return df


def clean_export_sales(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean USDA FAS export sales data.

    Steps:
        1. Ensure week_ending is datetime.
        2. Sort by week_ending.
        3. Drop rows where net_sales is NaN.
        4. Convert numeric columns to float.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"])
        df = df.sort_values("week_ending").reset_index(drop=True)

    # Convert numeric columns
    numeric_cols = ["net_sales", "weekly_exports", "accumulated_exports", "outstanding_sales"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no sales data
    if "net_sales" in df.columns:
        df = df.dropna(subset=["net_sales"])

    return df


def clean_forward_curve(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean forward curve data.

    Steps:
        1. Ensure contract_month is a date string (YYYY-MM-DD).
        2. Sort by contract_month.
        3. Drop rows where close is NaN or zero.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"])
        df = df[df["close"] > 0]

    if "contract_month" in df.columns:
        df = df.sort_values("contract_month").reset_index(drop=True)

    return df


def clean_contract_history(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean per-contract daily close history (Layer 11b).

    Steps:
        1. Coerce close numeric; drop NaN and non-positive closes — a CBOT
           soy price of zero is an error value, never an observation.
        2. Coerce volume numeric (nullable — Yahoo omits it on some bars).
        3. One row per (ticker, session): keep the last, matching the
           store's INSERT OR REPLACE outcome so re-runs converge.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "close" in df.columns:
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        before = len(df)
        df = df.dropna(subset=["close"])
        df = df[df["close"] > 0]
        if len(df) < before:
            logger.warning(
                "contract_history: dropped %d row(s) with missing or "
                "non-positive close", before - len(df),
            )

    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce")

    if {"ticker", "contract_month", "date"} <= set(df.columns):
        df = df.drop_duplicates(subset=["ticker", "date"], keep="last")
        df = df.sort_values(["contract_month", "date"]).reset_index(drop=True)

    return df


def clean_wasde(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean WASDE forecast data from USDA NASS.

    Steps:
        1. Ensure year is a string.
        2. Clean Value column (remove commas, convert to numeric).
        3. Drop rows with NaN values.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "year" in df.columns:
        df["year"] = df["year"].astype(str)

    if "Value" in df.columns:
        df["Value"] = df["Value"].astype(str).str.replace(",", "", regex=False)
        df["Value"] = pd.to_numeric(df["Value"], errors="coerce")
        df = df.dropna(subset=["Value"])

    return df.reset_index(drop=True)


def clean_eia(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean EIA energy data.

    Same pattern as clean_fred_series but for DataFrames:
        1. Ensure Date is datetime.
        2. Sort by date.
        3. Drop NaN values.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date").reset_index(drop=True)

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

    return df


def clean_inspections(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean export inspections data from AMS.

    Steps:
        1. Ensure week_ending is datetime.
        2. Sort by week_ending.
        3. Convert inspections_mt to numeric.
        4. Drop NaN rows.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "week_ending" in df.columns:
        df["week_ending"] = pd.to_datetime(df["week_ending"])
        df = df.sort_values("week_ending").reset_index(drop=True)

    if "inspections_mt" in df.columns:
        df["inspections_mt"] = pd.to_numeric(df["inspections_mt"], errors="coerce")
        df = df.dropna(subset=["inspections_mt"])

    return df


def clean_conab(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean CONAB Brazil crop estimate data.

    Steps:
        1. Ensure value is numeric.
        2. Drop rows with NaN values.
        3. Strip whitespace from string columns.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["value"])

    for col in ["commodity", "crop_year", "attribute", "source"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    return df.reset_index(drop=True)



def clean_india_domestic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean NCDEX India domestic price data.

    Steps:
        1. Copy first (never mutate originals).
        2. Parse Date to datetime and sort.
        3. Convert price columns to numeric.
        4. Drop rows where Close is missing or zero.

    Returns cleaned copy.
    """
    if df.empty:
        return df

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date").reset_index(drop=True)

    for col in ("Open", "High", "Low", "Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Close" in df.columns:
        df = df.dropna(subset=["Close"])
        df = df[df["Close"] > 0]

    return df.reset_index(drop=True)


def clean_brazil_spot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean CEPEA Brazil domestic spot price data.

    Steps:
        1. Copy first.
        2. Parse Date and sort.
        3. Convert price_brl_mt to numeric.
        4. Drop rows with missing or zero prices.

    Returns cleaned copy.
    """
    if df.empty:
        return df

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date").reset_index(drop=True)

    if "price_brl_mt" in df.columns:
        df["price_brl_mt"] = pd.to_numeric(df["price_brl_mt"], errors="coerce")
        df = df.dropna(subset=["price_brl_mt"])
        df = df[df["price_brl_mt"] > 0]

    return df.reset_index(drop=True)


def clean_safex(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean JSE SAFEX South Africa last-traded price data (not settlement/MTM —
    see fetchers/safex.py).

    Steps:
        1. Copy first.
        2. Parse Date and sort.
        3. Convert Close and Volume to numeric.
        4. Drop rows where Close is missing or zero.

    Returns cleaned copy.
    """
    if df.empty:
        return df

    df = df.copy()

    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.sort_values("Date").reset_index(drop=True)

    for col in ("Close", "Volume"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "Close" in df.columns:
        df = df.dropna(subset=["Close"])
        df = df[df["Close"] > 0]

    return df.reset_index(drop=True)


def clean_sagis_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean SAGIS weekly producer deliveries (Layer 23).

    Steps:
        1. Copy first.
        2. Parse week_end and sort by (season_year, week_number).
        3. Coerce the three tonnage components to numeric.
        4. Drop rows with no week_total.

    Zero and negative values are kept deliberately. A week of zero
    deliveries is real — the season is 52 weeks long but the harvest runs
    roughly March–July, so most of the year genuinely reports near-nothing.
    `adjustments` is signed and legitimately negative (week 1 of 2026/27:
    3,350 delivered, −666 adjustment), and a large enough downward revision
    can push `week_total` negative. Filtering either would silently rewrite
    the source's own arithmetic.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    if "week_end" in df.columns:
        df["week_end"] = pd.to_datetime(df["week_end"])

    for col in ("first_published", "adjustments", "week_total"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    sort_cols = [c for c in ("season_year", "week_number") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    if "week_total" in df.columns:
        df = df.dropna(subset=["week_total"])

    return df.reset_index(drop=True)


_SAGIS_SMD_NUMERIC = (
    "opening_stock", "deliveries", "imports", "processed_total",
    "processed_human", "processed_feed", "processed_oil_oilcake",
    "withdrawn_by_producers", "released_to_end_consumers", "seed_for_planting",
    "exports_whole", "exports_border_posts", "exports_harbours",
    "sundries_net_dispatches", "sundries_surplus_deficit", "unutilised_stock",
    "stock_storers_traders", "stock_processors", "products_exported",
    "products_exported_africa", "products_exported_other",
)


def clean_sagis_smd(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean SAGIS monthly soybean supply & demand (Layer 24).

    Steps:
        1. Copy first.
        2. Parse month_end / report_month and sort by (season_year, month_number).
        3. Coerce every tonnage column to numeric.
        4. Drop rows with no month_end — a row that cannot be dated cannot be
           graded for recency, and would read as an undated observation.

    Zeros and negatives are kept. A month of zero imports is the norm for
    South African soybeans (0 t in Mar and Apr 2026), and the two sundries
    columns are signed by definition — filtering either would rewrite the
    source's own balance sheet, which the fetcher checks arithmetically.
    Unreported months never reach here: the fetcher cuts the season at the
    workbook's own SMD vintage, because the report prints future months as 0.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()

    for col in ("month_end", "report_month"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in _SAGIS_SMD_NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    sort_cols = [c for c in ("season_year", "month_number") if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols)

    if "month_end" in df.columns:
        df = df.dropna(subset=["month_end"])

    return df.reset_index(drop=True)


def clean_ec_oilseeds(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean EC weekly world oilseed prices (Layer 22).

    Steps:
        1. Ensure Date is datetime and sort ascending.
        2. Drop rows with no USD price — the authoritative column.
        3. Drop duplicate weeks, keeping the last.

    `price_eur` NaNs are deliberately preserved: the Commission publishes
    `n.q.` ("not quoted") for weeks it did not convert, and the EUR block is
    derived from the USD one rather than independently assessed. Filling or
    recomputing those gaps would publish our arithmetic as the Commission's.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.dropna(subset=["price_usd"])
    df = df.drop_duplicates(subset=["Date"], keep="last")
    df = df.sort_values("Date").reset_index(drop=True)
    return df


def clean_ocean_freight(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean GTR monthly ocean freight rates (Layer 26).

    Steps:
        1. Ensure Date is datetime and sort ascending.
        2. Drop rows with no rate — the only column there is.
        3. Drop duplicate months, keeping the last.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.dropna(subset=["rate_usd_mt"])
    df = df.drop_duplicates(subset=["Date"], keep="last")
    df = df.sort_values("Date").reset_index(drop=True)
    return df


def clean_port_vessel_activity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean GTR weekly vessel activity (Layer 26b).

    Steps:
        1. Ensure week_ending is datetime and sort ascending.
        2. Drop weeks where every count is missing.
        3. Drop duplicate weeks, keeping the last.

    Individual NaN counts are deliberately preserved: the columns were
    introduced at different times (the 1990s rows carry `in_port` alone, and
    the PNW block starts later), so a missing count means the series did not
    exist that week — not that no vessel was there. Filling it with zero
    would publish an empty port.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    counts = ["loading", "waiting_to_load", "in_port", "loaded_7day", "due_10day"]
    present = [column for column in counts if column in df.columns]

    df = df.copy()
    df["week_ending"] = pd.to_datetime(df["week_ending"])
    if present:
        df = df.dropna(subset=present, how="all")
    df = df.drop_duplicates(subset=["week_ending"], keep="last")
    df = df.sort_values("week_ending").reset_index(drop=True)
    return df


def clean_worldbank(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean World Bank monthly price data.

    Steps:
        1. Ensure Date column is datetime.
        2. Sort by date.
        3. Drop rows with NaN prices.

    Returns cleaned copy (original is not mutated).
    """
    if df.empty:
        return df

    df = df.copy()
    df["Date"] = pd.to_datetime(df["Date"])
    df = df.sort_values("Date").reset_index(drop=True)
    df = df.dropna(subset=["price"])
    return df
