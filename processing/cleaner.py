"""
Data cleaning utilities.

Raw data from external APIs is messy — missing days (weekends/holidays),
occasional NaN values, inconsistent column names.  This module normalises
everything into a consistent format before it hits the database.

Key concepts for learning:
    - pandas .fillna(), .dropna(), .resample()
    - Forward-fill: carrying the last known price into a gap
    - logging.warning() flags unusual data (like long stretches of missing days)
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _validate_price_data(df: pd.DataFrame, label: str = ""):
    """
    Run sanity checks on price data and log warnings for suspicious values.

    Checks:
        - Daily close change >10% (possible data corruption or extreme event)
        - Zero or negative volume (missing data)

    These are warnings only — they don't block the pipeline.
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
    if "Volume" in df.columns:
        bad_volume = df[df["Volume"] <= 0]
        if not bad_volume.empty:
            logger.warning(
                "%s%d rows with zero/negative volume (possible data gap)",
                prefix, len(bad_volume),
            )


def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean a raw OHLCV DataFrame from yfinance.

    Steps:
        1. Drop rows where ALL price columns are NaN (total gaps).
        2. Forward-fill small gaps (e.g. a single NaN in Volume).
        3. Ensure the index is a proper DatetimeIndex named "Date".

    Parameters
    ----------
    df : pd.DataFrame
        Raw output from yfinance (Open, High, Low, Close, Volume).

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

    # Drop rows where every OHLC value is missing
    price_cols = ["Open", "High", "Low", "Close"]
    present = [c for c in price_cols if c in df.columns]
    df = df.dropna(subset=present, how="all")

    # Check for long consecutive NaN gaps before forward-filling
    for col in present:
        if df[col].isna().any():
            # Count consecutive NaN runs
            is_nan = df[col].isna()
            groups = (is_nan != is_nan.shift()).cumsum()
            nan_runs = is_nan.groupby(groups).sum()
            max_gap = int(nan_runs.max()) if not nan_runs.empty else 0
            if max_gap > 5:
                logger.warning(
                    "Column '%s' has a gap of %d consecutive missing days — "
                    "forward-fill may be masking a data issue",
                    col, max_gap,
                )

    # Forward-fill remaining small gaps (Volume can sometimes be NaN)
    df = df.ffill()

    # Run sanity checks (warnings only — doesn't block pipeline)
    _validate_price_data(df)

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
    series = series.ffill().dropna()
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
    df = df.ffill()
    return df


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


def clean_currencies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean currency OHLCV data — same logic as clean_ohlcv().

    Currency data from yfinance has the same format as commodity data,
    so we reuse the same cleaning steps: datetime index, drop full-NaN
    rows, forward-fill small gaps.

    Returns cleaned copy (original is not mutated).
    """
    return clean_ohlcv(df)


def clean_dce_futures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DCE futures data from AKShare.

    AKShare returns lowercase columns: date, open, high, low, close,
    volume, hold, settle.  We rename them to match our project conventions
    (capitalized names, Open_Interest instead of hold).

    Steps:
        1. Rename columns to project conventions.
        2. Parse Date to datetime and sort by date.
        3. Drop rows where all price columns are NaN.
        4. Forward-fill small gaps (same logic as clean_ohlcv).

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

    # Drop rows where all OHLC values are missing
    price_cols = ["Open", "High", "Low", "Close"]
    present = [c for c in price_cols if c in df.columns]
    df = df.dropna(subset=present, how="all")

    # Warn about long NaN gaps before forward-filling
    for col in present:
        if df[col].isna().any():
            is_nan = df[col].isna()
            groups = (is_nan != is_nan.shift()).cumsum()
            nan_runs = is_nan.groupby(groups).sum()
            max_gap = int(nan_runs.max()) if not nan_runs.empty else 0
            if max_gap > 5:
                logger.warning(
                    "DCE column '%s' has a gap of %d consecutive missing days — "
                    "forward-fill may be masking a data issue",
                    col, max_gap,
                )

    # Forward-fill remaining small gaps
    df = df.ffill()

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
