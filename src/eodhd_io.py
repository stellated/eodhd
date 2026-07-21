"""
eodhd_io.py
-----------
Round-trip helpers for EODHD daily and intraday OHLCV data between:
  CSV  <->  pandas DataFrame  <->  polars DataFrame  <->  SQLite

Also provides:
  - Network fetch functions that call EODHD's REST API directly
  - Database class: a caching SQLite wrapper that fetches from EODHD
    on demand when requested data is not already stored locally
  - tips(): populate a table with n1 days before / n2 days after each
    tip date, for backtesting a tipping newsletter

Column contract (daily)
-----------------------
code        str
timestamp   int64               UTC unix epoch of official market open for that session
datetime    datetime64[us]      tz-naive UTC  (time part is meaningful)
date        object              Python datetime.date  (local exchange date)
op hi lo cl ac  float64
vo          int64               (0 for padded rows)

Column contract (intraday)
--------------------------
code        str
timestamp   int64               UTC unix epoch (from EODHD Timestamp column)
datetime    datetime64[us]      tz-naive UTC
local_date  object              Python datetime.date  (date in exchange local timezone)
local_time  str                 "HH:MM:SS" in exchange local timezone (added by add_local_time())
op hi lo cl float64             (no ac for intraday)
vo          int64               (0 for padded rows)

SQLite storage
--------------
timestamp   INTEGER
datetime    TEXT  "YYYY-MM-DD HH:MM:SS"
date        TEXT  "YYYY-MM-DD"          (daily)
local_date  TEXT  "YYYY-MM-DD"          (intraday)
local_time  TEXT  "HH:MM:SS"            (intraday, optional — added by add_local_time())
Primary key: (code, timestamp)
INSERT OR REPLACE — re-importing is idempotent.
"""

from __future__ import annotations

import io
import logging
import pathlib
import re
import sqlite3
import warnings
from datetime import date, datetime, time, timedelta
from typing import Optional, Union, List, Tuple, Dict, Any
from zoneinfo import ZoneInfo

import exchange_calendars as ec
import pandas as pd
import polars as pl
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# Constants for exchange info
EXCHANGE_INFO: Dict[str, Dict[str, Union[str, time]]] = {
    "LSE": {
        "calendar": "XLON",
        "tz": "Europe/London",
        "open": time(8, 0),
        "close": time(16, 30),
    },
    "US": {
        "calendar": "XNYS",
        "tz": "America/New_York",
        "open": time(9, 30),
        "close": time(16, 0),
    },
    "AU": {
        "calendar": "XASX",
        "tz": "Australia/Sydney",
        "open": time(10, 0),
        "close": time(16, 0),
    },
}

# Default number of trading days to fetch when no date range is supplied
DEFAULT_N_DAYS: Dict[str, int] = {
    "1d": 60,
    "1m": 5,
    "5m": 10,
    "1h": 20,
}

# Default n1 / n2 for tips() (trading days before / after tip date)
DEFAULT_N1: int = 3
DEFAULT_N2: int = 10


# SQLite DDL constants
class SQL:
    DDL_DAILY = """
    CREATE TABLE IF NOT EXISTS {tablename} (
        code        TEXT    NOT NULL,
        timestamp   INTEGER NOT NULL,
        datetime    TEXT    NOT NULL,
        date        TEXT    NOT NULL,
        op          REAL,
        hi          REAL,
        lo          REAL,
        cl          REAL,
        ac          REAL,
        vo          INTEGER,
        PRIMARY KEY (code, timestamp)
    );
    """

    DDL_INTRADAY = """
    CREATE TABLE IF NOT EXISTS {tablename} (
        code        TEXT    NOT NULL,
        timestamp   INTEGER NOT NULL,
        datetime    TEXT    NOT NULL,
        local_date  TEXT    NOT NULL,
        op          REAL,
        hi          REAL,
        lo          REAL,
        cl          REAL,
        vo          INTEGER,
        PRIMARY KEY (code, timestamp)
    );
    """

    ALTER_ADD_LOCAL_TIME = """
    ALTER TABLE {tablename} ADD COLUMN local_time TEXT;
    """


# Cache open exchange_calendars objects (relatively expensive to construct)
_calendar_cache: Dict[str, ec.ExchangeCalendar] = {}


class EODHDError(Exception):
    """Custom exception for EODHD-related errors."""
    pass


class SQLiteError(Exception):
    """Custom exception for SQLite operations."""
    pass


def _get_calendar(suffix: str) -> ec.ExchangeCalendar:
    """Get or create an exchange calendar for the given suffix."""
    info = EXCHANGE_INFO[suffix]
    name = info["calendar"]
    if name not in _calendar_cache:
        _calendar_cache[name] = ec.get_calendar(name)
    return _calendar_cache[name]


def _suffix(code: str) -> str:
    """Extract exchange suffix from EODHD code, e.g. 'ANTO.LSE' -> 'LSE'."""
    parts = code.rsplit(".", 1)
    if len(parts) != 2 or parts[1] not in EXCHANGE_INFO:
        raise ValueError(
            f"Cannot determine exchange from code '{code}'. "
            f"Known suffixes: {list(EXCHANGE_INFO)}"
        )
    return parts[1]


def _session_open_ts(cal: ec.ExchangeCalendar, session: pd.Timestamp) -> int:
    """Return the UTC unix epoch of the market open for a given session."""
    return int(cal.schedule.loc[session, "open"].timestamp())


def _n_sessions_before(
    cal: ec.ExchangeCalendar,
    ref_date: date,
    n: int,
) -> date:
    """Return the session date that is n trading days before ref_date (inclusive)."""
    if n == 0:
        return ref_date
    ref_ts = pd.Timestamp(ref_date)
    lookback = cal.sessions_in_range(ref_ts - pd.Timedelta(days=n * 3), ref_ts)
    if len(lookback) < n + 1:
        return lookback[0].date()
    return lookback[-(n + 1)].date()


def _n_sessions_after(
    cal: ec.ExchangeCalendar,
    ref_date: date,
    n: int,
) -> date:
    """Return the session date that is n trading days after ref_date (inclusive)."""
    ref_ts = pd.Timestamp(ref_date)
    lookahead = cal.sessions_in_range(ref_ts, ref_ts + pd.Timedelta(days=n * 3))
    if len(lookahead) < n + 1:
        return lookahead[-1].date()
    return lookahead[n].date()


def _start_from_actual_dates(
    all_dates: List[date],
    end_date: date,
    n: int,
) -> date:
    """Given a sorted list of trading dates actually returned by EODHD,
    return the date that is n-1 positions before end_date (i.e. so that
    there are exactly n dates from start through end inclusive).
    """
    trading_dates = sorted(d for d in set(all_dates) if d <= end_date)
    if not trading_dates:
        return end_date
    if len(trading_dates) <= n:
        return trading_dates[0]
    return trading_dates[-n]


def _parse_ohlcv(df: pd.DataFrame, has_ac: bool) -> pd.DataFrame:
    """Rename and cast the common OHLCV columns."""
    rename = {
        "Open": "op",
        "High": "hi",
        "Low": "lo",
        "Close": "cl",
        "Volume": "vo",
    }
    if has_ac:
        rename["Adjusted_close"] = "ac"
    df = df.rename(columns=rename)
    for col in ["op", "hi", "lo", "cl"] + (["ac"] if has_ac else []):
        df[col] = df[col].astype("float64")
    df["vo"] = pd.to_numeric(df["vo"], errors="coerce").fillna(0).astype("int64")
    return df


def _interval_to_freq(interval: str) -> str:
    """Convert EODHD interval string to a pandas frequency alias."""
    interval = interval.strip().lower()
    if interval.endswith("m"):
        return f"{interval[:-1]}min"
    if interval.endswith("h"):
        return f"{interval[:-1]}h"
    raise ValueError(f"Unrecognised interval '{interval}'. Examples: '1m', '5m', '1h'")


def _is_intraday(interval: str) -> bool:
    """Check if the interval is intraday."""
    return interval != "1d"


def csv2pandas_daily(code: str, csv_path: pathlib.Path) -> pd.DataFrame:
    """Read an EODHD daily CSV and return a tidy pandas DataFrame.

    - Derives timestamp from the official UTC market open for each session
      (via exchange_calendars).
    - Pads missing trading days with zero volume and prices carried forward
      from the most recent real bar.
    - Clips rows earlier than the calendar's coverage start and warns.
    """
    suffix = _suffix(code)
    cal = _get_calendar(suffix)

    raw = pd.read_csv(csv_path)
    raw = _parse_ohlcv(raw, has_ac=True)

    # EODHD daily CSV date column is ISO format YYYY-MM-DD
    raw["date"] = pd.to_datetime(raw["Date"], format="%Y-%m-%d").dt.date
    raw = raw.drop(columns=["Date"])

    # Clip rows earlier than calendar coverage and warn
    first_covered = cal.first_session.date()
    n_before = len(raw)
    raw = raw[raw["date"] >= first_covered].reset_index(drop=True)
    n_dropped = n_before - len(raw)
    if n_dropped > 0:
        warnings.warn(
            f"{code}: dropped {n_dropped} row(s) dated before "
            f"{first_covered} (start of '{cal.name}' calendar coverage). "
            f"Earliest retained date: {raw['date'].min()}.",
            stacklevel=2,
        )
    if raw.empty:
        raise ValueError(
            f"{code}: no rows remain after clipping to calendar coverage "
            f"starting {first_covered}."
        )

    # Build full set of trading sessions spanning the CSV date range
    first_date = pd.Timestamp(raw["date"].min())
    last_date = pd.Timestamp(raw["date"].max())
    sessions = cal.sessions_in_range(first_date, last_date)
    schedule = cal.schedule.loc[sessions]

    open_ts = [int(schedule.loc[s, "open"].timestamp()) for s in sessions]

    skeleton = pd.DataFrame({
        "date": [s.date() for s in sessions],
        "timestamp": open_ts,
        "datetime": pd.to_datetime(open_ts, unit="s", utc=True)
                       .tz_localize(None).astype("datetime64[us]"),
    })

    merged = skeleton.merge(
        raw[["date", "op", "hi", "lo", "cl", "ac", "vo"]],
        on="date",
        how="left",
    )

    for col in ["op", "hi", "lo", "cl", "ac"]:
        merged[col] = merged[col].ffill()
    merged["vo"] = merged["vo"].fillna(0).astype("int64")
    merged["code"] = code
    merged["timestamp"] = merged["timestamp"].astype("int64")

    cols = ["code", "timestamp", "datetime", "date", "op", "hi", "lo", "cl", "ac", "vo"]
    return merged[cols].reset_index(drop=True)


def csv2pandas_intraday(
    code: str,
    csv_path: pathlib.Path,
    interval: str,
) -> pd.DataFrame:
    """Read an EODHD intraday CSV and return a tidy pandas DataFrame.

    - Drops Gmtoffset column (always zero; UTC is authoritative).
    - Derives local_date from timestamp + exchange timezone.
    - Pads missing bars between first and last bar of each day with zero
      volume and prices carried forward from the most recent real bar.
    """
    suffix = _suffix(code)
    tz = ZoneInfo(EXCHANGE_INFO[suffix]["tz"])
    freq = _interval_to_freq(interval)

    raw = pd.read_csv(csv_path)
    raw = _parse_ohlcv(raw, has_ac=False)

    raw["timestamp"] = raw["Timestamp"].astype("int64")
    raw["datetime"] = (
        pd.to_datetime(raw["timestamp"], unit="s", utc=True)
        .dt.tz_localize(None)
        .astype("datetime64[us]")
    )
    raw["local_date"] = (
        pd.to_datetime(raw["timestamp"], unit="s", utc=True)
        .dt.tz_convert(str(tz))
        .dt.date
    )
    raw = raw.drop(columns=["Timestamp", "Gmtoffset", "Datetime"])

    freq_seconds = int(
        pd.tseries.frequencies.to_offset(freq).nanos // 10**9
    )
    days = raw["local_date"].unique()
    padded_frames = []
    for day in days:
        day_df = raw[raw["local_date"] == day].copy()
        first_ts = int(day_df["timestamp"].min())
        last_ts = int(day_df["timestamp"].max())

        slot_ts = list(range(first_ts, last_ts + 1, freq_seconds))
        grid = pd.DataFrame({
            "timestamp": slot_ts,
            "datetime": pd.to_datetime(slot_ts, unit="s", utc=True)
                        .tz_localize(None).astype("datetime64[us]"),
            "local_date": day,
        })

        merged = grid.merge(
            day_df[["timestamp", "op", "hi", "lo", "cl", "vo"]],
            on="timestamp",
            how="left",
        )
        for col in ["op", "hi", "lo", "cl"]:
            merged[col] = merged[col].ffill()
        merged["vo"] = merged["vo"].fillna(0).astype("int64")
        padded_frames.append(merged)

    result = pd.concat(padded_frames, ignore_index=True)
    result["code"] = code

    cols = ["code", "timestamp", "datetime", "local_date", "op", "hi", "lo", "cl", "vo"]
    return result[cols].reset_index(drop=True)


def add_local_time(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add a local_time column (str "HH:MM:SS") to an intraday pandas DataFrame.

    The timezone is derived from the exchange suffix found in the code column.
    All rows must share the same exchange suffix — raises ValueError otherwise.
    The column is inserted immediately after local_date.
    """
    if "local_date" not in pdf.columns:
        raise ValueError(
            "add_local_time() requires an intraday DataFrame "
            "(daily DataFrames have no local_time)."
        )

    suffixes = pdf["code"].apply(_suffix).unique()
    if len(suffixes) > 1:
        raise ValueError(
            f"add_local_time() found mixed exchange suffixes: {sorted(suffixes)}. "
            "Split by suffix, call add_local_time() on each subset, "
            "then concatenate."
        )

    tz_name = EXCHANGE_INFO[suffixes[0]]["tz"]
    tz = ZoneInfo(tz_name)

    local_dt = pd.to_datetime(pdf["timestamp"], unit="s", utc=True).dt.tz_convert(str(tz))
    pdf = pdf.copy()
    pdf["local_time"] = local_dt.dt.strftime("%H:%M:%S")

    # Insert local_time after local_date
    cols = list(pdf.columns)
    cols.remove("local_time")
    ld_pos = cols.index("local_date")
    cols.insert(ld_pos + 1, "local_time")
    return pdf[cols]


def pandas2polars(pdf: pd.DataFrame) -> pl.DataFrame:
    """Convert a tidy pandas DataFrame (daily or intraday) to polars."""
    date_col = "date" if "date" in pdf.columns else "local_date"
    pdf2 = pdf.copy()
    pdf2[date_col] = pdf2[date_col].apply(
        lambda d: datetime(d.year, d.month, d.day) if isinstance(d, date) else d
    )

    df = pl.from_pandas(pdf2)
    df = df.with_columns([
        pl.col("datetime").cast(pl.Datetime("us")),
        pl.col(date_col).cast(pl.Datetime("us")).cast(pl.Date),
        pl.col("timestamp").cast(pl.Int64),
        pl.col("vo").cast(pl.Int64),
    ])
    return df


def polars2pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert a tidy polars DataFrame (daily or intraday) back to pandas."""
    pdf = df.to_pandas()
    date_col = "date" if "date" in pdf.columns else "local_date"
    pdf["datetime"] = pd.to_datetime(pdf["datetime"]).astype("datetime64[us]")
    pdf[date_col] = pd.to_datetime(pdf[date_col]).dt.date
    pdf["timestamp"] = pdf["timestamp"].astype("int64")
    pdf["vo"] = pdf["vo"].astype("int64")

    if "ac" in pdf.columns:
        base_cols = ["code", "timestamp", "datetime", "date",
                     "op", "hi", "lo", "cl", "ac", "vo"]
    else:
        base_cols = ["code", "timestamp", "datetime", "local_date",
                     "op", "hi", "lo", "cl", "vo"]

    # Preserve local_time if present
    if "local_time" in pdf.columns:
        ld_pos = base_cols.index("local_date")
        base_cols.insert(ld_pos + 1, "local_time")

    return pdf[base_cols].reset_index(drop=True)


def pandas2sqlite(
    pdf: pd.DataFrame,
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> None:
    """Write a tidy pandas DataFrame (daily or intraday) to SQLite using bulk inserts."""
    is_daily = "date" in pdf.columns
    has_lt = "local_time" in pdf.columns
    ddl = SQL.DDL_DAILY if is_daily else SQL.DDL_INTRADAY

    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        conn.execute(ddl.format(tablename=tablename))

        # Add local_time column if needed and not already present
        if has_lt and not is_daily:
            existing_columns = {
                row[1]
                for row in conn.execute(f"PRAGMA table_info({tablename})")
            }
            if "local_time" not in existing_columns:
                conn.execute(SQL.ALTER_ADD_LOCAL_TIME.format(tablename=tablename))

        # Use pandas.to_sql for bulk inserts
        pdf.to_sql(
            tablename,
            conn,
            if_exists="replace",
            index=False,
            method="multi",
        )
        conn.commit()
    except sqlite3.Error as e:
        logger.error("SQLite error in pandas2sqlite: %s", e)
        raise SQLiteError(f"Failed to write to SQLite: {e}")
    finally:
        if _own:
            conn.close()


def sqlite2pandas(
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> pd.DataFrame:
    """Read a SQLite table back into a tidy pandas DataFrame."""
    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        pdf = pd.read_sql(f"SELECT * FROM {tablename}", conn)  # noqa: S608
    except sqlite3.Error as e:
        logger.error("SQLite error in sqlite2pandas: %s", e)
        raise SQLiteError(f"Failed to read from SQLite: {e}")
    finally:
        if _own:
            conn.close()

    if pdf.empty:
        if "date" in [col[0] for col in conn.execute(f"PRAGMA table_info({tablename})").description]:
            pdf = pd.DataFrame(columns=[
                "code", "timestamp", "datetime", "date",
                "op", "hi", "lo", "cl", "ac", "vo"
            ])
        else:
            pdf = pd.DataFrame(columns=[
                "code", "timestamp", "datetime", "local_date",
                "op", "hi", "lo", "cl", "vo"
            ])

    pdf["datetime"] = pd.to_datetime(pdf["datetime"]).astype("datetime64[us]")
    pdf["timestamp"] = pdf["timestamp"].astype("int64")
    pdf["vo"] = pdf["vo"].astype("int64")

    has_lt = "local_time" in pdf.columns

    if "date" in pdf.columns:
        pdf["date"] = pd.to_datetime(pdf["date"]).dt.date
        cols = ["code", "timestamp", "datetime", "date",
                "op", "hi", "lo", "cl", "ac", "vo"]
    else:
        pdf["local_date"] = pd.to_datetime(pdf["local_date"]).dt.date
        cols = ["code", "timestamp", "datetime", "local_date",
                "op", "hi", "lo", "cl", "vo"]
        if has_lt:
            cols.insert(cols.index("local_date") + 1, "local_time")

    return pdf[cols].reset_index(drop=True)


def polars2sqlite(
    df: pl.DataFrame,
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> None:
    """Write a tidy polars DataFrame to SQLite (via pandas)."""
    pandas2sqlite(polars2pandas(df), db, tablename)


# EODHD network fetch functions
EODHD_BASE = "https://eodhd.com/api"


def _eodhd_fetch_csv(url: str) -> pd.DataFrame:
    """GET a URL that returns CSV and parse it into a DataFrame."""
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return pd.read_csv(io.StringIO(resp.text))
    except requests.RequestException as e:
        logger.error("Failed to fetch CSV from %s: %s", url, e)
        raise EODHDError(f"EODHD API request failed: {e}")


def fetch_daily(
    code: str,
    api_token: str,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV data from EODHD and return a tidy pandas DataFrame."""
    params = f"api_token={api_token}&fmt=csv&period=d"
    if from_date:
        params += f"&from={from_date.isoformat()}"
    if to_date:
        params += f"&to={to_date.isoformat()}"
    url = f"{EODHD_BASE}/eod/{code}?{params}"

    raw_csv = _eodhd_fetch_csv(url)
    buf = io.StringIO()
    raw_csv.to_csv(buf, index=False)
    buf.seek(0)
    return csv2pandas_daily(code, pathlib.Path(buf.name))  # type: ignore[arg-type]


def fetch_intraday(
    code: str,
    api_token: str,
    interval: str,
    from_ts: Optional[int] = None,
    to_ts: Optional[int] = None,
) -> pd.DataFrame:
    """Fetch intraday OHLCV data from EODHD and return a tidy pandas DataFrame."""
    params = f"api_token={api_token}&fmt=csv&interval={interval}"
    if from_ts:
        params += f"&from={from_ts}"
    if to_ts:
        params += f"&to={to_ts}"
    url = f"{EODHD_BASE}/intraday/{code}?{params}"

    raw_csv = _eodhd_fetch_csv(url)
    buf = io.StringIO()
    raw_csv.to_csv(buf, index=False)
    buf.seek(0)
    return csv2pandas_intraday(code, pathlib.Path(buf.name), interval)  # type: ignore[arg-type]


def _tips_daily(
    db: "Database",
    code: str,
    tip_date: date,
    tablename: str,
    n1: int,
    n2: int,
) -> None:
    """Fetch and store daily data around a tip date."""
    suffix = _suffix(code)
    cal = _get_calendar(suffix)

    cal_start = _n_sessions_before(cal, tip_date, n1)
    cal_end = _n_sessions_after(cal, tip_date, n2)

    pdf = fetch_daily(code, db.api_token, from_date=cal_start, to_date=cal_end)

    # Trim to actual dates
    actual_dates = sorted(d for d in pdf["date"].unique())
    start_idx = actual_dates.index(cal_start) if cal_start in actual_dates else 0
    end_idx = actual_dates.index(cal_end) if cal_end in actual_dates else len(actual_dates) - 1
    pdf = pdf.iloc[start_idx:end_idx + 1].reset_index(drop=True)

    if not pdf.empty:
        pandas2sqlite(pdf, db.conn, tablename)


def _tips_intraday(
    db: "Database",
    code: str,
    tip_date: date,
    tablename: str,
    interval: str,
    n1: int,
    n2: int,
) -> None:
    """Fetch and store intraday data around a tip date."""
    suffix = _suffix(code)
    cal = _get_calendar(suffix)

    cal_start = _n_sessions_before(cal, tip_date, n1)
    cal_end = _n_sessions_after(cal, tip_date, n2)

    # Convert dates to timestamps
    from_ts = int(datetime(cal_start.year, cal_start.month, cal_start.day).timestamp()) - 86400
    to_ts = int(datetime(cal_end.year, cal_end.month, cal_end.day, 23, 59, 59).timestamp()) + 86400

    pdf = fetch_intraday(code, db.api_token, interval, from_ts=from_ts, to_ts=to_ts)

    # Trim to actual dates
    start_date = tip_date - timedelta(days=n1)
    end_date = tip_date + timedelta(days=n2)

    pdf = pdf[
        (pdf["local_date"] >= start_date) &
        (pdf["local_date"] <= end_date)
    ].reset_index(drop=True)

    if not pdf.empty:
        pandas2sqlite(pdf, db.conn, tablename)


def tips(
    db: "Database",
    tip_list: List[Tuple[str, date]],
    tablename: str,
    interval: str,
    n1: Optional[int] = None,
    n2: Optional[int] = None,
) -> None:
    """Populate a database table with data around each tip date.

    For each (code, tip_date) in tip_list, fetches n1 trading days before
    tip_date through n2 trading days after tip_date and stores the data in
    tablename. Uses INSERT OR REPLACE so repeated calls are idempotent and
    safe to run as a daily scheduled job.
    """
    if db.api_token is None:
        raise ValueError(
            "Database.api_token must be set to fetch from EODHD. "
            "Pass api_token= to Database.__init__()."
        )

    n1 = n1 if n1 is not None else DEFAULT_N1
    n2 = n2 if n2 is not None else DEFAULT_N2

    for code, tip_date in tip_list:
        try:
            if _is_intraday(interval):
                _tips_intraday(db, code, tip_date, tablename, interval, n1, n2)
            else:
                _tips_daily(db, code, tip_date, tablename, n1, n2)
        except Exception as e:
            logger.error("Failed to process tip for %s on %s: %s", code, tip_date, e)
            continue


class Database:
    """SQLite-backed local cache for EODHD OHLCV data.

    Fetches from EODHD on demand when requested data is not already stored.

    Usage (context manager — recommended):
        with Database("market.db", api_token=os.getenv("EODHD_API_TOKEN")) as db:
            db.from_csv("ANTO.LSE", Path("anto.csv"), interval="1d", tablename="daily_lse")
            db.fetch("AAPL.US", interval="5m", tablename="intraday_5m")
            pdf = db.to_pandas("AAPL.US", interval="5m", tablename="intraday_5m", n_days=10)
    """

    def __init__(
        self,
        db_path: Union[str, pathlib.Path],
        api_token: Optional[str] = None,
    ) -> None:
        self.db_path = pathlib.Path(db_path)
        self.api_token = api_token
        self.conn = sqlite3.connect(self.db_path)

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return False

    def close(self) -> None:
        """Close the SQLite connection."""
        self.conn.close()

    def _require_token(self) -> str:
        """Ensure an API token is set for EODHD operations."""
        if not self.api_token:
            raise ValueError(
                "api_token is required for EODHD network operations. "
                "Pass api_token= to Database.__init__()."
            )
        return self.api_token

    def _table_exists(self, tablename: str) -> bool:
        """Check if a table exists in the database."""
        cur = self.conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
            (tablename,),
        )
        return cur.fetchone() is not None

    def _date_range_in_table(
        self,
        tablename: str,
        code: str,
        is_daily: bool,
    ) -> Tuple[Optional[date], Optional[date]]:
        """Return (min_date, max_date) for code in tablename, or (None, None)."""
        if not self._table_exists(tablename):
            return None, None
        date_col = "date" if is_daily else "local_date"
        cur = self.conn.execute(
            f"SELECT MIN({date_col}), MAX({date_col}) "  # noqa: S608
            f"FROM {tablename} WHERE code = ?",
            (code,),
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return None, None
        return (
            date.fromisoformat(row[0]),
            date.fromisoformat(row[1]),
        )

    def from_csv(
        self,
        code: str,
        csv_path: pathlib.Path,
        interval: str,
        tablename: str,
    ) -> None:
        """Ingest an EODHD CSV file into the database."""
        if _is_intraday(interval):
            pdf = csv2pandas_intraday(code, csv_path, interval)
        else:
            pdf = csv2pandas_daily(code, csv_path)
        pandas2sqlite(pdf, self.conn, tablename)

    def from_pandas(self, pdf: pd.DataFrame, tablename: str) -> None:
        """Ingest a pandas DataFrame into the database."""
        pandas2sqlite(pdf, self.conn, tablename)

    def from_polars(self, df: pl.DataFrame, tablename: str) -> None:
        """Ingest a polars DataFrame into the database."""
        polars2sqlite(df, self.conn, tablename)

    def fetch(
        self,
        code: str,
        interval: str,
        tablename: str,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None,
    ) -> None:
        """Fetch data from EODHD and store it in the database."""
        if _is_intraday(interval):
            from_ts = int(datetime(from_date.year, from_date.month, from_date.day).timestamp()) if from_date else None
            to_ts = int(datetime(to_date.year, to_date.month, to_date.day).timestamp()) if to_date else None
            pdf = fetch_intraday(code, self._require_token(), interval, from_ts=from_ts, to_ts=to_ts)
        else:
            pdf = fetch_daily(code, self._require_token(), from_date=from_date, to_date=to_date)
        pandas2sqlite(pdf, self.conn, tablename)

    def to_pandas(
        self,
        code: str,
        interval: str,
        tablename: str,
        n_days: Optional[int] = None,
    ) -> pd.DataFrame:
        """Retrieve data from the database as a pandas DataFrame."""
        if not self._table_exists(tablename):
            raise ValueError(f"Table {tablename} does not exist.")

        is_daily = interval == "1d"
        min_date, max_date = self._date_range_in_table(tablename, code, is_daily)

        if n_days is None:
            n_days = DEFAULT_N_DAYS.get(interval, 10)

        if min_date is None or max_date is None:
            # Fetch from EODHD if no data exists
            self.fetch(code, interval, tablename)
            return self.to_pandas(code, interval, tablename, n_days)

        # Check if we have enough data
        date_col = "date" if is_daily else "local_date"
        query = f"SELECT * FROM {tablename} WHERE code = ? ORDER BY {date_col} DESC LIMIT ?"
        pdf = pd.read_sql(query, self.conn, params=(code, n_days))

        if len(pdf) >= n_days:
            return pdf
        else:
            # Fetch missing data from EODHD
            self.fetch(code, interval, tablename)
            return self.to_pandas(code, interval, tablename, n_days)