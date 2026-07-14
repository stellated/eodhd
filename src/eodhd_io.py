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
import pathlib
import re
import sqlite3
import warnings
from datetime import date, datetime, time, timedelta
from typing import Optional, Union
from zoneinfo import ZoneInfo

import exchange_calendars as ec
import pandas as pd
import polars as pl
import requests

# ---------------------------------------------------------------------------
# Exchange reference table
# ---------------------------------------------------------------------------
# Keys are the EODHD exchange suffixes (without the dot).
# calendar: exchange_calendars name
# tz:       IANA timezone name (for local_date / local_time derivation)
# open/close: local exchange times — documentation only; authoritative UTC
#             open/close times always come from exchange_calendars at runtime.
#
# To add a new exchange, append one entry here.

EXCHANGE_INFO: dict[str, dict] = {
    "LSE": {
        "calendar": "XLON",
        "tz":       "Europe/London",
        "open":     time(8,  0),
        "close":    time(16, 30),
    },
    "US": {
        "calendar": "XNYS",
        "tz":       "America/New_York",
        "open":     time(9,  30),
        "close":    time(16,  0),
    },
    "AU": {
        "calendar": "XASX",
        "tz":       "Australia/Sydney",
        "open":     time(10,  0),
        "close":    time(16,  0),
    },
}

# Default number of trading days to fetch when no date range is supplied.
# Keys are EODHD interval strings; value is number of trading days.
DEFAULT_N_DAYS: dict[str, int] = {
    "1d":  60,
    "1m":   5,
    "5m":  10,
    "1h":  20,
}

# Default n1 / n2 for tips() (trading days before / after tip date).
DEFAULT_N1: int = 3
DEFAULT_N2: int = 10

# Cache open exchange_calendars objects (relatively expensive to construct)
_calendar_cache: dict[str, ec.ExchangeCalendar] = {}


def _get_calendar(suffix: str) -> ec.ExchangeCalendar:
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
    """Return the session date that is n trading days before ref_date (inclusive).

    n=0 returns ref_date itself (start == end == today for n_days=1).
    """
    # CHANGED: guard against n=0 (e.g. n_days=1 in to_pandas passes n-1=0)
    if n == 0:
        return ref_date
    ref_ts   = pd.Timestamp(ref_date)
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
    ref_ts   = pd.Timestamp(ref_date)
    lookahead = cal.sessions_in_range(ref_ts, ref_ts + pd.Timedelta(days=n * 3))
    if len(lookahead) < n + 1:
        return lookahead[-1].date()
    return lookahead[n].date()


# ---------------------------------------------------------------------------
# SQLite DDL
# ---------------------------------------------------------------------------

_DDL_DAILY = """
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

_DDL_INTRADAY = """
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

_INSERT_DAILY = """
INSERT OR REPLACE INTO {tablename}
    (code, timestamp, datetime, date, op, hi, lo, cl, ac, vo)
VALUES
    (:code, :timestamp, :datetime, :date, :op, :hi, :lo, :cl, :ac, :vo);
"""

_INSERT_INTRADAY = """
INSERT OR REPLACE INTO {tablename}
    (code, timestamp, datetime, local_date, op, hi, lo, cl, vo)
VALUES
    (:code, :timestamp, :datetime, :local_date, :op, :hi, :lo, :cl, :vo);
"""

# ---------------------------------------------------------------------------
# Shared private helpers
# ---------------------------------------------------------------------------

def _parse_ohlcv(df: pd.DataFrame, has_ac: bool) -> pd.DataFrame:
    """Rename and cast the common OHLCV columns."""
    rename = {
        "Open":  "op",
        "High":  "hi",
        "Low":   "lo",
        "Close": "cl",
        "Volume":"vo",
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
    return interval != "1d"


def _start_from_actual_dates(
    all_dates: list,
    end_date: date,
    n: int,
) -> date:
    """Given a sorted list of trading dates actually returned by EODHD,
    return the date that is n-1 positions before end_date (i.e. so that
    there are exactly n dates from start through end inclusive).

    Unlike _n_sessions_before(), this counts dates that EODHD actually
    provided — including half-days that exchange_calendars omits.

    If fewer than n dates are available, returns the earliest date.
    """
    # CHANGED: used to derive start from exchange_calendars sessions, which
    # omits half-days (e.g. July 3rd before July 4th holiday).  Now we count
    # back through dates that EODHD actually returned.
    trading_dates = sorted(d for d in set(all_dates) if d <= end_date)
    if not trading_dates:
        return end_date
    if len(trading_dates) <= n:
        return trading_dates[0]
    return trading_dates[-n]


# ---------------------------------------------------------------------------
# 1a. csv2pandas_daily
# ---------------------------------------------------------------------------

def csv2pandas_daily(code: str, csv_path: pathlib.Path) -> pd.DataFrame:
    """Read an EODHD daily CSV and return a tidy pandas DataFrame.

    - Derives timestamp from the official UTC market open for each session
      (via exchange_calendars).
    - Pads missing trading days with zero volume and prices carried forward
      from the most recent real bar.
    - Clips rows earlier than the calendar's coverage start and warns.

    Columns: code, timestamp, datetime, date, op, hi, lo, cl, ac, vo
    """
    suffix = _suffix(code)
    cal    = _get_calendar(suffix)

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
    last_date  = pd.Timestamp(raw["date"].max())
    sessions   = cal.sessions_in_range(first_date, last_date)
    schedule   = cal.schedule.loc[sessions]

    open_ts = [int(schedule.loc[s, "open"].timestamp()) for s in sessions]

    skeleton = pd.DataFrame({
        "date":      [s.date() for s in sessions],
        "timestamp": open_ts,
        "datetime":  pd.to_datetime(open_ts, unit="s", utc=True)
                       .tz_localize(None).astype("datetime64[us]"),
    })

    merged = skeleton.merge(
        raw[["date", "op", "hi", "lo", "cl", "ac", "vo"]],
        on="date",
        how="left",
    )

    for col in ["op", "hi", "lo", "cl", "ac"]:
        merged[col] = merged[col].ffill()
    merged["vo"]        = merged["vo"].fillna(0).astype("int64")
    merged["code"]      = code
    merged["timestamp"] = merged["timestamp"].astype("int64")

    cols = ["code", "timestamp", "datetime", "date", "op", "hi", "lo", "cl", "ac", "vo"]
    return merged[cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 1b. csv2pandas_intraday
# ---------------------------------------------------------------------------

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

    Columns: code, timestamp, datetime, local_date, op, hi, lo, cl, vo
    """
    suffix = _suffix(code)
    tz     = ZoneInfo(EXCHANGE_INFO[suffix]["tz"])
    freq   = _interval_to_freq(interval)

    raw = pd.read_csv(csv_path)
    raw = _parse_ohlcv(raw, has_ac=False)

    raw["timestamp"]  = raw["Timestamp"].astype("int64")
    raw["datetime"]   = (pd.to_datetime(raw["timestamp"], unit="s", utc=True)
                         .dt.tz_localize(None).astype("datetime64[us]"))
    raw["local_date"] = (pd.to_datetime(raw["timestamp"], unit="s", utc=True)
                         .dt.tz_convert(str(tz)).dt.date)
    raw = raw.drop(columns=["Timestamp", "Gmtoffset", "Datetime"])

    freq_seconds = int(
        pd.tseries.frequencies.to_offset(freq).nanos // 10**9
    )
    days = raw["local_date"].unique()
    padded_frames = []
    for day in days:
        day_df   = raw[raw["local_date"] == day].copy()
        first_ts = int(day_df["timestamp"].min())
        last_ts  = int(day_df["timestamp"].max())

        slot_ts = list(range(first_ts, last_ts + 1, freq_seconds))
        grid = pd.DataFrame({
            "timestamp":  slot_ts,
            "datetime":   pd.to_datetime(slot_ts, unit="s", utc=True)
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

    result       = pd.concat(padded_frames, ignore_index=True)
    result["code"] = code

    cols = ["code", "timestamp", "datetime", "local_date", "op", "hi", "lo", "cl", "vo"]
    return result[cols].reset_index(drop=True)


# ---------------------------------------------------------------------------
# NEW: add_local_time()
# ---------------------------------------------------------------------------

def add_local_time(pdf: pd.DataFrame) -> pd.DataFrame:
    """Add a local_time column (str "HH:MM:SS") to an intraday pandas DataFrame.

    The timezone is derived from the exchange suffix found in the code column.
    All rows must share the same exchange suffix — raises ValueError otherwise.
    The column is inserted immediately after local_date.

    This function is intentionally separate from csv2pandas_intraday so that
    mixed-exchange DataFrames can be assembled by:
        1. Split by exchange suffix
        2. Call add_local_time() on each subset
        3. Concatenate the results

    Daily DataFrames are not supported (they have no meaningful intraday time).
    """
    if "local_date" not in pdf.columns:
        raise ValueError("add_local_time() requires an intraday DataFrame "
                         "(daily DataFrames have no local_time).")

    suffixes = pdf["code"].apply(_suffix).unique()
    if len(suffixes) > 1:
        raise ValueError(
            f"add_local_time() found mixed exchange suffixes: {sorted(suffixes)}. "
            "Split by suffix, call add_local_time() on each subset, "
            "then concatenate."
        )

    tz_name = EXCHANGE_INFO[suffixes[0]]["tz"]
    tz      = ZoneInfo(tz_name)

    local_dt = pd.to_datetime(pdf["timestamp"], unit="s", utc=True).dt.tz_convert(str(tz))
    pdf = pdf.copy()
    pdf["local_time"] = local_dt.dt.strftime("%H:%M:%S")

    # Insert local_time after local_date
    cols = list(pdf.columns)
    cols.remove("local_time")
    ld_pos = cols.index("local_date")
    cols.insert(ld_pos + 1, "local_time")
    return pdf[cols]


# ---------------------------------------------------------------------------
# 2. pandas2polars
# ---------------------------------------------------------------------------

def pandas2polars(pdf: pd.DataFrame) -> pl.DataFrame:
    """Convert a tidy pandas DataFrame (daily or intraday) to polars.

    datetime        -> pl.Datetime("us")
    date/local_date -> pl.Date
    local_time      -> pl.Utf8 (if present)
    """
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


# ---------------------------------------------------------------------------
# 3. polars2pandas
# ---------------------------------------------------------------------------

def polars2pandas(df: pl.DataFrame) -> pd.DataFrame:
    """Convert a tidy polars DataFrame (daily or intraday) back to pandas."""
    pdf = df.to_pandas()
    date_col = "date" if "date" in pdf.columns else "local_date"
    pdf["datetime"]  = pd.to_datetime(pdf["datetime"]).astype("datetime64[us]")
    pdf[date_col]    = pd.to_datetime(pdf[date_col]).dt.date
    pdf["timestamp"] = pdf["timestamp"].astype("int64")
    pdf["vo"]        = pdf["vo"].astype("int64")

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


# ---------------------------------------------------------------------------
# 4. pandas2sqlite
# ---------------------------------------------------------------------------

def pandas2sqlite(
    pdf: pd.DataFrame,
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> None:
    """Write a tidy pandas DataFrame (daily or intraday) to SQLite.

    Creates the table if it doesn't exist.  Uses INSERT OR REPLACE so the
    operation is idempotent.  local_time, if present, is stored as an extra
    TEXT column added via ALTER TABLE when first encountered.
    """
    is_daily = "date" in pdf.columns
    has_lt   = "local_time" in pdf.columns
    ddl      = _DDL_DAILY    if is_daily else _DDL_INTRADAY
    insert   = _INSERT_DAILY if is_daily else _INSERT_INTRADAY

    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        conn.execute(ddl.format(tablename=tablename))

        # Add local_time column if needed and not already present
        if has_lt and not is_daily:
            existing = {
                row[1]
                for row in conn.execute(f"PRAGMA table_info({tablename})")
            }
            if "local_time" not in existing:
                conn.execute(
                    f"ALTER TABLE {tablename} ADD COLUMN local_time TEXT"
                )

        rows = []
        for row in pdf.itertuples(index=False):
            d = dict(row._asdict())
            d["datetime"] = row.datetime.strftime("%Y-%m-%d %H:%M:%S")
            if is_daily:
                d["date"] = (row.date.strftime("%Y-%m-%d")
                             if isinstance(row.date, date) else str(row.date))
            else:
                d["local_date"] = (row.local_date.strftime("%Y-%m-%d")
                                   if isinstance(row.local_date, date)
                                   else str(row.local_date))
            d["vo"] = int(row.vo)
            rows.append(d)

        if has_lt and not is_daily:
            insert_lt = insert.rstrip(";").replace(
                "op, hi, lo, cl, vo)",
                "op, hi, lo, cl, vo, local_time)"
            ).replace(
                ":op, :hi, :lo, :cl, :vo);",
                ":op, :hi, :lo, :cl, :vo, :local_time);"
            ) + ";"
            # Rebuild insert to include local_time
            insert_lt = f"""
INSERT OR REPLACE INTO {{tablename}}
    (code, timestamp, datetime, local_date, op, hi, lo, cl, vo, local_time)
VALUES
    (:code, :timestamp, :datetime, :local_date, :op, :hi, :lo, :cl, :vo, :local_time);
"""
            conn.executemany(insert_lt.format(tablename=tablename), rows)
        else:
            conn.executemany(insert.format(tablename=tablename), rows)

        conn.commit()
    finally:
        if _own:
            conn.close()


# ---------------------------------------------------------------------------
# 5. sqlite2pandas
# ---------------------------------------------------------------------------

def sqlite2pandas(
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> pd.DataFrame:
    """Read a SQLite table back into a tidy pandas DataFrame."""
    _own = not isinstance(db, sqlite3.Connection)
    conn = sqlite3.connect(db) if _own else db
    try:
        pdf = pd.read_sql(f"SELECT * FROM {tablename}", conn)  # noqa: S608
    finally:
        if _own:
            conn.close()

    pdf["datetime"]  = pd.to_datetime(pdf["datetime"]).astype("datetime64[us]")
    pdf["timestamp"] = pdf["timestamp"].astype("int64")
    pdf["vo"]        = pdf["vo"].astype("int64")

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


# ---------------------------------------------------------------------------
# 6. polars2sqlite
# ---------------------------------------------------------------------------

def polars2sqlite(
    df: pl.DataFrame,
    db: Union[sqlite3.Connection, str, pathlib.Path],
    tablename: str,
) -> None:
    """Write a tidy polars DataFrame to SQLite (via pandas)."""
    pandas2sqlite(polars2pandas(df), db, tablename)


# ---------------------------------------------------------------------------
# NEW: EODHD network fetch functions
# ---------------------------------------------------------------------------

EODHD_BASE = "https://eodhd.com/api"


def _eodhd_fetch_csv(url: str) -> pd.DataFrame:
    """GET a URL that returns CSV and parse it into a DataFrame."""
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def fetch_daily(
    code: str,
    api_token: str,
    from_date: Optional[date] = None,
    to_date:   Optional[date] = None,
) -> pd.DataFrame:
    """Fetch daily OHLCV data from EODHD and return a tidy pandas DataFrame.

    from_date / to_date are Python date objects (YYYY-MM-DD sent to API).
    If omitted, EODHD returns its full history for the ticker.
    The returned DataFrame is identical in schema to csv2pandas_daily().
    """
    params = f"api_token={api_token}&fmt=csv&period=d"
    if from_date:
        params += f"&from={from_date.isoformat()}"
    if to_date:
        params += f"&to={to_date.isoformat()}"
    url = f"{EODHD_BASE}/eod/{code}?{params}"

    raw_csv = _eodhd_fetch_csv(url)
    # Write to a temp buffer so csv2pandas_daily can process it normally
    buf = io.StringIO()
    raw_csv.to_csv(buf, index=False)
    buf.seek(0)
    return csv2pandas_daily(code, buf)  # type: ignore[arg-type]


def fetch_intraday(
    code: str,
    api_token: str,
    interval: str,
    from_ts: Optional[int] = None,
    to_ts:   Optional[int] = None,
) -> pd.DataFrame:
    """Fetch intraday OHLCV data from EODHD and return a tidy pandas DataFrame.

    from_ts / to_ts are Unix timestamps (EODHD intraday uses epoch, not dates).
    If omitted, EODHD returns the last 120 days.
    The returned DataFrame is identical in schema to csv2pandas_intraday().
    """
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
    return csv2pandas_intraday(code, buf, interval)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# NEW: tips()
# ---------------------------------------------------------------------------

def tips(
    db: "Database",
    tip_list: list[tuple[str, date]],
    tablename: str,
    interval: str,
    n1: Optional[int] = None,
    n2: Optional[int] = None,
) -> None:
    """Populate a database table with data around each tip date.

    For each (code, tip_date) in tip_list, fetches n1 trading days before
    tip_date through n2 trading days after tip_date and stores the data in
    tablename.  Uses INSERT OR REPLACE so repeated calls are idempotent and
    safe to run as a daily scheduled job.

    Parameters
    ----------
    db          : Database instance (must have api_token set)
    tip_list    : list of (code, tip_date) tuples
    tablename   : SQLite table to write into
    interval    : "1d", "5m", etc.
    n1          : trading days before tip_date  (default: DEFAULT_N1)
    n2          : trading days after tip_date   (default: DEFAULT_N2)
    """
    if db.api_token is None:
        raise ValueError(
            "Database.api_token must be set to fetch from EODHD. "
            "Pass api_token= to Database.__init__()."
        )

    n1 = n1 if n1 is not None else DEFAULT_N1
    n2 = n2 if n2 is not None else DEFAULT_N2

    for code, tip_date in tip_list:
        suffix = _suffix(code)
        cal    = _get_calendar(suffix)

        # CHANGED: use calendar-day buffers for the fetch window instead of
        # session counts.  exchange_calendars omits half-days (e.g. July 3rd
        # before Independence Day), so session-based end_date would miss them.
        # We fetch a wide calendar-day window and trim after using actual dates.
        cal_start = _n_sessions_before(cal, tip_date, n1)
        # Go wide on the end: n2 sessions * 2 calendar days is always enough
        # to cover any half-days or long weekends.
        cal_end   = tip_date + timedelta(days=n2 * 2 + 5)

        if _is_intraday(interval):
            from_ts = int(datetime(
                cal_start.year, cal_start.month, cal_start.day
            ).timestamp()) - 86400
            to_ts = int(datetime(
                cal_end.year, cal_end.month, cal_end.day, 23, 59, 59
            ).timestamp()) + 86400

            pdf = fetch_intraday(code, db.api_token, interval,
                                 from_ts=from_ts, to_ts=to_ts)
            # Trim: keep cal_start through the nth actual trading date after tip
            actual_after = sorted(
                d for d in pdf["local_date"].unique() if d >= tip_date
            )
            end_date = actual_after[n2] if len(actual_after) > n2 else (
                actual_after[-1] if actual_after else cal_end
            )
            pdf = pdf[
                (pdf["local_date"] >= cal_start) &
                (pdf["local_date"] <= end_date)
            ].reset_index(drop=True)
        else:
            pdf = fetch_daily(code, db.api_token,
                              from_date=cal_start, to_date=cal_end)
            # Trim daily: keep cal_start through nth actual date after tip
            actual_after = sorted(
                d for d in pdf["date"].unique() if d >= tip_date
            )
            end_date = actual_after[n2] if len(actual_after) > n2 else (
                actual_after[-1] if actual_after else cal_end
            )
            pdf = pdf[
                (pdf["date"] >= cal_start) &
                (pdf["date"] <= end_date)
            ].reset_index(drop=True)

        if not pdf.empty:
            pandas2sqlite(pdf, db.conn, tablename)


# ---------------------------------------------------------------------------
# Database class
# ---------------------------------------------------------------------------

class Database:
    """SQLite-backed local cache for EODHD OHLCV data.

    Fetches from EODHD on demand when requested data is not already stored.

    Usage (context manager — recommended):

        with Database("market.db", api_token=os.getenv("EODHD_API_TOKEN")) as db:
            # Seed from a CSV file already on disk
            db.from_csv("ANTO.LSE", Path("anto.csv"), interval="1d",
                        tablename="daily_lse")

            # Fetch directly from EODHD into the cache
            db.fetch("AAPL.US", interval="5m", tablename="intraday_5m")

            # Retrieve (fetches from EODHD if not cached)
            pdf = db.to_pandas("AAPL.US", interval="5m",
                               tablename="intraday_5m", n_days=10)

    Default n_days per interval can be overridden by editing DEFAULT_N_DAYS.

    api_token is optional if you only use from_csv / from_pandas / from_polars
    and never need to hit the EODHD API.
    """

    def __init__(
        self,
        db_path:   Union[str, pathlib.Path],
        api_token: Optional[str] = None,
    ) -> None:
        self.db_path   = pathlib.Path(db_path)
        self.api_token = api_token
        self.conn      = sqlite3.connect(self.db_path)

    # --- context manager ---------------------------------------------------

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
        return False

    def close(self) -> None:
        self.conn.close()

    # --- private helpers ---------------------------------------------------

    def _require_token(self) -> str:
        if not self.api_token:
            raise ValueError(
                "api_token is required for EODHD network operations. "
                "Pass api_token= to Database.__init__()."
            )
        return self.api_token

    def _table_exists(self, tablename: str) -> bool:
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
    ) -> tuple[Optional[date], Optional[date]]:
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

    # --- CSV / DataFrame ingestion (unchanged from v1) ---------------------

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
        pandas2sqlite(pdf, self.conn, tablename)

    def from_polars(self, df: pl.DataFrame, tablename: str) -> None:
        polars2sqlite(df, self.conn, tablename)

    # --- EODHD fetch (new in v2) ------------------------------------------

    def fetch(
        self,
        code: str,
        interval: str,
        tablename: str,
        from_date: Optional[date] = None,
        to_date:   Optional[date] = None,
    ) -> None:
        """Fetch data from EODHD and store it in the database.

        from_date / to_date are Python date objects.  If omitted, EODHD
        returns its full default window (last 120 days for intraday).
        """
        token = self._require_token()
        if _is_intraday(interval):
            from_ts = (int(datetime(from_date.year, from_date.month,
                                    from_date.day).timestamp())
                       if from_date else None)
            to_ts   = (int(datetime(to_date.year, to_date.month,
                                    to_date.day, 23, 59, 59).timestamp())
                       if to_date else None)
            pdf = fetch_intraday(code, token, interval,
                                 from_ts=from_ts, to_ts=to_ts)
        else:
            pdf = fetch_daily(code, token,
                              from_date=from_date, to_date=to_date)
        pandas2sqlite(pdf, self.conn, tablename)

    # --- Retrieval with optional auto-fetch (new in v2) -------------------

    def to_pandas(
        self,
        tablename: str,
        code:      Optional[str] = None,
        interval:  Optional[str] = None,
        start:     Optional[date] = None,
        end:       Optional[date] = None,
        n_days:    Optional[int] = None,
    ) -> pd.DataFrame:
        """Return table contents as a pandas DataFrame.

        If code and interval are supplied, the method checks whether the
        requested date range is present in the database and fetches from
        EODHD if not.

        Parameters
        ----------
        tablename : SQLite table name
        code      : EODHD ticker (required for auto-fetch)
        interval  : "1d", "5m", etc. (required for auto-fetch)
        start     : first date to return (Python date)
        end       : last date to return (Python date); defaults to today
        n_days    : number of trading days to return when start is omitted.
                    Falls back to DEFAULT_N_DAYS[interval] if also omitted.
        """
        # If enough info is given, check cache and fetch if needed
        if code and interval and self.api_token:
            is_daily = not _is_intraday(interval)

            # CHANGED: do not default end to date.today().
            # Today's data may not yet exist (market not yet open, holiday,
            # weekend), which caused n_days=1 to return an empty DataFrame.
            # Instead, fetch up to today so the cache is current, then
            # resolve end to the latest date actually in the table.
            fetch_end = end or date.today()

            if start is None:
                n      = n_days or DEFAULT_N_DAYS.get(interval, 30)
                suffix = _suffix(code)
                cal    = _get_calendar(suffix)
                # Temporarily set start wide enough to cover n days before
                # fetch_end; we'll re-derive start after the cache is warm.
                start = _n_sessions_before(cal, fetch_end, n - 1)

            cached_min, cached_max = self._date_range_in_table(
                tablename, code, is_daily
            )

            needs_fetch = (
                cached_min is None              # nothing cached yet
                or start < cached_min           # need earlier data
                or fetch_end > cached_max       # need later data
            )

            if needs_fetch:
                # Wide fetch: request a generous window around start/fetch_end
                # to avoid multiple small fetches; INSERT OR REPLACE handles
                # overlap with existing rows.
                if _is_intraday(interval):
                    from_ts = int(datetime(
                        start.year, start.month, start.day
                    ).timestamp()) - 86400
                    to_ts = int(datetime(
                        fetch_end.year, fetch_end.month, fetch_end.day, 23, 59, 59
                    ).timestamp()) + 86400
                    pdf = fetch_intraday(code, self.api_token, interval,
                                         from_ts=from_ts, to_ts=to_ts)
                else:
                    pdf = fetch_daily(code, self.api_token,
                                      from_date=start, to_date=fetch_end)
                pandas2sqlite(pdf, self.conn, tablename)

            # Re-derive end and start from dates actually in the table.
            # CHANGED: use _start_from_actual_dates() instead of
            # _n_sessions_before() so half-days are counted correctly.
            _, cached_max = self._date_range_in_table(tablename, code, is_daily)
            end = min(fetch_end, cached_max) if cached_max else fetch_end
            if n_days or (start is None):
                date_col = "date" if is_daily else "local_date"
                rows = self.conn.execute(
                    f"SELECT DISTINCT {date_col} FROM {tablename} "
                    f"WHERE code = ? AND {date_col} <= ? ORDER BY {date_col}",
                    (code, end.isoformat()),
                ).fetchall()
                actual_dates = [date.fromisoformat(r[0]) for r in rows]
                start = _start_from_actual_dates(actual_dates, end, n)

            cached_min, cached_max = self._date_range_in_table(
                tablename, code, is_daily
            )

            needs_fetch = (
                cached_min is None           # nothing cached yet
                or start < cached_min        # need earlier data
                or end   > cached_max        # need later data
            )

            if needs_fetch:
                # Wide fetch: request a generous window around start/end
                # to avoid multiple small fetches; INSERT OR REPLACE handles
                # overlap with existing rows.
                if _is_intraday(interval):
                    from_ts = int(datetime(
                        start.year, start.month, start.day
                    ).timestamp()) - 86400
                    to_ts = int(datetime(
                        end.year, end.month, end.day, 23, 59, 59
                    ).timestamp()) + 86400
                    pdf = fetch_intraday(code, self.api_token, interval,
                                         from_ts=from_ts, to_ts=to_ts)
                else:
                    pdf = fetch_daily(code, self.api_token,
                                      from_date=start, to_date=end)
                pandas2sqlite(pdf, self.conn, tablename)

        # Read from SQLite, applying date filter if requested.
        # CHANGED: detect date column from the actual table schema rather than
        # relying on the interval parameter (which may be None for plain reads).
        table_cols = {
            row[1]
            for row in self.conn.execute(f"PRAGMA table_info({tablename})")
        }
        date_col = "date" if "date" in table_cols else "local_date"

        if start or end:
            conditions = []
            params: list = []
            if code:
                conditions.append("code = ?")
                params.append(code)
            if start:
                conditions.append(f"{date_col} >= ?")
                params.append(start.isoformat())
            if end:
                conditions.append(f"{date_col} <= ?")
                params.append(end.isoformat())
            where = " AND ".join(conditions)
            sql   = f"SELECT * FROM {tablename} WHERE {where}"  # noqa: S608
            pdf   = pd.read_sql(sql, self.conn, params=params)
        else:
            pdf = pd.read_sql(
                f"SELECT * FROM {tablename}"  # noqa: S608
                + (f" WHERE code = ?" if code else ""),
                self.conn,
                params=[code] if code else [],
            )

        # Restore proper types
        pdf["datetime"]  = pd.to_datetime(pdf["datetime"]).astype("datetime64[us]")
        pdf["timestamp"] = pdf["timestamp"].astype("int64")
        pdf["vo"]        = pdf["vo"].astype("int64")
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

        return pdf[[c for c in cols if c in pdf.columns]].reset_index(drop=True)

    def to_polars(
        self,
        tablename: str,
        **kwargs,
    ) -> pl.DataFrame:
        """Return table contents as a polars DataFrame (accepts same kwargs as to_pandas)."""
        return pandas2polars(self.to_pandas(tablename, **kwargs))

    def to_csv(self, tablename: str, csv_path: pathlib.Path, **kwargs) -> None:
        """Export table contents to a CSV file."""
        self.to_pandas(tablename, **kwargs).to_csv(csv_path, index=False)