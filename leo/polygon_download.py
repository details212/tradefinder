#!/usr/bin/env python3
"""
Download daily OHLCV aggregates from Polygon.io and save to MySQL.

First run  : fetches 4 years of daily bars for the symbol.
Second run+: compares the latest stored date to today and only inserts
             bars that are not yet in the table.

Current-day behaviour
---------------------
On the current trading day, 30-minute intraday bars are fetched and
aggregated into a single pseudo daily bar (open from first bar, high/low
across all bars, close from last bar, summed volume/transactions,
volume-weighted VWAP).  This gives a near-real-time "today" row.

5-day lookback
--------------
Each run also re-fetches the most recent 5 *daily* bars from Polygon so
that any pseudo bars from prior runs are overwritten with official
closed/adjusted data.

Usage
-----
    python polygon_download.py --symbol AAPL
    python polygon_download.py --days 30                      # only last 30 days
    python polygon_download.py --days 30 --symbol AAPL
    python polygon_download.py                                # uses leo.tickers query
    python polygon_download.py --workers 20                   # increase parallelism
    python polygon_download.py --normalized                   # adjusted bars → daily_ohlcv
    python polygon_download.py --normalized --symbol AAPL     # single symbol, adjusted
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from threading import Lock
from zoneinfo import ZoneInfo

import mysql.connector
from polygon import RESTClient

_print_lock = Lock()

# US Eastern timezone — Polygon daily bar timestamps are midnight ET
_ET = ZoneInfo("America/New_York")

def tprint(*args, **kwargs):
    """Thread-safe print."""
    with _print_lock:
        print(*args, **kwargs)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

POLYGON_API_KEY = "pntJnvnXxV3q2nAIdsph4RbT0b_oUlPE"

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "remote",
    "password": "Chamba4347!",
    "database": "leo",
}

TABLE_NAME = "daily_ohlcv"

# Set to True at runtime by --normalized; controls the Polygon adjusted= flag.
_API_ADJUSTED = False

LOOKBACK_DAYS = 5          # re-fetch this many recent daily bars each run
INTRADAY_TIMESPAN = "minute"
INTRADAY_MULTIPLIER = 1    # 1-minute bars for maximum intraday accuracy
BATCH_SIZE = 200           # rows per INSERT batch to reduce lock duration
MAX_RETRIES = 5            # deadlock retry attempts
RETRY_BASE_DELAY = 0.5     # seconds; doubles each retry

API_MAX_RETRIES = 5        # Polygon API retry attempts
API_RETRY_BASE_DELAY = 2.0 # seconds; doubles each retry
CHUNK_MONTHS = 6           # fetch historical data in 6-month chunks

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

def _create_table_sql(table_name: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS `{table_name}` (
    id            BIGINT AUTO_INCREMENT  PRIMARY KEY,
    symbol        VARCHAR(20)            NOT NULL,
    bar_date      DATE                   NOT NULL,
    open          DECIMAL(18, 6)         NOT NULL,
    high          DECIMAL(18, 6)         NOT NULL,
    low           DECIMAL(18, 6)         NOT NULL,
    close         DECIMAL(18, 6)         NOT NULL,
    volume        BIGINT                 NOT NULL,
    vwap          DECIMAL(18, 6)         NULL,
    transactions  INT                    NULL,
    is_intraday   TINYINT(1)             DEFAULT 0,
    created_at    DATETIME               DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_symbol_date (symbol, bar_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute(_create_table_sql(TABLE_NAME))
        # Add is_intraday column if upgrading from an older schema
        try:
            cur.execute(
                f"ALTER TABLE `{TABLE_NAME}` ADD COLUMN is_intraday TINYINT(1) DEFAULT 0"
            )
        except mysql.connector.errors.ProgrammingError:
            pass  # column already exists
    conn.commit()


def get_latest_bar_date(conn, symbol: str) -> date | None:
    """Return the most recent bar_date stored for *symbol*, or None."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT MAX(bar_date) FROM `{TABLE_NAME}` WHERE symbol = %s",
            (symbol,),
        )
        row = cur.fetchone()
    return row[0] if row and row[0] is not None else None


def delete_old_bars(conn, symbol: str, cutoff: date) -> int:
    """Delete bars older than *cutoff* for *symbol*. Returns rows deleted."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DELETE FROM `{TABLE_NAME}` WHERE symbol = %s AND bar_date < %s",
                    (symbol, cutoff),
                )
                deleted = cur.rowcount
            conn.commit()
            return deleted
        except mysql.connector.errors.DatabaseError as exc:
            if exc.errno == 1213 and attempt < MAX_RETRIES:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                tprint(f"[{symbol}] Deadlock on delete (attempt {attempt}/{MAX_RETRIES}), "
                       f"retrying in {delay:.1f}s ...")
                conn.rollback()
                time.sleep(delay)
            else:
                raise


def _execute_with_retry(conn, sql, rows, symbol=""):
    """Execute a batch INSERT with deadlock retry and exponential back-off."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()
            return
        except mysql.connector.errors.DatabaseError as exc:
            if exc.errno == 1213 and attempt < MAX_RETRIES:  # deadlock
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
                tprint(f"[{symbol}] Deadlock (attempt {attempt}/{MAX_RETRIES}), "
                       f"retrying in {delay:.1f}s ...")
                conn.rollback()
                time.sleep(delay)
            else:
                raise


def upsert_bars(conn, symbol: str, aggs: list, is_intraday: bool = False) -> int:
    """
    Insert bars in batches, updating existing rows on duplicate (symbol, bar_date).
    Retries automatically on deadlock.  Returns the number of rows processed.
    """
    if not aggs:
        return 0

    sql = f"""
        INSERT INTO `{TABLE_NAME}`
            (symbol, bar_date, open, high, low, close, volume, vwap, transactions, is_intraday)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            open         = VALUES(open),
            high         = VALUES(high),
            low          = VALUES(low),
            close        = VALUES(close),
            volume       = VALUES(volume),
            vwap         = VALUES(vwap),
            transactions = VALUES(transactions),
            is_intraday  = VALUES(is_intraday)
    """

    rows = []
    for agg in aggs:
        # Polygon daily timestamps are millisecond Unix epoch at midnight ET.
        # Convert via ET to get the correct trading date.
        bar_date = datetime.fromtimestamp(
            agg.timestamp / 1000, tz=timezone.utc
        ).astimezone(_ET).date()
        rows.append((
            symbol,
            bar_date,
            agg.open,
            agg.high,
            agg.low,
            agg.close,
            int(agg.volume),
            getattr(agg, "vwap", None),
            getattr(agg, "transactions", None),
            1 if is_intraday else 0,
        ))

    # Insert in batches to hold locks for shorter periods
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        _execute_with_retry(conn, sql, batch, symbol)

    return len(rows)


def upsert_pseudo_bar(conn, symbol: str, bar_date: date,
                      open_: float, high: float, low: float, close: float,
                      volume: int, vwap: float | None, transactions: int | None) -> int:
    """Insert/update a single pseudo (intraday-derived) daily bar."""
    sql = f"""
        INSERT INTO `{TABLE_NAME}`
            (symbol, bar_date, open, high, low, close, volume, vwap, transactions, is_intraday)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
        ON DUPLICATE KEY UPDATE
            open         = VALUES(open),
            high         = VALUES(high),
            low          = VALUES(low),
            close        = VALUES(close),
            volume       = VALUES(volume),
            vwap         = VALUES(vwap),
            transactions = VALUES(transactions),
            is_intraday  = 1
    """
    row = [(symbol, bar_date, open_, high, low, close,
            volume, vwap, transactions)]
    _execute_with_retry(conn, sql, row, symbol)
    return 1


# ---------------------------------------------------------------------------
# Polygon helpers
# ---------------------------------------------------------------------------

def _api_call_with_retry(func, symbol: str, description: str = "API call"):
    """
    Call *func* with retries on any exception (network, rate-limit, etc.).
    Returns the result of func() on success, or raises after exhausting retries.
    """
    last_exc = None
    for attempt in range(1, API_MAX_RETRIES + 1):
        try:
            return func()
        except Exception as exc:
            last_exc = exc
            if attempt < API_MAX_RETRIES:
                delay = API_RETRY_BASE_DELAY * (2 ** (attempt - 1))
                tprint(f"[{symbol}] {description} failed (attempt {attempt}/{API_MAX_RETRIES}): "
                       f"{exc}  — retrying in {delay:.1f}s ...")
                time.sleep(delay)
            else:
                tprint(f"[{symbol}] {description} failed after {API_MAX_RETRIES} attempts: {exc}")
    raise last_exc


def _date_chunks(from_date: date, to_date: date, months: int = CHUNK_MONTHS):
    """
    Yield (chunk_start, chunk_end) pairs splitting [from_date, to_date]
    into roughly *months*-month windows.
    """
    chunk_start = from_date
    while chunk_start <= to_date:
        # Advance by N months
        m = chunk_start.month - 1 + months
        y = chunk_start.year + m // 12
        m = m % 12 + 1
        try:
            chunk_end = chunk_start.replace(year=y, month=m) - timedelta(days=1)
        except ValueError:
            # Handle month-end edge cases (e.g. Jan 31 + 1 month)
            if m == 12:
                chunk_end = chunk_start.replace(year=y, month=12, day=31) - timedelta(days=1)
            else:
                chunk_end = chunk_start.replace(year=y, month=m + 1, day=1) - timedelta(days=1)
        chunk_end = min(chunk_end, to_date)
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def fetch_polygon_aggs(symbol: str, from_date: date, to_date: date) -> list:
    """
    Fetch daily bars from Polygon in chunked date ranges with retry.
    Each chunk is independently retried so a single failure doesn't lose
    the entire 4-year fetch.
    """
    all_aggs = []
    for chunk_start, chunk_end in _date_chunks(from_date, to_date):
        desc = f"daily bars {chunk_start}→{chunk_end}"

        def _fetch(s=chunk_start, e=chunk_end):
            client = RESTClient(api_key=POLYGON_API_KEY)
            aggs = []
            for agg in client.list_aggs(
                ticker=symbol,
                multiplier=1,
                timespan="day",
                from_=s.strftime("%Y-%m-%d"),
                to=e.strftime("%Y-%m-%d"),
                adjusted=_API_ADJUSTED,
                sort="asc",
                limit=50000,
            ):
                aggs.append(agg)
            return aggs

        try:
            chunk_aggs = _api_call_with_retry(_fetch, symbol, desc)
            all_aggs.extend(chunk_aggs)
        except Exception as exc:
            tprint(f"[{symbol}] WARNING: Chunk {chunk_start}→{chunk_end} failed "
                   f"permanently, skipping: {exc}")
            # Continue with other chunks instead of losing everything

    # Sanity check: for ranges > 30 days, we expect a reasonable number of
    # trading days (~21 per month).  Flag suspiciously low counts.
    range_days = (to_date - from_date).days
    if range_days > 30:
        expected_min = int(range_days * 0.6)  # ~60% should be trading days
        if len(all_aggs) < expected_min:
            tprint(f"[{symbol}] WARNING: Only {len(all_aggs)} bars returned for "
                   f"{from_date}→{to_date} ({range_days} calendar days) — "
                   f"expected at least ~{expected_min}. Possible data issue.")

    return all_aggs


def fetch_intraday_aggs(symbol: str, target_date: date) -> list:
    """Fetch 30-minute bars for *target_date* from Polygon with retry."""
    desc = f"intraday bars {target_date}"

    def _fetch():
        client = RESTClient(api_key=POLYGON_API_KEY)
        aggs = []
        for agg in client.list_aggs(
            ticker=symbol,
            multiplier=INTRADAY_MULTIPLIER,
            timespan=INTRADAY_TIMESPAN,
            from_=target_date.strftime("%Y-%m-%d"),
            to=target_date.strftime("%Y-%m-%d"),
            adjusted=_API_ADJUSTED,
            sort="asc",
            limit=50000,
        ):
            aggs.append(agg)
        return aggs

    return _api_call_with_retry(_fetch, symbol, desc)


# ---------------------------------------------------------------------------
# Gap detection
# ---------------------------------------------------------------------------

# Major US market holidays — comprehensive list to avoid false-positive gaps.
_US_HOLIDAYS_MD = {
    (1, 1),   # New Year's Day
    (7, 4),   # Independence Day
    (6, 19),  # Juneteenth
    (12, 25), # Christmas
}


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """Return the *n*-th occurrence of *weekday* (0=Mon) in the given month."""
    first = date(year, month, 1)
    # Days until the first occurrence of weekday
    offset = (weekday - first.weekday()) % 7
    d = first + timedelta(days=offset + 7 * (n - 1))
    return d


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Return the last occurrence of *weekday* in the given month."""
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _get_market_holidays(year: int) -> set[date]:
    """
    Return the set of dates the US stock market is closed for *year*.
    Covers NYSE/NASDAQ observed holidays.
    """
    holidays = set()

    # --- Fixed-date holidays (observed on nearest weekday) ----------------
    for month, day in _US_HOLIDAYS_MD:
        d = date(year, month, day)
        if d.weekday() == 5:       # Saturday → observe Friday
            d -= timedelta(days=1)
        elif d.weekday() == 6:     # Sunday → observe Monday
            d += timedelta(days=1)
        holidays.add(d)

    # --- Floating holidays ------------------------------------------------
    # MLK Day: 3rd Monday in January
    holidays.add(_nth_weekday(year, 1, 0, 3))
    # Presidents' Day: 3rd Monday in February
    holidays.add(_nth_weekday(year, 2, 0, 3))
    # Memorial Day: last Monday in May
    holidays.add(_last_weekday(year, 5, 0))
    # Labor Day: 1st Monday in September
    holidays.add(_nth_weekday(year, 9, 0, 1))
    # Thanksgiving: 4th Thursday in November
    holidays.add(_nth_weekday(year, 11, 3, 4))

    # --- Special closures (rare, add as needed) ---------------------------
    # National Day of Mourning for Jimmy Carter
    if year == 2025:
        holidays.add(date(2025, 1, 9))

    return holidays


# Cache per year so we don't recompute for every date check
_holiday_cache: dict[int, set[date]] = {}


def _is_market_closed(d: date) -> bool:
    """Return True if *d* is a weekend or a US market holiday."""
    if d.weekday() >= 5:  # Saturday / Sunday
        return True
    if d.year not in _holiday_cache:
        _holiday_cache[d.year] = _get_market_holidays(d.year)
    return d in _holiday_cache[d.year]


def detect_gaps(conn, symbol: str, from_date: date, to_date: date) -> list[date]:
    """
    Return a list of dates in [from_date, to_date] that are likely missing
    trading days (weekdays that are not market holidays, with no bar stored).
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT bar_date FROM `{TABLE_NAME}` "
            f"WHERE symbol = %s AND bar_date BETWEEN %s AND %s "
            f"ORDER BY bar_date",
            (symbol, from_date, to_date),
        )
        stored = {row[0] for row in cur.fetchall()}

    gaps = []
    d = from_date
    while d <= to_date:
        if d not in stored and not _is_market_closed(d):
            gaps.append(d)
        d += timedelta(days=1)
    return gaps


def fill_gaps(conn, symbol: str, gaps: list[date]) -> int:
    """
    Attempt to re-fetch and insert bars for specific missing dates.
    Returns the number of bars successfully filled.
    """
    if not gaps:
        return 0

    filled = 0
    # Group consecutive gaps into small ranges to minimize API calls
    ranges = []
    start = gaps[0]
    end = gaps[0]
    for g in gaps[1:]:
        if (g - end).days <= 3:  # allow small jumps (weekends between gaps)
            end = g
        else:
            ranges.append((start, end))
            start = g
            end = g
    ranges.append((start, end))

    for range_start, range_end in ranges:
        desc = f"gap-fill {range_start}→{range_end}"
        try:
            def _fetch(s=range_start, e=range_end):
                client = RESTClient(api_key=POLYGON_API_KEY)
                aggs = []
                for agg in client.list_aggs(
                    ticker=symbol,
                    multiplier=1,
                    timespan="day",
                    from_=s.strftime("%Y-%m-%d"),
                    to=e.strftime("%Y-%m-%d"),
                    adjusted=_API_ADJUSTED,
                    sort="asc",
                    limit=50000,
                ):
                    aggs.append(agg)
                return aggs

            aggs = _api_call_with_retry(_fetch, symbol, desc)
            count = upsert_bars(conn, symbol, aggs, is_intraday=False) if aggs else 0
            filled += count
            if count:
                tprint(f"[{symbol}] Gap-filled {count} bar(s) for {range_start}→{range_end}")
        except Exception as exc:
            tprint(f"[{symbol}] WARNING: Gap-fill {range_start}→{range_end} "
                   f"failed: {exc}")

    return filled


def aggregate_intraday_to_daily(aggs: list, target_date: date) -> dict | None:
    """
    Collapse a list of intraday bars into a single pseudo daily bar.

    Only includes bars during regular market hours (9:30 AM – 4:00 PM ET)
    so the pseudo bar matches the official daily bar as closely as possible.

    Returns a dict with open, high, low, close, volume, vwap, transactions
    or None if no bars are provided.
    """
    if not aggs:
        return None

    # Filter to regular trading hours only (9:30 AM – 3:59 PM ET)
    market_open = datetime(target_date.year, target_date.month, target_date.day,
                           9, 30, tzinfo=_ET)
    market_close = datetime(target_date.year, target_date.month, target_date.day,
                            16, 0, tzinfo=_ET)

    rth_aggs = []
    for a in aggs:
        bar_time = datetime.fromtimestamp(
            a.timestamp / 1000, tz=timezone.utc
        ).astimezone(_ET)
        if market_open <= bar_time < market_close:
            rth_aggs.append(a)

    if not rth_aggs:
        return None

    open_ = rth_aggs[0].open
    high = max(a.high for a in rth_aggs)
    low = min(a.low for a in rth_aggs)
    close = rth_aggs[-1].close
    volume = int(sum(a.volume for a in rth_aggs))
    transactions = sum(getattr(a, "transactions", 0) or 0 for a in rth_aggs)

    # Volume-weighted average price (weighted by per-bar volume)
    vwap = None
    total_vwap_vol = 0
    weighted_sum = 0
    for a in rth_aggs:
        v = a.volume or 0
        vw = getattr(a, "vwap", None)
        if vw is not None and v > 0:
            weighted_sum += vw * v
            total_vwap_vol += v
    if total_vwap_vol > 0:
        vwap = weighted_sum / total_vwap_vol

    return {
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "vwap": vwap,
        "transactions": transactions,
    }


# ---------------------------------------------------------------------------
# Lookback verification
# ---------------------------------------------------------------------------

# Tolerance for DECIMAL(18,6) comparison — values within this are "equal"
_PRICE_TOL = 1e-5

def _rows_to_dict(rows) -> dict:
    """Convert DB rows [(bar_date, open, high, low, close, volume, vwap)] to
    a dict keyed by bar_date."""
    return {
        row[0]: {
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": int(row[5]),
            "vwap": float(row[6]) if row[6] is not None else None,
        }
        for row in rows
    }


def _aggs_to_dict(aggs) -> dict:
    """Convert Polygon agg objects to a dict keyed by bar_date."""
    result = {}
    for agg in aggs:
        bar_date = datetime.fromtimestamp(
            agg.timestamp / 1000, tz=timezone.utc
        ).astimezone(_ET).date()
        result[bar_date] = {
            "open": float(agg.open),
            "high": float(agg.high),
            "low": float(agg.low),
            "close": float(agg.close),
            "volume": int(agg.volume),
            "vwap": float(getattr(agg, "vwap", None)) if getattr(agg, "vwap", None) is not None else None,
        }
    return result


def _values_match(db_val, api_val, field: str) -> bool:
    """Compare a single field between DB and API values."""
    if db_val is None and api_val is None:
        return True
    if db_val is None or api_val is None:
        return False
    if field == "volume":
        return int(db_val) == int(api_val)
    return abs(float(db_val) - float(api_val)) < _PRICE_TOL


def verify_lookback_bars(conn, symbol: str, lookback_start: date,
                         yesterday: date, polygon_aggs: list) -> list[str]:
    """
    Compare the Polygon lookback bars against what's stored in the DB.

    1) Before upsert: logs any pre-existing mismatches (stale pseudo bars, etc.)
    2) After upsert: re-reads from DB to confirm the write stuck.

    Returns a list of warning strings (empty = all good).
    """
    warnings = []
    fields = ("open", "high", "low", "close", "volume", "vwap")
    api_dict = _aggs_to_dict(polygon_aggs)

    if not api_dict:
        warnings.append(f"[{symbol}] Lookback: Polygon returned 0 bars for "
                        f"{lookback_start}→{yesterday}")
        return warnings

    # --- Pre-upsert: compare existing DB rows vs Polygon -----------------
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT bar_date, open, high, low, close, volume, vwap "
            f"FROM `{TABLE_NAME}` "
            f"WHERE symbol = %s AND bar_date BETWEEN %s AND %s "
            f"ORDER BY bar_date",
            (symbol, lookback_start, yesterday),
        )
        pre_rows = cur.fetchall()

    pre_dict = _rows_to_dict(pre_rows)

    corrections = 0
    for bar_date, api_bar in api_dict.items():
        if bar_date not in pre_dict:
            warnings.append(f"[{symbol}] Lookback: {bar_date} missing from DB "
                            f"(will be inserted)")
            corrections += 1
            continue
        db_bar = pre_dict[bar_date]
        for f in fields:
            if not _values_match(db_bar.get(f), api_bar.get(f), f):
                warnings.append(
                    f"[{symbol}] Lookback: {bar_date} {f} mismatch — "
                    f"DB={db_bar.get(f)} vs Polygon={api_bar.get(f)} (will be corrected)")
                corrections += 1
                break  # one mismatch per date is enough to flag it

    if corrections:
        tprint(f"[{symbol}] Lookback: {corrections} correction(s) needed in "
               f"{lookback_start}→{yesterday}")

    return warnings


def verify_lookback_write(conn, symbol: str, lookback_start: date,
                          yesterday: date, polygon_aggs: list) -> list[str]:
    """
    Post-upsert verification: re-read DB and confirm every Polygon bar
    was written correctly.  Returns a list of error strings (empty = all good).
    """
    errors = []
    fields = ("open", "high", "low", "close", "volume", "vwap")
    api_dict = _aggs_to_dict(polygon_aggs)

    with conn.cursor() as cur:
        cur.execute(
            f"SELECT bar_date, open, high, low, close, volume, vwap "
            f"FROM `{TABLE_NAME}` "
            f"WHERE symbol = %s AND bar_date BETWEEN %s AND %s "
            f"ORDER BY bar_date",
            (symbol, lookback_start, yesterday),
        )
        post_rows = cur.fetchall()

    post_dict = _rows_to_dict(post_rows)

    for bar_date, api_bar in api_dict.items():
        if bar_date not in post_dict:
            errors.append(f"[{symbol}] VERIFY FAIL: {bar_date} still missing "
                          f"from DB after upsert!")
            continue
        db_bar = post_dict[bar_date]
        for f in fields:
            if not _values_match(db_bar.get(f), api_bar.get(f), f):
                errors.append(
                    f"[{symbol}] VERIFY FAIL: {bar_date} {f} still wrong "
                    f"after upsert — DB={db_bar.get(f)} vs Polygon={api_bar.get(f)}")
                break

    return errors


# ---------------------------------------------------------------------------
# Per-symbol worker (runs in a thread)
# ---------------------------------------------------------------------------

def process_symbol(symbol: str, today: date, backfill_start: date, prune_cutoff: date) -> str:
    """
    Fetch and store bars for one symbol.  Each call opens its own DB
    connection so threads don't share state.
    Returns a status string for the summary line.
    """
    conn = get_db_connection()
    try:
        # ==================================================================
        # 1) Full historical back-fill (4 years of daily bars)
        #    ON DUPLICATE KEY UPDATE handles rows that already exist.
        #    Stops before the lookback window to avoid duplicate fetches.
        # ==================================================================
        from_date = backfill_start
        yesterday = today - timedelta(days=1)
        lookback_start = today - timedelta(days=LOOKBACK_DAYS + 3)  # pad for weekends
        backfill_end = lookback_start - timedelta(days=1)

        daily_count = 0
        if from_date <= backfill_end:
            tprint(f"[{symbol}] Fetching daily bars {from_date} → {backfill_end} ...")
            aggs = fetch_polygon_aggs(symbol, from_date, backfill_end)
            daily_count = upsert_bars(conn, symbol, aggs, is_intraday=False) if aggs else 0

        # ==================================================================
        # 2) 5-day lookback: re-fetch the last 5 daily bars, verify
        #    against stored data, upsert, then confirm the write.
        # ==================================================================
        tprint(f"[{symbol}] Re-fetching lookback daily bars {lookback_start} → {yesterday} ...")
        lookback_aggs = fetch_polygon_aggs(symbol, lookback_start, yesterday)

        # Pre-upsert: compare existing DB rows vs fresh Polygon data
        pre_warnings = verify_lookback_bars(conn, symbol, lookback_start, yesterday, lookback_aggs)
        for w in pre_warnings:
            tprint(w)

        # Upsert the fresh Polygon data
        lookback_count = upsert_bars(conn, symbol, lookback_aggs, is_intraday=False) if lookback_aggs else 0

        # Post-upsert: re-read DB and confirm every bar matches Polygon
        post_errors = verify_lookback_write(conn, symbol, lookback_start, yesterday, lookback_aggs)
        for e in post_errors:
            tprint(e, file=sys.stderr)

        # ==================================================================
        # 3) Current-day pseudo bar from 1-minute intraday aggregates
        #    (regular trading hours only: 9:30 AM – 4:00 PM ET)
        # ==================================================================
        tprint(f"[{symbol}] Fetching 1-min intraday bars for today ({today}) ...")
        intraday_aggs = fetch_intraday_aggs(symbol, today)
        pseudo = aggregate_intraday_to_daily(intraday_aggs, today)
        intraday_count = 0
        if pseudo:
            intraday_count = upsert_pseudo_bar(
                conn, symbol, today,
                pseudo["open"], pseudo["high"], pseudo["low"], pseudo["close"],
                pseudo["volume"], pseudo["vwap"], pseudo["transactions"],
            )
            tprint(f"[{symbol}] Pseudo bar for {today}: "
                   f"O={pseudo['open']:.2f} H={pseudo['high']:.2f} "
                   f"L={pseudo['low']:.2f} C={pseudo['close']:.2f} "
                   f"V={pseudo['volume']}")
        else:
            tprint(f"[{symbol}] No intraday data for {today} (market may be closed).")

        # ==================================================================
        # 4) Prune bars older than 4 years
        # ==================================================================
        deleted = delete_old_bars(conn, symbol, prune_cutoff)

        # ==================================================================
        # 5) Gap detection & fill — find missing Tue-Thu trading days and
        #    attempt to re-fetch them individually.
        # ==================================================================
        gaps = detect_gaps(conn, symbol, backfill_start, yesterday)
        gap_filled = 0
        if gaps:
            tprint(f"[{symbol}] Detected {len(gaps)} potential gap(s), attempting fill ...")
            gap_filled = fill_gaps(conn, symbol, gaps)
            # Re-check for remaining gaps
            remaining = detect_gaps(conn, symbol, backfill_start, yesterday)
            if remaining:
                tprint(f"[{symbol}] WARNING: {len(remaining)} gap(s) remain after fill: "
                       f"{', '.join(str(d) for d in remaining[:10])}"
                       f"{'...' if len(remaining) > 10 else ''}")

        tprint(f"[{symbol}] Done — {daily_count} daily upserted, "
               f"{lookback_count} lookback refreshed, "
               f"{intraday_count} pseudo bar(s), {gap_filled} gap(s) filled, "
               f"{deleted} old bar(s) removed.")
        return (f"[{symbol}] {daily_count} daily, {lookback_count} lookback, "
                f"{intraday_count} pseudo, {gap_filled} gap-filled, {deleted} deleted")

    except Exception as exc:
        tprint(f"[{symbol}] ERROR: {exc}", file=sys.stderr)
        return f"[{symbol}] ERROR: {exc}"
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Download Polygon.io daily bars into MySQL."
    )
    parser.add_argument(
        "--symbol",
        required=False,
        help="Ticker symbol to download, e.g. --symbol AAPL. If omitted, symbols are loaded from leo.tickers.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel download threads (default: 10).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Override the 4-year backfill with N days (e.g. --days 30).",
    )
    parser.add_argument(
        "--normalized",
        action="store_true",
        default=False,
        help=(
            "Download split/dividend-adjusted (normalized) bars instead of raw bars. "
            "Data is stored in the same 'daily_ohlcv' table."
        ),
    )
    args = parser.parse_args()

    global _API_ADJUSTED
    if args.normalized:
        _API_ADJUSTED = True
        print("Mode: normalized (split/dividend-adjusted) bars → table 'daily_ohlcv'.")
    else:
        print("Mode: raw (unadjusted) bars → table 'daily_ohlcv'.")

    today = date.today()
    if args.days is not None:
        backfill_start = today - timedelta(days=args.days)
        print(f"Backfill overridden to {args.days} day(s) (from {backfill_start}).")
    else:
        backfill_start = today.replace(year=today.year - 4)

    # ------------------------------------------------------------------
    # Connect for setup / symbol list, then close — workers open their own
    # ------------------------------------------------------------------
    print(f"Connecting to MySQL ({DB_CONFIG['host']}/{DB_CONFIG['database']}) ...")
    conn = get_db_connection()
    ensure_table(conn)

    if args.symbol:
        symbols = [args.symbol.upper()]
    else:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ticker
                FROM leo.tickers
                WHERE (last_day_volume > 500000
                  AND type = 'CS'
                  AND last_day_close > 5)
                  OR ticker = 'SPY';
                """
            )
            rows = cur.fetchall()

        symbols = [row[0] for row in rows]

        if not symbols:
            print("No symbols found from leo.tickers for the specified criteria.")
            conn.close()
            return

    conn.close()

    prune_cutoff = today.replace(year=today.year - 4)

    print(f"Processing {len(symbols)} symbol(s) with {args.workers} worker thread(s) ...\n")

    # ------------------------------------------------------------------
    # Parallel download
    # ------------------------------------------------------------------
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(process_symbol, sym, today, backfill_start, prune_cutoff): sym
            for sym in symbols
        }
        for future in as_completed(futures):
            results.append(future.result())

    print(f"\nFinished. {len(results)} symbol(s) processed.")


if __name__ == "__main__":
    main()
