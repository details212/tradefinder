"""
Polygon.io OHLCV Bar Fetcher
============================
Fetches 1m / 5m / 15m / 30m / 60m / daily bars for the active ticker
universe (CS stocks with volume>500k, close>5, plus SPY and QQQ) and
stores them in MySQL with upsert semantics.

Modes
-----
  --backfill              One-time historical load:
                            • 756 trading days (≈1095 cal days / 3 years) of daily bars
                            • 10 trading days (≈14 cal days) of all intraday bars
  --live                  60-second refresh loop during market hours (ET)
  --backfill --live       Backfill first, then enter the live loop
  --refresh-reference     Pull/update float, shares outstanding, market cap
  --once                  Run a single intraday refresh cycle (for cron use)
  --test                  Verify API key and DB connectivity

Rate-limit note
---------------
Adjust REQUESTS_PER_SECOND to your Polygon plan:
  Free      →  5   (default is fine)
  Starter   →  100
  Developer+ → 200 (or higher; Polygon won't enforce a hard cap)
"""

import argparse
import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import mysql.connector
from mysql.connector import Error
import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:                          # Python < 3.9
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

# ─── Configuration ────────────────────────────────────────────────────────────

POLYGON_API_KEY  = "pntJnvnXxV3q2nAIdsph4RbT0b_oUlPE"
POLYGON_BASE_URL = "https://api.polygon.io"

DB_CONFIG: Dict = {
    "host":      "127.0.0.1",
    "user":      "remote",
    "password":  "Chamba4347!",
    "database":  "tradefinder",
    "charset":   "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
}

# Adjust to your Polygon subscription:  Free=5  Starter=100  Developer+=200
REQUESTS_PER_SECOND = 100
MAX_WORKERS         = 20        # parallel HTTP workers (keep ≤ REQUESTS_PER_SECOND)
BATCH_INSERT_SIZE   = 500       # rows per executemany call

ET = ZoneInfo("America/New_York")
MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  30
MARKET_CLOSE_H, MARKET_CLOSE_M = 16, 0
LIVE_INTERVAL   = 900           # seconds between live refresh cycles (15 minutes)

# Calendar-day windows (Polygon uses calendar dates, not trading days)
BACKFILL_CAL_INTRADAY = 14      # ≈ 10 trading days
BACKFILL_CAL_DAILY    = 1095    # ≈ 756 trading days (3 years)

# (multiplier, timespan, table_name, backfill_calendar_days)
TIMEFRAMES: List[Tuple[int, str, str, int]] = [
    # 1-min and 15-min bars removed — no registered strategy uses them.
    # Strategies use: daily, m5, m30, m60 only.
    (5,  "minute", "bars_5min",   BACKFILL_CAL_INTRADAY),
    (30, "minute", "bars_30min",  BACKFILL_CAL_INTRADAY),
    (60, "minute", "bars_60min",  BACKFILL_CAL_INTRADAY),
    (1,  "day",    "bars_daily",  BACKFILL_CAL_DAILY),
]

# Live refresh uses a shorter window so responses stay small and fast.
# We fetch "from today" for intraday and "last 3 days" for daily.
LIVE_INTRADAY_DAYS = 1   # today only — all completed bars since midnight ET
LIVE_DAILY_DAYS    = 3   # capture today's accumulating daily bar

TICKER_QUERY = """
    SELECT ticker FROM tradefinder.tickers
    WHERE last_day_volume > 500000
      AND type = 'CS'
      AND last_day_close > 5
       OR ticker = 'SPY' OR ticker = 'QQQ'
    ORDER BY ticker
"""

# ─── DDL ──────────────────────────────────────────────────────────────────────

_DDL_BAR_TABLE = """\
CREATE TABLE IF NOT EXISTS `{table}` (
  `ticker`       VARCHAR(20)   NOT NULL,
  `bar_time`     DATETIME      NOT NULL,
  `open`         DECIMAL(18,4) DEFAULT NULL,
  `high`         DECIMAL(18,4) DEFAULT NULL,
  `low`          DECIMAL(18,4) DEFAULT NULL,
  `close`        DECIMAL(18,4) DEFAULT NULL,
  `volume`       BIGINT        DEFAULT NULL,
  `vwap`         DECIMAL(18,4) DEFAULT NULL,
  `transactions` INT           DEFAULT NULL,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_DDL_REFERENCE = """\
CREATE TABLE IF NOT EXISTS `ticker_reference` (
  `ticker`             VARCHAR(20)    NOT NULL PRIMARY KEY,
  `float_shares`       BIGINT         DEFAULT NULL,
  `shares_outstanding` BIGINT         DEFAULT NULL,
  `weighted_shares`    BIGINT         DEFAULT NULL,
  `market_cap`         DECIMAL(22,2)  DEFAULT NULL,
  `description`        TEXT           DEFAULT NULL,
  `updated_at`         DATETIME       NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Rate limiter ─────────────────────────────────────────────────────────────

class _RateLimiter:
    """Thread-safe token-bucket rate limiter."""

    def __init__(self, rate: float):
        self._min_interval = 1.0 / rate
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self):
        # Calculate the required sleep outside the lock so other threads
        # can proceed in parallel during the sleep.  Re-acquire only to
        # stamp _last, which prevents two threads from both seeing a
        # "no wait needed" result at the same instant.
        while True:
            with self._lock:
                now  = time.monotonic()
                gap  = self._min_interval - (now - self._last)
                if gap <= 0:
                    self._last = now
                    return
            time.sleep(gap)

_limiter = _RateLimiter(REQUESTS_PER_SECOND)

# ─── Graceful shutdown ────────────────────────────────────────────────────────
# Set by SIGINT / SIGTERM so the live loop exits cleanly after its current cycle.

_shutdown = threading.Event()

def _request_shutdown(sig=None, frame=None):
    log.info("Shutdown requested — finishing current cycle then stopping...")
    _shutdown.set()

# ─── Connection pool ──────────────────────────────────────────────────────────
# A single pool is shared across all worker threads.  Each thread borrows a
# connection, uses it, then returns it automatically when conn.close() is called.
# Pool size = MAX_WORKERS + headroom for the main thread (DDL, purge, tickers).

_pool: Optional[mysql.connector.pooling.MySQLConnectionPool] = None


def _init_pool():
    """Create the module-level connection pool.  Call once after MAX_WORKERS is final."""
    global _pool
    size = min(MAX_WORKERS + 4, 32)   # mysql-connector caps pool_size at 32
    _pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="bars_pool",
        pool_size=size,
        **DB_CONFIG,
    )
    log.info("MySQL connection pool ready (%d connections).", size)

# ─── Polygon API helpers ──────────────────────────────────────────────────────

def _polygon_get(endpoint_or_url: str,
                 params: Optional[Dict] = None,
                 max_retries: int = 3) -> Optional[Dict]:
    """
    GET from Polygon with rate limiting, 429 back-off, and retry logic.
    Pass a full URL (for pagination cursors) or a path like /v2/aggs/...
    """
    url = (endpoint_or_url
           if endpoint_or_url.startswith("http")
           else f"{POLYGON_BASE_URL}{endpoint_or_url}")

    p = dict(params or {})
    p["apiKey"] = POLYGON_API_KEY

    for attempt in range(max_retries):
        _limiter.wait()
        try:
            r = requests.get(url, params=p, timeout=30)

            if r.status_code == 429:
                # Respect Retry-After if present, else back off 60 s
                retry_after = int(r.headers.get("Retry-After", 60))
                log.warning("Rate limited by Polygon — sleeping %ds", retry_after)
                time.sleep(retry_after)
                continue

            if r.status_code == 403:
                log.error("Polygon 403 Forbidden — check API key / subscription")
                return None

            r.raise_for_status()
            return r.json()

        except requests.exceptions.Timeout:
            wait = (attempt + 1) * 5
            log.warning("Timeout on %s (attempt %d/%d), retrying in %ds",
                        url, attempt + 1, max_retries, wait)
            if attempt < max_retries - 1:
                time.sleep(wait)

        except requests.exceptions.RequestException as exc:
            log.error("Request error %s: %s", url, exc)
            if attempt < max_retries - 1:
                time.sleep((attempt + 1) * 3)

    return None


def fetch_bars(ticker: str,
               multiplier: int,
               timespan: str,
               from_date: str,
               to_date: str) -> List[Dict]:
    """
    Fetch all OHLCV bars for one ticker/timeframe, handling Polygon pagination.

    Parameters
    ----------
    from_date, to_date : 'YYYY-MM-DD' strings (inclusive on both ends)

    Returns
    -------
    List of Polygon result dicts: {t, o, h, l, c, v, vw, n}
    """
    url = (f"{POLYGON_BASE_URL}/v2/aggs/ticker/{ticker}"
           f"/range/{multiplier}/{timespan}/{from_date}/{to_date}")
    params: Optional[Dict] = {"adjusted": "true", "sort": "asc", "limit": 50000}
    bars: List[Dict] = []

    while url:
        data = _polygon_get(url, params)
        if not data:
            break

        status = data.get("status", "")
        if status not in ("OK", "DELAYED"):
            # "NO_DATA" is normal for tickers that had no trading that period
            if status not in ("NO_DATA", ""):
                log.debug("No bars for %s %d%s: %s", ticker, multiplier, timespan, status)
            break

        results = data.get("results") or []
        bars.extend(results)

        next_url = data.get("next_url")
        if next_url:
            url    = next_url   # full URL with embedded cursor
            params = None       # don't add original params; only apiKey will be injected
        else:
            url = None

    return bars


def fetch_reference(ticker: str) -> Optional[Dict]:
    """Fetch reference data (float, shares outstanding, market cap) for a ticker."""
    data = _polygon_get(f"/v3/reference/tickers/{ticker}")
    if data and data.get("status") == "OK":
        return data.get("results") or {}
    return None


# ─── Database helpers ─────────────────────────────────────────────────────────

def get_db_conn() -> mysql.connector.MySQLConnection:
    """Borrow a connection from the pool; falls back to a direct connect before the pool is initialised."""
    if _pool is not None:
        return _pool.get_connection()
    return mysql.connector.connect(**DB_CONFIG)


def create_tables():
    """Create bar and reference tables if they don't already exist."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        for _, _, table, _ in TIMEFRAMES:
            cur.execute(_DDL_BAR_TABLE.format(table=table))
        cur.execute(_DDL_REFERENCE)
        conn.commit()
        log.info("Tables verified / created.")
    finally:
        conn.close()


def purge_old_bars():
    """
    Delete bar rows that fall outside each timeframe's retention window.
    Called automatically at startup so the tables don't grow unboundedly.

    Retention windows match the backfill depths:
      Intraday (1m/5m/15m/30m/60m) : BACKFILL_CAL_INTRADAY calendar days
      Daily                         : BACKFILL_CAL_DAILY    calendar days
    """
    today = date.today()
    conn  = get_db_conn()
    try:
        cur = conn.cursor()
        total_deleted = 0
        for _, _, table, cal_days in TIMEFRAMES:
            cutoff = today - timedelta(days=cal_days)
            cur.execute(
                f"DELETE FROM `{table}` WHERE bar_time < %s",
                (cutoff,),
            )
            n = cur.rowcount
            total_deleted += n
            if n:
                log.info("  Purged %6d rows from %-14s (older than %s)", n, table, cutoff)
        conn.commit()
        if total_deleted:
            log.info("Purge complete — %d total rows removed.", total_deleted)
        else:
            log.info("Purge complete — nothing to remove.")
    finally:
        conn.close()


def get_tickers() -> List[str]:
    """Return the active ticker universe from trade_finder.tickers."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(TICKER_QUERY)
        rows = [r[0] for r in cur.fetchall()]
        return rows
    finally:
        conn.close()


def _bar_to_row(ticker: str, bar: Dict) -> Tuple:
    """
    Convert a Polygon aggregate result dict into a DB row tuple.
    Timestamps are stored as naive datetimes in US/Eastern time.
    """
    ts_ms  = bar["t"]
    dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    dt_et  = dt_utc.astimezone(ET).replace(tzinfo=None)
    return (
        ticker,
        dt_et,
        bar.get("o"),
        bar.get("h"),
        bar.get("l"),
        bar.get("c"),
        bar.get("v"),
        bar.get("vw"),
        bar.get("n"),
    )


_UPSERT_SQL = """\
INSERT INTO `{table}`
  (ticker, bar_time, open, high, low, close, volume, vwap, transactions)
VALUES
  (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  open         = VALUES(open),
  high         = VALUES(high),
  low          = VALUES(low),
  close        = VALUES(close),
  volume       = VALUES(volume),
  vwap         = VALUES(vwap),
  transactions = VALUES(transactions)
"""

_UPSERT_REF_SQL = """\
INSERT INTO ticker_reference
  (ticker, float_shares, shares_outstanding, weighted_shares, market_cap, description, updated_at)
VALUES
  (%s, %s, %s, %s, %s, %s, %s)
ON DUPLICATE KEY UPDATE
  float_shares       = VALUES(float_shares),
  shares_outstanding = VALUES(shares_outstanding),
  weighted_shares    = VALUES(weighted_shares),
  market_cap         = VALUES(market_cap),
  description        = VALUES(description),
  updated_at         = VALUES(updated_at)
"""


def upsert_bars(table: str, rows: List[Tuple]):
    """Batch-upsert bar rows into the named table."""
    if not rows:
        return
    sql  = _UPSERT_SQL.format(table=table)
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        for i in range(0, len(rows), BATCH_INSERT_SIZE):
            cur.executemany(sql, rows[i : i + BATCH_INSERT_SIZE])
        conn.commit()
    finally:
        conn.close()


def upsert_reference(ticker: str, ref: Dict):
    """Upsert one ticker's reference data."""
    now = datetime.now()
    row = (
        ticker,
        ref.get("share_class_shares_outstanding"),
        ref.get("weighted_shares_outstanding"),
        ref.get("weighted_shares_outstanding"),   # Polygon doesn't expose float directly
        ref.get("market_cap"),
        ref.get("description"),
        now,
    )
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(_UPSERT_REF_SQL, row)
        conn.commit()
    finally:
        conn.close()


# ─── Market hours ─────────────────────────────────────────────────────────────

def is_market_open() -> bool:
    """Return True if the US equities market is currently open (ET)."""
    now_et = datetime.now(tz=ET)
    if now_et.weekday() >= 5:          # Saturday or Sunday
        return False
    open_time  = now_et.replace(hour=MARKET_OPEN_H,  minute=MARKET_OPEN_M,  second=0, microsecond=0)
    close_time = now_et.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M, second=0, microsecond=0)
    return open_time <= now_et < close_time


def seconds_until_market_open() -> float:
    """Return how many seconds until the next market open (ET)."""
    now_et = datetime.now(tz=ET)
    # Advance to next weekday
    candidate = now_et.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                                second=0, microsecond=0)
    if candidate <= now_et:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return max(0.0, (candidate - now_et).total_seconds())


# ─── Core fetch / store task ──────────────────────────────────────────────────

def _fetch_and_store(ticker: str,
                     multiplier: int,
                     timespan: str,
                     table: str,
                     from_date: str,
                     to_date: str) -> Tuple[str, str, int]:
    """
    Worker function: fetch bars for one ticker/timeframe and upsert to DB.
    Returns (ticker, table, bars_inserted) for logging.
    """
    bars = fetch_bars(ticker, multiplier, timespan, from_date, to_date)
    if bars:
        rows = [_bar_to_row(ticker, b) for b in bars]
        upsert_bars(table, rows)
    return ticker, table, len(bars)


# ─── Backfill ─────────────────────────────────────────────────────────────────

def _get_latest_bar_times(table: str) -> Dict[str, date]:
    """
    Return {ticker: latest_bar_date} for every ticker that already has rows
    in *table*.  One bulk query — no per-ticker round-trips.
    """
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT ticker, DATE(MAX(bar_time)) FROM `{table}` GROUP BY ticker")
        return {row[0]: row[1] for row in cur.fetchall()}
    finally:
        conn.close()


def run_backfill(tickers: List[str]):
    """
    Gap-aware backfill.

    For each ticker / timeframe the from_date is chosen as:
      • No existing data  → full retention window (first run)
      • Has existing data → MAX(bar_time) - 1 day  (fills any gap since last run)
      • Already current   → skipped entirely        (today's date already present)

    The -1 day overlap on the resume boundary catches partially-written bars
    at the end of the previous run and costs only a handful of duplicate upserts.
    """
    today = date.today()

    # Bulk-fetch the latest stored bar date per ticker for every table.
    # One GROUP BY query per table — fast even with 500+ tickers.
    log.info("Checking existing data to compute gap ranges...")
    latest: Dict[str, Dict[str, date]] = {}   # latest[table][ticker] = date
    for _, _, table, _ in TIMEFRAMES:
        latest[table] = _get_latest_bar_times(table)

    tasks = []
    skipped = 0
    for ticker in tickers:
        for mult, span, table, cal_days in TIMEFRAMES:
            full_from = today - timedelta(days=cal_days)
            last_date  = latest[table].get(ticker)

            if last_date is None:
                # No data at all — full historical load
                from_dt = full_from
            elif last_date >= today:
                # Already up to date — nothing to fetch
                skipped += 1
                continue
            else:
                # Resume from one day before the last stored bar to close any gap
                from_dt = max(last_date - timedelta(days=1), full_from)

            tasks.append((ticker, mult, span, table, str(from_dt), str(today)))

    total  = len(tasks)
    done   = 0
    errors = 0
    log.info(
        "Backfill — %d tasks to run, %d ticker/table pairs already current.",
        total, skipped,
    )

    if not tasks:
        log.info("Nothing to backfill — all data is up to date.")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_and_store, *task): task for task in tasks}
        for fut in as_completed(futures):
            done += 1
            try:
                ticker, table, n = fut.result()
                if done % 100 == 0 or done == total:
                    log.info("  [%d/%d] %s → %s: %d bars", done, total, ticker, table, n)
            except Exception as exc:
                errors += 1
                log.error("  Task failed: %s", exc)

    log.info("Backfill complete — %d tasks, %d errors.", total, errors)


# ─── Reference data ───────────────────────────────────────────────────────────

def run_refresh_reference(tickers: List[str]):
    """Fetch and store float / shares / market-cap for every ticker."""
    log.info("Refreshing reference data for %d tickers...", len(tickers))
    ok = errors = 0

    def _do(ticker):
        ref = fetch_reference(ticker)
        if ref:
            upsert_reference(ticker, ref)
            return True
        return False

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_do, t): t for t in tickers}
        for fut in as_completed(futures):
            try:
                if fut.result():
                    ok += 1
                else:
                    errors += 1
            except Exception as exc:
                errors += 1
                log.error("Reference fetch failed: %s", exc)

    log.info("Reference refresh complete — %d ok, %d errors.", ok, errors)


# ─── Live refresh cycle ───────────────────────────────────────────────────────

def run_live_cycle(tickers: List[str]):
    """
    One live refresh pass: fetch the latest bars for every ticker/timeframe
    and upsert.  Intraday timeframes use today's date; daily uses last 3 days.
    """
    today     = date.today()
    intra_from = str(today)
    intra_to   = str(today)
    daily_from = str(today - timedelta(days=LIVE_DAILY_DAYS))
    daily_to   = str(today)

    tasks = []
    for ticker in tickers:
        for mult, span, table, _ in TIMEFRAMES:
            if span == "day":
                tasks.append((ticker, mult, span, table, daily_from, daily_to))
            else:
                tasks.append((ticker, mult, span, table, intra_from, intra_to))

    total = len(tasks)
    done = errors = bars_total = 0
    cycle_start = time.monotonic()
    _PROGRESS_EVERY = max(1, total // 20)   # log ~20 progress lines per cycle

    log.info("Live cycle starting — %d tasks (%d tickers × %d timeframes), "
             "%d workers, %d req/s.",
             total, len(tickers), len(TIMEFRAMES), MAX_WORKERS, REQUESTS_PER_SECOND)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(_fetch_and_store, *t): t for t in tasks}
        for fut in as_completed(futures):
            done += 1
            try:
                _, _, n = fut.result()
                bars_total += n
            except Exception as exc:
                errors += 1
                log.error("Live task failed: %s", exc)

            if done % _PROGRESS_EVERY == 0 or done == total:
                elapsed = time.monotonic() - cycle_start
                rate    = done / elapsed if elapsed > 0 else 0
                log.info("  [%d/%d]  %.0f req/s  %d bars  %d errors",
                         done, total, rate, bars_total, errors)

    elapsed = time.monotonic() - cycle_start
    log.info("Live cycle done — %d tasks, %d bars, %.1fs, %d errors.",
             total, bars_total, elapsed, errors)


# ─── Live loop ────────────────────────────────────────────────────────────────

def run_live_loop(tickers: List[str], force: bool = False):
    """
    Main live loop.  Runs run_live_cycle() every LIVE_INTERVAL seconds while
    the market is open, sleeps until the next open when the market is closed.

    force=True skips the market-hours check so the loop runs at any time
    (useful for testing outside market hours).

    All sleeps use _shutdown.wait(timeout) so a SIGINT / SIGTERM wakes the
    process immediately and the loop exits cleanly without abandoning threads.
    """
    log.info("Entering live loop (interval=%ds, workers=%d, rate=%d req/s%s).",
             LIVE_INTERVAL, MAX_WORKERS, REQUESTS_PER_SECOND,
             ", FORCE MODE — ignoring market hours" if force else "")

    while not _shutdown.is_set():
        if not force and not is_market_open():
            wait = seconds_until_market_open()
            next_open = datetime.now(tz=ET) + timedelta(seconds=wait)
            log.info("Market closed — sleeping %.0fs until %s ET.",
                     wait, next_open.strftime("%Y-%m-%d %H:%M"))
            _shutdown.wait(timeout=wait)   # wakes immediately on shutdown signal
            if _shutdown.is_set():
                break
            # Reload the ticker universe after the overnight sleep
            tickers = get_tickers()
            log.info("Ticker universe refreshed: %d symbols.", len(tickers))

        cycle_start = time.monotonic()
        run_live_cycle(tickers)
        elapsed = time.monotonic() - cycle_start

        sleep_for = max(0.0, LIVE_INTERVAL - elapsed)
        if sleep_for > 0 and not _shutdown.is_set():
            log.info("Waiting %.1fs until next cycle...", sleep_for)
            _shutdown.wait(timeout=sleep_for)

    log.info("Live loop stopped cleanly.")


# ─── Connectivity tests ───────────────────────────────────────────────────────

def run_test():
    """Verify Polygon API key and MySQL connection."""
    print("\n" + "=" * 70)
    print("  CONNECTIVITY TEST")
    print("=" * 70)

    # Polygon
    print("\n[1] Polygon API...")
    data = _polygon_get("/v1/marketstatus/now")
    if data:
        print(f"    OK — market status: {data.get('market', '?')}")
    else:
        print("    FAILED — could not reach Polygon API")
        return False

    # MySQL
    print("\n[2] MySQL connection...")
    try:
        conn = get_db_conn()
        cur  = conn.cursor()
        cur.execute("SELECT VERSION()")
        ver = cur.fetchone()[0]
        conn.close()
        print(f"    OK — MySQL {ver}")
    except Error as exc:
        print(f"    FAILED — {exc}")
        return False

    # Ticker query
    print("\n[3] Ticker query (trade_finder.tickers)...")
    try:
        tickers = get_tickers()
        print(f"    OK — {len(tickers)} tickers")
        if tickers:
            sample = ", ".join(tickers[:5])
            suffix = " ..." if len(tickers) > 5 else ""
            print(f"    Sample: {sample}{suffix}")
    except Error as exc:
        print(f"    FAILED — {exc}")
        return False

    print("\n" + "=" * 70)
    print("  ALL CHECKS PASSED")
    print("=" * 70 + "\n")
    return True


# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    global REQUESTS_PER_SECOND, MAX_WORKERS, _limiter

    parser = argparse.ArgumentParser(
        description="Polygon.io OHLCV bar fetcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--backfill",           action="store_true",
                        help="Run full historical backfill")
    parser.add_argument("--live",               action="store_true",
                        help="Start 60-second live refresh loop")
    parser.add_argument("--once",               action="store_true",
                        help="Run one live cycle and exit (for cron)")
    parser.add_argument("--refresh-reference",  action="store_true",
                        help="Refresh float / shares / market-cap reference data")
    parser.add_argument("--test",               action="store_true",
                        help="Test API and DB connectivity")
    parser.add_argument("--rate",               type=int, default=REQUESTS_PER_SECOND,
                        metavar="N",
                        help=f"Polygon requests/sec (default: {REQUESTS_PER_SECOND})")
    parser.add_argument("--workers",            type=int, default=MAX_WORKERS,
                        metavar="N",
                        help=f"Parallel HTTP workers (default: {MAX_WORKERS})")
    parser.add_argument("--force",              action="store_true",
                        help="Run live loop regardless of market hours (for testing)")
    args = parser.parse_args()

    # Apply CLI overrides
    REQUESTS_PER_SECOND = args.rate
    MAX_WORKERS         = args.workers
    _limiter            = _RateLimiter(REQUESTS_PER_SECOND)

    # Register shutdown handlers before any threads are spawned
    signal.signal(signal.SIGINT,  _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    if args.test:
        run_test()
        return

    # Initialise the connection pool now that MAX_WORKERS is final
    _init_pool()

    # Always ensure tables exist, then trim stale data
    create_tables()
    log.info("Purging bars outside retention windows...")
    purge_old_bars()

    tickers = get_tickers()
    if not tickers:
        log.error("No tickers returned from trade_finder.tickers — aborting.")
        sys.exit(1)
    log.info("Ticker universe: %d symbols.", len(tickers))

    if args.backfill:
        run_backfill(tickers)

    if args.refresh_reference:
        run_refresh_reference(tickers)

    if args.once:
        run_live_cycle(tickers)
        return

    if args.live:
        run_live_loop(tickers, force=args.force)
        return

    if not any([args.backfill, args.refresh_reference, args.once, args.live]):
        parser.print_help()


if __name__ == "__main__":
    main()
