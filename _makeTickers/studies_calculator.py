"""
Studies Calculator
==================
Reads from the pre-fetched bars_* tables (polygon_bars_fetcher.py) and
calculates technical-analysis indicators for trading strategies, storing
results in study_* tables.  No Polygon API calls are made here.

Architecture
------------
Bars are loaded ONCE per cycle (6 bulk queries regardless of strategy count)
and shared across every registered strategy.  Adding strategy N requires only:

  1. Define _DDL_SN and _UPSERT_SN SQL strings
  2. Define _SN_COLS list (columns matching the INSERT placeholder order)
  3. Write a vectorized calc function and decorate it with @register(...)

The live cycle then automatically includes the new strategy with zero changes
to the orchestration layer.

Modes
-----
  --backfill          Calculate TA for all available historical bars
  --live              1 minute loop — updates last 2 bars per ticker
  --backfill --live   Backfill then enter the live loop
  --once              Single live pass (cron / Task Scheduler)
  --test              Verify DB connectivity and ticker count

Dependencies
------------
  pip install mysql-connector-python pandas
"""

import argparse
import dataclasses
import logging
import signal
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, date, timedelta
from typing import Callable, Dict, List, Optional, Set, Tuple

import mysql.connector
import pandas as pd

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # pip install backports.zoneinfo

# ─── Configuration ────────────────────────────────────────────────────────────

DB_CONFIG: Dict = {
    "host":      "127.0.0.1",
    "user":      "remote",
    "password":  "Chamba4347!",
    "database":  "tradefinder",
    "charset":   "utf8mb4",
    "collation": "utf8mb4_unicode_ci",
}

ET = ZoneInfo("America/New_York")
MARKET_OPEN_H,  MARKET_OPEN_M  = 9,  30
MARKET_CLOSE_H, MARKET_CLOSE_M = 16, 0
LIVE_INTERVAL = 300             # seconds between live cycles (5 minutes)
LIVE_WORKERS  = 4               # parallel strategy threads in the live cycle

# Retention window — must match polygon_bars_fetcher.py
RETENTION_INTRADAY_DAYS  = 14
LIVE_DAILY_LOOKBACK_DAYS = 14   # enough history for 5-day lookback in live mode

TICKER_QUERY = """
    SELECT ticker FROM tickers
    WHERE last_day_volume > 500000
      AND type = 'CS'
      AND last_day_close > 5
       OR ticker = 'SPY' OR ticker = 'QQQ'
    ORDER BY ticker
"""

# ─── Strategy registry ────────────────────────────────────────────────────────
#
# BarCache: dict of pre-loaded bulk DataFrames, one entry per timeframe.
# Keys: 'daily' | 'm1' | 'm5' | 'm15' | 'm30' | 'm60'
# Each DataFrame has columns: ticker, bar_time, open, high, low, close, volume, vwap

BarCache = Dict[str, pd.DataFrame]


@dataclasses.dataclass
class StrategyDef:
    id:            str            # short label, e.g. "s1"
    table:         str            # MySQL table name, e.g. "study_s1"
    ddl:           str            # CREATE TABLE IF NOT EXISTS ...
    upsert_sql:    str            # INSERT ... ON DUPLICATE KEY UPDATE ...
    result_cols:   List[str]      # column names in order matching upsert VALUES(%s,...)
    needs:         Set[str]       # subset of BarCache keys required
    calc:          Callable       # calc(cache: BarCache, live: bool) -> pd.DataFrame
    ticker_filter: Optional[str]  # extra SQL WHERE clause applied to tickers,
                                  # e.g. "last_day_close < 20" pre-qualifies symbols
                                  # so bars for ineligible tickers are never processed.
                                  # None = use the full universe.


_STRATEGIES: List[StrategyDef] = []


def register(sid: str,
             table: str,
             ddl: str,
             upsert_sql: str,
             result_cols: List[str],
             needs: Set[str],
             ticker_filter: Optional[str] = None):
    """
    Decorator that registers a strategy calculation function.

    The decorated function must accept (cache: BarCache, live: bool) and
    return a pd.DataFrame whose columns exactly match *result_cols*.

    Parameters
    ----------
    ticker_filter : optional SQL expression evaluated against tickers columns
        (last_day_close, last_day_volume, float_shares, …).  The base universe
        query is always applied first; this clause narrows it further.
        Example: "last_day_close < 20"

    Usage
    -----
    @register("s2", "study_s2", _DDL_S2, _UPSERT_S2,
              result_cols=["ticker","bar_time",...], needs={"daily","m5"},
              ticker_filter="last_day_close < 20")
    def calc_s2(cache: BarCache, live: bool) -> pd.DataFrame:
        ...
        return result_df
    """
    def decorator(fn: Callable) -> Callable:
        _STRATEGIES.append(StrategyDef(
            id=sid, table=table, ddl=ddl, upsert_sql=upsert_sql,
            result_cols=result_cols, needs=needs, calc=fn,
            ticker_filter=ticker_filter,
        ))
        return fn
    return decorator

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)-5s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

# ─── Graceful shutdown ────────────────────────────────────────────────────────

_shutdown = threading.Event()


def _request_shutdown(sig=None, frame=None):
    log.info("Shutdown requested — finishing current cycle then stopping...")
    _shutdown.set()

# ─── Connection pool ──────────────────────────────────────────────────────────

_pool: Optional[mysql.connector.pooling.MySQLConnectionPool] = None

# ─── Qualified-ticker TTL cache ───────────────────────────────────────────────
# _load_qualified_tickers() is called once per strategy per live cycle.
# Tickers very rarely change during a session, so we cache the result for
# _TICKER_CACHE_TTL seconds to avoid the extra DB round-trip every 60 s.

_qualified_cache: Dict[str, Tuple[float, Set[str]]] = {}
_TICKER_CACHE_TTL = 300  # seconds (5 minutes)


def _init_pool():
    global _pool
    _pool = mysql.connector.pooling.MySQLConnectionPool(
        pool_name="studies_pool",
        pool_size=8,
        **DB_CONFIG,
    )
    log.info("MySQL connection pool ready (8 connections).")


def get_db_conn() -> mysql.connector.MySQLConnection:
    if _pool is not None:
        return _pool.get_connection()
    return mysql.connector.connect(**DB_CONFIG)

# ─── Generic database helpers ─────────────────────────────────────────────────

def create_tables():
    """Create every registered strategy's table if it doesn't exist."""
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        for s in _STRATEGIES:
            cur.execute(s.ddl)
        conn.commit()
        log.info("Study tables verified / created (%d strategies).", len(_STRATEGIES))
    finally:
        conn.close()


def get_tickers() -> List[str]:
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(TICKER_QUERY)
        return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def _load_qualified_tickers(ticker_filter: str) -> Set[str]:
    """
    Return the subset of the base ticker universe that also satisfies
    *ticker_filter* — a SQL expression referencing tickers columns
    (e.g. "last_day_close < 20").

    Results are cached for _TICKER_CACHE_TTL seconds so that repeated
    live-cycle calls do not hit the DB every 60 seconds.
    """
    now = time.monotonic()
    cached = _qualified_cache.get(ticker_filter)
    if cached and (now - cached[0]) < _TICKER_CACHE_TTL:
        return cached[1]

    sql = f"""
        SELECT ticker FROM tickers
        WHERE (
            (last_day_volume > 500000 AND type = 'CS' AND last_day_close > 5)
            OR ticker = 'SPY' OR ticker = 'QQQ'
        )
        AND ({ticker_filter})
        ORDER BY ticker
    """
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        result: Set[str] = {r[0] for r in cur.fetchall()}
    finally:
        conn.close()

    _qualified_cache[ticker_filter] = (now, result)
    return result


def _filter_cache(cache: BarCache, tickers: Set[str]) -> BarCache:
    """Return a new BarCache containing only rows for *tickers*."""
    return {
        tf: df[df["ticker"].isin(tickers)].reset_index(drop=True)
        for tf, df in cache.items()
    }


def purge_old_studies():
    """Trim every study table to the intraday retention window."""
    cutoff = date.today() - timedelta(days=RETENTION_INTRADAY_DAYS)
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        total = 0
        for s in _STRATEGIES:
            cur.execute(f"DELETE FROM `{s.table}` WHERE bar_time < %s", (cutoff,))
            n = cur.rowcount
            if n:
                log.info("  Purged %6d rows from %-15s (before %s)", n, s.table, cutoff)
            total += n
        conn.commit()
        if total:
            log.info("Purge complete — %d total rows removed.", total)
        else:
            log.info("Purge complete — all tables within retention window.")
    finally:
        conn.close()


_BATCH = 500


def _upsert(upsert_sql: str, rows: List[Tuple]):
    """Batch-upsert rows into the target strategy table."""
    if not rows:
        return
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        for i in range(0, len(rows), _BATCH):
            cur.executemany(upsert_sql, rows[i : i + _BATCH])
        conn.commit()
    finally:
        conn.close()


def _df_to_rows(df: pd.DataFrame, cols: List[str]) -> List[Tuple]:
    """
    Extract *cols* from df and return MySQL-safe tuples:
      • bar_time  → Python datetime
      • bool cols → int (0 / 1)
      • NaN / NaT → None
    """
    sub = df[cols].copy()

    # bar_time → Python datetime
    if "bar_time" in sub.columns:
        sub["bar_time"] = [
            x.to_pydatetime() if pd.notna(x) else None
            for x in sub["bar_time"]
        ]

    # bool → int  (pandas bool columns)
    for col in sub.select_dtypes(include="bool").columns:
        sub[col] = sub[col].astype(int)

    # Replace all remaining NaN / None / NaT with None
    sub = sub.astype(object).where(pd.notna(sub), None)

    return list(sub.itertuples(index=False, name=None))

# ─── Bulk bar loading ─────────────────────────────────────────────────────────

_TIMEFRAME_TABLE: Dict[str, str] = {
    "daily": "bars_daily",
    "m1":    "bars_1min",
    "m5":    "bars_5min",
    "m15":   "bars_15min",
    "m30":   "bars_30min",
    "m60":   "bars_60min",
}

_EMPTY_BARS = pd.DataFrame(
    columns=["ticker", "bar_time", "open", "high", "low", "close", "volume", "vwap"]
)


def _load_all_bars_bulk(table: str,
                        from_date: Optional[date] = None) -> pd.DataFrame:
    """One SQL query → DataFrame with ticker column included."""
    cols = ["ticker", "bar_time", "open", "high", "low", "close", "volume", "vwap"]
    conn = get_db_conn()
    try:
        cur = conn.cursor()
        sql = f"SELECT {', '.join(cols)} FROM `{table}`"
        params: List = []
        if from_date:
            # Use a direct datetime comparison so MySQL can use the bar_time index.
            # DATE(bar_time) >= %s forces a full scan; bar_time >= %s does not.
            sql += " WHERE bar_time >= %s"
            params.append(datetime.combine(from_date, datetime.min.time()))
        sql += " ORDER BY ticker, bar_time"
        cur.execute(sql, params if params else None)
        rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame(columns=cols, dtype=object)

    df = pd.DataFrame(rows, columns=cols)
    df["bar_time"] = pd.to_datetime(df["bar_time"])
    for col in ("open", "high", "low", "close", "volume", "vwap"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _build_bar_cache(backfill: bool = False) -> BarCache:
    """
    Load every timeframe needed by at least one registered strategy.
    Queries are 6 max regardless of strategy count.

    backfill=True  → full retention history for all intraday timeframes
    backfill=False → today only for intraday; 14-day window for daily
    """
    today         = date.today()
    intraday_from = (today - timedelta(days=RETENTION_INTRADAY_DAYS)) if backfill else today
    daily_from    = None if backfill else (today - timedelta(days=LIVE_DAILY_LOOKBACK_DAYS))

    needed: Set[str] = set().union(*[s.needs for s in _STRATEGIES])

    log.info("Loading bars (%s mode, timeframes: %s)...",
             "backfill" if backfill else "live", sorted(needed))

    cache: BarCache = {}
    for tf in sorted(needed):
        table = _TIMEFRAME_TABLE.get(tf)
        if not table:
            log.warning("Unknown timeframe key '%s' — skipped.", tf)
            continue
        from_dt = daily_from if tf == "daily" else intraday_from
        t0 = time.monotonic()
        df = _load_all_bars_bulk(table, from_dt)
        log.info("  %-7s  %8d rows  (%.1fs)", tf, len(df), time.monotonic() - t0)
        cache[tf] = df

    return cache

# ─── Strategy 1 — Low Bounce ──────────────────────────────────────────────────
#
# Trigger: stock < $20, in the lower 25% of its 5-day range,
#          crosses above prior-day-close resistance,
#          and is simultaneously making a new 60-min session high.
#
# Primary timeframe : 5-min bars
# Lookback sources  : bars_daily (5-day range + resistance), bars_60min (session high)

_DDL_S1 = """\
CREATE TABLE IF NOT EXISTS `study_s1` (
  `ticker`                 VARCHAR(20)   NOT NULL,
  `bar_time`               DATETIME      NOT NULL  COMMENT '5-min bar start (ET)',
  `close`                  DECIMAL(18,4) DEFAULT NULL,
  `high_5d`                DECIMAL(18,4) DEFAULT NULL COMMENT 'Max high — last 5 daily bars',
  `low_5d`                 DECIMAL(18,4) DEFAULT NULL COMMENT 'Min low  — last 5 daily bars',
  `range_5d`               DECIMAL(18,4) DEFAULT NULL,
  `pct_from_5d_low`        DECIMAL(8,2)  DEFAULT NULL COMMENT '0=at low, 100=at high',
  `resistance`             DECIMAL(18,4) DEFAULT NULL COMMENT 'Prior day close',
  `prev_bar_close`         DECIMAL(18,4) DEFAULT NULL,
  `high_60min`             DECIMAL(18,4) DEFAULT NULL COMMENT 'Session 60-min high',
  `is_near_5d_low`         TINYINT(1)    DEFAULT NULL COMMENT 'pct_from_5d_low < 25',
  `cross_above_resistance` TINYINT(1)    DEFAULT NULL,
  `new_60min_high`         TINYINT(1)    DEFAULT NULL,
  `trigger_fired`          TINYINT(1)    DEFAULT NULL,
  `updated_at`             DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_UPSERT_S1 = """\
INSERT INTO `study_s1`
  (ticker, bar_time, close, high_5d, low_5d, range_5d, pct_from_5d_low,
   resistance, prev_bar_close, high_60min,
   is_near_5d_low, cross_above_resistance, new_60min_high, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close                  = VALUES(close),
  high_5d                = VALUES(high_5d),
  low_5d                 = VALUES(low_5d),
  range_5d               = VALUES(range_5d),
  pct_from_5d_low        = VALUES(pct_from_5d_low),
  resistance             = VALUES(resistance),
  prev_bar_close         = VALUES(prev_bar_close),
  high_60min             = VALUES(high_60min),
  is_near_5d_low         = VALUES(is_near_5d_low),
  cross_above_resistance = VALUES(cross_above_resistance),
  new_60min_high         = VALUES(new_60min_high),
  trigger_fired          = VALUES(trigger_fired)
"""

_S1_COLS = [
    "ticker", "bar_time", "close",
    "high_5d", "low_5d", "range_5d", "pct_from_5d_low",
    "resistance", "prev_bar_close", "high_60min",
    "is_near_5d_low", "cross_above_resistance", "new_60min_high", "trigger_fired",
]


@register("s1", "study_s1", _DDL_S1, _UPSERT_S1,
          result_cols=_S1_COLS, needs={"daily", "m5", "m60"},
          ticker_filter="last_day_close < 20")
def calc_s1(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Fully vectorized Strategy 1 calculation — no Python row loops.

    Uses pd.merge_asof to align each 5-min bar with the prior trading day's
    daily metrics (rolling 5-day high/low, resistance = prior close).
    The trick: daily bar_time is nudged to 17:00 ET so merge_asof's backward
    search naturally selects the PRIOR day's bar for any intraday bar starting
    at 09:30+, without needing an explicit date-subtraction step.
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)
    m60   = cache.get("m60",   _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S1_COLS)

    # ── Step 1: Daily rolling metrics ─────────────────────────────────────────
    # rolling(5) at position i  = window [i-4 … i]  (includes today)
    # After merge_asof with merge_time = bar_time+17h, each 5-min bar on date D
    # is matched against the daily bar for D-1 (at D-1 17:00).
    # So high_5d from the matched row = max(D-5 … D-1). ✓
    d = daily.sort_values(["ticker", "bar_time"]).copy()
    d["high_5d"] = d.groupby("ticker")["high"].transform(
        lambda x: x.rolling(5, min_periods=5).max()
    )
    d["low_5d"] = d.groupby("ticker")["low"].transform(
        lambda x: x.rolling(5, min_periods=5).min()
    )
    # resistance = close of the matched daily bar (= prior day close once merged)
    d["merge_time"] = d["bar_time"] + pd.Timedelta(hours=17)

    daily_lookup = (
        d[["ticker", "merge_time", "high_5d", "low_5d", "close"]]
        .rename(columns={"close": "resistance"})
        .sort_values("merge_time")   # merge_asof requires right key globally sorted
    )

    # ── Step 2: 60-min session highs ──────────────────────────────────────────
    if not m60.empty:
        m60c = m60.copy()
        m60c["date"] = m60c["bar_time"].dt.normalize()
        session_high = (
            m60c.groupby(["ticker", "date"])["high"]
            .max()
            .reset_index()
            .rename(columns={"high": "high_60min"})
        )
    else:
        session_high = pd.DataFrame(columns=["ticker", "date", "high_60min"])

    # ── Step 3: Prepare 5-min bars ────────────────────────────────────────────
    # Sort by (ticker, bar_time) first so groupby shift gives the correct
    # per-ticker previous bar.  Then re-sort by bar_time alone, because
    # merge_asof requires the left join key to be globally monotone.
    m = m5.sort_values(["ticker", "bar_time"]).copy()
    m["date"]           = m["bar_time"].dt.normalize()
    m["prev_bar_close"] = m.groupby("ticker")["close"].shift(1)
    m = m.sort_values("bar_time")   # global sort required by merge_asof

    # ── Step 4: Merge lookups ─────────────────────────────────────────────────
    # merge_asof (backward): each 5-min bar finds the latest daily merge_time ≤ bar_time
    m = pd.merge_asof(
        m,
        daily_lookup,
        left_on="bar_time",
        right_on="merge_time",
        by="ticker",
        direction="backward",
    )
    # Exact date join for 60-min session high
    m = m.merge(session_high, on=["ticker", "date"], how="left")

    # ── Step 5: Indicators ────────────────────────────────────────────────────
    m["range_5d"] = (m["high_5d"] - m["low_5d"]).round(4)

    valid_range = m["range_5d"] > 0
    m["pct_from_5d_low"] = (
        ((m["close"] - m["low_5d"]) / m["range_5d"] * 100)
        .where(valid_range)
        .round(2)
    )

    m["is_near_5d_low"] = (m["pct_from_5d_low"] < 25).fillna(False)

    m["cross_above_resistance"] = (
        m["prev_bar_close"].notna()
        & m["resistance"].notna()
        & (m["close"] > m["resistance"])
        & (m["prev_bar_close"] <= m["resistance"])
    ).fillna(False)

    m["new_60min_high"] = (
        m["high_60min"].notna() & (m["close"] >= m["high_60min"])
    ).fillna(False)

    m["trigger_fired"] = (
        (m["close"] < 20)
        & m["is_near_5d_low"]
        & m["cross_above_resistance"]
        & m["new_60min_high"]
    )

    # ── Step 6: Round price columns ───────────────────────────────────────────
    for col in ("close", "high_5d", "low_5d", "resistance", "prev_bar_close", "high_60min"):
        m[col] = m[col].round(4)

    # ── Step 7: Live mode — keep last 2 bars per ticker only ─────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S1_COLS]

# ─── Strategy 2 — Bullish Pullback (Alpha Predators) ─────────────────────────
#
# Reference: check_alpha_predators() — four-gate logic
#   Gate 1: price < $20, 5-day uptrend, first bar of session opened above prev close
#   Gate 2: close > EMA9, close > EMA20, EMA9 > EMA20 (5-min EMAs, stacked)
#   Gate 3: session made ≥1% move above prev close, then pulled back ≥1% from that high
#   Gate 4: trigger candle is green, just crossed back above EMA9, and is higher than 60 min ago
#
# Entry: bar close.  Stop: min(bar low, EMA9).

_DDL_S2 = """\
CREATE TABLE IF NOT EXISTS `study_s2` (
  `ticker`        VARCHAR(20)   NOT NULL,
  `bar_time`      DATETIME      NOT NULL   COMMENT '5-min bar start (ET)',
  `close`         DECIMAL(18,4) DEFAULT NULL,
  `green_5d`      TINYINT(1)    DEFAULT NULL COMMENT 'Gate1: daily close > close 5 days ago',
  `above_prev`    TINYINT(1)    DEFAULT NULL COMMENT 'Gate1: first session bar closed above prev day close',
  `ema_9`         DECIMAL(18,4) DEFAULT NULL COMMENT 'Gate2: EMA(9) of 5-min closes',
  `ema_20`        DECIMAL(18,4) DEFAULT NULL COMMENT 'Gate2: EMA(20) of 5-min closes',
  `emas_stacked`  TINYINT(1)    DEFAULT NULL COMMENT 'Gate2: EMA9 > EMA20',
  `session_high`  DECIMAL(18,4) DEFAULT NULL COMMENT 'Gate3: running session high',
  `made_move`     TINYINT(1)    DEFAULT NULL COMMENT 'Gate3: session high > prev_close * 1.01',
  `pulled_back`   TINYINT(1)    DEFAULT NULL COMMENT 'Gate3: current close >= 1% below session high',
  `green_candle`  TINYINT(1)    DEFAULT NULL COMMENT 'Gate4: close > open',
  `cross_ema9`    TINYINT(1)    DEFAULT NULL COMMENT 'Gate4: price crossed back above EMA9 this bar',
  `hour_positive` TINYINT(1)    DEFAULT NULL COMMENT 'Gate4: close > close 12 x 5-min bars ago',
  `entry`         DECIMAL(18,4) DEFAULT NULL COMMENT 'Suggested entry (bar close)',
  `stop`          DECIMAL(18,4) DEFAULT NULL COMMENT 'Suggested stop: min(bar low, EMA9)',
  `trigger_fired` TINYINT(1)    DEFAULT NULL,
  `updated_at`    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""

_UPSERT_S2 = """\
INSERT INTO `study_s2`
  (ticker, bar_time, close, green_5d, above_prev, ema_9, ema_20, emas_stacked,
   session_high, made_move, pulled_back, green_candle, cross_ema9, hour_positive,
   entry, stop, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close         = VALUES(close),
  green_5d      = VALUES(green_5d),
  above_prev    = VALUES(above_prev),
  ema_9         = VALUES(ema_9),
  ema_20        = VALUES(ema_20),
  emas_stacked  = VALUES(emas_stacked),
  session_high  = VALUES(session_high),
  made_move     = VALUES(made_move),
  pulled_back   = VALUES(pulled_back),
  green_candle  = VALUES(green_candle),
  cross_ema9    = VALUES(cross_ema9),
  hour_positive = VALUES(hour_positive),
  entry         = VALUES(entry),
  stop          = VALUES(stop),
  trigger_fired = VALUES(trigger_fired)"""

_S2_COLS = [
    "ticker", "bar_time", "close",
    "green_5d", "above_prev",
    "ema_9", "ema_20", "emas_stacked",
    "session_high", "made_move", "pulled_back",
    "green_candle", "cross_ema9", "hour_positive",
    "entry", "stop",
    "trigger_fired",
]


@register("s2", "study_s2", _DDL_S2, _UPSERT_S2,
          result_cols=_S2_COLS, needs={"daily", "m5"},
          ticker_filter="last_day_close < 20")
def calc_s2(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Strategy 2 — Bullish Pullback (Alpha Predators)

    Vectorised translation of check_alpha_predators():
      Gate 1 – pre-conditions  : price < $20, 5-day uptrend, gap above prior close
      Gate 2 – MA structure    : above EMA9, above EMA20, EMA9 > EMA20 (5-min)
      Gate 3 – pullback exists : session moved ≥1% above prev close then fell ≥1% from high
      Gate 4 – trigger candle  : green bar, just crossed back above EMA9, higher than 60 min ago
    Entry = bar close.  Stop = min(bar low, EMA9).
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S2_COLS)

    # ── Step 1: Per-ticker / per-session operations (ticker+bar_time sort) ────
    m = m5.sort_values(["ticker", "bar_time"]).copy()
    m["date"] = m["bar_time"].dt.normalize()

    # 5-min EMAs
    m["ema_9"]  = m.groupby("ticker")["close"].transform(
        lambda x: x.ewm(span=9, adjust=False).mean()
    )
    m["ema_20"] = m.groupby("ticker")["close"].transform(
        lambda x: x.ewm(span=20, adjust=False).mean()
    )

    # Previous bar values needed for cross detection
    m["prev_bar_close"] = m.groupby("ticker")["close"].shift(1)
    m["prev_ema_9"]     = m.groupby("ticker")["ema_9"].shift(1)

    # Close 12 bars ago *within the same session* (= 60 min on 5-min bars)
    m["close_12bar_ago"] = m.groupby(["ticker", "date"])["close"].shift(12)

    # Running session high (expanding max within each ticker+day)
    m["session_high"] = m.groupby(["ticker", "date"])["high"].cummax()

    # ── Step 2: Daily lookups — prev_day_close + green_5d ────────────────────
    # Both are shifted from daily closes then carried to intraday via merge_asof.
    d = daily.sort_values(["ticker", "bar_time"]).copy()
    d["prev_day_close"] = d.groupby("ticker")["close"].shift(1)
    d["close_5d_ago"]   = d.groupby("ticker")["close"].shift(5)
    d["green_5d"]       = (
        (d["close"] > d["close_5d_ago"]) & d["close_5d_ago"].notna()
    )
    d["merge_time"] = d["bar_time"] + pd.Timedelta(hours=17)
    daily_lookup = (
        d[["ticker", "merge_time", "prev_day_close", "green_5d"]]
        .dropna(subset=["prev_day_close"])
        .sort_values("merge_time")
    )

    # ── Step 3: Sort by bar_time globally for merge_asof ─────────────────────
    m = m.sort_values("bar_time")

    m = pd.merge_asof(
        m, daily_lookup,
        left_on="bar_time", right_on="merge_time",
        by="ticker", direction="backward",
    )

    # ── Step 4: Gate 1 — above_prev ──────────────────────────────────────────
    # "First bar of the session opened above prior close."
    # Use transform("first") to broadcast the opening bar's close to all rows
    # in the session, then compare to the merged prev_day_close.
    m["session_first_close"] = m.groupby(["ticker", "date"])["close"].transform("first")
    m["above_prev"] = (
        m["prev_day_close"].notna()
        & (m["session_first_close"] > m["prev_day_close"])
    )

    # ── Step 5: Gate indicators ───────────────────────────────────────────────
    # Gate 2
    m["emas_stacked"] = m["ema_9"] > m["ema_20"]

    # Gate 3
    m["made_move"] = (
        m["prev_day_close"].notna()
        & (m["session_high"] > m["prev_day_close"] * 1.01)
    )
    valid_sh = m["session_high"].notna() & (m["session_high"] > 0)
    m["pulled_back"] = valid_sh & (
        (m["session_high"] - m["close"]) / m["session_high"] > 0.01
    )

    # Gate 4
    m["green_candle"] = m["close"] > m["open"]
    m["cross_ema9"] = (
        m["prev_bar_close"].notna()
        & m["prev_ema_9"].notna()
        & (m["close"] > m["ema_9"])
        & (m["prev_bar_close"] <= m["prev_ema_9"])
    )
    m["hour_positive"] = (
        m["close_12bar_ago"].notna()
        & (m["close"] > m["close_12bar_ago"])
    )

    # ── Step 6: Entry / stop ──────────────────────────────────────────────────
    m["entry"] = m["close"].round(4)
    m["stop"]  = m[["low", "ema_9"]].min(axis=1).round(4)

    # ── Step 7: Trigger — all four gates ─────────────────────────────────────
    m["trigger_fired"] = (
        (m["close"] < 20)                                  # Gate 1 price
        & m["green_5d"].fillna(False).astype(bool)         # Gate 1 trend
        & m["above_prev"].fillna(False)                    # Gate 1 gap-up
        & (m["close"] > m["ema_9"])                        # Gate 2
        & (m["close"] > m["ema_20"])                       # Gate 2
        & m["emas_stacked"]                                # Gate 2
        & m["made_move"]                                   # Gate 3
        & m["pulled_back"]                                 # Gate 3
        & m["green_candle"]                                # Gate 4
        & m["cross_ema9"]                                  # Gate 4
        & m["hour_positive"]                               # Gate 4
    )

    # ── Step 8: Round price columns ───────────────────────────────────────────
    for col in ("close", "ema_9", "ema_20", "session_high"):
        m[col] = m[col].round(4)

    # ── Step 9: Live mode — last 2 bars per ticker only ───────────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S2_COLS]

# ─────────────────────────────────────────────────────────────────────────────

# ─── Strategy 3 — Downward Momentum / Short Breakdown ────────────────────────

_DDL_S3 = """\
CREATE TABLE IF NOT EXISTS `study_s3` (
  `ticker`              VARCHAR(20)   NOT NULL,
  `bar_time`            DATETIME      NOT NULL   COMMENT '5-min bar start (ET)',
  `close`               DECIMAL(18,4) DEFAULT NULL,
  `prev_day_close`      DECIMAL(18,4) DEFAULT NULL COMMENT 'Prior session close (breakdown support level)',
  `down_from_prev`      TINYINT(1)    DEFAULT NULL COMMENT 'Close < prior day close',
  `breakdown`           TINYINT(1)    DEFAULT NULL COMMENT 'Crossed below support this bar',
  `spy_5m_change`       DECIMAL(8,4)  DEFAULT NULL COMMENT 'SPY % change on the current 5-min bar',
  `market_ok_to_short`  TINYINT(1)    DEFAULT NULL COMMENT 'SPY 5-min change <= 0.15%',
  `trigger_fired`       TINYINT(1)    DEFAULT NULL,
  `updated_at`          DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;"""

_UPSERT_S3 = """\
INSERT INTO `study_s3`
  (ticker, bar_time, close, prev_day_close, down_from_prev,
   breakdown, spy_5m_change, market_ok_to_short, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close              = VALUES(close),
  prev_day_close     = VALUES(prev_day_close),
  down_from_prev     = VALUES(down_from_prev),
  breakdown          = VALUES(breakdown),
  spy_5m_change      = VALUES(spy_5m_change),
  market_ok_to_short = VALUES(market_ok_to_short),
  trigger_fired      = VALUES(trigger_fired)"""

_S3_COLS = [
    "ticker", "bar_time", "close",
    "prev_day_close", "down_from_prev", "breakdown",
    "spy_5m_change", "market_ok_to_short", "trigger_fired",
]

# SPY is explicitly kept in the cache (it doesn't pass the price band filter)
# so it can be used as the market filter inside calc_s3.
_S3_FILTER = "(last_day_close BETWEEN 15 AND 85) OR ticker = 'SPY'"


@register("s3", "study_s3", _DDL_S3, _UPSERT_S3,
          result_cols=_S3_COLS, needs={"daily", "m5"},
          ticker_filter=_S3_FILTER)
def calc_s3(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Strategy 3 — Downward Momentum / Short Breakdown

    Stocks $15–$85 that are already below the prior day close fire when they
    cross DOWN through that close level on a 5-min bar, provided SPY's
    current 5-min bar change is not surging (≤ +0.15%).

    SPY bars are included in the cache via the ticker_filter override but are
    stripped from the final output so no SPY rows land in study_s3.
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S3_COLS)

    # ── Step 1: Separate SPY from tradeable stocks ────────────────────────────
    spy_m5   = m5[m5["ticker"] == "SPY"].copy()
    stock_m5 = m5[m5["ticker"] != "SPY"].copy()

    if stock_m5.empty:
        return pd.DataFrame(columns=_S3_COLS)

    # ── Step 2: SPY 5-min bar change lookup ───────────────────────────────────
    # For each stock bar at time T, find the SPY bar that started at or just
    # before T and report its intra-bar % change (open → close).
    if not spy_m5.empty:
        spy_m5 = spy_m5.sort_values("bar_time").copy()
        valid_spy_open = spy_m5["open"].notna() & (spy_m5["open"] != 0)
        spy_m5["spy_5m_change"] = (
            ((spy_m5["close"] - spy_m5["open"]) / spy_m5["open"] * 100)
            .where(valid_spy_open)
            .round(4)
        )
        spy_lookup = spy_m5[["bar_time", "spy_5m_change"]].rename(
            columns={"bar_time": "spy_bar_time"}
        )
    else:
        spy_lookup = pd.DataFrame(columns=["spy_bar_time", "spy_5m_change"])

    # ── Step 3: Per-ticker operations (ticker+bar_time sort) ─────────────────
    m = stock_m5.sort_values(["ticker", "bar_time"]).copy()
    m["prev_bar_close"] = m.groupby("ticker")["close"].shift(1)

    # ── Step 4: Daily prev_day_close lookup ───────────────────────────────────
    # prev_day_close is the prior session's close — used as the support/
    # breakdown level.  Shift daily closes by 1 so each day's entry carries
    # the *previous* day's close.
    d = daily[daily["ticker"] != "SPY"].sort_values(["ticker", "bar_time"]).copy()
    d["prev_day_close"] = d.groupby("ticker")["close"].shift(1)
    d["merge_time"]     = d["bar_time"] + pd.Timedelta(hours=17)
    daily_lookup = (
        d[["ticker", "merge_time", "prev_day_close"]]
        .dropna(subset=["prev_day_close"])
        .sort_values("merge_time")
    )

    # ── Step 5: Sort by bar_time globally for merge_asof ─────────────────────
    m = m.sort_values("bar_time")

    m = pd.merge_asof(
        m, daily_lookup,
        left_on="bar_time", right_on="merge_time",
        by="ticker", direction="backward",
    )

    if not spy_lookup.empty:
        spy_lookup = spy_lookup.sort_values("spy_bar_time")
        m = pd.merge_asof(
            m, spy_lookup,
            left_on="bar_time", right_on="spy_bar_time",
            direction="backward",
        )
    else:
        m["spy_5m_change"] = None

    # ── Step 6: Indicators ────────────────────────────────────────────────────
    m["down_from_prev"] = (
        m["prev_day_close"].notna() & (m["close"] < m["prev_day_close"])
    ).fillna(False)

    # breakdown: this bar closed below support AND the previous bar was at or
    # above it — i.e. the cross happened on THIS bar.
    m["breakdown"] = (
        m["prev_bar_close"].notna()
        & m["prev_day_close"].notna()
        & (m["close"] < m["prev_day_close"])
        & (m["prev_bar_close"] >= m["prev_day_close"])
    ).fillna(False)

    # If SPY data is absent treat the market filter as passing (don't block signal).
    m["market_ok_to_short"] = (
        m["spy_5m_change"].isna() | (m["spy_5m_change"] <= 0.15)
    ).fillna(True)

    m["trigger_fired"] = (
        (m["close"] >= 15)
        & (m["close"] <= 85)
        & m["down_from_prev"]
        & m["breakdown"]
        & m["market_ok_to_short"]
    )

    # ── Step 7: Round price columns ───────────────────────────────────────────
    for col in ("close", "prev_day_close"):
        if col in m.columns:
            m[col] = m[col].round(4)

    # ── Step 8: Live mode — last 2 bars per ticker only ───────────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S3_COLS]

# ─────────────────────────────────────────────────────────────────────────────

# ─── Strategy 4 — Resistance Breakout on Above-Average Relative Volume ────────
#
# Trigger: stock $10–$150, at least 125 k shares traded intraday,
#          close above the prior-day range midpoint,
#          relative volume > 1.0x (vs 10-day avg),
#          fresh breakout above prior-day-high (resistance),
#          AND the broad market (SPY 30-min bar) is flat (≤ 0.5% change).
#
# Primary timeframe : 5-min bars
# Lookback sources  : bars_daily (prev high/low/volume), bars_30min (SPY filter)

_DDL_S4 = """\
CREATE TABLE IF NOT EXISTS `study_s4` (
  `ticker`           VARCHAR(20)    NOT NULL,
  `bar_time`         DATETIME       NOT NULL   COMMENT '5-min bar start (ET)',
  `close`            DECIMAL(18,4)  DEFAULT NULL,
  `resistance`       DECIMAL(18,4)  DEFAULT NULL  COMMENT 'Prior day high — breakout level',
  `prev_range_mid`   DECIMAL(18,4)  DEFAULT NULL  COMMENT '(prev_high + prev_low) / 2',
  `avg_volume_10d`   DECIMAL(18,2)  DEFAULT NULL  COMMENT '10-day avg daily volume',
  `today_volume`     BIGINT         DEFAULT NULL  COMMENT 'Cumulative intraday volume at bar',
  `relative_volume`  DECIMAL(8,4)   DEFAULT NULL  COMMENT 'today_volume / avg_volume_10d',
  `above_range_mid`  TINYINT(1)     DEFAULT NULL  COMMENT 'close > prev_range_mid',
  `shares_traded_ok` TINYINT(1)     DEFAULT NULL  COMMENT 'today_volume >= 125000',
  `breakout`         TINYINT(1)     DEFAULT NULL  COMMENT 'Cross above resistance this bar',
  `spy_30m_change`   DECIMAL(8,4)   DEFAULT NULL  COMMENT 'Abs % change of current SPY 30-min bar',
  `market_flat`      TINYINT(1)     DEFAULT NULL  COMMENT 'spy_30m_change <= 0.5',
  `entry`            DECIMAL(18,4)  DEFAULT NULL  COMMENT 'Suggested entry — bar close',
  `stop`             DECIMAL(18,4)  DEFAULT NULL  COMMENT 'Suggested stop — prior day low',
  `trigger_fired`    TINYINT(1)     DEFAULT NULL,
  `updated_at`       DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_UPSERT_S4 = """\
INSERT INTO `study_s4`
  (ticker, bar_time, close, resistance, prev_range_mid, avg_volume_10d,
   today_volume, relative_volume, above_range_mid, shares_traded_ok,
   breakout, spy_30m_change, market_flat, entry, stop, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close             = VALUES(close),
  resistance        = VALUES(resistance),
  prev_range_mid    = VALUES(prev_range_mid),
  avg_volume_10d    = VALUES(avg_volume_10d),
  today_volume      = VALUES(today_volume),
  relative_volume   = VALUES(relative_volume),
  above_range_mid   = VALUES(above_range_mid),
  shares_traded_ok  = VALUES(shares_traded_ok),
  breakout          = VALUES(breakout),
  spy_30m_change    = VALUES(spy_30m_change),
  market_flat       = VALUES(market_flat),
  entry             = VALUES(entry),
  stop              = VALUES(stop),
  trigger_fired     = VALUES(trigger_fired)
"""

_S4_COLS = [
    "ticker", "bar_time", "close",
    "resistance", "prev_range_mid",
    "avg_volume_10d", "today_volume", "relative_volume",
    "above_range_mid", "shares_traded_ok",
    "breakout", "spy_30m_change", "market_flat",
    "entry", "stop", "trigger_fired",
]

# Keep SPY in the cache so it can drive the market-flat filter
_S4_FILTER = "(last_day_close BETWEEN 10 AND 150) OR ticker = 'SPY'"


@register("s4", "study_s4", _DDL_S4, _UPSERT_S4,
          result_cols=_S4_COLS, needs={"daily", "m5", "m30"},
          ticker_filter=_S4_FILTER)
def calc_s4(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Strategy 4 — Resistance Breakout on Above-Average Relative Volume

    Conditions (all must be true on the same 5-min bar):
      1. Price $10–$150
      2. today_volume >= 125,000 shares
      3. close > (prev_day_high + prev_day_low) / 2  (above mid of prior range)
      4. relative_volume > 1.0  (vs 10-day avg daily volume)
      5. Breakout: close > prev_day_high  AND  prev_bar_close <= prev_day_high
      6. Market flat: abs(SPY 30-min bar % change) <= 0.5%

    Entry = bar close.  Stop = prior day low.
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)
    m30   = cache.get("m30",   _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S4_COLS)

    # ── Step 1: Separate SPY 30-min bars from tradeable stocks ───────────────
    spy_m30   = m30[m30["ticker"] == "SPY"].copy() if not m30.empty else pd.DataFrame()
    stock_m5  = m5[m5["ticker"]  != "SPY"].copy()

    if stock_m5.empty:
        return pd.DataFrame(columns=_S4_COLS)

    # ── Step 2: SPY 30-min market-flat lookup ────────────────────────────────
    # For each stock 5-min bar at T, find the SPY 30-min bar starting at or
    # before T and report its abs intra-bar % change.
    if not spy_m30.empty:
        spy_m30 = spy_m30.sort_values("bar_time").copy()
        valid_open = spy_m30["open"].notna() & (spy_m30["open"] != 0)
        spy_m30["spy_30m_change"] = (
            ((spy_m30["close"] - spy_m30["open"]).abs() / spy_m30["open"] * 100)
            .where(valid_open)
            .round(4)
        )
        spy_lookup = spy_m30[["bar_time", "spy_30m_change"]].rename(
            columns={"bar_time": "spy_bar_time"}
        )
    else:
        spy_lookup = pd.DataFrame(columns=["spy_bar_time", "spy_30m_change"])

    # ── Step 3: Daily lookups — resistance, range mid, avg volume ────────────
    # Shift so each entry reflects the *prior* day's values.
    d = daily[daily["ticker"] != "SPY"].sort_values(["ticker", "bar_time"]).copy()
    d["prev_high"]  = d.groupby("ticker")["high"].shift(1)
    d["prev_low"]   = d.groupby("ticker")["low"].shift(1)
    d["avg_vol_10d"] = d.groupby("ticker")["volume"].transform(
        lambda x: x.rolling(10, min_periods=5).mean().shift(1)
    )
    d["merge_time"] = d["bar_time"] + pd.Timedelta(hours=17)

    daily_lookup = (
        d[["ticker", "merge_time", "prev_high", "prev_low", "avg_vol_10d"]]
        .dropna(subset=["prev_high", "prev_low"])
        .sort_values("merge_time")
    )

    # ── Step 4: Per-ticker 5-min operations ──────────────────────────────────
    m = stock_m5.sort_values(["ticker", "bar_time"]).copy()
    m["date"]           = m["bar_time"].dt.normalize()
    m["prev_bar_close"] = m.groupby("ticker")["close"].shift(1)

    # Cumulative intraday volume (resets each session)
    m["today_volume"] = m.groupby(["ticker", "date"])["volume"].cumsum()

    # ── Step 5: Merge daily and SPY lookups ──────────────────────────────────
    m = m.sort_values("bar_time")   # required by merge_asof

    m = pd.merge_asof(
        m, daily_lookup,
        left_on="bar_time", right_on="merge_time",
        by="ticker", direction="backward",
    )

    if not spy_lookup.empty:
        spy_lookup = spy_lookup.sort_values("spy_bar_time")
        m = pd.merge_asof(
            m, spy_lookup,
            left_on="bar_time", right_on="spy_bar_time",
            direction="backward",
        )
    else:
        m["spy_30m_change"] = None

    # ── Step 6: Derived columns ───────────────────────────────────────────────
    m["resistance"]      = m["prev_high"].round(4)
    m["prev_range_mid"]  = ((m["prev_high"] + m["prev_low"]) / 2).round(4)
    m["stop"]            = m["prev_low"].round(4)

    valid_avg = m["avg_vol_10d"].notna() & (m["avg_vol_10d"] > 0)
    m["avg_volume_10d"]  = m["avg_vol_10d"].round(2)
    m["relative_volume"] = (
        (m["today_volume"] / m["avg_vol_10d"])
        .where(valid_avg)
        .round(4)
    )

    # ── Step 7: Conditions ────────────────────────────────────────────────────
    m["above_range_mid"]  = (
        m["prev_range_mid"].notna() & (m["close"] > m["prev_range_mid"])
    ).fillna(False)

    m["shares_traded_ok"] = (m["today_volume"] >= 125_000).fillna(False)

    m["breakout"] = (
        m["prev_bar_close"].notna()
        & m["resistance"].notna()
        & (m["close"]         > m["resistance"])
        & (m["prev_bar_close"] <= m["resistance"])
    ).fillna(False)

    # If SPY data absent, don't block the signal
    m["market_flat"] = (
        m["spy_30m_change"].isna() | (m["spy_30m_change"] <= 0.5)
    ).fillna(True)

    m["entry"] = m["close"].round(4)

    # ── Step 8: Trigger ───────────────────────────────────────────────────────
    m["trigger_fired"] = (
        (m["close"] >= 10)
        & (m["close"] <= 150)
        & m["shares_traded_ok"]
        & m["above_range_mid"]
        & m["relative_volume"].notna()
        & (m["relative_volume"] > 1.0)
        & m["breakout"]
        & m["market_flat"]
    )

    # ── Step 9: Live mode — last 2 bars per ticker only ───────────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S4_COLS]

# ─────────────────────────────────────────────────────────────────────────────

# ─── Strategy 5 — Second-Day Continuation Short ───────────────────────────────
#
# Trigger: the prior session was genuinely weak (candle body down > 1%),
#          and the stock is continuing lower today (close < prev day close),
#          with a combined 2-day decline exceeding 2% — confirming multi-day
#          downward momentum rather than a single-day flush.
#
# Primary timeframe : 5-min bars
# Lookback sources  : bars_daily (prior open/close + 2-session-ago close)

_DDL_S5 = """\
CREATE TABLE IF NOT EXISTS `study_s5` (
  `ticker`                 VARCHAR(20)   NOT NULL,
  `bar_time`               DATETIME      NOT NULL   COMMENT '5-min bar start (ET)',
  `close`                  DECIMAL(18,4) DEFAULT NULL,
  `prev_day_open`          DECIMAL(18,4) DEFAULT NULL COMMENT 'Prior session open',
  `prev_day_close`         DECIMAL(18,4) DEFAULT NULL COMMENT 'Prior session close — continuation level',
  `close_2d_ago`           DECIMAL(18,4) DEFAULT NULL COMMENT 'Close 2 sessions ago',
  `prev_day_pct_change`    DECIMAL(8,4)  DEFAULT NULL COMMENT '(prev_close - prev_open) / prev_open * 100',
  `two_day_change`         DECIMAL(8,4)  DEFAULT NULL COMMENT '(close - close_2d_ago) / close_2d_ago * 100',
  `prev_day_weak`          TINYINT(1)    DEFAULT NULL COMMENT 'Prior day candle body down > 1%',
  `today_continuing_lower` TINYINT(1)    DEFAULT NULL COMMENT 'Current 5-min close < prior day close',
  `trigger_fired`          TINYINT(1)    DEFAULT NULL,
  `updated_at`             DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_UPSERT_S5 = """\
INSERT INTO `study_s5`
  (ticker, bar_time, close, prev_day_open, prev_day_close, close_2d_ago,
   prev_day_pct_change, two_day_change,
   prev_day_weak, today_continuing_lower, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close                  = VALUES(close),
  prev_day_open          = VALUES(prev_day_open),
  prev_day_close         = VALUES(prev_day_close),
  close_2d_ago           = VALUES(close_2d_ago),
  prev_day_pct_change    = VALUES(prev_day_pct_change),
  two_day_change         = VALUES(two_day_change),
  prev_day_weak          = VALUES(prev_day_weak),
  today_continuing_lower = VALUES(today_continuing_lower),
  trigger_fired          = VALUES(trigger_fired)
"""

_S5_COLS = [
    "ticker", "bar_time", "close",
    "prev_day_open", "prev_day_close", "close_2d_ago",
    "prev_day_pct_change", "two_day_change",
    "prev_day_weak", "today_continuing_lower",
    "trigger_fired",
]


@register("s5", "study_s5", _DDL_S5, _UPSERT_S5,
          result_cols=_S5_COLS, needs={"daily", "m5"},
          ticker_filter="last_day_close BETWEEN 5 AND 150")
def calc_s5(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Strategy 5 — Second-Day Continuation Short

    Vectorized.  Uses the same merge_asof daily-lookup pattern as S3/S4.
    Three conditions must all be true on the same 5-min bar:

      prev_day_weak          : prior day's candle body was down > 1%
                               (prev_close - prev_open) / prev_open * 100 < -1.0
      today_continuing_lower : current 5-min close < prior day close
      two_day_change < -2.0  : (close - close_2d_ago) / close_2d_ago * 100 < -2.0

    Entry timing = bar close when trigger fires.
    Stop suggestion = prior day close (breakdown level turned resistance).
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S5_COLS)

    # ── Step 1: Build daily lookup ────────────────────────────────────────────
    # shift(1) = prior session; shift(2) = two sessions ago.
    # merge_time nudged to 17:00 so merge_asof naturally selects the PRIOR
    # day's row for every intraday bar starting at 09:30+.
    d = daily.sort_values(["ticker", "bar_time"]).copy()
    d["prev_day_open"]  = d.groupby("ticker")["open"].shift(1)
    d["prev_day_close"] = d.groupby("ticker")["close"].shift(1)
    d["close_2d_ago"]   = d.groupby("ticker")["close"].shift(2)

    valid_prev_open = d["prev_day_open"].notna() & (d["prev_day_open"] != 0)
    d["prev_day_pct_change"] = (
        ((d["prev_day_close"] - d["prev_day_open"]) / d["prev_day_open"] * 100)
        .where(valid_prev_open)
        .round(4)
    )
    d["prev_day_weak"] = (
        d["prev_day_pct_change"].notna() & (d["prev_day_pct_change"] < -1.0)
    )

    d["merge_time"] = d["bar_time"] + pd.Timedelta(hours=17)
    daily_lookup = (
        d[["ticker", "merge_time",
           "prev_day_open", "prev_day_close", "close_2d_ago",
           "prev_day_pct_change", "prev_day_weak"]]
        .dropna(subset=["prev_day_close", "close_2d_ago"])
        .sort_values("merge_time")
    )

    # ── Step 2: Prepare 5-min bars ────────────────────────────────────────────
    m = m5.sort_values(["ticker", "bar_time"]).copy()
    m = m.sort_values("bar_time")   # global sort required by merge_asof

    # ── Step 3: Merge daily lookups ───────────────────────────────────────────
    m = pd.merge_asof(
        m, daily_lookup,
        left_on="bar_time", right_on="merge_time",
        by="ticker", direction="backward",
    )

    # ── Step 4: Intraday indicators ───────────────────────────────────────────
    m["today_continuing_lower"] = (
        m["prev_day_close"].notna() & (m["close"] < m["prev_day_close"])
    ).fillna(False)

    valid_2d = m["close_2d_ago"].notna() & (m["close_2d_ago"] != 0)
    m["two_day_change"] = (
        ((m["close"] - m["close_2d_ago"]) / m["close_2d_ago"] * 100)
        .where(valid_2d)
        .round(4)
    )

    # ── Step 5: Trigger ───────────────────────────────────────────────────────
    m["trigger_fired"] = (
        m["prev_day_weak"].fillna(False).astype(bool)
        & m["today_continuing_lower"]
        & m["two_day_change"].notna()
        & (m["two_day_change"] < -2.0)
    )

    # ── Step 6: Round price columns ───────────────────────────────────────────
    for col in ("close", "prev_day_open", "prev_day_close", "close_2d_ago"):
        m[col] = m[col].round(4)

    # ── Step 7: Live mode — last 2 bars per ticker only ───────────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S5_COLS]

# ─────────────────────────────────────────────────────────────────────────────

# ─── Strategy 6 — Resistance Cross with 60-Min High Confirmation ──────────────
#
# Trigger: stock > $20, crosses above the prior-day close (resistance) on a
#          5-min bar, AND that close is simultaneously at or above the session's
#          rolling 60-min high — signalling a trend-change, not just a bounce.
#
# Primary timeframe : 5-min bars
# Lookback sources  : bars_daily (resistance = prior day close), bars_60min (session high)

_DDL_S6 = """\
CREATE TABLE IF NOT EXISTS `study_s6` (
  `ticker`                 VARCHAR(20)   NOT NULL,
  `bar_time`               DATETIME      NOT NULL  COMMENT '5-min bar start (ET)',
  `close`                  DECIMAL(18,4) DEFAULT NULL,
  `resistance`             DECIMAL(18,4) DEFAULT NULL COMMENT 'Prior day close — breakout level',
  `prev_bar_close`         DECIMAL(18,4) DEFAULT NULL,
  `high_60min`             DECIMAL(18,4) DEFAULT NULL COMMENT 'Rolling session high across all 60-min bars so far',
  `cross_above_resistance` TINYINT(1)    DEFAULT NULL COMMENT 'prev_bar_close <= resistance AND close > resistance',
  `is_60min_high`          TINYINT(1)    DEFAULT NULL COMMENT 'close >= session 60-min high',
  `trigger_fired`          TINYINT(1)    DEFAULT NULL,
  `updated_at`             DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`ticker`, `bar_time`),
  INDEX `idx_trigger`  (`trigger_fired`, `bar_time`),
  INDEX `idx_bar_time` (`bar_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
"""

_UPSERT_S6 = """\
INSERT INTO `study_s6`
  (ticker, bar_time, close, resistance, prev_bar_close, high_60min,
   cross_above_resistance, is_60min_high, trigger_fired)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  close                  = VALUES(close),
  resistance             = VALUES(resistance),
  prev_bar_close         = VALUES(prev_bar_close),
  high_60min             = VALUES(high_60min),
  cross_above_resistance = VALUES(cross_above_resistance),
  is_60min_high          = VALUES(is_60min_high),
  trigger_fired          = VALUES(trigger_fired)
"""

_S6_COLS = [
    "ticker", "bar_time", "close",
    "resistance", "prev_bar_close", "high_60min",
    "cross_above_resistance", "is_60min_high",
    "trigger_fired",
]


@register("s6", "study_s6", _DDL_S6, _UPSERT_S6,
          result_cols=_S6_COLS, needs={"daily", "m5", "m60"},
          ticker_filter="last_day_close > 20")
def calc_s6(cache: BarCache, live: bool = False) -> pd.DataFrame:
    """
    Strategy 6 — Resistance Cross with 60-Min High Confirmation

    Vectorized.  Uses the same merge_asof daily-lookup pattern as S1, but
    targets stocks ABOVE $20 and omits the 5-day range / low-proximity check.

    Two conditions must both be true on the same 5-min bar:

      cross_above_resistance : prev_bar_close <= prior_day_close
                               AND close > prior_day_close
      is_60min_high          : close >= rolling session high across 60-min bars

    The 60-min session high is computed as the per-day maximum of the 60-min
    bar highs seen so far that session (same date join as S1).
    """
    daily = cache.get("daily", _EMPTY_BARS)
    m5    = cache.get("m5",    _EMPTY_BARS)
    m60   = cache.get("m60",   _EMPTY_BARS)

    if m5.empty or daily.empty:
        return pd.DataFrame(columns=_S6_COLS)

    # ── Step 1: Daily resistance lookup (prior day close = resistance) ─────────
    # Nudge daily bar_time to 17:00 so merge_asof backward-search picks the
    # PRIOR day's close for every intraday bar starting at 09:30+.
    d = daily.sort_values(["ticker", "bar_time"]).copy()
    d["resistance"] = d.groupby("ticker")["close"].shift(1)
    d["merge_time"] = d["bar_time"] + pd.Timedelta(hours=17)

    daily_lookup = (
        d[["ticker", "merge_time", "resistance"]]
        .dropna(subset=["resistance"])
        .sort_values("merge_time")
    )

    # ── Step 2: 60-min session highs ──────────────────────────────────────────
    if not m60.empty:
        m60c = m60.copy()
        m60c["date"] = m60c["bar_time"].dt.normalize()
        session_high = (
            m60c.groupby(["ticker", "date"])["high"]
            .max()
            .reset_index()
            .rename(columns={"high": "high_60min"})
        )
    else:
        session_high = pd.DataFrame(columns=["ticker", "date", "high_60min"])

    # ── Step 3: Prepare 5-min bars ────────────────────────────────────────────
    # Sort per-ticker first so shift() gives the correct previous bar, then
    # re-sort globally so merge_asof's monotone left-key requirement is met.
    m = m5.sort_values(["ticker", "bar_time"]).copy()
    m["date"]           = m["bar_time"].dt.normalize()
    m["prev_bar_close"] = m.groupby("ticker")["close"].shift(1)
    m = m.sort_values("bar_time")

    # ── Step 4: Merge daily resistance and 60-min session high ────────────────
    m = pd.merge_asof(
        m, daily_lookup,
        left_on="bar_time", right_on="merge_time",
        by="ticker", direction="backward",
    )
    m = m.merge(session_high, on=["ticker", "date"], how="left")

    # ── Step 5: Indicators ────────────────────────────────────────────────────
    m["cross_above_resistance"] = (
        m["prev_bar_close"].notna()
        & m["resistance"].notna()
        & (m["close"]         > m["resistance"])
        & (m["prev_bar_close"] <= m["resistance"])
    ).fillna(False)

    m["is_60min_high"] = (
        m["high_60min"].notna() & (m["close"] >= m["high_60min"])
    ).fillna(False)

    # ── Step 6: Trigger ───────────────────────────────────────────────────────
    m["trigger_fired"] = (
        (m["close"] > 20)
        & m["cross_above_resistance"]
        & m["is_60min_high"]
    )

    # ── Step 7: Round price columns ───────────────────────────────────────────
    for col in ("close", "resistance", "prev_bar_close", "high_60min"):
        m[col] = m[col].round(4)

    # ── Step 8: Live mode — last 2 bars per ticker only ───────────────────────
    if live:
        m = m.groupby("ticker", sort=False).tail(2)

    return m[_S6_COLS]

# ─────────────────────────────────────────────────────────────────────────────

# ─── Market-hours helpers ─────────────────────────────────────────────────────

def _is_market_open() -> bool:
    now = datetime.now(tz=ET)
    if now.weekday() >= 5:
        return False
    open_t  = now.replace(hour=MARKET_OPEN_H,  minute=MARKET_OPEN_M,  second=0, microsecond=0)
    close_t = now.replace(hour=MARKET_CLOSE_H, minute=MARKET_CLOSE_M, second=0, microsecond=0)
    return open_t <= now < close_t


def _seconds_until_open() -> float:
    now = datetime.now(tz=ET)
    candidate = now.replace(hour=MARKET_OPEN_H, minute=MARKET_OPEN_M,
                             second=0, microsecond=0)
    if candidate <= now:
        candidate += timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)
    return max(0.0, (candidate - now).total_seconds())

# ─── Orchestration ────────────────────────────────────────────────────────────

def _strategy_cache(cache: BarCache, s: StrategyDef) -> BarCache:
    """
    Return a BarCache scoped to the strategy's required timeframes and,
    if the strategy declares a ticker_filter, narrowed to qualifying tickers.
    """
    scoped = {k: v for k, v in cache.items() if k in s.needs}
    if s.ticker_filter:
        qualified = _load_qualified_tickers(s.ticker_filter)
        scoped    = _filter_cache(scoped, qualified)
        log.info("  %s ticker filter '%s' → %d qualifying symbols",
                 s.id, s.ticker_filter, len(qualified))
    return scoped


def run_backfill():
    """
    Historical backfill for all registered strategies.
    Bars are loaded once and shared; each strategy receives a cache
    already filtered to its qualifying ticker universe.
    """
    cache = _build_bar_cache(backfill=True)

    for s in _STRATEGIES:
        if _shutdown.is_set():
            break
        log.info("Calculating %s (%s)...", s.id, s.table)
        t0 = time.monotonic()

        df = s.calc(_strategy_cache(cache, s), live=False)
        log.info("  %s: %d rows calculated in %.1fs", s.id, len(df), time.monotonic() - t0)

        t1 = time.monotonic()
        rows = _df_to_rows(df, s.result_cols)
        _upsert(s.upsert_sql, rows)
        log.info("  %s: upserted in %.1fs", s.id, time.monotonic() - t1)

    log.info("Backfill complete.")


def run_live_cycle():
    """
    One live pass: load bars once, then run every strategy in parallel.

    Each strategy gets its own thread from the pool; they all share the
    read-only BarCache and write to separate study_* tables, so there
    is no contention.  Total wall-clock time ≈ slowest single strategy
    instead of sum-of-all-strategies.
    """
    t0    = time.monotonic()
    cache = _build_bar_cache(backfill=False)
    t_loaded = time.monotonic() - t0
    log.info("  Bars loaded in %.1fs — launching %d strategies in parallel...",
             t_loaded, len(_STRATEGIES))

    def _run_one(s: StrategyDef) -> Tuple[str, int, float, float]:
        """Returns (sid, row_count, calc_secs, upsert_secs)."""
        scoped      = _strategy_cache(cache, s)
        t_s         = time.monotonic()
        df          = s.calc(scoped, live=True)
        t_calc      = time.monotonic()
        rows        = _df_to_rows(df, s.result_cols)
        _upsert(s.upsert_sql, rows)
        t_upsert    = time.monotonic()
        return s.id, len(rows), t_calc - t_s, t_upsert - t_calc

    active = [s for s in _STRATEGIES if not _shutdown.is_set()]
    with ThreadPoolExecutor(max_workers=min(LIVE_WORKERS, len(active))) as pool:
        futures = {pool.submit(_run_one, s): s for s in active}
        for fut in as_completed(futures):
            s = futures[fut]
            try:
                sid, n_rows, t_calc, t_upsert = fut.result()
                log.info("  %-4s  %5d rows  calc=%4.1fs  upsert=%4.1fs",
                         sid, n_rows, t_calc, t_upsert)
            except Exception as exc:
                log.error("  %s failed: %s", s.id, exc)

    log.info("Live cycle complete — %d strategies in %.1fs total.",
             len(_STRATEGIES), time.monotonic() - t0)


def run_live_loop(force: bool = False):
    """Loop during market hours; sleeps to next open otherwise.

    force=True skips the market-hours check so the loop runs at any time
    (useful for testing outside market hours).
    """
    log.info("Entering live loop (%ds interval, %d strategies%s).",
             LIVE_INTERVAL, len(_STRATEGIES),
             ", FORCE MODE — ignoring market hours" if force else "")

    while not _shutdown.is_set():
        if not force and not _is_market_open():
            wait    = _seconds_until_open()
            open_at = datetime.now(tz=ET) + timedelta(seconds=wait)
            log.info("Market closed — sleeping %.0fs until %s ET.",
                     wait, open_at.strftime("%Y-%m-%d %H:%M"))
            _shutdown.wait(timeout=wait)
            if _shutdown.is_set():
                break
            log.info("Market open — resuming live loop.")

        t0 = time.monotonic()
        run_live_cycle()
        sleep_for = max(0.0, LIVE_INTERVAL - (time.monotonic() - t0))
        if sleep_for and not _shutdown.is_set():
            log.info("Waiting %.0fs until next cycle...", sleep_for)
            _shutdown.wait(timeout=sleep_for)

    log.info("Live loop stopped cleanly.")

# ─── Entry point ──────────────────────────────────────────────────────────────

def main():
    global LIVE_INTERVAL, LIVE_WORKERS

    parser = argparse.ArgumentParser(
        description="Studies Calculator — pre-compute TA for trading strategies",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--backfill",  action="store_true",
                        help="Calculate TA for all historical bars")
    parser.add_argument("--live",      action="store_true",
                        help="Run live refresh loop")
    parser.add_argument("--once",      action="store_true",
                        help="Run one live cycle and exit (for cron)")
    parser.add_argument("--test",      action="store_true",
                        help="Test DB connectivity and print ticker count")
    parser.add_argument("--force",     action="store_true",
                        help="Run live loop regardless of market hours (for testing)")
    parser.add_argument("--interval",  type=int, default=LIVE_INTERVAL,
                        metavar="SEC",
                        help=f"Seconds between live cycles (default: {LIVE_INTERVAL})")
    parser.add_argument("--workers",   type=int, default=LIVE_WORKERS,
                        metavar="N",
                        help=f"Parallel strategy threads in live cycle (default: {LIVE_WORKERS})")
    args = parser.parse_args()

    LIVE_INTERVAL = args.interval
    LIVE_WORKERS  = args.workers

    signal.signal(signal.SIGINT,  _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    log.info("Registered strategies: %s", [s.id for s in _STRATEGIES])

    _init_pool()
    create_tables()
    log.info("Purging study rows outside retention window...")
    purge_old_studies()

    if args.test:
        tickers = get_tickers()
        log.info("DB OK — %d tickers in universe.", len(tickers))
        return

    if args.backfill:
        run_backfill()

    if args.once:
        run_live_cycle()
        return

    if args.live:
        run_live_loop(force=args.force)
        return

    if not any([args.backfill, args.once, args.live]):
        parser.print_help()


if __name__ == "__main__":
    main()
