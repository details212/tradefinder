"""
Background service: keeps ma_cache current for the full liquid-stock universe.

Architecture
------------
Tickers are processed in batches of BATCH_SIZE.  Within each batch every ticker
is fetched concurrently (ThreadPoolExecutor), and within each ticker all five
Polygon indicator calls are also made concurrently.  After each batch the thread
sleeps BATCH_PAUSE seconds to stay within Polygon rate limits.

Effective throughput: ~BATCH_SIZE tickers every (network latency + BATCH_PAUSE).
With BATCH_SIZE=5 and BATCH_PAUSE=2 s that is roughly 5 tickers / 2–3 s ≈
100–150 tickers/minute vs the original ~50/minute.

Tickers whose cached row is still fresh (< MA_TTL_HOURS) are skipped, so the
service becomes nearly idle once the cache is warm.
"""
import concurrent.futures
import threading
import time
import logging
from datetime import datetime, timezone, timedelta

import requests

BATCH_SIZE   = 5            # tickers fetched concurrently per batch
BATCH_PAUSE  = 2.0          # seconds between batches (rate-limit courtesy)
CYCLE_PAUSE  = 60 * 60      # 1 h between complete passes
MA_TTL_HOURS = 24
POLYGON_BASE = "https://api.polygon.io/v1/indicators"

UNIVERSE_QUERY = """
    SELECT ticker FROM tradefinder.tickers
     WHERE (last_day_volume > 500000 AND type = 'CS' AND last_day_close > 5)
        OR ticker IN ('SPY', 'QQQ')
     ORDER BY ticker
"""

SPECS = [
    ("ema", 10,  "ema10"),
    ("ema", 20,  "ema20"),
    ("sma", 50,  "sma50"),
    ("sma", 150, "sma150"),
    ("sma", 200, "sma200"),
]

UPSERT_SQL = """
    INSERT INTO ma_cache (ticker, ema10, ema20, sma50, sma150, sma200, fetched_at)
    VALUES (:ticker, :ema10, :ema20, :sma50, :sma150, :sma200, :fetched_at)
    ON DUPLICATE KEY UPDATE
        ema10=VALUES(ema10), ema20=VALUES(ema20), sma50=VALUES(sma50),
        sma150=VALUES(sma150), sma200=VALUES(sma200), fetched_at=VALUES(fetched_at)
"""

# Tickers requested by the Trade Ideas UI (or API) for out-of-band refresh — processed
# slowly so Polygon stays shared fairly with other jobs.
_priority_queue = set()
_queue_lock = threading.Lock()

logger = logging.getLogger("ma_cache_service")


def _fetch_one_indicator(ticker: str, kind: str, window: int, col: str, api_key: str):
    """Fetch a single indicator value for one ticker. Returns (col, value|None)."""
    try:
        resp = requests.get(
            f"{POLYGON_BASE}/{kind}/{ticker}",
            params={
                "timespan": "day",
                "window":   window,
                "adjusted": "true",
                "order":    "desc",
                "limit":    1,
                "apiKey":   api_key,
            },
            timeout=30,
        )
        resp.raise_for_status()
        values = resp.json().get("results", {}).get("values", [])
        return col, (values[0]["value"] if values else None)
    except Exception as exc:
        logger.warning("MA fetch failed %s %s/%s: %s", ticker, kind, window, exc)
        return col, None


def _fetch_ticker(ticker: str, api_key: str) -> dict:
    """
    Fetch all five MA values for one ticker, firing all five Polygon requests
    concurrently so total time ≈ one round-trip instead of five.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
        futures = [
            pool.submit(_fetch_one_indicator, ticker, kind, window, col, api_key)
            for kind, window, col in SPECS
        ]
        return dict(f.result() for f in concurrent.futures.as_completed(futures))


def _fetch_batch(tickers: list, api_key: str) -> dict:
    """
    Fetch all five MA values for every ticker in the batch concurrently.
    Returns { ticker: { col: value, … }, … }
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tickers)) as pool:
        futures = {pool.submit(_fetch_ticker, t, api_key): t for t in tickers}
        return {
            futures[f]: f.result()
            for f in concurrent.futures.as_completed(futures)
        }


def _process_ma_batch_core(db, tickers: list, api_key: str) -> tuple[int, int]:
    """
    Polygon fetch + DB upsert for one batch. Caller must hold Flask app context.
    Returns (updated_count, error_count).
    """
    if not tickers:
        return 0, 0
    results = _fetch_batch(tickers, api_key)
    now = datetime.now(timezone.utc)
    updated = errors = 0

    for ticker, data in results.items():
        if all(v is None for v in data.values()):
            logger.debug("All MA values None for %s – skipping upsert", ticker)
            errors += 1
            continue
        try:
            db.session.execute(db.text(UPSERT_SQL), {
                "ticker":     ticker,
                "ema10":      data.get("ema10"),
                "ema20":      data.get("ema20"),
                "sma50":      data.get("sma50"),
                "sma150":     data.get("sma150"),
                "sma200":     data.get("sma200"),
                "fetched_at": now,
            })
            updated += 1
        except Exception as exc:
            db.session.rollback()
            logger.warning("Upsert failed for %s: %s", ticker, exc)
            errors += 1

    try:
        db.session.commit()
    except Exception as exc:
        db.session.rollback()
        logger.warning("MA batch commit failed: %s", exc)
        return 0, len(tickers)

    return updated, errors


def process_ma_batch(app, tickers: list[str]) -> tuple[int, int]:
    """
    Public entry: fetch + upsert MA for tickers (Polygon). Safe to call from a worker thread.
    """
    if not tickers:
        return 0, 0
    with app.app_context():
        from extensions import db
        from routes.stock_routes import get_polygon_api_key

        api_key = get_polygon_api_key()
        if not api_key:
            logger.error("Polygon API key not configured – skipping MA batch")
            return 0, len(tickers)
        return _process_ma_batch_core(db, tickers, api_key)


def enqueue_ma_refresh(tickers: list[str]) -> None:
    """Queue tickers for background refresh (deduped, thread-safe)."""
    with _queue_lock:
        for t in tickers:
            if t:
                _priority_queue.add(t.upper().strip())


def _refresh_once(app):
    """One full pass over the universe, processing BATCH_SIZE tickers at a time."""
    with app.app_context():
        from extensions import db
        from routes.stock_routes import get_polygon_api_key
        from models import MaCache

        api_key = get_polygon_api_key()
        if not api_key:
            logger.error("Polygon API key not configured – skipping MA cache refresh")
            return

        # Load universe
        rows    = db.session.execute(db.text(UNIVERSE_QUERY)).fetchall()
        tickers = [r[0] for r in rows]
        if not tickers:
            logger.warning("Universe query returned 0 tickers – skipping")
            return

        # Load all existing cache rows in one query
        cached = {
            r.ticker: r
            for r in MaCache.query.filter(MaCache.ticker.in_(tickers)).all()
        }

        stale_cutoff = datetime.now(timezone.utc) - timedelta(hours=MA_TTL_HOURS)

        # Filter to only stale/missing tickers
        stale = [
            t for t in tickers
            if not (
                (row := cached.get(t))
                and row.fetched_at
                and row.fetched_at.replace(tzinfo=timezone.utc) > stale_cutoff
            )
        ]

        logger.info(
            "MA cache pass: %d stale / %d skipped (fresh) — universe %d",
            len(stale), len(tickers) - len(stale), len(tickers),
        )

        updated = errors = 0

        for i in range(0, len(stale), BATCH_SIZE):
            batch = stale[i : i + BATCH_SIZE]
            u, e = _process_ma_batch_core(db, batch, api_key)
            updated += u
            errors += e

            if i + BATCH_SIZE < len(stale):
                time.sleep(BATCH_PAUSE)

        logger.info(
            "MA cache pass complete: %d updated, %d errors",
            updated, errors,
        )


def start_ma_cache_updater(app):
    """Start the background MA cache daemon thread."""

    def run():
        time.sleep(15)  # let Flask finish initialising
        while True:
            try:
                _refresh_once(app)
            except Exception as exc:
                logger.error("MA cache updater error: %s", exc)
            time.sleep(CYCLE_PAUSE)

    t = threading.Thread(target=run, daemon=True, name="ma-cache-updater")
    t.start()
    logger.info(
        "MA cache updater started (batch=%d, pause=%.1fs, TTL=%dh)",
        BATCH_SIZE, BATCH_PAUSE, MA_TTL_HOURS,
    )


def start_priority_ma_refresher(app):
    """
    Drain the on-demand priority queue in small batches so Trade Ideas can return
    stale DB rows immediately without a huge Polygon burst in the HTTP request.
    """

    def run():
        time.sleep(20)
        while True:
            try:
                time.sleep(5)
                batch = []
                with _queue_lock:
                    while len(batch) < BATCH_SIZE and _priority_queue:
                        batch.append(_priority_queue.pop())
                if not batch:
                    continue
                u, e = process_ma_batch(app, batch)
                if u or e:
                    logger.info("MA priority queue: batch %d tickers → %d ok, %d err", len(batch), u, e)
            except Exception as exc:
                logger.warning("MA priority refresher: %s", exc)

    t = threading.Thread(target=run, daemon=True, name="ma-priority-refresher")
    t.start()
    logger.info("MA priority queue refresher started (batch=%d, interval=5s)", BATCH_SIZE)
