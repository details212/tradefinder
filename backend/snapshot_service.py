"""
Background service: keeps snapshot_cache current for the full liquid-stock universe.

Every REFRESH_INTERVAL seconds the thread:
  1. Queries tradefinder.tickers for the tradeable universe (same filter used in search).
  2. Upserts those tickers into snapshot_cache (so /prices always has something to return).
  3. Fetches live prices from Polygon in BATCH_SIZE chunks and updates the cache.

The client never needs to "register" tickers – prices are ready the moment any
ticker from the universe is requested.
"""
import threading
import time
import logging

REFRESH_INTERVAL = 60   # seconds between full refresh cycles
BATCH_SIZE       = 50   # tickers per Polygon /v3/snapshot request
BATCH_PAUSE      = 1.2  # seconds between batches (rate-limit courtesy)

UNIVERSE_QUERY = """
    SELECT ticker FROM tradefinder.tickers
     WHERE (last_day_volume > 500000 AND type = 'CS' AND last_day_close > 5)
        OR ticker IN ('SPY', 'QQQ')
     ORDER BY ticker
"""

logger = logging.getLogger("snapshot_service")


def _refresh_once(app):
    """Sync the universe and refresh all cached prices."""
    with app.app_context():
        from extensions import db
        from routes.stock_routes import polygon_get

        # ── 1. Load current universe from tickers table ──────────────────────
        rows = db.session.execute(db.text(UNIVERSE_QUERY)).fetchall()
        tickers = [r[0] for r in rows]

        if not tickers:
            logger.warning("Universe query returned 0 tickers – skipping refresh")
            return

        # ── 2. Seed universe into snapshot_cache (skip rows that already exist) ─
        db.session.execute(db.text(
            "INSERT IGNORE INTO snapshot_cache (ticker) "
            "SELECT ticker FROM tradefinder.tickers "
            "WHERE (last_day_volume > 500000 AND type = 'CS' AND last_day_close > 5) "
            "   OR ticker IN ('SPY', 'QQQ')"
        ))
        db.session.commit()

        # ── 3. Fetch Polygon snapshots in batches ────────────────────────────
        updated = 0
        for i in range(0, len(tickers), BATCH_SIZE):
            batch = tickers[i : i + BATCH_SIZE]
            try:
                data = polygon_get("/v3/snapshot", {
                    "ticker.any_of": ",".join(batch),
                    "limit": BATCH_SIZE,
                })
                if data and "results" in data:
                    for item in data["results"]:
                        ticker     = item.get("ticker")
                        last_trade = item.get("last_trade") or {}
                        session    = item.get("session")    or {}
                        price      = last_trade.get("price") or session.get("close")
                        change_pct = session.get("change_percent")
                        if ticker and price is not None:
                            db.session.execute(db.text(
                                "UPDATE snapshot_cache "
                                "SET price=:p, change_pct=:c, updated_at=NOW() "
                                "WHERE ticker=:t"
                            ), {"p": float(price),
                                "c": float(change_pct) if change_pct is not None else None,
                                "t": ticker})
                            updated += 1
                    db.session.commit()
            except Exception as e:
                logger.warning("Snapshot batch %d error: %s", i // BATCH_SIZE, e)

            if i + BATCH_SIZE < len(tickers):
                time.sleep(BATCH_PAUSE)

        logger.info(
            "Snapshot cache refreshed: %d/%d tickers updated", updated, len(tickers)
        )


def start_snapshot_updater(app):
    """Start the background refresh daemon thread."""

    def run():
        time.sleep(5)   # brief startup delay so Flask is fully initialised
        while True:
            try:
                _refresh_once(app)
            except Exception as e:
                logger.error("Snapshot updater error: %s", e)
            time.sleep(REFRESH_INTERVAL)

    t = threading.Thread(target=run, daemon=True, name="snapshot-updater")
    t.start()
    logger.info(
        "Snapshot updater started (interval=%ds, batch=%d)", REFRESH_INTERVAL, BATCH_SIZE
    )
