"""
Background job: for users who opted in, close open positions with a market order
when the cached snapshot price is past the take-profit level (same rule as the
My Trades UI flash: long ≥ target, short ≤ target).

Uses snapshot_cache (Polygon-backed) and the same flatten path as manual close.
"""
import logging
import threading
import time

logger = logging.getLogger("tp_auto_close")

INTERVAL_SEC = 60

PREF_BROKER = "preferences"
PREF_KEY_AUTO_CLOSE = "auto_market_close_beyond_tp"


def _truthy(val) -> bool:
    if val is None:
        return False
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "on")


def _user_ids_with_auto_close_enabled(app):
    from models import BrokerCredential

    with app.app_context():
        rows = BrokerCredential.query.filter_by(broker=PREF_BROKER, key=PREF_KEY_AUTO_CLOSE).all()
        return [r.user_id for r in rows if _truthy(r.value)]


def _snapshot_prices(tickers: set) -> dict:
    """ticker -> float price from snapshot_cache."""
    from extensions import db

    if not tickers:
        return {}
    rows = db.session.execute(
        db.text("SELECT ticker, price FROM snapshot_cache WHERE ticker IN :t"),
        {"t": tuple(sorted(tickers))},
    ).fetchall()
    return {r.ticker: float(r.price) for r in rows if r.price is not None}


def _beyond_take_profit(order, live_px: float) -> bool:
    t = float(order.target_price)
    if order.direction == "long":
        return live_px >= t
    return live_px <= t


def _run_cycle(app):
    from extensions import db
    from models import BrokerCredential, Order
    from alpaca_close import perform_market_close_for_order

    with app.app_context():
        user_ids = [
            r.user_id
            for r in BrokerCredential.query.filter_by(broker=PREF_BROKER, key=PREF_KEY_AUTO_CLOSE).all()
            if _truthy(r.value)
        ]
        if not user_ids:
            return

        for uid in user_ids:
            open_rows = (
                Order.query.filter_by(user_id=uid, is_open=True)
                .filter(Order.alpaca_order_id.isnot(None))
                .all()
            )
            if not open_rows:
                continue

            tickers = {o.ticker.strip().upper() for o in open_rows if o.ticker}
            prices = _snapshot_prices(tickers)

            candidate_ids = []
            for o in open_rows:
                sym = o.ticker.strip().upper() if o.ticker else ""
                if not sym or o.target_price is None:
                    continue
                px = prices.get(sym)
                if px is None:
                    continue
                if _beyond_take_profit(o, px):
                    candidate_ids.append(o.id)

            for oid in candidate_ids:
                order = Order.query.filter_by(id=oid, user_id=uid, is_open=True).first()
                if not order or not order.alpaca_order_id:
                    continue
                sym = order.ticker.strip().upper()
                px = prices.get(sym)
                if px is None or order.target_price is None:
                    continue
                if not _beyond_take_profit(order, px):
                    continue
                try:
                    errs = perform_market_close_for_order(uid, order)
                    if errs:
                        logger.warning(
                            "auto TP close partial uid=%s order=%s ticker=%s errors=%s",
                            uid, oid, sym, errs,
                        )
                    else:
                        logger.info(
                            "auto TP close ok uid=%s order=%s ticker=%s px=%s target=%s",
                            uid, oid, sym, px, float(order.target_price),
                        )
                except Exception as exc:
                    logger.exception("auto TP close failed uid=%s order=%s: %s", uid, oid, exc)


def start_tp_auto_close(app):
    def run():
        time.sleep(12)
        while True:
            try:
                _run_cycle(app)
            except Exception as e:
                logger.error("tp_auto_close cycle error: %s", e)
            time.sleep(INTERVAL_SEC)

    t = threading.Thread(target=run, daemon=True, name="tp-auto-close")
    t.start()
    logger.info("TP auto-close worker started (interval=%ds)", INTERVAL_SEC)
