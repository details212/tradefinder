import logging
from flask import Blueprint, jsonify, request, current_app

_log = logging.getLogger(__name__)
from auth import token_required
from extensions import db
from models import MaCache

tradeideas_bp = Blueprint("tradeideas", __name__, url_prefix="/api/tradeideas")

# ── Strategy registry ─────────────────────────────────────────────────────────
# Add up to 50 strategies here. Each entry: id, name, direction, description, query.
STRATEGIES = {
    1: {
        "id": 1,
        "name": "Strategy 1",
        "direction": "Long",
        "description": "Near 5-day low · Cross above resistance · New 60-min high · Close < $20",
        "query": (
            "SELECT * FROM tradefinder.study_s1 "
            "WHERE close < 20 "
            "  AND is_near_5d_low = 1 "
            "  AND cross_above_resistance = 1 "
            "  AND new_60min_high = 1 "
            "  AND bar_time >= NOW() - INTERVAL 7 DAY "
            "ORDER BY bar_time DESC"
        ),
    },
    2: {
        "id": 2,
        "name": "Strategy 2",
        "direction": "Long",
        "description": "Green 5-day · Price above EMAs · Consecutive green 5m bars · Pullback from high · Close < $20",
        "query": (
            "SELECT agg.* "
            "FROM ( "
            "  SELECT "
            "    ticker, "
            "    DATE(bar_time)  AS signal_date, "
            "    MIN(bar_time)   AS first_signal_time, "
            "    COUNT(*)        AS signal_count, "
            "    MIN(entry)      AS first_entry, "
            "    MIN(stop)       AS tightest_stop "
            "  FROM tradefinder.study_s2 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= CURDATE() - INTERVAL 7 DAY "
            "  GROUP BY ticker, DATE(bar_time) "
            ") agg "
            "INNER JOIN ( "
            "  SELECT ticker, MIN(signal_date) AS earliest_date "
            "  FROM ( "
            "    SELECT ticker, DATE(bar_time) AS signal_date "
            "    FROM tradefinder.study_s2 "
            "    WHERE trigger_fired = 1 "
            "      AND bar_time >= CURDATE() - INTERVAL 7 DAY "
            "    GROUP BY ticker, DATE(bar_time) "
            "  ) dates "
            "  GROUP BY ticker "
            ") first ON agg.ticker = first.ticker AND agg.signal_date = first.earliest_date "
            "ORDER BY agg.signal_date DESC, agg.ticker"
        ),
    },
    3: {
        "id": 3,
        "name": "Strategy 3",
        "direction": "Short",
        "description": "Down from prev close · Breakdown · SPY weak · Market OK to short",
        "query": (
            "SELECT "
            "  s.ticker, "
            "  s.bar_time, "
            "  s.close, "
            "  s.prev_day_close, "
            "  ROUND((s.close - s.prev_day_close) / s.prev_day_close * 100, 2) AS pct_from_prev, "
            "  s.down_from_prev, "
            "  s.breakdown, "
            "  s.spy_5m_change, "
            "  s.market_ok_to_short, "
            "  s.trigger_fired "
            "FROM tradefinder.study_s3 s "
            "INNER JOIN ( "
            "  SELECT ticker, MAX(bar_time) AS latest "
            "  FROM tradefinder.study_s3 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= CURDATE() - INTERVAL 7 DAY "
            "  GROUP BY ticker, DATE(bar_time) "
            ") latest ON s.ticker = latest.ticker AND s.bar_time = latest.latest "
            "ORDER BY s.bar_time DESC"
        ),
    },
    4: {
        "id": 4,
        "name": "Strategy 4",
        "direction": "Long",
        "description": "Resistance breakout · Above-avg relative volume · $10–$150 · Market flat",
        "query": (
            # First trigger per ticker per day — earliest breakout bar only
            "SELECT s.* "
            "FROM tradefinder.study_s4 s "
            "INNER JOIN ( "
            "  SELECT ticker, MIN(bar_time) AS first_bar "
            "  FROM tradefinder.study_s4 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= CURDATE() - INTERVAL 7 DAY "
            "  GROUP BY ticker, DATE(bar_time) "
            ") first_day ON s.ticker = first_day.ticker "
            "         AND s.bar_time = first_day.first_bar "
            "INNER JOIN ( "
            "  SELECT ticker, MIN(DATE(bar_time)) AS earliest_date "
            "  FROM tradefinder.study_s4 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= CURDATE() - INTERVAL 7 DAY "
            "  GROUP BY ticker "
            ") first_tick ON s.ticker = first_tick.ticker "
            "           AND DATE(s.bar_time) = first_tick.earliest_date "
            "ORDER BY s.bar_time DESC"
        ),
    },
    5: {
        "id": 5,
        "name": "Strategy 5",
        "direction": "Short",
        "description": "Prior day < −2% · 2-day cumulative < −3% · First trigger bar per day",
        "query": (
            "SELECT "
            "  s.ticker, "
            "  s.bar_time, "
            "  s.close, "
            "  s.prev_day_close                             AS stop_level, "
            "  ROUND(s.prev_day_close - s.close, 4)        AS pts_below_support, "
            "  s.prev_day_pct_change                       AS prior_day_candle_pct, "
            "  s.two_day_change "
            "FROM tradefinder.study_s5 s "
            "INNER JOIN ( "
            "  SELECT ticker, DATE(bar_time) AS sig_date, MIN(bar_time) AS first_bar "
            "  FROM tradefinder.study_s5 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= NOW() - INTERVAL 7 DAY "
            "  GROUP BY ticker, DATE(bar_time) "
            ") first ON s.ticker = first.ticker "
            "       AND s.bar_time = first.first_bar "
            "WHERE s.prev_day_pct_change < -2.0 "
            "  AND s.two_day_change       < -3.0 "
            "ORDER BY s.two_day_change ASC"
        ),
    },
    6: {
        "id": 6,
        "name": "Strategy 6",
        "direction": "Long",
        "description": "Cross above resistance · 60-min high · First trigger bar per day",
        "query": (
            "SELECT "
            "  s.ticker, "
            "  s.bar_time                                           AS entry_time, "
            "  s.close                                             AS entry_price, "
            "  s.resistance, "
            "  ROUND(s.close - s.resistance, 4)                   AS pts_above_resistance, "
            "  ROUND((s.close - s.resistance) / s.resistance * 100, 2) AS pct_above_resistance, "
            "  s.high_60min "
            "FROM tradefinder.study_s6 s "
            "INNER JOIN ( "
            "  SELECT ticker, DATE(bar_time) AS sig_date, MIN(bar_time) AS first_bar "
            "  FROM tradefinder.study_s6 "
            "  WHERE trigger_fired = 1 "
            "    AND bar_time >= NOW() - INTERVAL 7 DAY "
            "  GROUP BY ticker, DATE(bar_time) "
            ") first ON s.ticker = first.ticker "
            "       AND s.bar_time = first.first_bar "
            "ORDER BY s.bar_time DESC"
        ),
    },
    # Strategies 7–50 can be added here following the same structure.
}


def _serialize_row(mapping):
    """Convert a SQLAlchemy row mapping to a JSON-safe dict."""
    result = {}
    for col, val in mapping.items():
        if val is None:
            result[col] = None
        elif hasattr(val, "isoformat"):
            result[col] = val.isoformat()
        elif hasattr(val, "__float__"):
            result[col] = float(val)
        else:
            result[col] = val
    return result


@tradeideas_bp.route("/snapshots", methods=["GET"])
@token_required
def get_snapshots(current_user):
    """Return current prices for a comma-separated list of tickers via Polygon v3/snapshot."""
    tickers = request.args.get("tickers", "").strip()
    if not tickers:
        return jsonify({"prices": {}}), 200

    from routes.stock_routes import polygon_get
    data = polygon_get("/v3/snapshot", {
        "ticker.any_of": tickers,
        "limit": 250,
    })

    prices = {}
    if data and "results" in data:
        for item in data["results"]:
            ticker = item.get("ticker")
            last_trade = (item.get("last_trade") or {})
            session    = (item.get("session")    or {})
            price      = last_trade.get("price") or session.get("close")
            change_pct = session.get("change_percent")
            if ticker and price is not None:
                prices[ticker] = {
                    "price":      float(price),
                    "change_pct": float(change_pct) if change_pct is not None else None,
                }

    return jsonify({"prices": prices}), 200


@tradeideas_bp.route("/", methods=["GET"])
@token_required
def list_strategies(current_user):
    return jsonify({
        "strategies": [
            {k: v for k, v in s.items() if k != "query"}
            for s in STRATEGIES.values()
        ]
    }), 200


@tradeideas_bp.route("/<int:strategy_id>", methods=["GET"])
@token_required
def run_strategy(current_user, strategy_id):
    strategy = STRATEGIES.get(strategy_id)
    if not strategy:
        return jsonify({"error": "Strategy not found"}), 404

    try:
        rows = db.session.execute(db.text(strategy["query"])).fetchall()
        if not rows:
            return jsonify({
                "strategy": {k: v for k, v in strategy.items() if k != "query"},
                "columns": [],
                "results": [],
                "count": 0,
            }), 200

        columns = list(rows[0]._mapping.keys())
        results = [_serialize_row(row._mapping) for row in rows]

        return jsonify({
            "strategy": {k: v for k, v in strategy.items() if k != "query"},
            "columns": columns,
            "results": results,
            "count": len(results),
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── MA cache ──────────────────────────────────────────────────────────────────

MA_CACHE_MAX_TICKERS = 400
MA_SYNC_REFRESH_MAX = 25  # Polygon work in request only for small batches (legacy clients)


@tradeideas_bp.route("/ma-cache", methods=["POST"])
@token_required
def bulk_ma_cache(current_user):
    """
    Body: {
      "tickers": ["AAPL", ...],
      "stale_ok": true,       # optional — return DB rows immediately (no Polygon in this request)
      "queue_refresh": true   # optional — enqueue stale/missing for background refresh (default true)
    }

    If stale_ok is true (recommended for Trade Ideas lists), returns cached rows only and never
    blocks on Polygon. Stale/missing tickers can be queued for the priority worker.

    If stale_ok is false, at most MA_SYNC_REFRESH_MAX tickers are refreshed synchronously.
    """
    body = request.get_json(silent=True) or {}
    tickers = [t.upper().strip() for t in (body.get("tickers") or []) if t]
    stale_ok = bool(body.get("stale_ok", False))
    queue_refresh = bool(body.get("queue_refresh", True))

    if not tickers:
        return jsonify({"ma": {}}), 200

    if len(tickers) > MA_CACHE_MAX_TICKERS:
        tickers = tickers[:MA_CACHE_MAX_TICKERS]

    from ma_cache_service import enqueue_ma_refresh, process_ma_batch
    from routes.stock_routes import get_polygon_api_key

    rows = {r.ticker: r for r in MaCache.query.filter(MaCache.ticker.in_(tickers)).all()}
    stale = [t for t in tickers if t not in rows or not rows[t].is_fresh()]

    def _response():
        fresh_rows = {r.ticker: r for r in MaCache.query.filter(MaCache.ticker.in_(tickers)).all()}
        return jsonify({"ma": {t: fresh_rows[t].to_dict() for t in tickers if t in fresh_rows}}), 200

    if stale_ok:
        if queue_refresh and stale:
            enqueue_ma_refresh(stale)
        return _response()

    if len(tickers) > MA_SYNC_REFRESH_MAX:
        return jsonify({
            "error": (
                f"At most {MA_SYNC_REFRESH_MAX} tickers per synchronous MA refresh. "
                "Send stale_ok: true for large lists."
            ),
        }), 400

    api_key = get_polygon_api_key()
    if not api_key:
        return _response()

    if stale:
        process_ma_batch(current_app._get_current_object(), stale)

    return _response()
