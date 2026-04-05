#!/usr/bin/env python3
"""
Candlestick Chart Flask App

Serves ticker selection and OHLCV candlestick charts from the leo MySQL database.
Uses Highcharts Stock for interactive candlestick charts.

Usage:
    python candlestick_app.py
    Open http://127.0.0.1:5001 in your browser.
"""

import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal

import mysql.connector
import urllib.request
from flask import Flask, jsonify, render_template, request

POLYGON_API_KEY = "pntJnvnXxV3q2nAIdsph4RbT0b_oUlPE"

DB_CONFIG = {
    "host": "127.0.0.1",
    "user": "remote",
    "password": "Chamba4347!",
    "database": "leo",
    "use_pure": True,
}

app = Flask(__name__)


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


class _JsonEncoder(json.JSONEncoder):
    """Handle Decimal and date/datetime serialization."""
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if hasattr(o, "isoformat"):
            return o.isoformat()
        return super().default(o)


def _serialize(data):
    return json.loads(json.dumps(data, cls=_JsonEncoder))


@app.route("/")
def index():
    return render_template("candlestick.html")


@app.route("/scanner")
def scanner():
    return render_template("scanner.html")


@app.route("/api/zones/<ticker>")
def api_zones(ticker):
    """Return consolidation zone start/end dates for a ticker."""
    ticker = ticker.strip().upper()
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT zone_start_date, zone_end_date
            FROM consolidation_zones
            WHERE symbol = %s
            ORDER BY zone_start_date
            """,
            (ticker,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/api/developing_zones/<ticker>")
def api_developing_zones(ticker):
    """Return developing consolidation zone start/end dates for a ticker."""
    ticker = ticker.strip().upper()
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT zone_start_date, zone_end_date
            FROM consolidation_developing
            WHERE symbol = %s
            ORDER BY zone_start_date
            """,
            (ticker,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/developing_scanner")
def developing_scanner():
    return render_template("developing_scanner.html")


@app.route("/api/developing_scanner")
def api_developing_scanner():
    """
    Full scanner query restricted to symbols present in consolidation_developing.
    Accepts the same filter/sort parameters as /api/scanner.
    """
    p = request.args

    days = max(1, min(int(p.get("days", 1)), 30))
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    cur.execute(
        "SELECT DISTINCT bar_date FROM daily_studies ORDER BY bar_date DESC LIMIT %s",
        (days,),
    )
    date_rows = cur.fetchall()

    if not date_rows:
        cur.close(); conn.close()
        return jsonify([])

    target_date = date_rows[-1]["bar_date"]

    where = [
        "s.bar_date = %s",
        "s.symbol IN (SELECT symbol FROM consolidation_developing)",
    ]
    args = [target_date]

    def add(clause, *values):
        where.append(clause)
        args.extend(values)

    if p.get("price_min"): add("o.close >= %s", float(p["price_min"]))
    if p.get("price_max"): add("o.close <= %s", float(p["price_max"]))

    if p.get("above_sma50")  == "1": add("o.close > s.sma_50")
    if p.get("below_sma50")  == "1": add("o.close < s.sma_50")
    if p.get("above_sma150") == "1": add("o.close > s.sma_150")
    if p.get("below_sma150") == "1": add("o.close < s.sma_150")
    if p.get("above_sma200") == "1": add("o.close > s.sma_200")
    if p.get("below_sma200") == "1": add("o.close < s.sma_200")

    if p.get("rsi_min"): add("s.rsi_14 >= %s", float(p["rsi_min"]))
    if p.get("rsi_max"): add("s.rsi_14 <= %s", float(p["rsi_max"]))

    rsi_state_clauses = []
    if p.get("rsi_cross50")      == "1": rsi_state_clauses.append("s.rsi_cross_50 = 1")
    if p.get("rsi_cross_down50") == "1": rsi_state_clauses.append("s.rsi_cross_down_50 = 1")
    if p.get("rsi_above50")      == "1": rsi_state_clauses.append("s.rsi_14 > 50")
    if p.get("rsi_below50")      == "1": rsi_state_clauses.append("s.rsi_14 < 50")
    if rsi_state_clauses:
        add("(" + " OR ".join(rsi_state_clauses) + ")")

    vlabels = [v.strip() for v in p.get("vol_labels", "").split(",") if v.strip()]
    if vlabels:
        ph = ",".join(["%s"] * len(vlabels))
        add(f"s.vol_label IN ({ph})", *vlabels)

    if p.get("vol_ratio_min"): add("o.volume / s.vol_ma >= %s", float(p["vol_ratio_min"]))

    if p.get("atr_squeeze")   == "1": add("s.atr_squeeze = 1")
    if p.get("atr_declining"):        add("s.atr_declining_bars >= %s", int(p["atr_declining"]))
    if p.get("atr_squeeze_break") == "1":
        add("s.atr_squeeze = 0")
        add("""1 = (
            SELECT prev.atr_squeeze
            FROM daily_studies prev
            WHERE prev.symbol  = s.symbol
              AND prev.bar_date < s.bar_date
            ORDER BY prev.bar_date DESC
            LIMIT 1
        )""")

    if p.get("pct_from_high_max"):
        add("(s.dc_upper - o.close) / s.dc_upper * 100 <= %s", float(p["pct_from_high_max"]))
    if p.get("pct_from_low_min"):
        add("(o.close - s.dc_lower) / s.dc_lower * 100 >= %s", float(p["pct_from_low_min"]))

    if p.get("rs_above_sma50") == "1": add("s.rs_line > s.rs_sma_50")
    if p.get("rs_below_sma50") == "1": add("s.rs_line < s.rs_sma_50")

    if p.get("rmv_15_max"):               add("s.rmv_15 <= %s", float(p["rmv_15_max"]))
    if p.get("rmv_15_compressed") == "1": add("s.rmv_15 < 20")

    if p.get("as_1m_min"): add("s.as_1m >= %s", int(p["as_1m_min"]))
    if p.get("as_1m_max"): add("s.as_1m <= %s", int(p["as_1m_max"]))

    valid_sorts = {
        "ticker": "t.ticker", "bar_date": "s.bar_date", "close": "o.close",
        "volume": "o.volume", "vol_ratio": "o.volume/s.vol_ma",
        "rsi_14": "s.rsi_14", "atr_declining_bars": "s.atr_declining_bars",
        "pct_from_high": "(s.dc_upper-o.close)/s.dc_upper*100",
        "pct_from_low":  "(o.close-s.dc_lower)/s.dc_lower*100",
        "sma50_dist":    "(o.close-s.sma_50)/s.sma_50*100",
        "sma200_dist":   "(o.close-s.sma_200)/s.sma_200*100",
        "as_1m":         "s.as_1m",
        "rmv_15":        "s.rmv_15",
    }
    sort_col = valid_sorts.get(p.get("sort_by", "ticker"), "t.ticker")
    sort_dir = "ASC" if p.get("sort_dir", "asc").upper() == "ASC" else "DESC"

    sql = f"""
        SELECT
            t.ticker, t.name, t.sector,
            s.bar_date,
            t.manual_check,
            ROUND(o.close,  2)                                          AS close,
            o.volume,
            ROUND(s.vol_ma, 0)                                          AS vol_ma,
            ROUND(o.volume / NULLIF(s.vol_ma,0), 2)                    AS vol_ratio,
            s.vol_label,
            ROUND(s.rsi_14, 1)                                          AS rsi_14,
            s.rsi_cross_50,
            ROUND(s.sma_50,  2)                                         AS sma_50,
            ROUND(s.sma_150, 2)                                         AS sma_150,
            ROUND(s.sma_200, 2)                                         AS sma_200,
            ROUND(s.ema_10,  2)                                         AS ema_10,
            ROUND(s.ema_20,  2)                                         AS ema_20,
            ROUND(s.dc_upper,2)                                         AS dc_upper,
            ROUND(s.dc_lower,2)                                         AS dc_lower,
            ROUND((s.dc_upper - o.close) / NULLIF(s.dc_upper,0)*100,1) AS pct_from_high,
            ROUND((o.close - s.dc_lower) / NULLIF(s.dc_lower,0)*100,1) AS pct_from_low,
            ROUND((o.close - s.sma_50)   / NULLIF(s.sma_50,0)*100,  1) AS sma50_dist,
            ROUND((o.close - s.sma_200)  / NULLIF(s.sma_200,0)*100, 1) AS sma200_dist,
            s.atr_declining_bars,
            s.atr_streak_len,
            s.atr_streak_ago,
            s.atr_squeeze,
            s.rsi_cross_down_50,
            ROUND(s.rmv_15, 4)                                          AS rmv_15,
            s.as_1m,
            s.rs_line,
            s.rs_sma_50,
            (
                SELECT GROUP_CONCAT(x.vol_label ORDER BY x.bar_date ASC SEPARATOR ',')
                FROM (
                    SELECT vol_label, bar_date
                    FROM daily_studies ds2
                    WHERE ds2.symbol = s.symbol AND ds2.bar_date <= s.bar_date
                    ORDER BY ds2.bar_date DESC
                    LIMIT 5
                ) x
            ) AS vol_history
        FROM daily_studies s
        INNER JOIN daily_ohlcv o ON o.symbol = s.symbol AND o.bar_date = s.bar_date
        INNER JOIN tickers     t ON t.ticker = s.symbol COLLATE utf8mb4_unicode_ci
        WHERE {' AND '.join(where)}
        ORDER BY {sort_col} {sort_dir}
        LIMIT 500
    """

    try:
        cur.execute(sql, args)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify(_serialize(rows))


@app.route("/watchlist")
def watchlist_page():
    return render_template("watchlist.html")


@app.route("/active_symbols")
def active_symbols_page():
    return render_template("active_symbols.html")


@app.route("/api/active_symbols/check/<ticker>", methods=["POST"])
def api_active_symbols_check(ticker):
    """Stamp manual_check = NOW() for a ticker."""
    ticker = ticker.strip().upper()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "UPDATE tickers SET manual_check = NOW() WHERE ticker = %s",
            (ticker,)
        )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return jsonify({"success": True, "ticker": ticker})


@app.route("/api/active_symbols")
def api_active_symbols():
    """Return tickers matching the active-symbols criteria."""
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT ticker, name, sector, manual_check,
                   last_day_close, last_day_volume, market_cap
            FROM leo.tickers
            WHERE last_day_volume > 500000
              AND type = 'CS'
              AND last_day_close > 5
               OR ticker = 'SPY'
            ORDER BY ticker
            """
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/symbol_history")
def symbol_history():
    return render_template("symbol_history.html")


@app.route("/api/symbol_history")
def api_symbol_history():
    """
    Return daily_studies rows for a single symbol over a date range.
    Params: ticker (required), days (default 252), date_from, date_to.
    """
    p = request.args
    ticker = (p.get("ticker") or "").strip().upper()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400

    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    where = ["s.symbol = %s"]
    args  = [ticker]

    where.append("s.sma_200 IS NOT NULL")

    def add(clause, *vals):
        where.append(clause)
        args.extend(vals)

    # Price
    if p.get("price_min"): add("o.close >= %s", float(p["price_min"]))
    if p.get("price_max"): add("o.close <= %s", float(p["price_max"]))

    # MA alignment
    if p.get("above_ema10")  == "1": add("o.close > s.ema_10")
    if p.get("above_ema20")  == "1": add("o.close > s.ema_20")
    if p.get("above_sma50")  == "1": add("o.close > s.sma_50")
    if p.get("below_sma50")  == "1": add("o.close < s.sma_50")
    if p.get("above_sma150") == "1": add("o.close > s.sma_150")
    if p.get("above_sma200") == "1": add("o.close > s.sma_200")

    # RSI
    if p.get("rsi_min"): add("s.rsi_14 >= %s", float(p["rsi_min"]))
    if p.get("rsi_max"): add("s.rsi_14 <= %s", float(p["rsi_max"]))
    rsi_state = []
    if p.get("rsi_above50") == "1": rsi_state.append("s.rsi_14 > 50")
    if p.get("rsi_below50") == "1": rsi_state.append("s.rsi_14 < 50")
    if rsi_state: add("(" + " OR ".join(rsi_state) + ")")

    # Volume label
    vlabels = [v.strip() for v in p.get("vol_labels", "").split(",") if v.strip()]
    if vlabels:
        ph = ",".join(["%s"] * len(vlabels))
        add(f"s.vol_label IN ({ph})", *vlabels)

    # Volume vs MA ratio
    if p.get("vol_ratio_min"): add("o.volume / s.vol_ma >= %s", float(p["vol_ratio_min"]))

    # 52-week range
    if p.get("pct_from_high_max"):
        add("(s.dc_upper - o.close) / s.dc_upper * 100 <= %s", float(p["pct_from_high_max"]))
    if p.get("pct_from_low_min"):
        add("(o.close - s.dc_lower) / s.dc_lower * 100 >= %s", float(p["pct_from_low_min"]))

    # ATR compression
    if p.get("atr_squeeze")       == "1": add("s.atr_squeeze = 1")
    if p.get("atr_squeeze_break") == "1":
        add("s.atr_squeeze = 0")
        add("""1 = (
            SELECT prev.atr_squeeze FROM daily_studies prev
            WHERE prev.symbol = s.symbol AND prev.bar_date < s.bar_date
            ORDER BY prev.bar_date DESC LIMIT 1)""")

    # RMV 15
    if p.get("rmv_15_compressed") == "1": add("s.rmv_15 < 20")
    if p.get("rmv_15_max"):               add("s.rmv_15 <= %s", float(p["rmv_15_max"]))

    # Accumulation Score 1M
    if p.get("as_1m_min"): add("s.as_1m >= %s", int(p["as_1m_min"]))
    if p.get("as_1m_max"): add("s.as_1m <= %s", int(p["as_1m_max"]))

    # RS Line vs RS SMA 50
    if p.get("rs_above_sma50")       == "1": add("s.rs_line > s.rs_sma_50")
    if p.get("rs_below_sma50")       == "1": add("s.rs_line < s.rs_sma_50")
    if p.get("rs_cross_above_sma50") == "1": add("s.rs_cross_above_sma50 = 1")
    if p.get("rs_cross_below_sma50") == "1": add("s.rs_cross_below_sma50 = 1")

    sql = f"""
        SELECT
            s.bar_date,
            ROUND(o.close,  2)                                          AS close,
            o.volume,
            ROUND(o.volume / NULLIF(s.vol_ma, 0), 2)                   AS vol_ratio,
            s.vol_label,
            ROUND(s.rsi_14, 1)                                          AS rsi_14,
            s.rsi_cross_50,
            s.rsi_cross_down_50,
            ROUND(s.sma_50,  2)                                         AS sma_50,
            ROUND(s.sma_150, 2)                                         AS sma_150,
            ROUND(s.sma_200, 2)                                         AS sma_200,
            ROUND(s.ema_10,  2)                                         AS ema_10,
            ROUND(s.ema_20,  2)                                         AS ema_20,
            ROUND(s.atr_10,  4)                                         AS atr_10,
            s.atr_squeeze,
            s.atr_declining_bars,
            ROUND(s.dc_upper, 2)                                        AS dc_upper,
            ROUND(s.dc_lower, 2)                                        AS dc_lower,
            ROUND((s.dc_upper - o.close) / NULLIF(s.dc_upper,0)*100,1) AS pct_from_high,
            ROUND((o.close - s.sma_50)  / NULLIF(s.sma_50,0)*100,   1) AS sma50_dist,
            ROUND((o.close - s.sma_200) / NULLIF(s.sma_200,0)*100,  1) AS sma200_dist,
            ROUND(s.rs_line,   4)                                        AS rs_line,
            ROUND(s.rs_sma_50, 4)                                        AS rs_sma_50,
            s.as_1m,
            ROUND(s.rmv_15, 4)                                           AS rmv_15,
            s.vcp_score,
            s.vcp_is_signal,
            s.vcp_num_legs,
            (
                SELECT GROUP_CONCAT(x.vol_label ORDER BY x.bar_date ASC SEPARATOR ',')
                FROM (
                    SELECT vol_label, bar_date
                    FROM daily_studies ds2
                    WHERE ds2.symbol = s.symbol AND ds2.bar_date <= s.bar_date
                    ORDER BY ds2.bar_date DESC
                    LIMIT 5
                ) x
            ) AS vol_history
        FROM daily_studies s
        INNER JOIN daily_ohlcv o ON o.symbol = s.symbol AND o.bar_date = s.bar_date
        WHERE {' AND '.join(where)}
        ORDER BY s.bar_date DESC
        LIMIT 1260
    """

    try:
        cur.execute(sql, args)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify(_serialize(rows))


@app.route("/api/trading_dates")
def api_trading_dates():
    """Return the last 30 distinct trading dates present in daily_studies."""
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute(
        "SELECT DISTINCT bar_date FROM daily_studies ORDER BY bar_date DESC LIMIT 30"
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()
    result = []
    for i, r in enumerate(rows, start=1):
        result.append({"n": i, "date": str(r["bar_date"])})
    return jsonify(result)


@app.route("/api/scanner")
def api_scanner():
    """
    Dynamic scanner over the daily_ohlcv ⨝ daily_studies join.
    All filter params are optional; results are capped at 500 rows.
    """
    p = request.args

    # ── date window ──────────────────────────────────────────────────────────
    # days=1 → most recent trading date, days=2 → 2nd most recent, etc.
    days = max(1, min(int(p.get("days", 1)), 30))
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)

    # Resolve the single target trading date (Nth most recent)
    cur.execute(
        "SELECT DISTINCT bar_date FROM daily_studies ORDER BY bar_date DESC LIMIT %s",
        (days,),
    )
    date_rows  = cur.fetchall()

    if not date_rows:
        cur.close(); conn.close()
        return jsonify([])

    # Pick the last row → that is exactly N trading days ago
    target_date = date_rows[-1]["bar_date"]

    # ── dynamic WHERE clauses ─────────────────────────────────────────────────
    where = ["s.bar_date = %s"]
    args  = [target_date]

    def add(clause, *values):
        where.append(clause)
        args.extend(values)

    # Price
    if p.get("price_min"): add("o.close >= %s", float(p["price_min"]))
    if p.get("price_max"): add("o.close <= %s", float(p["price_max"]))

    # MA alignment
    if p.get("above_sma50")  == "1": add("o.close > s.sma_50")
    if p.get("below_sma50")  == "1": add("o.close < s.sma_50")
    if p.get("above_sma150") == "1": add("o.close > s.sma_150")
    if p.get("below_sma150") == "1": add("o.close < s.sma_150")
    if p.get("above_sma200") == "1": add("o.close > s.sma_200")
    if p.get("below_sma200") == "1": add("o.close < s.sma_200")
    if p.get("stage2")       == "1": add("o.close > s.sma_50 AND s.sma_50 > s.sma_150 AND s.sma_150 > s.sma_200")
    if p.get("stage4")       == "1": add("o.close < s.sma_50 AND s.sma_50 < s.sma_150 AND s.sma_150 < s.sma_200")
    if p.get("sma50_gt_200") == "1": add("s.sma_50 > s.sma_200")
    if p.get("sma50_lt_200") == "1": add("s.sma_50 < s.sma_200")

    # RSI
    if p.get("rsi_min"):      add("s.rsi_14 >= %s",  float(p["rsi_min"]))
    if p.get("rsi_max"):      add("s.rsi_14 <= %s",  float(p["rsi_max"]))
    # RSI state filters are OR-ed so ticking multiple shows the union
    rsi_state_clauses = []
    if p.get("rsi_cross50")      == "1": rsi_state_clauses.append("s.rsi_cross_50 = 1")
    if p.get("rsi_cross_down50") == "1": rsi_state_clauses.append("s.rsi_cross_down_50 = 1")
    if p.get("rsi_above50")      == "1": rsi_state_clauses.append("s.rsi_14 > 50")
    if p.get("rsi_below50")      == "1": rsi_state_clauses.append("s.rsi_14 < 50")
    if rsi_state_clauses:
        add("(" + " OR ".join(rsi_state_clauses) + ")")

    # Volume label  (comma-separated: XHigh,High,Med,Norm,Low)
    vlabels = [v.strip() for v in p.get("vol_labels", "").split(",") if v.strip()]
    if vlabels:
        ph = ",".join(["%s"] * len(vlabels))
        add(f"s.vol_label IN ({ph})", *vlabels)

    # Volume vs MA ratio
    if p.get("vol_ratio_min"): add("o.volume / s.vol_ma >= %s", float(p["vol_ratio_min"]))

    # ATR compression
    if p.get("atr_squeeze")   == "1": add("s.atr_squeeze = 1")
    if p.get("atr_declining"):        add("s.atr_declining_bars >= %s", int(p["atr_declining"]))
    if p.get("atr_squeeze_break") == "1":
        add("s.atr_squeeze = 0")
        add("""1 = (
            SELECT prev.atr_squeeze
            FROM daily_studies prev
            WHERE prev.symbol  = s.symbol
              AND prev.bar_date < s.bar_date
            ORDER BY prev.bar_date DESC
            LIMIT 1
        )""")

    # VCP (kept for backwards compatibility)
    if p.get("vcp_score_min"):  add("s.vcp_score >= %s",  float(p["vcp_score_min"]))
    if p.get("vcp_score_max"):  add("s.vcp_score <= %s",  float(p["vcp_score_max"]))
    if p.get("vcp_signal")   == "1": add("s.vcp_is_signal = 1")
    if p.get("vcp_legs_min"):   add("s.vcp_num_legs >= %s", int(p["vcp_legs_min"]))

    # 52-week range
    if p.get("pct_from_high_max"):
        add("(s.dc_upper - o.close) / s.dc_upper * 100 <= %s", float(p["pct_from_high_max"]))
    if p.get("pct_from_low_min"):
        add("(o.close - s.dc_lower) / s.dc_lower * 100 >= %s", float(p["pct_from_low_min"]))

    # RS Line vs RS SMA 50
    if p.get("rs_above_sma50") == "1": add("s.rs_line > s.rs_sma_50")
    if p.get("rs_below_sma50") == "1": add("s.rs_line < s.rs_sma_50")

    # RMV 15 compression
    if p.get("rmv_15_max"):              add("s.rmv_15 <= %s", float(p["rmv_15_max"]))
    if p.get("rmv_15_compressed") == "1": add("s.rmv_15 < 20")

    # Accumulation Score 1M
    if p.get("as_1m_min"): add("s.as_1m >= %s", int(p["as_1m_min"]))
    if p.get("as_1m_max"): add("s.as_1m <= %s", int(p["as_1m_max"]))

    # RSI divergence helper: near 52W high but RSI not extreme
    if p.get("near_high_low_rsi") == "1":
        add("(s.dc_upper - o.close) / s.dc_upper * 100 <= 10 AND s.rsi_14 < 80")

    # ── Sorting ───────────────────────────────────────────────────────────────
    valid_sorts = {
        "ticker": "t.ticker", "bar_date": "s.bar_date", "close": "o.close",
        "volume": "o.volume", "vol_ratio": "o.volume/s.vol_ma",
        "rsi_14": "s.rsi_14", "atr_declining_bars": "s.atr_declining_bars",
        "pct_from_high": "(s.dc_upper-o.close)/s.dc_upper*100",
        "pct_from_low":  "(o.close-s.dc_lower)/s.dc_lower*100",
        "sma50_dist":    "(o.close-s.sma_50)/s.sma_50*100",
        "sma200_dist":   "(o.close-s.sma_200)/s.sma_200*100",
        "as_1m":         "s.as_1m",
        "rmv_15":        "s.rmv_15",
    }
    sort_col = valid_sorts.get(p.get("sort_by", "ticker"), "t.ticker")
    sort_dir = "ASC" if p.get("sort_dir", "asc").upper() == "ASC" else "DESC"

    sql = f"""
        SELECT
            t.ticker, t.name, t.sector,
            s.bar_date,
            t.manual_check,
            ROUND(o.close,  2)                                         AS close,
            o.volume,
            ROUND(s.vol_ma, 0)                                         AS vol_ma,
            ROUND(o.volume / NULLIF(s.vol_ma,0), 2)                   AS vol_ratio,
            s.vol_label,
            ROUND(s.rsi_14, 1)                                         AS rsi_14,
            s.rsi_cross_50,
            ROUND(s.sma_50,  2)                                        AS sma_50,
            ROUND(s.sma_150, 2)                                        AS sma_150,
            ROUND(s.sma_200, 2)                                        AS sma_200,
            ROUND(s.ema_10,  2)                                        AS ema_10,
            ROUND(s.ema_20,  2)                                        AS ema_20,
            ROUND(s.dc_upper,2)                                        AS dc_upper,
            ROUND(s.dc_lower,2)                                        AS dc_lower,
            ROUND((s.dc_upper - o.close) / NULLIF(s.dc_upper,0)*100,1) AS pct_from_high,
            ROUND((o.close - s.dc_lower) / NULLIF(s.dc_lower,0)*100,1) AS pct_from_low,
            ROUND((o.close - s.sma_50)   / NULLIF(s.sma_50,0)*100,  1) AS sma50_dist,
            ROUND((o.close - s.sma_200)  / NULLIF(s.sma_200,0)*100, 1) AS sma200_dist,
            s.atr_declining_bars,
            s.atr_streak_len,
            s.atr_streak_ago,
            s.atr_squeeze,
            s.rsi_cross_down_50,
            ROUND(s.rmv_15, 4)                                         AS rmv_15,
            s.as_1m,
            s.rs_line,
            s.rs_sma_50,
            (
                SELECT GROUP_CONCAT(x.vol_label ORDER BY x.bar_date ASC SEPARATOR ',')
                FROM (
                    SELECT vol_label, bar_date
                    FROM daily_studies ds2
                    WHERE ds2.symbol = s.symbol AND ds2.bar_date <= s.bar_date
                    ORDER BY ds2.bar_date DESC
                    LIMIT 5
                ) x
            ) AS vol_history
        FROM daily_studies s
        INNER JOIN daily_ohlcv o   ON o.symbol   = s.symbol   AND o.bar_date = s.bar_date
        INNER JOIN tickers     t   ON t.ticker   = s.symbol COLLATE utf8mb4_unicode_ci
        WHERE {' AND '.join(where)}
        ORDER BY {sort_col} {sort_dir}
        LIMIT 500
    """

    try:
        cur.execute(sql, args)
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return jsonify(_serialize(rows))


@app.route("/api/tickers")
def api_tickers():
    """Search tickers by symbol or name (CS, volume>500k, close>5 only). Returns up to 100 results."""
    q = request.args.get("q", "").strip().upper()
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        if q:
            cur.execute(
                """
                SELECT ticker, name, sector, last_day_close, market_cap
                FROM tickers
                WHERE last_day_volume > 500000
                  AND type = 'CS'
                  AND last_day_close > 5
                  AND (ticker LIKE %s OR name LIKE %s)
                ORDER BY
                    CASE WHEN ticker LIKE %s THEN 0 ELSE 1 END,
                    ticker
                LIMIT 100
                """,
                (q + "%", "%" + q + "%", q + "%"),
            )
        else:
            cur.execute(
                """
                SELECT ticker, name, sector, last_day_close, market_cap
                FROM tickers
                WHERE last_day_volume > 500000
                  AND type = 'CS'
                  AND last_day_close > 5
                ORDER BY ticker
                LIMIT 200
                """
            )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/api/chart/<ticker>")
def api_chart(ticker):
    """Return OHLCV bars + pre-computed studies + ticker info for the given symbol."""
    ticker = ticker.upper().strip()
    conn = get_conn()
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute(
            """
            SELECT
                ticker, name, sector, industry, type, market, primary_exchange,
                last_day_close, last_day_open, last_day_high, last_day_low,
                last_day_volume, last_day_date, last_day_vwap,
                market_cap, trailing_pe, forward_pe, price_to_book,
                trailing_eps, forward_eps, target_mean_price, recommendation_mean,
                earnings_growth, revenue_growth, profit_margins,
                debt_to_equity, current_ratio, short_ratio,
                short_percent_of_float, held_percent_institutions,
                held_percent_insiders, average_volume
            FROM tickers
            WHERE ticker = %s
            """,
            (ticker,),
        )
        info = cur.fetchone()

        cur.execute(
            """
            SELECT
                o.bar_date, o.open, o.high, o.low, o.close, o.volume,
                s.sma_50, s.sma_150, s.sma_200,
                s.ema_10, s.ema_20,
                s.rsi_14, s.rsi_cross_50,
                s.vol_ma, s.vol_label,
                s.dc_upper, s.dc_lower,
                s.atr_10, s.atr_squeeze,
                s.pivot_high,
                s.atr_declining_bars,
                s.rs_line, s.rs_ema_21, s.rs_sma_50, s.rs_new_high,
                s.rmv_15,
                s.as_1m
            FROM daily_ohlcv o
            LEFT JOIN daily_studies s
                ON s.symbol = o.symbol AND s.bar_date = o.bar_date
            WHERE o.symbol = %s
            ORDER BY o.bar_date
            """,
            (ticker,),
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    _LABEL_COLORS = {
        "XHigh": "#ff0000",
        "High":  "#ff7800",
        "Med":   "#ffcf03",
        "Norm":  "#a0d6dc",
        "Low":   "#1f9cac",
    }

    ohlcv        = []
    volume       = []
    sma50_s      = []
    sma150_s     = []
    sma200_s     = []
    ema10_s      = []
    ema20_s      = []
    rsi14_s      = []
    rsi_cup_ts   = []   # cross-up timestamps (RSI crosses above 50)
    rsi_cdn_ts   = []   # cross-down timestamps
    vol_bars_s   = []   # [{x, y, color}, ...] per-bar heatmap volume
    vol_ma_s     = []
    dc_upper_s   = []
    dc_lower_s   = []
    atr10_s      = []
    atr_sq_s     = []   # scatter points where atr_squeeze = 1
    atr_decl_s   = []   # [[ts, declining_bars], ...]
    pivot_chg    = []   # [{ts, high}] distinct pivot level changes
    rs_line_s    = []
    rs_ema21_s   = []
    rs_sma50_s   = []
    rs_new_hi_ts = []   # timestamps where rs_new_high = 1
    rmv15_s      = []
    as1m_s       = []
    has_studies  = False
    prev_pivot   = None
    prev_rsi     = None
    last_as1m    = None

    for r in rows:
        ts = int(
            datetime.combine(r["bar_date"], time())
            .replace(tzinfo=timezone.utc)
            .timestamp()
            * 1000
        )
        o  = float(r["open"])   if r["open"]   is not None else None
        h  = float(r["high"])   if r["high"]   is not None else None
        lo = float(r["low"])    if r["low"]    is not None else None
        c  = float(r["close"])  if r["close"]  is not None else None
        v  = int(r["volume"])   if r["volume"] is not None else 0

        ohlcv.append([ts, o, h, lo, c])
        volume.append([ts, v])

        sma50_v  = float(r["sma_50"])    if r["sma_50"]    is not None else None
        sma150_v = float(r["sma_150"])   if r["sma_150"]   is not None else None
        sma200_v = float(r["sma_200"])   if r["sma_200"]   is not None else None
        ema10_v  = float(r["ema_10"])    if r["ema_10"]    is not None else None
        ema20_v  = float(r["ema_20"])    if r["ema_20"]    is not None else None
        rsi_v    = float(r["rsi_14"])    if r["rsi_14"]    is not None else None
        atr_v    = float(r["atr_10"])    if r["atr_10"]    is not None else None
        dcu_v    = float(r["dc_upper"])  if r["dc_upper"]  is not None else None
        dcl_v    = float(r["dc_lower"])  if r["dc_lower"]  is not None else None
        ph_v     = float(r["pivot_high"])if r["pivot_high"]is not None else None
        vol_ma_v = float(r["vol_ma"])    if r["vol_ma"]    is not None else None
        vol_lbl  = r["vol_label"] or "Norm"
        decl_v   = r["atr_declining_bars"]

        if sma50_v is not None:
            has_studies = True
            sma50_s.append([ts, sma50_v])
        if sma150_v is not None: sma150_s.append([ts, sma150_v])
        if sma200_v is not None: sma200_s.append([ts, sma200_v])
        if ema10_v  is not None: ema10_s.append([ts, ema10_v])
        if ema20_v  is not None: ema20_s.append([ts, ema20_v])
        if rsi_v    is not None: rsi14_s.append([ts, rsi_v])

        # RSI crossovers — cross_50 flag covers upward; derive downward from sequence
        if r["rsi_cross_50"] == 1:
            rsi_cup_ts.append(ts)
        if rsi_v is not None and prev_rsi is not None:
            if prev_rsi > 50 and rsi_v <= 50:
                rsi_cdn_ts.append(ts)
        if rsi_v is not None:
            prev_rsi = rsi_v

        # Volume heatmap — color from pre-computed label
        color = _LABEL_COLORS.get(vol_lbl, "#a0d6dc")
        vol_bars_s.append({"x": ts, "y": v, "color": color})
        if vol_ma_v is not None: vol_ma_s.append([ts, vol_ma_v])

        # Donchian 252
        if dcu_v is not None: dc_upper_s.append([ts, dcu_v])
        if dcl_v is not None: dc_lower_s.append([ts, dcl_v])

        # ATR(10)
        if atr_v is not None:
            atr10_s.append([ts, atr_v])
            if r["atr_squeeze"] == 1:
                atr_sq_s.append([ts, atr_v])

        # ATR declining streak
        if decl_v is not None:
            atr_decl_s.append([ts, int(decl_v)])

        # Pivot highs — emit only when the level changes (step-function breakpoints)
        if ph_v is not None and ph_v != prev_pivot:
            pivot_chg.append({"ts": ts, "high": ph_v})
            prev_pivot = ph_v

        # RS Line vs SPY
        rs_v    = float(r["rs_line"])   if r["rs_line"]   is not None else None
        rs_e_v  = float(r["rs_ema_21"]) if r["rs_ema_21"] is not None else None
        rs_s_v  = float(r["rs_sma_50"]) if r["rs_sma_50"] is not None else None
        if rs_v   is not None: rs_line_s.append([ts, rs_v])
        if rs_e_v is not None: rs_ema21_s.append([ts, rs_e_v])
        if rs_s_v is not None: rs_sma50_s.append([ts, rs_s_v])
        if r["rs_new_high"] == 1 and rs_v is not None:
            rs_new_hi_ts.append(ts)

        # RMV 15
        rmv15_v = float(r["rmv_15"]) if r["rmv_15"] is not None else None
        if rmv15_v is not None: rmv15_s.append([ts, rmv15_v])

        # AS 1M — build time series and track latest non-null value
        if r.get("as_1m") is not None:
            last_as1m = int(r["as_1m"])
            as1m_s.append([ts, last_as1m])

    # Keep last 5 distinct pivot levels (matches JS calcPivotHighs count=5)
    pivot_chg = pivot_chg[-5:]

    studies = {
        "sma50":          sma50_s,
        "sma150":         sma150_s,
        "sma200":         sma200_s,
        "ema10":          ema10_s,
        "ema20":          ema20_s,
        "rsi14":          rsi14_s,
        "rsiCrossUpTs":   rsi_cup_ts,
        "rsiCrossDownTs": rsi_cdn_ts,
        "volBars":        vol_bars_s,
        "volMa":          vol_ma_s,
        "dcUpper":        dc_upper_s,
        "dcLower":        dc_lower_s,
        "atr10":          atr10_s,
        "atrSqueeze":     atr_sq_s,
        "pivotHighs":     pivot_chg,
        "atrDeclining":   atr_decl_s,
        "rsLine":         rs_line_s,
        "rsEma21":        rs_ema21_s,
        "rsSma50":        rs_sma50_s,
        "rsNewHighTs":    rs_new_hi_ts,
        "rmv15":          rmv15_s,
        "as1m":           as1m_s,
    } if has_studies else None

    if info is not None and last_as1m is not None:
        info["as_1m"] = last_as1m

    return jsonify(
        {
            "ohlcv":   ohlcv,
            "volume":  volume,
            "studies": _serialize(studies),
            "info":    _serialize(info) if info else None,
        }
    )


@app.route("/orders")
def orders_page():
    return render_template("orders.html")


@app.route("/api/orders")
def api_orders():
    """Return all trades from the trades table, newest first."""
    _ensure_trades_table()
    _migrate_add_last_price()
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT id, order_id, symbol, qty,
                   entry_price, stop_price, target_price,
                   stop_pct, target_pct, rr_ratio,
                   order_status, is_paper,
                   submitted_at, closed_at, exit_price, last_price,
                   filled_avg_price, filled_qty,
                   alpaca_unrlzd_pl, alpaca_unrlzd_plpc,
                   notes
            FROM trades
            ORDER BY submitted_at DESC
            LIMIT 500
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/api/orders/<int:trade_id>/close", methods=["POST"])
def api_close_order(trade_id):
    """Mark a trade as closed with an optional exit price and notes."""
    data       = request.get_json(force=True)
    exit_price = data.get("exit_price")
    notes      = data.get("notes")
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE trades
            SET closed_at  = NOW(),
                order_status = 'closed',
                exit_price = %s,
                notes      = %s
            WHERE id = %s
            """,
            (exit_price, notes, trade_id),
        )
        conn.commit()
        affected = cur.rowcount
        cur.close()
    finally:
        conn.close()
    if affected == 0:
        return jsonify({"error": "Trade not found"}), 404
    return jsonify({"success": True})


@app.route("/api/orders/open_symbols")
def api_open_symbols():
    """Return the set of symbols that have at least one non-closed trade."""
    _ensure_trades_table()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT symbol FROM trades WHERE closed_at IS NULL"
        )
        symbols = [r[0] for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    return jsonify(symbols)


@app.route("/api/watchlist", methods=["GET"])
def api_watchlist_get():
    """Return all watchlist entries, newest first."""
    _ensure_watchlist_table()
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, ticker, price_target, box_date, note, created_at "
            "FROM watchlist ORDER BY created_at DESC LIMIT 1000"
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/api/watchlist", methods=["POST"])
def api_watchlist_post():
    """Add a new watchlist entry."""
    _ensure_watchlist_table()
    data         = request.get_json(force=True)
    ticker       = data.get("ticker", "").strip().upper()
    price_target = data.get("price_target")
    box_date     = data.get("box_date")
    note         = data.get("note") or None

    if not ticker or price_target is None or not box_date:
        return jsonify({"error": "ticker, price_target and box_date are required"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO watchlist (ticker, price_target, box_date, note) "
            "VALUES (%s, %s, %s, %s)",
            (ticker, float(price_target), box_date, note),
        )
        conn.commit()
        new_id = cur.lastrowid
        cur.close()
    finally:
        conn.close()
    return jsonify({"success": True, "id": new_id})


@app.route("/api/watchlist/<int:entry_id>", methods=["DELETE"])
def api_watchlist_delete(entry_id):
    """Remove a watchlist entry."""
    _ensure_watchlist_table()
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM watchlist WHERE id = %s", (entry_id,))
        conn.commit()
        affected = cur.rowcount
        cur.close()
    finally:
        conn.close()
    if affected == 0:
        return jsonify({"error": "Entry not found"}), 404
    return jsonify({"success": True})


@app.route("/api/polygon/snapshots")
def api_polygon_snapshots():
    """
    Proxy Polygon snapshot endpoint for a list of tickers.
    Query param: tickers=AAPL,MSFT,...
    Returns: { "AAPL": { "last": 150.25, "prev_close": 148.10, "change_pct": 1.45 }, ... }
    """
    tickers_param = request.args.get("tickers", "").strip().upper()
    if not tickers_param:
        return jsonify({})

    tickers = [t.strip() for t in tickers_param.split(",") if t.strip()]
    if not tickers:
        return jsonify({})

    # Polygon allows up to 250 tickers per request
    url = (
        "https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
        "?tickers=" + ",".join(tickers) +
        "&apiKey=" + POLYGON_API_KEY
    )
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        return jsonify({"error": str(e)}), 502

    result = {}
    for t in (data.get("tickers") or []):
        sym = t.get("ticker", "")
        last_trade = (t.get("lastTrade") or {}).get("p")
        prev_close = (t.get("prevDay") or {}).get("c")
        change_pct = None
        if last_trade is not None and prev_close and prev_close != 0:
            change_pct = round((last_trade - prev_close) / prev_close * 100, 2)
        result[sym] = {
            "last":       last_trade,
            "prev_close": prev_close,
            "change_pct": change_pct,
        }
    return jsonify(result)


def _ensure_boxes_table():
    """Create the chart_boxes table if it does not yet exist."""
    ddl = """
        CREATE TABLE IF NOT EXISTS chart_boxes (
            id           INT            NOT NULL AUTO_INCREMENT,
            client_id    BIGINT         NOT NULL,
            ticker       VARCHAR(20)    NOT NULL COLLATE utf8mb4_unicode_ci,
            x1           BIGINT         NOT NULL,
            x2           BIGINT         NOT NULL,
            y1           DECIMAL(20,6)  NOT NULL,
            y2           DECIMAL(20,6)  NOT NULL,
            color        VARCHAR(20)    NOT NULL DEFAULT '#ffd700',
            watchlist_id INT            DEFAULT NULL,
            created_at   DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            UNIQUE KEY uq_ticker_client (ticker, client_id),
            KEY ix_ticker (ticker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
    finally:
        conn.close()


@app.route("/api/boxes")
def api_boxes_get():
    """Return all boxes for a ticker."""
    _ensure_boxes_table()
    ticker = request.args.get("ticker", "").strip().upper()
    if not ticker:
        return jsonify([])
    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT client_id AS id, x1, x2, y1, y2, color, watchlist_id "
            "FROM chart_boxes WHERE ticker = %s ORDER BY created_at",
            (ticker,)
        )
        rows = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    return jsonify(_serialize(rows))


@app.route("/api/boxes/sync", methods=["POST"])
def api_boxes_sync():
    """Replace all boxes for a ticker with the provided list."""
    _ensure_boxes_table()
    data   = request.get_json(force=True)
    ticker = (data.get("ticker") or "").strip().upper()
    boxes  = data.get("boxes") or []
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM chart_boxes WHERE ticker = %s", (ticker,))
        for b in boxes:
            cur.execute(
                """INSERT INTO chart_boxes
                       (client_id, ticker, x1, x2, y1, y2, color, watchlist_id)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       x1=VALUES(x1), x2=VALUES(x2),
                       y1=VALUES(y1), y2=VALUES(y2),
                       color=VALUES(color), watchlist_id=VALUES(watchlist_id)""",
                (
                    int(b["id"]), ticker,
                    int(b["x1"]), int(b["x2"]),
                    float(b["y1"]), float(b["y2"]),
                    b.get("color", "#ffd700"),
                    b.get("watchlist_id") or None,
                )
            )
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return jsonify({"success": True})


def _ensure_watchlist_table():
    """Create the watchlist table if it does not yet exist."""
    ddl = """
        CREATE TABLE IF NOT EXISTS watchlist (
            id           INT            NOT NULL AUTO_INCREMENT,
            ticker       VARCHAR(20)    NOT NULL,
            price_target DECIMAL(14,4)  NOT NULL,
            box_date     DATE           NOT NULL,
            note         VARCHAR(255)   DEFAULT NULL,
            created_at   DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id),
            KEY ix_ticker (ticker)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
    finally:
        conn.close()


@app.route("/api/alpaca/snapshots")
def api_alpaca_snapshots():
    """
    Return latest ask/last-trade price for a comma-separated list of symbols.
    Query param: ?symbols=AAPL,TSLA,NVDA
    Returns: { "AAPL": 185.23, "TSLA": 245.10, ... }
    """
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest

    symbols_raw = request.args.get("symbols", "")
    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    if not symbols:
        return jsonify({}), 200

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT `key`, `secret` FROM settings WHERE id = 1")
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row or not row.get("key"):
        return jsonify({"error": "Alpaca credentials not configured"}), 400

    data_client = StockHistoricalDataClient(
        api_key=row["key"], secret_key=row["secret"]
    )

    result = {}
    try:
        quotes = data_client.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=symbols)
        )
        for sym in symbols:
            ask = float(quotes[sym].ask_price or 0) if sym in quotes else 0
            result[sym] = ask if ask > 0 else None
    except Exception:
        pass

    # Fall back to last trade for any symbol that still has no price
    missing = [s for s in symbols if not result.get(s)]
    if missing:
        try:
            trades = data_client.get_stock_latest_trade(
                StockLatestTradeRequest(symbol_or_symbols=missing)
            )
            for sym in missing:
                if sym in trades:
                    result[sym] = float(trades[sym].price or 0) or None
        except Exception:
            pass

    return jsonify(result)


@app.route("/api/alpaca/reconcile", methods=["POST"])
def api_alpaca_reconcile():
    """
    Reconcile ALL closed trades against Alpaca's actual fill data.
    Force-overwrites filled_avg_price, filled_qty, and exit_price
    so closed P&L exactly matches Alpaca's records.
    """
    from alpaca.trading.client import TradingClient

    _migrate_add_last_price()

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, order_id, is_paper FROM trades WHERE closed_at IS NOT NULL"
        )
        closed_trades = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not closed_trades:
        return jsonify({"reconciled": 0, "errors": []})

    conn2 = get_conn()
    try:
        c2 = conn2.cursor(dictionary=True)
        c2.execute("SELECT `key`, `secret`, `endpoint` FROM settings WHERE id = 1")
        creds = c2.fetchone()
        c2.close()
    finally:
        conn2.close()

    if not creds or not creds.get("key"):
        return jsonify({"error": "Alpaca credentials not configured"}), 400

    def _ev(val):
        if val is None:
            return None
        s = str(val)
        return s.split(".")[-1].lower() if "." in s else s.lower()

    reconciled = 0
    errors     = []

    for trade in closed_trades:
        try:
            client = TradingClient(
                api_key=creds["key"], secret_key=creds["secret"],
                paper=bool(trade["is_paper"])
            )
            order = client.get_order_by_id(trade["order_id"])

            filled_avg = float(order.filled_avg_price) if order.filled_avg_price else None
            filled_qty = float(order.filled_qty)       if order.filled_qty       else None

            legs       = order.legs or []
            leg_filled = next((l for l in legs if _ev(l.status) == "filled"), None)
            exit_price = float(leg_filled.filled_avg_price) if leg_filled and leg_filled.filled_avg_price else None

            conn_upd = get_conn()
            try:
                c_upd = conn_upd.cursor()
                if exit_price is not None:
                    c_upd.execute(
                        """UPDATE trades
                           SET filled_avg_price = %s,
                               filled_qty       = %s,
                               exit_price       = %s
                           WHERE id = %s""",
                        (filled_avg, filled_qty, exit_price, trade["id"]),
                    )
                else:
                    # No filled exit leg found — update entry fill only, keep existing exit_price
                    c_upd.execute(
                        "UPDATE trades SET filled_avg_price = %s, filled_qty = %s WHERE id = %s",
                        (filled_avg, filled_qty, trade["id"]),
                    )
                conn_upd.commit()
                c_upd.close()
            finally:
                conn_upd.close()
            reconciled += 1
        except Exception as exc:
            errors.append({"order_id": trade["order_id"], "error": str(exc)})

    return jsonify({"reconciled": reconciled, "errors": errors})


@app.route("/api/alpaca/order/<order_id>")
def api_alpaca_order_detail(order_id):
    """Fetch full bracket order details from Alpaca including all legs."""
    from alpaca.trading.client import TradingClient

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT `key`, `secret`, is_paper FROM settings s "
                    "JOIN trades t ON t.order_id = %s "
                    "WHERE s.id = 1 LIMIT 1", (order_id,))
        row = cur.fetchone()
        if not row:
            # fallback — just load settings
            cur.execute("SELECT `key`, `secret`, `endpoint` FROM settings WHERE id = 1")
            row = cur.fetchone()
            is_paper = "paper" in (row.get("endpoint") or "").lower() if row else True
        else:
            is_paper = bool(row.get("is_paper", True))
        cur.close()
    finally:
        conn.close()

    if not row or not row.get("key"):
        return jsonify({"error": "Alpaca credentials not configured"}), 400

    try:
        client = TradingClient(
            api_key=row["key"], secret_key=row["secret"], paper=is_paper
        )
        order = client.get_order_by_id(order_id)

        def ev(val):
            """Strip enum class prefix, e.g. 'OrderStatus.FILLED' → 'filled'."""
            if val is None:
                return None
            s = str(val)
            return s.split(".")[-1].lower() if "." in s else s.lower()

        def fmt_order(o):
            return {
                "id":           str(o.id),
                "client_id":    str(o.client_order_id or ""),
                "symbol":       str(o.symbol),
                "side":         ev(o.side),
                "type":         ev(o.type),
                "order_class":  ev(o.order_class) if o.order_class else None,
                "qty":          str(o.qty or ""),
                "filled_qty":   str(o.filled_qty or "0"),
                "limit_price":  str(o.limit_price)  if o.limit_price  else None,
                "stop_price":   str(o.stop_price)   if o.stop_price   else None,
                "filled_avg_price": str(o.filled_avg_price) if o.filled_avg_price else None,
                "status":       ev(o.status),
                "time_in_force":ev(o.time_in_force),
                "submitted_at": o.submitted_at.isoformat() if o.submitted_at else None,
                "filled_at":    o.filled_at.isoformat()    if o.filled_at    else None,
                "canceled_at":  o.canceled_at.isoformat()  if o.canceled_at  else None,
                "expired_at":   o.expired_at.isoformat()   if o.expired_at   else None,
                "legs": [fmt_order(l) for l in (o.legs or [])],
            }

        return jsonify(fmt_order(order))
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/alpaca/sync", methods=["POST"])
def api_alpaca_sync():
    """Sync order statuses from Alpaca for all non-closed trades."""
    from alpaca.trading.client import TradingClient

    # Add last_price column if it doesn't exist yet
    _migrate_add_last_price()

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, order_id, symbol, is_paper FROM trades WHERE closed_at IS NULL"
        )
        open_trades = cur.fetchall()
        cur.close()
    finally:
        conn.close()

    if not open_trades:
        return jsonify({"updated": 0})

    # Load credentials once
    conn2 = get_conn()
    try:
        c2 = conn2.cursor(dictionary=True)
        c2.execute("SELECT `key`, `secret`, `endpoint` FROM settings WHERE id = 1")
        creds = c2.fetchone()
        c2.close()
    finally:
        conn2.close()

    if not creds or not creds.get("key"):
        return jsonify({"error": "Alpaca credentials not configured"}), 400

    def _ev(val):
        """Strip Python enum prefix: 'OrderStatus.FILLED' → 'filled'."""
        if val is None:
            return None
        s = str(val)
        return s.split(".")[-1].lower() if "." in s else s.lower()

    TERMINAL = {"filled", "canceled", "expired", "replaced"}

    # Fetch current positions once per account type — gives Alpaca-exact P&L figures
    # pos_map: (is_paper, symbol_upper) -> {last_price, unrlzd_pl, unrlzd_plpc}
    pos_map = {}
    for is_paper in set(t["is_paper"] for t in open_trades):
        try:
            client = TradingClient(
                api_key=creds["key"], secret_key=creds["secret"], paper=bool(is_paper)
            )
            for pos in client.get_all_positions():
                sym = str(pos.symbol).upper()
                try:
                    pos_map[(bool(is_paper), sym)] = {
                        "last_price":         float(pos.current_price)      if pos.current_price      else None,
                        "alpaca_unrlzd_pl":   float(pos.unrealized_pl)      if pos.unrealized_pl      else None,
                        "alpaca_unrlzd_plpc": float(pos.unrealized_plpc)    if pos.unrealized_plpc    else None,
                    }
                except Exception:
                    pass
        except Exception:
            pass

    # Build one client per account type (paper/live) to avoid re-authenticating per trade
    clients = {}
    for is_paper in set(t["is_paper"] for t in open_trades):
        try:
            clients[bool(is_paper)] = TradingClient(
                api_key=creds["key"], secret_key=creds["secret"], paper=bool(is_paper)
            )
        except Exception as exc:
            errors = []
            return jsonify({"updated": 0, "errors": [{"error": str(exc)}]})

    updated = 0
    errors  = []
    for trade in open_trades:
        is_paper = trade["is_paper"]
        try:
            client = clients.get(bool(is_paper))
            if client is None:
                continue
            order  = client.get_order_by_id(trade["order_id"])
            status = _ev(order.status)

            # For bracket orders: check if any leg has filled/canceled,
            # which means the exit has occurred even if parent status is just "filled"
            legs         = order.legs or []
            leg_statuses = [_ev(l.status) for l in legs]
            leg_filled   = next((l for l in legs if _ev(l.status) == "filled"), None)
            exit_price   = float(leg_filled.filled_avg_price) if leg_filled and leg_filled.filled_avg_price else None

            # Trade is done when parent AND at least one leg is terminal
            is_closed = status in TERMINAL and any(s in TERMINAL for s in leg_statuses) if legs else status in TERMINAL

            pos_data = pos_map.get((bool(is_paper), trade["symbol"].upper()), {})

            # Actual fill details from parent order
            filled_avg = float(order.filled_avg_price) if order.filled_avg_price else None
            filled_qty = float(order.filled_qty)       if order.filled_qty       else None

            conn3  = get_conn()
            try:
                c3 = conn3.cursor()
                if is_closed:
                    c3.execute(
                        """UPDATE trades
                           SET order_status      = %s,
                               closed_at         = NOW(),
                               exit_price        = COALESCE(exit_price, %s),
                               filled_avg_price  = COALESCE(filled_avg_price, %s),
                               filled_qty        = COALESCE(filled_qty, %s),
                               last_price        = NULL,
                               alpaca_unrlzd_pl  = NULL,
                               alpaca_unrlzd_plpc= NULL
                           WHERE id = %s""",
                        (status, exit_price, filled_avg, filled_qty, trade["id"]),
                    )
                else:
                    c3.execute(
                        """UPDATE trades
                           SET order_status       = %s,
                               filled_avg_price   = COALESCE(filled_avg_price, %s),
                               filled_qty         = COALESCE(filled_qty, %s),
                               last_price         = %s,
                               alpaca_unrlzd_pl   = %s,
                               alpaca_unrlzd_plpc = %s
                           WHERE id = %s""",
                        (status,
                         filled_avg, filled_qty,
                         pos_data.get("last_price"),
                         pos_data.get("alpaca_unrlzd_pl"),
                         pos_data.get("alpaca_unrlzd_plpc"),
                         trade["id"]),
                    )
                conn3.commit()
                c3.close()
            finally:
                conn3.close()
            updated += 1
        except Exception as exc:
            errors.append({"order_id": trade["order_id"], "error": str(exc)})

    # ── Backfill fill data for legacy closed trades missing filled_avg_price ──
    conn_bf = get_conn()
    try:
        cur_bf = conn_bf.cursor(dictionary=True)
        cur_bf.execute(
            "SELECT id, order_id, is_paper FROM trades "
            "WHERE closed_at IS NOT NULL AND filled_avg_price IS NULL"
        )
        legacy_trades = cur_bf.fetchall()
        cur_bf.close()
    finally:
        conn_bf.close()

    for trade in legacy_trades:
        try:
            client = TradingClient(
                api_key=creds["key"], secret_key=creds["secret"],
                paper=bool(trade["is_paper"])
            )
            order      = client.get_order_by_id(trade["order_id"])
            filled_avg = float(order.filled_avg_price) if order.filled_avg_price else None
            filled_qty = float(order.filled_qty)       if order.filled_qty       else None
            if filled_avg or filled_qty:
                conn_upd = get_conn()
                try:
                    c_upd = conn_upd.cursor()
                    c_upd.execute(
                        "UPDATE trades SET filled_avg_price = COALESCE(filled_avg_price, %s), "
                        "filled_qty = COALESCE(filled_qty, %s) WHERE id = %s",
                        (filled_avg, filled_qty, trade["id"]),
                    )
                    conn_upd.commit()
                    c_upd.close()
                finally:
                    conn_upd.close()
        except Exception:
            pass  # silently skip if order no longer exists in Alpaca

    return jsonify({"updated": updated, "errors": errors})


def _migrate_studies_columns():
    """Add ATR / pivot columns to daily_studies if they don't exist yet."""
    new_cols = [
        ("atr_10",             "DECIMAL(14,4) DEFAULT NULL"),
        ("atr_squeeze",        "TINYINT(1)    NOT NULL DEFAULT 0"),
        ("pivot_high",         "DECIMAL(14,4) DEFAULT NULL"),
        ("atr_declining_bars", "SMALLINT      DEFAULT NULL"),
        ("atr_streak_len",     "SMALLINT      DEFAULT NULL"),
        ("atr_streak_ago",     "SMALLINT      DEFAULT NULL"),
    ]
    conn = get_conn()
    try:
        cur = conn.cursor()
        for col, defn in new_cols:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'daily_studies' AND COLUMN_NAME = %s",
                (col,)
            )
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE daily_studies ADD COLUMN {col} {defn}")
                conn.commit()
        cur.close()
    except Exception:
        pass   # table may not exist yet — calculate_studies.py will create it
    finally:
        conn.close()


def _migrate_add_last_price():
    """Add last_price, alpaca_unrlzd_pl, alpaca_unrlzd_plpc, filled_qty columns if missing."""
    new_cols = [
        ("last_price",         "DECIMAL(14,4)  DEFAULT NULL"),
        ("alpaca_unrlzd_pl",   "DECIMAL(14,4)  DEFAULT NULL"),
        ("alpaca_unrlzd_plpc", "DECIMAL(10,6)  DEFAULT NULL"),
        ("filled_qty",         "DECIMAL(12,4)  DEFAULT NULL"),
        ("filled_avg_price",   "DECIMAL(14,4)  DEFAULT NULL"),
    ]
    conn = get_conn()
    try:
        cur = conn.cursor()
        for col, defn in new_cols:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.COLUMNS "
                "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'trades' AND COLUMN_NAME = %s",
                (col,)
            )
            if cur.fetchone()[0] == 0:
                cur.execute(f"ALTER TABLE trades ADD COLUMN {col} {defn}")
                conn.commit()
        cur.close()
    finally:
        conn.close()


@app.route("/api/alpaca/order", methods=["POST"])
def alpaca_order():
    from alpaca.trading.client import TradingClient
    from alpaca.trading.requests import LimitOrderRequest, TakeProfitRequest, StopLossRequest
    from alpaca.trading.enums import OrderSide, TimeInForce, OrderClass
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import StockLatestQuoteRequest, StockLatestTradeRequest

    data       = request.get_json(force=True)
    ticker     = (data.get("ticker") or "").upper().strip()
    stop_pct   = float(data.get("stop_pct"))    # e.g. 5.0
    target_pct = float(data.get("target_pct"))  # e.g. 15.0
    # Use integer qty — Alpaca bracket orders require whole shares for equities
    qty        = int(float(data.get("qty", 1)))

    if not ticker or qty <= 0:
        return jsonify({"error": "Invalid ticker or quantity"}), 400

    conn = get_conn()
    try:
        cur = conn.cursor(dictionary=True)
        cur.execute("SELECT `key`, `secret`, `endpoint` FROM settings WHERE id = 1")
        row = cur.fetchone()
        cur.close()
    finally:
        conn.close()

    if not row or not row.get("key"):
        return jsonify({"error": "Alpaca API credentials not configured in settings table"}), 400

    api_key    = row["key"]
    secret_key = row["secret"]
    is_paper   = "paper" in (row.get("endpoint") or "").lower()

    try:
        # ── Fetch live price (ask → last trade fallback when market is closed) ──
        data_client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)
        quote = data_client.get_stock_latest_quote(
            StockLatestQuoteRequest(symbol_or_symbols=ticker)
        )
        ask        = float(quote[ticker].ask_price or 0)
        price_src  = "ask"

        if ask <= 0:
            # Market is likely closed — fall back to the last trade price
            try:
                trade     = data_client.get_stock_latest_trade(
                    StockLatestTradeRequest(symbol_or_symbols=ticker)
                )
                ask       = float(trade[ticker].price or 0)
                price_src = "last trade (market closed)"
            except Exception:
                ask = 0

        if ask <= 0:
            return jsonify({
                "error": (
                    f"Could not retrieve a valid price for {ticker}. "
                    "The market may be closed and no last trade price is available."
                )
            }), 400

        # ── Derive TP / SL from ask price using the original percentages ──────
        entry = round(ask, 2)
        tp    = round(ask * (1 + target_pct / 100), 2)
        sl    = round(ask * (1 - stop_pct   / 100), 2)

        # ── Place bracket limit order ─────────────────────────────────────────
        trading_client = TradingClient(
            api_key=api_key,
            secret_key=secret_key,
            paper=is_paper,
        )
        order_req = LimitOrderRequest(
            symbol=ticker,
            qty=qty,
            side=OrderSide.BUY,
            time_in_force=TimeInForce.DAY,
            limit_price=entry,
            order_class=OrderClass.BRACKET,
            take_profit=TakeProfitRequest(limit_price=tp),
            stop_loss=StopLossRequest(stop_price=sl),
        )
        order = trading_client.submit_order(order_req)

        # ── Persist trade to database ─────────────────────────────────────────
        _ensure_trades_table()
        log_conn = get_conn()
        try:
            log_cur = log_conn.cursor()
            log_cur.execute(
                """
                INSERT INTO trades
                    (order_id, symbol, qty, entry_price, stop_price, target_price,
                     stop_pct, target_pct, rr_ratio, order_status, is_paper, submitted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """,
                (
                    str(order.id),
                    ticker,
                    qty,
                    entry,
                    sl,
                    tp,
                    stop_pct,
                    target_pct,
                    round(target_pct / stop_pct, 2) if stop_pct else None,
                    str(order.status),
                    is_paper,
                ),
            )
            log_conn.commit()
        finally:
            log_cur.close()
            log_conn.close()

        legs      = order.legs or []
        leg_info  = [{"id": str(l.id), "type": str(l.type), "side": str(l.side)} for l in legs]

        return jsonify({
            "success":     True,
            "order_id":    str(order.id),
            "status":      str(order.status),
            "order_class": str(order.order_class) if order.order_class else None,
            "legs":        leg_info,
            "paper":       is_paper,
            "ask":         entry,
            "price_src":   price_src,
            "tp":          tp,
            "sl":          sl,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def _ensure_trades_table():
    """Create the trades table if it does not yet exist."""
    ddl = """
        CREATE TABLE IF NOT EXISTS trades (
            id             INT            NOT NULL AUTO_INCREMENT,
            order_id       VARCHAR(64)    NOT NULL,
            symbol         VARCHAR(20)    NOT NULL,
            qty            DECIMAL(12,4)  NOT NULL,
            entry_price    DECIMAL(14,4)  NOT NULL,
            stop_price     DECIMAL(14,4)  NOT NULL,
            target_price   DECIMAL(14,4)  NOT NULL,
            stop_pct       DECIMAL(7,3)   NOT NULL,
            target_pct     DECIMAL(7,3)   NOT NULL,
            rr_ratio       DECIMAL(7,3)   DEFAULT NULL,
            order_status   VARCHAR(40)    DEFAULT NULL,
            is_paper       TINYINT(1)     NOT NULL DEFAULT 0,
            submitted_at   DATETIME       NOT NULL,
            closed_at      DATETIME       DEFAULT NULL,
            exit_price     DECIMAL(14,4)  DEFAULT NULL,
            notes          TEXT           DEFAULT NULL,
            PRIMARY KEY (id),
            UNIQUE KEY uq_order_id (order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(ddl)
        conn.commit()
        cur.close()
    finally:
        conn.close()


if __name__ == "__main__":
    _migrate_studies_columns()
    app.run(debug=True, port=5001)
