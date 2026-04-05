from datetime import datetime, time, timezone
from flask import Blueprint, jsonify, request
from auth import token_required
from extensions import db

scanner_bp = Blueprint("scanner", __name__, url_prefix="/api/scanner")


def _serialize_row(mapping):
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


@scanner_bp.route("/dates", methods=["GET"])
@token_required
def get_dates(current_user):
    """Return the last 30 distinct trading dates present in vcz."""
    rows = db.session.execute(
        db.text("SELECT DISTINCT bar_date FROM vcz ORDER BY bar_date DESC LIMIT 30")
    ).fetchall()
    result = [{"n": i + 1, "date": str(row[0])} for i, row in enumerate(rows)]
    return jsonify(result), 200


@scanner_bp.route("/scan", methods=["GET"])
@token_required
def run_scan(current_user):
    """
    Dynamic scanner over vcz ⨝ bars_daily ⨝ tickers.
    All filter params are optional; results are capped at 500 rows.

    Query params:
      days — trading-day lookback window (ignored when ticker/symbol is set)
      ticker or symbol — when set, return all cached history rows for that symbol
        (still subject to other filters); sort defaults to bar_date on the client
    """
    p = request.args

    symbol_only = (p.get("ticker") or p.get("symbol") or "").strip().upper()

    # ── Build WHERE clauses ───────────────────────────────────────────────────
    if symbol_only:
        # Single-symbol history: all bar dates for this symbol (bypasses date window)
        where = ["v.symbol = :scan_symbol"]
        args = {"scan_symbol": symbol_only}
    else:
        # ── Resolve target trading date ───────────────────────────────────────────
        days = max(1, min(int(p.get("days", 1)), 30))
        date_rows = db.session.execute(
            db.text("SELECT DISTINCT bar_date FROM vcz ORDER BY bar_date DESC LIMIT :days"),
            {"days": days},
        ).fetchall()

        if not date_rows:
            return jsonify([]), 200

        target_date = date_rows[-1][0]
        where = ["v.bar_date = :bar_date"]
        args = {"bar_date": target_date}

    _counter = [0]

    def add(clause, **kwargs):
        where.append(clause)
        for k, v in kwargs.items():
            args[k] = v

    def _key(base):
        _counter[0] += 1
        return f"{base}_{_counter[0]}"

    # Price
    if p.get("price_min"):
        k = _key("price_min")
        add(f"b.close >= :{k}", **{k: float(p["price_min"])})
    if p.get("price_max"):
        k = _key("price_max")
        add(f"b.close <= :{k}", **{k: float(p["price_max"])})

    # MA alignment
    if p.get("above_sma50")  == "1": add("b.close > v.sma_50")
    if p.get("below_sma50")  == "1": add("b.close < v.sma_50")
    if p.get("above_sma150") == "1": add("b.close > v.sma_150")
    if p.get("below_sma150") == "1": add("b.close < v.sma_150")
    if p.get("above_sma200") == "1": add("b.close > v.sma_200")
    if p.get("below_sma200") == "1": add("b.close < v.sma_200")

    # RSI
    if p.get("rsi_min"):
        k = _key("rsi_min")
        add(f"v.rsi_14 >= :{k}", **{k: float(p["rsi_min"])})
    if p.get("rsi_max"):
        k = _key("rsi_max")
        add(f"v.rsi_14 <= :{k}", **{k: float(p["rsi_max"])})

    rsi_state = []
    if p.get("rsi_cross50")      == "1": rsi_state.append("v.rsi_cross_50 = 1")
    if p.get("rsi_cross_down50") == "1": rsi_state.append("v.rsi_cross_down_50 = 1")
    if p.get("rsi_above50")      == "1": rsi_state.append("v.rsi_14 > 50")
    if p.get("rsi_below50")      == "1": rsi_state.append("v.rsi_14 < 50")
    if rsi_state:
        add("(" + " OR ".join(rsi_state) + ")")

    # Volume label (comma-separated)
    vlabels = [v.strip() for v in p.get("vol_labels", "").split(",") if v.strip()]
    if vlabels:
        ph = ",".join([f":vl{i}" for i in range(len(vlabels))])
        for i, lbl in enumerate(vlabels):
            args[f"vl{i}"] = lbl
        add(f"v.vol_label IN ({ph})")

    # Volume vs MA ratio
    if p.get("vol_ratio_min"):
        k = _key("vol_ratio_min")
        add(f"b.volume / NULLIF(v.vol_ma, 0) >= :{k}", **{k: float(p["vol_ratio_min"])})

    # ATR compression
    if p.get("atr_squeeze") == "1":
        add("v.atr_squeeze = 1")
    if p.get("atr_decl_bars_min"):
        k = _key("atr_decl_bars_min")
        add(f"v.atr_declining_bars >= :{k}", **{k: int(p["atr_decl_bars_min"])})
    if p.get("atr_squeeze_break") == "1":
        add("v.atr_squeeze = 0")
        add("""1 = (
            SELECT prev.atr_squeeze
            FROM vcz prev
            WHERE prev.symbol  = v.symbol
              AND prev.bar_date < v.bar_date
            ORDER BY prev.bar_date DESC
            LIMIT 1
        )""")

    # 52-week range
    if p.get("pct_from_high_max"):
        k = _key("pct_from_high_max")
        add(f"(v.dc_upper - b.close) / NULLIF(v.dc_upper, 0) * 100 <= :{k}",
            **{k: float(p["pct_from_high_max"])})
    if p.get("pct_from_low_min"):
        k = _key("pct_from_low_min")
        add(f"(b.close - v.dc_lower) / NULLIF(v.dc_lower, 0) * 100 >= :{k}",
            **{k: float(p["pct_from_low_min"])})

    # RS Line vs SPY SMA 50
    if p.get("rs_above_sma50") == "1": add("v.rs_line > v.rs_sma_50")
    if p.get("rs_below_sma50") == "1": add("v.rs_line < v.rs_sma_50")

    # RMV 15
    if p.get("rmv_15_max"):
        k = _key("rmv_15_max")
        add(f"v.rmv_15 <= :{k}", **{k: float(p["rmv_15_max"])})
    if p.get("rmv_15_compressed") == "1":
        add("v.rmv_15 < 20")

    # Accumulation Score 1M
    if p.get("as_1m_min"):
        k = _key("as_1m_min")
        add(f"v.as_1m >= :{k}", **{k: int(p["as_1m_min"])})
    if p.get("as_1m_max"):
        k = _key("as_1m_max")
        add(f"v.as_1m <= :{k}", **{k: int(p["as_1m_max"])})

    # ── Sorting ───────────────────────────────────────────────────────────────
    valid_sorts = {
        "ticker":        "v.symbol",
        "bar_date":      "v.bar_date",
        "close":         "b.close",
        "volume":        "b.volume",
        "vol_ratio":     "b.volume / NULLIF(v.vol_ma, 0)",
        "rsi_14":        "v.rsi_14",
        "pct_from_high": "(v.dc_upper - b.close) / NULLIF(v.dc_upper, 0) * 100",
        "pct_from_low":  "(b.close - v.dc_lower)  / NULLIF(v.dc_lower, 0)  * 100",
        "sma50_dist":    "(b.close - v.sma_50)   / NULLIF(v.sma_50, 0)   * 100",
        "sma200_dist":   "(b.close - v.sma_200)  / NULLIF(v.sma_200, 0)  * 100",
        "as_1m":         "v.as_1m",
        "rmv_15":        "v.rmv_15",
    }
    sort_col = valid_sorts.get(p.get("sort_by", "ticker"), "v.symbol")
    sort_dir = "ASC" if p.get("sort_dir", "asc").upper() == "ASC" else "DESC"

    sql = f"""
        SELECT
            v.symbol                                                                AS ticker,
            t.name,
            t.sector,
            v.bar_date,
            ROUND(b.close,   2)                                                     AS close,
            b.volume,
            ROUND(v.vol_ma,  0)                                                     AS vol_ma,
            ROUND(b.volume / NULLIF(v.vol_ma, 0), 2)                               AS vol_ratio,
            v.vol_label,
            ROUND(v.rsi_14,  1)                                                     AS rsi_14,
            v.rsi_cross_50,
            ROUND(v.sma_50,  2)                                                     AS sma_50,
            ROUND(v.sma_150, 2)                                                     AS sma_150,
            ROUND(v.sma_200, 2)                                                     AS sma_200,
            ROUND(v.ema_10,  2)                                                     AS ema_10,
            ROUND(v.ema_20,  2)                                                     AS ema_20,
            ROUND(v.dc_upper, 2)                                                    AS dc_upper,
            ROUND(v.dc_lower, 2)                                                    AS dc_lower,
            ROUND((v.dc_upper - b.close) / NULLIF(v.dc_upper, 0) * 100, 1)         AS pct_from_high,
            ROUND((b.close - v.dc_lower) / NULLIF(v.dc_lower, 0) * 100, 1)         AS pct_from_low,
            ROUND((b.close - v.sma_50)   / NULLIF(v.sma_50,   0) * 100, 1)         AS sma50_dist,
            ROUND((b.close - v.sma_200)  / NULLIF(v.sma_200,  0) * 100, 1)         AS sma200_dist,
            v.atr_declining_bars,
            v.atr_streak_len,
            v.atr_streak_ago,
            v.atr_squeeze,
            v.rsi_cross_down_50,
            ROUND(v.rmv_15, 4)                                                      AS rmv_15,
            v.as_1m,
            v.rs_line,
            v.rs_sma_50,
            (
                SELECT GROUP_CONCAT(x.vol_label ORDER BY x.bar_date ASC SEPARATOR ',')
                FROM (
                    SELECT vol_label, bar_date
                    FROM vcz v2
                    WHERE v2.symbol = v.symbol AND v2.bar_date <= v.bar_date
                    ORDER BY v2.bar_date DESC
                    LIMIT 5
                ) x
            ) AS vol_history
        FROM vcz v
        INNER JOIN bars_daily b ON b.ticker = v.symbol COLLATE utf8mb4_unicode_ci AND DATE(b.bar_time) = v.bar_date
        INNER JOIN tickers    t ON t.ticker = v.symbol COLLATE utf8mb4_unicode_ci
        WHERE {' AND '.join(where)}
        ORDER BY {sort_col} {sort_dir}
        LIMIT 500
    """

    try:
        rows = db.session.execute(db.text(sql), args).fetchall()
        results = [_serialize_row(row._mapping) for row in rows]
        return jsonify(results), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Chart endpoint ────────────────────────────────────────────────────────────

_LABEL_COLORS = {
    "XHigh": "#ff0000",
    "High":  "#ff7800",
    "Med":   "#ffcf03",
    "Norm":  "#a0d6dc",
    "Low":   "#1f9cac",
}


def _ts(bar_date):
    """Convert a date object to UTC midnight timestamp in milliseconds."""
    return int(
        datetime.combine(bar_date, time())
        .replace(tzinfo=timezone.utc)
        .timestamp() * 1000
    )


@scanner_bp.route("/chart/<ticker>", methods=["GET"])
@token_required
def get_chart(current_user, ticker):
    """
    Return OHLCV bars + pre-computed studies + ticker info for the given symbol.
    Mirrors the Leo /api/chart/<ticker> JSON shape using the tradefinder schema
    (bars_daily + vcz + tickers instead of daily_ohlcv + daily_studies + tickers).
    """
    try:
        return _get_chart_inner(ticker.upper().strip())
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500


def _get_chart_inner(ticker):

    # ── Ticker info ── select only guaranteed polygon columns; try yahoo extras ──
    # Polygon columns always present
    BASE_INFO = """
        SELECT ticker, name, sector, industry, type, market, primary_exchange,
               last_day_close, last_day_open, last_day_high, last_day_low,
               last_day_volume, last_day_date, last_day_vwap
        FROM tickers WHERE ticker = :t
    """
    # Yahoo optional extras (added by yahoo_fundamentals_to_mysql.py if it has been run)
    FULL_INFO = """
        SELECT ticker, name, sector, industry, type, market, primary_exchange,
               last_day_close, last_day_open, last_day_high, last_day_low,
               last_day_volume, last_day_date, last_day_vwap,
               market_cap, trailing_pe, average_volume,
               held_percent_institutions, held_percent_insiders,
               recommendation_mean
        FROM tickers WHERE ticker = :t
    """
    try:
        info_row = db.session.execute(db.text(FULL_INFO), {"t": ticker}).mappings().fetchone()
    except Exception:
        db.session.rollback()
        info_row = db.session.execute(db.text(BASE_INFO), {"t": ticker}).mappings().fetchone()
    info = dict(info_row) if info_row else None

    # ── Two fast indexed queries; merge in Python by date ────────────────────
    # bars_daily PK is (ticker, bar_time) — fast scan by ticker
    bar_rows = db.session.execute(db.text("""
        SELECT DATE(bar_time) AS bar_date,
               open, high, low, close, volume
        FROM bars_daily
        WHERE ticker = :t
        ORDER BY bar_time ASC
    """), {"t": ticker}).mappings().fetchall()

    # vcz PK is (symbol, bar_date) — fast scan by symbol
    vcz_rows = db.session.execute(db.text("""
        SELECT bar_date,
               sma_50, sma_150, sma_200,
               ema_10, ema_20,
               rsi_14, rsi_cross_50, rsi_cross_down_50,
               vol_ma, vol_label,
               dc_upper, dc_lower,
               atr_10, atr_squeeze,
               pivot_high, atr_declining_bars,
               rs_line, rs_ema_21, rs_sma_50, rs_new_high,
               rmv_15, as_1m
        FROM vcz
        WHERE symbol = :t
        ORDER BY bar_date ASC
    """), {"t": ticker}).mappings().fetchall()

    # Index vcz rows by bar_date for O(1) lookup
    vcz_by_date = {r["bar_date"]: r for r in vcz_rows}

    def _flt(row, col):
        val = row.get(col) if row else None
        return float(val) if val is not None else None

    ohlcv = []; volume = []
    sma50_s = []; sma150_s = []; sma200_s = []
    ema10_s = []; ema20_s  = []; rsi14_s  = []
    rsi_cup_ts = []; rsi_cdn_ts = []
    vol_bars_s = []; vol_ma_s   = []
    dc_upper_s = []; dc_lower_s = []
    atr10_s    = []; atr_sq_s   = []; atr_decl_s = []
    pivot_chg  = []
    rs_line_s  = []; rs_ema21_s = []; rs_sma50_s = []; rs_new_hi_ts = []
    rmv15_s    = []; as1m_s     = []
    has_studies = False
    prev_pivot = prev_rsi = last_as1m = None

    for b in bar_rows:
        bar_date = b["bar_date"]
        ts = _ts(bar_date)
        v  = vcz_by_date.get(bar_date)   # may be None for older bars

        o_  = float(b["open"])   if b["open"]   is not None else None
        h_  = float(b["high"])   if b["high"]   is not None else None
        lo_ = float(b["low"])    if b["low"]    is not None else None
        c_  = float(b["close"])  if b["close"]  is not None else None
        vol = int(b["volume"])   if b["volume"] is not None else 0

        ohlcv.append([ts, o_, h_, lo_, c_])
        volume.append([ts, vol])

        if v is None:
            vol_bars_s.append({"x": ts, "y": vol, "color": "#a0d6dc"})
            continue

        sma50_v = _flt(v, "sma_50");  sma150_v = _flt(v, "sma_150"); sma200_v = _flt(v, "sma_200")
        ema10_v = _flt(v, "ema_10");  ema20_v  = _flt(v, "ema_20");  rsi_v    = _flt(v, "rsi_14")
        atr_v   = _flt(v, "atr_10"); dcu_v    = _flt(v, "dc_upper"); dcl_v    = _flt(v, "dc_lower")
        ph_v    = _flt(v, "pivot_high"); vol_ma_v = _flt(v, "vol_ma")
        decl_v  = v["atr_declining_bars"]
        vol_lbl = v["vol_label"] or "Norm"

        if sma50_v is not None:
            has_studies = True
            sma50_s.append([ts, sma50_v])
        if sma150_v is not None: sma150_s.append([ts, sma150_v])
        if sma200_v is not None: sma200_s.append([ts, sma200_v])
        if ema10_v  is not None: ema10_s.append([ts,  ema10_v])
        if ema20_v  is not None: ema20_s.append([ts,  ema20_v])
        if rsi_v    is not None: rsi14_s.append([ts,  rsi_v])

        if v["rsi_cross_50"] == 1:
            rsi_cup_ts.append(ts)
        if rsi_v is not None and prev_rsi is not None:
            if prev_rsi > 50 and rsi_v <= 50:
                rsi_cdn_ts.append(ts)
        if rsi_v is not None:
            prev_rsi = rsi_v

        color = _LABEL_COLORS.get(vol_lbl, "#a0d6dc")
        vol_bars_s.append({"x": ts, "y": vol, "color": color})
        if vol_ma_v is not None: vol_ma_s.append([ts, vol_ma_v])

        if dcu_v is not None: dc_upper_s.append([ts, dcu_v])
        if dcl_v is not None: dc_lower_s.append([ts, dcl_v])

        if atr_v is not None:
            atr10_s.append([ts, atr_v])
            if v["atr_squeeze"] == 1:
                atr_sq_s.append([ts, atr_v])
        if decl_v is not None:
            atr_decl_s.append([ts, int(decl_v)])

        if ph_v is not None and ph_v != prev_pivot:
            pivot_chg.append({"ts": ts, "high": ph_v})
            prev_pivot = ph_v

        rs_v   = _flt(v, "rs_line");  rs_e_v = _flt(v, "rs_ema_21"); rs_s_v = _flt(v, "rs_sma_50")
        if rs_v   is not None: rs_line_s.append([ts,  rs_v])
        if rs_e_v is not None: rs_ema21_s.append([ts, rs_e_v])
        if rs_s_v is not None: rs_sma50_s.append([ts, rs_s_v])
        if v["rs_new_high"] == 1 and rs_v is not None:
            rs_new_hi_ts.append(ts)

        rmv15_v = _flt(v, "rmv_15")
        if rmv15_v is not None: rmv15_s.append([ts, rmv15_v])

        if v["as_1m"] is not None:
            last_as1m = int(v["as_1m"])
            as1m_s.append([ts, last_as1m])

    pivot_chg = pivot_chg[-5:]

    studies = {
        "sma50": sma50_s, "sma150": sma150_s, "sma200": sma200_s,
        "ema10": ema10_s, "ema20":  ema20_s,
        "rsi14": rsi14_s, "rsiCrossUpTs": rsi_cup_ts, "rsiCrossDownTs": rsi_cdn_ts,
        "volBars": vol_bars_s, "volMa": vol_ma_s,
        "dcUpper": dc_upper_s, "dcLower": dc_lower_s,
        "atr10": atr10_s, "atrSqueeze": atr_sq_s, "atrDeclining": atr_decl_s,
        "pivotHighs": pivot_chg,
        "rsLine": rs_line_s, "rsEma21": rs_ema21_s, "rsSma50": rs_sma50_s,
        "rsNewHighTs": rs_new_hi_ts,
        "rmv15": rmv15_s, "as1m": as1m_s,
    } if has_studies else None

    if info is not None and last_as1m is not None:
        info["as_1m"] = last_as1m

    return jsonify({
        "ohlcv":   ohlcv,
        "volume":  volume,
        "studies": studies,
        "info":    _serialize_row(info) if info else None,
    }), 200


# ── Chart box routes ─────────────────────────────────────────────────────────

@scanner_bp.route("/boxes", methods=["GET"])
@token_required
def list_boxes(current_user):
    from models import ChartBox
    ticker = request.args.get("ticker", "").upper().strip()
    if not ticker:
        return jsonify({"error": "ticker required"}), 400
    boxes = ChartBox.query.filter_by(user_id=current_user.id, ticker=ticker).all()
    return jsonify([{
        "id": b.id, "ticker": b.ticker,
        "x1": b.x1, "x2": b.x2,
        "y1": float(b.y1) if b.y1 is not None else None,
        "y2": float(b.y2) if b.y2 is not None else None,
        "color": b.color,
    } for b in boxes])


@scanner_bp.route("/boxes", methods=["POST"])
@token_required
def create_box(current_user):
    from models import ChartBox
    data = request.get_json() or {}
    try:
        box = ChartBox(
            user_id=current_user.id,
            ticker=(data.get("ticker") or "").upper().strip(),
            x1=int(data["x1"]), x2=int(data["x2"]),
            y1=data["y1"],      y2=data["y2"],
            color=data.get("color", "#ffd700"),
        )
        db.session.add(box)
        db.session.commit()
        return jsonify({
            "id": box.id, "ticker": box.ticker,
            "x1": box.x1,      "x2": box.x2,
            "y1": float(box.y1), "y2": float(box.y2),
            "color": box.color,
        }), 201
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 400


@scanner_bp.route("/boxes/<int:box_id>", methods=["DELETE"])
@token_required
def delete_box(current_user, box_id):
    from models import ChartBox
    box = ChartBox.query.filter_by(id=box_id, user_id=current_user.id).first()
    if not box:
        return jsonify({"error": "not found"}), 404
    db.session.delete(box)
    db.session.commit()
    return jsonify({"ok": True})
