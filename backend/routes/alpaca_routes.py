"""
Alpaca broker settings — per-user storage and retrieval.

Credentials are stored in the `broker_credentials` table, scoped to the
authenticated user. No credentials are shared between accounts.

Keys stored per user:
  api_key     — Alpaca API Key ID       (sensitive, masked in responses)
  api_secret  — Alpaca API Secret Key   (sensitive, masked in responses)
  paper_mode  — "true" | "false"

Endpoints:
  GET  /api/broker/alpaca                  → return current user's settings (secrets masked)
  PUT  /api/broker/alpaca                  → save one or more settings
  GET  /api/broker/alpaca/test             → verify credentials against Alpaca /v2/account
  GET  /api/broker/alpaca/quote/<ticker>   → live bid/ask + last trade from Alpaca market data
  POST /api/broker/alpaca/order            → place a bracket order; persists result to orders table
  GET  /api/broker/alpaca/orders           → list all orders for the current user

Market-data note: Alpaca market data always uses https://data.alpaca.markets regardless
of whether the account is paper or live.
"""
import json
from flask import Blueprint, request, jsonify
import requests as http_req
from auth import token_required
from models import BrokerCredential, Order
from extensions import db
from alpaca_close import perform_market_close_for_order

alpaca_bp = Blueprint("alpaca", __name__, url_prefix="/api/broker/alpaca")

BROKER = "alpaca"


def _mask(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "****"
    return value[:4] + "****" + value[-4:]


@alpaca_bp.route("", methods=["GET"])
@token_required
def get_alpaca(current_user):
    creds = BrokerCredential.get_all(current_user.id, BROKER)

    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true")

    return jsonify({
        "api_key": {
            "configured": bool(api_key),
            "masked":     _mask(api_key),
        },
        "api_secret": {
            "configured": bool(api_secret),
            "masked":     _mask(api_secret),
        },
        "paper_mode": paper_mode,
    }), 200


@alpaca_bp.route("/test", methods=["GET"])
@token_required
def test_alpaca(current_user):
    """
    Retrieve the current user's stored Alpaca credentials and call
    GET /v2/account against the Alpaca API to verify they are valid.
    The raw keys are never sent to the browser.
    """
    creds      = BrokerCredential.get_all(current_user.id, BROKER)
    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true") == "true"

    if not api_key or not api_secret:
        return jsonify({"ok": False, "error": "Credentials not configured."}), 400

    base = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    try:
        resp = http_req.get(
            f"{base}/v2/account",
            headers={
                "APCA-API-KEY-ID":     api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            d = resp.json()
            return jsonify({
                "ok":              True,
                "paper":           paper_mode,
                "account_id":      d.get("id"),
                "status":          d.get("status"),
                "buying_power":    d.get("buying_power"),
                "portfolio_value": d.get("portfolio_value"),
                "currency":        d.get("currency", "USD"),
            }), 200
        else:
            msg = resp.json().get("message") or resp.text or "Authentication failed."
            return jsonify({"ok": False, "error": msg}), 200
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 200


@alpaca_bp.route("", methods=["PUT"])
@token_required
def save_alpaca(current_user):
    data    = request.get_json(silent=True) or {}
    updated = []

    if "api_key" in data:
        val = str(data["api_key"]).strip()
        if val:
            BrokerCredential.set(current_user.id, BROKER, "api_key", val)
            updated.append("api_key")

    if "api_secret" in data:
        val = str(data["api_secret"]).strip()
        if val:
            BrokerCredential.set(current_user.id, BROKER, "api_secret", val)
            updated.append("api_secret")

    if "paper_mode" in data:
        raw   = data["paper_mode"]
        value = "true" if raw in (True, "true", "True", 1, "1") else "false"
        BrokerCredential.set(current_user.id, BROKER, "paper_mode", value)
        updated.append("paper_mode")

    return jsonify({
        "message": "Alpaca settings saved.",
        "updated": updated,
    }), 200


ALPACA_DATA_BASE = "https://data.alpaca.markets"


@alpaca_bp.route("/quote/<string:ticker>", methods=["GET"])
@token_required
def live_quote(current_user, ticker: str):
    """
    Fetch the latest bid/ask quote and last trade price for a ticker using the
    logged-in user's stored Alpaca credentials.

    Alpaca market data (data.alpaca.markets) is the same endpoint for both
    paper and live accounts — only the trading API differs.

    Primary source:  GET /v2/stocks/{ticker}/quotes/latest
      quote.bp = bid price
      quote.ap = ask price
      quote.bs = bid size
      quote.as = ask size

    Fallback source: GET /v2/stocks/{ticker}/trades/latest
      trade.p  = last trade price
      trade.s  = last trade size

    Returns { bid, ask, bid_size, ask_size, spread, last, last_size, timestamp }
    """
    ticker = ticker.upper()

    creds      = BrokerCredential.get_all(current_user.id, BROKER)
    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")

    if not api_key or not api_secret:
        return jsonify({"error": "Alpaca credentials not configured."}), 400

    headers = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }

    bid = ask = bid_size = ask_size = spread = last = last_size = timestamp = None

    # ── Latest quote (bid / ask) ──────────────────────────────────────────────
    try:
        q_resp = http_req.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/{ticker}/quotes/latest",
            headers=headers,
            timeout=8,
        )
        if q_resp.status_code == 200:
            q = q_resp.json().get("quote", {})
            bid       = q.get("bp")
            ask       = q.get("ap")
            bid_size  = q.get("bs")
            ask_size  = q.get("as")
            timestamp = q.get("t")
            if bid is not None and ask is not None:
                spread = round(ask - bid, 4)
    except Exception:
        pass

    # ── Latest trade (last price fallback) ───────────────────────────────────
    try:
        t_resp = http_req.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/{ticker}/trades/latest",
            headers=headers,
            timeout=8,
        )
        if t_resp.status_code == 200:
            tr = t_resp.json().get("trade", {})
            last      = tr.get("p")
            last_size = tr.get("s")
            if timestamp is None:
                timestamp = tr.get("t")
    except Exception:
        pass

    if bid is None and ask is None and last is None:
        return jsonify({"error": "No quote data returned from Alpaca."}), 502

    return jsonify({
        "ticker":    ticker,
        "bid":       bid,
        "ask":       ask,
        "bid_size":  bid_size,
        "ask_size":  ask_size,
        "spread":    spread,
        "last":      last,
        "last_size": last_size,
        "timestamp": timestamp,
    }), 200


@alpaca_bp.route("/order", methods=["POST"])
@token_required
def place_order(current_user):
    """
    Place a bracket order on Alpaca using the current user's stored credentials.

    Expected JSON body
    ------------------
    {
      "ticker":       "AAPL",
      "direction":    "long",          // "long" | "short"
      "order_type":   "limit",         // "limit" | "market"
      "qty":          100,
      "entry_price":  150.25,          // required for limit orders
      "stop_price":   147.00,
      "target_price": 156.50,

      // Chart-reconstruction metadata (optional but stored when provided)
      "bias":         "long",
      "bar_time":     "2026-03-17 09:35:00 ET",
      "threshold":    149.80,
      "rr_ratio":     2.14,
      "risk_amt":     325.00,
      "reward_amt":   625.00
    }

    Alpaca bracket-order specifics
    --------------------------------
    - Entry (parent) leg: time_in_force = "day"  → expires unfilled at market close
    - Stop-loss / take-profit legs: patched to "gtc" immediately after bracket creation
      Alpaca does not support mixed TIF in a single bracket payload — legs inherit the
      parent's TIF. The workaround is to PATCH each leg's TIF to "gtc" right after
      the bracket is created via PATCH /v2/orders/{leg_id}.
    - For short entries the side is "sell"; stop and target flip accordingly.
    """
    data = request.get_json(silent=True) or {}

    required = ["ticker", "direction", "order_type", "qty", "stop_price", "target_price"]
    missing  = [f for f in required if f not in data or data[f] is None]
    if missing:
        return jsonify({"error": f"Missing required fields: {', '.join(missing)}"}), 400

    if data["order_type"] == "limit" and not data.get("entry_price"):
        return jsonify({"error": "entry_price is required for limit orders."}), 400

    creds      = BrokerCredential.get_all(current_user.id, BROKER)
    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true") == "true"

    if not api_key or not api_secret:
        return jsonify({"error": "Alpaca credentials are not configured for this account."}), 400

    base    = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    headers = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Content-Type":        "application/json",
    }

    direction    = data["direction"]
    order_type   = data["order_type"]
    # entry_tif: "day"  → entry expires at market close; legs patched to GTC after
    #            "gtc"  → entire bracket is GTC; no patching needed
    entry_tif    = data.get("entry_tif", "gtc")
    if entry_tif not in ("day", "gtc"):
        entry_tif = "gtc"
    side         = "buy" if direction == "long" else "sell"
    qty          = int(data["qty"])
    entry_price  = float(data["entry_price"])  if data.get("entry_price")  else None
    stop_price   = float(data["stop_price"])
    target_price = float(data["target_price"])

    alpaca_payload = {
        "symbol":        data["ticker"].upper(),
        "qty":           str(qty),
        "side":          side,
        "type":          order_type,
        "time_in_force": entry_tif,
        "order_class":   "bracket",
        "take_profit": {
            "limit_price": str(round(target_price, 2)),
        },
        "stop_loss": {
            "stop_price": str(round(stop_price, 2)),
        },
    }

    if order_type == "limit" and entry_price is not None:
        alpaca_payload["limit_price"] = str(round(entry_price, 2))

    try:
        resp = http_req.post(
            f"{base}/v2/orders",
            headers=headers,
            json=alpaca_payload,
            timeout=15,
        )
        resp_json = resp.json()
    except Exception as exc:
        return jsonify({"error": f"Network error contacting Alpaca: {exc}"}), 502

    if resp.status_code not in (200, 201):
        msg = resp_json.get("message") or resp_json.get("error") or resp.text
        return jsonify({"error": msg, "alpaca_status": resp.status_code}), 400

    # ── Patch each bracket leg to GTC (only needed when entry is DAY) ───────────
    # When entry_tif = "day", Alpaca sets all legs to "day" too. We immediately
    # PATCH each leg to "gtc" so the stop and take-profit survive past today's
    # close once the entry fills.
    # When entry_tif = "gtc", the whole bracket is already GTC — no patch needed.
    leg_patch_errors = []
    if entry_tif == "day":
        for leg in resp_json.get("legs", []):
            leg_id = leg.get("id")
            if not leg_id:
                continue
            try:
                patch_resp = http_req.patch(
                    f"{base}/v2/orders/{leg_id}",
                    headers=headers,
                    json={"time_in_force": "gtc"},
                    timeout=10,
                )
                if patch_resp.status_code not in (200, 201):
                    leg_patch_errors.append({
                        "leg_id": leg_id,
                        "status": patch_resp.status_code,
                        "error":  patch_resp.json().get("message", patch_resp.text),
                    })
            except Exception as exc:
                leg_patch_errors.append({"leg_id": leg_id, "error": str(exc)})

    # Persist the order so the user can reconstruct chart levels later
    risk   = abs(entry_price - stop_price)   if entry_price else None
    reward = abs(target_price - entry_price) if entry_price else None

    entry_time_raw = data.get("entry_time")
    record = Order(
        user_id          = current_user.id,
        ticker           = data["ticker"].upper(),
        bias             = data.get("bias"),
        direction        = direction,
        bar_time         = data.get("bar_time"),
        threshold        = float(data["threshold"])  if data.get("threshold")  is not None else None,
        entry_time       = int(entry_time_raw) if entry_time_raw is not None else None,
        order_type       = order_type,
        qty              = qty,
        entry_price      = entry_price,
        stop_price       = stop_price,
        target_price     = target_price,
        rr_ratio          = float(data["rr_ratio"])           if data.get("rr_ratio")           is not None else None,
        rr_ratio_effective = float(data["rr_ratio_effective"]) if data.get("rr_ratio_effective") is not None else (round(reward / risk, 4) if risk and risk > 0 else None),
        risk_amt         = float(data["risk_amt"])   if data.get("risk_amt")   is not None else (round(risk * qty, 4)    if risk   is not None else None),
        reward_amt       = float(data["reward_amt"]) if data.get("reward_amt") is not None else (round(reward * qty, 4)  if reward is not None else None),
        alpaca_order_id  = resp_json.get("id"),
        alpaca_client_id = resp_json.get("client_order_id"),
        status           = resp_json.get("status"),
        paper_mode       = paper_mode,
        raw_response     = json.dumps(resp_json),
    )
    db.session.add(record)
    db.session.commit()

    return jsonify({
        "message":          "Bracket order placed successfully.",
        "order":            record.to_dict(),
        "alpaca_raw":       resp_json,
        "leg_patch_errors": leg_patch_errors or None,
    }), 201


@alpaca_bp.route("/order/<string:alpaca_order_id>", methods=["GET"])
@token_required
def get_order_detail(current_user, alpaca_order_id: str):
    """
    Fetch a single order (including its bracket legs) directly from Alpaca.
    The nested_feed=true param tells Alpaca to include leg orders in the response.
    """
    creds      = BrokerCredential.get_all(current_user.id, BROKER)
    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true") == "true"

    if not api_key or not api_secret:
        return jsonify({"error": "Alpaca credentials not configured."}), 400

    base    = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    headers = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }

    try:
        resp = http_req.get(
            f"{base}/v2/orders/{alpaca_order_id}",
            headers=headers,
            params={"nested": "true"},
            timeout=10,
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 502

    if resp.status_code == 404:
        return jsonify({"error": "Order not found on Alpaca."}), 404
    if resp.status_code != 200:
        msg = resp.json().get("message") or resp.text
        return jsonify({"error": msg}), resp.status_code

    return jsonify(resp.json()), 200


@alpaca_bp.route("/order/<int:db_order_id>/close", methods=["POST"])
@token_required
def close_trade(current_user, db_order_id: int):
    """
    Close an open bracket trade in two steps:
      1. Cancel all open child orders (stop-loss and take-profit legs) by
         fetching the parent order's nested legs from Alpaca and cancelling each.
      2. Submit a market order in the opposite direction (flatten position).

    The DB order record is updated to status="canceled" after the close.
    """
    order = Order.query.filter_by(id=db_order_id, user_id=current_user.id).first()
    if not order:
        return jsonify({"error": "Order not found."}), 404
    if not order.alpaca_order_id:
        return jsonify({"error": "No Alpaca order ID stored for this trade."}), 400

    errors = perform_market_close_for_order(current_user.id, order)

    if errors:
        return jsonify({"status": "partial", "errors": errors}), 207

    return jsonify({"status": "closed", "ticker": order.ticker.upper()}), 200


@alpaca_bp.route("/orders", methods=["GET"])
@token_required
def list_orders(current_user):
    """
    Return all persisted orders for the logged-in user, newest first.
    Optional query param: ?ticker=AAPL to filter by symbol.
    """
    ticker_filter = request.args.get("ticker", "").strip().upper()

    query = Order.query.filter_by(user_id=current_user.id)
    if ticker_filter:
        query = query.filter_by(ticker=ticker_filter)

    orders = query.order_by(Order.created_at.desc()).all()
    return jsonify([o.to_dict() for o in orders]), 200


# Statuses that are permanently closed — Alpaca will never update these again.
_CLOSED_STATUSES = {"canceled", "expired", "rejected", "done_for_day"}


@alpaca_bp.route("/orders/open-tickers", methods=["GET"])
@token_required
def open_tickers(current_user):
    """
    Return the set of ticker symbols the user currently has open positions in.
    Reads the local DB (status not in closed set) — no Alpaca round-trip.
    """
    rows = (
        Order.query
        .filter(
            Order.user_id == current_user.id,
            Order.is_open == True,  # noqa: E712  — SQLAlchemy requires == not 'is'
        )
        .with_entities(Order.ticker)
        .distinct()
        .all()
    )
    return jsonify({"tickers": [r.ticker for r in rows if r.ticker]}), 200


@alpaca_bp.route("/orders/sync", methods=["POST"])
@token_required
def sync_orders(current_user):
    """
    Refresh order statuses and open P/L from Alpaca. Alpaca is authoritative.

    Pass 1 — GET /v2/orders?status=open  (Alpaca authoritative open orders)
      Build a set of Alpaca order IDs that are currently pending/active.

    Pass 2 — GET /v2/positions           (Alpaca authoritative open positions)
      Build a symbol → position map with live P/L already calculated.
      Tickers present here represent filled entries still holding a position.

    Pass 3 — GET /v2/orders/{id} for DB orders needing a status/fill refresh
      Only re-fetches orders not yet permanently closed and missing fill data.

    Pass 4 — Write P/L from positions into filled DB orders.

    is_open flag
      Each returned order is tagged is_open=True when Alpaca confirms it is
      either a currently pending order OR an open position. The frontend
      displays only is_open=True records.

    Returns the filtered open order list (newest first).
    """
    from datetime import datetime as dt
    creds      = BrokerCredential.get_all(current_user.id, BROKER)
    api_key    = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true") == "true"

    has_creds = bool(api_key and api_secret)
    base      = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    headers   = {
        "APCA-API-KEY-ID":     api_key,
        "APCA-API-SECRET-KEY": api_secret,
    }

    all_orders = (
        Order.query
        .filter_by(user_id=current_user.id)
        .order_by(Order.created_at.desc())
        .all()
    )

    updated_count    = 0
    errors           = []
    open_order_ids   = set()   # Alpaca order IDs that are currently pending
    open_tickers     = set()   # tickers with an open position (filled + holding)
    position_map     = {}

    if has_creds:
        # ── Pass 1: currently open (pending) orders from Alpaca ───────────────
        try:
            open_resp = http_req.get(
                f"{base}/v2/orders",
                headers=headers,
                params={"status": "open", "limit": 500},
                timeout=10,
            )
            if open_resp.status_code == 200:
                for o in open_resp.json():
                    oid = o.get("id")
                    if oid:
                        open_order_ids.add(oid)
                        # Also capture any leg IDs
                        for leg in o.get("legs") or []:
                            if leg.get("id"):
                                open_order_ids.add(leg["id"])
        except Exception as exc:
            errors.append({"pass": 1, "error": str(exc)})

        # ── Pass 2: open positions (filled entries still holding) ─────────────
        try:
            pos_resp = http_req.get(
                f"{base}/v2/positions",
                headers=headers,
                timeout=10,
            )
            if pos_resp.status_code == 200:
                for pos in pos_resp.json():
                    sym = pos.get("symbol", "").upper()
                    if not sym:
                        continue
                    open_tickers.add(sym)
                    position_map[sym] = {
                        "unrealized_pl":   _safe_float(pos.get("unrealized_pl")),
                        "current_price":   _safe_float(pos.get("current_price")),
                        "avg_entry_price": _safe_float(pos.get("avg_entry_price")),
                    }
        except Exception as exc:
            errors.append({"pass": 2, "error": str(exc)})

        # ── Pass 3: refresh status + fill price for DB orders that need it ────
        for order in all_orders:
            if not order.alpaca_order_id:
                continue
            if order.status in _CLOSED_STATUSES:
                continue
            if order.status == "filled" and order.filled_avg_price is not None:
                continue

            try:
                resp = http_req.get(
                    f"{base}/v2/orders/{order.alpaca_order_id}",
                    headers=headers,
                    timeout=8,
                )
                if resp.status_code == 200:
                    data               = resp.json()
                    order.status       = data.get("status", order.status)
                    order.raw_response = json.dumps(data)
                    raw_fill           = data.get("filled_avg_price")
                    if raw_fill is not None:
                        order.filled_avg_price = _safe_float(raw_fill)
                    updated_count += 1
                elif resp.status_code == 404:
                    order.status  = "canceled"
                    updated_count += 1
                else:
                    errors.append({
                        "order_id":        order.id,
                        "alpaca_order_id": order.alpaca_order_id,
                        "http_status":     resp.status_code,
                    })
            except Exception as exc:
                errors.append({
                    "order_id":        order.id,
                    "alpaca_order_id": order.alpaca_order_id,
                    "error":           str(exc),
                })

        # ── Pass 4: write P/L from positions into filled DB orders ───────────
        now = dt.utcnow()
        for order in all_orders:
            if order.status != "filled":
                continue
            pos = position_map.get(order.ticker)
            if not pos:
                continue
            if pos["unrealized_pl"] is not None:
                order.unrealized_pl = round(pos["unrealized_pl"], 4)
            if pos["current_price"] is not None:
                order.current_price = pos["current_price"]
            if pos["avg_entry_price"] is not None and order.filled_avg_price is None:
                order.filled_avg_price = pos["avg_entry_price"]
            order.synced_at = now

        db.session.commit()

    # ── Persist is_open flag (Alpaca authoritative) and annotate response ────
    # Open = currently pending in Alpaca's open orders list
    #      OR ticker has an active position (filled entry still holding)
    def _calc_is_open(order):
        if order.alpaca_order_id and order.alpaca_order_id in open_order_ids:
            return True
        if order.ticker in open_tickers:
            return True
        return False

    for order in all_orders:
        order.is_open = _calc_is_open(order)

    db.session.commit()

    return jsonify({
        "synced":  updated_count,
        "errors":  errors or None,
        "orders":  [o.to_dict() for o in all_orders],
    }), 200


def _safe_float(value):
    """Convert a string or numeric value to float, return None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None
