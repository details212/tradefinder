import time
import requests
from datetime import datetime, timezone
from flask import Blueprint, request, jsonify, current_app
from auth import token_required
from sqlalchemy.exc import OperationalError
from extensions import db
from models import WatchlistItem, Setting

stock_bp = Blueprint("stocks", __name__, url_prefix="/api/stocks")

POLYGON_BASE = "https://api.polygon.io"

_api_key_cache = {"value": None, "expires": 0}


def get_polygon_api_key() -> str:
    """Read the Polygon API key from the database (settings table). DB is the only source."""
    now = time.time()
    if _api_key_cache["value"] and now < _api_key_cache["expires"]:
        return _api_key_cache["value"]
    key = Setting.get("polygon_api_key") or ""
    _api_key_cache["value"] = key
    _api_key_cache["expires"] = now + 60
    return key


POLYGON_TIMEOUT      = 30   # seconds per request (raised from 10/15)
POLYGON_RETRY_DELAY  = 2    # seconds to wait before the single retry


def polygon_get(path: str, params: dict = None) -> dict | None:
    api_key = get_polygon_api_key()
    if not api_key:
        current_app.logger.error("Polygon API key not configured")
        return None
    url   = f"{POLYGON_BASE}{path}"
    query = {"apiKey": api_key, **(params or {})}
    for attempt in range(2):
        try:
            resp = requests.get(url, params=query, timeout=POLYGON_TIMEOUT)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            current_app.logger.error("Polygon HTTP error %s: %s", e.response.status_code, e)
            return None
        except requests.exceptions.RequestException as e:
            if attempt == 0:
                current_app.logger.warning("Polygon request error (will retry): %s", e)
                time.sleep(POLYGON_RETRY_DELAY)
            else:
                current_app.logger.error("Polygon request error (retry exhausted): %s", e)
    return None


def polygon_get_paged(path: str, params: dict = None, max_results: int = 50000) -> dict | None:
    """
    Like polygon_get but follows Polygon's next_url pagination so that the
    full date range is returned rather than just the first page of results.
    Each page is retried once on timeout before giving up.
    """
    api_key = get_polygon_api_key()
    if not api_key:
        current_app.logger.error("Polygon API key not configured")
        return None

    url   = f"{POLYGON_BASE}{path}"
    query = {"apiKey": api_key, **(params or {})}
    all_results: list = []
    envelope: dict = {}

    while url and len(all_results) < max_results:
        data = None
        for attempt in range(2):
            try:
                resp = requests.get(url, params=query, timeout=POLYGON_TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                break
            except requests.exceptions.HTTPError as e:
                current_app.logger.error("Polygon HTTP error %s: %s", e.response.status_code, e)
                return None
            except requests.exceptions.RequestException as e:
                if attempt == 0:
                    current_app.logger.warning("Polygon paged request error (will retry): %s", e)
                    time.sleep(POLYGON_RETRY_DELAY)
                else:
                    current_app.logger.error("Polygon paged request error (retry exhausted): %s", e)
                    return None

        if data is None:
            return None

        envelope = data
        all_results.extend(data.get("results") or [])

        next_url = data.get("next_url")
        if next_url:
            # next_url contains all original params; only inject the API key
            url   = next_url
            query = {"apiKey": api_key}
        else:
            break

    envelope["results"] = all_results
    return envelope


# ── Search (local DB) ─────────────────────────────────────────────────────────

@stock_bp.route("/search", methods=["GET"])
@token_required
def search(current_user):
    query = (request.args.get("q") or "").strip()
    limit = min(int(request.args.get("limit", 200)), 500)

    if not query:
        rows = db.session.execute(
            db.text(
                """
                SELECT ticker, name, type, market, locale, primary_exchange,
                       currency_name, sector, industry,
                       last_day_close, last_day_volume
                FROM tradefinder.tickers
                ORDER BY ticker
                LIMIT :limit
                """
            ),
            {"limit": limit},
        ).fetchall()
    else:
        rows = db.session.execute(
            db.text(
                """
                SELECT ticker, name, type, market, locale, primary_exchange,
                       currency_name, sector, industry,
                       last_day_close, last_day_volume
                FROM tradefinder.tickers
                WHERE ticker LIKE :exact_prefix OR name LIKE :like
                ORDER BY
                    CASE WHEN ticker = :upper THEN 0
                         WHEN ticker LIKE :exact_prefix THEN 1
                         ELSE 2
                    END,
                    ticker
                LIMIT :limit
                """
            ),
            {"like": f"%{query}%", "exact_prefix": f"{query.upper()}%",
             "upper": query.upper(), "limit": limit},
        ).fetchall()

    return jsonify({
        "results": [
            {
                "ticker": r.ticker,
                "name": r.name,
                "type": r.type,
                "market": r.market,
                "locale": r.locale,
                "primary_exchange": r.primary_exchange,
                "currency_name": r.currency_name,
                "sector": r.sector,
                "industry": r.industry,
                "last_day_close": float(r.last_day_close) if r.last_day_close else None,
                "last_day_volume": r.last_day_volume,
            }
            for r in rows
        ]
    }), 200


# ── Quote / Snapshot ──────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/quote", methods=["GET"])
@token_required
def quote(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
    if data is None:
        return jsonify({"error": "Failed to fetch quote"}), 502

    t = data.get("ticker", {})
    day = t.get("day", {})
    prev = t.get("prevDay", {})
    last_trade = t.get("lastTrade", {})
    last_quote = t.get("lastQuote", {})
    minute = t.get("min", {})

    return jsonify({
        "ticker": ticker,
        "last_trade_price": last_trade.get("p"),
        "last_trade_size": last_trade.get("s"),
        "bid": last_quote.get("p"),   # lowercase p = bid price in Polygon snapshot
        "ask": last_quote.get("P"),   # uppercase P = ask price
        "bid_size": last_quote.get("s"),  # lowercase s = bid size
        "ask_size": last_quote.get("S"),  # uppercase S = ask size
        "open": day.get("o"),
        "high": day.get("h"),
        "low": day.get("l"),
        "close": day.get("c"),
        "volume": day.get("v"),
        "vwap": day.get("vw"),
        "prev_close": prev.get("c"),
        "prev_volume": prev.get("v"),
        "change": t.get("todaysChange"),
        "change_pct": t.get("todaysChangePerc"),
        "minute_open": minute.get("o"),
        "minute_close": minute.get("c"),
        "minute_volume": minute.get("v"),
        "updated": t.get("updated"),
    }), 200


# ── Company Details ───────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/details", methods=["GET"])
@token_required
def details(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get(f"/v3/reference/tickers/{ticker}")
    if data is None:
        return jsonify({"error": "Failed to fetch ticker details"}), 502

    r = data.get("results", {})
    address = r.get("address", {})
    branding = r.get("branding", {})
    api_key = get_polygon_api_key()

    def branded_url(url):
        if url:
            return f"{url}?apiKey={api_key}"
        return None

    return jsonify({
        "ticker": r.get("ticker"),
        "name": r.get("name"),
        "description": r.get("description"),
        "homepage_url": r.get("homepage_url"),
        "phone_number": r.get("phone_number"),
        "market_cap": r.get("market_cap"),
        "employees": r.get("total_employees"),
        "sic_code": r.get("sic_code"),
        "sic_description": r.get("sic_description"),
        "list_date": r.get("list_date"),
        "share_class_shares_outstanding": r.get("share_class_shares_outstanding"),
        "weighted_shares_outstanding": r.get("weighted_shares_outstanding"),
        "round_lot": r.get("round_lot"),
        "primary_exchange": r.get("primary_exchange"),
        "currency_name": r.get("currency_name"),
        "type": r.get("type"),
        "market": r.get("market"),
        "locale": r.get("locale"),
        "active": r.get("active"),
        "cik": r.get("cik"),
        "address": {
            "address1": address.get("address1"),
            "city": address.get("city"),
            "state": address.get("state"),
            "postal_code": address.get("postal_code"),
        },
        "logo_url": branded_url(branding.get("logo_url")),
        "icon_url": branded_url(branding.get("icon_url")),
    }), 200


# ── Previous Close ────────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/prev-close", methods=["GET"])
@token_required
def prev_close(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get(f"/v2/aggs/ticker/{ticker}/prev")
    if data is None:
        return jsonify({"error": "Failed to fetch previous close"}), 502

    results = data.get("results", [])
    if not results:
        return jsonify({"error": "No data"}), 404

    r = results[0]
    return jsonify({
        "ticker": ticker,
        "open": r.get("o"),
        "high": r.get("h"),
        "low": r.get("l"),
        "close": r.get("c"),
        "volume": r.get("v"),
        "vwap": r.get("vw"),
        "transactions": r.get("n"),
        "timestamp": r.get("t"),
    }), 200


# ── OHLC History / Aggregates ─────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/history", methods=["GET"])
@token_required
def history(current_user, ticker: str):
    ticker = ticker.upper()
    multiplier = request.args.get("multiplier", "1")
    timespan = request.args.get("timespan", "day")
    from_date = request.args.get("from", "2024-01-01")
    to_date = request.args.get("to", "2026-03-14")
    limit = request.args.get("limit", "500")
    adjusted = request.args.get("adjusted", "true")

    data = polygon_get_paged(
        f"/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
        {"adjusted": adjusted, "sort": "asc", "limit": limit},
        max_results=int(limit) if str(limit).isdigit() else 5000,
    )
    if data is None:
        return jsonify({"error": "Failed to fetch history"}), 502

    bars = [
        {
            "t": r.get("t"),
            "o": r.get("o"),
            "h": r.get("h"),
            "l": r.get("l"),
            "c": r.get("c"),
            "v": r.get("v"),
            "vw": r.get("vw"),
            "n": r.get("n"),
        }
        for r in data.get("results", [])
    ]
    return jsonify({
        "ticker": ticker,
        "bars": bars,
        "count": len(bars),
        "adjusted": adjusted,
        "status": data.get("status"),
    }), 200


# ── Technical Indicators ──────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/indicators/<string:indicator>", methods=["GET"])
@token_required
def technical_indicator(current_user, ticker: str, indicator: str):
    """
    Supported indicators: sma, ema, macd, rsi
    Query params: timespan (day/hour), window (int), series_type (close/open/high/low)
    """
    ticker = ticker.upper()
    indicator = indicator.lower()

    valid = {"sma", "ema", "macd", "rsi"}
    if indicator not in valid:
        return jsonify({"error": f"indicator must be one of: {', '.join(valid)}"}), 400

    params = {
        "timespan": request.args.get("timespan", "day"),
        "adjusted": request.args.get("adjusted", "true"),
        "series_type": request.args.get("series_type", "close"),
        "order": "desc",
        "limit": request.args.get("limit", "100"),
    }

    if indicator in ("sma", "ema"):
        params["window"] = request.args.get("window", "50")
    elif indicator == "rsi":
        params["window"] = request.args.get("window", "14")
    elif indicator == "macd":
        params["short_window"] = request.args.get("short_window", "12")
        params["long_window"] = request.args.get("long_window", "26")
        params["signal_window"] = request.args.get("signal_window", "9")

    data = polygon_get(f"/v1/indicators/{indicator}/{ticker}", params)
    if data is None:
        return jsonify({"error": f"Failed to fetch {indicator.upper()}"}), 502

    results = data.get("results", {})
    values = results.get("values", [])

    return jsonify({
        "ticker": ticker,
        "indicator": indicator.upper(),
        "values": values,
        "parameters": results.get("parameters", {}),
    }), 200


# ── Financials ────────────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/financials", methods=["GET"])
@token_required
def financials(current_user, ticker: str):
    ticker = ticker.upper()
    timeframe = request.args.get("timeframe", "quarterly")  # annual | quarterly | ttm

    data = polygon_get("/vX/reference/financials", {
        "ticker": ticker,
        "timeframe": timeframe,
        "order": "desc",
        "limit": 8,
        "include_sources": "false",
    })
    if data is None:
        return jsonify({"error": "Failed to fetch financials"}), 502

    def safe(d, *keys):
        for k in keys:
            if not isinstance(d, dict):
                return None
            d = d.get(k)
        if isinstance(d, dict):
            return d.get("value")
        return d

    periods = []
    for r in data.get("results", []):
        ic = r.get("financials", {}).get("income_statement", {})
        bs = r.get("financials", {}).get("balance_sheet", {})
        cf = r.get("financials", {}).get("cash_flow_statement", {})
        comp = r.get("financials", {}).get("comprehensive_income", {})

        periods.append({
            "period": r.get("period_of_report_date"),
            "timeframe": r.get("timeframe"),
            "fiscal_year": r.get("fiscal_year"),
            "fiscal_period": r.get("fiscal_period"),
            "start_date": r.get("start_date"),
            "end_date": r.get("end_date"),
            "income_statement": {
                "revenues": safe(ic, "revenues", "value") or safe(ic, "net_revenues", "value"),
                "gross_profit": safe(ic, "gross_profit", "value"),
                "operating_income": safe(ic, "operating_income_loss", "value"),
                "net_income": safe(ic, "net_income_loss", "value"),
                "eps_basic": safe(ic, "basic_earnings_per_share", "value"),
                "eps_diluted": safe(ic, "diluted_earnings_per_share", "value"),
                "ebit": safe(ic, "income_loss_from_continuing_operations_before_tax", "value"),
                "interest_expense": safe(ic, "interest_expense_operating", "value"),
                "rd_expense": safe(ic, "research_and_development", "value"),
                "operating_expenses": safe(ic, "operating_expenses", "value"),
                "income_tax": safe(ic, "income_tax_expense_benefit", "value"),
            },
            "balance_sheet": {
                "assets": safe(bs, "assets", "value"),
                "current_assets": safe(bs, "current_assets", "value"),
                "noncurrent_assets": safe(bs, "noncurrent_assets", "value"),
                "liabilities": safe(bs, "liabilities", "value"),
                "current_liabilities": safe(bs, "current_liabilities", "value"),
                "noncurrent_liabilities": safe(bs, "noncurrent_liabilities", "value"),
                "equity": safe(bs, "equity", "value"),
                "cash": safe(bs, "cash", "value"),
                "inventory": safe(bs, "inventory", "value"),
                "long_term_debt": safe(bs, "long_term_debt", "value"),
            },
            "cash_flow": {
                "operating": safe(cf, "net_cash_flow_from_operating_activities", "value"),
                "investing": safe(cf, "net_cash_flow_from_investing_activities", "value"),
                "financing": safe(cf, "net_cash_flow_from_financing_activities", "value"),
                "capex": safe(cf, "capital_expenditure", "value"),
                "free_cash_flow": safe(cf, "free_cash_flow", "value"),
            },
            "comprehensive_income": {
                "net_income_loss": safe(comp, "comprehensive_income_loss", "value"),
            },
        })

    return jsonify({"ticker": ticker, "timeframe": timeframe, "periods": periods}), 200


# ── Dividends ─────────────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/dividends", methods=["GET"])
@token_required
def dividends(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get("/v3/reference/dividends", {
        "ticker": ticker,
        "order": "desc",
        "limit": 20,
    })
    if data is None:
        return jsonify({"error": "Failed to fetch dividends"}), 502

    records = [
        {
            "cash_amount": r.get("cash_amount"),
            "currency": r.get("currency"),
            "declaration_date": r.get("declaration_date"),
            "dividend_type": r.get("dividend_type"),
            "ex_dividend_date": r.get("ex_dividend_date"),
            "frequency": r.get("frequency"),
            "pay_date": r.get("pay_date"),
            "record_date": r.get("record_date"),
        }
        for r in data.get("results", [])
    ]
    return jsonify({"ticker": ticker, "dividends": records}), 200


# ── Stock Splits ──────────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/splits", methods=["GET"])
@token_required
def splits(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get("/v3/reference/splits", {
        "ticker": ticker,
        "order": "desc",
        "limit": 20,
    })
    if data is None:
        return jsonify({"error": "Failed to fetch splits"}), 502

    records = [
        {
            "execution_date": r.get("execution_date"),
            "split_from": r.get("split_from"),
            "split_to": r.get("split_to"),
            "ratio": (r.get("split_to") / r.get("split_from")) if r.get("split_from") else None,
        }
        for r in data.get("results", [])
    ]
    return jsonify({"ticker": ticker, "splits": records}), 200


# ── Related Companies ─────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/related", methods=["GET"])
@token_required
def related(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get(f"/v1/related-companies/{ticker}")
    if data is None:
        return jsonify({"error": "Failed to fetch related companies"}), 502

    return jsonify({
        "ticker": ticker,
        "related": data.get("results", []),
    }), 200


# ── Market Status ─────────────────────────────────────────────────────────────

@stock_bp.route("/market-status", methods=["GET"])
@token_required
def market_status(current_user):
    data = polygon_get("/v1/marketstatus/now")
    if data is None:
        return jsonify({"error": "Failed to fetch market status"}), 502
    return jsonify(data), 200


# ── News ──────────────────────────────────────────────────────────────────────

@stock_bp.route("/<string:ticker>/news", methods=["GET"])
@token_required
def news(current_user, ticker: str):
    ticker = ticker.upper()
    data = polygon_get("/v2/reference/news", {
        "ticker": ticker,
        "limit": 10,
        "order": "desc",
        "sort": "published_utc",
    })
    if data is None:
        return jsonify({"error": "Failed to fetch news"}), 502

    articles = [
        {
            "id": a.get("id"),
            "title": a.get("title"),
            "author": a.get("author"),
            "published_utc": a.get("published_utc"),
            "article_url": a.get("article_url"),
            "description": a.get("description"),
            "image_url": a.get("image_url"),
            "publisher": a.get("publisher", {}).get("name"),
            "tickers": a.get("tickers", []),
            "keywords": a.get("keywords", []),
        }
        for a in data.get("results", [])
    ]
    return jsonify({"ticker": ticker, "articles": articles}), 200


# ── Watchlist ─────────────────────────────────────────────────────────────────

def _parse_watchlist_bar_time(raw):
    """
    Parse bar_time from JSON: ISO-8601 string (with optional Z), unix seconds, or ms.
    Returns naive UTC datetime for DB storage, or None if unparseable.
    Never raises — avoids 500 when the client sends a number (e.g. epoch ms).
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        ts = float(raw)
        if ts > 1e12:  # assume milliseconds
            ts /= 1000.0
        try:
            return datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
        except (OSError, ValueError, OverflowError):
            return None
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            try:
                return _parse_watchlist_bar_time(float(s))
            except (TypeError, ValueError):
                return None
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    return None


@stock_bp.route("/watchlist", methods=["GET"])
@token_required
def get_watchlist(current_user):
    items = WatchlistItem.query.filter_by(user_id=current_user.id).all()
    return jsonify({"watchlist": [i.to_dict() for i in items]}), 200


@stock_bp.route("/watchlist/<string:ticker>", methods=["POST"])
@token_required
def add_to_watchlist(current_user, ticker: str):
    from flask import request as req
    ticker = ticker.upper()
    body = req.get_json(silent=True) or {}
    bias      = body.get("bias")      or None
    threshold = body.get("threshold") or None
    source_raw = body.get("source")
    source = None
    if source_raw is not None and isinstance(source_raw, str) and source_raw.strip():
        s = source_raw.strip().lower()[:32]
        if s in ("tradeideas", "patternanalysis", "stocks"):
            source = s
    bar_time_raw = body.get("bar_time")
    bar_time = _parse_watchlist_bar_time(bar_time_raw)

    existing = WatchlistItem.query.filter_by(user_id=current_user.id, ticker=ticker).first()
    created = False
    if existing:
        item = existing
        # Already exists — update fields if provided
        if bias      is not None: item.bias      = bias
        if threshold is not None: item.threshold = threshold
        if bar_time  is not None: item.bar_time  = bar_time
        if source is not None:
            item.source = source
    else:
        item = WatchlistItem(user_id=current_user.id, ticker=ticker,
                             bias=bias, threshold=threshold, bar_time=bar_time, source=source)
        db.session.add(item)
        created = True

    try:
        db.session.commit()
    except OperationalError as e:
        db.session.rollback()
        msg = str(getattr(e, "orig", e) or e).lower()
        if "source" in msg or "unknown column" in msg:
            current_app.logger.error("watchlist commit failed (missing column?): %s", e)
            return jsonify({
                "error": (
                    "Database is missing the watchlist.source column. "
                    "Run: ALTER TABLE watchlist ADD COLUMN source VARCHAR(32) NULL;"
                ),
            }), 500
        raise

    return jsonify({"item": item.to_dict()}), (201 if created else 200)


@stock_bp.route("/watchlist/<string:ticker>", methods=["DELETE"])
@token_required
def remove_from_watchlist(current_user, ticker: str):
    ticker = ticker.upper()
    item = WatchlistItem.query.filter_by(user_id=current_user.id, ticker=ticker).first()
    if not item:
        return jsonify({"error": "Ticker not in watchlist"}), 404
    db.session.delete(item)
    db.session.commit()
    return jsonify({"message": f"{ticker} removed from watchlist"}), 200
