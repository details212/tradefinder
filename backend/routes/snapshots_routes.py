from flask import Blueprint, jsonify, request
from auth import token_required
from extensions import db

snapshots_bp = Blueprint("snapshots", __name__, url_prefix="/api/snapshots")


@snapshots_bp.route("/prices", methods=["GET"])
@token_required
def get_prices(current_user):
    """
    Return cached prices for a comma-separated list of tickers.
    Prices are maintained by the background snapshot_service thread – this
    endpoint is a fast DB read only, no Polygon calls on the request path.
    """
    raw = request.args.get("tickers", "").strip()
    if not raw:
        return jsonify({"prices": {}}), 200

    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    if not tickers:
        return jsonify({"prices": {}}), 200

    rows = db.session.execute(
        db.text(
            "SELECT ticker, price, change_pct "
            "FROM snapshot_cache "
            "WHERE ticker IN :tickers"
        ),
        {"tickers": tuple(tickers)},
    ).fetchall()

    prices = {
        r.ticker: {
            "price":      float(r.price),
            "change_pct": float(r.change_pct) if r.change_pct is not None else None,
        }
        for r in rows
        if r.price is not None
    }

    return jsonify({"prices": prices}), 200
