"""
Shared Alpaca flatten logic for HTTP handlers and background jobs.

Cancels bracket legs, cancels parent if needed, then closes the position
(DELETE /v2/positions/{symbol} or fallback market order).
"""
import requests as http_req
from models import BrokerCredential, Order
from extensions import db

BROKER = "alpaca"


def perform_market_close_for_order(user_id: int, order: Order) -> list:
    """
    Cancel bracket legs and flatten via Alpaca. Updates order row (status, is_open).

    Returns a list of error strings; empty means no errors were collected.
    """
    creds = BrokerCredential.get_all(user_id, BROKER)
    api_key = creds.get("api_key", "")
    api_secret = creds.get("api_secret", "")
    paper_mode = creds.get("paper_mode", "true") == "true"

    if not api_key or not api_secret:
        return ["Alpaca credentials not configured."]

    base = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret,
        "Content-Type": "application/json",
    }

    errors = []

    try:
        parent_resp = http_req.get(
            f"{base}/v2/orders/{order.alpaca_order_id}",
            headers=headers,
            params={"nested": "true"},
            timeout=10,
        )
        if parent_resp.status_code == 200:
            parent_data = parent_resp.json()
            legs = parent_data.get("legs") or []
            for leg in legs:
                leg_id = leg.get("id")
                leg_status = leg.get("status", "")
                if leg_id and leg_status not in ("canceled", "filled", "expired", "rejected"):
                    cancel_resp = http_req.delete(
                        f"{base}/v2/orders/{leg_id}",
                        headers=headers,
                        timeout=10,
                    )
                    if cancel_resp.status_code not in (200, 204):
                        errors.append(f"Leg {leg_id}: {cancel_resp.text}")
        else:
            errors.append(f"Could not fetch parent order: {parent_resp.text}")
    except Exception as exc:
        errors.append(f"Leg cancellation error: {str(exc)}")

    try:
        http_req.delete(
            f"{base}/v2/orders/{order.alpaca_order_id}",
            headers=headers,
            timeout=10,
        )
    except Exception as exc:
        errors.append(f"Parent cancel error: {str(exc)}")

    ticker = order.ticker.upper()
    side = "sell" if order.direction == "long" else "buy"
    try:
        close_resp = http_req.delete(
            f"{base}/v2/positions/{ticker}",
            headers=headers,
            timeout=15,
        )
        if close_resp.status_code not in (200, 204):
            mkt_resp = http_req.post(
                f"{base}/v2/orders",
                headers=headers,
                json={
                    "symbol": ticker,
                    "qty": str(order.qty),
                    "side": side,
                    "type": "market",
                    "time_in_force": "day",
                },
                timeout=15,
            )
            if mkt_resp.status_code not in (200, 201):
                errors.append(f"Market close failed: {mkt_resp.text}")
    except Exception as exc:
        errors.append(f"Position close error: {str(exc)}")

    try:
        order.status = "canceled"
        order.is_open = False
        db.session.commit()
    except Exception:
        db.session.rollback()

    return errors
