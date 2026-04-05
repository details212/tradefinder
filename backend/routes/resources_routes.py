"""
Resource status endpoint — checks Flask, Polygon, Alpaca, and Yahoo Finance
in parallel and returns a structured status report.
GET /api/resources/status   (requires auth token)
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Blueprint, jsonify
import requests as http_req
from auth import token_required
from models import BrokerCredential, Setting

resources_bp = Blueprint("resources", __name__, url_prefix="/api/resources")

BROKER   = "alpaca"
TIMEOUT  = 6   # seconds per probe


def _probe_polygon(api_key: str) -> dict:
    """
    Probe Polygon.io by hitting the same data API the app uses.
    Uses the previous-close endpoint for SPY as a lightweight liveness check.
    """
    if not api_key:
        return {"ok": False, "latency_ms": 0, "detail": "API key not configured"}
    start = time.monotonic()
    try:
        r = http_req.get(
            "https://api.polygon.io/v2/aggs/ticker/SPY/prev",
            params={"apiKey": api_key},
            timeout=TIMEOUT,
        )
        ms = round((time.monotonic() - start) * 1000)
        if r.status_code == 200:
            return {"ok": True, "latency_ms": ms, "detail": "OK"}
        if r.status_code == 403:
            return {"ok": False, "latency_ms": ms, "detail": "Invalid API key"}
        if r.status_code == 429:
            return {"ok": False, "latency_ms": ms, "detail": "Rate limited"}
        return {"ok": False, "latency_ms": ms, "detail": f"HTTP {r.status_code}"}
    except Exception as exc:
        ms = round((time.monotonic() - start) * 1000)
        return {"ok": False, "latency_ms": ms, "detail": str(exc)[:120]}


def _probe_alpaca(api_key: str, api_secret: str, paper_mode: bool) -> dict:
    """
    Ping the Alpaca account endpoint using pre-fetched credentials.
    Credentials are read in the request thread (Flask app context) and passed
    in as plain strings so this function is safe to run in a thread pool.
    """
    if not api_key or not api_secret:
        return {"ok": False, "latency_ms": 0, "detail": "Not configured"}

    base  = "https://paper-api.alpaca.markets" if paper_mode else "https://api.alpaca.markets"
    start = time.monotonic()
    try:
        r = http_req.get(
            f"{base}/v2/account",
            headers={
                "APCA-API-KEY-ID":     api_key,
                "APCA-API-SECRET-KEY": api_secret,
            },
            timeout=TIMEOUT,
        )
        ms = round((time.monotonic() - start) * 1000)
        if r.status_code == 200:
            acct_status = r.json().get("status", "unknown")
            mode_label  = "Paper" if paper_mode else "Live"
            return {"ok": True, "latency_ms": ms, "detail": f"{mode_label} · {acct_status}"}
        try:
            msg = r.json().get("message") or r.text or "Auth failed"
        except Exception:
            msg = r.text or "Auth failed"
        return {"ok": False, "latency_ms": ms, "detail": str(msg)[:80]}
    except Exception as exc:
        ms = round((time.monotonic() - start) * 1000)
        return {"ok": False, "latency_ms": ms, "detail": str(exc)[:120]}


def _probe_yahoo() -> dict:
    """Check Yahoo Finance by fetching a known ticker quote."""
    start = time.monotonic()
    try:
        r = http_req.get(
            "https://query1.finance.yahoo.com/v8/finance/chart/SPY",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=TIMEOUT,
        )
        ms = round((time.monotonic() - start) * 1000)
        ok = r.status_code == 200
        return {"ok": ok, "latency_ms": ms, "detail": "OK" if ok else f"HTTP {r.status_code}"}
    except Exception as exc:
        ms = round((time.monotonic() - start) * 1000)
        return {"ok": False, "latency_ms": ms, "detail": str(exc)[:120]}


@resources_bp.route("/status", methods=["GET"])
@token_required
def resource_status(current_user):
    """
    Probe all four external resources in parallel and return their status.
    Flask itself is always "ok" because we're responding.
    Alpaca credentials are read here (in the request/app context) and passed
    as plain values into the thread pool to avoid SQLAlchemy context errors.
    """
    wall_start = time.monotonic()

    # Read all credentials/keys while we still have the Flask app context
    # Polygon key is stored in the settings table — DB is the only source
    polygon_key = Setting.get("polygon_api_key") or ""
    creds       = BrokerCredential.get_all(current_user.id, BROKER)
    api_key     = creds.get("api_key", "")
    api_secret  = creds.get("api_secret", "")
    paper_mode  = creds.get("paper_mode", "true") == "true"

    results = {}
    tasks = {
        "polygon": lambda: _probe_polygon(polygon_key),
        "alpaca":  lambda: _probe_alpaca(api_key, api_secret, paper_mode),
        "yahoo":   _probe_yahoo,
    }

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn): name for name, fn in tasks.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except Exception as exc:
                results[name] = {"ok": False, "latency_ms": 0, "detail": str(exc)[:120]}

    total_ms = round((time.monotonic() - wall_start) * 1000)
    results["flask"] = {"ok": True, "latency_ms": total_ms, "detail": "Running"}

    return jsonify({"resources": results}), 200
