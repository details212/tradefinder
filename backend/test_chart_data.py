"""
Diagnostic test for trade-idea chart data download.

Tests each layer of the chain in order:
  1. Flask app context + DB connectivity
  2. Polygon API key present in the settings table
  3. Direct Polygon history call (bypasses Flask routing)
  4. Full Flask route: GET /api/stocks/{ticker}/history

Run from the backend directory:
    python test_chart_data.py [TICKER]          (default: AAPL)
"""

import sys
import json
import time
import requests as _requests

TICKER  = (sys.argv[1].upper() if len(sys.argv) > 1 else "AAPL")
BASE    = "http://localhost:5000"
PASS    = "  [PASS]"
FAIL    = "  [FAIL]"
WARN    = "  [WARN]"


def section(title):
    bar = "-" * max(0, 60 - len(title))
    print(f"\n-- {title} {bar}")


def ok(msg):
    print(f"{PASS}  {msg}")


def fail(msg):
    print(f"{FAIL}  {msg}")


def warn(msg):
    print(f"{WARN}  {msg}")


# ── 1. Flask app + DB ─────────────────────────────────────────────────────────
section("1. Flask app context & database connection")
try:
    from app import create_app
    app = create_app()
    ok("Flask app created successfully")
except Exception as e:
    fail(f"Could not create Flask app: {e}")
    sys.exit(1)

try:
    with app.app_context():
        from extensions import db
        db.session.execute(db.text("SELECT 1"))
        ok("Database connection OK")
except Exception as e:
    fail(f"Database connection failed: {e}")
    sys.exit(1)


# ── 2. Polygon API key in DB ──────────────────────────────────────────────────
section("2. Polygon API key in settings table")
polygon_key = None
try:
    with app.app_context():
        from models import Setting
        polygon_key = Setting.get("polygon_api_key")
        if not polygon_key:
            fail("polygon_api_key not found in settings table (is it configured in Admin → Settings?)")
            sys.exit(1)
        masked = polygon_key[:4] + "*" * (len(polygon_key) - 8) + polygon_key[-4:]
        ok(f"Key found: {masked}  (length {len(polygon_key)})")
except Exception as e:
    fail(f"Could not read settings table: {e}")
    sys.exit(1)


# ── 3. Direct Polygon API call ────────────────────────────────────────────────
section(f"3. Direct Polygon /v2/aggs call for {TICKER}")
POLYGON_BASE = "https://api.polygon.io"
today   = time.strftime("%Y-%m-%d")
from_dt = time.strftime("%Y-%m-%d", time.localtime(time.time() - 3 * 24 * 3600))

poly_url = (
    f"{POLYGON_BASE}/v2/aggs/ticker/{TICKER}/range/5/minute/{from_dt}/{today}"
)
try:
    resp = _requests.get(
        poly_url,
        params={"apiKey": polygon_key, "adjusted": "true", "sort": "asc", "limit": "100"},
        timeout=15,
    )
    if resp.status_code == 200:
        data = resp.json()
        results = data.get("results") or []
        count   = len(results)
        status  = data.get("status")
        if count > 0:
            ok(f"Polygon returned {count} bars  (status={status})")
        else:
            warn(f"Polygon responded 200 but returned 0 bars  (status={status})")
            warn(f"  → Market may be closed / no data for range {from_dt} → {today}")
    elif resp.status_code == 403:
        fail(f"Polygon returned 403 Forbidden — API key is invalid or expired")
        print(f"     Response: {resp.text[:300]}")
        sys.exit(1)
    elif resp.status_code == 429:
        fail("Polygon returned 429 Too Many Requests — rate limit hit")
        sys.exit(1)
    else:
        fail(f"Polygon returned HTTP {resp.status_code}")
        print(f"     Response: {resp.text[:300]}")
        sys.exit(1)
except _requests.exceptions.ConnectionError:
    fail("Could not reach api.polygon.io — check internet connectivity")
    sys.exit(1)
except _requests.exceptions.Timeout:
    fail("Polygon request timed out (>15 s)")
    sys.exit(1)


# ── 4. Flask route: GET /api/stocks/{ticker}/history ─────────────────────────
section(f"4. Flask route  GET /api/stocks/{TICKER}/history")

# Try to get a valid JWT for the test (reads any active token from DB)
jwt_token = None
try:
    with app.app_context():
        from models import User
        user = User.query.first()
        if user:
            from auth import generate_token
            jwt_token = generate_token(user.id)
            ok(f"Test JWT obtained for user '{user.username}'")
        else:
            warn("No users found in DB — will test without auth (expect 401)")
except Exception as e:
    warn(f"Could not generate JWT ({e}) — will test without auth (expect 401)")

headers = {"Authorization": f"Bearer {jwt_token}"} if jwt_token else {}

try:
    resp = _requests.get(
        f"{BASE}/api/stocks/{TICKER}/history",
        params={"multiplier": 5, "timespan": "minute",
                "from": from_dt, "to": today, "limit": 500},
        headers=headers,
        timeout=30,
    )
    if resp.status_code == 200:
        data = resp.json()
        bars  = data.get("bars", [])
        count = len(bars)
        if count > 0:
            first_t = bars[0].get("t")
            last_t  = bars[-1].get("t")
            ok(f"Route returned {count} bars")
            ok(f"  First bar t={first_t}   Last bar t={last_t}")
        else:
            warn(f"Route responded 200 but bars=[]  (Polygon returned no data for this range)")
    elif resp.status_code == 401:
        fail("Route returned 401 Unauthorized — JWT is missing or invalid")
        print(f"     Response: {resp.text[:300]}")
    elif resp.status_code == 502:
        fail("Route returned 502 — backend failed to fetch from Polygon (check server logs)")
        print(f"     Response: {resp.text[:300]}")
    else:
        fail(f"Route returned HTTP {resp.status_code}")
        print(f"     Response: {resp.text[:300]}")
except _requests.exceptions.ConnectionError:
    fail(f"Could not connect to {BASE} — is the Flask server running?")
except _requests.exceptions.Timeout:
    fail("Flask route timed out (>30 s)")


# ── Summary ───────────────────────────────────────────────────────────────────
section("Done")
print("  All checks complete. Review any FAIL / WARN items above.\n")
