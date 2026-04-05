#!/usr/bin/env python3
"""
Debug script: compare the local trades table record vs live Alpaca order status.

Usage:
    python debug_order.py PCAR
    python debug_order.py PCAR --all      # show all DB rows for symbol
"""

import argparse
import sys
import mysql.connector

DB_CONFIG = {
    "host":     "127.0.0.1",
    "user":     "remote",
    "password": "Chamba4347!",
    "database": "leo",
}


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def hr(title=""):
    print("\n" + ("─" * 60))
    if title:
        print(f"  {title}")
        print("─" * 60)


def main():
    parser = argparse.ArgumentParser(description="Debug a trade vs Alpaca order status.")
    parser.add_argument("symbol", help="Ticker symbol, e.g. PCAR")
    parser.add_argument("--all", action="store_true", help="Show all DB rows, not just open ones")
    args = parser.parse_args()
    symbol = args.symbol.upper().strip()

    # ── 1. Load credentials ───────────────────────────────────────────────────
    conn = get_conn()
    cur  = conn.cursor(dictionary=True)
    cur.execute("SELECT `key`, `secret`, `endpoint` FROM settings WHERE id = 1")
    creds = cur.fetchone()
    if not creds or not creds.get("key"):
        print("ERROR: No Alpaca credentials found in settings table.")
        sys.exit(1)
    is_paper = "paper" in (creds.get("endpoint") or "").lower()
    print(f"Alpaca endpoint : {creds.get('endpoint')}")
    print(f"Paper mode      : {is_paper}")

    # ── 2. Load DB trades for symbol ──────────────────────────────────────────
    where = "symbol = %s" if args.all else "symbol = %s AND closed_at IS NULL"
    cur.execute(
        f"""
        SELECT id, order_id, symbol, qty, entry_price, stop_price, target_price,
               order_status, is_paper, submitted_at, closed_at, exit_price
        FROM trades
        WHERE {where}
        ORDER BY submitted_at DESC
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    hr(f"DB trades for {symbol} ({'all' if args.all else 'open only'})")
    if not rows:
        print("  No rows found.")
    for r in rows:
        print(f"\n  id           : {r['id']}")
        print(f"  order_id     : {r['order_id']}")
        print(f"  qty          : {r['qty']}")
        print(f"  entry_price  : {r['entry_price']}")
        print(f"  stop_price   : {r['stop_price']}")
        print(f"  target_price : {r['target_price']}")
        print(f"  order_status : {r['order_status']}")
        print(f"  is_paper     : {r['is_paper']}")
        print(f"  submitted_at : {r['submitted_at']}")
        print(f"  closed_at    : {r['closed_at']}")
        print(f"  exit_price   : {r['exit_price']}")

    if not rows:
        sys.exit(0)

    # ── 3. Query Alpaca for each order ────────────────────────────────────────
    from alpaca.trading.client import TradingClient

    client = TradingClient(
        api_key    = creds["key"],
        secret_key = creds["secret"],
        paper      = is_paper,
    )

    def ev(val):
        if val is None:
            return None
        s = str(val)
        return s.split(".")[-1].lower() if "." in s else s.lower()

    for r in rows:
        order_id = r["order_id"]
        hr(f"Alpaca order: {order_id[:12]}…  ({symbol})")
        try:
            order = client.get_order_by_id(order_id)
            print(f"  status        : {ev(order.status)}")
            print(f"  order_class   : {ev(order.order_class)}")
            print(f"  type          : {ev(order.type)}")
            print(f"  side          : {ev(order.side)}")
            print(f"  qty           : {order.qty}")
            print(f"  filled_qty    : {order.filled_qty}")
            print(f"  limit_price   : {order.limit_price}")
            print(f"  filled_avg    : {order.filled_avg_price}")
            print(f"  submitted_at  : {order.submitted_at}")
            print(f"  filled_at     : {order.filled_at}")
            print(f"  canceled_at   : {order.canceled_at}")
            print(f"  expired_at    : {order.expired_at}")

            legs = order.legs or []
            print(f"\n  Legs ({len(legs)}):")
            for i, leg in enumerate(legs):
                ltype = ev(leg.type)
                tag   = "TP" if (ltype == "limit" and ev(leg.side) in ("sell", "short")) else \
                        "SL" if "stop" in (ltype or "") else f"Leg{i+1}"
                print(f"    [{tag}] status={ev(leg.status)}  type={ltype}  "
                      f"price={leg.limit_price or leg.stop_price}  "
                      f"filled_qty={leg.filled_qty}  avg_fill={leg.filled_avg_price}")

            # ── 4. Diagnose discrepancy ───────────────────────────────────────
            alpaca_status = ev(order.status)
            db_closed     = r["closed_at"] is not None

            hr("Diagnosis")
            terminal = {"filled", "canceled", "expired", "replaced"}
            if alpaca_status in terminal and not db_closed:
                print(f"  ⚠  Alpaca status is '{alpaca_status}' but DB closed_at is NULL.")
                print("     → Run '⟳ Sync Alpaca' on the /orders page, or:")
                print(f"     UPDATE trades SET order_status='{alpaca_status}', closed_at=NOW() WHERE id={r['id']};")
            elif alpaca_status not in terminal and db_closed:
                print(f"  ⚠  DB shows closed but Alpaca status is '{alpaca_status}'.")
            elif alpaca_status in terminal and db_closed:
                print(f"  ✓  Both agree: order is closed ({alpaca_status}).")
            else:
                print(f"  ✓  Both agree: order is open ({alpaca_status}).")

        except Exception as exc:
            print(f"  ERROR fetching from Alpaca: {exc}")


if __name__ == "__main__":
    main()
