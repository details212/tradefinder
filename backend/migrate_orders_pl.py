"""
Migration: add P/L tracking columns to the orders table.

Columns added (all safe to re-run — uses IF NOT EXISTS pattern):
  filled_avg_price  DECIMAL(18,4)  actual execution price from Alpaca
  current_price     DECIMAL(18,4)  price at last sync
  unrealized_pl     DECIMAL(18,4)  open P/L = (current - fill) * qty * side
  synced_at         DATETIME       when P/L was last refreshed
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

MIGRATIONS = [
    "ALTER TABLE orders ADD COLUMN filled_avg_price DECIMAL(18,4) NULL",
    "ALTER TABLE orders ADD COLUMN current_price    DECIMAL(18,4) NULL",
    "ALTER TABLE orders ADD COLUMN unrealized_pl    DECIMAL(18,4) NULL",
    "ALTER TABLE orders ADD COLUMN synced_at        DATETIME      NULL",
]

def column_exists(conn, table: str, column: str) -> bool:
    result = conn.execute(
        db.text(
            "SELECT COUNT(*) FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = DATABASE() "
            "  AND TABLE_NAME   = :table "
            "  AND COLUMN_NAME  = :column"
        ),
        {"table": table, "column": column},
    )
    return result.scalar() > 0


def run():
    app = create_app()
    with app.app_context():
        with db.engine.begin() as conn:
            for ddl in MIGRATIONS:
                # Extract column name from the DDL string
                col = ddl.split("ADD COLUMN")[1].strip().split()[0]
                if column_exists(conn, "orders", col):
                    print(f"  skip  — orders.{col} already exists")
                else:
                    conn.execute(db.text(ddl))
                    print(f"  added — orders.{col}")

    print("Migration complete.")


if __name__ == "__main__":
    run()
