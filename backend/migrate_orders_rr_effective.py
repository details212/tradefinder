"""
Migration: add rr_ratio_effective column to the orders table.

  rr_ratio           — the user's intended R/R input value (e.g. "3")
  rr_ratio_effective — actual ratio computed from entry/stop/target prices at submission
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

MIGRATIONS = [
    "ALTER TABLE orders ADD COLUMN rr_ratio_effective DECIMAL(10,4) NULL",
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
                col = ddl.split("ADD COLUMN")[1].strip().split()[0]
                if column_exists(conn, "orders", col):
                    print(f"  skip  — orders.{col} already exists")
                else:
                    conn.execute(db.text(ddl))
                    print(f"  added — orders.{col}")

    print("Migration complete.")


if __name__ == "__main__":
    run()
