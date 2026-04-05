"""
Migration: add is_open column to the orders table.
Run once from the backend directory:
    py migrate_orders_is_open.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

CHECK_COL = """
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
  AND TABLE_NAME   = 'orders'
  AND COLUMN_NAME  = 'is_open';
"""

DDL = """
ALTER TABLE orders
  ADD COLUMN is_open TINYINT(1) NULL DEFAULT 1
    COMMENT 'Alpaca-authoritative open flag; updated every sync';
"""

# Back-fill: mark orders in known-closed statuses as is_open=0
BACKFILL = """
UPDATE orders
SET is_open = 0
WHERE status IN ('canceled', 'expired', 'rejected', 'done_for_day');
"""

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        exists = conn.execute(db.text(CHECK_COL)).scalar()
        if exists:
            print("orders.is_open already exists — skipping ALTER.")
        else:
            conn.execute(db.text(DDL))
            print("orders.is_open column added.")
        conn.execute(db.text(BACKFILL))
        conn.commit()
    print("Back-fill complete.")
