"""
Migration: create the login_events table.
Run once from the backend directory:
    python migrate_login_events.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

from app import create_app
from extensions import db

DDL = """
CREATE TABLE IF NOT EXISTS login_events (
    id              INT AUTO_INCREMENT PRIMARY KEY,
    user_id         INT          NOT NULL,
    logged_in_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ip_address      VARCHAR(45)  NULL,
    user_agent      VARCHAR(512) NULL,
    platform        VARCHAR(120) NULL,
    open_trades     INT          NULL,
    total_trades    INT          NULL,
    unrealized_pl   DECIMAL(18,4) NULL,
    net_pl          DECIMAL(18,4) NULL,
    win_count       INT          NULL,
    loss_count      INT          NULL,
    CONSTRAINT fk_le_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE,
    INDEX idx_le_user_id (user_id),
    INDEX idx_le_logged_in_at (logged_in_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

app = create_app()
with app.app_context():
    with db.engine.connect() as conn:
        conn.execute(db.text(DDL))
        conn.commit()
    print("login_events table created (or already exists).")
